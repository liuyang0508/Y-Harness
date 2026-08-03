//! Explicit, backup-first SQLite State schema migration.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use sha2::{Digest, Sha256};
use tokio::task;

use super::{
    AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION, MAX_STATE_EVENT_BYTES, MAX_THREAD_NAME_BYTES,
    STATE_EVENT_RECOVERY_OVERHEAD_BYTES, STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION,
    SqliteEventStore, configure_sqlite_busy_timeout, configure_sqlite_session, decode_row,
    validate_thread_name,
    wait_due::{
        AgentLoopDuePhase, AgentLoopWaitProjection, AgentLoopWaitProjectionChange,
        projection_change_for_materialized_replay,
    },
};
use crate::{
    AgentLoopResumeCommandId, ApprovalId, AuthorityContext, EventId, HarnessError, ThreadId,
    TurnId,
    sqlite::{bounded_optional_text, bounded_text},
};

const FIRST_STATE_EVENT_SCHEMA_VERSION: u32 = 1;
const FIRST_METADATA_STATE_EVENT_SCHEMA_VERSION: u32 = 2;
const MIN_MIGRATION_WORKING_BYTES: u64 = 1_048_576;
const STATE_METADATA_TABLE: &str = "state_store_metadata";
const BACKUP_MANIFEST_TABLE: &str = "y_harness_migration_backup";
const AGENT_LOOP_WAIT_PROJECTION_TABLE: &str = "agent_loop_wait_projection";
const AGENT_LOOP_WAIT_DUE_INDEX: &str = "agent_loop_wait_due";

/// Result category for one explicit SQLite State migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateMigrationStatus {
    /// A legacy store was backed up and advanced.
    Migrated,
    /// The store already used the current writer coordinate.
    AlreadyCurrent,
}

/// Content-free evidence returned by one migration attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateMigrationReport {
    /// Settlement category.
    pub status: StateMigrationStatus,
    /// Event schema written before migration.
    pub from_event_schema: u32,
    /// Event schema written after migration.
    pub to_event_schema: u32,
    /// Live-wait projection schema written before migration, when present.
    pub from_agent_loop_wait_projection_schema: Option<u32>,
    /// Live-wait projection schema installed after migration.
    pub to_agent_loop_wait_projection_schema: u32,
    /// Number of immutable historical event rows left in place.
    pub historical_events: u64,
    /// Additional backup bytes required by this attempt.
    ///
    /// This is zero when an existing backup passed validation and was reused.
    pub required_backup_bytes: u64,
    /// Available bytes observed on the backup filesystem during preflight.
    pub available_backup_bytes: u64,
    /// Durable backup used as the rollback boundary, when migration ran.
    pub backup_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoreFingerprint {
    event_count: u64,
    max_sequence: u64,
    events_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MigrationSource {
    event_schema: u32,
    snapshot_schema: Option<u32>,
    agent_loop_wait_projection_schema: Option<u32>,
    has_metadata: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetadataCoordinates {
    event_schema: u32,
    snapshot_schema: u32,
    agent_loop_wait_projection_schema: Option<u32>,
}

impl SqliteEventStore {
    /// Migrates a legacy SQLite State store after creating or validating a
    /// complete backup at `backup_path`.
    ///
    /// The caller must stop every old and new writer before invoking this
    /// operation. Historical event JSON and schema labels are never rewritten.
    pub async fn migrate(
        path: impl AsRef<Path>,
        backup_path: impl AsRef<Path>,
    ) -> Result<StateMigrationReport, HarnessError> {
        let path = path.as_ref().to_owned();
        let backup_path = backup_path.as_ref().to_owned();
        task::spawn_blocking(move || migrate_sync(&path, &backup_path, MigrationStop::None))
            .await
            .map_err(|error| {
                HarnessError::State(format!("SQLite migration task failed: {error}"))
            })?
    }
}

pub(super) fn validate_or_bootstrap_store(connection: &Connection) -> Result<(), HarnessError> {
    if !table_exists(connection, "events")? {
        for table in [
            "streams",
            "stream_recovery",
            "state_snapshots",
            AGENT_LOOP_WAIT_PROJECTION_TABLE,
            STATE_METADATA_TABLE,
        ] {
            if table_exists(connection, table)? {
                return Err(HarnessError::State(
                    "SQLite State store is partial: auxiliary tables exist without events"
                        .to_owned(),
                ));
            }
        }
        return Ok(());
    }
    validate_required_state_tables(connection)?;
    let fingerprint = store_fingerprint(connection)?;
    if !table_exists(connection, STATE_METADATA_TABLE)? {
        if fingerprint.event_count == 0 && legacy_auxiliary_state_is_empty(connection)? {
            return Ok(());
        }
        return Err(HarnessError::State(
            "SQLite State schema migration required; run `yh state-migrate <database> <backup>` before opening this store"
                .to_owned(),
        ));
    }
    match read_metadata(connection)? {
        Some(MetadataCoordinates {
            event_schema,
            snapshot_schema,
            agent_loop_wait_projection_schema: Some(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION),
        }) if event_schema == STATE_EVENT_SCHEMA_VERSION
            && snapshot_schema == STATE_SNAPSHOT_SCHEMA_VERSION =>
        {
            validate_event_schema_set(connection, STATE_EVENT_SCHEMA_VERSION)?;
            validate_auxiliary_state(connection)?;
            validate_stream_name_projection(connection)?;
            validate_stream_tenant_projection(connection)?;
            validate_agent_loop_wait_projection(connection)?;
        }
        Some(MetadataCoordinates {
            event_schema,
            snapshot_schema,
            agent_loop_wait_projection_schema: None,
        }) if ((FIRST_METADATA_STATE_EVENT_SCHEMA_VERSION..STATE_EVENT_SCHEMA_VERSION)
            .contains(&event_schema)
            && legacy_snapshot_schema(event_schema) == Some(snapshot_schema))
            || (event_schema == STATE_EVENT_SCHEMA_VERSION
                && snapshot_schema == STATE_SNAPSHOT_SCHEMA_VERSION) =>
        {
            return Err(migration_required());
        }
        Some(metadata) => {
            return Err(HarnessError::State(format!(
                "unsupported SQLite State metadata event_schema={}, snapshot_schema={}, agent_loop_wait_projection_schema={:?}",
                metadata.event_schema,
                metadata.snapshot_schema,
                metadata.agent_loop_wait_projection_schema
            )));
        }
        None => {
            return Err(HarnessError::State(
                "SQLite State metadata table is empty".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(super) fn ensure_stream_name_column_for_bootstrap(
    connection: &Connection,
) -> Result<(), HarnessError> {
    if column_exists(connection, "streams", "name")? {
        return Ok(());
    }
    if !legacy_auxiliary_state_is_empty(connection)? {
        return Err(migration_required());
    }
    connection
        .execute("ALTER TABLE streams ADD COLUMN name TEXT", [])
        .map_err(|error| HarnessError::State(error.to_string()))?;
    Ok(())
}

pub(super) fn ensure_stream_tenant_column_for_bootstrap(
    connection: &Connection,
) -> Result<(), HarnessError> {
    if column_exists(connection, "streams", "tenant_id")? {
        return Ok(());
    }
    if !legacy_auxiliary_state_is_empty(connection)? {
        return Err(migration_required());
    }
    connection
        .execute("ALTER TABLE streams ADD COLUMN tenant_id TEXT", [])
        .map_err(|error| HarnessError::State(error.to_string()))?;
    Ok(())
}

pub(super) fn metadata_schema_sql() -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {STATE_METADATA_TABLE} (
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL CHECK(value > 0)
        );
        INSERT OR IGNORE INTO {STATE_METADATA_TABLE} (key, value)
            VALUES ('event_schema', {STATE_EVENT_SCHEMA_VERSION});
        INSERT OR IGNORE INTO {STATE_METADATA_TABLE} (key, value)
            VALUES ('snapshot_schema', {STATE_SNAPSHOT_SCHEMA_VERSION});
        INSERT OR IGNORE INTO {STATE_METADATA_TABLE} (key, value)
            VALUES (
                'agent_loop_wait_projection_schema',
                {AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION}
            );
        "
    )
}

fn agent_loop_wait_projection_schema_sql() -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {AGENT_LOOP_WAIT_PROJECTION_TABLE} (
            thread_id                  TEXT PRIMARY KEY,
            tenant_key                 TEXT NOT NULL,
            turn_id                    TEXT NOT NULL,
            wait_id                    TEXT NOT NULL,
            revision                   INTEGER NOT NULL CHECK(revision > 0),
            phase                      INTEGER NOT NULL CHECK(phase IN (1, 2, 3)),
            due_at_ms                  INTEGER CHECK(due_at_ms > 0),
            approval_id                TEXT NOT NULL,
            envelope_sha256            TEXT NOT NULL CHECK(length(envelope_sha256) = 64),
            wait_started_event_id       TEXT NOT NULL,
            current_transition_event_id TEXT NOT NULL,
            resume_command_id           TEXT,
            FOREIGN KEY(thread_id) REFERENCES streams(thread_id),
            FOREIGN KEY(wait_started_event_id) REFERENCES events(event_id),
            FOREIGN KEY(current_transition_event_id) REFERENCES events(event_id)
        );
        CREATE INDEX IF NOT EXISTS {AGENT_LOOP_WAIT_DUE_INDEX}
            ON {AGENT_LOOP_WAIT_PROJECTION_TABLE}(
                tenant_key, due_at_ms, thread_id, turn_id, wait_id
            )
            WHERE due_at_ms IS NOT NULL;
        "
    )
}

fn backfill_agent_loop_wait_projection(transaction: &Transaction<'_>) -> Result<(), HarnessError> {
    let rows = projected_agent_loop_waits(transaction)?;
    let mut insert = transaction
        .prepare(&format!(
            "INSERT INTO {AGENT_LOOP_WAIT_PROJECTION_TABLE}
                (thread_id, tenant_key, turn_id, wait_id, revision, phase,
                 due_at_ms, approval_id, envelope_sha256,
                 wait_started_event_id, current_transition_event_id,
                 resume_command_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
        ))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for row in rows.values() {
        row.validate()?;
        insert
            .execute(params![
                row.thread_id.as_str(),
                row.tenant_id.as_deref().unwrap_or(""),
                row.turn_id.as_str(),
                row.wait_id.as_str(),
                to_i64(row.revision, "Agent Loop wait revision")?,
                row.phase.as_sql(),
                row.due_at_ms
                    .map(|value| to_i64(value, "Agent Loop due time"))
                    .transpose()?,
                row.approval_id.as_str(),
                row.envelope_sha256,
                row.wait_started_event_id.as_str(),
                row.current_transition_event_id.as_str(),
                row.resume_command_id
                    .as_ref()
                    .map(AgentLoopResumeCommandId::as_str),
            ])
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    Ok(())
}

/// Rebuilds only the Agent Loop lifecycle subset, never complete Threads.
fn projected_agent_loop_waits(
    connection: &Connection,
) -> Result<BTreeMap<ThreadId, AgentLoopWaitProjection>, HarnessError> {
    let mut statement = connection
        .prepare(
            "SELECT sequence,
                    length(CAST(event_id AS BLOB)), event_id,
                    length(CAST(thread_id AS BLOB)), thread_id,
                    recorded_at_ms, schema_version,
                    length(CAST(event_json AS BLOB)), event_json
             FROM events
             WHERE schema_version >= 16
               AND json_extract(event_json, '$.type') IN (
                   'thread_forked', 'thread_imported',
                   'wait_started', 'accept_resume', 'claim_ready',
                   'wait_closed', 'deny_wait', 'turn_finished', 'turn_completed'
               )
             ORDER BY sequence",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                bounded_text(row, 1, 2, 256, "State event identity")?,
                bounded_text(row, 3, 4, 256, "State thread identity")?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                bounded_text(row, 7, 8, MAX_STATE_EVENT_BYTES, "State event")?,
            ))
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let mut projected = BTreeMap::new();
    let mut materialized_streams = BTreeSet::new();
    let mut rebound_wait_turns = BTreeMap::new();
    for row in rows {
        let (sequence, event_id, thread_id, recorded_at_ms, schema_version, event_json) =
            row.map_err(|error| HarnessError::State(error.to_string()))?;
        let stored = decode_row(
            EventId::from_string(event_id),
            (
                sequence,
                thread_id,
                recorded_at_ms,
                schema_version,
                event_json,
            ),
        )?;
        if matches!(
            &stored.event,
            crate::StateEvent::ThreadForked { .. } | crate::StateEvent::ThreadImported { .. }
        ) {
            validate_materialized_stream_provenance(connection, &stored)?;
            if projected.contains_key(&stored.thread_id)
                || !materialized_streams.insert(stored.thread_id.clone())
            {
                return Err(HarnessError::State(
                    "materialized Thread provenance is duplicated or follows a live wait"
                        .to_owned(),
                ));
            }
            continue;
        }

        let rebound_turn = match &stored.event {
            crate::StateEvent::WaitStarted {
                turn_id,
                transition:
                    crate::Item {
                        kind: crate::ItemKind::AgentLoopWaitStarted { envelope },
                        ..
                    },
                ..
            } if envelope.thread_id != stored.thread_id => {
                if !materialized_streams.contains(&stored.thread_id) {
                    return Err(HarnessError::State(
                        "Agent Loop wait source Thread differs without materialization provenance"
                            .to_owned(),
                    ));
                }
                Some((
                    (stored.thread_id.clone(), turn_id.clone()),
                    envelope.thread_id.clone(),
                ))
            }
            _ => None,
        };
        if let Some((coordinate, source_thread_id)) = &rebound_turn {
            // One terminal Turn closes every sequential wait in that Turn.
            // Keeping Turn coordinates therefore covers multiple waits in one
            // Turn as well as multiple independent Turns without accepting an
            // unrelated terminal event from the same materialized stream.
            if rebound_wait_turns
                .insert(coordinate.clone(), source_thread_id.clone())
                .is_some_and(|previous| previous != *source_thread_id)
            {
                return Err(HarnessError::State(
                    "materialized Agent Loop waits in one Turn have differing source Threads"
                        .to_owned(),
                ));
            }
        }

        let source_thread_rebind = lifecycle_turn_id(&stored.event).and_then(|turn_id| {
            rebound_wait_turns.get(&(stored.thread_id.clone(), turn_id.clone()))
        });

        let change = projection_change_for_materialized_replay(
            &stored,
            projected.get(&stored.thread_id),
            source_thread_rebind,
        )?;
        match change {
            AgentLoopWaitProjectionChange::Unchanged => {}
            AgentLoopWaitProjectionChange::Upsert(next) => {
                projected.insert(next.thread_id.clone(), next);
            }
            AgentLoopWaitProjectionChange::Delete(previous) => {
                projected.remove(&previous.thread_id);
            }
        }
        if let Some(turn_id) = terminal_turn_id(&stored.event) {
            rebound_wait_turns.remove(&(stored.thread_id.clone(), turn_id.clone()));
        }
    }
    if !rebound_wait_turns.is_empty() {
        return Err(HarnessError::State(
            "materialized Agent Loop wait history does not end at a terminal Turn".to_owned(),
        ));
    }
    Ok(projected)
}

fn validate_materialized_stream_provenance(
    connection: &Connection,
    provenance: &crate::StoredEvent,
) -> Result<(), HarnessError> {
    let provenance_count: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM events
             WHERE thread_id = ?1
               AND json_extract(event_json, '$.type') IN (
                   'thread_forked', 'thread_imported'
               )",
            [provenance.thread_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    if provenance_count != 1 {
        return Err(HarnessError::State(
            "materialized Thread must contain exactly one provenance event".to_owned(),
        ));
    }

    let mut statement = connection
        .prepare(
            "SELECT sequence,
                    CASE json_extract(event_json, '$.type')
                        WHEN 'thread_created' THEN 1 ELSE 0
                    END,
                    CASE WHEN json_extract(event_json, '$.type') IN (
                        'thread_forked', 'thread_imported'
                    ) THEN 1 ELSE 0 END
             FROM events
             WHERE thread_id = ?1
             ORDER BY sequence
             LIMIT 2",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let mut rows = statement
        .query([provenance.thread_id.as_str()])
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let first = rows
        .next()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .map(|row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .transpose()
        .map_err(|error: rusqlite::Error| HarnessError::State(error.to_string()))?;
    let second = rows
        .next()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .map(|row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .transpose()
        .map_err(|error: rusqlite::Error| HarnessError::State(error.to_string()))?;
    let provenance_sequence = to_i64(provenance.sequence, "State sequence")?;
    if !matches!(first, Some((_, 1, 0)))
        || !matches!(second, Some((sequence, 0, 1)) if sequence == provenance_sequence)
    {
        return Err(HarnessError::State(
            "materialized Thread provenance must be the second event after ThreadCreated"
                .to_owned(),
        ));
    }
    Ok(())
}

fn terminal_turn_id(event: &crate::StateEvent) -> Option<&TurnId> {
    match event {
        crate::StateEvent::WaitClosed { turn_id, .. }
        | crate::StateEvent::DenyWait { turn_id, .. }
        | crate::StateEvent::TurnFinished { turn_id, .. }
        | crate::StateEvent::TurnCompleted { turn_id, .. } => Some(turn_id),
        _ => None,
    }
}

fn lifecycle_turn_id(event: &crate::StateEvent) -> Option<&TurnId> {
    match event {
        crate::StateEvent::WaitStarted { turn_id, .. }
        | crate::StateEvent::AcceptResume { turn_id, .. }
        | crate::StateEvent::ClaimReady { turn_id, .. }
        | crate::StateEvent::WaitClosed { turn_id, .. }
        | crate::StateEvent::DenyWait { turn_id, .. }
        | crate::StateEvent::TurnFinished { turn_id, .. }
        | crate::StateEvent::TurnCompleted { turn_id, .. } => Some(turn_id),
        _ => None,
    }
}

fn migrate_sync(
    path: &Path,
    backup_path: &Path,
    stop: MigrationStop,
) -> Result<StateMigrationReport, HarnessError> {
    validate_migration_paths(path, backup_path)?;
    let mut connection =
        Connection::open(path).map_err(|error| HarnessError::State(error.to_string()))?;
    configure_migration_connection(&connection)?;
    if !table_exists(&connection, "events")? {
        return Err(HarnessError::State(
            "SQLite State migration source has no events table".to_owned(),
        ));
    }
    validate_required_state_tables(&connection)?;
    validate_auxiliary_state(&connection)?;
    let fingerprint = store_fingerprint(&connection)?;
    let source = migration_source(&connection)?;
    if source.event_schema == STATE_EVENT_SCHEMA_VERSION
        && source.snapshot_schema == Some(STATE_SNAPSHOT_SCHEMA_VERSION)
        && source.agent_loop_wait_projection_schema
            == Some(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION)
    {
        validate_event_schema_set(&connection, STATE_EVENT_SCHEMA_VERSION)?;
        validate_stream_name_projection(&connection)?;
        validate_stream_tenant_projection(&connection)?;
        validate_agent_loop_wait_projection(&connection)?;
        return Ok(StateMigrationReport {
            status: StateMigrationStatus::AlreadyCurrent,
            from_event_schema: STATE_EVENT_SCHEMA_VERSION,
            to_event_schema: STATE_EVENT_SCHEMA_VERSION,
            from_agent_loop_wait_projection_schema: Some(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION),
            to_agent_loop_wait_projection_schema: AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION,
            historical_events: fingerprint.event_count,
            required_backup_bytes: 0,
            available_backup_bytes: 0,
            backup_path: None,
        });
    }
    validate_event_schema_set(&connection, source.event_schema)?;
    if source.event_schema >= 8 {
        validate_stream_name_projection(&connection)?;
    }
    let (required_backup_bytes, available_backup_bytes) =
        migration_space_preflight(&connection, backup_path)?;
    if stop == MigrationStop::AfterPreflight {
        return Err(injected_stop("after preflight"));
    }

    create_or_validate_backup(path, backup_path, source, fingerprint)?;
    if stop == MigrationStop::AfterBackup {
        return Err(injected_stop("after backup"));
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| HarnessError::State(error.to_string()))?;
    if migration_source(&transaction)? != source {
        return Err(HarnessError::State(
            "SQLite State metadata changed during migration; retry after confirming exclusive access"
                .to_owned(),
        ));
    }
    if store_fingerprint_transaction(&transaction)? != fingerprint {
        return Err(HarnessError::State(
            "SQLite State changed after migration backup; stop all writers and retry with a new backup"
                .to_owned(),
        ));
    }
    validate_auxiliary_state(&transaction)?;
    validate_event_schema_set_transaction(&transaction, source.event_schema)?;
    if source.event_schema < 8 {
        transaction
            .execute("ALTER TABLE streams ADD COLUMN name TEXT", [])
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    if source.event_schema < 12 {
        transaction
            .execute("ALTER TABLE streams ADD COLUMN tenant_id TEXT", [])
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    if source.event_schema != STATE_EVENT_SCHEMA_VERSION
        || source.snapshot_schema != Some(STATE_SNAPSHOT_SCHEMA_VERSION)
    {
        transaction
            .execute("DELETE FROM state_snapshots", [])
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    transaction
        .execute_batch(&agent_loop_wait_projection_schema_sql())
        .map_err(|error| HarnessError::State(error.to_string()))?;
    transaction
        .execute(
            &format!("DELETE FROM {AGENT_LOOP_WAIT_PROJECTION_TABLE}"),
            [],
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    backfill_agent_loop_wait_projection(&transaction)?;
    if source.has_metadata {
        let event_schema_changed = transaction
            .execute(
                &format!(
                    "UPDATE {STATE_METADATA_TABLE}
                     SET value = ?1
                     WHERE key = 'event_schema' AND value = ?2"
                ),
                params![
                    i64::from(STATE_EVENT_SCHEMA_VERSION),
                    i64::from(source.event_schema)
                ],
            )
            .map_err(|error| HarnessError::State(error.to_string()))?;
        if event_schema_changed != 1 {
            return Err(HarnessError::State(
                "SQLite State event schema metadata changed during migration".to_owned(),
            ));
        }
        let snapshot_schema = source.snapshot_schema.ok_or_else(|| {
            HarnessError::State("SQLite State snapshot schema metadata disappeared".to_owned())
        })?;
        let snapshot_schema_changed = transaction
            .execute(
                &format!(
                    "UPDATE {STATE_METADATA_TABLE}
                     SET value = ?1
                     WHERE key = 'snapshot_schema' AND value = ?2"
                ),
                params![
                    i64::from(STATE_SNAPSHOT_SCHEMA_VERSION),
                    i64::from(snapshot_schema)
                ],
            )
            .map_err(|error| HarnessError::State(error.to_string()))?;
        if snapshot_schema_changed != 1 {
            return Err(HarnessError::State(
                "SQLite State snapshot schema metadata changed during migration".to_owned(),
            ));
        }
        let projection_schema_inserted = transaction
            .execute(
                &format!(
                    "INSERT INTO {STATE_METADATA_TABLE} (key, value)
                     VALUES ('agent_loop_wait_projection_schema', ?1)"
                ),
                [i64::from(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION)],
            )
            .map_err(|error| HarnessError::State(error.to_string()))?;
        if projection_schema_inserted != 1 {
            return Err(HarnessError::State(
                "SQLite State wait-projection schema metadata was not inserted".to_owned(),
            ));
        }
    } else {
        transaction
            .execute_batch(&metadata_schema_sql())
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    validate_stream_name_projection(&transaction)?;
    validate_stream_tenant_projection(&transaction)?;
    validate_agent_loop_wait_projection(&transaction)?;
    if stop == MigrationStop::BeforeCommit {
        return Err(injected_stop("before commit"));
    }
    transaction
        .commit()
        .map_err(|error| HarnessError::State(error.to_string()))?;

    Ok(StateMigrationReport {
        status: StateMigrationStatus::Migrated,
        from_event_schema: source.event_schema,
        to_event_schema: STATE_EVENT_SCHEMA_VERSION,
        from_agent_loop_wait_projection_schema: source.agent_loop_wait_projection_schema,
        to_agent_loop_wait_projection_schema: AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION,
        historical_events: fingerprint.event_count,
        required_backup_bytes,
        available_backup_bytes,
        backup_path: Some(backup_path.to_owned()),
    })
}

fn validate_migration_paths(path: &Path, backup_path: &Path) -> Result<(), HarnessError> {
    if !path.is_file() {
        return Err(HarnessError::State(
            "SQLite State migration source must be an existing file".to_owned(),
        ));
    }
    if path == backup_path {
        return Err(HarnessError::State(
            "SQLite State migration backup must differ from the source".to_owned(),
        ));
    }
    if backup_path.to_str().is_none() {
        return Err(HarnessError::State(
            "SQLite State migration backup path must be valid UTF-8".to_owned(),
        ));
    }
    let parent = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if parent.is_some_and(|parent| !parent.is_dir()) {
        return Err(HarnessError::State(
            "SQLite State migration backup parent must already exist".to_owned(),
        ));
    }
    Ok(())
}

fn configure_migration_connection(connection: &Connection) -> Result<(), HarnessError> {
    configure_sqlite_busy_timeout(connection)?;
    configure_sqlite_session(connection)
}

fn migration_space_preflight(
    connection: &Connection,
    backup_path: &Path,
) -> Result<(u64, u64), HarnessError> {
    let page_count = pragma_u64(connection, "page_count")?;
    let freelist_count = pragma_u64(connection, "freelist_count")?;
    let page_size = pragma_u64(connection, "page_size")?;
    let used_pages = page_count.saturating_sub(freelist_count);
    let required_backup_bytes = if backup_path.exists() {
        0
    } else {
        used_pages
            .checked_mul(page_size)
            .and_then(|bytes| bytes.checked_add(MIN_MIGRATION_WORKING_BYTES))
            .ok_or_else(|| HarnessError::State("migration disk requirement overflow".to_owned()))?
    };
    let probe = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let available_backup_bytes =
        fs2::available_space(probe).map_err(|error| HarnessError::State(error.to_string()))?;
    if available_backup_bytes < required_backup_bytes {
        return Err(HarnessError::State(format!(
            "SQLite State migration requires {required_backup_bytes} available backup bytes, found {available_backup_bytes}"
        )));
    }
    let source_probe = connection
        .path()
        .map(Path::new)
        .and_then(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let source_available = fs2::available_space(source_probe)
        .map_err(|error| HarnessError::State(error.to_string()))?;
    if source_available < MIN_MIGRATION_WORKING_BYTES {
        return Err(HarnessError::State(format!(
            "SQLite State migration requires {MIN_MIGRATION_WORKING_BYTES} working bytes on the source filesystem, found {source_available}"
        )));
    }
    Ok((required_backup_bytes, available_backup_bytes))
}

fn create_or_validate_backup(
    source_path: &Path,
    backup_path: &Path,
    migration_source: MigrationSource,
    fingerprint: StoreFingerprint,
) -> Result<(), HarnessError> {
    if backup_path.exists() {
        return validate_backup(backup_path, migration_source, fingerprint);
    }
    let partial_path = partial_backup_path(backup_path);
    let source =
        Connection::open(source_path).map_err(|error| HarnessError::State(error.to_string()))?;
    configure_migration_connection(&source)?;
    let partial_path_text = partial_path.to_str().ok_or_else(|| {
        HarnessError::State("SQLite State migration partial path must be valid UTF-8".to_owned())
    })?;
    source
        .execute("VACUUM INTO ?1", [partial_path_text])
        .map_err(|error| HarnessError::State(format!("cannot create State backup: {error}")))?;
    drop(source);

    let backup =
        Connection::open(&partial_path).map_err(|error| HarnessError::State(error.to_string()))?;
    backup
        .execute_batch(
            "
            PRAGMA journal_mode = DELETE;
            PRAGMA synchronous = FULL;
            ",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    backup
        .execute_batch(&format!(
            "
            CREATE TABLE {BACKUP_MANIFEST_TABLE} (
                id                   INTEGER PRIMARY KEY CHECK(id = 1),
                from_event_schema    INTEGER NOT NULL,
                to_event_schema      INTEGER NOT NULL,
                from_snapshot_schema INTEGER NOT NULL CHECK(from_snapshot_schema >= 0),
                to_snapshot_schema   INTEGER NOT NULL,
                from_agent_loop_wait_projection_schema
                                     INTEGER NOT NULL CHECK(from_agent_loop_wait_projection_schema >= 0),
                to_agent_loop_wait_projection_schema
                                     INTEGER NOT NULL,
                event_count          INTEGER NOT NULL,
                max_sequence         INTEGER NOT NULL,
                events_sha256        TEXT NOT NULL CHECK(length(events_sha256) = 64)
            );
            "
        ))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    backup
        .execute(
            &format!(
                "INSERT INTO {BACKUP_MANIFEST_TABLE}
                    (id, from_event_schema, to_event_schema, from_snapshot_schema,
                     to_snapshot_schema, from_agent_loop_wait_projection_schema,
                     to_agent_loop_wait_projection_schema,
                     event_count, max_sequence, events_sha256)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)"
            ),
            params![
                i64::from(migration_source.event_schema),
                i64::from(STATE_EVENT_SCHEMA_VERSION),
                i64::from(migration_source.snapshot_schema.unwrap_or(0)),
                i64::from(STATE_SNAPSHOT_SCHEMA_VERSION),
                i64::from(
                    migration_source
                        .agent_loop_wait_projection_schema
                        .unwrap_or(0)
                ),
                i64::from(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION),
                to_i64(fingerprint.event_count, "event count")?,
                to_i64(fingerprint.max_sequence, "event sequence")?,
                fingerprint_hex(&fingerprint.events_sha256),
            ],
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    drop(backup);
    File::open(&partial_path)
        .and_then(|file| file.sync_all())
        .map_err(|error| HarnessError::State(error.to_string()))?;
    fs::hard_link(&partial_path, backup_path).map_err(|error| {
        HarnessError::State(format!(
            "cannot publish State backup without overwriting an existing path: {error}"
        ))
    })?;
    sync_parent_directory(backup_path)?;
    fs::remove_file(&partial_path).map_err(|error| HarnessError::State(error.to_string()))?;
    sync_parent_directory(backup_path)?;
    validate_backup(backup_path, migration_source, fingerprint)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), HarnessError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| HarnessError::State(error.to_string()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), HarnessError> {
    Ok(())
}

fn validate_backup(
    backup_path: &Path,
    migration_source: MigrationSource,
    expected: StoreFingerprint,
) -> Result<(), HarnessError> {
    if !backup_path.is_file() {
        return Err(HarnessError::State(
            "SQLite State migration backup is not a regular file".to_owned(),
        ));
    }
    let backup = Connection::open_with_flags(backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let integrity: String = backup
        .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    if integrity != "ok" {
        return Err(HarnessError::State(
            "SQLite State migration backup failed integrity_check".to_owned(),
        ));
    }
    validate_auxiliary_state(&backup)?;
    let manifest = backup
        .query_row(
            &format!(
                "SELECT from_event_schema, to_event_schema, from_snapshot_schema,
                        to_snapshot_schema,
                        from_agent_loop_wait_projection_schema,
                        to_agent_loop_wait_projection_schema,
                        event_count, max_sequence,
                        length(CAST(events_sha256 AS BLOB)), events_sha256
                 FROM {BACKUP_MANIFEST_TABLE} WHERE id = 1"
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    bounded_text(row, 8, 9, 64, "State backup event fingerprint")?,
                ))
            },
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .ok_or_else(|| {
            HarnessError::State("SQLite State migration backup has no manifest".to_owned())
        })?;
    if manifest.0 != i64::from(migration_source.event_schema)
        || manifest.1 != i64::from(STATE_EVENT_SCHEMA_VERSION)
        || manifest.2 != i64::from(migration_source.snapshot_schema.unwrap_or(0))
        || manifest.3 != i64::from(STATE_SNAPSHOT_SCHEMA_VERSION)
        || manifest.4
            != i64::from(
                migration_source
                    .agent_loop_wait_projection_schema
                    .unwrap_or(0),
            )
        || manifest.5 != i64::from(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION)
        || to_u64(manifest.6, "backup event count")? != expected.event_count
        || to_u64(manifest.7, "backup max sequence")? != expected.max_sequence
        || manifest.8 != fingerprint_hex(&expected.events_sha256)
        || store_fingerprint(&backup)? != expected
    {
        return Err(HarnessError::State(
            "SQLite State migration backup does not match the source preflight".to_owned(),
        ));
    }
    Ok(())
}

fn read_metadata(connection: &Connection) -> Result<Option<MetadataCoordinates>, HarnessError> {
    if !table_exists(connection, STATE_METADATA_TABLE)? {
        return Ok(None);
    }
    let mut statement = connection
        .prepare(&format!(
            "SELECT length(CAST(key AS BLOB)), key, value
             FROM {STATE_METADATA_TABLE}
             ORDER BY key"
        ))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                bounded_text(row, 0, 1, 64, "State metadata key")?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let mut event_schema = None;
    let mut snapshot_schema = None;
    let mut agent_loop_wait_projection_schema = None;
    let mut entries = 0_usize;
    for row in rows {
        let (key, value) = row.map_err(|error| HarnessError::State(error.to_string()))?;
        entries = entries
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("State metadata count overflow".to_owned()))?;
        let value = u32::try_from(value)
            .map_err(|_| HarnessError::State(format!("invalid SQLite State {key} {value}")))?;
        match key.as_str() {
            "event_schema" => event_schema = Some(value),
            "snapshot_schema" => snapshot_schema = Some(value),
            "agent_loop_wait_projection_schema" => {
                agent_loop_wait_projection_schema = Some(value);
            }
            _ => {
                return Err(HarnessError::State(format!(
                    "unsupported SQLite State metadata key {key}"
                )));
            }
        }
    }
    if !(2..=3).contains(&entries) {
        return Err(HarnessError::State(format!(
            "unsupported SQLite State metadata entry count {entries}; expected 2 or 3"
        )));
    }
    let event_schema = event_schema.ok_or_else(|| {
        HarnessError::State("SQLite State metadata is missing event_schema".to_owned())
    })?;
    let snapshot_schema = snapshot_schema.ok_or_else(|| {
        HarnessError::State("SQLite State metadata is missing snapshot_schema".to_owned())
    })?;
    if entries == 3 && agent_loop_wait_projection_schema.is_none() {
        return Err(HarnessError::State(
            "SQLite State metadata has an unexpected third coordinate".to_owned(),
        ));
    }
    Ok(Some(MetadataCoordinates {
        event_schema,
        snapshot_schema,
        agent_loop_wait_projection_schema,
    }))
}

fn validate_required_state_tables(connection: &Connection) -> Result<(), HarnessError> {
    for table in ["streams", "stream_recovery", "state_snapshots"] {
        if !table_exists(connection, table)? {
            return Err(HarnessError::State(format!(
                "SQLite State store is partial: missing {table} table"
            )));
        }
    }
    Ok(())
}

fn validate_auxiliary_state(connection: &Connection) -> Result<(), HarnessError> {
    let invalid_stream = connection
        .query_row(
            "
            SELECT 1
            FROM events AS event
            LEFT JOIN streams AS stream ON stream.thread_id = event.thread_id
            GROUP BY event.thread_id
            HAVING stream.thread_id IS NULL OR stream.version != COUNT(*)
            UNION ALL
            SELECT 1
            FROM streams AS stream
            LEFT JOIN events AS event ON event.thread_id = stream.thread_id
            WHERE event.thread_id IS NULL
            LIMIT 1
            ",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .is_some();
    if invalid_stream {
        return Err(HarnessError::State(
            "SQLite State stream versions do not match authoritative events".to_owned(),
        ));
    }

    let invalid_recovery = connection
        .query_row(
            &format!(
                "
                SELECT 1
                FROM events AS event
                LEFT JOIN stream_recovery AS recovery
                    ON recovery.thread_id = event.thread_id
                GROUP BY event.thread_id
                HAVING recovery.thread_id IS NULL
                    OR recovery.recovery_bytes !=
                       SUM(length(CAST(event.event_json AS BLOB))
                           + {STATE_EVENT_RECOVERY_OVERHEAD_BYTES})
                UNION ALL
                SELECT 1
                FROM stream_recovery AS recovery
                LEFT JOIN events AS event ON event.thread_id = recovery.thread_id
                WHERE event.thread_id IS NULL
                LIMIT 1
                "
            ),
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .is_some();
    if invalid_recovery {
        return Err(HarnessError::State(
            "SQLite State recovery charges do not match authoritative events".to_owned(),
        ));
    }
    Ok(())
}

fn validate_agent_loop_wait_projection(connection: &Connection) -> Result<(), HarnessError> {
    if !table_exists(connection, AGENT_LOOP_WAIT_PROJECTION_TABLE)? {
        return Err(HarnessError::State(format!(
            "SQLite State store is partial: missing {AGENT_LOOP_WAIT_PROJECTION_TABLE} table"
        )));
    }
    if !index_exists(connection, AGENT_LOOP_WAIT_DUE_INDEX)? {
        return Err(HarnessError::State(format!(
            "SQLite State store is partial: missing {AGENT_LOOP_WAIT_DUE_INDEX} index"
        )));
    }
    let expected = projected_agent_loop_waits(connection)?;
    let actual = stored_agent_loop_wait_projection(connection)?;
    if actual != expected {
        return Err(HarnessError::State(
            "SQLite State Agent Loop wait projection does not match authoritative lifecycle events"
                .to_owned(),
        ));
    }
    Ok(())
}

fn stored_agent_loop_wait_projection(
    connection: &Connection,
) -> Result<BTreeMap<ThreadId, AgentLoopWaitProjection>, HarnessError> {
    let mut statement = connection
        .prepare(&format!(
            "SELECT length(CAST(thread_id AS BLOB)), thread_id,
                    length(CAST(tenant_key AS BLOB)), tenant_key,
                    length(CAST(turn_id AS BLOB)), turn_id,
                    length(CAST(wait_id AS BLOB)), wait_id,
                    revision, phase, due_at_ms,
                    length(CAST(approval_id AS BLOB)), approval_id,
                    length(CAST(envelope_sha256 AS BLOB)), envelope_sha256,
                    length(CAST(wait_started_event_id AS BLOB)), wait_started_event_id,
                    length(CAST(current_transition_event_id AS BLOB)),
                        current_transition_event_id,
                    length(CAST(resume_command_id AS BLOB)), resume_command_id
             FROM {AGENT_LOOP_WAIT_PROJECTION_TABLE}
             ORDER BY thread_id"
        ))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                bounded_text(row, 0, 1, 256, "Agent Loop wait Thread")?,
                bounded_text(row, 2, 3, 128, "Agent Loop wait tenant")?,
                bounded_text(row, 4, 5, 256, "Agent Loop wait Turn")?,
                bounded_text(row, 6, 7, 256, "Agent Loop wait identity")?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, Option<i64>>(10)?,
                bounded_text(row, 11, 12, 256, "Agent Loop Approval identity")?,
                bounded_text(row, 13, 14, 64, "Agent Loop envelope digest")?,
                bounded_text(row, 15, 16, 256, "Agent Loop wait-start event")?,
                bounded_text(row, 17, 18, 256, "Agent Loop transition event")?,
                bounded_optional_text(row, 19, 20, 256, "Agent Loop resume command")?,
            ))
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let mut projections = BTreeMap::new();
    for row in rows {
        let (
            thread_id,
            tenant_key,
            turn_id,
            wait_id,
            revision,
            phase,
            due_at_ms,
            approval_id,
            envelope_sha256,
            wait_started_event_id,
            current_transition_event_id,
            resume_command_id,
        ) = row.map_err(|error| HarnessError::State(error.to_string()))?;
        let projection = AgentLoopWaitProjection {
            phase: AgentLoopDuePhase::from_sql(phase)?,
            tenant_id: (!tenant_key.is_empty()).then_some(tenant_key),
            thread_id: ThreadId::from_string(thread_id),
            turn_id: TurnId::from_string(turn_id),
            wait_id: crate::AgentLoopWaitId::from_string(wait_id),
            revision: to_u64(revision, "Agent Loop wait revision")?,
            due_at_ms: due_at_ms
                .map(|value| to_u64(value, "Agent Loop due time"))
                .transpose()?,
            approval_id: ApprovalId::from_string(approval_id),
            envelope_sha256,
            wait_started_event_id: EventId::from_string(wait_started_event_id),
            current_transition_event_id: EventId::from_string(current_transition_event_id),
            resume_command_id: resume_command_id.map(AgentLoopResumeCommandId::from_string),
        };
        projection.validate()?;
        if projections
            .insert(projection.thread_id.clone(), projection)
            .is_some()
        {
            return Err(HarnessError::State(
                "SQLite State Agent Loop wait projection has duplicate Threads".to_owned(),
            ));
        }
    }
    Ok(projections)
}

fn validate_stream_name_projection(connection: &Connection) -> Result<(), HarnessError> {
    if !column_exists(connection, "streams", "name")? {
        return Err(HarnessError::State(
            "SQLite State store is partial: streams.name is missing".to_owned(),
        ));
    }
    let mut names = connection
        .prepare(
            "SELECT length(CAST(name AS BLOB)), name
             FROM streams
             WHERE name IS NOT NULL",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = names
        .query_map([], |row| {
            bounded_text(row, 0, 1, MAX_THREAD_NAME_BYTES, "Thread name")
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for name in rows {
        let name = name.map_err(|error| HarnessError::State(error.to_string()))?;
        validate_thread_name(Some(&name))?;
    }
    let invalid_name = connection
        .query_row(
            "
                WITH ranked_names AS (
                    SELECT thread_id,
                           json_extract(event_json, '$.name') AS name,
                           ROW_NUMBER() OVER (
                               PARTITION BY thread_id ORDER BY sequence DESC
                           ) AS rank
                    FROM events
                    WHERE schema_version >= 8
                      AND json_extract(event_json, '$.type') = 'thread_named'
                )
                SELECT 1
                FROM streams AS stream
                LEFT JOIN ranked_names AS expected
                  ON expected.thread_id = stream.thread_id
                 AND expected.rank = 1
                WHERE NOT (stream.name IS expected.name)
                LIMIT 1
                ",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .is_some();
    if invalid_name {
        return Err(HarnessError::State(
            "SQLite State Thread names do not match authoritative events".to_owned(),
        ));
    }
    Ok(())
}

fn validate_stream_tenant_projection(connection: &Connection) -> Result<(), HarnessError> {
    if !column_exists(connection, "streams", "tenant_id")? {
        return Err(HarnessError::State(
            "SQLite State store is partial: streams.tenant_id is missing".to_owned(),
        ));
    }
    let mut tenants = connection
        .prepare(
            "SELECT length(CAST(tenant_id AS BLOB)), tenant_id
             FROM streams
             WHERE tenant_id IS NOT NULL",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = tenants
        .query_map([], |row| {
            bounded_text(row, 0, 1, 128, "Thread tenant identity")
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for tenant_id in rows {
        AuthorityContext::validate_tenant(
            &tenant_id.map_err(|error| HarnessError::State(error.to_string()))?,
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    let invalid_tenant = connection
        .query_row(
            "
            WITH creation AS (
                SELECT thread_id, json_extract(event_json, '$.tenant_id') AS tenant_id
                FROM events
                WHERE json_extract(event_json, '$.type') = 'thread_created'
            )
            SELECT 1
            FROM streams AS stream
            LEFT JOIN creation ON creation.thread_id = stream.thread_id
            WHERE creation.thread_id IS NULL
               OR NOT (stream.tenant_id IS creation.tenant_id)
            LIMIT 1
            ",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?
        .is_some();
    if invalid_tenant {
        return Err(HarnessError::State(
            "SQLite State Thread tenants do not match authoritative creation events".to_owned(),
        ));
    }
    Ok(())
}

fn validate_event_schema_set(connection: &Connection, maximum: u32) -> Result<(), HarnessError> {
    let mut statement = connection
        .prepare("SELECT DISTINCT schema_version FROM events ORDER BY schema_version")
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let versions = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for version in versions {
        let version = version.map_err(|error| HarnessError::State(error.to_string()))?;
        let supported =
            (i64::from(FIRST_STATE_EVENT_SCHEMA_VERSION)..=i64::from(maximum)).contains(&version);
        if !supported {
            return Err(HarnessError::State(format!(
                "unsupported stored State event schema version {version}"
            )));
        }
    }
    Ok(())
}

fn validate_event_schema_set_transaction(
    transaction: &Transaction<'_>,
    maximum: u32,
) -> Result<(), HarnessError> {
    let mut statement = transaction
        .prepare("SELECT DISTINCT schema_version FROM events ORDER BY schema_version")
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let versions = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for version in versions {
        let version = version.map_err(|error| HarnessError::State(error.to_string()))?;
        if !(i64::from(FIRST_STATE_EVENT_SCHEMA_VERSION)..=i64::from(maximum)).contains(&version) {
            return Err(HarnessError::State(format!(
                "legacy migration found State event schema version {version}"
            )));
        }
    }
    Ok(())
}

fn migration_source(connection: &Connection) -> Result<MigrationSource, HarnessError> {
    let metadata = read_metadata(connection)?;
    let projection_table = table_exists(connection, AGENT_LOOP_WAIT_PROJECTION_TABLE)?;
    let projection_index = index_exists(connection, AGENT_LOOP_WAIT_DUE_INDEX)?;
    let projection_coordinate = metadata
        .as_ref()
        .and_then(|metadata| metadata.agent_loop_wait_projection_schema);
    match (projection_coordinate, projection_table, projection_index) {
        (None, false, false) | (Some(_), true, true) => {}
        (None, _, _) => {
            return Err(HarnessError::State(
                "SQLite State wait projection exists without its metadata coordinate".to_owned(),
            ));
        }
        (Some(_), _, _) => {
            return Err(HarnessError::State(
                "SQLite State wait-projection metadata has a partial table or index".to_owned(),
            ));
        }
    }
    match metadata {
        None => Ok(MigrationSource {
            event_schema: FIRST_STATE_EVENT_SCHEMA_VERSION,
            snapshot_schema: None,
            agent_loop_wait_projection_schema: None,
            has_metadata: false,
        }),
        Some(MetadataCoordinates {
            event_schema,
            snapshot_schema,
            agent_loop_wait_projection_schema,
        }) if event_schema == STATE_EVENT_SCHEMA_VERSION
            && snapshot_schema == STATE_SNAPSHOT_SCHEMA_VERSION
            && matches!(
                agent_loop_wait_projection_schema,
                None | Some(AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION)
            ) =>
        {
            Ok(MigrationSource {
                event_schema,
                snapshot_schema: Some(snapshot_schema),
                agent_loop_wait_projection_schema,
                has_metadata: true,
            })
        }
        Some(MetadataCoordinates {
            event_schema,
            snapshot_schema,
            agent_loop_wait_projection_schema: None,
        }) if (FIRST_METADATA_STATE_EVENT_SCHEMA_VERSION..STATE_EVENT_SCHEMA_VERSION)
            .contains(&event_schema)
            && legacy_snapshot_schema(event_schema) == Some(snapshot_schema) =>
        {
            Ok(MigrationSource {
                event_schema,
                snapshot_schema: Some(snapshot_schema),
                agent_loop_wait_projection_schema: None,
                has_metadata: true,
            })
        }
        Some(metadata) => Err(HarnessError::State(format!(
            "unsupported SQLite State migration source event_schema={}, snapshot_schema={}, agent_loop_wait_projection_schema={:?}",
            metadata.event_schema,
            metadata.snapshot_schema,
            metadata.agent_loop_wait_projection_schema
        ))),
    }
}

fn legacy_snapshot_schema(event_schema: u32) -> Option<u32> {
    match event_schema {
        2 | 3 => Some(3),
        4 => Some(4),
        5 => Some(5),
        6 => Some(6),
        7 => Some(7),
        8 => Some(8),
        9 => Some(9),
        10 => Some(10),
        11 => Some(11),
        12 => Some(12),
        13 => Some(13),
        14 => Some(14),
        15 => Some(15),
        _ => None,
    }
}

fn migration_required() -> HarnessError {
    HarnessError::State(
        "SQLite State schema migration required; run `yh state-migrate <database> <backup>` before opening this store"
            .to_owned(),
    )
}

fn legacy_auxiliary_state_is_empty(connection: &Connection) -> Result<bool, HarnessError> {
    for table in [
        "streams",
        "stream_recovery",
        "state_snapshots",
        AGENT_LOOP_WAIT_PROJECTION_TABLE,
    ] {
        if table_exists(connection, table)? {
            let count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .map_err(|error| HarnessError::State(error.to_string()))?;
            if count != 0 {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn column_exists(connection: &Connection, table: &str, column: &str) -> Result<bool, HarnessError> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    for candidate in columns {
        if candidate.map_err(|error| HarnessError::State(error.to_string()))? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn store_fingerprint(connection: &Connection) -> Result<StoreFingerprint, HarnessError> {
    let mut statement = connection
        .prepare(
            "
            SELECT sequence,
                   length(CAST(event_id AS BLOB)), event_id,
                   length(CAST(thread_id AS BLOB)), thread_id,
                   recorded_at_ms, schema_version,
                   length(CAST(event_json AS BLOB)), event_json
            FROM events
            ORDER BY sequence
            ",
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                bounded_text(row, 1, 2, 256, "State event identity")?,
                bounded_text(row, 3, 4, 256, "State thread identity")?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                bounded_text(row, 7, 8, MAX_STATE_EVENT_BYTES, "State event")?,
            ))
        })
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let mut hasher = Sha256::new();
    let mut event_count = 0_u64;
    let mut max_sequence = 0_u64;
    for row in rows {
        let (sequence, event_id, thread_id, recorded_at_ms, schema_version, event_json) =
            row.map_err(|error| HarnessError::State(error.to_string()))?;
        let sequence = to_u64(sequence, "event sequence")?;
        event_count = event_count
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("event count overflow".to_owned()))?;
        max_sequence = sequence;
        hasher.update(sequence.to_le_bytes());
        update_fingerprint_text(&mut hasher, &event_id)?;
        update_fingerprint_text(&mut hasher, &thread_id)?;
        hasher.update(recorded_at_ms.to_le_bytes());
        hasher.update(schema_version.to_le_bytes());
        update_fingerprint_text(&mut hasher, &event_json)?;
    }
    hasher.update(event_count.to_le_bytes());
    Ok(StoreFingerprint {
        event_count,
        max_sequence,
        events_sha256: hasher.finalize().into(),
    })
}

fn store_fingerprint_transaction(
    transaction: &Transaction<'_>,
) -> Result<StoreFingerprint, HarnessError> {
    store_fingerprint(transaction)
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(|error| HarnessError::State(error.to_string()))
}

fn index_exists(connection: &Connection, index: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'index' AND name = ?1",
            [index],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(|error| HarnessError::State(error.to_string()))
}

fn pragma_u64(connection: &Connection, pragma: &str) -> Result<u64, HarnessError> {
    let value: i64 = connection
        .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    to_u64(value, pragma)
}

fn to_u64(value: i64, kind: &str) -> Result<u64, HarnessError> {
    u64::try_from(value).map_err(|_| HarnessError::State(format!("negative {kind}")))
}

fn to_i64(value: u64, kind: &str) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| HarnessError::State(format!("{kind} exceeds SQLite INTEGER")))
}

fn update_fingerprint_text(hasher: &mut Sha256, value: &str) -> Result<(), HarnessError> {
    let length = u64::try_from(value.len())
        .map_err(|_| HarnessError::State("State fingerprint length exceeds u64".to_owned()))?;
    hasher.update(length.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn fingerprint_hex(value: &[u8; 32]) -> String {
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn partial_backup_path(backup_path: &Path) -> PathBuf {
    let mut value: OsString = backup_path.as_os_str().to_owned();
    value.push(format!(".partial-{}", EventId::generate()));
    PathBuf::from(value)
}

fn injected_stop(phase: &str) -> HarnessError {
    HarnessError::State(format!("injected migration stop {phase}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationStop {
    None,
    AfterPreflight,
    AfterBackup,
    BeforeCommit,
}

#[cfg(test)]
pub(super) fn migrate_with_stop(
    path: &Path,
    backup_path: &Path,
    stop: &'static str,
) -> Result<StateMigrationReport, HarnessError> {
    let stop = match stop {
        "after_preflight" => MigrationStop::AfterPreflight,
        "after_backup" => MigrationStop::AfterBackup,
        "before_commit" => MigrationStop::BeforeCommit,
        _ => MigrationStop::None,
    };
    migrate_sync(path, backup_path, stop)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };

    use rusqlite::{Connection, params};
    use serde_json::json;

    use super::{
        BACKUP_MANIFEST_TABLE, STATE_METADATA_TABLE, StateMigrationStatus, migrate_with_stop,
    };
    use crate::{
        ActorIdentity, AgentLoopCloseCommandId, AgentLoopResumeCommandId, AgentLoopWaitId,
        ApprovalDecision, ApprovalId, ApprovalRecord, ApprovalRecordStatus, ApprovalRequest,
        CapabilityOrigin, Checkpoint, CheckpointId, CompletionAssurance, CompletionGeneration,
        EventId, EventStore, Item, ItemKind, PolicyDecision, RiskLevel, STATE_EVENT_SCHEMA_VERSION,
        STATE_SNAPSHOT_SCHEMA_VERSION, STATE_TERMINAL_RECOVERY_BYTE_RESERVE,
        STATE_THREAD_RECOVERY_BYTE_LIMIT, SqliteEventStore, StateEngine, StateEvent, ThreadId,
        ToolAuthorization, ToolDescriptor, Turn, TurnStatus, TurnStopReason,
        completion_model_request_sha256, completion_model_route_sha256,
        completion_runtime_governance_sha256, completion_tool_view_sha256,
        completion_verifier_manifest_sha256,
    };

    const STATE_V1_FIXTURE: &str = include_str!("../../tests/fixtures/state-v1.sql");

    async fn start_projection_test_wait(
        state: &StateEngine,
        wait_id: AgentLoopWaitId,
    ) -> (Turn, ApprovalRequest) {
        let thread = state.create_thread().await.expect("create Thread");
        let turn = state.start_turn(&thread.id).await.expect("start Turn");
        let descriptor = ToolDescriptor {
            name: "write_record".to_owned(),
            description: "Writes one bounded migration record".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"]
            }),
        };
        let call_id = "migration-approval-call".to_owned();
        let input = json!({"value": 7});
        state
            .append_item(
                &turn,
                Item::new(ItemKind::ToolCall {
                    model_id: Some("test/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: call_id.clone(),
                    name: descriptor.name.clone(),
                    input: input.clone(),
                    batch: None,
                }),
            )
            .await
            .expect("append Tool call");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: call_id.clone(),
                    tool_origin: Some(CapabilityOrigin::BuiltIn),
                    decision: PolicyDecision::Ask {
                        reason: "operator confirmation required".to_owned(),
                        risk: RiskLevel::High,
                    },
                }),
            )
            .await
            .expect("append Ask decision");
        let request = ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: ActorIdentity::LocalProcess,
            authorization: ToolAuthorization {
                thread_id: thread.id,
                turn_id: turn.id.clone(),
                call_id,
                descriptor,
                origin: CapabilityOrigin::BuiltIn,
                input,
            },
            reason: "operator confirmation required".to_owned(),
            risk: RiskLevel::High,
        };
        let model_request_sha256 = completion_model_request_sha256(&json!({
            "turn": turn.id.as_str(),
            "approval": request.id.as_str()
        }))
        .expect("Model request digest");
        let generation = CompletionGeneration::new(
            model_request_sha256,
            completion_model_route_sha256(&["test/model"]).expect("Model route digest"),
            completion_tool_view_sha256(&Vec::<String>::new()).expect("Tool view digest"),
            completion_verifier_manifest_sha256(&[]).expect("Verifier manifest digest"),
            completion_runtime_governance_sha256(&json!({"max_steps": 16}))
                .expect("Runtime governance digest"),
            None,
            CompletionAssurance::RuntimeMeasured,
        )
        .expect("completion generation");
        let frozen_request = request.clone();
        state
            .start_approval_wait(
                &turn,
                wait_id,
                request,
                generation,
                Some(Duration::from_secs(60)),
                Some(10_000),
            )
            .await
            .expect("start durable wait");
        (turn, frozen_request)
    }

    #[tokio::test]
    async fn migration_backs_up_v1_and_preserves_immutable_history() {
        let source = fixture_path("source");
        let backup = fixture_path("backup");
        create_v1_fixture(&source);
        assert_eq!(journal_mode(&source), "delete");

        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("legacy store must require migration");
        assert!(error.to_string().contains("state-migrate"));
        assert_eq!(
            journal_mode(&source),
            "delete",
            "rejected ordinary open must not reconfigure a legacy store"
        );
        let report = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect("migrate");
        assert_eq!(report.status, StateMigrationStatus::Migrated);
        assert_eq!(report.from_event_schema, 1);
        assert_eq!(report.to_event_schema, STATE_EVENT_SCHEMA_VERSION);
        assert_eq!(report.historical_events, 1);
        assert_eq!(report.backup_path.as_deref(), Some(backup.as_path()));
        assert!(report.available_backup_bytes >= report.required_backup_bytes);

        let store = Arc::new(
            SqliteEventStore::open(&source)
                .await
                .expect("open migrated store"),
        );
        let state = StateEngine::new(store.clone());
        let legacy = state
            .events(&ThreadId::from_static("thread-v1"))
            .await
            .expect("read v1 event");
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].schema_version, 1);
        let current = state.create_thread().await.expect("write current event");
        let current_events = state.events(&current.id).await.expect("read current event");
        assert_eq!(current_events[0].schema_version, STATE_EVENT_SCHEMA_VERSION);

        let backup_connection = Connection::open(&backup).expect("open backup");
        assert!(!table_exists(&backup_connection, STATE_METADATA_TABLE));
        assert!(table_exists(&backup_connection, BACKUP_MANIFEST_TABLE));
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn migration_advances_metadata_schemas_without_rewriting_history() {
        for legacy_schema in [
            2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32, 10_u32, 11_u32, 12_u32, 13_u32,
            14_u32, 15_u32,
        ] {
            let source = fixture_path(&format!("source-v{legacy_schema}"));
            let backup = fixture_path(&format!("backup-v{legacy_schema}"));
            create_metadata_fixture(&source, legacy_schema);
            let legacy_thread = ThreadId::from_static("thread-v1");

            let error = SqliteEventStore::open(&source)
                .await
                .err()
                .expect("legacy store must require migration");
            assert!(error.to_string().contains("state-migrate"));

            let report = SqliteEventStore::migrate(&source, &backup)
                .await
                .expect("migrate metadata-bearing State store");
            assert_eq!(report.from_event_schema, legacy_schema);
            assert_eq!(report.to_event_schema, STATE_EVENT_SCHEMA_VERSION);
            assert_eq!(report.historical_events, 1);

            let migrated = Connection::open(&source).expect("inspect migrated store");
            assert!(super::column_exists(&migrated, "streams", "name").expect("name column"));
            assert!(
                super::column_exists(&migrated, "streams", "tenant_id").expect("tenant column")
            );
            let tenant_id: Option<String> = migrated
                .query_row(
                    "SELECT tenant_id FROM streams WHERE thread_id = 'thread-v1'",
                    [],
                    |row| row.get(0),
                )
                .expect("legacy Thread tenant");
            assert_eq!(
                tenant_id, None,
                "migration must preserve legacy Threads as unscoped"
            );
            let snapshots: i64 = migrated
                .query_row("SELECT COUNT(*) FROM state_snapshots", [], |row| row.get(0))
                .expect("snapshot count");
            assert_eq!(snapshots, 0, "legacy snapshots are disposable");
            drop(migrated);

            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&source)
                    .await
                    .expect("open migrated store"),
            ));
            let historical = state.events(&legacy_thread).await.expect("read history");
            assert_eq!(historical[0].schema_version, legacy_schema);
            let current = state.create_thread().await.expect("write current event");
            assert_eq!(
                state.events(&current.id).await.expect("read current")[0].schema_version,
                STATE_EVENT_SCHEMA_VERSION
            );
            let backup_connection = Connection::open(&backup).expect("open backup");
            let backup_coordinates: (i64, i64) = backup_connection
                .query_row(
                    &format!(
                        "SELECT
                            MAX(CASE WHEN key = 'event_schema' THEN value END),
                            MAX(CASE WHEN key = 'snapshot_schema' THEN value END)
                         FROM {STATE_METADATA_TABLE}"
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("backup coordinates");
            assert_eq!(
                backup_coordinates,
                (
                    i64::from(legacy_schema),
                    i64::from(super::legacy_snapshot_schema(legacy_schema).expect("snapshot"))
                )
            );
            cleanup(&source);
            cleanup(&backup);
        }
    }

    #[test]
    fn migration_restarts_after_every_mutating_phase() {
        for phase in ["after_preflight", "after_backup", "before_commit"] {
            let source = fixture_path(phase);
            let backup = fixture_path(&format!("{phase}-backup"));
            create_v1_fixture(&source);

            migrate_with_stop(&source, &backup, phase).expect_err("injected stop");
            let source_connection = Connection::open(&source).expect("open source");
            assert!(!table_exists(&source_connection, STATE_METADATA_TABLE));
            drop(source_connection);

            let report = migrate_with_stop(&source, &backup, "none").expect("resume migration");
            assert_eq!(report.status, StateMigrationStatus::Migrated);
            let source_connection = Connection::open(&source).expect("open migrated source");
            assert!(table_exists(&source_connection, STATE_METADATA_TABLE));
            cleanup(&source);
            cleanup(&backup);
        }
    }

    #[test]
    fn metadata_migration_restarts_after_every_phase() {
        for legacy_schema in [
            2_u32, 3_u32, 4_u32, 5_u32, 6_u32, 7_u32, 8_u32, 9_u32, 10_u32, 11_u32, 12_u32, 13_u32,
            14_u32, 15_u32,
        ] {
            for phase in ["after_preflight", "after_backup", "before_commit"] {
                let source = fixture_path(&format!("v{legacy_schema}-{phase}"));
                let backup = fixture_path(&format!("v{legacy_schema}-{phase}-backup"));
                create_metadata_fixture(&source, legacy_schema);

                migrate_with_stop(&source, &backup, phase).expect_err("injected stop");
                let source_connection = Connection::open(&source).expect("open legacy source");
                let event_schema: i64 = source_connection
                    .query_row(
                        &format!(
                            "SELECT value FROM {STATE_METADATA_TABLE} WHERE key = 'event_schema'"
                        ),
                        [],
                        |row| row.get(0),
                    )
                    .expect("read legacy source coordinate");
                assert_eq!(event_schema, i64::from(legacy_schema));
                drop(source_connection);

                let report = migrate_with_stop(&source, &backup, "none").expect("resume migration");
                assert_eq!(report.status, StateMigrationStatus::Migrated);
                assert_eq!(report.from_event_schema, legacy_schema);
                let source_connection = Connection::open(&source).expect("open migrated source");
                let coordinates: (i64, i64) = source_connection
                    .query_row(
                        &format!(
                            "SELECT
                                MAX(CASE WHEN key = 'event_schema' THEN value END),
                                MAX(CASE WHEN key = 'snapshot_schema' THEN value END)
                             FROM {STATE_METADATA_TABLE}"
                        ),
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .expect("read current source coordinates");
                assert_eq!(
                    coordinates,
                    (
                        i64::from(STATE_EVENT_SCHEMA_VERSION),
                        i64::from(STATE_SNAPSHOT_SCHEMA_VERSION)
                    )
                );
                cleanup(&source);
                cleanup(&backup);
            }
        }
    }

    #[tokio::test]
    async fn current_store_is_idempotent_without_replacing_backup() {
        let source = fixture_path("current");
        let backup = fixture_path("unused-backup");
        let store = SqliteEventStore::open(&source)
            .await
            .expect("create current store");
        let pending = crate::PendingEvent {
            event_id: EventId::generate(),
            thread_id: ThreadId::generate(),
            expected_stream_version: 0,
            expected_stream_recovery_bytes: 0,
            recorded_at_ms: crate::kernel::now_ms(),
            event: crate::StateEvent::ThreadCreated {
                created_at_ms: 1,
                tenant_id: None,
            },
        };
        store.append(pending).await.expect("append current event");
        drop(store);

        let report = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect("idempotent current migration");
        assert_eq!(report.status, StateMigrationStatus::AlreadyCurrent);
        assert!(report.backup_path.is_none());
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn current_event_schema_migrates_missing_wait_projection_coordinate() {
        let source = fixture_path("projection-only-source");
        let backup = fixture_path("projection-only-backup");
        let store = SqliteEventStore::open(&source)
            .await
            .expect("create current store");
        let state = StateEngine::new(Arc::new(store));
        let thread = state.create_thread().await.expect("create current Thread");
        state
            .create_snapshot(&thread.id)
            .await
            .expect("create current snapshot");
        drop(state);

        let connection = Connection::open(&source).expect("open current fixture");
        connection
            .execute_batch(&format!(
                "DELETE FROM {STATE_METADATA_TABLE}
                   WHERE key = 'agent_loop_wait_projection_schema';
                 DROP INDEX agent_loop_wait_due;
                 DROP TABLE agent_loop_wait_projection;"
            ))
            .expect("remove post-schema projection");
        drop(connection);

        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("projection-less current store requires migration");
        assert!(error.to_string().contains("state-migrate"));

        let report = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect("projection-only migration");
        assert_eq!(report.status, StateMigrationStatus::Migrated);
        assert_eq!(report.from_event_schema, STATE_EVENT_SCHEMA_VERSION);
        assert_eq!(report.to_event_schema, STATE_EVENT_SCHEMA_VERSION);
        assert_eq!(report.from_agent_loop_wait_projection_schema, None);
        assert_eq!(report.to_agent_loop_wait_projection_schema, 1);
        assert!(backup.is_file());

        let snapshot_count: i64 = Connection::open(&source)
            .expect("inspect projection-only snapshot")
            .query_row("SELECT COUNT(*) FROM state_snapshots", [], |row| row.get(0))
            .expect("count projection-only snapshots");
        assert_eq!(
            snapshot_count, 1,
            "projection-only migration preserves current snapshots"
        );

        SqliteEventStore::open(&source)
            .await
            .expect("open projection-migrated store");
        let connection = Connection::open(&source).expect("inspect migrated metadata");
        let projection_schema: i64 = connection
            .query_row(
                &format!(
                    "SELECT value FROM {STATE_METADATA_TABLE}
                     WHERE key = 'agent_loop_wait_projection_schema'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("projection coordinate");
        assert_eq!(projection_schema, 1);
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn projection_only_migration_backfills_a_live_wait() {
        let source = fixture_path("projection-backfill-source");
        let backup = fixture_path("projection-backfill-backup");
        let store = Arc::new(
            SqliteEventStore::open(&source)
                .await
                .expect("create current store"),
        );
        let state = StateEngine::new(store.clone());
        let wait_id = AgentLoopWaitId::from_static("migration-live-wait");
        let _ = start_projection_test_wait(&state, wait_id.clone()).await;
        drop(state);
        drop(store);

        let connection = Connection::open(&source).expect("open current fixture");
        connection
            .execute_batch(&format!(
                "DELETE FROM {STATE_METADATA_TABLE}
                   WHERE key = 'agent_loop_wait_projection_schema';
                 DROP INDEX agent_loop_wait_due;
                 DROP TABLE agent_loop_wait_projection;"
            ))
            .expect("remove derived projection");
        drop(connection);

        SqliteEventStore::migrate(&source, &backup)
            .await
            .expect("backfill live wait");
        let connection = Connection::open(&source).expect("inspect wait projection");
        let projected: (String, i64, i64) = connection
            .query_row(
                "SELECT wait_id, phase, due_at_ms
                 FROM agent_loop_wait_projection",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("live wait projection");
        assert_eq!(projected.0, wait_id.as_str());
        assert_eq!(projected.1, 1);
        assert!(projected.2 > 0);
        drop(connection);
        SqliteEventStore::open(&source)
            .await
            .expect("open backfilled live-wait store");
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn projection_only_migration_rolls_back_before_commit_and_retries_exactly() {
        let source = fixture_path("projection-before-commit-source");
        let backup = fixture_path("projection-before-commit-backup");
        let store = Arc::new(
            SqliteEventStore::open(&source)
                .await
                .expect("create current store"),
        );
        let state = StateEngine::new(store.clone());
        let wait_id = AgentLoopWaitId::from_static("migration-before-commit-wait");
        let _ = start_projection_test_wait(&state, wait_id.clone()).await;
        drop(state);
        drop(store);

        let connection = Connection::open(&source).expect("open current fixture");
        connection
            .execute_batch(&format!(
                "DELETE FROM {STATE_METADATA_TABLE}
                   WHERE key = 'agent_loop_wait_projection_schema';
                 DROP INDEX agent_loop_wait_due;
                 DROP TABLE agent_loop_wait_projection;"
            ))
            .expect("remove derived projection");
        drop(connection);

        let error = migrate_with_stop(&source, &backup, "before_commit")
            .expect_err("injected stop must roll back projection-only migration");
        assert!(error.to_string().contains("before commit"));
        assert!(backup.is_file());
        let connection = Connection::open(&source).expect("inspect rolled-back source");
        let coordinate_count: i64 = connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM {STATE_METADATA_TABLE}
                     WHERE key = 'agent_loop_wait_projection_schema'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("count rolled-back coordinate");
        assert_eq!(coordinate_count, 0);
        assert!(!table_exists(&connection, "agent_loop_wait_projection"));
        assert!(!super::index_exists(&connection, "agent_loop_wait_due").expect("index state"));
        drop(connection);

        let report = migrate_with_stop(&source, &backup, "none")
            .expect("retry projection-only migration against exact backup");
        assert_eq!(report.status, StateMigrationStatus::Migrated);
        let connection = Connection::open(&source).expect("inspect retried projection");
        let projected: (String, i64) = connection
            .query_row(
                "SELECT wait_id, revision FROM agent_loop_wait_projection",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("retried live-wait projection");
        assert_eq!(projected, (wait_id.as_str().to_owned(), 1));
        drop(connection);
        SqliteEventStore::open(&source)
            .await
            .expect("open atomically retried projection migration");
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn current_projection_drift_and_missing_due_index_fail_closed_on_open() {
        let source = fixture_path("projection-open-validation");
        let store = Arc::new(
            SqliteEventStore::open(&source)
                .await
                .expect("create current store"),
        );
        let state = StateEngine::new(store.clone());
        let _ = start_projection_test_wait(
            &state,
            AgentLoopWaitId::from_static("migration-open-validation-wait"),
        )
        .await;
        drop(state);
        drop(store);

        let connection = Connection::open(&source).expect("open current fixture");
        assert_eq!(
            connection
                .execute(
                    "UPDATE agent_loop_wait_projection
                     SET due_at_ms = due_at_ms + 1",
                    [],
                )
                .expect("drift due coordinate"),
            1
        );
        drop(connection);
        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("projection drift must fail current-store validation");
        assert!(
            error
                .to_string()
                .contains("projection does not match authoritative lifecycle events")
        );

        let connection = Connection::open(&source).expect("repair current fixture");
        assert_eq!(
            connection
                .execute(
                    "UPDATE agent_loop_wait_projection
                     SET due_at_ms = due_at_ms - 1",
                    [],
                )
                .expect("restore due coordinate"),
            1
        );
        drop(connection);
        let restored = SqliteEventStore::open(&source)
            .await
            .expect("restored projection must validate");
        drop(restored);

        Connection::open(&source)
            .expect("open index fixture")
            .execute("DROP INDEX agent_loop_wait_due", [])
            .expect("drop due index");
        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("missing due index must fail current-store validation");
        assert!(
            error
                .to_string()
                .contains("missing agent_loop_wait_due index")
        );
        cleanup(&source);
    }

    #[tokio::test]
    async fn terminal_materialized_wait_histories_remain_absent_from_live_projection() {
        let source = fixture_path("materialized-wait-source");
        let backup = fixture_path("materialized-wait-backup");
        let child_id = ThreadId::from_static("migration-forked-terminal-wait");
        let grandchild_id = ThreadId::from_static("migration-nested-forked-terminal-wait");
        let imported_id = ThreadId::from_static("migration-imported-terminal-wait");
        let nested_imported_id = ThreadId::from_static("migration-nested-import-terminal-wait");
        let store = Arc::new(
            SqliteEventStore::open(&source)
                .await
                .expect("create current store"),
        );
        let state = StateEngine::new(store.clone());
        let wait_id = AgentLoopWaitId::from_static("migration-terminal-wait");
        let (turn, request) = start_projection_test_wait(&state, wait_id.clone()).await;
        let settled_at_ms = crate::kernel::now_ms();
        let settlement = ApprovalRecord {
            schema_version: crate::APPROVAL_INBOX_SCHEMA_VERSION,
            request,
            tenant_id: None,
            status: ApprovalRecordStatus::Settled {
                decision: ApprovalDecision::Approve,
                decided_by: ActorIdentity::Authenticated {
                    authority: "migration-test-approver".to_owned(),
                    subject: "operator".to_owned(),
                },
            },
            revision: 2,
            requested_at_ms: settled_at_ms.saturating_sub(1),
            settled_at_ms: Some(settled_at_ms),
        };
        state
            .accept_resume(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("migration-accept-terminal-wait"),
                &settlement,
            )
            .await
            .expect("accept durable wait settlement");
        state
            .close_wait(
                &turn,
                &wait_id,
                2,
                AgentLoopCloseCommandId::from_static("migration-close-terminal-wait"),
                TurnStatus::Cancelled,
                TurnStopReason::Cancelled,
            )
            .await
            .expect("close durable wait");
        state
            .fork_thread(&turn.thread_id, child_id.clone(), Some(&turn.id))
            .await
            .expect("fork terminal wait history");
        state
            .fork_thread(&child_id, grandchild_id, Some(&turn.id))
            .await
            .expect("fork terminal wait history through another materialized Thread");
        let archive = state
            .export_thread(&turn.thread_id)
            .await
            .expect("export terminal wait history");
        state
            .import_thread(&archive, imported_id.clone())
            .await
            .expect("import terminal wait history");
        let nested_archive = state
            .export_thread(&child_id)
            .await
            .expect("export an already materialized terminal wait history");
        state
            .import_thread(&nested_archive, nested_imported_id)
            .await
            .expect("import terminal wait history through another materialized Thread");
        drop(state);
        drop(store);

        let reopened = SqliteEventStore::open(&source)
            .await
            .expect("terminal materializations must pass current projection validation");
        drop(reopened);
        let connection = Connection::open(&source).expect("inspect current projection");
        let projected: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_loop_wait_projection",
                [],
                |row| row.get(0),
            )
            .expect("count current live waits");
        assert_eq!(projected, 0);
        connection
            .execute_batch(&format!(
                "DELETE FROM {STATE_METADATA_TABLE}
                   WHERE key = 'agent_loop_wait_projection_schema';
                 DROP INDEX agent_loop_wait_due;
                 DROP TABLE agent_loop_wait_projection;"
            ))
            .expect("remove derived projection");
        drop(connection);

        SqliteEventStore::migrate(&source, &backup)
            .await
            .expect("rebuild projection with terminal materializations");
        let connection = Connection::open(&source).expect("inspect rebuilt projection");
        let projected: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_loop_wait_projection",
                [],
                |row| row.get(0),
            )
            .expect("count rebuilt live waits");
        assert_eq!(projected, 0);
        drop(connection);
        SqliteEventStore::open(&source)
            .await
            .expect("open projection rebuilt from terminal materializations");

        let mut backup_connection = Connection::open(&backup).expect("open migration backup");
        let transaction = backup_connection
            .transaction()
            .expect("start provenance mutation");
        assert_eq!(
            transaction
                .execute(
                    "DELETE FROM events
                     WHERE thread_id = ?1
                       AND json_extract(event_json, '$.type') = 'thread_imported'",
                    [imported_id.as_str()],
                )
                .expect("remove import provenance"),
            1
        );
        let error = super::projected_agent_loop_waits(&transaction)
            .expect_err("rebound wait without provenance must fail closed");
        assert!(
            error
                .to_string()
                .contains("differs without materialization provenance")
        );
        transaction.rollback().expect("restore import provenance");

        let transaction = backup_connection
            .transaction()
            .expect("start provenance-order mutation");
        let provenance_sequence: i64 = transaction
            .query_row(
                "SELECT sequence
                 FROM events
                 WHERE thread_id = ?1
                   AND json_extract(event_json, '$.type') = 'thread_imported'",
                [imported_id.as_str()],
                |row| row.get(0),
            )
            .expect("import provenance sequence");
        let third_sequence: i64 = transaction
            .query_row(
                "SELECT sequence
                 FROM events
                 WHERE thread_id = ?1
                 ORDER BY sequence
                 LIMIT 1 OFFSET 2",
                [imported_id.as_str()],
                |row| row.get(0),
            )
            .expect("third imported event sequence");
        assert_eq!(
            transaction
                .execute(
                    "UPDATE events SET sequence = -1
                     WHERE thread_id = ?1 AND sequence = ?2",
                    params![imported_id.as_str(), provenance_sequence],
                )
                .expect("temporarily move provenance"),
            1
        );
        assert_eq!(
            transaction
                .execute(
                    "UPDATE events SET sequence = ?2
                     WHERE thread_id = ?1 AND sequence = ?3",
                    params![imported_id.as_str(), provenance_sequence, third_sequence],
                )
                .expect("move third event to second position"),
            1
        );
        assert_eq!(
            transaction
                .execute(
                    "UPDATE events SET sequence = ?2
                     WHERE thread_id = ?1 AND sequence = -1",
                    params![imported_id.as_str(), third_sequence],
                )
                .expect("move provenance to third position"),
            1
        );
        let error = super::projected_agent_loop_waits(&transaction)
            .expect_err("late provenance must not authorize historical rebinding");
        assert!(
            error
                .to_string()
                .contains("must be the second event after ThreadCreated")
        );
        transaction.rollback().expect("restore provenance ordering");

        let transaction = backup_connection
            .transaction()
            .expect("start terminal mutation");
        assert_eq!(
            transaction
                .execute(
                    "DELETE FROM events
                     WHERE thread_id = ?1
                       AND json_extract(event_json, '$.type') = 'wait_closed'",
                    [child_id.as_str()],
                )
                .expect("remove copied terminal event"),
            1
        );
        let error = super::projected_agent_loop_waits(&transaction)
            .expect_err("rebound wait without terminal evidence must fail closed");
        assert!(
            error
                .to_string()
                .contains("does not end at a terminal Turn")
        );
        transaction.rollback().expect("restore terminal event");

        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn projection_table_without_coordinate_is_rejected_before_backup() {
        let source = fixture_path("partial-projection-source");
        let backup = fixture_path("partial-projection-backup");
        let store = SqliteEventStore::open(&source)
            .await
            .expect("create current store");
        drop(store);
        Connection::open(&source)
            .expect("open current fixture")
            .execute(
                &format!(
                    "DELETE FROM {STATE_METADATA_TABLE}
                     WHERE key = 'agent_loop_wait_projection_schema'"
                ),
                [],
            )
            .expect("remove projection coordinate");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("partial projection must fail");
        assert!(
            error
                .to_string()
                .contains("wait projection exists without its metadata coordinate")
        );
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn current_store_rejects_unknown_metadata_coordinates() {
        let source = fixture_path("future-metadata");
        let store = SqliteEventStore::open(&source)
            .await
            .expect("create current store");
        drop(store);
        Connection::open(&source)
            .expect("open current fixture")
            .execute(
                &format!("INSERT INTO {STATE_METADATA_TABLE} (key, value) VALUES ('future', 1)"),
                [],
            )
            .expect("add unknown coordinate");

        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("future metadata must fail");
        assert!(
            error
                .to_string()
                .contains("unsupported SQLite State metadata key")
        );
        cleanup(&source);
    }

    #[tokio::test]
    async fn unknown_legacy_schema_fails_before_backup_creation() {
        let source = fixture_path("unknown");
        let backup = fixture_path("unknown-backup");
        create_v1_fixture(&source);
        Connection::open(&source)
            .expect("open fixture")
            .execute("UPDATE events SET schema_version = 99", [])
            .expect("corrupt schema");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("unknown schema");
        assert!(error.to_string().contains("schema version 99"));
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn inconsistent_legacy_accounting_fails_before_backup_creation() {
        let source = fixture_path("inconsistent-accounting");
        let backup = fixture_path("inconsistent-accounting-backup");
        create_v1_fixture(&source);
        Connection::open(&source)
            .expect("open inconsistent fixture")
            .execute(
                "UPDATE stream_recovery SET recovery_bytes = recovery_bytes + 1",
                [],
            )
            .expect("corrupt recovery charge");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("inconsistent accounting");
        assert!(error.to_string().contains("recovery charges"));
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn schema_eight_name_drift_fails_before_backup_creation() {
        let source = fixture_path("schema-eight-name-drift");
        let backup = fixture_path("schema-eight-name-drift-backup");
        create_metadata_fixture(&source, 8);
        Connection::open(&source)
            .expect("open schema-8 fixture")
            .execute(
                "UPDATE streams SET name = 'not-in-the-journal' WHERE thread_id = 'thread-v1'",
                [],
            )
            .expect("drift name projection");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("schema-8 projection drift");
        assert!(
            error
                .to_string()
                .contains("Thread names do not match authoritative events")
        );
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn partial_legacy_store_fails_without_bootstrapping_metadata() {
        let source = fixture_path("partial");
        let connection = Connection::open(&source).expect("create partial fixture");
        connection
            .execute_batch(
                "
                CREATE TABLE events (
                    sequence       INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id       TEXT NOT NULL UNIQUE,
                    thread_id      TEXT NOT NULL,
                    recorded_at_ms INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    event_json     TEXT NOT NULL
                );
                ",
            )
            .expect("create events only");
        drop(connection);

        let error = SqliteEventStore::open(&source)
            .await
            .err()
            .expect("partial store must fail");
        assert!(error.to_string().contains("missing streams"));
        let connection = Connection::open(&source).expect("reopen partial fixture");
        assert!(!table_exists(&connection, STATE_METADATA_TABLE));
        cleanup(&source);
    }

    #[tokio::test]
    async fn migration_never_replaces_an_existing_backup_path() {
        let source = fixture_path("no-clobber");
        let backup = fixture_path("no-clobber-backup");
        create_v1_fixture(&source);
        std::fs::write(&backup, b"operator-owned backup").expect("create occupied backup path");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("occupied backup path must fail validation");
        assert!(
            error.to_string().contains("database")
                || error.to_string().contains("manifest")
                || error.to_string().contains("SQLite")
        );
        assert_eq!(
            std::fs::read(&backup).expect("read occupied backup"),
            b"operator-owned backup"
        );
        let source_connection = Connection::open(&source).expect("open unchanged source");
        assert!(!table_exists(&source_connection, STATE_METADATA_TABLE));
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn migration_rejects_a_same_shape_backup_from_different_history() {
        let source = fixture_path("fingerprint-source");
        let other = fixture_path("fingerprint-other");
        let backup = fixture_path("fingerprint-backup");
        create_v1_fixture(&source);
        create_v1_fixture(&other);
        Connection::open(&other)
            .expect("open other history")
            .execute(
                "UPDATE events
                 SET event_json = '{\"type\":\"thread_created\",\"created_at_ms\":2}'
                 WHERE event_id = 'event-v1-created'",
                [],
            )
            .expect("change other history without changing its shape");
        migrate_with_stop(&other, &backup, "none").expect("create other valid backup");

        let error = SqliteEventStore::migrate(&source, &backup)
            .await
            .expect_err("same-shape backup must not match different events");
        assert!(error.to_string().contains("does not match"));
        let source_connection = Connection::open(&source).expect("open unchanged source");
        assert!(!table_exists(&source_connection, STATE_METADATA_TABLE));
        cleanup(&source);
        cleanup(&other);
        cleanup(&backup);
    }

    #[test]
    fn migration_rejects_a_backup_with_changed_schema_coordinates() {
        let source = fixture_path("coordinate-source");
        let backup = fixture_path("coordinate-backup");
        create_metadata_fixture(&source, 3);
        migrate_with_stop(&source, &backup, "after_backup")
            .expect_err("stop after publishing the valid backup");
        Connection::open(&backup)
            .expect("open coordinate-bound backup")
            .execute(
                &format!(
                    "UPDATE {BACKUP_MANIFEST_TABLE}
                     SET from_snapshot_schema = 2
                     WHERE id = 1"
                ),
                [],
            )
            .expect("change backup source coordinate");

        let error = migrate_with_stop(&source, &backup, "none")
            .expect_err("backup coordinates must match the source");
        assert!(error.to_string().contains("does not match"));
        let source_connection = Connection::open(&source).expect("open unchanged source");
        let event_schema: i64 = source_connection
            .query_row(
                &format!("SELECT value FROM {STATE_METADATA_TABLE} WHERE key = 'event_schema'"),
                [],
                |row| row.get(0),
            )
            .expect("read unchanged source coordinate");
        assert_eq!(event_schema, 3);
        cleanup(&source);
        cleanup(&backup);
    }

    #[test]
    #[ignore = "manual 64 MiB migration performance evidence"]
    fn migrates_largest_supported_thread_fixture() {
        let source = fixture_path("largest");
        let backup = fixture_path("largest-backup");
        create_v1_fixture(&source);
        let mut connection = Connection::open(&source).expect("open largest fixture");
        let transaction = connection.transaction().expect("begin fixture transaction");
        let recovery_bytes: i64 = transaction
            .query_row(
                "SELECT recovery_bytes FROM stream_recovery WHERE thread_id = 'thread-v1'",
                [],
                |row| row.get(0),
            )
            .expect("fixture recovery charge");
        let mut recovery_bytes =
            u64::try_from(recovery_bytes).expect("non-negative fixture recovery charge");
        let limit = STATE_THREAD_RECOVERY_BYTE_LIMIT - STATE_TERMINAL_RECOVERY_BYTE_RESERVE;
        let mut checkpoints = 0_u64;
        loop {
            let event = StateEvent::CheckpointCreated {
                checkpoint: Checkpoint {
                    id: CheckpointId::from_string(format!("checkpoint-{checkpoints}")),
                    thread_id: ThreadId::from_static("thread-v1"),
                    turn_id: None,
                    target_sequence: 1,
                    created_at_ms: 1,
                    label: Some("x".repeat(3_072)),
                },
            };
            let json = serde_json::to_string(&event).expect("encode fixture event");
            let charge = u64::try_from(json.len()).expect("event length") + 512;
            if recovery_bytes + charge > limit {
                break;
            }
            checkpoints += 1;
            recovery_bytes += charge;
            transaction
                .execute(
                    "INSERT INTO events
                        (event_id, thread_id, recorded_at_ms, schema_version, event_json)
                     VALUES (?1, 'thread-v1', 1, 1, ?2)",
                    params![format!("event-checkpoint-{checkpoints}"), json],
                )
                .expect("insert fixture checkpoint");
        }
        transaction
            .execute(
                "UPDATE streams SET version = ?1 WHERE thread_id = 'thread-v1'",
                [i64::try_from(checkpoints + 1).expect("stream version")],
            )
            .expect("update fixture stream");
        transaction
            .execute(
                "UPDATE stream_recovery SET recovery_bytes = ?1
                 WHERE thread_id = 'thread-v1'",
                [i64::try_from(recovery_bytes).expect("recovery bytes")],
            )
            .expect("update fixture recovery");
        transaction.commit().expect("commit largest fixture");
        drop(connection);

        let started = Instant::now();
        let report = migrate_with_stop(&source, &backup, "none").expect("migrate largest fixture");
        let elapsed = started.elapsed();
        assert_eq!(report.status, StateMigrationStatus::Migrated);
        assert_eq!(report.historical_events, checkpoints + 1);
        assert!(recovery_bytes > limit - 4_096);
        eprintln!(
            "migrated {} events / {} recovery bytes in {:.3} ms",
            report.historical_events,
            recovery_bytes,
            elapsed.as_secs_f64() * 1_000.0
        );
        cleanup(&source);
        cleanup(&backup);
    }

    fn create_v1_fixture(path: &PathBuf) {
        let connection = Connection::open(path).expect("create v1 fixture");
        connection
            .execute_batch(STATE_V1_FIXTURE)
            .expect("apply v1 fixture");
    }

    fn create_metadata_fixture(path: &PathBuf, event_schema: u32) {
        let snapshot_schema =
            super::legacy_snapshot_schema(event_schema).expect("supported legacy coordinate");
        create_v1_fixture(path);
        let connection = Connection::open(path).expect("open metadata fixture");
        if event_schema >= 8 {
            connection
                .execute("ALTER TABLE streams ADD COLUMN name TEXT", [])
                .expect("add schema-8 Thread name projection");
        }
        if event_schema >= 12 {
            connection
                .execute("ALTER TABLE streams ADD COLUMN tenant_id TEXT", [])
                .expect("add schema-12 Thread tenant projection");
        }
        connection
            .execute_batch(&format!(
                "
                UPDATE events SET schema_version = {event_schema};
                CREATE TABLE {STATE_METADATA_TABLE} (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL CHECK(value > 0)
                );
                INSERT INTO {STATE_METADATA_TABLE} (key, value)
                    VALUES ('event_schema', {event_schema});
                INSERT INTO {STATE_METADATA_TABLE} (key, value)
                    VALUES ('snapshot_schema', {snapshot_schema});
                INSERT INTO state_snapshots
                    (thread_id, stream_version, snapshot_json)
                    VALUES ('thread-v1', 1, '{{}}');
                "
            ))
            .expect("advance metadata fixture");
    }

    fn table_exists(connection: &Connection, table: &str) -> bool {
        connection
            .query_row(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                [table],
                |_| Ok(()),
            )
            .is_ok()
    }

    fn journal_mode(path: &PathBuf) -> String {
        Connection::open(path)
            .expect("open journal mode fixture")
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode")
    }

    fn fixture_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-migration-{label}-{}.db",
            EventId::generate()
        ))
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
        let _ = std::fs::remove_file(format!("{}.partial", path.display()));
    }
}
