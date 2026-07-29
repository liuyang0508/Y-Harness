//! Atomic persistence ports for durable Human Handoff aggregates.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};

use super::{
    HumanHandoff, HumanHandoffApplyOutcome, HumanHandoffCommand, HumanHandoffCreateRequest,
    HumanHandoffStatus, MAX_HANDOFF_JSON_BYTES, validate_identity,
};
use crate::{AuthorityContext, HarnessError, HarnessFuture, HumanHandoffId, sqlite::bounded_text};

/// Current durable Human Handoff store schema.
pub const HUMAN_HANDOFF_SCHEMA_VERSION: u32 = 1;
const MAX_HANDOFF_PAGE: usize = 256;

/// Stable queue cursor in priority-descending, request-time, identity order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanHandoffCursor {
    /// Priority of the last returned case.
    pub priority: u8,
    /// Request time of the last returned case.
    pub requested_at_ms: u64,
    /// Identity of the last returned case.
    pub handoff_id: HumanHandoffId,
}

/// Immutable revisioned Human Handoff projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HumanHandoffSnapshot {
    id: HumanHandoffId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    revision: u64,
    handoff: HumanHandoff,
}

impl HumanHandoffSnapshot {
    /// Returns the stable case identity.
    #[must_use]
    pub fn id(&self) -> &HumanHandoffId {
        &self.id
    }

    /// Returns the immutable tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the positive optimistic-concurrency revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the current validated aggregate projection.
    #[must_use]
    pub fn handoff(&self) -> &HumanHandoff {
        &self.handoff
    }
}

/// One bounded queue page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HumanHandoffPage {
    /// Queued cases in stable scheduling order.
    pub handoffs: Vec<HumanHandoffSnapshot>,
    /// Cursor for a later page.
    pub next_cursor: Option<HumanHandoffCursor>,
    /// Whether another queued case exists.
    pub has_more: bool,
}

/// Result of one idempotent Human Handoff command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HumanHandoffCommandResult {
    /// Current durable snapshot after application or duplicate recognition.
    pub snapshot: HumanHandoffSnapshot,
    /// Whether the command changed the durable revision.
    pub outcome: HumanHandoffApplyOutcome,
}

/// Atomic persistence, queue discovery, and command boundary.
pub trait HumanHandoffCoordinator: Send + Sync {
    /// Creates one unscoped Human Handoff.
    fn create<'a>(
        &'a self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, HumanHandoffSnapshot> {
        Box::pin(async move {
            self.create_as(
                handoff_id,
                request,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Creates or recognizes one exact case under trusted tenant authority.
    fn create_as<'a>(
        &'a self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffSnapshot>;

    /// Loads one unscoped case.
    fn load<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
    ) -> HarnessFuture<'a, Option<HumanHandoffSnapshot>> {
        Box::pin(async move {
            self.load_as(handoff_id, &AuthorityContext::local_process())
                .await
        })
    }

    /// Loads one case only inside the exact trusted tenant boundary.
    fn load_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<HumanHandoffSnapshot>>;

    /// Lists one unscoped queued work page.
    fn list_queued<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a HumanHandoffCursor>,
        limit: usize,
    ) -> HarnessFuture<'a, HumanHandoffPage> {
        Box::pin(async move {
            self.list_queued_as(queue, after, limit, &AuthorityContext::local_process())
                .await
        })
    }

    /// Lists queued work only inside the exact trusted tenant boundary.
    fn list_queued_as<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a HumanHandoffCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffPage>;

    /// Applies one idempotent command to an unscoped case.
    fn apply<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, HumanHandoffCommandResult> {
        Box::pin(async move {
            self.apply_as(
                handoff_id,
                expected_revision,
                command,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Applies one actor-bound command with exact revision and tenant fencing.
    fn apply_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffCommandResult>;
}

/// In-memory Human Handoff Coordinator with SQLite-equivalent semantics.
#[derive(Default)]
pub struct MemoryHumanHandoffCoordinator {
    handoffs: Mutex<BTreeMap<(String, HumanHandoffId), HumanHandoffSnapshot>>,
}

impl MemoryHumanHandoffCoordinator {
    /// Creates an empty Coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl HumanHandoffCoordinator for MemoryHumanHandoffCoordinator {
    fn create_as<'a>(
        &'a self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffSnapshot> {
        Box::pin(async move {
            validate_access(&handoff_id, authority)?;
            let mut handoffs = self.handoffs.lock().await;
            let key = storage_key(&handoff_id, authority.tenant_id());
            if let Some(existing) = handoffs.get(&key) {
                if existing
                    .handoff
                    .create_matches(&request, authority.actor())?
                {
                    return Ok(existing.clone());
                }
                return Err(HarnessError::HumanHandoff(format!(
                    "Human Handoff {handoff_id} already exists with different actor or creation content"
                )));
            }
            let handoff = HumanHandoff::new(request, applied_at_ms, authority)?;
            let snapshot = HumanHandoffSnapshot {
                id: handoff_id,
                tenant_id: authority.tenant_id().map(str::to_owned),
                revision: 1,
                handoff,
            };
            handoffs.insert(key, snapshot.clone());
            Ok(snapshot)
        })
    }

    fn load_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<HumanHandoffSnapshot>> {
        Box::pin(async move {
            validate_access(handoff_id, authority)?;
            Ok(self
                .handoffs
                .lock()
                .await
                .get(&storage_key(handoff_id, authority.tenant_id()))
                .cloned())
        })
    }

    fn list_queued_as<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a HumanHandoffCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffPage> {
        Box::pin(async move {
            validate_list(queue, after, limit, authority)?;
            let tenant = tenant_storage_key(authority.tenant_id());
            let mut candidates = self
                .handoffs
                .lock()
                .await
                .values()
                .filter(|snapshot| {
                    tenant_storage_key(snapshot.tenant_id()) == tenant
                        && snapshot.handoff.queue() == queue
                        && matches!(snapshot.handoff.status(), HumanHandoffStatus::Queued)
                        && after.is_none_or(|cursor| comes_after(snapshot, cursor))
                })
                .cloned()
                .collect::<Vec<_>>();
            candidates.sort_by(queue_order);
            page_from_candidates(candidates, limit)
        })
    }

    fn apply_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffCommandResult> {
        Box::pin(async move {
            validate_access(handoff_id, authority)?;
            validate_expected_revision(expected_revision)?;
            let mut handoffs = self.handoffs.lock().await;
            let key = storage_key(handoff_id, authority.tenant_id());
            let current = handoffs
                .get(&key)
                .ok_or_else(|| missing_handoff(handoff_id))?
                .clone();
            if current
                .handoff
                .recognizes_command(&command, authority.actor())?
            {
                return Ok(HumanHandoffCommandResult {
                    snapshot: current,
                    outcome: HumanHandoffApplyOutcome::Duplicate,
                });
            }
            if current.revision != expected_revision {
                return Err(HarnessError::HumanHandoffConflict {
                    handoff_id: handoff_id.clone(),
                    expected: expected_revision,
                    actual: current.revision,
                });
            }
            let mut handoff = current.handoff.clone();
            let outcome = handoff.apply(command, applied_at_ms, authority)?;
            debug_assert_eq!(outcome, HumanHandoffApplyOutcome::Applied);
            let revision = current.revision.checked_add(1).ok_or_else(|| {
                HarnessError::HumanHandoff("Human Handoff revision overflow".to_owned())
            })?;
            let saved = HumanHandoffSnapshot {
                id: current.id,
                tenant_id: current.tenant_id,
                revision,
                handoff,
            };
            handoffs.insert(key, saved.clone());
            Ok(HumanHandoffCommandResult {
                snapshot: saved,
                outcome,
            })
        })
    }
}

/// SQLite Human Handoff Coordinator using one immediate transaction per
/// mutation.
pub struct SqliteHumanHandoffCoordinator {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteHumanHandoffCoordinator {
    /// Opens or creates a schema-1 Human Handoff store.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_owned();
        let connection = task::spawn_blocking(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| HarnessError::HumanHandoff(error.to_string()))?;
            }
            let mut connection = Connection::open(path)
                .map_err(|error| HarnessError::HumanHandoff(error.to_string()))?;
            configure_connection(&connection)?;
            initialize_or_validate(&mut connection)?;
            Ok::<_, HarnessError>(connection)
        })
        .await
        .map_err(|error| {
            HarnessError::HumanHandoff(format!("Human Handoff open task failed: {error}"))
        })??;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }
}

impl HumanHandoffCoordinator for SqliteHumanHandoffCoordinator {
    fn create_as<'a>(
        &'a self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffSnapshot> {
        let connection = self.connection.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_access(&handoff_id, &authority)?;
            task::spawn_blocking(move || {
                let mut connection = connection.lock().map_err(|_| {
                    HarnessError::HumanHandoff("Human Handoff lock poisoned".to_owned())
                })?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                if let Some(existing) =
                    load_snapshot(&transaction, &handoff_id, authority.tenant_id())?
                {
                    if existing
                        .handoff
                        .create_matches(&request, authority.actor())?
                    {
                        transaction.commit().map_err(sql_error)?;
                        return Ok(existing);
                    }
                    return Err(HarnessError::HumanHandoff(format!(
                        "Human Handoff {handoff_id} already exists with different actor or creation content"
                    )));
                }
                let handoff = HumanHandoff::new(request, applied_at_ms, &authority)?;
                let encoded = encode_handoff(&handoff)?;
                transaction
                    .execute(
                        "INSERT INTO human_handoffs
                         (tenant_id, handoff_id, schema_version, revision, queue,
                          priority, requested_at_ms, status, handoff_json)
                         VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?8)",
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            handoff_id.as_str(),
                            HUMAN_HANDOFF_SCHEMA_VERSION,
                            handoff.queue(),
                            i64::from(handoff.priority()),
                            sql_time(handoff.requested_at_ms())?,
                            status_name(handoff.status()),
                            encoded
                        ],
                    )
                    .map_err(sql_error)?;
                transaction.commit().map_err(sql_error)?;
                Ok(HumanHandoffSnapshot {
                    id: handoff_id,
                    tenant_id: authority.tenant_id().map(str::to_owned),
                    revision: 1,
                    handoff,
                })
            })
            .await
            .map_err(|error| {
                HarnessError::HumanHandoff(format!("Human Handoff create task failed: {error}"))
            })?
        })
    }

    fn load_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<HumanHandoffSnapshot>> {
        let connection = self.connection.clone();
        let handoff_id = handoff_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_access(&handoff_id, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection.lock().map_err(|_| {
                    HarnessError::HumanHandoff("Human Handoff lock poisoned".to_owned())
                })?;
                load_snapshot(&connection, &handoff_id, authority.tenant_id())
            })
            .await
            .map_err(|error| {
                HarnessError::HumanHandoff(format!("Human Handoff load task failed: {error}"))
            })?
        })
    }

    fn list_queued_as<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a HumanHandoffCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffPage> {
        let connection = self.connection.clone();
        let queue = queue.to_owned();
        let after = after.cloned();
        let authority = authority.clone();
        Box::pin(async move {
            validate_list(&queue, after.as_ref(), limit, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection.lock().map_err(|_| {
                    HarnessError::HumanHandoff("Human Handoff lock poisoned".to_owned())
                })?;
                let fetch = limit.checked_add(1).ok_or_else(|| {
                    HarnessError::HumanHandoff("Human Handoff page overflow".to_owned())
                })?;
                let (has_cursor, cursor_priority, cursor_time, cursor_id) =
                    if let Some(cursor) = &after {
                        (
                            1_i64,
                            i64::from(cursor.priority),
                            i64::try_from(cursor.requested_at_ms).map_err(|_| {
                                HarnessError::HumanHandoff(
                                    "Human Handoff cursor time exceeds SQLite".to_owned(),
                                )
                            })?,
                            cursor.handoff_id.as_str(),
                        )
                    } else {
                        (0_i64, 0_i64, 0_i64, "")
                    };
                let mut statement = connection
                    .prepare(
                        "SELECT handoff_id, schema_version, revision, queue, priority,
                                requested_at_ms, status,
                                length(CAST(handoff_json AS BLOB)), handoff_json
                         FROM human_handoffs
                         WHERE tenant_id = ?1 AND queue = ?2 AND status = 'queued'
                           AND (
                             ?3 = 0 OR priority < ?4
                             OR (priority = ?4 AND requested_at_ms > ?5)
                             OR (priority = ?4 AND requested_at_ms = ?5 AND handoff_id > ?6)
                           )
                         ORDER BY priority DESC, requested_at_ms ASC, handoff_id ASC
                         LIMIT ?7",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            queue,
                            has_cursor,
                            cursor_priority,
                            cursor_time,
                            cursor_id,
                            i64::try_from(fetch).map_err(|_| {
                                HarnessError::HumanHandoff(
                                    "Human Handoff page limit overflow".to_owned(),
                                )
                            })?
                        ],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, i64>(4)?,
                                row.get::<_, i64>(5)?,
                                row.get::<_, String>(6)?,
                                bounded_text(row, 7, 8, MAX_HANDOFF_JSON_BYTES, "Human Handoff")?,
                            ))
                        },
                    )
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                let mut snapshots = Vec::with_capacity(rows.len());
                for (id, schema, revision, stored_queue, priority, requested_at, status, encoded) in
                    rows
                {
                    let id = HumanHandoffId::from_string(id);
                    snapshots.push(decode_snapshot(
                        id,
                        authority.tenant_id(),
                        schema,
                        revision,
                        stored_queue,
                        priority,
                        requested_at,
                        status,
                        encoded,
                    )?);
                }
                page_from_candidates(snapshots, limit)
            })
            .await
            .map_err(|error| {
                HarnessError::HumanHandoff(format!("Human Handoff list task failed: {error}"))
            })?
        })
    }

    fn apply_as<'a>(
        &'a self,
        handoff_id: &'a HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, HumanHandoffCommandResult> {
        let connection = self.connection.clone();
        let handoff_id = handoff_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_access(&handoff_id, &authority)?;
            validate_expected_revision(expected_revision)?;
            task::spawn_blocking(move || {
                let mut connection = connection.lock().map_err(|_| {
                    HarnessError::HumanHandoff("Human Handoff lock poisoned".to_owned())
                })?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                let current = load_snapshot(&transaction, &handoff_id, authority.tenant_id())?
                    .ok_or_else(|| missing_handoff(&handoff_id))?;
                if current
                    .handoff
                    .recognizes_command(&command, authority.actor())?
                {
                    transaction.commit().map_err(sql_error)?;
                    return Ok(HumanHandoffCommandResult {
                        snapshot: current,
                        outcome: HumanHandoffApplyOutcome::Duplicate,
                    });
                }
                if current.revision != expected_revision {
                    return Err(HarnessError::HumanHandoffConflict {
                        handoff_id,
                        expected: expected_revision,
                        actual: current.revision,
                    });
                }
                let mut handoff = current.handoff.clone();
                let outcome = handoff.apply(command, applied_at_ms, &authority)?;
                debug_assert_eq!(outcome, HumanHandoffApplyOutcome::Applied);
                let revision = current.revision.checked_add(1).ok_or_else(|| {
                    HarnessError::HumanHandoff("Human Handoff revision overflow".to_owned())
                })?;
                let encoded = encode_handoff(&handoff)?;
                let changed = transaction
                    .execute(
                        "UPDATE human_handoffs
                         SET revision = ?1, status = ?2, handoff_json = ?3
                         WHERE tenant_id = ?4 AND handoff_id = ?5 AND revision = ?6",
                        params![
                            sql_revision(revision)?,
                            status_name(handoff.status()),
                            encoded,
                            tenant_storage_key(authority.tenant_id()),
                            handoff_id.as_str(),
                            sql_revision(current.revision)?
                        ],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff atomic update changed an unexpected row count".to_owned(),
                    ));
                }
                transaction.commit().map_err(sql_error)?;
                Ok(HumanHandoffCommandResult {
                    snapshot: HumanHandoffSnapshot {
                        id: handoff_id,
                        tenant_id: current.tenant_id,
                        revision,
                        handoff,
                    },
                    outcome,
                })
            })
            .await
            .map_err(|error| {
                HarnessError::HumanHandoff(format!("Human Handoff command task failed: {error}"))
            })?
        })
    }
}

fn configure_connection(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(sql_error)?;
    let mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(sql_error)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(HarnessError::HumanHandoff(format!(
            "SQLite refused WAL mode and selected {mode}"
        )));
    }
    connection
        .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(sql_error)
}

fn initialize_or_validate(connection: &mut Connection) -> Result<(), HarnessError> {
    let has_meta = table_exists(connection, "human_handoff_store_meta")?;
    let has_handoffs = table_exists(connection, "human_handoffs")?;
    match (has_meta, has_handoffs) {
        (false, false) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            transaction
                .execute_batch(
                    "
                    CREATE TABLE human_handoff_store_meta (
                        singleton      INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO human_handoff_store_meta
                        (singleton, schema_version) VALUES (1, 1);
                    CREATE TABLE human_handoffs (
                        tenant_id       TEXT NOT NULL,
                        handoff_id      TEXT NOT NULL,
                        schema_version  INTEGER NOT NULL,
                        revision        INTEGER NOT NULL CHECK(revision > 0),
                        queue           TEXT NOT NULL,
                        priority        INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 255),
                        requested_at_ms INTEGER NOT NULL CHECK(requested_at_ms > 0),
                        status          TEXT NOT NULL,
                        handoff_json    TEXT NOT NULL,
                        PRIMARY KEY (tenant_id, handoff_id)
                    );
                    CREATE INDEX human_handoffs_queue
                        ON human_handoffs
                        (tenant_id, queue, status, priority DESC,
                         requested_at_ms ASC, handoff_id ASC);
                    ",
                )
                .map_err(sql_error)?;
            transaction.commit().map_err(sql_error)
        }
        (true, true) => validate_store(connection),
        _ => Err(HarnessError::HumanHandoff(
            "SQLite Human Handoff store is partial".to_owned(),
        )),
    }
}

fn validate_store(connection: &Connection) -> Result<(), HarnessError> {
    let versions = connection
        .prepare("SELECT singleton, schema_version FROM human_handoff_store_meta")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(sql_error)?;
    if versions != vec![(1, i64::from(HUMAN_HANDOFF_SCHEMA_VERSION))] {
        return Err(HarnessError::HumanHandoff(
            "SQLite Human Handoff store schema is unknown or malformed".to_owned(),
        ));
    }
    let invalid_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM human_handoffs
             WHERE schema_version != ?1 OR revision <= 0
                OR length(tenant_id) > 128
                OR length(handoff_id) = 0 OR length(handoff_id) > 256
                OR length(queue) = 0 OR length(queue) > 256
                OR priority < 0 OR priority > 255 OR requested_at_ms <= 0
                OR status NOT IN ('queued', 'claimed', 'resolved', 'cancelled')
                OR length(CAST(handoff_json AS BLOB)) > ?2",
            params![
                HUMAN_HANDOFF_SCHEMA_VERSION,
                i64::try_from(MAX_HANDOFF_JSON_BYTES).map_err(|_| {
                    HarnessError::HumanHandoff("Human Handoff size overflow".to_owned())
                })?
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if invalid_rows != 0 {
        return Err(HarnessError::HumanHandoff(
            "SQLite Human Handoff store contains invalid row metadata".to_owned(),
        ));
    }
    Ok(())
}

fn load_snapshot(
    connection: &Connection,
    handoff_id: &HumanHandoffId,
    tenant_id: Option<&str>,
) -> Result<Option<HumanHandoffSnapshot>, HarnessError> {
    let row = connection
        .query_row(
            "SELECT schema_version, revision, queue, priority, requested_at_ms, status,
                    length(CAST(handoff_json AS BLOB)), handoff_json
             FROM human_handoffs WHERE tenant_id = ?1 AND handoff_id = ?2",
            params![tenant_storage_key(tenant_id), handoff_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    bounded_text(row, 6, 7, MAX_HANDOFF_JSON_BYTES, "Human Handoff")?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((schema, revision, queue, priority, requested_at, status, encoded)) = row else {
        return Ok(None);
    };
    decode_snapshot(
        handoff_id.clone(),
        tenant_id,
        schema,
        revision,
        queue,
        priority,
        requested_at,
        status,
        encoded,
    )
    .map(Some)
}

#[allow(clippy::too_many_arguments)]
fn decode_snapshot(
    handoff_id: HumanHandoffId,
    tenant_id: Option<&str>,
    schema: i64,
    revision: i64,
    queue: String,
    priority: i64,
    requested_at_ms: i64,
    status: String,
    encoded: String,
) -> Result<HumanHandoffSnapshot, HarnessError> {
    if schema != i64::from(HUMAN_HANDOFF_SCHEMA_VERSION) {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff {handoff_id} uses unsupported schema {schema}"
        )));
    }
    let revision = u64::try_from(revision)
        .map_err(|_| HarnessError::HumanHandoff("invalid Handoff revision".to_owned()))?;
    validate_expected_revision(revision)?;
    let priority = u8::try_from(priority)
        .map_err(|_| HarnessError::HumanHandoff("invalid Handoff priority".to_owned()))?;
    let requested_at_ms = u64::try_from(requested_at_ms)
        .map_err(|_| HarnessError::HumanHandoff("invalid Handoff request time".to_owned()))?;
    let handoff: HumanHandoff = serde_json::from_str(&encoded)
        .map_err(|error| HarnessError::HumanHandoff(format!("decode Human Handoff: {error}")))?;
    handoff.validate()?;
    if handoff.queue() != queue
        || handoff.priority() != priority
        || handoff.requested_at_ms() != requested_at_ms
        || status_name(handoff.status()) != status
    {
        return Err(HarnessError::HumanHandoff(
            "Human Handoff SQLite projection differs from aggregate".to_owned(),
        ));
    }
    Ok(HumanHandoffSnapshot {
        id: handoff_id,
        tenant_id: tenant_id.map(str::to_owned),
        revision,
        handoff,
    })
}

fn encode_handoff(handoff: &HumanHandoff) -> Result<String, HarnessError> {
    handoff.validate()?;
    let encoded = serde_json::to_string(handoff)
        .map_err(|_| HarnessError::HumanHandoff("cannot encode Human Handoff".to_owned()))?;
    if encoded.len() > MAX_HANDOFF_JSON_BYTES {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff exceeds {MAX_HANDOFF_JSON_BYTES} encoded bytes"
        )));
    }
    Ok(encoded)
}

fn validate_access(
    handoff_id: &HumanHandoffId,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority.validate_current("Human Handoff Coordinator authority")?;
    validate_identity("Human Handoff", handoff_id.as_str())
}

fn validate_list(
    queue: &str,
    after: Option<&HumanHandoffCursor>,
    limit: usize,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority.validate_current("Human Handoff list authority")?;
    super::validate_application_time(after.map_or(1, |cursor| cursor.requested_at_ms))?;
    super::validate_capability_name("Human Handoff queue", queue)?;
    if let Some(cursor) = after {
        validate_identity("Human Handoff cursor", cursor.handoff_id.as_str())?;
        i64::try_from(cursor.requested_at_ms).map_err(|_| {
            HarnessError::HumanHandoff("Human Handoff cursor time exceeds SQLite".to_owned())
        })?;
    }
    if !(1..=MAX_HANDOFF_PAGE).contains(&limit) {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff page limit must be 1-{MAX_HANDOFF_PAGE}"
        )));
    }
    Ok(())
}

fn validate_expected_revision(revision: u64) -> Result<(), HarnessError> {
    if revision == 0 {
        Err(HarnessError::HumanHandoff(
            "Human Handoff revision must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn page_from_candidates(
    mut candidates: Vec<HumanHandoffSnapshot>,
    limit: usize,
) -> Result<HumanHandoffPage, HarnessError> {
    let has_more = candidates.len() > limit;
    candidates.truncate(limit);
    let next_cursor = candidates.last().map(|snapshot| HumanHandoffCursor {
        priority: snapshot.handoff.priority(),
        requested_at_ms: snapshot.handoff.requested_at_ms(),
        handoff_id: snapshot.id.clone(),
    });
    Ok(HumanHandoffPage {
        handoffs: candidates,
        next_cursor,
        has_more,
    })
}

fn comes_after(snapshot: &HumanHandoffSnapshot, cursor: &HumanHandoffCursor) -> bool {
    snapshot.handoff.priority() < cursor.priority
        || (snapshot.handoff.priority() == cursor.priority
            && (snapshot.handoff.requested_at_ms() > cursor.requested_at_ms
                || (snapshot.handoff.requested_at_ms() == cursor.requested_at_ms
                    && snapshot.id.as_str() > cursor.handoff_id.as_str())))
}

fn queue_order(left: &HumanHandoffSnapshot, right: &HumanHandoffSnapshot) -> std::cmp::Ordering {
    right
        .handoff
        .priority()
        .cmp(&left.handoff.priority())
        .then_with(|| {
            left.handoff
                .requested_at_ms()
                .cmp(&right.handoff.requested_at_ms())
        })
        .then_with(|| left.id.as_str().cmp(right.id.as_str()))
}

fn status_name(status: &HumanHandoffStatus) -> &'static str {
    match status {
        HumanHandoffStatus::Queued => "queued",
        HumanHandoffStatus::Claimed { .. } => "claimed",
        HumanHandoffStatus::Resolved { .. } => "resolved",
        HumanHandoffStatus::Cancelled { .. } => "cancelled",
    }
}

fn storage_key(handoff_id: &HumanHandoffId, tenant_id: Option<&str>) -> (String, HumanHandoffId) {
    (tenant_storage_key(tenant_id).to_owned(), handoff_id.clone())
}

fn tenant_storage_key(tenant_id: Option<&str>) -> &str {
    tenant_id.unwrap_or("")
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
            )",
            [table],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn missing_handoff(handoff_id: &HumanHandoffId) -> HarnessError {
    HarnessError::HumanHandoff(format!("Human Handoff {handoff_id} does not exist"))
}

fn sql_revision(revision: u64) -> Result<i64, HarnessError> {
    i64::try_from(revision)
        .map_err(|_| HarnessError::HumanHandoff("Handoff revision exceeds SQLite".to_owned()))
}

fn sql_time(value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value)
        .map_err(|_| HarnessError::HumanHandoff("Handoff time exceeds SQLite".to_owned()))
}

fn sql_error(error: rusqlite::Error) -> HarnessError {
    HarnessError::HumanHandoff(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::{
        ActorIdentity, HumanHandoffClaimId, HumanHandoffCommandId, HumanHandoffCommandKind,
        HumanHandoffSubject, ThreadId,
    };

    fn tenant(id: &str, actor: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: actor.to_owned(),
            },
            Some(id.to_owned()),
        )
        .expect("authority")
    }

    fn create(command: &str, priority: u8) -> HumanHandoffCreateRequest {
        HumanHandoffCreateRequest {
            command_id: HumanHandoffCommandId::from_string(command.to_owned()),
            subject: HumanHandoffSubject::Thread {
                thread_id: ThreadId::from_string(format!("thread-{command}")),
            },
            queue: "support.primary".to_owned(),
            reason_code: "agent.escalation".to_owned(),
            priority,
        }
    }

    fn claim() -> HumanHandoffCommand {
        HumanHandoffCommand {
            id: HumanHandoffCommandId::from_static("claim-command"),
            kind: HumanHandoffCommandKind::Claim {
                claim_id: HumanHandoffClaimId::from_static("claim"),
                lease_duration_ms: 1_000,
            },
        }
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "y-harness-human-handoff-{label}-{}-{stamp}.db",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn memory_coordinator_is_actor_idempotent_revisioned_and_tenant_fenced() {
        let coordinator = MemoryHumanHandoffCoordinator::new();
        let handoff_id = HumanHandoffId::from_static("handoff");
        let alice = tenant("alpha", "alice");
        let bob = tenant("alpha", "bob");
        let other = tenant("beta", "alice");
        let created = coordinator
            .create_as(handoff_id.clone(), create("create", 1), 10, &alice)
            .await
            .expect("create");
        assert_eq!(created.revision(), 1);
        assert!(
            coordinator
                .load_as(&handoff_id, &other)
                .await
                .expect("other tenant")
                .is_none()
        );
        let applied = coordinator
            .apply_as(&handoff_id, 1, claim(), 20, &alice)
            .await
            .expect("claim");
        assert_eq!(applied.snapshot.revision(), 2);
        let replay = coordinator
            .apply_as(&handoff_id, 1, claim(), 21, &alice)
            .await
            .expect("replay");
        assert_eq!(replay.outcome, HumanHandoffApplyOutcome::Duplicate);
        let collision = coordinator
            .apply_as(&handoff_id, 1, claim(), 22, &bob)
            .await
            .expect_err("actor-bound command");
        assert!(collision.to_string().contains("different actor"));
    }

    #[tokio::test]
    async fn queue_page_is_priority_ordered_cursor_stable_and_queued_only() {
        let coordinator = MemoryHumanHandoffCoordinator::new();
        let authority = tenant("alpha", "alice");
        for (id, priority, time) in [("low", 1, 10), ("high-a", 9, 20), ("high-b", 9, 20)] {
            coordinator
                .create_as(
                    HumanHandoffId::from_string(id.to_owned()),
                    create(&format!("create-{id}"), priority),
                    time,
                    &authority,
                )
                .await
                .expect("create");
        }
        coordinator
            .apply_as(
                &HumanHandoffId::from_static("high-a"),
                1,
                claim(),
                30,
                &authority,
            )
            .await
            .expect("claim");
        let first = coordinator
            .list_queued_as("support.primary", None, 1, &authority)
            .await
            .expect("first");
        assert_eq!(first.handoffs[0].id().as_str(), "high-b");
        assert!(first.has_more);
        let second = coordinator
            .list_queued_as("support.primary", first.next_cursor.as_ref(), 1, &authority)
            .await
            .expect("second");
        assert_eq!(second.handoffs[0].id().as_str(), "low");
        assert!(!second.has_more);
    }

    #[tokio::test]
    async fn sqlite_queue_matches_priority_cursor_and_status_contract() {
        let path = temp_path("queue");
        let coordinator = SqliteHumanHandoffCoordinator::open(&path)
            .await
            .expect("open");
        let authority = tenant("alpha", "alice");
        for (id, priority, time) in [("low", 1, 10), ("high-a", 9, 20), ("high-b", 9, 20)] {
            coordinator
                .create_as(
                    HumanHandoffId::from_string(id.to_owned()),
                    create(&format!("create-{id}"), priority),
                    time,
                    &authority,
                )
                .await
                .expect("create");
        }
        coordinator
            .apply_as(
                &HumanHandoffId::from_static("high-a"),
                1,
                claim(),
                30,
                &authority,
            )
            .await
            .expect("claim");

        let first = coordinator
            .list_queued_as("support.primary", None, 1, &authority)
            .await
            .expect("first");
        assert_eq!(first.handoffs[0].id().as_str(), "high-b");
        assert!(first.has_more);
        let second = coordinator
            .list_queued_as("support.primary", first.next_cursor.as_ref(), 1, &authority)
            .await
            .expect("second");
        assert_eq!(second.handoffs[0].id().as_str(), "low");
        assert!(!second.has_more);

        drop(coordinator);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_reopens_and_rejects_projection_tampering() {
        let path = temp_path("reopen");
        let handoff_id = HumanHandoffId::from_static("handoff");
        let authority = tenant("alpha", "alice");
        {
            let coordinator = SqliteHumanHandoffCoordinator::open(&path)
                .await
                .expect("open");
            coordinator
                .create_as(handoff_id.clone(), create("create", 7), 10, &authority)
                .await
                .expect("create");
        }
        {
            let coordinator = SqliteHumanHandoffCoordinator::open(&path)
                .await
                .expect("reopen");
            let loaded = coordinator
                .load_as(&handoff_id, &authority)
                .await
                .expect("load")
                .expect("handoff");
            assert_eq!(loaded.handoff().priority(), 7);
        }
        {
            let connection = Connection::open(&path).expect("fixture");
            connection
                .execute(
                    "UPDATE human_handoffs SET priority = 8 WHERE handoff_id = ?1",
                    [handoff_id.as_str()],
                )
                .expect("tamper");
        }
        let coordinator = SqliteHumanHandoffCoordinator::open(&path)
            .await
            .expect("open metadata");
        let error = coordinator
            .load_as(&handoff_id, &authority)
            .await
            .expect_err("projection drift");
        assert!(error.to_string().contains("projection differs"));
        drop(coordinator);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_rejects_tampered_actor_bound_command_digest() {
        let path = temp_path("digest");
        let handoff_id = HumanHandoffId::from_static("handoff");
        let authority = tenant("alpha", "alice");
        {
            let coordinator = SqliteHumanHandoffCoordinator::open(&path)
                .await
                .expect("open");
            coordinator
                .create_as(handoff_id.clone(), create("create", 7), 10, &authority)
                .await
                .expect("create");
        }
        {
            let connection = Connection::open(&path).expect("fixture");
            let encoded: String = connection
                .query_row(
                    "SELECT handoff_json FROM human_handoffs WHERE handoff_id = ?1",
                    [handoff_id.as_str()],
                    |row| row.get(0),
                )
                .expect("encoded handoff");
            let mut value: serde_json::Value =
                serde_json::from_str(&encoded).expect("handoff JSON");
            value["transitions"][0]["command_sha256"] = serde_json::Value::String("0".repeat(64));
            connection
                .execute(
                    "UPDATE human_handoffs SET handoff_json = ?1 WHERE handoff_id = ?2",
                    params![
                        serde_json::to_string(&value).expect("encode"),
                        handoff_id.as_str()
                    ],
                )
                .expect("tamper");
        }

        let coordinator = SqliteHumanHandoffCoordinator::open(&path)
            .await
            .expect("open metadata");
        let error = coordinator
            .load_as(&handoff_id, &authority)
            .await
            .expect_err("digest drift");
        assert!(error.to_string().contains("digest"));

        drop(coordinator);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_claim_compare_and_swap_has_one_owner_across_connections() {
        let path = temp_path("claim-race");
        let first = SqliteHumanHandoffCoordinator::open(&path)
            .await
            .expect("first");
        let second = SqliteHumanHandoffCoordinator::open(&path)
            .await
            .expect("second");
        let handoff_id = HumanHandoffId::from_static("handoff");
        let alice = tenant("alpha", "alice");
        let bob = tenant("alpha", "bob");
        first
            .create_as(handoff_id.clone(), create("create", 7), 10, &alice)
            .await
            .expect("create");
        let alice_claim = HumanHandoffCommand {
            id: HumanHandoffCommandId::from_static("claim-alice"),
            kind: HumanHandoffCommandKind::Claim {
                claim_id: HumanHandoffClaimId::from_static("alice"),
                lease_duration_ms: 1_000,
            },
        };
        let bob_claim = HumanHandoffCommand {
            id: HumanHandoffCommandId::from_static("claim-bob"),
            kind: HumanHandoffCommandKind::Claim {
                claim_id: HumanHandoffClaimId::from_static("bob"),
                lease_duration_ms: 1_000,
            },
        };
        let (alice_result, bob_result) = tokio::join!(
            first.apply_as(&handoff_id, 1, alice_claim, 20, &alice),
            second.apply_as(&handoff_id, 1, bob_claim, 20, &bob)
        );
        assert_ne!(alice_result.is_ok(), bob_result.is_ok());
        let error = alice_result
            .err()
            .or_else(|| bob_result.err())
            .expect("one conflict");
        assert!(matches!(
            error,
            HarnessError::HumanHandoffConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        let loaded = first
            .load_as(&handoff_id, &alice)
            .await
            .expect("load")
            .expect("handoff");
        assert!(matches!(
            loaded.handoff().status(),
            HumanHandoffStatus::Claimed { .. }
        ));
        drop(first);
        drop(second);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_partial_store_fails_closed() {
        let path = temp_path("partial");
        {
            let connection = Connection::open(&path).expect("fixture");
            connection
                .execute_batch(
                    "CREATE TABLE human_handoff_store_meta (
                        singleton INTEGER PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );",
                )
                .expect("partial");
        }
        let result = SqliteHumanHandoffCoordinator::open(&path).await;
        assert!(result.is_err());
        assert!(result.err().expect("error").to_string().contains("partial"));
        let _ = std::fs::remove_file(path);
    }
}
