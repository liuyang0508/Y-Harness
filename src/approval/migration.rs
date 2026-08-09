//! Explicit, backup-first SQLite Approval Inbox migration.

use std::{
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
    time::Duration,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::task;

use super::{
    APPROVAL_INBOX_SCHEMA_VERSION, APPROVAL_METADATA_TABLE, ApprovalRecord, ApprovalRecordStatus,
    MAX_APPROVAL_REASON_BYTES, PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION, SqliteApprovalInbox,
    encode_record, metadata_schema_sql, table_exists, validate_current_metadata,
};
use crate::{
    ApprovalActor, ApprovalDecision, ApprovalId, HarnessError, RiskLevel, ToolAuthorization,
    json::validate_value_shape,
    kernel::{now_ms, validate_capability_name},
    sqlite::bounded_text,
};

const LEGACY_APPROVAL_SCHEMA_VERSION: u32 = 1;
const LEGACY_MAX_APPROVAL_RECORD_BYTES: usize = 524_288;
const LEGACY_PENDING_RECORD_BYTES: usize =
    LEGACY_MAX_APPROVAL_RECORD_BYTES - MAX_APPROVAL_REASON_BYTES - 512;
const MIN_MIGRATION_WORKING_BYTES: u64 = 1_048_576;
const BACKUP_MANIFEST_TABLE: &str = "y_harness_approval_migration_backup";
const MIGRATION_PAGE: usize = 16;
const MIGRATED_PENDING_REASON: &str =
    "schema-1 pending approval orphaned during identity migration";

/// Result category for one explicit SQLite Approval Inbox migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApprovalMigrationStatus {
    /// A legacy inbox was backed up and advanced.
    Migrated,
    /// The inbox already used the current writer coordinate.
    AlreadyCurrent,
}

/// Content-free evidence returned by one Approval Inbox migration attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalMigrationReport {
    /// Settlement category.
    pub status: ApprovalMigrationStatus,
    /// Record schema written before migration.
    pub from_record_schema: u32,
    /// Record schema written after migration.
    pub to_record_schema: u32,
    /// Number of historical approval records migrated or observed.
    pub historical_records: u64,
    /// Pending schema-1 requests made non-actionable during migration.
    pub orphaned_pending_records: u64,
    /// Additional backup bytes required by this attempt.
    pub required_backup_bytes: u64,
    /// Available bytes observed on the backup filesystem during preflight.
    pub available_backup_bytes: u64,
    /// Durable backup used as the rollback boundary, when migration ran.
    pub backup_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoreFingerprint {
    record_count: u64,
    records_sha256: [u8; 32],
}

#[derive(Deserialize)]
struct LegacyApprovalRecord {
    schema_version: u32,
    request: LegacyApprovalRequest,
    status: LegacyApprovalRecordStatus,
    revision: u64,
    requested_at_ms: u64,
    settled_at_ms: Option<u64>,
}

#[derive(Deserialize)]
struct SchemaTwoApprovalRecord {
    schema_version: u32,
    request: crate::ApprovalRequest,
    status: ApprovalRecordStatus,
    revision: u64,
    requested_at_ms: u64,
    settled_at_ms: Option<u64>,
}

#[derive(Deserialize)]
struct LegacyApprovalRequest {
    id: ApprovalId,
    authorization: ToolAuthorization,
    reason: String,
    risk: RiskLevel,
}

#[derive(Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum LegacyApprovalRecordStatus {
    Pending,
    Settled { decision: ApprovalDecision },
    Orphaned { reason: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationSource {
    SchemaOne,
    SchemaTwo,
}

impl MigrationSource {
    const fn version(self) -> u32 {
        match self {
            Self::SchemaOne => LEGACY_APPROVAL_SCHEMA_VERSION,
            Self::SchemaTwo => PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION,
        }
    }
}

impl SqliteApprovalInbox {
    /// Migrates a schema-1 or schema-2 SQLite Approval Inbox after creating or
    /// validating a complete rollback backup.
    ///
    /// Every old and new writer must be stopped. Pending legacy requests are
    /// orphaned because their requester identity cannot be reconstructed.
    /// Schema-2 records remain unscoped because tenant ownership cannot be
    /// inferred safely from historical Thread or actor data.
    pub async fn migrate(
        path: impl AsRef<Path>,
        backup_path: impl AsRef<Path>,
    ) -> Result<ApprovalMigrationReport, HarnessError> {
        let path = path.as_ref().to_owned();
        let backup_path = backup_path.as_ref().to_owned();
        task::spawn_blocking(move || migrate_sync(&path, &backup_path, MigrationStop::None))
            .await
            .map_err(|error| {
                HarnessError::Approval(format!("SQLite approval migration task failed: {error}"))
            })?
    }
}

fn migrate_sync(
    path: &Path,
    backup_path: &Path,
    stop: MigrationStop,
) -> Result<ApprovalMigrationReport, HarnessError> {
    validate_migration_paths(path, backup_path)?;
    let mut connection =
        Connection::open(path).map_err(|error| HarnessError::Approval(error.to_string()))?;
    configure_connection(&connection)?;
    if !table_exists(&connection, "approval_records")? {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox migration source has no approval_records table".to_owned(),
        ));
    }
    let records = record_count(&connection)?;
    let Some(source) = migration_source(&connection)? else {
        validate_current_metadata(&connection)?;
        return Ok(ApprovalMigrationReport {
            status: ApprovalMigrationStatus::AlreadyCurrent,
            from_record_schema: APPROVAL_INBOX_SCHEMA_VERSION,
            to_record_schema: APPROVAL_INBOX_SCHEMA_VERSION,
            historical_records: records,
            orphaned_pending_records: 0,
            required_backup_bytes: 0,
            available_backup_bytes: 0,
            backup_path: None,
        });
    };
    let fingerprint = source_store_fingerprint(&connection, source)?;
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
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if migration_source(&transaction)? != Some(source) {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox metadata changed during migration".to_owned(),
        ));
    }
    if source_store_fingerprint(&transaction, source)? != fingerprint {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox changed after migration backup; stop all writers and retry with a new backup"
                .to_owned(),
        ));
    }
    transaction
        .execute("ALTER TABLE approval_records ADD COLUMN tenant_id TEXT", [])
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    let orphaned_pending_records = match source {
        MigrationSource::SchemaOne => migrate_schema_one_records(&transaction)?,
        MigrationSource::SchemaTwo => migrate_schema_two_records(&transaction)?,
    };
    match source {
        MigrationSource::SchemaOne => transaction
            .execute_batch(&metadata_schema_sql())
            .map_err(|error| HarnessError::Approval(error.to_string()))?,
        MigrationSource::SchemaTwo => {
            let updated = transaction
                .execute(
                    &format!(
                        "UPDATE {APPROVAL_METADATA_TABLE}
                         SET value = ?1
                         WHERE key = 'record_schema' AND value = ?2"
                    ),
                    params![
                        i64::from(APPROVAL_INBOX_SCHEMA_VERSION),
                        i64::from(PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION)
                    ],
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            if updated != 1 {
                return Err(HarnessError::Approval(
                    "SQLite Approval Inbox metadata changed during migration".to_owned(),
                ));
            }
        }
    }
    transaction
        .execute_batch(
            "
            DROP INDEX IF EXISTS approval_pending_order;
            DROP INDEX IF EXISTS approval_turn;
            CREATE INDEX approval_pending_order
                ON approval_records(
                    tenant_id, status, requested_at_ms, approval_id
                );
            CREATE INDEX approval_turn
                ON approval_records(tenant_id, thread_id, turn_id, status);
            ",
        )
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if stop == MigrationStop::BeforeCommit {
        return Err(injected_stop("before commit"));
    }
    transaction
        .commit()
        .map_err(|error| HarnessError::Approval(error.to_string()))?;

    Ok(ApprovalMigrationReport {
        status: ApprovalMigrationStatus::Migrated,
        from_record_schema: source.version(),
        to_record_schema: APPROVAL_INBOX_SCHEMA_VERSION,
        historical_records: fingerprint.record_count,
        orphaned_pending_records,
        required_backup_bytes,
        available_backup_bytes,
        backup_path: Some(backup_path.to_owned()),
    })
}

fn migrate_schema_one_records(transaction: &Transaction<'_>) -> Result<u64, HarnessError> {
    let mut after_id = String::new();
    let mut orphaned = 0_u64;
    loop {
        let page = {
            let mut statement = transaction
                .prepare(
                    "SELECT length(CAST(approval_id AS BLOB)), approval_id,
                            length(CAST(status AS BLOB)), status, revision,
                            length(CAST(record_json AS BLOB)), record_json
                     FROM approval_records
                     WHERE approval_id > ?1
                     ORDER BY approval_id
                     LIMIT ?2",
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            let rows = statement
                .query_map(
                    params![after_id, i64::try_from(MIGRATION_PAGE).unwrap_or(i64::MAX)],
                    |row| {
                        Ok((
                            bounded_text(row, 0, 1, 256, "legacy approval identity")?,
                            bounded_text(row, 2, 3, 8, "legacy approval status")?,
                            row.get::<_, i64>(4)?,
                            bounded_text(
                                row,
                                5,
                                6,
                                LEGACY_MAX_APPROVAL_RECORD_BYTES,
                                "legacy approval record",
                            )?,
                        ))
                    },
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            let mut page = Vec::with_capacity(MIGRATION_PAGE);
            for row in rows {
                page.push(row.map_err(|error| HarnessError::Approval(error.to_string()))?);
            }
            page
        };
        if page.is_empty() {
            return Ok(orphaned);
        }
        for (approval_id, indexed_status, indexed_revision, encoded) in page {
            let legacy = decode_legacy_record(&encoded)?;
            validate_legacy_indexes(&legacy, &approval_id, &indexed_status, indexed_revision)?;
            let (record, was_pending) = convert_record(legacy)?;
            let encoded_current = encode_record(&record)?;
            let updated = transaction
                .execute(
                    "UPDATE approval_records
                     SET status = ?1, revision = ?2, record_json = ?3
                     WHERE approval_id = ?4 AND status = ?5 AND revision = ?6
                           AND record_json = ?7",
                    params![
                        status_name(&record.status),
                        to_i64(record.revision, "approval revision")?,
                        encoded_current,
                        approval_id,
                        indexed_status,
                        indexed_revision,
                        encoded,
                    ],
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            if updated != 1 {
                return Err(HarnessError::Approval(
                    "legacy approval changed during migration".to_owned(),
                ));
            }
            orphaned = orphaned
                .checked_add(u64::from(was_pending))
                .ok_or_else(|| HarnessError::Approval("orphan count overflow".to_owned()))?;
            after_id = approval_id;
        }
    }
}

fn migrate_schema_two_records(transaction: &Transaction<'_>) -> Result<u64, HarnessError> {
    let mut after_id = String::new();
    loop {
        let page = {
            let mut statement = transaction
                .prepare(
                    "SELECT length(CAST(approval_id AS BLOB)), approval_id,
                            length(CAST(status AS BLOB)), status, revision,
                            length(CAST(record_json AS BLOB)), record_json
                     FROM approval_records
                     WHERE approval_id > ?1
                     ORDER BY approval_id
                     LIMIT ?2",
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            let rows = statement
                .query_map(
                    params![after_id, i64::try_from(MIGRATION_PAGE).unwrap_or(i64::MAX)],
                    |row| {
                        Ok((
                            bounded_text(row, 0, 1, 256, "schema-2 approval identity")?,
                            bounded_text(row, 2, 3, 8, "schema-2 approval status")?,
                            row.get::<_, i64>(4)?,
                            bounded_text(
                                row,
                                5,
                                6,
                                super::MAX_APPROVAL_RECORD_BYTES,
                                "schema-2 approval record",
                            )?,
                        ))
                    },
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            let mut page = Vec::with_capacity(MIGRATION_PAGE);
            for row in rows {
                page.push(row.map_err(|error| HarnessError::Approval(error.to_string()))?);
            }
            page
        };
        if page.is_empty() {
            return Ok(0);
        }
        for (approval_id, indexed_status, indexed_revision, encoded) in page {
            let record = decode_schema_two_record(&encoded)?;
            validate_current_indexes(&record, &approval_id, &indexed_status, indexed_revision)?;
            let encoded_current = encode_record(&record)?;
            let updated = transaction
                .execute(
                    "UPDATE approval_records
                     SET record_json = ?1
                     WHERE approval_id = ?2 AND status = ?3 AND revision = ?4
                           AND record_json = ?5",
                    params![
                        encoded_current,
                        approval_id,
                        indexed_status,
                        indexed_revision,
                        encoded,
                    ],
                )
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            if updated != 1 {
                return Err(HarnessError::Approval(
                    "schema-2 approval changed during migration".to_owned(),
                ));
            }
            after_id = approval_id;
        }
    }
}

fn convert_record(legacy: LegacyApprovalRecord) -> Result<(ApprovalRecord, bool), HarnessError> {
    let was_pending = matches!(legacy.status, LegacyApprovalRecordStatus::Pending);
    let (status, revision, settled_at_ms) = match legacy.status {
        LegacyApprovalRecordStatus::Pending => (
            ApprovalRecordStatus::Orphaned {
                reason: MIGRATED_PENDING_REASON.to_owned(),
            },
            legacy
                .revision
                .checked_add(1)
                .ok_or_else(|| HarnessError::Approval("approval revision overflow".to_owned()))?,
            Some(now_ms()),
        ),
        LegacyApprovalRecordStatus::Settled { decision } => (
            ApprovalRecordStatus::Settled {
                decision,
                decided_by: ApprovalActor::UnattributedLegacy,
            },
            legacy.revision,
            legacy.settled_at_ms,
        ),
        LegacyApprovalRecordStatus::Orphaned { reason } => (
            ApprovalRecordStatus::Orphaned { reason },
            legacy.revision,
            legacy.settled_at_ms,
        ),
    };
    Ok((
        ApprovalRecord {
            schema_version: APPROVAL_INBOX_SCHEMA_VERSION,
            request: crate::ApprovalRequest {
                id: legacy.request.id,
                requested_by: ApprovalActor::UnattributedLegacy,
                authorization: legacy.request.authorization,
                reason: legacy.request.reason,
                risk: legacy.request.risk,
            },
            tenant_id: None,
            status,
            revision,
            requested_at_ms: legacy.requested_at_ms,
            settled_at_ms,
        },
        was_pending,
    ))
}

fn decode_schema_two_record(encoded: &str) -> Result<ApprovalRecord, HarnessError> {
    if encoded.len() > super::MAX_APPROVAL_RECORD_BYTES {
        return Err(HarnessError::Approval(format!(
            "schema-2 approval record exceeds {} bytes",
            super::MAX_APPROVAL_RECORD_BYTES
        )));
    }
    let record: SchemaTwoApprovalRecord =
        serde_json::from_str(encoded).map_err(|error| HarnessError::Approval(error.to_string()))?;
    if record.schema_version != PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION {
        return Err(HarnessError::Approval(
            "schema-2 approval has unsupported schema".to_owned(),
        ));
    }
    let current = ApprovalRecord {
        schema_version: APPROVAL_INBOX_SCHEMA_VERSION,
        request: record.request,
        tenant_id: None,
        status: record.status,
        revision: record.revision,
        requested_at_ms: record.requested_at_ms,
        settled_at_ms: record.settled_at_ms,
    };
    encode_record(&current)?;
    Ok(current)
}

fn decode_legacy_record(encoded: &str) -> Result<LegacyApprovalRecord, HarnessError> {
    let record: LegacyApprovalRecord =
        serde_json::from_str(encoded).map_err(|error| HarnessError::Approval(error.to_string()))?;
    validate_legacy_record(&record)?;
    let maximum = if matches!(record.status, LegacyApprovalRecordStatus::Pending) {
        LEGACY_PENDING_RECORD_BYTES
    } else {
        LEGACY_MAX_APPROVAL_RECORD_BYTES
    };
    if encoded.len() > maximum {
        return Err(HarnessError::Approval(format!(
            "legacy approval record exceeds its {maximum}-byte lifecycle limit"
        )));
    }
    Ok(record)
}

fn validate_legacy_record(record: &LegacyApprovalRecord) -> Result<(), HarnessError> {
    if record.schema_version != LEGACY_APPROVAL_SCHEMA_VERSION || record.revision == 0 {
        return Err(HarnessError::Approval(
            "legacy approval has unsupported schema or revision".to_owned(),
        ));
    }
    validate_legacy_text("approval", record.request.id.as_str(), 256)?;
    validate_legacy_text(
        "thread",
        record.request.authorization.thread_id.as_str(),
        256,
    )?;
    validate_legacy_text("turn", record.request.authorization.turn_id.as_str(), 256)?;
    validate_legacy_text("tool call", &record.request.authorization.call_id, 256)?;
    validate_capability_name(
        "legacy approval Tool",
        &record.request.authorization.descriptor.name,
    )?;
    if record
        .request
        .authorization
        .descriptor
        .description
        .trim()
        .is_empty()
    {
        return Err(HarnessError::Approval(
            "legacy approval Tool description is empty".to_owned(),
        ));
    }
    for value in [
        &record.request.authorization.descriptor.input_schema,
        &record.request.authorization.input,
    ] {
        validate_value_shape(value).map_err(|_| {
            HarnessError::Approval(
                "legacy approval JSON exceeds the supported depth or node count".to_owned(),
            )
        })?;
    }
    validate_legacy_reason(
        "Policy reason",
        &record.request.reason,
        MAX_APPROVAL_REASON_BYTES,
    )?;
    match &record.status {
        LegacyApprovalRecordStatus::Pending if record.settled_at_ms.is_none() => {}
        LegacyApprovalRecordStatus::Settled { decision } if record.settled_at_ms.is_some() => {
            if let ApprovalDecision::Deny { reason } = decision {
                validate_legacy_reason("denial reason", reason, MAX_APPROVAL_REASON_BYTES)?;
            }
        }
        LegacyApprovalRecordStatus::Orphaned { reason } if record.settled_at_ms.is_some() => {
            validate_legacy_reason("orphan reason", reason, MAX_APPROVAL_REASON_BYTES)?;
        }
        _ => {
            return Err(HarnessError::Approval(
                "legacy approval status and timestamp disagree".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_legacy_reason(kind: &str, value: &str, maximum: usize) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(HarnessError::Approval(format!(
            "legacy {kind} must be 1-{maximum} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_legacy_text(kind: &str, value: &str, maximum: usize) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(HarnessError::Approval(format!(
            "legacy {kind} must be 1-{maximum} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_legacy_indexes(
    record: &LegacyApprovalRecord,
    approval_id: &str,
    indexed_status: &str,
    indexed_revision: i64,
) -> Result<(), HarnessError> {
    if record.request.id.as_str() != approval_id
        || legacy_status_name(&record.status) != indexed_status
        || to_u64(indexed_revision, "legacy approval revision")? != record.revision
    {
        return Err(HarnessError::Approval(
            "legacy approval indexes do not match its body".to_owned(),
        ));
    }
    Ok(())
}

fn validate_current_indexes(
    record: &ApprovalRecord,
    approval_id: &str,
    indexed_status: &str,
    indexed_revision: i64,
) -> Result<(), HarnessError> {
    if record.request.id.as_str() != approval_id
        || status_name(&record.status) != indexed_status
        || to_u64(indexed_revision, "schema-2 approval revision")? != record.revision
    {
        return Err(HarnessError::Approval(
            "schema-2 approval indexes do not match its body".to_owned(),
        ));
    }
    Ok(())
}

fn legacy_status_name(status: &LegacyApprovalRecordStatus) -> &'static str {
    match status {
        LegacyApprovalRecordStatus::Pending => "pending",
        LegacyApprovalRecordStatus::Settled { .. } => "settled",
        LegacyApprovalRecordStatus::Orphaned { .. } => "orphaned",
    }
}

fn status_name(status: &ApprovalRecordStatus) -> &'static str {
    match status {
        ApprovalRecordStatus::Pending => "pending",
        ApprovalRecordStatus::Settled { .. } => "settled",
        ApprovalRecordStatus::Orphaned { .. } => "orphaned",
    }
}

fn source_store_fingerprint(
    connection: &Connection,
    source: MigrationSource,
) -> Result<StoreFingerprint, HarnessError> {
    let mut statement = connection
        .prepare(
            "SELECT length(CAST(approval_id AS BLOB)), approval_id,
                    length(CAST(thread_id AS BLOB)), thread_id,
                    length(CAST(turn_id AS BLOB)), turn_id,
                    length(CAST(status AS BLOB)), status, revision, requested_at_ms,
                    length(CAST(record_json AS BLOB)), record_json
             FROM approval_records
             ORDER BY approval_id",
        )
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                bounded_text(row, 0, 1, 256, "legacy approval identity")?,
                bounded_text(row, 2, 3, 256, "legacy approval thread")?,
                bounded_text(row, 4, 5, 256, "legacy approval turn")?,
                bounded_text(row, 6, 7, 8, "legacy approval status")?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                bounded_text(
                    row,
                    10,
                    11,
                    super::MAX_APPROVAL_RECORD_BYTES,
                    "source approval record",
                )?,
            ))
        })
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    let mut hasher = Sha256::new();
    let mut count = 0_u64;
    for row in rows {
        let (id, thread, turn, status, revision, requested_at_ms, encoded) =
            row.map_err(|error| HarnessError::Approval(error.to_string()))?;
        match source {
            MigrationSource::SchemaOne => {
                let record = decode_legacy_record(&encoded)?;
                validate_legacy_indexes(&record, &id, &status, revision)?;
                if record.request.authorization.thread_id.as_str() != thread
                    || record.request.authorization.turn_id.as_str() != turn
                    || to_u64(requested_at_ms, "legacy approval timestamp")?
                        != record.requested_at_ms
                {
                    return Err(HarnessError::Approval(
                        "legacy approval indexes do not match its body".to_owned(),
                    ));
                }
            }
            MigrationSource::SchemaTwo => {
                let record = decode_schema_two_record(&encoded)?;
                validate_current_indexes(&record, &id, &status, revision)?;
                if record.request.authorization.thread_id.as_str() != thread
                    || record.request.authorization.turn_id.as_str() != turn
                    || to_u64(requested_at_ms, "schema-2 approval timestamp")?
                        != record.requested_at_ms
                {
                    return Err(HarnessError::Approval(
                        "schema-2 approval indexes do not match its body".to_owned(),
                    ));
                }
            }
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| HarnessError::Approval("approval count overflow".to_owned()))?;
        for value in [&id, &thread, &turn, &status, &encoded] {
            update_fingerprint_text(&mut hasher, value)?;
        }
        hasher.update(revision.to_le_bytes());
        hasher.update(requested_at_ms.to_le_bytes());
    }
    hasher.update(count.to_le_bytes());
    Ok(StoreFingerprint {
        record_count: count,
        records_sha256: hasher.finalize().into(),
    })
}

fn migration_source(connection: &Connection) -> Result<Option<MigrationSource>, HarnessError> {
    if !table_exists(connection, APPROVAL_METADATA_TABLE)? {
        return Ok(Some(MigrationSource::SchemaOne));
    }
    let entries: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM {APPROVAL_METADATA_TABLE}"),
            [],
            |row| row.get(0),
        )
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    let schema = connection
        .query_row(
            &format!("SELECT value FROM {APPROVAL_METADATA_TABLE} WHERE key = 'record_schema'"),
            [],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if entries != 1 {
        return Err(HarnessError::Approval(
            "unsupported SQLite Approval Inbox metadata".to_owned(),
        ));
    }
    match schema {
        Some(value) if value == i64::from(APPROVAL_INBOX_SCHEMA_VERSION) => Ok(None),
        Some(value) if value == i64::from(PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION) => {
            Ok(Some(MigrationSource::SchemaTwo))
        }
        _ => Err(HarnessError::Approval(format!(
            "unsupported SQLite Approval Inbox metadata; expected record schema {APPROVAL_INBOX_SCHEMA_VERSION} or migratable schema {PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION}"
        ))),
    }
}

fn record_count(connection: &Connection) -> Result<u64, HarnessError> {
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM approval_records", [], |row| {
            row.get(0)
        })
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    to_u64(count, "approval record count")
}

fn validate_migration_paths(path: &Path, backup_path: &Path) -> Result<(), HarnessError> {
    if !path.is_file() {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox migration source must be an existing file".to_owned(),
        ));
    }
    if path == backup_path {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox migration backup must differ from the source".to_owned(),
        ));
    }
    if backup_path.to_str().is_none() {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox backup path must be valid UTF-8".to_owned(),
        ));
    }
    let parent = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if parent.is_some_and(|parent| !parent.is_dir()) {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox backup parent must already exist".to_owned(),
        ));
    }
    Ok(())
}

fn configure_connection(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    connection
        .execute_batch("PRAGMA synchronous = FULL;")
        .map_err(|error| HarnessError::Approval(error.to_string()))
}

fn migration_space_preflight(
    connection: &Connection,
    backup_path: &Path,
) -> Result<(u64, u64), HarnessError> {
    let used_pages = pragma_u64(connection, "page_count")?
        .saturating_sub(pragma_u64(connection, "freelist_count")?);
    let required_backup_bytes = if backup_path.exists() {
        0
    } else {
        used_pages
            .checked_mul(pragma_u64(connection, "page_size")?)
            .and_then(|bytes| bytes.checked_add(MIN_MIGRATION_WORKING_BYTES))
            .ok_or_else(|| {
                HarnessError::Approval("approval migration disk requirement overflow".to_owned())
            })?
    };
    let backup_probe = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let available_backup_bytes = fs2::available_space(backup_probe)
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if available_backup_bytes < required_backup_bytes {
        return Err(HarnessError::Approval(format!(
            "SQLite Approval Inbox migration requires {required_backup_bytes} backup bytes, found {available_backup_bytes}"
        )));
    }
    let source_probe = connection
        .path()
        .map(Path::new)
        .and_then(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let source_available = fs2::available_space(source_probe)
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if source_available < MIN_MIGRATION_WORKING_BYTES {
        return Err(HarnessError::Approval(format!(
            "SQLite Approval Inbox migration requires {MIN_MIGRATION_WORKING_BYTES} working bytes on the source filesystem, found {source_available}"
        )));
    }
    Ok((required_backup_bytes, available_backup_bytes))
}

fn create_or_validate_backup(
    source_path: &Path,
    backup_path: &Path,
    source_schema: MigrationSource,
    fingerprint: StoreFingerprint,
) -> Result<(), HarnessError> {
    if backup_path.exists() {
        return validate_backup(backup_path, source_schema, fingerprint);
    }
    let partial_path = partial_backup_path(backup_path);
    let source =
        Connection::open(source_path).map_err(|error| HarnessError::Approval(error.to_string()))?;
    configure_connection(&source)?;
    let partial_text = partial_path.to_str().ok_or_else(|| {
        HarnessError::Approval("approval migration partial path is not UTF-8".to_owned())
    })?;
    source
        .execute("VACUUM INTO ?1", [partial_text])
        .map_err(|error| {
            HarnessError::Approval(format!("cannot create approval backup: {error}"))
        })?;
    drop(source);

    let backup = Connection::open(&partial_path)
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    backup
        .execute_batch("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;")
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    backup
        .execute_batch(&format!(
            "CREATE TABLE {BACKUP_MANIFEST_TABLE} (
                id                 INTEGER PRIMARY KEY CHECK(id = 1),
                from_record_schema INTEGER NOT NULL,
                to_record_schema   INTEGER NOT NULL,
                record_count       INTEGER NOT NULL,
                records_sha256     TEXT NOT NULL CHECK(length(records_sha256) = 64)
            );"
        ))
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    backup
        .execute(
            &format!(
                "INSERT INTO {BACKUP_MANIFEST_TABLE}
                    (id, from_record_schema, to_record_schema, record_count, records_sha256)
                 VALUES (1, ?1, ?2, ?3, ?4)"
            ),
            params![
                i64::from(source_schema.version()),
                i64::from(APPROVAL_INBOX_SCHEMA_VERSION),
                to_i64(fingerprint.record_count, "approval count")?,
                fingerprint_hex(&fingerprint.records_sha256),
            ],
        )
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    drop(backup);
    // Windows FlushFileBuffers requires a writable handle.
    File::options()
        .read(true)
        .write(true)
        .open(&partial_path)
        .and_then(|file| file.sync_all())
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    fs::hard_link(&partial_path, backup_path).map_err(|error| {
        HarnessError::Approval(format!(
            "cannot publish approval backup without overwriting an existing path: {error}"
        ))
    })?;
    sync_parent_directory(backup_path)?;
    fs::remove_file(&partial_path).map_err(|error| HarnessError::Approval(error.to_string()))?;
    sync_parent_directory(backup_path)?;
    validate_backup(backup_path, source_schema, fingerprint)
}

fn validate_backup(
    backup_path: &Path,
    source_schema: MigrationSource,
    expected: StoreFingerprint,
) -> Result<(), HarnessError> {
    if !backup_path.is_file() {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox backup is not a regular file".to_owned(),
        ));
    }
    let backup = Connection::open_with_flags(backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    let integrity: String = backup
        .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    if integrity != "ok" {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox backup failed integrity_check".to_owned(),
        ));
    }
    let manifest = backup
        .query_row(
            &format!(
                "SELECT from_record_schema, to_record_schema, record_count,
                        length(CAST(records_sha256 AS BLOB)), records_sha256
                 FROM {BACKUP_MANIFEST_TABLE} WHERE id = 1"
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    bounded_text(row, 3, 4, 64, "approval backup fingerprint")?,
                ))
            },
        )
        .optional()
        .map_err(|error| HarnessError::Approval(error.to_string()))?
        .ok_or_else(|| {
            HarnessError::Approval("SQLite Approval Inbox backup has no manifest".to_owned())
        })?;
    if manifest.0 != i64::from(source_schema.version())
        || manifest.1 != i64::from(APPROVAL_INBOX_SCHEMA_VERSION)
        || to_u64(manifest.2, "backup approval count")? != expected.record_count
        || manifest.3 != fingerprint_hex(&expected.records_sha256)
        || source_store_fingerprint(&backup, source_schema)? != expected
    {
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox backup does not match the source preflight".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<(), HarnessError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| HarnessError::Approval(error.to_string()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<(), HarnessError> {
    Ok(())
}

fn pragma_u64(connection: &Connection, pragma: &str) -> Result<u64, HarnessError> {
    let value: i64 = connection
        .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .map_err(|error| HarnessError::Approval(error.to_string()))?;
    to_u64(value, pragma)
}

fn to_u64(value: i64, kind: &str) -> Result<u64, HarnessError> {
    u64::try_from(value).map_err(|_| HarnessError::Approval(format!("negative {kind}")))
}

fn to_i64(value: u64, kind: &str) -> Result<i64, HarnessError> {
    i64::try_from(value)
        .map_err(|_| HarnessError::Approval(format!("{kind} exceeds SQLite INTEGER")))
}

fn update_fingerprint_text(hasher: &mut Sha256, value: &str) -> Result<(), HarnessError> {
    let length = u64::try_from(value.len())
        .map_err(|_| HarnessError::Approval("approval fingerprint length overflow".to_owned()))?;
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
    value.push(format!(".partial-{}", ApprovalId::generate()));
    PathBuf::from(value)
}

fn injected_stop(phase: &str) -> HarnessError {
    HarnessError::Approval(format!("injected approval migration stop {phase}"))
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
) -> Result<ApprovalMigrationReport, HarnessError> {
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
    use std::{path::PathBuf, time::Instant};

    use rusqlite::{Connection, OptionalExtension, params};
    use serde_json::json;

    use super::{
        APPROVAL_METADATA_TABLE, ApprovalMigrationStatus, BACKUP_MANIFEST_TABLE,
        LEGACY_APPROVAL_SCHEMA_VERSION, LEGACY_MAX_APPROVAL_RECORD_BYTES,
        LEGACY_PENDING_RECORD_BYTES, MIGRATED_PENDING_REASON,
        PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION, migrate_with_stop,
    };
    use crate::{
        ApprovalActor, ApprovalId, ApprovalInbox, ApprovalRecordStatus, SqliteApprovalInbox,
    };

    #[tokio::test]
    async fn migration_backs_up_v1_and_preserves_terminal_evidence() {
        let source = fixture_path("source");
        let backup = fixture_path("backup");
        create_legacy_database(&source, 3, 0);

        let open_error = match SqliteApprovalInbox::open(&source).await {
            Ok(_) => panic!("populated legacy inbox must not open"),
            Err(error) => error,
        };
        assert!(open_error.to_string().contains("approval-migrate"));

        let report = SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect("migrate");
        assert_eq!(report.status, ApprovalMigrationStatus::Migrated);
        assert_eq!(report.from_record_schema, LEGACY_APPROVAL_SCHEMA_VERSION);
        assert_eq!(
            report.to_record_schema,
            crate::APPROVAL_INBOX_SCHEMA_VERSION
        );
        assert_eq!(report.historical_records, 3);
        assert_eq!(report.orphaned_pending_records, 1);
        assert_eq!(report.backup_path.as_deref(), Some(backup.as_path()));

        let inbox = SqliteApprovalInbox::open(&source)
            .await
            .expect("open migrated inbox");
        let pending = inbox
            .get(&ApprovalId::from_string("approval-000".to_owned()))
            .await
            .expect("read pending")
            .expect("pending record");
        assert!(matches!(
            pending.status,
            ApprovalRecordStatus::Orphaned { ref reason }
                if reason == MIGRATED_PENDING_REASON
        ));
        assert_eq!(
            pending.request.requested_by,
            ApprovalActor::UnattributedLegacy
        );
        let settled = inbox
            .get(&ApprovalId::from_string("approval-001".to_owned()))
            .await
            .expect("read settled")
            .expect("settled record");
        assert!(matches!(
            settled.status,
            ApprovalRecordStatus::Settled {
                decided_by: ApprovalActor::UnattributedLegacy,
                ..
            }
        ));

        let backup_connection = Connection::open(&backup).expect("open backup");
        let legacy_schema: i64 = backup_connection
            .query_row(
                "SELECT json_extract(record_json, '$.schema_version')
                 FROM approval_records WHERE approval_id = 'approval-000'",
                [],
                |row| row.get(0),
            )
            .expect("legacy body");
        assert_eq!(legacy_schema, i64::from(LEGACY_APPROVAL_SCHEMA_VERSION));
        assert!(table(&backup_connection, BACKUP_MANIFEST_TABLE));
        assert!(!table(&backup_connection, APPROVAL_METADATA_TABLE));
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn migration_preserves_schema_two_records_as_explicitly_unscoped() {
        let source = fixture_path("schema-two-source");
        let backup = fixture_path("schema-two-backup");
        create_schema_two_database(&source);

        let open_error = match SqliteApprovalInbox::open(&source).await {
            Ok(_) => panic!("schema-2 inbox must require migration"),
            Err(error) => error,
        };
        assert!(open_error.to_string().contains("approval-migrate"));

        let report = SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect("migrate schema two");
        assert_eq!(report.status, ApprovalMigrationStatus::Migrated);
        assert_eq!(
            report.from_record_schema,
            PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION
        );
        assert_eq!(
            report.to_record_schema,
            crate::APPROVAL_INBOX_SCHEMA_VERSION
        );
        assert_eq!(report.historical_records, 1);
        assert_eq!(report.orphaned_pending_records, 0);

        let inbox = SqliteApprovalInbox::open(&source)
            .await
            .expect("open migrated schema-two inbox");
        let record = inbox
            .get(&ApprovalId::from_string("approval-v2".to_owned()))
            .await
            .expect("read migrated approval")
            .expect("approval");
        assert!(matches!(record.status, ApprovalRecordStatus::Pending));
        assert_eq!(record.tenant_id(), None);

        let backup_connection = Connection::open(&backup).expect("open backup");
        let backup_schema: i64 = backup_connection
            .query_row(
                &format!(
                    "SELECT value FROM {APPROVAL_METADATA_TABLE}
                     WHERE key = 'record_schema'"
                ),
                [],
                |row| row.get(0),
            )
            .expect("backup schema");
        assert_eq!(
            backup_schema,
            i64::from(PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION)
        );
        assert!(!column(&backup_connection, "approval_records", "tenant_id"));

        let source_connection = Connection::open(&source).expect("open source");
        assert!(column(&source_connection, "approval_records", "tenant_id"));
        let tenant: Option<String> = source_connection
            .query_row(
                "SELECT tenant_id FROM approval_records
                 WHERE approval_id = 'approval-v2'",
                [],
                |row| row.get(0),
            )
            .expect("tenant projection");
        assert_eq!(tenant, None);
        cleanup(&source);
        cleanup(&backup);
    }

    #[tokio::test]
    async fn migration_restarts_after_every_mutating_phase() {
        for phase in ["after_preflight", "after_backup", "before_commit"] {
            let source = fixture_path(&format!("{phase}-source"));
            let backup = fixture_path(&format!("{phase}-backup"));
            create_legacy_database(&source, 3, 0);
            migrate_with_stop(&source, &backup, phase).expect_err("injected stop");

            let report = SqliteApprovalInbox::migrate(&source, &backup)
                .await
                .expect("resume migration");
            assert_eq!(report.status, ApprovalMigrationStatus::Migrated);
            SqliteApprovalInbox::open(&source)
                .await
                .expect("reopen migrated inbox");
            cleanup(&source);
            cleanup(&backup);
        }
    }

    #[tokio::test]
    async fn schema_two_migration_restarts_after_every_mutating_phase() {
        for phase in ["after_preflight", "after_backup", "before_commit"] {
            let source = fixture_path(&format!("{phase}-schema-two-source"));
            let backup = fixture_path(&format!("{phase}-schema-two-backup"));
            create_schema_two_database(&source);
            migrate_with_stop(&source, &backup, phase).expect_err("injected stop");

            let report = SqliteApprovalInbox::migrate(&source, &backup)
                .await
                .expect("resume schema-two migration");
            assert_eq!(report.status, ApprovalMigrationStatus::Migrated);
            assert_eq!(
                report.from_record_schema,
                PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION
            );
            SqliteApprovalInbox::open(&source)
                .await
                .expect("reopen migrated schema-two inbox");
            cleanup(&source);
            cleanup(&backup);
        }
    }

    #[tokio::test]
    async fn current_store_is_idempotent_without_creating_backup() {
        let source = fixture_path("current-source");
        let backup = fixture_path("unused-backup");
        let inbox = SqliteApprovalInbox::open(&source)
            .await
            .expect("create current inbox");
        drop(inbox);

        let report = SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect("current migration");
        assert_eq!(report.status, ApprovalMigrationStatus::AlreadyCurrent);
        assert!(report.backup_path.is_none());
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn unknown_legacy_schema_fails_before_backup_creation() {
        let source = fixture_path("unknown-source");
        let backup = fixture_path("unknown-backup");
        create_legacy_database(&source, 1, 0);
        let connection = Connection::open(&source).expect("open source");
        connection
            .execute(
                "UPDATE approval_records
                 SET record_json = json_set(record_json, '$.schema_version', 99)",
                [],
            )
            .expect("corrupt schema");
        drop(connection);

        SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect_err("unknown schema");
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn invalid_legacy_lifecycle_size_fails_before_backup_creation() {
        let source = fixture_path("oversized-pending-source");
        let backup = fixture_path("oversized-pending-backup");
        create_legacy_database(&source, 1, LEGACY_PENDING_RECORD_BYTES + 1);

        let error = SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect_err("invalid pending lifecycle size");
        assert!(error.to_string().contains("lifecycle limit"));
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn deeply_nested_legacy_json_fails_before_backup_creation() {
        let source = fixture_path("deep-source");
        let backup = fixture_path("deep-backup");
        create_legacy_database(&source, 1, 0);
        let connection = Connection::open(&source).expect("open source");
        let encoded: String = connection
            .query_row("SELECT record_json FROM approval_records", [], |row| {
                row.get(0)
            })
            .expect("legacy record");
        let mut record: serde_json::Value = serde_json::from_str(&encoded).expect("decode fixture");
        let mut nested = serde_json::Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            nested = serde_json::Value::Array(vec![nested]);
        }
        record["request"]["authorization"]["input"] = nested;
        connection
            .execute(
                "UPDATE approval_records SET record_json = ?1",
                [serde_json::to_string(&record).expect("encode deep record")],
            )
            .expect("update deep record");
        drop(connection);

        let error = SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect_err("deep legacy JSON");
        assert!(error.to_string().contains("depth or node count"));
        assert!(!backup.exists());
        cleanup(&source);
    }

    #[tokio::test]
    async fn migration_never_replaces_an_existing_backup_path() {
        let source = fixture_path("occupied-source");
        let backup = fixture_path("occupied-backup");
        create_legacy_database(&source, 1, 0);
        std::fs::write(&backup, b"operator-owned backup").expect("occupy backup");

        SqliteApprovalInbox::migrate(&source, &backup)
            .await
            .expect_err("occupied backup is not reusable");
        assert_eq!(
            std::fs::read(&backup).expect("read occupied backup"),
            b"operator-owned backup"
        );
        cleanup(&source);
        cleanup(&backup);
    }

    #[test]
    #[ignore = "manual maximum-size Approval Inbox migration performance evidence"]
    fn migrates_largest_supported_turn_fixture() {
        let source = fixture_path("largest-source");
        let backup = fixture_path("largest-backup");
        create_legacy_database(&source, 256, 519_680);
        let started = Instant::now();
        let report = migrate_with_stop(&source, &backup, "none").expect("migrate largest inbox");
        assert_eq!(report.historical_records, 256);
        assert_eq!(report.orphaned_pending_records, 86);
        eprintln!(
            "migrated {} approval records in {:.3} ms",
            report.historical_records,
            started.elapsed().as_secs_f64() * 1_000.0
        );
        cleanup(&source);
        cleanup(&backup);
    }

    fn create_legacy_database(path: &PathBuf, records: usize, target_bytes: usize) {
        let connection = Connection::open(path).expect("create legacy database");
        connection
            .execute_batch(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE approval_records (
                    approval_id     TEXT PRIMARY KEY,
                    thread_id       TEXT NOT NULL,
                    turn_id         TEXT NOT NULL,
                    status          TEXT NOT NULL
                                    CHECK(status IN ('pending', 'settled', 'orphaned')),
                    revision        INTEGER NOT NULL CHECK(revision > 0),
                    requested_at_ms INTEGER NOT NULL,
                    record_json     TEXT NOT NULL
                );
                CREATE INDEX approval_pending_order
                    ON approval_records(status, requested_at_ms, approval_id);
                CREATE INDEX approval_turn
                    ON approval_records(thread_id, turn_id, status);
                ",
            )
            .expect("legacy schema");
        for index in 0..records {
            let status = match index % 3 {
                0 => json!({"status": "pending"}),
                1 => json!({
                    "status": "settled",
                    "decision": {"action": "approve"}
                }),
                _ => json!({
                    "status": "orphaned",
                    "reason": "originating runtime stopped"
                }),
            };
            let indexed_status = match index % 3 {
                0 => "pending",
                1 => "settled",
                _ => "orphaned",
            };
            let settled_at_ms = if index % 3 == 0 {
                serde_json::Value::Null
            } else {
                json!(2)
            };
            let mut record = json!({
                "schema_version": LEGACY_APPROVAL_SCHEMA_VERSION,
                "request": {
                    "id": format!("approval-{index:03}"),
                    "authorization": {
                        "thread_id": "thread-1",
                        "turn_id": "turn-1",
                        "call_id": format!("call-{index:03}"),
                        "descriptor": {
                            "name": "deploy",
                            "description": "deploy one bounded artifact",
                            "input_schema": {"type": "object"}
                        },
                        "origin": {"kind": "built_in"},
                        "input": {"padding": ""}
                    },
                    "reason": "deployment changes external state",
                    "risk": "high"
                },
                "status": status,
                "revision": 1,
                "requested_at_ms": 1,
                "settled_at_ms": settled_at_ms
            });
            if target_bytes > 0 {
                let base = serde_json::to_vec(&record).expect("encode base").len();
                let padding = target_bytes.checked_sub(base).expect("target fits base");
                record["request"]["authorization"]["input"]["padding"] = json!("x".repeat(padding));
            }
            let encoded = serde_json::to_string(&record).expect("encode legacy");
            assert!(encoded.len() <= LEGACY_MAX_APPROVAL_RECORD_BYTES);
            if target_bytes > 0 {
                assert_eq!(encoded.len(), target_bytes);
            }
            connection
                .execute(
                    "INSERT INTO approval_records
                        (approval_id, thread_id, turn_id, status, revision,
                         requested_at_ms, record_json)
                     VALUES (?1, 'thread-1', 'turn-1', ?2, 1, 1, ?3)",
                    params![format!("approval-{index:03}"), indexed_status, encoded],
                )
                .expect("insert legacy record");
        }
    }

    fn create_schema_two_database(path: &PathBuf) {
        let connection = Connection::open(path).expect("create schema-two database");
        connection
            .execute_batch(&format!(
                "
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE approval_records (
                    approval_id     TEXT PRIMARY KEY,
                    thread_id       TEXT NOT NULL,
                    turn_id         TEXT NOT NULL,
                    status          TEXT NOT NULL
                                    CHECK(status IN ('pending', 'settled', 'orphaned')),
                    revision        INTEGER NOT NULL CHECK(revision > 0),
                    requested_at_ms INTEGER NOT NULL,
                    record_json     TEXT NOT NULL
                );
                CREATE INDEX approval_pending_order
                    ON approval_records(status, requested_at_ms, approval_id);
                CREATE INDEX approval_turn
                    ON approval_records(thread_id, turn_id, status);
                CREATE TABLE {APPROVAL_METADATA_TABLE} (
                    key   TEXT PRIMARY KEY,
                    value INTEGER NOT NULL CHECK(value > 0)
                );
                INSERT INTO {APPROVAL_METADATA_TABLE} (key, value)
                    VALUES ('record_schema', {PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION});
                "
            ))
            .expect("schema-two schema");
        let record = json!({
            "schema_version": PREVIOUS_APPROVAL_INBOX_SCHEMA_VERSION,
            "request": {
                "id": "approval-v2",
                "requested_by": {
                    "kind": "authenticated",
                    "authority": "fixture",
                    "subject": "requester"
                },
                "authorization": {
                    "thread_id": "thread-v2",
                    "turn_id": "turn-v2",
                    "call_id": "call-v2",
                    "descriptor": {
                        "name": "deploy",
                        "description": "deploy one bounded artifact",
                        "input_schema": {"type": "object"}
                    },
                    "origin": {"kind": "built_in"},
                    "input": {}
                },
                "reason": "deployment changes external state",
                "risk": "high"
            },
            "status": {"status": "pending"},
            "revision": 1,
            "requested_at_ms": 1,
            "settled_at_ms": null
        });
        connection
            .execute(
                "INSERT INTO approval_records
                    (approval_id, thread_id, turn_id, status, revision,
                     requested_at_ms, record_json)
                 VALUES ('approval-v2', 'thread-v2', 'turn-v2', 'pending', 1, 1, ?1)",
                [serde_json::to_string(&record).expect("encode schema-two record")],
            )
            .expect("insert schema-two record");
    }

    fn table(connection: &Connection, name: &str) -> bool {
        connection
            .query_row(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
                [name],
                |_| Ok(()),
            )
            .optional()
            .expect("query table")
            .is_some()
    }

    fn column(connection: &Connection, table: &str, column: &str) -> bool {
        let mut statement = connection
            .prepare(&format!("PRAGMA table_info({table})"))
            .expect("prepare columns");
        let rows = statement
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query columns");
        rows.filter_map(Result::ok).any(|name| name == column)
    }

    fn fixture_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-approval-migration-{label}-{}-{}.db",
            std::process::id(),
            ApprovalId::generate()
        ))
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
