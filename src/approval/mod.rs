//! Durable approval inboxes and polling approval-handler adapter.

mod migration;

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task, time};

pub use migration::{ApprovalMigrationReport, ApprovalMigrationStatus};

use crate::{
    ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, HarnessError, HarnessFuture,
    ThreadId, TurnId,
    json::{BoundedJsonError, bounded_serialized_size, to_bounded_json_vec, validate_value_shape},
    kernel::{now_ms, validate_capability_name},
    runtime::ApprovalHandler,
    sqlite::bounded_text,
};

/// Current durable Approval Inbox record schema.
pub const APPROVAL_INBOX_SCHEMA_VERSION: u32 = 2;
// One record must fit comfortably inside the 1 MiB reference-protocol frame
// together with its response envelope.
const MAX_APPROVAL_RECORD_BYTES: usize = 525_312;
const MAX_APPROVAL_REASON_BYTES: usize = 4_096;
// Covers one maximum denial/orphan reason plus status, revision, and timestamp
// growth. Tests pin the worst supported terminal forms at the pending ceiling.
const APPROVAL_TERMINAL_RECORD_RESERVE_BYTES: usize = MAX_APPROVAL_REASON_BYTES + 1_024;
// Sixteen maximum pending records retain less than 8 MiB of record bodies.
const MAX_APPROVAL_PAGE: usize = 16;
const _: () = assert!(
    MAX_APPROVAL_PAGE * (MAX_APPROVAL_RECORD_BYTES - APPROVAL_TERMINAL_RECORD_RESERVE_BYTES)
        < 8_388_608
);
const MAX_APPROVALS_PER_TURN: usize = 256;
const MIN_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(60);
const APPROVAL_METADATA_TABLE: &str = "approval_inbox_metadata";

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
/// Durable lifecycle of one approval request.
pub enum ApprovalRecordStatus {
    /// No authorized settler has supplied a decision.
    Pending,
    /// The request has one immutable approval settlement.
    Settled {
        /// Human or delegated approval decision.
        decision: ApprovalDecision,
        /// Authenticated identity that supplied the decision.
        decided_by: ApprovalActor,
    },
    /// The originating execution can no longer consume a decision.
    Orphaned {
        /// Bounded operational explanation.
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Revisioned durable approval request and its settlement state.
pub struct ApprovalRecord {
    /// Approval inbox schema used by this record.
    pub schema_version: u32,
    /// Fully correlated request submitted by Runtime.
    pub request: ApprovalRequest,
    /// Current durable lifecycle.
    pub status: ApprovalRecordStatus,
    /// Optimistic concurrency revision, beginning at one.
    pub revision: u64,
    /// Initial submission time in Unix milliseconds.
    pub requested_at_ms: u64,
    /// Terminal settlement or orphaning time.
    pub settled_at_ms: Option<u64>,
}

/// Durable, revisioned settlement authority for approval requests.
pub trait ApprovalInbox: Send + Sync {
    /// Idempotently creates one pending request.
    fn submit<'a>(&'a self, request: ApprovalRequest) -> HarnessFuture<'a, ApprovalRecord>;

    /// Loads one request by identity.
    fn get<'a>(&'a self, approval_id: &'a ApprovalId) -> HarnessFuture<'a, Option<ApprovalRecord>>;

    /// Returns the oldest pending records within a hard page bound.
    fn pending<'a>(&'a self, limit: usize) -> HarnessFuture<'a, Vec<ApprovalRecord>>;

    /// Atomically settles a pending record at the observed revision.
    ///
    /// The trusted host supplies `decided_by`; the Inbox validates it and
    /// rejects equality with the immutable requester before committing.
    fn settle<'a>(
        &'a self,
        approval_id: &'a ApprovalId,
        expected_revision: u64,
        decision: ApprovalDecision,
        decided_by: ApprovalActor,
    ) -> HarnessFuture<'a, ApprovalRecord>;

    /// Marks pending requests for an abandoned Turn as non-actionable.
    fn orphan_turn<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        reason: &'a str,
    ) -> HarnessFuture<'a, usize>;
}

#[derive(Default)]
/// In-memory Approval Inbox with production-equivalent CAS behavior.
pub struct MemoryApprovalInbox {
    records: Mutex<BTreeMap<ApprovalId, ApprovalRecord>>,
}

impl MemoryApprovalInbox {
    /// Creates an empty inbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl ApprovalInbox for MemoryApprovalInbox {
    fn submit<'a>(&'a self, request: ApprovalRequest) -> HarnessFuture<'a, ApprovalRecord> {
        Box::pin(async move {
            validate_current_request(&request)?;
            let mut records = self.records.lock().await;
            if let Some(existing) = records.get(&request.id) {
                return matching_request(existing, &request);
            }
            enforce_turn_capacity(records.values(), &request)?;
            let record = new_record(request);
            validate_record(&record)?;
            records.insert(record.request.id.clone(), record.clone());
            Ok(record)
        })
    }

    fn get<'a>(&'a self, approval_id: &'a ApprovalId) -> HarnessFuture<'a, Option<ApprovalRecord>> {
        Box::pin(async move {
            validate_identity("approval", approval_id.as_str())?;
            Ok(self.records.lock().await.get(approval_id).cloned())
        })
    }

    fn pending<'a>(&'a self, limit: usize) -> HarnessFuture<'a, Vec<ApprovalRecord>> {
        Box::pin(async move {
            validate_page(limit)?;
            let records = self.records.lock().await;
            let mut selected: Vec<&ApprovalRecord> = Vec::with_capacity(limit);
            for record in records
                .values()
                .filter(|record| matches!(record.status, ApprovalRecordStatus::Pending))
            {
                let position = selected
                    .binary_search_by(|existing| {
                        (existing.requested_at_ms, &existing.request.id)
                            .cmp(&(record.requested_at_ms, &record.request.id))
                    })
                    .unwrap_or_else(|position| position);
                if position < limit {
                    selected.insert(position, record);
                    if selected.len() > limit {
                        selected.pop();
                    }
                }
            }
            Ok(selected.into_iter().cloned().collect())
        })
    }

    fn settle<'a>(
        &'a self,
        approval_id: &'a ApprovalId,
        expected_revision: u64,
        decision: ApprovalDecision,
        decided_by: ApprovalActor,
    ) -> HarnessFuture<'a, ApprovalRecord> {
        Box::pin(async move {
            validate_identity("approval", approval_id.as_str())?;
            validate_decision(&decision)?;
            validate_current_actor("approval settler", &decided_by)?;
            let mut records = self.records.lock().await;
            let record = records.get_mut(approval_id).ok_or_else(|| {
                HarnessError::Approval(format!("approval {approval_id} does not exist"))
            })?;
            settle_record(record, expected_revision, decision, decided_by)
        })
    }

    fn orphan_turn<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        reason: &'a str,
    ) -> HarnessFuture<'a, usize> {
        Box::pin(async move {
            validate_identity("thread", thread_id.as_str())?;
            validate_identity("turn", turn_id.as_str())?;
            validate_reason("orphan", reason)?;
            let mut records = self.records.lock().await;
            let mut changed = 0_usize;
            for record in records.values_mut().filter(|record| {
                record.request.authorization.thread_id == *thread_id
                    && record.request.authorization.turn_id == *turn_id
                    && matches!(record.status, ApprovalRecordStatus::Pending)
            }) {
                orphan_record(record, reason)?;
                changed = changed.saturating_add(1);
            }
            Ok(changed)
        })
    }
}

/// SQLite-backed durable Approval Inbox.
pub struct SqliteApprovalInbox {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteApprovalInbox {
    /// Opens or creates an inbox with WAL and full synchronous durability.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_owned();
        let connection = task::spawn_blocking(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
            }
            let connection = Connection::open(path)
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            let mode: String = connection
                .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(HarnessError::Approval(format!(
                    "SQLite refused WAL mode and selected {mode}"
                )));
            }
            validate_or_bootstrap_store(&connection)?;
            connection
                .execute_batch(&format!(
                    "
                    PRAGMA synchronous = FULL;
                    CREATE TABLE IF NOT EXISTS approval_records (
                        approval_id    TEXT PRIMARY KEY,
                        thread_id      TEXT NOT NULL,
                        turn_id        TEXT NOT NULL,
                        status         TEXT NOT NULL
                                       CHECK(status IN ('pending', 'settled', 'orphaned')),
                        revision       INTEGER NOT NULL CHECK(revision > 0),
                        requested_at_ms INTEGER NOT NULL,
                        record_json    TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS approval_pending_order
                        ON approval_records(status, requested_at_ms, approval_id);
                    CREATE INDEX IF NOT EXISTS approval_turn
                        ON approval_records(thread_id, turn_id, status);
                    {}
                    ",
                    metadata_schema_sql()
                ))
                .map_err(|error| HarnessError::Approval(error.to_string()))?;
            validate_current_metadata(&connection)?;
            Ok(connection)
        })
        .await
        .map_err(|error| HarnessError::Approval(format!("SQLite task failed: {error}")))??;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }

    async fn with_connection<T, F>(&self, operation: F) -> Result<T, HarnessError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, HarnessError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        task::spawn_blocking(move || {
            let mut connection = connection.lock().map_err(|_| {
                HarnessError::Approval("SQLite connection lock poisoned".to_owned())
            })?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| HarnessError::Approval(format!("SQLite task failed: {error}")))?
    }
}

impl ApprovalInbox for SqliteApprovalInbox {
    fn submit<'a>(&'a self, request: ApprovalRequest) -> HarnessFuture<'a, ApprovalRecord> {
        Box::pin(async move {
            validate_current_request(&request)?;
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                if let Some(indexed) = transaction
                    .query_row(
                        "SELECT length(CAST(status AS BLOB)), status, revision,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                length(CAST(turn_id AS BLOB)), turn_id,
                                length(CAST(record_json AS BLOB)), record_json
                         FROM approval_records WHERE approval_id = ?1",
                        [request.id.as_str()],
                        read_indexed_record,
                    )
                    .optional()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?
                {
                    return matching_request(&decode_indexed_record(indexed)?, &request);
                }
                let count: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM approval_records
                         WHERE thread_id = ?1 AND turn_id = ?2",
                        params![
                            request.authorization.thread_id.as_str(),
                            request.authorization.turn_id.as_str()
                        ],
                        |row| row.get(0),
                    )
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                if usize::try_from(count).unwrap_or(usize::MAX) >= MAX_APPROVALS_PER_TURN {
                    return Err(HarnessError::Approval(format!(
                        "Turn exceeds {MAX_APPROVALS_PER_TURN} approval records"
                    )));
                }

                let record = new_record(request);
                let encoded = encode_record(&record)?;
                transaction
                    .execute(
                        "INSERT INTO approval_records
                            (approval_id, thread_id, turn_id, status, revision,
                             requested_at_ms, record_json)
                         VALUES (?1, ?2, ?3, 'pending', ?4, ?5, ?6)",
                        params![
                            record.request.id.as_str(),
                            record.request.authorization.thread_id.as_str(),
                            record.request.authorization.turn_id.as_str(),
                            to_sql_u64("approval revision", record.revision)?,
                            to_sql_u64("approval timestamp", record.requested_at_ms)?,
                            encoded
                        ],
                    )
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                Ok(record)
            })
            .await
        })
    }

    fn get<'a>(&'a self, approval_id: &'a ApprovalId) -> HarnessFuture<'a, Option<ApprovalRecord>> {
        let approval_id = approval_id.clone();
        Box::pin(async move {
            validate_identity("approval", approval_id.as_str())?;
            self.with_connection(move |connection| {
                connection
                    .query_row(
                        "SELECT length(CAST(status AS BLOB)), status, revision,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                length(CAST(turn_id AS BLOB)), turn_id,
                                length(CAST(record_json AS BLOB)), record_json
                         FROM approval_records WHERE approval_id = ?1",
                        [approval_id.as_str()],
                        read_indexed_record,
                    )
                    .optional()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?
                    .map(decode_indexed_record)
                    .transpose()
            })
            .await
        })
    }

    fn pending<'a>(&'a self, limit: usize) -> HarnessFuture<'a, Vec<ApprovalRecord>> {
        Box::pin(async move {
            validate_page(limit)?;
            self.with_connection(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT length(CAST(status AS BLOB)), status, revision,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                length(CAST(turn_id AS BLOB)), turn_id,
                                length(CAST(record_json AS BLOB)), record_json
                         FROM approval_records
                         WHERE status = 'pending'
                         ORDER BY requested_at_ms, approval_id
                         LIMIT ?1",
                    )
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                let rows = statement
                    .query_map(
                        [i64::try_from(limit).map_err(|_| {
                            HarnessError::Approval(
                                "approval page exceeds SQLite INTEGER".to_owned(),
                            )
                        })?],
                        read_indexed_record,
                    )
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                let mut records = Vec::new();
                for row in rows {
                    let indexed = row.map_err(|error| HarnessError::Approval(error.to_string()))?;
                    let record = decode_indexed_record(indexed)?;
                    if !matches!(record.status, ApprovalRecordStatus::Pending) {
                        return Err(HarnessError::Approval(
                            "pending approval index returned a terminal record".to_owned(),
                        ));
                    }
                    records.push(record);
                }
                Ok(records)
            })
            .await
        })
    }

    fn settle<'a>(
        &'a self,
        approval_id: &'a ApprovalId,
        expected_revision: u64,
        decision: ApprovalDecision,
        decided_by: ApprovalActor,
    ) -> HarnessFuture<'a, ApprovalRecord> {
        let approval_id = approval_id.clone();
        Box::pin(async move {
            validate_identity("approval", approval_id.as_str())?;
            validate_decision(&decision)?;
            validate_current_actor("approval settler", &decided_by)?;
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                let indexed = transaction
                    .query_row(
                        "SELECT length(CAST(status AS BLOB)), status, revision,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                length(CAST(turn_id AS BLOB)), turn_id,
                                length(CAST(record_json AS BLOB)), record_json
                         FROM approval_records WHERE approval_id = ?1",
                        [approval_id.as_str()],
                        read_indexed_record,
                    )
                    .optional()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?
                    .ok_or_else(|| {
                        HarnessError::Approval(format!("approval {approval_id} does not exist"))
                    })?;
                let mut record = decode_indexed_record(indexed)?;
                let settled = settle_record(&mut record, expected_revision, decision, decided_by)?;
                let encoded = encode_record(&settled)?;
                let changed = transaction
                    .execute(
                        "UPDATE approval_records
                         SET status = 'settled', revision = ?1, record_json = ?2
                         WHERE approval_id = ?3 AND revision = ?4 AND status = 'pending'",
                        params![
                            to_sql_u64("approval revision", settled.revision)?,
                            encoded,
                            approval_id.as_str(),
                            to_sql_u64("expected approval revision", expected_revision)?
                        ],
                    )
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                if changed != 1 {
                    return Err(HarnessError::ApprovalConflict {
                        approval_id,
                        expected: expected_revision,
                        actual: record.revision,
                    });
                }
                transaction
                    .commit()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                Ok(settled)
            })
            .await
        })
    }

    fn orphan_turn<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        reason: &'a str,
    ) -> HarnessFuture<'a, usize> {
        let thread_id = thread_id.clone();
        let turn_id = turn_id.clone();
        let reason = reason.to_owned();
        Box::pin(async move {
            validate_identity("thread", thread_id.as_str())?;
            validate_identity("turn", turn_id.as_str())?;
            validate_reason("orphan", &reason)?;
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                let approval_ids = {
                    let mut statement = transaction
                        .prepare(
                            "SELECT length(CAST(approval_id AS BLOB)), approval_id
                             FROM approval_records
                             WHERE thread_id = ?1 AND turn_id = ?2 AND status = 'pending'
                             ORDER BY approval_id
                             LIMIT ?3",
                        )
                        .map_err(|error| HarnessError::Approval(error.to_string()))?;
                    let rows = statement
                        .query_map(
                            params![
                                thread_id.as_str(),
                                turn_id.as_str(),
                                i64::try_from(MAX_APPROVALS_PER_TURN).unwrap_or(i64::MAX)
                            ],
                            |row| bounded_text(row, 0, 1, 256, "stored approval identity"),
                        )
                        .map_err(|error| HarnessError::Approval(error.to_string()))?;
                    let mut approval_ids = Vec::new();
                    for row in rows {
                        approval_ids
                            .push(row.map_err(|error| HarnessError::Approval(error.to_string()))?);
                    }
                    approval_ids
                };

                let mut changed = 0_usize;
                for approval_id in approval_ids {
                    let indexed = transaction
                        .query_row(
                            "SELECT length(CAST(status AS BLOB)), status, revision,
                                    length(CAST(thread_id AS BLOB)), thread_id,
                                    length(CAST(turn_id AS BLOB)), turn_id,
                                    length(CAST(record_json AS BLOB)), record_json
                             FROM approval_records WHERE approval_id = ?1",
                            [approval_id.as_str()],
                            read_indexed_record,
                        )
                        .optional()
                        .map_err(|error| HarnessError::Approval(error.to_string()))?
                        .ok_or_else(|| {
                            HarnessError::Approval(format!(
                                "approval {approval_id} disappeared during orphaning"
                            ))
                        })?;
                    let mut record = decode_indexed_record(indexed)?;
                    orphan_record(&mut record, &reason)?;
                    let encoded = encode_record(&record)?;
                    let updated = transaction
                        .execute(
                            "UPDATE approval_records
                             SET status = 'orphaned', revision = ?1, record_json = ?2
                             WHERE approval_id = ?3 AND status = 'pending'",
                            params![
                                to_sql_u64("approval revision", record.revision)?,
                                encoded,
                                record.request.id.as_str()
                            ],
                        )
                        .map_err(|error| HarnessError::Approval(error.to_string()))?;
                    changed = changed.saturating_add(updated);
                }
                transaction
                    .commit()
                    .map_err(|error| HarnessError::Approval(error.to_string()))?;
                Ok(changed)
            })
            .await
        })
    }
}

/// Approval handler that submits to a durable inbox and awaits external CAS settlement.
pub struct InboxApprovalHandler {
    inbox: Arc<dyn ApprovalInbox>,
    poll_interval: Duration,
}

impl InboxApprovalHandler {
    /// Creates a polling handler with an explicitly bounded interval.
    pub fn new(
        inbox: Arc<dyn ApprovalInbox>,
        poll_interval: Duration,
    ) -> Result<Self, HarnessError> {
        if !(MIN_POLL_INTERVAL..=MAX_POLL_INTERVAL).contains(&poll_interval) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "approval poll interval must be {}-{} milliseconds",
                MIN_POLL_INTERVAL.as_millis(),
                MAX_POLL_INTERVAL.as_millis()
            )));
        }
        Ok(Self {
            inbox,
            poll_interval,
        })
    }
}

impl ApprovalHandler for InboxApprovalHandler {
    fn decide<'a>(&'a self, request: &'a ApprovalRequest) -> HarnessFuture<'a, ApprovalDecision> {
        let request = request.clone();
        Box::pin(async move {
            let mut record = self.inbox.submit(request).await?;
            loop {
                match record.status {
                    ApprovalRecordStatus::Pending => {
                        time::sleep(self.poll_interval).await;
                        record = self.inbox.get(&record.request.id).await?.ok_or_else(|| {
                            HarnessError::Approval(
                                "approval disappeared while awaiting settlement".to_owned(),
                            )
                        })?;
                    }
                    ApprovalRecordStatus::Settled { decision, .. } => return Ok(decision),
                    ApprovalRecordStatus::Orphaned { reason } => {
                        return Err(HarnessError::Approval(format!(
                            "approval request was orphaned: {reason}"
                        )));
                    }
                }
            }
        })
    }

    fn abandon_turn<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        reason: &'a str,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            self.inbox.orphan_turn(thread_id, turn_id, reason).await?;
            Ok(())
        })
    }
}

type IndexedRecord = (String, i64, String, String, String);

fn read_indexed_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexedRecord> {
    Ok((
        bounded_text(row, 0, 1, 8, "stored approval status")?,
        row.get(2)?,
        bounded_text(row, 3, 4, 256, "stored approval Thread identity")?,
        bounded_text(row, 5, 6, 256, "stored approval Turn identity")?,
        bounded_text(
            row,
            7,
            8,
            MAX_APPROVAL_RECORD_BYTES,
            "stored approval record",
        )?,
    ))
}

fn decode_indexed_record(indexed: IndexedRecord) -> Result<ApprovalRecord, HarnessError> {
    let (status, revision, thread_id, turn_id, encoded) = indexed;
    let record = decode_record(&encoded)?;
    let revision = u64::try_from(revision)
        .map_err(|_| HarnessError::Approval("invalid stored approval revision".to_owned()))?;
    if status != status_name(&record.status)
        || revision != record.revision
        || thread_id != record.request.authorization.thread_id.as_str()
        || turn_id != record.request.authorization.turn_id.as_str()
    {
        return Err(HarnessError::Approval(
            "approval record indexes do not match its body".to_owned(),
        ));
    }
    Ok(record)
}

fn status_name(status: &ApprovalRecordStatus) -> &'static str {
    match status {
        ApprovalRecordStatus::Pending => "pending",
        ApprovalRecordStatus::Settled { .. } => "settled",
        ApprovalRecordStatus::Orphaned { .. } => "orphaned",
    }
}

fn new_record(request: ApprovalRequest) -> ApprovalRecord {
    ApprovalRecord {
        schema_version: APPROVAL_INBOX_SCHEMA_VERSION,
        request,
        status: ApprovalRecordStatus::Pending,
        revision: 1,
        requested_at_ms: now_ms(),
        settled_at_ms: None,
    }
}

fn matching_request(
    existing: &ApprovalRecord,
    request: &ApprovalRequest,
) -> Result<ApprovalRecord, HarnessError> {
    if &existing.request == request {
        Ok(existing.clone())
    } else {
        Err(HarnessError::Approval(format!(
            "approval {} was reused with different content",
            request.id
        )))
    }
}

fn settle_record(
    record: &mut ApprovalRecord,
    expected_revision: u64,
    decision: ApprovalDecision,
    decided_by: ApprovalActor,
) -> Result<ApprovalRecord, HarnessError> {
    if record.revision != expected_revision {
        return Err(HarnessError::ApprovalConflict {
            approval_id: record.request.id.clone(),
            expected: expected_revision,
            actual: record.revision,
        });
    }
    if !matches!(record.status, ApprovalRecordStatus::Pending) {
        return Err(HarnessError::Approval(format!(
            "approval {} is already terminal",
            record.request.id
        )));
    }
    validate_current_actor("approval settler", &decided_by)?;
    if record.request.requested_by == decided_by {
        return Err(HarnessError::Approval(format!(
            "approval {} requester cannot settle the same request",
            record.request.id
        )));
    }
    let mut candidate = record.clone();
    candidate.revision = candidate
        .revision
        .checked_add(1)
        .ok_or_else(|| HarnessError::Approval("approval revision overflow".to_owned()))?;
    candidate.status = ApprovalRecordStatus::Settled {
        decision,
        decided_by,
    };
    candidate.settled_at_ms = Some(now_ms());
    validate_record(&candidate)?;
    *record = candidate.clone();
    Ok(candidate)
}

fn orphan_record(record: &mut ApprovalRecord, reason: &str) -> Result<(), HarnessError> {
    if !matches!(record.status, ApprovalRecordStatus::Pending) {
        return Ok(());
    }
    let mut candidate = record.clone();
    candidate.revision = candidate
        .revision
        .checked_add(1)
        .ok_or_else(|| HarnessError::Approval("approval revision overflow".to_owned()))?;
    candidate.status = ApprovalRecordStatus::Orphaned {
        reason: reason.to_owned(),
    };
    candidate.settled_at_ms = Some(now_ms());
    validate_record(&candidate)?;
    *record = candidate;
    Ok(())
}

fn enforce_turn_capacity<'a>(
    records: impl Iterator<Item = &'a ApprovalRecord>,
    request: &ApprovalRequest,
) -> Result<(), HarnessError> {
    let count = records
        .filter(|record| {
            record.request.authorization.thread_id == request.authorization.thread_id
                && record.request.authorization.turn_id == request.authorization.turn_id
        })
        .count();
    if count >= MAX_APPROVALS_PER_TURN {
        Err(HarnessError::Approval(format!(
            "Turn exceeds {MAX_APPROVALS_PER_TURN} approval records"
        )))
    } else {
        Ok(())
    }
}

fn encode_record(record: &ApprovalRecord) -> Result<String, HarnessError> {
    validate_record(record)?;
    let encoded = to_bounded_json_vec(record, MAX_APPROVAL_RECORD_BYTES).map_err(|error| {
        approval_json_error("approval record", MAX_APPROVAL_RECORD_BYTES, error)
    })?;
    String::from_utf8(encoded)
        .map_err(|_| HarnessError::Approval("approval record is not UTF-8 JSON".to_owned()))
}

fn decode_record(encoded: &str) -> Result<ApprovalRecord, HarnessError> {
    if encoded.len() > MAX_APPROVAL_RECORD_BYTES {
        return Err(HarnessError::Approval(format!(
            "stored approval record exceeds {MAX_APPROVAL_RECORD_BYTES} bytes"
        )));
    }
    let record: ApprovalRecord =
        serde_json::from_str(encoded).map_err(|error| HarnessError::Approval(error.to_string()))?;
    validate_record(&record)?;
    Ok(record)
}

fn validate_record(record: &ApprovalRecord) -> Result<(), HarnessError> {
    if record.schema_version != APPROVAL_INBOX_SCHEMA_VERSION || record.revision == 0 {
        return Err(HarnessError::Approval(
            "approval record has unsupported schema or revision".to_owned(),
        ));
    }
    validate_request(&record.request)?;
    if matches!(
        record.request.requested_by,
        ApprovalActor::UnattributedLegacy
    ) && matches!(record.status, ApprovalRecordStatus::Pending)
    {
        return Err(HarnessError::Approval(
            "legacy unattributed approval request cannot remain pending".to_owned(),
        ));
    }
    match &record.status {
        ApprovalRecordStatus::Pending if record.settled_at_ms.is_none() => {}
        ApprovalRecordStatus::Settled {
            decision,
            decided_by,
        } if record.settled_at_ms.is_some() => {
            validate_decision(decision)?;
            validate_record_actor("approval settler", decided_by)?;
            if record.request.requested_by == *decided_by
                && !matches!(decided_by, ApprovalActor::UnattributedLegacy)
            {
                return Err(HarnessError::Approval(
                    "approval requester and settler identities are equal".to_owned(),
                ));
            }
        }
        ApprovalRecordStatus::Orphaned { reason } if record.settled_at_ms.is_some() => {
            validate_reason("orphan", reason)?;
        }
        _ => {
            return Err(HarnessError::Approval(
                "approval status and settlement timestamp disagree".to_owned(),
            ));
        }
    }
    let maximum_bytes = if matches!(record.status, ApprovalRecordStatus::Pending) {
        MAX_APPROVAL_RECORD_BYTES - APPROVAL_TERMINAL_RECORD_RESERVE_BYTES
    } else {
        MAX_APPROVAL_RECORD_BYTES
    };
    bounded_serialized_size(record, maximum_bytes)
        .map_err(|error| approval_json_error("approval record lifecycle", maximum_bytes, error))?;
    Ok(())
}

fn validate_request(request: &ApprovalRequest) -> Result<(), HarnessError> {
    validate_identity("approval", request.id.as_str())?;
    validate_record_actor("approval requester", &request.requested_by)?;
    validate_identity("thread", request.authorization.thread_id.as_str())?;
    validate_identity("turn", request.authorization.turn_id.as_str())?;
    validate_identity("tool call", &request.authorization.call_id)?;
    validate_capability_name("approval Tool", &request.authorization.descriptor.name)?;
    validate_reason("Policy", &request.reason)?;
    if request
        .authorization
        .descriptor
        .description
        .trim()
        .is_empty()
    {
        return Err(HarnessError::Approval(
            "approval Tool description is empty".to_owned(),
        ));
    }
    for (kind, value) in [
        (
            "approval Tool input schema",
            &request.authorization.descriptor.input_schema,
        ),
        ("approval Tool input", &request.authorization.input),
    ] {
        validate_value_shape(value).map_err(|_| {
            HarnessError::Approval(format!(
                "{kind} exceeds the supported JSON depth or node count"
            ))
        })?;
    }
    bounded_serialized_size(request, MAX_APPROVAL_RECORD_BYTES).map_err(|error| {
        approval_json_error("approval request", MAX_APPROVAL_RECORD_BYTES, error)
    })?;
    Ok(())
}

fn approval_json_error(kind: &str, maximum: usize, error: BoundedJsonError) -> HarnessError {
    match error {
        BoundedJsonError::LimitExceeded => {
            HarnessError::Approval(format!("{kind} exceeds {maximum} bytes"))
        }
        BoundedJsonError::CannotEncode => {
            HarnessError::Approval(format!("{kind} cannot be encoded"))
        }
    }
}

fn validate_current_request(request: &ApprovalRequest) -> Result<(), HarnessError> {
    validate_request(request)?;
    validate_current_actor("approval requester", &request.requested_by)
}

fn validate_record_actor(kind: &str, actor: &ApprovalActor) -> Result<(), HarnessError> {
    actor.validate_shape(kind)
}

fn validate_current_actor(kind: &str, actor: &ApprovalActor) -> Result<(), HarnessError> {
    actor.validate_current(kind)
}

fn validate_decision(decision: &ApprovalDecision) -> Result<(), HarnessError> {
    if let ApprovalDecision::Deny { reason } = decision {
        validate_reason("denial", reason)?;
    }
    Ok(())
}

fn validate_reason(kind: &str, reason: &str) -> Result<(), HarnessError> {
    if reason.trim().is_empty()
        || reason.len() > MAX_APPROVAL_REASON_BYTES
        || reason.chars().any(char::is_control)
    {
        return Err(HarnessError::Approval(format!(
            "{kind} reason must be 1-{MAX_APPROVAL_REASON_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(HarnessError::Approval(format!(
            "{kind} identity must be 1-256 non-control bytes"
        )));
    }
    Ok(())
}

fn validate_page(limit: usize) -> Result<(), HarnessError> {
    if !(1..=MAX_APPROVAL_PAGE).contains(&limit) {
        return Err(HarnessError::Approval(format!(
            "approval page limit must be 1-{MAX_APPROVAL_PAGE}"
        )));
    }
    Ok(())
}

fn to_sql_u64(kind: &str, value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value)
        .map_err(|_| HarnessError::Approval(format!("{kind} exceeds SQLite INTEGER")))
}

pub(super) fn validate_or_bootstrap_store(connection: &Connection) -> Result<(), HarnessError> {
    if !table_exists(connection, "approval_records")? {
        if table_exists(connection, APPROVAL_METADATA_TABLE)? {
            return Err(HarnessError::Approval(
                "SQLite Approval Inbox is partial: metadata exists without records".to_owned(),
            ));
        }
        return Ok(());
    }
    if !table_exists(connection, APPROVAL_METADATA_TABLE)? {
        let records: i64 = connection
            .query_row("SELECT COUNT(*) FROM approval_records", [], |row| {
                row.get(0)
            })
            .map_err(|error| HarnessError::Approval(error.to_string()))?;
        if records == 0 {
            return Ok(());
        }
        return Err(HarnessError::Approval(
            "SQLite Approval Inbox migration required; run `yh approval-migrate <database> <backup>` before opening this store"
                .to_owned(),
        ));
    }
    validate_current_metadata(connection)
}

pub(super) fn metadata_schema_sql() -> String {
    format!(
        "
        CREATE TABLE IF NOT EXISTS {APPROVAL_METADATA_TABLE} (
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL CHECK(value > 0)
        );
        INSERT OR IGNORE INTO {APPROVAL_METADATA_TABLE} (key, value)
            VALUES ('record_schema', {APPROVAL_INBOX_SCHEMA_VERSION});
        "
    )
}

pub(super) fn validate_current_metadata(connection: &Connection) -> Result<(), HarnessError> {
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
    if entries != 1 || schema != Some(i64::from(APPROVAL_INBOX_SCHEMA_VERSION)) {
        return Err(HarnessError::Approval(format!(
            "unsupported SQLite Approval Inbox metadata; expected record schema {APPROVAL_INBOX_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

pub(super) fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(|error| HarnessError::Approval(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc, time::Duration};

    use rusqlite::Connection;
    use serde_json::json;

    use super::{
        ApprovalInbox, ApprovalRecordStatus, InboxApprovalHandler, MemoryApprovalInbox,
        SqliteApprovalInbox,
    };
    use crate::{
        ApprovalActor, ApprovalDecision, ApprovalHandler, ApprovalId, ApprovalRequest,
        CapabilityOrigin, HarnessError, RiskLevel, ThreadId, ToolAuthorization, ToolDescriptor,
        TurnId,
    };

    fn approver(subject: &str) -> ApprovalActor {
        ApprovalActor::Authenticated {
            authority: "test-authority".to_owned(),
            subject: subject.to_owned(),
        }
    }

    fn request() -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: ApprovalActor::LocalProcess,
            authorization: ToolAuthorization {
                thread_id: ThreadId::generate(),
                turn_id: TurnId::generate(),
                call_id: "call-1".to_owned(),
                descriptor: ToolDescriptor {
                    name: "deploy".to_owned(),
                    description: "deploy one bounded artifact".to_owned(),
                    input_schema: json!({"type": "object"}),
                },
                origin: CapabilityOrigin::BuiltIn,
                input: json!({"artifact": "a-1"}),
            },
            reason: "deployment changes external state".to_owned(),
            risk: RiskLevel::High,
        }
    }

    fn request_with_pending_record_bytes(target_bytes: usize) -> ApprovalRequest {
        let mut request = request();
        request.authorization.input = json!({"padding": ""});
        let base = serde_json::to_vec(&super::new_record(request.clone()))
            .expect("encode base pending record")
            .len();
        let padding = target_bytes.checked_sub(base).expect("base request fits");
        request.authorization.input = json!({"padding": "x".repeat(padding)});
        assert_eq!(
            serde_json::to_vec(&super::new_record(request.clone()))
                .expect("encode maximum pending record")
                .len(),
            target_bytes
        );
        request
    }

    #[tokio::test]
    async fn memory_inbox_is_idempotent_revisioned_and_pollable() {
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let request = request();
        let first = inbox.submit(request.clone()).await.expect("submit");
        let duplicate = inbox.submit(request.clone()).await.expect("idempotent");
        assert_eq!(first, duplicate);
        assert_eq!(inbox.pending(10).await.expect("pending").len(), 1);

        let handler =
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10)).expect("handler");
        let waiter = tokio::spawn({
            let request = request.clone();
            async move { handler.decide(&request).await }
        });
        for _ in 0..100 {
            if inbox
                .get(&request.id)
                .await
                .expect("poll submission")
                .is_some()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(inbox.get(&request.id).await.expect("submitted").is_some());
        inbox
            .settle(
                &request.id,
                first.revision,
                ApprovalDecision::Approve,
                approver("operator-1"),
            )
            .await
            .expect("settle");
        assert_eq!(
            waiter.await.expect("waiter task").expect("decision"),
            ApprovalDecision::Approve
        );
        let conflict = inbox
            .settle(
                &request.id,
                first.revision,
                ApprovalDecision::Approve,
                approver("operator-1"),
            )
            .await
            .expect_err("stale revision");
        assert!(matches!(conflict, HarnessError::ApprovalConflict { .. }));
    }

    #[tokio::test]
    async fn requester_cannot_settle_own_request_and_actor_is_durable() {
        let inbox = MemoryApprovalInbox::new();
        let mut request = request();
        request.requested_by = approver("requester");
        let submitted = inbox.submit(request.clone()).await.expect("submit");

        let error = inbox
            .settle(
                &request.id,
                submitted.revision,
                ApprovalDecision::Approve,
                approver("requester"),
            )
            .await
            .expect_err("self approval must fail");
        assert!(error.to_string().contains("requester cannot settle"));
        assert_eq!(
            inbox.get(&request.id).await.expect("read").expect("record"),
            submitted
        );

        let settled = inbox
            .settle(
                &request.id,
                submitted.revision,
                ApprovalDecision::Approve,
                approver("independent-approver"),
            )
            .await
            .expect("independent settlement");
        assert!(matches!(
            settled.status,
            ApprovalRecordStatus::Settled {
                decided_by: ApprovalActor::Authenticated { ref subject, .. },
                ..
            } if subject == "independent-approver"
        ));
    }

    #[tokio::test]
    async fn current_workflow_rejects_unattributed_legacy_actor() {
        let inbox = MemoryApprovalInbox::new();
        let mut legacy_request = request();
        legacy_request.requested_by = ApprovalActor::UnattributedLegacy;
        let error = inbox
            .submit(legacy_request)
            .await
            .expect_err("legacy requester is migration-only");
        assert!(error.to_string().contains("legacy unattributed"));

        let request = request();
        let submitted = inbox.submit(request.clone()).await.expect("submit");
        let error = inbox
            .settle(
                &request.id,
                submitted.revision,
                ApprovalDecision::Approve,
                ApprovalActor::UnattributedLegacy,
            )
            .await
            .expect_err("legacy settler is migration-only");
        assert!(error.to_string().contains("legacy unattributed"));
    }

    #[tokio::test]
    async fn inboxes_reject_unbounded_or_deep_json_without_partial_submission() {
        let path = temp_database_path();
        let sqlite = SqliteApprovalInbox::open(&path).await.expect("open inbox");
        let memory = MemoryApprovalInbox::new();
        for inbox in [&memory as &dyn ApprovalInbox, &sqlite as &dyn ApprovalInbox] {
            let mut oversized = request();
            oversized.authorization.input =
                json!({"padding": "x".repeat(super::MAX_APPROVAL_RECORD_BYTES * 2)});
            let oversized_id = oversized.id.clone();
            let error = inbox
                .submit(oversized)
                .await
                .expect_err("oversized request");
            assert!(error.to_string().contains("exceeds"));
            assert!(
                inbox
                    .get(&oversized_id)
                    .await
                    .expect("read oversized")
                    .is_none()
            );

            let mut nested = serde_json::Value::Null;
            for _ in 0..=crate::json::MAX_JSON_DEPTH {
                nested = serde_json::Value::Array(vec![nested]);
            }
            let mut too_deep = request();
            too_deep.authorization.input = nested;
            let too_deep_id = too_deep.id.clone();
            let error = inbox.submit(too_deep).await.expect_err("deep request");
            assert!(error.to_string().contains("depth or node count"));
            assert!(inbox.get(&too_deep_id).await.expect("read deep").is_none());
        }
        drop(sqlite);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn memory_pending_selects_only_the_bounded_oldest_window() {
        let inbox = MemoryApprovalInbox::new();
        {
            let mut records = inbox.records.lock().await;
            for requested_at_ms in (0..32).rev() {
                let mut record = super::new_record(request());
                record.requested_at_ms = requested_at_ms;
                records.insert(record.request.id.clone(), record);
            }
        }

        let page = inbox
            .pending(super::MAX_APPROVAL_PAGE)
            .await
            .expect("bounded page");
        assert_eq!(page.len(), super::MAX_APPROVAL_PAGE);
        assert_eq!(
            page.iter()
                .map(|record| record.requested_at_ms)
                .collect::<Vec<_>>(),
            (0..u64::try_from(super::MAX_APPROVAL_PAGE).expect("page fits")).collect::<Vec<_>>()
        );
        inbox
            .pending(super::MAX_APPROVAL_PAGE + 1)
            .await
            .expect_err("oversized page");
    }

    #[test]
    fn failed_record_transition_never_mutates_its_input() {
        let request = request_with_pending_record_bytes(super::MAX_APPROVAL_RECORD_BYTES);
        let mut record = super::new_record(request);
        let original = record.clone();
        super::settle_record(
            &mut record,
            1,
            ApprovalDecision::Approve,
            approver("operator-1"),
        )
        .expect_err("terminal form exceeds the durable record limit");
        assert_eq!(record, original);

        super::orphan_record(&mut record, "originating runtime stopped")
            .expect_err("orphaned form exceeds the durable record limit");
        assert_eq!(record, original);
    }

    #[tokio::test]
    async fn pending_capacity_reserves_every_supported_terminal_form() {
        let pending_limit =
            super::MAX_APPROVAL_RECORD_BYTES - super::APPROVAL_TERMINAL_RECORD_RESERVE_BYTES;
        let rejected = request_with_pending_record_bytes(pending_limit + 1);
        MemoryApprovalInbox::new()
            .submit(rejected)
            .await
            .expect_err("pending record must retain terminal reserve");

        let deny_inbox = MemoryApprovalInbox::new();
        let deny_request = request_with_pending_record_bytes(pending_limit);
        let deny_pending = deny_inbox
            .submit(deny_request.clone())
            .await
            .expect("submit at pending ceiling");
        deny_inbox
            .settle(
                &deny_request.id,
                deny_pending.revision,
                ApprovalDecision::Deny {
                    reason: "x".repeat(super::MAX_APPROVAL_REASON_BYTES),
                },
                approver("operator-1"),
            )
            .await
            .expect("maximum denial must fit the terminal reserve");

        let orphan_inbox = MemoryApprovalInbox::new();
        let orphan_request = request_with_pending_record_bytes(pending_limit);
        orphan_inbox
            .submit(orphan_request.clone())
            .await
            .expect("submit at pending ceiling");
        assert_eq!(
            orphan_inbox
                .orphan_turn(
                    &orphan_request.authorization.thread_id,
                    &orphan_request.authorization.turn_id,
                    &"x".repeat(super::MAX_APPROVAL_REASON_BYTES),
                )
                .await
                .expect("maximum orphan reason must fit the terminal reserve"),
            1
        );
    }

    #[tokio::test]
    async fn sqlite_inbox_survives_reopen_and_fences_competing_settlers() {
        let path = temp_database_path();
        let first = Arc::new(
            SqliteApprovalInbox::open(&path)
                .await
                .expect("first connection"),
        );
        let second = Arc::new(
            SqliteApprovalInbox::open(&path)
                .await
                .expect("second connection"),
        );
        let request = request();
        let submitted = first.submit(request.clone()).await.expect("submit");
        let (left, right) = tokio::join!(
            first.settle(
                &request.id,
                submitted.revision,
                ApprovalDecision::Approve,
                approver("operator-1")
            ),
            second.settle(
                &request.id,
                submitted.revision,
                ApprovalDecision::Deny {
                    reason: "operator rejected".to_owned()
                },
                approver("operator-2")
            )
        );
        assert_ne!(left.is_ok(), right.is_ok());
        assert!(
            left.as_ref()
                .err()
                .or_else(|| right.as_ref().err())
                .is_some_and(|error| matches!(error, HarnessError::ApprovalConflict { .. }))
        );
        drop(first);
        drop(second);

        let reopened = SqliteApprovalInbox::open(&path).await.expect("reopen");
        let record = reopened
            .get(&request.id)
            .await
            .expect("read")
            .expect("record");
        assert!(matches!(
            record.status,
            ApprovalRecordStatus::Settled {
                decided_by: ApprovalActor::Authenticated { ref subject, .. },
                ..
            } if subject == "operator-1" || subject == "operator-2"
        ));
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_inbox_rejects_oversized_corrupt_text_at_the_row_boundary() {
        let path = temp_database_path();
        let inbox = SqliteApprovalInbox::open(&path).await.expect("open inbox");
        let request = request();
        inbox.submit(request.clone()).await.expect("submit");
        drop(inbox);

        let corrupt = Connection::open(&path).expect("open corrupting connection");
        corrupt
            .execute(
                "UPDATE approval_records SET record_json = ?1 WHERE approval_id = ?2",
                rusqlite::params![
                    "x".repeat(super::MAX_APPROVAL_RECORD_BYTES + 1),
                    request.id.as_str()
                ],
            )
            .expect("inject oversized record");
        drop(corrupt);

        let reopened = SqliteApprovalInbox::open(&path)
            .await
            .expect("reopen inbox");
        let error = reopened
            .get(&request.id)
            .await
            .expect_err("oversized text must fail before decoding");
        assert!(error.to_string().contains(&format!(
            "stored approval record exceeds {} bytes",
            super::MAX_APPROVAL_RECORD_BYTES
        )));
        drop(reopened);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_orphans_a_turn_through_bounded_identity_selection() {
        let path = temp_database_path();
        let inbox = SqliteApprovalInbox::open(&path).await.expect("open inbox");
        let base = request();
        let mut approval_ids = Vec::new();
        for index in 0..3 {
            let mut request = base.clone();
            request.id = ApprovalId::generate();
            request.authorization.call_id = format!("call-{index}");
            approval_ids.push(request.id.clone());
            inbox.submit(request).await.expect("submit");
        }

        assert_eq!(
            inbox
                .orphan_turn(
                    &base.authorization.thread_id,
                    &base.authorization.turn_id,
                    "originating runtime stopped",
                )
                .await
                .expect("orphan turn"),
            approval_ids.len()
        );
        for approval_id in approval_ids {
            let record = inbox
                .get(&approval_id)
                .await
                .expect("read orphan")
                .expect("approval");
            assert!(matches!(
                record.status,
                ApprovalRecordStatus::Orphaned { .. }
            ));
        }
        drop(inbox);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn orphaned_turn_cannot_receive_a_late_settlement() {
        let inbox = MemoryApprovalInbox::new();
        let request = request();
        let submitted = inbox.submit(request.clone()).await.expect("submit");
        assert_eq!(
            inbox
                .orphan_turn(
                    &request.authorization.thread_id,
                    &request.authorization.turn_id,
                    "originating runtime stopped"
                )
                .await
                .expect("orphan"),
            1
        );
        let error = inbox
            .settle(
                &request.id,
                submitted.revision + 1,
                ApprovalDecision::Approve,
                approver("operator-1"),
            )
            .await
            .expect_err("terminal record");
        assert!(matches!(error, HarnessError::Approval(_)));
    }

    fn temp_database_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-approval-{}-{}.db",
            std::process::id(),
            ApprovalId::generate()
        ))
    }

    fn remove_database_files(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
