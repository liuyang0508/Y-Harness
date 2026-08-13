//! Append-only runtime state, deterministic projection, and recovery.

mod migration;
mod wait_due;

pub use migration::{StateMigrationReport, StateMigrationStatus};
pub use wait_due::{
    AgentLoopDueCursor, AgentLoopDuePhase, AgentLoopDueScanPage, AgentLoopDueWait,
    MAX_AGENT_LOOP_DUE_SCAN_LIMIT,
};

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::{self, Write},
    ops::Bound,
    path::Path,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, Notify, Semaphore},
    task,
};

use crate::{
    AgentLoopClaimId, AgentLoopCloseCommandId, AgentLoopDenyCommandId, AgentLoopExecution,
    AgentLoopResumeCommandId, AgentLoopWaitId, AgentLoopWorkerId, ApprovalRecord,
    ApprovalRecordStatus, ApprovalSettlementEvidence, AuthorityContext, Checkpoint, CheckpointId,
    CompletionGeneration, CompletionReceipt, EventId, ExecutionClaimEvidence, ExecutionPhase,
    HarnessError, HarnessFuture, InboxTombstoneReason, Item, ItemId, ItemKind, NewStreamEvent,
    PendingEvent, ResumeEvidence, StateEvent, StoredEvent, Thread, ThreadId, ThreadImportOrigin,
    ThreadLineage, Turn, TurnId, TurnStatus, TurnStopReason, TurnWaitEnvelope, WaitClosureEvidence,
    WaitDenialEvidence, WaitKind,
    completion::{
        completion_receipt_sha256, validate_inherited_projected_turn_completion_receipt,
        validate_projected_turn_completion_receipt, validate_turn_completion_receipt,
    },
    json::{
        BoundedJsonError, bounded_serialized_sha256, bounded_serialized_size, to_bounded_json_vec,
        validate_value_shape,
    },
    kernel::validate_capability_name,
    sqlite::{bounded_optional_text, bounded_text, open_read_only},
};
use wait_due::{
    AgentLoopDueIndexKey, AgentLoopWaitProjection, AgentLoopWaitProjectionChange,
    deterministic_denial_command_id, deterministic_timeout_command_id, projection_change,
    validate_due_scan_request,
};

/// Current append-only State event schema.
pub const STATE_EVENT_SCHEMA_VERSION: u32 = 16;
/// Current transactionally maintained live Agent Loop wait projection layout.
pub const AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION: u32 = 2;
// A Runtime text field is bounded at 1 MiB, but JSON control-character
// escaping can expand each input byte sixfold. Keep the journal envelope above
// that worst case while retaining an absolute per-event allocation bound.
const MAX_STATE_EVENT_BYTES: usize = 8_388_608;
const MAX_STATE_EVENT_PAGE: usize = 10_000;
const MAX_STATE_EVENT_PAGE_RECOVERY_BYTES: u64 = 16_777_216;
const MAX_THREAD_SUMMARY_PAGE: usize = 64;
const MAX_THREAD_NAME_BYTES: usize = 256;
const MAX_CHECKPOINT_LABEL_BYTES: usize = 4_096;
const MAX_STATE_SNAPSHOT_BYTES: usize = 67_108_864;
/// Hard serialized-plus-overhead recovery boundary for one Thread.
pub const STATE_THREAD_RECOVERY_BYTE_LIMIT: u64 = 67_108_864;
/// Conservative in-memory bookkeeping charge added to every encoded event.
const STATE_EVENT_RECOVERY_OVERHEAD_BYTES: u64 = 512;
/// Space unavailable to general events so a running Turn can always settle.
///
/// One atomic denial carries the complete, at-most 525,312-byte Approval
/// record plus a second ordinary decision Item. One MiB keeps the strongest
/// audit evidence intact even at the Approval Inbox ceiling.
pub const STATE_TERMINAL_RECOVERY_BYTE_RESERVE: u64 = 1_048_576;
// Beyond the bounded settlement, a denial event duplicates at most one
// 4,096-byte non-control reason and carries only bounded 256-byte identities,
// fixed digests, timestamps, serde tags, and the recovery charge.
const _: () = assert!(
    crate::approval::MAX_APPROVAL_RECORD_BYTES as u64 + 65_536
        <= STATE_TERMINAL_RECOVERY_BYTE_RESERVE
);
/// Hard per-Thread journal boundary that keeps worst-case recovery finite.
pub const STATE_THREAD_EVENT_LIMIT: u64 = 1_000_000;
/// Final event slot reserved so a running Turn can always settle durably.
pub const STATE_TERMINAL_EVENT_RESERVE: u64 = 1;
const MAX_STATE_RECOVERY_EVENTS: usize = STATE_THREAD_EVENT_LIMIT as usize;
const STATE_CAPACITY_WARNING_AT: u64 = STATE_THREAD_EVENT_LIMIT * 80 / 100;
const STATE_CAPACITY_CRITICAL_AT: u64 = STATE_THREAD_EVENT_LIMIT * 95 / 100;
const STATE_RECOVERY_CAPACITY_WARNING_AT: u64 = STATE_THREAD_RECOVERY_BYTE_LIMIT * 80 / 100;
const STATE_RECOVERY_CAPACITY_CRITICAL_AT: u64 = STATE_THREAD_RECOVERY_BYTE_LIMIT * 95 / 100;
const SNAPSHOT_TAIL_PAGE: usize = 1_000;
const MAX_SNAPSHOT_MAINTENANCE_CONCURRENCY: usize = 64;
/// Current disposable State snapshot schema.
pub const STATE_SNAPSHOT_SCHEMA_VERSION: u32 = 16;
/// Current portable Thread archive format.
pub const THREAD_ARCHIVE_FORMAT_VERSION: u32 = 6;
const PREVIOUS_THREAD_ARCHIVE_FORMAT_VERSION: u32 = 5;
/// Maximum accepted encoded Thread archive.
pub const MAX_THREAD_ARCHIVE_BYTES: usize = 75_497_472;
const MAX_STEERING_CONTENT_BYTES: usize = 1_048_576;
const MAX_INVOCATION_CONTEXT_BLOCKS: usize = 64;
const MAX_INVOCATION_CONTEXT_REFERENCE_BYTES: usize = 4_096;
const MAX_INVOCATION_CONTEXT_BLOCK_BYTES: usize = 1_048_576;
const MAX_INVOCATION_CONTEXT_TOTAL_BYTES: usize = 1_061_184;
/// Maximum server-issued approval wait or retained active timeout.
pub const MAX_AGENT_LOOP_WAIT_MS: u64 = 86_400_000;

#[derive(Clone, Debug)]
/// Immutable input for atomically entering an Approval-backed Agent Loop wait.
///
/// Grouping the wait identity, frozen completion coordinate, and bounded time
/// budgets prevents positional argument drift at trusted State call sites.
pub struct AgentLoopWaitStartCommand {
    wait_id: AgentLoopWaitId,
    request: crate::ApprovalRequest,
    completion_generation: CompletionGeneration,
    wait_ttl: Option<Duration>,
    remaining_active_timeout_ms: Option<u64>,
}

impl AgentLoopWaitStartCommand {
    /// Creates one wait-start command. State validates every field atomically
    /// against the authoritative Turn when the command is applied.
    #[must_use]
    pub fn new(
        wait_id: AgentLoopWaitId,
        request: crate::ApprovalRequest,
        completion_generation: CompletionGeneration,
        wait_ttl: Option<Duration>,
        remaining_active_timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            wait_id,
            request,
            completion_generation,
            wait_ttl,
            remaining_active_timeout_ms,
        }
    }
}

#[derive(Clone, Debug)]
/// Immutable input for atomically closing an unclaimed durable wait.
///
/// The command identity and expected revision belong to the requested terminal
/// outcome, so they travel as one unit through retry and digest verification.
pub struct AgentLoopWaitCloseCommand {
    wait_id: AgentLoopWaitId,
    expected_revision: u64,
    command_id: AgentLoopCloseCommandId,
    status: TurnStatus,
    reason: TurnStopReason,
}

impl AgentLoopWaitCloseCommand {
    /// Creates one exact wait-close command.
    #[must_use]
    pub fn new(
        wait_id: AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopCloseCommandId,
        status: TurnStatus,
        reason: TurnStopReason,
    ) -> Self {
        Self {
            wait_id,
            expected_revision,
            command_id,
            status,
            reason,
        }
    }
}

#[derive(Clone, Debug)]
/// Immutable input for atomically claiming one ready durable execution.
///
/// A claim is meaningful only for the exact wait revision, accepted resume
/// command, claim identity, and worker coordinate represented here.
pub struct AgentLoopReadyClaimCommand {
    wait_id: AgentLoopWaitId,
    expected_revision: u64,
    resume_command_id: AgentLoopResumeCommandId,
    claim_id: AgentLoopClaimId,
    worker_id: AgentLoopWorkerId,
}

impl AgentLoopReadyClaimCommand {
    /// Creates one exact ready-execution claim command.
    #[must_use]
    pub fn new(
        wait_id: AgentLoopWaitId,
        expected_revision: u64,
        resume_command_id: AgentLoopResumeCommandId,
        claim_id: AgentLoopClaimId,
        worker_id: AgentLoopWorkerId,
    ) -> Self {
        Self {
            wait_id,
            expected_revision,
            resume_command_id,
            claim_id,
            worker_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Validated, disposable projection cache anchored to the event journal.
///
/// The event journal remains authoritative. Fields are private, values are
/// normally created by [`StateEngine`], and deserialized values are revalidated.
pub struct StateSnapshot {
    schema_version: u32,
    thread: Thread,
    metadata_events: Vec<StateEvent>,
    through_sequence: u64,
    stream_version: u64,
    recovery_bytes: u64,
    anchor_event_id: EventId,
    projection_sha256: String,
    created_at_ms: u64,
}

impl StateSnapshot {
    /// Returns the projected Thread cached by this snapshot.
    #[must_use]
    pub fn thread(&self) -> &Thread {
        &self.thread
    }

    /// Returns the last global journal sequence included.
    #[must_use]
    pub fn through_sequence(&self) -> u64 {
        self.through_sequence
    }

    /// Returns the number of Thread events included.
    #[must_use]
    pub fn stream_version(&self) -> u64 {
        self.stream_version
    }

    /// Returns the exact recovery charge of the represented journal prefix.
    #[must_use]
    pub fn recovery_bytes(&self) -> u64 {
        self.recovery_bytes
    }

    /// Returns the event identity anchoring the included journal prefix.
    #[must_use]
    pub fn anchor_event_id(&self) -> &EventId {
        &self.anchor_event_id
    }

    /// Returns the snapshot creation time in Unix milliseconds.
    #[must_use]
    pub fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Operator-facing pressure level before a Thread reaches its hard boundary.
pub enum StateCapacityLevel {
    /// Less than 80% of both boundaries is occupied.
    Healthy,
    /// At least 80% of either boundary is occupied; archival planning should begin.
    Warning,
    /// At least 95% of either boundary is occupied; new work risks rejection.
    Critical,
    /// Only reserved terminal-settlement count or bytes remain.
    TerminalOnly,
    /// At least one hard boundary is exhausted.
    Exhausted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Read-only journal pressure projection for one Thread.
pub struct StateCapacity {
    /// Current number of authoritative events in this Thread stream.
    pub used_events: u64,
    /// Maximum supported events in one Thread stream.
    pub event_limit: u64,
    /// Events that can still be appended before the hard boundary.
    pub remaining_events: u64,
    /// Remaining events available for non-terminal State transitions.
    pub general_events_remaining: u64,
    /// Remaining slot reserved exclusively for terminal Turn settlement.
    pub terminal_event_reserve: u64,
    /// Current serialized-plus-overhead recovery charge.
    pub used_recovery_bytes: u64,
    /// Maximum supported recovery charge for one Thread.
    pub recovery_byte_limit: u64,
    /// Recovery bytes available before the hard boundary.
    pub remaining_recovery_bytes: u64,
    /// Recovery bytes available to non-terminal State transitions.
    pub general_recovery_bytes_remaining: u64,
    /// Remaining bytes reserved exclusively for terminal Turn settlement.
    pub terminal_recovery_byte_reserve: u64,
    /// Stable worst-case pressure classification across count and bytes.
    pub level: StateCapacityLevel,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Bounded recent-Thread projection for product session navigation.
pub struct ThreadSummary {
    /// Opaque authoritative Thread identity.
    pub thread_id: ThreadId,
    /// Immutable tenant boundary for this Thread.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Optional operator-authored display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Direct immutable ancestry when this Thread was forked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ThreadLineage>,
    /// Global sequence of the latest event currently observed for this Thread.
    pub last_sequence: u64,
    /// Timestamp of that latest event in Unix milliseconds.
    pub updated_at_ms: u64,
    /// Number of authoritative events currently recorded for this Thread.
    pub stream_version: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// One bounded page of recent Threads ordered by latest event sequence.
pub struct ThreadSummaryPage {
    /// Most recently updated Threads first.
    pub threads: Vec<ThreadSummary>,
    /// Exclusive sequence cursor for the next older page.
    pub next_before_sequence: Option<u64>,
    /// Whether at least one older Thread was observed.
    pub has_more: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Self-contained, bounded export of one authoritative Thread journal.
pub struct ThreadArchive {
    /// Exact archive format coordinate.
    pub format_version: u32,
    /// Source Thread identity.
    pub source_thread_id: ThreadId,
    /// Number of ordered source events.
    pub source_stream_version: u64,
    /// Last global sequence observed in the source store.
    pub source_last_sequence: u64,
    /// SHA-256 of the exact ordered source Stored Events.
    pub source_events_sha256: String,
    /// Complete validated source journal.
    pub events: Vec<StoredEvent>,
}

/// Encodes one validated portable Thread archive as bounded UTF-8 JSON.
pub fn encode_thread_archive(archive: &ThreadArchive) -> Result<Vec<u8>, HarnessError> {
    if archive.format_version != THREAD_ARCHIVE_FORMAT_VERSION {
        return Err(HarnessError::State(format!(
            "encoding requires Thread archive format {THREAD_ARCHIVE_FORMAT_VERSION}"
        )));
    }
    validate_thread_archive(archive)?;
    to_bounded_json_vec(archive, MAX_THREAD_ARCHIVE_BYTES)
        .map_err(|error| state_json_error("Thread archive", MAX_THREAD_ARCHIVE_BYTES, error))
}

/// Decodes and validates one bounded portable Thread archive.
pub fn decode_thread_archive(encoded: &[u8]) -> Result<ThreadArchive, HarnessError> {
    if encoded.len() > MAX_THREAD_ARCHIVE_BYTES {
        return Err(HarnessError::State(format!(
            "Thread archive exceeds {MAX_THREAD_ARCHIVE_BYTES} bytes"
        )));
    }
    let mut archive = serde_json::from_slice::<ThreadArchive>(encoded)
        .map_err(|error| HarnessError::State(format!("invalid Thread archive JSON: {error}")))?;
    validate_thread_archive(&archive)?;
    if archive.format_version == PREVIOUS_THREAD_ARCHIVE_FORMAT_VERSION {
        archive.format_version = THREAD_ARCHIVE_FORMAT_VERSION;
    }
    Ok(archive)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Validated opt-in policy for automatic disposable snapshot maintenance.
pub struct SnapshotMaintenanceConfig {
    every_events: u64,
    max_concurrency: usize,
}

impl SnapshotMaintenanceConfig {
    /// Creates a policy that considers a Thread after this many new events.
    pub fn new(every_events: u64, max_concurrency: usize) -> Result<Self, HarnessError> {
        if every_events == 0 || every_events > MAX_STATE_RECOVERY_EVENTS as u64 {
            return Err(HarnessError::InvalidConfiguration(format!(
                "snapshot interval must be 1-{MAX_STATE_RECOVERY_EVENTS} events"
            )));
        }
        if max_concurrency == 0 || max_concurrency > MAX_SNAPSHOT_MAINTENANCE_CONCURRENCY {
            return Err(HarnessError::InvalidConfiguration(format!(
                "snapshot maintenance concurrency must be 1-{MAX_SNAPSHOT_MAINTENANCE_CONCURRENCY}"
            )));
        }
        Ok(Self {
            every_events,
            max_concurrency,
        })
    }

    /// Returns the minimum journal growth between maintenance attempts.
    #[must_use]
    pub fn every_events(self) -> u64 {
        self.every_events
    }

    /// Returns the maximum simultaneous snapshot workers.
    #[must_use]
    pub fn max_concurrency(self) -> usize {
        self.max_concurrency
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Content-free class of the most recent isolated maintenance failure.
pub enum SnapshotMaintenanceFailure {
    /// Snapshot loading, validation, projection, or persistence failed.
    Operation,
    /// A snapshot worker panicked.
    WorkerPanicked,
    /// A snapshot worker was cancelled before settlement.
    WorkerCancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Content-free operational counters for automatic snapshot maintenance.
pub struct SnapshotMaintenanceStats {
    /// Workers accepted by the scheduler.
    pub scheduled: u64,
    /// New snapshots persisted successfully.
    pub created: u64,
    /// Attempts avoided because another store writer already had a fresh cache.
    pub already_current: u64,
    /// Attempts isolated after an operation error, panic, or cancellation.
    pub failed: u64,
    /// Terminal Turns below the configured event cadence.
    pub skipped_cadence: u64,
    /// Terminal Turns skipped because this Thread already had a worker.
    pub skipped_in_flight: u64,
    /// Terminal Turns skipped because all global worker permits were occupied.
    pub skipped_capacity: u64,
    /// Workers currently active.
    pub active: usize,
    /// Time of the latest successful create, or `None` before the first one.
    pub last_created_at_ms: Option<u64>,
    /// Time of the latest isolated failure, or `None` before the first one.
    pub last_failure_at_ms: Option<u64>,
    /// Stable class of the latest isolated failure.
    pub last_failure: Option<SnapshotMaintenanceFailure>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Whether one append created a journal row or returned its exact prior row.
pub enum EventAppendDisposition {
    /// This call committed the event.
    Applied,
    /// The exact event had already committed.
    Duplicate,
    /// A compatibility Event Store cannot distinguish the two outcomes.
    Unknown,
}

#[derive(Clone, Debug)]
/// Stored event plus the Event Store's atomic idempotency observation.
pub struct EventAppendResult {
    /// Authoritative stored event.
    pub stored: StoredEvent,
    /// Whether this call applied or replayed it.
    pub disposition: EventAppendDisposition,
}

/// Append-only persistence port for ordered State events.
pub trait EventStore: Send + Sync {
    /// Atomically compares stream version and appends, or returns an idempotent event.
    ///
    /// Implementations must compare both expected stream fields and update the
    /// version and recovery-byte charge in the same transaction as the event.
    /// Returned writes must use [`STATE_EVENT_SCHEMA_VERSION`]; prior supported
    /// coordinates are read compatibility, not write authority.
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent>;

    /// Appends while atomically reporting exact idempotency disposition.
    ///
    /// Compatibility implementations may return `Unknown`. Stores that expose
    /// the live Agent Loop due projection must override this method and return
    /// only `Applied` or `Duplicate` so maintenance metrics remain truthful.
    fn append_with_disposition<'a>(
        &'a self,
        pending: PendingEvent,
    ) -> HarnessFuture<'a, EventAppendResult> {
        Box::pin(async move {
            self.append(pending).await.map(|stored| EventAppendResult {
                stored,
                disposition: EventAppendDisposition::Unknown,
            })
        })
    }

    /// Whether this store can materialize a complete derived stream atomically.
    fn supports_atomic_stream_creation(&self) -> bool {
        false
    }

    /// Creates one complete derived stream or leaves no target stream behind.
    ///
    /// The target identity must not already exist. Implementations must reject
    /// every partial or duplicate stream and update all stream projections in
    /// the same transaction as the event rows.
    fn create_stream_atomic<'a>(
        &'a self,
        _thread_id: ThreadId,
        _events: Vec<NewStreamEvent>,
    ) -> HarnessFuture<'a, Vec<StoredEvent>> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support atomic stream creation".to_owned(),
            ))
        })
    }

    /// Returns events after one sequence within both caller-supplied bounds.
    ///
    /// Implementations must not materialize more than `max_recovery_bytes`
    /// plus one bounded event needed to determine that the page is full.
    fn events_page<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        after_sequence: u64,
        limit: usize,
        max_recovery_bytes: u64,
    ) -> HarnessFuture<'a, Vec<StoredEvent>>;

    /// Whether this store implements bounded recent-Thread listing.
    fn supports_thread_listing(&self) -> bool {
        false
    }

    /// Returns latest-per-Thread summaries before one exclusive global cursor.
    fn thread_summaries_page(
        &self,
        _tenant_id: Option<String>,
        _before_sequence: Option<u64>,
        _limit: usize,
    ) -> HarnessFuture<'_, Vec<ThreadSummary>> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support Thread listing".to_owned(),
            ))
        })
    }

    /// Returns whether one Thread exists inside the exact tenant boundary.
    ///
    /// Implementations may override this with a disposable index. The default
    /// reads only the creation event and treats malformed storage as an error.
    fn thread_accessible<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        tenant_id: Option<String>,
    ) -> HarnessFuture<'a, bool> {
        Box::pin(async move {
            let events = self
                .events_page(
                    thread_id,
                    0,
                    1,
                    u64::try_from(MAX_STATE_EVENT_BYTES)
                        .unwrap_or(u64::MAX)
                        .saturating_add(STATE_EVENT_RECOVERY_OVERHEAD_BYTES),
                )
                .await?;
            match events.first() {
                None => Ok(false),
                Some(StoredEvent {
                    event:
                        StateEvent::ThreadCreated {
                            tenant_id: owner, ..
                        },
                    ..
                }) => Ok(owner == &tenant_id),
                Some(_) => Err(HarnessError::State(
                    "Thread stream does not begin with creation".to_owned(),
                )),
            }
        })
    }

    /// Loads an optional disposable projection snapshot.
    fn load_snapshot<'a>(
        &'a self,
        _thread_id: &'a ThreadId,
    ) -> HarnessFuture<'a, Option<StateSnapshot>> {
        Box::pin(async { Ok(None) })
    }

    /// Persists a validated disposable projection snapshot.
    fn save_snapshot<'a>(&'a self, _snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support State snapshots".to_owned(),
            ))
        })
    }

    /// Whether this store transactionally maintains the bounded live-wait index.
    fn supports_agent_loop_wait_projection(&self) -> bool {
        false
    }

    /// Returns one exact-tenant keyset page from the live-wait due index.
    fn scan_due_agent_loop_waits<'a>(
        &'a self,
        _tenant_id: Option<String>,
        _at_ms: u64,
        _after: Option<AgentLoopDueCursor>,
        _scan_limit: usize,
    ) -> HarnessFuture<'a, AgentLoopDueScanPage> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support Agent Loop wait due discovery".to_owned(),
            ))
        })
    }

    /// Loads one exact bounded journal event without replaying its Thread.
    fn event_by_id<'a>(&'a self, _event_id: &'a EventId) -> HarnessFuture<'a, Option<StoredEvent>> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support exact event lookup".to_owned(),
            ))
        })
    }

    /// Looks up an existing inbox-orphan tombstone for one wait.
    ///
    /// Returns `Ok(None)` when no tombstone is recorded, or the backends
    /// do not support durable inbox repair. Compatibility default.
    fn lookup_inbox_tombstone<'a>(
        &'a self,
        _wait_id: &'a AgentLoopWaitId,
    ) -> HarnessFuture<'a, Option<InboxTombstoneRecord>> {
        Box::pin(async { Ok(None) })
    }

    /// Records one inbox-orphan tombstone in the same transaction as the
    /// State wait-terminal CAS that depends on it.
    ///
    /// Compatibility default returns `Ok(())` so backends without durable
    /// inbox repair remain functional. SQLite override commits the row.
    fn record_inbox_tombstone<'a>(
        &'a self,
        _wait_id: &'a AgentLoopWaitId,
        _reason: InboxTombstoneReason,
        _source_revision: u64,
        _tombstoned_at_ms: u64,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    /// Whether this backend persists the inbox-repair outbox + tombstone.
    fn supports_inbox_repair_durability(&self) -> bool {
        false
    }
}

/// Tombstone returned by [`EventStore::lookup_inbox_tombstone`].
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct InboxTombstoneRecord {
    /// Wait whose tombstone made the settlement a no-op.
    pub wait_id: AgentLoopWaitId,
    /// Tombstone reason recorded by the State terminal transaction.
    pub reason: InboxTombstoneReason,
    /// Final wait revision observed at tombstone commit time.
    pub source_revision: u64,
    /// Unix milliseconds when the tombstone was committed.
    pub tombstoned_at_ms: u64,
}

#[derive(Default)]
/// In-memory Event Store with production-equivalent idempotency semantics.
pub struct MemoryEventStore {
    data: Mutex<MemoryStoreData>,
}

#[derive(Default)]
struct MemoryStoreData {
    events: Vec<StoredEvent>,
    event_positions: BTreeMap<EventId, usize>,
    stream_versions: BTreeMap<ThreadId, u64>,
    stream_recovery_bytes: BTreeMap<ThreadId, u64>,
    stream_tenants: BTreeMap<ThreadId, Option<String>>,
    stream_names: BTreeMap<ThreadId, String>,
    stream_lineages: BTreeMap<ThreadId, ThreadLineage>,
    snapshots: BTreeMap<ThreadId, StateSnapshot>,
    agent_loop_waits: BTreeMap<ThreadId, AgentLoopWaitProjection>,
    agent_loop_due: BTreeSet<AgentLoopDueIndexKey>,
}

impl MemoryEventStore {
    #[must_use]
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn append_classified<'a>(
        &'a self,
        pending: PendingEvent,
    ) -> HarnessFuture<'a, EventAppendResult> {
        Box::pin(async move {
            let encoded = validate_pending_event(&pending)?;
            let mut data = self.data.lock().await;
            if let Some(position) = data.event_positions.get(&pending.event_id).copied() {
                let existing = data.events.get(position).ok_or_else(|| {
                    HarnessError::State("Memory Event Store identity index is corrupt".to_owned())
                })?;
                return matching_existing(existing, &pending).map(|stored| EventAppendResult {
                    stored,
                    disposition: EventAppendDisposition::Duplicate,
                });
            }

            let actual_stream_version = data
                .stream_versions
                .get(&pending.thread_id)
                .copied()
                .unwrap_or(0);
            validate_stream_version(actual_stream_version, &pending)?;
            let actual_recovery_bytes = data
                .stream_recovery_bytes
                .get(&pending.thread_id)
                .copied()
                .unwrap_or(0);
            validate_stream_recovery_bytes(actual_recovery_bytes, &pending)?;
            validate_thread_existence(actual_stream_version > 0, &pending)?;
            let next_stream_version = actual_stream_version
                .checked_add(1)
                .ok_or_else(|| HarnessError::State("stream version overflow".to_owned()))?;
            let next_recovery_bytes = actual_recovery_bytes
                .checked_add(encoded.recovery_bytes)
                .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))?;

            let name_change = match &pending.event {
                StateEvent::ThreadNamed { name } => Some(name.clone()),
                _ => None,
            };
            let lineage_change = match &pending.event {
                StateEvent::ThreadForked { lineage } => Some(lineage.clone()),
                _ => None,
            };
            let tenant_change = match &pending.event {
                StateEvent::ThreadCreated { tenant_id, .. } => Some(tenant_id.clone()),
                _ => None,
            };
            let next_sequence = u64::try_from(data.events.len())
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| HarnessError::State("global sequence overflow".to_owned()))?;
            let thread_id = pending.thread_id.clone();
            let stored = StoredEvent {
                schema_version: STATE_EVENT_SCHEMA_VERSION,
                sequence: next_sequence,
                event_id: pending.event_id,
                thread_id: thread_id.clone(),
                recorded_at_ms: pending.recorded_at_ms,
                event: pending.event,
            };
            let wait_change = projection_change(&stored, data.agent_loop_waits.get(&thread_id))?;

            apply_memory_wait_projection(&mut data, wait_change);
            data.stream_versions
                .insert(thread_id.clone(), next_stream_version);
            data.stream_recovery_bytes
                .insert(thread_id.clone(), next_recovery_bytes);
            if let Some(tenant_id) = tenant_change {
                data.stream_tenants.insert(thread_id.clone(), tenant_id);
            }
            if let Some(name) = name_change {
                match name {
                    Some(name) => {
                        data.stream_names.insert(thread_id.clone(), name);
                    }
                    None => {
                        data.stream_names.remove(&thread_id);
                    }
                }
            }
            if let Some(lineage) = lineage_change {
                data.stream_lineages.insert(thread_id, lineage);
            }
            let position = data.events.len();
            data.event_positions
                .insert(stored.event_id.clone(), position);
            data.events.push(stored.clone());
            Ok(EventAppendResult {
                stored,
                disposition: EventAppendDisposition::Applied,
            })
        })
    }
}

fn apply_memory_wait_projection(data: &mut MemoryStoreData, change: AgentLoopWaitProjectionChange) {
    match change {
        AgentLoopWaitProjectionChange::Unchanged => {}
        AgentLoopWaitProjectionChange::Upsert(next) => {
            if let Some(previous) = data.agent_loop_waits.get(&next.thread_id)
                && let Some(key) = previous.due_index_key()
            {
                data.agent_loop_due.remove(&key);
            }
            if let Some(key) = next.due_index_key() {
                data.agent_loop_due.insert(key);
            }
            data.agent_loop_waits.insert(next.thread_id.clone(), next);
        }
        AgentLoopWaitProjectionChange::Delete(previous) => {
            if let Some(key) = previous.due_index_key() {
                data.agent_loop_due.remove(&key);
            }
            data.agent_loop_waits.remove(&previous.thread_id);
        }
    }
}

impl EventStore for MemoryEventStore {
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
        Box::pin(async move {
            self.append_classified(pending)
                .await
                .map(|result| result.stored)
        })
    }

    fn append_with_disposition<'a>(
        &'a self,
        pending: PendingEvent,
    ) -> HarnessFuture<'a, EventAppendResult> {
        self.append_classified(pending)
    }

    fn supports_atomic_stream_creation(&self) -> bool {
        true
    }

    fn create_stream_atomic<'a>(
        &'a self,
        thread_id: ThreadId,
        events: Vec<NewStreamEvent>,
    ) -> HarnessFuture<'a, Vec<StoredEvent>> {
        Box::pin(async move {
            let encoded = validate_new_stream(&thread_id, &events)?;
            let mut data = self.data.lock().await;
            let actual = data.stream_versions.get(&thread_id).copied().unwrap_or(0);
            if actual != 0 {
                return Err(HarnessError::StateConflict {
                    thread_id,
                    expected: 0,
                    actual,
                });
            }
            let requested_ids = events
                .iter()
                .map(|event| &event.event_id)
                .collect::<BTreeSet<_>>();
            if requested_ids
                .iter()
                .any(|event_id| data.event_positions.contains_key(*event_id))
            {
                return Err(HarnessError::State(
                    "atomic stream contains an Event identity already used by another stream"
                        .to_owned(),
                ));
            }

            let mut stored = Vec::with_capacity(events.len());
            let mut next_sequence = u64::try_from(data.events.len())
                .map_err(|_| HarnessError::State("global sequence exceeds u64".to_owned()))?;
            for new in events {
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::State("global sequence overflow".to_owned()))?;
                stored.push(StoredEvent {
                    schema_version: new.schema_version,
                    sequence: next_sequence,
                    event_id: new.event_id,
                    thread_id: thread_id.clone(),
                    recorded_at_ms: new.recorded_at_ms,
                    event: new.event,
                });
            }
            let stream_version = u64::try_from(stored.len())
                .map_err(|_| HarnessError::State("stream version exceeds u64".to_owned()))?;
            let recovery_bytes = encoded.iter().try_fold(0_u64, |total, event| {
                total.checked_add(event.recovery_bytes).ok_or_else(|| {
                    HarnessError::State("stream recovery charge overflow".to_owned())
                })
            })?;
            let tenant_id = final_stream_tenant(&stored)?;
            let name = final_stream_name(&stored);
            let lineage = final_stream_lineage(&stored);
            if data.agent_loop_waits.contains_key(&thread_id) {
                return Err(HarnessError::State(
                    "atomic stream target has an orphaned live-wait projection".to_owned(),
                ));
            }
            data.stream_versions
                .insert(thread_id.clone(), stream_version);
            data.stream_recovery_bytes
                .insert(thread_id.clone(), recovery_bytes);
            data.stream_tenants.insert(thread_id.clone(), tenant_id);
            if let Some(name) = name {
                data.stream_names.insert(thread_id.clone(), name);
            }
            if let Some(lineage) = lineage {
                data.stream_lineages.insert(thread_id.clone(), lineage);
            }
            let first_position = data.events.len();
            for (offset, event) in stored.iter().enumerate() {
                data.event_positions
                    .insert(event.event_id.clone(), first_position + offset);
            }
            data.events.extend(stored.iter().cloned());
            Ok(stored)
        })
    }

    fn events_page<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        after_sequence: u64,
        limit: usize,
        max_recovery_bytes: u64,
    ) -> HarnessFuture<'a, Vec<StoredEvent>> {
        Box::pin(async move {
            validate_event_page_request(limit, max_recovery_bytes)?;
            let data = self.data.lock().await;
            let mut page = Vec::new();
            let mut recovery_bytes = 0_u64;
            for event in data
                .events
                .iter()
                .filter(|event| &event.thread_id == thread_id && event.sequence > after_sequence)
                .take(limit)
            {
                let event_bytes = stored_event_recovery_bytes(event)?;
                let next = recovery_bytes
                    .checked_add(event_bytes)
                    .ok_or_else(|| HarnessError::State("event page charge overflow".to_owned()))?;
                if next > max_recovery_bytes {
                    if page.is_empty() {
                        return Err(HarnessError::State(
                            "next State event exceeds the requested page byte budget".to_owned(),
                        ));
                    }
                    break;
                }
                recovery_bytes = next;
                page.push(event.clone());
            }
            Ok(page)
        })
    }

    fn supports_thread_listing(&self) -> bool {
        true
    }

    fn thread_summaries_page(
        &self,
        tenant_id: Option<String>,
        before_sequence: Option<u64>,
        limit: usize,
    ) -> HarnessFuture<'_, Vec<ThreadSummary>> {
        Box::pin(async move {
            validate_thread_summary_page_request(before_sequence, limit)?;
            let data = self.data.lock().await;
            let mut seen = BTreeSet::new();
            let mut page = Vec::new();
            for event in data.events.iter().rev() {
                if !seen.insert(event.thread_id.clone())
                    || before_sequence.is_some_and(|before| event.sequence >= before)
                    || data.stream_tenants.get(&event.thread_id) != Some(&tenant_id)
                {
                    continue;
                }
                let stream_version = data
                    .stream_versions
                    .get(&event.thread_id)
                    .copied()
                    .ok_or_else(|| {
                        HarnessError::State("Thread stream metadata is missing".to_owned())
                    })?;
                page.push(ThreadSummary {
                    thread_id: event.thread_id.clone(),
                    tenant_id: tenant_id.clone(),
                    name: data.stream_names.get(&event.thread_id).cloned(),
                    lineage: data.stream_lineages.get(&event.thread_id).cloned(),
                    last_sequence: event.sequence,
                    updated_at_ms: event.recorded_at_ms,
                    stream_version,
                });
                if page.len() == limit {
                    break;
                }
            }
            Ok(page)
        })
    }

    fn thread_accessible<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        tenant_id: Option<String>,
    ) -> HarnessFuture<'a, bool> {
        Box::pin(async move {
            Ok(self.data.lock().await.stream_tenants.get(thread_id) == Some(&tenant_id))
        })
    }

    fn load_snapshot<'a>(
        &'a self,
        thread_id: &'a ThreadId,
    ) -> HarnessFuture<'a, Option<StateSnapshot>> {
        Box::pin(async move { Ok(self.data.lock().await.snapshots.get(thread_id).cloned()) })
    }

    fn save_snapshot<'a>(&'a self, snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            validate_snapshot(&snapshot)?;
            let mut data = self.data.lock().await;
            let replace = data
                .snapshots
                .get(&snapshot.thread.id)
                .is_none_or(|existing| existing.stream_version <= snapshot.stream_version);
            if replace {
                data.snapshots.insert(snapshot.thread.id.clone(), snapshot);
            }
            Ok(())
        })
    }

    fn supports_agent_loop_wait_projection(&self) -> bool {
        true
    }

    fn scan_due_agent_loop_waits<'a>(
        &'a self,
        tenant_id: Option<String>,
        at_ms: u64,
        after: Option<AgentLoopDueCursor>,
        scan_limit: usize,
    ) -> HarnessFuture<'a, AgentLoopDueScanPage> {
        Box::pin(async move {
            validate_due_scan_request(at_ms, after.as_ref(), scan_limit, tenant_id.as_deref())?;
            let data = self.data.lock().await;
            let start = match &after {
                Some(after) => Bound::Excluded(AgentLoopDueIndexKey {
                    tenant_id: tenant_id.clone(),
                    due_at_ms: after.due_at_ms,
                    thread_id: after.thread_id.clone(),
                    turn_id: after.turn_id.clone(),
                    wait_id: after.wait_id.clone(),
                }),
                None => Bound::Included(AgentLoopDueIndexKey {
                    tenant_id: tenant_id.clone(),
                    due_at_ms: 0,
                    thread_id: ThreadId::from_string(String::new()),
                    turn_id: TurnId::from_string(String::new()),
                    wait_id: AgentLoopWaitId::from_string(String::new()),
                }),
            };
            let mut due = Vec::with_capacity(scan_limit.saturating_add(1));
            for key in data.agent_loop_due.range((start, Bound::Unbounded)) {
                if key.tenant_id != tenant_id || key.due_at_ms > at_ms {
                    break;
                }
                let projection = data.agent_loop_waits.get(&key.thread_id).ok_or_else(|| {
                    HarnessError::State(
                        "Memory Agent Loop due index has no live projection".to_owned(),
                    )
                })?;
                if projection.due_index_key().as_ref() != Some(key) {
                    return Err(HarnessError::State(
                        "Memory Agent Loop due index differs from its live projection".to_owned(),
                    ));
                }
                let stream_version = data
                    .stream_versions
                    .get(&key.thread_id)
                    .copied()
                    .ok_or_else(|| {
                        HarnessError::State(
                            "Memory Agent Loop due row has no stream version".to_owned(),
                        )
                    })?;
                let recovery_bytes = data
                    .stream_recovery_bytes
                    .get(&key.thread_id)
                    .copied()
                    .ok_or_else(|| {
                        HarnessError::State(
                            "Memory Agent Loop due row has no recovery fence".to_owned(),
                        )
                    })?;
                due.push(projection.due_wait(stream_version, recovery_bytes)?);
                if due.len() > scan_limit {
                    break;
                }
            }
            let has_more = due.len() > scan_limit;
            if has_more {
                due.pop();
            }
            let next_cursor = due.last().map(AgentLoopDueWait::cursor);
            let page = AgentLoopDueScanPage {
                scanned: due.len(),
                due,
                next_cursor,
                has_more,
            };
            page.validate(at_ms, after.as_ref(), scan_limit, tenant_id.as_deref())?;
            Ok(page)
        })
    }

    fn event_by_id<'a>(&'a self, event_id: &'a EventId) -> HarnessFuture<'a, Option<StoredEvent>> {
        Box::pin(async move {
            let data = self.data.lock().await;
            let Some(position) = data.event_positions.get(event_id).copied() else {
                return Ok(None);
            };
            data.events.get(position).cloned().map(Some).ok_or_else(|| {
                HarnessError::State("Memory Event Store identity index is corrupt".to_owned())
            })
        })
    }
}

/// SQLite-backed append-only Event Store.
pub struct SqliteEventStore {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteEventStore {
    /// Validates one existing State database without creating or mutating it.
    ///
    /// Missing paths are errors. Hosts that treat absence as a fresh store
    /// should check existence before calling this preflight.
    pub async fn validate_existing(path: impl AsRef<Path>) -> Result<(), HarnessError> {
        let path = path.as_ref().to_owned();
        task::spawn_blocking(move || {
            let connection =
                open_read_only(&path).map_err(|error| HarnessError::State(error.to_string()))?;
            configure_sqlite_busy_timeout(&connection)?;
            migration::validate_or_bootstrap_store(&connection)
        })
        .await
        .map_err(|error| HarnessError::State(format!("SQLite validation task failed: {error}")))?
    }

    /// Opens or creates a database and enforces required durability pragmas.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_owned();
        let connection = task::spawn_blocking(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
            }

            let connection =
                Connection::open(path).map_err(|error| HarnessError::State(error.to_string()))?;
            configure_sqlite_busy_timeout(&connection)?;
            migration::validate_or_bootstrap_store(&connection)?;
            configure_sqlite_connection(&connection)?;
            let schema = format!(
                "
                    CREATE TABLE IF NOT EXISTS events (
                        sequence       INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id       TEXT NOT NULL UNIQUE,
                        thread_id      TEXT NOT NULL,
                        recorded_at_ms INTEGER NOT NULL,
                        schema_version INTEGER NOT NULL,
                        event_json     TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS events_thread_sequence
                        ON events(thread_id, sequence);
                    CREATE TABLE IF NOT EXISTS streams (
                        thread_id TEXT PRIMARY KEY,
                        version   INTEGER NOT NULL CHECK(version >= 0),
                        name      TEXT,
                        tenant_id TEXT
                    );
                    CREATE TABLE IF NOT EXISTS stream_recovery (
                        thread_id      TEXT PRIMARY KEY,
                        recovery_bytes INTEGER NOT NULL CHECK(recovery_bytes >= 0),
                        FOREIGN KEY(thread_id) REFERENCES streams(thread_id)
                    );
                    CREATE TABLE IF NOT EXISTS state_snapshots (
                        thread_id       TEXT PRIMARY KEY,
                        stream_version  INTEGER NOT NULL CHECK(stream_version > 0),
                        snapshot_json   TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS agent_loop_wait_projection (
                        thread_id                  TEXT PRIMARY KEY,
                        tenant_key                 TEXT NOT NULL,
                        turn_id                    TEXT NOT NULL,
                        wait_id                    TEXT NOT NULL,
                        revision                   INTEGER NOT NULL CHECK(revision > 0),
                        phase                      INTEGER NOT NULL CHECK(phase IN (1, 2, 3)),
                        due_at_ms                  INTEGER CHECK(due_at_ms > 0),
                        approval_id                TEXT NOT NULL,
                        envelope_sha256            TEXT NOT NULL
                                                   CHECK(length(envelope_sha256) = 64),
                        wait_started_event_id       TEXT NOT NULL,
                        current_transition_event_id TEXT NOT NULL,
                        resume_command_id           TEXT,
                        FOREIGN KEY(thread_id) REFERENCES streams(thread_id),
                        FOREIGN KEY(wait_started_event_id) REFERENCES events(event_id),
                        FOREIGN KEY(current_transition_event_id) REFERENCES events(event_id)
                    );
                    CREATE INDEX IF NOT EXISTS agent_loop_wait_due
                        ON agent_loop_wait_projection(
                            tenant_key, due_at_ms, thread_id, turn_id, wait_id
                        )
                        WHERE due_at_ms IS NOT NULL;
                    CREATE TABLE IF NOT EXISTS inbox_repair_outbox (
                        op_id           TEXT PRIMARY KEY,
                        wait_id         TEXT NOT NULL,
                        op_kind         TEXT NOT NULL CHECK (op_kind IN ('submit','settle','orphan_close')),
                        payload_json    BLOB NOT NULL,
                        status          TEXT NOT NULL CHECK (status IN ('pending','in_flight','succeeded','exhausted')),
                        attempt_count   INTEGER NOT NULL DEFAULT 0,
                        last_attempt_ms INTEGER,
                        next_attempt_ms INTEGER NOT NULL,
                        last_error      TEXT,
                        created_ms      INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_outbox_pending
                        ON inbox_repair_outbox(next_attempt_ms) WHERE status = 'pending';
                    CREATE INDEX IF NOT EXISTS idx_outbox_wait
                        ON inbox_repair_outbox(wait_id);
                    CREATE TABLE IF NOT EXISTS inbox_orphan_tombstone (
                        wait_id         TEXT PRIMARY KEY,
                        tombstoned_ms   INTEGER NOT NULL,
                        reason          TEXT NOT NULL CHECK (reason IN ('settled','cancelled','timeout','denied','terminal_failure')),
                        source_revision INTEGER NOT NULL
                    );
                    CREATE VIEW IF NOT EXISTS inbox_retry_age_view AS
                        SELECT op_id, wait_id, op_kind, status, attempt_count,
                               (CAST((strftime('%s','now')*1000) AS INTEGER) - last_attempt_ms) AS age_ms,
                               last_error
                        FROM inbox_repair_outbox
                        WHERE status IN ('pending','in_flight');
                    INSERT OR IGNORE INTO streams (thread_id, version)
                        SELECT thread_id, COUNT(*)
                        FROM events
                        GROUP BY thread_id;
                    INSERT OR IGNORE INTO stream_recovery (thread_id, recovery_bytes)
                        SELECT thread_id,
                               COALESCE(SUM(
                                   length(CAST(event_json AS BLOB))
                                   + {STATE_EVENT_RECOVERY_OVERHEAD_BYTES}
                               ), 0)
                        FROM events
                        GROUP BY thread_id;
                    ",
            );
            connection
                .execute_batch(&schema)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            migration::ensure_stream_name_column_for_bootstrap(&connection)?;
            migration::ensure_stream_tenant_column_for_bootstrap(&connection)?;
            connection
                .execute_batch(&migration::metadata_schema_sql())
                .map_err(|error| HarnessError::State(error.to_string()))?;
            Ok(connection)
        })
        .await
        .map_err(|error| {
            HarnessError::State(format!("SQLite initialization task failed: {error}"))
        })??;

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
            let mut connection = connection
                .lock()
                .map_err(|_| HarnessError::State("SQLite connection lock poisoned".to_owned()))?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| HarnessError::State(format!("SQLite task failed: {error}")))?
    }
}

fn configure_sqlite_connection(connection: &Connection) -> Result<(), HarnessError> {
    configure_sqlite_busy_timeout(connection)?;
    let mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|error| HarnessError::State(error.to_string()))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(HarnessError::State(format!(
            "SQLite refused WAL mode and selected {mode}"
        )));
    }
    configure_sqlite_session(connection)
}

pub(super) fn configure_sqlite_busy_timeout(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| HarnessError::State(error.to_string()))
}

pub(super) fn configure_sqlite_session(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .execute_batch(
            "
            PRAGMA synchronous = FULL;
            PRAGMA foreign_keys = ON;
            ",
        )
        .map_err(|error| HarnessError::State(error.to_string()))
}

fn load_sqlite_wait_projection(
    transaction: &Transaction<'_>,
    thread_id: &ThreadId,
) -> Result<Option<AgentLoopWaitProjection>, HarnessError> {
    let row = transaction
        .query_row(
            "SELECT phase,
                    length(CAST(tenant_key AS BLOB)), tenant_key,
                    length(CAST(turn_id AS BLOB)), turn_id,
                    length(CAST(wait_id AS BLOB)), wait_id,
                    revision, due_at_ms,
                    length(CAST(approval_id AS BLOB)), approval_id,
                    length(CAST(envelope_sha256 AS BLOB)), envelope_sha256,
                    length(CAST(wait_started_event_id AS BLOB)), wait_started_event_id,
                    length(CAST(current_transition_event_id AS BLOB)), current_transition_event_id,
                    length(CAST(resume_command_id AS BLOB)), resume_command_id
             FROM agent_loop_wait_projection
             WHERE thread_id = ?1",
            [thread_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    bounded_text(row, 1, 2, 128, "Agent Loop wait tenant")?,
                    bounded_text(row, 3, 4, 256, "Agent Loop wait Turn")?,
                    bounded_text(row, 5, 6, 256, "Agent Loop wait identity")?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    bounded_text(row, 9, 10, 256, "Agent Loop Approval identity")?,
                    bounded_text(row, 11, 12, 64, "Agent Loop envelope digest")?,
                    bounded_text(row, 13, 14, 256, "Agent Loop wait-start event")?,
                    bounded_text(row, 15, 16, 256, "Agent Loop transition event")?,
                    bounded_optional_text(row, 17, 18, 256, "Agent Loop resume command")?,
                ))
            },
        )
        .optional()
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let Some((
        phase,
        tenant_key,
        turn_id,
        wait_id,
        revision,
        due_at_ms,
        approval_id,
        envelope_sha256,
        wait_started_event_id,
        current_transition_event_id,
        resume_command_id,
    )) = row
    else {
        return Ok(None);
    };
    let projection = AgentLoopWaitProjection {
        phase: AgentLoopDuePhase::from_sql(phase)?,
        tenant_id: (!tenant_key.is_empty()).then_some(tenant_key),
        thread_id: thread_id.clone(),
        turn_id: TurnId::from_string(turn_id),
        wait_id: AgentLoopWaitId::from_string(wait_id),
        revision: u64::try_from(revision)
            .map_err(|_| HarnessError::State("negative Agent Loop wait revision".to_owned()))?,
        due_at_ms: due_at_ms
            .map(|value| {
                u64::try_from(value)
                    .map_err(|_| HarnessError::State("negative Agent Loop due time".to_owned()))
            })
            .transpose()?,
        approval_id: crate::ApprovalId::from_string(approval_id),
        envelope_sha256,
        wait_started_event_id: EventId::from_string(wait_started_event_id),
        current_transition_event_id: EventId::from_string(current_transition_event_id),
        resume_command_id: resume_command_id.map(AgentLoopResumeCommandId::from_string),
    };
    projection.validate()?;
    Ok(Some(projection))
}

fn apply_sqlite_wait_projection(
    transaction: &Transaction<'_>,
    stored: &StoredEvent,
) -> Result<(), HarnessError> {
    let current = load_sqlite_wait_projection(transaction, &stored.thread_id)?;
    match projection_change(stored, current.as_ref())? {
        AgentLoopWaitProjectionChange::Unchanged => Ok(()),
        AgentLoopWaitProjectionChange::Upsert(next) => {
            let due_at_ms = next
                .due_at_ms
                .map(|value| sqlite_state_u64(value, "Agent Loop due time"))
                .transpose()?;
            let revision = sqlite_state_u64(next.revision, "Agent Loop wait revision")?;
            let tenant_key = next.tenant_id.as_deref().unwrap_or("");
            let changed = if let Some(previous) = current {
                if previous.tenant_id != next.tenant_id
                    || previous.thread_id != next.thread_id
                    || previous.turn_id != next.turn_id
                    || previous.wait_id != next.wait_id
                    || previous.approval_id != next.approval_id
                    || previous.envelope_sha256 != next.envelope_sha256
                    || previous.wait_started_event_id != next.wait_started_event_id
                {
                    return Err(HarnessError::State(
                        "Agent Loop wait projection attempted to change immutable coordinates"
                            .to_owned(),
                    ));
                }
                transaction
                    .execute(
                        "UPDATE agent_loop_wait_projection
                         SET revision = ?2, phase = ?3, due_at_ms = ?4,
                             current_transition_event_id = ?5, resume_command_id = ?6
                         WHERE thread_id = ?1 AND turn_id = ?7 AND wait_id = ?8
                           AND revision = ?9 AND phase = ?10
                           AND current_transition_event_id = ?11",
                        params![
                            next.thread_id.as_str(),
                            revision,
                            next.phase.as_sql(),
                            due_at_ms,
                            next.current_transition_event_id.as_str(),
                            next.resume_command_id
                                .as_ref()
                                .map(AgentLoopResumeCommandId::as_str),
                            previous.turn_id.as_str(),
                            previous.wait_id.as_str(),
                            sqlite_state_u64(previous.revision, "Agent Loop wait revision")?,
                            previous.phase.as_sql(),
                            previous.current_transition_event_id.as_str(),
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?
            } else {
                transaction
                    .execute(
                        "INSERT INTO agent_loop_wait_projection
                            (thread_id, tenant_key, turn_id, wait_id, revision, phase,
                             due_at_ms, approval_id, envelope_sha256,
                             wait_started_event_id, current_transition_event_id,
                             resume_command_id)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                        params![
                            next.thread_id.as_str(),
                            tenant_key,
                            next.turn_id.as_str(),
                            next.wait_id.as_str(),
                            revision,
                            next.phase.as_sql(),
                            due_at_ms,
                            next.approval_id.as_str(),
                            next.envelope_sha256,
                            next.wait_started_event_id.as_str(),
                            next.current_transition_event_id.as_str(),
                            next.resume_command_id
                                .as_ref()
                                .map(AgentLoopResumeCommandId::as_str),
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?
            };
            if changed != 1 {
                return Err(HarnessError::State(
                    "Agent Loop wait projection CAS did not affect exactly one row".to_owned(),
                ));
            }
            Ok(())
        }
        AgentLoopWaitProjectionChange::Delete(previous) => {
            let changed = transaction
                .execute(
                    "DELETE FROM agent_loop_wait_projection
                     WHERE thread_id = ?1 AND turn_id = ?2 AND wait_id = ?3
                       AND revision = ?4 AND phase = ?5
                       AND current_transition_event_id = ?6",
                    params![
                        previous.thread_id.as_str(),
                        previous.turn_id.as_str(),
                        previous.wait_id.as_str(),
                        sqlite_state_u64(previous.revision, "Agent Loop wait revision")?,
                        previous.phase.as_sql(),
                        previous.current_transition_event_id.as_str(),
                    ],
                )
                .map_err(|error| HarnessError::State(error.to_string()))?;
            if changed != 1 {
                return Err(HarnessError::State(
                    "Agent Loop wait projection delete CAS did not affect exactly one row"
                        .to_owned(),
                ));
            }
            Ok(())
        }
    }
}

fn sqlite_state_u64(value: u64, kind: &str) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| HarnessError::State(format!("{kind} exceeds SQLite INTEGER")))
}

impl EventStore for SqliteEventStore {
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
        Box::pin(async move {
            self.append_with_disposition(pending)
                .await
                .map(|result| result.stored)
        })
    }

    fn append_with_disposition<'a>(
        &'a self,
        pending: PendingEvent,
    ) -> HarnessFuture<'a, EventAppendResult> {
        Box::pin(async move {
            let encoded = validate_pending_event(&pending)?;
            self.with_connection(move |connection| {
                let recorded_at_ms = i64::try_from(pending.recorded_at_ms).map_err(|_| {
                    HarnessError::State("timestamp exceeds SQLite INTEGER".to_owned())
                })?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                let existing = transaction
                    .query_row(
                        "SELECT sequence,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                recorded_at_ms, schema_version,
                                length(CAST(event_json AS BLOB)), event_json
                         FROM events WHERE event_id = ?1",
                        [pending.event_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                bounded_text(row, 1, 2, 256, "State thread identity")?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                                bounded_text(row, 5, 6, MAX_STATE_EVENT_BYTES, "State event")?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                if let Some(row) = existing {
                    let stored = decode_row(pending.event_id.clone(), row)?;
                    return matching_existing(&stored, &pending).map(|stored| EventAppendResult {
                        stored,
                        disposition: EventAppendDisposition::Duplicate,
                    });
                }

                let actual_stream_version: Option<i64> = transaction
                    .query_row(
                        "SELECT version FROM streams WHERE thread_id = ?1",
                        [pending.thread_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let actual_stream_version = u64::try_from(actual_stream_version.unwrap_or(0))
                    .map_err(|_| HarnessError::State("negative stream version".to_owned()))?;
                validate_stream_version(actual_stream_version, &pending)?;
                let actual_recovery_bytes: Option<i64> = transaction
                    .query_row(
                        "SELECT recovery_bytes FROM stream_recovery WHERE thread_id = ?1",
                        [pending.thread_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let actual_recovery_bytes = match (actual_stream_version, actual_recovery_bytes) {
                    (0, None) => 0,
                    (0, Some(_)) => {
                        return Err(HarnessError::State(
                            "orphaned stream recovery metadata".to_owned(),
                        ));
                    }
                    (_, None) => {
                        return Err(HarnessError::State(
                            "missing stream recovery metadata".to_owned(),
                        ));
                    }
                    (_, Some(bytes)) => u64::try_from(bytes).map_err(|_| {
                        HarnessError::State("negative stream recovery charge".to_owned())
                    })?,
                };
                validate_stream_recovery_bytes(actual_recovery_bytes, &pending)?;
                let thread_exists = actual_stream_version > 0;
                validate_thread_existence(thread_exists, &pending)?;
                let next_stream_version = actual_stream_version
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::State("stream version overflow".to_owned()))?;
                let next_recovery_bytes = actual_recovery_bytes
                    .checked_add(encoded.recovery_bytes)
                    .ok_or_else(|| {
                        HarnessError::State("stream recovery charge overflow".to_owned())
                    })?;

                transaction
                    .execute(
                        "INSERT INTO events
                            (event_id, thread_id, recorded_at_ms, schema_version, event_json)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            pending.event_id.as_str(),
                            pending.thread_id.as_str(),
                            recorded_at_ms,
                            i64::from(STATE_EVENT_SCHEMA_VERSION),
                            encoded.json
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                // Capture the authoritative event row before inserts into
                // auxiliary rowid tables can change last_insert_rowid().
                let sequence = u64::try_from(transaction.last_insert_rowid())
                    .map_err(|_| HarnessError::State("negative SQLite sequence".to_owned()))?;
                transaction
                    .execute(
                        "INSERT INTO streams (thread_id, version, tenant_id) VALUES (?1, ?2, ?3)
                         ON CONFLICT(thread_id) DO UPDATE SET version = excluded.version",
                        params![
                            pending.thread_id.as_str(),
                            i64::try_from(next_stream_version).map_err(|_| {
                                HarnessError::State(
                                    "stream version exceeds SQLite INTEGER".to_owned(),
                                )
                            })?,
                            match &pending.event {
                                StateEvent::ThreadCreated { tenant_id, .. } => tenant_id.as_deref(),
                                _ => None,
                            }
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                if let StateEvent::ThreadNamed { name } = &pending.event {
                    let changed = transaction
                        .execute(
                            "UPDATE streams SET name = ?2 WHERE thread_id = ?1",
                            params![pending.thread_id.as_str(), name],
                        )
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                    if changed != 1 {
                        return Err(HarnessError::State(
                            "Thread name projection row is missing".to_owned(),
                        ));
                    }
                }
                transaction
                    .execute(
                        "INSERT INTO stream_recovery (thread_id, recovery_bytes)
                         VALUES (?1, ?2)
                         ON CONFLICT(thread_id) DO UPDATE SET
                            recovery_bytes = excluded.recovery_bytes",
                        params![
                            pending.thread_id.as_str(),
                            i64::try_from(next_recovery_bytes).map_err(|_| {
                                HarnessError::State(
                                    "stream recovery charge exceeds SQLite INTEGER".to_owned(),
                                )
                            })?
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let stored = StoredEvent {
                    schema_version: STATE_EVENT_SCHEMA_VERSION,
                    sequence,
                    event_id: pending.event_id,
                    thread_id: pending.thread_id,
                    recorded_at_ms: pending.recorded_at_ms,
                    event: pending.event,
                };
                apply_sqlite_wait_projection(&transaction, &stored)?;
                transaction
                    .commit()
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                Ok(EventAppendResult {
                    stored,
                    disposition: EventAppendDisposition::Applied,
                })
            })
            .await
        })
    }

    fn supports_atomic_stream_creation(&self) -> bool {
        true
    }

    fn create_stream_atomic<'a>(
        &'a self,
        thread_id: ThreadId,
        events: Vec<NewStreamEvent>,
    ) -> HarnessFuture<'a, Vec<StoredEvent>> {
        Box::pin(async move {
            let encoded = validate_new_stream(&thread_id, &events)?;
            let recovery_bytes = encoded.iter().try_fold(0_u64, |total, event| {
                total.checked_add(event.recovery_bytes).ok_or_else(|| {
                    HarnessError::State("stream recovery charge overflow".to_owned())
                })
            })?;
            let tenant_id = final_new_stream_tenant(&events)?;
            let prepared = events
                .into_iter()
                .zip(encoded)
                .map(|(event, encoded)| {
                    let recorded_at_ms = i64::try_from(event.recorded_at_ms).map_err(|_| {
                        HarnessError::State("timestamp exceeds SQLite INTEGER".to_owned())
                    })?;
                    Ok((event, recorded_at_ms, encoded.json))
                })
                .collect::<Result<Vec<_>, HarnessError>>()?;
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let actual: Option<i64> = transaction
                    .query_row(
                        "SELECT version FROM streams WHERE thread_id = ?1",
                        [thread_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                if let Some(actual) = actual {
                    return Err(HarnessError::StateConflict {
                        thread_id,
                        expected: 0,
                        actual: u64::try_from(actual).map_err(|_| {
                            HarnessError::State("negative stream version".to_owned())
                        })?,
                    });
                }

                let stream_version = u64::try_from(prepared.len())
                    .map_err(|_| HarnessError::State("stream version exceeds u64".to_owned()))?;
                let stream_version_sql = i64::try_from(stream_version).map_err(|_| {
                    HarnessError::State("stream version exceeds SQLite INTEGER".to_owned())
                })?;
                let recovery_bytes_sql = i64::try_from(recovery_bytes).map_err(|_| {
                    HarnessError::State("stream recovery charge exceeds SQLite INTEGER".to_owned())
                })?;
                transaction
                    .execute(
                        "INSERT INTO streams (thread_id, version, tenant_id)
                         VALUES (?1, ?2, ?3)",
                        params![thread_id.as_str(), stream_version_sql, tenant_id.as_deref()],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                let mut stored = Vec::with_capacity(prepared.len());
                {
                    let mut insert = transaction
                        .prepare(
                            "INSERT INTO events
                                (event_id, thread_id, recorded_at_ms, schema_version, event_json)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                        )
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                    for (new, recorded_at_ms, json) in prepared {
                        insert
                            .execute(params![
                                new.event_id.as_str(),
                                thread_id.as_str(),
                                recorded_at_ms,
                                i64::from(new.schema_version),
                                json
                            ])
                            .map_err(|error| HarnessError::State(error.to_string()))?;
                        let sequence =
                            u64::try_from(transaction.last_insert_rowid()).map_err(|_| {
                                HarnessError::State("negative SQLite sequence".to_owned())
                            })?;
                        stored.push(StoredEvent {
                            schema_version: new.schema_version,
                            sequence,
                            event_id: new.event_id,
                            thread_id: thread_id.clone(),
                            recorded_at_ms: new.recorded_at_ms,
                            event: new.event,
                        });
                    }
                }
                if let Some(name) = final_stream_name(&stored) {
                    let changed = transaction
                        .execute(
                            "UPDATE streams SET name = ?2 WHERE thread_id = ?1",
                            params![thread_id.as_str(), name],
                        )
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                    if changed != 1 {
                        return Err(HarnessError::State(
                            "Thread name projection row is missing".to_owned(),
                        ));
                    }
                }
                transaction
                    .execute(
                        "INSERT INTO stream_recovery (thread_id, recovery_bytes)
                         VALUES (?1, ?2)",
                        params![thread_id.as_str(), recovery_bytes_sql],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                Ok(stored)
            })
            .await
        })
    }

    fn events_page<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        after_sequence: u64,
        limit: usize,
        max_recovery_bytes: u64,
    ) -> HarnessFuture<'a, Vec<StoredEvent>> {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            validate_event_page_request(limit, max_recovery_bytes)?;
            let after_sequence = i64::try_from(after_sequence).map_err(|_| {
                HarnessError::State("event cursor exceeds SQLite INTEGER".to_owned())
            })?;
            let limit = i64::try_from(limit).map_err(|_| {
                HarnessError::State("event page limit exceeds SQLite INTEGER".to_owned())
            })?;
            self.with_connection(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT sequence,
                                length(CAST(event_id AS BLOB)), event_id,
                                recorded_at_ms, schema_version,
                                length(CAST(event_json AS BLOB)), event_json
                         FROM events
                         WHERE thread_id = ?1 AND sequence > ?2
                         ORDER BY sequence
                         LIMIT ?3",
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let rows = statement
                    .query_map(params![thread_id.as_str(), after_sequence, limit], |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            bounded_text(row, 1, 2, 256, "State event identity")?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            bounded_text(row, 5, 6, MAX_STATE_EVENT_BYTES, "State event")?,
                        ))
                    })
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                let mut events = Vec::new();
                let mut recovery_bytes = 0_u64;
                for row in rows {
                    let (sequence, event_id, recorded_at_ms, schema_version, event_json) =
                        row.map_err(|error| HarnessError::State(error.to_string()))?;
                    let event = decode_row(
                        EventId::from_string(event_id),
                        (
                            sequence,
                            thread_id.as_str().to_owned(),
                            recorded_at_ms,
                            schema_version,
                            event_json,
                        ),
                    )?;
                    let event_bytes = stored_event_recovery_bytes(&event)?;
                    let next = recovery_bytes.checked_add(event_bytes).ok_or_else(|| {
                        HarnessError::State("event page charge overflow".to_owned())
                    })?;
                    if next > max_recovery_bytes {
                        if events.is_empty() {
                            return Err(HarnessError::State(
                                "next State event exceeds the requested page byte budget"
                                    .to_owned(),
                            ));
                        }
                        break;
                    }
                    recovery_bytes = next;
                    events.push(event);
                }
                Ok(events)
            })
            .await
        })
    }

    fn supports_thread_listing(&self) -> bool {
        true
    }

    fn thread_summaries_page(
        &self,
        tenant_id: Option<String>,
        before_sequence: Option<u64>,
        limit: usize,
    ) -> HarnessFuture<'_, Vec<ThreadSummary>> {
        Box::pin(async move {
            validate_thread_summary_page_request(before_sequence, limit)?;
            let before_sequence = before_sequence
                .map(i64::try_from)
                .transpose()
                .map_err(|_| {
                    HarnessError::State("Thread cursor exceeds SQLite INTEGER".to_owned())
                })?
                .unwrap_or(i64::MAX);
            let limit = i64::try_from(limit).map_err(|_| {
                HarnessError::State("Thread page limit exceeds SQLite INTEGER".to_owned())
            })?;
            self.with_connection(move |connection| {
                let mut statement = connection
                    .prepare(
                        "WITH recent AS (
                             SELECT length(CAST(events.thread_id AS BLOB)) AS thread_id_bytes,
                                    events.thread_id,
                                    events.sequence AS last_sequence,
                                    events.recorded_at_ms AS updated_at_ms,
                                    streams.version,
                                    length(CAST(streams.name AS BLOB)) AS name_bytes,
                                    streams.name
                             FROM events
                             JOIN streams ON streams.thread_id = events.thread_id
                             JOIN (
                                 SELECT thread_id, MAX(sequence) AS last_sequence
                                 FROM events
                                 GROUP BY thread_id
                             ) AS latest
                               ON latest.thread_id = events.thread_id
                              AND latest.last_sequence = events.sequence
                             WHERE events.sequence < ?1
                               AND streams.tenant_id IS ?2
                             ORDER BY events.sequence DESC
                             LIMIT ?3
                         )
                         SELECT recent.thread_id_bytes,
                                recent.thread_id,
                                recent.last_sequence,
                                recent.updated_at_ms,
                                recent.version,
                                recent.name_bytes,
                                recent.name,
                                length(CAST(lineage.event_id AS BLOB)),
                                lineage.event_id,
                                lineage.sequence,
                                lineage.recorded_at_ms,
                                lineage.schema_version,
                                length(CAST(lineage.event_json AS BLOB)),
                                lineage.event_json
                         FROM recent
                         LEFT JOIN events AS lineage
                           ON lineage.sequence = (
                               SELECT candidate.sequence
                               FROM events AS candidate
                               WHERE candidate.thread_id = recent.thread_id
                               ORDER BY candidate.sequence
                               LIMIT 1 OFFSET 1
                           )
                         ORDER BY recent.last_sequence DESC",
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let rows = statement
                    .query_map(params![before_sequence, tenant_id, limit], |row| {
                        Ok((
                            bounded_text(row, 0, 1, 256, "State thread identity")?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                            bounded_optional_text(row, 5, 6, MAX_THREAD_NAME_BYTES, "Thread name")?,
                            bounded_optional_text(row, 7, 8, 256, "Thread lineage event identity")?,
                            row.get::<_, Option<i64>>(9)?,
                            row.get::<_, Option<i64>>(10)?,
                            row.get::<_, Option<i64>>(11)?,
                            bounded_optional_text(
                                row,
                                12,
                                13,
                                MAX_STATE_EVENT_BYTES,
                                "Thread lineage event",
                            )?,
                        ))
                    })
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let mut page = Vec::new();
                for row in rows {
                    let (
                        thread_id,
                        last_sequence,
                        updated_at_ms,
                        stream_version,
                        name,
                        lineage_event_id,
                        lineage_sequence,
                        lineage_recorded_at_ms,
                        lineage_schema_version,
                        lineage_event_json,
                    ) = row.map_err(|error| HarnessError::State(error.to_string()))?;
                    validate_thread_name(name.as_deref())?;
                    let summary_thread_id = ThreadId::from_string(thread_id.clone());
                    let lineage = match (
                        lineage_event_id,
                        lineage_sequence,
                        lineage_recorded_at_ms,
                        lineage_schema_version,
                        lineage_event_json,
                    ) {
                        (
                            Some(event_id),
                            Some(sequence),
                            Some(recorded_at_ms),
                            Some(schema_version),
                            Some(event_json),
                        ) => {
                            let event = decode_row(
                                EventId::from_string(event_id),
                                (
                                    sequence,
                                    thread_id,
                                    recorded_at_ms,
                                    schema_version,
                                    event_json,
                                ),
                            )?;
                            if event.thread_id != summary_thread_id {
                                return Err(HarnessError::State(
                                    "Thread lineage event belongs to another stream".to_owned(),
                                ));
                            }
                            match event.event {
                                StateEvent::ThreadForked { lineage } => Some(lineage),
                                _ => None,
                            }
                        }
                        (None, None, None, None, None) => None,
                        _ => {
                            return Err(HarnessError::State(
                                "Thread lineage event row is incomplete".to_owned(),
                            ));
                        }
                    };
                    page.push(ThreadSummary {
                        thread_id: summary_thread_id,
                        tenant_id: tenant_id.clone(),
                        name,
                        lineage,
                        last_sequence: u64::try_from(last_sequence).map_err(|_| {
                            HarnessError::State("invalid Thread sequence".to_owned())
                        })?,
                        updated_at_ms: u64::try_from(updated_at_ms).map_err(|_| {
                            HarnessError::State("invalid Thread timestamp".to_owned())
                        })?,
                        stream_version: u64::try_from(stream_version).map_err(|_| {
                            HarnessError::State("invalid Thread stream version".to_owned())
                        })?,
                    });
                }
                Ok(page)
            })
            .await
        })
    }

    fn thread_accessible<'a>(
        &'a self,
        thread_id: &'a ThreadId,
        tenant_id: Option<String>,
    ) -> HarnessFuture<'a, bool> {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            self.with_connection(move |connection| {
                connection
                    .query_row(
                        "SELECT 1 FROM streams
                         WHERE thread_id = ?1 AND tenant_id IS ?2",
                        params![thread_id.as_str(), tenant_id],
                        |_| Ok(()),
                    )
                    .optional()
                    .map(|row| row.is_some())
                    .map_err(|error| HarnessError::State(error.to_string()))
            })
            .await
        })
    }

    fn load_snapshot<'a>(
        &'a self,
        thread_id: &'a ThreadId,
    ) -> HarnessFuture<'a, Option<StateSnapshot>> {
        let thread_id = thread_id.clone();
        Box::pin(async move {
            self.with_connection(move |connection| {
                let stored = connection
                    .query_row(
                        "SELECT stream_version,
                                length(CAST(snapshot_json AS BLOB)), snapshot_json
                         FROM state_snapshots WHERE thread_id = ?1",
                        [thread_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                bounded_text(
                                    row,
                                    1,
                                    2,
                                    MAX_STATE_SNAPSHOT_BYTES,
                                    "State snapshot",
                                )?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let Some((stream_version, snapshot_json)) = stored else {
                    return Ok(None);
                };
                if snapshot_json.len() > MAX_STATE_SNAPSHOT_BYTES {
                    return Err(HarnessError::State(format!(
                        "stored State snapshot exceeds {MAX_STATE_SNAPSHOT_BYTES} bytes"
                    )));
                }
                let snapshot: StateSnapshot = serde_json::from_str(&snapshot_json)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let stored_stream_version = u64::try_from(stream_version).map_err(|_| {
                    HarnessError::State("invalid snapshot stream version".to_owned())
                })?;
                if snapshot.stream_version != stored_stream_version {
                    return Err(HarnessError::State(
                        "snapshot stream-version index does not match its body".to_owned(),
                    ));
                }
                validate_snapshot(&snapshot)?;
                Ok(Some(snapshot))
            })
            .await
        })
    }

    fn save_snapshot<'a>(&'a self, snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            let snapshot_json = encode_snapshot(&snapshot)?;
            let thread_id = snapshot.thread.id.clone();
            let stream_version = i64::try_from(snapshot.stream_version).map_err(|_| {
                HarnessError::State("snapshot stream version exceeds SQLite INTEGER".to_owned())
            })?;
            self.with_connection(move |connection| {
                connection
                    .execute(
                        "INSERT INTO state_snapshots
                            (thread_id, stream_version, snapshot_json)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT(thread_id) DO UPDATE SET
                            stream_version = excluded.stream_version,
                            snapshot_json = excluded.snapshot_json
                         WHERE excluded.stream_version >= state_snapshots.stream_version",
                        params![thread_id.as_str(), stream_version, snapshot_json],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                Ok(())
            })
            .await
        })
    }

    fn supports_agent_loop_wait_projection(&self) -> bool {
        true
    }

    fn scan_due_agent_loop_waits<'a>(
        &'a self,
        tenant_id: Option<String>,
        at_ms: u64,
        after: Option<AgentLoopDueCursor>,
        scan_limit: usize,
    ) -> HarnessFuture<'a, AgentLoopDueScanPage> {
        Box::pin(async move {
            validate_due_scan_request(at_ms, after.as_ref(), scan_limit, tenant_id.as_deref())?;
            let at_ms_sql = sqlite_state_u64(at_ms, "Agent Loop scan time")?;
            let query_limit = scan_limit
                .checked_add(1)
                .filter(|limit| *limit <= MAX_AGENT_LOOP_DUE_SCAN_LIMIT + 1)
                .ok_or_else(|| HarnessError::State("Agent Loop scan limit overflow".to_owned()))?;
            let query_limit = i64::try_from(query_limit)
                .map_err(|_| HarnessError::State("Agent Loop scan limit overflow".to_owned()))?;
            let tenant_key = tenant_id.clone().unwrap_or_default();
            let has_after = i64::from(after.is_some());
            let (after_due_at_ms, after_thread_id, after_turn_id, after_wait_id) = after
                .as_ref()
                .map(|cursor| {
                    Ok((
                        sqlite_state_u64(cursor.due_at_ms, "Agent Loop due cursor")?,
                        cursor.thread_id.as_str().to_owned(),
                        cursor.turn_id.as_str().to_owned(),
                        cursor.wait_id.as_str().to_owned(),
                    ))
                })
                .transpose()?
                .unwrap_or((0, String::new(), String::new(), String::new()));

            self.with_connection(move |connection| {
                let mut statement = connection
                    .prepare(
                        "SELECT p.phase,
                                length(CAST(p.tenant_key AS BLOB)), p.tenant_key,
                                length(CAST(p.thread_id AS BLOB)), p.thread_id,
                                length(CAST(p.turn_id AS BLOB)), p.turn_id,
                                length(CAST(p.wait_id AS BLOB)), p.wait_id,
                                p.revision, p.due_at_ms,
                                length(CAST(p.approval_id AS BLOB)), p.approval_id,
                                length(CAST(p.envelope_sha256 AS BLOB)), p.envelope_sha256,
                                length(CAST(p.wait_started_event_id AS BLOB)),
                                    p.wait_started_event_id,
                                length(CAST(p.current_transition_event_id AS BLOB)),
                                    p.current_transition_event_id,
                                length(CAST(p.resume_command_id AS BLOB)), p.resume_command_id,
                                s.version, r.recovery_bytes
                         FROM agent_loop_wait_projection AS p
                         JOIN streams AS s ON s.thread_id = p.thread_id
                         JOIN stream_recovery AS r ON r.thread_id = p.thread_id
                         WHERE p.tenant_key = ?1
                           AND p.due_at_ms IS NOT NULL
                           AND p.due_at_ms <= ?2
                           AND (
                               ?3 = 0 OR
                               (p.due_at_ms, p.thread_id, p.turn_id, p.wait_id)
                                   > (?4, ?5, ?6, ?7)
                           )
                         ORDER BY p.due_at_ms, p.thread_id, p.turn_id, p.wait_id
                         LIMIT ?8",
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let rows = statement
                    .query_map(
                        params![
                            tenant_key,
                            at_ms_sql,
                            has_after,
                            after_due_at_ms,
                            after_thread_id,
                            after_turn_id,
                            after_wait_id,
                            query_limit,
                        ],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                bounded_text(row, 1, 2, 128, "Agent Loop wait tenant")?,
                                bounded_text(row, 3, 4, 256, "Agent Loop wait Thread")?,
                                bounded_text(row, 5, 6, 256, "Agent Loop wait Turn")?,
                                bounded_text(row, 7, 8, 256, "Agent Loop wait identity")?,
                                row.get::<_, i64>(9)?,
                                row.get::<_, i64>(10)?,
                                bounded_text(row, 11, 12, 256, "Agent Loop Approval identity")?,
                                bounded_text(row, 13, 14, 64, "Agent Loop envelope digest")?,
                                bounded_text(row, 15, 16, 256, "Agent Loop wait-start event")?,
                                bounded_text(row, 17, 18, 256, "Agent Loop transition event")?,
                                bounded_optional_text(
                                    row,
                                    19,
                                    20,
                                    256,
                                    "Agent Loop resume command",
                                )?,
                                row.get::<_, i64>(21)?,
                                row.get::<_, i64>(22)?,
                            ))
                        },
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                let mut due = Vec::with_capacity(scan_limit.saturating_add(1));
                for row in rows {
                    let (
                        phase,
                        stored_tenant_key,
                        thread_id,
                        turn_id,
                        wait_id,
                        revision,
                        due_at_ms,
                        approval_id,
                        envelope_sha256,
                        wait_started_event_id,
                        current_transition_event_id,
                        resume_command_id,
                        stream_version,
                        recovery_bytes,
                    ) = row.map_err(|error| HarnessError::State(error.to_string()))?;
                    if stored_tenant_key != tenant_key {
                        return Err(HarnessError::State(
                            "Agent Loop due query crossed its tenant boundary".to_owned(),
                        ));
                    }
                    let projection = AgentLoopWaitProjection {
                        phase: AgentLoopDuePhase::from_sql(phase)?,
                        tenant_id: (!stored_tenant_key.is_empty()).then_some(stored_tenant_key),
                        thread_id: ThreadId::from_string(thread_id),
                        turn_id: TurnId::from_string(turn_id),
                        wait_id: AgentLoopWaitId::from_string(wait_id),
                        revision: u64::try_from(revision).map_err(|_| {
                            HarnessError::State("negative Agent Loop wait revision".to_owned())
                        })?,
                        due_at_ms: Some(u64::try_from(due_at_ms).map_err(|_| {
                            HarnessError::State("negative Agent Loop due time".to_owned())
                        })?),
                        approval_id: crate::ApprovalId::from_string(approval_id),
                        envelope_sha256,
                        wait_started_event_id: EventId::from_string(wait_started_event_id),
                        current_transition_event_id: EventId::from_string(
                            current_transition_event_id,
                        ),
                        resume_command_id: resume_command_id
                            .map(AgentLoopResumeCommandId::from_string),
                    };
                    projection.validate()?;
                    let stream_version = u64::try_from(stream_version).map_err(|_| {
                        HarnessError::State("negative Agent Loop stream version".to_owned())
                    })?;
                    let recovery_bytes = u64::try_from(recovery_bytes).map_err(|_| {
                        HarnessError::State("negative Agent Loop recovery fence".to_owned())
                    })?;
                    due.push(projection.due_wait(stream_version, recovery_bytes)?);
                }
                let has_more = due.len() > scan_limit;
                if has_more {
                    due.pop();
                }
                let next_cursor = due.last().map(AgentLoopDueWait::cursor);
                let page = AgentLoopDueScanPage {
                    scanned: due.len(),
                    due,
                    next_cursor,
                    has_more,
                };
                page.validate(at_ms, after.as_ref(), scan_limit, tenant_id.as_deref())?;
                Ok(page)
            })
            .await
        })
    }

    fn event_by_id<'a>(&'a self, event_id: &'a EventId) -> HarnessFuture<'a, Option<StoredEvent>> {
        let event_id = event_id.clone();
        Box::pin(async move {
            self.with_connection(move |connection| {
                let row = connection
                    .query_row(
                        "SELECT sequence,
                                length(CAST(thread_id AS BLOB)), thread_id,
                                recorded_at_ms, schema_version,
                                length(CAST(event_json AS BLOB)), event_json
                         FROM events WHERE event_id = ?1",
                        [event_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                bounded_text(row, 1, 2, 256, "State thread identity")?,
                                row.get::<_, i64>(3)?,
                                row.get::<_, i64>(4)?,
                                bounded_text(row, 5, 6, MAX_STATE_EVENT_BYTES, "State event")?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                row.map(|row| decode_row(event_id, row)).transpose()
            })
            .await
        })
    }

    fn lookup_inbox_tombstone<'a>(
        &'a self,
        wait_id: &'a AgentLoopWaitId,
    ) -> HarnessFuture<'a, Option<InboxTombstoneRecord>> {
        let wait_id = wait_id.clone();
        Box::pin(async move {
            wait_due::validate_identity("wait", wait_id.as_str())?;
            self.with_connection(move |connection| {
                let mut stmt = connection
                    .prepare(
                        "SELECT reason, source_revision, tombstoned_ms
                         FROM inbox_orphan_tombstone
                         WHERE wait_id = ?1",
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let row = stmt
                    .query_row(params![wait_id.as_str()], |row| {
                        let reason: String = row.get(0)?;
                        let source_revision: i64 = row.get(1)?;
                        let tombstoned_ms: i64 = row.get(2)?;
                        Ok((reason, source_revision, tombstoned_ms))
                    })
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                row.map(|(reason, source_revision, tombstoned_ms)| {
                    Ok(InboxTombstoneRecord {
                        wait_id: wait_id.clone(),
                        reason: decode_tombstone_reason(&reason)?,
                        source_revision: to_u64(source_revision, "tombstone source revision")?,
                        tombstoned_at_ms: to_u64(tombstoned_ms, "tombstone timestamp")?,
                    })
                })
                .transpose()
            })
            .await
        })
    }

    fn record_inbox_tombstone<'a>(
        &'a self,
        wait_id: &'a AgentLoopWaitId,
        reason: InboxTombstoneReason,
        source_revision: u64,
        tombstoned_at_ms: u64,
    ) -> HarnessFuture<'a, ()> {
        let wait_id = wait_id.clone();
        Box::pin(async move {
            wait_due::validate_identity("wait", wait_id.as_str())?;
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                transaction
                    .execute(
                        "INSERT OR REPLACE INTO inbox_orphan_tombstone
                            (wait_id, tombstoned_ms, reason, source_revision)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            wait_id.as_str(),
                            to_sql_u64("tombstone timestamp", tombstoned_at_ms)?,
                            reason.as_sql(),
                            to_sql_u64("tombstone source revision", source_revision)?,
                        ],
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                transaction
                    .commit()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                Ok(())
            })
            .await
        })
    }

    fn supports_inbox_repair_durability(&self) -> bool {
        true
    }
}

fn decode_tombstone_reason(raw: &str) -> Result<InboxTombstoneReason, HarnessError> {
    match raw {
        "settled" => Ok(InboxTombstoneReason::Settled),
        "cancelled" => Ok(InboxTombstoneReason::Cancelled),
        "timeout" => Ok(InboxTombstoneReason::Timeout),
        "denied" => Ok(InboxTombstoneReason::Denied),
        "terminal_failure" => Ok(InboxTombstoneReason::TerminalFailure),
        other => Err(HarnessError::State(format!(
            "unknown inbox tombstone reason: {other}"
        ))),
    }
}

fn to_sql_u64(label: &str, value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| HarnessError::State(format!("{label} overflows i64")))
}

fn to_u64(value: i64, label: &str) -> Result<u64, HarnessError> {
    u64::try_from(value).map_err(|_| HarnessError::State(format!("{label} is negative")))
}

struct SnapshotMaintenanceMetrics {
    scheduled: AtomicU64,
    created: AtomicU64,
    already_current: AtomicU64,
    failed: AtomicU64,
    skipped_cadence: AtomicU64,
    skipped_in_flight: AtomicU64,
    skipped_capacity: AtomicU64,
    active: AtomicUsize,
    last_created_at_ms: AtomicU64,
    last_failure_at_ms: AtomicU64,
    last_failure: AtomicU8,
}

impl SnapshotMaintenanceMetrics {
    fn new() -> Self {
        Self {
            scheduled: AtomicU64::new(0),
            created: AtomicU64::new(0),
            already_current: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            skipped_cadence: AtomicU64::new(0),
            skipped_in_flight: AtomicU64::new(0),
            skipped_capacity: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            last_created_at_ms: AtomicU64::new(0),
            last_failure_at_ms: AtomicU64::new(0),
            last_failure: AtomicU8::new(0),
        }
    }

    fn snapshot(&self) -> SnapshotMaintenanceStats {
        SnapshotMaintenanceStats {
            scheduled: self.scheduled.load(Ordering::Relaxed),
            created: self.created.load(Ordering::Acquire),
            already_current: self.already_current.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Acquire),
            skipped_cadence: self.skipped_cadence.load(Ordering::Relaxed),
            skipped_in_flight: self.skipped_in_flight.load(Ordering::Relaxed),
            skipped_capacity: self.skipped_capacity.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Acquire),
            last_created_at_ms: nonzero(self.last_created_at_ms.load(Ordering::Relaxed)),
            last_failure_at_ms: nonzero(self.last_failure_at_ms.load(Ordering::Relaxed)),
            last_failure: match self.last_failure.load(Ordering::Relaxed) {
                1 => Some(SnapshotMaintenanceFailure::Operation),
                2 => Some(SnapshotMaintenanceFailure::WorkerPanicked),
                3 => Some(SnapshotMaintenanceFailure::WorkerCancelled),
                _ => None,
            },
        }
    }

    fn record_failure(&self, failure: SnapshotMaintenanceFailure) {
        self.last_failure_at_ms
            .store(crate::kernel::now_ms(), Ordering::Relaxed);
        self.last_failure.store(
            match failure {
                SnapshotMaintenanceFailure::Operation => 1,
                SnapshotMaintenanceFailure::WorkerPanicked => 2,
                SnapshotMaintenanceFailure::WorkerCancelled => 3,
            },
            Ordering::Relaxed,
        );
        self.failed.fetch_add(1, Ordering::Release);
    }
}

#[derive(Default)]
struct SnapshotScheduleState {
    in_flight: BTreeSet<ThreadId>,
    watermarks: BTreeMap<ThreadId, u64>,
}

struct SnapshotMaintenance {
    config: SnapshotMaintenanceConfig,
    permits: Arc<Semaphore>,
    schedule: Mutex<SnapshotScheduleState>,
    metrics: SnapshotMaintenanceMetrics,
    drained: Notify,
}

impl SnapshotMaintenance {
    fn new(config: SnapshotMaintenanceConfig) -> Self {
        Self {
            config,
            permits: Arc::new(Semaphore::new(config.max_concurrency)),
            schedule: Mutex::new(SnapshotScheduleState::default()),
            metrics: SnapshotMaintenanceMetrics::new(),
            drained: Notify::new(),
        }
    }
}

enum SnapshotMaintenanceOutcome {
    Created(u64),
    AlreadyCurrent(u64),
}

#[derive(Clone)]
/// Validates State transitions and projects Thread aggregates from events.
pub struct StateEngine {
    store: Arc<dyn EventStore>,
    heads: Arc<Mutex<BTreeMap<ThreadId, StreamHead>>>,
    snapshot_maintenance: Option<Arc<SnapshotMaintenance>>,
}

#[derive(Clone)]
struct StreamHead {
    stream_version: u64,
    recovery_bytes: u64,
    last_sequence: u64,
    turns: Arc<BTreeSet<TurnId>>,
    running_turn: Option<TurnId>,
}

struct LoadedProjection {
    thread: Option<Thread>,
    stream_version: u64,
    recovery_bytes: u64,
    last_sequence: u64,
}

struct ExactDueWaitEvidence {
    envelope: TurnWaitEnvelope,
    resume: Option<ResumeEvidence>,
}

struct CheckedEvents {
    events: Vec<StoredEvent>,
    recovery_bytes: u64,
}

impl StateEngine {
    #[must_use]
    /// Creates an engine over an Event Store implementation.
    pub fn new(store: Arc<dyn EventStore>) -> Self {
        Self {
            store,
            heads: Arc::new(Mutex::new(BTreeMap::new())),
            snapshot_maintenance: None,
        }
    }

    /// Looks up the tombstone for one wait, if the underlying store
    /// persists inbox-repair durability.
    pub fn lookup_inbox_tombstone<'a>(
        &'a self,
        wait_id: &'a AgentLoopWaitId,
    ) -> HarnessFuture<'a, Option<InboxTombstoneRecord>> {
        self.store.lookup_inbox_tombstone(wait_id)
    }

    /// Records an inbox-orphan tombstone for one wait. Compatibility
    /// stores no-op; SQLite writes the row.
    pub fn record_inbox_tombstone<'a>(
        &'a self,
        wait_id: &'a AgentLoopWaitId,
        reason: InboxTombstoneReason,
        source_revision: u64,
        tombstoned_at_ms: u64,
    ) -> HarnessFuture<'a, ()> {
        self.store
            .record_inbox_tombstone(wait_id, reason, source_revision, tombstoned_at_ms)
    }

    /// Whether the underlying store persists inbox-repair durability.
    pub fn supports_inbox_repair_durability(&self) -> bool {
        self.store.supports_inbox_repair_durability()
    }

    /// Enables failure-isolated automatic snapshots after terminal Turns.
    #[must_use]
    pub fn with_snapshot_maintenance(mut self, config: SnapshotMaintenanceConfig) -> Self {
        self.snapshot_maintenance = Some(Arc::new(SnapshotMaintenance::new(config)));
        self
    }

    /// Returns content-free maintenance state when automatic snapshots are enabled.
    #[must_use]
    pub fn snapshot_maintenance_stats(&self) -> Option<SnapshotMaintenanceStats> {
        self.snapshot_maintenance
            .as_ref()
            .map(|maintenance| maintenance.metrics.snapshot())
    }

    /// Waits for accepted snapshot workers, returning `false` on timeout.
    ///
    /// Call this during graceful host shutdown. A timeout does not affect the
    /// authoritative journal and detached work is cancelled when its Tokio
    /// runtime stops.
    pub async fn drain_snapshot_maintenance(&self, timeout: Duration) -> bool {
        let Some(maintenance) = &self.snapshot_maintenance else {
            return true;
        };
        if maintenance.metrics.active.load(Ordering::Acquire) == 0 {
            return true;
        }
        tokio::time::timeout(timeout, async {
            loop {
                let notified = maintenance.drained.notified();
                if maintenance.metrics.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await
        .is_ok()
    }

    /// Creates and persists a new Thread stream.
    pub async fn create_thread(&self) -> Result<Thread, HarnessError> {
        self.create_thread_as(&AuthorityContext::local_process())
            .await
    }

    /// Creates a Thread owned by the trusted authority's exact tenant boundary.
    pub async fn create_thread_as(
        &self,
        authority: &AuthorityContext,
    ) -> Result<Thread, HarnessError> {
        authority.validate_current("Thread creation authority")?;
        let thread = Thread::new_in_tenant(authority.tenant_id().map(str::to_owned));
        self.commit(
            thread.id.clone(),
            0,
            0,
            StateEvent::ThreadCreated {
                created_at_ms: thread.created_at_ms,
                tenant_id: authority.tenant_id().map(str::to_owned),
            },
        )
        .await?;
        Ok(thread)
    }

    /// Whether the configured Event Store can create derived streams atomically.
    #[must_use]
    pub fn supports_thread_fork(&self) -> bool {
        self.store.supports_atomic_stream_creation()
    }

    /// Whether the configured Event Store can import Thread archives atomically.
    #[must_use]
    pub fn supports_thread_import(&self) -> bool {
        self.store.supports_atomic_stream_creation()
    }

    /// Creates an independent Thread from one exact terminal parent boundary.
    ///
    /// `child_thread_id` is caller-supplied idempotency identity. Reusing it
    /// returns the existing matching child. `through_turn_id = None` means the
    /// complete parent as currently observed and therefore requires no running
    /// Turn; an explicit terminal Turn may be forked while newer work continues.
    pub async fn fork_thread(
        &self,
        parent_thread_id: &ThreadId,
        child_thread_id: ThreadId,
        through_turn_id: Option<&TurnId>,
    ) -> Result<Thread, HarnessError> {
        self.fork_thread_as(
            &AuthorityContext::local_process(),
            parent_thread_id,
            child_thread_id,
            through_turn_id,
        )
        .await
    }

    /// Forks a Thread inside the trusted authority's exact tenant boundary.
    pub async fn fork_thread_as(
        &self,
        authority: &AuthorityContext,
        parent_thread_id: &ThreadId,
        child_thread_id: ThreadId,
        through_turn_id: Option<&TurnId>,
    ) -> Result<Thread, HarnessError> {
        validate_state_id("parent thread", parent_thread_id.as_str())?;
        validate_state_id("child thread", child_thread_id.as_str())?;
        if parent_thread_id == &child_thread_id {
            return Err(HarnessError::State(
                "fork child identity must differ from its parent".to_owned(),
            ));
        }
        if !self.store.supports_atomic_stream_creation() {
            return Err(HarnessError::State(
                "Event Store does not support atomic Thread fork".to_owned(),
            ));
        }

        self.require_thread_access(parent_thread_id, authority)
            .await?;
        let checked = self.checked_events(parent_thread_id).await?;
        let parent = project_events(&checked.events)?.ok_or_else(|| {
            HarnessError::State(format!("thread {parent_thread_id} does not exist"))
        })?;
        validate_thread_authority(&parent, authority)?;
        let existing_child = self.load_thread_as(&child_thread_id, authority).await?;
        let boundary = match (&existing_child, through_turn_id) {
            (Some(existing), None) => {
                let lineage = existing.lineage.as_ref().ok_or_else(|| {
                    HarnessError::State(format!(
                        "thread {child_thread_id} already exists and is not a fork"
                    ))
                })?;
                if &lineage.parent_thread_id != parent_thread_id {
                    return Err(HarnessError::State(format!(
                        "thread {child_thread_id} was forked from another parent"
                    )));
                }
                usize::try_from(lineage.parent_stream_version).map_err(|_| {
                    HarnessError::State("existing fork boundary exceeds usize".to_owned())
                })?
            }
            _ => fork_boundary(&checked.events, &parent, through_turn_id)?,
        };
        if boundary == 0 || boundary > checked.events.len() {
            return Err(HarnessError::State(
                "fork lineage points outside the available parent journal".to_owned(),
            ));
        }
        let parent_prefix = &checked.events[..boundary];
        let inherited = project_events(parent_prefix)?.ok_or_else(|| {
            HarnessError::State("fork boundary has no parent projection".to_owned())
        })?;
        if inherited
            .turns
            .iter()
            .any(|turn| turn.status == TurnStatus::Running)
        {
            return Err(HarnessError::State(
                "Thread fork boundary must end at a terminal Turn".to_owned(),
            ));
        }
        let anchor = parent_prefix
            .last()
            .ok_or_else(|| HarnessError::State("cannot fork an empty journal".to_owned()))?;
        let lineage = ThreadLineage {
            parent_thread_id: parent_thread_id.clone(),
            parent_through_sequence: anchor.sequence,
            parent_stream_version: u64::try_from(parent_prefix.len())
                .map_err(|_| HarnessError::State("parent fork boundary exceeds u64".to_owned()))?,
            parent_events_sha256: state_events_sha256(parent_prefix)?,
        };

        if let Some(existing) = existing_child {
            validate_existing_fork(&existing, &lineage, &inherited.turns)?;
            return Ok(existing);
        }

        let recorded_at_ms = crate::kernel::now_ms();
        let mut new_events = Vec::with_capacity(parent_prefix.len().saturating_add(2));
        new_events.push(NewStreamEvent {
            event_id: EventId::generate(),
            schema_version: STATE_EVENT_SCHEMA_VERSION,
            recorded_at_ms,
            event: StateEvent::ThreadCreated {
                created_at_ms: recorded_at_ms,
                tenant_id: authority.tenant_id().map(str::to_owned),
            },
        });
        new_events.push(NewStreamEvent {
            event_id: EventId::generate(),
            schema_version: STATE_EVENT_SCHEMA_VERSION,
            recorded_at_ms,
            event: StateEvent::ThreadForked {
                lineage: lineage.clone(),
            },
        });
        new_events.extend(
            parent_prefix
                .iter()
                .filter_map(|stored| match &stored.event {
                    StateEvent::TurnStarted { .. }
                    | StateEvent::ItemAppended { .. }
                    | StateEvent::ToolCallsAppended { .. }
                    | StateEvent::WaitStarted { .. }
                    | StateEvent::AcceptResume { .. }
                    | StateEvent::ClaimReady { .. }
                    | StateEvent::WaitClosed { .. }
                    | StateEvent::DenyWait { .. }
                    | StateEvent::TurnFinished { .. }
                    | StateEvent::TurnCompleted { .. } => Some(NewStreamEvent {
                        event_id: EventId::generate(),
                        schema_version: stored.schema_version,
                        recorded_at_ms: stored.recorded_at_ms,
                        event: stored.event.clone(),
                    }),
                    StateEvent::ThreadCreated { .. }
                    | StateEvent::ThreadNamed { .. }
                    | StateEvent::ThreadForked { .. }
                    | StateEvent::ThreadImported { .. }
                    | StateEvent::CheckpointCreated { .. } => None,
                }),
        );
        let expected = new_events.clone();
        let stored = match self
            .store
            .create_stream_atomic(child_thread_id.clone(), new_events)
            .await
        {
            Ok(stored) => stored,
            Err(HarnessError::StateConflict {
                thread_id,
                expected: 0,
                ..
            }) if thread_id == child_thread_id => {
                let existing = self
                    .load_thread_as(&child_thread_id, authority)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::State(
                            "fork child appeared concurrently but cannot be loaded".to_owned(),
                        )
                    })?;
                validate_existing_fork(&existing, &lineage, &inherited.turns)?;
                return Ok(existing);
            }
            Err(error) => return Err(error),
        };
        let recovery_bytes = validate_atomic_stream_result(&child_thread_id, &expected, &stored)?;
        let child = project_events(&stored)?
            .ok_or_else(|| HarnessError::State("fork created no Thread projection".to_owned()))?;
        validate_existing_fork(&child, &lineage, &inherited.turns)?;
        let stream_version = u64::try_from(stored.len())
            .map_err(|_| HarnessError::State("fork stream version exceeds u64".to_owned()))?;
        let last_sequence = stored.last().map_or(0, |event| event.sequence);
        self.cache_head(
            child_thread_id,
            stream_head_from_parts(&child, stream_version, recovery_bytes, last_sequence),
        )
        .await;
        Ok(child)
    }

    /// Loads and validates the projected Thread, returning `None` when absent.
    pub async fn load_thread(&self, thread_id: &ThreadId) -> Result<Option<Thread>, HarnessError> {
        self.load_thread_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Loads a Thread only inside the trusted authority's tenant boundary.
    pub async fn load_thread_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Option<Thread>, HarnessError> {
        if !self.thread_accessible(thread_id, authority).await? {
            return Ok(None);
        }
        let loaded = self.load_projection(thread_id).await?;
        if let Some(thread) = &loaded.thread {
            validate_thread_authority(thread, authority)?;
            self.cache_head(
                thread_id.clone(),
                stream_head_from_parts(
                    thread,
                    loaded.stream_version,
                    loaded.recovery_bytes,
                    loaded.last_sequence,
                ),
            )
            .await;
        } else {
            self.heads.lock().await.remove(thread_id);
        }
        Ok(loaded.thread)
    }

    /// Changes or clears the durable operator-authored Thread name.
    pub async fn set_thread_name(
        &self,
        thread_id: &ThreadId,
        name: Option<String>,
    ) -> Result<StoredEvent, HarnessError> {
        self.set_thread_name_as(thread_id, name, &AuthorityContext::local_process())
            .await
    }

    /// Changes a Thread name inside the trusted authority's tenant boundary.
    pub async fn set_thread_name_as(
        &self,
        thread_id: &ThreadId,
        name: Option<String>,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        validate_thread_name(name.as_deref())?;
        self.require_thread_access(thread_id, authority).await?;
        let head = self.require_stream_head(thread_id).await?;
        self.commit(
            thread_id.clone(),
            head.stream_version,
            head.recovery_bytes,
            StateEvent::ThreadNamed { name },
        )
        .await
    }

    /// Returns bounded journal pressure for a Thread, or `None` when absent.
    pub async fn thread_capacity(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<StateCapacity>, HarnessError> {
        self.thread_capacity_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Returns capacity only inside the trusted authority's tenant boundary.
    pub async fn thread_capacity_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Option<StateCapacity>, HarnessError> {
        if !self.thread_accessible(thread_id, authority).await? {
            return Ok(None);
        }
        let loaded = self.load_projection(thread_id).await?;
        let Some(thread) = loaded.thread else {
            self.heads.lock().await.remove(thread_id);
            return Ok(None);
        };
        validate_thread_authority(&thread, authority)?;
        self.cache_head(
            thread_id.clone(),
            stream_head_from_parts(
                &thread,
                loaded.stream_version,
                loaded.recovery_bytes,
                loaded.last_sequence,
            ),
        )
        .await;
        Ok(Some(state_capacity(
            loaded.stream_version,
            loaded.recovery_bytes,
        )))
    }

    /// Whether the configured Event Store supports recent-Thread navigation.
    #[must_use]
    pub fn supports_thread_listing(&self) -> bool {
        self.store.supports_thread_listing()
    }

    /// Returns one bounded recent-Thread page without projecting full histories.
    pub async fn list_threads(
        &self,
        before_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ThreadSummaryPage, HarnessError> {
        self.list_threads_as(before_sequence, limit, &AuthorityContext::local_process())
            .await
    }

    /// Lists only Threads inside the trusted authority's tenant boundary.
    pub async fn list_threads_as(
        &self,
        before_sequence: Option<u64>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<ThreadSummaryPage, HarnessError> {
        authority.validate_current("Thread listing authority")?;
        if !(1..=MAX_THREAD_SUMMARY_PAGE).contains(&limit) {
            return Err(HarnessError::State(format!(
                "Thread page limit must be 1-{MAX_THREAD_SUMMARY_PAGE}"
            )));
        }
        if before_sequence == Some(0) {
            return Err(HarnessError::State(
                "Thread page cursor must be greater than zero".to_owned(),
            ));
        }
        let fetch_limit = limit
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("Thread page limit overflow".to_owned()))?;
        let mut threads = self
            .store
            .thread_summaries_page(
                authority.tenant_id().map(str::to_owned),
                before_sequence,
                fetch_limit,
            )
            .await?;
        if threads
            .iter()
            .any(|thread| thread.tenant_id.as_deref() != authority.tenant_id())
        {
            return Err(HarnessError::State(
                "Event Store returned a Thread outside the requested tenant boundary".to_owned(),
            ));
        }
        validate_thread_summaries(&threads, before_sequence, fetch_limit)?;
        let has_more = threads.len() > limit;
        if has_more {
            threads.truncate(limit);
        }
        let next_before_sequence = has_more
            .then(|| threads.last().map(|thread| thread.last_sequence))
            .flatten();
        Ok(ThreadSummaryPage {
            threads,
            next_before_sequence,
            has_more,
        })
    }

    /// Returns the authoritative ordered events for a Thread.
    pub async fn events(&self, thread_id: &ThreadId) -> Result<Vec<StoredEvent>, HarnessError> {
        self.events_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Returns events only inside the trusted authority's tenant boundary.
    pub async fn events_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Vec<StoredEvent>, HarnessError> {
        self.require_thread_access(thread_id, authority).await?;
        Ok(self.checked_events(thread_id).await?.events)
    }

    /// Exports one complete terminal Thread journal with an integrity digest.
    pub async fn export_thread(&self, thread_id: &ThreadId) -> Result<ThreadArchive, HarnessError> {
        self.export_thread_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Exports a Thread only inside the trusted authority's tenant boundary.
    pub async fn export_thread_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<ThreadArchive, HarnessError> {
        self.require_thread_access(thread_id, authority).await?;
        let checked = self.checked_events(thread_id).await?;
        let thread = project_events(&checked.events)?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        validate_thread_authority(&thread, authority)?;
        if thread
            .turns
            .iter()
            .any(|turn| turn.status == TurnStatus::Running)
        {
            return Err(HarnessError::State(
                "cannot export a Thread while a Turn is running".to_owned(),
            ));
        }
        let last = checked
            .events
            .last()
            .ok_or_else(|| HarnessError::State("cannot export an empty Thread".to_owned()))?;
        let archive = ThreadArchive {
            format_version: THREAD_ARCHIVE_FORMAT_VERSION,
            source_thread_id: thread.id,
            source_stream_version: u64::try_from(checked.events.len()).map_err(|_| {
                HarnessError::State("archive stream version exceeds u64".to_owned())
            })?,
            source_last_sequence: last.sequence,
            source_events_sha256: state_events_sha256(&checked.events)?,
            events: checked.events,
        };
        validate_thread_archive(&archive)?;
        Ok(archive)
    }

    /// Atomically materializes a portable archive as a new local Thread.
    ///
    /// `target_thread_id` is caller-supplied idempotency identity. A retry
    /// returns an existing Thread only when its import provenance matches.
    pub async fn import_thread(
        &self,
        archive: &ThreadArchive,
        target_thread_id: ThreadId,
    ) -> Result<Thread, HarnessError> {
        self.import_thread_as(
            archive,
            target_thread_id,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Imports an archive into the trusted authority's tenant boundary.
    ///
    /// Source ownership remains archive evidence only and never grants target
    /// access. The new local Thread is always rebound to `authority`.
    pub async fn import_thread_as(
        &self,
        archive: &ThreadArchive,
        target_thread_id: ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Thread, HarnessError> {
        authority.validate_current("Thread import authority")?;
        validate_state_id("target thread", target_thread_id.as_str())?;
        if !self.store.supports_atomic_stream_creation() {
            return Err(HarnessError::State(
                "Event Store does not support atomic Thread import".to_owned(),
            ));
        }
        let source = validate_thread_archive(archive)?;
        validate_import_authority_bindings(&source, authority)?;
        let origin = ThreadImportOrigin {
            source_thread_id: archive.source_thread_id.clone(),
            source_stream_version: archive.source_stream_version,
            source_last_sequence: archive.source_last_sequence,
            source_events_sha256: archive.source_events_sha256.clone(),
            source_lineage: source.lineage.clone(),
        };
        if let Some(existing) = self.load_thread_as(&target_thread_id, authority).await? {
            validate_existing_import(&existing, &origin, &source.turns)?;
            return Ok(existing);
        }

        let recorded_at_ms = crate::kernel::now_ms();
        let mut new_events = Vec::with_capacity(archive.events.len().saturating_add(2));
        new_events.push(NewStreamEvent {
            event_id: EventId::generate(),
            schema_version: STATE_EVENT_SCHEMA_VERSION,
            recorded_at_ms,
            event: StateEvent::ThreadCreated {
                created_at_ms: recorded_at_ms,
                tenant_id: authority.tenant_id().map(str::to_owned),
            },
        });
        new_events.push(NewStreamEvent {
            event_id: EventId::generate(),
            schema_version: STATE_EVENT_SCHEMA_VERSION,
            recorded_at_ms,
            event: StateEvent::ThreadImported {
                origin: origin.clone(),
            },
        });
        new_events.extend(
            archive
                .events
                .iter()
                .filter_map(|stored| match &stored.event {
                    StateEvent::ThreadNamed { .. }
                    | StateEvent::TurnStarted { .. }
                    | StateEvent::ItemAppended { .. }
                    | StateEvent::ToolCallsAppended { .. }
                    | StateEvent::WaitStarted { .. }
                    | StateEvent::AcceptResume { .. }
                    | StateEvent::ClaimReady { .. }
                    | StateEvent::WaitClosed { .. }
                    | StateEvent::DenyWait { .. }
                    | StateEvent::TurnFinished { .. }
                    | StateEvent::TurnCompleted { .. } => Some(NewStreamEvent {
                        event_id: EventId::generate(),
                        schema_version: stored.schema_version,
                        recorded_at_ms: stored.recorded_at_ms,
                        event: stored.event.clone(),
                    }),
                    StateEvent::ThreadCreated { .. }
                    | StateEvent::ThreadForked { .. }
                    | StateEvent::ThreadImported { .. }
                    | StateEvent::CheckpointCreated { .. } => None,
                }),
        );
        let expected = new_events.clone();
        let stored = match self
            .store
            .create_stream_atomic(target_thread_id.clone(), new_events)
            .await
        {
            Ok(stored) => stored,
            Err(HarnessError::StateConflict {
                thread_id,
                expected: 0,
                ..
            }) if thread_id == target_thread_id => {
                let existing = self
                    .load_thread_as(&target_thread_id, authority)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::State(
                            "import target appeared concurrently but cannot be loaded".to_owned(),
                        )
                    })?;
                validate_existing_import(&existing, &origin, &source.turns)?;
                return Ok(existing);
            }
            Err(error) => return Err(error),
        };
        let recovery_bytes = validate_atomic_stream_result(&target_thread_id, &expected, &stored)?;
        let imported = project_events(&stored)?
            .ok_or_else(|| HarnessError::State("import created no Thread projection".to_owned()))?;
        validate_existing_import(&imported, &origin, &source.turns)?;
        let stream_version = u64::try_from(stored.len())
            .map_err(|_| HarnessError::State("import stream version exceeds u64".to_owned()))?;
        let last_sequence = stored.last().map_or(0, |event| event.sequence);
        self.cache_head(
            target_thread_id,
            stream_head_from_parts(&imported, stream_version, recovery_bytes, last_sequence),
        )
        .await;
        Ok(imported)
    }

    /// Returns a bounded authoritative event page after one durable sequence.
    pub async fn events_page(
        &self,
        thread_id: &ThreadId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, HarnessError> {
        self.events_page_as(
            thread_id,
            after_sequence,
            limit,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Returns a bounded event page inside the exact tenant boundary.
    pub async fn events_page_as(
        &self,
        thread_id: &ThreadId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<Vec<StoredEvent>, HarnessError> {
        validate_state_id("thread", thread_id.as_str())?;
        validate_event_page_limit(limit)?;
        self.require_thread_access(thread_id, authority).await?;
        let events = self
            .store
            .events_page(
                thread_id,
                after_sequence,
                limit,
                MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
            )
            .await?;
        let _ = validate_stored_events(
            thread_id,
            &events,
            after_sequence,
            Some(limit),
            Some(MAX_STATE_EVENT_PAGE_RECOVERY_BYTES),
        )?;
        Ok(events)
    }

    /// Materializes and persists a validated snapshot of the current journal prefix.
    ///
    /// Snapshot creation is an explicit maintenance operation. It never deletes
    /// journal events and remains safe while another writer appends a newer tail.
    pub async fn create_snapshot(
        &self,
        thread_id: &ThreadId,
    ) -> Result<StateSnapshot, HarnessError> {
        self.create_snapshot_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Creates a snapshot inside the trusted authority's tenant boundary.
    pub async fn create_snapshot_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<StateSnapshot, HarnessError> {
        self.require_thread_access(thread_id, authority).await?;
        self.create_snapshot_unchecked(thread_id).await
    }

    async fn create_snapshot_unchecked(
        &self,
        thread_id: &ThreadId,
    ) -> Result<StateSnapshot, HarnessError> {
        let checked = self.checked_events(thread_id).await?;
        let events = checked.events;
        let thread = project_events(&events)?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        let metadata_events = events
            .iter()
            .filter_map(|stored| match &stored.event {
                StateEvent::ThreadNamed { .. } => Some(stored.event.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let anchor = events
            .last()
            .ok_or_else(|| HarnessError::State("cannot snapshot an empty stream".to_owned()))?;
        let snapshot = StateSnapshot {
            schema_version: STATE_SNAPSHOT_SCHEMA_VERSION,
            projection_sha256: projection_sha256(&thread, &metadata_events)?,
            thread,
            metadata_events,
            through_sequence: anchor.sequence,
            stream_version: u64::try_from(events.len())
                .map_err(|_| HarnessError::State("snapshot stream version overflow".to_owned()))?,
            recovery_bytes: checked.recovery_bytes,
            anchor_event_id: anchor.event_id.clone(),
            created_at_ms: crate::kernel::now_ms(),
        };
        validate_snapshot(&snapshot)?;
        self.store.save_snapshot(snapshot.clone()).await?;
        Ok(snapshot)
    }

    /// Starts a Turn when no other Turn is running in the Thread.
    pub async fn start_turn(&self, thread_id: &ThreadId) -> Result<Turn, HarnessError> {
        self.start_turn_as(thread_id, &AuthorityContext::local_process())
            .await
    }

    /// Starts a Turn inside the trusted authority's tenant boundary.
    pub async fn start_turn_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Turn, HarnessError> {
        self.require_thread_access(thread_id, authority).await?;
        let mut head = self.require_stream_head(thread_id).await?;
        if head.running_turn.is_some() {
            head = self.refresh_stream_head(thread_id).await?;
            if head.running_turn.is_some() {
                return Err(HarnessError::State(format!(
                    "thread {thread_id} already has a running turn"
                )));
            }
        }

        let turn = Turn::new(thread_id.clone());
        self.commit(
            thread_id.clone(),
            head.stream_version,
            head.recovery_bytes,
            StateEvent::TurnStarted {
                turn_id: turn.id.clone(),
            },
        )
        .await?;
        Ok(turn)
    }

    /// Appends an Item after validating that its Turn is still running.
    pub async fn append_item(&self, turn: &Turn, item: Item) -> Result<StoredEvent, HarnessError> {
        self.append_item_as(turn, item, &AuthorityContext::local_process())
            .await
    }

    /// Appends an Item inside the trusted authority's tenant boundary.
    pub async fn append_item_as(
        &self,
        turn: &Turn,
        item: Item,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        self.require_thread_access(&turn.thread_id, authority)
            .await?;
        if matches!(
            &item.kind,
            ItemKind::ExecutionBinding { .. }
                | ItemKind::SteeringQueued { .. }
                | ItemKind::SteeringApplied { .. }
        ) || matches!(
            &item.kind,
            ItemKind::ToolResult {
                connector_evidence,
                ..
            } if !connector_evidence.is_empty()
        ) {
            let thread = self
                .load_thread_as(&turn.thread_id, authority)
                .await?
                .ok_or_else(|| {
                    HarnessError::State(format!("thread {} does not exist", turn.thread_id))
                })?;
            match &item.kind {
                ItemKind::ExecutionBinding { bound_by, binding } => {
                    validate_execution_binding_append(
                        &thread, &turn.id, bound_by, binding, authority,
                    )?;
                }
                ItemKind::SteeringQueued { .. } | ItemKind::SteeringApplied { .. } => {
                    validate_steering_append(&thread, &turn.id, &item)?;
                }
                ItemKind::ToolResult {
                    connector_evidence, ..
                } if !connector_evidence.is_empty() => {
                    validate_connector_evidence_append(&thread, &turn.id, &item, authority)?;
                }
                _ => {}
            }
        }
        let mut head = self.require_stream_head(&turn.thread_id).await?;
        if require_running_head(&head, &turn.id).is_err() {
            head = self.refresh_stream_head(&turn.thread_id).await?;
            require_running_head(&head, &turn.id)?;
        }
        self.commit(
            turn.thread_id.clone(),
            head.stream_version,
            head.recovery_bytes,
            StateEvent::ItemAppended {
                turn_id: turn.id.clone(),
                item,
            },
        )
        .await
    }

    /// Atomically appends one same-response Tool-call batch.
    pub(crate) async fn append_tool_calls_as(
        &self,
        turn: &Turn,
        calls: Vec<Item>,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        self.require_thread_access(&turn.thread_id, authority)
            .await?;
        let mut head = self.require_stream_head(&turn.thread_id).await?;
        if require_running_head(&head, &turn.id).is_err() {
            head = self.refresh_stream_head(&turn.thread_id).await?;
            require_running_head(&head, &turn.id)?;
        }
        self.commit(
            turn.thread_id.clone(),
            head.stream_version,
            head.recovery_bytes,
            StateEvent::ToolCallsAppended {
                turn_id: turn.id.clone(),
                calls,
            },
        )
        .await
    }

    /// Atomically records an Approval request and enters a durable wait.
    pub async fn start_approval_wait(
        &self,
        turn: &Turn,
        wait_id: AgentLoopWaitId,
        request: crate::ApprovalRequest,
        completion_generation: CompletionGeneration,
        wait_ttl: Option<Duration>,
        remaining_active_timeout_ms: Option<u64>,
    ) -> Result<StoredEvent, HarnessError> {
        self.start_approval_wait_as(
            turn,
            AgentLoopWaitStartCommand::new(
                wait_id,
                request,
                completion_generation,
                wait_ttl,
                remaining_active_timeout_ms,
            ),
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Atomically enters a durable Approval wait under trusted Turn authority.
    ///
    /// The command's `wait_id` is the caller-generated idempotency identity.
    /// An exact command retry returns the original event; the server supplies
    /// start and expiry time after validating the complete command.
    pub async fn start_approval_wait_as(
        &self,
        turn: &Turn,
        command: AgentLoopWaitStartCommand,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        let AgentLoopWaitStartCommand {
            wait_id,
            request,
            completion_generation,
            wait_ttl,
            remaining_active_timeout_ms,
        } = command;
        authority.validate_current("Agent Loop wait authority")?;
        validate_state_id("Agent Loop wait", wait_id.as_str())?;
        let wait_ttl_ms = bounded_wait_ttl_ms(wait_ttl)?;
        validate_remaining_active_timeout(remaining_active_timeout_ms)?;
        completion_generation
            .validate()
            .map_err(state_completion_error)?;

        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = projected_turn(thread, turn)?;
        require_running_projection(projected)?;
        if let Some(execution) = agent_loop_execution_projection(projected)? {
            if execution.wait_id() == &wait_id
                && wait_start_matches(
                    execution.envelope(),
                    &request,
                    &completion_generation,
                    wait_ttl_ms,
                    remaining_active_timeout_ms,
                    authority,
                )
            {
                return self
                    .matching_wait_event(&turn.thread_id, &wait_id)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::State(format!(
                            "wait {wait_id} is projected without its atomic event"
                        ))
                    });
            }
            require_new_wait_boundary(projected, &execution)?;
        }
        validate_approval_wait_request(
            thread,
            projected,
            &request,
            completion_generation.model_request_sha256(),
            authority,
        )?;

        let server_started_at_ms = crate::kernel::now_ms();
        let expires_at_ms = wait_ttl_ms
            .map(|ttl| {
                server_started_at_ms.checked_add(ttl).ok_or_else(|| {
                    HarnessError::State("Agent Loop wait expiry overflow".to_owned())
                })
            })
            .transpose()?;
        let model_request_sha256 = completion_generation.model_request_sha256().to_owned();
        let mut envelope = TurnWaitEnvelope {
            wait_id: wait_id.clone(),
            revision: 1,
            thread_id: turn.thread_id.clone(),
            turn_id: turn.id.clone(),
            tenant_id: authority.tenant_id().map(str::to_owned),
            requested_by: authority.actor().clone(),
            server_started_at_ms,
            expires_at_ms,
            remaining_active_timeout_ms,
            completion_generation,
            wait_kind: WaitKind::Approval {
                request: request.clone(),
                model_request_sha256,
            },
            envelope_sha256: String::new(),
        };
        envelope.envelope_sha256 = wait_envelope_sha256(&envelope)?;
        validate_wait_envelope(&envelope)?;

        let approval_requested = Item {
            id: ItemId::generate(),
            created_at_ms: server_started_at_ms,
            kind: ItemKind::ApprovalRequested {
                approval_id: request.id.clone(),
                call_id: request.authorization.call_id.clone(),
                tool: request.authorization.descriptor.name.clone(),
                reason: request.reason.clone(),
                risk: request.risk,
                requested_by: Some(request.requested_by.clone()),
                tool_origin: Some(request.authorization.origin.clone()),
                model_request_sha256: Some(
                    envelope
                        .completion_generation
                        .model_request_sha256()
                        .to_owned(),
                ),
            },
        };
        let transition = Item {
            id: ItemId::generate(),
            created_at_ms: server_started_at_ms,
            kind: ItemKind::AgentLoopWaitStarted {
                envelope: Box::new(envelope.clone()),
            },
        };
        let pending = PendingEvent {
            event_id: agent_loop_lifecycle_event_id(
                AgentLoopLifecycleEvent::WaitStarted,
                &turn.thread_id,
                &turn.id,
                wait_id.as_str(),
            )?,
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: server_started_at_ms,
            event: StateEvent::WaitStarted {
                turn_id: turn.id.clone(),
                approval_requested,
                transition,
            },
        };
        match self.commit_pending(pending).await {
            Ok(stored) => Ok(stored),
            Err(error) => {
                if let Some(stored) = self.matching_wait_event(&turn.thread_id, &wait_id).await? {
                    let thread = self
                        .load_thread_as(&turn.thread_id, authority)
                        .await?
                        .ok_or_else(|| HarnessError::State("wait Thread disappeared".to_owned()))?;
                    let projected = projected_turn(&thread, turn)?;
                    let execution =
                        agent_loop_execution_projection(projected)?.ok_or_else(|| {
                            HarnessError::State("wait event has no execution projection".to_owned())
                        })?;
                    if wait_start_matches(
                        execution.envelope(),
                        &request,
                        &envelope.completion_generation,
                        wait_ttl_ms,
                        remaining_active_timeout_ms,
                        authority,
                    ) {
                        return Ok(stored);
                    }
                }
                Err(error)
            }
        }
    }

    /// Atomically appends an Approval decision and makes a wait ready.
    pub async fn accept_resume(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopResumeCommandId,
        settlement: &ApprovalRecord,
    ) -> Result<StoredEvent, HarnessError> {
        self.accept_resume_as(
            turn,
            wait_id,
            expected_revision,
            command_id,
            settlement,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Accepts one exact Approval Inbox settlement under trusted Turn authority.
    pub async fn accept_resume_as(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopResumeCommandId,
        settlement: &ApprovalRecord,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        authority.validate_current("Agent Loop resume authority")?;
        validate_state_id("Agent Loop wait", wait_id.as_str())?;
        validate_state_id("Agent Loop resume command", command_id.as_str())?;
        let settlement = approval_settlement_evidence(settlement)?;
        let command_sha256 = resume_command_sha256(
            &turn.thread_id,
            &turn.id,
            wait_id,
            expected_revision,
            &command_id,
            &settlement,
            authority,
        )?;
        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = projected_turn(thread, turn)?;
        require_running_projection(projected)?;
        let execution = agent_loop_execution_projection(projected)?
            .ok_or_else(|| HarnessError::State(format!("turn {} has no durable wait", turn.id)))?;
        if resume_retry_matches(&execution, &command_id, &command_sha256) {
            return self
                .matching_resume_event(&turn.thread_id, &command_id, &command_sha256)
                .await?
                .ok_or_else(|| {
                    HarnessError::State("resume projection has no atomic event".to_owned())
                });
        }
        let AgentLoopExecution::Waiting { envelope } = execution else {
            return Err(HarnessError::State(format!(
                "wait {wait_id} is not waiting at revision {expected_revision}"
            )));
        };
        validate_wait_resume_authority(&envelope, wait_id, expected_revision, authority)?;
        validate_wait_not_expired(&envelope, crate::kernel::now_ms())?;
        validate_approval_settlement(&envelope, &settlement)?;
        let accepted_at_ms = crate::kernel::now_ms();
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("Agent Loop revision overflow".to_owned()))?;
        let evidence = ResumeEvidence {
            wait_id: wait_id.clone(),
            previous_revision: expected_revision,
            revision,
            command_id: command_id.clone(),
            command_sha256: command_sha256.clone(),
            settlement: settlement.clone(),
            accepted_at_ms,
        };
        validate_resume_evidence(&envelope, &evidence)?;
        let approval_decision = Item {
            id: ItemId::generate(),
            created_at_ms: accepted_at_ms,
            kind: ItemKind::ApprovalDecision {
                approval_id: settlement.request.id.clone(),
                call_id: settlement.request.authorization.call_id.clone(),
                decision: settlement.decision.clone(),
            },
        };
        let transition = Item {
            id: ItemId::generate(),
            created_at_ms: accepted_at_ms,
            kind: ItemKind::AgentLoopResumeAccepted {
                evidence: Box::new(evidence),
            },
        };
        let pending = PendingEvent {
            event_id: agent_loop_lifecycle_event_id(
                AgentLoopLifecycleEvent::ResumeAccepted,
                &turn.thread_id,
                &turn.id,
                command_id.as_str(),
            )?,
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: accepted_at_ms,
            event: StateEvent::AcceptResume {
                turn_id: turn.id.clone(),
                approval_decision,
                transition,
            },
        };
        match self.commit_pending(pending).await {
            Ok(stored) => Ok(stored),
            Err(error) => self
                .matching_resume_event(&turn.thread_id, &command_id, &command_sha256)
                .await?
                .ok_or(error),
        }
    }

    /// Atomically consumes an Approval denial and settles its Turn as failed.
    pub async fn deny_wait(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopDenyCommandId,
        settlement: &ApprovalRecord,
    ) -> Result<StoredEvent, HarnessError> {
        self.deny_wait_as(
            turn,
            wait_id,
            expected_revision,
            command_id,
            settlement,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Atomically fails a Waiting or denial-ready Turn under trusted authority.
    ///
    /// State writes the ordinary denial and model-hidden closure evidence in
    /// one terminal CAS. A Ready execution may converge only when its accepted
    /// settlement is byte-for-byte equivalent to this denial. No worker claim
    /// is created, so a denial can never cross the Tool-effect boundary.
    pub async fn deny_wait_as(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopDenyCommandId,
        settlement: &ApprovalRecord,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        authority.validate_current("Agent Loop denial authority")?;
        validate_state_id("Agent Loop wait", wait_id.as_str())?;
        validate_state_id("Agent Loop denial command", command_id.as_str())?;
        let settlement = approval_denial_settlement_evidence(settlement)?;
        let command_sha256 = wait_denial_command_sha256(
            &turn.thread_id,
            &turn.id,
            wait_id,
            expected_revision,
            &command_id,
            &settlement,
            authority,
        )?;

        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = projected_turn(thread, turn)?;
        let envelope = wait_envelope_for_id(projected, wait_id)?.ok_or_else(|| {
            HarnessError::State(format!("turn {} has no wait {wait_id}", turn.id))
        })?;

        // Exact terminal retry must be recognized before the running fence.
        if wait_denial_retry_matches(projected, &command_id, &command_sha256, &settlement)? {
            let stored = self
                .matching_wait_denial_event(&turn.thread_id, &command_id, &command_sha256)
                .await?
                .ok_or_else(|| {
                    HarnessError::State("wait denial projection has no atomic event".to_owned())
                })?;
            self.schedule_snapshot(turn.thread_id.clone(), loaded.stream_version)
                .await;
            return Ok(stored);
        }

        require_running_projection(projected)?;
        let execution = agent_loop_execution_projection(projected)?.ok_or_else(|| {
            HarnessError::State(format!("turn {} has no live durable wait", turn.id))
        })?;
        validate_wait_current_authority(
            execution.envelope(),
            wait_id,
            expected_revision,
            execution.revision(),
            authority,
        )?;
        let preceding_transition_ms = match &execution {
            AgentLoopExecution::Waiting { envelope } => {
                validate_wait_not_expired(envelope, crate::kernel::now_ms())?;
                envelope.server_started_at_ms
            }
            AgentLoopExecution::Ready { resume, .. } => {
                if resume.settlement != settlement {
                    return Err(HarnessError::State(
                        "Ready execution differs from the exact Approval denial".to_owned(),
                    ));
                }
                resume.accepted_at_ms
            }
            AgentLoopExecution::Executing { .. } => {
                return Err(HarnessError::State(
                    "an Executing Agent Loop wait cannot be denied before its effect settles"
                        .to_owned(),
                ));
            }
        };
        validate_approval_settlement(&envelope, &settlement)?;

        let denied_at_ms = crate::kernel::now_ms();
        if denied_at_ms < preceding_transition_ms {
            return Err(HarnessError::State(
                "Agent Loop denial precedes the current wait transition".to_owned(),
            ));
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("Agent Loop revision overflow".to_owned()))?;
        let evidence = WaitDenialEvidence {
            wait_id: wait_id.clone(),
            previous_revision: expected_revision,
            revision,
            command_id: command_id.clone(),
            command_sha256: command_sha256.clone(),
            settlement: settlement.clone(),
            denied_at_ms,
        };
        validate_wait_denial_evidence(&envelope, &evidence)?;
        let approval_decision = Item {
            id: ItemId::generate(),
            created_at_ms: denied_at_ms,
            kind: ItemKind::ApprovalDecision {
                approval_id: settlement.request.id.clone(),
                call_id: settlement.request.authorization.call_id.clone(),
                decision: settlement.decision.clone(),
            },
        };
        let transition = Item {
            id: ItemId::generate(),
            created_at_ms: denied_at_ms,
            kind: ItemKind::AgentLoopWaitDenied {
                evidence: Box::new(evidence),
            },
        };
        let next_stream_version = loaded.stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        let pending = PendingEvent {
            event_id: agent_loop_lifecycle_event_id(
                AgentLoopLifecycleEvent::WaitDenied,
                &turn.thread_id,
                &turn.id,
                command_id.as_str(),
            )?,
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: denied_at_ms,
            event: StateEvent::DenyWait {
                turn_id: turn.id.clone(),
                approval_decision,
                transition,
            },
        };
        match self.commit_pending(pending).await {
            Ok(stored) => {
                self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
                    .await;
                Ok(stored)
            }
            Err(error) => {
                if let Some(stored) = self
                    .matching_wait_denial_event(&turn.thread_id, &command_id, &command_sha256)
                    .await?
                {
                    self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
                        .await;
                    Ok(stored)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Atomically closes one Waiting or Ready execution under local authority.
    pub async fn close_wait(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        command_id: AgentLoopCloseCommandId,
        status: TurnStatus,
        reason: TurnStopReason,
    ) -> Result<StoredEvent, HarnessError> {
        self.close_wait_as(
            turn,
            AgentLoopWaitCloseCommand::new(
                wait_id.clone(),
                expected_revision,
                command_id,
                status,
                reason,
            ),
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Atomically appends stop and closure evidence and settles a durable wait.
    ///
    /// Cancellation may close immediately. Timeout closure requires the
    /// server-issued expiry to have elapsed. The exact command is idempotent;
    /// stale revisions and command identity reuse with changed content fail.
    pub async fn close_wait_as(
        &self,
        turn: &Turn,
        command: AgentLoopWaitCloseCommand,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        let wait_id = &command.wait_id;
        let expected_revision = command.expected_revision;
        let command_id = &command.command_id;
        let status = &command.status;
        let reason = command.reason;
        authority.validate_current("Agent Loop wait close authority")?;
        validate_state_id("Agent Loop wait", wait_id.as_str())?;
        validate_state_id("Agent Loop close command", command_id.as_str())?;
        validate_wait_close_status(status, reason)?;

        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = projected_turn(thread, turn)?;
        let envelope = wait_envelope_for_id(projected, wait_id)?.ok_or_else(|| {
            HarnessError::State(format!("turn {} has no wait {wait_id}", turn.id))
        })?;
        let command_sha256 =
            wait_close_command_sha256(&turn.thread_id, &turn.id, &command, authority)?;
        if wait_close_retry_matches(projected, command_id, &command_sha256, status, reason)? {
            let stored = self
                .matching_wait_close_event(&turn.thread_id, command_id, &command_sha256)
                .await?
                .ok_or_else(|| {
                    HarnessError::State("wait closure projection has no atomic event".to_owned())
                })?;
            self.schedule_snapshot(turn.thread_id.clone(), loaded.stream_version)
                .await;
            return Ok(stored);
        }
        require_running_projection(projected)?;
        let execution = agent_loop_execution_projection(projected)?.ok_or_else(|| {
            HarnessError::State(format!("turn {} has no live durable wait", turn.id))
        })?;
        if matches!(execution, AgentLoopExecution::Executing { .. }) {
            return Err(HarnessError::State(
                "an Executing Agent Loop wait cannot be closed as unclaimed".to_owned(),
            ));
        }
        validate_wait_current_authority(
            execution.envelope(),
            wait_id,
            expected_revision,
            execution.revision(),
            authority,
        )?;
        if matches!(
            &execution,
            AgentLoopExecution::Ready { resume, .. }
                if matches!(&resume.settlement.decision, crate::ApprovalDecision::Deny { .. })
        ) {
            return Err(HarnessError::State(
                "an Agent Loop Ready state with an accepted Deny settlement must use atomic denial settlement"
                    .to_owned(),
            ));
        }
        let closed_at_ms = crate::kernel::now_ms();
        if *status == TurnStatus::TimedOut
            && execution
                .envelope()
                .expires_at_ms
                .is_none_or(|expires_at_ms| closed_at_ms < expires_at_ms)
        {
            return Err(HarnessError::State(
                "TimedOut wait closure requires an elapsed server expiry".to_owned(),
            ));
        }
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("Agent Loop revision overflow".to_owned()))?;
        let evidence = WaitClosureEvidence {
            wait_id: wait_id.clone(),
            previous_revision: expected_revision,
            revision,
            command_id: command_id.clone(),
            status: status.clone(),
            reason,
            command_sha256: command_sha256.clone(),
            closed_at_ms,
        };
        validate_wait_closure_evidence(&envelope, &evidence)?;
        let stopped = Item {
            id: ItemId::generate(),
            created_at_ms: closed_at_ms,
            kind: ItemKind::TurnStopped {
                reason,
                phase: ExecutionPhase::Approval,
            },
        };
        let transition = Item {
            id: ItemId::generate(),
            created_at_ms: closed_at_ms,
            kind: ItemKind::AgentLoopWaitClosed {
                evidence: Box::new(evidence),
            },
        };
        let next_stream_version = loaded.stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        let pending = PendingEvent {
            event_id: agent_loop_lifecycle_event_id(
                AgentLoopLifecycleEvent::WaitClosed,
                &turn.thread_id,
                &turn.id,
                command_id.as_str(),
            )?,
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: closed_at_ms,
            event: StateEvent::WaitClosed {
                turn_id: turn.id.clone(),
                stopped,
                transition,
                status: status.clone(),
            },
        };
        match self.commit_pending(pending).await {
            Ok(stored) => {
                self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
                    .await;
                Ok(stored)
            }
            Err(error) => {
                if let Some(stored) = self
                    .matching_wait_close_event(&turn.thread_id, command_id, &command_sha256)
                    .await?
                {
                    self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
                        .await;
                    Ok(stored)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Atomically claims a ready execution for one explicit worker coordinate.
    pub async fn claim_ready(
        &self,
        turn: &Turn,
        wait_id: &AgentLoopWaitId,
        expected_revision: u64,
        resume_command_id: &AgentLoopResumeCommandId,
        claim_id: AgentLoopClaimId,
        worker_id: AgentLoopWorkerId,
    ) -> Result<StoredEvent, HarnessError> {
        self.claim_ready_as(
            turn,
            AgentLoopReadyClaimCommand::new(
                wait_id.clone(),
                expected_revision,
                resume_command_id.clone(),
                claim_id,
                worker_id,
            ),
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Claims a ready execution under trusted Turn authority; one worker wins CAS.
    ///
    /// `worker_id` is an execution-worker coordinate, not a user identity and
    /// not a substitute for `authority` or its tenant access decision.
    pub async fn claim_ready_as(
        &self,
        turn: &Turn,
        command: AgentLoopReadyClaimCommand,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        let wait_id = &command.wait_id;
        let expected_revision = command.expected_revision;
        let resume_command_id = &command.resume_command_id;
        let claim_id = &command.claim_id;
        let worker_id = &command.worker_id;
        authority.validate_current("Agent Loop claim authority")?;
        validate_state_id("Agent Loop wait", wait_id.as_str())?;
        validate_state_id("Agent Loop resume command", resume_command_id.as_str())?;
        validate_state_id("Agent Loop claim", claim_id.as_str())?;
        validate_state_id("Agent Loop worker", worker_id.as_str())?;
        let claim_sha256 = execution_claim_sha256(&turn.thread_id, &turn.id, &command, authority)?;
        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = projected_turn(thread, turn)?;
        require_running_projection(projected)?;
        let execution = agent_loop_execution_projection(projected)?
            .ok_or_else(|| HarnessError::State(format!("turn {} has no durable wait", turn.id)))?;
        if claim_retry_matches(&execution, claim_id, &claim_sha256) {
            return self
                .matching_claim_event(&turn.thread_id, claim_id, &claim_sha256)
                .await?
                .ok_or_else(|| {
                    HarnessError::State("claim projection has no atomic event".to_owned())
                });
        }
        let AgentLoopExecution::Ready { envelope, resume } = execution else {
            return Err(HarnessError::State(format!(
                "wait {wait_id} is not ready at revision {expected_revision}"
            )));
        };
        validate_wait_resume_authority(&envelope, wait_id, envelope.revision, authority)?;
        if expected_revision != resume.revision || resume_command_id != &resume.command_id {
            return Err(HarnessError::State(
                "Agent Loop claim does not match the ready revision or resume command".to_owned(),
            ));
        }
        if matches!(
            &resume.settlement.decision,
            crate::ApprovalDecision::Deny { .. }
        ) {
            return Err(HarnessError::State(
                "an Agent Loop Ready state with an accepted Deny settlement cannot be claimed"
                    .to_owned(),
            ));
        }
        validate_wait_not_expired(&envelope, crate::kernel::now_ms())?;
        let claimed_at_ms = crate::kernel::now_ms();
        let revision = expected_revision
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("Agent Loop revision overflow".to_owned()))?;
        let evidence = ExecutionClaimEvidence {
            wait_id: wait_id.clone(),
            previous_revision: expected_revision,
            revision,
            resume_command_id: resume_command_id.clone(),
            claim_id: claim_id.clone(),
            worker_id: worker_id.clone(),
            claim_sha256: claim_sha256.clone(),
            claimed_at_ms,
        };
        validate_claim_evidence(&envelope, &resume, &evidence)?;
        let transition = Item {
            id: ItemId::generate(),
            created_at_ms: claimed_at_ms,
            kind: ItemKind::AgentLoopReadyClaimed {
                evidence: Box::new(evidence),
            },
        };
        let pending = PendingEvent {
            event_id: agent_loop_lifecycle_event_id(
                AgentLoopLifecycleEvent::ReadyClaimed,
                &turn.thread_id,
                &turn.id,
                claim_id.as_str(),
            )?,
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: claimed_at_ms,
            event: StateEvent::ClaimReady {
                turn_id: turn.id.clone(),
                transition,
            },
        };
        match self.commit_pending(pending).await {
            Ok(stored) => Ok(stored),
            Err(error) => self
                .matching_claim_event(&turn.thread_id, claim_id, &claim_sha256)
                .await?
                .ok_or(error),
        }
    }

    /// Loads the latest live wait projection for one Turn.
    pub async fn agent_loop_execution(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
    ) -> Result<Option<AgentLoopExecution>, HarnessError> {
        self.agent_loop_execution_as(thread_id, turn_id, &AuthorityContext::local_process())
            .await
    }

    /// Loads a live wait projection inside the trusted tenant boundary.
    ///
    /// Terminal Turns return `None`: their terminal event atomically closes
    /// the execution while transition Items remain immutable audit evidence.
    pub async fn agent_loop_execution_as(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
        authority: &AuthorityContext,
    ) -> Result<Option<AgentLoopExecution>, HarnessError> {
        let thread = self
            .load_thread_as(thread_id, authority)
            .await?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        let turn = thread
            .turns
            .iter()
            .find(|turn| &turn.id == turn_id)
            .ok_or_else(|| HarnessError::State(format!("turn {turn_id} does not exist")))?;
        let execution = agent_loop_execution_projection(turn)?;
        if turn.status == TurnStatus::Running {
            Ok(execution)
        } else {
            Ok(None)
        }
    }

    /// Discovers one bounded page of due Agent Loop waits for local authority.
    pub async fn scan_due_agent_loop_waits(
        &self,
        at_ms: u64,
        after: Option<&AgentLoopDueCursor>,
        scan_limit: usize,
    ) -> Result<AgentLoopDueScanPage, HarnessError> {
        self.scan_due_agent_loop_waits_as(
            at_ms,
            after,
            scan_limit,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Discovers due waits from the exact tenant-scoped materialized index.
    ///
    /// Discovery returns fixed-size fences only. It never scans Thread lists,
    /// loads snapshots, or replays an aggregate journal.
    pub async fn scan_due_agent_loop_waits_as(
        &self,
        at_ms: u64,
        after: Option<&AgentLoopDueCursor>,
        scan_limit: usize,
        authority: &AuthorityContext,
    ) -> Result<AgentLoopDueScanPage, HarnessError> {
        authority.validate_current("Agent Loop wait maintenance authority")?;
        if !self.store.supports_agent_loop_wait_projection() {
            return Err(HarnessError::State(
                "Event Store does not support bounded Agent Loop wait maintenance".to_owned(),
            ));
        }
        validate_due_scan_request(at_ms, after, scan_limit, authority.tenant_id())?;
        let page = self
            .store
            .scan_due_agent_loop_waits(
                authority.tenant_id().map(str::to_owned),
                at_ms,
                after.cloned(),
                scan_limit,
            )
            .await?;
        page.validate(at_ms, after, scan_limit, authority.tenant_id())?;
        Ok(page)
    }

    /// Settles one exact due fence under local maintenance authority.
    pub async fn settle_due_agent_loop_wait(
        &self,
        due: &AgentLoopDueWait,
        at_ms: u64,
    ) -> Result<EventAppendResult, HarnessError> {
        self.settle_due_agent_loop_wait_as(due, at_ms, &AuthorityContext::local_process())
            .await
    }

    /// Atomically settles one due wait without recovering its complete Thread.
    ///
    /// The maintenance caller authorizes only the tenant-scoped scan. Command
    /// evidence remains bound to the original requester frozen in the wait
    /// envelope. A concurrent resume, claim, cancellation, or denial wins the
    /// stream CAS and fences this attempt.
    pub async fn settle_due_agent_loop_wait_as(
        &self,
        due: &AgentLoopDueWait,
        at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EventAppendResult, HarnessError> {
        authority.validate_current("Agent Loop wait maintenance authority")?;
        if !self.store.supports_agent_loop_wait_projection() {
            return Err(HarnessError::State(
                "Event Store does not support bounded Agent Loop wait maintenance".to_owned(),
            ));
        }
        due.validate(at_ms, authority.tenant_id())?;
        let exact = self.load_exact_due_wait_evidence(due).await?;
        let frozen_authority = AuthorityContext::new(
            exact.envelope.requested_by.clone(),
            exact.envelope.tenant_id.clone(),
        )
        .map_err(|error| HarnessError::State(error.to_string()))?;

        let pending = match due.phase {
            AgentLoopDuePhase::Waiting | AgentLoopDuePhase::ReadyAllow => {
                let expires_at_ms = exact.envelope.expires_at_ms.ok_or_else(|| {
                    HarnessError::State(
                        "Agent Loop timeout projection has no envelope expiry".to_owned(),
                    )
                })?;
                if expires_at_ms != due.due_at_ms {
                    return Err(HarnessError::State(
                        "Agent Loop timeout projection differs from its envelope expiry".to_owned(),
                    ));
                }
                let preceding_transition_ms = exact
                    .resume
                    .as_ref()
                    .map_or(exact.envelope.server_started_at_ms, |resume| {
                        resume.accepted_at_ms
                    });
                let closed_at_ms = due.due_at_ms.max(preceding_transition_ms);
                if closed_at_ms > at_ms {
                    return Err(HarnessError::State(
                        "Agent Loop wait transition is later than the trusted maintenance time"
                            .to_owned(),
                    ));
                }
                let command_id = deterministic_timeout_command_id(due)?;
                let command = AgentLoopWaitCloseCommand::new(
                    due.wait_id.clone(),
                    due.revision,
                    command_id.clone(),
                    TurnStatus::TimedOut,
                    TurnStopReason::TimedOut,
                );
                let command_sha256 = wait_close_command_sha256(
                    &due.thread_id,
                    &due.turn_id,
                    &command,
                    &frozen_authority,
                )?;
                let revision = due.revision.checked_add(1).ok_or_else(|| {
                    HarnessError::State("Agent Loop revision overflow".to_owned())
                })?;
                let evidence = WaitClosureEvidence {
                    wait_id: due.wait_id.clone(),
                    previous_revision: due.revision,
                    revision,
                    command_id: command_id.clone(),
                    status: TurnStatus::TimedOut,
                    reason: TurnStopReason::TimedOut,
                    command_sha256,
                    closed_at_ms,
                };
                validate_wait_closure_evidence(&exact.envelope, &evidence)?;
                let event_id = agent_loop_lifecycle_event_id(
                    AgentLoopLifecycleEvent::WaitClosed,
                    &due.thread_id,
                    &due.turn_id,
                    command_id.as_str(),
                )?;
                let stopped = Item {
                    id: agent_loop_maintenance_item_id(&event_id, "turn-stopped")?,
                    created_at_ms: closed_at_ms,
                    kind: ItemKind::TurnStopped {
                        reason: TurnStopReason::TimedOut,
                        phase: ExecutionPhase::Approval,
                    },
                };
                let transition = Item {
                    id: agent_loop_maintenance_item_id(&event_id, "wait-closed")?,
                    created_at_ms: closed_at_ms,
                    kind: ItemKind::AgentLoopWaitClosed {
                        evidence: Box::new(evidence),
                    },
                };
                PendingEvent {
                    event_id,
                    thread_id: due.thread_id.clone(),
                    expected_stream_version: due.expected_stream_version,
                    expected_stream_recovery_bytes: due.expected_stream_recovery_bytes,
                    recorded_at_ms: closed_at_ms,
                    event: StateEvent::WaitClosed {
                        turn_id: due.turn_id.clone(),
                        stopped,
                        transition,
                        status: TurnStatus::TimedOut,
                    },
                }
            }
            AgentLoopDuePhase::ReadyDeny => {
                let resume = exact.resume.as_ref().ok_or_else(|| {
                    HarnessError::State(
                        "Agent Loop denial projection has no accepted settlement".to_owned(),
                    )
                })?;
                if resume.accepted_at_ms != due.due_at_ms
                    || !matches!(
                        &resume.settlement.decision,
                        crate::ApprovalDecision::Deny { .. }
                    )
                {
                    return Err(HarnessError::State(
                        "Agent Loop denial projection differs from its accepted settlement"
                            .to_owned(),
                    ));
                }
                if resume.accepted_at_ms > at_ms {
                    return Err(HarnessError::State(
                        "Agent Loop denial transition is later than trusted maintenance time"
                            .to_owned(),
                    ));
                }
                let command_id = deterministic_denial_command_id(due)?;
                let command_sha256 = wait_denial_command_sha256(
                    &due.thread_id,
                    &due.turn_id,
                    &due.wait_id,
                    due.revision,
                    &command_id,
                    &resume.settlement,
                    &frozen_authority,
                )?;
                let revision = due.revision.checked_add(1).ok_or_else(|| {
                    HarnessError::State("Agent Loop revision overflow".to_owned())
                })?;
                let evidence = WaitDenialEvidence {
                    wait_id: due.wait_id.clone(),
                    previous_revision: due.revision,
                    revision,
                    command_id: command_id.clone(),
                    command_sha256,
                    settlement: resume.settlement.clone(),
                    denied_at_ms: resume.accepted_at_ms,
                };
                validate_wait_denial_evidence(&exact.envelope, &evidence)?;
                let event_id = agent_loop_lifecycle_event_id(
                    AgentLoopLifecycleEvent::WaitDenied,
                    &due.thread_id,
                    &due.turn_id,
                    command_id.as_str(),
                )?;
                let approval_decision = Item {
                    id: agent_loop_maintenance_item_id(&event_id, "approval-decision")?,
                    created_at_ms: resume.accepted_at_ms,
                    kind: ItemKind::ApprovalDecision {
                        approval_id: resume.settlement.request.id.clone(),
                        call_id: resume.settlement.request.authorization.call_id.clone(),
                        decision: resume.settlement.decision.clone(),
                    },
                };
                let transition = Item {
                    id: agent_loop_maintenance_item_id(&event_id, "wait-denied")?,
                    created_at_ms: resume.accepted_at_ms,
                    kind: ItemKind::AgentLoopWaitDenied {
                        evidence: Box::new(evidence),
                    },
                };
                PendingEvent {
                    event_id,
                    thread_id: due.thread_id.clone(),
                    expected_stream_version: due.expected_stream_version,
                    expected_stream_recovery_bytes: due.expected_stream_recovery_bytes,
                    recorded_at_ms: resume.accepted_at_ms,
                    event: StateEvent::DenyWait {
                        turn_id: due.turn_id.clone(),
                        approval_decision,
                        transition,
                    },
                }
            }
        };

        let next_stream_version = due.expected_stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        let result = self.commit_pending_with_disposition(pending).await?;
        self.schedule_snapshot(due.thread_id.clone(), next_stream_version)
            .await;
        Ok(result)
    }

    async fn load_exact_due_wait_evidence(
        &self,
        due: &AgentLoopDueWait,
    ) -> Result<ExactDueWaitEvidence, HarnessError> {
        let wait_started = self
            .store
            .event_by_id(&due.wait_started_event_id)
            .await?
            .ok_or_else(|| {
                HarnessError::State(
                    "Agent Loop wait projection references a missing start event".to_owned(),
                )
            })?;
        let _ = validate_stored_event(&wait_started)?;
        if wait_started.event_id != due.wait_started_event_id
            || wait_started.thread_id != due.thread_id
        {
            return Err(HarnessError::State(
                "Agent Loop wait-start event differs from its projection".to_owned(),
            ));
        }
        let envelope = match &wait_started.event {
            StateEvent::WaitStarted {
                turn_id,
                approval_requested,
                transition:
                    Item {
                        kind: ItemKind::AgentLoopWaitStarted { envelope },
                        ..
                    },
            } if turn_id == &due.turn_id
                && envelope.wait_id == due.wait_id
                && approval_requested_matches_envelope(approval_requested, envelope) =>
            {
                (**envelope).clone()
            }
            _ => {
                return Err(HarnessError::State(
                    "Agent Loop wait-start event does not match its due fence".to_owned(),
                ));
            }
        };
        validate_wait_envelope(&envelope)?;
        if envelope.thread_id != due.thread_id
            || envelope.turn_id != due.turn_id
            || envelope.tenant_id != due.tenant_id
            || envelope.envelope_sha256 != due.envelope_sha256
        {
            return Err(HarnessError::State(
                "Agent Loop wait envelope differs from its due projection".to_owned(),
            ));
        }

        if due.phase == AgentLoopDuePhase::Waiting {
            if due.current_transition_event_id != due.wait_started_event_id
                || due.revision != envelope.revision
            {
                return Err(HarnessError::State(
                    "Waiting Agent Loop projection has an invalid current transition".to_owned(),
                ));
            }
            return Ok(ExactDueWaitEvidence {
                envelope,
                resume: None,
            });
        }

        let current = self
            .store
            .event_by_id(&due.current_transition_event_id)
            .await?
            .ok_or_else(|| {
                HarnessError::State(
                    "Agent Loop wait projection references a missing transition event".to_owned(),
                )
            })?;
        let _ = validate_stored_event(&current)?;
        if current.event_id != due.current_transition_event_id
            || current.thread_id != due.thread_id
            || current.sequence <= wait_started.sequence
        {
            return Err(HarnessError::State(
                "Agent Loop transition event differs from its projection".to_owned(),
            ));
        }
        let resume = match &current.event {
            StateEvent::AcceptResume {
                turn_id,
                approval_decision,
                transition:
                    Item {
                        kind: ItemKind::AgentLoopResumeAccepted { evidence },
                        ..
                    },
            } if turn_id == &due.turn_id
                && approval_decision_matches_resume(approval_decision, evidence) =>
            {
                (**evidence).clone()
            }
            _ => {
                return Err(HarnessError::State(
                    "Agent Loop transition event does not match its due fence".to_owned(),
                ));
            }
        };
        validate_resume_evidence(&envelope, &resume)?;
        let decision_matches_phase = matches!(
            (&due.phase, &resume.settlement.decision),
            (
                AgentLoopDuePhase::ReadyAllow,
                crate::ApprovalDecision::Approve
            ) | (
                AgentLoopDuePhase::ReadyDeny,
                crate::ApprovalDecision::Deny { .. }
            )
        );
        if !decision_matches_phase
            || resume.wait_id != due.wait_id
            || resume.revision != due.revision
        {
            return Err(HarnessError::State(
                "Agent Loop resume evidence differs from its due projection".to_owned(),
            ));
        }
        Ok(ExactDueWaitEvidence {
            envelope,
            resume: Some(resume),
        })
    }

    async fn matching_wait_event(
        &self,
        thread_id: &ThreadId,
        wait_id: &AgentLoopWaitId,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::WaitStarted {
                        transition: Item {
                            kind: ItemKind::AgentLoopWaitStarted { envelope },
                            ..
                        },
                        ..
                    } if &envelope.wait_id == wait_id
                )
            }))
    }

    async fn matching_resume_event(
        &self,
        thread_id: &ThreadId,
        command_id: &AgentLoopResumeCommandId,
        command_sha256: &str,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::AcceptResume {
                        transition: Item {
                            kind: ItemKind::AgentLoopResumeAccepted { evidence },
                            ..
                        },
                        ..
                    } if &evidence.command_id == command_id
                        && evidence.command_sha256 == command_sha256
                )
            }))
    }

    async fn matching_claim_event(
        &self,
        thread_id: &ThreadId,
        claim_id: &AgentLoopClaimId,
        claim_sha256: &str,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::ClaimReady {
                        transition: Item {
                            kind: ItemKind::AgentLoopReadyClaimed { evidence },
                            ..
                        },
                        ..
                    } if &evidence.claim_id == claim_id
                        && evidence.claim_sha256 == claim_sha256
                )
            }))
    }

    async fn matching_wait_close_event(
        &self,
        thread_id: &ThreadId,
        command_id: &AgentLoopCloseCommandId,
        command_sha256: &str,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::WaitClosed {
                        transition: Item {
                            kind: ItemKind::AgentLoopWaitClosed { evidence },
                            ..
                        },
                        ..
                    } if &evidence.command_id == command_id
                        && evidence.command_sha256 == command_sha256
                )
            }))
    }

    async fn matching_wait_denial_event(
        &self,
        thread_id: &ThreadId,
        command_id: &AgentLoopDenyCommandId,
        command_sha256: &str,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::DenyWait {
                        transition: Item {
                            kind: ItemKind::AgentLoopWaitDenied { evidence },
                            ..
                        },
                        ..
                    } if &evidence.command_id == command_id
                        && evidence.command_sha256 == command_sha256
                )
            }))
    }

    /// Settles a running Turn with a terminal status.
    pub async fn finish_turn(
        &self,
        turn: &Turn,
        status: TurnStatus,
    ) -> Result<StoredEvent, HarnessError> {
        self.finish_turn_as(turn, status, &AuthorityContext::local_process())
            .await
    }

    /// Settles a Turn inside the trusted authority's tenant boundary.
    pub async fn finish_turn_as(
        &self,
        turn: &Turn,
        status: TurnStatus,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        self.require_thread_access(&turn.thread_id, authority)
            .await?;
        if status == TurnStatus::Running {
            return Err(HarnessError::State(
                "cannot finish a turn with running status".to_owned(),
            ));
        }
        if status == TurnStatus::Completed {
            return Err(HarnessError::State(
                "completed Turns require an atomic CompletionReceipt".to_owned(),
            ));
        }
        let thread = self
            .load_thread_as(&turn.thread_id, authority)
            .await?
            .ok_or_else(|| {
                HarnessError::State(format!("thread {} does not exist", turn.thread_id))
            })?;
        let projected = projected_turn(&thread, turn)?;
        if matches!(
            agent_loop_execution_projection(projected)?,
            Some(AgentLoopExecution::Waiting { .. } | AgentLoopExecution::Ready { .. })
        ) {
            return Err(HarnessError::State(
                "cannot finish a Turn with an open Waiting or Ready execution".to_owned(),
            ));
        }
        let mut head = self.require_stream_head(&turn.thread_id).await?;
        if require_running_head(&head, &turn.id).is_err() {
            head = self.refresh_stream_head(&turn.thread_id).await?;
            require_running_head(&head, &turn.id)?;
        }
        let next_stream_version = head.stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        let stored = self
            .commit(
                turn.thread_id.clone(),
                head.stream_version,
                head.recovery_bytes,
                StateEvent::TurnFinished {
                    turn_id: turn.id.clone(),
                    status,
                },
            )
            .await?;
        self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
            .await;
        Ok(stored)
    }

    /// Atomically settles a running Turn as completed with its exact receipt.
    pub async fn complete_turn(
        &self,
        turn: &Turn,
        receipt: CompletionReceipt,
    ) -> Result<StoredEvent, HarnessError> {
        self.complete_turn_as(turn, receipt, &AuthorityContext::local_process())
            .await
    }

    /// Atomically completes a Turn inside the trusted authority boundary.
    ///
    /// The receipt is validated against the same authoritative projection
    /// that supplies both optimistic-CAS coordinates. After any ambiguous
    /// append outcome, the journal is reloaded and an exact matching receipt
    /// is treated as an idempotent success.
    pub async fn complete_turn_as(
        &self,
        turn: &Turn,
        receipt: CompletionReceipt,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        authority.validate_current("Turn completion authority")?;
        if receipt.authority() != authority {
            return Err(HarnessError::State(
                "CompletionReceipt authority differs from the completing authority".to_owned(),
            ));
        }
        let receipt_sha256 = completion_receipt_sha256(&receipt)?;
        let loaded = self.load_projection(&turn.thread_id).await?;
        let thread = loaded.thread.as_ref().ok_or_else(|| {
            HarnessError::State(format!("thread {} does not exist", turn.thread_id))
        })?;
        validate_thread_authority(thread, authority)?;
        let projected = thread
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .ok_or_else(|| {
                HarnessError::State(format!(
                    "turn {} does not belong to thread {}",
                    turn.id, turn.thread_id
                ))
            })?;

        match projected.status {
            TurnStatus::Completed => {
                if projected.completion_receipt.as_ref() != Some(&receipt) {
                    return Err(HarnessError::State(format!(
                        "turn {} is already completed with a different receipt",
                        turn.id
                    )));
                }
                validate_projected_turn_completion_receipt(
                    projected,
                    thread.tenant_id(),
                    &receipt,
                )?;
                return self
                    .matching_completion_event(&turn.thread_id, &turn.id, &receipt)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::State(format!(
                            "completed turn {} has no atomic completion event",
                            turn.id
                        ))
                    });
            }
            TurnStatus::Running => {}
            _ => {
                return Err(HarnessError::State(format!(
                    "turn {} is already terminal with status {:?}",
                    turn.id, projected.status
                )));
            }
        }
        if has_pending_steering(projected)? {
            return Err(HarnessError::State(format!(
                "cannot complete turn {} with unapplied steering",
                turn.id
            )));
        }
        if matches!(
            agent_loop_execution_projection(projected)?,
            Some(AgentLoopExecution::Waiting { .. } | AgentLoopExecution::Ready { .. })
        ) {
            return Err(HarnessError::State(
                "cannot complete a Turn with an open Waiting or Ready execution".to_owned(),
            ));
        }
        validate_turn_completion_receipt(projected, thread.tenant_id(), &receipt)?;

        let next_stream_version = loaded.stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        let pending = PendingEvent {
            event_id: EventId::from_string(format!("turn-completion-{receipt_sha256}")),
            thread_id: turn.thread_id.clone(),
            expected_stream_version: loaded.stream_version,
            expected_stream_recovery_bytes: loaded.recovery_bytes,
            recorded_at_ms: crate::kernel::now_ms(),
            event: StateEvent::TurnCompleted {
                turn_id: turn.id.clone(),
                receipt: receipt.clone(),
            },
        };
        let result = self.commit_pending(pending).await;
        match result {
            Ok(stored) => {
                self.schedule_snapshot(turn.thread_id.clone(), next_stream_version)
                    .await;
                Ok(stored)
            }
            Err(error) => {
                if let Some(stored) = self
                    .completion_after_ambiguous_append(turn, &receipt, authority)
                    .await?
                {
                    Ok(stored)
                } else {
                    Err(error)
                }
            }
        }
    }

    async fn completion_after_ambiguous_append(
        &self,
        turn: &Turn,
        receipt: &CompletionReceipt,
        authority: &AuthorityContext,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        let loaded = self.load_projection(&turn.thread_id).await?;
        let Some(thread) = loaded.thread.as_ref() else {
            return Ok(None);
        };
        validate_thread_authority(thread, authority)?;
        let Some(projected) = thread
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
        else {
            return Ok(None);
        };
        if projected.status != TurnStatus::Completed
            || projected.completion_receipt.as_ref() != Some(receipt)
        {
            return Ok(None);
        }
        validate_projected_turn_completion_receipt(projected, thread.tenant_id(), receipt)?;
        self.cache_head(
            turn.thread_id.clone(),
            stream_head_from_parts(
                thread,
                loaded.stream_version,
                loaded.recovery_bytes,
                loaded.last_sequence,
            ),
        )
        .await;
        self.matching_completion_event(&turn.thread_id, &turn.id, receipt)
            .await
    }

    async fn matching_completion_event(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
        receipt: &CompletionReceipt,
    ) -> Result<Option<StoredEvent>, HarnessError> {
        Ok(self
            .checked_events(thread_id)
            .await?
            .events
            .into_iter()
            .find(|stored| {
                matches!(
                    &stored.event,
                    StateEvent::TurnCompleted {
                        turn_id: completed_turn_id,
                        receipt: completed_receipt,
                    } if completed_turn_id == turn_id && completed_receipt == receipt
                )
            }))
    }

    /// Marks one exact unfinished Turn interrupted and returns the recovered projection.
    ///
    /// Callers must hold exclusive Thread ownership and know that the previous
    /// worker is no longer live. Recovery is a takeover operation, not a normal
    /// preflight for starting a Turn. The expected Turn identity is rechecked
    /// at the optimistic commit boundary, so a stale takeover cannot interrupt
    /// a newer running Turn. A durable Waiting, Ready, or Executing lifecycle
    /// must be settled or explicitly reconciled through its own fenced API and
    /// is never converted to `Interrupted` by this generic takeover path.
    pub async fn recover_thread(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
    ) -> Result<Option<Thread>, HarnessError> {
        self.recover_thread_as(
            thread_id,
            expected_turn_id,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Recovers one exact Turn inside the trusted tenant boundary.
    pub async fn recover_thread_as(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        authority: &AuthorityContext,
    ) -> Result<Option<Thread>, HarnessError> {
        authority.validate_current("Thread recovery authority")?;
        let loaded = self.load_projection(thread_id).await?;
        let Some(thread) = loaded.thread.as_ref() else {
            return Ok(None);
        };
        validate_thread_authority(thread, authority)?;
        let expected = thread
            .turns
            .iter()
            .find(|turn| &turn.id == expected_turn_id)
            .ok_or_else(|| {
                HarnessError::State(format!(
                    "expected recovery Turn {expected_turn_id} does not belong to thread {thread_id}"
                ))
            })?;
        if expected.status == TurnStatus::Interrupted
            && thread
                .turns
                .iter()
                .all(|turn| turn.status != TurnStatus::Running)
        {
            return Ok(Some(thread.clone()));
        }
        if expected.status != TurnStatus::Running
            || thread
                .turns
                .iter()
                .filter(|turn| turn.status == TurnStatus::Running)
                .count()
                != 1
        {
            return Err(HarnessError::State(format!(
                "thread {thread_id} is not awaiting takeover of Turn {expected_turn_id}"
            )));
        }
        if agent_loop_execution_projection(expected)?.is_some() {
            return Err(HarnessError::State(format!(
                "exclusive recovery cannot interrupt live durable Agent Loop execution for Turn {expected_turn_id}; settle or explicitly reconcile its wait first"
            )));
        }

        // Recovery is an exclusive takeover CAS. Commit against the exact
        // projection inspected above so a concurrent wait start, resume, or
        // worker claim wins the stream instead of being overwritten by a
        // second projection load in the generic worker-settlement path.
        let next_stream_version = loaded.stream_version.checked_add(1).ok_or_else(|| {
            HarnessError::State("cannot schedule snapshot after stream-version overflow".to_owned())
        })?;
        self.commit(
            thread_id.clone(),
            loaded.stream_version,
            loaded.recovery_bytes,
            StateEvent::TurnFinished {
                turn_id: expected_turn_id.clone(),
                status: TurnStatus::Interrupted,
            },
        )
        .await?;
        self.schedule_snapshot(thread_id.clone(), next_stream_version)
            .await;
        self.load_thread_as(thread_id, authority).await
    }

    /// Persists a checkpoint targeting the current Thread sequence.
    pub async fn create_checkpoint(
        &self,
        thread_id: &ThreadId,
        turn_id: Option<TurnId>,
        label: Option<String>,
    ) -> Result<Checkpoint, HarnessError> {
        self.create_checkpoint_as(
            thread_id,
            turn_id,
            label,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Persists a checkpoint inside the trusted tenant boundary.
    pub async fn create_checkpoint_as(
        &self,
        thread_id: &ThreadId,
        turn_id: Option<TurnId>,
        label: Option<String>,
        authority: &AuthorityContext,
    ) -> Result<Checkpoint, HarnessError> {
        validate_checkpoint_label(label.as_deref())?;
        self.require_thread_access(thread_id, authority).await?;
        let mut head = self.require_stream_head(thread_id).await?;
        if let Some(turn_id) = &turn_id
            && !head.turns.contains(turn_id)
        {
            head = self.refresh_stream_head(thread_id).await?;
            if !head.turns.contains(turn_id) {
                return Err(HarnessError::State(format!(
                    "turn {turn_id} does not belong to thread {thread_id}"
                )));
            }
        }
        let checkpoint = Checkpoint {
            id: CheckpointId::generate(),
            thread_id: thread_id.clone(),
            turn_id,
            target_sequence: head.last_sequence,
            created_at_ms: crate::kernel::now_ms(),
            label,
        };
        self.commit(
            thread_id.clone(),
            head.stream_version,
            head.recovery_bytes,
            StateEvent::CheckpointCreated {
                checkpoint: checkpoint.clone(),
            },
        )
        .await?;
        Ok(checkpoint)
    }

    async fn require_stream_head(&self, thread_id: &ThreadId) -> Result<StreamHead, HarnessError> {
        if let Some(head) = self.heads.lock().await.get(thread_id).cloned() {
            return Ok(head);
        }
        self.refresh_stream_head(thread_id).await
    }

    async fn thread_accessible(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<bool, HarnessError> {
        validate_state_id("thread", thread_id.as_str())?;
        authority.validate_current("Thread access authority")?;
        self.store
            .thread_accessible(thread_id, authority.tenant_id().map(str::to_owned))
            .await
    }

    async fn require_thread_access(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        if self.thread_accessible(thread_id, authority).await? {
            Ok(())
        } else {
            Err(HarnessError::State(format!(
                "thread {thread_id} does not exist"
            )))
        }
    }

    async fn refresh_stream_head(&self, thread_id: &ThreadId) -> Result<StreamHead, HarnessError> {
        let loaded = self.load_projection(thread_id).await?;
        let thread = loaded
            .thread
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        let head = stream_head_from_parts(
            &thread,
            loaded.stream_version,
            loaded.recovery_bytes,
            loaded.last_sequence,
        );
        self.cache_head(thread_id.clone(), head.clone()).await;
        Ok(head)
    }

    async fn load_projection(
        &self,
        thread_id: &ThreadId,
    ) -> Result<LoadedProjection, HarnessError> {
        validate_state_id("thread", thread_id.as_str())?;
        if let Some(loaded) = self.try_snapshot_projection(thread_id).await? {
            return Ok(loaded);
        }
        let checked = self.checked_events(thread_id).await?;
        let events = checked.events;
        Ok(LoadedProjection {
            thread: project_events(&events)?,
            stream_version: u64::try_from(events.len())
                .map_err(|_| HarnessError::State("stream version exceeds u64".to_owned()))?,
            recovery_bytes: checked.recovery_bytes,
            last_sequence: events.last().map_or(0, |event| event.sequence),
        })
    }

    async fn try_snapshot_projection(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<LoadedProjection>, HarnessError> {
        let snapshot = match self.store.load_snapshot(thread_id).await {
            Ok(Some(snapshot)) if validate_snapshot(&snapshot).is_ok() => snapshot,
            Ok(None) | Ok(Some(_)) | Err(_) => return Ok(None),
        };
        if &snapshot.thread.id != thread_id {
            return Ok(None);
        }

        let anchor_after = snapshot.through_sequence.saturating_sub(1);
        match self
            .store
            .events_page(
                thread_id,
                anchor_after,
                1,
                MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
            )
            .await
        {
            Ok(events)
                if validate_stored_events(
                    thread_id,
                    &events,
                    anchor_after,
                    Some(1),
                    Some(MAX_STATE_EVENT_PAGE_RECOVERY_BYTES),
                )
                .is_ok()
                    && events.len() == 1
                    && events[0].sequence == snapshot.through_sequence
                    && events[0].event_id == snapshot.anchor_event_id => {}
            Ok(_) | Err(_) => return Ok(None),
        }

        let checked_tail = self
            .snapshot_tail(
                thread_id,
                snapshot.through_sequence,
                STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(snapshot.recovery_bytes),
            )
            .await?;
        let tail = checked_tail.events;
        let tail_len = u64::try_from(tail.len())
            .map_err(|_| HarnessError::State("snapshot tail length exceeds u64".to_owned()))?;
        let stream_version = snapshot
            .stream_version
            .checked_add(tail_len)
            .ok_or_else(|| HarnessError::State("snapshot stream version overflow".to_owned()))?;
        if stream_version > MAX_STATE_RECOVERY_EVENTS as u64 {
            return Err(HarnessError::State(format!(
                "Thread exceeds {MAX_STATE_RECOVERY_EVENTS} recoverable events"
            )));
        }
        let recovery_bytes = snapshot
            .recovery_bytes
            .checked_add(checked_tail.recovery_bytes)
            .ok_or_else(|| HarnessError::State("snapshot recovery charge overflow".to_owned()))?;
        if recovery_bytes > STATE_THREAD_RECOVERY_BYTE_LIMIT {
            return Err(HarnessError::State(format!(
                "Thread exceeds its {STATE_THREAD_RECOVERY_BYTE_LIMIT}-byte recovery boundary"
            )));
        }
        let last_sequence = tail
            .last()
            .map_or(snapshot.through_sequence, |event| event.sequence);
        let mut thread = Some(snapshot.thread);
        apply_events(&mut thread, &tail)?;
        Ok(Some(LoadedProjection {
            thread,
            stream_version,
            recovery_bytes,
            last_sequence,
        }))
    }

    async fn snapshot_tail(
        &self,
        thread_id: &ThreadId,
        through_sequence: u64,
        max_recovery_bytes: u64,
    ) -> Result<CheckedEvents, HarnessError> {
        let mut after_sequence = through_sequence;
        let mut recovery_bytes = 0_u64;
        let mut event_ids = BTreeSet::new();
        let mut tail = Vec::new();
        loop {
            let page = self
                .store
                .events_page(
                    thread_id,
                    after_sequence,
                    SNAPSHOT_TAIL_PAGE,
                    MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
                )
                .await?;
            let page_recovery_bytes = validate_stored_events(
                thread_id,
                &page,
                after_sequence,
                Some(SNAPSHOT_TAIL_PAGE),
                Some(MAX_STATE_EVENT_PAGE_RECOVERY_BYTES),
            )?;
            if page.is_empty() {
                break;
            }
            recovery_bytes = recovery_bytes
                .checked_add(page_recovery_bytes)
                .ok_or_else(|| HarnessError::State("snapshot tail charge overflow".to_owned()))?;
            if recovery_bytes > max_recovery_bytes {
                return Err(HarnessError::State(
                    "snapshot tail exceeds the Thread recovery-byte boundary".to_owned(),
                ));
            }
            remember_event_ids(&mut event_ids, &page)?;
            after_sequence = page.last().map_or(after_sequence, |event| event.sequence);
            tail.extend(page);
            if tail.len() > MAX_STATE_RECOVERY_EVENTS {
                return Err(HarnessError::State(format!(
                    "snapshot tail exceeds {MAX_STATE_RECOVERY_EVENTS} events"
                )));
            }
        }
        Ok(CheckedEvents {
            events: tail,
            recovery_bytes,
        })
    }

    async fn checked_events(&self, thread_id: &ThreadId) -> Result<CheckedEvents, HarnessError> {
        validate_state_id("thread", thread_id.as_str())?;
        let mut after_sequence = 0_u64;
        let mut recovery_bytes = 0_u64;
        let mut event_ids = BTreeSet::new();
        let mut events = Vec::new();
        loop {
            let page = self
                .store
                .events_page(
                    thread_id,
                    after_sequence,
                    SNAPSHOT_TAIL_PAGE,
                    MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
                )
                .await?;
            let page_recovery_bytes = validate_stored_events(
                thread_id,
                &page,
                after_sequence,
                Some(SNAPSHOT_TAIL_PAGE),
                Some(MAX_STATE_EVENT_PAGE_RECOVERY_BYTES),
            )?;
            if page.is_empty() {
                break;
            }
            recovery_bytes = recovery_bytes
                .checked_add(page_recovery_bytes)
                .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))?;
            if recovery_bytes > STATE_THREAD_RECOVERY_BYTE_LIMIT {
                return Err(HarnessError::State(format!(
                    "Thread exceeds its {STATE_THREAD_RECOVERY_BYTE_LIMIT}-byte recovery boundary"
                )));
            }
            remember_event_ids(&mut event_ids, &page)?;
            after_sequence = page.last().map_or(after_sequence, |event| event.sequence);
            events.extend(page);
            if events.len() > MAX_STATE_RECOVERY_EVENTS {
                return Err(HarnessError::State(format!(
                    "Thread exceeds {MAX_STATE_RECOVERY_EVENTS} recoverable events"
                )));
            }
        }
        Ok(CheckedEvents {
            events,
            recovery_bytes,
        })
    }

    async fn schedule_snapshot(&self, thread_id: ThreadId, stream_version: u64) {
        let Some(maintenance) = self.snapshot_maintenance.clone() else {
            return;
        };
        if stream_version < maintenance.config.every_events {
            maintenance
                .metrics
                .skipped_cadence
                .fetch_add(1, Ordering::Relaxed);
            return;
        }

        let mut schedule = maintenance.schedule.lock().await;
        if schedule.in_flight.contains(&thread_id) {
            maintenance
                .metrics
                .skipped_in_flight
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        if schedule
            .watermarks
            .get(&thread_id)
            .is_some_and(|watermark| {
                stream_version.saturating_sub(*watermark) < maintenance.config.every_events
            })
        {
            maintenance
                .metrics
                .skipped_cadence
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(permit) = Arc::clone(&maintenance.permits).try_acquire_owned() else {
            maintenance
                .metrics
                .skipped_capacity
                .fetch_add(1, Ordering::Relaxed);
            return;
        };

        schedule.in_flight.insert(thread_id.clone());
        schedule
            .watermarks
            .insert(thread_id.clone(), stream_version);
        drop(schedule);
        maintenance
            .metrics
            .scheduled
            .fetch_add(1, Ordering::Relaxed);
        maintenance.metrics.active.fetch_add(1, Ordering::Release);

        let engine = self.clone();
        task::spawn(async move {
            let worker_thread_id = thread_id.clone();
            let worker = task::spawn(async move {
                engine
                    .maintain_snapshot(&worker_thread_id, stream_version)
                    .await
            });
            let result = worker.await;

            let mut schedule = maintenance.schedule.lock().await;
            match result {
                Ok(Ok(SnapshotMaintenanceOutcome::Created(version))) => {
                    schedule
                        .watermarks
                        .entry(thread_id.clone())
                        .and_modify(|watermark| *watermark = (*watermark).max(version))
                        .or_insert(version);
                    maintenance
                        .metrics
                        .last_created_at_ms
                        .store(crate::kernel::now_ms(), Ordering::Relaxed);
                    maintenance.metrics.created.fetch_add(1, Ordering::Release);
                }
                Ok(Ok(SnapshotMaintenanceOutcome::AlreadyCurrent(version))) => {
                    schedule
                        .watermarks
                        .entry(thread_id.clone())
                        .and_modify(|watermark| *watermark = (*watermark).max(version))
                        .or_insert(version);
                    maintenance
                        .metrics
                        .already_current
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(Err(_)) => maintenance
                    .metrics
                    .record_failure(SnapshotMaintenanceFailure::Operation),
                Err(error) if error.is_panic() => maintenance
                    .metrics
                    .record_failure(SnapshotMaintenanceFailure::WorkerPanicked),
                Err(_) => maintenance
                    .metrics
                    .record_failure(SnapshotMaintenanceFailure::WorkerCancelled),
            }
            schedule.in_flight.remove(&thread_id);
            drop(schedule);
            drop(permit);
            maintenance.metrics.active.fetch_sub(1, Ordering::Release);
            maintenance.drained.notify_waiters();
        });
    }

    async fn maintain_snapshot(
        &self,
        thread_id: &ThreadId,
        scheduled_stream_version: u64,
    ) -> Result<SnapshotMaintenanceOutcome, HarnessError> {
        if let Ok(Some(snapshot)) = self.store.load_snapshot(thread_id).await
            && validate_snapshot(&snapshot).is_ok()
            && &snapshot.thread.id == thread_id
            && scheduled_stream_version.saturating_sub(snapshot.stream_version)
                < self
                    .snapshot_maintenance
                    .as_ref()
                    .map_or(u64::MAX, |maintenance| maintenance.config.every_events)
        {
            return Ok(SnapshotMaintenanceOutcome::AlreadyCurrent(
                snapshot.stream_version,
            ));
        }
        let snapshot = self.create_snapshot_unchecked(thread_id).await?;
        Ok(SnapshotMaintenanceOutcome::Created(snapshot.stream_version))
    }

    async fn cache_head(&self, thread_id: ThreadId, head: StreamHead) {
        let mut heads = self.heads.lock().await;
        if heads
            .get(&thread_id)
            .is_none_or(|current| current.stream_version <= head.stream_version)
        {
            heads.insert(thread_id, head);
        }
    }

    async fn advance_head(
        &self,
        expected_version: u64,
        expected_recovery_bytes: u64,
        event_recovery_bytes: u64,
        stored: &StoredEvent,
    ) {
        let mut heads = self.heads.lock().await;
        let base = match heads.get(&stored.thread_id) {
            Some(head)
                if head.stream_version == expected_version
                    && head.recovery_bytes == expected_recovery_bytes =>
            {
                head.clone()
            }
            None if expected_version == 0
                && expected_recovery_bytes == 0
                && matches!(&stored.event, StateEvent::ThreadCreated { .. }) =>
            {
                StreamHead {
                    stream_version: 0,
                    recovery_bytes: 0,
                    last_sequence: 0,
                    turns: Arc::new(BTreeSet::new()),
                    running_turn: None,
                }
            }
            _ => return,
        };
        let Some(stream_version) = expected_version.checked_add(1) else {
            return;
        };
        let Some(recovery_bytes) = expected_recovery_bytes.checked_add(event_recovery_bytes) else {
            return;
        };
        let mut next = StreamHead {
            stream_version,
            recovery_bytes,
            last_sequence: stored.sequence,
            ..base
        };
        match &stored.event {
            StateEvent::ThreadCreated { .. }
            | StateEvent::ThreadNamed { .. }
            | StateEvent::ThreadForked { .. }
            | StateEvent::ThreadImported { .. }
            | StateEvent::ItemAppended { .. }
            | StateEvent::ToolCallsAppended { .. }
            | StateEvent::WaitStarted { .. }
            | StateEvent::AcceptResume { .. }
            | StateEvent::ClaimReady { .. }
            | StateEvent::CheckpointCreated { .. } => {}
            StateEvent::TurnStarted { turn_id } => {
                let mut turns = (*next.turns).clone();
                turns.insert(turn_id.clone());
                next.turns = Arc::new(turns);
                next.running_turn = Some(turn_id.clone());
            }
            StateEvent::TurnFinished { turn_id, .. } => {
                if next.running_turn.as_ref() == Some(turn_id) {
                    next.running_turn = None;
                }
            }
            StateEvent::TurnCompleted { turn_id, .. } => {
                if next.running_turn.as_ref() == Some(turn_id) {
                    next.running_turn = None;
                }
            }
            StateEvent::WaitClosed { turn_id, .. } | StateEvent::DenyWait { turn_id, .. } => {
                if next.running_turn.as_ref() == Some(turn_id) {
                    next.running_turn = None;
                }
            }
        }
        heads.insert(stored.thread_id.clone(), next);
    }

    async fn invalidate_head(&self, thread_id: &ThreadId, expected: u64) {
        let mut heads = self.heads.lock().await;
        if heads
            .get(thread_id)
            .is_some_and(|head| head.stream_version <= expected)
        {
            heads.remove(thread_id);
        }
    }

    async fn commit(
        &self,
        thread_id: ThreadId,
        expected_stream_version: u64,
        expected_stream_recovery_bytes: u64,
        event: StateEvent,
    ) -> Result<StoredEvent, HarnessError> {
        let pending = PendingEvent {
            event_id: EventId::generate(),
            thread_id,
            expected_stream_version,
            expected_stream_recovery_bytes,
            recorded_at_ms: crate::kernel::now_ms(),
            event,
        };
        self.commit_pending(pending).await
    }

    async fn commit_pending(&self, pending: PendingEvent) -> Result<StoredEvent, HarnessError> {
        let thread_id = pending.thread_id.clone();
        let expected_stream_version = pending.expected_stream_version;
        let expected_stream_recovery_bytes = pending.expected_stream_recovery_bytes;
        let encoded = validate_pending_event(&pending)?;
        let result = self.store.append(pending.clone()).await;
        match result {
            Ok(stored) => {
                validate_append_result(&pending, &stored)?;
                self.advance_head(
                    expected_stream_version,
                    expected_stream_recovery_bytes,
                    encoded.recovery_bytes,
                    &stored,
                )
                .await;
                Ok(stored)
            }
            Err(error @ HarnessError::StateConflict { .. }) => {
                self.invalidate_head(&thread_id, expected_stream_version)
                    .await;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    async fn commit_pending_with_disposition(
        &self,
        pending: PendingEvent,
    ) -> Result<EventAppendResult, HarnessError> {
        let thread_id = pending.thread_id.clone();
        let expected_stream_version = pending.expected_stream_version;
        let expected_stream_recovery_bytes = pending.expected_stream_recovery_bytes;
        let encoded = validate_pending_event(&pending)?;
        let result = self.store.append_with_disposition(pending.clone()).await;
        match result {
            Ok(result) => {
                validate_append_result(&pending, &result.stored)?;
                if result.disposition == EventAppendDisposition::Unknown {
                    return Err(HarnessError::State(
                        "Agent Loop wait maintenance requires exact append disposition".to_owned(),
                    ));
                }
                self.advance_head(
                    expected_stream_version,
                    expected_stream_recovery_bytes,
                    encoded.recovery_bytes,
                    &result.stored,
                )
                .await;
                Ok(result)
            }
            Err(error @ HarnessError::StateConflict { .. }) => {
                self.invalidate_head(&thread_id, expected_stream_version)
                    .await;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

fn stream_head_from_parts(
    thread: &Thread,
    stream_version: u64,
    recovery_bytes: u64,
    last_sequence: u64,
) -> StreamHead {
    let turns = thread
        .turns
        .iter()
        .map(|turn| turn.id.clone())
        .collect::<BTreeSet<_>>();
    let running_turn = thread
        .turns
        .iter()
        .find(|turn| turn.status == TurnStatus::Running)
        .map(|turn| turn.id.clone());
    StreamHead {
        stream_version,
        recovery_bytes,
        last_sequence,
        turns: Arc::new(turns),
        running_turn,
    }
}

fn fork_boundary(
    events: &[StoredEvent],
    parent: &Thread,
    through_turn_id: Option<&TurnId>,
) -> Result<usize, HarnessError> {
    let Some(turn_id) = through_turn_id else {
        if parent
            .turns
            .iter()
            .any(|turn| turn.status == TurnStatus::Running)
        {
            return Err(HarnessError::State(
                "cannot fork the latest parent state while a Turn is running; select an earlier terminal Turn"
                    .to_owned(),
            ));
        }
        return Ok(events.len());
    };
    let turn = parent
        .turns
        .iter()
        .find(|turn| &turn.id == turn_id)
        .ok_or_else(|| HarnessError::State(format!("turn {turn_id} does not exist")))?;
    if turn.status == TurnStatus::Running {
        return Err(HarnessError::State(format!(
            "turn {turn_id} is not terminal"
        )));
    }
    events
        .iter()
        .position(|stored| {
            matches!(
                &stored.event,
                StateEvent::TurnFinished {
                    turn_id: finished,
                    ..
                }
                | StateEvent::TurnCompleted {
                    turn_id: finished,
                    ..
                }
                | StateEvent::WaitClosed {
                    turn_id: finished,
                    ..
                }
                | StateEvent::DenyWait {
                    turn_id: finished,
                    ..
                } if finished == turn_id
            )
        })
        .and_then(|index| index.checked_add(1))
        .ok_or_else(|| HarnessError::State(format!("turn {turn_id} has no terminal event")))
}

fn validate_existing_fork(
    child: &Thread,
    lineage: &ThreadLineage,
    inherited_turns: &[Turn],
) -> Result<(), HarnessError> {
    let history_matches = child.turns.len() >= inherited_turns.len()
        && child
            .turns
            .iter()
            .zip(inherited_turns)
            .all(|(child_turn, parent_turn)| {
                child_turn.thread_id == child.id
                    && child_turn.id == parent_turn.id
                    && child_turn.status == parent_turn.status
                    && child_turn.completion_receipt == parent_turn.completion_receipt
                    && child_turn.items == parent_turn.items
            });
    if child.lineage.as_ref() != Some(lineage) || !history_matches {
        return Err(HarnessError::State(format!(
            "thread {} already exists with different fork provenance",
            child.id
        )));
    }
    Ok(())
}

fn validate_existing_import(
    target: &Thread,
    origin: &ThreadImportOrigin,
    imported_turns: &[Turn],
) -> Result<(), HarnessError> {
    let history_matches = target.turns.len() >= imported_turns.len()
        && target
            .turns
            .iter()
            .zip(imported_turns)
            .all(|(target_turn, source_turn)| {
                target_turn.thread_id == target.id
                    && target_turn.id == source_turn.id
                    && target_turn.status == source_turn.status
                    && target_turn.completion_receipt == source_turn.completion_receipt
                    && target_turn.items == source_turn.items
            });
    if target.import_origin.as_ref() != Some(origin) || target.lineage.is_some() || !history_matches
    {
        return Err(HarnessError::State(format!(
            "thread {} already exists with different import provenance",
            target.id
        )));
    }
    Ok(())
}

fn validate_import_authority_bindings(
    source: &Thread,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    let execution_tenant_differs = source
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| {
            if let ItemKind::ExecutionBinding { binding, .. } = &item.kind {
                Some(binding)
            } else {
                None
            }
        })
        .any(|binding| binding.tenant_id() != authority.tenant_id());
    let evidence_tenant_differs = source
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| {
            if let ItemKind::ToolResult {
                connector_evidence, ..
            } = &item.kind
            {
                Some(connector_evidence)
            } else {
                None
            }
        })
        .flatten()
        .any(|evidence| evidence.authority().tenant_id() != authority.tenant_id());
    let completion_tenant_differs = source
        .turns
        .iter()
        .filter_map(|turn| turn.completion_receipt.as_ref())
        .any(|receipt| receipt.authority().tenant_id() != authority.tenant_id());
    let wait_tenant_differs = source
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter_map(|item| {
            if let ItemKind::AgentLoopWaitStarted { envelope } = &item.kind {
                Some(envelope.tenant_id.as_deref())
            } else {
                None
            }
        })
        .any(|tenant_id| tenant_id != authority.tenant_id());
    if execution_tenant_differs
        || evidence_tenant_differs
        || completion_tenant_differs
        || wait_tenant_differs
    {
        return Err(HarnessError::State(
            "cannot rebind a Thread archive containing tenant-bound authority evidence".to_owned(),
        ));
    }
    Ok(())
}

fn validate_thread_archive(archive: &ThreadArchive) -> Result<Thread, HarnessError> {
    if !matches!(
        archive.format_version,
        PREVIOUS_THREAD_ARCHIVE_FORMAT_VERSION | THREAD_ARCHIVE_FORMAT_VERSION
    ) {
        return Err(HarnessError::State(format!(
            "unsupported Thread archive format {}",
            archive.format_version
        )));
    }
    if archive.format_version == PREVIOUS_THREAD_ARCHIVE_FORMAT_VERSION
        && archive.events.iter().any(|stored| {
            stored.schema_version > 15
                || matches!(
                    &stored.event,
                    StateEvent::WaitStarted { .. }
                        | StateEvent::AcceptResume { .. }
                        | StateEvent::ClaimReady { .. }
                        | StateEvent::WaitClosed { .. }
                        | StateEvent::DenyWait { .. }
                )
        })
    {
        return Err(HarnessError::State(
            "Thread archive format 5 cannot contain schema-16 Agent Loop waits".to_owned(),
        ));
    }
    validate_state_id("archive source thread", archive.source_thread_id.as_str())?;
    if archive.events.is_empty()
        || archive.source_stream_version
            != u64::try_from(archive.events.len())
                .map_err(|_| HarnessError::State("archive event count exceeds u64".to_owned()))?
        || archive.source_last_sequence != archive.events.last().map_or(0, |event| event.sequence)
    {
        return Err(HarnessError::State(
            "Thread archive boundary does not match its event journal".to_owned(),
        ));
    }
    let _ = validate_stored_events(&archive.source_thread_id, &archive.events, 0, None, None)?;
    if archive.source_events_sha256 != state_events_sha256(&archive.events)? {
        return Err(HarnessError::State(
            "Thread archive event digest mismatch".to_owned(),
        ));
    }
    let thread = project_events(&archive.events)?
        .ok_or_else(|| HarnessError::State("Thread archive has no projection".to_owned()))?;
    if thread.id != archive.source_thread_id {
        return Err(HarnessError::State(
            "Thread archive projection identity mismatch".to_owned(),
        ));
    }
    if thread
        .turns
        .iter()
        .any(|turn| turn.status == TurnStatus::Running)
    {
        return Err(HarnessError::State(
            "Thread archive cannot end with a running Turn".to_owned(),
        ));
    }
    bounded_serialized_size(archive, MAX_THREAD_ARCHIVE_BYTES)
        .map_err(|error| state_json_error("Thread archive", MAX_THREAD_ARCHIVE_BYTES, error))?;
    Ok(thread)
}

fn state_events_sha256(events: &[StoredEvent]) -> Result<String, HarnessError> {
    struct DigestWriter<'a>(&'a mut Sha256);

    impl Write for DigestWriter<'_> {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.update(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut digest = Sha256::new();
    serde_json::to_writer(DigestWriter(&mut digest), events)
        .map_err(|error| HarnessError::State(format!("cannot hash State journal: {error}")))?;
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn validate_atomic_stream_result(
    thread_id: &ThreadId,
    expected: &[NewStreamEvent],
    stored: &[StoredEvent],
) -> Result<u64, HarnessError> {
    let recovery_bytes = validate_stored_events(thread_id, stored, 0, None, None)?;
    if stored.len() != expected.len()
        || stored.iter().zip(expected).any(|(stored, expected)| {
            stored.schema_version != expected.schema_version
                || stored.event_id != expected.event_id
                || stored.recorded_at_ms != expected.recorded_at_ms
                || stored.event != expected.event
        })
    {
        return Err(HarnessError::State(
            "Event Store returned an atomic stream that differs from the request".to_owned(),
        ));
    }
    Ok(recovery_bytes)
}

fn nonzero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn state_capacity(used_events: u64, used_recovery_bytes: u64) -> StateCapacity {
    let event_level = if used_events >= STATE_THREAD_EVENT_LIMIT {
        StateCapacityLevel::Exhausted
    } else if used_events >= STATE_THREAD_EVENT_LIMIT.saturating_sub(STATE_TERMINAL_EVENT_RESERVE) {
        StateCapacityLevel::TerminalOnly
    } else if used_events >= STATE_CAPACITY_CRITICAL_AT {
        StateCapacityLevel::Critical
    } else if used_events >= STATE_CAPACITY_WARNING_AT {
        StateCapacityLevel::Warning
    } else {
        StateCapacityLevel::Healthy
    };
    let recovery_level = if used_recovery_bytes >= STATE_THREAD_RECOVERY_BYTE_LIMIT {
        StateCapacityLevel::Exhausted
    } else if used_recovery_bytes
        >= STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(STATE_TERMINAL_RECOVERY_BYTE_RESERVE)
    {
        StateCapacityLevel::TerminalOnly
    } else if used_recovery_bytes >= STATE_RECOVERY_CAPACITY_CRITICAL_AT {
        StateCapacityLevel::Critical
    } else if used_recovery_bytes >= STATE_RECOVERY_CAPACITY_WARNING_AT {
        StateCapacityLevel::Warning
    } else {
        StateCapacityLevel::Healthy
    };
    let level = more_severe_capacity_level(event_level, recovery_level);
    let remaining_events = STATE_THREAD_EVENT_LIMIT.saturating_sub(used_events);
    let terminal_event_reserve = remaining_events.min(STATE_TERMINAL_EVENT_RESERVE);
    let remaining_recovery_bytes =
        STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(used_recovery_bytes);
    let terminal_recovery_byte_reserve =
        remaining_recovery_bytes.min(STATE_TERMINAL_RECOVERY_BYTE_RESERVE);
    StateCapacity {
        used_events,
        event_limit: STATE_THREAD_EVENT_LIMIT,
        remaining_events,
        general_events_remaining: remaining_events.saturating_sub(terminal_event_reserve),
        terminal_event_reserve,
        used_recovery_bytes,
        recovery_byte_limit: STATE_THREAD_RECOVERY_BYTE_LIMIT,
        remaining_recovery_bytes,
        general_recovery_bytes_remaining: remaining_recovery_bytes
            .saturating_sub(terminal_recovery_byte_reserve),
        terminal_recovery_byte_reserve,
        level,
    }
}

fn more_severe_capacity_level(
    left: StateCapacityLevel,
    right: StateCapacityLevel,
) -> StateCapacityLevel {
    fn rank(level: StateCapacityLevel) -> u8 {
        match level {
            StateCapacityLevel::Healthy => 0,
            StateCapacityLevel::Warning => 1,
            StateCapacityLevel::Critical => 2,
            StateCapacityLevel::TerminalOnly => 3,
            StateCapacityLevel::Exhausted => 4,
        }
    }
    if rank(left) >= rank(right) {
        left
    } else {
        right
    }
}

fn require_running_head(head: &StreamHead, turn_id: &TurnId) -> Result<(), HarnessError> {
    if !head.turns.contains(turn_id) {
        return Err(HarnessError::State(format!(
            "turn {turn_id} does not exist"
        )));
    }
    if head.running_turn.as_ref() != Some(turn_id) {
        return Err(HarnessError::State(format!(
            "turn {turn_id} is not running"
        )));
    }
    Ok(())
}

fn projected_turn<'a>(thread: &'a Thread, turn: &Turn) -> Result<&'a Turn, HarnessError> {
    if thread.id != turn.thread_id {
        return Err(HarnessError::State(format!(
            "turn {} does not belong to thread {}",
            turn.id, thread.id
        )));
    }
    thread
        .turns
        .iter()
        .find(|candidate| candidate.id == turn.id)
        .ok_or_else(|| HarnessError::State(format!("turn {} does not exist", turn.id)))
}

fn require_running_projection(turn: &Turn) -> Result<(), HarnessError> {
    if turn.status == TurnStatus::Running {
        Ok(())
    } else {
        Err(HarnessError::State(format!(
            "turn {} is not running",
            turn.id
        )))
    }
}

fn bounded_wait_ttl_ms(wait_ttl: Option<Duration>) -> Result<Option<u64>, HarnessError> {
    wait_ttl
        .map(|duration| {
            let milliseconds = u64::try_from(duration.as_millis()).map_err(|_| {
                HarnessError::State("Agent Loop wait duration exceeds u64".to_owned())
            })?;
            if !(1..=MAX_AGENT_LOOP_WAIT_MS).contains(&milliseconds) {
                return Err(HarnessError::State(format!(
                    "Agent Loop wait duration must be 1-{MAX_AGENT_LOOP_WAIT_MS} milliseconds"
                )));
            }
            Ok(milliseconds)
        })
        .transpose()
}

fn validate_remaining_active_timeout(value: Option<u64>) -> Result<(), HarnessError> {
    if value.is_some_and(|milliseconds| !(1..=MAX_AGENT_LOOP_WAIT_MS).contains(&milliseconds)) {
        return Err(HarnessError::State(format!(
            "remaining active timeout must be 1-{MAX_AGENT_LOOP_WAIT_MS} milliseconds"
        )));
    }
    Ok(())
}

fn validate_approval_request_shape(request: &crate::ApprovalRequest) -> Result<(), HarnessError> {
    validate_state_id("approval", request.id.as_str())?;
    request
        .requested_by
        .validate_current_state("State approval requester")?;
    validate_state_id("approval Thread", request.authorization.thread_id.as_str())?;
    validate_state_id("approval Turn", request.authorization.turn_id.as_str())?;
    validate_state_id("approval Tool call", &request.authorization.call_id)?;
    validate_capability_name("approval Tool", &request.authorization.descriptor.name)
        .map_err(|error| HarnessError::State(error.to_string()))?;
    crate::kernel::validate_capability_origin(&request.authorization.origin)
        .map_err(|error| HarnessError::State(error.to_string()))?;
    let descriptor = &request.authorization.descriptor;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > 65_536
        || descriptor.description.chars().any(char::is_control)
        || !descriptor.input_schema.is_object()
        || validate_value_shape(&descriptor.input_schema).is_err()
        || validate_value_shape(&request.authorization.input).is_err()
    {
        return Err(HarnessError::State(
            "approval Tool descriptor or input violates bounded State shape".to_owned(),
        ));
    }
    if request.reason.trim().is_empty()
        || request.reason.len() > 4_096
        || request.reason.chars().any(char::is_control)
    {
        return Err(HarnessError::State(
            "approval reason must be 1-4096 trimmed non-control bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_approval_wait_request(
    thread: &Thread,
    turn: &Turn,
    request: &crate::ApprovalRequest,
    model_request_sha256: &str,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    validate_approval_request_shape(request)?;
    if request.requested_by != *authority.actor()
        || request.authorization.thread_id != thread.id
        || request.authorization.turn_id != turn.id
        || !is_lower_sha256(model_request_sha256)
    {
        return Err(HarnessError::State(
            "approval wait requester, Turn coordinates, or Model request digest differ".to_owned(),
        ));
    }
    let call = turn.items.iter().find(|item| {
        matches!(
            &item.kind,
            ItemKind::ToolCall { call_id, .. } if call_id == &request.authorization.call_id
        )
    });
    let Some(Item {
        kind: ItemKind::ToolCall { name, input, .. },
        ..
    }) = call
    else {
        return Err(HarnessError::State(
            "approval wait has no matching durable ToolCall".to_owned(),
        ));
    };
    if name != &request.authorization.descriptor.name || input != &request.authorization.input {
        return Err(HarnessError::State(
            "approval request differs from its durable ToolCall".to_owned(),
        ));
    }
    let policy_matches = turn.items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::PolicyDecision {
                call_id,
                tool_origin: Some(tool_origin),
                decision: crate::PolicyDecision::Ask { reason, risk },
            } if call_id == &request.authorization.call_id
                && tool_origin == &request.authorization.origin
                && reason == &request.reason
                && risk == &request.risk
        )
    });
    if !policy_matches {
        return Err(HarnessError::State(
            "approval wait has no matching durable Ask PolicyDecision".to_owned(),
        ));
    }
    if turn.items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::ApprovalRequested { approval_id, .. } if approval_id == &request.id
        )
    }) {
        return Err(HarnessError::State(format!(
            "approval {} was already recorded",
            request.id
        )));
    }
    Ok(())
}

fn state_evidence_sha256<T: Serialize>(domain: &str, value: &T) -> Result<String, HarnessError> {
    bounded_serialized_sha256(&(domain, value), MAX_STATE_EVENT_BYTES)
        .map_err(|error| state_json_error("Agent Loop evidence", MAX_STATE_EVENT_BYTES, error))
}

#[derive(Clone, Copy)]
enum AgentLoopLifecycleEvent {
    WaitStarted,
    ResumeAccepted,
    WaitDenied,
    WaitClosed,
    ReadyClaimed,
}

impl AgentLoopLifecycleEvent {
    const fn domain(self) -> &'static str {
        match self {
            Self::WaitStarted => "y-harness.agent-loop.event.wait-started.v1",
            Self::ResumeAccepted => "y-harness.agent-loop.event.resume-accepted.v1",
            Self::WaitDenied => "y-harness.agent-loop.event.wait-denied.v1",
            Self::WaitClosed => "y-harness.agent-loop.event.wait-closed.v1",
            Self::ReadyClaimed => "y-harness.agent-loop.event.ready-claimed.v1",
        }
    }
}

/// Derives a globally safe idempotency identity for one durable-wait event.
///
/// Caller identities are bounded but opaque and may be reused in another
/// Thread. Hashing a domain-separated `(thread, turn, identity)` coordinate
/// keeps every Event identity at one fixed size without changing the command
/// or evidence digests that form the durable-wait wire contract.
fn agent_loop_lifecycle_event_id(
    event: AgentLoopLifecycleEvent,
    thread_id: &ThreadId,
    turn_id: &TurnId,
    stable_identity: &str,
) -> Result<EventId, HarnessError> {
    let digest = state_evidence_sha256(event.domain(), &(thread_id, turn_id, stable_identity))?;
    Ok(EventId::from_string(format!(
        "agent-loop-event-v1-{digest}"
    )))
}

fn agent_loop_maintenance_item_id(event_id: &EventId, role: &str) -> Result<ItemId, HarnessError> {
    let digest = state_evidence_sha256(
        "y-harness.agent-loop.maintenance-item.v1",
        &(event_id, role),
    )?;
    Ok(ItemId::from_string(format!(
        "agent-loop-maintenance-item-v1-{digest}"
    )))
}

pub(crate) fn agent_loop_due_command_id(due: &AgentLoopDueWait) -> Result<String, HarnessError> {
    match due.phase {
        AgentLoopDuePhase::Waiting | AgentLoopDuePhase::ReadyAllow => {
            deterministic_timeout_command_id(due).map(|command| command.as_str().to_owned())
        }
        AgentLoopDuePhase::ReadyDeny => {
            deterministic_denial_command_id(due).map(|command| command.as_str().to_owned())
        }
    }
}

fn wait_envelope_sha256(envelope: &TurnWaitEnvelope) -> Result<String, HarnessError> {
    let mut unsigned = envelope.clone();
    unsigned.envelope_sha256.clear();
    state_evidence_sha256("y-harness.agent-loop.wait-envelope.v1", &unsigned)
}

fn resume_command_sha256(
    thread_id: &ThreadId,
    turn_id: &TurnId,
    wait_id: &AgentLoopWaitId,
    expected_revision: u64,
    command_id: &AgentLoopResumeCommandId,
    settlement: &ApprovalSettlementEvidence,
    authority: &AuthorityContext,
) -> Result<String, HarnessError> {
    state_evidence_sha256(
        "y-harness.agent-loop.resume-command.v1",
        &(
            thread_id,
            turn_id,
            wait_id,
            expected_revision,
            command_id,
            settlement,
            authority,
        ),
    )
}

fn execution_claim_sha256(
    thread_id: &ThreadId,
    turn_id: &TurnId,
    command: &AgentLoopReadyClaimCommand,
    authority: &AuthorityContext,
) -> Result<String, HarnessError> {
    // Preserve the schema-16 canonical tuple shape. The command is only an API
    // parameter aggregate; it is deliberately not serialized as a struct.
    state_evidence_sha256(
        "y-harness.agent-loop.execution-claim.v1",
        &(
            thread_id,
            turn_id,
            &command.wait_id,
            command.expected_revision,
            &command.resume_command_id,
            &command.claim_id,
            &command.worker_id,
            authority,
        ),
    )
}

fn wait_close_command_sha256(
    thread_id: &ThreadId,
    turn_id: &TurnId,
    command: &AgentLoopWaitCloseCommand,
    authority: &AuthorityContext,
) -> Result<String, HarnessError> {
    // Preserve the schema-16 canonical tuple shape. The command is only an API
    // parameter aggregate; it is deliberately not serialized as a struct.
    state_evidence_sha256(
        "y-harness.agent-loop.wait-close-command.v1",
        &(
            thread_id,
            turn_id,
            &command.wait_id,
            command.expected_revision,
            &command.command_id,
            &command.status,
            command.reason,
            authority,
        ),
    )
}

fn wait_denial_command_sha256(
    thread_id: &ThreadId,
    turn_id: &TurnId,
    wait_id: &AgentLoopWaitId,
    expected_revision: u64,
    command_id: &AgentLoopDenyCommandId,
    settlement: &ApprovalSettlementEvidence,
    authority: &AuthorityContext,
) -> Result<String, HarnessError> {
    state_evidence_sha256(
        "y-harness.agent-loop.wait-denial-command.v1",
        &(
            thread_id,
            turn_id,
            wait_id,
            expected_revision,
            command_id,
            settlement,
            authority,
        ),
    )
}

fn validate_wait_envelope(envelope: &TurnWaitEnvelope) -> Result<(), HarnessError> {
    validate_state_id("Agent Loop wait", envelope.wait_id.as_str())?;
    validate_state_id("Agent Loop wait Thread", envelope.thread_id.as_str())?;
    validate_state_id("Agent Loop wait Turn", envelope.turn_id.as_str())?;
    if envelope.revision != 1 || envelope.server_started_at_ms == 0 {
        return Err(HarnessError::State(
            "new Agent Loop wait requires revision one and non-zero server time".to_owned(),
        ));
    }
    envelope
        .requested_by
        .validate_current_state("Agent Loop wait requester")?;
    if let Some(tenant_id) = &envelope.tenant_id {
        AuthorityContext::validate_tenant(tenant_id)
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    validate_remaining_active_timeout(envelope.remaining_active_timeout_ms)?;
    if let Some(expires_at_ms) = envelope.expires_at_ms {
        let lifetime = expires_at_ms
            .checked_sub(envelope.server_started_at_ms)
            .filter(|lifetime| (1..=MAX_AGENT_LOOP_WAIT_MS).contains(lifetime))
            .ok_or_else(|| {
                HarnessError::State(
                    "Agent Loop wait expiry violates its server-time bound".to_owned(),
                )
            })?;
        let _ = lifetime;
    }
    envelope
        .completion_generation
        .validate()
        .map_err(state_completion_error)?;
    match &envelope.wait_kind {
        WaitKind::Approval {
            request,
            model_request_sha256,
        } => {
            validate_approval_request_shape(request)?;
            if request.id.as_str().is_empty()
                || request.requested_by != envelope.requested_by
                || request.authorization.thread_id != envelope.thread_id
                || request.authorization.turn_id != envelope.turn_id
                || !is_lower_sha256(model_request_sha256)
                || model_request_sha256 != envelope.completion_generation.model_request_sha256()
            {
                return Err(HarnessError::State(
                    "Agent Loop wait Approval coordinates do not match its envelope".to_owned(),
                ));
            }
        }
    }
    if !is_lower_sha256(&envelope.envelope_sha256)
        || wait_envelope_sha256(envelope)? != envelope.envelope_sha256
    {
        return Err(HarnessError::State(
            "Agent Loop wait envelope digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn approval_settlement_evidence(
    record: &ApprovalRecord,
) -> Result<ApprovalSettlementEvidence, HarnessError> {
    bounded_serialized_size(record, crate::approval::MAX_APPROVAL_RECORD_BYTES).map_err(
        |error| {
            state_json_error(
                "Approval Inbox settlement",
                crate::approval::MAX_APPROVAL_RECORD_BYTES,
                error,
            )
        },
    )?;
    if record.schema_version != crate::APPROVAL_INBOX_SCHEMA_VERSION || record.revision < 2 {
        return Err(HarnessError::State(
            "resume requires a current, terminal Approval Inbox revision".to_owned(),
        ));
    }
    let ApprovalRecordStatus::Settled {
        decision,
        decided_by,
    } = &record.status
    else {
        return Err(HarnessError::State(
            "resume requires a settled Approval Inbox record".to_owned(),
        ));
    };
    let settled_at_ms = record.settled_at_ms.ok_or_else(|| {
        HarnessError::State("settled Approval Inbox record has no settlement time".to_owned())
    })?;
    let evidence = ApprovalSettlementEvidence {
        inbox_schema_version: record.schema_version,
        request: record.request.clone(),
        tenant_id: record.tenant_id().map(str::to_owned),
        decision: decision.clone(),
        decided_by: decided_by.clone(),
        inbox_revision: record.revision,
        requested_at_ms: record.requested_at_ms,
        settled_at_ms,
    };
    validate_approval_settlement_shape(&evidence)?;
    Ok(evidence)
}

fn approval_denial_settlement_evidence(
    record: &ApprovalRecord,
) -> Result<ApprovalSettlementEvidence, HarnessError> {
    let settlement = approval_settlement_evidence(record)?;
    if !matches!(&settlement.decision, crate::ApprovalDecision::Deny { .. }) {
        return Err(HarnessError::State(
            "atomic wait denial requires a settled Deny decision".to_owned(),
        ));
    }
    Ok(settlement)
}

fn validate_approval_settlement_shape(
    settlement: &ApprovalSettlementEvidence,
) -> Result<(), HarnessError> {
    bounded_serialized_size(settlement, crate::approval::MAX_APPROVAL_RECORD_BYTES).map_err(
        |error| {
            state_json_error(
                "Approval settlement evidence",
                crate::approval::MAX_APPROVAL_RECORD_BYTES,
                error,
            )
        },
    )?;
    validate_approval_request_shape(&settlement.request)?;
    settlement
        .decided_by
        .validate_current_state("Approval settlement actor")?;
    if let Some(tenant_id) = &settlement.tenant_id {
        AuthorityContext::validate_tenant(tenant_id)
            .map_err(|error| HarnessError::State(error.to_string()))?;
    }
    if settlement.inbox_schema_version != crate::APPROVAL_INBOX_SCHEMA_VERSION
        || settlement.inbox_revision < 2
        || settlement.requested_at_ms == 0
        || settlement.settled_at_ms < settlement.requested_at_ms
        || settlement.decided_by == settlement.request.requested_by
    {
        return Err(HarnessError::State(
            "Approval settlement evidence violates schema, revision, time, or actor separation"
                .to_owned(),
        ));
    }
    if let crate::ApprovalDecision::Deny { reason } = &settlement.decision
        && (reason.trim().is_empty()
            || reason.len() > 4_096
            || reason.chars().any(char::is_control))
    {
        return Err(HarnessError::State(
            "Approval denial reason must be 1-4096 trimmed non-control bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_approval_settlement(
    envelope: &TurnWaitEnvelope,
    settlement: &ApprovalSettlementEvidence,
) -> Result<(), HarnessError> {
    validate_approval_settlement_shape(settlement)?;
    let WaitKind::Approval { request, .. } = &envelope.wait_kind;
    if &settlement.request != request || settlement.tenant_id != envelope.tenant_id {
        return Err(HarnessError::State(
            "Approval Inbox settlement differs from the exact durable wait".to_owned(),
        ));
    }
    Ok(())
}

fn validate_wait_resume_authority(
    envelope: &TurnWaitEnvelope,
    wait_id: &AgentLoopWaitId,
    expected_revision: u64,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    if &envelope.wait_id != wait_id
        || envelope.revision != 1
        || expected_revision != envelope.revision
        || envelope.requested_by != *authority.actor()
        || envelope.tenant_id.as_deref() != authority.tenant_id()
    {
        return Err(HarnessError::State(
            "Agent Loop wait identity, revision, requester, or tenant differs".to_owned(),
        ));
    }
    Ok(())
}

fn validate_wait_current_authority(
    envelope: &TurnWaitEnvelope,
    wait_id: &AgentLoopWaitId,
    expected_revision: u64,
    current_revision: u64,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    if &envelope.wait_id != wait_id
        || expected_revision != current_revision
        || envelope.requested_by != *authority.actor()
        || envelope.tenant_id.as_deref() != authority.tenant_id()
    {
        return Err(HarnessError::State(
            "Agent Loop wait identity, current revision, requester, or tenant differs".to_owned(),
        ));
    }
    Ok(())
}

fn validate_wait_close_status(
    status: &TurnStatus,
    reason: TurnStopReason,
) -> Result<(), HarnessError> {
    if matches!(
        (status, reason),
        (TurnStatus::Cancelled, TurnStopReason::Cancelled)
            | (TurnStatus::TimedOut, TurnStopReason::TimedOut)
    ) {
        Ok(())
    } else {
        Err(HarnessError::State(
            "wait closure requires exactly Cancelled/cancelled or TimedOut/timed_out".to_owned(),
        ))
    }
}

fn validate_wait_not_expired(
    envelope: &TurnWaitEnvelope,
    server_now_ms: u64,
) -> Result<(), HarnessError> {
    if envelope
        .expires_at_ms
        .is_some_and(|expires_at_ms| server_now_ms >= expires_at_ms)
    {
        return Err(HarnessError::State(format!(
            "Agent Loop wait {} expired",
            envelope.wait_id
        )));
    }
    Ok(())
}

fn validate_resume_evidence(
    envelope: &TurnWaitEnvelope,
    evidence: &ResumeEvidence,
) -> Result<(), HarnessError> {
    validate_state_id("Agent Loop resume command", evidence.command_id.as_str())?;
    validate_approval_settlement(envelope, &evidence.settlement)?;
    if evidence.wait_id != envelope.wait_id
        || evidence.previous_revision != envelope.revision
        || evidence.revision != envelope.revision.saturating_add(1)
        || evidence.accepted_at_ms < envelope.server_started_at_ms
        || evidence.accepted_at_ms < evidence.settlement.settled_at_ms
        || !is_lower_sha256(&evidence.command_sha256)
    {
        return Err(HarnessError::State(
            "Agent Loop resume evidence violates wait revision, time, or digest shape".to_owned(),
        ));
    }
    let authority =
        AuthorityContext::new(envelope.requested_by.clone(), envelope.tenant_id.clone())
            .map_err(|error| HarnessError::State(error.to_string()))?;
    let expected = resume_command_sha256(
        &envelope.thread_id,
        &envelope.turn_id,
        &envelope.wait_id,
        evidence.previous_revision,
        &evidence.command_id,
        &evidence.settlement,
        &authority,
    )?;
    if expected != evidence.command_sha256 {
        return Err(HarnessError::State(
            "Agent Loop resume command digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn validate_wait_closure_evidence(
    envelope: &TurnWaitEnvelope,
    evidence: &WaitClosureEvidence,
) -> Result<(), HarnessError> {
    validate_state_id("Agent Loop close command", evidence.command_id.as_str())?;
    validate_wait_close_status(&evidence.status, evidence.reason)?;
    let next_revision = evidence
        .previous_revision
        .checked_add(1)
        .ok_or_else(|| HarnessError::State("Agent Loop closure revision overflow".to_owned()))?;
    if evidence.wait_id != envelope.wait_id
        || evidence.previous_revision == 0
        || evidence.revision != next_revision
        || evidence.closed_at_ms < envelope.server_started_at_ms
        || !is_lower_sha256(&evidence.command_sha256)
    {
        return Err(HarnessError::State(
            "Agent Loop closure evidence violates wait revision, time, or digest shape".to_owned(),
        ));
    }
    if evidence.status == TurnStatus::TimedOut
        && envelope
            .expires_at_ms
            .is_none_or(|expires_at_ms| evidence.closed_at_ms < expires_at_ms)
    {
        return Err(HarnessError::State(
            "TimedOut wait closure requires an elapsed server expiry".to_owned(),
        ));
    }
    let authority =
        AuthorityContext::new(envelope.requested_by.clone(), envelope.tenant_id.clone())
            .map_err(|error| HarnessError::State(error.to_string()))?;
    let command = AgentLoopWaitCloseCommand::new(
        envelope.wait_id.clone(),
        evidence.previous_revision,
        evidence.command_id.clone(),
        evidence.status.clone(),
        evidence.reason,
    );
    let expected =
        wait_close_command_sha256(&envelope.thread_id, &envelope.turn_id, &command, &authority)?;
    if expected != evidence.command_sha256 {
        return Err(HarnessError::State(
            "Agent Loop wait closure command digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn validate_wait_denial_evidence(
    envelope: &TurnWaitEnvelope,
    evidence: &WaitDenialEvidence,
) -> Result<(), HarnessError> {
    validate_state_id("Agent Loop denial command", evidence.command_id.as_str())?;
    validate_approval_settlement(envelope, &evidence.settlement)?;
    if !matches!(
        &evidence.settlement.decision,
        crate::ApprovalDecision::Deny { .. }
    ) {
        return Err(HarnessError::State(
            "Agent Loop denial evidence does not contain a Deny settlement".to_owned(),
        ));
    }
    let next_revision = evidence
        .previous_revision
        .checked_add(1)
        .ok_or_else(|| HarnessError::State("Agent Loop denial revision overflow".to_owned()))?;
    if evidence.wait_id != envelope.wait_id
        || evidence.previous_revision == 0
        || evidence.revision != next_revision
        || evidence.denied_at_ms < envelope.server_started_at_ms
        || evidence.denied_at_ms < evidence.settlement.settled_at_ms
        || !is_lower_sha256(&evidence.command_sha256)
    {
        return Err(HarnessError::State(
            "Agent Loop denial evidence violates wait revision, time, or digest shape".to_owned(),
        ));
    }
    let authority =
        AuthorityContext::new(envelope.requested_by.clone(), envelope.tenant_id.clone())
            .map_err(|error| HarnessError::State(error.to_string()))?;
    let expected = wait_denial_command_sha256(
        &envelope.thread_id,
        &envelope.turn_id,
        &envelope.wait_id,
        evidence.previous_revision,
        &evidence.command_id,
        &evidence.settlement,
        &authority,
    )?;
    if expected != evidence.command_sha256 {
        return Err(HarnessError::State(
            "Agent Loop wait denial command digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn validate_claim_evidence(
    envelope: &TurnWaitEnvelope,
    resume: &ResumeEvidence,
    evidence: &ExecutionClaimEvidence,
) -> Result<(), HarnessError> {
    validate_state_id("Agent Loop claim", evidence.claim_id.as_str())?;
    validate_state_id("Agent Loop worker", evidence.worker_id.as_str())?;
    if evidence.wait_id != envelope.wait_id
        || evidence.previous_revision != resume.revision
        || evidence.revision != resume.revision.saturating_add(1)
        || evidence.resume_command_id != resume.command_id
        || evidence.claimed_at_ms < resume.accepted_at_ms
        || !is_lower_sha256(&evidence.claim_sha256)
    {
        return Err(HarnessError::State(
            "Agent Loop claim evidence violates ready revision, worker, time, or digest shape"
                .to_owned(),
        ));
    }
    let authority =
        AuthorityContext::new(envelope.requested_by.clone(), envelope.tenant_id.clone())
            .map_err(|error| HarnessError::State(error.to_string()))?;
    let command = AgentLoopReadyClaimCommand::new(
        envelope.wait_id.clone(),
        evidence.previous_revision,
        evidence.resume_command_id.clone(),
        evidence.claim_id.clone(),
        evidence.worker_id.clone(),
    );
    let expected =
        execution_claim_sha256(&envelope.thread_id, &envelope.turn_id, &command, &authority)?;
    if expected != evidence.claim_sha256 {
        return Err(HarnessError::State(
            "Agent Loop execution claim digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn agent_loop_execution_projection(
    turn: &Turn,
) -> Result<Option<AgentLoopExecution>, HarnessError> {
    let mut execution = None;
    let mut claim_index = None;
    let mut closed = false;
    for (index, item) in turn.items.iter().enumerate() {
        if closed {
            return Err(HarnessError::State(
                "Turn history contains an Item after its Agent Loop wait terminal transition"
                    .to_owned(),
            ));
        }
        match &item.kind {
            ItemKind::AgentLoopWaitStarted { envelope } => {
                validate_wait_envelope(envelope)?;
                if envelope.turn_id != turn.id {
                    return Err(HarnessError::State(
                        "Agent Loop wait envelope Turn differs from its projection".to_owned(),
                    ));
                }
                let preceding = index
                    .checked_sub(1)
                    .and_then(|previous| turn.items.get(previous));
                if !preceding.is_some_and(|requested| {
                    approval_requested_matches_envelope(requested, envelope)
                }) {
                    return Err(HarnessError::State(
                        "Agent Loop wait transition lacks its adjacent ApprovalRequested evidence"
                            .to_owned(),
                    ));
                }
                if let Some(current) = &execution {
                    require_new_wait_boundary_at(turn, current, claim_index, index)?;
                }
                execution = Some(AgentLoopExecution::Waiting {
                    envelope: (**envelope).clone(),
                });
                claim_index = None;
            }
            ItemKind::AgentLoopResumeAccepted { evidence } => {
                let Some(AgentLoopExecution::Waiting { envelope }) = execution.take() else {
                    return Err(HarnessError::State(
                        "Agent Loop resume transition does not follow Waiting".to_owned(),
                    ));
                };
                validate_resume_evidence(&envelope, evidence)?;
                let preceding = index
                    .checked_sub(1)
                    .and_then(|previous| turn.items.get(previous));
                if !preceding
                    .is_some_and(|decision| approval_decision_matches_resume(decision, evidence))
                {
                    return Err(HarnessError::State(
                        "Agent Loop resume transition lacks its adjacent ApprovalDecision evidence"
                            .to_owned(),
                    ));
                }
                execution = Some(AgentLoopExecution::Ready {
                    envelope,
                    resume: (**evidence).clone(),
                });
            }
            ItemKind::AgentLoopReadyClaimed { evidence } => {
                let Some(AgentLoopExecution::Ready { envelope, resume }) = execution.take() else {
                    return Err(HarnessError::State(
                        "Agent Loop claim transition does not follow Ready".to_owned(),
                    ));
                };
                if matches!(
                    &resume.settlement.decision,
                    crate::ApprovalDecision::Deny { .. }
                ) {
                    return Err(HarnessError::State(
                        "an Agent Loop Ready state with an accepted Deny settlement cannot be claimed"
                            .to_owned(),
                    ));
                }
                validate_claim_evidence(&envelope, &resume, evidence)?;
                execution = Some(AgentLoopExecution::Executing {
                    envelope,
                    resume,
                    claim: (**evidence).clone(),
                });
                claim_index = Some(index);
            }
            ItemKind::AgentLoopWaitClosed { evidence } => {
                let Some(current) = execution.take() else {
                    return Err(HarnessError::State(
                        "Agent Loop closure transition has no live Waiting or Ready execution"
                            .to_owned(),
                    ));
                };
                let (envelope, current_revision, preceding_transition_ms) = match current {
                    AgentLoopExecution::Waiting { envelope } => {
                        let server_started_at_ms = envelope.server_started_at_ms;
                        let revision = envelope.revision;
                        (envelope, revision, server_started_at_ms)
                    }
                    AgentLoopExecution::Ready { envelope, resume } => {
                        if matches!(
                            &resume.settlement.decision,
                            crate::ApprovalDecision::Deny { .. }
                        ) {
                            return Err(HarnessError::State(
                                "an Agent Loop Ready state with an accepted Deny settlement cannot be closed"
                                    .to_owned(),
                            ));
                        }
                        let accepted_at_ms = resume.accepted_at_ms;
                        let revision = resume.revision;
                        (envelope, revision, accepted_at_ms)
                    }
                    AgentLoopExecution::Executing { .. } => {
                        return Err(HarnessError::State(
                            "Agent Loop closure cannot settle an Executing claim".to_owned(),
                        ));
                    }
                };
                if item.created_at_ms != evidence.closed_at_ms
                    || evidence.previous_revision != current_revision
                    || evidence.closed_at_ms < preceding_transition_ms
                {
                    return Err(HarnessError::State(
                        "Agent Loop closure does not consume the current wait revision and time"
                            .to_owned(),
                    ));
                }
                validate_wait_closure_evidence(&envelope, evidence)?;
                let preceding = index
                    .checked_sub(1)
                    .and_then(|previous| turn.items.get(previous));
                if !preceding.is_some_and(|stopped| turn_stopped_matches_closure(stopped, evidence))
                {
                    return Err(HarnessError::State(
                        "Agent Loop closure transition lacks its adjacent TurnStopped evidence"
                            .to_owned(),
                    ));
                }
                claim_index = None;
                closed = true;
            }
            ItemKind::AgentLoopWaitDenied { evidence } => {
                let Some(current) = execution.take() else {
                    return Err(HarnessError::State(
                        "Agent Loop denial transition has no live Waiting or Ready execution"
                            .to_owned(),
                    ));
                };
                let (envelope, current_revision, preceding_transition_ms) = match current {
                    AgentLoopExecution::Waiting { envelope } => {
                        let server_started_at_ms = envelope.server_started_at_ms;
                        let revision = envelope.revision;
                        (envelope, revision, server_started_at_ms)
                    }
                    AgentLoopExecution::Ready { envelope, resume } => {
                        if resume.settlement != evidence.settlement {
                            return Err(HarnessError::State(
                                "Agent Loop denial differs from the accepted Ready settlement"
                                    .to_owned(),
                            ));
                        }
                        let accepted_at_ms = resume.accepted_at_ms;
                        let revision = resume.revision;
                        (envelope, revision, accepted_at_ms)
                    }
                    AgentLoopExecution::Executing { .. } => {
                        return Err(HarnessError::State(
                            "Agent Loop denial cannot settle an Executing claim".to_owned(),
                        ));
                    }
                };
                if item.created_at_ms != evidence.denied_at_ms
                    || evidence.previous_revision != current_revision
                    || evidence.denied_at_ms < preceding_transition_ms
                {
                    return Err(HarnessError::State(
                        "Agent Loop denial does not consume the current wait revision and time"
                            .to_owned(),
                    ));
                }
                validate_wait_denial_evidence(&envelope, evidence)?;
                let preceding = index
                    .checked_sub(1)
                    .and_then(|previous| turn.items.get(previous));
                if !preceding
                    .is_some_and(|decision| approval_decision_matches_denial(decision, evidence))
                {
                    return Err(HarnessError::State(
                        "Agent Loop denial transition lacks its adjacent ApprovalDecision evidence"
                            .to_owned(),
                    ));
                }
                claim_index = None;
                closed = true;
            }
            _ => {}
        }
    }
    Ok(execution)
}

fn approval_requested_matches_envelope(item: &Item, envelope: &TurnWaitEnvelope) -> bool {
    let WaitKind::Approval {
        request,
        model_request_sha256,
    } = &envelope.wait_kind;
    matches!(
        &item.kind,
        ItemKind::ApprovalRequested {
            approval_id,
            call_id,
            tool,
            reason,
            risk,
            requested_by: Some(requested_by),
            tool_origin: Some(tool_origin),
            model_request_sha256: Some(recorded_model_request_sha256),
        } if approval_id == &request.id
            && call_id == &request.authorization.call_id
            && tool == &request.authorization.descriptor.name
            && reason == &request.reason
            && risk == &request.risk
            && requested_by == &request.requested_by
            && tool_origin == &request.authorization.origin
            && recorded_model_request_sha256 == model_request_sha256
    )
}

fn approval_decision_matches_resume(item: &Item, evidence: &ResumeEvidence) -> bool {
    matches!(
        &item.kind,
        ItemKind::ApprovalDecision {
            approval_id,
            call_id,
            decision,
        } if approval_id == &evidence.settlement.request.id
            && call_id == &evidence.settlement.request.authorization.call_id
            && decision == &evidence.settlement.decision
    )
}

fn approval_decision_matches_denial(item: &Item, evidence: &WaitDenialEvidence) -> bool {
    matches!(
        &item.kind,
        ItemKind::ApprovalDecision {
            approval_id,
            call_id,
            decision,
        } if approval_id == &evidence.settlement.request.id
            && call_id == &evidence.settlement.request.authorization.call_id
            && decision == &evidence.settlement.decision
            && matches!(decision, crate::ApprovalDecision::Deny { .. })
    )
}

fn require_new_wait_boundary(
    turn: &Turn,
    execution: &AgentLoopExecution,
) -> Result<(), HarnessError> {
    let claim_index = turn.items.iter().rposition(|item| {
        matches!(
            (&item.kind, execution),
            (
                ItemKind::AgentLoopReadyClaimed { evidence },
                AgentLoopExecution::Executing { claim, .. }
            ) if evidence.claim_id == claim.claim_id
        )
    });
    require_new_wait_boundary_at(turn, execution, claim_index, turn.items.len())
}

fn require_new_wait_boundary_at(
    turn: &Turn,
    execution: &AgentLoopExecution,
    claim_index: Option<usize>,
    before_index: usize,
) -> Result<(), HarnessError> {
    let AgentLoopExecution::Executing { envelope, .. } = execution else {
        return Err(HarnessError::State(format!(
            "turn {} already has an open Waiting or Ready execution",
            turn.id
        )));
    };
    let WaitKind::Approval { request, .. } = &envelope.wait_kind;
    let settled = claim_index.is_some_and(|claim_index| {
        turn.items[claim_index.saturating_add(1)..before_index]
            .iter()
            .any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::ToolResult { call_id, .. }
                        if call_id == &request.authorization.call_id
                )
            })
    });
    if settled {
        Ok(())
    } else {
        Err(HarnessError::State(
            "claimed Agent Loop execution has no authoritative ToolResult; unknown effect cannot be replayed"
                .to_owned(),
        ))
    }
}

fn wait_start_matches(
    envelope: &TurnWaitEnvelope,
    request: &crate::ApprovalRequest,
    generation: &CompletionGeneration,
    wait_ttl_ms: Option<u64>,
    remaining_active_timeout_ms: Option<u64>,
    authority: &AuthorityContext,
) -> bool {
    let lifetime = envelope
        .expires_at_ms
        .and_then(|expiry| expiry.checked_sub(envelope.server_started_at_ms));
    matches!(
        &envelope.wait_kind,
        WaitKind::Approval { request: recorded, model_request_sha256 }
            if recorded == request
                && model_request_sha256 == generation.model_request_sha256()
    ) && envelope.completion_generation == *generation
        && envelope.tenant_id.as_deref() == authority.tenant_id()
        && envelope.requested_by == *authority.actor()
        && lifetime == wait_ttl_ms
        && envelope.remaining_active_timeout_ms == remaining_active_timeout_ms
}

fn wait_envelope_for_id(
    turn: &Turn,
    wait_id: &AgentLoopWaitId,
) -> Result<Option<TurnWaitEnvelope>, HarnessError> {
    let mut found = None;
    for item in &turn.items {
        let ItemKind::AgentLoopWaitStarted { envelope } = &item.kind else {
            continue;
        };
        if &envelope.wait_id != wait_id {
            continue;
        }
        validate_wait_envelope(envelope)?;
        if found.replace((**envelope).clone()).is_some() {
            return Err(HarnessError::State(format!(
                "turn {} contains duplicate Agent Loop wait {wait_id}",
                turn.id
            )));
        }
    }
    Ok(found)
}

fn wait_close_retry_matches(
    turn: &Turn,
    command_id: &AgentLoopCloseCommandId,
    command_sha256: &str,
    status: &TurnStatus,
    reason: TurnStopReason,
) -> Result<bool, HarnessError> {
    let mut matched = false;
    for evidence in turn.items.iter().filter_map(|item| {
        if let ItemKind::AgentLoopWaitClosed { evidence } = &item.kind {
            Some(evidence.as_ref())
        } else {
            None
        }
    }) {
        if &evidence.command_id != command_id {
            continue;
        }
        if evidence.command_sha256 != command_sha256
            || &evidence.status != status
            || evidence.reason != reason
        {
            return Err(HarnessError::State(format!(
                "Agent Loop close command {command_id} was reused with different content"
            )));
        }
        matched = true;
    }
    Ok(matched)
}

fn wait_denial_retry_matches(
    turn: &Turn,
    command_id: &AgentLoopDenyCommandId,
    command_sha256: &str,
    settlement: &ApprovalSettlementEvidence,
) -> Result<bool, HarnessError> {
    let mut matched = false;
    for evidence in turn.items.iter().filter_map(|item| {
        if let ItemKind::AgentLoopWaitDenied { evidence } = &item.kind {
            Some(evidence.as_ref())
        } else {
            None
        }
    }) {
        if &evidence.command_id != command_id {
            continue;
        }
        if evidence.command_sha256 != command_sha256 || &evidence.settlement != settlement {
            return Err(HarnessError::State(format!(
                "Agent Loop denial command {command_id} was reused with different content"
            )));
        }
        matched = true;
    }
    Ok(matched)
}

fn resume_retry_matches(
    execution: &AgentLoopExecution,
    command_id: &AgentLoopResumeCommandId,
    command_sha256: &str,
) -> bool {
    match execution {
        AgentLoopExecution::Ready { resume, .. } | AgentLoopExecution::Executing { resume, .. } => {
            &resume.command_id == command_id && resume.command_sha256 == command_sha256
        }
        AgentLoopExecution::Waiting { .. } => false,
    }
}

fn claim_retry_matches(
    execution: &AgentLoopExecution,
    claim_id: &AgentLoopClaimId,
    claim_sha256: &str,
) -> bool {
    matches!(
        execution,
        AgentLoopExecution::Executing { claim, .. }
            if &claim.claim_id == claim_id && claim.claim_sha256 == claim_sha256
    )
}

fn turn_stopped_matches_closure(item: &Item, evidence: &WaitClosureEvidence) -> bool {
    item.created_at_ms == evidence.closed_at_ms
        && matches!(
            &item.kind,
            ItemKind::TurnStopped { reason, phase }
                if *reason == evidence.reason && *phase == ExecutionPhase::Approval
        )
}

fn project_events(events: &[StoredEvent]) -> Result<Option<Thread>, HarnessError> {
    let mut thread: Option<Thread> = None;
    apply_events(&mut thread, events)?;
    Ok(thread)
}

fn apply_events(thread: &mut Option<Thread>, events: &[StoredEvent]) -> Result<(), HarnessError> {
    let mut turn_ids = thread
        .iter()
        .flat_map(|thread| thread.turns.iter())
        .map(|turn| turn.id.clone())
        .collect::<BTreeSet<_>>();
    let mut item_ids = thread
        .iter()
        .flat_map(|thread| thread.turns.iter())
        .flat_map(|turn| turn.items.iter())
        .map(|item| item.id.clone())
        .collect::<BTreeSet<_>>();
    let mut checkpoint_ids = thread
        .iter()
        .flat_map(|thread| thread.checkpoints.iter())
        .map(|checkpoint| checkpoint.id.clone())
        .collect::<BTreeSet<_>>();
    for stored in events {
        validate_stored_schema(stored)?;
        match &stored.event {
            StateEvent::ThreadCreated {
                created_at_ms,
                tenant_id,
            } => {
                if thread.is_some() {
                    return Err(HarnessError::State(
                        "thread has multiple creation events".to_owned(),
                    ));
                }
                if let Some(tenant_id) = tenant_id {
                    AuthorityContext::validate_tenant(tenant_id)
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                }
                *thread = Some(Thread {
                    id: stored.thread_id.clone(),
                    tenant_id: tenant_id.clone(),
                    name: None,
                    lineage: None,
                    import_origin: None,
                    created_at_ms: *created_at_ms,
                    turns: Vec::new(),
                    checkpoints: Vec::new(),
                });
            }
            StateEvent::ThreadNamed { name } => {
                validate_thread_name(name.as_deref())?;
                projection_thread(thread)?.name.clone_from(name);
            }
            StateEvent::ThreadForked { lineage } => {
                validate_thread_lineage(lineage)?;
                if lineage.parent_thread_id == stored.thread_id {
                    return Err(HarnessError::State(
                        "Thread cannot be forked from itself".to_owned(),
                    ));
                }
                let thread = projection_thread(thread)?;
                if thread.lineage.is_some()
                    || thread.import_origin.is_some()
                    || thread.name.is_some()
                    || !thread.turns.is_empty()
                    || !thread.checkpoints.is_empty()
                {
                    return Err(HarnessError::State(
                        "Thread fork lineage must immediately follow creation".to_owned(),
                    ));
                }
                thread.lineage = Some(lineage.clone());
            }
            StateEvent::ThreadImported { origin } => {
                validate_thread_import_origin(origin)?;
                let thread = projection_thread(thread)?;
                if thread.lineage.is_some()
                    || thread.import_origin.is_some()
                    || thread.name.is_some()
                    || !thread.turns.is_empty()
                    || !thread.checkpoints.is_empty()
                {
                    return Err(HarnessError::State(
                        "Thread import provenance must immediately follow creation".to_owned(),
                    ));
                }
                thread.import_origin = Some(origin.clone());
            }
            StateEvent::TurnStarted { turn_id } => {
                if !turn_ids.insert(turn_id.clone()) {
                    return Err(HarnessError::State(format!("duplicate turn {turn_id}")));
                }
                let thread = projection_thread(thread)?;
                if thread
                    .turns
                    .iter()
                    .any(|turn| turn.status == TurnStatus::Running)
                {
                    return Err(HarnessError::State(
                        "thread contains overlapping running turns".to_owned(),
                    ));
                }
                thread.turns.push(Turn {
                    id: turn_id.clone(),
                    thread_id: stored.thread_id.clone(),
                    status: TurnStatus::Running,
                    completion_receipt: None,
                    items: Vec::new(),
                });
            }
            StateEvent::ItemAppended { turn_id, item } => {
                append_projected_items(thread, turn_id, std::slice::from_ref(item), &mut item_ids)?;
            }
            StateEvent::ToolCallsAppended { turn_id, calls } => {
                append_projected_items(thread, turn_id, calls, &mut item_ids)?;
            }
            StateEvent::WaitStarted {
                turn_id,
                approval_requested,
                transition,
            } => {
                append_projected_items(
                    thread,
                    turn_id,
                    &[approval_requested.clone(), transition.clone()],
                    &mut item_ids,
                )?;
                let turn = projection_thread(thread)?
                    .turns
                    .iter()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State("wait references unknown Turn".to_owned())
                    })?;
                let _ = agent_loop_execution_projection(turn)?;
            }
            StateEvent::AcceptResume {
                turn_id,
                approval_decision,
                transition,
            } => {
                append_projected_items(
                    thread,
                    turn_id,
                    &[approval_decision.clone(), transition.clone()],
                    &mut item_ids,
                )?;
                let turn = projection_thread(thread)?
                    .turns
                    .iter()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State("resume references unknown Turn".to_owned())
                    })?;
                let _ = agent_loop_execution_projection(turn)?;
            }
            StateEvent::ClaimReady {
                turn_id,
                transition,
            } => {
                append_projected_items(
                    thread,
                    turn_id,
                    std::slice::from_ref(transition),
                    &mut item_ids,
                )?;
                let turn = projection_thread(thread)?
                    .turns
                    .iter()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State("claim references unknown Turn".to_owned())
                    })?;
                let _ = agent_loop_execution_projection(turn)?;
            }
            StateEvent::WaitClosed {
                turn_id,
                stopped,
                transition,
                status,
            } => {
                append_projected_items(
                    thread,
                    turn_id,
                    &[stopped.clone(), transition.clone()],
                    &mut item_ids,
                )?;
                let thread = projection_thread(thread)?;
                let turn = thread
                    .turns
                    .iter_mut()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State(format!(
                            "wait closure references unknown turn {turn_id}"
                        ))
                    })?;
                if agent_loop_execution_projection(turn)?.is_some() {
                    return Err(HarnessError::State(format!(
                        "wait closure did not close turn {turn_id} execution"
                    )));
                }
                turn.status = status.clone();
            }
            StateEvent::DenyWait {
                turn_id,
                approval_decision,
                transition,
            } => {
                append_projected_items(
                    thread,
                    turn_id,
                    &[approval_decision.clone(), transition.clone()],
                    &mut item_ids,
                )?;
                let thread = projection_thread(thread)?;
                let turn = thread
                    .turns
                    .iter_mut()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State(format!(
                            "wait denial references unknown turn {turn_id}"
                        ))
                    })?;
                if agent_loop_execution_projection(turn)?.is_some() {
                    return Err(HarnessError::State(format!(
                        "wait denial did not close turn {turn_id} execution"
                    )));
                }
                turn.status = TurnStatus::Failed;
            }
            StateEvent::TurnFinished { turn_id, status } => {
                if status == &TurnStatus::Running {
                    return Err(HarnessError::State(
                        "turn finish event contains running status".to_owned(),
                    ));
                }
                let thread = projection_thread(thread)?;
                let turn = thread
                    .turns
                    .iter_mut()
                    .find(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State(format!("finish references unknown turn {turn_id}"))
                    })?;
                if turn.status != TurnStatus::Running {
                    return Err(HarnessError::State(format!(
                        "turn {turn_id} has multiple terminal events"
                    )));
                }
                if matches!(
                    agent_loop_execution_projection(turn)?,
                    Some(AgentLoopExecution::Waiting { .. } | AgentLoopExecution::Ready { .. })
                ) {
                    return Err(HarnessError::State(format!(
                        "cannot finish turn {turn_id} with an open Waiting or Ready execution"
                    )));
                }
                turn.status = status.clone();
            }
            StateEvent::TurnCompleted { turn_id, receipt } => {
                let thread = projection_thread(thread)?;
                let turn_index = thread
                    .turns
                    .iter()
                    .position(|turn| &turn.id == turn_id)
                    .ok_or_else(|| {
                        HarnessError::State(format!("completion references unknown turn {turn_id}"))
                    })?;
                let turn = &thread.turns[turn_index];
                if turn.status != TurnStatus::Running {
                    return Err(HarnessError::State(format!(
                        "turn {turn_id} has multiple terminal events"
                    )));
                }
                if has_pending_steering(turn)? {
                    return Err(HarnessError::State(format!(
                        "cannot complete turn {turn_id} with unapplied steering"
                    )));
                }
                if matches!(
                    agent_loop_execution_projection(turn)?,
                    Some(AgentLoopExecution::Waiting { .. } | AgentLoopExecution::Ready { .. })
                ) {
                    return Err(HarnessError::State(format!(
                        "cannot complete turn {turn_id} with an open Waiting or Ready execution"
                    )));
                }
                let mut completed = turn.clone();
                completed.completion_receipt = Some(receipt.clone());
                completed.status = TurnStatus::Completed;
                validate_projected_completion_placement(thread, &completed, receipt)?;
                thread.turns[turn_index] = completed;
            }
            StateEvent::CheckpointCreated { checkpoint } => {
                if !checkpoint_ids.insert(checkpoint.id.clone()) {
                    return Err(HarnessError::State(format!(
                        "duplicate checkpoint {}",
                        checkpoint.id
                    )));
                }
                let thread = projection_thread(thread)?;
                if checkpoint.thread_id != stored.thread_id {
                    return Err(HarnessError::State(
                        "checkpoint thread does not match event thread".to_owned(),
                    ));
                }
                if checkpoint.target_sequence >= stored.sequence {
                    return Err(HarnessError::State(
                        "checkpoint target must precede its event".to_owned(),
                    ));
                }
                thread.checkpoints.push(checkpoint.clone());
            }
        }
    }
    if let Some(thread) = thread {
        for turn in &thread.turns {
            if let Some(receipt) = &turn.completion_receipt {
                validate_projected_completion_placement(thread, turn, receipt)?;
            }
        }
        validate_execution_binding_projection(thread)?;
        validate_connector_evidence_projection(thread)?;
        validate_steering_projection(thread)?;
        validate_tool_call_batch_projection(thread)?;
        validate_agent_loop_execution_projection(thread)?;
    }
    Ok(())
}

fn validate_agent_loop_execution_projection(thread: &Thread) -> Result<(), HarnessError> {
    for turn in &thread.turns {
        let _ = agent_loop_execution_projection(turn)?;
        for envelope in turn.items.iter().filter_map(|item| {
            if let ItemKind::AgentLoopWaitStarted { envelope } = &item.kind {
                Some(envelope.as_ref())
            } else {
                None
            }
        }) {
            if envelope.tenant_id.as_deref() != thread.tenant_id() {
                return Err(HarnessError::State(format!(
                    "turn {} Agent Loop wait tenant differs from its Thread",
                    turn.id
                )));
            }
            let inherited_terminal = turn.status != TurnStatus::Running
                && (thread.lineage.is_some() || thread.import_origin.is_some());
            if envelope.thread_id != thread.id && !inherited_terminal {
                return Err(HarnessError::State(format!(
                    "turn {} Agent Loop wait Thread differs without terminal materialization provenance",
                    turn.id
                )));
            }
        }
    }
    Ok(())
}

fn append_projected_items(
    thread: &mut Option<Thread>,
    turn_id: &TurnId,
    items: &[Item],
    item_ids: &mut BTreeSet<ItemId>,
) -> Result<(), HarnessError> {
    if let Some(duplicate) = items.iter().find(|item| !item_ids.insert(item.id.clone())) {
        return Err(HarnessError::State(format!(
            "duplicate item {}",
            duplicate.id
        )));
    }
    let turn = projection_thread(thread)?
        .turns
        .iter_mut()
        .find(|turn| &turn.id == turn_id)
        .ok_or_else(|| HarnessError::State(format!("item references unknown turn {turn_id}")))?;
    if turn.status != TurnStatus::Running {
        return Err(HarnessError::State(format!(
            "item appended after turn {turn_id} finished"
        )));
    }
    turn.items.extend_from_slice(items);
    Ok(())
}

fn projection_thread(thread: &mut Option<Thread>) -> Result<&mut Thread, HarnessError> {
    thread
        .as_mut()
        .ok_or_else(|| HarnessError::State("event precedes thread creation".to_owned()))
}

fn validate_thread_authority(
    thread: &Thread,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority.validate_current("Thread access authority")?;
    if thread.tenant_id() == authority.tenant_id() {
        Ok(())
    } else {
        Err(HarnessError::State(
            "Event Store tenant projection differs from authoritative Thread creation".to_owned(),
        ))
    }
}

fn validate_projected_completion_placement(
    thread: &Thread,
    turn: &Turn,
    receipt: &CompletionReceipt,
) -> Result<(), HarnessError> {
    if receipt.source_thread_id() == &turn.thread_id {
        return validate_projected_turn_completion_receipt(turn, thread.tenant_id(), receipt)
            .map_err(state_completion_error);
    }
    if thread.lineage.is_none() && thread.import_origin.is_none() {
        return Err(HarnessError::State(format!(
            "turn {} contains a completion receipt from another Thread without fork or import provenance",
            turn.id
        )));
    }
    validate_inherited_projected_turn_completion_receipt(turn, thread.tenant_id(), receipt)
        .map_err(state_completion_error)
}

fn state_completion_error(error: HarnessError) -> HarnessError {
    match error {
        error @ HarnessError::State(_) => error,
        error => HarnessError::State(error.to_string()),
    }
}

fn validate_stream_version(actual: u64, pending: &PendingEvent) -> Result<(), HarnessError> {
    if actual == pending.expected_stream_version {
        Ok(())
    } else {
        Err(HarnessError::StateConflict {
            thread_id: pending.thread_id.clone(),
            expected: pending.expected_stream_version,
            actual,
        })
    }
}

fn validate_stream_recovery_bytes(actual: u64, pending: &PendingEvent) -> Result<(), HarnessError> {
    if actual == pending.expected_stream_recovery_bytes {
        Ok(())
    } else {
        Err(HarnessError::State(format!(
            "stream recovery charge mismatch: expected {}, found {actual}",
            pending.expected_stream_recovery_bytes
        )))
    }
}

fn projection_sha256(
    thread: &Thread,
    metadata_events: &[StateEvent],
) -> Result<String, HarnessError> {
    struct DigestWriter<'a>(&'a mut Sha256);

    impl Write for DigestWriter<'_> {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.update(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let mut digest = Sha256::new();
    serde_json::to_writer(DigestWriter(&mut digest), &(thread, metadata_events))
        .map_err(|error| HarnessError::State(format!("cannot encode State projection: {error}")))?;
    let digest = digest.finalize();
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn encode_snapshot(snapshot: &StateSnapshot) -> Result<String, HarnessError> {
    validate_snapshot(snapshot)?;
    let encoded = to_bounded_json_vec(snapshot, MAX_STATE_SNAPSHOT_BYTES)
        .map_err(|error| state_json_error("State snapshot", MAX_STATE_SNAPSHOT_BYTES, error))?;
    String::from_utf8(encoded)
        .map_err(|_| HarnessError::State("State snapshot is not UTF-8 JSON".to_owned()))
}

fn validate_snapshot(snapshot: &StateSnapshot) -> Result<(), HarnessError> {
    if snapshot.schema_version != STATE_SNAPSHOT_SCHEMA_VERSION {
        return Err(HarnessError::State(format!(
            "unsupported State snapshot schema version {}",
            snapshot.schema_version
        )));
    }
    if snapshot.through_sequence == 0
        || snapshot.stream_version == 0
        || snapshot.stream_version > MAX_STATE_RECOVERY_EVENTS as u64
        || snapshot.stream_version > snapshot.through_sequence
        || snapshot.recovery_bytes == 0
        || snapshot.recovery_bytes > STATE_THREAD_RECOVERY_BYTE_LIMIT
    {
        return Err(HarnessError::State(
            "State snapshot has invalid sequence metadata".to_owned(),
        ));
    }
    validate_state_id("snapshot thread", snapshot.thread.id.as_str())?;
    validate_state_id("snapshot anchor event", snapshot.anchor_event_id.as_str())?;
    let projected_recovery_bytes = validate_projected_thread(&snapshot.thread, snapshot)?;
    if projected_recovery_bytes != snapshot.recovery_bytes {
        return Err(HarnessError::State(
            "State snapshot recovery charge does not match its projection".to_owned(),
        ));
    }
    let digest = projection_sha256(&snapshot.thread, &snapshot.metadata_events)?;
    if snapshot.projection_sha256 != digest {
        return Err(HarnessError::State(
            "State snapshot projection digest mismatch".to_owned(),
        ));
    }
    bounded_serialized_size(snapshot, MAX_STATE_SNAPSHOT_BYTES)
        .map_err(|error| state_json_error("State snapshot", MAX_STATE_SNAPSHOT_BYTES, error))?;
    Ok(())
}

fn validate_projected_thread(
    thread: &Thread,
    snapshot: &StateSnapshot,
) -> Result<u64, HarnessError> {
    let mut turn_ids = BTreeSet::new();
    let mut item_ids = BTreeSet::new();
    let mut running_turns = 0_usize;
    let mut represented_events = 1_usize;

    let mut recovery_bytes = encode_state_event(&StateEvent::ThreadCreated {
        created_at_ms: thread.created_at_ms,
        tenant_id: thread.tenant_id().map(str::to_owned),
    })?
    .recovery_bytes;
    if let Some(lineage) = &thread.lineage {
        validate_thread_lineage(lineage)?;
        if lineage.parent_thread_id == thread.id {
            return Err(HarnessError::State(
                "State snapshot contains self-referential Thread lineage".to_owned(),
            ));
        }
        represented_events = represented_events
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
        add_recovery_bytes(
            &mut recovery_bytes,
            encode_state_event(&StateEvent::ThreadForked {
                lineage: lineage.clone(),
            })?
            .recovery_bytes,
        )?;
    }
    if let Some(origin) = &thread.import_origin {
        if thread.lineage.is_some() {
            return Err(HarnessError::State(
                "State snapshot cannot contain both fork and import provenance".to_owned(),
            ));
        }
        validate_thread_import_origin(origin)?;
        represented_events = represented_events
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
        add_recovery_bytes(
            &mut recovery_bytes,
            encode_state_event(&StateEvent::ThreadImported {
                origin: origin.clone(),
            })?
            .recovery_bytes,
        )?;
    }
    let mut projected_name = None;
    for event in &snapshot.metadata_events {
        let StateEvent::ThreadNamed { name } = event else {
            return Err(HarnessError::State(
                "State snapshot contains a non-metadata event".to_owned(),
            ));
        };
        validate_state_event(event)?;
        validate_state_event_schema(event, STATE_EVENT_SCHEMA_VERSION)?;
        projected_name.clone_from(name);
        represented_events = represented_events
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
        add_recovery_bytes(
            &mut recovery_bytes,
            encode_state_event(event)?.recovery_bytes,
        )?;
    }
    if projected_name != thread.name {
        return Err(HarnessError::State(
            "State snapshot Thread name does not match its metadata events".to_owned(),
        ));
    }
    for turn in &thread.turns {
        validate_state_id("snapshot turn", turn.id.as_str())?;
        if turn.thread_id != thread.id || !turn_ids.insert(turn.id.as_str()) {
            return Err(HarnessError::State(
                "State snapshot contains an invalid or duplicate Turn".to_owned(),
            ));
        }
        represented_events = represented_events
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
        add_recovery_bytes(
            &mut recovery_bytes,
            encode_state_event(&StateEvent::TurnStarted {
                turn_id: turn.id.clone(),
            })?
            .recovery_bytes,
        )?;

        let mut wait_closed_status = None;
        let mut item_index = 0;
        while item_index < turn.items.len() {
            let item = &turn.items[item_index];
            let item_count = match &item.kind {
                ItemKind::ToolCall {
                    batch: Some(batch), ..
                } if batch.index == 0 => batch.size,
                ItemKind::ToolCall { batch: Some(_), .. } => {
                    return Err(HarnessError::State(
                        "State snapshot starts inside a Tool-call batch".to_owned(),
                    ));
                }
                ItemKind::ApprovalRequested { .. }
                    if turn
                        .items
                        .get(item_index.saturating_add(1))
                        .is_some_and(|next| {
                            matches!(
                                &next.kind,
                                ItemKind::AgentLoopWaitStarted { envelope }
                                    if approval_requested_matches_envelope(item, envelope)
                            )
                        }) =>
                {
                    2
                }
                ItemKind::ApprovalDecision { .. }
                    if turn
                        .items
                        .get(item_index.saturating_add(1))
                        .is_some_and(|next| {
                            matches!(
                                &next.kind,
                                ItemKind::AgentLoopResumeAccepted { evidence }
                                    if approval_decision_matches_resume(item, evidence)
                            )
                        }) =>
                {
                    2
                }
                ItemKind::ApprovalDecision { .. }
                    if turn
                        .items
                        .get(item_index.saturating_add(1))
                        .is_some_and(|next| {
                            matches!(
                                &next.kind,
                                ItemKind::AgentLoopWaitDenied { evidence }
                                    if approval_decision_matches_denial(item, evidence)
                            )
                        }) =>
                {
                    2
                }
                ItemKind::TurnStopped { .. }
                    if turn
                        .items
                        .get(item_index.saturating_add(1))
                        .is_some_and(|next| {
                            matches!(
                                &next.kind,
                                ItemKind::AgentLoopWaitClosed { evidence }
                                    if turn_stopped_matches_closure(item, evidence)
                            )
                        }) =>
                {
                    2
                }
                ItemKind::AgentLoopWaitStarted { .. }
                | ItemKind::AgentLoopResumeAccepted { .. }
                | ItemKind::AgentLoopWaitClosed { .. }
                | ItemKind::AgentLoopWaitDenied { .. } => {
                    return Err(HarnessError::State(
                        "State snapshot starts inside an Agent Loop compound transition".to_owned(),
                    ));
                }
                _ => 1,
            };
            let batch_end = item_index
                .checked_add(item_count)
                .filter(|end| *end <= turn.items.len())
                .ok_or_else(|| {
                    HarnessError::State(
                        "State snapshot contains a truncated Tool-call batch".to_owned(),
                    )
                })?;
            let items = &turn.items[item_index..batch_end];
            if matches!(&item.kind, ItemKind::ToolCall { batch: Some(_), .. }) {
                validate_tool_call_batch_items(items)?;
            }
            for item in items {
                validate_state_id("snapshot item", item.id.as_str())?;
                validate_state_item(item)?;
                if !item_ids.insert(item.id.as_str()) {
                    return Err(HarnessError::State(
                        "State snapshot contains a duplicate Item".to_owned(),
                    ));
                }
            }
            represented_events = represented_events
                .checked_add(1)
                .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
            let event = match (&item.kind, items) {
                (ItemKind::ToolCall { batch: Some(_), .. }, _) => StateEvent::ToolCallsAppended {
                    turn_id: turn.id.clone(),
                    calls: items.to_vec(),
                },
                (ItemKind::ApprovalRequested { .. }, [approval_requested, transition])
                    if matches!(&transition.kind, ItemKind::AgentLoopWaitStarted { .. }) =>
                {
                    StateEvent::WaitStarted {
                        turn_id: turn.id.clone(),
                        approval_requested: approval_requested.clone(),
                        transition: transition.clone(),
                    }
                }
                (ItemKind::ApprovalDecision { .. }, [approval_decision, transition])
                    if matches!(&transition.kind, ItemKind::AgentLoopResumeAccepted { .. }) =>
                {
                    StateEvent::AcceptResume {
                        turn_id: turn.id.clone(),
                        approval_decision: approval_decision.clone(),
                        transition: transition.clone(),
                    }
                }
                (ItemKind::ApprovalDecision { .. }, [approval_decision, transition])
                    if matches!(&transition.kind, ItemKind::AgentLoopWaitDenied { .. }) =>
                {
                    if batch_end != turn.items.len() {
                        return Err(HarnessError::State(
                            "State snapshot contains Items after an Agent Loop wait denial"
                                .to_owned(),
                        ));
                    }
                    if wait_closed_status.replace(TurnStatus::Failed).is_some() {
                        return Err(HarnessError::State(
                            "State snapshot contains multiple Agent Loop wait terminal events"
                                .to_owned(),
                        ));
                    }
                    StateEvent::DenyWait {
                        turn_id: turn.id.clone(),
                        approval_decision: approval_decision.clone(),
                        transition: transition.clone(),
                    }
                }
                (ItemKind::AgentLoopReadyClaimed { .. }, [transition]) => StateEvent::ClaimReady {
                    turn_id: turn.id.clone(),
                    transition: transition.clone(),
                },
                (ItemKind::TurnStopped { .. }, [stopped, transition])
                    if matches!(&transition.kind, ItemKind::AgentLoopWaitClosed { .. }) =>
                {
                    let ItemKind::AgentLoopWaitClosed { evidence } = &transition.kind else {
                        return Err(HarnessError::State(
                            "State snapshot closure guard lost its transition evidence".to_owned(),
                        ));
                    };
                    if batch_end != turn.items.len() {
                        return Err(HarnessError::State(
                            "State snapshot contains Items after an Agent Loop wait closure"
                                .to_owned(),
                        ));
                    }
                    if wait_closed_status
                        .replace(evidence.status.clone())
                        .is_some()
                    {
                        return Err(HarnessError::State(
                            "State snapshot contains multiple Agent Loop wait closures".to_owned(),
                        ));
                    }
                    StateEvent::WaitClosed {
                        turn_id: turn.id.clone(),
                        stopped: stopped.clone(),
                        transition: transition.clone(),
                        status: evidence.status.clone(),
                    }
                }
                (_, [item]) => StateEvent::ItemAppended {
                    turn_id: turn.id.clone(),
                    item: item.clone(),
                },
                _ => {
                    return Err(HarnessError::State(
                        "State snapshot contains an invalid compound Item sequence".to_owned(),
                    ));
                }
            };
            validate_state_event(&event)?;
            add_recovery_bytes(
                &mut recovery_bytes,
                encode_state_event(&event)?.recovery_bytes,
            )?;
            item_index = batch_end;
        }

        if let Some(closed_status) = wait_closed_status {
            if turn.status != closed_status || turn.completion_receipt.is_some() {
                return Err(HarnessError::State(
                    "State snapshot wait closure differs from its terminal Turn".to_owned(),
                ));
            }
            continue;
        }

        match (&turn.status, &turn.completion_receipt) {
            (TurnStatus::Running, None) => {
                running_turns = running_turns
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::State("running Turn count overflow".to_owned()))?;
            }
            (TurnStatus::Running, Some(_)) => {
                return Err(HarnessError::State(
                    "State snapshot contains a receipt on a running Turn".to_owned(),
                ));
            }
            (TurnStatus::Completed, Some(receipt)) => {
                validate_projected_completion_placement(thread, turn, receipt)?;
                represented_events = represented_events.checked_add(1).ok_or_else(|| {
                    HarnessError::State("snapshot event count overflow".to_owned())
                })?;
                add_recovery_bytes(
                    &mut recovery_bytes,
                    encode_state_event(&StateEvent::TurnCompleted {
                        turn_id: turn.id.clone(),
                        receipt: receipt.clone(),
                    })?
                    .recovery_bytes,
                )?;
            }
            (TurnStatus::Completed, None) => {
                // Supported legacy State schemas wrote receipt-free completion.
                represented_events = represented_events.checked_add(1).ok_or_else(|| {
                    HarnessError::State("snapshot event count overflow".to_owned())
                })?;
                add_recovery_bytes(
                    &mut recovery_bytes,
                    encode_state_event(&StateEvent::TurnFinished {
                        turn_id: turn.id.clone(),
                        status: TurnStatus::Completed,
                    })?
                    .recovery_bytes,
                )?;
            }
            (_, Some(_)) => {
                return Err(HarnessError::State(
                    "State snapshot contains a receipt on a non-completed Turn".to_owned(),
                ));
            }
            (status, None) => {
                represented_events = represented_events.checked_add(1).ok_or_else(|| {
                    HarnessError::State("snapshot event count overflow".to_owned())
                })?;
                add_recovery_bytes(
                    &mut recovery_bytes,
                    encode_state_event(&StateEvent::TurnFinished {
                        turn_id: turn.id.clone(),
                        status: status.clone(),
                    })?
                    .recovery_bytes,
                )?;
            }
        }
    }
    if running_turns > 1 {
        return Err(HarnessError::State(
            "State snapshot contains overlapping running Turns".to_owned(),
        ));
    }
    validate_steering_projection(thread)?;
    validate_tool_call_batch_projection(thread)?;
    validate_execution_binding_projection(thread)?;
    validate_connector_evidence_projection(thread)?;
    validate_agent_loop_execution_projection(thread)?;

    let mut checkpoint_ids = BTreeSet::new();
    for checkpoint in &thread.checkpoints {
        if checkpoint.thread_id != thread.id
            || checkpoint.target_sequence >= snapshot.through_sequence
            || !checkpoint_ids.insert(checkpoint.id.as_str())
            || checkpoint
                .turn_id
                .as_ref()
                .is_some_and(|turn_id| !turn_ids.contains(turn_id.as_str()))
        {
            return Err(HarnessError::State(
                "State snapshot contains an invalid Checkpoint".to_owned(),
            ));
        }
        represented_events = represented_events
            .checked_add(1)
            .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
        add_recovery_bytes(
            &mut recovery_bytes,
            encode_state_event(&StateEvent::CheckpointCreated {
                checkpoint: checkpoint.clone(),
            })?
            .recovery_bytes,
        )?;
    }

    if u64::try_from(represented_events)
        .map_err(|_| HarnessError::State("snapshot event count exceeds u64".to_owned()))?
        != snapshot.stream_version
    {
        return Err(HarnessError::State(
            "State snapshot projection does not match its stream version".to_owned(),
        ));
    }
    Ok(recovery_bytes)
}

struct EncodedStateEvent {
    json: String,
    recovery_bytes: u64,
}

fn validate_pending_event(pending: &PendingEvent) -> Result<EncodedStateEvent, HarnessError> {
    validate_state_id("event", pending.event_id.as_str())?;
    validate_state_id("thread", pending.thread_id.as_str())?;
    if pending.expected_stream_version >= MAX_STATE_RECOVERY_EVENTS as u64 {
        return Err(HarnessError::State(format!(
            "Thread reached its {MAX_STATE_RECOVERY_EVENTS}-event retention boundary"
        )));
    }
    if pending.expected_stream_version
        >= STATE_THREAD_EVENT_LIMIT.saturating_sub(STATE_TERMINAL_EVENT_RESERVE)
        && !matches!(
            &pending.event,
            StateEvent::TurnFinished { .. }
                | StateEvent::TurnCompleted { .. }
                | StateEvent::WaitClosed { .. }
                | StateEvent::DenyWait { .. }
        )
    {
        return Err(HarnessError::State(
            "Thread has only its terminal-settlement event reserve remaining".to_owned(),
        ));
    }
    validate_state_event(&pending.event)?;
    validate_state_event_schema(&pending.event, STATE_EVENT_SCHEMA_VERSION)?;
    let encoded = encode_state_event(&pending.event)?;
    let next_recovery_bytes = pending
        .expected_stream_recovery_bytes
        .checked_add(encoded.recovery_bytes)
        .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))?;
    let limit = if matches!(
        &pending.event,
        StateEvent::TurnFinished { .. }
            | StateEvent::TurnCompleted { .. }
            | StateEvent::WaitClosed { .. }
            | StateEvent::DenyWait { .. }
    ) {
        STATE_THREAD_RECOVERY_BYTE_LIMIT
    } else {
        STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(STATE_TERMINAL_RECOVERY_BYTE_RESERVE)
    };
    if next_recovery_bytes > limit {
        return Err(HarnessError::State(format!(
            "Thread reached its {STATE_THREAD_RECOVERY_BYTE_LIMIT}-byte recovery boundary"
        )));
    }
    Ok(encoded)
}

fn validate_new_stream(
    thread_id: &ThreadId,
    events: &[NewStreamEvent],
) -> Result<Vec<EncodedStateEvent>, HarnessError> {
    validate_state_id("thread", thread_id.as_str())?;
    if events.len() < 2
        || !matches!(
            events.first().map(|event| &event.event),
            Some(StateEvent::ThreadCreated { .. })
        )
        || !matches!(
            events.get(1).map(|event| &event.event),
            Some(StateEvent::ThreadForked { .. } | StateEvent::ThreadImported { .. })
        )
    {
        return Err(HarnessError::State(
            "atomic materialized stream must begin with creation then provenance".to_owned(),
        ));
    }
    let stream_version = u64::try_from(events.len())
        .map_err(|_| HarnessError::State("stream version exceeds u64".to_owned()))?;
    if stream_version > STATE_THREAD_EVENT_LIMIT.saturating_sub(STATE_TERMINAL_EVENT_RESERVE) {
        return Err(HarnessError::State(format!(
            "materialized Thread exceeds its {}-event general boundary",
            STATE_THREAD_EVENT_LIMIT.saturating_sub(STATE_TERMINAL_EVENT_RESERVE)
        )));
    }

    let mut event_ids = BTreeSet::new();
    let mut encoded = Vec::with_capacity(events.len());
    let mut recovery_bytes = 0_u64;
    let mut synthetic = Vec::with_capacity(events.len());
    for (index, new) in events.iter().enumerate() {
        validate_state_id("event", new.event_id.as_str())?;
        if !event_ids.insert(new.event_id.as_str()) {
            return Err(HarnessError::State(
                "atomic stream contains duplicate Event identities".to_owned(),
            ));
        }
        if !(1..=STATE_EVENT_SCHEMA_VERSION).contains(&new.schema_version) {
            return Err(HarnessError::State(format!(
                "unsupported State event schema {}",
                new.schema_version
            )));
        }
        if matches!(new.event, StateEvent::CheckpointCreated { .. }) {
            return Err(HarnessError::State(
                "materialized history cannot copy recovery Checkpoints".to_owned(),
            ));
        }
        validate_state_event(&new.event)?;
        validate_state_event_schema(&new.event, new.schema_version)?;
        let event = encode_state_event(&new.event)?;
        recovery_bytes = recovery_bytes
            .checked_add(event.recovery_bytes)
            .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))?;
        encoded.push(event);
        synthetic.push(StoredEvent {
            schema_version: new.schema_version,
            sequence: u64::try_from(index + 1)
                .map_err(|_| HarnessError::State("synthetic sequence exceeds u64".to_owned()))?,
            event_id: new.event_id.clone(),
            thread_id: thread_id.clone(),
            recorded_at_ms: new.recorded_at_ms,
            event: new.event.clone(),
        });
    }
    if recovery_bytes
        > STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(STATE_TERMINAL_RECOVERY_BYTE_RESERVE)
    {
        return Err(HarnessError::State(format!(
            "materialized Thread exceeds its {}-byte general recovery boundary",
            STATE_THREAD_RECOVERY_BYTE_LIMIT.saturating_sub(STATE_TERMINAL_RECOVERY_BYTE_RESERVE)
        )));
    }
    let projected = project_events(&synthetic)?
        .ok_or_else(|| HarnessError::State("atomic stream has no Thread projection".to_owned()))?;
    if &projected.id != thread_id
        || projected.lineage.is_some() == projected.import_origin.is_some()
    {
        return Err(HarnessError::State(
            "atomic materialized stream projection is inconsistent".to_owned(),
        ));
    }
    if projected
        .turns
        .iter()
        .any(|turn| turn.status == TurnStatus::Running)
    {
        return Err(HarnessError::State(
            "atomic materialized stream cannot end with a running Turn".to_owned(),
        ));
    }
    Ok(encoded)
}

fn final_stream_name(events: &[StoredEvent]) -> Option<String> {
    events
        .iter()
        .filter_map(|stored| match &stored.event {
            StateEvent::ThreadNamed { name } => Some(name.clone()),
            _ => None,
        })
        .next_back()
        .flatten()
}

fn final_stream_lineage(events: &[StoredEvent]) -> Option<ThreadLineage> {
    events.iter().find_map(|stored| match &stored.event {
        StateEvent::ThreadForked { lineage } => Some(lineage.clone()),
        _ => None,
    })
}

fn final_stream_tenant(events: &[StoredEvent]) -> Result<Option<String>, HarnessError> {
    match events.first().map(|stored| &stored.event) {
        Some(StateEvent::ThreadCreated { tenant_id, .. }) => Ok(tenant_id.clone()),
        _ => Err(HarnessError::State(
            "Thread stream does not begin with creation".to_owned(),
        )),
    }
}

fn final_new_stream_tenant(events: &[NewStreamEvent]) -> Result<Option<String>, HarnessError> {
    match events.first().map(|new| &new.event) {
        Some(StateEvent::ThreadCreated { tenant_id, .. }) => Ok(tenant_id.clone()),
        _ => Err(HarnessError::State(
            "Thread stream does not begin with creation".to_owned(),
        )),
    }
}

fn validate_state_event(event: &StateEvent) -> Result<(), HarnessError> {
    match event {
        StateEvent::ThreadCreated { tenant_id, .. } => {
            if let Some(tenant_id) = tenant_id {
                AuthorityContext::validate_tenant(tenant_id)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
            }
        }
        StateEvent::ThreadNamed { name } => validate_thread_name(name.as_deref())?,
        StateEvent::ThreadForked { lineage } => validate_thread_lineage(lineage)?,
        StateEvent::ThreadImported { origin } => validate_thread_import_origin(origin)?,
        StateEvent::TurnStarted { turn_id } | StateEvent::TurnFinished { turn_id, .. } => {
            validate_state_id("turn", turn_id.as_str())?;
        }
        StateEvent::TurnCompleted { turn_id, receipt } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id(
                "completion receipt source thread",
                receipt.source_thread_id().as_str(),
            )?;
            validate_state_id("completion receipt turn", receipt.turn_id().as_str())?;
            if receipt.turn_id() != turn_id {
                return Err(HarnessError::State(
                    "completion event Turn differs from its receipt".to_owned(),
                ));
            }
            let _ = completion_receipt_sha256(receipt).map_err(state_completion_error)?;
        }
        StateEvent::ItemAppended { turn_id, item } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", item.id.as_str())?;
            validate_state_item(item)?;
        }
        StateEvent::ToolCallsAppended { turn_id, calls } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_tool_call_batch_items(calls)?;
        }
        StateEvent::WaitStarted {
            turn_id,
            approval_requested,
            transition,
        } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", approval_requested.id.as_str())?;
            validate_state_id("item", transition.id.as_str())?;
            validate_state_item(approval_requested)?;
            validate_state_item(transition)?;
            let ItemKind::AgentLoopWaitStarted { envelope } = &transition.kind else {
                return Err(HarnessError::State(
                    "WaitStarted transition has the wrong Item kind".to_owned(),
                ));
            };
            if approval_requested.id == transition.id
                || approval_requested.created_at_ms != transition.created_at_ms
                || &envelope.turn_id != turn_id
                || !approval_requested_matches_envelope(approval_requested, envelope)
            {
                return Err(HarnessError::State(
                    "WaitStarted items or envelope correlation differ".to_owned(),
                ));
            }
        }
        StateEvent::AcceptResume {
            turn_id,
            approval_decision,
            transition,
        } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", approval_decision.id.as_str())?;
            validate_state_id("item", transition.id.as_str())?;
            validate_state_item(approval_decision)?;
            validate_state_item(transition)?;
            let ItemKind::AgentLoopResumeAccepted { evidence } = &transition.kind else {
                return Err(HarnessError::State(
                    "ResumeAccepted transition has the wrong Item kind".to_owned(),
                ));
            };
            if approval_decision.id == transition.id
                || approval_decision.created_at_ms != transition.created_at_ms
                || !approval_decision_matches_resume(approval_decision, evidence)
            {
                return Err(HarnessError::State(
                    "ResumeAccepted items or settlement correlation differ".to_owned(),
                ));
            }
        }
        StateEvent::ClaimReady {
            turn_id,
            transition,
        } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", transition.id.as_str())?;
            validate_state_item(transition)?;
            if !matches!(&transition.kind, ItemKind::AgentLoopReadyClaimed { .. }) {
                return Err(HarnessError::State(
                    "ReadyClaimed transition has the wrong Item kind".to_owned(),
                ));
            }
        }
        StateEvent::WaitClosed {
            turn_id,
            stopped,
            transition,
            status,
        } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", stopped.id.as_str())?;
            validate_state_id("item", transition.id.as_str())?;
            validate_state_item(stopped)?;
            validate_state_item(transition)?;
            let ItemKind::AgentLoopWaitClosed { evidence } = &transition.kind else {
                return Err(HarnessError::State(
                    "WaitClosed transition has the wrong Item kind".to_owned(),
                ));
            };
            validate_wait_close_status(status, evidence.reason)?;
            if stopped.id == transition.id
                || stopped.created_at_ms != transition.created_at_ms
                || status != &evidence.status
                || !turn_stopped_matches_closure(stopped, evidence)
            {
                return Err(HarnessError::State(
                    "WaitClosed items, reason, status, or time correlation differ".to_owned(),
                ));
            }
        }
        StateEvent::DenyWait {
            turn_id,
            approval_decision,
            transition,
        } => {
            validate_state_id("turn", turn_id.as_str())?;
            validate_state_id("item", approval_decision.id.as_str())?;
            validate_state_id("item", transition.id.as_str())?;
            validate_state_item(approval_decision)?;
            validate_state_item(transition)?;
            let ItemKind::AgentLoopWaitDenied { evidence } = &transition.kind else {
                return Err(HarnessError::State(
                    "WaitDenied transition has the wrong Item kind".to_owned(),
                ));
            };
            if approval_decision.id == transition.id
                || approval_decision.created_at_ms != transition.created_at_ms
                || approval_decision.created_at_ms != evidence.denied_at_ms
                || !approval_decision_matches_denial(approval_decision, evidence)
            {
                return Err(HarnessError::State(
                    "WaitDenied items, decision, or time correlation differ".to_owned(),
                ));
            }
        }
        StateEvent::CheckpointCreated { checkpoint } => {
            validate_state_id("checkpoint", checkpoint.id.as_str())?;
            validate_state_id("checkpoint thread", checkpoint.thread_id.as_str())?;
            if let Some(turn_id) = &checkpoint.turn_id {
                validate_state_id("checkpoint turn", turn_id.as_str())?;
            }
            validate_checkpoint_label(checkpoint.label.as_deref())?;
        }
    }
    Ok(())
}

fn validate_thread_lineage(lineage: &ThreadLineage) -> Result<(), HarnessError> {
    validate_state_id("parent thread", lineage.parent_thread_id.as_str())?;
    if lineage.parent_through_sequence == 0
        || lineage.parent_stream_version == 0
        || lineage.parent_stream_version > lineage.parent_through_sequence
        || !is_lower_sha256(&lineage.parent_events_sha256)
    {
        return Err(HarnessError::State(
            "Thread lineage contains an invalid boundary or SHA-256".to_owned(),
        ));
    }
    Ok(())
}

fn validate_thread_import_origin(origin: &ThreadImportOrigin) -> Result<(), HarnessError> {
    validate_state_id("source thread", origin.source_thread_id.as_str())?;
    if origin.source_stream_version == 0
        || origin.source_last_sequence == 0
        || origin.source_stream_version > origin.source_last_sequence
        || !is_lower_sha256(&origin.source_events_sha256)
    {
        return Err(HarnessError::State(
            "Thread import origin contains an invalid boundary or SHA-256".to_owned(),
        ));
    }
    if let Some(lineage) = &origin.source_lineage {
        validate_thread_lineage(lineage)?;
    }
    Ok(())
}

fn validate_tool_call_batch_items(calls: &[Item]) -> Result<(), HarnessError> {
    if !(2..=crate::MAX_TOOL_CALLS_PER_BATCH).contains(&calls.len()) {
        return Err(HarnessError::State(format!(
            "Tool-call batch must contain 2-{} calls",
            crate::MAX_TOOL_CALLS_PER_BATCH
        )));
    }
    let mut batch_id = None;
    let mut call_ids = BTreeSet::new();
    for (index, item) in calls.iter().enumerate() {
        validate_state_id("item", item.id.as_str())?;
        validate_state_item(item)?;
        let ItemKind::ToolCall {
            call_id,
            batch: Some(batch),
            ..
        } = &item.kind
        else {
            return Err(HarnessError::State(
                "Tool-call batch contains a non-batched Item".to_owned(),
            ));
        };
        validate_state_id("Tool-call batch", batch.id.as_str())?;
        if batch.index != index
            || batch.size != calls.len()
            || *batch_id.get_or_insert(&batch.id) != &batch.id
            || !call_ids.insert(call_id)
        {
            return Err(HarnessError::State(
                "Tool-call batch order, size, identity, or correlation is inconsistent".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_checkpoint_label(label: Option<&str>) -> Result<(), HarnessError> {
    if let Some(label) = label
        && (label.trim().is_empty() || label.len() > MAX_CHECKPOINT_LABEL_BYTES)
    {
        return Err(HarnessError::State(format!(
            "checkpoint label must be 1-{MAX_CHECKPOINT_LABEL_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_thread_name(name: Option<&str>) -> Result<(), HarnessError> {
    if let Some(name) = name
        && (name.is_empty()
            || name.len() > MAX_THREAD_NAME_BYTES
            || name.trim() != name
            || name.chars().any(char::is_control))
    {
        return Err(HarnessError::State(format!(
            "Thread name must be 1-{MAX_THREAD_NAME_BYTES} trimmed non-control bytes"
        )));
    }
    Ok(())
}

fn validate_state_item(item: &Item) -> Result<(), HarnessError> {
    match &item.kind {
        crate::ItemKind::AssistantMessage {
            model_id,
            model_origin,
            model_request_sha256,
            ..
        } => match (model_id, model_origin, model_request_sha256) {
            (Some(model_id), Some(model_origin), model_request_sha256) => {
                crate::kernel::validate_model_id(model_id)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                crate::kernel::validate_capability_origin(model_origin)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                if model_request_sha256
                    .as_ref()
                    .is_some_and(|digest| !is_lower_sha256(digest))
                {
                    return Err(HarnessError::State(
                        "assistant candidate requires a lowercase Model request SHA-256".to_owned(),
                    ));
                }
            }
            (None, None, None) => {}
            _ => {
                return Err(HarnessError::State(
                    "assistant candidate attribution must be wholly present or legacy-compatible"
                        .to_owned(),
                ));
            }
        },
        crate::ItemKind::VerificationResult {
            verifier,
            candidate_item_id,
            verifier_origin,
            verifier_binding_sha256,
            outcome,
        } => {
            validate_capability_name("verification result", verifier)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            crate::verification::validate_outcome(verifier, outcome)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            match (candidate_item_id, verifier_origin, verifier_binding_sha256) {
                (Some(candidate_item_id), Some(verifier_origin), Some(binding_sha256)) => {
                    validate_state_id("verification candidate item", candidate_item_id.as_str())?;
                    crate::kernel::validate_capability_origin(verifier_origin)
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                    if !is_lower_sha256(binding_sha256) {
                        return Err(HarnessError::State(
                            "verification result requires a lowercase binding SHA-256".to_owned(),
                        ));
                    }
                }
                (None, None, None) => {}
                _ => {
                    return Err(HarnessError::State(
                        "verification result binding evidence must be wholly present or absent"
                            .to_owned(),
                    ));
                }
            }
        }
        crate::ItemKind::ExecutionBinding { bound_by, binding } => {
            bound_by.validate_current_state("State execution binding actor")?;
            binding
                .validate()
                .map_err(|error| HarnessError::State(error.to_string()))?;
        }
        crate::ItemKind::SteeringQueued {
            steering_id,
            submitted_by,
            content,
        } => {
            validate_state_id("steering", steering_id.as_str())?;
            submitted_by.validate_current_state("State steering actor")?;
            validate_steering_content(content)?;
        }
        crate::ItemKind::SteeringApplied {
            steering_id,
            content,
        } => {
            validate_state_id("steering", steering_id.as_str())?;
            validate_steering_content(content)?;
        }
        crate::ItemKind::ProviderContinuation {
            model_id,
            model_origin,
            continuation,
        } => {
            crate::kernel::validate_model_id(model_id)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            crate::kernel::validate_capability_origin(model_origin)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            continuation
                .validate()
                .map_err(|error| HarnessError::State(error.to_string()))?;
        }
        crate::ItemKind::ToolResult {
            output,
            is_error,
            connector_evidence,
            ..
        } => {
            validate_connector_evidence_records(output, *is_error, connector_evidence)?;
        }
        crate::ItemKind::ConversationSummary {
            compactor,
            covered_turns,
            older_omitted_turns,
            source_sha256,
            content_sha256,
            estimated_tokens,
            serialized_bytes,
        } => {
            validate_capability_name("conversation compactor", compactor)
                .map_err(|error| HarnessError::State(error.to_string()))?;
            if covered_turns.is_empty() || covered_turns.len() > 256 {
                return Err(HarnessError::State(
                    "conversation summary evidence must cover 1-256 Turns".to_owned(),
                ));
            }
            let mut seen = BTreeSet::new();
            for turn_id in covered_turns {
                validate_state_id("conversation summary Turn", turn_id.as_str())?;
                if !seen.insert(turn_id.as_str()) {
                    return Err(HarnessError::State(
                        "conversation summary evidence contains duplicate Turns".to_owned(),
                    ));
                }
            }
            if *older_omitted_turns > MAX_STATE_RECOVERY_EVENTS
                || !is_lower_sha256(source_sha256)
                || !is_lower_sha256(content_sha256)
                || !(1..=1_048_576).contains(estimated_tokens)
                || !(1..=1_048_576).contains(serialized_bytes)
            {
                return Err(HarnessError::State(
                    "conversation summary evidence violates bounded provenance".to_owned(),
                ));
            }
        }
        crate::ItemKind::InvocationContext {
            submitted_by,
            blocks,
        } => {
            submitted_by.validate_current_state("State Turn context submitter")?;
            if blocks.is_empty() || blocks.len() > MAX_INVOCATION_CONTEXT_BLOCKS {
                return Err(HarnessError::State(format!(
                    "Turn context evidence must contain 1-{MAX_INVOCATION_CONTEXT_BLOCKS} blocks"
                )));
            }
            let mut seen = BTreeSet::new();
            let mut total_bytes = 0_usize;
            let mut total_tokens = 0_usize;
            for block in blocks {
                validate_capability_name("Turn context source", &block.source)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                if block.reference.trim().is_empty()
                    || block.reference.len() > MAX_INVOCATION_CONTEXT_REFERENCE_BYTES
                    || block.reference.chars().any(char::is_control)
                    || !seen.insert((block.source.as_str(), block.reference.as_str()))
                {
                    return Err(HarnessError::State(
                        "Turn context evidence contains an invalid or duplicate source reference"
                            .to_owned(),
                    ));
                }
                if !is_lower_sha256(&block.source_sha256)
                    || !is_lower_sha256(&block.content_sha256)
                    || !(1..=MAX_INVOCATION_CONTEXT_BLOCK_BYTES).contains(&block.estimated_tokens)
                    || !(1..=MAX_INVOCATION_CONTEXT_BLOCK_BYTES).contains(&block.serialized_bytes)
                {
                    return Err(HarnessError::State(
                        "Turn context evidence violates bounded provenance".to_owned(),
                    ));
                }
                total_bytes = total_bytes
                    .checked_add(block.serialized_bytes)
                    .ok_or_else(|| {
                        HarnessError::State("Turn context evidence size overflow".to_owned())
                    })?;
                total_tokens = total_tokens
                    .checked_add(block.estimated_tokens)
                    .ok_or_else(|| {
                        HarnessError::State("Turn context evidence token overflow".to_owned())
                    })?;
            }
            if total_bytes > MAX_INVOCATION_CONTEXT_TOTAL_BYTES
                || total_tokens > MAX_INVOCATION_CONTEXT_BLOCK_BYTES
            {
                return Err(HarnessError::State(
                    "Turn context evidence exceeds its aggregate byte or token bound".to_owned(),
                ));
            }
        }
        crate::ItemKind::PolicyDecision {
            tool_origin: Some(tool_origin),
            ..
        } => {
            crate::kernel::validate_capability_origin(tool_origin)
                .map_err(|error| HarnessError::State(error.to_string()))?;
        }
        crate::ItemKind::ApprovalRequested {
            requested_by,
            tool_origin,
            model_request_sha256,
            ..
        } => match (requested_by, tool_origin, model_request_sha256) {
            (Some(requested_by), Some(tool_origin), Some(model_request_sha256)) => {
                requested_by
                    .validate_current("State approval requester")
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                crate::kernel::validate_capability_origin(tool_origin)
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                if !is_lower_sha256(model_request_sha256) {
                    return Err(HarnessError::State(
                        "approval continuation requires a lowercase Model request SHA-256"
                            .to_owned(),
                    ));
                }
            }
            (None, None, None) => {}
            _ => {
                return Err(HarnessError::State(
                    "approval continuation evidence must be wholly present or absent".to_owned(),
                ));
            }
        },
        crate::ItemKind::AgentLoopWaitStarted { envelope } => {
            validate_wait_envelope(envelope)?;
        }
        crate::ItemKind::AgentLoopResumeAccepted { evidence } => {
            validate_state_id("Agent Loop wait", evidence.wait_id.as_str())?;
            validate_state_id("Agent Loop resume command", evidence.command_id.as_str())?;
            validate_approval_settlement_shape(&evidence.settlement)?;
            if evidence.previous_revision == 0
                || evidence.revision != evidence.previous_revision.saturating_add(1)
                || evidence.accepted_at_ms == 0
                || !is_lower_sha256(&evidence.command_sha256)
            {
                return Err(HarnessError::State(
                    "Agent Loop resume transition has invalid revision, time, or digest".to_owned(),
                ));
            }
        }
        crate::ItemKind::AgentLoopReadyClaimed { evidence } => {
            validate_state_id("Agent Loop wait", evidence.wait_id.as_str())?;
            validate_state_id(
                "Agent Loop resume command",
                evidence.resume_command_id.as_str(),
            )?;
            validate_state_id("Agent Loop claim", evidence.claim_id.as_str())?;
            validate_state_id("Agent Loop worker", evidence.worker_id.as_str())?;
            if evidence.previous_revision == 0
                || evidence.revision != evidence.previous_revision.saturating_add(1)
                || evidence.claimed_at_ms == 0
                || !is_lower_sha256(&evidence.claim_sha256)
            {
                return Err(HarnessError::State(
                    "Agent Loop claim transition has invalid revision, time, or digest".to_owned(),
                ));
            }
        }
        crate::ItemKind::AgentLoopWaitClosed { evidence } => {
            validate_state_id("Agent Loop wait", evidence.wait_id.as_str())?;
            validate_state_id("Agent Loop close command", evidence.command_id.as_str())?;
            validate_wait_close_status(&evidence.status, evidence.reason)?;
            let next_revision = evidence.previous_revision.checked_add(1).ok_or_else(|| {
                HarnessError::State("Agent Loop closure revision overflow".to_owned())
            })?;
            if evidence.previous_revision == 0
                || evidence.revision != next_revision
                || evidence.closed_at_ms == 0
                || !is_lower_sha256(&evidence.command_sha256)
            {
                return Err(HarnessError::State(
                    "Agent Loop closure transition has invalid revision, time, or digest"
                        .to_owned(),
                ));
            }
        }
        crate::ItemKind::AgentLoopWaitDenied { evidence } => {
            validate_state_id("Agent Loop wait", evidence.wait_id.as_str())?;
            validate_state_id("Agent Loop denial command", evidence.command_id.as_str())?;
            validate_approval_settlement_shape(&evidence.settlement)?;
            let next_revision = evidence.previous_revision.checked_add(1).ok_or_else(|| {
                HarnessError::State("Agent Loop denial revision overflow".to_owned())
            })?;
            if evidence.previous_revision == 0
                || evidence.revision != next_revision
                || evidence.denied_at_ms == 0
                || !matches!(
                    &evidence.settlement.decision,
                    crate::ApprovalDecision::Deny { .. }
                )
                || !is_lower_sha256(&evidence.command_sha256)
            {
                return Err(HarnessError::State(
                    "Agent Loop denial transition has invalid settlement, revision, time, or digest"
                        .to_owned(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_state_event_schema(
    event: &StateEvent,
    schema_version: u32,
) -> Result<(), HarnessError> {
    match event {
        StateEvent::ThreadCreated {
            tenant_id: Some(_), ..
        } if schema_version < 12 => Err(HarnessError::State(format!(
            "schema-{schema_version} cannot contain a Thread tenant"
        ))),
        StateEvent::ThreadNamed { .. } if schema_version < 8 => Err(HarnessError::State(format!(
            "schema-{schema_version} cannot contain a Thread name"
        ))),
        StateEvent::ThreadForked { .. } if schema_version < 9 => Err(HarnessError::State(format!(
            "schema-{schema_version} cannot contain Thread fork lineage"
        ))),
        StateEvent::ThreadImported { .. } if schema_version < 10 => Err(HarnessError::State(
            format!("schema-{schema_version} cannot contain Thread import provenance"),
        )),
        StateEvent::TurnCompleted { .. } if schema_version < 15 => Err(HarnessError::State(
            format!("schema-{schema_version} cannot contain an atomic CompletionReceipt"),
        )),
        StateEvent::TurnFinished {
            status: TurnStatus::Completed,
            ..
        } if schema_version >= 15 => Err(HarnessError::State(format!(
            "schema-{schema_version} completion requires an atomic CompletionReceipt"
        ))),
        StateEvent::WaitStarted { .. }
        | StateEvent::AcceptResume { .. }
        | StateEvent::ClaimReady { .. }
        | StateEvent::WaitClosed { .. }
        | StateEvent::DenyWait { .. }
            if schema_version < 16 =>
        {
            Err(HarnessError::State(format!(
                "schema-{schema_version} cannot contain Agent Loop wait transitions"
            )))
        }
        StateEvent::ToolCallsAppended { calls, .. } => {
            if schema_version < 7 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain an atomic Tool-call batch"
                )));
            }
            for call in calls {
                validate_state_item_schema(&call.kind, schema_version)?;
            }
            Ok(())
        }
        StateEvent::ItemAppended {
            item:
                Item {
                    kind: ItemKind::ToolCall { batch: Some(_), .. },
                    ..
                },
            ..
        } => Err(HarnessError::State(
            "batched Tool calls require one atomic ToolCallsAppended event".to_owned(),
        )),
        StateEvent::ItemAppended {
            item:
                Item {
                    kind:
                        ItemKind::AgentLoopWaitStarted { .. }
                        | ItemKind::AgentLoopResumeAccepted { .. }
                        | ItemKind::AgentLoopReadyClaimed { .. }
                        | ItemKind::AgentLoopWaitClosed { .. }
                        | ItemKind::AgentLoopWaitDenied { .. },
                    ..
                },
            ..
        } => Err(HarnessError::State(
            "Agent Loop transitions require their atomic State event".to_owned(),
        )),
        StateEvent::WaitStarted {
            approval_requested,
            transition,
            ..
        } => {
            validate_state_item_schema(&approval_requested.kind, schema_version)?;
            validate_state_item_schema(&transition.kind, schema_version)
        }
        StateEvent::AcceptResume {
            approval_decision,
            transition,
            ..
        } => {
            validate_state_item_schema(&approval_decision.kind, schema_version)?;
            validate_state_item_schema(&transition.kind, schema_version)
        }
        StateEvent::ClaimReady { transition, .. } => {
            validate_state_item_schema(&transition.kind, schema_version)
        }
        StateEvent::WaitClosed {
            stopped,
            transition,
            ..
        } => {
            validate_state_item_schema(&stopped.kind, schema_version)?;
            validate_state_item_schema(&transition.kind, schema_version)
        }
        StateEvent::DenyWait {
            approval_decision,
            transition,
            ..
        } => {
            validate_state_item_schema(&approval_decision.kind, schema_version)?;
            validate_state_item_schema(&transition.kind, schema_version)
        }
        StateEvent::ItemAppended {
            item: Item { kind, .. },
            ..
        } => validate_state_item_schema(kind, schema_version),
        _ => Ok(()),
    }
}

fn validate_state_item_schema(kind: &ItemKind, schema_version: u32) -> Result<(), HarnessError> {
    match kind {
        ItemKind::AssistantMessage {
            model_id,
            model_origin,
            model_request_sha256,
            ..
        } => {
            let attributed = model_id.is_some() && model_origin.is_some();
            if schema_version >= 15 && !(attributed && model_request_sha256.is_some()) {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} assistant candidate requires Model identity, origin, and request digest"
                )));
            }
            if schema_version < 15 && model_request_sha256.is_some() {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} assistant candidate cannot contain a Model request digest"
                )));
            }
            if model_id.is_some() != model_origin.is_some() {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} assistant candidate has partial Model attribution"
                )));
            }
        }
        ItemKind::VerificationResult {
            candidate_item_id,
            verifier_origin,
            verifier_binding_sha256,
            ..
        } => {
            let binding_fields = [
                candidate_item_id.is_some(),
                verifier_origin.is_some(),
                verifier_binding_sha256.is_some(),
            ];
            if schema_version >= 15 && binding_fields.iter().any(|present| !*present) {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} verification result requires candidate, origin, and binding digest"
                )));
            }
            if schema_version < 15 && binding_fields.iter().any(|present| *present) {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} verification result cannot contain completion binding evidence"
                )));
            }
        }
        ItemKind::ExecutionBinding { .. } => {
            if schema_version < 13 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain execution binding evidence"
                )));
            }
        }
        ItemKind::ToolResult {
            connector_evidence, ..
        } if !connector_evidence.is_empty() && schema_version < 14 => {
            return Err(HarnessError::State(format!(
                "schema-{schema_version} cannot contain Connector evidence"
            )));
        }
        ItemKind::SteeringQueued { .. } | ItemKind::SteeringApplied { .. } => {
            if schema_version < 6 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain Turn steering evidence"
                )));
            }
        }
        ItemKind::ProviderContinuation { .. } => {
            if schema_version < 5 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain Provider continuation evidence"
                )));
            }
        }
        ItemKind::InvocationContext { .. } => {
            if schema_version < 11 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain Turn context evidence"
                )));
            }
        }
        ItemKind::PolicyDecision { tool_origin, .. } => {
            if schema_version >= 4 && tool_origin.is_none() {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} Policy decision requires Tool origin evidence"
                )));
            }
            if schema_version < 4 && tool_origin.is_some() {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} Policy decision cannot contain Tool origin evidence"
                )));
            }
        }
        ItemKind::ApprovalRequested {
            requested_by,
            tool_origin,
            model_request_sha256,
            ..
        } => {
            let has_continuation =
                requested_by.is_some() && tool_origin.is_some() && model_request_sha256.is_some();
            if schema_version >= 3 && !has_continuation {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} approval request requires continuation evidence"
                )));
            }
            if schema_version < 3 && has_continuation {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} approval request cannot contain continuation evidence"
                )));
            }
        }
        ItemKind::AgentLoopWaitStarted { .. }
        | ItemKind::AgentLoopResumeAccepted { .. }
        | ItemKind::AgentLoopReadyClaimed { .. }
        | ItemKind::AgentLoopWaitClosed { .. }
        | ItemKind::AgentLoopWaitDenied { .. } => {
            if schema_version < 16 {
                return Err(HarnessError::State(format!(
                    "schema-{schema_version} cannot contain Agent Loop transition evidence"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_steering_content(content: &str) -> Result<(), HarnessError> {
    if content.trim().is_empty() || content.len() > MAX_STEERING_CONTENT_BYTES {
        return Err(HarnessError::State(format!(
            "steering content must be 1-{MAX_STEERING_CONTENT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_execution_binding_projection(thread: &Thread) -> Result<(), HarnessError> {
    for turn in &thread.turns {
        let mut bindings = turn.items.iter().filter_map(|item| {
            if let ItemKind::ExecutionBinding { binding, .. } = &item.kind {
                Some(binding)
            } else {
                None
            }
        });
        let Some(binding) = bindings.next() else {
            continue;
        };
        if bindings.next().is_some() {
            return Err(HarnessError::State(format!(
                "turn {} contains multiple execution bindings",
                turn.id
            )));
        }
        if binding.tenant_id() != thread.tenant_id() {
            return Err(HarnessError::State(format!(
                "turn {} execution binding tenant differs from its Thread",
                turn.id
            )));
        }
    }
    Ok(())
}

fn validate_connector_evidence_records(
    output: &serde_json::Value,
    is_error: bool,
    evidence: &[crate::ConnectorEvidence],
) -> Result<(), HarnessError> {
    if evidence.is_empty() {
        return Ok(());
    }
    if is_error {
        return Err(HarnessError::State(
            "failed Tool result cannot retain Connector evidence".to_owned(),
        ));
    }
    if evidence.len() > crate::MAX_CONNECTOR_EVIDENCE_PER_RESULT {
        return Err(HarnessError::State(format!(
            "Tool result exceeds {} Connector evidence records",
            crate::MAX_CONNECTOR_EVIDENCE_PER_RESULT
        )));
    }
    let output_sha256 =
        bounded_serialized_sha256(output, MAX_STATE_EVENT_BYTES).map_err(|error| {
            state_json_error("Connector evidence output", MAX_STATE_EVENT_BYTES, error)
        })?;
    let mut claims = BTreeSet::new();
    for record in evidence {
        record
            .validate()
            .map_err(|error| HarnessError::State(error.to_string()))?;
        if record.output_sha256() != output_sha256 {
            return Err(HarnessError::State(
                "Connector evidence digest differs from its Tool output".to_owned(),
            ));
        }
        let claim = record.claim();
        if !claims.insert((claim.source(), claim.resource(), claim.version())) {
            return Err(HarnessError::State(
                "Tool result contains duplicate Connector evidence".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_connector_evidence_projection(thread: &Thread) -> Result<(), HarnessError> {
    for turn in &thread.turns {
        let mut provenance = BTreeMap::new();
        for item in &turn.items {
            record_connector_execution_provenance(&mut provenance, item);
            let ItemKind::ToolResult {
                call_id,
                connector_evidence,
                ..
            } = &item.kind
            else {
                continue;
            };
            if connector_evidence.is_empty() {
                continue;
            }
            validate_connector_evidence_provenance(
                thread.tenant_id(),
                call_id,
                connector_evidence,
                provenance.get(call_id.as_str()),
            )?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum ConnectorAuthorization {
    #[default]
    Missing,
    Allowed,
    Denied,
    AskPending,
    AskApproved,
}

#[derive(Default)]
struct ConnectorExecutionProvenance<'a> {
    connector: Option<&'a str>,
    origin: Option<&'a crate::CapabilityOrigin>,
    authorization: ConnectorAuthorization,
    pending_approval: Option<&'a crate::ApprovalId>,
    call_ambiguous: bool,
    policy_ambiguous: bool,
    approval_invalid: bool,
}

fn record_connector_execution_provenance<'a>(
    provenance: &mut BTreeMap<&'a str, ConnectorExecutionProvenance<'a>>,
    item: &'a Item,
) {
    match &item.kind {
        ItemKind::ToolCall { call_id, name, .. } => {
            let entry = provenance.entry(call_id.as_str()).or_default();
            if entry.connector.replace(name.as_str()).is_some() {
                entry.call_ambiguous = true;
            }
        }
        ItemKind::PolicyDecision {
            call_id,
            tool_origin: Some(origin),
            decision,
        } => {
            let entry = provenance.entry(call_id.as_str()).or_default();
            if entry.origin.replace(origin).is_some() {
                entry.policy_ambiguous = true;
            }
            entry.authorization = match decision {
                crate::PolicyDecision::Allow => ConnectorAuthorization::Allowed,
                crate::PolicyDecision::Deny { .. } => ConnectorAuthorization::Denied,
                crate::PolicyDecision::Ask { .. } => ConnectorAuthorization::AskPending,
            };
        }
        ItemKind::ApprovalRequested {
            approval_id,
            call_id,
            tool,
            tool_origin,
            ..
        } => {
            let entry = provenance.entry(call_id.as_str()).or_default();
            let valid = entry.authorization == ConnectorAuthorization::AskPending
                && entry.pending_approval.is_none()
                && entry.connector == Some(tool.as_str())
                && entry.origin == tool_origin.as_ref();
            if valid {
                entry.pending_approval = Some(approval_id);
            } else {
                entry.approval_invalid = true;
            }
        }
        ItemKind::ApprovalDecision {
            approval_id,
            call_id,
            decision,
        } => {
            let entry = provenance.entry(call_id.as_str()).or_default();
            if entry.authorization == ConnectorAuthorization::AskPending
                && entry.pending_approval == Some(approval_id)
            {
                entry.authorization = match decision {
                    crate::ApprovalDecision::Approve => ConnectorAuthorization::AskApproved,
                    crate::ApprovalDecision::Deny { .. } => ConnectorAuthorization::Denied,
                };
            } else {
                entry.approval_invalid = true;
            }
        }
        _ => {}
    }
}

fn validate_connector_evidence_provenance(
    thread_tenant_id: Option<&str>,
    call_id: &str,
    evidence: &[crate::ConnectorEvidence],
    provenance: Option<&ConnectorExecutionProvenance<'_>>,
) -> Result<(), HarnessError> {
    let Some(provenance) = provenance else {
        return Err(HarnessError::State(format!(
            "Connector evidence references unknown Tool call {call_id}"
        )));
    };
    let Some(connector) = provenance.connector else {
        return Err(HarnessError::State(format!(
            "Connector evidence references unknown Tool call {call_id}"
        )));
    };
    if provenance.call_ambiguous {
        return Err(HarnessError::State(format!(
            "Connector evidence Tool call {call_id} is ambiguous"
        )));
    }
    let Some(origin) = provenance.origin else {
        return Err(HarnessError::State(format!(
            "Connector evidence lacks Policy origin for Tool call {call_id}"
        )));
    };
    if provenance.policy_ambiguous {
        return Err(HarnessError::State(format!(
            "Connector evidence Policy origin for Tool call {call_id} is ambiguous"
        )));
    }
    if provenance.approval_invalid
        || !matches!(
            provenance.authorization,
            ConnectorAuthorization::Allowed | ConnectorAuthorization::AskApproved
        )
    {
        return Err(HarnessError::State(format!(
            "Connector evidence Tool call {call_id} lacks an authorized execution path"
        )));
    }
    for record in evidence {
        if record.connector() != connector
            || record.connector_origin() != origin
            || record.authority().tenant_id() != thread_tenant_id
        {
            return Err(HarnessError::State(format!(
                "Connector evidence for Tool call {call_id} differs from registered execution provenance"
            )));
        }
    }
    Ok(())
}

fn validate_connector_evidence_append(
    thread: &Thread,
    turn_id: &TurnId,
    item: &Item,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    let turn = thread
        .turns
        .iter()
        .find(|turn| &turn.id == turn_id)
        .ok_or_else(|| {
            HarnessError::State(format!(
                "Connector evidence references unknown turn {turn_id}"
            ))
        })?;
    let ItemKind::ToolResult {
        call_id,
        connector_evidence,
        ..
    } = &item.kind
    else {
        return Ok(());
    };
    if connector_evidence
        .iter()
        .any(|record| record.authority() != authority)
    {
        return Err(HarnessError::State(format!(
            "turn {turn_id} Connector evidence authority differs from trusted append authority"
        )));
    }
    let mut provenance = BTreeMap::new();
    for prior in &turn.items {
        record_connector_execution_provenance(&mut provenance, prior);
    }
    validate_connector_evidence_provenance(
        thread.tenant_id(),
        call_id,
        connector_evidence,
        provenance.get(call_id.as_str()),
    )
}

fn validate_execution_binding_append(
    thread: &Thread,
    turn_id: &TurnId,
    bound_by: &crate::ActorIdentity,
    binding: &crate::ExecutionBinding,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    let turn = thread
        .turns
        .iter()
        .find(|turn| &turn.id == turn_id)
        .ok_or_else(|| {
            HarnessError::State(format!(
                "execution binding references unknown turn {turn_id}"
            ))
        })?;
    if turn
        .items
        .iter()
        .any(|item| matches!(item.kind, ItemKind::ExecutionBinding { .. }))
    {
        return Err(HarnessError::State(format!(
            "turn {turn_id} already has an execution binding"
        )));
    }
    if bound_by != authority.actor() {
        return Err(HarnessError::State(format!(
            "turn {turn_id} execution binding actor differs from its trusted authority"
        )));
    }
    if binding.tenant_id() != thread.tenant_id() {
        return Err(HarnessError::State(format!(
            "turn {turn_id} execution binding tenant differs from its Thread"
        )));
    }
    Ok(())
}

fn validate_steering_projection(thread: &Thread) -> Result<(), HarnessError> {
    let mut all_ids = BTreeSet::new();
    for turn in &thread.turns {
        let mut queued = VecDeque::new();
        for item in &turn.items {
            match &item.kind {
                ItemKind::SteeringQueued {
                    steering_id,
                    content,
                    ..
                } => {
                    if !all_ids.insert(steering_id.clone()) {
                        return Err(HarnessError::State(format!(
                            "duplicate steering identity {steering_id}"
                        )));
                    }
                    queued.push_back((steering_id.clone(), content.clone()));
                }
                ItemKind::SteeringApplied {
                    steering_id,
                    content,
                } => {
                    let Some((queued_id, queued_content)) = queued.pop_front() else {
                        return Err(HarnessError::State(format!(
                            "applied steering {steering_id} has no earlier queue record"
                        )));
                    };
                    if queued_id != *steering_id {
                        return Err(HarnessError::State(format!(
                            "applied steering {steering_id} violates queue order"
                        )));
                    }
                    if queued_content != *content {
                        return Err(HarnessError::State(format!(
                            "applied steering {steering_id} changed its queued content"
                        )));
                    }
                }
                _ => {}
            }
        }
        if turn.status == TurnStatus::Completed && !queued.is_empty() {
            return Err(HarnessError::State(format!(
                "completed turn {} contains unapplied steering",
                turn.id
            )));
        }
    }
    Ok(())
}

fn validate_tool_call_batch_projection(thread: &Thread) -> Result<(), HarnessError> {
    let mut batch_ids = BTreeSet::new();
    for turn in &thread.turns {
        let mut index = 0;
        while index < turn.items.len() {
            let ItemKind::ToolCall {
                batch: Some(batch), ..
            } = &turn.items[index].kind
            else {
                index += 1;
                continue;
            };
            if batch.index != 0 || !batch_ids.insert(batch.id.clone()) {
                return Err(HarnessError::State(
                    "Tool-call batch projection starts out of order or reuses an identity"
                        .to_owned(),
                ));
            }
            let end = index
                .checked_add(batch.size)
                .filter(|end| *end <= turn.items.len())
                .ok_or_else(|| {
                    HarnessError::State("Tool-call batch projection is truncated".to_owned())
                })?;
            validate_tool_call_batch_items(&turn.items[index..end])?;
            index = end;
        }
    }
    Ok(())
}

fn validate_steering_append(
    thread: &Thread,
    turn_id: &TurnId,
    item: &Item,
) -> Result<(), HarnessError> {
    let turn = thread
        .turns
        .iter()
        .find(|turn| &turn.id == turn_id)
        .ok_or_else(|| {
            HarnessError::State(format!("steering references unknown turn {turn_id}"))
        })?;
    let mut queued = pending_steering(turn)?;
    match &item.kind {
        ItemKind::SteeringQueued { steering_id, .. } => {
            if thread
                .turns
                .iter()
                .flat_map(|turn| &turn.items)
                .any(|item| {
                    matches!(
                        &item.kind,
                        ItemKind::SteeringQueued {
                            steering_id: existing,
                            ..
                        } if existing == steering_id
                    )
                })
            {
                return Err(HarnessError::State(format!(
                    "duplicate steering identity {steering_id}"
                )));
            }
        }
        ItemKind::SteeringApplied {
            steering_id,
            content,
        } => {
            let Some((queued_id, queued_content)) = queued.pop_front() else {
                return Err(HarnessError::State(format!(
                    "applied steering {steering_id} has no pending queue record"
                )));
            };
            if queued_id != *steering_id {
                return Err(HarnessError::State(format!(
                    "applied steering {steering_id} violates queue order"
                )));
            }
            if queued_content != *content {
                return Err(HarnessError::State(format!(
                    "applied steering {steering_id} changed its queued content"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn pending_steering(turn: &Turn) -> Result<VecDeque<(crate::SteeringId, String)>, HarnessError> {
    let mut queued = VecDeque::new();
    let mut seen = BTreeSet::new();
    for item in &turn.items {
        match &item.kind {
            ItemKind::SteeringQueued {
                steering_id,
                content,
                ..
            } => {
                if !seen.insert(steering_id.clone()) {
                    return Err(HarnessError::State(format!(
                        "duplicate steering identity {steering_id}"
                    )));
                }
                queued.push_back((steering_id.clone(), content.clone()));
            }
            ItemKind::SteeringApplied { steering_id, .. } => {
                let Some((queued_id, _)) = queued.pop_front() else {
                    return Err(HarnessError::State(format!(
                        "applied steering {steering_id} has no earlier queue record"
                    )));
                };
                if queued_id != *steering_id {
                    return Err(HarnessError::State(format!(
                        "applied steering {steering_id} violates queue order"
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(queued)
}

fn has_pending_steering(turn: &Turn) -> Result<bool, HarnessError> {
    Ok(!pending_steering(turn)?.is_empty())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_state_id(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(HarnessError::State(format!(
            "{kind} identity must be 1-256 non-control bytes"
        )));
    }
    Ok(())
}

fn validate_event_page_limit(limit: usize) -> Result<(), HarnessError> {
    if !(1..=MAX_STATE_EVENT_PAGE).contains(&limit) {
        return Err(HarnessError::State(format!(
            "event page limit must be 1-{MAX_STATE_EVENT_PAGE}"
        )));
    }
    Ok(())
}

fn validate_event_page_request(limit: usize, max_recovery_bytes: u64) -> Result<(), HarnessError> {
    validate_event_page_limit(limit)?;
    if max_recovery_bytes == 0 || max_recovery_bytes > MAX_STATE_EVENT_PAGE_RECOVERY_BYTES {
        return Err(HarnessError::State(format!(
            "event page recovery budget must be 1-{MAX_STATE_EVENT_PAGE_RECOVERY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_thread_summary_page_request(
    before_sequence: Option<u64>,
    limit: usize,
) -> Result<(), HarnessError> {
    if limit == 0 || limit > MAX_THREAD_SUMMARY_PAGE + 1 {
        return Err(HarnessError::State(format!(
            "Thread store page limit must be 1-{}",
            MAX_THREAD_SUMMARY_PAGE + 1
        )));
    }
    if before_sequence == Some(0) {
        return Err(HarnessError::State(
            "Thread page cursor must be greater than zero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_thread_summaries(
    summaries: &[ThreadSummary],
    before_sequence: Option<u64>,
    limit: usize,
) -> Result<(), HarnessError> {
    if summaries.len() > limit {
        return Err(HarnessError::State(
            "Event Store exceeded the requested Thread page limit".to_owned(),
        ));
    }
    let mut previous = before_sequence;
    let mut identities = BTreeSet::new();
    for summary in summaries {
        validate_state_id("Thread summary", summary.thread_id.as_str())?;
        validate_thread_name(summary.name.as_deref())?;
        if let Some(lineage) = &summary.lineage {
            validate_thread_lineage(lineage)?;
            if lineage.parent_thread_id == summary.thread_id
                || lineage.parent_through_sequence >= summary.last_sequence
            {
                return Err(HarnessError::State(
                    "Thread summary contains invalid direct lineage".to_owned(),
                ));
            }
        }
        if summary.last_sequence == 0
            || summary.stream_version == 0
            || summary.stream_version > summary.last_sequence
            || previous.is_some_and(|cursor| summary.last_sequence >= cursor)
            || !identities.insert(&summary.thread_id)
        {
            return Err(HarnessError::State(
                "Event Store returned invalid or unordered Thread summaries".to_owned(),
            ));
        }
        previous = Some(summary.last_sequence);
    }
    Ok(())
}

fn encode_state_event(event: &StateEvent) -> Result<EncodedStateEvent, HarnessError> {
    validate_state_event_json_shape(event)?;
    let encoded = to_bounded_json_vec(event, MAX_STATE_EVENT_BYTES)
        .map_err(|error| state_json_error("state event", MAX_STATE_EVENT_BYTES, error))?;
    let encoded_bytes = u64::try_from(encoded.len())
        .map_err(|_| HarnessError::State("state event size exceeds u64".to_owned()))?;
    let recovery_bytes = encoded_bytes
        .checked_add(STATE_EVENT_RECOVERY_OVERHEAD_BYTES)
        .ok_or_else(|| HarnessError::State("state event recovery charge overflow".to_owned()))?;
    Ok(EncodedStateEvent {
        json: String::from_utf8(encoded)
            .map_err(|_| HarnessError::State("state event is not UTF-8 JSON".to_owned()))?,
        recovery_bytes,
    })
}

fn validate_state_event_json_shape(event: &StateEvent) -> Result<(), HarnessError> {
    let items: &[Item] = match event {
        StateEvent::ItemAppended { item, .. } => std::slice::from_ref(item),
        StateEvent::ToolCallsAppended { calls, .. } => calls,
        _ => &[],
    };
    for item in items {
        let value = match &item.kind {
            ItemKind::ToolCall { input, .. } => Some(input),
            ItemKind::ToolResult { output, .. } => Some(output),
            _ => None,
        };
        if value.is_some_and(|value| validate_value_shape(value).is_err()) {
            return Err(HarnessError::State(
                "state event JSON exceeds the supported depth or node count".to_owned(),
            ));
        }
    }
    Ok(())
}

fn state_json_error(kind: &str, maximum: usize, error: BoundedJsonError) -> HarnessError {
    match error {
        BoundedJsonError::LimitExceeded => {
            HarnessError::State(format!("{kind} exceeds {maximum} bytes"))
        }
        BoundedJsonError::CannotEncode => HarnessError::State(format!("cannot encode {kind}")),
    }
}

fn stored_event_recovery_bytes(event: &StoredEvent) -> Result<u64, HarnessError> {
    Ok(encode_state_event(&event.event)?.recovery_bytes)
}

#[cfg(test)]
fn stored_events_recovery_bytes(events: &[StoredEvent]) -> Result<u64, HarnessError> {
    events.iter().try_fold(0_u64, |total, event| {
        total
            .checked_add(stored_event_recovery_bytes(event)?)
            .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))
    })
}

fn add_recovery_bytes(total: &mut u64, increment: u64) -> Result<(), HarnessError> {
    *total = total
        .checked_add(increment)
        .ok_or_else(|| HarnessError::State("stream recovery charge overflow".to_owned()))?;
    Ok(())
}

fn validate_append_result(
    pending: &PendingEvent,
    stored: &StoredEvent,
) -> Result<(), HarnessError> {
    let _ = validate_stored_event(stored)?;
    if stored.schema_version != STATE_EVENT_SCHEMA_VERSION
        || stored.sequence == 0
        || stored.event_id != pending.event_id
        || stored.thread_id != pending.thread_id
        || stored.recorded_at_ms != pending.recorded_at_ms
        || stored.event != pending.event
    {
        return Err(HarnessError::State(
            "Event Store returned an append result that does not match the pending event"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_stored_events(
    thread_id: &ThreadId,
    events: &[StoredEvent],
    after_sequence: u64,
    limit: Option<usize>,
    max_recovery_bytes: Option<u64>,
) -> Result<u64, HarnessError> {
    if limit.is_some_and(|limit| events.len() > limit) {
        return Err(HarnessError::State(
            "Event Store returned more events than requested".to_owned(),
        ));
    }
    if limit.is_none() && events.len() > MAX_STATE_RECOVERY_EVENTS {
        return Err(HarnessError::State(format!(
            "Thread exceeds {MAX_STATE_RECOVERY_EVENTS} recoverable events"
        )));
    }
    let mut previous_sequence = after_sequence;
    let mut event_ids = BTreeSet::new();
    let mut recovery_bytes = 0_u64;
    for stored in events {
        add_recovery_bytes(&mut recovery_bytes, validate_stored_event(stored)?)?;
        if &stored.thread_id != thread_id {
            return Err(HarnessError::State(
                "Event Store returned an event from another thread".to_owned(),
            ));
        }
        if stored.sequence <= previous_sequence {
            return Err(HarnessError::State(
                "Event Store returned non-increasing event sequences".to_owned(),
            ));
        }
        if !event_ids.insert(stored.event_id.as_str()) {
            return Err(HarnessError::State(
                "Event Store returned duplicate event identities".to_owned(),
            ));
        }
        previous_sequence = stored.sequence;
    }
    if max_recovery_bytes.is_some_and(|maximum| recovery_bytes > maximum) {
        return Err(HarnessError::State(
            "Event Store returned more recovery bytes than requested".to_owned(),
        ));
    }
    if limit.is_none() && recovery_bytes > STATE_THREAD_RECOVERY_BYTE_LIMIT {
        return Err(HarnessError::State(format!(
            "Thread exceeds its {STATE_THREAD_RECOVERY_BYTE_LIMIT}-byte recovery boundary"
        )));
    }
    Ok(recovery_bytes)
}

fn validate_stored_event(stored: &StoredEvent) -> Result<u64, HarnessError> {
    validate_stored_schema(stored)?;
    validate_state_id("event", stored.event_id.as_str())?;
    validate_state_id("thread", stored.thread_id.as_str())?;
    validate_state_event(&stored.event)?;
    Ok(encode_state_event(&stored.event)?.recovery_bytes)
}

fn validate_stored_schema(stored: &StoredEvent) -> Result<(), HarnessError> {
    if !(1..=STATE_EVENT_SCHEMA_VERSION).contains(&stored.schema_version) {
        return Err(HarnessError::State(format!(
            "unsupported state schema version {}",
            stored.schema_version
        )));
    }
    if stored.schema_version == 1
        && matches!(
            &stored.event,
            StateEvent::ItemAppended {
                item: Item {
                    kind: crate::ItemKind::ConversationSummary { .. },
                    ..
                },
                ..
            }
        )
    {
        return Err(HarnessError::State(
            "schema-1 State event cannot contain conversation summary evidence".to_owned(),
        ));
    }
    validate_state_event_schema(&stored.event, stored.schema_version)?;
    Ok(())
}

fn remember_event_ids(
    event_ids: &mut BTreeSet<EventId>,
    events: &[StoredEvent],
) -> Result<(), HarnessError> {
    for event in events {
        if !event_ids.insert(event.event_id.clone()) {
            return Err(HarnessError::State(
                "Event Store returned a duplicate event identity across pages".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_thread_existence(
    thread_exists: bool,
    pending: &PendingEvent,
) -> Result<(), HarnessError> {
    match (&pending.event, thread_exists) {
        (StateEvent::ThreadCreated { .. }, true) => Err(HarnessError::State(format!(
            "thread {} already exists",
            pending.thread_id
        ))),
        (StateEvent::ThreadCreated { .. }, false) | (_, true) => Ok(()),
        (_, false) => Err(HarnessError::State(format!(
            "thread {} does not exist",
            pending.thread_id
        ))),
    }
}

fn matching_existing(
    existing: &StoredEvent,
    pending: &PendingEvent,
) -> Result<StoredEvent, HarnessError> {
    if existing.thread_id == pending.thread_id
        && existing.recorded_at_ms == pending.recorded_at_ms
        && existing.event == pending.event
    {
        Ok(existing.clone())
    } else {
        Err(HarnessError::State(format!(
            "event id {} was reused with different content",
            pending.event_id
        )))
    }
}

fn decode_row(
    event_id: EventId,
    row: (i64, String, i64, i64, String),
) -> Result<StoredEvent, HarnessError> {
    let (sequence, thread_id, recorded_at_ms, schema_version, event_json) = row;
    if event_json.len() > MAX_STATE_EVENT_BYTES {
        return Err(HarnessError::State(format!(
            "stored state event exceeds {MAX_STATE_EVENT_BYTES} bytes"
        )));
    }
    let stored = StoredEvent {
        schema_version: u32::try_from(schema_version)
            .map_err(|_| HarnessError::State("invalid schema version".to_owned()))?,
        sequence: u64::try_from(sequence)
            .map_err(|_| HarnessError::State("negative event sequence".to_owned()))?,
        event_id,
        thread_id: ThreadId::from_string(thread_id),
        recorded_at_ms: u64::try_from(recorded_at_ms)
            .map_err(|_| HarnessError::State("negative event timestamp".to_owned()))?,
        event: serde_json::from_str(&event_json)
            .map_err(|error| HarnessError::State(error.to_string()))?,
    };
    validate_stored_event(&stored)?;
    Ok(stored)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use rusqlite::OptionalExtension;
    use serde_json::{Value, json};

    use super::{
        AgentLoopDueCursor, AgentLoopDuePhase, AgentLoopDueScanPage, AgentLoopDueWait,
        AgentLoopReadyClaimCommand, AgentLoopWaitCloseCommand, AgentLoopWaitStartCommand,
        EventAppendDisposition, EventAppendResult, EventStore, MemoryEventStore,
        SnapshotMaintenanceConfig, SnapshotMaintenanceFailure, SqliteEventStore,
        StateCapacityLevel, StateEngine, StateSnapshot,
    };
    use crate::{
        ActorIdentity, AgentLoopClaimId, AgentLoopCloseCommandId, AgentLoopDenyCommandId,
        AgentLoopExecution, AgentLoopResumeCommandId, AgentLoopWaitId, AgentLoopWorkerId,
        InboxTombstoneReason,
        ApprovalDecision, ApprovalId, ApprovalRecord, ApprovalRecordStatus, ApprovalRequest,
        AuthorityContext, CapabilityOrigin, CompletionAssurance, CompletionContract,
        CompletionGeneration, CompletionReceipt, ConnectorEvidence, ConnectorEvidenceClaim,
        EventId, HarnessError, HarnessFuture, InvocationContextEvidence, Item, ItemKind,
        ModelContinuation, NewStreamEvent, PendingEvent, PolicyDecision, RiskLevel, StateEvent,
        SteeringId, StoredEvent, ThreadId, ThreadImportOrigin, ThreadLineage, ToolAuthorization,
        ToolCallBatch, ToolCallBatchId, ToolDescriptor, Turn, TurnId, TurnStatus, TurnStopReason,
        VerificationOutcome, build_completion_receipt, completion_model_request_sha256,
        completion_model_route_sha256, completion_receipt_sha256,
        completion_runtime_governance_sha256, completion_tool_view_sha256,
        completion_verifier_manifest_sha256, kernel::now_ms,
    };

    fn test_completion_generation(model_request_sha256: &str) -> CompletionGeneration {
        CompletionGeneration::new(
            model_request_sha256,
            completion_model_route_sha256(&["test/model"]).expect("Model route digest"),
            completion_tool_view_sha256(&Vec::<String>::new()).expect("Tool view digest"),
            completion_verifier_manifest_sha256(&[]).expect("Verifier manifest digest"),
            completion_runtime_governance_sha256(&json!({"max_steps": 16}))
                .expect("Runtime governance digest"),
            None,
            CompletionAssurance::RuntimeMeasured,
        )
        .expect("completion generation")
    }

    async fn approval_wait_fixture(
        state: &StateEngine,
        authority: &AuthorityContext,
    ) -> (Turn, ApprovalRequest, CompletionGeneration) {
        approval_wait_fixture_with_input(state, authority, json!({"value": 7})).await
    }

    async fn approval_wait_fixture_with_input(
        state: &StateEngine,
        authority: &AuthorityContext,
        input: Value,
    ) -> (Turn, ApprovalRequest, CompletionGeneration) {
        let thread = state
            .create_thread_as(authority)
            .await
            .expect("create approval Thread");
        let turn = state
            .start_turn_as(&thread.id, authority)
            .await
            .expect("start approval Turn");
        let call_id = "approval-call-1".to_owned();
        let descriptor = ToolDescriptor {
            name: "write_record".to_owned(),
            description: "Writes one bounded test record".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"]
            }),
        };
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ToolCall {
                    model_id: Some("test/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: call_id.clone(),
                    name: descriptor.name.clone(),
                    input: input.clone(),
                    batch: None,
                }),
                authority,
            )
            .await
            .expect("append ToolCall");
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: call_id.clone(),
                    tool_origin: Some(CapabilityOrigin::BuiltIn),
                    decision: PolicyDecision::Ask {
                        reason: "operator confirmation required".to_owned(),
                        risk: RiskLevel::High,
                    },
                }),
                authority,
            )
            .await
            .expect("append Ask decision");
        let request = ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: authority.actor().clone(),
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
            "call": request.authorization.call_id.as_str()
        }))
        .expect("approval Model request digest");
        let generation = test_completion_generation(&model_request_sha256);
        (turn, request, generation)
    }

    async fn executing_approval_wait_fixture(
        state: &StateEngine,
        authority: &AuthorityContext,
    ) -> Turn {
        let (turn, request, generation) = approval_wait_fixture(state, authority).await;
        let wait_id = AgentLoopWaitId::generate();
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    None,
                    Some(10_000),
                ),
                authority,
            )
            .await
            .expect("start recovery-fenced wait");
        let resume_command_id = AgentLoopResumeCommandId::generate();
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                resume_command_id.clone(),
                &settled_approval(request, authority.tenant_id().map(str::to_owned)),
                authority,
            )
            .await
            .expect("accept recovery-fenced wait");
        state
            .claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id,
                    2,
                    resume_command_id,
                    AgentLoopClaimId::generate(),
                    AgentLoopWorkerId::generate(),
                ),
                authority,
            )
            .await
            .expect("claim recovery-fenced wait");
        turn
    }

    fn settled_approval(request: ApprovalRequest, tenant_id: Option<String>) -> ApprovalRecord {
        let settled_at_ms = now_ms();
        ApprovalRecord {
            schema_version: crate::APPROVAL_INBOX_SCHEMA_VERSION,
            request,
            tenant_id,
            status: ApprovalRecordStatus::Settled {
                decision: ApprovalDecision::Approve,
                decided_by: ActorIdentity::Authenticated {
                    authority: "test-approver".to_owned(),
                    subject: "operator-7".to_owned(),
                },
            },
            revision: 2,
            requested_at_ms: settled_at_ms.saturating_sub(1),
            settled_at_ms: Some(settled_at_ms),
        }
    }

    fn denied_approval(
        request: ApprovalRequest,
        tenant_id: Option<String>,
        reason: &str,
    ) -> ApprovalRecord {
        let settled_at_ms = now_ms();
        ApprovalRecord {
            schema_version: crate::APPROVAL_INBOX_SCHEMA_VERSION,
            request,
            tenant_id,
            status: ApprovalRecordStatus::Settled {
                decision: ApprovalDecision::Deny {
                    reason: reason.to_owned(),
                },
                decided_by: ActorIdentity::Authenticated {
                    authority: "test-approver".to_owned(),
                    subject: "operator-7".to_owned(),
                },
            },
            revision: 2,
            requested_at_ms: settled_at_ms.saturating_sub(1),
            settled_at_ms: Some(settled_at_ms),
        }
    }

    async fn append_completion_candidate(
        state: &StateEngine,
        turn: &Turn,
        authority: &AuthorityContext,
        content: &str,
    ) -> (Turn, CompletionReceipt) {
        let model_request_sha256 = completion_model_request_sha256(&json!({"content": content}))
            .expect("Model request digest");
        let candidate = Item::new(ItemKind::AssistantMessage {
            model_id: Some("test/model".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(model_request_sha256.clone()),
            content: content.to_owned(),
        });
        state
            .append_item_as(turn, candidate.clone(), authority)
            .await
            .expect("append completion candidate");
        let projected = state
            .load_thread_as(&turn.thread_id, authority)
            .await
            .expect("load completion candidate")
            .expect("completion Thread")
            .turns
            .into_iter()
            .find(|candidate_turn| candidate_turn.id == turn.id)
            .expect("completion Turn");
        let receipt = build_completion_receipt(
            &projected,
            authority,
            &candidate.id,
            test_completion_generation(&model_request_sha256),
            CompletionContract::v1_no_external_requirements(),
        )
        .expect("build completion receipt");
        (projected, receipt)
    }

    fn connector_evidence(
        connector: &str,
        origin: CapabilityOrigin,
        authority: AuthorityContext,
        output: &serde_json::Value,
    ) -> ConnectorEvidence {
        ConnectorEvidence::bind(
            connector.to_owned(),
            origin,
            authority,
            crate::json::bounded_serialized_sha256(output, super::MAX_STATE_EVENT_BYTES)
                .expect("output digest"),
            ConnectorEvidenceClaim::new("crm", "contacts/customer-42", "revision-7", 1, None, None)
                .expect("claim"),
        )
        .expect("bound evidence")
    }

    struct LyingAppendStore;

    impl EventStore for LyingAppendStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            Box::pin(async move {
                Ok(StoredEvent {
                    schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                    sequence: 1,
                    event_id: EventId::from_static("wrong-event"),
                    thread_id: pending.thread_id,
                    recorded_at_ms: pending.recorded_at_ms,
                    event: pending.event,
                })
            })
        }

        fn events_page<'a>(
            &'a self,
            _thread_id: &'a ThreadId,
            _after_sequence: u64,
            _limit: usize,
            _max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct LegacyLabelAppendStore;

    impl EventStore for LegacyLabelAppendStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            Box::pin(async move {
                Ok(StoredEvent {
                    schema_version: 1,
                    sequence: 1,
                    event_id: pending.event_id,
                    thread_id: pending.thread_id,
                    recorded_at_ms: pending.recorded_at_ms,
                    event: pending.event,
                })
            })
        }

        fn events_page<'a>(
            &'a self,
            _thread_id: &'a ThreadId,
            _after_sequence: u64,
            _limit: usize,
            _max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct CommitThenFailStore {
        inner: MemoryEventStore,
        fail_completion_once: AtomicBool,
    }

    impl CommitThenFailStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                fail_completion_once: AtomicBool::new(true),
            }
        }
    }

    impl EventStore for CommitThenFailStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            Box::pin(async move {
                let is_completion = matches!(&pending.event, StateEvent::TurnCompleted { .. });
                let stored = self.inner.append(pending).await?;
                if is_completion && self.fail_completion_once.swap(false, Ordering::SeqCst) {
                    Err(HarnessError::State(
                        "simulated ambiguous completion outcome".to_owned(),
                    ))
                } else {
                    Ok(stored)
                }
            })
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }
    }

    #[derive(Clone, Copy)]
    #[repr(u8)]
    enum ExactReadFault {
        None = 0,
        Missing = 1,
        WrongShape = 2,
        CrossThread = 3,
    }

    /// Counts settlement reads while preserving the Memory store's real due index.
    ///
    /// Faults are injected only at the exact-event port. This models a corrupt
    /// disposable projection or identity index without weakening the bounded
    /// production store API to admit oversized rows.
    struct ExactReadProbeStore {
        inner: MemoryEventStore,
        exact_read_calls: AtomicUsize,
        event_page_calls: AtomicUsize,
        snapshot_load_calls: AtomicUsize,
        fault: AtomicU8,
        fault_on_call: AtomicUsize,
    }

    impl ExactReadProbeStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                exact_read_calls: AtomicUsize::new(0),
                event_page_calls: AtomicUsize::new(0),
                snapshot_load_calls: AtomicUsize::new(0),
                fault: AtomicU8::new(ExactReadFault::None as u8),
                fault_on_call: AtomicUsize::new(usize::MAX),
            }
        }

        fn reset_read_counts(&self) {
            self.exact_read_calls.store(0, Ordering::SeqCst);
            self.event_page_calls.store(0, Ordering::SeqCst);
            self.snapshot_load_calls.store(0, Ordering::SeqCst);
        }

        fn arm_exact_read_fault(&self, fault: ExactReadFault, on_call: usize) {
            assert!(on_call > 0, "exact-read fault call must be positive");
            self.fault.store(fault as u8, Ordering::SeqCst);
            self.fault_on_call.store(on_call, Ordering::SeqCst);
        }

        fn clear_exact_read_fault(&self) {
            self.fault
                .store(ExactReadFault::None as u8, Ordering::SeqCst);
            self.fault_on_call.store(usize::MAX, Ordering::SeqCst);
        }

        fn read_counts(&self) -> (usize, usize, usize) {
            (
                self.exact_read_calls.load(Ordering::SeqCst),
                self.event_page_calls.load(Ordering::SeqCst),
                self.snapshot_load_calls.load(Ordering::SeqCst),
            )
        }
    }

    impl EventStore for ExactReadProbeStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            self.inner.append(pending)
        }

        fn append_with_disposition<'a>(
            &'a self,
            pending: PendingEvent,
        ) -> HarnessFuture<'a, EventAppendResult> {
            self.inner.append_with_disposition(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.event_page_calls.fetch_add(1, Ordering::SeqCst);
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }

        fn thread_accessible<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            tenant_id: Option<String>,
        ) -> HarnessFuture<'a, bool> {
            self.inner.thread_accessible(thread_id, tenant_id)
        }

        fn load_snapshot<'a>(
            &'a self,
            thread_id: &'a ThreadId,
        ) -> HarnessFuture<'a, Option<StateSnapshot>> {
            self.snapshot_load_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.load_snapshot(thread_id)
        }

        fn save_snapshot<'a>(&'a self, snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
            self.inner.save_snapshot(snapshot)
        }

        fn supports_agent_loop_wait_projection(&self) -> bool {
            true
        }

        fn scan_due_agent_loop_waits<'a>(
            &'a self,
            tenant_id: Option<String>,
            at_ms: u64,
            after: Option<AgentLoopDueCursor>,
            scan_limit: usize,
        ) -> HarnessFuture<'a, AgentLoopDueScanPage> {
            self.inner
                .scan_due_agent_loop_waits(tenant_id, at_ms, after, scan_limit)
        }

        fn event_by_id<'a>(
            &'a self,
            event_id: &'a EventId,
        ) -> HarnessFuture<'a, Option<StoredEvent>> {
            Box::pin(async move {
                let call = self.exact_read_calls.fetch_add(1, Ordering::SeqCst) + 1;
                let fault = (self.fault_on_call.load(Ordering::SeqCst) == call)
                    .then(|| self.fault.load(Ordering::SeqCst));
                if fault == Some(ExactReadFault::Missing as u8) {
                    return Ok(None);
                }
                let mut stored = self.inner.event_by_id(event_id).await?;
                let Some(stored) = stored.as_mut() else {
                    return Ok(None);
                };
                match fault {
                    Some(value) if value == ExactReadFault::WrongShape as u8 => {
                        stored.event = StateEvent::ThreadCreated {
                            created_at_ms: stored.recorded_at_ms,
                            tenant_id: None,
                        };
                    }
                    Some(value) if value == ExactReadFault::CrossThread as u8 => {
                        stored.thread_id =
                            ThreadId::from_string("fault-injected-cross-thread".to_owned());
                    }
                    _ => {}
                }
                Ok(Some(stored.clone()))
            })
        }
    }

    struct OversizedPageStore {
        events: Vec<StoredEvent>,
    }

    impl EventStore for OversizedPageStore {
        fn append<'a>(&'a self, _pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            Box::pin(async { Err(HarnessError::State("read-only test Event Store".to_owned())) })
        }

        fn events_page<'a>(
            &'a self,
            _thread_id: &'a ThreadId,
            _after_sequence: u64,
            _limit: usize,
            _max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            let events = self.events.clone();
            Box::pin(async move { Ok(events) })
        }
    }

    struct FailingSnapshotStore {
        inner: MemoryEventStore,
        panic_on_save: bool,
    }

    impl FailingSnapshotStore {
        fn new(panic_on_save: bool) -> Self {
            Self {
                inner: MemoryEventStore::new(),
                panic_on_save,
            }
        }
    }

    struct BlockingSnapshotStore {
        inner: MemoryEventStore,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl BlockingSnapshotStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            }
        }
    }

    impl EventStore for BlockingSnapshotStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }

        fn load_snapshot<'a>(
            &'a self,
            thread_id: &'a ThreadId,
        ) -> HarnessFuture<'a, Option<StateSnapshot>> {
            self.inner.load_snapshot(thread_id)
        }

        fn save_snapshot<'a>(&'a self, snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                self.inner.save_snapshot(snapshot).await
            })
        }
    }

    impl EventStore for FailingSnapshotStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }

        fn save_snapshot<'a>(&'a self, _snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
            let panic_on_save = self.panic_on_save;
            Box::pin(async move {
                assert!(!panic_on_save, "simulated snapshot provider panic");
                Err(HarnessError::State(
                    "simulated snapshot provider failure".to_owned(),
                ))
            })
        }
    }

    #[tokio::test]
    async fn event_ids_are_idempotent_but_cannot_change_content() {
        let store = MemoryEventStore::new();
        let event_id = EventId::generate();
        let thread_id = ThreadId::generate();
        let pending = PendingEvent {
            event_id,
            thread_id,
            expected_stream_version: 0,
            expected_stream_recovery_bytes: 0,
            recorded_at_ms: now_ms(),
            event: StateEvent::ThreadCreated {
                created_at_ms: now_ms(),
                tenant_id: None,
            },
        };

        let first = store.append(pending.clone()).await.expect("first append");
        let second = store
            .append(pending.clone())
            .await
            .expect("idempotent retry");
        assert_eq!(first, second);

        let mut changed = pending;
        changed.recorded_at_ms += 1;
        assert!(store.append(changed).await.is_err());
    }

    #[tokio::test]
    async fn completion_is_atomic_idempotent_and_recovers_an_ambiguous_append() {
        let state = StateEngine::new(Arc::new(CommitThenFailStore::new()));
        let thread = state.create_thread().await.expect("create Thread");
        let turn = state.start_turn(&thread.id).await.expect("start Turn");
        let error = state
            .finish_turn(&turn, TurnStatus::Completed)
            .await
            .expect_err("receipt-free completion must be forbidden");
        assert!(error.to_string().contains("CompletionReceipt"));

        let (candidate_turn, receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "authoritative answer",
        )
        .await;
        let mut mismatched_value = serde_json::to_value(&receipt).expect("receipt JSON");
        mismatched_value["source_thread_id"] = json!("another-source-thread");
        let mismatched_receipt: CompletionReceipt =
            serde_json::from_value(mismatched_value).expect("shape-valid mismatched receipt");
        let error = state
            .complete_turn(&candidate_turn, mismatched_receipt)
            .await
            .expect_err("direct writer must reject a foreign receipt source");
        assert!(error.to_string().contains("source Thread"));

        let receipt_sha256 = completion_receipt_sha256(&receipt).expect("receipt digest");
        let stored = state
            .complete_turn(&candidate_turn, receipt.clone())
            .await
            .expect("committed receipt must survive an ambiguous provider result");
        assert_eq!(
            stored.event_id.as_str(),
            format!("turn-completion-{receipt_sha256}")
        );
        assert!(matches!(
            &stored.event,
            StateEvent::TurnCompleted {
                turn_id,
                receipt: stored_receipt,
            } if turn_id == &turn.id && stored_receipt == &receipt
        ));
        assert!(
            super::validate_state_event_schema(&stored.event, 14)
                .expect_err("legacy schema cannot claim a receipt")
                .to_string()
                .contains("schema-14")
        );

        let retried = state
            .complete_turn(&candidate_turn, receipt.clone())
            .await
            .expect("same receipt retry");
        assert_eq!(retried, stored);
        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load completed Thread")
            .expect("Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Completed);
        assert_eq!(
            projected.turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );

        let other_turn = state
            .start_turn(&thread.id)
            .await
            .expect("start other Turn");
        let (_, other_receipt) = append_completion_candidate(
            &state,
            &other_turn,
            &AuthorityContext::local_process(),
            "different answer",
        )
        .await;
        let error = state
            .complete_turn(&candidate_turn, other_receipt)
            .await
            .expect_err("different receipt cannot replace committed truth");
        assert!(error.to_string().contains("different receipt"));
    }

    #[tokio::test]
    async fn completion_receipt_survives_snapshot_fork_archive_and_import() {
        let store = Arc::new(MemoryEventStore::new());
        let state = StateEngine::new(store.clone());
        let thread = state.create_thread().await.expect("create source Thread");
        let turn = state
            .start_turn(&thread.id)
            .await
            .expect("start source Turn");
        let (candidate_turn, receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "portable completed answer",
        )
        .await;
        state
            .complete_turn(&candidate_turn, receipt.clone())
            .await
            .expect("complete source Turn");

        let foreign_thread_id = ThreadId::from_static("foreign-direct-replay");
        let foreign_events = state
            .events(&thread.id)
            .await
            .expect("source events")
            .into_iter()
            .map(|mut stored| {
                stored.thread_id = foreign_thread_id.clone();
                stored
            })
            .collect::<Vec<_>>();
        let error = super::project_events(&foreign_events)
            .expect_err("foreign receipt requires explicit stream provenance");
        assert!(
            error
                .to_string()
                .contains("without fork or import provenance")
        );

        let snapshot = state
            .create_snapshot(&thread.id)
            .await
            .expect("snapshot completed source");
        assert_eq!(snapshot.stream_version(), 4);
        assert_eq!(
            snapshot.thread().turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );
        let recovered = StateEngine::new(store)
            .load_thread(&thread.id)
            .await
            .expect("recover snapshot")
            .expect("source Thread");
        assert_eq!(
            recovered.turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );

        let fork = state
            .fork_thread(
                &thread.id,
                ThreadId::from_static("receipt-fork"),
                Some(&turn.id),
            )
            .await
            .expect("fork receipt history");
        assert_eq!(fork.turns[0].completion_receipt.as_ref(), Some(&receipt));
        assert_eq!(receipt.source_thread_id(), &thread.id);
        assert_ne!(receipt.source_thread_id(), &fork.id);
        let fork_of_fork = state
            .fork_thread(
                &fork.id,
                ThreadId::from_static("receipt-fork-of-fork"),
                Some(&turn.id),
            )
            .await
            .expect("fork inherited receipt history");
        assert_eq!(
            fork_of_fork.turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );
        assert_ne!(receipt.source_thread_id(), &fork_of_fork.id);

        let archive = state
            .export_thread(&thread.id)
            .await
            .expect("export receipt");
        assert_eq!(archive.format_version, super::THREAD_ARCHIVE_FORMAT_VERSION);
        let imported = state
            .import_thread(&archive, ThreadId::from_static("receipt-import"))
            .await
            .expect("import receipt history");
        assert_eq!(
            imported.turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );
        assert_ne!(receipt.source_thread_id(), &imported.id);
    }

    #[test]
    fn schema_14_completion_remains_legacy_but_schema_15_rejects_it() {
        let thread_id = ThreadId::from_static("legacy-completed-thread");
        let turn_id = TurnId::from_static("legacy-completed-turn");
        let mut events = vec![
            StoredEvent {
                schema_version: 14,
                sequence: 1,
                event_id: EventId::from_static("legacy-completed-created"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 1,
                event: StateEvent::ThreadCreated {
                    created_at_ms: 1,
                    tenant_id: None,
                },
            },
            StoredEvent {
                schema_version: 14,
                sequence: 2,
                event_id: EventId::from_static("legacy-completed-started"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 2,
                event: StateEvent::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            },
            StoredEvent {
                schema_version: 14,
                sequence: 3,
                event_id: EventId::from_static("legacy-completed-finished"),
                thread_id,
                recorded_at_ms: 3,
                event: StateEvent::TurnFinished {
                    turn_id,
                    status: TurnStatus::Completed,
                },
            },
        ];
        let projected = super::project_events(&events)
            .expect("legacy completion projection")
            .expect("legacy Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Completed);
        assert_eq!(projected.turns[0].completion_receipt, None);

        events[2].schema_version = 15;
        let error = super::project_events(&events).expect_err("new schema must require a receipt");
        assert!(error.to_string().contains("CompletionReceipt"));
    }

    #[tokio::test]
    async fn memory_store_atomically_rejects_a_stream_version_race() {
        let store: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        assert_competing_turn_starts(store.clone(), store).await;
    }

    #[tokio::test]
    async fn stale_state_engine_head_is_safe_and_refreshes_before_rejection() {
        let store: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        let first = StateEngine::new(store.clone());
        let second = StateEngine::new(store);
        let thread = first.create_thread().await.expect("create thread");
        second
            .load_thread(&thread.id)
            .await
            .expect("prime stale head");

        let first_turn = first.start_turn(&thread.id).await.expect("first turn");
        let conflict = second
            .start_turn(&thread.id)
            .await
            .expect_err("stale writer must conflict");
        assert!(matches!(conflict, HarnessError::StateConflict { .. }));
        second
            .load_thread(&thread.id)
            .await
            .expect("cache running head");

        first
            .finish_turn(&first_turn, TurnStatus::Interrupted)
            .await
            .expect("finish first");
        let second_turn = second
            .start_turn(&thread.id)
            .await
            .expect("refresh should observe finished turn");
        assert_eq!(second_turn.status, TurnStatus::Running);
    }

    #[tokio::test]
    async fn separate_sqlite_connections_reject_a_stream_version_race() {
        let path = temp_database_path();
        let first: Arc<dyn EventStore> = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("first connection"),
        );
        let second: Arc<dyn EventStore> = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("second connection"),
        );
        assert_competing_turn_starts(first, second).await;
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_append_returns_the_event_rowid_after_auxiliary_metadata_writes() {
        let path = temp_database_path();
        let store = SqliteEventStore::open(&path).await.expect("open database");
        let first_thread = ThreadId::from_static("rowid-first-thread");
        let first_created = store
            .append(PendingEvent {
                event_id: EventId::generate(),
                thread_id: first_thread.clone(),
                expected_stream_version: 0,
                expected_stream_recovery_bytes: 0,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            })
            .await
            .expect("create first stream");
        let first_charge =
            super::stored_event_recovery_bytes(&first_created).expect("first event charge");
        let first_started = store
            .append(PendingEvent {
                event_id: EventId::generate(),
                thread_id: first_thread,
                expected_stream_version: 1,
                expected_stream_recovery_bytes: first_charge,
                recorded_at_ms: now_ms(),
                event: StateEvent::TurnStarted {
                    turn_id: TurnId::generate(),
                },
            })
            .await
            .expect("grow first stream");
        assert_eq!(first_started.sequence, 2);

        let second_thread = ThreadId::from_static("rowid-second-thread");
        let second_created = store
            .append(PendingEvent {
                event_id: EventId::generate(),
                thread_id: second_thread.clone(),
                expected_stream_version: 0,
                expected_stream_recovery_bytes: 0,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            })
            .await
            .expect("create second stream");
        assert_eq!(second_created.sequence, 3);
        let persisted = store
            .events_page(
                &second_thread,
                0,
                1,
                super::MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
            )
            .await
            .expect("read second stream");
        assert_eq!(persisted[0].sequence, second_created.sequence);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn memory_event_pages_are_bounded_and_cursor_ordered() {
        let store: Arc<dyn EventStore> = Arc::new(MemoryEventStore::new());
        assert_event_pages(store).await;
    }

    #[tokio::test]
    async fn state_authority_rejects_oversized_events_and_unbounded_pages() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "x".repeat(super::MAX_STATE_EVENT_BYTES + 1),
                }),
            )
            .await
            .expect_err("oversized event");
        assert!(matches!(error, HarnessError::State(_)));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 2);

        let mut nested = serde_json::Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            nested = serde_json::Value::Array(vec![nested]);
        }
        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::ToolResult {
                    call_id: "deep-call".to_owned(),
                    output: nested,
                    is_error: false,
                    connector_evidence: Vec::new(),
                }),
            )
            .await
            .expect_err("deep event");
        assert!(error.to_string().contains("depth or node count"));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 2);
        assert!(state.events_page(&thread.id, 0, 0).await.is_err());
        assert!(
            state
                .events_page(&thread.id, 0, super::MAX_STATE_EVENT_PAGE + 1)
                .await
                .is_err()
        );

        let store = MemoryEventStore::new();
        let error = store
            .append(PendingEvent {
                event_id: EventId::generate(),
                thread_id: ThreadId::generate(),
                expected_stream_version: super::MAX_STATE_RECOVERY_EVENTS as u64,
                expected_stream_recovery_bytes: 0,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            })
            .await
            .expect_err("retention boundary");
        assert!(matches!(error, HarnessError::State(_)));
    }

    #[tokio::test]
    async fn state_atomically_projects_one_ordered_tool_call_batch() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        let batch_id = ToolCallBatchId::from_static("tool-batch-test");
        let calls = ["call-1", "call-2"]
            .into_iter()
            .enumerate()
            .map(|(index, call_id)| {
                Item::new(ItemKind::ToolCall {
                    model_id: Some("test/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: call_id.to_owned(),
                    name: "echo".to_owned(),
                    input: json!({"index": index}),
                    batch: Some(ToolCallBatch {
                        id: batch_id.clone(),
                        index,
                        size: 2,
                    }),
                })
            })
            .collect::<Vec<_>>();

        state
            .append_tool_calls_as(
                &turn,
                calls.clone(),
                &crate::AuthorityContext::local_process(),
            )
            .await
            .expect("append atomic batch");
        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].items, calls);
        let events = state.events(&thread.id).await.expect("events");
        assert_eq!(events.len(), 3);
        assert!(matches!(
            &events[2].event,
            StateEvent::ToolCallsAppended {
                turn_id,
                calls: persisted,
            } if turn_id == &turn.id && persisted == &calls
        ));

        let malformed = vec![
            Item::new(ItemKind::ToolCall {
                model_id: Some("test/model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                call_id: "bad-1".to_owned(),
                name: "echo".to_owned(),
                input: json!({}),
                batch: Some(ToolCallBatch {
                    id: ToolCallBatchId::from_static("bad-batch"),
                    index: 0,
                    size: 2,
                }),
            }),
            Item::new(ItemKind::ToolCall {
                model_id: Some("test/model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                call_id: "bad-2".to_owned(),
                name: "echo".to_owned(),
                input: json!({}),
                batch: Some(ToolCallBatch {
                    id: ToolCallBatchId::from_static("bad-batch"),
                    index: 0,
                    size: 2,
                }),
            }),
        ];
        let error = state
            .append_tool_calls_as(&turn, malformed, &crate::AuthorityContext::local_process())
            .await
            .expect_err("reject inconsistent positions");
        assert!(error.to_string().contains("inconsistent"));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 3);
    }

    #[tokio::test]
    async fn state_authority_enforces_steering_correlation_order_and_completion_fence() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        let steering_id = SteeringId::from_static("steering-first");

        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringApplied {
                    steering_id: steering_id.clone(),
                    content: "first".to_owned(),
                }),
            )
            .await
            .expect_err("application without a durable queue record");
        assert!(error.to_string().contains("no pending queue record"));

        state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringQueued {
                    steering_id: steering_id.clone(),
                    submitted_by: ActorIdentity::LocalProcess,
                    content: "first".to_owned(),
                }),
            )
            .await
            .expect("queue steering");

        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringQueued {
                    steering_id: steering_id.clone(),
                    submitted_by: ActorIdentity::LocalProcess,
                    content: "duplicate".to_owned(),
                }),
            )
            .await
            .expect_err("duplicate identity");
        assert!(error.to_string().contains("duplicate steering identity"));

        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringApplied {
                    steering_id: SteeringId::from_static("steering-second"),
                    content: "first".to_owned(),
                }),
            )
            .await
            .expect_err("out-of-order application");
        assert!(error.to_string().contains("violates queue order"));

        let error = state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringApplied {
                    steering_id: steering_id.clone(),
                    content: "changed".to_owned(),
                }),
            )
            .await
            .expect_err("content mutation");
        assert!(error.to_string().contains("changed its queued content"));

        let (pending_candidate, pending_receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "stale while Steering is pending",
        )
        .await;
        let error = state
            .complete_turn(&pending_candidate, pending_receipt)
            .await
            .expect_err("completion with pending steering");
        assert!(error.to_string().contains("with unapplied steering"));

        state
            .append_item(
                &turn,
                Item::new(ItemKind::SteeringApplied {
                    steering_id,
                    content: "first".to_owned(),
                }),
            )
            .await
            .expect("apply matching steering");
        let (final_candidate, receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "answer after Steering",
        )
        .await;
        state
            .complete_turn(&final_candidate, receipt.clone())
            .await
            .expect("complete after steering application");

        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load thread")
            .expect("thread exists");
        assert_eq!(projected.turns[0].items.len(), 4);
        assert_eq!(projected.turns[0].status, TurnStatus::Completed);
        assert_eq!(
            projected.turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );
    }

    #[tokio::test]
    async fn state_engine_revalidates_untrusted_event_store_results() {
        let state = StateEngine::new(Arc::new(LyingAppendStore));
        assert!(state.create_thread().await.is_err());
        let state = StateEngine::new(Arc::new(LegacyLabelAppendStore));
        assert!(
            state.create_thread().await.is_err(),
            "a current writer must reject predecessor-labeled append results"
        );

        let thread_id = ThreadId::from_static("thread-test");
        let events = [1_u64, 2]
            .into_iter()
            .map(|sequence| StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence,
                event_id: EventId::from_string(format!("event-{sequence}")),
                thread_id: thread_id.clone(),
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            })
            .collect();
        let state = StateEngine::new(Arc::new(OversizedPageStore { events }));
        let error = state
            .events_page(&thread_id, 0, 1)
            .await
            .expect_err("provider page overflow");
        assert!(matches!(error, HarnessError::State(_)));
    }

    #[tokio::test]
    async fn snapshot_replays_only_newer_tail_and_corruption_falls_back() {
        let store = Arc::new(MemoryEventStore::new());
        let state = StateEngine::new(store.clone());
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "before snapshot".to_owned(),
                }),
            )
            .await
            .expect("append before snapshot");
        state
            .set_thread_name(&thread.id, Some("First name".to_owned()))
            .await
            .expect("first name");
        state
            .set_thread_name(&thread.id, Some("Snapshot name".to_owned()))
            .await
            .expect("snapshot name");
        let snapshot = state
            .create_snapshot(&thread.id)
            .await
            .expect("create snapshot");
        assert_eq!(snapshot.stream_version(), 5);
        assert_eq!(snapshot.metadata_events.len(), 2);

        state
            .set_thread_name(&thread.id, Some("Tail name".to_owned()))
            .await
            .expect("tail name");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "after snapshot".to_owned(),
                }),
            )
            .await
            .expect("append tail");
        state
            .finish_turn(&turn, TurnStatus::Interrupted)
            .await
            .expect("finish tail");

        let loaded = StateEngine::new(store.clone())
            .load_thread(&thread.id)
            .await
            .expect("load from snapshot")
            .expect("thread");
        assert_eq!(loaded.turns[0].items.len(), 2);
        assert_eq!(loaded.turns[0].status, TurnStatus::Interrupted);
        assert_eq!(loaded.name.as_deref(), Some("Tail name"));

        store
            .data
            .lock()
            .await
            .snapshots
            .get_mut(&thread.id)
            .expect("stored snapshot")
            .projection_sha256 = "0".repeat(64);
        let loaded = StateEngine::new(store)
            .load_thread(&thread.id)
            .await
            .expect("corrupt snapshot must fall back")
            .expect("thread");
        assert_eq!(loaded.turns[0].items.len(), 2);
    }

    #[test]
    fn automatic_snapshot_policy_rejects_unbounded_values() {
        assert!(SnapshotMaintenanceConfig::new(0, 1).is_err());
        assert!(SnapshotMaintenanceConfig::new(1, 0).is_err());
        assert!(SnapshotMaintenanceConfig::new(1, 65).is_err());
        assert!(SnapshotMaintenanceConfig::new(1_000_001, 1).is_err());
        let config = SnapshotMaintenanceConfig::new(100, 4).expect("bounded policy");
        assert_eq!(config.every_events(), 100);
        assert_eq!(config.max_concurrency(), 4);
    }

    #[tokio::test]
    async fn thread_capacity_exposes_stable_pressure_boundaries() {
        assert_eq!(
            super::state_capacity(799_999, 0).level,
            StateCapacityLevel::Healthy
        );
        assert_eq!(
            super::state_capacity(800_000, 0).level,
            StateCapacityLevel::Warning
        );
        assert_eq!(
            super::state_capacity(950_000, 0).level,
            StateCapacityLevel::Critical
        );
        let terminal_only = super::state_capacity(999_999, 0);
        assert_eq!(terminal_only.level, StateCapacityLevel::TerminalOnly);
        assert_eq!(terminal_only.remaining_events, 1);
        assert_eq!(terminal_only.general_events_remaining, 0);
        assert_eq!(terminal_only.terminal_event_reserve, 1);
        let exhausted = super::state_capacity(1_000_000, 0);
        assert_eq!(exhausted.level, StateCapacityLevel::Exhausted);
        assert_eq!(exhausted.remaining_events, 0);
        assert_eq!(exhausted.terminal_event_reserve, 0);
        let byte_critical = super::state_capacity(1, super::STATE_RECOVERY_CAPACITY_CRITICAL_AT);
        assert_eq!(byte_critical.level, StateCapacityLevel::Critical);
        let byte_terminal = super::state_capacity(
            1,
            super::STATE_THREAD_RECOVERY_BYTE_LIMIT - super::STATE_TERMINAL_RECOVERY_BYTE_RESERVE,
        );
        assert_eq!(byte_terminal.level, StateCapacityLevel::TerminalOnly);
        assert_eq!(byte_terminal.general_recovery_bytes_remaining, 0);

        let thread_id = ThreadId::from_static("capacity-thread");
        let turn_id = TurnId::from_static("capacity-turn");
        let non_terminal = PendingEvent {
            event_id: EventId::generate(),
            thread_id: thread_id.clone(),
            expected_stream_version: 999_999,
            expected_stream_recovery_bytes: 0,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnStarted {
                turn_id: TurnId::generate(),
            },
        };
        assert!(super::validate_pending_event(&non_terminal).is_err());
        let terminal = PendingEvent {
            event_id: EventId::generate(),
            thread_id,
            expected_stream_version: 999_999,
            expected_stream_recovery_bytes: 0,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnFinished {
                turn_id,
                status: TurnStatus::Interrupted,
            },
        };
        super::validate_pending_event(&terminal).expect("terminal reserve remains usable");

        let terminal_event = StateEvent::TurnFinished {
            // Backslashes and quotes maximize JSON expansion while remaining a
            // valid 256-byte State identity.
            turn_id: TurnId::from_string("\\\"".repeat(128)),
            status: TurnStatus::Interrupted,
        };
        let terminal_charge = super::encode_state_event(&terminal_event)
            .expect("terminal charge")
            .recovery_bytes;
        assert!(terminal_charge <= super::STATE_TERMINAL_RECOVERY_BYTE_RESERVE);
        let byte_terminal = PendingEvent {
            event_id: EventId::generate(),
            thread_id: ThreadId::from_static("byte-reserve-thread"),
            expected_stream_version: 1,
            expected_stream_recovery_bytes: super::STATE_THREAD_RECOVERY_BYTE_LIMIT
                - terminal_charge,
            recorded_at_ms: now_ms(),
            event: terminal_event,
        };
        super::validate_pending_event(&byte_terminal)
            .expect("terminal byte reserve remains usable");
        let byte_general = PendingEvent {
            event_id: EventId::generate(),
            thread_id: ThreadId::from_static("byte-general-thread"),
            expected_stream_version: 1,
            expected_stream_recovery_bytes: super::STATE_THREAD_RECOVERY_BYTE_LIMIT
                - super::STATE_TERMINAL_RECOVERY_BYTE_RESERVE,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnStarted {
                turn_id: TurnId::generate(),
            },
        };
        assert!(super::validate_pending_event(&byte_general).is_err());

        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let missing = ThreadId::from_static("missing-thread");
        assert_eq!(
            state
                .thread_capacity(&missing)
                .await
                .expect("missing capacity"),
            None
        );
        let thread = state.create_thread().await.expect("create thread");
        let capacity = state
            .thread_capacity(&thread.id)
            .await
            .expect("thread capacity")
            .expect("existing thread");
        assert_eq!(capacity.used_events, 1);
        assert_eq!(capacity.event_limit, 1_000_000);
        assert_eq!(capacity.remaining_events, 999_999);
        assert_eq!(capacity.general_events_remaining, 999_998);
        assert_eq!(capacity.terminal_event_reserve, 1);
        assert!(capacity.used_recovery_bytes > 0);
        assert_eq!(
            capacity.recovery_byte_limit,
            super::STATE_THREAD_RECOVERY_BYTE_LIMIT
        );
        assert_eq!(capacity.level, StateCapacityLevel::Healthy);
    }

    #[tokio::test]
    async fn terminal_turn_automatically_creates_and_drains_snapshot() {
        let store = Arc::new(MemoryEventStore::new());
        let state = StateEngine::new(store.clone()).with_snapshot_maintenance(
            SnapshotMaintenanceConfig::new(1, 1).expect("snapshot policy"),
        );
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        let (candidate_turn, receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "snapshot completion",
        )
        .await;
        state
            .complete_turn(&candidate_turn, receipt.clone())
            .await
            .expect("journal settlement must not wait for snapshot");
        assert!(
            state
                .drain_snapshot_maintenance(Duration::from_secs(1))
                .await
        );

        let snapshot = store
            .load_snapshot(&thread.id)
            .await
            .expect("load snapshot")
            .expect("automatic snapshot");
        assert_eq!(snapshot.stream_version(), 4);
        assert_eq!(
            snapshot.thread().turns[0].completion_receipt.as_ref(),
            Some(&receipt)
        );
        let stats = state
            .snapshot_maintenance_stats()
            .expect("maintenance enabled");
        assert_eq!(stats.scheduled, 1);
        assert_eq!(stats.created, 1);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.active, 0);
        assert!(stats.last_created_at_ms.is_some());
    }

    #[tokio::test]
    async fn automatic_snapshot_scheduler_sheds_work_at_capacity() {
        let store = Arc::new(BlockingSnapshotStore::new());
        let state = StateEngine::new(store.clone()).with_snapshot_maintenance(
            SnapshotMaintenanceConfig::new(1, 1).expect("snapshot policy"),
        );
        let first_thread = state.create_thread().await.expect("first thread");
        let first_turn = state
            .start_turn(&first_thread.id)
            .await
            .expect("first turn");
        let second_thread = state.create_thread().await.expect("second thread");
        let second_turn = state
            .start_turn(&second_thread.id)
            .await
            .expect("second turn");

        state
            .finish_turn(&first_turn, TurnStatus::Interrupted)
            .await
            .expect("settle first turn");
        tokio::time::timeout(Duration::from_secs(1), store.entered.notified())
            .await
            .expect("first snapshot worker entered");
        state
            .finish_turn(&second_turn, TurnStatus::Interrupted)
            .await
            .expect("settle second turn without queueing");

        let busy = state
            .snapshot_maintenance_stats()
            .expect("maintenance enabled");
        assert_eq!(busy.scheduled, 1);
        assert_eq!(busy.skipped_capacity, 1);
        assert_eq!(busy.active, 1);

        store.release.notify_one();
        assert!(
            state
                .drain_snapshot_maintenance(Duration::from_secs(1))
                .await
        );
        assert_eq!(
            state
                .snapshot_maintenance_stats()
                .expect("maintenance enabled")
                .active,
            0
        );
    }

    #[tokio::test]
    async fn automatic_snapshot_errors_and_panics_never_reverse_turn_settlement() {
        for (panic_on_save, expected) in [
            (false, SnapshotMaintenanceFailure::Operation),
            (true, SnapshotMaintenanceFailure::WorkerPanicked),
        ] {
            let state = StateEngine::new(Arc::new(FailingSnapshotStore::new(panic_on_save)))
                .with_snapshot_maintenance(
                    SnapshotMaintenanceConfig::new(1, 1).expect("snapshot policy"),
                );
            let thread = state.create_thread().await.expect("create thread");
            let turn = state.start_turn(&thread.id).await.expect("start turn");
            state
                .finish_turn(&turn, TurnStatus::Interrupted)
                .await
                .expect("journal settlement survives snapshot failure");
            assert!(
                state
                    .drain_snapshot_maintenance(Duration::from_secs(1))
                    .await
            );

            let loaded = state
                .load_thread(&thread.id)
                .await
                .expect("load settled thread")
                .expect("thread");
            assert_eq!(loaded.turns[0].status, TurnStatus::Interrupted);
            let stats = state
                .snapshot_maintenance_stats()
                .expect("maintenance enabled");
            assert_eq!(stats.created, 0);
            assert_eq!(stats.failed, 1);
            assert_eq!(stats.last_failure, Some(expected));
            assert!(stats.last_failure_at_ms.is_some());
        }
    }

    #[tokio::test]
    async fn sqlite_snapshot_survives_reopen_with_authoritative_tail() {
        let path = temp_database_path();
        let store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("open snapshot database"),
        );
        let state = StateEngine::new(store);
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "snapshot body".to_owned(),
                }),
            )
            .await
            .expect("append body");
        state
            .set_thread_name(&thread.id, Some("Persistent name".to_owned()))
            .await
            .expect("name Thread");
        state
            .create_snapshot(&thread.id)
            .await
            .expect("persist snapshot");
        let (candidate_turn, receipt) = append_completion_candidate(
            &state,
            &turn,
            &AuthorityContext::local_process(),
            "persistent completion",
        )
        .await;
        state
            .complete_turn(&candidate_turn, receipt.clone())
            .await
            .expect("append authoritative tail");
        drop(state);

        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen snapshot database"),
        ));
        let loaded = reopened
            .load_thread(&thread.id)
            .await
            .expect("load snapshot plus tail")
            .expect("thread");
        assert_eq!(loaded.turns.len(), 1);
        assert_eq!(loaded.turns[0].status, TurnStatus::Completed);
        assert_eq!(loaded.turns[0].items.len(), 2);
        assert_eq!(loaded.turns[0].completion_receipt.as_ref(), Some(&receipt));
        assert_eq!(loaded.name.as_deref(), Some("Persistent name"));
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn checkpoint_labels_are_bounded_before_state_growth() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("create thread");
        let error = state
            .create_checkpoint(&thread.id, None, Some(String::new()))
            .await
            .expect_err("empty checkpoint label");
        assert!(matches!(error, HarnessError::State(_)));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 1);
    }

    #[tokio::test]
    async fn sqlite_event_pages_are_bounded_and_cursor_ordered() {
        let path = temp_database_path();
        let store: Arc<dyn EventStore> =
            Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        assert_event_pages(store).await;
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn recent_thread_pages_are_bounded_and_store_consistent() {
        assert_recent_threads(StateEngine::new(Arc::new(MemoryEventStore::new()))).await;

        let path = temp_database_path();
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        assert_recent_threads(state).await;
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn thread_tenant_ownership_fences_reads_mutations_and_reopen() {
        let tenant_a = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "operator-a".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant A authority");
        let tenant_b = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "operator-b".to_owned(),
            },
            Some("tenant-b".to_owned()),
        )
        .expect("tenant B authority");

        let memory = StateEngine::new(Arc::new(MemoryEventStore::new()));
        assert_tenant_fencing(&memory, &tenant_a, &tenant_b).await;

        let path = temp_database_path();
        let sqlite = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        let tenant_a_thread = assert_tenant_fencing(&sqlite, &tenant_a, &tenant_b).await;
        drop(sqlite);

        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen tenant database"),
        ));
        assert!(
            reopened
                .load_thread_as(&tenant_a_thread, &tenant_a)
                .await
                .expect("tenant A reopen read")
                .is_some()
        );
        assert_eq!(
            reopened
                .load_thread_as(&tenant_a_thread, &tenant_b)
                .await
                .expect("tenant B denied reopen read"),
            None
        );
        remove_database_files(&path);
    }

    async fn assert_tenant_fencing(
        state: &StateEngine,
        tenant_a: &AuthorityContext,
        tenant_b: &AuthorityContext,
    ) -> ThreadId {
        let thread_a = state
            .create_thread_as(tenant_a)
            .await
            .expect("create tenant A Thread");
        let thread_b = state
            .create_thread_as(tenant_b)
            .await
            .expect("create tenant B Thread");
        assert_eq!(thread_a.tenant_id(), Some("tenant-a"));
        assert_eq!(thread_b.tenant_id(), Some("tenant-b"));
        assert_eq!(
            state
                .load_thread_as(&thread_a.id, tenant_b)
                .await
                .expect("cross-tenant read is hidden"),
            None
        );
        assert!(
            state
                .set_thread_name_as(&thread_a.id, Some("forbidden".to_owned()), tenant_b)
                .await
                .is_err()
        );
        assert!(
            state
                .events_page_as(&thread_a.id, 0, 1, tenant_b)
                .await
                .is_err()
        );

        let page_a = state
            .list_threads_as(None, 64, tenant_a)
            .await
            .expect("list tenant A");
        assert_eq!(page_a.threads.len(), 1);
        assert_eq!(page_a.threads[0].thread_id, thread_a.id);
        assert_eq!(page_a.threads[0].tenant_id.as_deref(), Some("tenant-a"));
        let page_b = state
            .list_threads_as(None, 64, tenant_b)
            .await
            .expect("list tenant B");
        assert_eq!(page_b.threads.len(), 1);
        assert_eq!(page_b.threads[0].thread_id, thread_b.id);

        let turn = state
            .start_turn_as(&thread_a.id, tenant_a)
            .await
            .expect("start tenant A Turn");
        assert!(
            state
                .finish_turn_as(&turn, TurnStatus::Interrupted, tenant_b)
                .await
                .is_err()
        );
        state
            .finish_turn_as(&turn, TurnStatus::Interrupted, tenant_a)
            .await
            .expect("finish tenant A Turn");

        let child = state
            .fork_thread_as(tenant_a, &thread_a.id, ThreadId::generate(), Some(&turn.id))
            .await
            .expect("fork inside tenant A");
        assert_eq!(child.tenant_id(), Some("tenant-a"));
        assert_eq!(
            state
                .load_thread_as(&child.id, tenant_b)
                .await
                .expect("cross-tenant child read hidden"),
            None
        );
        assert!(
            state
                .export_thread_as(&thread_a.id, tenant_b)
                .await
                .is_err()
        );
        let archive = state
            .export_thread_as(&thread_a.id, tenant_a)
            .await
            .expect("export inside tenant A");
        let imported = state
            .import_thread_as(&archive, ThreadId::generate(), tenant_b)
            .await
            .expect("import rebinds archive into tenant B");
        assert_eq!(imported.tenant_id(), Some("tenant-b"));
        assert_eq!(
            state
                .load_thread_as(&imported.id, tenant_a)
                .await
                .expect("source tenant cannot read imported copy"),
            None
        );
        thread_a.id
    }

    #[tokio::test]
    async fn execution_binding_is_single_tenant_exact_and_archive_rebind_safe() {
        let tenant_a = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "operator-a".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant A authority");
        let tenant_b = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "operator-b".to_owned(),
            },
            Some("tenant-b".to_owned()),
        )
        .expect("tenant B authority");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state
            .create_thread_as(&tenant_a)
            .await
            .expect("create Thread");
        let turn = state
            .start_turn_as(&thread.id, &tenant_a)
            .await
            .expect("start Turn");
        let tenant_b_binding = crate::ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            1,
            Some("tenant-b".to_owned()),
        )
        .expect("tenant B binding");
        assert!(
            state
                .append_item_as(
                    &turn,
                    Item::new(ItemKind::ExecutionBinding {
                        bound_by: tenant_a.actor().clone(),
                        binding: tenant_b_binding,
                    }),
                    &tenant_a,
                )
                .await
                .expect_err("mismatched tenant")
                .to_string()
                .contains("tenant differs")
        );

        let binding = crate::ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            1,
            Some("tenant-a".to_owned()),
        )
        .expect("tenant A binding");
        assert!(
            state
                .append_item_as(
                    &turn,
                    Item::new(ItemKind::ExecutionBinding {
                        bound_by: ActorIdentity::LocalProcess,
                        binding: binding.clone(),
                    }),
                    &tenant_a,
                )
                .await
                .expect_err("forged actor")
                .to_string()
                .contains("actor differs")
        );
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ExecutionBinding {
                    bound_by: tenant_a.actor().clone(),
                    binding: binding.clone(),
                }),
                &tenant_a,
            )
            .await
            .expect("append binding");
        assert!(
            state
                .append_item_as(
                    &turn,
                    Item::new(ItemKind::ExecutionBinding {
                        bound_by: tenant_a.actor().clone(),
                        binding,
                    }),
                    &tenant_a,
                )
                .await
                .expect_err("duplicate binding")
                .to_string()
                .contains("already has")
        );
        state
            .finish_turn_as(&turn, TurnStatus::Interrupted, &tenant_a)
            .await
            .expect("finish Turn");
        let archive = state
            .export_thread_as(&thread.id, &tenant_a)
            .await
            .expect("export archive");
        let imported = state
            .import_thread_as(&archive, ThreadId::generate(), &tenant_a)
            .await
            .expect("same-tenant import");
        assert_eq!(imported.tenant_id(), Some("tenant-a"));
        let error = state
            .import_thread_as(&archive, ThreadId::generate(), &tenant_b)
            .await
            .expect_err("bound archive cannot change tenant");
        assert!(error.to_string().contains("cannot rebind"));
    }

    #[tokio::test]
    async fn thread_fork_is_terminal_idempotent_and_independent_across_stores() {
        assert_thread_fork(StateEngine::new(Arc::new(MemoryEventStore::new()))).await;

        let path = temp_database_path();
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        let child_id = assert_thread_fork(state.clone()).await;
        drop(state);

        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen fork database"),
        ));
        let child = reopened
            .load_thread(&child_id)
            .await
            .expect("load reopened child")
            .expect("child exists");
        assert!(child.lineage.is_some());
        assert_eq!(child.turns.len(), 2);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn thread_archive_is_integrity_bound_idempotent_and_portable_across_stores() {
        assert_thread_archive(StateEngine::new(Arc::new(MemoryEventStore::new()))).await;

        let path = temp_database_path();
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        let target_id = assert_thread_archive(state.clone()).await;
        drop(state);

        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen archive database"),
        ));
        let imported = reopened
            .load_thread(&target_id)
            .await
            .expect("load imported Thread")
            .expect("imported Thread");
        assert!(imported.import_origin.is_some());
        assert!(imported.lineage.is_none());
        assert_eq!(imported.turns.len(), 2);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_atomic_stream_failure_leaves_no_partial_child() {
        let path = temp_database_path();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let parent = state.create_thread().await.expect("create parent");
        let source = state.events(&parent.id).await.expect("source events");
        let child_id = ThreadId::from_static("atomic-rollback-child");
        let turn_id = TurnId::from_static("atomic-rollback-turn");
        let events = vec![
            NewStreamEvent {
                event_id: EventId::from_static("atomic-child-created"),
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            },
            NewStreamEvent {
                event_id: EventId::from_static("atomic-child-lineage"),
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadForked {
                    lineage: ThreadLineage {
                        parent_thread_id: parent.id,
                        parent_through_sequence: source[0].sequence,
                        parent_stream_version: 1,
                        parent_events_sha256: "0".repeat(64),
                    },
                },
            },
            NewStreamEvent {
                event_id: source[0].event_id.clone(),
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                recorded_at_ms: now_ms(),
                event: StateEvent::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            },
            NewStreamEvent {
                event_id: EventId::from_static("atomic-child-finished"),
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                recorded_at_ms: now_ms(),
                event: StateEvent::TurnFinished {
                    turn_id,
                    status: TurnStatus::Interrupted,
                },
            },
        ];
        store
            .create_stream_atomic(child_id.clone(), events)
            .await
            .expect_err("global Event identity collision");
        assert!(
            state
                .load_thread(&child_id)
                .await
                .expect("load child after rollback")
                .is_none()
        );
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_rejects_thread_name_projection_drift() {
        let path = temp_database_path();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let thread = state.create_thread().await.expect("create Thread");
        state
            .set_thread_name(&thread.id, Some("Authoritative".to_owned()))
            .await
            .expect("name Thread");
        drop(state);
        drop(store);

        rusqlite::Connection::open(&path)
            .expect("open raw database")
            .execute(
                "UPDATE streams SET name = 'Drifted' WHERE thread_id = ?1",
                [thread.id.as_str()],
            )
            .expect("tamper projection");
        let error = SqliteEventStore::open(&path)
            .await
            .err()
            .expect("projection drift must fail closed");
        assert!(
            error
                .to_string()
                .contains("Thread names do not match authoritative events")
        );
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_rejects_thread_tenant_projection_drift() {
        let path = temp_database_path();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "operator".to_owned(),
            },
            Some("tenant-authoritative".to_owned()),
        )
        .expect("tenant authority");
        let thread = state
            .create_thread_as(&authority)
            .await
            .expect("create tenant Thread");
        drop(state);
        drop(store);

        rusqlite::Connection::open(&path)
            .expect("open raw database")
            .execute(
                "UPDATE streams SET tenant_id = 'tenant-drifted' WHERE thread_id = ?1",
                [thread.id.as_str()],
            )
            .expect("tamper tenant projection");
        let error = SqliteEventStore::open(&path)
            .await
            .err()
            .expect("tenant projection drift must fail closed");
        assert!(
            error
                .to_string()
                .contains("Thread tenants do not match authoritative creation events")
        );
        remove_database_files(&path);
    }

    async fn assert_recent_threads(state: StateEngine) {
        assert!(state.supports_thread_listing());
        let first = state.create_thread().await.expect("create first Thread");
        let second = state.create_thread().await.expect("create second Thread");
        state
            .set_thread_name(&first.id, Some("First Thread".to_owned()))
            .await
            .expect("name first Thread");
        state
            .create_checkpoint(&first.id, None, Some("recent".to_owned()))
            .await
            .expect("update first Thread");

        let page = state.list_threads(None, 1).await.expect("recent page");
        assert_eq!(page.threads[0].thread_id, first.id);
        assert_eq!(page.threads[0].name.as_deref(), Some("First Thread"));
        assert!(page.has_more);
        let cursor = page.next_before_sequence.expect("older cursor");

        let older = state
            .list_threads(Some(cursor), 1)
            .await
            .expect("older page");
        assert_eq!(older.threads[0].thread_id, second.id);
        assert_eq!(older.threads[0].name, None);
        assert!(!older.has_more);
        assert!(older.next_before_sequence.is_none());

        state
            .set_thread_name(&first.id, None)
            .await
            .expect("clear first Thread name");
        assert_eq!(
            state
                .load_thread(&first.id)
                .await
                .expect("load named Thread")
                .expect("named Thread")
                .name,
            None
        );
        let event_count = state.events(&first.id).await.expect("events").len();
        let error = state
            .set_thread_name(&first.id, Some(" padded ".to_owned()))
            .await
            .expect_err("reject non-canonical name");
        assert!(matches!(error, HarnessError::State(_)));
        assert_eq!(
            state.events(&first.id).await.expect("events").len(),
            event_count
        );
    }

    async fn assert_thread_fork(state: StateEngine) -> ThreadId {
        assert!(state.supports_thread_fork());
        let parent = state.create_thread().await.expect("create parent");
        state
            .set_thread_name(&parent.id, Some("Parent only".to_owned()))
            .await
            .expect("name parent");
        let first = state.start_turn(&parent.id).await.expect("first turn");
        let first_item = Item::new(ItemKind::UserMessage {
            content: "shared history".to_owned(),
        });
        state
            .append_item(&first, first_item.clone())
            .await
            .expect("first item");
        state
            .finish_turn(&first, TurnStatus::Interrupted)
            .await
            .expect("finish first");
        state
            .create_checkpoint(
                &parent.id,
                Some(first.id.clone()),
                Some("parent only".to_owned()),
            )
            .await
            .expect("parent checkpoint");
        let second = state.start_turn(&parent.id).await.expect("second turn");
        state
            .append_item(
                &second,
                Item::new(ItemKind::UserMessage {
                    content: "later history".to_owned(),
                }),
            )
            .await
            .expect("second item");
        state
            .finish_turn(&second, TurnStatus::Interrupted)
            .await
            .expect("finish second");

        let child_id = ThreadId::from_static("fork-child");
        let child = state
            .fork_thread(&parent.id, child_id.clone(), Some(&first.id))
            .await
            .expect("fork first terminal turn");
        assert_eq!(child.id, child_id);
        assert_eq!(child.name, None);
        assert!(child.checkpoints.is_empty());
        assert_eq!(child.turns.len(), 1);
        assert_eq!(child.turns[0].id, first.id);
        assert_eq!(child.turns[0].items[0].id, first_item.id);
        let lineage = child.lineage.as_ref().expect("fork lineage");
        assert_eq!(lineage.parent_thread_id, parent.id);
        assert_eq!(lineage.parent_stream_version, 5);
        assert_eq!(lineage.parent_events_sha256.len(), 64);
        let summaries = state
            .list_threads(None, 8)
            .await
            .expect("list forked Threads");
        let child_summary = summaries
            .threads
            .iter()
            .find(|summary| summary.thread_id == child_id)
            .expect("fork child summary");
        assert_eq!(child_summary.lineage.as_ref(), Some(lineage));
        assert!(
            summaries
                .threads
                .iter()
                .find(|summary| summary.thread_id == parent.id)
                .is_some_and(|summary| summary.lineage.is_none())
        );

        let snapshot = state
            .create_snapshot(&child_id)
            .await
            .expect("snapshot fork");
        assert_eq!(snapshot.stream_version(), 5);
        let active = state
            .start_turn(&parent.id)
            .await
            .expect("active parent turn");
        let latest_error = state
            .fork_thread(
                &parent.id,
                ThreadId::from_static("fork-active-latest"),
                None,
            )
            .await
            .expect_err("latest active parent must be rejected");
        assert!(latest_error.to_string().contains("Turn is running"));

        let retried = state
            .fork_thread(&parent.id, child_id.clone(), Some(&first.id))
            .await
            .expect("idempotent retry");
        assert_eq!(retried.lineage, child.lineage);
        let later_child = state
            .fork_thread(
                &parent.id,
                ThreadId::from_static("fork-later-child"),
                Some(&second.id),
            )
            .await
            .expect("fork earlier terminal boundary while parent is active");
        assert_eq!(later_child.turns.len(), 2);
        state
            .finish_turn(&active, TurnStatus::Cancelled)
            .await
            .expect("settle active parent");

        let child_turn = state.start_turn(&child_id).await.expect("continue child");
        state
            .finish_turn(&child_turn, TurnStatus::Interrupted)
            .await
            .expect("settle child");
        let parent_loaded = state
            .load_thread(&parent.id)
            .await
            .expect("load parent")
            .expect("parent");
        assert_eq!(parent_loaded.turns.len(), 3);
        child_id
    }

    async fn assert_thread_archive(state: StateEngine) -> ThreadId {
        assert!(state.supports_thread_import());
        let source = state.create_thread().await.expect("create source");
        state
            .set_thread_name(&source.id, Some("Portable history".to_owned()))
            .await
            .expect("name source");
        let source_turn = state.start_turn(&source.id).await.expect("start source");
        let source_item = Item::new(ItemKind::UserMessage {
            content: "portable evidence".to_owned(),
        });
        state
            .append_item(&source_turn, source_item.clone())
            .await
            .expect("append source item");
        state
            .finish_turn(&source_turn, TurnStatus::Interrupted)
            .await
            .expect("finish source");
        state
            .create_checkpoint(
                &source.id,
                Some(source_turn.id.clone()),
                Some("source-only recovery cache".to_owned()),
            )
            .await
            .expect("checkpoint source");

        let archive = state
            .export_thread(&source.id)
            .await
            .expect("export source");
        assert_eq!(archive.source_stream_version, 6);
        let encoded = super::encode_thread_archive(&archive).expect("encode archive");
        let decoded = super::decode_thread_archive(&encoded).expect("decode archive");
        assert_eq!(decoded, archive);

        let mut legacy = serde_json::to_value(&archive).expect("legacy archive value");
        legacy["format_version"] = json!(1);
        assert!(
            super::decode_thread_archive(
                &serde_json::to_vec(&legacy).expect("encode legacy archive")
            )
            .expect_err("legacy archive format")
            .to_string()
            .contains("unsupported Thread archive format")
        );

        let mut unknown = serde_json::to_value(&archive).expect("archive value");
        unknown
            .as_object_mut()
            .expect("archive object")
            .insert("unexpected".to_owned(), json!(true));
        assert!(
            super::decode_thread_archive(
                &serde_json::to_vec(&unknown).expect("encode unknown archive")
            )
            .expect_err("unknown archive field")
            .to_string()
            .contains("unknown field")
        );

        let mut tampered = archive.clone();
        tampered.events[1].recorded_at_ms = tampered.events[1].recorded_at_ms.saturating_add(1);
        let error = super::decode_thread_archive(
            &serde_json::to_vec(&tampered).expect("encode tampered archive"),
        )
        .expect_err("tampered digest");
        assert!(error.to_string().contains("digest mismatch"));

        let target_id = ThreadId::from_string(format!("imported-{}", source.id));
        let imported = state
            .import_thread(&archive, target_id.clone())
            .await
            .expect("import source");
        assert_eq!(imported.name.as_deref(), Some("Portable history"));
        assert_eq!(imported.turns.len(), 1);
        assert_eq!(imported.turns[0].id, source_turn.id);
        assert_eq!(imported.turns[0].items[0].id, source_item.id);
        assert!(imported.checkpoints.is_empty());
        assert!(imported.lineage.is_none());
        let origin = imported.import_origin.as_ref().expect("import origin");
        assert_eq!(origin.source_thread_id, source.id);
        assert_eq!(origin.source_stream_version, archive.source_stream_version);
        assert_eq!(origin.source_events_sha256, archive.source_events_sha256);
        assert!(origin.source_lineage.is_none());
        let source_ids = archive
            .events
            .iter()
            .map(|event| &event.event_id)
            .collect::<BTreeSet<_>>();
        assert!(
            state
                .events(&target_id)
                .await
                .expect("target events")
                .iter()
                .all(|event| !source_ids.contains(&event.event_id))
        );

        let continued = state.start_turn(&target_id).await.expect("continue import");
        state
            .finish_turn(&continued, TurnStatus::Interrupted)
            .await
            .expect("finish imported Turn");
        let retried = state
            .import_thread(&archive, target_id.clone())
            .await
            .expect("idempotent import retry");
        assert_eq!(retried.turns.len(), 2);
        let snapshot = state
            .create_snapshot(&target_id)
            .await
            .expect("snapshot imported Thread");
        assert_eq!(
            snapshot.thread().import_origin.as_ref(),
            imported.import_origin.as_ref()
        );

        let conflict = state.create_thread().await.expect("create conflict");
        let error = state
            .import_thread(&archive, conflict.id)
            .await
            .expect_err("reject unrelated target");
        assert!(error.to_string().contains("different import provenance"));

        let running = state.create_thread().await.expect("create running source");
        let _turn = state
            .start_turn(&running.id)
            .await
            .expect("start running Turn");
        assert!(
            state
                .export_thread(&running.id)
                .await
                .expect_err("reject running export")
                .to_string()
                .contains("Turn is running")
        );
        target_id
    }

    async fn assert_event_pages(store: Arc<dyn EventStore>) {
        let state = StateEngine::new(store.clone());
        let thread = state.create_thread().await.expect("create thread");
        let turn = state.start_turn(&thread.id).await.expect("start turn");
        state
            .finish_turn(&turn, TurnStatus::Interrupted)
            .await
            .expect("finish turn");

        let first = store
            .events_page(&thread.id, 0, 2, super::MAX_STATE_EVENT_PAGE_RECOVERY_BYTES)
            .await
            .expect("first page");
        assert_eq!(first.len(), 2);
        let second = store
            .events_page(
                &thread.id,
                first.last().expect("cursor event").sequence,
                2,
                super::MAX_STATE_EVENT_PAGE_RECOVERY_BYTES,
            )
            .await
            .expect("second page");
        assert_eq!(second.len(), 1);
        assert!(second[0].sequence > first[1].sequence);

        let first_charge =
            super::stored_event_recovery_bytes(&first[0]).expect("first event charge");
        let byte_page = store
            .events_page(&thread.id, 0, 3, first_charge)
            .await
            .expect("byte-bounded page");
        assert_eq!(byte_page.len(), 1);

        let current_recovery_bytes =
            super::stored_events_recovery_bytes(&[first, second].concat()).expect("stream charge");
        let mut pending = PendingEvent {
            event_id: EventId::generate(),
            thread_id: thread.id,
            expected_stream_version: 3,
            expected_stream_recovery_bytes: current_recovery_bytes + 1,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnStarted {
                turn_id: TurnId::generate(),
            },
        };
        assert!(
            store
                .append(pending.clone())
                .await
                .expect_err("recovery charge mismatch")
                .to_string()
                .contains("recovery charge mismatch")
        );
        pending.expected_stream_recovery_bytes = current_recovery_bytes;
        store.append(pending).await.expect("exact recovery charge");
    }

    async fn assert_competing_turn_starts(first: Arc<dyn EventStore>, second: Arc<dyn EventStore>) {
        let thread_id = ThreadId::generate();
        let created = first
            .append(PendingEvent {
                event_id: EventId::generate(),
                thread_id: thread_id.clone(),
                expected_stream_version: 0,
                expected_stream_recovery_bytes: 0,
                recorded_at_ms: now_ms(),
                event: StateEvent::ThreadCreated {
                    created_at_ms: now_ms(),
                    tenant_id: None,
                },
            })
            .await
            .expect("create stream");
        let expected_stream_recovery_bytes =
            super::stored_event_recovery_bytes(&created).expect("created event charge");
        let first_start = PendingEvent {
            event_id: EventId::generate(),
            thread_id: thread_id.clone(),
            expected_stream_version: 1,
            expected_stream_recovery_bytes,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnStarted {
                turn_id: TurnId::generate(),
            },
        };
        let second_start = PendingEvent {
            event_id: EventId::generate(),
            thread_id: thread_id.clone(),
            expected_stream_version: 1,
            expected_stream_recovery_bytes,
            recorded_at_ms: now_ms(),
            event: StateEvent::TurnStarted {
                turn_id: TurnId::generate(),
            },
        };

        let (left, right) = tokio::join!(first.append(first_start), second.append(second_start));
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let conflict = if let Err(error) = left {
            error
        } else {
            right.expect_err("second writer must conflict")
        };
        assert!(matches!(
            conflict,
            HarnessError::StateConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        let events = StateEngine::new(first.clone())
            .events(&thread_id)
            .await
            .expect("events");
        assert_eq!(events.len(), 2);
        let projected = super::project_events(&events)
            .expect("valid projection")
            .expect("thread");
        assert_eq!(
            projected
                .turns
                .iter()
                .filter(|turn| turn.status == TurnStatus::Running)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn sqlite_reopens_and_marks_unfinished_turn_interrupted_once() {
        let path = temp_database_path();
        let thread_id;
        let turn_id;
        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            let thread = state.create_thread().await.expect("create thread");
            thread_id = thread.id;
            let turn = state.start_turn(&thread_id).await.expect("start turn");
            turn_id = turn.id.clone();
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::UserMessage {
                        content: "before crash".to_owned(),
                    }),
                )
                .await
                .expect("append item");
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::ProviderContinuation {
                        model_id: "test/crash-model".to_owned(),
                        model_origin: CapabilityOrigin::BuiltIn,
                        continuation: ModelContinuation::new(
                            "test.provider.reasoning.v1",
                            vec![json!({"opaque": "ciphertext"})],
                        )
                        .expect("continuation"),
                    }),
                )
                .await
                .expect("append continuation");
        }

        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path)
                    .await
                    .expect("reopen database"),
            ));
            let recovered = state
                .recover_thread(&thread_id, &turn_id)
                .await
                .expect("recover")
                .expect("thread exists");
            assert_eq!(recovered.turns[0].status, TurnStatus::Interrupted);
            assert!(matches!(
                &recovered.turns[0].items[1].kind,
                ItemKind::ProviderContinuation {
                    model_id,
                    model_origin: CapabilityOrigin::BuiltIn,
                    continuation,
                } if model_id == "test/crash-model"
                    && continuation.format() == "test.provider.reasoning.v1"
            ));
            let count = state.events(&thread_id).await.expect("events").len();
            state
                .recover_thread(&thread_id, &turn_id)
                .await
                .expect("second recovery");
            assert_eq!(state.events(&thread_id).await.expect("events").len(), count);
        }

        remove_database_files(&path);
    }

    #[tokio::test]
    async fn exact_recovery_never_interrupts_a_newer_turn() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("create Thread");
        let abandoned = state.start_turn(&thread.id).await.expect("start old Turn");
        state
            .finish_turn(&abandoned, TurnStatus::Interrupted)
            .await
            .expect("settle old Turn");
        let newer = state
            .start_turn(&thread.id)
            .await
            .expect("start newer Turn");

        let error = state
            .recover_thread(&thread.id, &abandoned.id)
            .await
            .expect_err("stale recovery must fail");
        assert!(matches!(error, HarnessError::State(_)));
        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load Thread")
            .expect("Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Interrupted);
        assert_eq!(projected.turns[1].id, newer.id);
        assert_eq!(projected.turns[1].status, TurnStatus::Running);
    }

    #[tokio::test]
    async fn checkpoint_survives_projection_and_reopen() {
        let path = temp_database_path();
        let thread_id;
        let checkpoint_id;
        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            let thread = state.create_thread().await.expect("create thread");
            thread_id = thread.id;
            let checkpoint = state
                .create_checkpoint(&thread_id, None, Some("baseline".to_owned()))
                .await
                .expect("checkpoint");
            checkpoint_id = checkpoint.id;
        }
        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path)
                    .await
                    .expect("reopen database"),
            ));
            let thread = state
                .load_thread(&thread_id)
                .await
                .expect("load")
                .expect("thread exists");
            assert_eq!(thread.checkpoints[0].id, checkpoint_id);
            assert_eq!(thread.checkpoints[0].label.as_deref(), Some("baseline"));
        }
        remove_database_files(&path);
    }

    fn temp_database_path() -> PathBuf {
        std::env::temp_dir().join(format!("y-harness-{}.db", EventId::generate()))
    }

    fn remove_database_files(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[test]
    fn state_event_json_is_stable_enough_for_round_trip() {
        let event = StateEvent::ItemAppended {
            turn_id: crate::TurnId::from_static("turn-test"),
            item: Item::new(ItemKind::ToolResult {
                call_id: "call".to_owned(),
                output: json!({"ok": true}),
                is_error: false,
                connector_evidence: Vec::new(),
            }),
        };
        let encoded = serde_json::to_string(&event).expect("serialize");
        let decoded: StateEvent = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(event, decoded);
    }

    #[test]
    fn thread_tenant_evidence_requires_schema_twelve() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::from_static("tenant-event"),
            thread_id: ThreadId::from_static("tenant-thread"),
            recorded_at_ms: 1,
            event: StateEvent::ThreadCreated {
                created_at_ms: 1,
                tenant_id: Some("tenant-a".to_owned()),
            },
        };
        super::validate_stored_event(&stored).expect("schema-12 tenant evidence");
        stored.schema_version = 11;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-11 cannot claim tenant ownership");
        assert!(error.to_string().contains("schema-11"));
    }

    #[test]
    fn execution_binding_evidence_requires_schema_thirteen() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 3,
            event_id: EventId::from_static("event-execution-binding"),
            thread_id: ThreadId::from_static("thread-execution-binding"),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-execution-binding"),
                item: Item::new(ItemKind::ExecutionBinding {
                    bound_by: ActorIdentity::LocalProcess,
                    binding: crate::ExecutionBinding::new(
                        "domain-pack",
                        "course-assistant",
                        "1.0.0",
                        "a".repeat(64),
                        "b".repeat(64),
                        1,
                        None,
                    )
                    .expect("binding"),
                }),
            },
        };
        super::validate_stored_event(&stored).expect("schema-13 execution binding evidence");
        stored.schema_version = 12;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-12 cannot claim execution binding evidence");
        assert!(error.to_string().contains("schema-12"));
    }

    #[test]
    fn connector_evidence_requires_schema_fourteen_and_exact_output() {
        let output = json!({"status": "active"});
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 3,
            event_id: EventId::from_static("event-connector-evidence"),
            thread_id: ThreadId::from_static("thread-connector-evidence"),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-connector-evidence"),
                item: Item::new(ItemKind::ToolResult {
                    call_id: "call-connector".to_owned(),
                    output: output.clone(),
                    is_error: false,
                    connector_evidence: vec![connector_evidence(
                        "crm.read",
                        CapabilityOrigin::BuiltIn,
                        AuthorityContext::local_process(),
                        &output,
                    )],
                }),
            },
        };
        super::validate_stored_event(&stored).expect("schema-14 Connector evidence");
        stored.schema_version = 13;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-13 cannot claim Connector evidence");
        assert!(error.to_string().contains("schema-13"));

        stored.schema_version = super::STATE_EVENT_SCHEMA_VERSION;
        {
            let StateEvent::ItemAppended { item, .. } = &mut stored.event else {
                unreachable!()
            };
            let ItemKind::ToolResult { output, .. } = &mut item.kind else {
                unreachable!()
            };
            *output = json!({"status": "tampered"});
        }
        let error =
            super::validate_stored_event(&stored).expect_err("output digest tampering must fail");
        assert!(error.to_string().contains("digest differs"));

        {
            let StateEvent::ItemAppended { item, .. } = &mut stored.event else {
                unreachable!()
            };
            let ItemKind::ToolResult {
                output, is_error, ..
            } = &mut item.kind
            else {
                unreachable!()
            };
            *output = json!({"status": "active"});
            *is_error = true;
        }
        let error = super::validate_stored_event(&stored)
            .expect_err("failed Tool result cannot retain evidence");
        assert!(error.to_string().contains("failed Tool result"));
    }

    #[test]
    fn connector_evidence_projection_rejects_registered_origin_tampering() {
        let thread_id = ThreadId::from_static("thread-connector-projection");
        let turn_id = TurnId::from_static("turn-connector-projection");
        let output = json!({"status": "active"});
        let mut events = vec![
            StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence: 1,
                event_id: EventId::from_static("event-connector-created"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 1,
                event: StateEvent::ThreadCreated {
                    created_at_ms: 1,
                    tenant_id: None,
                },
            },
            StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence: 2,
                event_id: EventId::from_static("event-connector-turn"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 2,
                event: StateEvent::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            },
            StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence: 3,
                event_id: EventId::from_static("event-connector-call"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 3,
                event: StateEvent::ItemAppended {
                    turn_id: turn_id.clone(),
                    item: Item::new(ItemKind::ToolCall {
                        model_id: None,
                        model_origin: None,
                        call_id: "call-connector".to_owned(),
                        name: "crm.read".to_owned(),
                        input: json!({}),
                        batch: None,
                    }),
                },
            },
            StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence: 4,
                event_id: EventId::from_static("event-connector-policy"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 4,
                event: StateEvent::ItemAppended {
                    turn_id: turn_id.clone(),
                    item: Item::new(ItemKind::PolicyDecision {
                        call_id: "call-connector".to_owned(),
                        tool_origin: Some(CapabilityOrigin::BuiltIn),
                        decision: PolicyDecision::Allow,
                    }),
                },
            },
            StoredEvent {
                schema_version: super::STATE_EVENT_SCHEMA_VERSION,
                sequence: 5,
                event_id: EventId::from_static("event-connector-result"),
                thread_id,
                recorded_at_ms: 5,
                event: StateEvent::ItemAppended {
                    turn_id,
                    item: Item::new(ItemKind::ToolResult {
                        call_id: "call-connector".to_owned(),
                        output: output.clone(),
                        is_error: false,
                        connector_evidence: vec![connector_evidence(
                            "crm.read",
                            CapabilityOrigin::External {
                                id: "tampered-origin".to_owned(),
                            },
                            AuthorityContext::local_process(),
                            &output,
                        )],
                    }),
                },
            },
        ];

        let error = super::project_events(&events).expect_err("origin tampering must fail");
        assert!(error.to_string().contains("execution provenance"));

        let StateEvent::ItemAppended { item, .. } = &mut events[4].event else {
            unreachable!()
        };
        let ItemKind::ToolResult {
            connector_evidence: records,
            ..
        } = &mut item.kind
        else {
            unreachable!()
        };
        *records = vec![connector_evidence(
            "crm.read",
            CapabilityOrigin::BuiltIn,
            AuthorityContext::local_process(),
            &output,
        )];
        let StateEvent::ItemAppended { item, .. } = &mut events[3].event else {
            unreachable!()
        };
        let ItemKind::PolicyDecision { decision, .. } = &mut item.kind else {
            unreachable!()
        };
        *decision = PolicyDecision::Deny {
            reason: "not authorized".to_owned(),
        };
        let error = super::project_events(&events).expect_err("denied execution must fail");
        assert!(error.to_string().contains("authorized execution path"));
    }

    #[tokio::test]
    async fn connector_evidence_append_requires_exact_trusted_authority() {
        let bound_authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-a".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("bound authority");
        let append_authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-b".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("append authority");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state
            .create_thread_as(&bound_authority)
            .await
            .expect("thread");
        let turn = state
            .start_turn_as(&thread.id, &bound_authority)
            .await
            .expect("turn");
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ToolCall {
                    model_id: None,
                    model_origin: None,
                    call_id: "call-connector".to_owned(),
                    name: "crm.read".to_owned(),
                    input: json!({}),
                    batch: None,
                }),
                &bound_authority,
            )
            .await
            .expect("call");
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: "call-connector".to_owned(),
                    tool_origin: Some(CapabilityOrigin::BuiltIn),
                    decision: PolicyDecision::Allow,
                }),
                &bound_authority,
            )
            .await
            .expect("policy");
        let output = json!({"status": "active"});
        let error = state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ToolResult {
                    call_id: "call-connector".to_owned(),
                    output: output.clone(),
                    is_error: false,
                    connector_evidence: vec![connector_evidence(
                        "crm.read",
                        CapabilityOrigin::BuiltIn,
                        bound_authority,
                        &output,
                    )],
                }),
                &append_authority,
            )
            .await
            .expect_err("authority substitution must fail");
        assert!(error.to_string().contains("trusted append authority"));
    }

    #[tokio::test]
    async fn connector_evidence_survives_sqlite_snapshot_and_reopen() {
        let path = temp_database_path();
        let thread_id;
        let output = json!({"status": "active"});
        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            let thread = state.create_thread().await.expect("thread");
            thread_id = thread.id.clone();
            let turn = state.start_turn(&thread.id).await.expect("turn");
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::ToolCall {
                        model_id: None,
                        model_origin: None,
                        call_id: "call-connector".to_owned(),
                        name: "crm.read".to_owned(),
                        input: json!({}),
                        batch: None,
                    }),
                )
                .await
                .expect("call");
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::PolicyDecision {
                        call_id: "call-connector".to_owned(),
                        tool_origin: Some(CapabilityOrigin::BuiltIn),
                        decision: PolicyDecision::Allow,
                    }),
                )
                .await
                .expect("policy");
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::ToolResult {
                        call_id: "call-connector".to_owned(),
                        output: output.clone(),
                        is_error: false,
                        connector_evidence: vec![connector_evidence(
                            "crm.read",
                            CapabilityOrigin::BuiltIn,
                            AuthorityContext::local_process(),
                            &output,
                        )],
                    }),
                )
                .await
                .expect("result");
            state
                .finish_turn(&turn, TurnStatus::Interrupted)
                .await
                .expect("finish");
            state.create_snapshot(&thread.id).await.expect("snapshot");
        }

        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen database"),
        ));
        let recovered = state
            .load_thread(&thread_id)
            .await
            .expect("load")
            .expect("thread");
        assert!(matches!(
            &recovered.turns[0].items[2].kind,
            ItemKind::ToolResult {
                connector_evidence,
                ..
            } if connector_evidence.len() == 1
                && connector_evidence[0].claim().version() == "revision-7"
        ));
        remove_database_files(&path);
    }

    #[test]
    fn conversation_summary_evidence_requires_schema_two() {
        let mut stored: StoredEvent = serde_json::from_str(include_str!(
            "../../tests/fixtures/state-v2-summary-event.json"
        ))
        .expect("decode schema-2 fixture");
        super::validate_stored_event(&stored).expect("schema-2 summary evidence");
        stored.schema_version = 1;
        let error =
            super::validate_stored_event(&stored).expect_err("schema-1 cannot claim new evidence");
        assert!(error.to_string().contains("schema-1"));
    }

    #[test]
    fn approval_continuation_evidence_requires_schema_three() {
        let mut stored: StoredEvent = serde_json::from_str(include_str!(
            "../../tests/fixtures/state-v3-approval-event.json"
        ))
        .expect("decode schema-3 fixture");
        super::validate_stored_event(&stored).expect("schema-3 continuation evidence");
        stored.schema_version = 2;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-2 cannot claim continuation evidence");
        assert!(error.to_string().contains("schema-2"));

        if let StateEvent::ItemAppended { item, .. } = &mut stored.event
            && let ItemKind::ApprovalRequested {
                requested_by,
                tool_origin,
                model_request_sha256,
                ..
            } = &mut item.kind
        {
            *requested_by = None;
            *tool_origin = None;
            *model_request_sha256 = None;
        }
        super::validate_stored_event(&stored).expect("schema-2 legacy approval evidence");
        stored.schema_version = 3;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-3 requires continuation evidence");
        assert!(error.to_string().contains("schema-3"));
    }

    #[test]
    fn policy_tool_origin_evidence_requires_schema_four() {
        let mut stored: StoredEvent = serde_json::from_str(include_str!(
            "../../tests/fixtures/state-v4-policy-event.json"
        ))
        .expect("decode schema-4 fixture");
        super::validate_stored_event(&stored).expect("schema-4 Tool origin evidence");
        stored.schema_version = 3;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-3 cannot claim Tool origin evidence");
        assert!(error.to_string().contains("schema-3"));

        if let StateEvent::ItemAppended { item, .. } = &mut stored.event
            && let ItemKind::PolicyDecision { tool_origin, .. } = &mut item.kind
        {
            *tool_origin = None;
        }
        super::validate_stored_event(&stored).expect("schema-3 legacy Policy decision");
        stored.schema_version = 4;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-4 requires Tool origin evidence");
        assert!(error.to_string().contains("schema-4"));
    }

    #[test]
    fn provider_continuation_evidence_requires_schema_five() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::from_static("event-continuation"),
            thread_id: ThreadId::from_static("thread-continuation"),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-continuation"),
                item: Item::new(ItemKind::ProviderContinuation {
                    model_id: "test/continuation-model".to_owned(),
                    model_origin: CapabilityOrigin::BuiltIn,
                    continuation: ModelContinuation::new(
                        "test.provider.reasoning.v1",
                        vec![json!({"opaque": "ciphertext"})],
                    )
                    .expect("continuation"),
                }),
            },
        };
        super::validate_stored_event(&stored).expect("schema-5 continuation evidence");
        stored.schema_version = 4;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-4 cannot claim continuation evidence");
        assert!(error.to_string().contains("schema-4"));
    }

    #[test]
    fn steering_evidence_requires_schema_six() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::from_static("event-steering"),
            thread_id: ThreadId::from_static("thread-steering"),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-steering"),
                item: Item::new(ItemKind::SteeringQueued {
                    steering_id: SteeringId::from_static("steering-schema"),
                    submitted_by: ActorIdentity::LocalProcess,
                    content: "correct course".to_owned(),
                }),
            },
        };
        super::validate_stored_event(&stored).expect("schema-6 steering evidence");
        stored.schema_version = 5;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-5 cannot claim steering evidence");
        assert!(error.to_string().contains("schema-5"));
    }

    #[test]
    fn atomic_tool_call_batch_requires_schema_seven() {
        let batch_id = ToolCallBatchId::from_static("schema-seven-batch");
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::from_static("event-tool-batch"),
            thread_id: ThreadId::from_static("thread-tool-batch"),
            recorded_at_ms: 1,
            event: StateEvent::ToolCallsAppended {
                turn_id: TurnId::from_static("turn-tool-batch"),
                calls: ["call-1", "call-2"]
                    .into_iter()
                    .enumerate()
                    .map(|(index, call_id)| {
                        Item::new(ItemKind::ToolCall {
                            model_id: Some("test/model".to_owned()),
                            model_origin: Some(CapabilityOrigin::BuiltIn),
                            call_id: call_id.to_owned(),
                            name: "echo".to_owned(),
                            input: json!({}),
                            batch: Some(ToolCallBatch {
                                id: batch_id.clone(),
                                index,
                                size: 2,
                            }),
                        })
                    })
                    .collect(),
            },
        };
        super::validate_stored_event(&stored).expect("schema-7 Tool-call batch");
        stored.schema_version = 6;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-6 cannot claim atomic Tool-call batch");
        assert!(error.to_string().contains("schema-6"));
    }

    #[test]
    fn thread_fork_lineage_requires_schema_nine() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 2,
            event_id: EventId::from_static("event-thread-fork"),
            thread_id: ThreadId::from_static("thread-child"),
            recorded_at_ms: 1,
            event: StateEvent::ThreadForked {
                lineage: ThreadLineage {
                    parent_thread_id: ThreadId::from_static("thread-parent"),
                    parent_through_sequence: 1,
                    parent_stream_version: 1,
                    parent_events_sha256: "0".repeat(64),
                },
            },
        };
        super::validate_stored_event(&stored).expect("schema-9 Thread fork lineage");
        stored.schema_version = 8;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-8 cannot claim Thread fork lineage");
        assert!(error.to_string().contains("schema-8"));
    }

    #[test]
    fn thread_import_origin_requires_schema_ten() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 2,
            event_id: EventId::from_static("event-thread-import"),
            thread_id: ThreadId::from_static("thread-imported"),
            recorded_at_ms: 1,
            event: StateEvent::ThreadImported {
                origin: ThreadImportOrigin {
                    source_thread_id: ThreadId::from_static("thread-source"),
                    source_stream_version: 1,
                    source_last_sequence: 1,
                    source_events_sha256: "0".repeat(64),
                    source_lineage: None,
                },
            },
        };
        super::validate_stored_event(&stored).expect("schema-10 Thread import origin");
        stored.schema_version = 9;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-9 cannot claim Thread import origin");
        assert!(error.to_string().contains("schema-9"));
    }

    #[test]
    fn invocation_context_evidence_requires_schema_eleven() {
        let mut stored = StoredEvent {
            schema_version: super::STATE_EVENT_SCHEMA_VERSION,
            sequence: 3,
            event_id: EventId::from_static("event-invocation-context"),
            thread_id: ThreadId::from_static("thread-invocation-context"),
            recorded_at_ms: 1,
            event: StateEvent::ItemAppended {
                turn_id: TurnId::from_static("turn-invocation-context"),
                item: Item::new(ItemKind::InvocationContext {
                    submitted_by: ActorIdentity::LocalProcess,
                    blocks: vec![InvocationContextEvidence {
                        source: "rag".to_owned(),
                        reference: "document:1".to_owned(),
                        source_sha256: "1".repeat(64),
                        content_sha256: "2".repeat(64),
                        estimated_tokens: 4,
                        serialized_bytes: 16,
                    }],
                }),
            },
        };
        super::validate_stored_event(&stored).expect("schema-11 Turn context evidence");
        stored.schema_version = 10;
        let error = super::validate_stored_event(&stored)
            .expect_err("schema-10 cannot claim Turn context evidence");
        assert!(error.to_string().contains("schema-10"));
    }

    #[test]
    fn legacy_model_items_without_provenance_remain_readable() {
        let assistant: ItemKind =
            serde_json::from_str(r#"{"type":"assistant_message","content":"legacy"}"#)
                .expect("legacy assistant item");
        assert!(matches!(
            assistant,
            ItemKind::AssistantMessage {
                model_id: None,
                model_origin: None,
                ..
            }
        ));

        let tool_call: ItemKind = serde_json::from_str(
            r#"{"type":"tool_call","call_id":"call","name":"echo","input":{}}"#,
        )
        .expect("legacy tool call item");
        assert!(matches!(
            tool_call,
            ItemKind::ToolCall {
                model_id: None,
                model_origin: None,
                ..
            }
        ));
    }

    #[test]
    fn schema_14_completion_fields_replay_legacy_and_schema_15_fails_closed() {
        let thread_id = ThreadId::from_static("legacy-candidate-thread");
        let turn_id = TurnId::from_static("legacy-candidate-turn");
        let legacy_candidate = Item::new(ItemKind::AssistantMessage {
            model_id: Some("test/model".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: None,
            content: "legacy candidate".to_owned(),
        });
        let legacy_verification = Item::new(ItemKind::VerificationResult {
            verifier: "quality".to_owned(),
            candidate_item_id: None,
            verifier_origin: None,
            verifier_binding_sha256: None,
            outcome: VerificationOutcome::Passed { summary: None },
        });
        let events = vec![
            StoredEvent {
                schema_version: 14,
                sequence: 1,
                event_id: EventId::from_static("legacy-candidate-created"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 1,
                event: StateEvent::ThreadCreated {
                    created_at_ms: 1,
                    tenant_id: None,
                },
            },
            StoredEvent {
                schema_version: 14,
                sequence: 2,
                event_id: EventId::from_static("legacy-candidate-started"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 2,
                event: StateEvent::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            },
            StoredEvent {
                schema_version: 14,
                sequence: 3,
                event_id: EventId::from_static("legacy-candidate-message"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 3,
                event: StateEvent::ItemAppended {
                    turn_id: turn_id.clone(),
                    item: legacy_candidate.clone(),
                },
            },
            StoredEvent {
                schema_version: 14,
                sequence: 4,
                event_id: EventId::from_static("legacy-candidate-verification"),
                thread_id,
                recorded_at_ms: 4,
                event: StateEvent::ItemAppended {
                    turn_id,
                    item: legacy_verification.clone(),
                },
            },
        ];
        let projected = super::project_events(&events)
            .expect("schema-14 candidate history")
            .expect("legacy Thread");
        assert_eq!(projected.turns[0].items.len(), 2);

        let mut current_labeled_candidate = events[2].clone();
        current_labeled_candidate.schema_version = 15;
        assert!(
            super::validate_stored_event(&current_labeled_candidate)
                .expect_err("schema-15 candidate requires a request digest")
                .to_string()
                .contains("request digest")
        );
        let mut current_labeled_verification = events[3].clone();
        current_labeled_verification.schema_version = 15;
        assert!(
            super::validate_stored_event(&current_labeled_verification)
                .expect_err("schema-15 verification requires binding evidence")
                .to_string()
                .contains("binding digest")
        );

        let request_sha256 = "1".repeat(64);
        let current_candidate = Item::new(ItemKind::AssistantMessage {
            model_id: Some("test/model".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(request_sha256),
            content: "current candidate".to_owned(),
        });
        super::validate_state_item(&current_candidate).expect("current candidate shape");
        super::validate_state_item_schema(&current_candidate.kind, 15)
            .expect("schema-15 candidate");
        assert!(super::validate_state_item_schema(&current_candidate.kind, 14).is_err());

        let current_verification = Item::new(ItemKind::VerificationResult {
            verifier: "quality".to_owned(),
            candidate_item_id: Some(current_candidate.id.clone()),
            verifier_origin: Some(CapabilityOrigin::BuiltIn),
            verifier_binding_sha256: Some("2".repeat(64)),
            outcome: VerificationOutcome::Passed {
                summary: Some("verified".to_owned()),
            },
        });
        super::validate_state_item(&current_verification).expect("current verification shape");
        super::validate_state_item_schema(&current_verification.kind, 15)
            .expect("schema-15 verification");
        assert!(super::validate_state_item_schema(&current_verification.kind, 14).is_err());

        let partial = Item::new(ItemKind::VerificationResult {
            verifier: "quality".to_owned(),
            candidate_item_id: Some(current_candidate.id),
            verifier_origin: None,
            verifier_binding_sha256: None,
            outcome: VerificationOutcome::Passed { summary: None },
        });
        assert!(super::validate_state_item(&partial).is_err());
        let oversized = Item::new(ItemKind::VerificationResult {
            verifier: "quality".to_owned(),
            candidate_item_id: None,
            verifier_origin: None,
            verifier_binding_sha256: None,
            outcome: VerificationOutcome::Failed {
                reason: "x".repeat(4_097),
                retryable: false,
            },
        });
        assert!(super::validate_state_item(&oversized).is_err());
    }

    async fn start_due_approval_wait(
        state: &StateEngine,
        authority: &AuthorityContext,
        wait_id: AgentLoopWaitId,
        value: u64,
    ) -> (Turn, u64) {
        let (turn, request, generation) =
            approval_wait_fixture_with_input(state, authority, json!({"value": value})).await;
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id,
                    request,
                    generation,
                    Some(Duration::from_millis(1)),
                    None,
                ),
                authority,
            )
            .await
            .expect("start indexed due wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("indexed wait-start event");
        };
        (
            turn,
            envelope
                .expires_at_ms
                .expect("indexed wait has a bounded expiry"),
        )
    }

    async fn ready_allow_due_wait(
        state: &StateEngine,
        authority: &AuthorityContext,
        wait_id: AgentLoopWaitId,
    ) -> AgentLoopDueWait {
        let (turn, request, generation) = approval_wait_fixture(state, authority).await;
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                authority,
            )
            .await
            .expect("start exact-read ReadyAllow wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("exact-read ReadyAllow wait-start event");
        };
        assert!(
            envelope.expires_at_ms.is_some(),
            "exact-read ReadyAllow wait must have an expiry"
        );
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("exact-read-ready-resume"),
                &settled_approval(request, authority.tenant_id().map(str::to_owned)),
                authority,
            )
            .await
            .expect("accept exact-read ReadyAllow wait");
        let page = state
            .scan_due_agent_loop_waits_as(u64::MAX, None, 1, authority)
            .await
            .expect("scan exact-read ReadyAllow wait");
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyAllow);
        page.due[0].clone()
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SqliteEventFootprint {
        sequence: i64,
        event_id: String,
        recorded_at_ms: i64,
        schema_version: i64,
        event_json: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SqliteWaitProjectionFootprint {
        tenant_key: String,
        turn_id: String,
        wait_id: String,
        revision: i64,
        phase: i64,
        due_at_ms: Option<i64>,
        approval_id: String,
        envelope_sha256: String,
        wait_started_event_id: String,
        current_transition_event_id: String,
        resume_command_id: Option<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct SqliteThreadFootprint {
        events: Vec<SqliteEventFootprint>,
        stream: (i64, Option<String>, Option<String>),
        recovery_bytes: i64,
        projection: Option<SqliteWaitProjectionFootprint>,
    }

    async fn sqlite_thread_footprint(
        store: &SqliteEventStore,
        thread_id: &ThreadId,
    ) -> SqliteThreadFootprint {
        let thread_id = thread_id.clone();
        store
            .with_connection(move |connection| {
                let events = {
                    let mut statement = connection
                        .prepare(
                            "SELECT sequence, event_id, recorded_at_ms, schema_version, event_json
                             FROM events
                             WHERE thread_id = ?1
                             ORDER BY sequence ASC",
                        )
                        .map_err(|error| HarnessError::State(error.to_string()))?;
                    statement
                        .query_map([thread_id.as_str()], |row| {
                            Ok(SqliteEventFootprint {
                                sequence: row.get(0)?,
                                event_id: row.get(1)?,
                                recorded_at_ms: row.get(2)?,
                                schema_version: row.get(3)?,
                                event_json: row.get(4)?,
                            })
                        })
                        .map_err(|error| HarnessError::State(error.to_string()))?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(|error| HarnessError::State(error.to_string()))?
                };
                let stream = connection
                    .query_row(
                        "SELECT version, name, tenant_id FROM streams WHERE thread_id = ?1",
                        [thread_id.as_str()],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let recovery_bytes = connection
                    .query_row(
                        "SELECT recovery_bytes FROM stream_recovery WHERE thread_id = ?1",
                        [thread_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                let projection = connection
                    .query_row(
                        "SELECT tenant_key, turn_id, wait_id, revision, phase, due_at_ms,
                                approval_id, envelope_sha256, wait_started_event_id,
                                current_transition_event_id, resume_command_id
                         FROM agent_loop_wait_projection
                         WHERE thread_id = ?1",
                        [thread_id.as_str()],
                        |row| {
                            Ok(SqliteWaitProjectionFootprint {
                                tenant_key: row.get(0)?,
                                turn_id: row.get(1)?,
                                wait_id: row.get(2)?,
                                revision: row.get(3)?,
                                phase: row.get(4)?,
                                due_at_ms: row.get(5)?,
                                approval_id: row.get(6)?,
                                envelope_sha256: row.get(7)?,
                                wait_started_event_id: row.get(8)?,
                                current_transition_event_id: row.get(9)?,
                                resume_command_id: row.get(10)?,
                            })
                        },
                    )
                    .optional()
                    .map_err(|error| HarnessError::State(error.to_string()))?;
                Ok(SqliteThreadFootprint {
                    events,
                    stream,
                    recovery_bytes,
                    projection,
                })
            })
            .await
            .expect("read exact SQLite State footprint")
    }

    async fn install_projection_fault_trigger(store: &SqliteEventStore, mutation: &str) {
        let statement = match mutation {
            "INSERT" => {
                "CREATE TRIGGER fail_agent_loop_wait_projection_insert
                 BEFORE INSERT ON agent_loop_wait_projection
                 BEGIN
                     SELECT RAISE(ABORT, 'fault-injected wait projection INSERT');
                 END;"
            }
            "UPDATE" => {
                "CREATE TRIGGER fail_agent_loop_wait_projection_update
                 BEFORE UPDATE ON agent_loop_wait_projection
                 BEGIN
                     SELECT RAISE(ABORT, 'fault-injected wait projection UPDATE');
                 END;"
            }
            "DELETE" => {
                "CREATE TRIGGER fail_agent_loop_wait_projection_delete
                 BEFORE DELETE ON agent_loop_wait_projection
                 BEGIN
                     SELECT RAISE(ABORT, 'fault-injected wait projection DELETE');
                 END;"
            }
            _ => panic!("unsupported projection mutation {mutation}"),
        };
        store
            .with_connection(move |connection| {
                connection
                    .execute_batch(statement)
                    .map_err(|error| HarnessError::State(error.to_string()))
            })
            .await
            .expect("install SQLite projection fault trigger");
    }

    #[tokio::test]
    async fn due_settlement_uses_only_one_or_two_exact_event_reads() {
        let waiting_store = Arc::new(ExactReadProbeStore::new());
        let waiting_state = StateEngine::new(waiting_store.clone());
        let authority = AuthorityContext::local_process();
        let (_, expires_at_ms) = start_due_approval_wait(
            &waiting_state,
            &authority,
            AgentLoopWaitId::from_static("wait-exact-read-count"),
            21,
        )
        .await;
        let waiting_page = waiting_state
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("scan Waiting exact-read count fixture");
        assert_eq!(waiting_page.due.len(), 1);
        assert_eq!(waiting_page.due[0].phase, AgentLoopDuePhase::Waiting);

        waiting_store.reset_read_counts();
        waiting_state
            .settle_due_agent_loop_wait(&waiting_page.due[0], expires_at_ms)
            .await
            .expect("settle Waiting exact-read count fixture");
        assert_eq!(waiting_store.read_counts(), (1, 0, 0));

        let ready_store = Arc::new(ExactReadProbeStore::new());
        let ready_state = StateEngine::new(ready_store.clone());
        let ready = ready_allow_due_wait(
            &ready_state,
            &authority,
            AgentLoopWaitId::from_static("ready-exact-read-count"),
        )
        .await;

        ready_store.reset_read_counts();
        ready_state
            .settle_due_agent_loop_wait(&ready, u64::MAX)
            .await
            .expect("settle ReadyAllow exact-read count fixture");
        assert_eq!(ready_store.read_counts(), (2, 0, 0));
    }

    #[tokio::test]
    async fn corrupt_ready_exact_event_fails_closed_without_mutating_due_fences() {
        let authority = AuthorityContext::local_process();
        for (fault, expected_error) in [
            (
                ExactReadFault::Missing,
                "projection references a missing transition event",
            ),
            (
                ExactReadFault::WrongShape,
                "transition event does not match its due fence",
            ),
            (
                ExactReadFault::CrossThread,
                "transition event differs from its projection",
            ),
        ] {
            let store = Arc::new(ExactReadProbeStore::new());
            let state = StateEngine::new(store.clone());
            let due = ready_allow_due_wait(
                &state,
                &authority,
                AgentLoopWaitId::from_static("ready-exact-read-corrupt"),
            )
            .await;
            let before = state
                .scan_due_agent_loop_waits(u64::MAX, None, 1)
                .await
                .expect("scan due fence before exact-read fault");
            assert_eq!(before.due, [due.clone()]);

            store.reset_read_counts();
            store.arm_exact_read_fault(fault, 2);
            let error = state
                .settle_due_agent_loop_wait(&due, u64::MAX)
                .await
                .expect_err("corrupt exact transition evidence must fail closed");
            assert!(error.to_string().contains(expected_error));
            assert_eq!(store.read_counts(), (2, 0, 0));

            store.clear_exact_read_fault();
            let after = state
                .scan_due_agent_loop_waits(u64::MAX, None, 1)
                .await
                .expect("scan due fence after rejected exact-read evidence");
            assert_eq!(after, before);
            assert_eq!(store.read_counts(), (2, 0, 0));
        }
    }

    async fn assert_due_wait_tenant_keyset(state: &StateEngine) {
        let tenant_a = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "due-maintainer-a".to_owned(),
            },
            Some("tenant-due-a".to_owned()),
        )
        .expect("tenant A due authority");
        let tenant_b = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test-identity".to_owned(),
                subject: "due-maintainer-b".to_owned(),
            },
            Some("tenant-due-b".to_owned()),
        )
        .expect("tenant B due authority");

        let (turn_a1, expires_a1) = start_due_approval_wait(
            state,
            &tenant_a,
            AgentLoopWaitId::from_static("tenant-a-due-wait-1"),
            1,
        )
        .await;
        let (turn_b, expires_b) = start_due_approval_wait(
            state,
            &tenant_b,
            AgentLoopWaitId::from_static("tenant-b-due-wait"),
            2,
        )
        .await;
        let (turn_a2, expires_a2) = start_due_approval_wait(
            state,
            &tenant_a,
            AgentLoopWaitId::from_static("tenant-a-due-wait-2"),
            3,
        )
        .await;
        let at_ms = expires_a1.max(expires_b).max(expires_a2);

        let first = state
            .scan_due_agent_loop_waits_as(at_ms, None, 1, &tenant_a)
            .await
            .expect("scan first tenant A due page");
        assert_eq!(first.scanned, 1);
        assert_eq!(first.due.len(), 1);
        assert!(first.has_more);
        assert_eq!(first.due[0].tenant_id.as_deref(), Some("tenant-due-a"));
        assert_eq!(first.next_cursor, Some(first.due[0].cursor()));
        let first_cursor = first.next_cursor.expect("first page cursor");

        let second = state
            .scan_due_agent_loop_waits_as(at_ms, Some(&first_cursor), 1, &tenant_a)
            .await
            .expect("scan second tenant A due page");
        assert_eq!(second.scanned, 1);
        assert_eq!(second.due.len(), 1);
        assert!(!second.has_more);
        assert_eq!(second.due[0].tenant_id.as_deref(), Some("tenant-due-a"));
        assert!(second.due[0].cursor() > first_cursor);
        assert_eq!(second.next_cursor, Some(second.due[0].cursor()));

        let tenant_a_threads = [
            first.due[0].thread_id.clone(),
            second.due[0].thread_id.clone(),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        assert_eq!(
            tenant_a_threads,
            [turn_a1.thread_id, turn_a2.thread_id]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let tenant_b_page = state
            .scan_due_agent_loop_waits_as(at_ms, None, 2, &tenant_b)
            .await
            .expect("scan tenant B due page");
        assert_eq!(tenant_b_page.scanned, 1);
        assert!(!tenant_b_page.has_more);
        assert_eq!(tenant_b_page.due[0].thread_id, turn_b.thread_id);
        assert_eq!(
            tenant_b_page.due[0].tenant_id.as_deref(),
            Some("tenant-due-b")
        );

        assert!(
            state
                .scan_due_agent_loop_waits(at_ms, None, 4)
                .await
                .expect("scan unscoped due page")
                .due
                .is_empty()
        );
    }

    #[tokio::test]
    async fn due_wait_scan_is_tenant_isolated_and_keyset_paginated() {
        let memory = StateEngine::new(Arc::new(MemoryEventStore::new()));
        assert_due_wait_tenant_keyset(&memory).await;

        let path = temp_database_path();
        let sqlite = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        assert_due_wait_tenant_keyset(&sqlite).await;
        drop(sqlite);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn memory_due_wait_scan_and_timeout_are_bounded_deterministic_and_idempotent() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-due-memory");
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request,
                    generation,
                    Some(Duration::from_millis(1)),
                    Some(10_000),
                ),
                &authority,
            )
            .await
            .expect("start due wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("wait start event");
        };
        let expires_at_ms = envelope.expires_at_ms.expect("bounded wait expiry");

        assert!(
            state
                .scan_due_agent_loop_waits(expires_at_ms.saturating_sub(1), None, 4)
                .await
                .expect("scan before expiry")
                .due
                .is_empty()
        );
        let page = state
            .scan_due_agent_loop_waits(expires_at_ms, None, 4)
            .await
            .expect("scan at expiry");
        assert_eq!(page.scanned, 1);
        assert!(!page.has_more);
        let due = &page.due[0];
        assert_eq!(due.phase, AgentLoopDuePhase::Waiting);
        assert_eq!(due.thread_id, turn.thread_id);
        assert_eq!(due.turn_id, turn.id);
        assert_eq!(due.wait_id, wait_id);
        assert_eq!(due.due_at_ms, expires_at_ms);

        let applied = state
            .settle_due_agent_loop_wait(due, expires_at_ms)
            .await
            .expect("settle due wait");
        assert_eq!(applied.disposition, EventAppendDisposition::Applied);
        assert_eq!(applied.stored.recorded_at_ms, expires_at_ms);
        assert!(matches!(
            &applied.stored.event,
            StateEvent::WaitClosed {
                status: TurnStatus::TimedOut,
                ..
            }
        ));
        let duplicate = state
            .settle_due_agent_loop_wait(due, expires_at_ms)
            .await
            .expect("retry exact due fence");
        assert_eq!(duplicate.disposition, EventAppendDisposition::Duplicate);
        assert_eq!(duplicate.stored, applied.stored);
        assert!(
            state
                .scan_due_agent_loop_waits(expires_at_ms, None, 4)
                .await
                .expect("scan after terminal settlement")
                .due
                .is_empty()
        );
        let projected = state
            .load_thread(&turn.thread_id)
            .await
            .expect("load timed-out Thread")
            .expect("timed-out Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
    }

    #[tokio::test]
    async fn accepted_denial_is_immediately_due_and_never_becomes_timeout() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-due-denial");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start denial wait");
        let accepted = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-denial-for-maintenance"),
                &denied_approval(request, None, "maintenance denial"),
                &authority,
            )
            .await
            .expect("accept denial");
        let StateEvent::AcceptResume {
            transition:
                Item {
                    kind: ItemKind::AgentLoopResumeAccepted { evidence },
                    ..
                },
            ..
        } = &accepted.event
        else {
            panic!("accepted resume event");
        };
        let accepted_at_ms = evidence.accepted_at_ms;
        let page = state
            .scan_due_agent_loop_waits(accepted_at_ms, None, 1)
            .await
            .expect("scan accepted denial");
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyDeny);
        assert_eq!(page.due[0].due_at_ms, accepted_at_ms);

        let result = state
            .settle_due_agent_loop_wait(&page.due[0], accepted_at_ms)
            .await
            .expect("settle accepted denial");
        assert_eq!(result.disposition, EventAppendDisposition::Applied);
        assert!(matches!(&result.stored.event, StateEvent::DenyWait { .. }));
        assert!(!matches!(
            &result.stored.event,
            StateEvent::WaitClosed {
                status: TurnStatus::TimedOut,
                ..
            }
        ));
        let projected = state
            .load_thread(&turn.thread_id)
            .await
            .expect("load denied Thread")
            .expect("denied Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
    }

    #[tokio::test]
    async fn sqlite_due_projection_survives_reopen_and_is_removed_atomically() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let (turn, expires_at_ms) = {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
            let started = state
                .start_approval_wait_as(
                    &turn,
                    AgentLoopWaitStartCommand::new(
                        AgentLoopWaitId::from_static("wait-due-sqlite-reopen"),
                        request,
                        generation,
                        Some(Duration::from_millis(1)),
                        None,
                    ),
                    &authority,
                )
                .await
                .expect("start SQLite due wait");
            let StateEvent::WaitStarted {
                transition:
                    Item {
                        kind: ItemKind::AgentLoopWaitStarted { envelope },
                        ..
                    },
                ..
            } = &started.event
            else {
                panic!("SQLite wait-start event");
            };
            (turn, envelope.expires_at_ms.expect("SQLite wait expiry"))
        };

        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen projection database"),
        ));
        let page = state
            .scan_due_agent_loop_waits(expires_at_ms, None, 2)
            .await
            .expect("scan reopened projection");
        assert_eq!(page.due.len(), 1);
        state
            .settle_due_agent_loop_wait(&page.due[0], expires_at_ms)
            .await
            .expect("settle reopened due wait");
        drop(state);

        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen terminal database"),
        ));
        assert!(
            reopened
                .scan_due_agent_loop_waits(expires_at_ms, None, 2)
                .await
                .expect("scan terminal projection")
                .due
                .is_empty()
        );
        let projected = reopened
            .load_thread(&turn.thread_id)
            .await
            .expect("load reopened terminal Thread")
            .expect("reopened terminal Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        drop(reopened);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_ready_allow_expiry_uses_envelope_time_and_reopens_terminal() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-due-sqlite-ready-allow");
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start SQLite ReadyAllow wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("SQLite ReadyAllow wait-start event");
        };
        let expires_at_ms = envelope.expires_at_ms.expect("ReadyAllow expiry");
        let settlement = settled_approval(request, None);
        let expected_decided_by = match &settlement.status {
            ApprovalRecordStatus::Settled { decided_by, .. } => decided_by.clone(),
            _ => panic!("settled Approval fixture"),
        };
        let accepted = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-sqlite-ready-allow"),
                &settlement,
                &authority,
            )
            .await
            .expect("accept SQLite ReadyAllow settlement");
        let StateEvent::AcceptResume {
            transition:
                Item {
                    kind: ItemKind::AgentLoopResumeAccepted { evidence },
                    ..
                },
            ..
        } = &accepted.event
        else {
            panic!("SQLite ReadyAllow AcceptResume event");
        };
        let accepted_at_ms = evidence.accepted_at_ms;
        assert!(accepted_at_ms <= expires_at_ms);
        assert_eq!(evidence.settlement.decision, ApprovalDecision::Approve);
        assert_eq!(evidence.settlement.decided_by, expected_decided_by);

        let page = state
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("scan SQLite ReadyAllow expiry");
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyAllow);
        assert_eq!(page.due[0].due_at_ms, expires_at_ms);
        let result = state
            .settle_due_agent_loop_wait(&page.due[0], expires_at_ms)
            .await
            .expect("settle SQLite ReadyAllow expiry");
        assert_eq!(result.disposition, EventAppendDisposition::Applied);
        assert_eq!(result.stored.recorded_at_ms, expires_at_ms);
        let StateEvent::WaitClosed {
            stopped,
            transition,
            status,
            ..
        } = &result.stored.event
        else {
            panic!("SQLite ReadyAllow terminal WaitClosed event");
        };
        assert_eq!(status, &TurnStatus::TimedOut);
        assert_eq!(stopped.created_at_ms, expires_at_ms);
        assert_eq!(transition.created_at_ms, expires_at_ms);
        assert!(matches!(
            &stopped.kind,
            ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: crate::ExecutionPhase::Approval,
            }
        ));
        assert!(matches!(
            &transition.kind,
            ItemKind::AgentLoopWaitClosed { evidence }
                if evidence.previous_revision == 2
                    && evidence.revision == 3
                    && evidence.closed_at_ms == expires_at_ms
                    && evidence.status == TurnStatus::TimedOut
                    && evidence.reason == TurnStopReason::TimedOut
        ));
        assert!(
            sqlite_thread_footprint(&store, &turn.thread_id)
                .await
                .projection
                .is_none()
        );

        drop(state);
        drop(store);
        let reopened_store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen ReadyAllow terminal database"),
        );
        let reopened = StateEngine::new(reopened_store.clone());
        assert!(
            reopened
                .scan_due_agent_loop_waits(expires_at_ms, None, 1)
                .await
                .expect("scan reopened ReadyAllow terminal database")
                .due
                .is_empty()
        );
        assert!(
            sqlite_thread_footprint(&reopened_store, &turn.thread_id)
                .await
                .projection
                .is_none()
        );
        let projected = reopened
            .load_thread(&turn.thread_id)
            .await
            .expect("load reopened ReadyAllow Thread")
            .expect("reopened ReadyAllow Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        drop(reopened);
        drop(reopened_store);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_ready_deny_preserves_real_settlement_and_acceptance_time_across_reopen() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-due-sqlite-ready-deny");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start SQLite ReadyDeny wait");
        let denial_reason = "operator rejected the irreversible production write";
        let settlement = denied_approval(request, None, denial_reason);
        let (expected_decided_by, expected_settled_at_ms) = match &settlement.status {
            ApprovalRecordStatus::Settled { decided_by, .. } => (
                decided_by.clone(),
                settlement.settled_at_ms.expect("denial settlement time"),
            ),
            _ => panic!("denied Approval fixture"),
        };
        let accepted = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-sqlite-ready-deny"),
                &settlement,
                &authority,
            )
            .await
            .expect("accept SQLite ReadyDeny settlement");
        let StateEvent::AcceptResume {
            transition:
                Item {
                    kind: ItemKind::AgentLoopResumeAccepted { evidence },
                    ..
                },
            ..
        } = &accepted.event
        else {
            panic!("SQLite ReadyDeny AcceptResume event");
        };
        let accepted_at_ms = evidence.accepted_at_ms;
        assert!(matches!(
            &evidence.settlement.decision,
            ApprovalDecision::Deny { reason } if reason == denial_reason
        ));
        assert_eq!(evidence.settlement.decided_by, expected_decided_by);
        assert_eq!(evidence.settlement.settled_at_ms, expected_settled_at_ms);

        let page = state
            .scan_due_agent_loop_waits(accepted_at_ms, None, 1)
            .await
            .expect("scan SQLite ReadyDeny settlement");
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyDeny);
        assert_eq!(page.due[0].due_at_ms, accepted_at_ms);
        let later_maintenance_time = accepted_at_ms.saturating_add(10_000);
        let result = state
            .settle_due_agent_loop_wait(&page.due[0], later_maintenance_time)
            .await
            .expect("settle SQLite ReadyDeny");
        assert_eq!(result.disposition, EventAppendDisposition::Applied);
        assert_eq!(result.stored.recorded_at_ms, accepted_at_ms);
        let StateEvent::DenyWait {
            approval_decision,
            transition,
            ..
        } = &result.stored.event
        else {
            panic!("SQLite ReadyDeny terminal DenyWait event");
        };
        assert_eq!(approval_decision.created_at_ms, accepted_at_ms);
        assert_eq!(transition.created_at_ms, accepted_at_ms);
        assert!(matches!(
            &approval_decision.kind,
            ItemKind::ApprovalDecision {
                decision: ApprovalDecision::Deny { reason },
                ..
            } if reason == denial_reason
        ));
        assert!(matches!(
            &transition.kind,
            ItemKind::AgentLoopWaitDenied { evidence }
                if evidence.previous_revision == 2
                    && evidence.revision == 3
                    && evidence.denied_at_ms == accepted_at_ms
                    && matches!(
                        &evidence.settlement.decision,
                        ApprovalDecision::Deny { reason } if reason == denial_reason
                    )
                    && evidence.settlement.decided_by == expected_decided_by
                    && evidence.settlement.settled_at_ms == expected_settled_at_ms
        ));
        assert!(
            sqlite_thread_footprint(&store, &turn.thread_id)
                .await
                .projection
                .is_none()
        );

        drop(state);
        drop(store);
        let reopened_store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen ReadyDeny terminal database"),
        );
        let reopened = StateEngine::new(reopened_store.clone());
        assert!(
            reopened
                .scan_due_agent_loop_waits(later_maintenance_time, None, 1)
                .await
                .expect("scan reopened ReadyDeny terminal database")
                .due
                .is_empty()
        );
        assert!(
            sqlite_thread_footprint(&reopened_store, &turn.thread_id)
                .await
                .projection
                .is_none()
        );
        let projected = reopened
            .load_thread(&turn.thread_id)
            .await
            .expect("load reopened ReadyDeny Thread")
            .expect("reopened ReadyDeny Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        drop(reopened);
        drop(reopened_store);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_projection_insert_abort_rolls_back_event_stream_and_recovery() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let before = sqlite_thread_footprint(&store, &turn.thread_id).await;
        assert!(before.projection.is_none());
        install_projection_fault_trigger(&store, "INSERT").await;

        let error = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    AgentLoopWaitId::from_static("wait-projection-insert-abort"),
                    request,
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect_err("projection INSERT trigger must abort the whole State transaction");
        assert!(error.to_string().contains("projection INSERT"));
        assert_eq!(
            sqlite_thread_footprint(&store, &turn.thread_id).await,
            before
        );

        drop(state);
        drop(store);
        let reopened_store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen projection INSERT rollback database"),
        );
        assert_eq!(
            sqlite_thread_footprint(&reopened_store, &turn.thread_id).await,
            before
        );
        let reopened = StateEngine::new(reopened_store.clone());
        assert!(
            reopened
                .agent_loop_execution(&turn.thread_id, &turn.id)
                .await
                .expect("load INSERT rollback execution")
                .is_none()
        );
        drop(reopened);
        drop(reopened_store);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_projection_update_abort_rolls_back_event_stream_and_recovery() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-projection-update-abort");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start UPDATE rollback wait");
        let before = sqlite_thread_footprint(&store, &turn.thread_id).await;
        assert_eq!(before.projection.as_ref().map(|row| row.phase), Some(1));
        install_projection_fault_trigger(&store, "UPDATE").await;

        let error = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-projection-update-abort"),
                &settled_approval(request, None),
                &authority,
            )
            .await
            .expect_err("projection UPDATE trigger must abort the whole State transaction");
        assert!(error.to_string().contains("projection UPDATE"));
        assert_eq!(
            sqlite_thread_footprint(&store, &turn.thread_id).await,
            before
        );

        drop(state);
        drop(store);
        let reopened_store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen projection UPDATE rollback database"),
        );
        assert_eq!(
            sqlite_thread_footprint(&reopened_store, &turn.thread_id).await,
            before
        );
        let reopened = StateEngine::new(reopened_store.clone());
        assert!(matches!(
            reopened
                .agent_loop_execution(&turn.thread_id, &turn.id)
                .await
                .expect("load UPDATE rollback execution"),
            Some(AgentLoopExecution::Waiting { .. })
        ));
        drop(reopened);
        drop(reopened_store);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_projection_delete_abort_rolls_back_event_stream_and_recovery() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open database"));
        let state = StateEngine::new(store.clone());
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-projection-delete-abort");
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start DELETE rollback wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("DELETE rollback wait-start event");
        };
        let expires_at_ms = envelope.expires_at_ms.expect("DELETE rollback expiry");
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-projection-delete-abort"),
                &settled_approval(request, None),
                &authority,
            )
            .await
            .expect("accept DELETE rollback ReadyAllow settlement");
        let page = state
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("scan DELETE rollback ReadyAllow wait");
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyAllow);
        let before = sqlite_thread_footprint(&store, &turn.thread_id).await;
        assert_eq!(before.projection.as_ref().map(|row| row.phase), Some(2));
        install_projection_fault_trigger(&store, "DELETE").await;

        let error = state
            .settle_due_agent_loop_wait(&page.due[0], expires_at_ms)
            .await
            .expect_err("projection DELETE trigger must abort the whole State transaction");
        assert!(error.to_string().contains("projection DELETE"));
        assert_eq!(
            sqlite_thread_footprint(&store, &turn.thread_id).await,
            before
        );

        drop(state);
        drop(store);
        let reopened_store = Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen projection DELETE rollback database"),
        );
        assert_eq!(
            sqlite_thread_footprint(&reopened_store, &turn.thread_id).await,
            before
        );
        let reopened = StateEngine::new(reopened_store.clone());
        let reopened_page = reopened
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("scan reopened DELETE rollback projection");
        assert_eq!(reopened_page.due.len(), 1);
        assert_eq!(reopened_page.due[0].phase, AgentLoopDuePhase::ReadyAllow);
        drop(reopened);
        drop(reopened_store);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn independent_sqlite_due_settlers_report_one_applied_and_one_duplicate() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let first = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("open first database"),
        ));
        let (turn, expires_at_ms) = start_due_approval_wait(
            &first,
            &authority,
            AgentLoopWaitId::from_static("wait-due-sqlite-two-writers"),
            11,
        )
        .await;
        let second = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("open second database"),
        ));

        let first_page = first
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("first connection due scan");
        let second_page = second
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("second connection due scan");
        assert_eq!(first_page.due.len(), 1);
        assert_eq!(first_page.due, second_page.due);
        let first_due = first_page.due[0].clone();
        let second_due = second_page.due[0].clone();

        let (first_result, second_result) = tokio::join!(
            first.settle_due_agent_loop_wait(&first_due, expires_at_ms),
            second.settle_due_agent_loop_wait(&second_due, expires_at_ms),
        );
        let first_result = first_result.expect("first SQLite settlement");
        let second_result = second_result.expect("second SQLite settlement");
        assert!(matches!(
            (first_result.disposition, second_result.disposition),
            (
                EventAppendDisposition::Applied,
                EventAppendDisposition::Duplicate
            ) | (
                EventAppendDisposition::Duplicate,
                EventAppendDisposition::Applied
            )
        ));
        assert_eq!(first_result.stored, second_result.stored);
        assert!(matches!(
            &first_result.stored.event,
            StateEvent::WaitClosed {
                status: TurnStatus::TimedOut,
                ..
            }
        ));
        assert!(
            first
                .scan_due_agent_loop_waits(expires_at_ms, None, 1)
                .await
                .expect("scan first terminal projection")
                .due
                .is_empty()
        );
        assert!(
            second
                .scan_due_agent_loop_waits(expires_at_ms, None, 1)
                .await
                .expect("scan second terminal projection")
                .due
                .is_empty()
        );
        let projected = second
            .load_thread(&turn.thread_id)
            .await
            .expect("load terminal Thread")
            .expect("terminal Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);

        drop(first);
        drop(second);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn ready_claim_and_due_timeout_have_exactly_one_stream_cas_winner() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-due-claim-race");
        let started = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start claim race wait");
        let StateEvent::WaitStarted {
            transition:
                Item {
                    kind: ItemKind::AgentLoopWaitStarted { envelope },
                    ..
                },
            ..
        } = &started.event
        else {
            panic!("claim race wait start");
        };
        let expires_at_ms = envelope.expires_at_ms.expect("claim race expiry");
        let resume_command_id = AgentLoopResumeCommandId::from_static("resume-claim-race");
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                resume_command_id.clone(),
                &settled_approval(request, None),
                &authority,
            )
            .await
            .expect("accept claim race wait");
        let page = state
            .scan_due_agent_loop_waits(expires_at_ms, None, 1)
            .await
            .expect("scan future due Ready wait");
        assert_eq!(page.due[0].phase, AgentLoopDuePhase::ReadyAllow);
        let due = page.due[0].clone();

        let (claim, timeout) = tokio::join!(
            state.claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id,
                    2,
                    resume_command_id,
                    AgentLoopClaimId::from_static("claim-due-race"),
                    AgentLoopWorkerId::from_static("worker-due-race"),
                ),
                &authority,
            ),
            state.settle_due_agent_loop_wait_as(&due, expires_at_ms, &authority),
        );
        assert_eq!(usize::from(claim.is_ok()) + usize::from(timeout.is_ok()), 1);

        let projected = state
            .load_thread(&turn.thread_id)
            .await
            .expect("load claim race Thread")
            .expect("claim race Thread");
        if claim.is_ok() {
            assert_eq!(projected.turns[0].status, TurnStatus::Running);
            assert!(matches!(
                state
                    .agent_loop_execution(&turn.thread_id, &turn.id)
                    .await
                    .expect("load claim winner"),
                Some(AgentLoopExecution::Executing { .. })
            ));
        } else {
            assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
            assert!(
                state
                    .agent_loop_execution(&turn.thread_id, &turn.id)
                    .await
                    .expect("load timeout winner")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn durable_agent_loop_wait_is_atomic_hidden_snapshot_safe_and_terminal_closed() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-lifecycle");
        let wait_event = state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation.clone(),
                    Some(Duration::from_secs(60)),
                    Some(30_000),
                ),
                &authority,
            )
            .await
            .expect("start durable wait");
        assert!(matches!(wait_event.event, StateEvent::WaitStarted { .. }));
        let waiting = state
            .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
            .await
            .expect("load Waiting")
            .expect("Waiting execution");
        assert!(matches!(waiting, AgentLoopExecution::Waiting { .. }));
        assert_eq!(waiting.revision(), 1);
        assert_eq!(waiting.wait_id(), &wait_id);
        state
            .create_snapshot_as(&turn.thread_id, &authority)
            .await
            .expect("snapshot compound wait event");
        assert!(
            state
                .finish_turn_as(&turn, TurnStatus::Failed, &authority)
                .await
                .expect_err("Waiting terminal fence")
                .to_string()
                .contains("Waiting or Ready")
        );

        let settlement =
            settled_approval(request.clone(), authority.tenant_id().map(str::to_owned));
        let command_id = AgentLoopResumeCommandId::from_static("resume-lifecycle");
        let resume_event = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                command_id.clone(),
                &settlement,
                &authority,
            )
            .await
            .expect("accept resume");
        let duplicate_resume = state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                command_id.clone(),
                &settlement,
                &authority,
            )
            .await
            .expect("idempotent resume");
        assert_eq!(resume_event.sequence, duplicate_resume.sequence);
        assert!(matches!(
            state
                .agent_loop_execution(&turn.thread_id, &turn.id)
                .await
                .expect("load Ready"),
            Some(AgentLoopExecution::Ready { .. })
        ));
        assert!(
            state
                .finish_turn(&turn, TurnStatus::Failed)
                .await
                .expect_err("Ready terminal fence")
                .to_string()
                .contains("Waiting or Ready")
        );

        let claim_id = AgentLoopClaimId::from_static("claim-lifecycle");
        let worker_id = AgentLoopWorkerId::from_static("worker-lifecycle");
        let claim_event = state
            .claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id.clone(),
                    2,
                    command_id.clone(),
                    claim_id.clone(),
                    worker_id.clone(),
                ),
                &authority,
            )
            .await
            .expect("claim Ready");
        let duplicate_claim = state
            .claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id.clone(),
                    2,
                    command_id.clone(),
                    claim_id.clone(),
                    worker_id.clone(),
                ),
                &authority,
            )
            .await
            .expect("idempotent claim");
        assert_eq!(claim_event.sequence, duplicate_claim.sequence);
        assert!(
            state
                .claim_ready_as(
                    &turn,
                    AgentLoopReadyClaimCommand::new(
                        wait_id.clone(),
                        2,
                        command_id.clone(),
                        claim_id,
                        AgentLoopWorkerId::from_static("worker-lifecycle-other"),
                    ),
                    &authority,
                )
                .await
                .is_err()
        );
        assert!(matches!(
            state
                .agent_loop_execution(&turn.thread_id, &turn.id)
                .await
                .expect("load worker-bound claim"),
            Some(AgentLoopExecution::Executing { claim, .. })
                if claim.worker_id == worker_id
        ));
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ToolResult {
                    call_id: request.authorization.call_id.clone(),
                    output: json!({"written": true}),
                    is_error: false,
                    connector_evidence: Vec::new(),
                }),
                &authority,
            )
            .await
            .expect("append authoritative ToolResult");
        let (projected, receipt) =
            append_completion_candidate(&state, &turn, &authority, "record written").await;
        state
            .complete_turn_as(&projected, receipt, &authority)
            .await
            .expect("complete claimed execution");
        assert!(
            state
                .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                .await
                .expect("load terminal execution")
                .is_none()
        );
        let terminal = state
            .load_thread(&turn.thread_id)
            .await
            .expect("load terminal Thread")
            .expect("terminal Thread");
        let terminal_turn = terminal
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .expect("terminal Turn");
        let model_visible = crate::context::model_visible_items(&terminal_turn.items);
        assert!(model_visible.iter().all(|item| {
            !matches!(
                item.kind,
                ItemKind::AgentLoopWaitStarted { .. }
                    | ItemKind::AgentLoopResumeAccepted { .. }
                    | ItemKind::AgentLoopReadyClaimed { .. }
            )
        }));
        state
            .create_snapshot(&turn.thread_id)
            .await
            .expect("snapshot terminal wait history");
        let forked = state
            .fork_thread(
                &turn.thread_id,
                ThreadId::from_static("forked-wait-thread"),
                None,
            )
            .await
            .expect("fork terminal wait history");
        assert!(
            forked.turns[0]
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::AgentLoopReadyClaimed { .. }))
        );
        let archive = state
            .export_thread(&turn.thread_id)
            .await
            .expect("export wait archive");
        let encoded = super::encode_thread_archive(&archive).expect("encode wait archive");
        let decoded = super::decode_thread_archive(&encoded).expect("decode wait archive");
        let imported = state
            .import_thread(&decoded, ThreadId::from_static("imported-wait-thread"))
            .await
            .expect("import wait archive");
        assert!(
            imported.turns[0]
                .items
                .iter()
                .any(|item| { matches!(item.kind, ItemKind::AgentLoopReadyClaimed { .. }) })
        );
    }

    #[tokio::test]
    async fn memory_recovery_refuses_executing_wait_without_blocking_worker_settlement() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let turn = executing_approval_wait_fixture(&state, &authority).await;

        let error = state
            .recover_thread_as(&turn.thread_id, &turn.id, &authority)
            .await
            .expect_err("recovery must not overwrite an Executing claim");
        assert!(
            error
                .to_string()
                .contains("exclusive recovery cannot interrupt")
        );
        let execution = state
            .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
            .await
            .expect("load recovery-fenced execution");
        assert!(matches!(
            execution,
            Some(AgentLoopExecution::Executing { .. })
        ));

        state
            .finish_turn_as(&turn, TurnStatus::Failed, &authority)
            .await
            .expect("the owning worker may still settle its Executing Turn");
        let projected = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load worker-settled Thread")
            .expect("worker-settled Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
    }

    #[tokio::test]
    async fn recovery_and_wait_start_have_exactly_one_cas_winner() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-recovery-race");

        let (wait, recovery) = tokio::join!(
            state.start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(wait_id.clone(), request, generation, None, None,),
                &authority,
            ),
            state.recover_thread_as(&turn.thread_id, &turn.id, &authority),
        );
        assert_eq!(usize::from(wait.is_ok()) + usize::from(recovery.is_ok()), 1);

        let projected = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load recovery race Thread")
            .expect("recovery race Thread");
        if wait.is_ok() {
            assert_eq!(projected.turns[0].status, TurnStatus::Running);
            assert!(matches!(
                state
                    .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                    .await
                    .expect("load wait winner"),
                Some(AgentLoopExecution::Waiting { .. })
            ));
        } else {
            assert_eq!(projected.turns[0].status, TurnStatus::Interrupted);
            assert!(
                state
                    .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                    .await
                    .expect("load recovery winner")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn sqlite_reopen_recovery_refuses_executing_wait() {
        let path = temp_database_path();
        let authority = AuthorityContext::local_process();
        let turn = {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            executing_approval_wait_fixture(&state, &authority).await
        };

        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen database"),
        ));
        let error = state
            .recover_thread_as(&turn.thread_id, &turn.id, &authority)
            .await
            .expect_err("reopened recovery must not overwrite an Executing claim");
        assert!(
            error
                .to_string()
                .contains("exclusive recovery cannot interrupt")
        );
        assert!(matches!(
            state
                .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                .await
                .expect("load reopened execution"),
            Some(AgentLoopExecution::Executing { .. })
        ));

        drop(state);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn lifecycle_event_ids_are_fixed_global_and_accept_maximum_close_command() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let wait_id = AgentLoopWaitId::from_static("shared-wait-identity");
        let maximum_command = AgentLoopCloseCommandId::from_string("c".repeat(256));

        let (first_turn, first_request, first_generation) =
            approval_wait_fixture(&state, &authority).await;
        let first_started = state
            .start_approval_wait_as(
                &first_turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    first_request,
                    first_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start first shared-identity wait");
        let first_closed = state
            .close_wait_as(
                &first_turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    maximum_command.clone(),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("maximum legal close command must fit its derived Event identity");
        let first_retry = state
            .close_wait_as(
                &first_turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    maximum_command.clone(),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("exact maximum close command retry");
        assert_eq!(first_closed.sequence, first_retry.sequence);
        assert_eq!(first_closed.event_id, first_retry.event_id);

        let (second_turn, second_request, second_generation) =
            approval_wait_fixture(&state, &authority).await;
        let second_started = state
            .start_approval_wait_as(
                &second_turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    second_request,
                    second_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("same wait identity is valid in another Thread");
        let second_closed = state
            .close_wait_as(
                &second_turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    maximum_command.clone(),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("same close command is valid in another Thread");

        assert_ne!(first_started.event_id, second_started.event_id);
        assert_ne!(first_closed.event_id, second_closed.event_id);
        assert_eq!(
            first_started.event_id,
            super::agent_loop_lifecycle_event_id(
                super::AgentLoopLifecycleEvent::WaitStarted,
                &first_turn.thread_id,
                &first_turn.id,
                wait_id.as_str(),
            )
            .expect("derive wait Event identity")
        );
        assert_eq!(
            first_closed.event_id,
            super::agent_loop_lifecycle_event_id(
                super::AgentLoopLifecycleEvent::WaitClosed,
                &first_turn.thread_id,
                &first_turn.id,
                maximum_command.as_str(),
            )
            .expect("derive close Event identity")
        );

        let variants = [
            super::AgentLoopLifecycleEvent::WaitStarted,
            super::AgentLoopLifecycleEvent::ResumeAccepted,
            super::AgentLoopLifecycleEvent::WaitDenied,
            super::AgentLoopLifecycleEvent::WaitClosed,
            super::AgentLoopLifecycleEvent::ReadyClaimed,
        ];
        let derived = variants
            .into_iter()
            .map(|event| {
                super::agent_loop_lifecycle_event_id(
                    event,
                    &first_turn.thread_id,
                    &first_turn.id,
                    "same-stable-identity",
                )
                .expect("derive lifecycle Event identity")
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(derived.len(), 5, "each lifecycle phase is domain-separated");
        assert!(derived.iter().all(|event_id| {
            event_id.as_str().starts_with("agent-loop-event-v1-")
                && event_id.as_str().len() == "agent-loop-event-v1-".len() + 64
        }));
    }

    #[tokio::test]
    async fn sqlite_lifecycle_event_ids_allow_cross_thread_command_reuse() {
        let path = temp_database_path();
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path).await.expect("open database"),
        ));
        let authority = AuthorityContext::local_process();
        let wait_id = AgentLoopWaitId::from_static("sqlite-shared-wait");
        let command_id = AgentLoopCloseCommandId::from_string("x".repeat(256));
        let mut closed = Vec::new();

        for _ in 0..2 {
            let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
            state
                .start_approval_wait_as(
                    &turn,
                    AgentLoopWaitStartCommand::new(
                        wait_id.clone(),
                        request,
                        generation,
                        None,
                        None,
                    ),
                    &authority,
                )
                .await
                .expect("same wait identity must be scoped to its SQLite Thread");
            let event = state
                .close_wait_as(
                    &turn,
                    AgentLoopWaitCloseCommand::new(
                        wait_id.clone(),
                        1,
                        command_id.clone(),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &authority,
                )
                .await
                .expect("same maximum close command must be scoped to its SQLite Thread");
            let retry = state
                .close_wait_as(
                    &turn,
                    AgentLoopWaitCloseCommand::new(
                        wait_id.clone(),
                        1,
                        command_id.clone(),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &authority,
                )
                .await
                .expect("SQLite exact close retry");
            assert_eq!(event.sequence, retry.sequence);
            assert_eq!(event.event_id, retry.event_id);
            closed.push(event.event_id);
        }
        assert_ne!(closed[0], closed[1]);

        drop(state);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn wait_close_is_atomic_idempotent_hidden_and_materialization_safe() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-close-cancel");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request,
                    generation,
                    Some(Duration::from_secs(60)),
                    Some(30_000),
                ),
                &authority,
            )
            .await
            .expect("start cancellable wait");

        let command_id = AgentLoopCloseCommandId::from_static("close-wait-cancel");
        let closed = state
            .close_wait_as(
                &turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    command_id.clone(),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("cancel Waiting execution");
        let duplicate = state
            .close_wait_as(
                &turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    command_id.clone(),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("retry exact wait closure");
        assert_eq!(closed.sequence, duplicate.sequence);
        let StateEvent::WaitClosed {
            stopped,
            transition,
            status,
            ..
        } = &closed.event
        else {
            panic!("close_wait must append one WaitClosed event");
        };
        assert_eq!(status, &TurnStatus::Cancelled);
        assert_eq!(stopped.created_at_ms, transition.created_at_ms);
        assert!(matches!(
            &stopped.kind,
            ItemKind::TurnStopped {
                reason: TurnStopReason::Cancelled,
                phase: crate::ExecutionPhase::Approval,
            }
        ));
        assert!(matches!(
            &transition.kind,
            ItemKind::AgentLoopWaitClosed { evidence }
                if evidence.command_id == command_id
                    && evidence.previous_revision == 1
                    && evidence.revision == 2
        ));
        assert!(
            state
                .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                .await
                .expect("load closed execution")
                .is_none()
        );

        let reused = state
            .close_wait_as(
                &turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    command_id,
                    TurnStatus::TimedOut,
                    TurnStopReason::TimedOut,
                ),
                &authority,
            )
            .await
            .expect_err("close command content reuse must fail closed");
        assert!(reused.to_string().contains("reused with different content"));
        assert!(
            state
                .close_wait(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopCloseCommandId::from_static("close-wait-stale"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                )
                .await
                .expect_err("new close command cannot replay a terminal Turn")
                .to_string()
                .contains("not running")
        );

        let terminal = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load closed Thread")
            .expect("closed Thread");
        let terminal_turn = terminal
            .turns
            .iter()
            .find(|candidate| candidate.id == turn.id)
            .expect("closed Turn");
        assert_eq!(terminal_turn.status, TurnStatus::Cancelled);
        assert!(terminal_turn.completion_receipt.is_none());
        assert!(
            crate::context::model_visible_items(&terminal_turn.items)
                .iter()
                .all(|item| !matches!(&item.kind, ItemKind::AgentLoopWaitClosed { .. }))
        );

        let mut non_adjacent = terminal_turn.clone();
        let last = non_adjacent.items.len() - 1;
        non_adjacent.items.swap(last - 1, last);
        assert!(
            super::agent_loop_execution_projection(&non_adjacent)
                .expect_err("closure evidence must remain adjacent to TurnStopped")
                .to_string()
                .contains("adjacent TurnStopped")
        );
        assert!(super::validate_state_event_schema(&closed.event, 15).is_err());

        state
            .create_snapshot_as(&turn.thread_id, &authority)
            .await
            .expect("snapshot atomic wait closure");
        let forked = state
            .fork_thread_as(
                &authority,
                &turn.thread_id,
                ThreadId::from_static("forked-closed-wait"),
                Some(&turn.id),
            )
            .await
            .expect("fork closed wait");
        assert_eq!(forked.turns[0].status, TurnStatus::Cancelled);
        assert!(
            forked.turns[0]
                .items
                .iter()
                .any(|item| matches!(&item.kind, ItemKind::AgentLoopWaitClosed { .. }))
        );

        let archive = state
            .export_thread_as(&turn.thread_id, &authority)
            .await
            .expect("export closed wait");
        assert!(
            archive
                .events
                .iter()
                .any(|stored| matches!(&stored.event, StateEvent::WaitClosed { .. }))
        );
        let mut legacy_format = archive.clone();
        legacy_format.format_version = 5;
        assert!(super::validate_thread_archive(&legacy_format).is_err());
        let encoded = super::encode_thread_archive(&archive).expect("encode closed wait archive");
        let decoded = super::decode_thread_archive(&encoded).expect("decode closed wait archive");
        let imported = state
            .import_thread_as(
                &decoded,
                ThreadId::from_static("imported-closed-wait"),
                &authority,
            )
            .await
            .expect("import closed wait");
        assert_eq!(imported.turns[0].status, TurnStatus::Cancelled);
        assert!(
            imported.turns[0]
                .items
                .iter()
                .any(|item| matches!(&item.kind, ItemKind::AgentLoopWaitClosed { .. }))
        );
        state
            .create_snapshot_as(&imported.id, &authority)
            .await
            .expect("snapshot imported closure");
    }

    #[tokio::test]
    async fn wait_denial_is_atomic_idempotent_hidden_and_materialization_safe() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-deny-atomic");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    Some(30_000),
                ),
                &authority,
            )
            .await
            .expect("start denial wait");
        let settlement = denied_approval(request, None, "operator denied");
        let command_id = AgentLoopDenyCommandId::from_static("deny-wait-atomic");
        let denied = state
            .deny_wait_as(
                &turn,
                &wait_id,
                1,
                command_id.clone(),
                &settlement,
                &authority,
            )
            .await
            .expect("deny Waiting execution");
        let duplicate = state
            .deny_wait_as(
                &turn,
                &wait_id,
                1,
                command_id.clone(),
                &settlement,
                &authority,
            )
            .await
            .expect("retry exact denial");
        assert_eq!(denied.sequence, duplicate.sequence);
        let StateEvent::DenyWait {
            approval_decision,
            transition,
            ..
        } = &denied.event
        else {
            panic!("deny_wait must append one DenyWait event");
        };
        assert_eq!(approval_decision.created_at_ms, transition.created_at_ms);
        assert!(matches!(
            &approval_decision.kind,
            ItemKind::ApprovalDecision {
                decision: ApprovalDecision::Deny { reason },
                ..
            } if reason == "operator denied"
        ));
        assert!(matches!(
            &transition.kind,
            ItemKind::AgentLoopWaitDenied { evidence }
                if evidence.command_id == command_id
                    && evidence.previous_revision == 1
                    && evidence.revision == 2
        ));
        let projected = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load denied Thread")
            .expect("denied Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(
            state
                .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                .await
                .expect("load terminal denial")
                .is_none()
        );
        assert!(
            crate::context::model_visible_items(&projected.turns[0].items)
                .iter()
                .all(|item| !matches!(&item.kind, ItemKind::AgentLoopWaitDenied { .. }))
        );

        let mut changed = settlement.clone();
        changed.status = ApprovalRecordStatus::Settled {
            decision: ApprovalDecision::Deny {
                reason: "different denial".to_owned(),
            },
            decided_by: ActorIdentity::Authenticated {
                authority: "test-approver".to_owned(),
                subject: "operator-7".to_owned(),
            },
        };
        assert!(
            state
                .deny_wait_as(&turn, &wait_id, 1, command_id, &changed, &authority,)
                .await
                .expect_err("denial command content drift must fail")
                .to_string()
                .contains("reused with different content")
        );
        assert!(
            state
                .deny_wait(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopDenyCommandId::from_static("deny-wait-stale"),
                    &settlement,
                )
                .await
                .expect_err("new denial cannot replay a terminal Turn")
                .to_string()
                .contains("not running")
        );
        assert!(super::validate_state_event_schema(&denied.event, 15).is_err());

        state
            .create_snapshot_as(&turn.thread_id, &authority)
            .await
            .expect("snapshot atomic denial");
        let forked = state
            .fork_thread_as(
                &authority,
                &turn.thread_id,
                ThreadId::from_static("forked-denied-wait"),
                Some(&turn.id),
            )
            .await
            .expect("fork denied wait");
        assert_eq!(forked.turns[0].status, TurnStatus::Failed);
        assert!(
            forked.turns[0]
                .items
                .iter()
                .any(|item| matches!(&item.kind, ItemKind::AgentLoopWaitDenied { .. }))
        );
        let archive = state
            .export_thread_as(&turn.thread_id, &authority)
            .await
            .expect("export denied wait");
        assert!(
            archive
                .events
                .iter()
                .any(|stored| matches!(&stored.event, StateEvent::DenyWait { .. }))
        );
        let encoded = super::encode_thread_archive(&archive).expect("encode denial archive");
        let decoded = super::decode_thread_archive(&encoded).expect("decode denial archive");
        let imported = state
            .import_thread_as(
                &decoded,
                ThreadId::from_static("imported-denied-wait"),
                &authority,
            )
            .await
            .expect("import denial archive");
        assert_eq!(imported.turns[0].status, TurnStatus::Failed);
        state
            .create_snapshot_as(&imported.id, &authority)
            .await
            .expect("snapshot imported denial");
    }

    #[tokio::test]
    async fn wait_denial_converges_only_from_waiting_or_the_same_denial_ready_state() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();

        let (ready_turn, ready_request, ready_generation) =
            approval_wait_fixture(&state, &authority).await;
        let ready_wait_id = AgentLoopWaitId::from_static("wait-deny-ready");
        state
            .start_approval_wait_as(
                &ready_turn,
                AgentLoopWaitStartCommand::new(
                    ready_wait_id.clone(),
                    ready_request.clone(),
                    ready_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start Ready denial wait");
        let ready_denial = denied_approval(ready_request, None, "ready denial");
        state
            .accept_resume_as(
                &ready_turn,
                &ready_wait_id,
                1,
                AgentLoopResumeCommandId::from_static("accept-ready-denial"),
                &ready_denial,
                &authority,
            )
            .await
            .expect("accept denial into Ready");
        assert!(
            state
                .claim_ready_as(
                    &ready_turn,
                    AgentLoopReadyClaimCommand::new(
                        ready_wait_id.clone(),
                        2,
                        AgentLoopResumeCommandId::from_static("accept-ready-denial"),
                        AgentLoopClaimId::from_static("claim-ready-denial"),
                        AgentLoopWorkerId::from_static("worker-ready-denial"),
                    ),
                    &authority,
                )
                .await
                .expect_err("accepted denial can never cross the Tool-effect claim boundary")
                .to_string()
                .contains("accepted Deny")
        );
        assert!(
            state
                .close_wait_as(
                    &ready_turn,
                    AgentLoopWaitCloseCommand::new(
                        ready_wait_id.clone(),
                        2,
                        AgentLoopCloseCommandId::from_static("close-ready-denial"),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &authority,
                )
                .await
                .expect_err("accepted denial cannot be overwritten by cancellation")
                .to_string()
                .contains("accepted Deny")
        );
        state
            .deny_wait_as(
                &ready_turn,
                &ready_wait_id,
                2,
                AgentLoopDenyCommandId::from_static("settle-ready-denial"),
                &ready_denial,
                &authority,
            )
            .await
            .expect("same denial converges from Ready");
        let ready_projected = state
            .load_thread(&ready_turn.thread_id)
            .await
            .expect("load Ready denial")
            .expect("Ready denial Thread");
        assert_eq!(ready_projected.turns[0].status, TurnStatus::Failed);

        let (approved_turn, approved_request, approved_generation) =
            approval_wait_fixture(&state, &authority).await;
        let approved_wait_id = AgentLoopWaitId::from_static("wait-deny-approved-ready");
        state
            .start_approval_wait_as(
                &approved_turn,
                AgentLoopWaitStartCommand::new(
                    approved_wait_id.clone(),
                    approved_request.clone(),
                    approved_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start approved Ready wait");
        state
            .accept_resume_as(
                &approved_turn,
                &approved_wait_id,
                1,
                AgentLoopResumeCommandId::from_static("accept-approved-ready"),
                &settled_approval(approved_request.clone(), None),
                &authority,
            )
            .await
            .expect("accept approval into Ready");
        assert!(
            state
                .deny_wait_as(
                    &approved_turn,
                    &approved_wait_id,
                    2,
                    AgentLoopDenyCommandId::from_static("deny-approved-ready"),
                    &denied_approval(approved_request, None, "late conflicting denial"),
                    &authority,
                )
                .await
                .expect_err("Approve-ready execution cannot become denied")
                .to_string()
                .contains("differs")
        );
    }

    #[tokio::test]
    async fn wait_denial_rejects_wrong_lifecycle_authority_and_stale_revision() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise".to_owned(),
                subject: "requester".to_owned(),
            },
            Some("tenant-denial".to_owned()),
        )
        .expect("request authority");
        let other_actor = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise".to_owned(),
                subject: "other-actor".to_owned(),
            },
            Some("tenant-denial".to_owned()),
        )
        .expect("other authority");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-denial-fences");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start fenced denial wait");
        let denial = denied_approval(
            request.clone(),
            authority.tenant_id().map(str::to_owned),
            "denied",
        );
        assert!(
            state
                .deny_wait_as(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopDenyCommandId::from_static("wrong-actor-denial"),
                    &denial,
                    &other_actor,
                )
                .await
                .expect_err("same-tenant actor substitution must fail")
                .to_string()
                .contains("requester")
        );
        assert!(
            state
                .deny_wait_as(
                    &turn,
                    &wait_id,
                    2,
                    AgentLoopDenyCommandId::from_static("stale-denial-revision"),
                    &denial,
                    &authority,
                )
                .await
                .expect_err("future revision must fail")
                .to_string()
                .contains("current revision")
        );
        let approval = settled_approval(request.clone(), authority.tenant_id().map(str::to_owned));
        assert!(
            state
                .deny_wait_as(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopDenyCommandId::from_static("approve-is-not-denial"),
                    &approval,
                    &authority,
                )
                .await
                .expect_err("Approve must not enter denial event")
                .to_string()
                .contains("requires a settled Deny")
        );
        let mut pending = denial.clone();
        pending.status = ApprovalRecordStatus::Pending;
        pending.revision = 1;
        pending.settled_at_ms = None;
        assert!(
            state
                .deny_wait_as(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopDenyCommandId::from_static("pending-is-not-denial"),
                    &pending,
                    &authority,
                )
                .await
                .expect_err("Pending must not enter denial event")
                .to_string()
                .contains("terminal Approval Inbox")
        );
        let mut orphaned = denial.clone();
        orphaned.status = ApprovalRecordStatus::Orphaned {
            reason: "orphaned".to_owned(),
        };
        assert!(
            state
                .deny_wait_as(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopDenyCommandId::from_static("orphan-is-not-denial"),
                    &orphaned,
                    &authority,
                )
                .await
                .expect_err("Orphan must not enter denial event")
                .to_string()
                .contains("settled")
        );
    }

    #[tokio::test]
    async fn denial_races_accept_and_close_with_exactly_one_cas_winner() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (accept_turn, accept_request, accept_generation) =
            approval_wait_fixture(&state, &authority).await;
        let accept_wait_id = AgentLoopWaitId::from_static("denial-accept-race");
        state
            .start_approval_wait_as(
                &accept_turn,
                AgentLoopWaitStartCommand::new(
                    accept_wait_id.clone(),
                    accept_request.clone(),
                    accept_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start denial-accept race");
        let accept_denial = denied_approval(accept_request, None, "race denial");
        let (accepted, denied) = tokio::join!(
            state.accept_resume_as(
                &accept_turn,
                &accept_wait_id,
                1,
                AgentLoopResumeCommandId::from_static("accept-denial-race"),
                &accept_denial,
                &authority,
            ),
            state.deny_wait_as(
                &accept_turn,
                &accept_wait_id,
                1,
                AgentLoopDenyCommandId::from_static("deny-accept-race"),
                &accept_denial,
                &authority,
            )
        );
        assert_eq!(
            usize::from(accepted.is_ok()) + usize::from(denied.is_ok()),
            1
        );
        if accepted.is_ok() {
            state
                .deny_wait_as(
                    &accept_turn,
                    &accept_wait_id,
                    2,
                    AgentLoopDenyCommandId::from_static("converge-accept-race"),
                    &accept_denial,
                    &authority,
                )
                .await
                .expect("accepted denial can converge in a new CAS");
        }

        let (close_turn, close_request, close_generation) =
            approval_wait_fixture(&state, &authority).await;
        let close_wait_id = AgentLoopWaitId::from_static("denial-close-race");
        state
            .start_approval_wait_as(
                &close_turn,
                AgentLoopWaitStartCommand::new(
                    close_wait_id.clone(),
                    close_request.clone(),
                    close_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start denial-close race");
        let close_denial = denied_approval(close_request, None, "race denial");
        let (denied, closed) = tokio::join!(
            state.deny_wait_as(
                &close_turn,
                &close_wait_id,
                1,
                AgentLoopDenyCommandId::from_static("deny-close-race"),
                &close_denial,
                &authority,
            ),
            state.close_wait_as(
                &close_turn,
                AgentLoopWaitCloseCommand::new(
                    close_wait_id.clone(),
                    1,
                    AgentLoopCloseCommandId::from_static("close-denial-race"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
        );
        assert_eq!(usize::from(denied.is_ok()) + usize::from(closed.is_ok()), 1);
        let projected = state
            .load_thread(&close_turn.thread_id)
            .await
            .expect("load denial-close race")
            .expect("denial-close race Thread");
        assert!(matches!(
            projected.turns[0].status,
            TurnStatus::Failed | TurnStatus::Cancelled
        ));

        let (ready_turn, ready_request, ready_generation) =
            approval_wait_fixture(&state, &authority).await;
        let ready_wait_id = AgentLoopWaitId::from_static("ready-denial-close-race");
        state
            .start_approval_wait_as(
                &ready_turn,
                AgentLoopWaitStartCommand::new(
                    ready_wait_id.clone(),
                    ready_request.clone(),
                    ready_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start Ready-denial close race");
        let ready_denial = denied_approval(ready_request, None, "accepted race denial");
        let ready_resume_command_id =
            AgentLoopResumeCommandId::from_static("accept-ready-race-denial");
        state
            .accept_resume_as(
                &ready_turn,
                &ready_wait_id,
                1,
                ready_resume_command_id.clone(),
                &ready_denial,
                &authority,
            )
            .await
            .expect("accept race denial into Ready");
        let (denied, closed, claimed) = tokio::join!(
            state.deny_wait_as(
                &ready_turn,
                &ready_wait_id,
                2,
                AgentLoopDenyCommandId::from_static("deny-ready-close-race"),
                &ready_denial,
                &authority,
            ),
            state.close_wait_as(
                &ready_turn,
                AgentLoopWaitCloseCommand::new(
                    ready_wait_id.clone(),
                    2,
                    AgentLoopCloseCommandId::from_static("close-ready-denial-race"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            ),
            state.claim_ready_as(
                &ready_turn,
                AgentLoopReadyClaimCommand::new(
                    ready_wait_id.clone(),
                    2,
                    ready_resume_command_id.clone(),
                    AgentLoopClaimId::from_static("claim-ready-denial-race"),
                    AgentLoopWorkerId::from_static("worker-ready-denial-race"),
                ),
                &authority,
            )
        );
        denied.expect("accepted denial must win over concurrent cancellation");
        closed.expect_err("cancellation must not overwrite accepted denial");
        claimed.expect_err("claim must not cross an accepted denial");
        let projected = state
            .load_thread(&ready_turn.thread_id)
            .await
            .expect("load Ready-denial close race")
            .expect("Ready-denial race Thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
    }

    #[test]
    fn wait_close_is_admitted_through_the_terminal_capacity_reserve() {
        let stopped = Item {
            id: crate::ItemId::from_static("terminal-reserve-stop"),
            created_at_ms: 1,
            kind: ItemKind::TurnStopped {
                reason: TurnStopReason::Cancelled,
                phase: crate::ExecutionPhase::Approval,
            },
        };
        let transition = Item {
            id: crate::ItemId::from_static("terminal-reserve-close"),
            created_at_ms: 1,
            kind: ItemKind::AgentLoopWaitClosed {
                evidence: Box::new(crate::WaitClosureEvidence {
                    wait_id: AgentLoopWaitId::from_static("terminal-reserve-wait"),
                    previous_revision: 1,
                    revision: 2,
                    command_id: AgentLoopCloseCommandId::from_static("terminal-reserve-command"),
                    status: TurnStatus::Cancelled,
                    reason: TurnStopReason::Cancelled,
                    command_sha256: "0".repeat(64),
                    closed_at_ms: 1,
                }),
            },
        };
        let pending = PendingEvent {
            event_id: EventId::from_static("terminal-reserve-event"),
            thread_id: ThreadId::from_static("terminal-reserve-thread"),
            expected_stream_version: super::STATE_THREAD_EVENT_LIMIT
                .saturating_sub(super::STATE_TERMINAL_EVENT_RESERVE),
            expected_stream_recovery_bytes: 0,
            recorded_at_ms: 1,
            event: StateEvent::WaitClosed {
                turn_id: TurnId::from_static("terminal-reserve-turn"),
                stopped,
                transition,
                status: TurnStatus::Cancelled,
            },
        };
        super::validate_pending_event(&pending)
            .expect("atomic wait closure may use the terminal event reserve");
    }

    #[tokio::test]
    async fn maximum_approval_denial_after_wait_started_fits_only_terminal_capacity() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let thread = state.create_thread().await.expect("create maximum Thread");
        let turn = state
            .start_turn(&thread.id)
            .await
            .expect("start maximum Turn");
        let descriptor = ToolDescriptor {
            name: "maximum_write".to_owned(),
            description: "Writes one maximum bounded record".to_owned(),
            input_schema: json!({"type": "object"}),
        };
        let mut request = ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: authority.actor().clone(),
            authorization: ToolAuthorization {
                thread_id: thread.id.clone(),
                turn_id: turn.id.clone(),
                call_id: "maximum-denial-call".to_owned(),
                descriptor: descriptor.clone(),
                origin: CapabilityOrigin::BuiltIn,
                input: json!({"padding": ""}),
            },
            reason: "maximum record requires approval".to_owned(),
            risk: RiskLevel::High,
        };
        let denial_reason = "\\".repeat(4_096);
        let mut low = 0_usize;
        let mut high = crate::approval::MAX_APPROVAL_RECORD_BYTES;
        let mut best = 0_usize;
        while low <= high {
            let middle = low + (high - low) / 2;
            request.authorization.input = json!({"padding": "x".repeat(middle)});
            let candidate = denied_approval(request.clone(), None, &denial_reason);
            let encoded = serde_json::to_vec(&candidate).expect("encode candidate record");
            if encoded.len() <= crate::approval::MAX_APPROVAL_RECORD_BYTES {
                best = middle;
                low = middle.saturating_add(1);
            } else if middle == 0 {
                break;
            } else {
                high = middle - 1;
            }
        }
        request.authorization.input = json!({"padding": "x".repeat(best)});
        let settlement = denied_approval(request.clone(), None, &denial_reason);
        assert_eq!(
            serde_json::to_vec(&settlement)
                .expect("encode maximum record")
                .len(),
            crate::approval::MAX_APPROVAL_RECORD_BYTES
        );
        let mut oversized_request = request.clone();
        oversized_request.authorization.input = json!({"padding": "x".repeat(best + 1)});
        assert!(
            serde_json::to_vec(&denied_approval(oversized_request, None, &denial_reason,))
                .expect("encode oversized record")
                .len()
                > crate::approval::MAX_APPROVAL_RECORD_BYTES
        );

        state
            .append_item(
                &turn,
                Item::new(ItemKind::ToolCall {
                    model_id: Some("test/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: request.authorization.call_id.clone(),
                    name: descriptor.name.clone(),
                    input: request.authorization.input.clone(),
                    batch: None,
                }),
            )
            .await
            .expect("append maximum ToolCall");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: request.authorization.call_id.clone(),
                    tool_origin: Some(CapabilityOrigin::BuiltIn),
                    decision: PolicyDecision::Ask {
                        reason: request.reason.clone(),
                        risk: request.risk,
                    },
                }),
            )
            .await
            .expect("append maximum Ask");
        let generation = test_completion_generation(
            &completion_model_request_sha256(&json!({
                "turn": turn.id.as_str(),
                "call": request.authorization.call_id.as_str(),
            }))
            .expect("maximum Model request digest"),
        );
        let wait_id = AgentLoopWaitId::from_static("maximum-denial-wait");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(wait_id.clone(), request, generation, None, None),
                &authority,
            )
            .await
            .expect("start maximum denial wait");
        let denied = state
            .deny_wait_as(
                &turn,
                &wait_id,
                1,
                AgentLoopDenyCommandId::from_static("maximum-denial-command"),
                &settlement,
                &authority,
            )
            .await
            .expect("maximum denial must settle");
        let encoded = super::encode_state_event(&denied.event).expect("encode maximum denial");
        assert!(encoded.recovery_bytes <= super::STATE_TERMINAL_RECOVERY_BYTE_RESERVE);
        let expected_recovery_bytes = super::STATE_THREAD_RECOVERY_BYTE_LIMIT
            .checked_sub(encoded.recovery_bytes)
            .expect("terminal event fits hard limit");
        let capacity = super::state_capacity(
            super::STATE_THREAD_EVENT_LIMIT.saturating_sub(super::STATE_TERMINAL_EVENT_RESERVE),
            expected_recovery_bytes,
        );
        assert_eq!(capacity.general_events_remaining, 0);
        assert_eq!(capacity.general_recovery_bytes_remaining, 0);
        let pending = PendingEvent {
            event_id: EventId::from_static("maximum-terminal-denial-event"),
            thread_id: turn.thread_id,
            expected_stream_version: super::STATE_THREAD_EVENT_LIMIT
                .saturating_sub(super::STATE_TERMINAL_EVENT_RESERVE),
            expected_stream_recovery_bytes: expected_recovery_bytes,
            recorded_at_ms: denied.recorded_at_ms,
            event: denied.event,
        };
        super::validate_pending_event(&pending)
            .expect("maximum denial must use the only terminal slot and byte reserve");
    }

    #[tokio::test]
    async fn wait_close_enforces_ready_revision_timeout_and_executing_fences() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();

        let (ready_turn, ready_request, ready_generation) =
            approval_wait_fixture(&state, &authority).await;
        let ready_wait_id = AgentLoopWaitId::from_static("wait-close-ready");
        state
            .start_approval_wait_as(
                &ready_turn,
                AgentLoopWaitStartCommand::new(
                    ready_wait_id.clone(),
                    ready_request.clone(),
                    ready_generation,
                    None,
                    Some(30_000),
                ),
                &authority,
            )
            .await
            .expect("start Ready closure wait");
        assert!(
            state
                .close_wait_as(
                    &ready_turn,
                    AgentLoopWaitCloseCommand::new(
                        ready_wait_id.clone(),
                        1,
                        AgentLoopCloseCommandId::from_static("close-status-reason-mismatch"),
                        TurnStatus::Cancelled,
                        TurnStopReason::TimedOut,
                    ),
                    &authority,
                )
                .await
                .expect_err("closure status and reason must correspond exactly")
                .to_string()
                .contains("exactly")
        );
        assert!(
            state
                .close_wait_as(
                    &ready_turn,
                    AgentLoopWaitCloseCommand::new(
                        ready_wait_id.clone(),
                        1,
                        AgentLoopCloseCommandId::from_static("close-timeout-without-expiry"),
                        TurnStatus::TimedOut,
                        TurnStopReason::TimedOut,
                    ),
                    &authority,
                )
                .await
                .expect_err("non-expiring wait cannot time out")
                .to_string()
                .contains("elapsed server expiry")
        );
        let resume_id = AgentLoopResumeCommandId::from_static("resume-before-close");
        state
            .accept_resume_as(
                &ready_turn,
                &ready_wait_id,
                1,
                resume_id,
                &settled_approval(ready_request, None),
                &authority,
            )
            .await
            .expect("make wait Ready");
        assert!(
            state
                .close_wait_as(
                    &ready_turn,
                    AgentLoopWaitCloseCommand::new(
                        ready_wait_id.clone(),
                        1,
                        AgentLoopCloseCommandId::from_static("close-ready-stale"),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &authority,
                )
                .await
                .expect_err("Ready closure requires revision two")
                .to_string()
                .contains("current revision")
        );
        state
            .close_wait_as(
                &ready_turn,
                AgentLoopWaitCloseCommand::new(
                    ready_wait_id.clone(),
                    2,
                    AgentLoopCloseCommandId::from_static("close-ready"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
            .await
            .expect("close Ready execution");

        let (timeout_turn, timeout_request, timeout_generation) =
            approval_wait_fixture(&state, &authority).await;
        let timeout_wait_id = AgentLoopWaitId::from_static("wait-close-timeout");
        state
            .start_approval_wait_as(
                &timeout_turn,
                AgentLoopWaitStartCommand::new(
                    timeout_wait_id.clone(),
                    timeout_request.clone(),
                    timeout_generation,
                    Some(Duration::from_millis(500)),
                    None,
                ),
                &authority,
            )
            .await
            .expect("start expiring wait");
        assert!(
            state
                .close_wait_as(
                    &timeout_turn,
                    AgentLoopWaitCloseCommand::new(
                        timeout_wait_id.clone(),
                        1,
                        AgentLoopCloseCommandId::from_static("close-timeout-early"),
                        TurnStatus::TimedOut,
                        TurnStopReason::TimedOut,
                    ),
                    &authority,
                )
                .await
                .expect_err("server expiry must elapse")
                .to_string()
                .contains("elapsed server expiry")
        );
        tokio::time::sleep(Duration::from_millis(550)).await;
        assert!(
            state
                .accept_resume_as(
                    &timeout_turn,
                    &timeout_wait_id,
                    1,
                    AgentLoopResumeCommandId::from_static("resume-expired-wait"),
                    &settled_approval(timeout_request, None),
                    &authority,
                )
                .await
                .expect_err("expired wait cannot accept resume")
                .to_string()
                .contains("expired")
        );
        state
            .close_wait_as(
                &timeout_turn,
                AgentLoopWaitCloseCommand::new(
                    timeout_wait_id.clone(),
                    1,
                    AgentLoopCloseCommandId::from_static("close-timeout"),
                    TurnStatus::TimedOut,
                    TurnStopReason::TimedOut,
                ),
                &authority,
            )
            .await
            .expect("close expired wait");
        let timed_out = state
            .load_thread(&timeout_turn.thread_id)
            .await
            .expect("load timed-out Thread")
            .expect("timed-out Thread");
        assert_eq!(timed_out.turns[0].status, TurnStatus::TimedOut);

        let (executing_turn, executing_request, executing_generation) =
            approval_wait_fixture(&state, &authority).await;
        let executing_wait_id = AgentLoopWaitId::from_static("wait-close-executing");
        state
            .start_approval_wait_as(
                &executing_turn,
                AgentLoopWaitStartCommand::new(
                    executing_wait_id.clone(),
                    executing_request.clone(),
                    executing_generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start executing closure wait");
        let executing_resume = AgentLoopResumeCommandId::from_static("resume-close-executing");
        state
            .accept_resume_as(
                &executing_turn,
                &executing_wait_id,
                1,
                executing_resume.clone(),
                &settled_approval(executing_request, None),
                &authority,
            )
            .await
            .expect("ready executing closure wait");
        state
            .claim_ready_as(
                &executing_turn,
                AgentLoopReadyClaimCommand::new(
                    executing_wait_id.clone(),
                    2,
                    executing_resume.clone(),
                    AgentLoopClaimId::from_static("claim-before-close"),
                    AgentLoopWorkerId::from_static("worker-before-close"),
                ),
                &authority,
            )
            .await
            .expect("claim before closure attempt");
        assert!(
            state
                .close_wait_as(
                    &executing_turn,
                    AgentLoopWaitCloseCommand::new(
                        executing_wait_id.clone(),
                        3,
                        AgentLoopCloseCommandId::from_static("close-executing"),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &authority,
                )
                .await
                .expect_err("Executing claim cannot be closed as unclaimed")
                .to_string()
                .contains("Executing")
        );
    }

    #[tokio::test]
    async fn accept_resume_and_wait_close_have_exactly_one_cas_winner() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-accept-close-race");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    None,
                    None,
                ),
                &authority,
            )
            .await
            .expect("start accept-close race");
        let settlement = settled_approval(request, None);
        let (accepted, closed) = tokio::join!(
            state.accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-accept-close-race"),
                &settlement,
                &authority,
            ),
            state.close_wait_as(
                &turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    1,
                    AgentLoopCloseCommandId::from_static("close-accept-close-race"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &authority,
            )
        );
        assert_eq!(
            usize::from(accepted.is_ok()) + usize::from(closed.is_ok()),
            1
        );
        let thread = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load race Thread")
            .expect("race Thread");
        if accepted.is_ok() {
            assert_eq!(thread.turns[0].status, TurnStatus::Running);
            assert!(matches!(
                state
                    .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                    .await
                    .expect("load resume winner"),
                Some(AgentLoopExecution::Ready { .. })
            ));
        } else {
            assert_eq!(thread.turns[0].status, TurnStatus::Cancelled);
            assert!(
                state
                    .agent_loop_execution_as(&turn.thread_id, &turn.id, &authority)
                    .await
                    .expect("load close winner")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn ready_claim_has_one_cas_winner_and_unknown_effect_is_not_replayed() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::local_process();
        let (turn, request, generation) = approval_wait_fixture(&state, &authority).await;
        let wait_id = AgentLoopWaitId::from_static("wait-race");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    None,
                    Some(10_000),
                ),
                &authority,
            )
            .await
            .expect("start race wait");
        let command_id = AgentLoopResumeCommandId::from_static("resume-race");
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                command_id.clone(),
                &settled_approval(request.clone(), None),
                &authority,
            )
            .await
            .expect("ready race wait");
        let claim_a = AgentLoopClaimId::from_static("claim-race-a");
        let claim_b = AgentLoopClaimId::from_static("claim-race-b");
        let worker_a = AgentLoopWorkerId::from_static("worker-race-a");
        let worker_b = AgentLoopWorkerId::from_static("worker-race-b");
        let (left, right) = tokio::join!(
            state.claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id.clone(),
                    2,
                    command_id.clone(),
                    claim_a.clone(),
                    worker_a,
                ),
                &authority,
            ),
            state.claim_ready_as(
                &turn,
                AgentLoopReadyClaimCommand::new(
                    wait_id.clone(),
                    2,
                    command_id.clone(),
                    claim_b.clone(),
                    worker_b,
                ),
                &authority,
            )
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        let execution = state
            .agent_loop_execution(&turn.thread_id, &turn.id)
            .await
            .expect("load claim winner")
            .expect("Executing projection");
        let AgentLoopExecution::Executing { claim, .. } = execution else {
            panic!("claim winner must be Executing");
        };
        let winning_event = if claim.claim_id == claim_a {
            left.expect("claim A winner")
        } else {
            right.expect("claim B winner")
        };
        let duplicate = state
            .claim_ready(
                &turn,
                &wait_id,
                2,
                &command_id,
                claim.claim_id.clone(),
                claim.worker_id.clone(),
            )
            .await
            .expect("winning claim retry");
        assert_eq!(winning_event.sequence, duplicate.sequence);

        let new_request = ApprovalRequest {
            id: ApprovalId::generate(),
            ..request
        };
        let error = state
            .start_approval_wait(
                &turn,
                AgentLoopWaitId::from_static("wait-after-unknown-effect"),
                new_request,
                test_completion_generation(
                    "3f6f0a73c7f86f7d7803cf00cda2821b6bb646c377d891a928bc1d67dc575400",
                ),
                None,
                Some(9_000),
            )
            .await
            .expect_err("unknown effect must not replay");
        assert!(
            error
                .to_string()
                .contains("unknown effect cannot be replayed")
        );
    }

    #[tokio::test]
    async fn wait_resume_is_bound_to_original_actor_and_tenant() {
        let owner = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "oidc".to_owned(),
                subject: "owner".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("owner authority");
        let same_tenant_other_actor = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "oidc".to_owned(),
                subject: "other".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("other actor");
        let other_tenant =
            AuthorityContext::new(owner.actor().clone(), Some("tenant-b".to_owned()))
                .expect("other tenant");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let (turn, request, generation) = approval_wait_fixture(&state, &owner).await;
        let wait_id = AgentLoopWaitId::from_static("wait-tenant");
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    None,
                    Some(10_000),
                ),
                &owner,
            )
            .await
            .expect("tenant wait");
        let settlement = settled_approval(request, Some("tenant-a".to_owned()));
        assert!(
            state
                .accept_resume_as(
                    &turn,
                    &wait_id,
                    1,
                    AgentLoopResumeCommandId::from_static("resume-wrong-actor"),
                    &settlement,
                    &same_tenant_other_actor,
                )
                .await
                .expect_err("original actor fence")
                .to_string()
                .contains("requester")
        );
        assert!(
            state
                .agent_loop_execution_as(&turn.thread_id, &turn.id, &other_tenant)
                .await
                .is_err()
        );
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("resume-owner"),
                &settlement,
                &owner,
            )
            .await
            .expect("owner resumes");
        assert!(
            state
                .close_wait_as(
                    &turn,
                    AgentLoopWaitCloseCommand::new(
                        wait_id.clone(),
                        2,
                        AgentLoopCloseCommandId::from_static("close-wrong-actor"),
                        TurnStatus::Cancelled,
                        TurnStopReason::Cancelled,
                    ),
                    &same_tenant_other_actor,
                )
                .await
                .expect_err("wait closure retains original actor fence")
                .to_string()
                .contains("requester")
        );
        state
            .close_wait_as(
                &turn,
                AgentLoopWaitCloseCommand::new(
                    wait_id.clone(),
                    2,
                    AgentLoopCloseCommandId::from_static("close-owner"),
                    TurnStatus::Cancelled,
                    TurnStopReason::Cancelled,
                ),
                &owner,
            )
            .await
            .expect("owner closes Ready wait");
    }

    #[tokio::test]
    async fn archive_five_upgrades_without_fabricating_wait_state() {
        let thread_id = ThreadId::from_static("legacy-archive-thread");
        let turn_id = TurnId::from_static("legacy-archive-turn");
        let events = vec![
            StoredEvent {
                schema_version: 1,
                sequence: 1,
                event_id: EventId::from_static("legacy-create"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 1,
                event: StateEvent::ThreadCreated {
                    created_at_ms: 1,
                    tenant_id: None,
                },
            },
            StoredEvent {
                schema_version: 1,
                sequence: 2,
                event_id: EventId::from_static("legacy-turn"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 2,
                event: StateEvent::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            },
            StoredEvent {
                schema_version: 1,
                sequence: 3,
                event_id: EventId::from_static("legacy-finish"),
                thread_id: thread_id.clone(),
                recorded_at_ms: 3,
                event: StateEvent::TurnFinished {
                    turn_id,
                    status: TurnStatus::Failed,
                },
            },
        ];
        let archive = super::ThreadArchive {
            format_version: 5,
            source_thread_id: thread_id,
            source_stream_version: 3,
            source_last_sequence: 3,
            source_events_sha256: super::state_events_sha256(&events)
                .expect("legacy archive digest"),
            events,
        };
        let encoded = serde_json::to_vec(&archive).expect("legacy archive JSON");
        let upgraded = super::decode_thread_archive(&encoded).expect("upgrade archive five");
        assert_eq!(
            upgraded.format_version,
            super::THREAD_ARCHIVE_FORMAT_VERSION
        );
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let imported = state
            .import_thread(&upgraded, ThreadId::from_static("legacy-archive-import"))
            .await
            .expect("import upgraded archive");
        assert!(
            state
                .agent_loop_execution(&imported.id, &imported.turns[0].id)
                .await
                .expect("legacy execution projection")
                .is_none()
        );
        let wait_event = StateEvent::ClaimReady {
            turn_id: imported.turns[0].id.clone(),
            transition: Item::new(ItemKind::RuntimeError {
                message: "not a claim".to_owned(),
            }),
        };
        assert!(super::validate_state_event_schema(&wait_event, 15).is_err());
    }

    #[tokio::test]
    async fn inbox_tombstone_recorded_and_looked_up_by_wait() {
        let store = Arc::new(SqliteEventStore::open(scratch_path("inbox-tombstone")).await.unwrap());
        let engine = StateEngine::new(store.clone());
        let wait_id = AgentLoopWaitId::from_static("wait-tombstone-1");

        let initial = engine
            .lookup_inbox_tombstone(&wait_id)
            .await
            .expect("tombstone lookup");
        assert!(initial.is_none());

        engine
            .record_inbox_tombstone(&wait_id, InboxTombstoneReason::Denied, 7, 123)
            .await
            .expect("record tombstone");

        let after = engine
            .lookup_inbox_tombstone(&wait_id)
            .await
            .expect("tombstone lookup after record");
        let record = after.expect("tombstone must be present");
        assert_eq!(record.wait_id, wait_id);
        assert_eq!(record.reason, InboxTombstoneReason::Denied);
        assert_eq!(record.source_revision, 7);
        assert_eq!(record.tombstoned_at_ms, 123);
    }

    #[tokio::test]
    async fn inbox_tombstone_isolated_per_wait_id() {
        let store = Arc::new(SqliteEventStore::open(scratch_path("inbox-tombstone-iso")).await.unwrap());
        let engine = StateEngine::new(store.clone());
        let wait_a = AgentLoopWaitId::from_static("wait-a");
        let wait_b = AgentLoopWaitId::from_static("wait-b");

        engine
            .record_inbox_tombstone(&wait_a, InboxTombstoneReason::Cancelled, 1, 100)
            .await
            .expect("record a");

        assert!(engine
            .lookup_inbox_tombstone(&wait_a)
            .await
            .expect("lookup a")
            .is_some());
        assert!(engine
            .lookup_inbox_tombstone(&wait_b)
            .await
            .expect("lookup b")
            .is_none());
    }

    #[tokio::test]
    async fn inbox_tombstone_record_overwrites_with_latest_revision() {
        let store = Arc::new(SqliteEventStore::open(scratch_path("inbox-tombstone-replace")).await.unwrap());
        let engine = StateEngine::new(store.clone());
        let wait = AgentLoopWaitId::from_static("wait-replace");

        engine
            .record_inbox_tombstone(&wait, InboxTombstoneReason::Settled, 1, 100)
            .await
            .expect("first record");
        engine
            .record_inbox_tombstone(&wait, InboxTombstoneReason::TerminalFailure, 5, 200)
            .await
            .expect("second record");

        let record = engine
            .lookup_inbox_tombstone(&wait)
            .await
            .expect("lookup")
            .expect("present");
        assert_eq!(record.reason, InboxTombstoneReason::TerminalFailure);
        assert_eq!(record.source_revision, 5);
        assert_eq!(record.tombstoned_at_ms, 200);
    }

    #[tokio::test]
    async fn memory_backend_reports_no_tombstone_and_supports_flag_false() {
        let engine = StateEngine::new(Arc::new(MemoryEventStore::new()));
        assert!(!engine.supports_inbox_repair_durability());
        assert!(engine
            .lookup_inbox_tombstone(&AgentLoopWaitId::from_static("any"))
            .await
            .expect("memory lookup")
            .is_none());
        engine
            .record_inbox_tombstone(
                &AgentLoopWaitId::from_static("any"),
                InboxTombstoneReason::Denied,
                1,
                1,
            )
            .await
            .expect("memory record is a no-op");
    }

    fn scratch_path(label: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "y-harness-tombstone-test-{label}-{}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }
}
