//! Append-only runtime state, deterministic projection, and recovery.

mod migration;

pub use migration::{StateMigrationReport, StateMigrationStatus};

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io::{self, Write},
    path::Path,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    sync::{Mutex, Notify, Semaphore},
    task,
};

use crate::{
    Checkpoint, CheckpointId, EventId, HarnessError, HarnessFuture, Item, ItemId, ItemKind,
    NewStreamEvent, PendingEvent, StateEvent, StoredEvent, Thread, ThreadId, ThreadImportOrigin,
    ThreadLineage, Turn, TurnId, TurnStatus,
    json::{BoundedJsonError, bounded_serialized_size, to_bounded_json_vec, validate_value_shape},
    kernel::validate_capability_name,
    sqlite::{bounded_optional_text, bounded_text},
};

/// Current append-only State event schema.
pub const STATE_EVENT_SCHEMA_VERSION: u32 = 11;
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
pub const STATE_TERMINAL_RECOVERY_BYTE_RESERVE: u64 = 4_096;
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
pub const STATE_SNAPSHOT_SCHEMA_VERSION: u32 = 11;
/// Current portable Thread archive format.
pub const THREAD_ARCHIVE_FORMAT_VERSION: u32 = 1;
/// Maximum accepted encoded Thread archive.
pub const MAX_THREAD_ARCHIVE_BYTES: usize = 75_497_472;
const MAX_STEERING_CONTENT_BYTES: usize = 1_048_576;
const MAX_INVOCATION_CONTEXT_BLOCKS: usize = 64;
const MAX_INVOCATION_CONTEXT_REFERENCE_BYTES: usize = 4_096;
const MAX_INVOCATION_CONTEXT_BLOCK_BYTES: usize = 1_048_576;
const MAX_INVOCATION_CONTEXT_TOTAL_BYTES: usize = 1_061_184;

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
    let archive = serde_json::from_slice::<ThreadArchive>(encoded)
        .map_err(|error| HarnessError::State(format!("invalid Thread archive JSON: {error}")))?;
    validate_thread_archive(&archive)?;
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

/// Append-only persistence port for ordered State events.
pub trait EventStore: Send + Sync {
    /// Atomically compares stream version and appends, or returns an idempotent event.
    ///
    /// Implementations must compare both expected stream fields and update the
    /// version and recovery-byte charge in the same transaction as the event.
    /// Returned writes must use [`STATE_EVENT_SCHEMA_VERSION`]; prior supported
    /// coordinates are read compatibility, not write authority.
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent>;

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
        _before_sequence: Option<u64>,
        _limit: usize,
    ) -> HarnessFuture<'_, Vec<ThreadSummary>> {
        Box::pin(async {
            Err(HarnessError::State(
                "Event Store does not support Thread listing".to_owned(),
            ))
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
}

#[derive(Default)]
/// In-memory Event Store with production-equivalent idempotency semantics.
pub struct MemoryEventStore {
    data: Mutex<MemoryStoreData>,
}

#[derive(Default)]
struct MemoryStoreData {
    events: Vec<StoredEvent>,
    stream_versions: BTreeMap<ThreadId, u64>,
    stream_recovery_bytes: BTreeMap<ThreadId, u64>,
    stream_names: BTreeMap<ThreadId, String>,
    stream_lineages: BTreeMap<ThreadId, ThreadLineage>,
    snapshots: BTreeMap<ThreadId, StateSnapshot>,
}

impl MemoryEventStore {
    #[must_use]
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl EventStore for MemoryEventStore {
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
        Box::pin(async move {
            let encoded = validate_pending_event(&pending)?;
            let mut data = self.data.lock().await;
            if let Some(existing) = data
                .events
                .iter()
                .find(|event| event.event_id == pending.event_id)
            {
                return matching_existing(existing, &pending);
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
            let thread_exists = actual_stream_version > 0;
            validate_thread_existence(thread_exists, &pending)?;
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
            let stored = StoredEvent {
                schema_version: STATE_EVENT_SCHEMA_VERSION,
                sequence: u64::try_from(data.events.len() + 1).unwrap_or(u64::MAX),
                event_id: pending.event_id,
                thread_id: pending.thread_id.clone(),
                recorded_at_ms: pending.recorded_at_ms,
                event: pending.event,
            };
            data.stream_versions
                .insert(pending.thread_id.clone(), next_stream_version);
            data.stream_recovery_bytes
                .insert(pending.thread_id.clone(), next_recovery_bytes);
            if let Some(name) = name_change {
                match name {
                    Some(name) => {
                        data.stream_names.insert(pending.thread_id.clone(), name);
                    }
                    None => {
                        data.stream_names.remove(&pending.thread_id);
                    }
                }
            }
            if let Some(lineage) = lineage_change {
                data.stream_lineages
                    .insert(pending.thread_id.clone(), lineage);
            }
            data.events.push(stored.clone());
            Ok(stored)
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
            if data
                .events
                .iter()
                .any(|stored| requested_ids.contains(&stored.event_id))
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
            data.stream_versions
                .insert(thread_id.clone(), stream_version);
            data.stream_recovery_bytes
                .insert(thread_id.clone(), recovery_bytes);
            if let Some(name) = final_stream_name(&stored) {
                data.stream_names.insert(thread_id.clone(), name);
            }
            if let Some(lineage) = final_stream_lineage(&stored) {
                data.stream_lineages.insert(thread_id, lineage);
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
}

/// SQLite-backed append-only Event Store.
pub struct SqliteEventStore {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteEventStore {
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
                        name      TEXT
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

impl EventStore for SqliteEventStore {
    fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
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
                    return matching_existing(&stored, &pending);
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
                        "INSERT INTO streams (thread_id, version) VALUES (?1, ?2)
                         ON CONFLICT(thread_id) DO UPDATE SET version = excluded.version",
                        params![
                            pending.thread_id.as_str(),
                            i64::try_from(next_stream_version).map_err(|_| {
                                HarnessError::State(
                                    "stream version exceeds SQLite INTEGER".to_owned(),
                                )
                            })?
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
                transaction
                    .commit()
                    .map_err(|error| HarnessError::State(error.to_string()))?;

                Ok(StoredEvent {
                    schema_version: STATE_EVENT_SCHEMA_VERSION,
                    sequence,
                    event_id: pending.event_id,
                    thread_id: pending.thread_id,
                    recorded_at_ms: pending.recorded_at_ms,
                    event: pending.event,
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
                        "INSERT INTO streams (thread_id, version) VALUES (?1, ?2)",
                        params![thread_id.as_str(), stream_version_sql],
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
                             ORDER BY events.sequence DESC
                             LIMIT ?2
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
                    .query_map(params![before_sequence, limit], |row| {
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
        let thread = Thread::new();
        self.commit(
            thread.id.clone(),
            0,
            0,
            StateEvent::ThreadCreated {
                created_at_ms: thread.created_at_ms,
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

        let checked = self.checked_events(parent_thread_id).await?;
        let parent = project_events(&checked.events)?.ok_or_else(|| {
            HarnessError::State(format!("thread {parent_thread_id} does not exist"))
        })?;
        let existing_child = self.load_thread(&child_thread_id).await?;
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
                    | StateEvent::TurnFinished { .. } => Some(NewStreamEvent {
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
                let existing = self.load_thread(&child_thread_id).await?.ok_or_else(|| {
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
        let loaded = self.load_projection(thread_id).await?;
        if let Some(thread) = &loaded.thread {
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
        validate_thread_name(name.as_deref())?;
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
        let loaded = self.load_projection(thread_id).await?;
        let Some(thread) = loaded.thread else {
            self.heads.lock().await.remove(thread_id);
            return Ok(None);
        };
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
            .thread_summaries_page(before_sequence, fetch_limit)
            .await?;
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
        Ok(self.checked_events(thread_id).await?.events)
    }

    /// Exports one complete terminal Thread journal with an integrity digest.
    pub async fn export_thread(&self, thread_id: &ThreadId) -> Result<ThreadArchive, HarnessError> {
        let checked = self.checked_events(thread_id).await?;
        let thread = project_events(&checked.events)?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
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
        validate_state_id("target thread", target_thread_id.as_str())?;
        if !self.store.supports_atomic_stream_creation() {
            return Err(HarnessError::State(
                "Event Store does not support atomic Thread import".to_owned(),
            ));
        }
        let source = validate_thread_archive(archive)?;
        let origin = ThreadImportOrigin {
            source_thread_id: archive.source_thread_id.clone(),
            source_stream_version: archive.source_stream_version,
            source_last_sequence: archive.source_last_sequence,
            source_events_sha256: archive.source_events_sha256.clone(),
            source_lineage: source.lineage.clone(),
        };
        if let Some(existing) = self.load_thread(&target_thread_id).await? {
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
                    | StateEvent::TurnFinished { .. } => Some(NewStreamEvent {
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
                let existing = self.load_thread(&target_thread_id).await?.ok_or_else(|| {
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
        validate_state_id("thread", thread_id.as_str())?;
        validate_event_page_limit(limit)?;
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
        if matches!(
            item.kind,
            ItemKind::SteeringQueued { .. } | ItemKind::SteeringApplied { .. }
        ) {
            let thread = self.load_thread(&turn.thread_id).await?.ok_or_else(|| {
                HarnessError::State(format!("thread {} does not exist", turn.thread_id))
            })?;
            validate_steering_append(&thread, &turn.id, &item)?;
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
    pub(crate) async fn append_tool_calls(
        &self,
        turn: &Turn,
        calls: Vec<Item>,
    ) -> Result<StoredEvent, HarnessError> {
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

    /// Settles a running Turn with a terminal status.
    pub async fn finish_turn(
        &self,
        turn: &Turn,
        status: TurnStatus,
    ) -> Result<StoredEvent, HarnessError> {
        if status == TurnStatus::Running {
            return Err(HarnessError::State(
                "cannot finish a turn with running status".to_owned(),
            ));
        }
        if status == TurnStatus::Completed {
            let thread = self.load_thread(&turn.thread_id).await?.ok_or_else(|| {
                HarnessError::State(format!("thread {} does not exist", turn.thread_id))
            })?;
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
            if has_pending_steering(projected)? {
                return Err(HarnessError::State(format!(
                    "cannot complete turn {} with unapplied steering",
                    turn.id
                )));
            }
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

    /// Marks one exact unfinished Turn interrupted and returns the recovered projection.
    ///
    /// Callers must hold exclusive Thread ownership and know that the previous
    /// worker is no longer live. Recovery is a takeover operation, not a normal
    /// preflight for starting a Turn. The expected Turn identity is rechecked
    /// at the optimistic commit boundary, so a stale takeover cannot interrupt
    /// a newer running Turn.
    pub async fn recover_thread(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
    ) -> Result<Option<Thread>, HarnessError> {
        let Some(thread) = self.load_thread(thread_id).await? else {
            return Ok(None);
        };
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
            return Ok(Some(thread));
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
        self.finish_turn(expected, TurnStatus::Interrupted).await?;
        self.load_thread(thread_id).await
    }

    /// Persists a checkpoint targeting the current Thread sequence.
    pub async fn create_checkpoint(
        &self,
        thread_id: &ThreadId,
        turn_id: Option<TurnId>,
        label: Option<String>,
    ) -> Result<Checkpoint, HarnessError> {
        validate_checkpoint_label(label.as_deref())?;
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
        let snapshot = self.create_snapshot(thread_id).await?;
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
            thread_id: thread_id.clone(),
            expected_stream_version,
            expected_stream_recovery_bytes,
            recorded_at_ms: crate::kernel::now_ms(),
            event,
        };
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

fn validate_thread_archive(archive: &ThreadArchive) -> Result<Thread, HarnessError> {
    if archive.format_version != THREAD_ARCHIVE_FORMAT_VERSION {
        return Err(HarnessError::State(format!(
            "unsupported Thread archive format {}",
            archive.format_version
        )));
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
            StateEvent::ThreadCreated { created_at_ms } => {
                if thread.is_some() {
                    return Err(HarnessError::State(
                        "thread has multiple creation events".to_owned(),
                    ));
                }
                *thread = Some(Thread {
                    id: stored.thread_id.clone(),
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
                    items: Vec::new(),
                });
            }
            StateEvent::ItemAppended { turn_id, item } => {
                append_projected_items(thread, turn_id, std::slice::from_ref(item), &mut item_ids)?;
            }
            StateEvent::ToolCallsAppended { turn_id, calls } => {
                append_projected_items(thread, turn_id, calls, &mut item_ids)?;
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
                turn.status = status.clone();
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
        validate_steering_projection(thread)?;
        validate_tool_call_batch_projection(thread)?;
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

        let mut item_index = 0;
        while item_index < turn.items.len() {
            let item = &turn.items[item_index];
            let batch_size = match &item.kind {
                ItemKind::ToolCall {
                    batch: Some(batch), ..
                } if batch.index == 0 => batch.size,
                ItemKind::ToolCall { batch: Some(_), .. } => {
                    return Err(HarnessError::State(
                        "State snapshot starts inside a Tool-call batch".to_owned(),
                    ));
                }
                _ => 1,
            };
            let batch_end = item_index
                .checked_add(batch_size)
                .filter(|end| *end <= turn.items.len())
                .ok_or_else(|| {
                    HarnessError::State(
                        "State snapshot contains a truncated Tool-call batch".to_owned(),
                    )
                })?;
            let items = &turn.items[item_index..batch_end];
            if batch_size > 1 {
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
            let event = if batch_size > 1 {
                StateEvent::ToolCallsAppended {
                    turn_id: turn.id.clone(),
                    calls: items.to_vec(),
                }
            } else {
                StateEvent::ItemAppended {
                    turn_id: turn.id.clone(),
                    item: item.clone(),
                }
            };
            add_recovery_bytes(
                &mut recovery_bytes,
                encode_state_event(&event)?.recovery_bytes,
            )?;
            item_index = batch_end;
        }

        if turn.status == TurnStatus::Running {
            running_turns = running_turns
                .checked_add(1)
                .ok_or_else(|| HarnessError::State("running Turn count overflow".to_owned()))?;
        } else {
            represented_events = represented_events
                .checked_add(1)
                .ok_or_else(|| HarnessError::State("snapshot event count overflow".to_owned()))?;
            add_recovery_bytes(
                &mut recovery_bytes,
                encode_state_event(&StateEvent::TurnFinished {
                    turn_id: turn.id.clone(),
                    status: turn.status.clone(),
                })?
                .recovery_bytes,
            )?;
        }
    }
    if running_turns > 1 {
        return Err(HarnessError::State(
            "State snapshot contains overlapping running Turns".to_owned(),
        ));
    }
    validate_steering_projection(thread)?;
    validate_tool_call_batch_projection(thread)?;

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
        && !matches!(&pending.event, StateEvent::TurnFinished { .. })
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
    let limit = if matches!(&pending.event, StateEvent::TurnFinished { .. }) {
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

fn validate_state_event(event: &StateEvent) -> Result<(), HarnessError> {
    match event {
        StateEvent::ThreadCreated { .. } => {}
        StateEvent::ThreadNamed { name } => validate_thread_name(name.as_deref())?,
        StateEvent::ThreadForked { lineage } => validate_thread_lineage(lineage)?,
        StateEvent::ThreadImported { origin } => validate_thread_import_origin(origin)?,
        StateEvent::TurnStarted { turn_id } | StateEvent::TurnFinished { turn_id, .. } => {
            validate_state_id("turn", turn_id.as_str())?;
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
        _ => {}
    }
    Ok(())
}

fn validate_state_event_schema(
    event: &StateEvent,
    schema_version: u32,
) -> Result<(), HarnessError> {
    match event {
        StateEvent::ThreadNamed { .. } if schema_version < 8 => Err(HarnessError::State(format!(
            "schema-{schema_version} cannot contain a Thread name"
        ))),
        StateEvent::ThreadForked { .. } if schema_version < 9 => Err(HarnessError::State(format!(
            "schema-{schema_version} cannot contain Thread fork lineage"
        ))),
        StateEvent::ThreadImported { .. } if schema_version < 10 => Err(HarnessError::State(
            format!("schema-{schema_version} cannot contain Thread import provenance"),
        )),
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
            item: Item { kind, .. },
            ..
        } => validate_state_item_schema(kind, schema_version),
        _ => Ok(()),
    }
}

fn validate_state_item_schema(kind: &ItemKind, schema_version: u32) -> Result<(), HarnessError> {
    match kind {
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
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

    use serde_json::json;

    use super::{
        EventStore, MemoryEventStore, SnapshotMaintenanceConfig, SnapshotMaintenanceFailure,
        SqliteEventStore, StateCapacityLevel, StateEngine, StateSnapshot,
    };
    use crate::{
        ActorIdentity, CapabilityOrigin, EventId, HarnessError, HarnessFuture,
        InvocationContextEvidence, Item, ItemKind, ModelContinuation, NewStreamEvent, PendingEvent,
        StateEvent, SteeringId, StoredEvent, ThreadId, ThreadImportOrigin, ThreadLineage,
        ToolCallBatch, ToolCallBatchId, TurnId, TurnStatus, kernel::now_ms,
    };

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
            .finish_turn(&first_turn, TurnStatus::Completed)
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
            .append_tool_calls(&turn, calls.clone())
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
            .append_tool_calls(&turn, malformed)
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

        let error = state
            .finish_turn(&turn, TurnStatus::Completed)
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
        state
            .finish_turn(&turn, TurnStatus::Completed)
            .await
            .expect("complete after steering application");

        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load thread")
            .expect("thread exists");
        assert_eq!(projected.turns[0].items.len(), 2);
        assert_eq!(projected.turns[0].status, TurnStatus::Completed);
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
                Item::new(ItemKind::AssistantMessage {
                    model_id: None,
                    model_origin: None,
                    content: "after snapshot".to_owned(),
                }),
            )
            .await
            .expect("append tail");
        state
            .finish_turn(&turn, TurnStatus::Completed)
            .await
            .expect("finish tail");

        let loaded = StateEngine::new(store.clone())
            .load_thread(&thread.id)
            .await
            .expect("load from snapshot")
            .expect("thread");
        assert_eq!(loaded.turns[0].items.len(), 2);
        assert_eq!(loaded.turns[0].status, TurnStatus::Completed);
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
                status: TurnStatus::Completed,
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

        state
            .finish_turn(&turn, TurnStatus::Completed)
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
        assert_eq!(snapshot.stream_version(), 3);
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
            .finish_turn(&first_turn, TurnStatus::Completed)
            .await
            .expect("settle first turn");
        tokio::time::timeout(Duration::from_secs(1), store.entered.notified())
            .await
            .expect("first snapshot worker entered");
        state
            .finish_turn(&second_turn, TurnStatus::Completed)
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
                .finish_turn(&turn, TurnStatus::Completed)
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
            assert_eq!(loaded.turns[0].status, TurnStatus::Completed);
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
        state
            .finish_turn(&turn, TurnStatus::Completed)
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
        assert_eq!(loaded.turns[0].items.len(), 1);
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
                    status: TurnStatus::Completed,
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
            .finish_turn(&first, TurnStatus::Completed)
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
                Item::new(ItemKind::AssistantMessage {
                    model_id: None,
                    model_origin: None,
                    content: "later history".to_owned(),
                }),
            )
            .await
            .expect("second item");
        state
            .finish_turn(&second, TurnStatus::Completed)
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
            .finish_turn(&child_turn, TurnStatus::Completed)
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
            .finish_turn(&source_turn, TurnStatus::Completed)
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
            .finish_turn(&continued, TurnStatus::Completed)
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
            .finish_turn(&turn, TurnStatus::Completed)
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
            }),
        };
        let encoded = serde_json::to_string(&event).expect("serialize");
        let decoded: StateEvent = serde_json::from_str(&encoded).expect("deserialize");
        assert_eq!(event, decoded);
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
}
