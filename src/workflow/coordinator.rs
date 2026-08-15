//! Atomic persistence ports for durable Workflow Run aggregates.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use tokio::{sync::Mutex, task};

use super::{
    MAX_WORKFLOW_IDEMPOTENCY_BYTES, MAX_WORKFLOW_JSON_BYTES, WorkflowApplyOutcome, WorkflowCommand,
    WorkflowCreateRequest, WorkflowRun, WorkflowStatus, WorkflowWait, validate_identity,
};
use crate::{
    AuthorityContext, HarnessError, HarnessFuture, WorkflowRunId, WorkflowWaitId,
    sqlite::{SqliteStoreIdentity, bounded_text, open_read_only},
};

/// Current durable Workflow Run schema.
pub const WORKFLOW_RUN_SCHEMA_VERSION: u32 = 1;
const MAX_WORKFLOW_SCAN_PAGE: usize = 256;

/// Immutable revisioned Workflow Run projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowRunSnapshot {
    id: WorkflowRunId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    revision: u64,
    run: WorkflowRun,
}

impl WorkflowRunSnapshot {
    /// Returns the stable Run identity.
    #[must_use]
    pub fn id(&self) -> &WorkflowRunId {
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

    /// Returns the current validated Workflow projection.
    #[must_use]
    pub fn run(&self) -> &WorkflowRun {
        &self.run
    }
}

/// Result of one idempotent Workflow command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowCommandResult {
    /// Current durable snapshot after application or duplicate recognition.
    pub snapshot: WorkflowRunSnapshot,
    /// Whether the command changed the durable revision.
    pub outcome: WorkflowApplyOutcome,
}

/// One due Workflow wait discovered from authoritative Run state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowDueWait {
    /// Stable Run identity.
    pub run_id: WorkflowRunId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Revision observed with the wait.
    pub revision: u64,
    /// Exact current wait fence.
    pub wait_id: WorkflowWaitId,
    /// Inclusive server-clock wake boundary.
    pub due_at_ms: u64,
}

/// One bounded identity-ordered scan page over Workflow Runs.
///
/// `scanned` counts every visited Run, including records that are not due.
/// The cursor therefore makes progress through sparse due workloads without
/// requiring an authoritative secondary scheduler database.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowDueScanPage {
    /// Due waits among the visited Runs.
    pub due: Vec<WorkflowDueWait>,
    /// Last visited Run identity.
    pub next_after_run_id: Option<WorkflowRunId>,
    /// Whether another Run remains in this tenant's current scan.
    pub has_more: bool,
    /// Number of authoritative Runs visited.
    pub scanned: usize,
}

/// Atomic persistence and command boundary for durable Workflow Runs.
pub trait WorkflowCoordinator: Send + Sync {
    /// Creates one unscoped Workflow Run.
    fn create<'a>(
        &'a self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, WorkflowRunSnapshot> {
        Box::pin(async move {
            self.create_as(
                run_id,
                request,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Creates or recognizes one exact Run under trusted tenant authority.
    fn create_as<'a>(
        &'a self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowRunSnapshot>;

    /// Loads one unscoped Workflow Run.
    fn load<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
    ) -> HarnessFuture<'a, Option<WorkflowRunSnapshot>> {
        Box::pin(async move {
            self.load_as(run_id, &AuthorityContext::local_process())
                .await
        })
    }

    /// Loads one Run only inside the exact trusted tenant boundary.
    fn load_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<WorkflowRunSnapshot>>;

    /// Scans one bounded unscoped Run page for waits due at `at_ms`.
    fn scan_due<'a>(
        &'a self,
        at_ms: u64,
        after_run_id: Option<&'a WorkflowRunId>,
        scan_limit: usize,
    ) -> HarnessFuture<'a, WorkflowDueScanPage> {
        Box::pin(async move {
            self.scan_due_as(
                at_ms,
                after_run_id,
                scan_limit,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Scans one bounded identity-ordered Run page inside the exact tenant.
    ///
    /// The default keeps existing custom coordinators source-compatible while
    /// failing closed until they implement temporal discovery.
    fn scan_due_as<'a>(
        &'a self,
        _at_ms: u64,
        _after_run_id: Option<&'a WorkflowRunId>,
        _scan_limit: usize,
        _authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowDueScanPage> {
        Box::pin(async {
            Err(HarnessError::Workflow(
                "Workflow Coordinator does not support temporal discovery".to_owned(),
            ))
        })
    }

    /// Applies one idempotent command to an unscoped Run.
    fn apply<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, WorkflowCommandResult> {
        Box::pin(async move {
            self.apply_as(
                run_id,
                expected_revision,
                command,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Applies one command with exact revision and tenant fencing.
    ///
    /// Replaying the exact committed command is idempotent even when
    /// `expected_revision` is stale. A new command always requires the current
    /// revision.
    fn apply_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowCommandResult>;
}

/// In-memory Workflow Coordinator with SQLite-equivalent semantics.
#[derive(Default)]
pub struct MemoryWorkflowCoordinator {
    runs: Mutex<BTreeMap<(String, WorkflowRunId), WorkflowRunSnapshot>>,
}

impl MemoryWorkflowCoordinator {
    /// Creates an empty Coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl WorkflowCoordinator for MemoryWorkflowCoordinator {
    fn create_as<'a>(
        &'a self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowRunSnapshot> {
        Box::pin(async move {
            validate_run_access(&run_id, authority)?;
            let mut runs = self.runs.lock().await;
            let key = storage_key(&run_id, authority.tenant_id());
            if let Some(existing) = runs.get(&key) {
                if existing.run.create_matches(&request)? {
                    return Ok(existing.clone());
                }
                return Err(HarnessError::Workflow(format!(
                    "Workflow Run {run_id} already exists with different creation content"
                )));
            }
            let run = WorkflowRun::new(request, applied_at_ms, authority)?;
            let snapshot = WorkflowRunSnapshot {
                id: run_id,
                tenant_id: authority.tenant_id().map(str::to_owned),
                revision: 1,
                run,
            };
            runs.insert(key, snapshot.clone());
            Ok(snapshot)
        })
    }

    fn load_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<WorkflowRunSnapshot>> {
        Box::pin(async move {
            validate_run_access(run_id, authority)?;
            Ok(self
                .runs
                .lock()
                .await
                .get(&storage_key(run_id, authority.tenant_id()))
                .cloned())
        })
    }

    fn scan_due_as<'a>(
        &'a self,
        at_ms: u64,
        after_run_id: Option<&'a WorkflowRunId>,
        scan_limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowDueScanPage> {
        Box::pin(async move {
            validate_due_scan(at_ms, after_run_id, scan_limit, authority)?;
            let runs = self.runs.lock().await;
            let tenant = tenant_storage_key(authority.tenant_id()).to_owned();
            let range_start = (
                tenant.clone(),
                after_run_id
                    .cloned()
                    .unwrap_or_else(|| WorkflowRunId::from_string(String::new())),
            );
            let mut visited = runs
                .range(range_start..)
                .take_while(|((stored_tenant, _), _)| stored_tenant == &tenant)
                .filter(|((_, run_id), _)| {
                    after_run_id.is_none_or(|after| run_id.as_str() > after.as_str())
                })
                .take(scan_limit.saturating_add(1))
                .map(|(_, snapshot)| snapshot.clone())
                .collect::<Vec<_>>();
            Ok(page_from_scan(&mut visited, scan_limit, at_ms))
        })
    }

    fn apply_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowCommandResult> {
        Box::pin(async move {
            validate_run_access(run_id, authority)?;
            validate_expected_revision(expected_revision)?;
            let mut runs = self.runs.lock().await;
            let key = storage_key(run_id, authority.tenant_id());
            let current = runs.get(&key).ok_or_else(|| missing_run(run_id))?.clone();
            if current.run.recognizes_command(&command)? {
                return Ok(WorkflowCommandResult {
                    snapshot: current,
                    outcome: WorkflowApplyOutcome::Duplicate,
                });
            }
            if current.revision != expected_revision {
                return Err(HarnessError::WorkflowConflict {
                    run_id: run_id.clone(),
                    expected: expected_revision,
                    actual: current.revision,
                });
            }
            let mut run = current.run.clone();
            let outcome = run.apply(command, applied_at_ms, authority)?;
            debug_assert_eq!(outcome, WorkflowApplyOutcome::Applied);
            let revision = current
                .revision
                .checked_add(1)
                .ok_or_else(|| HarnessError::Workflow("Workflow revision overflow".to_owned()))?;
            let saved = WorkflowRunSnapshot {
                id: current.id,
                tenant_id: current.tenant_id,
                revision,
                run,
            };
            runs.insert(key, saved.clone());
            Ok(WorkflowCommandResult {
                snapshot: saved,
                outcome,
            })
        })
    }
}

/// SQLite Workflow Coordinator using one immediate transaction per mutation.
pub struct SqliteWorkflowCoordinator {
    connection: Arc<StdMutex<Connection>>,
    current_guard_store: Option<Arc<SqliteStoreIdentity>>,
}

impl SqliteWorkflowCoordinator {
    /// Validates one existing Workflow database without creating or mutating it.
    ///
    /// Missing paths are errors. An existing database with neither Workflow
    /// table remains eligible for first-open bootstrap.
    pub async fn validate_existing(path: impl AsRef<Path>) -> Result<(), HarnessError> {
        let path = path.as_ref().to_owned();
        task::spawn_blocking(move || {
            let connection =
                open_read_only(&path).map_err(|error| HarnessError::Workflow(error.to_string()))?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(sql_error)?;
            validate_existing_store(&connection)
        })
        .await
        .map_err(|error| {
            HarnessError::Workflow(format!("Workflow validation task failed: {error}"))
        })?
    }

    /// Opens or creates a schema-1 Workflow store with durable WAL settings.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_owned();
        let connection = task::spawn_blocking(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| HarnessError::Workflow(error.to_string()))?;
            }
            let mut connection = Connection::open(&path)
                .map_err(|error| HarnessError::Workflow(error.to_string()))?;
            configure_connection(&connection)?;
            initialize_or_validate(&mut connection)?;
            let current_guard_store = SqliteStoreIdentity::capture(&path).map(Arc::new);
            Ok::<_, HarnessError>((connection, current_guard_store))
        })
        .await
        .map_err(|error| HarnessError::Workflow(format!("Workflow open task failed: {error}")))??;
        let (connection, current_guard_store) = connection;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
            current_guard_store,
        })
    }

    /// Returns the canonical on-disk store identity for the paired current guard.
    pub(crate) fn current_guard_store(&self) -> Option<Arc<SqliteStoreIdentity>> {
        self.current_guard_store.clone()
    }

    /// Strictly hydrates one exact Workflow Run current row for the paired guard.
    pub(crate) fn load_current_for_guard(
        connection: &Connection,
        run_id: &WorkflowRunId,
        tenant_id: Option<&str>,
    ) -> Result<Option<(WorkflowRunSnapshot, [u8; 32])>, HarnessError> {
        validate_identity("Workflow Run", run_id.as_str())?;
        load_stored_snapshot(connection, run_id, tenant_id).map(|stored| {
            stored.map(|stored| {
                let digest = crate::sqlite::current_row_digest(
                    b"y-harness:sqlite-workflow-run-current:v1",
                    tenant_id,
                    run_id.as_str(),
                    WORKFLOW_RUN_SCHEMA_VERSION,
                    stored.snapshot.revision,
                    stored.encoded.as_bytes(),
                );
                (stored.snapshot, digest)
            })
        })
    }
}

impl WorkflowCoordinator for SqliteWorkflowCoordinator {
    fn create_as<'a>(
        &'a self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowRunSnapshot> {
        let connection = self.connection.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_run_access(&run_id, &authority)?;
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Workflow("Workflow lock poisoned".to_owned()))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                if let Some(existing) = load_snapshot(&transaction, &run_id, authority.tenant_id())?
                {
                    if existing.run.create_matches(&request)? {
                        transaction.commit().map_err(sql_error)?;
                        return Ok(existing);
                    }
                    return Err(HarnessError::Workflow(format!(
                        "Workflow Run {run_id} already exists with different creation content"
                    )));
                }
                let run = WorkflowRun::new(request, applied_at_ms, &authority)?;
                let encoded = encode_run(&run)?;
                transaction
                    .execute(
                        "INSERT INTO workflow_runs
                         (tenant_id, run_id, schema_version, revision, run_json)
                         VALUES (?1, ?2, ?3, 1, ?4)",
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            run_id.as_str(),
                            WORKFLOW_RUN_SCHEMA_VERSION,
                            encoded
                        ],
                    )
                    .map_err(sql_error)?;
                transaction.commit().map_err(sql_error)?;
                Ok(WorkflowRunSnapshot {
                    id: run_id,
                    tenant_id: authority.tenant_id().map(str::to_owned),
                    revision: 1,
                    run,
                })
            })
            .await
            .map_err(|error| {
                HarnessError::Workflow(format!("Workflow create task failed: {error}"))
            })?
        })
    }

    fn load_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<WorkflowRunSnapshot>> {
        let connection = self.connection.clone();
        let run_id = run_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_run_access(&run_id, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Workflow("Workflow lock poisoned".to_owned()))?;
                load_snapshot(&connection, &run_id, authority.tenant_id())
            })
            .await
            .map_err(|error| {
                HarnessError::Workflow(format!("Workflow load task failed: {error}"))
            })?
        })
    }

    fn scan_due_as<'a>(
        &'a self,
        at_ms: u64,
        after_run_id: Option<&'a WorkflowRunId>,
        scan_limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowDueScanPage> {
        let connection = self.connection.clone();
        let after_run_id = after_run_id.cloned();
        let authority = authority.clone();
        Box::pin(async move {
            validate_due_scan(at_ms, after_run_id.as_ref(), scan_limit, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Workflow("Workflow lock poisoned".to_owned()))?;
                let fetch = scan_limit.checked_add(1).ok_or_else(|| {
                    HarnessError::Workflow("Workflow scan limit overflow".to_owned())
                })?;
                let mut statement = connection
                    .prepare(
                        "SELECT length(CAST(run_id AS BLOB)), run_id,
                                schema_version, revision,
                                length(CAST(run_json AS BLOB)), run_json
                         FROM workflow_runs
                         WHERE tenant_id = ?1 AND run_id > ?2
                         ORDER BY run_id ASC
                         LIMIT ?3",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            after_run_id.as_ref().map_or("", |id| id.as_str()),
                            i64::try_from(fetch).map_err(|_| {
                                HarnessError::Workflow(
                                    "Workflow scan limit exceeds SQLite".to_owned(),
                                )
                            })?
                        ],
                        |row| {
                            Ok((
                                bounded_text(
                                    row,
                                    0,
                                    1,
                                    MAX_WORKFLOW_IDEMPOTENCY_BYTES,
                                    "Workflow Run identity",
                                )?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, i64>(3)?,
                                bounded_text(row, 4, 5, MAX_WORKFLOW_JSON_BYTES, "Workflow Run")?,
                            ))
                        },
                    )
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                let mut visited = Vec::with_capacity(rows.len());
                for (run_id, schema, revision, encoded) in rows {
                    visited.push(decode_snapshot(
                        WorkflowRunId::from_string(run_id),
                        authority.tenant_id(),
                        schema,
                        revision,
                        encoded,
                    )?);
                }
                Ok(page_from_scan(&mut visited, scan_limit, at_ms))
            })
            .await
            .map_err(|error| {
                HarnessError::Workflow(format!("Workflow scan task failed: {error}"))
            })?
        })
    }

    fn apply_as<'a>(
        &'a self,
        run_id: &'a WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, WorkflowCommandResult> {
        let connection = self.connection.clone();
        let run_id = run_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_run_access(&run_id, &authority)?;
            validate_expected_revision(expected_revision)?;
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Workflow("Workflow lock poisoned".to_owned()))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                let current = load_snapshot(&transaction, &run_id, authority.tenant_id())?
                    .ok_or_else(|| missing_run(&run_id))?;
                if current.run.recognizes_command(&command)? {
                    transaction.commit().map_err(sql_error)?;
                    return Ok(WorkflowCommandResult {
                        snapshot: current,
                        outcome: WorkflowApplyOutcome::Duplicate,
                    });
                }
                if current.revision != expected_revision {
                    return Err(HarnessError::WorkflowConflict {
                        run_id,
                        expected: expected_revision,
                        actual: current.revision,
                    });
                }
                let mut run = current.run.clone();
                let outcome = run.apply(command, applied_at_ms, &authority)?;
                debug_assert_eq!(outcome, WorkflowApplyOutcome::Applied);
                let revision = current.revision.checked_add(1).ok_or_else(|| {
                    HarnessError::Workflow("Workflow revision overflow".to_owned())
                })?;
                let encoded = encode_run(&run)?;
                let changed = transaction
                    .execute(
                        "UPDATE workflow_runs
                         SET revision = ?1, run_json = ?2
                         WHERE tenant_id = ?3 AND run_id = ?4 AND revision = ?5",
                        params![
                            sql_revision(revision)?,
                            encoded,
                            tenant_storage_key(authority.tenant_id()),
                            run_id.as_str(),
                            sql_revision(current.revision)?
                        ],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(HarnessError::Workflow(
                        "Workflow atomic update changed an unexpected row count".to_owned(),
                    ));
                }
                transaction.commit().map_err(sql_error)?;
                Ok(WorkflowCommandResult {
                    snapshot: WorkflowRunSnapshot {
                        id: run_id,
                        tenant_id: current.tenant_id,
                        revision,
                        run,
                    },
                    outcome,
                })
            })
            .await
            .map_err(|error| {
                HarnessError::Workflow(format!("Workflow command task failed: {error}"))
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
        return Err(HarnessError::Workflow(format!(
            "SQLite refused WAL mode and selected {mode}"
        )));
    }
    connection
        .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(sql_error)
}

fn initialize_or_validate(connection: &mut Connection) -> Result<(), HarnessError> {
    let has_meta = table_exists(connection, "workflow_store_meta")?;
    let has_runs = table_exists(connection, "workflow_runs")?;
    match (has_meta, has_runs) {
        (false, false) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            transaction
                .execute_batch(
                    "
                    CREATE TABLE workflow_store_meta (
                        singleton      INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO workflow_store_meta (singleton, schema_version) VALUES (1, 1);
                    CREATE TABLE workflow_runs (
                        tenant_id      TEXT NOT NULL,
                        run_id         TEXT NOT NULL,
                        schema_version INTEGER NOT NULL,
                        revision       INTEGER NOT NULL CHECK(revision > 0),
                        run_json       TEXT NOT NULL,
                        PRIMARY KEY (tenant_id, run_id)
                    );
                    ",
                )
                .map_err(sql_error)?;
            transaction.commit().map_err(sql_error)
        }
        (true, true) => validate_store(connection),
        _ => Err(HarnessError::Workflow(
            "SQLite Workflow store is partial".to_owned(),
        )),
    }
}

fn validate_existing_store(connection: &Connection) -> Result<(), HarnessError> {
    let has_meta = table_exists(connection, "workflow_store_meta")?;
    let has_runs = table_exists(connection, "workflow_runs")?;
    match (has_meta, has_runs) {
        (false, false) => Ok(()),
        (true, true) => validate_store(connection),
        _ => Err(HarnessError::Workflow(
            "SQLite Workflow store is partial".to_owned(),
        )),
    }
}

fn validate_store(connection: &Connection) -> Result<(), HarnessError> {
    let versions = connection
        .prepare("SELECT singleton, schema_version FROM workflow_store_meta")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(sql_error)?;
    if versions != vec![(1, i64::from(WORKFLOW_RUN_SCHEMA_VERSION))] {
        return Err(HarnessError::Workflow(
            "SQLite Workflow store schema is unknown or malformed".to_owned(),
        ));
    }
    let invalid_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workflow_runs
             WHERE schema_version != ?1 OR revision <= 0
                OR length(tenant_id) > 128 OR length(run_id) = 0 OR length(run_id) > 256
                OR length(CAST(run_json AS BLOB)) > ?2",
            params![
                WORKFLOW_RUN_SCHEMA_VERSION,
                i64::try_from(MAX_WORKFLOW_JSON_BYTES)
                    .map_err(|_| HarnessError::Workflow("Workflow size overflow".to_owned()))?
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if invalid_rows != 0 {
        return Err(HarnessError::Workflow(
            "SQLite Workflow store contains invalid row metadata".to_owned(),
        ));
    }
    Ok(())
}

/// Exact persisted Workflow Run row retained long enough to bind its raw bytes.
struct StoredWorkflowRunSnapshot {
    /// Strictly decoded aggregate snapshot.
    snapshot: WorkflowRunSnapshot,
    /// Exact stored JSON bytes used by the current digest.
    encoded: String,
}

/// Loads and validates one exact Workflow Run row without exposing its schema.
fn load_stored_snapshot(
    connection: &Connection,
    run_id: &WorkflowRunId,
    tenant_id: Option<&str>,
) -> Result<Option<StoredWorkflowRunSnapshot>, HarnessError> {
    let row = connection
        .query_row(
            "SELECT schema_version, revision,
                    length(CAST(run_json AS BLOB)), run_json
             FROM workflow_runs WHERE tenant_id = ?1 AND run_id = ?2",
            params![tenant_storage_key(tenant_id), run_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    bounded_text(row, 2, 3, MAX_WORKFLOW_JSON_BYTES, "Workflow Run")?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    let Some((schema_version, revision, encoded)) = row else {
        return Ok(None);
    };
    let snapshot = decode_snapshot(
        run_id.clone(),
        tenant_id,
        schema_version,
        revision,
        encoded.clone(),
    )?;
    Ok(Some(StoredWorkflowRunSnapshot { snapshot, encoded }))
}

/// Loads one strict Workflow snapshot for existing Coordinator operations.
fn load_snapshot(
    connection: &Connection,
    run_id: &WorkflowRunId,
    tenant_id: Option<&str>,
) -> Result<Option<WorkflowRunSnapshot>, HarnessError> {
    load_stored_snapshot(connection, run_id, tenant_id)
        .map(|stored| stored.map(|stored| stored.snapshot))
}

fn decode_snapshot(
    run_id: WorkflowRunId,
    tenant_id: Option<&str>,
    schema_version: i64,
    revision: i64,
    encoded: String,
) -> Result<WorkflowRunSnapshot, HarnessError> {
    if schema_version != i64::from(WORKFLOW_RUN_SCHEMA_VERSION) {
        return Err(HarnessError::Workflow(format!(
            "Workflow Run {run_id} uses unsupported schema {schema_version}"
        )));
    }
    let revision = u64::try_from(revision)
        .map_err(|_| HarnessError::Workflow("Workflow revision is invalid".to_owned()))?;
    validate_expected_revision(revision)?;
    let run: WorkflowRun = serde_json::from_str(&encoded)
        .map_err(|error| HarnessError::Workflow(format!("decode Workflow Run: {error}")))?;
    run.validate()?;
    Ok(WorkflowRunSnapshot {
        id: run_id,
        tenant_id: tenant_id.map(str::to_owned),
        revision,
        run,
    })
}

fn page_from_scan(
    visited: &mut Vec<WorkflowRunSnapshot>,
    scan_limit: usize,
    at_ms: u64,
) -> WorkflowDueScanPage {
    let has_more = visited.len() > scan_limit;
    visited.truncate(scan_limit);
    let next_after_run_id = visited.last().map(|snapshot| snapshot.id.clone());
    let mut due = Vec::new();
    for snapshot in visited.iter() {
        let Some((wait_id, due_at_ms)) = due_wait(snapshot.run.status()) else {
            continue;
        };
        if due_at_ms <= at_ms {
            due.push(WorkflowDueWait {
                run_id: snapshot.id.clone(),
                tenant_id: snapshot.tenant_id.clone(),
                revision: snapshot.revision,
                wait_id: wait_id.clone(),
                due_at_ms,
            });
        }
    }
    WorkflowDueScanPage {
        due,
        next_after_run_id,
        has_more,
        scanned: visited.len(),
    }
}

fn due_wait(status: &WorkflowStatus) -> Option<(&WorkflowWaitId, u64)> {
    let WorkflowStatus::Waiting { wait } = status else {
        return None;
    };
    match wait {
        WorkflowWait::Signal {
            id,
            expires_at_ms: Some(due_at_ms),
            ..
        }
        | WorkflowWait::Timer { id, due_at_ms }
        | WorkflowWait::Retry { id, due_at_ms, .. } => Some((id, *due_at_ms)),
        WorkflowWait::Signal {
            expires_at_ms: None,
            ..
        } => None,
    }
}

fn encode_run(run: &WorkflowRun) -> Result<String, HarnessError> {
    run.validate()?;
    let encoded = serde_json::to_string(run)
        .map_err(|_| HarnessError::Workflow("cannot encode Workflow Run".to_owned()))?;
    if encoded.len() > MAX_WORKFLOW_JSON_BYTES {
        return Err(HarnessError::Workflow(format!(
            "Workflow Run exceeds {MAX_WORKFLOW_JSON_BYTES} encoded bytes"
        )));
    }
    Ok(encoded)
}

fn validate_run_access(
    run_id: &WorkflowRunId,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority.validate_current("Workflow Coordinator authority")?;
    validate_identity("Workflow Run", run_id.as_str())
}

fn validate_due_scan(
    at_ms: u64,
    after_run_id: Option<&WorkflowRunId>,
    scan_limit: usize,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority.validate_current("Workflow temporal scan authority")?;
    if at_ms == 0 {
        return Err(HarnessError::Workflow(
            "Workflow temporal scan time must be positive".to_owned(),
        ));
    }
    if let Some(run_id) = after_run_id {
        validate_identity("Workflow scan cursor", run_id.as_str())?;
    }
    if !(1..=MAX_WORKFLOW_SCAN_PAGE).contains(&scan_limit) {
        return Err(HarnessError::Workflow(format!(
            "Workflow scan limit must be 1-{MAX_WORKFLOW_SCAN_PAGE}"
        )));
    }
    Ok(())
}

fn validate_expected_revision(revision: u64) -> Result<(), HarnessError> {
    if revision == 0 {
        Err(HarnessError::Workflow(
            "Workflow revision must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn storage_key(run_id: &WorkflowRunId, tenant_id: Option<&str>) -> (String, WorkflowRunId) {
    (tenant_storage_key(tenant_id).to_owned(), run_id.clone())
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

fn missing_run(run_id: &WorkflowRunId) -> HarnessError {
    HarnessError::Workflow(format!("Workflow Run {run_id} does not exist"))
}

fn sql_revision(revision: u64) -> Result<i64, HarnessError> {
    i64::try_from(revision)
        .map_err(|_| HarnessError::Workflow("Workflow revision exceeds SQLite".to_owned()))
}

fn sql_error(error: rusqlite::Error) -> HarnessError {
    HarnessError::Workflow(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use semver::Version;

    use super::*;
    use crate::{
        ActorIdentity, TaskGraphId, WorkflowCommandId, WorkflowCommandKind, WorkflowWaitId,
    };

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn create() -> WorkflowCreateRequest {
        WorkflowCreateRequest {
            command_id: WorkflowCommandId::from_static("create"),
            definition: crate::WorkflowDefinition {
                name: "test.workflow".to_owned(),
                version: Version::new(1, 0, 0),
                content_sha256: digest('a'),
            },
            task_graph_id: TaskGraphId::from_static("graph"),
        }
    }

    fn wait_command() -> WorkflowCommand {
        WorkflowCommand {
            id: WorkflowCommandId::from_static("wait"),
            kind: WorkflowCommandKind::WaitUntil {
                wait_id: WorkflowWaitId::from_static("timer"),
                due_at_ms: 100,
            },
        }
    }

    #[test]
    fn due_wait_covers_every_time_owned_wait_variant() {
        let signal = WorkflowStatus::Waiting {
            wait: WorkflowWait::Signal {
                id: WorkflowWaitId::from_static("signal"),
                name: "event.ready".to_owned(),
                source: "connector".to_owned(),
                expires_at_ms: Some(101),
            },
        };
        let signal_without_timeout = WorkflowStatus::Waiting {
            wait: WorkflowWait::Signal {
                id: WorkflowWaitId::from_static("signal-open"),
                name: "event.open".to_owned(),
                source: "connector".to_owned(),
                expires_at_ms: None,
            },
        };
        let timer = WorkflowStatus::Waiting {
            wait: WorkflowWait::Timer {
                id: WorkflowWaitId::from_static("timer"),
                due_at_ms: 102,
            },
        };
        let retry = WorkflowStatus::Waiting {
            wait: WorkflowWait::Retry {
                id: WorkflowWaitId::from_static("retry"),
                activity: "connector.fetch".to_owned(),
                attempt: 2,
                due_at_ms: 103,
                idempotency_key: "effect-1".to_owned(),
            },
        };

        assert_eq!(
            due_wait(&signal).map(|(id, due)| (id.as_str(), due)),
            Some(("signal", 101))
        );
        assert_eq!(due_wait(&signal_without_timeout), None);
        assert_eq!(
            due_wait(&timer).map(|(id, due)| (id.as_str(), due)),
            Some(("timer", 102))
        );
        assert_eq!(
            due_wait(&retry).map(|(id, due)| (id.as_str(), due)),
            Some(("retry", 103))
        );
        assert_eq!(due_wait(&WorkflowStatus::Running), None);
    }

    fn tenant(id: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: format!("actor-{id}"),
            },
            Some(id.to_owned()),
        )
        .expect("authority")
    }

    fn temp_path(label: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "y-harness-workflow-{label}-{}-{stamp}.db",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn memory_coordinator_is_idempotent_revisioned_and_tenant_fenced() {
        let coordinator = MemoryWorkflowCoordinator::new();
        let run_id = WorkflowRunId::from_static("run");
        let alpha = tenant("alpha");
        let beta = tenant("beta");
        let created = coordinator
            .create_as(run_id.clone(), create(), 10, &alpha)
            .await
            .expect("create");
        assert_eq!(created.revision(), 1);
        let duplicate_create = coordinator
            .create_as(run_id.clone(), create(), 11, &alpha)
            .await
            .expect("duplicate create");
        assert_eq!(duplicate_create, created);
        assert!(
            coordinator
                .load_as(&run_id, &beta)
                .await
                .expect("cross tenant load")
                .is_none()
        );
        let applied = coordinator
            .apply_as(&run_id, 1, wait_command(), 20, &alpha)
            .await
            .expect("apply");
        assert_eq!(applied.snapshot.revision(), 2);
        assert_eq!(applied.outcome, WorkflowApplyOutcome::Applied);
        let replay = coordinator
            .apply_as(&run_id, 1, wait_command(), 21, &alpha)
            .await
            .expect("stale duplicate");
        assert_eq!(replay.snapshot.revision(), 2);
        assert_eq!(replay.outcome, WorkflowApplyOutcome::Duplicate);
    }

    #[tokio::test]
    async fn memory_coordinator_rejects_new_stale_command_without_mutation() {
        let coordinator = MemoryWorkflowCoordinator::new();
        let run_id = WorkflowRunId::from_static("run");
        coordinator
            .create(run_id.clone(), create(), 10)
            .await
            .expect("create");
        coordinator
            .apply(&run_id, 1, wait_command(), 20)
            .await
            .expect("wait");
        let error = coordinator
            .apply(
                &run_id,
                1,
                WorkflowCommand {
                    id: WorkflowCommandId::from_static("cancel"),
                    kind: WorkflowCommandKind::Cancel {
                        reason: "stop".to_owned(),
                    },
                },
                30,
            )
            .await
            .expect_err("conflict");
        assert!(matches!(
            error,
            HarnessError::WorkflowConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        let loaded = coordinator.load(&run_id).await.expect("load").expect("run");
        assert!(matches!(
            loaded.run().status(),
            crate::WorkflowStatus::Waiting { .. }
        ));
    }

    #[tokio::test]
    async fn memory_due_scan_advances_across_sparse_identity_pages() {
        let coordinator = MemoryWorkflowCoordinator::new();
        let authority = tenant("alpha");
        for id in ["a-running", "b-due", "c-future"] {
            coordinator
                .create_as(
                    WorkflowRunId::from_string(id.to_owned()),
                    create(),
                    10,
                    &authority,
                )
                .await
                .expect("create");
        }
        for (id, due_at_ms) in [("b-due", 100), ("c-future", 200)] {
            coordinator
                .apply_as(
                    &WorkflowRunId::from_string(id.to_owned()),
                    1,
                    WorkflowCommand {
                        id: WorkflowCommandId::from_string(format!("wait-{id}")),
                        kind: WorkflowCommandKind::WaitUntil {
                            wait_id: WorkflowWaitId::from_string(format!("wait-{id}")),
                            due_at_ms,
                        },
                    },
                    20,
                    &authority,
                )
                .await
                .expect("wait");
        }

        let first = coordinator
            .scan_due_as(100, None, 1, &authority)
            .await
            .expect("first");
        assert_eq!(first.scanned, 1);
        assert!(first.due.is_empty());
        assert!(first.has_more);
        assert_eq!(
            first.next_after_run_id.as_ref().map(WorkflowRunId::as_str),
            Some("a-running")
        );
        let second = coordinator
            .scan_due_as(100, first.next_after_run_id.as_ref(), 1, &authority)
            .await
            .expect("second");
        assert_eq!(second.due.len(), 1);
        assert_eq!(second.due[0].run_id.as_str(), "b-due");
        assert_eq!(second.due[0].revision, 2);
        assert_eq!(second.due[0].due_at_ms, 100);
        assert!(second.has_more);
        let third = coordinator
            .scan_due_as(100, second.next_after_run_id.as_ref(), 1, &authority)
            .await
            .expect("third");
        assert!(third.due.is_empty());
        assert!(!third.has_more);
    }

    #[tokio::test]
    async fn sqlite_coordinator_reopens_exact_state_and_duplicate_commands() {
        let path = temp_path("reopen");
        let run_id = WorkflowRunId::from_static("run");
        {
            let coordinator = SqliteWorkflowCoordinator::open(&path).await.expect("open");
            coordinator
                .create(run_id.clone(), create(), 10)
                .await
                .expect("create");
            coordinator
                .apply(&run_id, 1, wait_command(), 20)
                .await
                .expect("wait");
        }
        {
            let coordinator = SqliteWorkflowCoordinator::open(&path)
                .await
                .expect("reopen");
            let loaded = coordinator.load(&run_id).await.expect("load").expect("run");
            assert_eq!(loaded.revision(), 2);
            assert_eq!(loaded.run().transition_count(), 2);
            let duplicate = coordinator
                .apply(&run_id, 1, wait_command(), 30)
                .await
                .expect("duplicate");
            assert_eq!(duplicate.outcome, WorkflowApplyOutcome::Duplicate);
            assert_eq!(duplicate.snapshot.revision(), 2);
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_due_scan_reopens_with_memory_equivalent_boundaries() {
        let path = temp_path("due-scan");
        let authority = tenant("alpha");
        {
            let coordinator = SqliteWorkflowCoordinator::open(&path).await.expect("open");
            for id in ["a-due", "b-future"] {
                let run_id = WorkflowRunId::from_string(id.to_owned());
                coordinator
                    .create_as(run_id.clone(), create(), 10, &authority)
                    .await
                    .expect("create");
                coordinator
                    .apply_as(
                        &run_id,
                        1,
                        WorkflowCommand {
                            id: WorkflowCommandId::from_string(format!("wait-{id}")),
                            kind: WorkflowCommandKind::WaitUntil {
                                wait_id: WorkflowWaitId::from_string(format!("wait-{id}")),
                                due_at_ms: if id == "a-due" { 100 } else { 200 },
                            },
                        },
                        20,
                        &authority,
                    )
                    .await
                    .expect("wait");
            }
        }
        let coordinator = SqliteWorkflowCoordinator::open(&path)
            .await
            .expect("reopen");
        let page = coordinator
            .scan_due_as(100, None, 2, &authority)
            .await
            .expect("scan");
        assert_eq!(page.scanned, 2);
        assert_eq!(page.due.len(), 1);
        assert_eq!(page.due[0].run_id.as_str(), "a-due");
        assert!(!page.has_more);
        {
            let connection = Connection::open(&path).expect("tamper fixture");
            connection
                .execute(
                    "UPDATE workflow_runs SET run_id = ?1 WHERE run_id = 'b-future'",
                    params!["x".repeat(MAX_WORKFLOW_IDEMPOTENCY_BYTES + 1)],
                )
                .expect("tamper identity");
        }
        let error = coordinator
            .scan_due_as(100, None, 2, &authority)
            .await
            .expect_err("oversized identity");
        assert!(
            error
                .to_string()
                .contains("Workflow Run identity exceeds 256 bytes")
        );
        drop(coordinator);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    #[tokio::test]
    async fn sqlite_load_rejects_tampered_transition_evidence() {
        let path = temp_path("tamper");
        let run_id = WorkflowRunId::from_static("run");
        {
            let coordinator = SqliteWorkflowCoordinator::open(&path).await.expect("open");
            coordinator
                .create(run_id.clone(), create(), 10)
                .await
                .expect("create");
        }
        {
            let connection = Connection::open(&path).expect("fixture");
            let encoded: String = connection
                .query_row(
                    "SELECT run_json FROM workflow_runs WHERE run_id = ?1",
                    params![run_id.as_str()],
                    |row| row.get(0),
                )
                .expect("stored run");
            let mut value: serde_json::Value =
                serde_json::from_str(&encoded).expect("stored run JSON");
            value["transitions"][0]["command_sha256"] = serde_json::Value::String(digest('b'));
            connection
                .execute(
                    "UPDATE workflow_runs SET run_json = ?1 WHERE run_id = ?2",
                    params![
                        serde_json::to_string(&value).expect("tampered JSON"),
                        run_id.as_str()
                    ],
                )
                .expect("tamper fixture");
        }
        let coordinator = SqliteWorkflowCoordinator::open(&path)
            .await
            .expect("reopen");
        let error = coordinator
            .load(&run_id)
            .await
            .expect_err("tampered run must fail closed");
        assert!(error.to_string().contains("digest differs"));
        drop(coordinator);
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
                    "CREATE TABLE workflow_store_meta (
                        singleton INTEGER PRIMARY KEY,
                        schema_version INTEGER NOT NULL
                    );",
                )
                .expect("partial store");
        }
        let result = SqliteWorkflowCoordinator::open(&path).await;
        assert!(result.is_err());
        let error = result.err().expect("partial store error");
        assert!(error.to_string().contains("partial"));
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_cross_tenant_same_identity_is_partitioned() {
        let path = temp_path("tenant");
        let coordinator = Arc::new(SqliteWorkflowCoordinator::open(&path).await.expect("open"));
        let run_id = WorkflowRunId::from_static("run");
        let alpha = tenant("alpha");
        let beta = tenant("beta");
        coordinator
            .create_as(run_id.clone(), create(), 10, &alpha)
            .await
            .expect("alpha");
        coordinator
            .create_as(run_id.clone(), create(), 11, &beta)
            .await
            .expect("beta");
        assert_eq!(
            coordinator
                .load_as(&run_id, &alpha)
                .await
                .expect("alpha load")
                .expect("alpha run")
                .tenant_id(),
            Some("alpha")
        );
        assert_eq!(
            coordinator
                .load_as(&run_id, &beta)
                .await
                .expect("beta load")
                .expect("beta run")
                .tenant_id(),
            Some("beta")
        );
        drop(coordinator);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
