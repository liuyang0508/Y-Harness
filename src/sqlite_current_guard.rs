//! Caller-held current guard over one exact SQLite Task Graph and Workflow Run.
//!
//! SQLite has no row-level writer lock. This API therefore holds short-lived
//! `BEGIN IMMEDIATE` transactions in the fixed Task-store then Workflow-store
//! order. The caller receives only validated snapshots and domain-separated
//! digests; table names, columns, connections, and transactions stay private.
//! The guard is valid only inside the controlled-local immutable namespace
//! lifecycle documented by both SQLite Coordinators. Same-file checks detect
//! aliases and observable replacement; they are not a durable store UUID and
//! do not make hot replacement safe.

use std::{sync::mpsc, time::Duration};

use rusqlite::{Connection, ErrorCode, OpenFlags, Transaction, TransactionBehavior};
use tokio::{sync::oneshot, task, task::JoinHandle};

use crate::{
    AuthorityContext, HarnessError, SqliteTaskCoordinator, SqliteWorkflowCoordinator, TaskGraphId,
    TaskGraphSnapshot, WorkflowRunId, WorkflowRunSnapshot, sqlite::SqliteStoreIdentity,
};

/// Short busy window used before releasing the first store and retrying.
const CURRENT_GUARD_BUSY_WINDOW: Duration = Duration::from_millis(20);
/// Bounded pause between ordered lock attempts.
const CURRENT_GUARD_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Domain-separated digest of one exact validated SQLite current row.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SqliteCurrentDigest([u8; 32]);

impl SqliteCurrentDigest {
    /// Returns the exact 32-byte SHA-256 digest.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for SqliteCurrentDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteCurrentDigest(<redacted>)")
    }
}

/// Strict Task Graph current observed inside the held pair of write guards.
pub struct GuardedTaskGraphCurrent {
    snapshot: TaskGraphSnapshot,
    digest: SqliteCurrentDigest,
}

impl GuardedTaskGraphCurrent {
    /// Returns the strictly hydrated current snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &TaskGraphSnapshot {
        &self.snapshot
    }

    /// Returns the current optimistic revision bound into the digest.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.snapshot.revision()
    }

    /// Returns the domain-separated digest of identity, revision, and raw row bytes.
    #[must_use]
    pub fn digest(&self) -> SqliteCurrentDigest {
        self.digest
    }
}

impl std::fmt::Debug for GuardedTaskGraphCurrent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GuardedTaskGraphCurrent(<redacted>)")
    }
}

/// Strict Workflow Run current observed inside the held pair of write guards.
pub struct GuardedWorkflowRunCurrent {
    snapshot: WorkflowRunSnapshot,
    digest: SqliteCurrentDigest,
}

impl GuardedWorkflowRunCurrent {
    /// Returns the strictly hydrated current snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &WorkflowRunSnapshot {
        &self.snapshot
    }

    /// Returns the current optimistic revision bound into the digest.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.snapshot.revision()
    }

    /// Returns the domain-separated digest of identity, revision, and raw row bytes.
    #[must_use]
    pub fn digest(&self) -> SqliteCurrentDigest {
        self.digest
    }
}

impl std::fmt::Debug for GuardedWorkflowRunCurrent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("GuardedWorkflowRunCurrent(<redacted>)")
    }
}

/// Caller-held pair of exact Task Graph and Workflow Run SQLite current guards.
///
/// The owning blocking task retains both database write reservations until
/// [`Self::release`] or `Drop`. Callers must keep this scope short and must not
/// perform network, model, Tool, or other unbounded work while it is held.
/// The database, `-wal`, and `-shm` paths must not be renamed, unlinked, or
/// replaced from before Coordinator open until all Coordinators and guards drop.
pub struct SqliteTaskWorkflowCurrentGuard {
    task_graph: GuardedTaskGraphCurrent,
    workflow_run: GuardedWorkflowRunCurrent,
    release_sender: Option<mpsc::Sender<()>>,
    worker: Option<JoinHandle<Result<(), HarnessError>>>,
}

impl SqliteTaskWorkflowCurrentGuard {
    /// Acquires exact current records under one authority in fixed Task→Workflow order.
    ///
    /// Only controlled local filesystems whose writer locking is provided by
    /// SQLite are supported. Memory stores, same-file aliases, and observable
    /// path replacement fail closed. Uncontrolled same-permission processes and
    /// hot replacement remain outside the supported authority model.
    pub async fn acquire_as(
        tasks: &SqliteTaskCoordinator,
        graph_id: &TaskGraphId,
        workflows: &SqliteWorkflowCoordinator,
        run_id: &WorkflowRunId,
        authority: &AuthorityContext,
    ) -> Result<Self, HarnessError> {
        authority.validate_current("SQLite current guard authority")?;
        let task_store = tasks.current_guard_store().ok_or_else(unsupported_store)?;
        let workflow_store = workflows
            .current_guard_store()
            .ok_or_else(unsupported_store)?;
        if same_store(&task_store, &workflow_store) {
            return Err(HarnessError::InvalidConfiguration(
                "Task and Workflow current guards require distinct SQLite files".to_owned(),
            ));
        }

        let graph_id = graph_id.clone();
        let run_id = run_id.clone();
        let tenant_id = authority.tenant_id().map(str::to_owned);
        let (startup_sender, startup_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let worker = task::spawn_blocking(move || {
            hold_current_pair(
                &task_store,
                &workflow_store,
                &graph_id,
                &run_id,
                tenant_id.as_deref(),
                startup_sender,
                release_receiver,
            )
        });

        match startup_receiver.await {
            Ok(Ok((task_graph, workflow_run))) => Ok(Self {
                task_graph,
                workflow_run,
                release_sender: Some(release_sender),
                worker: Some(worker),
            }),
            Ok(Err(error)) => {
                drop(release_sender);
                let _ = worker.await;
                Err(error)
            }
            Err(_) => {
                drop(release_sender);
                match worker.await {
                    Ok(Err(error)) => Err(error),
                    Ok(Ok(())) => Err(HarnessError::Orchestration(
                        "SQLite current guard closed before acquisition".to_owned(),
                    )),
                    Err(error) => Err(join_error(error)),
                }
            }
        }
    }

    /// Returns the exact Task Graph current while both store guards remain held.
    #[must_use]
    pub fn task_graph(&self) -> &GuardedTaskGraphCurrent {
        &self.task_graph
    }

    /// Returns the exact Workflow Run current while both store guards remain held.
    #[must_use]
    pub fn workflow_run(&self) -> &GuardedWorkflowRunCurrent {
        &self.workflow_run
    }

    /// Explicitly releases both transactions and confirms the owning task stopped.
    pub async fn release(mut self) -> Result<(), HarnessError> {
        if let Some(sender) = self.release_sender.take() {
            let _ = sender.send(());
        }
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker.await.map_err(join_error)?
    }
}

impl std::fmt::Debug for SqliteTaskWorkflowCurrentGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SqliteTaskWorkflowCurrentGuard(<redacted>)")
    }
}

impl Drop for SqliteTaskWorkflowCurrentGuard {
    fn drop(&mut self) {
        self.release_sender.take();
    }
}

/// Holds both exact currents until the caller releases or abandons the guard.
fn hold_current_pair(
    task_store: &SqliteStoreIdentity,
    workflow_store: &SqliteStoreIdentity,
    graph_id: &TaskGraphId,
    run_id: &WorkflowRunId,
    tenant_id: Option<&str>,
    startup: oneshot::Sender<
        Result<(GuardedTaskGraphCurrent, GuardedWorkflowRunCurrent), HarnessError>,
    >,
    release: mpsc::Receiver<()>,
) -> Result<(), HarnessError> {
    let mut task_connection = open_guard_connection(task_store)?;
    let mut workflow_connection = open_guard_connection(workflow_store)?;
    loop {
        if startup.is_closed() {
            return Ok(());
        }
        let task_transaction =
            match task_connection.transaction_with_behavior(TransactionBehavior::Immediate) {
                Ok(transaction) => transaction,
                Err(error) if is_lock_contention(&error) => {
                    std::thread::sleep(CURRENT_GUARD_RETRY_DELAY);
                    continue;
                }
                Err(error) => return Err(sql_error("Task current guard", error)),
            };
        if startup.is_closed() {
            task_transaction
                .rollback()
                .map_err(|error| sql_error("Task current guard rollback", error))?;
            return Ok(());
        }
        let workflow_transaction =
            match workflow_connection.transaction_with_behavior(TransactionBehavior::Immediate) {
                Ok(transaction) => transaction,
                Err(error) if is_lock_contention(&error) => {
                    task_transaction
                        .rollback()
                        .map_err(|error| sql_error("Task current guard retry rollback", error))?;
                    std::thread::sleep(CURRENT_GUARD_RETRY_DELAY);
                    continue;
                }
                Err(error) => {
                    let rollback = task_transaction.rollback();
                    if let Err(rollback_error) = rollback {
                        return Err(sql_error(
                            "Task current guard failure rollback",
                            rollback_error,
                        ));
                    }
                    return Err(sql_error("Workflow current guard", error));
                }
            };

        let material = load_guarded_currents(
            &task_transaction,
            &workflow_transaction,
            graph_id,
            run_id,
            tenant_id,
        );
        let material = match material {
            Ok(material) => material,
            Err(error) => {
                let _ = startup.send(Err(error));
                return rollback_pair(workflow_transaction, task_transaction);
            }
        };
        if startup.send(Ok(material)).is_err() {
            return rollback_pair(workflow_transaction, task_transaction);
        }
        let _ = release.recv();
        return rollback_pair(workflow_transaction, task_transaction);
    }
}

/// Opens one existing store without schema creation authority.
fn open_guard_connection(store: &SqliteStoreIdentity) -> Result<Connection, HarnessError> {
    if !store.is_current() {
        return Err(unsupported_store());
    }
    let connection = Connection::open_with_flags(store.path(), OpenFlags::SQLITE_OPEN_READ_WRITE)
        .map_err(|error| sql_error("SQLite current guard open", error))?;
    if !store.is_current() {
        return Err(unsupported_store());
    }
    connection
        .busy_timeout(CURRENT_GUARD_BUSY_WINDOW)
        .map_err(|error| sql_error("SQLite current guard timeout", error))?;
    connection
        .execute_batch("PRAGMA foreign_keys = ON;")
        .map_err(|error| sql_error("SQLite current guard setup", error))?;
    Ok(connection)
}

/// Strictly loads both records after both write reservations are held.
fn load_guarded_currents(
    task_transaction: &Transaction<'_>,
    workflow_transaction: &Transaction<'_>,
    graph_id: &TaskGraphId,
    run_id: &WorkflowRunId,
    tenant_id: Option<&str>,
) -> Result<(GuardedTaskGraphCurrent, GuardedWorkflowRunCurrent), HarnessError> {
    let task =
        SqliteTaskCoordinator::load_current_for_guard(task_transaction, graph_id, tenant_id)?;
    let workflow =
        SqliteWorkflowCoordinator::load_current_for_guard(workflow_transaction, run_id, tenant_id)?;
    let (task_snapshot, task_digest) = task.ok_or_else(missing_guarded_pair)?;
    let (workflow_snapshot, workflow_digest) = workflow.ok_or_else(missing_guarded_pair)?;
    if workflow_snapshot.run().task_graph_id() != graph_id {
        return Err(missing_guarded_pair());
    }
    Ok((
        GuardedTaskGraphCurrent {
            snapshot: task_snapshot,
            digest: SqliteCurrentDigest(task_digest),
        },
        GuardedWorkflowRunCurrent {
            snapshot: workflow_snapshot,
            digest: SqliteCurrentDigest(workflow_digest),
        },
    ))
}

/// Rolls back both stores in reverse order and never skips the Task release.
fn rollback_pair(workflow: Transaction<'_>, task: Transaction<'_>) -> Result<(), HarnessError> {
    let workflow_result = workflow.rollback();
    let task_result = task.rollback();
    workflow_result.map_err(|error| sql_error("Workflow current guard rollback", error))?;
    task_result.map_err(|error| sql_error("Task current guard rollback", error))
}

/// Recognizes only transient SQLite writer contention as retryable.
fn is_lock_contention(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

/// Same-file handles reject path, symlink, and hard-link aliases within the lifecycle.
fn same_store(task_store: &SqliteStoreIdentity, workflow_store: &SqliteStoreIdentity) -> bool {
    task_store == workflow_store
}

/// Current guards require durable file-backed SQLite stores.
fn unsupported_store() -> HarnessError {
    HarnessError::InvalidConfiguration(
        "SQLite current guard requires a canonical file-backed store".to_owned(),
    )
}

/// Missing and foreign records share one low-information rejection.
fn missing_guarded_pair() -> HarnessError {
    HarnessError::Orchestration(
        "paired SQLite current guard record is absent or outside authority".to_owned(),
    )
}

/// Keeps SQLite details inside the Harness persistence boundary.
fn sql_error(operation: &'static str, error: rusqlite::Error) -> HarnessError {
    HarnessError::Orchestration(format!("{operation} failed: {error}"))
}

/// Maps an owning blocking task failure without exposing snapshots or paths.
fn join_error(error: task::JoinError) -> HarnessError {
    HarnessError::Orchestration(format!("SQLite current guard task failed: {error}"))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::same_store;
    use crate::sqlite::SqliteStoreIdentity;

    #[test]
    fn hard_link_alias_is_rejected_before_transactions_when_supported() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let original = std::env::temp_dir().join(format!(
            "y-harness-current-guard-same-file-{}-{stamp}.db",
            std::process::id()
        ));
        let alias = original.with_extension("hardlink.db");
        std::fs::write(&original, b"sqlite-store-identity").expect("write identity fixture");
        if std::fs::hard_link(&original, &alias).is_ok() {
            let original_identity =
                SqliteStoreIdentity::capture(&original).expect("original store identity");
            let alias_identity =
                SqliteStoreIdentity::capture(&alias).expect("alias store identity");
            assert!(same_store(&original_identity, &alias_identity));
        }
        let _ = std::fs::remove_file(&alias);
        let _ = std::fs::remove_file(&original);
    }
}
