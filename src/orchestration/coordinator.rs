//! Atomic persistence ports for complete Task Graph aggregates.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use tokio::{sync::Mutex, task};

use super::{MAX_TASK_GRAPH_JSON_BYTES, TaskGraph};
use crate::{HarnessError, HarnessFuture, TaskGraphId, sqlite::bounded_text};

/// Current durable Task Coordinator graph schema.
pub const TASK_GRAPH_SCHEMA_VERSION: u32 = 1;

/// Revisioned, durable Task Graph aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TaskGraphSnapshot {
    /// Stable graph identity.
    id: TaskGraphId,
    /// Positive optimistic-concurrency revision.
    revision: u64,
    /// Complete validated graph projection.
    graph: TaskGraph,
}

impl TaskGraphSnapshot {
    /// Returns the stable graph identity.
    #[must_use]
    pub fn id(&self) -> &TaskGraphId {
        &self.id
    }

    /// Returns the observed optimistic-concurrency revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the current graph projection.
    #[must_use]
    pub fn graph(&self) -> &TaskGraph {
        &self.graph
    }

    /// Borrows the graph for mutations guarded by its domain methods.
    pub fn graph_mut(&mut self) -> &mut TaskGraph {
        &mut self.graph
    }
}

/// Atomic persistence boundary for Task Graph coordination.
pub trait TaskCoordinator: Send + Sync {
    /// Creates a graph at revision one, rejecting identity replacement.
    fn create<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
    ) -> HarnessFuture<'a, TaskGraphSnapshot>;

    /// Loads one current graph snapshot.
    fn load<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>>;

    /// Atomically saves a mutation only if its observed revision is current.
    fn compare_and_swap<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
    ) -> HarnessFuture<'a, TaskGraphSnapshot>;
}

/// In-memory coordinator with the same revision semantics as SQLite.
#[derive(Default)]
pub struct MemoryTaskCoordinator {
    graphs: Mutex<BTreeMap<TaskGraphId, TaskGraphSnapshot>>,
}

impl MemoryTaskCoordinator {
    /// Creates an empty coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskCoordinator for MemoryTaskCoordinator {
    fn create<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            validate_graph_id(&graph_id)?;
            validate_graph(&graph)?;
            let mut graphs = self.graphs.lock().await;
            if graphs.contains_key(&graph_id) {
                return Err(HarnessError::Orchestration(format!(
                    "Task Graph {graph_id} already exists"
                )));
            }
            let snapshot = TaskGraphSnapshot {
                id: graph_id.clone(),
                revision: 1,
                graph,
            };
            graphs.insert(graph_id, snapshot.clone());
            Ok(snapshot)
        })
    }

    fn load<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
        Box::pin(async move {
            validate_graph_id(graph_id)?;
            Ok(self.graphs.lock().await.get(graph_id).cloned())
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            validate_graph_id(&snapshot.id)?;
            validate_graph(&snapshot.graph)?;
            let mut graphs = self.graphs.lock().await;
            let current = graphs.get(&snapshot.id).ok_or_else(|| {
                HarnessError::Orchestration(format!("Task Graph {} does not exist", snapshot.id))
            })?;
            if current.revision != snapshot.revision {
                return Err(HarnessError::OrchestrationConflict {
                    graph_id: snapshot.id,
                    expected: snapshot.revision,
                    actual: current.revision,
                });
            }
            let next_revision = snapshot.revision.checked_add(1).ok_or_else(|| {
                HarnessError::Orchestration("Task Graph revision overflow".to_owned())
            })?;
            let saved = TaskGraphSnapshot {
                id: snapshot.id.clone(),
                revision: next_revision,
                graph: snapshot.graph,
            };
            graphs.insert(snapshot.id, saved.clone());
            Ok(saved)
        })
    }
}

/// SQLite coordinator using one transaction per graph creation or CAS.
pub struct SqliteTaskCoordinator {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteTaskCoordinator {
    /// Opens or creates a coordinator database with durable WAL settings.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_owned();
        let connection = task::spawn_blocking(move || {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            }
            let connection = Connection::open(path)
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            connection
                .busy_timeout(Duration::from_secs(5))
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            let mode: String = connection
                .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(HarnessError::Orchestration(format!(
                    "SQLite refused WAL mode and selected {mode}"
                )));
            }
            connection
                .execute_batch(
                    "
                    PRAGMA synchronous = FULL;
                    PRAGMA foreign_keys = ON;
                    CREATE TABLE IF NOT EXISTS task_graphs (
                        graph_id       TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL,
                        revision       INTEGER NOT NULL CHECK(revision > 0),
                        graph_json     TEXT NOT NULL
                    );
                    ",
                )
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            ensure_schema_column(&connection)?;
            Ok(connection)
        })
        .await
        .map_err(|error| {
            HarnessError::Orchestration(format!(
                "SQLite coordinator initialization task failed: {error}"
            ))
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
            let mut connection = connection.lock().map_err(|_| {
                HarnessError::Orchestration("SQLite coordinator lock poisoned".to_owned())
            })?;
            operation(&mut connection)
        })
        .await
        .map_err(|error| {
            HarnessError::Orchestration(format!("SQLite coordinator task failed: {error}"))
        })?
    }
}

impl TaskCoordinator for SqliteTaskCoordinator {
    fn create<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            validate_graph_id(&graph_id)?;
            let graph_json = encode_graph(&graph)?;
            let stored_id = graph_id.clone();
            let changed = self
                .with_connection(move |connection| {
                    connection
                        .execute(
                            "INSERT OR IGNORE INTO task_graphs
                                (graph_id, schema_version, revision, graph_json)
                             VALUES (?1, ?2, 1, ?3)",
                            params![
                                stored_id.as_str(),
                                i64::from(TASK_GRAPH_SCHEMA_VERSION),
                                graph_json
                            ],
                        )
                        .map_err(|error| HarnessError::Orchestration(error.to_string()))
                })
                .await?;
            if changed != 1 {
                return Err(HarnessError::Orchestration(format!(
                    "Task Graph {graph_id} already exists"
                )));
            }
            Ok(TaskGraphSnapshot {
                id: graph_id,
                revision: 1,
                graph,
            })
        })
    }

    fn load<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
        Box::pin(async move {
            validate_graph_id(graph_id)?;
            let requested_id = graph_id.clone();
            let loaded = self
                .with_connection(move |connection| {
                    connection
                        .query_row(
                            "SELECT schema_version, revision,
                                    length(CAST(graph_json AS BLOB)), graph_json
                             FROM task_graphs WHERE graph_id = ?1",
                            params![requested_id.as_str()],
                            |row| {
                                Ok((
                                    row.get::<_, i64>(0)?,
                                    row.get::<_, i64>(1)?,
                                    bounded_text(
                                        row,
                                        2,
                                        3,
                                        MAX_TASK_GRAPH_JSON_BYTES,
                                        "stored Task Graph snapshot",
                                    )?,
                                ))
                            },
                        )
                        .optional()
                        .map_err(|error| HarnessError::Orchestration(error.to_string()))
                })
                .await?;
            loaded
                .map(|(schema_version, revision, graph_json)| {
                    decode_snapshot(graph_id.clone(), schema_version, revision, &graph_json)
                })
                .transpose()
        })
    }

    fn compare_and_swap<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            validate_graph_id(&snapshot.id)?;
            let graph_json = encode_graph(&snapshot.graph)?;
            let expected = snapshot.revision;
            let next_revision = expected.checked_add(1).ok_or_else(|| {
                HarnessError::Orchestration("Task Graph revision overflow".to_owned())
            })?;
            let expected_sql = sql_revision(expected)?;
            let next_sql = sql_revision(next_revision)?;
            let graph_id = snapshot.id.clone();
            let conflict_id = snapshot.id.clone();
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
                let (schema_version, actual_sql) = transaction
                    .query_row(
                        "SELECT schema_version, revision
                         FROM task_graphs WHERE graph_id = ?1",
                        params![graph_id.as_str()],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .optional()
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?
                    .ok_or_else(|| {
                        HarnessError::Orchestration(format!("Task Graph {graph_id} does not exist"))
                    })?;
                validate_schema_version(schema_version)?;
                let actual = revision_from_sql(actual_sql)?;
                if actual != expected {
                    return Err(HarnessError::OrchestrationConflict {
                        graph_id: conflict_id,
                        expected,
                        actual,
                    });
                }
                let changed = transaction
                    .execute(
                        "UPDATE task_graphs
                         SET revision = ?1, graph_json = ?2
                         WHERE graph_id = ?3 AND revision = ?4",
                        params![next_sql, graph_json, graph_id.as_str(), expected_sql],
                    )
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
                if changed != 1 {
                    return Err(HarnessError::Orchestration(
                        "Task Graph CAS changed an unexpected row count".to_owned(),
                    ));
                }
                transaction
                    .commit()
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))
            })
            .await?;
            Ok(TaskGraphSnapshot {
                id: snapshot.id,
                revision: next_revision,
                graph: snapshot.graph,
            })
        })
    }
}

fn validate_graph_id(graph_id: &TaskGraphId) -> Result<(), HarnessError> {
    if graph_id.as_str().is_empty()
        || graph_id.as_str().len() > 256
        || graph_id.as_str().chars().any(char::is_control)
    {
        return Err(HarnessError::Orchestration(
            "Task Graph identity must be 1-256 non-control bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_graph(graph: &TaskGraph) -> Result<(), HarnessError> {
    graph.validate_integrity()?;
    let bytes = serde_json::to_vec(graph)
        .map_err(|error| HarnessError::Orchestration(format!("encode Task Graph: {error}")))?;
    if bytes.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "Task Graph snapshot exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn encode_graph(graph: &TaskGraph) -> Result<String, HarnessError> {
    graph.validate_integrity()?;
    let json = serde_json::to_string(graph)
        .map_err(|error| HarnessError::Orchestration(format!("encode Task Graph: {error}")))?;
    if json.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "Task Graph snapshot exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    Ok(json)
}

fn decode_snapshot(
    id: TaskGraphId,
    schema_version: i64,
    revision: i64,
    graph_json: &str,
) -> Result<TaskGraphSnapshot, HarnessError> {
    validate_schema_version(schema_version)?;
    if graph_json.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "stored Task Graph snapshot exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    let graph: TaskGraph = serde_json::from_str(graph_json)
        .map_err(|error| HarnessError::Orchestration(format!("decode Task Graph: {error}")))?;
    graph.validate_integrity()?;
    Ok(TaskGraphSnapshot {
        id,
        revision: revision_from_sql(revision)?,
        graph,
    })
}

fn validate_schema_version(schema_version: i64) -> Result<(), HarnessError> {
    if schema_version == i64::from(TASK_GRAPH_SCHEMA_VERSION) {
        Ok(())
    } else {
        Err(HarnessError::Orchestration(format!(
            "unsupported Task Graph schema version {schema_version}"
        )))
    }
}

fn ensure_schema_column(connection: &Connection) -> Result<(), HarnessError> {
    let mut statement = connection
        .prepare("PRAGMA table_info(task_graphs)")
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let mut has_schema_version = false;
    for column in columns {
        if column.map_err(|error| HarnessError::Orchestration(error.to_string()))?
            == "schema_version"
        {
            has_schema_version = true;
            break;
        }
    }
    drop(statement);
    if !has_schema_version {
        connection
            .execute(
                "ALTER TABLE task_graphs
                 ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1",
                [],
            )
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    }
    Ok(())
}

fn sql_revision(revision: u64) -> Result<i64, HarnessError> {
    i64::try_from(revision)
        .map_err(|_| HarnessError::Orchestration("Task Graph revision exceeds SQLite".to_owned()))
}

fn revision_from_sql(revision: i64) -> Result<u64, HarnessError> {
    u64::try_from(revision).map_err(|_| {
        HarnessError::Orchestration("stored Task Graph revision is invalid".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use rusqlite::Connection;

    use super::{MemoryTaskCoordinator, SqliteTaskCoordinator, TaskCoordinator};
    use crate::{
        HarnessError, TaskCompletion, TaskDefinition, TaskGraph, TaskGraphId, TaskId, WorkspaceMode,
    };

    fn graph() -> TaskGraph {
        TaskGraph::new(vec![TaskDefinition {
            id: TaskId::from_static("task-a"),
            description: "persistent work".to_owned(),
            dependencies: BTreeSet::new(),
            priority: 0,
            workspace: WorkspaceMode::Isolated,
        }])
        .expect("graph")
    }

    #[tokio::test]
    async fn memory_coordinator_rejects_stale_revision() {
        let coordinator = MemoryTaskCoordinator::new();
        let graph_id = TaskGraphId::from_static("graph-memory");
        let first = coordinator
            .create(graph_id.clone(), graph())
            .await
            .expect("create");
        let stale = first.clone();
        let saved = coordinator
            .compare_and_swap(first)
            .await
            .expect("first save");
        assert_eq!(saved.revision(), 2);
        let conflict = coordinator
            .compare_and_swap(stale)
            .await
            .expect_err("stale revision");
        assert!(matches!(
            conflict,
            HarnessError::OrchestrationConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn sqlite_reopens_and_fences_competing_workers() {
        let path = temporary_database_path();
        let first = Arc::new(
            SqliteTaskCoordinator::open(&path)
                .await
                .expect("first coordinator"),
        );
        let second = Arc::new(
            SqliteTaskCoordinator::open(&path)
                .await
                .expect("second coordinator"),
        );
        let graph_id = TaskGraphId::from_static("graph-sqlite");
        first
            .create(graph_id.clone(), graph())
            .await
            .expect("create");

        let mut left = first.load(&graph_id).await.expect("load").expect("graph");
        let mut right = second.load(&graph_id).await.expect("load").expect("graph");
        let left_claim = left
            .graph_mut()
            .claim_ready("worker-left", 100, 10, 1)
            .expect("left claim")
            .remove(0);
        right
            .graph_mut()
            .claim_ready("worker-right", 100, 10, 1)
            .expect("right claim");
        first.compare_and_swap(left).await.expect("left wins CAS");
        let conflict = second
            .compare_and_swap(right)
            .await
            .expect_err("right loses CAS");
        assert!(matches!(
            conflict,
            HarnessError::OrchestrationConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));

        drop(first);
        drop(second);
        let reopened = SqliteTaskCoordinator::open(&path)
            .await
            .expect("reopen coordinator");
        let mut recovered = reopened
            .load(&graph_id)
            .await
            .expect("load recovered")
            .expect("recovered graph");
        let new_claim = recovered
            .graph_mut()
            .claim_ready("worker-new", 110, 10, 1)
            .expect("reclaim expired")
            .remove(0);
        assert_ne!(left_claim.lease.id, new_claim.lease.id);
        let stale = recovered.graph_mut().complete(
            &left_claim.task.id,
            &left_claim.lease.id,
            111,
            TaskCompletion {
                summary: "stale".to_owned(),
                artifacts: Vec::new(),
            },
        );
        assert!(matches!(stale, Err(HarnessError::Orchestration(_))));
        recovered
            .graph_mut()
            .complete(
                &new_claim.task.id,
                &new_claim.lease.id,
                111,
                TaskCompletion {
                    summary: "current".to_owned(),
                    artifacts: Vec::new(),
                },
            )
            .expect("current lease");
        let saved = reopened
            .compare_and_swap(recovered)
            .await
            .expect("save recovered graph");
        assert_eq!(saved.revision(), 3);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_adds_the_dev_baseline_column_and_rejects_unknown_schema() {
        let path = temporary_database_path();
        let legacy = Connection::open(&path).expect("open legacy database");
        legacy
            .execute_batch(
                "
                CREATE TABLE task_graphs (
                    graph_id   TEXT PRIMARY KEY,
                    revision   INTEGER NOT NULL CHECK(revision > 0),
                    graph_json TEXT NOT NULL
                );
                ",
            )
            .expect("create legacy table");
        drop(legacy);

        let coordinator = SqliteTaskCoordinator::open(&path)
            .await
            .expect("add schema baseline");
        let graph_id = TaskGraphId::from_static("graph-schema");
        coordinator
            .create(graph_id.clone(), graph())
            .await
            .expect("create versioned graph");
        drop(coordinator);

        let corrupt = Connection::open(&path).expect("open versioned database");
        corrupt
            .execute(
                "UPDATE task_graphs SET schema_version = 99 WHERE graph_id = ?1",
                [graph_id.as_str()],
            )
            .expect("inject unknown schema");
        drop(corrupt);
        let coordinator = SqliteTaskCoordinator::open(&path).await.expect("reopen");
        let error = coordinator
            .load(&graph_id)
            .await
            .expect_err("unknown schema must fail closed");
        assert!(matches!(error, HarnessError::Orchestration(_)));
        remove_database_files(&path);
    }

    fn temporary_database_path() -> PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-orchestration-{}.db",
            TaskGraphId::generate()
        ))
    }

    fn remove_database_files(path: &PathBuf) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
