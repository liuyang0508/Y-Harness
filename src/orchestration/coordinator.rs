//! Atomic persistence ports for complete Task Graph aggregates.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};

use super::{MAX_TASK_GRAPH_JSON_BYTES, TaskGraph};
use crate::{AuthorityContext, HarnessError, HarnessFuture, TaskGraphId, sqlite::bounded_text};

/// Current durable Task Coordinator graph schema.
pub const TASK_GRAPH_SCHEMA_VERSION: u32 = 3;

/// Revisioned, durable Task Graph aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TaskGraphSnapshot {
    /// Stable graph identity.
    id: TaskGraphId,
    /// Immutable tenant boundary, or `None` for an unscoped graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
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

    /// Returns the immutable tenant owner, or `None` for an unscoped graph.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
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
    /// Creates an unscoped graph at revision one.
    fn create<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            self.create_as(graph_id, graph, &AuthorityContext::local_process())
                .await
        })
    }

    /// Creates a graph under the exact trusted tenant authority.
    fn create_as<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot>;

    /// Loads one current unscoped graph snapshot.
    fn load<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
        Box::pin(async move {
            self.load_as(graph_id, &AuthorityContext::local_process())
                .await
        })
    }

    /// Loads one graph only when its tenant exactly matches the authority.
    fn load_as<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>>;

    /// Atomically saves an unscoped mutation at its observed revision.
    fn compare_and_swap<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            self.compare_and_swap_as(snapshot, &AuthorityContext::local_process())
                .await
        })
    }

    /// Atomically saves a mutation inside the exact tenant boundary.
    fn compare_and_swap_as<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot>;
}

/// In-memory coordinator with the same revision semantics as SQLite.
#[derive(Default)]
pub struct MemoryTaskCoordinator {
    graphs: Mutex<BTreeMap<(String, TaskGraphId), TaskGraphSnapshot>>,
}

impl MemoryTaskCoordinator {
    /// Creates an empty coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TaskCoordinator for MemoryTaskCoordinator {
    fn create_as<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(&graph_id)?;
            validate_graph(&graph, authority.tenant_id())?;
            let mut graphs = self.graphs.lock().await;
            let key = (
                tenant_storage_key(authority.tenant_id()).to_owned(),
                graph_id.clone(),
            );
            if graphs.contains_key(&key) {
                return Err(HarnessError::Orchestration(format!(
                    "Task Graph {graph_id} already exists"
                )));
            }
            let snapshot = TaskGraphSnapshot {
                id: graph_id.clone(),
                tenant_id: authority.tenant_id().map(str::to_owned),
                revision: 1,
                graph,
            };
            graphs.insert(key, snapshot.clone());
            Ok(snapshot)
        })
    }

    fn load_as<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(graph_id)?;
            let key = (
                tenant_storage_key(authority.tenant_id()).to_owned(),
                graph_id.clone(),
            );
            Ok(self.graphs.lock().await.get(&key).cloned())
        })
    }

    fn compare_and_swap_as<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(&snapshot.id)?;
            validate_graph(&snapshot.graph, authority.tenant_id())?;
            if snapshot.tenant_id() != authority.tenant_id() {
                return Err(graph_does_not_exist(&snapshot.id));
            }
            let mut graphs = self.graphs.lock().await;
            let key = (
                tenant_storage_key(authority.tenant_id()).to_owned(),
                snapshot.id.clone(),
            );
            let current = graphs
                .get(&key)
                .ok_or_else(|| graph_does_not_exist(&snapshot.id))?;
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
                tenant_id: snapshot.tenant_id,
                revision: next_revision,
                graph: snapshot.graph,
            };
            graphs.insert(key, saved.clone());
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
                        tenant_id      TEXT NOT NULL,
                        graph_id       TEXT NOT NULL,
                        schema_version INTEGER NOT NULL,
                        revision       INTEGER NOT NULL CHECK(revision > 0),
                        graph_json     TEXT NOT NULL,
                        PRIMARY KEY (tenant_id, graph_id)
                    );
                    ",
                )
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            validate_current_table(&connection)?;
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
    fn create_as<'a>(
        &'a self,
        graph_id: TaskGraphId,
        graph: TaskGraph,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(&graph_id)?;
            let tenant_id = authority.tenant_id().map(str::to_owned);
            let stored_tenant = tenant_storage_key(tenant_id.as_deref()).to_owned();
            let graph_json = encode_graph(&graph, tenant_id.as_deref())?;
            let stored_id = graph_id.clone();
            let changed = self
                .with_connection(move |connection| {
                    connection
                        .execute(
                            "INSERT OR IGNORE INTO task_graphs
                                (tenant_id, graph_id, schema_version, revision, graph_json)
                             VALUES (?1, ?2, ?3, 1, ?4)",
                            params![
                                stored_tenant,
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
                tenant_id,
                revision: 1,
                graph,
            })
        })
    }

    fn load_as<'a>(
        &'a self,
        graph_id: &'a TaskGraphId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(graph_id)?;
            let requested_id = graph_id.clone();
            let requested_tenant = tenant_storage_key(authority.tenant_id()).to_owned();
            let loaded = self
                .with_connection(move |connection| {
                    connection
                        .query_row(
                            "SELECT length(CAST(tenant_id AS BLOB)), tenant_id,
                                    schema_version, revision,
                                    length(CAST(graph_json AS BLOB)), graph_json
                             FROM task_graphs
                             WHERE tenant_id = ?1 AND graph_id = ?2",
                            params![requested_tenant, requested_id.as_str()],
                            |row| {
                                Ok((
                                    bounded_text(row, 0, 1, 256, "stored Task tenant")?,
                                    row.get::<_, i64>(2)?,
                                    row.get::<_, i64>(3)?,
                                    bounded_text(
                                        row,
                                        4,
                                        5,
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
                .map(|(stored_tenant, schema_version, revision, graph_json)| {
                    decode_snapshot(
                        graph_id.clone(),
                        &stored_tenant,
                        schema_version,
                        revision,
                        &graph_json,
                    )
                })
                .transpose()
        })
    }

    fn compare_and_swap_as<'a>(
        &'a self,
        snapshot: TaskGraphSnapshot,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, TaskGraphSnapshot> {
        Box::pin(async move {
            authority.validate_current("Task Coordinator authority")?;
            validate_graph_id(&snapshot.id)?;
            if snapshot.tenant_id() != authority.tenant_id() {
                return Err(graph_does_not_exist(&snapshot.id));
            }
            let graph_json = encode_graph(&snapshot.graph, snapshot.tenant_id())?;
            let expected = snapshot.revision;
            let next_revision = expected.checked_add(1).ok_or_else(|| {
                HarnessError::Orchestration("Task Graph revision overflow".to_owned())
            })?;
            let expected_sql = sql_revision(expected)?;
            let next_sql = sql_revision(next_revision)?;
            let graph_id = snapshot.id.clone();
            let conflict_id = snapshot.id.clone();
            let stored_tenant = tenant_storage_key(snapshot.tenant_id()).to_owned();
            self.with_connection(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
                let (schema_version, actual_sql) = transaction
                    .query_row(
                        "SELECT schema_version, revision
                         FROM task_graphs WHERE tenant_id = ?1 AND graph_id = ?2",
                        params![stored_tenant, graph_id.as_str()],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                    )
                    .optional()
                    .map_err(|error| HarnessError::Orchestration(error.to_string()))?
                    .ok_or_else(|| graph_does_not_exist(&graph_id))?;
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
                         WHERE tenant_id = ?3 AND graph_id = ?4 AND revision = ?5",
                        params![
                            next_sql,
                            graph_json,
                            stored_tenant,
                            graph_id.as_str(),
                            expected_sql
                        ],
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
                tenant_id: snapshot.tenant_id,
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

fn validate_graph(graph: &TaskGraph, tenant_id: Option<&str>) -> Result<(), HarnessError> {
    graph.validate_integrity()?;
    graph.validate_execution_binding_tenant(tenant_id)?;
    let bytes = serde_json::to_vec(graph)
        .map_err(|error| HarnessError::Orchestration(format!("encode Task Graph: {error}")))?;
    if bytes.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "Task Graph snapshot exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

#[derive(Deserialize)]
struct StoredTaskGraph {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    graph: TaskGraph,
}

#[derive(Serialize)]
struct StoredTaskGraphRef<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<&'a str>,
    graph: &'a TaskGraph,
}

pub(super) fn encode_graph(
    graph: &TaskGraph,
    tenant_id: Option<&str>,
) -> Result<String, HarnessError> {
    graph.validate_integrity()?;
    graph.validate_execution_binding_tenant(tenant_id)?;
    let json = serde_json::to_string(&StoredTaskGraphRef { tenant_id, graph })
        .map_err(|error| HarnessError::Orchestration(format!("encode Task Graph: {error}")))?;
    if json.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "Task Graph snapshot exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    Ok(json)
}

pub(super) fn decode_snapshot(
    id: TaskGraphId,
    stored_tenant: &str,
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
    let stored: StoredTaskGraph = serde_json::from_str(graph_json)
        .map_err(|error| HarnessError::Orchestration(format!("decode Task Graph: {error}")))?;
    let tenant_id = tenant_from_storage_key(stored_tenant)?;
    if stored.tenant_id.as_deref() != tenant_id.as_deref() {
        return Err(HarnessError::Orchestration(
            "stored Task Graph tenant projection does not match its body".to_owned(),
        ));
    }
    stored.graph.validate_integrity()?;
    stored
        .graph
        .validate_execution_binding_tenant(tenant_id.as_deref())?;
    Ok(TaskGraphSnapshot {
        id,
        tenant_id,
        revision: revision_from_sql(revision)?,
        graph: stored.graph,
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

fn validate_current_table(connection: &Connection) -> Result<(), HarnessError> {
    let mut statement = connection
        .prepare("PRAGMA table_info(task_graphs)")
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let mut column_count = 0_usize;
    let mut tenant_primary_key = 0;
    let mut graph_primary_key = 0;
    let mut tenant_not_null = false;
    let mut graph_not_null = false;
    let mut schema_not_null = false;
    let mut revision_not_null = false;
    let mut graph_json_not_null = false;
    for column in columns {
        let (name, not_null, primary_key) =
            column.map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        column_count = column_count.saturating_add(1);
        match name.as_str() {
            "tenant_id" => {
                tenant_not_null = not_null == 1;
                tenant_primary_key = primary_key;
            }
            "graph_id" => {
                graph_not_null = not_null == 1;
                graph_primary_key = primary_key;
            }
            "schema_version" => schema_not_null = not_null == 1,
            "revision" => revision_not_null = not_null == 1,
            "graph_json" => graph_json_not_null = not_null == 1,
            _ => {}
        }
    }
    drop(statement);
    if column_count != 5
        || !tenant_not_null
        || !graph_not_null
        || !schema_not_null
        || !revision_not_null
        || !graph_json_not_null
        || tenant_primary_key != 1
        || graph_primary_key != 2
    {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph migration required; run `yh task-migrate <database> <backup>` before opening this store"
                .to_owned(),
        ));
    }
    let unsupported: Option<i64> = connection
        .query_row(
            "SELECT schema_version FROM task_graphs
             WHERE schema_version != ?1 LIMIT 1",
            [i64::from(TASK_GRAPH_SCHEMA_VERSION)],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if let Some(version) = unsupported {
        return Err(HarnessError::Orchestration(format!(
            "unsupported Task Graph schema version {version}"
        )));
    }
    Ok(())
}

fn tenant_storage_key(tenant_id: Option<&str>) -> &str {
    tenant_id.unwrap_or("")
}

fn tenant_from_storage_key(value: &str) -> Result<Option<String>, HarnessError> {
    if value.is_empty() {
        return Ok(None);
    }
    AuthorityContext::validate_tenant(value)?;
    Ok(Some(value.to_owned()))
}

fn graph_does_not_exist(graph_id: &TaskGraphId) -> HarnessError {
    HarnessError::Orchestration(format!("Task Graph {graph_id} does not exist"))
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
        ActorIdentity, AuthorityContext, ExecutionBinding, HarnessError, TaskCompletion,
        TaskDefinition, TaskGraph, TaskGraphId, TaskId, WorkspaceMode,
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
    async fn tenant_ownership_fences_memory_and_sqlite_graph_access() {
        let path = temporary_database_path();
        let sqlite = SqliteTaskCoordinator::open(&path)
            .await
            .expect("sqlite coordinator");
        assert_tenant_fencing(&MemoryTaskCoordinator::new()).await;
        assert_tenant_fencing(&sqlite).await;
        drop(sqlite);
        let reopened = SqliteTaskCoordinator::open(&path)
            .await
            .expect("reopen sqlite coordinator");
        let graph_id = TaskGraphId::from_static("shared-graph");
        assert_eq!(
            reopened
                .load_as(&graph_id, &authority("tenant-a"))
                .await
                .expect("load tenant a after reopen")
                .expect("tenant a graph")
                .tenant_id(),
            Some("tenant-a")
        );
        assert_eq!(
            reopened
                .load_as(&graph_id, &authority("tenant-b"))
                .await
                .expect("load tenant b after reopen")
                .expect("tenant b graph")
                .tenant_id(),
            Some("tenant-b")
        );
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_rejects_task_tenant_projection_drift() {
        let path = temporary_database_path();
        let coordinator = SqliteTaskCoordinator::open(&path)
            .await
            .expect("create current store");
        let graph_id = TaskGraphId::from_static("graph-drift");
        let tenant_a = authority("tenant-a");
        coordinator
            .create_as(graph_id.clone(), graph(), &tenant_a)
            .await
            .expect("create tenant graph");
        drop(coordinator);

        let corrupt = Connection::open(&path).expect("open versioned database");
        corrupt
            .execute(
                "UPDATE task_graphs SET tenant_id = 'tenant-b' WHERE graph_id = ?1",
                [graph_id.as_str()],
            )
            .expect("inject tenant drift");
        drop(corrupt);
        let coordinator = SqliteTaskCoordinator::open(&path).await.expect("reopen");
        let tenant_b = authority("tenant-b");
        let error = coordinator
            .load_as(&graph_id, &tenant_b)
            .await
            .expect_err("tenant projection drift must fail closed");
        assert!(matches!(error, HarnessError::Orchestration(_)));
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn task_attempt_binding_is_tenant_exact_and_survives_sqlite_reopen() {
        let path = temporary_database_path();
        let coordinator = SqliteTaskCoordinator::open(&path)
            .await
            .expect("create current store");
        let tenant_a = authority("tenant-a");
        let binding = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            7,
            Some("tenant-a".to_owned()),
        )
        .expect("binding");
        let mut bound = graph();
        let claim = bound
            .claim_ready_with_binding("worker-a", 100, 10, 1, Some(&binding))
            .expect("claim")
            .remove(0);
        let graph_id = TaskGraphId::from_static("graph-bound");
        coordinator
            .create_as(graph_id.clone(), bound.clone(), &tenant_a)
            .await
            .expect("persist bound graph");
        let mismatch = coordinator
            .create_as(
                TaskGraphId::from_static("graph-bound-mismatch"),
                bound,
                &authority("tenant-b"),
            )
            .await
            .expect_err("binding cannot cross tenant");
        assert!(mismatch.to_string().contains("binding tenant"));
        drop(coordinator);

        let reopened = SqliteTaskCoordinator::open(&path)
            .await
            .expect("reopen coordinator");
        let restored = reopened
            .load_as(&graph_id, &tenant_a)
            .await
            .expect("load")
            .expect("bound graph");
        assert_eq!(
            restored
                .graph()
                .execution_binding_for_lease(&claim.lease.id),
            Some(&binding)
        );
        remove_database_files(&path);
    }

    async fn assert_tenant_fencing(coordinator: &dyn TaskCoordinator) {
        let graph_id = TaskGraphId::from_static("shared-graph");
        let tenant_a = authority("tenant-a");
        let tenant_b = authority("tenant-b");
        let created_a = coordinator
            .create_as(graph_id.clone(), graph(), &tenant_a)
            .await
            .expect("create tenant a");
        let created_b = coordinator
            .create_as(graph_id.clone(), graph(), &tenant_b)
            .await
            .expect("create tenant b with same graph id");
        assert_eq!(created_a.tenant_id(), Some("tenant-a"));
        assert_eq!(created_b.tenant_id(), Some("tenant-b"));
        assert!(
            coordinator
                .load(&graph_id)
                .await
                .expect("unscoped load")
                .is_none()
        );
        assert_eq!(
            coordinator
                .load_as(&graph_id, &tenant_a)
                .await
                .expect("tenant a load")
                .expect("tenant a graph")
                .tenant_id(),
            Some("tenant-a")
        );
        let error = coordinator
            .compare_and_swap_as(created_a, &tenant_b)
            .await
            .expect_err("cross-tenant CAS");
        assert!(error.to_string().contains("does not exist"));
    }

    fn authority(tenant: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "task-test".to_owned(),
            },
            Some(tenant.to_owned()),
        )
        .expect("authority")
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
