use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, TransactionBehavior};
use semver::Version;
use tokio::time::{sleep, timeout};
use y_harness::{
    ActorIdentity, AuthorityContext, SqliteTaskCoordinator, SqliteTaskWorkflowCurrentGuard,
    SqliteWorkflowCoordinator, TaskCapabilitySet, TaskCoordinator, TaskDefinition, TaskGraph,
    TaskGraphId, TaskId, WorkflowCommand, WorkflowCommandId, WorkflowCommandKind,
    WorkflowCoordinator, WorkflowCreateRequest, WorkflowDefinition, WorkflowRunId, WorkflowWaitId,
    WorkspaceMode,
};

/// Complete pair of isolated stores and exact authority used by one test.
struct GuardFixture {
    task_path: PathBuf,
    workflow_path: PathBuf,
    tasks: Arc<SqliteTaskCoordinator>,
    workflows: Arc<SqliteWorkflowCoordinator>,
    authority: AuthorityContext,
    graph_id: TaskGraphId,
    run_id: WorkflowRunId,
}

impl GuardFixture {
    /// Creates one exact Task Graph and linked Workflow Run in separate stores.
    async fn create(label: &str) -> Self {
        let task_path = temporary_database_path(label, "tasks");
        let workflow_path = temporary_database_path(label, "workflows");
        let tasks = Arc::new(
            SqliteTaskCoordinator::open(&task_path)
                .await
                .expect("open Task store"),
        );
        let workflows = Arc::new(
            SqliteWorkflowCoordinator::open(&workflow_path)
                .await
                .expect("open Workflow store"),
        );
        let authority = authority("tenant-current-guard");
        let graph_id = TaskGraphId::from_static("guarded-graph");
        let run_id = WorkflowRunId::from_static("guarded-run");
        tasks
            .create_as(graph_id.clone(), graph(), &authority)
            .await
            .expect("create graph");
        workflows
            .create_as(
                run_id.clone(),
                workflow_request(graph_id.clone()),
                10,
                &authority,
            )
            .await
            .expect("create workflow");
        Self {
            task_path,
            workflow_path,
            tasks,
            workflows,
            authority,
            graph_id,
            run_id,
        }
    }

    /// Removes the fixture after every Coordinator and manual connection is dropped.
    fn remove_files(self) {
        let Self {
            task_path,
            workflow_path,
            tasks,
            workflows,
            ..
        } = self;
        drop(tasks);
        drop(workflows);
        remove_database_files(&task_path);
        remove_database_files(&workflow_path);
    }
}

/// Both legitimate writers block under the pair and succeed after explicit release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paired_guard_blocks_both_writers_and_releases() {
    let fixture = GuardFixture::create("writers").await;
    let task_writer_store = Arc::new(
        SqliteTaskCoordinator::open(&fixture.task_path)
            .await
            .expect("open competing Task store"),
    );
    let workflow_writer_store = Arc::new(
        SqliteWorkflowCoordinator::open(&fixture.workflow_path)
            .await
            .expect("open competing Workflow store"),
    );
    let task_update = task_writer_store
        .load_as(&fixture.graph_id, &fixture.authority)
        .await
        .expect("load Task update")
        .expect("Task current");
    let guard = SqliteTaskWorkflowCurrentGuard::acquire_as(
        &fixture.tasks,
        &fixture.graph_id,
        &fixture.workflows,
        &fixture.run_id,
        &fixture.authority,
    )
    .await
    .expect("acquire pair");
    assert_eq!(guard.task_graph().revision(), 1);
    assert_eq!(guard.workflow_run().revision(), 1);
    assert_eq!(guard.task_graph().snapshot().id(), &fixture.graph_id);
    assert_eq!(guard.workflow_run().snapshot().id(), &fixture.run_id);
    let task_digest = guard.task_graph().digest();
    let workflow_digest = guard.workflow_run().digest();

    let task_authority = fixture.authority.clone();
    let task_writer = tokio::spawn(async move {
        task_writer_store
            .compare_and_swap_as(task_update, &task_authority)
            .await
    });
    let workflow_authority = fixture.authority.clone();
    let run_id = fixture.run_id.clone();
    let workflow_writer = tokio::spawn(async move {
        workflow_writer_store
            .apply_as(&run_id, 1, wait_command(), 20, &workflow_authority)
            .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(!task_writer.is_finished(), "Task CAS crossed held guard");
    assert!(
        !workflow_writer.is_finished(),
        "Workflow apply crossed held guard"
    );

    guard.release().await.expect("release pair");
    let task_saved = timeout(Duration::from_secs(2), task_writer)
        .await
        .expect("Task writer remained blocked")
        .expect("Task writer join")
        .expect("Task writer update");
    let workflow_saved = timeout(Duration::from_secs(2), workflow_writer)
        .await
        .expect("Workflow writer remained blocked")
        .expect("Workflow writer join")
        .expect("Workflow writer update");
    assert_eq!(task_saved.revision(), 2);
    assert_eq!(workflow_saved.snapshot.revision(), 2);

    let changed = SqliteTaskWorkflowCurrentGuard::acquire_as(
        &fixture.tasks,
        &fixture.graph_id,
        &fixture.workflows,
        &fixture.run_id,
        &fixture.authority,
    )
    .await
    .expect("reacquire changed pair");
    assert_ne!(changed.task_graph().digest(), task_digest);
    assert_ne!(changed.workflow_run().digest(), workflow_digest);
    changed.release().await.expect("release changed pair");
    fixture.remove_files();
}

/// Preexisting writers, cancellation, and second-store contention never strand Task locks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contention_and_cancel_release_the_first_store() {
    let fixture = GuardFixture::create("contention").await;

    let mut preexisting_task_connection = Connection::open(&fixture.task_path).expect("Task SQL");
    let preexisting_task = preexisting_task_connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("preexisting Task writer");
    let blocked_tasks = fixture.tasks.clone();
    let blocked_workflows = fixture.workflows.clone();
    let blocked_graph = fixture.graph_id.clone();
    let blocked_run = fixture.run_id.clone();
    let blocked_authority = fixture.authority.clone();
    let preexisting_wait = tokio::spawn(async move {
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &blocked_tasks,
            &blocked_graph,
            &blocked_workflows,
            &blocked_run,
            &blocked_authority,
        )
        .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(
        !preexisting_wait.is_finished(),
        "preexisting Task writer was crossed"
    );
    preexisting_task.rollback().expect("release Task writer");
    let acquired = timeout(Duration::from_secs(2), preexisting_wait)
        .await
        .expect("guard did not recover preexisting writer")
        .expect("preexisting guard join")
        .expect("preexisting guard acquire");
    acquired.release().await.expect("release recovered guard");

    let mut workflow_connection = Connection::open(&fixture.workflow_path).expect("Workflow SQL");
    let workflow_writer = workflow_connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("preexisting Workflow writer");
    let cancelled_tasks = fixture.tasks.clone();
    let cancelled_workflows = fixture.workflows.clone();
    let cancelled_graph = fixture.graph_id.clone();
    let cancelled_run = fixture.run_id.clone();
    let cancelled_authority = fixture.authority.clone();
    let cancelled = tokio::spawn(async move {
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &cancelled_tasks,
            &cancelled_graph,
            &cancelled_workflows,
            &cancelled_run,
            &cancelled_authority,
        )
        .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(!cancelled.is_finished(), "Workflow contention was crossed");
    cancelled.abort();
    let _ = cancelled.await;

    let task_writer_store = SqliteTaskCoordinator::open(&fixture.task_path)
        .await
        .expect("open Task writer after cancel");
    let task_update = task_writer_store
        .load_as(&fixture.graph_id, &fixture.authority)
        .await
        .expect("load Task after cancel")
        .expect("Task after cancel");
    let task_saved = timeout(
        Duration::from_secs(2),
        task_writer_store.compare_and_swap_as(task_update, &fixture.authority),
    )
    .await
    .expect("cancelled pair stranded Task lock")
    .expect("Task update after cancel");
    assert_eq!(task_saved.revision(), 2);

    let waiting_tasks = fixture.tasks.clone();
    let waiting_workflows = fixture.workflows.clone();
    let waiting_graph = fixture.graph_id.clone();
    let waiting_run = fixture.run_id.clone();
    let waiting_authority = fixture.authority.clone();
    let waiting = tokio::spawn(async move {
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &waiting_tasks,
            &waiting_graph,
            &waiting_workflows,
            &waiting_run,
            &waiting_authority,
        )
        .await
    });
    sleep(Duration::from_millis(100)).await;
    let second_update = task_writer_store
        .load_as(&fixture.graph_id, &fixture.authority)
        .await
        .expect("load Task during second-store contention")
        .expect("Task during second-store contention");
    let second_saved = timeout(
        Duration::from_secs(2),
        task_writer_store.compare_and_swap_as(second_update, &fixture.authority),
    )
    .await
    .expect("Workflow contention retained first Task lock")
    .expect("Task update during Workflow contention");
    assert_eq!(second_saved.revision(), 3);
    workflow_writer.rollback().expect("release Workflow writer");
    let waiting = timeout(Duration::from_secs(2), waiting)
        .await
        .expect("guard did not recover second-store contention")
        .expect("second-store guard join")
        .expect("second-store guard acquire");
    assert_eq!(waiting.task_graph().revision(), 3);
    waiting.release().await.expect("release waiting guard");
    drop(task_writer_store);
    drop(preexisting_task_connection);
    drop(workflow_connection);
    fixture.remove_files();
}

/// Pair acquisition is tenant fenced, rejects aliases, and serializes without deadlock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pair_guard_rejects_foreign_coordinates_and_has_one_lock_order() {
    let fixture = GuardFixture::create("authority").await;
    let foreign = authority("foreign-tenant");
    assert!(
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &fixture.tasks,
            &fixture.graph_id,
            &fixture.workflows,
            &fixture.run_id,
            &foreign,
        )
        .await
        .is_err()
    );
    assert!(
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &fixture.tasks,
            &TaskGraphId::from_static("foreign-graph"),
            &fixture.workflows,
            &fixture.run_id,
            &fixture.authority,
        )
        .await
        .is_err()
    );
    assert!(
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &fixture.tasks,
            &fixture.graph_id,
            &fixture.workflows,
            &WorkflowRunId::from_static("foreign-run"),
            &fixture.authority,
        )
        .await
        .is_err()
    );
    let other_graph = TaskGraphId::from_static("other-existing-graph");
    fixture
        .tasks
        .create_as(other_graph.clone(), graph(), &fixture.authority)
        .await
        .expect("create other exact graph");
    assert!(
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &fixture.tasks,
            &other_graph,
            &fixture.workflows,
            &fixture.run_id,
            &fixture.authority,
        )
        .await
        .is_err(),
        "existing cross-linked graph must not form a current pair"
    );

    let first = SqliteTaskWorkflowCurrentGuard::acquire_as(
        &fixture.tasks,
        &fixture.graph_id,
        &fixture.workflows,
        &fixture.run_id,
        &fixture.authority,
    )
    .await
    .expect("first ordered guard");
    let second_tasks = fixture.tasks.clone();
    let second_workflows = fixture.workflows.clone();
    let second_graph = fixture.graph_id.clone();
    let second_run = fixture.run_id.clone();
    let second_authority = fixture.authority.clone();
    let second = tokio::spawn(async move {
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &second_tasks,
            &second_graph,
            &second_workflows,
            &second_run,
            &second_authority,
        )
        .await
    });
    sleep(Duration::from_millis(100)).await;
    assert!(!second.is_finished(), "second guard crossed first guard");
    drop(first);
    let second = timeout(Duration::from_secs(2), second)
        .await
        .expect("ordered guards deadlocked")
        .expect("second guard join")
        .expect("second ordered guard");
    second.release().await.expect("release second guard");

    let same_path = temporary_database_path("same-store", "combined");
    let initialize_tasks = SqliteTaskCoordinator::open(&same_path)
        .await
        .expect("initialize same Task store");
    let initialize_workflows = SqliteWorkflowCoordinator::open(&same_path)
        .await
        .expect("initialize same Workflow store");
    drop(initialize_tasks);
    drop(initialize_workflows);
    let same_tasks = SqliteTaskCoordinator::open(&same_path)
        .await
        .expect("same Task store");
    let same_workflows = SqliteWorkflowCoordinator::open(&same_path)
        .await
        .expect("same Workflow store");
    assert!(
        SqliteTaskWorkflowCurrentGuard::acquire_as(
            &same_tasks,
            &fixture.graph_id,
            &same_workflows,
            &fixture.run_id,
            &fixture.authority,
        )
        .await
        .is_err()
    );
    drop(same_tasks);
    drop(same_workflows);
    remove_database_files(&same_path);

    assert!(
        SqliteTaskCoordinator::open(":memory:").await.is_err(),
        "memory Task stores must fail closed before guard acquisition"
    );
    assert!(
        SqliteWorkflowCoordinator::open(":memory:").await.is_err(),
        "memory Workflow stores must fail closed before guard acquisition"
    );

    let displaced = fixture.task_path.with_extension("displaced.db");
    if std::fs::rename(&fixture.task_path, &displaced).is_ok() {
        std::fs::write(&fixture.task_path, b"replacement").expect("write replacement path");
        let replacement = timeout(
            Duration::from_secs(1),
            SqliteTaskWorkflowCurrentGuard::acquire_as(
                &fixture.tasks,
                &fixture.graph_id,
                &fixture.workflows,
                &fixture.run_id,
                &fixture.authority,
            ),
        )
        .await
        .expect("replacement rejection must be bounded");
        assert!(
            replacement.is_err(),
            "observable replacement must fail closed"
        );
        std::fs::remove_file(&fixture.task_path).expect("remove replacement path");
        std::fs::rename(&displaced, &fixture.task_path).expect("restore fixture path");
    }

    let readme = include_str!("../README.md");
    let quickstart = include_str!("../docs/quickstart.zh-CN.md");
    assert!(readme.contains("immutable namespace"));
    assert!(readme.contains("durable store UUID"));
    assert!(quickstart.contains("不可变命名空间契约"));
    assert!(quickstart.contains("路径 ABA"));
    fixture.remove_files();
}

/// Exact tenant authority used by each fixture.
fn authority(tenant_id: &str) -> AuthorityContext {
    AuthorityContext::new(
        ActorIdentity::Authenticated {
            authority: "sqlite-current-guard-test".to_owned(),
            subject: "guard-test-worker".to_owned(),
        },
        Some(tenant_id.to_owned()),
    )
    .expect("test authority")
}

/// Minimal domain-neutral Task Graph used only for current locking behavior.
fn graph() -> TaskGraph {
    TaskGraph::new(vec![TaskDefinition {
        id: TaskId::from_static("guarded-task"),
        description: "domain-neutral persistence fixture".to_owned(),
        dependencies: BTreeSet::new(),
        priority: 0,
        workspace: WorkspaceMode::None,
        required_capabilities: TaskCapabilitySet::empty(),
    }])
    .expect("test graph")
}

/// Stable Workflow creation request linked to the fixture graph.
fn workflow_request(graph_id: TaskGraphId) -> WorkflowCreateRequest {
    WorkflowCreateRequest {
        command_id: WorkflowCommandId::from_static("create-guarded-run"),
        definition: WorkflowDefinition {
            name: "test.sqlite-current-guard".to_owned(),
            version: Version::new(1, 0, 0),
            content_sha256: "a".repeat(64),
        },
        task_graph_id: graph_id,
    }
}

/// Legal first Workflow mutation used to prove the held writer reservation.
fn wait_command() -> WorkflowCommand {
    WorkflowCommand {
        id: WorkflowCommandId::from_static("wait-under-guard"),
        kind: WorkflowCommandKind::WaitUntil {
            wait_id: WorkflowWaitId::from_static("guarded-wait"),
            due_at_ms: 100,
        },
    }
}

/// Unique on-disk SQLite fixture path.
fn temporary_database_path(label: &str, store: &str) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "y-harness-current-guard-{label}-{store}-{}-{stamp}.db",
        std::process::id()
    ))
}

/// Removes one SQLite database and its WAL sidecars.
fn remove_database_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}
