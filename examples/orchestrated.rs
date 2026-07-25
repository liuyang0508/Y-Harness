//! Minimal host that executes a durable Task DAG through public contracts.

use std::{collections::BTreeSet, error::Error, sync::Arc};

use y_harness::{
    CancellationToken, EventId, HarnessError, HarnessFuture, LocalDirectoryWorkspaceProvider,
    MemoryTaskCoordinator, Orchestrator, TaskCompletion, TaskCoordinator, TaskDefinition,
    TaskExecutionRequest, TaskExecutor, TaskGraph, TaskGraphId, TaskId, WorkspaceMode,
};

struct HostTaskExecutor;

impl TaskExecutor for HostTaskExecutor {
    fn execute<'a>(&'a self, request: TaskExecutionRequest) -> HarnessFuture<'a, TaskCompletion> {
        Box::pin(async move {
            let workspace = request.workspace.root().ok_or_else(|| {
                HarnessError::Execution("isolated workspace was not provisioned".to_owned())
            })?;
            if !workspace.is_dir() {
                return Err(HarnessError::Execution(
                    "provisioned workspace is not a directory".to_owned(),
                ));
            }
            if request.claim.task.id.as_str() == "collect" {
                request
                    .mailbox
                    .send(&TaskId::from_static("synthesize"), "collection ready")
                    .await?;
            } else {
                let inbox = request.mailbox.inbox(0, 16).await?;
                if inbox.messages.len() != 1 || inbox.messages[0].body != "collection ready" {
                    return Err(HarnessError::Orchestration(
                        "dependency message was not delivered".to_owned(),
                    ));
                }
            }
            Ok(TaskCompletion {
                summary: format!("completed {}", request.claim.task.id),
                artifacts: Vec::new(),
            })
        })
    }
}

fn task(id: &'static str, dependencies: &[&'static str]) -> TaskDefinition {
    TaskDefinition {
        id: TaskId::from_static(id),
        description: format!("execute {id}"),
        dependencies: dependencies
            .iter()
            .map(|dependency| TaskId::from_static(dependency))
            .collect::<BTreeSet<_>>(),
        priority: 0,
        workspace: WorkspaceMode::Isolated,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let coordinator = Arc::new(MemoryTaskCoordinator::new());
    let graph_id = TaskGraphId::from_static("example-graph");
    coordinator
        .create(
            graph_id.clone(),
            TaskGraph::new(vec![task("collect", &[]), task("synthesize", &["collect"])])?,
        )
        .await?;
    let workspace_root = std::env::temp_dir().join(format!(
        "y-harness-example-workspaces-{}",
        EventId::generate()
    ));
    let workspace_provider = Arc::new(LocalDirectoryWorkspaceProvider::new(&workspace_root)?);
    let orchestrator =
        Orchestrator::new(coordinator, Arc::new(HostTaskExecutor), "example-worker")?
            .with_workspace_provider(workspace_provider)?;
    let result = orchestrator.run(&graph_id, CancellationToken::new()).await;
    let cleanup = std::fs::remove_dir(&workspace_root);
    let snapshot = result?;
    cleanup?;

    if !snapshot.graph().is_terminal() {
        return Err("orchestrated Task Graph did not settle".into());
    }
    println!(
        "graph: {} revision: {} tasks: {}",
        snapshot.id(),
        snapshot.revision(),
        snapshot.graph().tasks().count()
    );
    Ok(())
}
