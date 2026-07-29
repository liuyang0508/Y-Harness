//! Runs one explicit bounded Temporal Driver tick over a durable Workflow wait.

use std::{collections::BTreeSet, sync::Arc};

use semver::Version;
use y_harness::{
    HarnessError, MemoryTaskCoordinator, MemoryWorkflowCoordinator, TaskCoordinator,
    TaskDefinition, TaskGraph, TaskGraphId, TaskId, TemporalDriver, TemporalTickCursor,
    TemporalTickRequest, WorkflowCommand, WorkflowCommandId, WorkflowCommandKind,
    WorkflowCreateRequest, WorkflowDefinition, WorkflowEngine, WorkflowRunId, WorkflowWaitId,
    WorkspaceMode,
};

#[tokio::main]
async fn main() -> Result<(), HarnessError> {
    let tasks = Arc::new(MemoryTaskCoordinator::new());
    let graph_id = TaskGraphId::from_static("temporal-example-graph");
    tasks
        .create(
            graph_id.clone(),
            TaskGraph::new(vec![TaskDefinition {
                id: TaskId::from_static("work"),
                description: "host-owned work remains in the Task Graph".to_owned(),
                dependencies: BTreeSet::new(),
                priority: 0,
                workspace: WorkspaceMode::None,
            }])?,
        )
        .await?;

    let workflows = Arc::new(MemoryWorkflowCoordinator::new());
    let workflow = WorkflowEngine::new(workflows, tasks);
    let run_id = WorkflowRunId::from_static("temporal-example-run");
    workflow
        .create(
            run_id.clone(),
            WorkflowCreateRequest {
                command_id: WorkflowCommandId::from_static("create"),
                definition: WorkflowDefinition {
                    name: "example.temporal".to_owned(),
                    version: Version::new(1, 0, 0),
                    content_sha256: "a".repeat(64),
                },
                task_graph_id: graph_id,
            },
            10,
        )
        .await?;
    workflow
        .apply(
            &run_id,
            1,
            WorkflowCommand {
                id: WorkflowCommandId::from_static("wait"),
                kind: WorkflowCommandKind::WaitUntil {
                    wait_id: WorkflowWaitId::from_static("timer"),
                    due_at_ms: 100,
                },
            },
            20,
        )
        .await?;

    let report = TemporalDriver::new()
        .with_workflow_engine(workflow)
        .tick(TemporalTickRequest {
            at_ms: 100,
            scan_limit: 16,
            cursor: TemporalTickCursor::default(),
        })
        .await?;

    println!(
        "temporal tick: scanned={} due={} attempts={}",
        report.workflows.scanned,
        report.workflows.due,
        report.attempts.len()
    );
    Ok(())
}
