//! Composition boundary between Workflow time/event state and Task execution state.

use std::sync::Arc;

use crate::{
    AuthorityContext, HarnessError, TaskCoordinator, TaskStatus, WorkflowCommand,
    WorkflowCommandKind, WorkflowCoordinator, WorkflowCreateRequest, WorkflowRunId,
    WorkflowRunSnapshot,
};

use super::WorkflowCommandResult;

/// Governed Workflow lifecycle service above one Task Coordinator.
///
/// The Workflow Coordinator owns cross-time state. The Task Coordinator
/// remains authoritative for executable DAG state. This service verifies that
/// a Run links an existing same-tenant Task Graph and refuses successful
/// Workflow completion until every linked Task is durably complete.
#[derive(Clone)]
pub struct WorkflowEngine {
    workflows: Arc<dyn WorkflowCoordinator>,
    tasks: Arc<dyn TaskCoordinator>,
}

impl WorkflowEngine {
    /// Composes one Workflow persistence port with one Task authority.
    #[must_use]
    pub fn new(workflows: Arc<dyn WorkflowCoordinator>, tasks: Arc<dyn TaskCoordinator>) -> Self {
        Self { workflows, tasks }
    }

    /// Creates one unscoped Run linked to an existing unscoped Task Graph.
    pub async fn create(
        &self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
    ) -> Result<WorkflowRunSnapshot, HarnessError> {
        self.create_as(
            run_id,
            request,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Creates one Run only when its linked Task Graph exists in the exact
    /// trusted tenant boundary.
    pub async fn create_as(
        &self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<WorkflowRunSnapshot, HarnessError> {
        if self
            .tasks
            .load_as(&request.task_graph_id, authority)
            .await?
            .is_none()
        {
            return Err(HarnessError::Workflow(format!(
                "Workflow Task Graph {} does not exist in the authority boundary",
                request.task_graph_id
            )));
        }
        self.workflows
            .create_as(run_id, request, applied_at_ms, authority)
            .await
    }

    /// Loads one unscoped Run.
    pub async fn load(
        &self,
        run_id: &WorkflowRunId,
    ) -> Result<Option<WorkflowRunSnapshot>, HarnessError> {
        self.load_as(run_id, &AuthorityContext::local_process())
            .await
    }

    /// Loads one Run only inside the exact trusted tenant boundary.
    pub async fn load_as(
        &self,
        run_id: &WorkflowRunId,
        authority: &AuthorityContext,
    ) -> Result<Option<WorkflowRunSnapshot>, HarnessError> {
        self.workflows.load_as(run_id, authority).await
    }

    /// Applies one command to an unscoped Run.
    pub async fn apply(
        &self,
        run_id: &WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
    ) -> Result<WorkflowCommandResult, HarnessError> {
        self.apply_as(
            run_id,
            expected_revision,
            command,
            applied_at_ms,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Applies one command with tenant fencing and completion evidence checks.
    pub async fn apply_as(
        &self,
        run_id: &WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<WorkflowCommandResult, HarnessError> {
        if matches!(command.kind, WorkflowCommandKind::Complete { .. }) {
            let run = self
                .workflows
                .load_as(run_id, authority)
                .await?
                .ok_or_else(|| {
                    HarnessError::Workflow(format!("Workflow Run {run_id} does not exist"))
                })?;
            if run.run().recognizes_command(&command)? {
                return self
                    .workflows
                    .apply_as(run_id, expected_revision, command, applied_at_ms, authority)
                    .await;
            }
            let graph_id = run.run().task_graph_id();
            let graph = self
                .tasks
                .load_as(graph_id, authority)
                .await?
                .ok_or_else(|| {
                    HarnessError::Workflow(format!(
                        "Workflow Task Graph {graph_id} is no longer available"
                    ))
                })?;
            if !graph
                .graph()
                .tasks()
                .all(|record| matches!(record.status, TaskStatus::Completed { .. }))
            {
                return Err(HarnessError::Workflow(
                    "Workflow cannot complete before every linked Task completes".to_owned(),
                ));
            }
        }
        self.workflows
            .apply_as(run_id, expected_revision, command, applied_at_ms, authority)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use semver::Version;

    use super::*;
    use crate::{
        MemoryTaskCoordinator, MemoryWorkflowCoordinator, TaskCompletion, TaskDefinition,
        TaskGraph, TaskGraphId, TaskId, WorkflowCommandId, WorkflowDefinition, WorkflowStatus,
        WorkspaceMode,
    };

    fn definition() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "test.workflow".to_owned(),
            version: Version::new(1, 0, 0),
            content_sha256: std::iter::repeat_n('a', 64).collect(),
        }
    }

    fn task() -> TaskDefinition {
        TaskDefinition {
            id: TaskId::from_static("task"),
            description: "work".to_owned(),
            dependencies: BTreeSet::new(),
            priority: 0,
            workspace: WorkspaceMode::None,
        }
    }

    fn create(graph_id: TaskGraphId) -> WorkflowCreateRequest {
        WorkflowCreateRequest {
            command_id: WorkflowCommandId::from_static("create"),
            definition: definition(),
            task_graph_id: graph_id,
        }
    }

    #[tokio::test]
    async fn creation_requires_existing_task_graph() {
        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let engine = WorkflowEngine::new(Arc::new(MemoryWorkflowCoordinator::new()), tasks);
        let error = engine
            .create(
                WorkflowRunId::from_static("run"),
                create(TaskGraphId::from_static("missing")),
                10,
            )
            .await
            .expect_err("missing graph");
        assert!(error.to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn successful_completion_requires_every_task_complete() {
        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph");
        let task_graph = TaskGraph::new(vec![task()]).expect("graph");
        tasks
            .create(graph_id.clone(), task_graph)
            .await
            .expect("create graph");
        let engine = WorkflowEngine::new(Arc::new(MemoryWorkflowCoordinator::new()), tasks.clone());
        let run_id = WorkflowRunId::from_static("run");
        engine
            .create(run_id.clone(), create(graph_id.clone()), 10)
            .await
            .expect("create run");
        let completion = WorkflowCommand {
            id: WorkflowCommandId::from_static("complete"),
            kind: WorkflowCommandKind::Complete {
                summary: "done".to_owned(),
            },
        };
        let error = engine
            .apply(&run_id, 1, completion.clone(), 20)
            .await
            .expect_err("unfinished graph");
        assert!(error.to_string().contains("every linked Task"));

        let mut graph = tasks
            .load(&graph_id)
            .await
            .expect("load graph")
            .expect("graph");
        let claim = graph
            .graph_mut()
            .claim_ready("worker", 20, 100, 1)
            .expect("claim")
            .pop()
            .expect("task");
        graph
            .graph_mut()
            .complete(
                &claim.task.id,
                &claim.lease.id,
                30,
                TaskCompletion {
                    summary: "done".to_owned(),
                    artifacts: Vec::new(),
                },
            )
            .expect("complete task");
        tasks.compare_and_swap(graph).await.expect("save graph");

        let result = engine
            .apply(&run_id, 1, completion.clone(), 40)
            .await
            .expect("complete run");
        assert!(matches!(
            result.snapshot.run().status(),
            WorkflowStatus::Completed { .. }
        ));
        let replay = engine
            .apply(&run_id, 1, completion, 50)
            .await
            .expect("replay completed command");
        assert_eq!(replay.outcome, crate::WorkflowApplyOutcome::Duplicate);
        assert_eq!(replay.snapshot.revision(), 2);
    }
}
