//! Service-safe Workflow Run reads and lifecycle mutations.

use serde::{Deserialize, Serialize};

use crate::{
    AuthorityContext, HarnessError, WorkflowApplyOutcome, WorkflowCommand, WorkflowCreateRequest,
    WorkflowDefinition, WorkflowEngine, WorkflowRunId, WorkflowRunSnapshot, WorkflowStatus,
    WorkflowTransition,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::now_ms,
};

const MAX_WORKFLOW_TRANSITION_PAGE: usize = 64;
const MAX_WORKFLOW_TRANSITION_PAGE_BYTES: usize = 4_194_304;

/// Bounded current Workflow Run projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRunSummary {
    /// Stable Run identity.
    pub run_id: WorkflowRunId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Current optimistic-concurrency revision.
    pub revision: u64,
    /// Current immutable Workflow implementation.
    pub definition: WorkflowDefinition,
    /// Linked executable Task Graph.
    pub task_graph_id: crate::TaskGraphId,
    /// Current lifecycle projection.
    pub status: WorkflowStatus,
    /// Number of retained immutable transitions.
    pub transition_count: u64,
    /// Conservative durable materialization charge.
    pub materialization_charge_bytes: u64,
}

/// Count- and byte-bounded Workflow transition page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowTransitionPage {
    /// Owning Run.
    pub run_id: WorkflowRunId,
    /// Revision from which the page was read.
    pub revision: u64,
    /// Transitions strictly after the requested sequence.
    pub transitions: Vec<WorkflowTransition>,
    /// Sequence cursor for a later page.
    pub next_after_sequence: Option<u64>,
    /// Whether a later transition exists.
    pub has_more: bool,
}

#[derive(Clone)]
pub(crate) struct WorkflowProtocolService {
    engine: WorkflowEngine,
}

impl WorkflowProtocolService {
    pub(crate) fn new(engine: WorkflowEngine) -> Self {
        Self { engine }
    }

    pub(crate) async fn create(
        &self,
        run_id: WorkflowRunId,
        request: WorkflowCreateRequest,
        authority: &AuthorityContext,
    ) -> Result<WorkflowRunSummary, HarnessError> {
        let snapshot = self
            .engine
            .create_as(run_id, request, now_ms(), authority)
            .await?;
        summary(&snapshot)
    }

    pub(crate) async fn summary(
        &self,
        run_id: &WorkflowRunId,
        authority: &AuthorityContext,
    ) -> Result<Option<WorkflowRunSummary>, HarnessError> {
        self.engine
            .load_as(run_id, authority)
            .await?
            .as_ref()
            .map(summary)
            .transpose()
    }

    pub(crate) async fn transitions(
        &self,
        run_id: &WorkflowRunId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<WorkflowTransitionPage, HarnessError> {
        if !(1..=MAX_WORKFLOW_TRANSITION_PAGE).contains(&limit) {
            return Err(HarnessError::Protocol(format!(
                "Workflow transition limit must be 1-{MAX_WORKFLOW_TRANSITION_PAGE}"
            )));
        }
        let snapshot = self
            .engine
            .load_as(run_id, authority)
            .await?
            .ok_or_else(|| {
                HarnessError::Workflow(format!("Workflow Run {run_id} does not exist"))
            })?;
        let mut transitions = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut has_more = false;
        for transition in snapshot
            .run()
            .transitions()
            .filter(|transition| transition.sequence > after_sequence)
        {
            if transitions.len() == limit {
                has_more = true;
                break;
            }
            let remaining = MAX_WORKFLOW_TRANSITION_PAGE_BYTES.saturating_sub(encoded_bytes);
            let transition_bytes = match bounded_serialized_size(transition, remaining) {
                Ok(bytes) => bytes,
                Err(BoundedJsonError::LimitExceeded) => {
                    if transitions.is_empty() {
                        return Err(HarnessError::Protocol(
                            "one Workflow transition exceeds the protocol response budget"
                                .to_owned(),
                        ));
                    }
                    has_more = true;
                    break;
                }
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Workflow transition page".to_owned(),
                    ));
                }
            };
            encoded_bytes = encoded_bytes.checked_add(transition_bytes).ok_or_else(|| {
                HarnessError::Protocol("Workflow transition page byte count overflow".to_owned())
            })?;
            transitions.push(transition.clone());
        }
        let next_after_sequence = transitions.last().map(|transition| transition.sequence);
        Ok(WorkflowTransitionPage {
            run_id: run_id.clone(),
            revision: snapshot.revision(),
            transitions,
            next_after_sequence,
            has_more,
        })
    }

    pub(crate) async fn apply(
        &self,
        run_id: &WorkflowRunId,
        expected_revision: u64,
        command: WorkflowCommand,
        authority: &AuthorityContext,
    ) -> Result<(WorkflowRunSummary, WorkflowApplyOutcome), HarnessError> {
        let result = self
            .engine
            .apply_as(run_id, expected_revision, command, now_ms(), authority)
            .await?;
        Ok((summary(&result.snapshot)?, result.outcome))
    }
}

fn summary(snapshot: &WorkflowRunSnapshot) -> Result<WorkflowRunSummary, HarnessError> {
    Ok(WorkflowRunSummary {
        run_id: snapshot.id().clone(),
        tenant_id: snapshot.tenant_id().map(str::to_owned),
        revision: snapshot.revision(),
        definition: snapshot.run().definition().clone(),
        task_graph_id: snapshot.run().task_graph_id().clone(),
        status: snapshot.run().status().clone(),
        transition_count: u64::try_from(snapshot.run().transition_count())
            .map_err(|_| HarnessError::Protocol("Workflow transition count overflow".to_owned()))?,
        materialization_charge_bytes: u64::try_from(snapshot.run().materialization_charge_bytes())
            .map_err(|_| HarnessError::Protocol("Workflow materialization overflow".to_owned()))?,
    })
}
