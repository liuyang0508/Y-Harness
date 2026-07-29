//! Bounded host-driven advancement of durable time-owned state.
//!
//! The Temporal Driver owns no authoritative scheduler database and starts no
//! background task. A host supplies trusted time and calls [`TemporalDriver::tick_as`].
//! Workflow and Human Handoff aggregates remain the durable source of truth;
//! their existing revisions and fences settle concurrent ticks.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ActorIdentity, AuthorityContext, HarnessError, HumanHandoffApplyOutcome, HumanHandoffClaimId,
    HumanHandoffCommand, HumanHandoffCommandId, HumanHandoffCommandKind, HumanHandoffDueClaim,
    HumanHandoffDueScanPage, HumanHandoffEngine, HumanHandoffId, WorkflowApplyOutcome,
    WorkflowCommand, WorkflowCommandId, WorkflowCommandKind, WorkflowDueScanPage, WorkflowDueWait,
    WorkflowEngine, WorkflowRunId, WorkflowWaitId,
};

/// Exact embedded Temporal Driver API coordinate.
pub const TEMPORAL_DRIVER_API_VERSION: u32 = 1;
/// Maximum authoritative identities visited per configured source and tick.
pub const MAX_TEMPORAL_SCAN_LIMIT: usize = 256;

const MAX_TEMPORAL_IDENTITY_BYTES: usize = 256;

/// Disposable scan position supplied to a later bounded tick.
///
/// Each source resets independently to `None` after reaching the end of its
/// identity-ordered sweep. Losing this cursor can increase scan latency but
/// cannot lose durable due work.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalTickCursor {
    /// Last visited Workflow Run while that source still has more records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_after_run_id: Option<WorkflowRunId>,
    /// Last visited Human Handoff while that source still has more records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff_after_handoff_id: Option<HumanHandoffId>,
}

/// One bounded deterministic tick request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalTickRequest {
    /// Trusted host server time in Unix milliseconds.
    pub at_ms: u64,
    /// Maximum authoritative aggregates visited per configured source.
    pub scan_limit: usize,
    /// Optional disposable continuation from a prior tick.
    #[serde(default)]
    pub cursor: TemporalTickCursor,
}

/// Bounded scan evidence for one configured temporal source.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalScanProgress {
    /// Number of authoritative aggregates visited.
    pub scanned: usize,
    /// Number of due fences discovered among those aggregates.
    pub due: usize,
    /// Whether the current identity sweep has another page.
    pub has_more: bool,
}

/// Exact durable fence a tick attempted to advance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalTarget {
    /// One due Workflow wait.
    WorkflowWait {
        /// Stable Run identity.
        run_id: WorkflowRunId,
        /// Revision observed with the due wait.
        revision: u64,
        /// Exact wait fence.
        wait_id: WorkflowWaitId,
        /// Inclusive wake boundary.
        due_at_ms: u64,
    },
    /// One expired Human Handoff claim.
    HumanHandoffClaim {
        /// Stable case identity.
        handoff_id: HumanHandoffId,
        /// Revision observed with the claim.
        revision: u64,
        /// Exact claim fence.
        claim_id: HumanHandoffClaimId,
        /// Exclusive expiration boundary.
        expires_at_ms: u64,
    },
}

/// Content-free settlement of one temporal command.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum TemporalAttemptOutcome {
    /// The due transition advanced authoritative state.
    Applied,
    /// The exact deterministic command was already committed.
    Duplicate,
    /// A concurrent mutation changed the aggregate revision first.
    Fenced {
        /// Revision found by the authoritative coordinator.
        actual_revision: u64,
    },
    /// The authoritative source rejected or could not persist the command.
    ///
    /// Coordinator diagnostics are deliberately excluded from this bounded
    /// cross-source report. Hosts should instrument the authoritative
    /// coordinators independently.
    Failed,
}

/// One bounded temporal advancement result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalAttempt {
    /// Exact durable fence observed by the scan.
    pub target: TemporalTarget,
    /// Stable actor-and-fence-derived command identity.
    pub command_id: String,
    /// Content-free settlement.
    pub outcome: TemporalAttemptOutcome,
}

/// Result of one bounded host-driven temporal tick.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalTickReport {
    /// Trusted application time used for discovery and settlement.
    pub at_ms: u64,
    /// Workflow scan evidence, or zeros when that source is not installed.
    pub workflows: TemporalScanProgress,
    /// Human Handoff scan evidence, or zeros when that source is not installed.
    pub human_handoffs: TemporalScanProgress,
    /// Continuation for the next tick; completed source sweeps reset to `None`.
    pub next_cursor: TemporalTickCursor,
    /// Deterministically source-then-identity ordered advancement attempts.
    pub attempts: Vec<TemporalAttempt>,
}

/// Optional composition of durable time-owned subsystems.
#[derive(Clone, Default)]
pub struct TemporalDriver {
    workflows: Option<WorkflowEngine>,
    human_handoffs: Option<HumanHandoffEngine>,
}

impl TemporalDriver {
    /// Creates an empty driver. Sources are installed explicitly.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs due Workflow discovery and fenced wake commands.
    #[must_use]
    pub fn with_workflow_engine(mut self, engine: WorkflowEngine) -> Self {
        self.workflows = Some(engine);
        self
    }

    /// Installs expired Human Handoff discovery and fenced release commands.
    #[must_use]
    pub fn with_human_handoff_engine(mut self, engine: HumanHandoffEngine) -> Self {
        self.human_handoffs = Some(engine);
        self
    }

    /// Runs one bounded tick with unscoped local-process authority.
    pub async fn tick(
        &self,
        request: TemporalTickRequest,
    ) -> Result<TemporalTickReport, HarnessError> {
        self.tick_as(request, &AuthorityContext::local_process())
            .await
    }

    /// Runs one bounded tick inside the exact trusted tenant boundary.
    ///
    /// Both source scans finish before the first mutation. Once mutation
    /// begins, every attempt settles independently so a late failure never
    /// erases evidence of an earlier committed transition.
    pub async fn tick_as(
        &self,
        request: TemporalTickRequest,
        authority: &AuthorityContext,
    ) -> Result<TemporalTickReport, HarnessError> {
        validate_request(&request, authority)?;
        validate_installed_cursors(self, &request.cursor)?;

        let workflow_page = if let Some(engine) = &self.workflows {
            engine
                .scan_due_as(
                    request.at_ms,
                    request.cursor.workflow_after_run_id.as_ref(),
                    request.scan_limit,
                    authority,
                )
                .await?
        } else {
            empty_workflow_page()
        };
        let handoff_page = if let Some(engine) = &self.human_handoffs {
            engine
                .scan_due_as(
                    request.at_ms,
                    request.cursor.handoff_after_handoff_id.as_ref(),
                    request.scan_limit,
                    authority,
                )
                .await?
        } else {
            empty_handoff_page()
        };
        validate_workflow_scan_page(&workflow_page, &request, authority)?;
        validate_handoff_scan_page(&handoff_page, &request, authority)?;

        // Precompute every fallible command identity before the first durable
        // mutation. Later provider failures become per-attempt settlements.
        let workflow_commands = workflow_page
            .due
            .iter()
            .map(|due| {
                temporal_command_id(
                    "workflow-wake",
                    authority.actor(),
                    due.run_id.as_str(),
                    due.wait_id.as_str(),
                )
                .map(|id| {
                    (
                        due.clone(),
                        id.clone(),
                        WorkflowCommand {
                            id: WorkflowCommandId::from_string(id),
                            kind: WorkflowCommandKind::WakeDue {
                                wait_id: due.wait_id.clone(),
                            },
                        },
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let handoff_commands = handoff_page
            .due
            .iter()
            .map(|due| {
                temporal_command_id(
                    "handoff-expire",
                    authority.actor(),
                    due.handoff_id.as_str(),
                    due.claim_id.as_str(),
                )
                .map(|id| {
                    (
                        due.clone(),
                        id.clone(),
                        HumanHandoffCommand {
                            id: HumanHandoffCommandId::from_string(id),
                            kind: HumanHandoffCommandKind::ExpireClaim {
                                claim_id: due.claim_id.clone(),
                            },
                        },
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut attempts = Vec::with_capacity(
            workflow_commands
                .len()
                .saturating_add(handoff_commands.len()),
        );
        if let Some(engine) = &self.workflows {
            for (due, command_id, command) in workflow_commands {
                let outcome = match engine
                    .apply_as(&due.run_id, due.revision, command, request.at_ms, authority)
                    .await
                {
                    Ok(result) => match result.outcome {
                        WorkflowApplyOutcome::Applied => TemporalAttemptOutcome::Applied,
                        WorkflowApplyOutcome::Duplicate => TemporalAttemptOutcome::Duplicate,
                    },
                    Err(HarnessError::WorkflowConflict { actual, .. }) => {
                        TemporalAttemptOutcome::Fenced {
                            actual_revision: actual,
                        }
                    }
                    Err(_) => TemporalAttemptOutcome::Failed,
                };
                attempts.push(TemporalAttempt {
                    target: workflow_target(due),
                    command_id,
                    outcome,
                });
            }
        }
        if let Some(engine) = &self.human_handoffs {
            for (due, command_id, command) in handoff_commands {
                let outcome = match engine
                    .apply_as(
                        &due.handoff_id,
                        due.revision,
                        command,
                        request.at_ms,
                        authority,
                    )
                    .await
                {
                    Ok(result) => match result.outcome {
                        HumanHandoffApplyOutcome::Applied => TemporalAttemptOutcome::Applied,
                        HumanHandoffApplyOutcome::Duplicate => TemporalAttemptOutcome::Duplicate,
                    },
                    Err(HarnessError::HumanHandoffConflict { actual, .. }) => {
                        TemporalAttemptOutcome::Fenced {
                            actual_revision: actual,
                        }
                    }
                    Err(_) => TemporalAttemptOutcome::Failed,
                };
                attempts.push(TemporalAttempt {
                    target: handoff_target(due),
                    command_id,
                    outcome,
                });
            }
        }

        Ok(TemporalTickReport {
            at_ms: request.at_ms,
            workflows: progress(&workflow_page),
            human_handoffs: handoff_progress(&handoff_page),
            next_cursor: TemporalTickCursor {
                workflow_after_run_id: workflow_page
                    .has_more
                    .then_some(workflow_page.next_after_run_id)
                    .flatten(),
                handoff_after_handoff_id: handoff_page
                    .has_more
                    .then_some(handoff_page.next_after_handoff_id)
                    .flatten(),
            },
            attempts,
        })
    }
}

fn validate_request(
    request: &TemporalTickRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Temporal Driver authority")
        .map_err(|error| HarnessError::Temporal(error.to_string()))?;
    if request.at_ms == 0 {
        return Err(HarnessError::Temporal(
            "application time must be positive".to_owned(),
        ));
    }
    if !(1..=MAX_TEMPORAL_SCAN_LIMIT).contains(&request.scan_limit) {
        return Err(HarnessError::Temporal(format!(
            "scan limit must be 1-{MAX_TEMPORAL_SCAN_LIMIT}"
        )));
    }
    if let Some(run_id) = &request.cursor.workflow_after_run_id {
        validate_temporal_identity("Workflow continuation", run_id.as_str())?;
    }
    if let Some(handoff_id) = &request.cursor.handoff_after_handoff_id {
        validate_temporal_identity("Human Handoff continuation", handoff_id.as_str())?;
    }
    Ok(())
}

fn validate_installed_cursors(
    driver: &TemporalDriver,
    cursor: &TemporalTickCursor,
) -> Result<(), HarnessError> {
    if driver.workflows.is_none() && cursor.workflow_after_run_id.is_some() {
        return Err(HarnessError::Temporal(
            "Workflow cursor supplied without a Workflow Engine".to_owned(),
        ));
    }
    if driver.human_handoffs.is_none() && cursor.handoff_after_handoff_id.is_some() {
        return Err(HarnessError::Temporal(
            "Human Handoff cursor supplied without a Human Handoff Engine".to_owned(),
        ));
    }
    Ok(())
}

fn validate_workflow_scan_page(
    page: &WorkflowDueScanPage,
    request: &TemporalTickRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    validate_scan_progress(
        "Workflow",
        page.scanned,
        page.due.len(),
        page.has_more,
        page.next_after_run_id.is_some(),
        request.scan_limit,
    )?;
    if let Some(next) = &page.next_after_run_id {
        validate_temporal_identity("Workflow continuation", next.as_str())?;
        if request
            .cursor
            .workflow_after_run_id
            .as_ref()
            .is_some_and(|after| next.as_str() <= after.as_str())
        {
            return Err(invalid_scan("Workflow", "continuation did not advance"));
        }
    }
    let mut previous = request
        .cursor
        .workflow_after_run_id
        .as_ref()
        .map(WorkflowRunId::as_str);
    for due in &page.due {
        validate_temporal_identity("Workflow Run", due.run_id.as_str())?;
        validate_temporal_identity("Workflow wait", due.wait_id.as_str())?;
        if due.tenant_id.as_deref() != authority.tenant_id() {
            return Err(invalid_scan("Workflow", "tenant projection mismatch"));
        }
        if due.revision == 0 || due.due_at_ms == 0 || due.due_at_ms > request.at_ms {
            return Err(invalid_scan(
                "Workflow",
                "due fence is not currently eligible",
            ));
        }
        if previous.is_some_and(|prior| due.run_id.as_str() <= prior) {
            return Err(invalid_scan("Workflow", "due identities are not ordered"));
        }
        if page
            .next_after_run_id
            .as_ref()
            .is_none_or(|next| due.run_id.as_str() > next.as_str())
        {
            return Err(invalid_scan(
                "Workflow",
                "due identity exceeds the visited page",
            ));
        }
        previous = Some(due.run_id.as_str());
    }
    Ok(())
}

fn validate_handoff_scan_page(
    page: &HumanHandoffDueScanPage,
    request: &TemporalTickRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    validate_scan_progress(
        "Human Handoff",
        page.scanned,
        page.due.len(),
        page.has_more,
        page.next_after_handoff_id.is_some(),
        request.scan_limit,
    )?;
    if let Some(next) = &page.next_after_handoff_id {
        validate_temporal_identity("Human Handoff continuation", next.as_str())?;
        if request
            .cursor
            .handoff_after_handoff_id
            .as_ref()
            .is_some_and(|after| next.as_str() <= after.as_str())
        {
            return Err(invalid_scan(
                "Human Handoff",
                "continuation did not advance",
            ));
        }
    }
    let mut previous = request
        .cursor
        .handoff_after_handoff_id
        .as_ref()
        .map(HumanHandoffId::as_str);
    for due in &page.due {
        validate_temporal_identity("Human Handoff", due.handoff_id.as_str())?;
        validate_temporal_identity("Human Handoff claim", due.claim_id.as_str())?;
        if due.tenant_id.as_deref() != authority.tenant_id() {
            return Err(invalid_scan("Human Handoff", "tenant projection mismatch"));
        }
        if due.revision == 0 || due.expires_at_ms == 0 || due.expires_at_ms > request.at_ms {
            return Err(invalid_scan(
                "Human Handoff",
                "claim is not currently expired",
            ));
        }
        if previous.is_some_and(|prior| due.handoff_id.as_str() <= prior) {
            return Err(invalid_scan(
                "Human Handoff",
                "due identities are not ordered",
            ));
        }
        if page
            .next_after_handoff_id
            .as_ref()
            .is_none_or(|next| due.handoff_id.as_str() > next.as_str())
        {
            return Err(invalid_scan(
                "Human Handoff",
                "due identity exceeds the visited page",
            ));
        }
        previous = Some(due.handoff_id.as_str());
    }
    Ok(())
}

fn validate_scan_progress(
    kind: &str,
    scanned: usize,
    due: usize,
    has_more: bool,
    has_continuation: bool,
    scan_limit: usize,
) -> Result<(), HarnessError> {
    if scanned > scan_limit || due > scanned {
        return Err(invalid_scan(kind, "reported counts exceed the request"));
    }
    if (scanned == 0 && has_continuation) || (scanned != 0 && !has_continuation) {
        return Err(invalid_scan(
            kind,
            "continuation does not match the visited count",
        ));
    }
    if has_more && scanned != scan_limit {
        return Err(invalid_scan(
            kind,
            "partial page cannot report another page",
        ));
    }
    Ok(())
}

fn validate_temporal_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_TEMPORAL_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::Temporal(format!(
            "{kind} identity must be 1-{MAX_TEMPORAL_IDENTITY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn invalid_scan(kind: &str, reason: &str) -> HarnessError {
    HarnessError::Temporal(format!("{kind} temporal scan {reason}"))
}

fn temporal_command_id(
    kind: &str,
    actor: &ActorIdentity,
    aggregate_id: &str,
    fence_id: &str,
) -> Result<String, HarnessError> {
    let encoded = serde_json::to_vec(&(kind, actor, aggregate_id, fence_id)).map_err(|_| {
        HarnessError::Temporal("cannot encode temporal command identity".to_owned())
    })?;
    let digest = Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("temporal-{kind}-{digest}"))
}

fn workflow_target(due: WorkflowDueWait) -> TemporalTarget {
    TemporalTarget::WorkflowWait {
        run_id: due.run_id,
        revision: due.revision,
        wait_id: due.wait_id,
        due_at_ms: due.due_at_ms,
    }
}

fn handoff_target(due: HumanHandoffDueClaim) -> TemporalTarget {
    TemporalTarget::HumanHandoffClaim {
        handoff_id: due.handoff_id,
        revision: due.revision,
        claim_id: due.claim_id,
        expires_at_ms: due.expires_at_ms,
    }
}

fn progress(page: &WorkflowDueScanPage) -> TemporalScanProgress {
    TemporalScanProgress {
        scanned: page.scanned,
        due: page.due.len(),
        has_more: page.has_more,
    }
}

fn handoff_progress(page: &HumanHandoffDueScanPage) -> TemporalScanProgress {
    TemporalScanProgress {
        scanned: page.scanned,
        due: page.due.len(),
        has_more: page.has_more,
    }
}

fn empty_workflow_page() -> WorkflowDueScanPage {
    WorkflowDueScanPage {
        due: Vec::new(),
        next_after_run_id: None,
        has_more: false,
        scanned: 0,
    }
}

fn empty_handoff_page() -> HumanHandoffDueScanPage {
    HumanHandoffDueScanPage {
        due: Vec::new(),
        next_after_handoff_id: None,
        has_more: false,
        scanned: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use semver::Version;

    use super::*;
    use crate::{
        HarnessFuture, HumanHandoffCreateRequest, HumanHandoffStatus, HumanHandoffSubject,
        HumanHandoffSubjectResolver, MemoryHumanHandoffCoordinator, MemoryTaskCoordinator,
        MemoryWorkflowCoordinator, TaskCoordinator, TaskDefinition, TaskGraph, TaskGraphId, TaskId,
        WorkflowCoordinator, WorkflowDefinition, WorkflowStatus, WorkspaceMode,
    };

    struct ExistingSubject;

    impl HumanHandoffSubjectResolver for ExistingSubject {
        fn exists<'a>(
            &'a self,
            _subject: &'a HumanHandoffSubject,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, bool> {
            Box::pin(async { Ok(true) })
        }
    }

    #[derive(Clone, Copy)]
    enum WorkflowScanBehavior {
        FenceAfterScan,
        InvalidCounts,
    }

    struct ControlledWorkflowScan {
        inner: Arc<MemoryWorkflowCoordinator>,
        behavior: WorkflowScanBehavior,
    }

    impl crate::WorkflowCoordinator for ControlledWorkflowScan {
        fn create_as<'a>(
            &'a self,
            run_id: WorkflowRunId,
            request: crate::WorkflowCreateRequest,
            applied_at_ms: u64,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, crate::WorkflowRunSnapshot> {
            self.inner
                .create_as(run_id, request, applied_at_ms, authority)
        }

        fn load_as<'a>(
            &'a self,
            run_id: &'a WorkflowRunId,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, Option<crate::WorkflowRunSnapshot>> {
            self.inner.load_as(run_id, authority)
        }

        fn scan_due_as<'a>(
            &'a self,
            at_ms: u64,
            after_run_id: Option<&'a WorkflowRunId>,
            scan_limit: usize,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, WorkflowDueScanPage> {
            Box::pin(async move {
                let mut page = self
                    .inner
                    .scan_due_as(at_ms, after_run_id, scan_limit, authority)
                    .await?;
                match self.behavior {
                    WorkflowScanBehavior::FenceAfterScan => {
                        if let Some(due) = page.due.first() {
                            self.inner
                                .apply_as(
                                    &due.run_id,
                                    due.revision,
                                    WorkflowCommand {
                                        id: WorkflowCommandId::from_static("concurrent-cancel"),
                                        kind: WorkflowCommandKind::Cancel {
                                            reason: "concurrent owner".to_owned(),
                                        },
                                    },
                                    at_ms,
                                    authority,
                                )
                                .await?;
                        }
                    }
                    WorkflowScanBehavior::InvalidCounts => {
                        page.scanned = scan_limit.saturating_add(1);
                    }
                }
                Ok(page)
            })
        }

        fn apply_as<'a>(
            &'a self,
            run_id: &'a WorkflowRunId,
            expected_revision: u64,
            command: WorkflowCommand,
            applied_at_ms: u64,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, crate::WorkflowCommandResult> {
            self.inner
                .apply_as(run_id, expected_revision, command, applied_at_ms, authority)
        }
    }

    fn authority(subject: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: subject.to_owned(),
            },
            Some("tenant".to_owned()),
        )
        .expect("authority")
    }

    fn task() -> TaskDefinition {
        TaskDefinition {
            id: TaskId::from_static("task"),
            description: "temporal fixture".to_owned(),
            dependencies: BTreeSet::new(),
            priority: 0,
            workspace: WorkspaceMode::None,
        }
    }

    fn workflow_create(graph_id: TaskGraphId) -> crate::WorkflowCreateRequest {
        crate::WorkflowCreateRequest {
            command_id: WorkflowCommandId::from_static("create-workflow"),
            definition: WorkflowDefinition {
                name: "test.temporal".to_owned(),
                version: Version::new(1, 0, 0),
                content_sha256: std::iter::repeat_n('a', 64).collect(),
            },
            task_graph_id: graph_id,
        }
    }

    fn handoff_create() -> HumanHandoffCreateRequest {
        HumanHandoffCreateRequest {
            command_id: HumanHandoffCommandId::from_static("create-handoff"),
            subject: HumanHandoffSubject::Thread {
                thread_id: crate::ThreadId::from_static("thread"),
            },
            queue: "support.primary".to_owned(),
            reason_code: "agent.escalation".to_owned(),
            priority: 7,
        }
    }

    #[tokio::test]
    async fn exact_boundary_tick_advances_workflow_and_handoff_without_side_effect_loop() {
        let authority = authority("worker");
        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph");
        tasks
            .create_as(
                graph_id.clone(),
                TaskGraph::new(vec![task()]).expect("graph"),
                &authority,
            )
            .await
            .expect("create graph");
        let workflows = Arc::new(MemoryWorkflowCoordinator::new());
        let workflow_engine = WorkflowEngine::new(workflows, tasks);
        let run_id = WorkflowRunId::from_static("run");
        workflow_engine
            .create_as(run_id.clone(), workflow_create(graph_id), 10, &authority)
            .await
            .expect("create workflow");
        workflow_engine
            .apply_as(
                &run_id,
                1,
                WorkflowCommand {
                    id: WorkflowCommandId::from_static("wait"),
                    kind: WorkflowCommandKind::WaitUntil {
                        wait_id: WorkflowWaitId::from_static("timer"),
                        due_at_ms: 1_020,
                    },
                },
                20,
                &authority,
            )
            .await
            .expect("wait");

        let handoff_engine = HumanHandoffEngine::new(
            Arc::new(MemoryHumanHandoffCoordinator::new()),
            Arc::new(ExistingSubject),
        );
        let handoff_id = HumanHandoffId::from_static("handoff");
        handoff_engine
            .create_as(handoff_id.clone(), handoff_create(), 10, &authority)
            .await
            .expect("create handoff");
        handoff_engine
            .apply_as(
                &handoff_id,
                1,
                HumanHandoffCommand {
                    id: HumanHandoffCommandId::from_static("claim"),
                    kind: HumanHandoffCommandKind::Claim {
                        claim_id: HumanHandoffClaimId::from_static("claim"),
                        lease_duration_ms: 1_000,
                    },
                },
                20,
                &authority,
            )
            .await
            .expect("claim");

        let driver = TemporalDriver::new()
            .with_workflow_engine(workflow_engine.clone())
            .with_human_handoff_engine(handoff_engine.clone());
        let early = driver
            .tick_as(
                TemporalTickRequest {
                    at_ms: 1_019,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("early tick");
        assert!(early.attempts.is_empty());

        let due = driver
            .tick_as(
                TemporalTickRequest {
                    at_ms: 1_020,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("due tick");
        assert_eq!(due.workflows.due, 1);
        assert_eq!(due.human_handoffs.due, 1);
        assert_eq!(due.attempts.len(), 2);
        assert!(
            due.attempts
                .iter()
                .all(|attempt| attempt.outcome == TemporalAttemptOutcome::Applied)
        );
        assert!(matches!(
            workflow_engine
                .load_as(&run_id, &authority)
                .await
                .expect("load")
                .expect("workflow")
                .run()
                .status(),
            WorkflowStatus::Running
        ));
        assert!(matches!(
            handoff_engine
                .load_as(&handoff_id, &authority)
                .await
                .expect("load")
                .expect("handoff")
                .handoff()
                .status(),
            HumanHandoffStatus::Queued
        ));
    }

    #[tokio::test]
    async fn every_time_owned_workflow_wait_obeys_the_exact_boundary() {
        let authority = authority("worker");
        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-wait-variants");
        tasks
            .create_as(
                graph_id.clone(),
                TaskGraph::new(vec![task()]).expect("graph"),
                &authority,
            )
            .await
            .expect("create graph");
        let workflows = Arc::new(MemoryWorkflowCoordinator::new());
        let engine = WorkflowEngine::new(workflows, tasks);
        let fixtures = [
            (
                "signal-timeout",
                WorkflowCommandKind::WaitForSignal {
                    wait_id: WorkflowWaitId::from_static("signal-timeout-wait"),
                    name: "event.ready".to_owned(),
                    source: "connector".to_owned(),
                    expires_at_ms: Some(100),
                },
            ),
            (
                "timer",
                WorkflowCommandKind::WaitUntil {
                    wait_id: WorkflowWaitId::from_static("timer-wait"),
                    due_at_ms: 100,
                },
            ),
            (
                "retry",
                WorkflowCommandKind::ScheduleRetry {
                    wait_id: WorkflowWaitId::from_static("retry-wait"),
                    activity: "connector.fetch".to_owned(),
                    attempt: 2,
                    due_at_ms: 100,
                    idempotency_key: "effect-1".to_owned(),
                },
            ),
            (
                "signal-open",
                WorkflowCommandKind::WaitForSignal {
                    wait_id: WorkflowWaitId::from_static("signal-open-wait"),
                    name: "event.open".to_owned(),
                    source: "connector".to_owned(),
                    expires_at_ms: None,
                },
            ),
        ];
        for (id, kind) in fixtures {
            let run_id = WorkflowRunId::from_string(id.to_owned());
            engine
                .create_as(
                    run_id.clone(),
                    workflow_create(graph_id.clone()),
                    10,
                    &authority,
                )
                .await
                .expect("create workflow");
            engine
                .apply_as(
                    &run_id,
                    1,
                    WorkflowCommand {
                        id: WorkflowCommandId::from_string(format!("wait-{id}")),
                        kind,
                    },
                    20,
                    &authority,
                )
                .await
                .expect("install wait");
        }

        let driver = TemporalDriver::new().with_workflow_engine(engine.clone());
        let early = driver
            .tick_as(
                TemporalTickRequest {
                    at_ms: 99,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("early tick");
        assert!(early.attempts.is_empty());

        let due = driver
            .tick_as(
                TemporalTickRequest {
                    at_ms: 100,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("boundary tick");
        assert_eq!(due.workflows.due, 3);
        assert_eq!(due.attempts.len(), 3);
        assert!(
            due.attempts
                .iter()
                .all(|attempt| attempt.outcome == TemporalAttemptOutcome::Applied)
        );
        let open = engine
            .load_as(&WorkflowRunId::from_static("signal-open"), &authority)
            .await
            .expect("load")
            .expect("open signal");
        assert!(matches!(
            open.run().status(),
            WorkflowStatus::Waiting { .. }
        ));
    }

    #[test]
    fn command_identity_is_stable_for_one_actor_and_distinct_between_actors() {
        let alice = authority("alice");
        let bob = authority("bob");
        let first =
            temporal_command_id("workflow-wake", alice.actor(), "run", "wait").expect("first");
        let replay =
            temporal_command_id("workflow-wake", alice.actor(), "run", "wait").expect("replay");
        let other =
            temporal_command_id("workflow-wake", bob.actor(), "run", "wait").expect("other");
        assert_eq!(first, replay);
        assert_ne!(first, other);
        assert!(first.len() <= 256);
    }

    #[tokio::test]
    async fn cursor_for_an_uninstalled_source_fails_closed() {
        let error = TemporalDriver::new()
            .tick(TemporalTickRequest {
                at_ms: 10,
                scan_limit: 1,
                cursor: TemporalTickCursor {
                    workflow_after_run_id: Some(WorkflowRunId::from_static("run")),
                    handoff_after_handoff_id: None,
                },
            })
            .await
            .expect_err("uninstalled cursor");
        assert!(error.to_string().contains("without a Workflow Engine"));
    }

    #[tokio::test]
    async fn malformed_custom_scan_page_fails_before_any_mutation() {
        let authority = authority("worker");
        let coordinator: Arc<dyn crate::WorkflowCoordinator> = Arc::new(ControlledWorkflowScan {
            inner: Arc::new(MemoryWorkflowCoordinator::new()),
            behavior: WorkflowScanBehavior::InvalidCounts,
        });
        let engine = WorkflowEngine::new(coordinator, Arc::new(MemoryTaskCoordinator::new()));
        let error = TemporalDriver::new()
            .with_workflow_engine(engine)
            .tick_as(
                TemporalTickRequest {
                    at_ms: 100,
                    scan_limit: 1,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect_err("malformed scan");
        assert!(
            error
                .to_string()
                .contains("reported counts exceed the request")
        );
    }

    #[tokio::test]
    async fn concurrent_mutation_is_reported_as_fenced_without_replaying_state() {
        let authority = authority("worker");
        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-race");
        tasks
            .create_as(
                graph_id.clone(),
                TaskGraph::new(vec![task()]).expect("graph"),
                &authority,
            )
            .await
            .expect("create graph");
        let inner = Arc::new(MemoryWorkflowCoordinator::new());
        let coordinator: Arc<dyn crate::WorkflowCoordinator> = Arc::new(ControlledWorkflowScan {
            inner: inner.clone(),
            behavior: WorkflowScanBehavior::FenceAfterScan,
        });
        let engine = WorkflowEngine::new(coordinator, tasks);
        let run_id = WorkflowRunId::from_static("run-race");
        engine
            .create_as(run_id.clone(), workflow_create(graph_id), 10, &authority)
            .await
            .expect("create workflow");
        engine
            .apply_as(
                &run_id,
                1,
                WorkflowCommand {
                    id: WorkflowCommandId::from_static("wait-race"),
                    kind: WorkflowCommandKind::WaitUntil {
                        wait_id: WorkflowWaitId::from_static("timer-race"),
                        due_at_ms: 100,
                    },
                },
                20,
                &authority,
            )
            .await
            .expect("wait");

        let report = TemporalDriver::new()
            .with_workflow_engine(engine)
            .tick_as(
                TemporalTickRequest {
                    at_ms: 100,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("tick");
        assert_eq!(report.attempts.len(), 1);
        assert_eq!(
            report.attempts[0].outcome,
            TemporalAttemptOutcome::Fenced { actual_revision: 3 }
        );
        let loaded = inner
            .load_as(&run_id, &authority)
            .await
            .expect("load")
            .expect("run");
        assert!(matches!(
            loaded.run().status(),
            WorkflowStatus::Cancelled { .. }
        ));
        assert_eq!(loaded.revision(), 3);
    }
}
