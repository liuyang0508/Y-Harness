//! Bounded host-driven advancement of durable time-owned state.
//!
//! The Temporal Driver owns no authoritative scheduler database and starts no
//! background task. A host supplies trusted time and calls [`TemporalDriver::tick_as`].
//! Workflow, Human Handoff, Effect, and Agent Loop wait aggregates remain the
//! durable source of truth; their existing revisions and fences settle
//! concurrent ticks.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::state::agent_loop_due_command_id;
use crate::{
    ActorIdentity, AgentLoopDueCursor, AgentLoopDuePhase, AgentLoopDueScanPage, AgentLoopDueWait,
    AgentLoopWaitId, AuthorityContext, EffectApplyOutcome, EffectCommand, EffectCommandId,
    EffectCommandKind, EffectDueLease, EffectDueScanPage, EffectEngine, EffectId, EffectLeaseId,
    EventAppendDisposition, HarnessError, HumanHandoffApplyOutcome, HumanHandoffClaimId,
    HumanHandoffCommand, HumanHandoffCommandId, HumanHandoffCommandKind, HumanHandoffDueClaim,
    HumanHandoffDueScanPage, HumanHandoffEngine, HumanHandoffId, StateEngine, ThreadId, TurnId,
    WorkflowApplyOutcome, WorkflowCommand, WorkflowCommandId, WorkflowCommandKind,
    WorkflowDueScanPage, WorkflowDueWait, WorkflowEngine, WorkflowRunId, WorkflowWaitId,
};

/// Exact embedded Temporal Driver API coordinate.
pub const TEMPORAL_DRIVER_API_VERSION: u32 = 3;
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
    /// Last visited Effect while that source still has more records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_after_effect_id: Option<EffectId>,
    /// Last visited Agent Loop due-index coordinate while that source has more.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_loop_wait_after: Option<AgentLoopDueCursor>,
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
    /// One expired external-effect execution lease.
    EffectLease {
        /// Stable Effect identity.
        effect_id: EffectId,
        /// Revision observed with the lease.
        revision: u64,
        /// Exact execution fence.
        lease_id: EffectLeaseId,
        /// Positive attempt owned by the lease.
        attempt: u32,
        /// Exclusive expiration boundary.
        expires_at_ms: u64,
    },
    /// One due non-effecting Agent Loop wait lifecycle.
    AgentLoopWait {
        /// Current lifecycle phase, which selects timeout or denial convergence.
        phase: AgentLoopDuePhase,
        /// Owning Thread.
        thread_id: ThreadId,
        /// Owning running Turn.
        turn_id: TurnId,
        /// Exact wait fence.
        wait_id: AgentLoopWaitId,
        /// Lifecycle revision observed with the due row.
        revision: u64,
        /// State stream version observed atomically with the due row.
        observed_stream_version: u64,
        /// Inclusive deterministic settlement boundary.
        due_at_ms: u64,
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
    /// A concurrent State append changed the owning Thread stream first.
    StreamFenced {
        /// Stream version found by the authoritative Event Store.
        actual_stream_version: u64,
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
    /// Stable command identity used by the authoritative source transition.
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
    /// Effect scan evidence, or zeros when that source is not installed.
    pub effects: TemporalScanProgress,
    /// Agent Loop wait scan evidence, or zeros when State is not installed.
    pub agent_loop_waits: TemporalScanProgress,
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
    effects: Option<EffectEngine>,
    state: Option<StateEngine>,
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

    /// Installs expired Effect-lease discovery and fail-closed unknown settlement.
    #[must_use]
    pub fn with_effect_engine(mut self, engine: EffectEngine) -> Self {
        self.effects = Some(engine);
        self
    }

    /// Installs bounded Agent Loop wait expiry and denial convergence.
    #[must_use]
    pub fn with_state_engine(mut self, engine: StateEngine) -> Self {
        self.state = Some(engine);
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
        let effect_page = if let Some(engine) = &self.effects {
            engine
                .scan_due_as(
                    request.at_ms,
                    request.cursor.effect_after_effect_id.as_ref(),
                    request.scan_limit,
                    authority,
                )
                .await?
        } else {
            empty_effect_page()
        };
        let agent_loop_wait_page = if let Some(state) = &self.state {
            state
                .scan_due_agent_loop_waits_as(
                    request.at_ms,
                    request.cursor.agent_loop_wait_after.as_ref(),
                    request.scan_limit,
                    authority,
                )
                .await?
        } else {
            empty_agent_loop_wait_page()
        };
        validate_workflow_scan_page(&workflow_page, &request, authority)?;
        validate_handoff_scan_page(&handoff_page, &request, authority)?;
        validate_effect_scan_page(&effect_page, &request, authority)?;
        validate_agent_loop_wait_scan_page(&agent_loop_wait_page, &request, authority)?;

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
        let effect_commands = effect_page
            .due
            .iter()
            .map(|due| {
                temporal_command_id(
                    "effect-expire",
                    authority.actor(),
                    due.effect_id.as_str(),
                    due.lease_id.as_str(),
                )
                .map(|id| {
                    (
                        due.clone(),
                        id.clone(),
                        EffectCommand {
                            id: EffectCommandId::from_string(id),
                            kind: EffectCommandKind::ExpireLease {
                                lease_id: due.lease_id.clone(),
                            },
                        },
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let agent_loop_wait_commands = agent_loop_wait_page
            .due
            .iter()
            .map(|due| agent_loop_due_command_id(due).map(|id| (due.clone(), id)))
            .collect::<Result<Vec<_>, _>>()?;

        let mut attempts = Vec::with_capacity(
            workflow_commands
                .len()
                .saturating_add(handoff_commands.len())
                .saturating_add(effect_commands.len())
                .saturating_add(agent_loop_wait_commands.len()),
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
        if let Some(engine) = &self.effects {
            for (due, command_id, command) in effect_commands {
                let outcome = match engine
                    .apply_as(
                        &due.effect_id,
                        due.revision,
                        command,
                        request.at_ms,
                        authority,
                    )
                    .await
                {
                    Ok(result) => match result.outcome {
                        EffectApplyOutcome::Applied => TemporalAttemptOutcome::Applied,
                        EffectApplyOutcome::Duplicate => TemporalAttemptOutcome::Duplicate,
                    },
                    Err(HarnessError::EffectConflict { actual, .. }) => {
                        TemporalAttemptOutcome::Fenced {
                            actual_revision: actual,
                        }
                    }
                    Err(_) => TemporalAttemptOutcome::Failed,
                };
                attempts.push(TemporalAttempt {
                    target: effect_target(due),
                    command_id,
                    outcome,
                });
            }
        }
        if let Some(state) = &self.state {
            for (due, command_id) in agent_loop_wait_commands {
                let outcome = match state
                    .settle_due_agent_loop_wait_as(&due, request.at_ms, authority)
                    .await
                {
                    Ok(result) => match result.disposition {
                        EventAppendDisposition::Applied => TemporalAttemptOutcome::Applied,
                        EventAppendDisposition::Duplicate => TemporalAttemptOutcome::Duplicate,
                        EventAppendDisposition::Unknown => TemporalAttemptOutcome::Failed,
                    },
                    Err(HarnessError::StateConflict { actual, .. }) => {
                        TemporalAttemptOutcome::StreamFenced {
                            actual_stream_version: actual,
                        }
                    }
                    Err(_) => TemporalAttemptOutcome::Failed,
                };
                attempts.push(TemporalAttempt {
                    target: agent_loop_wait_target(due),
                    command_id,
                    outcome,
                });
            }
        }

        Ok(TemporalTickReport {
            at_ms: request.at_ms,
            workflows: progress(&workflow_page),
            human_handoffs: handoff_progress(&handoff_page),
            effects: effect_progress(&effect_page),
            agent_loop_waits: agent_loop_wait_progress(&agent_loop_wait_page),
            next_cursor: TemporalTickCursor {
                workflow_after_run_id: workflow_page
                    .has_more
                    .then_some(workflow_page.next_after_run_id)
                    .flatten(),
                handoff_after_handoff_id: handoff_page
                    .has_more
                    .then_some(handoff_page.next_after_handoff_id)
                    .flatten(),
                effect_after_effect_id: effect_page
                    .has_more
                    .then_some(effect_page.next_after_effect_id)
                    .flatten(),
                agent_loop_wait_after: agent_loop_wait_page
                    .has_more
                    .then_some(agent_loop_wait_page.next_cursor)
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
    if let Some(effect_id) = &request.cursor.effect_after_effect_id {
        validate_temporal_identity("Effect continuation", effect_id.as_str())?;
    }
    if let Some(after) = &request.cursor.agent_loop_wait_after {
        validate_agent_loop_wait_cursor(after)?;
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
    if driver.effects.is_none() && cursor.effect_after_effect_id.is_some() {
        return Err(HarnessError::Temporal(
            "Effect cursor supplied without an Effect Engine".to_owned(),
        ));
    }
    if driver.state.is_none() && cursor.agent_loop_wait_after.is_some() {
        return Err(HarnessError::Temporal(
            "Agent Loop wait cursor supplied without a State Engine".to_owned(),
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

fn validate_effect_scan_page(
    page: &EffectDueScanPage,
    request: &TemporalTickRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    validate_scan_progress(
        "Effect",
        page.scanned,
        page.due.len(),
        page.has_more,
        page.next_after_effect_id.is_some(),
        request.scan_limit,
    )?;
    if let Some(next) = &page.next_after_effect_id {
        validate_temporal_identity("Effect continuation", next.as_str())?;
        if request
            .cursor
            .effect_after_effect_id
            .as_ref()
            .is_some_and(|after| next.as_str() <= after.as_str())
        {
            return Err(invalid_scan("Effect", "continuation did not advance"));
        }
    }
    let mut previous = request
        .cursor
        .effect_after_effect_id
        .as_ref()
        .map(EffectId::as_str);
    for due in &page.due {
        validate_temporal_identity("Effect", due.effect_id.as_str())?;
        validate_temporal_identity("Effect lease", due.lease_id.as_str())?;
        if due.tenant_id.as_deref() != authority.tenant_id() {
            return Err(invalid_scan("Effect", "tenant projection mismatch"));
        }
        if due.revision == 0
            || due.attempt == 0
            || due.expires_at_ms == 0
            || due.expires_at_ms > request.at_ms
        {
            return Err(invalid_scan("Effect", "lease is not currently expired"));
        }
        if previous.is_some_and(|prior| due.effect_id.as_str() <= prior) {
            return Err(invalid_scan("Effect", "due identities are not ordered"));
        }
        if page
            .next_after_effect_id
            .as_ref()
            .is_none_or(|next| due.effect_id.as_str() > next.as_str())
        {
            return Err(invalid_scan(
                "Effect",
                "due identity exceeds the visited page",
            ));
        }
        previous = Some(due.effect_id.as_str());
    }
    Ok(())
}

fn validate_agent_loop_wait_scan_page(
    page: &AgentLoopDueScanPage,
    request: &TemporalTickRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    page.validate(
        request.at_ms,
        request.cursor.agent_loop_wait_after.as_ref(),
        request.scan_limit,
        authority.tenant_id(),
    )
    .map_err(|error| invalid_scan("Agent Loop wait", &error.to_string()))
}

fn validate_agent_loop_wait_cursor(cursor: &AgentLoopDueCursor) -> Result<(), HarnessError> {
    if cursor.due_at_ms == 0 {
        return Err(invalid_scan(
            "Agent Loop wait",
            "cursor due time must be positive",
        ));
    }
    validate_temporal_identity("Agent Loop Thread continuation", cursor.thread_id.as_str())?;
    validate_temporal_identity("Agent Loop Turn continuation", cursor.turn_id.as_str())?;
    validate_temporal_identity("Agent Loop wait continuation", cursor.wait_id.as_str())
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

fn effect_target(due: EffectDueLease) -> TemporalTarget {
    TemporalTarget::EffectLease {
        effect_id: due.effect_id,
        revision: due.revision,
        lease_id: due.lease_id,
        attempt: due.attempt,
        expires_at_ms: due.expires_at_ms,
    }
}

fn agent_loop_wait_target(due: AgentLoopDueWait) -> TemporalTarget {
    TemporalTarget::AgentLoopWait {
        phase: due.phase,
        thread_id: due.thread_id,
        turn_id: due.turn_id,
        wait_id: due.wait_id,
        revision: due.revision,
        observed_stream_version: due.expected_stream_version,
        due_at_ms: due.due_at_ms,
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

fn effect_progress(page: &EffectDueScanPage) -> TemporalScanProgress {
    TemporalScanProgress {
        scanned: page.scanned,
        due: page.due.len(),
        has_more: page.has_more,
    }
}

fn agent_loop_wait_progress(page: &AgentLoopDueScanPage) -> TemporalScanProgress {
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

fn empty_effect_page() -> EffectDueScanPage {
    EffectDueScanPage {
        due: Vec::new(),
        next_after_effect_id: None,
        has_more: false,
        scanned: 0,
    }
}

fn empty_agent_loop_wait_page() -> AgentLoopDueScanPage {
    AgentLoopDueScanPage {
        due: Vec::new(),
        next_cursor: None,
        has_more: false,
        scanned: 0,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc, time::Duration};

    use semver::Version;

    use super::*;
    use crate::{
        AgentLoopResumeCommandId, AgentLoopWaitStartCommand, ApprovalDecision, ApprovalId,
        ApprovalRecord, ApprovalRecordStatus, ApprovalRequest, CapabilityOrigin,
        CompletionAssurance, CompletionGeneration, EffectCreateRequest, EffectOperation,
        EffectStatus, HarnessFuture, HumanHandoffCreateRequest, HumanHandoffStatus,
        HumanHandoffSubject, HumanHandoffSubjectResolver, Item, ItemKind, MemoryEffectCoordinator,
        MemoryEventStore, MemoryHumanHandoffCoordinator, MemoryTaskCoordinator,
        MemoryWorkflowCoordinator, PolicyDecision, RiskLevel, TaskCoordinator, TaskDefinition,
        TaskGraph, TaskGraphId, TaskId, ToolAuthorization, ToolDescriptor, Turn, TurnStatus,
        WorkflowCoordinator, WorkflowDefinition, WorkflowStatus, WorkspaceMode,
        completion_model_request_sha256, completion_model_route_sha256,
        completion_runtime_governance_sha256, completion_tool_view_sha256,
        completion_verifier_manifest_sha256,
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

    #[derive(Clone, Copy)]
    enum AgentLoopApplyMutation {
        Settle,
        Append,
    }

    struct AgentLoopMutatingWorkflow {
        inner: Arc<MemoryWorkflowCoordinator>,
        state: StateEngine,
        due: AgentLoopDueWait,
        turn: Turn,
        mutation: AgentLoopApplyMutation,
    }

    impl WorkflowCoordinator for AgentLoopMutatingWorkflow {
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
            self.inner
                .scan_due_as(at_ms, after_run_id, scan_limit, authority)
        }

        fn apply_as<'a>(
            &'a self,
            run_id: &'a WorkflowRunId,
            expected_revision: u64,
            command: WorkflowCommand,
            applied_at_ms: u64,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, crate::WorkflowCommandResult> {
            Box::pin(async move {
                if matches!(&command.kind, WorkflowCommandKind::WakeDue { .. }) {
                    match self.mutation {
                        AgentLoopApplyMutation::Settle => {
                            self.state
                                .settle_due_agent_loop_wait_as(&self.due, applied_at_ms, authority)
                                .await?;
                        }
                        AgentLoopApplyMutation::Append => {
                            self.state
                                .append_item_as(
                                    &self.turn,
                                    Item::new(ItemKind::UserMessage {
                                        content: "concurrent State mutation".to_owned(),
                                    }),
                                    authority,
                                )
                                .await?;
                        }
                    }
                }
                self.inner
                    .apply_as(run_id, expected_revision, command, applied_at_ms, authority)
                    .await
            })
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
            required_capabilities: Default::default(),
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

    async fn agent_loop_wait_fixture(
        state: &StateEngine,
        authority: &AuthorityContext,
        suffix: &str,
    ) -> (Turn, ApprovalRequest, AgentLoopWaitId) {
        let thread = state
            .create_thread_as(authority)
            .await
            .expect("create Agent Loop wait Thread");
        let turn = state
            .start_turn_as(&thread.id, authority)
            .await
            .expect("start Agent Loop wait Turn");
        let call_id = format!("temporal-call-{suffix}");
        let descriptor = ToolDescriptor {
            name: "write_record".to_owned(),
            description: "Writes one bounded test record".to_owned(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"value": {"type": "integer"}},
                "required": ["value"]
            }),
        };
        let input = serde_json::json!({"value": 7});
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::ToolCall {
                    model_id: Some("test/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: call_id.clone(),
                    name: descriptor.name.clone(),
                    input: input.clone(),
                    batch: None,
                }),
                authority,
            )
            .await
            .expect("append Agent Loop ToolCall");
        state
            .append_item_as(
                &turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: call_id.clone(),
                    tool_origin: Some(CapabilityOrigin::BuiltIn),
                    decision: PolicyDecision::Ask {
                        reason: "operator confirmation required".to_owned(),
                        risk: RiskLevel::High,
                    },
                }),
                authority,
            )
            .await
            .expect("append Agent Loop Policy decision");
        let request = ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: authority.actor().clone(),
            authorization: ToolAuthorization {
                thread_id: thread.id,
                turn_id: turn.id.clone(),
                call_id,
                descriptor,
                origin: CapabilityOrigin::BuiltIn,
                input,
            },
            reason: "operator confirmation required".to_owned(),
            risk: RiskLevel::High,
        };
        let model_request_sha256 = completion_model_request_sha256(&serde_json::json!({
            "turn": turn.id.as_str(),
            "approval": request.id.as_str()
        }))
        .expect("Model request digest");
        let generation = CompletionGeneration::new(
            &model_request_sha256,
            completion_model_route_sha256(&["test/model"]).expect("Model route digest"),
            completion_tool_view_sha256(&Vec::<String>::new()).expect("Tool view digest"),
            completion_verifier_manifest_sha256(&[]).expect("Verifier manifest digest"),
            completion_runtime_governance_sha256(&serde_json::json!({"max_steps": 16}))
                .expect("Runtime governance digest"),
            None,
            CompletionAssurance::RuntimeMeasured,
        )
        .expect("completion generation");
        let wait_id = AgentLoopWaitId::from_string(format!("temporal-wait-{suffix}"));
        state
            .start_approval_wait_as(
                &turn,
                AgentLoopWaitStartCommand::new(
                    wait_id.clone(),
                    request.clone(),
                    generation,
                    Some(Duration::from_secs(60)),
                    Some(30_000),
                ),
                authority,
            )
            .await
            .expect("start durable Agent Loop wait");
        (turn, request, wait_id)
    }

    fn denied_approval(request: ApprovalRequest, tenant_id: Option<String>) -> ApprovalRecord {
        let settled_at_ms = crate::kernel::now_ms();
        ApprovalRecord {
            schema_version: crate::APPROVAL_INBOX_SCHEMA_VERSION,
            request,
            tenant_id,
            status: ApprovalRecordStatus::Settled {
                decision: ApprovalDecision::Deny {
                    reason: "operator rejected".to_owned(),
                },
                decided_by: ActorIdentity::Authenticated {
                    authority: "test-approver".to_owned(),
                    subject: "operator-7".to_owned(),
                },
            },
            revision: 2,
            requested_at_ms: settled_at_ms.saturating_sub(1),
            settled_at_ms: Some(settled_at_ms),
        }
    }

    #[test]
    fn temporal_api_three_names_the_agent_loop_source() {
        assert_eq!(TEMPORAL_DRIVER_API_VERSION, 3);
    }

    #[tokio::test]
    async fn agent_loop_wait_tick_times_out_the_exact_due_fence() {
        let authority = authority("maintenance");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let (turn, _, wait_id) = agent_loop_wait_fixture(&state, &authority, "timeout").await;
        let report = TemporalDriver::new()
            .with_state_engine(state.clone())
            .tick_as(
                TemporalTickRequest {
                    at_ms: u64::MAX,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("settle due Agent Loop wait");

        assert_eq!(report.agent_loop_waits.scanned, 1);
        assert_eq!(report.agent_loop_waits.due, 1);
        assert_eq!(report.attempts.len(), 1);
        assert_eq!(report.attempts[0].outcome, TemporalAttemptOutcome::Applied);
        assert!(
            report.attempts[0]
                .command_id
                .starts_with("agent-loop-timeout-")
        );
        assert!(matches!(
            &report.attempts[0].target,
            TemporalTarget::AgentLoopWait {
                phase: AgentLoopDuePhase::Waiting,
                wait_id: observed,
                ..
            } if observed == &wait_id
        ));
        let thread = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load timed-out Thread")
            .expect("timed-out Thread");
        assert_eq!(thread.turns[0].status, TurnStatus::TimedOut);
    }

    #[tokio::test]
    async fn ready_denial_tick_preserves_denial_instead_of_timing_out() {
        let authority = authority("maintenance");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let (turn, request, wait_id) = agent_loop_wait_fixture(&state, &authority, "deny").await;
        state
            .accept_resume_as(
                &turn,
                &wait_id,
                1,
                AgentLoopResumeCommandId::from_static("temporal-denial-resume"),
                &denied_approval(request, authority.tenant_id().map(str::to_owned)),
                &authority,
            )
            .await
            .expect("accept denial");

        let report = TemporalDriver::new()
            .with_state_engine(state.clone())
            .tick_as(
                TemporalTickRequest {
                    at_ms: u64::MAX,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("converge accepted denial");
        assert!(matches!(
            &report.attempts[0].target,
            TemporalTarget::AgentLoopWait {
                phase: AgentLoopDuePhase::ReadyDeny,
                wait_id: observed,
                ..
            } if observed == &wait_id
        ));
        assert_eq!(report.attempts[0].outcome, TemporalAttemptOutcome::Applied);
        assert!(
            report.attempts[0]
                .command_id
                .starts_with("agent-loop-denial-")
        );
        let thread = state
            .load_thread_as(&turn.thread_id, &authority)
            .await
            .expect("load denied Thread")
            .expect("denied Thread");
        assert_eq!(thread.turns[0].status, TurnStatus::Failed);
        assert!(
            thread.turns[0]
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::AgentLoopWaitDenied { .. }))
        );
        assert!(
            thread.turns[0]
                .items
                .iter()
                .all(|item| !matches!(item.kind, ItemKind::AgentLoopWaitClosed { .. }))
        );
    }

    async fn agent_loop_attempt_after_workflow_mutation(
        mutation: AgentLoopApplyMutation,
    ) -> TemporalTickReport {
        let authority = authority("maintenance");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let (turn, _, _) = agent_loop_wait_fixture(&state, &authority, "race").await;
        let due = state
            .scan_due_agent_loop_waits_as(u64::MAX, None, 16, &authority)
            .await
            .expect("scan Agent Loop fixture")
            .due
            .into_iter()
            .next()
            .expect("due Agent Loop wait");

        let tasks = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("agent-loop-race-graph");
        tasks
            .create_as(
                graph_id.clone(),
                TaskGraph::new(vec![task()]).expect("graph"),
                &authority,
            )
            .await
            .expect("create graph");
        let coordinator: Arc<dyn WorkflowCoordinator> = Arc::new(AgentLoopMutatingWorkflow {
            inner: Arc::new(MemoryWorkflowCoordinator::new()),
            state: state.clone(),
            due,
            turn,
            mutation,
        });
        let workflow_engine = WorkflowEngine::new(coordinator, tasks);
        let run_id = WorkflowRunId::from_static("agent-loop-race-run");
        workflow_engine
            .create_as(run_id.clone(), workflow_create(graph_id), 10, &authority)
            .await
            .expect("create Workflow");
        workflow_engine
            .apply_as(
                &run_id,
                1,
                WorkflowCommand {
                    id: WorkflowCommandId::from_static("agent-loop-race-wait"),
                    kind: WorkflowCommandKind::WaitUntil {
                        wait_id: WorkflowWaitId::from_static("agent-loop-race-timer"),
                        due_at_ms: 100,
                    },
                },
                20,
                &authority,
            )
            .await
            .expect("install Workflow wait");

        TemporalDriver::new()
            .with_workflow_engine(workflow_engine)
            .with_state_engine(state)
            .tick_as(
                TemporalTickRequest {
                    at_ms: u64::MAX,
                    scan_limit: 16,
                    cursor: TemporalTickCursor::default(),
                },
                &authority,
            )
            .await
            .expect("tick after all source scans")
    }

    #[tokio::test]
    async fn agent_loop_disposition_and_conflict_map_exactly_after_all_scans() {
        let duplicate =
            agent_loop_attempt_after_workflow_mutation(AgentLoopApplyMutation::Settle).await;
        assert_eq!(duplicate.attempts.len(), 2);
        assert_eq!(
            duplicate.attempts[0].outcome,
            TemporalAttemptOutcome::Applied
        );
        assert!(matches!(
            duplicate.attempts[1].target,
            TemporalTarget::AgentLoopWait { .. }
        ));
        assert_eq!(
            duplicate.attempts[1].outcome,
            TemporalAttemptOutcome::Duplicate
        );

        let fenced =
            agent_loop_attempt_after_workflow_mutation(AgentLoopApplyMutation::Append).await;
        assert_eq!(fenced.attempts.len(), 2);
        assert_eq!(
            fenced.attempts[1].outcome,
            TemporalAttemptOutcome::StreamFenced {
                actual_stream_version: 6
            }
        );
    }

    #[tokio::test]
    async fn exact_boundary_tick_advances_workflow_handoff_and_effect_fences() {
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

        let effect_engine = EffectEngine::new(Arc::new(MemoryEffectCoordinator::new()));
        let effect_id = EffectId::from_static("effect");
        effect_engine
            .create_as(
                effect_id.clone(),
                EffectCreateRequest {
                    command_id: EffectCommandId::from_static("create-effect"),
                    operation: EffectOperation {
                        capability: "channel.email".to_owned(),
                        operation: "send".to_owned(),
                    },
                    idempotency_key: "message".to_owned(),
                    input: serde_json::json!({"artifact_ref":"message"}),
                    not_before_ms: 10,
                },
                10,
                &authority,
            )
            .await
            .expect("create effect");
        effect_engine
            .apply_as(
                &effect_id,
                1,
                EffectCommand {
                    id: EffectCommandId::from_static("claim-effect"),
                    kind: EffectCommandKind::Claim {
                        lease_id: EffectLeaseId::from_static("effect-lease"),
                        lease_duration_ms: 1_000,
                    },
                },
                20,
                &authority,
            )
            .await
            .expect("claim effect");

        let driver = TemporalDriver::new()
            .with_workflow_engine(workflow_engine.clone())
            .with_human_handoff_engine(handoff_engine.clone())
            .with_effect_engine(effect_engine.clone());
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
        assert_eq!(due.effects.due, 1);
        assert_eq!(due.attempts.len(), 3);
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
        assert!(matches!(
            effect_engine
                .load_as(&effect_id, &authority)
                .await
                .expect("load")
                .expect("effect")
                .effect()
                .status(),
            EffectStatus::Unknown { .. }
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
                    effect_after_effect_id: None,
                    agent_loop_wait_after: None,
                },
            })
            .await
            .expect_err("uninstalled cursor");
        assert!(error.to_string().contains("without a Workflow Engine"));

        let error = TemporalDriver::new()
            .tick(TemporalTickRequest {
                at_ms: 10,
                scan_limit: 1,
                cursor: TemporalTickCursor {
                    workflow_after_run_id: None,
                    handoff_after_handoff_id: None,
                    effect_after_effect_id: None,
                    agent_loop_wait_after: Some(AgentLoopDueCursor {
                        due_at_ms: 1,
                        thread_id: ThreadId::from_static("thread"),
                        turn_id: TurnId::from_static("turn"),
                        wait_id: AgentLoopWaitId::from_static("wait"),
                    }),
                },
            })
            .await
            .expect_err("uninstalled State cursor");
        assert!(error.to_string().contains("without a State Engine"));
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
