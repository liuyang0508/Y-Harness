//! Durable, domain-neutral Workflow lifecycle above executable Task Graphs.

mod coordinator;
mod engine;

use semver::Version;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use sha2::{Digest, Sha256};

use crate::{
    ActorIdentity, AuthorityContext, HarnessError, TaskGraphId, WorkflowCommandId,
    WorkflowSignalId, WorkflowWaitId, kernel::validate_capability_name,
};

pub use coordinator::{
    MemoryWorkflowCoordinator, SqliteWorkflowCoordinator, WORKFLOW_RUN_SCHEMA_VERSION,
    WorkflowCommandResult, WorkflowCoordinator, WorkflowDueScanPage, WorkflowDueWait,
    WorkflowRunSnapshot,
};
pub use engine::WorkflowEngine;

const MAX_WORKFLOW_WORK_TRANSITIONS: usize = 4_096;
const WORKFLOW_SETTLEMENT_TRANSITION_RESERVE: usize = 2;
const MAX_WORKFLOW_TRANSITIONS: usize =
    MAX_WORKFLOW_WORK_TRANSITIONS + WORKFLOW_SETTLEMENT_TRANSITION_RESERVE;
const MAX_WORKFLOW_WORK_JSON_BYTES: usize = 16_777_216;
const MAX_WORKFLOW_COMMAND_JSON_BYTES: usize = 131_072;
const MAX_WORKFLOW_TEXT_BYTES: usize = 65_536;
const MAX_WORKFLOW_IDEMPOTENCY_BYTES: usize = 256;
// One recovery transition plus one terminal transition can duplicate a maximum
// escaped terminal text in both the current projection and immutable evidence;
// 16 KiB covers both actor, fence, digest, sequence, and JSON envelopes.
const WORKFLOW_SETTLEMENT_JSON_BYTE_RESERVE: usize = MAX_WORKFLOW_COMMAND_JSON_BYTES * 2 + 16_384;
const MAX_WORKFLOW_JSON_BYTES: usize =
    MAX_WORKFLOW_WORK_JSON_BYTES + WORKFLOW_SETTLEMENT_JSON_BYTE_RESERVE;
const _: () = assert!(WORKFLOW_SETTLEMENT_JSON_BYTE_RESERVE < MAX_WORKFLOW_WORK_JSON_BYTES);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkflowCapacityClass {
    Work,
    Settlement,
}

/// Exact immutable Workflow implementation coordinate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowDefinition {
    /// Stable capability name.
    pub name: String,
    /// Exact semantic implementation version.
    pub version: Version,
    /// Lowercase SHA-256 digest of the immutable definition.
    pub content_sha256: String,
}

/// Caller-chosen, retry-stable creation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCreateRequest {
    /// Stable command identity reused after an uncertain response.
    pub command_id: WorkflowCommandId,
    /// Exact Workflow implementation coordinate.
    pub definition: WorkflowDefinition,
    /// Existing Task Graph whose executable work this Run coordinates.
    pub task_graph_id: TaskGraphId,
}

/// One durable cross-time wait owned by a Workflow Run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowWait {
    /// Wait for one exact external signal coordinate.
    Signal {
        /// Fences a late signal from a later wait.
        id: WorkflowWaitId,
        /// Stable signal name.
        name: String,
        /// Stable authority or Connector source.
        source: String,
        /// Optional exclusive timeout boundary.
        expires_at_ms: Option<u64>,
    },
    /// Wait until an absolute server-clock boundary.
    Timer {
        /// Fences a stale timer delivery.
        id: WorkflowWaitId,
        /// Inclusive wake boundary in Unix milliseconds.
        due_at_ms: u64,
    },
    /// Wait before an explicitly chosen retry attempt becomes eligible.
    Retry {
        /// Fences a stale retry wake.
        id: WorkflowWaitId,
        /// Stable activity or operation name.
        activity: String,
        /// Positive caller-owned retry attempt.
        attempt: u32,
        /// Inclusive eligibility boundary in Unix milliseconds.
        due_at_ms: u64,
        /// Stable effect-scoped key required by the retrying implementation.
        idempotency_key: String,
    },
}

impl WorkflowWait {
    /// Returns the fencing identity shared by all wait variants.
    #[must_use]
    pub fn id(&self) -> &WorkflowWaitId {
        match self {
            Self::Signal { id, .. } | Self::Timer { id, .. } | Self::Retry { id, .. } => id,
        }
    }
}

/// Current durable Workflow lifecycle projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowStatus {
    /// The Workflow driver may make forward progress.
    Running,
    /// Progress is suspended at one durable fenced wait.
    Waiting {
        /// Exact wait that must settle before progress continues.
        wait: WorkflowWait,
    },
    /// The Workflow settled successfully.
    Completed {
        /// Bounded completion evidence summary.
        summary: String,
    },
    /// The Workflow settled unsuccessfully.
    Failed {
        /// Bounded failure reason.
        reason: String,
    },
    /// An operator or parent explicitly stopped the Workflow.
    Cancelled {
        /// Bounded cancellation reason.
        reason: String,
    },
}

impl WorkflowStatus {
    /// Returns whether no later command may change this status.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }
}

/// Idempotent mutation submitted to one Workflow Run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowCommand {
    /// Stable identity reused after an uncertain response.
    pub id: WorkflowCommandId,
    /// Typed lifecycle mutation.
    pub kind: WorkflowCommandKind,
}

/// Typed Workflow lifecycle mutations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowCommandKind {
    /// Suspend until an exact external signal arrives or the optional timeout wins.
    WaitForSignal {
        /// New fencing identity for this wait.
        wait_id: WorkflowWaitId,
        /// Stable signal name.
        name: String,
        /// Stable authority or Connector source.
        source: String,
        /// Optional exclusive timeout boundary.
        expires_at_ms: Option<u64>,
    },
    /// Suspend until one absolute server-clock boundary.
    WaitUntil {
        /// New fencing identity for this wait.
        wait_id: WorkflowWaitId,
        /// Inclusive wake boundary in Unix milliseconds.
        due_at_ms: u64,
    },
    /// Suspend until an explicitly chosen retry attempt is eligible.
    ScheduleRetry {
        /// New fencing identity for this wait.
        wait_id: WorkflowWaitId,
        /// Stable activity or operation name.
        activity: String,
        /// Positive caller-owned retry attempt.
        attempt: u32,
        /// Inclusive eligibility boundary in Unix milliseconds.
        due_at_ms: u64,
        /// Stable effect-scoped key reused by the retrying implementation.
        idempotency_key: String,
    },
    /// Deliver one external signal to the exact current wait.
    DeliverSignal {
        /// Exact wait observed by the signal router.
        wait_id: WorkflowWaitId,
        /// Stable source event identity.
        signal_id: WorkflowSignalId,
        /// Signal name that must match the wait.
        name: String,
        /// Signal source that must match the wait.
        source: String,
        /// Stable source-scoped delivery key.
        idempotency_key: String,
    },
    /// Wake an exact timer, retry, or expired signal wait using the server clock.
    WakeDue {
        /// Exact wait observed by the timer worker.
        wait_id: WorkflowWaitId,
    },
    /// Move the generic Run state to a newer immutable Workflow implementation.
    MigrateDefinition {
        /// Strictly newer compatible implementation coordinate.
        definition: WorkflowDefinition,
    },
    /// Settle a running Workflow successfully.
    Complete {
        /// Bounded completion evidence summary.
        summary: String,
    },
    /// Settle a nonterminal Workflow unsuccessfully.
    Fail {
        /// Bounded failure reason.
        reason: String,
    },
    /// Stop a nonterminal Workflow explicitly.
    Cancel {
        /// Bounded cancellation reason.
        reason: String,
    },
}

impl WorkflowCommandKind {
    fn capacity_class(&self) -> WorkflowCapacityClass {
        match self {
            Self::DeliverSignal { .. }
            | Self::WakeDue { .. }
            | Self::Complete { .. }
            | Self::Fail { .. }
            | Self::Cancel { .. } => WorkflowCapacityClass::Settlement,
            Self::WaitForSignal { .. }
            | Self::WaitUntil { .. }
            | Self::ScheduleRetry { .. }
            | Self::MigrateDefinition { .. } => WorkflowCapacityClass::Work,
        }
    }
}

/// Durable reason why a time-owned wait resumed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowWakeReason {
    /// The signal wait reached its timeout before a signal was committed.
    SignalTimeout,
    /// The absolute timer reached its due boundary.
    Timer,
    /// The explicit retry delay reached its due boundary.
    Retry,
}

/// Immutable transition evidence retained by the Workflow Run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTransition {
    /// Run-local positive total ordering.
    pub sequence: u64,
    /// Retry-stable command identity.
    pub command_id: WorkflowCommandId,
    /// Lowercase SHA-256 of the exact create request or command.
    pub command_sha256: String,
    /// Server-clock application time in Unix milliseconds.
    pub applied_at_ms: u64,
    /// Trusted actor attributed by the embedding host or transport.
    pub actor: ActorIdentity,
    /// Typed transition evidence.
    pub kind: WorkflowTransitionKind,
}

/// Typed immutable Workflow transition evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkflowTransitionKind {
    /// The Run linked one exact definition to one existing Task Graph.
    Created {
        /// Initial immutable Workflow implementation.
        definition: WorkflowDefinition,
        /// Executable work coordinated by this Run.
        task_graph_id: TaskGraphId,
    },
    /// The Run entered one durable wait.
    WaitStarted {
        /// Exact wait projection.
        wait: WorkflowWait,
    },
    /// One exact external signal resumed the Run.
    SignalDelivered {
        /// Settled wait fence.
        wait_id: WorkflowWaitId,
        /// Stable source event identity.
        signal_id: WorkflowSignalId,
        /// Exact signal name.
        name: String,
        /// Exact signal source.
        source: String,
        /// Stable source-scoped delivery key.
        idempotency_key: String,
    },
    /// A server-clock boundary resumed the Run.
    WaitDue {
        /// Settled wait fence.
        wait_id: WorkflowWaitId,
        /// Time-owned settlement category.
        reason: WorkflowWakeReason,
    },
    /// The generic Run moved at a durable wait boundary.
    DefinitionMigrated {
        /// Previous immutable implementation.
        from: WorkflowDefinition,
        /// New immutable implementation.
        to: WorkflowDefinition,
    },
    /// The Run settled successfully.
    Completed {
        /// Bounded completion evidence summary.
        summary: String,
    },
    /// The Run settled unsuccessfully.
    Failed {
        /// Bounded failure reason.
        reason: String,
    },
    /// The Run was explicitly stopped.
    Cancelled {
        /// Bounded cancellation reason.
        reason: String,
    },
}

impl WorkflowTransitionKind {
    fn capacity_class(&self) -> WorkflowCapacityClass {
        match self {
            Self::SignalDelivered { .. }
            | Self::WaitDue { .. }
            | Self::Completed { .. }
            | Self::Failed { .. }
            | Self::Cancelled { .. } => WorkflowCapacityClass::Settlement,
            Self::Created { .. } | Self::WaitStarted { .. } | Self::DefinitionMigrated { .. } => {
                WorkflowCapacityClass::Work
            }
        }
    }
}

/// Whether a Workflow command changed durable state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowApplyOutcome {
    /// A new transition was committed.
    Applied,
    /// The exact command identity and digest were already committed.
    Duplicate,
}

/// Pure serializable Workflow Run aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorkflowRun {
    definition: WorkflowDefinition,
    task_graph_id: TaskGraphId,
    status: WorkflowStatus,
    transitions: Vec<WorkflowTransition>,
    #[serde(skip)]
    materialization_charge_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowRunWire {
    definition: WorkflowDefinition,
    task_graph_id: TaskGraphId,
    status: WorkflowStatus,
    transitions: Vec<WorkflowTransition>,
}

impl<'de> Deserialize<'de> for WorkflowRun {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = WorkflowRunWire::deserialize(deserializer)?;
        let mut run = Self {
            definition: wire.definition,
            task_graph_id: wire.task_graph_id,
            status: wire.status,
            transitions: wire.transitions,
            materialization_charge_bytes: 0,
        };
        run.validate().map_err(D::Error::custom)?;
        run.materialization_charge_bytes = encoded_size(&run).map_err(D::Error::custom)?;
        Ok(run)
    }
}

impl WorkflowRun {
    /// Constructs a new running Workflow linked to one existing Task Graph.
    pub(crate) fn new(
        request: WorkflowCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<Self, HarnessError> {
        validate_create_request(&request)?;
        validate_application_time(applied_at_ms)?;
        authority.validate_current("Workflow creation authority")?;
        let digest = command_digest(&request)?;
        let transition = WorkflowTransition {
            sequence: 1,
            command_id: request.command_id,
            command_sha256: digest,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: WorkflowTransitionKind::Created {
                definition: request.definition.clone(),
                task_graph_id: request.task_graph_id.clone(),
            },
        };
        let mut run = Self {
            definition: request.definition,
            task_graph_id: request.task_graph_id,
            status: WorkflowStatus::Running,
            transitions: vec![transition],
            materialization_charge_bytes: 0,
        };
        run.validate()?;
        run.materialization_charge_bytes = encoded_size(&run)?;
        Ok(run)
    }

    /// Returns the current immutable implementation coordinate.
    #[must_use]
    pub fn definition(&self) -> &WorkflowDefinition {
        &self.definition
    }

    /// Returns the linked executable Task Graph.
    #[must_use]
    pub fn task_graph_id(&self) -> &TaskGraphId {
        &self.task_graph_id
    }

    /// Returns the current lifecycle projection.
    #[must_use]
    pub fn status(&self) -> &WorkflowStatus {
        &self.status
    }

    /// Returns immutable transitions in sequence order.
    pub fn transitions(&self) -> impl Iterator<Item = &WorkflowTransition> {
        self.transitions.iter()
    }

    /// Returns the current number of immutable transitions.
    #[must_use]
    pub fn transition_count(&self) -> usize {
        self.transitions.len()
    }

    /// Returns the conservative encoded materialization charge.
    #[must_use]
    pub fn materialization_charge_bytes(&self) -> usize {
        self.materialization_charge_bytes
    }

    pub(crate) fn create_matches(
        &self,
        request: &WorkflowCreateRequest,
    ) -> Result<bool, HarnessError> {
        let digest = command_digest(request)?;
        Ok(self.transitions.first().is_some_and(|transition| {
            transition.command_id == request.command_id && transition.command_sha256 == digest
        }))
    }

    pub(crate) fn apply(
        &mut self,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<WorkflowApplyOutcome, HarnessError> {
        if self.recognizes_command(&command)? {
            return Ok(WorkflowApplyOutcome::Duplicate);
        }
        validate_application_time(applied_at_ms)?;
        authority.validate_current("Workflow command authority")?;
        let digest = command_digest(&command)?;
        let capacity_class = command.kind.capacity_class();
        validate_transition_capacity(self.transitions.len(), capacity_class)?;
        if self
            .transitions
            .last()
            .is_some_and(|transition| applied_at_ms < transition.applied_at_ms)
        {
            return Err(HarnessError::Workflow(
                "Workflow application time cannot move backwards".to_owned(),
            ));
        }
        let mut next = self.clone();
        let transition_kind = next.apply_kind(command.kind, applied_at_ms)?;
        let sequence = u64::try_from(next.transitions.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| HarnessError::Workflow("Workflow sequence overflow".to_owned()))?;
        next.transitions.push(WorkflowTransition {
            sequence,
            command_id: command.id,
            command_sha256: digest,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: transition_kind,
        });
        next.validate()?;
        next.materialization_charge_bytes = encoded_size_for(&next, capacity_class)?;
        *self = next;
        Ok(WorkflowApplyOutcome::Applied)
    }

    pub(crate) fn recognizes_command(
        &self,
        command: &WorkflowCommand,
    ) -> Result<bool, HarnessError> {
        validate_command(command)?;
        let digest = command_digest(command)?;
        let Some(existing) = self
            .transitions
            .iter()
            .find(|transition| transition.command_id == command.id)
        else {
            return Ok(false);
        };
        if existing.command_sha256 == digest {
            Ok(true)
        } else {
            Err(HarnessError::Workflow(format!(
                "Workflow command {} was reused with different content",
                command.id
            )))
        }
    }

    fn apply_kind(
        &mut self,
        kind: WorkflowCommandKind,
        applied_at_ms: u64,
    ) -> Result<WorkflowTransitionKind, HarnessError> {
        match kind {
            WorkflowCommandKind::WaitForSignal {
                wait_id,
                name,
                source,
                expires_at_ms,
            } => {
                self.require_running("start a signal wait")?;
                if expires_at_ms.is_some_and(|expires| expires <= applied_at_ms) {
                    return Err(HarnessError::Workflow(
                        "signal wait expiration must be later than application time".to_owned(),
                    ));
                }
                validate_capability_name("Workflow signal", &name)?;
                validate_capability_name("Workflow signal source", &source)?;
                self.ensure_new_wait_id(&wait_id)?;
                let wait = WorkflowWait::Signal {
                    id: wait_id,
                    name,
                    source,
                    expires_at_ms,
                };
                self.status = WorkflowStatus::Waiting { wait: wait.clone() };
                Ok(WorkflowTransitionKind::WaitStarted { wait })
            }
            WorkflowCommandKind::WaitUntil { wait_id, due_at_ms } => {
                self.require_running("start a timer wait")?;
                if due_at_ms <= applied_at_ms {
                    return Err(HarnessError::Workflow(
                        "timer due time must be later than application time".to_owned(),
                    ));
                }
                self.ensure_new_wait_id(&wait_id)?;
                let wait = WorkflowWait::Timer {
                    id: wait_id,
                    due_at_ms,
                };
                self.status = WorkflowStatus::Waiting { wait: wait.clone() };
                Ok(WorkflowTransitionKind::WaitStarted { wait })
            }
            WorkflowCommandKind::ScheduleRetry {
                wait_id,
                activity,
                attempt,
                due_at_ms,
                idempotency_key,
            } => {
                self.require_running("schedule a retry")?;
                validate_capability_name("Workflow retry activity", &activity)?;
                validate_idempotency_key("Workflow retry", &idempotency_key)?;
                if attempt == 0 {
                    return Err(HarnessError::Workflow(
                        "Workflow retry attempt must be positive".to_owned(),
                    ));
                }
                if due_at_ms <= applied_at_ms {
                    return Err(HarnessError::Workflow(
                        "Workflow retry due time must be later than application time".to_owned(),
                    ));
                }
                self.ensure_new_wait_id(&wait_id)?;
                let wait = WorkflowWait::Retry {
                    id: wait_id,
                    activity,
                    attempt,
                    due_at_ms,
                    idempotency_key,
                };
                self.status = WorkflowStatus::Waiting { wait: wait.clone() };
                Ok(WorkflowTransitionKind::WaitStarted { wait })
            }
            WorkflowCommandKind::DeliverSignal {
                wait_id,
                signal_id,
                name,
                source,
                idempotency_key,
            } => {
                validate_capability_name("Workflow signal", &name)?;
                validate_capability_name("Workflow signal source", &source)?;
                validate_idempotency_key("Workflow signal", &idempotency_key)?;
                validate_identity("Workflow signal", signal_id.as_str())?;
                if self.transitions.iter().any(|transition| {
                    matches!(
                        &transition.kind,
                        WorkflowTransitionKind::SignalDelivered {
                            signal_id: existing,
                            ..
                        } if existing == &signal_id
                    )
                }) {
                    return Err(HarnessError::Workflow(format!(
                        "Workflow signal {signal_id} is already committed"
                    )));
                }
                let WorkflowStatus::Waiting {
                    wait:
                        WorkflowWait::Signal {
                            id,
                            name: expected_name,
                            source: expected_source,
                            expires_at_ms,
                        },
                } = &self.status
                else {
                    return Err(HarnessError::Workflow(
                        "Workflow is not waiting for a signal".to_owned(),
                    ));
                };
                if id != &wait_id || expected_name != &name || expected_source != &source {
                    return Err(HarnessError::Workflow(
                        "Workflow signal does not match the current wait".to_owned(),
                    ));
                }
                if expires_at_ms.is_some_and(|expires| applied_at_ms >= expires) {
                    return Err(HarnessError::Workflow(
                        "Workflow signal arrived at or after the timeout boundary".to_owned(),
                    ));
                }
                self.status = WorkflowStatus::Running;
                Ok(WorkflowTransitionKind::SignalDelivered {
                    wait_id,
                    signal_id,
                    name,
                    source,
                    idempotency_key,
                })
            }
            WorkflowCommandKind::WakeDue { wait_id } => {
                let WorkflowStatus::Waiting { wait } = &self.status else {
                    return Err(HarnessError::Workflow(
                        "Workflow has no current wait to wake".to_owned(),
                    ));
                };
                if wait.id() != &wait_id {
                    return Err(HarnessError::Workflow(
                        "Workflow wake does not match the current wait".to_owned(),
                    ));
                }
                let reason = match wait {
                    WorkflowWait::Signal {
                        expires_at_ms: Some(expires_at_ms),
                        ..
                    } if applied_at_ms >= *expires_at_ms => WorkflowWakeReason::SignalTimeout,
                    WorkflowWait::Timer { due_at_ms, .. } if applied_at_ms >= *due_at_ms => {
                        WorkflowWakeReason::Timer
                    }
                    WorkflowWait::Retry { due_at_ms, .. } if applied_at_ms >= *due_at_ms => {
                        WorkflowWakeReason::Retry
                    }
                    WorkflowWait::Signal {
                        expires_at_ms: None,
                        ..
                    } => {
                        return Err(HarnessError::Workflow(
                            "signal wait has no due boundary".to_owned(),
                        ));
                    }
                    _ => {
                        return Err(HarnessError::Workflow(
                            "Workflow wait is not due".to_owned(),
                        ));
                    }
                };
                self.status = WorkflowStatus::Running;
                Ok(WorkflowTransitionKind::WaitDue { wait_id, reason })
            }
            WorkflowCommandKind::MigrateDefinition { definition } => {
                if !matches!(self.status, WorkflowStatus::Waiting { .. }) {
                    return Err(HarnessError::Workflow(
                        "Workflow definition migration requires a durable wait boundary".to_owned(),
                    ));
                }
                validate_definition(&definition)?;
                if definition.name != self.definition.name
                    || definition.version <= self.definition.version
                    || definition.content_sha256 == self.definition.content_sha256
                {
                    return Err(HarnessError::Workflow(
                        "Workflow migration requires the same name, a newer version, and a different digest"
                            .to_owned(),
                    ));
                }
                let from = std::mem::replace(&mut self.definition, definition.clone());
                Ok(WorkflowTransitionKind::DefinitionMigrated {
                    from,
                    to: definition,
                })
            }
            WorkflowCommandKind::Complete { summary } => {
                self.require_running("complete")?;
                validate_text("Workflow completion summary", &summary)?;
                self.status = WorkflowStatus::Completed {
                    summary: summary.clone(),
                };
                Ok(WorkflowTransitionKind::Completed { summary })
            }
            WorkflowCommandKind::Fail { reason } => {
                self.require_nonterminal("fail")?;
                validate_text("Workflow failure reason", &reason)?;
                self.status = WorkflowStatus::Failed {
                    reason: reason.clone(),
                };
                Ok(WorkflowTransitionKind::Failed { reason })
            }
            WorkflowCommandKind::Cancel { reason } => {
                self.require_nonterminal("cancel")?;
                validate_text("Workflow cancellation reason", &reason)?;
                self.status = WorkflowStatus::Cancelled {
                    reason: reason.clone(),
                };
                Ok(WorkflowTransitionKind::Cancelled { reason })
            }
        }
    }

    fn ensure_new_wait_id(&self, wait_id: &WorkflowWaitId) -> Result<(), HarnessError> {
        validate_identity("Workflow wait", wait_id.as_str())?;
        if self.transitions.iter().any(|transition| {
            matches!(
                &transition.kind,
                WorkflowTransitionKind::WaitStarted { wait } if wait.id() == wait_id
            )
        }) {
            return Err(HarnessError::Workflow(format!(
                "Workflow wait {wait_id} is already committed"
            )));
        }
        Ok(())
    }

    fn require_running(&self, action: &str) -> Result<(), HarnessError> {
        if matches!(self.status, WorkflowStatus::Running) {
            Ok(())
        } else {
            Err(HarnessError::Workflow(format!(
                "Workflow must be running to {action}"
            )))
        }
    }

    fn require_nonterminal(&self, action: &str) -> Result<(), HarnessError> {
        if self.status.is_terminal() {
            Err(HarnessError::Workflow(format!(
                "terminal Workflow cannot {action}"
            )))
        } else {
            Ok(())
        }
    }

    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        validate_definition(&self.definition)?;
        validate_identity("Workflow Task Graph", self.task_graph_id.as_str())?;
        if self.transitions.is_empty() || self.transitions.len() > MAX_WORKFLOW_TRANSITIONS {
            return Err(HarnessError::Workflow(format!(
                "Workflow Run must retain 1-{MAX_WORKFLOW_TRANSITIONS} transitions"
            )));
        }
        let mut expected_sequence = 1_u64;
        let mut command_ids = std::collections::BTreeMap::new();
        let mut wait_ids = std::collections::BTreeSet::new();
        let mut signal_ids = std::collections::BTreeSet::new();
        let mut projected_definition = None;
        let mut projected_status = None;
        let mut projected_task_graph = None;
        let mut previous_applied_at_ms = 0_u64;
        let work_transition_limit = u64::try_from(MAX_WORKFLOW_WORK_TRANSITIONS)
            .map_err(|_| HarnessError::Workflow("Workflow capacity overflow".to_owned()))?;
        for transition in &self.transitions {
            if transition.sequence != expected_sequence {
                return Err(HarnessError::Workflow(
                    "Workflow transition sequence is not contiguous".to_owned(),
                ));
            }
            expected_sequence = expected_sequence
                .checked_add(1)
                .ok_or_else(|| HarnessError::Workflow("Workflow sequence overflow".to_owned()))?;
            validate_identity("Workflow command", transition.command_id.as_str())?;
            validate_digest("Workflow command", &transition.command_sha256)?;
            validate_application_time(transition.applied_at_ms)?;
            if transition.applied_at_ms < previous_applied_at_ms {
                return Err(HarnessError::Workflow(
                    "Workflow transition time is not monotonic".to_owned(),
                ));
            }
            previous_applied_at_ms = transition.applied_at_ms;
            transition
                .actor
                .validate_shape("Workflow transition actor")
                .map_err(|error| HarnessError::Workflow(error.to_string()))?;
            if transition.command_sha256 != transition_digest(transition)? {
                return Err(HarnessError::Workflow(
                    "Workflow transition command digest differs from its content".to_owned(),
                ));
            }
            if transition.sequence > work_transition_limit
                && transition.kind.capacity_class() == WorkflowCapacityClass::Work
            {
                return Err(HarnessError::Workflow(
                    "Workflow work transition consumed the settlement reserve".to_owned(),
                ));
            }
            if command_ids
                .insert(
                    transition.command_id.as_str(),
                    transition.command_sha256.as_str(),
                )
                .is_some()
            {
                return Err(HarnessError::Workflow(
                    "Workflow contains duplicate command identities".to_owned(),
                ));
            }
            match &transition.kind {
                WorkflowTransitionKind::Created {
                    definition,
                    task_graph_id,
                } if transition.sequence == 1 => {
                    validate_definition(definition)?;
                    validate_identity("Workflow Task Graph", task_graph_id.as_str())?;
                    projected_definition = Some(definition.clone());
                    projected_task_graph = Some(task_graph_id.clone());
                    projected_status = Some(WorkflowStatus::Running);
                }
                WorkflowTransitionKind::Created { .. } => {
                    return Err(HarnessError::Workflow(
                        "Workflow creation must be the first transition".to_owned(),
                    ));
                }
                WorkflowTransitionKind::WaitStarted { wait } => {
                    if !matches!(projected_status, Some(WorkflowStatus::Running)) {
                        return Err(HarnessError::Workflow(
                            "Workflow wait does not follow running state".to_owned(),
                        ));
                    }
                    validate_wait(wait)?;
                    if !wait_ids.insert(wait.id().as_str()) {
                        return Err(HarnessError::Workflow(
                            "Workflow contains duplicate wait identities".to_owned(),
                        ));
                    }
                    projected_status = Some(WorkflowStatus::Waiting { wait: wait.clone() });
                }
                WorkflowTransitionKind::SignalDelivered {
                    wait_id,
                    signal_id,
                    name,
                    source,
                    idempotency_key,
                } => {
                    validate_identity("Workflow signal", signal_id.as_str())?;
                    validate_capability_name("Workflow signal", name)?;
                    validate_capability_name("Workflow signal source", source)?;
                    validate_idempotency_key("Workflow signal", idempotency_key)?;
                    if !signal_ids.insert(signal_id.as_str()) {
                        return Err(HarnessError::Workflow(
                            "Workflow contains duplicate signal identities".to_owned(),
                        ));
                    }
                    let Some(WorkflowStatus::Waiting {
                        wait:
                            WorkflowWait::Signal {
                                id,
                                name: expected_name,
                                source: expected_source,
                                expires_at_ms,
                            },
                    }) = &projected_status
                    else {
                        return Err(HarnessError::Workflow(
                            "Workflow signal does not follow a signal wait".to_owned(),
                        ));
                    };
                    if id != wait_id || expected_name != name || expected_source != source {
                        return Err(HarnessError::Workflow(
                            "Workflow signal transition differs from its wait".to_owned(),
                        ));
                    }
                    if expires_at_ms.is_some_and(|expires| transition.applied_at_ms >= expires) {
                        return Err(HarnessError::Workflow(
                            "Workflow signal transition crossed its timeout".to_owned(),
                        ));
                    }
                    projected_status = Some(WorkflowStatus::Running);
                }
                WorkflowTransitionKind::WaitDue { wait_id, reason } => {
                    let Some(WorkflowStatus::Waiting { wait }) = &projected_status else {
                        return Err(HarnessError::Workflow(
                            "Workflow due transition does not follow a wait".to_owned(),
                        ));
                    };
                    if wait.id() != wait_id
                        || due_reason(wait, transition.applied_at_ms)? != *reason
                    {
                        return Err(HarnessError::Workflow(
                            "Workflow due transition differs from its wait".to_owned(),
                        ));
                    }
                    projected_status = Some(WorkflowStatus::Running);
                }
                WorkflowTransitionKind::DefinitionMigrated { from, to } => {
                    if !matches!(projected_status, Some(WorkflowStatus::Waiting { .. }))
                        || projected_definition.as_ref() != Some(from)
                    {
                        return Err(HarnessError::Workflow(
                            "Workflow definition migration has no matching safe boundary"
                                .to_owned(),
                        ));
                    }
                    validate_definition(to)?;
                    if to.name != from.name
                        || to.version <= from.version
                        || to.content_sha256 == from.content_sha256
                    {
                        return Err(HarnessError::Workflow(
                            "Workflow definition migration is not monotonic".to_owned(),
                        ));
                    }
                    projected_definition = Some(to.clone());
                }
                WorkflowTransitionKind::Completed { summary } => {
                    if !matches!(projected_status, Some(WorkflowStatus::Running)) {
                        return Err(HarnessError::Workflow(
                            "Workflow completion does not follow running state".to_owned(),
                        ));
                    }
                    validate_text("Workflow completion summary", summary)?;
                    projected_status = Some(WorkflowStatus::Completed {
                        summary: summary.clone(),
                    });
                }
                WorkflowTransitionKind::Failed { reason } => {
                    if projected_status
                        .as_ref()
                        .is_none_or(WorkflowStatus::is_terminal)
                    {
                        return Err(HarnessError::Workflow(
                            "Workflow failure does not follow nonterminal state".to_owned(),
                        ));
                    }
                    validate_text("Workflow failure reason", reason)?;
                    projected_status = Some(WorkflowStatus::Failed {
                        reason: reason.clone(),
                    });
                }
                WorkflowTransitionKind::Cancelled { reason } => {
                    if projected_status
                        .as_ref()
                        .is_none_or(WorkflowStatus::is_terminal)
                    {
                        return Err(HarnessError::Workflow(
                            "Workflow cancellation does not follow nonterminal state".to_owned(),
                        ));
                    }
                    validate_text("Workflow cancellation reason", reason)?;
                    projected_status = Some(WorkflowStatus::Cancelled {
                        reason: reason.clone(),
                    });
                }
            }
        }
        if projected_definition.as_ref() != Some(&self.definition)
            || projected_task_graph.as_ref() != Some(&self.task_graph_id)
            || projected_status.as_ref() != Some(&self.status)
        {
            return Err(HarnessError::Workflow(
                "Workflow projection differs from immutable transitions".to_owned(),
            ));
        }
        Ok(())
    }
}

fn due_reason(wait: &WorkflowWait, applied_at_ms: u64) -> Result<WorkflowWakeReason, HarnessError> {
    match wait {
        WorkflowWait::Signal {
            expires_at_ms: Some(expires_at_ms),
            ..
        } if applied_at_ms >= *expires_at_ms => Ok(WorkflowWakeReason::SignalTimeout),
        WorkflowWait::Timer { due_at_ms, .. } if applied_at_ms >= *due_at_ms => {
            Ok(WorkflowWakeReason::Timer)
        }
        WorkflowWait::Retry { due_at_ms, .. } if applied_at_ms >= *due_at_ms => {
            Ok(WorkflowWakeReason::Retry)
        }
        WorkflowWait::Signal {
            expires_at_ms: None,
            ..
        } => Err(HarnessError::Workflow(
            "signal wait has no due boundary".to_owned(),
        )),
        _ => Err(HarnessError::Workflow(
            "Workflow wait is not due".to_owned(),
        )),
    }
}

fn transition_digest(transition: &WorkflowTransition) -> Result<String, HarnessError> {
    match &transition.kind {
        WorkflowTransitionKind::Created {
            definition,
            task_graph_id,
        } => command_digest(&WorkflowCreateRequest {
            command_id: transition.command_id.clone(),
            definition: definition.clone(),
            task_graph_id: task_graph_id.clone(),
        }),
        kind => {
            let kind = match kind {
                WorkflowTransitionKind::Created { .. } => unreachable!(),
                WorkflowTransitionKind::WaitStarted { wait } => match wait {
                    WorkflowWait::Signal {
                        id,
                        name,
                        source,
                        expires_at_ms,
                    } => WorkflowCommandKind::WaitForSignal {
                        wait_id: id.clone(),
                        name: name.clone(),
                        source: source.clone(),
                        expires_at_ms: *expires_at_ms,
                    },
                    WorkflowWait::Timer { id, due_at_ms } => WorkflowCommandKind::WaitUntil {
                        wait_id: id.clone(),
                        due_at_ms: *due_at_ms,
                    },
                    WorkflowWait::Retry {
                        id,
                        activity,
                        attempt,
                        due_at_ms,
                        idempotency_key,
                    } => WorkflowCommandKind::ScheduleRetry {
                        wait_id: id.clone(),
                        activity: activity.clone(),
                        attempt: *attempt,
                        due_at_ms: *due_at_ms,
                        idempotency_key: idempotency_key.clone(),
                    },
                },
                WorkflowTransitionKind::SignalDelivered {
                    wait_id,
                    signal_id,
                    name,
                    source,
                    idempotency_key,
                } => WorkflowCommandKind::DeliverSignal {
                    wait_id: wait_id.clone(),
                    signal_id: signal_id.clone(),
                    name: name.clone(),
                    source: source.clone(),
                    idempotency_key: idempotency_key.clone(),
                },
                WorkflowTransitionKind::WaitDue { wait_id, .. } => WorkflowCommandKind::WakeDue {
                    wait_id: wait_id.clone(),
                },
                WorkflowTransitionKind::DefinitionMigrated { to, .. } => {
                    WorkflowCommandKind::MigrateDefinition {
                        definition: to.clone(),
                    }
                }
                WorkflowTransitionKind::Completed { summary } => WorkflowCommandKind::Complete {
                    summary: summary.clone(),
                },
                WorkflowTransitionKind::Failed { reason } => WorkflowCommandKind::Fail {
                    reason: reason.clone(),
                },
                WorkflowTransitionKind::Cancelled { reason } => WorkflowCommandKind::Cancel {
                    reason: reason.clone(),
                },
            };
            command_digest(&WorkflowCommand {
                id: transition.command_id.clone(),
                kind,
            })
        }
    }
}

fn validate_create_request(request: &WorkflowCreateRequest) -> Result<(), HarnessError> {
    validate_identity("Workflow command", request.command_id.as_str())?;
    validate_definition(&request.definition)?;
    validate_identity("Workflow Task Graph", request.task_graph_id.as_str())
}

fn validate_command(command: &WorkflowCommand) -> Result<(), HarnessError> {
    validate_identity("Workflow command", command.id.as_str())?;
    let _ = command_digest(command)?;
    Ok(())
}

fn validate_definition(definition: &WorkflowDefinition) -> Result<(), HarnessError> {
    validate_capability_name("Workflow definition", &definition.name)?;
    validate_digest("Workflow definition", &definition.content_sha256)
}

fn validate_wait(wait: &WorkflowWait) -> Result<(), HarnessError> {
    validate_identity("Workflow wait", wait.id().as_str())?;
    match wait {
        WorkflowWait::Signal {
            name,
            source,
            expires_at_ms,
            ..
        } => {
            validate_capability_name("Workflow signal", name)?;
            validate_capability_name("Workflow signal source", source)?;
            if expires_at_ms == &Some(0) {
                return Err(HarnessError::Workflow(
                    "Workflow signal expiration must be positive".to_owned(),
                ));
            }
        }
        WorkflowWait::Timer { due_at_ms, .. } => {
            if *due_at_ms == 0 {
                return Err(HarnessError::Workflow(
                    "Workflow timer due time must be positive".to_owned(),
                ));
            }
        }
        WorkflowWait::Retry {
            activity,
            attempt,
            due_at_ms,
            idempotency_key,
            ..
        } => {
            validate_capability_name("Workflow retry activity", activity)?;
            validate_idempotency_key("Workflow retry", idempotency_key)?;
            if *attempt == 0 || *due_at_ms == 0 {
                return Err(HarnessError::Workflow(
                    "Workflow retry attempt and due time must be positive".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_WORKFLOW_IDEMPOTENCY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::Workflow(format!(
            "{kind} identity must be 1-{MAX_WORKFLOW_IDEMPOTENCY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_idempotency_key(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_WORKFLOW_IDEMPOTENCY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::Workflow(format!(
            "{kind} idempotency key must be 1-{MAX_WORKFLOW_IDEMPOTENCY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_text(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_WORKFLOW_TEXT_BYTES
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(HarnessError::Workflow(format!(
            "{kind} must be 1-{MAX_WORKFLOW_TEXT_BYTES} bounded text bytes"
        )));
    }
    Ok(())
}

fn validate_digest(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::Workflow(format!(
            "{kind} digest must be lowercase SHA-256"
        )));
    }
    Ok(())
}

fn validate_application_time(value: u64) -> Result<(), HarnessError> {
    if value == 0 {
        Err(HarnessError::Workflow(
            "Workflow application time must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn command_digest(value: &impl Serialize) -> Result<String, HarnessError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| HarnessError::Workflow("cannot encode Workflow command".to_owned()))?;
    if encoded.len() > MAX_WORKFLOW_COMMAND_JSON_BYTES {
        return Err(HarnessError::Workflow(format!(
            "Workflow command exceeds {MAX_WORKFLOW_COMMAND_JSON_BYTES} encoded bytes"
        )));
    }
    Ok(Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_transition_capacity(
    current_transitions: usize,
    capacity_class: WorkflowCapacityClass,
) -> Result<(), HarnessError> {
    if current_transitions >= MAX_WORKFLOW_TRANSITIONS {
        return Err(HarnessError::Workflow(format!(
            "Workflow Run exceeds {MAX_WORKFLOW_TRANSITIONS} transitions"
        )));
    }
    if capacity_class == WorkflowCapacityClass::Work
        && current_transitions >= MAX_WORKFLOW_WORK_TRANSITIONS
    {
        return Err(HarnessError::Workflow(
            "Workflow Run has only its settlement transition reserve remaining".to_owned(),
        ));
    }
    Ok(())
}

fn validate_materialization_capacity(
    encoded_bytes: usize,
    capacity_class: WorkflowCapacityClass,
) -> Result<(), HarnessError> {
    if encoded_bytes > MAX_WORKFLOW_JSON_BYTES {
        return Err(HarnessError::Workflow(format!(
            "Workflow Run exceeds {MAX_WORKFLOW_JSON_BYTES} encoded bytes"
        )));
    }
    if capacity_class == WorkflowCapacityClass::Work && encoded_bytes > MAX_WORKFLOW_WORK_JSON_BYTES
    {
        return Err(HarnessError::Workflow(
            "Workflow Run has only its settlement encoded-byte reserve remaining".to_owned(),
        ));
    }
    Ok(())
}

fn encoded_size(run: &WorkflowRun) -> Result<usize, HarnessError> {
    encoded_size_for(run, WorkflowCapacityClass::Settlement)
}

fn encoded_size_for(
    run: &WorkflowRun,
    capacity_class: WorkflowCapacityClass,
) -> Result<usize, HarnessError> {
    let encoded = serde_json::to_vec(run)
        .map_err(|_| HarnessError::Workflow("cannot encode Workflow Run".to_owned()))?;
    validate_materialization_capacity(encoded.len(), capacity_class)?;
    Ok(encoded.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn definition(version: &str, byte: char) -> WorkflowDefinition {
        WorkflowDefinition {
            name: "example.workflow".to_owned(),
            version: Version::parse(version).expect("version"),
            content_sha256: digest(byte),
        }
    }

    fn create() -> WorkflowCreateRequest {
        WorkflowCreateRequest {
            command_id: WorkflowCommandId::from_static("create"),
            definition: definition("1.0.0", 'a'),
            task_graph_id: TaskGraphId::from_static("graph"),
        }
    }

    fn command(id: &str, kind: WorkflowCommandKind) -> WorkflowCommand {
        WorkflowCommand {
            id: WorkflowCommandId::from_string(id.to_owned()),
            kind,
        }
    }

    fn append_fixture_transition(
        run: &mut WorkflowRun,
        command: WorkflowCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) {
        let command_sha256 = command_digest(&command).expect("fixture command digest");
        let WorkflowCommand { id, kind } = command;
        let transition_kind = run
            .apply_kind(kind, applied_at_ms)
            .expect("fixture transition");
        let sequence = u64::try_from(run.transitions.len())
            .expect("fixture sequence")
            .checked_add(1)
            .expect("fixture sequence increment");
        run.transitions.push(WorkflowTransition {
            sequence,
            command_id: id,
            command_sha256,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: transition_kind,
        });
    }

    fn running_one_slot_before_work_capacity(authority: &AuthorityContext) -> (WorkflowRun, u64) {
        let mut run = WorkflowRun::new(create(), 10, authority).expect("run");
        let cycles = (MAX_WORKFLOW_WORK_TRANSITIONS - 2) / 2;
        let mut applied_at_ms = 10_u64;
        for index in 0..cycles {
            let wait_id = WorkflowWaitId::from_string(format!("capacity-wait-{index}"));
            applied_at_ms = applied_at_ms.checked_add(1).expect("fixture time");
            let due_at_ms = applied_at_ms.checked_add(1).expect("fixture due time");
            append_fixture_transition(
                &mut run,
                command(
                    &format!("capacity-wait-command-{index}"),
                    WorkflowCommandKind::WaitUntil {
                        wait_id: wait_id.clone(),
                        due_at_ms,
                    },
                ),
                applied_at_ms,
                authority,
            );
            append_fixture_transition(
                &mut run,
                command(
                    &format!("capacity-wake-command-{index}"),
                    WorkflowCommandKind::WakeDue { wait_id },
                ),
                due_at_ms,
                authority,
            );
            applied_at_ms = due_at_ms;
        }
        assert_eq!(run.transition_count(), MAX_WORKFLOW_WORK_TRANSITIONS - 1);
        assert_eq!(run.status(), &WorkflowStatus::Running);
        run.validate().expect("fixture projection");
        run.materialization_charge_bytes = encoded_size(&run).expect("fixture size");
        (run, applied_at_ms)
    }

    fn escaped_identity(suffix: char) -> String {
        let mut value = "\\".repeat(MAX_WORKFLOW_IDEMPOTENCY_BYTES - suffix.len_utf8());
        value.push(suffix);
        value
    }

    fn largest_escaped_failure_command(id: &str) -> WorkflowCommand {
        let mut accepted = 1_usize;
        let mut rejected = MAX_WORKFLOW_TEXT_BYTES + 1;
        while accepted + 1 < rejected {
            let candidate = accepted + (rejected - accepted) / 2;
            let command = command(
                id,
                WorkflowCommandKind::Fail {
                    reason: "\\".repeat(candidate),
                },
            );
            if command_digest(&command).is_ok() {
                accepted = candidate;
            } else {
                rejected = candidate;
            }
        }
        command(
            id,
            WorkflowCommandKind::Fail {
                reason: "\\".repeat(accepted),
            },
        )
    }

    #[test]
    fn signal_wait_is_fenced_idempotent_and_timeout_ordered() {
        let authority = AuthorityContext::local_process();
        let mut run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let wait = WorkflowWaitId::from_static("wait");
        let start = command(
            "wait-command",
            WorkflowCommandKind::WaitForSignal {
                wait_id: wait.clone(),
                name: "order.updated".to_owned(),
                source: "connector.orders".to_owned(),
                expires_at_ms: Some(100),
            },
        );
        assert_eq!(
            run.apply(start.clone(), 20, &authority).expect("wait"),
            WorkflowApplyOutcome::Applied
        );
        assert_eq!(
            run.apply(start, 21, &authority).expect("duplicate"),
            WorkflowApplyOutcome::Duplicate
        );
        let signal = command(
            "signal-command",
            WorkflowCommandKind::DeliverSignal {
                wait_id: wait,
                signal_id: WorkflowSignalId::from_static("signal"),
                name: "order.updated".to_owned(),
                source: "connector.orders".to_owned(),
                idempotency_key: "source-event-1".to_owned(),
            },
        );
        assert_eq!(
            run.apply(signal, 99, &authority).expect("signal"),
            WorkflowApplyOutcome::Applied
        );
        assert_eq!(run.status(), &WorkflowStatus::Running);
        assert_eq!(run.transition_count(), 3);
        run.validate().expect("valid projection");
    }

    #[test]
    fn late_signal_cannot_beat_timeout_or_reused_wait() {
        let authority = AuthorityContext::local_process();
        let mut run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let wait = WorkflowWaitId::from_static("wait");
        run.apply(
            command(
                "wait-command",
                WorkflowCommandKind::WaitForSignal {
                    wait_id: wait.clone(),
                    name: "event.name".to_owned(),
                    source: "source.name".to_owned(),
                    expires_at_ms: Some(100),
                },
            ),
            20,
            &authority,
        )
        .expect("wait");
        let late = run
            .apply(
                command(
                    "late-signal",
                    WorkflowCommandKind::DeliverSignal {
                        wait_id: wait.clone(),
                        signal_id: WorkflowSignalId::from_static("signal"),
                        name: "event.name".to_owned(),
                        source: "source.name".to_owned(),
                        idempotency_key: "event-1".to_owned(),
                    },
                ),
                100,
                &authority,
            )
            .expect_err("late signal");
        assert!(late.to_string().contains("timeout"));
        run.apply(
            command(
                "wake",
                WorkflowCommandKind::WakeDue {
                    wait_id: wait.clone(),
                },
            ),
            100,
            &authority,
        )
        .expect("timeout");
        let reused = run
            .apply(
                command(
                    "reuse",
                    WorkflowCommandKind::WaitUntil {
                        wait_id: wait,
                        due_at_ms: 200,
                    },
                ),
                110,
                &authority,
            )
            .expect_err("reused wait");
        assert!(reused.to_string().contains("already committed"));
    }

    #[test]
    fn retry_and_definition_migration_require_safe_boundaries() {
        let authority = AuthorityContext::local_process();
        let mut run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let unsafe_migration = run
            .apply(
                command(
                    "migrate-unsafe",
                    WorkflowCommandKind::MigrateDefinition {
                        definition: definition("2.0.0", 'b'),
                    },
                ),
                20,
                &authority,
            )
            .expect_err("unsafe migration");
        assert!(unsafe_migration.to_string().contains("wait boundary"));
        let wait = WorkflowWaitId::from_static("retry");
        run.apply(
            command(
                "schedule",
                WorkflowCommandKind::ScheduleRetry {
                    wait_id: wait.clone(),
                    activity: "connector.write".to_owned(),
                    attempt: 2,
                    due_at_ms: 100,
                    idempotency_key: "effect-1".to_owned(),
                },
            ),
            20,
            &authority,
        )
        .expect("retry");
        run.apply(
            command(
                "migrate",
                WorkflowCommandKind::MigrateDefinition {
                    definition: definition("2.0.0", 'b'),
                },
            ),
            30,
            &authority,
        )
        .expect("migration");
        assert_eq!(run.definition(), &definition("2.0.0", 'b'));
        run.apply(
            command("wake", WorkflowCommandKind::WakeDue { wait_id: wait }),
            100,
            &authority,
        )
        .expect("retry wake");
        assert_eq!(run.status(), &WorkflowStatus::Running);
    }

    #[test]
    fn failed_mutation_preserves_exact_run() {
        let authority = AuthorityContext::local_process();
        let mut run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let before = run.clone();
        let error = run
            .apply(
                command(
                    "bad-timer",
                    WorkflowCommandKind::WaitUntil {
                        wait_id: WorkflowWaitId::from_static("wait"),
                        due_at_ms: 20,
                    },
                ),
                20,
                &authority,
            )
            .expect_err("invalid timer");
        assert!(error.to_string().contains("later"));
        assert_eq!(run, before);
    }

    #[test]
    fn deserialization_rejects_projection_tampering() {
        let authority = AuthorityContext::local_process();
        let run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let mut encoded = serde_json::to_value(run).expect("encode");
        encoded["status"] = serde_json::json!({
            "status": "completed",
            "summary": "forged"
        });
        let error = serde_json::from_value::<WorkflowRun>(encoded).expect_err("tamper");
        assert!(error.to_string().contains("projection"));
    }

    #[test]
    fn deserialization_rejects_command_digest_tampering() {
        let authority = AuthorityContext::local_process();
        let run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let mut encoded = serde_json::to_value(run).expect("encode");
        encoded["transitions"][0]["command_sha256"] = serde_json::json!("b".repeat(64));
        let error = serde_json::from_value::<WorkflowRun>(encoded).expect_err("tamper");
        assert!(error.to_string().contains("digest differs"));
    }

    #[test]
    fn final_work_slot_can_wait_then_wake_and_settle() {
        let authority = AuthorityContext::local_process();
        let (mut waiting, previous_at_ms) = running_one_slot_before_work_capacity(&authority);
        let wait_id = WorkflowWaitId::from_static("final-capacity-wait");
        let wait_at_ms = previous_at_ms.checked_add(1).expect("wait time");
        let due_at_ms = wait_at_ms.checked_add(1).expect("due time");
        waiting
            .apply(
                command(
                    "final-capacity-wait-command",
                    WorkflowCommandKind::WaitUntil {
                        wait_id: wait_id.clone(),
                        due_at_ms,
                    },
                ),
                wait_at_ms,
                &authority,
            )
            .expect("last work slot enters Waiting");
        assert_eq!(waiting.transition_count(), MAX_WORKFLOW_WORK_TRANSITIONS);
        assert!(matches!(waiting.status(), WorkflowStatus::Waiting { .. }));
        waiting =
            serde_json::from_slice(&serde_json::to_vec(&waiting).expect("encode old work ceiling"))
                .expect("old work ceiling remains wire-compatible");

        let before_rejected_work = waiting.clone();
        let rejected_work = waiting
            .apply(
                command(
                    "capacity-migration",
                    WorkflowCommandKind::MigrateDefinition {
                        definition: definition("2.0.0", 'b'),
                    },
                ),
                due_at_ms,
                &authority,
            )
            .expect_err("work cannot consume settlement reserve");
        assert!(
            rejected_work
                .to_string()
                .contains("settlement transition reserve")
        );
        assert_eq!(waiting, before_rejected_work);

        let mut woken_and_failed = waiting.clone();
        woken_and_failed
            .apply(
                command(
                    "capacity-wake",
                    WorkflowCommandKind::WakeDue {
                        wait_id: wait_id.clone(),
                    },
                ),
                due_at_ms,
                &authority,
            )
            .expect("reserved wake");
        assert_eq!(
            woken_and_failed.transition_count(),
            MAX_WORKFLOW_WORK_TRANSITIONS + 1
        );
        assert_eq!(woken_and_failed.status(), &WorkflowStatus::Running);
        let failure = command(
            "capacity-fail",
            WorkflowCommandKind::Fail {
                reason: "capacity exhausted".to_owned(),
            },
        );
        woken_and_failed
            .apply(failure.clone(), due_at_ms + 1, &authority)
            .expect("reserved terminal failure");
        assert_eq!(
            woken_and_failed.transition_count(),
            MAX_WORKFLOW_TRANSITIONS
        );
        assert!(matches!(
            woken_and_failed.status(),
            WorkflowStatus::Failed { .. }
        ));
        assert_eq!(
            woken_and_failed
                .apply(failure, due_at_ms + 2, &authority)
                .expect("duplicate at hard boundary"),
            WorkflowApplyOutcome::Duplicate
        );
        let hard_error = woken_and_failed
            .apply(
                command(
                    "beyond-hard-capacity",
                    WorkflowCommandKind::Cancel {
                        reason: "too late".to_owned(),
                    },
                ),
                due_at_ms + 3,
                &authority,
            )
            .expect_err("hard transition boundary");
        assert!(
            hard_error
                .to_string()
                .contains(&format!("{MAX_WORKFLOW_TRANSITIONS} transitions"))
        );

        let mut cancelled = waiting;
        cancelled
            .apply(
                command(
                    "capacity-cancel",
                    WorkflowCommandKind::Cancel {
                        reason: "operator stopped the run".to_owned(),
                    },
                ),
                due_at_ms,
                &authority,
            )
            .expect("reserved cancellation from Waiting");
        assert!(matches!(
            cancelled.status(),
            WorkflowStatus::Cancelled { .. }
        ));
    }

    #[test]
    fn settlement_byte_reserve_has_a_finite_hard_boundary() {
        validate_materialization_capacity(
            MAX_WORKFLOW_WORK_JSON_BYTES,
            WorkflowCapacityClass::Work,
        )
        .expect("last work byte");
        assert!(
            validate_materialization_capacity(
                MAX_WORKFLOW_WORK_JSON_BYTES + 1,
                WorkflowCapacityClass::Work,
            )
            .expect_err("work byte overflow")
            .to_string()
            .contains("settlement encoded-byte reserve")
        );
        validate_materialization_capacity(
            MAX_WORKFLOW_WORK_JSON_BYTES + 1,
            WorkflowCapacityClass::Settlement,
        )
        .expect("settlement reserve begins");
        validate_materialization_capacity(
            MAX_WORKFLOW_JSON_BYTES,
            WorkflowCapacityClass::Settlement,
        )
        .expect("last hard byte");
        assert!(
            validate_materialization_capacity(
                MAX_WORKFLOW_JSON_BYTES + 1,
                WorkflowCapacityClass::Settlement,
            )
            .expect_err("hard byte overflow")
            .to_string()
            .contains(&MAX_WORKFLOW_JSON_BYTES.to_string())
        );
    }

    #[test]
    fn settlement_byte_reserve_covers_maximum_supported_recovery_and_failure() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "\\".repeat(256),
                subject: "\\".repeat(256),
            },
            None,
        )
        .expect("maximum actor");
        let mut run = WorkflowRun::new(create(), 10, &authority).expect("run");
        let wait_id = WorkflowWaitId::from_string(escaped_identity('w'));
        run.apply(
            command(
                &escaped_identity('a'),
                WorkflowCommandKind::WaitForSignal {
                    wait_id: wait_id.clone(),
                    name: "event.name".to_owned(),
                    source: "source.name".to_owned(),
                    expires_at_ms: Some(100),
                },
            ),
            20,
            &authority,
        )
        .expect("wait");
        let work_boundary_shape = run.materialization_charge_bytes();
        run.apply(
            command(
                &escaped_identity('d'),
                WorkflowCommandKind::DeliverSignal {
                    wait_id,
                    signal_id: WorkflowSignalId::from_string(escaped_identity('s')),
                    name: "event.name".to_owned(),
                    source: "source.name".to_owned(),
                    idempotency_key: "\\".repeat(MAX_WORKFLOW_IDEMPOTENCY_BYTES),
                },
            ),
            30,
            &authority,
        )
        .expect("maximum recovery transition");
        run.apply(
            largest_escaped_failure_command(&escaped_identity('f')),
            40,
            &authority,
        )
        .expect("maximum terminal transition");
        let reserved_growth = run
            .materialization_charge_bytes()
            .checked_sub(work_boundary_shape)
            .expect("settlement grows the aggregate");
        assert!(reserved_growth <= WORKFLOW_SETTLEMENT_JSON_BYTE_RESERVE);
    }
}
