//! Deterministic Task DAG scheduling, fenced leases, messages, and artifacts.

mod coordinator;
mod migration;
mod runner;
mod workspace;

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};

use crate::{
    ArtifactId, HarnessError, TaskId, TaskLeaseId, TaskMessageId, kernel::validate_capability_name,
};

pub use coordinator::{
    MemoryTaskCoordinator, SqliteTaskCoordinator, TASK_GRAPH_SCHEMA_VERSION, TaskCoordinator,
    TaskGraphSnapshot,
};
pub use migration::{TaskMigrationReport, TaskMigrationStatus};
pub use runner::{Orchestrator, TaskExecutionRequest, TaskExecutor, TaskMailbox};
pub use workspace::{
    DenyWorkspaceProvider, GitWorktreeWorkspaceProvider, LocalDirectoryWorkspaceProvider,
    TaskWorkspace, WORKSPACE_PROVIDER_API_VERSION, WorkspaceDisposition, WorkspaceLease,
    WorkspaceProvider, WorkspaceProviderDescriptor, WorkspaceProvisioning, WorkspaceRequest,
};

const MAX_TASKS: usize = 10_000;
const MAX_CLAIMS_PER_BATCH: usize = 64;
const MAX_DEPENDENCIES_PER_TASK: usize = 1_024;
const MAX_ARTIFACTS_PER_COMPLETION: usize = 1_024;
const MAX_MESSAGES: usize = 100_000;
const MAX_MESSAGE_PAGE_ITEMS: usize = 256;
const MAX_MESSAGE_PAGE_BYTES: usize = 2_097_152;
pub(crate) const MAX_TASK_GRAPH_JSON_BYTES: usize = 67_108_864;
const TASK_GRAPH_BASE_CHARGE_BYTES: usize = 1_024;
// Pending/Running records retain room for the largest dependency-block reason.
const ACTIVE_TASK_TERMINAL_RESERVE_BYTES: usize = 1_024;
const MAX_TASK_TEXT_BYTES: usize = 65_536;
const MAX_MESSAGE_BYTES: usize = 65_536;
const MAX_URI_BYTES: usize = 4_096;

/// Workspace isolation requested from a later execution-environment broker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceMode {
    /// No filesystem workspace is required.
    None,
    /// A writable workspace isolated from sibling Tasks is required.
    Isolated,
    /// A shared workspace may be mounted read-only.
    SharedReadOnly,
}

/// Immutable Task definition inside one dependency graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskDefinition {
    /// Stable Task identity.
    pub id: TaskId,
    /// Human-readable work description.
    pub description: String,
    /// Required predecessor Tasks.
    pub dependencies: BTreeSet<TaskId>,
    /// Larger values are claimed first; Task identity breaks ties.
    pub priority: i32,
    /// Required workspace isolation.
    pub workspace: WorkspaceMode,
}

/// Fenced ownership grant for one running Task attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskLease {
    /// Unique fencing token required for every settlement.
    pub id: TaskLeaseId,
    /// Stable worker or sub-Agent identity.
    pub owner: String,
    /// Monotonic Task-local attempt number.
    pub attempt: u32,
    /// Caller-supplied clock deadline in Unix milliseconds.
    pub expires_at_ms: u64,
}

/// Immutable Artifact reference produced by a completed Task.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskArtifact {
    /// Stable Artifact identity.
    pub id: ArtifactId,
    /// Producer Task.
    pub producer: TaskId,
    /// External blob, file, or object-store locator.
    pub uri: String,
    /// Lowercase SHA-256 digest of referenced content.
    pub content_sha256: String,
    /// Declared media type.
    pub media_type: String,
    /// Declared byte size.
    pub size_bytes: u64,
}

/// Successful Task output retained by orchestration state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskCompletion {
    /// Bounded human-readable result summary.
    pub summary: String,
    /// Immutable output references.
    pub artifacts: Vec<TaskArtifact>,
}

/// Current Task lifecycle state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TaskStatus {
    /// Waiting for dependencies or an available worker.
    Pending,
    /// Owned by one current fenced lease.
    Running {
        /// Current ownership grant.
        lease: TaskLease,
    },
    /// Settled successfully.
    Completed {
        /// Validated output.
        completion: TaskCompletion,
    },
    /// Worker reported an unrecoverable attempt failure.
    Failed {
        /// Bounded failure reason.
        reason: String,
    },
    /// Operator or parent orchestration cancelled the Task.
    Cancelled {
        /// Bounded cancellation reason.
        reason: String,
    },
    /// An upstream terminal failure made this Task unschedulable.
    Blocked {
        /// Deterministic dependency explanation.
        reason: String,
    },
}

/// Mutable Task projection inside a graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRecord {
    /// Immutable definition.
    pub definition: TaskDefinition,
    /// Current lifecycle state.
    pub status: TaskStatus,
    /// Number of leases ever issued.
    pub attempts: u32,
}

/// Work handed to one worker with the required fencing lease.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskClaim {
    /// Claimed immutable definition.
    pub task: TaskDefinition,
    /// Current ownership grant.
    pub lease: TaskLease,
}

/// Ordered message passed between Tasks or their workers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskMessage {
    /// Stable message identity.
    pub id: TaskMessageId,
    /// Graph-local total ordering sequence.
    pub sequence: u64,
    /// Sending Task.
    pub from: TaskId,
    /// Receiving Task.
    pub to: TaskId,
    /// Bounded message body.
    pub body: String,
    /// Caller-supplied Unix timestamp.
    pub created_at_ms: u64,
}

/// Bounded ordered inbox page for one Task.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskMessagePage {
    /// Messages after the requested cursor in graph sequence order.
    pub messages: Vec<TaskMessage>,
    /// Sequence of the final returned message, if any.
    pub next_after_sequence: Option<u64>,
    /// Whether a later matching message exists.
    pub has_more: bool,
}

/// Pure, serializable orchestration aggregate.
///
/// A host must persist mutations atomically for multi-process use. Lease tokens
/// fence stale workers even when a previous attempt finishes late.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TaskGraph {
    tasks: BTreeMap<TaskId, TaskRecord>,
    messages: Vec<TaskMessage>,
    next_message_sequence: u64,
    #[serde(skip)]
    materialization_charge_bytes: usize,
}

#[derive(Deserialize)]
struct TaskGraphWire {
    tasks: BTreeMap<TaskId, TaskRecord>,
    messages: Vec<TaskMessage>,
    next_message_sequence: u64,
}

struct TaskMutation {
    id: TaskId,
    status: TaskStatus,
    attempts: u32,
}

#[derive(Serialize)]
struct TaskRecordView<'a> {
    definition: &'a TaskDefinition,
    status: &'a TaskStatus,
    attempts: u32,
}

impl<'de> Deserialize<'de> for TaskGraph {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TaskGraphWire::deserialize(deserializer)?;
        let mut graph = Self {
            tasks: wire.tasks,
            messages: wire.messages,
            next_message_sequence: wire.next_message_sequence,
            materialization_charge_bytes: 0,
        };
        graph.materialization_charge_bytes = graph
            .calculate_materialization_charge()
            .map_err(D::Error::custom)?;
        Ok(graph)
    }
}

impl TaskGraph {
    /// Validates dependencies and constructs a pending acyclic graph.
    pub fn new(definitions: Vec<TaskDefinition>) -> Result<Self, HarnessError> {
        if definitions.len() > MAX_TASKS {
            return Err(HarnessError::Orchestration(format!(
                "Task Graph exceeds {MAX_TASKS} Tasks"
            )));
        }
        let mut tasks = BTreeMap::new();
        for definition in definitions {
            validate_task_definition(&definition)?;
            if tasks.contains_key(&definition.id) {
                return Err(HarnessError::Orchestration(format!(
                    "duplicate Task {}",
                    definition.id
                )));
            }
            tasks.insert(
                definition.id.clone(),
                TaskRecord {
                    definition,
                    status: TaskStatus::Pending,
                    attempts: 0,
                },
            );
        }
        for record in tasks.values() {
            for dependency in &record.definition.dependencies {
                if dependency == &record.definition.id {
                    return Err(HarnessError::Orchestration(format!(
                        "Task {} depends on itself",
                        record.definition.id
                    )));
                }
                if !tasks.contains_key(dependency) {
                    return Err(HarnessError::Orchestration(format!(
                        "Task {} depends on missing Task {dependency}",
                        record.definition.id
                    )));
                }
            }
        }
        validate_acyclic(&tasks)?;
        let mut graph = Self {
            tasks,
            messages: Vec::new(),
            next_message_sequence: 1,
            materialization_charge_bytes: 0,
        };
        graph.materialization_charge_bytes = graph.calculate_materialization_charge()?;
        Ok(graph)
    }

    /// Looks up one Task projection.
    #[must_use]
    pub fn task(&self, id: &TaskId) -> Option<&TaskRecord> {
        self.tasks.get(id)
    }

    /// Returns every Task projection in identity order.
    pub fn tasks(&self) -> impl Iterator<Item = &TaskRecord> {
        self.tasks.values()
    }

    /// Returns whether every Task has reached a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.tasks.values().all(|record| {
            matches!(
                record.status,
                TaskStatus::Completed { .. }
                    | TaskStatus::Failed { .. }
                    | TaskStatus::Cancelled { .. }
                    | TaskStatus::Blocked { .. }
            )
        })
    }

    /// Returns the conservative encoded materialization charge.
    #[must_use]
    pub fn materialization_charge_bytes(&self) -> usize {
        self.materialization_charge_bytes
    }

    /// Returns capacity remaining under the durable Task Graph boundary.
    #[must_use]
    pub fn remaining_materialization_bytes(&self) -> usize {
        MAX_TASK_GRAPH_JSON_BYTES.saturating_sub(self.materialization_charge_bytes)
    }

    /// Returns currently schedulable Task identities in claim order.
    #[must_use]
    pub fn ready(&self) -> Vec<TaskId> {
        let mut ready = self
            .tasks
            .values()
            .filter(|record| {
                record.status == TaskStatus::Pending
                    && record.definition.dependencies.iter().all(|dependency| {
                        self.tasks.get(dependency).is_some_and(|dependency| {
                            matches!(dependency.status, TaskStatus::Completed { .. })
                        })
                    })
            })
            .map(|record| (record.definition.priority, record.definition.id.clone()))
            .collect::<Vec<_>>();
        ready.sort_by(|(left_priority, left_id), (right_priority, right_id)| {
            right_priority
                .cmp(left_priority)
                .then_with(|| left_id.cmp(right_id))
        });
        ready.into_iter().map(|(_, id)| id).collect()
    }

    /// Releases expired leases, propagates blocked dependencies, and claims work.
    pub fn claim_ready(
        &mut self,
        owner: &str,
        now_ms: u64,
        lease_duration_ms: u64,
        maximum: usize,
    ) -> Result<Vec<TaskClaim>, HarnessError> {
        validate_capability_name("worker", owner)?;
        if lease_duration_ms == 0 || !(1..=MAX_CLAIMS_PER_BATCH).contains(&maximum) {
            return Err(HarnessError::Orchestration(format!(
                "lease duration must be positive and maximum claims must be 1-{MAX_CLAIMS_PER_BATCH}"
            )));
        }
        let expires_at_ms = now_ms.checked_add(lease_duration_ms).ok_or_else(|| {
            HarnessError::Orchestration("Task lease expiration overflow".to_owned())
        })?;
        for record in self.tasks.values() {
            let can_enter_pending = match &record.status {
                TaskStatus::Pending => true,
                TaskStatus::Running { lease } => lease.expires_at_ms <= now_ms,
                TaskStatus::Completed { .. }
                | TaskStatus::Failed { .. }
                | TaskStatus::Cancelled { .. }
                | TaskStatus::Blocked { .. } => false,
            };
            let claimable_after_maintenance = can_enter_pending
                && record.definition.dependencies.iter().all(|dependency| {
                    self.tasks.get(dependency).is_some_and(|dependency| {
                        matches!(dependency.status, TaskStatus::Completed { .. })
                    })
                });
            if claimable_after_maintenance && record.attempts == u32::MAX {
                return Err(HarnessError::Orchestration(format!(
                    "Task {} attempt counter is exhausted",
                    record.definition.id
                )));
            }
        }
        let expired = self
            .tasks
            .values()
            .filter_map(|record| {
                matches!(
                    &record.status,
                    TaskStatus::Running { lease } if lease.expires_at_ms <= now_ms
                )
                .then(|| record.definition.id.clone())
            })
            .collect::<BTreeSet<_>>();
        let blocked = self.blocked_mutations(&expired, None);
        let blocked_ids = blocked
            .iter()
            .map(|mutation| mutation.id.clone())
            .collect::<BTreeSet<_>>();
        let mut ready = self
            .tasks
            .values()
            .filter(|record| {
                !blocked_ids.contains(&record.definition.id)
                    && (record.status == TaskStatus::Pending
                        || expired.contains(&record.definition.id))
                    && record.definition.dependencies.iter().all(|dependency| {
                        self.tasks.get(dependency).is_some_and(|dependency| {
                            matches!(dependency.status, TaskStatus::Completed { .. })
                        })
                    })
            })
            .map(|record| (record.definition.priority, record.definition.id.clone()))
            .collect::<Vec<_>>();
        ready.sort_by(|(left_priority, left_id), (right_priority, right_id)| {
            right_priority
                .cmp(left_priority)
                .then_with(|| left_id.cmp(right_id))
        });
        ready.truncate(maximum);
        let selected = ready
            .iter()
            .map(|(_, task_id)| task_id.clone())
            .collect::<BTreeSet<_>>();

        let mut mutations = blocked;
        for task_id in expired.difference(&selected) {
            if blocked_ids.contains(task_id) {
                continue;
            }
            let record = self.tasks.get(task_id).ok_or_else(|| {
                HarnessError::Orchestration(format!("expired Task {task_id} disappeared"))
            })?;
            mutations.push(TaskMutation {
                id: task_id.clone(),
                status: TaskStatus::Pending,
                attempts: record.attempts,
            });
        }

        let mut claims = Vec::with_capacity(ready.len());
        for (_, task_id) in ready {
            let record = self.tasks.get(&task_id).ok_or_else(|| {
                HarnessError::Orchestration(format!("ready Task {task_id} disappeared"))
            })?;
            let attempts = record.attempts.checked_add(1).ok_or_else(|| {
                HarnessError::Orchestration(format!("Task {task_id} attempt overflow"))
            })?;
            let lease = TaskLease {
                id: TaskLeaseId::generate(),
                owner: owner.to_owned(),
                attempt: attempts,
                expires_at_ms,
            };
            mutations.push(TaskMutation {
                id: task_id.clone(),
                status: TaskStatus::Running {
                    lease: lease.clone(),
                },
                attempts,
            });
            claims.push(TaskClaim {
                task: record.definition.clone(),
                lease,
            });
        }
        self.apply_task_mutations(mutations)?;
        Ok(claims)
    }

    /// Extends the current lease while preserving its fencing token.
    pub fn heartbeat(
        &mut self,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        now_ms: u64,
        expires_at_ms: u64,
    ) -> Result<(), HarnessError> {
        let mut lease = current_lease(&self.tasks, task_id, lease_id, now_ms)?.clone();
        if expires_at_ms <= now_ms || expires_at_ms <= lease.expires_at_ms {
            return Err(HarnessError::Orchestration(
                "heartbeat must extend the lease into the future".to_owned(),
            ));
        }
        lease.expires_at_ms = expires_at_ms;
        let attempts = self
            .tasks
            .get(task_id)
            .ok_or_else(|| HarnessError::Orchestration(format!("Task {task_id} does not exist")))?
            .attempts;
        self.apply_task_mutations(vec![TaskMutation {
            id: task_id.clone(),
            status: TaskStatus::Running { lease },
            attempts,
        }])
    }

    /// Settles a Task only when the current unexpired fencing token matches.
    pub fn complete(
        &mut self,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        now_ms: u64,
        completion: TaskCompletion,
    ) -> Result<(), HarnessError> {
        validate_completion(task_id, &completion)?;
        current_lease(&self.tasks, task_id, lease_id, now_ms)?;
        let attempts = self
            .tasks
            .get(task_id)
            .ok_or_else(|| HarnessError::Orchestration(format!("Task {task_id} does not exist")))?
            .attempts;
        self.apply_task_mutations(vec![TaskMutation {
            id: task_id.clone(),
            status: TaskStatus::Completed { completion },
            attempts,
        }])
    }

    /// Fails a Task only when the current unexpired fencing token matches.
    pub fn fail(
        &mut self,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        now_ms: u64,
        reason: impl Into<String>,
    ) -> Result<(), HarnessError> {
        let reason = reason.into();
        validate_task_text("Task failure", &reason)?;
        current_lease(&self.tasks, task_id, lease_id, now_ms)?;
        let attempts = self
            .tasks
            .get(task_id)
            .ok_or_else(|| HarnessError::Orchestration(format!("Task {task_id} does not exist")))?
            .attempts;
        let pending_overrides = BTreeSet::new();
        let mut mutations = vec![TaskMutation {
            id: task_id.clone(),
            status: TaskStatus::Failed { reason },
            attempts,
        }];
        mutations.extend(self.blocked_mutations(&pending_overrides, Some(task_id)));
        self.apply_task_mutations(mutations)
    }

    /// Cancels a non-terminal Task and fences any running worker immediately.
    pub fn cancel(
        &mut self,
        task_id: &TaskId,
        reason: impl Into<String>,
    ) -> Result<(), HarnessError> {
        let reason = reason.into();
        validate_task_text("Task cancellation", &reason)?;
        let record = self
            .tasks
            .get(task_id)
            .ok_or_else(|| HarnessError::Orchestration(format!("Task {task_id} does not exist")))?;
        if matches!(
            record.status,
            TaskStatus::Completed { .. }
                | TaskStatus::Failed { .. }
                | TaskStatus::Cancelled { .. }
                | TaskStatus::Blocked { .. }
        ) {
            return Err(HarnessError::Orchestration(format!(
                "Task {task_id} is already terminal"
            )));
        }
        let attempts = record.attempts;
        let pending_overrides = BTreeSet::new();
        let mut mutations = vec![TaskMutation {
            id: task_id.clone(),
            status: TaskStatus::Cancelled { reason },
            attempts,
        }];
        mutations.extend(self.blocked_mutations(&pending_overrides, Some(task_id)));
        self.apply_task_mutations(mutations)
    }

    /// Sends one bounded, ordered message between existing Tasks.
    pub fn send_message(
        &mut self,
        from: &TaskId,
        to: &TaskId,
        body: impl Into<String>,
        created_at_ms: u64,
    ) -> Result<TaskMessage, HarnessError> {
        if self.messages.len() >= MAX_MESSAGES {
            return Err(HarnessError::Orchestration(format!(
                "Task Graph exceeds {MAX_MESSAGES} messages"
            )));
        }
        if !self.tasks.contains_key(from) || !self.tasks.contains_key(to) {
            return Err(HarnessError::Orchestration(
                "Task messages require existing sender and receiver".to_owned(),
            ));
        }
        let body = body.into();
        if body.trim().is_empty() || body.len() > MAX_MESSAGE_BYTES {
            return Err(HarnessError::Orchestration(format!(
                "Task message must be 1-{MAX_MESSAGE_BYTES} bytes"
            )));
        }
        let sequence = self.next_message_sequence;
        let next_message_sequence = self.next_message_sequence.checked_add(1).ok_or_else(|| {
            HarnessError::Orchestration("Task message sequence overflow".to_owned())
        })?;
        let message = TaskMessage {
            id: TaskMessageId::generate(),
            sequence,
            from: from.clone(),
            to: to.clone(),
            body,
            created_at_ms,
        };
        let next_charge = checked_graph_charge_add(
            self.materialization_charge_bytes,
            message_materialization_charge(&message)?,
        )?;
        validate_graph_charge(next_charge)?;
        self.next_message_sequence = next_message_sequence;
        self.messages.push(message.clone());
        self.materialization_charge_bytes = next_charge;
        Ok(message)
    }

    /// Returns receiver messages in graph-local sequence order.
    #[must_use]
    pub fn messages_for(&self, task_id: &TaskId) -> Vec<&TaskMessage> {
        self.messages
            .iter()
            .filter(|message| &message.to == task_id)
            .collect()
    }

    /// Returns a count- and byte-bounded inbox page after one graph sequence.
    pub fn messages_page_for(
        &self,
        task_id: &TaskId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<TaskMessagePage, HarnessError> {
        if !self.tasks.contains_key(task_id) {
            return Err(HarnessError::Orchestration(format!(
                "Task {task_id} does not exist"
            )));
        }
        if !(1..=MAX_MESSAGE_PAGE_ITEMS).contains(&limit) {
            return Err(HarnessError::Orchestration(format!(
                "Task message page limit must be 1-{MAX_MESSAGE_PAGE_ITEMS}"
            )));
        }
        let mut messages = Vec::new();
        let mut bytes = 0_usize;
        let mut has_more = false;
        for message in self
            .messages
            .iter()
            .filter(|message| &message.to == task_id && message.sequence > after_sequence)
        {
            let next_bytes =
                checked_graph_charge_add(bytes, message_materialization_charge(message)?)?;
            if messages.len() == limit || next_bytes > MAX_MESSAGE_PAGE_BYTES {
                has_more = true;
                break;
            }
            bytes = next_bytes;
            messages.push(message.clone());
        }
        let next_after_sequence = messages.last().map(|message| message.sequence);
        Ok(TaskMessagePage {
            messages,
            next_after_sequence,
            has_more,
        })
    }

    /// Requeues expired running Tasks and returns their identities.
    pub fn release_expired(&mut self, now_ms: u64) -> Result<Vec<TaskId>, HarnessError> {
        let mut mutations = Vec::new();
        for record in self.tasks.values() {
            if matches!(
                &record.status,
                TaskStatus::Running { lease } if lease.expires_at_ms <= now_ms
            ) {
                mutations.push(TaskMutation {
                    id: record.definition.id.clone(),
                    status: TaskStatus::Pending,
                    attempts: record.attempts,
                });
            }
        }
        let released = mutations
            .iter()
            .map(|mutation| mutation.id.clone())
            .collect();
        self.apply_task_mutations(mutations)?;
        Ok(released)
    }

    fn blocked_mutations(
        &self,
        pending_overrides: &BTreeSet<TaskId>,
        additional_failed: Option<&TaskId>,
    ) -> Vec<TaskMutation> {
        let mut failed = self
            .tasks
            .values()
            .filter(|record| {
                matches!(
                    record.status,
                    TaskStatus::Failed { .. }
                        | TaskStatus::Cancelled { .. }
                        | TaskStatus::Blocked { .. }
                )
            })
            .map(|record| record.definition.id.clone())
            .collect::<BTreeSet<_>>();
        if let Some(task_id) = additional_failed {
            failed.insert(task_id.clone());
        }
        let mut mutations = Vec::new();
        loop {
            let newly_blocked = self
                .tasks
                .values()
                .filter(|record| !failed.contains(&record.definition.id))
                .filter(|record| {
                    record.status == TaskStatus::Pending
                        || pending_overrides.contains(&record.definition.id)
                })
                .filter_map(|record| {
                    record
                        .definition
                        .dependencies
                        .iter()
                        .find(|dependency| failed.contains(*dependency))
                        .map(|dependency| TaskMutation {
                            id: record.definition.id.clone(),
                            status: TaskStatus::Blocked {
                                reason: format!("dependency {dependency} did not complete"),
                            },
                            attempts: record.attempts,
                        })
                })
                .collect::<Vec<_>>();
            if newly_blocked.is_empty() {
                return mutations;
            }
            for mutation in &newly_blocked {
                failed.insert(mutation.id.clone());
            }
            mutations.extend(newly_blocked);
        }
    }

    fn calculate_materialization_charge(&self) -> Result<usize, HarnessError> {
        let mut total = TASK_GRAPH_BASE_CHARGE_BYTES;
        for (task_id, record) in &self.tasks {
            total = checked_graph_charge_add(total, task_materialization_charge(task_id, record)?)?;
        }
        for message in &self.messages {
            total = checked_graph_charge_add(total, message_materialization_charge(message)?)?;
        }
        validate_graph_charge(total)?;
        Ok(total)
    }

    fn apply_task_mutations(&mut self, mutations: Vec<TaskMutation>) -> Result<(), HarnessError> {
        let mut seen = BTreeSet::new();
        let mut next_charge = self.materialization_charge_bytes;
        for mutation in &mutations {
            if !seen.insert(mutation.id.clone()) {
                return Err(HarnessError::Orchestration(format!(
                    "duplicate Task mutation {}",
                    mutation.id
                )));
            }
            let current = self.tasks.get(&mutation.id).ok_or_else(|| {
                HarnessError::Orchestration(format!("Task {} does not exist", mutation.id))
            })?;
            let current_charge = task_materialization_charge(&mutation.id, current)?;
            let candidate_charge = task_materialization_charge_parts(
                &mutation.id,
                &current.definition,
                &mutation.status,
                mutation.attempts,
            )?;
            next_charge = next_charge
                .checked_sub(current_charge)
                .ok_or_else(|| {
                    HarnessError::Orchestration(
                        "Task Graph materialization charge is inconsistent".to_owned(),
                    )
                })
                .and_then(|charge| checked_graph_charge_add(charge, candidate_charge))?;
        }
        validate_graph_charge(next_charge)?;

        for mutation in mutations {
            // Every key was checked above and Task mutations never change keys.
            if let Some(record) = self.tasks.get_mut(&mutation.id) {
                record.status = mutation.status;
                record.attempts = mutation.attempts;
            }
        }
        self.materialization_charge_bytes = next_charge;
        Ok(())
    }

    fn validate_integrity(&self) -> Result<(), HarnessError> {
        if self.tasks.len() > MAX_TASKS {
            return Err(HarnessError::Orchestration(format!(
                "Task Graph exceeds {MAX_TASKS} Tasks"
            )));
        }
        let definitions = self
            .tasks
            .iter()
            .map(|(id, record)| {
                if id != &record.definition.id {
                    return Err(HarnessError::Orchestration(format!(
                        "Task map key {id} does not match definition {}",
                        record.definition.id
                    )));
                }
                Ok(record.definition.clone())
            })
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(definitions)?;

        for record in self.tasks.values() {
            match &record.status {
                TaskStatus::Pending => {}
                TaskStatus::Running { lease } => {
                    validate_capability_name("worker", &lease.owner)?;
                    if record.attempts == 0 || lease.attempt != record.attempts {
                        return Err(HarnessError::Orchestration(format!(
                            "Task {} lease attempt does not match attempt counter",
                            record.definition.id
                        )));
                    }
                }
                TaskStatus::Completed { completion } => {
                    validate_completion(&record.definition.id, completion)?;
                }
                TaskStatus::Failed { reason }
                | TaskStatus::Cancelled { reason }
                | TaskStatus::Blocked { reason } => {
                    validate_task_text("Task terminal reason", reason)?;
                }
            }
        }

        if self.messages.len() > MAX_MESSAGES {
            return Err(HarnessError::Orchestration(format!(
                "Task Graph exceeds {MAX_MESSAGES} messages"
            )));
        }
        let mut message_ids = BTreeSet::new();
        for (index, message) in self.messages.iter().enumerate() {
            let expected = u64::try_from(index)
                .unwrap_or(u64::MAX)
                .checked_add(1)
                .ok_or_else(|| {
                    HarnessError::Orchestration("Task message sequence overflow".to_owned())
                })?;
            if message.sequence != expected {
                return Err(HarnessError::Orchestration(
                    "Task message sequence is not contiguous".to_owned(),
                ));
            }
            if !message_ids.insert(message.id.clone()) {
                return Err(HarnessError::Orchestration(format!(
                    "duplicate Task message {}",
                    message.id
                )));
            }
            if !self.tasks.contains_key(&message.from) || !self.tasks.contains_key(&message.to) {
                return Err(HarnessError::Orchestration(
                    "Task message references a missing Task".to_owned(),
                ));
            }
            if message.body.trim().is_empty() || message.body.len() > MAX_MESSAGE_BYTES {
                return Err(HarnessError::Orchestration(format!(
                    "Task message must be 1-{MAX_MESSAGE_BYTES} bytes"
                )));
            }
        }
        let expected_next = u64::try_from(self.messages.len())
            .unwrap_or(u64::MAX)
            .checked_add(1)
            .ok_or_else(|| {
                HarnessError::Orchestration("Task message sequence overflow".to_owned())
            })?;
        if self.next_message_sequence != expected_next {
            return Err(HarnessError::Orchestration(
                "Task next message sequence is inconsistent".to_owned(),
            ));
        }
        let calculated_charge = self.calculate_materialization_charge()?;
        if calculated_charge != self.materialization_charge_bytes {
            return Err(HarnessError::Orchestration(
                "Task Graph materialization charge is inconsistent".to_owned(),
            ));
        }
        Ok(())
    }
}

fn task_materialization_charge(
    task_id: &TaskId,
    record: &TaskRecord,
) -> Result<usize, HarnessError> {
    task_materialization_charge_parts(task_id, &record.definition, &record.status, record.attempts)
}

fn task_materialization_charge_parts(
    task_id: &TaskId,
    definition: &TaskDefinition,
    status: &TaskStatus,
    attempts: u32,
) -> Result<usize, HarnessError> {
    let encoded = serde_json::to_vec(&(
        task_id,
        TaskRecordView {
            definition,
            status,
            attempts,
        },
    ))
    .map_err(|error| HarnessError::Orchestration(format!("encode Task record: {error}")))?;
    let reserve = usize::from(matches!(
        status,
        TaskStatus::Pending | TaskStatus::Running { .. }
    ))
    .checked_mul(ACTIVE_TASK_TERMINAL_RESERVE_BYTES)
    .ok_or_else(|| HarnessError::Orchestration("Task charge overflow".to_owned()))?;
    encoded
        .len()
        .checked_add(reserve)
        .ok_or_else(|| HarnessError::Orchestration("Task charge overflow".to_owned()))
}

fn message_materialization_charge(message: &TaskMessage) -> Result<usize, HarnessError> {
    serde_json::to_vec(message)
        .map_err(|error| HarnessError::Orchestration(format!("encode Task message: {error}")))?
        .len()
        .checked_add(1)
        .ok_or_else(|| HarnessError::Orchestration("Task message charge overflow".to_owned()))
}

fn checked_graph_charge_add(left: usize, right: usize) -> Result<usize, HarnessError> {
    left.checked_add(right)
        .ok_or_else(|| HarnessError::Orchestration("Task Graph charge overflow".to_owned()))
}

fn validate_graph_charge(charge: usize) -> Result<(), HarnessError> {
    if charge > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "Task Graph materialization charge exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn current_lease<'a>(
    tasks: &'a BTreeMap<TaskId, TaskRecord>,
    task_id: &TaskId,
    lease_id: &TaskLeaseId,
    now_ms: u64,
) -> Result<&'a TaskLease, HarnessError> {
    let record = tasks
        .get(task_id)
        .ok_or_else(|| HarnessError::Orchestration(format!("Task {task_id} does not exist")))?;
    let TaskStatus::Running { lease } = &record.status else {
        return Err(HarnessError::Orchestration(format!(
            "Task {task_id} is not running"
        )));
    };
    if &lease.id != lease_id {
        return Err(HarnessError::Orchestration(format!(
            "Task {task_id} lease token is stale"
        )));
    }
    if lease.expires_at_ms <= now_ms {
        return Err(HarnessError::Orchestration(format!(
            "Task {task_id} lease expired"
        )));
    }
    Ok(lease)
}

fn validate_task_definition(definition: &TaskDefinition) -> Result<(), HarnessError> {
    validate_task_id(&definition.id)?;
    if definition.dependencies.len() > MAX_DEPENDENCIES_PER_TASK {
        return Err(HarnessError::Orchestration(format!(
            "Task {} exceeds {MAX_DEPENDENCIES_PER_TASK} dependencies",
            definition.id
        )));
    }
    for dependency in &definition.dependencies {
        validate_task_id(dependency)?;
    }
    validate_task_text("Task description", &definition.description)
}

fn validate_task_id(task_id: &TaskId) -> Result<(), HarnessError> {
    if task_id.as_str().is_empty()
        || task_id.as_str().len() > 256
        || task_id.as_str().chars().any(char::is_control)
    {
        return Err(HarnessError::Orchestration(
            "Task identity must be 1-256 non-control bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_task_text(field: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > MAX_TASK_TEXT_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "{field} must be 1-{MAX_TASK_TEXT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_completion(task_id: &TaskId, completion: &TaskCompletion) -> Result<(), HarnessError> {
    validate_task_text("Task completion summary", &completion.summary)?;
    if completion.artifacts.len() > MAX_ARTIFACTS_PER_COMPLETION {
        return Err(HarnessError::Orchestration(format!(
            "Task completion exceeds {MAX_ARTIFACTS_PER_COMPLETION} Artifacts"
        )));
    }
    let mut artifact_ids = BTreeSet::new();
    for artifact in &completion.artifacts {
        if &artifact.producer != task_id {
            return Err(HarnessError::Orchestration(format!(
                "Artifact {} producer does not match Task {task_id}",
                artifact.id
            )));
        }
        if !artifact_ids.insert(artifact.id.clone()) {
            return Err(HarnessError::Orchestration(format!(
                "duplicate Artifact {}",
                artifact.id
            )));
        }
        if artifact.uri.trim().is_empty() || artifact.uri.len() > MAX_URI_BYTES {
            return Err(HarnessError::Orchestration(format!(
                "Artifact {} URI must be 1-{MAX_URI_BYTES} bytes",
                artifact.id
            )));
        }
        if artifact.media_type.trim().is_empty() || artifact.media_type.len() > 255 {
            return Err(HarnessError::Orchestration(format!(
                "Artifact {} media type is invalid",
                artifact.id
            )));
        }
        if artifact.content_sha256.len() != 64
            || !artifact
                .content_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(HarnessError::Orchestration(format!(
                "Artifact {} digest must be lowercase SHA-256",
                artifact.id
            )));
        }
    }
    Ok(())
}

fn validate_acyclic(tasks: &BTreeMap<TaskId, TaskRecord>) -> Result<(), HarnessError> {
    fn visit(
        id: &TaskId,
        tasks: &BTreeMap<TaskId, TaskRecord>,
        visiting: &mut BTreeSet<TaskId>,
        visited: &mut BTreeSet<TaskId>,
    ) -> Result<(), HarnessError> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.clone()) {
            return Err(HarnessError::Orchestration(format!(
                "Task dependency cycle includes {id}"
            )));
        }
        let record = tasks.get(id).ok_or_else(|| {
            HarnessError::Orchestration(format!("Task {id} disappeared during validation"))
        })?;
        for dependency in &record.definition.dependencies {
            visit(dependency, tasks, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id.clone());
        Ok(())
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for id in tasks.keys() {
        visit(id, tasks, &mut visiting, &mut visited)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{TaskCompletion, TaskDefinition, TaskGraph, TaskStatus, WorkspaceMode};
    use crate::{HarnessError, TaskId};

    fn task(id: &'static str, dependencies: &[&'static str], priority: i32) -> TaskDefinition {
        TaskDefinition {
            id: TaskId::from_static(id),
            description: format!("work for {id}"),
            dependencies: dependencies
                .iter()
                .map(|dependency| TaskId::from_static(dependency))
                .collect::<BTreeSet<_>>(),
            priority,
            workspace: WorkspaceMode::Isolated,
        }
    }

    #[test]
    fn validates_dag_and_claims_in_dependency_priority_order() {
        let mut graph = TaskGraph::new(vec![
            task("task-root", &[], 0),
            task("task-low", &["task-root"], 1),
            task("task-high", &["task-root"], 10),
        ])
        .expect("graph");
        let root = graph
            .claim_ready("worker-1", 100, 50, 2)
            .expect("claim root");
        assert_eq!(root.len(), 1);
        assert_eq!(root[0].task.id, TaskId::from_static("task-root"));
        graph
            .complete(
                &root[0].task.id,
                &root[0].lease.id,
                120,
                TaskCompletion {
                    summary: "done".to_owned(),
                    artifacts: Vec::new(),
                },
            )
            .expect("complete root");

        let children = graph
            .claim_ready("worker-1", 121, 50, 2)
            .expect("claim children");
        assert_eq!(
            children
                .iter()
                .map(|claim| claim.task.id.clone())
                .collect::<Vec<_>>(),
            [
                TaskId::from_static("task-high"),
                TaskId::from_static("task-low")
            ]
        );
    }

    #[test]
    fn rejects_missing_dependencies_and_cycles() {
        assert!(TaskGraph::new(vec![task("task-a", &["task-missing"], 0)]).is_err());
        assert!(
            TaskGraph::new(vec![
                task("task-a", &["task-b"], 0),
                task("task-b", &["task-a"], 0)
            ])
            .is_err()
        );
    }

    #[test]
    fn expired_or_stale_worker_cannot_settle_a_new_attempt() {
        let task_id = TaskId::from_static("task-a");
        let mut graph = TaskGraph::new(vec![task("task-a", &[], 0)]).expect("graph");
        let first = graph.claim_ready("worker-1", 100, 10, 1).expect("claim")[0].clone();
        let second = graph.claim_ready("worker-2", 110, 10, 1).expect("reclaim")[0].clone();
        assert_ne!(first.lease.id, second.lease.id);
        assert_eq!(second.lease.attempt, 2);

        let stale = graph.complete(
            &task_id,
            &first.lease.id,
            111,
            TaskCompletion {
                summary: "late".to_owned(),
                artifacts: Vec::new(),
            },
        );
        assert!(matches!(stale, Err(HarnessError::Orchestration(_))));
        graph
            .complete(
                &task_id,
                &second.lease.id,
                111,
                TaskCompletion {
                    summary: "current".to_owned(),
                    artifacts: Vec::new(),
                },
            )
            .expect("current worker");
    }

    #[test]
    fn invalid_claim_batch_never_releases_an_expired_lease() {
        let mut graph = TaskGraph::new(vec![task("task-a", &[], 0)]).expect("graph");
        graph
            .claim_ready("worker-1", 100, 10, 1)
            .expect("initial claim");
        let before = graph.clone();
        graph
            .claim_ready("worker-2", 111, 10, super::MAX_CLAIMS_PER_BATCH + 1)
            .expect_err("oversized claim batch");
        assert_eq!(graph, before);
    }

    #[test]
    fn exhausted_claim_attempt_never_partially_mutates_the_graph() {
        let mut graph =
            TaskGraph::new(vec![task("task-a", &[], 0), task("task-b", &[], 0)]).expect("graph");
        graph
            .tasks
            .get_mut(&TaskId::from_static("task-b"))
            .expect("task")
            .attempts = u32::MAX;
        let before = graph.clone();
        graph
            .claim_ready("worker", 100, 10, 2)
            .expect_err("attempt counter exhausted");
        assert_eq!(graph, before);
    }

    #[test]
    fn materialization_charge_is_rebuilt_and_dominates_serialized_json() {
        let mut graph =
            TaskGraph::new(vec![task("task-a", &[], 0), task("task-b", &["task-a"], 0)])
                .expect("graph");
        graph
            .send_message(
                &TaskId::from_static("task-a"),
                &TaskId::from_static("task-b"),
                "bounded message",
                1,
            )
            .expect("message");
        let encoded = serde_json::to_vec(&graph).expect("encode graph");
        assert!(encoded.len() <= graph.materialization_charge_bytes);
        let decoded: TaskGraph = serde_json::from_slice(&encoded).expect("decode graph");
        assert_eq!(decoded, graph);
        decoded.validate_integrity().expect("valid accounting");
    }

    #[test]
    fn capacity_rejection_never_appends_a_task_message() {
        let mut graph =
            TaskGraph::new(vec![task("task-a", &[], 0), task("task-b", &[], 0)]).expect("graph");
        let message = super::TaskMessage {
            id: crate::TaskMessageId::from_static("message"),
            sequence: 1,
            from: TaskId::from_static("task-a"),
            to: TaskId::from_static("task-b"),
            body: "message".to_owned(),
            created_at_ms: 1,
        };
        let message_charge =
            super::message_materialization_charge(&message).expect("message charge");
        graph.materialization_charge_bytes = super::MAX_TASK_GRAPH_JSON_BYTES - message_charge + 1;
        let before = graph.clone();
        graph
            .send_message(
                &TaskId::from_static("task-a"),
                &TaskId::from_static("task-b"),
                "message",
                1,
            )
            .expect_err("capacity");
        assert_eq!(graph, before);
    }

    #[test]
    fn active_task_charge_reserves_the_largest_block_reason() {
        let dependency = TaskId::from_string("\\".repeat(256));
        let task_id = TaskId::from_static("dependant");
        let definition = TaskDefinition {
            id: task_id.clone(),
            description: "bounded dependant".to_owned(),
            dependencies: [dependency.clone()].into_iter().collect(),
            priority: 0,
            workspace: WorkspaceMode::Isolated,
        };
        let pending = super::TaskRecord {
            definition: definition.clone(),
            status: TaskStatus::Pending,
            attempts: 0,
        };
        let blocked = super::TaskRecord {
            definition,
            status: TaskStatus::Blocked {
                reason: format!("dependency {dependency} did not complete"),
            },
            attempts: 0,
        };
        assert!(
            super::task_materialization_charge(&task_id, &blocked).expect("blocked charge")
                <= super::task_materialization_charge(&task_id, &pending).expect("pending charge")
        );
    }

    #[test]
    fn failure_blocks_transitive_dependants_and_messages_are_ordered() {
        let mut graph = TaskGraph::new(vec![
            task("task-a", &[], 0),
            task("task-b", &["task-a"], 0),
            task("task-c", &["task-b"], 0),
        ])
        .expect("graph");
        graph
            .send_message(
                &TaskId::from_static("task-a"),
                &TaskId::from_static("task-b"),
                "first",
                1,
            )
            .expect("first message");
        graph
            .send_message(
                &TaskId::from_static("task-c"),
                &TaskId::from_static("task-b"),
                "second",
                2,
            )
            .expect("second message");
        let claim = graph.claim_ready("worker", 10, 10, 1).expect("claim")[0].clone();
        graph
            .fail(&claim.task.id, &claim.lease.id, 11, "failed")
            .expect("fail");

        assert!(matches!(
            graph
                .task(&TaskId::from_static("task-b"))
                .expect("task b")
                .status,
            TaskStatus::Blocked { .. }
        ));
        assert!(matches!(
            graph
                .task(&TaskId::from_static("task-c"))
                .expect("task c")
                .status,
            TaskStatus::Blocked { .. }
        ));
        assert_eq!(
            graph
                .messages_for(&TaskId::from_static("task-b"))
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        let first_page = graph
            .messages_page_for(&TaskId::from_static("task-b"), 0, 1)
            .expect("first message page");
        assert_eq!(first_page.messages[0].sequence, 1);
        assert_eq!(first_page.next_after_sequence, Some(1));
        assert!(first_page.has_more);
        let second_page = graph
            .messages_page_for(
                &TaskId::from_static("task-b"),
                first_page.next_after_sequence.expect("cursor"),
                1,
            )
            .expect("second message page");
        assert_eq!(second_page.messages[0].sequence, 2);
        assert!(!second_page.has_more);
    }
}
