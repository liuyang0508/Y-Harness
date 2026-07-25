//! Service-safe Task Graph administration and authenticated worker mutations.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    HarnessError, TaskClaim, TaskCompletion, TaskCoordinator, TaskDefinition, TaskGraph,
    TaskGraphId, TaskGraphSnapshot, TaskId, TaskLease, TaskLeaseId, TaskMessage, TaskMessagePage,
    TaskRecord, TaskStatus,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::now_ms,
};

const MAX_TASK_CAS_ATTEMPTS: usize = 64;
const MAX_TASK_CLAIMS: usize = 16;
const MAX_TASK_RECORD_PAGE: usize = 64;
const MAX_TASK_RECORD_PAGE_BYTES: usize = 16_711_680;
const MAX_TASK_LEASE_DURATION_MS: u64 = 604_800_000;

/// Bounded Task Graph metadata safe to return without cloning the full graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskGraphSummary {
    /// Stable Task Graph identity.
    pub graph_id: TaskGraphId,
    /// Current optimistic-concurrency revision.
    pub revision: u64,
    /// Number of Task records.
    pub task_count: u64,
    /// Whether every Task is terminal.
    pub terminal: bool,
    /// Conservative durable materialization charge.
    pub materialization_charge_bytes: u64,
    /// Remaining bytes under the Task Graph authority boundary.
    pub remaining_materialization_bytes: u64,
}

/// Count- and byte-bounded Task record page in Task identity order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskRecordPage {
    /// Owning Task Graph.
    pub graph_id: TaskGraphId,
    /// Revision from which the page was read.
    pub revision: u64,
    /// Task records after the requested identity cursor.
    pub records: Vec<TaskRecord>,
    /// Identity cursor for the next page.
    pub next_after_task_id: Option<TaskId>,
    /// Whether a later Task record exists.
    pub has_more: bool,
}

#[derive(Clone)]
pub(crate) struct TaskProtocolService {
    coordinator: Arc<dyn TaskCoordinator>,
}

impl TaskProtocolService {
    pub(crate) fn new(coordinator: Arc<dyn TaskCoordinator>) -> Self {
        Self { coordinator }
    }

    pub(crate) async fn create(
        &self,
        graph_id: TaskGraphId,
        definitions: Vec<TaskDefinition>,
    ) -> Result<TaskGraphSummary, HarnessError> {
        let graph = TaskGraph::new(definitions)?;
        let snapshot = self.coordinator.create(graph_id, graph).await?;
        summary(&snapshot)
    }

    pub(crate) async fn summary(
        &self,
        graph_id: &TaskGraphId,
    ) -> Result<Option<TaskGraphSummary>, HarnessError> {
        self.coordinator
            .load(graph_id)
            .await?
            .as_ref()
            .map(summary)
            .transpose()
    }

    pub(crate) async fn records(
        &self,
        graph_id: &TaskGraphId,
        after_task_id: Option<&str>,
        limit: usize,
    ) -> Result<TaskRecordPage, HarnessError> {
        if !(1..=MAX_TASK_RECORD_PAGE).contains(&limit) {
            return Err(HarnessError::Protocol(format!(
                "Task record limit must be 1-{MAX_TASK_RECORD_PAGE}"
            )));
        }
        let snapshot = self.load(graph_id).await?;
        let mut records = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut has_more = false;
        for record in snapshot.graph().tasks().filter(|record| {
            after_task_id.is_none_or(|cursor| record.definition.id.as_str() > cursor)
        }) {
            if records.len() == limit {
                has_more = true;
                break;
            }
            let remaining = MAX_TASK_RECORD_PAGE_BYTES.saturating_sub(encoded_bytes);
            let record_bytes = match bounded_serialized_size(record, remaining) {
                Ok(bytes) => bytes,
                Err(BoundedJsonError::LimitExceeded) => {
                    if records.is_empty() {
                        return Err(HarnessError::Protocol(
                            "one Task record exceeds the protocol response budget".to_owned(),
                        ));
                    }
                    has_more = true;
                    break;
                }
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Task record page".to_owned(),
                    ));
                }
            };
            encoded_bytes = encoded_bytes.checked_add(record_bytes).ok_or_else(|| {
                HarnessError::Protocol("Task record page byte count overflow".to_owned())
            })?;
            records.push(record.clone());
        }
        let next_after_task_id = records.last().map(|record| record.definition.id.clone());
        Ok(TaskRecordPage {
            graph_id: graph_id.clone(),
            revision: snapshot.revision(),
            records,
            next_after_task_id,
            has_more,
        })
    }

    pub(crate) async fn claim(
        &self,
        graph_id: &TaskGraphId,
        worker: &str,
        lease_duration_ms: u64,
        maximum: usize,
    ) -> Result<(u64, Vec<TaskClaim>), HarnessError> {
        validate_lease_duration(lease_duration_ms)?;
        if !(1..=MAX_TASK_CLAIMS).contains(&maximum) {
            return Err(HarnessError::Protocol(format!(
                "Task claim maximum must be 1-{MAX_TASK_CLAIMS}"
            )));
        }
        let mut last_conflict = None;
        for _ in 0..MAX_TASK_CAS_ATTEMPTS {
            let mut snapshot = self.load(graph_id).await?;
            let claims =
                snapshot
                    .graph_mut()
                    .claim_ready(worker, now_ms(), lease_duration_ms, maximum)?;
            if claims.is_empty() {
                return Ok((snapshot.revision(), claims));
            }
            match bounded_serialized_size(&claims, MAX_TASK_RECORD_PAGE_BYTES) {
                Ok(_) => {}
                Err(BoundedJsonError::LimitExceeded) => {
                    return Err(HarnessError::Protocol(
                        "Task claims exceed the protocol response budget".to_owned(),
                    ));
                }
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Task claims".to_owned(),
                    ));
                }
            }
            match self.coordinator.compare_and_swap(snapshot).await {
                Ok(saved) => return Ok((saved.revision(), claims)),
                Err(error @ HarnessError::OrchestrationConflict { .. }) => {
                    last_conflict = Some(error);
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            HarnessError::Orchestration(
                "Task claim retry window ended without a Coordinator result".to_owned(),
            )
        }))
    }

    pub(crate) async fn heartbeat(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        lease_duration_ms: u64,
    ) -> Result<(u64, u64), HarnessError> {
        validate_lease_duration(lease_duration_ms)?;
        self.mutate_worker(graph_id, task_id, lease_id, worker, |graph, now, lease| {
            let expires_at_ms = now.checked_add(lease_duration_ms).ok_or_else(|| {
                HarnessError::Protocol("Task heartbeat expiration overflow".to_owned())
            })?;
            graph.heartbeat(task_id, &lease.id, now, expires_at_ms)?;
            Ok(expires_at_ms)
        })
        .await
    }

    pub(crate) async fn complete(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        completion: TaskCompletion,
    ) -> Result<u64, HarnessError> {
        let (revision, ()) = self
            .mutate_worker(graph_id, task_id, lease_id, worker, |graph, now, lease| {
                graph.complete(task_id, &lease.id, now, completion.clone())
            })
            .await?;
        Ok(revision)
    }

    pub(crate) async fn fail(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        reason: String,
    ) -> Result<u64, HarnessError> {
        let (revision, ()) = self
            .mutate_worker(graph_id, task_id, lease_id, worker, |graph, now, lease| {
                graph.fail(task_id, &lease.id, now, reason.clone())
            })
            .await?;
        Ok(revision)
    }

    pub(crate) async fn cancel(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        expected_revision: u64,
        reason: String,
    ) -> Result<u64, HarnessError> {
        let mut snapshot = self.load(graph_id).await?;
        if snapshot.revision() != expected_revision {
            return Err(HarnessError::OrchestrationConflict {
                graph_id: graph_id.clone(),
                expected: expected_revision,
                actual: snapshot.revision(),
            });
        }
        snapshot.graph_mut().cancel(task_id, reason)?;
        Ok(self
            .coordinator
            .compare_and_swap(snapshot)
            .await?
            .revision())
    }

    pub(crate) async fn inbox(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<(u64, TaskMessagePage), HarnessError> {
        let snapshot = self.load(graph_id).await?;
        require_worker_lease(snapshot.graph(), task_id, lease_id, worker, now_ms())?;
        let page = snapshot
            .graph()
            .messages_page_for(task_id, after_sequence, limit)?;
        Ok((snapshot.revision(), page))
    }

    pub(crate) async fn send(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        to: &TaskId,
        body: String,
    ) -> Result<(u64, TaskMessage), HarnessError> {
        self.mutate_worker(graph_id, task_id, lease_id, worker, |graph, now, _lease| {
            graph.send_message(task_id, to, body.clone(), now)
        })
        .await
    }

    async fn mutate_worker<T>(
        &self,
        graph_id: &TaskGraphId,
        task_id: &TaskId,
        lease_id: &TaskLeaseId,
        worker: &str,
        mut mutation: impl FnMut(&mut TaskGraph, u64, &TaskLease) -> Result<T, HarnessError>,
    ) -> Result<(u64, T), HarnessError> {
        let mut last_conflict = None;
        for _ in 0..MAX_TASK_CAS_ATTEMPTS {
            let mut snapshot = self.load(graph_id).await?;
            let now = now_ms();
            let lease =
                require_worker_lease(snapshot.graph(), task_id, lease_id, worker, now)?.clone();
            let output = mutation(snapshot.graph_mut(), now, &lease)?;
            match self.coordinator.compare_and_swap(snapshot).await {
                Ok(saved) => return Ok((saved.revision(), output)),
                Err(error @ HarnessError::OrchestrationConflict { .. }) => {
                    last_conflict = Some(error);
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_conflict.unwrap_or_else(|| {
            HarnessError::Orchestration(
                "Task worker retry window ended without a Coordinator result".to_owned(),
            )
        }))
    }

    async fn load(&self, graph_id: &TaskGraphId) -> Result<TaskGraphSnapshot, HarnessError> {
        self.coordinator
            .load(graph_id)
            .await?
            .ok_or_else(|| HarnessError::Protocol(format!("Task Graph {graph_id} does not exist")))
    }
}

fn summary(snapshot: &TaskGraphSnapshot) -> Result<TaskGraphSummary, HarnessError> {
    Ok(TaskGraphSummary {
        graph_id: snapshot.id().clone(),
        revision: snapshot.revision(),
        task_count: snapshot
            .graph()
            .tasks()
            .count()
            .try_into()
            .map_err(|_| HarnessError::Protocol("Task count exceeds u64".to_owned()))?,
        terminal: snapshot.graph().is_terminal(),
        materialization_charge_bytes: snapshot
            .graph()
            .materialization_charge_bytes()
            .try_into()
            .map_err(|_| {
                HarnessError::Protocol("Task Graph materialization charge exceeds u64".to_owned())
            })?,
        remaining_materialization_bytes: snapshot
            .graph()
            .remaining_materialization_bytes()
            .try_into()
            .map_err(|_| {
                HarnessError::Protocol("Task Graph remaining capacity exceeds u64".to_owned())
            })?,
    })
}

fn require_worker_lease<'a>(
    graph: &'a TaskGraph,
    task_id: &TaskId,
    lease_id: &TaskLeaseId,
    worker: &str,
    now_ms: u64,
) -> Result<&'a TaskLease, HarnessError> {
    graph
        .task(task_id)
        .and_then(|record| match &record.status {
            TaskStatus::Running { lease }
                if &lease.id == lease_id
                    && lease.owner == worker
                    && lease.expires_at_ms > now_ms =>
            {
                Some(lease)
            }
            _ => None,
        })
        .ok_or_else(|| {
            HarnessError::Orchestration(
                "Task lease is not current for the authenticated worker".to_owned(),
            )
        })
}

fn validate_lease_duration(duration_ms: u64) -> Result<(), HarnessError> {
    if !(1..=MAX_TASK_LEASE_DURATION_MS).contains(&duration_ms) {
        return Err(HarnessError::Protocol(format!(
            "Task lease duration must be 1-{MAX_TASK_LEASE_DURATION_MS} milliseconds"
        )));
    }
    Ok(())
}
