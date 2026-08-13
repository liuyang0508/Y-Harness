//! Durable Inbox-Repair worker: drains the State-resident outbox.
//!
//! The worker does not decide what to do; it commits rows in
//! [`crate::state`] (`inbox_repair_outbox`, `inbox_orphan_tombstone`) and
//! delegates the actual cross-domain side effect to a caller-supplied
//! [`InboxRepairExecutor`]. The executor is the only piece that knows
//! about the live Inbox API.

use std::sync::Arc;

use crate::{
    HarnessError, HarnessFuture, InboxRepairMetrics, InboxRepairOpKind, InboxRepairOpStatus,
    InboxRepairRetryPolicy, InboxTombstoneReason,
};

use super::StateEngine;

fn now_ms() -> u64 {
    crate::kernel::now_ms()
}

/// Outcome of one [`InboxRepairWorker::tick`] pass.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InboxRepairTickOutcome {
    /// Total ops the worker attempted this tick.
    pub scanned: u64,
    /// Ops that ended in [`InboxRepairOpStatus::Succeeded`].
    pub succeeded: u64,
    /// Ops that ended in [`InboxRepairOpStatus::Exhausted`].
    pub exhausted: u64,
    /// Ops that remain `pending` after the tick (backoff pushed next attempt out).
    pub retried: u64,
    /// Wall-clock duration of the tick in milliseconds.
    pub duration_ms: u64,
}

/// Successful work item handed to the executor by [`InboxRepairWorker::tick`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboxRepairOpRow {
    /// Stable row identity (used as the worker key when updating status).
    pub op_id: String,
    /// Wait the op targets.
    pub wait_id: crate::AgentLoopWaitId,
    /// Operation kind.
    pub kind: InboxRepairOpKind,
    /// Original immutable payload bytes (opaque to the worker).
    pub payload: Vec<u8>,
    /// Number of times this op has previously been attempted.
    pub attempt_count: u8,
    /// Unix milliseconds when the row was first enqueued.
    pub created_at_ms: u64,
}

/// Outcome of one [`InboxRepairExecutor::execute`] call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InboxRepairExecuteOutcome {
    /// Side effect landed; row can be marked `succeeded`.
    Succeeded,
    /// A retryable error happened; row stays `pending` with backoff.
    Retryable,
    /// The error is non-retryable; row transitions to `exhausted`.
    NonRetryable,
}

/// Pluggable bridge between the State-resident outbox and the live Inbox.
pub trait InboxRepairExecutor: Send + Sync {
    /// Executes one op. The worker calls this synchronously inside a tick.
    fn execute<'a>(&'a self, op: &'a InboxRepairOpRow) -> HarnessFuture<'a, InboxRepairExecuteOutcome>;

    /// Closes one orphan Inbox record by force, used by `orphan_close` ops.
    fn close_orphan<'a>(
        &'a self,
        op: &'a InboxRepairOpRow,
    ) -> HarnessFuture<'a, InboxRepairExecuteOutcome> {
        let _ = op;
        Box::pin(async { Ok(InboxRepairExecuteOutcome::NonRetryable) })
    }
}

/// No-op executor used when no live Inbox bridge is available.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopInboxRepairExecutor;

impl InboxRepairExecutor for NoopInboxRepairExecutor {
    fn execute<'a>(&'a self, op: &'a InboxRepairOpRow) -> HarnessFuture<'a, InboxRepairExecuteOutcome> {
        let _ = op;
        Box::pin(async { Ok(InboxRepairExecuteOutcome::Retryable) })
    }
}

/// Worker that drains the inbox-repair outbox against a caller-supplied executor.
///
/// The worker is intentionally side-effect free except via the executor and
/// the State store; no background tokio task lives here. Higher layers
/// (e.g. `HarnessRuntime`) decide when and how often to call
/// [`Self::tick`] and [`Self::coldstart`].
pub struct InboxRepairWorker {
    state: Arc<StateEngine>,
    executor: Arc<dyn InboxRepairExecutor>,
    retry_policy: InboxRepairRetryPolicy,
}

impl InboxRepairWorker {
    /// Constructs a worker with default retry policy (8 attempts, 100ms→60s).
    pub fn new(state: Arc<StateEngine>, executor: Arc<dyn InboxRepairExecutor>) -> Self {
        Self {
            state,
            executor,
            retry_policy: InboxRepairRetryPolicy::default(),
        }
    }

    /// Replaces the retry policy.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: InboxRepairRetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Bounded coldstart pass; mirrors the spec section 7.1 contract.
    pub async fn coldstart(&self, max_ops: usize) -> Result<InboxRepairTickOutcome, HarnessError> {
        self.tick(max_ops).await
    }

    /// Processes up to `max_ops` pending rows in a single pass.
    ///
    /// The worker reads `pending` rows ordered by `next_attempt_ms` ASC,
    /// executes each via the executor, and updates the row's status, attempt
    /// count, and next-attempt timestamp according to the outcome table from
    /// the inbox-repair spec.
    pub async fn tick(&self, max_ops: usize) -> Result<InboxRepairTickOutcome, HarnessError> {
        let started = now_ms();
        let mut outcome = InboxRepairTickOutcome::default();
        if !self.state.supports_inbox_repair_durability() {
            outcome.duration_ms = now_ms().saturating_sub(started);
            return Ok(outcome);
        }
        let rows = self.fetch_pending_rows(max_ops).await?;
        outcome.scanned = rows.len() as u64;
        for row in rows {
            let outcome_row = match self.executor.execute(&row).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.mark_exhausted(&row.op_id, &format!("executor_error: {error}")).await?;
                    outcome.exhausted += 1;
                    continue;
                }
            };
            match outcome_row {
                InboxRepairExecuteOutcome::Succeeded => {
                    self.mark_succeeded(&row.op_id).await?;
                    outcome.succeeded += 1;
                }
                InboxRepairExecuteOutcome::Retryable => {
                    let new_attempt = row.attempt_count.saturating_add(1);
                    if self.retry_policy.is_exhausted(new_attempt) {
                        self.mark_exhausted(&row.op_id, "retry_budget_exhausted").await?;
                        outcome.exhausted += 1;
                    } else {
                        self.schedule_retry(&row, new_attempt).await?;
                        outcome.retried += 1;
                    }
                }
                InboxRepairExecuteOutcome::NonRetryable => {
                    self.mark_exhausted(&row.op_id, "non_retryable").await?;
                    outcome.exhausted += 1;
                }
            }
        }
        outcome.duration_ms = now_ms().saturating_sub(started);
        Ok(outcome)
    }

    /// Aggregates operational counters from the live State store.
    pub async fn metrics(&self) -> Result<InboxRepairMetrics, HarnessError> {
        if !self.state.supports_inbox_repair_durability() {
            return Ok(InboxRepairMetrics::default());
        }
        self.state.inbox_repair_metrics().await
    }

    /// Pushes one row to `exhausted` because the caller detected a
    /// terminal, non-retryable failure mode.
    pub async fn exhaust_op(
        &self,
        op_id: &str,
        reason: &str,
    ) -> Result<(), HarnessError> {
        self.mark_exhausted(op_id, reason).await
    }

    async fn fetch_pending_rows(
        &self,
        max_ops: usize,
    ) -> Result<Vec<InboxRepairOpRow>, HarnessError> {
        if !self.state.supports_inbox_repair_durability() {
            return Ok(Vec::new());
        }
        self.state.fetch_pending_repair_ops(max_ops).await
    }

    async fn mark_succeeded(&self, op_id: &str) -> Result<(), HarnessError> {
        self.state.update_repair_op_status(op_id, InboxRepairOpStatus::Succeeded, None).await
    }

    async fn mark_exhausted(&self, op_id: &str, reason: &str) -> Result<(), HarnessError> {
        self.state
            .update_repair_op_status(op_id, InboxRepairOpStatus::Exhausted, Some(reason.to_owned()))
            .await
    }

    async fn schedule_retry(
        &self,
        row: &InboxRepairOpRow,
        new_attempt: u8,
    ) -> Result<(), HarnessError> {
        let next_at = self.retry_policy.next_attempt_ms(new_attempt, now_ms());
        self.state
            .reschedule_repair_op(&row.op_id, new_attempt, next_at)
            .await
    }
}

/// Convenience constructors that derive an [`InboxRepairWorker`] from
/// [`StateEngine`] and the runtime's approval handler.
pub fn default_worker(
    state: Arc<StateEngine>,
    executor: Arc<dyn InboxRepairExecutor>,
) -> InboxRepairWorker {
    InboxRepairWorker::new(state, executor)
}

/// Tombstone reason → text bridge used by the runtime when committing
/// State-terminal CAS rows.
#[must_use]
pub fn reason_label(reason: InboxTombstoneReason) -> &'static str {
    match reason {
        InboxTombstoneReason::Settled => "settled",
        InboxTombstoneReason::Cancelled => "cancelled",
        InboxTombstoneReason::Timeout => "timeout",
        InboxTombstoneReason::Denied => "denied",
        InboxTombstoneReason::TerminalFailure => "terminal_failure",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::Mutex,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        AgentLoopWaitId, InboxRepairOpKind, MemoryEventStore, SqliteEventStore,
    };

    /// Counting executor that yields a caller-programmed outcome sequence.
    struct ScriptedExecutor {
        outcomes: Mutex<Vec<InboxRepairExecuteOutcome>>,
        calls: AtomicUsize,
    }

    impl ScriptedExecutor {
        fn new(outcomes: Vec<InboxRepairExecuteOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(outcomes),
                calls: AtomicUsize::new(0),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl InboxRepairExecutor for ScriptedExecutor {
        fn execute<'a>(
            &'a self,
            _op: &'a InboxRepairOpRow,
        ) -> HarnessFuture<'a, InboxRepairExecuteOutcome> {
            let outcomes = self.outcomes.lock().expect("outcomes lock").clone();
            let calls = &self.calls;
            Box::pin(async move {
                let idx = calls.fetch_add(1, Ordering::SeqCst);
                Ok(outcomes
                    .get(idx)
                    .copied()
                    .unwrap_or(InboxRepairExecuteOutcome::NonRetryable))
            })
        }
    }

    async fn scratch_engine(label: &str) -> Arc<StateEngine> {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "y-harness-inbox-repair-{label}-{}.sqlite",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(SqliteEventStore::open(&path).await.expect("open sqlite"));
        Arc::new(StateEngine::new(store))
    }

    #[tokio::test]
    async fn tick_with_no_pending_rows_reports_zero_scanned() {
        let engine = scratch_engine("empty").await;
        let worker = InboxRepairWorker::new(
            engine.clone(),
            Arc::new(NoopInboxRepairExecutor),
        );
        let outcome = worker.tick(16).await.expect("tick");
        assert_eq!(outcome.scanned, 0);
        assert_eq!(outcome.succeeded, 0);
        assert_eq!(outcome.exhausted, 0);
        assert_eq!(outcome.retried, 0);
    }

    #[tokio::test]
    async fn tick_marks_succeeded_when_executor_returns_succeeded() {
        let engine = scratch_engine("succeed").await;
        let wait = AgentLoopWaitId::from_static("wait-succeed");
        let op_id = engine
            .enqueue_repair_op(&wait, InboxRepairOpKind::Submit, b"payload".to_vec())
            .await
            .expect("enqueue");

        let executor = Arc::new(ScriptedExecutor::new(vec![
            InboxRepairExecuteOutcome::Succeeded,
        ]));
        let worker = InboxRepairWorker::new(engine.clone(), executor.clone());
        let outcome = worker.tick(16).await.expect("tick");
        assert_eq!(outcome.scanned, 1);
        assert_eq!(outcome.succeeded, 1);
        assert_eq!(executor.call_count(), 1);

        let metrics = worker.metrics().await.expect("metrics");
        assert_eq!(metrics.pending_ops, 0);
        assert_eq!(metrics.in_flight_ops, 0);
        assert!(op_id.starts_with("op-"));
    }

    #[tokio::test]
    async fn tick_schedules_retry_when_executor_returns_retryable() {
        let engine = scratch_engine("retry").await;
        let wait = AgentLoopWaitId::from_static("wait-retry");
        let _ = engine
            .enqueue_repair_op(&wait, InboxRepairOpKind::Submit, b"payload".to_vec())
            .await
            .expect("enqueue");

        let executor = Arc::new(ScriptedExecutor::new(vec![
            InboxRepairExecuteOutcome::Retryable,
        ]));
        let worker = InboxRepairWorker::new(engine.clone(), executor.clone());
        let outcome = worker.tick(16).await.expect("tick");
        assert_eq!(outcome.scanned, 1);
        assert_eq!(outcome.retried, 1);
        assert_eq!(executor.call_count(), 1);

        let metrics = worker.metrics().await.expect("metrics");
        assert_eq!(metrics.pending_ops, 1);
    }

    #[tokio::test]
    async fn retry_policy_exhaustion_transitions_to_exhausted_after_budget() {
        let engine = scratch_engine("exhaust").await;
        let wait = AgentLoopWaitId::from_static("wait-exhaust");
        let _ = engine
            .enqueue_repair_op(&wait, InboxRepairOpKind::Settle, b"p".to_vec())
            .await
            .expect("enqueue");

        let policy = InboxRepairRetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        };
        let executor = Arc::new(ScriptedExecutor::new(vec![
            InboxRepairExecuteOutcome::Retryable,
            InboxRepairExecuteOutcome::Retryable,
            InboxRepairExecuteOutcome::Retryable,
            InboxRepairExecuteOutcome::Retryable,
        ]));
        let worker = InboxRepairWorker::new(engine.clone(), executor.clone())
            .with_retry_policy(policy);

        let first = worker.tick(16).await.expect("first tick");
        assert_eq!(first.scanned, 1);
        assert_eq!(first.retried, 1);

        let second = worker.tick(16).await.expect("second tick");
        eprintln!("second: scanned={} succeeded={} retried={} exhausted={}", second.scanned, second.succeeded, second.retried, second.exhausted);
        assert_eq!(second.retried, 1);

        let third = worker.tick(16).await.expect("third tick");
        eprintln!("third: scanned={} succeeded={} retried={} exhausted={}", third.scanned, third.succeeded, third.retried, third.exhausted);
        assert_eq!(third.exhausted, 1, "third tick should exhaust");
        assert_eq!(executor.call_count(), 3);

        let metrics = worker.metrics().await.expect("metrics");
        assert_eq!(metrics.pending_ops, 0);
        assert_eq!(metrics.exhausted_ops, 1);
    }

    #[tokio::test]
    async fn non_retryable_outcome_short_circuits_to_exhausted() {
        let engine = scratch_engine("nonretry").await;
        let wait = AgentLoopWaitId::from_static("wait-nonretry");
        let _ = engine
            .enqueue_repair_op(&wait, InboxRepairOpKind::Submit, b"p".to_vec())
            .await
            .expect("enqueue");

        let executor = Arc::new(ScriptedExecutor::new(vec![
            InboxRepairExecuteOutcome::NonRetryable,
        ]));
        let worker = InboxRepairWorker::new(engine.clone(), executor.clone());
        let outcome = worker.tick(16).await.expect("tick");
        assert_eq!(outcome.exhausted, 1);
        let metrics = worker.metrics().await.expect("metrics");
        assert_eq!(metrics.exhausted_ops, 1);
    }

    #[tokio::test]
    async fn memory_backend_reports_empty_metrics_and_zero_scan() {
        let engine = Arc::new(StateEngine::new(Arc::new(MemoryEventStore::new())));
        let worker = InboxRepairWorker::new(engine.clone(), Arc::new(NoopInboxRepairExecutor));
        let outcome = worker.tick(16).await.expect("memory tick");
        assert_eq!(outcome.scanned, 0);
        let metrics = worker.metrics().await.expect("memory metrics");
        assert_eq!(metrics.pending_ops, 0);
        assert!(!engine.supports_inbox_repair_durability());
    }

    #[test]
    fn retry_policy_backoff_grows_then_caps() {
        let policy = InboxRepairRetryPolicy {
            max_attempts: 8,
            initial_backoff_ms: 100,
            max_backoff_ms: 60_000,
        };
        assert_eq!(policy.next_attempt_ms(0, 0), 100);
        assert_eq!(policy.next_attempt_ms(1, 0), 200);
        assert_eq!(policy.next_attempt_ms(2, 0), 400);
        assert_eq!(policy.next_attempt_ms(3, 0), 800);
        assert_eq!(policy.next_attempt_ms(20, 0), 60_000);

        assert!(!policy.is_exhausted(0));
        assert!(!policy.is_exhausted(7));
        assert!(policy.is_exhausted(8));
        assert!(policy.is_exhausted(100));
    }
}
