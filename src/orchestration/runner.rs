//! Bounded execution of durable, fenced Task Graph claims.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::Arc,
    time::Duration,
};

use tokio::{
    task::JoinSet,
    time::{Instant, sleep, sleep_until},
};

use super::{
    DenyWorkspaceProvider, TaskClaim, TaskCompletion, TaskCoordinator, TaskGraph,
    TaskGraphSnapshot, TaskMessage, TaskMessagePage, TaskStatus, TaskWorkspace,
    WorkspaceDisposition, WorkspaceLease, WorkspaceProvider, WorkspaceProviderDescriptor,
    WorkspaceRequest,
    workspace::{validate_provider_descriptor, validate_workspace_lease},
};
use crate::{
    AuthorityContext, CancellationToken, ExecutionBinding, HarnessError, HarnessFuture,
    TaskGraphId, TaskId,
    isolation::isolate_future,
    kernel::{capture_capability_metadata, now_ms, validate_capability_name},
};

const DEFAULT_MAX_CONCURRENCY: usize = 8;
const MAX_ORCHESTRATOR_CONCURRENCY: usize = 64;
const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(240);
const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(300);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const MAX_TASK_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_LEASE_DURATION: Duration = Duration::from_secs(604_800);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CAS_ATTEMPTS: usize = 64;
const EXECUTOR_ERROR_BYTES: usize = 4_096;
const WORKSPACE_PREPARATION_DRAIN_GRACE: Duration = Duration::from_secs(1);
const WORKSPACE_RELEASE_TIMEOUT: Duration = Duration::from_secs(4);
const WORKSPACE_CLEANUP_BUDGET: Duration = Duration::from_secs(5);
const CANCELLATION_DRAIN_GRACE: Duration = Duration::from_secs(6);

/// Input supplied to one claimed Task executor.
#[derive(Clone, Debug)]
pub struct TaskExecutionRequest {
    /// Durable graph containing the claim.
    pub graph_id: TaskGraphId,
    /// Immutable Task definition and current fencing lease.
    pub claim: TaskClaim,
    /// Lease-fenced, coordinator-backed Task message access.
    pub mailbox: TaskMailbox,
    /// Validated workspace provisioned for this exact Task attempt.
    pub workspace: TaskWorkspace,
    /// Cooperative signal cancelled on timeout, fencing, or scheduler stop.
    pub cancellation: CancellationToken,
}

/// Lease-fenced message access for one running Task attempt.
#[derive(Clone)]
pub struct TaskMailbox {
    coordinator: Arc<dyn TaskCoordinator>,
    graph_id: TaskGraphId,
    claim: TaskClaim,
    authority: AuthorityContext,
    cancellation: CancellationToken,
}

impl TaskMailbox {
    /// Reads one bounded inbox page after a graph-local message sequence.
    pub async fn inbox(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<TaskMessagePage, HarnessError> {
        self.require_active()?;
        let snapshot = self.load_graph().await?;
        if !claim_is_current(snapshot.graph(), &self.claim, now_ms()) {
            return Err(mailbox_fenced());
        }
        snapshot
            .graph()
            .messages_page_for(&self.claim.task.id, after_sequence, limit)
    }

    /// Sends one durable bounded message from the current Task attempt.
    pub async fn send(
        &self,
        to: &TaskId,
        body: impl Into<String>,
    ) -> Result<TaskMessage, HarnessError> {
        let body = body.into();
        for _ in 0..MAX_CAS_ATTEMPTS {
            self.require_active()?;
            let mut snapshot = self.load_graph().await?;
            if !claim_is_current(snapshot.graph(), &self.claim, now_ms()) {
                return Err(mailbox_fenced());
            }
            let message = snapshot.graph_mut().send_message(
                &self.claim.task.id,
                to,
                body.clone(),
                now_ms(),
            )?;
            match self
                .coordinator
                .compare_and_swap_as(snapshot, &self.authority)
                .await
            {
                Ok(_) => return Ok(message),
                Err(HarnessError::OrchestrationConflict { .. }) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(HarnessError::Orchestration(
            "Task message contention exceeded the bounded retry window".to_owned(),
        ))
    }

    fn require_active(&self) -> Result<(), HarnessError> {
        if self.cancellation.is_cancelled() {
            Err(HarnessError::Orchestration(
                "Task mailbox is cancelled".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    async fn load_graph(&self) -> Result<TaskGraphSnapshot, HarnessError> {
        self.coordinator
            .load_as(&self.graph_id, &self.authority)
            .await?
            .ok_or_else(|| {
                HarnessError::Orchestration(format!("Task Graph {} does not exist", self.graph_id))
            })
    }
}

impl fmt::Debug for TaskMailbox {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskMailbox")
            .field("graph_id", &self.graph_id)
            .field("task_id", &self.claim.task.id)
            .field("lease_id", &self.claim.lease.id)
            .finish_non_exhaustive()
    }
}

/// Host-provided sub-Agent or Task execution capability.
///
/// The Orchestrator provisions and validates [`TaskExecutionRequest::workspace`]
/// before entry and releases it after the Future settles. Returning success
/// does not bypass workspace cleanup or fenced settlement in the Task
/// Coordinator. In-process implementations are trusted; untrusted executors
/// must be placed behind a governed Process Broker.
pub trait TaskExecutor: Send + Sync {
    /// Executes one current claim and returns its bounded durable completion.
    fn execute<'a>(&'a self, request: TaskExecutionRequest) -> HarnessFuture<'a, TaskCompletion>;
}

/// Concurrent scheduler for one durable Task Graph.
pub struct Orchestrator {
    coordinator: Arc<dyn TaskCoordinator>,
    executor: Arc<dyn TaskExecutor>,
    worker: String,
    max_concurrency: usize,
    task_timeout: Duration,
    lease_duration: Duration,
    poll_interval: Duration,
    workspace_provider: Arc<dyn WorkspaceProvider>,
    workspace_descriptor: WorkspaceProviderDescriptor,
    authority: AuthorityContext,
    execution_binding: Option<ExecutionBinding>,
}

impl Orchestrator {
    /// Creates a scheduler with eight workers, four-minute Task timeouts, and
    /// five-minute leases.
    pub fn new(
        coordinator: Arc<dyn TaskCoordinator>,
        executor: Arc<dyn TaskExecutor>,
        worker: impl Into<String>,
    ) -> Result<Self, HarnessError> {
        let worker = worker.into();
        validate_capability_name("worker", &worker)?;
        let workspace_provider: Arc<dyn WorkspaceProvider> = Arc::new(DenyWorkspaceProvider);
        let workspace_descriptor = workspace_provider.descriptor();
        Ok(Self {
            coordinator,
            executor,
            worker,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            task_timeout: DEFAULT_TASK_TIMEOUT,
            lease_duration: DEFAULT_LEASE_DURATION,
            poll_interval: DEFAULT_POLL_INTERVAL,
            workspace_provider,
            workspace_descriptor,
            authority: AuthorityContext::local_process(),
            execution_binding: None,
        })
    }

    /// Installs the trusted tenant authority and optional governed execution
    /// coordinate used for every claim created by this scheduler.
    ///
    /// Embedding hosts must derive both values from their authenticated control
    /// plane rather than from Task-authored data.
    pub fn with_execution_context(
        mut self,
        authority: AuthorityContext,
        execution_binding: Option<ExecutionBinding>,
    ) -> Result<Self, HarnessError> {
        authority.validate_current("Orchestrator authority")?;
        if let Some(binding) = &execution_binding {
            binding.validate()?;
            if binding.tenant_id() != authority.tenant_id() {
                return Err(HarnessError::InvalidConfiguration(
                    "Orchestrator execution binding tenant does not match its trusted authority"
                        .to_owned(),
                ));
            }
        }
        self.authority = authority;
        self.execution_binding = execution_binding;
        Ok(self)
    }

    /// Installs one frozen Workspace Provider for all claims executed by this
    /// scheduler.
    pub fn with_workspace_provider(
        mut self,
        provider: Arc<dyn WorkspaceProvider>,
    ) -> Result<Self, HarnessError> {
        let descriptor =
            capture_capability_metadata("workspace provider descriptor", || provider.descriptor())?;
        validate_provider_descriptor(&descriptor)?;
        self.workspace_provider = provider;
        self.workspace_descriptor = descriptor;
        Ok(self)
    }

    /// Sets the maximum claims executed concurrently by this scheduler.
    pub fn with_concurrency_limit(mut self, limit: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_ORCHESTRATOR_CONCURRENCY).contains(&limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Orchestrator concurrency must be 1-{MAX_ORCHESTRATOR_CONCURRENCY}"
            )));
        }
        self.max_concurrency = limit;
        Ok(self)
    }

    /// Sets per-Task timeout, fencing lease duration, and coordinator poll
    /// interval.
    ///
    /// The lease must outlive the Task timeout so a timely result has a valid
    /// settlement window.
    pub fn with_timing(
        mut self,
        task_timeout: Duration,
        lease_duration: Duration,
        poll_interval: Duration,
    ) -> Result<Self, HarnessError> {
        validate_duration("Task timeout", task_timeout, MAX_TASK_TIMEOUT)?;
        validate_duration("Task lease", lease_duration, MAX_LEASE_DURATION)?;
        validate_duration(
            "Orchestrator poll interval",
            poll_interval,
            MAX_POLL_INTERVAL,
        )?;
        let task_timeout_ms = duration_millis(task_timeout)?;
        let release_timeout_ms = duration_millis(WORKSPACE_CLEANUP_BUDGET)?;
        let required_lease_ms =
            task_timeout_ms
                .checked_add(release_timeout_ms)
                .ok_or_else(|| {
                    HarnessError::InvalidConfiguration(
                        "Task timeout and Workspace release budget overflow".to_owned(),
                    )
                })?;
        if duration_millis(lease_duration)? <= required_lease_ms {
            return Err(HarnessError::InvalidConfiguration(
                "Task lease duration must exceed the Task timeout plus the five-second Workspace release budget at millisecond precision".to_owned(),
            ));
        }
        self.task_timeout = task_timeout;
        self.lease_duration = lease_duration;
        self.poll_interval = poll_interval;
        Ok(self)
    }

    /// Claims and executes work until the graph is terminal or cancellation is
    /// requested.
    ///
    /// Execution failures become durable Task failures. A result whose lease
    /// was replaced, expired, or cancelled is discarded without settlement.
    pub async fn run(
        &self,
        graph_id: &TaskGraphId,
        cancellation: CancellationToken,
    ) -> Result<TaskGraphSnapshot, HarnessError> {
        let mut workers = JoinSet::new();
        let mut claims = HashMap::new();
        let mut worker_cancellations = BTreeMap::new();

        loop {
            if cancellation.is_cancelled() {
                cancel_workers(&worker_cancellations, &mut workers).await;
                return Err(orchestration_cancelled());
            }

            let snapshot = self.load_graph(graph_id).await?;
            cancel_fenced_workers(&snapshot, &claims, &worker_cancellations);
            if snapshot.graph().is_terminal() {
                cancel_workers(&worker_cancellations, &mut workers).await;
                return Ok(snapshot);
            }

            let available = self.max_concurrency.saturating_sub(claims.len());
            if available > 0 {
                let claimed = self
                    .claim_available(graph_id, available, &cancellation)
                    .await?;
                for claim in claimed {
                    self.spawn_claim(
                        &mut workers,
                        &mut claims,
                        &mut worker_cancellations,
                        graph_id.clone(),
                        claim,
                    );
                }
            }

            if workers.is_empty() {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        return Err(orchestration_cancelled());
                    }
                    () = sleep(self.poll_interval) => {}
                }
                continue;
            }

            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    cancel_workers(&worker_cancellations, &mut workers).await;
                    return Err(orchestration_cancelled());
                }
                joined = workers.join_next_with_id() => {
                    let Some(joined) = joined else {
                        return Err(HarnessError::Orchestration(
                            "Orchestrator lost its active worker set".to_owned(),
                        ));
                    };
                    let (task_handle_id, outcome) = match joined {
                        Ok((task_handle_id, outcome)) => (task_handle_id, outcome),
                        Err(error) => {
                            let task_handle_id = error.id();
                            let outcome = if error.is_panic() {
                                ExecutorOutcome::Failed(
                                    "Task executor panicked".to_owned(),
                                )
                            } else {
                                ExecutorOutcome::Failed(
                                    "Task executor stopped unexpectedly".to_owned(),
                                )
                            };
                            (task_handle_id, outcome)
                        }
                    };
                    let claim = claims.remove(&task_handle_id).ok_or_else(|| {
                        HarnessError::Orchestration(
                            "Orchestrator lost a worker claim".to_owned(),
                        )
                    })?;
                    worker_cancellations.remove(&claim.task.id);
                    if !matches!(outcome, ExecutorOutcome::Cancelled) {
                        self.settle(graph_id, &claim, &outcome, &cancellation)
                            .await?;
                    }
                }
                () = sleep(self.poll_interval) => {}
            }
        }
    }

    fn spawn_claim(
        &self,
        workers: &mut JoinSet<ExecutorOutcome>,
        claims: &mut HashMap<tokio::task::Id, TaskClaim>,
        cancellations: &mut BTreeMap<TaskId, CancellationToken>,
        graph_id: TaskGraphId,
        claim: TaskClaim,
    ) {
        let scheduler_cancellation = CancellationToken::new();
        let execution = ClaimExecution {
            coordinator: self.coordinator.clone(),
            executor: self.executor.clone(),
            workspace_provider: self.workspace_provider.clone(),
            workspace_descriptor: self.workspace_descriptor.clone(),
            authority: self.authority.clone(),
            graph_id: graph_id.clone(),
            claim: claim.clone(),
            timeout: self.task_timeout,
        };
        let worker_cancellation = scheduler_cancellation.clone();
        let handle =
            workers.spawn(async move { execute_claim(execution, worker_cancellation).await });
        claims.insert(handle.id(), claim.clone());
        cancellations.insert(claim.task.id, scheduler_cancellation);
    }

    async fn load_graph(&self, graph_id: &TaskGraphId) -> Result<TaskGraphSnapshot, HarnessError> {
        self.coordinator
            .load_as(graph_id, &self.authority)
            .await?
            .ok_or_else(|| {
                HarnessError::Orchestration(format!("Task Graph {graph_id} does not exist"))
            })
    }

    async fn claim_available(
        &self,
        graph_id: &TaskGraphId,
        maximum: usize,
        cancellation: &CancellationToken,
    ) -> Result<Vec<TaskClaim>, HarnessError> {
        let maximum = maximum.min(MAX_ORCHESTRATOR_CONCURRENCY);
        let lease_duration_ms = duration_millis(self.lease_duration)?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            if cancellation.is_cancelled() {
                return Err(orchestration_cancelled());
            }
            let mut snapshot = self.load_graph(graph_id).await?;
            if snapshot.graph().is_terminal() {
                return Ok(Vec::new());
            }
            let claimed = snapshot.graph_mut().claim_ready_with_binding(
                &self.worker,
                now_ms(),
                lease_duration_ms,
                maximum,
                self.execution_binding.as_ref(),
            )?;
            if claimed.is_empty() {
                return Ok(claimed);
            }
            match self
                .coordinator
                .compare_and_swap_as(snapshot, &self.authority)
                .await
            {
                Ok(_) => return Ok(claimed),
                Err(HarnessError::OrchestrationConflict { .. }) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(HarnessError::Orchestration(
            "Task claim contention exceeded the bounded retry window".to_owned(),
        ))
    }

    async fn settle(
        &self,
        graph_id: &TaskGraphId,
        claim: &TaskClaim,
        outcome: &ExecutorOutcome,
        cancellation: &CancellationToken,
    ) -> Result<(), HarnessError> {
        for _ in 0..MAX_CAS_ATTEMPTS {
            if cancellation.is_cancelled() {
                return Err(orchestration_cancelled());
            }
            let mut snapshot = self.load_graph(graph_id).await?;
            let now = now_ms();
            if !claim_is_current(snapshot.graph(), claim, now) {
                return Ok(());
            }
            let mutation = match outcome {
                ExecutorOutcome::Completed(completion) => snapshot.graph_mut().complete(
                    &claim.task.id,
                    &claim.lease.id,
                    now,
                    completion.clone(),
                ),
                ExecutorOutcome::Failed(reason) => {
                    snapshot
                        .graph_mut()
                        .fail(&claim.task.id, &claim.lease.id, now, reason.clone())
                }
                ExecutorOutcome::TimedOut => snapshot.graph_mut().fail(
                    &claim.task.id,
                    &claim.lease.id,
                    now,
                    "Task executor timed out",
                ),
                ExecutorOutcome::Cancelled => return Ok(()),
            };
            if let Err(error) = mutation {
                let reason = bounded_reason(&format!(
                    "Task executor returned an invalid completion: {error}"
                ));
                snapshot
                    .graph_mut()
                    .fail(&claim.task.id, &claim.lease.id, now, reason)?;
            }
            match self
                .coordinator
                .compare_and_swap_as(snapshot, &self.authority)
                .await
            {
                Ok(_) => return Ok(()),
                Err(HarnessError::OrchestrationConflict { .. }) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(HarnessError::Orchestration(
            "Task settlement contention exceeded the bounded retry window".to_owned(),
        ))
    }
}

enum ExecutorOutcome {
    Completed(TaskCompletion),
    Failed(String),
    TimedOut,
    Cancelled,
}

struct ClaimExecution {
    coordinator: Arc<dyn TaskCoordinator>,
    executor: Arc<dyn TaskExecutor>,
    workspace_provider: Arc<dyn WorkspaceProvider>,
    workspace_descriptor: WorkspaceProviderDescriptor,
    authority: AuthorityContext,
    graph_id: TaskGraphId,
    claim: TaskClaim,
    timeout: Duration,
}

async fn execute_claim(
    execution: ClaimExecution,
    scheduler_cancellation: CancellationToken,
) -> ExecutorOutcome {
    let deadline = Instant::now() + execution.timeout;
    let workspace_request = WorkspaceRequest {
        graph_id: execution.graph_id.clone(),
        task_id: execution.claim.task.id.clone(),
        lease_id: execution.claim.lease.id.clone(),
        attempt: execution.claim.lease.attempt,
        mode: execution.claim.task.workspace,
    };
    let workspace_lease = match prepare_workspace(
        execution.workspace_provider.clone(),
        workspace_request.clone(),
        scheduler_cancellation.clone(),
        deadline,
    )
    .await
    {
        Ok(lease) => lease,
        Err(outcome) => return outcome,
    };

    let workspace = match validate_workspace_lease(
        &workspace_lease,
        &workspace_request,
        &execution.workspace_descriptor,
    )
    .await
    {
        Ok(workspace) => workspace,
        Err(error) => {
            let _ = release_workspace(
                execution.workspace_provider,
                workspace_lease,
                WorkspaceDisposition::Failed,
            )
            .await;
            return ExecutorOutcome::Failed(bounded_reason(&format!(
                "Workspace Provider returned an invalid lease: {error}"
            )));
        }
    };

    let task_cancellation = CancellationToken::new();
    let mailbox = TaskMailbox {
        coordinator: execution.coordinator,
        graph_id: execution.graph_id.clone(),
        claim: execution.claim.clone(),
        authority: execution.authority,
        cancellation: task_cancellation.clone(),
    };
    let request = TaskExecutionRequest {
        graph_id: execution.graph_id,
        claim: execution.claim,
        mailbox,
        workspace,
        cancellation: task_cancellation.clone(),
    };
    let outcome = execute_task(
        execution.executor,
        request,
        scheduler_cancellation,
        task_cancellation.clone(),
        deadline,
    )
    .await;
    task_cancellation.cancel();
    let disposition = match outcome {
        ExecutorOutcome::Completed(_) => WorkspaceDisposition::Completed,
        ExecutorOutcome::Failed(_) => WorkspaceDisposition::Failed,
        ExecutorOutcome::TimedOut => WorkspaceDisposition::TimedOut,
        ExecutorOutcome::Cancelled => WorkspaceDisposition::Cancelled,
    };
    match release_workspace(execution.workspace_provider, workspace_lease, disposition).await {
        Ok(()) => outcome,
        Err(error) => merge_workspace_release_failure(outcome, &error),
    }
}

async fn prepare_workspace(
    provider: Arc<dyn WorkspaceProvider>,
    request: WorkspaceRequest,
    scheduler_cancellation: CancellationToken,
    deadline: Instant,
) -> Result<WorkspaceLease, ExecutorOutcome> {
    let preparation_cancellation = CancellationToken::new();
    let preparation = match isolate_future(
        || provider.prepare(request, preparation_cancellation.clone()),
        None,
    ) {
        Ok(preparation) => preparation,
        Err(()) => {
            return Err(ExecutorOutcome::Failed(
                "Workspace Provider panicked".to_owned(),
            ));
        }
    };
    tokio::pin!(preparation);
    tokio::select! {
        biased;
        () = scheduler_cancellation.cancelled() => {
            preparation_cancellation.cancel();
            if let Ok(Ok(Ok(lease))) = tokio::time::timeout(
                WORKSPACE_PREPARATION_DRAIN_GRACE,
                &mut preparation,
            ).await {
                let _ = release_workspace(
                    provider.clone(),
                    lease,
                    WorkspaceDisposition::Cancelled,
                ).await;
            }
            Err(ExecutorOutcome::Cancelled)
        }
        () = sleep_until(deadline) => {
            preparation_cancellation.cancel();
            if let Ok(Ok(Ok(lease))) = tokio::time::timeout(
                WORKSPACE_PREPARATION_DRAIN_GRACE,
                &mut preparation,
            ).await {
                let _ = release_workspace(
                    provider.clone(),
                    lease,
                    WorkspaceDisposition::TimedOut,
                ).await;
            }
            Err(ExecutorOutcome::TimedOut)
        }
        result = &mut preparation => match result {
            Ok(Ok(lease)) => Ok(lease),
            Ok(Err(error)) => Err(ExecutorOutcome::Failed(bounded_reason(
                &format!("Workspace preparation failed: {error}")
            ))),
            Err(()) => Err(ExecutorOutcome::Failed(
                "Workspace Provider panicked".to_owned()
            )),
        }
    }
}

async fn execute_task(
    executor: Arc<dyn TaskExecutor>,
    request: TaskExecutionRequest,
    scheduler_cancellation: CancellationToken,
    task_cancellation: CancellationToken,
    deadline: Instant,
) -> ExecutorOutcome {
    let execution = match isolate_future(
        || executor.execute(request),
        Some(task_cancellation.clone()),
    ) {
        Ok(execution) => execution,
        Err(()) => return ExecutorOutcome::Failed("Task executor panicked".to_owned()),
    };
    tokio::pin!(execution);
    tokio::select! {
        biased;
        () = scheduler_cancellation.cancelled() => {
            task_cancellation.cancel();
            ExecutorOutcome::Cancelled
        }
        () = sleep_until(deadline) => {
            task_cancellation.cancel();
            ExecutorOutcome::TimedOut
        }
        result = &mut execution => match result {
            Ok(Ok(completion)) => ExecutorOutcome::Completed(completion),
            Ok(Err(error)) => ExecutorOutcome::Failed(bounded_reason(&error.to_string())),
            Err(()) => ExecutorOutcome::Failed("Task executor panicked".to_owned()),
        }
    }
}

async fn release_workspace(
    provider: Arc<dyn WorkspaceProvider>,
    lease: WorkspaceLease,
    disposition: WorkspaceDisposition,
) -> Result<(), HarnessError> {
    let cancellation = CancellationToken::new();
    let release = isolate_future(
        || provider.release(lease, disposition, cancellation.clone()),
        None,
    )
    .map_err(|()| HarnessError::Execution("Workspace Provider panicked".to_owned()))?;
    tokio::pin!(release);
    tokio::select! {
        result = &mut release => result.map_err(|()| {
            HarnessError::Execution("Workspace Provider panicked".to_owned())
        })?,
        () = sleep(WORKSPACE_RELEASE_TIMEOUT) => {
            cancellation.cancel();
            Err(HarnessError::Execution(
                "Workspace release timed out".to_owned()
            ))
        }
    }
}

fn merge_workspace_release_failure(
    outcome: ExecutorOutcome,
    error: &HarnessError,
) -> ExecutorOutcome {
    match outcome {
        ExecutorOutcome::Completed(_) => ExecutorOutcome::Failed(bounded_reason(&format!(
            "Workspace release failed: {error}"
        ))),
        ExecutorOutcome::Failed(reason) => ExecutorOutcome::Failed(bounded_reason(&format!(
            "{reason}; Workspace release also failed: {error}"
        ))),
        ExecutorOutcome::TimedOut => ExecutorOutcome::Failed(bounded_reason(&format!(
            "Task executor timed out; Workspace release also failed: {error}"
        ))),
        ExecutorOutcome::Cancelled => ExecutorOutcome::Cancelled,
    }
}

fn cancel_fenced_workers(
    snapshot: &TaskGraphSnapshot,
    claims: &HashMap<tokio::task::Id, TaskClaim>,
    cancellations: &BTreeMap<TaskId, CancellationToken>,
) {
    let now = now_ms();
    for claim in claims.values() {
        if !claim_is_current(snapshot.graph(), claim, now)
            && let Some(cancellation) = cancellations.get(&claim.task.id)
        {
            cancellation.cancel();
        }
    }
}

async fn cancel_workers(
    cancellations: &BTreeMap<TaskId, CancellationToken>,
    workers: &mut JoinSet<ExecutorOutcome>,
) {
    for cancellation in cancellations.values() {
        cancellation.cancel();
    }
    let drain = async { while workers.join_next().await.is_some() {} };
    if tokio::time::timeout(CANCELLATION_DRAIN_GRACE, drain)
        .await
        .is_err()
    {
        workers.abort_all();
    }
}

fn claim_is_current(graph: &TaskGraph, claim: &TaskClaim, now_ms: u64) -> bool {
    graph.task(&claim.task.id).is_some_and(|record| {
        matches!(
            &record.status,
            TaskStatus::Running { lease }
                if lease.id == claim.lease.id
                    && lease.owner == claim.lease.owner
                    && lease.attempt == claim.lease.attempt
                    && lease.expires_at_ms > now_ms
        )
    }) && graph.execution_binding_for_lease(&claim.lease.id) == claim.execution_binding.as_ref()
}

fn validate_duration(
    label: &str,
    duration: Duration,
    maximum: Duration,
) -> Result<(), HarnessError> {
    if duration < Duration::from_millis(1) || duration > maximum {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{label} must be 1-{} milliseconds",
            maximum.as_millis()
        )));
    }
    Ok(())
}

fn duration_millis(duration: Duration) -> Result<u64, HarnessError> {
    u64::try_from(duration.as_millis()).map_err(|_| {
        HarnessError::InvalidConfiguration("Task lease duration exceeds u64".to_owned())
    })
}

fn bounded_reason(message: &str) -> String {
    let message = if message.trim().is_empty() {
        "Task executor failed"
    } else {
        message
    };
    if message.len() <= EXECUTOR_ERROR_BYTES {
        return message.to_owned();
    }
    let mut end = EXECUTOR_ERROR_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message[..end].to_owned()
}

fn orchestration_cancelled() -> HarnessError {
    HarnessError::Orchestration("Task orchestration cancelled".to_owned())
}

fn mailbox_fenced() -> HarnessError {
    HarnessError::Orchestration("Task mailbox lease is no longer current".to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        future::pending,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use tokio::sync::{Barrier, Notify};

    use super::{Orchestrator, TaskExecutionRequest, TaskExecutor, TaskMailbox};
    use crate::{
        ActorIdentity, AuthorityContext, CancellationToken, ExecutionBinding, HarnessError,
        HarnessFuture, LocalDirectoryWorkspaceProvider, MemoryTaskCoordinator,
        SqliteTaskCoordinator, TaskCompletion, TaskCoordinator, TaskDefinition, TaskGraph,
        TaskGraphId, TaskId, TaskStatus, WorkspaceDisposition, WorkspaceLease, WorkspaceMode,
        WorkspaceProvider, WorkspaceProviderDescriptor, WorkspaceProvisioning, WorkspaceRequest,
    };

    fn task(id: &'static str, dependencies: &[&'static str]) -> TaskDefinition {
        task_with_workspace(id, dependencies, WorkspaceMode::None)
    }

    fn task_with_workspace(
        id: &'static str,
        dependencies: &[&'static str],
        workspace: WorkspaceMode,
    ) -> TaskDefinition {
        TaskDefinition {
            id: TaskId::from_static(id),
            description: format!("execute {id}"),
            dependencies: dependencies
                .iter()
                .map(|dependency| TaskId::from_static(dependency))
                .collect(),
            priority: 0,
            workspace,
        }
    }

    fn completion(id: &TaskId) -> TaskCompletion {
        TaskCompletion {
            summary: format!("completed {id}"),
            artifacts: Vec::new(),
        }
    }

    struct DependencyExecutor {
        root_barrier: Barrier,
        active: AtomicUsize,
        maximum_active: AtomicUsize,
        completed: Mutex<BTreeSet<TaskId>>,
    }

    impl TaskExecutor for DependencyExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                let task_id = request.claim.task.id.clone();
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum_active.fetch_max(active, Ordering::SeqCst);
                if task_id.as_str() == "task-final" {
                    {
                        let completed = self.completed.lock().map_err(|_| {
                            HarnessError::Orchestration("test completion lock poisoned".to_owned())
                        })?;
                        if !completed.contains(&TaskId::from_static("task-a"))
                            || !completed.contains(&TaskId::from_static("task-b"))
                        {
                            return Err(HarnessError::Orchestration(
                                "dependent Task started before its prerequisites".to_owned(),
                            ));
                        }
                    }
                    let inbox = request.mailbox.inbox(0, 10).await?;
                    let bodies = inbox
                        .messages
                        .iter()
                        .map(|message| message.body.as_str())
                        .collect::<BTreeSet<_>>();
                    if inbox.has_more || bodies != BTreeSet::from(["task-a ready", "task-b ready"])
                    {
                        return Err(HarnessError::Orchestration(
                            "dependent Task did not receive its prerequisite messages".to_owned(),
                        ));
                    }
                } else {
                    self.root_barrier.wait().await;
                    request
                        .mailbox
                        .send(
                            &TaskId::from_static("task-final"),
                            format!("{task_id} ready"),
                        )
                        .await?;
                }
                self.completed
                    .lock()
                    .map_err(|_| {
                        HarnessError::Orchestration("test completion lock poisoned".to_owned())
                    })?
                    .insert(task_id.clone());
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(completion(&task_id))
            })
        }
    }

    #[tokio::test]
    async fn executes_dependencies_with_bounded_parallelism() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-parallel");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task("task-a", &[]),
                    task("task-b", &[]),
                    task("task-final", &["task-a", "task-b"]),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(DependencyExecutor {
            root_barrier: Barrier::new(2),
            active: AtomicUsize::new(0),
            maximum_active: AtomicUsize::new(0),
            completed: Mutex::new(BTreeSet::new()),
        });
        let orchestrator = Orchestrator::new(coordinator, executor.clone(), "worker-parallel")
            .expect("orchestrator")
            .with_concurrency_limit(2)
            .expect("concurrency")
            .with_timing(
                Duration::from_secs(1),
                Duration::from_secs(7),
                Duration::from_millis(1),
            )
            .expect("timing");

        let snapshot = orchestrator
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");

        assert!(snapshot.graph().is_terminal());
        assert_eq!(executor.maximum_active.load(Ordering::SeqCst), 2);
        assert!(snapshot.graph().tasks().all(|record| {
            matches!(record.status, TaskStatus::Completed { .. }) && record.attempts == 1
        }));
    }

    struct BoundExecutor {
        expected: ExecutionBinding,
    }

    impl TaskExecutor for BoundExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                if request.claim.execution_binding.as_ref() != Some(&self.expected) {
                    return Err(HarnessError::Orchestration(
                        "executor received the wrong execution binding".to_owned(),
                    ));
                }
                Ok(completion(&request.claim.task.id))
            })
        }
    }

    #[tokio::test]
    async fn tenant_orchestrator_binds_claim_before_executor_entry() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-bound-runner");
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "runner".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority");
        coordinator
            .create_as(
                graph_id.clone(),
                TaskGraph::new(vec![task("task-bound", &[])]).expect("graph"),
                &authority,
            )
            .await
            .expect("create graph");
        let binding = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            1,
            Some("tenant-a".to_owned()),
        )
        .expect("binding");
        let snapshot = Orchestrator::new(
            coordinator,
            Arc::new(BoundExecutor {
                expected: binding.clone(),
            }),
            "worker-bound",
        )
        .expect("orchestrator")
        .with_execution_context(authority, Some(binding.clone()))
        .expect("execution context")
        .run(&graph_id, CancellationToken::new())
        .await
        .expect("terminal graph");

        assert!(snapshot.graph().is_terminal());
        let evidence = snapshot.graph().attempt_bindings().collect::<Vec<_>>();
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].execution_binding, binding);
    }

    struct ConcurrentWorkspaceExecutor {
        barrier: Barrier,
        roots: Mutex<Vec<PathBuf>>,
    }

    impl TaskExecutor for ConcurrentWorkspaceExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                let root = request
                    .workspace
                    .root()
                    .ok_or_else(|| {
                        HarnessError::Execution(
                            "isolated Task did not receive a workspace".to_owned(),
                        )
                    })?
                    .to_owned();
                if request.workspace.mode() != WorkspaceMode::Isolated
                    || request.workspace.provider() != "local-directory"
                {
                    return Err(HarnessError::Execution(
                        "Task received the wrong workspace contract".to_owned(),
                    ));
                }
                std::fs::write(root.join("task-output.txt"), request.claim.task.id.as_str())
                    .map_err(|error| {
                        HarnessError::Execution(format!("cannot write workspace fixture: {error}"))
                    })?;
                self.roots
                    .lock()
                    .map_err(|_| {
                        HarnessError::Execution("workspace test lock poisoned".to_owned())
                    })?
                    .push(root);
                self.barrier.wait().await;
                Ok(completion(&request.claim.task.id))
            })
        }
    }

    #[tokio::test]
    async fn provisions_concurrent_isolated_workspaces_and_cleans_before_settlement() {
        let root = std::env::temp_dir().join(format!(
            "y-harness-runner-workspaces-{}",
            crate::EventId::generate()
        ));
        let provider =
            Arc::new(LocalDirectoryWorkspaceProvider::new(&root).expect("Workspace Provider"));
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspaces");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task_with_workspace("task-workspace-a", &[], WorkspaceMode::Isolated),
                    task_with_workspace("task-workspace-b", &[], WorkspaceMode::Isolated),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(ConcurrentWorkspaceExecutor {
            barrier: Barrier::new(2),
            roots: Mutex::new(Vec::new()),
        });
        let orchestrator = Orchestrator::new(coordinator, executor.clone(), "worker-workspaces")
            .expect("orchestrator")
            .with_concurrency_limit(2)
            .expect("concurrency")
            .with_workspace_provider(provider)
            .expect("Workspace Provider");

        let snapshot = orchestrator
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");
        assert!(
            snapshot
                .graph()
                .tasks()
                .all(|record| { matches!(record.status, TaskStatus::Completed { .. }) })
        );
        let roots = executor.roots.lock().expect("workspace roots");
        assert_eq!(roots.len(), 2);
        assert_ne!(roots[0], roots[1]);
        assert!(roots.iter().all(|path| !path.exists()));
        drop(roots);
        std::fs::remove_dir(&root).expect("remove empty workspace root");
    }

    struct CountingExecutor {
        calls: AtomicUsize,
    }

    impl TaskExecutor for CountingExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(completion(&request.claim.task.id)) })
        }
    }

    #[tokio::test]
    async fn default_provider_denies_filesystem_tasks_before_executor_entry() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspace-denied");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task_with_workspace(
                    "task-workspace-denied",
                    &[],
                    WorkspaceMode::Isolated,
                )])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(CountingExecutor {
            calls: AtomicUsize::new(0),
        });
        let snapshot = Orchestrator::new(coordinator, executor.clone(), "worker-workspace-denied")
            .expect("orchestrator")
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");

        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-workspace-denied"))
                .expect("Task")
                .status,
            TaskStatus::Failed { reason } if reason.contains("workspace provisioning is disabled")
        ));
    }

    struct PanickingDescriptorProvider;

    impl WorkspaceProvider for PanickingDescriptorProvider {
        fn descriptor(&self) -> WorkspaceProviderDescriptor {
            panic!("sensitive descriptor panic")
        }

        fn prepare<'a>(
            &'a self,
            _request: WorkspaceRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, WorkspaceLease> {
            Box::pin(async { unreachable!("descriptor must fail first") })
        }

        fn release<'a>(
            &'a self,
            _lease: WorkspaceLease,
            _disposition: WorkspaceDisposition,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async { unreachable!("descriptor must fail first") })
        }
    }

    #[test]
    fn workspace_provider_descriptor_panic_is_rejected_during_installation() {
        let result = Orchestrator::new(
            Arc::new(MemoryTaskCoordinator::new()),
            Arc::new(CountingExecutor {
                calls: AtomicUsize::new(0),
            }),
            "worker-provider-descriptor-panic",
        )
        .expect("orchestrator")
        .with_workspace_provider(Arc::new(PanickingDescriptorProvider));
        let error = match result {
            Ok(_) => panic!("panicking descriptor was accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("workspace provider descriptor provider panicked")
        );
    }

    struct PanickingPrepareProvider;

    impl WorkspaceProvider for PanickingPrepareProvider {
        fn descriptor(&self) -> WorkspaceProviderDescriptor {
            WorkspaceProviderDescriptor {
                name: "panicking-prepare".to_owned(),
                provisioning: WorkspaceProvisioning::Directory,
            }
        }

        fn prepare<'a>(
            &'a self,
            _request: WorkspaceRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, WorkspaceLease> {
            Box::pin(async { panic!("sensitive preparation panic") })
        }

        fn release<'a>(
            &'a self,
            _lease: WorkspaceLease,
            _disposition: WorkspaceDisposition,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn workspace_provider_future_panic_is_content_free_and_skips_executor() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspace-provider-panic");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task("task-workspace-provider-panic", &[])]).expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(CountingExecutor {
            calls: AtomicUsize::new(0),
        });
        let snapshot = Orchestrator::new(
            coordinator,
            executor.clone(),
            "worker-workspace-provider-panic",
        )
        .expect("orchestrator")
        .with_workspace_provider(Arc::new(PanickingPrepareProvider))
        .expect("Workspace Provider")
        .run(&graph_id, CancellationToken::new())
        .await
        .expect("terminal graph");

        assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-workspace-provider-panic"))
                .expect("Task")
                .status,
            TaskStatus::Failed { reason } if reason == "Workspace Provider panicked"
        ));
    }

    struct PanickingReleaseProvider;

    impl WorkspaceProvider for PanickingReleaseProvider {
        fn descriptor(&self) -> WorkspaceProviderDescriptor {
            WorkspaceProviderDescriptor {
                name: "panicking-release".to_owned(),
                provisioning: WorkspaceProvisioning::Directory,
            }
        }

        fn prepare<'a>(
            &'a self,
            request: WorkspaceRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, WorkspaceLease> {
            Box::pin(async move { WorkspaceLease::new(request, "panicking-release", None, "none") })
        }

        fn release<'a>(
            &'a self,
            _lease: WorkspaceLease,
            _disposition: WorkspaceDisposition,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async { panic!("sensitive release panic") })
        }
    }

    #[tokio::test]
    async fn workspace_release_panic_replaces_success_without_leaking_payload() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspace-release-panic");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task("task-workspace-release-panic", &[])]).expect("graph"),
            )
            .await
            .expect("create graph");
        let snapshot = Orchestrator::new(
            coordinator,
            Arc::new(CountingExecutor {
                calls: AtomicUsize::new(0),
            }),
            "worker-workspace-release-panic",
        )
        .expect("orchestrator")
        .with_workspace_provider(Arc::new(PanickingReleaseProvider))
        .expect("Workspace Provider")
        .run(&graph_id, CancellationToken::new())
        .await
        .expect("terminal graph");

        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-workspace-release-panic"))
                .expect("Task")
                .status,
            TaskStatus::Failed { reason }
                if reason.contains("Workspace release failed")
                    && !reason.contains("sensitive release panic")
        ));
    }

    struct TimeoutWorkspaceExecutor {
        root: Mutex<Option<PathBuf>>,
    }

    impl TaskExecutor for TimeoutWorkspaceExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                let root = request.workspace.root().expect("workspace root").to_owned();
                std::fs::write(root.join("partial-output.txt"), "partial").expect("partial output");
                *self.root.lock().expect("workspace root lock") = Some(root);
                pending().await
            })
        }
    }

    #[tokio::test]
    async fn timeout_cancels_executor_and_releases_workspace_before_failure() {
        let root = std::env::temp_dir().join(format!(
            "y-harness-timeout-workspace-{}",
            crate::EventId::generate()
        ));
        let provider =
            Arc::new(LocalDirectoryWorkspaceProvider::new(&root).expect("Workspace Provider"));
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspace-timeout");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task_with_workspace(
                    "task-workspace-timeout",
                    &[],
                    WorkspaceMode::Isolated,
                )])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(TimeoutWorkspaceExecutor {
            root: Mutex::new(None),
        });
        let snapshot = Orchestrator::new(coordinator, executor.clone(), "worker-workspace-timeout")
            .expect("orchestrator")
            .with_workspace_provider(provider)
            .expect("Workspace Provider")
            .with_timing(
                Duration::from_millis(250),
                Duration::from_secs(6),
                Duration::from_millis(1),
            )
            .expect("timing")
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");

        let workspace = executor
            .root
            .lock()
            .expect("workspace root lock")
            .clone()
            .expect("captured workspace");
        assert!(!workspace.exists());
        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-workspace-timeout"))
                .expect("Task")
                .status,
            TaskStatus::Failed { reason } if reason == "Task executor timed out"
        ));
        std::fs::remove_dir(&root).expect("remove empty workspace root");
    }

    struct MarkerTamperingExecutor {
        root: Mutex<Option<PathBuf>>,
    }

    impl TaskExecutor for MarkerTamperingExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                let root = request.workspace.root().expect("workspace root").to_owned();
                std::fs::remove_file(
                    root.parent()
                        .expect("workspace container")
                        .join(".y-harness-workspace"),
                )
                .expect("remove private marker");
                *self.root.lock().expect("workspace root lock") = Some(root);
                Ok(completion(&request.claim.task.id))
            })
        }
    }

    #[tokio::test]
    async fn cleanup_failure_replaces_executor_success_with_task_failure() {
        let root = std::env::temp_dir().join(format!(
            "y-harness-cleanup-failure-{}",
            crate::EventId::generate()
        ));
        let provider =
            Arc::new(LocalDirectoryWorkspaceProvider::new(&root).expect("Workspace Provider"));
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-workspace-cleanup-failure");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task_with_workspace(
                    "task-workspace-cleanup-failure",
                    &[],
                    WorkspaceMode::Isolated,
                )])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(MarkerTamperingExecutor {
            root: Mutex::new(None),
        });
        let snapshot = Orchestrator::new(
            coordinator,
            executor.clone(),
            "worker-workspace-cleanup-failure",
        )
        .expect("orchestrator")
        .with_workspace_provider(provider)
        .expect("Workspace Provider")
        .run(&graph_id, CancellationToken::new())
        .await
        .expect("terminal graph");

        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-workspace-cleanup-failure"))
                .expect("Task")
                .status,
            TaskStatus::Failed { reason } if reason.contains("Workspace release failed")
        ));
        let workspace = executor
            .root
            .lock()
            .expect("workspace root lock")
            .clone()
            .expect("captured workspace");
        assert!(workspace.exists());
        std::fs::remove_dir_all(workspace.parent().expect("workspace container"))
            .expect("remove quarantined workspace");
        std::fs::remove_dir(&root).expect("remove workspace root");
    }

    struct IsolatedFailureExecutor;

    impl TaskExecutor for IsolatedFailureExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                match request.claim.task.id.as_str() {
                    "task-panic" => panic!("sensitive executor panic"),
                    "task-timeout" => pending().await,
                    _ => Ok(completion(&request.claim.task.id)),
                }
            })
        }
    }

    #[tokio::test]
    async fn isolates_executor_panic_and_timeout_without_stopping_independent_work() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-failures");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task("task-panic", &[]),
                    task("task-timeout", &[]),
                    task("task-success", &[]),
                    task("task-after-panic", &["task-panic"]),
                    task("task-after-timeout", &["task-timeout"]),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let orchestrator = Orchestrator::new(
            coordinator,
            Arc::new(IsolatedFailureExecutor),
            "worker-failure",
        )
        .expect("orchestrator")
        .with_concurrency_limit(3)
        .expect("concurrency")
        .with_timing(
            Duration::from_millis(5),
            Duration::from_secs(6),
            Duration::from_millis(1),
        )
        .expect("timing");

        let snapshot = orchestrator
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");

        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-panic"))
                .expect("panic Task")
                .status,
            TaskStatus::Failed { reason } if reason == "Task executor panicked"
        ));
        assert!(matches!(
            &snapshot
                .graph()
                .task(&TaskId::from_static("task-timeout"))
                .expect("timeout Task")
                .status,
            TaskStatus::Failed { reason } if reason == "Task executor timed out"
        ));
        assert!(matches!(
            snapshot
                .graph()
                .task(&TaskId::from_static("task-success"))
                .expect("success Task")
                .status,
            TaskStatus::Completed { .. }
        ));
        for task_id in ["task-after-panic", "task-after-timeout"] {
            assert!(matches!(
                snapshot
                    .graph()
                    .task(&TaskId::from_static(task_id))
                    .expect("dependent Task")
                    .status,
                TaskStatus::Blocked { .. }
            ));
        }
    }

    struct FencedExecutor {
        entered: Arc<Notify>,
        cancellation_observed: Arc<Notify>,
    }

    impl TaskExecutor for FencedExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                let cancellation = request.cancellation;
                let observed = self.cancellation_observed.clone();
                tokio::spawn(async move {
                    cancellation.cancelled().await;
                    observed.notify_one();
                });
                self.entered.notify_one();
                pending().await
            })
        }
    }

    #[tokio::test]
    async fn external_fencing_cancels_the_old_executor_and_discards_its_result() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-fenced");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task("task-fenced", &[])]).expect("graph"),
            )
            .await
            .expect("create graph");
        let entered = Arc::new(Notify::new());
        let cancellation_observed = Arc::new(Notify::new());
        let orchestrator = Arc::new(
            Orchestrator::new(
                coordinator.clone(),
                Arc::new(FencedExecutor {
                    entered: entered.clone(),
                    cancellation_observed: cancellation_observed.clone(),
                }),
                "worker-fenced",
            )
            .expect("orchestrator")
            .with_concurrency_limit(1)
            .expect("concurrency")
            .with_timing(
                Duration::from_secs(1),
                Duration::from_secs(7),
                Duration::from_millis(1),
            )
            .expect("timing"),
        );
        let runner = {
            let orchestrator = orchestrator.clone();
            let graph_id = graph_id.clone();
            tokio::spawn(async move { orchestrator.run(&graph_id, CancellationToken::new()).await })
        };
        entered.notified().await;

        let mut external = coordinator
            .load(&graph_id)
            .await
            .expect("load graph")
            .expect("graph");
        external
            .graph_mut()
            .cancel(&TaskId::from_static("task-fenced"), "operator cancellation")
            .expect("cancel Task");
        coordinator
            .compare_and_swap(external)
            .await
            .expect("fence old executor");

        let snapshot = runner.await.expect("runner task").expect("terminal graph");
        tokio::time::timeout(Duration::from_secs(1), cancellation_observed.notified())
            .await
            .expect("executor cancellation");
        assert!(matches!(
            snapshot
                .graph()
                .task(&TaskId::from_static("task-fenced"))
                .expect("Task")
                .status,
            TaskStatus::Cancelled { .. }
        ));
    }

    #[tokio::test]
    async fn stale_mailbox_cannot_send_or_read_after_its_lease_is_fenced() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-mailbox-fenced");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task("task-sender", &[]),
                    task("task-receiver", &["task-sender"]),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let mut claimed = coordinator
            .load(&graph_id)
            .await
            .expect("load graph")
            .expect("graph");
        let claim = claimed
            .graph_mut()
            .claim_ready("worker-mailbox", 1, 100, 1)
            .expect("claim")
            .pop()
            .expect("sender claim");
        coordinator
            .compare_and_swap(claimed)
            .await
            .expect("persist claim");
        let mailbox = super::TaskMailbox {
            coordinator: coordinator.clone(),
            graph_id: graph_id.clone(),
            claim: claim.clone(),
            authority: AuthorityContext::local_process(),
            cancellation: CancellationToken::new(),
        };
        let mut fenced = coordinator
            .load(&graph_id)
            .await
            .expect("load claimed graph")
            .expect("graph");
        fenced
            .graph_mut()
            .cancel(&claim.task.id, "fence sender")
            .expect("cancel sender");
        coordinator
            .compare_and_swap(fenced)
            .await
            .expect("persist fence");

        assert!(
            mailbox
                .send(&TaskId::from_static("task-receiver"), "late message")
                .await
                .expect_err("stale send")
                .to_string()
                .contains("lease is no longer current")
        );
        assert!(
            mailbox
                .inbox(0, 1)
                .await
                .expect_err("stale inbox")
                .to_string()
                .contains("lease is no longer current")
        );
        let current = coordinator
            .load(&graph_id)
            .await
            .expect("load final graph")
            .expect("graph");
        assert!(
            current
                .graph()
                .messages_page_for(&TaskId::from_static("task-receiver"), 0, 1)
                .expect("receiver inbox")
                .messages
                .is_empty()
        );
    }

    struct CapturingMailboxExecutor {
        sender_mailbox: Mutex<Option<TaskMailbox>>,
    }

    impl TaskExecutor for CapturingMailboxExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                if request.claim.task.id.as_str() == "task-sender" {
                    *self.sender_mailbox.lock().map_err(|_| {
                        HarnessError::Orchestration("test mailbox lock poisoned".to_owned())
                    })? = Some(request.mailbox.clone());
                }
                Ok(completion(&request.claim.task.id))
            })
        }
    }

    #[tokio::test]
    async fn completed_executor_cannot_publish_from_a_detached_mailbox_clone() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-mailbox-complete");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task("task-sender", &[]),
                    task("task-receiver", &["task-sender"]),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let executor = Arc::new(CapturingMailboxExecutor {
            sender_mailbox: Mutex::new(None),
        });
        let orchestrator =
            Orchestrator::new(coordinator, executor.clone(), "worker-mailbox-complete")
                .expect("orchestrator");
        orchestrator
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("terminal graph");
        let mailbox = executor
            .sender_mailbox
            .lock()
            .expect("mailbox lock")
            .clone()
            .expect("captured mailbox");

        assert!(
            mailbox
                .send(&TaskId::from_static("task-receiver"), "late message")
                .await
                .expect_err("completed mailbox")
                .to_string()
                .contains("mailbox is cancelled")
        );
    }

    #[test]
    fn rejects_unbounded_or_inconsistent_scheduler_configuration() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let executor = Arc::new(IsolatedFailureExecutor);
        assert!(Orchestrator::new(coordinator.clone(), executor.clone(), "").is_err());
        assert!(
            Orchestrator::new(coordinator.clone(), executor.clone(), "worker-config")
                .expect("orchestrator")
                .with_concurrency_limit(0)
                .is_err()
        );
        assert!(
            Orchestrator::new(coordinator, executor, "worker-config")
                .expect("orchestrator")
                .with_timing(
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    Duration::from_millis(1),
                )
                .is_err()
        );
        assert!(
            Orchestrator::new(
                Arc::new(MemoryTaskCoordinator::new()),
                Arc::new(IsolatedFailureExecutor),
                "worker-config",
            )
            .expect("orchestrator")
            .with_timing(
                Duration::from_micros(1_500),
                Duration::from_micros(1_900),
                Duration::from_millis(1),
            )
            .is_err()
        );
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "runner".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority");
        let mismatched = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            1,
            Some("tenant-b".to_owned()),
        )
        .expect("binding");
        assert!(
            Orchestrator::new(
                Arc::new(MemoryTaskCoordinator::new()),
                Arc::new(IsolatedFailureExecutor),
                "worker-config",
            )
            .expect("orchestrator")
            .with_execution_context(authority, Some(mismatched))
            .is_err()
        );
    }

    #[tokio::test]
    async fn pre_cancelled_run_never_claims_or_mutates_a_task() {
        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let graph_id = TaskGraphId::from_static("graph-pre-cancelled");
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![task("task-pending", &[])]).expect("graph"),
            )
            .await
            .expect("create graph");
        let orchestrator = Orchestrator::new(
            coordinator.clone(),
            Arc::new(IsolatedFailureExecutor),
            "worker-cancelled",
        )
        .expect("orchestrator");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = orchestrator
            .run(&graph_id, cancellation)
            .await
            .expect_err("cancelled run");
        assert!(error.to_string().contains("orchestration cancelled"));
        let snapshot = coordinator
            .load(&graph_id)
            .await
            .expect("load graph")
            .expect("graph");
        assert_eq!(snapshot.revision(), 1);
        let pending = snapshot
            .graph()
            .task(&TaskId::from_static("task-pending"))
            .expect("Task");
        assert_eq!(pending.status, TaskStatus::Pending);
        assert_eq!(pending.attempts, 0);
    }

    struct SqliteMailboxExecutor;

    impl TaskExecutor for SqliteMailboxExecutor {
        fn execute<'a>(
            &'a self,
            request: TaskExecutionRequest,
        ) -> HarnessFuture<'a, TaskCompletion> {
            Box::pin(async move {
                if request.claim.task.id.as_str() == "task-sqlite-a" {
                    request
                        .mailbox
                        .send(&TaskId::from_static("task-sqlite-b"), "sqlite message")
                        .await?;
                } else {
                    let inbox = request.mailbox.inbox(0, 1).await?;
                    if inbox.messages.len() != 1 || inbox.messages[0].body != "sqlite message" {
                        return Err(HarnessError::Orchestration(
                            "SQLite Task message was not recovered".to_owned(),
                        ));
                    }
                }
                Ok(completion(&request.claim.task.id))
            })
        }
    }

    #[tokio::test]
    async fn sqlite_orchestration_survives_coordinator_reopen() {
        let path = std::env::temp_dir().join(format!(
            "y-harness-orchestrator-{}.sqlite",
            TaskGraphId::generate()
        ));
        let graph_id = TaskGraphId::from_static("graph-sqlite-runner");
        let coordinator = Arc::new(
            SqliteTaskCoordinator::open(&path)
                .await
                .expect("open coordinator"),
        );
        coordinator
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![
                    task("task-sqlite-a", &[]),
                    task("task-sqlite-b", &["task-sqlite-a"]),
                ])
                .expect("graph"),
            )
            .await
            .expect("create graph");
        let orchestrator = Orchestrator::new(
            coordinator.clone(),
            Arc::new(SqliteMailboxExecutor),
            "worker-sqlite",
        )
        .expect("orchestrator")
        .with_timing(
            Duration::from_secs(1),
            Duration::from_secs(7),
            Duration::from_millis(1),
        )
        .expect("timing");
        let settled = orchestrator
            .run(&graph_id, CancellationToken::new())
            .await
            .expect("settled graph");
        assert!(settled.graph().is_terminal());
        drop(orchestrator);
        drop(coordinator);

        let reopened = SqliteTaskCoordinator::open(&path)
            .await
            .expect("reopen coordinator")
            .load(&graph_id)
            .await
            .expect("load graph")
            .expect("graph");
        assert!(reopened.graph().tasks().all(|record| {
            matches!(record.status, TaskStatus::Completed { .. }) && record.attempts == 1
        }));
        assert_eq!(
            reopened
                .graph()
                .messages_page_for(&TaskId::from_static("task-sqlite-b"), 0, 1)
                .expect("reopened inbox")
                .messages[0]
                .body,
            "sqlite message"
        );

        for suffix in ["", "-wal", "-shm"] {
            let mut target = path.as_os_str().to_os_string();
            target.push(suffix);
            let _ = std::fs::remove_file(target);
        }
    }
}
