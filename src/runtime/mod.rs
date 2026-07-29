//! Agent Loop execution, policy settlement, and ordered state recording.

mod control;
mod policy;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::{sync::Semaphore, task::JoinSet, time::Instant};

use crate::verification::validate_outcome;
pub use control::TurnExecutionOptions;
use control::{
    controlled, controlled_with_settlement_cancellation, controlled_with_settlement_grace, deadline,
};
pub use policy::{AllowListPolicy, ApprovalHandler, DenyAllApprovals, PolicyEngine};

pub use crate::kernel::{LanguageModel, Tool};

use crate::{
    ActorIdentity, ApprovalDecision, ApprovalId, ApprovalRequest, AuthorityContext,
    CapabilityOrigin, ConnectorEvidence, ContextBlock, ContextEngine, ContextSource,
    ExecutionPhase, HarnessError, InvocationContextEvidence, Item, ItemKind,
    MemoryContextRecordStatus, MemoryContextStatus, MemoryScope, ModelContinuation, ModelOutput,
    ModelProviderFailureKind, ModelRegistry, ModelRequest, ModelResponse, ModelStream,
    ModelToolCall, Observability, ObservationOutcome, PhaseObservation, PolicyDecision,
    StateCapacity, StateEngine, SteeringId, StoredEvent, Thread, ThreadArchive, ThreadId,
    ToolAuthorization, ToolBatchExecution, ToolCallBatch, ToolCallBatchId, ToolContext,
    ToolRegistry, Turn, TurnId, TurnOutcome, TurnStatus, TurnStopReason, VerificationOutcome,
    VerificationRegistry, VerificationRequest,
    context::{model_visible_items, validate_turn_context_inputs},
    kernel::{validate_capability_origin, validate_model_id},
};

const MAX_PROMPT_BYTES: usize = 1_048_576;
const MAX_MODEL_TEXT_BYTES: usize = 1_048_576;
const MAX_MODEL_TOOL_INPUT_BYTES: usize = 1_048_576;
const MAX_MODEL_TOOL_BATCH_BYTES: usize = 4_194_304;
const MAX_TOOL_OUTPUT_BYTES: usize = 1_048_576;
const MAX_MODEL_REQUEST_BYTES: usize = 16_777_216;
const MAX_RUNTIME_ERROR_CHARS: usize = 4_096;
const MAX_MODEL_CALL_ID_BYTES: usize = 256;
const MAX_PROVIDER_EVIDENCE_ID_BYTES: usize = 256;
const MAX_POLICY_REASON_BYTES: usize = 4_096;
const DEFAULT_MAX_AGENT_STEPS: usize = 32;
const MAX_AGENT_STEPS: usize = 256;
const MAX_MODEL_ROUTE_ENTRIES: usize = 16;
const DEFAULT_MODEL_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_MODEL_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_MODEL_TIMEOUT_COOLDOWN: Duration = Duration::from_secs(86_400);
/// Maximum additional calls permitted for one configured Model candidate.
pub const MAX_MODEL_RETRIES: u8 = 8;
/// Maximum configured fallback delay between calls to the same Model.
pub const MAX_MODEL_RETRY_DELAY_MS: u64 = 60_000;
/// Default Provider-call ceiling for one Agent Loop Model step.
pub const DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP: usize = MAX_MODEL_ROUTE_ENTRIES;
/// Hard Provider-call ceiling for one Agent Loop Model step.
pub const MAX_MODEL_ATTEMPTS_PER_STEP: usize =
    MAX_MODEL_ROUTE_ENTRIES * (MAX_MODEL_RETRIES as usize + 1);
const DEFAULT_MAX_CONCURRENT_TURNS: usize = 32;
const MAX_CONCURRENT_TURNS: usize = 4_096;
/// Default same-batch concurrency ceiling for explicitly `ParallelSafe` Tools.
pub const DEFAULT_MAX_PARALLEL_TOOL_CALLS: usize = 4;
/// Hard same-batch concurrency ceiling.
pub const MAX_PARALLEL_TOOL_CALLS: usize = 64;
const MIN_RUNTIME_GENERAL_EVENTS: u64 = 4;
const MAX_PENDING_STEERING: usize = 64;
const MAX_PENDING_STEERING_BYTES: usize = 1_048_576;

/// Durable acknowledgement for one Turn steering submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SteeringReceipt {
    /// Runtime-generated steering identity.
    pub steering_id: SteeringId,
    /// Exact active Turn that accepted the input.
    pub turn_id: TurnId,
}

/// Explicit bounded policy for retrying one Model before Route failover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ModelRetryPolicy {
    max_retries: u8,
    initial_delay: Duration,
    max_delay: Duration,
}

impl ModelRetryPolicy {
    /// Creates a policy with bounded equal-jitter exponential fallback delays.
    pub fn new(
        max_retries: u8,
        initial_delay: Duration,
        max_delay: Duration,
    ) -> Result<Self, HarnessError> {
        if !(1..=MAX_MODEL_RETRIES).contains(&max_retries) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model retries must be 1-{MAX_MODEL_RETRIES}"
            )));
        }
        let supported = Duration::from_millis(1)..=Duration::from_millis(MAX_MODEL_RETRY_DELAY_MS);
        if !supported.contains(&initial_delay) || !supported.contains(&max_delay) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model retry delays must be 1-{MAX_MODEL_RETRY_DELAY_MS} milliseconds"
            )));
        }
        if initial_delay > max_delay {
            return Err(HarnessError::InvalidConfiguration(
                "Model initial retry delay cannot exceed its maximum delay".to_owned(),
            ));
        }
        Ok(Self {
            max_retries,
            initial_delay,
            max_delay,
        })
    }

    /// Returns the maximum number of calls after the initial attempt.
    #[must_use]
    pub const fn max_retries(&self) -> u8 {
        self.max_retries
    }

    /// Returns the first fallback backoff ceiling.
    #[must_use]
    pub const fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Returns the maximum accepted or computed retry delay.
    #[must_use]
    pub const fn max_delay(&self) -> Duration {
        self.max_delay
    }
}

#[derive(Clone)]
struct PendingSteering {
    steering_id: SteeringId,
    content: String,
}

struct ToolCallSettlement {
    call_id: String,
    output: Value,
    is_error: bool,
    connector_evidence: Vec<ConnectorEvidence>,
}

struct ToolCapabilityInvocation {
    tool: Arc<dyn Tool>,
    origin: CapabilityOrigin,
    cancellation_settlement_timeout: Duration,
    call: ModelToolCall,
}

struct ActiveTurnControl {
    turn_id: TurnId,
    authority: AuthorityContext,
    accepting_steering: bool,
    pending_steering: VecDeque<PendingSteering>,
    pending_steering_bytes: usize,
}

/// Headless Agent Loop coordinating model, context, policy, tools, and state.
pub struct HarnessRuntime {
    models: ModelRoute,
    tools: ToolRegistry,
    policy: Arc<dyn PolicyEngine>,
    approvals: Arc<dyn ApprovalHandler>,
    state: StateEngine,
    context: ContextEngine,
    verification: VerificationRegistry,
    observability: Observability,
    active_threads: Mutex<BTreeSet<ThreadId>>,
    turn_controls: Mutex<BTreeMap<ThreadId, Arc<tokio::sync::Mutex<ActiveTurnControl>>>>,
    max_concurrent_turns: usize,
    max_parallel_tool_calls: usize,
    max_model_attempts_per_step: usize,
    max_steps: usize,
}

impl HarnessRuntime {
    #[must_use]
    /// Creates a runtime for a statically linked built-in model.
    ///
    /// Extension hosts should use [`Self::from_model_registry`] so the
    /// operator-assigned model origin is retained in State evidence.
    pub fn new(
        model: Arc<dyn LanguageModel>,
        tools: ToolRegistry,
        policy: Arc<dyn PolicyEngine>,
        state: StateEngine,
    ) -> Self {
        Self {
            models: ModelRoute::built_in(model),
            tools,
            policy,
            approvals: Arc::new(DenyAllApprovals),
            state,
            context: ContextEngine::without_memory(),
            verification: VerificationRegistry::new(),
            observability: Observability::new(),
            active_threads: Mutex::new(BTreeSet::new()),
            turn_controls: Mutex::new(BTreeMap::new()),
            max_concurrent_turns: DEFAULT_MAX_CONCURRENT_TURNS,
            max_parallel_tool_calls: DEFAULT_MAX_PARALLEL_TOOL_CALLS,
            max_model_attempts_per_step: DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP,
            max_steps: DEFAULT_MAX_AGENT_STEPS,
        }
    }

    /// Creates a runtime from one validated, trust-bearing model registration.
    pub fn from_model_registry(
        models: &ModelRegistry,
        model_id: &str,
        tools: ToolRegistry,
        policy: Arc<dyn PolicyEngine>,
        state: StateEngine,
    ) -> Result<Self, HarnessError> {
        Self::from_model_registry_failover(models, &[model_id], tools, policy, state)
    }

    /// Creates a runtime with an explicit ordered Model failover route.
    ///
    /// Each identity must already exist in `models`. The first Model is tried
    /// first on every step; a later Model is attempted only after an ordinary
    /// pre-output failure. Multi-model routes use a 30-second per-attempt
    /// timeout by default. Cancellation, the Turn deadline, or successfully
    /// delivered provisional output stop failover.
    pub fn from_model_registry_failover(
        models: &ModelRegistry,
        model_ids: &[&str],
        tools: ToolRegistry,
        policy: Arc<dyn PolicyEngine>,
        state: StateEngine,
    ) -> Result<Self, HarnessError> {
        Ok(Self {
            models: ModelRoute::from_registry(models, model_ids)?,
            tools,
            policy,
            approvals: Arc::new(DenyAllApprovals),
            state,
            context: ContextEngine::without_memory(),
            verification: VerificationRegistry::new(),
            observability: Observability::new(),
            active_threads: Mutex::new(BTreeSet::new()),
            turn_controls: Mutex::new(BTreeMap::new()),
            max_concurrent_turns: DEFAULT_MAX_CONCURRENT_TURNS,
            max_parallel_tool_calls: DEFAULT_MAX_PARALLEL_TOOL_CALLS,
            max_model_attempts_per_step: DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP,
            max_steps: DEFAULT_MAX_AGENT_STEPS,
        })
    }

    #[must_use]
    /// Installs the Context Engine used before model execution.
    pub fn with_context_engine(mut self, context: ContextEngine) -> Self {
        self.context = context;
        self
    }

    #[must_use]
    /// Installs the handler used only when Policy returns `Ask`.
    pub fn with_approval_handler(mut self, approvals: Arc<dyn ApprovalHandler>) -> Self {
        self.approvals = approvals;
        self
    }

    #[must_use]
    /// Installs the ordered completion verifiers used for assistant candidates.
    pub fn with_verification(mut self, verification: VerificationRegistry) -> Self {
        self.verification = verification;
        self
    }

    #[must_use]
    /// Installs failure-isolated Runtime phase observers.
    pub fn with_observability(mut self, observability: Observability) -> Self {
        self.observability = observability;
        self
    }

    #[must_use]
    /// Sets the hard model-step budget within the supported bounded range.
    pub fn with_max_steps(mut self, max_steps: usize) -> Self {
        self.max_steps = max_steps.clamp(1, MAX_AGENT_STEPS);
        self
    }

    /// Sets the independent deadline applied to every configured Model attempt.
    ///
    /// The total Turn deadline remains authoritative when it expires first.
    /// A timed-out attempt may fall through only when no provisional output
    /// was delivered.
    pub fn with_model_attempt_timeout(mut self, timeout: Duration) -> Result<Self, HarnessError> {
        if timeout < Duration::from_millis(1) || timeout > MAX_MODEL_ATTEMPT_TIMEOUT {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model attempt timeout must be 1-{} milliseconds",
                MAX_MODEL_ATTEMPT_TIMEOUT.as_millis()
            )));
        }
        self.models.attempt_timeout = Some(timeout);
        Ok(self)
    }

    /// Temporarily deprioritizes a routed Model after its Runtime attempt timeout.
    ///
    /// The cooldown is process-local, applies only when another routed Model
    /// is available, and never overrides Provider Continuation affinity.
    pub fn with_model_timeout_cooldown(mut self, cooldown: Duration) -> Result<Self, HarnessError> {
        if self.models.entries.len() < 2 {
            return Err(HarnessError::InvalidConfiguration(
                "Model timeout cooldown requires a multi-Model route".to_owned(),
            ));
        }
        if cooldown < Duration::from_millis(1) || cooldown > MAX_MODEL_TIMEOUT_COOLDOWN {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model timeout cooldown must be 1-{} milliseconds",
                MAX_MODEL_TIMEOUT_COOLDOWN.as_millis()
            )));
        }
        self.models.timeout_cooldown = Some(cooldown);
        Ok(self)
    }

    /// Enables bounded typed-failure retries for each Model candidate.
    ///
    /// Retries share the candidate's existing attempt deadline and stop after
    /// any provisional output. The total Turn deadline remains authoritative.
    #[must_use]
    pub fn with_model_retry_policy(mut self, policy: ModelRetryPolicy) -> Self {
        self.models.retry_policy = Some(policy);
        self
    }

    /// Sets the Provider-call ceiling for each Agent Loop Model step.
    ///
    /// Route failover and same-Model retries share this budget. Together with
    /// `max_steps`, it gives a hard Runtime-managed Model-call bound for one
    /// Turn, including Turns resumed after approval.
    pub fn with_max_model_attempts_per_step(mut self, limit: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_MODEL_ATTEMPTS_PER_STEP).contains(&limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model attempts per step must be 1-{MAX_MODEL_ATTEMPTS_PER_STEP}"
            )));
        }
        self.max_model_attempts_per_step = limit;
        Ok(self)
    }

    /// Returns the maximum Runtime-managed Model calls possible in one Turn.
    #[must_use]
    pub const fn model_attempts_per_turn_bound(&self) -> usize {
        self.max_steps * self.max_model_attempts_per_step
    }

    /// Sets the maximum number of Turns executing concurrently in this Runtime.
    ///
    /// Admission fails before any Turn state is written when the limit is
    /// reached. Separate Runtime instances need host-level coordination.
    pub fn with_turn_concurrency_limit(mut self, limit: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_CONCURRENT_TURNS).contains(&limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Turn concurrency limit must be 1-{MAX_CONCURRENT_TURNS}"
            )));
        }
        self.max_concurrent_turns = limit;
        Ok(self)
    }

    /// Sets the maximum concurrently executing `ParallelSafe` calls from one
    /// same-response Tool batch.
    pub fn with_max_parallel_tool_calls(mut self, limit: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_PARALLEL_TOOL_CALLS).contains(&limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "parallel Tool limit must be 1-{MAX_PARALLEL_TOOL_CALLS}"
            )));
        }
        self.max_parallel_tool_calls = limit;
        Ok(self)
    }

    /// Creates and persists a new Thread.
    pub async fn create_thread(&self) -> Result<Thread, HarnessError> {
        self.state.create_thread().await
    }

    /// Creates a Thread owned by the trusted authority's tenant boundary.
    pub async fn create_thread_as(
        &self,
        authority: &AuthorityContext,
    ) -> Result<Thread, HarnessError> {
        self.state.create_thread_as(authority).await
    }

    /// Loads a projected Thread without mutating it.
    pub async fn load_thread(&self, thread_id: &ThreadId) -> Result<Option<Thread>, HarnessError> {
        self.state.load_thread(thread_id).await
    }

    /// Loads a Thread only inside the trusted authority's tenant boundary.
    pub async fn load_thread_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Option<Thread>, HarnessError> {
        self.state.load_thread_as(thread_id, authority).await
    }

    /// Whether the configured State store supports atomic Thread forks.
    #[must_use]
    pub fn supports_thread_fork(&self) -> bool {
        self.state.supports_thread_fork()
    }

    /// Forks one terminal parent boundary into an independent child Thread.
    pub async fn fork_thread(
        &self,
        parent_thread_id: &ThreadId,
        child_thread_id: ThreadId,
        through_turn_id: Option<&TurnId>,
    ) -> Result<Thread, HarnessError> {
        self.state
            .fork_thread(parent_thread_id, child_thread_id, through_turn_id)
            .await
    }

    /// Forks a Thread inside the trusted authority's tenant boundary.
    pub async fn fork_thread_as(
        &self,
        authority: &AuthorityContext,
        parent_thread_id: &ThreadId,
        child_thread_id: ThreadId,
        through_turn_id: Option<&TurnId>,
    ) -> Result<Thread, HarnessError> {
        self.state
            .fork_thread_as(
                authority,
                parent_thread_id,
                child_thread_id,
                through_turn_id,
            )
            .await
    }

    /// Exports one terminal Thread as a portable integrity-bound archive.
    pub async fn export_thread(&self, thread_id: &ThreadId) -> Result<ThreadArchive, HarnessError> {
        self.state.export_thread(thread_id).await
    }

    /// Exports a Thread only inside the trusted authority's tenant boundary.
    pub async fn export_thread_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<ThreadArchive, HarnessError> {
        self.state.export_thread_as(thread_id, authority).await
    }

    /// Whether the configured State store supports atomic Thread imports.
    #[must_use]
    pub fn supports_thread_import(&self) -> bool {
        self.state.supports_thread_import()
    }

    /// Atomically imports one portable archive under a caller-chosen identity.
    pub async fn import_thread(
        &self,
        archive: &ThreadArchive,
        target_thread_id: ThreadId,
    ) -> Result<Thread, HarnessError> {
        self.state.import_thread(archive, target_thread_id).await
    }

    /// Imports an archive into the trusted authority's tenant boundary.
    pub async fn import_thread_as(
        &self,
        archive: &ThreadArchive,
        target_thread_id: ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Thread, HarnessError> {
        self.state
            .import_thread_as(archive, target_thread_id, authority)
            .await
    }

    /// Changes or clears the durable operator-authored Thread name.
    pub async fn set_thread_name(
        &self,
        thread_id: &ThreadId,
        name: Option<String>,
    ) -> Result<StoredEvent, HarnessError> {
        self.state.set_thread_name(thread_id, name).await
    }

    /// Changes a Thread name inside the trusted authority's tenant boundary.
    pub async fn set_thread_name_as(
        &self,
        thread_id: &ThreadId,
        name: Option<String>,
        authority: &AuthorityContext,
    ) -> Result<StoredEvent, HarnessError> {
        self.state
            .set_thread_name_as(thread_id, name, authority)
            .await
    }

    /// Returns journal pressure before one Thread reaches its finite boundary.
    pub async fn thread_capacity(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Option<StateCapacity>, HarnessError> {
        self.state.thread_capacity(thread_id).await
    }

    /// Returns capacity only inside the trusted authority's tenant boundary.
    pub async fn thread_capacity_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Option<StateCapacity>, HarnessError> {
        self.state.thread_capacity_as(thread_id, authority).await
    }

    /// Whether the configured State store supports recent-Thread navigation.
    #[must_use]
    pub fn supports_thread_listing(&self) -> bool {
        self.state.supports_thread_listing()
    }

    /// Returns one bounded recent-Thread page without loading full histories.
    pub async fn list_threads(
        &self,
        before_sequence: Option<u64>,
        limit: usize,
    ) -> Result<crate::ThreadSummaryPage, HarnessError> {
        self.state.list_threads(before_sequence, limit).await
    }

    /// Lists only Threads inside the trusted authority's tenant boundary.
    pub async fn list_threads_as(
        &self,
        before_sequence: Option<u64>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<crate::ThreadSummaryPage, HarnessError> {
        self.state
            .list_threads_as(before_sequence, limit, authority)
            .await
    }

    /// Prepares a bounded, digest-bound source delta for an optional Thread handoff.
    ///
    /// This read-only operation does not synthesize or persist a summary. The
    /// embedding host may pass the request to any summarizer, then convert its
    /// response with [`crate::ThreadHandoffRequest::to_context`].
    pub async fn prepare_thread_handoff(
        &self,
        source_thread_id: &ThreadId,
        target_thread_id: &ThreadId,
        config: &crate::ThreadHandoffConfig,
    ) -> Result<Option<crate::ThreadHandoffRequest>, HarnessError> {
        self.prepare_thread_handoff_as(
            source_thread_id,
            target_thread_id,
            config,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Prepares a Thread handoff inside the trusted authority's tenant boundary.
    pub async fn prepare_thread_handoff_as(
        &self,
        source_thread_id: &ThreadId,
        target_thread_id: &ThreadId,
        config: &crate::ThreadHandoffConfig,
        authority: &AuthorityContext,
    ) -> Result<Option<crate::ThreadHandoffRequest>, HarnessError> {
        let source = self
            .load_thread_as(source_thread_id, authority)
            .await?
            .ok_or_else(|| {
                HarnessError::State(format!("thread {source_thread_id} does not exist"))
            })?;
        let target = self
            .load_thread_as(target_thread_id, authority)
            .await?
            .ok_or_else(|| {
                HarnessError::State(format!("thread {target_thread_id} does not exist"))
            })?;
        crate::ThreadHandoffRequest::prepare(&source, &target, config)
    }

    /// Durably queues additional user input for one exact active Turn.
    ///
    /// The input is not exposed to the Model until the Runtime reaches a safe
    /// boundary. A mismatched or already sealed Turn fails without writing.
    pub async fn steer_turn(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        content: impl Into<String>,
        submitted_by: ActorIdentity,
    ) -> Result<SteeringReceipt, HarnessError> {
        let authority = AuthorityContext::new(submitted_by, None)?;
        self.steer_turn_as(thread_id, expected_turn_id, content, &authority)
            .await
    }

    /// Queues steering inside the trusted authority's tenant boundary.
    pub async fn steer_turn_as(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        content: impl Into<String>,
        authority: &AuthorityContext,
    ) -> Result<SteeringReceipt, HarnessError> {
        let content = content.into();
        validate_steering(&content)?;
        authority.validate_current("steering authority")?;
        let control = self.turn_control(thread_id)?;
        let mut control = control.lock().await;
        if control.turn_id != *expected_turn_id {
            return Err(HarnessError::State(format!(
                "steering expected turn {expected_turn_id}, active turn is {}",
                control.turn_id
            )));
        }
        if !control.accepting_steering {
            return Err(HarnessError::State(format!(
                "turn {expected_turn_id} no longer accepts steering"
            )));
        }
        if control.pending_steering.len() >= MAX_PENDING_STEERING
            || control
                .pending_steering_bytes
                .checked_add(content.len())
                .is_none_or(|bytes| bytes > MAX_PENDING_STEERING_BYTES)
        {
            return Err(HarnessError::State(format!(
                "turn steering capacity reached ({MAX_PENDING_STEERING} messages or {MAX_PENDING_STEERING_BYTES} bytes)"
            )));
        }

        let steering_id = SteeringId::generate();
        let queued = Item::new(ItemKind::SteeringQueued {
            steering_id: steering_id.clone(),
            submitted_by: authority.actor().clone(),
            content: content.clone(),
        });
        let turn = Turn {
            id: expected_turn_id.clone(),
            thread_id: thread_id.clone(),
            status: TurnStatus::Running,
            items: Vec::new(),
        };
        self.state.append_item_as(&turn, queued, authority).await?;
        control.pending_steering_bytes += content.len();
        control.pending_steering.push_back(PendingSteering {
            steering_id: steering_id.clone(),
            content,
        });
        Ok(SteeringReceipt {
            steering_id,
            turn_id: expected_turn_id.clone(),
        })
    }

    /// Recovers unfinished execution and orphans approvals it can no longer consume.
    ///
    /// The caller must first establish exclusive ownership of the Thread and
    /// confirm that its previous worker has stopped. Normal Turn execution
    /// never invokes recovery implicitly because a running Turn may belong to
    /// another healthy Runtime sharing the same Event Store.
    pub async fn recover_thread(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
    ) -> Result<Option<Thread>, HarnessError> {
        self.recover_thread_as(
            thread_id,
            expected_turn_id,
            &AuthorityContext::local_process(),
        )
        .await
    }

    /// Recovers unfinished execution inside the trusted tenant boundary.
    pub async fn recover_thread_as(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        authority: &AuthorityContext,
    ) -> Result<Option<Thread>, HarnessError> {
        let recovered = self
            .state
            .recover_thread_as(thread_id, expected_turn_id, authority)
            .await?;
        if let Some(thread) = &recovered
            && let Some(turn) = thread
                .turns
                .iter()
                .find(|turn| &turn.id == expected_turn_id)
                .filter(|turn| turn.status == TurnStatus::Interrupted)
        {
            self.approvals
                .abandon_turn_as(
                    thread_id,
                    &turn.id,
                    "originating Turn was interrupted before approval settlement",
                    authority,
                )
                .await?;
        }
        Ok(recovered)
    }

    /// Waits for accepted failure-isolated Runtime maintenance work.
    ///
    /// Protocol and embedding hosts should call this only after no new Turns
    /// can start and all accepted Turns have settled.
    pub async fn drain_background_work(&self, timeout: Duration) -> bool {
        self.state.drain_snapshot_maintenance(timeout).await
    }

    /// Returns authoritative ordered events for one Thread.
    pub async fn events(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Vec<crate::StoredEvent>, HarnessError> {
        self.state.events(thread_id).await
    }

    /// Returns events only inside the trusted authority's tenant boundary.
    pub async fn events_as(
        &self,
        thread_id: &ThreadId,
        authority: &AuthorityContext,
    ) -> Result<Vec<crate::StoredEvent>, HarnessError> {
        self.state.events_as(thread_id, authority).await
    }

    /// Returns a bounded authoritative event page after one durable sequence.
    pub async fn events_page(
        &self,
        thread_id: &ThreadId,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<crate::StoredEvent>, HarnessError> {
        self.state
            .events_page(thread_id, after_sequence, limit)
            .await
    }

    /// Returns an event page inside the trusted authority's tenant boundary.
    pub async fn events_page_as(
        &self,
        thread_id: &ThreadId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<Vec<crate::StoredEvent>, HarnessError> {
        self.state
            .events_page_as(thread_id, after_sequence, limit, authority)
            .await
    }

    /// Runs one Turn with an empty memory scope.
    pub async fn run_turn(
        &self,
        thread_id: &ThreadId,
        prompt: impl Into<String>,
    ) -> Result<TurnOutcome, HarnessError> {
        self.run_turn_with_options(thread_id, prompt, TurnExecutionOptions::default())
            .await
    }

    /// Runs one Turn using an explicit memory isolation scope.
    pub async fn run_turn_scoped(
        &self,
        thread_id: &ThreadId,
        prompt: impl Into<String>,
        memory_scope: MemoryScope,
    ) -> Result<TurnOutcome, HarnessError> {
        self.run_turn_with_options(
            thread_id,
            prompt,
            TurnExecutionOptions {
                memory_scope,
                ..TurnExecutionOptions::default()
            },
        )
        .await
    }

    /// Runs one Turn with explicit memory scope, cancellation, and deadline.
    pub async fn run_turn_with_options(
        &self,
        thread_id: &ThreadId,
        prompt: impl Into<String>,
        options: TurnExecutionOptions,
    ) -> Result<TurnOutcome, HarnessError> {
        self.execute_turn(thread_id, TurnEntry::Start(prompt.into()), options)
            .await
    }

    /// Resumes a running Turn stopped at the durable pre-Tool approval boundary.
    ///
    /// The caller must first establish exclusive ownership of the Thread and
    /// confirm that its previous worker has stopped. Recovery revalidates the
    /// exact Model request, Tool metadata, requester, and ordered State before
    /// consuming an approval or executing the Tool.
    pub async fn resume_approval_turn_with_options(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
        options: TurnExecutionOptions,
    ) -> Result<TurnOutcome, HarnessError> {
        self.execute_turn(
            thread_id,
            TurnEntry::ResumeApproval(turn_id.clone()),
            options,
        )
        .await
    }

    async fn execute_turn(
        &self,
        thread_id: &ThreadId,
        entry: TurnEntry,
        options: TurnExecutionOptions,
    ) -> Result<TurnOutcome, HarnessError> {
        self.models.validate()?;
        let execution_binding = options.validated_execution_binding()?;
        let memory_scope = options.validated_memory_scope()?;
        validate_turn_context_inputs(&options.context)?;
        let deadline = deadline(options.timeout)?;
        let existing = self
            .load_thread_as(thread_id, &options.authority)
            .await?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        let _active = self.claim_thread(thread_id)?;
        let (
            mut turn,
            conversation_items,
            context_blocks,
            starting_step,
            mut tool_call_ids,
            _turn_control,
        ) = match entry {
            TurnEntry::Start(prompt) => {
                validate_prompt(&prompt)?;
                let capacity = self
                    .state
                    .thread_capacity_as(thread_id, &options.authority)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::State(format!("thread {thread_id} does not exist"))
                    })?;
                require_runtime_capacity(&capacity)?;
                let conversation = self.context.compile_conversation(&existing)?;

                let mut turn = self
                    .state
                    .start_turn_as(thread_id, &options.authority)
                    .await?;
                let turn_control = self.register_turn_control(&turn, options.authority.clone())?;
                self.record(
                    &mut turn,
                    ItemKind::UserMessage {
                        content: prompt.clone(),
                    },
                )
                .await?;
                if let Some(binding) = execution_binding {
                    self.record(
                        &mut turn,
                        ItemKind::ExecutionBinding {
                            bound_by: options.authority.actor().clone(),
                            binding,
                        },
                    )
                    .await?;
                }
                self.record(
                    &mut turn,
                    ItemKind::ConversationContext {
                        included_turns: conversation.included_turns.clone(),
                        dropped_turns: conversation.dropped_turns,
                        estimated_tokens: conversation.serialized_bytes,
                    },
                )
                .await?;

                let conversation_summary = if let Some(compactor) =
                    self.context.conversation_compactor_name(&conversation)
                {
                    match self
                        .controlled_observed(
                            ObservationTarget::new(
                                thread_id,
                                &turn.id,
                                compactor,
                                ExecutionPhase::Context,
                            ),
                            &options.cancellation,
                            deadline,
                            || {
                                self.context.compile_conversation_summary(
                                    &conversation,
                                    &prompt,
                                    options.cancellation.clone(),
                                )
                            },
                        )
                        .await
                    {
                        Ok(summary) => summary,
                        Err(error) => {
                            self.settle_error(&mut turn, &error).await?;
                            return Err(error);
                        }
                    }
                } else {
                    None
                };
                let conversation_summary_record = match conversation_summary
                    .as_ref()
                    .map(conversation_summary_record)
                    .transpose()
                {
                    Ok(record) => record,
                    Err(error) => {
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                };
                let compilation = match self
                    .controlled_observed(
                        ObservationTarget::new(
                            thread_id,
                            &turn.id,
                            "context-engine",
                            ExecutionPhase::Context,
                        ),
                        &options.cancellation,
                        deadline,
                        || self.context.compile(&prompt, memory_scope.clone()),
                    )
                    .await
                {
                    Ok(compilation) => compilation,
                    Err(error) => {
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                };
                let compilation = match self
                    .context
                    .merge_conversation_summary(compilation, conversation_summary)
                {
                    Ok(compilation) => compilation,
                    Err(error) => {
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                };
                let compilation = match self
                    .context
                    .merge_turn_context(compilation, &options.context)
                {
                    Ok(compilation) => compilation,
                    Err(error) => {
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                };
                if let Some(record) = conversation_summary_record {
                    self.record(&mut turn, record).await?;
                }
                let invocation_record = invocation_context_record(
                    &compilation.blocks,
                    options.authority.actor().clone(),
                );
                if let Some(record) = invocation_record {
                    self.record(&mut turn, record).await?;
                }
                if let Some(observation) = compilation.memory {
                    let status = match observation.status {
                        MemoryContextStatus::Loaded => MemoryContextRecordStatus::Loaded,
                        MemoryContextStatus::Degraded => MemoryContextRecordStatus::Degraded,
                    };
                    self.record(
                        &mut turn,
                        ItemKind::MemoryContext {
                            provider: observation.provider,
                            status,
                            references: observation.references,
                            packed_tokens: observation.packed_tokens,
                            warnings: observation.warnings,
                        },
                    )
                    .await?;
                }
                (
                    turn,
                    conversation.items,
                    compilation.blocks,
                    0,
                    BTreeSet::new(),
                    turn_control,
                )
            }
            TurnEntry::ResumeApproval(turn_id) => {
                let turn = existing
                    .turns
                    .iter()
                    .find(|turn| turn.id == turn_id && turn.status == TurnStatus::Running)
                    .ok_or_else(|| {
                        HarnessError::State(format!(
                            "turn {turn_id} is not the running turn in thread {thread_id}"
                        ))
                    })?;
                let turn_control = self.register_turn_control(turn, options.authority.clone())?;
                let (turn, conversation, context, step, call_ids) = self
                    .prepare_approval_resume(thread_id, &turn_id, &options, deadline)
                    .await?;
                (turn, conversation, context, step, call_ids, turn_control)
            }
        };

        let model_stream = options
            .model_event_sink
            .clone()
            .map_or_else(ModelStream::disabled, ModelStream::new)
            .with_cancellation(options.cancellation.clone());
        for step in starting_step..self.max_steps {
            let _ = self
                .apply_pending_steering(&mut turn, false, &model_stream, None)
                .await?;
            let mut items = conversation_items.clone();
            items.extend(model_visible_items(&turn.items));
            let request = ModelRequest {
                thread_id: thread_id.clone(),
                turn_id: turn.id.clone(),
                authority: options.authority.clone(),
                items,
                context: context_blocks.clone(),
                tools: self.tools.descriptors(),
            };
            let model_request_sha256 = match model_request_sha256(&request) {
                Ok(value) => value,
                Err(error) => {
                    self.settle_error(&mut turn, &error).await?;
                    return Err(error);
                }
            };

            match self
                .complete_model_routed(
                    ObservationTarget::new(
                        thread_id,
                        &turn.id,
                        "model-route",
                        ExecutionPhase::Model,
                    ),
                    &options.cancellation,
                    deadline,
                    request,
                    &model_stream,
                    u32::try_from(step + 1).unwrap_or(u32::MAX),
                )
                .await
            {
                Ok(SettledModelOutput {
                    model_id,
                    model_origin,
                    continuation,
                    output: ModelOutput::Message { content },
                }) => {
                    let continuation =
                        continuation.map(|continuation| ItemKind::ProviderContinuation {
                            model_id: model_id.clone(),
                            model_origin: model_origin.clone(),
                            continuation,
                        });
                    if self
                        .record_model_decision_if_current(
                            &mut turn,
                            &model_stream,
                            u32::try_from(step + 1).unwrap_or(u32::MAX),
                            continuation,
                            ItemKind::AssistantMessage {
                                model_id: Some(model_id),
                                model_origin: Some(model_origin),
                                content: content.clone(),
                            },
                        )
                        .await?
                    {
                        continue;
                    }
                    let verification_request = VerificationRequest {
                        thread_id: thread_id.clone(),
                        turn_id: turn.id.clone(),
                        items: turn.items.clone(),
                        candidate: content.clone(),
                        cancellation: options.cancellation.clone(),
                    };
                    let mut retry_candidate = false;
                    for registered in self.verification.registered() {
                        let verifier_name = registered.descriptor.name.clone();
                        let outcome = match self
                            .controlled_observed(
                                ObservationTarget::new(
                                    thread_id,
                                    &turn.id,
                                    &verifier_name,
                                    ExecutionPhase::Verification,
                                ),
                                &options.cancellation,
                                deadline,
                                || registered.verifier.verify(verification_request.clone()),
                            )
                            .await
                        {
                            Ok(outcome) => outcome,
                            Err(
                                error @ (HarnessError::Cancelled { .. }
                                | HarnessError::TimedOut { .. }),
                            ) => {
                                self.settle_error(&mut turn, &error).await?;
                                return Err(error);
                            }
                            Err(error) => {
                                let error =
                                    HarnessError::Verification(format!("{verifier_name}: {error}"));
                                self.settle_error(&mut turn, &error).await?;
                                return Err(error);
                            }
                        };
                        if let Err(error) = validate_outcome(&verifier_name, &outcome) {
                            self.settle_error(&mut turn, &error).await?;
                            return Err(error);
                        }
                        self.record(
                            &mut turn,
                            ItemKind::VerificationResult {
                                verifier: verifier_name.clone(),
                                outcome: outcome.clone(),
                            },
                        )
                        .await?;
                        if let VerificationOutcome::Failed { reason, retryable } = outcome {
                            if retryable {
                                retry_candidate = true;
                            } else {
                                let error = HarnessError::Verification(format!(
                                    "{verifier_name}: {reason}"
                                ));
                                self.settle_error(&mut turn, &error).await?;
                                return Err(error);
                            }
                        }
                    }
                    if self
                        .apply_pending_steering(
                            &mut turn,
                            !retry_candidate,
                            &model_stream,
                            Some(u32::try_from(step + 1).unwrap_or(u32::MAX)),
                        )
                        .await?
                    {
                        continue;
                    }
                    if retry_candidate {
                        continue;
                    }
                    self.state
                        .finish_turn_as(&turn, TurnStatus::Completed, &options.authority)
                        .await?;
                    turn.status = TurnStatus::Completed;
                    return Ok(TurnOutcome {
                        turn,
                        final_text: content,
                    });
                }
                Ok(SettledModelOutput {
                    model_id,
                    model_origin,
                    continuation,
                    output:
                        ModelOutput::ToolCall {
                            call_id,
                            name,
                            input,
                        },
                }) => {
                    if tool_call_ids.contains(&call_id) {
                        let error = HarnessError::Model(format!(
                            "model reused Tool call id {call_id:?} within one Turn"
                        ));
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                    let continuation =
                        continuation.map(|continuation| ItemKind::ProviderContinuation {
                            model_id: model_id.clone(),
                            model_origin: model_origin.clone(),
                            continuation,
                        });
                    if self
                        .record_model_decision_if_current(
                            &mut turn,
                            &model_stream,
                            u32::try_from(step + 1).unwrap_or(u32::MAX),
                            continuation,
                            ItemKind::ToolCall {
                                model_id: Some(model_id),
                                model_origin: Some(model_origin),
                                call_id: call_id.clone(),
                                name: name.clone(),
                                input: input.clone(),
                                batch: None,
                            },
                        )
                        .await?
                    {
                        continue;
                    }
                    tool_call_ids.insert(call_id.clone());

                    self.authorize_tool_call(
                        &mut turn,
                        &ModelToolCall {
                            call_id: call_id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                        },
                        &model_request_sha256,
                        &options,
                        deadline,
                    )
                    .await?;

                    if self
                        .supersede_tool_before_effect(&mut turn, &call_id)
                        .await?
                    {
                        continue;
                    }
                    self.execute_tool_call(&mut turn, &name, call_id, input, &options, deadline)
                        .await?;
                }
                Ok(SettledModelOutput {
                    model_id,
                    model_origin,
                    continuation,
                    output: ModelOutput::ToolCalls { calls },
                }) => {
                    if let Some(call) = calls
                        .iter()
                        .find(|call| tool_call_ids.contains(&call.call_id))
                    {
                        let error = HarnessError::Model(format!(
                            "model reused Tool call id {:?} within one Turn",
                            call.call_id
                        ));
                        self.settle_error(&mut turn, &error).await?;
                        return Err(error);
                    }
                    let batch_id = ToolCallBatchId::generate();
                    let batch_size = calls.len();
                    let decisions = calls
                        .iter()
                        .enumerate()
                        .map(|(index, call)| ItemKind::ToolCall {
                            model_id: Some(model_id.clone()),
                            model_origin: Some(model_origin.clone()),
                            call_id: call.call_id.clone(),
                            name: call.name.clone(),
                            input: call.input.clone(),
                            batch: Some(ToolCallBatch {
                                id: batch_id.clone(),
                                index,
                                size: batch_size,
                            }),
                        })
                        .collect::<Vec<_>>();
                    let continuation =
                        continuation.map(|continuation| ItemKind::ProviderContinuation {
                            model_id,
                            model_origin,
                            continuation,
                        });
                    if self
                        .record_model_tool_batch_if_current(
                            &mut turn,
                            &model_stream,
                            u32::try_from(step + 1).unwrap_or(u32::MAX),
                            continuation,
                            decisions,
                        )
                        .await?
                    {
                        continue;
                    }
                    for call in &calls {
                        tool_call_ids.insert(call.call_id.clone());
                        self.authorize_tool_call(
                            &mut turn,
                            call,
                            &model_request_sha256,
                            &options,
                            deadline,
                        )
                        .await?;
                    }

                    if self
                        .execute_tool_batch(&mut turn, &calls, &options, deadline)
                        .await?
                    {
                        continue;
                    }
                }
                Err(error) => {
                    self.settle_error(&mut turn, &error).await?;
                    return Err(error);
                }
            }
        }

        let error = HarnessError::MaxSteps(self.max_steps);
        self.settle_error(&mut turn, &error).await?;
        Err(error)
    }

    async fn prepare_approval_resume(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
        options: &TurnExecutionOptions,
        deadline: Option<Instant>,
    ) -> Result<PreparedExecution, HarnessError> {
        let memory_scope = options.validated_memory_scope()?;
        let thread = self
            .load_thread_as(thread_id, &options.authority)
            .await?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        let capacity = self
            .state
            .thread_capacity_as(thread_id, &options.authority)
            .await?
            .ok_or_else(|| HarnessError::State(format!("thread {thread_id} does not exist")))?;
        require_runtime_capacity(&capacity)?;
        let mut running = thread
            .turns
            .iter()
            .filter(|turn| turn.status == TurnStatus::Running);
        let turn = running
            .next()
            .filter(|turn| &turn.id == turn_id)
            .cloned()
            .ok_or_else(|| {
                HarnessError::State(format!(
                    "turn {turn_id} is not the running turn in thread {thread_id}"
                ))
            })?;
        if running.next().is_some() {
            return Err(HarnessError::State(
                "thread projection contains multiple running turns".to_owned(),
            ));
        }
        require_execution_binding(&turn, options.execution_binding.as_ref())?;
        let evidence = approval_resume_evidence(&turn, options.authority.actor())?;
        if !self
            .models
            .contains(&evidence.model_id, &evidence.model_origin)
        {
            return Err(HarnessError::State(
                "approval continuation Model identity or origin is absent from the configured route"
                    .to_owned(),
            ));
        }
        let registered = self.tools.get(&evidence.tool).ok_or_else(|| {
            HarnessError::State(format!(
                "approval continuation Tool {} is not registered",
                evidence.tool
            ))
        })?;
        if registered.origin != evidence.tool_origin {
            return Err(HarnessError::State(
                "approval continuation Tool origin changed after restart".to_owned(),
            ));
        }

        let mut prior_thread = thread.clone();
        prior_thread
            .turns
            .retain(|candidate| &candidate.id != turn_id);
        let conversation = self.context.compile_conversation(&prior_thread)?;
        let prompt = turn_prompt(&turn)?;
        let conversation_summary =
            if let Some(compactor) = self.context.conversation_compactor_name(&conversation) {
                self.controlled_observed(
                    ObservationTarget::new(thread_id, turn_id, compactor, ExecutionPhase::Context),
                    &options.cancellation,
                    deadline,
                    || {
                        self.context.compile_conversation_summary(
                            &conversation,
                            &prompt,
                            options.cancellation.clone(),
                        )
                    },
                )
                .await?
            } else {
                None
            };
        let compilation = self
            .controlled_observed(
                ObservationTarget::new(
                    thread_id,
                    turn_id,
                    "context-engine",
                    ExecutionPhase::Context,
                ),
                &options.cancellation,
                deadline,
                || self.context.compile(&prompt, memory_scope.clone()),
            )
            .await?;
        let compilation = self
            .context
            .merge_conversation_summary(compilation, conversation_summary)?;
        let compilation = self
            .context
            .merge_turn_context(compilation, &options.context)?;
        let mut original_items = conversation.items.clone();
        original_items.extend(model_visible_items(
            &turn.items[..evidence.model_request_item_index],
        ));
        let original_request = ModelRequest {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            authority: options.authority.clone(),
            items: original_items,
            context: compilation.blocks.clone(),
            tools: self.tools.descriptors(),
        };
        if model_request_sha256(&original_request)? != evidence.model_request_sha256 {
            return Err(HarnessError::State(
                "approval continuation Model request changed after restart".to_owned(),
            ));
        }

        let request = ApprovalRequest {
            id: evidence.approval_id.clone(),
            requested_by: evidence.requested_by,
            authorization: ToolAuthorization {
                thread_id: thread_id.clone(),
                turn_id: turn_id.clone(),
                call_id: evidence.call_id.clone(),
                descriptor: registered.descriptor.clone(),
                origin: registered.origin.clone(),
                input: evidence.input.clone(),
            },
            reason: evidence.reason,
            risk: evidence.risk,
        };
        let approval = self
            .controlled_observed(
                ObservationTarget::new(
                    thread_id,
                    turn_id,
                    "approval-handler",
                    ExecutionPhase::Approval,
                ),
                &options.cancellation,
                deadline,
                || self.approvals.decide_as(&request, &options.authority),
            )
            .await?;
        validate_approval_decision(&approval)?;

        let mut turn = turn;
        self.record(
            &mut turn,
            ItemKind::ApprovalDecision {
                approval_id: evidence.approval_id,
                call_id: evidence.call_id.clone(),
                decision: approval.clone(),
            },
        )
        .await?;
        if let ApprovalDecision::Deny { reason } = approval {
            let error = HarnessError::ApprovalDenied {
                tool: evidence.tool,
                reason,
            };
            self.settle_error(&mut turn, &error).await?;
            return Err(error);
        }
        for call in &evidence.batch_calls[evidence.current_batch_index + 1..] {
            self.authorize_tool_call(
                &mut turn,
                call,
                &evidence.model_request_sha256,
                options,
                deadline,
            )
            .await?;
        }
        validate_resumed_batch_authority(
            &turn,
            &evidence.batch_calls[..=evidence.current_batch_index],
            &self.tools,
        )?;
        let _ = self
            .execute_tool_batch(&mut turn, &evidence.batch_calls, options, deadline)
            .await?;

        Ok((
            turn,
            conversation.items,
            compilation.blocks,
            evidence.consumed_steps,
            evidence.tool_call_ids,
        ))
    }

    async fn execute_tool_call(
        &self,
        turn: &mut Turn,
        name: &str,
        call_id: String,
        input: Value,
        options: &TurnExecutionOptions,
        deadline: Option<Instant>,
    ) -> Result<(), HarnessError> {
        let registered = self
            .tools
            .get(name)
            .ok_or_else(|| HarnessError::UnknownTool(name.to_owned()))?;
        let call = ModelToolCall {
            call_id,
            name: name.to_owned(),
            input,
        };
        match invoke_tool_capability(
            ToolCapabilityInvocation {
                tool: registered.tool.clone(),
                origin: registered.origin.clone(),
                cancellation_settlement_timeout: registered.cancellation_settlement_timeout,
                call,
            },
            self.observability.clone(),
            turn.thread_id.clone(),
            turn.id.clone(),
            options.authority.clone(),
            options.cancellation.clone(),
            deadline,
        )
        .await
        {
            Ok(settlement) => self.record_tool_settlement(turn, settlement).await,
            Err(error) => {
                self.settle_error(turn, &error).await?;
                Err(error)
            }
        }
    }

    async fn execute_tool_batch(
        &self,
        turn: &mut Turn,
        calls: &[ModelToolCall],
        options: &TurnExecutionOptions,
        deadline: Option<Instant>,
    ) -> Result<bool, HarnessError> {
        let mut index = 0;
        while index < calls.len() {
            let registered = self
                .tools
                .get(&calls[index].name)
                .ok_or_else(|| HarnessError::UnknownTool(calls[index].name.clone()))?;
            if self.max_parallel_tool_calls == 1
                || registered.batch_execution == ToolBatchExecution::Sequential
            {
                let pending = calls[index..]
                    .iter()
                    .map(|call| call.call_id.as_str())
                    .collect::<Vec<_>>();
                if self
                    .supersede_tool_calls_before_effect(turn, &pending)
                    .await?
                {
                    return Ok(true);
                }
                let call = &calls[index];
                self.execute_tool_call(
                    turn,
                    &call.name,
                    call.call_id.clone(),
                    call.input.clone(),
                    options,
                    deadline,
                )
                .await?;
                index += 1;
                continue;
            }

            let mut end = index + 1;
            while end < calls.len()
                && self
                    .tools
                    .get(&calls[end].name)
                    .is_some_and(|tool| tool.batch_execution == ToolBatchExecution::ParallelSafe)
            {
                end += 1;
            }
            let pending = calls[index..]
                .iter()
                .map(|call| call.call_id.as_str())
                .collect::<Vec<_>>();
            if self
                .supersede_tool_calls_before_effect(turn, &pending)
                .await?
            {
                return Ok(true);
            }
            if end == index + 1 {
                let call = &calls[index];
                self.execute_tool_call(
                    turn,
                    &call.name,
                    call.call_id.clone(),
                    call.input.clone(),
                    options,
                    deadline,
                )
                .await?;
            } else {
                self.execute_parallel_tool_calls(turn, &calls[index..end], options, deadline)
                    .await?;
            }
            index = end;
        }
        Ok(false)
    }

    async fn execute_parallel_tool_calls(
        &self,
        turn: &mut Turn,
        calls: &[ModelToolCall],
        options: &TurnExecutionOptions,
        deadline: Option<Instant>,
    ) -> Result<(), HarnessError> {
        let mut jobs = Vec::with_capacity(calls.len());
        for (index, call) in calls.iter().cloned().enumerate() {
            let Some(registered) = self.tools.get(&call.name) else {
                let error = HarnessError::UnknownTool(call.name);
                self.settle_error(turn, &error).await?;
                return Err(error);
            };
            jobs.push((
                index,
                ToolCapabilityInvocation {
                    tool: registered.tool.clone(),
                    origin: registered.origin.clone(),
                    cancellation_settlement_timeout: registered.cancellation_settlement_timeout,
                    call,
                },
            ));
        }
        let semaphore = Arc::new(Semaphore::new(self.max_parallel_tool_calls));
        let mut tasks = JoinSet::new();
        for (index, invocation) in jobs {
            let semaphore = semaphore.clone();
            let observability = self.observability.clone();
            let thread_id = turn.thread_id.clone();
            let turn_id = turn.id.clone();
            let authority = options.authority.clone();
            let cancellation = options.cancellation.clone();
            tasks.spawn(async move {
                let result = match semaphore.acquire_owned().await {
                    Ok(permit) => {
                        let result = invoke_tool_capability(
                            invocation,
                            observability,
                            thread_id,
                            turn_id,
                            authority,
                            cancellation,
                            deadline,
                        )
                        .await;
                        drop(permit);
                        result
                    }
                    Err(_) => Err(HarnessError::Tool(
                        "parallel Tool scheduler closed unexpectedly".to_owned(),
                    )),
                };
                (index, result)
            });
        }

        let mut results = std::iter::repeat_with(|| None)
            .take(calls.len())
            .collect::<Vec<Option<Result<ToolCallSettlement, HarnessError>>>>();
        while let Some(joined) = tasks.join_next().await {
            let (index, result) = match joined {
                Ok(result) => result,
                Err(_) => {
                    tasks.shutdown().await;
                    let error = HarnessError::CapabilityPanicked {
                        phase: ExecutionPhase::Tool,
                    };
                    self.settle_error(turn, &error).await?;
                    return Err(error);
                }
            };
            let Some(slot) = results.get_mut(index) else {
                tasks.shutdown().await;
                let error = HarnessError::State("parallel Tool result index is invalid".to_owned());
                self.settle_error(turn, &error).await?;
                return Err(error);
            };
            if slot.replace(result).is_some() {
                tasks.shutdown().await;
                let error = HarnessError::State("parallel Tool result index was reused".to_owned());
                self.settle_error(turn, &error).await?;
                return Err(error);
            }
        }

        let mut stop_error = None;
        for result in results {
            let Some(result) = result else {
                let error = HarnessError::State("parallel Tool result is missing".to_owned());
                self.settle_error(turn, &error).await?;
                return Err(error);
            };
            match result {
                Ok(settlement) => self.record_tool_settlement(turn, settlement).await?,
                Err(error) if stop_error.is_none() => stop_error = Some(error),
                Err(_) => {}
            }
        }
        if let Some(error) = stop_error {
            self.settle_error(turn, &error).await?;
            return Err(error);
        }
        Ok(())
    }

    async fn record_tool_settlement(
        &self,
        turn: &mut Turn,
        settlement: ToolCallSettlement,
    ) -> Result<(), HarnessError> {
        self.record(
            turn,
            ItemKind::ToolResult {
                call_id: settlement.call_id,
                output: settlement.output,
                is_error: settlement.is_error,
                connector_evidence: settlement.connector_evidence,
            },
        )
        .await
    }

    async fn authorize_tool_call(
        &self,
        turn: &mut Turn,
        call: &ModelToolCall,
        model_request_sha256: &str,
        options: &TurnExecutionOptions,
        deadline: Option<Instant>,
    ) -> Result<(), HarnessError> {
        let Some(registered) = self.tools.get(&call.name) else {
            let error = HarnessError::UnknownTool(call.name.clone());
            self.settle_error(turn, &error).await?;
            return Err(error);
        };
        let authorization = ToolAuthorization {
            thread_id: turn.thread_id.clone(),
            turn_id: turn.id.clone(),
            call_id: call.call_id.clone(),
            descriptor: registered.descriptor.clone(),
            origin: registered.origin.clone(),
            input: call.input.clone(),
        };
        let decision = match self
            .controlled_observed(
                ObservationTarget::new(
                    &turn.thread_id,
                    &turn.id,
                    "policy-engine",
                    ExecutionPhase::Policy,
                ),
                &options.cancellation,
                deadline,
                || self.policy.authorize(&authorization, &options.authority),
            )
            .await
        {
            Ok(decision) => decision,
            Err(error) => {
                self.settle_error(turn, &error).await?;
                return Err(error);
            }
        };
        if let Err(error) = validate_policy_decision(&decision) {
            self.settle_error(turn, &error).await?;
            return Err(error);
        }
        self.record(
            turn,
            ItemKind::PolicyDecision {
                call_id: call.call_id.clone(),
                tool_origin: Some(authorization.origin.clone()),
                decision: decision.clone(),
            },
        )
        .await?;

        match decision {
            PolicyDecision::Allow => Ok(()),
            PolicyDecision::Deny { reason } => {
                let error = HarnessError::PolicyDenied {
                    tool: call.name.clone(),
                    reason,
                };
                self.settle_error(turn, &error).await?;
                Err(error)
            }
            PolicyDecision::Ask { reason, risk } => {
                let request = ApprovalRequest {
                    id: ApprovalId::generate(),
                    requested_by: options.authority.actor().clone(),
                    authorization,
                    reason,
                    risk,
                };
                self.record(
                    turn,
                    ItemKind::ApprovalRequested {
                        approval_id: request.id.clone(),
                        call_id: call.call_id.clone(),
                        tool: call.name.clone(),
                        reason: request.reason.clone(),
                        risk: request.risk,
                        requested_by: Some(request.requested_by.clone()),
                        tool_origin: Some(request.authorization.origin.clone()),
                        model_request_sha256: Some(model_request_sha256.to_owned()),
                    },
                )
                .await?;
                let approval = match self
                    .controlled_observed(
                        ObservationTarget::new(
                            &turn.thread_id,
                            &turn.id,
                            "approval-handler",
                            ExecutionPhase::Approval,
                        ),
                        &options.cancellation,
                        deadline,
                        || self.approvals.decide_as(&request, &options.authority),
                    )
                    .await
                {
                    Ok(approval) => approval,
                    Err(error) => {
                        let abandonment = self
                            .approvals
                            .abandon_turn_as(
                                &turn.thread_id,
                                &turn.id,
                                "approval wait ended without a settlement",
                                &options.authority,
                            )
                            .await;
                        self.settle_error(turn, &error).await?;
                        abandonment?;
                        return Err(error);
                    }
                };
                if let Err(error) = validate_approval_decision(&approval) {
                    self.settle_error(turn, &error).await?;
                    return Err(error);
                }
                self.record(
                    turn,
                    ItemKind::ApprovalDecision {
                        approval_id: request.id,
                        call_id: call.call_id.clone(),
                        decision: approval.clone(),
                    },
                )
                .await?;
                if let ApprovalDecision::Deny { reason } = approval {
                    let error = HarnessError::ApprovalDenied {
                        tool: call.name.clone(),
                        reason,
                    };
                    self.settle_error(turn, &error).await?;
                    return Err(error);
                }
                Ok(())
            }
        }
    }

    async fn controlled_observed<C, F, T>(
        &self,
        target: ObservationTarget<'_>,
        cancellation: &crate::CancellationToken,
        deadline: Option<Instant>,
        operation: C,
    ) -> Result<T, HarnessError>
    where
        C: FnOnce() -> F,
        F: Future<Output = Result<T, HarnessError>>,
    {
        let started = Instant::now();
        let result = controlled(cancellation, deadline, target.phase, operation).await;
        self.observability.emit(&PhaseObservation {
            thread_id: target.thread_id.clone(),
            turn_id: target.turn_id.clone(),
            phase: target.phase,
            capability: target.capability.to_owned(),
            duration_micros: elapsed_micros(started),
            outcome: observation_outcome(&result),
            model_usage: None,
            provider_model: None,
            provider_request_id: None,
            provider_failure_kind: None,
            provider_status_code: None,
            provider_retry_after_ms: None,
            model_retry_index: None,
            stream_events_dropped: 0,
        });
        result
    }

    async fn complete_model_routed(
        &self,
        target: ObservationTarget<'_>,
        cancellation: &crate::CancellationToken,
        deadline: Option<Instant>,
        request: ModelRequest,
        stream: &ModelStream,
        model_step: u32,
    ) -> Result<SettledModelOutput, HarnessError> {
        validate_model_request(&request)?;
        let continuation_target = pending_provider_continuation_target(&request.items)?;
        let mut candidates = self
            .models
            .entries
            .iter()
            .filter(|registered| {
                continuation_target.as_ref().is_none_or(|target| {
                    matches!(
                        &registered.identity,
                        FrozenModelIdentity::Valid(id) if id == &target.model_id
                    ) && registered.origin == target.model_origin
                })
            })
            .collect::<Vec<_>>();
        if let Some(target) = &continuation_target
            && candidates.is_empty()
        {
            return Err(HarnessError::State(format!(
                "provider continuation requires unavailable Model {} from its recorded origin",
                target.model_id
            )));
        }
        let mut cooling_fallbacks = Vec::new();
        let mut non_cooling_count = candidates.len();
        if continuation_target.is_none() {
            let (mut available, cooling) = self
                .models
                .partition_timeout_cooldown(candidates, Instant::now())?;
            non_cooling_count = available.len();
            available.extend(cooling.iter().copied());
            candidates = available;
            cooling_fallbacks = cooling;
        }
        let mut request = Some(request);
        let total = candidates.len();
        let mut attempts = 0_usize;
        for (index, registered) in candidates.into_iter().enumerate() {
            let model_id = registered.identity.get()?;
            let (attempt_deadline, attempt_timeout_elapsed) =
                self.models.attempt_deadline(deadline)?;
            let mut attempt_request = if index + 1 == total {
                request.take().ok_or_else(|| {
                    HarnessError::Model("Model route lost its bounded request".to_owned())
                })?
            } else {
                request
                    .as_ref()
                    .ok_or_else(|| {
                        HarnessError::Model("Model route lost its bounded request".to_owned())
                    })?
                    .clone()
            };
            retain_model_continuations(&mut attempt_request, model_id, &registered.origin);
            let retry_template = self.models.retry_policy.map(|_| attempt_request.clone());
            let mut initial_request = Some(attempt_request);
            let mut retry_index = 0_u8;
            loop {
                if attempts == self.max_model_attempts_per_step {
                    return Err(HarnessError::MaxModelAttempts(
                        self.max_model_attempts_per_step,
                    ));
                }
                attempts += 1;
                let request = if retry_index == 0 {
                    initial_request.take().ok_or_else(|| {
                        HarnessError::Model(
                            "Model retry lost its bounded initial request".to_owned(),
                        )
                    })?
                } else {
                    retry_template
                        .as_ref()
                        .ok_or_else(|| {
                            HarnessError::Model(
                                "Model retry lost its bounded request template".to_owned(),
                            )
                        })?
                        .clone()
                };
                let attempt_stream = stream
                    .for_step(model_step)
                    .with_cancellation(crate::CancellationToken::new());
                let delivered_before = attempt_stream.delivered_events();
                let settlement = self
                    .complete_model_attempt_observed(
                        ObservationTarget::new(
                            target.thread_id,
                            target.turn_id,
                            model_id,
                            ExecutionPhase::Model,
                        ),
                        registered,
                        cancellation,
                        attempt_deadline,
                        ModelAttemptInvocation {
                            request,
                            stream: attempt_stream.clone(),
                            retry_index,
                        },
                    )
                    .await;
                let delivered = attempt_stream
                    .delivered_events()
                    .saturating_sub(delivered_before);
                if settlement.control == ModelAttemptControl::Cancelled
                    || (settlement.control == ModelAttemptControl::DeadlineElapsed
                        && !attempt_timeout_elapsed)
                {
                    return settlement.result.map(|response| SettledModelOutput {
                        model_id: model_id.to_owned(),
                        model_origin: registered.origin.clone(),
                        output: response.output,
                        continuation: response.continuation,
                    });
                }
                if settlement.control == ModelAttemptControl::DeadlineElapsed {
                    self.models
                        .record_attempt_timeout(model_id, Instant::now())?;
                }
                let result = if settlement.control == ModelAttemptControl::DeadlineElapsed {
                    Err(HarnessError::Model(format!(
                        "Model {model_id} exceeded the configured failover attempt timeout"
                    )))
                } else {
                    settlement.result
                };
                match result {
                    Ok(response) => {
                        self.models.clear_attempt_timeout(model_id);
                        if index < non_cooling_count {
                            for cooling in &cooling_fallbacks {
                                self.observability.emit(&PhaseObservation {
                                    thread_id: target.thread_id.clone(),
                                    turn_id: target.turn_id.clone(),
                                    phase: ExecutionPhase::Model,
                                    capability: cooling.identity.get()?.to_owned(),
                                    duration_micros: 0,
                                    outcome: ObservationOutcome::Skipped,
                                    model_usage: None,
                                    provider_model: None,
                                    provider_request_id: None,
                                    provider_failure_kind: None,
                                    provider_status_code: None,
                                    provider_retry_after_ms: None,
                                    model_retry_index: None,
                                    stream_events_dropped: 0,
                                });
                            }
                        }
                        return Ok(SettledModelOutput {
                            model_id: model_id.to_owned(),
                            model_origin: registered.origin.clone(),
                            output: response.output,
                            continuation: response.continuation,
                        });
                    }
                    Err(_) if index + 1 < total && delivered > 0 => {
                        return Err(HarnessError::Model(format!(
                            "Model {model_id} failed after delivering provisional output; failover was suppressed"
                        )));
                    }
                    Err(error) => {
                        let next_retry = retry_index.saturating_add(1);
                        if delivered == 0
                            && let Some(delay) = self
                                .models
                                .retry_delay(&error, next_retry, &target, model_id)
                            && wait_for_model_retry(
                                cancellation,
                                attempt_deadline,
                                attempt_timeout_elapsed,
                                delay,
                            )
                            .await?
                        {
                            retry_index = next_retry;
                            continue;
                        }
                        if index + 1 == total {
                            return Err(error);
                        }
                        break;
                    }
                }
            }
        }
        Err(HarnessError::InvalidConfiguration(
            "Model route must contain at least one Model".to_owned(),
        ))
    }

    async fn complete_model_attempt_observed(
        &self,
        target: ObservationTarget<'_>,
        registered: &RuntimeModel,
        cancellation: &crate::CancellationToken,
        deadline: Option<Instant>,
        invocation: ModelAttemptInvocation,
    ) -> ModelAttemptSettlement {
        let ModelAttemptInvocation {
            request,
            stream,
            retry_index,
        } = invocation;
        let started = Instant::now();
        let dropped_before = stream.dropped_events();
        let provider_stream = stream.clone();
        let controlled_result = controlled_with_settlement_cancellation(
            cancellation,
            stream.cancellation_token(),
            deadline,
            ExecutionPhase::Model,
            || async {
                Ok(registered
                    .model
                    .complete_streaming(request, provider_stream)
                    .await)
            },
        )
        .await;
        stream.close();
        let stream_events_dropped = stream.dropped_events().saturating_sub(dropped_before);
        let (result, control) = match controlled_result {
            Ok(result) => (result, ModelAttemptControl::None),
            Err(error @ HarnessError::Cancelled { .. }) => {
                (Err(error), ModelAttemptControl::Cancelled)
            }
            Err(error @ HarnessError::TimedOut { .. }) => {
                (Err(error), ModelAttemptControl::DeadlineElapsed)
            }
            Err(error) => (Err(error), ModelAttemptControl::None),
        };
        let result = result
            .map_err(validate_model_attempt_error)
            .and_then(|response| {
                validate_model_response(&response)?;
                Ok(response)
            });
        let (model_usage, provider_model, provider_request_id) =
            result.as_ref().map_or((None, None, None), |response| {
                (
                    response.usage.clone(),
                    response.provider_model.clone(),
                    response.provider_request_id.clone(),
                )
            });
        let (provider_failure_kind, provider_status_code, provider_retry_after_ms) = match &result {
            Err(HarnessError::ModelProvider(failure)) => (
                Some(failure.kind()),
                failure.http_status(),
                failure.retry_after_ms(),
            ),
            _ => (None, None, None),
        };
        self.observability.emit(&PhaseObservation {
            thread_id: target.thread_id.clone(),
            turn_id: target.turn_id.clone(),
            phase: ExecutionPhase::Model,
            capability: target.capability.to_owned(),
            duration_micros: elapsed_micros(started),
            outcome: observation_outcome(&result),
            model_usage,
            provider_model,
            provider_request_id,
            provider_failure_kind,
            provider_status_code,
            provider_retry_after_ms,
            model_retry_index: Some(retry_index),
            stream_events_dropped,
        });
        ModelAttemptSettlement { result, control }
    }

    fn claim_thread(&self, thread_id: &ThreadId) -> Result<ActiveThreadGuard<'_>, HarnessError> {
        let mut active = self
            .active_threads
            .lock()
            .map_err(|_| HarnessError::State("active thread lock poisoned".to_owned()))?;
        if !active.insert(thread_id.clone()) {
            return Err(HarnessError::State(format!(
                "thread {thread_id} already has an active turn"
            )));
        }
        if active.len() > self.max_concurrent_turns {
            active.remove(thread_id);
            return Err(HarnessError::RuntimeOverloaded {
                limit: self.max_concurrent_turns,
            });
        }
        Ok(ActiveThreadGuard {
            active: &self.active_threads,
            thread_id: thread_id.clone(),
        })
    }

    fn register_turn_control(
        &self,
        turn: &Turn,
        authority: AuthorityContext,
    ) -> Result<TurnControlGuard<'_>, HarnessError> {
        let pending_steering = pending_steering_from_items(&turn.items)?;
        let pending_steering_bytes =
            pending_steering
                .iter()
                .try_fold(0_usize, |total, steering| {
                    total.checked_add(steering.content.len()).ok_or_else(|| {
                        HarnessError::State("pending steering byte count overflow".to_owned())
                    })
                })?;
        if pending_steering.len() > MAX_PENDING_STEERING
            || pending_steering_bytes > MAX_PENDING_STEERING_BYTES
        {
            return Err(HarnessError::State(
                "recovered Turn exceeds pending steering capacity".to_owned(),
            ));
        }
        let control = Arc::new(tokio::sync::Mutex::new(ActiveTurnControl {
            turn_id: turn.id.clone(),
            authority,
            accepting_steering: true,
            pending_steering,
            pending_steering_bytes,
        }));
        let mut controls = self
            .turn_controls
            .lock()
            .map_err(|_| HarnessError::State("Turn control lock poisoned".to_owned()))?;
        if controls.contains_key(&turn.thread_id) {
            return Err(HarnessError::State(format!(
                "thread {} already has an active Turn control",
                turn.thread_id
            )));
        }
        controls.insert(turn.thread_id.clone(), control);
        Ok(TurnControlGuard {
            controls: &self.turn_controls,
            thread_id: turn.thread_id.clone(),
        })
    }

    fn turn_control(
        &self,
        thread_id: &ThreadId,
    ) -> Result<Arc<tokio::sync::Mutex<ActiveTurnControl>>, HarnessError> {
        self.turn_controls
            .lock()
            .map_err(|_| HarnessError::State("Turn control lock poisoned".to_owned()))?
            .get(thread_id)
            .cloned()
            .ok_or_else(|| {
                HarnessError::State(format!("thread {thread_id} has no active steerable Turn"))
            })
    }

    async fn apply_pending_steering(
        &self,
        turn: &mut Turn,
        seal_if_empty: bool,
        model_stream: &ModelStream,
        invalidated_model_step: Option<u32>,
    ) -> Result<bool, HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        if control.pending_steering.is_empty() {
            if seal_if_empty {
                control.accepting_steering = false;
            }
            return Ok(false);
        }
        if let Err(error) = self.apply_pending_steering_locked(turn, &mut control).await {
            control.accepting_steering = false;
            return Err(error);
        }
        if let Some(model_step) = invalidated_model_step {
            model_stream.invalidate_step(model_step);
        }
        Ok(true)
    }

    async fn supersede_tool_before_effect(
        &self,
        turn: &mut Turn,
        call_id: &str,
    ) -> Result<bool, HarnessError> {
        self.supersede_tool_calls_before_effect(turn, &[call_id])
            .await
    }

    async fn supersede_tool_calls_before_effect(
        &self,
        turn: &mut Turn,
        call_ids: &[&str],
    ) -> Result<bool, HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        if control.pending_steering.is_empty() {
            return Ok(false);
        }
        for call_id in call_ids {
            if let Err(error) = self
                .record_unlocked(
                    turn,
                    ItemKind::ToolResult {
                        call_id: (*call_id).to_owned(),
                        output: serde_json::json!({
                            "error": "tool call superseded by user steering before execution"
                        }),
                        is_error: true,
                        connector_evidence: Vec::new(),
                    },
                    &control.authority,
                )
                .await
            {
                control.accepting_steering = false;
                return Err(error);
            }
        }
        if let Err(error) = self.apply_pending_steering_locked(turn, &mut control).await {
            control.accepting_steering = false;
            return Err(error);
        }
        Ok(true)
    }

    async fn record_model_decision_if_current(
        &self,
        turn: &mut Turn,
        model_stream: &ModelStream,
        model_step: u32,
        continuation: Option<ItemKind>,
        decision: ItemKind,
    ) -> Result<bool, HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        if !control.pending_steering.is_empty() {
            if let Err(error) = self.apply_pending_steering_locked(turn, &mut control).await {
                control.accepting_steering = false;
                return Err(error);
            }
            model_stream.invalidate_step(model_step);
            return Ok(true);
        }
        if let Some(continuation) = continuation
            && let Err(error) = self
                .record_unlocked(turn, continuation, &control.authority)
                .await
        {
            control.accepting_steering = false;
            return Err(error);
        }
        if let Err(error) = self
            .record_unlocked(turn, decision, &control.authority)
            .await
        {
            control.accepting_steering = false;
            return Err(error);
        }
        Ok(false)
    }

    async fn record_model_tool_batch_if_current(
        &self,
        turn: &mut Turn,
        model_stream: &ModelStream,
        model_step: u32,
        continuation: Option<ItemKind>,
        decisions: Vec<ItemKind>,
    ) -> Result<bool, HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        if !control.pending_steering.is_empty() {
            if let Err(error) = self.apply_pending_steering_locked(turn, &mut control).await {
                control.accepting_steering = false;
                return Err(error);
            }
            model_stream.invalidate_step(model_step);
            return Ok(true);
        }
        if let Some(continuation) = continuation
            && let Err(error) = self
                .record_unlocked(turn, continuation, &control.authority)
                .await
        {
            control.accepting_steering = false;
            return Err(error);
        }
        let calls = decisions.into_iter().map(Item::new).collect();
        if let Err(error) = self
            .record_tool_calls_unlocked(turn, calls, &control.authority)
            .await
        {
            control.accepting_steering = false;
            return Err(error);
        }
        Ok(false)
    }

    async fn apply_pending_steering_locked(
        &self,
        turn: &mut Turn,
        control: &mut ActiveTurnControl,
    ) -> Result<(), HarnessError> {
        while let Some(steering) = control.pending_steering.front().cloned() {
            let remaining_bytes = control
                .pending_steering_bytes
                .checked_sub(steering.content.len())
                .ok_or_else(|| {
                    HarnessError::State(
                        "pending steering byte count is internally inconsistent".to_owned(),
                    )
                })?;
            self.record_unlocked(
                turn,
                ItemKind::SteeringApplied {
                    steering_id: steering.steering_id,
                    content: steering.content,
                },
                &control.authority,
            )
            .await?;
            let _ = control.pending_steering.pop_front();
            control.pending_steering_bytes = remaining_bytes;
        }
        let thread = self
            .state
            .load_thread_as(&turn.thread_id, &control.authority)
            .await?
            .ok_or_else(|| HarnessError::State(format!("thread {} disappeared", turn.thread_id)))?;
        *turn = thread
            .turns
            .into_iter()
            .find(|candidate| candidate.id == turn.id)
            .ok_or_else(|| HarnessError::State(format!("turn {} disappeared", turn.id)))?;
        Ok(())
    }

    async fn record(&self, turn: &mut Turn, kind: ItemKind) -> Result<(), HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        let result = self.record_unlocked(turn, kind, &control.authority).await;
        if result.is_err() {
            control.accepting_steering = false;
        }
        result
    }

    async fn record_unlocked(
        &self,
        turn: &mut Turn,
        kind: ItemKind,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        let item = Item::new(kind);
        match self
            .state
            .append_item_as(turn, item.clone(), authority)
            .await
        {
            Ok(_) => {
                turn.items.push(item);
                Ok(())
            }
            Err(record_error) => match self
                .state
                .finish_turn_as(turn, TurnStatus::Failed, authority)
                .await
            {
                Ok(_) => {
                    turn.status = TurnStatus::Failed;
                    Err(record_error)
                }
                Err(settlement_error) => Err(HarnessError::State(format!(
                    "State Item append failed ({record_error}); terminal settlement also failed ({settlement_error})"
                ))),
            },
        }
    }

    async fn record_tool_calls_unlocked(
        &self,
        turn: &mut Turn,
        calls: Vec<Item>,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        match self
            .state
            .append_tool_calls_as(turn, calls.clone(), authority)
            .await
        {
            Ok(_) => {
                turn.items.extend(calls);
                Ok(())
            }
            Err(record_error) => match self
                .state
                .finish_turn_as(turn, TurnStatus::Failed, authority)
                .await
            {
                Ok(_) => {
                    turn.status = TurnStatus::Failed;
                    Err(record_error)
                }
                Err(settlement_error) => Err(HarnessError::State(format!(
                    "State Tool-call batch append failed ({record_error}); terminal settlement also failed ({settlement_error})"
                ))),
            },
        }
    }

    async fn settle_error(
        &self,
        turn: &mut Turn,
        error: &HarnessError,
    ) -> Result<(), HarnessError> {
        let control = self.turn_control(&turn.thread_id)?;
        let mut control = control.lock().await;
        require_control_turn(&control, turn)?;
        control.accepting_steering = false;
        let (item, status) = match error {
            HarnessError::Cancelled { phase } => (
                ItemKind::TurnStopped {
                    reason: TurnStopReason::Cancelled,
                    phase: *phase,
                },
                TurnStatus::Cancelled,
            ),
            HarnessError::TimedOut { phase } => (
                ItemKind::TurnStopped {
                    reason: TurnStopReason::TimedOut,
                    phase: *phase,
                },
                TurnStatus::TimedOut,
            ),
            _ => (
                ItemKind::RuntimeError {
                    message: bounded_runtime_error(&error.to_string()),
                },
                TurnStatus::Failed,
            ),
        };
        let settlement_item = Item::new(item);
        match self
            .state
            .append_item_as(turn, settlement_item.clone(), &control.authority)
            .await
        {
            Ok(_) => turn.items.push(settlement_item),
            Err(record_error) => {
                return match self
                    .state
                    .finish_turn_as(turn, status.clone(), &control.authority)
                    .await
                {
                    Ok(_) => {
                        turn.status = status;
                        Ok(())
                    }
                    Err(settlement_error) => Err(HarnessError::State(format!(
                        "State settlement evidence failed ({record_error}); terminal settlement also failed ({settlement_error})"
                    ))),
                };
            }
        }
        self.state
            .finish_turn_as(turn, status.clone(), &control.authority)
            .await?;
        turn.status = status;
        Ok(())
    }
}

type PreparedExecution = (Turn, Vec<Item>, Vec<ContextBlock>, usize, BTreeSet<String>);

enum TurnEntry {
    Start(String),
    ResumeApproval(TurnId),
}

struct ApprovalResumeEvidence {
    approval_id: ApprovalId,
    call_id: String,
    tool: String,
    input: Value,
    reason: String,
    risk: crate::RiskLevel,
    requested_by: crate::ApprovalActor,
    tool_origin: crate::CapabilityOrigin,
    model_id: String,
    model_origin: crate::CapabilityOrigin,
    model_request_sha256: String,
    model_request_item_index: usize,
    batch_calls: Vec<ModelToolCall>,
    current_batch_index: usize,
    consumed_steps: usize,
    tool_call_ids: BTreeSet<String>,
}

fn require_execution_binding(
    turn: &Turn,
    expected: Option<&crate::ExecutionBinding>,
) -> Result<(), HarnessError> {
    let mut recorded = turn.items.iter().filter_map(|item| {
        if let ItemKind::ExecutionBinding { binding, .. } = &item.kind {
            Some(binding)
        } else {
            None
        }
    });
    let actual = recorded.next();
    if recorded.next().is_some() {
        return Err(HarnessError::State(
            "Turn contains multiple execution bindings".to_owned(),
        ));
    }
    if actual == expected {
        return Ok(());
    }
    Err(HarnessError::State(
        "approval continuation execution binding does not match the recorded Turn".to_owned(),
    ))
}

fn approval_resume_evidence(
    turn: &Turn,
    expected_requester: &crate::ApprovalActor,
) -> Result<ApprovalResumeEvidence, HarnessError> {
    let boundary_end = turn
        .items
        .iter()
        .rposition(|item| !matches!(item.kind, ItemKind::SteeringQueued { .. }))
        .map_or(0, |index| index + 1);
    let approval_item_index = boundary_end.checked_sub(1).ok_or_else(|| {
        HarnessError::State(
            "running Turn has no complete pre-Tool approval continuation boundary".to_owned(),
        )
    })?;
    let policy_item_index = approval_item_index.checked_sub(1).ok_or_else(|| {
        HarnessError::State(
            "running Turn has no complete pre-Tool approval continuation boundary".to_owned(),
        )
    })?;
    let ItemKind::ApprovalRequested {
        approval_id,
        call_id: approval_call_id,
        tool,
        reason: approval_reason,
        risk: approval_risk,
        requested_by,
        tool_origin,
        model_request_sha256,
    } = &turn.items[approval_item_index].kind
    else {
        return Err(HarnessError::State(
            "running Turn is not paused at an approval request".to_owned(),
        ));
    };
    let ItemKind::PolicyDecision {
        call_id: policy_call_id,
        tool_origin: policy_tool_origin,
        decision: PolicyDecision::Ask { reason, risk },
    } = &turn.items[policy_item_index].kind
    else {
        return Err(HarnessError::State(
            "approval continuation has no immediately preceding Ask decision".to_owned(),
        ));
    };
    if policy_call_id != approval_call_id || approval_reason != reason || approval_risk != risk {
        return Err(HarnessError::State(
            "approval continuation Policy and request evidence is inconsistent".to_owned(),
        ));
    }
    let pending_tool_item_index = turn.items[..policy_item_index]
        .iter()
        .rposition(|item| {
            matches!(
                &item.kind,
                ItemKind::ToolCall { call_id, .. } if call_id == approval_call_id
            )
        })
        .ok_or_else(|| {
            HarnessError::State("approval continuation has no correlated Tool call".to_owned())
        })?;
    let ItemKind::ToolCall {
        model_id: recorded_model_id,
        model_origin: recorded_model_origin,
        call_id,
        name,
        input,
        batch,
        ..
    } = &turn.items[pending_tool_item_index].kind
    else {
        return Err(HarnessError::State(
            "approval continuation has no correlated Tool call".to_owned(),
        ));
    };
    let model_id = recorded_model_id.clone().ok_or_else(|| {
        HarnessError::State(
            "legacy approval continuation has no recorded Model identity".to_owned(),
        )
    })?;
    validate_model_id(&model_id).map_err(|_| {
        HarnessError::State("approval continuation contains an invalid Model identity".to_owned())
    })?;
    let model_origin = recorded_model_origin.clone().ok_or_else(|| {
        HarnessError::State("legacy approval continuation has no recorded Model origin".to_owned())
    })?;
    if policy_call_id != call_id
        || approval_call_id != call_id
        || tool != name
        || approval_reason != reason
        || approval_risk != risk
    {
        return Err(HarnessError::State(
            "approval continuation correlation evidence is inconsistent".to_owned(),
        ));
    }
    let requested_by = requested_by.clone().ok_or_else(|| {
        HarnessError::State(
            "legacy approval request has no resumable requester evidence".to_owned(),
        )
    })?;
    let tool_origin = tool_origin.clone().ok_or_else(|| {
        HarnessError::State("legacy approval request has no resumable Tool origin".to_owned())
    })?;
    if policy_tool_origin
        .as_ref()
        .is_some_and(|origin| origin != &tool_origin)
    {
        return Err(HarnessError::State(
            "approval continuation Policy and request Tool origins differ".to_owned(),
        ));
    }
    let model_request_sha256 = model_request_sha256.clone().ok_or_else(|| {
        HarnessError::State(
            "legacy approval request has no resumable Model request fingerprint".to_owned(),
        )
    })?;
    if &requested_by != expected_requester {
        return Err(HarnessError::Approval(
            "approval continuation requester differs from the original Turn actor".to_owned(),
        ));
    }

    let (model_request_item_index, batch_calls, current_batch_index) = if let Some(batch) = batch {
        let start = pending_tool_item_index
            .checked_sub(batch.index)
            .ok_or_else(|| {
                HarnessError::State(
                    "approval continuation Tool-call batch starts before the Turn".to_owned(),
                )
            })?;
        let end = start.checked_add(batch.size).ok_or_else(|| {
            HarnessError::State("approval continuation Tool-call batch size overflow".to_owned())
        })?;
        if end > policy_item_index {
            return Err(HarnessError::State(
                "approval continuation Tool-call batch overlaps Policy evidence".to_owned(),
            ));
        }
        let mut calls = Vec::with_capacity(batch.size);
        for (index, item) in turn.items[start..end].iter().enumerate() {
            let ItemKind::ToolCall {
                model_id: Some(candidate_model_id),
                model_origin: Some(candidate_model_origin),
                call_id,
                name,
                input,
                batch: Some(candidate_batch),
            } = &item.kind
            else {
                return Err(HarnessError::State(
                    "approval continuation Tool-call batch is incomplete".to_owned(),
                ));
            };
            if candidate_batch.id != batch.id
                || candidate_batch.index != index
                || candidate_batch.size != batch.size
                || candidate_model_id != &model_id
                || candidate_model_origin != &model_origin
            {
                return Err(HarnessError::State(
                    "approval continuation Tool-call batch evidence is inconsistent".to_owned(),
                ));
            }
            calls.push(ModelToolCall {
                call_id: call_id.clone(),
                name: name.clone(),
                input: input.clone(),
            });
        }
        (
            model_request_boundary(&turn.items, start, &model_id, &model_origin),
            calls,
            batch.index,
        )
    } else {
        if policy_item_index != pending_tool_item_index + 1 {
            return Err(HarnessError::State(
                "single approval continuation is not adjacent to its Tool call".to_owned(),
            ));
        }
        (
            model_request_boundary(
                &turn.items,
                pending_tool_item_index,
                &model_id,
                &model_origin,
            ),
            vec![ModelToolCall {
                call_id: call_id.clone(),
                name: name.clone(),
                input: input.clone(),
            }],
            0,
        )
    };

    let mut consumed_steps = 0_usize;
    let mut tool_call_ids = BTreeSet::new();
    for item in &turn.items {
        match &item.kind {
            ItemKind::AssistantMessage { .. } => {
                consumed_steps = consumed_steps
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::State("model step count overflow".to_owned()))?;
            }
            ItemKind::ToolCall { call_id, batch, .. } => {
                if batch.as_ref().is_none_or(|batch| batch.index == 0) {
                    consumed_steps = consumed_steps.checked_add(1).ok_or_else(|| {
                        HarnessError::State("model step count overflow".to_owned())
                    })?;
                }
                if !tool_call_ids.insert(call_id.clone()) {
                    return Err(HarnessError::State(
                        "approval continuation contains duplicate Tool call identities".to_owned(),
                    ));
                }
            }
            _ => {}
        }
    }

    Ok(ApprovalResumeEvidence {
        approval_id: approval_id.clone(),
        call_id: call_id.clone(),
        tool: tool.clone(),
        input: input.clone(),
        reason: reason.clone(),
        risk: *risk,
        requested_by,
        tool_origin,
        model_id,
        model_origin,
        model_request_sha256,
        model_request_item_index,
        batch_calls,
        current_batch_index,
        consumed_steps,
        tool_call_ids,
    })
}

fn model_request_boundary(
    items: &[Item],
    decision_start: usize,
    model_id: &str,
    model_origin: &crate::CapabilityOrigin,
) -> usize {
    decision_start
        .checked_sub(1)
        .filter(|index| {
            matches!(
                &items[*index].kind,
                ItemKind::ProviderContinuation {
                    model_id: continuation_model_id,
                    model_origin: continuation_model_origin,
                    ..
                } if continuation_model_id == model_id
                    && continuation_model_origin == model_origin
            )
        })
        .unwrap_or(decision_start)
}

fn validate_resumed_batch_authority(
    turn: &Turn,
    calls: &[ModelToolCall],
    tools: &ToolRegistry,
) -> Result<(), HarnessError> {
    for call in calls {
        let registered = tools.get(&call.name).ok_or_else(|| {
            HarnessError::State(format!(
                "approval continuation Tool {} is not registered",
                call.name
            ))
        })?;
        let mut decisions = turn.items.iter().filter_map(|item| {
            if let ItemKind::PolicyDecision {
                call_id,
                tool_origin,
                decision,
            } = &item.kind
                && call_id == &call.call_id
            {
                Some((tool_origin, decision))
            } else {
                None
            }
        });
        let (origin, decision) = decisions.next().ok_or_else(|| {
            HarnessError::State(format!(
                "approval continuation Tool call {} has no Policy decision",
                call.call_id
            ))
        })?;
        if decisions.next().is_some()
            || origin.as_ref() != Some(&registered.origin)
            || matches!(decision, PolicyDecision::Deny { .. })
        {
            return Err(HarnessError::State(format!(
                "approval continuation Tool call {} has inconsistent Policy authority",
                call.call_id
            )));
        }
        if matches!(decision, PolicyDecision::Ask { .. }) {
            let mut approvals = turn.items.iter().filter_map(|item| {
                if let ItemKind::ApprovalDecision {
                    call_id, decision, ..
                } = &item.kind
                    && call_id == &call.call_id
                {
                    Some(decision)
                } else {
                    None
                }
            });
            if !matches!(approvals.next(), Some(ApprovalDecision::Approve))
                || approvals.next().is_some()
            {
                return Err(HarnessError::State(format!(
                    "approval continuation Tool call {} is not approved",
                    call.call_id
                )));
            }
        }
    }
    Ok(())
}

fn turn_prompt(turn: &Turn) -> Result<String, HarnessError> {
    let mut prompts = turn.items.iter().filter_map(|item| {
        if let ItemKind::UserMessage { content } = &item.kind {
            Some(content)
        } else {
            None
        }
    });
    let prompt = prompts.next().cloned().ok_or_else(|| {
        HarnessError::State("approval continuation Turn has no user prompt".to_owned())
    })?;
    if prompts.next().is_some() {
        return Err(HarnessError::State(
            "approval continuation Turn has multiple user prompts".to_owned(),
        ));
    }
    Ok(prompt)
}

#[derive(Clone, Debug)]
struct ProviderContinuationTarget {
    model_id: String,
    model_origin: crate::CapabilityOrigin,
}

fn pending_provider_continuation_target(
    items: &[Item],
) -> Result<Option<ProviderContinuationTarget>, HarnessError> {
    let Some((tool_index, call_id, tool_model_id, tool_model_origin)) =
        items.iter().enumerate().rev().find_map(|(index, item)| {
            if let ItemKind::ToolCall {
                model_id,
                model_origin,
                call_id,
                ..
            } = &item.kind
            {
                Some((index, call_id, model_id, model_origin))
            } else {
                None
            }
        })
    else {
        return Ok(None);
    };
    if !items[tool_index + 1..].iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::ToolResult {
                call_id: result_call_id,
                ..
            } if result_call_id == call_id
        )
    }) {
        return Ok(None);
    }
    if items[tool_index + 1..].iter().any(|item| {
        matches!(
            item.kind,
            ItemKind::UserMessage { .. } | ItemKind::AssistantMessage { .. }
        )
    }) {
        return Ok(None);
    }
    let chain_start = items[..tool_index]
        .iter()
        .rposition(|item| {
            matches!(
                item.kind,
                ItemKind::UserMessage { .. } | ItemKind::AssistantMessage { .. }
            )
        })
        .map_or(0, |index| index + 1);
    let Some((continuation_model_id, continuation_model_origin)) = items[chain_start..tool_index]
        .iter()
        .rev()
        .find_map(|item| {
            if let ItemKind::ProviderContinuation {
                model_id,
                model_origin,
                ..
            } = &item.kind
            {
                Some((model_id, model_origin))
            } else {
                None
            }
        })
    else {
        return Ok(None);
    };
    let (Some(tool_model_id), Some(tool_model_origin)) = (tool_model_id, tool_model_origin) else {
        return Err(HarnessError::State(
            "provider continuation Tool call has no recorded Model provenance".to_owned(),
        ));
    };
    if tool_model_id != continuation_model_id || tool_model_origin != continuation_model_origin {
        return Err(HarnessError::State(
            "provider continuation and Tool call Model provenance differ".to_owned(),
        ));
    }
    Ok(Some(ProviderContinuationTarget {
        model_id: continuation_model_id.clone(),
        model_origin: continuation_model_origin.clone(),
    }))
}

fn retain_model_continuations(
    request: &mut ModelRequest,
    model_id: &str,
    model_origin: &crate::CapabilityOrigin,
) {
    request.items.retain(|item| {
        !matches!(
            &item.kind,
            ItemKind::ProviderContinuation {
                model_id: continuation_model_id,
                model_origin: continuation_model_origin,
                ..
            } if continuation_model_id != model_id || continuation_model_origin != model_origin
        )
    });
}

struct SettledModelOutput {
    model_id: String,
    model_origin: crate::CapabilityOrigin,
    output: ModelOutput,
    continuation: Option<ModelContinuation>,
}

struct ModelAttemptSettlement {
    result: Result<ModelResponse, HarnessError>,
    control: ModelAttemptControl,
}

struct ModelAttemptInvocation {
    request: ModelRequest,
    stream: ModelStream,
    retry_index: u8,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ModelAttemptControl {
    None,
    Cancelled,
    DeadlineElapsed,
}

struct RuntimeModel {
    model: Arc<dyn LanguageModel>,
    identity: FrozenModelIdentity,
    origin: crate::CapabilityOrigin,
}

struct ModelRoute {
    entries: Vec<RuntimeModel>,
    attempt_timeout: Option<Duration>,
    retry_policy: Option<ModelRetryPolicy>,
    timeout_cooldown: Option<Duration>,
    timeout_cooldowns: Mutex<BTreeMap<String, Instant>>,
}

impl ModelRoute {
    fn built_in(model: Arc<dyn LanguageModel>) -> Self {
        Self {
            entries: vec![RuntimeModel {
                identity: FrozenModelIdentity::capture(&model),
                model,
                origin: crate::CapabilityOrigin::BuiltIn,
            }],
            attempt_timeout: None,
            retry_policy: None,
            timeout_cooldown: None,
            timeout_cooldowns: Mutex::new(BTreeMap::new()),
        }
    }

    fn from_registry(models: &ModelRegistry, model_ids: &[&str]) -> Result<Self, HarnessError> {
        if model_ids.is_empty() || model_ids.len() > MAX_MODEL_ROUTE_ENTRIES {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model failover route must contain 1-{MAX_MODEL_ROUTE_ENTRIES} identities"
            )));
        }
        let mut seen = BTreeSet::new();
        let mut entries = Vec::with_capacity(model_ids.len());
        for model_id in model_ids {
            if !seen.insert(*model_id) {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "Model failover route contains duplicate identity {model_id}"
                )));
            }
            let registered = models
                .get(model_id)
                .ok_or_else(|| HarnessError::UnknownModel((*model_id).to_owned()))?;
            entries.push(RuntimeModel {
                model: registered.model.clone(),
                identity: FrozenModelIdentity::Valid(registered.id.clone()),
                origin: registered.origin.clone(),
            });
        }
        Ok(Self {
            entries,
            attempt_timeout: (model_ids.len() > 1).then_some(DEFAULT_MODEL_ATTEMPT_TIMEOUT),
            retry_policy: None,
            timeout_cooldown: None,
            timeout_cooldowns: Mutex::new(BTreeMap::new()),
        })
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if self.entries.is_empty() || self.entries.len() > MAX_MODEL_ROUTE_ENTRIES {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Model failover route must contain 1-{MAX_MODEL_ROUTE_ENTRIES} identities"
            )));
        }
        for entry in &self.entries {
            entry.identity.get()?;
        }
        Ok(())
    }

    fn contains(&self, model_id: &str, origin: &crate::CapabilityOrigin) -> bool {
        self.entries.iter().any(|entry| {
            matches!(&entry.identity, FrozenModelIdentity::Valid(id) if id == model_id)
                && &entry.origin == origin
        })
    }

    fn attempt_deadline(
        &self,
        turn_deadline: Option<Instant>,
    ) -> Result<(Option<Instant>, bool), HarnessError> {
        let Some(timeout) = self.attempt_timeout else {
            return Ok((turn_deadline, false));
        };
        let attempt_deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarnessError::InvalidConfiguration(
                "Model attempt timeout exceeds the runtime clock range".to_owned(),
            )
        })?;
        if turn_deadline.is_some_and(|deadline| deadline <= attempt_deadline) {
            Ok((turn_deadline, false))
        } else {
            Ok((Some(attempt_deadline), true))
        }
    }

    fn retry_delay(
        &self,
        error: &HarnessError,
        next_retry: u8,
        target: &ObservationTarget<'_>,
        model_id: &str,
    ) -> Option<Duration> {
        let policy = self.retry_policy?;
        if next_retry == 0 || next_retry > policy.max_retries {
            return None;
        }
        let HarnessError::ModelProvider(failure) = error else {
            return None;
        };
        if !matches!(
            failure.kind(),
            ModelProviderFailureKind::RateLimited
                | ModelProviderFailureKind::Overloaded
                | ModelProviderFailureKind::Server
                | ModelProviderFailureKind::Transport
        ) {
            return None;
        }
        if let Some(delay_ms) = failure.retry_after_ms() {
            let delay = Duration::from_millis(delay_ms);
            return (delay <= policy.max_delay).then_some(delay);
        }
        let multiplier = 1_u32 << u32::from(next_retry - 1);
        let ceiling = policy
            .initial_delay
            .saturating_mul(multiplier)
            .min(policy.max_delay);
        let ceiling_ms = u64::try_from(ceiling.as_millis()).ok()?;
        let floor_ms = ceiling_ms.div_ceil(2);
        let width = ceiling_ms.checked_sub(floor_ms)?.checked_add(1)?;
        let jitter = stable_retry_hash(
            target.thread_id.as_str(),
            target.turn_id.as_str(),
            model_id,
            next_retry,
        ) % width;
        Some(Duration::from_millis(floor_ms + jitter))
    }

    fn partition_timeout_cooldown<'a>(
        &self,
        candidates: Vec<&'a RuntimeModel>,
        now: Instant,
    ) -> Result<(Vec<&'a RuntimeModel>, Vec<&'a RuntimeModel>), HarnessError> {
        if self.timeout_cooldown.is_none() || candidates.len() < 2 {
            return Ok((candidates, Vec::new()));
        }
        let mut cooldowns = self
            .timeout_cooldowns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cooldowns.retain(|_, unavailable_until| *unavailable_until > now);
        let mut available = Vec::with_capacity(candidates.len());
        let mut cooling = Vec::new();
        for candidate in candidates {
            if cooldowns.contains_key(candidate.identity.get()?) {
                cooling.push(candidate);
            } else {
                available.push(candidate);
            }
        }
        if available.is_empty() {
            available = cooling;
            cooling = Vec::new();
        }
        Ok((available, cooling))
    }

    fn record_attempt_timeout(&self, model_id: &str, now: Instant) -> Result<(), HarnessError> {
        let Some(cooldown) = self.timeout_cooldown else {
            return Ok(());
        };
        let unavailable_until = now.checked_add(cooldown).ok_or_else(|| {
            HarnessError::InvalidConfiguration(
                "Model timeout cooldown exceeds the runtime clock range".to_owned(),
            )
        })?;
        self.timeout_cooldowns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(model_id.to_owned(), unavailable_until);
        Ok(())
    }

    fn clear_attempt_timeout(&self, model_id: &str) {
        if self.timeout_cooldown.is_none() {
            return;
        }
        self.timeout_cooldowns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(model_id);
    }
}

fn stable_retry_hash(thread_id: &str, turn_id: &str, model_id: &str, retry: u8) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for value in [thread_id, turn_id, model_id] {
        for byte in value.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash ^= u64::from(retry);
    hash.wrapping_mul(0x0000_0100_0000_01b3)
}

enum FrozenModelIdentity {
    Valid(String),
    Invalid,
    Panicked,
}

impl FrozenModelIdentity {
    fn capture(model: &Arc<dyn LanguageModel>) -> Self {
        match catch_unwind(AssertUnwindSafe(|| model.id().to_owned())) {
            Ok(id) if validate_model_id(&id).is_ok() => Self::Valid(id),
            Ok(_) => Self::Invalid,
            Err(_) => Self::Panicked,
        }
    }

    fn get(&self) -> Result<&str, HarnessError> {
        match self {
            Self::Valid(id) => Ok(id),
            Self::Invalid => Err(HarnessError::InvalidCapability(
                "model identity does not satisfy the portable contract".to_owned(),
            )),
            Self::Panicked => Err(HarnessError::CapabilityPanicked {
                phase: ExecutionPhase::Model,
            }),
        }
    }
}

struct ObservationTarget<'a> {
    thread_id: &'a ThreadId,
    turn_id: &'a TurnId,
    capability: &'a str,
    phase: ExecutionPhase,
}

impl<'a> ObservationTarget<'a> {
    fn new(
        thread_id: &'a ThreadId,
        turn_id: &'a TurnId,
        capability: &'a str,
        phase: ExecutionPhase,
    ) -> Self {
        Self {
            thread_id,
            turn_id,
            capability,
            phase,
        }
    }
}

pub(crate) fn validate_model_response(response: &ModelResponse) -> Result<(), HarnessError> {
    if let Some(provider_model) = &response.provider_model
        && (provider_model.trim().is_empty()
            || provider_model.len() > MAX_PROVIDER_EVIDENCE_ID_BYTES
            || provider_model.chars().any(char::is_control))
    {
        return Err(HarnessError::Model(format!(
            "provider model must be 1-{MAX_PROVIDER_EVIDENCE_ID_BYTES} non-control bytes"
        )));
    }
    if let Some(provider_request_id) = &response.provider_request_id
        && (provider_request_id.trim().is_empty()
            || provider_request_id.len() > MAX_PROVIDER_EVIDENCE_ID_BYTES
            || provider_request_id.chars().any(char::is_control))
    {
        return Err(HarnessError::Model(format!(
            "provider request id must be 1-{MAX_PROVIDER_EVIDENCE_ID_BYTES} non-control bytes"
        )));
    }
    if let Some(continuation) = &response.continuation {
        continuation.validate()?;
    }
    validate_model_output(&response.output)
}

pub(crate) fn validate_model_output(output: &ModelOutput) -> Result<(), HarnessError> {
    match output {
        ModelOutput::Message { content } => validate_model_message(content),
        ModelOutput::ToolCall {
            call_id,
            name,
            input,
        } => validate_model_tool_call(call_id, name, input),
        ModelOutput::ToolCalls { calls } => validate_model_tool_calls(calls),
    }
}

fn model_request_sha256(request: &ModelRequest) -> Result<String, HarnessError> {
    validate_model_request_json_shapes(request)?;
    crate::json::bounded_serialized_sha256(request, MAX_MODEL_REQUEST_BYTES).map_err(|failure| {
        HarnessError::Model(match failure {
            crate::json::BoundedJsonError::LimitExceeded => {
                format!("model request exceeds {MAX_MODEL_REQUEST_BYTES} bytes")
            }
            crate::json::BoundedJsonError::CannotEncode => "cannot encode model request".to_owned(),
        })
    })
}

fn validate_policy_decision(decision: &PolicyDecision) -> Result<(), HarnessError> {
    match decision {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::Deny { reason } | PolicyDecision::Ask { reason, .. } => {
            validate_decision_reason("Policy", reason)
        }
    }
}

fn validate_approval_decision(decision: &ApprovalDecision) -> Result<(), HarnessError> {
    match decision {
        ApprovalDecision::Approve => Ok(()),
        ApprovalDecision::Deny { reason } => validate_decision_reason("approval", reason),
    }
}

fn validate_decision_reason(kind: &str, reason: &str) -> Result<(), HarnessError> {
    if reason.trim().is_empty()
        || reason.len() > MAX_POLICY_REASON_BYTES
        || reason.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "{kind} reason must be 1-{MAX_POLICY_REASON_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_prompt(prompt: &str) -> Result<(), HarnessError> {
    if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "prompt must be 1-{MAX_PROMPT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_steering(content: &str) -> Result<(), HarnessError> {
    if content.trim().is_empty() || content.len() > MAX_PROMPT_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "steering content must be 1-{MAX_PROMPT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn pending_steering_from_items(items: &[Item]) -> Result<VecDeque<PendingSteering>, HarnessError> {
    let mut pending = VecDeque::new();
    let mut seen = BTreeSet::new();
    for item in items {
        match &item.kind {
            ItemKind::SteeringQueued {
                steering_id,
                content,
                ..
            } => {
                if !seen.insert(steering_id.clone()) {
                    return Err(HarnessError::State(format!(
                        "duplicate steering identity {steering_id}"
                    )));
                }
                pending.push_back(PendingSteering {
                    steering_id: steering_id.clone(),
                    content: content.clone(),
                });
            }
            ItemKind::SteeringApplied {
                steering_id,
                content,
            } => {
                let Some(queued) = pending.pop_front() else {
                    return Err(HarnessError::State(format!(
                        "applied steering {steering_id} has no pending queue record"
                    )));
                };
                if queued.steering_id != *steering_id || queued.content != *content {
                    return Err(HarnessError::State(format!(
                        "applied steering {steering_id} does not match queue order and content"
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(pending)
}

fn require_control_turn(control: &ActiveTurnControl, turn: &Turn) -> Result<(), HarnessError> {
    if control.turn_id != turn.id {
        return Err(HarnessError::State(format!(
            "Turn control {} does not match active turn {}",
            control.turn_id, turn.id
        )));
    }
    Ok(())
}

fn require_runtime_capacity(capacity: &StateCapacity) -> Result<(), HarnessError> {
    if capacity.general_events_remaining < MIN_RUNTIME_GENERAL_EVENTS {
        return Err(HarnessError::State(format!(
            "Thread needs at least {MIN_RUNTIME_GENERAL_EVENTS} general-purpose events for Runtime execution; {} remain",
            capacity.general_events_remaining
        )));
    }
    Ok(())
}

pub(crate) fn validate_model_request(request: &ModelRequest) -> Result<(), HarnessError> {
    request
        .authority
        .validate_current("Model request authority")?;
    validate_model_request_json_shapes(request)?;
    crate::json::bounded_serialized_size(request, MAX_MODEL_REQUEST_BYTES).map_or_else(
        |error| {
            Err(match error {
                crate::json::BoundedJsonError::LimitExceeded => HarnessError::Model(format!(
                    "model request exceeds {MAX_MODEL_REQUEST_BYTES} bytes"
                )),
                crate::json::BoundedJsonError::CannotEncode => {
                    HarnessError::Model("cannot encode model request".to_owned())
                }
            })
        },
        |_| Ok(()),
    )
}

fn validate_model_request_json_shapes(request: &ModelRequest) -> Result<(), HarnessError> {
    for item in &request.items {
        if let ItemKind::ProviderContinuation {
            model_id,
            model_origin,
            continuation,
        } = &item.kind
        {
            validate_model_id(model_id).map_err(|error| HarnessError::Model(error.to_string()))?;
            validate_capability_origin(model_origin)
                .map_err(|error| HarnessError::Model(error.to_string()))?;
            continuation.validate()?;
        }
        let value = match &item.kind {
            ItemKind::ToolCall { input, .. } => Some(input),
            ItemKind::ToolResult { output, .. } => Some(output),
            _ => None,
        };
        if let Some(value) = value {
            validate_runtime_json_shape("model request Item", value, HarnessError::Model)?;
        }
    }
    for tool in &request.tools {
        validate_runtime_json_shape(
            "model request Tool schema",
            &tool.input_schema,
            HarnessError::Model,
        )?;
    }
    Ok(())
}

fn validate_model_message(content: &str) -> Result<(), HarnessError> {
    if content.trim().is_empty() || content.len() > MAX_MODEL_TEXT_BYTES {
        return Err(HarnessError::Model(format!(
            "assistant message must be 1-{MAX_MODEL_TEXT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_model_tool_call(call_id: &str, name: &str, input: &Value) -> Result<(), HarnessError> {
    if call_id.trim().is_empty()
        || call_id.len() > MAX_MODEL_CALL_ID_BYTES
        || call_id.chars().any(char::is_control)
    {
        return Err(HarnessError::Model(format!(
            "tool call id must be 1-{MAX_MODEL_CALL_ID_BYTES} non-control bytes"
        )));
    }
    if name.is_empty() || name.len() > 64 {
        return Err(HarnessError::Model(
            "tool name must be 1-64 bytes".to_owned(),
        ));
    }
    validate_runtime_json(
        "tool input",
        input,
        MAX_MODEL_TOOL_INPUT_BYTES,
        HarnessError::Model,
    )
}

fn validate_model_tool_calls(calls: &[ModelToolCall]) -> Result<(), HarnessError> {
    if !(2..=crate::MAX_TOOL_CALLS_PER_BATCH).contains(&calls.len()) {
        return Err(HarnessError::Model(format!(
            "tool-call batch must contain 2-{} calls",
            crate::MAX_TOOL_CALLS_PER_BATCH
        )));
    }
    let mut call_ids = BTreeSet::new();
    for call in calls {
        validate_model_tool_call(&call.call_id, &call.name, &call.input)?;
        if !call_ids.insert(&call.call_id) {
            return Err(HarnessError::Model(format!(
                "tool-call batch reused correlation id {:?}",
                call.call_id
            )));
        }
    }
    crate::json::bounded_serialized_size(&calls, MAX_MODEL_TOOL_BATCH_BYTES).map_err(
        |failure| {
            HarnessError::Model(match failure {
                crate::json::BoundedJsonError::LimitExceeded => {
                    format!("tool-call batch exceeds {MAX_MODEL_TOOL_BATCH_BYTES} bytes")
                }
                crate::json::BoundedJsonError::CannotEncode => {
                    "cannot encode tool-call batch".to_owned()
                }
            })
        },
    )?;
    Ok(())
}

fn validate_tool_output(output: &Value) -> Result<(), HarnessError> {
    validate_runtime_json(
        "tool output",
        output,
        MAX_TOOL_OUTPUT_BYTES,
        HarnessError::Tool,
    )
}

fn validate_runtime_json(
    kind: &str,
    value: &Value,
    maximum: usize,
    error: fn(String) -> HarnessError,
) -> Result<(), HarnessError> {
    validate_runtime_json_shape(kind, value, error)?;
    crate::json::bounded_serialized_size(value, maximum).map_or_else(
        |failure| {
            Err(error(match failure {
                crate::json::BoundedJsonError::LimitExceeded => {
                    format!("{kind} exceeds {maximum} bytes")
                }
                crate::json::BoundedJsonError::CannotEncode => {
                    format!("cannot encode {kind}")
                }
            }))
        },
        |_| Ok(()),
    )
}

fn validate_runtime_json_shape(
    kind: &str,
    value: &Value,
    error: fn(String) -> HarnessError,
) -> Result<(), HarnessError> {
    crate::json::validate_value_shape(value).map_err(|_| {
        error(format!(
            "{kind} exceeds the supported JSON depth or node count"
        ))
    })
}

fn bounded_runtime_error(message: &str) -> String {
    let mut chars = message.chars();
    let bounded = chars
        .by_ref()
        .take(MAX_RUNTIME_ERROR_CHARS)
        .collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

fn conversation_summary_record(block: &ContextBlock) -> Result<ItemKind, HarnessError> {
    let ContextSource::ConversationSummary {
        compactor,
        covered_turns,
        older_omitted_turns,
        source_sha256,
        content_sha256,
    } = &block.source
    else {
        return Err(HarnessError::InvalidConfiguration(
            "Context Engine returned an invalid conversation summary source".to_owned(),
        ));
    };
    Ok(ItemKind::ConversationSummary {
        compactor: compactor.clone(),
        covered_turns: covered_turns.clone(),
        older_omitted_turns: *older_omitted_turns,
        source_sha256: source_sha256.clone(),
        content_sha256: content_sha256.clone(),
        estimated_tokens: block.estimated_tokens,
        serialized_bytes: block.text.len(),
    })
}

fn invocation_context_record(
    blocks: &[ContextBlock],
    submitted_by: ActorIdentity,
) -> Option<ItemKind> {
    let mut evidence = Vec::new();
    for block in blocks {
        let ContextSource::Invocation {
            source,
            reference,
            source_sha256,
            content_sha256,
        } = &block.source
        else {
            continue;
        };
        evidence.push(InvocationContextEvidence {
            source: source.clone(),
            reference: reference.clone(),
            source_sha256: source_sha256.clone(),
            content_sha256: content_sha256.clone(),
            estimated_tokens: block.estimated_tokens,
            serialized_bytes: block.text.len(),
        });
    }
    if evidence.is_empty() {
        None
    } else {
        Some(ItemKind::InvocationContext {
            submitted_by,
            blocks: evidence,
        })
    }
}

async fn invoke_tool_capability(
    invocation: ToolCapabilityInvocation,
    observability: Observability,
    thread_id: ThreadId,
    turn_id: TurnId,
    authority: AuthorityContext,
    cancellation: crate::CancellationToken,
    deadline: Option<Instant>,
) -> Result<ToolCallSettlement, HarnessError> {
    let ToolCapabilityInvocation {
        tool,
        origin,
        cancellation_settlement_timeout,
        call,
    } = invocation;
    let ModelToolCall {
        call_id,
        name,
        input,
    } = call;
    let capability_cancellation = crate::CancellationToken::new();
    let context = ToolContext {
        thread_id: thread_id.clone(),
        turn_id: turn_id.clone(),
        call_id: call_id.clone(),
        authority: authority.clone(),
        cancellation: capability_cancellation.clone(),
    };
    let started = Instant::now();
    let result = controlled_with_settlement_grace(
        &cancellation,
        capability_cancellation,
        deadline,
        ExecutionPhase::Tool,
        cancellation_settlement_timeout,
        || tool.execute_with_evidence(input, context),
    )
    .await;
    observability.emit(&PhaseObservation {
        thread_id,
        turn_id,
        phase: ExecutionPhase::Tool,
        capability: name.clone(),
        duration_micros: elapsed_micros(started),
        outcome: observation_outcome(&result),
        model_usage: None,
        provider_model: None,
        provider_request_id: None,
        provider_failure_kind: None,
        provider_status_code: None,
        provider_retry_after_ms: None,
        model_retry_index: None,
        stream_events_dropped: 0,
    });
    match result {
        Ok(result) => {
            let (output, claims) = result.into_parts();
            let (output, is_error, connector_evidence) = match validate_tool_output(&output) {
                Ok(()) => {
                    let output_sha256 =
                        crate::json::bounded_serialized_sha256(&output, MAX_TOOL_OUTPUT_BYTES)
                            .map_err(|_| {
                                HarnessError::Tool("cannot digest validated Tool output".to_owned())
                            })?;
                    let evidence = claims
                        .into_iter()
                        .map(|claim| {
                            ConnectorEvidence::bind(
                                name.clone(),
                                origin.clone(),
                                authority.clone(),
                                output_sha256.clone(),
                                claim,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    (output, false, evidence)
                }
                Err(error) => (
                    serde_json::json!({
                        "error": bounded_runtime_error(&error.to_string())
                    }),
                    true,
                    Vec::new(),
                ),
            };
            Ok(ToolCallSettlement {
                call_id,
                output,
                is_error,
                connector_evidence,
            })
        }
        Err(error @ (HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. })) => Err(error),
        Err(error) => Ok(ToolCallSettlement {
            call_id,
            output: serde_json::json!({
                "error": bounded_runtime_error(&error.to_string())
            }),
            is_error: true,
            connector_evidence: Vec::new(),
        }),
    }
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn observation_outcome<T>(result: &Result<T, HarnessError>) -> ObservationOutcome {
    match result {
        Ok(_) => ObservationOutcome::Success,
        Err(HarnessError::Cancelled { .. }) => ObservationOutcome::Cancelled,
        Err(HarnessError::TimedOut { .. }) => ObservationOutcome::TimedOut,
        Err(_) => ObservationOutcome::Error,
    }
}

fn validate_model_attempt_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::ModelProvider(failure) => match failure.validate() {
            Ok(()) => HarnessError::ModelProvider(failure),
            Err(_) => HarnessError::InvalidCapability(
                "Model returned invalid typed Provider failure metadata".to_owned(),
            ),
        },
        error => error,
    }
}

async fn wait_for_model_retry(
    cancellation: &crate::CancellationToken,
    deadline: Option<Instant>,
    deadline_is_attempt: bool,
    delay: Duration,
) -> Result<bool, HarnessError> {
    if let Some(deadline) = deadline {
        let now = Instant::now();
        if now >= deadline {
            return if deadline_is_attempt {
                Ok(false)
            } else {
                Err(HarnessError::TimedOut {
                    phase: ExecutionPhase::Model,
                })
            };
        }
        if now
            .checked_add(delay)
            .is_none_or(|ready_at| ready_at >= deadline)
        {
            return Ok(false);
        }
    }
    match controlled(
        cancellation,
        deadline,
        ExecutionPhase::Model,
        || async move {
            tokio::time::sleep(delay).await;
            Ok(())
        },
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(HarnessError::TimedOut { .. }) if deadline_is_attempt => Ok(false),
        Err(error) => Err(error),
    }
}

struct ActiveThreadGuard<'a> {
    active: &'a Mutex<BTreeSet<ThreadId>>,
    thread_id: ThreadId,
}

impl Drop for ActiveThreadGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.thread_id);
        }
    }
}

struct TurnControlGuard<'a> {
    controls: &'a Mutex<BTreeMap<ThreadId, Arc<tokio::sync::Mutex<ActiveTurnControl>>>>,
    thread_id: ThreadId,
}

impl Drop for TurnControlGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut controls) = self.controls.lock() {
            controls.remove(&self.thread_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        future::pending,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::{Value, json};
    use tokio::{
        sync::{Barrier, Notify},
        time::Instant,
    };

    use super::{
        AllowListPolicy, ApprovalHandler, HarnessRuntime, LanguageModel, MAX_PENDING_STEERING,
        MAX_PENDING_STEERING_BYTES, MAX_PROVIDER_EVIDENCE_ID_BYTES, ModelRetryPolicy, ModelRoute,
        PolicyEngine, Tool, TurnExecutionOptions, require_runtime_capacity,
        validate_approval_decision, validate_model_request, validate_model_response,
        validate_model_tool_call, validate_model_tool_calls, validate_policy_decision,
        validate_tool_output,
    };
    use crate::{
        ActorIdentity, ApprovalActor, ApprovalDecision, ApprovalInbox, ApprovalRecordStatus,
        ApprovalRequest, AuthorityContext, CONVERSATION_COMPACTOR_API_VERSION, CancellationToken,
        CapabilityOrigin, ConnectorEvidenceClaim, ContextEngine, ContextSource,
        ConversationCompactionConfig, ConversationCompactionRequest,
        ConversationCompactionResponse, ConversationCompactor, ConversationCompactorDescriptor,
        ConversationCompactorRegistry, ConversationContextConfig, EventId, EventStore,
        ExecutionBinding, ExecutionPhase, HarnessError, HarnessFuture, InboxApprovalHandler,
        ItemKind, MEMORY_API_VERSION, MemoryApprovalInbox, MemoryContextConfig, MemoryContextPack,
        MemoryContextRecordStatus, MemoryEventStore, MemoryFailureMode, MemoryOperation,
        MemoryProvider, MemoryProviderDescriptor, MemoryReference, MemoryRegistry,
        MemorySearchRequest, MemorySearchResponse, MemoryView, ModelContinuation, ModelEventSink,
        ModelOutput, ModelProviderFailure, ModelProviderFailureKind, ModelRegistry, ModelRequest,
        ModelResponse, ModelStream, ModelStreamEvent, ModelToolCall, ModelUsage, Observability,
        ObservationOutcome, PendingEvent, PolicyDecision, RiskLevel, SqliteApprovalInbox,
        SqliteEventStore, StateCapacity, StateCapacityLevel, StateEngine, StateEvent, StoredEvent,
        ThreadHandoffConfig, ThreadId, ToolAuthorization, ToolBatchExecution, ToolContext,
        ToolDescriptor, ToolExecutionResult, ToolRegistry, TraceCollector, TurnContextInput,
        TurnStatus, TurnStopReason, VerificationOutcome, VerificationRegistry, VerificationRequest,
        Verifier, VerifierDescriptor,
    };

    struct EchoTool {
        calls: Arc<AtomicUsize>,
    }

    struct DriftedEchoTool {
        calls: Arc<AtomicUsize>,
    }

    struct AuthorityProbeTool {
        observed: Arc<Mutex<Option<AuthorityContext>>>,
    }

    struct EvidenceConnectorTool;

    struct AuthorityRecordingPolicy {
        observed: Arc<Mutex<Option<AuthorityContext>>>,
    }

    struct AuthorityRecordingModel {
        observed: Arc<Mutex<Option<AuthorityContext>>>,
    }

    fn sqlite_test_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-runtime-{label}-{}.db",
            EventId::generate()
        ))
    }

    fn remove_sqlite_files(path: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            let mut candidate = path.as_os_str().to_os_string();
            candidate.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(candidate));
        }
    }

    struct RejectFirstItemStore {
        inner: MemoryEventStore,
        rejected: AtomicUsize,
    }

    impl RejectFirstItemStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                rejected: AtomicUsize::new(0),
            }
        }
    }

    struct RejectStopEvidenceStore {
        inner: MemoryEventStore,
    }

    struct RejectSteeringApplicationStore {
        inner: MemoryEventStore,
    }

    impl RejectSteeringApplicationStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
            }
        }
    }

    impl RejectStopEvidenceStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
            }
        }
    }

    impl EventStore for RejectStopEvidenceStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            if matches!(
                &pending.event,
                StateEvent::ItemAppended {
                    item,
                    ..
                } if matches!(&item.kind, ItemKind::TurnStopped { .. })
            ) {
                return Box::pin(async {
                    Err(HarnessError::State(
                        "simulated stop-evidence persistence failure".to_owned(),
                    ))
                });
            }
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }
    }

    impl EventStore for RejectFirstItemStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            if matches!(&pending.event, StateEvent::ItemAppended { .. })
                && self.rejected.fetch_add(1, Ordering::SeqCst) == 0
            {
                return Box::pin(async {
                    Err(HarnessError::State(
                        "simulated Item persistence failure".to_owned(),
                    ))
                });
            }
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }
    }

    impl EventStore for RejectSteeringApplicationStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            if matches!(
                &pending.event,
                StateEvent::ItemAppended {
                    item,
                    ..
                } if matches!(&item.kind, ItemKind::SteeringApplied { .. })
            ) {
                return Box::pin(async {
                    Err(HarnessError::State(
                        "simulated steering application persistence failure".to_owned(),
                    ))
                });
            }
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }
    }

    #[test]
    fn policy_and_approval_reasons_are_bounded_before_state() {
        assert!(
            validate_policy_decision(&PolicyDecision::Ask {
                reason: "x".repeat(super::MAX_POLICY_REASON_BYTES + 1),
                risk: RiskLevel::High,
            })
            .is_err()
        );
        assert!(
            validate_approval_decision(&ApprovalDecision::Deny {
                reason: "\n".to_owned(),
            })
            .is_err()
        );
    }

    #[test]
    fn runtime_turn_preflight_preserves_minimum_general_event_budget() {
        let capacity = StateCapacity {
            used_events: 999_996,
            event_limit: 1_000_000,
            remaining_events: 4,
            general_events_remaining: 3,
            terminal_event_reserve: 1,
            used_recovery_bytes: 0,
            recovery_byte_limit: crate::STATE_THREAD_RECOVERY_BYTE_LIMIT,
            remaining_recovery_bytes: crate::STATE_THREAD_RECOVERY_BYTE_LIMIT,
            general_recovery_bytes_remaining: crate::STATE_THREAD_RECOVERY_BYTE_LIMIT
                - crate::STATE_TERMINAL_RECOVERY_BYTE_RESERVE,
            terminal_recovery_byte_reserve: crate::STATE_TERMINAL_RECOVERY_BYTE_RESERVE,
            level: StateCapacityLevel::Critical,
        };
        assert!(require_runtime_capacity(&capacity).is_err());
        assert!(
            require_runtime_capacity(&StateCapacity {
                general_events_remaining: 4,
                ..capacity
            })
            .is_ok()
        );
    }

    #[test]
    fn runtime_rejects_an_unbounded_parallel_tool_limit() {
        for limit in [0, super::MAX_PARALLEL_TOOL_CALLS + 1] {
            let runtime = HarnessRuntime::new(
                Arc::new(EchoModel),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            );
            assert!(runtime.with_max_parallel_tool_calls(limit).is_err());
        }
    }

    #[tokio::test]
    async fn state_item_failure_still_durably_settles_the_turn() {
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(RejectFirstItemStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let error = runtime
            .run_turn(&thread.id, "must settle")
            .await
            .expect_err("first Item append fails");
        assert!(matches!(error, HarnessError::State(_)));

        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load thread")
            .expect("thread");
        assert_eq!(projected.turns.len(), 1);
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(projected.turns[0].items.is_empty());
    }

    impl Tool for EchoTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Return the supplied text".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "required": ["text"]
                }),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            ToolBatchExecution::ParallelSafe
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let text = input
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or_else(|| HarnessError::Tool("missing text".to_owned()))?;
                Ok(json!({ "text": text }))
            })
        }
    }

    impl Tool for DriftedEchoTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Changed after the original authorization".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "required": ["text"],
                    "properties": { "text": { "type": "string" } }
                }),
            }
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(input)
            })
        }
    }

    impl Tool for AuthorityProbeTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Records the trusted Tool authority".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                *self
                    .observed
                    .lock()
                    .map_err(|_| HarnessError::Tool("authority recorder poisoned".to_owned()))? =
                    Some(context.authority);
                Ok(input)
            })
        }
    }

    impl Tool for EvidenceConnectorTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Returns one source-bound Connector fact".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move { Ok(input) })
        }

        fn execute_with_evidence<'a>(
            &'a self,
            input: Value,
            _context: ToolContext,
        ) -> HarnessFuture<'a, ToolExecutionResult> {
            Box::pin(async move {
                let output = json!({"record": input, "status": "active"});
                ToolExecutionResult::with_connector_evidence(
                    output,
                    vec![
                        ConnectorEvidenceClaim::new(
                            "crm",
                            "contacts/customer-42",
                            "revision-7",
                            1,
                            Some(10_000),
                            Some("read-customer-42-revision-7".to_owned()),
                        )
                        .map_err(|error| HarnessError::Tool(error.to_string()))?,
                    ],
                )
                .map_err(|error| HarnessError::Tool(error.to_string()))
            })
        }
    }

    impl PolicyEngine for AuthorityRecordingPolicy {
        fn authorize<'a>(
            &'a self,
            _request: &'a ToolAuthorization,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async move {
                *self
                    .observed
                    .lock()
                    .map_err(|_| HarnessError::PolicyDenied {
                        tool: "echo".to_owned(),
                        reason: "authority recorder poisoned".to_owned(),
                    })? = Some(authority.clone());
                Ok(PolicyDecision::Allow)
            })
        }
    }

    struct PendingTool;

    impl Tool for PendingTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Never completes".to_owned(),
                input_schema: json!({ "type": "object" }),
            }
        }

        fn execute<'a>(&'a self, _input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(pending())
        }
    }

    struct PendingCountingTool {
        calls: Arc<AtomicUsize>,
        entered: Arc<Notify>,
    }

    struct ParallelEchoTool {
        rendezvous: Arc<Barrier>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    struct ParallelFastOrPendingTool {
        fast_done: Arc<Notify>,
        pending_entered: Arc<Notify>,
    }

    struct LoggingBatchTool {
        name: &'static str,
        execution: ToolBatchExecution,
        events: Arc<Mutex<Vec<String>>>,
    }

    impl Tool for PendingCountingTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Never completes after recording entry".to_owned(),
                input_schema: json!({ "type": "object" }),
            }
        }

        fn execute<'a>(&'a self, _input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_one();
                pending().await
            })
        }
    }

    impl Tool for ParallelEchoTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Parallel-safe echo probe".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "required": ["text"]
                }),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            ToolBatchExecution::ParallelSafe
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(current, Ordering::SeqCst);
                self.rendezvous.wait().await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(input)
            })
        }
    }

    impl Tool for ParallelFastOrPendingTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "echo".to_owned(),
                description: "Completes only the first parallel probe".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "text": { "type": "string" } },
                    "required": ["text"]
                }),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            ToolBatchExecution::ParallelSafe
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                if input.get("text").and_then(Value::as_str) == Some("first") {
                    self.fast_done.notify_one();
                    Ok(input)
                } else {
                    self.fast_done.notified().await;
                    self.pending_entered.notify_one();
                    pending().await
                }
            })
        }
    }

    impl Tool for LoggingBatchTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: self.name.to_owned(),
                description: "Records batch scheduling boundaries".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "label": { "type": "string" } },
                    "required": ["label"]
                }),
            }
        }

        fn batch_execution(&self) -> ToolBatchExecution {
            self.execution
        }

        fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                let label = input
                    .get("label")
                    .or_else(|| input.get("text"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| HarnessError::Tool("missing label".to_owned()))?
                    .to_owned();
                self.events
                    .lock()
                    .map_err(|_| HarnessError::Tool("event recorder poisoned".to_owned()))?
                    .push(format!("start:{label}"));
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.events
                    .lock()
                    .map_err(|_| HarnessError::Tool("event recorder poisoned".to_owned()))?
                    .push(format!("end:{label}"));
                Ok(input)
            })
        }
    }

    struct EchoModel;

    struct DuplicateCallModel;

    struct BatchToolModel;

    struct MixedBatchToolModel;

    struct ContinuationModel {
        requests: Arc<Mutex<Vec<ModelRequest>>>,
    }

    struct ContinuationFailingModel {
        calls: Arc<AtomicUsize>,
    }

    impl LanguageModel for EchoModel {
        fn id(&self) -> &str {
            "test/echo-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if let Some(output) = request
                    .items
                    .iter()
                    .rev()
                    .find_map(|item| match &item.kind {
                        ItemKind::ToolResult {
                            output,
                            is_error: false,
                            ..
                        } => Some(output.clone()),
                        _ => None,
                    })
                {
                    return Ok(ModelOutput::Message {
                        content: format!("observed: {output}"),
                    });
                }

                let prompt = request
                    .items
                    .iter()
                    .find_map(|item| match &item.kind {
                        ItemKind::UserMessage { content } => Some(content.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                Ok(ModelOutput::ToolCall {
                    call_id: "call-1".to_owned(),
                    name: "echo".to_owned(),
                    input: json!({ "text": prompt }),
                })
            })
        }
    }

    impl LanguageModel for AuthorityRecordingModel {
        fn id(&self) -> &str {
            "test/authority-recording-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                *self.observed.lock().map_err(|_| {
                    HarnessError::Model("model authority recorder poisoned".to_owned())
                })? = Some(request.authority.clone());
                EchoModel.complete(request).await
            })
        }
    }

    impl LanguageModel for DuplicateCallModel {
        fn id(&self) -> &str {
            "test/duplicate-call-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::ToolCall {
                    call_id: "reused-call".to_owned(),
                    name: "echo".to_owned(),
                    input: json!({"text": "once"}),
                })
            })
        }
    }

    impl LanguageModel for BatchToolModel {
        fn id(&self) -> &str {
            "test/batch-tool-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                let results = request
                    .items
                    .iter()
                    .filter(|item| matches!(item.kind, ItemKind::ToolResult { .. }))
                    .count();
                if results == 2 {
                    return Ok(ModelOutput::Message {
                        content: "observed ordered batch".to_owned(),
                    });
                }
                Ok(ModelOutput::ToolCalls {
                    calls: vec![
                        ModelToolCall {
                            call_id: "batch-call-1".to_owned(),
                            name: "echo".to_owned(),
                            input: json!({"text": "first"}),
                        },
                        ModelToolCall {
                            call_id: "batch-call-2".to_owned(),
                            name: "echo".to_owned(),
                            input: json!({"text": "second"}),
                        },
                    ],
                })
            })
        }
    }

    impl LanguageModel for MixedBatchToolModel {
        fn id(&self) -> &str {
            "test/mixed-batch-tool-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                let results = request
                    .items
                    .iter()
                    .filter(|item| matches!(item.kind, ItemKind::ToolResult { .. }))
                    .count();
                if results == 5 {
                    return Ok(ModelOutput::Message {
                        content: "observed fenced batch".to_owned(),
                    });
                }
                let calls = [
                    ("mixed-call-1", "parallel-probe", "p1"),
                    ("mixed-call-2", "parallel-probe", "p2"),
                    ("mixed-call-3", "exclusive-probe", "x"),
                    ("mixed-call-4", "parallel-probe", "p3"),
                    ("mixed-call-5", "parallel-probe", "p4"),
                ]
                .into_iter()
                .map(|(call_id, name, label)| ModelToolCall {
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                    input: json!({"label": label}),
                })
                .collect();
                Ok(ModelOutput::ToolCalls { calls })
            })
        }
    }

    impl LanguageModel for ContinuationModel {
        fn id(&self) -> &str {
            "test/continuation-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                self.complete_with_metadata(request)
                    .await
                    .map(|response| response.output)
            })
        }

        fn complete_with_metadata<'a>(
            &'a self,
            request: ModelRequest,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                let has_tool_result = request
                    .items
                    .iter()
                    .any(|item| matches!(item.kind, ItemKind::ToolResult { .. }));
                self.requests
                    .lock()
                    .expect("continuation requests")
                    .push(request.clone());
                if has_tool_result {
                    let continuation = request.items.iter().find_map(|item| {
                        if let ItemKind::ProviderContinuation { continuation, .. } = &item.kind {
                            Some(continuation)
                        } else {
                            None
                        }
                    });
                    if continuation.is_none_or(|continuation| {
                        continuation.format() != "test.provider.reasoning.v1"
                    }) {
                        return Err(HarnessError::Model(
                            "provider continuation was not replayed".to_owned(),
                        ));
                    }
                    return Ok(ModelResponse::from(ModelOutput::Message {
                        content: "continued after tool".to_owned(),
                    }));
                }
                Ok(ModelResponse {
                    output: ModelOutput::ToolCall {
                        call_id: "continuation-call".to_owned(),
                        name: "echo".to_owned(),
                        input: json!({"text": "continued"}),
                    },
                    usage: None,
                    provider_model: None,
                    provider_request_id: Some("provider-step-1".to_owned()),
                    continuation: Some(ModelContinuation::new(
                        "test.provider.reasoning.v1",
                        vec![json!({"opaque": "ciphertext"})],
                    )?),
                })
            })
        }
    }

    impl LanguageModel for ContinuationFailingModel {
        fn id(&self) -> &str {
            "test/route-primary"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Err(HarnessError::Model(
                    "continuation model unavailable".to_owned(),
                ))
            })
        }

        fn complete_with_metadata<'a>(
            &'a self,
            _request: ModelRequest,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
                    return Err(HarnessError::Model(
                        "continuation model unavailable".to_owned(),
                    ));
                }
                Ok(ModelResponse {
                    output: ModelOutput::ToolCall {
                        call_id: "continuation-failover-call".to_owned(),
                        name: "echo".to_owned(),
                        input: json!({"text": "stay pinned"}),
                    },
                    usage: None,
                    provider_model: None,
                    provider_request_id: Some("provider-step-1".to_owned()),
                    continuation: Some(ModelContinuation::new(
                        "test.provider.reasoning.v1",
                        vec![json!({"opaque": "ciphertext"})],
                    )?),
                })
            })
        }
    }

    struct OversizedModel;

    impl LanguageModel for OversizedModel {
        fn id(&self) -> &str {
            "test/oversized-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "x".repeat(super::MAX_MODEL_TEXT_BYTES + 1),
                })
            })
        }
    }

    struct UsageModel;

    struct TypedProviderFailureModel;

    struct RouteFailingModel {
        calls: Arc<AtomicUsize>,
        emit_delta: bool,
    }

    struct TypedRouteFailingModel {
        calls: Arc<AtomicUsize>,
    }

    struct TypedRetryModel {
        id: &'static str,
        calls: Arc<AtomicUsize>,
        kind: ModelProviderFailureKind,
        retry_after_ms: Option<u64>,
        succeeds_on_call: Option<usize>,
        emit_before_failure: bool,
        failure_observed: Option<Arc<Notify>>,
    }

    struct RouteSuccessModel {
        calls: Arc<AtomicUsize>,
    }

    struct SuccessThenFailureModel {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Default)]
    struct RecordingModelSink {
        events: Mutex<Vec<ModelStreamEvent>>,
    }

    struct SteeringModel {
        calls: AtomicUsize,
        entered: Arc<Notify>,
        release: Arc<Notify>,
        requests: Arc<Mutex<Vec<ModelRequest>>>,
    }

    struct SteeringToolModel {
        calls: AtomicUsize,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct BlockingAllowPolicy {
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl PolicyEngine for BlockingAllowPolicy {
        fn authorize<'a>(
            &'a self,
            _request: &'a ToolAuthorization,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(PolicyDecision::Allow)
            })
        }
    }

    impl ModelEventSink for RecordingModelSink {
        fn emit(&self, event: &ModelStreamEvent) -> Result<(), String> {
            self.events
                .lock()
                .map_err(|_| "model event recorder poisoned".to_owned())?
                .push(event.clone());
            Ok(())
        }
    }

    impl LanguageModel for SteeringModel {
        fn id(&self) -> &str {
            "test/steering-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Err(HarnessError::Model(
                    "steering test requires streaming entrypoint".to_owned(),
                ))
            })
        }

        fn complete_streaming<'a>(
            &'a self,
            request: ModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                self.requests
                    .lock()
                    .expect("steering requests")
                    .push(request.clone());
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let _ = stream.emit_text_delta("stale provisional response");
                    self.entered.notify_one();
                    self.release.notified().await;
                    return Ok(ModelResponse::from(ModelOutput::Message {
                        content: "stale final response".to_owned(),
                    }));
                }
                let correction = request
                    .items
                    .iter()
                    .rev()
                    .find_map(|item| match &item.kind {
                        ItemKind::UserMessage { content } => Some(content.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                Ok(ModelResponse::from(ModelOutput::Message {
                    content: format!("accepted: {correction}"),
                }))
            })
        }
    }

    impl LanguageModel for SteeringToolModel {
        fn id(&self) -> &str {
            "test/steering-tool-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.entered.notify_one();
                    self.release.notified().await;
                    return Ok(ModelOutput::ToolCall {
                        call_id: "stale-tool-call".to_owned(),
                        name: "echo".to_owned(),
                        input: json!({"text": "must not execute"}),
                    });
                }
                let correction = request
                    .items
                    .iter()
                    .rev()
                    .find_map(|item| match &item.kind {
                        ItemKind::UserMessage { content } => Some(content.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                Ok(ModelOutput::Message {
                    content: format!("accepted: {correction}"),
                })
            })
        }
    }

    impl LanguageModel for RouteFailingModel {
        fn id(&self) -> &str {
            "test/route-primary"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async { Err(HarnessError::Model("primary model unavailable".to_owned())) })
        }

        fn complete_streaming<'a>(
            &'a self,
            _request: ModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if self.emit_delta {
                    let _ = stream.emit_text_delta("primary fragment");
                }
                Err(HarnessError::Model("primary model unavailable".to_owned()))
            })
        }
    }

    impl LanguageModel for RouteSuccessModel {
        fn id(&self) -> &str {
            "test/route-secondary"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(ModelOutput::Message {
                    content: "secondary result".to_owned(),
                })
            })
        }
    }

    impl LanguageModel for TypedRouteFailingModel {
        fn id(&self) -> &str {
            "test/typed-route-primary"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(HarnessError::ModelProvider(ModelProviderFailure::new(
                    ModelProviderFailureKind::RateLimited,
                    "provider rate limit reached",
                    Some(429),
                    Some(2_000),
                )?))
            })
        }
    }

    impl LanguageModel for TypedRetryModel {
        fn id(&self) -> &str {
            self.id
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Err(HarnessError::Model(
                    "typed retry fixture requires streaming entrypoint".to_owned(),
                ))
            })
        }

        fn complete_streaming<'a>(
            &'a self,
            _request: ModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                if self.succeeds_on_call == Some(call) {
                    return Ok(ModelResponse::from(ModelOutput::Message {
                        content: "retry settled".to_owned(),
                    }));
                }
                if self.emit_before_failure {
                    let _ = stream.emit_text_delta("provisional");
                }
                if let Some(observed) = &self.failure_observed {
                    observed.notify_one();
                }
                Err(HarnessError::ModelProvider(ModelProviderFailure::new(
                    self.kind,
                    "typed retry fixture failure",
                    None,
                    self.retry_after_ms,
                )?))
            })
        }
    }

    impl LanguageModel for SuccessThenFailureModel {
        fn id(&self) -> &str {
            "test/route-secondary"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(ModelOutput::Message {
                        content: "secondary result".to_owned(),
                    })
                } else {
                    Err(HarnessError::Model(
                        "secondary model unavailable".to_owned(),
                    ))
                }
            })
        }
    }

    impl LanguageModel for UsageModel {
        fn id(&self) -> &str {
            "test/usage-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "observed".to_owned(),
                })
            })
        }

        fn complete_with_metadata<'a>(
            &'a self,
            _request: ModelRequest,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async {
                Ok(ModelResponse {
                    output: ModelOutput::Message {
                        content: "observed".to_owned(),
                    },
                    usage: Some(ModelUsage {
                        input_tokens: 100,
                        output_tokens: 10,
                        cached_input_tokens: 40,
                        reasoning_tokens: 2,
                        cost_usd_ticks: Some(250_000),
                    }),
                    provider_model: Some("provider/settled-v2".to_owned()),
                    provider_request_id: Some("provider-request".to_owned()),
                    continuation: None,
                })
            })
        }
    }

    impl LanguageModel for TypedProviderFailureModel {
        fn id(&self) -> &str {
            "test/typed-provider-failure"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Err(HarnessError::ModelProvider(ModelProviderFailure::new(
                    ModelProviderFailureKind::RateLimited,
                    "provider rate limit reached",
                    Some(429),
                    Some(2_000),
                )?))
            })
        }
    }

    struct RecordingHistoryModel {
        requests: Arc<Mutex<Vec<ModelRequest>>>,
    }

    struct StaticCompactor;
    struct PanickingCompactor;

    impl ConversationCompactor for StaticCompactor {
        fn descriptor(&self) -> ConversationCompactorDescriptor {
            ConversationCompactorDescriptor {
                name: "test.static-summary".to_owned(),
                description: "Produces a stable runtime integration summary".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            }
        }

        fn compact<'a>(
            &'a self,
            request: ConversationCompactionRequest,
        ) -> HarnessFuture<'a, ConversationCompactionResponse> {
            Box::pin(async move {
                assert_eq!(request.turns.len(), 1);
                Ok(ConversationCompactionResponse {
                    summary: "An earlier request was answered.".to_owned(),
                })
            })
        }
    }

    impl ConversationCompactor for PanickingCompactor {
        fn descriptor(&self) -> ConversationCompactorDescriptor {
            ConversationCompactorDescriptor {
                name: "test.panicking-summary".to_owned(),
                description: "Panics only in a Runtime isolation fixture".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            }
        }

        fn compact<'a>(
            &'a self,
            _request: ConversationCompactionRequest,
        ) -> HarnessFuture<'a, ConversationCompactionResponse> {
            Box::pin(async { panic!("sensitive compactor payload") })
        }
    }

    impl LanguageModel for RecordingHistoryModel {
        fn id(&self) -> &str {
            "test/history-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                let prompt = request
                    .items
                    .iter()
                    .rev()
                    .find_map(|item| match &item.kind {
                        ItemKind::UserMessage { content } => Some(content.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                self.requests
                    .lock()
                    .map_err(|_| HarnessError::Model("request recorder poisoned".to_owned()))?
                    .push(request);
                Ok(ModelOutput::Message {
                    content: format!("answer to {prompt}"),
                })
            })
        }
    }

    struct OversizedTool;

    impl Tool for OversizedTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "oversized".to_owned(),
                description: "Returns an oversized value".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(&'a self, _input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
            Box::pin(async { Ok(Value::String("x".repeat(super::MAX_TOOL_OUTPUT_BYTES + 1))) })
        }
    }

    struct ToolErrorObserverModel;

    impl LanguageModel for ToolErrorObserverModel {
        fn id(&self) -> &str {
            "test/tool-error-observer"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if request
                    .items
                    .iter()
                    .any(|item| matches!(item.kind, ItemKind::ToolResult { .. }))
                {
                    return Ok(ModelOutput::Message {
                        content: "handled bounded tool error".to_owned(),
                    });
                }
                Ok(ModelOutput::ToolCall {
                    call_id: "oversized-call".to_owned(),
                    name: "oversized".to_owned(),
                    input: json!({}),
                })
            })
        }
    }

    struct ContextModel;

    impl LanguageModel for ContextModel {
        fn id(&self) -> &str {
            "test/context-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                let text = request
                    .context
                    .first()
                    .map(|block| block.text.clone())
                    .ok_or_else(|| HarnessError::Model("missing compiled context".to_owned()))?;
                Ok(ModelOutput::Message { content: text })
            })
        }
    }

    struct PendingModel {
        entered: Arc<Notify>,
    }

    struct CancellablePendingModel {
        calls: Arc<AtomicUsize>,
        cancellation_observed: Arc<AtomicUsize>,
    }

    struct PanickingModel;

    struct PanickingIdentityModel;

    struct CountingIdentityModel {
        calls: Arc<AtomicUsize>,
    }

    struct RevisionModel;

    impl LanguageModel for RevisionModel {
        fn id(&self) -> &str {
            "test/revision-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                let revising = request.items.iter().any(|item| {
                    matches!(
                        &item.kind,
                        ItemKind::VerificationResult {
                            outcome: VerificationOutcome::Failed { .. },
                            ..
                        }
                    )
                });
                Ok(ModelOutput::Message {
                    content: if revising { "good" } else { "bad" }.to_owned(),
                })
            })
        }
    }

    struct CandidateVerifier {
        retryable: bool,
    }

    struct BlockingRetryVerifier {
        calls: AtomicUsize,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Verifier for CandidateVerifier {
        fn descriptor(&self) -> VerifierDescriptor {
            VerifierDescriptor {
                name: "candidate-quality".to_owned(),
                description: "Requires the candidate to equal good".to_owned(),
            }
        }

        fn verify<'a>(
            &'a self,
            request: VerificationRequest,
        ) -> HarnessFuture<'a, VerificationOutcome> {
            Box::pin(async move {
                if request.candidate == "good" {
                    Ok(VerificationOutcome::Passed {
                        summary: Some("candidate accepted".to_owned()),
                    })
                } else {
                    Ok(VerificationOutcome::Failed {
                        reason: "candidate must equal good".to_owned(),
                        retryable: self.retryable,
                    })
                }
            })
        }
    }

    impl Verifier for BlockingRetryVerifier {
        fn descriptor(&self) -> VerifierDescriptor {
            VerifierDescriptor {
                name: "blocking-candidate-quality".to_owned(),
                description: "Exercises steering across a retryable completion gate".to_owned(),
            }
        }

        fn verify<'a>(
            &'a self,
            request: VerificationRequest,
        ) -> HarnessFuture<'a, VerificationOutcome> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                if request.candidate == "good" {
                    Ok(VerificationOutcome::Passed {
                        summary: Some("candidate accepted".to_owned()),
                    })
                } else {
                    Ok(VerificationOutcome::Failed {
                        reason: "candidate must equal good".to_owned(),
                        retryable: true,
                    })
                }
            })
        }
    }

    struct PendingVerifier;

    impl Verifier for PendingVerifier {
        fn descriptor(&self) -> VerifierDescriptor {
            VerifierDescriptor {
                name: "pending".to_owned(),
                description: "Never settles".to_owned(),
            }
        }

        fn verify<'a>(
            &'a self,
            _request: VerificationRequest,
        ) -> HarnessFuture<'a, VerificationOutcome> {
            Box::pin(pending())
        }
    }

    impl LanguageModel for PendingModel {
        fn id(&self) -> &str {
            "test/pending-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                self.entered.notify_one();
                pending().await
            })
        }
    }

    impl LanguageModel for CancellablePendingModel {
        fn id(&self) -> &str {
            "test/cancellable-pending-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(pending())
        }

        fn complete_streaming<'a>(
            &'a self,
            _request: ModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let cancellation = stream.cancellation_token();
                let observed = self.cancellation_observed.clone();
                tokio::spawn(async move {
                    cancellation.cancelled().await;
                    observed.fetch_add(1, Ordering::SeqCst);
                });
                pending().await
            })
        }
    }

    impl LanguageModel for PanickingModel {
        fn id(&self) -> &str {
            "test/panicking-model"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            panic!("sensitive model panic")
        }

        fn complete_streaming<'a>(
            &'a self,
            _request: ModelRequest,
            _stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            panic!("sensitive model panic")
        }
    }

    impl LanguageModel for PanickingIdentityModel {
        fn id(&self) -> &str {
            panic!("sensitive identity panic")
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "unreachable".to_owned(),
                })
            })
        }
    }

    impl LanguageModel for CountingIdentityModel {
        fn id(&self) -> &str {
            self.calls.fetch_add(1, Ordering::SeqCst);
            "test/counting-identity"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "done".to_owned(),
                })
            })
        }
    }

    struct ErrorPolicy;

    impl PolicyEngine for ErrorPolicy {
        fn authorize<'a>(
            &'a self,
            _request: &'a ToolAuthorization,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async { Err(HarnessError::Policy("provider unavailable".to_owned())) })
        }
    }

    struct AskPolicy;

    impl PolicyEngine for AskPolicy {
        fn authorize<'a>(
            &'a self,
            request: &'a ToolAuthorization,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async move {
                assert_eq!(request.descriptor.name, "echo");
                assert_eq!(request.call_id, "call-1");
                Ok(PolicyDecision::Ask {
                    reason: "operator confirmation required".to_owned(),
                    risk: RiskLevel::High,
                })
            })
        }
    }

    struct BatchAskFirstPolicy;

    impl PolicyEngine for BatchAskFirstPolicy {
        fn authorize<'a>(
            &'a self,
            request: &'a ToolAuthorization,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async move {
                match request.call_id.as_str() {
                    "batch-call-1" => Ok(PolicyDecision::Ask {
                        reason: "approve the ordered batch".to_owned(),
                        risk: RiskLevel::High,
                    }),
                    "batch-call-2" => Ok(PolicyDecision::Allow),
                    other => Err(HarnessError::Policy(format!(
                        "unexpected batch call {other}"
                    ))),
                }
            })
        }
    }

    struct ApproveAll {
        decisions: Arc<AtomicUsize>,
    }

    impl ApprovalHandler for ApproveAll {
        fn decide<'a>(
            &'a self,
            request: &'a ApprovalRequest,
        ) -> HarnessFuture<'a, ApprovalDecision> {
            Box::pin(async move {
                assert_eq!(request.authorization.descriptor.name, "echo");
                assert_eq!(request.authorization.call_id, "call-1");
                assert_eq!(request.risk, RiskLevel::High);
                self.decisions.fetch_add(1, Ordering::SeqCst);
                Ok(ApprovalDecision::Approve)
            })
        }
    }

    struct PendingApproval;

    impl ApprovalHandler for PendingApproval {
        fn decide<'a>(
            &'a self,
            _request: &'a ApprovalRequest,
        ) -> HarnessFuture<'a, ApprovalDecision> {
            Box::pin(pending())
        }
    }

    struct TestMemoryProvider;

    impl MemoryProvider for TestMemoryProvider {
        fn descriptor(&self) -> MemoryProviderDescriptor {
            MemoryProviderDescriptor {
                name: "agent-memory-hub".to_owned(),
                description: "test memory provider".to_owned(),
                api_version: MEMORY_API_VERSION,
                operations: BTreeSet::from([MemoryOperation::Search]),
            }
        }

        fn search<'a>(
            &'a self,
            _request: MemorySearchRequest,
        ) -> HarnessFuture<'a, MemorySearchResponse> {
            Box::pin(async {
                Ok(MemorySearchResponse {
                    packs: vec![MemoryContextPack {
                        reference: MemoryReference::new("mem-1"),
                        title: Some("Remembered decision".to_owned()),
                        text: "use the remembered decision".to_owned(),
                        selected_view: MemoryView::Overview,
                        detail_uri: Some("memory://items/mem-1/body".to_owned()),
                        packed_tokens: 7,
                        provenance: Vec::new(),
                    }],
                    warnings: Vec::new(),
                })
            })
        }
    }

    fn registry(calls: Arc<AtomicUsize>) -> ToolRegistry {
        let mut registry = ToolRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(EchoTool { calls }))
            .expect("echo tool should register");
        registry
    }

    fn runtime(
        calls: Arc<AtomicUsize>,
        policy: AllowListPolicy,
        state: StateEngine,
    ) -> HarnessRuntime {
        HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls),
            Arc::new(policy),
            state,
        )
    }

    #[tokio::test]
    async fn runs_tool_loop_and_records_ordered_state() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = runtime(
            calls.clone(),
            AllowListPolicy::deny_by_default().allow("echo"),
            state.clone(),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "hello")
            .await
            .expect("turn should complete");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.turn.status, TurnStatus::Completed);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns.len(), 1);
        assert_eq!(projected.turns[0], outcome.turn);
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 9);
        assert!(matches!(
            outcome.turn.items.as_slice(),
            [
                crate::Item {
                    kind: ItemKind::UserMessage { .. },
                    ..
                },
                crate::Item {
                    kind: ItemKind::ConversationContext { .. },
                    ..
                },
                crate::Item {
                    kind: ItemKind::ToolCall { .. },
                    ..
                },
                crate::Item {
                    kind: ItemKind::PolicyDecision {
                        tool_origin: Some(CapabilityOrigin::BuiltIn),
                        ..
                    },
                    ..
                },
                crate::Item {
                    kind: ItemKind::ToolResult {
                        is_error: false,
                        ..
                    },
                    ..
                },
                crate::Item {
                    kind: ItemKind::AssistantMessage { .. },
                    ..
                }
            ]
        ));
    }

    #[tokio::test]
    async fn connector_evidence_is_runtime_bound_atomic_model_hidden_and_archive_safe() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-42".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("scoped authority");
        let origin = CapabilityOrigin::External {
            id: "connector.crm".to_owned(),
        };
        let mut tools = ToolRegistry::new();
        tools
            .register(origin.clone(), Arc::new(EvidenceConnectorTool))
            .expect("Connector Tool");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            state.clone(),
        );
        let thread = runtime.create_thread_as(&authority).await.expect("thread");

        let outcome = runtime
            .run_turn_with_options(
                &thread.id,
                "hello",
                TurnExecutionOptions {
                    authority: authority.clone(),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect("evidence-aware Turn");
        let (output, evidence) = outcome
            .turn
            .items
            .iter()
            .find_map(|item| {
                if let ItemKind::ToolResult {
                    output,
                    connector_evidence,
                    ..
                } = &item.kind
                {
                    Some((output, connector_evidence))
                } else {
                    None
                }
            })
            .expect("Tool result");
        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].connector(), "echo");
        assert_eq!(evidence[0].connector_origin(), &origin);
        assert_eq!(evidence[0].authority(), &authority);
        assert_eq!(
            evidence[0].output_sha256(),
            crate::json::bounded_serialized_sha256(output, super::MAX_TOOL_OUTPUT_BYTES)
                .expect("output digest")
        );
        assert_eq!(evidence[0].claim().source(), "crm");
        assert!(
            crate::context::model_visible_items(&outcome.turn.items)
                .iter()
                .filter_map(|item| {
                    if let ItemKind::ToolResult {
                        connector_evidence, ..
                    } = &item.kind
                    {
                        Some(connector_evidence)
                    } else {
                        None
                    }
                })
                .all(Vec::is_empty)
        );

        let events = state
            .events_as(&thread.id, &authority)
            .await
            .expect("events");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    &event.event,
                    StateEvent::ItemAppended {
                        item: crate::Item {
                            kind: ItemKind::ToolResult {
                                connector_evidence,
                                ..
                            },
                            ..
                        },
                        ..
                    } if connector_evidence.len() == 1
                ))
                .count(),
            1,
            "Tool output and Connector evidence must share one atomic event"
        );

        let archive = state
            .export_thread_as(&thread.id, &authority)
            .await
            .expect("archive");
        state
            .import_thread_as(
                &archive,
                ThreadId::from_static("connector-import-same-tenant"),
                &authority,
            )
            .await
            .expect("same-tenant import");
        let other_tenant =
            AuthorityContext::new(authority.actor().clone(), Some("tenant-b".to_owned()))
                .expect("other tenant");
        let error = state
            .import_thread_as(
                &archive,
                ThreadId::from_static("connector-import-other-tenant"),
                &other_tenant,
            )
            .await
            .expect_err("tenant rebinding must fail");
        assert!(
            error
                .to_string()
                .contains("tenant-bound authority evidence")
        );
    }

    #[tokio::test]
    async fn trusted_authority_reaches_policy_and_tool_execution() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-42".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("scoped authority");
        let observed_policy = Arc::new(Mutex::new(None));
        let observed_tool = Arc::new(Mutex::new(None));
        let observed_model = Arc::new(Mutex::new(None));
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(AuthorityProbeTool {
                    observed: observed_tool.clone(),
                }),
            )
            .expect("probe Tool");
        let runtime = HarnessRuntime::new(
            Arc::new(AuthorityRecordingModel {
                observed: observed_model.clone(),
            }),
            tools,
            Arc::new(AuthorityRecordingPolicy {
                observed: observed_policy.clone(),
            }),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread_as(&authority).await.expect("thread");
        runtime
            .run_turn_with_options(
                &thread.id,
                "hello",
                TurnExecutionOptions {
                    authority: authority.clone(),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect("Turn");
        assert_eq!(
            observed_model
                .lock()
                .expect("observed Model authority")
                .as_ref(),
            Some(&authority)
        );
        assert_eq!(
            observed_policy
                .lock()
                .expect("observed Policy authority")
                .as_ref(),
            Some(&authority)
        );
        assert_eq!(
            observed_tool
                .lock()
                .expect("observed Tool authority")
                .as_ref(),
            Some(&authority)
        );
    }

    #[tokio::test]
    async fn tenant_scoped_approval_is_durable_and_executes_only_after_settlement() {
        let calls = Arc::new(AtomicUsize::new(0));
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-42".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("scoped authority");
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let handler = InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
            .expect("approval handler");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(EchoModel),
                registry(calls.clone()),
                Arc::new(AskPolicy),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_approval_handler(Arc::new(handler)),
        );
        let thread = runtime.create_thread_as(&authority).await.expect("thread");
        let waiter = tokio::spawn({
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            let authority = authority.clone();
            async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "protected",
                        TurnExecutionOptions {
                            authority,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });
        let pending = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(record) = inbox
                    .pending_as(1, &authority)
                    .await
                    .expect("pending approvals")
                    .pop()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval request timeout");
        assert_eq!(pending.tenant_id(), Some("tenant-a"));
        let other_tenant = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "tenant-b-approver".to_owned(),
            },
            Some("tenant-b".to_owned()),
        )
        .expect("other tenant");
        assert!(
            inbox
                .get_as(&pending.request.id, &other_tenant)
                .await
                .expect("cross-tenant approval read")
                .is_none()
        );
        let approver = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "tenant-a-approver".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("same tenant approver");
        inbox
            .settle_as(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                &approver,
            )
            .await
            .expect("settle tenant approval");
        waiter
            .await
            .expect("join tenant Turn")
            .expect("complete tenant Turn");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn prepares_a_read_only_digest_bound_thread_handoff() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = runtime(
            calls,
            AllowListPolicy::deny_by_default().allow("echo"),
            state.clone(),
        );
        let source = runtime.create_thread().await.expect("create source");
        let shared = runtime
            .run_turn(&source.id, "shared")
            .await
            .expect("shared turn");
        let target_id = ThreadId::from_static("runtime-handoff-target");
        runtime
            .fork_thread(&source.id, target_id.clone(), Some(&shared.turn.id))
            .await
            .expect("fork target");
        runtime
            .run_turn(&source.id, "source-only")
            .await
            .expect("source-only turn");
        let target_events_before = state.events(&target_id).await.expect("target events").len();

        let request = runtime
            .prepare_thread_handoff(&source.id, &target_id, &ThreadHandoffConfig::default())
            .await
            .expect("prepare handoff")
            .expect("source delta");

        assert_eq!(request.shared_prefix_turns, 1);
        assert_eq!(request.turns.len(), 1);
        assert_eq!(request.older_source_turns, 0);
        assert_eq!(
            state.events(&target_id).await.expect("target events").len(),
            target_events_before
        );
        let context = request
            .to_context("Source explored a separate path.")
            .expect("context");
        assert_eq!(context.source, "thread-handoff");
        assert!(context.reference.contains(&request.source_sha256));
    }

    #[tokio::test]
    async fn runs_same_response_tool_calls_as_one_ordered_durable_batch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(BatchToolModel),
            registry(calls.clone()),
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            state.clone(),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "batch")
            .await
            .expect("batch turn");

        assert_eq!(outcome.final_text, "observed ordered batch");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let events = state.events(&thread.id).await.expect("events");
        let batches = events
            .iter()
            .filter_map(|event| match &event.event {
                StateEvent::ToolCallsAppended { calls, .. } => Some(calls),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 2);

        let items = &outcome.turn.items;
        assert_eq!(items.len(), 9);
        let first_batch = match &items[2].kind {
            ItemKind::ToolCall { call_id, batch, .. } if call_id == "batch-call-1" => {
                batch.as_ref().expect("first batch position")
            }
            other => panic!("unexpected first batch Item: {other:?}"),
        };
        let second_batch = match &items[3].kind {
            ItemKind::ToolCall { call_id, batch, .. } if call_id == "batch-call-2" => {
                batch.as_ref().expect("second batch position")
            }
            other => panic!("unexpected second batch Item: {other:?}"),
        };
        assert_eq!(first_batch.id, second_batch.id);
        assert_eq!((first_batch.index, first_batch.size), (0, 2));
        assert_eq!((second_batch.index, second_batch.size), (1, 2));
        assert!(matches!(
            &items[4].kind,
            ItemKind::PolicyDecision { call_id, .. } if call_id == "batch-call-1"
        ));
        assert!(matches!(
            &items[5].kind,
            ItemKind::PolicyDecision { call_id, .. } if call_id == "batch-call-2"
        ));
        assert!(matches!(
            &items[6].kind,
            ItemKind::ToolResult {
                call_id,
                is_error: false,
                ..
            } if call_id == "batch-call-1"
        ));
        assert!(matches!(
            &items[7].kind,
            ItemKind::ToolResult {
                call_id,
                is_error: false,
                ..
            } if call_id == "batch-call-2"
        ));
        assert!(matches!(items[8].kind, ItemKind::AssistantMessage { .. }));
    }

    #[tokio::test]
    async fn provider_continuation_is_durable_and_replayed_through_the_tool_loop() {
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(ContinuationModel {
                requests: requests.clone(),
            }),
            registry(calls.clone()),
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "continue safely")
            .await
            .expect("continuation turn");

        assert_eq!(outcome.final_text, "continued after tool");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(requests.lock().expect("requests").len(), 2);
        let continuation_index = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::ProviderContinuation { .. }))
            .expect("durable continuation");
        let tool_index = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::ToolCall { .. }))
            .expect("tool call");
        assert!(continuation_index < tool_index);
        assert!(matches!(
            &outcome.turn.items[continuation_index].kind,
            ItemKind::ProviderContinuation {
                model_id,
                model_origin: CapabilityOrigin::BuiltIn,
                continuation,
            } if model_id == "test/continuation-model"
                && continuation.format() == "test.provider.reasoning.v1"
        ));
    }

    #[tokio::test]
    async fn explicitly_safe_tool_calls_overlap_but_settle_in_source_order() {
        let rendezvous = Arc::new(Barrier::new(3));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(ParallelEchoTool {
                    rendezvous: rendezvous.clone(),
                    in_flight: in_flight.clone(),
                    max_in_flight: max_in_flight.clone(),
                }),
            )
            .expect("parallel-safe Tool");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(BatchToolModel),
                tools,
                Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_max_parallel_tool_calls(2)
            .expect("parallel limit"),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = tokio::spawn({
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            async move { runtime.run_turn(&thread_id, "parallel batch").await }
        });

        if tokio::time::timeout(Duration::from_secs(1), rendezvous.wait())
            .await
            .is_err()
        {
            worker.abort();
            let _ = worker.await;
            panic!("parallel-safe calls did not overlap");
        }
        let outcome = worker.await.expect("worker").expect("parallel batch");

        assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        let results = outcome
            .turn
            .items
            .iter()
            .filter_map(|item| match &item.kind {
                ItemKind::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, ["batch-call-1", "batch-call-2"]);
    }

    #[tokio::test]
    async fn sequential_tool_fences_neighboring_parallel_safe_runs() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut tools = ToolRegistry::new();
        tools
            .register_batch([
                (
                    CapabilityOrigin::BuiltIn,
                    Arc::new(LoggingBatchTool {
                        name: "parallel-probe",
                        execution: ToolBatchExecution::ParallelSafe,
                        events: events.clone(),
                    }) as Arc<dyn Tool>,
                ),
                (
                    CapabilityOrigin::BuiltIn,
                    Arc::new(LoggingBatchTool {
                        name: "exclusive-probe",
                        execution: ToolBatchExecution::Sequential,
                        events: events.clone(),
                    }) as Arc<dyn Tool>,
                ),
            ])
            .expect("logging Tools");
        let runtime = HarnessRuntime::new(
            Arc::new(MixedBatchToolModel),
            tools,
            Arc::new(
                AllowListPolicy::deny_by_default()
                    .allow("parallel-probe")
                    .allow("exclusive-probe"),
            ),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_max_parallel_tool_calls(2)
        .expect("parallel limit");
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "mixed batch")
            .await
            .expect("mixed batch");
        assert_eq!(outcome.final_text, "observed fenced batch");
        let events = events.lock().expect("events").clone();
        let position = |event: &str| {
            events
                .iter()
                .position(|candidate| candidate == event)
                .expect("event position")
        };
        assert!(position("end:p1") < position("start:x"));
        assert!(position("end:p2") < position("start:x"));
        assert!(position("end:x") < position("start:p3"));
        assert!(position("end:x") < position("start:p4"));
        let results = outcome
            .turn
            .items
            .iter()
            .filter_map(|item| match &item.kind {
                ItemKind::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            results,
            [
                "mixed-call-1",
                "mixed-call-2",
                "mixed-call-3",
                "mixed-call-4",
                "mixed-call-5"
            ]
        );
    }

    #[tokio::test]
    async fn parallel_safe_calls_respect_a_runtime_limit_of_one() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(LoggingBatchTool {
                    name: "echo",
                    execution: ToolBatchExecution::ParallelSafe,
                    events: events.clone(),
                }),
            )
            .expect("logging Tool");
        let runtime = HarnessRuntime::new(
            Arc::new(BatchToolModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_max_parallel_tool_calls(1)
        .expect("sequential limit");
        let thread = runtime.create_thread().await.expect("create thread");

        runtime
            .run_turn(&thread.id, "bounded batch")
            .await
            .expect("bounded batch");

        assert_eq!(
            *events.lock().expect("events"),
            ["start:first", "end:first", "start:second", "end:second"]
        );
    }

    #[tokio::test]
    async fn parallel_batch_timeout_keeps_completed_effect_evidence_and_stops() {
        let fast_done = Arc::new(Notify::new());
        let pending_entered = Arc::new(Notify::new());
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(ParallelFastOrPendingTool {
                    fast_done,
                    pending_entered,
                }),
            )
            .expect("parallel timeout Tool");
        let runtime = HarnessRuntime::new(
            Arc::new(BatchToolModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_max_parallel_tool_calls(2)
        .expect("parallel limit");
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "parallel timeout",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(20)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("parallel timeout");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Tool
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        let results = projected.turns[0]
            .items
            .iter()
            .filter_map(|item| match &item.kind {
                ItemKind::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, ["batch-call-1"]);
        assert!(projected.turns[0].items.iter().any(|item| matches!(
            item.kind,
            ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: ExecutionPhase::Tool,
            }
        )));
    }

    #[tokio::test]
    async fn parallel_batch_cancellation_keeps_completed_effect_evidence_and_stops() {
        let fast_done = Arc::new(Notify::new());
        let pending_entered = Arc::new(Notify::new());
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(ParallelFastOrPendingTool {
                    fast_done,
                    pending_entered: pending_entered.clone(),
                }),
            )
            .expect("parallel cancellation Tool");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(BatchToolModel),
                tools,
                Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_max_parallel_tool_calls(2)
            .expect("parallel limit"),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let cancellation = CancellationToken::new();
        let running = tokio::spawn({
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            let cancellation = cancellation.clone();
            async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "parallel cancellation",
                        TurnExecutionOptions {
                            cancellation,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), pending_entered.notified())
            .await
            .expect("pending sibling entered");
        cancellation.cancel();

        assert_eq!(
            running
                .await
                .expect("parallel task")
                .expect_err("parallel cancellation"),
            HarnessError::Cancelled {
                phase: ExecutionPhase::Tool
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Cancelled);
        let results = projected.turns[0]
            .items
            .iter()
            .filter_map(|item| match &item.kind {
                ItemKind::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, ["batch-call-1"]);
        assert!(projected.turns[0].items.iter().any(|item| matches!(
            item.kind,
            ItemKind::TurnStopped {
                reason: TurnStopReason::Cancelled,
                phase: ExecutionPhase::Tool,
            }
        )));
    }

    #[tokio::test]
    async fn provider_continuation_suppresses_cross_model_failover() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "primary-provider".to_owned(),
                },
                Arc::new(ContinuationFailingModel {
                    calls: primary_calls.clone(),
                }),
            )
            .expect("register primary");
        models
            .register(
                CapabilityOrigin::External {
                    id: "secondary-provider".to_owned(),
                },
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("register secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/route-secondary"],
            registry(tool_calls.clone()),
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("failover route");
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn(&thread.id, "do not cross providers")
            .await
            .expect_err("continuation failure must not reach secondary");

        assert!(error.to_string().contains("continuation model unavailable"));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
    }

    #[test]
    fn provider_continuation_rejects_tool_call_provenance_tampering() {
        let items = vec![
            crate::Item::new(ItemKind::UserMessage {
                content: "tampered".to_owned(),
            }),
            crate::Item::new(ItemKind::ProviderContinuation {
                model_id: "test/continuation-model".to_owned(),
                model_origin: CapabilityOrigin::BuiltIn,
                continuation: ModelContinuation::new(
                    "test.provider.reasoning.v1",
                    vec![json!({"opaque": "ciphertext"})],
                )
                .expect("continuation"),
            }),
            crate::Item::new(ItemKind::ToolCall {
                model_id: Some("test/other-model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                call_id: "tampered-call".to_owned(),
                name: "echo".to_owned(),
                input: json!({}),
                batch: None,
            }),
            crate::Item::new(ItemKind::ToolResult {
                call_id: "tampered-call".to_owned(),
                output: json!({"ok": true}),
                is_error: false,
                connector_evidence: Vec::new(),
            }),
        ];

        let error =
            super::pending_provider_continuation_target(&items).expect_err("provenance mismatch");
        assert!(error.to_string().contains("provenance differ"));
    }

    #[test]
    fn provider_continuation_does_not_pin_a_later_user_turn() {
        let items = vec![
            crate::Item::new(ItemKind::UserMessage {
                content: "first turn".to_owned(),
            }),
            crate::Item::new(ItemKind::ProviderContinuation {
                model_id: "test/continuation-model".to_owned(),
                model_origin: CapabilityOrigin::BuiltIn,
                continuation: ModelContinuation::new(
                    "test.provider.reasoning.v1",
                    vec![json!({"opaque": "ciphertext"})],
                )
                .expect("continuation"),
            }),
            crate::Item::new(ItemKind::ToolCall {
                model_id: Some("test/continuation-model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                call_id: "completed-call".to_owned(),
                name: "echo".to_owned(),
                input: json!({}),
                batch: None,
            }),
            crate::Item::new(ItemKind::ToolResult {
                call_id: "completed-call".to_owned(),
                output: json!({"ok": true}),
                is_error: false,
                connector_evidence: Vec::new(),
            }),
            crate::Item::new(ItemKind::AssistantMessage {
                model_id: Some("test/continuation-model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                content: "first turn complete".to_owned(),
            }),
            crate::Item::new(ItemKind::UserMessage {
                content: "second turn".to_owned(),
            }),
        ];

        assert!(
            super::pending_provider_continuation_target(&items)
                .expect("completed chain")
                .is_none()
        );
    }

    #[tokio::test]
    async fn registered_tool_origin_reaches_authoritative_policy_state() {
        let calls = Arc::new(AtomicUsize::new(0));
        let origin = CapabilityOrigin::External {
            id: "test-tool-provider".to_owned(),
        };
        let mut tools = ToolRegistry::new();
        tools
            .register(
                origin.clone(),
                Arc::new(EchoTool {
                    calls: calls.clone(),
                }),
            )
            .expect("register external Tool");
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "provenance")
            .await
            .expect("run Tool turn");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(outcome.turn.items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::PolicyDecision {
                    tool_origin: Some(tool_origin),
                    decision: PolicyDecision::Allow,
                    ..
                } if tool_origin == &origin
            )
        }));
    }

    #[tokio::test]
    async fn duplicate_tool_call_identity_never_reexecutes_a_side_effect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = HarnessRuntime::new(
            Arc::new(DuplicateCallModel),
            registry(calls.clone()),
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("thread");

        let error = runtime
            .run_turn(&thread.id, "duplicate")
            .await
            .expect_err("duplicate Tool call id");

        assert!(matches!(error, HarnessError::Model(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
    }

    #[tokio::test]
    async fn policy_denial_happens_before_tool_execution() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = runtime(
            calls.clone(),
            AllowListPolicy::deny_by_default(),
            state.clone(),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn(&thread.id, "blocked")
            .await
            .expect_err("tool should be denied");

        assert!(matches!(error, HarnessError::PolicyDenied { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 8);
    }

    #[tokio::test]
    async fn compiled_memory_reaches_model_and_is_recorded() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let mut memories = MemoryRegistry::new();
        memories
            .register(
                CapabilityOrigin::TrustedExtension {
                    id: "agent-memory-hub".to_owned(),
                },
                Arc::new(TestMemoryProvider),
            )
            .expect("register memory");
        let context = ContextEngine::with_memory(
            memories,
            MemoryContextConfig {
                provider: "agent-memory-hub".to_owned(),
                top_k: 5,
                budget_tokens: 100,
                failure_mode: MemoryFailureMode::FailTurn,
            },
        );
        let runtime = HarnessRuntime::new(
            Arc::new(ContextModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state.clone(),
        )
        .with_context_engine(context);
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "what did we decide?")
            .await
            .expect("turn");

        assert_eq!(outcome.final_text, "use the remembered decision");
        assert!(matches!(
            &outcome.turn.items[2].kind,
            ItemKind::MemoryContext {
                status: MemoryContextRecordStatus::Loaded,
                references,
                packed_tokens: 7,
                ..
            } if references == &["mem-1"]
        ));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 7);
    }

    #[tokio::test]
    async fn retryable_verification_failure_returns_to_the_agent_loop() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let mut verification = VerificationRegistry::new();
        verification
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(CandidateVerifier { retryable: true }),
            )
            .expect("register verifier");
        let runtime = HarnessRuntime::new(
            Arc::new(RevisionModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state.clone(),
        )
        .with_verification(verification);
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "produce a candidate")
            .await
            .expect("revised candidate");

        assert_eq!(outcome.final_text, "good");
        assert_eq!(outcome.turn.status, TurnStatus::Completed);
        assert!(matches!(
            &outcome.turn.items[3].kind,
            ItemKind::VerificationResult {
                outcome: VerificationOutcome::Failed {
                    retryable: true,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            &outcome.turn.items[5].kind,
            ItemKind::VerificationResult {
                outcome: VerificationOutcome::Passed { .. },
                ..
            }
        ));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 9);
    }

    #[tokio::test]
    async fn steering_remains_open_across_a_retryable_verification_gate() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut verification = VerificationRegistry::new();
        verification
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(BlockingRetryVerifier {
                    calls: AtomicUsize::new(0),
                    entered: entered.clone(),
                    release: release.clone(),
                }),
            )
            .expect("register verifier");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(RevisionModel),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_verification(verification),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "produce a candidate").await })
        };
        entered.notified().await;
        let turn_id = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active")
            .expect("thread")
            .turns[0]
            .id
            .clone();
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "apply this before retrying",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("steering remains open during retryable verification");
        release.notify_one();

        let outcome = worker.await.expect("worker").expect("revised candidate");
        assert_eq!(outcome.final_text, "good");
        let queued = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::SteeringQueued { .. }))
            .expect("queue evidence");
        let applied = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::SteeringApplied { .. }))
            .expect("application evidence");
        assert!(queued < applied);
    }

    #[tokio::test]
    async fn non_retryable_verification_failure_fails_the_turn() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let mut verification = VerificationRegistry::new();
        verification
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(CandidateVerifier { retryable: false }),
            )
            .expect("register verifier");
        let runtime = HarnessRuntime::new(
            Arc::new(RevisionModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        )
        .with_verification(verification);
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn(&thread.id, "produce a candidate")
            .await
            .expect_err("hard verification failure");

        assert!(matches!(error, HarnessError::Verification(_)));
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(matches!(
            &projected.turns[0].items[3].kind,
            ItemKind::VerificationResult {
                outcome: VerificationOutcome::Failed {
                    retryable: false,
                    ..
                },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn verification_deadline_has_its_own_stop_phase() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let mut verification = VerificationRegistry::new();
        verification
            .register(CapabilityOrigin::BuiltIn, Arc::new(PendingVerifier))
            .expect("register verifier");
        let runtime = HarnessRuntime::new(
            Arc::new(RevisionModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        )
        .with_verification(verification);
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "verification timeout",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("verification should time out");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Verification
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: ExecutionPhase::Verification,
            })
        ));
    }

    #[tokio::test]
    async fn in_flight_cancellation_is_recorded_as_a_terminal_turn() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let entered = Arc::new(Notify::new());
        let runtime = HarnessRuntime::new(
            Arc::new(PendingModel {
                entered: entered.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let cancellation = CancellationToken::new();
        let cancel_from_task = cancellation.clone();
        let canceller = tokio::spawn(async move {
            entered.notified().await;
            cancel_from_task.cancel();
        });

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "cancel me",
                TurnExecutionOptions {
                    cancellation,
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("turn should be cancelled");
        canceller.await.expect("canceller");

        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Cancelled);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::TurnStopped {
                reason: TurnStopReason::Cancelled,
                phase: ExecutionPhase::Model,
            })
        ));
    }

    #[tokio::test]
    async fn model_panic_is_sanitized_and_durably_settled() {
        let runtime = HarnessRuntime::new(
            Arc::new(PanickingModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let error = runtime
            .run_turn(&thread.id, "panic safely")
            .await
            .expect_err("model panic");
        assert_eq!(
            error,
            HarnessError::CapabilityPanicked {
                phase: ExecutionPhase::Model
            }
        );

        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::RuntimeError { message })
                if message.contains("capability panicked during Model")
                    && !message.contains("sensitive")
        ));
    }

    #[tokio::test]
    async fn model_identity_is_panic_isolated_and_frozen_before_turn_state() {
        let runtime = HarnessRuntime::new(
            Arc::new(PanickingIdentityModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        assert_eq!(
            runtime
                .run_turn(&thread.id, "cannot start")
                .await
                .expect_err("identity panic"),
            HarnessError::CapabilityPanicked {
                phase: ExecutionPhase::Model
            }
        );
        assert!(
            runtime
                .load_thread(&thread.id)
                .await
                .expect("load")
                .expect("thread")
                .turns
                .is_empty()
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = HarnessRuntime::new(
            Arc::new(CountingIdentityModel {
                calls: calls.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("counting thread");
        runtime
            .run_turn(&thread.id, "freeze identity")
            .await
            .expect("counting Turn");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn deadline_is_recorded_as_a_distinct_terminal_turn() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(PendingModel {
                entered: Arc::new(Notify::new()),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "time out",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("turn should time out");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Model
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: ExecutionPhase::Model,
            })
        ));
    }

    #[tokio::test]
    async fn missing_stop_evidence_does_not_change_terminal_status() {
        let runtime = HarnessRuntime::new(
            Arc::new(PendingModel {
                entered: Arc::new(Notify::new()),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(RejectStopEvidenceStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "time out safely",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("turn should time out");
        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Model
            }
        );

        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        assert!(
            !projected.turns[0]
                .items
                .iter()
                .any(|item| matches!(&item.kind, ItemKind::TurnStopped { .. }))
        );
    }

    #[tokio::test]
    async fn tool_deadline_stops_instead_of_becoming_a_tool_error() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let mut tools = ToolRegistry::new();
        tools
            .register(CapabilityOrigin::BuiltIn, Arc::new(PendingTool))
            .expect("register tool");
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "time out in tool",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("tool should time out");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Tool
            }
        );
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: ExecutionPhase::Tool,
            })
        ));
        assert!(
            !projected.turns[0]
                .items
                .iter()
                .any(|item| matches!(&item.kind, ItemKind::ToolResult { .. }))
        );
    }

    #[tokio::test]
    async fn policy_provider_failure_settles_the_turn() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(ErrorPolicy),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn(&thread.id, "policy failure")
            .await
            .expect_err("policy should fail");

        assert_eq!(
            error,
            HarnessError::Policy("provider unavailable".to_owned())
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::RuntimeError { message })
                if message == "policy error: provider unavailable"
        ));
    }

    #[tokio::test]
    async fn asked_tool_executes_only_after_audited_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let decisions = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(Arc::new(ApproveAll {
            decisions: decisions.clone(),
        }));
        let thread = runtime.create_thread().await.expect("create thread");

        let outcome = runtime
            .run_turn(&thread.id, "approve me")
            .await
            .expect("approved turn");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(decisions.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.turn.status, TurnStatus::Completed);
        assert!(matches!(
            &outcome.turn.items[3].kind,
            ItemKind::PolicyDecision {
                tool_origin: Some(CapabilityOrigin::BuiltIn),
                decision: PolicyDecision::Ask {
                    risk: RiskLevel::High,
                    ..
                },
                ..
            }
        ));
        assert!(matches!(
            &outcome.turn.items[4].kind,
            ItemKind::ApprovalRequested {
                risk: RiskLevel::High,
                ..
            }
        ));
        assert!(matches!(
            &outcome.turn.items[5].kind,
            ItemKind::ApprovalDecision {
                decision: ApprovalDecision::Approve,
                ..
            }
        ));
        assert_eq!(state.events(&thread.id).await.expect("events").len(), 11);
    }

    #[tokio::test]
    async fn durable_approval_wait_resumes_after_worker_loss() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let handler = Arc::new(
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                .expect("approval handler"),
        );
        let first = Arc::new(
            HarnessRuntime::new(
                Arc::new(EchoModel),
                registry(calls.clone()),
                Arc::new(AskPolicy),
                state.clone(),
            )
            .with_approval_handler(handler.clone()),
        );
        let thread = first.create_thread().await.expect("create thread");
        let turn_context = TurnContextInput {
            source: "branch-handoff".to_owned(),
            reference: "thread:source/turn:terminal".to_owned(),
            text: "exact resumable handoff".to_owned(),
        };
        let execution_binding = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            1,
            None,
        )
        .expect("execution binding");
        let first_worker = {
            let runtime = first.clone();
            let thread_id = thread.id.clone();
            let turn_context = turn_context.clone();
            let execution_binding = execution_binding.clone();
            tokio::spawn(async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "resume me",
                        TurnExecutionOptions {
                            context: vec![turn_context],
                            execution_binding: Some(execution_binding),
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            })
        };
        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = inbox
                    .pending(1)
                    .await
                    .expect("pending approvals")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval submission");
        let turn_id = pending.request.authorization.turn_id.clone();
        first_worker.abort();
        first_worker.await.expect_err("simulated worker loss");

        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load running turn")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Running);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        inbox
            .settle(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                ApprovalActor::Authenticated {
                    authority: "test-operator".to_owned(),
                    subject: "approver".to_owned(),
                },
            )
            .await
            .expect("independent settlement");

        let binding_error = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler.clone())
        .resume_approval_turn_with_options(&thread.id, &turn_id, TurnExecutionOptions::default())
        .await
        .expect_err("missing execution binding must fail closed");
        assert!(binding_error.to_string().contains("execution binding"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let substituted_binding = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.1",
            "c".repeat(64),
            "b".repeat(64),
            2,
            None,
        )
        .expect("substituted binding");
        let substitution_error = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler.clone())
        .resume_approval_turn_with_options(
            &thread.id,
            &turn_id,
            TurnExecutionOptions {
                execution_binding: Some(substituted_binding),
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect_err("substituted execution binding must fail closed");
        assert!(substitution_error.to_string().contains("execution binding"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let actor_error = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler.clone())
        .resume_approval_turn_with_options(
            &thread.id,
            &turn_id,
            TurnExecutionOptions {
                authority: crate::AuthorityContext::new(
                    ApprovalActor::Authenticated {
                        authority: "different-authority".to_owned(),
                        subject: "different-requester".to_owned(),
                    },
                    None,
                )
                .expect("different authority"),
                context: vec![turn_context.clone()],
                execution_binding: Some(execution_binding.clone()),
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect_err("requester drift must fail closed");
        assert!(actor_error.to_string().contains("requester differs"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let context_error = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler.clone())
        .resume_approval_turn_with_options(
            &thread.id,
            &turn_id,
            TurnExecutionOptions {
                execution_binding: Some(execution_binding.clone()),
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect_err("missing invocation context must fail closed");
        assert!(context_error.to_string().contains("Model request changed"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut drifted_tools = ToolRegistry::new();
        drifted_tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(DriftedEchoTool {
                    calls: calls.clone(),
                }),
            )
            .expect("drifted Tool registration");
        let drifted = HarnessRuntime::new(
            Arc::new(EchoModel),
            drifted_tools,
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler.clone());
        let drift_error = drifted
            .resume_approval_turn_with_options(
                &thread.id,
                &turn_id,
                TurnExecutionOptions {
                    context: vec![turn_context.clone()],
                    execution_binding: Some(execution_binding.clone()),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("Tool descriptor drift must fail closed");
        assert!(drift_error.to_string().contains("Model request changed"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            state
                .load_thread(&thread.id)
                .await
                .expect("load after drift")
                .expect("thread")
                .turns[0]
                .status,
            TurnStatus::Running
        );

        let resumed = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler)
        .resume_approval_turn_with_options(
            &thread.id,
            &turn_id,
            TurnExecutionOptions {
                context: vec![turn_context],
                execution_binding: Some(execution_binding),
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect("resume approval boundary");

        assert_eq!(resumed.turn.id, turn_id);
        assert_eq!(resumed.turn.status, TurnStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(resumed.final_text.contains("resume me"));
        assert_eq!(
            resumed
                .turn
                .items
                .iter()
                .filter(|item| matches!(&item.kind, ItemKind::ApprovalDecision { .. }))
                .count(),
            1
        );
        assert_eq!(
            resumed
                .turn
                .items
                .iter()
                .filter(|item| matches!(&item.kind, ItemKind::ToolResult { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn durable_batch_approval_resume_executes_every_call_once_in_source_order() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let handler = Arc::new(
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                .expect("approval handler"),
        );
        let first = Arc::new(
            HarnessRuntime::new(
                Arc::new(BatchToolModel),
                registry(calls.clone()),
                Arc::new(BatchAskFirstPolicy),
                state.clone(),
            )
            .with_approval_handler(handler.clone()),
        );
        let thread = first.create_thread().await.expect("create thread");
        let worker = {
            let runtime = first.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "batch resume").await })
        };
        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = inbox
                    .pending(1)
                    .await
                    .expect("pending approvals")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("batch approval submission");
        let turn_id = pending.request.authorization.turn_id.clone();
        worker.abort();
        worker.await.expect_err("simulated worker loss");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        inbox
            .settle(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                ApprovalActor::Authenticated {
                    authority: "test-operator".to_owned(),
                    subject: "approver".to_owned(),
                },
            )
            .await
            .expect("settle batch approval");

        let resumed = HarnessRuntime::new(
            Arc::new(BatchToolModel),
            registry(calls.clone()),
            Arc::new(BatchAskFirstPolicy),
            state,
        )
        .with_approval_handler(handler)
        .resume_approval_turn_with_options(&thread.id, &turn_id, TurnExecutionOptions::default())
        .await
        .expect("resume batch approval");

        assert_eq!(resumed.final_text, "observed ordered batch");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let results = resumed
            .turn
            .items
            .iter()
            .filter_map(|item| match &item.kind {
                ItemKind::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results, ["batch-call-1", "batch-call-2"]);
        assert_eq!(
            resumed
                .turn
                .items
                .iter()
                .filter(|item| matches!(item.kind, ItemKind::ApprovalDecision { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn tenant_scoped_sqlite_approval_wait_resumes_after_store_reopen() {
        let state_path = sqlite_test_path("resume-state");
        let approval_path = sqlite_test_path("resume-approval");
        let calls = Arc::new(AtomicUsize::new(0));
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "requester".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        let first_state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&state_path)
                .await
                .expect("open first State store"),
        ));
        let first_inbox = Arc::new(
            SqliteApprovalInbox::open(&approval_path)
                .await
                .expect("open first Approval Inbox"),
        );
        let first_handler = Arc::new(
            InboxApprovalHandler::new(first_inbox.clone(), Duration::from_millis(10))
                .expect("first approval handler"),
        );
        let first = Arc::new(
            HarnessRuntime::new(
                Arc::new(EchoModel),
                registry(calls.clone()),
                Arc::new(AskPolicy),
                first_state.clone(),
            )
            .with_approval_handler(first_handler.clone()),
        );
        let thread = first
            .create_thread_as(&authority)
            .await
            .expect("create SQLite thread");
        let first_worker = tokio::spawn({
            let runtime = first.clone();
            let thread_id = thread.id.clone();
            let authority = authority.clone();
            async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "resume from SQLite",
                        TurnExecutionOptions {
                            authority,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });
        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = first_inbox
                    .pending_as(1, &authority)
                    .await
                    .expect("poll SQLite approvals")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SQLite approval submission");
        let turn_id = pending.request.authorization.turn_id.clone();
        first_worker.abort();
        first_worker.await.expect_err("simulated process loss");
        drop(first);
        drop(first_handler);
        drop(first_state);
        drop(first_inbox);

        let reopened_inbox = Arc::new(
            SqliteApprovalInbox::open(&approval_path)
                .await
                .expect("reopen Approval Inbox"),
        );
        let approver = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "sqlite-approver".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant approver");
        reopened_inbox
            .settle_as(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                &approver,
            )
            .await
            .expect("settle reopened approval");
        let reopened_state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&state_path)
                .await
                .expect("reopen State store"),
        ));
        let resumed = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            reopened_state.clone(),
        )
        .with_approval_handler(Arc::new(
            InboxApprovalHandler::new(reopened_inbox.clone(), Duration::from_millis(10))
                .expect("reopened approval handler"),
        ))
        .resume_approval_turn_with_options(
            &thread.id,
            &turn_id,
            TurnExecutionOptions {
                authority,
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect("resume from reopened SQLite stores");

        assert_eq!(resumed.turn.status, TurnStatus::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(reopened_state);
        drop(reopened_inbox);
        remove_sqlite_files(&state_path);
        remove_sqlite_files(&approval_path);
    }

    #[tokio::test]
    async fn resume_refuses_post_approval_unknown_tool_boundary() {
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let handler = Arc::new(
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                .expect("approval handler"),
        );
        let mut first_tools = ToolRegistry::new();
        first_tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(PendingCountingTool {
                    calls: calls.clone(),
                    entered: entered.clone(),
                }),
            )
            .expect("register pending Tool");
        let first = Arc::new(
            HarnessRuntime::new(
                Arc::new(EchoModel),
                first_tools,
                Arc::new(AskPolicy),
                state.clone(),
            )
            .with_approval_handler(handler.clone()),
        );
        let thread = first.create_thread().await.expect("create thread");
        let first_worker = tokio::spawn({
            let runtime = first.clone();
            let thread_id = thread.id.clone();
            async move { runtime.run_turn(&thread_id, "do not replay").await }
        });
        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = inbox
                    .pending(1)
                    .await
                    .expect("poll approval")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval request");
        inbox
            .settle(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                ApprovalActor::Authenticated {
                    authority: "test-operator".to_owned(),
                    subject: "approver".to_owned(),
                },
            )
            .await
            .expect("settle approval");
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("Tool entry");
        first_worker.abort();
        first_worker.await.expect_err("simulated loss in Tool");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let projected = state
            .load_thread(&thread.id)
            .await
            .expect("load uncertain boundary")
            .expect("thread");
        let turn_id = projected.turns[0].id.clone();
        assert_eq!(projected.turns[0].status, TurnStatus::Running);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::ApprovalDecision {
                decision: ApprovalDecision::Approve,
                ..
            })
        ));

        let mut resumed_tools = ToolRegistry::new();
        resumed_tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(PendingCountingTool {
                    calls: calls.clone(),
                    entered,
                }),
            )
            .expect("register recovery Tool");
        let error = HarnessRuntime::new(
            Arc::new(EchoModel),
            resumed_tools,
            Arc::new(AskPolicy),
            state.clone(),
        )
        .with_approval_handler(handler)
        .resume_approval_turn_with_options(&thread.id, &turn_id, TurnExecutionOptions::default())
        .await
        .expect_err("unknown Tool boundary must not replay");
        assert!(
            error
                .to_string()
                .contains("not paused at an approval request")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            state
                .load_thread(&thread.id)
                .await
                .expect("reload uncertain boundary")
                .expect("thread")
                .turns[0]
                .status,
            TurnStatus::Running
        );
    }

    #[tokio::test]
    async fn ask_is_denied_when_no_approval_handler_is_installed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "requester".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state,
        );
        let thread = runtime
            .create_thread_as(&authority)
            .await
            .expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "default deny",
                TurnExecutionOptions {
                    authority: authority.clone(),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("ask must not auto-approve");

        assert!(matches!(error, HarnessError::ApprovalDenied { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let projected = runtime
            .load_thread_as(&thread.id, &authority)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(matches!(
            &projected.turns[0].items[4].kind,
            ItemKind::ApprovalRequested { .. }
        ));
        assert!(matches!(
            &projected.turns[0].items[5].kind,
            ItemKind::ApprovalDecision {
                decision: ApprovalDecision::Deny { .. },
                ..
            }
        ));
    }

    #[tokio::test]
    async fn tenant_recovery_orphans_only_its_durable_approval() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "requester".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(EchoModel),
                registry(calls.clone()),
                Arc::new(AskPolicy),
                state.clone(),
            )
            .with_approval_handler(Arc::new(
                InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                    .expect("inbox handler"),
            )),
        );
        let thread = runtime
            .create_thread_as(&authority)
            .await
            .expect("create thread");
        let task = tokio::spawn({
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            let authority = authority.clone();
            async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "wait durably",
                        TurnExecutionOptions {
                            authority,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });

        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = inbox
                    .pending_as(1, &authority)
                    .await
                    .expect("poll durable approval")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval request timeout");
        task.abort();
        assert!(task.await.expect_err("aborted runtime").is_cancelled());

        let recovered_runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state,
        )
        .with_approval_handler(Arc::new(
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                .expect("recovery handler"),
        ));
        let recovered = recovered_runtime
            .recover_thread_as(
                &thread.id,
                &pending.request.authorization.turn_id,
                &authority,
            )
            .await
            .expect("recover")
            .expect("thread");
        assert_eq!(recovered.turns[0].status, TurnStatus::Interrupted);
        let record = inbox
            .get_as(&pending.request.id, &authority)
            .await
            .expect("read orphan")
            .expect("approval");
        assert!(matches!(
            record.status,
            ApprovalRecordStatus::Orphaned { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn normal_execution_never_interrupts_another_runtime_owner() {
        let store = Arc::new(MemoryEventStore::new());
        let entered = Arc::new(Notify::new());
        let first = Arc::new(HarnessRuntime::new(
            Arc::new(PendingModel {
                entered: entered.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(store.clone()),
        ));
        let second = HarnessRuntime::new(
            Arc::new(RevisionModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(store),
        );
        let thread = first.create_thread().await.expect("create thread");
        let cancellation = CancellationToken::new();
        let running = tokio::spawn({
            let first = first.clone();
            let thread_id = thread.id.clone();
            let cancellation = cancellation.clone();
            async move {
                first
                    .run_turn_with_options(
                        &thread_id,
                        "first owner",
                        TurnExecutionOptions {
                            cancellation,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("first model entered");

        let error = second
            .run_turn(&thread.id, "competing owner")
            .await
            .expect_err("a live Turn must not be recovered implicitly");
        assert!(
            matches!(&error, HarnessError::State(message) if message.contains("already has a running turn"))
        );
        let projected = second
            .load_thread(&thread.id)
            .await
            .expect("load contested thread")
            .expect("thread");
        assert_eq!(projected.turns.len(), 1);
        assert_eq!(projected.turns[0].status, TurnStatus::Running);

        cancellation.cancel();
        assert_eq!(
            running
                .await
                .expect("first task")
                .expect_err("first Turn cancelled"),
            HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            }
        );
        let settled = second
            .load_thread(&thread.id)
            .await
            .expect("load settled thread")
            .expect("thread");
        assert_eq!(settled.turns.len(), 1);
        assert_eq!(settled.turns[0].status, TurnStatus::Cancelled);
    }

    #[tokio::test]
    async fn runtime_concurrency_admission_is_bounded_before_state_mutation() {
        let entered = Arc::new(Notify::new());
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(PendingModel {
                    entered: entered.clone(),
                }),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_turn_concurrency_limit(1)
            .expect("concurrency limit"),
        );
        let first_thread = runtime.create_thread().await.expect("first thread");
        let second_thread = runtime.create_thread().await.expect("second thread");
        let cancellation = CancellationToken::new();
        let running = tokio::spawn({
            let runtime = runtime.clone();
            let thread_id = first_thread.id.clone();
            let cancellation = cancellation.clone();
            async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "hold admission",
                        TurnExecutionOptions {
                            cancellation,
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(1), entered.notified())
            .await
            .expect("first model entered");

        assert_eq!(
            runtime
                .run_turn(&second_thread.id, "must shed")
                .await
                .expect_err("second Turn must be rejected"),
            HarnessError::RuntimeOverloaded { limit: 1 }
        );
        let untouched = runtime
            .load_thread(&second_thread.id)
            .await
            .expect("load second thread")
            .expect("second thread");
        assert!(untouched.turns.is_empty());

        cancellation.cancel();
        assert_eq!(
            running
                .await
                .expect("first task")
                .expect_err("first Turn cancellation"),
            HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            }
        );
        let stopped = CancellationToken::new();
        stopped.cancel();
        assert_eq!(
            runtime
                .run_turn_with_options(
                    &second_thread.id,
                    "admitted after release",
                    TurnExecutionOptions {
                        cancellation: stopped,
                        ..TurnExecutionOptions::default()
                    },
                )
                .await
                .expect_err("pre-cancelled Turn"),
            HarnessError::Cancelled {
                phase: ExecutionPhase::Context
            }
        );
    }

    #[tokio::test]
    async fn approval_deadline_has_its_own_stop_phase() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            registry(calls.clone()),
            Arc::new(AskPolicy),
            state,
        )
        .with_approval_handler(Arc::new(PendingApproval));
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "approval timeout",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("approval should time out");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Approval
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::TimedOut);
        assert!(matches!(
            projected.turns[0].items.last().map(|item| &item.kind),
            Some(ItemKind::TurnStopped {
                reason: TurnStopReason::TimedOut,
                phase: ExecutionPhase::Approval,
            })
        ));
    }

    #[tokio::test]
    async fn oversized_model_output_is_rejected_before_state_growth() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(OversizedModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let error = runtime
            .run_turn(&thread.id, "bounded")
            .await
            .expect_err("oversized output must fail");
        assert!(matches!(error, HarnessError::Model(_)));
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert_eq!(projected.turns[0].items.len(), 3);
    }

    #[tokio::test]
    async fn model_usage_and_phase_latency_reach_observability_only() {
        let collector = Arc::new(TraceCollector::new(8).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("register observer");
        let runtime = HarnessRuntime::new(
            Arc::new(UsageModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("create thread");
        runtime
            .run_turn(&thread.id, "usage")
            .await
            .expect("observed turn");

        let records = collector.snapshot();
        assert!(records.iter().any(|record| {
            record.phase == ExecutionPhase::Context && record.outcome == ObservationOutcome::Success
        }));
        let model = records
            .iter()
            .find(|record| record.phase == ExecutionPhase::Model)
            .expect("model observation");
        assert_eq!(model.capability, "test/usage-model");
        assert_eq!(model.provider_model.as_deref(), Some("provider/settled-v2"));
        assert_eq!(
            model.provider_request_id.as_deref(),
            Some("provider-request")
        );
        assert_eq!(
            model
                .model_usage
                .as_ref()
                .and_then(|usage| usage.cost_usd_ticks),
            Some(250_000)
        );
    }

    #[tokio::test]
    async fn typed_provider_failure_reaches_trace_without_diagnostic_content() {
        let collector = Arc::new(TraceCollector::new(8).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("register observer");
        let runtime = HarnessRuntime::new(
            Arc::new(TypedProviderFailureModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("create thread");

        let error = runtime
            .run_turn(&thread.id, "typed failure")
            .await
            .expect_err("provider must fail");

        let HarnessError::ModelProvider(failure) = error else {
            panic!("expected typed Provider failure");
        };
        assert_eq!(failure.kind(), ModelProviderFailureKind::RateLimited);
        let model = collector
            .snapshot()
            .into_iter()
            .find(|record| record.phase == ExecutionPhase::Model)
            .expect("model observation");
        assert_eq!(model.outcome, ObservationOutcome::Error);
        assert_eq!(
            model.provider_failure_kind,
            Some(ModelProviderFailureKind::RateLimited)
        );
        assert_eq!(model.provider_status_code, Some(429));
        assert_eq!(model.provider_retry_after_ms, Some(2_000));
        assert!(model.provider_model.is_none());
        assert!(model.provider_request_id.is_none());
    }

    #[test]
    fn provider_model_evidence_is_bounded_before_observation() {
        let mut response = ModelResponse::from(ModelOutput::Message {
            content: "unused".to_owned(),
        });
        response.provider_model = Some("x".repeat(MAX_PROVIDER_EVIDENCE_ID_BYTES + 1));

        let error = validate_model_response(&response).expect_err("oversized provider model");
        assert!(error.to_string().contains("provider model"));
    }

    #[test]
    fn model_tool_call_batch_rejects_duplicate_correlations() {
        let calls = vec![
            ModelToolCall {
                call_id: "duplicate".to_owned(),
                name: "echo".to_owned(),
                input: json!({"text": "first"}),
            },
            ModelToolCall {
                call_id: "duplicate".to_owned(),
                name: "echo".to_owned(),
                input: json!({"text": "second"}),
            },
        ];

        let error = validate_model_tool_calls(&calls).expect_err("duplicate correlation");
        assert!(error.to_string().contains("reused correlation"));
    }

    #[tokio::test]
    async fn registry_selected_model_origin_reaches_authoritative_state() {
        let origin = CapabilityOrigin::External {
            id: "test-provider".to_owned(),
        };
        let mut models = ModelRegistry::new();
        models
            .register(origin.clone(), Arc::new(UsageModel))
            .expect("register model");
        let runtime = HarnessRuntime::from_model_registry(
            &models,
            "test/usage-model",
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("select registered model");
        let thread = runtime.create_thread().await.expect("create thread");
        let outcome = runtime
            .run_turn(&thread.id, "provenance")
            .await
            .expect("run turn");

        assert!(matches!(
            &outcome.turn.items[2].kind,
            ItemKind::AssistantMessage {
                model_id,
                model_origin,
                content,
            } if model_id.as_deref() == Some("test/usage-model")
                && model_origin.as_ref() == Some(&origin)
                && content == "observed"
        ));
    }

    #[test]
    fn model_failover_route_rejects_empty_duplicate_and_unknown_identities() {
        let mut models = ModelRegistry::new();
        models
            .register(CapabilityOrigin::BuiltIn, Arc::new(UsageModel))
            .expect("register model");

        assert!(matches!(
            HarnessRuntime::from_model_registry_failover(
                &models,
                &[],
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            ),
            Err(HarnessError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            HarnessRuntime::from_model_registry_failover(
                &models,
                &["test/usage-model", "test/usage-model"],
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            ),
            Err(HarnessError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            HarnessRuntime::from_model_registry_failover(
                &models,
                &["test/missing-model"],
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            ),
            Err(HarnessError::UnknownModel(id)) if id == "test/missing-model"
        ));
        let runtime = HarnessRuntime::from_model_registry(
            &models,
            "test/usage-model",
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("single Model route");
        assert!(matches!(
            runtime.with_model_attempt_timeout(Duration::ZERO),
            Err(HarnessError::InvalidConfiguration(_))
        ));
        let single = HarnessRuntime::from_model_registry(
            &models,
            "test/usage-model",
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("single Model route");
        assert!(matches!(
            single.with_model_timeout_cooldown(Duration::from_secs(1)),
            Err(HarnessError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn model_timeout_cooldown_expires_and_never_removes_the_complete_route() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteFailingModel {
                    calls: primary_calls,
                    emit_delta: false,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls,
                }),
            )
            .expect("secondary");
        for cooldown in [
            Duration::ZERO,
            super::MAX_MODEL_TIMEOUT_COOLDOWN + Duration::from_millis(1),
        ] {
            let runtime = HarnessRuntime::from_model_registry_failover(
                &models,
                &["test/route-primary", "test/route-secondary"],
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .expect("route");
            assert!(matches!(
                runtime.with_model_timeout_cooldown(cooldown),
                Err(HarnessError::InvalidConfiguration(_))
            ));
        }
        let mut route =
            ModelRoute::from_registry(&models, &["test/route-primary", "test/route-secondary"])
                .expect("route");
        route.timeout_cooldown = Some(Duration::from_secs(1));
        let now = Instant::now();
        route
            .record_attempt_timeout("test/route-primary", now)
            .expect("record timeout");

        let (available, cooling) = route
            .partition_timeout_cooldown(route.entries.iter().collect(), now)
            .expect("partition");
        assert_eq!(
            available[0].identity.get().expect("identity"),
            "test/route-secondary"
        );
        assert_eq!(
            cooling[0].identity.get().expect("identity"),
            "test/route-primary"
        );

        let (available, cooling) = route
            .partition_timeout_cooldown(
                route.entries.iter().collect(),
                now + Duration::from_secs(1),
            )
            .expect("expired partition");
        assert_eq!(available.len(), 2);
        assert!(cooling.is_empty());

        route
            .record_attempt_timeout("test/route-primary", now)
            .expect("record primary timeout");
        route
            .record_attempt_timeout("test/route-secondary", now)
            .expect("record secondary timeout");
        let (available, cooling) = route
            .partition_timeout_cooldown(route.entries.iter().collect(), now)
            .expect("all cooling partition");
        assert_eq!(available.len(), 2);
        assert!(cooling.is_empty());
    }

    #[tokio::test]
    async fn model_failover_records_each_attempt_and_settled_provenance() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let primary_origin = CapabilityOrigin::External {
            id: "primary-provider".to_owned(),
        };
        let secondary_origin = CapabilityOrigin::TrustedExtension {
            id: "secondary-provider".to_owned(),
        };
        let mut models = ModelRegistry::new();
        models
            .register(
                primary_origin,
                Arc::new(RouteFailingModel {
                    calls: primary_calls.clone(),
                    emit_delta: false,
                }),
            )
            .expect("register primary");
        models
            .register(
                secondary_origin.clone(),
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("register secondary");
        let collector = Arc::new(TraceCollector::new(16).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("observer");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("failover route")
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("thread");
        let outcome = runtime
            .run_turn(&thread.id, "route")
            .await
            .expect("secondary settles");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 1);
        assert!(outcome.turn.items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::AssistantMessage {
                    model_id,
                    model_origin,
                    content,
                } if model_id.as_deref() == Some("test/route-secondary")
                    && model_origin.as_ref() == Some(&secondary_origin)
                    && content == "secondary result"
            )
        }));
        let attempts = collector
            .snapshot()
            .into_iter()
            .filter(|record| record.phase == ExecutionPhase::Model)
            .map(|record| (record.capability, record.outcome))
            .collect::<Vec<_>>();
        assert_eq!(
            attempts,
            [
                ("test/route-primary".to_owned(), ObservationOutcome::Error),
                (
                    "test/route-secondary".to_owned(),
                    ObservationOutcome::Success
                ),
            ]
        );
    }

    #[tokio::test]
    async fn model_failover_stops_after_delivered_provisional_output() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "primary-provider".to_owned(),
                },
                Arc::new(RouteFailingModel {
                    calls: primary_calls.clone(),
                    emit_delta: true,
                }),
            )
            .expect("register primary");
        models
            .register(
                CapabilityOrigin::External {
                    id: "secondary-provider".to_owned(),
                },
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("register secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("failover route");
        let sink = Arc::new(RecordingModelSink::default());
        let thread = runtime.create_thread().await.expect("thread");
        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "streamed route",
                TurnExecutionOptions {
                    model_event_sink: Some(sink.clone()),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("delivered output must suppress failover");

        assert!(error.to_string().contains("failover was suppressed"));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sink.events.lock().expect("events").as_slice(),
            [ModelStreamEvent::TextDelta {
                model_step: 1,
                delta: "primary fragment".to_owned(),
            }]
        );
    }

    #[tokio::test]
    async fn model_attempt_timeout_cancels_primary_and_reaches_secondary() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let cancellation_observed = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "pending-provider".to_owned(),
                },
                Arc::new(CancellablePendingModel {
                    calls: primary_calls,
                    cancellation_observed: cancellation_observed.clone(),
                }),
            )
            .expect("register primary");
        models
            .register(
                CapabilityOrigin::External {
                    id: "secondary-provider".to_owned(),
                },
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("register secondary");
        let collector = Arc::new(TraceCollector::new(16).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("observer");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/cancellable-pending-model", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("failover route")
        .with_model_attempt_timeout(Duration::from_millis(5))
        .expect("attempt timeout")
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("thread");
        let outcome = runtime
            .run_turn(&thread.id, "timeout failover")
            .await
            .expect("secondary settles");
        tokio::time::timeout(Duration::from_secs(1), async {
            while cancellation_observed.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("attempt cancellation");

        assert_eq!(secondary_calls.load(Ordering::SeqCst), 1);
        assert!(outcome.turn.items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::AssistantMessage { model_id, .. }
                    if model_id.as_deref() == Some("test/route-secondary")
            )
        }));
        let attempts = collector
            .snapshot()
            .into_iter()
            .filter(|record| record.phase == ExecutionPhase::Model)
            .map(|record| (record.capability, record.outcome))
            .collect::<Vec<_>>();
        assert_eq!(
            attempts,
            [
                (
                    "test/cancellable-pending-model".to_owned(),
                    ObservationOutcome::TimedOut
                ),
                (
                    "test/route-secondary".to_owned(),
                    ObservationOutcome::Success
                ),
            ]
        );
    }

    #[tokio::test]
    async fn model_timeout_cooldown_skips_repeated_wait_but_keeps_trace_evidence() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "pending-provider".to_owned(),
                },
                Arc::new(CancellablePendingModel {
                    calls: primary_calls.clone(),
                    cancellation_observed: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::External {
                    id: "secondary-provider".to_owned(),
                },
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let collector = Arc::new(TraceCollector::new(16).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("observer");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/cancellable-pending-model", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_attempt_timeout(Duration::from_millis(5))
        .expect("attempt timeout")
        .with_model_timeout_cooldown(Duration::from_secs(1))
        .expect("timeout cooldown")
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("thread");

        runtime
            .run_turn(&thread.id, "first")
            .await
            .expect("first fallback");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("cooled fallback");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);
        let attempts = collector
            .snapshot()
            .into_iter()
            .filter(|record| record.phase == ExecutionPhase::Model)
            .map(|record| (record.capability, record.outcome))
            .collect::<Vec<_>>();
        assert_eq!(
            attempts,
            [
                (
                    "test/cancellable-pending-model".to_owned(),
                    ObservationOutcome::TimedOut
                ),
                (
                    "test/route-secondary".to_owned(),
                    ObservationOutcome::Success
                ),
                (
                    "test/route-secondary".to_owned(),
                    ObservationOutcome::Success
                ),
                (
                    "test/cancellable-pending-model".to_owned(),
                    ObservationOutcome::Skipped
                ),
            ]
        );
    }

    #[tokio::test]
    async fn model_timeout_cooldown_fails_open_after_ready_candidates_fail() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(CancellablePendingModel {
                    calls: primary_calls.clone(),
                    cancellation_observed: Arc::new(AtomicUsize::new(0)),
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(SuccessThenFailureModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/cancellable-pending-model", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_attempt_timeout(Duration::from_millis(5))
        .expect("attempt timeout")
        .with_model_timeout_cooldown(Duration::from_secs(1))
        .expect("timeout cooldown");
        let thread = runtime.create_thread().await.expect("thread");

        runtime
            .run_turn(&thread.id, "first")
            .await
            .expect("first fallback");
        let error = runtime
            .run_turn(&thread.id, "second")
            .await
            .expect_err("ready and cooling candidates fail");

        assert!(matches!(error, HarnessError::Model(_)));
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            primary_calls.load(Ordering::SeqCst),
            2,
            "cooling primary remains the last-resort candidate"
        );
    }

    #[tokio::test]
    async fn ordinary_model_failure_does_not_open_timeout_cooldown() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteFailingModel {
                    calls: primary_calls.clone(),
                    emit_delta: false,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_timeout_cooldown(Duration::from_secs(1))
        .expect("timeout cooldown");
        let thread = runtime.create_thread().await.expect("thread");

        runtime.run_turn(&thread.id, "first").await.expect("first");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn typed_provider_failure_does_not_open_timeout_cooldown() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TypedRouteFailingModel {
                    calls: primary_calls.clone(),
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/typed-route-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_timeout_cooldown(Duration::from_secs(1))
        .expect("timeout cooldown");
        let thread = runtime.create_thread().await.expect("thread");

        runtime.run_turn(&thread.id, "first").await.expect("first");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn model_retry_policy_rejects_unbounded_or_inverted_delays() {
        let policy =
            ModelRetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(100))
                .expect("retry policy");
        assert_eq!(policy.max_retries(), 2);
        assert_eq!(policy.initial_delay(), Duration::from_millis(10));
        assert_eq!(policy.max_delay(), Duration::from_millis(100));
        assert!(
            ModelRetryPolicy::new(0, Duration::from_millis(1), Duration::from_millis(1)).is_err()
        );
        assert!(
            ModelRetryPolicy::new(
                super::MAX_MODEL_RETRIES + 1,
                Duration::from_millis(1),
                Duration::from_millis(1)
            )
            .is_err()
        );
        assert!(ModelRetryPolicy::new(1, Duration::ZERO, Duration::from_millis(1)).is_err());
        assert!(
            ModelRetryPolicy::new(1, Duration::from_millis(2), Duration::from_millis(1)).is_err()
        );
    }

    #[test]
    fn model_attempt_budget_is_bounded_and_exposes_the_turn_ceiling() {
        let runtime = HarnessRuntime::new(
            Arc::new(UsageModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_max_model_attempts_per_step(3)
        .expect("bounded attempt budget")
        .with_max_steps(4);

        assert_eq!(runtime.model_attempts_per_turn_bound(), 12);
        assert!(matches!(
            HarnessRuntime::new(
                Arc::new(UsageModel),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_max_model_attempts_per_step(0),
            Err(HarnessError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            HarnessRuntime::new(
                Arc::new(UsageModel),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_max_model_attempts_per_step(super::MAX_MODEL_ATTEMPTS_PER_STEP + 1),
            Err(HarnessError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn model_attempt_budget_stops_retry_before_an_extra_provider_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = HarnessRuntime::new(
            Arc::new(TypedRetryModel {
                id: "test/retry-budget",
                calls: calls.clone(),
                kind: ModelProviderFailureKind::Transport,
                retry_after_ms: None,
                succeeds_on_call: None,
                emit_before_failure: false,
                failure_observed: None,
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_model_retry_policy(
            ModelRetryPolicy::new(8, Duration::from_millis(1), Duration::from_millis(1))
                .expect("retry policy"),
        )
        .with_max_model_attempts_per_step(2)
        .expect("attempt budget");
        let thread = runtime.create_thread().await.expect("thread");

        let error = runtime
            .run_turn(&thread.id, "bounded retry")
            .await
            .expect_err("attempt budget must stop retry");

        assert_eq!(error, HarnessError::MaxModelAttempts(2));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn model_attempt_budget_stops_failover_before_an_extra_provider_call() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteFailingModel {
                    calls: primary_calls.clone(),
                    emit_delta: false,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_max_model_attempts_per_step(1)
        .expect("attempt budget");
        let thread = runtime.create_thread().await.expect("thread");

        let error = runtime
            .run_turn(&thread.id, "bounded failover")
            .await
            .expect_err("attempt budget must stop failover");

        assert_eq!(error, HarnessError::MaxModelAttempts(1));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn model_attempt_budget_resets_for_each_agent_loop_step() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = runtime(
            calls.clone(),
            AllowListPolicy::deny_by_default().allow("echo"),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_max_model_attempts_per_step(1)
        .expect("one attempt per step");
        let thread = runtime.create_thread().await.expect("thread");

        let outcome = runtime
            .run_turn(&thread.id, "two model steps")
            .await
            .expect("each step gets its own attempt");

        assert_eq!(
            outcome.final_text,
            r#"observed: {"text":"two model steps"}"#
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retryable_provider_failure_retries_same_model_with_trace_indices() {
        let calls = Arc::new(AtomicUsize::new(0));
        let collector = Arc::new(TraceCollector::new(8).expect("collector"));
        let mut observability = Observability::new();
        observability
            .register("collector", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("observer");
        let runtime = HarnessRuntime::new(
            Arc::new(TypedRetryModel {
                id: "test/retry-success",
                calls: calls.clone(),
                kind: ModelProviderFailureKind::Transport,
                retry_after_ms: None,
                succeeds_on_call: Some(2),
                emit_before_failure: false,
                failure_observed: None,
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(1))
                .expect("retry policy"),
        )
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("thread");

        let outcome = runtime
            .run_turn(&thread.id, "retry")
            .await
            .expect("retry settles");

        assert_eq!(outcome.final_text, "retry settled");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let attempts = collector
            .snapshot()
            .into_iter()
            .filter(|record| record.phase == ExecutionPhase::Model)
            .map(|record| {
                (
                    record.model_retry_index,
                    record.outcome,
                    record.provider_failure_kind,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            attempts,
            [
                (
                    Some(0),
                    ObservationOutcome::Error,
                    Some(ModelProviderFailureKind::Transport)
                ),
                (Some(1), ObservationOutcome::Success, None),
            ]
        );
    }

    #[tokio::test]
    async fn model_retry_exhaustion_is_exact_and_returns_typed_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let runtime = HarnessRuntime::new(
            Arc::new(TypedRetryModel {
                id: "test/retry-exhausted",
                calls: calls.clone(),
                kind: ModelProviderFailureKind::Server,
                retry_after_ms: None,
                succeeds_on_call: None,
                emit_before_failure: false,
                failure_observed: None,
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(1))
                .expect("retry policy"),
        );
        let thread = runtime.create_thread().await.expect("thread");

        let error = runtime
            .run_turn(&thread.id, "exhaust")
            .await
            .expect_err("retry exhaustion");

        assert!(matches!(error, HarnessError::ModelProvider(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_retryable_provider_failure_falls_through_without_same_model_retry() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TypedRetryModel {
                    id: "test/non-retryable-primary",
                    calls: primary_calls.clone(),
                    kind: ModelProviderFailureKind::Authentication,
                    retry_after_ms: Some(1),
                    succeeds_on_call: None,
                    emit_before_failure: false,
                    failure_observed: None,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/non-retryable-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(10))
                .expect("retry policy"),
        );
        let thread = runtime.create_thread().await.expect("thread");

        runtime
            .run_turn(&thread.id, "fallback")
            .await
            .expect("secondary settles");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_backoff_that_cannot_fit_candidate_budget_yields_without_cooldown() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TypedRetryModel {
                    id: "test/retry-budget-primary",
                    calls: primary_calls.clone(),
                    kind: ModelProviderFailureKind::RateLimited,
                    retry_after_ms: Some(20),
                    succeeds_on_call: None,
                    emit_before_failure: false,
                    failure_observed: None,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/retry-budget-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_attempt_timeout(Duration::from_millis(5))
        .expect("attempt timeout")
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(100))
                .expect("retry policy"),
        )
        .with_model_timeout_cooldown(Duration::from_secs(1))
        .expect("timeout cooldown");
        let thread = runtime.create_thread().await.expect("thread");

        runtime.run_turn(&thread.id, "first").await.expect("first");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn provider_retry_hint_above_policy_max_is_not_shortened() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TypedRetryModel {
                    id: "test/retry-hint-primary",
                    calls: primary_calls.clone(),
                    kind: ModelProviderFailureKind::RateLimited,
                    retry_after_ms: Some(20),
                    succeeds_on_call: Some(2),
                    emit_before_failure: false,
                    failure_observed: None,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/retry-hint-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(10))
                .expect("retry policy"),
        );
        let thread = runtime.create_thread().await.expect("thread");

        runtime
            .run_turn(&thread.id, "do not shorten")
            .await
            .expect("secondary settles");

        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn provisional_output_suppresses_typed_retry_and_route_failover() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(TypedRetryModel {
                    id: "test/retry-streaming-primary",
                    calls: primary_calls.clone(),
                    kind: ModelProviderFailureKind::Transport,
                    retry_after_ms: None,
                    succeeds_on_call: Some(2),
                    emit_before_failure: true,
                    failure_observed: None,
                }),
            )
            .expect("primary");
        models
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/retry-streaming-primary", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("route")
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(1))
                .expect("retry policy"),
        );
        let thread = runtime.create_thread().await.expect("thread");

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "stream",
                TurnExecutionOptions {
                    model_event_sink: Some(Arc::new(RecordingModelSink::default())),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("provisional failure");

        assert!(error.to_string().contains("failover was suppressed"));
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancellation_interrupts_retry_backoff_before_another_provider_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(Notify::new());
        let cancellation = CancellationToken::new();
        let runtime = HarnessRuntime::new(
            Arc::new(TypedRetryModel {
                id: "test/retry-cancelled",
                calls: calls.clone(),
                kind: ModelProviderFailureKind::RateLimited,
                retry_after_ms: Some(50),
                succeeds_on_call: Some(2),
                emit_before_failure: false,
                failure_observed: Some(observed.clone()),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_model_retry_policy(
            ModelRetryPolicy::new(2, Duration::from_millis(1), Duration::from_millis(100))
                .expect("retry policy"),
        );
        let thread = runtime.create_thread().await.expect("thread");
        let thread_id = thread.id.clone();
        let run_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            runtime
                .run_turn_with_options(
                    &thread_id,
                    "cancel retry",
                    TurnExecutionOptions {
                        cancellation: run_cancellation,
                        ..TurnExecutionOptions::default()
                    },
                )
                .await
        });

        observed.notified().await;
        cancellation.cancel();
        let error = task
            .await
            .expect("runtime task")
            .expect_err("cancelled retry");

        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn model_failover_never_crosses_the_turn_deadline() {
        let secondary_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "pending-provider".to_owned(),
                },
                Arc::new(PendingModel {
                    entered: Arc::new(Notify::new()),
                }),
            )
            .expect("register primary");
        models
            .register(
                CapabilityOrigin::External {
                    id: "secondary-provider".to_owned(),
                },
                Arc::new(RouteSuccessModel {
                    calls: secondary_calls.clone(),
                }),
            )
            .expect("register secondary");
        let runtime = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/pending-model", "test/route-secondary"],
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .expect("failover route");
        let thread = runtime.create_thread().await.expect("thread");
        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "deadline",
                TurnExecutionOptions {
                    timeout: Some(Duration::from_millis(5)),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("Turn deadline must stop the route");

        assert_eq!(
            error,
            HarnessError::TimedOut {
                phase: ExecutionPhase::Model
            }
        );
        assert_eq!(secondary_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn approval_resume_requires_the_recorded_failover_model_in_the_route() {
        let primary_calls = Arc::new(AtomicUsize::new(0));
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut models = ModelRegistry::new();
        models
            .register(
                CapabilityOrigin::External {
                    id: "primary-provider".to_owned(),
                },
                Arc::new(RouteFailingModel {
                    calls: primary_calls.clone(),
                    emit_delta: false,
                }),
            )
            .expect("register primary");
        models
            .register(
                CapabilityOrigin::TrustedExtension {
                    id: "echo-provider".to_owned(),
                },
                Arc::new(EchoModel),
            )
            .expect("register secondary");
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let handler = Arc::new(
            InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
                .expect("approval handler"),
        );
        let first = Arc::new(
            HarnessRuntime::from_model_registry_failover(
                &models,
                &["test/route-primary", "test/echo-model"],
                registry(tool_calls.clone()),
                Arc::new(AskPolicy),
                state.clone(),
            )
            .expect("failover route")
            .with_approval_handler(handler.clone()),
        );
        let thread = first.create_thread().await.expect("thread");
        let first_worker = tokio::spawn({
            let runtime = first.clone();
            let thread_id = thread.id.clone();
            async move { runtime.run_turn(&thread_id, "route approval").await }
        });
        let pending = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(record) = inbox
                    .pending(1)
                    .await
                    .expect("pending approvals")
                    .into_iter()
                    .next()
                {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval request");
        let turn_id = pending.request.authorization.turn_id.clone();
        first_worker.abort();
        first_worker.await.expect_err("simulated worker loss");
        inbox
            .settle(
                &pending.request.id,
                pending.revision,
                ApprovalDecision::Approve,
                ApprovalActor::Authenticated {
                    authority: "test-operator".to_owned(),
                    subject: "approver".to_owned(),
                },
            )
            .await
            .expect("settle approval");

        let missing = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary"],
            registry(tool_calls.clone()),
            Arc::new(AskPolicy),
            state.clone(),
        )
        .expect("reduced route")
        .with_approval_handler(handler.clone())
        .resume_approval_turn_with_options(&thread.id, &turn_id, TurnExecutionOptions::default())
        .await
        .expect_err("recorded secondary must remain configured");
        assert!(
            missing
                .to_string()
                .contains("absent from the configured route")
        );
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);

        let resumed = HarnessRuntime::from_model_registry_failover(
            &models,
            &["test/route-primary", "test/echo-model"],
            registry(tool_calls.clone()),
            Arc::new(AskPolicy),
            state,
        )
        .expect("restored route")
        .with_approval_handler(handler)
        .resume_approval_turn_with_options(&thread.id, &turn_id, TurnExecutionOptions::default())
        .await
        .expect("resume with exact Model provenance");

        assert_eq!(resumed.turn.status, TurnStatus::Completed);
        assert_eq!(tool_calls.load(Ordering::SeqCst), 1);
        assert_eq!(primary_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn later_turn_receives_bounded_model_visible_thread_history() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(RecordingHistoryModel {
                requests: requests.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        runtime
            .run_turn(&thread.id, "first")
            .await
            .expect("first turn");
        let second = runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second turn");

        let requests = requests.lock().expect("recorded requests");
        assert_eq!(requests.len(), 2);
        assert!(requests[1].items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::UserMessage { content } if content == "first"
            )
        }));
        assert!(requests[1].items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::AssistantMessage { content, .. } if content == "answer to first"
            )
        }));
        assert!(
            !requests[1]
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::ConversationContext { .. }))
        );
        assert!(matches!(
            &second.turn.items[1].kind,
            ItemKind::ConversationContext {
                included_turns,
                dropped_turns: 0,
                ..
            } if included_turns.len() == 1
        ));
    }

    #[tokio::test]
    async fn execution_binding_is_tenant_fenced_durable_and_model_invisible() {
        let path = sqlite_test_path("execution-binding");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "executor".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        let binding = ExecutionBinding::new(
            "domain-pack",
            "course-assistant",
            "1.0.0",
            "a".repeat(64),
            "b".repeat(64),
            9,
            Some("tenant-a".to_owned()),
        )
        .expect("execution binding");
        let thread_id;
        {
            let state = StateEngine::new(Arc::new(
                SqliteEventStore::open(&path).await.expect("open database"),
            ));
            let runtime = HarnessRuntime::new(
                Arc::new(RecordingHistoryModel {
                    requests: requests.clone(),
                }),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                state.clone(),
            );
            let thread = runtime
                .create_thread_as(&authority)
                .await
                .expect("create tenant Thread");
            thread_id = thread.id.clone();
            let outcome = runtime
                .run_turn_with_options(
                    &thread.id,
                    "execute governed release",
                    TurnExecutionOptions {
                        authority: authority.clone(),
                        execution_binding: Some(binding.clone()),
                        ..TurnExecutionOptions::default()
                    },
                )
                .await
                .expect("bound Turn");
            assert!(outcome.turn.items.iter().any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::ExecutionBinding {
                        bound_by,
                        binding: recorded,
                    } if bound_by == authority.actor() && recorded == &binding
                )
            }));
            assert!(
                !requests
                    .lock()
                    .expect("recorded request")
                    .iter()
                    .flat_map(|request| &request.items)
                    .any(|item| matches!(item.kind, ItemKind::ExecutionBinding { .. }))
            );
            state
                .create_snapshot_as(&thread.id, &authority)
                .await
                .expect("create snapshot");
        }
        let reopened = StateEngine::new(Arc::new(
            SqliteEventStore::open(&path)
                .await
                .expect("reopen database"),
        ));
        let projected = reopened
            .load_thread_as(&thread_id, &authority)
            .await
            .expect("load tenant Thread")
            .expect("Thread");
        assert!(projected.turns[0].items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::ExecutionBinding {
                    bound_by,
                    binding: recorded,
                } if bound_by == authority.actor() && recorded == &binding
            )
        }));
        remove_sqlite_files(&path);
    }

    #[tokio::test]
    async fn execution_binding_tenant_mismatch_fails_before_turn_creation() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "executor".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime
            .create_thread_as(&authority)
            .await
            .expect("create tenant Thread");
        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "reject mismatched binding",
                TurnExecutionOptions {
                    authority: authority.clone(),
                    execution_binding: Some(
                        ExecutionBinding::new(
                            "domain-pack",
                            "course-assistant",
                            "1.0.0",
                            "a".repeat(64),
                            "b".repeat(64),
                            1,
                            Some("tenant-b".to_owned()),
                        )
                        .expect("valid other-tenant binding"),
                    ),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("tenant mismatch");
        assert!(error.to_string().contains("tenant does not match"));
        assert!(
            runtime
                .load_thread_as(&thread.id, &authority)
                .await
                .expect("load")
                .expect("Thread")
                .turns
                .is_empty()
        );
    }

    #[tokio::test]
    async fn turn_context_is_model_visible_but_state_keeps_only_attributed_provenance() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(RecordingHistoryModel {
                requests: requests.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let secret_body = "branch-only detail that must not enter State";
        let outcome = runtime
            .run_turn_with_options(
                &thread.id,
                "continue from the selected branch",
                TurnExecutionOptions {
                    context: vec![TurnContextInput {
                        source: "branch-handoff".to_owned(),
                        reference: "thread:source/turn:terminal".to_owned(),
                        text: secret_body.to_owned(),
                    }],
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect("Turn with invocation context");

        let requests = requests.lock().expect("recorded requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].context.len(), 1);
        assert!(requests[0].context[0].text.contains(secret_body));
        assert!(matches!(
            &requests[0].context[0].source,
            ContextSource::Invocation {
                source,
                reference,
                ..
            } if source == "branch-handoff"
                && reference == "thread:source/turn:terminal"
        ));
        let record = outcome
            .turn
            .items
            .iter()
            .find_map(|item| match &item.kind {
                ItemKind::InvocationContext {
                    submitted_by,
                    blocks,
                } => Some((submitted_by, blocks)),
                _ => None,
            })
            .expect("State provenance");
        assert_eq!(record.0, &ActorIdentity::LocalProcess);
        assert_eq!(record.1.len(), 1);
        assert_eq!(record.1[0].source_sha256.len(), 64);
        assert_eq!(record.1[0].content_sha256.len(), 64);
        assert!(
            !serde_json::to_string(&outcome.turn)
                .expect("serialize Turn")
                .contains(secret_body)
        );
    }

    #[tokio::test]
    async fn invalid_turn_context_fails_before_turn_state_is_created() {
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let duplicated = TurnContextInput {
            source: "rag".to_owned(),
            reference: "document:1".to_owned(),
            text: "same provenance".to_owned(),
        };

        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "must not mutate",
                TurnExecutionOptions {
                    context: vec![duplicated.clone(), duplicated],
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("invalid context");

        assert!(matches!(error, HarnessError::InvalidConfiguration(_)));
        assert!(
            runtime
                .load_thread(&thread.id)
                .await
                .expect("load")
                .expect("thread")
                .turns
                .is_empty()
        );
    }

    #[tokio::test]
    async fn semantic_summary_is_observed_and_never_replaces_authoritative_history() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut compactors = ConversationCompactorRegistry::new();
        compactors
            .register(CapabilityOrigin::BuiltIn, Arc::new(StaticCompactor))
            .expect("register compactor");
        let context = ContextEngine::without_memory()
            .with_conversation_config(ConversationContextConfig {
                max_turns: 1,
                budget_tokens: 65_536,
                budget_bytes: 65_536,
            })
            .expect("conversation config")
            .with_conversation_compactor(
                compactors,
                ConversationCompactionConfig {
                    compactor: "test.static-summary".to_owned(),
                    max_input_turns: 1,
                    input_budget_bytes: 65_536,
                    output_budget_tokens: 1_024,
                    output_budget_bytes: 4_096,
                },
            )
            .expect("compactor selection");
        let collector = Arc::new(TraceCollector::new(64).expect("trace collector"));
        let mut observability = Observability::new();
        observability
            .register("trace", CapabilityOrigin::BuiltIn, collector.clone())
            .expect("register trace collector");
        let runtime = HarnessRuntime::new(
            Arc::new(RecordingHistoryModel {
                requests: requests.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_context_engine(context)
        .with_observability(observability);
        let thread = runtime.create_thread().await.expect("create thread");
        let first = runtime
            .run_turn(&thread.id, "first")
            .await
            .expect("first turn");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second turn");
        runtime
            .run_turn(&thread.id, "third")
            .await
            .expect("third turn");

        {
            let requests = requests.lock().expect("recorded requests");
            let third_request = &requests[2];
            assert!(!third_request.items.iter().any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::UserMessage { content } if content == "first"
                )
            }));
            assert!(third_request.items.iter().any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::UserMessage { content } if content == "second"
                )
            }));
            assert!(matches!(
                &third_request.context[0].source,
                ContextSource::ConversationSummary {
                    compactor,
                    covered_turns,
                    older_omitted_turns: 0,
                    ..
                } if compactor == "test.static-summary"
                    && covered_turns == &[first.turn.id.clone()]
            ));
        }

        let authoritative = runtime
            .load_thread(&thread.id)
            .await
            .expect("load state")
            .expect("thread");
        assert!(authoritative.turns[0].items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::UserMessage { content } if content == "first"
            )
        }));
        assert!(authoritative.turns[2].items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::ConversationSummary {
                    compactor,
                    covered_turns,
                    older_omitted_turns: 0,
                    source_sha256,
                    content_sha256,
                    estimated_tokens,
                    serialized_bytes,
                } if compactor == "test.static-summary"
                    && covered_turns == &[first.turn.id.clone()]
                    && source_sha256.len() == 64
                    && content_sha256.len() == 64
                    && *estimated_tokens > 0
                    && *serialized_bytes > 0
            )
        }));
        assert!(collector.snapshot().iter().any(|record| {
            record.phase == ExecutionPhase::Context
                && record.capability == "test.static-summary"
                && record.outcome == ObservationOutcome::Success
        }));
    }

    #[tokio::test]
    async fn semantic_compactor_panic_is_sanitized_and_durably_settled() {
        let mut compactors = ConversationCompactorRegistry::new();
        compactors
            .register(CapabilityOrigin::BuiltIn, Arc::new(PanickingCompactor))
            .expect("register compactor");
        let context = ContextEngine::without_memory()
            .with_conversation_config(ConversationContextConfig {
                max_turns: 1,
                budget_tokens: 65_536,
                budget_bytes: 65_536,
            })
            .expect("conversation config")
            .with_conversation_compactor(
                compactors,
                ConversationCompactionConfig {
                    compactor: "test.panicking-summary".to_owned(),
                    max_input_turns: 1,
                    input_budget_bytes: 65_536,
                    output_budget_tokens: 1_024,
                    output_budget_bytes: 4_096,
                },
            )
            .expect("compactor selection");
        let runtime = HarnessRuntime::new(
            Arc::new(RecordingHistoryModel {
                requests: Arc::new(Mutex::new(Vec::new())),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )
        .with_context_engine(context);
        let thread = runtime.create_thread().await.expect("create thread");
        runtime
            .run_turn(&thread.id, "first")
            .await
            .expect("first turn");
        runtime
            .run_turn(&thread.id, "second")
            .await
            .expect("second turn");
        let error = runtime
            .run_turn(&thread.id, "third")
            .await
            .expect_err("compactor panic");

        assert_eq!(
            error,
            HarnessError::CapabilityPanicked {
                phase: ExecutionPhase::Context
            }
        );
        assert!(!error.to_string().contains("sensitive"));
        let authoritative = runtime
            .load_thread(&thread.id)
            .await
            .expect("load state")
            .expect("thread");
        let failed = &authoritative.turns[2];
        assert_eq!(failed.status, TurnStatus::Failed);
        assert!(failed.items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::RuntimeError { message }
                    if !message.contains("sensitive")
                        && message.contains("capability panicked during Context")
            )
        }));
    }

    #[tokio::test]
    async fn oversized_tool_output_becomes_a_bounded_model_visible_error() {
        let mut tools = ToolRegistry::new();
        tools
            .register(CapabilityOrigin::BuiltIn, Arc::new(OversizedTool))
            .expect("register oversized tool");
        let runtime = HarnessRuntime::new(
            Arc::new(ToolErrorObserverModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("oversized")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let outcome = runtime
            .run_turn(&thread.id, "bounded tool output")
            .await
            .expect("turn");

        let result = outcome
            .turn
            .items
            .iter()
            .find_map(|item| match &item.kind {
                ItemKind::ToolResult {
                    output,
                    is_error: true,
                    ..
                } => Some(output),
                _ => None,
            })
            .expect("bounded Tool error");
        assert!(result.to_string().len() < 1_024);
    }

    #[tokio::test]
    async fn steering_is_durable_fenced_and_invalidates_a_crossed_model_response() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(RecordingModelSink::default());
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(SteeringModel {
                calls: AtomicUsize::new(0),
                entered: entered.clone(),
                release: release.clone(),
                requests: requests.clone(),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        ));
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            let sink = sink.clone();
            tokio::spawn(async move {
                runtime
                    .run_turn_with_options(
                        &thread_id,
                        "original",
                        TurnExecutionOptions {
                            model_event_sink: Some(sink),
                            ..TurnExecutionOptions::default()
                        },
                    )
                    .await
            })
        };
        entered.notified().await;
        let active = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active thread")
            .expect("thread");
        let turn_id = active
            .turns
            .iter()
            .find(|turn| turn.status == TurnStatus::Running)
            .expect("running turn")
            .id
            .clone();
        let mismatch = runtime
            .steer_turn(
                &thread.id,
                &crate::TurnId::generate(),
                "wrong target",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect_err("mismatched Turn must fail");
        assert!(matches!(mismatch, HarnessError::State(_)));
        let receipt = runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "corrected",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue steering");
        assert_eq!(receipt.turn_id, turn_id);
        release.notify_one();

        let outcome = worker.await.expect("worker").expect("steered turn");
        assert_eq!(outcome.final_text, "accepted: corrected");
        assert!(!outcome.turn.items.iter().any(|item| {
            matches!(
                &item.kind,
                ItemKind::AssistantMessage { content, .. } if content.contains("stale")
            )
        }));
        let queued = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::SteeringQueued { .. }))
            .expect("durable queue record");
        let applied = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::SteeringApplied { .. }))
            .expect("durable application record");
        assert!(queued < applied);
        assert!(matches!(
            sink.events.lock().expect("stream events").as_slice(),
            [
                ModelStreamEvent::TextDelta { delta, .. },
                ModelStreamEvent::StepInvalidated { model_step: 1 }
            ] if delta == "stale provisional response"
        ));
        {
            let requests = requests.lock().expect("requests");
            assert_eq!(requests.len(), 2);
            assert!(requests[1].items.iter().any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::UserMessage { content } if content == "corrected"
                )
            }));
        }
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "too late",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect_err("terminal Turn must reject steering");
    }

    #[tokio::test]
    async fn failed_steering_application_preserves_the_pending_runtime_projection() {
        let runtime = HarnessRuntime::new(
            Arc::new(EchoModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(RejectSteeringApplicationStore::new())),
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let mut turn = runtime
            .state
            .start_turn(&thread.id)
            .await
            .expect("start turn");
        let _turn_control = runtime
            .register_turn_control(&turn, AuthorityContext::local_process())
            .expect("register Turn control");
        runtime
            .steer_turn(
                &thread.id,
                &turn.id,
                "keep me pending",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue steering");

        let error = runtime
            .apply_pending_steering(&mut turn, false, &ModelStream::disabled(), None)
            .await
            .expect_err("SteeringApplied append must fail");
        assert!(matches!(error, HarnessError::State(_)));

        let control = runtime
            .turn_control(&thread.id)
            .expect("Turn control remains registered");
        let control = control.lock().await;
        assert!(!control.accepting_steering);
        assert_eq!(control.pending_steering.len(), 1);
        assert_eq!(
            control.pending_steering_bytes,
            "keep me pending".len(),
            "in-memory projection must not advance before durable State"
        );
        drop(control);

        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load thread")
            .expect("thread");
        assert_eq!(projected.turns[0].status, TurnStatus::Failed);
        assert!(
            projected.turns[0]
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::SteeringQueued { .. }))
        );
        assert!(
            !projected.turns[0]
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::SteeringApplied { .. }))
        );
    }

    #[tokio::test]
    async fn steering_crossing_model_inference_discards_a_stale_tool_call() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(EchoTool {
                    calls: tool_calls.clone(),
                }),
            )
            .expect("register echo");
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(SteeringToolModel {
                calls: AtomicUsize::new(0),
                entered: entered.clone(),
                release: release.clone(),
            }),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        ));
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "original").await })
        };
        entered.notified().await;
        let active = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active")
            .expect("thread");
        let turn_id = active.turns[0].id.clone();
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "do not execute that tool",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue steering");
        release.notify_one();

        let outcome = worker.await.expect("worker").expect("steered turn");
        assert_eq!(outcome.final_text, "accepted: do not execute that tool");
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        assert!(
            !outcome
                .turn
                .items
                .iter()
                .any(|item| matches!(item.kind, ItemKind::ToolCall { .. }))
        );
    }

    #[tokio::test]
    async fn steering_before_the_tool_effect_preserves_call_result_structure_without_execution() {
        let policy_entered = Arc::new(Notify::new());
        let policy_release = Arc::new(Notify::new());
        let model_release = Arc::new(Notify::new());
        model_release.notify_one();
        let tool_calls = Arc::new(AtomicUsize::new(0));
        let mut tools = ToolRegistry::new();
        tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(EchoTool {
                    calls: tool_calls.clone(),
                }),
            )
            .expect("register echo");
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(SteeringToolModel {
                calls: AtomicUsize::new(0),
                entered: Arc::new(Notify::new()),
                release: model_release,
            }),
            tools,
            Arc::new(BlockingAllowPolicy {
                entered: policy_entered.clone(),
                release: policy_release.clone(),
            }),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        ));
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "original").await })
        };
        policy_entered.notified().await;
        let active = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active")
            .expect("thread");
        let turn_id = active.turns[0].id.clone();
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "skip the pending tool",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue steering");
        policy_release.notify_one();

        let outcome = worker.await.expect("worker").expect("steered turn");
        assert_eq!(outcome.final_text, "accepted: skip the pending tool");
        assert_eq!(tool_calls.load(Ordering::SeqCst), 0);
        let tool_call = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::ToolCall { .. }))
            .expect("recorded Tool call");
        let tool_result = outcome
            .turn
            .items
            .iter()
            .position(|item| {
                matches!(
                    &item.kind,
                    ItemKind::ToolResult {
                        call_id,
                        is_error: true,
                        ..
                    } if call_id == "stale-tool-call"
                )
            })
            .expect("synthetic Tool result");
        let applied = outcome
            .turn
            .items
            .iter()
            .position(|item| matches!(item.kind, ItemKind::SteeringApplied { .. }))
            .expect("steering application");
        assert!(tool_call < tool_result && tool_result < applied);
    }

    #[tokio::test]
    async fn steering_pending_count_and_bytes_are_bounded_before_durable_acceptance() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(SteeringModel {
                calls: AtomicUsize::new(0),
                entered: entered.clone(),
                release: release.clone(),
                requests: Arc::new(Mutex::new(Vec::new())),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        ));
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "original").await })
        };
        entered.notified().await;
        let turn_id = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active")
            .expect("thread")
            .turns[0]
            .id
            .clone();
        for _ in 0..MAX_PENDING_STEERING {
            runtime
                .steer_turn(&thread.id, &turn_id, "x", ApprovalActor::LocalProcess)
                .await
                .expect("queue within count bound");
        }
        let error = runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "overflow",
                ApprovalActor::LocalProcess,
            )
            .await
            .expect_err("reject count overflow");
        assert!(error.to_string().contains("steering capacity reached"));
        release.notify_one();
        worker.await.expect("worker").expect("count-bounded turn");

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(SteeringModel {
                calls: AtomicUsize::new(0),
                entered: entered.clone(),
                release: release.clone(),
                requests: Arc::new(Mutex::new(Vec::new())),
            }),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        ));
        let thread = runtime.create_thread().await.expect("create thread");
        let worker = {
            let runtime = runtime.clone();
            let thread_id = thread.id.clone();
            tokio::spawn(async move { runtime.run_turn(&thread_id, "original").await })
        };
        entered.notified().await;
        let turn_id = runtime
            .load_thread(&thread.id)
            .await
            .expect("load active")
            .expect("thread")
            .turns[0]
            .id
            .clone();
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "a".repeat(600_000),
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue first byte range");
        runtime
            .steer_turn(
                &thread.id,
                &turn_id,
                "b".repeat(MAX_PENDING_STEERING_BYTES - 600_000),
                ApprovalActor::LocalProcess,
            )
            .await
            .expect("queue through exact byte bound");
        let error = runtime
            .steer_turn(&thread.id, &turn_id, "x", ApprovalActor::LocalProcess)
            .await
            .expect_err("reject byte overflow");
        assert!(error.to_string().contains("steering capacity reached"));
        release.notify_one();
        worker.await.expect("worker").expect("byte-bounded turn");
    }

    #[tokio::test]
    async fn invalid_embedded_prompt_is_rejected_before_turn_creation() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(UsageModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let error = runtime
            .run_turn(&thread.id, " ")
            .await
            .expect_err("empty prompt");
        assert!(matches!(error, HarnessError::InvalidConfiguration(_)));
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert!(projected.turns.is_empty());
    }

    #[tokio::test]
    async fn invalid_authority_is_rejected_before_turn_creation() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = HarnessRuntime::new(
            Arc::new(UsageModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        );
        let thread = runtime.create_thread().await.expect("create thread");
        let error = runtime
            .run_turn_with_options(
                &thread.id,
                "valid prompt",
                TurnExecutionOptions {
                    authority: serde_json::from_value(serde_json::json!({
                        "actor": {
                            "kind": "authenticated",
                            "authority": " ",
                            "subject": "operator"
                        }
                    }))
                    .expect("shape-only decode"),
                    ..TurnExecutionOptions::default()
                },
            )
            .await
            .expect_err("invalid requester");
        assert!(matches!(error, HarnessError::Approval(_)));
        let projected = runtime
            .load_thread(&thread.id)
            .await
            .expect("load")
            .expect("thread");
        assert!(projected.turns.is_empty());
    }

    #[test]
    fn model_and_tool_json_shape_is_bounded_before_serialization() {
        let mut nested = Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            nested = Value::Array(vec![nested]);
        }
        assert!(matches!(
            validate_model_tool_call("call-1", "tool", &nested),
            Err(HarnessError::Model(_))
        ));
        assert!(matches!(
            validate_tool_output(&nested),
            Err(HarnessError::Tool(_))
        ));
        let request = ModelRequest {
            thread_id: ThreadId::generate(),
            turn_id: crate::TurnId::generate(),
            authority: crate::AuthorityContext::local_process(),
            items: vec![crate::Item::new(ItemKind::ToolCall {
                model_id: Some("test/model".to_owned()),
                model_origin: Some(CapabilityOrigin::BuiltIn),
                call_id: "call-1".to_owned(),
                name: "tool".to_owned(),
                input: nested,
                batch: None,
            })],
            context: Vec::new(),
            tools: Vec::new(),
        };
        assert!(matches!(
            validate_model_request(&request),
            Err(HarnessError::Model(_))
        ));
    }

    #[test]
    fn duplicate_tool_names_are_rejected() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = registry(calls.clone());
        let error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(EchoTool { calls }))
            .expect_err("duplicate must fail");
        assert_eq!(error, HarnessError::DuplicateCapability("echo".to_owned()));
    }
}
