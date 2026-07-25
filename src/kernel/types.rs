//! Stable identities and serializable state/model contracts owned by the kernel.

use std::{
    error::Error,
    fmt::{self, Display},
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CancellationToken, ContextBlock};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Boxed asynchronous result used by object-safe capability contracts.
pub type HarnessFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, HarnessError>> + Send + 'a>>;

macro_rules! id_type {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        #[doc = concat!("Opaque kernel identity for `", stringify!($name), "`.")]
        pub struct $name(String);

        impl $name {
            /// Generates a process-unique, time-correlated identity.
            #[must_use]
            pub fn generate() -> Self {
                Self(next_id($prefix))
            }

            /// Constructs an identity from a static value, primarily for fixtures.
            #[must_use]
            pub fn from_static(value: &'static str) -> Self {
                Self(value.to_owned())
            }

            /// Constructs an identity from an owned persisted or protocol value.
            #[must_use]
            pub fn from_string(value: String) -> Self {
                Self(value)
            }

            /// Returns the opaque identity as text without transferring ownership.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

id_type!(ThreadId, "thread");
id_type!(TurnId, "turn");
id_type!(ItemId, "item");
id_type!(EventId, "event");
id_type!(CheckpointId, "checkpoint");
id_type!(ApprovalId, "approval");
id_type!(TaskGraphId, "task-graph");
id_type!(TaskId, "task");
id_type!(TaskLeaseId, "lease");
id_type!(TaskMessageId, "message");
id_type!(ArtifactId, "artifact");
id_type!(OperationId, "operation");

fn next_id(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    format!(
        "{prefix}-{timestamp:x}-{:x}-{sequence:x}",
        std::process::id()
    )
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Durable projection of one conversation thread.
pub struct Thread {
    /// Stable thread identity.
    pub id: ThreadId,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Ordered Turn projections.
    pub turns: Vec<Turn>,
    /// Checkpoints associated with this thread.
    pub checkpoints: Vec<Checkpoint>,
}

impl Thread {
    /// Creates an empty thread with a generated identity.
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: ThreadId::generate(),
            created_at_ms: now_ms(),
            turns: Vec::new(),
            checkpoints: Vec::new(),
        }
    }
}

impl Default for Thread {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// One bounded Agent Loop execution inside a Thread.
pub struct Turn {
    /// Stable turn identity.
    pub id: TurnId,
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Current projected lifecycle status.
    pub status: TurnStatus,
    /// Ordered items recorded during the turn.
    pub items: Vec<Item>,
}

impl Turn {
    /// Creates a running turn for `thread_id`.
    #[must_use]
    pub fn new(thread_id: ThreadId) -> Self {
        Self {
            id: TurnId::generate(),
            thread_id,
            status: TurnStatus::Running,
            items: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Lifecycle states for a Turn.
pub enum TurnStatus {
    /// Execution has started but no terminal event has settled.
    Running,
    /// Completion conditions were reached.
    Completed,
    /// Execution settled with a failure.
    Failed,
    /// A caller explicitly requested cooperative cancellation.
    Cancelled,
    /// The configured Turn execution deadline elapsed.
    TimedOut,
    /// Recovery found execution unfinished after a runtime interruption.
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Timestamped unit of ordered Turn history.
pub struct Item {
    /// Stable item identity.
    pub id: ItemId,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: u64,
    #[serde(flatten)]
    /// Typed item payload.
    pub kind: ItemKind,
}

impl Item {
    /// Creates a timestamped item with a generated identity.
    #[must_use]
    pub fn new(kind: ItemKind) -> Self {
        Self {
            id: ItemId::generate(),
            created_at_ms: now_ms(),
            kind,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Runtime item payloads recorded in ordered state.
pub enum ItemKind {
    /// User input supplied to the turn.
    UserMessage {
        /// Message text.
        content: String,
    },
    /// Final or intermediate assistant text.
    AssistantMessage {
        /// Registered model identity, absent only on legacy imported events.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_id: Option<String>,
        /// Trust-bearing model origin, absent only on legacy imported events.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_origin: Option<crate::CapabilityOrigin>,
        /// Message text.
        content: String,
    },
    /// Model-requested tool invocation.
    ToolCall {
        /// Registered model identity, absent only on legacy imported events.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_id: Option<String>,
        /// Trust-bearing model origin, absent only on legacy imported events.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_origin: Option<crate::CapabilityOrigin>,
        /// Provider-generated correlation ID.
        call_id: String,
        /// Registered tool name.
        name: String,
        /// Validated JSON input.
        input: Value,
    },
    /// Policy settlement associated with a tool call.
    PolicyDecision {
        /// Tool-call correlation ID.
        call_id: String,
        /// Trust-bearing Tool origin evaluated by Policy, absent only on
        /// legacy imported events.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_origin: Option<crate::CapabilityOrigin>,
        /// Authorization outcome.
        decision: PolicyDecision,
    },
    /// Durable request emitted before waiting for an approval settlement.
    ApprovalRequested {
        /// Kernel-generated approval request identity.
        approval_id: ApprovalId,
        /// Tool-call correlation ID.
        call_id: String,
        /// Registered Tool identity.
        tool: String,
        /// Bounded Policy rationale shown to the approver.
        reason: String,
        /// Policy-assigned risk classification.
        risk: RiskLevel,
        /// Authenticated actor that initiated the owning Turn.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        requested_by: Option<ApprovalActor>,
        /// Trust-bearing origin of the Tool authorized by this request.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_origin: Option<crate::CapabilityOrigin>,
        /// SHA-256 of the exact Model request that produced this Tool call.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model_request_sha256: Option<String>,
    },
    /// Settlement returned by an approval handler after a policy asks.
    ApprovalDecision {
        /// Kernel-generated approval request identity.
        approval_id: ApprovalId,
        /// Tool-call correlation ID.
        call_id: String,
        /// Approval outcome.
        decision: ApprovalDecision,
    },
    /// Tool execution settlement.
    ToolResult {
        /// Tool-call correlation ID.
        call_id: String,
        /// Structured tool output or normalized error object.
        output: Value,
        /// Whether the tool execution failed.
        is_error: bool,
    },
    /// Evidence describing compiled long-term memory context.
    MemoryContext {
        /// Registered provider name.
        provider: String,
        /// Whether memory loaded or explicitly degraded.
        status: MemoryContextRecordStatus,
        /// Opaque provider references included in context.
        references: Vec<String>,
        /// Provider-reported token estimate selected by Context Engine.
        packed_tokens: usize,
        /// Non-fatal provider or compilation warnings.
        warnings: Vec<String>,
    },
    /// Evidence describing the previous-Turn context window.
    ConversationContext {
        /// Previous Turns included in chronological order.
        included_turns: Vec<TurnId>,
        /// Older or oversized candidate Turns omitted from the window.
        dropped_turns: usize,
        /// Conservative serialized-byte charge retained for schema-1 evidence.
        estimated_tokens: usize,
    },
    /// Content-free evidence for one model-visible derived conversation summary.
    ConversationSummary {
        /// Stable registered compactor name.
        compactor: String,
        /// Exact omitted whole Turns represented by the summary.
        covered_turns: Vec<TurnId>,
        /// Still-older omitted Turns not represented by the summary.
        older_omitted_turns: usize,
        /// SHA-256 of the canonical covered-Turn input.
        source_sha256: String,
        /// SHA-256 of the exact model-visible summary block.
        content_sha256: String,
        /// Provider-specific token charge for the final summary block.
        estimated_tokens: usize,
        /// Exact UTF-8 bytes in the final summary block.
        serialized_bytes: usize,
    },
    /// Runtime-level failure evidence.
    RuntimeError {
        /// Actionable error message without secret payloads.
        message: String,
    },
    /// Explicit cancellation or deadline evidence.
    TurnStopped {
        /// Why execution stopped.
        reason: TurnStopReason,
        /// External operation active when the stop was observed.
        phase: ExecutionPhase,
    },
    /// One verifier's settlement for an assistant candidate.
    VerificationResult {
        /// Registered verifier name.
        verifier: String,
        /// Completion-condition outcome.
        outcome: VerificationOutcome,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Journal representation of memory context settlement.
pub enum MemoryContextRecordStatus {
    /// Provider packs were compiled.
    Loaded,
    /// Configured fail-open behavior continued without provider context.
    Degraded,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Externally controlled execution phase used for stop evidence.
pub enum ExecutionPhase {
    /// Long-term memory retrieval and context compilation.
    Context,
    /// Language-model inference.
    Model,
    /// Tool authorization policy evaluation.
    Policy,
    /// Operator or delegated approval settlement.
    Approval,
    /// Authorized tool execution.
    Tool,
    /// Candidate-result verification.
    Verification,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Controlled reason for stopping a Turn.
pub enum TurnStopReason {
    /// A caller explicitly requested cancellation.
    Cancelled,
    /// The configured execution deadline elapsed.
    TimedOut,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Policy-assigned risk class for an approval request.
pub enum RiskLevel {
    /// Read-only or readily reversible effect.
    Low,
    /// Bounded mutation with a clear recovery path.
    Medium,
    /// Material or difficult-to-reverse effect.
    High,
    /// Destructive, security-sensitive, or broadly scoped effect.
    Critical,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
/// Provider-neutral identity participating in an approval workflow.
///
/// This value is attribution supplied by a trusted transport or embedding
/// host; constructing `Authenticated` does not authenticate its strings.
pub enum ApprovalActor {
    /// Caller trusted only through the embedding process boundary.
    LocalProcess,
    /// Subject established by one named authentication authority.
    Authenticated {
        /// Stable authority or authentication mechanism name.
        authority: String,
        /// Stable authority-scoped subject identity.
        subject: String,
    },
    /// Unrecoverable identity from a migrated schema-1 terminal record.
    ///
    /// Current requests and settlements reject this value. It exists only so
    /// migration can preserve historical decisions without inventing actors.
    UnattributedLegacy,
}

impl ApprovalActor {
    pub(crate) fn validate_shape(&self, kind: &str) -> Result<(), HarnessError> {
        match self {
            Self::LocalProcess | Self::UnattributedLegacy => Ok(()),
            Self::Authenticated { authority, subject } => {
                validate_actor_identity(kind, "authority", authority)?;
                validate_actor_identity(kind, "subject", subject)
            }
        }
    }

    pub(crate) fn validate_current(&self, kind: &str) -> Result<(), HarnessError> {
        self.validate_shape(kind)?;
        if matches!(self, Self::UnattributedLegacy) {
            return Err(HarnessError::Approval(format!(
                "{kind} cannot use the legacy unattributed identity"
            )));
        }
        Ok(())
    }
}

fn validate_actor_identity(kind: &str, field: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(HarnessError::Approval(format!(
            "{kind} {field} must be 1-256 non-control bytes"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
/// Policy decision evaluated before a tool side effect.
pub enum PolicyDecision {
    /// Execution is authorized.
    Allow,
    /// Execution is denied.
    Deny {
        /// Human-readable denial rationale.
        reason: String,
    },
    /// Execution requires a separate approval settlement.
    Ask {
        /// Human-readable reason shown to the approver.
        reason: String,
        /// Policy-assigned risk class.
        risk: RiskLevel,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
/// Settlement returned for a policy-generated approval request.
pub enum ApprovalDecision {
    /// The requested side effect is approved.
    Approve,
    /// The requested side effect is rejected.
    Deny {
        /// Human-readable rejection rationale.
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
/// Completion-condition settlement returned by a verifier.
pub enum VerificationOutcome {
    /// The candidate satisfies this verifier.
    Passed {
        /// Optional bounded explanation suitable for audit logs.
        summary: Option<String>,
    },
    /// The candidate violates this verifier.
    Failed {
        /// Bounded actionable failure explanation.
        reason: String,
        /// Whether another Agent Loop step may correct the candidate.
        retryable: bool,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Model-visible metadata for a registered tool.
pub struct ToolDescriptor {
    /// Stable registry name.
    pub name: String,
    /// Human-readable behavior description.
    pub description: String,
    /// JSON Schema describing accepted input.
    pub input_schema: Value,
}

#[derive(Clone, Debug)]
/// Correlation context supplied to tool execution.
pub struct ToolContext {
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Owning turn.
    pub turn_id: TurnId,
    /// Tool-call correlation ID.
    pub call_id: String,
    /// Cooperative stop signal shared with the owning Turn.
    pub cancellation: CancellationToken,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Policy input describing a proposed tool execution.
pub struct ToolAuthorization {
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Active turn.
    pub turn_id: TurnId,
    /// Tool-call correlation ID.
    pub call_id: String,
    /// Tool metadata.
    pub descriptor: ToolDescriptor,
    /// Registered implementation origin.
    pub origin: crate::CapabilityOrigin,
    /// Proposed JSON input.
    pub input: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Fully correlated request created when Policy returns `Ask`.
pub struct ApprovalRequest {
    /// Kernel-generated request identity.
    pub id: ApprovalId,
    /// Authenticated identity that initiated the owning Turn.
    pub requested_by: ApprovalActor,
    /// Original tool authorization proposal.
    pub authorization: ToolAuthorization,
    /// Policy rationale shown to the approver.
    pub reason: String,
    /// Policy-assigned risk class.
    pub risk: RiskLevel,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Complete model-step input assembled by the runtime.
pub struct ModelRequest {
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Active turn.
    pub turn_id: TurnId,
    /// Ordered conversation and runtime items.
    pub items: Vec<Item>,
    /// Compiled external context, kept distinct from conversation history.
    pub context: Vec<ContextBlock>,
    /// Tools visible to the model for this step.
    pub tools: Vec<ToolDescriptor>,
}

/// Provider-reported token and cost accounting for one model call.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelUsage {
    /// Tokens sent to the provider, including cached input when reported.
    pub input_tokens: u64,
    /// Newly generated output tokens.
    pub output_tokens: u64,
    /// Input tokens served from provider cache.
    pub cached_input_tokens: u64,
    /// Provider-reported reasoning tokens.
    pub reasoning_tokens: u64,
    /// Optional provider-calculated cost in millionths of one US dollar.
    pub cost_microusd: Option<u64>,
}

/// Model decision plus optional provider evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelResponse {
    /// Decision consumed by the Agent Loop.
    pub output: ModelOutput,
    /// Provider-reported accounting; the Runtime never invents missing usage.
    pub usage: Option<ModelUsage>,
    /// Optional opaque provider request identity for support correlation.
    pub provider_request_id: Option<String>,
}

/// Provisional model output emitted before the authoritative response settles.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelStreamEvent {
    /// One bounded assistant-text fragment for a model step.
    TextDelta {
        /// One-based Agent Loop model-step number.
        model_step: u32,
        /// Provisional text fragment.
        delta: String,
    },
}

impl From<ModelOutput> for ModelResponse {
    fn from(output: ModelOutput) -> Self {
        Self {
            output,
            usage: None,
            provider_request_id: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Model decision returned to the Agent Loop.
pub enum ModelOutput {
    /// Produce assistant text and complete the current loop.
    Message {
        /// Assistant text.
        content: String,
    },
    /// Request a registered tool.
    ToolCall {
        /// Provider-generated correlation ID.
        call_id: String,
        /// Requested registered tool name.
        name: String,
        /// Proposed JSON input.
        input: Value,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Named recovery marker targeting an ordered journal position.
pub struct Checkpoint {
    /// Stable checkpoint identity.
    pub id: CheckpointId,
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Optional associated turn.
    pub turn_id: Option<TurnId>,
    /// Last journal sequence included by the checkpoint.
    pub target_sequence: u64,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Optional operator-facing description.
    pub label: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Append-only events accepted by the State Engine projector.
pub enum StateEvent {
    /// Creates the thread stream.
    ThreadCreated {
        /// Thread creation time in Unix milliseconds.
        created_at_ms: u64,
    },
    /// Starts a new turn.
    TurnStarted {
        /// Started turn identity.
        turn_id: TurnId,
    },
    /// Appends an ordered item to a running turn.
    ItemAppended {
        /// Target turn.
        turn_id: TurnId,
        /// Appended item.
        item: Item,
    },
    /// Settles a running turn.
    TurnFinished {
        /// Target turn.
        turn_id: TurnId,
        /// Non-running terminal status.
        status: TurnStatus,
    },
    /// Adds a checkpoint projection.
    CheckpointCreated {
        /// Checkpoint data.
        checkpoint: Checkpoint,
    },
}

#[derive(Clone, Debug, PartialEq)]
/// Caller-authored event before durable sequence assignment.
pub struct PendingEvent {
    /// Idempotency identity.
    pub event_id: EventId,
    /// Target thread stream.
    pub thread_id: ThreadId,
    /// Required current event count for optimistic concurrency control.
    pub expected_stream_version: u64,
    /// Required current bounded recovery charge for optimistic concurrency control.
    pub expected_stream_recovery_bytes: u64,
    /// Recording time in Unix milliseconds.
    pub recorded_at_ms: u64,
    /// Typed state transition.
    pub event: StateEvent,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Durably sequenced event returned by an Event Store.
pub struct StoredEvent {
    /// Persisted event schema version.
    pub schema_version: u32,
    /// Global durable ordering sequence.
    pub sequence: u64,
    /// Idempotency identity.
    pub event_id: EventId,
    /// Owning thread stream.
    pub thread_id: ThreadId,
    /// Recording time in Unix milliseconds.
    pub recorded_at_ms: u64,
    #[serde(flatten)]
    /// Typed state transition.
    pub event: StateEvent,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Successful terminal result of running one Turn.
pub struct TurnOutcome {
    /// Final projected turn.
    pub turn: Turn,
    /// Final assistant text.
    pub final_text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Typed subsystem errors surfaced by the public runtime.
pub enum HarnessError {
    /// Runtime or provider configuration is invalid before execution.
    InvalidConfiguration(String),
    /// Descriptor or registration metadata is invalid.
    InvalidCapability(String),
    /// A capability attempted to replace an existing name.
    DuplicateCapability(String),
    /// The model requested an unknown tool.
    UnknownTool(String),
    /// Runtime configuration selected an unregistered model.
    UnknownModel(String),
    /// Policy rejected a proposed tool call.
    PolicyDenied {
        /// Rejected tool name.
        tool: String,
        /// Policy rationale.
        reason: String,
    },
    /// An approval handler rejected a policy-generated request.
    ApprovalDenied {
        /// Rejected tool name.
        tool: String,
        /// Approval rationale.
        reason: String,
    },
    /// Model-provider failure.
    Model(String),
    /// Policy-provider evaluation failure.
    Policy(String),
    /// Approval-provider settlement failure.
    Approval(String),
    /// An approval settlement lost an optimistic revision race.
    ApprovalConflict {
        /// Contended approval request.
        approval_id: ApprovalId,
        /// Revision observed by the caller.
        expected: u64,
        /// Revision found atomically by the inbox.
        actual: u64,
    },
    /// Completion verification failure or provider error.
    Verification(String),
    /// Evaluation suite, grader, report, or baseline failure.
    Evaluation(String),
    /// Task graph, lease, message, or artifact contract failure.
    Orchestration(String),
    /// An atomic Task Graph save lost an optimistic revision race.
    OrchestrationConflict {
        /// Contended Task Graph.
        graph_id: TaskGraphId,
        /// Revision observed before mutation.
        expected: u64,
        /// Revision found atomically by the coordinator.
        actual: u64,
    },
    /// Tool execution failure.
    Tool(String),
    /// External process broker, isolation, or I/O failure.
    Execution(String),
    /// Secret reference, resolution, or credential-boundary failure.
    Secret(String),
    /// Memory provider or contract failure.
    Memory(String),
    /// Skill package validation, resolution, or loading failure.
    Skill(String),
    /// MCP lifecycle, transport, or tool-call failure.
    Mcp(String),
    /// Typed client protocol framing or service failure.
    Protocol(String),
    /// The Runtime rejected admission before creating a Turn.
    RuntimeOverloaded {
        /// Configured maximum simultaneously active Turns.
        limit: usize,
    },
    /// An executable capability panicked at a Runtime-governed invocation.
    CapabilityPanicked {
        /// Phase whose provider crossed the panic boundary.
        phase: ExecutionPhase,
    },
    /// A client principal lacks one exact protocol permission.
    ProtocolDenied {
        /// Stable permission name rejected before command execution.
        permission: String,
    },
    /// State storage, projection, or recovery failure.
    State(String),
    /// An atomic append lost an optimistic stream-version race.
    StateConflict {
        /// Contended thread stream.
        thread_id: ThreadId,
        /// Version observed by the caller before append.
        expected: u64,
        /// Version found atomically by the Event Store.
        actual: u64,
    },
    /// Evidence export failure.
    Trace(String),
    /// Agent Loop exhausted its configured step budget.
    MaxSteps(usize),
    /// A caller cancelled the Turn during an external operation.
    Cancelled {
        /// Operation active when cancellation was observed.
        phase: ExecutionPhase,
    },
    /// The Turn deadline elapsed during an external operation.
    TimedOut {
        /// Operation active when the deadline elapsed.
        phase: ExecutionPhase,
    },
}

impl Display for HarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid configuration: {message}")
            }
            Self::InvalidCapability(message) => write!(formatter, "invalid capability: {message}"),
            Self::DuplicateCapability(name) => write!(formatter, "duplicate capability: {name}"),
            Self::UnknownTool(name) => write!(formatter, "unknown tool: {name}"),
            Self::UnknownModel(id) => write!(formatter, "unknown model: {id}"),
            Self::PolicyDenied { tool, reason } => {
                write!(formatter, "policy denied tool {tool}: {reason}")
            }
            Self::ApprovalDenied { tool, reason } => {
                write!(formatter, "approval denied tool {tool}: {reason}")
            }
            Self::Model(message) => write!(formatter, "model error: {message}"),
            Self::Policy(message) => write!(formatter, "policy error: {message}"),
            Self::Approval(message) => write!(formatter, "approval error: {message}"),
            Self::ApprovalConflict {
                approval_id,
                expected,
                actual,
            } => write!(
                formatter,
                "approval conflict on {approval_id}: expected revision {expected}, found {actual}"
            ),
            Self::Verification(message) => write!(formatter, "verification error: {message}"),
            Self::Evaluation(message) => write!(formatter, "evaluation error: {message}"),
            Self::Orchestration(message) => write!(formatter, "orchestration error: {message}"),
            Self::OrchestrationConflict {
                graph_id,
                expected,
                actual,
            } => write!(
                formatter,
                "orchestration conflict on graph {graph_id}: expected revision {expected}, found {actual}"
            ),
            Self::Tool(message) => write!(formatter, "tool error: {message}"),
            Self::Execution(message) => write!(formatter, "execution error: {message}"),
            Self::Secret(message) => write!(formatter, "secret error: {message}"),
            Self::Memory(message) => write!(formatter, "memory error: {message}"),
            Self::Skill(message) => write!(formatter, "skill error: {message}"),
            Self::Mcp(message) => write!(formatter, "MCP error: {message}"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::RuntimeOverloaded { limit } => {
                write!(formatter, "runtime concurrent Turn limit {limit} reached")
            }
            Self::CapabilityPanicked { phase } => {
                write!(formatter, "capability panicked during {phase:?}")
            }
            Self::ProtocolDenied { permission } => {
                write!(formatter, "protocol permission denied: {permission}")
            }
            Self::State(message) => write!(formatter, "state error: {message}"),
            Self::StateConflict {
                thread_id,
                expected,
                actual,
            } => write!(
                formatter,
                "state conflict on thread {thread_id}: expected stream version {expected}, found {actual}"
            ),
            Self::Trace(message) => write!(formatter, "trace error: {message}"),
            Self::MaxSteps(max) => write!(formatter, "agent loop exceeded {max} steps"),
            Self::Cancelled { phase } => {
                write!(formatter, "turn cancelled during {phase:?}")
            }
            Self::TimedOut { phase } => {
                write!(formatter, "turn timed out during {phase:?}")
            }
        }
    }
}

impl Error for HarnessError {}

#[must_use]
/// Returns the current Unix time in milliseconds, saturating on overflow.
pub fn now_ms() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}
