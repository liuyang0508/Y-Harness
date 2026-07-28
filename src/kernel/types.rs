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
const MAX_MODEL_CONTINUATION_ITEMS: usize = 64;
const MAX_MODEL_CONTINUATION_BYTES: usize = 1_048_576;
/// Maximum diagnostic text retained in one typed model-provider failure.
pub const MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES: usize = 4_096;
/// Maximum provider-requested retry delay retained as evidence.
pub const MAX_MODEL_PROVIDER_RETRY_AFTER_MS: u64 = 86_400_000;
/// Maximum Tool calls accepted from one Model response.
pub const MAX_TOOL_CALLS_PER_BATCH: usize = 64;

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
id_type!(SteeringId, "steering");
id_type!(ToolCallBatchId, "tool-batch");

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
    /// Immutable tenant boundary established by trusted creation authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tenant_id: Option<String>,
    /// Optional operator-authored display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Direct immutable ancestry when this Thread was forked.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ThreadLineage>,
    /// Immutable provenance when materialized from a portable archive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_origin: Option<ThreadImportOrigin>,
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
        Self::new_in_tenant(None)
    }

    pub(crate) fn new_in_tenant(tenant_id: Option<String>) -> Self {
        Self {
            id: ThreadId::generate(),
            tenant_id,
            name: None,
            lineage: None,
            import_origin: None,
            created_at_ms: now_ms(),
            turns: Vec::new(),
            checkpoints: Vec::new(),
        }
    }

    /// Returns the immutable tenant boundary, if this Thread is tenant-scoped.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Content-free provenance for one caller-supplied Turn context block.
pub struct InvocationContextEvidence {
    /// Stable caller-assigned source class.
    pub source: String,
    /// Opaque source-specific locator.
    pub reference: String,
    /// SHA-256 of the exact caller-supplied text.
    pub source_sha256: String,
    /// SHA-256 of the exact model-visible, provenance-prefixed block.
    pub content_sha256: String,
    /// Provider-specific token charge for the final block.
    pub estimated_tokens: usize,
    /// Exact UTF-8 bytes in the final model-visible block.
    pub serialized_bytes: usize,
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
    /// Durable external input accepted for a running Turn but not yet exposed
    /// to the Model.
    SteeringQueued {
        /// Runtime-generated correlation identity.
        steering_id: SteeringId,
        /// Authenticated actor established by the embedding host or transport.
        submitted_by: ActorIdentity,
        /// Bounded correction or additional instruction.
        content: String,
    },
    /// Steering input exposed to the Model at a protocol-safe Agent Loop
    /// boundary.
    SteeringApplied {
        /// Identity of the earlier durable queue record.
        steering_id: SteeringId,
        /// Exact text copied from the matching queue record.
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
    /// Opaque provider state required to continue a model response safely.
    ProviderContinuation {
        /// Registered model identity that produced the continuation.
        model_id: String,
        /// Trust-bearing origin of the registered model.
        model_origin: crate::CapabilityOrigin,
        /// Provider-formatted, non-executable continuation capsule.
        continuation: ModelContinuation,
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
        /// Same-response batch position for schema-7 multi-Tool decisions.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch: Option<ToolCallBatch>,
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
    /// Content-free evidence for caller-supplied non-authoritative context.
    InvocationContext {
        /// Authenticated actor that supplied the context with this Turn.
        submitted_by: ActorIdentity,
        /// Ordered provenance matching the model-visible context blocks.
        blocks: Vec<InvocationContextEvidence>,
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
/// Externally controlled execution phase.
///
/// Live Turn phases may enter durable stop evidence. Evaluation is an
/// out-of-loop process phase and is never written as Turn stop evidence.
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
    /// Offline or online evaluation grading outside the live Agent Loop.
    Evaluation,
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
/// Provider-neutral identity attributed to an authority-bearing Runtime action.
///
/// This value is attribution supplied by a trusted transport or embedding
/// host; constructing `Authenticated` does not authenticate its strings.
pub enum ActorIdentity {
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

impl ActorIdentity {
    pub(crate) fn validate_shape(&self, kind: &str) -> Result<(), HarnessError> {
        self.validate_shape_message(kind)
            .map_err(HarnessError::Approval)
    }

    fn validate_shape_message(&self, kind: &str) -> Result<(), String> {
        match self {
            Self::LocalProcess | Self::UnattributedLegacy => Ok(()),
            Self::Authenticated { authority, subject } => {
                validate_actor_identity(kind, "authority", authority)?;
                validate_actor_identity(kind, "subject", subject)
            }
        }
    }

    pub(crate) fn validate_current(&self, kind: &str) -> Result<(), HarnessError> {
        self.validate_current_message(kind)
            .map_err(HarnessError::Approval)
    }

    pub(crate) fn validate_current_state(&self, kind: &str) -> Result<(), HarnessError> {
        self.validate_current_message(kind)
            .map_err(HarnessError::State)
    }

    fn validate_current_message(&self, kind: &str) -> Result<(), String> {
        self.validate_shape_message(kind)?;
        if matches!(self, Self::UnattributedLegacy) {
            return Err(format!(
                "{kind} cannot use the legacy unattributed identity"
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Trusted execution identity and optional tenant boundary for one operation.
///
/// Embedding hosts and protocol authorizers must construct this value only
/// after authenticating the caller. The Runtime validates it before creating
/// Turn State, but constructing a value does not itself authenticate it.
pub struct AuthorityContext {
    /// Authenticated actor attributed to governed actions.
    actor: ActorIdentity,
    /// Optional case-sensitive tenant isolation identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
}

impl AuthorityContext {
    /// Creates and validates one authority context.
    pub fn new(actor: ActorIdentity, tenant_id: Option<String>) -> Result<Self, HarnessError> {
        let context = Self { actor, tenant_id };
        context.validate_current("authority context")?;
        Ok(context)
    }

    /// Returns trusted local-process authority without a tenant boundary.
    #[must_use]
    pub fn local_process() -> Self {
        Self {
            actor: ActorIdentity::LocalProcess,
            tenant_id: None,
        }
    }

    /// Returns the authenticated actor.
    #[must_use]
    pub fn actor(&self) -> &ActorIdentity {
        &self.actor
    }

    /// Returns the optional tenant isolation identity.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    pub(crate) fn validate_current(&self, kind: &str) -> Result<(), HarnessError> {
        self.actor.validate_current(kind)?;
        if let Some(tenant_id) = &self.tenant_id {
            validate_tenant_id(tenant_id)?;
        }
        Ok(())
    }

    pub(crate) fn validate_tenant(value: &str) -> Result<(), HarnessError> {
        validate_tenant_id(value)
    }
}

impl Default for AuthorityContext {
    fn default() -> Self {
        Self::local_process()
    }
}

/// Backwards-compatible name for the actor participating in an approval flow.
///
/// Approval is one use of the shared authenticated actor identity; Turn input
/// uses the same authority/subject coordinate without inventing another
/// principal type.
pub type ApprovalActor = ActorIdentity;

fn validate_actor_identity(kind: &str, field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(format!("{kind} {field} must be 1-256 non-control bytes"));
    }
    Ok(())
}

fn validate_tenant_id(value: &str) -> Result<(), HarnessError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(HarnessError::InvalidConfiguration(
            "tenant identity must be 1-128 case-sensitive ASCII letters, digits, '.', '_', '-' or ':'"
                .to_owned(),
        ));
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Same-response batch scheduling guarantee declared by a Tool implementation.
pub enum ToolBatchExecution {
    /// Never overlap this call with another call from the same Model decision.
    #[default]
    Sequential,
    /// The Tool guarantees semantic safety when overlapping any other
    /// `ParallelSafe` call in the same batch, including another call to itself.
    ParallelSafe,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Durable source position of one Tool call in a same-response batch.
pub struct ToolCallBatch {
    /// Runtime-generated identity shared by every call in the batch.
    pub id: ToolCallBatchId,
    /// Zero-based source position.
    pub index: usize,
    /// Total calls emitted by the Model decision.
    pub size: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// One provider-proposed Tool invocation within a Model decision.
pub struct ModelToolCall {
    /// Provider-generated correlation ID.
    pub call_id: String,
    /// Requested registered Tool name.
    pub name: String,
    /// Proposed JSON input.
    pub input: Value,
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
    /// Trusted identity and tenant boundary for this Tool call.
    pub authority: AuthorityContext,
    /// Per-call stop signal derived from owning Turn cancellation and deadline.
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
    /// Trusted Turn authority available only to in-process Harness adapters.
    ///
    /// This field is intentionally excluded from serialized provider payloads.
    #[serde(skip)]
    pub authority: AuthorityContext,
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
    /// Optional exact provider cost in [`crate::MODEL_COST_USD_TICKS_PER_USD`] ticks per USD.
    ///
    /// Adapters must omit this when the provider reports an incomplete cost or
    /// when conversion to this integer scale would require rounding.
    pub cost_usd_ticks: Option<u64>,
}

/// Bounded provider-formatted state needed to continue a model response.
///
/// The Harness treats these items as opaque data. A model adapter owns the
/// format-specific validation and replay rules; the Runtime binds the capsule
/// to the registered model identity and origin before it enters durable State.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelContinuation {
    format: String,
    items: Vec<Value>,
}

impl ModelContinuation {
    /// Creates a validated continuation capsule from ordered provider items.
    pub fn new(format: impl Into<String>, items: Vec<Value>) -> Result<Self, HarnessError> {
        let continuation = Self {
            format: format.into(),
            items,
        };
        continuation.validate()?;
        Ok(continuation)
    }

    /// Returns the provider-owned format coordinate.
    #[must_use]
    pub fn format(&self) -> &str {
        &self.format
    }

    /// Returns the ordered opaque provider items.
    #[must_use]
    pub fn items(&self) -> &[Value] {
        &self.items
    }

    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        let valid_format = !self.format.is_empty()
            && self.format.len() <= 64
            && self.format.chars().all(|character| {
                character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || matches!(character, '_' | '-' | '.')
            });
        if !valid_format {
            return Err(HarnessError::Model(
                "model continuation format must be 1-64 lowercase portable ASCII bytes".to_owned(),
            ));
        }
        if self.items.is_empty() || self.items.len() > MAX_MODEL_CONTINUATION_ITEMS {
            return Err(HarnessError::Model(format!(
                "model continuation must contain 1-{MAX_MODEL_CONTINUATION_ITEMS} items"
            )));
        }
        for item in &self.items {
            crate::json::validate_value_shape(item).map_err(|_| {
                HarnessError::Model(
                    "model continuation exceeds the supported JSON depth or node count".to_owned(),
                )
            })?;
        }
        crate::json::bounded_serialized_size(self, MAX_MODEL_CONTINUATION_BYTES).map_err(
            |failure| {
                HarnessError::Model(match failure {
                    crate::json::BoundedJsonError::LimitExceeded => {
                        format!("model continuation exceeds {MAX_MODEL_CONTINUATION_BYTES} bytes")
                    }
                    crate::json::BoundedJsonError::CannotEncode => {
                        "cannot encode model continuation".to_owned()
                    }
                })
            },
        )?;
        Ok(())
    }
}

/// Model decision plus optional provider evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelResponse {
    /// Decision consumed by the Agent Loop.
    pub output: ModelOutput,
    /// Provider-reported accounting; the Runtime never invents missing usage.
    pub usage: Option<ModelUsage>,
    /// Optional Provider-reported model identity that settled this call.
    ///
    /// This is evidence only. It never replaces the registered Model identity
    /// used for routing, Policy, continuation binding, or durable provenance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_model: Option<String>,
    /// Optional opaque provider request identity for support correlation.
    pub provider_request_id: Option<String>,
    /// Optional provider state that must precede this decision on replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ModelContinuation>,
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
    /// Invalidates provisional output from a superseded model step.
    StepInvalidated {
        /// One-based Agent Loop model-step number.
        model_step: u32,
    },
}

impl From<ModelOutput> for ModelResponse {
    fn from(output: ModelOutput) -> Self {
        Self {
            output,
            usage: None,
            provider_model: None,
            provider_request_id: None,
            continuation: None,
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
    /// Request several registered Tools from one Model response.
    ToolCalls {
        /// Calls in provider source order.
        calls: Vec<ModelToolCall>,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Immutable evidence identifying the exact parent journal prefix of a fork.
pub struct ThreadLineage {
    /// Direct parent Thread.
    pub parent_thread_id: ThreadId,
    /// Last global parent-journal sequence included in the child history.
    pub parent_through_sequence: u64,
    /// Number of parent-stream events included by the fork boundary.
    pub parent_stream_version: u64,
    /// SHA-256 of the exact ordered parent events through the boundary.
    pub parent_events_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Immutable evidence identifying one imported source Thread archive.
pub struct ThreadImportOrigin {
    /// Thread identity recorded by the source archive.
    pub source_thread_id: ThreadId,
    /// Number of source-stream events bound by the archive.
    pub source_stream_version: u64,
    /// Last global sequence observed in the source store.
    pub source_last_sequence: u64,
    /// SHA-256 of the exact ordered source Stored Events.
    pub source_events_sha256: String,
    /// Source fork ancestry retained as evidence, not local lineage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_lineage: Option<ThreadLineage>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Append-only events accepted by the State Engine projector.
pub enum StateEvent {
    /// Creates the thread stream.
    ThreadCreated {
        /// Thread creation time in Unix milliseconds.
        created_at_ms: u64,
        /// Trusted tenant boundary, absent for unscoped and legacy Threads.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_id: Option<String>,
    },
    /// Changes or clears the operator-authored Thread name.
    ThreadNamed {
        /// Canonical display name, or `None` to clear it.
        name: Option<String>,
    },
    /// Records the immutable source prefix used to create this Thread.
    ThreadForked {
        /// Direct parent and exact source-journal boundary.
        lineage: ThreadLineage,
    },
    /// Records the source archive used to materialize this Thread.
    ThreadImported {
        /// Exact source identity, boundary, digest, and optional ancestry.
        origin: ThreadImportOrigin,
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
    /// Atomically appends every Tool call from one Model response.
    ToolCallsAppended {
        /// Target turn.
        turn_id: TurnId,
        /// Ordered Tool-call items sharing one batch identity.
        calls: Vec<Item>,
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

#[derive(Clone, Debug, PartialEq)]
/// One event in a new stream that must be created atomically as a whole.
pub struct NewStreamEvent {
    /// Globally idempotent event identity.
    pub event_id: EventId,
    /// Schema coordinate carried by this event.
    ///
    /// New control events use the current writer schema. Copied immutable
    /// history retains its original supported schema coordinate.
    pub schema_version: u32,
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Provider-reported failure fact, independent from Runtime recovery policy.
pub enum ModelProviderFailureKind {
    /// Credentials were absent, invalid, or expired.
    Authentication,
    /// Valid credentials lacked permission for the request.
    Authorization,
    /// A request-rate limit was reached.
    RateLimited,
    /// The account or project exhausted an allocated usage quota.
    QuotaExhausted,
    /// The provider rejected the request as unsupported or invalid.
    RequestRejected,
    /// The requested provider model is unavailable.
    ModelUnavailable,
    /// Provider safety or content policy rejected the request.
    ContentPolicy,
    /// Provider capacity is temporarily overloaded.
    Overloaded,
    /// The provider returned an internal server failure.
    Server,
    /// Network transport failed before a valid response was received.
    Transport,
    /// A response violated the selected provider protocol.
    Protocol,
}

impl Display for ModelProviderFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Authentication => "authentication",
            Self::Authorization => "authorization",
            Self::RateLimited => "rate_limited",
            Self::QuotaExhausted => "quota_exhausted",
            Self::RequestRejected => "request_rejected",
            Self::ModelUnavailable => "model_unavailable",
            Self::ContentPolicy => "content_policy",
            Self::Overloaded => "overloaded",
            Self::Server => "server",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
        };
        formatter.write_str(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Bounded typed evidence returned by a model-provider adapter.
///
/// The adapter must remove response bodies, secrets, and untrusted control
/// characters before construction. Runtime enforces structural bounds but
/// cannot recognize secret material.
pub struct ModelProviderFailure {
    kind: ModelProviderFailureKind,
    message: String,
    http_status: Option<u16>,
    retry_after_ms: Option<u64>,
}

impl ModelProviderFailure {
    /// Creates and validates one provider failure from an adapter-sanitized diagnostic.
    pub fn new(
        kind: ModelProviderFailureKind,
        message: impl Into<String>,
        http_status: Option<u16>,
        retry_after_ms: Option<u64>,
    ) -> Result<Self, HarnessError> {
        let failure = Self {
            kind,
            message: message.into(),
            http_status,
            retry_after_ms,
        };
        failure.validate()?;
        Ok(failure)
    }

    /// Returns the stable failure class.
    #[must_use]
    pub const fn kind(&self) -> ModelProviderFailureKind {
        self.kind
    }

    /// Returns the bounded human-readable diagnostic.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the provider HTTP status when one was received.
    #[must_use]
    pub const fn http_status(&self) -> Option<u16> {
        self.http_status
    }

    /// Returns the provider-requested retry delay when explicitly reported.
    #[must_use]
    pub const fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }

    /// Revalidates evidence at an executable capability boundary.
    pub fn validate(&self) -> Result<(), HarnessError> {
        if self.message.trim().is_empty()
            || self.message.len() > MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES
            || self.message.chars().any(char::is_control)
        {
            return Err(HarnessError::InvalidCapability(format!(
                "model Provider failure message must be 1-{MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES} non-control bytes"
            )));
        }
        if self
            .http_status
            .is_some_and(|status| !(100..=599).contains(&status))
        {
            return Err(HarnessError::InvalidCapability(
                "model Provider HTTP status must be 100-599".to_owned(),
            ));
        }
        if self
            .retry_after_ms
            .is_some_and(|delay| delay == 0 || delay > MAX_MODEL_PROVIDER_RETRY_AFTER_MS)
        {
            return Err(HarnessError::InvalidCapability(format!(
                "model Provider retry-after must be 1-{MAX_MODEL_PROVIDER_RETRY_AFTER_MS} milliseconds"
            )));
        }
        Ok(())
    }
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
    /// Legacy model contract or unclassified provider failure.
    Model(String),
    /// Typed bounded model-provider failure evidence.
    ModelProvider(ModelProviderFailure),
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
    /// One Agent Loop step exhausted its Model Provider-call budget.
    MaxModelAttempts(usize),
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
            Self::ModelProvider(failure) => {
                write!(
                    formatter,
                    "model Provider error ({}): {}",
                    failure.kind, failure.message
                )?;
                if let Some(status) = failure.http_status {
                    write!(formatter, " [HTTP {status}]")?;
                }
                if let Some(delay) = failure.retry_after_ms {
                    write!(formatter, " [retry after {delay} ms]")?;
                }
                Ok(())
            }
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
            Self::MaxModelAttempts(max) => {
                write!(formatter, "model step exceeded {max} Provider attempts")
            }
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        ActorIdentity, AuthorityContext, HarnessError, MAX_MODEL_CONTINUATION_BYTES,
        MAX_MODEL_CONTINUATION_ITEMS, MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES,
        MAX_MODEL_PROVIDER_RETRY_AFTER_MS, ModelContinuation, ModelProviderFailure,
        ModelProviderFailureKind, ModelRequest, ModelUsage, ThreadId, TurnId,
    };

    #[test]
    fn model_provider_failure_is_typed_bounded_evidence() {
        let failure = ModelProviderFailure::new(
            ModelProviderFailureKind::RateLimited,
            "provider rate limit reached",
            Some(429),
            Some(2_000),
        )
        .expect("typed failure");
        assert_eq!(failure.kind(), ModelProviderFailureKind::RateLimited);
        assert_eq!(failure.http_status(), Some(429));
        assert_eq!(failure.retry_after_ms(), Some(2_000));
        assert_eq!(
            HarnessError::ModelProvider(failure).to_string(),
            "model Provider error (rate_limited): provider rate limit reached [HTTP 429] [retry after 2000 ms]"
        );

        assert!(
            ModelProviderFailure::new(
                ModelProviderFailureKind::Protocol,
                "x".repeat(MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES + 1),
                None,
                None,
            )
            .is_err()
        );
        assert!(
            ModelProviderFailure::new(
                ModelProviderFailureKind::Server,
                "server failure",
                Some(99),
                None,
            )
            .is_err()
        );
        assert!(
            ModelProviderFailure::new(
                ModelProviderFailureKind::Overloaded,
                "provider overloaded",
                Some(503),
                Some(MAX_MODEL_PROVIDER_RETRY_AFTER_MS + 1),
            )
            .is_err()
        );
    }

    #[test]
    fn model_continuation_enforces_format_count_and_size_bounds() {
        ModelContinuation::new("provider.reasoning-v1", vec![json!({"opaque": "state"})])
            .expect("valid continuation");

        assert!(ModelContinuation::new("Provider/V1", vec![json!({})]).is_err());
        assert!(ModelContinuation::new("provider.v1", Vec::new()).is_err());
        assert!(
            ModelContinuation::new(
                "provider.v1",
                vec![Value::Null; MAX_MODEL_CONTINUATION_ITEMS + 1],
            )
            .is_err()
        );
        assert!(
            ModelContinuation::new(
                "provider.v1",
                vec![Value::String("x".repeat(MAX_MODEL_CONTINUATION_BYTES))],
            )
            .is_err()
        );
    }

    #[test]
    fn model_usage_serializes_exact_cost_ticks_without_reinterpreting_legacy_cost() {
        let usage = ModelUsage {
            input_tokens: 10,
            output_tokens: 2,
            cached_input_tokens: 3,
            reasoning_tokens: 1,
            cost_usd_ticks: Some(12_345),
        };
        let encoded = serde_json::to_value(&usage).expect("serialize usage");
        assert_eq!(encoded["cost_usd_ticks"], 12_345);
        assert!(encoded.get("cost_microusd").is_none());

        let legacy: ModelUsage = serde_json::from_value(json!({
            "input_tokens": 10,
            "output_tokens": 2,
            "cached_input_tokens": 3,
            "reasoning_tokens": 1,
            "cost_microusd": 12_345
        }))
        .expect("legacy cost is safely ignored");
        assert_eq!(legacy.cost_usd_ticks, None);
    }

    #[test]
    fn model_request_keeps_authority_out_of_provider_payloads() {
        let authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "caller".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority");
        let request = ModelRequest {
            thread_id: ThreadId::from_static("thread"),
            turn_id: TurnId::from_static("turn"),
            authority,
            items: Vec::new(),
            context: Vec::new(),
            tools: Vec::new(),
        };

        let encoded = serde_json::to_value(&request).expect("serialize request");
        assert!(encoded.get("authority").is_none());
        let decoded: ModelRequest =
            serde_json::from_value(encoded).expect("deserialize provider payload");
        assert_eq!(decoded.authority, AuthorityContext::local_process());
    }
}
