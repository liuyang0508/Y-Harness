//! Microkernel-owned identities, contracts, and typed capability registries.

mod capability;
mod control;
mod registry;
mod types;

pub use capability::{
    LanguageModel, MAX_TOOL_CANCELLATION_SETTLEMENT_TIMEOUT, ModelEventSink, ModelStream, Tool,
};
pub use control::CancellationToken;
pub use registry::{
    CapabilityOrigin, ModelRegistry, RegisteredModel, RegisteredTool, ToolRegistry,
};
pub(crate) use registry::{
    capture_capability_metadata, validate_capability_name, validate_capability_origin,
    validate_model_id, validate_registry_growth,
};
pub(crate) use types::now_ms;
pub use types::{
    ActorIdentity, ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, ArtifactId,
    AuthorityContext, Checkpoint, CheckpointId, ConnectorEvidence, ConnectorEvidenceClaim, EventId,
    ExecutionBinding, ExecutionPhase, HarnessError, HarnessFuture, HumanHandoffClaimId,
    HumanHandoffCommandId, HumanHandoffId, InvocationContextEvidence, Item, ItemId, ItemKind,
    MAX_CONNECTOR_EVIDENCE_PER_RESULT, MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES,
    MAX_MODEL_PROVIDER_RETRY_AFTER_MS, MAX_TOOL_CALLS_PER_BATCH, MemoryContextRecordStatus,
    ModelContinuation, ModelOutput, ModelProviderFailure, ModelProviderFailureKind, ModelRequest,
    ModelResponse, ModelStreamEvent, ModelToolCall, ModelUsage, NewStreamEvent, OperationId,
    PendingEvent, PolicyDecision, RiskLevel, StateEvent, SteeringId, StoredEvent, TaskGraphId,
    TaskId, TaskLeaseId, TaskMessageId, Thread, ThreadId, ThreadImportOrigin, ThreadLineage,
    ToolAuthorization, ToolBatchExecution, ToolCallBatch, ToolCallBatchId, ToolContext,
    ToolDescriptor, ToolExecutionResult, Turn, TurnId, TurnOutcome, TurnStatus, TurnStopReason,
    VerificationOutcome, WorkflowCommandId, WorkflowRunId, WorkflowSignalId, WorkflowWaitId,
};
