//! Microkernel-owned identities, contracts, and typed capability registries.

mod capability;
mod control;
mod registry;
mod types;

pub use capability::{LanguageModel, ModelEventSink, ModelStream, Tool};
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
    ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, ArtifactId, Checkpoint,
    CheckpointId, EventId, ExecutionPhase, HarnessError, HarnessFuture, Item, ItemKind,
    MemoryContextRecordStatus, ModelOutput, ModelRequest, ModelResponse, ModelStreamEvent,
    ModelUsage, OperationId, PendingEvent, PolicyDecision, RiskLevel, StateEvent, StoredEvent,
    TaskGraphId, TaskId, TaskLeaseId, TaskMessageId, Thread, ThreadId, ToolAuthorization,
    ToolContext, ToolDescriptor, Turn, TurnId, TurnOutcome, TurnStatus, TurnStopReason,
    VerificationOutcome,
};
