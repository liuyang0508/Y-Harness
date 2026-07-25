//! Headless, embeddable primitives for the Y-Harness Engineering runtime.
//!
//! The crate exposes kernel contracts, the Agent Loop, durable state,
//! provider-neutral context and memory capabilities, protocol transports, and
//! evidence exports. Application clients remain outside the semantic core.

#![warn(missing_docs)]

mod approval;
mod context;
mod evaluation;
mod execution;
mod isolation;
mod json;
mod kernel;
mod memory;
#[cfg(feature = "https-model")]
mod model;
mod observability;
mod orchestration;
mod protocol;
mod runtime;
mod secret;
mod skill;
mod sqlite;
mod state;
mod transport;
mod verification;

pub use approval::{
    APPROVAL_INBOX_SCHEMA_VERSION, ApprovalInbox, ApprovalMigrationReport, ApprovalMigrationStatus,
    ApprovalRecord, ApprovalRecordStatus, InboxApprovalHandler, MemoryApprovalInbox,
    SqliteApprovalInbox,
};
pub use context::{
    CONVERSATION_COMPACTOR_API_VERSION, ContextBlock, ContextCompilation, ContextEngine,
    ContextSource, ConversationCompactionConfig, ConversationCompactionRequest,
    ConversationCompactionResponse, ConversationCompactionTurn, ConversationCompactor,
    ConversationCompactorDescriptor, ConversationCompactorRegistry, ConversationContext,
    ConversationContextConfig, MemoryContextConfig, MemoryContextObservation, MemoryContextStatus,
    MemoryFailureMode, RegisteredConversationCompactor, RegisteredTokenCounter,
    TOKEN_COUNTER_API_VERSION, TokenCounter, TokenCounterDescriptor, TokenCounterRegistry,
};
pub use evaluation::{
    BaselineComparison, BaselineFailure, BaselineRequirement, EVALUATION_FORMAT_VERSION,
    EvaluationBaseline, EvaluationCase, EvaluationCaseReport, EvaluationEngine,
    EvaluationExecution, EvaluationReport, EvaluationSample, EvaluationSuite, EvaluationTarget,
    Grade, GradeOutcome, GradeRecord, Grader, GraderDescriptor, GraderRegistry, RegisteredGrader,
};
pub use execution::{
    CompensationContext, CompensationDescriptor, CompensationRequest, CompensationTool,
    DenyProcessBroker, JsonCommandModel, JsonCommandTool, JsonProcessConfig, JsonToolRequest,
    LocalProcessBroker, MacOsSeatbeltBroker, NetworkAccess, ProcessBroker, ProcessBrokerDescriptor,
    ProcessIsolation, ProcessOutput, ProcessRequest, ToolCompensator,
};
pub use kernel::{
    ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, ArtifactId, CancellationToken,
    CapabilityOrigin, Checkpoint, CheckpointId, EventId, ExecutionPhase, HarnessError,
    HarnessFuture, Item, ItemKind, MemoryContextRecordStatus, ModelContinuation, ModelEventSink,
    ModelOutput, ModelRegistry, ModelRequest, ModelResponse, ModelStream, ModelStreamEvent,
    ModelUsage, OperationId, PendingEvent, PolicyDecision, RegisteredModel, RegisteredTool,
    RiskLevel, StateEvent, StoredEvent, TaskGraphId, TaskId, TaskLeaseId, TaskMessageId, Thread,
    ThreadId, ToolAuthorization, ToolContext, ToolDescriptor, ToolRegistry, Turn, TurnId,
    TurnOutcome, TurnStatus, TurnStopReason, VerificationOutcome,
};
pub use memory::{
    AgentMemoryHubProvider, MEMORY_API_VERSION, MemoryBriefRequest, MemoryBriefResponse,
    MemoryContextPack, MemoryFeedbackRequest, MemoryHealth, MemoryHealthStatus, MemoryOperation,
    MemoryProvenance, MemoryProvider, MemoryProviderDescriptor, MemoryReadRequest,
    MemoryReadResponse, MemoryReference, MemoryRegistry, MemoryScope, MemorySearchRequest,
    MemorySearchResponse, MemoryView, MemoryWriteRequest, MemoryWriteResponse,
    RegisteredMemoryProvider,
};
#[cfg(feature = "https-model")]
pub use model::{
    HttpModelRequest, HttpModelResponse, HttpModelTransport, HttpsJsonModel, HttpsJsonModelConfig,
    OpenAiResponsesModel, OpenAiResponsesModelConfig, ReqwestHttpModelTransport,
};
pub use observability::{
    Observability, ObservationOutcome, Observer, PhaseObservation, RegisteredObserver,
    TraceCollector, export_jsonl,
};
pub use orchestration::{
    DenyWorkspaceProvider, GitWorktreeWorkspaceProvider, LocalDirectoryWorkspaceProvider,
    MemoryTaskCoordinator, Orchestrator, SqliteTaskCoordinator, TASK_GRAPH_SCHEMA_VERSION,
    TaskArtifact, TaskClaim, TaskCompletion, TaskCoordinator, TaskDefinition, TaskExecutionRequest,
    TaskExecutor, TaskGraph, TaskGraphSnapshot, TaskLease, TaskMailbox, TaskMessage,
    TaskMessagePage, TaskRecord, TaskStatus, TaskWorkspace, WORKSPACE_PROVIDER_API_VERSION,
    WorkspaceDisposition, WorkspaceLease, WorkspaceMode, WorkspaceProvider,
    WorkspaceProviderDescriptor, WorkspaceProvisioning, WorkspaceRequest,
};
pub use protocol::{
    CompatibilityManifest, FingerprintProtocolAuthorizer, OperationStatus, OperationStreamEvent,
    PROTOCOL_VERSION, ProtocolAuthorizer, ProtocolCommand, ProtocolError, ProtocolHandler,
    ProtocolPrincipal, ProtocolRequest, ProtocolResponse, ProtocolResponseBody, ProtocolResult,
    ProtocolShutdownReport, TaskGraphSummary, TaskRecordPage, serve_jsonl, serve_jsonl_as,
    serve_stdio,
};
pub use runtime::{
    AllowListPolicy, ApprovalHandler, DenyAllApprovals, HarnessRuntime, LanguageModel,
    PolicyEngine, Tool, TurnExecutionOptions,
};
pub use secret::{
    EnvironmentSecretProvider, RegisteredSecretProvider, SECRET_API_VERSION, SecretProvider,
    SecretProviderDescriptor, SecretReference, SecretRegistry, SecretRequest, SecretValue,
};
#[cfg(feature = "https-skill")]
pub use skill::{
    HttpSkillRequest, HttpSkillResponse, HttpSkillTransport, HttpsSkillSource,
    HttpsSkillSourceConfig, ReqwestHttpSkillTransport,
};
pub use skill::{
    RegisteredSkill, ResolvedSkill, ResolvedSkillSet, SKILL_API_VERSION, SignedSkillPackage,
    SkillDependency, SkillEngine, SkillId, SkillKeyRevocation, SkillManifest, SkillPackage,
    SkillPublisherPolicy, SkillRegistry, SkillSignature, SkillTransparencyReceipt,
    SkillTransparencyRequirement, SkillTrustStore, VerifiedSkillTransparency,
};
pub use state::{
    EventStore, MemoryEventStore, STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION,
    STATE_TERMINAL_EVENT_RESERVE, STATE_TERMINAL_RECOVERY_BYTE_RESERVE, STATE_THREAD_EVENT_LIMIT,
    STATE_THREAD_RECOVERY_BYTE_LIMIT, SnapshotMaintenanceConfig, SnapshotMaintenanceFailure,
    SnapshotMaintenanceStats, SqliteEventStore, StateCapacity, StateCapacityLevel, StateEngine,
    StateMigrationReport, StateMigrationStatus, StateSnapshot,
};
pub use transport::{
    McpClient, McpToolDescriptor, StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority,
    mcp_client, register_mcp_tools, register_selected_mcp_tools,
};
#[cfg(feature = "tls-host")]
pub use transport::{TlsJsonlServer, TlsJsonlServerConfig, TlsJsonlServerReport};
pub use verification::{
    RegisteredVerifier, VerificationRegistry, VerificationRequest, Verifier, VerifierDescriptor,
};

/// Exact HTTPS JSON model-gateway contract version.
pub const MODEL_GATEWAY_API_VERSION: &str = "3";
