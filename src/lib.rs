//! Headless, embeddable primitives for the Y-Harness Engineering runtime.
//!
//! The crate exposes kernel contracts, the Agent Loop, durable state,
//! provider-neutral context and memory capabilities, protocol transports, and
//! evidence exports. Application clients remain outside the semantic core.

#![warn(missing_docs)]

mod approval;
mod completion;
mod context;
mod effect;
mod evaluation;
mod execution;
mod human_handoff;
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
mod temporal;
mod transport;
mod verification;
mod workflow;

pub use approval::{
    APPROVAL_INBOX_SCHEMA_VERSION, ApprovalInbox, ApprovalMigrationReport, ApprovalMigrationStatus,
    ApprovalRecord, ApprovalRecordStatus, InboxApprovalHandler, MemoryApprovalInbox,
    SqliteApprovalInbox,
};
pub use completion::{
    COMPLETION_FORMAT_VERSION, CompletionAssurance, CompletionContract, CompletionGeneration,
    CompletionReceipt, CompletionRequirementStatus, CompletionVerifierBinding,
    MAX_COMPLETION_HASH_INPUT_BYTES, MAX_COMPLETION_RECEIPT_BYTES, build_completion_receipt,
    completion_execution_binding_sha256, completion_model_request_sha256,
    completion_model_route_sha256, completion_receipt_sha256, completion_runtime_governance_sha256,
    completion_tool_view_sha256, completion_verifier_binding_sha256,
    completion_verifier_manifest_sha256, validate_inherited_projected_turn_completion_receipt,
    validate_projected_turn_completion_receipt, validate_turn_completion_receipt,
};
pub use context::{
    CONVERSATION_COMPACTOR_API_VERSION, ContextBlock, ContextCompilation, ContextEngine,
    ContextSource, ConversationCompactionConfig, ConversationCompactionRequest,
    ConversationCompactionResponse, ConversationCompactionTurn, ConversationCompactor,
    ConversationCompactorDescriptor, ConversationCompactorRegistry, ConversationContext,
    ConversationContextConfig, MemoryContextConfig, MemoryContextObservation, MemoryContextStatus,
    MemoryFailureMode, RegisteredConversationCompactor, RegisteredTokenCounter,
    THREAD_HANDOFF_FORMAT_VERSION, TOKEN_COUNTER_API_VERSION, ThreadHandoffConfig,
    ThreadHandoffRequest, TokenCounter, TokenCounterDescriptor, TokenCounterRegistry,
    TurnContextInput,
};
pub use effect::{
    AllowListEffectExecutionPolicy, AllowListEffectReconciliationPolicy, DenyAllEffectExecutions,
    DenyAllEffectReconciliations, EFFECT_DISPATCH_GOVERNOR_API_VERSION,
    EFFECT_DISPATCH_GOVERNOR_SCHEMA_VERSION, EFFECT_EXECUTOR_API_VERSION,
    EFFECT_LEDGER_SCHEMA_VERSION, EFFECT_RECONCILER_API_VERSION, Effect, EffectApplyOutcome,
    EffectCommand, EffectCommandKind, EffectCommandResult, EffectConnector,
    EffectConnectorDescriptor, EffectConnectorRegistry, EffectCoordinator, EffectCreateRequest,
    EffectDispatchAdmissionDecision, EffectDispatchAdmissionRequest, EffectDispatchGovernor,
    EffectDispatchGovernorPolicy, EffectDispatchSettlement, EffectDueLease, EffectDueScanPage,
    EffectEngine, EffectExecutionDecision, EffectExecutionOutcome, EffectExecutionPolicy,
    EffectExecutionPolicyRequest, EffectExecutionRequest, EffectExecutor, EffectExecutorAttempt,
    EffectExecutorAttemptOutcome, EffectExecutorClock, EffectExecutorConfig,
    EffectExecutorRunReport, EffectExecutorRunRequest, EffectIdempotencyContract, EffectLease,
    EffectOperation, EffectPage, EffectPageCursor, EffectReceipt, EffectReconciler,
    EffectReconcilerAttempt, EffectReconcilerAttemptOutcome, EffectReconcilerClock,
    EffectReconcilerConfig, EffectReconcilerRunReport, EffectReconcilerRunRequest,
    EffectReconciliationConnector, EffectReconciliationConnectorDescriptor,
    EffectReconciliationConnectorRegistry, EffectReconciliationContract,
    EffectReconciliationDecision, EffectReconciliationOutcome, EffectReconciliationPolicy,
    EffectReconciliationPolicyRequest, EffectReconciliationRequest, EffectSnapshot, EffectStatus,
    EffectTransition, EffectTransitionKind, MemoryEffectCoordinator, MemoryEffectDispatchGovernor,
    RegisteredEffectConnector, RegisteredEffectReconciliationConnector, SqliteEffectCoordinator,
    SqliteEffectDispatchGovernor, SystemEffectExecutorClock, SystemEffectReconcilerClock,
};
pub use evaluation::{
    BaselineComparison, BaselineFailure, BaselineRequirement, EVALUATION_FORMAT_VERSION,
    EvaluationBaseline, EvaluationCase, EvaluationCaseReport, EvaluationEngine,
    EvaluationExecution, EvaluationReport, EvaluationSample, EvaluationSuite, EvaluationTarget,
    Grade, GradeOutcome, GradeRecord, Grader, GraderDescriptor, GraderRegistry, RegisteredGrader,
};
pub use execution::{
    CompensationContext, CompensationDescriptor, CompensationRequest, CompensationTool,
    DenyProcessBroker, DigestLockedProcessBroker, EffectSecretEnvironment,
    JSON_COMMAND_MAX_INPUT_BYTES, JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
    JSON_GRADER_MAX_INPUT_BYTES, JsonCommandConversationCompactor, JsonCommandEffectConnector,
    JsonCommandEffectReconciliationConnector, JsonCommandGrader, JsonCommandModel,
    JsonCommandModelProtocol, JsonCommandTool, JsonCommandVerifier,
    JsonConversationCompactionRequest, JsonConversationCompactionResponse,
    JsonEffectExecutionRequest, JsonEffectExecutionResponse, JsonEffectReconciliationRequest,
    JsonEffectReconciliationResponse, JsonGradeRequest, JsonGradeResponse, JsonModelSettlement,
    JsonProcessConfig, JsonToolRequest, JsonVerificationOutcome, JsonVerificationRequest,
    LocalProcessBroker, MAX_DIGEST_LOCKED_PROGRAM_BYTES, MAX_EFFECT_SECRET_ENVIRONMENT_ENTRIES,
    MacOsSeatbeltBroker, NetworkAccess, ProcessBroker, ProcessBrokerDescriptor,
    ProcessExecutableIntegrity, ProcessIsolation, ProcessOutput, ProcessRequest, ToolCompensator,
};
pub use human_handoff::{
    HUMAN_HANDOFF_SCHEMA_VERSION, HumanHandoff, HumanHandoffApplyOutcome, HumanHandoffClaim,
    HumanHandoffCommand, HumanHandoffCommandKind, HumanHandoffCommandResult,
    HumanHandoffCoordinator, HumanHandoffCreateRequest, HumanHandoffCursor, HumanHandoffDueClaim,
    HumanHandoffDueScanPage, HumanHandoffEngine, HumanHandoffPage, HumanHandoffSnapshot,
    HumanHandoffStatus, HumanHandoffSubject, HumanHandoffSubjectResolver, HumanHandoffTransition,
    HumanHandoffTransitionKind, MemoryHumanHandoffCoordinator, SqliteHumanHandoffCoordinator,
};
pub use kernel::{
    ActorIdentity, AgentLoopClaimId, AgentLoopCloseCommandId, AgentLoopDenyCommandId,
    AgentLoopExecution, AgentLoopResumeCommandId, AgentLoopWaitId, AgentLoopWorkerId,
    ApprovalActor, ApprovalDecision, ApprovalId, ApprovalRequest, ApprovalSettlementEvidence,
    ArtifactId, AuthorityContext, CancellationToken, CapabilityOrigin, Checkpoint, CheckpointId,
    ConnectorEvidence, ConnectorEvidenceClaim, EffectCommandId, EffectId, EffectLeaseId, EventId,
    ExecutionBinding, ExecutionClaimEvidence, ExecutionPhase, HarnessError, HarnessFuture,
    HumanHandoffClaimId, HumanHandoffCommandId, HumanHandoffId, InvocationContextEvidence, Item,
    ItemId, ItemKind, MAX_CONNECTOR_EVIDENCE_PER_RESULT, MAX_MODEL_PROVIDER_FAILURE_MESSAGE_BYTES,
    MAX_MODEL_PROVIDER_RETRY_AFTER_MS, MAX_TOOL_CALLS_PER_BATCH,
    MAX_TOOL_CANCELLATION_SETTLEMENT_TIMEOUT, MemoryContextRecordStatus, ModelContinuation,
    ModelEventSink, ModelOutput, ModelProviderFailure, ModelProviderFailureKind, ModelRegistry,
    ModelRequest, ModelResponse, ModelStream, ModelStreamEvent, ModelToolCall, ModelToolChoice,
    ModelToolTraceOutcome, ModelUsage, NewStreamEvent, OperationId, PendingEvent, PolicyDecision,
    RegisteredModel, RegisteredTool, ResumeEvidence, RiskLevel, StateEvent, SteeringId,
    StoredEvent, TaskGraphId, TaskId, TaskLeaseId, TaskMessageId, Thread, ThreadId,
    ThreadImportOrigin, ThreadLineage, ToolAuthorization, ToolBatchExecution, ToolCallBatch,
    ToolCallBatchId, ToolContext, ToolDescriptor, ToolExecutionResult, ToolRegistry, Turn, TurnId,
    TurnOutcome, TurnStatus, TurnStopReason, TurnWaitEnvelope, VerificationOutcome,
    WaitClosureEvidence, WaitDenialEvidence, WaitKind, WorkflowCommandId, WorkflowRunId,
    WorkflowSignalId, WorkflowWaitId,
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
    AnthropicMessagesModel, AnthropicMessagesModelConfig, ChatCompletionTokenLimitField,
    GeminiGenerateContentModel, GeminiGenerateContentModelConfig, HttpModelRequest,
    HttpModelResponse, HttpModelTransport, HttpsJsonModel, HttpsJsonModelConfig,
    OpenAiChatCompletionsModel, OpenAiChatCompletionsModelConfig, OpenAiResponsesModel,
    OpenAiResponsesModelConfig, ReqwestHttpModelTransport,
};
pub use observability::{
    Observability, ObservationOutcome, Observer, PhaseObservation, RegisteredObserver,
    TraceCollector, export_jsonl,
};
pub use orchestration::{
    DenyWorkspaceProvider, GitWorktreeWorkspaceProvider, LocalDirectoryWorkspaceProvider,
    MemoryTaskCoordinator, Orchestrator, SqliteTaskCoordinator, TASK_GRAPH_SCHEMA_VERSION,
    TaskArtifact, TaskAttemptBinding, TaskCapabilitySet, TaskClaim, TaskCompletion,
    TaskCoordinator, TaskDefinition, TaskExecutionRequest, TaskExecutor, TaskGraph,
    TaskGraphSnapshot, TaskLease, TaskMailbox, TaskMessage, TaskMessagePage, TaskMigrationReport,
    TaskMigrationStatus, TaskRecord, TaskStatus, TaskWorkspace, WORKSPACE_PROVIDER_API_VERSION,
    WorkspaceDisposition, WorkspaceLease, WorkspaceMode, WorkspaceProvider,
    WorkspaceProviderDescriptor, WorkspaceProvisioning, WorkspaceRequest,
};
pub use protocol::{
    ApprovalDeliveryAction, ApprovalDeliveryStatus, CompatibilityManifest, EffectListEntry,
    EffectListPage, EffectSummary, EffectTransitionPage, FingerprintProtocolAuthorizer,
    HumanHandoffQueuePage, HumanHandoffSummary, HumanHandoffTransitionPage, OperationStatus,
    OperationStreamEvent, PROTOCOL_VERSION, ProtocolAdmissionState, ProtocolAuthorizer,
    ProtocolCommand, ProtocolError, ProtocolHandler, ProtocolPrincipal, ProtocolRequest,
    ProtocolResponse, ProtocolResponseBody, ProtocolResult, ProtocolServiceStatus,
    ProtocolShutdownReport, RuntimeCatalog, RuntimeMcpCatalogEntry, RuntimeModelCatalogEntry,
    RuntimeSkillCatalogEntry, RuntimeSkillRegistryCatalogEntry, TaskGraphSummary, TaskRecordPage,
    TurnExecutionProjection, TurnExecutionState, WorkflowRunSummary, WorkflowTransitionPage,
    serve_jsonl, serve_jsonl_as, serve_jsonl_as_until_cancelled, serve_jsonl_until_cancelled,
    serve_stdio,
};
pub use runtime::{
    AllowListPolicy, ApprovalDeliveryOperation, ApprovalHandler, ApprovalWait, ApprovalWaitStatus,
    DEFAULT_MAX_FAILURE_CYCLE_REPETITIONS, DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP,
    DEFAULT_MAX_PARALLEL_TOOL_CALLS, DenyAllApprovals, HarnessRuntime, LanguageModel,
    MAX_FAILURE_CYCLE_REPETITIONS, MAX_MODEL_ATTEMPTS_PER_STEP, MAX_MODEL_RETRIES,
    MAX_MODEL_RETRY_DELAY_MS, MAX_PARALLEL_TOOL_CALLS, ModelRetryPolicy, PolicyEngine,
    ProgressPolicy, SteeringReceipt, Tool, TurnExecutionOptions, TurnExecutionResult,
    TurnRunProgress,
};
pub use secret::{
    EnvironmentSecretProvider, RegisteredSecretProvider, SECRET_API_VERSION, SecretEffectPhase,
    SecretProvider, SecretProviderDescriptor, SecretReference, SecretRegistry, SecretRequest,
    SecretServiceUse, SecretUseContext, SecretValue, TenantEnvironmentSecretProvider,
};
#[cfg(feature = "https-skill")]
pub use skill::{
    HttpSkillAuthorization, HttpSkillRequest, HttpSkillResponse, HttpSkillTransport,
    HttpsSkillSource, HttpsSkillSourceConfig, ReqwestHttpSkillTransport,
};
pub use skill::{
    RegisteredSkill, ResolvedSkill, ResolvedSkillSet, SKILL_API_VERSION, SignedSkillPackage,
    SkillDependency, SkillEngine, SkillId, SkillKeyRevocation, SkillManifest, SkillPackage,
    SkillPublisherPolicy, SkillRegistry, SkillSignature, SkillTransparencyReceipt,
    SkillTransparencyRequirement, SkillTrustStore, VerifiedSkillTransparency,
};
pub use state::{
    AGENT_LOOP_WAIT_PROJECTION_SCHEMA_VERSION, AgentLoopDueCursor, AgentLoopDuePhase,
    AgentLoopDueScanPage, AgentLoopDueWait, AgentLoopReadyClaimCommand, AgentLoopWaitCloseCommand,
    AgentLoopWaitStartCommand, EventAppendDisposition, EventAppendResult, EventStore,
    MAX_AGENT_LOOP_DUE_SCAN_LIMIT, MAX_AGENT_LOOP_WAIT_MS, MAX_THREAD_ARCHIVE_BYTES,
    MemoryEventStore, STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION,
    STATE_TERMINAL_EVENT_RESERVE, STATE_TERMINAL_RECOVERY_BYTE_RESERVE, STATE_THREAD_EVENT_LIMIT,
    STATE_THREAD_RECOVERY_BYTE_LIMIT, SnapshotMaintenanceConfig, SnapshotMaintenanceFailure,
    SnapshotMaintenanceStats, SqliteEventStore, StateCapacity, StateCapacityLevel, StateEngine,
    StateMigrationReport, StateMigrationStatus, StateSnapshot, THREAD_ARCHIVE_FORMAT_VERSION,
    ThreadArchive, ThreadSummary, ThreadSummaryPage, decode_thread_archive, encode_thread_archive,
};
pub use temporal::{
    MAX_TEMPORAL_SCAN_LIMIT, TEMPORAL_DRIVER_API_VERSION, TemporalAttempt, TemporalAttemptOutcome,
    TemporalDriver, TemporalScanProgress, TemporalTarget, TemporalTickCursor, TemporalTickReport,
    TemporalTickRequest,
};
#[cfg(feature = "http-probe")]
pub use transport::{
    HttpProbeServer, HttpProbeServerConfig, HttpProbeServerReport, ServiceStatusSource,
};
#[cfg(feature = "https-mcp")]
pub use transport::{HttpsJsonMcpClient, HttpsJsonMcpConfig};
pub use transport::{
    McpClient, McpToolDescriptor, StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority,
    mcp_client, register_mcp_tools, register_selected_mcp_tools,
};
#[cfg(feature = "tls-host")]
pub use transport::{TlsJsonlServer, TlsJsonlServerConfig, TlsJsonlServerReport};
pub use verification::{
    RegisteredVerifier, VerificationRegistry, VerificationRequest, Verifier, VerifierDescriptor,
};
pub use workflow::{
    MemoryWorkflowCoordinator, SqliteWorkflowCoordinator, WORKFLOW_RUN_SCHEMA_VERSION,
    WorkflowApplyOutcome, WorkflowCommand, WorkflowCommandKind, WorkflowCommandResult,
    WorkflowCoordinator, WorkflowCreateRequest, WorkflowDefinition, WorkflowDueScanPage,
    WorkflowDueWait, WorkflowEngine, WorkflowRun, WorkflowRunSnapshot, WorkflowStatus,
    WorkflowTransition, WorkflowTransitionKind, WorkflowWait, WorkflowWakeReason,
};

/// Exact HTTPS JSON model-gateway contract version.
pub const MODEL_GATEWAY_API_VERSION: &str = "7";

/// Exact model-cost scale used by [`ModelUsage`].
///
/// One US dollar equals ten billion ticks. Provider adapters must not round
/// an unavailable or more precise amount into this scale.
pub const MODEL_COST_USD_TICKS_PER_USD: u64 = 10_000_000_000;
