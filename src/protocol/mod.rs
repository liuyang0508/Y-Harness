//! Versioned runtime commands, asynchronous operations, and bounded JSONL stdio.

mod task;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{self, AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    sync::{Mutex, Notify},
    time::{Instant, timeout_at},
};

use crate::isolation::isolate_future;
use crate::{
    APPROVAL_INBOX_SCHEMA_VERSION, ActorIdentity, ApprovalDecision, ApprovalId, ApprovalInbox,
    ApprovalRecord, AuthorityContext, CONVERSATION_COMPACTOR_API_VERSION, CancellationToken,
    HarnessError, HarnessRuntime, MEMORY_API_VERSION, MODEL_GATEWAY_API_VERSION, MemoryScope,
    ModelEventSink, ModelStreamEvent, OperationId, SECRET_API_VERSION, SKILL_API_VERSION,
    STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION, StateCapacity, SteeringId,
    StoredEvent, TASK_GRAPH_SCHEMA_VERSION, TOKEN_COUNTER_API_VERSION, TaskClaim, TaskCompletion,
    TaskCoordinator, TaskDefinition, TaskGraphId, TaskId, TaskLeaseId, TaskMessage,
    TaskMessagePage, Thread, ThreadId, ThreadSummary, TurnContextInput, TurnExecutionOptions,
    TurnId, WORKSPACE_PROVIDER_API_VERSION,
};

pub use task::{TaskGraphSummary, TaskRecordPage};
use task::{TaskProtocolService, TaskWorkerAccess};

/// Current Y-Harness client protocol version.
pub const PROTOCOL_VERSION: &str = "22";

const MAX_REQUEST_FRAME_BYTES: usize = 2_097_152;
const MAX_RESPONSE_FRAME_BYTES: usize = 16_777_216;
const MAX_PROTOCOL_EVENT_CONTENT_BYTES: usize = MAX_RESPONSE_FRAME_BYTES - 65_536;
const EVENT_FETCH_BATCH: usize = 2;
const MAX_PROMPT_BYTES: usize = 1_048_576;
const MAX_ERROR_CHARS: usize = 4_096;
const MAX_IDENTIFIER_BYTES: usize = 256;
const DEFAULT_OPERATION_RETENTION_LIMIT: usize = 64;
const MAX_OPERATION_RETENTION_LIMIT: usize = 4_096;
const MAX_OPERATION_STREAM_EVENTS: usize = 4_096;
const MAX_OPERATION_STREAM_BYTES: usize = 1_048_576;
const DEFAULT_OPERATION_EVENT_PAGE: usize = 16;
const MAX_OPERATION_EVENT_PAGE: usize = 32;
const DEFAULT_EVENT_PAGE: usize = 16;
const MAX_EVENT_PAGE: usize = 32;
const DEFAULT_THREAD_PAGE: usize = 16;
const DEFAULT_APPROVAL_PAGE: usize = 8;
const MAX_APPROVAL_PAGE: usize = 16;
const DEFAULT_TASK_RECORD_PAGE: usize = 16;
const DEFAULT_TASK_CLAIM_MAXIMUM: usize = 1;
const DEFAULT_TASK_MESSAGE_PAGE: usize = 32;
const MAX_PROTOCOL_PRINCIPALS: usize = 4_096;
const MAX_OPERATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3_600);
const DEFAULT_OPERATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_PROTOCOL_PERMISSIONS: usize = 64;

const PROTOCOL_PERMISSIONS: [&str; 27] = [
    "initialize",
    "operation.cancel",
    "operation.events",
    "operation.forget",
    "operation.get",
    "thread.capacity",
    "thread.create",
    "thread.events",
    "thread.fork",
    "thread.get",
    "thread.list",
    "thread.name",
    "thread.recover",
    "turn.start",
    "turn.steer",
    "approval.get",
    "approval.pending",
    "approval.settle",
    "task.graph.cancel",
    "task.graph.create",
    "task.graph.get",
    "task.worker.claim",
    "task.worker.complete",
    "task.worker.fail",
    "task.worker.heartbeat",
    "task.worker.messages.read",
    "task.worker.messages.send",
];

/// One correlated protocol request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolRequest {
    /// Caller-generated correlation identity.
    pub id: String,
    /// Exact requested protocol version.
    pub protocol_version: String,
    /// Typed command.
    pub command: ProtocolCommand,
}

/// Commands supported by the headless Runtime service.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProtocolCommand {
    /// Negotiates the exact protocol and returns server capabilities.
    Initialize {},
    /// Creates a new Thread.
    CreateThread {},
    /// Forks one terminal parent boundary under a caller-chosen child identity.
    ForkThread {
        /// Existing parent Thread identity.
        parent_thread_id: String,
        /// New child Thread identity and retry key.
        child_thread_id: String,
        /// Optional terminal Turn boundary; absent means the latest settled parent.
        through_turn_id: Option<String>,
    },
    /// Lists recent Threads without loading their histories.
    ListThreads {
        /// Exclusive global sequence cursor for an older page.
        before_sequence: Option<u64>,
        /// Optional page size; the Engine enforces its finite maximum.
        limit: Option<usize>,
    },
    /// Changes or clears one durable operator-authored Thread name.
    SetThreadName {
        /// Opaque Thread identity.
        thread_id: String,
        /// Trimmed display name, or `None` to clear it.
        name: Option<String>,
    },
    /// Loads one projected Thread.
    GetThread {
        /// Opaque Thread identity.
        thread_id: String,
    },
    /// Takes over one Thread after the caller has established that its
    /// previous worker is no longer live.
    RecoverThread {
        /// Opaque Thread identity.
        thread_id: String,
        /// Exact abandoned running Turn observed by the takeover authority.
        expected_turn_id: String,
    },
    /// Reads finite journal capacity and pressure for one Thread.
    GetThreadCapacity {
        /// Opaque Thread identity.
        thread_id: String,
    },
    /// Starts one asynchronous Turn operation.
    StartTurn {
        /// Existing Thread identity.
        thread_id: String,
        /// User input.
        prompt: String,
        /// Optional memory isolation scope.
        #[serde(default)]
        memory_scope: MemoryScope,
        /// Optional non-authoritative reference context for this Turn.
        #[serde(default)]
        context: Vec<TurnContextInput>,
        /// Optional total external-work deadline.
        timeout_ms: Option<u64>,
    },
    /// Queues durable additional input for one exact active Turn.
    SteerTurn {
        /// Existing Thread identity.
        thread_id: String,
        /// Exact active Turn observed by the caller.
        expected_turn_id: String,
        /// Correction or additional instruction.
        content: String,
    },
    /// Polls one asynchronous operation.
    GetOperation {
        /// Opaque operation identity.
        operation_id: String,
    },
    /// Reads bounded provisional model events for one operation.
    GetOperationEvents {
        /// Opaque operation identity.
        operation_id: String,
        /// Returns events strictly after this process-local sequence.
        after_sequence: Option<u64>,
        /// Maximum number of events to return.
        limit: Option<usize>,
    },
    /// Requests cooperative cancellation of a running operation.
    CancelOperation {
        /// Opaque operation identity.
        operation_id: String,
    },
    /// Removes one terminal operation from process-local retention.
    ForgetOperation {
        /// Opaque operation identity.
        operation_id: String,
    },
    /// Reads authoritative ordered events for one Thread.
    GetEvents {
        /// Opaque Thread identity.
        thread_id: String,
        /// Returns events strictly after this durable global sequence.
        after_sequence: Option<u64>,
        /// Maximum number of events to return.
        limit: Option<usize>,
    },
    /// Reads the oldest pending durable approvals.
    GetPendingApprovals {
        /// Maximum records to return.
        limit: Option<usize>,
    },
    /// Loads one durable approval record.
    GetApproval {
        /// Opaque approval identity.
        approval_id: String,
    },
    /// Atomically settles one pending durable approval.
    SettleApproval {
        /// Opaque approval identity.
        approval_id: String,
        /// Revision observed by the approver.
        expected_revision: u64,
        /// Immutable approval decision.
        decision: ApprovalDecision,
    },
    /// Creates one durable Task Graph under a caller-chosen recovery identity.
    CreateTaskGraph {
        /// Stable graph identity used for retries and recovery.
        graph_id: String,
        /// Complete immutable Task DAG.
        definitions: Vec<TaskDefinition>,
    },
    /// Reads bounded metadata for one Task Graph.
    GetTaskGraph {
        /// Stable graph identity.
        graph_id: String,
    },
    /// Reads a bounded Task record page in Task identity order.
    GetTaskRecords {
        /// Stable graph identity.
        graph_id: String,
        /// Returns records strictly after this Task identity.
        after_task_id: Option<String>,
        /// Maximum number of records to return.
        limit: Option<usize>,
    },
    /// Cancels one Task against an explicitly observed graph revision.
    CancelTask {
        /// Stable graph identity.
        graph_id: String,
        /// Target Task identity.
        task_id: String,
        /// Revision observed by the operator.
        expected_revision: u64,
        /// Bounded cancellation reason.
        reason: String,
    },
    /// Claims ready Tasks for the authenticated worker principal.
    ClaimTasks {
        /// Stable graph identity.
        graph_id: String,
        /// Requested lease duration using the server clock.
        lease_duration_ms: u64,
        /// Maximum ready Tasks to claim.
        maximum: Option<usize>,
    },
    /// Extends one current authenticated worker lease.
    HeartbeatTask {
        /// Stable graph identity.
        graph_id: String,
        /// Running Task identity.
        task_id: String,
        /// Current fencing-token identity.
        lease_id: String,
        /// Requested lease duration using the server clock.
        lease_duration_ms: u64,
    },
    /// Settles one current authenticated worker lease successfully.
    CompleteTask {
        /// Stable graph identity.
        graph_id: String,
        /// Running Task identity.
        task_id: String,
        /// Current fencing-token identity.
        lease_id: String,
        /// Validated output references and summary.
        completion: TaskCompletion,
    },
    /// Settles one current authenticated worker lease as failed.
    FailTask {
        /// Stable graph identity.
        graph_id: String,
        /// Running Task identity.
        task_id: String,
        /// Current fencing-token identity.
        lease_id: String,
        /// Bounded failure reason.
        reason: String,
    },
    /// Reads the authenticated worker's bounded Task inbox.
    GetTaskMessages {
        /// Stable graph identity.
        graph_id: String,
        /// Running Task identity.
        task_id: String,
        /// Current fencing-token identity.
        lease_id: String,
        /// Returns messages strictly after this graph-local sequence.
        after_sequence: Option<u64>,
        /// Maximum number of messages to return.
        limit: Option<usize>,
    },
    /// Sends one message from the authenticated worker's running Task.
    SendTaskMessage {
        /// Stable graph identity.
        graph_id: String,
        /// Running sender Task identity.
        task_id: String,
        /// Current fencing-token identity.
        lease_id: String,
        /// Receiving Task identity.
        to: String,
        /// Bounded message body.
        body: String,
    },
}

impl ProtocolCommand {
    fn permission(&self) -> &'static str {
        match self {
            Self::Initialize {} => "initialize",
            Self::CreateThread {} => "thread.create",
            Self::ForkThread { .. } => "thread.fork",
            Self::ListThreads { .. } => "thread.list",
            Self::SetThreadName { .. } => "thread.name",
            Self::GetThread { .. } => "thread.get",
            Self::RecoverThread { .. } => "thread.recover",
            Self::GetThreadCapacity { .. } => "thread.capacity",
            Self::StartTurn { .. } => "turn.start",
            Self::SteerTurn { .. } => "turn.steer",
            Self::GetOperation { .. } => "operation.get",
            Self::GetOperationEvents { .. } => "operation.events",
            Self::CancelOperation { .. } => "operation.cancel",
            Self::ForgetOperation { .. } => "operation.forget",
            Self::GetEvents { .. } => "thread.events",
            Self::GetPendingApprovals { .. } => "approval.pending",
            Self::GetApproval { .. } => "approval.get",
            Self::SettleApproval { .. } => "approval.settle",
            Self::CreateTaskGraph { .. } => "task.graph.create",
            Self::GetTaskGraph { .. } | Self::GetTaskRecords { .. } => "task.graph.get",
            Self::CancelTask { .. } => "task.graph.cancel",
            Self::ClaimTasks { .. } => "task.worker.claim",
            Self::HeartbeatTask { .. } => "task.worker.heartbeat",
            Self::CompleteTask { .. } => "task.worker.complete",
            Self::FailTask { .. } => "task.worker.fail",
            Self::GetTaskMessages { .. } => "task.worker.messages.read",
            Self::SendTaskMessage { .. } => "task.worker.messages.send",
        }
    }
}

/// Authenticated identity supplied by a trusted protocol transport.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolPrincipal {
    /// Caller sharing the hosting process boundary, such as stdio.
    LocalProcess,
    /// SHA-256 fingerprint of a mutually authenticated client leaf certificate.
    MtlsCertificate {
        /// Lowercase hexadecimal SHA-256 certificate fingerprint.
        sha256: String,
    },
}

impl ProtocolPrincipal {
    /// Derives a stable principal from the exact client leaf certificate DER.
    #[must_use]
    pub fn from_mtls_certificate(certificate_der: &[u8]) -> Self {
        let digest = Sha256::digest(certificate_der);
        let mut fingerprint = String::with_capacity(64);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(fingerprint, "{byte:02x}");
        }
        Self::MtlsCertificate {
            sha256: fingerprint,
        }
    }

    /// Returns the mTLS certificate fingerprint when this is a remote principal.
    #[must_use]
    pub fn mtls_sha256(&self) -> Option<&str> {
        match self {
            Self::LocalProcess => None,
            Self::MtlsCertificate { sha256 } => Some(sha256),
        }
    }

    fn actor_identity(&self) -> ActorIdentity {
        match self {
            Self::LocalProcess => ActorIdentity::LocalProcess,
            Self::MtlsCertificate { sha256 } => ActorIdentity::Authenticated {
                authority: "mtls-certificate-sha256".to_owned(),
                subject: sha256.clone(),
            },
        }
    }

    fn authority_context(&self) -> Result<AuthorityContext, HarnessError> {
        AuthorityContext::new(self.actor_identity(), None)
    }

    fn task_worker_identity(&self) -> String {
        match self {
            Self::LocalProcess => "local-process".to_owned(),
            Self::MtlsCertificate { sha256 } => sha256.clone(),
        }
    }
}

/// Synchronous fail-closed authorization policy for typed protocol commands.
///
/// Implementations must be non-blocking. Panics are caught and treated as
/// denial before command execution.
pub trait ProtocolAuthorizer: Send + Sync {
    /// Returns whether one authenticated principal has one exact permission.
    fn allows(&self, principal: &ProtocolPrincipal, permission: &str) -> bool;

    /// Resolves the trusted Runtime authority for one transport principal.
    ///
    /// Implementations may map a certificate identity to a user and tenant.
    /// The default preserves the transport identity without a tenant scope.
    fn authority_context(
        &self,
        principal: &ProtocolPrincipal,
    ) -> Result<AuthorityContext, HarnessError> {
        principal.authority_context()
    }
}

struct LocalProcessAuthorizer;

impl ProtocolAuthorizer for LocalProcessAuthorizer {
    fn allows(&self, principal: &ProtocolPrincipal, _permission: &str) -> bool {
        matches!(principal, ProtocolPrincipal::LocalProcess)
    }
}

/// Exact certificate-fingerprint allow-list for network protocol permissions.
pub struct FingerprintProtocolAuthorizer {
    grants: BTreeMap<String, BTreeSet<String>>,
    allow_local_process: bool,
}

impl FingerprintProtocolAuthorizer {
    /// Validates exact fingerprint-to-permission grants.
    pub fn new(grants: BTreeMap<String, BTreeSet<String>>) -> Result<Self, HarnessError> {
        if grants.is_empty() || grants.len() > MAX_PROTOCOL_PRINCIPALS {
            return Err(HarnessError::InvalidConfiguration(format!(
                "protocol grants must contain 1-{MAX_PROTOCOL_PRINCIPALS} principals"
            )));
        }
        for (fingerprint, permissions) in &grants {
            validate_certificate_fingerprint(fingerprint)?;
            if permissions.is_empty() || permissions.len() > MAX_PROTOCOL_PERMISSIONS {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "protocol principal permissions must contain 1-{MAX_PROTOCOL_PERMISSIONS} entries"
                )));
            }
            if permissions
                .iter()
                .any(|permission| !PROTOCOL_PERMISSIONS.contains(&permission.as_str()))
            {
                return Err(HarnessError::InvalidConfiguration(
                    "protocol grants contain an unknown permission".to_owned(),
                ));
            }
        }
        Ok(Self {
            grants,
            allow_local_process: false,
        })
    }

    /// Grants every current protocol permission to exact fingerprints.
    pub fn allow_all(fingerprints: impl IntoIterator<Item = String>) -> Result<Self, HarnessError> {
        let permissions = PROTOCOL_PERMISSIONS
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let grants = fingerprints
            .into_iter()
            .map(|fingerprint| (fingerprint, permissions.clone()))
            .collect();
        Self::new(grants)
    }

    /// Selects whether the same policy also trusts local-process callers.
    #[must_use]
    pub fn with_local_process(mut self, allowed: bool) -> Self {
        self.allow_local_process = allowed;
        self
    }
}

impl ProtocolAuthorizer for FingerprintProtocolAuthorizer {
    fn allows(&self, principal: &ProtocolPrincipal, permission: &str) -> bool {
        match principal {
            ProtocolPrincipal::LocalProcess => self.allow_local_process,
            ProtocolPrincipal::MtlsCertificate { sha256 } => self
                .grants
                .get(sha256)
                .is_some_and(|permissions| permissions.contains(permission)),
        }
    }
}

/// One correlated protocol response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProtocolResponse {
    /// Request correlation identity, absent only for an unreadable frame.
    pub id: Option<String>,
    /// Server protocol version.
    pub protocol_version: String,
    /// Success payload or normalized error.
    pub body: ProtocolResponseBody,
}

/// Top-level response settlement.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "inline typed results are a public pattern-matching contract, not a retained hot-path collection"
)]
pub enum ProtocolResponseBody {
    /// Command completed successfully.
    Success {
        /// Typed result.
        result: ProtocolResult,
    },
    /// Command or frame was rejected.
    Error {
        /// Normalized client-safe error.
        error: ProtocolError,
    },
}

/// Typed successful command results.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProtocolResult {
    /// Initialization settlement.
    Initialized {
        /// Stable server product identity.
        server: String,
        /// Supported command capability names.
        capabilities: Vec<String>,
        /// Exact engine and persistence compatibility coordinates.
        compatibility: CompatibilityManifest,
    },
    /// Newly created Thread.
    ThreadCreated {
        /// Projected Thread.
        thread: Thread,
    },
    /// Atomically created or idempotently recovered fork child.
    ThreadForked {
        /// Independent child Thread with immutable direct lineage.
        thread: Thread,
    },
    /// Bounded recent-Thread page.
    Threads {
        /// Content-free recent Thread summaries.
        threads: Vec<ThreadSummary>,
        /// Exclusive sequence cursor for an older page.
        next_before_sequence: Option<u64>,
        /// Whether an older Thread was observed.
        has_more: bool,
    },
    /// Durable Thread name mutation settlement.
    ThreadNamed {
        /// Name accepted by the Engine, or `None` when cleared.
        name: Option<String>,
    },
    /// Loaded Thread or explicit absence.
    Thread {
        /// Projected Thread when found.
        thread: Option<Thread>,
    },
    /// Explicit exclusive-takeover settlement.
    ThreadRecovered {
        /// Recovered Thread, or `None` when the identity does not exist.
        thread: Option<Thread>,
    },
    /// Journal pressure before the finite Thread boundary.
    ThreadCapacity {
        /// Capacity projection for an existing Thread.
        capacity: StateCapacity,
    },
    /// Newly started asynchronous operation.
    TurnStarted {
        /// Pollable operation identity.
        operation_id: OperationId,
    },
    /// Durable steering submission acknowledgement.
    TurnSteered {
        /// Runtime-generated steering identity.
        steering_id: SteeringId,
        /// Exact Turn that accepted the input.
        turn_id: TurnId,
    },
    /// Current operation projection.
    Operation {
        /// Operation state.
        operation: OperationStatus,
    },
    /// Bounded provisional model events for one operation.
    OperationEvents {
        /// Retained events in process-local sequence order.
        events: Vec<OperationStreamEvent>,
        /// Cursor to pass as `after_sequence` for the following page.
        next_after_sequence: Option<u64>,
        /// Whether at least one later retained event exists.
        has_more: bool,
        /// Highest sequence irreversibly evicted before this read.
        dropped_through_sequence: Option<u64>,
    },
    /// Cancellation request acknowledgement.
    Cancellation {
        /// Target operation identity.
        operation_id: OperationId,
        /// Whether a running operation received the signal.
        accepted: bool,
    },
    /// Terminal operation retention was released.
    OperationForgotten {
        /// Removed operation identity.
        operation_id: OperationId,
    },
    /// Authoritative ordered State events.
    Events {
        /// Stored events.
        events: Vec<StoredEvent>,
        /// Cursor to pass as `after_sequence` for the following page.
        next_after_sequence: Option<u64>,
        /// Whether at least one later event exists.
        has_more: bool,
    },
    /// Bounded pending durable approvals.
    PendingApprovals {
        /// Oldest pending records in deterministic order.
        approvals: Vec<ApprovalRecord>,
    },
    /// One durable approval record or explicit absence.
    Approval {
        /// Approval when present.
        approval: Option<Box<ApprovalRecord>>,
    },
    /// Successful immutable approval settlement.
    ApprovalSettled {
        /// Updated terminal record.
        approval: Box<ApprovalRecord>,
    },
    /// Newly created durable Task Graph.
    TaskGraphCreated {
        /// Bounded graph metadata.
        graph: TaskGraphSummary,
    },
    /// Loaded Task Graph metadata or explicit absence.
    TaskGraph {
        /// Bounded graph metadata when found.
        graph: Option<TaskGraphSummary>,
    },
    /// Bounded Task record page.
    TaskRecords {
        /// Identity-ordered records and continuation cursor.
        page: TaskRecordPage,
    },
    /// Ready Tasks claimed for one authenticated worker.
    TasksClaimed {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing every returned lease.
        revision: u64,
        /// Principal-derived worker identity.
        worker: String,
        /// Fenced Task claims.
        claims: Vec<TaskClaim>,
    },
    /// Successful Task lease heartbeat.
    TaskHeartbeat {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing the extension.
        revision: u64,
        /// Running Task.
        task_id: TaskId,
        /// Current fencing token.
        lease_id: TaskLeaseId,
        /// New server-clock expiration.
        expires_at_ms: u64,
    },
    /// Successful Task completion settlement.
    TaskCompleted {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing the settlement.
        revision: u64,
        /// Settled Task.
        task_id: TaskId,
    },
    /// Successful Task failure settlement.
    TaskFailed {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing the settlement.
        revision: u64,
        /// Settled Task.
        task_id: TaskId,
    },
    /// Successful operator cancellation.
    TaskCancelled {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing the settlement.
        revision: u64,
        /// Settled Task.
        task_id: TaskId,
    },
    /// Authenticated worker inbox page.
    TaskMessages {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision from which the page was read.
        revision: u64,
        /// Bounded ordered inbox page.
        page: TaskMessagePage,
    },
    /// Successful authenticated worker message send.
    TaskMessageSent {
        /// Owning Task Graph.
        graph_id: TaskGraphId,
        /// Revision containing the message.
        revision: u64,
        /// Persisted ordered message.
        message: TaskMessage,
    },
}

/// Pollable asynchronous Turn state.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OperationStatus {
    /// Background Turn is still executing.
    Running {
        /// Target Thread.
        thread_id: ThreadId,
    },
    /// Turn completed successfully.
    Completed {
        /// Target Thread.
        thread_id: ThreadId,
        /// Terminal Turn identity.
        turn_id: TurnId,
        /// Final assistant text.
        final_text: String,
    },
    /// Turn failed for a non-control reason.
    Failed {
        /// Bounded error.
        error: String,
    },
    /// Explicit cancellation settled.
    Cancelled {
        /// Bounded stop explanation.
        error: String,
    },
    /// Configured deadline settled.
    TimedOut {
        /// Bounded stop explanation.
        error: String,
    },
}

/// One process-local provisional event emitted by a running model.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationStreamEvent {
    /// Monotonic operation-local sequence.
    pub sequence: u64,
    /// Bounded provider event.
    pub event: ModelStreamEvent,
}

/// Client-safe normalized protocol error.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtocolError {
    /// Stable machine-readable category.
    pub code: String,
    /// Bounded actionable message.
    pub message: String,
    /// Whether retry may succeed without changing the request.
    pub retryable: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Exact compatibility coordinates advertised during initialization.
pub struct CompatibilityManifest {
    /// Cargo package version of this engine build.
    pub engine_version: String,
    /// Append-only State event schema accepted and written.
    pub state_event_schema: u32,
    /// Disposable State snapshot schema accepted and written.
    pub state_snapshot_schema: u32,
    /// Durable Approval Inbox record schema accepted and written.
    pub approval_inbox_schema: u32,
    /// Durable Task Coordinator graph schema accepted and written.
    pub task_graph_schema: u32,
    /// Exact Memory Provider API version.
    pub memory_api: u32,
    /// Exact provider-specific Token Counter API version.
    pub token_counter_api: u32,
    /// Exact semantic conversation-compactor API version.
    pub conversation_compactor_api: u32,
    /// Exact Secret Provider API version.
    pub secret_api: u32,
    /// Exact Skill package API version.
    pub skill_api: String,
    /// Exact HTTPS JSON model-gateway API version.
    pub model_gateway_api: String,
    /// Exact Workspace Provider API version.
    pub workspace_provider_api: String,
}

struct ManagedOperation {
    tenant_id: Option<String>,
    status: OperationStatus,
    cancellation: CancellationToken,
    events: Arc<OperationEventBuffer>,
}

struct OperationLifecycle {
    accepting: AtomicBool,
    settled: Notify,
}

impl OperationLifecycle {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            settled: Notify::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
/// Content-free result of draining process-local protocol Operations.
pub struct ProtocolShutdownReport {
    /// Running Operations asked to cancel when draining began.
    pub cancellation_requests: u64,
    /// Those Operations that reached a terminal process-local status in time.
    pub settled_operations: u64,
    /// Operations still running when the shutdown deadline elapsed.
    pub remaining_operations: u64,
    /// Whether Runtime snapshot maintenance also drained within the deadline.
    pub background_work_drained: bool,
}

#[derive(Default)]
struct OperationEventBuffer {
    inner: StdMutex<OperationEventBufferInner>,
}

#[derive(Default)]
struct OperationEventBufferInner {
    events: VecDeque<OperationStreamEvent>,
    retained_bytes: usize,
    next_sequence: u64,
    dropped_through_sequence: Option<u64>,
}

impl OperationEventBuffer {
    fn page(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<(Vec<OperationStreamEvent>, bool, Option<u64>), HarnessError> {
        let inner = self.inner.lock().map_err(|_| {
            HarnessError::Protocol("operation event buffer lock poisoned".to_owned())
        })?;
        let mut retained = inner
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .take(limit.saturating_add(1))
            .cloned()
            .collect::<Vec<_>>();
        let has_more = retained.len() > limit;
        if has_more {
            retained.pop();
        }
        Ok((retained, has_more, inner.dropped_through_sequence))
    }
}

impl ModelEventSink for OperationEventBuffer {
    fn emit(&self, event: &ModelStreamEvent) -> Result<(), String> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| "operation event buffer lock poisoned".to_owned())?;
        let bytes = match event {
            ModelStreamEvent::TextDelta { delta, .. } => delta.len(),
            ModelStreamEvent::StepInvalidated { .. } => 0,
        };
        let sequence = inner
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| "operation event sequence overflow".to_owned())?;
        inner.next_sequence = sequence;
        inner.retained_bytes = inner
            .retained_bytes
            .checked_add(bytes)
            .ok_or_else(|| "operation event byte count overflow".to_owned())?;
        inner.events.push_back(OperationStreamEvent {
            sequence,
            event: event.clone(),
        });
        while inner.events.len() > MAX_OPERATION_STREAM_EVENTS
            || inner.retained_bytes > MAX_OPERATION_STREAM_BYTES
        {
            let Some(evicted) = inner.events.pop_front() else {
                break;
            };
            let evicted_bytes = match &evicted.event {
                ModelStreamEvent::TextDelta { delta, .. } => delta.len(),
                ModelStreamEvent::StepInvalidated { .. } => 0,
            };
            inner.retained_bytes = inner.retained_bytes.saturating_sub(evicted_bytes);
            inner.dropped_through_sequence = Some(evicted.sequence);
        }
        Ok(())
    }
}

/// Shared typed command handler used by stdio and future clients.
#[derive(Clone)]
pub struct ProtocolHandler {
    runtime: Arc<HarnessRuntime>,
    approvals: Option<Arc<dyn ApprovalInbox>>,
    tasks: Option<TaskProtocolService>,
    authorizer: Arc<dyn ProtocolAuthorizer>,
    operations: Arc<Mutex<BTreeMap<OperationId, ManagedOperation>>>,
    lifecycle: Arc<OperationLifecycle>,
    operation_retention_limit: usize,
}

impl ProtocolHandler {
    /// Creates a handler over the same Runtime used by embedded callers.
    #[must_use]
    pub fn new(runtime: Arc<HarnessRuntime>) -> Self {
        Self {
            runtime,
            approvals: None,
            tasks: None,
            authorizer: Arc::new(LocalProcessAuthorizer),
            operations: Arc::new(Mutex::new(BTreeMap::new())),
            lifecycle: Arc::new(OperationLifecycle::new()),
            operation_retention_limit: DEFAULT_OPERATION_RETENTION_LIMIT,
        }
    }

    /// Exposes a durable Approval Inbox through the typed protocol.
    #[must_use]
    pub fn with_approval_inbox(mut self, approvals: Arc<dyn ApprovalInbox>) -> Self {
        self.approvals = Some(approvals);
        self
    }

    /// Exposes durable Task Graph administration and worker coordination.
    #[must_use]
    pub fn with_task_coordinator(mut self, coordinator: Arc<dyn TaskCoordinator>) -> Self {
        self.tasks = Some(TaskProtocolService::new(coordinator));
        self
    }

    /// Installs the authority used before every typed command.
    #[must_use]
    pub fn with_authorizer(mut self, authorizer: Arc<dyn ProtocolAuthorizer>) -> Self {
        self.authorizer = authorizer;
        self
    }

    /// Sets the maximum number of running and terminal Operations retained.
    ///
    /// Clients must forget terminal Operations to release capacity.
    pub fn with_operation_retention_limit(mut self, limit: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_OPERATION_RETENTION_LIMIT).contains(&limit) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "protocol Operation retention limit must be 1-{MAX_OPERATION_RETENTION_LIMIT}"
            )));
        }
        self.operation_retention_limit = limit;
        Ok(self)
    }

    /// Stops accepting new Turns, requests cancellation of every running
    /// Operation, and waits up to `timeout` for process-local settlement.
    ///
    /// The handler remains in draining state permanently. A non-zero
    /// `remaining_operations` requires the host to retain the Runtime or later
    /// perform explicit exclusive Thread recovery; this method never reports a
    /// forced abort as successful cancellation.
    pub async fn shutdown(
        &self,
        timeout: Duration,
    ) -> Result<ProtocolShutdownReport, HarnessError> {
        if timeout < Duration::from_millis(1) || timeout > MAX_OPERATION_SHUTDOWN_TIMEOUT {
            return Err(HarnessError::InvalidConfiguration(format!(
                "protocol Operation shutdown timeout must be 1 millisecond to {} seconds",
                MAX_OPERATION_SHUTDOWN_TIMEOUT.as_secs()
            )));
        }
        self.lifecycle.accepting.store(false, Ordering::Release);
        let cancellation_requests = {
            let operations = self.operations.lock().await;
            let mut count = 0_u64;
            for operation in operations.values() {
                if matches!(operation.status, OperationStatus::Running { .. }) {
                    operation.cancellation.cancel();
                    count = count.saturating_add(1);
                }
            }
            count
        };
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            HarnessError::InvalidConfiguration("shutdown deadline overflow".to_owned())
        })?;
        let remaining_operations = loop {
            let notified = self.lifecycle.settled.notified();
            let remaining = self.running_operation_count().await;
            if remaining == 0 {
                break 0;
            }
            if timeout_at(deadline, notified).await.is_err() {
                break self.running_operation_count().await;
            }
        };
        let background_work_drained = remaining_operations == 0
            && self
                .runtime
                .drain_background_work(deadline.saturating_duration_since(Instant::now()))
                .await;
        Ok(ProtocolShutdownReport {
            cancellation_requests,
            settled_operations: cancellation_requests.saturating_sub(remaining_operations),
            remaining_operations,
            background_work_drained,
        })
    }

    /// Handles one typed request without exposing internal transport errors.
    pub async fn handle(&self, request: ProtocolRequest) -> ProtocolResponse {
        self.handle_as(&ProtocolPrincipal::LocalProcess, request)
            .await
    }

    /// Handles one request as a transport-authenticated principal.
    pub async fn handle_as(
        &self,
        principal: &ProtocolPrincipal,
        request: ProtocolRequest,
    ) -> ProtocolResponse {
        let id = Some(request.id.clone());
        if let Err(error) = validate_request_envelope(&request) {
            return error_response(id, error);
        }
        let permission = request.command.permission();
        if !self.is_allowed(principal, permission) {
            return error_response(
                id,
                protocol_error(HarnessError::ProtocolDenied {
                    permission: permission.to_owned(),
                }),
            );
        }
        let authority = match self.resolve_authority(principal) {
            Ok(authority) => authority,
            Err(error) => return error_response(id, protocol_error(error)),
        };
        let result = match isolate_future(
            || self.handle_command(request.command, principal, &authority),
            None,
        ) {
            Ok(command) => match command.await {
                Ok(result) => result,
                Err(()) => Err(HarnessError::Execution(
                    "protocol command execution panicked".to_owned(),
                )),
            },
            Err(()) => Err(HarnessError::Execution(
                "protocol command construction panicked".to_owned(),
            )),
        };
        match result {
            Ok(result) => ProtocolResponse {
                id,
                protocol_version: PROTOCOL_VERSION.to_owned(),
                body: ProtocolResponseBody::Success { result },
            },
            Err(error) => error_response(id, protocol_error(error)),
        }
    }

    async fn handle_command(
        &self,
        command: ProtocolCommand,
        principal: &ProtocolPrincipal,
        authority: &AuthorityContext,
    ) -> Result<ProtocolResult, HarnessError> {
        match command {
            ProtocolCommand::Initialize {} => {
                let mut capabilities = vec![
                    "operation.cancel".to_owned(),
                    "operation.events".to_owned(),
                    "operation.forget".to_owned(),
                    "operation.get".to_owned(),
                    "thread.capacity".to_owned(),
                    "thread.create".to_owned(),
                    "thread.events".to_owned(),
                    "thread.get".to_owned(),
                    "thread.name".to_owned(),
                    "thread.recover".to_owned(),
                    "turn.start".to_owned(),
                    "turn.steer".to_owned(),
                ];
                if self.runtime.supports_thread_listing() {
                    capabilities.push("thread.list".to_owned());
                }
                if self.runtime.supports_thread_fork() {
                    capabilities.push("thread.fork".to_owned());
                }
                if self.approvals.is_some() {
                    capabilities.extend([
                        "approval.get".to_owned(),
                        "approval.pending".to_owned(),
                        "approval.settle".to_owned(),
                    ]);
                }
                if self.tasks.is_some() {
                    capabilities.extend([
                        "task.graph.cancel".to_owned(),
                        "task.graph.create".to_owned(),
                        "task.graph.get".to_owned(),
                        "task.worker.claim".to_owned(),
                        "task.worker.complete".to_owned(),
                        "task.worker.fail".to_owned(),
                        "task.worker.heartbeat".to_owned(),
                        "task.worker.messages.read".to_owned(),
                        "task.worker.messages.send".to_owned(),
                    ]);
                }
                capabilities.sort();
                capabilities.retain(|permission| self.is_allowed(principal, permission));
                Ok(ProtocolResult::Initialized {
                    server: "Y-Harness Engineering".to_owned(),
                    capabilities,
                    compatibility: CompatibilityManifest {
                        engine_version: env!("CARGO_PKG_VERSION").to_owned(),
                        state_event_schema: STATE_EVENT_SCHEMA_VERSION,
                        state_snapshot_schema: STATE_SNAPSHOT_SCHEMA_VERSION,
                        approval_inbox_schema: APPROVAL_INBOX_SCHEMA_VERSION,
                        task_graph_schema: TASK_GRAPH_SCHEMA_VERSION,
                        memory_api: MEMORY_API_VERSION,
                        token_counter_api: TOKEN_COUNTER_API_VERSION,
                        conversation_compactor_api: CONVERSATION_COMPACTOR_API_VERSION,
                        secret_api: SECRET_API_VERSION,
                        skill_api: SKILL_API_VERSION.to_owned(),
                        model_gateway_api: MODEL_GATEWAY_API_VERSION.to_owned(),
                        workspace_provider_api: WORKSPACE_PROVIDER_API_VERSION.to_owned(),
                    },
                })
            }
            ProtocolCommand::CreateThread {} => {
                let thread = self.runtime.create_thread_as(authority).await?;
                Ok(ProtocolResult::ThreadCreated { thread })
            }
            ProtocolCommand::ForkThread {
                parent_thread_id,
                child_thread_id,
                through_turn_id,
            } => {
                validate_opaque_id("parent_thread_id", &parent_thread_id)?;
                validate_opaque_id("child_thread_id", &child_thread_id)?;
                if let Some(turn_id) = through_turn_id.as_deref() {
                    validate_opaque_id("through_turn_id", turn_id)?;
                }
                let parent_thread_id = ThreadId::from_string(parent_thread_id);
                let child_thread_id = ThreadId::from_string(child_thread_id);
                let through_turn_id = through_turn_id.map(TurnId::from_string);
                let thread = self
                    .runtime
                    .fork_thread_as(
                        authority,
                        &parent_thread_id,
                        child_thread_id,
                        through_turn_id.as_ref(),
                    )
                    .await?;
                Ok(ProtocolResult::ThreadForked { thread })
            }
            ProtocolCommand::ListThreads {
                before_sequence,
                limit,
            } => {
                let page = self
                    .runtime
                    .list_threads_as(
                        before_sequence,
                        limit.unwrap_or(DEFAULT_THREAD_PAGE),
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::Threads {
                    threads: page.threads,
                    next_before_sequence: page.next_before_sequence,
                    has_more: page.has_more,
                })
            }
            ProtocolCommand::SetThreadName { thread_id, name } => {
                validate_opaque_id("thread_id", &thread_id)?;
                let thread_id = ThreadId::from_string(thread_id);
                self.runtime
                    .set_thread_name_as(&thread_id, name.clone(), authority)
                    .await?;
                Ok(ProtocolResult::ThreadNamed { name })
            }
            ProtocolCommand::GetThread { thread_id } => {
                validate_opaque_id("thread_id", &thread_id)?;
                let thread_id = ThreadId::from_string(thread_id);
                let thread = self.runtime.load_thread_as(&thread_id, authority).await?;
                Ok(ProtocolResult::Thread { thread })
            }
            ProtocolCommand::RecoverThread {
                thread_id,
                expected_turn_id,
            } => {
                validate_opaque_id("thread_id", &thread_id)?;
                validate_opaque_id("expected_turn_id", &expected_turn_id)?;
                let thread_id = ThreadId::from_string(thread_id);
                let expected_turn_id = TurnId::from_string(expected_turn_id);
                if self.operations.lock().await.values().any(|operation| {
                    operation.tenant_id.as_deref() == authority.tenant_id()
                        && matches!(
                            &operation.status,
                            OperationStatus::Running {
                                thread_id: active_thread
                            } if active_thread == &thread_id
                        )
                }) {
                    return Err(HarnessError::Protocol(format!(
                        "thread {thread_id} still has a live operation in this protocol host"
                    )));
                }
                let Some(current) = self.runtime.load_thread_as(&thread_id, authority).await?
                else {
                    return Ok(ProtocolResult::ThreadRecovered { thread: None });
                };
                let expected = current
                    .turns
                    .iter()
                    .find(|turn| turn.id == expected_turn_id)
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!(
                            "expected recovery Turn {expected_turn_id} does not belong to thread {thread_id}"
                        ))
                    })?;
                let running_turns = current
                    .turns
                    .iter()
                    .filter(|turn| turn.status == crate::TurnStatus::Running)
                    .count();
                if !((expected.status == crate::TurnStatus::Running && running_turns == 1)
                    || (expected.status == crate::TurnStatus::Interrupted && running_turns == 0))
                {
                    return Err(HarnessError::Protocol(format!(
                        "thread {thread_id} is not awaiting takeover of Turn {expected_turn_id}"
                    )));
                }
                let thread = self
                    .runtime
                    .recover_thread_as(&thread_id, &expected_turn_id, authority)
                    .await?;
                Ok(ProtocolResult::ThreadRecovered { thread })
            }
            ProtocolCommand::GetThreadCapacity { thread_id } => {
                validate_opaque_id("thread_id", &thread_id)?;
                let thread_id = ThreadId::from_string(thread_id);
                let capacity = self
                    .runtime
                    .thread_capacity_as(&thread_id, authority)
                    .await?
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("thread {thread_id} does not exist"))
                    })?;
                Ok(ProtocolResult::ThreadCapacity { capacity })
            }
            ProtocolCommand::StartTurn {
                thread_id,
                prompt,
                memory_scope,
                context,
                timeout_ms,
            } => {
                if !self.lifecycle.accepting.load(Ordering::Acquire) {
                    return Err(HarnessError::Protocol(
                        "protocol handler is shutting down".to_owned(),
                    ));
                }
                validate_opaque_id("thread_id", &thread_id)?;
                if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
                    return Err(HarnessError::Protocol(format!(
                        "prompt must be 1-{MAX_PROMPT_BYTES} bytes"
                    )));
                }
                if timeout_ms == Some(0) {
                    return Err(HarnessError::Protocol(
                        "timeout_ms must be greater than zero".to_owned(),
                    ));
                }
                crate::context::validate_turn_context_inputs(&context)
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                let thread_id = ThreadId::from_string(thread_id);
                if self
                    .runtime
                    .load_thread_as(&thread_id, authority)
                    .await?
                    .is_none()
                {
                    return Err(HarnessError::Protocol(format!(
                        "thread {thread_id} does not exist"
                    )));
                }
                let operation_id = OperationId::generate();
                let cancellation = CancellationToken::new();
                let events = Arc::new(OperationEventBuffer::default());
                let mut operations = self.operations.lock().await;
                if !self.lifecycle.accepting.load(Ordering::Acquire) {
                    return Err(HarnessError::Protocol(
                        "protocol handler is shutting down".to_owned(),
                    ));
                }
                if operations.len() >= self.operation_retention_limit {
                    return Err(HarnessError::Protocol(format!(
                        "operation capacity {} reached; forget terminal operations",
                        self.operation_retention_limit
                    )));
                }
                operations.insert(
                    operation_id.clone(),
                    ManagedOperation {
                        tenant_id: authority.tenant_id().map(str::to_owned),
                        status: OperationStatus::Running {
                            thread_id: thread_id.clone(),
                        },
                        cancellation: cancellation.clone(),
                        events: events.clone(),
                    },
                );
                drop(operations);
                let runtime = self.runtime.clone();
                let operations = self.operations.clone();
                let lifecycle = self.lifecycle.clone();
                let operation_for_task = operation_id.clone();
                let authority = authority.clone();
                let worker = tokio::spawn(async move {
                    runtime
                        .run_turn_with_options(
                            &thread_id,
                            prompt,
                            TurnExecutionOptions {
                                authority,
                                memory_scope,
                                context,
                                timeout: timeout_ms.map(Duration::from_millis),
                                cancellation,
                                model_event_sink: Some(events),
                            },
                        )
                        .await
                });
                tokio::spawn(async move {
                    let status = match worker.await {
                        Ok(Ok(outcome)) => OperationStatus::Completed {
                            thread_id: outcome.turn.thread_id,
                            turn_id: outcome.turn.id,
                            final_text: outcome.final_text,
                        },
                        Ok(Err(error @ HarnessError::Cancelled { .. })) => {
                            OperationStatus::Cancelled {
                                error: bounded_error(&error.to_string()),
                            }
                        }
                        Ok(Err(error @ HarnessError::TimedOut { .. })) => {
                            OperationStatus::TimedOut {
                                error: bounded_error(&error.to_string()),
                            }
                        }
                        Ok(Err(error)) => OperationStatus::Failed {
                            error: bounded_error(&error.to_string()),
                        },
                        Err(error) if error.is_panic() => OperationStatus::Failed {
                            error: "operation task panicked before protocol settlement".to_owned(),
                        },
                        Err(_) => OperationStatus::Failed {
                            error: "operation task stopped before protocol settlement".to_owned(),
                        },
                    };
                    if let Some(operation) = operations.lock().await.get_mut(&operation_for_task) {
                        operation.status = status;
                    }
                    lifecycle.settled.notify_waiters();
                });
                Ok(ProtocolResult::TurnStarted { operation_id })
            }
            ProtocolCommand::SteerTurn {
                thread_id,
                expected_turn_id,
                content,
            } => {
                validate_opaque_id("thread_id", &thread_id)?;
                validate_opaque_id("expected_turn_id", &expected_turn_id)?;
                if content.trim().is_empty() || content.len() > MAX_PROMPT_BYTES {
                    return Err(HarnessError::Protocol(format!(
                        "steering content must be 1-{MAX_PROMPT_BYTES} bytes"
                    )));
                }
                let receipt = self
                    .runtime
                    .steer_turn_as(
                        &ThreadId::from_string(thread_id),
                        &TurnId::from_string(expected_turn_id),
                        content,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TurnSteered {
                    steering_id: receipt.steering_id,
                    turn_id: receipt.turn_id,
                })
            }
            ProtocolCommand::GetOperation { operation_id } => {
                validate_opaque_id("operation_id", &operation_id)?;
                let operation_id = OperationId::from_string(operation_id);
                let operation = self
                    .operations
                    .lock()
                    .await
                    .get(&operation_id)
                    .filter(|operation| operation.tenant_id.as_deref() == authority.tenant_id())
                    .map(|operation| operation.status.clone())
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("operation {operation_id} does not exist"))
                    })?;
                Ok(ProtocolResult::Operation { operation })
            }
            ProtocolCommand::GetOperationEvents {
                operation_id,
                after_sequence,
                limit,
            } => {
                validate_opaque_id("operation_id", &operation_id)?;
                let limit = limit.unwrap_or(DEFAULT_OPERATION_EVENT_PAGE);
                if !(1..=MAX_OPERATION_EVENT_PAGE).contains(&limit) {
                    return Err(HarnessError::Protocol(format!(
                        "operation event limit must be 1-{MAX_OPERATION_EVENT_PAGE}"
                    )));
                }
                let operation_id = OperationId::from_string(operation_id);
                let events = self
                    .operations
                    .lock()
                    .await
                    .get(&operation_id)
                    .filter(|operation| operation.tenant_id.as_deref() == authority.tenant_id())
                    .map(|operation| operation.events.clone())
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("operation {operation_id} does not exist"))
                    })?;
                let (events, has_more, dropped_through_sequence) =
                    events.page(after_sequence.unwrap_or(0), limit)?;
                let next_after_sequence = events.last().map(|event| event.sequence);
                Ok(ProtocolResult::OperationEvents {
                    events,
                    next_after_sequence,
                    has_more,
                    dropped_through_sequence,
                })
            }
            ProtocolCommand::CancelOperation { operation_id } => {
                validate_opaque_id("operation_id", &operation_id)?;
                let operation_id = OperationId::from_string(operation_id);
                let operations = self.operations.lock().await;
                let operation = operations
                    .get(&operation_id)
                    .filter(|operation| operation.tenant_id.as_deref() == authority.tenant_id())
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("operation {operation_id} does not exist"))
                    })?;
                let accepted = matches!(operation.status, OperationStatus::Running { .. });
                if accepted {
                    operation.cancellation.cancel();
                }
                Ok(ProtocolResult::Cancellation {
                    operation_id,
                    accepted,
                })
            }
            ProtocolCommand::ForgetOperation { operation_id } => {
                validate_opaque_id("operation_id", &operation_id)?;
                let operation_id = OperationId::from_string(operation_id);
                let mut operations = self.operations.lock().await;
                let operation = operations
                    .get(&operation_id)
                    .filter(|operation| operation.tenant_id.as_deref() == authority.tenant_id())
                    .ok_or_else(|| {
                        HarnessError::Protocol(format!("operation {operation_id} does not exist"))
                    })?;
                if matches!(operation.status, OperationStatus::Running { .. }) {
                    return Err(HarnessError::Protocol(format!(
                        "operation {operation_id} is still running"
                    )));
                }
                operations.remove(&operation_id);
                Ok(ProtocolResult::OperationForgotten { operation_id })
            }
            ProtocolCommand::GetEvents {
                thread_id,
                after_sequence,
                limit,
            } => {
                validate_opaque_id("thread_id", &thread_id)?;
                let limit = limit.unwrap_or(DEFAULT_EVENT_PAGE);
                if !(1..=MAX_EVENT_PAGE).contains(&limit) {
                    return Err(HarnessError::Protocol(format!(
                        "event limit must be 1-{MAX_EVENT_PAGE}"
                    )));
                }
                let thread_id = ThreadId::from_string(thread_id);
                let after_sequence = after_sequence.unwrap_or(0);
                let (events, has_more) = self
                    .bounded_event_page(&thread_id, after_sequence, limit, authority)
                    .await?;
                if events.is_empty()
                    && self
                        .runtime
                        .events_page_as(&thread_id, 0, 1, authority)
                        .await?
                        .is_empty()
                {
                    return Err(HarnessError::Protocol(format!(
                        "thread {thread_id} does not exist"
                    )));
                }
                let next_after_sequence = events.last().map(|event| event.sequence);
                Ok(ProtocolResult::Events {
                    events,
                    next_after_sequence,
                    has_more,
                })
            }
            ProtocolCommand::GetPendingApprovals { limit } => {
                let limit = limit.unwrap_or(DEFAULT_APPROVAL_PAGE);
                if !(1..=MAX_APPROVAL_PAGE).contains(&limit) {
                    return Err(HarnessError::Protocol(format!(
                        "approval limit must be 1-{MAX_APPROVAL_PAGE}"
                    )));
                }
                let approvals = self.approval_inbox()?.pending_as(limit, authority).await?;
                Ok(ProtocolResult::PendingApprovals { approvals })
            }
            ProtocolCommand::GetApproval { approval_id } => {
                validate_opaque_id("approval_id", &approval_id)?;
                let approval = self
                    .approval_inbox()?
                    .get_as(&ApprovalId::from_string(approval_id), authority)
                    .await?;
                Ok(ProtocolResult::Approval {
                    approval: approval.map(Box::new),
                })
            }
            ProtocolCommand::SettleApproval {
                approval_id,
                expected_revision,
                decision,
            } => {
                validate_opaque_id("approval_id", &approval_id)?;
                if expected_revision == 0 {
                    return Err(HarnessError::Protocol(
                        "expected_revision must be greater than zero".to_owned(),
                    ));
                }
                let approval = self
                    .approval_inbox()?
                    .settle_as(
                        &ApprovalId::from_string(approval_id),
                        expected_revision,
                        decision,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::ApprovalSettled {
                    approval: Box::new(approval),
                })
            }
            ProtocolCommand::CreateTaskGraph {
                graph_id,
                definitions,
            } => {
                validate_task_identity("graph_id", &graph_id)?;
                let graph = self
                    .task_service()?
                    .create(TaskGraphId::from_string(graph_id), definitions, authority)
                    .await?;
                Ok(ProtocolResult::TaskGraphCreated { graph })
            }
            ProtocolCommand::GetTaskGraph { graph_id } => {
                validate_task_identity("graph_id", &graph_id)?;
                let graph = self
                    .task_service()?
                    .summary(&TaskGraphId::from_string(graph_id), authority)
                    .await?;
                Ok(ProtocolResult::TaskGraph { graph })
            }
            ProtocolCommand::GetTaskRecords {
                graph_id,
                after_task_id,
                limit,
            } => {
                validate_task_identity("graph_id", &graph_id)?;
                if let Some(task_id) = after_task_id.as_deref() {
                    validate_task_identity("after_task_id", task_id)?;
                }
                let page = self
                    .task_service()?
                    .records(
                        &TaskGraphId::from_string(graph_id),
                        after_task_id.as_deref(),
                        limit.unwrap_or(DEFAULT_TASK_RECORD_PAGE),
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskRecords { page })
            }
            ProtocolCommand::CancelTask {
                graph_id,
                task_id,
                expected_revision,
                reason,
            } => {
                validate_task_identity("graph_id", &graph_id)?;
                validate_task_identity("task_id", &task_id)?;
                if expected_revision == 0 {
                    return Err(HarnessError::Protocol(
                        "expected_revision must be greater than zero".to_owned(),
                    ));
                }
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let revision = self
                    .task_service()?
                    .cancel(&graph_id, &task_id, expected_revision, reason, authority)
                    .await?;
                Ok(ProtocolResult::TaskCancelled {
                    graph_id,
                    revision,
                    task_id,
                })
            }
            ProtocolCommand::ClaimTasks {
                graph_id,
                lease_duration_ms,
                maximum,
            } => {
                validate_task_identity("graph_id", &graph_id)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let worker = principal.task_worker_identity();
                let (revision, claims) = self
                    .task_service()?
                    .claim(
                        &graph_id,
                        &worker,
                        lease_duration_ms,
                        maximum.unwrap_or(DEFAULT_TASK_CLAIM_MAXIMUM),
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TasksClaimed {
                    graph_id,
                    revision,
                    worker,
                    claims,
                })
            }
            ProtocolCommand::HeartbeatTask {
                graph_id,
                task_id,
                lease_id,
                lease_duration_ms,
            } => {
                validate_task_worker_ids(&graph_id, &task_id, &lease_id)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let lease_id = TaskLeaseId::from_string(lease_id);
                let worker = principal.task_worker_identity();
                let (revision, expires_at_ms) = self
                    .task_service()?
                    .heartbeat(
                        &graph_id,
                        TaskWorkerAccess::new(&task_id, &lease_id, &worker),
                        lease_duration_ms,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskHeartbeat {
                    graph_id,
                    revision,
                    task_id,
                    lease_id,
                    expires_at_ms,
                })
            }
            ProtocolCommand::CompleteTask {
                graph_id,
                task_id,
                lease_id,
                completion,
            } => {
                validate_task_worker_ids(&graph_id, &task_id, &lease_id)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let lease_id = TaskLeaseId::from_string(lease_id);
                let worker = principal.task_worker_identity();
                let revision = self
                    .task_service()?
                    .complete(
                        &graph_id,
                        TaskWorkerAccess::new(&task_id, &lease_id, &worker),
                        completion,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskCompleted {
                    graph_id,
                    revision,
                    task_id,
                })
            }
            ProtocolCommand::FailTask {
                graph_id,
                task_id,
                lease_id,
                reason,
            } => {
                validate_task_worker_ids(&graph_id, &task_id, &lease_id)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let lease_id = TaskLeaseId::from_string(lease_id);
                let worker = principal.task_worker_identity();
                let revision = self
                    .task_service()?
                    .fail(
                        &graph_id,
                        TaskWorkerAccess::new(&task_id, &lease_id, &worker),
                        reason,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskFailed {
                    graph_id,
                    revision,
                    task_id,
                })
            }
            ProtocolCommand::GetTaskMessages {
                graph_id,
                task_id,
                lease_id,
                after_sequence,
                limit,
            } => {
                validate_task_worker_ids(&graph_id, &task_id, &lease_id)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let lease_id = TaskLeaseId::from_string(lease_id);
                let worker = principal.task_worker_identity();
                let (revision, page) = self
                    .task_service()?
                    .inbox(
                        &graph_id,
                        TaskWorkerAccess::new(&task_id, &lease_id, &worker),
                        after_sequence.unwrap_or(0),
                        limit.unwrap_or(DEFAULT_TASK_MESSAGE_PAGE),
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskMessages {
                    graph_id,
                    revision,
                    page,
                })
            }
            ProtocolCommand::SendTaskMessage {
                graph_id,
                task_id,
                lease_id,
                to,
                body,
            } => {
                validate_task_worker_ids(&graph_id, &task_id, &lease_id)?;
                validate_task_identity("to", &to)?;
                let graph_id = TaskGraphId::from_string(graph_id);
                let task_id = TaskId::from_string(task_id);
                let lease_id = TaskLeaseId::from_string(lease_id);
                let worker = principal.task_worker_identity();
                let (revision, message) = self
                    .task_service()?
                    .send(
                        &graph_id,
                        TaskWorkerAccess::new(&task_id, &lease_id, &worker),
                        &TaskId::from_string(to),
                        body,
                        authority,
                    )
                    .await?;
                Ok(ProtocolResult::TaskMessageSent {
                    graph_id,
                    revision,
                    message,
                })
            }
        }
    }

    async fn running_operation_count(&self) -> u64 {
        self.operations
            .lock()
            .await
            .values()
            .filter(|operation| matches!(operation.status, OperationStatus::Running { .. }))
            .count()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn approval_inbox(&self) -> Result<&Arc<dyn ApprovalInbox>, HarnessError> {
        self.approvals.as_ref().ok_or_else(|| {
            HarnessError::Protocol("durable Approval Inbox is not configured".to_owned())
        })
    }

    fn task_service(&self) -> Result<&TaskProtocolService, HarnessError> {
        self.tasks.as_ref().ok_or_else(|| {
            HarnessError::Protocol("durable Task Coordinator is not configured".to_owned())
        })
    }

    fn is_allowed(&self, principal: &ProtocolPrincipal, permission: &str) -> bool {
        catch_unwind(AssertUnwindSafe(|| {
            self.authorizer.allows(principal, permission)
        }))
        .unwrap_or(false)
    }

    fn resolve_authority(
        &self,
        principal: &ProtocolPrincipal,
    ) -> Result<AuthorityContext, HarnessError> {
        let authority = catch_unwind(AssertUnwindSafe(|| {
            self.authorizer.authority_context(principal)
        }))
        .map_err(|_| {
            HarnessError::Execution("protocol authority resolution failed".to_owned())
        })??;
        authority.validate_current("protocol authority")?;
        Ok(authority)
    }

    async fn bounded_event_page(
        &self,
        thread_id: &ThreadId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<(Vec<StoredEvent>, bool), HarnessError> {
        let mut events = Vec::new();
        let mut cursor = after_sequence;
        let mut encoded_bytes = 0_usize;
        loop {
            let remaining = limit.saturating_sub(events.len());
            if remaining == 0 {
                let has_more = !self
                    .runtime
                    .events_page_as(thread_id, cursor, 1, authority)
                    .await?
                    .is_empty();
                return Ok((events, has_more));
            }
            let fetch = remaining.saturating_add(1).min(EVENT_FETCH_BATCH);
            let page = self
                .runtime
                .events_page_as(thread_id, cursor, fetch, authority)
                .await?;
            if page.is_empty() {
                return Ok((events, false));
            }
            let page_len = page.len();
            for event in page {
                if events.len() == limit {
                    return Ok((events, true));
                }
                let event_bytes = serde_json::to_vec(&event)
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?
                    .len();
                let candidate_bytes = encoded_bytes
                    .checked_add(event_bytes)
                    .and_then(|bytes| bytes.checked_add(usize::from(!events.is_empty())))
                    .ok_or_else(|| {
                        HarnessError::Protocol("event page byte count overflow".to_owned())
                    })?;
                if candidate_bytes > MAX_PROTOCOL_EVENT_CONTENT_BYTES {
                    if events.is_empty() {
                        return Err(HarnessError::Protocol(
                            "one State event exceeds the protocol response budget".to_owned(),
                        ));
                    }
                    return Ok((events, true));
                }
                cursor = event.sequence;
                encoded_bytes = candidate_bytes;
                events.push(event);
            }
            if page_len < fetch {
                return Ok((events, false));
            }
        }
    }
}

/// Serves bounded newline-delimited JSON requests over process stdio.
pub async fn serve_stdio(handler: ProtocolHandler) -> Result<(), HarnessError> {
    let serving = serve_jsonl(
        &handler,
        BufReader::new(io::stdin()),
        BufWriter::new(io::stdout()),
    )
    .await;
    let shutdown = handler.shutdown(DEFAULT_OPERATION_SHUTDOWN_TIMEOUT).await?;
    if shutdown.remaining_operations > 0 || !shutdown.background_work_drained {
        return Err(HarnessError::Protocol(format!(
            "stdio shutdown incomplete: {} Operations remain; background drained={}",
            shutdown.remaining_operations, shutdown.background_work_drained
        )));
    }
    serving
}

/// Serves typed protocol frames over caller-secured asynchronous streams.
///
/// This function provides framing only. It performs no peer authentication or
/// encryption; network hosts must establish those properties before passing a
/// stream here.
pub async fn serve_jsonl<R, W>(
    handler: &ProtocolHandler,
    input: R,
    output: W,
) -> Result<(), HarnessError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_jsonl_as(handler, &ProtocolPrincipal::LocalProcess, input, output).await
}

/// Serves frames as one identity established by the calling transport.
pub async fn serve_jsonl_as<R, W>(
    handler: &ProtocolHandler,
    principal: &ProtocolPrincipal,
    input: R,
    output: W,
) -> Result<(), HarnessError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    serve_jsonl_with_limits(handler, principal, input, output, None, None).await
}

pub(crate) async fn serve_jsonl_with_limits<R, W>(
    handler: &ProtocolHandler,
    principal: &ProtocolPrincipal,
    mut input: R,
    mut output: W,
    idle_timeout: Option<Duration>,
    maximum_frames: Option<usize>,
) -> Result<(), HarnessError>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut frames = 0_usize;
    loop {
        if maximum_frames.is_some_and(|maximum| frames >= maximum) {
            return Err(HarnessError::Protocol(
                "connection frame limit reached".to_owned(),
            ));
        }
        let frame = match idle_timeout {
            Some(idle_timeout) => tokio::time::timeout(
                idle_timeout,
                read_bounded_frame(&mut input, MAX_REQUEST_FRAME_BYTES),
            )
            .await
            .map_err(|_| HarnessError::Protocol("connection idle timeout elapsed".to_owned()))?,
            None => read_bounded_frame(&mut input, MAX_REQUEST_FRAME_BYTES).await,
        }
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
        match frame {
            FrameRead::Eof => {
                output
                    .flush()
                    .await
                    .map_err(|error| HarnessError::Protocol(error.to_string()))?;
                return Ok(());
            }
            FrameRead::TooLong => {
                frames = frames.saturating_add(1);
                write_response(
                    &mut output,
                    &error_response(
                        None,
                        ProtocolError {
                            code: "frame_too_large".to_owned(),
                            message: format!(
                                "request frame exceeds {MAX_REQUEST_FRAME_BYTES} bytes"
                            ),
                            retryable: false,
                        },
                    ),
                )
                .await?;
            }
            FrameRead::Line(line) => {
                frames = frames.saturating_add(1);
                let response = match serde_json::from_slice::<ProtocolRequest>(&line) {
                    Ok(request) => handler.handle_as(principal, request).await,
                    Err(error) => error_response(
                        None,
                        ProtocolError {
                            code: "invalid_json".to_owned(),
                            message: bounded_error(&error.to_string()),
                            retryable: false,
                        },
                    ),
                };
                write_response(&mut output, &response).await?;
            }
        }
    }
}

enum FrameRead {
    Eof,
    Line(Vec<u8>),
    TooLong,
}

async fn read_bounded_frame<R>(reader: &mut R, maximum: usize) -> Result<FrameRead, std::io::Error>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let mut too_long = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if too_long {
                return Ok(FrameRead::TooLong);
            }
            if line.is_empty() {
                return Ok(FrameRead::Eof);
            }
            return Ok(FrameRead::Line(line));
        }
        if let Some(newline) = available.iter().position(|byte| *byte == b'\n') {
            if !too_long {
                if line.len().saturating_add(newline) > maximum {
                    too_long = true;
                } else {
                    line.extend_from_slice(&available[..newline]);
                }
            }
            reader.consume(newline + 1);
            if too_long {
                return Ok(FrameRead::TooLong);
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(FrameRead::Line(line));
        }
        let length = available.len();
        if !too_long {
            if line.len().saturating_add(length) > maximum {
                too_long = true;
            } else {
                line.extend_from_slice(available);
            }
        }
        reader.consume(length);
    }
}

async fn write_response<W>(output: &mut W, response: &ProtocolResponse) -> Result<(), HarnessError>
where
    W: AsyncWrite + Unpin,
{
    let mut writer = BoundedJsonWriter::new(MAX_RESPONSE_FRAME_BYTES);
    let encoded = match serde_json::to_writer(&mut writer, response) {
        Ok(()) => writer.into_inner(),
        Err(_) if writer.exceeded() => serde_json::to_vec(&error_response(
            response.id.clone(),
            ProtocolError {
                code: "response_too_large".to_owned(),
                message: format!("response frame exceeds {MAX_RESPONSE_FRAME_BYTES} bytes"),
                retryable: false,
            },
        ))
        .map_err(|error| HarnessError::Protocol(error.to_string()))?,
        Err(error) => return Err(HarnessError::Protocol(error.to_string())),
    };
    let mut encoded = encoded;
    encoded.push(b'\n');
    output
        .write_all(&encoded)
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))?;
    output
        .flush()
        .await
        .map_err(|error| HarnessError::Protocol(error.to_string()))
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    maximum: usize,
    exceeded: bool,
}

impl BoundedJsonWriter {
    fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(maximum.min(65_536)),
            maximum,
            exceeded: false,
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl std::io::Write for BoundedJsonWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(buffer.len())
            .is_none_or(|length| length > self.maximum)
        {
            self.exceeded = true;
            return Err(std::io::Error::other(
                "protocol response exceeds bounded writer capacity",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_request_envelope(request: &ProtocolRequest) -> Result<(), ProtocolError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ProtocolError {
            code: "unsupported_version".to_owned(),
            message: format!(
                "requested protocol {}, server supports {PROTOCOL_VERSION}",
                request.protocol_version
            ),
            retryable: false,
        });
    }
    if request.id.is_empty()
        || request.id.len() > 128
        || !request
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ProtocolError {
            code: "invalid_request_id".to_owned(),
            message: "request id must be 1-128 ASCII letters, digits, '.', '_' or '-'".to_owned(),
            retryable: false,
        });
    }
    Ok(())
}

fn validate_opaque_id(name: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(HarnessError::Protocol(format!(
            "{name} must be 1-{MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_task_identity(name: &str, value: &str) -> Result<(), HarnessError> {
    validate_opaque_id(name, value)?;
    if value.chars().any(char::is_control) {
        return Err(HarnessError::Protocol(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_task_worker_ids(
    graph_id: &str,
    task_id: &str,
    lease_id: &str,
) -> Result<(), HarnessError> {
    validate_task_identity("graph_id", graph_id)?;
    validate_task_identity("task_id", task_id)?;
    validate_task_identity("lease_id", lease_id)
}

fn validate_certificate_fingerprint(fingerprint: &str) -> Result<(), HarnessError> {
    if fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::InvalidConfiguration(
            "mTLS certificate fingerprint must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn protocol_error(error: HarnessError) -> ProtocolError {
    let retryable = matches!(
        error,
        HarnessError::RuntimeOverloaded { .. }
            | HarnessError::StateConflict { .. }
            | HarnessError::OrchestrationConflict { .. }
            | HarnessError::ApprovalConflict { .. }
            | HarnessError::Mcp(_)
    );
    ProtocolError {
        code: match error {
            HarnessError::RuntimeOverloaded { .. } => "runtime_overloaded",
            HarnessError::StateConflict { .. } => "state_conflict",
            HarnessError::OrchestrationConflict { .. } => "orchestration_conflict",
            HarnessError::ApprovalConflict { .. } => "approval_conflict",
            HarnessError::ProtocolDenied { .. } => "forbidden",
            HarnessError::Protocol(_) => "invalid_request",
            _ => "runtime_error",
        }
        .to_owned(),
        message: bounded_error(&error.to_string()),
        retryable,
    }
}

fn error_response(id: Option<String>, error: ProtocolError) -> ProtocolResponse {
    ProtocolResponse {
        id,
        protocol_version: PROTOCOL_VERSION.to_owned(),
        body: ProtocolResponseBody::Error { error },
    }
}

fn bounded_error(message: &str) -> String {
    let mut chars = message.chars();
    let bounded = chars.by_ref().take(MAX_ERROR_CHARS).collect::<String>();
    if chars.next().is_some() {
        format!("{bounded}…")
    } else {
        bounded
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::json;
    use tokio::io::BufReader;

    use super::{
        FingerprintProtocolAuthorizer, FrameRead, MAX_OPERATION_STREAM_EVENTS,
        MAX_RESPONSE_FRAME_BYTES, OperationEventBuffer, OperationStatus, PROTOCOL_VERSION,
        ProtocolAuthorizer, ProtocolCommand, ProtocolError, ProtocolHandler, ProtocolPrincipal,
        ProtocolRequest, ProtocolResponse, ProtocolResponseBody, ProtocolResult, TaskGraphSummary,
        read_bounded_frame, write_response,
    };
    use crate::{
        AllowListPolicy, ApprovalActor, ApprovalDecision, ApprovalId, ApprovalInbox,
        ApprovalRecordStatus, ApprovalRequest, AuthorityContext, CapabilityOrigin, EventStore,
        HarnessError, HarnessFuture, HarnessRuntime, InboxApprovalHandler, Item, ItemId, ItemKind,
        LanguageModel, MemoryApprovalInbox, MemoryEventStore, MemoryScope, MemoryTaskCoordinator,
        ModelContinuation, ModelEventSink, ModelOutput, ModelRequest, ModelResponse, ModelStream,
        ModelStreamEvent, OperationId, PendingEvent, PolicyDecision, PolicyEngine, RiskLevel,
        SnapshotMaintenanceConfig, StateCapacityLevel, StateEngine, StateEvent, StateSnapshot,
        StoredEvent, TaskCompletion, TaskCoordinator, TaskDefinition, TaskGraph, TaskGraphId,
        TaskGraphSnapshot, TaskId, Thread, ThreadId, Tool, ToolAuthorization, ToolCallBatch,
        ToolCallBatchId, ToolContext, ToolDescriptor, ToolRegistry, TurnContextInput, TurnId,
        TurnStatus, WorkspaceMode,
    };

    struct ImmediateModel;

    impl LanguageModel for ImmediateModel {
        fn id(&self) -> &str {
            "test/immediate"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "done".to_owned(),
                })
            })
        }
    }

    struct PendingModel;

    impl LanguageModel for PendingModel {
        fn id(&self) -> &str {
            "test/pending"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(pending())
        }
    }

    struct ApprovalToolCallModel;

    struct PanickingTaskCoordinator;

    impl TaskCoordinator for PanickingTaskCoordinator {
        fn create_as<'a>(
            &'a self,
            _graph_id: TaskGraphId,
            _graph: TaskGraph,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, TaskGraphSnapshot> {
            panic!("sensitive Task Coordinator constructor panic")
        }

        fn load_as<'a>(
            &'a self,
            _graph_id: &'a TaskGraphId,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, Option<TaskGraphSnapshot>> {
            Box::pin(async { panic!("sensitive Task Coordinator poll panic") })
        }

        fn compare_and_swap_as<'a>(
            &'a self,
            _snapshot: TaskGraphSnapshot,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, TaskGraphSnapshot> {
            Box::pin(async { panic!("sensitive Task Coordinator CAS panic") })
        }
    }

    impl LanguageModel for ApprovalToolCallModel {
        fn id(&self) -> &str {
            "test/approval-tool-call"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::ToolCall {
                    call_id: "approval-call".to_owned(),
                    name: "approval-tool".to_owned(),
                    input: json!({}),
                })
            })
        }
    }

    struct ApprovalTool;

    impl Tool for ApprovalTool {
        fn descriptor(&self) -> ToolDescriptor {
            ToolDescriptor {
                name: "approval-tool".to_owned(),
                description: "Exercise the approval identity path".to_owned(),
                input_schema: json!({"type": "object"}),
            }
        }

        fn execute<'a>(
            &'a self,
            _input: serde_json::Value,
            _context: ToolContext,
        ) -> HarnessFuture<'a, serde_json::Value> {
            Box::pin(async { Ok(json!({})) })
        }
    }

    struct AskPolicy;

    impl PolicyEngine for AskPolicy {
        fn authorize<'a>(
            &'a self,
            _request: &'a ToolAuthorization,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, PolicyDecision> {
            Box::pin(async {
                Ok(PolicyDecision::Ask {
                    reason: "independent approval required".to_owned(),
                    risk: RiskLevel::High,
                })
            })
        }
    }

    struct StreamingModel;

    impl LanguageModel for StreamingModel {
        fn id(&self) -> &str {
            "test/streaming"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "hello".to_owned(),
                })
            })
        }

        fn complete_streaming<'a>(
            &'a self,
            _request: ModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, ModelResponse> {
            Box::pin(async move {
                assert!(stream.emit_text_delta("hel"));
                assert!(stream.emit_text_delta("lo"));
                Ok(ModelResponse::from(ModelOutput::Message {
                    content: "hello".to_owned(),
                }))
            })
        }
    }

    struct PanickingAuthorizer;

    impl ProtocolAuthorizer for PanickingAuthorizer {
        fn allows(&self, _principal: &ProtocolPrincipal, _permission: &str) -> bool {
            panic!("authorization fixture panic")
        }
    }

    struct ScopedAuthorizer {
        authority: AuthorityContext,
    }

    impl ProtocolAuthorizer for ScopedAuthorizer {
        fn allows(&self, _principal: &ProtocolPrincipal, _permission: &str) -> bool {
            true
        }

        fn authority_context(
            &self,
            _principal: &ProtocolPrincipal,
        ) -> Result<AuthorityContext, HarnessError> {
            Ok(self.authority.clone())
        }
    }

    struct TenantMapAuthorizer {
        tenants: BTreeMap<String, String>,
    }

    impl ProtocolAuthorizer for TenantMapAuthorizer {
        fn allows(&self, _principal: &ProtocolPrincipal, _permission: &str) -> bool {
            true
        }

        fn authority_context(
            &self,
            principal: &ProtocolPrincipal,
        ) -> Result<AuthorityContext, HarnessError> {
            let fingerprint = principal.mtls_sha256().ok_or_else(|| {
                HarnessError::InvalidConfiguration(
                    "tenant fixture requires a certificate principal".to_owned(),
                )
            })?;
            let tenant_id = self.tenants.get(fingerprint).cloned().ok_or_else(|| {
                HarnessError::InvalidConfiguration("certificate has no tenant mapping".to_owned())
            })?;
            AuthorityContext::new(principal.actor_identity(), Some(tenant_id))
        }
    }

    struct PanickingAuthorityResolver;

    impl ProtocolAuthorizer for PanickingAuthorityResolver {
        fn allows(&self, _principal: &ProtocolPrincipal, _permission: &str) -> bool {
            true
        }

        fn authority_context(
            &self,
            _principal: &ProtocolPrincipal,
        ) -> Result<AuthorityContext, HarnessError> {
            panic!("authority resolver fixture panic")
        }
    }

    struct PanicOnSecondReadStore {
        inner: MemoryEventStore,
        reads: AtomicUsize,
    }

    struct BlockingItemStore {
        inner: MemoryEventStore,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
        blocked: AtomicUsize,
    }

    impl BlockingItemStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                blocked: AtomicUsize::new(0),
            }
        }
    }

    impl EventStore for BlockingItemStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            Box::pin(async move {
                if matches!(&pending.event, crate::StateEvent::ItemAppended { .. })
                    && self.blocked.fetch_add(1, Ordering::SeqCst) == 0
                {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                self.inner.append(pending).await
            })
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

    struct BlockingProtocolSnapshotStore {
        inner: MemoryEventStore,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl BlockingProtocolSnapshotStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            }
        }
    }

    impl EventStore for BlockingProtocolSnapshotStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
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

        fn load_snapshot<'a>(
            &'a self,
            thread_id: &'a ThreadId,
        ) -> HarnessFuture<'a, Option<StateSnapshot>> {
            self.inner.load_snapshot(thread_id)
        }

        fn save_snapshot<'a>(&'a self, snapshot: StateSnapshot) -> HarnessFuture<'a, ()> {
            Box::pin(async move {
                self.entered.notify_one();
                self.release.notified().await;
                self.inner.save_snapshot(snapshot).await
            })
        }
    }

    impl PanicOnSecondReadStore {
        fn new() -> Self {
            Self {
                inner: MemoryEventStore::new(),
                reads: AtomicUsize::new(0),
            }
        }
    }

    impl EventStore for PanicOnSecondReadStore {
        fn append<'a>(&'a self, pending: PendingEvent) -> HarnessFuture<'a, StoredEvent> {
            self.inner.append(pending)
        }

        fn events_page<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            after_sequence: u64,
            limit: usize,
            max_recovery_bytes: u64,
        ) -> HarnessFuture<'a, Vec<StoredEvent>> {
            // StartTurn performs one complete bounded preflight (data page +
            // empty page). Panic on the worker's first projection read.
            if self.reads.fetch_add(1, Ordering::SeqCst) == 2 {
                panic!("sensitive State projection panic")
            }
            self.inner
                .events_page(thread_id, after_sequence, limit, max_recovery_bytes)
        }

        fn thread_accessible<'a>(
            &'a self,
            thread_id: &'a ThreadId,
            tenant_id: Option<String>,
        ) -> HarnessFuture<'a, bool> {
            self.inner.thread_accessible(thread_id, tenant_id)
        }
    }

    fn handler(model: Arc<dyn LanguageModel>) -> ProtocolHandler {
        ProtocolHandler::new(Arc::new(HarnessRuntime::new(
            model,
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        )))
    }

    fn request(id: &str, command: ProtocolCommand) -> ProtocolRequest {
        ProtocolRequest {
            id: id.to_owned(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command,
        }
    }

    fn task_definition(id: &str, dependencies: &[&str]) -> TaskDefinition {
        TaskDefinition {
            id: TaskId::from_string(id.to_owned()),
            description: format!("Task {id}"),
            dependencies: dependencies
                .iter()
                .map(|dependency| TaskId::from_string((*dependency).to_owned()))
                .collect(),
            priority: 0,
            workspace: WorkspaceMode::None,
        }
    }

    #[test]
    fn protocol_twenty_two_wire_envelopes_state_provenance_and_permissions_are_stable() {
        let request_value =
            serde_json::to_value(request("request-1", ProtocolCommand::Initialize {}))
                .expect("encode request");
        assert_eq!(
            request_value,
            json!({
                "id": "request-1",
                "protocol_version": "22",
                "command": { "method": "initialize" }
            })
        );
        assert!(
            serde_json::from_value::<ProtocolRequest>(json!({
                "id": "request-1",
                "protocol_version": "22",
                "command": { "method": "initialize" },
                "unexpected": true
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ProtocolRequest>(json!({
                "id": "request-1",
                "protocol_version": "22",
                "command": {
                    "method": "initialize",
                    "unexpected": true
                }
            }))
            .is_err()
        );
        let response = ProtocolResponse {
            id: Some("request-1".to_owned()),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            body: ProtocolResponseBody::Success {
                result: ProtocolResult::Cancellation {
                    operation_id: OperationId::from_static("operation-fixture"),
                    accepted: true,
                },
            },
        };
        assert_eq!(
            serde_json::to_value(response).expect("encode response"),
            json!({
                "id": "request-1",
                "protocol_version": "22",
                "body": {
                    "status": "success",
                    "result": {
                        "type": "cancellation",
                        "operation_id": "operation-fixture",
                        "accepted": true
                    }
                }
            })
        );
        assert_eq!(
            serde_json::to_value(ItemKind::PolicyDecision {
                call_id: "call-fixture".to_owned(),
                tool_origin: Some(CapabilityOrigin::External {
                    id: "fixture-tool-provider".to_owned(),
                }),
                decision: PolicyDecision::Allow,
            })
            .expect("encode schema-4 Policy evidence"),
            json!({
                "type": "policy_decision",
                "call_id": "call-fixture",
                "tool_origin": {
                    "kind": "external",
                    "id": "fixture-tool-provider"
                },
                "decision": { "action": "allow" }
            })
        );
        assert_eq!(
            serde_json::to_value(ItemKind::ProviderContinuation {
                model_id: "openai/default".to_owned(),
                model_origin: CapabilityOrigin::BuiltIn,
                continuation: ModelContinuation::new(
                    "openai.responses.reasoning.v1",
                    vec![json!({
                        "type": "reasoning",
                        "encrypted_content": "opaque"
                    })],
                )
                .expect("continuation"),
            })
            .expect("encode schema-5 Provider continuation evidence"),
            json!({
                "type": "provider_continuation",
                "model_id": "openai/default",
                "model_origin": {"kind": "built_in"},
                "continuation": {
                    "format": "openai.responses.reasoning.v1",
                    "items": [{
                        "type": "reasoning",
                        "encrypted_content": "opaque"
                    }]
                }
            })
        );
        assert_eq!(
            serde_json::to_value(ItemKind::SteeringQueued {
                steering_id: crate::SteeringId::from_static("steering-fixture"),
                submitted_by: crate::ActorIdentity::LocalProcess,
                content: "correct course".to_owned(),
            })
            .expect("encode schema-6 steering evidence"),
            json!({
                "type": "steering_queued",
                "steering_id": "steering-fixture",
                "submitted_by": {"kind": "local_process"},
                "content": "correct course"
            })
        );
        assert_eq!(
            serde_json::to_value(ModelStreamEvent::StepInvalidated { model_step: 3 })
                .expect("encode provisional-step invalidation"),
            json!({
                "type": "step_invalidated",
                "model_step": 3
            })
        );
        let batch = serde_json::to_value(StateEvent::ToolCallsAppended {
            turn_id: TurnId::from_static("turn-fixture"),
            calls: vec![
                Item {
                    id: ItemId::from_static("item-call-1"),
                    created_at_ms: 1,
                    kind: ItemKind::ToolCall {
                        model_id: Some("openai/default".to_owned()),
                        model_origin: Some(CapabilityOrigin::BuiltIn),
                        call_id: "call-1".to_owned(),
                        name: "echo".to_owned(),
                        input: json!({"text": "first"}),
                        batch: Some(ToolCallBatch {
                            id: ToolCallBatchId::from_static("tool-batch-fixture"),
                            index: 0,
                            size: 2,
                        }),
                    },
                },
                Item {
                    id: ItemId::from_static("item-call-2"),
                    created_at_ms: 2,
                    kind: ItemKind::ToolCall {
                        model_id: Some("openai/default".to_owned()),
                        model_origin: Some(CapabilityOrigin::BuiltIn),
                        call_id: "call-2".to_owned(),
                        name: "echo".to_owned(),
                        input: json!({"text": "second"}),
                        batch: Some(ToolCallBatch {
                            id: ToolCallBatchId::from_static("tool-batch-fixture"),
                            index: 1,
                            size: 2,
                        }),
                    },
                },
            ],
        })
        .expect("encode schema-7 Tool-call batch evidence");
        assert_eq!(batch["type"], "tool_calls_appended");
        assert_eq!(batch["turn_id"], "turn-fixture");
        assert_eq!(batch["calls"][0]["batch"]["index"], 0);
        assert_eq!(batch["calls"][1]["batch"]["index"], 1);
        assert_eq!(batch["calls"][1]["batch"]["size"], 2);

        let created = serde_json::to_value(StateEvent::ThreadCreated {
            created_at_ms: 1_785_000_000_000,
            tenant_id: Some("tenant-a".to_owned()),
        })
        .expect("encode schema-12 Thread tenant evidence");
        assert_eq!(
            created,
            json!({
                "type": "thread_created",
                "created_at_ms": 1_785_000_000_000_u64,
                "tenant_id": "tenant-a"
            })
        );

        let named = serde_json::to_value(StateEvent::ThreadNamed {
            name: Some("Harness design".to_owned()),
        })
        .expect("encode schema-8 Thread name evidence");
        assert_eq!(
            named,
            json!({
                "type": "thread_named",
                "name": "Harness design"
            })
        );

        let forked = serde_json::to_value(StateEvent::ThreadForked {
            lineage: crate::ThreadLineage {
                parent_thread_id: ThreadId::from_static("thread-parent"),
                parent_through_sequence: 42,
                parent_stream_version: 7,
                parent_events_sha256: "0".repeat(64),
            },
        })
        .expect("encode schema-9 Thread lineage evidence");
        assert_eq!(
            forked,
            json!({
                "type": "thread_forked",
                "lineage": {
                    "parent_thread_id": "thread-parent",
                    "parent_through_sequence": 42,
                    "parent_stream_version": 7,
                    "parent_events_sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                }
            })
        );
        let imported = serde_json::to_value(StateEvent::ThreadImported {
            origin: crate::ThreadImportOrigin {
                source_thread_id: ThreadId::from_static("thread-source"),
                source_stream_version: 7,
                source_last_sequence: 42,
                source_events_sha256: "1".repeat(64),
                source_lineage: None,
            },
        })
        .expect("encode schema-10 Thread import evidence");
        assert_eq!(
            imported,
            json!({
                "type": "thread_imported",
                "origin": {
                    "source_thread_id": "thread-source",
                    "source_stream_version": 7,
                    "source_last_sequence": 42,
                    "source_events_sha256": "1111111111111111111111111111111111111111111111111111111111111111"
                }
            })
        );
        let lineage_summary = serde_json::to_value(crate::ThreadSummary {
            thread_id: ThreadId::from_static("thread-child"),
            tenant_id: Some("tenant-a".to_owned()),
            name: Some("Branch".to_owned()),
            lineage: Some(crate::ThreadLineage {
                parent_thread_id: ThreadId::from_static("thread-parent"),
                parent_through_sequence: 42,
                parent_stream_version: 7,
                parent_events_sha256: "0".repeat(64),
            }),
            last_sequence: 50,
            updated_at_ms: 1_785_000_000_000,
            stream_version: 9,
        })
        .expect("encode lineage-aware Thread summary");
        assert_eq!(
            lineage_summary["lineage"]["parent_thread_id"],
            "thread-parent"
        );
        assert_eq!(lineage_summary["tenant_id"], "tenant-a");
        assert_eq!(lineage_summary["lineage"]["parent_stream_version"], 7);
        let turn_context = serde_json::to_value(ProtocolCommand::StartTurn {
            thread_id: "thread-fixture".to_owned(),
            prompt: "continue".to_owned(),
            memory_scope: MemoryScope::default(),
            context: vec![TurnContextInput {
                source: "branch-handoff".to_owned(),
                reference: "thread:source/turn:terminal".to_owned(),
                text: "bounded handoff".to_owned(),
            }],
            timeout_ms: Some(1_000),
        })
        .expect("encode Turn context");
        assert_eq!(turn_context["context"][0]["source"], "branch-handoff");
        assert_eq!(
            turn_context["context"][0]["reference"],
            "thread:source/turn:terminal"
        );

        let commands = [
            (ProtocolCommand::Initialize {}, "initialize", "initialize"),
            (
                ProtocolCommand::CreateThread {},
                "create_thread",
                "thread.create",
            ),
            (
                ProtocolCommand::ForkThread {
                    parent_thread_id: "thread-parent".to_owned(),
                    child_thread_id: "thread-child".to_owned(),
                    through_turn_id: Some("turn-fixture".to_owned()),
                },
                "fork_thread",
                "thread.fork",
            ),
            (
                ProtocolCommand::ListThreads {
                    before_sequence: Some(42),
                    limit: Some(16),
                },
                "list_threads",
                "thread.list",
            ),
            (
                ProtocolCommand::SetThreadName {
                    thread_id: "thread-fixture".to_owned(),
                    name: Some("Harness design".to_owned()),
                },
                "set_thread_name",
                "thread.name",
            ),
            (
                ProtocolCommand::GetThread {
                    thread_id: "thread-fixture".to_owned(),
                },
                "get_thread",
                "thread.get",
            ),
            (
                ProtocolCommand::RecoverThread {
                    thread_id: "thread-fixture".to_owned(),
                    expected_turn_id: "turn-fixture".to_owned(),
                },
                "recover_thread",
                "thread.recover",
            ),
            (
                ProtocolCommand::GetThreadCapacity {
                    thread_id: "thread-fixture".to_owned(),
                },
                "get_thread_capacity",
                "thread.capacity",
            ),
            (
                ProtocolCommand::StartTurn {
                    thread_id: "thread-fixture".to_owned(),
                    prompt: "hello".to_owned(),
                    memory_scope: MemoryScope::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
                "start_turn",
                "turn.start",
            ),
            (
                ProtocolCommand::SteerTurn {
                    thread_id: "thread-fixture".to_owned(),
                    expected_turn_id: "turn-fixture".to_owned(),
                    content: "correct course".to_owned(),
                },
                "steer_turn",
                "turn.steer",
            ),
            (
                ProtocolCommand::GetOperation {
                    operation_id: "operation-fixture".to_owned(),
                },
                "get_operation",
                "operation.get",
            ),
            (
                ProtocolCommand::GetOperationEvents {
                    operation_id: "operation-fixture".to_owned(),
                    after_sequence: Some(7),
                    limit: Some(8),
                },
                "get_operation_events",
                "operation.events",
            ),
            (
                ProtocolCommand::CancelOperation {
                    operation_id: "operation-fixture".to_owned(),
                },
                "cancel_operation",
                "operation.cancel",
            ),
            (
                ProtocolCommand::ForgetOperation {
                    operation_id: "operation-fixture".to_owned(),
                },
                "forget_operation",
                "operation.forget",
            ),
            (
                ProtocolCommand::GetEvents {
                    thread_id: "thread-fixture".to_owned(),
                    after_sequence: Some(7),
                    limit: Some(8),
                },
                "get_events",
                "thread.events",
            ),
            (
                ProtocolCommand::GetPendingApprovals { limit: Some(8) },
                "get_pending_approvals",
                "approval.pending",
            ),
            (
                ProtocolCommand::GetApproval {
                    approval_id: "approval-fixture".to_owned(),
                },
                "get_approval",
                "approval.get",
            ),
            (
                ProtocolCommand::SettleApproval {
                    approval_id: "approval-fixture".to_owned(),
                    expected_revision: 1,
                    decision: ApprovalDecision::Approve,
                },
                "settle_approval",
                "approval.settle",
            ),
            (
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "graph-fixture".to_owned(),
                    definitions: vec![task_definition("task-fixture", &[])],
                },
                "create_task_graph",
                "task.graph.create",
            ),
            (
                ProtocolCommand::GetTaskGraph {
                    graph_id: "graph-fixture".to_owned(),
                },
                "get_task_graph",
                "task.graph.get",
            ),
            (
                ProtocolCommand::GetTaskRecords {
                    graph_id: "graph-fixture".to_owned(),
                    after_task_id: None,
                    limit: Some(8),
                },
                "get_task_records",
                "task.graph.get",
            ),
            (
                ProtocolCommand::CancelTask {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    expected_revision: 1,
                    reason: "operator stop".to_owned(),
                },
                "cancel_task",
                "task.graph.cancel",
            ),
            (
                ProtocolCommand::ClaimTasks {
                    graph_id: "graph-fixture".to_owned(),
                    lease_duration_ms: 1_000,
                    maximum: Some(1),
                },
                "claim_tasks",
                "task.worker.claim",
            ),
            (
                ProtocolCommand::HeartbeatTask {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    lease_id: "lease-fixture".to_owned(),
                    lease_duration_ms: 1_000,
                },
                "heartbeat_task",
                "task.worker.heartbeat",
            ),
            (
                ProtocolCommand::CompleteTask {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    lease_id: "lease-fixture".to_owned(),
                    completion: TaskCompletion {
                        summary: "done".to_owned(),
                        artifacts: Vec::new(),
                    },
                },
                "complete_task",
                "task.worker.complete",
            ),
            (
                ProtocolCommand::FailTask {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    lease_id: "lease-fixture".to_owned(),
                    reason: "failed".to_owned(),
                },
                "fail_task",
                "task.worker.fail",
            ),
            (
                ProtocolCommand::GetTaskMessages {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    lease_id: "lease-fixture".to_owned(),
                    after_sequence: Some(7),
                    limit: Some(8),
                },
                "get_task_messages",
                "task.worker.messages.read",
            ),
            (
                ProtocolCommand::SendTaskMessage {
                    graph_id: "graph-fixture".to_owned(),
                    task_id: "task-fixture".to_owned(),
                    lease_id: "lease-fixture".to_owned(),
                    to: "task-recipient".to_owned(),
                    body: "ready".to_owned(),
                },
                "send_task_message",
                "task.worker.messages.send",
            ),
        ];
        for (command, method, permission) in commands {
            assert_eq!(command.permission(), permission);
            assert_eq!(
                serde_json::to_value(command)
                    .expect("encode command")
                    .get("method"),
                Some(&json!(method))
            );
        }
    }

    #[tokio::test]
    async fn task_capabilities_are_conditional_and_records_are_cursor_bounded() {
        let unconfigured = handler(Arc::new(ImmediateModel))
            .handle(request("init-plain", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            unconfigured.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized {
                    ref capabilities,
                    ..
                }
            } if !capabilities.iter().any(|capability| capability.starts_with("task."))
        ));

        let coordinator = Arc::new(MemoryTaskCoordinator::new());
        let configured =
            handler(Arc::new(ImmediateModel)).with_task_coordinator(coordinator.clone());
        let initialized = configured
            .handle(request("init-task", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized {
                    ref capabilities,
                    ..
                }
            } if capabilities.iter().filter(|capability| capability.starts_with("task.")).count()
                == 9
        ));

        let created = configured
            .handle(request(
                "create-graph",
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "graph-page".to_owned(),
                    definitions: vec![
                        task_definition("task-a", &[]),
                        task_definition("task-b", &[]),
                        task_definition("task-c", &[]),
                    ],
                },
            ))
            .await;
        assert!(matches!(
            created.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraphCreated {
                    ref graph
                }
            } if graph.revision == 1 && graph.task_count == 3 && !graph.terminal
        ));

        let first = configured
            .handle(request(
                "records-first",
                ProtocolCommand::GetTaskRecords {
                    graph_id: "graph-page".to_owned(),
                    after_task_id: None,
                    limit: Some(2),
                },
            ))
            .await;
        let cursor = match first.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskRecords { page },
            } => {
                assert_eq!(page.records.len(), 2);
                assert!(page.has_more);
                page.next_after_task_id
                    .expect("first-page cursor")
                    .to_string()
            }
            other => panic!("unexpected first Task page: {other:?}"),
        };
        let second = configured
            .handle(request(
                "records-second",
                ProtocolCommand::GetTaskRecords {
                    graph_id: "graph-page".to_owned(),
                    after_task_id: Some(cursor),
                    limit: Some(2),
                },
            ))
            .await;
        assert!(matches!(
            second.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskRecords { ref page },
            } if page.records.len() == 1 && !page.has_more
        ));

        let oversized_page = configured
            .handle(request(
                "records-too-many",
                ProtocolCommand::GetTaskRecords {
                    graph_id: "graph-page".to_owned(),
                    after_task_id: None,
                    limit: Some(65),
                },
            ))
            .await;
        assert!(matches!(
            oversized_page.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: false,
                    ..
                }
            } if code == "invalid_request"
        ));

        let oversized_claim = configured
            .handle(request(
                "claims-too-many",
                ProtocolCommand::ClaimTasks {
                    graph_id: "graph-page".to_owned(),
                    lease_duration_ms: 1_000,
                    maximum: Some(17),
                },
            ))
            .await;
        assert!(matches!(
            oversized_claim.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: false,
                    ..
                }
            } if code == "invalid_request"
        ));
    }

    #[tokio::test]
    async fn task_coordinator_panics_are_content_free_protocol_failures() {
        let configured = handler(Arc::new(ImmediateModel))
            .with_task_coordinator(Arc::new(PanickingTaskCoordinator));
        let response = configured
            .handle(request(
                "panic-coordinator",
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "graph-panic".to_owned(),
                    definitions: vec![task_definition("task", &[])],
                },
            ))
            .await;
        assert!(matches!(
            response.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    ref message,
                    retryable: false,
                }
            } if code == "runtime_error"
                && message == "execution error: protocol command execution panicked"
                && !message.contains("sensitive")
        ));
    }

    #[tokio::test]
    async fn authenticated_task_workers_are_principal_derived_fenced_and_recoverable() {
        let worker_a = ProtocolPrincipal::from_mtls_certificate(b"worker-a-certificate");
        let worker_b = ProtocolPrincipal::from_mtls_certificate(b"worker-b-certificate");
        let fingerprint_a = worker_a
            .mtls_sha256()
            .expect("worker A fingerprint")
            .to_owned();
        let fingerprint_b = worker_b
            .mtls_sha256()
            .expect("worker B fingerprint")
            .to_owned();
        let authorizer = FingerprintProtocolAuthorizer::allow_all([
            fingerprint_a.clone(),
            fingerprint_b.clone(),
        ])
        .expect("worker grants");
        let configured = handler(Arc::new(ImmediateModel))
            .with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()))
            .with_authorizer(Arc::new(authorizer));

        let created = configured
            .handle_as(
                &worker_a,
                request(
                    "create-worker-graph",
                    ProtocolCommand::CreateTaskGraph {
                        graph_id: "graph-workers".to_owned(),
                        definitions: vec![
                            task_definition("root", &[]),
                            task_definition("dependent", &["root"]),
                        ],
                    },
                ),
            )
            .await;
        assert!(matches!(
            created.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraphCreated { .. }
            }
        ));

        let claimed_root = configured
            .handle_as(
                &worker_a,
                request(
                    "claim-root",
                    ProtocolCommand::ClaimTasks {
                        graph_id: "graph-workers".to_owned(),
                        lease_duration_ms: 60_000,
                        maximum: Some(1),
                    },
                ),
            )
            .await;
        let root_lease = match claimed_root.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::TasksClaimed {
                        revision,
                        worker,
                        claims,
                        ..
                    },
            } => {
                assert_eq!(revision, 2);
                assert_eq!(worker, fingerprint_a);
                assert_eq!(claims.len(), 1);
                assert_eq!(claims[0].task.id.as_str(), "root");
                assert_eq!(claims[0].lease.owner, worker);
                claims[0].lease.clone()
            }
            other => panic!("unexpected root claim: {other:?}"),
        };

        let stolen = configured
            .handle_as(
                &worker_b,
                request(
                    "steal-root",
                    ProtocolCommand::CompleteTask {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "root".to_owned(),
                        lease_id: root_lease.id.to_string(),
                        completion: TaskCompletion {
                            summary: "forged".to_owned(),
                            artifacts: Vec::new(),
                        },
                    },
                ),
            )
            .await;
        assert!(matches!(
            stolen.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: false,
                    ..
                }
            } if code == "runtime_error"
        ));

        let sent = configured
            .handle_as(
                &worker_a,
                request(
                    "send-dependent",
                    ProtocolCommand::SendTaskMessage {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "root".to_owned(),
                        lease_id: root_lease.id.to_string(),
                        to: "dependent".to_owned(),
                        body: "root output is ready".to_owned(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            sent.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskMessageSent { revision: 3, .. }
            }
        ));

        let completed_root = configured
            .handle_as(
                &worker_a,
                request(
                    "complete-root",
                    ProtocolCommand::CompleteTask {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "root".to_owned(),
                        lease_id: root_lease.id.to_string(),
                        completion: TaskCompletion {
                            summary: "root done".to_owned(),
                            artifacts: Vec::new(),
                        },
                    },
                ),
            )
            .await;
        assert!(matches!(
            completed_root.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskCompleted { revision: 4, .. }
            }
        ));

        let claimed_dependent = configured
            .handle_as(
                &worker_b,
                request(
                    "claim-dependent",
                    ProtocolCommand::ClaimTasks {
                        graph_id: "graph-workers".to_owned(),
                        lease_duration_ms: 1_000,
                        maximum: None,
                    },
                ),
            )
            .await;
        let dependent_lease = match claimed_dependent.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::TasksClaimed {
                        revision,
                        worker,
                        claims,
                        ..
                    },
            } => {
                assert_eq!(revision, 5);
                assert_eq!(worker, fingerprint_b);
                assert_eq!(claims.len(), 1);
                assert_eq!(claims[0].task.id.as_str(), "dependent");
                claims[0].lease.clone()
            }
            other => panic!("unexpected dependent claim: {other:?}"),
        };

        let inbox = configured
            .handle_as(
                &worker_b,
                request(
                    "read-dependent",
                    ProtocolCommand::GetTaskMessages {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "dependent".to_owned(),
                        lease_id: dependent_lease.id.to_string(),
                        after_sequence: None,
                        limit: None,
                    },
                ),
            )
            .await;
        assert!(matches!(
            inbox.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskMessages {
                    revision: 5,
                    ref page,
                    ..
                }
            } if page.messages.len() == 1 && page.messages[0].body == "root output is ready"
        ));

        let heartbeat = configured
            .handle_as(
                &worker_b,
                request(
                    "heartbeat-dependent",
                    ProtocolCommand::HeartbeatTask {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "dependent".to_owned(),
                        lease_id: dependent_lease.id.to_string(),
                        lease_duration_ms: 60_000,
                    },
                ),
            )
            .await;
        assert!(matches!(
            heartbeat.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskHeartbeat {
                    revision: 6,
                    expires_at_ms,
                    ..
                }
            } if expires_at_ms > dependent_lease.expires_at_ms
        ));

        let completed_dependent = configured
            .handle_as(
                &worker_b,
                request(
                    "complete-dependent",
                    ProtocolCommand::CompleteTask {
                        graph_id: "graph-workers".to_owned(),
                        task_id: "dependent".to_owned(),
                        lease_id: dependent_lease.id.to_string(),
                        completion: TaskCompletion {
                            summary: "dependent done".to_owned(),
                            artifacts: Vec::new(),
                        },
                    },
                ),
            )
            .await;
        assert!(matches!(
            completed_dependent.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskCompleted { revision: 7, .. }
            }
        ));

        let terminal = configured
            .handle_as(
                &worker_a,
                request(
                    "get-terminal",
                    ProtocolCommand::GetTaskGraph {
                        graph_id: "graph-workers".to_owned(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            terminal.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraph {
                    graph: Some(TaskGraphSummary {
                        revision: 7,
                        terminal: true,
                        ..
                    })
                }
            }
        ));
    }

    #[tokio::test]
    async fn task_admin_cancel_requires_the_observed_revision() {
        let configured = handler(Arc::new(ImmediateModel))
            .with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()));
        let _created = configured
            .handle(request(
                "create-cancel",
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "graph-cancel".to_owned(),
                    definitions: vec![task_definition("task", &[])],
                },
            ))
            .await;
        let claimed = configured
            .handle(request(
                "claim-cancel",
                ProtocolCommand::ClaimTasks {
                    graph_id: "graph-cancel".to_owned(),
                    lease_duration_ms: 60_000,
                    maximum: None,
                },
            ))
            .await;
        assert!(matches!(
            claimed.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TasksClaimed { revision: 2, .. }
            }
        ));

        let stale = configured
            .handle(request(
                "cancel-stale",
                ProtocolCommand::CancelTask {
                    graph_id: "graph-cancel".to_owned(),
                    task_id: "task".to_owned(),
                    expected_revision: 1,
                    reason: "stale operator".to_owned(),
                },
            ))
            .await;
        assert!(matches!(
            stale.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: true,
                    ..
                }
            } if code == "orchestration_conflict"
        ));

        let current = configured
            .handle(request(
                "cancel-current",
                ProtocolCommand::CancelTask {
                    graph_id: "graph-cancel".to_owned(),
                    task_id: "task".to_owned(),
                    expected_revision: 2,
                    reason: "operator stop".to_owned(),
                },
            ))
            .await;
        assert!(matches!(
            current.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskCancelled { revision: 3, .. }
            }
        ));
    }

    fn approval_request() -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalId::generate(),
            requested_by: ApprovalActor::Authenticated {
                authority: "test-authority".to_owned(),
                subject: "turn-initiator".to_owned(),
            },
            authorization: ToolAuthorization {
                thread_id: ThreadId::generate(),
                turn_id: TurnId::generate(),
                call_id: "call-protocol".to_owned(),
                descriptor: ToolDescriptor {
                    name: "deploy".to_owned(),
                    description: "deploy one artifact".to_owned(),
                    input_schema: json!({"type": "object"}),
                },
                origin: CapabilityOrigin::BuiltIn,
                input: json!({"artifact": "a-1"}),
            },
            reason: "external state change".to_owned(),
            risk: RiskLevel::High,
        }
    }

    #[tokio::test]
    async fn thread_capacity_is_an_additive_authorized_protocol_capability() {
        let handler = handler(Arc::new(ImmediateModel));
        let initialized = handler
            .handle(request("init-capacity", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities.contains(&"thread.capacity".to_owned())
        ));

        let created = handler
            .handle(request("create-capacity", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let response = handler
            .handle(request(
                "get-capacity",
                ProtocolCommand::GetThreadCapacity {
                    thread_id: thread_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            response.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCapacity { capacity }
            } if capacity.used_events == 1
                && capacity.remaining_events == 999_999
                && capacity.level == StateCapacityLevel::Healthy
        ));
    }

    #[tokio::test]
    async fn thread_recovery_is_explicit_idempotent_and_protocol_authorized() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(ImmediateModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state.clone(),
        ));
        let thread = runtime.create_thread().await.expect("create Thread");
        let turn = state
            .start_turn(&thread.id)
            .await
            .expect("start abandoned Turn");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "before worker loss".to_owned(),
                }),
            )
            .await
            .expect("append abandoned input");
        let handler = ProtocolHandler::new(runtime);
        let initialized = handler
            .handle(request("init-recovery", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities.contains(&"thread.recover".to_owned())
        ));

        let stale = handler
            .handle(request(
                "recover-stale-turn",
                ProtocolCommand::RecoverThread {
                    thread_id: thread.id.to_string(),
                    expected_turn_id: "turn-stale".to_owned(),
                },
            ))
            .await;
        assert!(matches!(
            stale.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: false,
                    ..
                }
            } if code == "invalid_request"
        ));
        assert_eq!(
            state
                .load_thread(&thread.id)
                .await
                .expect("load after stale recovery")
                .expect("Thread")
                .turns[0]
                .status,
            TurnStatus::Running
        );

        let recovered = handler
            .handle(request(
                "recover-thread",
                ProtocolCommand::RecoverThread {
                    thread_id: thread.id.to_string(),
                    expected_turn_id: turn.id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            recovered.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadRecovered {
                    thread: Some(ref recovered)
                }
            } if recovered.turns[0].id == turn.id
                && recovered.turns[0].status == TurnStatus::Interrupted
        ));
        let event_count = state
            .events(&thread.id)
            .await
            .expect("recovery events")
            .len();
        let repeated = handler
            .handle(request(
                "recover-thread-again",
                ProtocolCommand::RecoverThread {
                    thread_id: thread.id.to_string(),
                    expected_turn_id: turn.id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            repeated.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadRecovered {
                    thread: Some(ref recovered)
                }
            } if recovered.turns[0].status == TurnStatus::Interrupted
        ));
        assert_eq!(
            state
                .events(&thread.id)
                .await
                .expect("idempotent recovery events")
                .len(),
            event_count
        );
    }

    #[tokio::test]
    async fn thread_recovery_refuses_a_live_operation_in_the_same_host() {
        let handler = handler(Arc::new(PendingModel));
        let created = handler
            .handle(request("create-live", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start-live",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "still owned".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        assert!(matches!(
            &started.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { .. }
            }
        ));
        let refused = handler
            .handle(request(
                "recover-live",
                ProtocolCommand::RecoverThread {
                    thread_id: thread_id.to_string(),
                    expected_turn_id: "turn-live".to_owned(),
                },
            ))
            .await;
        assert!(matches!(
            refused.body,
            ProtocolResponseBody::Error {
                error: ProtocolError {
                    ref code,
                    retryable: false,
                    ..
                }
            } if code == "invalid_request"
        ));
        let report = handler
            .shutdown(Duration::from_secs(2))
            .await
            .expect("shutdown live operation");
        assert_eq!(report.remaining_operations, 0);
    }

    #[tokio::test]
    async fn thread_fork_is_capability_gated_and_retry_identified_by_child() {
        let handler = handler(Arc::new(ImmediateModel));
        let initialized = handler
            .handle(request("init-fork", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities.contains(&"thread.fork".to_owned())
        ));
        let created = handler
            .handle(request("create-parent", ProtocolCommand::CreateThread {}))
            .await;
        let parent_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let child_id = ThreadId::from_static("protocol-fork-child");
        for id in ["fork", "fork-retry"] {
            let forked = handler
                .handle(request(
                    id,
                    ProtocolCommand::ForkThread {
                        parent_thread_id: parent_id.to_string(),
                        child_thread_id: child_id.to_string(),
                        through_turn_id: None,
                    },
                ))
                .await;
            assert!(matches!(
                forked.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::ThreadForked { thread }
                } if thread.id == child_id
                    && thread
                        .lineage
                        .as_ref()
                        .is_some_and(|lineage| lineage.parent_thread_id == parent_id)
                    && thread.turns.is_empty()
            ));
        }
        let listed = handler
            .handle(request(
                "list-fork-lineage",
                ProtocolCommand::ListThreads {
                    before_sequence: None,
                    limit: Some(8),
                },
            ))
            .await;
        assert!(matches!(
            listed.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Threads { threads, .. }
            } if threads.iter().any(|summary| {
                summary.thread_id == child_id
                    && summary
                        .lineage
                        .as_ref()
                        .is_some_and(|lineage| lineage.parent_thread_id == parent_id)
            })
        ));
    }

    #[tokio::test]
    async fn recent_threads_are_capability_gated_and_cursor_bounded() {
        let handler = handler(Arc::new(ImmediateModel));
        let initialized = handler
            .handle(request("init-threads", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities.contains(&"thread.list".to_owned())
        ));

        for id in ["create-first", "create-second"] {
            let created = handler
                .handle(request(id, ProtocolCommand::CreateThread {}))
                .await;
            assert!(matches!(
                created.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::ThreadCreated { .. }
                }
            ));
        }
        let first = handler
            .handle(request(
                "list-first",
                ProtocolCommand::ListThreads {
                    before_sequence: None,
                    limit: Some(1),
                },
            ))
            .await;
        let cursor = match first.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Threads {
                        threads,
                        next_before_sequence: Some(cursor),
                        has_more: true,
                    },
            } if threads.len() == 1 => cursor,
            other => panic!("unexpected Thread page: {other:?}"),
        };
        let second = handler
            .handle(request(
                "list-second",
                ProtocolCommand::ListThreads {
                    before_sequence: Some(cursor),
                    limit: Some(1),
                },
            ))
            .await;
        assert!(matches!(
            second.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Threads {
                    threads,
                    next_before_sequence: None,
                    has_more: false,
                }
            } if threads.len() == 1
        ));
    }

    #[tokio::test]
    async fn thread_names_are_authorized_durable_and_listed() {
        let handler = handler(Arc::new(ImmediateModel));
        let initialized = handler
            .handle(request("init-name", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities.contains(&"thread.name".to_owned())
        ));
        let created = handler
            .handle(request("create-name", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let named = handler
            .handle(request(
                "name",
                ProtocolCommand::SetThreadName {
                    thread_id: thread_id.to_string(),
                    name: Some("Harness design".to_owned()),
                },
            ))
            .await;
        assert!(matches!(
            named.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadNamed {
                    name: Some(ref name)
                }
            } if name == "Harness design"
        ));
        let loaded = handler
            .handle(request(
                "load-name",
                ProtocolCommand::GetThread {
                    thread_id: thread_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            loaded.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Thread {
                    thread: Some(Thread {
                        name: Some(ref name),
                        ..
                    })
                }
            } if name == "Harness design"
        ));
        let listed = handler
            .handle(request(
                "list-name",
                ProtocolCommand::ListThreads {
                    before_sequence: None,
                    limit: Some(1),
                },
            ))
            .await;
        assert!(matches!(
            listed.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Threads { threads, .. }
            } if threads[0].name.as_deref() == Some("Harness design")
        ));
        let rejected = handler
            .handle(request(
                "invalid-name",
                ProtocolCommand::SetThreadName {
                    thread_id: thread_id.to_string(),
                    name: Some(" padded ".to_owned()),
                },
            ))
            .await;
        assert!(matches!(rejected.body, ProtocolResponseBody::Error { .. }));
        let cleared = handler
            .handle(request(
                "clear-name",
                ProtocolCommand::SetThreadName {
                    thread_id: thread_id.to_string(),
                    name: None,
                },
            ))
            .await;
        assert!(matches!(
            cleared.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadNamed { name: None }
            }
        ));
    }

    #[tokio::test]
    async fn starts_and_polls_an_asynchronous_turn() {
        let handler = handler(Arc::new(ImmediateModel));
        let created = handler
            .handle(request("create", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "go".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };

        let mut completed = false;
        for _ in 0..100 {
            let polled = handler
                .handle(request(
                    "poll",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await;
            if matches!(
                polled.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::Operation {
                        operation: OperationStatus::Completed { .. }
                    }
                }
            ) {
                completed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(completed, "operation did not complete");

        let first_page = handler
            .handle(request(
                "events",
                ProtocolCommand::GetEvents {
                    thread_id: thread_id.to_string(),
                    after_sequence: None,
                    limit: Some(1),
                },
            ))
            .await;
        assert!(matches!(
            first_page.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Events {
                    ref events,
                    has_more: true,
                    next_after_sequence: Some(_),
                }
            } if events.len() == 1
        ));

        let forgotten = handler
            .handle(request(
                "forget",
                ProtocolCommand::ForgetOperation {
                    operation_id: operation_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            forgotten.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::OperationForgotten { .. }
            }
        ));
        assert!(matches!(
            handler
                .handle(request(
                    "missing",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await
                .body,
            ProtocolResponseBody::Error { .. }
        ));
    }

    #[tokio::test]
    async fn steering_protocol_requires_the_exact_running_turn_and_persists_acceptance() {
        let handler = handler(Arc::new(PendingModel));
        let thread_id = match handler
            .handle(request("create-steering", ProtocolCommand::CreateThread {}))
            .await
            .body
        {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let operation_id = match handler
            .handle(request(
                "start-steering",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "initial".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(10_000),
                },
            ))
            .await
            .body
        {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };

        let mut running_turn_id = None;
        for _ in 0..100 {
            let loaded = handler
                .handle(request(
                    "load-running-steering",
                    ProtocolCommand::GetThread {
                        thread_id: thread_id.to_string(),
                    },
                ))
                .await;
            if let ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Thread {
                        thread: Some(thread),
                    },
            } = loaded.body
                && let Some(turn) = thread.turns.last()
                && turn.status == TurnStatus::Running
            {
                running_turn_id = Some(turn.id.clone());
                break;
            }
            tokio::task::yield_now().await;
        }
        let running_turn_id = running_turn_id.expect("worker did not expose a running Turn");

        let stale = handler
            .handle(request(
                "steer-stale",
                ProtocolCommand::SteerTurn {
                    thread_id: thread_id.to_string(),
                    expected_turn_id: "turn-stale".to_owned(),
                    content: "must not be accepted".to_owned(),
                },
            ))
            .await;
        assert!(matches!(
            stale.body,
            ProtocolResponseBody::Error { error }
                if error.message.contains("active turn is")
        ));

        let accepted = handler
            .handle(request(
                "steer-current",
                ProtocolCommand::SteerTurn {
                    thread_id: thread_id.to_string(),
                    expected_turn_id: running_turn_id.to_string(),
                    content: "correct course".to_owned(),
                },
            ))
            .await;
        let steering_id = match accepted.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::TurnSteered {
                        steering_id,
                        turn_id,
                    },
            } => {
                assert_eq!(turn_id, running_turn_id);
                steering_id
            }
            other => panic!("unexpected response: {other:?}"),
        };

        let loaded = handler
            .handle(request(
                "load-steered",
                ProtocolCommand::GetThread {
                    thread_id: thread_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            loaded.body,
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Thread {
                        thread: Some(thread)
                    }
            } if thread.turns.last().is_some_and(|turn| {
                turn.items.iter().any(|item| matches!(
                    &item.kind,
                    ItemKind::SteeringQueued {
                        steering_id: persisted,
                        submitted_by: ApprovalActor::LocalProcess,
                        content,
                    } if persisted == &steering_id && content == "correct course"
                ))
            })
        ));

        let cancelled = handler
            .handle(request(
                "cancel-steering",
                ProtocolCommand::CancelOperation {
                    operation_id: operation_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            cancelled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Cancellation { accepted: true, .. }
            }
        ));
    }

    #[tokio::test]
    async fn operation_retention_requires_explicit_terminal_release() {
        let handler = handler(Arc::new(ImmediateModel))
            .with_operation_retention_limit(1)
            .expect("retention limit");
        let first_thread = match handler
            .handle(request("create-retained", ProtocolCommand::CreateThread {}))
            .await
            .body
        {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let second_thread = match handler
            .handle(request("create-blocked", ProtocolCommand::CreateThread {}))
            .await
            .body
        {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let first_operation = match handler
            .handle(request(
                "start-retained",
                ProtocolCommand::StartTurn {
                    thread_id: first_thread.to_string(),
                    prompt: "first".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
            ))
            .await
            .body
        {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };
        let mut completed = false;
        for _ in 0..100 {
            let polled = handler
                .handle(request(
                    "poll-retained",
                    ProtocolCommand::GetOperation {
                        operation_id: first_operation.to_string(),
                    },
                ))
                .await;
            if matches!(
                polled.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::Operation {
                        operation: OperationStatus::Completed { .. }
                    }
                }
            ) {
                completed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(completed, "retained operation did not complete");

        let blocked = handler
            .handle(request(
                "start-blocked",
                ProtocolCommand::StartTurn {
                    thread_id: second_thread.to_string(),
                    prompt: "second".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
            ))
            .await;
        assert!(matches!(
            blocked.body,
            ProtocolResponseBody::Error { error }
                if error.message.contains("operation capacity 1 reached")
        ));
        handler
            .handle(request(
                "forget-retained",
                ProtocolCommand::ForgetOperation {
                    operation_id: first_operation.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            handler
                .handle(request(
                    "start-after-forget",
                    ProtocolCommand::StartTurn {
                        thread_id: second_thread.to_string(),
                        prompt: "second".to_owned(),
                        memory_scope: Default::default(),
                        context: Vec::new(),
                        timeout_ms: Some(1_000),
                    },
                ))
                .await
                .body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { .. }
            }
        ));
    }

    #[tokio::test]
    async fn supervises_operation_task_panics_without_leaking_payloads() {
        let handler = ProtocolHandler::new(Arc::new(HarnessRuntime::new(
            Arc::new(ImmediateModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(Arc::new(PanicOnSecondReadStore::new())),
        )));
        let created = handler
            .handle(request("create-panic", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start-panic",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "go".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };

        for _ in 0..100 {
            let polled = handler
                .handle(request(
                    "poll-panic",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await;
            if let ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Operation {
                        operation: OperationStatus::Failed { error },
                    },
            } = polled.body
            {
                assert!(error.contains("operation task panicked"));
                assert!(!error.contains("sensitive State projection panic"));
                let forgotten = handler
                    .handle(request(
                        "forget-panic",
                        ProtocolCommand::ForgetOperation {
                            operation_id: operation_id.to_string(),
                        },
                    ))
                    .await;
                assert!(matches!(
                    forgotten.body,
                    ProtocolResponseBody::Success {
                        result: ProtocolResult::OperationForgotten { .. }
                    }
                ));
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("supervisor left panicked operation running");
    }

    #[tokio::test]
    async fn durable_approvals_are_capability_gated_and_cas_settled() {
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let approval = approval_request();
        let submitted = inbox
            .submit(approval.clone())
            .await
            .expect("submit approval");
        let handler = handler(Arc::new(ImmediateModel)).with_approval_inbox(inbox.clone());

        let initialized = handler
            .handle(request("init", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized {
                    ref capabilities,
                    ref compatibility,
                    ..
                }
            } if capabilities.contains(&"approval.settle".to_owned())
                && compatibility.engine_version == env!("CARGO_PKG_VERSION")
                && compatibility.state_event_schema == crate::STATE_EVENT_SCHEMA_VERSION
                && compatibility.state_snapshot_schema == crate::STATE_SNAPSHOT_SCHEMA_VERSION
                && compatibility.approval_inbox_schema
                    == crate::APPROVAL_INBOX_SCHEMA_VERSION
                && compatibility.task_graph_schema == crate::TASK_GRAPH_SCHEMA_VERSION
                && compatibility.memory_api == crate::MEMORY_API_VERSION
                && compatibility.token_counter_api == crate::TOKEN_COUNTER_API_VERSION
                && compatibility.conversation_compactor_api
                    == crate::CONVERSATION_COMPACTOR_API_VERSION
                && compatibility.secret_api == crate::SECRET_API_VERSION
                && compatibility.skill_api == crate::SKILL_API_VERSION
                && compatibility.model_gateway_api == crate::MODEL_GATEWAY_API_VERSION
                && compatibility.workspace_provider_api == crate::WORKSPACE_PROVIDER_API_VERSION
        ));

        let pending = handler
            .handle(request(
                "pending",
                ProtocolCommand::GetPendingApprovals { limit: Some(10) },
            ))
            .await;
        assert!(matches!(
            pending.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::PendingApprovals { ref approvals }
            } if approvals.len() == 1 && approvals[0].request.id == approval.id
        ));

        let settled = handler
            .handle(request(
                "settle",
                ProtocolCommand::SettleApproval {
                    approval_id: approval.id.to_string(),
                    expected_revision: submitted.revision,
                    decision: ApprovalDecision::Approve,
                },
            ))
            .await;
        assert!(matches!(
            settled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ApprovalSettled { .. }
            }
        ));

        let conflict = handler
            .handle(request(
                "stale",
                ProtocolCommand::SettleApproval {
                    approval_id: approval.id.to_string(),
                    expected_revision: submitted.revision,
                    decision: ApprovalDecision::Approve,
                },
            ))
            .await;
        assert!(matches!(
            conflict.body,
            ProtocolResponseBody::Error { ref error }
                if error.code == "approval_conflict" && error.retryable
        ));
    }

    #[tokio::test]
    async fn authenticated_requester_cannot_self_approve_but_independent_principal_can() {
        let requester = ProtocolPrincipal::from_mtls_certificate(b"requester-certificate");
        let approver = ProtocolPrincipal::from_mtls_certificate(b"approver-certificate");
        let mut approval = approval_request();
        approval.requested_by = requester.actor_identity();
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let submitted = inbox.submit(approval.clone()).await.expect("submit");
        let authorizer = FingerprintProtocolAuthorizer::allow_all([
            requester.mtls_sha256().expect("requester").to_owned(),
            approver.mtls_sha256().expect("approver").to_owned(),
        ])
        .expect("authorizer");
        let handler = handler(Arc::new(ImmediateModel))
            .with_approval_inbox(inbox)
            .with_authorizer(Arc::new(authorizer));
        let command = || ProtocolCommand::SettleApproval {
            approval_id: approval.id.to_string(),
            expected_revision: submitted.revision,
            decision: ApprovalDecision::Approve,
        };

        let denied = handler
            .handle_as(&requester, request("self-settle", command()))
            .await;
        assert!(matches!(
            denied.body,
            ProtocolResponseBody::Error { ref error }
                if error.code == "runtime_error"
                    && error.message.contains("requester cannot settle")
                    && !error.retryable
        ));

        let settled = handler
            .handle_as(&approver, request("independent-settle", command()))
            .await;
        let expected_subject = approver.mtls_sha256().expect("approver");
        assert!(matches!(
            settled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ApprovalSettled { approval }
            } if matches!(
                approval.status,
                ApprovalRecordStatus::Settled {
                    decided_by: ApprovalActor::Authenticated {
                        ref authority,
                        ref subject
                    },
                    ..
                } if authority == "mtls-certificate-sha256"
                    && subject == expected_subject
            )
        ));
    }

    #[tokio::test]
    async fn start_turn_carries_resolved_actor_into_durable_attribution() {
        let principal = ProtocolPrincipal::from_mtls_certificate(b"turn-requester-certificate");
        let authority = AuthorityContext::new(
            ApprovalActor::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-42".to_owned(),
            },
            None,
        )
        .expect("scoped authority");
        let authorizer = ScopedAuthorizer {
            authority: authority.clone(),
        };
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let approval_handler = InboxApprovalHandler::new(inbox.clone(), Duration::from_millis(10))
            .expect("approval handler");
        let mut tools = ToolRegistry::new();
        tools
            .register(CapabilityOrigin::BuiltIn, Arc::new(ApprovalTool))
            .expect("register tool");
        let runtime = Arc::new(
            HarnessRuntime::new(
                Arc::new(ApprovalToolCallModel),
                tools,
                Arc::new(AskPolicy),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )
            .with_approval_handler(Arc::new(approval_handler)),
        );
        let handler = ProtocolHandler::new(runtime.clone())
            .with_approval_inbox(inbox.clone())
            .with_authorizer(Arc::new(authorizer));
        let created = handler
            .handle_as(
                &principal,
                request("create-approval-thread", ProtocolCommand::CreateThread {}),
            )
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected create response: {other:?}"),
        };
        let started = handler
            .handle_as(
                &principal,
                request(
                    "start-approval-turn",
                    ProtocolCommand::StartTurn {
                        thread_id: thread_id.to_string(),
                        prompt: "request a protected action".to_owned(),
                        memory_scope: Default::default(),
                        context: vec![TurnContextInput {
                            source: "branch-handoff".to_owned(),
                            reference: "thread:source/turn:terminal".to_owned(),
                            text: "derived branch context".to_owned(),
                        }],
                        timeout_ms: None,
                    },
                ),
            )
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected start response: {other:?}"),
        };

        let record = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(record) = inbox.pending(1).await.expect("pending").pop() {
                    break record;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("approval request timeout");
        assert_eq!(record.request.requested_by, authority.actor().clone());
        let projected = runtime
            .load_thread(&thread_id)
            .await
            .expect("load attributed Turn")
            .expect("Thread");
        assert!(projected.turns[0].items.iter().any(|item| {
            matches!(
                &item.kind,
                crate::ItemKind::InvocationContext { submitted_by, .. }
                    if submitted_by == authority.actor()
            )
        }));

        let cancelled = handler
            .handle_as(
                &principal,
                request(
                    "cancel-approval-turn",
                    ProtocolCommand::CancelOperation {
                        operation_id: operation_id.to_string(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            cancelled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Cancellation { accepted: true, .. }
            }
        ));
        let report = handler
            .shutdown(Duration::from_secs(2))
            .await
            .expect("shutdown");
        assert_eq!(report.remaining_operations, 0);
    }

    #[tokio::test]
    async fn certificate_principal_sees_and_executes_only_granted_capabilities() {
        let principal = ProtocolPrincipal::from_mtls_certificate(b"authorized-certificate");
        let fingerprint = principal.mtls_sha256().expect("fingerprint").to_owned();
        let authorizer = FingerprintProtocolAuthorizer::new(BTreeMap::from([(
            fingerprint,
            BTreeSet::from(["initialize".to_owned(), "thread.get".to_owned()]),
        )]))
        .expect("authorizer");
        let handler = handler(Arc::new(ImmediateModel))
            .with_approval_inbox(Arc::new(MemoryApprovalInbox::new()))
            .with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()))
            .with_authorizer(Arc::new(authorizer));

        let initialized = handler
            .handle_as(&principal, request("init", ProtocolCommand::Initialize {}))
            .await;
        assert!(matches!(
            initialized.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. }
            } if capabilities == ["thread.get"]
        ));

        let denied = handler
            .handle_as(
                &principal,
                request("create", ProtocolCommand::CreateThread {}),
            )
            .await;
        assert!(matches!(
            denied.body,
            ProtocolResponseBody::Error { error }
                if error.code == "forbidden" && !error.retryable
        ));

        let unknown = ProtocolPrincipal::from_mtls_certificate(b"unknown-certificate");
        let denied = handler
            .handle_as(
                &unknown,
                request("init-unknown", ProtocolCommand::Initialize {}),
            )
            .await;
        assert!(matches!(
            denied.body,
            ProtocolResponseBody::Error { error } if error.code == "forbidden"
        ));
    }

    #[test]
    fn protocol_authorizer_can_resolve_a_scoped_runtime_authority() {
        let expected = AuthorityContext::new(
            ApprovalActor::Authenticated {
                authority: "enterprise-identity".to_owned(),
                subject: "operator-42".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("scoped authority");
        let handler =
            handler(Arc::new(ImmediateModel)).with_authorizer(Arc::new(ScopedAuthorizer {
                authority: expected.clone(),
            }));
        let principal = ProtocolPrincipal::from_mtls_certificate(b"mapped-certificate");
        assert_eq!(
            handler.resolve_authority(&principal).expect("authority"),
            expected
        );
    }

    #[tokio::test]
    async fn protocol_tenant_fencing_hides_threads_operations_approvals_and_tasks() {
        let tenant_a = ProtocolPrincipal::from_mtls_certificate(b"tenant-a-certificate");
        let tenant_a_approver =
            ProtocolPrincipal::from_mtls_certificate(b"tenant-a-approver-certificate");
        let tenant_b = ProtocolPrincipal::from_mtls_certificate(b"tenant-b-certificate");
        let authorizer = TenantMapAuthorizer {
            tenants: BTreeMap::from([
                (
                    tenant_a
                        .mtls_sha256()
                        .expect("tenant A fingerprint")
                        .to_owned(),
                    "tenant-a".to_owned(),
                ),
                (
                    tenant_a_approver
                        .mtls_sha256()
                        .expect("tenant A approver fingerprint")
                        .to_owned(),
                    "tenant-a".to_owned(),
                ),
                (
                    tenant_b
                        .mtls_sha256()
                        .expect("tenant B fingerprint")
                        .to_owned(),
                    "tenant-b".to_owned(),
                ),
            ]),
        };
        let inbox = Arc::new(MemoryApprovalInbox::new());
        let mut approval_a = approval_request();
        approval_a.requested_by = tenant_a.actor_identity();
        let tenant_a_authority =
            AuthorityContext::new(tenant_a.actor_identity(), Some("tenant-a".to_owned()))
                .expect("tenant A authority");
        let submitted_a = inbox
            .submit_as(approval_a.clone(), &tenant_a_authority)
            .await
            .expect("submit tenant A approval");
        let mut approval_b = approval_request();
        approval_b.requested_by = tenant_b.actor_identity();
        let tenant_b_authority =
            AuthorityContext::new(tenant_b.actor_identity(), Some("tenant-b".to_owned()))
                .expect("tenant B authority");
        inbox
            .submit_as(approval_b.clone(), &tenant_b_authority)
            .await
            .expect("submit tenant B approval");
        let handler = handler(Arc::new(ImmediateModel))
            .with_approval_inbox(inbox)
            .with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()))
            .with_authorizer(Arc::new(authorizer));

        let initialized = handler
            .handle_as(
                &tenant_a,
                request("initialize-tenant-a", ProtocolCommand::Initialize {}),
            )
            .await;
        match initialized.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { capabilities, .. },
            } => {
                assert!(
                    capabilities
                        .iter()
                        .any(|capability| capability == "approval.settle")
                );
                assert!(
                    capabilities
                        .iter()
                        .any(|capability| capability == "task.graph.create")
                );
            }
            other => panic!("unexpected initialize response: {other:?}"),
        }

        let created = handler
            .handle_as(
                &tenant_a,
                request("create-tenant-a", ProtocolCommand::CreateThread {}),
            )
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => {
                assert_eq!(thread.tenant_id(), Some("tenant-a"));
                thread.id
            }
            other => panic!("unexpected create response: {other:?}"),
        };

        let hidden = handler
            .handle_as(
                &tenant_b,
                request(
                    "get-cross-tenant",
                    ProtocolCommand::GetThread {
                        thread_id: thread_id.to_string(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            hidden.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Thread { thread: None }
            }
        ));
        let listed = handler
            .handle_as(
                &tenant_b,
                request(
                    "list-cross-tenant",
                    ProtocolCommand::ListThreads {
                        before_sequence: None,
                        limit: None,
                    },
                ),
            )
            .await;
        assert!(matches!(
            listed.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Threads { threads, .. }
            } if threads.is_empty()
        ));

        let started = handler
            .handle_as(
                &tenant_a,
                request(
                    "start-tenant-a",
                    ProtocolCommand::StartTurn {
                        thread_id: thread_id.to_string(),
                        prompt: "hello".to_owned(),
                        memory_scope: Default::default(),
                        context: Vec::new(),
                        timeout_ms: Some(1_000),
                    },
                ),
            )
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected start response: {other:?}"),
        };
        for (request_id, command) in [
            (
                "get-cross-tenant-operation",
                ProtocolCommand::GetOperation {
                    operation_id: operation_id.to_string(),
                },
            ),
            (
                "stream-cross-tenant-operation",
                ProtocolCommand::GetOperationEvents {
                    operation_id: operation_id.to_string(),
                    after_sequence: None,
                    limit: None,
                },
            ),
            (
                "cancel-cross-tenant-operation",
                ProtocolCommand::CancelOperation {
                    operation_id: operation_id.to_string(),
                },
            ),
            (
                "forget-cross-tenant-operation",
                ProtocolCommand::ForgetOperation {
                    operation_id: operation_id.to_string(),
                },
            ),
        ] {
            let hidden_operation = handler
                .handle_as(&tenant_b, request(request_id, command))
                .await;
            assert!(matches!(
                hidden_operation.body,
                ProtocolResponseBody::Error { error } if error.code == "invalid_request"
            ));
        }

        let pending = handler
            .handle_as(
                &tenant_a,
                request(
                    "pending-tenant-a",
                    ProtocolCommand::GetPendingApprovals { limit: None },
                ),
            )
            .await;
        assert!(matches!(
            pending.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::PendingApprovals { approvals }
            } if approvals.len() == 1
                && approvals[0].request.id == approval_a.id
                && approvals[0].tenant_id() == Some("tenant-a")
        ));
        let hidden_approval = handler
            .handle_as(
                &tenant_b,
                request(
                    "get-cross-tenant-approval",
                    ProtocolCommand::GetApproval {
                        approval_id: approval_a.id.to_string(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            hidden_approval.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Approval { approval: None }
            }
        ));
        let denied_settlement = handler
            .handle_as(
                &tenant_b,
                request(
                    "settle-cross-tenant-approval",
                    ProtocolCommand::SettleApproval {
                        approval_id: approval_a.id.to_string(),
                        expected_revision: submitted_a.revision,
                        decision: ApprovalDecision::Approve,
                    },
                ),
            )
            .await;
        assert!(matches!(
            denied_settlement.body,
            ProtocolResponseBody::Error { error }
                if error.message.contains("does not exist")
        ));
        let settled = handler
            .handle_as(
                &tenant_a_approver,
                request(
                    "settle-same-tenant-approval",
                    ProtocolCommand::SettleApproval {
                        approval_id: approval_a.id.to_string(),
                        expected_revision: submitted_a.revision,
                        decision: ApprovalDecision::Approve,
                    },
                ),
            )
            .await;
        assert!(matches!(
            settled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::ApprovalSettled { approval }
            } if approval.tenant_id() == Some("tenant-a")
        ));
        let created_task = handler
            .handle_as(
                &tenant_a,
                request(
                    "create-task-tenant-a",
                    ProtocolCommand::CreateTaskGraph {
                        graph_id: "tenant-graph".to_owned(),
                        definitions: vec![TaskDefinition {
                            id: TaskId::from_static("task-a"),
                            description: "tenant work".to_owned(),
                            dependencies: BTreeSet::new(),
                            priority: 0,
                            workspace: WorkspaceMode::Isolated,
                        }],
                    },
                ),
            )
            .await;
        assert!(matches!(
            created_task.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraphCreated {
                    graph: TaskGraphSummary { tenant_id, .. }
                }
            } if tenant_id.as_deref() == Some("tenant-a")
        ));
        let hidden_task = handler
            .handle_as(
                &tenant_b,
                request(
                    "get-cross-tenant-task",
                    ProtocolCommand::GetTaskGraph {
                        graph_id: "tenant-graph".to_owned(),
                    },
                ),
            )
            .await;
        assert!(matches!(
            hidden_task.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraph { graph: None }
            }
        ));
        let tenant_b_same_id = handler
            .handle_as(
                &tenant_b,
                request(
                    "create-task-tenant-b",
                    ProtocolCommand::CreateTaskGraph {
                        graph_id: "tenant-graph".to_owned(),
                        definitions: vec![TaskDefinition {
                            id: TaskId::from_static("task-b"),
                            description: "other tenant work".to_owned(),
                            dependencies: BTreeSet::new(),
                            priority: 0,
                            workspace: WorkspaceMode::Isolated,
                        }],
                    },
                ),
            )
            .await;
        assert!(matches!(
            tenant_b_same_id.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TaskGraphCreated {
                    graph: TaskGraphSummary { tenant_id, .. }
                }
            } if tenant_id.as_deref() == Some("tenant-b")
        ));
    }

    #[tokio::test]
    async fn authorization_panic_fails_closed_before_command_execution() {
        let handler =
            handler(Arc::new(ImmediateModel)).with_authorizer(Arc::new(PanickingAuthorizer));
        let response = handler
            .handle(request("create", ProtocolCommand::CreateThread {}))
            .await;

        assert!(matches!(
            response.body,
            ProtocolResponseBody::Error { error } if error.code == "forbidden"
        ));
    }

    #[tokio::test]
    async fn authority_resolution_panic_fails_closed_before_command_execution() {
        let handler =
            handler(Arc::new(ImmediateModel)).with_authorizer(Arc::new(PanickingAuthorityResolver));
        let response = handler
            .handle(request("create", ProtocolCommand::CreateThread {}))
            .await;

        assert!(matches!(
            response.body,
            ProtocolResponseBody::Error { error } if error.code == "runtime_error"
        ));
    }

    #[tokio::test]
    async fn cancel_command_reaches_the_running_turn() {
        let handler = handler(Arc::new(PendingModel));
        let created = handler
            .handle(request("create", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "wait".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };
        let cancelled = handler
            .handle(request(
                "cancel",
                ProtocolCommand::CancelOperation {
                    operation_id: operation_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            cancelled.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Cancellation { accepted: true, .. }
            }
        ));

        for _ in 0..100 {
            let polled = handler
                .handle(request(
                    "poll",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await;
            if matches!(
                polled.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::Operation {
                        operation: OperationStatus::Cancelled { .. }
                    }
                }
            ) {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("operation did not cancel");
    }

    #[tokio::test]
    async fn shutdown_rejects_new_turns_and_drains_running_operations() {
        let handler = handler(Arc::new(PendingModel));
        assert!(handler.shutdown(Duration::ZERO).await.is_err());
        let created = handler
            .handle(request(
                "create-before-shutdown",
                ProtocolCommand::CreateThread {},
            ))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start-before-shutdown",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "wait".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };

        let report = handler
            .shutdown(Duration::from_secs(1))
            .await
            .expect("shutdown");
        assert_eq!(report.cancellation_requests, 1);
        assert_eq!(report.settled_operations, 1);
        assert_eq!(report.remaining_operations, 0);
        assert!(report.background_work_drained);
        let operation = handler
            .handle(request(
                "poll-after-shutdown",
                ProtocolCommand::GetOperation {
                    operation_id: operation_id.to_string(),
                },
            ))
            .await;
        assert!(matches!(
            operation.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Operation {
                    operation: OperationStatus::Cancelled { .. }
                }
            }
        ));
        let rejected = handler
            .handle(request(
                "start-after-shutdown",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "again".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        assert!(matches!(
            rejected.body,
            ProtocolResponseBody::Error { error }
                if error.message.contains("shutting down")
        ));
        let repeated = handler
            .shutdown(Duration::from_secs(1))
            .await
            .expect("idempotent shutdown");
        assert_eq!(repeated.cancellation_requests, 0);
        assert_eq!(repeated.settled_operations, 0);
        assert_eq!(repeated.remaining_operations, 0);
        assert!(repeated.background_work_drained);
    }

    #[tokio::test]
    async fn shutdown_reports_uninterruptible_state_work_without_forced_success() {
        let store = Arc::new(BlockingItemStore::new());
        let handler = ProtocolHandler::new(Arc::new(HarnessRuntime::new(
            Arc::new(ImmediateModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            StateEngine::new(store.clone()),
        )));
        let created = handler
            .handle(request("create-blocked", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start-blocked",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "block in State".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        assert!(matches!(
            started.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { .. }
            }
        ));
        tokio::time::timeout(Duration::from_secs(1), store.entered.notified())
            .await
            .expect("State append entered");

        let timed_out = handler
            .shutdown(Duration::from_millis(5))
            .await
            .expect("bounded shutdown");
        assert_eq!(timed_out.cancellation_requests, 1);
        assert_eq!(timed_out.settled_operations, 0);
        assert_eq!(timed_out.remaining_operations, 1);
        assert!(!timed_out.background_work_drained);

        store.release.notify_one();
        let drained = handler
            .shutdown(Duration::from_secs(1))
            .await
            .expect("drain after State release");
        assert_eq!(drained.cancellation_requests, 1);
        assert_eq!(drained.settled_operations, 1);
        assert_eq!(drained.remaining_operations, 0);
        assert!(drained.background_work_drained);
    }

    #[tokio::test]
    async fn shutdown_uses_the_same_deadline_to_drain_snapshot_maintenance() {
        let store = Arc::new(BlockingProtocolSnapshotStore::new());
        let state = StateEngine::new(store.clone()).with_snapshot_maintenance(
            SnapshotMaintenanceConfig::new(1, 1).expect("snapshot policy"),
        );
        let handler = ProtocolHandler::new(Arc::new(HarnessRuntime::new(
            Arc::new(ImmediateModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state,
        )));
        let created = handler
            .handle(request("create-snapshot", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start-snapshot",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "finish".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: None,
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };
        tokio::time::timeout(Duration::from_secs(1), store.entered.notified())
            .await
            .expect("snapshot entered");
        let mut completed = false;
        for _ in 0..100 {
            let response = handler
                .handle(request(
                    "poll-snapshot",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await;
            if matches!(
                response.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::Operation {
                        operation: OperationStatus::Completed { .. }
                    }
                }
            ) {
                completed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            completed,
            "Operation did not settle before maintenance drain"
        );

        let timed_out = handler
            .shutdown(Duration::from_millis(5))
            .await
            .expect("bounded maintenance drain");
        assert_eq!(timed_out.remaining_operations, 0);
        assert!(!timed_out.background_work_drained);
        store.release.notify_one();
        let drained = handler
            .shutdown(Duration::from_secs(1))
            .await
            .expect("maintenance drain");
        assert!(drained.background_work_drained);
    }

    #[tokio::test]
    async fn streams_bounded_cursor_events_without_changing_final_settlement() {
        let handler = handler(Arc::new(StreamingModel));
        let created = handler
            .handle(request("create", ProtocolCommand::CreateThread {}))
            .await;
        let thread_id = match created.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::ThreadCreated { thread },
            } => thread.id,
            other => panic!("unexpected response: {other:?}"),
        };
        let started = handler
            .handle(request(
                "start",
                ProtocolCommand::StartTurn {
                    thread_id: thread_id.to_string(),
                    prompt: "stream".to_owned(),
                    memory_scope: Default::default(),
                    context: Vec::new(),
                    timeout_ms: Some(1_000),
                },
            ))
            .await;
        let operation_id = match started.body {
            ProtocolResponseBody::Success {
                result: ProtocolResult::TurnStarted { operation_id },
            } => operation_id,
            other => panic!("unexpected response: {other:?}"),
        };
        let mut completed = false;
        for _ in 0..100 {
            let polled = handler
                .handle(request(
                    "poll",
                    ProtocolCommand::GetOperation {
                        operation_id: operation_id.to_string(),
                    },
                ))
                .await;
            if matches!(
                polled.body,
                ProtocolResponseBody::Success {
                    result: ProtocolResult::Operation {
                        operation: OperationStatus::Completed {
                            ref final_text,
                            ..
                        }
                    }
                } if final_text == "hello"
            ) {
                completed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(completed, "streaming operation did not complete");

        let streamed = handler
            .handle(request(
                "stream",
                ProtocolCommand::GetOperationEvents {
                    operation_id: operation_id.to_string(),
                    after_sequence: None,
                    limit: Some(1),
                },
            ))
            .await;
        assert!(matches!(
            streamed.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::OperationEvents {
                    ref events,
                    next_after_sequence: Some(1),
                    has_more: true,
                    dropped_through_sequence: None,
                }
            } if events == &[super::OperationStreamEvent {
                sequence: 1,
                event: ModelStreamEvent::TextDelta {
                    model_step: 1,
                    delta: "hel".to_owned(),
                },
            }]
        ));
    }

    #[test]
    fn operation_stream_buffer_reports_evicted_cursor_gap() {
        let buffer = OperationEventBuffer::default();
        for _ in 0..=MAX_OPERATION_STREAM_EVENTS {
            buffer
                .emit(&ModelStreamEvent::TextDelta {
                    model_step: 1,
                    delta: "x".to_owned(),
                })
                .expect("emit");
        }
        let (events, has_more, dropped) = buffer.page(0, 1).expect("page");
        assert_eq!(events[0].sequence, 2);
        assert!(has_more);
        assert_eq!(dropped, Some(1));
    }

    #[tokio::test]
    async fn state_event_pages_stop_before_the_response_byte_budget() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let runtime = Arc::new(HarnessRuntime::new(
            Arc::new(ImmediateModel),
            ToolRegistry::new(),
            Arc::new(AllowListPolicy::deny_by_default()),
            state.clone(),
        ));
        let handler = ProtocolHandler::new(runtime);
        let thread = state.create_thread().await.expect("thread");
        let turn = state.start_turn(&thread.id).await.expect("turn");
        for _ in 0..3 {
            state
                .append_item(
                    &turn,
                    Item::new(ItemKind::UserMessage {
                        content: "x".repeat(6 * 1_048_576),
                    }),
                )
                .await
                .expect("large bounded event");
        }
        state
            .finish_turn(&turn, TurnStatus::Completed)
            .await
            .expect("finish");

        let response = handler
            .handle(request(
                "large-events",
                ProtocolCommand::GetEvents {
                    thread_id: thread.id.to_string(),
                    after_sequence: None,
                    limit: Some(32),
                },
            ))
            .await;
        let cursor = match &response.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Events {
                        events,
                        next_after_sequence: Some(cursor),
                        has_more: true,
                    },
            } => {
                assert!(events.len() < 6);
                *cursor
            }
            other => panic!("unexpected response: {other:?}"),
        };
        let mut encoded = Vec::new();
        write_response(&mut encoded, &response)
            .await
            .expect("byte-bounded response");
        assert!(encoded.len() <= MAX_RESPONSE_FRAME_BYTES + 1);
        assert!(matches!(
            serde_json::from_slice::<ProtocolResponse>(&encoded)
                .expect("decode byte-bounded response")
                .body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Events { has_more: true, .. }
            }
        ));

        assert!(matches!(
            handler
                .handle(request(
                    "remaining-events",
                    ProtocolCommand::GetEvents {
                        thread_id: thread.id.to_string(),
                        after_sequence: Some(cursor),
                        limit: Some(32),
                    },
                ))
                .await
                .body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Events {
                    ref events,
                    has_more: false,
                    ..
                }
            } if !events.is_empty() && events[0].sequence > cursor
        ));
    }

    #[tokio::test]
    async fn rejects_version_mismatch_and_oversized_frames() {
        let handler = handler(Arc::new(ImmediateModel));
        let mut invalid = request("version", ProtocolCommand::Initialize {});
        invalid.protocol_version = "99".to_owned();
        assert!(matches!(
            handler.handle(invalid).await.body,
            ProtocolResponseBody::Error { .. }
        ));

        let bytes = format!("{}\n", "x".repeat(9));
        let mut reader = BufReader::new(bytes.as_bytes());
        assert!(matches!(
            read_bounded_frame(&mut reader, 8).await.expect("read"),
            FrameRead::TooLong
        ));

        let response = ProtocolResponse {
            id: Some("large".to_owned()),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            body: ProtocolResponseBody::Success {
                result: ProtocolResult::Operation {
                    operation: OperationStatus::Completed {
                        thread_id: ThreadId::from_static("thread-test"),
                        turn_id: TurnId::from_static("turn-test"),
                        final_text: "x".repeat(MAX_RESPONSE_FRAME_BYTES),
                    },
                },
            },
        };
        let mut output = Vec::new();
        write_response(&mut output, &response)
            .await
            .expect("bounded response");
        let decoded: ProtocolResponse =
            serde_json::from_slice(&output).expect("decode fallback response");
        assert!(matches!(
            decoded.body,
            ProtocolResponseBody::Error {
                error: super::ProtocolError { ref code, .. }
            } if code == "response_too_large"
        ));

        let retrievable = ProtocolResponse {
            id: Some("model-limit".to_owned()),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            body: ProtocolResponseBody::Success {
                result: ProtocolResult::Operation {
                    operation: OperationStatus::Completed {
                        thread_id: ThreadId::from_static("thread-model-limit"),
                        turn_id: TurnId::from_static("turn-model-limit"),
                        final_text: "x".repeat(1_048_576),
                    },
                },
            },
        };
        let mut output = Vec::new();
        write_response(&mut output, &retrievable)
            .await
            .expect("runtime-limit response");
        assert!(matches!(
            serde_json::from_slice::<ProtocolResponse>(&output)
                .expect("decode runtime-limit response")
                .body,
            ProtocolResponseBody::Success { .. }
        ));

        let invalid_id = handler
            .handle(request(
                "bad-id",
                ProtocolCommand::GetOperation {
                    operation_id: " ".repeat(2),
                },
            ))
            .await;
        assert!(matches!(
            invalid_id.body,
            ProtocolResponseBody::Error { .. }
        ));

        let unknown = OperationId::from_static("operation-not-found");
        assert!(matches!(
            handler
                .handle(request(
                    "unknown",
                    ProtocolCommand::GetOperation {
                        operation_id: unknown.to_string(),
                    },
                ))
                .await
                .body,
            ProtocolResponseBody::Error { .. }
        ));
    }
}
