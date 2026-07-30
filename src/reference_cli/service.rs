//! Minimal project configuration and persistent stdio service host.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::{self as tokio_io, BufReader as TokioBufReader, BufWriter as TokioBufWriter};
use y_harness::{
    APPROVAL_INBOX_SCHEMA_VERSION, ActorIdentity, AgentMemoryHubProvider, AllowListPolicy,
    ApprovalInbox, AuthorityContext, CONVERSATION_COMPACTOR_API_VERSION, CancellationToken,
    CapabilityOrigin, ContextEngine, ConversationCompactionConfig, ConversationCompactorDescriptor,
    ConversationCompactorRegistry, ConversationContextConfig, DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP,
    DEFAULT_MAX_PARALLEL_TOOL_CALLS, DigestLockedProcessBroker, EFFECT_LEDGER_SCHEMA_VERSION,
    EVALUATION_FORMAT_VERSION, EffectCoordinator, EffectEngine, EffectSecretEnvironment,
    EvaluationBaseline, EvaluationCase, EvaluationEngine, EvaluationReport, EvaluationSuite,
    EvaluationTarget, GraderDescriptor, GraderRegistry, HUMAN_HANDOFF_SCHEMA_VERSION, HarnessError,
    HarnessFuture, HarnessRuntime, HumanHandoffCoordinator, HumanHandoffEngine,
    HumanHandoffSubject, HumanHandoffSubjectResolver, InboxApprovalHandler,
    JSON_COMMAND_MAX_INPUT_BYTES, JsonCommandConversationCompactor, JsonCommandGrader,
    JsonCommandModel, JsonCommandModelProtocol, JsonCommandTool, JsonCommandVerifier,
    JsonProcessConfig, LanguageModel, LocalProcessBroker, MAX_MODEL_ATTEMPTS_PER_STEP,
    MAX_PARALLEL_TOOL_CALLS, MAX_THREAD_ARCHIVE_BYTES, MacOsSeatbeltBroker, McpClient,
    MemoryContextConfig, MemoryEventStore, MemoryFailureMode, MemoryHealthStatus, MemoryProvider,
    MemoryRegistry, ModelRegistry, ModelRetryPolicy, NetworkAccess, PROTOCOL_VERSION,
    ProcessBroker, ProtocolAuthorizer, ProtocolHandler, ProtocolPrincipal, SECRET_API_VERSION,
    STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION, SignedSkillPackage, SkillEngine,
    SkillId, SkillPackage, SkillPublisherPolicy, SkillRegistry, SkillTransparencyRequirement,
    SkillTrustStore, SqliteApprovalInbox, SqliteEffectCoordinator, SqliteEventStore,
    SqliteHumanHandoffCoordinator, SqliteTaskCoordinator, SqliteWorkflowCoordinator, StateEngine,
    StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority, TASK_GRAPH_SCHEMA_VERSION,
    TaskCoordinator, TemporalDriver, ThreadId, ToolBatchExecution, ToolDescriptor, ToolRegistry,
    TurnExecutionOptions, TurnOutcome, VerificationRegistry, VerifierDescriptor,
    WORKFLOW_RUN_SCHEMA_VERSION, WorkflowCoordinator, WorkflowEngine, decode_thread_archive,
    encode_thread_archive, register_selected_mcp_tools, serve_jsonl,
};

use y_harness::{
    EnvironmentSecretProvider, SecretProvider, SecretReference, TenantEnvironmentSecretProvider,
};
#[cfg(any(feature = "https-mcp", feature = "https-model"))]
use y_harness::{SecretRequest, SecretServiceUse, SecretUseContext};

#[cfg(feature = "https-mcp")]
use y_harness::{HttpsJsonMcpClient, HttpsJsonMcpConfig};
#[cfg(feature = "https-model")]
use y_harness::{
    HttpsJsonModel, HttpsJsonModelConfig, OpenAiResponsesModel, OpenAiResponsesModelConfig,
};
#[cfg(feature = "https-skill")]
use y_harness::{HttpsSkillSource, HttpsSkillSourceConfig};

use super::{
    CliResult, DemoModel, EchoTool,
    effect_service::{self, ServiceEffectConsumerConfig, build as build_effect_consumer},
    temporal_service::{self, ServiceTemporalConfig},
};

const CONFIG_FILE: &str = "y-harness.json";
const CONFIG_SCHEMA_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: u64 = 65_536;
const MAX_SKILL_PACKAGE_FILE_BYTES: u64 = 16_777_216;
const MAX_PROJECT_SKILL_FILES: usize = 4_096;
const MAX_PINNED_COMMAND_BYTES: u64 = 268_435_456;
const MAX_EVALUATION_ARTIFACT_BYTES: u64 = 16_777_216;
const OPERATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(any(feature = "https-mcp", feature = "https-model"))]
const MAX_CA_BYTES: u64 = 1_048_576;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceConfig {
    schema_version: u32,
    data_directory: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    authority: Option<ServiceAuthorityConfig>,
    #[serde(default = "default_max_parallel_tool_calls")]
    max_parallel_tool_calls: usize,
    #[serde(default = "default_max_model_attempts_per_step")]
    max_model_attempts_per_step: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<ServiceModelConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    models: Vec<ServiceModelConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_route: Option<ServiceModelRouteConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ServiceToolConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    verifiers: Vec<ServiceVerifierConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mcp_servers: Vec<ServiceMcpServerConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    https_mcp_servers: Vec<ServiceHttpsMcpServerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory: Option<ServiceMemoryConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    conversation: Option<ServiceConversationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skills: Option<ServiceSkillsConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    evaluation: Option<ServiceEvaluationConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    temporal: Option<ServiceTemporalConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    effect_consumer: Option<ServiceEffectConsumerConfig>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            data_directory: ".y-harness".to_owned(),
            authority: None,
            max_parallel_tool_calls: DEFAULT_MAX_PARALLEL_TOOL_CALLS,
            max_model_attempts_per_step: DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP,
            model: Some(ServiceModelConfig::Demo {
                id: default_demo_model_id(),
            }),
            models: Vec::new(),
            model_route: None,
            tools: Vec::new(),
            verifiers: Vec::new(),
            mcp_servers: Vec::new(),
            https_mcp_servers: Vec::new(),
            memory: None,
            conversation: None,
            skills: None,
            evaluation: None,
            temporal: None,
            effect_consumer: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
/// Trusted deployment authority selected before the stdio service accepts input.
enum ServiceAuthorityConfig {
    /// Treat every local request as belonging to one exact tenant.
    LocalProcessTenant { tenant_id: String },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceModelConfig {
    Demo {
        #[serde(default = "default_demo_model_id")]
        id: String,
    },
    JsonCommand {
        id: String,
        #[serde(default)]
        protocol: JsonCommandModelProtocol,
        process: ServiceJsonProcessConfig,
    },
    OpenAiResponses {
        id: String,
        model: String,
        api_key_secret_reference: String,
        api_key_environment: String,
        #[serde(default = "default_openai_request_timeout_ms")]
        request_timeout_ms: u64,
        #[serde(default = "default_connect_timeout_ms")]
        connect_timeout_ms: u64,
        #[serde(default = "default_openai_max_response_bytes")]
        max_response_bytes: usize,
        #[serde(default = "default_max_concurrency")]
        max_concurrency: usize,
    },
    HttpsJsonGateway {
        id: String,
        endpoint: String,
        bearer_secret_reference: String,
        bearer_environment: String,
        #[serde(default = "default_request_timeout_ms")]
        request_timeout_ms: u64,
        #[serde(default = "default_connect_timeout_ms")]
        connect_timeout_ms: u64,
        #[serde(default = "default_max_response_bytes")]
        max_response_bytes: usize,
        #[serde(default = "default_max_concurrency")]
        max_concurrency: usize,
        #[serde(default)]
        exclusive_root_ca_pem_path: Option<String>,
    },
}

impl ServiceModelConfig {
    fn id(&self) -> &str {
        match self {
            Self::Demo { id }
            | Self::JsonCommand { id, .. }
            | Self::OpenAiResponses { id, .. }
            | Self::HttpsJsonGateway { id, .. } => id,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceModelRouteConfig {
    models: Vec<String>,
    #[serde(default = "default_model_attempt_timeout_ms")]
    attempt_timeout_ms: u64,
    #[serde(default)]
    timeout_cooldown_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry: Option<ServiceModelRetryConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceModelRetryConfig {
    max_retries: u8,
    #[serde(default = "default_model_retry_initial_delay_ms")]
    initial_delay_ms: u64,
    #[serde(default = "default_model_retry_max_delay_ms")]
    max_delay_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceToolConfig {
    JsonCommand {
        name: String,
        description: String,
        input_schema: Value,
        #[serde(default)]
        batch_execution: ToolBatchExecution,
        process: ServiceJsonProcessConfig,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceVerifierConfig {
    name: String,
    description: String,
    process: ServiceJsonProcessConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceEvaluationConfig {
    #[serde(default = "default_evaluation_case_concurrency")]
    case_concurrency: usize,
    #[serde(default = "default_evaluation_grader_concurrency")]
    grader_concurrency: usize,
    #[serde(default = "default_evaluation_case_timeout_ms")]
    default_case_timeout_ms: u64,
    #[serde(default = "default_evaluation_grader_timeout_ms")]
    grader_timeout_ms: u64,
    #[serde(default)]
    graders: Vec<ServiceGraderConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceGraderConfig {
    name: String,
    description: String,
    process: ServiceJsonProcessConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceJsonProcessConfig {
    command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) command_sha256: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "default_current_directory")]
    current_directory: String,
    #[serde(default)]
    environment_from_host: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(super) secret_environment: BTreeMap<String, ServiceProcessSecretConfig>,
    #[serde(default = "default_tool_timeout_ms")]
    pub(super) timeout_ms: u64,
    #[serde(default = "default_tool_max_output_bytes")]
    max_output_bytes: usize,
    launch: ServiceProcessLaunchConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceProcessSecretConfig {
    reference: String,
    host_environment: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceMcpServerConfig {
    id: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command_sha256: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "default_current_directory")]
    current_directory: String,
    #[serde(default)]
    environment_from_host: BTreeMap<String, String>,
    #[serde(default = "default_mcp_request_timeout_ms")]
    request_timeout_ms: u64,
    launch: ServiceProcessLaunchConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tools: Option<ServiceMcpToolsConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceHttpsMcpServerConfig {
    id: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    endpoint: String,
    bearer_secret_reference: String,
    bearer_environment: String,
    #[serde(default = "default_mcp_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_mcp_max_response_bytes")]
    max_response_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    exclusive_root_ca_pem_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tools: Option<ServiceMcpToolsConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceMcpToolsConfig {
    namespace: String,
    allow: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceSkillsConfig {
    #[serde(default)]
    package_files: Vec<String>,
    #[serde(default)]
    external_package_files: Vec<String>,
    #[serde(default)]
    activate: Vec<SkillId>,
    #[serde(default = "default_skill_budget_tokens")]
    budget_tokens: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trust: Option<ServiceSkillTrustConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceSkillTrustConfig {
    #[serde(default)]
    publishers: Vec<ServiceSkillPublisherConfig>,
    #[serde(default)]
    transparency_logs: Vec<ServiceSkillTransparencyLogConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceSkillPublisherConfig {
    key_id: String,
    public_key_hex: String,
    #[serde(default)]
    not_before_ms: Option<u64>,
    #[serde(default)]
    not_after_ms: Option<u64>,
    #[serde(default)]
    transparency: SkillTransparencyRequirement,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revocation: Option<ServiceSkillRevocationConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceSkillTransparencyLogConfig {
    log_id: String,
    public_key_hex: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    revocation: Option<ServiceSkillRevocationConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceSkillRevocationConfig {
    revoked_at_ms: u64,
    reason_code: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceProcessLaunchConfig {
    Unrestricted {
        max_concurrency: usize,
    },
    MacosSeatbelt {
        max_concurrency: usize,
        writable_roots: Vec<String>,
        network_access: NetworkAccess,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceMemoryConfig {
    AgentMemoryHub {
        mcp_server: String,
        #[serde(default = "default_memory_top_k")]
        top_k: usize,
        #[serde(default = "default_memory_budget_tokens")]
        budget_tokens: usize,
        #[serde(default)]
        failure_mode: ServiceMemoryFailureMode,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceConversationConfig {
    #[serde(default = "default_conversation_max_turns")]
    max_turns: usize,
    #[serde(default = "default_conversation_budget_tokens")]
    budget_tokens: usize,
    #[serde(default = "default_conversation_budget_bytes")]
    budget_bytes: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compaction: Option<ServiceConversationCompactionConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceConversationCompactionConfig {
    name: String,
    description: String,
    #[serde(default = "default_compaction_max_input_turns")]
    max_input_turns: usize,
    #[serde(default = "default_compaction_input_budget_bytes")]
    input_budget_bytes: usize,
    #[serde(default = "default_compaction_output_budget_tokens")]
    output_budget_tokens: usize,
    #[serde(default = "default_compaction_output_budget_bytes")]
    output_budget_bytes: usize,
    process: ServiceJsonProcessConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceMemoryFailureMode {
    #[default]
    Degrade,
    FailTurn,
}

pub(super) struct LoadedConfig {
    config: ServiceConfig,
    path: PathBuf,
    root: PathBuf,
    data_directory: PathBuf,
}

#[derive(Default)]
struct ExistingStoreReadiness {
    state: bool,
    approvals: bool,
    tasks: bool,
    workflows: bool,
    human_handoffs: bool,
    effects: bool,
}

impl LoadedConfig {
    pub(super) fn effect_consumer(&self) -> Option<&ServiceEffectConsumerConfig> {
        self.config.effect_consumer.as_ref()
    }

    /// Resolves and revalidates the deployment authority at each trust boundary.
    fn authority(&self) -> Result<AuthorityContext, HarnessError> {
        configured_authority(&self.config)
    }
}

/// Builds authority only from trusted host configuration, never request data.
fn configured_authority(config: &ServiceConfig) -> Result<AuthorityContext, HarnessError> {
    match &config.authority {
        None => Ok(AuthorityContext::local_process()),
        Some(ServiceAuthorityConfig::LocalProcessTenant { tenant_id }) => {
            AuthorityContext::new(ActorIdentity::LocalProcess, Some(tenant_id.clone()))
        }
    }
}

/// Preserves local stdio permissions while replacing its unscoped authority.
struct FixedLocalProcessAuthorizer {
    authority: AuthorityContext,
}

impl ProtocolAuthorizer for FixedLocalProcessAuthorizer {
    fn allows(&self, principal: &ProtocolPrincipal, _permission: &str) -> bool {
        matches!(principal, ProtocolPrincipal::LocalProcess)
    }

    fn authority_context(
        &self,
        principal: &ProtocolPrincipal,
    ) -> Result<AuthorityContext, HarnessError> {
        if matches!(principal, ProtocolPrincipal::LocalProcess) {
            Ok(self.authority.clone())
        } else {
            Err(HarnessError::InvalidConfiguration(
                "configured local-process authority cannot resolve a remote principal".to_owned(),
            ))
        }
    }
}

struct ReferenceHumanHandoffSubjects {
    runtime: Arc<HarnessRuntime>,
    workflows: WorkflowEngine,
}

impl HumanHandoffSubjectResolver for ReferenceHumanHandoffSubjects {
    fn exists<'a>(
        &'a self,
        subject: &'a HumanHandoffSubject,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, bool> {
        Box::pin(async move {
            match subject {
                HumanHandoffSubject::Thread { thread_id } => Ok(self
                    .runtime
                    .load_thread_as(thread_id, authority)
                    .await?
                    .is_some()),
                HumanHandoffSubject::WorkflowRun { run_id } => {
                    Ok(self.workflows.load_as(run_id, authority).await?.is_some())
                }
            }
        })
    }
}

/// Runs isolated Evaluation State under the same authority as configured service use.
struct ConfiguredEvaluationTarget {
    runtime: Arc<HarnessRuntime>,
    authority: AuthorityContext,
}

impl EvaluationTarget for ConfiguredEvaluationTarget {
    fn execute<'a>(
        &'a self,
        case: EvaluationCase,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, TurnOutcome> {
        Box::pin(async move {
            let thread = self.runtime.create_thread_as(&self.authority).await?;
            self.runtime
                .run_turn_with_options(
                    &thread.id,
                    case.prompt,
                    TurnExecutionOptions {
                        authority: self.authority.clone(),
                        memory_scope: case.memory_scope,
                        context: Vec::new(),
                        execution_binding: None,
                        timeout: None,
                        cancellation,
                        model_event_sink: None,
                    },
                )
                .await
        })
    }
}

struct ConfiguredModels {
    registry: ModelRegistry,
    route: Vec<String>,
    attempt_timeout: Option<Duration>,
    retry_policy: Option<ModelRetryPolicy>,
    timeout_cooldown: Option<Duration>,
    demo_tools: bool,
}

struct ConfiguredCapabilities {
    tools: ToolRegistry,
    policy: AllowListPolicy,
    context: ContextEngine,
    verification: VerificationRegistry,
    mcp_clients: BTreeMap<String, ConfiguredMcpClient>,
    mcp_configured: usize,
    mcp_locked: usize,
    mcp_stdio_enabled: usize,
    memory_health: Option<MemoryHealthStatus>,
    skill_locks: Vec<String>,
}

struct ConfiguredEvaluation {
    graders: GraderRegistry,
    case_concurrency: usize,
    grader_concurrency: usize,
    default_case_timeout: Duration,
    grader_timeout: Duration,
}

impl ConfiguredEvaluation {
    fn engine(self) -> Result<EvaluationEngine, y_harness::HarnessError> {
        EvaluationEngine::new(self.graders)
            .with_concurrency(self.case_concurrency, self.grader_concurrency)?
            .with_timeouts(self.default_case_timeout, self.grader_timeout)
    }
}

struct ConfiguredRuntime {
    runtime: HarnessRuntime,
    mcp_clients: BTreeMap<String, ConfiguredMcpClient>,
}

#[derive(Serialize)]
struct ConfiguredEvaluationOutput {
    schema_version: u32,
    report: EvaluationReport,
    comparison: y_harness::BaselineComparison,
}

enum ConfiguredMcpClient {
    Stdio(Arc<StdioMcpClient>),
    #[cfg(feature = "https-mcp")]
    Https(Arc<HttpsJsonMcpClient>),
}

impl ConfiguredMcpClient {
    fn client(&self) -> Arc<dyn McpClient> {
        match self {
            Self::Stdio(client) => client.clone(),
            #[cfg(feature = "https-mcp")]
            Self::Https(client) => client.clone(),
        }
    }

    async fn shutdown(&self) -> Result<(), y_harness::HarnessError> {
        match self {
            Self::Stdio(client) => client.shutdown().await,
            #[cfg(feature = "https-mcp")]
            Self::Https(client) => client.shutdown().await,
        }
    }
}

struct InstalledProjectSkill {
    source: InstalledProjectSkillSource,
    path: PathBuf,
}

#[derive(Eq, PartialEq)]
enum InstalledProjectSkillSource {
    Trusted(SkillPackage),
    External(SignedSkillPackage),
}

impl InstalledProjectSkill {
    fn package(&self) -> &SkillPackage {
        self.source.package()
    }

    fn trust_label(&self) -> &'static str {
        self.source.trust_label()
    }
}

impl InstalledProjectSkillSource {
    fn package(&self) -> &SkillPackage {
        match self {
            Self::Trusted(package) => package,
            Self::External(signed) => &signed.package,
        }
    }

    fn trust_label(&self) -> &'static str {
        match self {
            InstalledProjectSkillSource::Trusted(_) => "trusted",
            InstalledProjectSkillSource::External(_) => "external",
        }
    }

    fn file_suffix(&self) -> &'static str {
        match self {
            Self::Trusted(_) => "skill.json",
            Self::External(_) => "signed-skill.json",
        }
    }

    fn activation_field(&self) -> &'static str {
        match self {
            Self::Trusted(_) => "skills.package_files",
            Self::External(_) => "skills.external_package_files",
        }
    }

    fn encode_pretty(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Self::Trusted(package) => serde_json::to_vec_pretty(package),
            Self::External(signed) => serde_json::to_vec_pretty(signed),
        }
    }
}

/// Creates a no-clobber local project configuration.
pub fn run_init(directory: String) -> CliResult<()> {
    let requested = PathBuf::from(directory);
    fs::create_dir_all(&requested)?;
    let root = fs::canonicalize(&requested)?;
    let config_path = root.join(CONFIG_FILE);
    if config_path.exists() {
        return Err(format!("refusing to replace {}", config_path.display()).into());
    }

    let config = ServiceConfig::default();
    let data_directory = resolve_data_directory(&root, &config.data_directory)?;
    fs::create_dir_all(&data_directory)?;
    require_contained_directory(&root, &data_directory)?;

    let mut encoded = serde_json::to_vec_pretty(&config)?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config_path)?;
    file.write_all(&encoded)?;
    file.sync_all()?;

    let gitignore = root.join(".gitignore");
    if !gitignore.exists() {
        let mut ignore = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&gitignore)?;
        ignore.write_all(b".y-harness/\n")?;
        ignore.sync_all()?;
    }

    println!("initialized: {}", config_path.display());
    println!("next: yh doctor {}", config_path.display());
    println!("then: yh serve {}", config_path.display());
    Ok(())
}

/// Installs one digest-verified declarative package without activating it.
pub fn run_skill_install(package_path: String, config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let source = canonical_regular_file(&package_path, "Skill package")?;
    let package = read_skill_package(&source)?;
    validate_local_skill(&package)?;
    install_project_skill(&loaded, InstalledProjectSkillSource::Trusted(package))
}

/// Installs one trust-verified signed declarative package without activating it.
pub fn run_skill_install_external(package_path: String, config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let source = canonical_regular_file(&package_path, "signed Skill package")?;
    let signed = read_signed_skill_package(&source)?;
    let trust = configured_skill_trust(&loaded)?;
    validate_external_skill(&signed, &trust)?;
    install_project_skill(&loaded, InstalledProjectSkillSource::External(signed))
}

/// Fetches and installs one exact trust-verified public HTTPS Skill.
#[cfg(feature = "https-skill")]
pub async fn run_skill_install_https(
    endpoint: String,
    identity: String,
    expected_sha256: String,
    config_path: String,
) -> CliResult<()> {
    let expected = parse_skill_identity(&identity)?;
    let loaded = load_config(&config_path)?;
    let trust = configured_skill_trust(&loaded)?;
    let source = HttpsSkillSource::new(HttpsSkillSourceConfig::new(endpoint)?)?;
    let signed = source.fetch(&expected, &expected_sha256).await?;
    validate_external_skill(&signed, &trust)?;
    install_project_skill(&loaded, InstalledProjectSkillSource::External(signed))
}

/// Reports that the optional network acquisition surface is not compiled.
#[cfg(not(feature = "https-skill"))]
pub async fn run_skill_install_https(
    _endpoint: String,
    _identity: String,
    _expected_sha256: String,
    _config_path: String,
) -> CliResult<()> {
    Err("HTTPS Skill installation requires the `https-skill` Cargo feature".into())
}

/// Lists installed packages after revalidating every digest and identity.
pub fn run_skill_list(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let installed = installed_project_skills(&loaded)?;
    verify_installed_external_skills(&loaded, &installed)?;
    println!("installed skills: {}", installed.len());
    for (id, skill) in installed {
        println!(
            "skill: {}@{} {} {} {}",
            id.name,
            id.version,
            skill.package().content_sha256,
            skill.trust_label(),
            skill.path.display()
        );
    }
    Ok(())
}

/// Verifies the complete bounded project Skill store.
pub fn run_skill_verify(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let installed = installed_project_skills(&loaded)?;
    verify_installed_external_skills(&loaded, &installed)?;
    for (id, skill) in &installed {
        println!(
            "verified: {}@{} {} {}",
            id.name,
            id.version,
            skill.package().content_sha256,
            skill.trust_label()
        );
    }
    println!("verified skills: {}", installed.len());
    println!("status: ok");
    Ok(())
}

/// Removes one exact unreferenced package by moving it into project-local trash.
pub fn run_skill_remove(identity: String, config_path: String) -> CliResult<()> {
    let requested = parse_skill_identity(&identity)?;
    let loaded = load_config(&config_path)?;
    let installed = installed_project_skills(&loaded)?;
    let selected = installed.get(&requested).ok_or_else(|| {
        format!(
            "Skill {}@{} is not installed",
            requested.name, requested.version
        )
    })?;
    if let Some(skills) = &loaded.config.skills {
        if skills.activate.contains(&requested) {
            return Err(format!(
                "refusing to remove active Skill {}@{}; remove it from skills.activate first",
                requested.name, requested.version
            )
            .into());
        }
        for configured in &skills.package_files {
            if resolve_project_file(&loaded.root, configured)? == selected.path {
                return Err(format!(
                    "refusing to remove configured Skill {identity}; remove {configured:?} from skills.package_files first"
                )
                .into());
            }
        }
        for configured in &skills.external_package_files {
            if resolve_project_file(&loaded.root, configured)? == selected.path {
                return Err(format!(
                    "refusing to remove configured external Skill {identity}; remove {configured:?} from skills.external_package_files first"
                )
                .into());
            }
        }
    }

    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;
    let trash = loaded.data_directory.join("skill-trash");
    fs::create_dir_all(&trash)?;
    require_contained_directory(&loaded.root, &trash)?;
    let removed_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let destination = trash.join(format!(
        "{}.removed-{removed_at}-{}.{}",
        selected.package().content_sha256,
        std::process::id(),
        selected.source.file_suffix()
    ));
    if destination.exists() {
        return Err(format!("Skill trash destination exists: {}", destination.display()).into());
    }
    fs::rename(&selected.path, &destination)?;
    println!("removed: {}", selected.path.display());
    println!("recoverable: {}", destination.display());
    Ok(())
}

/// Validates configuration, provider construction, credentials, and storage boundaries.
pub async fn run_doctor(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let authority = loaded.authority()?;
    let data_state = if loaded.data_directory.exists() {
        require_contained_directory(&loaded.root, &loaded.data_directory)?;
        if !loaded.data_directory.is_dir() {
            return Err(format!(
                "data directory is not a directory: {}",
                loaded.data_directory.display()
            )
            .into());
        }
        "ready"
    } else {
        "will be created"
    };
    let stores = validate_existing_stores(&loaded).await?;
    let effect_consumer = build_effect_consumer(&loaded).await?;
    let evaluation = build_evaluation(&loaded)?;
    let models = build_models(&loaded).await?;
    let capabilities = build_capabilities(&loaded, models.demo_tools).await?;

    println!("Y-Harness doctor");
    println!("engine: {}", env!("CARGO_PKG_VERSION"));
    println!("protocol: {PROTOCOL_VERSION}");
    println!("config schema: {}", loaded.config.schema_version);
    println!("config: {}", loaded.path.display());
    println!(
        "authority: local-process / {}",
        authority.tenant_id().unwrap_or("unscoped")
    );
    println!("model: {}", models.route[0]);
    println!("models: {}", models.registry.ids().len());
    println!("model route: {}", models.route.join(" -> "));
    println!(
        "model timeout cooldown: {}",
        models.timeout_cooldown.map_or_else(
            || "disabled".to_owned(),
            |value| format!("{} ms", value.as_millis())
        )
    );
    println!(
        "model retries: {}",
        models.retry_policy.map_or_else(
            || "disabled".to_owned(),
            |policy| format!(
                "{} ({}-{} ms)",
                policy.max_retries(),
                policy.initial_delay().as_millis(),
                policy.max_delay().as_millis()
            )
        )
    );
    println!("tools: {}", capabilities.tools.descriptors().len());
    let parallel_safe_tools = capabilities
        .tools
        .descriptors()
        .iter()
        .filter(|descriptor| {
            capabilities
                .tools
                .get(&descriptor.name)
                .is_some_and(|tool| tool.batch_execution == ToolBatchExecution::ParallelSafe)
        })
        .count();
    println!(
        "parallel tools: {parallel_safe_tools} safe / {} maximum",
        loaded.config.max_parallel_tool_calls
    );
    println!(
        "model attempt budget: {} per Agent Loop step",
        loaded.config.max_model_attempts_per_step
    );
    println!(
        "verifiers: {}",
        capabilities.verification.descriptors().len()
    );
    println!(
        "evaluation graders: {}",
        evaluation
            .as_ref()
            .map_or(0, |configured| configured.graders.descriptors().len())
    );
    println!(
        "mcp servers: {} enabled / {} configured",
        capabilities.mcp_clients.len(),
        capabilities.mcp_configured
    );
    println!(
        "mcp command locks: {} / {} stdio enabled",
        capabilities.mcp_locked, capabilities.mcp_stdio_enabled
    );
    println!("skills: {}", capabilities.skill_locks.len());
    for skill in &capabilities.skill_locks {
        println!("skill lock: {skill}");
    }
    if let Some(status) = &capabilities.memory_health {
        println!("memory: agent-memory-hub ({status:?})");
    } else {
        println!("memory: disabled");
    }
    if let Some(conversation) = &loaded.config.conversation {
        println!(
            "conversation: {} Turns / {} tokens / {} bytes",
            conversation.max_turns, conversation.budget_tokens, conversation.budget_bytes
        );
        println!(
            "conversation compactor: {}",
            conversation
                .compaction
                .as_ref()
                .map_or("disabled", |compaction| compaction.name.as_str())
        );
    } else {
        let defaults = ConversationContextConfig::default();
        println!(
            "conversation: {} Turns / {} tokens / {} bytes",
            defaults.max_turns, defaults.budget_tokens, defaults.budget_bytes
        );
        println!("conversation compactor: disabled");
    }
    if let Some(temporal) = &loaded.config.temporal {
        println!(
            "temporal: enabled ({} ms / {} identities per source)",
            temporal.poll_interval_ms, temporal.scan_limit
        );
    } else {
        println!("temporal: disabled");
    }
    if let Some(effect_consumer) = &effect_consumer {
        println!(
            "effect consumer: enabled ({})",
            effect_consumer.doctor_summary()
        );
    } else {
        println!("effect consumer: disabled");
    }
    println!("data: {} ({data_state})", loaded.data_directory.display());
    println!(
        "stores: state={} approval={} task={} workflow={} handoff={} effect={}",
        readiness_label(stores.state),
        readiness_label(stores.approvals),
        readiness_label(stores.tasks),
        readiness_label(stores.workflows),
        readiness_label(stores.human_handoffs),
        readiness_label(stores.effects)
    );
    println!(
        "schemas: state={STATE_EVENT_SCHEMA_VERSION}/{STATE_SNAPSHOT_SCHEMA_VERSION} approval={APPROVAL_INBOX_SCHEMA_VERSION} task={TASK_GRAPH_SCHEMA_VERSION} workflow={WORKFLOW_RUN_SCHEMA_VERSION} handoff={HUMAN_HANDOFF_SCHEMA_VERSION} effect={EFFECT_LEDGER_SCHEMA_VERSION} secret={SECRET_API_VERSION}"
    );
    shutdown_mcp_clients(&capabilities.mcp_clients).await?;
    println!("status: ok");
    Ok(())
}

async fn validate_existing_stores(loaded: &LoadedConfig) -> CliResult<ExistingStoreReadiness> {
    if !loaded.data_directory.exists() {
        return Ok(ExistingStoreReadiness::default());
    }
    let state = existing_database(&loaded.data_directory.join("state.db"), "State")?;
    let approvals = existing_database(
        &loaded.data_directory.join("approvals.db"),
        "Approval Inbox",
    )?;
    let tasks = existing_database(&loaded.data_directory.join("tasks.db"), "Task Coordinator")?;
    let workflows = existing_database(
        &loaded.data_directory.join("workflows.db"),
        "Workflow Coordinator",
    )?;
    let human_handoffs = existing_database(
        &loaded.data_directory.join("human-handoffs.db"),
        "Human Handoff Coordinator",
    )?;
    let effects = existing_database(
        &loaded.data_directory.join("effects.db"),
        "Effect Coordinator",
    )?;

    if state {
        SqliteEventStore::validate_existing(loaded.data_directory.join("state.db")).await?;
    }
    if approvals {
        SqliteApprovalInbox::validate_existing(loaded.data_directory.join("approvals.db")).await?;
    }
    if tasks {
        SqliteTaskCoordinator::validate_existing(loaded.data_directory.join("tasks.db")).await?;
    }
    if workflows {
        SqliteWorkflowCoordinator::validate_existing(loaded.data_directory.join("workflows.db"))
            .await?;
    }
    if human_handoffs {
        SqliteHumanHandoffCoordinator::validate_existing(
            loaded.data_directory.join("human-handoffs.db"),
        )
        .await?;
    }
    if effects {
        SqliteEffectCoordinator::validate_existing(loaded.data_directory.join("effects.db"))
            .await?;
    }

    Ok(ExistingStoreReadiness {
        state,
        approvals,
        tasks,
        workflows,
        human_handoffs,
        effects,
    })
}

fn existing_database(path: &Path, kind: &str) -> CliResult<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(format!(
            "{kind} database must be a regular file without symlinks: {}",
            path.display()
        )
        .into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

const fn readiness_label(present: bool) -> &'static str {
    if present { "ready" } else { "will be created" }
}

/// Exports one terminal durable Thread without loading service capabilities.
pub async fn run_thread_export(
    thread_id: String,
    archive_path: String,
    config_path: String,
) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(loaded.data_directory.join("state.db")).await?,
    ));
    let archive = state
        .export_thread_as(&ThreadId::from_string(thread_id), &loaded.authority()?)
        .await?;
    let encoded = encode_thread_archive(&archive)?;
    write_new_file(Path::new(&archive_path), &encoded, "Thread archive")?;
    println!(
        "exported Thread {}: {} events; archive: {}",
        archive.source_thread_id,
        archive.source_stream_version,
        Path::new(&archive_path).display()
    );
    Ok(())
}

/// Imports one bounded archive as an atomic local Thread stream.
pub async fn run_thread_import(
    archive_path: String,
    target_thread_id: String,
    config_path: String,
) -> CliResult<()> {
    let encoded = read_bounded(
        Path::new(&archive_path),
        u64::try_from(MAX_THREAD_ARCHIVE_BYTES)?,
        "Thread archive",
    )?;
    let archive = decode_thread_archive(&encoded)?;
    let loaded = load_config(&config_path)?;
    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(loaded.data_directory.join("state.db")).await?,
    ));
    let target_thread_id = ThreadId::from_string(target_thread_id);
    let imported = state
        .import_thread_as(&archive, target_thread_id, &loaded.authority()?)
        .await?;
    println!(
        "imported Thread {} from {}: {} turns",
        imported.id,
        archive.source_thread_id,
        imported.turns.len()
    );
    Ok(())
}

/// Runs the durable stdio service described by one validated project configuration.
pub async fn run_service(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let authority = loaded.authority()?;
    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;
    validate_existing_stores(&loaded).await?;

    let configured_effect_consumer = build_effect_consumer(&loaded).await?;
    let configured_models = build_models(&loaded).await?;
    let capabilities = build_capabilities(&loaded, configured_models.demo_tools).await?;
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(loaded.data_directory.join("state.db")).await?,
    ));
    let ConfiguredRuntime {
        runtime,
        mcp_clients,
    } = assemble_configured_runtime(&loaded, configured_models, capabilities, state)?;
    let approvals =
        Arc::new(SqliteApprovalInbox::open(loaded.data_directory.join("approvals.db")).await?);
    let tasks =
        Arc::new(SqliteTaskCoordinator::open(loaded.data_directory.join("tasks.db")).await?);
    let workflows = Arc::new(
        SqliteWorkflowCoordinator::open(loaded.data_directory.join("workflows.db")).await?,
    );
    let human_handoffs = Arc::new(
        SqliteHumanHandoffCoordinator::open(loaded.data_directory.join("human-handoffs.db"))
            .await?,
    );
    let effects =
        Arc::new(SqliteEffectCoordinator::open(loaded.data_directory.join("effects.db")).await?);

    let approval_handler = Arc::new(InboxApprovalHandler::new(
        approvals.clone(),
        Duration::from_millis(250),
    )?);
    let runtime = Arc::new(runtime.with_approval_handler(approval_handler));
    let approval_port: Arc<dyn ApprovalInbox> = approvals;
    let task_port: Arc<dyn TaskCoordinator> = tasks.clone();
    let workflow_port: Arc<dyn WorkflowCoordinator> = workflows;
    let workflow_engine = WorkflowEngine::new(workflow_port, tasks);
    let handoff_port: Arc<dyn HumanHandoffCoordinator> = human_handoffs;
    let handoff_engine = HumanHandoffEngine::new(
        handoff_port,
        Arc::new(ReferenceHumanHandoffSubjects {
            runtime: runtime.clone(),
            workflows: workflow_engine.clone(),
        }),
    );
    let effect_port: Arc<dyn EffectCoordinator> = effects;
    let effect_engine = EffectEngine::new(effect_port);
    let effect_service = configured_effect_consumer
        .map(|configured| {
            let configured = configured.assemble(effect_engine.clone())?;
            effect_service::start(configured, authority.clone())
        })
        .transpose()?;
    let temporal_service = loaded
        .config
        .temporal
        .as_ref()
        .cloned()
        .map(|config| {
            temporal_service::start(
                TemporalDriver::new()
                    .with_workflow_engine(workflow_engine.clone())
                    .with_human_handoff_engine(handoff_engine.clone())
                    .with_effect_engine(effect_engine.clone()),
                authority.clone(),
                config,
            )
        })
        .transpose()?;
    let handler = ProtocolHandler::new(runtime)
        .with_approval_inbox(approval_port)
        .with_task_coordinator(task_port)
        .with_workflow_engine(workflow_engine)
        .with_human_handoff_engine(handoff_engine)
        .with_effect_engine(effect_engine)
        .with_authorizer(Arc::new(FixedLocalProcessAuthorizer { authority }));
    let served = serve_jsonl(
        &handler,
        TokioBufReader::new(tokio_io::stdin()),
        TokioBufWriter::new(tokio_io::stdout()),
    )
    .await;
    let effect_shutdown = match effect_service {
        Some(effect_service) => effect_service.shutdown().await,
        None => Ok(()),
    };
    let temporal_shutdown = match temporal_service {
        Some(temporal_service) => temporal_service.shutdown().await,
        None => Ok(()),
    };
    let protocol_shutdown = shutdown_protocol_handler(&handler).await;
    let mcp_shutdown = shutdown_mcp_clients(&mcp_clients).await;
    served?;
    effect_shutdown?;
    temporal_shutdown?;
    protocol_shutdown?;
    mcp_shutdown
}

async fn shutdown_protocol_handler(handler: &ProtocolHandler) -> Result<(), HarnessError> {
    let report = handler.shutdown(OPERATION_SHUTDOWN_TIMEOUT).await?;
    if report.remaining_operations > 0 || !report.background_work_drained {
        return Err(HarnessError::Protocol(format!(
            "stdio shutdown incomplete: {} Operations remain; background drained={}",
            report.remaining_operations, report.background_work_drained
        )));
    }
    Ok(())
}

/// Runs one configured Evaluation suite and exact baseline without opening service State.
pub async fn run_evaluation(
    suite_path: String,
    baseline_path: String,
    config_path: String,
) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let configured_evaluation = build_evaluation(&loaded)?
        .ok_or("evaluation configuration with at least one Grader is required")?;
    if configured_evaluation.graders.descriptors().is_empty() {
        return Err("evaluation configuration must contain at least one Grader".into());
    }
    let suite_path = canonical_regular_file(&suite_path, "Evaluation suite")?;
    let baseline_path = canonical_regular_file(&baseline_path, "Evaluation baseline")?;
    let suite: EvaluationSuite = serde_json::from_slice(&read_bounded(
        &suite_path,
        MAX_EVALUATION_ARTIFACT_BYTES,
        "Evaluation suite",
    )?)
    .map_err(|error| format!("Evaluation suite is malformed: {error}"))?;
    let baseline: EvaluationBaseline = serde_json::from_slice(&read_bounded(
        &baseline_path,
        MAX_EVALUATION_ARTIFACT_BYTES,
        "Evaluation baseline",
    )?)
    .map_err(|error| format!("Evaluation baseline is malformed: {error}"))?;
    let configured_models = build_models(&loaded).await?;
    let capabilities = build_capabilities(&loaded, configured_models.demo_tools).await?;
    let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
    let ConfiguredRuntime {
        runtime,
        mcp_clients,
    } = assemble_configured_runtime(&loaded, configured_models, capabilities, state)?;
    let evaluation = configured_evaluation.engine()?;
    let target: Arc<dyn EvaluationTarget> = Arc::new(ConfiguredEvaluationTarget {
        runtime: Arc::new(runtime),
        authority: loaded.authority()?,
    });
    let evaluated = async {
        let report = evaluation.run(target, suite).await?;
        let comparison = baseline.compare(&report)?;
        Ok::<_, y_harness::HarnessError>((report, comparison))
    }
    .await;
    let shutdown = shutdown_mcp_clients(&mcp_clients).await;
    let (report, comparison) = evaluated?;
    shutdown?;
    let passed = comparison.passed;
    println!(
        "{}",
        serde_json::to_string_pretty(&ConfiguredEvaluationOutput {
            schema_version: EVALUATION_FORMAT_VERSION,
            report,
            comparison,
        })?
    );
    if passed {
        Ok(())
    } else {
        Err("configured Evaluation baseline regressed".into())
    }
}

fn assemble_configured_runtime(
    loaded: &LoadedConfig,
    configured_models: ConfiguredModels,
    capabilities: ConfiguredCapabilities,
    state: StateEngine,
) -> CliResult<ConfiguredRuntime> {
    let ConfiguredCapabilities {
        tools,
        policy,
        context,
        verification,
        mcp_clients,
        mcp_configured: _,
        mcp_locked: _,
        mcp_stdio_enabled: _,
        memory_health: _,
        skill_locks: _,
    } = capabilities;
    let route = configured_models
        .route
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut runtime = HarnessRuntime::from_model_registry_failover(
        &configured_models.registry,
        &route,
        tools,
        Arc::new(policy),
        state,
    )?;
    if let Some(timeout) = configured_models.attempt_timeout {
        runtime = runtime.with_model_attempt_timeout(timeout)?;
    }
    if let Some(policy) = configured_models.retry_policy {
        runtime = runtime.with_model_retry_policy(policy);
    }
    if let Some(cooldown) = configured_models.timeout_cooldown {
        runtime = runtime.with_model_timeout_cooldown(cooldown)?;
    }
    runtime = runtime.with_max_parallel_tool_calls(loaded.config.max_parallel_tool_calls)?;
    runtime =
        runtime.with_max_model_attempts_per_step(loaded.config.max_model_attempts_per_step)?;
    Ok(ConfiguredRuntime {
        runtime: runtime
            .with_context_engine(context)
            .with_verification(verification),
        mcp_clients,
    })
}

fn build_evaluation(loaded: &LoadedConfig) -> CliResult<Option<ConfiguredEvaluation>> {
    let Some(configured) = &loaded.config.evaluation else {
        return Ok(None);
    };
    let default_case_timeout = Duration::from_millis(configured.default_case_timeout_ms);
    let grader_timeout = Duration::from_millis(configured.grader_timeout_ms);
    EvaluationEngine::new(GraderRegistry::new())
        .with_concurrency(configured.case_concurrency, configured.grader_concurrency)?
        .with_timeouts(default_case_timeout, grader_timeout)?;

    let mut names = BTreeSet::new();
    for grader in &configured.graders {
        GraderDescriptor {
            name: grader.name.clone(),
            description: grader.description.clone(),
        }
        .validate()?;
        if !names.insert(&grader.name) {
            return Err(format!("duplicate evaluation Grader {}", grader.name).into());
        }
    }

    let mut graders = GraderRegistry::new();
    for grader in &configured.graders {
        let (process, broker) = build_json_process(
            loaded,
            &grader.process,
            &format!("JSON Evaluation Grader {}", grader.name),
        )?;
        graders.register(
            CapabilityOrigin::External {
                id: format!("json-command-grader/{}", grader.name),
            },
            Arc::new(JsonCommandGrader::new(
                GraderDescriptor {
                    name: grader.name.clone(),
                    description: grader.description.clone(),
                },
                process,
                broker,
            )?),
        )?;
    }

    Ok(Some(ConfiguredEvaluation {
        graders,
        case_concurrency: configured.case_concurrency,
        grader_concurrency: configured.grader_concurrency,
        default_case_timeout,
        grader_timeout,
    }))
}

async fn build_capabilities(
    loaded: &LoadedConfig,
    demo_tools: bool,
) -> CliResult<ConfiguredCapabilities> {
    let mut verifier_names = BTreeSet::new();
    for configured in &loaded.config.verifiers {
        let descriptor = VerifierDescriptor {
            name: configured.name.clone(),
            description: configured.description.clone(),
        };
        descriptor.validate()?;
        if !verifier_names.insert(&configured.name) {
            return Err(format!("duplicate verifier {}", configured.name).into());
        }
    }
    let mut clients = BTreeMap::new();
    let mut configured_ids = BTreeSet::new();
    for configured in &loaded.config.mcp_servers {
        if !configured_ids.insert(&configured.id) {
            return Err(format!("duplicate MCP server id {}", configured.id).into());
        }
    }
    for configured in &loaded.config.https_mcp_servers {
        if !configured_ids.insert(&configured.id) {
            return Err(format!("duplicate MCP server id {}", configured.id).into());
        }
    }
    let enabled_servers = loaded
        .config
        .mcp_servers
        .iter()
        .filter(|configured| configured.enabled)
        .collect::<Vec<_>>();
    let enabled_https_servers = loaded
        .config
        .https_mcp_servers
        .iter()
        .filter(|configured| configured.enabled)
        .collect::<Vec<_>>();
    if loaded.authority()?.tenant_id().is_some()
        && (!enabled_servers.is_empty() || !enabled_https_servers.is_empty())
    {
        return Err(
            "fixed-tenant service authority requires tenant-partitioned MCP sessions; configured shared MCP servers are unsupported"
                .into(),
        );
    }
    #[cfg(not(feature = "https-mcp"))]
    if !enabled_https_servers.is_empty() {
        return Err(
            "HTTPS MCP configuration requires a binary built with `--features https-mcp`".into(),
        );
    }
    let mut mcp_locked = 0;
    for configured in &enabled_servers {
        let command = PathBuf::from(&configured.command);
        if !command.is_absolute() || !command.is_file() {
            return Err(format!(
                "MCP server {} command must be an existing absolute file: {}",
                configured.id,
                command.display()
            )
            .into());
        }
        if let Some(expected) = &configured.command_sha256 {
            verify_file_sha256(&command, expected, "MCP command")?;
            mcp_locked += 1;
        }
        let current_dir = resolve_runtime_directory(
            &loaded.root,
            &configured.current_directory,
            "MCP working directory",
        )?;
        let environment = environment_from_host(&configured.environment_from_host)?;
        let authority = build_mcp_launch_authority(&loaded.root, &configured.launch)?;
        let client = Arc::new(StdioMcpClient::new(
            StdioMcpConfig {
                command,
                args: configured.args.clone(),
                env: environment,
                current_dir,
                request_timeout: Duration::from_millis(configured.request_timeout_ms),
            },
            authority,
        )?);
        clients.insert(configured.id.clone(), ConfiguredMcpClient::Stdio(client));
    }
    #[cfg(feature = "https-mcp")]
    for configured in &enabled_https_servers {
        clients.insert(
            configured.id.clone(),
            build_https_mcp_client(loaded, configured).await?,
        );
    }

    let mut tools = ToolRegistry::new();
    let mut policy = AllowListPolicy::deny_by_default();
    if demo_tools {
        tools.register(CapabilityOrigin::BuiltIn, Arc::new(EchoTool))?;
        policy = policy.allow("echo");
    }
    for configured in &loaded.config.tools {
        match configured {
            ServiceToolConfig::JsonCommand {
                name,
                description,
                input_schema,
                batch_execution,
                process,
            } => {
                let (process, broker) =
                    build_json_process(loaded, process, &format!("JSON Tool {name}"))?;
                let tool = JsonCommandTool::new(
                    ToolDescriptor {
                        name: name.clone(),
                        description: description.clone(),
                        input_schema: input_schema.clone(),
                    },
                    process,
                    broker,
                )?
                .with_batch_execution(*batch_execution);
                tools.register(
                    CapabilityOrigin::External {
                        id: format!("json-command/{name}"),
                    },
                    Arc::new(tool),
                )?;
                policy = policy.allow(name);
            }
        }
    }
    let mut verification = VerificationRegistry::new();
    for configured in &loaded.config.verifiers {
        let (process, broker) = build_json_process(
            loaded,
            &configured.process,
            &format!("JSON verifier {}", configured.name),
        )?;
        verification.register(
            CapabilityOrigin::External {
                id: format!("json-command-verifier/{}", configured.name),
            },
            Arc::new(JsonCommandVerifier::new(
                VerifierDescriptor {
                    name: configured.name.clone(),
                    description: configured.description.clone(),
                },
                process,
                broker,
            )?),
        )?;
    }
    let mcp_exposures = enabled_servers
        .iter()
        .map(|configured| (&configured.id, configured.tools.as_ref()))
        .chain(
            enabled_https_servers
                .iter()
                .map(|configured| (&configured.id, configured.tools.as_ref())),
        );
    for (id, exposure) in mcp_exposures {
        let Some(exposure) = exposure else {
            continue;
        };
        if exposure.allow.is_empty() {
            return Err(
                format!("MCP server {id} tools.allow must name at least one remote tool").into(),
            );
        }
        let client = clients
            .get(id)
            .ok_or_else(|| format!("MCP server {id} was not constructed"))?;
        let registered = register_selected_mcp_tools(
            &mut tools,
            CapabilityOrigin::External {
                id: format!("mcp/{id}"),
            },
            &exposure.namespace,
            client.client(),
            &exposure.allow,
        )
        .await?;
        for name in registered {
            policy = policy.allow(name);
        }
    }

    let (mut context, memory_health) = match &loaded.config.memory {
        None => (ContextEngine::without_memory(), None),
        Some(ServiceMemoryConfig::AgentMemoryHub {
            mcp_server,
            top_k,
            budget_tokens,
            failure_mode,
        }) => {
            let failure_mode = match failure_mode {
                ServiceMemoryFailureMode::Degrade => MemoryFailureMode::Degrade,
                ServiceMemoryFailureMode::FailTurn => MemoryFailureMode::FailTurn,
            };
            let config = MemoryContextConfig {
                provider: "agent-memory-hub".to_owned(),
                top_k: *top_k,
                budget_tokens: *budget_tokens,
                failure_mode,
            };
            config.validate()?;
            let client = clients
                .get(mcp_server)
                .ok_or_else(|| format!("memory references unknown MCP server {mcp_server}"))?;
            let provider = Arc::new(AgentMemoryHubProvider::new(client.client()));
            let health = provider.health().await?;
            if health.status == MemoryHealthStatus::Unavailable {
                return Err(format!(
                    "Agent Memory Hub is unavailable{}",
                    health
                        .message
                        .as_deref()
                        .map(|message| format!(": {message}"))
                        .unwrap_or_default()
                )
                .into());
            }
            let health = Some(health.status);
            let mut memories = MemoryRegistry::new();
            memories.register(
                CapabilityOrigin::External {
                    id: format!("mcp/{mcp_server}"),
                },
                provider,
            )?;
            (ContextEngine::with_memory(memories, config), health)
        }
    };
    if let Some(configured) = &loaded.config.conversation {
        context = context.with_conversation_config(ConversationContextConfig {
            max_turns: configured.max_turns,
            budget_tokens: configured.budget_tokens,
            budget_bytes: configured.budget_bytes,
        })?;
        if let Some(compaction) = &configured.compaction {
            let config = ConversationCompactionConfig {
                compactor: compaction.name.clone(),
                max_input_turns: compaction.max_input_turns,
                input_budget_bytes: compaction.input_budget_bytes,
                output_budget_tokens: compaction.output_budget_tokens,
                output_budget_bytes: compaction.output_budget_bytes,
            };
            config.validate()?;
            if config.input_budget_bytes > JSON_COMMAND_MAX_INPUT_BYTES {
                return Err(format!(
                    "JSON conversation compactor input_budget_bytes must be 1-{JSON_COMMAND_MAX_INPUT_BYTES}"
                )
                .into());
            }
            let descriptor = ConversationCompactorDescriptor {
                name: compaction.name.clone(),
                description: compaction.description.clone(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            };
            descriptor.validate()?;
            let (process, broker) = build_json_process(
                loaded,
                &compaction.process,
                &format!("JSON conversation compactor {}", compaction.name),
            )?;
            let compactor = Arc::new(JsonCommandConversationCompactor::new(
                descriptor, process, broker,
            )?);
            let mut registry = ConversationCompactorRegistry::new();
            registry.register(
                CapabilityOrigin::External {
                    id: format!("json-command-compactor/{}", compaction.name),
                },
                compactor,
            )?;
            context = context.with_conversation_compactor(registry, config)?;
        }
    }
    let skill_locks = if let Some(resolved) = load_project_skills(loaded, &tools)? {
        let locks = resolved
            .skills
            .iter()
            .map(|skill| {
                let mut lock = format!(
                    "{}@{} {}",
                    skill.id.name, skill.id.version, skill.content_sha256
                );
                if let Some(publisher) = &skill.publisher_key_id {
                    lock.push_str(&format!(" publisher={publisher}"));
                }
                if let Some(transparency) = &skill.transparency {
                    lock.push_str(&format!(
                        " transparency={}@{}",
                        transparency.log_id, transparency.entry_id
                    ));
                }
                lock
            })
            .collect();
        context = context.with_skills(resolved);
        locks
    } else {
        Vec::new()
    };

    Ok(ConfiguredCapabilities {
        tools,
        policy,
        context,
        verification,
        mcp_clients: clients,
        mcp_configured: loaded
            .config
            .mcp_servers
            .len()
            .saturating_add(loaded.config.https_mcp_servers.len()),
        mcp_locked,
        mcp_stdio_enabled: enabled_servers.len(),
        memory_health,
        skill_locks,
    })
}

fn load_project_skills(
    loaded: &LoadedConfig,
    tools: &ToolRegistry,
) -> CliResult<Option<y_harness::ResolvedSkillSet>> {
    let Some(config) = &loaded.config.skills else {
        return Ok(None);
    };
    let trust = configured_skill_trust(loaded)?;
    let packages_empty =
        config.package_files.is_empty() && config.external_package_files.is_empty();
    if packages_empty && config.activate.is_empty() {
        return Ok(None);
    }
    if packages_empty || config.activate.is_empty() {
        return Err(
            "skills requires at least one package file and a non-empty activate list".into(),
        );
    }

    let mut registry = SkillRegistry::new();
    for configured in &config.package_files {
        let path = resolve_project_file(&loaded.root, configured)?;
        let encoded = read_bounded(&path, MAX_SKILL_PACKAGE_FILE_BYTES, "Skill package")?;
        let package: SkillPackage = serde_json::from_slice(&encoded)
            .map_err(|_| format!("Skill package is malformed: {}", path.display()))?;
        let origin = CapabilityOrigin::TrustedExtension {
            id: format!(
                "project-skill/{}@{}",
                package.manifest.name, package.manifest.version
            ),
        };
        registry.register(origin, package)?;
    }
    for configured in &config.external_package_files {
        let path = resolve_project_file(&loaded.root, configured)?;
        let signed = read_signed_skill_package(&path)?;
        let package = &signed.package;
        let origin = CapabilityOrigin::External {
            id: format!(
                "project-external-skill/{}@{}",
                package.manifest.name, package.manifest.version
            ),
        };
        registry.register_signed(origin, signed, &trust)?;
    }

    Ok(Some(SkillEngine::new(registry).resolve(
        &config.activate,
        tools,
        config.budget_tokens,
    )?))
}

fn installed_project_skills(
    loaded: &LoadedConfig,
) -> CliResult<BTreeMap<SkillId, InstalledProjectSkill>> {
    let directory = project_skill_directory(loaded, false)?;
    if !directory.exists() {
        return Ok(BTreeMap::new());
    }
    let mut registry = SkillRegistry::new();
    let mut installed = BTreeMap::new();
    let mut entries = fs::read_dir(&directory)?;
    for index in 0..=MAX_PROJECT_SKILL_FILES {
        let Some(entry) = entries.next().transpose()? else {
            return Ok(installed);
        };
        if index == MAX_PROJECT_SKILL_FILES {
            return Err(format!(
                "project Skill directory exceeds {MAX_PROJECT_SKILL_FILES} entries"
            )
            .into());
        }
        let file_name = entry
            .file_name()
            .into_string()
            .map_err(|_| "project Skill directory contains a non-UTF-8 entry")?;
        let signed = file_name.ends_with(".signed-skill.json");
        if !signed && !file_name.ends_with(".skill.json") {
            continue;
        }
        let file_type = entry.file_type()?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(format!(
                "project Skill entry must be a regular non-symlink file: {}",
                entry.path().display()
            )
            .into());
        }
        let path = fs::canonicalize(entry.path())?;
        if !path.starts_with(&directory) {
            return Err(format!(
                "project Skill entry escapes {}: {}",
                directory.display(),
                path.display()
            )
            .into());
        }
        let source = if signed {
            InstalledProjectSkillSource::External(read_signed_skill_package(&path)?)
        } else {
            InstalledProjectSkillSource::Trusted(read_skill_package(&path)?)
        };
        let package = source.package();
        let id = SkillId {
            name: package.manifest.name.clone(),
            version: package.manifest.version.clone(),
        };
        registry.register(
            CapabilityOrigin::TrustedExtension {
                id: format!("project-skill/{}@{}", id.name, id.version),
            },
            package.clone(),
        )?;
        if installed
            .insert(id, InstalledProjectSkill { source, path })
            .is_some()
        {
            return Err("project Skill store contains a duplicate identity".into());
        }
    }
    Err(format!("project Skill directory exceeds {MAX_PROJECT_SKILL_FILES} entries").into())
}

fn project_skill_directory(loaded: &LoadedConfig, create: bool) -> CliResult<PathBuf> {
    let requested = loaded.root.join("skills");
    if create {
        fs::create_dir_all(&requested)?;
    } else if !requested.exists() {
        return Ok(requested);
    }
    let directory = fs::canonicalize(&requested)?;
    if directory == loaded.root || !directory.starts_with(&loaded.root) {
        return Err(format!(
            "project Skill directory must remain below {}",
            loaded.root.display()
        )
        .into());
    }
    if !directory.is_dir() {
        return Err(format!(
            "project Skill directory is not a directory: {}",
            directory.display()
        )
        .into());
    }
    Ok(directory)
}

fn canonical_regular_file(path: &str, kind: &str) -> CliResult<PathBuf> {
    let requested = PathBuf::from(path);
    let requested = if requested.is_absolute() {
        requested
    } else {
        env::current_dir()?.join(requested)
    };
    let canonical = fs::canonicalize(&requested)?;
    if !canonical.is_file() {
        return Err(format!("{kind} must resolve to a regular file").into());
    }
    Ok(canonical)
}

fn read_skill_package(path: &Path) -> CliResult<SkillPackage> {
    let encoded = read_bounded(path, MAX_SKILL_PACKAGE_FILE_BYTES, "Skill package")?;
    let package: SkillPackage = serde_json::from_slice(&encoded)
        .map_err(|_| format!("Skill package is malformed: {}", path.display()))?;
    validate_local_skill(&package)?;
    Ok(package)
}

fn read_signed_skill_package(path: &Path) -> CliResult<SignedSkillPackage> {
    let encoded = read_bounded(path, MAX_SKILL_PACKAGE_FILE_BYTES, "signed Skill package")?;
    let signed: SignedSkillPackage = serde_json::from_slice(&encoded)
        .map_err(|_| format!("signed Skill package is malformed: {}", path.display()))?;
    validate_local_skill(&signed.package)?;
    Ok(signed)
}

fn validate_local_skill(package: &SkillPackage) -> CliResult<()> {
    let mut registry = SkillRegistry::new();
    registry.register(
        CapabilityOrigin::TrustedExtension {
            id: "skill-cli-validation".to_owned(),
        },
        package.clone(),
    )?;
    Ok(())
}

fn validate_external_skill(signed: &SignedSkillPackage, trust: &SkillTrustStore) -> CliResult<()> {
    let package = &signed.package;
    let mut registry = SkillRegistry::new();
    registry.register_signed(
        CapabilityOrigin::External {
            id: format!(
                "project-external-skill/{}@{}",
                package.manifest.name, package.manifest.version
            ),
        },
        signed.clone(),
        trust,
    )?;
    Ok(())
}

fn verify_installed_external_skills(
    loaded: &LoadedConfig,
    installed: &BTreeMap<SkillId, InstalledProjectSkill>,
) -> CliResult<()> {
    let trust = configured_skill_trust(loaded)?;
    let mut registry = SkillRegistry::new();
    for skill in installed.values() {
        if let InstalledProjectSkillSource::External(signed) = &skill.source {
            let package = &signed.package;
            registry.register_signed(
                CapabilityOrigin::External {
                    id: format!(
                        "project-external-skill/{}@{}",
                        package.manifest.name, package.manifest.version
                    ),
                },
                signed.clone(),
                &trust,
            )?;
        }
    }
    Ok(())
}

fn configured_skill_trust(loaded: &LoadedConfig) -> CliResult<SkillTrustStore> {
    let trust = SkillTrustStore::new();
    let Some(config) = loaded
        .config
        .skills
        .as_ref()
        .and_then(|skills| skills.trust.as_ref())
    else {
        return Ok(trust);
    };
    for publisher in &config.publishers {
        trust.trust_with_policy(
            publisher.key_id.clone(),
            decode_ed25519_public_key(&publisher.public_key_hex, "publisher key")?,
            SkillPublisherPolicy {
                not_before_ms: publisher.not_before_ms,
                not_after_ms: publisher.not_after_ms,
                transparency: publisher.transparency,
            },
        )?;
        if let Some(revocation) = &publisher.revocation {
            trust.revoke_publisher(
                &publisher.key_id,
                revocation.revoked_at_ms,
                revocation.reason_code.clone(),
            )?;
        }
    }
    for log in &config.transparency_logs {
        trust.trust_transparency_log(
            log.log_id.clone(),
            decode_ed25519_public_key(&log.public_key_hex, "transparency-log key")?,
        )?;
        if let Some(revocation) = &log.revocation {
            trust.revoke_transparency_log(
                &log.log_id,
                revocation.revoked_at_ms,
                revocation.reason_code.clone(),
            )?;
        }
    }
    Ok(trust)
}

fn decode_ed25519_public_key(value: &str, kind: &str) -> CliResult<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(
            format!("{kind} must be exactly 32 bytes encoded as lowercase hexadecimal").into(),
        );
    }
    let encoded = value.as_bytes();
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        let high = decode_lower_hex_nibble(encoded[index * 2]);
        let low = decode_lower_hex_nibble(encoded[index * 2 + 1]);
        *byte = (high << 4) | low;
    }
    Ok(key)
}

const fn decode_lower_hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

fn install_project_skill(
    loaded: &LoadedConfig,
    source: InstalledProjectSkillSource,
) -> CliResult<()> {
    let package = source.package();
    let id = SkillId {
        name: package.manifest.name.clone(),
        version: package.manifest.version.clone(),
    };
    let installed = installed_project_skills(loaded)?;
    if let Some(existing) = installed.get(&id) {
        if existing.source != source {
            return Err(format!(
                "Skill {}@{} is already installed with a different package or trust envelope",
                id.name, id.version
            )
            .into());
        }
        println!("already installed: {}", existing.path.display());
        println!(
            "skill lock: {}@{} {} {}",
            id.name,
            id.version,
            package.content_sha256,
            source.trust_label()
        );
        return Ok(());
    }

    let directory = project_skill_directory(loaded, true)?;
    let destination = directory.join(format!(
        "{}.{}",
        package.content_sha256,
        source.file_suffix()
    ));
    if destination.exists() {
        return Err(format!(
            "Skill destination already exists without a matching installed identity: {}",
            destination.display()
        )
        .into());
    }
    let mut encoded = source.encode_pretty()?;
    encoded.push(b'\n');
    write_new_file(&destination, &encoded, "Skill package")?;

    println!("installed: {}", destination.display());
    println!(
        "skill lock: {}@{} {} {}",
        id.name,
        id.version,
        package.content_sha256,
        source.trust_label()
    );
    let relative = destination
        .strip_prefix(&loaded.root)
        .map_err(|_| "installed Skill escaped the project root")?;
    println!(
        "activation required: add {:?} to {} and {}@{} to skills.activate",
        relative.to_string_lossy(),
        source.activation_field(),
        id.name,
        id.version
    );
    Ok(())
}

fn parse_skill_identity(value: &str) -> CliResult<SkillId> {
    let (name, version) = value
        .rsplit_once('@')
        .ok_or("Skill identity must be name@version")?;
    let valid_name = !name.is_empty()
        && name.len() <= 64
        && name.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '_' | '-' | '.')
        });
    if !valid_name {
        return Err("Skill name must use 1-64 lowercase portable identity characters".into());
    }
    Ok(SkillId {
        name: name.to_owned(),
        version: Version::parse(version)?,
    })
}

fn build_mcp_launch_authority(
    project_root: &Path,
    configured: &ServiceProcessLaunchConfig,
) -> CliResult<StdioMcpLaunchAuthority> {
    match configured {
        ServiceProcessLaunchConfig::Unrestricted { max_concurrency } => {
            Ok(StdioMcpLaunchAuthority::unrestricted(*max_concurrency)?)
        }
        ServiceProcessLaunchConfig::MacosSeatbelt {
            max_concurrency,
            writable_roots,
            network_access,
        } => {
            let writable_roots = writable_roots
                .iter()
                .map(|root| resolve_runtime_directory(project_root, root, "Seatbelt writable root"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(StdioMcpLaunchAuthority::macos_seatbelt(
                *max_concurrency,
                writable_roots,
                *network_access,
            )?)
        }
    }
}

fn build_process_broker(
    project_root: &Path,
    configured: &ServiceProcessLaunchConfig,
) -> CliResult<Arc<dyn ProcessBroker>> {
    match configured {
        ServiceProcessLaunchConfig::Unrestricted { max_concurrency } => {
            Ok(Arc::new(LocalProcessBroker::new(*max_concurrency)?))
        }
        ServiceProcessLaunchConfig::MacosSeatbelt {
            max_concurrency,
            writable_roots,
            network_access,
        } => {
            let writable_roots = writable_roots
                .iter()
                .map(|root| resolve_runtime_directory(project_root, root, "Seatbelt writable root"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(MacOsSeatbeltBroker::new(
                *max_concurrency,
                writable_roots,
                *network_access,
            )?))
        }
    }
}

pub(super) fn build_json_process(
    loaded: &LoadedConfig,
    configured: &ServiceJsonProcessConfig,
    role: &str,
) -> CliResult<(JsonProcessConfig, Arc<dyn ProcessBroker>)> {
    if !configured.secret_environment.is_empty() {
        return Err(format!(
            "{role} does not support secret_environment; only governed Effect Connectors may resolve process credentials"
        )
        .into());
    }
    build_json_process_base(loaded, configured, role)
}

pub(super) async fn build_json_effect_process(
    loaded: &LoadedConfig,
    configured: &ServiceJsonProcessConfig,
    role: &str,
    consumer: &str,
) -> CliResult<(
    JsonProcessConfig,
    Arc<dyn ProcessBroker>,
    Option<EffectSecretEnvironment>,
)> {
    let (process, broker) = build_json_process_base(loaded, configured, role)?;
    let secret_environment = configured_effect_secret_environment(loaded, configured)?;
    if let Some(environment) = &secret_environment {
        environment
            .probe(consumer, &loaded.authority()?)
            .await
            .map_err(|_| format!("{role} credential availability probe failed"))?;
    }
    Ok((process, broker, secret_environment))
}

fn build_json_process_base(
    loaded: &LoadedConfig,
    configured: &ServiceJsonProcessConfig,
    role: &str,
) -> CliResult<(JsonProcessConfig, Arc<dyn ProcessBroker>)> {
    let command = PathBuf::from(&configured.command);
    if !command.is_absolute() || !command.is_file() {
        return Err(format!(
            "{role} command must be an existing absolute file: {}",
            command.display()
        )
        .into());
    }
    let current_dir = resolve_runtime_directory(
        &loaded.root,
        &configured.current_directory,
        &format!("{role} working directory"),
    )?;
    let mut process = JsonProcessConfig {
        program: command,
        args: configured.args.clone(),
        current_dir,
        environment: BTreeMap::new(),
        timeout: Duration::from_millis(configured.timeout_ms),
        max_output_bytes: configured.max_output_bytes,
    };
    process.validate()?;
    let broker = build_process_broker(&loaded.root, &configured.launch)?;
    let broker: Arc<dyn ProcessBroker> = match &configured.command_sha256 {
        Some(expected_sha256) => Arc::new(DigestLockedProcessBroker::new(
            broker,
            process.program.clone(),
            expected_sha256.clone(),
        )?),
        None => broker,
    };
    process.environment = environment_from_host(&configured.environment_from_host)?;
    process.validate()?;
    Ok((process, broker))
}

fn configured_effect_secret_environment(
    loaded: &LoadedConfig,
    configured: &ServiceJsonProcessConfig,
) -> Result<Option<EffectSecretEnvironment>, HarnessError> {
    if configured.secret_environment.is_empty() {
        return Ok(None);
    }
    if configured
        .secret_environment
        .keys()
        .any(|name| configured.environment_from_host.contains_key(name))
    {
        return Err(HarnessError::InvalidConfiguration(
            "Effect plain and secret environment names must not overlap".to_owned(),
        ));
    }
    let mut provider_mappings = BTreeMap::new();
    let mut child_mappings = BTreeMap::new();
    for (child_name, configured_secret) in &configured.secret_environment {
        let reference = SecretReference::new(configured_secret.reference.clone())?;
        if let Some(existing) = provider_mappings.insert(
            reference.clone(),
            configured_secret.host_environment.clone(),
        ) && existing != configured_secret.host_environment
        {
            return Err(HarnessError::InvalidConfiguration(
                "one Effect secret reference cannot select multiple host environment variables"
                    .to_owned(),
            ));
        }
        child_mappings.insert(child_name.clone(), reference);
    }
    let provider = configured_environment_secret_provider(loaded, provider_mappings)?;
    EffectSecretEnvironment::new(provider, child_mappings).map(Some)
}

fn environment_from_host(
    configured: &BTreeMap<String, String>,
) -> CliResult<BTreeMap<String, String>> {
    configured
        .iter()
        .map(|(child_name, host_name)| {
            let value = env::var(host_name).map_err(|_| {
                format!(
                    "required host environment variable {host_name} for child variable {child_name} is unavailable"
                )
            })?;
            Ok((child_name.clone(), value))
        })
        .collect()
}

fn resolve_runtime_directory(root: &Path, configured: &str, kind: &str) -> CliResult<PathBuf> {
    let configured = Path::new(configured);
    let requested = if configured.is_absolute() {
        configured.to_owned()
    } else {
        if configured
            .components()
            .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(format!("{kind} cannot contain parent traversal").into());
        }
        root.join(configured)
    };
    let canonical = fs::canonicalize(&requested)?;
    if !canonical.is_dir() {
        return Err(format!("{kind} is not a directory: {}", canonical.display()).into());
    }
    Ok(canonical)
}

async fn shutdown_mcp_clients(clients: &BTreeMap<String, ConfiguredMcpClient>) -> CliResult<()> {
    let mut first_error = None;
    for (id, client) in clients {
        if let Err(error) = client.shutdown().await
            && first_error.is_none()
        {
            first_error = Some(format!("MCP server {id} shutdown failed: {error}"));
        }
    }
    match first_error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

#[cfg(feature = "https-mcp")]
async fn build_https_mcp_client(
    loaded: &LoadedConfig,
    configured: &ServiceHttpsMcpServerConfig,
) -> CliResult<ConfiguredMcpClient> {
    let reference = SecretReference::new(configured.bearer_secret_reference.clone())?;
    let secrets = Arc::new(EnvironmentSecretProvider::new(
        "service-environment",
        BTreeMap::from([(reference.clone(), configured.bearer_environment.clone())]),
    )?);
    let mut config = HttpsJsonMcpConfig::new(&configured.endpoint, reference.clone())?
        .with_limits(
            Duration::from_millis(configured.request_timeout_ms),
            Duration::from_millis(configured.connect_timeout_ms),
            configured.max_response_bytes,
        )?;
    if let Some(path) = &configured.exclusive_root_ca_pem_path {
        let path = resolve_project_file(&loaded.root, path)?;
        config = config.with_exclusive_root_certificates_pem(read_bounded(
            &path,
            MAX_CA_BYTES,
            "exclusive MCP root CA",
        )?)?;
    }
    let _credential = secrets
        .resolve(SecretRequest {
            reference,
            consumer: configured.id.clone(),
            use_context: SecretUseContext::Service {
                use_case: SecretServiceUse::StartupProbe,
            },
        })
        .await?;
    Ok(ConfiguredMcpClient::Https(Arc::new(
        HttpsJsonMcpClient::new(config, secrets)?,
    )))
}

async fn build_models(loaded: &LoadedConfig) -> CliResult<ConfiguredModels> {
    let (configured, route, attempt_timeout, retry_config, timeout_cooldown) = match (
        loaded.config.model.as_ref(),
        loaded.config.models.as_slice(),
        loaded.config.model_route.as_ref(),
    ) {
        (Some(model), [], None) => (vec![model], vec![model.id().to_owned()], None, None, None),
        (None, models, Some(route)) if !models.is_empty() => {
            if !(1..=16).contains(&route.models.len()) {
                return Err("model_route.models must contain 1-16 identities".into());
            }
            (
                models.iter().collect(),
                route.models.clone(),
                Some(Duration::from_millis(route.attempt_timeout_ms)),
                route.retry.as_ref(),
                (route.timeout_cooldown_ms > 0)
                    .then(|| Duration::from_millis(route.timeout_cooldown_ms)),
            )
        }
        _ => {
            return Err(
                "configure either legacy model or the models plus model_route catalog, never both"
                    .into(),
            );
        }
    };

    if let Some(timeout) = attempt_timeout
        && !(1..=86_400_000).contains(&timeout.as_millis())
    {
        return Err("model_route.attempt_timeout_ms must be 1-86400000".into());
    }
    if timeout_cooldown.is_some_and(|cooldown| cooldown.as_millis() > 86_400_000) {
        return Err("model_route.timeout_cooldown_ms must be 0-86400000".into());
    }
    if timeout_cooldown.is_some() && route.len() < 2 {
        return Err("model_route.timeout_cooldown_ms requires at least two route models".into());
    }
    let retry_policy = retry_config
        .map(|retry| {
            ModelRetryPolicy::new(
                retry.max_retries,
                Duration::from_millis(retry.initial_delay_ms),
                Duration::from_millis(retry.max_delay_ms),
            )
        })
        .transpose()?;
    let mut configured_ids = BTreeSet::new();
    for model in &configured {
        if !configured_ids.insert(model.id()) {
            return Err(format!("duplicate configured model identity {}", model.id()).into());
        }
    }
    let mut route_ids = BTreeSet::new();
    for id in &route {
        if !route_ids.insert(id) {
            return Err(format!("model_route contains duplicate identity {id}").into());
        }
        if !configured_ids.contains(id.as_str()) {
            return Err(format!("model_route references unknown model {id}").into());
        }
    }

    let mut registry = ModelRegistry::new();
    let mut demo_ids = BTreeSet::new();
    for configured in configured {
        let (origin, model, demo) = build_model(loaded, configured).await?;
        let id = configured.id().to_owned();
        registry.register(origin, model)?;
        if demo {
            demo_ids.insert(id);
        }
    }

    Ok(ConfiguredModels {
        registry,
        demo_tools: route.iter().any(|id| demo_ids.contains(id)),
        route,
        attempt_timeout,
        retry_policy,
        timeout_cooldown,
    })
}

async fn build_model(
    loaded: &LoadedConfig,
    configured: &ServiceModelConfig,
) -> CliResult<(CapabilityOrigin, Arc<dyn LanguageModel>, bool)> {
    match configured {
        ServiceModelConfig::Demo { id } => {
            if id != "local/demo" {
                return Err("demo model id must be local/demo".into());
            }
            Ok((CapabilityOrigin::BuiltIn, Arc::new(DemoModel), true))
        }
        ServiceModelConfig::JsonCommand {
            id,
            protocol,
            process,
        } => {
            let (process, broker) =
                build_json_process(loaded, process, &format!("JSON Model {id}"))?;
            let model =
                Arc::new(JsonCommandModel::new(id, process, broker)?.with_protocol(*protocol));
            Ok((
                CapabilityOrigin::External {
                    id: format!("json-command-model/{id}"),
                },
                model,
                false,
            ))
        }
        ServiceModelConfig::OpenAiResponses {
            id,
            model,
            api_key_secret_reference,
            api_key_environment,
            request_timeout_ms,
            connect_timeout_ms,
            max_response_bytes,
            max_concurrency,
        } => {
            let model = build_openai_model(
                loaded,
                id,
                model,
                api_key_secret_reference,
                api_key_environment,
                *request_timeout_ms,
                *connect_timeout_ms,
                *max_response_bytes,
                *max_concurrency,
            )
            .await?;
            Ok((
                CapabilityOrigin::TrustedExtension {
                    id: "first-party-openai-responses".to_owned(),
                },
                model,
                false,
            ))
        }
        ServiceModelConfig::HttpsJsonGateway {
            id,
            endpoint,
            bearer_secret_reference,
            bearer_environment,
            request_timeout_ms,
            connect_timeout_ms,
            max_response_bytes,
            max_concurrency,
            exclusive_root_ca_pem_path,
        } => {
            let model = build_https_model(
                loaded,
                id,
                endpoint,
                bearer_secret_reference,
                bearer_environment,
                *request_timeout_ms,
                *connect_timeout_ms,
                *max_response_bytes,
                *max_concurrency,
                exclusive_root_ca_pem_path.as_deref(),
            )
            .await?;
            Ok((
                CapabilityOrigin::TrustedExtension {
                    id: "reference-https-json-gateway".to_owned(),
                },
                model,
                false,
            ))
        }
    }
}

#[cfg(feature = "https-model")]
/// Selects the unscoped or exact-tenant environment adapter for one Model Secret.
fn configured_environment_secrets(
    loaded: &LoadedConfig,
    reference: &SecretReference,
    environment: String,
) -> Result<Arc<dyn SecretProvider>, HarnessError> {
    configured_environment_secret_provider(
        loaded,
        BTreeMap::from([(reference.clone(), environment)]),
    )
}

/// Selects the unscoped or exact-tenant environment adapter for explicit mappings.
fn configured_environment_secret_provider(
    loaded: &LoadedConfig,
    mappings: BTreeMap<SecretReference, String>,
) -> Result<Arc<dyn SecretProvider>, HarnessError> {
    let authority = loaded.authority()?;
    match authority.tenant_id() {
        None => Ok(Arc::new(EnvironmentSecretProvider::new(
            "service-environment",
            mappings,
        )?)),
        Some(tenant_id) => Ok(Arc::new(TenantEnvironmentSecretProvider::new(
            "service-tenant-environment",
            BTreeMap::from([(tenant_id.to_owned(), mappings)]),
        )?)),
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "https-model")]
async fn build_openai_model(
    loaded: &LoadedConfig,
    id: &str,
    model: &str,
    api_key_secret_reference: &str,
    api_key_environment: &str,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
    max_response_bytes: usize,
    max_concurrency: usize,
) -> CliResult<Arc<dyn LanguageModel>> {
    let reference = SecretReference::new(api_key_secret_reference.to_owned())?;
    let secrets =
        configured_environment_secrets(loaded, &reference, api_key_environment.to_owned())?;
    let _credential = secrets
        .resolve_as(
            SecretRequest {
                reference: reference.clone(),
                consumer: id.to_owned(),
                use_context: SecretUseContext::Service {
                    use_case: SecretServiceUse::StartupProbe,
                },
            },
            &loaded.authority()?,
        )
        .await?;
    let config = OpenAiResponsesModelConfig::new(model, reference)?.with_limits(
        Duration::from_millis(request_timeout_ms),
        Duration::from_millis(connect_timeout_ms),
        max_response_bytes,
        max_concurrency,
    )?;
    let model = OpenAiResponsesModel::new(id, config, secrets)?;
    Ok(Arc::new(model))
}

#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "https-model"))]
async fn build_openai_model(
    _loaded: &LoadedConfig,
    _id: &str,
    _model: &str,
    _api_key_secret_reference: &str,
    _api_key_environment: &str,
    _request_timeout_ms: u64,
    _connect_timeout_ms: u64,
    _max_response_bytes: usize,
    _max_concurrency: usize,
) -> CliResult<Arc<dyn LanguageModel>> {
    Err(
        "OpenAI Responses configuration requires a binary built with `--features https-model`"
            .into(),
    )
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "https-model")]
async fn build_https_model(
    loaded: &LoadedConfig,
    id: &str,
    endpoint: &str,
    bearer_secret_reference: &str,
    bearer_environment: &str,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
    max_response_bytes: usize,
    max_concurrency: usize,
    root_ca_path: Option<&str>,
) -> CliResult<Arc<dyn LanguageModel>> {
    let reference = SecretReference::new(bearer_secret_reference.to_owned())?;
    let secrets =
        configured_environment_secrets(loaded, &reference, bearer_environment.to_owned())?;
    let _credential = secrets
        .resolve_as(
            SecretRequest {
                reference: reference.clone(),
                consumer: id.to_owned(),
                use_context: SecretUseContext::Service {
                    use_case: SecretServiceUse::StartupProbe,
                },
            },
            &loaded.authority()?,
        )
        .await?;
    let mut config = HttpsJsonModelConfig::new(endpoint, reference)?.with_limits(
        Duration::from_millis(request_timeout_ms),
        Duration::from_millis(connect_timeout_ms),
        max_response_bytes,
        max_concurrency,
    )?;
    if let Some(path) = root_ca_path {
        let path = resolve_project_file(&loaded.root, path)?;
        config = config.with_exclusive_root_certificates_pem(read_bounded(
            &path,
            MAX_CA_BYTES,
            "exclusive root CA",
        )?)?;
    }
    let model = HttpsJsonModel::new(id, config, secrets)?;
    Ok(Arc::new(model))
}

#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "https-model"))]
async fn build_https_model(
    _loaded: &LoadedConfig,
    _id: &str,
    _endpoint: &str,
    _bearer_secret_reference: &str,
    _bearer_environment: &str,
    _request_timeout_ms: u64,
    _connect_timeout_ms: u64,
    _max_response_bytes: usize,
    _max_concurrency: usize,
    _root_ca_path: Option<&str>,
) -> CliResult<Arc<dyn LanguageModel>> {
    Err("HTTPS model configuration requires a binary built with `--features https-model`".into())
}

fn load_config(path: &str) -> CliResult<LoadedConfig> {
    let requested = PathBuf::from(path);
    let requested = if requested.is_absolute() {
        requested
    } else {
        env::current_dir()?.join(requested)
    };
    let path = fs::canonicalize(&requested)?;
    let encoded = read_bounded(&path, MAX_CONFIG_BYTES, "service config")?;
    let config: ServiceConfig = serde_json::from_slice(&encoded)?;
    if config.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(format!(
            "unsupported service config schema {}; expected {CONFIG_SCHEMA_VERSION}",
            config.schema_version
        )
        .into());
    }
    if !(1..=MAX_PARALLEL_TOOL_CALLS).contains(&config.max_parallel_tool_calls) {
        return Err(format!("max_parallel_tool_calls must be 1-{MAX_PARALLEL_TOOL_CALLS}").into());
    }
    if !(1..=MAX_MODEL_ATTEMPTS_PER_STEP).contains(&config.max_model_attempts_per_step) {
        return Err(
            format!("max_model_attempts_per_step must be 1-{MAX_MODEL_ATTEMPTS_PER_STEP}").into(),
        );
    }
    if let Some(temporal) = &config.temporal {
        temporal.validate()?;
    }
    if let Some(effect_consumer) = &config.effect_consumer {
        effect_consumer.validate()?;
    }
    configured_authority(&config)?;
    let root = path
        .parent()
        .ok_or_else(|| format!("config has no parent directory: {}", path.display()))?
        .to_owned();
    let data_directory = resolve_data_directory(&root, &config.data_directory)?;
    Ok(LoadedConfig {
        config,
        path,
        root,
        data_directory,
    })
}

fn resolve_data_directory(root: &Path, configured: &str) -> CliResult<PathBuf> {
    let path = Path::new(configured);
    let mut has_normal_component = false;
    for component in path.components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(
                    "data_directory must be a project-relative path without parent traversal"
                        .into(),
                );
            }
        }
    }
    if !has_normal_component {
        return Err("data_directory must name a project subdirectory".into());
    }
    Ok(root.join(path))
}

fn resolve_project_file(root: &Path, configured: &str) -> CliResult<PathBuf> {
    let relative = resolve_data_directory(root, configured)?;
    let canonical = fs::canonicalize(&relative)?;
    if !canonical.starts_with(root) {
        return Err(format!("project file escapes {}", root.display()).into());
    }
    Ok(canonical)
}

fn require_contained_directory(root: &Path, directory: &Path) -> CliResult<()> {
    let canonical = fs::canonicalize(directory)?;
    if canonical == root || !canonical.starts_with(root) {
        return Err(format!(
            "data directory must remain below project root {}",
            root.display()
        )
        .into());
    }
    Ok(())
}

fn read_bounded(path: &Path, maximum: u64, kind: &str) -> CliResult<Vec<u8>> {
    let file = File::open(path)?;
    let mut reader = file.take(maximum.saturating_add(1));
    let mut bytes = Vec::with_capacity(usize::try_from(maximum.min(8_192))?);
    reader.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > maximum {
        return Err(format!("{kind} exceeds {maximum} bytes: {}", path.display()).into());
    }
    Ok(bytes)
}

fn write_new_file(path: &Path, bytes: &[u8], kind: &str) -> CliResult<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let result = file.write_all(bytes).and_then(|()| file.sync_all());
    drop(file);
    if let Err(error) = result {
        let cleanup = fs::remove_file(path);
        return match cleanup {
            Ok(()) => Err(format!("cannot write {kind} {}: {error}", path.display()).into()),
            Err(cleanup_error) => Err(format!(
                "cannot write {kind} {}; cleanup also failed: {cleanup_error}",
                path.display()
            )
            .into()),
        };
    }
    Ok(())
}

fn verify_file_sha256(path: &Path, expected: &str, kind: &str) -> CliResult<()> {
    if expected.len() != 64
        || !expected
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("{kind} SHA-256 must be 64 lowercase hexadecimal characters").into());
    }
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_PINNED_COMMAND_BYTES {
        return Err(format!(
            "{kind} exceeds the {MAX_PINNED_COMMAND_BYTES}-byte digest boundary: {}",
            path.display()
        )
        .into());
    }
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 65_536];
    let mut bytes = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read)?)
            .ok_or("command digest byte count overflow")?;
        if bytes > MAX_PINNED_COMMAND_BYTES {
            return Err(format!(
                "{kind} exceeds the {MAX_PINNED_COMMAND_BYTES}-byte digest boundary: {}",
                path.display()
            )
            .into());
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    let mut actual = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        actual.push(char::from(HEX[usize::from(byte >> 4)]));
        actual.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    if actual != expected {
        return Err(format!("{kind} SHA-256 mismatch: {}", path.display()).into());
    }
    Ok(())
}

const fn default_request_timeout_ms() -> u64 {
    60_000
}

const fn default_enabled() -> bool {
    true
}

fn default_demo_model_id() -> String {
    "local/demo".to_owned()
}

const fn default_model_attempt_timeout_ms() -> u64 {
    30_000
}

const fn default_model_retry_initial_delay_ms() -> u64 {
    250
}

const fn default_model_retry_max_delay_ms() -> u64 {
    5_000
}

const fn default_openai_request_timeout_ms() -> u64 {
    120_000
}

const fn default_connect_timeout_ms() -> u64 {
    10_000
}

const fn default_max_response_bytes() -> usize {
    2_097_152
}

const fn default_openai_max_response_bytes() -> usize {
    4_194_304
}

const fn default_max_concurrency() -> usize {
    16
}

const fn default_max_parallel_tool_calls() -> usize {
    DEFAULT_MAX_PARALLEL_TOOL_CALLS
}

const fn default_max_model_attempts_per_step() -> usize {
    DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP
}

fn default_current_directory() -> String {
    ".".to_owned()
}

const fn default_mcp_request_timeout_ms() -> u64 {
    45_000
}

const fn default_mcp_max_response_bytes() -> usize {
    8_388_608
}

const fn default_memory_top_k() -> usize {
    8
}

const fn default_memory_budget_tokens() -> usize {
    2_000
}

const fn default_conversation_max_turns() -> usize {
    32
}

const fn default_conversation_budget_tokens() -> usize {
    65_536
}

const fn default_conversation_budget_bytes() -> usize {
    65_536
}

const fn default_compaction_max_input_turns() -> usize {
    32
}

const fn default_compaction_input_budget_bytes() -> usize {
    524_288
}

const fn default_compaction_output_budget_tokens() -> usize {
    4_096
}

const fn default_compaction_output_budget_bytes() -> usize {
    262_144
}

const fn default_skill_budget_tokens() -> usize {
    8_192
}

const fn default_evaluation_case_concurrency() -> usize {
    4
}

const fn default_evaluation_grader_concurrency() -> usize {
    4
}

const fn default_evaluation_case_timeout_ms() -> u64 {
    300_000
}

const fn default_evaluation_grader_timeout_ms() -> u64 {
    30_000
}

const fn default_tool_timeout_ms() -> u64 {
    30_000
}

const fn default_tool_max_output_bytes() -> usize {
    1_048_576
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{
        CapabilityOrigin, EvaluationBaseline, EvaluationSuite, LoadedConfig, ServiceConfig,
        ServiceEvaluationConfig, ServiceGraderConfig, ServiceJsonProcessConfig,
        ServiceProcessLaunchConfig, ServiceToolConfig, ServiceVerifierConfig, ToolBatchExecution,
        build_capabilities, build_effect_consumer, build_evaluation, build_models,
        configured_authority, configured_skill_trust, load_config, resolve_data_directory,
        verify_file_sha256,
    };
    use std::{
        fs,
        path::{Path, PathBuf},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn config_is_strict_and_data_directory_cannot_escape() {
        let defaulted = serde_json::from_str::<ServiceConfig>(
            r#"{"schema_version":1,"data_directory":".y-harness","model":{"type":"demo"}}"#,
        )
        .expect("minimal config");
        assert_eq!(
            defaulted.max_parallel_tool_calls,
            super::DEFAULT_MAX_PARALLEL_TOOL_CALLS
        );
        assert_eq!(
            defaulted.max_model_attempts_per_step,
            super::DEFAULT_MAX_MODEL_ATTEMPTS_PER_STEP
        );
        let explicitly_safe = serde_json::from_str::<ServiceConfig>(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "tools": [{
                "type": "json_command",
                "name": "pure",
                "description": "pure fixture",
                "input_schema": {},
                "batch_execution": "parallel_safe",
                "process": {
                  "command": "/unused",
                  "launch": {"type": "unrestricted", "max_concurrency": 1}
                }
              }]
            }"#,
        )
        .expect("explicit Tool scheduling");
        let ServiceToolConfig::JsonCommand {
            batch_execution, ..
        } = &explicitly_safe.tools[0];
        assert_eq!(*batch_execution, ToolBatchExecution::ParallelSafe);
        assert!(
            serde_json::from_str::<ServiceConfig>(
                r#"{"schema_version":1,"data_directory":".y-harness","model":{"type":"open_ai_responses","id":"openai/default","model":"model-explicit","api_key_secret_reference":"openai/default","api_key_environment":"OPENAI_API_KEY"}}"#
            )
            .is_ok()
        );
        assert!(
            serde_json::from_str::<ServiceConfig>(
                r#"{"schema_version":1,"data_directory":".y-harness","model":{"type":"demo"},"extra":true}"#
            )
            .is_err()
        );
        assert!(resolve_data_directory(Path::new("/project"), "../outside").is_err());
        assert!(resolve_data_directory(Path::new("/project"), "/outside").is_err());
        assert!(resolve_data_directory(Path::new("/project"), ".").is_err());
    }

    #[tokio::test]
    async fn fixed_tenant_authority_is_exact_and_rejects_shared_mcp_sessions() {
        let tenant = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "authority": {
                "type": "local_process_tenant",
                "tenant_id": "tenant-a"
              },
              "model": {"type": "demo"}
            }"#,
        );
        assert_eq!(
            configured_authority(&tenant.config)
                .expect("fixed tenant authority")
                .tenant_id(),
            Some("tenant-a")
        );

        let invalid = serde_json::from_str::<ServiceConfig>(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "authority": {
                "type": "local_process_tenant",
                "tenant_id": "\n"
              },
              "model": {"type": "demo"}
            }"#,
        )
        .expect("shape-valid authority");
        assert!(configured_authority(&invalid).is_err());

        let shared_mcp = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "authority": {
                "type": "local_process_tenant",
                "tenant_id": "tenant-a"
              },
              "model": {"type": "demo"},
              "mcp_servers": [{
                "id": "shared",
                "command": "/unused",
                "launch": {
                  "type": "unrestricted",
                  "max_concurrency": 1
                }
              }]
            }"#,
        );
        let error = build_capabilities(&shared_mcp, true)
            .await
            .err()
            .expect("shared MCP must fail closed");
        assert!(
            error
                .to_string()
                .contains("requires tenant-partitioned MCP sessions")
        );
    }

    #[test]
    fn external_skill_trust_rejects_noncanonical_keys_and_invalid_policy() {
        let noncanonical = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "skills": {
                "package_files": [],
                "external_package_files": [],
                "activate": [],
                "trust": {
                  "publishers": [{
                    "key_id": "publisher",
                    "public_key_hex": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                  }]
                }
              }
            }"#,
        );
        let error = configured_skill_trust(&noncanonical)
            .err()
            .expect("reject noncanonical public key");
        assert!(error.to_string().contains("lowercase hexadecimal"));

        let reversed_window = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "skills": {
                "package_files": [],
                "external_package_files": [],
                "activate": [],
                "trust": {
                  "publishers": [{
                    "key_id": "publisher",
                    "public_key_hex": "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
                    "not_before_ms": 10,
                    "not_after_ms": 10
                  }]
                }
              }
            }"#,
        );
        let error = configured_skill_trust(&reversed_window)
            .err()
            .expect("reject reversed validity");
        assert!(error.to_string().contains("empty or reversed"));
    }

    #[test]
    fn config_rejects_an_unbounded_parallel_tool_limit() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "y-harness-parallel-config-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create config fixture");
        let path = root.join("y-harness.json");
        fs::write(
            &path,
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "max_parallel_tool_calls": 0,
              "model": {"type": "demo"}
            }"#,
        )
        .expect("write invalid config");
        let error = load_config(path.to_str().expect("UTF-8 fixture path"))
            .err()
            .expect("reject invalid parallel limit");
        assert!(
            error
                .to_string()
                .contains("max_parallel_tool_calls must be 1-64")
        );
        fs::remove_dir_all(root).expect("remove config fixture");
    }

    #[test]
    fn config_rejects_an_unbounded_model_attempt_limit() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "y-harness-model-attempt-config-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create config fixture");
        let path = root.join("y-harness.json");
        fs::write(
            &path,
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "max_model_attempts_per_step": 0,
              "model": {"type": "demo"}
            }"#,
        )
        .expect("write invalid config");

        let error = load_config(path.to_str().expect("UTF-8 fixture path"))
            .err()
            .expect("reject invalid Model attempt limit");

        assert!(
            error
                .to_string()
                .contains("max_model_attempts_per_step must be 1-144")
        );
        fs::remove_dir_all(root).expect("remove config fixture");
    }

    #[test]
    fn config_rejects_temporal_bounds_before_service_assembly() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "y-harness-temporal-config-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create Temporal config fixture");
        let path = root.join("y-harness.json");
        fs::write(
            &path,
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "temporal": {
                "poll_interval_ms": 99,
                "scan_limit": 257
              }
            }"#,
        )
        .expect("write invalid Temporal config");

        let error = load_config(path.to_str().expect("UTF-8 fixture path"))
            .err()
            .expect("reject invalid Temporal bounds");

        assert!(
            error
                .to_string()
                .contains("temporal.poll_interval_ms must be 100-86400000")
        );
        fs::remove_dir_all(root).expect("remove Temporal config fixture");
    }

    #[tokio::test]
    async fn effect_consumer_requires_explicit_exact_authority_and_bounded_timeouts() {
        let root = fs::canonicalize(std::env::current_dir().expect("current directory"))
            .expect("canonical project");
        let command = std::env::current_exe()
            .expect("current executable")
            .to_string_lossy()
            .into_owned();
        let digest = Sha256::digest(fs::read(&command).expect("read current executable"));
        let command_sha256 = digest
            .iter()
            .fold(String::with_capacity(64), |mut encoded, byte| {
                use std::fmt::Write as _;
                write!(encoded, "{byte:02x}").expect("encode digest");
                encoded
            });
        let configured = |allow_operation: &str, connectors: usize, process_timeout_ms: u64| {
            let connector = serde_json::json!({
                "origin_id": "test/effect-execution",
                "capability": "notification.test",
                "operations": ["send"],
                "idempotency": "target_enforced",
                "process": {
                    "command": command,
                    "command_sha256": command_sha256,
                    "current_directory": ".",
                    "timeout_ms": process_timeout_ms,
                    "max_output_bytes": 1_024,
                    "launch": {"type": "unrestricted", "max_concurrency": 1}
                }
            });
            let connector_list = std::iter::repeat_n(connector, connectors).collect::<Vec<_>>();
            serde_json::from_value::<ServiceConfig>(serde_json::json!({
                "schema_version": 1,
                "data_directory": ".y-harness",
                "model": {"type": "demo"},
                "effect_consumer": {
                    "execution": {
                        "poll_interval_ms": 100,
                        "failure_backoff_ms": 100,
                        "executor": {
                            "scan_limit": 8,
                            "max_concurrency": 2,
                            "policy_timeout_ms": 1_000,
                            "execution_timeout_ms": 2_000,
                            "settlement_reserve_ms": 1_000,
                            "lease_duration_ms": 5_000
                        },
                        "allow": [{
                            "capability": "notification.test",
                            "operation": allow_operation
                        }],
                        "connectors": connector_list
                    }
                }
            }))
            .expect("Effect consumer config")
        };
        let loaded = |config| LoadedConfig {
            config,
            path: root.join("y-harness.json"),
            data_directory: root.join(".y-harness"),
            root: root.clone(),
        };

        let valid = build_effect_consumer(&loaded(configured("send", 1, 1_000)))
            .await
            .expect("valid Effect consumer")
            .expect("Effect consumer enabled");
        assert!(valid.doctor_summary().contains(
            "execution 1 dispatch-locked connector(s) / 0 credential-scoped / 0 secret variable(s) / 1 allow(s) / 100 ms poll / 100 ms backoff"
        ));

        let mut unlocked =
            serde_json::to_value(configured("send", 1, 1_000)).expect("encode unlocked fixture");
        unlocked["effect_consumer"]["execution"]["connectors"][0]["process"]
            .as_object_mut()
            .expect("process object")
            .remove("command_sha256");
        let unlocked =
            serde_json::from_value::<ServiceConfig>(unlocked).expect("decode unlocked fixture");
        let error = build_effect_consumer(&loaded(unlocked))
            .await
            .err()
            .expect("reject unlocked Effect Connector");
        assert!(
            error
                .to_string()
                .contains("Effect execution Connector notification.test requires command_sha256")
        );

        let unsupported = build_effect_consumer(&loaded(configured("delete", 1, 1_000)))
            .await
            .err()
            .expect("reject unsupported allow");
        assert!(unsupported.to_string().contains(
            "Effect execution allow notification.test/delete has no exact configured Connector"
        ));

        let duplicate = build_effect_consumer(&loaded(configured("send", 2, 1_000)))
            .await
            .err()
            .expect("reject duplicate Connector");
        assert!(
            duplicate
                .to_string()
                .contains("duplicate Effect execution Connector notification.test")
        );

        let timeout = build_effect_consumer(&loaded(configured("send", 1, 2_001)))
            .await
            .err()
            .expect("reject Connector timeout outside Executor budget");
        assert!(
            timeout
                .to_string()
                .contains("process timeout 2001 ms exceeds executor execution timeout 2000 ms")
        );

        let mut missing_secret =
            serde_json::to_value(configured("send", 1, 1_000)).expect("encode Secret fixture");
        missing_secret["effect_consumer"]["execution"]["connectors"][0]["process"]["secret_environment"] = serde_json::json!({
            "EFFECT_TOKEN": {
                "reference": "effect/private-test",
                "host_environment": "YH_DELIBERATELY_MISSING_EFFECT_SECRET"
            }
        });
        let missing_secret =
            serde_json::from_value::<ServiceConfig>(missing_secret).expect("decode Secret fixture");
        let error = build_effect_consumer(&loaded(missing_secret))
            .await
            .err()
            .expect("reject unavailable Effect Secret");
        assert!(error.to_string().contains(
            "Effect execution Connector notification.test credential availability probe failed"
        ));
        assert!(!error.to_string().contains("private-test"));
        assert!(!error.to_string().contains("YH_DELIBERATELY"));
    }

    #[tokio::test]
    async fn configured_json_tool_preserves_explicit_parallel_safety() {
        let root = fs::canonicalize(std::env::current_dir().expect("current directory"))
            .expect("canonical project");
        let config = ServiceConfig {
            tools: vec![ServiceToolConfig::JsonCommand {
                name: "pure-command".to_owned(),
                description: "pure command fixture".to_owned(),
                input_schema: serde_json::json!({"type": "object"}),
                batch_execution: ToolBatchExecution::ParallelSafe,
                process: ServiceJsonProcessConfig {
                    command: std::env::current_exe()
                        .expect("current executable")
                        .to_string_lossy()
                        .into_owned(),
                    command_sha256: None,
                    args: Vec::new(),
                    current_directory: ".".to_owned(),
                    environment_from_host: std::collections::BTreeMap::new(),
                    secret_environment: std::collections::BTreeMap::new(),
                    timeout_ms: 1_000,
                    max_output_bytes: 1_024,
                    launch: ServiceProcessLaunchConfig::Unrestricted { max_concurrency: 1 },
                },
            }],
            ..ServiceConfig::default()
        };
        let loaded = LoadedConfig {
            config,
            path: root.join("y-harness.json"),
            data_directory: root.join(".y-harness"),
            root,
        };

        let capabilities = build_capabilities(&loaded, false)
            .await
            .expect("configured Tool");
        assert_eq!(
            capabilities
                .tools
                .get("pure-command")
                .map(|tool| tool.batch_execution),
            Some(ToolBatchExecution::ParallelSafe)
        );
    }

    #[tokio::test]
    async fn configured_json_verifier_retains_external_registry_origin() {
        let root = fs::canonicalize(std::env::current_dir().expect("current directory"))
            .expect("canonical project");
        let config = ServiceConfig {
            verifiers: vec![ServiceVerifierConfig {
                name: "project.completion-gate".to_owned(),
                description: "Fixture completion gate".to_owned(),
                process: ServiceJsonProcessConfig {
                    command: std::env::current_exe()
                        .expect("current executable")
                        .to_string_lossy()
                        .into_owned(),
                    command_sha256: None,
                    args: Vec::new(),
                    current_directory: ".".to_owned(),
                    environment_from_host: std::collections::BTreeMap::new(),
                    secret_environment: std::collections::BTreeMap::new(),
                    timeout_ms: 1_000,
                    max_output_bytes: 1_024,
                    launch: ServiceProcessLaunchConfig::Unrestricted { max_concurrency: 1 },
                },
            }],
            ..ServiceConfig::default()
        };
        let loaded = LoadedConfig {
            config,
            path: root.join("y-harness.json"),
            data_directory: root.join(".y-harness"),
            root,
        };

        let capabilities = build_capabilities(&loaded, false)
            .await
            .expect("configured Verifier");
        assert_eq!(
            capabilities
                .verification
                .get("project.completion-gate")
                .map(|registered| &registered.origin),
            Some(&CapabilityOrigin::External {
                id: "json-command-verifier/project.completion-gate".to_owned()
            })
        );
    }

    #[test]
    fn configured_json_grader_retains_external_registry_origin() {
        let root = fs::canonicalize(std::env::current_dir().expect("current directory"))
            .expect("canonical project");
        let config = ServiceConfig {
            evaluation: Some(ServiceEvaluationConfig {
                case_concurrency: 2,
                grader_concurrency: 2,
                default_case_timeout_ms: 1_000,
                grader_timeout_ms: 1_000,
                graders: vec![ServiceGraderConfig {
                    name: "project.quality".to_owned(),
                    description: "Fixture Evaluation quality Grader".to_owned(),
                    process: ServiceJsonProcessConfig {
                        command: std::env::current_exe()
                            .expect("current executable")
                            .to_string_lossy()
                            .into_owned(),
                        command_sha256: None,
                        args: Vec::new(),
                        current_directory: ".".to_owned(),
                        environment_from_host: std::collections::BTreeMap::new(),
                        secret_environment: std::collections::BTreeMap::new(),
                        timeout_ms: 1_000,
                        max_output_bytes: 1_024,
                        launch: ServiceProcessLaunchConfig::Unrestricted { max_concurrency: 1 },
                    },
                }],
            }),
            ..ServiceConfig::default()
        };
        let loaded = LoadedConfig {
            config,
            path: root.join("y-harness.json"),
            data_directory: root.join(".y-harness"),
            root,
        };

        let configured = build_evaluation(&loaded)
            .expect("configured Evaluation")
            .expect("Evaluation enabled");
        assert_eq!(
            configured
                .graders
                .get("project.quality")
                .map(|registered| &registered.origin),
            Some(&CapabilityOrigin::External {
                id: "json-command-grader/project.quality".to_owned()
            })
        );
    }

    #[test]
    fn shipped_real_provider_configs_follow_the_strict_schema() {
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.openai.example.json"
        ))
        .expect("OpenAI example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.openai-amh.macos.example.json"
        ))
        .expect("OpenAI plus Agent Memory Hub example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.openai-command.macos.example.json"
        ))
        .expect("OpenAI plus JSON command Tool example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.skill.example.json"
        ))
        .expect("project Skill example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.route.example.json"
        ))
        .expect("explicit Model route example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.https-mcp.example.json"
        ))
        .expect("HTTPS MCP example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.command-model.example.json"
        ))
        .expect("JSON command Model example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.command-compactor.example.json"
        ))
        .expect("JSON command conversation compactor example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.verifier.example.json"
        ))
        .expect("JSON command Verifier example config");
        serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.eval.example.json"
        ))
        .expect("JSON command Evaluation Grader example config");
        let temporal = serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.temporal.example.json"
        ))
        .expect("Temporal service example config");
        temporal
            .temporal
            .as_ref()
            .expect("Temporal example enables polling")
            .validate()
            .expect("valid Temporal example bounds");
        let effect_consumer = serde_json::from_str::<ServiceConfig>(include_str!(
            "../../config/y-harness.effect-consumer.example.json"
        ))
        .expect("Effect consumer service example config");
        effect_consumer
            .effect_consumer
            .as_ref()
            .expect("Effect consumer example enables polling")
            .validate()
            .expect("valid Effect consumer example bounds");
        let suite: EvaluationSuite =
            serde_json::from_str(include_str!("../../evals/configured-example-suite.json"))
                .expect("configured Evaluation example suite");
        EvaluationSuite::new(suite.name, suite.cases).expect("validated Evaluation example suite");
        let baseline: EvaluationBaseline =
            serde_json::from_str(include_str!("../../evals/configured-example-baseline.json"))
                .expect("configured Evaluation example baseline");
        EvaluationBaseline::new(baseline.requirements)
            .expect("validated Evaluation example baseline");
    }

    #[tokio::test]
    async fn model_catalog_validation_precedes_provider_construction() {
        let mixed = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "models": [{"type": "demo"}],
              "model_route": {"models": ["local/demo"]}
            }"#,
        );
        let error = build_models(&mixed)
            .await
            .err()
            .expect("reject mixed configuration");
        assert!(error.to_string().contains("never both"));

        let unknown = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {"models": ["missing/model"]}
            }"#,
        );
        let error = build_models(&unknown)
            .await
            .err()
            .expect("reject unknown route entry");
        assert!(error.to_string().contains("unknown model"));

        let invalid_timeout = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {
                "models": ["local/demo"],
                "attempt_timeout_ms": 0
              }
            }"#,
        );
        let error = build_models(&invalid_timeout)
            .await
            .err()
            .expect("reject invalid attempt timeout");
        assert!(error.to_string().contains("1-86400000"));

        let invalid_cooldown = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {
                "models": ["local/demo"],
                "timeout_cooldown_ms": 86400001
              }
            }"#,
        );
        let error = build_models(&invalid_cooldown)
            .await
            .err()
            .expect("reject invalid timeout cooldown");
        assert!(error.to_string().contains("0-86400000"));

        let single_cooldown = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {
                "models": ["local/demo"],
                "timeout_cooldown_ms": 1000
              }
            }"#,
        );
        let error = build_models(&single_cooldown)
            .await
            .err()
            .expect("reject useless single-Model cooldown");
        assert!(error.to_string().contains("at least two"));

        let invalid_retries = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo", "id": "must-not-be-constructed"}],
              "model_route": {
                "models": ["must-not-be-constructed"],
                "retry": {"max_retries": 0}
              }
            }"#,
        );
        let error = build_models(&invalid_retries)
            .await
            .err()
            .expect("reject invalid retry count");
        assert!(error.to_string().contains("1-8"));

        let inverted_retry_delay = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {
                "models": ["local/demo"],
                "retry": {
                  "max_retries": 2,
                  "initial_delay_ms": 100,
                  "max_delay_ms": 10
                }
              }
            }"#,
        );
        let error = build_models(&inverted_retry_delay)
            .await
            .err()
            .expect("reject inverted retry delay");
        assert!(error.to_string().contains("cannot exceed"));

        let configured_retry = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "models": [{"type": "demo"}],
              "model_route": {
                "models": ["local/demo"],
                "retry": {"max_retries": 2}
              }
            }"#,
        );
        let configured_retry = build_models(&configured_retry)
            .await
            .expect("valid retry policy")
            .retry_policy
            .expect("enabled retry policy");
        assert_eq!(configured_retry.max_retries(), 2);
        assert_eq!(configured_retry.initial_delay(), Duration::from_millis(250));
        assert_eq!(configured_retry.max_delay(), Duration::from_millis(5_000));
    }

    #[tokio::test]
    async fn disabled_mcp_server_grants_no_process_or_tool_authority() {
        let configured = loaded(
            r#"{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {"type": "demo"},
              "mcp_servers": [{
                "id": "disabled",
                "enabled": false,
                "command": "/path/that/must/not/be-opened",
                "launch": {
                  "type": "unrestricted",
                  "max_concurrency": 1
                },
                "tools": {
                  "namespace": "disabled",
                  "allow": ["never"]
                }
              }]
            }"#,
        );
        let capabilities = super::build_capabilities(&configured, true)
            .await
            .expect("disabled server");
        assert!(capabilities.mcp_clients.is_empty());
        assert_eq!(capabilities.mcp_configured, 1);
        assert_eq!(capabilities.tools.descriptors().len(), 1);
    }

    #[test]
    fn optional_mcp_command_pin_is_exact_and_content_sensitive() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "y-harness-mcp-command-{}-{nonce}",
            std::process::id()
        ));
        fs::write(&path, b"mcp").expect("write command fixture");
        verify_file_sha256(
            &path,
            "10182ab855ff772753c05b2fea333666b5f312835d32936b6b03e08ef2cbd6d3",
            "MCP command",
        )
        .expect("matching digest");
        let error =
            verify_file_sha256(&path, &"0".repeat(64), "MCP command").expect_err("digest mismatch");
        assert!(error.to_string().contains("mismatch"));
        fs::remove_file(path).expect("remove command fixture");
    }

    fn loaded(encoded: &str) -> LoadedConfig {
        LoadedConfig {
            config: serde_json::from_str(encoded).expect("test Service config"),
            path: PathBuf::from("/project/y-harness.json"),
            root: PathBuf::from("/project"),
            data_directory: PathBuf::from("/project/.y-harness"),
        }
    }
}
