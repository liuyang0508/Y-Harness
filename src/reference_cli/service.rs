//! Minimal project configuration and persistent stdio service host.

use std::{
    collections::BTreeMap,
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use y_harness::{
    APPROVAL_INBOX_SCHEMA_VERSION, AgentMemoryHubProvider, AllowListPolicy, ApprovalInbox,
    CapabilityOrigin, ContextEngine, HarnessRuntime, InboxApprovalHandler, JsonCommandTool,
    JsonProcessConfig, LanguageModel, LocalProcessBroker, MacOsSeatbeltBroker, McpClient,
    MemoryContextConfig, MemoryFailureMode, MemoryHealthStatus, MemoryProvider, MemoryRegistry,
    ModelRegistry, NetworkAccess, PROTOCOL_VERSION, ProcessBroker, ProtocolHandler,
    SECRET_API_VERSION, STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION,
    SqliteApprovalInbox, SqliteEventStore, SqliteTaskCoordinator, StateEngine, StdioMcpClient,
    StdioMcpConfig, StdioMcpLaunchAuthority, TASK_GRAPH_SCHEMA_VERSION, TaskCoordinator,
    ToolDescriptor, ToolRegistry, register_selected_mcp_tools, serve_stdio,
};

#[cfg(feature = "https-model")]
use y_harness::{
    EnvironmentSecretProvider, HttpsJsonModel, HttpsJsonModelConfig, OpenAiResponsesModel,
    OpenAiResponsesModelConfig, SecretProvider, SecretReference, SecretRequest, ThreadId, TurnId,
};

use super::{CliResult, DemoModel, EchoTool};

const CONFIG_FILE: &str = "y-harness.json";
const CONFIG_SCHEMA_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: u64 = 65_536;
#[cfg(feature = "https-model")]
const MAX_CA_BYTES: u64 = 1_048_576;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceConfig {
    schema_version: u32,
    data_directory: String,
    model: ServiceModelConfig,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ServiceToolConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    mcp_servers: Vec<ServiceMcpServerConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory: Option<ServiceMemoryConfig>,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            data_directory: ".y-harness".to_owned(),
            model: ServiceModelConfig::Demo,
            tools: Vec::new(),
            mcp_servers: Vec::new(),
            memory: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceModelConfig {
    Demo,
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

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceToolConfig {
    JsonCommand {
        name: String,
        description: String,
        input_schema: Value,
        process: ServiceJsonProcessConfig,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceJsonProcessConfig {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "default_current_directory")]
    current_directory: String,
    #[serde(default)]
    environment_from_host: BTreeMap<String, String>,
    #[serde(default = "default_tool_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_tool_max_output_bytes")]
    max_output_bytes: usize,
    launch: ServiceProcessLaunchConfig,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ServiceMcpServerConfig {
    id: String,
    command: String,
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
struct ServiceMcpToolsConfig {
    namespace: String,
    allow: Vec<String>,
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ServiceMemoryFailureMode {
    #[default]
    Degrade,
    FailTurn,
}

struct LoadedConfig {
    config: ServiceConfig,
    path: PathBuf,
    root: PathBuf,
    data_directory: PathBuf,
}

struct ConfiguredModel {
    id: String,
    origin: CapabilityOrigin,
    model: Arc<dyn LanguageModel>,
    demo_tools: bool,
}

struct ConfiguredCapabilities {
    tools: ToolRegistry,
    policy: AllowListPolicy,
    context: ContextEngine,
    mcp_clients: BTreeMap<String, Arc<StdioMcpClient>>,
    memory_health: Option<MemoryHealthStatus>,
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

/// Validates configuration, provider construction, credentials, and storage boundaries.
pub async fn run_doctor(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    let model = build_model(&loaded).await?;
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
    let capabilities = build_capabilities(&loaded, model.demo_tools).await?;

    println!("Y-Harness doctor");
    println!("engine: {}", env!("CARGO_PKG_VERSION"));
    println!("protocol: {PROTOCOL_VERSION}");
    println!("config schema: {}", loaded.config.schema_version);
    println!("config: {}", loaded.path.display());
    println!("model: {}", model.id);
    println!("tools: {}", capabilities.tools.descriptors().len());
    println!("mcp servers: {}", capabilities.mcp_clients.len());
    if let Some(status) = &capabilities.memory_health {
        println!("memory: agent-memory-hub ({status:?})");
    } else {
        println!("memory: disabled");
    }
    println!("data: {} ({data_state})", loaded.data_directory.display());
    println!(
        "schemas: state={STATE_EVENT_SCHEMA_VERSION}/{STATE_SNAPSHOT_SCHEMA_VERSION} approval={APPROVAL_INBOX_SCHEMA_VERSION} task={TASK_GRAPH_SCHEMA_VERSION} secret={SECRET_API_VERSION}"
    );
    shutdown_mcp_clients(&capabilities.mcp_clients).await?;
    println!("status: ok");
    Ok(())
}

/// Runs the durable stdio service described by one validated project configuration.
pub async fn run_service(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;

    let configured_model = build_model(&loaded).await?;
    let capabilities = build_capabilities(&loaded, configured_model.demo_tools).await?;
    let ConfiguredCapabilities {
        tools,
        policy,
        context,
        mcp_clients,
        memory_health: _,
    } = capabilities;
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(loaded.data_directory.join("state.db")).await?,
    ));
    let approvals =
        Arc::new(SqliteApprovalInbox::open(loaded.data_directory.join("approvals.db")).await?);
    let tasks =
        Arc::new(SqliteTaskCoordinator::open(loaded.data_directory.join("tasks.db")).await?);

    let mut models = ModelRegistry::new();
    models.register(configured_model.origin, configured_model.model)?;
    let approval_handler = Arc::new(InboxApprovalHandler::new(
        approvals.clone(),
        Duration::from_millis(250),
    )?);
    let runtime = Arc::new(
        HarnessRuntime::from_model_registry(
            &models,
            &configured_model.id,
            tools,
            Arc::new(policy),
            state,
        )?
        .with_context_engine(context)
        .with_approval_handler(approval_handler),
    );
    let approval_port: Arc<dyn ApprovalInbox> = approvals;
    let task_port: Arc<dyn TaskCoordinator> = tasks;
    let handler = ProtocolHandler::new(runtime)
        .with_approval_inbox(approval_port)
        .with_task_coordinator(task_port);
    let served = serve_stdio(handler).await;
    let shutdown = shutdown_mcp_clients(&mcp_clients).await;
    served?;
    shutdown
}

async fn build_capabilities(
    loaded: &LoadedConfig,
    demo_tools: bool,
) -> CliResult<ConfiguredCapabilities> {
    let mut clients = BTreeMap::new();
    for configured in &loaded.config.mcp_servers {
        if clients.contains_key(&configured.id) {
            return Err(format!("duplicate MCP server id {}", configured.id).into());
        }
        let command = PathBuf::from(&configured.command);
        if !command.is_absolute() || !command.is_file() {
            return Err(format!(
                "MCP server {} command must be an existing absolute file: {}",
                configured.id,
                command.display()
            )
            .into());
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
        clients.insert(configured.id.clone(), client);
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
                process,
            } => {
                let command = PathBuf::from(&process.command);
                if !command.is_absolute() || !command.is_file() {
                    return Err(format!(
                        "JSON Tool {name} command must be an existing absolute file: {}",
                        command.display()
                    )
                    .into());
                }
                let current_dir = resolve_runtime_directory(
                    &loaded.root,
                    &process.current_directory,
                    "JSON Tool working directory",
                )?;
                let environment = environment_from_host(&process.environment_from_host)?;
                let broker = build_process_broker(&loaded.root, &process.launch)?;
                let tool = JsonCommandTool::new(
                    ToolDescriptor {
                        name: name.clone(),
                        description: description.clone(),
                        input_schema: input_schema.clone(),
                    },
                    JsonProcessConfig {
                        program: command,
                        args: process.args.clone(),
                        current_dir,
                        environment,
                        timeout: Duration::from_millis(process.timeout_ms),
                        max_output_bytes: process.max_output_bytes,
                    },
                    broker,
                )?;
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
    for configured in &loaded.config.mcp_servers {
        let Some(exposure) = &configured.tools else {
            continue;
        };
        if exposure.allow.is_empty() {
            return Err(format!(
                "MCP server {} tools.allow must name at least one remote tool",
                configured.id
            )
            .into());
        }
        let client = clients
            .get(&configured.id)
            .ok_or_else(|| format!("MCP server {} was not constructed", configured.id))?;
        let registered = register_selected_mcp_tools(
            &mut tools,
            CapabilityOrigin::External {
                id: format!("mcp/{}", configured.id),
            },
            &exposure.namespace,
            client.clone() as Arc<dyn McpClient>,
            &exposure.allow,
        )
        .await?;
        for name in registered {
            policy = policy.allow(name);
        }
    }

    let (context, memory_health) = match &loaded.config.memory {
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
            let provider = Arc::new(AgentMemoryHubProvider::new(
                client.clone() as Arc<dyn McpClient>
            ));
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

    Ok(ConfiguredCapabilities {
        tools,
        policy,
        context,
        mcp_clients: clients,
        memory_health,
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

async fn shutdown_mcp_clients(clients: &BTreeMap<String, Arc<StdioMcpClient>>) -> CliResult<()> {
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

async fn build_model(loaded: &LoadedConfig) -> CliResult<ConfiguredModel> {
    match &loaded.config.model {
        ServiceModelConfig::Demo => Ok(ConfiguredModel {
            id: "local/demo".to_owned(),
            origin: CapabilityOrigin::BuiltIn,
            model: Arc::new(DemoModel),
            demo_tools: true,
        }),
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
            build_openai_model(
                id,
                model,
                api_key_secret_reference,
                api_key_environment,
                *request_timeout_ms,
                *connect_timeout_ms,
                *max_response_bytes,
                *max_concurrency,
            )
            .await
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
            build_https_model(
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
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "https-model")]
async fn build_openai_model(
    id: &str,
    model: &str,
    api_key_secret_reference: &str,
    api_key_environment: &str,
    request_timeout_ms: u64,
    connect_timeout_ms: u64,
    max_response_bytes: usize,
    max_concurrency: usize,
) -> CliResult<ConfiguredModel> {
    let reference = SecretReference::new(api_key_secret_reference.to_owned())?;
    let secrets = Arc::new(EnvironmentSecretProvider::new(
        "service-environment",
        BTreeMap::from([(reference.clone(), api_key_environment.to_owned())]),
    )?);
    let _credential = secrets
        .resolve(SecretRequest {
            reference: reference.clone(),
            consumer: id.to_owned(),
            thread_id: ThreadId::from_static("doctor-thread"),
            turn_id: TurnId::from_static("doctor-turn"),
        })
        .await?;
    let config = OpenAiResponsesModelConfig::new(model, reference)?.with_limits(
        Duration::from_millis(request_timeout_ms),
        Duration::from_millis(connect_timeout_ms),
        max_response_bytes,
        max_concurrency,
    )?;
    let model = OpenAiResponsesModel::new(id, config, secrets)?;
    Ok(ConfiguredModel {
        id: id.to_owned(),
        origin: CapabilityOrigin::TrustedExtension {
            id: "first-party-openai-responses".to_owned(),
        },
        model: Arc::new(model),
        demo_tools: false,
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "https-model"))]
async fn build_openai_model(
    _id: &str,
    _model: &str,
    _api_key_secret_reference: &str,
    _api_key_environment: &str,
    _request_timeout_ms: u64,
    _connect_timeout_ms: u64,
    _max_response_bytes: usize,
    _max_concurrency: usize,
) -> CliResult<ConfiguredModel> {
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
) -> CliResult<ConfiguredModel> {
    let reference = SecretReference::new(bearer_secret_reference.to_owned())?;
    let secrets = Arc::new(EnvironmentSecretProvider::new(
        "service-environment",
        BTreeMap::from([(reference.clone(), bearer_environment.to_owned())]),
    )?);
    let _credential = secrets
        .resolve(SecretRequest {
            reference: reference.clone(),
            consumer: id.to_owned(),
            thread_id: ThreadId::from_static("doctor-thread"),
            turn_id: TurnId::from_static("doctor-turn"),
        })
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
    Ok(ConfiguredModel {
        id: id.to_owned(),
        origin: CapabilityOrigin::TrustedExtension {
            id: "reference-https-json-gateway".to_owned(),
        },
        model: Arc::new(model),
        demo_tools: false,
    })
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
) -> CliResult<ConfiguredModel> {
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

#[cfg(feature = "https-model")]
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

const fn default_request_timeout_ms() -> u64 {
    60_000
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

fn default_current_directory() -> String {
    ".".to_owned()
}

const fn default_mcp_request_timeout_ms() -> u64 {
    45_000
}

const fn default_memory_top_k() -> usize {
    8
}

const fn default_memory_budget_tokens() -> usize {
    2_000
}

const fn default_tool_timeout_ms() -> u64 {
    30_000
}

const fn default_tool_max_output_bytes() -> usize {
    1_048_576
}

#[cfg(test)]
mod tests {
    use super::{ServiceConfig, resolve_data_directory};
    use std::path::Path;

    #[test]
    fn config_is_strict_and_data_directory_cannot_escape() {
        assert!(
            serde_json::from_str::<ServiceConfig>(
                r#"{"schema_version":1,"data_directory":".y-harness","model":{"type":"demo"}}"#
            )
            .is_ok()
        );
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
    }
}
