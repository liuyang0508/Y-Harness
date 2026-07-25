//! Minimal project configuration and persistent stdio service host.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use y_harness::{
    APPROVAL_INBOX_SCHEMA_VERSION, AllowListPolicy, ApprovalInbox, CapabilityOrigin,
    HarnessRuntime, InboxApprovalHandler, LanguageModel, ModelRegistry, PROTOCOL_VERSION,
    ProtocolHandler, SECRET_API_VERSION, STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION,
    SqliteApprovalInbox, SqliteEventStore, SqliteTaskCoordinator, StateEngine,
    TASK_GRAPH_SCHEMA_VERSION, TaskCoordinator, ToolRegistry, serve_stdio,
};

#[cfg(feature = "https-model")]
use std::collections::BTreeMap;
#[cfg(feature = "https-model")]
use y_harness::{
    EnvironmentSecretProvider, HttpsJsonModel, HttpsJsonModelConfig, SecretProvider,
    SecretReference, SecretRequest, ThreadId, TurnId,
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
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            data_directory: ".y-harness".to_owned(),
            model: ServiceModelConfig::Demo,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ServiceModelConfig {
    Demo,
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

    println!("Y-Harness doctor");
    println!("engine: {}", env!("CARGO_PKG_VERSION"));
    println!("protocol: {PROTOCOL_VERSION}");
    println!("config schema: {}", loaded.config.schema_version);
    println!("config: {}", loaded.path.display());
    println!("model: {}", model.id);
    println!("data: {} ({data_state})", loaded.data_directory.display());
    println!(
        "schemas: state={STATE_EVENT_SCHEMA_VERSION}/{STATE_SNAPSHOT_SCHEMA_VERSION} approval={APPROVAL_INBOX_SCHEMA_VERSION} task={TASK_GRAPH_SCHEMA_VERSION} secret={SECRET_API_VERSION}"
    );
    println!("status: ok");
    Ok(())
}

/// Runs the durable stdio service described by one validated project configuration.
pub async fn run_service(config_path: String) -> CliResult<()> {
    let loaded = load_config(&config_path)?;
    fs::create_dir_all(&loaded.data_directory)?;
    require_contained_directory(&loaded.root, &loaded.data_directory)?;

    let configured_model = build_model(&loaded).await?;
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(loaded.data_directory.join("state.db")).await?,
    ));
    let approvals =
        Arc::new(SqliteApprovalInbox::open(loaded.data_directory.join("approvals.db")).await?);
    let tasks =
        Arc::new(SqliteTaskCoordinator::open(loaded.data_directory.join("tasks.db")).await?);

    let mut models = ModelRegistry::new();
    models.register(configured_model.origin, configured_model.model)?;
    let mut tools = ToolRegistry::new();
    let policy = if configured_model.demo_tools {
        tools.register(CapabilityOrigin::BuiltIn, Arc::new(EchoTool))?;
        AllowListPolicy::deny_by_default().allow("echo")
    } else {
        AllowListPolicy::deny_by_default()
    };
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
        .with_approval_handler(approval_handler),
    );
    let approval_port: Arc<dyn ApprovalInbox> = approvals;
    let task_port: Arc<dyn TaskCoordinator> = tasks;
    let handler = ProtocolHandler::new(runtime)
        .with_approval_inbox(approval_port)
        .with_task_coordinator(task_port);
    serve_stdio(handler).await?;
    Ok(())
}

async fn build_model(loaded: &LoadedConfig) -> CliResult<ConfiguredModel> {
    match &loaded.config.model {
        ServiceModelConfig::Demo => Ok(ConfiguredModel {
            id: "local/demo".to_owned(),
            origin: CapabilityOrigin::BuiltIn,
            model: Arc::new(DemoModel),
            demo_tools: true,
        }),
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

const fn default_connect_timeout_ms() -> u64 {
    10_000
}

const fn default_max_response_bytes() -> usize {
    2_097_152
}

const fn default_max_concurrency() -> usize {
    16
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
                r#"{"schema_version":1,"data_directory":".y-harness","model":{"type":"demo"},"extra":true}"#
            )
            .is_err()
        );
        assert!(resolve_data_directory(Path::new("/project"), "../outside").is_err());
        assert!(resolve_data_directory(Path::new("/project"), "/outside").is_err());
        assert!(resolve_data_directory(Path::new("/project"), ".").is_err());
    }
}
