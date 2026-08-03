//! Bounded adapter for SWE-agent's released single-instance trajectory format.

use super::*;

const ADAPTER_VERSION: &str = "swe-agent-traj-v1";
const RUN_FORMAT_VERSION: u32 = 10;
const MAX_MODEL_CALLS: u64 = 512;
const MAX_TRAJECTORY_STEPS: usize = 512;
const MAX_HISTORY_ITEMS: usize = 4_096;
const MAX_TRAJECTORY_BYTES: u64 = 2_097_152;
const OWNED_ENVIRONMENT: [&str; 13] = [
    "HOME",
    "USERPROFILE",
    "TMPDIR",
    "PYTHONPATH",
    "PYTHONNOUSERSITE",
    "PYTHONDONTWRITEBYTECODE",
    "SWE_AGENT_CONFIG_ROOT",
    "SWE_AGENT_CONFIG_DIR",
    "SWE_AGENT_TOOLS_DIR",
    "SWE_AGENT_TRAJECTORY_DIR",
    "SWE_AGENT_ENV_VAR_PATH",
    "SWE_AGENT_OUTPUT_DIR",
    "SWE_AGENT_CONFIG",
];

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    Bare,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RunSpec {
    format_version: u32,
    run_id: String,
    benchmark_version: String,
    case_id: String,
    program: PathBuf,
    expected_cli_version: String,
    expected_product_executable_sha256: String,
    expected_source_commit: String,
    expected_swe_rex_version: String,
    source_root: PathBuf,
    config: PathBuf,
    expected_config_sha256: String,
    workspace: PathBuf,
    workspace_snapshot: String,
    profile: Profile,
    provider: String,
    provider_base_url: String,
    model: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    max_budget_usd: f64,
    max_model_calls: u64,
    inherit_environment: Vec<String>,
    home: PathBuf,
    output_dir: PathBuf,
    temp_dir: PathBuf,
}

struct NormalizedResult {
    is_error: bool,
    cli_version: String,
    subtype: String,
    num_turns: u64,
    raw: Value,
}

struct PrivateFile {
    path: PathBuf,
}

impl Drop for PrivateFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid SWE-agent run spec: {error}"))?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &RunSpec) -> AppResult<()> {
    validate_common_spec(
        spec.format_version,
        RUN_FORMAT_VERSION,
        &spec.run_id,
        &spec.benchmark_version,
        &spec.case_id,
        &spec.expected_cli_version,
        &spec.expected_product_executable_sha256,
        &spec.program,
        &spec.workspace,
        &spec.workspace_snapshot,
        &spec.model,
        &spec.system_prompt,
        &spec.prompt,
        spec.timeout_ms,
        &spec.inherit_environment,
    )?;
    validate_path_component("case_id", &spec.case_id)?;
    validate_text("SWE-agent provider", &spec.provider)?;
    validate_text("expected_swe_rex_version", &spec.expected_swe_rex_version)?;
    validate_text("SWE-agent provider_base_url", &spec.provider_base_url)?;
    validate_loopback_base_url(&spec.provider_base_url)?;
    if !is_lower_git_commit(&spec.expected_source_commit) {
        return Err("expected_source_commit must be 40 lowercase hexadecimal bytes".to_owned());
    }
    if !is_lower_sha256(&spec.expected_config_sha256) {
        return Err("expected_config_sha256 must be 64 lowercase hexadecimal bytes".to_owned());
    }
    if !spec.source_root.is_absolute()
        || !spec.config.is_absolute()
        || !spec.home.is_absolute()
        || !spec.output_dir.is_absolute()
        || !spec.temp_dir.is_absolute()
    {
        return Err(
            "source_root, config, home, output_dir, and temp_dir must be absolute paths".to_owned(),
        );
    }
    if !spec.max_budget_usd.is_finite()
        || spec.max_budget_usd < 0.0
        || spec.max_budget_usd > MAX_BUDGET_USD
    {
        return Err(format!(
            "max_budget_usd must be >= 0 and <= {MAX_BUDGET_USD}"
        ));
    }
    if !(1..=MAX_MODEL_CALLS).contains(&spec.max_model_calls) {
        return Err(format!("max_model_calls must be 1-{MAX_MODEL_CALLS}"));
    }
    let inherited = spec
        .inherit_environment
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if OWNED_ENVIRONMENT
        .iter()
        .any(|name| inherited.contains(name))
        || inherited
            .iter()
            .any(|name| name.starts_with("SWE_AGENT_") || name.starts_with("PYTHON"))
    {
        return Err("SWE-agent adapter-owned environment must not be inherited".to_owned());
    }
    Ok(())
}

pub(super) async fn execute(spec: RunSpec) -> AppResult<ExternalRunReport> {
    let program = canonical_file(&spec.program, "program")?;
    let workspace = canonical_directory(&spec.workspace, "workspace")?;
    let source_root = canonical_directory(&spec.source_root, "source_root")?;
    let config = canonical_file(&spec.config, "SWE-agent config")?;
    if !config.starts_with(&source_root) {
        return Err("SWE-agent config must resolve inside source_root".to_owned());
    }
    let config_dir =
        canonical_directory(&source_root.join("config"), "SWE-agent config directory")?;
    let tools_dir = canonical_directory(&source_root.join("tools"), "SWE-agent tools directory")?;
    let _validated_package_dir =
        canonical_directory(&source_root.join("sweagent"), "SWE-agent package directory")?;
    let home = canonical_empty_directory(&spec.home, "SWE-agent home")?;
    let output_dir = canonical_empty_directory(&spec.output_dir, "SWE-agent output directory")?;
    let temp_dir = canonical_empty_directory(&spec.temp_dir, "SWE-agent temp directory")?;
    validate_path_boundaries(&workspace, &source_root, &home, &output_dir, &temp_dir)?;

    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "SWE-agent executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let config_sha256 = sha256_file(&config)?;
    if config_sha256 != spec.expected_config_sha256 {
        return Err(format!(
            "SWE-agent config digest mismatch: expected {}, observed {}",
            spec.expected_config_sha256, config_sha256
        ));
    }

    let prompt_file = write_private_file(&temp_dir, "problem.md", spec.prompt.as_bytes())?;
    let env_file = write_private_file(&temp_dir, "empty.env", b"")?;
    let mut environment = inherited_environment(&spec.inherit_environment)?;
    prepare_environment(
        &source_root,
        &config_dir,
        &tools_dir,
        &output_dir,
        &home,
        &temp_dir,
        &mut environment,
    )?;

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let adapter_executable_sha256 = env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark adapter executable: {error}"))
        .and_then(|path| sha256_file(&path))?;
    let prompt_sha256 = sha256_bytes(spec.prompt.as_bytes());
    let system_prompt_sha256 = sha256_bytes(spec.system_prompt.as_bytes());
    let started_at_ms = now_ms();
    let started = Instant::now();
    let request = ProcessRequest {
        program,
        args: arguments(
            &spec,
            &config,
            &workspace,
            &output_dir,
            &prompt_file.path,
            &env_file.path,
        )?,
        current_dir: workspace,
        environment,
        secret_environment: BTreeMap::new(),
        stdin: Vec::new(),
        timeout: Duration::from_millis(spec.timeout_ms),
        max_output_bytes: MAX_OUTPUT_BYTES,
        cancellation_phase: ExecutionPhase::Model,
    };
    let process = broker.execute(request, CancellationToken::new()).await;
    let wall_time_ms = elapsed_ms(started);
    let isolation = broker.descriptor().isolation;

    let mut cli_version = "unobserved".to_owned();
    let execution = match process {
        Ok(output) => {
            let stdout_sha256 = sha256_bytes(&output.stdout);
            let stderr_sha256 = sha256_bytes(&output.stderr);
            if output.stdout_truncated || output.stderr_truncated {
                RunExecution::AdapterError {
                    wall_time_ms,
                    message: "SWE-agent output exceeded the adapter retention bound".to_owned(),
                    process: Some(failed_process_evidence(
                        &output,
                        stdout_sha256,
                        stderr_sha256,
                    )),
                }
            } else {
                let trajectory_path = output_dir
                    .join(&spec.case_id)
                    .join(format!("{}.traj", spec.case_id));
                match read_trajectory(&trajectory_path, &output_dir).and_then(|value| {
                    normalize_result(
                        value,
                        &spec.expected_cli_version,
                        &spec.expected_source_commit,
                        &spec.expected_swe_rex_version,
                        spec.max_model_calls,
                    )
                }) {
                    Ok(normalized) => {
                        cli_version = normalized.cli_version;
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: None,
                            product_api_duration_ms: None,
                            num_turns: normalized.num_turns,
                            actual_cost_usd: None,
                            actual_cost_usd_ticks: None,
                            result_subtype: Some(normalized.subtype),
                            stdout_bytes: output.stdout.len(),
                            stdout_sha256,
                            stderr_bytes: output.stderr.len(),
                            stderr_sha256,
                            raw_result: normalized.raw,
                        };
                        if output.success && !normalized.is_error {
                            RunExecution::Completed { settlement }
                        } else {
                            RunExecution::ProductError { settlement }
                        }
                    }
                    Err(message) => RunExecution::AdapterError {
                        wall_time_ms,
                        message,
                        process: Some(failed_process_evidence(
                            &output,
                            stdout_sha256,
                            stderr_sha256,
                        )),
                    },
                }
            }
        }
        Err(error) => RunExecution::AdapterError {
            wall_time_ms,
            message: bounded_error(&error.to_string()),
            process: None,
        },
    };

    let unsupported_controls = vec![
        "adapter conformance is not a Harness-effect or product-quality result",
        "no cross-product model, tool, prompt, or container parity has been established",
        "SWE-agent source commit does not attest that source_root has a clean working tree",
        "the selected config digest does not transitively attest every referenced tool bundle",
        "SWE-ReX deployment and container identity are configured by the product but not settled by .traj",
        "the command blocklist is an Agent retry mechanism, not an approval or authorization boundary",
        "SWE-agent call and cost limits are checked after a Model call and are not pre-call hard fences",
        "SWE-agent .traj is a rewritten post-step artifact, not a durable effect journal",
        "requested Provider and Model identity are not independently settled by .traj",
        "product/API durations and actual Provider spend are unavailable",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values, launcher dependencies, and external container state are not recorded",
    ];

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "swe-agent",
            cli_version,
            adapter_executable_sha256,
            product_executable_sha256,
        },
        coordinate: RunCoordinate {
            run_id: spec.run_id,
            benchmark_version: spec.benchmark_version,
            case_id: spec.case_id,
            workspace_snapshot: spec.workspace_snapshot,
            started_at_ms,
            host_os: env::consts::OS,
            host_arch: env::consts::ARCH,
        },
        controls: RunControls {
            track: "adapter_conformance",
            claim_eligible: false,
            profile: match spec.profile {
                Profile::Bare => "bare",
            },
            requested_provider: Some(spec.provider),
            requested_model: spec.model,
            observed_models: Vec::new(),
            prompt_sha256,
            system_prompt_sha256,
            tools: "swe-agent-aci-config",
            permission_mode: "command_blocklist",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: (spec.max_budget_usd > 0.0).then_some(spec.max_budget_usd),
            requested_reasoning_effort: None,
            requested_max_turns: Some(spec.max_model_calls),
            product_sandbox: None,
            unsupported_controls,
        },
        execution,
    })
}

fn arguments(
    spec: &RunSpec,
    config: &Path,
    workspace: &Path,
    output_dir: &Path,
    prompt_file: &Path,
    env_file: &Path,
) -> AppResult<Vec<String>> {
    Ok(vec![
        "run".to_owned(),
        format!("--config={}", utf8_path(config, "config")?),
        format!("--agent.model.name={}", spec.model),
        format!("--agent.model.api_base={}", spec.provider_base_url),
        format!(
            "--agent.model.per_instance_call_limit={}",
            spec.max_model_calls
        ),
        format!(
            "--agent.model.per_instance_cost_limit={}",
            spec.max_budget_usd
        ),
        "--agent.model.total_cost_limit=0".to_owned(),
        format!("--agent.templates.system_template={}", spec.system_prompt),
        format!("--env.repo.path={}", utf8_path(workspace, "workspace")?),
        format!(
            "--problem_statement.path={}",
            utf8_path(prompt_file, "problem prompt")?
        ),
        format!("--problem_statement.id={}", spec.case_id),
        format!("--output_dir={}", utf8_path(output_dir, "output_dir")?),
        format!("--env_var_path={}", utf8_path(env_file, "empty env file")?),
        "--actions.open_pr=false".to_owned(),
        "--actions.apply_patch_locally=false".to_owned(),
    ])
}

#[allow(clippy::too_many_arguments)]
fn prepare_environment(
    source_root: &Path,
    config_dir: &Path,
    tools_dir: &Path,
    output_dir: &Path,
    home: &Path,
    temp_dir: &Path,
    environment: &mut BTreeMap<String, String>,
) -> AppResult<()> {
    for (name, path) in [
        ("HOME", home),
        ("USERPROFILE", home),
        ("TMPDIR", temp_dir),
        ("PYTHONPATH", source_root),
        ("SWE_AGENT_CONFIG_ROOT", source_root),
        ("SWE_AGENT_CONFIG_DIR", config_dir),
        ("SWE_AGENT_TOOLS_DIR", tools_dir),
        ("SWE_AGENT_TRAJECTORY_DIR", output_dir),
    ] {
        environment.insert(name.to_owned(), utf8_path(path, name)?.to_owned());
    }
    environment.insert("PYTHONNOUSERSITE".to_owned(), "1".to_owned());
    environment.insert("PYTHONDONTWRITEBYTECODE".to_owned(), "1".to_owned());
    Ok(())
}

fn normalize_result(
    value: Value,
    expected_cli_version: &str,
    expected_source_commit: &str,
    expected_swe_rex_version: &str,
    requested_model_calls: u64,
) -> AppResult<NormalizedResult> {
    let object = value
        .as_object()
        .ok_or_else(|| "SWE-agent trajectory must be a JSON object".to_owned())?;
    let trajectory = object
        .get("trajectory")
        .and_then(Value::as_array)
        .ok_or_else(|| "SWE-agent trajectory has no trajectory array".to_owned())?;
    if trajectory.len() > MAX_TRAJECTORY_STEPS {
        return Err(format!(
            "SWE-agent trajectory exceeds {MAX_TRAJECTORY_STEPS} steps"
        ));
    }
    let history = object
        .get("history")
        .and_then(Value::as_array)
        .ok_or_else(|| "SWE-agent trajectory has no history array".to_owned())?;
    if history.len() > MAX_HISTORY_ITEMS || history.iter().any(|item| !item.is_object()) {
        return Err(format!(
            "SWE-agent history must contain at most {MAX_HISTORY_ITEMS} objects"
        ));
    }
    for step in trajectory {
        validate_trajectory_step(step)?;
    }
    if !object.get("environment").is_some_and(Value::is_string) {
        return Err("SWE-agent trajectory has no string environment".to_owned());
    }
    if object
        .get("replay_config")
        .is_some_and(|item| !item.is_null() && !item.is_string())
    {
        return Err("SWE-agent replay_config must be a string or null".to_owned());
    }

    let info = object
        .get("info")
        .and_then(Value::as_object)
        .ok_or_else(|| "SWE-agent trajectory has no info object".to_owned())?;
    let cli_version = required_bounded_string(info.get("swe_agent_version"), "swe_agent_version")?;
    if cli_version != expected_cli_version {
        return Err(format!(
            "SWE-agent version mismatch: expected {expected_cli_version:?}, observed {cli_version:?}"
        ));
    }
    let source_commit = required_bounded_string(info.get("swe_agent_hash"), "swe_agent_hash")?;
    if source_commit != expected_source_commit {
        return Err(format!(
            "SWE-agent source commit mismatch: expected {expected_source_commit}, observed {source_commit}"
        ));
    }
    let swe_rex_version = required_bounded_string(info.get("swe_rex_version"), "swe_rex_version")?;
    if swe_rex_version != expected_swe_rex_version {
        return Err(format!(
            "SWE-ReX version mismatch: expected {expected_swe_rex_version:?}, observed {swe_rex_version:?}"
        ));
    }
    let subtype = required_bounded_string(info.get("exit_status"), "exit_status")?;
    let submission = info
        .get("submission")
        .ok_or_else(|| "SWE-agent info has no submission field".to_owned())?;
    if !submission.is_null()
        && submission
            .as_str()
            .is_none_or(|text| text.len() > MAX_PROMPT_BYTES)
    {
        return Err("SWE-agent submission must be null or bounded text".to_owned());
    }
    let model_stats = info
        .get("model_stats")
        .and_then(Value::as_object)
        .ok_or_else(|| "SWE-agent info has no model_stats object".to_owned())?;
    let api_calls = required_u64_field(model_stats.get("api_calls"), "api_calls")?;
    let allowed_calls = requested_model_calls.saturating_add(1);
    if api_calls > allowed_calls {
        return Err(format!(
            "SWE-agent api_calls exceeds the post-call limit bound {allowed_calls}"
        ));
    }
    let _ = required_u64_field(model_stats.get("tokens_sent"), "tokens_sent")?;
    let _ = required_u64_field(model_stats.get("tokens_received"), "tokens_received")?;
    let _ = required_nonnegative_number(model_stats.get("instance_cost"), "instance_cost")?;

    let submitted = subtype == "submitted" || subtype.starts_with("submitted (");
    let has_submission = submission
        .as_str()
        .is_some_and(|text| !text.trim().is_empty());
    Ok(NormalizedResult {
        is_error: !submitted || !has_submission,
        cli_version,
        subtype,
        num_turns: u64::try_from(trajectory.len()).unwrap_or(u64::MAX),
        raw: value,
    })
}

fn validate_trajectory_step(value: &Value) -> AppResult<()> {
    let object = value
        .as_object()
        .ok_or_else(|| "SWE-agent trajectory step must be an object".to_owned())?;
    for field in ["action", "observation", "response", "thought"] {
        if !object.get(field).is_some_and(Value::is_string) {
            return Err(format!("SWE-agent trajectory step has no string {field}"));
        }
    }
    if !object.get("query").is_some_and(Value::is_array)
        || !object.get("state").is_some_and(Value::is_object)
        || !object.get("extra_info").is_some_and(Value::is_object)
    {
        return Err("SWE-agent trajectory step has invalid query/state/extra_info".to_owned());
    }
    let execution_time = object
        .get("execution_time")
        .and_then(Value::as_f64)
        .ok_or_else(|| "SWE-agent trajectory step has no numeric execution_time".to_owned())?;
    if !execution_time.is_finite() || execution_time < 0.0 {
        return Err("SWE-agent execution_time must be finite and nonnegative".to_owned());
    }
    Ok(())
}

fn read_trajectory(path: &Path, output_dir: &Path) -> AppResult<Value> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| format!("cannot resolve SWE-agent trajectory: {error}"))?;
    if !canonical.starts_with(output_dir) {
        return Err("SWE-agent trajectory resolves outside output_dir".to_owned());
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect SWE-agent trajectory: {error}"))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_TRAJECTORY_BYTES {
        return Err(format!(
            "SWE-agent trajectory must be a regular file no larger than {MAX_TRAJECTORY_BYTES} bytes"
        ));
    }
    let bytes =
        fs::read(path).map_err(|error| format!("cannot read SWE-agent trajectory: {error}"))?;
    if bytes.is_empty() || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_TRAJECTORY_BYTES {
        return Err("SWE-agent trajectory is empty or exceeds its bound".to_owned());
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("SWE-agent trajectory is not valid JSON: {error}"))
}

fn write_private_file(directory: &Path, name: &str, bytes: &[u8]) -> AppResult<PrivateFile> {
    let path = directory.join(format!(
        ".yh-bench-swe-agent-{}-{}-{name}",
        std::process::id(),
        now_ms()
    ));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| format!("cannot create private SWE-agent {name}: {error}"))?;
    file.write_all(bytes)
        .and_then(|()| file.flush())
        .map_err(|error| format!("cannot write private SWE-agent {name}: {error}"))?;
    Ok(PrivateFile { path })
}

fn validate_path_boundaries(
    workspace: &Path,
    source_root: &Path,
    home: &Path,
    output_dir: &Path,
    temp_dir: &Path,
) -> AppResult<()> {
    let paths = [workspace, source_root, home, output_dir, temp_dir];
    for (index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(index + 1) {
            if paths_overlap(left, right) {
                return Err(
                    "workspace, source_root, home, output_dir, and temp_dir must be pairwise disjoint"
                        .to_owned(),
                );
            }
        }
    }
    Ok(())
}

fn canonical_file(path: &Path, kind: &str) -> AppResult<PathBuf> {
    let path =
        fs::canonicalize(path).map_err(|error| format!("cannot canonicalize {kind}: {error}"))?;
    if !path.is_file() {
        return Err(format!("{kind} must resolve to a regular file"));
    }
    Ok(path)
}

fn canonical_directory(path: &Path, kind: &str) -> AppResult<PathBuf> {
    let path =
        fs::canonicalize(path).map_err(|error| format!("cannot canonicalize {kind}: {error}"))?;
    if !path.is_dir() {
        return Err(format!("{kind} must resolve to a directory"));
    }
    Ok(path)
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn utf8_path<'a>(path: &'a Path, kind: &str) -> AppResult<&'a str> {
    path.to_str()
        .ok_or_else(|| format!("{kind} must be valid UTF-8 for the SWE-agent CLI"))
}

fn validate_path_component(kind: &str, value: &str) -> AppResult<()> {
    if value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(format!("{kind} must be one safe ASCII path component"));
    }
    Ok(())
}

fn validate_loopback_base_url(value: &str) -> AppResult<()> {
    let authority_and_path = value
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| "SWE-agent provider_base_url must use http://127.0.0.1:<port>".to_owned())?;
    let (port, path) = authority_and_path
        .split_once('/')
        .map_or((authority_and_path, ""), |(port, path)| (port, path));
    let port = port
        .parse::<u16>()
        .map_err(|_| "SWE-agent provider_base_url must contain a valid loopback port".to_owned())?;
    if port == 0
        || !matches!(path, "" | "v1")
        || path.contains('?')
        || path.contains('#')
        || value.ends_with('/') && path.is_empty()
    {
        return Err("SWE-agent provider_base_url is not a bounded loopback URL".to_owned());
    }
    Ok(())
}

fn is_lower_git_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn required_bounded_string(value: Option<&Value>, kind: &str) -> AppResult<String> {
    let value = value
        .and_then(Value::as_str)
        .ok_or_else(|| format!("SWE-agent {kind} must be a string"))?;
    validate_text(kind, value)?;
    Ok(value.to_owned())
}

fn required_u64_field(value: Option<&Value>, kind: &str) -> AppResult<u64> {
    value
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("SWE-agent {kind} must be an unsigned integer"))
}

fn required_nonnegative_number(value: Option<&Value>, kind: &str) -> AppResult<f64> {
    let value = value
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("SWE-agent {kind} must be numeric"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("SWE-agent {kind} must be finite and nonnegative"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn absolute_path(name: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(format!(r"C:\{name}"))
        } else {
            PathBuf::from(format!("/{name}"))
        }
    }

    fn valid_spec() -> RunSpec {
        RunSpec {
            format_version: RUN_FORMAT_VERSION,
            run_id: "swe-agent-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "issue-1".to_owned(),
            program: absolute_path("sweagent"),
            expected_cli_version: "1.1.0".to_owned(),
            expected_product_executable_sha256: "a".repeat(64),
            expected_source_commit: "b".repeat(40),
            expected_swe_rex_version: "1.2.1".to_owned(),
            source_root: absolute_path("SWE-agent"),
            config: absolute_path("SWE-agent/config/default.yaml"),
            expected_config_sha256: "c".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "repo-fixture-v1".to_owned(),
            profile: Profile::Bare,
            provider: "loopback-openai-compatible".to_owned(),
            provider_base_url: "http://127.0.0.1:12345/v1".to_owned(),
            model: "openai/test-model".to_owned(),
            system_prompt: "Solve the issue with the available tools.".to_owned(),
            prompt: "Fix issue 1.".to_owned(),
            timeout_ms: 30_000,
            max_budget_usd: 1.0,
            max_model_calls: 8,
            inherit_environment: vec!["OPENAI_API_KEY".to_owned()],
            home: absolute_path("home"),
            output_dir: absolute_path("output"),
            temp_dir: absolute_path("temp"),
        }
    }

    fn valid_trajectory() -> Value {
        serde_json::json!({
            "trajectory": [{
                "action": "submit",
                "observation": "diff --git a/a b/a",
                "response": "done",
                "thought": "fixed",
                "execution_time": 0.25,
                "state": {},
                "query": [],
                "extra_info": {}
            }],
            "history": [{"role": "system", "content": "prompt", "message_type": "system_prompt"}],
            "info": {
                "model_stats": {
                    "instance_cost": 0.01,
                    "tokens_sent": 10,
                    "tokens_received": 5,
                    "api_calls": 1
                },
                "exit_status": "submitted",
                "submission": "diff --git a/a b/a",
                "swe_agent_hash": "b".repeat(40),
                "swe_agent_version": "1.1.0",
                "swe_rex_version": "1.2.1"
            },
            "replay_config": "{}",
            "environment": "docker"
        })
    }

    #[test]
    fn spec_requires_loopback_and_safe_output_component() {
        let mut spec = valid_spec();
        validate_spec(&spec).expect("valid spec");
        spec.provider_base_url = "https://provider.example/v1".to_owned();
        assert!(validate_spec(&spec).is_err());
        spec.provider_base_url = "http://127.0.0.1:12345/v1".to_owned();
        spec.case_id = "../escape".to_owned();
        assert!(validate_spec(&spec).is_err());
        spec.case_id = "issue-1".to_owned();
        spec.inherit_environment = vec!["SWE_AGENT_AGENT".to_owned()];
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn arguments_keep_prompt_off_argv_and_fix_product_actions() {
        let spec = valid_spec();
        let prompt_file = absolute_path("temp/problem.md");
        let args = arguments(
            &spec,
            &spec.config,
            &spec.workspace,
            &spec.output_dir,
            &prompt_file,
            &absolute_path("temp/empty.env"),
        )
        .expect("arguments");
        assert!(!args.iter().any(|argument| argument.contains(&spec.prompt)));
        assert!(args.contains(&"--actions.open_pr=false".to_owned()));
        assert!(args.contains(&"--actions.apply_patch_locally=false".to_owned()));
        assert!(args.contains(&"--agent.model.per_instance_call_limit=8".to_owned()));
        assert!(args.contains(&format!(
            "--problem_statement.path={}",
            prompt_file.display()
        )));
    }

    #[test]
    fn trajectory_settles_version_commit_steps_and_submission() {
        let normalized = normalize_result(valid_trajectory(), "1.1.0", &"b".repeat(40), "1.2.1", 8)
            .expect("valid trajectory");
        assert!(!normalized.is_error);
        assert_eq!(normalized.cli_version, "1.1.0");
        assert_eq!(normalized.subtype, "submitted");
        assert_eq!(normalized.num_turns, 1);
    }

    #[test]
    fn trajectory_rejects_wrong_source_or_excess_calls() {
        assert!(
            normalize_result(valid_trajectory(), "1.1.0", &"d".repeat(40), "1.2.1", 8).is_err()
        );
        assert!(
            normalize_result(valid_trajectory(), "1.1.0", &"b".repeat(40), "1.2.0", 8).is_err()
        );
        let mut trajectory = valid_trajectory();
        trajectory["info"]["model_stats"]["api_calls"] = serde_json::json!(10);
        assert!(normalize_result(trajectory, "1.1.0", &"b".repeat(40), "1.2.1", 8).is_err());
    }

    #[test]
    fn non_submission_is_a_product_error_not_an_adapter_error() {
        let mut trajectory = valid_trajectory();
        trajectory["info"]["exit_status"] = serde_json::json!("exit_cost");
        trajectory["info"]["submission"] = Value::Null;
        let normalized = normalize_result(trajectory, "1.1.0", &"b".repeat(40), "1.2.1", 8)
            .expect("valid failed trajectory");
        assert!(normalized.is_error);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_broker_run_settles_the_exact_trajectory_file() {
        use std::os::unix::fs::PermissionsExt;

        let root = env::temp_dir().join(format!(
            "yh-bench-swe-agent-test-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let source_root = root.join("source");
        let config = source_root.join("config/default.yaml");
        let workspace = root.join("workspace");
        let home = root.join("home");
        let output_dir = root.join("output");
        let temp_dir = root.join("temp");
        for directory in [
            source_root.join("config"),
            source_root.join("tools"),
            source_root.join("sweagent"),
            workspace.clone(),
            home.clone(),
            output_dir.clone(),
            temp_dir.clone(),
        ] {
            fs::create_dir_all(directory).expect("create test directory");
        }
        fs::write(&config, b"agent: {}\n").expect("write config");

        let program = root.join("fake-sweagent");
        fs::write(
            &program,
            format!(
                r#"#!/bin/sh
output=
case_id=
for argument in "$@"; do
  case "$argument" in
    --output_dir=*) output="${{argument#*=}}" ;;
    --problem_statement.id=*) case_id="${{argument#*=}}" ;;
  esac
done
/bin/mkdir -p "$output/$case_id"
printf '%s' '{}' > "$output/$case_id/$case_id.traj"
"#,
                valid_trajectory()
            ),
        )
        .expect("write fake SWE-agent");
        let mut permissions = fs::metadata(&program)
            .expect("program metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&program, permissions).expect("make program executable");

        let mut spec = valid_spec();
        spec.program = program.clone();
        spec.expected_product_executable_sha256 = sha256_file(&program).expect("program digest");
        spec.source_root = source_root;
        spec.config = config.clone();
        spec.expected_config_sha256 = sha256_file(&config).expect("config digest");
        spec.workspace = workspace;
        spec.home = home;
        spec.output_dir = output_dir;
        spec.temp_dir = temp_dir.clone();
        spec.inherit_environment.clear();

        let report = execute(spec).await.expect("adapter report");
        assert_eq!(report.format_version, RUN_FORMAT_VERSION);
        assert_eq!(report.adapter.cli_version, "1.1.0");
        assert!(!report.controls.claim_eligible);
        assert!(matches!(report.execution, RunExecution::Completed { .. }));
        assert!(
            fs::read_dir(&temp_dir)
                .expect("inspect temp directory")
                .next()
                .is_none()
        );

        fs::remove_dir_all(&root).expect("remove test directory");
    }
}
