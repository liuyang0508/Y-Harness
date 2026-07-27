//! Source-pinned adapter for the released Hermes Agent one-shot CLI.

use super::*;

const ADAPTER_VERSION: &str = "hermes-oneshot-usage-v1";
const RUN_FORMAT_VERSION: u32 = 6;
const MAX_HERMES_PROMPT_BYTES: usize = 16_384;
const MAX_USAGE_BYTES: u64 = 65_536;
const MAX_API_CALLS: u64 = 90;
const VERSION_PROBE_REVISION: &str = "yh-bench-offline-version-probe";
const OWNED_ENVIRONMENT: [&str; 19] = [
    "HERMES_HOME",
    "HERMES_PROFILE",
    "HERMES_CONFIG",
    "HERMES_ENV",
    "HERMES_MANAGED_DIR",
    "HERMES_SAFE_MODE",
    "HERMES_IGNORE_USER_CONFIG",
    "HERMES_IGNORE_RULES",
    "HERMES_YOLO_MODE",
    "HERMES_ACCEPT_HOOKS",
    "HERMES_INFERENCE_MODEL",
    "HERMES_INFERENCE_PROVIDER",
    "HERMES_INTERACTIVE",
    "HERMES_KANBAN_TASK",
    "HERMES_KANBAN_BOARD",
    "HERMES_BUNDLED_SKILLS",
    "HERMES_OPTIONAL_SKILLS",
    "HERMES_OPTIONAL_MCPS",
    "HERMES_REVISION",
];
const REQUIRED_USAGE_FIELDS: [&str; 16] = [
    "estimated_cost_usd",
    "cost_status",
    "cost_source",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "reasoning_tokens",
    "total_tokens",
    "api_calls",
    "model",
    "provider",
    "session_id",
    "completed",
    "failed",
    "service_tier",
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
    workspace: PathBuf,
    workspace_snapshot: String,
    profile: Profile,
    provider: String,
    model: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    hermes_home: PathBuf,
    usage_directory: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UsageReport {
    estimated_cost_usd: Option<f64>,
    cost_status: Option<String>,
    cost_source: Option<String>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    total_tokens: Option<u64>,
    api_calls: Option<u64>,
    model: Option<String>,
    provider: Option<String>,
    session_id: Option<String>,
    completed: Option<bool>,
    failed: bool,
    service_tier: Option<String>,
    failure: Option<String>,
}

struct NormalizedResult {
    is_error: bool,
    subtype: &'static str,
    num_turns: u64,
    observed_models: Vec<String>,
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
        .map_err(|error| format!("invalid Hermes Agent run spec: {error}"))?;
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
    validate_text("Hermes provider", &spec.provider)?;
    expected_version_number(&spec.expected_cli_version)?;
    if spec.prompt.len() > MAX_HERMES_PROMPT_BYTES {
        return Err(format!(
            "Hermes prompt must be no larger than {MAX_HERMES_PROMPT_BYTES} bytes because the released CLI accepts it only as a process argument"
        ));
    }
    if spec.prompt.contains('\0') {
        return Err("Hermes prompt must not contain NUL".to_owned());
    }
    if !spec.hermes_home.is_absolute() || !spec.usage_directory.is_absolute() {
        return Err("hermes_home and usage_directory must be absolute paths".to_owned());
    }
    let inherited = spec
        .inherit_environment
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if OWNED_ENVIRONMENT
        .iter()
        .any(|name| inherited.contains(name))
    {
        return Err("Hermes adapter-owned environment must not be inherited".to_owned());
    }
    Ok(())
}

pub(super) async fn execute(spec: RunSpec) -> AppResult<ExternalRunReport> {
    let program = fs::canonicalize(&spec.program)
        .map_err(|error| format!("cannot canonicalize program: {error}"))?;
    if !program.is_file() {
        return Err("program must resolve to a regular file".to_owned());
    }
    let workspace = fs::canonicalize(&spec.workspace)
        .map_err(|error| format!("cannot canonicalize workspace: {error}"))?;
    if !workspace.is_dir() {
        return Err("workspace must resolve to a directory".to_owned());
    }
    let hermes_home = canonical_empty_directory(&spec.hermes_home, "hermes_home")?;
    let usage_directory = canonical_empty_directory(&spec.usage_directory, "usage_directory")?;
    validate_path_boundaries(&workspace, &hermes_home, &usage_directory)?;

    let mut environment = inherited_environment(&spec.inherit_environment)?;
    prepare_environment(&hermes_home, &mut environment)?;
    let usage_file = create_private_file(&usage_directory, "usage.json")?;

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Hermes Agent executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    prepare_version_cache(&hermes_home, &spec.expected_cli_version)?;
    let cli_version = read_hermes_cli_version(&broker, &program, &workspace, &environment).await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Hermes Agent version mismatch: expected {:?}, observed {:?}",
            spec.expected_cli_version, cli_version
        ));
    }

    let adapter_executable_sha256 = env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark adapter executable: {error}"))
        .and_then(|path| sha256_file(&path))?;
    let prompt_sha256 = sha256_bytes(spec.prompt.as_bytes());
    let system_prompt_sha256 = sha256_bytes(spec.system_prompt.as_bytes());
    let started_at_ms = now_ms();
    let started = Instant::now();
    let request = ProcessRequest {
        program,
        args: arguments(&spec, &usage_file.path)?,
        current_dir: workspace,
        environment,
        stdin: Vec::new(),
        timeout: Duration::from_millis(spec.timeout_ms),
        max_output_bytes: MAX_OUTPUT_BYTES,
        cancellation_phase: ExecutionPhase::Model,
    };
    let process = broker.execute(request, CancellationToken::new()).await;
    let wall_time_ms = elapsed_ms(started);
    let isolation = broker.descriptor().isolation;

    let (execution, observed_models) = match process {
        Ok(output) => {
            let stdout_sha256 = sha256_bytes(&output.stdout);
            let stderr_sha256 = sha256_bytes(&output.stderr);
            if output.stdout_truncated || output.stderr_truncated {
                (
                    RunExecution::AdapterError {
                        wall_time_ms,
                        message: "Hermes Agent output exceeded the adapter retention bound"
                            .to_owned(),
                        process: Some(failed_process_evidence(
                            &output,
                            stdout_sha256,
                            stderr_sha256,
                        )),
                    },
                    Vec::new(),
                )
            } else {
                match read_usage(&usage_file.path)
                    .and_then(|usage| normalize_result(&output.stdout, usage, output.success))
                {
                    Ok(normalized) => {
                        let observed_models = normalized.observed_models.clone();
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: None,
                            product_api_duration_ms: None,
                            num_turns: normalized.num_turns,
                            actual_cost_usd: None,
                            actual_cost_usd_ticks: None,
                            result_subtype: Some(normalized.subtype.to_owned()),
                            stdout_bytes: output.stdout.len(),
                            stdout_sha256,
                            stderr_bytes: output.stderr.len(),
                            stderr_sha256,
                            raw_result: normalized.raw,
                        };
                        let execution = if output.success && !normalized.is_error {
                            RunExecution::Completed { settlement }
                        } else {
                            RunExecution::ProductError { settlement }
                        };
                        (execution, observed_models)
                    }
                    Err(message) => (
                        RunExecution::AdapterError {
                            wall_time_ms,
                            message,
                            process: Some(failed_process_evidence(
                                &output,
                                stdout_sha256,
                                stderr_sha256,
                            )),
                        },
                        Vec::new(),
                    ),
                }
            }
        }
        Err(error) => (
            RunExecution::AdapterError {
                wall_time_ms,
                message: bounded_error(&error.to_string()),
                process: None,
            },
            Vec::new(),
        ),
    };

    let unsupported_controls = vec![
        "adapter conformance is not a Harness-effect or product-quality result",
        "no cross-product model parity has been established",
        "Tools are disabled, so Agent-loop effectiveness is not measured",
        "Hermes Agent exposes no documented hard monetary spend ceiling",
        "the released one-shot CLI exposes no caller-selected provider-call ceiling",
        "estimated_cost_usd is not promoted to actual_cost_usd",
        "the requested system prompt is a labeled user-message prefix, not a system-role instruction",
        "the prompt and requested instruction prefix are visible in operating-system process arguments",
        "workspace instructions are not proven disabled by the released one-shot path",
        "the product persists session state inside the isolated Hermes home",
        "a source-checkout project .env may supply otherwise undeclared environment values",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values, fallback routing, and Python package dependencies are not recorded",
        "the executable digest may identify only a Python console launcher, not its installed package graph",
    ];

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "hermes-agent",
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
            observed_models,
            prompt_sha256,
            system_prompt_sha256,
            tools: "disabled",
            permission_mode: "yolo_no_tools",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: None,
            requested_reasoning_effort: None,
            requested_max_turns: None,
            product_sandbox: None,
            unsupported_controls,
        },
        execution,
    })
}

fn prepare_environment(
    hermes_home: &Path,
    environment: &mut BTreeMap<String, String>,
) -> AppResult<()> {
    let managed_disabled = hermes_home.join(".yh-managed-disabled");
    for name in OWNED_ENVIRONMENT {
        environment.insert(name.to_owned(), String::new());
    }
    environment.extend([
        (
            "HERMES_HOME".to_owned(),
            utf8_path(hermes_home, "hermes_home")?.to_owned(),
        ),
        (
            "HERMES_MANAGED_DIR".to_owned(),
            utf8_path(&managed_disabled, "managed-disabled path")?.to_owned(),
        ),
        ("HERMES_SAFE_MODE".to_owned(), "1".to_owned()),
        ("HERMES_IGNORE_USER_CONFIG".to_owned(), "1".to_owned()),
        ("HERMES_IGNORE_RULES".to_owned(), "1".to_owned()),
        ("HERMES_YOLO_MODE".to_owned(), "1".to_owned()),
        ("HERMES_ACCEPT_HOOKS".to_owned(), "1".to_owned()),
    ]);
    create_empty_env_file(hermes_home)?;
    Ok(())
}

fn expected_version_number(expected: &str) -> AppResult<&str> {
    let version = expected
        .strip_prefix("Hermes Agent v")
        .and_then(|value| value.split_once(" ("))
        .filter(|(version, date_and_suffix)| {
            let Some((date, suffix)) = date_and_suffix.split_once(')') else {
                return false;
            };
            !version.is_empty()
                && !date.is_empty()
                && (suffix.is_empty()
                    || suffix
                        .strip_prefix(" · ")
                        .is_some_and(|revision| !revision.is_empty()))
        })
        .map(|(version, _)| version)
        .ok_or_else(|| {
            "expected_cli_version must use Hermes Agent v<version> (<release-date>) with an optional Hermes revision suffix".to_owned()
        })?;
    validate_text("Hermes expected version", version)?;
    Ok(version)
}

fn prepare_version_cache(hermes_home: &Path, expected: &str) -> AppResult<()> {
    let version = expected_version_number(expected)?;
    let path = hermes_home.join(".update_check");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| format!("cannot create Hermes offline version cache: {error}"))?;
    serde_json::to_writer(
        &mut file,
        &serde_json::json!({
            "ts": now_ms() / 1_000,
            "behind": 0,
            "rev": VERSION_PROBE_REVISION,
            "ver": version,
        }),
    )
    .map_err(|error| format!("cannot write Hermes offline version cache: {error}"))?;
    file.write_all(b"\n")
        .and_then(|()| file.flush())
        .map_err(|error| format!("cannot flush Hermes offline version cache: {error}"))
}

async fn read_hermes_cli_version(
    broker: &LocalProcessBroker,
    program: &Path,
    workspace: &Path,
    environment: &BTreeMap<String, String>,
) -> AppResult<String> {
    let mut probe_environment = environment.clone();
    probe_environment.insert(
        "HERMES_REVISION".to_owned(),
        VERSION_PROBE_REVISION.to_owned(),
    );
    for name in ["ALL_PROXY", "HTTP_PROXY", "HTTPS_PROXY"] {
        probe_environment.insert(name.to_owned(), "http://127.0.0.1:9".to_owned());
    }
    let output = broker
        .execute(
            ProcessRequest {
                program: program.to_path_buf(),
                args: vec!["--version".to_owned()],
                current_dir: workspace.to_path_buf(),
                environment: probe_environment,
                stdin: Vec::new(),
                timeout: Duration::from_secs(10),
                max_output_bytes: MAX_USAGE_BYTES as usize,
                cancellation_phase: ExecutionPhase::Model,
            },
            CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("Hermes Agent version probe failed: {error}"))?;
    if !output.success || output.stdout_truncated || output.stderr_truncated {
        return Err("Hermes Agent version probe did not settle successfully".to_owned());
    }
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|_| "Hermes Agent version output is not UTF-8".to_owned())?;
    let version = stdout
        .lines()
        .next()
        .ok_or_else(|| "Hermes Agent version output is empty".to_owned())?
        .trim()
        .to_owned();
    validate_text("observed CLI version", &version)?;
    Ok(version)
}

fn arguments(spec: &RunSpec, usage_file: &Path) -> AppResult<Vec<String>> {
    let prompt = format!(
        "[Y-Harness benchmark instruction]\n{}\n\n[Y-Harness benchmark user request]\n{}",
        spec.system_prompt, spec.prompt
    );
    Ok(vec![
        "--safe-mode".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--provider".to_owned(),
        spec.provider.clone(),
        "--toolsets".to_owned(),
        "context_engine".to_owned(),
        "--usage-file".to_owned(),
        utf8_path(usage_file, "usage_file")?.to_owned(),
        "--oneshot".to_owned(),
        prompt,
    ])
}

fn create_empty_env_file(hermes_home: &Path) -> AppResult<()> {
    let path = hermes_home.join(".env");
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .map(|_| ())
        .map_err(|error| format!("cannot create isolated Hermes .env: {error}"))
}

fn create_private_file(directory: &Path, suffix: &str) -> AppResult<PrivateFile> {
    for attempt in 0..32_u8 {
        let path = directory.join(format!(
            ".yh-bench-hermes-{}-{}-{attempt}.{suffix}",
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
        match options.open(&path) {
            Ok(_) => return Ok(PrivateFile { path }),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(format!("cannot create private Hermes usage file: {error}")),
        }
    }
    Err("cannot allocate a unique private Hermes usage file".to_owned())
}

fn validate_path_boundaries(
    workspace: &Path,
    hermes_home: &Path,
    usage_directory: &Path,
) -> AppResult<()> {
    if paths_overlap(workspace, hermes_home)
        || paths_overlap(workspace, usage_directory)
        || paths_overlap(hermes_home, usage_directory)
    {
        return Err(
            "workspace, hermes_home, and usage_directory must be pairwise disjoint".to_owned(),
        );
    }
    Ok(())
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn utf8_path<'a>(path: &'a Path, kind: &str) -> AppResult<&'a str> {
    path.to_str()
        .ok_or_else(|| format!("{kind} must be valid UTF-8 for the Hermes Agent CLI"))
}

fn read_usage(path: &Path) -> AppResult<Value> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("cannot inspect Hermes usage report: {error}"))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_USAGE_BYTES {
        return Err(format!(
            "Hermes usage report must be a regular file no larger than {MAX_USAGE_BYTES} bytes"
        ));
    }
    let bytes =
        fs::read(path).map_err(|error| format!("cannot read Hermes usage report: {error}"))?;
    if bytes.is_empty() || u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_USAGE_BYTES {
        return Err("Hermes usage report is empty or exceeds its bound".to_owned());
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("Hermes usage report is not valid JSON: {error}"))
}

fn normalize_result(
    stdout: &[u8],
    usage_value: Value,
    process_success: bool,
) -> AppResult<NormalizedResult> {
    let object = usage_value
        .as_object()
        .ok_or_else(|| "Hermes usage report must be a JSON object".to_owned())?;
    for field in REQUIRED_USAGE_FIELDS {
        if !object.contains_key(field) {
            return Err(format!("Hermes usage report has no {field} field"));
        }
    }
    let usage: UsageReport = serde_json::from_value(usage_value.clone())
        .map_err(|error| format!("Hermes usage report violates its schema: {error}"))?;
    validate_usage(&usage)?;

    let response = std::str::from_utf8(stdout)
        .map_err(|_| "Hermes Agent stdout is not UTF-8".to_owned())?
        .to_owned();
    let completed = usage.completed.unwrap_or(false);
    if completed && usage.failed {
        return Err("Hermes usage cannot be both completed and failed".to_owned());
    }
    if !process_success && completed && !usage.failed {
        return Err(
            "Hermes process failure contradicts a completed non-failed usage report".to_owned(),
        );
    }

    let is_error = !process_success || !completed || usage.failed;
    if !is_error && response.trim().is_empty() {
        return Err("successful Hermes Agent result has no text response".to_owned());
    }
    if response.len() > MAX_PROMPT_BYTES {
        return Err("Hermes Agent text response exceeds the adapter bound".to_owned());
    }
    if !is_error {
        for (field, value) in [
            ("input_tokens", usage.input_tokens),
            ("output_tokens", usage.output_tokens),
            ("cache_read_tokens", usage.cache_read_tokens),
            ("cache_write_tokens", usage.cache_write_tokens),
            ("reasoning_tokens", usage.reasoning_tokens),
            ("total_tokens", usage.total_tokens),
        ] {
            if value.is_none() {
                return Err(format!("successful Hermes usage has no {field}"));
            }
        }
    }

    let api_calls = usage.api_calls.unwrap_or(0);
    if !is_error && api_calls == 0 {
        return Err("successful Hermes usage must report at least one api_call".to_owned());
    }
    let mut observed_models = Vec::new();
    if let Some(model) = usage.model.as_ref() {
        validate_text("Hermes observed model", model)?;
        observed_models.push(model.clone());
    } else if !is_error {
        return Err("successful Hermes usage has no observed model".to_owned());
    }
    if !is_error && usage.provider.is_none() {
        return Err("successful Hermes usage has no observed provider".to_owned());
    }

    let raw = serde_json::json!({
        "response": response,
        "usage": usage_value,
    });
    Ok(NormalizedResult {
        is_error,
        subtype: if is_error { "failed" } else { "completed" },
        num_turns: u64::from(api_calls > 0),
        observed_models,
        raw,
    })
}

fn validate_usage(usage: &UsageReport) -> AppResult<()> {
    if let Some(cost) = usage.estimated_cost_usd {
        if !cost.is_finite() || cost < 0.0 {
            return Err("Hermes estimated_cost_usd must be finite and nonnegative".to_owned());
        }
    }
    let api_calls = usage.api_calls.unwrap_or(0);
    if api_calls > MAX_API_CALLS {
        return Err(format!("Hermes api_calls must be 0-{MAX_API_CALLS}"));
    }
    for (kind, value) in [
        ("cost_status", usage.cost_status.as_deref()),
        ("cost_source", usage.cost_source.as_deref()),
        ("model", usage.model.as_deref()),
        ("provider", usage.provider.as_deref()),
        ("session_id", usage.session_id.as_deref()),
        ("service_tier", usage.service_tier.as_deref()),
    ] {
        if let Some(value) = value {
            validate_text(kind, value)?;
        }
    }
    if let Some(failure) = usage.failure.as_deref() {
        if failure.is_empty() || failure.len() > MAX_SYSTEM_PROMPT_BYTES || failure.contains('\0') {
            return Err("Hermes failure text is empty or exceeds its bound".to_owned());
        }
    }
    if !usage.failed && usage.failure.is_some() {
        return Err("non-failed Hermes usage must not contain failure text".to_owned());
    }
    Ok(())
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
            run_id: "hermes-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("hermes"),
            expected_cli_version: "Hermes Agent v0.19.0 (2026.7.20)".to_owned(),
            expected_product_executable_sha256: "d".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: Profile::Bare,
            provider: "openrouter".to_owned(),
            model: "openai/gpt-5.5".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["OPENROUTER_API_KEY".to_owned()],
            hermes_home: absolute_path("hermes-home"),
            usage_directory: absolute_path("usage"),
        }
    }

    fn success_usage() -> Value {
        serde_json::json!({
            "estimated_cost_usd": 0.001,
            "cost_status": "estimated",
            "cost_source": "pricing-table",
            "input_tokens": 10,
            "output_tokens": 2,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "reasoning_tokens": 0,
            "total_tokens": 12,
            "api_calls": 1,
            "model": "openai/gpt-5.5",
            "provider": "openrouter",
            "session_id": "session-1",
            "completed": true,
            "failed": false,
            "service_tier": null
        })
    }

    #[test]
    fn bare_profile_rejects_environment_collisions_and_oversized_argv_prompt() {
        let spec = valid_spec();
        validate_spec(&spec).expect("valid Hermes spec");
        assert_eq!(
            expected_version_number("Hermes Agent v0.19.0 (2026.7.20) · upstream 3ef6bbd2")
                .expect("source-install version"),
            "0.19.0"
        );

        let mut collision = spec.clone();
        collision.inherit_environment.push("HERMES_HOME".to_owned());
        assert!(validate_spec(&collision).is_err());

        let mut oversized = spec;
        oversized.prompt = "x".repeat(MAX_HERMES_PROMPT_BYTES + 1);
        assert!(validate_spec(&oversized).is_err());

        let mut malformed_version = valid_spec();
        malformed_version.expected_cli_version = "0.19.0".to_owned();
        assert!(validate_spec(&malformed_version).is_err());
    }

    #[test]
    fn command_uses_safe_empty_tools_but_truthfully_places_prompt_in_argv() {
        let spec = valid_spec();
        let args = arguments(&spec, &absolute_path("usage.json")).expect("Hermes arguments");
        assert!(args.iter().any(|value| value == "--safe-mode"));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--toolsets", "context_engine"])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--provider", spec.provider.as_str()])
        );
        assert!(args.iter().any(|value| value.contains(&spec.prompt)));
    }

    #[test]
    fn path_boundaries_require_three_disjoint_directories() {
        let workspace = absolute_path("workspace");
        let home = absolute_path("home");
        let usage = absolute_path("usage");
        validate_path_boundaries(&workspace, &home, &usage).expect("disjoint paths");
        assert!(validate_path_boundaries(&workspace, &workspace.join("home"), &usage).is_err());
        assert!(validate_path_boundaries(&workspace, &home, &home.join("usage")).is_err());
    }

    #[test]
    fn success_preserves_observed_identity_and_estimate_without_promoting_cost() {
        let normalized =
            normalize_result(b"YH-OK\n", success_usage(), true).expect("valid Hermes result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.num_turns, 1);
        assert_eq!(normalized.observed_models, ["openai/gpt-5.5"]);
        assert_eq!(
            normalized.raw["usage"]["estimated_cost_usd"],
            serde_json::json!(0.001)
        );
    }

    #[test]
    fn usage_rejects_contradictions_missing_fields_and_excessive_calls() {
        let mut contradictory = success_usage();
        contradictory["failed"] = Value::Bool(true);
        assert!(normalize_result(b"YH-OK\n", contradictory, true).is_err());

        let mut missing = success_usage();
        missing.as_object_mut().expect("object").remove("provider");
        assert!(normalize_result(b"YH-OK\n", missing, true).is_err());

        let mut excessive = success_usage();
        excessive["api_calls"] = Value::from(MAX_API_CALLS + 1);
        assert!(normalize_result(b"YH-OK\n", excessive, true).is_err());
    }

    #[test]
    fn product_failure_remains_settled_without_inventing_a_turn() {
        let usage = serde_json::json!({
            "estimated_cost_usd": null,
            "cost_status": null,
            "cost_source": null,
            "input_tokens": null,
            "output_tokens": null,
            "cache_read_tokens": null,
            "cache_write_tokens": null,
            "reasoning_tokens": null,
            "total_tokens": null,
            "api_calls": 0,
            "model": null,
            "provider": null,
            "session_id": null,
            "completed": null,
            "failed": true,
            "service_tier": null,
            "failure": "provider unavailable"
        });
        let normalized = normalize_result(b"", usage, false).expect("settled product failure");
        assert!(normalized.is_error);
        assert_eq!(normalized.num_turns, 0);
        assert!(normalized.observed_models.is_empty());
    }
}
