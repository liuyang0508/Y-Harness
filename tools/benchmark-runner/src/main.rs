//! Released-product benchmark adapters kept outside the Harness semantic core.

mod codex;
mod grok_build;
mod pi;

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use y_harness::{
    CancellationToken, ExecutionPhase, LocalProcessBroker, ProcessBroker, ProcessIsolation,
    ProcessOutput, ProcessRequest,
};

const CLAUDE_RUN_FORMAT_VERSION: u32 = 1;
const CLAUDE_ADAPTER_VERSION: &str = "claude-code-json-v1";
const MAX_SPEC_BYTES: u64 = 2_097_152;
const MAX_OUTPUT_BYTES: usize = 2_097_152;
const MAX_PROMPT_BYTES: usize = 1_048_576;
const MAX_SYSTEM_PROMPT_BYTES: usize = 8_192;
const MAX_ID_BYTES: usize = 128;
const MAX_TEXT_FIELD_BYTES: usize = 256;
const MAX_ENVIRONMENT_NAMES: usize = 32;
const MAX_TIMEOUT_MS: u64 = 86_400_000;
const MAX_BUDGET_USD: f64 = 10_000.0;

type AppResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ClaudeProfile {
    Bare,
    Product,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClaudeRunSpec {
    format_version: u32,
    run_id: String,
    benchmark_version: String,
    case_id: String,
    program: PathBuf,
    expected_cli_version: String,
    expected_product_executable_sha256: String,
    workspace: PathBuf,
    workspace_snapshot: String,
    profile: ClaudeProfile,
    model: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    max_budget_usd: f64,
    inherit_environment: Vec<String>,
}

#[derive(Serialize)]
struct ExternalRunReport {
    format_version: u32,
    adapter: AdapterEvidence,
    coordinate: RunCoordinate,
    controls: RunControls,
    execution: RunExecution,
}

#[derive(Serialize)]
struct AdapterEvidence {
    name: &'static str,
    version: &'static str,
    product: &'static str,
    cli_version: String,
    adapter_executable_sha256: String,
    product_executable_sha256: String,
}

#[derive(Serialize)]
struct RunCoordinate {
    run_id: String,
    benchmark_version: String,
    case_id: String,
    workspace_snapshot: String,
    started_at_ms: u64,
    host_os: &'static str,
    host_arch: &'static str,
}

#[derive(Serialize)]
struct RunControls {
    track: &'static str,
    claim_eligible: bool,
    profile: &'static str,
    requested_model: String,
    observed_models: Vec<String>,
    prompt_sha256: String,
    system_prompt_sha256: String,
    tools: &'static str,
    permission_mode: &'static str,
    process_isolation: ProcessIsolation,
    inherited_environment_names: Vec<String>,
    timeout_ms: u64,
    requested_max_budget_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_max_turns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    product_sandbox: Option<&'static str>,
    unsupported_controls: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum RunExecution {
    Completed {
        settlement: ProductSettlement,
    },
    ProductError {
        settlement: ProductSettlement,
    },
    AdapterError {
        wall_time_ms: u64,
        message: String,
        process: Option<FailedProcessEvidence>,
    },
}

#[derive(Serialize)]
struct ProductSettlement {
    exit_code: Option<i32>,
    wall_time_ms: u64,
    product_duration_ms: Option<u64>,
    product_api_duration_ms: Option<u64>,
    num_turns: u64,
    actual_cost_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_cost_usd_ticks: Option<u64>,
    result_subtype: Option<String>,
    stdout_bytes: usize,
    stdout_sha256: String,
    stderr_bytes: usize,
    stderr_sha256: String,
    raw_result: Value,
}

#[derive(Serialize)]
struct FailedProcessEvidence {
    exit_code: Option<i32>,
    stdout_bytes: usize,
    stdout_sha256: String,
    stdout_truncated: bool,
    stderr_bytes: usize,
    stderr_sha256: String,
    stderr_truncated: bool,
}

struct NormalizedClaudeResult {
    is_error: bool,
    subtype: Option<String>,
    duration_ms: u64,
    duration_api_ms: u64,
    num_turns: u64,
    total_cost_usd: f64,
    observed_models: Vec<String>,
    raw: Value,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(report) => {
            let stdout = io::stdout();
            let mut output = stdout.lock();
            if serde_json::to_writer(&mut output, &report).is_err()
                || output.write_all(b"\n").is_err()
                || output.flush().is_err()
            {
                eprintln!("Error: could not write the benchmark report");
                return ExitCode::FAILURE;
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> AppResult<ExternalRunReport> {
    let mut arguments = env::args_os();
    let _ = arguments.next();
    let adapter = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(usage)?;
    let spec_path = arguments.next().map(PathBuf::from).ok_or_else(usage)?;
    if arguments.next().is_some() {
        return Err(usage());
    }
    match adapter.as_str() {
        "claude-code" => execute_claude(read_claude_spec(&spec_path)?).await,
        "codex" => codex::execute(codex::read_spec(&spec_path)?).await,
        "grok-build" => grok_build::execute(grok_build::read_spec(&spec_path)?).await,
        "pi" => pi::execute(pi::read_spec(&spec_path)?).await,
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: yh-bench <claude-code|codex|grok-build|pi> <run-spec.json>".to_owned()
}

fn read_spec_bytes(path: &Path) -> AppResult<Vec<u8>> {
    let metadata =
        fs::metadata(path).map_err(|error| format!("cannot inspect run spec: {error}"))?;
    if !metadata.is_file() || metadata.len() > MAX_SPEC_BYTES {
        return Err(format!(
            "run spec must be a file no larger than {MAX_SPEC_BYTES} bytes"
        ));
    }
    let bytes = fs::read(path).map_err(|error| format!("cannot read run spec: {error}"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_SPEC_BYTES {
        return Err(format!(
            "run spec must be a file no larger than {MAX_SPEC_BYTES} bytes"
        ));
    }
    Ok(bytes)
}

fn read_claude_spec(path: &Path) -> AppResult<ClaudeRunSpec> {
    let spec: ClaudeRunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Claude Code run spec: {error}"))?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &ClaudeRunSpec) -> AppResult<()> {
    validate_common_spec(
        spec.format_version,
        CLAUDE_RUN_FORMAT_VERSION,
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
    if !spec.max_budget_usd.is_finite()
        || spec.max_budget_usd <= 0.0
        || spec.max_budget_usd > MAX_BUDGET_USD
    {
        return Err(format!(
            "max_budget_usd must be > 0 and <= {MAX_BUDGET_USD}"
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_common_spec(
    format_version: u32,
    expected_format_version: u32,
    run_id: &str,
    benchmark_version: &str,
    case_id: &str,
    expected_cli_version: &str,
    expected_product_executable_sha256: &str,
    program: &Path,
    workspace: &Path,
    workspace_snapshot: &str,
    model: &str,
    system_prompt: &str,
    prompt: &str,
    timeout_ms: u64,
    inherit_environment: &[String],
) -> AppResult<()> {
    if format_version != expected_format_version {
        return Err(format!(
            "unsupported run spec format {format_version}; expected {expected_format_version}"
        ));
    }
    validate_id("run_id", run_id)?;
    validate_id("benchmark_version", benchmark_version)?;
    validate_id("case_id", case_id)?;
    validate_text("expected_cli_version", expected_cli_version)?;
    if !is_lower_sha256(expected_product_executable_sha256) {
        return Err(
            "expected_product_executable_sha256 must be 64 lowercase hexadecimal bytes".to_owned(),
        );
    }
    validate_text("workspace_snapshot", workspace_snapshot)?;
    validate_text("model", model)?;
    if system_prompt.trim().is_empty()
        || system_prompt.len() > MAX_SYSTEM_PROMPT_BYTES
        || system_prompt
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(format!(
            "system_prompt must be 1-{MAX_SYSTEM_PROMPT_BYTES} non-control bytes"
        ));
    }
    if prompt.trim().is_empty() || prompt.len() > MAX_PROMPT_BYTES {
        return Err(format!("prompt must be 1-{MAX_PROMPT_BYTES} bytes"));
    }
    if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
        return Err(format!("timeout_ms must be 1-{MAX_TIMEOUT_MS}"));
    }
    if inherit_environment.len() > MAX_ENVIRONMENT_NAMES {
        return Err(format!(
            "inherit_environment exceeds {MAX_ENVIRONMENT_NAMES} names"
        ));
    }
    let mut names = BTreeSet::new();
    for name in inherit_environment {
        if !valid_environment_name(name) || !names.insert(name) {
            return Err("inherit_environment contains an invalid or duplicate name".to_owned());
        }
    }
    if !program.is_absolute() || !workspace.is_absolute() {
        return Err("program and workspace must be absolute paths".to_owned());
    }
    Ok(())
}

fn validate_id(kind: &str, value: &str) -> AppResult<()> {
    if value.is_empty()
        || value.len() > MAX_ID_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-/".contains(&byte))
    {
        return Err(format!(
            "{kind} must be 1-{MAX_ID_BYTES} ASCII identity bytes"
        ));
    }
    Ok(())
}

fn validate_text(kind: &str, value: &str) -> AppResult<()> {
    if value.trim().is_empty()
        || value.len() > MAX_TEXT_FIELD_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(format!(
            "{kind} must be 1-{MAX_TEXT_FIELD_BYTES} non-control bytes"
        ));
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(byte) if byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && name.len() <= 128
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn execute_claude(spec: ClaudeRunSpec) -> AppResult<ExternalRunReport> {
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
    let environment = inherited_environment(&spec.inherit_environment)?;
    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Claude Code executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &environment, "Claude Code").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Claude Code version mismatch: expected {:?}, observed {:?}",
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
    let args = claude_arguments(&spec);
    let request = ProcessRequest {
        program,
        args,
        current_dir: workspace,
        environment,
        stdin: spec.prompt.as_bytes().to_vec(),
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
                        message: "Claude Code output exceeded the adapter retention bound"
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
                match normalize_claude_result(&output.stdout) {
                    Ok(normalized) => {
                        let observed_models = normalized.observed_models.clone();
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: Some(normalized.duration_ms),
                            product_api_duration_ms: Some(normalized.duration_api_ms),
                            num_turns: normalized.num_turns,
                            actual_cost_usd: Some(normalized.total_cost_usd),
                            actual_cost_usd_ticks: None,
                            result_subtype: normalized.subtype,
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

    let mut unsupported_controls = vec![
        "adapter conformance is not a Harness-effect or product-quality result",
        "no cross-product model parity has been established",
        "Tools are disabled, so Agent-loop effectiveness is not measured",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values and provider routing are not recorded",
    ];
    if matches!(spec.profile, ClaudeProfile::Product) {
        unsupported_controls.push("ambient product configuration is not eliminated");
    }

    Ok(ExternalRunReport {
        format_version: CLAUDE_RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: CLAUDE_ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "claude-code",
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
                ClaudeProfile::Bare => "bare",
                ClaudeProfile::Product => "product",
            },
            requested_model: spec.model,
            observed_models,
            prompt_sha256,
            system_prompt_sha256,
            tools: "disabled",
            permission_mode: "dont_ask",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: Some(spec.max_budget_usd),
            requested_reasoning_effort: None,
            requested_max_turns: None,
            product_sandbox: None,
            unsupported_controls,
        },
        execution,
    })
}

fn failed_process_evidence(
    output: &ProcessOutput,
    stdout_sha256: String,
    stderr_sha256: String,
) -> FailedProcessEvidence {
    FailedProcessEvidence {
        exit_code: output.code,
        stdout_bytes: output.stdout.len(),
        stdout_sha256,
        stdout_truncated: output.stdout_truncated,
        stderr_bytes: output.stderr.len(),
        stderr_sha256,
        stderr_truncated: output.stderr_truncated,
    }
}

fn inherited_environment(names: &[String]) -> AppResult<BTreeMap<String, String>> {
    names
        .iter()
        .map(|name| {
            env::var(name)
                .map(|value| (name.clone(), value))
                .map_err(|_| format!("required inherited environment variable {name} is absent"))
        })
        .collect()
}

fn canonical_empty_directory(path: &Path, kind: &str) -> AppResult<PathBuf> {
    let directory =
        fs::canonicalize(path).map_err(|error| format!("cannot canonicalize {kind}: {error}"))?;
    if !directory.is_dir() {
        return Err(format!("{kind} must resolve to a directory"));
    }
    let mut entries =
        fs::read_dir(&directory).map_err(|error| format!("cannot inspect {kind}: {error}"))?;
    if entries
        .next()
        .transpose()
        .map_err(|error| format!("cannot inspect {kind}: {error}"))?
        .is_some()
    {
        return Err(format!("{kind} must be empty before execution"));
    }
    Ok(directory)
}

async fn read_cli_version(
    broker: &LocalProcessBroker,
    program: &Path,
    workspace: &Path,
    environment: &BTreeMap<String, String>,
    product: &str,
) -> AppResult<String> {
    let output = broker
        .execute(
            ProcessRequest {
                program: program.to_path_buf(),
                args: vec!["--version".to_owned()],
                current_dir: workspace.to_path_buf(),
                environment: environment.clone(),
                stdin: Vec::new(),
                timeout: Duration::from_secs(10),
                max_output_bytes: 65_536,
                cancellation_phase: ExecutionPhase::Model,
            },
            CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("{product} version probe failed: {error}"))?;
    if !output.success || output.stdout_truncated || output.stderr_truncated {
        return Err(format!(
            "{product} version probe did not settle successfully"
        ));
    }
    let version = std::str::from_utf8(&output.stdout)
        .map_err(|_| format!("{product} version output is not UTF-8"))?
        .trim()
        .to_owned();
    validate_text("observed CLI version", &version)?;
    Ok(version)
}

fn claude_arguments(spec: &ClaudeRunSpec) -> Vec<String> {
    let mut args = Vec::with_capacity(17);
    if matches!(spec.profile, ClaudeProfile::Bare) {
        args.push("--bare".to_owned());
    }
    args.extend([
        "--print".to_owned(),
        "--output-format".to_owned(),
        "json".to_owned(),
        "--no-session-persistence".to_owned(),
        "--disable-slash-commands".to_owned(),
        "--strict-mcp-config".to_owned(),
        "--mcp-config".to_owned(),
        r#"{"mcpServers":{}}"#.to_owned(),
        "--permission-mode".to_owned(),
        "dontAsk".to_owned(),
        "--tools=".to_owned(),
        "--system-prompt".to_owned(),
        spec.system_prompt.clone(),
        "--model".to_owned(),
        spec.model.clone(),
        "--max-budget-usd".to_owned(),
        spec.max_budget_usd.to_string(),
    ]);
    args
}

fn normalize_claude_result(bytes: &[u8]) -> AppResult<NormalizedClaudeResult> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| "Claude Code stdout is not one JSON result")?;
    let object = value
        .as_object()
        .ok_or_else(|| "Claude Code result must be a JSON object".to_owned())?;
    if object.get("type").and_then(Value::as_str) != Some("result") {
        return Err("Claude Code JSON is not a result envelope".to_owned());
    }
    let is_error = object
        .get("is_error")
        .and_then(Value::as_bool)
        .ok_or_else(|| "Claude Code result has no boolean is_error".to_owned())?;
    let subtype = bounded_optional_string(object.get("subtype"), "subtype")?;
    let total_cost_usd = required_nonnegative_f64(object.get("total_cost_usd"), "total_cost_usd")?;
    if !is_error {
        let result = object
            .get("result")
            .and_then(Value::as_str)
            .ok_or_else(|| "successful Claude Code result has no text result".to_owned())?;
        if result.len() > MAX_PROMPT_BYTES {
            return Err("Claude Code text result exceeds the adapter bound".to_owned());
        }
    }
    let observed_models = object
        .get("modelUsage")
        .and_then(Value::as_object)
        .map(|models| {
            let mut names = models.keys().cloned().collect::<Vec<_>>();
            names.sort();
            names
        })
        .ok_or_else(|| "Claude Code result has no modelUsage object".to_owned())?;
    if observed_models.is_empty()
        || observed_models.len() > 64
        || observed_models
            .iter()
            .any(|name| validate_text("observed model", name).is_err())
    {
        return Err("Claude Code result contains invalid observed model identities".to_owned());
    }
    Ok(NormalizedClaudeResult {
        is_error,
        subtype,
        duration_ms: required_u64(object.get("duration_ms"), "duration_ms")?,
        duration_api_ms: required_u64(object.get("duration_api_ms"), "duration_api_ms")?,
        num_turns: required_u64(object.get("num_turns"), "num_turns")?,
        total_cost_usd,
        observed_models,
        raw: value,
    })
}

fn required_u64(value: Option<&Value>, kind: &str) -> AppResult<u64> {
    value
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("Claude Code {kind} must be an unsigned integer"))
}

fn required_nonnegative_f64(value: Option<&Value>, kind: &str) -> AppResult<f64> {
    let value = value
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("Claude Code {kind} must be numeric"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("Claude Code {kind} must be finite and nonnegative"));
    }
    Ok(value)
}

fn bounded_optional_string(value: Option<&Value>, kind: &str) -> AppResult<Option<String>> {
    value
        .map(|value| {
            let value = value
                .as_str()
                .ok_or_else(|| format!("Claude Code {kind} must be a string"))?;
            validate_text(kind, value)?;
            Ok(value.to_owned())
        })
        .transpose()
}

fn sha256_file(path: &Path) -> AppResult<String> {
    let mut file = fs::File::open(path).map_err(|error| format!("cannot hash program: {error}"))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash program: {error}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(lower_hex(&hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn bounded_error(message: &str) -> String {
    message.chars().take(1_024).collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        CLAUDE_RUN_FORMAT_VERSION, ClaudeProfile, ClaudeRunSpec, claude_arguments,
        normalize_claude_result, validate_spec,
    };

    fn valid_spec() -> ClaudeRunSpec {
        ClaudeRunSpec {
            format_version: CLAUDE_RUN_FORMAT_VERSION,
            run_id: "run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("claude"),
            expected_cli_version: "2.1.143 (Claude Code)".to_owned(),
            expected_product_executable_sha256: "a".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: ClaudeProfile::Bare,
            model: "claude-haiku-4-5".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            max_budget_usd: 0.1,
            inherit_environment: vec!["ANTHROPIC_API_KEY".to_owned()],
        }
    }

    fn absolute_path(name: &str) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(format!(r"C:\{name}"))
        } else {
            PathBuf::from(format!("/{name}"))
        }
    }

    #[test]
    fn run_spec_rejects_unknown_coordinates_and_duplicate_environment_names() {
        let mut spec = valid_spec();
        validate_spec(&spec).expect("valid spec");
        spec.format_version += 1;
        assert!(validate_spec(&spec).is_err());
        spec.format_version = CLAUDE_RUN_FORMAT_VERSION;
        spec.inherit_environment
            .push("ANTHROPIC_API_KEY".to_owned());
        assert!(validate_spec(&spec).is_err());
    }

    #[test]
    fn claude_command_is_shell_free_bounded_and_receives_prompt_only_on_stdin() {
        let spec = valid_spec();
        let arguments = claude_arguments(&spec);
        assert_eq!(arguments.first().map(String::as_str), Some("--bare"));
        assert!(arguments.iter().any(|value| value == "--tools="));
        assert!(arguments.iter().any(|value| value == "--strict-mcp-config"));
        assert!(!arguments.iter().any(|value| value == &spec.prompt));
    }

    #[test]
    fn budget_error_remains_a_product_result_with_actual_model_and_cost() {
        let normalized =
            normalize_claude_result(include_bytes!("../fixtures/claude-code-budget-error.json"))
                .expect("observed Claude Code result shape");
        assert!(normalized.is_error);
        assert_eq!(normalized.subtype.as_deref(), Some("error_max_budget_usd"));
        assert_eq!(normalized.total_cost_usd, 0.056875);
        assert_eq!(normalized.observed_models, ["MiniMax-M2.7"]);
    }

    #[test]
    fn checked_in_live_evidence_preserves_the_non_claim_boundary() {
        let report: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-26-claude-code-probe/result.json"
        ))
        .expect("checked-in external-run evidence");
        assert_eq!(report["format_version"], 1);
        assert_eq!(report["controls"]["claim_eligible"], false);
        assert_eq!(report["execution"]["status"], "completed");
        let raw = serde_json::to_vec(&report["execution"]["settlement"]["raw_result"])
            .expect("encode retained raw result");
        let normalized = normalize_claude_result(&raw).expect("normalize retained raw result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.observed_models, ["MiniMax-M2.7"]);
        assert_eq!(normalized.total_cost_usd, 0.024075000000000003);
    }
}
