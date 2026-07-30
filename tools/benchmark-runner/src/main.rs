//! Released-product benchmark adapters kept outside the Harness semantic core.

mod codex;
mod codex_fault;
mod grok_build;
mod hermes;
mod opencode;
mod pi;
mod y_harness_fault;

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
    CancellationToken, ExecutionPhase, HarnessError, LocalProcessBroker, ProcessBroker,
    ProcessIsolation, ProcessOutput, ProcessRequest,
};

const CLAUDE_RUN_FORMAT_VERSION: u32 = 1;
const CLAUDE_ADAPTER_VERSION: &str = "claude-code-json-v2";
const CLAUDE_PROVIDER_TOKEN_ENV: &str = "ANTHROPIC_API_KEY";
const CLAUDE_MAX_TURNS: u64 = 64;
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
    provider: Option<String>,
    provider_base_url: Option<String>,
    model: String,
    reasoning_effort: Option<String>,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    max_budget_usd: f64,
    max_turns: Option<u64>,
    inherit_environment: Vec<String>,
    home: Option<PathBuf>,
    claude_config_dir: Option<PathBuf>,
    temp_dir: Option<PathBuf>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_provider: Option<String>,
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
#[serde(untagged)]
enum BenchmarkReport {
    External(Box<ExternalRunReport>),
    CodexFault(Box<codex_fault::Report>),
    CodexRestartFault(Box<codex_fault::RestartReport>),
    YHarnessRestartFault(Box<y_harness_fault::Report>),
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

async fn run() -> AppResult<BenchmarkReport> {
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
        "claude-code" => execute_claude(read_claude_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "codex" => codex::execute(codex::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "codex-cf003" => codex_fault::execute(codex_fault::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::CodexFault(Box::new(report))),
        "codex-cf003-restart" => {
            codex_fault::execute_restart(codex_fault::read_restart_spec(&spec_path)?)
                .await
                .map(|report| BenchmarkReport::CodexRestartFault(Box::new(report)))
        }
        "grok-build" => grok_build::execute(grok_build::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "hermes" => hermes::execute(hermes::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "opencode" => opencode::execute(opencode::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "pi" => pi::execute(pi::read_spec(&spec_path)?)
            .await
            .map(|report| BenchmarkReport::External(Box::new(report))),
        "y-harness-cf003-restart" => {
            y_harness_fault::execute(y_harness_fault::read_spec(&spec_path)?)
                .await
                .map(|report| BenchmarkReport::YHarnessRestartFault(Box::new(report)))
        }
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: yh-bench <claude-code|codex|codex-cf003|codex-cf003-restart|grok-build|hermes|opencode|pi|y-harness-cf003-restart> <run-spec.json>".to_owned()
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
    match spec.profile {
        ClaudeProfile::Bare => {
            let Some(provider) = spec.provider.as_deref() else {
                return Err("bare Claude Code profile requires provider".to_owned());
            };
            validate_text("Claude Code provider", provider)?;
            let Some(provider_base_url) = spec.provider_base_url.as_deref() else {
                return Err("bare Claude Code profile requires provider_base_url".to_owned());
            };
            validate_claude_loopback_base_url(provider_base_url)?;
            let Some(reasoning_effort) = spec.reasoning_effort.as_deref() else {
                return Err("bare Claude Code profile requires reasoning_effort".to_owned());
            };
            if !matches!(
                reasoning_effort,
                "low" | "medium" | "high" | "xhigh" | "max"
            ) {
                return Err("unsupported Claude Code reasoning_effort".to_owned());
            }
            if spec
                .max_turns
                .is_none_or(|turns| !(1..=CLAUDE_MAX_TURNS).contains(&turns))
            {
                return Err(format!(
                    "bare Claude Code max_turns must be 1-{CLAUDE_MAX_TURNS}"
                ));
            }
            if spec.home.as_ref().is_none_or(|path| !path.is_absolute())
                || spec
                    .claude_config_dir
                    .as_ref()
                    .is_none_or(|path| !path.is_absolute())
                || spec
                    .temp_dir
                    .as_ref()
                    .is_none_or(|path| !path.is_absolute())
            {
                return Err(
                    "bare Claude Code profile requires absolute home, claude_config_dir, and temp_dir directories"
                        .to_owned(),
                );
            }
            if spec.inherit_environment.as_slice() != [CLAUDE_PROVIDER_TOKEN_ENV] {
                return Err("bare Claude Code profile inherits only ANTHROPIC_API_KEY".to_owned());
            }
        }
        ClaudeProfile::Product => {
            if spec.provider.is_some()
                || spec.provider_base_url.is_some()
                || spec.reasoning_effort.is_some()
                || spec.max_turns.is_some()
                || spec.home.is_some()
                || spec.claude_config_dir.is_some()
                || spec.temp_dir.is_some()
            {
                return Err(
                    "product Claude Code profile must not declare bare-profile controls".to_owned(),
                );
            }
        }
    }
    Ok(())
}

fn validate_claude_loopback_base_url(value: &str) -> AppResult<()> {
    let port = value
        .strip_prefix("http://127.0.0.1:")
        .ok_or_else(|| {
            "bare Claude Code provider_base_url must be http://127.0.0.1:<port>".to_owned()
        })?
        .parse::<u16>()
        .map_err(|_| {
            "bare Claude Code provider_base_url must contain a valid loopback port".to_owned()
        })?;
    if port == 0 {
        return Err("bare Claude Code provider_base_url port must be nonzero".to_owned());
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
    let mut environment = inherited_environment(&spec.inherit_environment)?;
    prepare_claude_environment(&spec, &mut environment)?;
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
    let args = claude_arguments(&spec)?;
    let request = ProcessRequest {
        program,
        args,
        current_dir: workspace,
        environment,
        secret_environment: BTreeMap::new(),
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
        "credential values and launcher dependencies are not recorded",
    ];
    if matches!(spec.profile, ClaudeProfile::Product) {
        unsupported_controls.push("ambient product configuration is not eliminated");
    } else {
        unsupported_controls.extend([
            "the Provider request sidecar is corroborating evidence rather than product settlement",
            "Claude Code JSON does not expose settled Provider identity",
            "product-reported cost is a price-table projection for the loopback fixture, not incurred Provider spend",
            "Claude Code sends an auxiliary HEAD probe before the Model request",
            "Claude Code exposes no hard Provider-call ceiling for one Turn",
            "Claude Code materializes configuration state despite --no-session-persistence",
            "built-in product prompt blocks and current-date Context are not eliminated",
            "no product OS sandbox is requested because all Tools are disabled",
        ]);
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
            requested_provider: spec.provider,
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
            requested_reasoning_effort: spec.reasoning_effort,
            requested_max_turns: spec.max_turns,
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
                secret_environment: BTreeMap::new(),
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

fn prepare_claude_environment(
    spec: &ClaudeRunSpec,
    environment: &mut BTreeMap<String, String>,
) -> AppResult<()> {
    let ClaudeProfile::Bare = spec.profile else {
        return Ok(());
    };
    let home = spec
        .home
        .as_ref()
        .ok_or_else(|| "bare Claude Code profile has no home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "Claude Code home"))?;
    let config = spec
        .claude_config_dir
        .as_ref()
        .ok_or_else(|| "bare Claude Code profile has no claude_config_dir".to_owned())
        .and_then(|path| canonical_empty_directory(path, "Claude Code config directory"))?;
    let temp = spec
        .temp_dir
        .as_ref()
        .ok_or_else(|| "bare Claude Code profile has no temp_dir".to_owned())
        .and_then(|path| canonical_empty_directory(path, "Claude Code temp directory"))?;
    if home == config || home == temp || config == temp {
        return Err("bare Claude Code state directories must be distinct".to_owned());
    }
    let home = home
        .to_str()
        .ok_or_else(|| "Claude Code home must be valid UTF-8".to_owned())?;
    let config = config
        .to_str()
        .ok_or_else(|| "Claude Code config directory must be valid UTF-8".to_owned())?;
    let temp = temp
        .to_str()
        .ok_or_else(|| "Claude Code temp directory must be valid UTF-8".to_owned())?;
    let provider_base_url = spec
        .provider_base_url
        .as_deref()
        .ok_or_else(|| "bare Claude Code profile has no provider_base_url".to_owned())?;

    environment.insert("HOME".to_owned(), home.to_owned());
    environment.insert("USERPROFILE".to_owned(), home.to_owned());
    environment.insert("TMPDIR".to_owned(), temp.to_owned());
    environment.insert("CLAUDE_CONFIG_DIR".to_owned(), config.to_owned());
    environment.insert("ANTHROPIC_CONFIG_DIR".to_owned(), config.to_owned());
    environment.insert(
        "ANTHROPIC_BASE_URL".to_owned(),
        provider_base_url.to_owned(),
    );
    for name in [
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
        "DISABLE_TELEMETRY",
        "DISABLE_ERROR_REPORTING",
        "DISABLE_AUTOUPDATER",
    ] {
        environment.insert(name.to_owned(), "1".to_owned());
    }
    Ok(())
}

fn claude_arguments(spec: &ClaudeRunSpec) -> AppResult<Vec<String>> {
    let mut args = Vec::with_capacity(25);
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
    if matches!(spec.profile, ClaudeProfile::Bare) {
        let effort = spec
            .reasoning_effort
            .as_ref()
            .ok_or_else(|| "bare Claude Code profile has no reasoning_effort".to_owned())?;
        let max_turns = spec
            .max_turns
            .ok_or_else(|| "bare Claude Code profile has no max_turns".to_owned())?;
        args.extend([
            "--settings".to_owned(),
            "{}".to_owned(),
            "--effort".to_owned(),
            effort.clone(),
            "--max-turns".to_owned(),
            max_turns.to_string(),
        ]);
    }
    Ok(args)
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
    use super::*;

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
            provider: Some("yh-loopback".to_owned()),
            provider_base_url: Some("http://127.0.0.1:1234".to_owned()),
            model: "claude-haiku-4-5".to_owned(),
            reasoning_effort: Some("medium".to_owned()),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            max_budget_usd: 0.1,
            max_turns: Some(1),
            inherit_environment: vec!["ANTHROPIC_API_KEY".to_owned()],
            home: Some(absolute_path("home")),
            claude_config_dir: Some(absolute_path("config")),
            temp_dir: Some(absolute_path("tmp")),
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
        let arguments = claude_arguments(&spec).expect("Claude Code arguments");
        assert_eq!(arguments.first().map(String::as_str), Some("--bare"));
        assert!(arguments.iter().any(|value| value == "--tools="));
        assert!(arguments.iter().any(|value| value == "--strict-mcp-config"));
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--effort", "medium"])
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--max-turns", "1"])
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--settings", "{}"])
        );
        assert!(!arguments.iter().any(|value| value == &spec.prompt));
    }

    #[test]
    fn claude_profiles_reject_ambiguous_runtime_authority() {
        let mut inherited_home = valid_spec();
        inherited_home
            .inherit_environment
            .push("claude_config_dir".to_owned());
        assert!(validate_spec(&inherited_home).is_err());

        let mut remote_provider = valid_spec();
        remote_provider.provider_base_url = Some("https://api.anthropic.com".to_owned());
        assert!(validate_spec(&remote_provider).is_err());

        let mut product = valid_spec();
        product.profile = ClaudeProfile::Product;
        assert!(validate_spec(&product).is_err());
        product.provider = None;
        product.provider_base_url = None;
        product.reasoning_effort = None;
        product.max_turns = None;
        product.home = None;
        product.claude_config_dir = None;
        product.temp_dir = None;
        assert!(validate_spec(&product).is_ok());
    }

    #[test]
    fn claude_bare_environment_owns_home_config_temp_and_provider() {
        let root = env::temp_dir().join(format!(
            "yh-claude-environment-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let home = root.join("home");
        let config = root.join("config");
        let temp = root.join("tmp");
        for directory in [&home, &config, &temp] {
            fs::create_dir_all(directory).expect("create isolated Claude Code directory");
        }
        let mut spec = valid_spec();
        spec.home = Some(home.clone());
        spec.claude_config_dir = Some(config.clone());
        spec.temp_dir = Some(temp.clone());
        let mut environment = BTreeMap::new();
        prepare_claude_environment(&spec, &mut environment)
            .expect("prepare bare Claude Code environment");

        assert_eq!(
            environment["HOME"],
            fs::canonicalize(home)
                .expect("canonical Claude Code home")
                .to_str()
                .expect("UTF-8 Claude Code home")
        );
        assert_eq!(
            environment["CLAUDE_CONFIG_DIR"],
            environment["ANTHROPIC_CONFIG_DIR"]
        );
        assert_eq!(environment["ANTHROPIC_BASE_URL"], "http://127.0.0.1:1234");
        assert_eq!(environment["DISABLE_TELEMETRY"], "1");
        fs::remove_dir_all(root).expect("remove isolated Claude Code environment");
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

    #[test]
    fn checked_in_claude_loopback_evidence_preserves_controls_and_auxiliary_probe() {
        let report: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-claude-code-fixed-output/result.json"
        ))
        .expect("checked-in Claude Code loopback report");
        let requests =
            include_str!("../evidence/2026-07-28-claude-code-fixed-output/provider-request.jsonl")
                .lines()
                .map(|line| {
                    serde_json::from_str::<Value>(line).expect("Claude Code Provider request")
                })
                .collect::<Vec<_>>();
        let provider =
            include_bytes!("../evidence/2026-07-28-claude-code-fixed-output/provider.mjs");

        assert_eq!(report["format_version"], CLAUDE_RUN_FORMAT_VERSION);
        assert_eq!(report["adapter"]["name"], CLAUDE_ADAPTER_VERSION);
        assert_eq!(
            report["adapter"]["adapter_executable_sha256"],
            "19db0b5d6d1d1bb93ddf66a3a279e38c226fd656b143ebc1b301742ae785d49b"
        );
        assert_eq!(
            report["adapter"]["product_executable_sha256"],
            "2701c6cfd68483f8faf0316a1ba6481a1455a90645ada179f0c48d8c36d722ef"
        );
        assert_eq!(report["controls"]["claim_eligible"], false);
        assert_eq!(report["controls"]["profile"], "bare");
        assert_eq!(
            report["controls"]["requested_provider"],
            "yh-loopback-anthropic-messages"
        );
        assert_eq!(
            report["controls"]["requested_model"],
            "claude-haiku-4-5-20251001"
        );
        assert_eq!(
            report["controls"]["observed_models"],
            serde_json::json!(["claude-haiku-4-5-20251001"])
        );
        assert_eq!(report["controls"]["requested_reasoning_effort"], "medium");
        assert_eq!(report["controls"]["requested_max_turns"], 1);
        assert_eq!(report["execution"]["status"], "completed");
        assert_eq!(
            report["execution"]["settlement"]["raw_result"]["result"],
            "YH-CLAUDE-ADAPTER-OK"
        );
        assert_eq!(
            sha256_bytes(provider),
            "d8c2c3abde00eb1a98f491610e8890a3f65d458099d35f069f272080d31267e9"
        );
        let normalized = normalize_claude_result(
            &serde_json::to_vec(&report["execution"]["settlement"]["raw_result"])
                .expect("encode Claude Code result"),
        )
        .expect("normalize retained Claude Code result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.observed_models, ["claude-haiku-4-5-20251001"]);

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["method"], "HEAD");
        assert_eq!(requests[0]["path"], "/");
        assert_eq!(requests[1]["method"], "POST");
        assert_eq!(requests[1]["path"], "/v1/messages?beta=true");
        assert_eq!(requests[1]["authorization"], "x-api-key-present");
        assert_eq!(requests[1]["body"]["model"], "claude-haiku-4-5-20251001");
        assert_eq!(requests[1]["body"]["system"]["has_requested_system"], true);
        assert_eq!(requests[1]["body"]["tool_names"], serde_json::json!([]));
        assert_eq!(
            requests[1]["body"]["thinking"],
            serde_json::json!({"budget_tokens": 31_999, "type": "enabled"})
        );
        assert_eq!(
            requests[1]["body"]["messages"][0]["last_text"],
            "Return exactly YH-CLAUDE-ADAPTER-OK"
        );
    }

    #[test]
    fn checked_in_harness_control_preflight_rejects_false_model_and_tool_parity() {
        let claude_spec: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-harness-control-preflight/claude-spec.json"
        ))
        .expect("checked-in Claude Code preflight spec");
        let codex_spec: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-harness-control-preflight/codex-spec.json"
        ))
        .expect("checked-in Codex preflight spec");
        let manifest: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-harness-control-preflight/preflight.json"
        ))
        .expect("checked-in Harness-control preflight");
        let claude: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-harness-control-preflight/claude-result.json"
        ))
        .expect("checked-in Claude Code preflight report");
        let codex: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-harness-control-preflight/codex-result.json"
        ))
        .expect("checked-in Codex preflight report");
        let requests =
            include_str!("../evidence/2026-07-28-harness-control-preflight/provider-request.jsonl")
                .lines()
                .map(|line| {
                    serde_json::from_str::<Value>(line).expect("preflight Provider request")
                })
                .collect::<Vec<_>>();
        let provider =
            include_bytes!("../evidence/2026-07-28-harness-control-preflight/provider.mjs");

        assert_eq!(manifest["verdict"], "not_comparable");
        assert_eq!(manifest["eligible_for_harness_effect_claim"], false);
        assert_eq!(
            sha256_bytes(provider),
            manifest["shared_requested_controls"]["provider_executable_sha256"]
        );

        for field in [
            "benchmark_version",
            "case_id",
            "workspace_snapshot",
            "profile",
            "provider",
            "model",
            "reasoning_effort",
            "system_prompt",
            "prompt",
            "timeout_ms",
        ] {
            assert_eq!(
                claude_spec[field], codex_spec[field],
                "shared input coordinate {field}"
            );
        }
        assert_eq!(
            codex_spec["provider_base_url"],
            format!(
                "{}/v1",
                claude_spec["provider_base_url"]
                    .as_str()
                    .expect("Claude Provider base URL")
            )
        );
        assert_eq!(
            sha256_bytes(
                claude_spec["system_prompt"]
                    .as_str()
                    .expect("Claude system prompt")
                    .as_bytes()
            ),
            claude["controls"]["system_prompt_sha256"]
        );
        assert_eq!(
            sha256_bytes(
                codex_spec["prompt"]
                    .as_str()
                    .expect("Codex prompt")
                    .as_bytes()
            ),
            codex["controls"]["prompt_sha256"]
        );
        assert_eq!(
            claude_spec["expected_product_executable_sha256"],
            claude["adapter"]["product_executable_sha256"]
        );
        assert_eq!(
            codex_spec["expected_product_executable_sha256"],
            codex["adapter"]["product_executable_sha256"]
        );

        for field in [
            "requested_provider",
            "requested_model",
            "prompt_sha256",
            "system_prompt_sha256",
            "requested_reasoning_effort",
            "timeout_ms",
        ] {
            assert_eq!(
                claude["controls"][field], codex["controls"][field],
                "shared requested control {field}"
            );
        }
        assert_eq!(
            claude["coordinate"]["benchmark_version"],
            codex["coordinate"]["benchmark_version"]
        );
        assert_eq!(
            claude["coordinate"]["case_id"],
            codex["coordinate"]["case_id"]
        );
        assert_eq!(
            claude["coordinate"]["workspace_snapshot"],
            codex["coordinate"]["workspace_snapshot"]
        );
        assert_eq!(claude["controls"]["claim_eligible"], false);
        assert_eq!(codex["controls"]["claim_eligible"], false);
        assert_eq!(
            claude["execution"]["settlement"]["raw_result"]["result"],
            "YH-HARNESS-CONTROL-OK"
        );
        assert_eq!(
            codex["execution"]["settlement"]["raw_result"][3]["item"]["text"],
            "YH-HARNESS-CONTROL-OK"
        );

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["protocol"], "claude_probe");
        assert_eq!(requests[1]["protocol"], "anthropic_messages");
        assert_eq!(requests[2]["protocol"], "openai_responses");
        assert_eq!(requests[1]["authorization"], "x-api-key-valid");
        assert_eq!(requests[2]["authorization"], "bearer-valid");
        assert_eq!(requests[1]["body"]["model"], requests[2]["body"]["model"]);
        assert_eq!(requests[1]["body"]["tool_names"], serde_json::json!([]));
        assert_eq!(
            requests[2]["body"]["tool_names"],
            serde_json::json!([
                "exec_command",
                "write_stdin",
                "update_plan",
                "request_user_input",
                "view_image"
            ])
        );
        assert_eq!(
            requests[1]["body"]["thinking"],
            serde_json::json!({"budget_tokens": 31_999, "type": "enabled"})
        );
        assert_eq!(
            requests[2]["body"]["reasoning"],
            serde_json::json!({"effort": "medium", "summary": "auto"})
        );

        assert_eq!(
            claude["controls"]["observed_models"],
            serde_json::json!(["claude-haiku-4-5-20251001"])
        );
        assert_eq!(codex["controls"]["observed_models"], serde_json::json!([]));
        assert_eq!(claude["controls"]["tools"], "disabled");
        assert_eq!(codex["controls"]["tools"], "product_builtins_read_only");
        assert!(claude["controls"]["product_sandbox"].is_null());
        assert_eq!(codex["controls"]["product_sandbox"], "read-only");
        assert_eq!(
            codex["execution"]["settlement"]["raw_result"][1]["item"]["message"],
            "Model metadata for `claude-haiku-4-5-20251001` not found. Defaulting to fallback metadata; this can degrade performance and cause issues."
        );
    }
}
