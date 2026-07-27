//! Source-pinned Codex driver for the deterministic CF-003 MCP fault fixture.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    net::Ipv4Addr,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

use super::*;

const RUN_FORMAT_VERSION: u32 = 7;
const ADAPTER_NAME: &str = "codex-cf003-responses-mcp-v1";
const CASE_ID: &str = "cf-003-uncertain-non-idempotent-tool-effect";
const CODEX_CLI_VERSION: &str = "codex-cli 0.145.0";
const CODEX_SOURCE_REPOSITORY: &str = "https://github.com/openai/codex";
const CODEX_SOURCE_REF: &str = "rust-v0.145.0";
const CODEX_SOURCE_COMMIT: &str = "25af12f7e61572b0bc18ddb1008be543b91519b0";
const CODEX_MODEL: &str = "gpt-5.4";
const MCP_SERVER_NAME: &str = "yh_fault";
const MCP_NAMESPACE: &str = "mcp__yh_fault";
const MCP_TOOL_NAME: &str = "commit_effect";
const SEARCH_CALL_ID: &str = "cf003-search-1";
const CALL_ID: &str = "cf003-call-1";
const PROVIDER_TOKEN_ENV: &str = "YH_FAULT_PROVIDER_TOKEN";
const PROVIDER_TOKEN: &str = "yh-fault-loopback-token";
const FINAL_MESSAGE: &str = "CF003_OBSERVED";
const SYSTEM_PROMPT: &str = "This is a deterministic Harness fault experiment. Follow the provider Tool decision, then return only the provider-selected final text.";
const USER_PROMPT: &str = "Execute the deterministic CF-003 experiment. Do not invoke any Tool except the requested MCP Tool.";
const MAX_HTTP_HEADER_BYTES: usize = 65_536;
const MAX_HTTP_BODY_BYTES: usize = 2_097_152;
const MAX_PROVIDER_REQUESTS: usize = 8;
const MAX_TOOL_IDENTITIES: usize = 256;
const PROVIDER_IO_TIMEOUT: Duration = Duration::from_secs(10);

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
    fixture_program: PathBuf,
    fixture_spec: PathBuf,
    expected_fixture_spec_sha256: String,
    workspace: PathBuf,
    workspace_snapshot: String,
    codex_home: PathBuf,
    model: String,
    timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureSpec {
    format_version: u32,
    fixture_id: String,
    case: String,
    expected_fixture_executable_sha256: String,
    journal: PathBuf,
    operation_id: String,
    expected_payload_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixturePrepared {
    format_version: u32,
    fixture_id: String,
    fixture_executable_sha256: String,
    fixture_spec_sha256: String,
    journal_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureObserved {
    format_version: u32,
    track: String,
    claim_eligible: bool,
    fixture_id: String,
    case: String,
    fixture_executable_sha256: String,
    fixture_spec_sha256: String,
    journal_sha256: String,
    invocation_count: u64,
    effect_count: u64,
    tail: String,
    oracle: FixtureOracle,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FixtureOracle {
    passed: bool,
    classification: String,
}

#[derive(Serialize)]
pub(super) struct Report {
    format_version: u32,
    adapter: Adapter,
    source_analysis: SourceAnalysis,
    coordinate: Coordinate,
    controls: Controls,
    execution: Execution,
    fixture: FixtureEvidence,
}

#[derive(Serialize)]
struct Adapter {
    name: &'static str,
    version: &'static str,
    product: &'static str,
    cli_version: String,
    adapter_executable_sha256: String,
    product_executable_sha256: String,
    fixture_executable_sha256: String,
}

#[derive(Serialize)]
struct SourceAnalysis {
    repository: &'static str,
    source_ref: &'static str,
    commit: &'static str,
    binary_source_equivalence_verified: bool,
}

#[derive(Serialize)]
struct Coordinate {
    run_id: String,
    benchmark_version: String,
    case_id: String,
    workspace_snapshot: String,
    started_at_ms: u64,
    host_os: &'static str,
    host_arch: &'static str,
}

#[derive(Serialize)]
struct Controls {
    track: &'static str,
    claim_eligible: bool,
    profile: &'static str,
    requested_provider: &'static str,
    requested_model: String,
    requested_tool: &'static str,
    permission_mode: &'static str,
    product_sandbox: &'static str,
    process_isolation: ProcessIsolation,
    supplied_environment_names: [&'static str; 2],
    timeout_ms: u64,
    product_restart_exercised: bool,
    unsupported_controls: [&'static str; 5],
}

#[derive(Serialize)]
struct Execution {
    passed: bool,
    wall_time_ms: u64,
    process: ProcessEvidence,
    codex: CodexEvidence,
    provider: ProviderObservation,
}

#[derive(Serialize)]
struct ProcessEvidence {
    success: bool,
    exit_code: Option<i32>,
    stdout_bytes: usize,
    stdout_sha256: String,
    stdout_truncated: bool,
    stderr_bytes: usize,
    stderr_sha256: String,
    stderr_truncated: bool,
}

#[derive(Serialize)]
struct CodexEvidence {
    valid: bool,
    terminal: Option<String>,
    final_message_sha256: Option<String>,
    error: Option<String>,
    events: Option<Value>,
}

#[derive(Serialize)]
struct FixtureEvidence {
    spec_file_sha256: String,
    prepared: FixturePrepared,
    observed: FixtureObserved,
}

#[derive(Default, Serialize)]
struct ProviderObservation {
    valid: bool,
    request_count: usize,
    requests: Vec<ProviderRequest>,
    tool_search_output_present: bool,
    tool_search_output_sha256: Option<String>,
    function_call_output_present: bool,
    function_call_output_sha256: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct ProviderRequest {
    ordinal: usize,
    body_sha256: String,
    model: String,
    advertised_tools: Vec<String>,
    input_item_types: Vec<String>,
}

#[derive(Default)]
struct RequestArtifacts {
    tool_search_output_sha256: Option<String>,
    function_call_output_sha256: Option<String>,
}

struct HttpRequest {
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Codex CF-003 run spec: {error}"))?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &RunSpec) -> AppResult<()> {
    if spec.format_version != RUN_FORMAT_VERSION {
        return Err(format!(
            "unsupported Codex CF-003 format {}; expected {RUN_FORMAT_VERSION}",
            spec.format_version
        ));
    }
    validate_id("run_id", &spec.run_id)?;
    validate_id("benchmark_version", &spec.benchmark_version)?;
    if spec.case_id != CASE_ID {
        return Err(format!("case_id must be {CASE_ID:?}"));
    }
    if spec.expected_cli_version != CODEX_CLI_VERSION {
        return Err(format!(
            "expected_cli_version must pin {CODEX_CLI_VERSION:?}"
        ));
    }
    for (kind, digest) in [
        (
            "expected_product_executable_sha256",
            &spec.expected_product_executable_sha256,
        ),
        (
            "expected_fixture_spec_sha256",
            &spec.expected_fixture_spec_sha256,
        ),
    ] {
        if !is_lower_sha256(digest) {
            return Err(format!(
                "{kind} must be 64 lowercase hexadecimal characters"
            ));
        }
    }
    validate_text("workspace_snapshot", &spec.workspace_snapshot)?;
    if spec.model != CODEX_MODEL {
        return Err(format!("model must pin {CODEX_MODEL:?}"));
    }
    if !(1..=MAX_TIMEOUT_MS).contains(&spec.timeout_ms) {
        return Err(format!("timeout_ms must be 1-{MAX_TIMEOUT_MS}"));
    }
    for (kind, path) in [
        ("program", &spec.program),
        ("fixture_program", &spec.fixture_program),
        ("fixture_spec", &spec.fixture_spec),
        ("workspace", &spec.workspace),
        ("codex_home", &spec.codex_home),
    ] {
        validate_absolute_normalized_path(path, kind)?;
    }
    Ok(())
}

pub(super) async fn execute(spec: RunSpec) -> AppResult<Report> {
    let program = canonical_file(&spec.program, "Codex program")?;
    let fixture_program = canonical_file(&spec.fixture_program, "fixture program")?;
    let fixture_spec_path = canonical_file(&spec.fixture_spec, "fixture spec")?;
    let workspace = canonical_empty_directory(&spec.workspace, "workspace")?;
    let codex_home = canonical_empty_directory(&spec.codex_home, "codex_home")?;
    let fixture_spec_sha256 = sha256_file(&fixture_spec_path)?;
    if fixture_spec_sha256 != spec.expected_fixture_spec_sha256 {
        return Err(format!(
            "fixture spec digest mismatch: expected {}, observed {fixture_spec_sha256}",
            spec.expected_fixture_spec_sha256
        ));
    }
    let fixture = read_fixture_spec(&fixture_spec_path)?;
    let fixture_coordinate_sha256 = serde_json::to_vec(&fixture)
        .map(|bytes| sha256_bytes(&bytes))
        .map_err(|_| "cannot encode the fixture coordinate".to_owned())?;
    validate_fixture(&fixture, &fixture_program, &workspace, &codex_home)?;

    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Codex executable digest mismatch: expected {}, observed {product_executable_sha256}",
            spec.expected_product_executable_sha256
        ));
    }
    let fixture_executable_sha256 = sha256_file(&fixture_program)?;
    if fixture_executable_sha256 != fixture.expected_fixture_executable_sha256 {
        return Err(format!(
            "fixture executable digest mismatch: expected {}, observed {fixture_executable_sha256}",
            fixture.expected_fixture_executable_sha256
        ));
    }

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let mut environment = BTreeMap::from([
        (
            "CODEX_HOME".to_owned(),
            codex_home.to_string_lossy().into_owned(),
        ),
        (PROVIDER_TOKEN_ENV.to_owned(), PROVIDER_TOKEN.to_owned()),
    ]);
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &environment, "Codex").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Codex version mismatch: expected {:?}, observed {cli_version:?}",
            spec.expected_cli_version
        ));
    }

    let prepared: FixturePrepared = run_fixture_command(
        &broker,
        &fixture_program,
        &fixture_spec_path,
        &workspace,
        "prepare",
    )
    .await?;
    validate_prepared(
        &prepared,
        &fixture,
        &fixture_executable_sha256,
        &fixture_coordinate_sha256,
    )?;

    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| format!("cannot bind deterministic Provider: {error}"))?;
    let provider_address = listener
        .local_addr()
        .map_err(|error| format!("cannot read deterministic Provider address: {error}"))?;
    let provider_base_url = format!("http://{provider_address}/v1");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let provider_fixture = fixture.clone();
    let provider_model = spec.model.clone();
    let mut provider_task = tokio::spawn(async move {
        serve_provider(listener, provider_fixture, provider_model, shutdown_rx).await
    });

    let adapter_executable_sha256 = env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark adapter executable: {error}"))
        .and_then(|path| sha256_file(&path))?;
    let started_at_ms = now_ms();
    let started = Instant::now();
    let arguments = codex_arguments(
        &spec,
        &fixture_program,
        &fixture_spec_path,
        &provider_base_url,
    )?;
    let process_result = broker
        .execute(
            ProcessRequest {
                program,
                args: arguments,
                current_dir: workspace.clone(),
                environment: std::mem::take(&mut environment),
                stdin: USER_PROMPT.as_bytes().to_vec(),
                timeout: Duration::from_millis(spec.timeout_ms),
                max_output_bytes: MAX_OUTPUT_BYTES,
                cancellation_phase: ExecutionPhase::Model,
            },
            CancellationToken::new(),
        )
        .await;
    let wall_time_ms = elapsed_ms(started);
    let _ = shutdown_tx.send(());
    let provider = match timeout(Duration::from_secs(2), &mut provider_task).await {
        Ok(result) => {
            result.map_err(|error| format!("deterministic Provider task failed: {error}"))?
        }
        Err(_) => {
            provider_task.abort();
            let _ = provider_task.await;
            return Err("deterministic Provider did not stop".to_owned());
        }
    };

    let process = process_result.map_err(|error| format!("Codex process failed: {error}"))?;
    let codex = codex_evidence(&process.stdout, &fixture);
    let process_evidence = ProcessEvidence {
        success: process.success,
        exit_code: process.code,
        stdout_bytes: process.stdout.len(),
        stdout_sha256: sha256_bytes(&process.stdout),
        stdout_truncated: process.stdout_truncated,
        stderr_bytes: process.stderr.len(),
        stderr_sha256: sha256_bytes(&process.stderr),
        stderr_truncated: process.stderr_truncated,
    };

    let observed: FixtureObserved = run_fixture_command(
        &broker,
        &fixture_program,
        &fixture_spec_path,
        &workspace,
        "inspect",
    )
    .await?;
    validate_observed(
        &observed,
        &fixture,
        &fixture_executable_sha256,
        &fixture_coordinate_sha256,
    )?;

    let passed = process_evidence.success
        && !process_evidence.stdout_truncated
        && !process_evidence.stderr_truncated
        && codex.valid
        && provider.valid
        && observed.oracle.passed;
    Ok(Report {
        format_version: RUN_FORMAT_VERSION,
        adapter: Adapter {
            name: ADAPTER_NAME,
            version: env!("CARGO_PKG_VERSION"),
            product: "codex",
            cli_version,
            adapter_executable_sha256,
            product_executable_sha256,
            fixture_executable_sha256,
        },
        source_analysis: SourceAnalysis {
            repository: CODEX_SOURCE_REPOSITORY,
            source_ref: CODEX_SOURCE_REF,
            commit: CODEX_SOURCE_COMMIT,
            binary_source_equivalence_verified: false,
        },
        coordinate: Coordinate {
            run_id: spec.run_id,
            benchmark_version: spec.benchmark_version,
            case_id: spec.case_id,
            workspace_snapshot: spec.workspace_snapshot,
            started_at_ms,
            host_os: env::consts::OS,
            host_arch: env::consts::ARCH,
        },
        controls: Controls {
            track: "fault_conformance",
            claim_eligible: false,
            profile: "bare",
            requested_provider: "deterministic_loopback_responses",
            requested_model: spec.model,
            requested_tool: "mcp__yh_fault__commit_effect",
            permission_mode: "never_with_fixture_tool_preapproved",
            product_sandbox: "read_only",
            process_isolation: broker.descriptor().isolation,
            supplied_environment_names: ["CODEX_HOME", PROVIDER_TOKEN_ENV],
            timeout_ms: spec.timeout_ms,
            product_restart_exercised: false,
            unsupported_controls: [
                "this is one-product fault conformance evidence, not a comparative result",
                "the released binary has not been reproducibly derived from the analyzed source commit",
                "Codex built-in Tools remain advertised even though the Provider selects only the pinned MCP Tool",
                "the deterministic Provider does not measure Model reasoning quality",
                "product restart and resume are not exercised by this format",
            ],
        },
        execution: Execution {
            passed,
            wall_time_ms,
            process: process_evidence,
            codex,
            provider,
        },
        fixture: FixtureEvidence {
            spec_file_sha256: fixture_spec_sha256,
            prepared,
            observed,
        },
    })
}

fn canonical_file(path: &Path, kind: &str) -> AppResult<PathBuf> {
    let path =
        fs::canonicalize(path).map_err(|error| format!("cannot canonicalize {kind}: {error}"))?;
    if !path.is_file() {
        return Err(format!("{kind} must resolve to a regular file"));
    }
    Ok(path)
}

fn validate_absolute_normalized_path(path: &Path, kind: &str) -> AppResult<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(format!("{kind} must be an absolute normalized path"));
    }
    Ok(())
}

fn read_fixture_spec(path: &Path) -> AppResult<FixtureSpec> {
    let bytes = fs::read(path).map_err(|error| format!("cannot read fixture spec: {error}"))?;
    let fixture: FixtureSpec =
        serde_json::from_slice(&bytes).map_err(|error| format!("invalid fixture spec: {error}"))?;
    Ok(fixture)
}

fn validate_fixture(
    fixture: &FixtureSpec,
    fixture_program: &Path,
    workspace: &Path,
    codex_home: &Path,
) -> AppResult<()> {
    if fixture.format_version != 1 || fixture.case != "crash_after_first_effect" {
        return Err("fixture must be format-1 crash_after_first_effect".to_owned());
    }
    validate_id("fixture_id", &fixture.fixture_id)?;
    validate_id("operation_id", &fixture.operation_id)?;
    if !is_lower_sha256(&fixture.expected_fixture_executable_sha256)
        || !is_lower_sha256(&fixture.expected_payload_sha256)
    {
        return Err("fixture contains an invalid SHA-256 coordinate".to_owned());
    }
    validate_absolute_normalized_path(&fixture.journal, "fixture journal")?;
    let journal_parent = fixture
        .journal
        .parent()
        .ok_or_else(|| "fixture journal has no parent".to_owned())
        .and_then(|parent| {
            fs::canonicalize(parent)
                .map_err(|error| format!("cannot canonicalize fixture journal parent: {error}"))
        })?;
    let journal_name = fixture
        .journal
        .file_name()
        .ok_or_else(|| "fixture journal has no file name".to_owned())?;
    let resolved_journal = journal_parent.join(journal_name);
    if resolved_journal.starts_with(workspace) || resolved_journal.starts_with(codex_home) {
        return Err(
            "fixture journal must be outside the product workspace and CODEX_HOME".to_owned(),
        );
    }
    if resolved_journal.exists() {
        return Err("fixture journal already exists".to_owned());
    }
    if !fixture_program.is_absolute() {
        return Err("fixture program did not resolve absolutely".to_owned());
    }
    Ok(())
}

fn validate_prepared(
    report: &FixturePrepared,
    fixture: &FixtureSpec,
    executable_sha256: &str,
    spec_sha256: &str,
) -> AppResult<()> {
    if report.format_version != 1
        || report.fixture_id != fixture.fixture_id
        || report.fixture_executable_sha256 != executable_sha256
        || report.fixture_spec_sha256 != spec_sha256
        || !is_lower_sha256(&report.journal_sha256)
    {
        return Err("fixture prepare report does not match its pinned coordinates".to_owned());
    }
    if sha256_file(&fixture.journal)? != report.journal_sha256 {
        return Err("fixture prepare report does not match its journal bytes".to_owned());
    }
    Ok(())
}

fn validate_observed(
    report: &FixtureObserved,
    fixture: &FixtureSpec,
    executable_sha256: &str,
    spec_sha256: &str,
) -> AppResult<()> {
    if report.format_version != 1
        || report.track != "fixture_oracle"
        || report.claim_eligible
        || report.fixture_id != fixture.fixture_id
        || report.case != fixture.case
        || report.fixture_executable_sha256 != executable_sha256
        || report.fixture_spec_sha256 != spec_sha256
        || !is_lower_sha256(&report.journal_sha256)
        || report.invocation_count != 1
        || report.effect_count != 1
        || report.tail != "effect_committed"
        || !report.oracle.passed
        || report.oracle.classification != "uncertain_effect_not_replayed"
    {
        return Err("fixture observation does not match its pinned coordinates".to_owned());
    }
    if sha256_file(&fixture.journal)? != report.journal_sha256 {
        return Err("fixture observation does not match its journal bytes".to_owned());
    }
    Ok(())
}

async fn run_fixture_command<T: for<'de> Deserialize<'de>>(
    broker: &LocalProcessBroker,
    fixture_program: &Path,
    fixture_spec: &Path,
    workspace: &Path,
    command: &str,
) -> AppResult<T> {
    let output = broker
        .execute(
            ProcessRequest {
                program: fixture_program.to_path_buf(),
                args: vec![
                    command.to_owned(),
                    fixture_spec.to_string_lossy().into_owned(),
                ],
                current_dir: workspace.to_path_buf(),
                environment: BTreeMap::new(),
                stdin: Vec::new(),
                timeout: Duration::from_secs(10),
                max_output_bytes: 1_048_576,
                cancellation_phase: ExecutionPhase::Evaluation,
            },
            CancellationToken::new(),
        )
        .await
        .map_err(|error| format!("fixture {command} failed: {error}"))?;
    if !output.success
        || output.stdout_truncated
        || output.stderr_truncated
        || !output.stderr.is_empty()
    {
        return Err(format!("fixture {command} did not settle cleanly"));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("fixture {command} returned invalid JSON: {error}"))
}

fn codex_arguments(
    spec: &RunSpec,
    fixture_program: &Path,
    fixture_spec: &Path,
    provider_base_url: &str,
) -> AppResult<Vec<String>> {
    let provider = format!(
        "model_providers.yh_fault={{name={},base_url={},env_key={},wire_api=\"responses\",supports_websockets=false}}",
        toml_string("Y-Harness CF-003 deterministic Provider")?,
        toml_string(provider_base_url)?,
        toml_string(PROVIDER_TOKEN_ENV)?,
    );
    let fixture_args = serde_json::to_string(&vec![
        "serve",
        fixture_spec
            .to_str()
            .ok_or_else(|| "fixture spec path is not UTF-8".to_owned())?,
    ])
    .map_err(|_| "cannot encode fixture arguments".to_owned())?;
    let mcp = format!(
        "mcp_servers.{MCP_SERVER_NAME}={{command={},args={fixture_args},required=true,enabled_tools=[\"{MCP_TOOL_NAME}\"],default_tools_approval_mode=\"approve\",startup_timeout_sec=10,tool_timeout_sec=10}}",
        toml_string(
            fixture_program
                .to_str()
                .ok_or_else(|| "fixture program path is not UTF-8".to_owned())?
        )?,
    );
    Ok(vec![
        "exec".to_owned(),
        "--strict-config".to_owned(),
        "--ignore-user-config".to_owned(),
        "--ignore-rules".to_owned(),
        "--skip-git-repo-check".to_owned(),
        "--ephemeral".to_owned(),
        "--json".to_owned(),
        "--color".to_owned(),
        "never".to_owned(),
        "--sandbox".to_owned(),
        "read-only".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--config".to_owned(),
        r#"approval_policy="never""#.to_owned(),
        "--config".to_owned(),
        provider,
        "--config".to_owned(),
        r#"model_provider="yh_fault""#.to_owned(),
        "--config".to_owned(),
        r#"features.enable_request_compression=false"#.to_owned(),
        "--config".to_owned(),
        r#"features.multi_agent=false"#.to_owned(),
        "--config".to_owned(),
        format!("developer_instructions={}", toml_string(SYSTEM_PROMPT)?),
        "--config".to_owned(),
        r#"web_search="disabled""#.to_owned(),
        "--config".to_owned(),
        mcp,
        "-".to_owned(),
    ])
}

fn toml_string(value: &str) -> AppResult<String> {
    serde_json::to_string(value).map_err(|_| "cannot encode TOML string".to_owned())
}

fn codex_evidence(stdout: &[u8], fixture: &FixtureSpec) -> CodexEvidence {
    match super::codex::normalize_result(stdout) {
        Ok(result) => {
            let events = result.raw.as_array();
            let final_message = events.and_then(|events| {
                events.iter().rev().find_map(|event| {
                    event
                        .get("item")
                        .and_then(Value::as_object)
                        .filter(|item| {
                            item.get("type").and_then(Value::as_str) == Some("agent_message")
                        })
                        .and_then(|item| item.get("text"))
                        .and_then(Value::as_str)
                })
            });
            let failed_calls = events
                .into_iter()
                .flatten()
                .filter(|event| event.get("type").and_then(Value::as_str) == Some("item.completed"))
                .filter_map(|event| event.get("item"))
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("mcp_tool_call"))
                .collect::<Vec<_>>();
            let fault_observed = failed_calls.len() == 1
                && failed_calls[0].get("server").and_then(Value::as_str) == Some(MCP_SERVER_NAME)
                && failed_calls[0].get("tool").and_then(Value::as_str) == Some(MCP_TOOL_NAME)
                && failed_calls[0].get("status").and_then(Value::as_str) == Some("failed")
                && failed_calls[0]
                    .pointer("/arguments/operation_id")
                    .and_then(Value::as_str)
                    == Some(fixture.operation_id.as_str())
                && failed_calls[0]
                    .pointer("/arguments/payload_sha256")
                    .and_then(Value::as_str)
                    == Some(fixture.expected_payload_sha256.as_str())
                && failed_calls[0]
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .is_some_and(|message| message.contains("Transport closed"));
            let valid = !result.is_error && final_message == Some(FINAL_MESSAGE) && fault_observed;
            CodexEvidence {
                valid,
                terminal: Some(result.subtype.to_owned()),
                final_message_sha256: final_message.map(|message| sha256_bytes(message.as_bytes())),
                error: (!valid).then(|| {
                    "Codex did not expose the pinned failed MCP call and deterministic final message"
                        .to_owned()
                }),
                events: Some(result.raw),
            }
        }
        Err(error) => CodexEvidence {
            valid: false,
            terminal: None,
            final_message_sha256: None,
            error: Some(bounded_error(&error)),
            events: None,
        },
    }
}

async fn serve_provider(
    listener: TcpListener,
    fixture: FixtureSpec,
    model: String,
    mut shutdown: oneshot::Receiver<()>,
) -> ProviderObservation {
    let mut observation = ProviderObservation::default();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            accepted = listener.accept() => {
                let Ok((mut stream, peer)) = accepted else {
                    set_provider_error(&mut observation, "deterministic Provider accept failed");
                    break;
                };
                if !peer.ip().is_loopback() {
                    set_provider_error(&mut observation, "deterministic Provider rejected a non-loopback peer");
                    let _ = write_http_error(&mut stream, 403, "loopback only").await;
                    continue;
                }
                observation.request_count = observation.request_count.saturating_add(1);
                let ordinal = observation.request_count;
                if ordinal > MAX_PROVIDER_REQUESTS {
                    set_provider_error(&mut observation, "deterministic Provider request bound exceeded");
                    let _ = write_http_error(&mut stream, 429, "request bound exceeded").await;
                    continue;
                }
                match handle_provider_request(&mut stream, ordinal, &fixture, &model).await {
                    Ok((request, artifacts)) => {
                        observation.requests.push(request);
                        if let Some(output_sha256) = artifacts.tool_search_output_sha256 {
                            observation.tool_search_output_present = true;
                            observation.tool_search_output_sha256 = Some(output_sha256);
                        }
                        if let Some(output_sha256) = artifacts.function_call_output_sha256 {
                            observation.function_call_output_present = true;
                            observation.function_call_output_sha256 = Some(output_sha256);
                        }
                    }
                    Err(error) => {
                        set_provider_error(&mut observation, &error);
                        let _ = write_http_error(&mut stream, 400, "invalid deterministic request").await;
                    }
                }
            }
        }
    }
    observation.valid = observation.error.is_none()
        && observation.request_count == 3
        && observation.requests.len() == 3
        && observation.tool_search_output_present
        && observation.function_call_output_present;
    observation
}

fn set_provider_error(observation: &mut ProviderObservation, error: &str) {
    if observation.error.is_none() {
        observation.error = Some(bounded_error(error));
    }
}

async fn handle_provider_request(
    stream: &mut TcpStream,
    ordinal: usize,
    fixture: &FixtureSpec,
    model: &str,
) -> AppResult<(ProviderRequest, RequestArtifacts)> {
    let request = timeout(PROVIDER_IO_TIMEOUT, read_http_request(stream))
        .await
        .map_err(|_| "deterministic Provider request timed out".to_owned())??;
    if request.headers.get("authorization").map(String::as_str)
        != Some(&format!("Bearer {PROVIDER_TOKEN}"))
    {
        return Err("deterministic Provider received invalid authorization".to_owned());
    }
    let value: Value = serde_json::from_slice(&request.body)
        .map_err(|_| "deterministic Provider request body is not JSON".to_owned())?;
    let object = value
        .as_object()
        .ok_or_else(|| "deterministic Provider request must be an object".to_owned())?;
    if object.get("model").and_then(Value::as_str) != Some(model)
        || object.get("stream").and_then(Value::as_bool) != Some(true)
    {
        return Err(
            "deterministic Provider request has an unexpected Model or stream mode".to_owned(),
        );
    }
    let advertised_tools = advertised_tools(object.get("tools"))?;
    let input_item_types = input_item_types(object.get("input"))?;
    let summary = ProviderRequest {
        ordinal,
        body_sha256: sha256_bytes(&request.body),
        model: model.to_owned(),
        advertised_tools,
        input_item_types,
    };
    match ordinal {
        1 => {
            if !summary
                .advertised_tools
                .iter()
                .any(|tool| tool == "type:tool_search")
                || summary
                    .advertised_tools
                    .iter()
                    .any(|tool| tool == "mcp__yh_fault::commit_effect")
            {
                return Err(
                    "Codex did not expose the expected deferred Tool-search boundary".to_owned(),
                );
            }
            let body = sse(&[
                json!({"type":"response.created","response":{"id":"cf003-response-1"}}),
                json!({
                    "type":"response.output_item.done",
                    "item":{
                        "type":"tool_search_call",
                        "call_id":SEARCH_CALL_ID,
                        "execution":"client",
                        "arguments":{
                            "query":"Commit the pinned deterministic non-idempotent benchmark effect.",
                            "limit":8
                        }
                    }
                }),
                completed_event("cf003-response-1"),
            ])?;
            write_http_ok(stream, &body).await?;
            Ok((summary, RequestArtifacts::default()))
        }
        2 => {
            let search_output = tool_search_output(object.get("input"), SEARCH_CALL_ID)?;
            let search_output_sha256 = sha256_bytes(
                &serde_json::to_vec(search_output)
                    .map_err(|_| "cannot encode observed tool_search_output".to_owned())?,
            );
            let arguments = serde_json::to_string(&json!({
                "operation_id": fixture.operation_id,
                "payload_sha256": fixture.expected_payload_sha256,
            }))
            .map_err(|_| "cannot encode deterministic Tool arguments".to_owned())?;
            let body = sse(&[
                json!({"type":"response.created","response":{"id":"cf003-response-2"}}),
                json!({
                    "type":"response.output_item.done",
                    "item":{
                        "type":"function_call",
                        "call_id":CALL_ID,
                        "namespace":MCP_NAMESPACE,
                        "name":MCP_TOOL_NAME,
                        "arguments":arguments
                    }
                }),
                completed_event("cf003-response-2"),
            ])?;
            write_http_ok(stream, &body).await?;
            Ok((
                summary,
                RequestArtifacts {
                    tool_search_output_sha256: Some(search_output_sha256),
                    function_call_output_sha256: None,
                },
            ))
        }
        3 => {
            let output = function_call_output(object.get("input"), CALL_ID)?;
            let output_sha256 = sha256_bytes(
                &serde_json::to_vec(output)
                    .map_err(|_| "cannot encode observed function_call_output".to_owned())?,
            );
            let body = sse(&[
                json!({"type":"response.created","response":{"id":"cf003-response-3"}}),
                json!({
                    "type":"response.output_item.done",
                    "item":{
                        "type":"message",
                        "role":"assistant",
                        "id":"cf003-message-1",
                        "content":[{"type":"output_text","text":FINAL_MESSAGE}]
                    }
                }),
                completed_event("cf003-response-3"),
            ])?;
            write_http_ok(stream, &body).await?;
            Ok((
                summary,
                RequestArtifacts {
                    tool_search_output_sha256: None,
                    function_call_output_sha256: Some(output_sha256),
                },
            ))
        }
        _ => Err("Codex issued an unexpected additional Provider request".to_owned()),
    }
}

fn advertised_tools(value: Option<&Value>) -> AppResult<Vec<String>> {
    let tools = value
        .and_then(Value::as_array)
        .ok_or_else(|| "Provider request has no tools array".to_owned())?;
    let mut identities = Vec::new();
    for tool in tools {
        let object = tool
            .as_object()
            .ok_or_else(|| "Provider tool must be an object".to_owned())?;
        match object.get("type").and_then(Value::as_str) {
            Some("function") => {
                push_tool_identity(&mut identities, object.get("name").and_then(Value::as_str))?
            }
            Some("namespace") => {
                let namespace = object
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Provider namespace has no name".to_owned())?;
                let children = object
                    .get("tools")
                    .and_then(Value::as_array)
                    .ok_or_else(|| "Provider namespace has no tools".to_owned())?;
                for child in children {
                    let name = child
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| "Provider namespace Tool has no name".to_owned())?;
                    push_tool_identity(&mut identities, Some(&format!("{namespace}::{name}")))?;
                }
            }
            Some(kind) => push_tool_identity(&mut identities, Some(&format!("type:{kind}")))?,
            None => return Err("Provider tool has no type".to_owned()),
        }
    }
    Ok(identities)
}

fn push_tool_identity(identities: &mut Vec<String>, identity: Option<&str>) -> AppResult<()> {
    let identity = identity.ok_or_else(|| "Provider Tool has no identity".to_owned())?;
    validate_text("Provider Tool identity", identity)?;
    if identities.len() >= MAX_TOOL_IDENTITIES {
        return Err(format!(
            "Provider advertised more than {MAX_TOOL_IDENTITIES} Tool identities"
        ));
    }
    identities.push(identity.to_owned());
    Ok(())
}

fn input_item_types(value: Option<&Value>) -> AppResult<Vec<String>> {
    let input = value
        .and_then(Value::as_array)
        .ok_or_else(|| "Provider request has no input array".to_owned())?;
    let mut types = BTreeSet::new();
    for item in input {
        if let Some(kind) = item.get("type").and_then(Value::as_str) {
            validate_text("Provider input item type", kind)?;
            types.insert(kind.to_owned());
        }
    }
    Ok(types.into_iter().collect())
}

fn function_call_output<'a>(value: Option<&'a Value>, call_id: &str) -> AppResult<&'a Value> {
    let input = value
        .and_then(Value::as_array)
        .ok_or_else(|| "Provider request has no input array".to_owned())?;
    let outputs = input
        .iter()
        .filter(|item| {
            item.get("type").and_then(Value::as_str) == Some("function_call_output")
                && item.get("call_id").and_then(Value::as_str) == Some(call_id)
        })
        .collect::<Vec<_>>();
    if outputs.len() != 1 {
        return Err(
            "Provider request must contain exactly one pinned function_call_output".to_owned(),
        );
    }
    let output = outputs[0]
        .get("output")
        .ok_or_else(|| "function_call_output has no output".to_owned())?;
    let transport_closed = if let Some(items) = output.as_array() {
        items.len() == 1
            && matches!(
                items[0].get("type").and_then(Value::as_str),
                Some("input_text" | "text")
            )
            && items[0]
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.contains("Transport closed"))
    } else if let Some(text) = output.as_str() {
        text.contains("Transport closed")
    } else {
        return Err("pinned function_call_output has an unsupported shape".to_owned());
    };
    if !transport_closed {
        return Err(
            "pinned function_call_output does not report the MCP transport failure".to_owned(),
        );
    }
    Ok(output)
}

fn tool_search_output<'a>(value: Option<&'a Value>, call_id: &str) -> AppResult<&'a Value> {
    let input = value
        .and_then(Value::as_array)
        .ok_or_else(|| "Provider request has no input array".to_owned())?;
    let outputs = input
        .iter()
        .filter(|item| {
            item.get("type").and_then(Value::as_str) == Some("tool_search_output")
                && item.get("call_id").and_then(Value::as_str) == Some(call_id)
        })
        .collect::<Vec<_>>();
    if outputs.len() != 1 {
        return Err(
            "Provider request must contain exactly one pinned tool_search_output".to_owned(),
        );
    }
    let tools = outputs[0]
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| "tool_search_output has no tools array".to_owned())?;
    let found = tools.iter().any(|tool| {
        tool.get("type").and_then(Value::as_str) == Some("namespace")
            && tool.get("name").and_then(Value::as_str) == Some(MCP_NAMESPACE)
            && tool
                .get("tools")
                .and_then(Value::as_array)
                .is_some_and(|children| {
                    children.iter().any(|child| {
                        child.get("type").and_then(Value::as_str) == Some("function")
                            && child.get("name").and_then(Value::as_str) == Some(MCP_TOOL_NAME)
                    })
                })
    });
    if !found {
        return Err("tool_search_output did not surface the pinned MCP Tool".to_owned());
    }
    Ok(outputs[0])
}

fn completed_event(id: &str) -> Value {
    json!({
        "type":"response.completed",
        "response":{
            "id":id,
            "usage":{
                "input_tokens":0,
                "input_tokens_details":null,
                "output_tokens":0,
                "output_tokens_details":null,
                "total_tokens":0
            }
        }
    })
}

fn sse(events: &[Value]) -> AppResult<Vec<u8>> {
    let mut body = Vec::new();
    for event in events {
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "SSE event has no type".to_owned())?;
        body.extend_from_slice(b"event: ");
        body.extend_from_slice(kind.as_bytes());
        body.extend_from_slice(b"\ndata: ");
        body.extend_from_slice(
            &serde_json::to_vec(event).map_err(|_| "cannot encode SSE event".to_owned())?,
        );
        body.extend_from_slice(b"\n\n");
    }
    Ok(body)
}

async fn read_http_request(stream: &mut TcpStream) -> AppResult<HttpRequest> {
    let mut retained = Vec::new();
    let header_end = loop {
        if retained.len() > MAX_HTTP_HEADER_BYTES {
            return Err("Provider request headers exceed their byte bound".to_owned());
        }
        if let Some(index) = retained.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        let mut chunk = [0_u8; 8_192];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|error| format!("cannot read Provider request: {error}"))?;
        if read == 0 {
            return Err("Provider connection closed before headers".to_owned());
        }
        retained.extend_from_slice(&chunk[..read]);
    };
    let header_text = std::str::from_utf8(&retained[..header_end])
        .map_err(|_| "Provider request headers are not UTF-8".to_owned())?;
    let mut lines = header_text[..header_text.len() - 4].split("\r\n");
    if lines.next() != Some("POST /v1/responses HTTP/1.1") {
        return Err("Provider request has an unexpected request line".to_owned());
    }
    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "Provider request contains a malformed header".to_owned())?;
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_owned();
        if name.is_empty() || headers.insert(name, value).is_some() {
            return Err("Provider request contains an invalid or duplicate header".to_owned());
        }
    }
    if headers.contains_key("transfer-encoding")
        || headers
            .get("content-encoding")
            .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
    {
        return Err("Provider request must use an uncompressed Content-Length body".to_owned());
    }
    let content_length = headers
        .get("content-length")
        .ok_or_else(|| "Provider request has no Content-Length".to_owned())?
        .parse::<usize>()
        .map_err(|_| "Provider Content-Length is invalid".to_owned())?;
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err("Provider request body exceeds its byte bound".to_owned());
    }
    let total_length = header_end
        .checked_add(content_length)
        .ok_or_else(|| "Provider request length overflow".to_owned())?;
    if retained.len() > total_length {
        return Err("Provider request contains pipelined or excess bytes".to_owned());
    }
    while retained.len() < total_length {
        let mut chunk = [0_u8; 8_192];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|error| format!("cannot read Provider request body: {error}"))?;
        if read == 0 {
            return Err("Provider connection closed before its body".to_owned());
        }
        retained.extend_from_slice(&chunk[..read]);
        if retained.len() > total_length {
            return Err("Provider request contains pipelined or excess bytes".to_owned());
        }
    }
    Ok(HttpRequest {
        headers,
        body: retained[header_end..total_length].to_vec(),
    })
}

async fn write_http_ok(stream: &mut TcpStream, body: &[u8]) -> AppResult<()> {
    let headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    timeout(PROVIDER_IO_TIMEOUT, async {
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| "Provider response timed out".to_owned())?
    .map_err(|error| format!("cannot write Provider response: {error}"))
}

async fn write_http_error(stream: &mut TcpStream, status: u16, message: &str) -> AppResult<()> {
    let body = message.as_bytes();
    let headers = format!(
        "HTTP/1.1 {status} Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    timeout(PROVIDER_IO_TIMEOUT, async {
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| "Provider error response timed out".to_owned())?
    .map_err(|error| format!("cannot write Provider error: {error}"))
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
            run_id: "codex-cf003-1".to_owned(),
            benchmark_version: "fault-conformance-v1".to_owned(),
            case_id: CASE_ID.to_owned(),
            program: absolute_path("codex"),
            expected_cli_version: CODEX_CLI_VERSION.to_owned(),
            expected_product_executable_sha256: "a".repeat(64),
            fixture_program: absolute_path("yh-fault-fixture"),
            fixture_spec: absolute_path("fixture.json"),
            expected_fixture_spec_sha256: "b".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-directory-v1".to_owned(),
            codex_home: absolute_path("codex-home"),
            model: CODEX_MODEL.to_owned(),
            timeout_ms: 30_000,
        }
    }

    #[test]
    fn source_and_case_coordinates_are_closed() {
        let spec = valid_spec();
        validate_spec(&spec).expect("valid source-pinned spec");
        let mut wrong_version = spec.clone();
        wrong_version.expected_cli_version = "codex-cli 0.146.0".to_owned();
        assert!(validate_spec(&wrong_version).is_err());
        let mut wrong_case = spec;
        wrong_case.case_id = "another-case".to_owned();
        assert!(validate_spec(&wrong_case).is_err());
    }

    #[test]
    fn codex_arguments_isolate_profile_and_preapprove_only_the_fixture_tool() {
        let spec = valid_spec();
        let args = codex_arguments(
            &spec,
            &spec.fixture_program,
            &spec.fixture_spec,
            "http://127.0.0.1:1234/v1",
        )
        .expect("arguments");
        for flag in [
            "--strict-config",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--ephemeral",
        ] {
            assert!(args.iter().any(|argument| argument == flag));
        }
        assert!(
            args.iter()
                .any(|argument| argument.contains("default_tools_approval_mode=\"approve\""))
        );
        assert!(
            args.iter()
                .any(|argument| argument.contains("enabled_tools=[\"commit_effect\"]"))
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--config", r#"approval_policy="never""#])
        );
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--config", "features.multi_agent=false"])
        );
        assert!(!args.iter().any(|argument| argument == "--ask-for-approval"));
        assert_eq!(args.last().map(String::as_str), Some("-"));
        assert!(!args.iter().any(|argument| argument == USER_PROMPT));
    }

    #[test]
    fn provider_tool_projection_preserves_namespaces() {
        let value = json!([
            {"type":"function","name":"shell"},
            {
                "type":"namespace",
                "name":"mcp__yh_fault",
                "tools":[{"type":"function","name":"commit_effect"}]
            }
        ]);
        assert_eq!(
            advertised_tools(Some(&value)).expect("tools"),
            ["shell", "mcp__yh_fault::commit_effect"]
        );
    }

    #[test]
    fn provider_requires_the_exact_function_call_output() {
        let value = json!([
            {
                "type":"function_call_output",
                "call_id":CALL_ID,
                "output":[{
                    "type":"input_text",
                    "text":"tool call error: Transport closed"
                }]
            }
        ]);
        assert_eq!(
            function_call_output(Some(&value), CALL_ID).expect("output"),
            &json!([{"type":"input_text","text":"tool call error: Transport closed"}])
        );
        assert!(function_call_output(Some(&value), "another-call").is_err());
        let successful = json!([
            {
                "type":"function_call_output",
                "call_id":CALL_ID,
                "output":[{"type":"input_text","text":"completed"}]
            }
        ]);
        assert!(function_call_output(Some(&successful), CALL_ID).is_err());
        let codex_text = json!([
            {
                "type":"function_call_output",
                "call_id":CALL_ID,
                "output":"[{\"type\":\"text\",\"text\":\"tool call error: Transport closed\"}]"
            }
        ]);
        assert!(function_call_output(Some(&codex_text), CALL_ID).is_ok());
    }

    #[test]
    fn provider_requires_search_to_surface_the_pinned_mcp_tool() {
        let value = json!([
            {
                "type":"tool_search_output",
                "call_id":SEARCH_CALL_ID,
                "tools":[{
                    "type":"namespace",
                    "name":"mcp__yh_fault",
                    "tools":[{"type":"function","name":"commit_effect"}]
                }]
            }
        ]);
        assert!(tool_search_output(Some(&value), SEARCH_CALL_ID).is_ok());
        assert!(tool_search_output(Some(&value), "another-search").is_err());
    }

    #[test]
    fn checked_in_fault_evidence_retains_product_provider_and_oracle_boundaries() {
        let report: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-codex-cf003-probe/result.json"
        ))
        .expect("checked-in Codex fault report");
        let fixture_spec =
            include_bytes!("../evidence/2026-07-28-codex-cf003-probe/fixture-spec.json");
        let journal = include_bytes!("../evidence/2026-07-28-codex-cf003-probe/journal.jsonl");
        let fixture: FixtureSpec =
            serde_json::from_slice(fixture_spec).expect("checked-in fixture spec");

        assert_eq!(report["format_version"], RUN_FORMAT_VERSION);
        assert_eq!(report["controls"]["claim_eligible"], false);
        assert_eq!(report["controls"]["product_restart_exercised"], false);
        assert_eq!(report["execution"]["passed"], true);
        assert_eq!(report["execution"]["provider"]["request_count"], 3);
        assert_eq!(
            report["execution"]["provider"]["tool_search_output_present"],
            true
        );
        assert_eq!(
            report["execution"]["provider"]["function_call_output_present"],
            true
        );
        assert_eq!(report["fixture"]["observed"]["invocation_count"], 1);
        assert_eq!(report["fixture"]["observed"]["effect_count"], 1);
        assert_eq!(
            report["fixture"]["observed"]["oracle"]["classification"],
            "uncertain_effect_not_replayed"
        );
        assert_eq!(
            report["fixture"]["spec_file_sha256"],
            sha256_bytes(fixture_spec)
        );
        assert_eq!(
            report["fixture"]["observed"]["journal_sha256"],
            sha256_bytes(journal)
        );
        let mut product_stdout = Vec::new();
        for event in report["execution"]["codex"]["events"]
            .as_array()
            .expect("Codex events")
        {
            serde_json::to_writer(&mut product_stdout, event).expect("encode Codex event");
            product_stdout.push(b'\n');
        }
        assert!(codex_evidence(&product_stdout, &fixture).valid);

        let records = journal
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice::<Value>(line).expect("journal record"))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        assert_eq!(records[1]["type"], "invocation_started");
        assert_eq!(records[2]["type"], "effect_committed");
    }
}
