//! Process-level Y-Harness restart proof for the deterministic CF-003 fixture.

use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    process::{ExitStatus, Stdio},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    task::JoinHandle,
    time::{sleep, timeout},
};
use y_harness::{
    CompatibilityManifest, ItemKind, OperationId, OperationStatus, PROTOCOL_VERSION,
    ProtocolCommand, ProtocolRequest, ProtocolResponse, ProtocolResponseBody, ProtocolResult,
    Thread, TurnStatus,
};

use super::*;

const RUN_FORMAT_VERSION: u32 = 9;
const CASE_ID: &str = "cf-003-y-harness-restart-after-uncertain-effect";
const ADAPTER_NAME: &str = "y-harness-cf003-restart-stdio-v1";
const MODEL_ID: &str = "fixture/cf003";
const MCP_SERVER_ID: &str = "fault";
const MCP_NAMESPACE: &str = "fault";
const MCP_REMOTE_TOOL: &str = "commit_effect";
const REGISTERED_TOOL: &str = "fault.commit_effect";
const MAX_PROTOCOL_FRAME_BYTES: usize = 2_097_152;
const MAX_CAPTURE_BYTES: usize = 1_048_576;
const MAX_POLL_ATTEMPTS: usize = 1_000;

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
    timeout_ms: u64,
    effect_wait_timeout_ms: u64,
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
    model: ModelFixtureSpec,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ModelFixtureSpec {
    call_id: String,
    registered_tool_name: String,
    trigger_prompt: String,
    post_restart_prompt: String,
    post_restart_message: String,
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
    adapter: AdapterEvidence,
    coordinate: Coordinate,
    controls: Controls,
    execution: Execution,
    fixture: FixtureEvidence,
}

#[derive(Serialize)]
struct AdapterEvidence {
    name: &'static str,
    version: &'static str,
    product: &'static str,
    cli_version: String,
    adapter_executable_sha256: String,
    product_executable_sha256: String,
    fixture_executable_sha256: String,
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
    model: &'static str,
    tool: &'static str,
    process_isolation: ProcessIsolation,
    timeout_ms: u64,
    effect_wait_timeout_ms: u64,
    product_restart_exercised: bool,
    interrupted_turn_continued_in_place: bool,
    model_reasoning_measured: bool,
}

#[derive(Serialize)]
struct Execution {
    passed: bool,
    wall_time_ms: u64,
    configuration_sha256: String,
    compatibility: CompatibilityManifest,
    thread_id: String,
    interruption: InterruptionEvidence,
    state: StateEvidence,
    processes: Vec<ProcessEvidence>,
}

#[derive(Serialize)]
struct InterruptionEvidence {
    effect_boundary_observed: bool,
    product_process_killed: bool,
    descendant_process_group_cleanup_claimed: bool,
    detached_fixture_release_requested: bool,
    detached_fixture_release_marker_persisted: bool,
    release_marker_sha256: String,
}

#[derive(Serialize)]
struct StateEvidence {
    pre_kill_turn_status: &'static str,
    restart_pre_recovery_turn_status: &'static str,
    explicit_recovery_requested: bool,
    recovered_turn_id: String,
    recovered_turn_status: &'static str,
    recovered_tool_call_count: usize,
    recovered_tool_result_count: usize,
    recovery_replayed_tool: bool,
    post_restart_turn_id: String,
    post_restart_turn_status: &'static str,
    post_restart_final_text: String,
    same_thread_after_restart: bool,
}

#[derive(Serialize)]
struct ProcessEvidence {
    phase: &'static str,
    controller_killed: bool,
    success: bool,
    exit_code: Option<i32>,
    stdout_bytes: u64,
    stdout_sha256: String,
    stdout_truncated: bool,
    stderr_bytes: u64,
    stderr_sha256: String,
    stderr_truncated: bool,
}

#[derive(Serialize)]
struct FixtureEvidence {
    spec_file_sha256: String,
    prepared: FixturePrepared,
    after_interruption: FixtureObserved,
    after_restart: FixtureObserved,
}

struct ServiceSession {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
    stderr_task: JoinHandle<AppResult<StreamCapture>>,
    stdout_hasher: Sha256,
    stdout_bytes: u64,
}

struct StreamCapture {
    bytes: u64,
    sha256: String,
    truncated: bool,
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Y-Harness CF-003 restart run spec: {error}"))?;
    validate_spec(&spec)?;
    Ok(spec)
}

fn validate_spec(spec: &RunSpec) -> AppResult<()> {
    if spec.format_version != RUN_FORMAT_VERSION {
        return Err(format!(
            "unsupported Y-Harness CF-003 format {}; expected {RUN_FORMAT_VERSION}",
            spec.format_version
        ));
    }
    validate_id("run_id", &spec.run_id)?;
    validate_id("benchmark_version", &spec.benchmark_version)?;
    if spec.case_id != CASE_ID {
        return Err(format!("case_id must be {CASE_ID}"));
    }
    validate_text("expected_cli_version", &spec.expected_cli_version)?;
    validate_text("workspace_snapshot", &spec.workspace_snapshot)?;
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
            return Err(format!("{kind} must be 64 lowercase hexadecimal bytes"));
        }
    }
    if !(1..=MAX_TIMEOUT_MS).contains(&spec.timeout_ms)
        || !(1..=MAX_TIMEOUT_MS).contains(&spec.effect_wait_timeout_ms)
        || spec.effect_wait_timeout_ms > spec.timeout_ms
    {
        return Err(format!(
            "timeouts must be 1-{MAX_TIMEOUT_MS} ms and effect_wait_timeout_ms must not exceed timeout_ms"
        ));
    }
    for (kind, path) in [
        ("program", &spec.program),
        ("fixture_program", &spec.fixture_program),
        ("fixture_spec", &spec.fixture_spec),
        ("workspace", &spec.workspace),
    ] {
        validate_absolute_normalized_path(path, kind)?;
    }
    Ok(())
}

pub(super) async fn execute(spec: RunSpec) -> AppResult<Report> {
    let program = canonical_file(&spec.program, "Y-Harness program")?;
    let fixture_program = canonical_file(&spec.fixture_program, "fault fixture program")?;
    let fixture_spec_path = canonical_file(&spec.fixture_spec, "fault fixture spec")?;
    let workspace = canonical_empty_directory(&spec.workspace, "Y-Harness fault workspace")?;
    let fixture_spec_file_sha256 = sha256_file(&fixture_spec_path)?;
    if fixture_spec_file_sha256 != spec.expected_fixture_spec_sha256 {
        return Err(format!(
            "fixture spec digest mismatch: expected {}, observed {fixture_spec_file_sha256}",
            spec.expected_fixture_spec_sha256
        ));
    }
    let fixture = read_fixture_spec(&fixture_spec_path)?;
    validate_fixture(&fixture, &fixture_program, &workspace)?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Y-Harness executable digest mismatch: expected {}, observed {product_executable_sha256}",
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
    let broker = LocalProcessBroker::new(1)
        .map_err(|error| format!("cannot create fixture process broker: {error}"))?;
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &BTreeMap::new(), "Y-Harness").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Y-Harness version mismatch: expected {:?}, observed {cli_version:?}",
            spec.expected_cli_version
        ));
    }
    let fixture_coordinate_sha256 = sha256_bytes(
        &serde_json::to_vec(&fixture)
            .map_err(|_| "cannot encode the validated fixture coordinate".to_owned())?,
    );
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

    let config_path = workspace.join("y-harness.json");
    let configuration = service_configuration(
        &fixture_program,
        &fixture_spec_path,
        &fixture_executable_sha256,
        spec.timeout_ms,
    );
    let configuration_bytes = serde_json::to_vec_pretty(&configuration)
        .map_err(|_| "cannot encode Y-Harness fault configuration".to_owned())?;
    write_new_file(
        &config_path,
        &configuration_bytes,
        "Y-Harness fault configuration",
    )?;
    let configuration_sha256 = sha256_bytes(&configuration_bytes);
    let adapter_executable_sha256 = env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark adapter executable: {error}"))
        .and_then(|path| sha256_file(&path))?;
    let started_at_ms = now_ms();
    let started = Instant::now();
    let phase_timeout = Duration::from_millis(spec.timeout_ms);

    let mut setup = ServiceSession::spawn(&program, &config_path, &workspace).await?;
    let compatibility = initialize(&mut setup, "setup-initialize", phase_timeout).await?;
    let thread = create_thread(&mut setup, "setup-create-thread", phase_timeout).await?;
    let setup_process = setup.finish("setup", false, phase_timeout).await?;
    require_clean_process(&setup_process)?;

    let mut fault = ServiceSession::spawn(&program, &config_path, &workspace).await?;
    let fault_compatibility = initialize(&mut fault, "fault-initialize", phase_timeout).await?;
    if fault_compatibility != compatibility {
        return Err("Y-Harness compatibility changed between service processes".to_owned());
    }
    let _operation_id = start_turn(
        &mut fault,
        &thread.id.to_string(),
        &fixture.model.trigger_prompt,
        "fault-start",
        spec.timeout_ms,
        phase_timeout,
    )
    .await?;
    let boundary =
        wait_for_effect_boundary(&fixture, Duration::from_millis(spec.effect_wait_timeout_ms))
            .await;
    let pre_kill = if boundary.is_ok() {
        get_thread(
            &mut fault,
            &thread.id.to_string(),
            "fault-thread",
            phase_timeout,
        )
        .await
    } else {
        Err("effect boundary was not observed".to_owned())
    };
    let fault_process = fault.finish("fault", true, phase_timeout).await;
    let release = release_fixture(&fixture);
    boundary?;
    let pre_kill = pre_kill?;
    let fault_process = fault_process?;
    if fault_process.success || !fault_process.controller_killed {
        return Err("fault service did not settle by controller kill".to_owned());
    }
    validate_pre_kill_thread(&pre_kill, &fixture)?;
    let release_marker_sha256 = release?;
    let after_interruption = wait_for_fixture_observation(
        &broker,
        &fixture_program,
        &fixture_spec_path,
        &workspace,
        Duration::from_secs(2),
    )
    .await?;
    validate_observed(
        &after_interruption,
        &fixture,
        &fixture_executable_sha256,
        &fixture_coordinate_sha256,
    )?;

    let mut restarted = ServiceSession::spawn(&program, &config_path, &workspace).await?;
    let restart_compatibility =
        initialize(&mut restarted, "restart-initialize", phase_timeout).await?;
    if restart_compatibility != compatibility {
        return Err("Y-Harness compatibility changed after restart".to_owned());
    }
    let before_recovery = get_thread(
        &mut restarted,
        &thread.id.to_string(),
        "restart-running-thread",
        phase_timeout,
    )
    .await?;
    validate_pre_kill_thread(&before_recovery, &fixture)?;
    let recovered = recover_thread(
        &mut restarted,
        &thread.id.to_string(),
        &pre_kill.turns[0].id.to_string(),
        "restart-recover-thread",
        phase_timeout,
    )
    .await?;
    let recovered_turn_id = validate_recovered_thread(&recovered, &pre_kill, &fixture)?;
    let audit_operation = start_turn(
        &mut restarted,
        &thread.id.to_string(),
        &fixture.model.post_restart_prompt,
        "restart-audit-start",
        spec.timeout_ms,
        phase_timeout,
    )
    .await?;
    let (audit_turn_id, final_text) = poll_operation(
        &mut restarted,
        &audit_operation,
        "restart-audit",
        phase_timeout,
    )
    .await?;
    if final_text != fixture.model.post_restart_message {
        return Err("post-restart Turn returned an unexpected final message".to_owned());
    }
    let final_thread = get_thread(
        &mut restarted,
        &thread.id.to_string(),
        "restart-final-thread",
        phase_timeout,
    )
    .await?;
    validate_final_thread(
        &final_thread,
        &fixture,
        &recovered_turn_id,
        &audit_turn_id.to_string(),
    )?;
    let restart_process = restarted.finish("restart", false, phase_timeout).await?;
    require_clean_process(&restart_process)?;

    let after_restart: FixtureObserved = run_fixture_command(
        &broker,
        &fixture_program,
        &fixture_spec_path,
        &workspace,
        "inspect",
    )
    .await?;
    validate_observed(
        &after_restart,
        &fixture,
        &fixture_executable_sha256,
        &fixture_coordinate_sha256,
    )?;

    let passed = after_interruption.oracle.passed
        && after_restart.oracle.passed
        && recovered.id == thread.id
        && final_thread.id == thread.id;
    Ok(Report {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_NAME,
            version: env!("CARGO_PKG_VERSION"),
            product: "y-harness",
            cli_version,
            adapter_executable_sha256,
            product_executable_sha256,
            fixture_executable_sha256,
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
            profile: "deterministic_json_command_model_and_stdio_mcp",
            model: MODEL_ID,
            tool: REGISTERED_TOOL,
            process_isolation: ProcessIsolation::Unrestricted,
            timeout_ms: spec.timeout_ms,
            effect_wait_timeout_ms: spec.effect_wait_timeout_ms,
            product_restart_exercised: true,
            interrupted_turn_continued_in_place: false,
            model_reasoning_measured: false,
        },
        execution: Execution {
            passed,
            wall_time_ms: elapsed_ms(started),
            configuration_sha256,
            compatibility,
            thread_id: thread.id.to_string(),
            interruption: InterruptionEvidence {
                effect_boundary_observed: true,
                product_process_killed: true,
                descendant_process_group_cleanup_claimed: false,
                detached_fixture_release_requested: true,
                detached_fixture_release_marker_persisted: true,
                release_marker_sha256,
            },
            state: StateEvidence {
                pre_kill_turn_status: "running",
                restart_pre_recovery_turn_status: "running",
                explicit_recovery_requested: true,
                recovered_turn_id,
                recovered_turn_status: "interrupted",
                recovered_tool_call_count: 1,
                recovered_tool_result_count: 0,
                recovery_replayed_tool: false,
                post_restart_turn_id: audit_turn_id.to_string(),
                post_restart_turn_status: "completed",
                post_restart_final_text: final_text,
                same_thread_after_restart: true,
            },
            processes: vec![setup_process, fault_process, restart_process],
        },
        fixture: FixtureEvidence {
            spec_file_sha256: fixture_spec_file_sha256,
            prepared,
            after_interruption,
            after_restart,
        },
    })
}

fn service_configuration(
    fixture_program: &Path,
    fixture_spec: &Path,
    fixture_sha256: &str,
    timeout_ms: u64,
) -> Value {
    json!({
        "schema_version": 1,
        "data_directory": ".y-harness",
        "max_parallel_tool_calls": 1,
        "model": {
            "type": "json_command",
            "id": MODEL_ID,
            "process": {
                "command": fixture_program,
                "args": ["model", fixture_spec],
                "current_directory": ".",
                "timeout_ms": timeout_ms,
                "max_output_bytes": 1048576,
                "launch": {
                    "type": "unrestricted",
                    "max_concurrency": 1
                }
            }
        },
        "mcp_servers": [{
            "id": MCP_SERVER_ID,
            "command": fixture_program,
            "command_sha256": fixture_sha256,
            "args": ["serve", fixture_spec],
            "current_directory": ".",
            "request_timeout_ms": timeout_ms,
            "launch": {
                "type": "unrestricted",
                "max_concurrency": 1
            },
            "tools": {
                "namespace": MCP_NAMESPACE,
                "allow": [MCP_REMOTE_TOOL]
            }
        }]
    })
}

impl ServiceSession {
    async fn spawn(program: &Path, config: &Path, workspace: &Path) -> AppResult<Self> {
        let mut command = Command::new(program);
        command
            .arg("serve")
            .arg(config)
            .current_dir(workspace)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| format!("cannot spawn Y-Harness service: {error}"))?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| "Y-Harness service has no stdin".to_owned())?;
        let output = child
            .stdout
            .take()
            .ok_or_else(|| "Y-Harness service has no stdout".to_owned())?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| "Y-Harness service has no stderr".to_owned())?;
        let stderr_task = tokio::spawn(capture_stream(stderr));
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
            stderr_task,
            stdout_hasher: Sha256::new(),
            stdout_bytes: 0,
        })
    }

    async fn exchange(
        &mut self,
        request: ProtocolRequest,
        maximum_wait: Duration,
    ) -> AppResult<ProtocolResponse> {
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|_| "cannot encode Y-Harness protocol request".to_owned())?;
        if encoded.len() > MAX_PROTOCOL_FRAME_BYTES {
            return Err("Y-Harness protocol request exceeds the controller bound".to_owned());
        }
        encoded.push(b'\n');
        let input = self
            .input
            .as_mut()
            .ok_or_else(|| "Y-Harness service stdin is closed".to_owned())?;
        timeout(maximum_wait, async {
            input.write_all(&encoded).await?;
            input.flush().await
        })
        .await
        .map_err(|_| "Y-Harness protocol write timed out".to_owned())?
        .map_err(|error| format!("cannot write Y-Harness protocol request: {error}"))?;
        let frame = timeout(
            maximum_wait,
            read_bounded_frame(&mut self.output, MAX_PROTOCOL_FRAME_BYTES),
        )
        .await
        .map_err(|_| "Y-Harness protocol response timed out".to_owned())??;
        self.stdout_bytes = self
            .stdout_bytes
            .checked_add(u64::try_from(frame.len()).unwrap_or(u64::MAX))
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(|| "Y-Harness stdout byte count overflow".to_owned())?;
        self.stdout_hasher.update(&frame);
        self.stdout_hasher.update(b"\n");
        let response: ProtocolResponse = serde_json::from_slice(&frame)
            .map_err(|_| "Y-Harness service returned invalid protocol JSON".to_owned())?;
        if response.id.as_deref() != Some(request.id.as_str())
            || response.protocol_version != PROTOCOL_VERSION
        {
            return Err("Y-Harness service returned a mismatched response envelope".to_owned());
        }
        Ok(response)
    }

    async fn finish(
        mut self,
        phase: &'static str,
        controller_killed: bool,
        maximum_wait: Duration,
    ) -> AppResult<ProcessEvidence> {
        self.input.take();
        if controller_killed {
            self.child
                .start_kill()
                .map_err(|error| format!("cannot kill Y-Harness {phase} process: {error}"))?;
        }
        let status = match timeout(maximum_wait, self.child.wait()).await {
            Ok(result) => {
                result.map_err(|error| format!("cannot wait for Y-Harness {phase}: {error}"))?
            }
            Err(_) => {
                let _ = self.child.start_kill();
                let _ = timeout(Duration::from_secs(2), self.child.wait()).await;
                return Err(format!("Y-Harness {phase} process did not settle"));
            }
        };
        let remaining = capture_stream(self.output.into_inner()).await?;
        let stderr = self
            .stderr_task
            .await
            .map_err(|error| format!("Y-Harness {phase} stderr task failed: {error}"))??;
        if remaining.bytes != 0 {
            return Err(format!(
                "Y-Harness {phase} emitted an unsolicited protocol response"
            ));
        }
        Ok(process_evidence(
            phase,
            controller_killed,
            status,
            self.stdout_bytes,
            lower_hex(&self.stdout_hasher.finalize()),
            stderr,
        ))
    }
}

fn process_evidence(
    phase: &'static str,
    controller_killed: bool,
    status: ExitStatus,
    stdout_bytes: u64,
    stdout_sha256: String,
    stderr: StreamCapture,
) -> ProcessEvidence {
    ProcessEvidence {
        phase,
        controller_killed,
        success: status.success(),
        exit_code: status.code(),
        stdout_bytes,
        stdout_sha256,
        stdout_truncated: false,
        stderr_bytes: stderr.bytes,
        stderr_sha256: stderr.sha256,
        stderr_truncated: stderr.truncated,
    }
}

async fn capture_stream(mut input: impl AsyncRead + Unpin) -> AppResult<StreamCapture> {
    let mut hasher = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .await
            .map_err(|error| format!("cannot read child process stream: {error}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| "child process stream byte count overflow".to_owned())?;
    }
    Ok(StreamCapture {
        bytes,
        sha256: lower_hex(&hasher.finalize()),
        truncated: bytes > u64::try_from(MAX_CAPTURE_BYTES).unwrap_or(u64::MAX),
    })
}

async fn read_bounded_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    maximum: usize,
) -> AppResult<Vec<u8>> {
    let mut frame = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|error| format!("cannot read Y-Harness protocol response: {error}"))?;
        if available.is_empty() {
            return Err("Y-Harness service ended before sending a complete response".to_owned());
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let content = newline.unwrap_or(available.len());
        if frame.len().saturating_add(content) > maximum {
            return Err("Y-Harness protocol response exceeds the controller bound".to_owned());
        }
        frame.extend_from_slice(&available[..content]);
        let consumed = content.saturating_add(usize::from(newline.is_some()));
        reader.consume(consumed);
        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(frame);
        }
    }
}

async fn initialize(
    session: &mut ServiceSession,
    id: &str,
    maximum_wait: Duration,
) -> AppResult<CompatibilityManifest> {
    match success(
        session
            .exchange(request(id, ProtocolCommand::Initialize {}), maximum_wait)
            .await?,
    )? {
        ProtocolResult::Initialized {
            server,
            compatibility,
            ..
        } if server == "Y-Harness Engineering" => Ok(compatibility),
        _ => Err("Y-Harness initialize returned an unexpected result".to_owned()),
    }
}

async fn create_thread(
    session: &mut ServiceSession,
    id: &str,
    maximum_wait: Duration,
) -> AppResult<Thread> {
    match success(
        session
            .exchange(request(id, ProtocolCommand::CreateThread {}), maximum_wait)
            .await?,
    )? {
        ProtocolResult::ThreadCreated { thread } => Ok(thread),
        _ => Err("Y-Harness create Thread returned an unexpected result".to_owned()),
    }
}

async fn get_thread(
    session: &mut ServiceSession,
    thread_id: &str,
    id: &str,
    maximum_wait: Duration,
) -> AppResult<Thread> {
    match success(
        session
            .exchange(
                request(
                    id,
                    ProtocolCommand::GetThread {
                        thread_id: thread_id.to_owned(),
                    },
                ),
                maximum_wait,
            )
            .await?,
    )? {
        ProtocolResult::Thread {
            thread: Some(thread),
        } => Ok(thread),
        _ => Err("Y-Harness get Thread returned an unexpected result".to_owned()),
    }
}

async fn recover_thread(
    session: &mut ServiceSession,
    thread_id: &str,
    expected_turn_id: &str,
    id: &str,
    maximum_wait: Duration,
) -> AppResult<Thread> {
    match success(
        session
            .exchange(
                request(
                    id,
                    ProtocolCommand::RecoverThread {
                        thread_id: thread_id.to_owned(),
                        expected_turn_id: expected_turn_id.to_owned(),
                    },
                ),
                maximum_wait,
            )
            .await?,
    )? {
        ProtocolResult::ThreadRecovered {
            thread: Some(thread),
        } => Ok(thread),
        _ => Err("Y-Harness recover Thread returned an unexpected result".to_owned()),
    }
}

async fn start_turn(
    session: &mut ServiceSession,
    thread_id: &str,
    prompt: &str,
    id: &str,
    timeout_ms: u64,
    maximum_wait: Duration,
) -> AppResult<OperationId> {
    match success(
        session
            .exchange(
                request(
                    id,
                    ProtocolCommand::StartTurn {
                        thread_id: thread_id.to_owned(),
                        prompt: prompt.to_owned(),
                        memory_scope: Default::default(),
                        context: Vec::new(),
                        timeout_ms: Some(timeout_ms),
                        approval_wait_ttl_ms: None,
                    },
                ),
                maximum_wait,
            )
            .await?,
    )? {
        ProtocolResult::TurnStarted { operation_id } => Ok(operation_id),
        _ => Err("Y-Harness start Turn returned an unexpected result".to_owned()),
    }
}

async fn poll_operation(
    session: &mut ServiceSession,
    operation_id: &OperationId,
    id_prefix: &str,
    maximum_wait: Duration,
) -> AppResult<(y_harness::TurnId, String)> {
    for attempt in 0..MAX_POLL_ATTEMPTS {
        let result = success(
            session
                .exchange(
                    request(
                        &format!("{id_prefix}-{attempt}"),
                        ProtocolCommand::GetOperation {
                            operation_id: operation_id.to_string(),
                        },
                    ),
                    maximum_wait,
                )
                .await?,
        )?;
        match result {
            ProtocolResult::Operation {
                operation: OperationStatus::Running { .. },
            } => sleep(Duration::from_millis(5)).await,
            ProtocolResult::Operation {
                operation:
                    OperationStatus::Completed {
                        turn_id,
                        final_text,
                        ..
                    },
            } => return Ok((turn_id, final_text)),
            ProtocolResult::Operation { operation } => {
                return Err(format!(
                    "post-restart operation settled unexpectedly: {operation:?}"
                ));
            }
            _ => return Err("Y-Harness operation poll returned an unexpected result".to_owned()),
        }
    }
    Err("post-restart operation did not settle within its poll bound".to_owned())
}

fn request(id: &str, command: ProtocolCommand) -> ProtocolRequest {
    ProtocolRequest {
        id: id.to_owned(),
        protocol_version: PROTOCOL_VERSION.to_owned(),
        command,
    }
}

fn success(response: ProtocolResponse) -> AppResult<ProtocolResult> {
    match response.body {
        ProtocolResponseBody::Success { result } => Ok(result),
        ProtocolResponseBody::Error { error } => Err(format!(
            "Y-Harness protocol error {}: {}",
            error.code, error.message
        )),
    }
}

fn validate_pre_kill_thread(thread: &Thread, fixture: &FixtureSpec) -> AppResult<()> {
    let turn = thread
        .turns
        .as_slice()
        .first()
        .filter(|_| thread.turns.len() == 1)
        .ok_or_else(|| "pre-kill Thread must contain exactly one Turn".to_owned())?;
    if turn.status != TurnStatus::Running {
        return Err("pre-kill Turn is not running at the effect boundary".to_owned());
    }
    validate_uncertain_call(turn.items.iter().map(|item| &item.kind), fixture)
}

fn validate_recovered_thread(
    recovered: &Thread,
    pre_kill: &Thread,
    fixture: &FixtureSpec,
) -> AppResult<String> {
    if recovered.id != pre_kill.id || recovered.turns.len() != 1 {
        return Err("restart did not recover the exact one-Turn Thread".to_owned());
    }
    let turn = &recovered.turns[0];
    if turn.id != pre_kill.turns[0].id || turn.status != TurnStatus::Interrupted {
        return Err("restart did not mark the exact unfinished Turn interrupted".to_owned());
    }
    validate_uncertain_call(turn.items.iter().map(|item| &item.kind), fixture)?;
    Ok(turn.id.to_string())
}

fn validate_uncertain_call<'a>(
    items: impl Iterator<Item = &'a ItemKind>,
    fixture: &FixtureSpec,
) -> AppResult<()> {
    let items = items.collect::<Vec<_>>();
    let calls = items
        .iter()
        .filter_map(|item| match item {
            ItemKind::ToolCall {
                model_id,
                call_id,
                name,
                input,
                ..
            } => Some((model_id, call_id, name, input)),
            _ => None,
        })
        .collect::<Vec<_>>();
    if calls.len() != 1
        || calls[0].0.as_deref() != Some(MODEL_ID)
        || calls[0].1 != &fixture.model.call_id
        || calls[0].2 != REGISTERED_TOOL
        || calls[0].3.get("operation_id").and_then(Value::as_str) != Some(&fixture.operation_id)
        || calls[0].3.get("payload_sha256").and_then(Value::as_str)
            != Some(&fixture.expected_payload_sha256)
    {
        return Err("Thread does not retain the exact pinned Tool call".to_owned());
    }
    if items
        .iter()
        .any(|item| matches!(item, ItemKind::ToolResult { .. }))
    {
        return Err("uncertain Tool call unexpectedly has a durable Tool result".to_owned());
    }
    Ok(())
}

fn validate_final_thread(
    thread: &Thread,
    fixture: &FixtureSpec,
    recovered_turn_id: &str,
    audit_turn_id: &str,
) -> AppResult<()> {
    if thread.turns.len() != 2
        || thread.turns[0].id.as_str() != recovered_turn_id
        || thread.turns[0].status != TurnStatus::Interrupted
        || thread.turns[1].id.as_str() != audit_turn_id
        || thread.turns[1].status != TurnStatus::Completed
    {
        return Err(
            "final Thread does not preserve interrupted then completed Turn order".to_owned(),
        );
    }
    validate_uncertain_call(thread.turns[0].items.iter().map(|item| &item.kind), fixture)?;
    let audit = &thread.turns[1];
    if audit.items.iter().any(|item| {
        matches!(
            item.kind,
            ItemKind::ToolCall { .. } | ItemKind::ToolResult { .. }
        )
    }) {
        return Err("post-restart audit Turn invoked a Tool".to_owned());
    }
    let assistant = audit.items.iter().filter_map(|item| match &item.kind {
        ItemKind::AssistantMessage {
            model_id, content, ..
        } => Some((model_id, content)),
        _ => None,
    });
    let assistant = assistant.collect::<Vec<_>>();
    if assistant.len() != 1
        || assistant[0].0.as_deref() != Some(MODEL_ID)
        || assistant[0].1 != &fixture.model.post_restart_message
    {
        return Err("post-restart audit Turn has unexpected assistant evidence".to_owned());
    }
    Ok(())
}

fn require_clean_process(process: &ProcessEvidence) -> AppResult<()> {
    if !process.success
        || process.controller_killed
        || process.stdout_truncated
        || process.stderr_truncated
        || process.stderr_bytes != 0
    {
        return Err(format!(
            "Y-Harness {} process did not settle cleanly",
            process.phase
        ));
    }
    Ok(())
}

fn read_fixture_spec(path: &Path) -> AppResult<FixtureSpec> {
    let bytes = fs::read(path).map_err(|error| format!("cannot read fixture spec: {error}"))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("invalid fixture spec: {error}"))
}

fn validate_fixture(
    fixture: &FixtureSpec,
    fixture_program: &Path,
    workspace: &Path,
) -> AppResult<()> {
    if fixture.format_version != 1 || fixture.case != "hold_after_first_effect" {
        return Err("fixture must be format-1 hold_after_first_effect".to_owned());
    }
    validate_id("fixture_id", &fixture.fixture_id)?;
    validate_id("operation_id", &fixture.operation_id)?;
    validate_id("model.call_id", &fixture.model.call_id)?;
    if fixture.model.registered_tool_name != REGISTERED_TOOL {
        return Err(format!(
            "fixture Model registered_tool_name must be {REGISTERED_TOOL}"
        ));
    }
    for (kind, value) in [
        ("model.trigger_prompt", &fixture.model.trigger_prompt),
        (
            "model.post_restart_prompt",
            &fixture.model.post_restart_prompt,
        ),
        (
            "model.post_restart_message",
            &fixture.model.post_restart_message,
        ),
    ] {
        validate_text(kind, value)?;
    }
    if fixture.model.trigger_prompt == fixture.model.post_restart_prompt {
        return Err("fixture Model prompts must be distinct".to_owned());
    }
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
    let journal = journal_parent.join(journal_name);
    if journal.starts_with(workspace) {
        return Err("fixture journal must be outside the product workspace".to_owned());
    }
    if journal.exists() {
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
        || sha256_file(&fixture.journal)? != report.journal_sha256
    {
        return Err("fixture prepare report does not match its pinned coordinates".to_owned());
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
        || sha256_file(&fixture.journal)? != report.journal_sha256
    {
        return Err("fixture observation does not match its pinned coordinates".to_owned());
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
                secret_environment: BTreeMap::new(),
                stdin: Vec::new(),
                timeout: Duration::from_secs(10),
                max_output_bytes: MAX_CAPTURE_BYTES,
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

async fn wait_for_fixture_observation(
    broker: &LocalProcessBroker,
    fixture_program: &Path,
    fixture_spec: &Path,
    workspace: &Path,
    maximum_wait: Duration,
) -> AppResult<FixtureObserved> {
    let deadline = Instant::now()
        .checked_add(maximum_wait)
        .ok_or_else(|| "fixture-settlement deadline exceeds the runtime clock".to_owned())?;
    loop {
        match run_fixture_command(broker, fixture_program, fixture_spec, workspace, "inspect").await
        {
            Ok(report) => return Ok(report),
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(10)).await,
            Err(error) => {
                return Err(format!(
                    "fixture did not settle after controller release: {error}"
                ));
            }
        }
    }
}

async fn wait_for_effect_boundary(fixture: &FixtureSpec, maximum_wait: Duration) -> AppResult<()> {
    let deadline = Instant::now()
        .checked_add(maximum_wait)
        .ok_or_else(|| "effect-boundary deadline exceeds the runtime clock".to_owned())?;
    loop {
        if journal_has_effect_boundary(fixture)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("fixture effect boundary was not observed before its deadline".to_owned());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

fn journal_has_effect_boundary(fixture: &FixtureSpec) -> AppResult<bool> {
    let metadata = match fs::symlink_metadata(&fixture.journal) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(format!("cannot inspect fixture journal: {error}")),
    };
    if !metadata.file_type().is_file() || metadata.len() > 1_048_576 {
        return Err("fixture journal is not a bounded regular file".to_owned());
    }
    let bytes = fs::read(&fixture.journal)
        .map_err(|error| format!("cannot read fixture journal: {error}"))?;
    if bytes.is_empty() || !bytes.ends_with(b"\n") {
        return Ok(false);
    }
    let lines = bytes[..bytes.len() - 1]
        .split(|byte| *byte == b'\n')
        .collect::<Vec<_>>();
    if lines.len() < 3 {
        return Ok(false);
    }
    if lines.len() > 3 {
        return Err("fixture journal crossed the single-effect boundary".to_owned());
    }
    let records = lines
        .into_iter()
        .map(|line| {
            serde_json::from_slice::<Value>(line)
                .map_err(|_| "fixture journal contains invalid JSON".to_owned())
        })
        .collect::<AppResult<Vec<_>>>()?;
    let header = &records[0];
    if header.get("type").and_then(Value::as_str) != Some("initialized")
        || header.get("sequence").and_then(Value::as_u64) != Some(0)
        || header.get("fixture_id").and_then(Value::as_str) != Some(&fixture.fixture_id)
        || header.get("case").and_then(Value::as_str) != Some(&fixture.case)
        || header.get("operation_id").and_then(Value::as_str) != Some(&fixture.operation_id)
        || header
            .get("expected_payload_sha256")
            .and_then(Value::as_str)
            != Some(&fixture.expected_payload_sha256)
    {
        return Err("fixture journal header does not match the pinned coordinate".to_owned());
    }
    for (index, (record_type, sequence)) in
        [("invocation_started", 1_u64), ("effect_committed", 2_u64)]
            .into_iter()
            .enumerate()
    {
        let record = &records[index + 1];
        if record.get("type").and_then(Value::as_str) != Some(record_type)
            || record.get("sequence").and_then(Value::as_u64) != Some(sequence)
            || record.get("call_ordinal").and_then(Value::as_u64) != Some(1)
            || record.get("operation_id").and_then(Value::as_str) != Some(&fixture.operation_id)
            || record.get("payload_sha256").and_then(Value::as_str)
                != Some(&fixture.expected_payload_sha256)
        {
            return Err("fixture journal effect boundary is not the pinned operation".to_owned());
        }
    }
    Ok(true)
}

fn release_fixture(fixture: &FixtureSpec) -> AppResult<String> {
    let journal_name = fixture
        .journal
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "fixture journal file name is not UTF-8".to_owned())?;
    let release_path = fixture
        .journal
        .with_file_name(format!("{journal_name}.release"));
    let bytes = format!("{}\n", fixture.fixture_id).into_bytes();
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&release_path)
        .map_err(|error| format!("cannot create fixture release marker: {error}"))?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot persist fixture release marker: {error}"))?;
    sync_parent_directory(&release_path)?;
    Ok(sha256_bytes(&bytes))
}

fn write_new_file(path: &Path, bytes: &[u8], kind: &str) -> AppResult<()> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| format!("cannot create {kind}: {error}"))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("cannot persist {kind}: {error}"))?;
    sync_parent_directory(path)
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

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> AppResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| "persisted file has no parent directory".to_owned())?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("cannot sync persisted file parent: {error}"))
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> AppResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        CASE_ID, ModelFixtureSpec, REGISTERED_TOOL, RUN_FORMAT_VERSION, RunSpec, sha256_bytes,
        validate_spec,
    };
    use serde_json::Value;
    use std::path::PathBuf;

    fn spec() -> RunSpec {
        RunSpec {
            format_version: RUN_FORMAT_VERSION,
            run_id: "yh-cf003-1".to_owned(),
            benchmark_version: "cf003-v1".to_owned(),
            case_id: CASE_ID.to_owned(),
            program: absolute_path("yh"),
            expected_cli_version: "yh 0.1.0".to_owned(),
            expected_product_executable_sha256: "a".repeat(64),
            fixture_program: absolute_path("yh-fault-fixture"),
            fixture_spec: absolute_path("fixture-spec.json"),
            expected_fixture_spec_sha256: "b".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty".to_owned(),
            timeout_ms: 10_000,
            effect_wait_timeout_ms: 5_000,
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
    fn restart_spec_is_exact_and_bounded() {
        validate_spec(&spec()).expect("valid restart spec");
        let mut invalid = spec();
        invalid.effect_wait_timeout_ms = invalid.timeout_ms + 1;
        assert!(validate_spec(&invalid).is_err());
        invalid = spec();
        invalid.case_id = "another-case".to_owned();
        assert!(validate_spec(&invalid).is_err());
    }

    #[test]
    fn fixture_model_coordinate_has_one_registered_tool() {
        let model = ModelFixtureSpec {
            call_id: "call-1".to_owned(),
            registered_tool_name: REGISTERED_TOOL.to_owned(),
            trigger_prompt: "trigger".to_owned(),
            post_restart_prompt: "audit".to_owned(),
            post_restart_message: "observed".to_owned(),
        };
        assert_eq!(model.registered_tool_name, "fault.commit_effect");
        assert_ne!(model.trigger_prompt, model.post_restart_prompt);
    }

    #[test]
    fn checked_evidence_retains_explicit_recovery_and_non_replay_boundaries() {
        let result: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-y-harness-cf003-restart/result.json"
        ))
        .expect("checked Y-Harness restart report");
        let fixture_spec =
            include_bytes!("../evidence/2026-07-28-y-harness-cf003-restart/fixture-spec.json");
        let journal =
            include_bytes!("../evidence/2026-07-28-y-harness-cf003-restart/journal.jsonl");
        let release =
            include_bytes!("../evidence/2026-07-28-y-harness-cf003-restart/release-marker.txt");

        assert_eq!(result["format_version"], RUN_FORMAT_VERSION);
        assert_eq!(result["controls"]["claim_eligible"], false);
        assert_eq!(result["execution"]["passed"], true);
        assert_eq!(
            result["execution"]["compatibility"]["engine_version"],
            "0.1.0"
        );
        assert_eq!(
            result["execution"]["state"]["restart_pre_recovery_turn_status"],
            "running"
        );
        assert_eq!(
            result["execution"]["state"]["explicit_recovery_requested"],
            true
        );
        assert_eq!(
            result["execution"]["state"]["recovered_turn_status"],
            "interrupted"
        );
        assert_eq!(
            result["execution"]["state"]["recovered_tool_result_count"],
            0
        );
        assert_eq!(
            result["execution"]["state"]["recovery_replayed_tool"],
            false
        );
        assert_eq!(
            result["execution"]["interruption"]["descendant_process_group_cleanup_claimed"],
            false
        );
        assert_eq!(
            result["execution"]["interruption"]["detached_fixture_release_marker_persisted"],
            true
        );
        assert_eq!(result["fixture"]["after_restart"]["invocation_count"], 1);
        assert_eq!(result["fixture"]["after_restart"]["effect_count"], 1);
        assert_eq!(
            result["fixture"]["spec_file_sha256"],
            sha256_bytes(fixture_spec)
        );
        assert_eq!(
            result["fixture"]["after_restart"]["journal_sha256"],
            sha256_bytes(journal)
        );
        assert_eq!(
            result["execution"]["interruption"]["release_marker_sha256"],
            sha256_bytes(release)
        );
    }
}
