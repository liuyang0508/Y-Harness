//! Source-pinned adapter for the released Pi non-interactive CLI.

use super::*;

const ADAPTER_VERSION: &str = "pi-jsonl-v1";
const RUN_FORMAT_VERSION: u32 = 4;
const MAX_EVENTS: usize = 4_096;
const PI_AGENT_DIR: &str = "PI_CODING_AGENT_DIR";
const THINKING_LEVELS: [&str; 7] = ["off", "minimal", "low", "medium", "high", "xhigh", "max"];

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    Bare,
    Product,
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
    thinking: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    pi_agent_dir: Option<PathBuf>,
}

struct NormalizedResult {
    is_error: bool,
    subtype: String,
    num_turns: u64,
    total_cost_usd: f64,
    observed_models: Vec<String>,
    raw: Value,
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Pi run spec: {error}"))?;
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
    validate_text("provider", &spec.provider)?;
    if !THINKING_LEVELS.contains(&spec.thinking.as_str()) {
        return Err(format!(
            "thinking must be one of {}",
            THINKING_LEVELS.join(", ")
        ));
    }
    if spec.prompt.trim() != spec.prompt {
        return Err("Pi prompt must not have leading or trailing whitespace".to_owned());
    }
    let inherited = spec
        .inherit_environment
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    match (spec.profile, spec.pi_agent_dir.as_ref()) {
        (Profile::Bare, Some(directory)) if directory.is_absolute() => {
            if inherited.contains(PI_AGENT_DIR) {
                return Err(format!(
                    "bare Pi profile owns {PI_AGENT_DIR}; do not inherit it"
                ));
            }
        }
        (Profile::Bare, _) => {
            return Err("bare Pi profile requires an absolute pi_agent_dir".to_owned());
        }
        (Profile::Product, None) => {}
        (Profile::Product, Some(_)) => {
            return Err("product Pi profile must not declare pi_agent_dir".to_owned());
        }
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
    let mut environment = inherited_environment(&spec.inherit_environment)?;
    prepare_bare_environment(&spec, &mut environment)?;

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Pi executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let cli_version = read_cli_version(&broker, &program, &workspace, &environment, "Pi").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Pi version mismatch: expected {:?}, observed {:?}",
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
        args: arguments(&spec),
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
                        message: "Pi output exceeded the adapter retention bound".to_owned(),
                        process: Some(failed_process_evidence(
                            &output,
                            stdout_sha256,
                            stderr_sha256,
                        )),
                    },
                    Vec::new(),
                )
            } else {
                match normalize_result(&output.stdout) {
                    Ok(normalized) => {
                        let observed_models = normalized.observed_models.clone();
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: None,
                            product_api_duration_ms: None,
                            num_turns: normalized.num_turns,
                            actual_cost_usd: Some(normalized.total_cost_usd),
                            actual_cost_usd_ticks: None,
                            result_subtype: Some(normalized.subtype),
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
        "Pi exposes no documented hard monetary spend ceiling",
        "Pi provides no built-in product sandbox",
        "Pi JSONL does not expose distinct product and API durations",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values, provider routing, and launcher dependencies are not recorded",
    ];
    if matches!(spec.profile, Profile::Product) {
        unsupported_controls.push("ambient product configuration is not eliminated");
    }

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "pi",
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
                Profile::Product => "product",
            },
            requested_provider: None,
            requested_model: format!("{}/{}", spec.provider, spec.model),
            observed_models,
            prompt_sha256,
            system_prompt_sha256,
            tools: "disabled",
            permission_mode: "no_approve",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: None,
            requested_reasoning_effort: Some(spec.thinking),
            requested_max_turns: None,
            product_sandbox: None,
            unsupported_controls,
        },
        execution,
    })
}

fn prepare_bare_environment(
    spec: &RunSpec,
    environment: &mut BTreeMap<String, String>,
) -> AppResult<()> {
    let Profile::Bare = spec.profile else {
        return Ok(());
    };
    let directory = spec
        .pi_agent_dir
        .as_ref()
        .ok_or_else(|| "bare Pi profile has no pi_agent_dir".to_owned())
        .and_then(|path| canonical_empty_directory(path, "pi_agent_dir"))?;
    let value = directory
        .to_str()
        .ok_or_else(|| "pi_agent_dir is not valid UTF-8".to_owned())?;
    environment.insert(PI_AGENT_DIR.to_owned(), value.to_owned());
    Ok(())
}

fn arguments(spec: &RunSpec) -> Vec<String> {
    vec![
        "--mode".to_owned(),
        "json".to_owned(),
        "--print".to_owned(),
        "--no-session".to_owned(),
        "--no-tools".to_owned(),
        "--no-extensions".to_owned(),
        "--no-skills".to_owned(),
        "--no-prompt-templates".to_owned(),
        "--no-themes".to_owned(),
        "--no-context-files".to_owned(),
        "--no-approve".to_owned(),
        "--offline".to_owned(),
        "--provider".to_owned(),
        spec.provider.clone(),
        "--model".to_owned(),
        spec.model.clone(),
        "--thinking".to_owned(),
        spec.thinking.clone(),
        "--system-prompt".to_owned(),
        spec.system_prompt.clone(),
    ]
}

fn normalize_result(bytes: &[u8]) -> AppResult<NormalizedResult> {
    let events = parse_events(bytes)?;
    let mut agent_active = false;
    let mut turn_active = false;
    let mut saw_agent = false;
    let mut saw_agent_end = false;
    let mut settled = false;
    let mut num_turns = 0_u64;
    let mut total_cost_usd = 0.0;
    let mut observed_models = BTreeSet::new();
    let mut last_stop_reason = None;

    for (index, event) in events.iter().enumerate() {
        if settled {
            return Err("Pi emitted an event after agent_settled".to_owned());
        }
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "Pi JSONL event has no string type".to_owned())?;
        match kind {
            "session" => {
                if index != 0 {
                    return Err("Pi session header must be the first event".to_owned());
                }
                validate_session_header(event)?;
            }
            "agent_start" => {
                if agent_active || turn_active {
                    return Err("Pi agent_start overlaps an active run".to_owned());
                }
                agent_active = true;
                saw_agent = true;
                saw_agent_end = false;
            }
            "turn_start" => {
                if !agent_active || turn_active {
                    return Err("Pi turn_start is outside an idle Agent run".to_owned());
                }
                turn_active = true;
            }
            "message_start" | "message_update" => {
                if !agent_active || !turn_active {
                    return Err(format!("Pi {kind} is outside an active Turn"));
                }
            }
            "message_end" => {
                if !agent_active || !turn_active {
                    return Err("Pi message_end is outside an active Turn".to_owned());
                }
                if event
                    .get("message")
                    .and_then(Value::as_object)
                    .and_then(|message| message.get("role"))
                    .and_then(Value::as_str)
                    == Some("assistant")
                {
                    let assistant = validate_assistant(
                        event
                            .get("message")
                            .ok_or_else(|| "Pi message_end has no message".to_owned())?,
                    )?;
                    total_cost_usd += assistant.cost;
                    if !total_cost_usd.is_finite() {
                        return Err("Pi accumulated cost is not finite".to_owned());
                    }
                    observed_models.insert(assistant.model);
                    last_stop_reason = Some(assistant.stop_reason);
                }
            }
            "turn_end" => {
                if !agent_active || !turn_active {
                    return Err("Pi turn_end is outside an active Turn".to_owned());
                }
                validate_assistant(
                    event
                        .get("message")
                        .ok_or_else(|| "Pi turn_end has no message".to_owned())?,
                )?;
                if event
                    .get("toolResults")
                    .and_then(Value::as_array)
                    .is_none_or(|results| !results.is_empty())
                {
                    return Err("Pi emitted Tool results while Tools were disabled".to_owned());
                }
                turn_active = false;
                num_turns = num_turns
                    .checked_add(1)
                    .ok_or_else(|| "Pi Turn count overflowed".to_owned())?;
            }
            "agent_end" => {
                if !agent_active || turn_active {
                    return Err("Pi agent_end is outside a settled Agent run".to_owned());
                }
                if event.get("messages").and_then(Value::as_array).is_none()
                    || event.get("willRetry").and_then(Value::as_bool).is_none()
                {
                    return Err("Pi agent_end has invalid settlement fields".to_owned());
                }
                agent_active = false;
                saw_agent_end = true;
            }
            "agent_settled" => {
                if !saw_agent || !saw_agent_end || agent_active || turn_active {
                    return Err("Pi agent_settled precedes Agent settlement".to_owned());
                }
                settled = true;
            }
            "entry_appended"
            | "queue_update"
            | "auto_retry_start"
            | "auto_retry_end"
            | "compaction_start"
            | "compaction_end"
            | "summarization_retry_scheduled"
            | "summarization_retry_attempt_start"
            | "summarization_retry_finished"
            | "session_info_changed"
            | "thinking_level_changed" => {}
            "tool_execution_start" | "tool_execution_update" | "tool_execution_end" => {
                return Err("Pi emitted Tool execution while Tools were disabled".to_owned());
            }
            _ => return Err(format!("unsupported Pi JSONL event type {kind:?}")),
        }
    }
    if !settled {
        return Err("Pi JSONL has no agent_settled event".to_owned());
    }
    let subtype =
        last_stop_reason.ok_or_else(|| "Pi JSONL has no completed assistant message".to_owned())?;
    Ok(NormalizedResult {
        is_error: matches!(subtype.as_str(), "error" | "aborted"),
        subtype,
        num_turns,
        total_cost_usd,
        observed_models: observed_models.into_iter().collect(),
        raw: Value::Array(events),
    })
}

fn parse_events(bytes: &[u8]) -> AppResult<Vec<Value>> {
    let mut events = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if events.len() >= MAX_EVENTS {
            return Err(format!("Pi JSONL exceeds {MAX_EVENTS} retained events"));
        }
        let value: Value = serde_json::from_slice(line)
            .map_err(|_| "Pi stdout contains an invalid JSONL event".to_owned())?;
        if !value.is_object() {
            return Err("Pi JSONL events must be objects".to_owned());
        }
        events.push(value);
    }
    if events.is_empty() {
        return Err("Pi stdout contains no JSONL events".to_owned());
    }
    Ok(events)
}

fn validate_session_header(event: &Value) -> AppResult<()> {
    let object = event
        .as_object()
        .ok_or_else(|| "Pi session header must be an object".to_owned())?;
    for field in ["id", "timestamp", "cwd"] {
        let value = object
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("Pi session header has no string {field}"))?;
        if value.is_empty() || value.len() > MAX_PROMPT_BYTES {
            return Err(format!("Pi session header {field} exceeds its bound"));
        }
    }
    Ok(())
}

struct AssistantEvidence {
    stop_reason: String,
    model: String,
    cost: f64,
}

fn validate_assistant(value: &Value) -> AppResult<AssistantEvidence> {
    let message = value
        .as_object()
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .ok_or_else(|| "Pi Turn settlement is not an assistant message".to_owned())?;
    let provider = message
        .get("provider")
        .and_then(Value::as_str)
        .ok_or_else(|| "Pi assistant message has no provider".to_owned())?;
    let model = message
        .get("responseModel")
        .and_then(Value::as_str)
        .or_else(|| message.get("model").and_then(Value::as_str))
        .ok_or_else(|| "Pi assistant message has no model".to_owned())?;
    validate_text("Pi observed provider", provider)?;
    validate_text("Pi observed model", model)?;
    let stop_reason = message
        .get("stopReason")
        .and_then(Value::as_str)
        .filter(|reason| matches!(*reason, "stop" | "length" | "toolUse" | "error" | "aborted"))
        .ok_or_else(|| "Pi assistant message has invalid stopReason".to_owned())?
        .to_owned();
    let cost = message
        .get("usage")
        .and_then(Value::as_object)
        .and_then(|usage| usage.get("cost"))
        .and_then(Value::as_object)
        .and_then(|cost| cost.get("total"))
        .and_then(Value::as_f64)
        .filter(|cost| cost.is_finite() && *cost >= 0.0)
        .ok_or_else(|| "Pi assistant message has invalid usage.cost.total".to_owned())?;
    Ok(AssistantEvidence {
        stop_reason,
        model: format!("{provider}/{model}"),
        cost,
    })
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
            run_id: "pi-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("pi"),
            expected_cli_version: "0.82.1".to_owned(),
            expected_product_executable_sha256: "d".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: Profile::Bare,
            provider: "openai".to_owned(),
            model: "gpt-test".to_owned(),
            thinking: "low".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["OPENAI_API_KEY".to_owned()],
            pi_agent_dir: Some(absolute_path("pi-agent")),
        }
    }

    fn assistant(stop_reason: &str, cost: f64) -> String {
        format!(
            r#"{{"role":"assistant","content":[{{"type":"text","text":"YH-OK"}}],"api":"openai-responses","provider":"openai","model":"gpt-test","usage":{{"input":10,"output":3,"cacheRead":0,"cacheWrite":0,"totalTokens":13,"cost":{{"input":0.01,"output":0.02,"cacheRead":0,"cacheWrite":0,"total":{cost}}}}},"stopReason":"{stop_reason}","timestamp":1}}"#
        )
    }

    #[test]
    fn bare_command_disables_ambient_capabilities_and_uses_stdin() {
        let spec = valid_spec();
        validate_spec(&spec).expect("valid Pi spec");
        let args = arguments(&spec);
        for flag in [
            "--no-session",
            "--no-tools",
            "--no-extensions",
            "--no-skills",
            "--no-prompt-templates",
            "--no-themes",
            "--no-context-files",
            "--no-approve",
            "--offline",
        ] {
            assert!(args.iter().any(|argument| argument == flag));
        }
        assert!(args.windows(2).any(|pair| pair == ["--mode", "json"]));
        assert!(!args.iter().any(|argument| argument == &spec.prompt));
    }

    #[test]
    fn profile_owns_bare_agent_directory_and_rejects_trimmed_prompts() {
        let mut spec = valid_spec();
        spec.inherit_environment.push(PI_AGENT_DIR.to_owned());
        assert!(validate_spec(&spec).is_err());

        let mut product = valid_spec();
        product.profile = Profile::Product;
        assert!(validate_spec(&product).is_err());
        product.pi_agent_dir = None;
        assert!(validate_spec(&product).is_ok());

        product.prompt.push('\n');
        assert!(validate_spec(&product).is_err());
    }

    #[test]
    fn jsonl_accepts_retries_and_preserves_reported_model_and_cost() {
        let first = assistant("error", 0.03);
        let second = assistant("stop", 0.04);
        let events = format!(
            r#"{{"type":"session","version":3,"id":"session-1","timestamp":"2026-07-27T00:00:00Z","cwd":"/workspace"}}
{{"type":"agent_start"}}
{{"type":"turn_start"}}
{{"type":"message_end","message":{first}}}
{{"type":"turn_end","message":{first},"toolResults":[]}}
{{"type":"agent_end","messages":[{first}],"willRetry":true}}
{{"type":"auto_retry_start","attempt":1,"maxAttempts":3,"delayMs":1000,"errorMessage":"retry"}}
{{"type":"agent_start"}}
{{"type":"turn_start"}}
{{"type":"message_end","message":{second}}}
{{"type":"auto_retry_end","success":true,"attempt":1}}
{{"type":"turn_end","message":{second},"toolResults":[]}}
{{"type":"agent_end","messages":[{second}],"willRetry":false}}
{{"type":"agent_settled"}}
"#
        );
        let normalized = normalize_result(events.as_bytes()).expect("valid Pi JSONL");
        assert!(!normalized.is_error);
        assert_eq!(normalized.subtype, "stop");
        assert_eq!(normalized.num_turns, 2);
        assert_eq!(normalized.total_cost_usd, 0.07);
        assert_eq!(normalized.observed_models, ["openai/gpt-test"]);
    }

    #[test]
    fn jsonl_rejects_tool_execution_and_events_after_settlement() {
        let message = assistant("stop", 0.01);
        let tool_event = r#"{"type":"agent_start"}
{"type":"turn_start"}
{"type":"tool_execution_start","toolCallId":"1","toolName":"read","args":{}}
"#;
        assert!(normalize_result(tool_event.as_bytes()).is_err());

        let trailing = format!(
            r#"{{"type":"agent_start"}}
{{"type":"turn_start"}}
{{"type":"message_end","message":{message}}}
{{"type":"turn_end","message":{message},"toolResults":[]}}
{{"type":"agent_end","messages":[{message}],"willRetry":false}}
{{"type":"agent_settled"}}
{{"type":"queue_update","steering":[],"followUp":[]}}
"#
        );
        assert!(normalize_result(trailing.as_bytes()).is_err());
    }
}
