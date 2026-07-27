//! Source-pinned adapter for the released Codex non-interactive CLI.

use super::*;

const ADAPTER_VERSION: &str = "codex-exec-jsonl-v1";
const RUN_FORMAT_VERSION: u32 = 2;
const MAX_EVENTS: usize = 4_096;

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
    model: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    codex_home: Option<PathBuf>,
}

pub(super) struct NormalizedResult {
    pub(super) is_error: bool,
    pub(super) subtype: &'static str,
    pub(super) raw: Value,
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Codex run spec: {error}"))?;
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
    match (spec.profile, spec.codex_home.as_ref()) {
        (Profile::Bare, Some(home)) if home.is_absolute() => {
            let names = spec
                .inherit_environment
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            if !names.contains("CODEX_API_KEY") || !names.contains("CODEX_HOME") {
                return Err(
                    "bare Codex profile requires CODEX_API_KEY and CODEX_HOME inheritance"
                        .to_owned(),
                );
            }
        }
        (Profile::Bare, _) => {
            return Err("bare Codex profile requires an absolute codex_home".to_owned());
        }
        (Profile::Product, None) => {}
        (Profile::Product, Some(_)) => {
            return Err("product Codex profile must not declare codex_home".to_owned());
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
    let environment = inherited_environment(&spec.inherit_environment)?;
    validate_home(&spec, &environment)?;
    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Codex executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &environment, "Codex").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Codex version mismatch: expected {:?}, observed {:?}",
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
        args: arguments(&spec)?,
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

    let execution = match process {
        Ok(output) => {
            let stdout_sha256 = sha256_bytes(&output.stdout);
            let stderr_sha256 = sha256_bytes(&output.stderr);
            if output.stdout_truncated || output.stderr_truncated {
                RunExecution::AdapterError {
                    wall_time_ms,
                    message: "Codex output exceeded the adapter retention bound".to_owned(),
                    process: Some(failed_process_evidence(
                        &output,
                        stdout_sha256,
                        stderr_sha256,
                    )),
                }
            } else {
                match normalize_result(&output.stdout) {
                    Ok(normalized) => {
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: None,
                            product_api_duration_ms: None,
                            num_turns: 1,
                            actual_cost_usd: None,
                            actual_cost_usd_ticks: None,
                            result_subtype: Some(normalized.subtype.to_owned()),
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

    let mut unsupported_controls = vec![
        "adapter conformance is not a Harness-effect or product-quality result",
        "no cross-product model parity has been established",
        "Codex JSONL does not expose the settled Model identity",
        "Codex exec has no documented hard monetary spend ceiling",
        "Codex built-in Tools are available inside its read-only sandbox",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values, provider routing, and launcher dependencies are not recorded",
    ];
    if matches!(spec.profile, Profile::Product) {
        unsupported_controls.push("ambient product configuration is not eliminated");
    } else {
        unsupported_controls
            .push("workspace instructions and built-in product behavior are not eliminated");
    }

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "codex",
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
            requested_model: spec.model,
            observed_models: Vec::new(),
            prompt_sha256,
            system_prompt_sha256,
            tools: "product_builtins_read_only",
            permission_mode: "never",
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

fn validate_home(spec: &RunSpec, environment: &BTreeMap<String, String>) -> AppResult<()> {
    let Profile::Bare = spec.profile else {
        return Ok(());
    };
    let expected = spec
        .codex_home
        .as_ref()
        .ok_or_else(|| "bare Codex profile has no codex_home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "codex_home"))?;
    let configured = environment
        .get("CODEX_HOME")
        .ok_or_else(|| "bare Codex profile has no CODEX_HOME environment".to_owned())
        .and_then(|path| {
            fs::canonicalize(path)
                .map_err(|error| format!("cannot canonicalize CODEX_HOME: {error}"))
        })?;
    if configured != expected {
        return Err("codex_home and inherited CODEX_HOME resolve differently".to_owned());
    }
    Ok(())
}

fn arguments(spec: &RunSpec) -> AppResult<Vec<String>> {
    // JSON strings are valid TOML basic strings, so the CLI receives an exact
    // developer-instruction override without invoking a shell.
    let developer_instructions = serde_json::to_string(&spec.system_prompt)
        .map_err(|_| "cannot encode Codex developer instructions".to_owned())?;
    let mut args = vec![
        "exec".to_owned(),
        "--strict-config".to_owned(),
        "--json".to_owned(),
        "--ephemeral".to_owned(),
    ];
    if matches!(spec.profile, Profile::Bare) {
        args.extend([
            "--ignore-user-config".to_owned(),
            "--ignore-rules".to_owned(),
        ]);
    }
    args.extend([
        "--sandbox".to_owned(),
        "read-only".to_owned(),
        "--ask-for-approval".to_owned(),
        "never".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--config".to_owned(),
        format!("developer_instructions={developer_instructions}"),
        "--config".to_owned(),
        r#"web_search="disabled""#.to_owned(),
        "-".to_owned(),
    ]);
    Ok(args)
}

pub(super) fn normalize_result(bytes: &[u8]) -> AppResult<NormalizedResult> {
    let mut events = Vec::new();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if events.len() >= MAX_EVENTS {
            return Err(format!("Codex JSONL exceeds {MAX_EVENTS} retained events"));
        }
        let value: Value = serde_json::from_slice(line)
            .map_err(|_| "Codex stdout contains an invalid JSONL event".to_owned())?;
        if !value.is_object() {
            return Err("Codex JSONL events must be objects".to_owned());
        }
        events.push(value);
    }
    if events.is_empty() {
        return Err("Codex stdout contains no JSONL events".to_owned());
    }

    let mut saw_thread = false;
    let mut saw_turn = false;
    let mut saw_final_message = false;
    let mut terminal = None;
    for (index, event) in events.iter().enumerate() {
        if terminal.is_some() {
            return Err("Codex emitted an event after terminal settlement".to_owned());
        }
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "Codex JSONL event has no string type".to_owned())?;
        match kind {
            "thread.started" => {
                if index != 0 || saw_thread {
                    return Err("Codex thread.started must be the first unique event".to_owned());
                }
                let thread_id = event
                    .get("thread_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "Codex thread.started has no thread_id".to_owned())?;
                validate_text("Codex thread_id", thread_id)?;
                saw_thread = true;
            }
            "turn.started" => {
                if !saw_thread || saw_turn {
                    return Err("Codex turn.started is missing or duplicated".to_owned());
                }
                saw_turn = true;
            }
            "item.started" | "item.updated" | "item.completed" => {
                // Codex 0.145.0 forwards app-server notifications
                // asynchronously. A Tool item can therefore reach JSONL
                // before turn.started even though both belong to the same
                // in-flight Turn. The terminal event still requires the
                // unique turn.started marker below.
                if !saw_thread {
                    return Err("Codex item event precedes thread.started".to_owned());
                }
                if kind == "item.completed"
                    && event
                        .get("item")
                        .and_then(Value::as_object)
                        .and_then(|item| item.get("type"))
                        .and_then(Value::as_str)
                        == Some("agent_message")
                {
                    let text = event
                        .get("item")
                        .and_then(Value::as_object)
                        .and_then(|item| item.get("text"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| "Codex completed agent message has no text".to_owned())?;
                    if text.len() > MAX_PROMPT_BYTES {
                        return Err("Codex final message exceeds the adapter bound".to_owned());
                    }
                    saw_final_message = true;
                }
            }
            "turn.completed" => {
                if !saw_turn || !saw_final_message {
                    return Err(
                        "Codex turn.completed lacks a started Turn or final message".to_owned()
                    );
                }
                validate_usage(event.get("usage"))?;
                terminal = Some((false, "turn.completed"));
            }
            "turn.failed" => {
                if !saw_turn {
                    return Err("Codex turn.failed precedes turn.started".to_owned());
                }
                validate_error(event.get("error"))?;
                terminal = Some((true, "turn.failed"));
            }
            "error" => {
                validate_error(Some(event))?;
                terminal = Some((true, "error"));
            }
            _ => return Err(format!("unsupported Codex JSONL event type {kind:?}")),
        }
    }
    let (is_error, subtype) =
        terminal.ok_or_else(|| "Codex JSONL has no terminal settlement".to_owned())?;
    Ok(NormalizedResult {
        is_error,
        subtype,
        raw: Value::Array(events),
    })
}

fn validate_usage(value: Option<&Value>) -> AppResult<()> {
    let usage = value
        .and_then(Value::as_object)
        .ok_or_else(|| "Codex turn.completed has no usage object".to_owned())?;
    for field in [
        "input_tokens",
        "cached_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
    ] {
        if usage
            .get(field)
            .and_then(Value::as_i64)
            .is_none_or(|value| value < 0)
        {
            return Err(format!("Codex usage {field} must be a nonnegative integer"));
        }
    }
    if usage
        .get("cache_write_input_tokens")
        .is_some_and(|value| value.as_i64().is_none_or(|value| value < 0))
    {
        return Err("Codex usage cache_write_input_tokens must be nonnegative".to_owned());
    }
    Ok(())
}

fn validate_error(value: Option<&Value>) -> AppResult<()> {
    let message = value
        .and_then(Value::as_object)
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .ok_or_else(|| "Codex failure has no error message".to_owned())?;
    if message.is_empty() || message.len() > MAX_PROMPT_BYTES {
        return Err("Codex error message exceeds its bound".to_owned());
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
            run_id: "codex-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("codex"),
            expected_cli_version: "codex-cli 1.2.3".to_owned(),
            expected_product_executable_sha256: "b".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: Profile::Bare,
            model: "gpt-test".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["CODEX_API_KEY".to_owned(), "CODEX_HOME".to_owned()],
            codex_home: Some(absolute_path("codex-home")),
        }
    }

    #[test]
    fn bare_profile_is_explicit_and_passes_prompt_only_on_stdin() {
        let spec = valid_spec();
        validate_spec(&spec).expect("valid Codex spec");
        let arguments = arguments(&spec).expect("Codex arguments");
        assert_eq!(arguments.first().map(String::as_str), Some("exec"));
        assert!(
            arguments
                .iter()
                .any(|value| value == "--ignore-user-config")
        );
        assert!(arguments.iter().any(|value| value == "--ignore-rules"));
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--sandbox", "read-only"])
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--ask-for-approval", "never"])
        );
        assert_eq!(arguments.last().map(String::as_str), Some("-"));
        assert!(!arguments.iter().any(|value| value == &spec.prompt));
    }

    #[test]
    fn profile_rejects_ambiguous_runtime_home_authority() {
        let mut wrong_format = valid_spec();
        wrong_format.format_version = 1;
        assert!(validate_spec(&wrong_format).is_err());

        let mut bare = valid_spec();
        bare.inherit_environment
            .retain(|name| name != "CODEX_API_KEY");
        assert!(validate_spec(&bare).is_err());

        let mut product = valid_spec();
        product.profile = Profile::Product;
        assert!(validate_spec(&product).is_err());
        product.codex_home = None;
        assert!(validate_spec(&product).is_ok());
    }

    #[test]
    fn jsonl_requires_one_ordered_terminal_turn() {
        let completed = br#"{"type":"thread.started","thread_id":"thread-1"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"YH-OK"}}
{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3,"reasoning_output_tokens":1}}
"#;
        let normalized = normalize_result(completed).expect("valid Codex JSONL");
        assert!(!normalized.is_error);
        assert_eq!(normalized.subtype, "turn.completed");
        assert_eq!(normalized.raw.as_array().map(Vec::len), Some(4));

        let crossed = br#"{"type":"thread.started","thread_id":"thread-1"}
{"type":"item.started","item":{"id":"item-1","type":"mcp_tool_call","server":"fixture","tool":"commit_effect","arguments":{},"status":"in_progress"}}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"YH-OK"}}
{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":3,"reasoning_output_tokens":1}}
"#;
        assert!(normalize_result(crossed).is_ok());

        let trailing = [
            completed.as_slice(),
            br#"{"type":"error","message":"late"}"#,
        ]
        .concat();
        assert!(normalize_result(&trailing).is_err());
    }

    #[test]
    fn failure_remains_a_product_settlement() {
        let failed = br#"{"type":"thread.started","thread_id":"thread-1"}
{"type":"turn.started"}
{"type":"turn.failed","error":{"message":"provider unavailable"}}
"#;
        let normalized = normalize_result(failed).expect("valid Codex failure");
        assert!(normalized.is_error);
        assert_eq!(normalized.subtype, "turn.failed");
    }
}
