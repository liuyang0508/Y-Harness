//! Source-pinned adapter for the released OpenCode non-interactive CLI.

use super::*;

const ADAPTER_VERSION: &str = "opencode-run-jsonl-v1";
const RUN_FORMAT_VERSION: u32 = 5;
const MAX_EVENTS: usize = 4_096;
const BENCHMARK_AGENT: &str = "yh-benchmark";
const OWNED_ENVIRONMENT: [&str; 21] = [
    "HOME",
    "USERPROFILE",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_STATE_HOME",
    "XDG_CACHE_HOME",
    "OPENCODE_TEST_HOME",
    "OPENCODE_AUTH_CONTENT",
    "OPENCODE_CONFIG_CONTENT",
    "OPENCODE_PURE",
    "OPENCODE_DISABLE_PROJECT_CONFIG",
    "OPENCODE_DISABLE_AUTOUPDATE",
    "OPENCODE_DISABLE_AUTOCOMPACT",
    "OPENCODE_DISABLE_PRUNE",
    "OPENCODE_DISABLE_MODELS_FETCH",
    "OPENCODE_DISABLE_SHARE",
    "OPENCODE_DISABLE_DEFAULT_PLUGINS",
    "OPENCODE_DISABLE_EXTERNAL_SKILLS",
    "OPENCODE_DISABLE_LSP_DOWNLOAD",
    "OPENCODE_DISABLE_CLAUDE_CODE",
    "OPENCODE_DB",
];

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
    variant: Option<String>,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    home: Option<PathBuf>,
}

struct NormalizedResult {
    is_error: bool,
    subtype: String,
    num_turns: u64,
    total_cost_usd: Option<f64>,
    raw: Value,
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid OpenCode run spec: {error}"))?;
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
    let (provider, model) = spec
        .model
        .split_once('/')
        .filter(|(provider, model)| !provider.is_empty() && !model.is_empty())
        .ok_or_else(|| "OpenCode model must use the exact provider/model form".to_owned())?;
    validate_text("OpenCode provider", provider)?;
    validate_text("OpenCode model", model)?;
    reject_config_substitution("OpenCode model", &spec.model)?;
    reject_config_substitution("OpenCode system_prompt", &spec.system_prompt)?;
    if let Some(variant) = &spec.variant {
        validate_text("OpenCode variant", variant)?;
        reject_config_substitution("OpenCode variant", variant)?;
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
        return Err("OpenCode adapter-owned environment must not be inherited".to_owned());
    }
    match (spec.profile, spec.home.as_ref()) {
        (Profile::Bare, Some(home)) if home.is_absolute() => {}
        (Profile::Bare, _) => {
            return Err("bare OpenCode profile requires an absolute home".to_owned());
        }
        (Profile::Product, None) => {}
        (Profile::Product, Some(_)) => {
            return Err("product OpenCode profile must not declare home".to_owned());
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
    prepare_environment(&spec, &mut environment)?;

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "OpenCode executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &environment, "OpenCode").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "OpenCode version mismatch: expected {:?}, observed {:?}",
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

    let execution = match process {
        Ok(output) => {
            let stdout_sha256 = sha256_bytes(&output.stdout);
            let stderr_sha256 = sha256_bytes(&output.stderr);
            if output.stdout_truncated || output.stderr_truncated {
                RunExecution::AdapterError {
                    wall_time_ms,
                    message: "OpenCode output exceeded the adapter retention bound".to_owned(),
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
                            num_turns: normalized.num_turns,
                            actual_cost_usd: normalized.total_cost_usd,
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

    let mut unsupported_controls = vec![
        "adapter conformance is not a Harness-effect or product-quality result",
        "no cross-product model parity has been established",
        "Tools are denied, so Agent-loop effectiveness is not measured",
        "OpenCode JSONL does not expose the settled Model identity",
        "OpenCode exposes no documented hard monetary spend ceiling",
        "OpenCode exposes no hard provider-call ceiling for run",
        "the requested agent prompt is additive to product/provider instructions",
        "OpenCode JSONL does not expose distinct product and API durations",
        "OpenCode may initialize or update its plugin SDK dependency cache",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "environment values, provider routing, and launcher dependencies are not recorded",
    ];
    if matches!(spec.profile, Profile::Product) {
        unsupported_controls.push(
            "ambient product authentication, global configuration, instructions, and MCP definitions are not eliminated",
        );
    }

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "opencode",
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
            tools: "disabled",
            permission_mode: "deny",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: None,
            requested_reasoning_effort: spec.variant,
            requested_max_turns: None,
            product_sandbox: None,
            unsupported_controls,
        },
        execution,
    })
}

fn prepare_environment(
    spec: &RunSpec,
    environment: &mut BTreeMap<String, String>,
) -> AppResult<()> {
    environment.insert(
        "OPENCODE_CONFIG_CONTENT".to_owned(),
        benchmark_config(spec)?,
    );
    environment.insert("OPENCODE_PURE".to_owned(), "1".to_owned());
    for name in [
        "OPENCODE_DISABLE_PROJECT_CONFIG",
        "OPENCODE_DISABLE_AUTOUPDATE",
        "OPENCODE_DISABLE_AUTOCOMPACT",
        "OPENCODE_DISABLE_PRUNE",
        "OPENCODE_DISABLE_MODELS_FETCH",
        "OPENCODE_DISABLE_SHARE",
    ] {
        environment.insert(name.to_owned(), "1".to_owned());
    }
    environment.insert("OPENCODE_DB".to_owned(), ":memory:".to_owned());

    let Profile::Bare = spec.profile else {
        return Ok(());
    };
    let home = spec
        .home
        .as_ref()
        .ok_or_else(|| "bare OpenCode profile has no home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "home"))?;
    let home = utf8_path(&home, "home")?;
    environment.insert("HOME".to_owned(), home.to_owned());
    environment.insert("USERPROFILE".to_owned(), home.to_owned());
    environment.insert("OPENCODE_TEST_HOME".to_owned(), home.to_owned());
    environment.insert("OPENCODE_AUTH_CONTENT".to_owned(), "{}".to_owned());
    for name in [
        "OPENCODE_DISABLE_DEFAULT_PLUGINS",
        "OPENCODE_DISABLE_EXTERNAL_SKILLS",
        "OPENCODE_DISABLE_LSP_DOWNLOAD",
        "OPENCODE_DISABLE_CLAUDE_CODE",
    ] {
        environment.insert(name.to_owned(), "1".to_owned());
    }
    for (name, suffix) in [
        ("XDG_CONFIG_HOME", ".config"),
        ("XDG_DATA_HOME", ".local/share"),
        ("XDG_STATE_HOME", ".local/state"),
        ("XDG_CACHE_HOME", ".cache"),
    ] {
        let path = Path::new(home).join(suffix);
        environment.insert(name.to_owned(), utf8_path(&path, name)?.to_owned());
    }
    Ok(())
}

fn reject_config_substitution(kind: &str, value: &str) -> AppResult<()> {
    if value.contains("{env:") || value.contains("{file:") {
        Err(format!(
            "{kind} must not contain OpenCode configuration substitution tokens"
        ))
    } else {
        Ok(())
    }
}

fn benchmark_config(spec: &RunSpec) -> AppResult<String> {
    let provider = spec
        .model
        .split_once('/')
        .map(|(provider, _)| provider)
        .ok_or_else(|| "OpenCode model has no provider".to_owned())?;
    let mut agent = serde_json::json!({
        "mode": "primary",
        "model": spec.model,
        "prompt": spec.system_prompt,
        "permission": "deny"
    });
    if let Some(variant) = &spec.variant {
        agent["variant"] = Value::String(variant.clone());
    }
    serde_json::to_string(&serde_json::json!({
        "share": "disabled",
        "autoupdate": false,
        "snapshot": false,
        "enabled_providers": [provider],
        "default_agent": BENCHMARK_AGENT,
        "agent": {
            BENCHMARK_AGENT: agent,
            "title": {
                "disable": true
            }
        },
        "mcp": {},
        "formatter": false,
        "lsp": false
    }))
    .map_err(|_| "cannot encode OpenCode benchmark configuration".to_owned())
}

fn arguments(spec: &RunSpec) -> Vec<String> {
    let mut arguments = vec![
        "--pure".to_owned(),
        "run".to_owned(),
        "--format".to_owned(),
        "json".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--agent".to_owned(),
        BENCHMARK_AGENT.to_owned(),
    ];
    if let Some(variant) = &spec.variant {
        arguments.extend(["--variant".to_owned(), variant.clone()]);
    }
    arguments
}

fn utf8_path<'a>(path: &'a Path, kind: &str) -> AppResult<&'a str> {
    path.to_str()
        .ok_or_else(|| format!("{kind} must be valid UTF-8 for the OpenCode CLI"))
}

fn normalize_result(bytes: &[u8]) -> AppResult<NormalizedResult> {
    let events = parse_events(bytes)?;
    let mut session_id = None;
    let mut step_active = false;
    let mut saw_text = false;
    let mut settled_error = false;
    let mut num_turns = 0_u64;
    let mut total_cost_usd = 0.0;
    let mut subtype = None;

    for (index, event) in events.iter().enumerate() {
        if settled_error {
            return Err("OpenCode emitted an event after error settlement".to_owned());
        }
        validate_event_envelope(event, &mut session_id)?;
        let kind = event
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| "OpenCode JSONL event has no string type".to_owned())?;
        match kind {
            "step_start" => {
                if step_active {
                    return Err("OpenCode step_start overlaps an active step".to_owned());
                }
                validate_part(event, "step-start")?;
                step_active = true;
            }
            "reasoning" => {
                if !step_active {
                    return Err("OpenCode reasoning is outside an active step".to_owned());
                }
                validate_text_part(event, "reasoning")?;
            }
            "text" => {
                if !step_active {
                    return Err("OpenCode text is outside an active step".to_owned());
                }
                validate_text_part(event, "text")?;
                saw_text = true;
            }
            "tool_use" => {
                return Err("OpenCode emitted Tool use while Tools were disabled".to_owned());
            }
            "step_finish" => {
                if !step_active {
                    return Err("OpenCode step_finish is outside an active step".to_owned());
                }
                let finish = validate_step_finish(event)?;
                total_cost_usd += finish.cost;
                if !total_cost_usd.is_finite() {
                    return Err("OpenCode accumulated cost is not finite".to_owned());
                }
                num_turns = num_turns
                    .checked_add(1)
                    .ok_or_else(|| "OpenCode Turn count overflowed".to_owned())?;
                subtype = Some(finish.reason);
                step_active = false;
            }
            "error" => {
                if index + 1 != events.len()
                    || event.get("error").and_then(Value::as_object).is_none()
                {
                    return Err("OpenCode error must be one final object event".to_owned());
                }
                settled_error = true;
                subtype = Some("error".to_owned());
            }
            _ => return Err(format!("unsupported OpenCode JSONL event type {kind:?}")),
        }
    }
    if settled_error {
        return Ok(NormalizedResult {
            is_error: true,
            subtype: subtype.unwrap_or_else(|| "error".to_owned()),
            num_turns,
            total_cost_usd: None,
            raw: Value::Array(events),
        });
    }
    if step_active || num_turns == 0 || !saw_text {
        return Err("OpenCode JSONL has no complete text settlement".to_owned());
    }
    Ok(NormalizedResult {
        is_error: false,
        subtype: subtype.ok_or_else(|| "OpenCode JSONL has no finish reason".to_owned())?,
        num_turns,
        total_cost_usd: Some(total_cost_usd),
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
            return Err(format!(
                "OpenCode JSONL exceeds {MAX_EVENTS} retained events"
            ));
        }
        let value: Value = serde_json::from_slice(line)
            .map_err(|_| "OpenCode stdout contains an invalid JSONL event".to_owned())?;
        if !value.is_object() {
            return Err("OpenCode JSONL events must be objects".to_owned());
        }
        events.push(value);
    }
    if events.is_empty() {
        return Err("OpenCode stdout contains no JSONL events".to_owned());
    }
    Ok(events)
}

fn validate_event_envelope(event: &Value, session_id: &mut Option<String>) -> AppResult<()> {
    let observed = event
        .get("sessionID")
        .and_then(Value::as_str)
        .ok_or_else(|| "OpenCode JSONL event has no string sessionID".to_owned())?;
    validate_text("OpenCode sessionID", observed)?;
    match session_id {
        Some(expected) if expected != observed => {
            return Err("OpenCode JSONL changes sessionID".to_owned());
        }
        None => *session_id = Some(observed.to_owned()),
        Some(_) => {}
    }
    event
        .get("timestamp")
        .and_then(Value::as_u64)
        .ok_or_else(|| "OpenCode JSONL event has no unsigned timestamp".to_owned())?;
    Ok(())
}

fn validate_part<'a>(
    event: &'a Value,
    expected_type: &str,
) -> AppResult<&'a serde_json::Map<String, Value>> {
    let part = event
        .get("part")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("OpenCode {expected_type} event has no part object"))?;
    if part.get("type").and_then(Value::as_str) != Some(expected_type) {
        return Err(format!(
            "OpenCode event and part type disagree for {expected_type}"
        ));
    }
    let outer_session = event
        .get("sessionID")
        .and_then(Value::as_str)
        .ok_or_else(|| "OpenCode event has no sessionID".to_owned())?;
    if part.get("sessionID").and_then(Value::as_str) != Some(outer_session) {
        return Err("OpenCode part and event sessionID disagree".to_owned());
    }
    Ok(part)
}

fn validate_text_part(event: &Value, expected_type: &str) -> AppResult<()> {
    let text = validate_part(event, expected_type)?
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("OpenCode {expected_type} part has no text"))?;
    if text.len() > MAX_PROMPT_BYTES {
        return Err(format!(
            "OpenCode {expected_type} text exceeds the adapter bound"
        ));
    }
    Ok(())
}

struct StepFinish {
    reason: String,
    cost: f64,
}

fn validate_step_finish(event: &Value) -> AppResult<StepFinish> {
    let part = validate_part(event, "step-finish")?;
    let reason = part
        .get("reason")
        .and_then(Value::as_str)
        .ok_or_else(|| "OpenCode step-finish has no reason".to_owned())?;
    validate_text("OpenCode finish reason", reason)?;
    let cost = nonnegative_f64(part.get("cost"), "cost")?;
    let tokens = part
        .get("tokens")
        .and_then(Value::as_object)
        .ok_or_else(|| "OpenCode step-finish has no tokens object".to_owned())?;
    for name in ["input", "output", "reasoning"] {
        nonnegative_f64(tokens.get(name), name)?;
    }
    if let Some(total) = tokens.get("total") {
        nonnegative_f64(Some(total), "total")?;
    }
    let cache = tokens
        .get("cache")
        .and_then(Value::as_object)
        .ok_or_else(|| "OpenCode step-finish has no token cache object".to_owned())?;
    for name in ["read", "write"] {
        nonnegative_f64(cache.get(name), name)?;
    }
    Ok(StepFinish {
        reason: reason.to_owned(),
        cost,
    })
}

fn nonnegative_f64(value: Option<&Value>, name: &str) -> AppResult<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| format!("OpenCode {name} must be finite and nonnegative"))
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
            run_id: "opencode-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("opencode"),
            expected_cli_version: "1.2.3".to_owned(),
            expected_product_executable_sha256: "e".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: Profile::Bare,
            model: "openai/gpt-test".to_owned(),
            variant: Some("low".to_owned()),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["OPENAI_API_KEY".to_owned()],
            home: Some(absolute_path("opencode-home")),
        }
    }

    fn part(kind: &str, fields: &str) -> String {
        format!(
            r#"{{"type":"{}","timestamp":1,"sessionID":"ses-1","part":{{"type":"{}","sessionID":"ses-1","messageID":"msg-1","id":"part-1"{}}}}}"#,
            kind.replace('-', "_"),
            kind,
            fields
        )
    }

    #[test]
    fn bare_profile_owns_ambient_state_and_command_keeps_prompt_on_stdin() {
        let home = std::env::temp_dir().join(format!(
            "y-harness-opencode-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos()
        ));
        fs::create_dir(&home).expect("create isolated OpenCode home");
        let mut spec = valid_spec();
        spec.home = Some(home.clone());
        validate_spec(&spec).expect("valid OpenCode spec");
        let args = arguments(&spec);
        assert!(args.iter().any(|value| value == "--pure"));
        assert!(args.windows(2).any(|pair| pair == ["--format", "json"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--agent", BENCHMARK_AGENT])
        );
        assert!(!args.iter().any(|value| value == &spec.prompt));

        let config: Value = serde_json::from_str(&benchmark_config(&spec).expect("configuration"))
            .expect("valid JSON");
        assert_eq!(config["agent"][BENCHMARK_AGENT]["permission"], "deny");
        assert_eq!(config["agent"]["title"]["disable"], true);
        assert_eq!(config["snapshot"], false);

        let mut environment = BTreeMap::new();
        prepare_environment(&spec, &mut environment).expect("prepare bare environment");
        assert_eq!(environment["OPENCODE_AUTH_CONTENT"], "{}");
        for name in [
            "OPENCODE_DISABLE_DEFAULT_PLUGINS",
            "OPENCODE_DISABLE_EXTERNAL_SKILLS",
            "OPENCODE_DISABLE_LSP_DOWNLOAD",
            "OPENCODE_DISABLE_CLAUDE_CODE",
        ] {
            assert_eq!(environment[name], "1");
        }
        fs::remove_dir(&home).expect("remove isolated OpenCode home");

        let mut without_variant = spec;
        without_variant.variant = None;
        let config: Value =
            serde_json::from_str(&benchmark_config(&without_variant).expect("configuration"))
                .expect("valid JSON");
        assert!(config["agent"][BENCHMARK_AGENT].get("variant").is_none());
    }

    #[test]
    fn profile_and_model_validation_reject_ambiguous_authority() {
        let mut spec = valid_spec();
        spec.inherit_environment.push("HOME".to_owned());
        assert!(validate_spec(&spec).is_err());

        let mut product = valid_spec();
        product.profile = Profile::Product;
        assert!(validate_spec(&product).is_err());
        product.home = None;
        assert!(validate_spec(&product).is_ok());

        product.model = "missing-provider".to_owned();
        assert!(validate_spec(&product).is_err());

        product.model = "openai/gpt-test".to_owned();
        product.system_prompt = "Read {file:/private/evidence}".to_owned();
        assert!(validate_spec(&product).is_err());
        product.system_prompt = "Use {env:SECRET}".to_owned();
        assert!(validate_spec(&product).is_err());
    }

    #[test]
    fn jsonl_preserves_step_cost_and_finish_reason() {
        let events = format!(
            "{}\n{}\n{}\n",
            part("step-start", ""),
            part("text", r#","text":"YH-OK","time":{"start":1,"end":2}"#),
            part(
                "step-finish",
                r#","reason":"stop","cost":0.0125,"tokens":{"total":15,"input":10,"output":5,"reasoning":0,"cache":{"read":0,"write":0}}"#
            )
        );
        let normalized = normalize_result(events.as_bytes()).expect("valid OpenCode JSONL");
        assert!(!normalized.is_error);
        assert_eq!(normalized.subtype, "stop");
        assert_eq!(normalized.num_turns, 1);
        assert_eq!(normalized.total_cost_usd, Some(0.0125));
    }

    #[test]
    fn jsonl_rejects_tools_cross_session_and_trailing_errors() {
        let tool = part(
            "tool-use",
            r#","tool":"bash","state":{"status":"completed"}"#,
        );
        assert!(normalize_result(tool.as_bytes()).is_err());

        let changed_session = format!(
            "{}\n{}\n",
            part("step-start", ""),
            r#"{"type":"text","timestamp":2,"sessionID":"ses-2","part":{"type":"text","sessionID":"ses-2","messageID":"msg-1","id":"part-2","text":"bad"}}"#
        );
        assert!(normalize_result(changed_session.as_bytes()).is_err());

        let trailing = concat!(
            "{\"type\":\"error\",\"timestamp\":1,\"sessionID\":\"ses-1\",\"error\":{}}\n",
            "{\"type\":\"step_start\",\"timestamp\":2,\"sessionID\":\"ses-1\",\"part\":{\"type\":\"step-start\",\"sessionID\":\"ses-1\"}}\n"
        );
        assert!(normalize_result(trailing.as_bytes()).is_err());

        let error =
            normalize_result(br#"{"type":"error","timestamp":1,"sessionID":"ses-1","error":{}}"#)
                .expect("valid product error");
        assert!(error.is_error);
        assert_eq!(error.total_cost_usd, None);
    }
}
