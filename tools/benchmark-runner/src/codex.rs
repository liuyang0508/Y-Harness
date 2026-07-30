//! Source-pinned adapter for the released Codex non-interactive CLI.

use super::*;

const ADAPTER_VERSION: &str = "codex-exec-jsonl-v2";
const RUN_FORMAT_VERSION: u32 = 2;
const MAX_EVENTS: usize = 4_096;
const BARE_PROVIDER_ID: &str = "yh_bench";
const PROVIDER_TOKEN_ENV: &str = "CODEX_API_KEY";
const OWNED_BARE_ENVIRONMENT: [&str; 6] = [
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "CODEX_HOME",
    "CODEX_SQLITE_HOME",
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
    provider: Option<String>,
    provider_base_url: Option<String>,
    model: String,
    reasoning_effort: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    home: Option<PathBuf>,
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
    validate_text("reasoning_effort", &spec.reasoning_effort)?;
    match spec.profile {
        Profile::Bare => {
            let (Some(home), Some(codex_home)) = (spec.home.as_ref(), spec.codex_home.as_ref())
            else {
                return Err(
                    "bare Codex profile requires absolute home and codex_home directories"
                        .to_owned(),
                );
            };
            if !home.is_absolute() || !codex_home.is_absolute() {
                return Err(
                    "bare Codex profile requires absolute home and codex_home directories"
                        .to_owned(),
                );
            }
            let provider = spec
                .provider
                .as_deref()
                .ok_or_else(|| "bare Codex profile requires provider".to_owned())?;
            validate_text("Codex provider", provider)?;
            let provider_base_url = spec
                .provider_base_url
                .as_deref()
                .ok_or_else(|| "bare Codex profile requires provider_base_url".to_owned())?;
            validate_loopback_provider_base_url(provider_base_url)?;
            let names = spec
                .inherit_environment
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            if !names.contains(PROVIDER_TOKEN_ENV) {
                return Err("bare Codex profile requires CODEX_API_KEY inheritance".to_owned());
            }
            if OWNED_BARE_ENVIRONMENT.iter().any(|owned| {
                spec.inherit_environment
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(owned))
            }) {
                return Err("bare Codex profile owns its state environment".to_owned());
            }
        }
        Profile::Product => {
            if spec.home.is_some()
                || spec.codex_home.is_some()
                || spec.provider.is_some()
                || spec.provider_base_url.is_some()
            {
                return Err(
                    "product Codex profile must not declare bare-profile state or routing"
                        .to_owned(),
                );
            }
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
        secret_environment: BTreeMap::new(),
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
        "Codex JSONL does not expose the settled Model or Provider identity",
        "Codex exec has no documented hard monetary spend ceiling",
        "Codex exec exposes no hard Provider-call ceiling for one Turn",
        "Codex built-in Tools are available inside its read-only sandbox",
        "Codex materializes state under the isolated CODEX_HOME despite --ephemeral",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "credential values and launcher dependencies are not recorded",
        "the Provider request sidecar is corroborating evidence rather than product settlement",
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
            requested_provider: spec.provider,
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
            requested_reasoning_effort: Some(spec.reasoning_effort),
            requested_max_turns: None,
            product_sandbox: Some("read-only"),
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
    let home = spec
        .home
        .as_ref()
        .ok_or_else(|| "bare Codex profile has no home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "home"))?;
    let codex_home = spec
        .codex_home
        .as_ref()
        .ok_or_else(|| "bare Codex profile has no codex_home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "codex_home"))?;
    if home == codex_home {
        return Err("bare Codex home and codex_home must be distinct".to_owned());
    }
    let home = home
        .to_str()
        .ok_or_else(|| "home must be valid UTF-8".to_owned())?;
    let codex_home = codex_home
        .to_str()
        .ok_or_else(|| "codex_home must be valid UTF-8".to_owned())?;
    environment.insert("HOME".to_owned(), home.to_owned());
    environment.insert("USERPROFILE".to_owned(), home.to_owned());
    environment.insert("CODEX_HOME".to_owned(), codex_home.to_owned());
    environment.insert("CODEX_SQLITE_HOME".to_owned(), codex_home.to_owned());
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
            "--skip-git-repo-check".to_owned(),
        ]);
    }
    args.extend([
        "--sandbox".to_owned(),
        "read-only".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--config".to_owned(),
        r#"approval_policy="never""#.to_owned(),
        "--config".to_owned(),
        format!(
            "model_reasoning_effort={}",
            toml_string(&spec.reasoning_effort)?
        ),
        "--config".to_owned(),
        format!("developer_instructions={developer_instructions}"),
        "--config".to_owned(),
        r#"web_search="disabled""#.to_owned(),
    ]);
    if matches!(spec.profile, Profile::Bare) {
        let provider = spec
            .provider
            .as_deref()
            .ok_or_else(|| "bare Codex profile has no provider".to_owned())?;
        let provider_base_url = spec
            .provider_base_url
            .as_deref()
            .ok_or_else(|| "bare Codex profile has no provider_base_url".to_owned())?;
        args.extend([
            "--config".to_owned(),
            format!(
                "model_providers.{BARE_PROVIDER_ID}={{name={},base_url={},env_key={},wire_api=\"responses\",supports_websockets=false}}",
                toml_string(provider)?,
                toml_string(provider_base_url)?,
                toml_string(PROVIDER_TOKEN_ENV)?,
            ),
            "--config".to_owned(),
            format!("model_provider={}", toml_string(BARE_PROVIDER_ID)?),
            "--config".to_owned(),
            "features.enable_request_compression=false".to_owned(),
            "--config".to_owned(),
            "features.multi_agent=false".to_owned(),
            "--config".to_owned(),
            "features.plugins=false".to_owned(),
            "--config".to_owned(),
            "features.apps=false".to_owned(),
            "--config".to_owned(),
            "skills.include_instructions=false".to_owned(),
            "--config".to_owned(),
            "skills.bundled.enabled=false".to_owned(),
            "--config".to_owned(),
            "include_apps_instructions=false".to_owned(),
        ]);
    }
    args.push("-".to_owned());
    Ok(args)
}

fn toml_string(value: &str) -> AppResult<String> {
    serde_json::to_string(value).map_err(|_| "cannot encode TOML string".to_owned())
}

fn validate_loopback_provider_base_url(value: &str) -> AppResult<()> {
    let port = value
        .strip_prefix("http://127.0.0.1:")
        .and_then(|suffix| suffix.strip_suffix("/v1"))
        .ok_or_else(|| {
            "bare Codex provider_base_url must be http://127.0.0.1:<port>/v1".to_owned()
        })?
        .parse::<u16>()
        .map_err(|_| {
            "bare Codex provider_base_url must contain a valid loopback port".to_owned()
        })?;
    if port == 0 {
        return Err("bare Codex provider_base_url port must be nonzero".to_owned());
    }
    Ok(())
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
            provider: Some("yh-loopback".to_owned()),
            provider_base_url: Some("http://127.0.0.1:1234/v1".to_owned()),
            model: "gpt-test".to_owned(),
            reasoning_effort: "medium".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["CODEX_API_KEY".to_owned()],
            home: Some(absolute_path("home")),
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
                .iter()
                .any(|value| value == "--skip-git-repo-check")
        );
        assert!(
            arguments
                .windows(2)
                .any(|pair| pair == ["--sandbox", "read-only"])
        );
        assert!(
            arguments
                .iter()
                .any(|value| value == r#"approval_policy="never""#)
        );
        assert!(
            arguments
                .iter()
                .any(|value| value == r#"model_reasoning_effort="medium""#)
        );
        assert_eq!(arguments.last().map(String::as_str), Some("-"));
        assert!(!arguments.iter().any(|value| value == &spec.prompt));
        assert!(arguments.iter().any(|value| {
            value.contains("model_providers.yh_bench=")
                && value.contains("supports_websockets=false")
        }));
        assert!(
            arguments
                .iter()
                .any(|value| value == "skills.include_instructions=false")
        );
        assert!(
            arguments
                .iter()
                .any(|value| value == "skills.bundled.enabled=false")
        );
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
        product.home = None;
        product.codex_home = None;
        product.provider = None;
        product.provider_base_url = None;
        assert!(validate_spec(&product).is_ok());

        let mut inherited_home = valid_spec();
        inherited_home
            .inherit_environment
            .push("codex_sqlite_home".to_owned());
        assert!(validate_spec(&inherited_home).is_err());

        let mut remote_provider = valid_spec();
        remote_provider.provider_base_url = Some("https://api.openai.com/v1".to_owned());
        assert!(validate_spec(&remote_provider).is_err());
    }

    #[test]
    fn bare_environment_owns_platform_codex_and_sqlite_homes() {
        let root = env::temp_dir().join(format!(
            "yh-codex-environment-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let home = root.join("home");
        let codex_home = root.join("codex-home");
        fs::create_dir_all(&home).expect("create isolated home");
        fs::create_dir(&codex_home).expect("create isolated Codex home");
        let mut spec = valid_spec();
        spec.home = Some(home.clone());
        spec.codex_home = Some(codex_home.clone());
        let mut environment = BTreeMap::new();
        prepare_bare_environment(&spec, &mut environment).expect("prepare bare Codex environment");

        let home = fs::canonicalize(home).expect("canonical home");
        let codex_home = fs::canonicalize(codex_home).expect("canonical Codex home");
        assert_eq!(environment["HOME"], home.to_str().expect("UTF-8 home"));
        assert_eq!(
            environment["CODEX_HOME"],
            codex_home.to_str().expect("UTF-8 Codex home")
        );
        assert_eq!(environment["CODEX_SQLITE_HOME"], environment["CODEX_HOME"]);
        assert!(!environment.contains_key("HOMEDRIVE"));
        assert!(!environment.contains_key("HOMEPATH"));
        fs::remove_dir_all(root).expect("remove isolated environment");
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

    #[test]
    fn checked_in_live_evidence_preserves_request_and_non_claim_boundaries() {
        let report: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-codex-fixed-output/result.json"
        ))
        .expect("checked-in Codex report");
        let request: Value = serde_json::from_str(include_str!(
            "../evidence/2026-07-28-codex-fixed-output/provider-request.jsonl"
        ))
        .expect("checked-in Codex Provider request");
        let provider = include_bytes!("../evidence/2026-07-28-codex-fixed-output/provider.mjs");

        assert_eq!(report["format_version"], RUN_FORMAT_VERSION);
        assert_eq!(report["adapter"]["name"], ADAPTER_VERSION);
        assert_eq!(report["adapter"]["cli_version"], "codex-cli 0.145.0");
        assert_eq!(
            report["adapter"]["adapter_executable_sha256"],
            "02a0dc688c84be6bfe99b5b5273d86654441a8fdd72c7f5abadf7903e7d3af09"
        );
        assert_eq!(
            report["adapter"]["product_executable_sha256"],
            "1da3f4e0e96028b8a771814293c3033dafd1971f943f6c7e79b0897fe705f590"
        );
        assert_eq!(report["controls"]["claim_eligible"], false);
        assert_eq!(
            report["controls"]["requested_provider"],
            "yh-loopback-responses"
        );
        assert_eq!(report["controls"]["requested_model"], "gpt-5.4");
        assert_eq!(report["controls"]["requested_reasoning_effort"], "medium");
        assert_eq!(report["controls"]["product_sandbox"], "read-only");
        assert_eq!(report["controls"]["observed_models"], serde_json::json!([]));
        assert_eq!(report["execution"]["status"], "completed");
        assert!(report["execution"]["settlement"]["actual_cost_usd"].is_null());
        assert_eq!(
            report["execution"]["settlement"]["raw_result"][2]["item"]["text"],
            "YH-CODEX-ADAPTER-OK"
        );
        assert_eq!(
            sha256_bytes(provider),
            "ffdaa14bb95e474ad2a4cfc44ebac5e9ad19d28203a6b9ca565fa2feb7c13782"
        );

        let mut jsonl = Vec::new();
        for event in report["execution"]["settlement"]["raw_result"]
            .as_array()
            .expect("retained Codex events")
        {
            serde_json::to_writer(&mut jsonl, event).expect("encode retained Codex event");
            jsonl.push(b'\n');
        }
        let normalized = normalize_result(&jsonl).expect("normalize retained Codex result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.subtype, "turn.completed");

        assert_eq!(request["ordinal"], 1);
        assert_eq!(request["method"], "POST");
        assert_eq!(request["path"], "/v1/responses");
        assert_eq!(request["authorization"], "bearer-present");
        assert_eq!(request["body"]["model"], "gpt-5.4");
        assert_eq!(request["body"]["stream"], true);
        assert_eq!(request["body"]["reasoning"]["effort"], "medium");
        assert_eq!(request["body"]["instructions"]["has_skills"], false);
        assert_eq!(request["body"]["instructions"]["has_apps"], false);
        assert_eq!(
            request["body"]["tool_names"],
            serde_json::json!([
                "exec_command",
                "write_stdin",
                "update_plan",
                "request_user_input",
                "apply_patch",
                "view_image"
            ])
        );
    }
}
