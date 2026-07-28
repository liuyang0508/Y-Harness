//! Source-pinned adapter for the released Grok Build headless CLI.

use super::*;

const ADAPTER_VERSION: &str = "grok-build-json-v1";
const RUN_FORMAT_VERSION: u32 = 3;
const MAX_TURNS: u64 = 1;
const MAX_MODELS: usize = 64;
const USD_TICKS_PER_USD: f64 = 10_000_000_000.0;
const OWNED_BARE_ENVIRONMENT: [&str; 7] = [
    "HOME",
    "USERPROFILE",
    "HOMEDRIVE",
    "HOMEPATH",
    "GROK_HOME",
    "GROK_MODELS_BASE_URL",
    "GROK_MODELS_LIST_URL",
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
    models_base_url: Option<String>,
    model: String,
    reasoning_effort: String,
    system_prompt: String,
    prompt: String,
    timeout_ms: u64,
    inherit_environment: Vec<String>,
    home: Option<PathBuf>,
    grok_home: Option<PathBuf>,
    prompt_directory: PathBuf,
}

struct NormalizedResult {
    is_error: bool,
    subtype: String,
    num_turns: u64,
    cost: Option<ValidatedCost>,
    observed_models: Vec<String>,
    raw: Value,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ValidatedCost {
    usd: f64,
    ticks: u64,
}

struct PromptFile {
    path: PathBuf,
}

impl Drop for PromptFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) fn read_spec(path: &Path) -> AppResult<RunSpec> {
    let spec: RunSpec = serde_json::from_slice(&read_spec_bytes(path)?)
        .map_err(|error| format!("invalid Grok Build run spec: {error}"))?;
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
    if !spec.prompt_directory.is_absolute() {
        return Err("prompt_directory must be an absolute path".to_owned());
    }
    let inherited = spec
        .inherit_environment
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    match spec.profile {
        Profile::Bare => {
            let (Some(home), Some(grok_home)) = (spec.home.as_ref(), spec.grok_home.as_ref())
            else {
                return Err(
                    "bare Grok Build profile requires absolute home and grok_home directories"
                        .to_owned(),
                );
            };
            if !home.is_absolute() || !grok_home.is_absolute() {
                return Err(
                    "bare Grok Build profile requires absolute home and grok_home directories"
                        .to_owned(),
                );
            }
            let provider = spec
                .provider
                .as_deref()
                .ok_or_else(|| "bare Grok Build profile requires provider".to_owned())?;
            validate_text("Grok Build provider", provider)?;
            let models_base_url = spec
                .models_base_url
                .as_deref()
                .ok_or_else(|| "bare Grok Build profile requires models_base_url".to_owned())?;
            validate_loopback_models_base_url(models_base_url)?;
            if !inherited.contains("XAI_API_KEY") {
                return Err("bare Grok Build profile requires XAI_API_KEY inheritance".to_owned());
            }
            if OWNED_BARE_ENVIRONMENT
                .iter()
                .any(|name| inherited.contains(name))
            {
                return Err(
                    "bare Grok Build profile owns its state and routing environment".to_owned(),
                );
            }
        }
        Profile::Product => {
            if spec.home.is_some()
                || spec.grok_home.is_some()
                || spec.provider.is_some()
                || spec.models_base_url.is_some()
            {
                return Err(
                    "product Grok Build profile must not declare bare-profile state or routing"
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
    let prompt_directory = canonical_empty_directory(&spec.prompt_directory, "prompt_directory")?;
    validate_prompt_boundary(&workspace, &prompt_directory)?;
    let mut environment = inherited_environment(&spec.inherit_environment)?;
    prepare_bare_environment(&spec, &mut environment)?;

    let broker = LocalProcessBroker::new(1).map_err(|error| error.to_string())?;
    let product_executable_sha256 = sha256_file(&program)?;
    if product_executable_sha256 != spec.expected_product_executable_sha256 {
        return Err(format!(
            "Grok Build executable digest mismatch: expected {}, observed {}",
            spec.expected_product_executable_sha256, product_executable_sha256
        ));
    }
    let cli_version =
        read_cli_version(&broker, &program, &workspace, &environment, "Grok Build").await?;
    if cli_version != spec.expected_cli_version {
        return Err(format!(
            "Grok Build version mismatch: expected {:?}, observed {:?}",
            spec.expected_cli_version, cli_version
        ));
    }
    let adapter_executable_sha256 = env::current_exe()
        .map_err(|error| format!("cannot resolve benchmark adapter executable: {error}"))
        .and_then(|path| sha256_file(&path))?;
    let prompt_sha256 = sha256_bytes(spec.prompt.as_bytes());
    let system_prompt_sha256 = sha256_bytes(spec.system_prompt.as_bytes());
    let prompt_file = create_prompt_file(&prompt_directory, spec.prompt.as_bytes())?;
    let started_at_ms = now_ms();
    let started = Instant::now();
    let request = ProcessRequest {
        program,
        args: arguments(&spec, &prompt_file.path)?,
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
                        message: "Grok Build output exceeded the adapter retention bound"
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
                match normalize_result(&output.stdout) {
                    Ok(normalized) => {
                        let observed_models = normalized.observed_models.clone();
                        let (actual_cost_usd, actual_cost_usd_ticks) = normalized
                            .cost
                            .map_or((None, None), |cost| (Some(cost.usd), Some(cost.ticks)));
                        let settlement = ProductSettlement {
                            exit_code: output.code,
                            wall_time_ms,
                            product_duration_ms: None,
                            product_api_duration_ms: None,
                            num_turns: normalized.num_turns,
                            actual_cost_usd,
                            actual_cost_usd_ticks,
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
        "Grok Build exposes no documented hard monetary spend ceiling",
        "read_file and always-on MCP meta-tools remain visible to the Model",
        "the product persists session state even in an isolated bare home",
        "requested_max_turns bounds main-agent rounds, not auxiliary Model calls such as session title generation",
        "workspace_snapshot is caller-asserted rather than adapter-verified",
        "the product read-only sandbox is not independently verified by this adapter",
        "credential values and launcher dependencies are not recorded",
    ];
    if matches!(spec.profile, Profile::Product) {
        unsupported_controls.push("ambient product configuration is not eliminated");
    } else {
        unsupported_controls
            .push("workspace instructions and project compatibility configuration are not ignored");
    }
    #[cfg(windows)]
    unsupported_controls
        .push("the prompt file inherits its caller-provided directory ACL on Windows");

    Ok(ExternalRunReport {
        format_version: RUN_FORMAT_VERSION,
        adapter: AdapterEvidence {
            name: ADAPTER_VERSION,
            version: env!("CARGO_PKG_VERSION"),
            product: "grok-build",
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
            observed_models,
            prompt_sha256,
            system_prompt_sha256,
            tools: "read_file_plus_mcp_meta",
            permission_mode: "dont_ask",
            process_isolation: isolation,
            inherited_environment_names: spec.inherit_environment,
            timeout_ms: spec.timeout_ms,
            requested_max_budget_usd: None,
            requested_reasoning_effort: Some(spec.reasoning_effort),
            requested_max_turns: Some(MAX_TURNS),
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
        .ok_or_else(|| "bare Grok Build profile has no home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "home"))?;
    let grok_home = spec
        .grok_home
        .as_ref()
        .ok_or_else(|| "bare Grok Build profile has no grok_home".to_owned())
        .and_then(|path| canonical_empty_directory(path, "grok_home"))?;
    if home == grok_home {
        return Err("bare Grok Build home and grok_home must be distinct".to_owned());
    }
    let home = utf8_path(&home, "home")?.to_owned();
    let grok_home = utf8_path(&grok_home, "grok_home")?.to_owned();
    let models_base_url = spec
        .models_base_url
        .as_ref()
        .ok_or_else(|| "bare Grok Build profile has no models_base_url".to_owned())?
        .to_owned();
    for name in OWNED_BARE_ENVIRONMENT {
        environment.insert(name.to_owned(), String::new());
    }
    environment.insert("HOME".to_owned(), home.clone());
    environment.insert("USERPROFILE".to_owned(), home);
    environment.insert("GROK_HOME".to_owned(), grok_home);
    environment.insert(
        "GROK_MODELS_LIST_URL".to_owned(),
        format!("{models_base_url}/models"),
    );
    environment.insert("GROK_MODELS_BASE_URL".to_owned(), models_base_url);
    Ok(())
}

fn validate_loopback_models_base_url(value: &str) -> AppResult<()> {
    let port = value
        .strip_prefix("http://127.0.0.1:")
        .and_then(|suffix| suffix.strip_suffix("/v1"))
        .ok_or_else(|| {
            "bare Grok Build models_base_url must be http://127.0.0.1:<port>/v1".to_owned()
        })?
        .parse::<u16>()
        .map_err(|_| {
            "bare Grok Build models_base_url must contain a valid loopback port".to_owned()
        })?;
    if port == 0 {
        return Err("bare Grok Build models_base_url port must be nonzero".to_owned());
    }
    Ok(())
}

fn arguments(spec: &RunSpec, prompt_file: &Path) -> AppResult<Vec<String>> {
    Ok(vec![
        "--prompt-file".to_owned(),
        utf8_path(prompt_file, "prompt_file")?.to_owned(),
        "--verbatim".to_owned(),
        "--output-format".to_owned(),
        "json".to_owned(),
        "--model".to_owned(),
        spec.model.clone(),
        "--reasoning-effort".to_owned(),
        spec.reasoning_effort.clone(),
        "--system-prompt-override".to_owned(),
        spec.system_prompt.clone(),
        "--tools".to_owned(),
        "read_file".to_owned(),
        "--disable-web-search".to_owned(),
        "--no-memory".to_owned(),
        "--no-plan".to_owned(),
        "--no-subagents".to_owned(),
        "--no-ask-user".to_owned(),
        "--permission-mode".to_owned(),
        "dontAsk".to_owned(),
        "--sandbox".to_owned(),
        "read-only".to_owned(),
        "--max-turns".to_owned(),
        MAX_TURNS.to_string(),
        "--no-auto-update".to_owned(),
    ])
}

fn create_prompt_file(directory: &Path, prompt: &[u8]) -> AppResult<PromptFile> {
    for attempt in 0..32_u8 {
        let path = directory.join(format!(
            ".yh-bench-prompt-{}-{}-{attempt}.txt",
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
            Ok(mut file) => {
                if let Err(error) = file.write_all(prompt).and_then(|()| file.flush()) {
                    let _ = fs::remove_file(&path);
                    return Err(format!("cannot write private prompt file: {error}"));
                }
                return Ok(PromptFile { path });
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(format!("cannot create private prompt file: {error}")),
        }
    }
    Err("cannot allocate a unique private prompt file".to_owned())
}

fn validate_prompt_boundary(workspace: &Path, prompt_directory: &Path) -> AppResult<()> {
    if prompt_directory.starts_with(workspace) {
        Err("prompt_directory must be outside the benchmark workspace".to_owned())
    } else {
        Ok(())
    }
}

fn utf8_path<'a>(path: &'a Path, kind: &str) -> AppResult<&'a str> {
    path.to_str()
        .ok_or_else(|| format!("{kind} must be valid UTF-8 for the Grok Build CLI"))
}

fn normalize_result(bytes: &[u8]) -> AppResult<NormalizedResult> {
    let value: Value = serde_json::from_slice(bytes)
        .map_err(|_| "Grok Build stdout is not one JSON result".to_owned())?;
    let object = value
        .as_object()
        .ok_or_else(|| "Grok Build result must be a JSON object".to_owned())?;
    if object.get("type").and_then(Value::as_str) == Some("error") {
        let message = object
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| "Grok Build error has no message".to_owned())?;
        if message.is_empty() || message.len() > MAX_PROMPT_BYTES {
            return Err("Grok Build error message exceeds its bound".to_owned());
        }
        let num_turns = optional_turns(object.get("num_turns"))?;
        let incomplete = incomplete_usage(object)?;
        return Ok(NormalizedResult {
            is_error: true,
            subtype: "error".to_owned(),
            num_turns,
            cost: validated_cost(object, incomplete)?,
            observed_models: observed_models(object, true, incomplete)?,
            raw: value,
        });
    }
    if object.contains_key("type") {
        return Err("Grok Build JSON has an unsupported result type".to_owned());
    }
    let text = object
        .get("text")
        .and_then(Value::as_str)
        .ok_or_else(|| "successful Grok Build result has no text".to_owned())?;
    if text.len() > MAX_PROMPT_BYTES {
        return Err("Grok Build text result exceeds the adapter bound".to_owned());
    }
    for field in ["stopReason", "sessionId", "requestId"] {
        let text = object
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| format!("successful Grok Build result has no {field}"))?;
        validate_text(field, text)?;
    }
    let num_turns = object
        .get("num_turns")
        .and_then(Value::as_u64)
        .ok_or_else(|| "successful Grok Build result has no num_turns".to_owned())?;
    if !(1..=MAX_TURNS).contains(&num_turns) {
        return Err(format!(
            "Grok Build num_turns must be 1-{MAX_TURNS} for this adapter"
        ));
    }
    let incomplete = incomplete_usage(object)?;
    validate_usage(object.get("usage"), incomplete)?;
    let stop_reason = object
        .get("stopReason")
        .and_then(Value::as_str)
        .ok_or_else(|| "successful Grok Build result has no stopReason".to_owned())?
        .to_owned();
    Ok(NormalizedResult {
        is_error: false,
        subtype: stop_reason,
        num_turns,
        cost: validated_cost(object, incomplete)?,
        observed_models: observed_models(object, incomplete, incomplete)?,
        raw: value,
    })
}

fn optional_turns(value: Option<&Value>) -> AppResult<u64> {
    match value {
        None => Ok(0),
        Some(value) => value
            .as_u64()
            .filter(|turns| *turns <= MAX_TURNS)
            .ok_or_else(|| format!("Grok Build num_turns must be 0-{MAX_TURNS}")),
    }
}

fn incomplete_usage(object: &serde_json::Map<String, Value>) -> AppResult<bool> {
    let mut incomplete = false;
    for field in ["usage_is_incomplete", "cost_is_partial"] {
        if let Some(value) = object.get(field) {
            incomplete |= value
                .as_bool()
                .ok_or_else(|| format!("Grok Build {field} must be boolean"))?;
        }
    }
    Ok(incomplete)
}

fn validate_usage(value: Option<&Value>, incomplete: bool) -> AppResult<()> {
    let Some(usage) = value else {
        return if incomplete {
            Ok(())
        } else {
            Err("Grok Build result has no usage object".to_owned())
        };
    };
    let usage = usage
        .as_object()
        .ok_or_else(|| "Grok Build usage must be an object".to_owned())?;
    for (field, value) in usage {
        if field.ends_with("_tokens") && value.as_u64().is_none() {
            return Err(format!("Grok Build usage {field} must be nonnegative"));
        }
    }
    Ok(())
}

fn observed_models(
    object: &serde_json::Map<String, Value>,
    allow_absent: bool,
    incomplete: bool,
) -> AppResult<Vec<String>> {
    let Some(models) = object.get("modelUsage") else {
        return if allow_absent {
            Ok(Vec::new())
        } else {
            Err("Grok Build result has no modelUsage object".to_owned())
        };
    };
    let models = models
        .as_object()
        .ok_or_else(|| "Grok Build modelUsage must be an object".to_owned())?;
    if models.is_empty() || models.len() > MAX_MODELS {
        return Err(format!(
            "Grok Build modelUsage must contain 1-{MAX_MODELS} models"
        ));
    }
    let mut names = Vec::with_capacity(models.len());
    for (name, usage) in models {
        validate_text("observed model", name)?;
        let usage = usage
            .as_object()
            .ok_or_else(|| "Grok Build per-model usage must be an object".to_owned())?;
        if usage
            .get("modelCalls")
            .and_then(Value::as_u64)
            .is_none_or(|calls| calls == 0)
        {
            return Err("Grok Build modelCalls must be a positive integer".to_owned());
        }
        if let Some(cost) = usage.get("costUSD") {
            if incomplete {
                return Err("Grok Build incomplete usage must omit per-model cost".to_owned());
            }
            nonnegative_f64(cost, "modelUsage costUSD")?;
        }
        names.push(name.clone());
    }
    names.sort();
    Ok(names)
}

fn validated_cost(
    object: &serde_json::Map<String, Value>,
    incomplete: bool,
) -> AppResult<Option<ValidatedCost>> {
    let cost = object
        .get("total_cost_usd")
        .map(|value| nonnegative_f64(value, "total_cost_usd"))
        .transpose()?;
    let ticks = object
        .get("total_cost_usd_ticks")
        .map(|value| -> AppResult<u64> {
            let ticks = value.as_u64().ok_or_else(|| {
                "Grok Build total_cost_usd_ticks must be a nonnegative integer".to_owned()
            })?;
            i64::try_from(ticks).map_err(|_| {
                "Grok Build total_cost_usd_ticks exceeds the product's signed integer range"
                    .to_owned()
            })?;
            Ok(ticks)
        })
        .transpose()?;
    if cost.is_some() != ticks.is_some() {
        return Err("Grok Build cost dollars and ticks must be reported together".to_owned());
    }
    if incomplete && cost.is_some() {
        return Err("Grok Build incomplete usage must omit total cost".to_owned());
    }
    match (cost, ticks) {
        (Some(usd), Some(ticks)) => {
            if usd != ticks as f64 / USD_TICKS_PER_USD {
                return Err(
                    "Grok Build total_cost_usd does not match total_cost_usd_ticks".to_owned(),
                );
            }
            Ok(Some(ValidatedCost { usd, ticks }))
        }
        (None, None) => Ok(None),
        _ => unreachable!("cost fields were checked for paired presence"),
    }
}

fn nonnegative_f64(value: &Value, kind: &str) -> AppResult<f64> {
    let value = value
        .as_f64()
        .ok_or_else(|| format!("Grok Build {kind} must be numeric"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("Grok Build {kind} must be finite and nonnegative"));
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
            run_id: "grok-run-1".to_owned(),
            benchmark_version: "adapter-probe-v1".to_owned(),
            case_id: "fixed-output".to_owned(),
            program: absolute_path("grok"),
            expected_cli_version: "grok 0.2.112 (9bbd559437aa)".to_owned(),
            expected_product_executable_sha256: "c".repeat(64),
            workspace: absolute_path("workspace"),
            workspace_snapshot: "empty-fixture".to_owned(),
            profile: Profile::Bare,
            provider: Some("yh-loopback".to_owned()),
            models_base_url: Some("http://127.0.0.1:1234/v1".to_owned()),
            model: "grok-4.5".to_owned(),
            reasoning_effort: "low".to_owned(),
            system_prompt: "Follow the exact response contract.".to_owned(),
            prompt: "Reply exactly YH-OK".to_owned(),
            timeout_ms: 30_000,
            inherit_environment: vec!["XAI_API_KEY".to_owned()],
            home: Some(absolute_path("home")),
            grok_home: Some(absolute_path("grok-home")),
            prompt_directory: absolute_path("prompt"),
        }
    }

    #[test]
    fn bare_profile_owns_home_and_requires_api_key() {
        let spec = valid_spec();
        validate_spec(&spec).expect("valid Grok Build spec");

        let mut inherited_home = spec.clone();
        inherited_home
            .inherit_environment
            .push("GROK_HOME".to_owned());
        assert!(validate_spec(&inherited_home).is_err());

        let mut inherited_endpoint = spec.clone();
        inherited_endpoint
            .inherit_environment
            .push("GROK_MODELS_BASE_URL".to_owned());
        assert!(validate_spec(&inherited_endpoint).is_err());

        let mut invalid_endpoint = spec.clone();
        invalid_endpoint.models_base_url = Some("https://api.x.ai/v1".to_owned());
        assert!(validate_spec(&invalid_endpoint).is_err());
        invalid_endpoint.models_base_url = Some("http://127.0.0.1:0/v1".to_owned());
        assert!(validate_spec(&invalid_endpoint).is_err());

        let mut product = spec.clone();
        product.profile = Profile::Product;
        product.provider = None;
        product.models_base_url = None;
        product.home = None;
        product.grok_home = None;
        validate_spec(&product).expect("valid product profile");
        product.provider = Some("ambient".to_owned());
        assert!(validate_spec(&product).is_err());

        let root = env::temp_dir().join(format!(
            "yh-grok-environment-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let home = root.join("home");
        let grok_home = root.join("grok-home");
        fs::create_dir_all(&home).expect("create isolated home");
        fs::create_dir(&grok_home).expect("create isolated Grok home");
        let mut isolated = spec.clone();
        isolated.home = Some(home.clone());
        isolated.grok_home = Some(grok_home.clone());
        let mut environment = BTreeMap::new();
        prepare_bare_environment(&isolated, &mut environment)
            .expect("prepare bare Grok Build environment");
        let canonical_home = fs::canonicalize(&home).expect("canonical home");
        let canonical_grok_home = fs::canonicalize(&grok_home).expect("canonical Grok home");
        assert_eq!(
            environment["HOME"],
            canonical_home.to_str().expect("UTF-8 home")
        );
        assert_eq!(
            environment["GROK_HOME"],
            canonical_grok_home.to_str().expect("UTF-8 Grok home")
        );
        assert_eq!(
            environment["GROK_MODELS_LIST_URL"],
            "http://127.0.0.1:1234/v1/models"
        );
        assert_eq!(environment["HOMEDRIVE"], "");
        fs::remove_dir_all(root).expect("remove isolated environment");

        let mut missing_key = spec;
        missing_key.inherit_environment.clear();
        assert!(validate_spec(&missing_key).is_err());
    }

    #[test]
    fn command_is_bounded_read_only_and_keeps_prompt_out_of_arguments() {
        let spec = valid_spec();
        let prompt_path = absolute_path("private-prompt");
        let args = arguments(&spec, &prompt_path).expect("Grok Build arguments");
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--sandbox", "read-only"])
        );
        assert!(args.windows(2).any(|pair| pair == ["--tools", "read_file"]));
        assert!(args.windows(2).any(|pair| pair == ["--max-turns", "1"]));
        assert!(args.iter().any(|value| value == "--no-memory"));
        assert!(!args.iter().any(|value| value == &spec.prompt));
    }

    #[test]
    fn prompt_directory_cannot_be_nested_under_the_workspace() {
        let workspace = absolute_path("workspace");
        assert!(validate_prompt_boundary(&workspace, &workspace.join("prompt")).is_err());
        assert!(validate_prompt_boundary(&workspace, &absolute_path("prompt")).is_ok());
    }

    #[test]
    fn success_preserves_observed_model_usage_and_exact_cost() {
        let result = br#"{
          "text":"YH-OK",
          "stopReason":"EndTurn",
          "sessionId":"session-1",
          "requestId":"request-1",
          "num_turns":1,
          "usage":{"input_tokens":10,"cache_read_input_tokens":2,"output_tokens":3,"total_tokens":15},
          "modelUsage":{"grok-4.5":{"inputTokens":10,"outputTokens":3,"cacheReadInputTokens":2,"modelCalls":1,"costUSD":0.000001}},
          "total_cost_usd":0.000001,
          "total_cost_usd_ticks":10000
        }"#;
        let normalized = normalize_result(result).expect("valid Grok Build result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.subtype, "EndTurn");
        assert_eq!(normalized.observed_models, ["grok-4.5"]);
        assert_eq!(
            normalized.cost,
            Some(ValidatedCost {
                usd: 0.000001,
                ticks: 10_000
            })
        );
    }

    #[test]
    fn product_error_without_usage_remains_valid_evidence() {
        let result = br#"{"type":"error","message":"provider unavailable"}"#;
        let normalized = normalize_result(result).expect("valid Grok Build error");
        assert!(normalized.is_error);
        assert_eq!(normalized.num_turns, 0);
        assert!(normalized.observed_models.is_empty());
        assert_eq!(normalized.cost, None);
    }

    #[test]
    fn mismatched_float_and_exact_cost_are_rejected() {
        let error = normalize_result(
            br#"{
              "text":"ok","stopReason":"end_turn","sessionId":"s","requestId":"r",
              "num_turns":1,
              "usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2},
              "modelUsage":{"grok-4.5":{"inputTokens":1,"outputTokens":1,"modelCalls":1}},
              "total_cost_usd":0.000002,
              "total_cost_usd_ticks":10000
            }"#,
        )
        .err()
        .expect("mismatched cost evidence must fail");
        assert!(error.contains("does not match"));
    }

    #[test]
    fn private_prompt_file_is_exact_and_removed_on_drop() {
        let directory = std::env::temp_dir().join(format!(
            "yh-grok-prompt-test-{}-{}",
            std::process::id(),
            now_ms()
        ));
        fs::create_dir(&directory).expect("create prompt test directory");
        let path = {
            let prompt = create_prompt_file(&directory, b"sensitive prompt")
                .expect("create private prompt file");
            assert_eq!(
                fs::read(&prompt.path).expect("read private prompt file"),
                b"sensitive prompt"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&prompt.path)
                        .expect("prompt metadata")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
            prompt.path.clone()
        };
        assert!(!path.exists());
        fs::remove_dir(&directory).expect("remove prompt test directory");
    }

    #[test]
    fn checked_in_live_evidence_preserves_auxiliary_call_and_non_claim_boundaries() {
        let report: Value = serde_json::from_slice(include_bytes!(
            "../evidence/2026-07-28-grok-build-fixed-output/result.json"
        ))
        .expect("checked-in Grok Build report");
        let requests =
            include_str!("../evidence/2026-07-28-grok-build-fixed-output/provider-request.jsonl")
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).expect("Provider request JSON"))
                .collect::<Vec<_>>();
        let provider =
            include_bytes!("../evidence/2026-07-28-grok-build-fixed-output/provider.mjs");

        assert_eq!(report["format_version"], RUN_FORMAT_VERSION);
        assert_eq!(
            report["adapter"]["cli_version"],
            "grok 0.2.112 (9bbd559437aa)"
        );
        assert_eq!(
            report["adapter"]["product_executable_sha256"],
            "5cf05fe670b1818561daf7566b580a5de6b81149166499d61072e49640b541a4"
        );
        assert_eq!(report["controls"]["claim_eligible"], false);
        assert_eq!(
            report["controls"]["requested_provider"],
            "yh-loopback-responses"
        );
        assert_eq!(report["controls"]["requested_model"], "grok-4.5");
        assert_eq!(
            report["controls"]["observed_models"],
            serde_json::json!(["grok-4.5"])
        );
        assert_eq!(report["execution"]["status"], "completed");
        assert_eq!(report["execution"]["settlement"]["num_turns"], 1);
        assert!(report["execution"]["settlement"]["actual_cost_usd"].is_null());
        assert_eq!(
            report["execution"]["settlement"]["raw_result"]["text"],
            "YH-GROK-BUILD-ADAPTER-OK"
        );
        assert_eq!(
            sha256_bytes(provider),
            "db0caf5a4c407e5734bb864a8a6414940f4959fca45f9bf87d4ab7deb9ed6df3"
        );

        let normalized = normalize_result(
            &serde_json::to_vec(&report["execution"]["settlement"]["raw_result"])
                .expect("encode retained Grok Build result"),
        )
        .expect("normalize retained Grok Build result");
        assert!(!normalized.is_error);
        assert_eq!(normalized.num_turns, 1);
        assert_eq!(normalized.observed_models, ["grok-4.5"]);
        assert_eq!(normalized.cost, None);

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0]["path"], "/v1/models");
        assert_eq!(requests[0]["authorization"], "bearer-present");
        for request in &requests[1..] {
            assert_eq!(request["path"], "/v1/responses");
            assert_eq!(request["authorization"], "bearer-present");
            assert_eq!(request["body"]["model"], "grok-4.5");
            assert_eq!(request["body"]["stream"], true);
        }
        assert_eq!(
            requests[1]["body"]["tool_names"],
            serde_json::json!(["session_title"])
        );
        assert_eq!(
            requests[2]["body"]["tool_names"],
            serde_json::json!(["read_file", "search_tool", "use_tool"])
        );
        assert_eq!(
            report["execution"]["settlement"]["raw_result"]["modelUsage"]["grok-4.5"]["modelCalls"],
            1
        );
    }
}
