//! Bounded external-process execution and JSON command capability adapters.

mod compensation;
mod digest;
mod effect;

pub use compensation::{
    CompensationContext, CompensationDescriptor, CompensationRequest, CompensationTool,
    ToolCompensator,
};
pub use digest::{DigestLockedProcessBroker, MAX_DIGEST_LOCKED_PROGRAM_BYTES};
pub use effect::{
    EffectSecretEnvironment, JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION, JsonCommandEffectConnector,
    JsonCommandEffectReconciliationConnector, JsonEffectExecutionRequest,
    JsonEffectExecutionResponse, JsonEffectReconciliationRequest, JsonEffectReconciliationResponse,
    MAX_EFFECT_SECRET_ENVIRONMENT_ENTRIES,
};

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::Semaphore,
    task::JoinHandle,
    time::{Instant, timeout_at},
};

#[cfg(unix)]
use tokio::time::sleep;

#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};

use crate::{
    CancellationToken, ConversationCompactionRequest, ConversationCompactionResponse,
    ConversationCompactionTurn, ConversationCompactor, ConversationCompactorDescriptor,
    EvaluationCase, EvaluationExecution, EvaluationSample, ExecutionPhase, Grade, Grader,
    GraderDescriptor, HarnessError, HarnessFuture, LanguageModel, ModelContinuation, ModelOutput,
    ModelProviderFailure, ModelProviderFailureKind, ModelRequest, ModelResponse, ModelStream,
    ModelUsage, SecretValue, ThreadId, Tool, ToolBatchExecution, ToolContext, ToolDescriptor,
    TurnId, VerificationOutcome, VerificationRequest, Verifier, VerifierDescriptor,
    kernel::{capture_capability_metadata, validate_capability_name, validate_model_id},
};

const MAX_ARGUMENTS: usize = 256;
const MAX_ARGUMENT_BYTES: usize = 16_384;
const MAX_ENVIRONMENT_ENTRIES: usize = 256;
const MAX_ENVIRONMENT_BYTES: usize = 65_536;
const MAX_STDIN_BYTES: usize = 1_048_576;
const MAX_GRADER_STDIN_BYTES: usize = 4_194_304;
const MAX_OUTPUT_BYTES: usize = 16_777_216;
const MAX_PROCESS_CONCURRENCY: usize = 4_096;
const MAX_PROCESS_TIMEOUT: Duration = Duration::from_secs(86_400);
const PROCESS_TERMINATION_GRACE: Duration = Duration::from_secs(5);
#[cfg(target_os = "macos")]
const MAX_WRITABLE_ROOTS: usize = 32;

/// Maximum encoded stdin accepted by every JSON-command adapter.
pub const JSON_COMMAND_MAX_INPUT_BYTES: usize = MAX_STDIN_BYTES;

/// Maximum encoded stdin accepted by the JSON-command Grader adapter.
pub const JSON_GRADER_MAX_INPUT_BYTES: usize = MAX_GRADER_STDIN_BYTES;

/// Isolation strength honestly reported by a Process Broker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessIsolation {
    /// Execution is disabled.
    Denied,
    /// Child process has the same operating-system authority as the Runtime.
    Unrestricted,
    /// A named operating-system isolation mechanism is enforced by the broker.
    Sandboxed {
        /// Stable mechanism identity, such as `linux-landlock`.
        mechanism: String,
    },
}

/// Network authority granted by a concrete sandbox broker.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkAccess {
    /// Deny all network operations covered by the platform sandbox.
    Deny,
    /// Retain the Runtime user's network authority.
    Allow,
}

/// Measured integrity enforced by one Process Broker.
///
/// A dispatch-time digest detects command-file drift before every launch. It
/// does not atomically bind the measured file to the operating-system exec,
/// cover interpreters or dynamic dependencies, or contain an unrestricted
/// process.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProcessExecutableIntegrity {
    /// The broker does not measure executable content.
    #[default]
    Unmeasured,
    /// The broker remeasures one exact command file before every dispatch.
    DispatchSha256 {
        /// Expected lowercase SHA-256 command-file digest.
        sha256: String,
    },
}

impl ProcessExecutableIntegrity {
    fn is_unmeasured(&self) -> bool {
        matches!(self, Self::Unmeasured)
    }
}

/// Trust-bearing description of one external execution broker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessBrokerDescriptor {
    /// Stable broker identity.
    pub name: String,
    /// Enforced isolation strength.
    pub isolation: ProcessIsolation,
    /// Executable-integrity measurement enforced by this broker.
    #[serde(
        default,
        skip_serializing_if = "ProcessExecutableIntegrity::is_unmeasured"
    )]
    pub executable_integrity: ProcessExecutableIntegrity,
}

/// One bounded, shell-free child-process request.
pub struct ProcessRequest {
    /// Absolute executable path.
    pub program: PathBuf,
    /// Fixed arguments passed without shell expansion.
    pub args: Vec<String>,
    /// Absolute child working directory.
    pub current_dir: PathBuf,
    /// Exact environment after the inherited environment is cleared.
    pub environment: BTreeMap<String, String>,
    /// Short-lived zeroizing environment resolved immediately before dispatch.
    ///
    /// These values are never serializable or cloneable. Zeroization covers
    /// only these `SecretValue` buffers; the process launcher, operating
    /// system, and child process necessarily receive unproved copies.
    pub secret_environment: BTreeMap<String, SecretValue>,
    /// Bytes written to child stdin before it is closed.
    ///
    /// Live Runtime phases accept at most [`JSON_COMMAND_MAX_INPUT_BYTES`];
    /// Evaluation accepts at most [`JSON_GRADER_MAX_INPUT_BYTES`].
    pub stdin: Vec<u8>,
    /// Per-process total queue-and-execution timeout.
    pub timeout: Duration,
    /// Maximum retained bytes for each output stream.
    pub max_output_bytes: usize,
    /// Runtime phase used if cooperative cancellation wins.
    pub cancellation_phase: ExecutionPhase,
}

/// Bounded child-process settlement.
#[derive(Clone, Eq, PartialEq)]
pub struct ProcessOutput {
    /// Whether the process reported successful exit.
    pub success: bool,
    /// Platform exit code, absent when terminated by a signal or equivalent.
    pub code: Option<i32>,
    /// Retained stdout bytes.
    pub stdout: Vec<u8>,
    /// Retained stderr bytes.
    pub stderr: Vec<u8>,
    /// Whether stdout exceeded its retention limit.
    pub stdout_truncated: bool,
    /// Whether stderr exceeded its retention limit.
    pub stderr_truncated: bool,
}

/// Replaceable authority boundary for launching external executables.
pub trait ProcessBroker: Send + Sync {
    /// Reports the isolation that this broker actually enforces.
    fn descriptor(&self) -> ProcessBrokerDescriptor;
    /// Executes one validated request without invoking a shell.
    fn execute<'a>(
        &'a self,
        request: ProcessRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ProcessOutput>;
}

/// Secure default broker that rejects every executable request.
pub struct DenyProcessBroker;

impl ProcessBroker for DenyProcessBroker {
    fn descriptor(&self) -> ProcessBrokerDescriptor {
        ProcessBrokerDescriptor {
            name: "deny".to_owned(),
            isolation: ProcessIsolation::Denied,
            executable_integrity: ProcessExecutableIntegrity::Unmeasured,
        }
    }

    fn execute<'a>(
        &'a self,
        _request: ProcessRequest,
        _cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ProcessOutput> {
        Box::pin(async {
            Err(HarnessError::Execution(
                "external process execution is disabled".to_owned(),
            ))
        })
    }
}

/// Explicitly unrestricted local child-process broker.
///
/// This broker clears inherited environment variables, requires absolute paths,
/// avoids a shell, and bounds concurrency/time/output. On Unix, every child
/// leads a private process group that is killed on completion, cancellation,
/// timeout, or future drop. Other platforms currently guarantee direct-child
/// settlement only. Cooperative cancellation also applies while settling
/// piped I/O after child exit. This broker does **not** restrict filesystem or
/// network authority.
pub struct LocalProcessBroker {
    concurrency: Arc<Semaphore>,
    maximum_concurrency: usize,
}

impl LocalProcessBroker {
    /// Creates a broker with a hard child-process concurrency limit.
    pub fn new(maximum_concurrency: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_PROCESS_CONCURRENCY).contains(&maximum_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "process maximum_concurrency must be 1-{MAX_PROCESS_CONCURRENCY}"
            )));
        }
        Ok(Self {
            concurrency: Arc::new(Semaphore::new(maximum_concurrency)),
            maximum_concurrency,
        })
    }
}

impl ProcessBroker for LocalProcessBroker {
    fn descriptor(&self) -> ProcessBrokerDescriptor {
        ProcessBrokerDescriptor {
            name: format!("local-unrestricted-{}", self.maximum_concurrency),
            isolation: ProcessIsolation::Unrestricted,
            executable_integrity: ProcessExecutableIntegrity::Unmeasured,
        }
    }

    fn execute<'a>(
        &'a self,
        request: ProcessRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ProcessOutput> {
        Box::pin(async move {
            validate_request(&request)?;
            let deadline = Instant::now().checked_add(request.timeout).ok_or_else(|| {
                HarnessError::Execution("process timeout exceeds runtime clock".to_owned())
            })?;
            let _permit = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    return Err(HarnessError::Cancelled {
                        phase: request.cancellation_phase,
                    });
                }
                acquired = timeout_at(deadline, self.concurrency.acquire()) => {
                    acquired
                        .map_err(|_| HarnessError::Execution("process queue timed out".to_owned()))?
                        .map_err(|_| HarnessError::Execution("process broker is closed".to_owned()))?
                }
            };

            let mut command = Command::new(&request.program);
            let secret_environment = request
                .secret_environment
                .iter()
                .map(|(name, value)| {
                    value
                        .expose_str()
                        .map(|value| (name.as_str(), value))
                        .map_err(|_| {
                            HarnessError::Execution(
                                "process secret environment contains a non-UTF-8 value".to_owned(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            command
                .args(&request.args)
                .current_dir(&request.current_dir)
                .env_clear()
                .envs(&request.environment)
                .envs(secret_environment)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            configure_process_group(&mut command);
            let mut child = command.spawn().map_err(|error| {
                HarnessError::Execution(format!("failed to start configured executable: {error}"))
            })?;
            let mut process_group = ChildProcessGroup::for_child(&child)?;
            let child_stdin = child
                .stdin
                .take()
                .ok_or_else(|| HarnessError::Execution("child stdin was not piped".to_owned()))?;
            let child_stdout = child
                .stdout
                .take()
                .ok_or_else(|| HarnessError::Execution("child stdout was not piped".to_owned()))?;
            let child_stderr = child
                .stderr
                .take()
                .ok_or_else(|| HarnessError::Execution("child stderr was not piped".to_owned()))?;

            let mut input_task = tokio::spawn(write_stdin(child_stdin, request.stdin));
            let mut stdout_task =
                tokio::spawn(read_bounded(child_stdout, request.max_output_bytes));
            let mut stderr_task =
                tokio::spawn(read_bounded(child_stderr, request.max_output_bytes));

            let status = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    abort_io(&input_task, &stdout_task, &stderr_task);
                    if !terminate(&mut child, &mut process_group).await {
                        return Err(HarnessError::Execution(
                            "cancelled child process group did not settle within termination grace"
                                .to_owned(),
                        ));
                    }
                    return Err(HarnessError::Cancelled {
                        phase: request.cancellation_phase,
                    });
                }
                waited = timeout_at(deadline, child.wait()) => {
                    match waited {
                        Ok(Ok(status)) => status,
                        Ok(Err(error)) => {
                            abort_io(&input_task, &stdout_task, &stderr_task);
                            let settled = terminate(&mut child, &mut process_group).await;
                            let cleanup = if settled {
                                ""
                            } else {
                                "; child process group also missed termination grace"
                            };
                            return Err(HarnessError::Execution(format!(
                                "failed while waiting for child process: {error}{cleanup}"
                            )));
                        }
                        Err(_) => {
                            abort_io(&input_task, &stdout_task, &stderr_task);
                            if !terminate(&mut child, &mut process_group).await {
                                return Err(HarnessError::Execution(
                                    "process timeout elapsed and child process group did not settle within termination grace"
                                        .to_owned(),
                                ));
                            }
                            return Err(HarnessError::Execution(
                                "process queue-and-execution timeout elapsed".to_owned(),
                            ));
                        }
                    }
                }
            };
            if !process_group.settle_remaining().await {
                abort_io(&input_task, &stdout_task, &stderr_task);
                return Err(HarnessError::Execution(
                    "descendant process group did not settle after direct-child exit".to_owned(),
                ));
            }

            if let Err(error) = join_io(
                deadline,
                &mut input_task,
                "stdin",
                &cancellation,
                request.cancellation_phase,
            )
            .await
            {
                abort_io(&input_task, &stdout_task, &stderr_task);
                return Err(error);
            }
            let (stdout, stdout_truncated) = match join_io(
                deadline,
                &mut stdout_task,
                "stdout",
                &cancellation,
                request.cancellation_phase,
            )
            .await
            {
                Ok(output) => output,
                Err(error) => {
                    abort_io(&input_task, &stdout_task, &stderr_task);
                    return Err(error);
                }
            };
            let (stderr, stderr_truncated) = match join_io(
                deadline,
                &mut stderr_task,
                "stderr",
                &cancellation,
                request.cancellation_phase,
            )
            .await
            {
                Ok(output) => output,
                Err(error) => {
                    abort_io(&input_task, &stdout_task, &stderr_task);
                    return Err(error);
                }
            };
            Ok(ProcessOutput {
                success: status.success(),
                code: status.code(),
                stdout,
                stderr,
                stdout_truncated,
                stderr_truncated,
            })
        })
    }
}

/// Reusable macOS Seatbelt command policy for external-process authorities.
///
/// Reads remain allowed so dynamically linked executables can load system and
/// configured resources. Writes are allowed only below canonical roots supplied
/// by the embedding host. This is a concrete but intentionally scoped sandbox,
/// not a claim of complete process isolation.
#[derive(Clone)]
pub(crate) struct MacOsSeatbeltPolicy {
    #[cfg(target_os = "macos")]
    writable_roots: Vec<String>,
    #[cfg(target_os = "macos")]
    network_access: NetworkAccess,
}

impl MacOsSeatbeltPolicy {
    pub(crate) fn new(
        writable_roots: Vec<PathBuf>,
        network_access: NetworkAccess,
    ) -> Result<Self, HarnessError> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (writable_roots, network_access);
            return Err(HarnessError::InvalidConfiguration(
                "macOS Seatbelt is unavailable on this platform".to_owned(),
            ));
        }
        #[cfg(target_os = "macos")]
        {
            if writable_roots.len() > MAX_WRITABLE_ROOTS {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "Seatbelt writable roots exceed {MAX_WRITABLE_ROOTS}"
                )));
            }
            if !PathBuf::from("/usr/bin/sandbox-exec").is_file() {
                return Err(HarnessError::InvalidConfiguration(
                    "macOS sandbox-exec is unavailable".to_owned(),
                ));
            }
            let mut canonical_roots = writable_roots
                .into_iter()
                .map(|root| {
                    std::fs::canonicalize(&root)
                        .map_err(|error| {
                            HarnessError::InvalidConfiguration(format!(
                                "cannot canonicalize Seatbelt writable root: {error}"
                            ))
                        })?
                        .into_os_string()
                        .into_string()
                        .map_err(|_| {
                            HarnessError::InvalidConfiguration(
                                "Seatbelt writable roots must be valid UTF-8".to_owned(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            canonical_roots.sort();
            canonical_roots.dedup();
            Ok(Self {
                writable_roots: canonical_roots,
                network_access,
            })
        }
    }

    #[cfg(target_os = "macos")]
    fn profile(&self) -> String {
        let mut profile = "(version 1) (allow default)".to_owned();
        if self.network_access == NetworkAccess::Deny {
            profile.push_str(" (deny network*)");
        }
        if self.writable_roots.is_empty() {
            profile.push_str(" (deny file-write* (require-not (literal \"/dev/null\")))");
        } else {
            profile
                .push_str(" (deny file-write* (require-not (require-any (literal \"/dev/null\")");
            for index in 0..self.writable_roots.len() {
                profile.push_str(&format!(" (subpath (param \"YH_WRITE_{index}\"))"));
            }
            profile.push_str(")))");
        }
        profile
    }

    pub(crate) fn wrap_command(
        &self,
        program: &Path,
        original_args: Vec<String>,
    ) -> Result<(PathBuf, Vec<String>), HarnessError> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (program, original_args);
            Err(HarnessError::InvalidConfiguration(
                "macOS Seatbelt is unavailable on this platform".to_owned(),
            ))
        }
        #[cfg(target_os = "macos")]
        {
            let original_program = program
                .to_str()
                .ok_or_else(|| {
                    HarnessError::InvalidConfiguration(
                        "Seatbelt executable path must be valid UTF-8".to_owned(),
                    )
                })?
                .to_owned();
            let mut sandbox_args =
                Vec::with_capacity(4 + self.writable_roots.len() * 2 + original_args.len());
            for (index, root) in self.writable_roots.iter().enumerate() {
                sandbox_args.push("-D".to_owned());
                sandbox_args.push(format!("YH_WRITE_{index}={root}"));
            }
            sandbox_args.push("-p".to_owned());
            sandbox_args.push(self.profile());
            sandbox_args.push(original_program);
            sandbox_args.extend(original_args);
            Ok((PathBuf::from("/usr/bin/sandbox-exec"), sandbox_args))
        }
    }
}

/// macOS Seatbelt broker that restricts network and filesystem writes.
pub struct MacOsSeatbeltBroker {
    #[cfg(target_os = "macos")]
    local: LocalProcessBroker,
    #[cfg(target_os = "macos")]
    policy: MacOsSeatbeltPolicy,
}

impl MacOsSeatbeltBroker {
    /// Creates a Seatbelt broker from existing canonicalizable writable roots.
    pub fn new(
        maximum_concurrency: usize,
        writable_roots: Vec<PathBuf>,
        network_access: NetworkAccess,
    ) -> Result<Self, HarnessError> {
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (maximum_concurrency, writable_roots, network_access);
            return Err(HarnessError::InvalidConfiguration(
                "macOS Seatbelt is unavailable on this platform".to_owned(),
            ));
        }
        #[cfg(target_os = "macos")]
        {
            Ok(Self {
                local: LocalProcessBroker::new(maximum_concurrency)?,
                policy: MacOsSeatbeltPolicy::new(writable_roots, network_access)?,
            })
        }
    }
}

impl ProcessBroker for MacOsSeatbeltBroker {
    fn descriptor(&self) -> ProcessBrokerDescriptor {
        ProcessBrokerDescriptor {
            name: "macos-seatbelt".to_owned(),
            isolation: ProcessIsolation::Sandboxed {
                mechanism: "macos-seatbelt-write-network".to_owned(),
            },
            executable_integrity: ProcessExecutableIntegrity::Unmeasured,
        }
    }

    fn execute<'a>(
        &'a self,
        mut request: ProcessRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ProcessOutput> {
        Box::pin(async move {
            #[cfg(not(target_os = "macos"))]
            {
                let _ = (request, cancellation);
                Err(HarnessError::Execution(
                    "macOS Seatbelt is unavailable on this platform".to_owned(),
                ))
            }
            #[cfg(target_os = "macos")]
            {
                let (program, args) = self
                    .policy
                    .wrap_command(&request.program, std::mem::take(&mut request.args))?;
                request.program = program;
                request.args = args;
                self.local.execute(request, cancellation).await
            }
        })
    }
}

/// Shared shell-free JSON process adapter configuration.
#[derive(Clone)]
pub struct JsonProcessConfig {
    /// Absolute executable path.
    pub program: PathBuf,
    /// Fixed command arguments.
    pub args: Vec<String>,
    /// Absolute child working directory.
    pub current_dir: PathBuf,
    /// Exact child environment after environment clearing.
    pub environment: BTreeMap<String, String>,
    /// Per-call queue-and-execution timeout.
    pub timeout: Duration,
    /// Per-stream retained output limit.
    pub max_output_bytes: usize,
}

impl JsonProcessConfig {
    /// Validates static process bounds without starting or authorizing a child.
    pub fn validate(&self) -> Result<(), HarnessError> {
        validate_request(&self.request(Vec::new(), ExecutionPhase::Tool))
    }

    fn request(&self, stdin: Vec<u8>, cancellation_phase: ExecutionPhase) -> ProcessRequest {
        ProcessRequest {
            program: self.program.clone(),
            args: self.args.clone(),
            current_dir: self.current_dir.clone(),
            environment: self.environment.clone(),
            secret_environment: BTreeMap::new(),
            stdin,
            timeout: self.timeout,
            max_output_bytes: self.max_output_bytes,
            cancellation_phase,
        }
    }
}

/// JSON envelope delivered to an external CLI Tool on stdin.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JsonToolRequest {
    /// Proposed model input.
    pub input: Value,
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Active Turn.
    pub turn_id: TurnId,
    /// Model-generated tool-call correlation identity.
    pub call_id: String,
}

/// External JSON command exposed through the normal Tool registry and Policy.
pub struct JsonCommandTool {
    descriptor: ToolDescriptor,
    batch_execution: ToolBatchExecution,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
}

impl JsonCommandTool {
    /// Creates a Tool adapter after validating its static process configuration.
    pub fn new(
        descriptor: ToolDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            batch_execution: ToolBatchExecution::Sequential,
            config,
            broker,
            broker_descriptor,
        })
    }

    /// Returns the broker isolation visible to operators and Policy wiring.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }

    /// Installs an explicit same-response scheduling guarantee.
    #[must_use]
    pub fn with_batch_execution(mut self, execution: ToolBatchExecution) -> Self {
        self.batch_execution = execution;
        self
    }
}

impl Tool for JsonCommandTool {
    fn descriptor(&self) -> ToolDescriptor {
        self.descriptor.clone()
    }

    fn batch_execution(&self) -> ToolBatchExecution {
        self.batch_execution
    }

    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            crate::json::validate_value_shape(&input).map_err(|_| {
                HarnessError::Tool(
                    "CLI input exceeds the supported JSON depth or node count".to_owned(),
                )
            })?;
            let request = JsonToolRequest {
                input,
                thread_id: context.thread_id,
                turn_id: context.turn_id,
                call_id: context.call_id,
            };
            let stdin =
                crate::json::to_bounded_json_vec(&request, MAX_STDIN_BYTES).map_err(|error| {
                    match error {
                        crate::json::BoundedJsonError::LimitExceeded => {
                            HarnessError::Tool(format!("CLI input exceeds {MAX_STDIN_BYTES} bytes"))
                        }
                        crate::json::BoundedJsonError::CannotEncode => {
                            HarnessError::Tool("cannot encode CLI input".to_owned())
                        }
                    }
                })?;
            let output = self
                .broker
                .execute(
                    self.config.request(stdin, ExecutionPhase::Tool),
                    context.cancellation,
                )
                .await
                .map_err(map_tool_execution_error)?;
            validate_process_success(&output, "CLI tool").map_err(HarnessError::Tool)?;
            let value: Value = serde_json::from_slice(&output.stdout)
                .map_err(|error| HarnessError::Tool(format!("invalid CLI JSON output: {error}")))?;
            crate::json::validate_value_shape(&value).map_err(|_| {
                HarnessError::Tool(
                    "CLI output exceeds the supported JSON depth or node count".to_owned(),
                )
            })?;
            Ok(value)
        })
    }
}

/// Versioned stdout contract for a JSON-command Model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonCommandModelProtocol {
    /// Backward-compatible bare [`ModelOutput`] response.
    #[default]
    OutputV1,
    /// Strict [`JsonModelSettlement`] with Provider evidence or failure facts.
    SettlementV1,
}

/// Strict terminal response returned by a settlement-v1 JSON-command Model.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonModelSettlement {
    /// Authoritative Model response and optional Provider-reported evidence.
    Completed {
        /// Decision consumed by the Agent Loop.
        output: ModelOutput,
        /// Provider-reported accounting, omitted when unavailable or incomplete.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage: Option<ModelUsage>,
        /// Provider-reported Model that settled this call.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_model: Option<String>,
        /// Opaque Provider request identity for support correlation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_request_id: Option<String>,
        /// Provider-owned state required to continue this response.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        continuation: Option<ModelContinuation>,
    },
    /// Sanitized Provider failure evidence independent from retry policy.
    Failed {
        /// Stable failure classification.
        kind: ModelProviderFailureKind,
        /// Bounded, non-secret diagnostic prepared by the adapter.
        message: String,
        /// Provider HTTP status when one was observed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        http_status: Option<u16>,
        /// Exact Provider-requested retry delay when one was observed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after_ms: Option<u64>,
    },
}

/// Language Model adapter backed by one shell-free JSON command.
///
/// A serialized [`ModelRequest`] is sent on stdin. The default stdout remains
/// one bare [`ModelOutput`]; callers may explicitly select settlement v1.
pub struct JsonCommandModel {
    id: String,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
    protocol: JsonCommandModelProtocol,
}

impl JsonCommandModel {
    /// Creates an external model adapter with a validated stable identity.
    pub fn new(
        id: impl Into<String>,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        let id = id.into();
        validate_model_id(&id)?;
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            id,
            config,
            broker,
            broker_descriptor,
            protocol: JsonCommandModelProtocol::OutputV1,
        })
    }

    /// Selects the explicit stdout protocol without changing process authority.
    #[must_use]
    pub fn with_protocol(mut self, protocol: JsonCommandModelProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Returns the broker isolation visible to the embedding host.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }

    async fn execute(
        &self,
        request: ModelRequest,
        cancellation: CancellationToken,
    ) -> Result<ModelResponse, HarnessError> {
        crate::runtime::validate_model_request(&request)?;
        let stdin = crate::json::to_bounded_json_vec(&request, MAX_STDIN_BYTES).map_err(
            |error| match error {
                crate::json::BoundedJsonError::LimitExceeded => {
                    HarnessError::Model(format!("model request exceeds {MAX_STDIN_BYTES} bytes"))
                }
                crate::json::BoundedJsonError::CannotEncode => {
                    HarnessError::Model("cannot encode model request".to_owned())
                }
            },
        )?;
        let output = self
            .broker
            .execute(
                self.config.request(stdin, ExecutionPhase::Model),
                cancellation,
            )
            .await
            .map_err(map_model_execution_error)?;
        validate_process_success(&output, "model command").map_err(HarnessError::Model)?;
        let response = match self.protocol {
            JsonCommandModelProtocol::OutputV1 => {
                let output: ModelOutput =
                    serde_json::from_slice(&output.stdout).map_err(|error| {
                        HarnessError::Model(format!("invalid model command JSON output: {error}"))
                    })?;
                ModelResponse::from(output)
            }
            JsonCommandModelProtocol::SettlementV1 => {
                let settlement: JsonModelSettlement = serde_json::from_slice(&output.stdout)
                    .map_err(|_| {
                        HarnessError::Model("invalid model command settlement JSON".to_owned())
                    })?;
                match settlement {
                    JsonModelSettlement::Completed {
                        output,
                        usage,
                        provider_model,
                        provider_request_id,
                        continuation,
                    } => ModelResponse {
                        output,
                        usage,
                        provider_model,
                        provider_request_id,
                        continuation,
                    },
                    JsonModelSettlement::Failed {
                        kind,
                        message,
                        http_status,
                        retry_after_ms,
                    } => {
                        let failure =
                            ModelProviderFailure::new(kind, message, http_status, retry_after_ms)
                                .map_err(|_| {
                                HarnessError::Model(
                                    "invalid model command Provider failure evidence".to_owned(),
                                )
                            })?;
                        return Err(HarnessError::ModelProvider(failure));
                    }
                }
            }
        };
        crate::runtime::validate_model_response(&response)?;
        Ok(response)
    }
}

impl LanguageModel for JsonCommandModel {
    fn id(&self) -> &str {
        &self.id
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move {
            self.execute(request, CancellationToken::new())
                .await
                .map(|response| response.output)
        })
    }

    fn complete_with_metadata<'a>(
        &'a self,
        request: ModelRequest,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move { self.execute(request, CancellationToken::new()).await })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move { self.execute(request, stream.cancellation_token()).await })
    }
}

/// Cancellation-free JSON envelope delivered to an external conversation compactor.
///
/// The active cancellation signal remains inside the engine and is propagated
/// separately to the selected [`ProcessBroker`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonConversationCompactionRequest {
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Bounded omitted whole Turns in chronological order.
    pub turns: Vec<ConversationCompactionTurn>,
    /// Number of still-older omitted Turns not represented by `turns`.
    pub older_omitted_turns: usize,
    /// Identities of raw whole Turns retained after the summary.
    pub retained_turns: Vec<TurnId>,
    /// Current user prompt for relevance-aware compaction.
    pub current_prompt: String,
    /// Maximum provider-specific tokens allowed in the final summary block.
    pub output_budget_tokens: usize,
    /// Independent byte ceiling for the final summary block.
    pub output_budget_bytes: usize,
}

/// JSON response expected from an external conversation compactor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonConversationCompactionResponse {
    /// Plain-text candidate; the Context Engine adds provenance and validates bounds.
    pub summary: String,
}

/// Semantic conversation compactor backed by one shell-free JSON command.
pub struct JsonCommandConversationCompactor {
    descriptor: ConversationCompactorDescriptor,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
}

impl JsonCommandConversationCompactor {
    /// Creates an external compactor after validating its process configuration.
    pub fn new(
        descriptor: ConversationCompactorDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        descriptor.validate()?;
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            config,
            broker,
            broker_descriptor,
        })
    }

    /// Returns the broker isolation visible to the embedding host.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }
}

impl ConversationCompactor for JsonCommandConversationCompactor {
    fn descriptor(&self) -> ConversationCompactorDescriptor {
        self.descriptor.clone()
    }

    fn compact<'a>(
        &'a self,
        request: ConversationCompactionRequest,
    ) -> HarnessFuture<'a, ConversationCompactionResponse> {
        Box::pin(async move {
            validate_compaction_json_shapes(&request)?;
            let ConversationCompactionRequest {
                thread_id,
                turns,
                older_omitted_turns,
                retained_turns,
                current_prompt,
                output_budget_tokens,
                output_budget_bytes,
                cancellation,
            } = request;
            let request = JsonConversationCompactionRequest {
                thread_id,
                turns,
                older_omitted_turns,
                retained_turns,
                current_prompt,
                output_budget_tokens,
                output_budget_bytes,
            };
            let stdin =
                crate::json::to_bounded_json_vec(&request, MAX_STDIN_BYTES).map_err(|error| {
                    match error {
                        crate::json::BoundedJsonError::LimitExceeded => {
                            HarnessError::Execution(format!(
                                "conversation compactor request exceeds {MAX_STDIN_BYTES} bytes"
                            ))
                        }
                        crate::json::BoundedJsonError::CannotEncode => HarnessError::Execution(
                            "cannot encode conversation compactor request".to_owned(),
                        ),
                    }
                })?;
            let output = self
                .broker
                .execute(
                    self.config.request(stdin, ExecutionPhase::Context),
                    cancellation,
                )
                .await
                .map_err(map_compactor_execution_error)?;
            validate_process_success(&output, "conversation compactor command")
                .map_err(HarnessError::Execution)?;
            let response: JsonConversationCompactionResponse =
                serde_json::from_slice(&output.stdout).map_err(|error| {
                    HarnessError::Execution(format!(
                        "invalid conversation compactor JSON output: {error}"
                    ))
                })?;
            Ok(ConversationCompactionResponse {
                summary: response.summary,
            })
        })
    }
}

/// Cancellation-free JSON envelope delivered to an external completion verifier.
///
/// Cancellation stays under Runtime authority and is propagated separately to
/// the selected [`ProcessBroker`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonVerificationRequest {
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Active Turn.
    pub turn_id: TurnId,
    /// Ordered Runtime history including the assistant candidate.
    pub items: Vec<crate::Item>,
    /// Candidate text being considered for Turn completion.
    pub candidate: String,
}

/// Strict JSON settlement returned by an external completion verifier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum JsonVerificationOutcome {
    /// The candidate satisfies the completion condition.
    Passed {
        /// Optional bounded audit explanation.
        summary: Option<String>,
    },
    /// The candidate violates the completion condition.
    Failed {
        /// Bounded actionable explanation.
        reason: String,
        /// Whether another Agent Loop step may correct the candidate.
        retryable: bool,
    },
}

impl From<JsonVerificationOutcome> for VerificationOutcome {
    fn from(outcome: JsonVerificationOutcome) -> Self {
        match outcome {
            JsonVerificationOutcome::Passed { summary } => Self::Passed { summary },
            JsonVerificationOutcome::Failed { reason, retryable } => {
                Self::Failed { reason, retryable }
            }
        }
    }
}

/// Completion verifier backed by one shell-free JSON command.
pub struct JsonCommandVerifier {
    descriptor: VerifierDescriptor,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
}

impl JsonCommandVerifier {
    /// Creates an external verifier after validating metadata and process configuration.
    pub fn new(
        descriptor: VerifierDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        descriptor.validate()?;
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            config,
            broker,
            broker_descriptor,
        })
    }

    /// Returns the broker isolation visible to the embedding host.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }
}

impl Verifier for JsonCommandVerifier {
    fn descriptor(&self) -> VerifierDescriptor {
        self.descriptor.clone()
    }

    fn verify<'a>(
        &'a self,
        request: VerificationRequest,
    ) -> HarnessFuture<'a, VerificationOutcome> {
        Box::pin(async move {
            validate_items_json_shapes(
                &request.items,
                "verifier input",
                HarnessError::Verification,
            )?;
            let VerificationRequest {
                thread_id,
                turn_id,
                items,
                candidate,
                cancellation,
            } = request;
            let request = JsonVerificationRequest {
                thread_id,
                turn_id,
                items,
                candidate,
            };
            let stdin =
                crate::json::to_bounded_json_vec(&request, MAX_STDIN_BYTES).map_err(|error| {
                    match error {
                        crate::json::BoundedJsonError::LimitExceeded => HarnessError::Verification(
                            format!("verifier request exceeds {MAX_STDIN_BYTES} bytes"),
                        ),
                        crate::json::BoundedJsonError::CannotEncode => {
                            HarnessError::Verification("cannot encode verifier request".to_owned())
                        }
                    }
                })?;
            let output = self
                .broker
                .execute(
                    self.config.request(stdin, ExecutionPhase::Verification),
                    cancellation,
                )
                .await
                .map_err(map_verifier_execution_error)?;
            validate_process_success(&output, "verifier command")
                .map_err(HarnessError::Verification)?;
            let outcome: JsonVerificationOutcome =
                serde_json::from_slice(&output.stdout).map_err(|error| {
                    HarnessError::Verification(format!(
                        "invalid verifier command JSON output: {error}"
                    ))
                })?;
            Ok(outcome.into())
        })
    }
}

/// Strict cancellation-free sample delivered to an external Evaluation Grader.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonGradeRequest {
    /// Original validated Evaluation case.
    pub case: EvaluationCase,
    /// Captured target execution shared by all Graders.
    pub execution: EvaluationExecution,
}

#[derive(Serialize)]
struct BorrowedJsonGradeRequest<'a> {
    case: &'a EvaluationCase,
    execution: &'a EvaluationExecution,
}

/// Strict JSON settlement returned by an external Evaluation Grader.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonGradeResponse {
    /// Normalized score from 0.0 through 1.0.
    pub score: f64,
    /// Grader-defined pass/fail result.
    pub passed: bool,
    /// Optional bounded explanation.
    pub rationale: Option<String>,
}

impl From<JsonGradeResponse> for Grade {
    fn from(response: JsonGradeResponse) -> Self {
        Self {
            score: response.score,
            passed: response.passed,
            rationale: response.rationale,
        }
    }
}

/// Evaluation Grader backed by one shell-free JSON command.
pub struct JsonCommandGrader {
    descriptor: GraderDescriptor,
    config: JsonProcessConfig,
    broker: Arc<dyn ProcessBroker>,
    broker_descriptor: ProcessBrokerDescriptor,
}

impl JsonCommandGrader {
    /// Creates an external Grader after validating metadata and process configuration.
    pub fn new(
        descriptor: GraderDescriptor,
        config: JsonProcessConfig,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        descriptor.validate()?;
        config.validate()?;
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_broker_descriptor(&broker_descriptor)?;
        Ok(Self {
            descriptor,
            config,
            broker,
            broker_descriptor,
        })
    }

    /// Returns the broker isolation visible to the embedding host.
    #[must_use]
    pub fn broker_descriptor(&self) -> ProcessBrokerDescriptor {
        self.broker_descriptor.clone()
    }
}

impl Grader for JsonCommandGrader {
    fn descriptor(&self) -> GraderDescriptor {
        self.descriptor.clone()
    }

    fn grade<'a>(
        &'a self,
        sample: Arc<EvaluationSample>,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, Grade> {
        Box::pin(async move {
            validate_evaluation_sample_json_shapes(&sample)?;
            let request = BorrowedJsonGradeRequest {
                case: &sample.case,
                execution: &sample.execution,
            };
            let stdin = crate::json::to_bounded_json_vec(&request, MAX_GRADER_STDIN_BYTES)
                .map_err(|error| match error {
                    crate::json::BoundedJsonError::LimitExceeded => HarnessError::Evaluation(
                        format!("grader request exceeds {MAX_GRADER_STDIN_BYTES} encoded bytes"),
                    ),
                    crate::json::BoundedJsonError::CannotEncode => {
                        HarnessError::Evaluation("cannot encode grader request".to_owned())
                    }
                })?;
            let output = self
                .broker
                .execute(
                    self.config.request(stdin, ExecutionPhase::Evaluation),
                    cancellation,
                )
                .await
                .map_err(map_grader_execution_error)?;
            validate_process_success(&output, "grader command")
                .map_err(HarnessError::Evaluation)?;
            let response: JsonGradeResponse =
                serde_json::from_slice(&output.stdout).map_err(|error| {
                    HarnessError::Evaluation(format!("invalid grader command JSON output: {error}"))
                })?;
            Ok(response.into())
        })
    }
}

fn validate_broker_descriptor(descriptor: &ProcessBrokerDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("process broker", &descriptor.name)?;
    if let ProcessIsolation::Sandboxed { mechanism } = &descriptor.isolation {
        validate_capability_name("process sandbox mechanism", mechanism)?;
    }
    if let ProcessExecutableIntegrity::DispatchSha256 { sha256 } = &descriptor.executable_integrity
    {
        digest::validate_program_sha256(sha256)?;
    }
    Ok(())
}

fn validate_request(request: &ProcessRequest) -> Result<(), HarnessError> {
    if !request.program.is_absolute() {
        return Err(HarnessError::InvalidConfiguration(
            "process executable path must be absolute".to_owned(),
        ));
    }
    if !request.current_dir.is_absolute() {
        return Err(HarnessError::InvalidConfiguration(
            "process working directory must be absolute".to_owned(),
        ));
    }
    if request.args.len() > MAX_ARGUMENTS
        || request
            .args
            .iter()
            .any(|argument| argument.is_empty() || argument.len() > MAX_ARGUMENT_BYTES)
    {
        return Err(HarnessError::InvalidConfiguration(format!(
            "process args must contain at most {MAX_ARGUMENTS} non-empty values of at most {MAX_ARGUMENT_BYTES} bytes"
        )));
    }
    let environment_entries = request
        .environment
        .len()
        .checked_add(request.secret_environment.len())
        .ok_or_else(|| {
            HarnessError::InvalidConfiguration("process environment count overflow".to_owned())
        })?;
    if environment_entries > MAX_ENVIRONMENT_ENTRIES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "process environment exceeds {MAX_ENVIRONMENT_ENTRIES} entries"
        )));
    }
    if request
        .secret_environment
        .keys()
        .any(|name| request.environment.contains_key(name))
    {
        return Err(HarnessError::InvalidConfiguration(
            "process plain and secret environment names must not overlap".to_owned(),
        ));
    }
    let plain_environment_bytes =
        request
            .environment
            .iter()
            .try_fold(0_usize, |total, (name, value)| {
                if !valid_environment_name(name) {
                    return Err(HarnessError::InvalidConfiguration(format!(
                        "invalid process environment name {name:?}"
                    )));
                }
                total
                    .checked_add(name.len())
                    .and_then(|total| total.checked_add(value.len()))
                    .ok_or_else(|| {
                        HarnessError::InvalidConfiguration(
                            "process environment size overflow".to_owned(),
                        )
                    })
            })?;
    let environment_bytes = request.secret_environment.iter().try_fold(
        plain_environment_bytes,
        |total, (name, value)| {
            if !valid_environment_name(name) {
                return Err(HarnessError::InvalidConfiguration(
                    "invalid process secret environment name".to_owned(),
                ));
            }
            total
                .checked_add(name.len())
                .and_then(|total| total.checked_add(value.len()))
                .ok_or_else(|| {
                    HarnessError::InvalidConfiguration(
                        "process environment size overflow".to_owned(),
                    )
                })
        },
    )?;
    if environment_bytes > MAX_ENVIRONMENT_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "process environment exceeds {MAX_ENVIRONMENT_BYTES} bytes"
        )));
    }
    let max_stdin_bytes = match request.cancellation_phase {
        ExecutionPhase::Evaluation => MAX_GRADER_STDIN_BYTES,
        _ => MAX_STDIN_BYTES,
    };
    if request.stdin.len() > max_stdin_bytes {
        return Err(HarnessError::Execution(format!(
            "process stdin exceeds {max_stdin_bytes} bytes"
        )));
    }
    if request.timeout.is_zero() || request.timeout > MAX_PROCESS_TIMEOUT {
        return Err(HarnessError::InvalidConfiguration(format!(
            "process timeout must be 1 millisecond to {} seconds",
            MAX_PROCESS_TIMEOUT.as_secs()
        )));
    }
    if !(1..=MAX_OUTPUT_BYTES).contains(&request.max_output_bytes) {
        return Err(HarnessError::InvalidConfiguration(format!(
            "process output limit must be 1-{MAX_OUTPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn validate_process_success(output: &ProcessOutput, kind: &str) -> Result<(), String> {
    if output.stdout_truncated {
        return Err(format!("{kind} stdout exceeded its configured limit"));
    }
    if !output.success {
        return Err(match output.code {
            Some(code) => format!("{kind} exited with code {code}"),
            None => format!("{kind} terminated without an exit code"),
        });
    }
    Ok(())
}

fn map_tool_execution_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        error => HarnessError::Tool(error.to_string()),
    }
}

fn map_model_execution_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        error => HarnessError::Model(error.to_string()),
    }
}

fn map_compactor_execution_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        _ => HarnessError::Execution("conversation compactor command failed".to_owned()),
    }
}

fn map_verifier_execution_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        _ => HarnessError::Verification("verifier command failed".to_owned()),
    }
}

fn map_grader_execution_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. } | HarnessError::TimedOut { .. } => error,
        _ => HarnessError::Evaluation("grader command failed".to_owned()),
    }
}

fn validate_evaluation_sample_json_shapes(sample: &EvaluationSample) -> Result<(), HarnessError> {
    crate::json::validate_value_shape(&sample.case.metadata).map_err(|_| {
        HarnessError::Evaluation(
            "grader input exceeds the supported JSON depth or node count".to_owned(),
        )
    })?;
    if let EvaluationExecution::Completed { outcome } = &sample.execution {
        validate_items_json_shapes(
            &outcome.turn.items,
            "grader input",
            HarnessError::Evaluation,
        )?;
    }
    Ok(())
}

fn validate_compaction_json_shapes(
    request: &ConversationCompactionRequest,
) -> Result<(), HarnessError> {
    for turn in &request.turns {
        validate_items_json_shapes(
            &turn.items,
            "conversation compactor input",
            HarnessError::Execution,
        )?;
    }
    Ok(())
}

fn validate_items_json_shapes(
    items: &[crate::Item],
    kind: &str,
    error: fn(String) -> HarnessError,
) -> Result<(), HarnessError> {
    for item in items {
        let value = match &item.kind {
            crate::ItemKind::ToolCall { input, .. } => Some(input),
            crate::ItemKind::ToolResult { output, .. } => Some(output),
            _ => None,
        };
        if let Some(value) = value {
            crate::json::validate_value_shape(value).map_err(|_| {
                error(format!(
                    "{kind} exceeds the supported JSON depth or node count"
                ))
            })?;
        }
    }
    Ok(())
}

async fn write_stdin(
    mut stdin: tokio::process::ChildStdin,
    bytes: Vec<u8>,
) -> Result<(), HarnessError> {
    stdin
        .write_all(&bytes)
        .await
        .map_err(|error| HarnessError::Execution(format!("child stdin write failed: {error}")))?;
    stdin
        .shutdown()
        .await
        .map_err(|error| HarnessError::Execution(format!("child stdin close failed: {error}")))
}

async fn read_bounded<R>(mut reader: R, limit: usize) -> Result<(Vec<u8>, bool), HarnessError>
where
    R: AsyncRead + Unpin,
{
    let mut retained = Vec::with_capacity(limit.min(8_192));
    let mut buffer = [0_u8; 8_192];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await.map_err(|error| {
            HarnessError::Execution(format!("child output read failed: {error}"))
        })?;
        if read == 0 {
            return Ok((retained, truncated));
        }
        let remaining = limit.saturating_sub(retained.len());
        let retained_now = remaining.min(read);
        retained.extend_from_slice(&buffer[..retained_now]);
        truncated |= retained_now < read;
    }
}

async fn join_io<T>(
    deadline: Instant,
    task: &mut JoinHandle<Result<T, HarnessError>>,
    stream: &str,
    cancellation: &CancellationToken,
    cancellation_phase: ExecutionPhase,
) -> Result<T, HarnessError> {
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(HarnessError::Cancelled {
            phase: cancellation_phase,
        }),
        joined = timeout_at(deadline, task) => {
            joined
                .map_err(|_| HarnessError::Execution(format!("{stream} task timed out")))?
                .map_err(|_| HarnessError::Execution(format!("{stream} task failed")))?
        }
    }
}

fn abort_io<A, B, C>(input: &JoinHandle<A>, stdout: &JoinHandle<B>, stderr: &JoinHandle<C>) {
    input.abort();
    stdout.abort();
    stderr.abort();
}

async fn terminate(child: &mut Child, process_group: &mut ChildProcessGroup) -> bool {
    process_group.request_kill();
    let _ = child.start_kill();
    let Some(deadline) = Instant::now().checked_add(PROCESS_TERMINATION_GRACE) else {
        return false;
    };
    let child_settled = matches!(timeout_at(deadline, child.wait()).await, Ok(Ok(_)));
    child_settled && process_group.wait_gone(deadline).await
}

#[cfg(unix)]
pub(crate) fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
pub(crate) fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
pub(crate) struct ChildProcessGroup {
    id: Pid,
    armed: bool,
}

#[cfg(unix)]
impl ChildProcessGroup {
    pub(crate) fn for_child(child: &Child) -> Result<Self, HarnessError> {
        let process_id = child.id().ok_or_else(|| {
            HarnessError::Execution("spawned child has no process identity".to_owned())
        })?;
        let process_id = i32::try_from(process_id).map_err(|_| {
            HarnessError::Execution("child process identity exceeds i32".to_owned())
        })?;
        Ok(Self {
            id: Pid::from_raw(process_id),
            armed: true,
        })
    }

    pub(crate) fn request_kill(&self) {
        if self.armed {
            let _ = killpg(self.id, Signal::SIGKILL);
        }
    }

    pub(crate) async fn settle_remaining(&mut self) -> bool {
        self.request_kill();
        let Some(deadline) = Instant::now().checked_add(PROCESS_TERMINATION_GRACE) else {
            return false;
        };
        self.wait_gone(deadline).await
    }

    async fn wait_gone(&mut self, deadline: Instant) -> bool {
        loop {
            match killpg(self.id, None) {
                Err(Errno::ESRCH) => {
                    self.armed = false;
                    return true;
                }
                Ok(()) | Err(_) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(10)).await;
                }
                Ok(()) | Err(_) => return false,
            }
        }
    }
}

#[cfg(unix)]
impl Drop for ChildProcessGroup {
    fn drop(&mut self) {
        self.request_kill();
    }
}

#[cfg(not(unix))]
pub(crate) struct ChildProcessGroup;

#[cfg(not(unix))]
impl ChildProcessGroup {
    pub(crate) fn for_child(_child: &Child) -> Result<Self, HarnessError> {
        Ok(Self)
    }

    pub(crate) fn request_kill(&self) {}

    pub(crate) async fn settle_remaining(&mut self) -> bool {
        true
    }

    async fn wait_gone(&mut self, _deadline: Instant) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use serde_json::json;
    use tokio::sync::Notify;

    use super::{
        DenyProcessBroker, JsonCommandConversationCompactor, JsonCommandGrader, JsonCommandModel,
        JsonCommandModelProtocol, JsonCommandTool, JsonCommandVerifier,
        JsonConversationCompactionRequest, JsonGradeRequest, JsonProcessConfig,
        JsonVerificationRequest, LocalProcessBroker, ProcessBroker, ProcessBrokerDescriptor,
        ProcessExecutableIntegrity, ProcessIsolation, ProcessOutput, ProcessRequest,
    };
    #[cfg(target_os = "macos")]
    use super::{MacOsSeatbeltBroker, NetworkAccess};
    use crate::{
        CONVERSATION_COMPACTOR_API_VERSION, CancellationToken, CapabilityOrigin,
        ConversationCompactionRequest, ConversationCompactionTurn, ConversationCompactor,
        ConversationCompactorDescriptor, EvaluationCase, EvaluationExecution, EvaluationSample,
        ExecutionPhase, Grade, Grader, GraderDescriptor, HarnessError, HarnessFuture, Item,
        ItemKind, LanguageModel, MemoryScope, ModelContinuation, ModelProviderFailureKind,
        ModelRequest, ModelStream, ThreadId, Tool, ToolBatchExecution, ToolContext, ToolDescriptor,
        TurnId, TurnOutcome, TurnStatus, VerificationOutcome, VerificationRequest, Verifier,
        VerifierDescriptor,
    };

    struct RecordingBroker {
        output: ProcessOutput,
        phases: Mutex<Vec<ExecutionPhase>>,
        inputs: Mutex<Vec<Vec<u8>>>,
    }

    struct CancellationBroker {
        entered: Arc<Notify>,
    }

    struct PanickingDescriptorBroker;

    impl ProcessBroker for RecordingBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "recording".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                self.phases
                    .lock()
                    .expect("phase lock")
                    .push(request.cancellation_phase);
                self.inputs.lock().expect("input lock").push(request.stdin);
                Ok(self.output.clone())
            })
        }
    }

    impl ProcessBroker for CancellationBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            ProcessBrokerDescriptor {
                name: "cancellation".to_owned(),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "test".to_owned(),
                },
                executable_integrity: ProcessExecutableIntegrity::Unmeasured,
            }
        }

        fn execute<'a>(
            &'a self,
            request: ProcessRequest,
            cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async move {
                self.entered.notify_one();
                cancellation.cancelled().await;
                Err(HarnessError::Cancelled {
                    phase: request.cancellation_phase,
                })
            })
        }
    }

    impl ProcessBroker for PanickingDescriptorBroker {
        fn descriptor(&self) -> ProcessBrokerDescriptor {
            panic!("sensitive broker descriptor panic")
        }

        fn execute<'a>(
            &'a self,
            _request: ProcessRequest,
            _cancellation: CancellationToken,
        ) -> HarnessFuture<'a, ProcessOutput> {
            Box::pin(async {
                Err(HarnessError::Execution(
                    "unreachable process execution".to_owned(),
                ))
            })
        }
    }

    fn config() -> JsonProcessConfig {
        JsonProcessConfig {
            program: std::env::temp_dir().join("adapter"),
            args: Vec::new(),
            current_dir: std::env::temp_dir(),
            environment: Default::default(),
            timeout: Duration::from_secs(1),
            max_output_bytes: 1_024,
        }
    }

    fn output(stdout: &[u8]) -> ProcessOutput {
        ProcessOutput {
            success: true,
            code: Some(0),
            stdout: stdout.to_vec(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
        }
    }

    fn evaluation_sample(metadata: serde_json::Value) -> Arc<EvaluationSample> {
        Arc::new(EvaluationSample {
            case: EvaluationCase {
                id: "case-test".to_owned(),
                prompt: "evaluate this output".to_owned(),
                memory_scope: MemoryScope::default(),
                timeout_ms: Some(1_000),
                metadata,
            },
            execution: EvaluationExecution::Completed {
                outcome: TurnOutcome {
                    turn: crate::Turn {
                        id: TurnId::from_static("turn-test"),
                        thread_id: ThreadId::from_static("thread-test"),
                        status: TurnStatus::Completed,
                        items: vec![Item::new(ItemKind::AssistantMessage {
                            model_id: Some("fixture/model".to_owned()),
                            model_origin: Some(CapabilityOrigin::BuiltIn),
                            content: "candidate".to_owned(),
                        })],
                    },
                    final_text: "candidate".to_owned(),
                },
            },
        })
    }

    #[test]
    fn process_request_reserves_the_larger_stdin_budget_for_evaluation() {
        let mut request = ProcessRequest {
            program: PathBuf::from("/fixture"),
            args: Vec::new(),
            current_dir: PathBuf::from("/"),
            environment: BTreeMap::new(),
            secret_environment: BTreeMap::new(),
            stdin: vec![0; super::MAX_STDIN_BYTES + 1],
            timeout: Duration::from_secs(1),
            max_output_bytes: 1_024,
            cancellation_phase: ExecutionPhase::Tool,
        };
        assert!(super::validate_request(&request).is_err());

        request.cancellation_phase = ExecutionPhase::Evaluation;
        super::validate_request(&request).expect("Evaluation stdin budget");

        request.stdin.resize(super::MAX_GRADER_STDIN_BYTES + 1, 0);
        assert!(super::validate_request(&request).is_err());
    }

    #[test]
    fn command_adapters_freeze_and_sanitize_broker_metadata() {
        let error = match JsonCommandModel::new(
            "fixture/model",
            config(),
            Arc::new(PanickingDescriptorBroker),
        ) {
            Ok(_) => panic!("broker descriptor panic must reject construction"),
            Err(error) => error,
        };
        assert!(matches!(error, HarnessError::InvalidCapability(_)));
        assert!(!error.to_string().contains("sensitive"));
    }

    #[tokio::test]
    async fn json_command_adapters_use_phase_specific_broker_requests() {
        let model_broker = Arc::new(RecordingBroker {
            output: output(br#"{"type":"message","content":"ok"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let model = JsonCommandModel::new("fixture/model", config(), model_broker.clone())
            .expect("model adapter");
        let model_output = model
            .complete(ModelRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                authority: crate::AuthorityContext::local_process(),
                items: Vec::new(),
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .expect("model output");
        assert_eq!(
            model_output,
            crate::ModelOutput::Message {
                content: "ok".to_owned()
            }
        );
        assert_eq!(
            *model_broker.phases.lock().expect("model phases"),
            vec![ExecutionPhase::Model]
        );

        let tool_broker = Arc::new(RecordingBroker {
            output: output(br#"{"ok":true}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let tool = JsonCommandTool::new(
            ToolDescriptor {
                name: "fixture".to_owned(),
                description: "Fixture".to_owned(),
                input_schema: json!({"type": "object"}),
            },
            config(),
            tool_broker.clone(),
        )
        .expect("tool adapter");
        assert_eq!(tool.batch_execution(), ToolBatchExecution::Sequential);
        let tool = tool.with_batch_execution(ToolBatchExecution::ParallelSafe);
        assert_eq!(tool.batch_execution(), ToolBatchExecution::ParallelSafe);
        assert_eq!(
            tool.execute(
                json!({"value": 1}),
                ToolContext {
                    thread_id: ThreadId::from_static("thread-test"),
                    turn_id: TurnId::from_static("turn-test"),
                    call_id: "call-test".to_owned(),
                    authority: crate::AuthorityContext::local_process(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect("tool output"),
            json!({"ok": true})
        );
        assert_eq!(
            *tool_broker.phases.lock().expect("tool phases"),
            vec![ExecutionPhase::Tool]
        );

        let compactor_broker = Arc::new(RecordingBroker {
            output: output(br#"{"summary":"bounded summary"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let compactor = JsonCommandConversationCompactor::new(
            ConversationCompactorDescriptor {
                name: "fixture.compactor".to_owned(),
                description: "Fixture semantic compactor".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            },
            config(),
            compactor_broker.clone(),
        )
        .expect("compactor adapter");
        let response = compactor
            .compact(ConversationCompactionRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turns: vec![ConversationCompactionTurn {
                    turn_id: TurnId::from_static("turn-omitted"),
                    items: vec![Item::new(ItemKind::UserMessage {
                        content: "omitted input".to_owned(),
                    })],
                }],
                older_omitted_turns: 2,
                retained_turns: vec![TurnId::from_static("turn-retained")],
                current_prompt: "current question".to_owned(),
                output_budget_tokens: 128,
                output_budget_bytes: 1_024,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("compactor output");
        assert_eq!(response.summary, "bounded summary");
        assert_eq!(
            *compactor_broker.phases.lock().expect("compactor phases"),
            vec![ExecutionPhase::Context]
        );
        let request: JsonConversationCompactionRequest =
            serde_json::from_slice(&compactor_broker.inputs.lock().expect("compactor inputs")[0])
                .expect("compactor request");
        assert_eq!(request.thread_id, ThreadId::from_static("thread-test"));
        assert_eq!(request.turns.len(), 1);
        assert_eq!(request.older_omitted_turns, 2);
        assert_eq!(
            request.retained_turns,
            [TurnId::from_static("turn-retained")]
        );
        assert_eq!(request.current_prompt, "current question");
        assert_eq!(request.output_budget_tokens, 128);
        assert_eq!(request.output_budget_bytes, 1_024);

        let verifier_broker = Arc::new(RecordingBroker {
            output: output(br#"{"status":"passed","summary":"verified"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let verifier = JsonCommandVerifier::new(
            VerifierDescriptor {
                name: "fixture.verifier".to_owned(),
                description: "Fixture completion verifier".to_owned(),
            },
            config(),
            verifier_broker.clone(),
        )
        .expect("verifier adapter");
        let outcome = verifier
            .verify(VerificationRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                items: vec![Item::new(ItemKind::AssistantMessage {
                    model_id: Some("fixture/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    content: "candidate".to_owned(),
                })],
                candidate: "candidate".to_owned(),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect("verifier outcome");
        assert_eq!(
            outcome,
            VerificationOutcome::Passed {
                summary: Some("verified".to_owned())
            }
        );
        assert_eq!(
            *verifier_broker.phases.lock().expect("verifier phases"),
            [ExecutionPhase::Verification]
        );
        let request: JsonVerificationRequest =
            serde_json::from_slice(&verifier_broker.inputs.lock().expect("verifier inputs")[0])
                .expect("verifier request");
        assert_eq!(request.thread_id, ThreadId::from_static("thread-test"));
        assert_eq!(request.turn_id, TurnId::from_static("turn-test"));
        assert_eq!(request.candidate, "candidate");
        assert_eq!(request.items.len(), 1);

        let grader_broker = Arc::new(RecordingBroker {
            output: output(br#"{"score":1.0,"passed":true,"rationale":"exact"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let grader = JsonCommandGrader::new(
            GraderDescriptor {
                name: "fixture.grader".to_owned(),
                description: "Fixture Evaluation grader".to_owned(),
            },
            config(),
            grader_broker.clone(),
        )
        .expect("grader adapter");
        let grade = grader
            .grade(
                evaluation_sample(json!({"expected": "candidate"})),
                CancellationToken::new(),
            )
            .await
            .expect("grader output");
        assert_eq!(
            grade,
            Grade {
                score: 1.0,
                passed: true,
                rationale: Some("exact".to_owned())
            }
        );
        assert_eq!(
            *grader_broker.phases.lock().expect("grader phases"),
            [ExecutionPhase::Evaluation]
        );
        let request: JsonGradeRequest =
            serde_json::from_slice(&grader_broker.inputs.lock().expect("grader inputs")[0])
                .expect("grader request");
        assert_eq!(request.case.id, "case-test");
        assert!(matches!(
            request.execution,
            EvaluationExecution::Completed { .. }
        ));
    }

    #[tokio::test]
    async fn json_model_settlement_preserves_provider_evidence() {
        let broker = Arc::new(RecordingBroker {
            output: output(
                br#"{
                    "status":"completed",
                    "output":{"type":"message","content":"settled"},
                    "usage":{
                        "input_tokens":11,
                        "output_tokens":7,
                        "cached_input_tokens":3,
                        "reasoning_tokens":2,
                        "cost_usd_ticks":125
                    },
                    "provider_model":"provider/model-v2",
                    "provider_request_id":"request-42",
                    "continuation":{
                        "format":"fixture.v1",
                        "items":[{"id":"continuation-1"}]
                    }
                }"#,
            ),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let model = JsonCommandModel::new("fixture/model", config(), broker)
            .expect("model adapter")
            .with_protocol(JsonCommandModelProtocol::SettlementV1);
        let response = model
            .complete_with_metadata(ModelRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                authority: crate::AuthorityContext::local_process(),
                items: Vec::new(),
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .expect("settled response");

        assert_eq!(
            response.output,
            crate::ModelOutput::Message {
                content: "settled".to_owned()
            }
        );
        assert_eq!(
            response.usage.as_ref().map(|usage| usage.input_tokens),
            Some(11)
        );
        assert_eq!(
            response
                .usage
                .as_ref()
                .and_then(|usage| usage.cost_usd_ticks),
            Some(125)
        );
        assert_eq!(
            response.provider_model.as_deref(),
            Some("provider/model-v2")
        );
        assert_eq!(response.provider_request_id.as_deref(), Some("request-42"));
        assert_eq!(
            response
                .continuation
                .as_ref()
                .map(ModelContinuation::format),
            Some("fixture.v1")
        );
    }

    #[tokio::test]
    async fn json_model_settlement_is_strict_and_returns_typed_failure() {
        let broker = Arc::new(RecordingBroker {
            output: output(
                br#"{
                    "status":"failed",
                    "kind":"rate_limited",
                    "message":"provider rate limit",
                    "http_status":429,
                    "retry_after_ms":250
                }"#,
            ),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let model = JsonCommandModel::new("fixture/model", config(), broker)
            .expect("model adapter")
            .with_protocol(JsonCommandModelProtocol::SettlementV1);
        let error = model
            .complete(ModelRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                authority: crate::AuthorityContext::local_process(),
                items: Vec::new(),
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .expect_err("typed failure");
        let HarnessError::ModelProvider(failure) = error else {
            panic!("expected typed Provider failure");
        };
        assert_eq!(failure.kind(), ModelProviderFailureKind::RateLimited);
        assert_eq!(failure.http_status(), Some(429));
        assert_eq!(failure.retry_after_ms(), Some(250));

        let broker = Arc::new(RecordingBroker {
            output: output(
                br#"{
                    "status":"failed",
                    "kind":"rate_limited",
                    "message":"provider rate limit",
                    "retry":true
                }"#,
            ),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let model = JsonCommandModel::new("fixture/model", config(), broker)
            .expect("model adapter")
            .with_protocol(JsonCommandModelProtocol::SettlementV1);
        let error = model
            .complete(ModelRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                authority: crate::AuthorityContext::local_process(),
                items: Vec::new(),
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .expect_err("unknown settlement field");
        assert!(
            error
                .to_string()
                .contains("invalid model command settlement JSON")
        );
        assert!(!error.to_string().contains("retry"));
    }

    #[tokio::test]
    async fn command_adapters_reject_deep_json_before_broker_execution() {
        let mut nested = serde_json::Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            nested = serde_json::Value::Array(vec![nested]);
        }
        let tool_broker = Arc::new(RecordingBroker {
            output: output(br#"{"ok":true}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let tool = JsonCommandTool::new(
            ToolDescriptor {
                name: "fixture".to_owned(),
                description: "Fixture".to_owned(),
                input_schema: json!({"type": "object"}),
            },
            config(),
            tool_broker.clone(),
        )
        .expect("tool adapter");
        let error = tool
            .execute(
                nested.clone(),
                ToolContext {
                    thread_id: ThreadId::from_static("thread-test"),
                    turn_id: TurnId::from_static("turn-test"),
                    call_id: "call-test".to_owned(),
                    authority: crate::AuthorityContext::local_process(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect_err("deep tool input");
        assert!(matches!(error, HarnessError::Tool(_)));
        assert!(tool_broker.phases.lock().expect("tool phases").is_empty());

        let compactor_broker = Arc::new(RecordingBroker {
            output: output(br#"{"summary":"unreachable"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let compactor = JsonCommandConversationCompactor::new(
            ConversationCompactorDescriptor {
                name: "fixture.compactor".to_owned(),
                description: "Fixture semantic compactor".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            },
            config(),
            compactor_broker.clone(),
        )
        .expect("compactor adapter");
        let error = compactor
            .compact(ConversationCompactionRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turns: vec![ConversationCompactionTurn {
                    turn_id: TurnId::from_static("turn-omitted"),
                    items: vec![Item::new(ItemKind::ToolResult {
                        call_id: "call-test".to_owned(),
                        output: nested.clone(),
                        is_error: false,
                        connector_evidence: Vec::new(),
                    })],
                }],
                older_omitted_turns: 0,
                retained_turns: Vec::new(),
                current_prompt: "prompt".to_owned(),
                output_budget_tokens: 128,
                output_budget_bytes: 1_024,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("deep compactor input");
        assert!(matches!(error, HarnessError::Execution(_)));
        assert!(
            compactor_broker
                .phases
                .lock()
                .expect("compactor phases")
                .is_empty()
        );

        let verifier_broker = Arc::new(RecordingBroker {
            output: output(br#"{"status":"passed","summary":null}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let verifier = JsonCommandVerifier::new(
            VerifierDescriptor {
                name: "fixture.verifier".to_owned(),
                description: "Fixture completion verifier".to_owned(),
            },
            config(),
            verifier_broker.clone(),
        )
        .expect("verifier adapter");
        let error = verifier
            .verify(VerificationRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                items: vec![Item::new(ItemKind::ToolResult {
                    call_id: "call-test".to_owned(),
                    output: nested.clone(),
                    is_error: false,
                    connector_evidence: Vec::new(),
                })],
                candidate: "candidate".to_owned(),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("deep verifier input");
        assert!(matches!(error, HarnessError::Verification(_)));
        assert!(
            verifier_broker
                .phases
                .lock()
                .expect("verifier phases")
                .is_empty()
        );

        let grader_broker = Arc::new(RecordingBroker {
            output: output(br#"{"score":1.0,"passed":true,"rationale":null}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let grader = JsonCommandGrader::new(
            GraderDescriptor {
                name: "fixture.grader".to_owned(),
                description: "Fixture Evaluation grader".to_owned(),
            },
            config(),
            grader_broker.clone(),
        )
        .expect("grader adapter");
        let error = grader
            .grade(evaluation_sample(nested.clone()), CancellationToken::new())
            .await
            .expect_err("deep grader input");
        assert!(matches!(error, HarnessError::Evaluation(_)));
        assert!(
            grader_broker
                .phases
                .lock()
                .expect("grader phases")
                .is_empty()
        );

        let model_broker = Arc::new(RecordingBroker {
            output: output(br#"{"type":"message","content":"ok"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let model = JsonCommandModel::new("fixture/model", config(), model_broker.clone())
            .expect("model adapter");
        let error = model
            .complete(ModelRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                authority: crate::AuthorityContext::local_process(),
                items: vec![Item::new(ItemKind::ToolCall {
                    model_id: Some("fixture/model".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: "call-test".to_owned(),
                    name: "fixture".to_owned(),
                    input: nested,
                    batch: None,
                })],
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .expect_err("deep model request");
        assert!(matches!(error, HarnessError::Model(_)));
        assert!(model_broker.phases.lock().expect("model phases").is_empty());
    }

    #[tokio::test]
    async fn json_compactor_rejects_oversized_input_before_broker_execution() {
        let broker = Arc::new(RecordingBroker {
            output: output(br#"{"summary":"unreachable"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let compactor = JsonCommandConversationCompactor::new(
            ConversationCompactorDescriptor {
                name: "fixture.compactor".to_owned(),
                description: "Fixture semantic compactor".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            },
            config(),
            broker.clone(),
        )
        .expect("compactor adapter");
        let error = compactor
            .compact(ConversationCompactionRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turns: Vec::new(),
                older_omitted_turns: 0,
                retained_turns: Vec::new(),
                current_prompt: "x".repeat(super::MAX_STDIN_BYTES),
                output_budget_tokens: 128,
                output_budget_bytes: 1_024,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("oversized compactor input");
        assert!(error.to_string().contains("exceeds 1048576 bytes"));
        assert!(broker.phases.lock().expect("compactor phases").is_empty());
    }

    #[tokio::test]
    async fn json_compactor_rejects_unknown_response_fields() {
        let broker = Arc::new(RecordingBroker {
            output: output(br#"{"summary":"candidate","authority":"replace history"}"#),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let compactor = JsonCommandConversationCompactor::new(
            ConversationCompactorDescriptor {
                name: "fixture.compactor".to_owned(),
                description: "Fixture semantic compactor".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            },
            config(),
            broker.clone(),
        )
        .expect("compactor adapter");
        let error = compactor
            .compact(ConversationCompactionRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turns: Vec::new(),
                older_omitted_turns: 0,
                retained_turns: Vec::new(),
                current_prompt: "prompt".to_owned(),
                output_budget_tokens: 128,
                output_budget_bytes: 1_024,
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("unknown response field");
        assert!(
            error
                .to_string()
                .contains("invalid conversation compactor JSON output")
        );
        assert_eq!(
            *broker.phases.lock().expect("compactor phases"),
            [ExecutionPhase::Context]
        );
    }

    #[tokio::test]
    async fn json_model_propagates_runtime_cancellation_to_its_broker() {
        let entered = Arc::new(Notify::new());
        let model = JsonCommandModel::new(
            "fixture/cancellable-model",
            config(),
            Arc::new(CancellationBroker {
                entered: entered.clone(),
            }),
        )
        .expect("model adapter");
        let cancellation = CancellationToken::new();
        let cancelling = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                entered.notified().await;
                cancellation.cancel();
            }
        });
        let error = model
            .complete_streaming(
                ModelRequest {
                    thread_id: ThreadId::from_static("thread-test"),
                    turn_id: TurnId::from_static("turn-test"),
                    authority: crate::AuthorityContext::local_process(),
                    items: Vec::new(),
                    context: Vec::new(),
                    tools: Vec::new(),
                },
                ModelStream::disabled()
                    .with_cancellation(cancellation)
                    .for_step(1),
            )
            .await
            .expect_err("cancelled model");
        cancelling.await.expect("canceller");
        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Model
            }
        );
    }

    #[tokio::test]
    async fn json_compactor_propagates_runtime_cancellation_to_its_broker() {
        let entered = Arc::new(Notify::new());
        let compactor = JsonCommandConversationCompactor::new(
            ConversationCompactorDescriptor {
                name: "fixture.cancellable-compactor".to_owned(),
                description: "Cancellable fixture compactor".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            },
            config(),
            Arc::new(CancellationBroker {
                entered: entered.clone(),
            }),
        )
        .expect("compactor adapter");
        let cancellation = CancellationToken::new();
        let cancelling = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                entered.notified().await;
                cancellation.cancel();
            }
        });
        let error = compactor
            .compact(ConversationCompactionRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turns: Vec::new(),
                older_omitted_turns: 0,
                retained_turns: Vec::new(),
                current_prompt: "prompt".to_owned(),
                output_budget_tokens: 128,
                output_budget_bytes: 1_024,
                cancellation,
            })
            .await
            .expect_err("cancelled compactor");
        cancelling.await.expect("canceller");
        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Context
            }
        );
    }

    #[tokio::test]
    async fn json_verifier_propagates_runtime_cancellation_to_its_broker() {
        let entered = Arc::new(Notify::new());
        let verifier = JsonCommandVerifier::new(
            VerifierDescriptor {
                name: "fixture.cancellable-verifier".to_owned(),
                description: "Cancellable fixture verifier".to_owned(),
            },
            config(),
            Arc::new(CancellationBroker {
                entered: entered.clone(),
            }),
        )
        .expect("verifier adapter");
        let cancellation = CancellationToken::new();
        let cancelling = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                entered.notified().await;
                cancellation.cancel();
            }
        });
        let error = verifier
            .verify(VerificationRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                items: Vec::new(),
                candidate: "candidate".to_owned(),
                cancellation,
            })
            .await
            .expect_err("cancelled verifier");
        cancelling.await.expect("canceller");
        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Verification
            }
        );
    }

    #[tokio::test]
    async fn json_verifier_rejects_unknown_response_fields() {
        let broker = Arc::new(RecordingBroker {
            output: output(
                br#"{"status":"passed","summary":"verified","authority":"complete turn"}"#,
            ),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let verifier = JsonCommandVerifier::new(
            VerifierDescriptor {
                name: "fixture.verifier".to_owned(),
                description: "Fixture completion verifier".to_owned(),
            },
            config(),
            broker,
        )
        .expect("verifier adapter");
        let error = verifier
            .verify(VerificationRequest {
                thread_id: ThreadId::from_static("thread-test"),
                turn_id: TurnId::from_static("turn-test"),
                items: Vec::new(),
                candidate: "candidate".to_owned(),
                cancellation: CancellationToken::new(),
            })
            .await
            .expect_err("unknown response field");
        assert!(
            error
                .to_string()
                .contains("invalid verifier command JSON output")
        );
    }

    #[tokio::test]
    async fn json_grader_propagates_evaluation_cancellation_to_its_broker() {
        let entered = Arc::new(Notify::new());
        let grader = JsonCommandGrader::new(
            GraderDescriptor {
                name: "fixture.cancellable-grader".to_owned(),
                description: "Cancellable fixture Grader".to_owned(),
            },
            config(),
            Arc::new(CancellationBroker {
                entered: entered.clone(),
            }),
        )
        .expect("grader adapter");
        let cancellation = CancellationToken::new();
        let cancelling = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                entered.notified().await;
                cancellation.cancel();
            }
        });
        let error = grader
            .grade(evaluation_sample(json!({})), cancellation)
            .await
            .expect_err("cancelled grader");
        cancelling.await.expect("canceller");
        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Evaluation
            }
        );
    }

    #[tokio::test]
    async fn json_grader_rejects_unknown_response_fields() {
        let broker = Arc::new(RecordingBroker {
            output: output(
                br#"{"score":1.0,"passed":true,"rationale":"ok","authority":"pass suite"}"#,
            ),
            phases: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
        });
        let grader = JsonCommandGrader::new(
            GraderDescriptor {
                name: "fixture.grader".to_owned(),
                description: "Fixture Evaluation grader".to_owned(),
            },
            config(),
            broker,
        )
        .expect("grader adapter");
        let error = grader
            .grade(
                evaluation_sample(json!({"expected": "candidate"})),
                CancellationToken::new(),
            )
            .await
            .expect_err("unknown response field");
        assert!(
            error
                .to_string()
                .contains("invalid grader command JSON output")
        );
    }

    #[tokio::test]
    async fn deny_broker_is_the_safe_default_behavior() {
        let result = DenyProcessBroker
            .execute(
                config().request(Vec::new(), ExecutionPhase::Tool),
                CancellationToken::new(),
            )
            .await;
        let error = match result {
            Ok(_) => panic!("process must be denied"),
            Err(error) => error,
        };
        assert!(matches!(error, HarnessError::Execution(_)));
    }

    #[test]
    fn local_broker_rejects_unbounded_process_concurrency() {
        assert!(LocalProcessBroker::new(super::MAX_PROCESS_CONCURRENCY).is_ok());
        assert!(LocalProcessBroker::new(super::MAX_PROCESS_CONCURRENCY + 1).is_err());
    }

    #[tokio::test]
    async fn io_settlement_remains_cooperatively_cancellable() {
        let mut task =
            tokio::spawn(async { std::future::pending::<Result<(), HarnessError>>().await });
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let error = super::join_io(
            tokio::time::Instant::now() + Duration::from_secs(1),
            &mut task,
            "fixture",
            &cancellation,
            ExecutionPhase::Tool,
        )
        .await
        .expect_err("cancelled I/O settlement");
        assert_eq!(
            error,
            HarnessError::Cancelled {
                phase: ExecutionPhase::Tool
            }
        );
        task.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn direct_child_termination_is_reaped_within_the_cleanup_boundary() {
        let mut command = tokio::process::Command::new("/bin/sleep");
        command.arg("30").kill_on_drop(true).process_group(0);
        let mut child = command.spawn().expect("spawn child");
        let mut process_group =
            super::ChildProcessGroup::for_child(&child).expect("capture process group");
        assert!(super::terminate(&mut child, &mut process_group).await);
        assert!(child.try_wait().expect("query child").is_some());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_broker_timeout_terminates_descendant_process_group() {
        use nix::{errno::Errno, sys::signal::kill, unistd::Pid};

        let pid_path = std::env::temp_dir().join(format!(
            "y-harness-descendant-{}.pid",
            crate::EventId::generate()
        ));
        let broker = LocalProcessBroker::new(1).expect("local broker");
        let error = broker
            .execute(
                ProcessRequest {
                    program: PathBuf::from("/bin/sh"),
                    args: vec![
                        "-c".to_owned(),
                        "/bin/sleep 30 & echo $! > \"$1\"; wait".to_owned(),
                        "y-harness-fixture".to_owned(),
                        pid_path.to_string_lossy().into_owned(),
                    ],
                    current_dir: std::env::temp_dir(),
                    environment: BTreeMap::new(),
                    secret_environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: Duration::from_millis(250),
                    max_output_bytes: 1_024,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                CancellationToken::new(),
            )
            .await
            .err()
            .expect("fixture must time out");
        assert!(error.to_string().contains("timeout"));
        let descendant_pid = tokio::fs::read_to_string(&pid_path)
            .await
            .expect("read descendant pid");
        let descendant_pid = descendant_pid
            .trim()
            .parse::<i32>()
            .expect("parse descendant pid");
        assert_eq!(
            kill(Pid::from_raw(descendant_pid), None),
            Err(Errno::ESRCH),
            "descendant must not survive broker settlement"
        );
        let _ = tokio::fs::remove_file(pid_path).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn local_broker_clears_environment_and_bounds_real_process_output() {
        let broker = LocalProcessBroker::new(1).expect("local broker");
        let output = broker
            .execute(
                ProcessRequest {
                    program: PathBuf::from("/bin/echo"),
                    args: vec!["bounded".to_owned()],
                    current_dir: std::env::temp_dir(),
                    environment: BTreeMap::new(),
                    secret_environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: Duration::from_secs(1),
                    max_output_bytes: 4,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                CancellationToken::new(),
            )
            .await
            .expect("execute echo");
        assert!(output.success);
        assert_eq!(output.stdout, b"boun");
        assert!(output.stdout_truncated);
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn seatbelt_broker_allows_only_configured_write_roots() {
        let root =
            std::env::temp_dir().join(format!("y-harness-seatbelt-{}", crate::EventId::generate()));
        let allowed = root.join("allowed");
        let denied = root.join("denied");
        std::fs::create_dir_all(&allowed).expect("create allowed root");
        std::fs::create_dir_all(&denied).expect("create denied root");
        let broker = MacOsSeatbeltBroker::new(1, vec![allowed.clone()], NetworkAccess::Deny)
            .expect("Seatbelt broker");

        let allowed_output = broker
            .execute(
                ProcessRequest {
                    program: PathBuf::from("/usr/bin/touch"),
                    args: vec![allowed.join("created").to_string_lossy().into_owned()],
                    current_dir: allowed.clone(),
                    environment: BTreeMap::new(),
                    secret_environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: Duration::from_secs(2),
                    max_output_bytes: 1_024,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                CancellationToken::new(),
            )
            .await
            .expect("allowed execution");
        assert!(allowed_output.success);
        assert!(allowed.join("created").is_file());

        let null_output = broker
            .execute(
                ProcessRequest {
                    program: PathBuf::from("/bin/sh"),
                    args: vec!["-c".to_owned(), "printf compatible >/dev/null".to_owned()],
                    current_dir: allowed.clone(),
                    environment: BTreeMap::new(),
                    secret_environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: Duration::from_secs(2),
                    max_output_bytes: 1_024,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                CancellationToken::new(),
            )
            .await
            .expect("null-device execution");
        assert!(null_output.success);

        let denied_output = broker
            .execute(
                ProcessRequest {
                    program: PathBuf::from("/usr/bin/touch"),
                    args: vec![denied.join("blocked").to_string_lossy().into_owned()],
                    current_dir: allowed.clone(),
                    environment: BTreeMap::new(),
                    secret_environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: Duration::from_secs(2),
                    max_output_bytes: 1_024,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                CancellationToken::new(),
            )
            .await
            .expect("denied execution settles");
        assert!(!denied_output.success);
        assert!(!denied.join("blocked").exists());

        std::fs::remove_file(allowed.join("created")).expect("remove fixture file");
        std::fs::remove_dir(&allowed).expect("remove allowed root");
        std::fs::remove_dir(&denied).expect("remove denied root");
        std::fs::remove_dir(&root).expect("remove fixture root");
    }
}
