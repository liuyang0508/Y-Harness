//! Reproducible evaluation suites, graders, reports, and regression baselines.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::{Id as TaskId, JoinSet};

use crate::{
    CancellationToken, CapabilityOrigin, HarnessError, HarnessFuture, HarnessRuntime, ItemKind,
    MemoryScope, TurnExecutionOptions, TurnOutcome, TurnStatus,
    json::{BoundedJsonError, bounded_serialized_size, validate_value_shape},
    kernel::{capture_capability_metadata, validate_capability_name, validate_capability_origin},
};

const MAX_PROMPT_BYTES: usize = 1_048_576;
const MAX_RATIONALE_BYTES: usize = 4_096;
const MAX_EVALUATION_CASES: usize = 64;
const MAX_EVALUATION_GRADERS: usize = 64;
const MAX_EVALUATION_CONCURRENCY: usize = 64;
const MAX_EVALUATION_CASE_BYTES: usize = 1_310_720;
const MAX_EVALUATION_SUITE_BYTES: usize = 16_777_216;
const MAX_EVALUATION_EXECUTION_BYTES: usize = 2_097_152;
const MAX_BASELINE_REQUIREMENTS: usize = MAX_EVALUATION_CASES * MAX_EVALUATION_GRADERS;
const MAX_EVALUATION_TIMEOUT_MS: u64 = 86_400_000;
const DEFAULT_CASE_CONCURRENCY: usize = 4;
const DEFAULT_GRADER_CONCURRENCY: usize = 4;
const DEFAULT_CASE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_GRADER_TIMEOUT: Duration = Duration::from_secs(30);
const EVALUATION_CLEANUP_GRACE: Duration = Duration::from_secs(2);

/// Exact serialized Evaluation suite, baseline, and report format.
pub const EVALUATION_FORMAT_VERSION: u32 = 2;

/// One isolated evaluation input and grader-specific metadata.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationCase {
    /// Stable suite-local identity.
    pub id: String,
    /// User input supplied to the target.
    pub prompt: String,
    /// Long-term memory isolation scope.
    pub memory_scope: MemoryScope,
    /// Optional wall-clock budget for this case.
    pub timeout_ms: Option<u64>,
    /// Arbitrary expected values consumed only by graders.
    pub metadata: Value,
}

/// Validated, deterministic collection of evaluation cases.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationSuite {
    /// Exact serialized format coordinate.
    pub format_version: u32,
    /// Stable suite name.
    pub name: String,
    /// Cases in deterministic identity order.
    pub cases: Vec<EvaluationCase>,
}

impl EvaluationSuite {
    /// Validates, sorts, and constructs a suite.
    pub fn new(
        name: impl Into<String>,
        mut cases: Vec<EvaluationCase>,
    ) -> Result<Self, HarnessError> {
        let name = name.into();
        validate_capability_name("evaluation suite", &name)?;
        if cases.len() > MAX_EVALUATION_CASES {
            return Err(HarnessError::Evaluation(format!(
                "evaluation suite exceeds {MAX_EVALUATION_CASES} cases"
            )));
        }
        cases.sort_by(|left, right| left.id.cmp(&right.id));
        let mut previous: Option<&str> = None;
        let mut suite_bytes = 0_usize;
        for case in &cases {
            validate_capability_name("evaluation case", &case.id)?;
            if previous == Some(case.id.as_str()) {
                return Err(HarnessError::Evaluation(format!(
                    "duplicate evaluation case {}",
                    case.id
                )));
            }
            previous = Some(&case.id);
            if case.prompt.trim().is_empty() || case.prompt.len() > MAX_PROMPT_BYTES {
                return Err(HarnessError::Evaluation(format!(
                    "case {} prompt must be 1-{MAX_PROMPT_BYTES} bytes",
                    case.id
                )));
            }
            if let Some(timeout_ms) = case.timeout_ms
                && !(1..=MAX_EVALUATION_TIMEOUT_MS).contains(&timeout_ms)
            {
                return Err(HarnessError::Evaluation(format!(
                    "case {} timeout_ms must be 1-{MAX_EVALUATION_TIMEOUT_MS}",
                    case.id
                )));
            }
            validate_value_shape(&case.metadata).map_err(|_| {
                HarnessError::Evaluation(format!(
                    "case {} metadata exceeds the supported JSON depth or node count",
                    case.id
                ))
            })?;
            let case_bytes =
                bounded_serialized_size(case, MAX_EVALUATION_CASE_BYTES).map_err(|error| {
                    match error {
                        BoundedJsonError::LimitExceeded => HarnessError::Evaluation(format!(
                            "case {} exceeds {MAX_EVALUATION_CASE_BYTES} encoded bytes",
                            case.id
                        )),
                        BoundedJsonError::CannotEncode => HarnessError::Evaluation(format!(
                            "case {} could not be encoded",
                            case.id
                        )),
                    }
                })?;
            suite_bytes = suite_bytes.checked_add(case_bytes).ok_or_else(|| {
                HarnessError::Evaluation("evaluation suite size overflow".to_owned())
            })?;
            if suite_bytes > MAX_EVALUATION_SUITE_BYTES {
                return Err(HarnessError::Evaluation(format!(
                    "evaluation suite exceeds {MAX_EVALUATION_SUITE_BYTES} encoded bytes"
                )));
            }
        }
        Ok(Self {
            format_version: EVALUATION_FORMAT_VERSION,
            name,
            cases,
        })
    }
}

/// Runtime-independent target used by Evaluation Engine.
pub trait EvaluationTarget: Send + Sync {
    /// Executes one case under an engine-owned cooperative cancellation signal.
    fn execute<'a>(
        &'a self,
        case: EvaluationCase,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, TurnOutcome>;
}

impl EvaluationTarget for HarnessRuntime {
    fn execute<'a>(
        &'a self,
        case: EvaluationCase,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, TurnOutcome> {
        Box::pin(async move {
            let thread = self.create_thread().await?;
            self.run_turn_with_options(
                &thread.id,
                case.prompt,
                TurnExecutionOptions {
                    approval_requester: crate::ApprovalActor::LocalProcess,
                    memory_scope: case.memory_scope,
                    context: Vec::new(),
                    timeout: None,
                    cancellation,
                    model_event_sink: None,
                },
            )
            .await
        })
    }
}

/// Captured target execution supplied to every grader.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum EvaluationExecution {
    /// Target completed a Turn.
    Completed {
        /// Full terminal outcome and ordered Items.
        outcome: TurnOutcome,
    },
    /// Target returned a runtime error.
    Failed {
        /// Bounded runtime error text.
        error: String,
    },
}

/// Immutable case and execution pair graded independently.
#[derive(Clone, Debug, PartialEq)]
pub struct EvaluationSample {
    /// Original validated case.
    pub case: EvaluationCase,
    /// Captured target execution.
    pub execution: EvaluationExecution,
}

/// Stable metadata for an Evaluation grader.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraderDescriptor {
    /// Stable registry name.
    pub name: String,
    /// Human-readable scoring behavior.
    pub description: String,
}

/// Pure scoring capability that cannot change live Agent Loop control.
pub trait Grader: Send + Sync {
    /// Returns stable registration metadata.
    fn descriptor(&self) -> GraderDescriptor;
    /// Scores one immutable sample under an engine-owned cancellation signal.
    fn grade<'a>(
        &'a self,
        sample: Arc<EvaluationSample>,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, Grade>;
}

/// Successful grader settlement before report normalization.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Grade {
    /// Normalized score from 0.0 through 1.0.
    pub score: f64,
    /// Grader-defined pass/fail result.
    pub passed: bool,
    /// Optional bounded explanation.
    pub rationale: Option<String>,
}

/// Normalized grade or isolated grader failure.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum GradeOutcome {
    /// Valid scored result.
    Scored {
        /// Normalized score from 0.0 through 1.0.
        score: f64,
        /// Grader-defined pass/fail result.
        passed: bool,
        /// Optional bounded explanation.
        rationale: Option<String>,
    },
    /// Grader execution or contract failure.
    Error {
        /// Bounded error text.
        message: String,
    },
}

/// One named grader result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GradeRecord {
    /// Registered grader name.
    pub grader: String,
    /// Trust-bearing origin of the registered grader that produced this result.
    pub grader_origin: CapabilityOrigin,
    /// Normalized result.
    pub outcome: GradeOutcome,
}

/// Complete report for one case.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationCaseReport {
    /// Stable case identity.
    pub case_id: String,
    /// Target execution captured once for all graders.
    pub execution: EvaluationExecution,
    /// Grader results in deterministic name order.
    pub grades: Vec<GradeRecord>,
}

/// Deterministic suite report suitable for persistence or comparison.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationReport {
    /// Exact serialized format coordinate.
    pub format_version: u32,
    /// Stable suite name.
    pub suite: String,
    /// Case reports in deterministic identity order.
    pub cases: Vec<EvaluationCaseReport>,
}

/// Registered grader implementation and trust origin.
pub struct RegisteredGrader {
    /// Validated descriptor.
    pub descriptor: GraderDescriptor,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Executable grader.
    pub grader: Arc<dyn Grader>,
}

#[derive(Default)]
/// Deterministic collision-safe grader registry.
pub struct GraderRegistry {
    graders: BTreeMap<String, RegisteredGrader>,
}

impl GraderRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers one grader.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        grader: Arc<dyn Grader>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        if self.graders.len() >= MAX_EVALUATION_GRADERS {
            return Err(HarnessError::InvalidCapability(format!(
                "grader registry exceeds {MAX_EVALUATION_GRADERS} entries"
            )));
        }
        let descriptor = capture_capability_metadata("grader descriptor", || grader.descriptor())?;
        descriptor.validate()?;
        if self.graders.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        self.graders.insert(
            descriptor.name.clone(),
            RegisteredGrader {
                descriptor,
                origin,
                grader,
            },
        );
        Ok(())
    }

    /// Looks up a grader by stable name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredGrader> {
        self.graders.get(name)
    }

    /// Returns descriptors in deterministic name order.
    #[must_use]
    pub fn descriptors(&self) -> Vec<GraderDescriptor> {
        self.graders
            .values()
            .map(|registered| registered.descriptor.clone())
            .collect()
    }

    fn registered(&self) -> impl Iterator<Item = &RegisteredGrader> {
        self.graders.values()
    }
}

impl GraderDescriptor {
    /// Validates stable identity and human-readable metadata.
    pub fn validate(&self) -> Result<(), HarnessError> {
        validate_capability_name("grader", &self.name)?;
        if self.description.trim().is_empty()
            || self.description.len() > MAX_RATIONALE_BYTES
            || self.description.chars().any(char::is_control)
        {
            return Err(HarnessError::InvalidCapability(format!(
                "grader {} description must be 1-{MAX_RATIONALE_BYTES} non-control bytes",
                self.name
            )));
        }
        Ok(())
    }
}

/// Offline/online comparison runner that never participates in live completion.
pub struct EvaluationEngine {
    graders: GraderRegistry,
    max_case_concurrency: usize,
    max_grader_concurrency: usize,
    default_case_timeout: Duration,
    grader_timeout: Duration,
}

impl EvaluationEngine {
    /// Creates an engine over a validated grader registry.
    #[must_use]
    pub fn new(graders: GraderRegistry) -> Self {
        Self {
            graders,
            max_case_concurrency: DEFAULT_CASE_CONCURRENCY,
            max_grader_concurrency: DEFAULT_GRADER_CONCURRENCY,
            default_case_timeout: DEFAULT_CASE_TIMEOUT,
            grader_timeout: DEFAULT_GRADER_TIMEOUT,
        }
    }

    /// Sets independent bounded concurrency for cases and graders.
    pub fn with_concurrency(mut self, cases: usize, graders: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_EVALUATION_CONCURRENCY).contains(&cases)
            || !(1..=MAX_EVALUATION_CONCURRENCY).contains(&graders)
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Evaluation concurrency must be 1-{MAX_EVALUATION_CONCURRENCY}"
            )));
        }
        self.max_case_concurrency = cases;
        self.max_grader_concurrency = graders;
        Ok(self)
    }

    /// Sets the fallback case timeout and the per-grader timeout.
    pub fn with_timeouts(
        mut self,
        default_case: Duration,
        grader: Duration,
    ) -> Result<Self, HarnessError> {
        validate_evaluation_timeout("default case", default_case)?;
        validate_evaluation_timeout("grader", grader)?;
        self.default_case_timeout = default_case;
        self.grader_timeout = grader;
        Ok(self)
    }

    /// Executes cases and graders with bounded concurrency and panic isolation.
    pub async fn run(
        &self,
        target: Arc<dyn EvaluationTarget>,
        suite: EvaluationSuite,
    ) -> Result<EvaluationReport, HarnessError> {
        validate_format_version("suite", suite.format_version)?;
        let suite = EvaluationSuite::new(suite.name, suite.cases)?;
        let suite_name = suite.name;
        let graders = self
            .graders
            .registered()
            .map(|registered| GraderTask {
                identity: GraderIdentity {
                    name: registered.descriptor.name.clone(),
                    origin: registered.origin.clone(),
                },
                grader: registered.grader.clone(),
            })
            .collect::<Vec<_>>();
        let grader_identities = graders
            .iter()
            .map(|task| task.identity.clone())
            .collect::<Vec<_>>();
        let config = EvaluationRunConfig {
            max_grader_concurrency: self.max_grader_concurrency,
            default_case_timeout: self.default_case_timeout,
            grader_timeout: self.grader_timeout,
        };
        let mut pending = suite.cases.into_iter();
        let mut tasks = JoinSet::new();
        let mut identities = HashMap::new();
        for _ in 0..self.max_case_concurrency {
            let Some(case) = pending.next() else {
                break;
            };
            spawn_case(
                &mut tasks,
                &mut identities,
                target.clone(),
                graders.clone(),
                config,
                case,
            );
        }
        let mut reports = Vec::new();
        while let Some(result) = tasks.join_next_with_id().await {
            match result {
                Ok((task_id, report)) => {
                    identities.remove(&task_id);
                    reports.push(report);
                }
                Err(error) => {
                    let case_id = identities
                        .remove(&error.id())
                        .unwrap_or_else(|| "unknown-case".to_owned());
                    reports.push(failed_case_report(
                        case_id,
                        &grader_identities,
                        task_failure_message("evaluation case", &error),
                    ));
                }
            }
            if let Some(case) = pending.next() {
                spawn_case(
                    &mut tasks,
                    &mut identities,
                    target.clone(),
                    graders.clone(),
                    config,
                    case,
                );
            }
        }
        reports.sort_by(|left, right| left.case_id.cmp(&right.case_id));
        Ok(EvaluationReport {
            format_version: EVALUATION_FORMAT_VERSION,
            suite: suite_name,
            cases: reports,
        })
    }
}

#[derive(Clone)]
struct GraderIdentity {
    name: String,
    origin: CapabilityOrigin,
}

#[derive(Clone)]
struct GraderTask {
    identity: GraderIdentity,
    grader: Arc<dyn Grader>,
}

#[derive(Clone, Copy)]
struct EvaluationRunConfig {
    max_grader_concurrency: usize,
    default_case_timeout: Duration,
    grader_timeout: Duration,
}

fn spawn_case(
    tasks: &mut JoinSet<EvaluationCaseReport>,
    identities: &mut HashMap<TaskId, String>,
    target: Arc<dyn EvaluationTarget>,
    graders: Vec<GraderTask>,
    config: EvaluationRunConfig,
    case: EvaluationCase,
) {
    let case_id = case.id.clone();
    let handle = tasks.spawn(async move { evaluate_case(target, graders, config, case).await });
    identities.insert(handle.id(), case_id);
}

async fn evaluate_case(
    target: Arc<dyn EvaluationTarget>,
    graders: Vec<GraderTask>,
    config: EvaluationRunConfig,
    case: EvaluationCase,
) -> EvaluationCaseReport {
    let timeout = case
        .timeout_ms
        .map(Duration::from_millis)
        .unwrap_or(config.default_case_timeout);
    let execution = execute_target(target, case.clone(), timeout).await;
    let sample = Arc::new(EvaluationSample {
        case: case.clone(),
        execution: execution.clone(),
    });
    let mut pending = graders.into_iter();
    let mut tasks = JoinSet::new();
    let mut identities = HashMap::new();
    for _ in 0..config.max_grader_concurrency {
        let Some(task) = pending.next() else {
            break;
        };
        spawn_grader(
            &mut tasks,
            &mut identities,
            task,
            sample.clone(),
            config.grader_timeout,
        );
    }
    let mut grades = Vec::new();
    while let Some(result) = tasks.join_next_with_id().await {
        match result {
            Ok((task_id, grade)) => {
                identities.remove(&task_id);
                grades.push(grade);
            }
            Err(error) => {
                if let Some(identity) = identities.remove(&error.id()) {
                    grades.push(GradeRecord {
                        grader: identity.name,
                        grader_origin: identity.origin,
                        outcome: GradeOutcome::Error {
                            message: task_failure_message("grader", &error).to_owned(),
                        },
                    });
                }
            }
        }
        if let Some(task) = pending.next() {
            spawn_grader(
                &mut tasks,
                &mut identities,
                task,
                sample.clone(),
                config.grader_timeout,
            );
        }
    }
    grades.sort_by(|left, right| left.grader.cmp(&right.grader));
    EvaluationCaseReport {
        case_id: case.id,
        execution,
        grades,
    }
}

async fn execute_target(
    target: Arc<dyn EvaluationTarget>,
    case: EvaluationCase,
    timeout: Duration,
) -> EvaluationExecution {
    let cancellation = CancellationToken::new();
    let mut execution = target.execute(case, cancellation.clone());
    tokio::select! {
        result = &mut execution => normalize_execution(result),
        () = tokio::time::sleep(timeout) => {
            cancellation.cancel();
            let _ = tokio::time::timeout(EVALUATION_CLEANUP_GRACE, &mut execution).await;
            EvaluationExecution::Failed {
                error: "evaluation target timed out".to_owned(),
            }
        }
    }
}

fn normalize_execution(result: Result<TurnOutcome, HarnessError>) -> EvaluationExecution {
    match result {
        Ok(outcome) => {
            if outcome.turn.status != TurnStatus::Completed {
                return EvaluationExecution::Failed {
                    error: "evaluation target returned a non-completed Turn".to_owned(),
                };
            }
            let valid_shape = outcome.turn.items.iter().all(|item| {
                let value = match &item.kind {
                    ItemKind::ToolCall { input, .. } => Some(input),
                    ItemKind::ToolResult { output, .. } => Some(output),
                    _ => None,
                };
                value.is_none_or(|value| validate_value_shape(value).is_ok())
            });
            if !valid_shape {
                return EvaluationExecution::Failed {
                    error: "evaluation execution exceeds the supported JSON depth or node count"
                        .to_owned(),
                };
            }
            match bounded_serialized_size(&outcome, MAX_EVALUATION_EXECUTION_BYTES) {
                Ok(_) => EvaluationExecution::Completed { outcome },
                Err(BoundedJsonError::LimitExceeded) => EvaluationExecution::Failed {
                    error: format!(
                        "evaluation execution exceeds {MAX_EVALUATION_EXECUTION_BYTES} encoded bytes"
                    ),
                },
                Err(BoundedJsonError::CannotEncode) => EvaluationExecution::Failed {
                    error: "evaluation execution could not be encoded".to_owned(),
                },
            }
        }
        Err(error) => EvaluationExecution::Failed {
            error: bounded_message(&error.to_string()),
        },
    }
}

fn spawn_grader(
    tasks: &mut JoinSet<GradeRecord>,
    identities: &mut HashMap<TaskId, GraderIdentity>,
    task: GraderTask,
    sample: Arc<EvaluationSample>,
    timeout: Duration,
) {
    let identity = task.identity.clone();
    let handle = tasks.spawn(async move {
        let cancellation = CancellationToken::new();
        let mut grading = task.grader.grade(sample, cancellation.clone());
        let outcome = tokio::select! {
            result = &mut grading => match result {
                Ok(grade) => normalize_grade(grade),
                Err(error) => GradeOutcome::Error {
                    message: bounded_message(&error.to_string()),
                },
            },
            () = tokio::time::sleep(timeout) => {
                cancellation.cancel();
                let _ = tokio::time::timeout(EVALUATION_CLEANUP_GRACE, &mut grading).await;
                GradeOutcome::Error {
                    message: "grader timed out".to_owned(),
                }
            }
        };
        GradeRecord {
            grader: task.identity.name,
            grader_origin: task.identity.origin,
            outcome,
        }
    });
    identities.insert(handle.id(), identity);
}

fn task_failure_message(kind: &str, error: &tokio::task::JoinError) -> &'static str {
    if error.is_panic() {
        if kind == "grader" {
            "grader task panicked"
        } else {
            "evaluation case task panicked"
        }
    } else if kind == "grader" {
        "grader task was cancelled"
    } else {
        "evaluation case task was cancelled"
    }
}

fn validate_evaluation_timeout(label: &str, timeout: Duration) -> Result<(), HarnessError> {
    if timeout < Duration::from_millis(1)
        || timeout > Duration::from_millis(MAX_EVALUATION_TIMEOUT_MS)
    {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{label} timeout must be 1-{MAX_EVALUATION_TIMEOUT_MS} milliseconds"
        )));
    }
    Ok(())
}

fn failed_case_report(
    case_id: String,
    grader_identities: &[GraderIdentity],
    message: &str,
) -> EvaluationCaseReport {
    let message = bounded_message(message);
    EvaluationCaseReport {
        case_id,
        execution: EvaluationExecution::Failed {
            error: message.clone(),
        },
        grades: grader_identities
            .iter()
            .map(|identity| GradeRecord {
                grader: identity.name.clone(),
                grader_origin: identity.origin.clone(),
                outcome: GradeOutcome::Error {
                    message: message.clone(),
                },
            })
            .collect(),
    }
}

/// Minimum requirement for one case/grader result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BaselineRequirement {
    /// Required case identity.
    pub case_id: String,
    /// Required grader identity.
    pub grader: String,
    /// Required trust-bearing grader origin.
    pub grader_origin: CapabilityOrigin,
    /// Smallest acceptable normalized score.
    pub minimum_score: f64,
    /// Whether the grader must also report `passed`.
    pub must_pass: bool,
}

/// Validated exact regression expectations.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationBaseline {
    /// Exact serialized format coordinate.
    pub format_version: u32,
    /// Requirements in deterministic case/grader order.
    pub requirements: Vec<BaselineRequirement>,
}

impl EvaluationBaseline {
    /// Validates, sorts, and constructs a baseline.
    pub fn new(mut requirements: Vec<BaselineRequirement>) -> Result<Self, HarnessError> {
        if requirements.len() > MAX_BASELINE_REQUIREMENTS {
            return Err(HarnessError::Evaluation(format!(
                "evaluation baseline exceeds {MAX_BASELINE_REQUIREMENTS} requirements"
            )));
        }
        requirements.sort_by(|left, right| {
            (&left.case_id, &left.grader).cmp(&(&right.case_id, &right.grader))
        });
        let mut previous: Option<(&str, &str)> = None;
        for requirement in &requirements {
            validate_capability_name("baseline case", &requirement.case_id)?;
            validate_capability_name("baseline grader", &requirement.grader)?;
            validate_evaluation_origin("baseline grader", &requirement.grader_origin)?;
            let key = (requirement.case_id.as_str(), requirement.grader.as_str());
            if previous == Some(key) {
                return Err(HarnessError::Evaluation(format!(
                    "duplicate baseline requirement {} / {}",
                    requirement.case_id, requirement.grader
                )));
            }
            previous = Some(key);
            if !requirement.minimum_score.is_finite()
                || !(0.0..=1.0).contains(&requirement.minimum_score)
            {
                return Err(HarnessError::Evaluation(
                    "baseline minimum_score must be finite and within 0.0-1.0".to_owned(),
                ));
            }
        }
        Ok(Self {
            format_version: EVALUATION_FORMAT_VERSION,
            requirements,
        })
    }

    /// Compares a report without changing or re-running it.
    pub fn compare(&self, report: &EvaluationReport) -> Result<BaselineComparison, HarnessError> {
        validate_format_version("baseline", self.format_version)?;
        let validated = Self::new(self.requirements.clone())?;
        validate_report_for_comparison(report)?;
        let mut failures = Vec::new();
        for requirement in &validated.requirements {
            let Some(case) = report
                .cases
                .iter()
                .find(|case| case.case_id == requirement.case_id)
            else {
                failures.push(baseline_failure(requirement, "case is missing"));
                continue;
            };
            let Some(grade) = case
                .grades
                .iter()
                .find(|grade| grade.grader == requirement.grader)
            else {
                failures.push(baseline_failure(requirement, "grade is missing"));
                continue;
            };
            if grade.grader_origin != requirement.grader_origin {
                failures.push(baseline_failure(
                    requirement,
                    "grader origin differs from baseline",
                ));
                continue;
            }
            match &grade.outcome {
                GradeOutcome::Scored { score, passed, .. } => {
                    if *score < requirement.minimum_score {
                        failures.push(baseline_failure(
                            requirement,
                            &format!("score {score:.4} is below {:.4}", requirement.minimum_score),
                        ));
                    } else if requirement.must_pass && !passed {
                        failures.push(baseline_failure(
                            requirement,
                            "grader did not report passed",
                        ));
                    }
                }
                GradeOutcome::Error { message } => failures.push(baseline_failure(
                    requirement,
                    &format!("grader error: {message}"),
                )),
            }
        }
        Ok(BaselineComparison {
            passed: failures.is_empty(),
            failures,
        })
    }
}

fn validate_report_for_comparison(report: &EvaluationReport) -> Result<(), HarnessError> {
    validate_format_version("report", report.format_version)?;
    validate_capability_name("evaluation report suite", &report.suite)?;
    if report.cases.len() > MAX_EVALUATION_CASES {
        return Err(HarnessError::Evaluation(format!(
            "evaluation report exceeds {MAX_EVALUATION_CASES} cases"
        )));
    }
    let mut case_ids = BTreeMap::new();
    for case in &report.cases {
        validate_capability_name("evaluation report case", &case.case_id)?;
        if case_ids.insert(case.case_id.as_str(), ()).is_some() {
            return Err(HarnessError::Evaluation(format!(
                "evaluation report contains duplicate case {}",
                case.case_id
            )));
        }
        validate_report_execution(&case.case_id, &case.execution)?;
        if case.grades.len() > MAX_EVALUATION_GRADERS {
            return Err(HarnessError::Evaluation(format!(
                "evaluation case {} exceeds {MAX_EVALUATION_GRADERS} grades",
                case.case_id
            )));
        }
        let mut graders = BTreeMap::new();
        for grade in &case.grades {
            validate_capability_name("evaluation report grader", &grade.grader)?;
            validate_evaluation_origin("evaluation report grader", &grade.grader_origin)?;
            if graders.insert(grade.grader.as_str(), ()).is_some() {
                return Err(HarnessError::Evaluation(format!(
                    "evaluation case {} contains duplicate grader {}",
                    case.case_id, grade.grader
                )));
            }
            match &grade.outcome {
                GradeOutcome::Scored {
                    score, rationale, ..
                } => {
                    if !score.is_finite() || !(0.0..=1.0).contains(score) {
                        return Err(HarnessError::Evaluation(format!(
                            "evaluation case {} grader {} has an invalid score",
                            case.case_id, grade.grader
                        )));
                    }
                    if rationale.as_ref().is_some_and(|rationale| {
                        rationale.trim().is_empty() || rationale.len() > MAX_RATIONALE_BYTES
                    }) {
                        return Err(HarnessError::Evaluation(format!(
                            "evaluation case {} grader {} has an invalid rationale",
                            case.case_id, grade.grader
                        )));
                    }
                }
                GradeOutcome::Error { message }
                    if message.trim().is_empty() || message.len() > MAX_RATIONALE_BYTES =>
                {
                    return Err(HarnessError::Evaluation(format!(
                        "evaluation case {} grader {} has an invalid error",
                        case.case_id, grade.grader
                    )));
                }
                GradeOutcome::Error { .. } => {}
            }
        }
    }
    Ok(())
}

fn validate_format_version(kind: &str, format_version: u32) -> Result<(), HarnessError> {
    if format_version != EVALUATION_FORMAT_VERSION {
        return Err(HarnessError::Evaluation(format!(
            "unsupported Evaluation {kind} format version {format_version}; expected {EVALUATION_FORMAT_VERSION}"
        )));
    }
    Ok(())
}

fn validate_evaluation_origin(kind: &str, origin: &CapabilityOrigin) -> Result<(), HarnessError> {
    validate_capability_origin(origin)
        .map_err(|_| HarnessError::Evaluation(format!("{kind} has an invalid capability origin")))
}

fn validate_report_execution(
    case_id: &str,
    execution: &EvaluationExecution,
) -> Result<(), HarnessError> {
    match execution {
        EvaluationExecution::Completed { outcome } => {
            if outcome.turn.status != TurnStatus::Completed {
                return Err(HarnessError::Evaluation(format!(
                    "evaluation case {case_id} completed execution has a non-completed Turn"
                )));
            }
            for item in &outcome.turn.items {
                let value = match &item.kind {
                    ItemKind::ToolCall { input, .. } => Some(input),
                    ItemKind::ToolResult { output, .. } => Some(output),
                    _ => None,
                };
                if value.is_some_and(|value| validate_value_shape(value).is_err()) {
                    return Err(HarnessError::Evaluation(format!(
                        "evaluation case {case_id} execution contains unsupported JSON"
                    )));
                }
            }
            bounded_serialized_size(outcome, MAX_EVALUATION_EXECUTION_BYTES).map_err(|error| {
                match error {
                    BoundedJsonError::LimitExceeded => HarnessError::Evaluation(format!(
                        "evaluation case {case_id} execution exceeds \
                         {MAX_EVALUATION_EXECUTION_BYTES} encoded bytes"
                    )),
                    BoundedJsonError::CannotEncode => HarnessError::Evaluation(format!(
                        "evaluation case {case_id} execution could not be encoded"
                    )),
                }
            })?;
        }
        EvaluationExecution::Failed { error }
            if error.trim().is_empty() || error.len() > MAX_RATIONALE_BYTES =>
        {
            return Err(HarnessError::Evaluation(format!(
                "evaluation case {case_id} has an invalid execution error"
            )));
        }
        EvaluationExecution::Failed { .. } => {}
    }
    Ok(())
}

/// One failed baseline expectation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaselineFailure {
    /// Missing or regressed case.
    pub case_id: String,
    /// Missing, failed, or regressed grader.
    pub grader: String,
    /// Actionable comparison reason.
    pub reason: String,
}

/// Complete deterministic baseline comparison.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BaselineComparison {
    /// Whether every exact requirement passed.
    pub passed: bool,
    /// Failed requirements in baseline order.
    pub failures: Vec<BaselineFailure>,
}

fn normalize_grade(grade: Grade) -> GradeOutcome {
    if !grade.score.is_finite() || !(0.0..=1.0).contains(&grade.score) {
        return GradeOutcome::Error {
            message: "grader score must be finite and within 0.0-1.0".to_owned(),
        };
    }
    if let Some(rationale) = &grade.rationale
        && (rationale.trim().is_empty() || rationale.len() > MAX_RATIONALE_BYTES)
    {
        return GradeOutcome::Error {
            message: format!("grader rationale must be 1-{MAX_RATIONALE_BYTES} bytes"),
        };
    }
    GradeOutcome::Scored {
        score: grade.score,
        passed: grade.passed,
        rationale: grade.rationale,
    }
}

fn bounded_message(message: &str) -> String {
    if message.len() <= MAX_RATIONALE_BYTES {
        message.to_owned()
    } else {
        let mut end = MAX_RATIONALE_BYTES - '…'.len_utf8();
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &message[..end])
    }
}

fn baseline_failure(requirement: &BaselineRequirement, reason: &str) -> BaselineFailure {
    BaselineFailure {
        case_id: requirement.case_id.clone(),
        grader: requirement.grader.clone(),
        reason: reason.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use serde_json::{Value, json};

    use super::{
        BaselineRequirement, EvaluationBaseline, EvaluationCase, EvaluationCaseReport,
        EvaluationEngine, EvaluationExecution, EvaluationReport, EvaluationSample, EvaluationSuite,
        EvaluationTarget, Grade, GradeOutcome, GradeRecord, Grader, GraderDescriptor,
        GraderRegistry, MAX_BASELINE_REQUIREMENTS,
    };
    use crate::{
        CapabilityOrigin, HarnessError, HarnessFuture, Item, ItemKind, MemoryScope, ThreadId, Turn,
        TurnId, TurnOutcome, TurnStatus,
    };

    struct FakeTarget;

    impl EvaluationTarget for FakeTarget {
        fn execute<'a>(
            &'a self,
            case: EvaluationCase,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, TurnOutcome> {
            Box::pin(async move {
                if case.prompt == "fail" {
                    return Err(HarnessError::Model("fixture failure".to_owned()));
                }
                let thread_id = ThreadId::generate();
                Ok(TurnOutcome {
                    turn: Turn {
                        id: TurnId::generate(),
                        thread_id,
                        status: TurnStatus::Completed,
                        items: Vec::<Item>::new(),
                    },
                    final_text: case.prompt,
                })
            })
        }
    }

    struct ExactGrader;

    impl Grader for ExactGrader {
        fn descriptor(&self) -> GraderDescriptor {
            GraderDescriptor {
                name: "exact".to_owned(),
                description: "Checks expected text".to_owned(),
            }
        }

        fn grade<'a>(
            &'a self,
            sample: Arc<EvaluationSample>,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, Grade> {
            Box::pin(async move {
                let expected = sample
                    .case
                    .metadata
                    .get("expected")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let actual = match &sample.execution {
                    EvaluationExecution::Completed { outcome } => outcome.final_text.clone(),
                    EvaluationExecution::Failed { .. } => String::new(),
                };
                let passed = actual == expected;
                Ok(Grade {
                    score: if passed { 1.0 } else { 0.0 },
                    passed,
                    rationale: Some(format!("expected {expected:?}")),
                })
            })
        }
    }

    struct PanickingGrader;

    impl Grader for PanickingGrader {
        fn descriptor(&self) -> GraderDescriptor {
            GraderDescriptor {
                name: "panic".to_owned(),
                description: "Exercises grader panic isolation".to_owned(),
            }
        }

        fn grade<'a>(
            &'a self,
            _sample: Arc<EvaluationSample>,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, Grade> {
            Box::pin(async move {
                panic!("grader fixture panic");
            })
        }
    }

    struct BoundedTarget {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl EvaluationTarget for BoundedTarget {
        fn execute<'a>(
            &'a self,
            case: EvaluationCase,
            _cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, TurnOutcome> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                if case.prompt == "panic" {
                    panic!("target fixture panic");
                }
                let thread_id = ThreadId::generate();
                Ok(TurnOutcome {
                    turn: Turn {
                        id: TurnId::generate(),
                        thread_id,
                        status: TurnStatus::Completed,
                        items: Vec::new(),
                    },
                    final_text: case.prompt,
                })
            })
        }
    }

    struct CooperativeSlowTarget {
        cancellation_observed: Arc<AtomicUsize>,
    }

    impl EvaluationTarget for CooperativeSlowTarget {
        fn execute<'a>(
            &'a self,
            _case: EvaluationCase,
            cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, TurnOutcome> {
            Box::pin(async move {
                cancellation.cancelled().await;
                self.cancellation_observed.store(1, Ordering::SeqCst);
                Err(HarnessError::Cancelled {
                    phase: crate::ExecutionPhase::Model,
                })
            })
        }
    }

    struct SlowGrader {
        cancellation_observed: Arc<AtomicUsize>,
    }

    impl Grader for SlowGrader {
        fn descriptor(&self) -> GraderDescriptor {
            GraderDescriptor {
                name: "slow".to_owned(),
                description: "Exercises grader timeout isolation".to_owned(),
            }
        }

        fn grade<'a>(
            &'a self,
            _sample: Arc<EvaluationSample>,
            cancellation: crate::CancellationToken,
        ) -> HarnessFuture<'a, Grade> {
            Box::pin(async move {
                cancellation.cancelled().await;
                self.cancellation_observed.store(1, Ordering::SeqCst);
                Err(HarnessError::Cancelled {
                    phase: crate::ExecutionPhase::Evaluation,
                })
            })
        }
    }

    fn case(id: &str, prompt: &str, expected: &str) -> EvaluationCase {
        EvaluationCase {
            id: id.to_owned(),
            prompt: prompt.to_owned(),
            memory_scope: MemoryScope::default(),
            timeout_ms: Some(1_000),
            metadata: json!({ "expected": expected }),
        }
    }

    #[tokio::test]
    async fn reports_case_failures_and_compares_exact_baseline() {
        let suite = EvaluationSuite::new(
            "core",
            vec![case("second", "fail", "ok"), case("first", "ok", "ok")],
        )
        .expect("suite");
        let mut graders = GraderRegistry::new();
        graders
            .register(CapabilityOrigin::BuiltIn, Arc::new(ExactGrader))
            .expect("grader");
        let report = EvaluationEngine::new(graders)
            .run(Arc::new(FakeTarget), suite)
            .await
            .expect("evaluation report");

        assert_eq!(report.cases[0].case_id, "first");
        assert_eq!(report.format_version, super::EVALUATION_FORMAT_VERSION);
        assert_eq!(
            report.cases[0].grades[0].grader_origin,
            CapabilityOrigin::BuiltIn
        );
        assert!(matches!(
            report.cases[0].grades[0].outcome,
            GradeOutcome::Scored {
                score: 1.0,
                passed: true,
                ..
            }
        ));
        assert!(matches!(
            report.cases[1].execution,
            EvaluationExecution::Failed { .. }
        ));

        let comparison = EvaluationBaseline::new(vec![
            BaselineRequirement {
                case_id: "first".to_owned(),
                grader: "exact".to_owned(),
                grader_origin: CapabilityOrigin::BuiltIn,
                minimum_score: 1.0,
                must_pass: true,
            },
            BaselineRequirement {
                case_id: "second".to_owned(),
                grader: "exact".to_owned(),
                grader_origin: CapabilityOrigin::BuiltIn,
                minimum_score: 1.0,
                must_pass: true,
            },
        ])
        .expect("baseline")
        .compare(&report)
        .expect("validated comparison");
        assert!(!comparison.passed);
        assert_eq!(comparison.failures.len(), 1);
        assert_eq!(comparison.failures[0].case_id, "second");

        let origin_mismatch = EvaluationBaseline::new(vec![BaselineRequirement {
            case_id: "first".to_owned(),
            grader: "exact".to_owned(),
            grader_origin: CapabilityOrigin::External {
                id: "replacement-grader".to_owned(),
            },
            minimum_score: 1.0,
            must_pass: true,
        }])
        .expect("origin-bound baseline")
        .compare(&report)
        .expect("origin comparison");
        assert!(!origin_mismatch.passed);
        assert_eq!(
            origin_mismatch.failures[0].reason,
            "grader origin differs from baseline"
        );
    }

    #[test]
    fn rejects_duplicate_cases_and_invalid_thresholds() {
        assert!(
            EvaluationSuite::new("core", vec![case("same", "a", "a"), case("same", "b", "b")])
                .is_err()
        );
        assert!(
            EvaluationBaseline::new(vec![BaselineRequirement {
                case_id: "case".to_owned(),
                grader: "grade".to_owned(),
                grader_origin: CapabilityOrigin::BuiltIn,
                minimum_score: f64::NAN,
                must_pass: false,
            }])
            .is_err()
        );
        assert!(
            EvaluationBaseline::new(vec![BaselineRequirement {
                case_id: "case".to_owned(),
                grader: "grade".to_owned(),
                grader_origin: CapabilityOrigin::External { id: " ".to_owned() },
                minimum_score: 1.0,
                must_pass: true,
            }])
            .is_err()
        );
        let mut invalid_timeout = case("timeout", "a", "a");
        invalid_timeout.timeout_ms = Some(0);
        assert!(EvaluationSuite::new("core", vec![invalid_timeout]).is_err());
        let oversized_suite = (0..=super::MAX_EVALUATION_CASES)
            .map(|index| case(&format!("case-{index:03}"), "a", "a"))
            .collect();
        assert!(EvaluationSuite::new("core", oversized_suite).is_err());
        let oversized_baseline = (0..=MAX_BASELINE_REQUIREMENTS)
            .map(|index| BaselineRequirement {
                case_id: format!("case-{:04}", index / super::MAX_EVALUATION_GRADERS),
                grader: format!("grade-{:04}", index % super::MAX_EVALUATION_GRADERS),
                grader_origin: CapabilityOrigin::BuiltIn,
                minimum_score: 1.0,
                must_pass: true,
            })
            .collect();
        assert!(EvaluationBaseline::new(oversized_baseline).is_err());

        let mut deeply_nested = Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            deeply_nested = Value::Array(vec![deeply_nested]);
        }
        let mut invalid_metadata = case("deep", "a", "a");
        invalid_metadata.metadata = deeply_nested;
        assert!(EvaluationSuite::new("core", vec![invalid_metadata]).is_err());
    }

    #[tokio::test]
    async fn engine_revalidates_a_deserialized_suite_before_execution() {
        let invalid = EvaluationSuite {
            format_version: super::EVALUATION_FORMAT_VERSION,
            name: "core".to_owned(),
            cases: vec![case("duplicate", "a", "a"), case("duplicate", "b", "b")],
        };
        let error = EvaluationEngine::new(GraderRegistry::new())
            .run(Arc::new(FakeTarget), invalid)
            .await
            .expect_err("deserialized duplicate must fail");
        assert!(error.to_string().contains("duplicate evaluation case"));

        let mut future = EvaluationSuite::new("core", vec![]).expect("current suite");
        future.format_version += 1;
        let error = EvaluationEngine::new(GraderRegistry::new())
            .run(Arc::new(FakeTarget), future)
            .await
            .expect_err("future suite format must fail");
        assert!(error.to_string().contains("format version"));
    }

    #[test]
    fn baseline_revalidates_a_deserialized_report() {
        let baseline = EvaluationBaseline::new(vec![BaselineRequirement {
            case_id: "case".to_owned(),
            grader: "grade".to_owned(),
            grader_origin: CapabilityOrigin::BuiltIn,
            minimum_score: 1.0,
            must_pass: true,
        }])
        .expect("baseline");
        let mut report = EvaluationReport {
            format_version: super::EVALUATION_FORMAT_VERSION,
            suite: "core".to_owned(),
            cases: vec![EvaluationCaseReport {
                case_id: "case".to_owned(),
                execution: EvaluationExecution::Failed {
                    error: "fixture".to_owned(),
                },
                grades: vec![GradeRecord {
                    grader: "grade".to_owned(),
                    grader_origin: CapabilityOrigin::BuiltIn,
                    outcome: GradeOutcome::Scored {
                        score: f64::NAN,
                        passed: true,
                        rationale: None,
                    },
                }],
            }],
        };

        assert!(baseline.compare(&report).is_err());

        report.format_version += 1;
        let error = baseline
            .compare(&report)
            .expect_err("future report format must fail");
        assert!(error.to_string().contains("format version"));

        report.format_version = super::EVALUATION_FORMAT_VERSION;
        let mut future_baseline = baseline.clone();
        future_baseline.format_version += 1;
        let error = future_baseline
            .compare(&report)
            .expect_err("future baseline format must fail");
        assert!(error.to_string().contains("format version"));
    }

    #[test]
    fn bounded_messages_preserve_utf8_and_the_byte_limit() {
        let message = "界".repeat(super::MAX_RATIONALE_BYTES);
        let bounded = super::bounded_message(&message);

        assert!(bounded.ends_with('…'));
        assert!(bounded.len() <= super::MAX_RATIONALE_BYTES);
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
    }

    #[test]
    fn normalizes_deep_target_json_to_a_bounded_failure() {
        let mut deeply_nested = Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            deeply_nested = Value::Array(vec![deeply_nested]);
        }
        let outcome = TurnOutcome {
            turn: Turn {
                id: TurnId::generate(),
                thread_id: ThreadId::generate(),
                status: TurnStatus::Completed,
                items: vec![Item::new(ItemKind::ToolResult {
                    call_id: "call-deep".to_owned(),
                    output: deeply_nested,
                    is_error: false,
                })],
            },
            final_text: "untrusted target output".to_owned(),
        };

        assert!(matches!(
            super::normalize_execution(Ok(outcome)),
            EvaluationExecution::Failed { ref error }
                if error.contains("depth or node count")
        ));
    }

    #[test]
    fn normalizes_inconsistent_success_to_a_bounded_failure() {
        let outcome = TurnOutcome {
            turn: Turn {
                id: TurnId::generate(),
                thread_id: ThreadId::generate(),
                status: TurnStatus::Running,
                items: Vec::new(),
            },
            final_text: "not terminal".to_owned(),
        };

        assert!(matches!(
            super::normalize_execution(Ok(outcome)),
            EvaluationExecution::Failed { ref error }
                if error == "evaluation target returned a non-completed Turn"
        ));
    }

    #[tokio::test]
    async fn bounds_parallelism_and_isolates_target_and_grader_panics() {
        let suite = EvaluationSuite::new(
            "parallel",
            vec![
                case("d", "d", "d"),
                case("b", "panic", "panic"),
                case("c", "c", "c"),
                case("a", "a", "a"),
            ],
        )
        .expect("suite");
        let mut graders = GraderRegistry::new();
        graders
            .register(CapabilityOrigin::BuiltIn, Arc::new(ExactGrader))
            .expect("exact grader");
        graders
            .register(
                CapabilityOrigin::External {
                    id: "panic-provider".to_owned(),
                },
                Arc::new(PanickingGrader),
            )
            .expect("panic grader");
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let report = EvaluationEngine::new(graders)
            .with_concurrency(2, 2)
            .expect("concurrency")
            .run(
                Arc::new(BoundedTarget {
                    active,
                    peak: peak.clone(),
                }),
                suite,
            )
            .await
            .expect("parallel report");

        assert_eq!(
            report
                .cases
                .iter()
                .map(|case| case.case_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c", "d"]
        );
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert!(matches!(
            report.cases[1].execution,
            EvaluationExecution::Failed { .. }
        ));
        assert!(report.cases.iter().all(|case| {
            case.grades.iter().any(|grade| {
                grade.grader == "panic"
                    && grade.grader_origin
                        == (CapabilityOrigin::External {
                            id: "panic-provider".to_owned(),
                        })
                    && matches!(grade.outcome, GradeOutcome::Error { .. })
            })
        }));
        assert!(
            EvaluationEngine::new(GraderRegistry::new())
                .with_concurrency(0, 1)
                .is_err()
        );
        assert!(
            EvaluationEngine::new(GraderRegistry::new())
                .with_timeouts(Duration::ZERO, Duration::from_secs(1))
                .is_err()
        );
    }

    #[tokio::test]
    async fn bounds_target_and_grader_duration() {
        let mut slow_case = case("slow-case", "wait", "wait");
        slow_case.timeout_ms = None;
        let suite = EvaluationSuite::new("timeouts", vec![slow_case]).expect("suite");
        let grader_cancellation_observed = Arc::new(AtomicUsize::new(0));
        let mut graders = GraderRegistry::new();
        graders
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(SlowGrader {
                    cancellation_observed: grader_cancellation_observed.clone(),
                }),
            )
            .expect("slow grader");
        let cancellation_observed = Arc::new(AtomicUsize::new(0));
        let report = EvaluationEngine::new(graders)
            .with_timeouts(Duration::from_millis(5), Duration::from_millis(5))
            .expect("timeouts")
            .run(
                Arc::new(CooperativeSlowTarget {
                    cancellation_observed: cancellation_observed.clone(),
                }),
                suite,
            )
            .await
            .expect("timeout report");

        assert_eq!(cancellation_observed.load(Ordering::SeqCst), 1);
        assert_eq!(grader_cancellation_observed.load(Ordering::SeqCst), 1);
        assert!(matches!(
            &report.cases[0].execution,
            EvaluationExecution::Failed { error }
                if error == "evaluation target timed out"
        ));
        assert!(matches!(
            &report.cases[0].grades[0].outcome,
            GradeOutcome::Error { message } if message == "grader timed out"
        ));
    }
}
