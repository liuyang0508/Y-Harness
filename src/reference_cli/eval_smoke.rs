//! Deterministic, dependency-free regression gate for the reference Harness.

use std::{error::Error, sync::Arc};

use serde::Serialize;
use y_harness::{
    BaselineComparison, CapabilityOrigin, EVALUATION_FORMAT_VERSION, EvaluationBaseline,
    EvaluationEngine, EvaluationExecution, EvaluationReport, EvaluationSample, EvaluationSuite,
    Grade, Grader, GraderDescriptor, GraderRegistry, HarnessError, HarnessFuture, ItemKind,
    MemoryEventStore, PolicyDecision, StateEngine, TurnStatus,
};

use super::runtime_with_demo_capabilities;

const SUITE_JSON: &str = include_str!("../../evals/harness-smoke-suite.json");
const BASELINE_JSON: &str = include_str!("../../evals/harness-smoke-baseline.json");

type EvalResult<T> = Result<T, Box<dyn Error>>;

#[derive(Serialize)]
struct SmokeEvaluationOutput {
    schema_version: u32,
    report: EvaluationReport,
    comparison: BaselineComparison,
}

pub(super) async fn run() -> EvalResult<()> {
    let suite = validated_suite()?;
    let baseline = validated_baseline()?;
    let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
    let runtime = Arc::new(runtime_with_demo_capabilities(state)?);
    let mut graders = GraderRegistry::new();
    graders.register(CapabilityOrigin::BuiltIn, Arc::new(ExactOutputGrader))?;
    graders.register(CapabilityOrigin::BuiltIn, Arc::new(StateContractGrader))?;
    let report = EvaluationEngine::new(graders).run(runtime, suite).await?;
    let comparison = baseline.compare(&report)?;
    let passed = comparison.passed;

    println!(
        "{}",
        serde_json::to_string_pretty(&SmokeEvaluationOutput {
            schema_version: EVALUATION_FORMAT_VERSION,
            report,
            comparison,
        })?
    );
    if passed {
        Ok(())
    } else {
        Err("Harness smoke evaluation regressed".into())
    }
}

fn validated_suite() -> Result<EvaluationSuite, HarnessError> {
    serde_json::from_str(SUITE_JSON)
        .map_err(|error| HarnessError::Evaluation(format!("invalid smoke suite: {error}")))
}

fn validated_baseline() -> Result<EvaluationBaseline, HarnessError> {
    serde_json::from_str(BASELINE_JSON)
        .map_err(|error| HarnessError::Evaluation(format!("invalid smoke baseline: {error}")))
}

struct ExactOutputGrader;

impl Grader for ExactOutputGrader {
    fn descriptor(&self) -> GraderDescriptor {
        GraderDescriptor {
            name: "exact-output".to_owned(),
            description: "Requires the final assistant text to match the versioned fixture"
                .to_owned(),
        }
    }

    fn grade<'a>(
        &'a self,
        sample: Arc<EvaluationSample>,
        _cancellation: y_harness::CancellationToken,
    ) -> HarnessFuture<'a, Grade> {
        Box::pin(async move {
            let expected = sample
                .case
                .metadata
                .get("expected_output")
                .and_then(serde_json::Value::as_str);
            let actual = match &sample.execution {
                EvaluationExecution::Completed { outcome } => Some(outcome.final_text.as_str()),
                EvaluationExecution::Failed { .. } => None,
            };
            Ok(boolean_grade(
                expected.is_some() && expected == actual,
                "final output matches the versioned fixture",
                "final output differs from the versioned fixture",
            ))
        })
    }
}

struct StateContractGrader;

impl Grader for StateContractGrader {
    fn descriptor(&self) -> GraderDescriptor {
        GraderDescriptor {
            name: "state-contract".to_owned(),
            description: "Requires a completed and correlated Tool-call state sequence".to_owned(),
        }
    }

    fn grade<'a>(
        &'a self,
        sample: Arc<EvaluationSample>,
        _cancellation: y_harness::CancellationToken,
    ) -> HarnessFuture<'a, Grade> {
        Box::pin(async move {
            let valid = match &sample.execution {
                EvaluationExecution::Completed { outcome } => {
                    state_contract_holds(&sample, outcome)
                }
                EvaluationExecution::Failed { .. } => false,
            };
            Ok(boolean_grade(
                valid,
                "Turn preserves the correlated Tool-call state contract",
                "Turn violates the correlated Tool-call state contract",
            ))
        })
    }
}

fn state_contract_holds(sample: &EvaluationSample, outcome: &y_harness::TurnOutcome) -> bool {
    if outcome.turn.status != TurnStatus::Completed {
        return false;
    }

    let expected_tool_payload = serde_json::json!({ "text": sample.case.prompt });
    let mut user_positions = Vec::new();
    let mut call = Vec::new();
    let mut policy = Vec::new();
    let mut result = Vec::new();
    let mut assistant = Vec::new();

    for (position, item) in outcome.turn.items.iter().enumerate() {
        match &item.kind {
            ItemKind::UserMessage { content } if content == &sample.case.prompt => {
                user_positions.push(position);
            }
            ItemKind::ToolCall {
                call_id,
                name,
                input,
                ..
            } if name == "echo" && input == &expected_tool_payload => {
                call.push((position, call_id.as_str()));
            }
            ItemKind::PolicyDecision {
                call_id,
                tool_origin: Some(CapabilityOrigin::BuiltIn),
                decision,
            } if decision == &PolicyDecision::Allow => {
                policy.push((position, call_id.as_str()));
            }
            ItemKind::ToolResult {
                call_id,
                output,
                is_error,
                ..
            } if !is_error && output == &expected_tool_payload => {
                result.push((position, call_id.as_str()));
            }
            ItemKind::AssistantMessage { content, .. } if content == &outcome.final_text => {
                assistant.push(position);
            }
            _ => {}
        }
    }

    matches!(
        (
            user_positions.as_slice(),
            call.as_slice(),
            policy.as_slice(),
            result.as_slice(),
            assistant.as_slice(),
        ),
        (
            [user_position],
            [(call_position, call_id)],
            [(policy_position, policy_call_id)],
            [(result_position, result_call_id)],
            [assistant_position],
        ) if user_position < call_position
            && call_position < policy_position
            && policy_position < result_position
            && result_position < assistant_position
            && call_id == policy_call_id
            && call_id == result_call_id
    )
}

fn boolean_grade(passed: bool, success: &str, failure: &str) -> Grade {
    Grade {
        score: if passed { 1.0 } else { 0.0 },
        passed,
        rationale: Some(if passed { success } else { failure }.to_owned()),
    }
}
