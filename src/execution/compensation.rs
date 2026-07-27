//! Explicit, State-backed adapters for Tool-specific compensation.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ApprovalDecision, CancellationToken, HarnessError, HarnessFuture, ItemKind, PolicyDecision,
    StateEngine, Thread, Tool, ToolContext, ToolDescriptor, Turn, TurnId,
    kernel::{capture_capability_metadata, validate_capability_name},
};

const MAX_COMPENSATION_DESCRIPTION_BYTES: usize = 4_096;
const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// Frozen metadata for one Tool-specific compensation capability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompensationDescriptor {
    /// Ordinary Tool name used for Policy, approval, Model, and State evidence.
    pub name: String,
    /// Model-visible explanation of the reversal and its limits.
    pub description: String,
    /// Exact Tool whose successful results this capability can compensate.
    pub target_tool: String,
}

/// Model-proposed reference to one successful, durably recorded Tool effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompensationRequest {
    /// Turn containing the original Tool call and result.
    pub target_turn_id: TurnId,
    /// Original Tool-call correlation identity.
    pub target_call_id: String,
    /// Stable provider-scoped key reused by every retry for this target.
    pub idempotency_key: String,
}

/// Authoritative original effect supplied to a Tool-specific compensator.
#[derive(Clone, Debug)]
pub struct CompensationContext {
    /// Thread containing both the original effect and compensation attempt.
    pub thread_id: crate::ThreadId,
    /// Turn executing the compensation Tool.
    pub compensation_turn_id: TurnId,
    /// Correlation identity of the compensation Tool call.
    pub compensation_call_id: String,
    /// Turn containing the original effect.
    pub target_turn_id: TurnId,
    /// Correlation identity of the original Tool call.
    pub target_call_id: String,
    /// Frozen Tool identity whose effect is being compensated.
    pub target_tool: String,
    /// Original model-proposed input reconstructed from authoritative State.
    pub original_input: Value,
    /// Original successful output reconstructed from authoritative State.
    pub original_output: Value,
    /// Stable idempotency identity required for safe uncertain retries.
    pub idempotency_key: String,
    /// Cooperative cancellation signal for the compensation attempt.
    pub cancellation: CancellationToken,
}

/// Tool-specific reversal implementation.
///
/// Implementations must settle repeated calls with the same
/// `idempotency_key` idempotently. Compensation remains a side effect and may
/// itself be only partially observed when its future fails.
pub trait ToolCompensator: Send + Sync {
    /// Returns stable metadata captured once by [`CompensationTool`].
    fn descriptor(&self) -> CompensationDescriptor;

    /// Attempts one explicitly authorized compensation.
    fn compensate<'a>(&'a self, context: CompensationContext) -> HarnessFuture<'a, Value>;
}

/// Ordinary Tool adapter that resolves compensation evidence from State.
///
/// Register this adapter in [`crate::ToolRegistry`]. The Agent Loop then applies
/// the same Policy, approval, cancellation, evidence ordering, and output
/// bounds used for every other Tool. The adapter never infers compensation from
/// Verification failure.
pub struct CompensationTool {
    descriptor: ToolDescriptor,
    compensation: CompensationDescriptor,
    state: StateEngine,
    compensator: Arc<dyn ToolCompensator>,
}

impl CompensationTool {
    /// Captures and validates one compensator before it becomes executable.
    pub fn new(
        state: StateEngine,
        compensator: Arc<dyn ToolCompensator>,
    ) -> Result<Self, HarnessError> {
        let compensation =
            capture_capability_metadata("compensator descriptor", || compensator.descriptor())?;
        validate_compensation_descriptor(&compensation)?;
        let descriptor = ToolDescriptor {
            name: compensation.name.clone(),
            description: compensation.description.clone(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "target_turn_id": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 256
                    },
                    "target_call_id": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 256
                    },
                    "idempotency_key": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_IDEMPOTENCY_KEY_BYTES
                    }
                },
                "required": [
                    "target_turn_id",
                    "target_call_id",
                    "idempotency_key"
                ],
                "additionalProperties": false
            }),
        };
        Ok(Self {
            descriptor,
            compensation,
            state,
            compensator,
        })
    }

    /// Returns the frozen Tool-to-compensator relationship.
    #[must_use]
    pub fn compensation_descriptor(&self) -> CompensationDescriptor {
        self.compensation.clone()
    }
}

impl Tool for CompensationTool {
    fn descriptor(&self) -> ToolDescriptor {
        self.descriptor.clone()
    }

    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            let request: CompensationRequest = serde_json::from_value(input).map_err(|_| {
                HarnessError::Tool("compensation request does not match its schema".to_owned())
            })?;
            validate_compensation_request(&request)?;
            let thread = self
                .state
                .load_thread(&context.thread_id)
                .await?
                .ok_or_else(|| {
                    HarnessError::Tool(
                        "compensation thread is absent from authoritative State".to_owned(),
                    )
                })?;
            validate_current_call(&thread, &context, &self.compensation.name, &request)?;
            let (original_input, original_output) =
                resolve_original_effect(&thread, &self.compensation, &request)?;
            if let Some(output) =
                prior_successful_compensation(&thread, &context, &self.compensation.name, &request)?
            {
                return Ok(output);
            }
            self.compensator
                .compensate(CompensationContext {
                    thread_id: context.thread_id,
                    compensation_turn_id: context.turn_id,
                    compensation_call_id: context.call_id,
                    target_turn_id: request.target_turn_id,
                    target_call_id: request.target_call_id,
                    target_tool: self.compensation.target_tool.clone(),
                    original_input,
                    original_output,
                    idempotency_key: request.idempotency_key,
                    cancellation: context.cancellation,
                })
                .await
        })
    }
}

fn validate_compensation_descriptor(
    descriptor: &CompensationDescriptor,
) -> Result<(), HarnessError> {
    validate_capability_name("compensation tool", &descriptor.name)?;
    validate_capability_name("compensation target tool", &descriptor.target_tool)?;
    if descriptor.name == descriptor.target_tool {
        return Err(HarnessError::InvalidCapability(
            "compensation Tool must differ from its target Tool".to_owned(),
        ));
    }
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_COMPENSATION_DESCRIPTION_BYTES
        || descriptor.description.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "compensation description must be 1-{MAX_COMPENSATION_DESCRIPTION_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_compensation_request(request: &CompensationRequest) -> Result<(), HarnessError> {
    validate_identity("target Tool call", &request.target_call_id)?;
    validate_identity("compensation idempotency key", &request.idempotency_key)
}

fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_IDEMPOTENCY_KEY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::Tool(format!(
            "{kind} must be 1-{MAX_IDEMPOTENCY_KEY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_current_call(
    thread: &Thread,
    context: &ToolContext,
    compensation_tool: &str,
    request: &CompensationRequest,
) -> Result<(), HarnessError> {
    let turn = find_turn(thread, &context.turn_id, "compensation")?;
    if !matches!(turn.status, crate::TurnStatus::Running) {
        return Err(HarnessError::Tool(
            "compensation Tool call is not owned by a running Turn".to_owned(),
        ));
    }
    let (call_index, name, input) = unique_tool_call(turn, &context.call_id)?;
    if name != compensation_tool {
        return Err(HarnessError::Tool(
            "compensation Tool identity does not match authoritative State".to_owned(),
        ));
    }
    if execution_authorization_index(turn, &context.call_id, compensation_tool, call_index)?
        .is_none()
    {
        return Err(HarnessError::Tool(
            "compensation Tool call has no authoritative execution authorization".to_owned(),
        ));
    }
    let recorded: CompensationRequest = serde_json::from_value(input.clone())
        .map_err(|_| HarnessError::Tool("recorded compensation request is malformed".to_owned()))?;
    if &recorded != request {
        return Err(HarnessError::Tool(
            "compensation request differs from authoritative State".to_owned(),
        ));
    }
    Ok(())
}

fn resolve_original_effect(
    thread: &Thread,
    compensation: &CompensationDescriptor,
    request: &CompensationRequest,
) -> Result<(Value, Value), HarnessError> {
    let turn = find_turn(thread, &request.target_turn_id, "target")?;
    let (call_index, name, input) = unique_tool_call(turn, &request.target_call_id)?;
    if name != compensation.target_tool {
        return Err(HarnessError::Tool(format!(
            "target call was not executed by {}",
            compensation.target_tool
        )));
    }
    let authorization_index =
        execution_authorization_index(turn, &request.target_call_id, name, call_index)?
            .ok_or_else(|| {
                HarnessError::Tool(
                    "target Tool call has no authoritative execution authorization".to_owned(),
                )
            })?;
    let Some((result_index, output, is_error)) = unique_tool_result(turn, &request.target_call_id)?
    else {
        return Err(HarnessError::Tool(
            "target Tool call has no authoritative result".to_owned(),
        ));
    };
    if result_index <= authorization_index || is_error {
        return Err(HarnessError::Tool(
            "only a successful Tool result ordered after authorization can be compensated"
                .to_owned(),
        ));
    }
    Ok((input.clone(), output.clone()))
}

fn prior_successful_compensation(
    thread: &Thread,
    context: &ToolContext,
    compensation_tool: &str,
    request: &CompensationRequest,
) -> Result<Option<Value>, HarnessError> {
    let mut settled = None;
    for turn in &thread.turns {
        for (call_index, item) in turn.items.iter().enumerate() {
            let ItemKind::ToolCall {
                call_id,
                name,
                input,
                ..
            } = &item.kind
            else {
                continue;
            };
            if name != compensation_tool
                || (turn.id == context.turn_id && call_id == &context.call_id)
            {
                continue;
            }
            let Ok(prior) = serde_json::from_value::<CompensationRequest>(input.clone()) else {
                continue;
            };
            if prior.target_turn_id != request.target_turn_id
                || prior.target_call_id != request.target_call_id
            {
                continue;
            }
            let result = unique_tool_result(turn, call_id)?;
            let authorization_index =
                execution_authorization_index(turn, call_id, name, call_index)?;
            let Some(authorization_index) = authorization_index else {
                if result.is_some() {
                    return Err(HarnessError::Tool(
                        "unauthorized compensation call has a result in State".to_owned(),
                    ));
                }
                continue;
            };
            if prior.idempotency_key != request.idempotency_key {
                return Err(HarnessError::Tool(
                    "compensation target already used a different idempotency key".to_owned(),
                ));
            }
            if let Some((result_index, output, is_error)) = result {
                if result_index <= authorization_index {
                    return Err(HarnessError::Tool(
                        "compensation result precedes its authorization in authoritative State"
                            .to_owned(),
                    ));
                }
                if !is_error {
                    settled = Some(output.clone());
                }
            }
        }
    }
    Ok(settled)
}

fn execution_authorization_index(
    turn: &Turn,
    call_id: &str,
    tool: &str,
    call_index: usize,
) -> Result<Option<usize>, HarnessError> {
    let mut policies = turn.items.iter().enumerate().filter_map(|(index, item)| {
        if let ItemKind::PolicyDecision {
            call_id: candidate,
            decision,
            ..
        } = &item.kind
            && candidate == call_id
        {
            Some((index, decision))
        } else {
            None
        }
    });
    let Some((policy_index, policy)) = policies.next() else {
        return Ok(None);
    };
    if policies.next().is_some() || policy_index <= call_index {
        return Err(HarnessError::Tool(
            "Tool authorization evidence is ambiguous or out of order".to_owned(),
        ));
    }
    match policy {
        PolicyDecision::Allow => Ok(Some(policy_index)),
        PolicyDecision::Deny { .. } => Ok(None),
        PolicyDecision::Ask { reason, risk } => {
            let mut requests = turn.items.iter().enumerate().filter_map(|(index, item)| {
                if let ItemKind::ApprovalRequested {
                    approval_id,
                    call_id: candidate,
                    tool,
                    reason,
                    risk,
                    ..
                } = &item.kind
                    && candidate == call_id
                {
                    Some((index, approval_id, tool, reason, risk))
                } else {
                    None
                }
            });
            let Some((
                request_index,
                approval_id,
                requested_tool,
                requested_reason,
                requested_risk,
            )) = requests.next()
            else {
                return Ok(None);
            };
            if requests.next().is_some() || request_index <= policy_index {
                return Err(HarnessError::Tool(
                    "Tool approval request evidence is ambiguous or out of order".to_owned(),
                ));
            }
            if requested_tool != tool || requested_reason != reason || requested_risk != risk {
                return Err(HarnessError::Tool(
                    "Tool approval request differs from its Policy decision".to_owned(),
                ));
            }
            let mut decisions = turn.items.iter().enumerate().filter_map(|(index, item)| {
                if let ItemKind::ApprovalDecision {
                    approval_id: candidate_approval,
                    call_id: candidate_call,
                    decision,
                } = &item.kind
                    && candidate_approval == approval_id
                    && candidate_call == call_id
                {
                    Some((index, decision))
                } else {
                    None
                }
            });
            let Some((decision_index, decision)) = decisions.next() else {
                return Ok(None);
            };
            if decisions.next().is_some() || decision_index <= request_index {
                return Err(HarnessError::Tool(
                    "Tool approval settlement evidence is ambiguous or out of order".to_owned(),
                ));
            }
            Ok(matches!(decision, ApprovalDecision::Approve).then_some(decision_index))
        }
    }
}

fn find_turn<'a>(
    thread: &'a Thread,
    turn_id: &TurnId,
    kind: &str,
) -> Result<&'a Turn, HarnessError> {
    let mut matches = thread.turns.iter().filter(|turn| &turn.id == turn_id);
    let turn = matches
        .next()
        .ok_or_else(|| HarnessError::Tool(format!("{kind} Turn is absent from State")))?;
    if matches.next().is_some() {
        return Err(HarnessError::Tool(format!(
            "{kind} Turn is duplicated in State"
        )));
    }
    Ok(turn)
}

fn unique_tool_call<'a>(
    turn: &'a Turn,
    call_id: &str,
) -> Result<(usize, &'a str, &'a Value), HarnessError> {
    let mut matches = turn.items.iter().enumerate().filter_map(|(index, item)| {
        if let ItemKind::ToolCall {
            call_id: candidate,
            name,
            input,
            ..
        } = &item.kind
            && candidate == call_id
        {
            Some((index, name.as_str(), input))
        } else {
            None
        }
    });
    let call = matches
        .next()
        .ok_or_else(|| HarnessError::Tool("Tool call is absent from State".to_owned()))?;
    if matches.next().is_some() {
        return Err(HarnessError::Tool(
            "Tool call identity is ambiguous in State".to_owned(),
        ));
    }
    Ok(call)
}

fn unique_tool_result<'a>(
    turn: &'a Turn,
    call_id: &str,
) -> Result<Option<(usize, &'a Value, bool)>, HarnessError> {
    let mut matches = turn.items.iter().enumerate().filter_map(|(index, item)| {
        if let ItemKind::ToolResult {
            call_id: candidate,
            output,
            is_error,
        } = &item.kind
            && candidate == call_id
        {
            Some((index, output, *is_error))
        } else {
            None
        }
    });
    let result = matches.next();
    if matches.next().is_some() {
        return Err(HarnessError::Tool(
            "Tool result identity is ambiguous in State".to_owned(),
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use serde_json::{Value, json};

    use super::{
        CompensationContext, CompensationDescriptor, CompensationRequest, CompensationTool,
        ToolCompensator,
    };
    use crate::{
        AllowListPolicy, ApprovalDecision, ApprovalId, CancellationToken, CapabilityOrigin,
        HarnessFuture, HarnessRuntime, Item, ItemKind, LanguageModel, MemoryEventStore,
        ModelOutput, ModelRequest, RiskLevel, StateEngine, Tool, ToolContext, ToolRegistry, Turn,
        TurnStatus,
    };

    struct RecordingCompensator {
        calls: AtomicUsize,
        contexts: Mutex<Vec<CompensationContext>>,
    }

    struct CompensationModel {
        target_turn_id: crate::TurnId,
    }

    impl LanguageModel for CompensationModel {
        fn id(&self) -> &str {
            "test/compensation-model"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if request.items.iter().any(|item| {
                    matches!(
                        &item.kind,
                        ItemKind::ToolResult { call_id, .. }
                            if call_id == "refund-request"
                    )
                }) {
                    return Ok(ModelOutput::Message {
                        content: "compensated".to_owned(),
                    });
                }
                Ok(ModelOutput::ToolCall {
                    call_id: "refund-request".to_owned(),
                    name: "refund".to_owned(),
                    input: serde_json::to_value(CompensationRequest {
                        target_turn_id: self.target_turn_id.clone(),
                        target_call_id: "charge-1".to_owned(),
                        idempotency_key: "refund-charge-1".to_owned(),
                    })
                    .map_err(|_| {
                        crate::HarnessError::Model("request encoding failed".to_owned())
                    })?,
                })
            })
        }
    }

    impl ToolCompensator for RecordingCompensator {
        fn descriptor(&self) -> CompensationDescriptor {
            CompensationDescriptor {
                name: "refund".to_owned(),
                description: "Refunds one successfully recorded charge".to_owned(),
                target_tool: "charge".to_owned(),
            }
        }

        fn compensate<'a>(&'a self, context: CompensationContext) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.contexts
                    .lock()
                    .map_err(|_| crate::HarnessError::Tool("context lock poisoned".to_owned()))?
                    .push(context);
                Ok(json!({"refunded": true}))
            })
        }
    }

    async fn append_call(
        state: &StateEngine,
        turn: &Turn,
        name: &str,
        call_id: &str,
        input: Value,
    ) {
        state
            .append_item(
                turn,
                Item::new(ItemKind::ToolCall {
                    model_id: None,
                    model_origin: None,
                    call_id: call_id.to_owned(),
                    name: name.to_owned(),
                    input,
                    batch: None,
                }),
            )
            .await
            .expect("append Tool call");
    }

    async fn append_result(
        state: &StateEngine,
        turn: &Turn,
        call_id: &str,
        output: Value,
        is_error: bool,
    ) {
        state
            .append_item(
                turn,
                Item::new(ItemKind::ToolResult {
                    call_id: call_id.to_owned(),
                    output,
                    is_error,
                }),
            )
            .await
            .expect("append Tool result");
    }

    async fn append_allow(state: &StateEngine, turn: &Turn, call_id: &str) {
        state
            .append_item(
                turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: call_id.to_owned(),
                    tool_origin: Some(crate::CapabilityOrigin::BuiltIn),
                    decision: crate::PolicyDecision::Allow,
                }),
            )
            .await
            .expect("append Policy allow");
    }

    async fn append_approved_ask(state: &StateEngine, turn: &Turn, call_id: &str, tool: &str) {
        let approval_id = ApprovalId::generate();
        state
            .append_item(
                turn,
                Item::new(ItemKind::PolicyDecision {
                    call_id: call_id.to_owned(),
                    tool_origin: Some(crate::CapabilityOrigin::BuiltIn),
                    decision: crate::PolicyDecision::Ask {
                        reason: "operator approval required".to_owned(),
                        risk: RiskLevel::High,
                    },
                }),
            )
            .await
            .expect("append Policy ask");
        state
            .append_item(
                turn,
                Item::new(ItemKind::ApprovalRequested {
                    approval_id: approval_id.clone(),
                    call_id: call_id.to_owned(),
                    tool: tool.to_owned(),
                    reason: "operator approval required".to_owned(),
                    risk: RiskLevel::High,
                    requested_by: Some(crate::ApprovalActor::LocalProcess),
                    tool_origin: Some(crate::CapabilityOrigin::BuiltIn),
                    model_request_sha256: Some("0".repeat(64)),
                }),
            )
            .await
            .expect("append approval request");
        state
            .append_item(
                turn,
                Item::new(ItemKind::ApprovalDecision {
                    approval_id,
                    call_id: call_id.to_owned(),
                    decision: ApprovalDecision::Approve,
                }),
            )
            .await
            .expect("append approval decision");
    }

    async fn finish(state: &StateEngine, turn: &mut Turn) {
        state
            .finish_turn(turn, TurnStatus::Completed)
            .await
            .expect("finish Turn");
        turn.status = TurnStatus::Completed;
    }

    #[tokio::test]
    async fn compensation_uses_authoritative_effect_and_stable_idempotency() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("thread");
        let mut target_turn = state.start_turn(&thread.id).await.expect("target Turn");
        append_call(
            &state,
            &target_turn,
            "charge",
            "charge-1",
            json!({"amount": 10}),
        )
        .await;
        append_allow(&state, &target_turn, "charge-1").await;
        append_result(
            &state,
            &target_turn,
            "charge-1",
            json!({"charge_id": "ch-1"}),
            false,
        )
        .await;
        finish(&state, &mut target_turn).await;

        let compensator = Arc::new(RecordingCompensator {
            calls: AtomicUsize::new(0),
            contexts: Mutex::new(Vec::new()),
        });
        let tool = Arc::new(
            CompensationTool::new(state.clone(), compensator.clone()).expect("compensation Tool"),
        );
        let mut registry = ToolRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, tool.clone())
            .expect("ordinary Tool registration");

        let request = CompensationRequest {
            target_turn_id: target_turn.id.clone(),
            target_call_id: "charge-1".to_owned(),
            idempotency_key: "refund-charge-1".to_owned(),
        };
        let input = serde_json::to_value(&request).expect("request JSON");
        let mut first_turn = state
            .start_turn(&thread.id)
            .await
            .expect("first refund Turn");
        append_call(&state, &first_turn, "refund", "refund-1", input.clone()).await;
        append_approved_ask(&state, &first_turn, "refund-1", "refund").await;
        let output = tool
            .execute(
                input.clone(),
                ToolContext {
                    thread_id: thread.id.clone(),
                    turn_id: first_turn.id.clone(),
                    call_id: "refund-1".to_owned(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect("first compensation");
        assert_eq!(output, json!({"refunded": true}));
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
        {
            let contexts = compensator.contexts.lock().expect("contexts");
            assert_eq!(contexts[0].original_input, json!({"amount": 10}));
            assert_eq!(contexts[0].original_output, json!({"charge_id": "ch-1"}));
        }
        append_result(&state, &first_turn, "refund-1", output.clone(), false).await;
        finish(&state, &mut first_turn).await;

        let mut retry_turn = state.start_turn(&thread.id).await.expect("retry Turn");
        append_call(&state, &retry_turn, "refund", "refund-2", input.clone()).await;
        append_allow(&state, &retry_turn, "refund-2").await;
        let cached = tool
            .execute(
                input,
                ToolContext {
                    thread_id: thread.id.clone(),
                    turn_id: retry_turn.id.clone(),
                    call_id: "refund-2".to_owned(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .expect("idempotent compensation");
        assert_eq!(cached, output);
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
        append_result(&state, &retry_turn, "refund-2", cached, false).await;
        finish(&state, &mut retry_turn).await;

        let malformed_approval_turn = state
            .start_turn(&thread.id)
            .await
            .expect("malformed approval Turn");
        append_call(
            &state,
            &malformed_approval_turn,
            "refund",
            "refund-malformed-approval",
            serde_json::to_value(&request).expect("request JSON"),
        )
        .await;
        append_approved_ask(
            &state,
            &malformed_approval_turn,
            "refund-malformed-approval",
            "another-tool",
        )
        .await;
        assert!(
            tool.execute(
                serde_json::to_value(&request).expect("request JSON"),
                ToolContext {
                    thread_id: thread.id.clone(),
                    turn_id: malformed_approval_turn.id.clone(),
                    call_id: "refund-malformed-approval".to_owned(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .is_err()
        );
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
        state
            .finish_turn(&malformed_approval_turn, TurnStatus::Failed)
            .await
            .expect("finish malformed approval Turn");

        let mut misordered_target = state
            .start_turn(&thread.id)
            .await
            .expect("misordered target Turn");
        append_call(
            &state,
            &misordered_target,
            "charge",
            "charge-2",
            json!({"amount": 20}),
        )
        .await;
        append_result(
            &state,
            &misordered_target,
            "charge-2",
            json!({"charge_id": "ch-2"}),
            false,
        )
        .await;
        append_allow(&state, &misordered_target, "charge-2").await;
        finish(&state, &mut misordered_target).await;
        let misordered_request = CompensationRequest {
            target_turn_id: misordered_target.id,
            target_call_id: "charge-2".to_owned(),
            idempotency_key: "refund-charge-2".to_owned(),
        };
        let misordered_input =
            serde_json::to_value(&misordered_request).expect("misordered request JSON");
        let misordered_turn = state
            .start_turn(&thread.id)
            .await
            .expect("misordered compensation Turn");
        append_call(
            &state,
            &misordered_turn,
            "refund",
            "refund-misordered",
            misordered_input.clone(),
        )
        .await;
        append_allow(&state, &misordered_turn, "refund-misordered").await;
        assert!(
            tool.execute(
                misordered_input,
                ToolContext {
                    thread_id: thread.id.clone(),
                    turn_id: misordered_turn.id.clone(),
                    call_id: "refund-misordered".to_owned(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .is_err()
        );
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
        state
            .finish_turn(&misordered_turn, TurnStatus::Failed)
            .await
            .expect("finish misordered compensation Turn");

        let conflicting = CompensationRequest {
            idempotency_key: "different-key".to_owned(),
            ..request
        };
        let conflicting_input = serde_json::to_value(conflicting).expect("conflicting JSON");
        let conflict_turn = state.start_turn(&thread.id).await.expect("conflict Turn");
        append_call(
            &state,
            &conflict_turn,
            "refund",
            "refund-3",
            conflicting_input.clone(),
        )
        .await;
        append_allow(&state, &conflict_turn, "refund-3").await;
        assert!(
            tool.execute(
                conflicting_input,
                ToolContext {
                    thread_id: thread.id,
                    turn_id: conflict_turn.id,
                    call_id: "refund-3".to_owned(),
                    cancellation: CancellationToken::new(),
                },
            )
            .await
            .is_err()
        );
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn compensation_follows_ordinary_policy_before_provider_execution() {
        let state = StateEngine::new(Arc::new(MemoryEventStore::new()));
        let thread = state.create_thread().await.expect("thread");
        let mut target_turn = state.start_turn(&thread.id).await.expect("target Turn");
        append_call(
            &state,
            &target_turn,
            "charge",
            "charge-1",
            json!({"amount": 10}),
        )
        .await;
        append_allow(&state, &target_turn, "charge-1").await;
        append_result(
            &state,
            &target_turn,
            "charge-1",
            json!({"charge_id": "ch-1"}),
            false,
        )
        .await;
        finish(&state, &mut target_turn).await;

        let compensator = Arc::new(RecordingCompensator {
            calls: AtomicUsize::new(0),
            contexts: Mutex::new(Vec::new()),
        });
        let model = Arc::new(CompensationModel {
            target_turn_id: target_turn.id.clone(),
        });
        let mut denied_tools = ToolRegistry::new();
        denied_tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(
                    CompensationTool::new(state.clone(), compensator.clone())
                        .expect("compensation Tool"),
                ),
            )
            .expect("register denied Tool");
        let denied = HarnessRuntime::new(
            model.clone(),
            denied_tools,
            Arc::new(AllowListPolicy::deny_by_default()),
            state.clone(),
        );
        assert!(matches!(
            denied.run_turn(&thread.id, "refund").await,
            Err(crate::HarnessError::PolicyDenied { .. })
        ));
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 0);

        let mut allowed_tools = ToolRegistry::new();
        allowed_tools
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(
                    CompensationTool::new(state.clone(), compensator.clone())
                        .expect("compensation Tool"),
                ),
            )
            .expect("register allowed Tool");
        let allowed = HarnessRuntime::new(
            model,
            allowed_tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("refund")),
            state,
        );
        let outcome = allowed
            .run_turn(&thread.id, "refund")
            .await
            .expect("authorized compensation");
        assert_eq!(outcome.final_text, "compensated");
        assert_eq!(compensator.calls.load(Ordering::Relaxed), 1);
    }
}
