//! Deterministic end-to-end replay through the real Y-Harness Agent Loop.

use std::{collections::BTreeSet, error::Error, sync::Arc};

use serde_json::{Value, json};
use y_harness::{
    ActorIdentity, AllowListPolicy, AuthorityContext, CapabilityOrigin, HarnessError,
    HarnessFuture, HarnessRuntime, ItemKind, LanguageModel, MemoryEventStore, MemoryScope,
    ModelOutput, ModelRegistry, ModelRequest, StateEngine, ToolRegistry, TurnContextInput,
    TurnExecutionOptions, VerificationRegistry,
};
use y_harness_aquaculture::{
    AgentRequest, ContextPackageBuilder, DataOrigin, InteractionContext, JourneyId, TimeWindow,
    register_poc_capabilities,
    verification::{AnswerClaim, AnswerEvidence, AquacultureAnswerEnvelope},
};

struct PocReplayModel {
    tenant_id: String,
    pond_id: String,
    time_window: TimeWindow,
}

impl LanguageModel for PocReplayModel {
    fn id(&self) -> &str {
        "aquaculture/poc-replay"
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move {
            let tool_results = request
                .items
                .iter()
                .filter_map(|item| match &item.kind {
                    ItemKind::ToolResult {
                        call_id,
                        output,
                        is_error: false,
                        ..
                    } => Some((call_id.as_str(), output)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if tool_results.is_empty() {
                return Ok(ModelOutput::ToolCall {
                    call_id: "iot-call".to_owned(),
                    name: "aquaculture.iot.query".to_owned(),
                    input: self.query_input(),
                });
            }
            if tool_results.len() == 1 {
                return Ok(ModelOutput::ToolCall {
                    call_id: "erp-call".to_owned(),
                    name: "aquaculture.erp.query".to_owned(),
                    input: self.query_input(),
                });
            }
            let envelope = AquacultureAnswerEnvelope {
                schema_version: "aquaculture.answer/v1".to_owned(),
                journey_id: JourneyId::AqJr001,
                pond_ids: vec![self.pond_id.clone()],
                answer: "模拟数据中溶氧由 6.8 mg/L 下降至 3.9 mg/L，同时出现死亡记录。建议先核验传感器与现场水样，再由值班人员确认是否启动增氧；当前证据不足以把相关性表述为唯一因果。".to_owned(),
                claims: vec![
                    AnswerClaim {
                        statement: "分析窗内溶氧呈持续下降趋势".to_owned(),
                        evidence_ids: vec!["ev-iot".to_owned()],
                        confidence: 0.92,
                    },
                    AnswerClaim {
                        statement: "同一分析窗存在死亡记录，但尚不能证明由溶氧单独导致".to_owned(),
                        evidence_ids: vec!["ev-iot".to_owned(), "ev-erp".to_owned()],
                        confidence: 0.72,
                    },
                ],
                evidence: vec![
                    AnswerEvidence {
                        id: "ev-iot".to_owned(),
                        source: "mock-iot".to_owned(),
                        data_origin: DataOrigin::Synthetic,
                        locator: Some(format!("pond/{}", self.pond_id)),
                    },
                    AnswerEvidence {
                        id: "ev-erp".to_owned(),
                        source: "mock-erp".to_owned(),
                        data_origin: DataOrigin::Synthetic,
                        locator: Some(format!("pond/{}", self.pond_id)),
                    },
                ],
                uncertainty: vec![
                    "缺少客户真实连续传感器数据".to_owned(),
                    "缺少现场水样、设备状态和处置后效果数据".to_owned(),
                ],
                confirmation_required: false,
                synthetic_disclaimer: Some(
                    "当前 IoT/ERP 数据为模拟数据，仅用于 POC 验证，不代表真实生产事实。"
                        .to_owned(),
                ),
            };
            let content = serde_json::to_string_pretty(&envelope)
                .map_err(|error| HarnessError::Model(error.to_string()))?;
            Ok(ModelOutput::Message { content })
        })
    }
}

impl PocReplayModel {
    fn query_input(&self) -> Value {
        json!({
            "tenant_id": self.tenant_id,
            "pond_id": self.pond_id,
            "time_window": self.time_window
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tenant_id = "tenant-fixture".to_owned();
    let pond_id = "pond-3".to_owned();
    let time_window = TimeWindow {
        start_ms: 1_722_182_400_000,
        end_ms: 1_722_441_600_000,
        timezone: "Asia/Shanghai".to_owned(),
    };
    let interaction = InteractionContext {
        tenant_id: tenant_id.clone(),
        user_id: "operator-fixture".to_owned(),
        active_site_id: Some("site-fixture".to_owned()),
        active_pond_id: None,
        authorized_pond_ids: BTreeSet::from([pond_id.clone()]),
        timezone: "Asia/Shanghai".to_owned(),
    };
    let package = ContextPackageBuilder::new(DataOrigin::Synthetic).build(&AgentRequest {
        query: "请诊断 3 号塘最近三天溶氧下降的原因".to_owned(),
        interaction: interaction.clone(),
        explicit_pond_id: Some(pond_id.clone()),
        time_window: Some(time_window.clone()),
    })?;

    let mut tools = ToolRegistry::new();
    let mut verification = VerificationRegistry::new();
    register_poc_capabilities(&mut tools, &mut verification)?;
    let mut models = ModelRegistry::new();
    models.register(
        CapabilityOrigin::TrustedExtension {
            id: "aquaculture-poc-replay".to_owned(),
        },
        Arc::new(PocReplayModel {
            tenant_id: tenant_id.clone(),
            pond_id,
            time_window,
        }),
    )?;
    let runtime = HarnessRuntime::from_model_registry(
        &models,
        "aquaculture/poc-replay",
        tools,
        Arc::new(
            AllowListPolicy::deny_by_default()
                .allow("aquaculture.iot.query")
                .allow("aquaculture.erp.query"),
        ),
        StateEngine::new(Arc::new(MemoryEventStore::new())),
    )?
    .with_verification(verification);
    let authority = AuthorityContext::new(
        ActorIdentity::Authenticated {
            authority: "aquaculture-poc".to_owned(),
            subject: interaction.user_id.clone(),
        },
        Some(tenant_id.clone()),
    )?;
    let thread = runtime.create_thread_as(&authority).await?;
    let outcome = runtime
        .run_turn_with_options(
            &thread.id,
            "请诊断 3 号塘最近三天溶氧下降的原因",
            TurnExecutionOptions {
                authority,
                memory_scope: MemoryScope {
                    project: Some("aquaculture-poc".to_owned()),
                    tenant_id: Some(tenant_id),
                    tags: vec!["synthetic".to_owned()],
                },
                context: vec![TurnContextInput {
                    source: "aquaculture-context-package".to_owned(),
                    reference: package.schema_version.clone(),
                    text: serde_json::to_string(&package)?,
                }],
                ..TurnExecutionOptions::default()
            },
        )
        .await?;

    println!("{}", outcome.final_text);
    eprintln!(
        "thread={} journey={:?}",
        thread.id, package.journey.selected
    );
    Ok(())
}
