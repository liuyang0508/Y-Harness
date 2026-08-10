//! Synthetic POC connectors with Y-Harness evidence claims.

use serde::Deserialize;
use serde_json::{Value, json};
use y_harness::{
    ConnectorEvidenceClaim, HarnessError, HarnessFuture, Tool, ToolContext, ToolDescriptor,
    ToolExecutionResult,
};

use crate::contracts::TimeWindow;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryInput {
    tenant_id: String,
    pond_id: String,
    time_window: TimeWindow,
}

/// Synthetic IoT connector for the first diagnostic story line.
pub struct MockIotQueryTool;

impl Tool for MockIotQueryTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "aquaculture.iot.query".to_owned(),
            description: "Reads synthetic pond sensor series for a tenant-fenced POC. Output is never production evidence.".to_owned(),
            input_schema: query_schema(),
        }
    }

    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            self.execute_result(input, context)
                .map(|result| result.output().clone())
        })
    }

    fn execute_with_evidence<'a>(
        &'a self,
        input: Value,
        context: ToolContext,
    ) -> HarnessFuture<'a, ToolExecutionResult> {
        Box::pin(async move { self.execute_result(input, context) })
    }
}

impl MockIotQueryTool {
    fn execute_result(
        &self,
        input: Value,
        context: ToolContext,
    ) -> Result<ToolExecutionResult, HarnessError> {
        let query = parse_and_authorize(input, &context)?;
        let span = query.time_window.end_ms - query.time_window.start_ms;
        let quarter = span / 4;
        let readings = [
            (query.time_window.start_ms, 6.8, 7.7, 13.2),
            (query.time_window.start_ms + quarter, 5.9, 7.8, 13.6),
            (query.time_window.start_ms + quarter * 2, 4.4, 7.9, 14.0),
            (query.time_window.end_ms - 1, 3.9, 8.0, 14.2),
        ]
        .into_iter()
        .map(
            |(timestamp_ms, dissolved_oxygen_mg_l, ph, water_temperature_c)| {
                json!({
                    "timestamp_ms": timestamp_ms,
                    "dissolved_oxygen_mg_l": dissolved_oxygen_mg_l,
                    "ph": ph,
                    "water_temperature_c": water_temperature_c
                })
            },
        )
        .collect::<Vec<_>>();
        let output = json!({
            "schema_version": "aquaculture.mock-iot/v1",
            "data_origin": "synthetic",
            "tenant_id": query.tenant_id,
            "pond_id": query.pond_id,
            "readings": readings,
            "disclaimer": "模拟 IoT 数据，仅用于 POC 验证。"
        });
        evidence_result(output, "mock-iot", &query, query.time_window.end_ms)
    }
}

/// Synthetic ERP connector for feed, mortality, and biomass observations.
pub struct MockErpQueryTool;

impl Tool for MockErpQueryTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "aquaculture.erp.query".to_owned(),
            description:
                "Reads synthetic feed, mortality, and biomass records for a tenant-fenced POC."
                    .to_owned(),
            input_schema: query_schema(),
        }
    }

    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            self.execute_result(input, context)
                .map(|result| result.output().clone())
        })
    }

    fn execute_with_evidence<'a>(
        &'a self,
        input: Value,
        context: ToolContext,
    ) -> HarnessFuture<'a, ToolExecutionResult> {
        Box::pin(async move { self.execute_result(input, context) })
    }
}

impl MockErpQueryTool {
    fn execute_result(
        &self,
        input: Value,
        context: ToolContext,
    ) -> Result<ToolExecutionResult, HarnessError> {
        let query = parse_and_authorize(input, &context)?;
        let output = json!({
            "schema_version": "aquaculture.mock-erp/v1",
            "data_origin": "synthetic",
            "tenant_id": query.tenant_id,
            "pond_id": query.pond_id,
            "summary": {
                "feed_kg": 300.0,
                "mortality_count": 37,
                "estimated_biomass_kg": 5125.0,
                "last_feeding_at_ms": query.time_window.end_ms.saturating_sub(21_600_000)
            },
            "disclaimer": "模拟 ERP 数据，仅用于 POC 验证。"
        });
        evidence_result(output, "mock-erp", &query, query.time_window.end_ms)
    }
}

fn parse_and_authorize(input: Value, context: &ToolContext) -> Result<QueryInput, HarnessError> {
    if context.cancellation.is_cancelled() {
        return Err(HarnessError::Cancelled {
            phase: y_harness::ExecutionPhase::Tool,
        });
    }
    let query: QueryInput = serde_json::from_value(input)
        .map_err(|error| HarnessError::Tool(format!("invalid aquaculture query: {error}")))?;
    query.time_window.validate().map_err(HarnessError::Tool)?;
    if context.authority.tenant_id() != Some(query.tenant_id.as_str()) {
        return Err(HarnessError::Tool(
            "tool tenant_id does not match trusted authority".to_owned(),
        ));
    }
    if query.pond_id.trim().is_empty() {
        return Err(HarnessError::Tool("pond_id must not be empty".to_owned()));
    }
    Ok(query)
}

fn evidence_result(
    output: Value,
    source: &str,
    query: &QueryInput,
    observed_at_ms: u64,
) -> Result<ToolExecutionResult, HarnessError> {
    let resource = format!(
        "tenant/{}/pond/{}/range/{}-{}",
        query.tenant_id, query.pond_id, query.time_window.start_ms, query.time_window.end_ms
    );
    let claim = ConnectorEvidenceClaim::new(
        source,
        resource.clone(),
        "synthetic-v1",
        observed_at_ms,
        None,
        Some(resource),
    )?;
    ToolExecutionResult::with_connector_evidence(output, vec![claim])
}

fn query_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["tenant_id", "pond_id", "time_window"],
        "properties": {
            "tenant_id": {"type": "string", "minLength": 1},
            "pond_id": {"type": "string", "minLength": 1},
            "time_window": {
                "type": "object",
                "additionalProperties": false,
                "required": ["start_ms", "end_ms", "timezone"],
                "properties": {
                    "start_ms": {"type": "integer", "minimum": 0},
                    "end_ms": {"type": "integer", "minimum": 1},
                    "timezone": {"type": "string", "minLength": 1}
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use y_harness::{ActorIdentity, AuthorityContext, CancellationToken, ThreadId, TurnId};

    fn context(tenant: &str) -> ToolContext {
        ToolContext {
            thread_id: ThreadId::from_static("thread-test"),
            turn_id: TurnId::from_static("turn-test"),
            call_id: "call-test".to_owned(),
            authority: AuthorityContext::new(
                ActorIdentity::Authenticated {
                    authority: "test".to_owned(),
                    subject: "user-a".to_owned(),
                },
                Some(tenant.to_owned()),
            )
            .expect("authority"),
            cancellation: CancellationToken::new(),
        }
    }

    fn input(tenant: &str) -> Value {
        json!({
            "tenant_id": tenant,
            "pond_id": "pond-3",
            "time_window": {
                "start_ms": 1_000,
                "end_ms": 2_000,
                "timezone": "Asia/Shanghai"
            }
        })
    }

    #[tokio::test]
    async fn connector_marks_data_as_synthetic_and_emits_evidence() {
        let result = MockIotQueryTool
            .execute_with_evidence(input("tenant-a"), context("tenant-a"))
            .await
            .expect("tool result");
        assert_eq!(result.output()["data_origin"], "synthetic");
        assert_eq!(result.connector_evidence().len(), 1);
    }

    #[tokio::test]
    async fn connector_rejects_cross_tenant_input() {
        let error = MockErpQueryTool
            .execute(input("tenant-b"), context("tenant-a"))
            .await
            .expect_err("tenant mismatch");
        assert!(error.to_string().contains("trusted authority"));
    }
}
