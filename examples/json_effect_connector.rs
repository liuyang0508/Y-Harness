//! Shell-free JSON-command Effect execution and reconciliation.
//!
//! The example launches a second copy of itself as the external Connector so
//! the public adapter, Process Broker, Executor, Reconciler, and Ledger paths
//! are exercised without a platform-specific fixture.

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    io::{Read, Write},
    sync::Arc,
    time::Duration,
};

use y_harness::{
    AllowListEffectExecutionPolicy, AllowListEffectReconciliationPolicy, AuthorityContext,
    CancellationToken, CapabilityOrigin, EFFECT_EXECUTOR_API_VERSION,
    EFFECT_RECONCILER_API_VERSION, EffectConnectorDescriptor, EffectConnectorRegistry,
    EffectCreateRequest, EffectEngine, EffectExecutionOutcome, EffectExecutor, EffectExecutorClock,
    EffectExecutorRunRequest, EffectId, EffectIdempotencyContract, EffectOperation, EffectReceipt,
    EffectReconciler, EffectReconcilerClock, EffectReconcilerRunRequest,
    EffectReconciliationConnectorDescriptor, EffectReconciliationConnectorRegistry,
    EffectReconciliationContract, EffectReconciliationOutcome, HarnessError,
    JSON_COMMAND_MAX_INPUT_BYTES, JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
    JsonCommandEffectConnector, JsonCommandEffectReconciliationConnector,
    JsonEffectExecutionRequest, JsonEffectExecutionResponse, JsonEffectReconciliationRequest,
    JsonEffectReconciliationResponse, JsonProcessConfig, LocalProcessBroker,
    MemoryEffectCoordinator,
};

const NOW_MS: u64 = 1_000;

struct ExampleClock;

impl EffectExecutorClock for ExampleClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        Ok(NOW_MS)
    }
}

impl EffectReconcilerClock for ExampleClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        Ok(NOW_MS)
    }
}

#[tokio::main]
async fn main() -> Result<(), HarnessError> {
    if let Some(mode) = env::args().nth(1) {
        return connector_process(&mode);
    }

    let authority = AuthorityContext::local_process();
    let engine = EffectEngine::new(Arc::new(MemoryEffectCoordinator::new()));
    engine
        .create_as(
            EffectId::from_static("json-effect-example"),
            EffectCreateRequest {
                command_id: y_harness::EffectCommandId::from_static("create-json-effect-example"),
                operation: EffectOperation {
                    capability: "notification.command".to_owned(),
                    operation: "send".to_owned(),
                },
                idempotency_key: "notification-command-example-42".to_owned(),
                input: serde_json::json!({"artifact_ref":"message-42"}),
                not_before_ms: NOW_MS,
            },
            NOW_MS,
            &authority,
        )
        .await?;

    let process = process_config("--execute")?;
    let mut execution_connectors = EffectConnectorRegistry::new();
    execution_connectors.register(
        CapabilityOrigin::External {
            id: "example/json-effect-execution".to_owned(),
        },
        Arc::new(JsonCommandEffectConnector::new(
            EffectConnectorDescriptor {
                capability: "notification.command".to_owned(),
                api_version: EFFECT_EXECUTOR_API_VERSION,
                operations: BTreeSet::from(["send".to_owned()]),
                idempotency: EffectIdempotencyContract::TargetEnforced,
            },
            process,
            Arc::new(LocalProcessBroker::new(1)?),
        )?),
    )?;
    let execution_policy =
        AllowListEffectExecutionPolicy::deny_by_default().allow("notification.command", "send")?;
    let execution_report = EffectExecutor::new(engine.clone(), execution_connectors)?
        .with_policy(Arc::new(execution_policy))
        .with_clock(Arc::new(ExampleClock))
        .run_once_as(
            EffectExecutorRunRequest {
                cycle_id: "json-effect-execution-cycle".to_owned(),
                after: None,
            },
            &authority,
            CancellationToken::new(),
        )
        .await?;

    let process = process_config("--reconcile")?;
    let mut reconciliation_connectors = EffectReconciliationConnectorRegistry::new();
    reconciliation_connectors.register(
        CapabilityOrigin::External {
            id: "example/json-effect-reconciliation".to_owned(),
        },
        Arc::new(JsonCommandEffectReconciliationConnector::new(
            EffectReconciliationConnectorDescriptor {
                capability: "notification.command".to_owned(),
                api_version: EFFECT_RECONCILER_API_VERSION,
                operations: BTreeSet::from(["send".to_owned()]),
                contract: EffectReconciliationContract::AuthoritativeReadOnly,
            },
            process,
            Arc::new(LocalProcessBroker::new(1)?),
        )?),
    )?;
    let reconciliation_policy = AllowListEffectReconciliationPolicy::deny_by_default()
        .allow("notification.command", "send")?;
    let reconciliation_report = EffectReconciler::new(engine, reconciliation_connectors)?
        .with_policy(Arc::new(reconciliation_policy))
        .with_clock(Arc::new(ExampleClock))
        .run_once_as(
            EffectReconcilerRunRequest {
                cycle_id: "json-effect-reconciliation-cycle".to_owned(),
                after: None,
            },
            &authority,
            CancellationToken::new(),
        )
        .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "execution": execution_report,
            "reconciliation": reconciliation_report,
        }))
        .map_err(|error| HarnessError::Effect(error.to_string()))?
    );
    Ok(())
}

fn process_config(mode: &str) -> Result<JsonProcessConfig, HarnessError> {
    let program = env::current_exe()
        .map_err(|error| HarnessError::Effect(format!("cannot locate example: {error}")))?;
    let current_dir = env::current_dir().map_err(|error| {
        HarnessError::Effect(format!("cannot locate working directory: {error}"))
    })?;
    Ok(JsonProcessConfig {
        program,
        args: vec![mode.to_owned()],
        current_dir,
        environment: BTreeMap::new(),
        timeout: Duration::from_secs(5),
        max_output_bytes: 65_536,
    })
}

fn connector_process(mode: &str) -> Result<(), HarnessError> {
    let limit = u64::try_from(JSON_COMMAND_MAX_INPUT_BYTES)
        .map_err(|_| HarnessError::Effect("JSON input bound exceeds u64".to_owned()))?;
    let mut input = Vec::new();
    std::io::stdin()
        .take(limit.saturating_add(1))
        .read_to_end(&mut input)
        .map_err(|error| HarnessError::Effect(format!("cannot read Connector input: {error}")))?;
    if input.len() > JSON_COMMAND_MAX_INPUT_BYTES {
        return Err(HarnessError::Effect(
            "Connector input exceeds protocol bound".to_owned(),
        ));
    }
    let output = match mode {
        "--execute" => {
            let request: JsonEffectExecutionRequest = serde_json::from_slice(&input)
                .map_err(|_| HarnessError::Effect("invalid execution request".to_owned()))?;
            serde_json::to_vec(&JsonEffectExecutionResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectExecutionOutcome::Unknown {
                    reason_code: format!("{}.query_required", request.operation.capability),
                },
            })
        }
        "--reconcile" => {
            let request: JsonEffectReconciliationRequest = serde_json::from_slice(&input)
                .map_err(|_| HarnessError::Effect("invalid reconciliation request".to_owned()))?;
            serde_json::to_vec(&JsonEffectReconciliationResponse {
                protocol_version: JSON_EFFECT_CONNECTOR_PROTOCOL_VERSION,
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: EffectReceipt {
                        source: "notification.command".to_owned(),
                        external_id: request.effect_id.to_string(),
                        observed_at_ms: NOW_MS,
                        response_sha256: "a".repeat(64),
                    },
                },
            })
        }
        _ => {
            return Err(HarnessError::Effect(
                "unknown Connector example mode".to_owned(),
            ));
        }
    }
    .map_err(|_| HarnessError::Effect("cannot encode Connector output".to_owned()))?;
    std::io::stdout()
        .write_all(&output)
        .map_err(|error| HarnessError::Effect(format!("cannot write Connector output: {error}")))
}
