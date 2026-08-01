//! Host-driven execution of one durable external Effect.
//!
//! The Connector is deliberately local and deterministic. Real hosts register
//! their own target-idempotent implementation and call `run_once_as` from an
//! independently governed service lifecycle.

use std::{collections::BTreeSet, sync::Arc};

use y_harness::{
    AllowListEffectExecutionPolicy, AuthorityContext, CancellationToken, CapabilityOrigin,
    EFFECT_EXECUTOR_API_VERSION, EffectConnector, EffectConnectorDescriptor,
    EffectConnectorRegistry, EffectCreateRequest, EffectDispatchGovernorPolicy, EffectEngine,
    EffectExecutionOutcome, EffectExecutionRequest, EffectExecutor, EffectExecutorClock,
    EffectExecutorRunRequest, EffectId, EffectIdempotencyContract, EffectOperation, EffectReceipt,
    HarnessError, HarnessFuture, MemoryEffectCoordinator, MemoryEffectDispatchGovernor,
};

const NOW_MS: u64 = 1_000;

struct ExampleClock;

impl EffectExecutorClock for ExampleClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        Ok(NOW_MS)
    }
}

struct IdempotentNotificationConnector;

impl EffectConnector for IdempotentNotificationConnector {
    fn descriptor(&self) -> EffectConnectorDescriptor {
        EffectConnectorDescriptor {
            capability: "notification.example".to_owned(),
            api_version: EFFECT_EXECUTOR_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            idempotency: EffectIdempotencyContract::TargetEnforced,
        }
    }

    fn execute<'a>(
        &'a self,
        request: EffectExecutionRequest,
    ) -> HarnessFuture<'a, EffectExecutionOutcome> {
        Box::pin(async move {
            // A real Connector supplies `request.idempotency_key` to the target
            // system and returns only authoritative, content-free evidence.
            Ok(EffectExecutionOutcome::Applied {
                receipt: EffectReceipt {
                    source: "notification.example".to_owned(),
                    external_id: request.effect_id.to_string(),
                    observed_at_ms: NOW_MS,
                    response_sha256: "a".repeat(64),
                },
            })
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), HarnessError> {
    let authority = AuthorityContext::local_process();
    let engine = EffectEngine::new(Arc::new(MemoryEffectCoordinator::new()));
    engine
        .create_as(
            EffectId::from_static("effect-example"),
            EffectCreateRequest {
                command_id: y_harness::EffectCommandId::from_static("create-effect-example"),
                operation: EffectOperation {
                    capability: "notification.example".to_owned(),
                    operation: "send".to_owned(),
                },
                idempotency_key: "notification-example-42".to_owned(),
                input: serde_json::json!({"artifact_ref":"message-42"}),
                not_before_ms: NOW_MS,
            },
            NOW_MS,
            &authority,
        )
        .await?;

    let mut connectors = EffectConnectorRegistry::new();
    connectors.register(
        CapabilityOrigin::BuiltIn,
        Arc::new(IdempotentNotificationConnector),
    )?;
    let policy =
        AllowListEffectExecutionPolicy::deny_by_default().allow("notification.example", "send")?;
    let executor = EffectExecutor::new(engine, connectors)?
        .with_policy(Arc::new(policy))
        .with_clock(Arc::new(ExampleClock))
        .with_dispatch_governor(
            Arc::new(MemoryEffectDispatchGovernor::new()),
            EffectDispatchGovernorPolicy {
                policy_id: "notification-example-v1".to_owned(),
                max_dispatches_per_window: 100,
                window_ms: 60_000,
                failure_threshold: 5,
                open_duration_ms: 30_000,
                probe_lease_ms: 10_000,
                admission_retention_ms: 604_800_000,
            },
        )?;

    let report = executor
        .run_once_as(
            EffectExecutorRunRequest {
                cycle_id: "example-cycle-1".to_owned(),
                after: None,
            },
            &authority,
            CancellationToken::new(),
        )
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| HarnessError::Effect(error.to_string()))?
    );
    Ok(())
}
