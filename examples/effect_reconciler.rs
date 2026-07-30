//! Host-driven reconciliation of one uncertain external Effect.
//!
//! The Connector is deliberately local and deterministic. Real hosts install
//! an authoritative read-only target lookup and invoke `run_once_as` from an
//! independently governed service lifecycle.

use std::{collections::BTreeSet, sync::Arc};

use y_harness::{
    AllowListEffectReconciliationPolicy, AuthorityContext, CancellationToken, CapabilityOrigin,
    EFFECT_RECONCILER_API_VERSION, EffectCommand, EffectCommandId, EffectCommandKind,
    EffectCreateRequest, EffectEngine, EffectId, EffectLeaseId, EffectOperation, EffectReceipt,
    EffectReconciler, EffectReconcilerClock, EffectReconcilerRunRequest,
    EffectReconciliationConnector, EffectReconciliationConnectorDescriptor,
    EffectReconciliationConnectorRegistry, EffectReconciliationContract,
    EffectReconciliationOutcome, EffectReconciliationRequest, HarnessError, HarnessFuture,
    MemoryEffectCoordinator,
};

const CREATED_AT_MS: u64 = 1_000;
const RECONCILED_AT_MS: u64 = 2_000;

struct ExampleClock;

impl EffectReconcilerClock for ExampleClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        Ok(RECONCILED_AT_MS)
    }
}

struct NotificationStatusConnector;

impl EffectReconciliationConnector for NotificationStatusConnector {
    fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
        EffectReconciliationConnectorDescriptor {
            capability: "notification.example".to_owned(),
            api_version: EFFECT_RECONCILER_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            contract: EffectReconciliationContract::AuthoritativeReadOnly,
        }
    }

    fn query<'a>(
        &'a self,
        request: EffectReconciliationRequest,
    ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
        Box::pin(async move {
            // A real Connector performs a side-effect-free lookup by the
            // stable target idempotency key and returns authoritative evidence.
            Ok(EffectReconciliationOutcome::Applied {
                receipt: EffectReceipt {
                    source: "notification.example".to_owned(),
                    external_id: request.effect_id.to_string(),
                    observed_at_ms: RECONCILED_AT_MS,
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
    let effect_id = EffectId::from_static("effect-reconciliation-example");
    let lease_id = EffectLeaseId::from_static("lease-reconciliation-example");
    engine
        .create_as(
            effect_id.clone(),
            EffectCreateRequest {
                command_id: EffectCommandId::from_static("create-reconciliation-example"),
                operation: EffectOperation {
                    capability: "notification.example".to_owned(),
                    operation: "send".to_owned(),
                },
                idempotency_key: "notification-example-42".to_owned(),
                input: serde_json::json!({"artifact_ref":"message-42"}),
                not_before_ms: CREATED_AT_MS,
            },
            CREATED_AT_MS,
            &authority,
        )
        .await?;
    engine
        .apply_as(
            &effect_id,
            1,
            EffectCommand {
                id: EffectCommandId::from_static("claim-reconciliation-example"),
                kind: EffectCommandKind::Claim {
                    lease_id: lease_id.clone(),
                    lease_duration_ms: 10_000,
                },
            },
            CREATED_AT_MS,
            &authority,
        )
        .await?;
    engine
        .apply_as(
            &effect_id,
            2,
            EffectCommand {
                id: EffectCommandId::from_static("unknown-reconciliation-example"),
                kind: EffectCommandKind::RecordUnknown {
                    lease_id,
                    reason_code: "connector.timeout".to_owned(),
                },
            },
            CREATED_AT_MS + 1,
            &authority,
        )
        .await?;

    let mut connectors = EffectReconciliationConnectorRegistry::new();
    connectors.register(
        CapabilityOrigin::BuiltIn,
        Arc::new(NotificationStatusConnector),
    )?;
    let policy = AllowListEffectReconciliationPolicy::deny_by_default()
        .allow("notification.example", "send")?;
    let reconciler = EffectReconciler::new(engine, connectors)?
        .with_policy(Arc::new(policy))
        .with_clock(Arc::new(ExampleClock));

    let report = reconciler
        .run_once_as(
            EffectReconcilerRunRequest {
                cycle_id: "reconciliation-example-cycle-1".to_owned(),
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
