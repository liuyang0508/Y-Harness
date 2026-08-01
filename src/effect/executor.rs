//! Host-driven, policy-gated execution of durable external Effect intents.
//!
//! The executor performs one bounded sweep when an embedding host calls it. It
//! owns no polling thread, scheduler database, credential store, or receipt
//! verifier. The durable [`super::Effect`] aggregate remains authoritative.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::{task::JoinSet, time::timeout};

#[cfg(test)]
use super::EffectPage;
use super::{
    EffectApplyOutcome, EffectCommand, EffectCommandKind, EffectEngine, EffectLease,
    EffectOperation, EffectPageCursor, EffectReceipt, EffectSnapshot, EffectStatus,
    governor::{
        EffectDispatchAdmissionDecision, EffectDispatchAdmissionRequest, EffectDispatchGovernor,
        EffectDispatchGovernorPolicy, EffectDispatchSettlement,
    },
    page::{EffectPageState, validate_effect_page},
    validate_application_time, validate_identity, validate_receipt,
};
use crate::{
    ActorIdentity, AuthorityContext, CancellationToken, CapabilityOrigin, EffectCommandId,
    EffectId, EffectLeaseId, HarnessError, HarnessFuture,
    isolation::isolate_future,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::{capture_capability_metadata, validate_capability_name, validate_capability_origin},
};

/// Exact embedded Governed Effect Executor API coordinate.
pub const EFFECT_EXECUTOR_API_VERSION: u32 = 1;

const MAX_CONNECTORS: usize = 256;
const MAX_OPERATIONS_PER_CONNECTOR: usize = 256;
const MAX_CONNECTOR_DESCRIPTOR_BYTES: usize = 65_536;
const MAX_EXECUTOR_SCAN_LIMIT: usize = 256;
const MAX_EXECUTOR_CONCURRENCY: usize = 64;
const MIN_POLICY_TIMEOUT_MS: u64 = 1;
const MAX_POLICY_TIMEOUT_MS: u64 = 60_000;
const MIN_EXECUTION_TIMEOUT_MS: u64 = 1;
const MAX_EXECUTION_TIMEOUT_MS: u64 = 604_800_000;
const MIN_LEASE_DURATION_MS: u64 = 1_000;
const MAX_LEASE_DURATION_MS: u64 = 604_800_000;
const MAX_RETRY_AFTER_MS: u64 = 604_800_000;
const DEFAULT_SCAN_LIMIT: usize = 64;
const DEFAULT_CONCURRENCY: usize = 8;
const DEFAULT_POLICY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_GOVERNOR_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_GOVERNOR_RETRY_AFTER_MS: u64 = 5_000;
const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 240_000;
const DEFAULT_SETTLEMENT_RESERVE_MS: u64 = 30_000;
const DEFAULT_LEASE_DURATION_MS: u64 = 300_000;

/// Where duplicate suppression for one Connector is enforced.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectIdempotencyContract {
    /// The target system atomically owns the supplied idempotency key.
    TargetEnforced,
    /// The Connector atomically owns duplicate suppression before target entry.
    ConnectorEnforced,
}

/// Frozen registration metadata for one external-effect Connector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectConnectorDescriptor {
    /// Exact capability routed to this Connector.
    pub capability: String,
    /// Exact Governed Effect Executor contract implemented by the Connector.
    pub api_version: u32,
    /// Explicit supported operation names; no wildcard is permitted.
    pub operations: BTreeSet<String>,
    /// Explicit duplicate-suppression contract.
    pub idempotency: EffectIdempotencyContract,
}

/// Input supplied only after Policy approval and a fresh durable Claim.
///
/// This type intentionally does not implement `Debug` or serialization so the
/// bounded request and idempotency key do not enter ambient diagnostics.
#[derive(Clone)]
pub struct EffectExecutionRequest {
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Trusted execution identity and tenant boundary.
    pub authority: AuthorityContext,
    /// Immutable operation selected during Effect creation.
    pub operation: EffectOperation,
    /// Target-system duplicate-suppression key.
    pub idempotency_key: String,
    /// Immutable bounded external request.
    pub input: Value,
    /// SHA-256 of `input`.
    pub input_sha256: String,
    /// Positive durable attempt.
    pub attempt: u32,
    /// Exact execution fence.
    pub lease_id: EffectLeaseId,
    /// Exclusive server-clock lease boundary.
    pub lease_expires_at_ms: u64,
    /// Cooperative cancellation raised on timeout or host cancellation.
    pub cancellation: CancellationToken,
}

/// Authoritative outcome reported by one registered Connector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectExecutionOutcome {
    /// The external system authoritatively confirmed the side effect.
    Applied {
        /// Content-free external-system evidence.
        receipt: EffectReceipt,
    },
    /// The Connector authoritatively proved that no side effect occurred.
    NotApplied {
        /// Content-free classification.
        reason_code: String,
        /// Optional bounded delay before the next attempt becomes eligible.
        retry_after_ms: Option<u64>,
    },
    /// The Connector cannot prove whether the side effect occurred.
    Unknown {
        /// Content-free uncertainty classification.
        reason_code: String,
    },
}

/// Idempotent external-effect implementation.
///
/// Repeating `execute` with the same `idempotency_key` must obey the frozen
/// [`EffectConnectorDescriptor::idempotency`] contract. Returning
/// [`EffectExecutionOutcome::NotApplied`] is an authoritative assertion, not a
/// retry hint inferred from an error string.
pub trait EffectConnector: Send + Sync {
    /// Returns registration metadata captured exactly once.
    fn descriptor(&self) -> EffectConnectorDescriptor;

    /// Executes one freshly claimed durable Effect.
    fn execute<'a>(
        &'a self,
        request: EffectExecutionRequest,
    ) -> HarnessFuture<'a, EffectExecutionOutcome>;
}

/// Connector paired with its frozen trust origin and metadata.
#[derive(Clone)]
pub struct RegisteredEffectConnector {
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Frozen routing and idempotency contract.
    pub descriptor: EffectConnectorDescriptor,
    /// Executable implementation.
    pub connector: Arc<dyn EffectConnector>,
}

/// Deterministic, collision-safe Connector registry.
#[derive(Default)]
pub struct EffectConnectorRegistry {
    connectors: BTreeMap<String, RegisteredEffectConnector>,
}

impl EffectConnectorRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Captures, validates, and registers one Connector atomically.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        connector: Arc<dyn EffectConnector>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        if self.connectors.len() >= MAX_CONNECTORS {
            return Err(HarnessError::Effect(format!(
                "Effect Connector registry exceeds {MAX_CONNECTORS} entries"
            )));
        }
        let descriptor =
            capture_capability_metadata("Effect Connector descriptor", || connector.descriptor())?;
        validate_connector_descriptor(&descriptor)?;
        if self.connectors.contains_key(&descriptor.capability) {
            return Err(HarnessError::DuplicateCapability(
                descriptor.capability.clone(),
            ));
        }
        self.connectors.insert(
            descriptor.capability.clone(),
            RegisteredEffectConnector {
                origin,
                descriptor,
                connector,
            },
        );
        Ok(())
    }

    /// Returns stable registered capability identities.
    #[must_use]
    pub fn capabilities(&self) -> Vec<String> {
        self.connectors.keys().cloned().collect()
    }

    /// Resolves an exact capability and operation without fallback.
    #[must_use]
    pub fn resolve(&self, operation: &EffectOperation) -> Option<&RegisteredEffectConnector> {
        self.connectors
            .get(&operation.capability)
            .filter(|registered| {
                registered
                    .descriptor
                    .operations
                    .contains(&operation.operation)
            })
    }
}

/// Content supplied to execution Policy before any Claim is created.
///
/// This type intentionally omits `Debug` and serialization because Policy may
/// inspect the complete bounded request.
#[derive(Clone)]
pub struct EffectExecutionPolicyRequest {
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Trusted caller and tenant boundary.
    pub authority: AuthorityContext,
    /// Immutable operation coordinate.
    pub operation: EffectOperation,
    /// Immutable bounded request.
    pub input: Value,
    /// SHA-256 of `input`.
    pub input_sha256: String,
    /// Trust origin of the selected Connector.
    pub connector_origin: CapabilityOrigin,
    /// Frozen duplicate-suppression contract.
    pub idempotency: EffectIdempotencyContract,
}

/// Pre-Claim execution Policy result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectExecutionDecision {
    /// Permit one Claim and Connector entry.
    Allow,
    /// Refuse execution without mutating the Effect.
    Deny {
        /// Content-free denial classification.
        reason_code: String,
    },
}

/// Host-selected pre-Claim Effect execution Policy.
pub trait EffectExecutionPolicy: Send + Sync {
    /// Authorizes or denies one exact Effect and frozen Connector.
    fn authorize<'a>(
        &'a self,
        request: EffectExecutionPolicyRequest,
    ) -> HarnessFuture<'a, EffectExecutionDecision>;
}

/// Safe default Policy that never permits execution.
#[derive(Default)]
pub struct DenyAllEffectExecutions;

impl EffectExecutionPolicy for DenyAllEffectExecutions {
    fn authorize<'a>(
        &'a self,
        _request: EffectExecutionPolicyRequest,
    ) -> HarnessFuture<'a, EffectExecutionDecision> {
        Box::pin(async {
            Ok(EffectExecutionDecision::Deny {
                reason_code: "policy.denied".to_owned(),
            })
        })
    }
}

/// Exact capability/operation allowlist with no wildcard or fallback.
#[derive(Default)]
pub struct AllowListEffectExecutionPolicy {
    allowed: BTreeSet<(String, String)>,
}

impl AllowListEffectExecutionPolicy {
    /// Creates a deny-by-default empty allowlist.
    #[must_use]
    pub fn deny_by_default() -> Self {
        Self::default()
    }

    /// Adds one exact capability and operation.
    pub fn allow(
        mut self,
        capability: impl Into<String>,
        operation: impl Into<String>,
    ) -> Result<Self, HarnessError> {
        let capability = capability.into();
        let operation = operation.into();
        validate_capability_name("Effect Policy capability", &capability)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
        validate_capability_name("Effect Policy operation", &operation)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
        self.allowed.insert((capability, operation));
        Ok(self)
    }
}

impl EffectExecutionPolicy for AllowListEffectExecutionPolicy {
    fn authorize<'a>(
        &'a self,
        request: EffectExecutionPolicyRequest,
    ) -> HarnessFuture<'a, EffectExecutionDecision> {
        Box::pin(async move {
            if self
                .allowed
                .contains(&(request.operation.capability, request.operation.operation))
            {
                Ok(EffectExecutionDecision::Allow)
            } else {
                Ok(EffectExecutionDecision::Deny {
                    reason_code: "policy.denied".to_owned(),
                })
            }
        })
    }
}

/// Trusted time source installed by an embedding host.
pub trait EffectExecutorClock: Send + Sync {
    /// Returns positive Unix milliseconds.
    fn now_ms(&self) -> Result<u64, HarnessError>;
}

/// Wall-clock implementation for ordinary single-host embedding.
#[derive(Default)]
pub struct SystemEffectExecutorClock;

impl EffectExecutorClock for SystemEffectExecutorClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| HarnessError::Effect("Effect Executor clock precedes epoch".to_owned()))?
            .as_millis();
        u64::try_from(millis)
            .map_err(|_| HarnessError::Effect("Effect Executor clock exceeds u64".to_owned()))
    }
}

/// Bounded host policy for one Executor instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EffectExecutorConfig {
    /// Pending Effects inspected per sweep.
    pub scan_limit: usize,
    /// Maximum concurrent Policy/Connector attempts.
    pub max_concurrency: usize,
    /// Maximum Policy duration before fail-closed denial.
    pub policy_timeout_ms: u64,
    /// Maximum durable dispatch-governor operation duration.
    pub governor_timeout_ms: u64,
    /// Safe retry delay when an installed governor is unavailable.
    pub governor_retry_after_ms: u64,
    /// Maximum Connector duration after dispatch.
    pub execution_timeout_ms: u64,
    /// Reserved lease time after the Connector deadline for settlement.
    pub settlement_reserve_ms: u64,
    /// Finite durable Claim duration.
    pub lease_duration_ms: u64,
}

impl Default for EffectExecutorConfig {
    fn default() -> Self {
        Self {
            scan_limit: DEFAULT_SCAN_LIMIT,
            max_concurrency: DEFAULT_CONCURRENCY,
            policy_timeout_ms: DEFAULT_POLICY_TIMEOUT_MS,
            governor_timeout_ms: DEFAULT_GOVERNOR_TIMEOUT_MS,
            governor_retry_after_ms: DEFAULT_GOVERNOR_RETRY_AFTER_MS,
            execution_timeout_ms: DEFAULT_EXECUTION_TIMEOUT_MS,
            settlement_reserve_ms: DEFAULT_SETTLEMENT_RESERVE_MS,
            lease_duration_ms: DEFAULT_LEASE_DURATION_MS,
        }
    }
}

impl EffectExecutorConfig {
    /// Validates count, timeout, and lease-settlement bounds.
    pub fn validate(&self) -> Result<(), HarnessError> {
        if !(1..=MAX_EXECUTOR_SCAN_LIMIT).contains(&self.scan_limit) {
            return Err(HarnessError::Effect(format!(
                "Effect Executor scan_limit must be 1-{MAX_EXECUTOR_SCAN_LIMIT}"
            )));
        }
        if !(1..=MAX_EXECUTOR_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::Effect(format!(
                "Effect Executor max_concurrency must be 1-{MAX_EXECUTOR_CONCURRENCY}"
            )));
        }
        if !(MIN_POLICY_TIMEOUT_MS..=MAX_POLICY_TIMEOUT_MS).contains(&self.policy_timeout_ms) {
            return Err(HarnessError::Effect(format!(
                "Effect Executor policy_timeout_ms must be {MIN_POLICY_TIMEOUT_MS}-{MAX_POLICY_TIMEOUT_MS}"
            )));
        }
        if !(MIN_POLICY_TIMEOUT_MS..=MAX_POLICY_TIMEOUT_MS).contains(&self.governor_timeout_ms) {
            return Err(HarnessError::Effect(format!(
                "Effect Executor governor_timeout_ms must be {MIN_POLICY_TIMEOUT_MS}-{MAX_POLICY_TIMEOUT_MS}"
            )));
        }
        if self.governor_retry_after_ms > MAX_RETRY_AFTER_MS {
            return Err(HarnessError::Effect(format!(
                "Effect Executor governor_retry_after_ms must be 0-{MAX_RETRY_AFTER_MS}"
            )));
        }
        if !(MIN_EXECUTION_TIMEOUT_MS..=MAX_EXECUTION_TIMEOUT_MS)
            .contains(&self.execution_timeout_ms)
        {
            return Err(HarnessError::Effect(format!(
                "Effect Executor execution_timeout_ms must be {MIN_EXECUTION_TIMEOUT_MS}-{MAX_EXECUTION_TIMEOUT_MS}"
            )));
        }
        if !(MIN_LEASE_DURATION_MS..=MAX_LEASE_DURATION_MS).contains(&self.lease_duration_ms) {
            return Err(HarnessError::Effect(format!(
                "Effect Executor lease_duration_ms must be {MIN_LEASE_DURATION_MS}-{MAX_LEASE_DURATION_MS}"
            )));
        }
        let required = self
            .execution_timeout_ms
            .checked_add(self.settlement_reserve_ms)
            .ok_or_else(|| {
                HarnessError::Effect("Effect Executor lease budget overflow".to_owned())
            })?;
        if required >= self.lease_duration_ms {
            return Err(HarnessError::Effect(
                "Effect Executor lease must outlive execution timeout plus settlement reserve"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Caller-stable sweep identity and disposable pending-page cursor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectExecutorRunRequest {
    /// Stable identity reused only when retrying the same uncertain sweep call.
    pub cycle_id: String,
    /// Exclusive pending-Effect cursor, or `None` to begin a sweep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<EffectPageCursor>,
}

/// Content-free result of one eligible Effect considered by a sweep.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectExecutorAttemptOutcome {
    /// No exact registered Connector supports the operation.
    ConnectorUnavailable,
    /// Policy denied execution before Claim.
    PolicyDenied {
        /// Content-free Policy classification.
        reason_code: String,
    },
    /// Policy failed, panicked, timed out, or returned invalid evidence.
    PolicyUnavailable,
    /// Host cancellation was observed before Claim.
    CancelledBeforeClaim,
    /// Another mutation won the observed revision.
    ClaimFenced {
        /// Current durable revision observed by the failed Claim.
        actual_revision: u64,
    },
    /// Claim failed without exposing internal diagnostics.
    ClaimFailed,
    /// The exact deterministic Claim was already committed, so this caller did
    /// not enter the Connector.
    ClaimAlreadyCommitted,
    /// Trusted time failed after Claim; the lease remains authoritative.
    ClockUnavailableAfterClaim,
    /// Durable dispatch governance was unavailable; no Connector was entered.
    GovernorUnavailable {
        /// Absolute trusted eligibility time durably selected for retry.
        retry_at_ms: u64,
    },
    /// The execution lane exhausted its fixed-window dispatch budget.
    RateLimited {
        /// Absolute trusted eligibility time durably selected for retry.
        retry_at_ms: u64,
    },
    /// The execution lane circuit is open or owns a half-open probe.
    CircuitOpen {
        /// Absolute trusted eligibility time durably selected for retry.
        retry_at_ms: u64,
    },
    /// External effect was authoritatively applied.
    Applied,
    /// External system authoritatively confirmed no effect and no retry.
    Rejected {
        /// Content-free terminal classification.
        reason_code: String,
    },
    /// External system confirmed no effect and selected a later attempt.
    RetryScheduled {
        /// Content-free classification.
        reason_code: String,
        /// Absolute trusted eligibility time.
        retry_at_ms: u64,
    },
    /// External outcome is uncertain and requires reconciliation.
    Unknown {
        /// Content-free uncertainty classification.
        reason_code: String,
    },
    /// Settlement lost the exact post-Claim revision.
    SettlementFenced {
        /// Current durable revision.
        actual_revision: u64,
    },
    /// Settlement failed without exposing Provider or persistence content.
    SettlementFailed,
    /// One bounded Executor worker stopped unexpectedly; durable state remains
    /// authoritative and may require lease-expiry recovery.
    AttemptFailed,
}

/// One source-ordered Executor attempt report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectExecutorAttempt {
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Revision observed during the pending scan.
    pub observed_revision: u64,
    /// Positive attempt advertised by pending state.
    pub attempt: u32,
    /// Exact lease when a Claim was attempted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<EffectLeaseId>,
    /// An allowed dispatch completed, but durable governor health settlement failed.
    #[serde(default, skip_serializing_if = "is_false")]
    pub governor_settlement_failed: bool,
    /// Content-free settlement.
    pub outcome: EffectExecutorAttemptOutcome,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// One bounded host-driven sweep report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectExecutorRunReport {
    /// Trusted time used to select eligible pending Effects.
    pub scanned_at_ms: u64,
    /// Number of authoritative pending records inspected.
    pub scanned: usize,
    /// Number eligible at `scanned_at_ms`.
    pub eligible: usize,
    /// Whether another pending identity remains in the current sweep.
    pub has_more: bool,
    /// Disposable continuation, reset to `None` at sweep completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<EffectPageCursor>,
    /// Eligible results in stable source identity order.
    pub attempts: Vec<EffectExecutorAttempt>,
}

/// Optional host module that safely consumes durable pending Effect intents.
#[derive(Clone)]
pub struct EffectExecutor {
    engine: EffectEngine,
    connectors: Arc<EffectConnectorRegistry>,
    policy: Arc<dyn EffectExecutionPolicy>,
    clock: Arc<dyn EffectExecutorClock>,
    governor: Option<ConfiguredDispatchGovernor>,
    config: EffectExecutorConfig,
}

#[derive(Clone)]
struct ConfiguredDispatchGovernor {
    governor: Arc<dyn EffectDispatchGovernor>,
    policy: EffectDispatchGovernorPolicy,
}

#[derive(Clone, Copy)]
struct EffectSettlementContext<'a> {
    cycle_id: &'a str,
    prepared: &'a PreparedEffect,
    claimed: &'a EffectSnapshot,
    lease: &'a EffectLease,
    authority: &'a AuthorityContext,
}

impl EffectExecutor {
    /// Creates a default-deny Executor with the system clock.
    pub fn new(
        engine: EffectEngine,
        connectors: EffectConnectorRegistry,
    ) -> Result<Self, HarnessError> {
        let config = EffectExecutorConfig::default();
        config.validate()?;
        Ok(Self {
            engine,
            connectors: Arc::new(connectors),
            policy: Arc::new(DenyAllEffectExecutions),
            clock: Arc::new(SystemEffectExecutorClock),
            governor: None,
            config,
        })
    }

    /// Installs a host-selected pre-Claim Policy.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn EffectExecutionPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Installs a trusted host clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn EffectExecutorClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Installs durable post-Claim dispatch governance for every execution lane.
    pub fn with_dispatch_governor(
        mut self,
        governor: Arc<dyn EffectDispatchGovernor>,
        policy: EffectDispatchGovernorPolicy,
    ) -> Result<Self, HarnessError> {
        policy.validate()?;
        self.governor = Some(ConfiguredDispatchGovernor { governor, policy });
        Ok(self)
    }

    /// Installs validated bounded execution policy.
    pub fn with_config(mut self, config: EffectExecutorConfig) -> Result<Self, HarnessError> {
        config.validate()?;
        self.config = config;
        Ok(self)
    }

    /// Runs one unscoped bounded pending sweep.
    pub async fn run_once(
        &self,
        request: EffectExecutorRunRequest,
        cancellation: CancellationToken,
    ) -> Result<EffectExecutorRunReport, HarnessError> {
        self.run_once_as(request, &AuthorityContext::local_process(), cancellation)
            .await
    }

    /// Runs one bounded pending sweep inside an exact trusted authority.
    pub async fn run_once_as(
        &self,
        request: EffectExecutorRunRequest,
        authority: &AuthorityContext,
        cancellation: CancellationToken,
    ) -> Result<EffectExecutorRunReport, HarnessError> {
        validate_run_request(&request, authority)?;
        self.config.validate()?;
        let scanned_at_ms = trusted_now(self.clock.as_ref())?;
        let page = self
            .engine
            .list_as(
                Some("pending"),
                request.after.as_ref(),
                self.config.scan_limit,
                authority,
            )
            .await?;
        validate_effect_page(
            "Effect Executor",
            &page,
            request.after.as_ref(),
            self.config.scan_limit,
            authority,
            EffectPageState::Pending,
        )?;

        let mut prepared = Vec::new();
        for snapshot in &page.effects {
            let EffectStatus::Pending {
                next_attempt,
                not_before_ms,
            } = snapshot.effect().status()
            else {
                return Err(HarnessError::Effect(
                    "Effect Executor pending scan returned non-pending state".to_owned(),
                ));
            };
            if *not_before_ms <= scanned_at_ms {
                let (lease_id, claim_command_id) =
                    execution_identities(&request.cycle_id, snapshot, *next_attempt, authority)?;
                prepared.push(PreparedEffect {
                    snapshot: snapshot.clone(),
                    attempt: *next_attempt,
                    lease_id,
                    claim_command_id,
                });
            }
        }
        let eligible = prepared.len();
        let context = ExecutorRunContext {
            cycle_id: request.cycle_id,
            authority: authority.clone(),
            cancellation,
        };
        let mut pending = prepared.into_iter().enumerate().collect::<VecDeque<_>>();
        let mut workers = JoinSet::new();
        let mut fallbacks = HashMap::new();
        for _ in 0..self.config.max_concurrency {
            let Some((index, prepared)) = pending.pop_front() else {
                break;
            };
            self.spawn_prepared(
                &mut workers,
                &mut fallbacks,
                index,
                prepared,
                context.clone(),
            );
        }
        let mut attempts = Vec::with_capacity(eligible);
        while !workers.is_empty() {
            let joined = workers.join_next_with_id().await.ok_or_else(|| {
                HarnessError::Effect("Effect Executor lost its bounded worker set".to_owned())
            })?;
            let task_id = match &joined {
                Ok((task_id, _)) => *task_id,
                Err(error) => error.id(),
            };
            let (fallback_index, fallback) = fallbacks.remove(&task_id).ok_or_else(|| {
                HarnessError::Effect("Effect Executor lost a worker identity".to_owned())
            })?;
            match joined {
                Ok((_, attempt)) => attempts.push(attempt),
                Err(_) => attempts.push((fallback_index, fallback)),
            }
            if let Some((index, prepared)) = pending.pop_front() {
                self.spawn_prepared(
                    &mut workers,
                    &mut fallbacks,
                    index,
                    prepared,
                    context.clone(),
                );
            }
        }
        attempts.sort_by_key(|(index, _)| *index);
        let attempts = attempts.into_iter().map(|(_, attempt)| attempt).collect();

        Ok(EffectExecutorRunReport {
            scanned_at_ms,
            scanned: page.effects.len(),
            eligible,
            has_more: page.has_more,
            next_after: page.has_more.then_some(page.next_cursor).flatten(),
            attempts,
        })
    }

    fn spawn_prepared(
        &self,
        workers: &mut JoinSet<(usize, EffectExecutorAttempt)>,
        fallbacks: &mut HashMap<tokio::task::Id, (usize, EffectExecutorAttempt)>,
        index: usize,
        prepared: PreparedEffect,
        context: ExecutorRunContext,
    ) {
        let fallback = EffectExecutorAttempt {
            effect_id: prepared.snapshot.id().clone(),
            observed_revision: prepared.snapshot.revision(),
            attempt: prepared.attempt,
            lease_id: None,
            governor_settlement_failed: false,
            outcome: EffectExecutorAttemptOutcome::AttemptFailed,
        };
        let executor = self.clone();
        let handle = workers.spawn(async move {
            (
                index,
                executor
                    .execute_prepared(
                        &context.cycle_id,
                        prepared,
                        &context.authority,
                        context.cancellation,
                    )
                    .await,
            )
        });
        fallbacks.insert(handle.id(), (index, fallback));
    }

    async fn execute_prepared(
        &self,
        cycle_id: &str,
        prepared: PreparedEffect,
        authority: &AuthorityContext,
        cancellation: CancellationToken,
    ) -> EffectExecutorAttempt {
        let base = |outcome| EffectExecutorAttempt {
            effect_id: prepared.snapshot.id().clone(),
            observed_revision: prepared.snapshot.revision(),
            attempt: prepared.attempt,
            lease_id: None,
            governor_settlement_failed: false,
            outcome,
        };
        let operation = prepared.snapshot.effect().operation();
        let Some(registered) = self.connectors.resolve(operation).cloned() else {
            return base(EffectExecutorAttemptOutcome::ConnectorUnavailable);
        };
        if cancellation.is_cancelled() {
            return base(EffectExecutorAttemptOutcome::CancelledBeforeClaim);
        }
        let policy_request = EffectExecutionPolicyRequest {
            effect_id: prepared.snapshot.id().clone(),
            authority: authority.clone(),
            operation: operation.clone(),
            input: prepared.snapshot.effect().input().clone(),
            input_sha256: prepared.snapshot.effect().input_sha256().to_owned(),
            connector_origin: registered.origin.clone(),
            idempotency: registered.descriptor.idempotency,
        };
        let decision = match isolate_future(|| self.policy.authorize(policy_request), None) {
            Err(()) => return base(EffectExecutorAttemptOutcome::PolicyUnavailable),
            Ok(future) => {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return base(EffectExecutorAttemptOutcome::CancelledBeforeClaim);
                    }
                    result = timeout(
                        Duration::from_millis(self.config.policy_timeout_ms),
                        future,
                    ) => {
                        match result {
                            Ok(Ok(Ok(decision))) => decision,
                            Ok(Ok(Err(_))) | Ok(Err(())) | Err(_) => {
                                return base(EffectExecutorAttemptOutcome::PolicyUnavailable);
                            }
                        }
                    }
                }
            }
        };
        match decision {
            EffectExecutionDecision::Deny { reason_code } => {
                if validate_capability_name("Effect Policy denial reason", &reason_code).is_err() {
                    return base(EffectExecutorAttemptOutcome::PolicyUnavailable);
                }
                return base(EffectExecutorAttemptOutcome::PolicyDenied { reason_code });
            }
            EffectExecutionDecision::Allow => {}
        }
        if cancellation.is_cancelled() {
            return base(EffectExecutorAttemptOutcome::CancelledBeforeClaim);
        }

        let claim_time = match trusted_now(self.clock.as_ref()) {
            Ok(now) => now,
            Err(_) => return base(EffectExecutorAttemptOutcome::ClaimFailed),
        };
        let claim = EffectCommand {
            id: prepared.claim_command_id.clone(),
            kind: EffectCommandKind::Claim {
                lease_id: prepared.lease_id.clone(),
                lease_duration_ms: self.config.lease_duration_ms,
            },
        };
        let claimed = match self
            .engine
            .apply_as(
                prepared.snapshot.id(),
                prepared.snapshot.revision(),
                claim,
                claim_time,
                authority,
            )
            .await
        {
            Ok(result) => result,
            Err(HarnessError::EffectConflict { actual, .. }) => {
                return EffectExecutorAttempt {
                    lease_id: Some(prepared.lease_id),
                    outcome: EffectExecutorAttemptOutcome::ClaimFenced {
                        actual_revision: actual,
                    },
                    ..base(EffectExecutorAttemptOutcome::ClaimFailed)
                };
            }
            Err(_) => {
                return EffectExecutorAttempt {
                    lease_id: Some(prepared.lease_id),
                    ..base(EffectExecutorAttemptOutcome::ClaimFailed)
                };
            }
        };
        if claimed.outcome == EffectApplyOutcome::Duplicate {
            return EffectExecutorAttempt {
                lease_id: Some(prepared.lease_id),
                ..base(EffectExecutorAttemptOutcome::ClaimAlreadyCommitted)
            };
        }
        let EffectStatus::Claimed { lease } = claimed.snapshot.effect().status() else {
            return EffectExecutorAttempt {
                lease_id: Some(prepared.lease_id),
                ..base(EffectExecutorAttemptOutcome::ClaimFailed)
            };
        };
        if lease.id != prepared.lease_id
            || lease.owner != *authority.actor()
            || lease.attempt != prepared.attempt
            || claimed.snapshot.revision() != prepared.snapshot.revision().saturating_add(1)
        {
            return EffectExecutorAttempt {
                lease_id: Some(prepared.lease_id),
                ..base(EffectExecutorAttemptOutcome::ClaimFailed)
            };
        }
        let lease = lease.clone();

        if cancellation.is_cancelled() {
            return self
                .settle(
                    cycle_id,
                    &prepared,
                    &claimed.snapshot,
                    &lease,
                    EffectExecutionOutcome::NotApplied {
                        reason_code: "executor.cancelled_before_dispatch".to_owned(),
                        retry_after_ms: Some(0),
                    },
                    authority,
                )
                .await;
        }

        let governed = if let Some(configured) = &self.governor {
            let admitted_at_ms = match trusted_now(self.clock.as_ref()) {
                Ok(now) => now,
                Err(_) => {
                    return EffectExecutorAttempt {
                        lease_id: Some(prepared.lease_id),
                        outcome: EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim,
                        ..base(EffectExecutorAttemptOutcome::ClaimFailed)
                    };
                }
            };
            let request = EffectDispatchAdmissionRequest {
                admission_id: prepared.lease_id.clone(),
                operation: operation.clone(),
                policy: configured.policy.clone(),
                admitted_at_ms,
            };
            let decision =
                match isolate_future(|| configured.governor.admit_as(request, authority), None) {
                    Err(()) => None,
                    Ok(future) => match timeout(
                        Duration::from_millis(self.config.governor_timeout_ms),
                        future,
                    )
                    .await
                    {
                        Ok(Ok(Ok(decision)))
                            if valid_governor_decision(&decision, admitted_at_ms) =>
                        {
                            Some(decision)
                        }
                        Ok(Ok(Ok(_))) => None,
                        Ok(Ok(Err(_))) | Ok(Err(())) | Err(_) => None,
                    },
                };
            match decision {
                Some(EffectDispatchAdmissionDecision::Allow) => Some(configured.clone()),
                Some(EffectDispatchAdmissionDecision::AllowProbe) => Some(configured.clone()),
                Some(EffectDispatchAdmissionDecision::RateLimited { retry_at_ms }) => {
                    return self
                        .settle_governor_denial(
                            EffectSettlementContext {
                                cycle_id,
                                prepared: &prepared,
                                claimed: &claimed.snapshot,
                                lease: &lease,
                                authority,
                            },
                            "governor.rate_limited",
                            retry_at_ms,
                            EffectExecutorAttemptOutcome::RateLimited { retry_at_ms },
                        )
                        .await;
                }
                Some(EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms }) => {
                    return self
                        .settle_governor_denial(
                            EffectSettlementContext {
                                cycle_id,
                                prepared: &prepared,
                                claimed: &claimed.snapshot,
                                lease: &lease,
                                authority,
                            },
                            "governor.circuit_open",
                            retry_at_ms,
                            EffectExecutorAttemptOutcome::CircuitOpen { retry_at_ms },
                        )
                        .await;
                }
                None => {
                    let retry_at_ms = admitted_at_ms
                        .checked_add(self.config.governor_retry_after_ms)
                        .unwrap_or(admitted_at_ms);
                    return self
                        .settle_governor_denial(
                            EffectSettlementContext {
                                cycle_id,
                                prepared: &prepared,
                                claimed: &claimed.snapshot,
                                lease: &lease,
                                authority,
                            },
                            "governor.unavailable",
                            retry_at_ms,
                            EffectExecutorAttemptOutcome::GovernorUnavailable { retry_at_ms },
                        )
                        .await;
                }
            }
        } else {
            None
        };

        if cancellation.is_cancelled() {
            let governor_settlement_failed = if let Some(configured) = &governed {
                match trusted_now(self.clock.as_ref()) {
                    Ok(settled_at_ms) => {
                        !self
                            .settle_governor(
                                configured,
                                &prepared.lease_id,
                                EffectDispatchSettlement::Abandoned,
                                settled_at_ms,
                                authority,
                            )
                            .await
                    }
                    Err(_) => true,
                }
            } else {
                false
            };
            let mut attempt = self
                .settle(
                    cycle_id,
                    &prepared,
                    &claimed.snapshot,
                    &lease,
                    EffectExecutionOutcome::NotApplied {
                        reason_code: "executor.cancelled_before_dispatch".to_owned(),
                        retry_after_ms: Some(0),
                    },
                    authority,
                )
                .await;
            attempt.governor_settlement_failed = governor_settlement_failed;
            return attempt;
        }

        let connector_cancellation = CancellationToken::new();
        let execution_request = EffectExecutionRequest {
            effect_id: prepared.snapshot.id().clone(),
            authority: authority.clone(),
            operation: prepared.snapshot.effect().operation().clone(),
            idempotency_key: prepared.snapshot.effect().idempotency_key().to_owned(),
            input: prepared.snapshot.effect().input().clone(),
            input_sha256: prepared.snapshot.effect().input_sha256().to_owned(),
            attempt: prepared.attempt,
            lease_id: prepared.lease_id.clone(),
            lease_expires_at_ms: lease.expires_at_ms,
            cancellation: connector_cancellation.clone(),
        };
        let (outcome, dispatch_settlement) = match isolate_future(
            || registered.connector.execute(execution_request),
            Some(connector_cancellation),
        ) {
            Err(()) => (
                EffectExecutionOutcome::Unknown {
                    reason_code: "connector.failed".to_owned(),
                },
                EffectDispatchSettlement::AvailabilityFailure,
            ),
            Ok(future) => {
                tokio::select! {
                    _ = cancellation.cancelled() => (
                        EffectExecutionOutcome::Unknown {
                            reason_code: "executor.cancelled_after_dispatch".to_owned(),
                        },
                        EffectDispatchSettlement::Abandoned,
                    ),
                    result = timeout(
                        Duration::from_millis(self.config.execution_timeout_ms),
                        future,
                    ) => {
                        match result {
                            Ok(Ok(Ok(outcome))) => (
                                outcome,
                                EffectDispatchSettlement::Healthy,
                            ),
                            Ok(Ok(Err(_))) | Ok(Err(())) => (
                                EffectExecutionOutcome::Unknown {
                                    reason_code: "connector.failed".to_owned(),
                                },
                                EffectDispatchSettlement::AvailabilityFailure,
                            ),
                            Err(_) => (
                                EffectExecutionOutcome::Unknown {
                                    reason_code: "connector.timeout".to_owned(),
                                },
                                EffectDispatchSettlement::AvailabilityFailure,
                            ),
                        }
                    }
                }
            }
        };
        let settled_at_ms = match trusted_now(self.clock.as_ref()) {
            Ok(now) => now,
            Err(_) => {
                return EffectExecutorAttempt {
                    effect_id: prepared.snapshot.id().clone(),
                    observed_revision: prepared.snapshot.revision(),
                    attempt: prepared.attempt,
                    lease_id: Some(prepared.lease_id),
                    governor_settlement_failed: governed.is_some(),
                    outcome: EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim,
                };
            }
        };
        let normalized = normalize_connector_outcome(outcome, settled_at_ms);
        let dispatch_settlement = if dispatch_settlement == EffectDispatchSettlement::Healthy
            && matches!(
                normalized,
                EffectExecutionOutcome::Unknown { ref reason_code }
                    if reason_code == "connector.invalid_outcome"
            ) {
            EffectDispatchSettlement::AvailabilityFailure
        } else {
            dispatch_settlement
        };
        let governor_settlement_failed = if let Some(configured) = &governed {
            !self
                .settle_governor(
                    configured,
                    &prepared.lease_id,
                    dispatch_settlement,
                    settled_at_ms,
                    authority,
                )
                .await
        } else {
            false
        };
        let mut attempt = self
            .settle_at(
                EffectSettlementContext {
                    cycle_id,
                    prepared: &prepared,
                    claimed: &claimed.snapshot,
                    lease: &lease,
                    authority,
                },
                normalized,
                None,
                settled_at_ms,
            )
            .await;
        attempt.governor_settlement_failed = governor_settlement_failed;
        attempt
    }

    async fn settle(
        &self,
        cycle_id: &str,
        prepared: &PreparedEffect,
        claimed: &EffectSnapshot,
        lease: &EffectLease,
        outcome: EffectExecutionOutcome,
        authority: &AuthorityContext,
    ) -> EffectExecutorAttempt {
        let settled_at_ms = match trusted_now(self.clock.as_ref()) {
            Ok(now) => now,
            Err(_) => {
                return EffectExecutorAttempt {
                    effect_id: prepared.snapshot.id().clone(),
                    observed_revision: prepared.snapshot.revision(),
                    attempt: prepared.attempt,
                    lease_id: Some(prepared.lease_id.clone()),
                    governor_settlement_failed: false,
                    outcome: EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim,
                };
            }
        };
        let outcome = normalize_connector_outcome(outcome, settled_at_ms);
        self.settle_at(
            EffectSettlementContext {
                cycle_id,
                prepared,
                claimed,
                lease,
                authority,
            },
            outcome,
            None,
            settled_at_ms,
        )
        .await
    }

    async fn settle_at(
        &self,
        context: EffectSettlementContext<'_>,
        outcome: EffectExecutionOutcome,
        absolute_retry_at_ms: Option<u64>,
        settled_at_ms: u64,
    ) -> EffectExecutorAttempt {
        let EffectSettlementContext {
            cycle_id,
            prepared,
            claimed,
            lease,
            authority,
        } = context;
        let mut attempt = EffectExecutorAttempt {
            effect_id: prepared.snapshot.id().clone(),
            observed_revision: prepared.snapshot.revision(),
            attempt: prepared.attempt,
            lease_id: Some(prepared.lease_id.clone()),
            governor_settlement_failed: false,
            outcome: EffectExecutorAttemptOutcome::SettlementFailed,
        };
        let (purpose, kind) = match &outcome {
            EffectExecutionOutcome::Applied { receipt } => (
                "applied",
                EffectCommandKind::RecordApplied {
                    lease_id: lease.id.clone(),
                    receipt: receipt.clone(),
                },
            ),
            EffectExecutionOutcome::NotApplied {
                reason_code,
                retry_after_ms,
            } => {
                let retry_at_ms = match (absolute_retry_at_ms, retry_after_ms) {
                    (Some(retry_at_ms), _) => Some(retry_at_ms.max(settled_at_ms)),
                    (None, Some(delay)) => match settled_at_ms.checked_add(*delay) {
                        Some(value) => Some(value),
                        None => {
                            return self
                                .settle_invalid_outcome(
                                    cycle_id,
                                    prepared,
                                    claimed,
                                    lease,
                                    settled_at_ms,
                                    authority,
                                )
                                .await;
                        }
                    },
                    (None, None) => None,
                };
                (
                    "not-applied",
                    EffectCommandKind::RecordNotApplied {
                        lease_id: lease.id.clone(),
                        reason_code: reason_code.clone(),
                        retry_at_ms,
                    },
                )
            }
            EffectExecutionOutcome::Unknown { reason_code } => (
                "unknown",
                EffectCommandKind::RecordUnknown {
                    lease_id: lease.id.clone(),
                    reason_code: reason_code.clone(),
                },
            ),
        };
        let command_id = settlement_command_id(
            cycle_id,
            prepared.snapshot.id(),
            claimed.revision(),
            &lease.id,
            purpose,
            authority,
        );
        let command_id = match command_id {
            Ok(command_id) => command_id,
            Err(_) => return attempt,
        };
        match self
            .engine
            .apply_as(
                prepared.snapshot.id(),
                claimed.revision(),
                EffectCommand {
                    id: command_id,
                    kind,
                },
                settled_at_ms,
                authority,
            )
            .await
        {
            Ok(result) => {
                attempt.outcome = report_settlement(&outcome, result.snapshot.effect().status());
            }
            Err(HarnessError::EffectConflict { actual, .. }) => {
                attempt.outcome = EffectExecutorAttemptOutcome::SettlementFenced {
                    actual_revision: actual,
                };
            }
            Err(_) => {}
        }
        attempt
    }

    async fn settle_governor_denial(
        &self,
        context: EffectSettlementContext<'_>,
        reason_code: &str,
        retry_at_ms: u64,
        governed_outcome: EffectExecutorAttemptOutcome,
    ) -> EffectExecutorAttempt {
        let settled_at_ms = match trusted_now(self.clock.as_ref()) {
            Ok(now) => now,
            Err(_) => {
                return EffectExecutorAttempt {
                    effect_id: context.prepared.snapshot.id().clone(),
                    observed_revision: context.prepared.snapshot.revision(),
                    attempt: context.prepared.attempt,
                    lease_id: Some(context.prepared.lease_id.clone()),
                    governor_settlement_failed: false,
                    outcome: EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim,
                };
            }
        };
        let mut attempt = self
            .settle_at(
                context,
                EffectExecutionOutcome::NotApplied {
                    reason_code: reason_code.to_owned(),
                    retry_after_ms: Some(0),
                },
                Some(retry_at_ms),
                settled_at_ms,
            )
            .await;
        if matches!(
            attempt.outcome,
            EffectExecutorAttemptOutcome::RetryScheduled { .. }
        ) {
            attempt.outcome = governed_outcome;
        }
        attempt
    }

    async fn settle_governor(
        &self,
        configured: &ConfiguredDispatchGovernor,
        admission_id: &EffectLeaseId,
        settlement: EffectDispatchSettlement,
        settled_at_ms: u64,
        authority: &AuthorityContext,
    ) -> bool {
        let Ok(future) = isolate_future(
            || {
                configured
                    .governor
                    .settle_as(admission_id, settlement, settled_at_ms, authority)
            },
            None,
        ) else {
            return false;
        };
        matches!(
            timeout(
                Duration::from_millis(self.config.governor_timeout_ms),
                future,
            )
            .await,
            Ok(Ok(Ok(())))
        )
    }

    async fn settle_invalid_outcome(
        &self,
        cycle_id: &str,
        prepared: &PreparedEffect,
        claimed: &EffectSnapshot,
        lease: &EffectLease,
        settled_at_ms: u64,
        authority: &AuthorityContext,
    ) -> EffectExecutorAttempt {
        let mut attempt = EffectExecutorAttempt {
            effect_id: prepared.snapshot.id().clone(),
            observed_revision: prepared.snapshot.revision(),
            attempt: prepared.attempt,
            lease_id: Some(prepared.lease_id.clone()),
            governor_settlement_failed: false,
            outcome: EffectExecutorAttemptOutcome::SettlementFailed,
        };
        let command_id = match settlement_command_id(
            cycle_id,
            prepared.snapshot.id(),
            claimed.revision(),
            &lease.id,
            "invalid-outcome",
            authority,
        ) {
            Ok(command_id) => command_id,
            Err(_) => return attempt,
        };
        match self
            .engine
            .apply_as(
                prepared.snapshot.id(),
                claimed.revision(),
                EffectCommand {
                    id: command_id,
                    kind: EffectCommandKind::RecordUnknown {
                        lease_id: lease.id.clone(),
                        reason_code: "connector.invalid_outcome".to_owned(),
                    },
                },
                settled_at_ms,
                authority,
            )
            .await
        {
            Ok(result) => {
                attempt.outcome = report_settlement(
                    &EffectExecutionOutcome::Unknown {
                        reason_code: "connector.invalid_outcome".to_owned(),
                    },
                    result.snapshot.effect().status(),
                );
            }
            Err(HarnessError::EffectConflict { actual, .. }) => {
                attempt.outcome = EffectExecutorAttemptOutcome::SettlementFenced {
                    actual_revision: actual,
                };
            }
            Err(_) => {}
        }
        attempt
    }
}

#[derive(Clone)]
struct PreparedEffect {
    snapshot: EffectSnapshot,
    attempt: u32,
    lease_id: EffectLeaseId,
    claim_command_id: EffectCommandId,
}

#[derive(Clone)]
struct ExecutorRunContext {
    cycle_id: String,
    authority: AuthorityContext,
    cancellation: CancellationToken,
}

fn validate_connector_descriptor(
    descriptor: &EffectConnectorDescriptor,
) -> Result<(), HarnessError> {
    validate_capability_name("Effect Connector capability", &descriptor.capability)
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    if descriptor.api_version != EFFECT_EXECUTOR_API_VERSION {
        return Err(HarnessError::Effect(format!(
            "Effect Connector {} requires API {}, received {}",
            descriptor.capability, EFFECT_EXECUTOR_API_VERSION, descriptor.api_version
        )));
    }
    if descriptor.operations.is_empty()
        || descriptor.operations.len() > MAX_OPERATIONS_PER_CONNECTOR
    {
        return Err(HarnessError::Effect(format!(
            "Effect Connector operations must contain 1-{MAX_OPERATIONS_PER_CONNECTOR} entries"
        )));
    }
    for operation in &descriptor.operations {
        validate_capability_name("Effect Connector operation", operation)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
    }
    bounded_serialized_size(descriptor, MAX_CONNECTOR_DESCRIPTOR_BYTES)
        .map_err(descriptor_bound_error)?;
    Ok(())
}

fn descriptor_bound_error(error: BoundedJsonError) -> HarnessError {
    let detail = match error {
        BoundedJsonError::LimitExceeded => "exceeds its encoded-byte limit",
        BoundedJsonError::CannotEncode => "cannot be encoded",
    };
    HarnessError::Effect(format!(
        "Effect Connector descriptor {detail}; limit is {MAX_CONNECTOR_DESCRIPTOR_BYTES} bytes"
    ))
}

fn validate_run_request(
    request: &EffectExecutorRunRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect Executor authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_identity("Effect Executor cycle", &request.cycle_id)?;
    if let Some(after) = &request.after {
        validate_identity("Effect Executor cursor", after.effect_id.as_str())?;
    }
    Ok(())
}

#[derive(Serialize)]
struct ExecutionIdentity<'a> {
    purpose: &'a str,
    cycle_id: &'a str,
    actor: &'a ActorIdentity,
    tenant_id: Option<&'a str>,
    effect_id: &'a str,
    revision: u64,
    attempt: u32,
    lease_id: Option<&'a str>,
}

fn execution_identities(
    cycle_id: &str,
    snapshot: &EffectSnapshot,
    attempt: u32,
    authority: &AuthorityContext,
) -> Result<(EffectLeaseId, EffectCommandId), HarnessError> {
    let digest = execution_digest(&ExecutionIdentity {
        purpose: "claim",
        cycle_id,
        actor: authority.actor(),
        tenant_id: authority.tenant_id(),
        effect_id: snapshot.id().as_str(),
        revision: snapshot.revision(),
        attempt,
        lease_id: None,
    })?;
    Ok((
        EffectLeaseId::from_string(format!("executor-lease-{digest}")),
        EffectCommandId::from_string(format!("executor-claim-{digest}")),
    ))
}

fn settlement_command_id(
    cycle_id: &str,
    effect_id: &EffectId,
    revision: u64,
    lease_id: &EffectLeaseId,
    purpose: &str,
    authority: &AuthorityContext,
) -> Result<EffectCommandId, HarnessError> {
    let digest = execution_digest(&ExecutionIdentity {
        purpose,
        cycle_id,
        actor: authority.actor(),
        tenant_id: authority.tenant_id(),
        effect_id: effect_id.as_str(),
        revision,
        attempt: 0,
        lease_id: Some(lease_id.as_str()),
    })?;
    Ok(EffectCommandId::from_string(format!(
        "executor-settle-{digest}"
    )))
}

fn execution_digest(value: &ExecutionIdentity<'_>) -> Result<String, HarnessError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| HarnessError::Effect("cannot encode Effect execution identity".to_owned()))?;
    Ok(Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn valid_governor_decision(
    decision: &EffectDispatchAdmissionDecision,
    admitted_at_ms: u64,
) -> bool {
    match decision {
        EffectDispatchAdmissionDecision::Allow | EffectDispatchAdmissionDecision::AllowProbe => {
            true
        }
        EffectDispatchAdmissionDecision::RateLimited { retry_at_ms }
        | EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms } => {
            *retry_at_ms >= admitted_at_ms && validate_application_time(*retry_at_ms).is_ok()
        }
    }
}

fn normalize_connector_outcome(
    outcome: EffectExecutionOutcome,
    settled_at_ms: u64,
) -> EffectExecutionOutcome {
    let valid = match &outcome {
        EffectExecutionOutcome::Applied { receipt } => {
            validate_receipt(receipt, settled_at_ms).is_ok()
        }
        EffectExecutionOutcome::NotApplied {
            reason_code,
            retry_after_ms,
        } => {
            validate_capability_name("Effect Connector not-applied reason", reason_code).is_ok()
                && retry_after_ms.is_none_or(|delay| delay <= MAX_RETRY_AFTER_MS)
        }
        EffectExecutionOutcome::Unknown { reason_code } => {
            validate_capability_name("Effect Connector uncertainty reason", reason_code).is_ok()
        }
    };
    if valid {
        outcome
    } else {
        EffectExecutionOutcome::Unknown {
            reason_code: "connector.invalid_outcome".to_owned(),
        }
    }
}

fn report_settlement(
    outcome: &EffectExecutionOutcome,
    status: &EffectStatus,
) -> EffectExecutorAttemptOutcome {
    match (outcome, status) {
        (EffectExecutionOutcome::Applied { .. }, EffectStatus::Applied { .. }) => {
            EffectExecutorAttemptOutcome::Applied
        }
        (
            EffectExecutionOutcome::NotApplied {
                reason_code,
                retry_after_ms: None,
            },
            EffectStatus::Rejected {
                reason_code: durable_reason,
                ..
            },
        ) if reason_code == durable_reason => EffectExecutorAttemptOutcome::Rejected {
            reason_code: reason_code.clone(),
        },
        (
            EffectExecutionOutcome::NotApplied {
                reason_code,
                retry_after_ms: Some(_),
            },
            EffectStatus::Pending { not_before_ms, .. },
        ) => EffectExecutorAttemptOutcome::RetryScheduled {
            reason_code: reason_code.clone(),
            retry_at_ms: *not_before_ms,
        },
        (
            EffectExecutionOutcome::Unknown { reason_code },
            EffectStatus::Unknown {
                reason_code: durable_reason,
                ..
            },
        ) if reason_code == durable_reason => EffectExecutorAttemptOutcome::Unknown {
            reason_code: reason_code.clone(),
        },
        _ => EffectExecutorAttemptOutcome::SettlementFailed,
    }
}

fn trusted_now(clock: &dyn EffectExecutorClock) -> Result<u64, HarnessError> {
    let value = catch_unwind(AssertUnwindSafe(|| clock.now_ms()))
        .map_err(|_| HarnessError::Effect("Effect Executor clock panicked".to_owned()))??;
    validate_application_time(value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
    };

    use tokio::sync::{Barrier, oneshot};

    use super::*;
    use crate::{
        ActorIdentity, EffectCommandResult, EffectCoordinator, EffectCreateRequest,
        EffectDueScanPage, MemoryEffectCoordinator, MemoryEffectDispatchGovernor,
    };

    const NOW_MS: u64 = 100;

    struct FixedClock {
        now_ms: AtomicU64,
    }

    impl FixedClock {
        fn new(now_ms: u64) -> Self {
            Self {
                now_ms: AtomicU64::new(now_ms),
            }
        }
    }

    impl EffectExecutorClock for FixedClock {
        fn now_ms(&self) -> Result<u64, HarnessError> {
            Ok(self.now_ms.load(Ordering::SeqCst))
        }
    }

    struct EventuallyUnavailableClock {
        successful_calls: usize,
        calls: AtomicUsize,
        now_ms: u64,
    }

    impl EffectExecutorClock for EventuallyUnavailableClock {
        fn now_ms(&self) -> Result<u64, HarnessError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < self.successful_calls {
                Ok(self.now_ms)
            } else {
                Err(HarnessError::Effect("clock fixture unavailable".to_owned()))
            }
        }
    }

    struct StaticConnector {
        calls: Arc<AtomicUsize>,
        outcome: EffectExecutionOutcome,
    }

    impl EffectConnector for StaticConnector {
        fn descriptor(&self) -> EffectConnectorDescriptor {
            connector_descriptor()
        }

        fn execute<'a>(
            &'a self,
            _request: EffectExecutionRequest,
        ) -> HarnessFuture<'a, EffectExecutionOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self.outcome.clone();
            Box::pin(async move { Ok(outcome) })
        }
    }

    struct PanicConnector {
        calls: Arc<AtomicUsize>,
    }

    impl EffectConnector for PanicConnector {
        fn descriptor(&self) -> EffectConnectorDescriptor {
            connector_descriptor()
        }

        fn execute<'a>(
            &'a self,
            _request: EffectExecutionRequest,
        ) -> HarnessFuture<'a, EffectExecutionOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("connector secret must remain isolated")
        }
    }

    struct HangingConnector {
        calls: Arc<AtomicUsize>,
        entered: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl EffectConnector for HangingConnector {
        fn descriptor(&self) -> EffectConnectorDescriptor {
            connector_descriptor()
        }

        fn execute<'a>(
            &'a self,
            _request: EffectExecutionRequest,
        ) -> HarnessFuture<'a, EffectExecutionOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = self.entered.lock().expect("entered lock").take() {
                let _ = entered.send(());
            }
            Box::pin(future::pending())
        }
    }

    struct ConcurrencyConnector {
        calls: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl EffectConnector for ConcurrencyConnector {
        fn descriptor(&self) -> EffectConnectorDescriptor {
            connector_descriptor()
        }

        fn execute<'a>(
            &'a self,
            request: EffectExecutionRequest,
        ) -> HarnessFuture<'a, EffectExecutionOutcome> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(EffectExecutionOutcome::Applied {
                    receipt: receipt(request.effect_id.as_str()),
                })
            })
        }
    }

    struct BarrierPolicy {
        barrier: Arc<Barrier>,
    }

    impl EffectExecutionPolicy for BarrierPolicy {
        fn authorize<'a>(
            &'a self,
            _request: EffectExecutionPolicyRequest,
        ) -> HarnessFuture<'a, EffectExecutionDecision> {
            Box::pin(async move {
                self.barrier.wait().await;
                Ok(EffectExecutionDecision::Allow)
            })
        }
    }

    struct PanicPolicy;

    impl EffectExecutionPolicy for PanicPolicy {
        fn authorize<'a>(
            &'a self,
            _request: EffectExecutionPolicyRequest,
        ) -> HarnessFuture<'a, EffectExecutionDecision> {
            panic!("policy secret must remain isolated")
        }
    }

    struct HangingPolicy;

    impl EffectExecutionPolicy for HangingPolicy {
        fn authorize<'a>(
            &'a self,
            _request: EffectExecutionPolicyRequest,
        ) -> HarnessFuture<'a, EffectExecutionDecision> {
            Box::pin(future::pending())
        }
    }

    struct UnavailableGovernor;

    impl EffectDispatchGovernor for UnavailableGovernor {
        fn admit_as<'a>(
            &'a self,
            _request: EffectDispatchAdmissionRequest,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision> {
            Box::pin(async {
                Err(HarnessError::Effect(
                    "governor fixture unavailable".to_owned(),
                ))
            })
        }

        fn settle_as<'a>(
            &'a self,
            _admission_id: &'a EffectLeaseId,
            _settlement: EffectDispatchSettlement,
            _settled_at_ms: u64,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    struct InvalidDecisionGovernor;

    impl EffectDispatchGovernor for InvalidDecisionGovernor {
        fn admit_as<'a>(
            &'a self,
            _request: EffectDispatchAdmissionRequest,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision> {
            Box::pin(async { Ok(EffectDispatchAdmissionDecision::RateLimited { retry_at_ms: 1 }) })
        }

        fn settle_as<'a>(
            &'a self,
            _admission_id: &'a EffectLeaseId,
            _settlement: EffectDispatchSettlement,
            _settled_at_ms: u64,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    struct SettlementUnavailableGovernor;

    impl EffectDispatchGovernor for SettlementUnavailableGovernor {
        fn admit_as<'a>(
            &'a self,
            _request: EffectDispatchAdmissionRequest,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision> {
            Box::pin(async { Ok(EffectDispatchAdmissionDecision::Allow) })
        }

        fn settle_as<'a>(
            &'a self,
            _admission_id: &'a EffectLeaseId,
            _settlement: EffectDispatchSettlement,
            _settled_at_ms: u64,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, ()> {
            Box::pin(async {
                Err(HarnessError::Effect(
                    "governor settlement fixture unavailable".to_owned(),
                ))
            })
        }
    }

    struct DescriptorPanicConnector;

    impl EffectConnector for DescriptorPanicConnector {
        fn descriptor(&self) -> EffectConnectorDescriptor {
            panic!("descriptor secret must remain isolated")
        }

        fn execute<'a>(
            &'a self,
            _request: EffectExecutionRequest,
        ) -> HarnessFuture<'a, EffectExecutionOutcome> {
            Box::pin(async {
                Err(HarnessError::Effect(
                    "unreachable descriptor connector".to_owned(),
                ))
            })
        }
    }

    struct PanicApplyCoordinator {
        inner: MemoryEffectCoordinator,
    }

    impl EffectCoordinator for PanicApplyCoordinator {
        fn create_as<'a>(
            &'a self,
            effect_id: EffectId,
            request: EffectCreateRequest,
            applied_at_ms: u64,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectSnapshot> {
            self.inner
                .create_as(effect_id, request, applied_at_ms, authority)
        }

        fn load_as<'a>(
            &'a self,
            effect_id: &'a EffectId,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, Option<EffectSnapshot>> {
            self.inner.load_as(effect_id, authority)
        }

        fn list_as<'a>(
            &'a self,
            status: Option<&'a str>,
            after: Option<&'a EffectPageCursor>,
            limit: usize,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectPage> {
            self.inner.list_as(status, after, limit, authority)
        }

        fn scan_due_as<'a>(
            &'a self,
            at_ms: u64,
            after_effect_id: Option<&'a EffectId>,
            scan_limit: usize,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectDueScanPage> {
            self.inner
                .scan_due_as(at_ms, after_effect_id, scan_limit, authority)
        }

        fn apply_as<'a>(
            &'a self,
            _effect_id: &'a EffectId,
            _expected_revision: u64,
            _command: EffectCommand,
            _applied_at_ms: u64,
            _authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectCommandResult> {
            Box::pin(async { panic!("coordinator secret must remain isolated") })
        }
    }

    fn authority() -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "effect-worker".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority")
    }

    fn connector_descriptor() -> EffectConnectorDescriptor {
        EffectConnectorDescriptor {
            capability: "channel.email".to_owned(),
            api_version: EFFECT_EXECUTOR_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            idempotency: EffectIdempotencyContract::TargetEnforced,
        }
    }

    fn receipt(external_id: &str) -> EffectReceipt {
        EffectReceipt {
            source: "mail.provider".to_owned(),
            external_id: external_id.to_owned(),
            observed_at_ms: NOW_MS,
            response_sha256: "a".repeat(64),
        }
    }

    fn config(execution_timeout_ms: u64, max_concurrency: usize) -> EffectExecutorConfig {
        EffectExecutorConfig {
            scan_limit: 16,
            max_concurrency,
            policy_timeout_ms: 100,
            governor_timeout_ms: 100,
            governor_retry_after_ms: 25,
            execution_timeout_ms,
            settlement_reserve_ms: 100,
            lease_duration_ms: 1_000,
        }
    }

    fn allow_policy() -> Arc<dyn EffectExecutionPolicy> {
        Arc::new(
            AllowListEffectExecutionPolicy::deny_by_default()
                .allow("channel.email", "send")
                .expect("allow policy"),
        )
    }

    fn governor_policy(
        max_dispatches_per_window: u32,
        failure_threshold: u32,
    ) -> EffectDispatchGovernorPolicy {
        EffectDispatchGovernorPolicy {
            policy_id: "test-v1".to_owned(),
            max_dispatches_per_window,
            window_ms: 1_000,
            failure_threshold,
            open_duration_ms: 500,
            probe_lease_ms: 100,
            admission_retention_ms: 604_800_000,
        }
    }

    fn engine() -> EffectEngine {
        EffectEngine::new(Arc::new(MemoryEffectCoordinator::new()))
    }

    fn registry(connector: Arc<dyn EffectConnector>) -> EffectConnectorRegistry {
        let mut registry = EffectConnectorRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, connector)
            .expect("register connector");
        registry
    }

    async fn create_effect(
        engine: &EffectEngine,
        authority: &AuthorityContext,
        id: &str,
        input: Value,
    ) {
        engine
            .create_as(
                EffectId::from_string(id.to_owned()),
                EffectCreateRequest {
                    command_id: EffectCommandId::from_string(format!("create-{id}")),
                    operation: EffectOperation {
                        capability: "channel.email".to_owned(),
                        operation: "send".to_owned(),
                    },
                    idempotency_key: format!("idempotency-secret-{id}"),
                    input,
                    not_before_ms: NOW_MS,
                },
                NOW_MS,
                authority,
            )
            .await
            .expect("create Effect");
    }

    fn request(cycle_id: &str) -> EffectExecutorRunRequest {
        EffectExecutorRunRequest {
            cycle_id: cycle_id.to_owned(),
            after: None,
        }
    }

    #[tokio::test]
    async fn pending_page_validation_rejects_bad_continuation_and_state() {
        let authority = authority();
        let engine = engine();
        create_effect(&engine, &authority, "effect-page", serde_json::json!({})).await;
        let canonical = engine
            .list_as(Some("pending"), None, 16, &authority)
            .await
            .expect("page");

        let mut bad_continuation = canonical.clone();
        bad_continuation.next_cursor = None;
        assert!(
            validate_effect_page(
                "Effect Executor",
                &bad_continuation,
                None,
                16,
                &authority,
                EffectPageState::Pending,
            )
            .is_err()
        );

        let claimed = engine
            .apply_as(
                &EffectId::from_static("effect-page"),
                1,
                EffectCommand {
                    id: EffectCommandId::from_static("claim-page"),
                    kind: EffectCommandKind::Claim {
                        lease_id: EffectLeaseId::from_static("lease-page"),
                        lease_duration_ms: 1_000,
                    },
                },
                NOW_MS,
                &authority,
            )
            .await
            .expect("claim")
            .snapshot;
        let non_pending = EffectPage {
            effects: vec![claimed.clone()],
            next_cursor: Some(EffectPageCursor {
                effect_id: claimed.id().clone(),
            }),
            has_more: false,
        };
        assert!(
            validate_effect_page(
                "Effect Executor",
                &non_pending,
                None,
                16,
                &authority,
                EffectPageState::Pending,
            )
            .is_err()
        );
    }

    #[test]
    fn registration_captures_descriptor_panics_without_mutation() {
        let mut registry = EffectConnectorRegistry::new();
        let error = registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(DescriptorPanicConnector),
            )
            .expect_err("descriptor panic");

        assert!(error.to_string().contains("descriptor"));
        assert!(registry.capabilities().is_empty());
    }

    #[test]
    fn registration_rejects_an_incompatible_connector_api() {
        struct IncompatibleConnector;

        impl EffectConnector for IncompatibleConnector {
            fn descriptor(&self) -> EffectConnectorDescriptor {
                EffectConnectorDescriptor {
                    api_version: EFFECT_EXECUTOR_API_VERSION + 1,
                    ..connector_descriptor()
                }
            }

            fn execute<'a>(
                &'a self,
                _request: EffectExecutionRequest,
            ) -> HarnessFuture<'a, EffectExecutionOutcome> {
                Box::pin(async {
                    Err(HarnessError::Effect(
                        "incompatible connector must not execute".to_owned(),
                    ))
                })
            }
        }

        let mut registry = EffectConnectorRegistry::new();
        let error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(IncompatibleConnector))
            .expect_err("incompatible API");

        assert!(error.to_string().contains("requires API"));
        assert!(registry.capabilities().is_empty());
    }

    #[test]
    fn config_requires_settlement_time_inside_the_lease() {
        let mut invalid = config(900, 1);
        invalid.settlement_reserve_ms = 100;
        assert!(invalid.validate().is_err());

        invalid.execution_timeout_ms = 899;
        assert!(invalid.validate().is_ok());
    }

    #[tokio::test]
    async fn default_policy_denies_before_claim_and_connector_entry() {
        let authority = authority();
        let engine = engine();
        create_effect(&engine, &authority, "effect-denied", serde_json::json!({})).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("denied"),
                },
            })),
        )
        .expect("executor")
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));

        let report = executor
            .run_once_as(
                request("cycle-denied"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::PolicyDenied { .. }
        ));
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-denied"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 1);
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Pending { .. }
        ));
    }

    #[tokio::test]
    async fn applied_execution_is_durable_and_report_is_content_free() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-applied",
            serde_json::json!({"private":"input-secret"}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("provider-42"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");

        let report = executor
            .run_once_as(
                request("cycle-applied"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Applied
        );
        let encoded = serde_json::to_string(&report).expect("report");
        assert!(!encoded.contains("input-secret"));
        assert!(!encoded.contains("idempotency-secret"));
        assert!(!encoded.contains("provider-42"));
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-applied"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 3);
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Applied { .. }
        ));
    }

    #[tokio::test]
    async fn durable_rate_limit_prevents_connector_entry_and_schedules_exact_retry() {
        let authority = authority();
        let engine = engine();
        create_effect(&engine, &authority, "effect-rate-a", serde_json::json!({})).await;
        create_effect(&engine, &authority, "effect-rate-b", serde_json::json!({})).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("rate"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(
            Arc::new(MemoryEffectDispatchGovernor::new()),
            governor_policy(1, 2),
        )
        .expect("governor");

        let report = executor
            .run_once_as(request("cycle-rate"), &authority, CancellationToken::new())
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Applied
        );
        assert_eq!(
            report.attempts[1].outcome,
            EffectExecutorAttemptOutcome::RateLimited { retry_at_ms: 1_000 }
        );
        let retry = engine
            .load_as(&EffectId::from_static("effect-rate-b"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            retry.effect().status(),
            EffectStatus::Pending {
                next_attempt: 2,
                not_before_ms: 1_000
            }
        ));
    }

    #[tokio::test]
    async fn availability_failure_opens_circuit_without_parsing_reason_strings() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-circuit-a",
            serde_json::json!({}),
        )
        .await;
        create_effect(
            &engine,
            &authority,
            "effect-circuit-b",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(PanicConnector {
                calls: calls.clone(),
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(
            Arc::new(MemoryEffectDispatchGovernor::new()),
            governor_policy(10, 1),
        )
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-circuit"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Unknown {
                reason_code: "connector.failed".to_owned(),
            }
        );
        assert_eq!(
            report.attempts[1].outcome,
            EffectExecutorAttemptOutcome::CircuitOpen { retry_at_ms: 600 }
        );
    }

    #[tokio::test]
    async fn connector_returned_unknown_is_healthy_transport_evidence() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-unknown-a",
            serde_json::json!({}),
        )
        .await;
        create_effect(
            &engine,
            &authority,
            "effect-unknown-b",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Unknown {
                    reason_code: "target.uncertain".to_owned(),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(
            Arc::new(MemoryEffectDispatchGovernor::new()),
            governor_policy(10, 1),
        )
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-unknown-health"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(report.attempts.iter().all(|attempt| {
            attempt.outcome
                == EffectExecutorAttemptOutcome::Unknown {
                    reason_code: "target.uncertain".to_owned(),
                }
        }));
    }

    #[tokio::test]
    async fn unavailable_governor_fails_closed_after_claim_without_connector_entry() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-governor-unavailable",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("must-not-enter"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(Arc::new(UnavailableGovernor), governor_policy(10, 2))
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-governor-unavailable"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::GovernorUnavailable { retry_at_ms: 125 }
        );
        let retry = engine
            .load_as(
                &EffectId::from_static("effect-governor-unavailable"),
                &authority,
            )
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            retry.effect().status(),
            EffectStatus::Pending {
                next_attempt: 2,
                not_before_ms: 125
            }
        ));
    }

    #[tokio::test]
    async fn invalid_governor_decision_fails_closed_without_connector_entry() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-governor-invalid",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("must-not-enter-invalid"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(Arc::new(InvalidDecisionGovernor), governor_policy(10, 2))
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-governor-invalid"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::GovernorUnavailable { retry_at_ms: 125 }
        );
    }

    #[tokio::test]
    async fn governor_settlement_failure_is_visible_without_corrupting_effect_truth() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-governor-settlement",
            serde_json::json!({}),
        )
        .await;
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("settlement"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(
            Arc::new(SettlementUnavailableGovernor),
            governor_policy(10, 2),
        )
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-governor-settlement"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Applied
        );
        assert!(report.attempts[0].governor_settlement_failed);
        let effect = engine
            .load_as(
                &EffectId::from_static("effect-governor-settlement"),
                &authority,
            )
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            effect.effect().status(),
            EffectStatus::Applied { .. }
        ));
    }

    #[tokio::test]
    async fn clock_failure_after_governed_dispatch_reports_unsettled_governor_health() {
        let authority = authority();
        let engine = engine();
        create_effect(
            &engine,
            &authority,
            "effect-governor-clock",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("clock-failure"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(EventuallyUnavailableClock {
            successful_calls: 3,
            calls: AtomicUsize::new(0),
            now_ms: NOW_MS,
        }))
        .with_config(config(100, 1))
        .expect("config")
        .with_dispatch_governor(
            Arc::new(MemoryEffectDispatchGovernor::new()),
            governor_policy(10, 2),
        )
        .expect("governor");

        let report = executor
            .run_once_as(
                request("cycle-governor-clock"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::ClockUnavailableAfterClaim
        );
        assert!(report.attempts[0].governor_settlement_failed);
    }

    #[tokio::test]
    async fn not_applied_preserves_reason_and_schedules_exact_retry() {
        let authority = authority();
        let engine = engine();
        create_effect(&engine, &authority, "effect-retry", serde_json::json!({})).await;
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectExecutionOutcome::NotApplied {
                    reason_code: "provider.not_ready".to_owned(),
                    retry_after_ms: Some(25),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");

        let report = executor
            .run_once_as(request("cycle-retry"), &authority, CancellationToken::new())
            .await
            .expect("run");

        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::RetryScheduled {
                reason_code: "provider.not_ready".to_owned(),
                retry_at_ms: 125,
            }
        );
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-retry"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Pending {
                next_attempt: 2,
                not_before_ms: 125
            }
        ));
    }

    #[tokio::test]
    async fn connector_panic_and_invalid_evidence_become_unknown() {
        let authority = authority();

        let panic_engine = engine();
        create_effect(
            &panic_engine,
            &authority,
            "effect-panic",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let panic_executor = EffectExecutor::new(
            panic_engine.clone(),
            registry(Arc::new(PanicConnector {
                calls: calls.clone(),
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");
        let panic_report = panic_executor
            .run_once_as(request("cycle-panic"), &authority, CancellationToken::new())
            .await
            .expect("run panic");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            panic_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Unknown {
                reason_code: "connector.failed".to_owned(),
            }
        );

        let invalid_engine = engine();
        create_effect(
            &invalid_engine,
            &authority,
            "effect-invalid",
            serde_json::json!({}),
        )
        .await;
        let invalid_executor = EffectExecutor::new(
            invalid_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: EffectReceipt {
                        response_sha256: "not-a-digest".to_owned(),
                        ..receipt("invalid")
                    },
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");
        let invalid_report = invalid_executor
            .run_once_as(
                request("cycle-invalid"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run invalid");
        assert_eq!(
            invalid_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Unknown {
                reason_code: "connector.invalid_outcome".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn timeout_and_cancellation_after_dispatch_become_unknown() {
        let authority = authority();

        let timeout_engine = engine();
        create_effect(
            &timeout_engine,
            &authority,
            "effect-timeout",
            serde_json::json!({}),
        )
        .await;
        let timeout_executor = EffectExecutor::new(
            timeout_engine,
            registry(Arc::new(HangingConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                entered: Mutex::new(None),
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(1, 1))
        .expect("config");
        let timeout_report = timeout_executor
            .run_once_as(
                request("cycle-timeout"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("timeout run");
        assert_eq!(
            timeout_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Unknown {
                reason_code: "connector.timeout".to_owned(),
            }
        );

        let cancel_engine = engine();
        create_effect(
            &cancel_engine,
            &authority,
            "effect-cancel",
            serde_json::json!({}),
        )
        .await;
        let (entered_tx, entered_rx) = oneshot::channel();
        let cancel_executor = EffectExecutor::new(
            cancel_engine,
            registry(Arc::new(HangingConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                entered: Mutex::new(Some(entered_tx)),
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");
        let cancellation = CancellationToken::new();
        let run_cancellation = cancellation.clone();
        let run_authority = authority.clone();
        let task = tokio::spawn(async move {
            cancel_executor
                .run_once_as(request("cycle-cancel"), &run_authority, run_cancellation)
                .await
        });
        entered_rx.await.expect("connector entered");
        cancellation.cancel();
        let cancel_report = task.await.expect("join").expect("cancel run");
        assert_eq!(
            cancel_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::Unknown {
                reason_code: "executor.cancelled_after_dispatch".to_owned(),
            }
        );
    }

    #[tokio::test]
    async fn same_cycle_concurrent_consumers_enter_connector_once() {
        let authority = authority();
        let engine = engine();
        create_effect(&engine, &authority, "effect-race", serde_json::json!({})).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("race"),
                },
            })),
        )
        .expect("executor")
        .with_policy(Arc::new(BarrierPolicy {
            barrier: Arc::new(Barrier::new(2)),
        }))
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");

        let left =
            executor.run_once_as(request("same-cycle"), &authority, CancellationToken::new());
        let right =
            executor.run_once_as(request("same-cycle"), &authority, CancellationToken::new());
        let (left, right) = tokio::join!(left, right);
        let outcomes = [
            left.expect("left").attempts[0].outcome.clone(),
            right.expect("right").attempts[0].outcome.clone(),
        ];

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(outcomes.contains(&EffectExecutorAttemptOutcome::Applied));
        assert!(outcomes.contains(&EffectExecutorAttemptOutcome::ClaimAlreadyCommitted));
    }

    #[tokio::test]
    async fn sweep_honors_concurrency_bound_and_source_order() {
        let authority = authority();
        let engine = engine();
        for index in 0..5 {
            create_effect(
                &engine,
                &authority,
                &format!("effect-{index}"),
                serde_json::json!({}),
            )
            .await;
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine,
            registry(Arc::new(ConcurrencyConnector {
                calls: calls.clone(),
                in_flight: in_flight.clone(),
                max_in_flight: max_in_flight.clone(),
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 2))
        .expect("config");

        let report = executor
            .run_once_as(
                request("cycle-concurrency"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        let ids = report
            .attempts
            .iter()
            .map(|attempt| attempt.effect_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(calls.load(Ordering::SeqCst), 5);
        assert_eq!(in_flight.load(Ordering::SeqCst), 0);
        assert_eq!(max_in_flight.load(Ordering::SeqCst), 2);
        assert_eq!(
            ids,
            vec!["effect-0", "effect-1", "effect-2", "effect-3", "effect-4"]
        );
    }

    #[tokio::test]
    async fn unavailable_connector_and_panicking_policy_do_not_claim() {
        let authority = authority();
        let unavailable_engine = engine();
        create_effect(
            &unavailable_engine,
            &authority,
            "effect-unavailable",
            serde_json::json!({}),
        )
        .await;
        let unavailable =
            EffectExecutor::new(unavailable_engine.clone(), EffectConnectorRegistry::new())
                .expect("executor")
                .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let report = unavailable
            .run_once_as(
                request("cycle-unavailable"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::ConnectorUnavailable
        );

        let policy_engine = engine();
        create_effect(
            &policy_engine,
            &authority,
            "effect-policy-panic",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let policy_executor = EffectExecutor::new(
            policy_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("policy"),
                },
            })),
        )
        .expect("executor")
        .with_policy(Arc::new(PanicPolicy))
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let report = policy_executor
            .run_once_as(
                request("cycle-policy-panic"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::PolicyUnavailable
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let snapshot = policy_engine
            .load_as(&EffectId::from_static("effect-policy-panic"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 1);
    }

    #[tokio::test]
    async fn policy_timeout_and_precancelled_sweep_do_not_claim() {
        let authority = authority();

        let timeout_engine = engine();
        create_effect(
            &timeout_engine,
            &authority,
            "effect-policy-timeout",
            serde_json::json!({}),
        )
        .await;
        let timeout_calls = Arc::new(AtomicUsize::new(0));
        let mut timeout_config = config(100, 1);
        timeout_config.policy_timeout_ms = 1;
        let timeout_executor = EffectExecutor::new(
            timeout_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: timeout_calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("policy-timeout"),
                },
            })),
        )
        .expect("executor")
        .with_policy(Arc::new(HangingPolicy))
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(timeout_config)
        .expect("config");
        let timeout_report = timeout_executor
            .run_once_as(
                request("cycle-policy-timeout"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("timeout run");
        assert_eq!(
            timeout_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::PolicyUnavailable
        );
        assert_eq!(timeout_calls.load(Ordering::SeqCst), 0);

        let cancelled_engine = engine();
        create_effect(
            &cancelled_engine,
            &authority,
            "effect-precancelled",
            serde_json::json!({}),
        )
        .await;
        let cancelled_calls = Arc::new(AtomicUsize::new(0));
        let cancelled_executor = EffectExecutor::new(
            cancelled_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: cancelled_calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("precancelled"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled_report = cancelled_executor
            .run_once_as(request("cycle-precancelled"), &authority, cancellation)
            .await
            .expect("cancelled run");
        assert_eq!(
            cancelled_report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::CancelledBeforeClaim
        );
        assert_eq!(cancelled_calls.load(Ordering::SeqCst), 0);

        for (engine, id) in [
            (&timeout_engine, "effect-policy-timeout"),
            (&cancelled_engine, "effect-precancelled"),
        ] {
            let snapshot = engine
                .load_as(&EffectId::from_string(id.to_owned()), &authority)
                .await
                .expect("load")
                .expect("Effect");
            assert_eq!(snapshot.revision(), 1);
        }
    }

    #[tokio::test]
    async fn unexpected_worker_panic_is_content_free_and_state_remains_authoritative() {
        let authority = authority();
        let engine = EffectEngine::new(Arc::new(PanicApplyCoordinator {
            inner: MemoryEffectCoordinator::new(),
        }));
        create_effect(
            &engine,
            &authority,
            "effect-worker-panic",
            serde_json::json!({"private":"worker-secret"}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let executor = EffectExecutor::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectExecutionOutcome::Applied {
                    receipt: receipt("worker-panic"),
                },
            })),
        )
        .expect("executor")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));

        let report = executor
            .run_once_as(
                request("cycle-worker-panic"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");

        assert_eq!(
            report.attempts[0].outcome,
            EffectExecutorAttemptOutcome::AttemptFailed
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            !serde_json::to_string(&report)
                .expect("report")
                .contains("worker-secret")
        );
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-worker-panic"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 1);
    }
}
