//! Host-driven, policy-gated reconciliation of uncertain external Effects.
//!
//! Reconciliation performs authoritative read-only lookup and settles through
//! the existing Effect revision CAS. It owns no polling thread, work lease,
//! credential store, or target-specific truth model.

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

use super::{
    EffectApplyOutcome, EffectCommand, EffectCommandKind, EffectEngine, EffectOperation,
    EffectPageCursor, EffectReceipt, EffectSnapshot, EffectStatus,
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

/// Exact embedded Governed Effect Reconciler API coordinate.
pub const EFFECT_RECONCILER_API_VERSION: u32 = 1;

const MAX_CONNECTORS: usize = 256;
const MAX_OPERATIONS_PER_CONNECTOR: usize = 256;
const MAX_DESCRIPTOR_BYTES: usize = 65_536;
const MAX_SCAN_LIMIT: usize = 256;
const MAX_CONCURRENCY: usize = 64;
const MIN_POLICY_TIMEOUT_MS: u64 = 1;
const MAX_POLICY_TIMEOUT_MS: u64 = 60_000;
const MIN_LOOKUP_TIMEOUT_MS: u64 = 1;
const MAX_LOOKUP_TIMEOUT_MS: u64 = 604_800_000;
const MAX_RETRY_AFTER_MS: u64 = 604_800_000;
const DEFAULT_SCAN_LIMIT: usize = 64;
const DEFAULT_CONCURRENCY: usize = 8;
const DEFAULT_POLICY_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_LOOKUP_TIMEOUT_MS: u64 = 60_000;

/// Safety contract required of every reconciliation Connector.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectReconciliationContract {
    /// Query authoritative target state without causing or retrying the Effect.
    AuthoritativeReadOnly,
}

/// Frozen registration metadata for one reconciliation Connector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReconciliationConnectorDescriptor {
    /// Exact capability routed to this Connector.
    pub capability: String,
    /// Exact Governed Effect Reconciler contract implemented by the Connector.
    pub api_version: u32,
    /// Explicit supported operation names; no wildcard is permitted.
    pub operations: BTreeSet<String>,
    /// Explicit authoritative and side-effect-free lookup contract.
    pub contract: EffectReconciliationContract,
}

/// Input supplied only after Policy approval.
///
/// This type intentionally has neither `Debug` nor serialization so immutable
/// Effect input and the idempotency key do not enter ambient diagnostics.
#[derive(Clone)]
pub struct EffectReconciliationRequest {
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Trusted lookup identity and tenant boundary.
    pub authority: AuthorityContext,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Target-system duplicate-suppression key used only for lookup.
    pub idempotency_key: String,
    /// Immutable bounded external request.
    pub input: Value,
    /// SHA-256 of `input`.
    pub input_sha256: String,
    /// Exact uncertain attempt.
    pub attempt: u32,
    /// Exact uncertain execution fence.
    pub lease_id: EffectLeaseId,
    /// Cooperative cancellation raised on timeout or host cancellation.
    pub cancellation: CancellationToken,
}

/// Authoritative read-only observation returned by a Connector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectReconciliationOutcome {
    /// Target state proves the external Effect was applied.
    Applied {
        /// Content-free target evidence.
        receipt: EffectReceipt,
    },
    /// Target state proves the external Effect was not applied.
    NotApplied {
        /// Content-free classification.
        reason_code: String,
        /// Optional bounded delay before a later execution attempt is eligible.
        retry_after_ms: Option<u64>,
    },
    /// Target state is still insufficient to prove either terminal fact.
    StillUnknown {
        /// Content-free uncertainty classification.
        reason_code: String,
    },
}

/// Authoritative, side-effect-free external-state lookup.
///
/// The implementation must not create, retry, compensate, or otherwise mutate
/// the external Effect. Hosts may repeat a lookup after an uncertain response
/// or concurrently on another node.
pub trait EffectReconciliationConnector: Send + Sync {
    /// Returns registration metadata captured exactly once.
    fn descriptor(&self) -> EffectReconciliationConnectorDescriptor;

    /// Queries one exact uncertain attempt without causing external mutation.
    fn query<'a>(
        &'a self,
        request: EffectReconciliationRequest,
    ) -> HarnessFuture<'a, EffectReconciliationOutcome>;
}

/// Connector paired with its frozen trust origin and metadata.
#[derive(Clone)]
pub struct RegisteredEffectReconciliationConnector {
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Frozen routing and lookup contract.
    pub descriptor: EffectReconciliationConnectorDescriptor,
    /// Read-only implementation.
    pub connector: Arc<dyn EffectReconciliationConnector>,
}

/// Deterministic, collision-safe reconciliation Connector registry.
#[derive(Default)]
pub struct EffectReconciliationConnectorRegistry {
    connectors: BTreeMap<String, RegisteredEffectReconciliationConnector>,
}

impl EffectReconciliationConnectorRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Captures, validates, and registers one Connector atomically.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        connector: Arc<dyn EffectReconciliationConnector>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        if self.connectors.len() >= MAX_CONNECTORS {
            return Err(HarnessError::Effect(format!(
                "Effect Reconciliation Connector registry exceeds {MAX_CONNECTORS} entries"
            )));
        }
        let descriptor =
            capture_capability_metadata("Effect Reconciliation Connector descriptor", || {
                connector.descriptor()
            })?;
        validate_descriptor(&descriptor)?;
        if self.connectors.contains_key(&descriptor.capability) {
            return Err(HarnessError::DuplicateCapability(
                descriptor.capability.clone(),
            ));
        }
        self.connectors.insert(
            descriptor.capability.clone(),
            RegisteredEffectReconciliationConnector {
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
    pub fn resolve(
        &self,
        operation: &EffectOperation,
    ) -> Option<&RegisteredEffectReconciliationConnector> {
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

/// Content supplied to reconciliation Policy before target lookup.
///
/// This type intentionally omits `Debug` and serialization because Policy may
/// inspect the complete immutable Effect request.
#[derive(Clone)]
pub struct EffectReconciliationPolicyRequest {
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
    /// Exact uncertain attempt.
    pub attempt: u32,
    /// Exact uncertain execution fence.
    pub lease_id: EffectLeaseId,
    /// Trust origin of the selected Connector.
    pub connector_origin: CapabilityOrigin,
    /// Frozen lookup safety contract.
    pub contract: EffectReconciliationContract,
}

/// Pre-query reconciliation Policy result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectReconciliationDecision {
    /// Permit one authoritative read-only lookup.
    Allow,
    /// Refuse lookup without mutating the Effect.
    Deny {
        /// Content-free denial classification.
        reason_code: String,
    },
}

/// Host-selected pre-query reconciliation Policy.
pub trait EffectReconciliationPolicy: Send + Sync {
    /// Authorizes or denies one exact uncertain Effect and frozen Connector.
    fn authorize<'a>(
        &'a self,
        request: EffectReconciliationPolicyRequest,
    ) -> HarnessFuture<'a, EffectReconciliationDecision>;
}

/// Safe default Policy that never permits external lookup.
#[derive(Default)]
pub struct DenyAllEffectReconciliations;

impl EffectReconciliationPolicy for DenyAllEffectReconciliations {
    fn authorize<'a>(
        &'a self,
        _request: EffectReconciliationPolicyRequest,
    ) -> HarnessFuture<'a, EffectReconciliationDecision> {
        Box::pin(async {
            Ok(EffectReconciliationDecision::Deny {
                reason_code: "policy.denied".to_owned(),
            })
        })
    }
}

/// Exact capability/operation allowlist with no wildcard or fallback.
#[derive(Default)]
pub struct AllowListEffectReconciliationPolicy {
    allowed: BTreeSet<(String, String)>,
}

impl AllowListEffectReconciliationPolicy {
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
        validate_capability_name("Effect Reconciliation Policy capability", &capability)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
        validate_capability_name("Effect Reconciliation Policy operation", &operation)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
        self.allowed.insert((capability, operation));
        Ok(self)
    }
}

impl EffectReconciliationPolicy for AllowListEffectReconciliationPolicy {
    fn authorize<'a>(
        &'a self,
        request: EffectReconciliationPolicyRequest,
    ) -> HarnessFuture<'a, EffectReconciliationDecision> {
        Box::pin(async move {
            if self
                .allowed
                .contains(&(request.operation.capability, request.operation.operation))
            {
                Ok(EffectReconciliationDecision::Allow)
            } else {
                Ok(EffectReconciliationDecision::Deny {
                    reason_code: "policy.denied".to_owned(),
                })
            }
        })
    }
}

/// Trusted time source installed by an embedding host.
pub trait EffectReconcilerClock: Send + Sync {
    /// Returns positive Unix milliseconds.
    fn now_ms(&self) -> Result<u64, HarnessError>;
}

/// Wall-clock implementation for ordinary single-host embedding.
#[derive(Default)]
pub struct SystemEffectReconcilerClock;

impl EffectReconcilerClock for SystemEffectReconcilerClock {
    fn now_ms(&self) -> Result<u64, HarnessError> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| HarnessError::Effect("Effect Reconciler clock precedes epoch".to_owned()))?
            .as_millis();
        u64::try_from(millis)
            .map_err(|_| HarnessError::Effect("Effect Reconciler clock exceeds u64".to_owned()))
    }
}

/// Bounded host policy for one Reconciler instance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct EffectReconcilerConfig {
    /// Unknown Effects inspected per sweep.
    pub scan_limit: usize,
    /// Maximum concurrent Policy/lookup attempts.
    pub max_concurrency: usize,
    /// Maximum Policy duration before fail-closed denial.
    pub policy_timeout_ms: u64,
    /// Maximum authoritative lookup duration.
    pub lookup_timeout_ms: u64,
}

impl Default for EffectReconcilerConfig {
    fn default() -> Self {
        Self {
            scan_limit: DEFAULT_SCAN_LIMIT,
            max_concurrency: DEFAULT_CONCURRENCY,
            policy_timeout_ms: DEFAULT_POLICY_TIMEOUT_MS,
            lookup_timeout_ms: DEFAULT_LOOKUP_TIMEOUT_MS,
        }
    }
}

impl EffectReconcilerConfig {
    /// Validates scan, concurrency, and timeout bounds.
    pub fn validate(&self) -> Result<(), HarnessError> {
        if !(1..=MAX_SCAN_LIMIT).contains(&self.scan_limit) {
            return Err(HarnessError::Effect(format!(
                "Effect Reconciler scan_limit must be 1-{MAX_SCAN_LIMIT}"
            )));
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::Effect(format!(
                "Effect Reconciler max_concurrency must be 1-{MAX_CONCURRENCY}"
            )));
        }
        if !(MIN_POLICY_TIMEOUT_MS..=MAX_POLICY_TIMEOUT_MS).contains(&self.policy_timeout_ms) {
            return Err(HarnessError::Effect(format!(
                "Effect Reconciler policy_timeout_ms must be {MIN_POLICY_TIMEOUT_MS}-{MAX_POLICY_TIMEOUT_MS}"
            )));
        }
        if !(MIN_LOOKUP_TIMEOUT_MS..=MAX_LOOKUP_TIMEOUT_MS).contains(&self.lookup_timeout_ms) {
            return Err(HarnessError::Effect(format!(
                "Effect Reconciler lookup_timeout_ms must be {MIN_LOOKUP_TIMEOUT_MS}-{MAX_LOOKUP_TIMEOUT_MS}"
            )));
        }
        Ok(())
    }
}

/// Caller-stable sweep identity and disposable unknown-page cursor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReconcilerRunRequest {
    /// Stable identity reused only when retrying the same uncertain sweep call.
    pub cycle_id: String,
    /// Exclusive unknown-Effect cursor, or `None` to begin a sweep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<EffectPageCursor>,
}

/// Content-free result of one uncertain Effect considered by a sweep.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectReconcilerAttemptOutcome {
    /// No exact registered reconciliation Connector supports the operation.
    ConnectorUnavailable,
    /// Policy denied lookup.
    PolicyDenied {
        /// Content-free Policy classification.
        reason_code: String,
    },
    /// Policy failed, panicked, timed out, or returned invalid evidence.
    PolicyUnavailable,
    /// Host cancellation was observed before target lookup.
    CancelledBeforeLookup,
    /// Host cancellation was observed after target lookup began.
    CancelledDuringLookup,
    /// Connector failed, panicked, or timed out; state remains unknown.
    LookupUnavailable,
    /// Connector returned structurally invalid evidence; state remains unknown.
    InvalidEvidence,
    /// Connector still cannot prove whether the Effect occurred.
    StillUnknown {
        /// Content-free uncertainty classification.
        reason_code: String,
    },
    /// Trusted time failed after authoritative evidence arrived.
    ClockUnavailable,
    /// Reconciliation proved that the Effect was applied.
    Applied,
    /// Reconciliation proved no Effect and selected terminal rejection.
    Rejected {
        /// Content-free classification.
        reason_code: String,
    },
    /// Reconciliation proved no Effect and selected a later attempt.
    RetryScheduled {
        /// Content-free classification.
        reason_code: String,
        /// Absolute trusted eligibility time.
        retry_at_ms: u64,
    },
    /// The exact deterministic settlement was already committed.
    AlreadyReconciled,
    /// Another mutation won the observed unknown revision.
    SettlementFenced {
        /// Current durable revision.
        actual_revision: u64,
    },
    /// Settlement failed without exposing persistence content.
    SettlementFailed,
    /// One bounded worker stopped unexpectedly; durable state remains authoritative.
    AttemptFailed,
}

/// One source-ordered Reconciler attempt report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReconcilerAttempt {
    /// Stable durable Effect identity.
    pub effect_id: EffectId,
    /// Revision observed during the unknown scan.
    pub observed_revision: u64,
    /// Exact uncertain attempt.
    pub attempt: u32,
    /// Exact uncertain execution fence.
    pub lease_id: EffectLeaseId,
    /// Content-free reconciliation result.
    pub outcome: EffectReconcilerAttemptOutcome,
}

/// One bounded host-driven reconciliation sweep report.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReconcilerRunReport {
    /// Trusted time captured before the unknown scan.
    pub scanned_at_ms: u64,
    /// Number of authoritative unknown records inspected.
    pub scanned: usize,
    /// Whether another unknown identity remains in the current sweep.
    pub has_more: bool,
    /// Disposable continuation, reset to `None` at sweep completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<EffectPageCursor>,
    /// Results in stable source identity order.
    pub attempts: Vec<EffectReconcilerAttempt>,
}

/// Optional host module that reconciles durable unknown Effect attempts.
#[derive(Clone)]
pub struct EffectReconciler {
    engine: EffectEngine,
    connectors: Arc<EffectReconciliationConnectorRegistry>,
    policy: Arc<dyn EffectReconciliationPolicy>,
    clock: Arc<dyn EffectReconcilerClock>,
    config: EffectReconcilerConfig,
}

impl EffectReconciler {
    /// Creates a default-deny Reconciler with the system clock.
    pub fn new(
        engine: EffectEngine,
        connectors: EffectReconciliationConnectorRegistry,
    ) -> Result<Self, HarnessError> {
        let config = EffectReconcilerConfig::default();
        config.validate()?;
        Ok(Self {
            engine,
            connectors: Arc::new(connectors),
            policy: Arc::new(DenyAllEffectReconciliations),
            clock: Arc::new(SystemEffectReconcilerClock),
            config,
        })
    }

    /// Installs a host-selected pre-query Policy.
    #[must_use]
    pub fn with_policy(mut self, policy: Arc<dyn EffectReconciliationPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Installs a trusted host clock.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn EffectReconcilerClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Installs validated bounded reconciliation policy.
    pub fn with_config(mut self, config: EffectReconcilerConfig) -> Result<Self, HarnessError> {
        config.validate()?;
        self.config = config;
        Ok(self)
    }

    /// Runs one unscoped bounded unknown sweep.
    pub async fn run_once(
        &self,
        request: EffectReconcilerRunRequest,
        cancellation: CancellationToken,
    ) -> Result<EffectReconcilerRunReport, HarnessError> {
        self.run_once_as(request, &AuthorityContext::local_process(), cancellation)
            .await
    }

    /// Runs one bounded unknown sweep inside an exact trusted authority.
    pub async fn run_once_as(
        &self,
        request: EffectReconcilerRunRequest,
        authority: &AuthorityContext,
        cancellation: CancellationToken,
    ) -> Result<EffectReconcilerRunReport, HarnessError> {
        validate_run_request(&request, authority)?;
        self.config.validate()?;
        let scanned_at_ms = trusted_now(self.clock.as_ref())?;
        let page = self
            .engine
            .list_as(
                Some("unknown"),
                request.after.as_ref(),
                self.config.scan_limit,
                authority,
            )
            .await?;
        validate_effect_page(
            "Effect Reconciler",
            &page,
            request.after.as_ref(),
            self.config.scan_limit,
            authority,
            EffectPageState::Unknown,
        )?;

        let prepared = page
            .effects
            .iter()
            .map(PreparedReconciliation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let context = ReconcilerRunContext {
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
        let mut attempts = Vec::with_capacity(page.effects.len());
        while !workers.is_empty() {
            let joined = workers.join_next_with_id().await.ok_or_else(|| {
                HarnessError::Effect("Effect Reconciler lost its bounded worker set".to_owned())
            })?;
            let task_id = match &joined {
                Ok((task_id, _)) => *task_id,
                Err(error) => error.id(),
            };
            let (fallback_index, fallback) = fallbacks.remove(&task_id).ok_or_else(|| {
                HarnessError::Effect("Effect Reconciler lost a worker identity".to_owned())
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

        Ok(EffectReconcilerRunReport {
            scanned_at_ms,
            scanned: page.effects.len(),
            has_more: page.has_more,
            next_after: page.has_more.then_some(page.next_cursor).flatten(),
            attempts: attempts.into_iter().map(|(_, attempt)| attempt).collect(),
        })
    }

    fn spawn_prepared(
        &self,
        workers: &mut JoinSet<(usize, EffectReconcilerAttempt)>,
        fallbacks: &mut HashMap<tokio::task::Id, (usize, EffectReconcilerAttempt)>,
        index: usize,
        prepared: PreparedReconciliation,
        context: ReconcilerRunContext,
    ) {
        let fallback = prepared.report(EffectReconcilerAttemptOutcome::AttemptFailed);
        let reconciler = self.clone();
        let handle = workers.spawn(async move {
            (
                index,
                reconciler
                    .reconcile_prepared(
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

    async fn reconcile_prepared(
        &self,
        cycle_id: &str,
        prepared: PreparedReconciliation,
        authority: &AuthorityContext,
        cancellation: CancellationToken,
    ) -> EffectReconcilerAttempt {
        let operation = prepared.snapshot.effect().operation();
        let Some(registered) = self.connectors.resolve(operation).cloned() else {
            return prepared.report(EffectReconcilerAttemptOutcome::ConnectorUnavailable);
        };
        if cancellation.is_cancelled() {
            return prepared.report(EffectReconcilerAttemptOutcome::CancelledBeforeLookup);
        }
        let policy_request = EffectReconciliationPolicyRequest {
            effect_id: prepared.snapshot.id().clone(),
            authority: authority.clone(),
            operation: operation.clone(),
            input: prepared.snapshot.effect().input().clone(),
            input_sha256: prepared.snapshot.effect().input_sha256().to_owned(),
            attempt: prepared.attempt,
            lease_id: prepared.lease_id.clone(),
            connector_origin: registered.origin.clone(),
            contract: registered.descriptor.contract,
        };
        let decision = match isolate_future(|| self.policy.authorize(policy_request), None) {
            Err(()) => {
                return prepared.report(EffectReconcilerAttemptOutcome::PolicyUnavailable);
            }
            Ok(future) => {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return prepared.report(
                            EffectReconcilerAttemptOutcome::CancelledBeforeLookup
                        );
                    }
                    result = timeout(
                        Duration::from_millis(self.config.policy_timeout_ms),
                        future,
                    ) => {
                        match result {
                            Ok(Ok(Ok(decision))) => decision,
                            Ok(Ok(Err(_))) | Ok(Err(())) | Err(_) => {
                                return prepared.report(
                                    EffectReconcilerAttemptOutcome::PolicyUnavailable
                                );
                            }
                        }
                    }
                }
            }
        };
        match decision {
            EffectReconciliationDecision::Deny { reason_code } => {
                if validate_capability_name(
                    "Effect Reconciliation Policy denial reason",
                    &reason_code,
                )
                .is_err()
                {
                    return prepared.report(EffectReconcilerAttemptOutcome::PolicyUnavailable);
                }
                return prepared
                    .report(EffectReconcilerAttemptOutcome::PolicyDenied { reason_code });
            }
            EffectReconciliationDecision::Allow => {}
        }
        if cancellation.is_cancelled() {
            return prepared.report(EffectReconcilerAttemptOutcome::CancelledBeforeLookup);
        }

        let connector_cancellation = CancellationToken::new();
        let query_request = EffectReconciliationRequest {
            effect_id: prepared.snapshot.id().clone(),
            authority: authority.clone(),
            operation: operation.clone(),
            idempotency_key: prepared.snapshot.effect().idempotency_key().to_owned(),
            input: prepared.snapshot.effect().input().clone(),
            input_sha256: prepared.snapshot.effect().input_sha256().to_owned(),
            attempt: prepared.attempt,
            lease_id: prepared.lease_id.clone(),
            cancellation: connector_cancellation.clone(),
        };
        let outcome = match isolate_future(
            || registered.connector.query(query_request),
            Some(connector_cancellation),
        ) {
            Err(()) => {
                return prepared.report(EffectReconcilerAttemptOutcome::LookupUnavailable);
            }
            Ok(future) => {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return prepared.report(
                            EffectReconcilerAttemptOutcome::CancelledDuringLookup
                        );
                    }
                    result = timeout(
                        Duration::from_millis(self.config.lookup_timeout_ms),
                        future,
                    ) => {
                        match result {
                            Ok(Ok(Ok(outcome))) => outcome,
                            Ok(Ok(Err(_))) | Ok(Err(())) | Err(_) => {
                                return prepared.report(
                                    EffectReconcilerAttemptOutcome::LookupUnavailable
                                );
                            }
                        }
                    }
                }
            }
        };

        match validate_outcome(&outcome) {
            Ok(()) => {}
            Err(()) => {
                return prepared.report(EffectReconcilerAttemptOutcome::InvalidEvidence);
            }
        }
        let EffectReconciliationOutcome::StillUnknown { reason_code } = &outcome else {
            return self.settle(cycle_id, prepared, outcome, authority).await;
        };
        prepared.report(EffectReconcilerAttemptOutcome::StillUnknown {
            reason_code: reason_code.clone(),
        })
    }

    async fn settle(
        &self,
        cycle_id: &str,
        prepared: PreparedReconciliation,
        outcome: EffectReconciliationOutcome,
        authority: &AuthorityContext,
    ) -> EffectReconcilerAttempt {
        let settled_at_ms = match trusted_now(self.clock.as_ref()) {
            Ok(now) => now,
            Err(_) => {
                return prepared.report(EffectReconcilerAttemptOutcome::ClockUnavailable);
            }
        };
        let (purpose, kind) = match &outcome {
            EffectReconciliationOutcome::Applied { receipt } => {
                if validate_receipt(receipt, settled_at_ms).is_err() {
                    return prepared.report(EffectReconcilerAttemptOutcome::InvalidEvidence);
                }
                (
                    "applied",
                    EffectCommandKind::ReconcileApplied {
                        lease_id: prepared.lease_id.clone(),
                        attempt: prepared.attempt,
                        receipt: receipt.clone(),
                    },
                )
            }
            EffectReconciliationOutcome::NotApplied {
                reason_code,
                retry_after_ms,
            } => {
                let retry_at_ms = match retry_after_ms {
                    Some(delay) => match settled_at_ms.checked_add(*delay) {
                        Some(value) => Some(value),
                        None => {
                            return prepared
                                .report(EffectReconcilerAttemptOutcome::InvalidEvidence);
                        }
                    },
                    None => None,
                };
                (
                    "not-applied",
                    EffectCommandKind::ReconcileNotApplied {
                        lease_id: prepared.lease_id.clone(),
                        attempt: prepared.attempt,
                        reason_code: reason_code.clone(),
                        retry_at_ms,
                    },
                )
            }
            EffectReconciliationOutcome::StillUnknown { .. } => {
                return prepared.report(EffectReconcilerAttemptOutcome::SettlementFailed);
            }
        };
        let command_id =
            match reconciliation_command_id(cycle_id, &prepared, purpose, &outcome, authority) {
                Ok(command_id) => command_id,
                Err(_) => return prepared.report(EffectReconcilerAttemptOutcome::SettlementFailed),
            };
        match self
            .engine
            .apply_as(
                prepared.snapshot.id(),
                prepared.snapshot.revision(),
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
                if result.outcome == EffectApplyOutcome::Duplicate {
                    prepared.report(EffectReconcilerAttemptOutcome::AlreadyReconciled)
                } else {
                    prepared.report(report_settlement(
                        &outcome,
                        result.snapshot.effect().status(),
                    ))
                }
            }
            Err(HarnessError::EffectConflict { actual, .. }) => {
                prepared.report(EffectReconcilerAttemptOutcome::SettlementFenced {
                    actual_revision: actual,
                })
            }
            Err(_) => prepared.report(EffectReconcilerAttemptOutcome::SettlementFailed),
        }
    }
}

#[derive(Clone)]
struct PreparedReconciliation {
    snapshot: EffectSnapshot,
    attempt: u32,
    lease_id: EffectLeaseId,
}

impl TryFrom<&EffectSnapshot> for PreparedReconciliation {
    type Error = HarnessError;

    fn try_from(snapshot: &EffectSnapshot) -> Result<Self, Self::Error> {
        let EffectStatus::Unknown {
            attempt, lease_id, ..
        } = snapshot.effect().status()
        else {
            return Err(HarnessError::Effect(
                "Effect Reconciler unknown scan returned non-unknown state".to_owned(),
            ));
        };
        Ok(Self {
            snapshot: snapshot.clone(),
            attempt: *attempt,
            lease_id: lease_id.clone(),
        })
    }
}

impl PreparedReconciliation {
    fn report(&self, outcome: EffectReconcilerAttemptOutcome) -> EffectReconcilerAttempt {
        EffectReconcilerAttempt {
            effect_id: self.snapshot.id().clone(),
            observed_revision: self.snapshot.revision(),
            attempt: self.attempt,
            lease_id: self.lease_id.clone(),
            outcome,
        }
    }
}

#[derive(Clone)]
struct ReconcilerRunContext {
    cycle_id: String,
    authority: AuthorityContext,
    cancellation: CancellationToken,
}

fn validate_descriptor(
    descriptor: &EffectReconciliationConnectorDescriptor,
) -> Result<(), HarnessError> {
    validate_capability_name(
        "Effect Reconciliation Connector capability",
        &descriptor.capability,
    )
    .map_err(|error| HarnessError::Effect(error.to_string()))?;
    if descriptor.api_version != EFFECT_RECONCILER_API_VERSION {
        return Err(HarnessError::Effect(format!(
            "Effect Reconciliation Connector {} requires API {}, received {}",
            descriptor.capability, EFFECT_RECONCILER_API_VERSION, descriptor.api_version
        )));
    }
    if descriptor.operations.is_empty()
        || descriptor.operations.len() > MAX_OPERATIONS_PER_CONNECTOR
    {
        return Err(HarnessError::Effect(format!(
            "Effect Reconciliation Connector operations must contain 1-{MAX_OPERATIONS_PER_CONNECTOR} entries"
        )));
    }
    for operation in &descriptor.operations {
        validate_capability_name("Effect Reconciliation Connector operation", operation)
            .map_err(|error| HarnessError::Effect(error.to_string()))?;
    }
    bounded_serialized_size(descriptor, MAX_DESCRIPTOR_BYTES).map_err(descriptor_bound_error)?;
    Ok(())
}

fn descriptor_bound_error(error: BoundedJsonError) -> HarnessError {
    let detail = match error {
        BoundedJsonError::LimitExceeded => "exceeds its encoded-byte limit",
        BoundedJsonError::CannotEncode => "cannot be encoded",
    };
    HarnessError::Effect(format!(
        "Effect Reconciliation Connector descriptor {detail}; limit is {MAX_DESCRIPTOR_BYTES} bytes"
    ))
}

fn validate_run_request(
    request: &EffectReconcilerRunRequest,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect Reconciler authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_identity("Effect Reconciler cycle", &request.cycle_id)?;
    if let Some(after) = &request.after {
        validate_identity("Effect Reconciler cursor", after.effect_id.as_str())?;
    }
    Ok(())
}

fn validate_outcome(outcome: &EffectReconciliationOutcome) -> Result<(), ()> {
    match outcome {
        EffectReconciliationOutcome::Applied { .. } => Ok(()),
        EffectReconciliationOutcome::NotApplied {
            reason_code,
            retry_after_ms,
        } => {
            if validate_capability_name(
                "Effect Reconciliation Connector not-applied reason",
                reason_code,
            )
            .is_ok()
                && retry_after_ms.is_none_or(|delay| delay <= MAX_RETRY_AFTER_MS)
            {
                Ok(())
            } else {
                Err(())
            }
        }
        EffectReconciliationOutcome::StillUnknown { reason_code } => validate_capability_name(
            "Effect Reconciliation Connector uncertainty reason",
            reason_code,
        )
        .map_err(|_| ()),
    }
}

#[derive(Serialize)]
struct ReconciliationIdentity<'a> {
    purpose: &'a str,
    cycle_id: &'a str,
    actor: &'a ActorIdentity,
    tenant_id: Option<&'a str>,
    effect_id: &'a str,
    revision: u64,
    attempt: u32,
    lease_id: &'a str,
    evidence_sha256: &'a str,
}

fn reconciliation_command_id(
    cycle_id: &str,
    prepared: &PreparedReconciliation,
    purpose: &str,
    outcome: &EffectReconciliationOutcome,
    authority: &AuthorityContext,
) -> Result<EffectCommandId, HarnessError> {
    let evidence = serde_json::to_vec(outcome).map_err(|_| {
        HarnessError::Effect("cannot encode Effect reconciliation evidence".to_owned())
    })?;
    let evidence_sha256 = hex_sha256(&evidence);
    let identity = ReconciliationIdentity {
        purpose,
        cycle_id,
        actor: authority.actor(),
        tenant_id: authority.tenant_id(),
        effect_id: prepared.snapshot.id().as_str(),
        revision: prepared.snapshot.revision(),
        attempt: prepared.attempt,
        lease_id: prepared.lease_id.as_str(),
        evidence_sha256: &evidence_sha256,
    };
    let encoded = serde_json::to_vec(&identity).map_err(|_| {
        HarnessError::Effect("cannot encode Effect reconciliation identity".to_owned())
    })?;
    Ok(EffectCommandId::from_string(format!(
        "reconciler-settle-{}",
        hex_sha256(&encoded)
    )))
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn report_settlement(
    outcome: &EffectReconciliationOutcome,
    status: &EffectStatus,
) -> EffectReconcilerAttemptOutcome {
    match (outcome, status) {
        (EffectReconciliationOutcome::Applied { .. }, EffectStatus::Applied { .. }) => {
            EffectReconcilerAttemptOutcome::Applied
        }
        (
            EffectReconciliationOutcome::NotApplied {
                reason_code,
                retry_after_ms: None,
            },
            EffectStatus::Rejected {
                reason_code: durable_reason,
                ..
            },
        ) if reason_code == durable_reason => EffectReconcilerAttemptOutcome::Rejected {
            reason_code: reason_code.clone(),
        },
        (
            EffectReconciliationOutcome::NotApplied {
                reason_code,
                retry_after_ms: Some(_),
            },
            EffectStatus::Pending { not_before_ms, .. },
        ) => EffectReconcilerAttemptOutcome::RetryScheduled {
            reason_code: reason_code.clone(),
            retry_at_ms: *not_before_ms,
        },
        _ => EffectReconcilerAttemptOutcome::SettlementFailed,
    }
}

fn trusted_now(clock: &dyn EffectReconcilerClock) -> Result<u64, HarnessError> {
    let value = catch_unwind(AssertUnwindSafe(|| clock.now_ms()))
        .map_err(|_| HarnessError::Effect("Effect Reconciler clock panicked".to_owned()))??;
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
        EffectCommandResult, EffectCoordinator, EffectCreateRequest, EffectDueScanPage, EffectPage,
        MemoryEffectCoordinator,
    };

    const CREATED_AT_MS: u64 = 100;
    const UNKNOWN_AT_MS: u64 = 101;
    const NOW_MS: u64 = 200;

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

    impl EffectReconcilerClock for FixedClock {
        fn now_ms(&self) -> Result<u64, HarnessError> {
            Ok(self.now_ms.load(Ordering::SeqCst))
        }
    }

    struct FailingAfterScanClock {
        calls: AtomicUsize,
    }

    impl EffectReconcilerClock for FailingAfterScanClock {
        fn now_ms(&self) -> Result<u64, HarnessError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(NOW_MS)
            } else {
                Err(HarnessError::Effect("clock secret".to_owned()))
            }
        }
    }

    struct StaticConnector {
        calls: Arc<AtomicUsize>,
        outcome: EffectReconciliationOutcome,
    }

    impl EffectReconciliationConnector for StaticConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            connector_descriptor()
        }

        fn query<'a>(
            &'a self,
            _request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self.outcome.clone();
            Box::pin(async move { Ok(outcome) })
        }
    }

    struct PanicConnector {
        calls: Arc<AtomicUsize>,
    }

    impl EffectReconciliationConnector for PanicConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            connector_descriptor()
        }

        fn query<'a>(
            &'a self,
            _request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            panic!("reconciliation lookup secret must remain isolated")
        }
    }

    struct HangingConnector {
        calls: Arc<AtomicUsize>,
        entered: Mutex<Option<oneshot::Sender<()>>>,
    }

    impl EffectReconciliationConnector for HangingConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            connector_descriptor()
        }

        fn query<'a>(
            &'a self,
            _request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(entered) = self.entered.lock().expect("entered lock").take() {
                let _ = entered.send(());
            }
            Box::pin(future::pending())
        }
    }

    struct BarrierConnector {
        calls: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    impl EffectReconciliationConnector for BarrierConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            connector_descriptor()
        }

        fn query<'a>(
            &'a self,
            request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.barrier.wait().await;
                Ok(EffectReconciliationOutcome::Applied {
                    receipt: receipt(request.effect_id.as_str()),
                })
            })
        }
    }

    struct ConcurrencyConnector {
        calls: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }

    impl EffectReconciliationConnector for ConcurrencyConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            connector_descriptor()
        }

        fn query<'a>(
            &'a self,
            request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(EffectReconciliationOutcome::Applied {
                    receipt: receipt(request.effect_id.as_str()),
                })
            })
        }
    }

    struct PanicPolicy;

    impl EffectReconciliationPolicy for PanicPolicy {
        fn authorize<'a>(
            &'a self,
            _request: EffectReconciliationPolicyRequest,
        ) -> HarnessFuture<'a, EffectReconciliationDecision> {
            panic!("reconciliation policy secret must remain isolated")
        }
    }

    struct HangingPolicy;

    impl EffectReconciliationPolicy for HangingPolicy {
        fn authorize<'a>(
            &'a self,
            _request: EffectReconciliationPolicyRequest,
        ) -> HarnessFuture<'a, EffectReconciliationDecision> {
            Box::pin(future::pending())
        }
    }

    struct DescriptorPanicConnector;

    impl EffectReconciliationConnector for DescriptorPanicConnector {
        fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
            panic!("reconciliation descriptor secret must remain isolated")
        }

        fn query<'a>(
            &'a self,
            _request: EffectReconciliationRequest,
        ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
            Box::pin(async {
                Err(HarnessError::Effect(
                    "unreachable reconciliation Connector".to_owned(),
                ))
            })
        }
    }

    struct PanicOnReconcileCoordinator {
        inner: MemoryEffectCoordinator,
    }

    impl EffectCoordinator for PanicOnReconcileCoordinator {
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
            effect_id: &'a EffectId,
            expected_revision: u64,
            command: EffectCommand,
            applied_at_ms: u64,
            authority: &'a AuthorityContext,
        ) -> HarnessFuture<'a, EffectCommandResult> {
            if matches!(
                command.kind,
                EffectCommandKind::ReconcileApplied { .. }
                    | EffectCommandKind::ReconcileNotApplied { .. }
            ) {
                return Box::pin(async {
                    panic!("reconciliation persistence secret must remain isolated")
                });
            }
            self.inner.apply_as(
                effect_id,
                expected_revision,
                command,
                applied_at_ms,
                authority,
            )
        }
    }

    fn authority() -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "effect-reconciler".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority")
    }

    fn connector_descriptor() -> EffectReconciliationConnectorDescriptor {
        EffectReconciliationConnectorDescriptor {
            capability: "channel.email".to_owned(),
            api_version: EFFECT_RECONCILER_API_VERSION,
            operations: BTreeSet::from(["send".to_owned()]),
            contract: EffectReconciliationContract::AuthoritativeReadOnly,
        }
    }

    fn receipt(external_id: &str) -> EffectReceipt {
        EffectReceipt {
            source: "mail.provider".to_owned(),
            external_id: external_id.to_owned(),
            observed_at_ms: 150,
            response_sha256: "a".repeat(64),
        }
    }

    fn config(lookup_timeout_ms: u64, max_concurrency: usize) -> EffectReconcilerConfig {
        EffectReconcilerConfig {
            scan_limit: 16,
            max_concurrency,
            policy_timeout_ms: 100,
            lookup_timeout_ms,
        }
    }

    fn allow_policy() -> Arc<dyn EffectReconciliationPolicy> {
        Arc::new(
            AllowListEffectReconciliationPolicy::deny_by_default()
                .allow("channel.email", "send")
                .expect("allow policy"),
        )
    }

    fn engine() -> EffectEngine {
        EffectEngine::new(Arc::new(MemoryEffectCoordinator::new()))
    }

    fn registry(
        connector: Arc<dyn EffectReconciliationConnector>,
    ) -> EffectReconciliationConnectorRegistry {
        let mut registry = EffectReconciliationConnectorRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, connector)
            .expect("register Connector");
        registry
    }

    async fn create_unknown(
        engine: &EffectEngine,
        authority: &AuthorityContext,
        id: &str,
        input: Value,
    ) {
        let effect_id = EffectId::from_string(id.to_owned());
        let lease_id = EffectLeaseId::from_string(format!("lease-{id}"));
        engine
            .create_as(
                effect_id.clone(),
                EffectCreateRequest {
                    command_id: EffectCommandId::from_string(format!("create-{id}")),
                    operation: EffectOperation {
                        capability: "channel.email".to_owned(),
                        operation: "send".to_owned(),
                    },
                    idempotency_key: format!("idempotency-secret-{id}"),
                    input,
                    not_before_ms: CREATED_AT_MS,
                },
                CREATED_AT_MS,
                authority,
            )
            .await
            .expect("create");
        engine
            .apply_as(
                &effect_id,
                1,
                EffectCommand {
                    id: EffectCommandId::from_string(format!("claim-{id}")),
                    kind: EffectCommandKind::Claim {
                        lease_id: lease_id.clone(),
                        lease_duration_ms: 1_000,
                    },
                },
                CREATED_AT_MS,
                authority,
            )
            .await
            .expect("claim");
        engine
            .apply_as(
                &effect_id,
                2,
                EffectCommand {
                    id: EffectCommandId::from_string(format!("unknown-{id}")),
                    kind: EffectCommandKind::RecordUnknown {
                        lease_id,
                        reason_code: "connector.timeout".to_owned(),
                    },
                },
                UNKNOWN_AT_MS,
                authority,
            )
            .await
            .expect("unknown");
    }

    fn request(cycle_id: &str) -> EffectReconcilerRunRequest {
        EffectReconcilerRunRequest {
            cycle_id: cycle_id.to_owned(),
            after: None,
        }
    }

    #[test]
    fn registration_is_atomic_across_panic_and_api_mismatch() {
        let mut registry = EffectReconciliationConnectorRegistry::new();
        let panic_error = registry
            .register(
                CapabilityOrigin::BuiltIn,
                Arc::new(DescriptorPanicConnector),
            )
            .expect_err("descriptor panic");
        assert!(panic_error.to_string().contains("descriptor"));
        assert!(registry.capabilities().is_empty());

        struct IncompatibleConnector;
        impl EffectReconciliationConnector for IncompatibleConnector {
            fn descriptor(&self) -> EffectReconciliationConnectorDescriptor {
                EffectReconciliationConnectorDescriptor {
                    api_version: EFFECT_RECONCILER_API_VERSION + 1,
                    ..connector_descriptor()
                }
            }

            fn query<'a>(
                &'a self,
                _request: EffectReconciliationRequest,
            ) -> HarnessFuture<'a, EffectReconciliationOutcome> {
                Box::pin(async {
                    Err(HarnessError::Effect(
                        "incompatible Connector must not query".to_owned(),
                    ))
                })
            }
        }
        let api_error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(IncompatibleConnector))
            .expect_err("API mismatch");
        assert!(api_error.to_string().contains("requires API"));
        assert!(registry.capabilities().is_empty());
    }

    #[test]
    fn config_rejects_unbounded_scan_concurrency_and_deadlines() {
        let mut invalid = config(100, 1);
        invalid.scan_limit = 0;
        assert!(invalid.validate().is_err());
        invalid.scan_limit = 16;
        invalid.max_concurrency = MAX_CONCURRENCY + 1;
        assert!(invalid.validate().is_err());
        invalid.max_concurrency = 1;
        invalid.policy_timeout_ms = 0;
        assert!(invalid.validate().is_err());
        invalid.policy_timeout_ms = 100;
        invalid.lookup_timeout_ms = MAX_LOOKUP_TIMEOUT_MS + 1;
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn unknown_page_validation_rejects_bad_continuation_and_state() {
        let authority = authority();
        let unknown_engine = engine();
        create_unknown(
            &unknown_engine,
            &authority,
            "effect-page",
            serde_json::json!({}),
        )
        .await;
        let canonical = unknown_engine
            .list_as(Some("unknown"), None, 16, &authority)
            .await
            .expect("unknown page");
        let mut bad_continuation = canonical;
        bad_continuation.next_cursor = None;
        assert!(
            validate_effect_page(
                "Effect Reconciler",
                &bad_continuation,
                None,
                16,
                &authority,
                EffectPageState::Unknown,
            )
            .is_err()
        );

        let pending_engine = engine();
        pending_engine
            .create_as(
                EffectId::from_static("effect-pending"),
                EffectCreateRequest {
                    command_id: EffectCommandId::from_static("create-pending"),
                    operation: EffectOperation {
                        capability: "channel.email".to_owned(),
                        operation: "send".to_owned(),
                    },
                    idempotency_key: "pending-key".to_owned(),
                    input: serde_json::json!({}),
                    not_before_ms: CREATED_AT_MS,
                },
                CREATED_AT_MS,
                &authority,
            )
            .await
            .expect("pending");
        let pending = pending_engine
            .list_as(Some("pending"), None, 16, &authority)
            .await
            .expect("pending page");
        assert!(
            validate_effect_page(
                "Effect Reconciler",
                &pending,
                None,
                16,
                &authority,
                EffectPageState::Unknown,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn default_policy_denies_before_lookup_and_mutation() {
        let authority = authority();
        let engine = engine();
        create_unknown(&engine, &authority, "effect-denied", serde_json::json!({})).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let reconciler = EffectReconciler::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: receipt("denied"),
                },
            })),
        )
        .expect("Reconciler")
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));

        let report = reconciler
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
            EffectReconcilerAttemptOutcome::PolicyDenied { .. }
        ));
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-denied"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 3);
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Unknown { .. }
        ));
    }

    #[tokio::test]
    async fn unavailable_connector_and_pre_cancelled_run_do_not_query() {
        let authority = authority();
        let unavailable_engine = engine();
        create_unknown(
            &unavailable_engine,
            &authority,
            "effect-unavailable",
            serde_json::json!({}),
        )
        .await;
        let unavailable = EffectReconciler::new(
            unavailable_engine,
            EffectReconciliationConnectorRegistry::new(),
        )
        .expect("Reconciler")
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let unavailable_report = unavailable
            .run_once_as(
                request("cycle-unavailable"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(
            unavailable_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::ConnectorUnavailable
        );

        let cancelled_engine = engine();
        create_unknown(
            &cancelled_engine,
            &authority,
            "effect-pre-cancelled",
            serde_json::json!({}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let cancelled = EffectReconciler::new(
            cancelled_engine,
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: receipt("never"),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled_report = cancelled
            .run_once_as(request("cycle-pre-cancelled"), &authority, cancellation)
            .await
            .expect("run");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            cancelled_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::CancelledBeforeLookup
        );
    }

    #[tokio::test]
    async fn applied_reconciliation_is_durable_and_report_is_content_free() {
        let authority = authority();
        let engine = engine();
        create_unknown(
            &engine,
            &authority,
            "effect-applied",
            serde_json::json!({"private":"input-secret"}),
        )
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let reconciler = EffectReconciler::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: calls.clone(),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: receipt("receipt-secret"),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");

        let report = reconciler
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
            EffectReconcilerAttemptOutcome::Applied
        );
        let encoded = serde_json::to_string(&report).expect("report");
        assert!(!encoded.contains("input-secret"));
        assert!(!encoded.contains("idempotency-secret"));
        assert!(!encoded.contains("receipt-secret"));
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-applied"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 4);
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Applied { attempt: 1, .. }
        ));
    }

    #[tokio::test]
    async fn not_applied_schedules_retry_while_still_unknown_does_not_mutate() {
        let authority = authority();
        let retry_engine = engine();
        create_unknown(
            &retry_engine,
            &authority,
            "effect-retry",
            serde_json::json!({}),
        )
        .await;
        let retry_reconciler = EffectReconciler::new(
            retry_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::NotApplied {
                    reason_code: "provider.absent".to_owned(),
                    retry_after_ms: Some(25),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let retry_report = retry_reconciler
            .run_once_as(request("cycle-retry"), &authority, CancellationToken::new())
            .await
            .expect("retry run");
        assert_eq!(
            retry_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::RetryScheduled {
                reason_code: "provider.absent".to_owned(),
                retry_at_ms: 225,
            }
        );
        let retry_snapshot = retry_engine
            .load_as(&EffectId::from_static("effect-retry"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            retry_snapshot.effect().status(),
            EffectStatus::Pending {
                next_attempt: 2,
                not_before_ms: 225
            }
        ));

        let unknown_engine = engine();
        create_unknown(
            &unknown_engine,
            &authority,
            "effect-still-unknown",
            serde_json::json!({}),
        )
        .await;
        let unknown_reconciler = EffectReconciler::new(
            unknown_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::StillUnknown {
                    reason_code: "provider.pending".to_owned(),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let unknown_report = unknown_reconciler
            .run_once_as(
                request("cycle-still-unknown"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("unknown run");
        assert_eq!(
            unknown_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::StillUnknown {
                reason_code: "provider.pending".to_owned(),
            }
        );
        let unknown_snapshot = unknown_engine
            .load_as(&EffectId::from_static("effect-still-unknown"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(unknown_snapshot.revision(), 3);
    }

    #[tokio::test]
    async fn terminal_absence_rejects_and_clock_failure_preserves_unknown() {
        let authority = authority();
        let rejected_engine = engine();
        create_unknown(
            &rejected_engine,
            &authority,
            "effect-rejected",
            serde_json::json!({}),
        )
        .await;
        let rejected = EffectReconciler::new(
            rejected_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::NotApplied {
                    reason_code: "provider.absent".to_owned(),
                    retry_after_ms: None,
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let rejected_report = rejected
            .run_once_as(
                request("cycle-rejected"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(
            rejected_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::Rejected {
                reason_code: "provider.absent".to_owned(),
            }
        );
        let rejected_snapshot = rejected_engine
            .load_as(&EffectId::from_static("effect-rejected"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert!(matches!(
            rejected_snapshot.effect().status(),
            EffectStatus::Rejected { attempt: 1, .. }
        ));

        let clock_engine = engine();
        create_unknown(
            &clock_engine,
            &authority,
            "effect-clock",
            serde_json::json!({}),
        )
        .await;
        let clock_failure = EffectReconciler::new(
            clock_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: receipt("clock"),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FailingAfterScanClock {
            calls: AtomicUsize::new(0),
        }));
        let clock_report = clock_failure
            .run_once_as(request("cycle-clock"), &authority, CancellationToken::new())
            .await
            .expect("run");
        assert_eq!(
            clock_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::ClockUnavailable
        );
        let clock_snapshot = clock_engine
            .load_as(&EffectId::from_static("effect-clock"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(clock_snapshot.revision(), 3);
        assert!(matches!(
            clock_snapshot.effect().status(),
            EffectStatus::Unknown { .. }
        ));
    }

    #[tokio::test]
    async fn invalid_panic_and_timeout_lookup_leave_state_unknown() {
        let authority = authority();

        let invalid_engine = engine();
        create_unknown(
            &invalid_engine,
            &authority,
            "effect-invalid",
            serde_json::json!({}),
        )
        .await;
        let invalid = EffectReconciler::new(
            invalid_engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: EffectReceipt {
                        response_sha256: "invalid".to_owned(),
                        ..receipt("invalid")
                    },
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let invalid_report = invalid
            .run_once_as(
                request("cycle-invalid"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("invalid run");
        assert_eq!(
            invalid_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::InvalidEvidence
        );

        let panic_engine = engine();
        create_unknown(
            &panic_engine,
            &authority,
            "effect-panic",
            serde_json::json!({}),
        )
        .await;
        let panic_calls = Arc::new(AtomicUsize::new(0));
        let panic = EffectReconciler::new(
            panic_engine.clone(),
            registry(Arc::new(PanicConnector {
                calls: panic_calls.clone(),
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));
        let panic_report = panic
            .run_once_as(request("cycle-panic"), &authority, CancellationToken::new())
            .await
            .expect("panic run");
        assert_eq!(panic_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            panic_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::LookupUnavailable
        );

        let timeout_engine = engine();
        create_unknown(
            &timeout_engine,
            &authority,
            "effect-timeout",
            serde_json::json!({}),
        )
        .await;
        let timeout = EffectReconciler::new(
            timeout_engine.clone(),
            registry(Arc::new(HangingConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                entered: Mutex::new(None),
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(1, 1))
        .expect("config");
        let timeout_report = timeout
            .run_once_as(
                request("cycle-timeout"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("timeout run");
        assert_eq!(
            timeout_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::LookupUnavailable
        );

        for (engine, id) in [
            (invalid_engine, "effect-invalid"),
            (panic_engine, "effect-panic"),
            (timeout_engine, "effect-timeout"),
        ] {
            let snapshot = engine
                .load_as(&EffectId::from_string(id.to_owned()), &authority)
                .await
                .expect("load")
                .expect("Effect");
            assert_eq!(snapshot.revision(), 3);
            assert!(matches!(
                snapshot.effect().status(),
                EffectStatus::Unknown { .. }
            ));
        }
    }

    #[tokio::test]
    async fn cancellation_and_policy_failure_never_settle_unknown_state() {
        let authority = authority();

        let cancelled_engine = engine();
        create_unknown(
            &cancelled_engine,
            &authority,
            "effect-cancelled",
            serde_json::json!({}),
        )
        .await;
        let (entered_tx, entered_rx) = oneshot::channel();
        let cancelled = EffectReconciler::new(
            cancelled_engine.clone(),
            registry(Arc::new(HangingConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                entered: Mutex::new(Some(entered_tx)),
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 1))
        .expect("config");
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task_authority = authority.clone();
        let task = tokio::spawn(async move {
            cancelled
                .run_once_as(
                    request("cycle-cancelled"),
                    &task_authority,
                    task_cancellation,
                )
                .await
        });
        entered_rx.await.expect("lookup entered");
        cancellation.cancel();
        let cancelled_report = task.await.expect("join").expect("cancelled run");
        assert_eq!(
            cancelled_report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::CancelledDuringLookup
        );

        for (policy, expected_cycle) in [
            (
                Arc::new(PanicPolicy) as Arc<dyn EffectReconciliationPolicy>,
                "cycle-policy-panic",
            ),
            (
                Arc::new(HangingPolicy) as Arc<dyn EffectReconciliationPolicy>,
                "cycle-policy-timeout",
            ),
        ] {
            let policy_engine = engine();
            create_unknown(
                &policy_engine,
                &authority,
                expected_cycle,
                serde_json::json!({}),
            )
            .await;
            let calls = Arc::new(AtomicUsize::new(0));
            let reconciler = EffectReconciler::new(
                policy_engine,
                registry(Arc::new(StaticConnector {
                    calls: calls.clone(),
                    outcome: EffectReconciliationOutcome::Applied {
                        receipt: receipt("never"),
                    },
                })),
            )
            .expect("Reconciler")
            .with_policy(policy)
            .with_clock(Arc::new(FixedClock::new(NOW_MS)))
            .with_config(EffectReconcilerConfig {
                policy_timeout_ms: 1,
                ..config(100, 1)
            })
            .expect("config");
            let report = reconciler
                .run_once_as(
                    request(expected_cycle),
                    &authority,
                    CancellationToken::new(),
                )
                .await
                .expect("policy run");
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                report.attempts[0].outcome,
                EffectReconcilerAttemptOutcome::PolicyUnavailable
            );
        }
    }

    #[tokio::test]
    async fn concurrent_same_cycle_queries_are_read_only_and_settlement_is_idempotent() {
        let authority = authority();
        let engine = engine();
        create_unknown(&engine, &authority, "effect-race", serde_json::json!({})).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let reconciler = EffectReconciler::new(
            engine.clone(),
            registry(Arc::new(BarrierConnector {
                calls: calls.clone(),
                barrier: Arc::new(Barrier::new(2)),
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));

        let left =
            reconciler.run_once_as(request("same-cycle"), &authority, CancellationToken::new());
        let right =
            reconciler.run_once_as(request("same-cycle"), &authority, CancellationToken::new());
        let (left, right) = tokio::join!(left, right);
        let outcomes = [
            left.expect("left").attempts[0].outcome.clone(),
            right.expect("right").attempts[0].outcome.clone(),
        ];

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(outcomes.contains(&EffectReconcilerAttemptOutcome::Applied));
        assert!(outcomes.contains(&EffectReconcilerAttemptOutcome::AlreadyReconciled));
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-race"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 4);
    }

    #[tokio::test]
    async fn sweep_honors_concurrency_bound_and_source_order() {
        let authority = authority();
        let engine = engine();
        for index in 0..5 {
            create_unknown(
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
        let reconciler = EffectReconciler::new(
            engine,
            registry(Arc::new(ConcurrencyConnector {
                calls: calls.clone(),
                in_flight: in_flight.clone(),
                max_in_flight: max_in_flight.clone(),
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)))
        .with_config(config(100, 2))
        .expect("config");
        let report = reconciler
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
    async fn unexpected_settlement_worker_panic_is_content_free_and_state_wins() {
        let authority = authority();
        let engine = EffectEngine::new(Arc::new(PanicOnReconcileCoordinator {
            inner: MemoryEffectCoordinator::new(),
        }));
        create_unknown(
            &engine,
            &authority,
            "effect-worker-panic",
            serde_json::json!({}),
        )
        .await;
        let reconciler = EffectReconciler::new(
            engine.clone(),
            registry(Arc::new(StaticConnector {
                calls: Arc::new(AtomicUsize::new(0)),
                outcome: EffectReconciliationOutcome::Applied {
                    receipt: receipt("worker-panic"),
                },
            })),
        )
        .expect("Reconciler")
        .with_policy(allow_policy())
        .with_clock(Arc::new(FixedClock::new(NOW_MS)));

        let report = reconciler
            .run_once_as(
                request("cycle-worker-panic"),
                &authority,
                CancellationToken::new(),
            )
            .await
            .expect("run");
        assert_eq!(
            report.attempts[0].outcome,
            EffectReconcilerAttemptOutcome::AttemptFailed
        );
        assert!(
            !serde_json::to_string(&report)
                .expect("report")
                .contains("secret")
        );
        let snapshot = engine
            .load_as(&EffectId::from_static("effect-worker-panic"), &authority)
            .await
            .expect("load")
            .expect("Effect");
        assert_eq!(snapshot.revision(), 3);
        assert!(matches!(
            snapshot.effect().status(),
            EffectStatus::Unknown { .. }
        ));
    }
}
