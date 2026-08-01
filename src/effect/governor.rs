//! Durable, tenant-exact admission control for external Effect dispatch.
//!
//! One lane is the trusted tuple `(tenant, capability, operation, policy_id)`.
//! The governor never interprets arbitrary Effect input as a target identity.
//! Admission is intended to run after a durable Effect Claim and before the
//! Connector boundary, so every denial can be settled authoritatively as a
//! retryable `NotApplied` outcome.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use rusqlite::{
    Connection, ErrorCode, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{sync::Mutex, task};

use super::{EffectOperation, validate_application_time, validate_identity};
use crate::{
    AuthorityContext, EffectLeaseId, HarnessError, HarnessFuture,
    kernel::validate_capability_name,
    sqlite::{bounded_text, open_read_only},
};

/// Exact embedded Effect dispatch-governor API coordinate.
pub const EFFECT_DISPATCH_GOVERNOR_API_VERSION: u32 = 1;
/// Current independent SQLite Effect dispatch-governor schema.
pub const EFFECT_DISPATCH_GOVERNOR_SCHEMA_VERSION: u32 = 1;

const MAX_POLICY_ID_BYTES: usize = 128;
const MAX_POLICY_JSON_BYTES: usize = 4_096;
const MAX_LANE_STATE_JSON_BYTES: usize = 4_096;
const MAX_ADMISSION_JSON_BYTES: usize = 8_192;
const MAX_ADMISSIONS: usize = 1_000_000;
const MAX_LANES: usize = 1_000_000;
const MAX_RATE_LIMIT: u32 = 1_000_000;
const MAX_WINDOW_MS: u64 = 86_400_000;
const MAX_FAILURE_THRESHOLD: u32 = 1_000_000;
const MAX_OPEN_DURATION_MS: u64 = 604_800_000;
const MAX_PROBE_LEASE_MS: u64 = 86_400_000;
const MIN_ADMISSION_RETENTION_MS: u64 = 604_800_000;
const MAX_ADMISSION_RETENTION_MS: u64 = 31_536_000_000;

/// Frozen rate-limit and circuit-breaker policy for one execution lane.
///
/// A `policy_id` is an immutable revision coordinate. Reusing it with changed
/// parameters is rejected by both stores rather than silently changing an
/// existing lane's semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectDispatchGovernorPolicy {
    /// Operator-owned immutable policy revision, such as `notification-v1`.
    pub policy_id: String,
    /// Maximum normal dispatch admissions in one deterministic fixed window.
    pub max_dispatches_per_window: u32,
    /// Fixed-window duration in milliseconds.
    pub window_ms: u64,
    /// Consecutive harness-proven availability failures that open the circuit.
    pub failure_threshold: u32,
    /// Duration for which an opened circuit denies every dispatch.
    pub open_duration_ms: u64,
    /// Exclusive lease for the sole half-open probe after the open duration.
    pub probe_lease_ms: u64,
    /// Duration retaining admission identities for idempotent retry.
    pub admission_retention_ms: u64,
}

impl EffectDispatchGovernorPolicy {
    /// Validates bounded counts, durations, identity, and retention safety.
    pub fn validate(&self) -> Result<(), HarnessError> {
        validate_capability_name("Effect dispatch-governor policy", &self.policy_id)
            .map_err(effect_error)?;
        if self.policy_id.len() > MAX_POLICY_ID_BYTES {
            return Err(governor_error(format!(
                "policy_id exceeds {MAX_POLICY_ID_BYTES} bytes"
            )));
        }
        if !(1..=MAX_RATE_LIMIT).contains(&self.max_dispatches_per_window) {
            return Err(governor_error(format!(
                "max_dispatches_per_window must be 1-{MAX_RATE_LIMIT}"
            )));
        }
        if !(1..=MAX_WINDOW_MS).contains(&self.window_ms) {
            return Err(governor_error(format!(
                "window_ms must be 1-{MAX_WINDOW_MS}"
            )));
        }
        if !(1..=MAX_FAILURE_THRESHOLD).contains(&self.failure_threshold) {
            return Err(governor_error(format!(
                "failure_threshold must be 1-{MAX_FAILURE_THRESHOLD}"
            )));
        }
        if !(1..=MAX_OPEN_DURATION_MS).contains(&self.open_duration_ms) {
            return Err(governor_error(format!(
                "open_duration_ms must be 1-{MAX_OPEN_DURATION_MS}"
            )));
        }
        if !(1..=MAX_PROBE_LEASE_MS).contains(&self.probe_lease_ms) {
            return Err(governor_error(format!(
                "probe_lease_ms must be 1-{MAX_PROBE_LEASE_MS}"
            )));
        }
        if !(MIN_ADMISSION_RETENTION_MS..=MAX_ADMISSION_RETENTION_MS)
            .contains(&self.admission_retention_ms)
        {
            return Err(governor_error(format!(
                "admission_retention_ms must be {MIN_ADMISSION_RETENTION_MS}-{MAX_ADMISSION_RETENTION_MS}"
            )));
        }
        let minimum_retention = self
            .window_ms
            .max(self.open_duration_ms)
            .max(self.probe_lease_ms);
        if self.admission_retention_ms < minimum_retention {
            return Err(governor_error(
                "admission_retention_ms must cover every governor duration",
            ));
        }
        policy_digest(self).map(|_| ())
    }
}

/// Exact request for one post-Claim, pre-Connector dispatch admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectDispatchAdmissionRequest {
    /// Never-reused durable Effect lease used as the admission identity.
    pub admission_id: EffectLeaseId,
    /// Trusted durable execution coordinate; arbitrary Effect input is absent.
    pub operation: EffectOperation,
    /// Frozen lane policy.
    pub policy: EffectDispatchGovernorPolicy,
    /// Positive trusted admission time in Unix milliseconds.
    pub admitted_at_ms: u64,
}

/// Durable result of one exact admission request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectDispatchAdmissionDecision {
    /// Normal closed-circuit dispatch; consumes one fixed-window slot.
    Allow,
    /// Sole half-open probe; bypasses the normal fixed-window count.
    AllowProbe,
    /// The fixed-window budget is exhausted until this absolute time.
    RateLimited {
        /// Inclusive trusted retry boundary.
        retry_at_ms: u64,
    },
    /// The circuit is open or another half-open probe owns the lane.
    CircuitOpen {
        /// Inclusive trusted retry boundary.
        retry_at_ms: u64,
    },
}

/// Harness-owned dispatch health evidence used to settle an admission.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectDispatchSettlement {
    /// The Connector returned a contract-valid typed outcome.
    Healthy,
    /// The harness proved panic, error, timeout, or invalid Connector evidence.
    AvailabilityFailure,
    /// Dispatch was cancelled before or while awaiting the Connector.
    Abandoned,
}

/// Durable execution-lane admission and settlement port.
pub trait EffectDispatchGovernor: Send + Sync {
    /// Atomically admits or denies one exact lease inside a tenant boundary.
    fn admit_as<'a>(
        &'a self,
        request: EffectDispatchAdmissionRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision>;

    /// Idempotently settles one previously allowed admission.
    fn settle_as<'a>(
        &'a self,
        admission_id: &'a EffectLeaseId,
        settlement: EffectDispatchSettlement,
        settled_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ()>;
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LaneState {
    policy_sha256: String,
    last_observed_at_ms: u64,
    window_started_at_ms: u64,
    window_count: u32,
    consecutive_failures: u32,
    circuit_epoch: u64,
    open_until_ms: Option<u64>,
    probe_admission_id: Option<EffectLeaseId>,
    probe_expires_at_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecord {
    operation: EffectOperation,
    policy: EffectDispatchGovernorPolicy,
    policy_sha256: String,
    admitted_at_ms: u64,
    expires_at_ms: u64,
    circuit_epoch: u64,
    decision: EffectDispatchAdmissionDecision,
    settlement: Option<EffectDispatchSettlement>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct LaneKey {
    tenant_id: String,
    capability: String,
    operation: String,
    policy_id: String,
}

#[derive(Default)]
struct MemoryState {
    lanes: BTreeMap<LaneKey, LaneState>,
    admissions: BTreeMap<(String, EffectLeaseId), AdmissionRecord>,
}

/// Process-local implementation with the same atomic semantics as SQLite.
#[derive(Default)]
pub struct MemoryEffectDispatchGovernor {
    state: Mutex<MemoryState>,
}

impl MemoryEffectDispatchGovernor {
    /// Creates an empty governor.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl EffectDispatchGovernor for MemoryEffectDispatchGovernor {
    fn admit_as<'a>(
        &'a self,
        request: EffectDispatchAdmissionRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision> {
        Box::pin(async move {
            let validated = ValidatedAdmission::new(request, authority)?;
            let mut state = self.state.lock().await;
            prune_memory(&mut state, validated.admitted_at_ms);
            let admission_key = (
                validated.lane.tenant_id.clone(),
                validated.admission_id.clone(),
            );
            if let Some(existing) = state.admissions.get(&admission_key) {
                validate_duplicate(existing, &validated)?;
                return Ok(existing.decision.clone());
            }
            if state.admissions.len() >= MAX_ADMISSIONS {
                return Err(governor_error("admission capacity exhausted"));
            }
            if !state.lanes.contains_key(&validated.lane) && state.lanes.len() >= MAX_LANES {
                return Err(governor_error("lane capacity exhausted"));
            }
            let lane = state
                .lanes
                .entry(validated.lane.clone())
                .or_insert_with(|| initial_lane(&validated));
            validate_lane_policy(lane, &validated.policy_sha256)?;
            let (decision, epoch) = decide(lane, &validated)?;
            state.admissions.insert(
                admission_key,
                admission_record(&validated, epoch, decision.clone())?,
            );
            Ok(decision)
        })
    }

    fn settle_as<'a>(
        &'a self,
        admission_id: &'a EffectLeaseId,
        settlement: EffectDispatchSettlement,
        settled_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            validate_settlement(admission_id, settled_at_ms, authority)?;
            let tenant = tenant_key(authority);
            let mut state = self.state.lock().await;
            let key = (tenant.clone(), admission_id.clone());
            let record =
                state.admissions.get(&key).cloned().ok_or_else(|| {
                    governor_error("admission does not exist or retention expired")
                })?;
            validate_settlement_record(&record, settlement, settled_at_ms)?;
            if record.settlement.is_some() {
                return Ok(());
            }
            let lane_key = lane_key(&record.operation, &record.policy, tenant);
            let lane = state
                .lanes
                .get_mut(&lane_key)
                .ok_or_else(|| governor_error("admission lane is missing"))?;
            apply_settlement(lane, admission_id, &record, settlement, settled_at_ms)?;
            state
                .admissions
                .get_mut(&key)
                .ok_or_else(|| governor_error("admission disappeared during settlement"))?
                .settlement = Some(settlement);
            Ok(())
        })
    }
}

/// SQLite governor for restart-safe and multi-process local service operation.
pub struct SqliteEffectDispatchGovernor {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteEffectDispatchGovernor {
    /// Opens or creates the independent dispatch-governance store.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_path_buf();
        let connection = task::spawn_blocking(move || {
            let mut connection = Connection::open(path).map_err(sql_error)?;
            configure_connection(&connection)?;
            initialize_or_validate(&mut connection)?;
            Ok::<_, HarnessError>(connection)
        })
        .await
        .map_err(|error| governor_error(format!("open task failed: {error}")))??;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }

    /// Validates an existing store read-only without creating or changing it.
    pub async fn validate_existing(path: impl AsRef<Path>) -> Result<(), HarnessError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(());
        }
        task::spawn_blocking(move || {
            let connection = open_read_only(&path).map_err(sql_error)?;
            validate_existing_store(&connection)
        })
        .await
        .map_err(|error| governor_error(format!("validation task failed: {error}")))?
    }
}

impl EffectDispatchGovernor for SqliteEffectDispatchGovernor {
    fn admit_as<'a>(
        &'a self,
        request: EffectDispatchAdmissionRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDispatchAdmissionDecision> {
        let connection = self.connection.clone();
        let authority = authority.clone();
        Box::pin(async move {
            let validated = ValidatedAdmission::new(request, &authority)?;
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| governor_error("SQLite lock poisoned"))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                prune_sql(&transaction, validated.admitted_at_ms)?;
                if let Some(existing) = load_admission(
                    &transaction,
                    &validated.lane.tenant_id,
                    &validated.admission_id,
                )? {
                    validate_duplicate(&existing, &validated)?;
                    transaction.commit().map_err(sql_error)?;
                    return Ok(existing.decision);
                }
                let (lane_count, admission_count) = load_counts(&transaction)?;
                if admission_count
                    >= i64::try_from(MAX_ADMISSIONS)
                        .map_err(|_| governor_error("admission capacity does not fit SQLite"))?
                {
                    return Err(governor_error("admission capacity exhausted"));
                }
                let existing_lane = load_lane(&transaction, &validated.lane)?;
                let new_lane = existing_lane.is_none();
                if new_lane
                    && lane_count
                        >= i64::try_from(MAX_LANES)
                            .map_err(|_| governor_error("lane capacity does not fit SQLite"))?
                {
                    return Err(governor_error("lane capacity exhausted"));
                }
                let mut lane = existing_lane.unwrap_or_else(|| initial_lane(&validated));
                validate_lane_policy(&lane, &validated.policy_sha256)?;
                let (decision, epoch) = decide(&mut lane, &validated)?;
                save_lane(&transaction, &validated.lane, &lane)?;
                if new_lane {
                    update_counts(&transaction, 1, 0)?;
                }
                save_admission(
                    &transaction,
                    &validated.lane.tenant_id,
                    &validated.admission_id,
                    &admission_record(&validated, epoch, decision.clone())?,
                )?;
                update_counts(&transaction, 0, 1)?;
                transaction.commit().map_err(sql_error)?;
                Ok(decision)
            })
            .await
            .map_err(|error| governor_error(format!("admission task failed: {error}")))?
        })
    }

    fn settle_as<'a>(
        &'a self,
        admission_id: &'a EffectLeaseId,
        settlement: EffectDispatchSettlement,
        settled_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, ()> {
        let connection = self.connection.clone();
        let admission_id = admission_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_settlement(&admission_id, settled_at_ms, &authority)?;
            let tenant = tenant_key(&authority);
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| governor_error("SQLite lock poisoned"))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                let record =
                    load_admission(&transaction, &tenant, &admission_id)?.ok_or_else(|| {
                        governor_error("admission does not exist or retention expired")
                    })?;
                validate_settlement_record(&record, settlement, settled_at_ms)?;
                if record.settlement.is_some() {
                    transaction.commit().map_err(sql_error)?;
                    return Ok(());
                }
                let key = lane_key(&record.operation, &record.policy, tenant.clone());
                let mut lane = load_lane(&transaction, &key)?
                    .ok_or_else(|| governor_error("admission lane is missing"))?;
                apply_settlement(&mut lane, &admission_id, &record, settlement, settled_at_ms)?;
                save_lane(&transaction, &key, &lane)?;
                let changed = transaction
                    .execute(
                        "UPDATE effect_dispatch_admissions SET settlement = ?3
                         WHERE tenant_id = ?1 AND admission_id = ?2 AND settlement IS NULL",
                        params![tenant, admission_id.as_str(), settlement_name(settlement)],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(governor_error("settlement changed an unexpected row count"));
                }
                transaction.commit().map_err(sql_error)
            })
            .await
            .map_err(|error| governor_error(format!("settlement task failed: {error}")))?
        })
    }
}

struct ValidatedAdmission {
    admission_id: EffectLeaseId,
    operation: EffectOperation,
    policy: EffectDispatchGovernorPolicy,
    policy_sha256: String,
    admitted_at_ms: u64,
    lane: LaneKey,
}

impl ValidatedAdmission {
    fn new(
        request: EffectDispatchAdmissionRequest,
        authority: &AuthorityContext,
    ) -> Result<Self, HarnessError> {
        authority
            .validate_current("Effect dispatch-governor authority")
            .map_err(effect_error)?;
        validate_identity("Effect dispatch admission", request.admission_id.as_str())?;
        validate_operation(&request.operation)?;
        request.policy.validate()?;
        validate_application_time(request.admitted_at_ms)?;
        let policy_sha256 = policy_digest(&request.policy)?;
        let lane = lane_key(&request.operation, &request.policy, tenant_key(authority));
        Ok(Self {
            admission_id: request.admission_id,
            operation: request.operation,
            policy: request.policy,
            policy_sha256,
            admitted_at_ms: request.admitted_at_ms,
            lane,
        })
    }
}

fn decide(
    lane: &mut LaneState,
    request: &ValidatedAdmission,
) -> Result<(EffectDispatchAdmissionDecision, u64), HarnessError> {
    let now = request.admitted_at_ms;
    if now < lane.last_observed_at_ms {
        return Err(governor_error("trusted admission clock moved backwards"));
    }
    lane.last_observed_at_ms = now;
    if lane
        .probe_expires_at_ms
        .is_some_and(|deadline| deadline <= now)
    {
        lane.probe_admission_id = None;
        lane.probe_expires_at_ms = None;
    }
    if let Some(open_until) = lane.open_until_ms {
        if open_until > now {
            return Ok((
                EffectDispatchAdmissionDecision::CircuitOpen {
                    retry_at_ms: open_until,
                },
                lane.circuit_epoch,
            ));
        }
        if let Some(probe_until) = lane.probe_expires_at_ms {
            return Ok((
                EffectDispatchAdmissionDecision::CircuitOpen {
                    retry_at_ms: probe_until,
                },
                lane.circuit_epoch,
            ));
        }
        let probe_until = now
            .checked_add(request.policy.probe_lease_ms)
            .ok_or_else(|| governor_error("probe lease time overflow"))?;
        lane.probe_admission_id = Some(request.admission_id.clone());
        lane.probe_expires_at_ms = Some(probe_until);
        return Ok((
            EffectDispatchAdmissionDecision::AllowProbe,
            lane.circuit_epoch,
        ));
    }

    let window_started = fixed_window_start(now, request.policy.window_ms);
    if lane.window_started_at_ms != window_started {
        lane.window_started_at_ms = window_started;
        lane.window_count = 0;
    }
    if lane.window_count >= request.policy.max_dispatches_per_window {
        let retry_at_ms = window_started
            .checked_add(request.policy.window_ms)
            .ok_or_else(|| governor_error("rate-limit retry time overflow"))?;
        return Ok((
            EffectDispatchAdmissionDecision::RateLimited { retry_at_ms },
            lane.circuit_epoch,
        ));
    }
    lane.window_count = lane
        .window_count
        .checked_add(1)
        .ok_or_else(|| governor_error("rate-limit counter overflow"))?;
    Ok((EffectDispatchAdmissionDecision::Allow, lane.circuit_epoch))
}

fn apply_settlement(
    lane: &mut LaneState,
    admission_id: &EffectLeaseId,
    record: &AdmissionRecord,
    settlement: EffectDispatchSettlement,
    settled_at_ms: u64,
) -> Result<(), HarnessError> {
    if record.circuit_epoch != lane.circuit_epoch {
        return Ok(());
    }
    let probe = matches!(record.decision, EffectDispatchAdmissionDecision::AllowProbe);
    if probe && lane.probe_admission_id.as_ref() != Some(admission_id) {
        return Ok(());
    }
    let effective_at_ms = settled_at_ms.max(lane.last_observed_at_ms);
    lane.last_observed_at_ms = effective_at_ms;
    match (probe, settlement) {
        (true, EffectDispatchSettlement::Healthy) => {
            lane.open_until_ms = None;
            lane.probe_admission_id = None;
            lane.probe_expires_at_ms = None;
            lane.consecutive_failures = 0;
        }
        (true, EffectDispatchSettlement::AvailabilityFailure) => {
            lane.circuit_epoch = lane
                .circuit_epoch
                .checked_add(1)
                .ok_or_else(|| governor_error("circuit epoch overflow"))?;
            lane.open_until_ms = Some(
                effective_at_ms
                    .checked_add(record.policy.open_duration_ms)
                    .ok_or_else(|| governor_error("circuit retry time overflow"))?,
            );
            lane.probe_admission_id = None;
            lane.probe_expires_at_ms = None;
            lane.consecutive_failures = record.policy.failure_threshold;
        }
        (true, EffectDispatchSettlement::Abandoned) => {
            lane.probe_admission_id = None;
            lane.probe_expires_at_ms = None;
        }
        (false, EffectDispatchSettlement::Healthy) => {
            lane.consecutive_failures = 0;
        }
        (false, EffectDispatchSettlement::AvailabilityFailure) => {
            lane.consecutive_failures = lane
                .consecutive_failures
                .checked_add(1)
                .ok_or_else(|| governor_error("failure counter overflow"))?;
            if lane.consecutive_failures >= record.policy.failure_threshold {
                lane.circuit_epoch = lane
                    .circuit_epoch
                    .checked_add(1)
                    .ok_or_else(|| governor_error("circuit epoch overflow"))?;
                lane.open_until_ms = Some(
                    effective_at_ms
                        .checked_add(record.policy.open_duration_ms)
                        .ok_or_else(|| governor_error("circuit retry time overflow"))?,
                );
                lane.probe_admission_id = None;
                lane.probe_expires_at_ms = None;
            }
        }
        (false, EffectDispatchSettlement::Abandoned) => {}
    }
    Ok(())
}

fn initial_lane(request: &ValidatedAdmission) -> LaneState {
    LaneState {
        policy_sha256: request.policy_sha256.clone(),
        last_observed_at_ms: request.admitted_at_ms,
        window_started_at_ms: fixed_window_start(request.admitted_at_ms, request.policy.window_ms),
        window_count: 0,
        consecutive_failures: 0,
        circuit_epoch: 1,
        open_until_ms: None,
        probe_admission_id: None,
        probe_expires_at_ms: None,
    }
}

fn admission_record(
    request: &ValidatedAdmission,
    circuit_epoch: u64,
    decision: EffectDispatchAdmissionDecision,
) -> Result<AdmissionRecord, HarnessError> {
    Ok(AdmissionRecord {
        operation: request.operation.clone(),
        policy: request.policy.clone(),
        policy_sha256: request.policy_sha256.clone(),
        admitted_at_ms: request.admitted_at_ms,
        expires_at_ms: request
            .admitted_at_ms
            .checked_add(request.policy.admission_retention_ms)
            .ok_or_else(|| governor_error("admission retention time overflow"))?,
        circuit_epoch,
        decision,
        settlement: None,
    })
}

fn validate_duplicate(
    existing: &AdmissionRecord,
    request: &ValidatedAdmission,
) -> Result<(), HarnessError> {
    if existing.operation != request.operation
        || existing.policy.policy_id != request.policy.policy_id
        || existing.policy_sha256 != request.policy_sha256
    {
        return Err(governor_error(
            "admission identity is already bound to different content",
        ));
    }
    Ok(())
}

fn validate_settlement_record(
    record: &AdmissionRecord,
    settlement: EffectDispatchSettlement,
    settled_at_ms: u64,
) -> Result<(), HarnessError> {
    if !matches!(
        record.decision,
        EffectDispatchAdmissionDecision::Allow | EffectDispatchAdmissionDecision::AllowProbe
    ) {
        return Err(governor_error("denied admission cannot be settled"));
    }
    if settled_at_ms < record.admitted_at_ms {
        return Err(governor_error("settlement time precedes admission"));
    }
    if let Some(existing) = record.settlement
        && existing != settlement
    {
        return Err(governor_error(
            "admission is already settled with different evidence",
        ));
    }
    Ok(())
}

fn validate_settlement(
    admission_id: &EffectLeaseId,
    settled_at_ms: u64,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect dispatch-governor authority")
        .map_err(effect_error)?;
    validate_identity("Effect dispatch admission", admission_id.as_str())?;
    validate_application_time(settled_at_ms)
}

fn validate_operation(operation: &EffectOperation) -> Result<(), HarnessError> {
    validate_capability_name("Effect dispatch-governor capability", &operation.capability)
        .map_err(effect_error)?;
    validate_capability_name("Effect dispatch-governor operation", &operation.operation)
        .map_err(effect_error)
}

fn validate_lane_policy(lane: &LaneState, digest: &str) -> Result<(), HarnessError> {
    if lane.policy_sha256 == digest {
        Ok(())
    } else {
        Err(governor_error(
            "policy_id is already bound to different parameters",
        ))
    }
}

fn validate_lane_state(lane: &LaneState) -> Result<(), HarnessError> {
    validate_digest(&lane.policy_sha256)?;
    if lane.last_observed_at_ms == 0
        || lane.circuit_epoch == 0
        || lane.consecutive_failures > MAX_FAILURE_THRESHOLD
    {
        return Err(governor_error("lane counters are invalid"));
    }
    if lane.probe_admission_id.is_some() != lane.probe_expires_at_ms.is_some() {
        return Err(governor_error("lane probe evidence is partial"));
    }
    if lane.open_until_ms == Some(0) || lane.probe_expires_at_ms == Some(0) {
        return Err(governor_error("lane deadline is invalid"));
    }
    Ok(())
}

fn validate_admission_record(record: &AdmissionRecord) -> Result<(), HarnessError> {
    validate_operation(&record.operation)?;
    record.policy.validate()?;
    validate_digest(&record.policy_sha256)?;
    if policy_digest(&record.policy)? != record.policy_sha256 {
        return Err(governor_error("admission policy digest is invalid"));
    }
    validate_application_time(record.admitted_at_ms)?;
    if record.expires_at_ms <= record.admitted_at_ms || record.circuit_epoch == 0 {
        return Err(governor_error("admission timing or epoch is invalid"));
    }
    match record.decision {
        EffectDispatchAdmissionDecision::RateLimited { retry_at_ms }
        | EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms }
            if retry_at_ms < record.admitted_at_ms =>
        {
            Err(governor_error("admission retry time precedes admission"))
        }
        _ => Ok(()),
    }
}

fn validate_digest(value: &str) -> Result<(), HarnessError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(governor_error("policy digest is not lowercase SHA-256"))
    }
}

fn policy_digest(policy: &EffectDispatchGovernorPolicy) -> Result<String, HarnessError> {
    let encoded = serde_json::to_vec(policy)
        .map_err(|_| governor_error("cannot encode dispatch-governor policy"))?;
    if encoded.len() > MAX_POLICY_JSON_BYTES {
        return Err(governor_error("dispatch-governor policy is too large"));
    }
    Ok(Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn fixed_window_start(now_ms: u64, window_ms: u64) -> u64 {
    now_ms - (now_ms % window_ms)
}

fn tenant_key(authority: &AuthorityContext) -> String {
    authority.tenant_id().unwrap_or("").to_owned()
}

fn lane_key(
    operation: &EffectOperation,
    policy: &EffectDispatchGovernorPolicy,
    tenant_id: String,
) -> LaneKey {
    LaneKey {
        tenant_id,
        capability: operation.capability.clone(),
        operation: operation.operation.clone(),
        policy_id: policy.policy_id.clone(),
    }
}

fn prune_memory(state: &mut MemoryState, at_ms: u64) {
    state
        .admissions
        .retain(|_, admission| admission.expires_at_ms > at_ms);
}

fn configure_connection(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(sql_error)?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mode: String = loop {
        match connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0)) {
            Ok(mode) => break mode,
            Err(error) if sqlite_locked(&error) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(sql_error(error)),
        }
    };
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(governor_error(format!(
            "SQLite refused WAL mode and selected {mode}"
        )));
    }
    connection
        .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(sql_error)
}

fn sqlite_locked(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(failure.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

fn initialize_or_validate(connection: &mut Connection) -> Result<(), HarnessError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let tables = store_tables(&transaction)?;
    if tables.iter().all(|present| !present) {
        transaction
            .execute_batch(
                "
                CREATE TABLE effect_dispatch_governor_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    schema_version INTEGER NOT NULL,
                    lane_count INTEGER NOT NULL CHECK(lane_count >= 0),
                    admission_count INTEGER NOT NULL CHECK(admission_count >= 0)
                );
                INSERT INTO effect_dispatch_governor_meta
                    (singleton, schema_version, lane_count, admission_count)
                    VALUES (1, 1, 0, 0);
                CREATE TABLE effect_dispatch_lanes (
                    tenant_id TEXT NOT NULL,
                    capability TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    policy_id TEXT NOT NULL,
                    policy_sha256 TEXT NOT NULL,
                    state_json TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, capability, operation, policy_id)
                );
                CREATE TABLE effect_dispatch_admissions (
                    tenant_id TEXT NOT NULL,
                    admission_id TEXT NOT NULL,
                    expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms > 0),
                    settlement TEXT,
                    admission_json TEXT NOT NULL,
                    PRIMARY KEY (tenant_id, admission_id)
                );
                CREATE INDEX effect_dispatch_admission_expiry
                    ON effect_dispatch_admissions (expires_at_ms);
                ",
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    } else if tables.iter().all(|present| *present) {
        validate_store(&transaction)?;
        transaction.commit().map_err(sql_error)
    } else {
        Err(governor_error("SQLite store is partial"))
    }
}

fn validate_existing_store(connection: &Connection) -> Result<(), HarnessError> {
    let tables = store_tables(connection)?;
    if tables.iter().all(|present| !present) {
        Ok(())
    } else if tables.iter().all(|present| *present) {
        validate_store(connection)
    } else {
        Err(governor_error("SQLite store is partial"))
    }
}

fn validate_store(connection: &Connection) -> Result<(), HarnessError> {
    let metadata = connection
        .prepare(
            "SELECT singleton, schema_version, lane_count, admission_count
             FROM effect_dispatch_governor_meta",
        )
        .and_then(|mut statement| {
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(sql_error)?;
    if metadata.len() != 1
        || metadata[0].0 != 1
        || metadata[0].1 != i64::from(EFFECT_DISPATCH_GOVERNOR_SCHEMA_VERSION)
        || metadata[0].2 < 0
        || metadata[0].3 < 0
    {
        return Err(governor_error("SQLite schema is unknown or malformed"));
    }
    let invalid_lanes: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM effect_dispatch_lanes
             WHERE length(CAST(tenant_id AS BLOB)) > 128
                OR length(CAST(capability AS BLOB)) = 0
                OR length(CAST(capability AS BLOB)) > 128
                OR length(CAST(operation AS BLOB)) = 0
                OR length(CAST(operation AS BLOB)) > 128
                OR length(CAST(policy_id AS BLOB)) = 0
                OR length(CAST(policy_id AS BLOB)) > ?1
                OR length(policy_sha256) != 64
                OR length(CAST(state_json AS BLOB)) > ?2",
            params![
                i64::try_from(MAX_POLICY_ID_BYTES)
                    .map_err(|_| governor_error("policy identity bound exceeds SQLite"))?,
                i64::try_from(MAX_LANE_STATE_JSON_BYTES)
                    .map_err(|_| governor_error("lane state bound exceeds SQLite"))?
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let invalid_admissions: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM effect_dispatch_admissions
             WHERE length(CAST(tenant_id AS BLOB)) > 128
                OR length(CAST(admission_id AS BLOB)) = 0
                OR length(CAST(admission_id AS BLOB)) > 256
                OR expires_at_ms <= 0
                OR (settlement IS NOT NULL AND settlement NOT IN
                    ('healthy', 'availability_failure', 'abandoned'))
                OR length(CAST(admission_json AS BLOB)) > ?1",
            [i64::try_from(MAX_ADMISSION_JSON_BYTES)
                .map_err(|_| governor_error("admission bound exceeds SQLite"))?],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let lane_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM effect_dispatch_lanes", [], |row| {
            row.get(0)
        })
        .map_err(sql_error)?;
    let admission_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM effect_dispatch_admissions",
            [],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if invalid_lanes != 0
        || invalid_admissions != 0
        || metadata[0].2 != lane_count
        || metadata[0].3 != admission_count
        || lane_count > i64::try_from(MAX_LANES).unwrap_or(i64::MAX)
        || admission_count > i64::try_from(MAX_ADMISSIONS).unwrap_or(i64::MAX)
    {
        return Err(governor_error("SQLite store contains invalid row metadata"));
    }
    Ok(())
}

fn store_tables(connection: &Connection) -> Result<[bool; 3], HarnessError> {
    Ok([
        table_exists(connection, "effect_dispatch_governor_meta")?,
        table_exists(connection, "effect_dispatch_lanes")?,
        table_exists(connection, "effect_dispatch_admissions")?,
    ])
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
             )",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .map_err(sql_error)
}

fn prune_sql(transaction: &Transaction<'_>, at_ms: u64) -> Result<(), HarnessError> {
    let deleted = transaction
        .execute(
            "DELETE FROM effect_dispatch_admissions WHERE expires_at_ms <= ?1",
            [sql_time(at_ms)?],
        )
        .map_err(sql_error)?;
    if deleted == 0 {
        return Ok(());
    }
    update_counts(
        transaction,
        0,
        -i64::try_from(deleted)
            .map_err(|_| governor_error("pruned admission count exceeds SQLite"))?,
    )
}

fn load_counts(transaction: &Transaction<'_>) -> Result<(i64, i64), HarnessError> {
    transaction
        .query_row(
            "SELECT lane_count, admission_count
             FROM effect_dispatch_governor_meta WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)
}

fn update_counts(
    transaction: &Transaction<'_>,
    lane_delta: i64,
    admission_delta: i64,
) -> Result<(), HarnessError> {
    let changed = transaction
        .execute(
            "UPDATE effect_dispatch_governor_meta
             SET lane_count = lane_count + ?1,
                 admission_count = admission_count + ?2
             WHERE singleton = 1",
            params![lane_delta, admission_delta],
        )
        .map_err(sql_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(governor_error(
            "metadata count update changed an unexpected row count",
        ))
    }
}

fn load_lane(
    transaction: &Transaction<'_>,
    key: &LaneKey,
) -> Result<Option<LaneState>, HarnessError> {
    let row = transaction
        .query_row(
            "SELECT policy_sha256, length(CAST(state_json AS BLOB)), state_json
             FROM effect_dispatch_lanes
             WHERE tenant_id = ?1 AND capability = ?2 AND operation = ?3 AND policy_id = ?4",
            params![key.tenant_id, key.capability, key.operation, key.policy_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    bounded_text(row, 1, 2, MAX_LANE_STATE_JSON_BYTES, "Effect governor lane")?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(digest, encoded)| {
        let lane: LaneState = serde_json::from_str(&encoded)
            .map_err(|_| governor_error("cannot decode SQLite lane state"))?;
        if digest != lane.policy_sha256 {
            return Err(governor_error("SQLite lane digest disagrees with state"));
        }
        validate_lane_state(&lane)?;
        Ok(lane)
    })
    .transpose()
}

fn save_lane(
    transaction: &Transaction<'_>,
    key: &LaneKey,
    lane: &LaneState,
) -> Result<(), HarnessError> {
    let encoded = encode_bounded(lane, MAX_LANE_STATE_JSON_BYTES, "lane state")?;
    let changed = transaction
        .execute(
            "INSERT INTO effect_dispatch_lanes
                (tenant_id, capability, operation, policy_id, policy_sha256, state_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(tenant_id, capability, operation, policy_id) DO UPDATE SET
                policy_sha256 = excluded.policy_sha256,
                state_json = excluded.state_json",
            params![
                key.tenant_id,
                key.capability,
                key.operation,
                key.policy_id,
                lane.policy_sha256,
                encoded
            ],
        )
        .map_err(sql_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(governor_error("lane save changed an unexpected row count"))
    }
}

fn load_admission(
    transaction: &Transaction<'_>,
    tenant_id: &str,
    admission_id: &EffectLeaseId,
) -> Result<Option<AdmissionRecord>, HarnessError> {
    let row = transaction
        .query_row(
            "SELECT expires_at_ms, settlement,
                    length(CAST(admission_json AS BLOB)), admission_json
             FROM effect_dispatch_admissions
             WHERE tenant_id = ?1 AND admission_id = ?2",
            params![tenant_id, admission_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    bounded_text(
                        row,
                        2,
                        3,
                        MAX_ADMISSION_JSON_BYTES,
                        "Effect governor admission",
                    )?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?;
    row.map(|(expires_at_ms, settlement, encoded)| {
        let mut record: AdmissionRecord = serde_json::from_str(&encoded)
            .map_err(|_| governor_error("cannot decode SQLite admission"))?;
        validate_admission_record(&record)?;
        if expires_at_ms != sql_time(record.expires_at_ms)? {
            return Err(governor_error(
                "SQLite admission expiry disagrees with state",
            ));
        }
        let column = settlement.as_deref().map(parse_settlement).transpose()?;
        if record.settlement.is_some() && record.settlement != column {
            return Err(governor_error(
                "SQLite admission settlement disagrees with state",
            ));
        }
        record.settlement = column;
        Ok(record)
    })
    .transpose()
}

fn save_admission(
    transaction: &Transaction<'_>,
    tenant_id: &str,
    admission_id: &EffectLeaseId,
    record: &AdmissionRecord,
) -> Result<(), HarnessError> {
    let encoded = encode_bounded(record, MAX_ADMISSION_JSON_BYTES, "admission")?;
    let changed = transaction
        .execute(
            "INSERT INTO effect_dispatch_admissions
                (tenant_id, admission_id, expires_at_ms, settlement, admission_json)
             VALUES (?1, ?2, ?3, NULL, ?4)",
            params![
                tenant_id,
                admission_id.as_str(),
                sql_time(record.expires_at_ms)?,
                encoded
            ],
        )
        .map_err(sql_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(governor_error(
            "admission save changed an unexpected row count",
        ))
    }
}

fn encode_bounded<T: Serialize>(
    value: &T,
    maximum: usize,
    kind: &str,
) -> Result<String, HarnessError> {
    let encoded = serde_json::to_string(value)
        .map_err(|_| governor_error(format!("cannot encode {kind}")))?;
    if encoded.len() > maximum {
        return Err(governor_error(format!("{kind} exceeds {maximum} bytes")));
    }
    Ok(encoded)
}

fn settlement_name(settlement: EffectDispatchSettlement) -> &'static str {
    match settlement {
        EffectDispatchSettlement::Healthy => "healthy",
        EffectDispatchSettlement::AvailabilityFailure => "availability_failure",
        EffectDispatchSettlement::Abandoned => "abandoned",
    }
}

fn parse_settlement(value: &str) -> Result<EffectDispatchSettlement, HarnessError> {
    match value {
        "healthy" => Ok(EffectDispatchSettlement::Healthy),
        "availability_failure" => Ok(EffectDispatchSettlement::AvailabilityFailure),
        "abandoned" => Ok(EffectDispatchSettlement::Abandoned),
        _ => Err(governor_error("SQLite admission settlement is invalid")),
    }
}

fn sql_time(value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| governor_error("time exceeds SQLite"))
}

fn sql_error(error: rusqlite::Error) -> HarnessError {
    governor_error(format!("SQLite: {error}"))
}

fn effect_error(error: HarnessError) -> HarnessError {
    governor_error(error.to_string())
}

fn governor_error(message: impl Into<String>) -> HarnessError {
    HarnessError::Effect(format!("Effect dispatch governor: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActorIdentity, EventId};

    fn authority(tenant: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "worker".to_owned(),
            },
            Some(tenant.to_owned()),
        )
        .expect("authority")
    }

    fn policy(id: &str) -> EffectDispatchGovernorPolicy {
        EffectDispatchGovernorPolicy {
            policy_id: id.to_owned(),
            max_dispatches_per_window: 2,
            window_ms: 1_000,
            failure_threshold: 2,
            open_duration_ms: 500,
            probe_lease_ms: 100,
            admission_retention_ms: MIN_ADMISSION_RETENTION_MS,
        }
    }

    fn request(
        id: &str,
        at_ms: u64,
        policy: EffectDispatchGovernorPolicy,
    ) -> EffectDispatchAdmissionRequest {
        EffectDispatchAdmissionRequest {
            admission_id: EffectLeaseId::from_string(id.to_owned()),
            operation: EffectOperation {
                capability: "notification.test".to_owned(),
                operation: "send".to_owned(),
            },
            policy,
            admitted_at_ms: at_ms,
        }
    }

    fn sqlite_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-effect-governor-{label}-{}.db",
            EventId::generate()
        ))
    }

    fn remove_sqlite(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
    }

    async fn assert_contract(governor: &dyn EffectDispatchGovernor) {
        let tenant_a = authority("tenant-a");
        let tenant_b = authority("tenant-b");
        let policy = policy("notification-v1");

        assert_eq!(
            governor
                .admit_as(request("lease-a1", 1_100, policy.clone()), &tenant_a)
                .await
                .expect("first admission"),
            EffectDispatchAdmissionDecision::Allow
        );
        assert_eq!(
            governor
                .admit_as(request("lease-a2", 1_101, policy.clone()), &tenant_a)
                .await
                .expect("second admission"),
            EffectDispatchAdmissionDecision::Allow
        );
        assert_eq!(
            governor
                .admit_as(request("lease-a3", 1_102, policy.clone()), &tenant_a)
                .await
                .expect("limited admission"),
            EffectDispatchAdmissionDecision::RateLimited { retry_at_ms: 2_000 }
        );
        assert_eq!(
            governor
                .admit_as(request("lease-b1", 1_102, policy.clone()), &tenant_b)
                .await
                .expect("tenant-isolated admission"),
            EffectDispatchAdmissionDecision::Allow
        );

        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-a1"),
                EffectDispatchSettlement::AvailabilityFailure,
                1_200,
                &tenant_a,
            )
            .await
            .expect("first failure");
        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-a2"),
                EffectDispatchSettlement::AvailabilityFailure,
                1_201,
                &tenant_a,
            )
            .await
            .expect("second failure opens circuit");
        assert_eq!(
            governor
                .admit_as(request("lease-a4", 1_300, policy.clone()), &tenant_a)
                .await
                .expect("open admission"),
            EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms: 1_701 }
        );
        assert_eq!(
            governor
                .admit_as(request("lease-probe", 1_701, policy.clone()), &tenant_a)
                .await
                .expect("half-open probe"),
            EffectDispatchAdmissionDecision::AllowProbe
        );
        assert_eq!(
            governor
                .admit_as(request("lease-blocked", 1_702, policy.clone()), &tenant_a)
                .await
                .expect("probe exclusion"),
            EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms: 1_801 }
        );
        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-probe"),
                EffectDispatchSettlement::Abandoned,
                1_703,
                &tenant_a,
            )
            .await
            .expect("abandon probe");
        assert_eq!(
            governor
                .admit_as(request("lease-probe-2", 1_704, policy.clone()), &tenant_a,)
                .await
                .expect("replacement half-open probe"),
            EffectDispatchAdmissionDecision::AllowProbe
        );
        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-probe-2"),
                EffectDispatchSettlement::Healthy,
                1_705,
                &tenant_a,
            )
            .await
            .expect("healthy probe closes circuit");
        assert_eq!(
            governor
                .admit_as(request("lease-after", 2_001, policy.clone()), &tenant_a)
                .await
                .expect("closed admission"),
            EffectDispatchAdmissionDecision::Allow
        );

        let duplicate = governor
            .admit_as(request("lease-after", 9_999, policy.clone()), &tenant_a)
            .await
            .expect("idempotent duplicate");
        assert_eq!(duplicate, EffectDispatchAdmissionDecision::Allow);
        let mut changed = policy;
        changed.failure_threshold = 3;
        let error = governor
            .admit_as(request("lease-policy-drift", 2_002, changed), &tenant_a)
            .await
            .expect_err("reject policy drift");
        assert!(error.to_string().contains("different parameters"));
    }

    #[tokio::test]
    async fn memory_governor_enforces_atomic_lane_contract() {
        assert_contract(&MemoryEffectDispatchGovernor::new()).await;
    }

    #[tokio::test]
    async fn sqlite_governor_persists_lane_state_across_reopen() {
        let path = sqlite_path("reopen");
        {
            let governor = SqliteEffectDispatchGovernor::open(&path)
                .await
                .expect("open governor");
            assert_contract(&governor).await;
        }
        SqliteEffectDispatchGovernor::validate_existing(&path)
            .await
            .expect("validate governor");
        let governor = SqliteEffectDispatchGovernor::open(&path)
            .await
            .expect("reopen governor");
        assert_eq!(
            governor
                .admit_as(
                    request("lease-reopened", 2_002, policy("notification-v1")),
                    &authority("tenant-a"),
                )
                .await
                .expect("persisted admission"),
            EffectDispatchAdmissionDecision::Allow
        );
        drop(governor);
        remove_sqlite(&path);
    }

    #[tokio::test]
    async fn stale_epoch_and_duplicate_settlement_cannot_close_new_circuit() {
        let governor = MemoryEffectDispatchGovernor::new();
        let authority = authority("tenant-a");
        let mut policy = policy("epoch-v1");
        policy.failure_threshold = 1;
        governor
            .admit_as(request("lease-old", 1_000, policy.clone()), &authority)
            .await
            .expect("old admission");
        governor
            .admit_as(request("lease-open", 1_001, policy.clone()), &authority)
            .await
            .expect("opening admission");
        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-open"),
                EffectDispatchSettlement::AvailabilityFailure,
                1_002,
                &authority,
            )
            .await
            .expect("open circuit");
        governor
            .settle_as(
                &EffectLeaseId::from_static("lease-old"),
                EffectDispatchSettlement::Healthy,
                1_003,
                &authority,
            )
            .await
            .expect("stale healthy settlement");
        let decision = governor
            .admit_as(request("lease-denied", 1_100, policy), &authority)
            .await
            .expect("circuit remains open");
        assert_eq!(
            decision,
            EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms: 1_502 }
        );
    }

    #[tokio::test]
    async fn read_only_validation_does_not_create_a_missing_store() {
        let path = sqlite_path("missing");
        SqliteEffectDispatchGovernor::validate_existing(&path)
            .await
            .expect("validate missing store");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn independent_sqlite_connections_serialize_one_rate_slot() {
        let path = sqlite_path("concurrent");
        let left_store = SqliteEffectDispatchGovernor::open(&path)
            .await
            .expect("open left governor");
        let right_store = SqliteEffectDispatchGovernor::open(&path)
            .await
            .expect("open right governor");
        let mut policy = policy("concurrent-v1");
        policy.max_dispatches_per_window = 1;
        let authority = authority("tenant-a");
        let (left, right) = tokio::join!(
            left_store.admit_as(request("lease-left", 1_100, policy.clone()), &authority),
            right_store.admit_as(request("lease-right", 1_100, policy), &authority),
        );
        let decisions = [
            left.expect("left admission"),
            right.expect("right admission"),
        ];
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| **decision == EffectDispatchAdmissionDecision::Allow)
                .count(),
            1
        );
        assert_eq!(
            decisions
                .iter()
                .filter(|decision| {
                    **decision
                        == EffectDispatchAdmissionDecision::RateLimited { retry_at_ms: 2_000 }
                })
                .count(),
            1
        );
        drop((left_store, right_store));
        remove_sqlite(&path);
    }

    #[tokio::test]
    async fn concurrent_sqlite_bootstrap_is_atomic() {
        let path = sqlite_path("bootstrap");
        let (left, right) = tokio::join!(
            SqliteEffectDispatchGovernor::open(&path),
            SqliteEffectDispatchGovernor::open(&path),
        );
        let left = left.expect("left bootstrap");
        let right = right.expect("right bootstrap");
        SqliteEffectDispatchGovernor::validate_existing(&path)
            .await
            .expect("validate bootstrapped store");
        drop((left, right));
        remove_sqlite(&path);
    }

    #[tokio::test]
    async fn sqlite_open_circuit_survives_reopen() {
        let path = sqlite_path("open-circuit");
        let authority = authority("tenant-a");
        let mut policy = policy("persistent-circuit-v1");
        policy.failure_threshold = 1;
        {
            let governor = SqliteEffectDispatchGovernor::open(&path)
                .await
                .expect("open governor");
            governor
                .admit_as(request("lease-failure", 1_100, policy.clone()), &authority)
                .await
                .expect("admit failure");
            governor
                .settle_as(
                    &EffectLeaseId::from_static("lease-failure"),
                    EffectDispatchSettlement::AvailabilityFailure,
                    1_101,
                    &authority,
                )
                .await
                .expect("open circuit");
        }
        let governor = SqliteEffectDispatchGovernor::open(&path)
            .await
            .expect("reopen governor");
        assert_eq!(
            governor
                .admit_as(request("lease-denied", 1_200, policy), &authority)
                .await
                .expect("persistent denial"),
            EffectDispatchAdmissionDecision::CircuitOpen { retry_at_ms: 1_601 }
        );
        drop(governor);
        remove_sqlite(&path);
    }

    #[tokio::test]
    async fn read_only_validation_rejects_partial_store() {
        let path = sqlite_path("partial");
        Connection::open(&path)
            .expect("open partial store")
            .execute_batch(
                "CREATE TABLE effect_dispatch_governor_meta (
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );",
            )
            .expect("create partial store");
        let error = SqliteEffectDispatchGovernor::validate_existing(&path)
            .await
            .expect_err("reject partial store");
        assert!(error.to_string().contains("partial"));
        remove_sqlite(&path);
    }

    #[tokio::test]
    async fn lane_rejects_a_regressing_trusted_clock() {
        let governor = MemoryEffectDispatchGovernor::new();
        let authority = authority("tenant-a");
        let policy = policy("clock-v1");
        governor
            .admit_as(request("lease-newer", 2_000, policy.clone()), &authority)
            .await
            .expect("newer admission");
        let error = governor
            .admit_as(request("lease-older", 1_999, policy), &authority)
            .await
            .expect_err("reject regressing clock");
        assert!(error.to_string().contains("clock moved backwards"));
    }
}
