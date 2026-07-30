//! Durable, tenant-fenced external-effect intent and settlement ledger.
//!
//! An Effect records one caller-authorized external side-effect request before
//! a worker may execute it. A finite lease fences concurrent workers. A lease
//! that expires without a conclusive settlement becomes `Unknown`; it is never
//! made retryable implicitly.

mod coordinator;
mod engine;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    ActorIdentity, AuthorityContext, EffectCommandId, EffectLeaseId, HarnessError,
    json::{BoundedJsonError, bounded_serialized_size, validate_value_shape},
    kernel::validate_capability_name,
};

pub use coordinator::{
    EFFECT_LEDGER_SCHEMA_VERSION, EffectCommandResult, EffectCoordinator, EffectDueLease,
    EffectDueScanPage, EffectPage, EffectPageCursor, EffectSnapshot, MemoryEffectCoordinator,
    SqliteEffectCoordinator,
};
pub use engine::EffectEngine;

const MAX_EFFECT_TRANSITIONS: usize = 4_096;
const MAX_EFFECT_JSON_BYTES: usize = 16_777_216;
const MAX_EFFECT_COMMAND_JSON_BYTES: usize = 131_072;
const MAX_EFFECT_INPUT_BYTES: usize = 131_072;
const MAX_EFFECT_IDENTITY_BYTES: usize = 256;
const MIN_EFFECT_LEASE_MS: u64 = 1_000;
const MAX_EFFECT_LEASE_MS: u64 = 604_800_000;
const MAX_EFFECT_ATTEMPTS: u32 = 1_000_000;

/// Immutable target and operation coordinate for one external side effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectOperation {
    /// Registered Tool, Connector, Channel, or host capability name.
    pub capability: String,
    /// Stable capability-owned operation name.
    pub operation: String,
}

/// Caller-chosen, retry-stable durable Effect creation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCreateRequest {
    /// Stable command identity reused after an uncertain create response.
    pub command_id: EffectCommandId,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Stable target-system idempotency identity.
    pub idempotency_key: String,
    /// Bounded structured request captured before execution.
    pub input: Value,
    /// Earliest server-clock time at which a worker may claim the Effect.
    pub not_before_ms: u64,
}

/// Content-free receipt supplied by the authoritative external Connector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectReceipt {
    /// Stable source-system name.
    pub source: String,
    /// Source-system receipt, transaction, message, or operation identity.
    pub external_id: String,
    /// Source-system observation time in Unix milliseconds.
    pub observed_at_ms: u64,
    /// Lowercase SHA-256 of the normalized source response or proof.
    pub response_sha256: String,
}

/// Finite execution ownership for one exact attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectLease {
    /// Never-reused lease fence.
    pub id: EffectLeaseId,
    /// Trusted worker identity.
    pub owner: ActorIdentity,
    /// Positive attempt number.
    pub attempt: u32,
    /// Server-clock claim time.
    pub claimed_at_ms: u64,
    /// Exclusive lease expiration.
    pub expires_at_ms: u64,
}

/// Current authoritative Effect lifecycle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectStatus {
    /// Safe to claim at or after `not_before_ms`.
    Pending {
        /// Positive attempt that the next claim will own.
        next_attempt: u32,
        /// Inclusive eligibility boundary.
        not_before_ms: u64,
    },
    /// One worker owns execution until the exclusive lease boundary.
    Claimed {
        /// Exact current worker fence.
        lease: EffectLease,
    },
    /// The last attempt may or may not have reached the external system.
    Unknown {
        /// Attempt whose outcome must be reconciled.
        attempt: u32,
        /// Lease that owned the uncertain attempt.
        lease_id: EffectLeaseId,
        /// Content-free uncertainty classification.
        reason_code: String,
    },
    /// The external system authoritatively confirmed the side effect.
    Applied {
        /// Attempt that produced or was reconciled to the receipt.
        attempt: u32,
        /// Content-free external-system evidence.
        receipt: EffectReceipt,
    },
    /// The external system authoritatively confirmed no effect and no retry.
    Rejected {
        /// Attempt that was confirmed not applied.
        attempt: u32,
        /// Content-free terminal classification.
        reason_code: String,
    },
    /// A never-claimed pending Effect was explicitly cancelled.
    Cancelled {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

impl EffectStatus {
    /// Returns whether no later command may change this status.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Applied { .. } | Self::Rejected { .. } | Self::Cancelled { .. }
        )
    }
}

/// One actor-bound, idempotent Effect lifecycle mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectCommand {
    /// Stable identity reused after an uncertain response.
    pub id: EffectCommandId,
    /// Typed lifecycle mutation.
    pub kind: EffectCommandKind,
}

/// Typed Effect lifecycle mutations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectCommandKind {
    /// Acquire the currently eligible attempt.
    Claim {
        /// New never-reused execution fence.
        lease_id: EffectLeaseId,
        /// Finite ownership duration.
        lease_duration_ms: u64,
    },
    /// Extend the exact active lease.
    RenewLease {
        /// Exact active execution fence.
        lease_id: EffectLeaseId,
        /// Finite duration measured from command application time.
        lease_duration_ms: u64,
    },
    /// Settle an active attempt as authoritatively applied.
    RecordApplied {
        /// Exact active execution fence.
        lease_id: EffectLeaseId,
        /// External-system evidence.
        receipt: EffectReceipt,
    },
    /// Settle an active attempt as authoritatively not applied.
    RecordNotApplied {
        /// Exact active execution fence.
        lease_id: EffectLeaseId,
        /// Content-free outcome classification.
        reason_code: String,
        /// Explicit retry eligibility, or `None` for terminal rejection.
        retry_at_ms: Option<u64>,
    },
    /// Record that an active attempt has an uncertain external outcome.
    RecordUnknown {
        /// Exact active execution fence.
        lease_id: EffectLeaseId,
        /// Content-free uncertainty classification.
        reason_code: String,
    },
    /// Convert an expired active lease to `Unknown`.
    ExpireLease {
        /// Exact expired execution fence.
        lease_id: EffectLeaseId,
    },
    /// Reconcile an uncertain attempt as applied.
    ReconcileApplied {
        /// Exact uncertain execution fence.
        lease_id: EffectLeaseId,
        /// Exact uncertain attempt.
        attempt: u32,
        /// External-system evidence.
        receipt: EffectReceipt,
    },
    /// Reconcile an uncertain attempt as not applied.
    ReconcileNotApplied {
        /// Exact uncertain execution fence.
        lease_id: EffectLeaseId,
        /// Exact uncertain attempt.
        attempt: u32,
        /// Content-free outcome classification.
        reason_code: String,
        /// Explicit retry eligibility, or `None` for terminal rejection.
        retry_at_ms: Option<u64>,
    },
    /// Cancel an Effect before any attempt is claimed.
    Cancel {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

/// Whether a command changed durable Effect state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectApplyOutcome {
    /// A new transition was committed.
    Applied,
    /// The exact actor, identity, and command content were already committed.
    Duplicate,
}

/// Immutable Effect transition evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectTransition {
    /// Effect-local positive ordering.
    pub sequence: u64,
    /// Retry-stable command identity.
    pub command_id: EffectCommandId,
    /// Lowercase SHA-256 of the exact actor-bound command.
    pub command_sha256: String,
    /// Trusted server application time.
    pub applied_at_ms: u64,
    /// Trusted actor attributed by the host or transport.
    pub actor: ActorIdentity,
    /// Typed immutable transition evidence.
    pub kind: EffectTransitionKind,
}

/// Typed immutable Effect transition evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum EffectTransitionKind {
    /// One immutable intent was persisted before execution.
    Created {
        /// Immutable trusted tenant boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_id: Option<String>,
        /// External operation coordinate.
        operation: EffectOperation,
        /// Target-system idempotency identity.
        idempotency_key: String,
        /// Bounded structured request.
        input: Value,
        /// Digest of the exact request JSON.
        input_sha256: String,
        /// Earliest claim time.
        not_before_ms: u64,
    },
    /// One worker acquired an attempt.
    Claimed {
        /// New execution fence.
        lease_id: EffectLeaseId,
        /// Positive attempt owned by the lease.
        attempt: u32,
        /// Exact requested lease duration.
        lease_duration_ms: u64,
    },
    /// The current worker extended the same fence.
    LeaseRenewed {
        /// Existing execution fence.
        lease_id: EffectLeaseId,
        /// Exact requested lease duration.
        lease_duration_ms: u64,
        /// Prior exclusive expiration.
        previous_expires_at_ms: u64,
    },
    /// An attempt was authoritatively applied.
    Applied {
        /// Settled execution fence.
        lease_id: EffectLeaseId,
        /// Settled attempt.
        attempt: u32,
        /// External-system evidence.
        receipt: EffectReceipt,
        /// Whether this settlement reconciled an earlier unknown outcome.
        reconciled: bool,
    },
    /// An attempt was authoritatively not applied.
    NotApplied {
        /// Settled execution fence.
        lease_id: EffectLeaseId,
        /// Settled attempt.
        attempt: u32,
        /// Content-free outcome classification.
        reason_code: String,
        /// Explicit retry eligibility, or `None` for terminal rejection.
        retry_at_ms: Option<u64>,
        /// Whether this settlement reconciled an earlier unknown outcome.
        reconciled: bool,
    },
    /// An attempt entered the fail-closed unknown state.
    BecameUnknown {
        /// Uncertain execution fence.
        lease_id: EffectLeaseId,
        /// Uncertain attempt.
        attempt: u32,
        /// Content-free uncertainty classification.
        reason_code: String,
        /// Whether lease expiration, rather than a worker report, caused it.
        expired: bool,
    },
    /// A never-claimed Effect was cancelled.
    Cancelled {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

/// Pure serializable Effect aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Effect {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    operation: EffectOperation,
    idempotency_key: String,
    input: Value,
    input_sha256: String,
    created_at_ms: u64,
    status: EffectStatus,
    transitions: Vec<EffectTransition>,
    #[serde(skip)]
    materialization_charge_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectWire {
    #[serde(default)]
    tenant_id: Option<String>,
    operation: EffectOperation,
    idempotency_key: String,
    input: Value,
    input_sha256: String,
    created_at_ms: u64,
    status: EffectStatus,
    transitions: Vec<EffectTransition>,
}

impl<'de> Deserialize<'de> for Effect {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = EffectWire::deserialize(deserializer)?;
        let mut effect = Self {
            tenant_id: wire.tenant_id,
            operation: wire.operation,
            idempotency_key: wire.idempotency_key,
            input: wire.input,
            input_sha256: wire.input_sha256,
            created_at_ms: wire.created_at_ms,
            status: wire.status,
            transitions: wire.transitions,
            materialization_charge_bytes: 0,
        };
        effect.validate().map_err(D::Error::custom)?;
        effect.materialization_charge_bytes = encoded_size(&effect).map_err(D::Error::custom)?;
        Ok(effect)
    }
}

impl Effect {
    pub(crate) fn new(
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<Self, HarnessError> {
        validate_create_request(&request)?;
        validate_application_time(applied_at_ms)?;
        validate_authority(authority, "Effect creation authority")?;
        let input_sha256 = value_sha256(&request.input)?;
        let command_sha256 = attributed_digest(authority.actor(), authority.tenant_id(), &request)?;
        let transition = EffectTransition {
            sequence: 1,
            command_id: request.command_id,
            command_sha256,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: EffectTransitionKind::Created {
                tenant_id: authority.tenant_id().map(str::to_owned),
                operation: request.operation.clone(),
                idempotency_key: request.idempotency_key.clone(),
                input: request.input.clone(),
                input_sha256: input_sha256.clone(),
                not_before_ms: request.not_before_ms,
            },
        };
        let mut effect = Self {
            tenant_id: authority.tenant_id().map(str::to_owned),
            operation: request.operation,
            idempotency_key: request.idempotency_key,
            input: request.input,
            input_sha256,
            created_at_ms: applied_at_ms,
            status: EffectStatus::Pending {
                next_attempt: 1,
                not_before_ms: request.not_before_ms,
            },
            transitions: vec![transition],
            materialization_charge_bytes: 0,
        };
        effect.validate()?;
        effect.materialization_charge_bytes = encoded_size(&effect)?;
        Ok(effect)
    }

    /// Returns the immutable trusted tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the immutable external operation coordinate.
    #[must_use]
    pub fn operation(&self) -> &EffectOperation {
        &self.operation
    }

    /// Returns the target-system idempotency identity.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the bounded durable request.
    #[must_use]
    pub fn input(&self) -> &Value {
        &self.input
    }

    /// Returns the lowercase SHA-256 of the durable request.
    #[must_use]
    pub fn input_sha256(&self) -> &str {
        &self.input_sha256
    }

    /// Returns the trusted creation time.
    #[must_use]
    pub fn created_at_ms(&self) -> u64 {
        self.created_at_ms
    }

    /// Returns the current authoritative lifecycle.
    #[must_use]
    pub fn status(&self) -> &EffectStatus {
        &self.status
    }

    /// Returns immutable transitions in sequence order.
    pub fn transitions(&self) -> impl Iterator<Item = &EffectTransition> {
        self.transitions.iter()
    }

    /// Returns the number of retained transitions.
    #[must_use]
    pub fn transition_count(&self) -> usize {
        self.transitions.len()
    }

    /// Returns the conservative encoded materialization charge.
    #[must_use]
    pub fn materialization_charge_bytes(&self) -> usize {
        self.materialization_charge_bytes
    }

    pub(crate) fn create_matches(
        &self,
        request: &EffectCreateRequest,
        authority: &AuthorityContext,
    ) -> Result<bool, HarnessError> {
        validate_authority(authority, "Effect creation replay authority")?;
        if self.tenant_id() != authority.tenant_id() {
            return Ok(false);
        }
        let digest = attributed_digest(authority.actor(), authority.tenant_id(), request)?;
        Ok(self.transitions.first().is_some_and(|transition| {
            transition.command_id == request.command_id && transition.command_sha256 == digest
        }))
    }

    pub(crate) fn recognizes_command(
        &self,
        command: &EffectCommand,
        authority: &AuthorityContext,
    ) -> Result<bool, HarnessError> {
        validate_command(command)?;
        validate_authority(authority, "Effect command replay authority")?;
        if self.tenant_id() != authority.tenant_id() {
            return Ok(false);
        }
        let digest = attributed_digest(authority.actor(), authority.tenant_id(), command)?;
        let Some(existing) = self
            .transitions
            .iter()
            .find(|transition| transition.command_id == command.id)
        else {
            return Ok(false);
        };
        if existing.command_sha256 == digest {
            Ok(true)
        } else {
            Err(HarnessError::Effect(format!(
                "Effect command {} was reused by a different actor or with different content",
                command.id
            )))
        }
    }

    pub(crate) fn apply(
        &mut self,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EffectApplyOutcome, HarnessError> {
        if self.recognizes_command(&command, authority)? {
            return Ok(EffectApplyOutcome::Duplicate);
        }
        validate_application_time(applied_at_ms)?;
        validate_authority(authority, "Effect command authority")?;
        if self.transitions.len() >= MAX_EFFECT_TRANSITIONS {
            return Err(HarnessError::Effect(format!(
                "Effect exceeds {MAX_EFFECT_TRANSITIONS} transitions"
            )));
        }
        if self
            .transitions
            .last()
            .is_some_and(|transition| applied_at_ms < transition.applied_at_ms)
        {
            return Err(HarnessError::Effect(
                "Effect application time cannot move backwards".to_owned(),
            ));
        }
        let digest = attributed_digest(authority.actor(), authority.tenant_id(), &command)?;
        let mut next = self.clone();
        let kind = next.apply_kind(command.kind, applied_at_ms, authority.actor())?;
        let sequence = u64::try_from(next.transitions.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| HarnessError::Effect("Effect sequence overflow".to_owned()))?;
        next.transitions.push(EffectTransition {
            sequence,
            command_id: command.id,
            command_sha256: digest,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind,
        });
        next.validate()?;
        next.materialization_charge_bytes = encoded_size(&next)?;
        *self = next;
        Ok(EffectApplyOutcome::Applied)
    }

    fn apply_kind(
        &mut self,
        kind: EffectCommandKind,
        applied_at_ms: u64,
        actor: &ActorIdentity,
    ) -> Result<EffectTransitionKind, HarnessError> {
        match kind {
            EffectCommandKind::Claim {
                lease_id,
                lease_duration_ms,
            } => {
                let EffectStatus::Pending {
                    next_attempt,
                    not_before_ms,
                } = self.status
                else {
                    return Err(HarnessError::Effect(
                        "Effect must be pending before claim".to_owned(),
                    ));
                };
                if applied_at_ms < not_before_ms {
                    return Err(HarnessError::Effect(
                        "Effect is not eligible for claim".to_owned(),
                    ));
                }
                validate_new_lease_id(&self.transitions, &lease_id)?;
                validate_lease_duration(lease_duration_ms)?;
                validate_attempt(next_attempt)?;
                self.status = EffectStatus::Claimed {
                    lease: EffectLease {
                        id: lease_id.clone(),
                        owner: actor.clone(),
                        attempt: next_attempt,
                        claimed_at_ms: applied_at_ms,
                        expires_at_ms: lease_expiration(applied_at_ms, lease_duration_ms)?,
                    },
                };
                Ok(EffectTransitionKind::Claimed {
                    lease_id,
                    attempt: next_attempt,
                    lease_duration_ms,
                })
            }
            EffectCommandKind::RenewLease {
                lease_id,
                lease_duration_ms,
            } => {
                validate_lease_duration(lease_duration_ms)?;
                let lease = active_owned_lease_mut(
                    &mut self.status,
                    &lease_id,
                    actor,
                    applied_at_ms,
                    "renew",
                )?;
                let previous_expires_at_ms = lease.expires_at_ms;
                let expires_at_ms = lease_expiration(applied_at_ms, lease_duration_ms)?;
                if expires_at_ms <= previous_expires_at_ms {
                    return Err(HarnessError::Effect(
                        "Effect renewal must extend the current lease".to_owned(),
                    ));
                }
                lease.expires_at_ms = expires_at_ms;
                Ok(EffectTransitionKind::LeaseRenewed {
                    lease_id,
                    lease_duration_ms,
                    previous_expires_at_ms,
                })
            }
            EffectCommandKind::RecordApplied { lease_id, receipt } => {
                validate_receipt(&receipt, applied_at_ms)?;
                let lease =
                    active_owned_lease(&self.status, &lease_id, actor, applied_at_ms, "settle")?;
                let attempt = lease.attempt;
                self.status = EffectStatus::Applied {
                    attempt,
                    receipt: receipt.clone(),
                };
                Ok(EffectTransitionKind::Applied {
                    lease_id,
                    attempt,
                    receipt,
                    reconciled: false,
                })
            }
            EffectCommandKind::RecordNotApplied {
                lease_id,
                reason_code,
                retry_at_ms,
            } => {
                validate_capability_name("Effect not-applied reason", &reason_code)?;
                validate_retry_time(retry_at_ms, applied_at_ms)?;
                let lease =
                    active_owned_lease(&self.status, &lease_id, actor, applied_at_ms, "settle")?;
                let attempt = lease.attempt;
                self.status = settled_not_applied(attempt, &reason_code, retry_at_ms)?;
                Ok(EffectTransitionKind::NotApplied {
                    lease_id,
                    attempt,
                    reason_code,
                    retry_at_ms,
                    reconciled: false,
                })
            }
            EffectCommandKind::RecordUnknown {
                lease_id,
                reason_code,
            } => {
                validate_capability_name("Effect uncertainty reason", &reason_code)?;
                let lease =
                    active_owned_lease(&self.status, &lease_id, actor, applied_at_ms, "report")?;
                let attempt = lease.attempt;
                self.status = EffectStatus::Unknown {
                    attempt,
                    lease_id: lease_id.clone(),
                    reason_code: reason_code.clone(),
                };
                Ok(EffectTransitionKind::BecameUnknown {
                    lease_id,
                    attempt,
                    reason_code,
                    expired: false,
                })
            }
            EffectCommandKind::ExpireLease { lease_id } => {
                let EffectStatus::Claimed { lease } = &self.status else {
                    return Err(HarnessError::Effect(
                        "Effect has no current lease to expire".to_owned(),
                    ));
                };
                if lease.id != lease_id {
                    return Err(HarnessError::Effect(
                        "Effect expiration does not match the current lease".to_owned(),
                    ));
                }
                if applied_at_ms < lease.expires_at_ms {
                    return Err(HarnessError::Effect(
                        "Effect lease is not expired".to_owned(),
                    ));
                }
                let attempt = lease.attempt;
                let reason_code = "lease.expired".to_owned();
                self.status = EffectStatus::Unknown {
                    attempt,
                    lease_id: lease_id.clone(),
                    reason_code: reason_code.clone(),
                };
                Ok(EffectTransitionKind::BecameUnknown {
                    lease_id,
                    attempt,
                    reason_code,
                    expired: true,
                })
            }
            EffectCommandKind::ReconcileApplied {
                lease_id,
                attempt,
                receipt,
            } => {
                validate_receipt(&receipt, applied_at_ms)?;
                validate_unknown(&self.status, &lease_id, attempt)?;
                self.status = EffectStatus::Applied {
                    attempt,
                    receipt: receipt.clone(),
                };
                Ok(EffectTransitionKind::Applied {
                    lease_id,
                    attempt,
                    receipt,
                    reconciled: true,
                })
            }
            EffectCommandKind::ReconcileNotApplied {
                lease_id,
                attempt,
                reason_code,
                retry_at_ms,
            } => {
                validate_capability_name("Effect reconciliation reason", &reason_code)?;
                validate_retry_time(retry_at_ms, applied_at_ms)?;
                validate_unknown(&self.status, &lease_id, attempt)?;
                self.status = settled_not_applied(attempt, &reason_code, retry_at_ms)?;
                Ok(EffectTransitionKind::NotApplied {
                    lease_id,
                    attempt,
                    reason_code,
                    retry_at_ms,
                    reconciled: true,
                })
            }
            EffectCommandKind::Cancel { reason_code } => {
                validate_capability_name("Effect cancellation reason", &reason_code)?;
                if !matches!(
                    self.status,
                    EffectStatus::Pending {
                        next_attempt: 1,
                        ..
                    }
                ) {
                    return Err(HarnessError::Effect(
                        "only a never-claimed pending Effect may be cancelled".to_owned(),
                    ));
                }
                self.status = EffectStatus::Cancelled {
                    reason_code: reason_code.clone(),
                };
                Ok(EffectTransitionKind::Cancelled { reason_code })
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        validate_tenant(self.tenant_id())?;
        validate_operation(&self.operation)?;
        validate_identity("Effect idempotency key", &self.idempotency_key)?;
        validate_input(&self.input)?;
        validate_digest(&self.input_sha256, "input")?;
        if value_sha256(&self.input)? != self.input_sha256 {
            return Err(HarnessError::Effect(
                "Effect input digest differs from durable input".to_owned(),
            ));
        }
        validate_application_time(self.created_at_ms)?;
        if self.transitions.is_empty() || self.transitions.len() > MAX_EFFECT_TRANSITIONS {
            return Err(HarnessError::Effect(format!(
                "Effect must retain 1-{MAX_EFFECT_TRANSITIONS} transitions"
            )));
        }

        let mut projection: Option<EffectProjection> = None;
        let mut command_ids = std::collections::BTreeSet::new();
        let mut lease_ids = std::collections::BTreeSet::new();
        let mut previous_time = 0_u64;
        for transition in &self.transitions {
            let expected_sequence = u64::try_from(command_ids.len())
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| HarnessError::Effect("Effect sequence overflow".to_owned()))?;
            if transition.sequence != expected_sequence {
                return Err(HarnessError::Effect(
                    "Effect transition sequence is not contiguous".to_owned(),
                ));
            }
            validate_identity("Effect command", transition.command_id.as_str())?;
            validate_digest(&transition.command_sha256, "command")?;
            validate_application_time(transition.applied_at_ms)?;
            if transition.applied_at_ms < previous_time {
                return Err(HarnessError::Effect(
                    "Effect transition time is not monotonic".to_owned(),
                ));
            }
            previous_time = transition.applied_at_ms;
            validate_actor(&transition.actor)?;
            if transition.command_sha256 != transition_digest(transition, self.tenant_id())? {
                return Err(HarnessError::Effect(
                    "Effect command digest differs from transition content".to_owned(),
                ));
            }
            if !command_ids.insert(transition.command_id.as_str()) {
                return Err(HarnessError::Effect(
                    "Effect contains duplicate command identities".to_owned(),
                ));
            }
            apply_projection_transition(&mut projection, &mut lease_ids, transition)?;
        }
        let projection = projection
            .ok_or_else(|| HarnessError::Effect("Effect has no creation transition".to_owned()))?;
        if projection.tenant_id != self.tenant_id
            || projection.operation != self.operation
            || projection.idempotency_key != self.idempotency_key
            || projection.input != self.input
            || projection.input_sha256 != self.input_sha256
            || projection.created_at_ms != self.created_at_ms
            || projection.status != self.status
        {
            return Err(HarnessError::Effect(
                "Effect projection differs from immutable transition history".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct EffectProjection {
    tenant_id: Option<String>,
    operation: EffectOperation,
    idempotency_key: String,
    input: Value,
    input_sha256: String,
    created_at_ms: u64,
    status: EffectStatus,
}

fn apply_projection_transition(
    projection: &mut Option<EffectProjection>,
    lease_ids: &mut std::collections::BTreeSet<String>,
    transition: &EffectTransition,
) -> Result<(), HarnessError> {
    match &transition.kind {
        EffectTransitionKind::Created {
            tenant_id,
            operation,
            idempotency_key,
            input,
            input_sha256,
            not_before_ms,
        } => {
            if projection.is_some() || transition.sequence != 1 {
                return Err(HarnessError::Effect(
                    "Effect creation must be the first transition".to_owned(),
                ));
            }
            validate_operation(operation)?;
            validate_tenant(tenant_id.as_deref())?;
            validate_identity("Effect idempotency key", idempotency_key)?;
            validate_input(input)?;
            validate_digest(input_sha256, "input")?;
            if value_sha256(input)? != *input_sha256 {
                return Err(HarnessError::Effect(
                    "Effect creation input digest differs from input".to_owned(),
                ));
            }
            validate_application_time(*not_before_ms)?;
            *projection = Some(EffectProjection {
                tenant_id: tenant_id.clone(),
                operation: operation.clone(),
                idempotency_key: idempotency_key.clone(),
                input: input.clone(),
                input_sha256: input_sha256.clone(),
                created_at_ms: transition.applied_at_ms,
                status: EffectStatus::Pending {
                    next_attempt: 1,
                    not_before_ms: *not_before_ms,
                },
            });
        }
        kind => {
            let projection = projection.as_mut().ok_or_else(|| {
                HarnessError::Effect("Effect transition precedes creation".to_owned())
            })?;
            apply_projected_kind(projection, lease_ids, transition, kind)?;
        }
    }
    Ok(())
}

fn apply_projected_kind(
    projection: &mut EffectProjection,
    lease_ids: &mut std::collections::BTreeSet<String>,
    transition: &EffectTransition,
    kind: &EffectTransitionKind,
) -> Result<(), HarnessError> {
    match kind {
        EffectTransitionKind::Claimed {
            lease_id,
            attempt,
            lease_duration_ms,
        } => {
            let EffectStatus::Pending {
                next_attempt,
                not_before_ms,
            } = projection.status
            else {
                return invalid_history("claim does not follow pending state");
            };
            if transition.applied_at_ms < not_before_ms || *attempt != next_attempt {
                return invalid_history("claim differs from pending attempt or eligibility");
            }
            validate_identity("Effect lease", lease_id.as_str())?;
            validate_lease_duration(*lease_duration_ms)?;
            if !lease_ids.insert(lease_id.as_str().to_owned()) {
                return invalid_history("lease identity was reused");
            }
            projection.status = EffectStatus::Claimed {
                lease: EffectLease {
                    id: lease_id.clone(),
                    owner: transition.actor.clone(),
                    attempt: *attempt,
                    claimed_at_ms: transition.applied_at_ms,
                    expires_at_ms: lease_expiration(transition.applied_at_ms, *lease_duration_ms)?,
                },
            };
        }
        EffectTransitionKind::LeaseRenewed {
            lease_id,
            lease_duration_ms,
            previous_expires_at_ms,
        } => {
            validate_lease_duration(*lease_duration_ms)?;
            let lease = active_projected_lease(
                &mut projection.status,
                lease_id,
                &transition.actor,
                transition.applied_at_ms,
                "renewal",
            )?;
            if lease.expires_at_ms != *previous_expires_at_ms {
                return invalid_history("renewal prior expiration differs from current lease");
            }
            let next = lease_expiration(transition.applied_at_ms, *lease_duration_ms)?;
            if next <= lease.expires_at_ms {
                return invalid_history("renewal does not extend the current lease");
            }
            lease.expires_at_ms = next;
        }
        EffectTransitionKind::Applied {
            lease_id,
            attempt,
            receipt,
            reconciled,
        } => {
            validate_receipt(receipt, transition.applied_at_ms)?;
            if *reconciled {
                validate_unknown(&projection.status, lease_id, *attempt)?;
            } else {
                let lease = active_projected_lease(
                    &mut projection.status,
                    lease_id,
                    &transition.actor,
                    transition.applied_at_ms,
                    "settlement",
                )?;
                if lease.attempt != *attempt {
                    return invalid_history("applied attempt differs from active lease");
                }
            }
            projection.status = EffectStatus::Applied {
                attempt: *attempt,
                receipt: receipt.clone(),
            };
        }
        EffectTransitionKind::NotApplied {
            lease_id,
            attempt,
            reason_code,
            retry_at_ms,
            reconciled,
        } => {
            validate_capability_name("Effect not-applied reason", reason_code)?;
            validate_retry_time(*retry_at_ms, transition.applied_at_ms)?;
            if *reconciled {
                validate_unknown(&projection.status, lease_id, *attempt)?;
            } else {
                let lease = active_projected_lease(
                    &mut projection.status,
                    lease_id,
                    &transition.actor,
                    transition.applied_at_ms,
                    "settlement",
                )?;
                if lease.attempt != *attempt {
                    return invalid_history("not-applied attempt differs from active lease");
                }
            }
            projection.status = settled_not_applied(*attempt, reason_code, *retry_at_ms)?;
        }
        EffectTransitionKind::BecameUnknown {
            lease_id,
            attempt,
            reason_code,
            expired,
        } => {
            validate_capability_name("Effect uncertainty reason", reason_code)?;
            let EffectStatus::Claimed { lease } = &projection.status else {
                return invalid_history("unknown transition does not follow claimed state");
            };
            if lease.id != *lease_id || lease.attempt != *attempt {
                return invalid_history("unknown transition differs from active lease");
            }
            if *expired {
                if reason_code != "lease.expired" || transition.applied_at_ms < lease.expires_at_ms
                {
                    return invalid_history("lease-expiration evidence is inconsistent");
                }
            } else if lease.owner != transition.actor
                || transition.applied_at_ms >= lease.expires_at_ms
            {
                return invalid_history("worker uncertainty report is not actively leased");
            }
            projection.status = EffectStatus::Unknown {
                attempt: *attempt,
                lease_id: lease_id.clone(),
                reason_code: reason_code.clone(),
            };
        }
        EffectTransitionKind::Cancelled { reason_code } => {
            validate_capability_name("Effect cancellation reason", reason_code)?;
            if !matches!(
                projection.status,
                EffectStatus::Pending {
                    next_attempt: 1,
                    ..
                }
            ) {
                return invalid_history("cancellation follows a claimed attempt");
            }
            projection.status = EffectStatus::Cancelled {
                reason_code: reason_code.clone(),
            };
        }
        EffectTransitionKind::Created { .. } => unreachable!(),
    }
    Ok(())
}

fn transition_digest(
    transition: &EffectTransition,
    tenant_id: Option<&str>,
) -> Result<String, HarnessError> {
    let actor = &transition.actor;
    match &transition.kind {
        EffectTransitionKind::Created {
            tenant_id: created_tenant_id,
            operation,
            idempotency_key,
            input,
            not_before_ms,
            ..
        } => {
            if created_tenant_id.as_deref() != tenant_id {
                return Err(HarnessError::Effect(
                    "Effect creation tenant differs from aggregate tenant".to_owned(),
                ));
            }
            attributed_digest(
                actor,
                tenant_id,
                &EffectCreateRequest {
                    command_id: transition.command_id.clone(),
                    operation: operation.clone(),
                    idempotency_key: idempotency_key.clone(),
                    input: input.clone(),
                    not_before_ms: *not_before_ms,
                },
            )
        }
        kind => attributed_digest(
            actor,
            tenant_id,
            &EffectCommand {
                id: transition.command_id.clone(),
                kind: transition_command_kind(kind)?,
            },
        ),
    }
}

fn transition_command_kind(kind: &EffectTransitionKind) -> Result<EffectCommandKind, HarnessError> {
    Ok(match kind {
        EffectTransitionKind::Claimed {
            lease_id,
            lease_duration_ms,
            ..
        } => EffectCommandKind::Claim {
            lease_id: lease_id.clone(),
            lease_duration_ms: *lease_duration_ms,
        },
        EffectTransitionKind::LeaseRenewed {
            lease_id,
            lease_duration_ms,
            ..
        } => EffectCommandKind::RenewLease {
            lease_id: lease_id.clone(),
            lease_duration_ms: *lease_duration_ms,
        },
        EffectTransitionKind::Applied {
            lease_id,
            attempt,
            receipt,
            reconciled,
        } => {
            if *reconciled {
                EffectCommandKind::ReconcileApplied {
                    lease_id: lease_id.clone(),
                    attempt: *attempt,
                    receipt: receipt.clone(),
                }
            } else {
                EffectCommandKind::RecordApplied {
                    lease_id: lease_id.clone(),
                    receipt: receipt.clone(),
                }
            }
        }
        EffectTransitionKind::NotApplied {
            lease_id,
            attempt,
            reason_code,
            retry_at_ms,
            reconciled,
        } => {
            if *reconciled {
                EffectCommandKind::ReconcileNotApplied {
                    lease_id: lease_id.clone(),
                    attempt: *attempt,
                    reason_code: reason_code.clone(),
                    retry_at_ms: *retry_at_ms,
                }
            } else {
                EffectCommandKind::RecordNotApplied {
                    lease_id: lease_id.clone(),
                    reason_code: reason_code.clone(),
                    retry_at_ms: *retry_at_ms,
                }
            }
        }
        EffectTransitionKind::BecameUnknown {
            lease_id,
            reason_code,
            expired,
            ..
        } => {
            if *expired {
                EffectCommandKind::ExpireLease {
                    lease_id: lease_id.clone(),
                }
            } else {
                EffectCommandKind::RecordUnknown {
                    lease_id: lease_id.clone(),
                    reason_code: reason_code.clone(),
                }
            }
        }
        EffectTransitionKind::Cancelled { reason_code } => EffectCommandKind::Cancel {
            reason_code: reason_code.clone(),
        },
        EffectTransitionKind::Created { .. } => {
            return Err(HarnessError::Effect(
                "cannot reconstruct creation as an Effect command".to_owned(),
            ));
        }
    })
}

#[derive(Serialize)]
struct AttributedCommand<'a, T> {
    actor: &'a ActorIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<&'a str>,
    command: &'a T,
}

fn attributed_digest<T: Serialize>(
    actor: &ActorIdentity,
    tenant_id: Option<&str>,
    command: &T,
) -> Result<String, HarnessError> {
    validate_actor(actor)?;
    validate_tenant(tenant_id)?;
    let attributed = AttributedCommand {
        actor,
        tenant_id,
        command,
    };
    let size = bounded_serialized_size(&attributed, MAX_EFFECT_COMMAND_JSON_BYTES)
        .map_err(|error| bounded_error("command", MAX_EFFECT_COMMAND_JSON_BYTES, error))?;
    if size == 0 {
        return Err(HarnessError::Effect(
            "Effect command encoding is empty".to_owned(),
        ));
    }
    let encoded = serde_json::to_vec(&attributed)
        .map_err(|_| HarnessError::Effect("cannot encode Effect command".to_owned()))?;
    Ok(lower_sha256(&encoded))
}

fn value_sha256(value: &Value) -> Result<String, HarnessError> {
    validate_input(value)?;
    let encoded = serde_json::to_vec(value)
        .map_err(|_| HarnessError::Effect("cannot encode Effect input".to_owned()))?;
    Ok(lower_sha256(&encoded))
}

fn lower_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn validate_create_request(request: &EffectCreateRequest) -> Result<(), HarnessError> {
    validate_identity("Effect creation command", request.command_id.as_str())?;
    validate_operation(&request.operation)?;
    validate_identity("Effect idempotency key", &request.idempotency_key)?;
    validate_input(&request.input)?;
    validate_application_time(request.not_before_ms)
}

fn validate_command(command: &EffectCommand) -> Result<(), HarnessError> {
    validate_identity("Effect command", command.id.as_str())?;
    let _ = bounded_serialized_size(command, MAX_EFFECT_COMMAND_JSON_BYTES)
        .map_err(|error| bounded_error("command", MAX_EFFECT_COMMAND_JSON_BYTES, error))?;
    Ok(())
}

fn validate_operation(operation: &EffectOperation) -> Result<(), HarnessError> {
    validate_capability_name("Effect capability", &operation.capability)
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_capability_name("Effect operation", &operation.operation)
        .map_err(|error| HarnessError::Effect(error.to_string()))
}

fn validate_input(input: &Value) -> Result<(), HarnessError> {
    validate_value_shape(input)
        .map_err(|_| HarnessError::Effect("invalid Effect input shape".to_owned()))?;
    let size = bounded_serialized_size(input, MAX_EFFECT_INPUT_BYTES)
        .map_err(|error| bounded_error("input", MAX_EFFECT_INPUT_BYTES, error))?;
    if size == 0 {
        return Err(HarnessError::Effect("Effect input is empty".to_owned()));
    }
    Ok(())
}

fn validate_receipt(receipt: &EffectReceipt, applied_at_ms: u64) -> Result<(), HarnessError> {
    validate_capability_name("Effect receipt source", &receipt.source)
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_identity("Effect receipt external identity", &receipt.external_id)?;
    validate_application_time(receipt.observed_at_ms)?;
    if receipt.observed_at_ms > applied_at_ms {
        return Err(HarnessError::Effect(
            "Effect receipt observation time exceeds settlement time".to_owned(),
        ));
    }
    validate_digest(&receipt.response_sha256, "receipt response")
}

fn settled_not_applied(
    attempt: u32,
    reason_code: &str,
    retry_at_ms: Option<u64>,
) -> Result<EffectStatus, HarnessError> {
    if let Some(not_before_ms) = retry_at_ms {
        let next_attempt = attempt
            .checked_add(1)
            .filter(|attempt| *attempt <= MAX_EFFECT_ATTEMPTS)
            .ok_or_else(|| HarnessError::Effect("Effect attempt overflow".to_owned()))?;
        Ok(EffectStatus::Pending {
            next_attempt,
            not_before_ms,
        })
    } else {
        Ok(EffectStatus::Rejected {
            attempt,
            reason_code: reason_code.to_owned(),
        })
    }
}

fn validate_retry_time(value: Option<u64>, applied_at_ms: u64) -> Result<(), HarnessError> {
    if value.is_some_and(|retry_at_ms| retry_at_ms < applied_at_ms) {
        return Err(HarnessError::Effect(
            "Effect retry time cannot precede settlement".to_owned(),
        ));
    }
    if let Some(value) = value {
        validate_application_time(value)?;
    }
    Ok(())
}

fn active_owned_lease<'a>(
    status: &'a EffectStatus,
    lease_id: &EffectLeaseId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<&'a EffectLease, HarnessError> {
    let EffectStatus::Claimed { lease } = status else {
        return Err(HarnessError::Effect(format!(
            "Effect must be claimed to {action}"
        )));
    };
    if &lease.id != lease_id || &lease.owner != actor {
        return Err(HarnessError::Effect(format!(
            "Effect {action} does not match the current owner and lease"
        )));
    }
    if applied_at_ms >= lease.expires_at_ms {
        return Err(HarnessError::Effect(format!(
            "Effect lease expired before {action}"
        )));
    }
    Ok(lease)
}

fn active_owned_lease_mut<'a>(
    status: &'a mut EffectStatus,
    lease_id: &EffectLeaseId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<&'a mut EffectLease, HarnessError> {
    let EffectStatus::Claimed { lease } = status else {
        return Err(HarnessError::Effect(format!(
            "Effect must be claimed to {action}"
        )));
    };
    if &lease.id != lease_id || &lease.owner != actor {
        return Err(HarnessError::Effect(format!(
            "Effect {action} does not match the current owner and lease"
        )));
    }
    if applied_at_ms >= lease.expires_at_ms {
        return Err(HarnessError::Effect(format!(
            "Effect lease expired before {action}"
        )));
    }
    Ok(lease)
}

fn active_projected_lease<'a>(
    status: &'a mut EffectStatus,
    lease_id: &EffectLeaseId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<&'a mut EffectLease, HarnessError> {
    active_owned_lease_mut(status, lease_id, actor, applied_at_ms, action)
        .map_err(|_| HarnessError::Effect(format!("Effect history has invalid {action} evidence")))
}

fn validate_unknown(
    status: &EffectStatus,
    lease_id: &EffectLeaseId,
    attempt: u32,
) -> Result<(), HarnessError> {
    let EffectStatus::Unknown {
        attempt: current_attempt,
        lease_id: current_lease,
        ..
    } = status
    else {
        return Err(HarnessError::Effect(
            "Effect must be unknown before reconciliation".to_owned(),
        ));
    };
    if current_attempt != &attempt || current_lease != lease_id {
        return Err(HarnessError::Effect(
            "Effect reconciliation does not match the uncertain attempt and lease".to_owned(),
        ));
    }
    Ok(())
}

fn validate_new_lease_id(
    transitions: &[EffectTransition],
    lease_id: &EffectLeaseId,
) -> Result<(), HarnessError> {
    validate_identity("Effect lease", lease_id.as_str())?;
    if transitions.iter().any(|transition| {
        matches!(
            &transition.kind,
            EffectTransitionKind::Claimed {
                lease_id: existing,
                ..
            } if existing == lease_id
        )
    }) {
        return Err(HarnessError::Effect(format!(
            "Effect lease {lease_id} is already committed"
        )));
    }
    Ok(())
}

fn validate_lease_duration(duration_ms: u64) -> Result<(), HarnessError> {
    if !(MIN_EFFECT_LEASE_MS..=MAX_EFFECT_LEASE_MS).contains(&duration_ms) {
        return Err(HarnessError::Effect(format!(
            "Effect lease must be {MIN_EFFECT_LEASE_MS}-{MAX_EFFECT_LEASE_MS} milliseconds"
        )));
    }
    Ok(())
}

fn lease_expiration(applied_at_ms: u64, duration_ms: u64) -> Result<u64, HarnessError> {
    applied_at_ms
        .checked_add(duration_ms)
        .ok_or_else(|| HarnessError::Effect("Effect lease expiration overflow".to_owned()))
}

fn validate_attempt(attempt: u32) -> Result<(), HarnessError> {
    if attempt == 0 || attempt > MAX_EFFECT_ATTEMPTS {
        Err(HarnessError::Effect(format!(
            "Effect attempt must be 1-{MAX_EFFECT_ATTEMPTS}"
        )))
    } else {
        Ok(())
    }
}

fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_EFFECT_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::Effect(format!(
            "{kind} must be 1-{MAX_EFFECT_IDENTITY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_actor(actor: &ActorIdentity) -> Result<(), HarnessError> {
    actor
        .validate_current("Effect actor")
        .map_err(|error| HarnessError::Effect(error.to_string()))
}

fn validate_tenant(tenant_id: Option<&str>) -> Result<(), HarnessError> {
    tenant_id
        .map(AuthorityContext::validate_tenant)
        .transpose()
        .map(|_| ())
        .map_err(|error| HarnessError::Effect(error.to_string()))
}

fn validate_authority(authority: &AuthorityContext, kind: &str) -> Result<(), HarnessError> {
    authority
        .validate_current(kind)
        .map_err(|error| HarnessError::Effect(error.to_string()))
}

fn validate_digest(value: &str, kind: &str) -> Result<(), HarnessError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::Effect(format!(
            "Effect {kind} digest must be lowercase SHA-256"
        )));
    }
    Ok(())
}

fn validate_application_time(value: u64) -> Result<(), HarnessError> {
    if value == 0 {
        Err(HarnessError::Effect(
            "Effect application time must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn encoded_size(effect: &Effect) -> Result<usize, HarnessError> {
    bounded_serialized_size(effect, MAX_EFFECT_JSON_BYTES)
        .map_err(|error| bounded_error("aggregate", MAX_EFFECT_JSON_BYTES, error))
}

fn bounded_error(kind: &str, maximum: usize, error: BoundedJsonError) -> HarnessError {
    let detail = match error {
        BoundedJsonError::LimitExceeded => "exceeds its encoded-byte limit",
        BoundedJsonError::CannotEncode => "cannot be encoded",
    };
    HarnessError::Effect(format!("Effect {kind} {detail}; limit is {maximum} bytes"))
}

fn invalid_history(message: &str) -> Result<(), HarnessError> {
    Err(HarnessError::Effect(format!(
        "Effect transition history is invalid: {message}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(subject: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: subject.to_owned(),
            },
            Some("tenant".to_owned()),
        )
        .expect("authority")
    }

    fn create() -> EffectCreateRequest {
        EffectCreateRequest {
            command_id: EffectCommandId::from_static("create"),
            operation: EffectOperation {
                capability: "channel.email".to_owned(),
                operation: "send".to_owned(),
            },
            idempotency_key: "mail-42".to_owned(),
            input: serde_json::json!({"message_ref":"artifact-42"}),
            not_before_ms: 10,
        }
    }

    fn command(id: &str, kind: EffectCommandKind) -> EffectCommand {
        EffectCommand {
            id: EffectCommandId::from_string(id.to_owned()),
            kind,
        }
    }

    fn receipt(at: u64) -> EffectReceipt {
        EffectReceipt {
            source: "mail.provider".to_owned(),
            external_id: "provider-42".to_owned(),
            observed_at_ms: at,
            response_sha256: "a".repeat(64),
        }
    }

    #[test]
    fn expired_lease_becomes_unknown_and_never_implicitly_retries() {
        let worker = authority("worker");
        let reconciler = authority("reconciler");
        let mut effect = Effect::new(create(), 10, &worker).expect("create");
        let lease_id = EffectLeaseId::from_static("lease-1");
        effect
            .apply(
                command(
                    "claim",
                    EffectCommandKind::Claim {
                        lease_id: lease_id.clone(),
                        lease_duration_ms: 1_000,
                    },
                ),
                20,
                &worker,
            )
            .expect("claim");
        effect
            .apply(
                command(
                    "expire",
                    EffectCommandKind::ExpireLease {
                        lease_id: lease_id.clone(),
                    },
                ),
                1_020,
                &reconciler,
            )
            .expect("expire");
        assert!(matches!(effect.status(), EffectStatus::Unknown { .. }));
        let retry = effect
            .apply(
                command(
                    "claim-again",
                    EffectCommandKind::Claim {
                        lease_id: EffectLeaseId::from_static("lease-2"),
                        lease_duration_ms: 1_000,
                    },
                ),
                1_021,
                &worker,
            )
            .expect_err("unknown cannot be claimed");
        assert!(retry.to_string().contains("pending"));
        effect
            .apply(
                command(
                    "reconcile",
                    EffectCommandKind::ReconcileNotApplied {
                        lease_id,
                        attempt: 1,
                        reason_code: "provider.not_found".to_owned(),
                        retry_at_ms: Some(2_000),
                    },
                ),
                1_100,
                &reconciler,
            )
            .expect("reconcile");
        assert!(matches!(
            effect.status(),
            EffectStatus::Pending {
                next_attempt: 2,
                not_before_ms: 2_000
            }
        ));
    }

    #[test]
    fn exact_owner_and_lease_settle_once_with_durable_receipt() {
        let alice = authority("alice");
        let bob = authority("bob");
        let mut effect = Effect::new(create(), 10, &alice).expect("create");
        let lease_id = EffectLeaseId::from_static("lease");
        effect
            .apply(
                command(
                    "claim",
                    EffectCommandKind::Claim {
                        lease_id: lease_id.clone(),
                        lease_duration_ms: 1_000,
                    },
                ),
                20,
                &alice,
            )
            .expect("claim");
        let denied = effect
            .apply(
                command(
                    "settle-bob",
                    EffectCommandKind::RecordApplied {
                        lease_id: lease_id.clone(),
                        receipt: receipt(25),
                    },
                ),
                30,
                &bob,
            )
            .expect_err("wrong owner");
        assert!(denied.to_string().contains("current owner"));
        let settle = command(
            "settle",
            EffectCommandKind::RecordApplied {
                lease_id,
                receipt: receipt(25),
            },
        );
        effect.apply(settle.clone(), 30, &alice).expect("settle");
        assert_eq!(
            effect
                .apply(settle, 31, &alice)
                .expect("duplicate after terminal"),
            EffectApplyOutcome::Duplicate
        );
        assert!(matches!(effect.status(), EffectStatus::Applied { .. }));
    }

    #[test]
    fn deserialization_rebuilds_projection_and_rejects_tampering() {
        let actor = authority("worker");
        let mut effect = Effect::new(create(), 10, &actor).expect("create");
        effect
            .apply(
                command(
                    "cancel",
                    EffectCommandKind::Cancel {
                        reason_code: "operator.cancelled".to_owned(),
                    },
                ),
                11,
                &actor,
            )
            .expect("cancel");
        let encoded = serde_json::to_value(&effect).expect("encode");
        let decoded: Effect = serde_json::from_value(encoded.clone()).expect("decode");
        assert_eq!(decoded, effect);
        let mut tampered = encoded;
        tampered["status"]["reason_code"] = Value::String("different".to_owned());
        let error = serde_json::from_value::<Effect>(tampered).expect_err("tampered");
        assert!(error.to_string().contains("projection differs"));
    }
}
