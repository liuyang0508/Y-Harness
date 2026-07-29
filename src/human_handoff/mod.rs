//! Durable, lease-fenced ownership transfer from automation to a human actor.

mod coordinator;
mod engine;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use sha2::{Digest, Sha256};

use crate::{
    ActorIdentity, AuthorityContext, HarnessError, HumanHandoffClaimId, HumanHandoffCommandId,
    ThreadId, WorkflowRunId,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::validate_capability_name,
};

pub use coordinator::{
    HUMAN_HANDOFF_SCHEMA_VERSION, HumanHandoffCommandResult, HumanHandoffCoordinator,
    HumanHandoffCursor, HumanHandoffDueClaim, HumanHandoffDueScanPage, HumanHandoffPage,
    HumanHandoffSnapshot, MemoryHumanHandoffCoordinator, SqliteHumanHandoffCoordinator,
};
pub use engine::{HumanHandoffEngine, HumanHandoffSubjectResolver};

const MAX_HANDOFF_TRANSITIONS: usize = 4_096;
const MAX_HANDOFF_JSON_BYTES: usize = 16_777_216;
const MAX_HANDOFF_COMMAND_JSON_BYTES: usize = 131_072;
const MAX_HANDOFF_TEXT_BYTES: usize = 65_536;
const MAX_HANDOFF_IDENTITY_BYTES: usize = 256;
const MAX_HANDOFF_QUEUE_BYTES: usize = 64;
const MIN_HANDOFF_LEASE_MS: u64 = 1_000;
const MAX_HANDOFF_LEASE_MS: u64 = 604_800_000;

/// Authoritative Engine resource whose conversational or process ownership is
/// being transferred.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanHandoffSubject {
    /// One durable Agent conversation.
    Thread {
        /// Exact Thread identity in the same tenant boundary.
        thread_id: ThreadId,
    },
    /// One durable cross-time Workflow Run.
    WorkflowRun {
        /// Exact Workflow Run identity in the same tenant boundary.
        run_id: WorkflowRunId,
    },
}

/// Caller-chosen, retry-stable Human Handoff creation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanHandoffCreateRequest {
    /// Stable command identity reused after an uncertain response.
    pub command_id: HumanHandoffCommandId,
    /// Resource whose ownership is being transferred.
    pub subject: HumanHandoffSubject,
    /// Stable operator queue name.
    pub queue: String,
    /// Content-free reason classification.
    pub reason_code: String,
    /// Higher values are selected before lower values.
    pub priority: u8,
}

/// Current lease-fenced human ownership.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanHandoffClaim {
    /// Fences an old operator from a later claim.
    pub id: HumanHandoffClaimId,
    /// Trusted actor derived by the embedding host or transport.
    pub owner: ActorIdentity,
    /// Server-clock claim time in Unix milliseconds.
    pub claimed_at_ms: u64,
    /// Exclusive server-clock lease expiration.
    pub expires_at_ms: u64,
}

/// Current durable Human Handoff lifecycle projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanHandoffStatus {
    /// Discoverable by an authorized operator queue.
    Queued,
    /// Exclusively owned by one actor until the claim expires.
    Claimed {
        /// Exact current ownership fence.
        claim: HumanHandoffClaim,
    },
    /// Human handling settled successfully with one content-free outcome code.
    Resolved {
        /// Stable domain-owned outcome classification.
        outcome_code: String,
        /// Bounded operator-authored settlement summary.
        summary: String,
    },
    /// The requesting application or administrator stopped the handoff.
    Cancelled {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

impl HumanHandoffStatus {
    /// Returns whether no later command may change the case.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Resolved { .. } | Self::Cancelled { .. })
    }
}

/// Idempotent mutation submitted to one Human Handoff.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanHandoffCommand {
    /// Stable identity reused after an uncertain response.
    pub id: HumanHandoffCommandId,
    /// Typed lifecycle mutation.
    pub kind: HumanHandoffCommandKind,
}

/// Typed Human Handoff lifecycle mutations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanHandoffCommandKind {
    /// Claim one queued case for the authenticated actor.
    Claim {
        /// New ownership fence.
        claim_id: HumanHandoffClaimId,
        /// Requested finite lease duration.
        lease_duration_ms: u64,
    },
    /// Extend the exact current claim without changing its owner or fence.
    RenewClaim {
        /// Exact ownership fence observed by the operator.
        claim_id: HumanHandoffClaimId,
        /// Requested finite lease duration from the server application time.
        lease_duration_ms: u64,
    },
    /// Voluntarily return an owned case to its queue.
    ReleaseClaim {
        /// Exact ownership fence observed by the operator.
        claim_id: HumanHandoffClaimId,
        /// Content-free release classification.
        reason_code: String,
    },
    /// Return an expired exact claim to the queue.
    ExpireClaim {
        /// Exact expired ownership fence observed by the queue worker.
        claim_id: HumanHandoffClaimId,
    },
    /// Settle an actively owned case.
    Resolve {
        /// Exact ownership fence observed by the operator.
        claim_id: HumanHandoffClaimId,
        /// Content-free outcome classification.
        outcome_code: String,
        /// Bounded operator-authored settlement summary.
        summary: String,
    },
    /// Stop any nonterminal case without pretending it was handled.
    Cancel {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

/// Whether a Human Handoff command changed durable state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanHandoffApplyOutcome {
    /// A new transition was committed.
    Applied,
    /// The exact actor, command identity, and content were already committed.
    Duplicate,
}

/// Immutable Human Handoff transition evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HumanHandoffTransition {
    /// Case-local positive total ordering.
    pub sequence: u64,
    /// Retry-stable command identity.
    pub command_id: HumanHandoffCommandId,
    /// Lowercase SHA-256 of the exact actor-bound request or command.
    pub command_sha256: String,
    /// Server-clock application time in Unix milliseconds.
    pub applied_at_ms: u64,
    /// Trusted actor attributed by the embedding host or transport.
    pub actor: ActorIdentity,
    /// Typed transition evidence.
    pub kind: HumanHandoffTransitionKind,
}

/// Typed immutable Human Handoff transition evidence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum HumanHandoffTransitionKind {
    /// A new case entered one operator queue.
    Created {
        /// Resource whose ownership may transfer.
        subject: HumanHandoffSubject,
        /// Stable operator queue.
        queue: String,
        /// Content-free reason classification.
        reason_code: String,
        /// Higher values are selected first.
        priority: u8,
    },
    /// One authenticated actor acquired a finite ownership lease.
    Claimed {
        /// New ownership fence.
        claim_id: HumanHandoffClaimId,
        /// Exact requested lease duration.
        lease_duration_ms: u64,
    },
    /// The current owner extended the same claim fence.
    ClaimRenewed {
        /// Existing ownership fence.
        claim_id: HumanHandoffClaimId,
        /// Exact requested lease duration.
        lease_duration_ms: u64,
        /// Prior exclusive expiration.
        previous_expires_at_ms: u64,
    },
    /// The current owner voluntarily returned the case to its queue.
    ClaimReleased {
        /// Settled ownership fence.
        claim_id: HumanHandoffClaimId,
        /// Content-free release classification.
        reason_code: String,
    },
    /// A queue worker returned an expired case to its queue.
    ClaimExpired {
        /// Expired ownership fence.
        claim_id: HumanHandoffClaimId,
    },
    /// The current owner settled the case.
    Resolved {
        /// Settled ownership fence.
        claim_id: HumanHandoffClaimId,
        /// Content-free outcome classification.
        outcome_code: String,
        /// Bounded operator-authored settlement summary.
        summary: String,
    },
    /// An authorized caller stopped the case.
    Cancelled {
        /// Content-free cancellation classification.
        reason_code: String,
    },
}

/// Pure serializable Human Handoff aggregate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HumanHandoff {
    subject: HumanHandoffSubject,
    queue: String,
    reason_code: String,
    priority: u8,
    requested_at_ms: u64,
    status: HumanHandoffStatus,
    transitions: Vec<HumanHandoffTransition>,
    #[serde(skip)]
    materialization_charge_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanHandoffWire {
    subject: HumanHandoffSubject,
    queue: String,
    reason_code: String,
    priority: u8,
    requested_at_ms: u64,
    status: HumanHandoffStatus,
    transitions: Vec<HumanHandoffTransition>,
}

impl<'de> Deserialize<'de> for HumanHandoff {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = HumanHandoffWire::deserialize(deserializer)?;
        let mut handoff = Self {
            subject: wire.subject,
            queue: wire.queue,
            reason_code: wire.reason_code,
            priority: wire.priority,
            requested_at_ms: wire.requested_at_ms,
            status: wire.status,
            transitions: wire.transitions,
            materialization_charge_bytes: 0,
        };
        handoff.validate().map_err(D::Error::custom)?;
        handoff.materialization_charge_bytes = encoded_size(&handoff).map_err(D::Error::custom)?;
        Ok(handoff)
    }
}

impl HumanHandoff {
    pub(crate) fn new(
        request: HumanHandoffCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<Self, HarnessError> {
        validate_create_request(&request)?;
        validate_application_time(applied_at_ms)?;
        authority.validate_current("Human Handoff creation authority")?;
        let digest = attributed_digest(authority.actor(), &request)?;
        let transition = HumanHandoffTransition {
            sequence: 1,
            command_id: request.command_id,
            command_sha256: digest,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: HumanHandoffTransitionKind::Created {
                subject: request.subject.clone(),
                queue: request.queue.clone(),
                reason_code: request.reason_code.clone(),
                priority: request.priority,
            },
        };
        let mut handoff = Self {
            subject: request.subject,
            queue: request.queue,
            reason_code: request.reason_code,
            priority: request.priority,
            requested_at_ms: applied_at_ms,
            status: HumanHandoffStatus::Queued,
            transitions: vec![transition],
            materialization_charge_bytes: 0,
        };
        handoff.validate()?;
        handoff.materialization_charge_bytes = encoded_size(&handoff)?;
        Ok(handoff)
    }

    /// Returns the authoritative resource reference.
    #[must_use]
    pub fn subject(&self) -> &HumanHandoffSubject {
        &self.subject
    }

    /// Returns the stable operator queue.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Returns the content-free request reason.
    #[must_use]
    pub fn reason_code(&self) -> &str {
        &self.reason_code
    }

    /// Returns the queue priority.
    #[must_use]
    pub fn priority(&self) -> u8 {
        self.priority
    }

    /// Returns the server application time of the creation transition.
    #[must_use]
    pub fn requested_at_ms(&self) -> u64 {
        self.requested_at_ms
    }

    /// Returns the current lifecycle projection.
    #[must_use]
    pub fn status(&self) -> &HumanHandoffStatus {
        &self.status
    }

    /// Returns immutable transitions in sequence order.
    pub fn transitions(&self) -> impl Iterator<Item = &HumanHandoffTransition> {
        self.transitions.iter()
    }

    /// Returns the current number of immutable transitions.
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
        request: &HumanHandoffCreateRequest,
        actor: &ActorIdentity,
    ) -> Result<bool, HarnessError> {
        let digest = attributed_digest(actor, request)?;
        Ok(self.transitions.first().is_some_and(|transition| {
            transition.command_id == request.command_id && transition.command_sha256 == digest
        }))
    }

    pub(crate) fn recognizes_command(
        &self,
        command: &HumanHandoffCommand,
        actor: &ActorIdentity,
    ) -> Result<bool, HarnessError> {
        validate_command(command)?;
        validate_actor(actor)?;
        let digest = attributed_digest(actor, command)?;
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
            Err(HarnessError::HumanHandoff(format!(
                "Human Handoff command {} was reused by a different actor or with different content",
                command.id
            )))
        }
    }

    pub(crate) fn apply(
        &mut self,
        command: HumanHandoffCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffApplyOutcome, HarnessError> {
        if self.recognizes_command(&command, authority.actor())? {
            return Ok(HumanHandoffApplyOutcome::Duplicate);
        }
        validate_application_time(applied_at_ms)?;
        authority.validate_current("Human Handoff command authority")?;
        if self.transitions.len() >= MAX_HANDOFF_TRANSITIONS {
            return Err(HarnessError::HumanHandoff(format!(
                "Human Handoff exceeds {MAX_HANDOFF_TRANSITIONS} transitions"
            )));
        }
        if self
            .transitions
            .last()
            .is_some_and(|transition| applied_at_ms < transition.applied_at_ms)
        {
            return Err(HarnessError::HumanHandoff(
                "Human Handoff application time cannot move backwards".to_owned(),
            ));
        }
        let digest = attributed_digest(authority.actor(), &command)?;
        let mut next = self.clone();
        let transition_kind = next.apply_kind(command.kind, applied_at_ms, authority.actor())?;
        let sequence = u64::try_from(next.transitions.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                HarnessError::HumanHandoff("Human Handoff sequence overflow".to_owned())
            })?;
        next.transitions.push(HumanHandoffTransition {
            sequence,
            command_id: command.id,
            command_sha256: digest,
            applied_at_ms,
            actor: authority.actor().clone(),
            kind: transition_kind,
        });
        next.validate()?;
        next.materialization_charge_bytes = encoded_size(&next)?;
        *self = next;
        Ok(HumanHandoffApplyOutcome::Applied)
    }

    fn apply_kind(
        &mut self,
        kind: HumanHandoffCommandKind,
        applied_at_ms: u64,
        actor: &ActorIdentity,
    ) -> Result<HumanHandoffTransitionKind, HarnessError> {
        match kind {
            HumanHandoffCommandKind::Claim {
                claim_id,
                lease_duration_ms,
            } => {
                if !matches!(self.status, HumanHandoffStatus::Queued) {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff must be queued before claim".to_owned(),
                    ));
                }
                validate_new_claim_id(&self.transitions, &claim_id)?;
                validate_lease_duration(lease_duration_ms)?;
                let expires_at_ms = lease_expiration(applied_at_ms, lease_duration_ms)?;
                self.status = HumanHandoffStatus::Claimed {
                    claim: HumanHandoffClaim {
                        id: claim_id.clone(),
                        owner: actor.clone(),
                        claimed_at_ms: applied_at_ms,
                        expires_at_ms,
                    },
                };
                Ok(HumanHandoffTransitionKind::Claimed {
                    claim_id,
                    lease_duration_ms,
                })
            }
            HumanHandoffCommandKind::RenewClaim {
                claim_id,
                lease_duration_ms,
            } => {
                validate_lease_duration(lease_duration_ms)?;
                let claim = active_owned_claim_mut(
                    &mut self.status,
                    &claim_id,
                    actor,
                    applied_at_ms,
                    "renew",
                )?;
                let previous_expires_at_ms = claim.expires_at_ms;
                let expires_at_ms = lease_expiration(applied_at_ms, lease_duration_ms)?;
                if expires_at_ms <= previous_expires_at_ms {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff renewal must extend the current lease".to_owned(),
                    ));
                }
                claim.expires_at_ms = expires_at_ms;
                Ok(HumanHandoffTransitionKind::ClaimRenewed {
                    claim_id,
                    lease_duration_ms,
                    previous_expires_at_ms,
                })
            }
            HumanHandoffCommandKind::ReleaseClaim {
                claim_id,
                reason_code,
            } => {
                validate_capability_name("Human Handoff release reason", &reason_code)?;
                let _ =
                    active_owned_claim(&self.status, &claim_id, actor, applied_at_ms, "release")?;
                self.status = HumanHandoffStatus::Queued;
                Ok(HumanHandoffTransitionKind::ClaimReleased {
                    claim_id,
                    reason_code,
                })
            }
            HumanHandoffCommandKind::ExpireClaim { claim_id } => {
                let HumanHandoffStatus::Claimed { claim } = &self.status else {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff has no current claim to expire".to_owned(),
                    ));
                };
                if claim.id != claim_id {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff expiration does not match the current claim".to_owned(),
                    ));
                }
                if applied_at_ms < claim.expires_at_ms {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff claim is not expired".to_owned(),
                    ));
                }
                self.status = HumanHandoffStatus::Queued;
                Ok(HumanHandoffTransitionKind::ClaimExpired { claim_id })
            }
            HumanHandoffCommandKind::Resolve {
                claim_id,
                outcome_code,
                summary,
            } => {
                validate_capability_name("Human Handoff outcome", &outcome_code)?;
                validate_text("Human Handoff resolution summary", &summary)?;
                let _ =
                    active_owned_claim(&self.status, &claim_id, actor, applied_at_ms, "resolve")?;
                self.status = HumanHandoffStatus::Resolved {
                    outcome_code: outcome_code.clone(),
                    summary: summary.clone(),
                };
                Ok(HumanHandoffTransitionKind::Resolved {
                    claim_id,
                    outcome_code,
                    summary,
                })
            }
            HumanHandoffCommandKind::Cancel { reason_code } => {
                if self.status.is_terminal() {
                    return Err(HarnessError::HumanHandoff(
                        "terminal Human Handoff cannot be cancelled".to_owned(),
                    ));
                }
                validate_capability_name("Human Handoff cancellation reason", &reason_code)?;
                self.status = HumanHandoffStatus::Cancelled {
                    reason_code: reason_code.clone(),
                };
                Ok(HumanHandoffTransitionKind::Cancelled { reason_code })
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        validate_subject(&self.subject)?;
        validate_capability_name("Human Handoff queue", &self.queue)?;
        validate_capability_name("Human Handoff reason", &self.reason_code)?;
        validate_application_time(self.requested_at_ms)?;
        if self.transitions.is_empty() || self.transitions.len() > MAX_HANDOFF_TRANSITIONS {
            return Err(HarnessError::HumanHandoff(format!(
                "Human Handoff must retain 1-{MAX_HANDOFF_TRANSITIONS} transitions"
            )));
        }
        let mut expected_sequence = 1_u64;
        let mut command_ids = std::collections::BTreeSet::new();
        let mut claim_ids = std::collections::BTreeSet::new();
        let mut projected_subject = None;
        let mut projected_queue = None;
        let mut projected_reason = None;
        let mut projected_priority = None;
        let mut projected_requested_at = None;
        let mut projected_status = None;
        let mut previous_applied_at_ms = 0_u64;
        for transition in &self.transitions {
            if transition.sequence != expected_sequence {
                return Err(HarnessError::HumanHandoff(
                    "Human Handoff transition sequence is not contiguous".to_owned(),
                ));
            }
            expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                HarnessError::HumanHandoff("Human Handoff sequence overflow".to_owned())
            })?;
            validate_identity("Human Handoff command", transition.command_id.as_str())?;
            validate_digest(&transition.command_sha256)?;
            validate_application_time(transition.applied_at_ms)?;
            if transition.applied_at_ms < previous_applied_at_ms {
                return Err(HarnessError::HumanHandoff(
                    "Human Handoff transition time is not monotonic".to_owned(),
                ));
            }
            previous_applied_at_ms = transition.applied_at_ms;
            validate_actor(&transition.actor)?;
            if transition.command_sha256 != transition_digest(transition)? {
                return Err(HarnessError::HumanHandoff(
                    "Human Handoff command digest differs from transition content".to_owned(),
                ));
            }
            if !command_ids.insert(transition.command_id.as_str()) {
                return Err(HarnessError::HumanHandoff(
                    "Human Handoff contains duplicate command identities".to_owned(),
                ));
            }
            match &transition.kind {
                HumanHandoffTransitionKind::Created {
                    subject,
                    queue,
                    reason_code,
                    priority,
                } if transition.sequence == 1 => {
                    validate_subject(subject)?;
                    validate_capability_name("Human Handoff queue", queue)?;
                    validate_capability_name("Human Handoff reason", reason_code)?;
                    projected_subject = Some(subject.clone());
                    projected_queue = Some(queue.clone());
                    projected_reason = Some(reason_code.clone());
                    projected_priority = Some(*priority);
                    projected_requested_at = Some(transition.applied_at_ms);
                    projected_status = Some(HumanHandoffStatus::Queued);
                }
                HumanHandoffTransitionKind::Created { .. } => {
                    return Err(HarnessError::HumanHandoff(
                        "Human Handoff creation must be the first transition".to_owned(),
                    ));
                }
                HumanHandoffTransitionKind::Claimed {
                    claim_id,
                    lease_duration_ms,
                } => {
                    if !matches!(projected_status, Some(HumanHandoffStatus::Queued)) {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff claim does not follow queued state".to_owned(),
                        ));
                    }
                    validate_identity("Human Handoff claim", claim_id.as_str())?;
                    validate_lease_duration(*lease_duration_ms)?;
                    if !claim_ids.insert(claim_id.as_str()) {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff contains duplicate claim identities".to_owned(),
                        ));
                    }
                    projected_status = Some(HumanHandoffStatus::Claimed {
                        claim: HumanHandoffClaim {
                            id: claim_id.clone(),
                            owner: transition.actor.clone(),
                            claimed_at_ms: transition.applied_at_ms,
                            expires_at_ms: lease_expiration(
                                transition.applied_at_ms,
                                *lease_duration_ms,
                            )?,
                        },
                    });
                }
                HumanHandoffTransitionKind::ClaimRenewed {
                    claim_id,
                    lease_duration_ms,
                    previous_expires_at_ms,
                } => {
                    validate_lease_duration(*lease_duration_ms)?;
                    let Some(HumanHandoffStatus::Claimed { claim }) = projected_status.as_mut()
                    else {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff renewal does not follow claimed state".to_owned(),
                        ));
                    };
                    if &claim.id != claim_id
                        || claim.owner != transition.actor
                        || claim.expires_at_ms != *previous_expires_at_ms
                        || transition.applied_at_ms >= claim.expires_at_ms
                    {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff renewal differs from its active claim".to_owned(),
                        ));
                    }
                    let expires_at_ms =
                        lease_expiration(transition.applied_at_ms, *lease_duration_ms)?;
                    if expires_at_ms <= claim.expires_at_ms {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff renewal does not extend its claim".to_owned(),
                        ));
                    }
                    claim.expires_at_ms = expires_at_ms;
                }
                HumanHandoffTransitionKind::ClaimReleased {
                    claim_id,
                    reason_code,
                } => {
                    validate_capability_name("Human Handoff release reason", reason_code)?;
                    validate_projected_owner(
                        &projected_status,
                        claim_id,
                        &transition.actor,
                        transition.applied_at_ms,
                        "release",
                    )?;
                    projected_status = Some(HumanHandoffStatus::Queued);
                }
                HumanHandoffTransitionKind::ClaimExpired { claim_id } => {
                    let Some(HumanHandoffStatus::Claimed { claim }) = &projected_status else {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff expiration does not follow claimed state".to_owned(),
                        ));
                    };
                    if &claim.id != claim_id || transition.applied_at_ms < claim.expires_at_ms {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff expiration differs from its claim".to_owned(),
                        ));
                    }
                    projected_status = Some(HumanHandoffStatus::Queued);
                }
                HumanHandoffTransitionKind::Resolved {
                    claim_id,
                    outcome_code,
                    summary,
                } => {
                    validate_capability_name("Human Handoff outcome", outcome_code)?;
                    validate_text("Human Handoff resolution summary", summary)?;
                    validate_projected_owner(
                        &projected_status,
                        claim_id,
                        &transition.actor,
                        transition.applied_at_ms,
                        "resolution",
                    )?;
                    projected_status = Some(HumanHandoffStatus::Resolved {
                        outcome_code: outcome_code.clone(),
                        summary: summary.clone(),
                    });
                }
                HumanHandoffTransitionKind::Cancelled { reason_code } => {
                    if projected_status
                        .as_ref()
                        .is_none_or(HumanHandoffStatus::is_terminal)
                    {
                        return Err(HarnessError::HumanHandoff(
                            "Human Handoff cancellation does not follow nonterminal state"
                                .to_owned(),
                        ));
                    }
                    validate_capability_name("Human Handoff cancellation reason", reason_code)?;
                    projected_status = Some(HumanHandoffStatus::Cancelled {
                        reason_code: reason_code.clone(),
                    });
                }
            }
        }
        if projected_subject.as_ref() != Some(&self.subject)
            || projected_queue.as_deref() != Some(self.queue.as_str())
            || projected_reason.as_deref() != Some(self.reason_code.as_str())
            || projected_priority != Some(self.priority)
            || projected_requested_at != Some(self.requested_at_ms)
            || projected_status.as_ref() != Some(&self.status)
        {
            return Err(HarnessError::HumanHandoff(
                "Human Handoff projection differs from immutable transitions".to_owned(),
            ));
        }
        Ok(())
    }
}

fn active_owned_claim<'a>(
    status: &'a HumanHandoffStatus,
    claim_id: &HumanHandoffClaimId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<&'a HumanHandoffClaim, HarnessError> {
    let HumanHandoffStatus::Claimed { claim } = status else {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff must be claimed to {action}"
        )));
    };
    if &claim.id != claim_id || &claim.owner != actor {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff {action} does not match the current owner and claim"
        )));
    }
    if applied_at_ms >= claim.expires_at_ms {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff claim expired before {action}"
        )));
    }
    Ok(claim)
}

fn active_owned_claim_mut<'a>(
    status: &'a mut HumanHandoffStatus,
    claim_id: &HumanHandoffClaimId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<&'a mut HumanHandoffClaim, HarnessError> {
    let HumanHandoffStatus::Claimed { claim } = status else {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff must be claimed to {action}"
        )));
    };
    if &claim.id != claim_id || &claim.owner != actor {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff {action} does not match the current owner and claim"
        )));
    }
    if applied_at_ms >= claim.expires_at_ms {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff claim expired before {action}"
        )));
    }
    Ok(claim)
}

fn validate_projected_owner(
    status: &Option<HumanHandoffStatus>,
    claim_id: &HumanHandoffClaimId,
    actor: &ActorIdentity,
    applied_at_ms: u64,
    action: &str,
) -> Result<(), HarnessError> {
    let Some(HumanHandoffStatus::Claimed { claim }) = status else {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff {action} does not follow claimed state"
        )));
    };
    if &claim.id != claim_id || &claim.owner != actor || applied_at_ms >= claim.expires_at_ms {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff {action} differs from its active claim"
        )));
    }
    Ok(())
}

fn transition_digest(transition: &HumanHandoffTransition) -> Result<String, HarnessError> {
    match &transition.kind {
        HumanHandoffTransitionKind::Created {
            subject,
            queue,
            reason_code,
            priority,
        } => attributed_digest(
            &transition.actor,
            &HumanHandoffCreateRequest {
                command_id: transition.command_id.clone(),
                subject: subject.clone(),
                queue: queue.clone(),
                reason_code: reason_code.clone(),
                priority: *priority,
            },
        ),
        kind => {
            let kind = match kind {
                HumanHandoffTransitionKind::Created { .. } => unreachable!(),
                HumanHandoffTransitionKind::Claimed {
                    claim_id,
                    lease_duration_ms,
                } => HumanHandoffCommandKind::Claim {
                    claim_id: claim_id.clone(),
                    lease_duration_ms: *lease_duration_ms,
                },
                HumanHandoffTransitionKind::ClaimRenewed {
                    claim_id,
                    lease_duration_ms,
                    ..
                } => HumanHandoffCommandKind::RenewClaim {
                    claim_id: claim_id.clone(),
                    lease_duration_ms: *lease_duration_ms,
                },
                HumanHandoffTransitionKind::ClaimReleased {
                    claim_id,
                    reason_code,
                } => HumanHandoffCommandKind::ReleaseClaim {
                    claim_id: claim_id.clone(),
                    reason_code: reason_code.clone(),
                },
                HumanHandoffTransitionKind::ClaimExpired { claim_id } => {
                    HumanHandoffCommandKind::ExpireClaim {
                        claim_id: claim_id.clone(),
                    }
                }
                HumanHandoffTransitionKind::Resolved {
                    claim_id,
                    outcome_code,
                    summary,
                } => HumanHandoffCommandKind::Resolve {
                    claim_id: claim_id.clone(),
                    outcome_code: outcome_code.clone(),
                    summary: summary.clone(),
                },
                HumanHandoffTransitionKind::Cancelled { reason_code } => {
                    HumanHandoffCommandKind::Cancel {
                        reason_code: reason_code.clone(),
                    }
                }
            };
            attributed_digest(
                &transition.actor,
                &HumanHandoffCommand {
                    id: transition.command_id.clone(),
                    kind,
                },
            )
        }
    }
}

#[derive(Serialize)]
struct AttributedCommand<'a, T> {
    actor: &'a ActorIdentity,
    command: &'a T,
}

fn attributed_digest<T: Serialize>(
    actor: &ActorIdentity,
    command: &T,
) -> Result<String, HarnessError> {
    validate_actor(actor)?;
    let attributed = AttributedCommand { actor, command };
    let size = bounded_serialized_size(&attributed, MAX_HANDOFF_COMMAND_JSON_BYTES)
        .map_err(|error| bounded_error("command", MAX_HANDOFF_COMMAND_JSON_BYTES, error))?;
    if size == 0 {
        return Err(HarnessError::HumanHandoff(
            "Human Handoff command encoding is empty".to_owned(),
        ));
    }
    let encoded = serde_json::to_vec(&attributed).map_err(|_| {
        HarnessError::HumanHandoff("cannot encode Human Handoff command".to_owned())
    })?;
    Ok(Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn validate_create_request(request: &HumanHandoffCreateRequest) -> Result<(), HarnessError> {
    validate_identity("Human Handoff command", request.command_id.as_str())?;
    validate_subject(&request.subject)?;
    validate_capability_name("Human Handoff queue", &request.queue)?;
    validate_capability_name("Human Handoff reason", &request.reason_code)
}

fn validate_command(command: &HumanHandoffCommand) -> Result<(), HarnessError> {
    validate_identity("Human Handoff command", command.id.as_str())?;
    let _ = bounded_serialized_size(command, MAX_HANDOFF_COMMAND_JSON_BYTES)
        .map_err(|error| bounded_error("command", MAX_HANDOFF_COMMAND_JSON_BYTES, error))?;
    Ok(())
}

fn validate_subject(subject: &HumanHandoffSubject) -> Result<(), HarnessError> {
    match subject {
        HumanHandoffSubject::Thread { thread_id } => {
            validate_identity("Human Handoff Thread", thread_id.as_str())
        }
        HumanHandoffSubject::WorkflowRun { run_id } => {
            validate_identity("Human Handoff Workflow Run", run_id.as_str())
        }
    }
}

fn validate_new_claim_id(
    transitions: &[HumanHandoffTransition],
    claim_id: &HumanHandoffClaimId,
) -> Result<(), HarnessError> {
    validate_identity("Human Handoff claim", claim_id.as_str())?;
    if transitions.iter().any(|transition| {
        matches!(
            &transition.kind,
            HumanHandoffTransitionKind::Claimed {
                claim_id: existing,
                ..
            } if existing == claim_id
        )
    }) {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff claim {claim_id} is already committed"
        )));
    }
    Ok(())
}

fn validate_lease_duration(duration_ms: u64) -> Result<(), HarnessError> {
    if !(MIN_HANDOFF_LEASE_MS..=MAX_HANDOFF_LEASE_MS).contains(&duration_ms) {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff lease must be {MIN_HANDOFF_LEASE_MS}-{MAX_HANDOFF_LEASE_MS} milliseconds"
        )));
    }
    Ok(())
}

fn lease_expiration(applied_at_ms: u64, duration_ms: u64) -> Result<u64, HarnessError> {
    applied_at_ms.checked_add(duration_ms).ok_or_else(|| {
        HarnessError::HumanHandoff("Human Handoff lease expiration overflow".to_owned())
    })
}

fn validate_text(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty() || value.len() > MAX_HANDOFF_TEXT_BYTES {
        return Err(HarnessError::HumanHandoff(format!(
            "{kind} must be 1-{MAX_HANDOFF_TEXT_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(HarnessError::HumanHandoff(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.trim().is_empty()
        || value.len() > MAX_HANDOFF_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(HarnessError::HumanHandoff(format!(
            "{kind} must be 1-{MAX_HANDOFF_IDENTITY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_actor(actor: &ActorIdentity) -> Result<(), HarnessError> {
    actor
        .validate_shape("Human Handoff actor")
        .map_err(|error| HarnessError::HumanHandoff(error.to_string()))?;
    if matches!(actor, ActorIdentity::UnattributedLegacy) {
        return Err(HarnessError::HumanHandoff(
            "Human Handoff cannot use a legacy unattributed actor".to_owned(),
        ));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), HarnessError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::HumanHandoff(
            "Human Handoff command digest must be lowercase SHA-256".to_owned(),
        ));
    }
    Ok(())
}

fn validate_application_time(value: u64) -> Result<(), HarnessError> {
    if value == 0 {
        Err(HarnessError::HumanHandoff(
            "Human Handoff application time must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn encoded_size(handoff: &HumanHandoff) -> Result<usize, HarnessError> {
    let size = bounded_serialized_size(handoff, MAX_HANDOFF_JSON_BYTES)
        .map_err(|error| bounded_error("aggregate", MAX_HANDOFF_JSON_BYTES, error))?;
    if size > MAX_HANDOFF_JSON_BYTES {
        return Err(HarnessError::HumanHandoff(format!(
            "Human Handoff exceeds {MAX_HANDOFF_JSON_BYTES} encoded bytes"
        )));
    }
    Ok(size)
}

fn bounded_error(kind: &str, maximum: usize, error: BoundedJsonError) -> HarnessError {
    let detail = match error {
        BoundedJsonError::LimitExceeded => "exceeds its encoded-byte limit",
        BoundedJsonError::CannotEncode => "cannot be encoded",
    };
    HarnessError::HumanHandoff(format!(
        "Human Handoff {kind} {detail}; limit is {maximum} bytes"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(subject: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: subject.to_owned(),
            },
            Some("tenant".to_owned()),
        )
        .expect("authority")
    }

    fn create() -> HumanHandoffCreateRequest {
        HumanHandoffCreateRequest {
            command_id: HumanHandoffCommandId::from_static("create"),
            subject: HumanHandoffSubject::Thread {
                thread_id: ThreadId::from_static("thread"),
            },
            queue: "support.primary".to_owned(),
            reason_code: "agent.escalation".to_owned(),
            priority: 7,
        }
    }

    fn command(id: &str, kind: HumanHandoffCommandKind) -> HumanHandoffCommand {
        HumanHandoffCommand {
            id: HumanHandoffCommandId::from_string(id.to_owned()),
            kind,
        }
    }

    #[test]
    fn claim_owner_and_exact_expiration_are_fenced() {
        let alice = actor("alice");
        let bob = actor("bob");
        let mut handoff = HumanHandoff::new(create(), 10, &alice).expect("create");
        let claim_id = HumanHandoffClaimId::from_static("claim");
        handoff
            .apply(
                command(
                    "claim",
                    HumanHandoffCommandKind::Claim {
                        claim_id: claim_id.clone(),
                        lease_duration_ms: 1_000,
                    },
                ),
                20,
                &alice,
            )
            .expect("claim");
        let wrong_owner = handoff
            .apply(
                command(
                    "resolve-bob",
                    HumanHandoffCommandKind::Resolve {
                        claim_id: claim_id.clone(),
                        outcome_code: "handled".to_owned(),
                        summary: "done".to_owned(),
                    },
                ),
                30,
                &bob,
            )
            .expect_err("wrong owner");
        assert!(wrong_owner.to_string().contains("current owner"));
        let at_expiration = handoff
            .apply(
                command(
                    "resolve-expired",
                    HumanHandoffCommandKind::Resolve {
                        claim_id: claim_id.clone(),
                        outcome_code: "handled".to_owned(),
                        summary: "done".to_owned(),
                    },
                ),
                1_020,
                &alice,
            )
            .expect_err("expiration wins");
        assert!(at_expiration.to_string().contains("expired"));
        handoff
            .apply(
                command("expire", HumanHandoffCommandKind::ExpireClaim { claim_id }),
                1_020,
                &bob,
            )
            .expect("expire");
        assert!(matches!(handoff.status(), HumanHandoffStatus::Queued));
    }

    #[test]
    fn renewal_release_reclaim_and_resolution_preserve_fences() {
        let alice = actor("alice");
        let mut handoff = HumanHandoff::new(create(), 10, &alice).expect("create");
        let first = HumanHandoffClaimId::from_static("first");
        handoff
            .apply(
                command(
                    "claim-first",
                    HumanHandoffCommandKind::Claim {
                        claim_id: first.clone(),
                        lease_duration_ms: 1_000,
                    },
                ),
                20,
                &alice,
            )
            .expect("claim");
        handoff
            .apply(
                command(
                    "renew",
                    HumanHandoffCommandKind::RenewClaim {
                        claim_id: first.clone(),
                        lease_duration_ms: 2_000,
                    },
                ),
                30,
                &alice,
            )
            .expect("renew");
        handoff
            .apply(
                command(
                    "release",
                    HumanHandoffCommandKind::ReleaseClaim {
                        claim_id: first.clone(),
                        reason_code: "operator.unavailable".to_owned(),
                    },
                ),
                40,
                &alice,
            )
            .expect("release");
        let duplicate_claim = handoff
            .apply(
                command(
                    "reuse-claim",
                    HumanHandoffCommandKind::Claim {
                        claim_id: first,
                        lease_duration_ms: 1_000,
                    },
                ),
                50,
                &alice,
            )
            .expect_err("claim ID reuse");
        assert!(duplicate_claim.to_string().contains("already committed"));
        let second = HumanHandoffClaimId::from_static("second");
        handoff
            .apply(
                command(
                    "claim-second",
                    HumanHandoffCommandKind::Claim {
                        claim_id: second.clone(),
                        lease_duration_ms: 1_000,
                    },
                ),
                60,
                &alice,
            )
            .expect("reclaim");
        handoff
            .apply(
                command(
                    "resolve",
                    HumanHandoffCommandKind::Resolve {
                        claim_id: second,
                        outcome_code: "handled".to_owned(),
                        summary: "Human completed the case.".to_owned(),
                    },
                ),
                70,
                &alice,
            )
            .expect("resolve");
        assert!(matches!(
            handoff.status(),
            HumanHandoffStatus::Resolved { .. }
        ));
    }

    #[test]
    fn idempotency_is_bound_to_actor_and_content() {
        let alice = actor("alice");
        let bob = actor("bob");
        let mut handoff = HumanHandoff::new(create(), 10, &alice).expect("create");
        let claim = command(
            "claim",
            HumanHandoffCommandKind::Claim {
                claim_id: HumanHandoffClaimId::from_static("claim"),
                lease_duration_ms: 1_000,
            },
        );
        handoff.apply(claim.clone(), 20, &alice).expect("claim");
        assert_eq!(
            handoff.apply(claim.clone(), 21, &alice).expect("replay"),
            HumanHandoffApplyOutcome::Duplicate
        );
        assert!(
            handoff
                .apply(claim.clone(), 22, &bob)
                .expect_err("actor collision")
                .to_string()
                .contains("different actor")
        );
        let changed = command(
            "claim",
            HumanHandoffCommandKind::Claim {
                claim_id: HumanHandoffClaimId::from_static("claim"),
                lease_duration_ms: 2_000,
            },
        );
        assert!(
            handoff
                .apply(changed, 23, &alice)
                .expect_err("content collision")
                .to_string()
                .contains("different content")
        );
    }

    #[test]
    fn deserialization_reconstructs_projection_and_actor_bound_digests() {
        let alice = actor("alice");
        let handoff = HumanHandoff::new(create(), 10, &alice).expect("create");
        let encoded = serde_json::to_value(&handoff).expect("encode");
        let mut projection_tamper = encoded.clone();
        projection_tamper["priority"] = serde_json::json!(99);
        assert!(
            serde_json::from_value::<HumanHandoff>(projection_tamper)
                .expect_err("projection tamper")
                .to_string()
                .contains("projection differs")
        );
        let mut actor_tamper = encoded;
        actor_tamper["transitions"][0]["actor"] = serde_json::json!({
            "kind": "authenticated",
            "authority": "test",
            "subject": "mallory"
        });
        assert!(
            serde_json::from_value::<HumanHandoff>(actor_tamper)
                .expect_err("actor tamper")
                .to_string()
                .contains("digest differs")
        );
    }
}
