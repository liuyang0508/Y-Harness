//! Bounded discovery contracts and the pure live-wait projection reducer.
//!
//! The journal remains authoritative. Event Stores apply the reducer in the
//! same atomic boundary as each event and expose only fixed-size discovery
//! coordinates to a host-driven Temporal tick.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    AgentLoopCloseCommandId, AgentLoopDenyCommandId, AgentLoopResumeCommandId, AgentLoopWaitId,
    ApprovalDecision, ApprovalId, ApprovalSettlementEvidence, AuthorityContext, EventId,
    HarnessError, ItemKind, StateEvent, StoredEvent, ThreadId, TurnId, WaitKind,
};

/// Maximum authoritative due rows returned by one State scan.
///
/// This matches the Temporal Driver's per-source hard ceiling.
pub const MAX_AGENT_LOOP_DUE_SCAN_LIMIT: usize = 256;

pub(crate) const MAX_STATE_IDENTITY_BYTES: usize = 256;
const LOWER_SHA256_BYTES: usize = 64;
const TIMEOUT_COMMAND_DOMAIN: &str = "y-harness.agent-loop.wait-timeout-command.v1";
const DENIAL_COMMAND_DOMAIN: &str = "y-harness.agent-loop.wait-denial-maintenance-command.v1";

/// Non-effecting execution phases that bounded maintenance may settle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLoopDuePhase {
    /// No Approval settlement has been accepted.
    Waiting,
    /// An Approve settlement was accepted, but no worker claimed execution.
    ReadyAllow,
    /// A Deny settlement was accepted but its atomic terminal event was lost
    /// with the process. Maintenance must finish denial, never timeout it.
    ReadyDeny,
}

impl AgentLoopDuePhase {
    const fn as_wire(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::ReadyAllow => "ready_allow",
            Self::ReadyDeny => "ready_deny",
        }
    }

    pub(super) const fn as_sql(self) -> i64 {
        match self {
            Self::Waiting => 1,
            Self::ReadyAllow => 2,
            Self::ReadyDeny => 3,
        }
    }

    pub(super) fn from_sql(value: i64) -> Result<Self, HarnessError> {
        match value {
            1 => Ok(Self::Waiting),
            2 => Ok(Self::ReadyAllow),
            3 => Ok(Self::ReadyDeny),
            _ => Err(invalid_due("projection phase is unsupported")),
        }
    }
}

/// Fixed-size live projection maintained atomically beside the State journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AgentLoopWaitProjection {
    pub phase: AgentLoopDuePhase,
    pub tenant_id: Option<String>,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub wait_id: AgentLoopWaitId,
    pub revision: u64,
    pub due_at_ms: Option<u64>,
    pub approval_id: ApprovalId,
    pub envelope_sha256: String,
    pub wait_started_event_id: EventId,
    pub current_transition_event_id: EventId,
    pub resume_command_id: Option<AgentLoopResumeCommandId>,
}

impl AgentLoopWaitProjection {
    pub(super) fn validate(&self) -> Result<(), HarnessError> {
        validate_identity("projection Thread", self.thread_id.as_str())?;
        validate_identity("projection Turn", self.turn_id.as_str())?;
        validate_identity("projection wait", self.wait_id.as_str())?;
        validate_identity("projection Approval", self.approval_id.as_str())?;
        validate_identity(
            "projection wait-start event",
            self.wait_started_event_id.as_str(),
        )?;
        validate_identity(
            "projection transition event",
            self.current_transition_event_id.as_str(),
        )?;
        if let Some(command_id) = &self.resume_command_id {
            validate_identity("projection resume command", command_id.as_str())?;
        }
        if let Some(tenant_id) = &self.tenant_id {
            AuthorityContext::validate_tenant(tenant_id)
                .map_err(|_| invalid_due("projection tenant is invalid"))?;
        }
        if self.due_at_ms == Some(0) || !is_lower_sha256(&self.envelope_sha256) {
            return Err(invalid_due(
                "projection due time or envelope digest is invalid",
            ));
        }
        match self.phase {
            AgentLoopDuePhase::Waiting
                if self.revision == 1
                    && self.resume_command_id.is_none()
                    && self.current_transition_event_id == self.wait_started_event_id => {}
            AgentLoopDuePhase::ReadyAllow | AgentLoopDuePhase::ReadyDeny
                if self.revision >= 2
                    && self.resume_command_id.is_some()
                    && self.current_transition_event_id != self.wait_started_event_id => {}
            _ => {
                return Err(invalid_due(
                    "projection phase, revision, or transition is inconsistent",
                ));
            }
        }
        if self.phase == AgentLoopDuePhase::ReadyDeny && self.due_at_ms.is_none() {
            return Err(invalid_due("accepted denial must be immediately due"));
        }
        Ok(())
    }

    pub(super) fn due_index_key(&self) -> Option<AgentLoopDueIndexKey> {
        self.due_at_ms.map(|due_at_ms| AgentLoopDueIndexKey {
            tenant_id: self.tenant_id.clone(),
            due_at_ms,
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            wait_id: self.wait_id.clone(),
        })
    }

    pub(super) fn due_wait(
        &self,
        expected_stream_version: u64,
        expected_stream_recovery_bytes: u64,
    ) -> Result<AgentLoopDueWait, HarnessError> {
        let due_at_ms = self
            .due_at_ms
            .ok_or_else(|| invalid_due("projection is not scheduled"))?;
        let due = AgentLoopDueWait {
            phase: self.phase,
            tenant_id: self.tenant_id.clone(),
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            wait_id: self.wait_id.clone(),
            revision: self.revision,
            due_at_ms,
            envelope_sha256: self.envelope_sha256.clone(),
            wait_started_event_id: self.wait_started_event_id.clone(),
            current_transition_event_id: self.current_transition_event_id.clone(),
            expected_stream_version,
            expected_stream_recovery_bytes,
        };
        due.validate(due_at_ms, self.tenant_id.as_deref())?;
        Ok(due)
    }
}

/// Total order used by Memory and SQLite keyset scans.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct AgentLoopDueIndexKey {
    pub tenant_id: Option<String>,
    pub due_at_ms: u64,
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub wait_id: AgentLoopWaitId,
}

/// Pure change requested by one validated lifecycle event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AgentLoopWaitProjectionChange {
    Unchanged,
    Upsert(AgentLoopWaitProjection),
    Delete(AgentLoopWaitProjection),
}

/// Reduces one lifecycle event against the exact current live row.
pub(super) fn projection_change(
    stored: &StoredEvent,
    current: Option<&AgentLoopWaitProjection>,
) -> Result<AgentLoopWaitProjectionChange, HarnessError> {
    projection_change_inner(stored, current, None)
}

/// Replays one lifecycle event copied into a provenanced materialized stream.
///
/// Fork and import preserve the immutable source envelope, including its
/// original Thread identity, while rebinding the outer `StoredEvent` to the
/// new local Thread. Only migration validation may allow that exact historical
/// rebind; ordinary Event Store appends remain strict through
/// [`projection_change`]. The caller must independently prove same-stream
/// provenance and that every rebound wait belongs to a terminal copied Turn.
pub(super) fn projection_change_for_materialized_replay(
    stored: &StoredEvent,
    current: Option<&AgentLoopWaitProjection>,
    source_thread_id: Option<&ThreadId>,
) -> Result<AgentLoopWaitProjectionChange, HarnessError> {
    projection_change_inner(stored, current, source_thread_id)
}

fn projection_change_inner(
    stored: &StoredEvent,
    current: Option<&AgentLoopWaitProjection>,
    source_thread_id: Option<&ThreadId>,
) -> Result<AgentLoopWaitProjectionChange, HarnessError> {
    let change = match &stored.event {
        StateEvent::WaitStarted {
            turn_id,
            transition,
            ..
        } => {
            if current.is_some() {
                return Err(invalid_due("WaitStarted found an existing live row"));
            }
            let ItemKind::AgentLoopWaitStarted { envelope } = &transition.kind else {
                return Err(invalid_due("WaitStarted transition kind is invalid"));
            };
            let WaitKind::Approval { request, .. } = &envelope.wait_kind;
            if source_thread_id.unwrap_or(&stored.thread_id) != &envelope.thread_id
                || turn_id != &envelope.turn_id
            {
                return Err(invalid_due("WaitStarted coordinates differ"));
            }
            AgentLoopWaitProjectionChange::Upsert(AgentLoopWaitProjection {
                phase: AgentLoopDuePhase::Waiting,
                tenant_id: envelope.tenant_id.clone(),
                thread_id: stored.thread_id.clone(),
                turn_id: turn_id.clone(),
                wait_id: envelope.wait_id.clone(),
                revision: envelope.revision,
                due_at_ms: envelope.expires_at_ms,
                approval_id: request.id.clone(),
                envelope_sha256: envelope.envelope_sha256.clone(),
                wait_started_event_id: stored.event_id.clone(),
                current_transition_event_id: stored.event_id.clone(),
                resume_command_id: None,
            })
        }
        StateEvent::AcceptResume {
            turn_id,
            transition,
            ..
        } => {
            let previous = require_current(current, &stored.thread_id, turn_id)?;
            if previous.phase != AgentLoopDuePhase::Waiting {
                return Err(invalid_due("AcceptResume requires Waiting"));
            }
            let ItemKind::AgentLoopResumeAccepted { evidence } = &transition.kind else {
                return Err(invalid_due("AcceptResume transition kind is invalid"));
            };
            if evidence.wait_id != previous.wait_id
                || evidence.previous_revision != previous.revision
                || previous.revision.checked_add(1) != Some(evidence.revision)
                || !settlement_matches_projection(&evidence.settlement, previous, source_thread_id)
            {
                return Err(invalid_due("AcceptResume fence differs"));
            }
            let (phase, due_at_ms) = match evidence.settlement.decision {
                ApprovalDecision::Approve => (AgentLoopDuePhase::ReadyAllow, previous.due_at_ms),
                ApprovalDecision::Deny { .. } => {
                    (AgentLoopDuePhase::ReadyDeny, Some(evidence.accepted_at_ms))
                }
            };
            let mut next = previous.clone();
            next.phase = phase;
            next.revision = evidence.revision;
            next.due_at_ms = due_at_ms;
            next.current_transition_event_id = stored.event_id.clone();
            next.resume_command_id = Some(evidence.command_id.clone());
            AgentLoopWaitProjectionChange::Upsert(next)
        }
        StateEvent::ClaimReady {
            turn_id,
            transition,
        } => {
            let previous = require_current(current, &stored.thread_id, turn_id)?;
            let ItemKind::AgentLoopReadyClaimed { evidence } = &transition.kind else {
                return Err(invalid_due("ClaimReady transition kind is invalid"));
            };
            if previous.phase != AgentLoopDuePhase::ReadyAllow
                || evidence.wait_id != previous.wait_id
                || evidence.previous_revision != previous.revision
                || previous.revision.checked_add(1) != Some(evidence.revision)
                || previous.resume_command_id.as_ref() != Some(&evidence.resume_command_id)
            {
                return Err(invalid_due("ClaimReady fence differs"));
            }
            AgentLoopWaitProjectionChange::Delete(previous.clone())
        }
        StateEvent::WaitClosed {
            turn_id,
            transition,
            ..
        } => {
            let previous = require_current(current, &stored.thread_id, turn_id)?;
            let ItemKind::AgentLoopWaitClosed { evidence } = &transition.kind else {
                return Err(invalid_due("WaitClosed transition kind is invalid"));
            };
            if !matches!(
                previous.phase,
                AgentLoopDuePhase::Waiting | AgentLoopDuePhase::ReadyAllow
            ) || evidence.wait_id != previous.wait_id
                || evidence.previous_revision != previous.revision
                || previous.revision.checked_add(1) != Some(evidence.revision)
            {
                return Err(invalid_due("WaitClosed fence differs"));
            }
            AgentLoopWaitProjectionChange::Delete(previous.clone())
        }
        StateEvent::DenyWait {
            turn_id,
            transition,
            ..
        } => {
            let previous = require_current(current, &stored.thread_id, turn_id)?;
            let ItemKind::AgentLoopWaitDenied { evidence } = &transition.kind else {
                return Err(invalid_due("DenyWait transition kind is invalid"));
            };
            if !matches!(
                previous.phase,
                AgentLoopDuePhase::Waiting | AgentLoopDuePhase::ReadyDeny
            ) || evidence.wait_id != previous.wait_id
                || evidence.previous_revision != previous.revision
                || previous.revision.checked_add(1) != Some(evidence.revision)
                || !settlement_matches_projection(&evidence.settlement, previous, source_thread_id)
                || !matches!(evidence.settlement.decision, ApprovalDecision::Deny { .. })
            {
                return Err(invalid_due("DenyWait fence differs"));
            }
            AgentLoopWaitProjectionChange::Delete(previous.clone())
        }
        StateEvent::TurnFinished { turn_id, .. } | StateEvent::TurnCompleted { turn_id, .. }
            if current.is_some_and(|row| row.turn_id == *turn_id) =>
        {
            return Err(invalid_due(
                "ordinary terminal event cannot bypass a live wait row",
            ));
        }
        _ => AgentLoopWaitProjectionChange::Unchanged,
    };
    if let AgentLoopWaitProjectionChange::Upsert(row) = &change {
        row.validate()?;
    }
    Ok(change)
}

fn require_current<'a>(
    current: Option<&'a AgentLoopWaitProjection>,
    thread_id: &ThreadId,
    turn_id: &TurnId,
) -> Result<&'a AgentLoopWaitProjection, HarnessError> {
    let current = current.ok_or_else(|| invalid_due("lifecycle event has no live row"))?;
    if &current.thread_id != thread_id || &current.turn_id != turn_id {
        return Err(invalid_due("lifecycle event coordinates differ"));
    }
    current.validate()?;
    Ok(current)
}

fn settlement_matches_projection(
    settlement: &ApprovalSettlementEvidence,
    projection: &AgentLoopWaitProjection,
    source_thread_id: Option<&ThreadId>,
) -> bool {
    settlement.request.id == projection.approval_id
        && settlement.tenant_id == projection.tenant_id
        && &settlement.request.authorization.thread_id
            == source_thread_id.unwrap_or(&projection.thread_id)
        && settlement.request.authorization.turn_id == projection.turn_id
}

/// Disposable continuation over the stable due-index order.
///
/// The order is `(due_at_ms, thread_id, turn_id, wait_id)`. Losing a cursor
/// can repeat bounded discovery, but cannot lose or mutate authoritative work.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLoopDueCursor {
    /// Inclusive effective time of the last visited due row.
    pub due_at_ms: u64,
    /// Owning Thread of the last visited due row.
    pub thread_id: ThreadId,
    /// Owning Turn of the last visited due row.
    pub turn_id: TurnId,
    /// Stable wait identity of the last visited due row.
    pub wait_id: AgentLoopWaitId,
}

impl AgentLoopDueCursor {
    /// Validates one untrusted disposable cursor before it reaches a store.
    pub(crate) fn validate(&self) -> Result<(), HarnessError> {
        if self.due_at_ms == 0 {
            return Err(invalid_due("cursor due time must be non-zero"));
        }
        validate_identity("cursor Thread", self.thread_id.as_str())?;
        validate_identity("cursor Turn", self.turn_id.as_str())?;
        validate_identity("cursor wait", self.wait_id.as_str())
    }
}

/// Exact current wait fence discovered from authoritative State.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AgentLoopDueWait {
    /// Non-effecting lifecycle phase observed by the scan.
    pub phase: AgentLoopDuePhase,
    /// Immutable tenant boundary copied from the wait envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Owning running Turn.
    pub turn_id: TurnId,
    /// Exact current wait identity.
    pub wait_id: AgentLoopWaitId,
    /// Positive optimistic lifecycle revision used by terminal CAS.
    pub revision: u64,
    /// Inclusive server-clock settlement boundary. This is the envelope expiry
    /// for timeout phases and resume acceptance time for `ReadyDeny`.
    pub due_at_ms: u64,
    /// Digest of the complete immutable wait envelope.
    pub envelope_sha256: String,
    /// Event that owns the complete immutable wait envelope.
    #[serde(skip)]
    pub(crate) wait_started_event_id: EventId,
    /// Event that owns the current lifecycle transition.
    #[serde(skip)]
    pub(crate) current_transition_event_id: EventId,
    /// Stream-version fence observed by the same due query.
    #[serde(skip)]
    pub(crate) expected_stream_version: u64,
    /// Recovery-byte fence observed by the same due query.
    #[serde(skip)]
    pub(crate) expected_stream_recovery_bytes: u64,
}

impl AgentLoopDueWait {
    /// Returns this row's disposable due-index continuation.
    #[must_use]
    pub fn cursor(&self) -> AgentLoopDueCursor {
        AgentLoopDueCursor {
            due_at_ms: self.due_at_ms,
            thread_id: self.thread_id.clone(),
            turn_id: self.turn_id.clone(),
            wait_id: self.wait_id.clone(),
        }
    }

    /// Validates one due row against trusted scan time and tenant scope.
    pub(crate) fn validate(
        &self,
        at_ms: u64,
        expected_tenant_id: Option<&str>,
    ) -> Result<(), HarnessError> {
        if at_ms == 0 || self.due_at_ms == 0 || self.due_at_ms > at_ms {
            return Err(invalid_due("wait has not reached its server due time"));
        }
        validate_identity("Thread", self.thread_id.as_str())?;
        validate_identity("Turn", self.turn_id.as_str())?;
        validate_identity("wait", self.wait_id.as_str())?;
        if let Some(tenant_id) = &self.tenant_id {
            AuthorityContext::validate_tenant(tenant_id)
                .map_err(|_| invalid_due("tenant identity is invalid"))?;
        }
        if self.tenant_id.as_deref() != expected_tenant_id {
            return Err(invalid_due("tenant projection differs from scan authority"));
        }
        let valid_revision = match self.phase {
            AgentLoopDuePhase::Waiting => self.revision == 1,
            AgentLoopDuePhase::ReadyAllow | AgentLoopDuePhase::ReadyDeny => self.revision >= 2,
        };
        if !valid_revision {
            return Err(invalid_due("phase and revision are inconsistent"));
        }
        if !is_lower_sha256(&self.envelope_sha256) {
            return Err(invalid_due("envelope digest is not lowercase SHA-256"));
        }
        validate_identity("wait-start event", self.wait_started_event_id.as_str())?;
        validate_identity(
            "current transition event",
            self.current_transition_event_id.as_str(),
        )?;
        let valid_transition = match self.phase {
            AgentLoopDuePhase::Waiting => {
                self.current_transition_event_id == self.wait_started_event_id
            }
            AgentLoopDuePhase::ReadyAllow | AgentLoopDuePhase::ReadyDeny => {
                self.current_transition_event_id != self.wait_started_event_id
            }
        };
        if !valid_transition {
            return Err(invalid_due(
                "phase and current transition event are inconsistent",
            ));
        }
        if self.expected_stream_version == 0 || self.expected_stream_recovery_bytes == 0 {
            return Err(invalid_due(
                "stream version and recovery-byte fences must be non-zero",
            ));
        }
        Ok(())
    }
}

/// One bounded page from the authoritative Agent Loop due index.
///
/// Every visited row is due, so `scanned` exactly equals `due.len()`. Stores
/// query with `limit + 1`, return at most `limit`, and expose the extra row only
/// through `has_more`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AgentLoopDueScanPage {
    /// Due waits in stable index order.
    pub due: Vec<AgentLoopDueWait>,
    /// Last returned index coordinate, or `None` for an empty page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<AgentLoopDueCursor>,
    /// Whether another due row existed at the same trusted scan time.
    pub has_more: bool,
    /// Number of authoritative due rows returned.
    pub scanned: usize,
}

impl AgentLoopDueScanPage {
    /// Validates boundedness, ordering, tenant isolation, and due eligibility.
    pub(crate) fn validate(
        &self,
        at_ms: u64,
        after: Option<&AgentLoopDueCursor>,
        scan_limit: usize,
        expected_tenant_id: Option<&str>,
    ) -> Result<(), HarnessError> {
        validate_scan_limit(scan_limit)?;
        if let Some(after) = after {
            after.validate()?;
        }
        if self.scanned != self.due.len() || self.scanned > scan_limit {
            return Err(invalid_due("page counts exceed the bounded request"));
        }
        if (self.scanned == 0) != self.next_cursor.is_none() {
            return Err(invalid_due("page cursor does not match its row count"));
        }
        if self.has_more && self.scanned != scan_limit {
            return Err(invalid_due("partial page cannot report more rows"));
        }

        let mut previous = after.cloned();
        let mut last_returned = None;
        for due in &self.due {
            due.validate(at_ms, expected_tenant_id)?;
            let cursor = due.cursor();
            if previous.as_ref().is_some_and(|prior| cursor <= *prior) {
                return Err(invalid_due("rows are not in advancing index order"));
            }
            previous = Some(cursor.clone());
            last_returned = Some(cursor);
        }
        if self.next_cursor.as_ref() != last_returned.as_ref() {
            return Err(invalid_due("page cursor is not its last returned row"));
        }
        Ok(())
    }
}

/// Derives the stable close-command identity for one exact due fence.
///
/// The digest binds tenant, aggregate coordinates, lifecycle phase, revision,
/// expiry, and the immutable envelope. State still performs the authoritative
/// revision and elapsed-time checks when applying the resulting command.
pub(crate) fn deterministic_timeout_command_id(
    due: &AgentLoopDueWait,
) -> Result<AgentLoopCloseCommandId, HarnessError> {
    if due.phase == AgentLoopDuePhase::ReadyDeny {
        return Err(invalid_due(
            "accepted denial cannot use the timeout command path",
        ));
    }
    // Validate structural fields without inventing a later scan time. Due
    // eligibility remains the page validator's responsibility.
    due.validate(due.due_at_ms, due.tenant_id.as_deref())?;
    let digest = maintenance_command_digest(TIMEOUT_COMMAND_DOMAIN, due)?;
    Ok(AgentLoopCloseCommandId::from_string(format!(
        "agent-loop-timeout-{digest}"
    )))
}

/// Derives the stable denial command for an accepted Deny that lost its worker.
pub(crate) fn deterministic_denial_command_id(
    due: &AgentLoopDueWait,
) -> Result<AgentLoopDenyCommandId, HarnessError> {
    if due.phase != AgentLoopDuePhase::ReadyDeny {
        return Err(invalid_due(
            "only an accepted denial can use the denial maintenance path",
        ));
    }
    due.validate(due.due_at_ms, due.tenant_id.as_deref())?;
    let digest = maintenance_command_digest(DENIAL_COMMAND_DOMAIN, due)?;
    Ok(AgentLoopDenyCommandId::from_string(format!(
        "agent-loop-denial-{digest}"
    )))
}

fn maintenance_command_digest(
    domain: &str,
    due: &AgentLoopDueWait,
) -> Result<String, HarnessError> {
    if domain.is_empty() || domain.len() > MAX_STATE_IDENTITY_BYTES || !domain.is_ascii() {
        return Err(invalid_due("maintenance command domain is invalid"));
    }
    let mut hasher = Sha256::new();
    hash_component(&mut hasher, b"domain", domain.as_bytes())?;
    hash_component(&mut hasher, b"phase", due.phase.as_wire().as_bytes())?;
    match &due.tenant_id {
        Some(tenant_id) => {
            hash_component(&mut hasher, b"tenant.presence", b"some")?;
            hash_component(&mut hasher, b"tenant.value", tenant_id.as_bytes())?;
        }
        None => hash_component(&mut hasher, b"tenant.presence", b"none")?,
    }
    hash_component(&mut hasher, b"thread", due.thread_id.as_str().as_bytes())?;
    hash_component(&mut hasher, b"turn", due.turn_id.as_str().as_bytes())?;
    hash_component(&mut hasher, b"wait", due.wait_id.as_str().as_bytes())?;
    hash_component(&mut hasher, b"revision.u64be", &due.revision.to_be_bytes())?;
    hash_component(
        &mut hasher,
        b"due_at_ms.u64be",
        &due.due_at_ms.to_be_bytes(),
    )?;
    hash_component(
        &mut hasher,
        b"envelope.sha256",
        due.envelope_sha256.as_bytes(),
    )?;
    hash_component(
        &mut hasher,
        b"wait_started.event",
        due.wait_started_event_id.as_str().as_bytes(),
    )?;
    hash_component(
        &mut hasher,
        b"current_transition.event",
        due.current_transition_event_id.as_str().as_bytes(),
    )?;
    let mut digest = String::with_capacity(LOWER_SHA256_BYTES);
    for byte in hasher.finalize() {
        write!(&mut digest, "{byte:02x}")
            .map_err(|_| invalid_due("cannot format timeout command digest"))?;
    }
    Ok(digest)
}

fn hash_component(
    hasher: &mut Sha256,
    field_type: &[u8],
    value: &[u8],
) -> Result<(), HarnessError> {
    let field_len = u64::try_from(field_type.len())
        .map_err(|_| invalid_due("maintenance command field type is too large"))?;
    let value_len = u64::try_from(value.len())
        .map_err(|_| invalid_due("maintenance command field value is too large"))?;
    hasher.update(field_len.to_be_bytes());
    hasher.update(field_type);
    hasher.update(value_len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

fn validate_scan_limit(scan_limit: usize) -> Result<(), HarnessError> {
    if !(1..=MAX_AGENT_LOOP_DUE_SCAN_LIMIT).contains(&scan_limit) {
        return Err(invalid_due(&format!(
            "scan limit must be 1-{MAX_AGENT_LOOP_DUE_SCAN_LIMIT}"
        )));
    }
    Ok(())
}

pub(super) fn validate_due_scan_request(
    at_ms: u64,
    after: Option<&AgentLoopDueCursor>,
    scan_limit: usize,
    tenant_id: Option<&str>,
) -> Result<(), HarnessError> {
    if at_ms == 0 {
        return Err(invalid_due("trusted scan time must be non-zero"));
    }
    validate_scan_limit(scan_limit)?;
    if let Some(after) = after {
        after.validate()?;
        if after.due_at_ms > at_ms {
            return Err(invalid_due("cursor is later than trusted scan time"));
        }
    }
    if let Some(tenant_id) = tenant_id {
        AuthorityContext::validate_tenant(tenant_id)
            .map_err(|_| invalid_due("scan tenant is invalid"))?;
    }
    Ok(())
}

pub(crate) fn validate_identity(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.is_empty()
        || value.len() > MAX_STATE_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(invalid_due(&format!(
            "{kind} identity must be 1-{MAX_STATE_IDENTITY_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == LOWER_SHA256_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid_due(reason: &str) -> HarnessError {
    HarnessError::State(format!("Agent Loop due scan {reason}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use crate::{
        ActorIdentity, AgentLoopClaimId, AgentLoopWorkerId, ApprovalRequest,
        ApprovalSettlementEvidence, CapabilityOrigin, CompletionAssurance, CompletionGeneration,
        ExecutionClaimEvidence, Item, ItemId, ResumeEvidence, RiskLevel, ToolAuthorization,
        ToolDescriptor, TurnStatus, TurnStopReason, TurnWaitEnvelope, WaitClosureEvidence,
        WaitDenialEvidence,
    };

    use super::*;

    fn projection(phase: AgentLoopDuePhase) -> AgentLoopWaitProjection {
        let ready = phase != AgentLoopDuePhase::Waiting;
        AgentLoopWaitProjection {
            phase,
            tenant_id: Some("tenant-a".to_owned()),
            thread_id: ThreadId::from_static("thread-reducer"),
            turn_id: TurnId::from_static("turn-reducer"),
            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
            revision: if ready { 2 } else { 1 },
            due_at_ms: Some(if phase == AgentLoopDuePhase::ReadyDeny {
                800
            } else {
                1_000
            }),
            approval_id: ApprovalId::from_static("approval-reducer"),
            envelope_sha256: "a".repeat(LOWER_SHA256_BYTES),
            wait_started_event_id: EventId::from_static("event-wait-start"),
            current_transition_event_id: EventId::from_static(if ready {
                "event-resume"
            } else {
                "event-wait-start"
            }),
            resume_command_id: ready
                .then(|| AgentLoopResumeCommandId::from_static("resume-command")),
        }
    }

    fn approval_request() -> ApprovalRequest {
        ApprovalRequest {
            id: ApprovalId::from_static("approval-reducer"),
            requested_by: ActorIdentity::LocalProcess,
            authorization: ToolAuthorization {
                thread_id: ThreadId::from_static("thread-reducer"),
                turn_id: TurnId::from_static("turn-reducer"),
                call_id: "call-reducer".to_owned(),
                descriptor: ToolDescriptor {
                    name: "test.tool".to_owned(),
                    description: "projection reducer fixture".to_owned(),
                    input_schema: json!({"type": "object"}),
                },
                origin: CapabilityOrigin::BuiltIn,
                input: json!({}),
            },
            reason: "test approval".to_owned(),
            risk: RiskLevel::Medium,
        }
    }

    fn settlement(decision: ApprovalDecision) -> ApprovalSettlementEvidence {
        ApprovalSettlementEvidence {
            inbox_schema_version: 3,
            request: approval_request(),
            tenant_id: Some("tenant-a".to_owned()),
            decision,
            decided_by: ActorIdentity::LocalProcess,
            inbox_revision: 2,
            requested_at_ms: 600,
            settled_at_ms: 700,
        }
    }

    fn item(id: &'static str, created_at_ms: u64, kind: ItemKind) -> Item {
        Item {
            id: ItemId::from_static(id),
            created_at_ms,
            kind,
        }
    }

    fn placeholder_item(id: &'static str, created_at_ms: u64) -> Item {
        item(
            id,
            created_at_ms,
            ItemKind::UserMessage {
                content: "projection reducer fixture".to_owned(),
            },
        )
    }

    fn stored(event_id: &'static str, event: StateEvent) -> StoredEvent {
        StoredEvent {
            schema_version: crate::STATE_EVENT_SCHEMA_VERSION,
            sequence: 1,
            event_id: EventId::from_static(event_id),
            thread_id: ThreadId::from_static("thread-reducer"),
            recorded_at_ms: 800,
            event,
        }
    }

    fn accept_resume(
        event_id: &'static str,
        decision: ApprovalDecision,
        previous_revision: u64,
    ) -> StoredEvent {
        let evidence = ResumeEvidence {
            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
            previous_revision,
            revision: previous_revision + 1,
            command_id: AgentLoopResumeCommandId::from_static("resume-command"),
            command_sha256: "b".repeat(LOWER_SHA256_BYTES),
            settlement: settlement(decision),
            accepted_at_ms: 800,
        };
        stored(
            event_id,
            StateEvent::AcceptResume {
                turn_id: TurnId::from_static("turn-reducer"),
                approval_decision: placeholder_item("item-resume-decision", 800),
                transition: item(
                    "item-resume-transition",
                    800,
                    ItemKind::AgentLoopResumeAccepted {
                        evidence: Box::new(evidence),
                    },
                ),
            },
        )
    }

    fn claim_ready(previous_revision: u64) -> StoredEvent {
        stored(
            "event-claim",
            StateEvent::ClaimReady {
                turn_id: TurnId::from_static("turn-reducer"),
                transition: item(
                    "item-claim-transition",
                    900,
                    ItemKind::AgentLoopReadyClaimed {
                        evidence: Box::new(ExecutionClaimEvidence {
                            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
                            previous_revision,
                            revision: previous_revision + 1,
                            resume_command_id: AgentLoopResumeCommandId::from_static(
                                "resume-command",
                            ),
                            claim_id: AgentLoopClaimId::from_static("claim-reducer"),
                            worker_id: AgentLoopWorkerId::from_static("worker-reducer"),
                            claim_sha256: "c".repeat(LOWER_SHA256_BYTES),
                            claimed_at_ms: 900,
                        }),
                    },
                ),
            },
        )
    }

    fn close_wait(previous_revision: u64) -> StoredEvent {
        stored(
            "event-close",
            StateEvent::WaitClosed {
                turn_id: TurnId::from_static("turn-reducer"),
                stopped: placeholder_item("item-close-stop", 1_000),
                transition: item(
                    "item-close-transition",
                    1_000,
                    ItemKind::AgentLoopWaitClosed {
                        evidence: Box::new(WaitClosureEvidence {
                            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
                            previous_revision,
                            revision: previous_revision + 1,
                            command_id: AgentLoopCloseCommandId::from_static("close-command"),
                            status: TurnStatus::TimedOut,
                            reason: TurnStopReason::TimedOut,
                            command_sha256: "d".repeat(LOWER_SHA256_BYTES),
                            closed_at_ms: 1_000,
                        }),
                    },
                ),
                status: TurnStatus::TimedOut,
            },
        )
    }

    fn deny_wait(previous_revision: u64, decision: ApprovalDecision) -> StoredEvent {
        stored(
            "event-deny",
            StateEvent::DenyWait {
                turn_id: TurnId::from_static("turn-reducer"),
                approval_decision: placeholder_item("item-deny-decision", 800),
                transition: item(
                    "item-deny-transition",
                    800,
                    ItemKind::AgentLoopWaitDenied {
                        evidence: Box::new(WaitDenialEvidence {
                            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
                            previous_revision,
                            revision: previous_revision + 1,
                            command_id: AgentLoopDenyCommandId::from_static("deny-command"),
                            command_sha256: "e".repeat(LOWER_SHA256_BYTES),
                            settlement: settlement(decision),
                            denied_at_ms: 800,
                        }),
                    },
                ),
            },
        )
    }

    fn due(id: &str) -> AgentLoopDueWait {
        AgentLoopDueWait {
            phase: AgentLoopDuePhase::Waiting,
            tenant_id: Some("tenant-a".to_owned()),
            thread_id: ThreadId::from_string(format!("thread-{id}")),
            turn_id: TurnId::from_string(format!("turn-{id}")),
            wait_id: AgentLoopWaitId::from_string(format!("wait-{id}")),
            revision: 1,
            due_at_ms: 1_000,
            envelope_sha256: "a".repeat(LOWER_SHA256_BYTES),
            wait_started_event_id: EventId::from_string(format!("wait-start-{id}")),
            current_transition_event_id: EventId::from_string(format!("wait-start-{id}")),
            expected_stream_version: 4,
            expected_stream_recovery_bytes: 4_096,
        }
    }

    #[test]
    fn projection_reducer_accepts_only_the_adr_0159_legal_transition_matrix() {
        let waiting = projection(AgentLoopDuePhase::Waiting);
        let ready_allow = projection(AgentLoopDuePhase::ReadyAllow);
        let ready_deny = projection(AgentLoopDuePhase::ReadyDeny);

        let approve = accept_resume(
            "event-resume-approve",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let mut approved = waiting.clone();
        approved.phase = AgentLoopDuePhase::ReadyAllow;
        approved.revision = 2;
        approved.current_transition_event_id = approve.event_id.clone();
        approved.resume_command_id = Some(AgentLoopResumeCommandId::from_static("resume-command"));

        let deny = accept_resume(
            "event-resume-deny",
            ApprovalDecision::Deny {
                reason: "operator denied".to_owned(),
            },
            waiting.revision,
        );
        let mut denied = waiting.clone();
        denied.phase = AgentLoopDuePhase::ReadyDeny;
        denied.revision = 2;
        denied.due_at_ms = Some(800);
        denied.current_transition_event_id = deny.event_id.clone();
        denied.resume_command_id = Some(AgentLoopResumeCommandId::from_static("resume-command"));

        let ordinary_other_turn = stored(
            "event-other-turn-terminal",
            StateEvent::TurnFinished {
                turn_id: TurnId::from_static("turn-other"),
                status: TurnStatus::Failed,
            },
        );
        let cases = vec![
            (
                "Waiting -> ReadyAllow",
                waiting.clone(),
                approve,
                AgentLoopWaitProjectionChange::Upsert(approved),
            ),
            (
                "Waiting -> ReadyDeny",
                waiting.clone(),
                deny,
                AgentLoopWaitProjectionChange::Upsert(denied),
            ),
            (
                "Waiting -> WaitClosed",
                waiting.clone(),
                close_wait(waiting.revision),
                AgentLoopWaitProjectionChange::Delete(waiting.clone()),
            ),
            (
                "ReadyAllow -> WaitClosed",
                ready_allow.clone(),
                close_wait(ready_allow.revision),
                AgentLoopWaitProjectionChange::Delete(ready_allow.clone()),
            ),
            (
                "ReadyAllow -> ClaimReady",
                ready_allow.clone(),
                claim_ready(ready_allow.revision),
                AgentLoopWaitProjectionChange::Delete(ready_allow.clone()),
            ),
            (
                "Waiting -> DenyWait",
                waiting.clone(),
                deny_wait(
                    waiting.revision,
                    ApprovalDecision::Deny {
                        reason: "operator denied".to_owned(),
                    },
                ),
                AgentLoopWaitProjectionChange::Delete(waiting.clone()),
            ),
            (
                "ReadyDeny -> DenyWait",
                ready_deny.clone(),
                deny_wait(
                    ready_deny.revision,
                    ApprovalDecision::Deny {
                        reason: "operator denied".to_owned(),
                    },
                ),
                AgentLoopWaitProjectionChange::Delete(ready_deny.clone()),
            ),
            (
                "terminal event for another Turn is unrelated",
                waiting,
                ordinary_other_turn,
                AgentLoopWaitProjectionChange::Unchanged,
            ),
        ];

        for (name, current, event, expected) in cases {
            let actual = projection_change(&event, Some(&current))
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn projection_reducer_fails_closed_for_the_illegal_transition_matrix() {
        let waiting = projection(AgentLoopDuePhase::Waiting);
        let ready_allow = projection(AgentLoopDuePhase::ReadyAllow);
        let ready_deny = projection(AgentLoopDuePhase::ReadyDeny);

        let mut wrong_transition_kind = accept_resume(
            "event-wrong-transition-kind",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut wrong_transition_kind.event else {
            unreachable!("fixture is an AcceptResume event");
        };
        transition.kind = ItemKind::UserMessage {
            content: "not lifecycle evidence".to_owned(),
        };

        let mut wrong_turn = claim_ready(ready_allow.revision);
        let StateEvent::ClaimReady { turn_id, .. } = &mut wrong_turn.event else {
            unreachable!("fixture is a ClaimReady event");
        };
        *turn_id = TurnId::from_static("turn-wrong");

        let mut wrong_accept_approval = accept_resume(
            "event-resume-wrong-approval",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut wrong_accept_approval.event else {
            unreachable!("fixture is an AcceptResume event");
        };
        let ItemKind::AgentLoopResumeAccepted { evidence } = &mut transition.kind else {
            unreachable!("fixture has resume evidence");
        };
        evidence.settlement.request.id = ApprovalId::from_static("approval-wrong");

        let mut wrong_deny_approval = deny_wait(
            ready_deny.revision,
            ApprovalDecision::Deny {
                reason: "operator denied".to_owned(),
            },
        );
        let StateEvent::DenyWait { transition, .. } = &mut wrong_deny_approval.event else {
            unreachable!("fixture is a DenyWait event");
        };
        let ItemKind::AgentLoopWaitDenied { evidence } = &mut transition.kind else {
            unreachable!("fixture has denial evidence");
        };
        evidence.settlement.request.id = ApprovalId::from_static("approval-wrong");

        let mut wrong_settlement_tenant = accept_resume(
            "event-resume-wrong-tenant",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut wrong_settlement_tenant.event else {
            unreachable!("fixture is an AcceptResume event");
        };
        let ItemKind::AgentLoopResumeAccepted { evidence } = &mut transition.kind else {
            unreachable!("fixture has resume evidence");
        };
        evidence.settlement.tenant_id = Some("tenant-wrong".to_owned());

        let mut wrong_authorization_thread = accept_resume(
            "event-resume-wrong-authorization-thread",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut wrong_authorization_thread.event
        else {
            unreachable!("fixture is an AcceptResume event");
        };
        let ItemKind::AgentLoopResumeAccepted { evidence } = &mut transition.kind else {
            unreachable!("fixture has resume evidence");
        };
        evidence.settlement.request.authorization.thread_id = ThreadId::from_static("thread-wrong");

        let mut wrong_next_revision = accept_resume(
            "event-resume-wrong-next-revision",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut wrong_next_revision.event else {
            unreachable!("fixture is an AcceptResume event");
        };
        let ItemKind::AgentLoopResumeAccepted { evidence } = &mut transition.kind else {
            unreachable!("fixture has resume evidence");
        };
        evidence.revision += 1;

        let cases = vec![
            (
                "AcceptResume cannot consume ReadyAllow",
                Some(ready_allow.clone()),
                accept_resume(
                    "event-resume-after-ready",
                    ApprovalDecision::Approve,
                    ready_allow.revision,
                ),
            ),
            (
                "ClaimReady requires ReadyAllow",
                Some(waiting.clone()),
                claim_ready(waiting.revision),
            ),
            (
                "WaitClosed cannot consume ReadyDeny",
                Some(ready_deny.clone()),
                close_wait(ready_deny.revision),
            ),
            (
                "DenyWait cannot consume ReadyAllow",
                Some(ready_allow.clone()),
                deny_wait(
                    ready_allow.revision,
                    ApprovalDecision::Deny {
                        reason: "operator denied".to_owned(),
                    },
                ),
            ),
            (
                "AcceptResume must consume the exact revision",
                Some(waiting.clone()),
                accept_resume(
                    "event-resume-wrong-revision",
                    ApprovalDecision::Approve,
                    waiting.revision + 1,
                ),
            ),
            (
                "ClaimReady must consume the exact revision",
                Some(ready_allow.clone()),
                claim_ready(ready_allow.revision + 1),
            ),
            (
                "WaitClosed must consume the exact revision",
                Some(waiting.clone()),
                close_wait(waiting.revision + 1),
            ),
            (
                "DenyWait must consume the exact revision",
                Some(ready_deny.clone()),
                deny_wait(
                    ready_deny.revision + 1,
                    ApprovalDecision::Deny {
                        reason: "operator denied".to_owned(),
                    },
                ),
            ),
            (
                "AcceptResume event cannot reuse WaitStarted identity",
                Some(waiting.clone()),
                accept_resume(
                    "event-wait-start",
                    ApprovalDecision::Approve,
                    waiting.revision,
                ),
            ),
            (
                "lifecycle event requires its typed transition Item",
                Some(waiting.clone()),
                wrong_transition_kind,
            ),
            (
                "lifecycle event coordinates must match the live row",
                Some(ready_allow.clone()),
                wrong_turn,
            ),
            (
                "AcceptResume Approval identity must match the live row",
                Some(waiting.clone()),
                wrong_accept_approval,
            ),
            (
                "DenyWait Approval identity must match the live row",
                Some(ready_deny.clone()),
                wrong_deny_approval,
            ),
            (
                "settlement tenant must match the immutable wait tenant",
                Some(waiting.clone()),
                wrong_settlement_tenant,
            ),
            (
                "settlement authorization must match the ordinary stream Thread",
                Some(waiting.clone()),
                wrong_authorization_thread,
            ),
            (
                "next revision must advance by exactly one",
                Some(waiting.clone()),
                wrong_next_revision,
            ),
            (
                "DenyWait requires a Deny settlement",
                Some(waiting.clone()),
                deny_wait(waiting.revision, ApprovalDecision::Approve),
            ),
            (
                "ordinary terminal event cannot bypass a live wait",
                Some(waiting.clone()),
                stored(
                    "event-terminal-bypass",
                    StateEvent::TurnFinished {
                        turn_id: waiting.turn_id.clone(),
                        status: TurnStatus::Failed,
                    },
                ),
            ),
            (
                "AcceptResume requires a live row",
                None,
                accept_resume(
                    "event-resume-no-row",
                    ApprovalDecision::Approve,
                    waiting.revision,
                ),
            ),
            (
                "ClaimReady requires a live row",
                None,
                claim_ready(ready_allow.revision),
            ),
            (
                "WaitClosed requires a live row",
                None,
                close_wait(waiting.revision),
            ),
            (
                "DenyWait requires a live row",
                None,
                deny_wait(
                    waiting.revision,
                    ApprovalDecision::Deny {
                        reason: "operator denied".to_owned(),
                    },
                ),
            ),
        ];

        for (name, current, event) in cases {
            assert!(
                projection_change(&event, current.as_ref()).is_err(),
                "{name}"
            );
        }
    }

    #[test]
    fn materialized_replay_requires_one_exact_source_thread_for_the_wait_lifecycle() {
        let source_thread_id = ThreadId::from_static("thread-source");
        let wrong_source_thread_id = ThreadId::from_static("thread-wrong-source");
        let mut request = approval_request();
        request.authorization.thread_id = source_thread_id.clone();
        let generation = CompletionGeneration::new(
            "1".repeat(LOWER_SHA256_BYTES),
            "2".repeat(LOWER_SHA256_BYTES),
            "3".repeat(LOWER_SHA256_BYTES),
            "4".repeat(LOWER_SHA256_BYTES),
            "5".repeat(LOWER_SHA256_BYTES),
            None,
            CompletionAssurance::RuntimeMeasured,
        )
        .expect("valid generation fixture");
        let wait_started = stored(
            "event-materialized-wait-start",
            StateEvent::WaitStarted {
                turn_id: TurnId::from_static("turn-reducer"),
                approval_requested: placeholder_item("item-materialized-approval", 700),
                transition: item(
                    "item-materialized-wait",
                    700,
                    ItemKind::AgentLoopWaitStarted {
                        envelope: Box::new(TurnWaitEnvelope {
                            wait_id: AgentLoopWaitId::from_static("wait-reducer"),
                            revision: 1,
                            thread_id: source_thread_id.clone(),
                            turn_id: TurnId::from_static("turn-reducer"),
                            tenant_id: Some("tenant-a".to_owned()),
                            requested_by: ActorIdentity::LocalProcess,
                            server_started_at_ms: 700,
                            expires_at_ms: Some(1_000),
                            remaining_active_timeout_ms: Some(500),
                            completion_generation: generation,
                            wait_kind: WaitKind::Approval {
                                request,
                                model_request_sha256: "1".repeat(LOWER_SHA256_BYTES),
                            },
                            envelope_sha256: "a".repeat(LOWER_SHA256_BYTES),
                        }),
                    },
                ),
            },
        );

        assert!(projection_change(&wait_started, None).is_err());
        assert!(
            projection_change_for_materialized_replay(
                &wait_started,
                None,
                Some(&wrong_source_thread_id),
            )
            .is_err()
        );
        let AgentLoopWaitProjectionChange::Upsert(waiting) =
            projection_change_for_materialized_replay(&wait_started, None, Some(&source_thread_id))
                .expect("exact source Thread authorizes historical rebind")
        else {
            panic!("WaitStarted must create a materialized projection")
        };
        assert_eq!(waiting.thread_id, ThreadId::from_static("thread-reducer"));

        let mut accepted = accept_resume(
            "event-materialized-resume",
            ApprovalDecision::Approve,
            waiting.revision,
        );
        let StateEvent::AcceptResume { transition, .. } = &mut accepted.event else {
            unreachable!("fixture is an AcceptResume event");
        };
        let ItemKind::AgentLoopResumeAccepted { evidence } = &mut transition.kind else {
            unreachable!("fixture has resume evidence");
        };
        evidence.settlement.request.authorization.thread_id = source_thread_id.clone();

        assert!(projection_change(&accepted, Some(&waiting)).is_err());
        assert!(
            projection_change_for_materialized_replay(
                &accepted,
                Some(&waiting),
                Some(&wrong_source_thread_id),
            )
            .is_err()
        );
        assert!(matches!(
            projection_change_for_materialized_replay(
                &accepted,
                Some(&waiting),
                Some(&source_thread_id),
            )
            .expect("settlement must retain the exact copied source Thread"),
            AgentLoopWaitProjectionChange::Upsert(AgentLoopWaitProjection {
                phase: AgentLoopDuePhase::ReadyAllow,
                ..
            })
        ));
    }

    #[test]
    fn timeout_command_identity_is_stable_and_binds_every_fence_dimension() {
        let baseline = due("a");
        let stable = deterministic_timeout_command_id(&baseline).expect("baseline command");
        assert_eq!(
            stable,
            deterministic_timeout_command_id(&baseline).expect("stable command")
        );

        let mut variants = Vec::new();
        let mut value = baseline.clone();
        value.phase = AgentLoopDuePhase::ReadyAllow;
        value.revision = 2;
        value.current_transition_event_id = EventId::from_static("resume-a");
        variants.push(value);
        let mut value = baseline.clone();
        value.tenant_id = Some("tenant-b".to_owned());
        variants.push(value);
        let mut value = baseline.clone();
        value.thread_id = ThreadId::from_static("thread-b");
        variants.push(value);
        let mut value = baseline.clone();
        value.turn_id = TurnId::from_static("turn-b");
        variants.push(value);
        let mut value = baseline.clone();
        value.wait_id = AgentLoopWaitId::from_static("wait-b");
        variants.push(value);
        let mut value = baseline.clone();
        value.revision = 3;
        value.phase = AgentLoopDuePhase::ReadyAllow;
        value.current_transition_event_id = EventId::from_static("resume-b");
        variants.push(value);
        let mut value = baseline.clone();
        value.due_at_ms += 1;
        variants.push(value);
        let mut value = baseline.clone();
        value.envelope_sha256 = "b".repeat(LOWER_SHA256_BYTES);
        variants.push(value);

        let identities = variants
            .iter()
            .map(|variant| {
                deterministic_timeout_command_id(variant)
                    .expect("variant command")
                    .as_str()
                    .to_owned()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(identities.len(), variants.len());
        assert!(!identities.contains(stable.as_str()));

        let default_domain = maintenance_command_digest(TIMEOUT_COMMAND_DOMAIN, &baseline)
            .expect("default domain digest");
        let other_domain =
            maintenance_command_digest("y-harness.agent-loop.wait-timeout-command.v2", &baseline)
                .expect("other domain digest");
        assert_ne!(default_domain, other_domain);

        let mut first_ready = baseline.clone();
        first_ready.phase = AgentLoopDuePhase::ReadyAllow;
        first_ready.revision = 2;
        first_ready.current_transition_event_id = EventId::from_static("resume-one");
        let mut second_ready = first_ready.clone();
        second_ready.current_transition_event_id = EventId::from_static("resume-two");
        assert_ne!(
            deterministic_timeout_command_id(&first_ready).expect("first resume command"),
            deterministic_timeout_command_id(&second_ready).expect("second resume command")
        );
    }

    #[test]
    fn cursor_and_page_validation_enforce_bounds_order_and_tenant() {
        let first = due("a");
        let mut second = due("b");
        second.due_at_ms = 1_001;
        let valid = AgentLoopDueScanPage {
            due: vec![first.clone(), second.clone()],
            next_cursor: Some(second.cursor()),
            has_more: false,
            scanned: 2,
        };
        valid
            .validate(1_001, None, 2, Some("tenant-a"))
            .expect("valid page");

        let mut reversed = valid.clone();
        reversed.due.reverse();
        assert!(reversed.validate(1_001, None, 2, Some("tenant-a")).is_err());

        let invalid_cursor = AgentLoopDueCursor {
            due_at_ms: 1,
            thread_id: ThreadId::from_string(String::new()),
            turn_id: TurnId::from_static("turn"),
            wait_id: AgentLoopWaitId::from_static("wait"),
        };
        assert!(invalid_cursor.validate().is_err());
        assert!(
            valid
                .validate(
                    1_001,
                    None,
                    MAX_AGENT_LOOP_DUE_SCAN_LIMIT + 1,
                    Some("tenant-a")
                )
                .is_err()
        );
        assert!(valid.validate(1_001, None, 2, Some("tenant-b")).is_err());

        AgentLoopDueScanPage {
            due: Vec::new(),
            next_cursor: None,
            has_more: false,
            scanned: 0,
        }
        .validate(1_001, Some(&second.cursor()), 2, Some("tenant-a"))
        .expect("empty terminal page after a continuation");

        let partial_more = AgentLoopDueScanPage {
            due: vec![first.clone()],
            next_cursor: Some(first.cursor()),
            has_more: true,
            scanned: 1,
        };
        assert!(
            partial_more
                .validate(1_001, None, 2, Some("tenant-a"))
                .is_err()
        );
    }

    #[test]
    fn due_candidate_requires_elapsed_expiry_digest_and_phase_revision() {
        let waiting = due("a");
        assert!(waiting.validate(999, Some("tenant-a")).is_err());

        let mut malformed = waiting.clone();
        malformed.envelope_sha256 = "A".repeat(LOWER_SHA256_BYTES);
        assert!(malformed.validate(1_000, Some("tenant-a")).is_err());

        let mut ready = waiting;
        ready.phase = AgentLoopDuePhase::ReadyAllow;
        ready.current_transition_event_id = EventId::from_static("resume-ready");
        assert!(ready.validate(1_000, Some("tenant-a")).is_err());
        ready.revision = 2;
        ready
            .validate(1_000, Some("tenant-a"))
            .expect("approved ready wait is eligible");
    }

    #[test]
    fn ready_deny_uses_a_distinct_deterministic_terminal_command() {
        let mut denied = due("deny");
        denied.phase = AgentLoopDuePhase::ReadyDeny;
        denied.revision = 2;
        denied.current_transition_event_id = EventId::from_static("resume-deny");
        let denial = deterministic_denial_command_id(&denied).expect("denial command");
        assert!(deterministic_timeout_command_id(&denied).is_err());
        assert_eq!(
            denial,
            deterministic_denial_command_id(&denied).expect("stable denial command")
        );
        assert!(deterministic_denial_command_id(&due("waiting")).is_err());
    }

    #[test]
    fn projection_requires_immediate_ready_deny_and_exact_transition_shape() {
        let mut projection = AgentLoopWaitProjection {
            phase: AgentLoopDuePhase::ReadyDeny,
            tenant_id: Some("tenant-a".to_owned()),
            thread_id: ThreadId::from_static("thread-deny"),
            turn_id: TurnId::from_static("turn-deny"),
            wait_id: AgentLoopWaitId::from_static("wait-deny"),
            revision: 2,
            due_at_ms: None,
            approval_id: ApprovalId::from_static("approval-deny"),
            envelope_sha256: "a".repeat(LOWER_SHA256_BYTES),
            wait_started_event_id: EventId::from_static("wait-start-deny"),
            current_transition_event_id: EventId::from_static("resume-deny"),
            resume_command_id: Some(AgentLoopResumeCommandId::from_static("resume-command")),
        };
        assert!(projection.validate().is_err());

        projection.due_at_ms = Some(500);
        projection
            .validate()
            .expect("accepted denial has an immediate due coordinate");
        let key = projection.due_index_key().expect("denial due index key");
        assert_eq!(key.due_at_ms, 500);
        assert_eq!(key.thread_id, projection.thread_id);
        assert_eq!(key.turn_id, projection.turn_id);
        assert_eq!(key.wait_id, projection.wait_id);

        projection.current_transition_event_id = projection.wait_started_event_id.clone();
        assert!(projection.validate().is_err());
    }

    #[test]
    fn stream_fences_and_scan_cursor_are_strictly_bounded() {
        let mut candidate = due("fence");
        candidate.expected_stream_recovery_bytes = 0;
        assert!(candidate.validate(1_000, Some("tenant-a")).is_err());

        let future = AgentLoopDueCursor {
            due_at_ms: 1_001,
            thread_id: ThreadId::from_static("thread-future"),
            turn_id: TurnId::from_static("turn-future"),
            wait_id: AgentLoopWaitId::from_static("wait-future"),
        };
        assert!(validate_due_scan_request(1_000, Some(&future), 1, Some("tenant-a")).is_err());
        assert!(validate_due_scan_request(1_000, None, 1, Some("tenant:a")).is_ok());
        assert!(validate_due_scan_request(1_000, None, 1, Some("bad tenant")).is_err());
    }
}
