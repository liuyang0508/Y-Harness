//! Service-safe Human Handoff queue reads and ownership mutations.

use serde::{Deserialize, Serialize};

use crate::{
    AuthorityContext, HarnessError, HumanHandoffApplyOutcome, HumanHandoffCommand,
    HumanHandoffCreateRequest, HumanHandoffCursor, HumanHandoffEngine, HumanHandoffId,
    HumanHandoffSnapshot, HumanHandoffStatus, HumanHandoffSubject, HumanHandoffTransition,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::now_ms,
};

const MAX_HANDOFF_PAGE: usize = 64;
const MAX_HANDOFF_PAGE_BYTES: usize = 4_194_304;

/// Bounded current Human Handoff projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HumanHandoffSummary {
    /// Stable case identity.
    pub handoff_id: HumanHandoffId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Current optimistic-concurrency revision.
    pub revision: u64,
    /// Resource whose ownership is being transferred.
    pub subject: HumanHandoffSubject,
    /// Stable operator queue.
    pub queue: String,
    /// Content-free escalation classification.
    pub reason_code: String,
    /// Queue scheduling priority.
    pub priority: u8,
    /// Server-clock request time.
    pub requested_at_ms: u64,
    /// Current ownership lifecycle.
    pub status: HumanHandoffStatus,
    /// Number of retained immutable transitions.
    pub transition_count: u64,
    /// Conservative durable materialization charge.
    pub materialization_charge_bytes: u64,
}

/// Count- and byte-bounded queued Human Handoff page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HumanHandoffQueuePage {
    /// Queued cases in stable scheduling order.
    pub handoffs: Vec<HumanHandoffSummary>,
    /// Cursor for a later page.
    pub next_cursor: Option<HumanHandoffCursor>,
    /// Whether a later queued case exists.
    pub has_more: bool,
}

/// Count- and byte-bounded Human Handoff transition page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HumanHandoffTransitionPage {
    /// Owning case.
    pub handoff_id: HumanHandoffId,
    /// Revision from which the page was read.
    pub revision: u64,
    /// Transitions strictly after the requested sequence.
    pub transitions: Vec<HumanHandoffTransition>,
    /// Sequence cursor for a later page.
    pub next_after_sequence: Option<u64>,
    /// Whether a later transition exists.
    pub has_more: bool,
}

#[derive(Clone)]
pub(crate) struct HumanHandoffProtocolService {
    engine: HumanHandoffEngine,
}

impl HumanHandoffProtocolService {
    pub(crate) fn new(engine: HumanHandoffEngine) -> Self {
        Self { engine }
    }

    pub(crate) async fn create(
        &self,
        handoff_id: HumanHandoffId,
        request: HumanHandoffCreateRequest,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffSummary, HarnessError> {
        let snapshot = self
            .engine
            .create_as(handoff_id, request, now_ms(), authority)
            .await?;
        summary(&snapshot)
    }

    pub(crate) async fn summary(
        &self,
        handoff_id: &HumanHandoffId,
        authority: &AuthorityContext,
    ) -> Result<Option<HumanHandoffSummary>, HarnessError> {
        self.engine
            .load_as(handoff_id, authority)
            .await?
            .as_ref()
            .map(summary)
            .transpose()
    }

    pub(crate) async fn list_queued(
        &self,
        queue: &str,
        after: Option<&HumanHandoffCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffQueuePage, HarnessError> {
        validate_limit(limit)?;
        let page = self
            .engine
            .list_queued_as(queue, after, limit, authority)
            .await?;
        let available = page.handoffs.len();
        let mut handoffs = Vec::with_capacity(available);
        let mut encoded_bytes = 0_usize;
        for snapshot in page.handoffs {
            let next = summary(&snapshot)?;
            let remaining = MAX_HANDOFF_PAGE_BYTES.saturating_sub(encoded_bytes);
            let bytes = match bounded_serialized_size(&next, remaining) {
                Ok(bytes) => bytes,
                Err(BoundedJsonError::LimitExceeded) => break,
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Human Handoff queue page".to_owned(),
                    ));
                }
            };
            encoded_bytes = encoded_bytes.checked_add(bytes).ok_or_else(|| {
                HarnessError::Protocol("Human Handoff queue page byte count overflow".to_owned())
            })?;
            handoffs.push(next);
        }
        if handoffs.is_empty() && available != 0 {
            return Err(HarnessError::Protocol(
                "one Human Handoff summary exceeds the protocol response budget".to_owned(),
            ));
        }
        let next_cursor = handoffs.last().map(summary_cursor);
        let has_more = page.has_more || handoffs.len() < available;
        Ok(HumanHandoffQueuePage {
            handoffs,
            next_cursor,
            has_more,
        })
    }

    pub(crate) async fn transitions(
        &self,
        handoff_id: &HumanHandoffId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<HumanHandoffTransitionPage, HarnessError> {
        validate_limit(limit)?;
        let snapshot = self
            .engine
            .load_as(handoff_id, authority)
            .await?
            .ok_or_else(|| {
                HarnessError::HumanHandoff(format!("Human Handoff {handoff_id} does not exist"))
            })?;
        let mut transitions = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut has_more = false;
        for transition in snapshot
            .handoff()
            .transitions()
            .filter(|transition| transition.sequence > after_sequence)
        {
            if transitions.len() == limit {
                has_more = true;
                break;
            }
            let remaining = MAX_HANDOFF_PAGE_BYTES.saturating_sub(encoded_bytes);
            let bytes = match bounded_serialized_size(transition, remaining) {
                Ok(bytes) => bytes,
                Err(BoundedJsonError::LimitExceeded) => {
                    if transitions.is_empty() {
                        return Err(HarnessError::Protocol(
                            "one Human Handoff transition exceeds the protocol response budget"
                                .to_owned(),
                        ));
                    }
                    has_more = true;
                    break;
                }
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Human Handoff transition page".to_owned(),
                    ));
                }
            };
            encoded_bytes = encoded_bytes.checked_add(bytes).ok_or_else(|| {
                HarnessError::Protocol(
                    "Human Handoff transition page byte count overflow".to_owned(),
                )
            })?;
            transitions.push(transition.clone());
        }
        let next_after_sequence = transitions.last().map(|transition| transition.sequence);
        Ok(HumanHandoffTransitionPage {
            handoff_id: handoff_id.clone(),
            revision: snapshot.revision(),
            transitions,
            next_after_sequence,
            has_more,
        })
    }

    pub(crate) async fn apply(
        &self,
        handoff_id: &HumanHandoffId,
        expected_revision: u64,
        command: HumanHandoffCommand,
        authority: &AuthorityContext,
    ) -> Result<(HumanHandoffSummary, HumanHandoffApplyOutcome), HarnessError> {
        let result = self
            .engine
            .apply_as(handoff_id, expected_revision, command, now_ms(), authority)
            .await?;
        Ok((summary(&result.snapshot)?, result.outcome))
    }
}

fn validate_limit(limit: usize) -> Result<(), HarnessError> {
    if !(1..=MAX_HANDOFF_PAGE).contains(&limit) {
        return Err(HarnessError::Protocol(format!(
            "Human Handoff page limit must be 1-{MAX_HANDOFF_PAGE}"
        )));
    }
    Ok(())
}

fn summary(snapshot: &HumanHandoffSnapshot) -> Result<HumanHandoffSummary, HarnessError> {
    Ok(HumanHandoffSummary {
        handoff_id: snapshot.id().clone(),
        tenant_id: snapshot.tenant_id().map(str::to_owned),
        revision: snapshot.revision(),
        subject: snapshot.handoff().subject().clone(),
        queue: snapshot.handoff().queue().to_owned(),
        reason_code: snapshot.handoff().reason_code().to_owned(),
        priority: snapshot.handoff().priority(),
        requested_at_ms: snapshot.handoff().requested_at_ms(),
        status: snapshot.handoff().status().clone(),
        transition_count: u64::try_from(snapshot.handoff().transition_count()).map_err(|_| {
            HarnessError::Protocol("Human Handoff transition count overflow".to_owned())
        })?,
        materialization_charge_bytes: u64::try_from(
            snapshot.handoff().materialization_charge_bytes(),
        )
        .map_err(|_| HarnessError::Protocol("Human Handoff materialization overflow".to_owned()))?,
    })
}

fn summary_cursor(summary: &HumanHandoffSummary) -> HumanHandoffCursor {
    HumanHandoffCursor {
        priority: summary.priority,
        requested_at_ms: summary.requested_at_ms,
        handoff_id: summary.handoff_id.clone(),
    }
}
