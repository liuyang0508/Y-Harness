//! Service-safe durable Effect reads and actor-bound lifecycle mutations.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AuthorityContext, EffectApplyOutcome, EffectCommand, EffectCreateRequest, EffectEngine,
    EffectId, EffectOperation, EffectPageCursor, EffectSnapshot, EffectStatus, EffectTransition,
    HarnessError,
    json::{BoundedJsonError, bounded_serialized_size},
    kernel::now_ms,
};

const MAX_EFFECT_TRANSITION_PAGE: usize = 64;
const MAX_EFFECT_TRANSITION_PAGE_BYTES: usize = 4_194_304;
const MAX_EFFECT_LIST_PAGE: usize = 64;

/// Bounded current Effect projection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectSummary {
    /// Stable Effect identity.
    pub effect_id: EffectId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Current optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Target-system idempotency identity.
    pub idempotency_key: String,
    /// Bounded structured request required by an authorized worker.
    pub input: Value,
    /// Lowercase SHA-256 of the request.
    pub input_sha256: String,
    /// Trusted creation time.
    pub created_at_ms: u64,
    /// Current lifecycle projection.
    pub status: EffectStatus,
    /// Number of retained immutable transitions.
    pub transition_count: u64,
    /// Conservative durable materialization charge.
    pub materialization_charge_bytes: u64,
}

/// Content-light Effect projection used by identity-ordered list pages.
///
/// The external request body is intentionally omitted. Callers with
/// `effect.get` permission must fetch one exact Effect before handing its input
/// to a worker. This keeps broad discovery responses bounded and avoids
/// duplicating potentially sensitive request material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectListEntry {
    /// Stable Effect identity.
    pub effect_id: EffectId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Current optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable external operation coordinate.
    pub operation: EffectOperation,
    /// Lowercase SHA-256 of the omitted request.
    pub input_sha256: String,
    /// Trusted creation time.
    pub created_at_ms: u64,
    /// Current lifecycle projection.
    pub status: EffectStatus,
    /// Number of retained immutable transitions.
    pub transition_count: u64,
}

/// Bounded identity-ordered, content-light Effect page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectListPage {
    /// Matching Effects in stable identity order.
    pub effects: Vec<EffectListEntry>,
    /// Cursor for a later page.
    pub next_cursor: Option<EffectPageCursor>,
    /// Whether another matching Effect exists.
    pub has_more: bool,
}

/// Count- and byte-bounded Effect transition page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectTransitionPage {
    /// Owning Effect.
    pub effect_id: EffectId,
    /// Revision from which the page was read.
    pub revision: u64,
    /// Transitions strictly after the requested sequence.
    pub transitions: Vec<EffectTransition>,
    /// Sequence cursor for a later page.
    pub next_after_sequence: Option<u64>,
    /// Whether a later transition exists.
    pub has_more: bool,
}

#[derive(Clone)]
pub(crate) struct EffectProtocolService {
    engine: EffectEngine,
}

impl EffectProtocolService {
    pub(crate) fn new(engine: EffectEngine) -> Self {
        Self { engine }
    }

    pub(crate) async fn create(
        &self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        authority: &AuthorityContext,
    ) -> Result<EffectSummary, HarnessError> {
        let snapshot = self
            .engine
            .create_as(effect_id, request, now_ms(), authority)
            .await?;
        summary(&snapshot)
    }

    pub(crate) async fn summary(
        &self,
        effect_id: &EffectId,
        authority: &AuthorityContext,
    ) -> Result<Option<EffectSummary>, HarnessError> {
        self.engine
            .load_as(effect_id, authority)
            .await?
            .as_ref()
            .map(summary)
            .transpose()
    }

    pub(crate) async fn list(
        &self,
        status: Option<&str>,
        after: Option<&EffectPageCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<EffectListPage, HarnessError> {
        if !(1..=MAX_EFFECT_LIST_PAGE).contains(&limit) {
            return Err(HarnessError::Protocol(format!(
                "Effect list limit must be 1-{MAX_EFFECT_LIST_PAGE}"
            )));
        }
        let page = self.engine.list_as(status, after, limit, authority).await?;
        let effects = page
            .effects
            .iter()
            .map(list_entry)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(EffectListPage {
            effects,
            next_cursor: page.next_cursor,
            has_more: page.has_more,
        })
    }

    pub(crate) async fn transitions(
        &self,
        effect_id: &EffectId,
        after_sequence: u64,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<EffectTransitionPage, HarnessError> {
        if !(1..=MAX_EFFECT_TRANSITION_PAGE).contains(&limit) {
            return Err(HarnessError::Protocol(format!(
                "Effect transition limit must be 1-{MAX_EFFECT_TRANSITION_PAGE}"
            )));
        }
        let snapshot = self
            .engine
            .load_as(effect_id, authority)
            .await?
            .ok_or_else(|| HarnessError::Effect(format!("Effect {effect_id} does not exist")))?;
        let mut transitions = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut has_more = false;
        for transition in snapshot
            .effect()
            .transitions()
            .filter(|transition| transition.sequence > after_sequence)
        {
            if transitions.len() == limit {
                has_more = true;
                break;
            }
            let remaining = MAX_EFFECT_TRANSITION_PAGE_BYTES.saturating_sub(encoded_bytes);
            let transition_bytes = match bounded_serialized_size(transition, remaining) {
                Ok(bytes) => bytes,
                Err(BoundedJsonError::LimitExceeded) => {
                    if transitions.is_empty() {
                        return Err(HarnessError::Protocol(
                            "one Effect transition exceeds the protocol response budget".to_owned(),
                        ));
                    }
                    has_more = true;
                    break;
                }
                Err(BoundedJsonError::CannotEncode) => {
                    return Err(HarnessError::Protocol(
                        "cannot encode Effect transition page".to_owned(),
                    ));
                }
            };
            encoded_bytes = encoded_bytes
                .checked_add(transition_bytes)
                .ok_or_else(|| HarnessError::Protocol("Effect page size overflow".to_owned()))?;
            transitions.push(transition.clone());
        }
        let next_after_sequence = transitions.last().map(|transition| transition.sequence);
        Ok(EffectTransitionPage {
            effect_id: snapshot.id().clone(),
            revision: snapshot.revision(),
            transitions,
            next_after_sequence,
            has_more,
        })
    }

    pub(crate) async fn apply(
        &self,
        effect_id: &EffectId,
        expected_revision: u64,
        command: EffectCommand,
        authority: &AuthorityContext,
    ) -> Result<(EffectSummary, EffectApplyOutcome), HarnessError> {
        if expected_revision == 0 {
            return Err(HarnessError::Protocol(
                "expected_revision must be greater than zero".to_owned(),
            ));
        }
        let result = self
            .engine
            .apply_as(effect_id, expected_revision, command, now_ms(), authority)
            .await?;
        Ok((summary(&result.snapshot)?, result.outcome))
    }
}

fn list_entry(snapshot: &EffectSnapshot) -> Result<EffectListEntry, HarnessError> {
    let transition_count = u64::try_from(snapshot.effect().transition_count())
        .map_err(|_| HarnessError::Protocol("Effect transition count overflow".to_owned()))?;
    Ok(EffectListEntry {
        effect_id: snapshot.id().clone(),
        tenant_id: snapshot.tenant_id().map(str::to_owned),
        revision: snapshot.revision(),
        operation: snapshot.effect().operation().clone(),
        input_sha256: snapshot.effect().input_sha256().to_owned(),
        created_at_ms: snapshot.effect().created_at_ms(),
        status: snapshot.effect().status().clone(),
        transition_count,
    })
}

fn summary(snapshot: &EffectSnapshot) -> Result<EffectSummary, HarnessError> {
    let transition_count = u64::try_from(snapshot.effect().transition_count())
        .map_err(|_| HarnessError::Protocol("Effect transition count overflow".to_owned()))?;
    let materialization_charge_bytes =
        u64::try_from(snapshot.effect().materialization_charge_bytes()).map_err(|_| {
            HarnessError::Protocol("Effect materialization size overflow".to_owned())
        })?;
    Ok(EffectSummary {
        effect_id: snapshot.id().clone(),
        tenant_id: snapshot.tenant_id().map(str::to_owned),
        revision: snapshot.revision(),
        operation: snapshot.effect().operation().clone(),
        idempotency_key: snapshot.effect().idempotency_key().to_owned(),
        input: snapshot.effect().input().clone(),
        input_sha256: snapshot.effect().input_sha256().to_owned(),
        created_at_ms: snapshot.effect().created_at_ms(),
        status: snapshot.effect().status().clone(),
        transition_count,
        materialization_charge_bytes,
    })
}
