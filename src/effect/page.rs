//! Shared validation for custom Coordinator lifecycle pages.

use super::{EffectPage, EffectPageCursor, EffectStatus};
use crate::{AuthorityContext, HarnessError};

#[derive(Clone, Copy)]
pub(super) enum EffectPageState {
    Pending,
    Unknown,
}

impl EffectPageState {
    fn name(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Unknown => "unknown",
        }
    }

    fn matches(self, status: &EffectStatus) -> bool {
        match self {
            Self::Pending => matches!(status, EffectStatus::Pending { .. }),
            Self::Unknown => matches!(status, EffectStatus::Unknown { .. }),
        }
    }
}

pub(super) fn validate_effect_page(
    consumer: &str,
    page: &EffectPage,
    after: Option<&EffectPageCursor>,
    limit: usize,
    authority: &AuthorityContext,
    expected_state: EffectPageState,
) -> Result<(), HarnessError> {
    if page.effects.len() > limit {
        return Err(HarnessError::Effect(format!(
            "{consumer} {} page exceeds scan limit",
            expected_state.name()
        )));
    }
    let expected_cursor = page.effects.last().map(|snapshot| EffectPageCursor {
        effect_id: snapshot.id().clone(),
    });
    if page.next_cursor != expected_cursor || (page.has_more && page.effects.len() != limit) {
        return Err(HarnessError::Effect(format!(
            "{consumer} {} page has inconsistent continuation",
            expected_state.name()
        )));
    }
    let mut previous = after.map(|cursor| cursor.effect_id.as_str());
    for snapshot in &page.effects {
        snapshot.effect().validate()?;
        let transition_count = u64::try_from(snapshot.effect().transition_count())
            .map_err(|_| HarnessError::Effect("Effect transition count overflow".to_owned()))?;
        if snapshot.tenant_id() != authority.tenant_id()
            || snapshot.effect().tenant_id() != authority.tenant_id()
            || snapshot.revision() != transition_count
            || !expected_state.matches(snapshot.effect().status())
        {
            return Err(HarnessError::Effect(format!(
                "{consumer} {} page projection is invalid",
                expected_state.name()
            )));
        }
        if previous.is_some_and(|prior| snapshot.id().as_str() <= prior) {
            return Err(HarnessError::Effect(format!(
                "{consumer} {} page identities are not ordered",
                expected_state.name()
            )));
        }
        previous = Some(snapshot.id().as_str());
    }
    Ok(())
}
