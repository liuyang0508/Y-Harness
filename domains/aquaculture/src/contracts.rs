//! Trusted request and structured domain-context contracts.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::journey::JourneyResolution;

/// Provenance class carried by every data-bearing result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataOrigin {
    /// Observed in a customer-owned source system.
    Real,
    /// Generated for development or demonstration.
    Synthetic,
    /// Derived from other evidence rather than directly observed.
    Inferred,
}

/// Trusted identity, authorization, and defaults supplied by the embedding host.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionContext {
    /// Authenticated enterprise tenant.
    pub tenant_id: String,
    /// Authenticated user identity.
    pub user_id: String,
    /// Optional active site selected in the product UI.
    pub active_site_id: Option<String>,
    /// Optional active pond selected in the product UI or user profile.
    pub active_pond_id: Option<String>,
    /// Exact ponds this caller may read.
    pub authorized_pond_ids: BTreeSet<String>,
    /// IANA-compatible product timezone coordinate.
    pub timezone: String,
}

/// Optional request-supplied time range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TimeWindow {
    /// Inclusive beginning in Unix milliseconds.
    pub start_ms: u64,
    /// Exclusive ending in Unix milliseconds.
    pub end_ms: u64,
    /// Timezone used to interpret human time expressions.
    pub timezone: String,
}

impl TimeWindow {
    /// Returns an error when the range is empty or reversed.
    pub fn validate(&self) -> Result<(), String> {
        if self.start_ms >= self.end_ms {
            return Err("time window start_ms must be earlier than end_ms".to_owned());
        }
        if self.timezone.trim().is_empty() {
            return Err("time window timezone must not be empty".to_owned());
        }
        Ok(())
    }
}

/// Raw user request plus trusted host context and explicit UI selections.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRequest {
    /// Natural-language user request.
    pub query: String,
    /// Trusted context created after authentication.
    pub interaction: InteractionContext,
    /// Pond explicitly named by the user or selected for this request.
    pub explicit_pond_id: Option<String>,
    /// Optional resolved time range.
    pub time_window: Option<TimeWindow>,
}

/// How the effective pond was resolved.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PondResolution {
    /// User text or request UI selected the pond.
    Explicit,
    /// Product session had one active pond.
    ActiveContext,
    /// Caller had exactly one authorized pond.
    SoleAuthorized,
    /// More information is required; no pond was guessed.
    NeedsConfirmation,
}

/// Authorized pond scope and its deterministic resolution evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedPondScope {
    /// Effective ponds; empty when confirmation is required.
    pub pond_ids: Vec<String>,
    /// Resolution rule that produced the scope.
    pub resolution: PondResolution,
    /// Rule-based confidence between zero and one.
    pub confidence: f32,
}

/// Machine-checkable context passed to skills, tools, and verifiers.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextPackage {
    /// Schema coordinate used for compatibility checks.
    pub schema_version: String,
    /// Trusted tenant boundary.
    pub tenant_id: String,
    /// Authenticated user.
    pub user_id: String,
    /// Routed Journey and its ambiguity evidence.
    pub journey: JourneyResolution,
    /// Authorized effective pond scope.
    pub pond_scope: ResolvedPondScope,
    /// Optional time range for source queries.
    pub time_window: Option<TimeWindow>,
    /// Source class for the current POC data plane.
    pub data_origin: DataOrigin,
    /// Questions that must be settled before a safe answer or action.
    pub open_questions: Vec<String>,
    /// Required disclosure when synthetic data is present.
    pub synthetic_disclaimer: Option<String>,
}

impl ContextPackage {
    /// Validates authorization, provenance, time, and uncertainty invariants.
    pub fn validate_against(&self, interaction: &InteractionContext) -> Result<(), String> {
        if self.tenant_id != interaction.tenant_id || self.user_id != interaction.user_id {
            return Err("context identity does not match trusted interaction context".to_owned());
        }
        if !self.pond_scope.confidence.is_finite()
            || !(0.0..=1.0).contains(&self.pond_scope.confidence)
        {
            return Err("pond confidence must be between zero and one".to_owned());
        }
        for pond_id in &self.pond_scope.pond_ids {
            if !interaction.authorized_pond_ids.contains(pond_id) {
                return Err(format!(
                    "pond {pond_id} is outside the caller authorization scope"
                ));
            }
        }
        if self.pond_scope.resolution == PondResolution::NeedsConfirmation
            && !self.pond_scope.pond_ids.is_empty()
        {
            return Err("unresolved pond scope must not contain guessed ponds".to_owned());
        }
        if let Some(window) = &self.time_window {
            window.validate()?;
        }
        if self.data_origin == DataOrigin::Synthetic
            && self
                .synthetic_disclaimer
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err("synthetic context requires a visible disclaimer".to_owned());
        }
        Ok(())
    }
}

/// Deterministic builder that refuses to guess an ambiguous pond.
pub struct ContextPackageBuilder {
    router: crate::journey::JourneyRouter,
    data_origin: DataOrigin,
}

impl ContextPackageBuilder {
    /// Creates a builder for one deployment data plane.
    #[must_use]
    pub fn new(data_origin: DataOrigin) -> Self {
        Self {
            router: crate::journey::JourneyRouter,
            data_origin,
        }
    }

    /// Resolves Journey, pond, time, and required clarification into one package.
    pub fn build(&self, request: &AgentRequest) -> Result<ContextPackage, String> {
        if request.query.trim().is_empty() {
            return Err("query must not be empty".to_owned());
        }
        let journey = self.router.resolve(&request.query);
        let pond_scope = resolve_pond_scope(request)?;
        let mut open_questions = Vec::new();
        if pond_scope.resolution == PondResolution::NeedsConfirmation
            && journey.requires_pond_scope()
        {
            open_questions.push("请确认要分析的池塘。".to_owned());
        }
        if journey.requires_confirmation {
            open_questions.push("请确认本次请求的主要业务目标。".to_owned());
        }
        let package = ContextPackage {
            schema_version: "aquaculture.context-package/v1".to_owned(),
            tenant_id: request.interaction.tenant_id.clone(),
            user_id: request.interaction.user_id.clone(),
            journey,
            pond_scope,
            time_window: request.time_window.clone(),
            data_origin: self.data_origin,
            open_questions,
            synthetic_disclaimer: (self.data_origin == DataOrigin::Synthetic).then(|| {
                "当前 IoT/ERP 数据为模拟数据，仅用于 POC 验证，不代表真实生产事实。".to_owned()
            }),
        };
        package.validate_against(&request.interaction)?;
        Ok(package)
    }
}

fn resolve_pond_scope(request: &AgentRequest) -> Result<ResolvedPondScope, String> {
    let authorized = &request.interaction.authorized_pond_ids;
    if let Some(pond_id) = request.explicit_pond_id.as_ref() {
        if !authorized.contains(pond_id) {
            return Err(format!("explicit pond {pond_id} is not authorized"));
        }
        return Ok(ResolvedPondScope {
            pond_ids: vec![pond_id.clone()],
            resolution: PondResolution::Explicit,
            confidence: 1.0,
        });
    }
    if let Some(pond_id) = request.interaction.active_pond_id.as_ref() {
        if !authorized.contains(pond_id) {
            return Err(format!("active pond {pond_id} is not authorized"));
        }
        return Ok(ResolvedPondScope {
            pond_ids: vec![pond_id.clone()],
            resolution: PondResolution::ActiveContext,
            confidence: 0.95,
        });
    }
    if authorized.len() == 1 {
        return Ok(ResolvedPondScope {
            pond_ids: authorized.iter().cloned().collect(),
            resolution: PondResolution::SoleAuthorized,
            confidence: 0.9,
        });
    }
    Ok(ResolvedPondScope {
        pond_ids: Vec::new(),
        resolution: PondResolution::NeedsConfirmation,
        confidence: 0.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interaction(ponds: &[&str]) -> InteractionContext {
        InteractionContext {
            tenant_id: "tenant-a".to_owned(),
            user_id: "user-a".to_owned(),
            active_site_id: Some("site-a".to_owned()),
            active_pond_id: None,
            authorized_pond_ids: ponds.iter().map(|value| (*value).to_owned()).collect(),
            timezone: "Asia/Shanghai".to_owned(),
        }
    }

    #[test]
    fn refuses_to_guess_between_multiple_ponds() {
        let request = AgentRequest {
            query: "分析最近三天溶氧异常".to_owned(),
            interaction: interaction(&["pond-1", "pond-2"]),
            explicit_pond_id: None,
            time_window: None,
        };
        let package = ContextPackageBuilder::new(DataOrigin::Synthetic)
            .build(&request)
            .expect("valid package");
        assert_eq!(
            package.pond_scope.resolution,
            PondResolution::NeedsConfirmation
        );
        assert!(package.pond_scope.pond_ids.is_empty());
        assert!(!package.open_questions.is_empty());
    }

    #[test]
    fn rejects_explicit_unauthorized_pond() {
        let request = AgentRequest {
            query: "诊断 3 号塘".to_owned(),
            interaction: interaction(&["pond-1"]),
            explicit_pond_id: Some("pond-3".to_owned()),
            time_window: None,
        };
        assert!(
            ContextPackageBuilder::new(DataOrigin::Synthetic)
                .build(&request)
                .is_err()
        );
    }
}
