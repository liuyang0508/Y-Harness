//! Domain answer contract and completion verifier.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use y_harness::{
    HarnessFuture, VerificationOutcome, VerificationRequest, Verifier, VerifierDescriptor,
};

use crate::{contracts::DataOrigin, journey::JourneyId};

/// Evidence reference included in a domain answer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerEvidence {
    /// Answer-local stable identity.
    pub id: String,
    /// Source system, document, expert record, or calculation.
    pub source: String,
    /// Real, synthetic, or inferred provenance.
    pub data_origin: DataOrigin,
    /// Optional exact source locator.
    pub locator: Option<String>,
}

/// One answer claim and its direct supporting evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AnswerClaim {
    /// Atomic conclusion.
    pub statement: String,
    /// Evidence identities supporting this conclusion.
    pub evidence_ids: Vec<String>,
    /// Calibrated confidence from zero to one.
    pub confidence: f32,
}

/// Structured output required for governed domain answers.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AquacultureAnswerEnvelope {
    /// Output contract coordinate.
    pub schema_version: String,
    /// Journey used to create the answer.
    pub journey_id: JourneyId,
    /// Exact pond scope; empty only when confirmation is required.
    pub pond_ids: Vec<String>,
    /// Human-readable conclusion and recommended next step.
    pub answer: String,
    /// Atomic conclusions.
    pub claims: Vec<AnswerClaim>,
    /// Source objects cited by claims.
    pub evidence: Vec<AnswerEvidence>,
    /// Known uncertainty, data gaps, and conflicts.
    pub uncertainty: Vec<String>,
    /// Whether the Agent must ask before diagnosing or acting.
    pub confirmation_required: bool,
    /// Visible warning required whenever synthetic evidence is used.
    pub synthetic_disclaimer: Option<String>,
}

impl AquacultureAnswerEnvelope {
    /// Returns every output-contract violation without stopping at the first.
    #[must_use]
    pub fn violations(&self) -> Vec<String> {
        let mut violations = Vec::new();
        if self.schema_version != "aquaculture.answer/v1" {
            violations.push("unsupported answer schema_version".to_owned());
        }
        if self.answer.trim().is_empty() {
            violations.push("answer must not be empty".to_owned());
        }
        if self.pond_ids.is_empty() && !self.confirmation_required {
            violations.push("missing pond scope must require confirmation".to_owned());
        }
        let evidence = self
            .evidence
            .iter()
            .map(|item| (item.id.as_str(), item))
            .collect::<BTreeMap<_, _>>();
        if evidence.len() != self.evidence.len() {
            violations.push("evidence ids must be unique".to_owned());
        }
        let mut cited = BTreeSet::new();
        for claim in &self.claims {
            if claim.statement.trim().is_empty() {
                violations.push("claim statement must not be empty".to_owned());
            }
            if !claim.confidence.is_finite() || !(0.0..=1.0).contains(&claim.confidence) {
                violations.push("claim confidence must be between zero and one".to_owned());
            }
            if claim.evidence_ids.is_empty() {
                violations.push(format!("claim '{}' has no evidence", claim.statement));
            }
            for evidence_id in &claim.evidence_ids {
                cited.insert(evidence_id.as_str());
                if !evidence.contains_key(evidence_id.as_str()) {
                    violations.push(format!("claim cites unknown evidence {evidence_id}"));
                }
            }
        }
        if evidence
            .values()
            .any(|item| item.data_origin == DataOrigin::Synthetic)
            && self
                .synthetic_disclaimer
                .as_deref()
                .is_none_or(str::is_empty)
        {
            violations.push("synthetic evidence requires a visible disclaimer".to_owned());
        }
        if evidence.keys().any(|id| !cited.contains(id)) {
            violations.push("answer contains evidence that is not linked to a claim".to_owned());
        }
        violations
    }
}

/// Y-Harness completion gate for structured aquaculture answers.
pub struct AquacultureOutputVerifier;

impl Verifier for AquacultureOutputVerifier {
    fn descriptor(&self) -> VerifierDescriptor {
        VerifierDescriptor {
            name: "aquaculture.output-contract".to_owned(),
            description: "Requires a scoped, evidence-linked answer with calibrated confidence and synthetic-data disclosure.".to_owned(),
        }
    }

    fn verify<'a>(
        &'a self,
        request: VerificationRequest,
    ) -> HarnessFuture<'a, VerificationOutcome> {
        Box::pin(async move {
            let envelope =
                match serde_json::from_str::<AquacultureAnswerEnvelope>(&request.candidate) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        return Ok(VerificationOutcome::Failed {
                            reason: format!("candidate is not aquaculture.answer/v1 JSON: {error}"),
                            retryable: true,
                        });
                    }
                };
            let violations = envelope.violations();
            if violations.is_empty() {
                Ok(VerificationOutcome::Passed {
                    summary: Some(
                        "pond scope, evidence lineage, confidence, and provenance are valid"
                            .to_owned(),
                    ),
                })
            } else {
                Ok(VerificationOutcome::Failed {
                    reason: violations.join("; "),
                    retryable: true,
                })
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_uncited_or_undisclosed_synthetic_answer() {
        let envelope = AquacultureAnswerEnvelope {
            schema_version: "aquaculture.answer/v1".to_owned(),
            journey_id: JourneyId::AqJr001,
            pond_ids: vec!["pond-3".to_owned()],
            answer: "溶氧存在下降趋势".to_owned(),
            claims: vec![AnswerClaim {
                statement: "溶氧下降".to_owned(),
                evidence_ids: Vec::new(),
                confidence: 0.8,
            }],
            evidence: vec![AnswerEvidence {
                id: "ev-1".to_owned(),
                source: "mock-iot".to_owned(),
                data_origin: DataOrigin::Synthetic,
                locator: None,
            }],
            uncertainty: Vec::new(),
            confirmation_required: false,
            synthetic_disclaimer: None,
        };
        let violations = envelope.violations();
        assert!(violations.iter().any(|item| item.contains("no evidence")));
        assert!(violations.iter().any(|item| item.contains("disclaimer")));
    }
}
