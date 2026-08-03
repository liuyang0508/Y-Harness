//! Multi-dimensional evidence scoring without a fixed source hierarchy.

use serde::{Deserialize, Serialize};

/// Evidence category retained for audit and calibration analysis.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// Peer-reviewed or otherwise published research.
    Paper,
    /// One or more named practitioners' experience.
    ExpertExperience,
    /// Approved enterprise procedure.
    Sop,
    /// Completed production-cycle observation.
    ProductionCase,
    /// Direct IoT measurement.
    SensorObservation,
    /// Direct ERP or production record.
    ErpRecord,
    /// Meeting statement not yet promoted to reviewed knowledge.
    MeetingStatement,
}

/// Normalized dimensions supplied by retrieval, rules, and reviewer feedback.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceAssessment {
    /// Category is descriptive; it does not impose a fixed rank.
    pub kind: EvidenceKind,
    /// Integrity and reputation of the exact source.
    pub source_quality: f32,
    /// Match to species, growth stage, region, season, equipment, and problem.
    pub applicability: f32,
    /// Temporal relevance to current production conditions.
    pub recency: f32,
    /// Independent support from other sources or cycles.
    pub corroboration: f32,
    /// Whether real outcomes validated the recommendation.
    pub outcome_validation: f32,
    /// Completeness of source data and experimental context.
    pub completeness: f32,
    /// Material contradiction not yet resolved, from zero to one.
    pub conflict: f32,
}

/// Computed score and the dimensions that most limit confidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceScore {
    /// Normalized confidence from zero to one.
    pub value: f32,
    /// Human-readable limiting factors for explanation and review.
    pub limiting_factors: Vec<String>,
}

impl EvidenceAssessment {
    /// Validates each normalized dimension and computes an explainable score.
    pub fn score(&self) -> Result<EvidenceScore, String> {
        let dimensions = [
            ("source_quality", self.source_quality),
            ("applicability", self.applicability),
            ("recency", self.recency),
            ("corroboration", self.corroboration),
            ("outcome_validation", self.outcome_validation),
            ("completeness", self.completeness),
            ("conflict", self.conflict),
        ];
        for (name, value) in dimensions {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(format!("{name} must be between zero and one"));
            }
        }
        let base = self.source_quality * 0.15
            + self.applicability * 0.25
            + self.recency * 0.10
            + self.corroboration * 0.15
            + self.outcome_validation * 0.25
            + self.completeness * 0.10;
        let value = (base * (1.0 - self.conflict * 0.6)).clamp(0.0, 1.0);
        let mut limiting_factors = dimensions
            .into_iter()
            .filter(|(name, value)| *name != "conflict" && *value < 0.5)
            .map(|(name, _)| name.to_owned())
            .collect::<Vec<_>>();
        if self.conflict > 0.3 {
            limiting_factors.push("unresolved_conflict".to_owned());
        }
        Ok(EvidenceScore {
            value,
            limiting_factors,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validated_experience_can_outscore_old_inapplicable_paper() {
        let old_paper = EvidenceAssessment {
            kind: EvidenceKind::Paper,
            source_quality: 0.9,
            applicability: 0.25,
            recency: 0.2,
            corroboration: 0.5,
            outcome_validation: 0.1,
            completeness: 0.8,
            conflict: 0.2,
        }
        .score()
        .expect("paper score");
        let validated_experience = EvidenceAssessment {
            kind: EvidenceKind::ExpertExperience,
            source_quality: 0.75,
            applicability: 0.95,
            recency: 0.9,
            corroboration: 0.8,
            outcome_validation: 0.9,
            completeness: 0.7,
            conflict: 0.1,
        }
        .score()
        .expect("experience score");
        assert!(validated_experience.value > old_paper.value);
    }
}
