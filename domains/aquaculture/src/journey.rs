//! Full Journey registry and deterministic baseline router.

use serde::{Deserialize, Serialize};

/// Stable business-journey identity.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JourneyId {
    /// Pond anomaly diagnosis and intervention advice.
    AqJr001,
    /// Production BI, trend, comparison, and warning analysis.
    AqJr002,
    /// Voice or manual production-record extraction.
    AqJr003,
    /// Enterprise knowledge Q&A and evidence-conflict handling.
    AqJr004,
    /// Meeting intelligence, decisions, and action extraction.
    AqJr005,
    /// Mind-map and document conversion into diagnostic knowledge graphs.
    AqJr006,
    /// Governed task execution and outcome comparison.
    AqJr007,
    /// Cross-meeting, cross-task, and cross-cycle case consolidation.
    AqJr008,
}

/// Delivery maturity for a Journey.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JourneyStatus {
    /// Contract and acceptance behavior are designed.
    Designed,
    /// Ready for the first executable POC.
    PocReady,
}

/// Declarative business Journey contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JourneySpec {
    /// Stable identity.
    pub id: JourneyId,
    /// Human-readable business name.
    pub name: String,
    /// Delivery status.
    pub status: JourneyStatus,
    /// Whether tools require a resolved pond.
    pub requires_pond: bool,
    /// Required domain skills.
    pub skills: Vec<String>,
    /// Required connector or action tools.
    pub tools: Vec<String>,
    /// Completion verifiers.
    pub verifiers: Vec<String>,
}

/// Router result retains alternatives instead of hiding ambiguity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JourneyResolution {
    /// Best deterministic match.
    pub selected: JourneyId,
    /// Rule-based confidence between zero and one.
    pub confidence: f32,
    /// Other plausible Journeys.
    pub alternatives: Vec<JourneyId>,
    /// Whether the business goal should be confirmed before side effects.
    pub requires_confirmation: bool,
}

impl JourneyResolution {
    /// Returns whether this Journey needs a pond scope.
    #[must_use]
    pub fn requires_pond_scope(&self) -> bool {
        journey_registry()
            .iter()
            .find(|spec| spec.id == self.selected)
            .is_some_and(|spec| spec.requires_pond)
    }
}

/// Complete Journey registry, including designed work beyond the first POC.
#[must_use]
pub fn journey_registry() -> Vec<JourneySpec> {
    vec![
        JourneySpec {
            id: JourneyId::AqJr001,
            name: "池塘异常诊断与处置建议".to_owned(),
            status: JourneyStatus::PocReady,
            requires_pond: true,
            skills: strings(&["aq-diagnostic-reasoning", "aq-evidence-synthesis"]),
            tools: strings(&["aquaculture.iot.query", "aquaculture.erp.query"]),
            verifiers: strings(&["aquaculture.output-contract"]),
        },
        JourneySpec {
            id: JourneyId::AqJr002,
            name: "生产数据分析、对比、预警与报告".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: true,
            skills: strings(&["aq-production-analytics"]),
            tools: strings(&["iot.query", "erp.query", "bi.render"]),
            verifiers: strings(&["numeric-consistency", "source-coverage"]),
        },
        JourneySpec {
            id: JourneyId::AqJr003,
            name: "语音与人工生产记录结构化录入".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: true,
            skills: strings(&["aq-record-extraction"]),
            tools: strings(&["speech.transcribe", "erp.record.write"]),
            verifiers: strings(&["record-schema", "write-confirmation"]),
        },
        JourneySpec {
            id: JourneyId::AqJr004,
            name: "企业知识问答、来源权重与冲突处理".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: false,
            skills: strings(&["aq-evidence-synthesis", "aq-conflict-analysis"]),
            tools: strings(&["knowledge.hybrid_search", "knowledge.graph_query"]),
            verifiers: strings(&["citation-coverage", "conflict-disclosure"]),
        },
        JourneySpec {
            id: JourneyId::AqJr005,
            name: "会议智能分析、结论与任务提取".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: false,
            skills: strings(&["aq-meeting-analysis"]),
            tools: strings(&["meeting.read", "task.create"]),
            verifiers: strings(&["decision-lineage", "task-owner"]),
        },
        JourneySpec {
            id: JourneyId::AqJr006,
            name: "思维导图与文档转诊断知识图谱".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: false,
            skills: strings(&["aq-knowledge-graph-ingestion"]),
            tools: strings(&["document.parse", "knowledge.graph_write"]),
            verifiers: strings(&["graph-schema", "source-lineage"]),
        },
        JourneySpec {
            id: JourneyId::AqJr007,
            name: "任务执行、过程留痕与效果对比".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: true,
            skills: strings(&["aq-intervention-planning"]),
            tools: strings(&["task.execute", "iot.query", "erp.query"]),
            verifiers: strings(&["approval-required", "before-after-comparison"]),
        },
        JourneySpec {
            id: JourneyId::AqJr008,
            name: "跨会议、跨任务与跨周期案例沉淀".to_owned(),
            status: JourneyStatus::Designed,
            requires_pond: false,
            skills: strings(&["aq-case-consolidation"]),
            tools: strings(&["case.search", "case.publish"]),
            verifiers: strings(&["case-completeness", "review-status"]),
        },
    ]
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// Rule-based router used before model-assisted disambiguation.
pub struct JourneyRouter;

impl JourneyRouter {
    /// Routes common business language while surfacing low-confidence requests.
    #[must_use]
    pub fn resolve(&self, query: &str) -> JourneyResolution {
        let rules: &[(JourneyId, &[&str])] = &[
            (JourneyId::AqJr006, &["思维导图", "知识图谱", "graph"]),
            (JourneyId::AqJr005, &["会议", "纪要", "逐字稿"]),
            (
                JourneyId::AqJr003,
                &["录入", "投喂了", "死亡了", "语音记录"],
            ),
            (JourneyId::AqJr007, &["执行任务", "处置任务", "效果对比"]),
            (JourneyId::AqJr008, &["案例沉淀", "生产周期", "跨会议"]),
            (
                JourneyId::AqJr002,
                &["报表", "趋势", "可视化", "同比", "预警"],
            ),
            (
                JourneyId::AqJr004,
                &["知识", "论文", "冲突", "依据", "专家经验"],
            ),
            (
                JourneyId::AqJr001,
                &["诊断", "异常", "溶氧", "死亡率", "pH", "病害"],
            ),
        ];
        let mut scored: Vec<(JourneyId, usize)> = rules
            .iter()
            .map(|(journey, terms)| {
                (
                    *journey,
                    terms.iter().filter(|term| query.contains(**term)).count(),
                )
            })
            .filter(|(_, score)| *score > 0)
            .collect();
        scored.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
        let Some((selected, best_score)) = scored.first().copied() else {
            return JourneyResolution {
                selected: JourneyId::AqJr004,
                confidence: 0.35,
                alternatives: vec![JourneyId::AqJr001],
                requires_confirmation: true,
            };
        };
        let alternatives = scored
            .iter()
            .skip(1)
            .take(2)
            .map(|(journey, _)| *journey)
            .collect::<Vec<_>>();
        let tied = scored.get(1).is_some_and(|(_, score)| *score == best_score);
        JourneyResolution {
            selected,
            confidence: if best_score >= 2 { 0.9 } else { 0.72 },
            alternatives,
            requires_confirmation: tied,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_contains_all_journeys_once() {
        let registry = journey_registry();
        assert_eq!(registry.len(), 8);
        let ids = registry
            .iter()
            .map(|spec| spec.id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), 8);
    }

    #[test]
    fn routes_pond_diagnosis() {
        let resolution = JourneyRouter.resolve("3号塘最近溶氧异常，请诊断原因");
        assert_eq!(resolution.selected, JourneyId::AqJr001);
        assert!(resolution.confidence >= 0.9);
    }
}
