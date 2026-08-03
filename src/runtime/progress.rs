//! Deterministic no-progress detection over durable Tool decisions and results.

use std::collections::VecDeque;

use serde::Serialize;
use serde_json::Value;

use crate::{HarnessError, Item, ItemKind, ToolCallBatchId};

const MAX_PROGRESS_VALUE_BYTES: usize = 1_052_672;
const MAX_PROGRESS_CYCLE_BYTES: usize = 16_384;
const MAX_FAILURE_CYCLE_PERIOD: usize = 4;

/// Default repetition ceiling for one exact failure-bearing Tool cycle.
pub const DEFAULT_MAX_FAILURE_CYCLE_REPETITIONS: u8 = 5;
/// Largest configurable failure-bearing cycle repetition ceiling.
pub const MAX_FAILURE_CYCLE_REPETITIONS: u8 = 16;

/// Bounded deterministic policy for stopping an Agent Loop that repeats an
/// exact cycle containing at least one failed Tool observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProgressPolicy {
    max_failure_cycle_repetitions: u8,
}

impl ProgressPolicy {
    /// Creates a policy that stops after the exact suffix of a failure-bearing
    /// Tool cycle repeats this many times without external input.
    pub fn new(max_failure_cycle_repetitions: u8) -> Result<Self, HarnessError> {
        if !(2..=MAX_FAILURE_CYCLE_REPETITIONS).contains(&max_failure_cycle_repetitions) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "failed Tool-cycle repetition limit must be 2-{MAX_FAILURE_CYCLE_REPETITIONS}"
            )));
        }
        Ok(Self {
            max_failure_cycle_repetitions,
        })
    }

    /// Returns the failure-bearing Tool-cycle repetition ceiling.
    #[must_use]
    pub const fn max_failure_cycle_repetitions(self) -> u8 {
        self.max_failure_cycle_repetitions
    }
}

impl Default for ProgressPolicy {
    fn default() -> Self {
        Self {
            max_failure_cycle_repetitions: DEFAULT_MAX_FAILURE_CYCLE_REPETITIONS,
        }
    }
}

/// Incremental reducer over the authoritative ordered Turn journal.
///
/// Only bounded correlation metadata and digests are retained. Raw Tool
/// inputs/results, Model text, and hidden reasoning never enter the governor
/// state.
pub(crate) struct ProgressGovernor {
    policy: ProgressPolicy,
    cursor: usize,
    pending: Option<PendingCycle>,
    failed_cycles: VecDeque<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProgressVerdict {
    Continue,
    Stop { cycle_period: u8, repetitions: u8 },
}

impl ProgressGovernor {
    pub(crate) fn from_items(policy: ProgressPolicy, items: &[Item]) -> Result<Self, HarnessError> {
        let mut governor = Self {
            policy,
            cursor: 0,
            pending: None,
            failed_cycles: VecDeque::new(),
        };
        let _ = governor.advance(items)?;
        Ok(governor)
    }

    pub(crate) fn advance(&mut self, items: &[Item]) -> Result<ProgressVerdict, HarnessError> {
        if self.cursor > items.len() {
            return Err(HarnessError::State(
                "Progress Governor item cursor moved beyond the Turn".to_owned(),
            ));
        }
        for item in &items[self.cursor..] {
            self.observe(item)?;
        }
        self.cursor = items.len();
        Ok(self.verdict())
    }

    fn observe(&mut self, item: &Item) -> Result<(), HarnessError> {
        match &item.kind {
            ItemKind::UserMessage { .. }
            | ItemKind::SteeringApplied { .. }
            | ItemKind::AssistantMessage { .. } => {
                self.require_no_pending_cycle()?;
                self.reset_boundary();
                Ok(())
            }
            ItemKind::VerificationResult { .. } => {
                self.require_no_pending_cycle()?;
                self.failed_cycles.clear();
                Ok(())
            }
            ItemKind::ToolCall {
                call_id,
                name,
                input,
                batch,
                ..
            } => self.observe_call(call_id, name, input, batch.as_ref()),
            ItemKind::ToolResult {
                call_id,
                output,
                is_error,
                ..
            } => self.observe_result(call_id, output, *is_error),
            _ => Ok(()),
        }
    }

    fn observe_call(
        &mut self,
        call_id: &str,
        name: &str,
        input: &Value,
        batch: Option<&crate::ToolCallBatch>,
    ) -> Result<(), HarnessError> {
        let proposal_sha256 =
            digest_progress_value(&ToolProposal { name, input }, MAX_PROGRESS_VALUE_BYTES)?;
        match batch {
            None => {
                self.require_no_pending_cycle()?;
                self.pending = Some(PendingCycle {
                    batch_id: None,
                    expected_calls: 1,
                    calls: vec![PendingCall::new(call_id, proposal_sha256)],
                });
            }
            Some(batch) if batch.index == 0 => {
                self.require_no_pending_cycle()?;
                self.pending = Some(PendingCycle {
                    batch_id: Some(batch.id.clone()),
                    expected_calls: batch.size,
                    calls: vec![PendingCall::new(call_id, proposal_sha256)],
                });
            }
            Some(batch) => {
                let pending = self.pending.as_mut().ok_or_else(|| {
                    HarnessError::State(
                        "Progress Governor observed a non-initial Tool batch member without its batch"
                            .to_owned(),
                    )
                })?;
                if pending.batch_id.as_ref() != Some(&batch.id)
                    || pending.expected_calls != batch.size
                    || pending.calls.len() != batch.index
                {
                    return Err(HarnessError::State(
                        "Progress Governor observed inconsistent Tool batch evidence".to_owned(),
                    ));
                }
                pending
                    .calls
                    .push(PendingCall::new(call_id, proposal_sha256));
            }
        }
        Ok(())
    }

    fn observe_result(
        &mut self,
        call_id: &str,
        output: &Value,
        is_error: bool,
    ) -> Result<(), HarnessError> {
        let result_sha256 = digest_progress_value(
            &ToolObservation { is_error, output },
            MAX_PROGRESS_VALUE_BYTES,
        )?;
        let pending = self.pending.as_mut().ok_or_else(|| {
            HarnessError::State(
                "Progress Governor observed a Tool result without a pending decision".to_owned(),
            )
        })?;
        let call = pending
            .calls
            .iter_mut()
            .find(|call| call.call_id == call_id)
            .ok_or_else(|| {
                HarnessError::State(
                    "Progress Governor observed a Tool result for a different decision".to_owned(),
                )
            })?;
        if call.result_sha256.replace(result_sha256).is_some() {
            return Err(HarnessError::State(
                "Progress Governor observed duplicate Tool-result evidence".to_owned(),
            ));
        }
        call.is_error = Some(is_error);
        if pending.calls.len() != pending.expected_calls
            || pending
                .calls
                .iter()
                .any(|call| call.result_sha256.is_none())
        {
            return Ok(());
        }

        let cycle = self.pending.take().ok_or_else(|| {
            HarnessError::State("Progress Governor lost its completed Tool cycle".to_owned())
        })?;
        if cycle.calls.iter().all(|call| call.is_error == Some(false)) {
            self.failed_cycles.clear();
            return Ok(());
        }
        let cycle_sha256 = digest_progress_value(
            &CompletedCycle {
                calls: &cycle.calls,
            },
            MAX_PROGRESS_CYCLE_BYTES,
        )?;
        self.failed_cycles.push_back(cycle_sha256);
        let retained = MAX_FAILURE_CYCLE_PERIOD
            .saturating_mul(usize::from(self.policy.max_failure_cycle_repetitions));
        while self.failed_cycles.len() > retained {
            self.failed_cycles.pop_front();
        }
        Ok(())
    }

    fn require_no_pending_cycle(&self) -> Result<(), HarnessError> {
        if self.pending.is_some() {
            Err(HarnessError::State(
                "Progress Governor observed a new Tool decision before the previous decision settled"
                    .to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    fn reset_boundary(&mut self) {
        self.pending = None;
        self.failed_cycles.clear();
    }

    fn verdict(&self) -> ProgressVerdict {
        repeated_suffix_period(
            &self.failed_cycles,
            usize::from(self.policy.max_failure_cycle_repetitions),
        )
        .map_or(ProgressVerdict::Continue, |period| ProgressVerdict::Stop {
            cycle_period: u8::try_from(period).unwrap_or(u8::MAX),
            repetitions: self.policy.max_failure_cycle_repetitions,
        })
    }
}

struct PendingCycle {
    batch_id: Option<ToolCallBatchId>,
    expected_calls: usize,
    calls: Vec<PendingCall>,
}

#[derive(Serialize)]
struct PendingCall {
    #[serde(skip)]
    call_id: String,
    proposal_sha256: String,
    result_sha256: Option<String>,
    is_error: Option<bool>,
}

impl PendingCall {
    fn new(call_id: &str, proposal_sha256: String) -> Self {
        Self {
            call_id: call_id.to_owned(),
            proposal_sha256,
            result_sha256: None,
            is_error: None,
        }
    }
}

#[derive(Serialize)]
struct ToolProposal<'a> {
    name: &'a str,
    input: &'a Value,
}

#[derive(Serialize)]
struct ToolObservation<'a> {
    is_error: bool,
    output: &'a Value,
}

#[derive(Serialize)]
struct CompletedCycle<'a> {
    calls: &'a [PendingCall],
}

fn digest_progress_value(value: &impl Serialize, maximum: usize) -> Result<String, HarnessError> {
    crate::json::bounded_serialized_sha256(value, maximum).map_err(|_| {
        HarnessError::State("Progress Governor could not digest bounded Tool evidence".to_owned())
    })
}

fn repeated_suffix_period(history: &VecDeque<String>, repetitions: usize) -> Option<usize> {
    (1..=MAX_FAILURE_CYCLE_PERIOD).find(|period| {
        let required = period.saturating_mul(repetitions);
        if history.len() < required {
            return false;
        }
        let start = history.len() - required;
        (start + period..history.len())
            .all(|index| history[index] == history[start + (index - start) % period])
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ProgressGovernor, ProgressPolicy, ProgressVerdict};
    use crate::{Item, ItemKind, SteeringId, ToolCallBatch, ToolCallBatchId, VerificationOutcome};

    fn append_cycle(
        items: &mut Vec<Item>,
        id: usize,
        input: &str,
        output: serde_json::Value,
        is_error: bool,
    ) {
        let call_id = format!("call-{id}");
        items.push(Item::new(ItemKind::ToolCall {
            model_id: Some("test/model".to_owned()),
            model_origin: Some(crate::CapabilityOrigin::BuiltIn),
            call_id: call_id.clone(),
            name: "probe".to_owned(),
            input: json!({"value": input}),
            batch: None,
        }));
        items.push(Item::new(ItemKind::ToolResult {
            call_id,
            output,
            is_error,
            connector_evidence: Vec::new(),
        }));
    }

    fn append_mixed_batch(items: &mut Vec<Item>, id: usize) {
        let batch_id = ToolCallBatchId::generate();
        let success_id = format!("success-{id}");
        let failure_id = format!("failure-{id}");
        for (index, call_id, input) in [
            (0, success_id.clone(), "success"),
            (1, failure_id.clone(), "failure"),
        ] {
            items.push(Item::new(ItemKind::ToolCall {
                model_id: Some("test/model".to_owned()),
                model_origin: Some(crate::CapabilityOrigin::BuiltIn),
                call_id,
                name: "probe".to_owned(),
                input: json!({"value": input}),
                batch: Some(ToolCallBatch {
                    id: batch_id.clone(),
                    index,
                    size: 2,
                }),
            }));
        }
        items.push(Item::new(ItemKind::ToolResult {
            call_id: success_id,
            output: json!({"value": "fixed"}),
            is_error: false,
            connector_evidence: Vec::new(),
        }));
        items.push(Item::new(ItemKind::ToolResult {
            call_id: failure_id,
            output: json!({"error": "fixed"}),
            is_error: true,
            connector_evidence: Vec::new(),
        }));
    }

    #[test]
    fn exact_repeated_failure_stops_at_the_configured_boundary() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let mut governor = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .expect("create governor");

        for id in 1..5 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
            assert_eq!(
                governor.advance(&items).expect("below stop boundary"),
                ProgressVerdict::Continue
            );
        }
        append_cycle(&mut items, 5, "same", json!({"error": "same"}), true);
        assert_eq!(
            governor.advance(&items).expect("governance verdict"),
            ProgressVerdict::Stop {
                cycle_period: 1,
                repetitions: 5
            }
        );
    }

    #[test]
    fn alternating_failure_cycle_is_detected() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let mut governor =
            ProgressGovernor::from_items(ProgressPolicy::new(3).expect("policy"), &items)
                .expect("create governor");
        for id in 0..5 {
            let input = if id % 2 == 0 { "a" } else { "b" };
            append_cycle(&mut items, id, input, json!({"error": input}), true);
            assert_eq!(
                governor.advance(&items).expect("cycle is incomplete"),
                ProgressVerdict::Continue
            );
        }
        append_cycle(&mut items, 5, "b", json!({"error": "b"}), true);
        assert_eq!(
            governor.advance(&items).expect("governance verdict"),
            ProgressVerdict::Stop {
                cycle_period: 2,
                repetitions: 3
            }
        );
    }

    #[test]
    fn successful_observation_resets_failure_history() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let mut governor = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .expect("create governor");
        for id in 1..5 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
            assert_eq!(
                governor.advance(&items).expect("failure below boundary"),
                ProgressVerdict::Continue
            );
        }
        append_cycle(&mut items, 5, "same", json!({"value": "ready"}), false);
        assert_eq!(
            governor.advance(&items).expect("successful reset"),
            ProgressVerdict::Continue
        );
        for id in 6..10 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
            assert_eq!(
                governor
                    .advance(&items)
                    .expect("new failure epoch below boundary"),
                ProgressVerdict::Continue
            );
        }
    }

    #[test]
    fn applied_user_steering_resets_failure_history() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let mut governor = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .expect("create governor");
        for id in 1..5 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
            assert_eq!(
                governor.advance(&items).expect("failure below boundary"),
                ProgressVerdict::Continue
            );
        }
        items.push(Item::new(ItemKind::SteeringApplied {
            steering_id: SteeringId::generate(),
            content: "new evidence".to_owned(),
        }));
        assert_eq!(
            governor.advance(&items).expect("steering reset"),
            ProgressVerdict::Continue
        );
        for id in 5..9 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
            assert_eq!(
                governor
                    .advance(&items)
                    .expect("new failure epoch below boundary"),
                ProgressVerdict::Continue
            );
        }
    }

    #[test]
    fn repeated_success_is_never_a_no_progress_failure() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let mut governor = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .expect("create governor");
        for id in 0..32 {
            append_cycle(&mut items, id, "same", json!({"value": "same"}), false);
            assert_eq!(
                governor.advance(&items).expect("successful polling"),
                ProgressVerdict::Continue
            );
        }
    }

    #[test]
    fn mixed_batch_failure_is_governed_and_fresh_ids_do_not_hide_the_cycle() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        let policy = ProgressPolicy::new(2).expect("policy");
        let mut governor = ProgressGovernor::from_items(policy, &items).expect("governor");
        append_mixed_batch(&mut items, 1);
        assert_eq!(
            governor.advance(&items).expect("first batch"),
            ProgressVerdict::Continue
        );
        append_mixed_batch(&mut items, 2);
        assert_eq!(
            governor.advance(&items).expect("second batch"),
            ProgressVerdict::Stop {
                cycle_period: 1,
                repetitions: 2
            }
        );
        assert_eq!(governor.cursor, items.len());

        let consumed_cursor = governor.cursor;
        assert_eq!(
            governor.advance(&items).expect("same consumed input"),
            ProgressVerdict::Stop {
                cycle_period: 1,
                repetitions: 2
            }
        );
        assert_eq!(governor.cursor, consumed_cursor);
        items.push(Item::new(ItemKind::SteeringApplied {
            steering_id: SteeringId::generate(),
            content: "override".to_owned(),
        }));
        assert_eq!(
            governor.advance(&items).expect("steering reset"),
            ProgressVerdict::Continue
        );
    }

    #[test]
    fn replay_preserves_a_reached_stop_verdict() {
        let mut items = vec![Item::new(ItemKind::UserMessage {
            content: "start".to_owned(),
        })];
        for id in 1..=5 {
            append_cycle(&mut items, id, "same", json!({"error": "same"}), true);
        }

        let mut governor = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .expect("replay durable items");
        assert_eq!(governor.cursor, items.len());
        assert_eq!(
            governor.advance(&items).expect("replayed verdict"),
            ProgressVerdict::Stop {
                cycle_period: 1,
                repetitions: 5
            }
        );
        assert_eq!(governor.cursor, items.len());
    }

    #[test]
    fn invalid_progress_policy_is_rejected() {
        for limit in [0, 1, 17, u8::MAX] {
            assert!(ProgressPolicy::new(limit).is_err());
        }
        assert_eq!(
            ProgressPolicy::new(2)
                .expect("minimum")
                .max_failure_cycle_repetitions(),
            2
        );
    }

    #[test]
    fn verification_cannot_interrupt_an_unsettled_tool_decision() {
        let items = vec![
            Item::new(ItemKind::UserMessage {
                content: "start".to_owned(),
            }),
            Item::new(ItemKind::ToolCall {
                model_id: Some("test/model".to_owned()),
                model_origin: Some(crate::CapabilityOrigin::BuiltIn),
                call_id: "pending".to_owned(),
                name: "probe".to_owned(),
                input: json!({"value": "same"}),
                batch: None,
            }),
            Item::new(ItemKind::VerificationResult {
                verifier: "test/verifier".to_owned(),
                candidate_item_id: None,
                verifier_origin: None,
                verifier_binding_sha256: None,
                outcome: VerificationOutcome::Passed { summary: None },
            }),
        ];

        let error = ProgressGovernor::from_items(ProgressPolicy::default(), &items)
            .err()
            .expect("verification cannot split a Tool decision");
        assert!(matches!(error, crate::HarnessError::State(_)));
    }
}
