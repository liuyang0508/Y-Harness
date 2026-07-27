//! Bounded, source-bound preparation for optional cross-Thread handoff summaries.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    HarnessError, Thread, ThreadId, TurnStatus,
    context::{
        MAX_TURN_CONTEXT_REFERENCE_BYTES, TurnContextInput, has_model_visible_items,
        model_visible_items, sha256_hex, validate_conversation_items_json,
        validate_turn_context_inputs,
    },
    json::{BoundedJsonError, bounded_serialized_size, to_bounded_json_vec},
};

use super::{
    ConversationCompactionTurn,
    compaction::{MAX_COMPACTION_INPUT_BYTES, MAX_COMPACTION_INPUT_TURNS},
};

/// Current canonical Thread-handoff request format.
pub const THREAD_HANDOFF_FORMAT_VERSION: u32 = 1;

/// Bounds applied while selecting source-Thread turns for a handoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadHandoffConfig {
    /// Maximum newest source-only model-visible Turns supplied to a summarizer.
    pub max_input_turns: usize,
    /// Maximum canonical JSON bytes supplied to a summarizer.
    pub input_budget_bytes: usize,
}

impl Default for ThreadHandoffConfig {
    fn default() -> Self {
        Self {
            max_input_turns: 64,
            input_budget_bytes: 1_048_576,
        }
    }
}

impl ThreadHandoffConfig {
    /// Validates configured input bounds before any Thread content is inspected.
    pub fn validate(&self) -> Result<(), HarnessError> {
        if !(1..=MAX_COMPACTION_INPUT_TURNS).contains(&self.max_input_turns) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff max_input_turns must be 1-{MAX_COMPACTION_INPUT_TURNS}"
            )));
        }
        if !(2..=MAX_COMPACTION_INPUT_BYTES).contains(&self.input_budget_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff input_budget_bytes must be 2-{MAX_COMPACTION_INPUT_BYTES}"
            )));
        }
        Ok(())
    }
}

/// Exact bounded source slice supplied to an external handoff summarizer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ThreadHandoffRequest {
    /// Canonical request format.
    pub format_version: u32,
    /// Thread whose divergent work is being handed off.
    pub source_thread_id: ThreadId,
    /// Thread that will receive the derived context.
    pub target_thread_id: ThreadId,
    /// Longest semantically identical Turn prefix shared by source and target.
    pub shared_prefix_turns: usize,
    /// Newest bounded source-only model-visible Turns, in chronological order.
    pub turns: Vec<ConversationCompactionTurn>,
    /// Still-older source-only model-visible Turns not present in `turns`.
    pub older_source_turns: usize,
    /// SHA-256 of the exact canonical source slice and boundary metadata.
    pub source_sha256: String,
}

impl ThreadHandoffRequest {
    /// Prepares a bounded source delta without synthesizing or persisting a summary.
    pub fn prepare(
        source: &Thread,
        target: &Thread,
        config: &ThreadHandoffConfig,
    ) -> Result<Option<Self>, HarnessError> {
        config.validate()?;
        validate_thread(source, "source")?;
        validate_thread(target, "target")?;
        if source.id == target.id {
            return Err(HarnessError::InvalidConfiguration(
                "Thread handoff source and target must differ".to_owned(),
            ));
        }

        let shared_prefix_turns = source
            .turns
            .iter()
            .zip(&target.turns)
            .take_while(|(source_turn, target_turn)| {
                source_turn.id == target_turn.id
                    && source_turn.status == target_turn.status
                    && source_turn.items == target_turn.items
            })
            .count();
        let candidate_count = source.turns[shared_prefix_turns..]
            .iter()
            .filter(|turn| has_model_visible_items(&turn.items))
            .count();
        if candidate_count == 0 {
            return Ok(None);
        }

        let mut encoded_bytes = 2_usize;
        let mut turns = Vec::new();
        for turn in source.turns[shared_prefix_turns..].iter().rev() {
            let items = model_visible_items(&turn.items);
            if items.is_empty() {
                continue;
            }
            if turns.len() >= config.max_input_turns {
                break;
            }
            let candidate = ConversationCompactionTurn {
                turn_id: turn.id.clone(),
                items,
            };
            validate_conversation_items_json(&candidate.items)?;
            let candidate_bytes = bounded_serialized_size(&candidate, MAX_COMPACTION_INPUT_BYTES)
                .map_err(|error| {
                handoff_json_error("Turn", MAX_COMPACTION_INPUT_BYTES, error)
            })?;
            let separator_bytes = usize::from(!turns.is_empty());
            let Some(next_bytes) = encoded_bytes
                .checked_add(separator_bytes)
                .and_then(|value| value.checked_add(candidate_bytes))
            else {
                return Err(HarnessError::InvalidConfiguration(
                    "Thread handoff input budget overflow".to_owned(),
                ));
            };
            if next_bytes > config.input_budget_bytes {
                if turns.is_empty() {
                    return Err(HarnessError::InvalidConfiguration(
                        "newest source-only Turn exceeds the Thread handoff input-byte budget"
                            .to_owned(),
                    ));
                }
                break;
            }
            encoded_bytes = next_bytes;
            turns.push(candidate);
        }
        turns.reverse();
        let older_source_turns = candidate_count.saturating_sub(turns.len());
        let source_sha256 = handoff_source_sha256(
            &source.id,
            &target.id,
            shared_prefix_turns,
            &turns,
            older_source_turns,
        )?;
        Ok(Some(Self {
            format_version: THREAD_HANDOFF_FORMAT_VERSION,
            source_thread_id: source.id.clone(),
            target_thread_id: target.id.clone(),
            shared_prefix_turns,
            turns,
            older_source_turns,
            source_sha256,
        }))
    }

    /// Wraps a candidate summary as non-authoritative per-Turn context.
    ///
    /// The summary provider remains outside the kernel. Runtime compilation
    /// will independently prefix, recount, hash, attribute, and journal the
    /// resulting invocation-context evidence.
    pub fn to_context(&self, summary: impl Into<String>) -> Result<TurnContextInput, HarnessError> {
        self.validate()?;
        let summary = summary.into();
        if summary.trim().is_empty() {
            return Err(HarnessError::InvalidConfiguration(
                "Thread handoff summary must not be empty".to_owned(),
            ));
        }
        let text = format!(
            "[Derived Thread handoff: non-authoritative summary of {} source Turns; {} still-older source Turns are not represented.]\n{summary}",
            self.turns.len(),
            self.older_source_turns
        );
        let input = TurnContextInput {
            source: "thread-handoff".to_owned(),
            reference: self.reference()?,
            text,
        };
        validate_turn_context_inputs(std::slice::from_ref(&input))?;
        Ok(input)
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if self.format_version != THREAD_HANDOFF_FORMAT_VERSION {
            return Err(HarnessError::InvalidConfiguration(format!(
                "unsupported Thread handoff format {}",
                self.format_version
            )));
        }
        validate_id("source Thread", self.source_thread_id.as_str())?;
        validate_id("target Thread", self.target_thread_id.as_str())?;
        if self.source_thread_id == self.target_thread_id {
            return Err(HarnessError::InvalidConfiguration(
                "Thread handoff source and target must differ".to_owned(),
            ));
        }
        if self.turns.is_empty() || self.turns.len() > MAX_COMPACTION_INPUT_TURNS {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff must contain 1-{MAX_COMPACTION_INPUT_TURNS} Turns"
            )));
        }
        let mut turn_ids = BTreeSet::new();
        for turn in &self.turns {
            validate_id("handoff Turn", turn.turn_id.as_str())?;
            if !turn_ids.insert(&turn.turn_id) {
                return Err(HarnessError::InvalidConfiguration(
                    "Thread handoff contains a duplicate Turn".to_owned(),
                ));
            }
            if turn.items.is_empty() {
                return Err(HarnessError::InvalidConfiguration(
                    "Thread handoff cannot contain an empty model-visible Turn".to_owned(),
                ));
            }
            validate_conversation_items_json(&turn.items)?;
        }
        let _ = self
            .turns
            .len()
            .checked_add(self.older_source_turns)
            .ok_or_else(|| {
                HarnessError::InvalidConfiguration(
                    "Thread handoff source Turn count overflows".to_owned(),
                )
            })?;
        let expected = handoff_source_sha256(
            &self.source_thread_id,
            &self.target_thread_id,
            self.shared_prefix_turns,
            &self.turns,
            self.older_source_turns,
        )?;
        if self.source_sha256 != expected {
            return Err(HarnessError::InvalidConfiguration(
                "Thread handoff source digest does not match its bounded input".to_owned(),
            ));
        }
        Ok(())
    }

    fn reference(&self) -> Result<String, HarnessError> {
        #[derive(Serialize)]
        struct Reference<'a> {
            format: &'static str,
            version: u32,
            source_thread_id: &'a ThreadId,
            target_thread_id: &'a ThreadId,
            shared_prefix_turns: usize,
            included_source_turns: usize,
            first_included_turn_id: &'a crate::TurnId,
            last_included_turn_id: &'a crate::TurnId,
            older_source_turns: usize,
            source_sha256: &'a str,
        }

        let first = &self
            .turns
            .first()
            .ok_or_else(|| {
                HarnessError::InvalidConfiguration(
                    "Thread handoff has no first included Turn".to_owned(),
                )
            })?
            .turn_id;
        let last = &self
            .turns
            .last()
            .ok_or_else(|| {
                HarnessError::InvalidConfiguration(
                    "Thread handoff has no last included Turn".to_owned(),
                )
            })?
            .turn_id;
        let encoded = to_bounded_json_vec(
            &Reference {
                format: "y-harness.thread-handoff",
                version: THREAD_HANDOFF_FORMAT_VERSION,
                source_thread_id: &self.source_thread_id,
                target_thread_id: &self.target_thread_id,
                shared_prefix_turns: self.shared_prefix_turns,
                included_source_turns: self.turns.len(),
                first_included_turn_id: first,
                last_included_turn_id: last,
                older_source_turns: self.older_source_turns,
                source_sha256: &self.source_sha256,
            },
            MAX_TURN_CONTEXT_REFERENCE_BYTES,
        )
        .map_err(|error| {
            handoff_json_error("reference", MAX_TURN_CONTEXT_REFERENCE_BYTES, error)
        })?;
        String::from_utf8(encoded).map_err(|_| {
            HarnessError::InvalidConfiguration(
                "Thread handoff reference is not UTF-8 JSON".to_owned(),
            )
        })
    }
}

#[derive(Serialize)]
struct HandoffSourceBinding<'a> {
    format_version: u32,
    source_thread_id: &'a ThreadId,
    target_thread_id: &'a ThreadId,
    shared_prefix_turns: usize,
    included_source_turns: usize,
    turns_sha256: &'a str,
    older_source_turns: usize,
}

fn handoff_source_sha256(
    source_thread_id: &ThreadId,
    target_thread_id: &ThreadId,
    shared_prefix_turns: usize,
    turns: &[ConversationCompactionTurn],
    older_source_turns: usize,
) -> Result<String, HarnessError> {
    let encoded_turns = to_bounded_json_vec(&turns, MAX_COMPACTION_INPUT_BYTES)
        .map_err(|error| handoff_json_error("source Turns", MAX_COMPACTION_INPUT_BYTES, error))?;
    let turns_sha256 = sha256_hex(&encoded_turns);
    let encoded_binding = to_bounded_json_vec(
        &HandoffSourceBinding {
            format_version: THREAD_HANDOFF_FORMAT_VERSION,
            source_thread_id,
            target_thread_id,
            shared_prefix_turns,
            included_source_turns: turns.len(),
            turns_sha256: &turns_sha256,
            older_source_turns,
        },
        MAX_TURN_CONTEXT_REFERENCE_BYTES,
    )
    .map_err(|error| {
        handoff_json_error("source binding", MAX_TURN_CONTEXT_REFERENCE_BYTES, error)
    })?;
    Ok(sha256_hex(&encoded_binding))
}

fn validate_thread(thread: &Thread, role: &str) -> Result<(), HarnessError> {
    validate_id(&format!("{role} Thread"), thread.id.as_str())?;
    let mut turn_ids = BTreeSet::new();
    for turn in &thread.turns {
        validate_id("handoff Turn", turn.id.as_str())?;
        if turn.thread_id != thread.id {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff {role} contains a Turn owned by another Thread"
            )));
        }
        if !turn_ids.insert(&turn.id) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff {role} contains a duplicate Turn"
            )));
        }
        if turn.status == TurnStatus::Running {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Thread handoff {role} must have no running Turn"
            )));
        }
    }
    Ok(())
}

fn validate_id(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{kind} identity must be 1-256 non-control bytes"
        )));
    }
    Ok(())
}

fn handoff_json_error(kind: &str, max_bytes: usize, error: BoundedJsonError) -> HarnessError {
    let detail = match error {
        BoundedJsonError::LimitExceeded => "exceeds its JSON or byte bound",
        BoundedJsonError::CannotEncode => "could not be serialized",
    };
    HarnessError::InvalidConfiguration(format!(
        "Thread handoff {kind} {detail}; limit is {max_bytes} bytes"
    ))
}

#[cfg(test)]
mod tests {
    use super::{ThreadHandoffConfig, ThreadHandoffRequest};
    use crate::{HarnessError, Item, ItemKind, Thread, Turn, TurnStatus};

    fn completed_turn(thread: &Thread, content: &str) -> Turn {
        let mut turn = Turn::new(thread.id.clone());
        turn.status = TurnStatus::Completed;
        turn.items.push(Item::new(ItemKind::UserMessage {
            content: content.to_owned(),
        }));
        turn
    }

    #[test]
    fn selects_only_the_bounded_source_delta_after_the_shared_prefix() {
        let mut source = Thread::new();
        source.turns.push(completed_turn(&source, "shared"));
        let shared = source.turns[0].clone();
        source.turns.push(completed_turn(&source, "source one"));
        source.turns.push(completed_turn(&source, "source two"));
        let mut target = Thread::new();
        let mut inherited = shared;
        inherited.thread_id = target.id.clone();
        target.turns.push(inherited);
        target.turns.push(completed_turn(&target, "target"));

        let request = ThreadHandoffRequest::prepare(
            &source,
            &target,
            &ThreadHandoffConfig {
                max_input_turns: 1,
                input_budget_bytes: 65_536,
            },
        )
        .expect("prepare")
        .expect("source delta");

        assert_eq!(request.shared_prefix_turns, 1);
        assert_eq!(request.turns.len(), 1);
        assert_eq!(request.older_source_turns, 1);
        assert!(matches!(
            &request.turns[0].items[0].kind,
            ItemKind::UserMessage { content } if content == "source two"
        ));
        assert_eq!(request.source_sha256.len(), 64);

        let context = request
            .to_context("Source evaluated option B.")
            .expect("context");
        assert_eq!(context.source, "thread-handoff");
        assert!(context.reference.contains("\"shared_prefix_turns\":1"));
        assert!(context.reference.contains(&request.source_sha256));
        assert!(context.text.contains("1 still-older source Turns"));
        assert!(context.text.ends_with("Source evaluated option B."));
    }

    #[test]
    fn unrelated_threads_handoff_the_complete_visible_source_history() {
        let mut source = Thread::new();
        source.turns.push(completed_turn(&source, "one"));
        source.turns.push(completed_turn(&source, "two"));
        let target = Thread::new();

        let request =
            ThreadHandoffRequest::prepare(&source, &target, &ThreadHandoffConfig::default())
                .expect("prepare")
                .expect("source history");

        assert_eq!(request.shared_prefix_turns, 0);
        assert_eq!(request.turns.len(), 2);
        assert_eq!(request.older_source_turns, 0);
    }

    #[test]
    fn identical_or_source_prefix_history_needs_no_handoff() {
        let mut source = Thread::new();
        source.turns.push(completed_turn(&source, "shared"));
        let mut target = Thread::new();
        let mut inherited = source.turns[0].clone();
        inherited.thread_id = target.id.clone();
        target.turns.push(inherited);
        target.turns.push(completed_turn(&target, "target only"));

        assert!(
            ThreadHandoffRequest::prepare(&source, &target, &ThreadHandoffConfig::default())
                .expect("prepare")
                .is_none()
        );
    }

    #[test]
    fn rejects_unstable_oversized_and_tampered_inputs() {
        let mut source = Thread::new();
        let mut running = completed_turn(&source, "running");
        running.status = TurnStatus::Running;
        source.turns.push(running);
        let target = Thread::new();
        assert!(matches!(
            ThreadHandoffRequest::prepare(&source, &target, &ThreadHandoffConfig::default()),
            Err(HarnessError::InvalidConfiguration(_))
        ));

        source.turns[0].status = TurnStatus::Completed;
        let error = ThreadHandoffRequest::prepare(
            &source,
            &target,
            &ThreadHandoffConfig {
                max_input_turns: 1,
                input_budget_bytes: 2,
            },
        )
        .expect_err("oversized newest Turn");
        assert!(error.to_string().contains("newest source-only Turn"));

        let mut request =
            ThreadHandoffRequest::prepare(&source, &target, &ThreadHandoffConfig::default())
                .expect("prepare")
                .expect("source history");
        request.turns[0].items[0] = Item::new(ItemKind::UserMessage {
            content: "tampered".to_owned(),
        });
        assert!(
            request
                .to_context("summary")
                .expect_err("digest mismatch")
                .to_string()
                .contains("digest")
        );
    }
}
