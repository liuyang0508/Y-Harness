//! Registered semantic conversation-compaction capabilities.

use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    CancellationToken, CapabilityOrigin, HarnessError, HarnessFuture, Item, ThreadId, TurnId,
    kernel::{
        capture_capability_metadata, validate_capability_name, validate_capability_origin,
        validate_registry_growth,
    },
};

/// Current semantic conversation-compactor contract version.
pub const CONVERSATION_COMPACTOR_API_VERSION: u32 = 1;

pub(crate) const MAX_COMPACTION_INPUT_TURNS: usize = 256;
pub(crate) const MAX_COMPACTION_INPUT_BYTES: usize = 8_388_608;
pub(crate) const MAX_COMPACTION_OUTPUT_TOKENS: usize = 1_048_576;
pub(crate) const MAX_COMPACTION_OUTPUT_BYTES: usize = 1_048_576;
const MAX_COMPACTOR_DESCRIPTION_BYTES: usize = 4_096;

/// Stable metadata for one semantic conversation compactor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationCompactorDescriptor {
    /// Stable registry identity.
    pub name: String,
    /// Human-readable compaction strategy and provider compatibility.
    pub description: String,
    /// Exact compactor contract implemented by the extension.
    pub api_version: u32,
}

/// Explicit input and output bounds for one selected conversation compactor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationCompactionConfig {
    /// Registered compactor name.
    pub compactor: String,
    /// Maximum newest omitted whole Turns supplied to the compactor.
    pub max_input_turns: usize,
    /// Maximum serialized bytes supplied from omitted Turns.
    pub input_budget_bytes: usize,
    /// Maximum provider-specific tokens in the final model-visible summary.
    pub output_budget_tokens: usize,
    /// Independent byte ceiling for the final model-visible summary.
    pub output_budget_bytes: usize,
}

/// One complete omitted Turn supplied to a semantic compactor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationCompactionTurn {
    /// Durable identity of the covered Turn.
    pub turn_id: TurnId,
    /// Chronological model-visible Items from that Turn.
    pub items: Vec<Item>,
}

/// Owned, bounded request passed to a semantic conversation compactor.
#[derive(Clone)]
pub struct ConversationCompactionRequest {
    /// Owning Thread.
    pub thread_id: ThreadId,
    /// Newest bounded slice of omitted whole Turns in chronological order.
    pub turns: Vec<ConversationCompactionTurn>,
    /// Number of still-older omitted Turns not present in `turns`.
    pub older_omitted_turns: usize,
    /// Identities of raw whole Turns retained after the summary.
    pub retained_turns: Vec<TurnId>,
    /// Current user prompt for relevance-aware compaction.
    pub current_prompt: String,
    /// Maximum provider-specific tokens allowed in the final summary block.
    pub output_budget_tokens: usize,
    /// Independent byte ceiling for the final summary block.
    pub output_budget_bytes: usize,
    /// Cooperative Turn cancellation signal.
    pub cancellation: CancellationToken,
}

/// Candidate semantic summary returned by a compactor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationCompactionResponse {
    /// Plain-text candidate. The engine adds a non-authoritative provenance header.
    pub summary: String,
}

/// Asynchronous semantic conversation-compaction boundary.
pub trait ConversationCompactor: Send + Sync {
    /// Returns stable metadata captured once at registration.
    fn descriptor(&self) -> ConversationCompactorDescriptor;

    /// Summarizes the exact bounded omitted-Turn slice in `request`.
    fn compact<'a>(
        &'a self,
        request: ConversationCompactionRequest,
    ) -> HarnessFuture<'a, ConversationCompactionResponse>;
}

/// Validated compactor paired with its operator-assigned trust origin.
pub struct RegisteredConversationCompactor {
    /// Frozen descriptor captured during registration.
    pub descriptor: ConversationCompactorDescriptor,
    /// Trust-bearing registration origin.
    pub origin: CapabilityOrigin,
    /// Executable compactor implementation.
    pub compactor: Arc<dyn ConversationCompactor>,
}

/// Deterministic, collision-safe conversation-compactor registry.
#[derive(Default)]
pub struct ConversationCompactorRegistry {
    compactors: BTreeMap<String, RegisteredConversationCompactor>,
}

impl ConversationCompactorRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers one compactor without identity replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        compactor: Arc<dyn ConversationCompactor>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("conversation compactor", self.compactors.len(), 1)?;
        let descriptor = capture_capability_metadata("conversation compactor descriptor", || {
            compactor.descriptor()
        })?;
        validate_descriptor(&descriptor)?;
        if self.compactors.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        self.compactors.insert(
            descriptor.name.clone(),
            RegisteredConversationCompactor {
                descriptor,
                origin,
                compactor,
            },
        );
        Ok(())
    }

    /// Looks up one compactor by exact stable identity.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredConversationCompactor> {
        self.compactors.get(name)
    }

    /// Returns registered compactor identities in deterministic order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.compactors.keys().cloned().collect()
    }
}

pub(crate) fn validate_config(config: &ConversationCompactionConfig) -> Result<(), HarnessError> {
    validate_capability_name("conversation compactor", &config.compactor)?;
    if !(1..=MAX_COMPACTION_INPUT_TURNS).contains(&config.max_input_turns)
        || !(1..=MAX_COMPACTION_INPUT_BYTES).contains(&config.input_budget_bytes)
        || !(1..=MAX_COMPACTION_OUTPUT_TOKENS).contains(&config.output_budget_tokens)
        || !(1..=MAX_COMPACTION_OUTPUT_BYTES).contains(&config.output_budget_bytes)
    {
        return Err(HarnessError::InvalidConfiguration(format!(
            "conversation compaction requires 1-{MAX_COMPACTION_INPUT_TURNS} input Turns, a 1-{MAX_COMPACTION_INPUT_BYTES} input-byte budget, a 1-{MAX_COMPACTION_OUTPUT_TOKENS} output-token budget, and a 1-{MAX_COMPACTION_OUTPUT_BYTES} output-byte budget"
        )));
    }
    Ok(())
}

fn validate_descriptor(descriptor: &ConversationCompactorDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("conversation compactor", &descriptor.name)?;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_COMPACTOR_DESCRIPTION_BYTES
        || descriptor.description.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "conversation compactor {} description must be 1-{MAX_COMPACTOR_DESCRIPTION_BYTES} non-control bytes",
            descriptor.name
        )));
    }
    if descriptor.api_version != CONVERSATION_COMPACTOR_API_VERSION {
        return Err(HarnessError::InvalidCapability(format!(
            "conversation compactor {} uses API version {}, expected {}",
            descriptor.name, descriptor.api_version, CONVERSATION_COMPACTOR_API_VERSION
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        CONVERSATION_COMPACTOR_API_VERSION, ConversationCompactionRequest,
        ConversationCompactionResponse, ConversationCompactor, ConversationCompactorDescriptor,
        ConversationCompactorRegistry,
    };
    use crate::{CapabilityOrigin, HarnessFuture};

    struct FixtureCompactor;

    impl ConversationCompactor for FixtureCompactor {
        fn descriptor(&self) -> ConversationCompactorDescriptor {
            ConversationCompactorDescriptor {
                name: "test.semantic-summary".to_owned(),
                description: "Summarizes bounded fixture history".to_owned(),
                api_version: CONVERSATION_COMPACTOR_API_VERSION,
            }
        }

        fn compact<'a>(
            &'a self,
            _request: ConversationCompactionRequest,
        ) -> HarnessFuture<'a, ConversationCompactionResponse> {
            Box::pin(async {
                Ok(ConversationCompactionResponse {
                    summary: "summary".to_owned(),
                })
            })
        }
    }

    #[test]
    fn registry_freezes_metadata_and_rejects_replacement() {
        let mut registry = ConversationCompactorRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(FixtureCompactor))
            .expect("first registration");

        let registered = registry
            .get("test.semantic-summary")
            .expect("registered compactor");
        assert_eq!(
            registered.descriptor.api_version,
            CONVERSATION_COMPACTOR_API_VERSION
        );
        assert_eq!(registry.names(), ["test.semantic-summary"]);
        assert!(
            registry
                .register(CapabilityOrigin::BuiltIn, Arc::new(FixtureCompactor))
                .is_err()
        );
    }
}
