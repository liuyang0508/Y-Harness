//! Registered provider-specific token-counting capabilities.

use std::{
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    CapabilityOrigin, ExecutionPhase, HarnessError,
    kernel::{
        capture_capability_metadata, validate_capability_name, validate_capability_origin,
        validate_registry_growth,
    },
};

/// Current provider-specific Token Counter contract version.
pub const TOKEN_COUNTER_API_VERSION: u32 = 1;

const MAX_TOKEN_COUNTER_DESCRIPTION_BYTES: usize = 4_096;
const MAX_TOKEN_COUNT: usize = 16_777_216;

/// Stable metadata for one provider-specific token counter.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TokenCounterDescriptor {
    /// Stable registry identity.
    pub name: String,
    /// Human-readable tokenizer/model compatibility.
    pub description: String,
    /// Exact counter contract implemented by the extension.
    pub api_version: u32,
}

/// Synchronous provider-specific tokenizer boundary used during Context compile.
pub trait TokenCounter: Send + Sync {
    /// Returns stable metadata captured once at registration.
    fn descriptor(&self) -> TokenCounterDescriptor;

    /// Counts tokens in one already bounded UTF-8 model-visible segment.
    fn count_tokens(&self, text: &str) -> Result<usize, HarnessError>;
}

/// Validated counter paired with its operator-assigned trust origin.
pub struct RegisteredTokenCounter {
    /// Frozen descriptor captured during registration.
    pub descriptor: TokenCounterDescriptor,
    /// Trust-bearing registration origin.
    pub origin: CapabilityOrigin,
    /// Executable counter implementation.
    pub counter: Arc<dyn TokenCounter>,
}

/// Deterministic, collision-safe Token Counter registry.
#[derive(Default)]
pub struct TokenCounterRegistry {
    counters: BTreeMap<String, RegisteredTokenCounter>,
}

impl TokenCounterRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers one counter without identity replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        counter: Arc<dyn TokenCounter>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("token counter", self.counters.len(), 1)?;
        let descriptor =
            capture_capability_metadata("token counter descriptor", || counter.descriptor())?;
        validate_descriptor(&descriptor)?;
        if self.counters.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        self.counters.insert(
            descriptor.name.clone(),
            RegisteredTokenCounter {
                descriptor,
                origin,
                counter,
            },
        );
        Ok(())
    }

    /// Looks up one counter by exact stable identity.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredTokenCounter> {
        self.counters.get(name)
    }

    /// Returns registered counter identities in deterministic order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.counters.keys().cloned().collect()
    }

    pub(crate) fn count(&self, name: &str, text: &str) -> Result<usize, HarnessError> {
        let registered = self.counters.get(name).ok_or_else(|| {
            HarnessError::InvalidConfiguration(format!("token counter {name} is not registered"))
        })?;
        let count = catch_unwind(AssertUnwindSafe(|| registered.counter.count_tokens(text)))
            .map_err(|_| HarnessError::CapabilityPanicked {
                phase: ExecutionPhase::Context,
            })?
            .map_err(|_| {
                HarnessError::InvalidConfiguration(format!("token counter {name} failed"))
            })?;
        if !(1..=MAX_TOKEN_COUNT).contains(&count) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "token counter {name} returned a count outside 1-{MAX_TOKEN_COUNT}"
            )));
        }
        Ok(count)
    }
}

fn validate_descriptor(descriptor: &TokenCounterDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("token counter", &descriptor.name)?;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_TOKEN_COUNTER_DESCRIPTION_BYTES
        || descriptor.description.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "token counter {} description must be 1-{MAX_TOKEN_COUNTER_DESCRIPTION_BYTES} non-control bytes",
            descriptor.name
        )));
    }
    if descriptor.api_version != TOKEN_COUNTER_API_VERSION {
        return Err(HarnessError::InvalidCapability(format!(
            "token counter {} uses API version {}, expected {}",
            descriptor.name, descriptor.api_version, TOKEN_COUNTER_API_VERSION
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        TOKEN_COUNTER_API_VERSION, TokenCounter, TokenCounterDescriptor, TokenCounterRegistry,
    };
    use crate::{CapabilityOrigin, HarnessError};

    struct Counting;
    struct Panicking;

    impl TokenCounter for Counting {
        fn descriptor(&self) -> TokenCounterDescriptor {
            TokenCounterDescriptor {
                name: "test.counter".to_owned(),
                description: "Counts fixture segments".to_owned(),
                api_version: TOKEN_COUNTER_API_VERSION,
            }
        }

        fn count_tokens(&self, _text: &str) -> Result<usize, HarnessError> {
            Ok(1)
        }
    }

    impl TokenCounter for Panicking {
        fn descriptor(&self) -> TokenCounterDescriptor {
            TokenCounterDescriptor {
                name: "test.panicking-counter".to_owned(),
                description: "Panics only in a regression fixture".to_owned(),
                api_version: TOKEN_COUNTER_API_VERSION,
            }
        }

        fn count_tokens(&self, _text: &str) -> Result<usize, HarnessError> {
            panic!("sensitive tokenizer panic")
        }
    }

    #[test]
    fn registry_freezes_identity_and_rejects_replacement() {
        let mut registry = TokenCounterRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(Counting))
            .expect("first counter");
        assert_eq!(
            registry
                .get("test.counter")
                .expect("registered counter")
                .descriptor
                .api_version,
            TOKEN_COUNTER_API_VERSION
        );
        assert!(matches!(
            registry.register(CapabilityOrigin::BuiltIn, Arc::new(Counting)),
            Err(HarnessError::DuplicateCapability(_))
        ));
    }

    #[test]
    fn counter_panic_is_content_free_and_fails_closed() {
        let mut registry = TokenCounterRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(Panicking))
            .expect("counter");
        let error = registry
            .count("test.panicking-counter", "bounded input")
            .expect_err("panic must fail");
        assert!(matches!(
            error,
            HarnessError::CapabilityPanicked {
                phase: crate::ExecutionPhase::Context
            }
        ));
        assert!(!error.to_string().contains("sensitive"));
    }
}
