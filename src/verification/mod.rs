//! Completion-condition contracts and deterministic verifier registration.

use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    CancellationToken, CapabilityOrigin, CompletionVerifierBinding, HarnessError, HarnessFuture,
    Item, ThreadId, TurnId, VerificationOutcome,
    kernel::{
        capture_capability_metadata, validate_capability_name, validate_capability_origin,
        validate_registry_growth,
    },
};

const MAX_OUTCOME_MESSAGE_BYTES: usize = 4_096;
const MAX_VERIFIER_DESCRIPTION_BYTES: usize = 4_096;

/// Immutable candidate snapshot supplied to every verifier in one pass.
#[derive(Clone, Debug)]
pub struct VerificationRequest {
    /// Owning thread.
    pub thread_id: ThreadId,
    /// Active turn.
    pub turn_id: TurnId,
    /// Ordered runtime history including the assistant candidate.
    pub items: Vec<Item>,
    /// Candidate text being considered for Turn completion.
    pub candidate: String,
    /// Cooperative Turn cancellation signal.
    pub cancellation: CancellationToken,
}

/// Stable model-visible metadata for a completion verifier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VerifierDescriptor {
    /// Stable registry name.
    pub name: String,
    /// Human-readable completion condition.
    pub description: String,
}

impl VerifierDescriptor {
    /// Validates stable identity and human-readable metadata.
    pub fn validate(&self) -> Result<(), HarnessError> {
        validate_descriptor(self)
    }
}

/// Completion-condition capability invoked before a Turn may complete.
pub trait Verifier: Send + Sync {
    /// Returns stable registration metadata.
    fn descriptor(&self) -> VerifierDescriptor;
    /// Evaluates one immutable candidate snapshot.
    fn verify<'a>(&'a self, request: VerificationRequest)
    -> HarnessFuture<'a, VerificationOutcome>;
}

/// Verifier implementation paired with validated metadata and trust origin.
pub struct RegisteredVerifier {
    /// Validated descriptor.
    pub descriptor: VerifierDescriptor,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Frozen descriptor-and-origin coordinate used by completion receipts.
    pub completion_binding: CompletionVerifierBinding,
    /// Executable verifier.
    pub verifier: Arc<dyn Verifier>,
}

#[derive(Default)]
/// Deterministic registry for completion verifiers.
pub struct VerificationRegistry {
    verifiers: BTreeMap<String, RegisteredVerifier>,
}

impl VerificationRegistry {
    /// Creates an empty registry, which imposes no additional completion gate.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers a verifier without allowing name replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        verifier: Arc<dyn Verifier>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("verifier", self.verifiers.len(), 1)?;
        let descriptor =
            capture_capability_metadata("verifier descriptor", || verifier.descriptor())?;
        validate_descriptor(&descriptor)?;
        if self.verifiers.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        let completion_binding = CompletionVerifierBinding::new(&descriptor, origin.clone())?;
        self.verifiers.insert(
            descriptor.name.clone(),
            RegisteredVerifier {
                descriptor,
                origin,
                completion_binding,
                verifier,
            },
        );
        Ok(())
    }

    /// Looks up a verifier by stable name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredVerifier> {
        self.verifiers.get(name)
    }

    /// Returns descriptors in deterministic name order.
    #[must_use]
    pub fn descriptors(&self) -> Vec<VerifierDescriptor> {
        self.verifiers
            .values()
            .map(|registered| registered.descriptor.clone())
            .collect()
    }

    /// Returns the frozen completion manifest in strict verifier-name order.
    #[must_use]
    pub fn completion_bindings(&self) -> Vec<CompletionVerifierBinding> {
        self.verifiers
            .values()
            .map(|registered| registered.completion_binding.clone())
            .collect()
    }

    pub(crate) fn registered(&self) -> impl Iterator<Item = &RegisteredVerifier> {
        self.verifiers.values()
    }
}

fn validate_descriptor(descriptor: &VerifierDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("verifier", &descriptor.name)?;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_VERIFIER_DESCRIPTION_BYTES
        || descriptor.description.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "verifier {} description must be 1-{MAX_VERIFIER_DESCRIPTION_BYTES} non-control bytes",
            descriptor.name,
        )));
    }
    Ok(())
}

pub(crate) fn validate_outcome(
    verifier: &str,
    outcome: &VerificationOutcome,
) -> Result<(), HarnessError> {
    let message = match outcome {
        VerificationOutcome::Passed {
            summary: Some(summary),
        } => Some(("summary", summary.as_str())),
        VerificationOutcome::Passed { summary: None } => None,
        VerificationOutcome::Failed { reason, .. } => Some(("reason", reason.as_str())),
    };
    if let Some((field, value)) = message {
        if value.trim().is_empty() {
            return Err(HarnessError::Verification(format!(
                "{verifier} returned an empty {field}"
            )));
        }
        if value.len() > MAX_OUTCOME_MESSAGE_BYTES {
            return Err(HarnessError::Verification(format!(
                "{verifier} returned a {field} larger than {MAX_OUTCOME_MESSAGE_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        VerificationRegistry, VerificationRequest, Verifier, VerifierDescriptor, validate_outcome,
    };
    use crate::{CapabilityOrigin, HarnessFuture, VerificationOutcome};

    struct PassingVerifier;

    impl Verifier for PassingVerifier {
        fn descriptor(&self) -> VerifierDescriptor {
            VerifierDescriptor {
                name: "required-output".to_owned(),
                description: "Requires an output".to_owned(),
            }
        }

        fn verify<'a>(
            &'a self,
            _request: VerificationRequest,
        ) -> HarnessFuture<'a, VerificationOutcome> {
            Box::pin(async { Ok(VerificationOutcome::Passed { summary: None }) })
        }
    }

    #[test]
    fn rejects_duplicate_verifier_names() {
        let mut registry = VerificationRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(PassingVerifier))
            .expect("first verifier");
        let error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(PassingVerifier))
            .expect_err("duplicate must fail");
        assert!(error.to_string().contains("duplicate capability"));
    }

    #[test]
    fn registration_freezes_completion_binding_and_origin() {
        let origin = CapabilityOrigin::TrustedExtension {
            id: "verified-package".to_owned(),
        };
        let mut registry = VerificationRegistry::new();
        registry
            .register(origin.clone(), Arc::new(PassingVerifier))
            .expect("register verifier");

        let bindings = registry.completion_bindings();
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].name(), "required-output");
        assert_eq!(bindings[0].origin(), &origin);
        assert_eq!(bindings[0].binding_sha256().len(), 64);
        assert!(
            bindings[0]
                .binding_sha256()
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
    }

    #[test]
    fn rejects_empty_or_unbounded_outcome_messages() {
        assert!(
            validate_outcome(
                "test",
                &VerificationOutcome::Failed {
                    reason: " ".to_owned(),
                    retryable: true,
                }
            )
            .is_err()
        );
        assert!(
            validate_outcome(
                "test",
                &VerificationOutcome::Passed {
                    summary: Some("x".repeat(4_097)),
                }
            )
            .is_err()
        );
    }
}
