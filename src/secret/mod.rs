//! Opaque secret references, zeroizing values, and resolver registration.

use std::{collections::BTreeMap, env, fmt, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use zeroize::Zeroizing;

use crate::{
    AuthorityContext, CapabilityOrigin, EffectId, EffectLeaseId, EffectOperation, HarnessError,
    HarnessFuture, ThreadId, TurnId,
    kernel::{
        capture_capability_metadata, validate_capability_name, validate_capability_origin,
        validate_registry_growth,
    },
};

/// Current Y-Harness Secret Provider contract version.
pub const SECRET_API_VERSION: u32 = 3;

const MAX_SECRET_REFERENCE_BYTES: usize = 256;
const MAX_SECRET_VALUE_BYTES: usize = 65_536;
const MAX_SECRET_DESCRIPTION_BYTES: usize = 4_096;
const MAX_SECRET_MAPPINGS: usize = 1_024;

/// Serializable locator that never contains secret material.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SecretReference(String);

impl SecretReference {
    /// Validates and constructs an opaque provider-owned reference.
    pub fn new(value: impl Into<String>) -> Result<Self, HarnessError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_SECRET_REFERENCE_BYTES
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':')
            });
        if !valid {
            return Err(HarnessError::Secret(format!(
                "secret reference must be 1-{MAX_SECRET_REFERENCE_BYTES} portable ASCII identity bytes"
            )));
        }
        Ok(Self(value))
    }

    /// Returns the opaque non-secret locator.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for SecretReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

/// Non-serializable credential bytes erased when their owner is dropped.
///
/// `Debug` is deliberately content-free. Providers should keep this value
/// short-lived and expose its bytes only while constructing an authenticated
/// request.
pub struct SecretValue {
    bytes: Zeroizing<Vec<u8>>,
}

impl SecretValue {
    /// Wraps a non-empty, bounded credential buffer.
    pub fn new(bytes: Vec<u8>) -> Result<Self, HarnessError> {
        if bytes.is_empty() || bytes.len() > MAX_SECRET_VALUE_BYTES {
            return Err(HarnessError::Secret(format!(
                "secret value must be 1-{MAX_SECRET_VALUE_BYTES} bytes"
            )));
        }
        Ok(Self {
            bytes: Zeroizing::new(bytes),
        })
    }

    /// Exposes credential bytes to the immediate authenticated operation.
    #[must_use]
    pub fn expose_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Exposes a UTF-8 credential to an immediate text-only boundary.
    ///
    /// The failure is deliberately content-free. Callers must not include
    /// provider diagnostics or credential bytes in a wider error.
    pub fn expose_str(&self) -> Result<&str, HarnessError> {
        std::str::from_utf8(self.expose_bytes())
            .map_err(|_| HarnessError::Secret("secret value is not valid UTF-8".to_owned()))
    }

    /// Returns the credential byte length without exposing its contents.
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([REDACTED])")
    }
}

/// Governed Effect phase that is consuming one credential.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretEffectPhase {
    /// A freshly claimed Effect is entering its mutating Connector.
    Execution,
    /// An uncertain Effect is entering its authoritative read-only lookup.
    Reconciliation,
}

/// Reference-service operation that consumes a credential without a Turn.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretServiceUse {
    /// Startup or Doctor availability validation under deployment authority.
    StartupProbe,
    /// One request on an explicitly configured shared transport session.
    TransportRequest,
}

/// Typed, non-secret reason for resolving one credential.
///
/// A usage is evidence for Provider authorization and audit. It is not an
/// authority source: actor and tenant identity remain exclusively in the
/// separately supplied [`AuthorityContext`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretUseContext {
    /// A Model or another capability serving one exact Agent Turn.
    AgentTurn {
        /// Owning Thread.
        thread_id: ThreadId,
        /// Active Turn.
        turn_id: TurnId,
    },
    /// A durable Governed Effect entering one exact Connector attempt.
    GovernedEffect {
        /// Stable durable Effect identity.
        effect_id: EffectId,
        /// Immutable external operation coordinate.
        operation: EffectOperation,
        /// Mutating execution or authoritative read-only reconciliation.
        phase: SecretEffectPhase,
        /// Positive durable attempt.
        attempt: u32,
        /// Exact execution fence associated with the attempt.
        lease_id: EffectLeaseId,
    },
    /// A deployment-owned operation with no legitimate Thread or Turn.
    Service {
        /// Exact bounded service purpose.
        use_case: SecretServiceUse,
    },
}

/// Context supplied to a secret resolver without any secret material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRequest {
    /// Opaque reference selected by host configuration.
    pub reference: SecretReference,
    /// Capability consuming the credential.
    pub consumer: String,
    /// Typed operation context; never a substitute for trusted authority.
    pub use_context: SecretUseContext,
}

/// Stable metadata for one resolver implementation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SecretProviderDescriptor {
    /// Stable registry name.
    pub name: String,
    /// Human-readable source and custody behavior.
    pub description: String,
    /// Exact provider contract implemented by the resolver.
    pub api_version: u32,
}

/// Host authority that resolves references into short-lived secret values.
pub trait SecretProvider: Send + Sync {
    /// Returns stable registration metadata.
    fn descriptor(&self) -> SecretProviderDescriptor;

    /// Resolves one explicitly scoped reference without persistence or caching.
    fn resolve<'a>(&'a self, request: SecretRequest) -> HarnessFuture<'a, SecretValue>;

    /// Resolves one reference under trusted actor and tenant authority.
    ///
    /// Legacy providers remain usable for unscoped embedded operations but
    /// fail closed for tenant-scoped access until they override this method.
    fn resolve_as<'a>(
        &'a self,
        request: SecretRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async move {
            authority.validate_current("Secret Provider authority")?;
            if authority.tenant_id().is_some() {
                return Err(HarnessError::Secret(
                    "secret provider does not support tenant-scoped resolution".to_owned(),
                ));
            }
            self.resolve(request).await
        })
    }
}

/// Registered resolver and its operator-assigned trust origin.
pub struct RegisteredSecretProvider {
    /// Validated descriptor.
    pub descriptor: SecretProviderDescriptor,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Resolver implementation.
    pub provider: Arc<dyn SecretProvider>,
}

/// Deterministic, collision-safe Secret Provider registry.
#[derive(Default)]
pub struct SecretRegistry {
    providers: BTreeMap<String, RegisteredSecretProvider>,
}

impl SecretRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers one resolver without identity replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        provider: Arc<dyn SecretProvider>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("secret provider", self.providers.len(), 1)?;
        let descriptor =
            capture_capability_metadata("secret provider descriptor", || provider.descriptor())?;
        validate_secret_descriptor(&descriptor)?;
        if self.providers.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        self.providers.insert(
            descriptor.name.clone(),
            RegisteredSecretProvider {
                descriptor,
                origin,
                provider,
            },
        );
        Ok(())
    }

    /// Looks up a resolver by exact stable identity.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&RegisteredSecretProvider> {
        self.providers.get(name)
    }

    /// Returns resolver identities in deterministic order.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.providers.keys().cloned().collect()
    }
}

/// Explicit allow-list adapter for credentials supplied by process environment.
///
/// Only mapped variable names are read. Values are loaded on demand, never
/// retained by this provider, and never included in errors or debug output.
pub struct EnvironmentSecretProvider {
    descriptor: SecretProviderDescriptor,
    mappings: BTreeMap<SecretReference, String>,
}

impl EnvironmentSecretProvider {
    /// Creates a resolver over an explicit reference-to-variable mapping.
    pub fn new(
        name: impl Into<String>,
        mappings: BTreeMap<SecretReference, String>,
    ) -> Result<Self, HarnessError> {
        let descriptor = SecretProviderDescriptor {
            name: name.into(),
            description: "Resolves explicitly mapped process environment variables on demand"
                .to_owned(),
            api_version: SECRET_API_VERSION,
        };
        validate_secret_descriptor(&descriptor)?;
        if mappings.is_empty() || mappings.len() > MAX_SECRET_MAPPINGS {
            return Err(HarnessError::InvalidConfiguration(format!(
                "environment secret mappings must contain 1-{MAX_SECRET_MAPPINGS} entries"
            )));
        }
        if mappings.values().any(|name| !valid_environment_name(name)) {
            return Err(HarnessError::InvalidConfiguration(
                "environment secret mappings contain an invalid variable name".to_owned(),
            ));
        }
        Ok(Self {
            descriptor,
            mappings,
        })
    }
}

impl SecretProvider for EnvironmentSecretProvider {
    fn descriptor(&self) -> SecretProviderDescriptor {
        self.descriptor.clone()
    }

    fn resolve<'a>(&'a self, request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async move {
            let variable = self.mappings.get(&request.reference).ok_or_else(|| {
                HarnessError::Secret("secret reference is not mapped by this provider".to_owned())
            })?;
            resolve_environment_value(variable)
        })
    }
}

/// Explicit tenant-to-environment mapping for embedded enterprise hosts.
///
/// Each lookup requires an exact trusted tenant. Unscoped and cross-tenant
/// requests never fall back to another tenant or to ambient variable names.
pub struct TenantEnvironmentSecretProvider {
    descriptor: SecretProviderDescriptor,
    mappings: BTreeMap<String, BTreeMap<SecretReference, String>>,
}

impl TenantEnvironmentSecretProvider {
    /// Creates a resolver from exact tenant/reference maps.
    pub fn new(
        name: impl Into<String>,
        tenant_mappings: BTreeMap<String, BTreeMap<SecretReference, String>>,
    ) -> Result<Self, HarnessError> {
        let descriptor = SecretProviderDescriptor {
            name: name.into(),
            description:
                "Resolves explicitly tenant-mapped process environment variables on demand"
                    .to_owned(),
            api_version: SECRET_API_VERSION,
        };
        validate_secret_descriptor(&descriptor)?;
        let total = tenant_mappings
            .values()
            .try_fold(0_usize, |total, mappings| {
                total.checked_add(mappings.len()).ok_or_else(|| {
                    HarnessError::InvalidConfiguration(
                        "tenant environment secret mapping count overflow".to_owned(),
                    )
                })
            })?;
        if tenant_mappings.is_empty()
            || tenant_mappings.values().any(BTreeMap::is_empty)
            || total == 0
            || total > MAX_SECRET_MAPPINGS
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "tenant environment secret mappings must contain 1-{MAX_SECRET_MAPPINGS} total entries"
            )));
        }
        for (tenant_id, references) in &tenant_mappings {
            AuthorityContext::validate_tenant(tenant_id)?;
            for variable in references.values() {
                if !valid_environment_name(variable) {
                    return Err(HarnessError::InvalidConfiguration(
                        "tenant environment secret mappings contain an invalid variable name"
                            .to_owned(),
                    ));
                }
            }
        }
        Ok(Self {
            descriptor,
            mappings: tenant_mappings,
        })
    }
}

impl SecretProvider for TenantEnvironmentSecretProvider {
    fn descriptor(&self) -> SecretProviderDescriptor {
        self.descriptor.clone()
    }

    fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async {
            Err(HarnessError::Secret(
                "tenant environment secret provider requires tenant authority".to_owned(),
            ))
        })
    }

    fn resolve_as<'a>(
        &'a self,
        request: SecretRequest,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async move {
            authority.validate_current("Secret Provider authority")?;
            let tenant_id = authority.tenant_id().ok_or_else(|| {
                HarnessError::Secret(
                    "tenant environment secret provider requires tenant authority".to_owned(),
                )
            })?;
            let variable = self
                .mappings
                .get(tenant_id)
                .and_then(|mappings| mappings.get(&request.reference))
                .ok_or_else(|| {
                    HarnessError::Secret(
                        "secret reference is not mapped for this tenant".to_owned(),
                    )
                })?;
            resolve_environment_value(variable)
        })
    }
}

fn resolve_environment_value(variable: &str) -> Result<SecretValue, HarnessError> {
    let value = env::var(variable).map_err(|_| {
        HarnessError::Secret(
            "mapped environment credential is unavailable or not Unicode".to_owned(),
        )
    })?;
    SecretValue::new(value.into_bytes())
}

fn validate_secret_descriptor(descriptor: &SecretProviderDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("secret provider", &descriptor.name)?;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_SECRET_DESCRIPTION_BYTES
    {
        return Err(HarnessError::InvalidCapability(format!(
            "secret provider {} description must be 1-{MAX_SECRET_DESCRIPTION_BYTES} bytes",
            descriptor.name
        )));
    }
    if descriptor.api_version != SECRET_API_VERSION {
        return Err(HarnessError::InvalidCapability(format!(
            "secret provider {} API version {} is unsupported; expected {SECRET_API_VERSION}",
            descriptor.name, descriptor.api_version
        )));
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use super::{
        EnvironmentSecretProvider, SECRET_API_VERSION, SecretProvider, SecretProviderDescriptor,
        SecretReference, SecretRegistry, SecretRequest, SecretUseContext, SecretValue,
        TenantEnvironmentSecretProvider,
    };
    use crate::{
        ActorIdentity, AuthorityContext, CapabilityOrigin, HarnessError, HarnessFuture, ThreadId,
        TurnId,
    };

    struct FixedProvider;

    impl SecretProvider for FixedProvider {
        fn descriptor(&self) -> SecretProviderDescriptor {
            SecretProviderDescriptor {
                name: "fixed".to_owned(),
                description: "Test-only fixed credential resolver".to_owned(),
                api_version: SECRET_API_VERSION,
            }
        }

        fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async { SecretValue::new(b"fixture-token".to_vec()) })
        }
    }

    #[test]
    fn values_are_non_serializable_and_debug_redacted() {
        let value = SecretValue::new(b"do-not-print".to_vec()).expect("secret");
        assert_eq!(format!("{value:?}"), "SecretValue([REDACTED])");
        assert_eq!(value.expose_bytes(), b"do-not-print");
    }

    #[test]
    fn registry_is_versioned_ordered_and_collision_safe() {
        let mut registry = SecretRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(FixedProvider))
            .expect("provider");
        assert_eq!(registry.names(), ["fixed"]);
        assert!(matches!(
            registry.register(CapabilityOrigin::BuiltIn, Arc::new(FixedProvider)),
            Err(HarnessError::DuplicateCapability(_))
        ));
    }

    #[tokio::test]
    async fn environment_provider_reads_only_explicit_mappings() {
        let reference = SecretReference::new("model/gateway").expect("reference");
        let provider = EnvironmentSecretProvider::new(
            "environment",
            BTreeMap::from([(reference.clone(), "YH_TEST_DELIBERATELY_MISSING".to_owned())]),
        )
        .expect("provider");
        let error = provider
            .resolve(SecretRequest {
                reference,
                consumer: "provider/model".to_owned(),
                use_context: SecretUseContext::AgentTurn {
                    thread_id: ThreadId::from_static("thread"),
                    turn_id: TurnId::from_static("turn"),
                },
            })
            .await
            .expect_err("missing value");
        assert_eq!(
            error,
            HarnessError::Secret(
                "mapped environment credential is unavailable or not Unicode".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn legacy_provider_fails_closed_for_tenant_authority() {
        let request = request("model/gateway");
        let error = FixedProvider
            .resolve_as(request, &authority("tenant-a"))
            .await
            .expect_err("legacy tenant resolution");
        assert_eq!(
            error,
            HarnessError::Secret(
                "secret provider does not support tenant-scoped resolution".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn tenant_environment_provider_never_falls_back_across_tenants() {
        let reference = SecretReference::new("model/gateway").expect("reference");
        let provider = TenantEnvironmentSecretProvider::new(
            "tenant-environment",
            BTreeMap::from([(
                "tenant-a".to_owned(),
                BTreeMap::from([(
                    reference,
                    "YH_TEST_DELIBERATELY_MISSING_TENANT_A".to_owned(),
                )]),
            )]),
        )
        .expect("provider");
        let mapped = provider
            .resolve_as(request("model/gateway"), &authority("tenant-a"))
            .await
            .expect_err("mapped variable is deliberately absent");
        assert!(mapped.to_string().contains("unavailable"));
        let hidden = provider
            .resolve_as(request("model/gateway"), &authority("tenant-b"))
            .await
            .expect_err("cross-tenant mapping");
        assert!(hidden.to_string().contains("not mapped for this tenant"));
        let unscoped = provider
            .resolve(request("model/gateway"))
            .await
            .expect_err("unscoped access");
        assert!(unscoped.to_string().contains("requires tenant authority"));
    }

    fn request(reference: &str) -> SecretRequest {
        SecretRequest {
            reference: SecretReference::new(reference).expect("reference"),
            consumer: "provider/model".to_owned(),
            use_context: SecretUseContext::AgentTurn {
                thread_id: ThreadId::from_static("thread"),
                turn_id: TurnId::from_static("turn"),
            },
        }
    }

    fn authority(tenant_id: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "secret-test".to_owned(),
            },
            Some(tenant_id.to_owned()),
        )
        .expect("authority")
    }
}
