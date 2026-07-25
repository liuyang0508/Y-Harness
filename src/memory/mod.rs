//! Provider-neutral long-term memory contracts and capability registry.

mod agent_memory_hub;

pub use agent_memory_hub::AgentMemoryHubProvider;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    CapabilityOrigin, HarnessError, HarnessFuture,
    kernel::{
        capture_capability_metadata, validate_capability_name, validate_capability_origin,
        validate_registry_growth,
    },
};

/// Current Y-Harness Memory Provider contract version.
pub const MEMORY_API_VERSION: u32 = 1;
const MAX_MEMORY_PROVIDER_DESCRIPTION_BYTES: usize = 4_096;

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Independently negotiable operations exposed by a memory provider.
pub enum MemoryOperation {
    /// Retrieve ranked reversible context packs.
    Search,
    /// Read one referenced memory at a requested view.
    Read,
    /// Submit a durable-memory candidate.
    Write,
    /// Produce a token-bounded resume summary.
    Brief,
    /// Apply explicit outcome feedback.
    Feedback,
    /// Report provider availability and degradation.
    Health,
    /// Ingest raw evidence without promoting it to durable knowledge.
    EvidenceIngest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Registration metadata and negotiated operation surface.
pub struct MemoryProviderDescriptor {
    /// Stable registry name.
    pub name: String,
    /// Human-readable provider description.
    pub description: String,
    /// Memory contract version implemented by the provider.
    pub api_version: u32,
    /// Explicitly supported operations.
    pub operations: BTreeSet<MemoryOperation>,
}

impl MemoryProviderDescriptor {
    #[must_use]
    /// Returns whether an operation is declared by this provider.
    pub fn supports(&self, operation: &MemoryOperation) -> bool {
        self.operations.contains(operation)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Isolation and relevance scope supplied to memory operations.
pub struct MemoryScope {
    /// Optional project boundary.
    pub project: Option<String>,
    /// Optional tenant isolation boundary.
    pub tenant_id: Option<String>,
    /// Optional tag constraints.
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
/// Opaque provider-owned memory identity.
pub struct MemoryReference(String);

impl MemoryReference {
    #[must_use]
    /// Wraps an opaque provider reference.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    /// Returns the reference without interpreting its format.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Reversible loading granularity selected by a provider.
pub enum MemoryView {
    /// Minimal locator suitable for discovery.
    Locator,
    /// Compressed explanatory view.
    Overview,
    /// Canonical or bounded detailed body.
    Detail,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Provider-supplied lineage reference.
pub struct MemoryProvenance {
    /// Provider-neutral reference class, such as `url` or `commit`.
    pub kind: String,
    /// Opaque locator within that class.
    pub reference: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Reversible, provider-governed text pack eligible for model context.
pub struct MemoryContextPack {
    /// Opaque memory identity.
    pub reference: MemoryReference,
    /// Optional display title.
    pub title: Option<String>,
    /// Provider-selected prompt text.
    pub text: String,
    /// Granularity represented by `text`.
    pub selected_view: MemoryView,
    /// Optional canonical deep-read locator.
    pub detail_uri: Option<String>,
    /// Provider-reported token estimate.
    pub packed_tokens: usize,
    /// Available lineage locators.
    pub provenance: Vec<MemoryProvenance>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Ranked memory retrieval request.
pub struct MemorySearchRequest {
    /// Semantic and lexical query.
    pub query: String,
    /// Isolation and relevance scope.
    pub scope: MemoryScope,
    /// Maximum candidate count requested.
    pub top_k: usize,
    /// Token budget communicated to the provider.
    pub budget_tokens: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Memory retrieval result before final Context Engine budgeting.
pub struct MemorySearchResponse {
    /// Provider-ranked context packs.
    pub packs: Vec<MemoryContextPack>,
    /// Non-fatal provider warnings.
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Bounded deep-read request.
pub struct MemoryReadRequest {
    /// Opaque memory identity.
    pub reference: MemoryReference,
    /// Requested loading view.
    pub view: MemoryView,
    /// Optional maximum character count for detailed text.
    pub head_chars: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Bounded text and lineage returned by a memory read.
pub struct MemoryReadResponse {
    /// Read memory identity.
    pub reference: MemoryReference,
    /// Requested provider text.
    pub text: String,
    /// Whether a provider-enforced bound omitted remaining text.
    pub truncated: bool,
    /// Available lineage locators.
    pub provenance: Vec<MemoryProvenance>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Governed durable-memory write candidate.
pub struct MemoryWriteRequest {
    /// Caller-generated key used only when a provider supports idempotent settlement.
    pub idempotency_key: String,
    /// Provider-mapped semantic record type.
    pub kind: String,
    /// Human-readable title.
    pub title: String,
    /// Retrieval-oriented summary.
    pub summary: String,
    /// Detailed record body.
    pub body: String,
    /// Isolation and relevance scope.
    pub scope: MemoryScope,
    /// Source lineage locators.
    pub provenance: Vec<MemoryProvenance>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Provider acknowledgement for a durable-memory write.
pub struct MemoryWriteResponse {
    /// Settled provider identity; absent means no durable acknowledgement.
    pub reference: Option<MemoryReference>,
    /// Governance or quality warnings.
    pub warnings: Vec<String>,
    /// Derived-service degradation such as delayed indexing.
    pub degraded: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Token-bounded resume briefing request.
pub struct MemoryBriefRequest {
    /// Isolation and relevance scope.
    pub scope: MemoryScope,
    /// Optional query used to prioritize summary items.
    pub query: Option<String>,
    /// Maximum requested token budget.
    pub budget_tokens: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Resume briefing represented as reversible context packs.
pub struct MemoryBriefResponse {
    /// Included summary packs.
    pub packs: Vec<MemoryContextPack>,
    /// Count omitted because of provider or caller budget.
    pub withheld: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Explicit outcome evidence for a prior memory-injection cohort.
pub struct MemoryFeedbackRequest {
    /// References supported by task outcome.
    pub adopted: Vec<MemoryReference>,
    /// References contradicted or rejected by outcome.
    pub rejected: Vec<MemoryReference>,
    /// Seen references with no outcome evidence; not negative feedback.
    pub ignored: Vec<MemoryReference>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Provider availability classification.
pub enum MemoryHealthStatus {
    /// Required operations are available.
    Healthy,
    /// Core operations work with declared limitations.
    Degraded,
    /// Required operations cannot be used.
    Unavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Memory provider health result.
pub struct MemoryHealth {
    /// Availability classification.
    pub status: MemoryHealthStatus,
    /// Optional actionable diagnostic.
    pub message: Option<String>,
}

/// Provider-neutral long-term memory capability.
///
/// Optional methods default to an explicit unsupported error. Implementations
/// must declare the matching operation before callers invoke it.
pub trait MemoryProvider: Send + Sync {
    /// Returns stable registration and operation metadata.
    fn descriptor(&self) -> MemoryProviderDescriptor;

    /// Retrieves ranked context packs within the requested scope.
    fn search<'a>(
        &'a self,
        request: MemorySearchRequest,
    ) -> HarnessFuture<'a, MemorySearchResponse>;

    /// Reads one referenced memory.
    fn read<'a>(&'a self, _request: MemoryReadRequest) -> HarnessFuture<'a, MemoryReadResponse> {
        unsupported("read")
    }

    /// Submits a governed durable-memory candidate.
    fn write<'a>(&'a self, _request: MemoryWriteRequest) -> HarnessFuture<'a, MemoryWriteResponse> {
        unsupported("write")
    }

    /// Produces a token-bounded resume briefing.
    fn brief<'a>(&'a self, _request: MemoryBriefRequest) -> HarnessFuture<'a, MemoryBriefResponse> {
        unsupported("brief")
    }

    /// Applies explicit outcome feedback; retrieval alone is never feedback.
    fn feedback<'a>(&'a self, _request: MemoryFeedbackRequest) -> HarnessFuture<'a, ()> {
        unsupported("feedback")
    }

    /// Reports current provider health.
    fn health<'a>(&'a self) -> HarnessFuture<'a, MemoryHealth> {
        unsupported("health")
    }
}

fn unsupported<'a, T>(operation: &'static str) -> HarnessFuture<'a, T> {
    Box::pin(async move {
        Err(HarnessError::Memory(format!(
            "memory provider does not support {operation}"
        )))
    })
}

/// Registered memory implementation with its validated origin.
pub struct RegisteredMemoryProvider {
    /// Validated provider metadata.
    pub descriptor: MemoryProviderDescriptor,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Provider implementation.
    pub provider: Arc<dyn MemoryProvider>,
}

#[derive(Default)]
/// Deterministic registry for memory provider capabilities.
pub struct MemoryRegistry {
    providers: BTreeMap<String, RegisteredMemoryProvider>,
}

impl MemoryRegistry {
    #[must_use]
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers a provider without allowing name replacement.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        provider: Arc<dyn MemoryProvider>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("memory provider", self.providers.len(), 1)?;
        let descriptor =
            capture_capability_metadata("memory provider descriptor", || provider.descriptor())?;
        validate_memory_descriptor(&descriptor)?;
        if self.providers.contains_key(&descriptor.name) {
            return Err(HarnessError::DuplicateCapability(descriptor.name));
        }
        self.providers.insert(
            descriptor.name.clone(),
            RegisteredMemoryProvider {
                descriptor,
                origin,
                provider,
            },
        );
        Ok(())
    }

    #[must_use]
    /// Looks up a provider by stable name.
    pub fn get(&self, name: &str) -> Option<&RegisteredMemoryProvider> {
        self.providers.get(name)
    }
}

fn validate_memory_descriptor(descriptor: &MemoryProviderDescriptor) -> Result<(), HarnessError> {
    validate_capability_name("memory provider", &descriptor.name)?;
    if descriptor.description.trim().is_empty()
        || descriptor.description.len() > MAX_MEMORY_PROVIDER_DESCRIPTION_BYTES
    {
        return Err(HarnessError::InvalidCapability(format!(
            "memory provider {} description must be 1-{MAX_MEMORY_PROVIDER_DESCRIPTION_BYTES} bytes",
            descriptor.name,
        )));
    }
    if descriptor.api_version != MEMORY_API_VERSION {
        return Err(HarnessError::InvalidCapability(format!(
            "memory provider {} uses API version {}, expected {}",
            descriptor.name, descriptor.api_version, MEMORY_API_VERSION
        )));
    }
    if descriptor.operations.is_empty() {
        return Err(HarnessError::InvalidCapability(format!(
            "memory provider {} declares no operations",
            descriptor.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use super::{
        MEMORY_API_VERSION, MemoryOperation, MemoryProvider, MemoryProviderDescriptor,
        MemoryRegistry, MemorySearchRequest, MemorySearchResponse,
    };
    use crate::{CapabilityOrigin, HarnessFuture};

    struct SearchProvider;

    impl MemoryProvider for SearchProvider {
        fn descriptor(&self) -> MemoryProviderDescriptor {
            MemoryProviderDescriptor {
                name: "memory".to_owned(),
                description: "test memory".to_owned(),
                api_version: MEMORY_API_VERSION,
                operations: BTreeSet::from([MemoryOperation::Search]),
            }
        }

        fn search<'a>(
            &'a self,
            _request: MemorySearchRequest,
        ) -> HarnessFuture<'a, MemorySearchResponse> {
            Box::pin(async { Ok(MemorySearchResponse::default()) })
        }
    }

    #[test]
    fn rejects_duplicate_provider_names() {
        let mut registry = MemoryRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(SearchProvider))
            .expect("first provider");
        let error = registry
            .register(CapabilityOrigin::BuiltIn, Arc::new(SearchProvider))
            .expect_err("duplicate should fail");
        assert!(error.to_string().contains("duplicate capability"));
    }
}
