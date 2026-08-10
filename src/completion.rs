//! Deterministic, evidence-bound contracts for successful Turn settlement.
//!
//! A receipt proves that one exact assistant candidate and its verification
//! suffix were settled against one frozen Runtime generation. It deliberately
//! contains no generated receipt identity or wall-clock value: identical
//! authoritative Turn state produces identical receipt bytes and digest.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    ActorIdentity, ApprovalDecision, ApprovalId, AuthorityContext, CapabilityOrigin,
    ExecutionBinding, HarnessError, Item, ItemId, ItemKind, PolicyDecision, RiskLevel, ThreadId,
    Turn, TurnId, TurnStatus, VerificationOutcome, VerifierDescriptor,
    json::{BoundedJsonError, to_bounded_json_vec, validate_value_shape},
    kernel::{validate_capability_name, validate_capability_origin, validate_model_id},
};

/// Current deterministic completion contract format.
pub const COMPLETION_FORMAT_VERSION: u32 = 1;
/// Maximum encoded size of one durable completion receipt.
pub const MAX_COMPLETION_RECEIPT_BYTES: usize = 4_096;
/// Maximum canonical JSON input accepted by one completion digest operation.
pub const MAX_COMPLETION_HASH_INPUT_BYTES: usize = 67_108_864;

const HASH_PREFIX: &[u8] = b"Y-HARNESS\0COMPLETION\0SHA-256\0V1\0";
const DOMAIN_MODEL_REQUEST: &str = "model-request";
const DOMAIN_MODEL_ROUTE: &str = "model-route";
const DOMAIN_TOOL_VIEW: &str = "tool-view";
const DOMAIN_VERIFIER_BINDING: &str = "verifier-binding";
const DOMAIN_VERIFIER_MANIFEST: &str = "verifier-manifest";
const DOMAIN_RUNTIME_GOVERNANCE: &str = "runtime-governance";
const DOMAIN_EXECUTION_BINDING: &str = "execution-binding-item";
const DOMAIN_CANDIDATE: &str = "candidate-item";
const DOMAIN_TURN_EVIDENCE: &str = "turn-evidence";
const DOMAIN_VERIFIER_RESULTS: &str = "verifier-results";
const DOMAIN_GENERATION: &str = "generation";
const DOMAIN_RECEIPT: &str = "receipt";

struct SerializableReference<'a, T: ?Sized>(&'a T);

impl<T: Serialize + ?Sized> Serialize for SerializableReference<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// Settlement state for a completion requirement declared by the caller.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionRequirementStatus {
    /// This requirement is explicitly outside the current completion claim.
    NotRequired,
    /// The requirement was satisfied by evidence understood by this format.
    Satisfied,
}

/// Strength of the Runtime generation bound into a completion receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionAssurance {
    /// Runtime configuration was measured, without a trusted deployment item.
    RuntimeMeasured,
    /// Runtime configuration is also bound to one trusted ExecutionBinding.
    DeploymentBound,
}

/// Explicit non-verifier obligations carried by a completion claim.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionContract {
    artifact: CompletionRequirementStatus,
    effect: CompletionRequirementStatus,
    business_delivery: CompletionRequirementStatus,
}

impl CompletionContract {
    /// Creates the only contract supported by format version 1.
    ///
    /// V1 does not pretend that a conversational Turn receipt proves external
    /// artifact delivery, durable Effects, or business-system delivery.
    #[must_use]
    pub const fn v1_no_external_requirements() -> Self {
        Self {
            artifact: CompletionRequirementStatus::NotRequired,
            effect: CompletionRequirementStatus::NotRequired,
            business_delivery: CompletionRequirementStatus::NotRequired,
        }
    }

    /// Returns the artifact obligation.
    #[must_use]
    pub const fn artifact(&self) -> CompletionRequirementStatus {
        self.artifact
    }

    /// Returns the durable Effect obligation.
    #[must_use]
    pub const fn effect(&self) -> CompletionRequirementStatus {
        self.effect
    }

    /// Returns the authoritative business-delivery obligation.
    #[must_use]
    pub const fn business_delivery(&self) -> CompletionRequirementStatus {
        self.business_delivery
    }

    fn validate_v1(&self) -> Result<(), HarnessError> {
        if self.artifact != CompletionRequirementStatus::NotRequired
            || self.effect != CompletionRequirementStatus::NotRequired
            || self.business_delivery != CompletionRequirementStatus::NotRequired
        {
            return Err(completion_configuration_error(
                "completion format 1 cannot claim artifact, Effect, or business delivery",
            ));
        }
        Ok(())
    }
}

/// Frozen identity of one verifier participating in a completion generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionVerifierBinding {
    name: String,
    origin: CapabilityOrigin,
    binding_sha256: String,
}

impl CompletionVerifierBinding {
    /// Freezes one validated verifier descriptor and trust origin.
    pub fn new(
        descriptor: &VerifierDescriptor,
        origin: CapabilityOrigin,
    ) -> Result<Self, HarnessError> {
        descriptor.validate()?;
        validate_capability_origin(&origin)?;
        let binding_sha256 = completion_verifier_binding_sha256(descriptor, &origin)?;
        Ok(Self {
            name: descriptor.name.clone(),
            origin,
            binding_sha256,
        })
    }

    /// Returns the registered verifier name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the verifier's trust-bearing origin.
    #[must_use]
    pub fn origin(&self) -> &CapabilityOrigin {
        &self.origin
    }

    /// Returns the frozen verifier contract digest.
    #[must_use]
    pub fn binding_sha256(&self) -> &str {
        &self.binding_sha256
    }

    fn from_result(
        name: String,
        origin: CapabilityOrigin,
        binding_sha256: String,
    ) -> Result<Self, HarnessError> {
        validate_capability_name("completion verifier", &name)?;
        validate_capability_origin(&origin)?;
        require_sha256("completion verifier binding", &binding_sha256)?;
        Ok(Self {
            name,
            origin,
            binding_sha256,
        })
    }
}

/// Frozen Runtime coordinates against which one candidate was produced.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionGeneration {
    format_version: u32,
    model_request_sha256: String,
    model_route_sha256: String,
    tool_view_sha256: String,
    verifier_manifest_sha256: String,
    runtime_governance_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    execution_binding_sha256: Option<String>,
    assurance: CompletionAssurance,
    generation_sha256: String,
}

impl CompletionGeneration {
    /// Constructs and self-digests one format-1 Runtime generation.
    pub fn new(
        model_request_sha256: impl Into<String>,
        model_route_sha256: impl Into<String>,
        tool_view_sha256: impl Into<String>,
        verifier_manifest_sha256: impl Into<String>,
        runtime_governance_sha256: impl Into<String>,
        execution_binding_sha256: Option<String>,
        assurance: CompletionAssurance,
    ) -> Result<Self, HarnessError> {
        let mut generation = Self {
            format_version: COMPLETION_FORMAT_VERSION,
            model_request_sha256: model_request_sha256.into(),
            model_route_sha256: model_route_sha256.into(),
            tool_view_sha256: tool_view_sha256.into(),
            verifier_manifest_sha256: verifier_manifest_sha256.into(),
            runtime_governance_sha256: runtime_governance_sha256.into(),
            execution_binding_sha256,
            assurance,
            generation_sha256: String::new(),
        };
        generation.validate_components()?;
        generation.generation_sha256 = generation.compute_sha256()?;
        Ok(generation)
    }

    /// Validates version, digest shapes, assurance, and the self digest.
    pub fn validate(&self) -> Result<(), HarnessError> {
        self.validate_components()?;
        require_sha256("completion generation", &self.generation_sha256)?;
        if self.compute_sha256()? != self.generation_sha256 {
            return Err(completion_configuration_error(
                "completion generation digest does not match its components",
            ));
        }
        Ok(())
    }

    /// Returns the model-request digest.
    #[must_use]
    pub fn model_request_sha256(&self) -> &str {
        &self.model_request_sha256
    }

    /// Returns the ordered Model-route digest.
    #[must_use]
    pub fn model_route_sha256(&self) -> &str {
        &self.model_route_sha256
    }

    /// Returns the exact model-visible Tool-view digest.
    #[must_use]
    pub fn tool_view_sha256(&self) -> &str {
        &self.tool_view_sha256
    }

    /// Returns the frozen verifier-manifest digest.
    #[must_use]
    pub fn verifier_manifest_sha256(&self) -> &str {
        &self.verifier_manifest_sha256
    }

    /// Returns the deterministic Runtime-governance digest.
    #[must_use]
    pub fn runtime_governance_sha256(&self) -> &str {
        &self.runtime_governance_sha256
    }

    /// Returns the exact ExecutionBinding item digest, when deployment-bound.
    #[must_use]
    pub fn execution_binding_sha256(&self) -> Option<&str> {
        self.execution_binding_sha256.as_deref()
    }

    /// Returns the declared assurance level.
    #[must_use]
    pub const fn assurance(&self) -> CompletionAssurance {
        self.assurance
    }

    /// Returns the digest of all generation components.
    #[must_use]
    pub fn generation_sha256(&self) -> &str {
        &self.generation_sha256
    }

    fn validate_components(&self) -> Result<(), HarnessError> {
        if self.format_version != COMPLETION_FORMAT_VERSION {
            return Err(completion_configuration_error(
                "unsupported completion generation format",
            ));
        }
        for (kind, digest) in [
            ("completion model request", &self.model_request_sha256),
            ("completion model route", &self.model_route_sha256),
            ("completion Tool view", &self.tool_view_sha256),
            (
                "completion verifier manifest",
                &self.verifier_manifest_sha256,
            ),
            (
                "completion Runtime governance",
                &self.runtime_governance_sha256,
            ),
        ] {
            require_sha256(kind, digest)?;
        }
        if let Some(digest) = &self.execution_binding_sha256 {
            require_sha256("completion ExecutionBinding", digest)?;
        }
        match (self.assurance, self.execution_binding_sha256.is_some()) {
            (CompletionAssurance::RuntimeMeasured, false)
            | (CompletionAssurance::DeploymentBound, true) => Ok(()),
            (CompletionAssurance::RuntimeMeasured, true) => Err(completion_configuration_error(
                "runtime-measured completion cannot carry an ExecutionBinding digest",
            )),
            (CompletionAssurance::DeploymentBound, false) => Err(completion_configuration_error(
                "deployment-bound completion requires an ExecutionBinding digest",
            )),
        }
    }

    fn compute_sha256(&self) -> Result<String, HarnessError> {
        #[derive(Serialize)]
        struct Payload<'a> {
            format_version: u32,
            model_request_sha256: &'a str,
            model_route_sha256: &'a str,
            tool_view_sha256: &'a str,
            verifier_manifest_sha256: &'a str,
            runtime_governance_sha256: &'a str,
            execution_binding_sha256: Option<&'a str>,
            assurance: CompletionAssurance,
        }
        canonical_json_sha256(
            DOMAIN_GENERATION,
            &Payload {
                format_version: self.format_version,
                model_request_sha256: &self.model_request_sha256,
                model_route_sha256: &self.model_route_sha256,
                tool_view_sha256: &self.tool_view_sha256,
                verifier_manifest_sha256: &self.verifier_manifest_sha256,
                runtime_governance_sha256: &self.runtime_governance_sha256,
                execution_binding_sha256: self.execution_binding_sha256.as_deref(),
                assurance: self.assurance,
            },
            MAX_COMPLETION_RECEIPT_BYTES,
        )
    }
}

/// Deterministic successful-completion proof stored with a Turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionReceipt {
    format_version: u32,
    authority: AuthorityContext,
    /// Original authoritative Thread whose item journal produced this proof.
    source_thread_id: ThreadId,
    /// Exact Turn identity settled by this proof. Fork/import projection does
    /// not rewrite this coordinate.
    turn_id: TurnId,
    candidate_item_id: ItemId,
    candidate_sha256: String,
    turn_evidence_sha256: String,
    verifier_results_sha256: String,
    verifier_count: u32,
    generation: CompletionGeneration,
    contract: CompletionContract,
}

impl CompletionReceipt {
    /// Returns the trusted authority carried by the receipt.
    #[must_use]
    pub fn authority(&self) -> &AuthorityContext {
        &self.authority
    }

    /// Returns the original authoritative Thread bound by this receipt.
    #[must_use]
    pub fn source_thread_id(&self) -> &ThreadId {
        &self.source_thread_id
    }

    /// Returns the exact settled Turn identity.
    #[must_use]
    pub fn turn_id(&self) -> &TurnId {
        &self.turn_id
    }

    /// Returns the exact assistant candidate identity.
    #[must_use]
    pub fn candidate_item_id(&self) -> &ItemId {
        &self.candidate_item_id
    }

    /// Returns the exact assistant Item digest.
    #[must_use]
    pub fn candidate_sha256(&self) -> &str {
        &self.candidate_sha256
    }

    /// Returns the digest of all Turn evidence through the candidate.
    #[must_use]
    pub fn turn_evidence_sha256(&self) -> &str {
        &self.turn_evidence_sha256
    }

    /// Returns the digest of the candidate-bound verifier suffix.
    #[must_use]
    pub fn verifier_results_sha256(&self) -> &str {
        &self.verifier_results_sha256
    }

    /// Returns the number of candidate-bound verifier results.
    #[must_use]
    pub const fn verifier_count(&self) -> u32 {
        self.verifier_count
    }

    /// Returns the frozen Runtime generation.
    #[must_use]
    pub fn generation(&self) -> &CompletionGeneration {
        &self.generation
    }

    /// Returns the explicit non-verifier completion contract.
    #[must_use]
    pub fn contract(&self) -> &CompletionContract {
        &self.contract
    }
}

/// Hashes the exact provider-neutral Model request in its dedicated domain.
pub fn completion_model_request_sha256<T: Serialize + ?Sized>(
    request: &T,
) -> Result<String, HarnessError> {
    canonical_json_sha256(
        DOMAIN_MODEL_REQUEST,
        request,
        MAX_COMPLETION_HASH_INPUT_BYTES,
    )
}

/// Hashes the ordered Model-route snapshot in its dedicated domain.
pub fn completion_model_route_sha256<T: Serialize + ?Sized>(
    route: &T,
) -> Result<String, HarnessError> {
    canonical_json_sha256(DOMAIN_MODEL_ROUTE, route, MAX_COMPLETION_HASH_INPUT_BYTES)
}

/// Hashes the exact model-visible Tool view in its dedicated domain.
pub fn completion_tool_view_sha256<T: Serialize + ?Sized>(
    tool_view: &T,
) -> Result<String, HarnessError> {
    canonical_json_sha256(DOMAIN_TOOL_VIEW, tool_view, MAX_COMPLETION_HASH_INPUT_BYTES)
}

/// Hashes one validated verifier descriptor and origin.
pub fn completion_verifier_binding_sha256(
    descriptor: &VerifierDescriptor,
    origin: &CapabilityOrigin,
) -> Result<String, HarnessError> {
    descriptor.validate()?;
    validate_capability_origin(origin)?;
    canonical_json_sha256(
        DOMAIN_VERIFIER_BINDING,
        &(descriptor, origin),
        MAX_COMPLETION_HASH_INPUT_BYTES,
    )
}

/// Hashes a strictly name-ordered frozen verifier manifest.
pub fn completion_verifier_manifest_sha256(
    bindings: &[CompletionVerifierBinding],
) -> Result<String, HarnessError> {
    validate_verifier_manifest(bindings)?;
    canonical_json_sha256(
        DOMAIN_VERIFIER_MANIFEST,
        bindings,
        MAX_COMPLETION_HASH_INPUT_BYTES,
    )
}

/// Hashes deterministic Agent-Loop budgets and policies in a dedicated domain.
pub fn completion_runtime_governance_sha256<T: Serialize + ?Sized>(
    governance: &T,
) -> Result<String, HarnessError> {
    canonical_json_sha256(
        DOMAIN_RUNTIME_GOVERNANCE,
        governance,
        MAX_COMPLETION_HASH_INPUT_BYTES,
    )
}

/// Hashes the exact trusted ExecutionBinding Item.
pub fn completion_execution_binding_sha256(item: &Item) -> Result<String, HarnessError> {
    if !matches!(item.kind, ItemKind::ExecutionBinding { .. }) {
        return Err(completion_configuration_error(
            "ExecutionBinding digest requires an ExecutionBinding Item",
        ));
    }
    canonical_json_sha256(
        DOMAIN_EXECUTION_BINDING,
        item,
        MAX_COMPLETION_HASH_INPUT_BYTES,
    )
}

/// Builds and fully validates a receipt from one still-running Turn.
pub fn build_completion_receipt(
    turn: &Turn,
    authority: &AuthorityContext,
    candidate_item_id: &ItemId,
    generation: CompletionGeneration,
    contract: CompletionContract,
) -> Result<CompletionReceipt, HarnessError> {
    let evidence = derive_turn_evidence(turn, candidate_item_id)?;
    let verifier_count = u32::try_from(evidence.verifier_count)
        .map_err(|_| completion_validation_error("completion verifier count exceeds u32"))?;
    let receipt = CompletionReceipt {
        format_version: COMPLETION_FORMAT_VERSION,
        authority: authority.clone(),
        source_thread_id: turn.thread_id.clone(),
        turn_id: turn.id.clone(),
        candidate_item_id: candidate_item_id.clone(),
        candidate_sha256: evidence.candidate_sha256,
        turn_evidence_sha256: evidence.turn_evidence_sha256,
        verifier_results_sha256: evidence.verifier_results_sha256,
        verifier_count,
        generation,
        contract,
    };
    validate_turn_completion_receipt(turn, authority.tenant_id(), &receipt)?;
    Ok(receipt)
}

/// Validates a receipt against the authoritative pre-transition running Turn.
pub fn validate_turn_completion_receipt(
    turn: &Turn,
    thread_tenant: Option<&str>,
    receipt: &CompletionReceipt,
) -> Result<(), HarnessError> {
    if turn.status != TurnStatus::Running || turn.completion_receipt.is_some() {
        return Err(completion_validation_error(
            "completion receipt requires an unsettled running Turn",
        ));
    }
    validate_receipt_against_items(turn, thread_tenant, receipt, SourcePlacement::Direct)
}

/// Validates the receipt retained by a projected completed Turn.
pub fn validate_projected_turn_completion_receipt(
    turn: &Turn,
    thread_tenant: Option<&str>,
    receipt: &CompletionReceipt,
) -> Result<(), HarnessError> {
    if turn.status != TurnStatus::Completed || turn.completion_receipt.as_ref() != Some(receipt) {
        return Err(completion_validation_error(
            "projected completed Turn must retain the exact completion receipt",
        ));
    }
    validate_receipt_against_items(turn, thread_tenant, receipt, SourcePlacement::Direct)
}

/// Validates an unchanged source receipt retained by a forked or imported
/// completed Turn.
///
/// The caller must first prove from authoritative Thread state that the
/// projected Thread has fork lineage or import provenance. This function
/// permits only the enclosing Thread identity to differ; Turn identity,
/// items, tenant, candidate, verifier evidence, and generation remain exact.
pub fn validate_inherited_projected_turn_completion_receipt(
    turn: &Turn,
    thread_tenant: Option<&str>,
    receipt: &CompletionReceipt,
) -> Result<(), HarnessError> {
    if turn.status != TurnStatus::Completed || turn.completion_receipt.as_ref() != Some(receipt) {
        return Err(completion_validation_error(
            "inherited completed Turn must retain the exact source completion receipt",
        ));
    }
    validate_receipt_against_items(turn, thread_tenant, receipt, SourcePlacement::Inherited)
}

/// Returns the deterministic digest of one standalone, structurally valid receipt.
pub fn completion_receipt_sha256(receipt: &CompletionReceipt) -> Result<String, HarnessError> {
    validate_receipt_shape(receipt)?;
    canonical_json_sha256(DOMAIN_RECEIPT, receipt, MAX_COMPLETION_RECEIPT_BYTES)
}

fn validate_receipt_against_items(
    turn: &Turn,
    thread_tenant: Option<&str>,
    receipt: &CompletionReceipt,
    placement: SourcePlacement,
) -> Result<(), HarnessError> {
    validate_receipt_shape(receipt)?;
    receipt
        .authority
        .validate_current("completion receipt authority")?;
    if receipt.authority.tenant_id() != thread_tenant {
        return Err(completion_validation_error(
            "completion receipt tenant differs from its Thread",
        ));
    }
    if receipt.turn_id != turn.id {
        return Err(completion_validation_error(
            "completion receipt Turn identity differs from its projected Turn",
        ));
    }
    if placement == SourcePlacement::Direct && receipt.source_thread_id != turn.thread_id {
        return Err(completion_validation_error(
            "direct completion receipt source Thread differs from its Turn",
        ));
    }
    validate_settled_tools_and_approvals(&turn.items)?;
    validate_execution_assurance(
        &turn.items,
        thread_tenant,
        &receipt.authority,
        &receipt.generation,
    )?;
    let evidence = derive_turn_evidence(turn, &receipt.candidate_item_id)?;
    if evidence.candidate_sha256 != receipt.candidate_sha256
        || evidence.turn_evidence_sha256 != receipt.turn_evidence_sha256
        || evidence.verifier_results_sha256 != receipt.verifier_results_sha256
        || evidence.verifier_count != receipt.verifier_count as usize
    {
        return Err(completion_validation_error(
            "completion receipt does not match current Turn evidence",
        ));
    }
    if evidence.model_request_sha256 != receipt.generation.model_request_sha256 {
        return Err(completion_validation_error(
            "completion candidate Model request differs from its generation",
        ));
    }
    if evidence.verifier_manifest_sha256 != receipt.generation.verifier_manifest_sha256 {
        return Err(completion_validation_error(
            "completion verifier suffix differs from its frozen manifest",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SourcePlacement {
    Direct,
    Inherited,
}

fn validate_receipt_shape(receipt: &CompletionReceipt) -> Result<(), HarnessError> {
    if receipt.format_version != COMPLETION_FORMAT_VERSION {
        return Err(completion_validation_error(
            "unsupported completion receipt format",
        ));
    }
    receipt
        .authority
        .validate_current("completion receipt authority")?;
    require_portable_id(
        "completion source Thread identity",
        receipt.source_thread_id.as_str(),
    )?;
    require_portable_id("completion Turn identity", receipt.turn_id.as_str())?;
    require_portable_id(
        "completion candidate Item identity",
        receipt.candidate_item_id.as_str(),
    )?;
    for (kind, digest) in [
        ("completion candidate", &receipt.candidate_sha256),
        ("completion Turn evidence", &receipt.turn_evidence_sha256),
        (
            "completion verifier results",
            &receipt.verifier_results_sha256,
        ),
    ] {
        require_sha256(kind, digest)?;
    }
    receipt.generation.validate()?;
    receipt.contract.validate_v1()?;
    to_bounded_json_vec(receipt, MAX_COMPLETION_RECEIPT_BYTES)
        .map(|_| ())
        .map_err(|error| bounded_error("completion receipt", MAX_COMPLETION_RECEIPT_BYTES, error))
}

struct DerivedTurnEvidence {
    candidate_sha256: String,
    turn_evidence_sha256: String,
    verifier_results_sha256: String,
    verifier_manifest_sha256: String,
    verifier_count: usize,
    model_request_sha256: String,
}

fn derive_turn_evidence(
    turn: &Turn,
    candidate_item_id: &ItemId,
) -> Result<DerivedTurnEvidence, HarnessError> {
    let candidate_index = turn
        .items
        .iter()
        .position(|item| &item.id == candidate_item_id)
        .ok_or_else(|| completion_validation_error("completion candidate Item does not exist"))?;
    if turn
        .items
        .iter()
        .skip(candidate_index + 1)
        .any(|item| !matches!(item.kind, ItemKind::VerificationResult { .. }))
    {
        return Err(completion_validation_error(
            "only candidate-bound VerificationResult Items may follow the completion candidate",
        ));
    }
    let candidate = &turn.items[candidate_index];
    let ItemKind::AssistantMessage {
        model_id: Some(model_id),
        model_origin: Some(model_origin),
        model_request_sha256: Some(model_request_sha256),
        ..
    } = &candidate.kind
    else {
        return Err(completion_validation_error(
            "completion candidate must be an attributed AssistantMessage with a Model request digest",
        ));
    };
    validate_model_id(model_id).map_err(|_| {
        completion_validation_error("completion candidate contains an invalid Model identity")
    })?;
    validate_capability_origin(model_origin).map_err(|_| {
        completion_validation_error("completion candidate contains an invalid Model origin")
    })?;
    require_sha256("completion candidate Model request", model_request_sha256)?;

    let suffix = &turn.items[candidate_index + 1..];
    let mut bindings = Vec::with_capacity(suffix.len());
    let mut previous_name: Option<&str> = None;
    for item in suffix {
        let ItemKind::VerificationResult {
            verifier,
            candidate_item_id: Some(result_candidate_id),
            verifier_origin: Some(verifier_origin),
            verifier_binding_sha256: Some(binding_sha256),
            outcome: VerificationOutcome::Passed { .. },
        } = &item.kind
        else {
            return Err(completion_validation_error(
                "completion suffix requires attributed, candidate-bound passing verifier results",
            ));
        };
        if result_candidate_id != candidate_item_id {
            return Err(completion_validation_error(
                "completion verifier result references a different candidate",
            ));
        }
        if previous_name.is_some_and(|previous| previous >= verifier.as_str()) {
            return Err(completion_validation_error(
                "completion verifier results must be strictly ordered by name",
            ));
        }
        previous_name = Some(verifier);
        bindings.push(CompletionVerifierBinding::from_result(
            verifier.clone(),
            verifier_origin.clone(),
            binding_sha256.clone(),
        )?);
    }

    Ok(DerivedTurnEvidence {
        candidate_sha256: canonical_json_sha256(
            DOMAIN_CANDIDATE,
            candidate,
            MAX_COMPLETION_HASH_INPUT_BYTES,
        )?,
        turn_evidence_sha256: canonical_json_sha256(
            DOMAIN_TURN_EVIDENCE,
            &turn.items[..=candidate_index],
            MAX_COMPLETION_HASH_INPUT_BYTES,
        )?,
        verifier_results_sha256: canonical_json_sha256(
            DOMAIN_VERIFIER_RESULTS,
            suffix,
            MAX_COMPLETION_HASH_INPUT_BYTES,
        )?,
        verifier_manifest_sha256: completion_verifier_manifest_sha256(&bindings)?,
        verifier_count: suffix.len(),
        model_request_sha256: model_request_sha256.clone(),
    })
}

fn validate_execution_assurance(
    items: &[Item],
    thread_tenant: Option<&str>,
    authority: &AuthorityContext,
    generation: &CompletionGeneration,
) -> Result<(), HarnessError> {
    let bindings = items
        .iter()
        .filter(|item| matches!(item.kind, ItemKind::ExecutionBinding { .. }))
        .collect::<Vec<_>>();
    match generation.assurance {
        CompletionAssurance::RuntimeMeasured if bindings.is_empty() => Ok(()),
        CompletionAssurance::RuntimeMeasured => Err(completion_validation_error(
            "runtime-measured completion cannot contain an ExecutionBinding Item",
        )),
        CompletionAssurance::DeploymentBound if bindings.len() == 1 => {
            let item = bindings[0];
            let ItemKind::ExecutionBinding { bound_by, binding } = &item.kind else {
                return Err(completion_validation_error(
                    "completion ExecutionBinding selection became inconsistent",
                ));
            };
            validate_execution_binding_authority(bound_by, binding, thread_tenant, authority)?;
            let actual = completion_execution_binding_sha256(item)?;
            if generation.execution_binding_sha256.as_deref() != Some(actual.as_str()) {
                return Err(completion_validation_error(
                    "completion generation does not match the exact ExecutionBinding Item",
                ));
            }
            Ok(())
        }
        CompletionAssurance::DeploymentBound => Err(completion_validation_error(
            "deployment-bound completion requires exactly one ExecutionBinding Item",
        )),
    }
}

fn validate_execution_binding_authority(
    bound_by: &ActorIdentity,
    binding: &ExecutionBinding,
    thread_tenant: Option<&str>,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    if bound_by != authority.actor() || binding.tenant_id() != thread_tenant {
        return Err(completion_validation_error(
            "completion ExecutionBinding authority or tenant does not match the receipt",
        ));
    }
    binding
        .validate()
        .map_err(|_| completion_validation_error("completion contains an invalid ExecutionBinding"))
}

fn validate_settled_tools_and_approvals(items: &[Item]) -> Result<(), HarnessError> {
    enum RecordedPolicy {
        Allow,
        Deny,
        Ask { reason: String, risk: RiskLevel },
    }

    struct CallSettlement {
        tool: String,
        policy: Option<RecordedPolicy>,
        approval_id: Option<ApprovalId>,
        approval_approved: bool,
        result_is_error: Option<bool>,
    }

    let mut calls: BTreeMap<String, CallSettlement> = BTreeMap::new();
    let mut approvals: BTreeMap<ApprovalId, String> = BTreeMap::new();
    for item in items {
        match &item.kind {
            ItemKind::ToolCall { call_id, name, .. } => {
                if calls
                    .insert(
                        call_id.clone(),
                        CallSettlement {
                            tool: name.clone(),
                            policy: None,
                            approval_id: None,
                            approval_approved: false,
                            result_is_error: None,
                        },
                    )
                    .is_some()
                {
                    return Err(completion_validation_error(
                        "completion Turn contains duplicate ToolCall identities",
                    ));
                }
            }
            ItemKind::PolicyDecision {
                call_id, decision, ..
            } => {
                let call = calls.get_mut(call_id).ok_or_else(|| {
                    completion_validation_error("completion Turn contains an orphan PolicyDecision")
                })?;
                if call.policy.is_some() || call.result_is_error.is_some() {
                    return Err(completion_validation_error(
                        "completion PolicyDecision is duplicate or follows its ToolResult",
                    ));
                }
                call.policy = Some(match decision {
                    PolicyDecision::Allow => RecordedPolicy::Allow,
                    PolicyDecision::Deny { .. } => RecordedPolicy::Deny,
                    PolicyDecision::Ask { reason, risk } => RecordedPolicy::Ask {
                        reason: reason.clone(),
                        risk: *risk,
                    },
                });
            }
            ItemKind::ApprovalRequested {
                approval_id,
                call_id,
                tool,
                reason,
                risk,
                ..
            } => {
                let call = calls.get_mut(call_id).ok_or_else(|| {
                    completion_validation_error(
                        "completion Turn contains an orphan ApprovalRequested",
                    )
                })?;
                let policy_matches = matches!(
                    &call.policy,
                    Some(RecordedPolicy::Ask {
                        reason: policy_reason,
                        risk: policy_risk,
                    }) if policy_reason == reason && policy_risk == risk
                );
                if !policy_matches
                    || &call.tool != tool
                    || call.approval_id.is_some()
                    || call.result_is_error.is_some()
                {
                    return Err(completion_validation_error(
                        "completion ApprovalRequested does not uniquely match its Ask decision",
                    ));
                }
                if approvals
                    .insert(approval_id.clone(), call_id.clone())
                    .is_some()
                {
                    return Err(completion_validation_error(
                        "completion Turn contains a duplicate approval identity",
                    ));
                }
                call.approval_id = Some(approval_id.clone());
            }
            ItemKind::ApprovalDecision {
                approval_id,
                call_id,
                decision,
            } => {
                if approvals.get(approval_id) != Some(call_id) {
                    return Err(completion_validation_error(
                        "completion Turn contains an orphan or mismatched ApprovalDecision",
                    ));
                }
                let call = calls.get_mut(call_id).ok_or_else(|| {
                    completion_validation_error(
                        "completion ApprovalDecision references an unknown ToolCall",
                    )
                })?;
                if call.approval_id.as_ref() != Some(approval_id)
                    || call.approval_approved
                    || call.result_is_error.is_some()
                {
                    return Err(completion_validation_error(
                        "completion Turn contains duplicate ApprovalDecision evidence",
                    ));
                }
                match decision {
                    ApprovalDecision::Approve => call.approval_approved = true,
                    ApprovalDecision::Deny { .. } => {
                        return Err(completion_validation_error(
                            "a denied approval cannot produce successful Turn completion",
                        ));
                    }
                }
            }
            ItemKind::ToolResult {
                call_id, is_error, ..
            } => {
                let call = calls.get_mut(call_id).ok_or_else(|| {
                    completion_validation_error("completion Turn contains an orphan ToolResult")
                })?;
                if call.result_is_error.replace(*is_error).is_some() {
                    return Err(completion_validation_error(
                        "completion Turn contains duplicate ToolResult evidence",
                    ));
                }
                match &call.policy {
                    Some(RecordedPolicy::Allow) if call.approval_id.is_none() => {}
                    Some(RecordedPolicy::Ask { .. })
                        if call.approval_id.is_some() && call.approval_approved => {}
                    None if *is_error && call.approval_id.is_none() => {}
                    Some(RecordedPolicy::Deny) => {
                        return Err(completion_validation_error(
                            "a denied Policy decision cannot precede a completing ToolResult",
                        ));
                    }
                    _ => {
                        return Err(completion_validation_error(
                            "ToolResult was recorded before its required Policy or approval settlement",
                        ));
                    }
                }
            }
            _ => {}
        }
    }

    for call in calls.values() {
        let Some(result_is_error) = call.result_is_error else {
            return Err(completion_validation_error(
                "every completion ToolCall requires exactly one ToolResult",
            ));
        };
        match &call.policy {
            Some(RecordedPolicy::Allow) if call.approval_id.is_none() => {}
            Some(RecordedPolicy::Allow) => {
                return Err(completion_validation_error(
                    "an Allow decision cannot carry approval evidence",
                ));
            }
            Some(RecordedPolicy::Ask { .. })
                if call.approval_id.is_some() && call.approval_approved => {}
            Some(RecordedPolicy::Ask { .. }) => {
                return Err(completion_validation_error(
                    "an Ask decision requires one matching approved settlement",
                ));
            }
            Some(RecordedPolicy::Deny) => {
                return Err(completion_validation_error(
                    "a denied Policy decision cannot produce successful Turn completion",
                ));
            }
            None if result_is_error && call.approval_id.is_none() => {
                // A Runtime may settle a superseded pre-effect call without
                // consulting Policy. It must remain an explicit error result.
            }
            None => {
                return Err(completion_validation_error(
                    "a successful ToolResult requires one unique PolicyDecision",
                ));
            }
        }
    }
    Ok(())
}

fn validate_verifier_manifest(bindings: &[CompletionVerifierBinding]) -> Result<(), HarnessError> {
    let mut previous_name: Option<&str> = None;
    for binding in bindings {
        validate_capability_name("completion verifier", &binding.name)?;
        validate_capability_origin(&binding.origin)?;
        require_sha256("completion verifier binding", &binding.binding_sha256)?;
        if previous_name.is_some_and(|previous| previous >= binding.name.as_str()) {
            return Err(completion_configuration_error(
                "completion verifier manifest must be strictly ordered by name",
            ));
        }
        previous_name = Some(&binding.name);
    }
    Ok(())
}

fn require_sha256(kind: &str, value: &str) -> Result<(), HarnessError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(completion_configuration_error(format!(
            "{kind} must be lowercase SHA-256"
        )))
    }
}

fn require_portable_id(kind: &str, value: &str) -> Result<(), HarnessError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':' | b'+')
        });
    if valid {
        Ok(())
    } else {
        Err(completion_configuration_error(format!(
            "{kind} must be 1-128 portable ASCII bytes"
        )))
    }
}

fn canonical_json_sha256<T: Serialize + ?Sized>(
    domain: &str,
    value: &T,
    maximum_bytes: usize,
) -> Result<String, HarnessError> {
    let encoded = to_bounded_json_vec(&SerializableReference(value), maximum_bytes)
        .map_err(|error| bounded_error("completion hash input", maximum_bytes, error))?;
    let mut canonical: Value = serde_json::from_slice(&encoded).map_err(|_| {
        completion_configuration_error("completion hash input cannot be represented as JSON")
    })?;
    validate_value_shape(&canonical)
        .map_err(|error| bounded_error("completion hash input", maximum_bytes, error))?;
    canonicalize_json(&mut canonical);
    let canonical = to_bounded_json_vec(&canonical, maximum_bytes)
        .map_err(|error| bounded_error("completion hash input", maximum_bytes, error))?;

    let mut hasher = Sha256::new();
    hasher.update(HASH_PREFIX);
    hasher.update(
        u32::try_from(domain.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    hasher.update(domain.as_bytes());
    hasher.update(
        u64::try_from(canonical.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(canonical);
    Ok(hex_lower(&hasher.finalize()))
}

fn canonicalize_json(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(canonicalize_json),
        Value::Object(object) => {
            let mut entries = std::mem::take(object).into_iter().collect::<Vec<_>>();
            for (_, value) in &mut entries {
                canonicalize_json(value);
            }
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            object.extend(entries);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn bounded_error(kind: &str, maximum: usize, error: BoundedJsonError) -> HarnessError {
    completion_configuration_error(match error {
        BoundedJsonError::LimitExceeded => format!("{kind} exceeds {maximum} bytes or JSON bounds"),
        BoundedJsonError::CannotEncode => format!("cannot encode {kind}"),
    })
}

fn completion_configuration_error(message: impl Into<String>) -> HarnessError {
    HarnessError::InvalidConfiguration(message.into())
}

fn completion_validation_error(message: impl Into<String>) -> HarnessError {
    HarnessError::State(message.into())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CompletionAssurance, CompletionContract, CompletionGeneration, build_completion_receipt,
        completion_model_request_sha256, completion_model_route_sha256, completion_receipt_sha256,
        completion_runtime_governance_sha256, completion_tool_view_sha256,
        completion_verifier_manifest_sha256, validate_inherited_projected_turn_completion_receipt,
        validate_projected_turn_completion_receipt, validate_turn_completion_receipt,
    };
    use crate::{
        ActorIdentity, ApprovalId, AuthorityContext, CapabilityOrigin, Item, ItemKind,
        PolicyDecision, RiskLevel, ThreadId, Turn, TurnId, TurnStatus, VerificationOutcome,
    };

    fn request_sha256() -> String {
        completion_model_request_sha256(&json!({"input": "candidate"})).expect("request digest")
    }

    fn runtime_generation(model_request_sha256: &str) -> CompletionGeneration {
        CompletionGeneration::new(
            model_request_sha256,
            completion_model_route_sha256(&[("model/test", "built_in")]).expect("route digest"),
            completion_tool_view_sha256(&Vec::<String>::new()).expect("Tool view digest"),
            completion_verifier_manifest_sha256(&[]).expect("verifier manifest digest"),
            completion_runtime_governance_sha256(&json!({"max_steps": 8}))
                .expect("governance digest"),
            None,
            CompletionAssurance::RuntimeMeasured,
        )
        .expect("generation")
    }

    fn candidate(model_request_sha256: &str) -> Item {
        Item::new(ItemKind::AssistantMessage {
            model_id: Some("model/test".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(model_request_sha256.to_owned()),
            content: "settled answer".to_owned(),
        })
    }

    fn append_candidate(turn: &mut Turn, model_request_sha256: &str) -> Item {
        let candidate = candidate(model_request_sha256);
        turn.items.push(candidate.clone());
        candidate
    }

    #[test]
    fn receipt_is_deterministic_and_turn_tampering_is_rejected() {
        let authority = AuthorityContext::local_process();
        let request_sha256 = request_sha256();
        let generation = runtime_generation(&request_sha256);
        let mut turn = Turn::new(ThreadId::from_static("thread-completion"));
        let candidate = append_candidate(&mut turn, &request_sha256);

        let first = build_completion_receipt(
            &turn,
            &authority,
            &candidate.id,
            generation.clone(),
            CompletionContract::v1_no_external_requirements(),
        )
        .expect("first receipt");
        let second = build_completion_receipt(
            &turn,
            &authority,
            &candidate.id,
            generation,
            CompletionContract::v1_no_external_requirements(),
        )
        .expect("second receipt");
        assert_eq!(first, second);
        assert_eq!(first.source_thread_id(), &turn.thread_id);
        assert_eq!(first.turn_id(), &turn.id);
        assert_eq!(
            completion_receipt_sha256(&first).expect("first digest"),
            completion_receipt_sha256(&second).expect("second digest")
        );

        let mut wrong_source = first.clone();
        wrong_source.source_thread_id = ThreadId::from_static("thread-other");
        assert!(validate_turn_completion_receipt(&turn, None, &wrong_source).is_err());
        let mut wrong_turn = first.clone();
        wrong_turn.turn_id = TurnId::from_static("turn-other");
        assert!(validate_turn_completion_receipt(&turn, None, &wrong_turn).is_err());
        let mut malformed_source = first.clone();
        malformed_source.source_thread_id = ThreadId::from_string(String::new());
        assert!(completion_receipt_sha256(&malformed_source).is_err());

        let mut inherited_turn = turn.clone();
        inherited_turn.thread_id = ThreadId::from_static("thread-child");
        inherited_turn.status = TurnStatus::Completed;
        inherited_turn.completion_receipt = Some(first.clone());
        assert!(validate_projected_turn_completion_receipt(&inherited_turn, None, &first).is_err());
        validate_inherited_projected_turn_completion_receipt(&inherited_turn, None, &first)
            .expect("lineage-authorized inherited placement");

        turn.items.last_mut().expect("candidate").kind = ItemKind::AssistantMessage {
            model_id: Some("model/test".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(request_sha256),
            content: "tampered answer".to_owned(),
        };
        assert!(validate_turn_completion_receipt(&turn, None, &first).is_err());
    }

    #[test]
    fn result_before_policy_cannot_be_rewritten_into_valid_completion() {
        let authority = AuthorityContext::local_process();
        let request_sha256 = request_sha256();
        let mut turn = Turn::new(ThreadId::from_static("thread-policy-order"));
        turn.items.push(Item::new(ItemKind::ToolCall {
            model_id: Some("model/test".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            call_id: "call-1".to_owned(),
            name: "read".to_owned(),
            input: json!({}),
            batch: None,
        }));
        turn.items.push(Item::new(ItemKind::ToolResult {
            call_id: "call-1".to_owned(),
            output: json!({"ok": true}),
            is_error: false,
            connector_evidence: Vec::new(),
        }));
        turn.items.push(Item::new(ItemKind::PolicyDecision {
            call_id: "call-1".to_owned(),
            tool_origin: Some(CapabilityOrigin::BuiltIn),
            decision: PolicyDecision::Allow,
        }));
        let candidate = append_candidate(&mut turn, &request_sha256);

        assert!(
            build_completion_receipt(
                &turn,
                &authority,
                &candidate.id,
                runtime_generation(&request_sha256),
                CompletionContract::v1_no_external_requirements(),
            )
            .is_err()
        );
    }

    #[test]
    fn ask_requires_approved_settlement_before_tool_result() {
        let authority = AuthorityContext::local_process();
        let request_sha256 = request_sha256();
        let mut turn = Turn::new(ThreadId::from_static("thread-approval-order"));
        turn.items.push(Item::new(ItemKind::ToolCall {
            model_id: Some("model/test".to_owned()),
            model_origin: Some(CapabilityOrigin::BuiltIn),
            call_id: "call-ask".to_owned(),
            name: "write".to_owned(),
            input: json!({}),
            batch: None,
        }));
        turn.items.push(Item::new(ItemKind::PolicyDecision {
            call_id: "call-ask".to_owned(),
            tool_origin: Some(CapabilityOrigin::BuiltIn),
            decision: PolicyDecision::Ask {
                reason: "write access".to_owned(),
                risk: RiskLevel::High,
            },
        }));
        turn.items.push(Item::new(ItemKind::ApprovalRequested {
            approval_id: ApprovalId::from_static("approval-1"),
            call_id: "call-ask".to_owned(),
            tool: "write".to_owned(),
            reason: "write access".to_owned(),
            risk: RiskLevel::High,
            requested_by: Some(ActorIdentity::LocalProcess),
            tool_origin: Some(CapabilityOrigin::BuiltIn),
            model_request_sha256: Some(request_sha256.clone()),
        }));
        turn.items.push(Item::new(ItemKind::ToolResult {
            call_id: "call-ask".to_owned(),
            output: json!({"ok": true}),
            is_error: false,
            connector_evidence: Vec::new(),
        }));
        let candidate = append_candidate(&mut turn, &request_sha256);

        assert!(
            build_completion_receipt(
                &turn,
                &authority,
                &candidate.id,
                runtime_generation(&request_sha256),
                CompletionContract::v1_no_external_requirements(),
            )
            .is_err()
        );
    }

    #[test]
    fn failed_verifier_cannot_be_part_of_a_completion_suffix() {
        let authority = AuthorityContext::local_process();
        let request_sha256 = request_sha256();
        let mut turn = Turn::new(ThreadId::from_static("thread-verifier"));
        let candidate = append_candidate(&mut turn, &request_sha256);
        turn.items.push(Item::new(ItemKind::VerificationResult {
            verifier: "required-output".to_owned(),
            candidate_item_id: Some(candidate.id.clone()),
            verifier_origin: Some(CapabilityOrigin::BuiltIn),
            verifier_binding_sha256: Some("a".repeat(64)),
            outcome: VerificationOutcome::Failed {
                reason: "missing proof".to_owned(),
                retryable: false,
            },
        }));

        assert!(
            build_completion_receipt(
                &turn,
                &authority,
                &candidate.id,
                runtime_generation(&request_sha256),
                CompletionContract::v1_no_external_requirements(),
            )
            .is_err()
        );
    }
}
