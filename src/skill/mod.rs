//! Digest-pinned Skill packages, exact dependency resolution, and context loading.

#[cfg(feature = "https-skill")]
mod https_source;

#[cfg(feature = "https-skill")]
pub use https_source::{
    HttpSkillRequest, HttpSkillResponse, HttpSkillTransport, HttpsSkillSource,
    HttpsSkillSourceConfig, ReqwestHttpSkillTransport,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use ed25519_dalek::{Signature, VerifyingKey};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CapabilityOrigin, ContextBlock, ContextSource, HarnessError, ToolRegistry,
    kernel::{validate_capability_name, validate_capability_origin, validate_registry_growth},
};

/// Current declarative Skill package contract version.
pub const SKILL_API_VERSION: &str = "1";

const MAX_DESCRIPTION_BYTES: usize = 4_096;
const MAX_INSTRUCTIONS_BYTES: usize = 1_048_576;
const MAX_RESOURCE_BYTES: usize = 1_048_576;
const MAX_RESOURCES: usize = 256;
const MAX_SKILL_DEPENDENCIES: usize = 256;
const MAX_SKILL_REQUIRED_TOOLS: usize = 256;
const MAX_SKILL_PACKAGE_CONTENT_BYTES: usize = 2_097_152;
const MAX_SKILL_CANONICAL_BYTES: usize = 16_777_216;
const MAX_SKILL_REGISTRY_CONTENT_BYTES: usize = 67_108_864;
const MAX_ESTIMATED_TOKENS: usize = 262_144;
const MAX_SKILL_TRUST_ANCHORS: usize = 4_096;
const MAX_TRANSPARENCY_ENTRY_ID_BYTES: usize = 256;
const MAX_TRANSPARENCY_CLOCK_SKEW_MS: u64 = 300_000;
const SKILL_SIGNATURE_DOMAIN: &[u8] = b"y-harness-skill-v1\0";
const SKILL_TRANSPARENCY_DOMAIN: &[u8] = b"y-harness-skill-transparency-v1\0";

/// Exact Skill identity used for selection and dependency pins.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct SkillId {
    /// Stable registry name.
    pub name: String,
    /// Exact semantic version.
    pub version: Version,
}

/// One exact dependency declared by a Skill package.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkillDependency {
    /// Required Skill name.
    pub name: String,
    /// Required exact version.
    pub version: Version,
}

impl From<&SkillDependency> for SkillId {
    fn from(dependency: &SkillDependency) -> Self {
        Self {
            name: dependency.name.clone(),
            version: dependency.version.clone(),
        }
    }
}

/// Declarative metadata covered by the package content digest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkillManifest {
    /// Skill contract version.
    pub api_version: String,
    /// Stable Skill name.
    pub name: String,
    /// Exact package version.
    pub version: Version,
    /// Human-readable capability summary.
    pub description: String,
    /// Provider-estimated instruction token cost.
    pub estimated_tokens: usize,
    /// Exact Skill dependencies in deterministic sorted order.
    pub dependencies: Vec<SkillDependency>,
    /// Tool names that must already be registered.
    pub required_tools: BTreeSet<String>,
}

/// Immutable Skill package whose complete content is digest pinned.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkillPackage {
    /// Declarative manifest.
    pub manifest: SkillManifest,
    /// Model-facing instructions loaded as one reversible context block.
    pub instructions: String,
    /// Optional named resources loaded explicitly rather than automatically.
    pub resources: BTreeMap<String, String>,
    /// Lowercase SHA-256 digest of manifest, instructions, and resources.
    pub content_sha256: String,
}

/// Detached publisher signature over one digest-verified Skill package.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SkillSignature {
    /// Operator-configured trusted publisher identity.
    pub key_id: String,
    /// Raw 64-byte Ed25519 signature.
    pub ed25519: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Publisher policy for signed transparency receipts.
pub enum SkillTransparencyRequirement {
    /// A missing receipt is accepted; any supplied receipt is still verified.
    #[default]
    Optional,
    /// Every package signed by this publisher must carry a valid receipt.
    Required,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
/// Time and transparency policy for one trusted publisher key.
pub struct SkillPublisherPolicy {
    /// First Unix millisecond at which signatures are accepted.
    pub not_before_ms: Option<u64>,
    /// Exclusive Unix millisecond after which signatures are rejected.
    pub not_after_ms: Option<u64>,
    /// Whether a trusted-log receipt is mandatory.
    pub transparency: SkillTransparencyRequirement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Signed transparency receipt binding one publisher signature to one package.
pub struct SkillTransparencyReceipt {
    /// Operator-configured transparency-log identity.
    pub log_id: String,
    /// Opaque unique entry identity assigned by the log.
    pub entry_id: String,
    /// Unix millisecond at which the log integrated the entry.
    pub integrated_at_ms: u64,
    /// Raw 64-byte Ed25519 signature by the configured log key.
    pub ed25519: Vec<u8>,
}

/// Skill package and its detached publisher signature.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignedSkillPackage {
    /// Digest-pinned declarative package.
    pub package: SkillPackage,
    /// Detached authenticity proof.
    pub signature: SkillSignature,
    /// Optional signed transparency receipt, required by publisher policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transparency: Option<SkillTransparencyReceipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
/// Content-free immutable key-revocation record.
pub struct SkillKeyRevocation {
    /// Unix millisecond at which the key stops being accepted.
    pub revoked_at_ms: u64,
    /// Stable operator reason code.
    pub reason_code: String,
}

#[derive(Clone)]
struct TrustedPublisher {
    key: VerifyingKey,
    policy: SkillPublisherPolicy,
    revocation: Option<SkillKeyRevocation>,
}

#[derive(Clone)]
struct TrustedLog {
    key: VerifyingKey,
    revocation: Option<SkillKeyRevocation>,
}

#[derive(Default)]
struct SkillTrustState {
    publishers: BTreeMap<String, TrustedPublisher>,
    logs: BTreeMap<String, TrustedLog>,
}

/// Explicit, live operator trust policy for external Skill publishers.
#[derive(Clone, Default)]
pub struct SkillTrustStore {
    state: Arc<RwLock<SkillTrustState>>,
}

impl SkillTrustStore {
    /// Creates an empty trust store; no publisher is trusted by default.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one raw Ed25519 public key without allowing replacement.
    pub fn trust(
        &self,
        key_id: impl Into<String>,
        public_key: [u8; 32],
    ) -> Result<(), HarnessError> {
        self.trust_with_policy(key_id, public_key, SkillPublisherPolicy::default())
    }

    /// Registers one publisher key with explicit time and transparency policy.
    pub fn trust_with_policy(
        &self,
        key_id: impl Into<String>,
        public_key: [u8; 32],
        policy: SkillPublisherPolicy,
    ) -> Result<(), HarnessError> {
        let key_id = key_id.into();
        validate_capability_name("publisher key", &key_id)?;
        validate_publisher_policy(policy)?;
        let key = validate_verifying_key("publisher key", &key_id, public_key)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        if state.publishers.contains_key(&key_id) {
            return Err(HarnessError::DuplicateCapability(format!(
                "publisher key {key_id}"
            )));
        }
        if state.publishers.len() >= MAX_SKILL_TRUST_ANCHORS {
            return Err(HarnessError::Skill(format!(
                "publisher trust store exceeds {MAX_SKILL_TRUST_ANCHORS} keys"
            )));
        }
        state.publishers.insert(
            key_id,
            TrustedPublisher {
                key,
                policy,
                revocation: None,
            },
        );
        Ok(())
    }

    /// Registers one transparency-log verification key without replacement.
    pub fn trust_transparency_log(
        &self,
        log_id: impl Into<String>,
        public_key: [u8; 32],
    ) -> Result<(), HarnessError> {
        let log_id = log_id.into();
        validate_capability_name("transparency log", &log_id)?;
        let key = validate_verifying_key("transparency log", &log_id, public_key)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        if state.logs.contains_key(&log_id) {
            return Err(HarnessError::DuplicateCapability(format!(
                "transparency log {log_id}"
            )));
        }
        if state.logs.len() >= MAX_SKILL_TRUST_ANCHORS {
            return Err(HarnessError::Skill(format!(
                "transparency-log trust store exceeds {MAX_SKILL_TRUST_ANCHORS} keys"
            )));
        }
        state.logs.insert(
            log_id,
            TrustedLog {
                key,
                revocation: None,
            },
        );
        Ok(())
    }

    /// Irreversibly records an effective publisher-key revocation.
    pub fn revoke_publisher(
        &self,
        key_id: &str,
        revoked_at_ms: u64,
        reason_code: impl Into<String>,
    ) -> Result<(), HarnessError> {
        validate_capability_name("publisher key", key_id)?;
        let reason_code = reason_code.into();
        let revocation = validate_revocation(revoked_at_ms, reason_code)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        let publisher = state
            .publishers
            .get_mut(key_id)
            .ok_or_else(|| HarnessError::Skill(format!("publisher key {key_id} is not trusted")))?;
        install_revocation(
            &mut publisher.revocation,
            revocation,
            "publisher key",
            key_id,
        )
    }

    /// Irreversibly records an effective transparency-log key revocation.
    pub fn revoke_transparency_log(
        &self,
        log_id: &str,
        revoked_at_ms: u64,
        reason_code: impl Into<String>,
    ) -> Result<(), HarnessError> {
        validate_capability_name("transparency log", log_id)?;
        let reason_code = reason_code.into();
        let revocation = validate_revocation(revoked_at_ms, reason_code)?;
        let mut state = self
            .state
            .write()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        let log = state.logs.get_mut(log_id).ok_or_else(|| {
            HarnessError::Skill(format!("transparency log {log_id} is not trusted"))
        })?;
        install_revocation(&mut log.revocation, revocation, "transparency log", log_id)
    }

    /// Returns the immutable publisher revocation record when present.
    pub fn publisher_revocation(
        &self,
        key_id: &str,
    ) -> Result<Option<SkillKeyRevocation>, HarnessError> {
        validate_capability_name("publisher key", key_id)?;
        let state = self
            .state
            .read()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        let publisher = state
            .publishers
            .get(key_id)
            .ok_or_else(|| HarnessError::Skill(format!("publisher key {key_id} is not trusted")))?;
        Ok(publisher.revocation.clone())
    }

    /// Returns the immutable transparency-log revocation record when present.
    pub fn transparency_log_revocation(
        &self,
        log_id: &str,
    ) -> Result<Option<SkillKeyRevocation>, HarnessError> {
        validate_capability_name("transparency log", log_id)?;
        let state = self
            .state
            .read()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        let log = state.logs.get(log_id).ok_or_else(|| {
            HarnessError::Skill(format!("transparency log {log_id} is not trusted"))
        })?;
        Ok(log.revocation.clone())
    }

    fn verify(
        &self,
        signed: &SignedSkillPackage,
        observed_at_ms: u64,
    ) -> Result<VerifiedSkillTrust, HarnessError> {
        validate_package(&signed.package)?;
        validate_capability_name("publisher key", &signed.signature.key_id)?;
        let (publisher, transparency_log) = {
            let state = self
                .state
                .read()
                .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
            let publisher = state
                .publishers
                .get(&signed.signature.key_id)
                .cloned()
                .ok_or_else(|| {
                    HarnessError::Skill(format!(
                        "publisher key {} is not trusted",
                        signed.signature.key_id
                    ))
                })?;
            let transparency_log = signed
                .transparency
                .as_ref()
                .and_then(|receipt| state.logs.get(&receipt.log_id))
                .cloned();
            (publisher, transparency_log)
        };
        validate_anchor_status(
            "publisher key",
            &signed.signature.key_id,
            publisher.policy.not_before_ms,
            publisher.policy.not_after_ms,
            publisher.revocation.as_ref(),
            observed_at_ms,
        )?;
        let signature_bytes: [u8; 64] =
            signed
                .signature
                .ed25519
                .as_slice()
                .try_into()
                .map_err(|_| {
                    HarnessError::Skill(
                        "Ed25519 Skill signature must be exactly 64 bytes".to_owned(),
                    )
                })?;
        let signature = Signature::from_bytes(&signature_bytes);
        publisher
            .key
            .verify_strict(&skill_signing_bytes(&signed.package), &signature)
            .map_err(|_| {
                HarnessError::Skill(format!(
                    "Skill {}@{} has an invalid publisher signature",
                    signed.package.manifest.name, signed.package.manifest.version
                ))
            })?;

        let transparency = match &signed.transparency {
            Some(receipt) => {
                validate_transparency_receipt(receipt, observed_at_ms)?;
                let log = transparency_log.ok_or_else(|| {
                    HarnessError::Skill(format!(
                        "transparency log {} is not trusted",
                        receipt.log_id
                    ))
                })?;
                validate_anchor_status(
                    "transparency log",
                    &receipt.log_id,
                    None,
                    None,
                    log.revocation.as_ref(),
                    observed_at_ms,
                )?;
                let receipt_signature: [u8; 64] =
                    receipt.ed25519.as_slice().try_into().map_err(|_| {
                        HarnessError::Skill(
                            "Ed25519 transparency signature must be exactly 64 bytes".to_owned(),
                        )
                    })?;
                log.key
                    .verify_strict(
                        &transparency_signing_bytes(signed)?,
                        &Signature::from_bytes(&receipt_signature),
                    )
                    .map_err(|_| {
                        HarnessError::Skill(format!(
                            "Skill {}@{} has an invalid transparency receipt",
                            signed.package.manifest.name, signed.package.manifest.version
                        ))
                    })?;
                Some(VerifiedSkillTransparency {
                    log_id: receipt.log_id.clone(),
                    entry_id: receipt.entry_id.clone(),
                    integrated_at_ms: receipt.integrated_at_ms,
                })
            }
            None if publisher.policy.transparency == SkillTransparencyRequirement::Required => {
                return Err(HarnessError::Skill(format!(
                    "publisher key {} requires a transparency receipt",
                    signed.signature.key_id
                )));
            }
            None => None,
        };

        Ok(VerifiedSkillTrust {
            publisher_key_id: signed.signature.key_id.clone(),
            transparency,
        })
    }

    fn revalidate_registered(
        &self,
        publisher_key_id: &str,
        transparency_log_id: Option<&str>,
        observed_at_ms: u64,
    ) -> Result<(), HarnessError> {
        let state = self
            .state
            .read()
            .map_err(|_| HarnessError::Skill("Skill trust-store lock poisoned".to_owned()))?;
        let publisher = state.publishers.get(publisher_key_id).ok_or_else(|| {
            HarnessError::Skill(format!("publisher key {publisher_key_id} is not trusted"))
        })?;
        validate_anchor_status(
            "publisher key",
            publisher_key_id,
            publisher.policy.not_before_ms,
            publisher.policy.not_after_ms,
            publisher.revocation.as_ref(),
            observed_at_ms,
        )?;
        if publisher.policy.transparency == SkillTransparencyRequirement::Required
            && transparency_log_id.is_none()
        {
            return Err(HarnessError::Skill(format!(
                "publisher key {publisher_key_id} requires a transparency receipt"
            )));
        }
        if let Some(log_id) = transparency_log_id {
            let log = state.logs.get(log_id).ok_or_else(|| {
                HarnessError::Skill(format!("transparency log {log_id} is not trusted"))
            })?;
            validate_anchor_status(
                "transparency log",
                log_id,
                None,
                None,
                log.revocation.as_ref(),
                observed_at_ms,
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Verified transparency evidence preserved with external Skill provenance.
pub struct VerifiedSkillTransparency {
    /// Trusted transparency-log identity.
    pub log_id: String,
    /// Opaque log entry identity.
    pub entry_id: String,
    /// Log integration time in Unix milliseconds.
    pub integrated_at_ms: u64,
}

struct VerifiedSkillTrust {
    publisher_key_id: String,
    transparency: Option<VerifiedSkillTransparency>,
}

impl SkillPackage {
    /// Creates a package and fills its content digest.
    pub fn seal(
        manifest: SkillManifest,
        instructions: String,
        resources: BTreeMap<String, String>,
    ) -> Result<Self, HarnessError> {
        let mut package = Self {
            manifest,
            instructions,
            resources,
            content_sha256: String::new(),
        };
        package.content_sha256 = package.computed_sha256()?;
        validate_package(&package)?;
        Ok(package)
    }

    /// Computes the canonical package digest without trusting the declared value.
    pub fn computed_sha256(&self) -> Result<String, HarnessError> {
        #[derive(Serialize)]
        struct DigestMaterial<'a> {
            manifest: &'a SkillManifest,
            instructions: &'a str,
            resources: &'a BTreeMap<String, String>,
        }

        let _ =
            validate_package_content_envelope(&self.manifest, &self.instructions, &self.resources)?;
        let bytes = serde_json::to_vec(&DigestMaterial {
            manifest: &self.manifest,
            instructions: &self.instructions,
            resources: &self.resources,
        })
        .map_err(|error| HarnessError::Skill(format!("cannot encode Skill digest: {error}")))?;
        if bytes.len() > MAX_SKILL_CANONICAL_BYTES {
            return Err(HarnessError::Skill(format!(
                "canonical Skill package exceeds {MAX_SKILL_CANONICAL_BYTES} bytes"
            )));
        }
        let digest = Sha256::digest(bytes);
        let mut encoded = String::with_capacity(64);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in digest {
            encoded.push(char::from(HEX[usize::from(byte >> 4)]));
            encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        Ok(encoded)
    }

    /// Returns domain-separated bytes for an external publisher to sign.
    pub fn publisher_signing_bytes(&self) -> Result<Vec<u8>, HarnessError> {
        validate_package(self)?;
        Ok(skill_signing_bytes(self))
    }

    fn id(&self) -> SkillId {
        SkillId {
            name: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
        }
    }
}

impl SignedSkillPackage {
    /// Returns canonical bytes for the selected transparency log to sign.
    ///
    /// The receipt must already contain its log, entry, and integration fields.
    /// Its `ed25519` field is deliberately excluded from these bytes.
    pub fn transparency_signing_bytes(&self) -> Result<Vec<u8>, HarnessError> {
        validate_package(&self.package)?;
        validate_capability_name("publisher key", &self.signature.key_id)?;
        let receipt = self.transparency.as_ref().ok_or_else(|| {
            HarnessError::Skill("transparency signing material has no receipt".to_owned())
        })?;
        validate_transparency_receipt_shape(receipt)?;
        transparency_signing_bytes(self)
    }
}

fn skill_signing_bytes(package: &SkillPackage) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(SKILL_SIGNATURE_DOMAIN.len() + package.content_sha256.len());
    bytes.extend_from_slice(SKILL_SIGNATURE_DOMAIN);
    bytes.extend_from_slice(package.content_sha256.as_bytes());
    bytes
}

fn transparency_signing_bytes(signed: &SignedSkillPackage) -> Result<Vec<u8>, HarnessError> {
    #[derive(Serialize)]
    struct TransparencyMaterial<'a> {
        log_id: &'a str,
        entry_id: &'a str,
        integrated_at_ms: u64,
        content_sha256: &'a str,
        publisher_key_id: &'a str,
        publisher_signature: &'a [u8],
    }

    let receipt = signed.transparency.as_ref().ok_or_else(|| {
        HarnessError::Skill("transparency signing material has no receipt".to_owned())
    })?;
    let encoded = serde_json::to_vec(&TransparencyMaterial {
        log_id: &receipt.log_id,
        entry_id: &receipt.entry_id,
        integrated_at_ms: receipt.integrated_at_ms,
        content_sha256: &signed.package.content_sha256,
        publisher_key_id: &signed.signature.key_id,
        publisher_signature: &signed.signature.ed25519,
    })
    .map_err(|error| HarnessError::Skill(format!("cannot encode transparency receipt: {error}")))?;
    let mut bytes = Vec::with_capacity(SKILL_TRANSPARENCY_DOMAIN.len() + encoded.len());
    bytes.extend_from_slice(SKILL_TRANSPARENCY_DOMAIN);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
}

fn validate_publisher_policy(policy: SkillPublisherPolicy) -> Result<(), HarnessError> {
    if policy.not_after_ms == Some(0)
        || matches!(
            (policy.not_before_ms, policy.not_after_ms),
            (Some(not_before), Some(not_after)) if not_before >= not_after
        )
    {
        return Err(HarnessError::Skill(
            "publisher key validity window is empty or reversed".to_owned(),
        ));
    }
    Ok(())
}

fn validate_verifying_key(
    kind: &str,
    id: &str,
    public_key: [u8; 32],
) -> Result<VerifyingKey, HarnessError> {
    let key = VerifyingKey::from_bytes(&public_key)
        .map_err(|_| HarnessError::Skill(format!("{kind} {id} is invalid")))?;
    if key.is_weak() {
        return Err(HarnessError::Skill(format!(
            "{kind} {id} is cryptographically weak"
        )));
    }
    Ok(key)
}

fn validate_revocation(
    revoked_at_ms: u64,
    reason_code: String,
) -> Result<SkillKeyRevocation, HarnessError> {
    if revoked_at_ms == 0 {
        return Err(HarnessError::Skill(
            "Skill key revocation time must be non-zero".to_owned(),
        ));
    }
    validate_capability_name("Skill key revocation reason", &reason_code)?;
    Ok(SkillKeyRevocation {
        revoked_at_ms,
        reason_code,
    })
}

fn install_revocation(
    current: &mut Option<SkillKeyRevocation>,
    revocation: SkillKeyRevocation,
    kind: &str,
    id: &str,
) -> Result<(), HarnessError> {
    match current {
        Some(existing) if existing == &revocation => Ok(()),
        Some(_) => Err(HarnessError::Skill(format!(
            "{kind} {id} already has a different immutable revocation"
        ))),
        None => {
            *current = Some(revocation);
            Ok(())
        }
    }
}

fn validate_anchor_status(
    kind: &str,
    id: &str,
    not_before_ms: Option<u64>,
    not_after_ms: Option<u64>,
    revocation: Option<&SkillKeyRevocation>,
    observed_at_ms: u64,
) -> Result<(), HarnessError> {
    if not_before_ms.is_some_and(|not_before| observed_at_ms < not_before) {
        return Err(HarnessError::Skill(format!("{kind} {id} is not valid yet")));
    }
    if not_after_ms.is_some_and(|not_after| observed_at_ms >= not_after) {
        return Err(HarnessError::Skill(format!("{kind} {id} has expired")));
    }
    if revocation.is_some_and(|revocation| observed_at_ms >= revocation.revoked_at_ms) {
        return Err(HarnessError::Skill(format!("{kind} {id} is revoked")));
    }
    Ok(())
}

fn validate_transparency_receipt(
    receipt: &SkillTransparencyReceipt,
    observed_at_ms: u64,
) -> Result<(), HarnessError> {
    validate_transparency_receipt_shape(receipt)?;
    if receipt.integrated_at_ms > observed_at_ms.saturating_add(MAX_TRANSPARENCY_CLOCK_SKEW_MS) {
        return Err(HarnessError::Skill(
            "transparency integration time is too far in the future".to_owned(),
        ));
    }
    Ok(())
}

fn validate_transparency_receipt_shape(
    receipt: &SkillTransparencyReceipt,
) -> Result<(), HarnessError> {
    validate_capability_name("transparency log", &receipt.log_id)?;
    if receipt.entry_id.is_empty()
        || receipt.entry_id.len() > MAX_TRANSPARENCY_ENTRY_ID_BYTES
        || !receipt
            .entry_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(HarnessError::Skill(format!(
            "transparency entry id must be 1-{MAX_TRANSPARENCY_ENTRY_ID_BYTES} ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    if receipt.integrated_at_ms == 0 {
        return Err(HarnessError::Skill(
            "transparency integration time must be non-zero".to_owned(),
        ));
    }
    Ok(())
}

/// Registered package with its operator-assigned trust origin.
pub struct RegisteredSkill {
    /// Exact package identity.
    pub id: SkillId,
    /// Registration trust origin.
    pub origin: CapabilityOrigin,
    /// Verified immutable package.
    pub package: SkillPackage,
    /// Verified publisher trust root for an external package.
    pub publisher_key_id: Option<String>,
    /// Verified signed transparency evidence for an external package.
    pub transparency: Option<VerifiedSkillTransparency>,
    publisher_trust: Option<SkillTrustStore>,
}

#[derive(Default)]
/// Registry that validates Skill metadata, content integrity, and collisions.
pub struct SkillRegistry {
    skills: BTreeMap<SkillId, RegisteredSkill>,
    content_bytes: usize,
}

impl SkillRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Validates and registers one exact package version.
    pub fn register(
        &mut self,
        origin: CapabilityOrigin,
        package: SkillPackage,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("Skill", self.skills.len(), 1)?;
        if matches!(origin, CapabilityOrigin::External { .. }) {
            return Err(HarnessError::Skill(
                "external Skill packages require register_signed".to_owned(),
            ));
        }
        validate_package(&package)?;
        self.insert(origin, package, None, None, None)
    }

    /// Verifies and registers one externally sourced signed package.
    pub fn register_signed(
        &mut self,
        origin: CapabilityOrigin,
        signed: SignedSkillPackage,
        trust: &SkillTrustStore,
    ) -> Result<(), HarnessError> {
        self.register_signed_at(origin, signed, trust, crate::kernel::now_ms())
    }

    /// Verifies at one explicit time for deterministic import pipelines.
    pub fn register_signed_at(
        &mut self,
        origin: CapabilityOrigin,
        signed: SignedSkillPackage,
        trust: &SkillTrustStore,
        observed_at_ms: u64,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("Skill", self.skills.len(), 1)?;
        if !matches!(origin, CapabilityOrigin::External { .. }) {
            return Err(HarnessError::Skill(
                "signed external registration requires an external origin".to_owned(),
            ));
        }
        let verified = trust.verify(&signed, observed_at_ms)?;
        self.insert(
            origin,
            signed.package,
            Some(verified.publisher_key_id),
            verified.transparency,
            Some(trust.clone()),
        )
    }

    fn insert(
        &mut self,
        origin: CapabilityOrigin,
        package: SkillPackage,
        publisher_key_id: Option<String>,
        transparency: Option<VerifiedSkillTransparency>,
        publisher_trust: Option<SkillTrustStore>,
    ) -> Result<(), HarnessError> {
        validate_capability_origin(&origin)?;
        validate_registry_growth("Skill", self.skills.len(), 1)?;
        let id = package.id();
        if self.skills.contains_key(&id) {
            return Err(HarnessError::DuplicateCapability(format!(
                "skill {}@{}",
                id.name, id.version
            )));
        }
        let package_bytes = validate_package_content_envelope(
            &package.manifest,
            &package.instructions,
            &package.resources,
        )?;
        let next_content_bytes = self
            .content_bytes
            .checked_add(package_bytes)
            .ok_or_else(|| HarnessError::Skill("Skill registry byte count overflow".to_owned()))?;
        if next_content_bytes > MAX_SKILL_REGISTRY_CONTENT_BYTES {
            return Err(HarnessError::Skill(format!(
                "Skill registry content exceeds {MAX_SKILL_REGISTRY_CONTENT_BYTES} bytes"
            )));
        }
        self.skills.insert(
            id.clone(),
            RegisteredSkill {
                id,
                origin,
                package,
                publisher_key_id,
                transparency,
                publisher_trust,
            },
        );
        self.content_bytes = next_content_bytes;
        Ok(())
    }

    /// Looks up one exact Skill version.
    #[must_use]
    pub fn get(&self, id: &SkillId) -> Option<&RegisteredSkill> {
        self.skills.get(id)
    }

    /// Returns every registered identity in deterministic order.
    #[must_use]
    pub fn identities(&self) -> Vec<SkillId> {
        self.skills.keys().cloned().collect()
    }
}

/// One package selected by deterministic dependency resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedSkill {
    /// Exact selected identity.
    pub id: SkillId,
    /// Verified package digest.
    pub content_sha256: String,
    /// Operator-assigned trust origin.
    pub origin: CapabilityOrigin,
    /// Verified publisher key for external packages.
    pub publisher_key_id: Option<String>,
    /// Verified signed transparency evidence when supplied.
    pub transparency: Option<VerifiedSkillTransparency>,
}

/// Dependency-ordered Skills and their model-visible instruction blocks.
#[derive(Clone)]
pub struct ResolvedSkillSet {
    /// Dependencies before dependants, with duplicates removed.
    pub skills: Vec<ResolvedSkill>,
    /// Instructions in the same deterministic order.
    pub context: Vec<ContextBlock>,
    /// Total manifest-estimated token cost.
    pub estimated_tokens: usize,
    trust_checks: Vec<ResolvedSkillTrust>,
}

impl std::fmt::Debug for ResolvedSkillSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedSkillSet")
            .field("skills", &self.skills)
            .field("context", &self.context)
            .field("estimated_tokens", &self.estimated_tokens)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ResolvedSkillSet {
    fn eq(&self, other: &Self) -> bool {
        self.skills == other.skills
            && self.context == other.context
            && self.estimated_tokens == other.estimated_tokens
    }
}

impl Eq for ResolvedSkillSet {}

impl ResolvedSkillSet {
    pub(crate) fn into_context_and_trust(self) -> (Vec<ContextBlock>, Vec<ResolvedSkillTrust>) {
        (self.context, self.trust_checks)
    }
}

#[derive(Clone)]
pub(crate) struct ResolvedSkillTrust {
    trust: SkillTrustStore,
    publisher_key_id: String,
    transparency_log_id: Option<String>,
}

impl ResolvedSkillTrust {
    pub(crate) fn validate(&self, observed_at_ms: u64) -> Result<(), HarnessError> {
        self.trust.revalidate_registered(
            &self.publisher_key_id,
            self.transparency_log_id.as_deref(),
            observed_at_ms,
        )
    }
}

/// Resolves exact Skill graphs without executing package code.
pub struct SkillEngine {
    registry: SkillRegistry,
}

impl SkillEngine {
    /// Creates an engine over a validated package registry.
    #[must_use]
    pub fn new(registry: SkillRegistry) -> Self {
        Self { registry }
    }

    /// Resolves an exact dependency graph and checks tools and token budget.
    pub fn resolve(
        &self,
        requested: &[SkillId],
        tools: &ToolRegistry,
        budget_tokens: usize,
    ) -> Result<ResolvedSkillSet, HarnessError> {
        validate_registry_growth("requested Skill", 0, requested.len())?;
        let observed_at_ms = crate::kernel::now_ms();
        let mut roots = requested.to_vec();
        roots.sort();
        roots.dedup();
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut ordered = Vec::new();
        for root in roots {
            visit(
                &self.registry,
                &root,
                tools,
                &mut visiting,
                &mut visited,
                &mut ordered,
                observed_at_ms,
            )?;
        }

        let mut estimated_tokens = 0usize;
        let mut skills = Vec::with_capacity(ordered.len());
        let mut context = Vec::with_capacity(ordered.len());
        let mut trust_checks = Vec::new();
        for id in ordered {
            let registered = self.registry.get(&id).ok_or_else(|| {
                HarnessError::Skill(format!(
                    "resolved Skill {}@{} disappeared",
                    id.name, id.version
                ))
            })?;
            estimated_tokens = estimated_tokens
                .checked_add(registered.package.manifest.estimated_tokens)
                .ok_or_else(|| HarnessError::Skill("Skill token budget overflow".to_owned()))?;
            if estimated_tokens > budget_tokens {
                return Err(HarnessError::Skill(format!(
                    "resolved Skills require {estimated_tokens} tokens, budget is {budget_tokens}"
                )));
            }
            skills.push(ResolvedSkill {
                id: id.clone(),
                content_sha256: registered.package.content_sha256.clone(),
                origin: registered.origin.clone(),
                publisher_key_id: registered.publisher_key_id.clone(),
                transparency: registered.transparency.clone(),
            });
            if let (Some(trust), Some(publisher_key_id)) = (
                registered.publisher_trust.clone(),
                registered.publisher_key_id.clone(),
            ) {
                trust_checks.push(ResolvedSkillTrust {
                    trust,
                    publisher_key_id,
                    transparency_log_id: registered
                        .transparency
                        .as_ref()
                        .map(|receipt| receipt.log_id.clone()),
                });
            }
            context.push(ContextBlock {
                source: ContextSource::Skill {
                    name: id.name,
                    version: id.version.to_string(),
                    content_sha256: registered.package.content_sha256.clone(),
                },
                text: registered.package.instructions.clone(),
                estimated_tokens: registered.package.manifest.estimated_tokens,
            });
        }
        Ok(ResolvedSkillSet {
            skills,
            context,
            estimated_tokens,
            trust_checks,
        })
    }

    /// Reads one digest-verified resource without loading every resource.
    pub fn read_resource<'a>(
        &'a self,
        skill: &SkillId,
        path: &str,
    ) -> Result<&'a str, HarnessError> {
        validate_resource_path(path)?;
        let registered = self.registry.get(skill).ok_or_else(|| {
            HarnessError::Skill(format!(
                "Skill {}@{} is not registered",
                skill.name, skill.version
            ))
        })?;
        validate_registered_skill_trust(registered, crate::kernel::now_ms())?;
        registered
            .package
            .resources
            .get(path)
            .map(String::as_str)
            .ok_or_else(|| {
                HarnessError::Skill(format!(
                    "Skill {}@{} has no resource {path:?}",
                    skill.name, skill.version
                ))
            })
    }
}

fn visit(
    registry: &SkillRegistry,
    id: &SkillId,
    tools: &ToolRegistry,
    visiting: &mut BTreeSet<SkillId>,
    visited: &mut BTreeSet<SkillId>,
    ordered: &mut Vec<SkillId>,
    observed_at_ms: u64,
) -> Result<(), HarnessError> {
    if visited.contains(id) {
        return Ok(());
    }
    if !visiting.insert(id.clone()) {
        return Err(HarnessError::Skill(format!(
            "Skill dependency cycle includes {}@{}",
            id.name, id.version
        )));
    }
    let registered = registry.get(id).ok_or_else(|| {
        HarnessError::Skill(format!(
            "Skill {}@{} is not registered",
            id.name, id.version
        ))
    })?;
    validate_registered_skill_trust(registered, observed_at_ms)?;
    for tool in &registered.package.manifest.required_tools {
        if tools.get(tool).is_none() {
            return Err(HarnessError::Skill(format!(
                "Skill {}@{} requires missing tool {tool}",
                id.name, id.version
            )));
        }
    }
    for dependency in &registered.package.manifest.dependencies {
        visit(
            registry,
            &SkillId::from(dependency),
            tools,
            visiting,
            visited,
            ordered,
            observed_at_ms,
        )?;
    }
    visiting.remove(id);
    visited.insert(id.clone());
    ordered.push(id.clone());
    Ok(())
}

fn validate_registered_skill_trust(
    registered: &RegisteredSkill,
    observed_at_ms: u64,
) -> Result<(), HarnessError> {
    match (
        &registered.origin,
        registered.publisher_key_id.as_deref(),
        registered.publisher_trust.as_ref(),
    ) {
        (CapabilityOrigin::External { .. }, Some(key_id), Some(trust)) => trust
            .revalidate_registered(
                key_id,
                registered
                    .transparency
                    .as_ref()
                    .map(|receipt| receipt.log_id.as_str()),
                observed_at_ms,
            ),
        (CapabilityOrigin::External { .. }, _, _) => Err(HarnessError::Skill(format!(
            "external Skill {}@{} has no live publisher trust",
            registered.id.name, registered.id.version
        ))),
        (_, None, None) => Ok(()),
        _ => Err(HarnessError::Skill(format!(
            "non-external Skill {}@{} has external trust metadata",
            registered.id.name, registered.id.version
        ))),
    }
}

fn validate_package(package: &SkillPackage) -> Result<(), HarnessError> {
    let manifest = &package.manifest;
    if manifest.api_version != SKILL_API_VERSION {
        return Err(HarnessError::Skill(format!(
            "Skill {} uses unsupported API version {}",
            manifest.name, manifest.api_version
        )));
    }
    validate_capability_name("skill", &manifest.name)?;
    validate_bounded_text(
        "Skill description",
        &manifest.description,
        MAX_DESCRIPTION_BYTES,
    )?;
    validate_bounded_text(
        "Skill instructions",
        &package.instructions,
        MAX_INSTRUCTIONS_BYTES,
    )?;
    if manifest.estimated_tokens == 0 || manifest.estimated_tokens > MAX_ESTIMATED_TOKENS {
        return Err(HarnessError::Skill(format!(
            "Skill estimated_tokens must be between 1 and {MAX_ESTIMATED_TOKENS}"
        )));
    }
    if manifest.dependencies.len() > MAX_SKILL_DEPENDENCIES {
        return Err(HarnessError::Skill(format!(
            "Skill contains more than {MAX_SKILL_DEPENDENCIES} dependencies"
        )));
    }
    if manifest.required_tools.len() > MAX_SKILL_REQUIRED_TOOLS {
        return Err(HarnessError::Skill(format!(
            "Skill contains more than {MAX_SKILL_REQUIRED_TOOLS} required tools"
        )));
    }
    let mut previous: Option<SkillId> = None;
    for dependency in &manifest.dependencies {
        validate_capability_name("skill dependency", &dependency.name)?;
        let id = SkillId::from(dependency);
        if previous.as_ref().is_some_and(|previous| previous >= &id) {
            return Err(HarnessError::Skill(
                "Skill dependencies must be unique and sorted".to_owned(),
            ));
        }
        previous = Some(id);
    }
    for tool in &manifest.required_tools {
        validate_capability_name("required tool", tool)?;
    }
    if package.resources.len() > MAX_RESOURCES {
        return Err(HarnessError::Skill(format!(
            "Skill contains more than {MAX_RESOURCES} resources"
        )));
    }
    for (path, content) in &package.resources {
        validate_resource_path(path)?;
        if content.len() > MAX_RESOURCE_BYTES {
            return Err(HarnessError::Skill(format!(
                "Skill resource {path:?} exceeds {MAX_RESOURCE_BYTES} bytes"
            )));
        }
    }
    if package.content_sha256.len() != 64
        || !package
            .content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::Skill(
            "Skill content_sha256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    let computed = package.computed_sha256()?;
    if computed != package.content_sha256 {
        return Err(HarnessError::Skill(format!(
            "Skill {}@{} content digest does not match",
            manifest.name, manifest.version
        )));
    }
    Ok(())
}

fn validate_package_content_envelope(
    manifest: &SkillManifest,
    instructions: &str,
    resources: &BTreeMap<String, String>,
) -> Result<usize, HarnessError> {
    if instructions.len() > MAX_INSTRUCTIONS_BYTES
        || manifest.description.len() > MAX_DESCRIPTION_BYTES
        || manifest.dependencies.len() > MAX_SKILL_DEPENDENCIES
        || manifest.required_tools.len() > MAX_SKILL_REQUIRED_TOOLS
        || resources.len() > MAX_RESOURCES
        || resources
            .values()
            .any(|content| content.len() > MAX_RESOURCE_BYTES)
    {
        return Err(HarnessError::Skill(
            "Skill package exceeds an individual or collection byte bound".to_owned(),
        ));
    }
    let mut total = instructions
        .len()
        .checked_add(manifest.api_version.len())
        .and_then(|total| total.checked_add(manifest.name.len()))
        .and_then(|total| total.checked_add(manifest.version.to_string().len()))
        .and_then(|total| total.checked_add(manifest.description.len()))
        .ok_or_else(|| HarnessError::Skill("Skill package byte count overflow".to_owned()))?;
    for dependency in &manifest.dependencies {
        total = total
            .checked_add(dependency.name.len())
            .and_then(|total| total.checked_add(dependency.version.to_string().len()))
            .ok_or_else(|| HarnessError::Skill("Skill package byte count overflow".to_owned()))?;
    }
    for tool in &manifest.required_tools {
        total = total
            .checked_add(tool.len())
            .ok_or_else(|| HarnessError::Skill("Skill package byte count overflow".to_owned()))?;
    }
    for (path, content) in resources {
        total = total
            .checked_add(path.len())
            .and_then(|total| total.checked_add(content.len()))
            .ok_or_else(|| HarnessError::Skill("Skill package byte count overflow".to_owned()))?;
    }
    if total > MAX_SKILL_PACKAGE_CONTENT_BYTES {
        return Err(HarnessError::Skill(format!(
            "Skill package content exceeds {MAX_SKILL_PACKAGE_CONTENT_BYTES} bytes"
        )));
    }
    Ok(total)
}

fn validate_bounded_text(field: &str, value: &str, maximum: usize) -> Result<(), HarnessError> {
    if value.trim().is_empty() {
        return Err(HarnessError::Skill(format!("{field} must not be empty")));
    }
    if value.len() > maximum {
        return Err(HarnessError::Skill(format!(
            "{field} exceeds {maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_resource_path(path: &str) -> Result<(), HarnessError> {
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(HarnessError::Skill(format!(
            "Skill resource path {path:?} must be a normalized relative slash path"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use ed25519_dalek::{Signer, SigningKey};
    use semver::Version;

    use super::{
        MAX_SKILL_REGISTRY_CONTENT_BYTES, SKILL_API_VERSION, SignedSkillPackage, SkillDependency,
        SkillEngine, SkillId, SkillManifest, SkillPackage, SkillPublisherPolicy, SkillRegistry,
        SkillSignature, SkillTransparencyReceipt, SkillTransparencyRequirement, SkillTrustStore,
        skill_signing_bytes,
    };
    use crate::{
        CapabilityOrigin, ContextEngine, ContextSource, HarnessError, MemoryScope, ToolRegistry,
    };

    fn package(
        name: &str,
        version: &str,
        dependencies: Vec<(&str, &str)>,
        required_tools: &[&str],
        estimated_tokens: usize,
    ) -> SkillPackage {
        SkillPackage::seal(
            SkillManifest {
                api_version: SKILL_API_VERSION.to_owned(),
                name: name.to_owned(),
                version: Version::parse(version).expect("version"),
                description: format!("{name} description"),
                estimated_tokens,
                dependencies: dependencies
                    .into_iter()
                    .map(|(name, version)| SkillDependency {
                        name: name.to_owned(),
                        version: Version::parse(version).expect("dependency version"),
                    })
                    .collect(),
                required_tools: required_tools
                    .iter()
                    .map(|tool| (*tool).to_owned())
                    .collect::<BTreeSet<_>>(),
            },
            format!("instructions for {name}"),
            BTreeMap::from([("references/guide.md".to_owned(), "guide".to_owned())]),
        )
        .expect("seal")
    }

    fn id(name: &str) -> SkillId {
        SkillId {
            name: name.to_owned(),
            version: Version::parse("1.0.0").expect("version"),
        }
    }

    #[test]
    fn skill_registry_capacity_rejection_is_failure_atomic() {
        let mut registry = SkillRegistry::new();
        registry.content_bytes = MAX_SKILL_REGISTRY_CONTENT_BYTES;
        let candidate = package("capacity", "1.0.0", Vec::new(), &[], 1);
        let candidate_id = candidate.id();

        assert!(
            registry
                .register(CapabilityOrigin::BuiltIn, candidate)
                .is_err()
        );
        assert!(registry.get(&candidate_id).is_none());
        assert_eq!(registry.content_bytes, MAX_SKILL_REGISTRY_CONTENT_BYTES);
    }

    fn signed_package(
        package: SkillPackage,
        key_id: &str,
        signing_key: &SigningKey,
    ) -> SignedSkillPackage {
        let signature = signing_key.sign(
            &package
                .publisher_signing_bytes()
                .expect("publisher signing material"),
        );
        SignedSkillPackage {
            package,
            signature: SkillSignature {
                key_id: key_id.to_owned(),
                ed25519: signature.to_bytes().to_vec(),
            },
            transparency: None,
        }
    }

    fn add_transparency(
        mut signed: SignedSkillPackage,
        log_id: &str,
        entry_id: &str,
        integrated_at_ms: u64,
        log_key: &SigningKey,
    ) -> SignedSkillPackage {
        signed.transparency = Some(SkillTransparencyReceipt {
            log_id: log_id.to_owned(),
            entry_id: entry_id.to_owned(),
            integrated_at_ms,
            ed25519: Vec::new(),
        });
        let signature = log_key.sign(
            &signed
                .transparency_signing_bytes()
                .expect("transparency signing material"),
        );
        signed.transparency.as_mut().expect("receipt").ed25519 = signature.to_bytes().to_vec();
        signed
    }

    #[test]
    fn external_skills_require_a_trusted_strict_signature() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
        let trust = SkillTrustStore::new();
        trust
            .trust("publisher", signing_key.verifying_key().to_bytes())
            .expect("trust publisher");
        let signed_package = package("signed", "1.0.0", vec![], &[], 10);
        let signature = signing_key.sign(&skill_signing_bytes(&signed_package));
        let signed = SignedSkillPackage {
            package: signed_package,
            signature: SkillSignature {
                key_id: "publisher".to_owned(),
                ed25519: signature.to_bytes().to_vec(),
            },
            transparency: None,
        };
        let encoded = serde_json::to_value(&signed).expect("encode signed package");
        assert!(encoded.get("transparency").is_none());
        let signed: SignedSkillPackage =
            serde_json::from_value(encoded).expect("decode receipt-free v1 package");
        let mut registry = SkillRegistry::new();
        registry
            .register_signed(
                CapabilityOrigin::External {
                    id: "fixture-source".to_owned(),
                },
                signed,
                &trust,
            )
            .expect("register signed package");
        assert_eq!(
            registry
                .get(&id("signed"))
                .expect("registered")
                .publisher_key_id
                .as_deref(),
            Some("publisher")
        );

        let unsigned = registry.register(
            CapabilityOrigin::External {
                id: "unsigned-source".to_owned(),
            },
            package("unsigned", "1.0.0", vec![], &[], 10),
        );
        assert!(matches!(unsigned, Err(HarnessError::Skill(_))));
    }

    #[test]
    fn signature_cannot_be_reused_for_different_skill_content() {
        let signing_key = SigningKey::from_bytes(&[9_u8; 32]);
        let trust = SkillTrustStore::new();
        trust
            .trust("publisher", signing_key.verifying_key().to_bytes())
            .expect("trust publisher");
        let original = package("original", "1.0.0", vec![], &[], 10);
        let signature = signing_key.sign(&skill_signing_bytes(&original));
        let substituted = SignedSkillPackage {
            package: package("substituted", "1.0.0", vec![], &[], 10),
            signature: SkillSignature {
                key_id: "publisher".to_owned(),
                ed25519: signature.to_bytes().to_vec(),
            },
            transparency: None,
        };
        let mut registry = SkillRegistry::new();
        let error = registry
            .register_signed(
                CapabilityOrigin::External {
                    id: "fixture-source".to_owned(),
                },
                substituted,
                &trust,
            )
            .expect_err("signature substitution must fail");
        assert!(matches!(error, HarnessError::Skill(_)));
    }

    #[test]
    fn publisher_validity_and_live_revocation_fail_closed() {
        let signing_key = SigningKey::from_bytes(&[11_u8; 32]);
        let trust = SkillTrustStore::new();
        trust
            .trust_with_policy(
                "publisher",
                signing_key.verifying_key().to_bytes(),
                SkillPublisherPolicy {
                    not_before_ms: Some(100),
                    not_after_ms: Some(u64::MAX),
                    transparency: SkillTransparencyRequirement::Optional,
                },
            )
            .expect("trust publisher");
        let signed = signed_package(
            package("windowed", "1.0.0", vec![], &[], 10),
            "publisher",
            &signing_key,
        );
        let origin = CapabilityOrigin::External {
            id: "fixture-source".to_owned(),
        };
        assert!(
            SkillRegistry::new()
                .register_signed_at(origin.clone(), signed.clone(), &trust, 99)
                .is_err()
        );
        let mut registry = SkillRegistry::new();
        registry
            .register_signed_at(origin, signed, &trust, 100)
            .expect("key is valid at inclusive start");

        let expiring_trust = SkillTrustStore::new();
        expiring_trust
            .trust_with_policy(
                "expiring",
                signing_key.verifying_key().to_bytes(),
                SkillPublisherPolicy {
                    not_before_ms: Some(100),
                    not_after_ms: Some(200),
                    transparency: SkillTransparencyRequirement::Optional,
                },
            )
            .expect("trust expiring publisher");
        let expiring = signed_package(
            package("expired", "1.0.0", vec![], &[], 10),
            "expiring",
            &signing_key,
        );
        let mut expiring_registry = SkillRegistry::new();
        expiring_registry
            .register_signed_at(
                CapabilityOrigin::External {
                    id: "fixture-source".to_owned(),
                },
                expiring.clone(),
                &expiring_trust,
                199,
            )
            .expect("register before expiry");
        assert!(
            SkillEngine::new(expiring_registry)
                .resolve(&[id("expired")], &ToolRegistry::new(), 10)
                .is_err()
        );
        assert!(
            SkillRegistry::new()
                .register_signed_at(
                    CapabilityOrigin::External {
                        id: "fixture-source".to_owned(),
                    },
                    expiring,
                    &expiring_trust,
                    200,
                )
                .is_err()
        );

        trust
            .revoke_publisher("publisher", 1, "compromised")
            .expect("revoke publisher");
        trust
            .revoke_publisher("publisher", 1, "compromised")
            .expect("exact revocation is idempotent");
        assert!(
            trust
                .revoke_publisher("publisher", 2, "superseded")
                .is_err()
        );
        assert_eq!(
            trust
                .publisher_revocation("publisher")
                .expect("revocation")
                .expect("present")
                .reason_code,
            "compromised"
        );
        let error = SkillEngine::new(registry)
            .resolve(&[id("windowed")], &ToolRegistry::new(), 10)
            .expect_err("live revocation blocks already registered package");
        assert!(error.to_string().contains("revoked"));
    }

    #[tokio::test]
    async fn required_transparency_is_signed_preserved_and_live_revocable() {
        let publisher_key = SigningKey::from_bytes(&[12_u8; 32]);
        let log_key = SigningKey::from_bytes(&[13_u8; 32]);
        let trust = SkillTrustStore::new();
        trust
            .trust_with_policy(
                "publisher",
                publisher_key.verifying_key().to_bytes(),
                SkillPublisherPolicy {
                    not_before_ms: None,
                    not_after_ms: None,
                    transparency: SkillTransparencyRequirement::Required,
                },
            )
            .expect("trust publisher");
        trust
            .trust_transparency_log("audit-log", log_key.verifying_key().to_bytes())
            .expect("trust log");

        let unsigned_receipt = signed_package(
            package("transparent", "1.0.0", vec![], &[], 10),
            "publisher",
            &publisher_key,
        );
        let origin = CapabilityOrigin::External {
            id: "fixture-source".to_owned(),
        };
        assert!(
            SkillRegistry::new()
                .register_signed_at(origin.clone(), unsigned_receipt.clone(), &trust, 1_000)
                .is_err()
        );

        let future_receipt = add_transparency(
            unsigned_receipt.clone(),
            "audit-log",
            "entry-future",
            301_001,
            &log_key,
        );
        assert!(
            SkillRegistry::new()
                .register_signed_at(origin.clone(), future_receipt, &trust, 1_000)
                .is_err()
        );

        let signed = add_transparency(unsigned_receipt, "audit-log", "entry-0001", 900, &log_key);
        let mut tampered = signed.clone();
        tampered.transparency.as_mut().expect("receipt").entry_id = "entry-0002".to_owned();
        assert!(
            SkillRegistry::new()
                .register_signed_at(origin.clone(), tampered, &trust, 1_000)
                .is_err()
        );

        let mut registry = SkillRegistry::new();
        registry
            .register_signed_at(origin, signed, &trust, 1_000)
            .expect("valid signed transparency receipt");
        let evidence = registry
            .get(&id("transparent"))
            .expect("registered")
            .transparency
            .as_ref()
            .expect("transparency evidence");
        assert_eq!(evidence.log_id, "audit-log");
        assert_eq!(evidence.entry_id, "entry-0001");
        let engine = SkillEngine::new(registry);
        let resolved = engine
            .resolve(&[id("transparent")], &ToolRegistry::new(), 10)
            .expect("resolve before revocation");
        let context = ContextEngine::without_memory().with_skills(resolved);

        trust
            .revoke_transparency_log("audit-log", 1, "log_compromised")
            .expect("revoke log");
        assert_eq!(
            trust
                .transparency_log_revocation("audit-log")
                .expect("revocation")
                .expect("present")
                .reason_code,
            "log_compromised"
        );
        let error = engine
            .resolve(&[id("transparent")], &ToolRegistry::new(), 10)
            .expect_err("live log revocation blocks resolution");
        assert!(error.to_string().contains("revoked"));
        assert!(
            engine
                .read_resource(&id("transparent"), "references/guide.md")
                .is_err()
        );
        assert!(
            context
                .compile("prompt", MemoryScope::default())
                .await
                .is_err()
        );
    }

    #[test]
    fn resolves_exact_dependencies_before_dependants() {
        let mut registry = SkillRegistry::new();
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("base", "1.0.0", vec![], &[], 10),
            )
            .expect("base");
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("review", "1.0.0", vec![("base", "1.0.0")], &[], 20),
            )
            .expect("review");
        let engine = SkillEngine::new(registry);

        let resolved = engine
            .resolve(&[id("review")], &ToolRegistry::new(), 30)
            .expect("resolve");

        assert_eq!(
            resolved
                .skills
                .iter()
                .map(|skill| skill.id.name.as_str())
                .collect::<Vec<_>>(),
            ["base", "review"]
        );
        assert_eq!(resolved.estimated_tokens, 30);
        assert!(matches!(
            &resolved.context[1].source,
            ContextSource::Skill { name, .. } if name == "review"
        ));
        assert_eq!(
            engine
                .read_resource(&id("review"), "references/guide.md")
                .expect("resource"),
            "guide"
        );
    }

    #[tokio::test]
    async fn resolved_instructions_enter_context_in_dependency_order() {
        let mut registry = SkillRegistry::new();
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("base", "1.0.0", vec![], &[], 10),
            )
            .expect("base");
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("review", "1.0.0", vec![("base", "1.0.0")], &[], 20),
            )
            .expect("review");
        let resolved = SkillEngine::new(registry)
            .resolve(&[id("review")], &ToolRegistry::new(), 30)
            .expect("resolve");

        let compiled = ContextEngine::without_memory()
            .with_skills(resolved)
            .compile("prompt", MemoryScope::default())
            .await
            .expect("compile");

        assert_eq!(
            compiled
                .blocks
                .iter()
                .map(|block| block.text.as_str())
                .collect::<Vec<_>>(),
            ["instructions for base", "instructions for review"]
        );
    }

    #[test]
    fn rejects_content_tampering_after_seal() {
        let mut package = package("review", "1.0.0", vec![], &[], 20);
        package.instructions.push_str(" tampered");
        let error = SkillRegistry::new()
            .register(CapabilityOrigin::BuiltIn, package)
            .expect_err("digest mismatch");
        assert!(error.to_string().contains("digest does not match"));
    }

    #[test]
    fn rejects_aggregate_package_growth_before_canonical_allocation() {
        let resources = (0..3)
            .map(|index| {
                (
                    format!("references/{index}.txt"),
                    "x".repeat(super::MAX_RESOURCE_BYTES),
                )
            })
            .collect();
        let error = SkillPackage::seal(
            SkillManifest {
                api_version: SKILL_API_VERSION.to_owned(),
                name: "oversized".to_owned(),
                version: Version::parse("1.0.0").expect("version"),
                description: "aggregate bound".to_owned(),
                estimated_tokens: 10,
                dependencies: Vec::new(),
                required_tools: BTreeSet::new(),
            },
            "instructions".to_owned(),
            resources,
        )
        .expect_err("aggregate package must be bounded");
        assert!(error.to_string().contains("content exceeds"));
    }

    #[test]
    fn detects_exact_dependency_cycles() {
        let mut registry = SkillRegistry::new();
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("alpha", "1.0.0", vec![("beta", "1.0.0")], &[], 10),
            )
            .expect("alpha");
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("beta", "1.0.0", vec![("alpha", "1.0.0")], &[], 10),
            )
            .expect("beta");
        let error = SkillEngine::new(registry)
            .resolve(&[id("alpha")], &ToolRegistry::new(), 100)
            .expect_err("cycle");
        assert!(error.to_string().contains("dependency cycle"));
    }

    #[test]
    fn rejects_missing_tools_and_whole_package_budget_overflow() {
        let required_package = package("review", "1.0.0", vec![], &["browser"], 20);
        let mut registry = SkillRegistry::new();
        registry
            .register(CapabilityOrigin::BuiltIn, required_package)
            .expect("register");
        let engine = SkillEngine::new(registry);
        let missing = engine
            .resolve(&[id("review")], &ToolRegistry::new(), 20)
            .expect_err("missing tool");
        assert!(missing.to_string().contains("missing tool browser"));

        let mut registry = SkillRegistry::new();
        registry
            .register(
                CapabilityOrigin::BuiltIn,
                package("review", "1.0.0", vec![], &[], 20),
            )
            .expect("register");
        let budget = SkillEngine::new(registry)
            .resolve(&[id("review")], &ToolRegistry::new(), 19)
            .expect_err("whole package budget");
        assert!(matches!(budget, HarnessError::Skill(_)));
    }
}
