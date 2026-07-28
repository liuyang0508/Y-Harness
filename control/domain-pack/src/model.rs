use std::collections::{BTreeMap, BTreeSet};

use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::DomainPackError;

/// Current immutable Domain Pack snapshot format.
pub const DOMAIN_PACK_FORMAT_VERSION: u32 = 1;

const MAX_COMPONENTS: usize = 256;
const MAX_DESCRIPTION_BYTES: usize = 4_096;
const MAX_COORDINATE_BYTES: usize = 128;
const MAX_SNAPSHOT_BYTES: usize = 1_048_576;

/// Stable identity of one immutable Domain Pack release.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackReleaseId {
    /// Stable Pack name.
    pub name: String,
    /// Exact semantic release version.
    pub version: Version,
}

/// Kind of exact component pinned by a Domain Pack.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainPackComponentKind {
    /// Durable or externally hosted workflow definition.
    Workflow,
    /// Declarative Skill package.
    Skill,
    /// Tool or Connector capability.
    Tool,
    /// Policy bundle or policy-provider ruleset.
    Policy,
    /// Evaluation suite and promotion baseline.
    Evaluation,
    /// Authoritative input/output data schema.
    Schema,
}

/// Exact immutable component coordinate required by a Domain Pack.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackComponentPin {
    /// Component class.
    pub kind: DomainPackComponentKind,
    /// Stable component identity.
    pub name: String,
    /// Exact provider-owned version coordinate.
    pub version: String,
    /// Lowercase SHA-256 of the immutable component content.
    pub content_sha256: String,
}

/// Immutable, digest-bound Domain Pack release snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackSnapshot {
    /// Exact snapshot format.
    pub format_version: u32,
    /// Stable Pack identity and semantic version.
    pub release: DomainPackReleaseId,
    /// Human-readable deployment purpose.
    pub description: String,
    /// Sorted exact component pins.
    pub components: Vec<DomainPackComponentPin>,
    /// Lowercase SHA-256 of all preceding fields.
    pub content_sha256: String,
}

/// Exact installed component inventory presented at activation time.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackInventory {
    components: Vec<DomainPackComponentPin>,
    content_sha256: String,
}

/// Constructor-only proof that one snapshot matches an exact inventory.
#[derive(Clone, Debug)]
pub struct VerifiedDomainPack {
    snapshot: DomainPackSnapshot,
    inventory_sha256: String,
}

impl DomainPackSnapshot {
    /// Sorts, validates, and digest-seals one immutable release.
    pub fn seal(
        release: DomainPackReleaseId,
        description: impl Into<String>,
        mut components: Vec<DomainPackComponentPin>,
    ) -> Result<Self, DomainPackError> {
        components.sort();
        let mut snapshot = Self {
            format_version: DOMAIN_PACK_FORMAT_VERSION,
            release,
            description: description.into(),
            components,
            content_sha256: String::new(),
        };
        snapshot.content_sha256 = snapshot.computed_sha256()?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Revalidates format, bounds, ordering, uniqueness, and content digest.
    pub fn validate(&self) -> Result<(), DomainPackError> {
        if self.format_version != DOMAIN_PACK_FORMAT_VERSION {
            return Err(DomainPackError::Invalid(format!(
                "unsupported Domain Pack format {}; expected {DOMAIN_PACK_FORMAT_VERSION}",
                self.format_version
            )));
        }
        validate_release_id(&self.release)?;
        if self.description.trim().is_empty()
            || self.description.len() > MAX_DESCRIPTION_BYTES
            || self.description.chars().any(char::is_control)
        {
            return Err(DomainPackError::Invalid(format!(
                "Domain Pack description must be 1-{MAX_DESCRIPTION_BYTES} non-control bytes"
            )));
        }
        validate_components(&self.components)?;
        validate_digest("Domain Pack content", &self.content_sha256)?;
        let computed = self.computed_sha256()?;
        if self.content_sha256 != computed {
            return Err(DomainPackError::Invalid(format!(
                "Domain Pack {}@{} content digest does not match",
                self.release.name, self.release.version
            )));
        }
        bounded_json_size(self, MAX_SNAPSHOT_BYTES, "Domain Pack snapshot")?;
        Ok(())
    }

    /// Computes the canonical digest without trusting the declared digest.
    pub fn computed_sha256(&self) -> Result<String, DomainPackError> {
        #[derive(Serialize)]
        struct DigestMaterial<'a> {
            format_version: u32,
            release: &'a DomainPackReleaseId,
            description: &'a str,
            components: &'a [DomainPackComponentPin],
        }

        let encoded = serde_json::to_vec(&DigestMaterial {
            format_version: self.format_version,
            release: &self.release,
            description: &self.description,
            components: &self.components,
        })
        .map_err(|_| DomainPackError::Invalid("cannot encode Domain Pack digest".to_owned()))?;
        if encoded.len() > MAX_SNAPSHOT_BYTES {
            return Err(DomainPackError::Invalid(format!(
                "Domain Pack snapshot exceeds {MAX_SNAPSHOT_BYTES} bytes"
            )));
        }
        Ok(sha256(&encoded))
    }

    /// Verifies every required pin against an exact installed inventory.
    pub fn verify(
        &self,
        inventory: &DomainPackInventory,
    ) -> Result<VerifiedDomainPack, DomainPackError> {
        self.validate()?;
        inventory.validate()?;
        let available: BTreeMap<_, _> = inventory
            .components
            .iter()
            .map(|component| ((component.kind, component.name.as_str()), component))
            .collect();
        for required in &self.components {
            let Some(installed) = available.get(&(required.kind, required.name.as_str())) else {
                return Err(DomainPackError::Invalid(format!(
                    "Domain Pack component {:?}/{} is not installed",
                    required.kind, required.name
                )));
            };
            if *installed != required {
                return Err(DomainPackError::Invalid(format!(
                    "Domain Pack component {:?}/{} does not match its exact pin",
                    required.kind, required.name
                )));
            }
        }
        Ok(VerifiedDomainPack {
            snapshot: self.clone(),
            inventory_sha256: inventory.content_sha256.clone(),
        })
    }
}

impl DomainPackInventory {
    /// Sorts, validates, and digest-seals an installed component inventory.
    pub fn new(mut components: Vec<DomainPackComponentPin>) -> Result<Self, DomainPackError> {
        components.sort();
        validate_components(&components)?;
        let content_sha256 = inventory_sha256(&components)?;
        Ok(Self {
            components,
            content_sha256,
        })
    }

    /// Returns sorted exact installed component pins.
    #[must_use]
    pub fn components(&self) -> &[DomainPackComponentPin] {
        &self.components
    }

    /// Returns the digest of the complete installed inventory.
    #[must_use]
    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    fn validate(&self) -> Result<(), DomainPackError> {
        validate_components(&self.components)?;
        validate_digest("Domain Pack inventory", &self.content_sha256)?;
        if self.content_sha256 != inventory_sha256(&self.components)? {
            return Err(DomainPackError::Invalid(
                "Domain Pack inventory digest does not match".to_owned(),
            ));
        }
        Ok(())
    }
}

impl VerifiedDomainPack {
    /// Returns the exact verified snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &DomainPackSnapshot {
        &self.snapshot
    }

    /// Returns the exact inventory digest observed during verification.
    #[must_use]
    pub fn inventory_sha256(&self) -> &str {
        &self.inventory_sha256
    }
}

fn validate_components(components: &[DomainPackComponentPin]) -> Result<(), DomainPackError> {
    if components.is_empty() || components.len() > MAX_COMPONENTS {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack must pin 1-{MAX_COMPONENTS} components"
        )));
    }
    let mut identities = BTreeSet::new();
    let mut previous: Option<&DomainPackComponentPin> = None;
    let mut has_evaluation = false;
    for component in components {
        if previous.is_some_and(|value| value > component) {
            return Err(DomainPackError::Invalid(
                "Domain Pack components are not in canonical order".to_owned(),
            ));
        }
        previous = Some(component);
        validate_name("Domain Pack component", &component.name)?;
        validate_coordinate("Domain Pack component version", &component.version)?;
        validate_digest("Domain Pack component", &component.content_sha256)?;
        has_evaluation |= component.kind == DomainPackComponentKind::Evaluation;
        if !identities.insert((component.kind, component.name.as_str())) {
            return Err(DomainPackError::Invalid(format!(
                "duplicate Domain Pack component {:?}/{}",
                component.kind, component.name
            )));
        }
    }
    if !has_evaluation {
        return Err(DomainPackError::Invalid(
            "Domain Pack must pin at least one Evaluation suite".to_owned(),
        ));
    }
    Ok(())
}

fn validate_name(kind: &str, value: &str) -> Result<(), DomainPackError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_COORDINATE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'));
    if !valid {
        return Err(DomainPackError::Invalid(format!(
            "{kind} identity must be 1-{MAX_COORDINATE_BYTES} portable ASCII bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_release_id(release: &DomainPackReleaseId) -> Result<(), DomainPackError> {
    validate_name("Domain Pack", &release.name)?;
    validate_coordinate("Domain Pack release version", &release.version.to_string())
}

fn validate_coordinate(kind: &str, value: &str) -> Result<(), DomainPackError> {
    let valid = !value.is_empty()
        && value.len() <= MAX_COORDINATE_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':' | b'+')
        });
    if !valid {
        return Err(DomainPackError::Invalid(format!(
            "{kind} must be 1-{MAX_COORDINATE_BYTES} portable ASCII bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_digest(kind: &str, value: &str) -> Result<(), DomainPackError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(DomainPackError::Invalid(format!(
            "{kind} digest must be lowercase SHA-256"
        )));
    }
    Ok(())
}

fn inventory_sha256(components: &[DomainPackComponentPin]) -> Result<String, DomainPackError> {
    let encoded = serde_json::to_vec(components)
        .map_err(|_| DomainPackError::Invalid("cannot encode Domain Pack inventory".to_owned()))?;
    if encoded.len() > MAX_SNAPSHOT_BYTES {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack inventory exceeds {MAX_SNAPSHOT_BYTES} bytes"
        )));
    }
    Ok(sha256(&encoded))
}

pub(crate) fn bounded_json_size<T: Serialize>(
    value: &T,
    maximum: usize,
    kind: &str,
) -> Result<usize, DomainPackError> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| DomainPackError::Invalid(format!("cannot encode {kind}")))?;
    if encoded.len() > maximum {
        return Err(DomainPackError::Invalid(format!(
            "{kind} exceeds {maximum} bytes"
        )));
    }
    Ok(encoded.len())
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_sealing_is_canonical_and_tamper_evident() {
        let workflow = component(
            DomainPackComponentKind::Workflow,
            "course-assistant",
            "workflow:v3",
            'a',
        );
        let policy = component(
            DomainPackComponentKind::Policy,
            "enterprise-default",
            "policy:v2",
            'b',
        );
        let evaluation = component(
            DomainPackComponentKind::Evaluation,
            "promotion",
            "eval:v1",
            'c',
        );
        let release = release("assistant", 1);
        let first = DomainPackSnapshot::seal(
            release.clone(),
            "Enterprise assistant",
            vec![workflow.clone(), policy.clone(), evaluation.clone()],
        )
        .expect("seal snapshot");
        let second = DomainPackSnapshot::seal(
            release,
            "Enterprise assistant",
            vec![evaluation, policy, workflow],
        )
        .expect("seal reordered snapshot");
        assert_eq!(first, second);

        let mut tampered = first;
        tampered.description = "A different deployment".to_owned();
        assert!(tampered.validate().is_err());

        let mut encoded = serde_json::to_value(second).expect("encode snapshot");
        encoded
            .as_object_mut()
            .expect("snapshot object")
            .insert("future_semantics".to_owned(), serde_json::json!(true));
        assert!(serde_json::from_value::<DomainPackSnapshot>(encoded).is_err());
    }

    #[test]
    fn inventory_requires_every_exact_pin_but_allows_host_extras() {
        let required = component(DomainPackComponentKind::Tool, "orders.read", "tool:v1", 'c');
        let snapshot = DomainPackSnapshot::seal(
            release("assistant", 1),
            "Enterprise assistant",
            vec![
                required.clone(),
                component(
                    DomainPackComponentKind::Evaluation,
                    "promotion",
                    "eval:v1",
                    'e',
                ),
            ],
        )
        .expect("seal snapshot");
        let extra = component(DomainPackComponentKind::Skill, "unrelated", "skill:v9", 'd');
        let evaluation = snapshot
            .components
            .iter()
            .find(|component| component.kind == DomainPackComponentKind::Evaluation)
            .expect("evaluation pin")
            .clone();
        let inventory = DomainPackInventory::new(vec![extra, required.clone(), evaluation.clone()])
            .expect("inventory");
        assert!(snapshot.verify(&inventory).is_ok());

        let mismatched = DomainPackInventory::new(vec![
            DomainPackComponentPin {
                content_sha256: digest('f'),
                ..required
            },
            evaluation,
        ])
        .expect("mismatched inventory");
        assert!(snapshot.verify(&mismatched).is_err());
    }

    fn release(name: &str, major: u64) -> DomainPackReleaseId {
        DomainPackReleaseId {
            name: name.to_owned(),
            version: Version::new(major, 0, 0),
        }
    }

    fn component(
        kind: DomainPackComponentKind,
        name: &str,
        version: &str,
        digest_character: char,
    ) -> DomainPackComponentPin {
        DomainPackComponentPin {
            kind,
            name: name.to_owned(),
            version: version.to_owned(),
            content_sha256: digest(digest_character),
        }
    }

    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }
}
