//! Immutable Domain Pack snapshot for the first aquaculture release.

use semver::Version;
use serde::Serialize;
use sha2::{Digest, Sha256};
use y_harness_domain_pack::{
    DomainPackComponentKind, DomainPackComponentPin, DomainPackError, DomainPackReleaseId,
    DomainPackSnapshot,
};

use crate::{evaluation::poc_evaluation_suite, journey::journey_registry};

/// First executable aquaculture Domain Pack version.
pub const AQUACULTURE_PACK_VERSION: &str = "0.1.0";

/// Builds and digest-seals the POC release without mutating the Core runtime.
pub fn build_domain_pack_snapshot() -> Result<DomainPackSnapshot, DomainPackError> {
    let journeys = journey_registry();
    let evaluation = poc_evaluation_suite()
        .map_err(|error| DomainPackError::Invalid(format!("evaluation suite: {error}")))?;
    let components = vec![
        pin(
            DomainPackComponentKind::Workflow,
            "aquaculture.journey-registry",
            AQUACULTURE_PACK_VERSION,
            &journeys,
        )?,
        pin(
            DomainPackComponentKind::Schema,
            "aquaculture.context-package",
            "1.0.0",
            &"aquaculture.context-package/v1",
        )?,
        pin(
            DomainPackComponentKind::Schema,
            "aquaculture.answer",
            "1.0.0",
            &"aquaculture.answer/v1",
        )?,
        pin(
            DomainPackComponentKind::Skill,
            "aq-diagnostic-reasoning",
            AQUACULTURE_PACK_VERSION,
            &"scope -> retrieve -> correlate -> hypotheses -> recommendation",
        )?,
        pin(
            DomainPackComponentKind::Tool,
            "aquaculture.iot.query",
            AQUACULTURE_PACK_VERSION,
            &"tenant-fenced synthetic IoT connector v1",
        )?,
        pin(
            DomainPackComponentKind::Tool,
            "aquaculture.erp.query",
            AQUACULTURE_PACK_VERSION,
            &"tenant-fenced synthetic ERP connector v1",
        )?,
        pin(
            DomainPackComponentKind::Policy,
            "aquaculture.output-contract",
            AQUACULTURE_PACK_VERSION,
            &"pond scope + evidence links + calibrated confidence + origin disclosure",
        )?,
        pin(
            DomainPackComponentKind::Evaluation,
            "aquaculture-poc-v1",
            AQUACULTURE_PACK_VERSION,
            &evaluation,
        )?,
    ];
    DomainPackSnapshot::seal(
        DomainPackReleaseId {
            name: "aquaculture-agent".to_owned(),
            version: Version::parse(AQUACULTURE_PACK_VERSION)
                .map_err(|error| DomainPackError::Invalid(error.to_string()))?,
        },
        "Aquaculture POC with all Journey contracts and executable AQ-JR-001 mock data plane",
        components,
    )
}

fn pin<T: Serialize>(
    kind: DomainPackComponentKind,
    name: &str,
    version: &str,
    content: &T,
) -> Result<DomainPackComponentPin, DomainPackError> {
    let encoded = serde_json::to_vec(content)
        .map_err(|error| DomainPackError::Invalid(format!("cannot encode {name}: {error}")))?;
    let digest = Sha256::digest(encoded);
    let content_sha256 = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(DomainPackComponentPin {
        kind,
        name: name.to_owned(),
        version: version.to_owned(),
        content_sha256,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use y_harness_domain_pack::DomainPackInventory;

    #[test]
    fn snapshot_matches_exact_inventory() {
        let snapshot = build_domain_pack_snapshot().expect("snapshot");
        let inventory = DomainPackInventory::new(snapshot.components.clone()).expect("inventory");
        snapshot.verify(&inventory).expect("verified pack");
    }
}
