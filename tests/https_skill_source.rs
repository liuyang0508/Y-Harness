#![cfg(feature = "https-skill")]

use semver::Version;
use y_harness::{
    CapabilityOrigin, HttpsSkillSource, HttpsSkillSourceConfig, SkillId, SkillRegistry,
    SkillTrustStore,
};

#[tokio::test]
#[ignore = "requires a pinned public HTTPS Skill fixture and publisher key"]
async fn pinned_https_skill_round_trip() {
    let endpoint = std::env::var("YH_HTTPS_SKILL_ENDPOINT").expect("fixture endpoint");
    let name = std::env::var("YH_HTTPS_SKILL_NAME").expect("fixture Skill name");
    let version = std::env::var("YH_HTTPS_SKILL_VERSION").expect("fixture Skill version");
    let digest = std::env::var("YH_HTTPS_SKILL_SHA256").expect("fixture content digest");
    let publisher_key =
        std::env::var("YH_HTTPS_SKILL_PUBLISHER_KEY_HEX").expect("fixture publisher public key");
    let publisher_key_id =
        std::env::var("YH_HTTPS_SKILL_PUBLISHER_KEY_ID").expect("fixture publisher key id");

    let source =
        HttpsSkillSource::new(HttpsSkillSourceConfig::new(endpoint).expect("source config"))
            .expect("HTTPS source");
    let expected = SkillId {
        name,
        version: Version::parse(&version).expect("semantic version"),
    };
    let trust = SkillTrustStore::new();
    trust
        .trust(publisher_key_id, decode_key(&publisher_key))
        .expect("publisher trust");
    let mut registry = SkillRegistry::new();
    source
        .fetch_and_register(
            &mut registry,
            CapabilityOrigin::External {
                id: "https-fixture".to_owned(),
            },
            &expected,
            &digest,
            &trust,
        )
        .await
        .expect("pinned fetch and verification");
    assert!(registry.get(&expected).is_some());
}

fn decode_key(encoded: &str) -> [u8; 32] {
    assert_eq!(encoded.len(), 64, "publisher key must be 32-byte hex");
    let mut decoded = [0_u8; 32];
    for (index, byte) in decoded.iter_mut().enumerate() {
        let offset = index * 2;
        *byte = u8::from_str_radix(&encoded[offset..offset + 2], 16)
            .expect("publisher key must be hexadecimal");
    }
    decoded
}
