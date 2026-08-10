#![cfg(feature = "https-skill")]

#[cfg(feature = "tls-host")]
use ed25519_dalek::{Signer, SigningKey};
#[cfg(feature = "tls-host")]
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
#[cfg(feature = "tls-host")]
use rustls::{
    ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};
use semver::Version;
#[cfg(feature = "tls-host")]
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
#[cfg(feature = "tls-host")]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
#[cfg(feature = "tls-host")]
use tokio_rustls::TlsAcceptor;
use y_harness::{
    CapabilityOrigin, HttpsSkillSource, HttpsSkillSourceConfig, SkillId, SkillRegistry,
    SkillTrustStore,
};
#[cfg(feature = "tls-host")]
use y_harness::{
    SKILL_API_VERSION, SecretValue, SignedSkillPackage, SkillManifest, SkillPackage, SkillSignature,
};

#[cfg(feature = "tls-host")]
#[tokio::test]
async fn private_ca_and_request_scoped_bearer_round_trip() {
    let (signed, expected) = signed_fixture();
    let digest = signed.package.content_sha256.clone();
    let body = serde_json::to_vec(&signed).expect("encode signed Skill");
    let (ca, issuer) = certificate_authority();
    let (server_certificate, server_key) = server_certificate(&issuer);
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![server_certificate.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
        )
        .expect("server identity");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind private Registry");
    let address = listener.local_addr().expect("Registry address");
    let server = tokio::spawn(serve_one_https_get(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        body,
    ));
    let config = HttpsSkillSourceConfig::new(format!(
        "https://127.0.0.1:{}/packages/private.json",
        address.port()
    ))
    .expect("private Registry endpoint")
    .with_exclusive_root_certificates_pem(ca.pem().into_bytes())
    .expect("private Registry CA");
    let source = HttpsSkillSource::new(config.clone()).expect("private source");
    let fetched = source
        .fetch_with_bearer(
            &expected,
            &digest,
            SecretValue::new(b"registry-fixture-token".to_vec()).expect("Bearer credential"),
        )
        .await
        .expect("authenticated private fetch");
    assert_eq!(fetched.package.content_sha256, digest);

    let request = String::from_utf8(server.await.expect("Registry task")).expect("HTTP request");
    assert!(request.starts_with("GET /packages/private.json HTTP/1.1\r\n"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nauthorization: bearer registry-fixture-token\r\n")
    );
    assert!(!format!("{config:?}").contains("registry-fixture-token"));
}

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

#[cfg(feature = "tls-host")]
fn signed_fixture() -> (SignedSkillPackage, SkillId) {
    let package = SkillPackage::seal(
        SkillManifest {
            api_version: SKILL_API_VERSION.to_owned(),
            name: "private-registry-fixture".to_owned(),
            version: Version::parse("1.0.0").expect("version"),
            description: "Private Registry transport fixture".to_owned(),
            estimated_tokens: 8,
            dependencies: Vec::new(),
            required_tools: BTreeSet::new(),
        },
        "Use only as a transport fixture.".to_owned(),
        BTreeMap::new(),
    )
    .expect("sealed Skill");
    let identity = SkillId {
        name: package.manifest.name.clone(),
        version: package.manifest.version.clone(),
    };
    let signing_key = SigningKey::from_bytes(&[41_u8; 32]);
    let signature = signing_key.sign(
        &package
            .publisher_signing_bytes()
            .expect("publisher signing bytes"),
    );
    (
        SignedSkillPackage {
            package,
            signature: SkillSignature {
                key_id: "private-registry-publisher".to_owned(),
                ed25519: signature.to_bytes().to_vec(),
            },
            transparency: None,
        },
        identity,
    )
}

#[cfg(feature = "tls-host")]
async fn serve_one_https_get(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    response: Vec<u8>,
) -> Vec<u8> {
    let (stream, _) = listener.accept().await.expect("accept private Registry");
    let mut stream = acceptor
        .accept(stream)
        .await
        .expect("Registry TLS handshake");
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4_096];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream
            .read(&mut buffer)
            .await
            .expect("read Registry request");
        assert!(read > 0, "Registry request ended before its headers");
        request.extend_from_slice(&buffer[..read]);
        assert!(request.len() <= 65_536, "Registry request exceeded bound");
    }
    let headers = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .expect("write Registry headers");
    stream
        .write_all(&response)
        .await
        .expect("write Registry body");
    stream.shutdown().await.expect("close Registry response");
    request
}

#[cfg(feature = "tls-host")]
fn certificate_authority() -> (Certificate, Issuer<'static, KeyPair>) {
    let mut parameters = CertificateParams::new(Vec::new()).expect("CA parameters");
    parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    parameters.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let key = KeyPair::generate().expect("CA key");
    let certificate = parameters.self_signed(&key).expect("CA certificate");
    (certificate, Issuer::new(parameters, key))
}

#[cfg(feature = "tls-host")]
fn server_certificate(issuer: &Issuer<'static, KeyPair>) -> (Certificate, KeyPair) {
    let mut parameters =
        CertificateParams::new(vec!["127.0.0.1".to_owned()]).expect("server parameters");
    parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().expect("server key");
    let certificate = parameters
        .signed_by(&key, issuer)
        .expect("server certificate");
    (certificate, key)
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
