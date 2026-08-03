#![cfg(all(feature = "https-skill", feature = "tls-host"))]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signer, SigningKey};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};
use semver::Version;
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
};
use tokio_rustls::TlsAcceptor;
use y_harness::{
    SKILL_API_VERSION, SignedSkillPackage, SkillManifest, SkillPackage, SkillSignature,
};

const FIXTURE_TOKEN: &str = "private-registry-token-never-persist";

#[tokio::test]
async fn configured_private_registry_installs_exact_signed_package_without_leaking_credential() {
    let project = isolated_project();
    fs::create_dir_all(&project).expect("create private Registry project");
    let signing_key = SigningKey::from_bytes(&[53_u8; 32]);
    let package = signed_package(&signing_key);
    let package_bytes = serde_json::to_vec(&package).expect("encode signed package");

    let (ca, issuer) = certificate_authority();
    let (server_certificate, server_key) = server_certificate(&issuer);
    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![server_certificate.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
        )
        .expect("Registry server identity");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind private Registry");
    let address = listener.local_addr().expect("Registry address");
    let origin = format!("https://127.0.0.1:{}", address.port());
    let package_endpoint = format!("{origin}/packages/private-fixture-1.0.0.json");
    let catalog_endpoint = format!("{origin}/catalog.json");
    let catalog = serde_json::json!({
        "format_version": 1,
        "entries": [{
            "name": package.package.manifest.name.clone(),
            "version": package.package.manifest.version.clone(),
            "description": package.package.manifest.description.clone(),
            "endpoint": package_endpoint.clone(),
            "content_sha256": package.package.content_sha256.clone(),
            "yanked": false,
            "tags": ["private", "test"]
        }]
    });
    let catalog_bytes = serde_json::to_vec(&catalog).expect("encode catalog");
    let catalog_sha256 = lower_sha256(&catalog_bytes);
    let server = tokio::spawn(serve_registry(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        vec![
            ("/catalog.json".to_owned(), catalog_bytes),
            (
                "/packages/private-fixture-1.0.0.json".to_owned(),
                package_bytes,
            ),
        ],
    ));

    fs::write(project.join("registry-ca.pem"), ca.pem()).expect("write Registry CA");
    let public_key = lower_hex(&signing_key.verifying_key().to_bytes());
    let config = serde_json::json!({
        "schema_version": 1,
        "data_directory": ".y-harness",
        "model": {"type": "demo"},
        "skills": {
            "package_files": [],
            "external_package_files": [],
            "activate": [],
            "trust": {
                "publishers": [{
                    "key_id": "private-registry-publisher",
                    "public_key_hex": public_key
                }],
                "transparency_logs": []
            }
        },
        "skill_registries": [{
            "id": "private/test",
            "catalog_endpoint": catalog_endpoint.clone(),
            "package_origins": [origin.clone()],
            "authentication": {
                "type": "bearer",
                "secret_reference": "registry/private-test",
                "environment": "YH_PRIVATE_REGISTRY_FIXTURE_TOKEN"
            },
            "exclusive_root_ca_pem_path": "registry-ca.pem"
        }]
    });
    let config_path = project.join("y-harness.json");
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("encode service config"),
    )
    .expect("write service config");

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args([
            "package",
            "registry-install",
            "private/test",
            &catalog_sha256,
            "private-fixture@1.0.0",
        ])
        .arg(&config_path)
        .env("YH_PRIVATE_REGISTRY_FIXTURE_TOKEN", FIXTURE_TOKEN)
        .output()
        .await
        .expect("run Registry install");
    assert!(
        output.status.success(),
        "Registry install failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("install stdout");
    let stderr = String::from_utf8(output.stderr).expect("install stderr");
    assert!(stdout.contains("catalog plan: 1 package(s) / 1 download(s)"));
    assert!(stdout.contains("activation required"));
    assert!(!stdout.contains(FIXTURE_TOKEN));
    assert!(!stderr.contains(FIXTURE_TOKEN));

    let requests = server.await.expect("Registry server task");
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert!(
            request
                .to_ascii_lowercase()
                .contains(&format!("\r\nauthorization: bearer {FIXTURE_TOKEN}\r\n"))
        );
    }
    let installed = project.join(format!(
        "skills/{}.signed-skill.json",
        package.package.content_sha256
    ));
    assert!(installed.is_file());
    assert_tree_does_not_contain(&project, FIXTURE_TOKEN.as_bytes());
    fs::remove_dir_all(project).expect("remove private Registry project");
}

fn signed_package(signing_key: &SigningKey) -> SignedSkillPackage {
    let package = SkillPackage::seal(
        SkillManifest {
            api_version: SKILL_API_VERSION.to_owned(),
            name: "private-fixture".to_owned(),
            version: Version::parse("1.0.0").expect("version"),
            description: "Private Registry end-to-end fixture".to_owned(),
            estimated_tokens: 16,
            dependencies: Vec::new(),
            required_tools: BTreeSet::new(),
        },
        "Private Registry fixture instructions.".to_owned(),
        BTreeMap::new(),
    )
    .expect("seal Skill");
    let signature = signing_key.sign(
        &package
            .publisher_signing_bytes()
            .expect("publisher signing bytes"),
    );
    SignedSkillPackage {
        package,
        signature: SkillSignature {
            key_id: "private-registry-publisher".to_owned(),
            ed25519: signature.to_bytes().to_vec(),
        },
        transparency: None,
    }
}

async fn serve_registry(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    responses: Vec<(String, Vec<u8>)>,
) -> Vec<String> {
    let mut requests = Vec::new();
    for (path, body) in responses {
        let (stream, _) = listener.accept().await.expect("accept Registry request");
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
        let request = String::from_utf8(request).expect("Registry request UTF-8");
        assert!(request.starts_with(&format!("GET {path} HTTP/1.1\r\n")));
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(headers.as_bytes())
            .await
            .expect("write Registry headers");
        stream.write_all(&body).await.expect("write Registry body");
        stream.shutdown().await.expect("close Registry response");
        requests.push(request);
    }
    requests
}

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

fn lower_sha256(bytes: &[u8]) -> String {
    lower_hex(&Sha256::digest(bytes))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn isolated_project() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "y-harness-private-registry-{}-{nonce}",
        std::process::id()
    ))
}

fn assert_tree_does_not_contain(root: &Path, needle: &[u8]) {
    for entry in fs::read_dir(root).expect("read project tree") {
        let entry = entry.expect("project entry");
        let path = entry.path();
        if path.is_dir() {
            assert_tree_does_not_contain(&path, needle);
        } else {
            let bytes = fs::read(&path).expect("read project file");
            assert!(
                !bytes.windows(needle.len()).any(|window| window == needle),
                "credential leaked into {}",
                path.display()
            );
        }
    }
}
