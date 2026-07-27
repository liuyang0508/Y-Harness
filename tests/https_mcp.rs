#![cfg(all(feature = "https-mcp", feature = "tls-host"))]

use std::{sync::Arc, time::Duration};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use y_harness::{
    HarnessFuture, HttpsJsonMcpClient, HttpsJsonMcpConfig, McpClient, SECRET_API_VERSION,
    SecretProvider, SecretProviderDescriptor, SecretReference, SecretRequest, SecretValue,
};

struct FixedSecret;

impl SecretProvider for FixedSecret {
    fn descriptor(&self) -> SecretProviderDescriptor {
        SecretProviderDescriptor {
            name: "fixed".to_owned(),
            description: "HTTPS MCP integration credential".to_owned(),
            api_version: SECRET_API_VERSION,
        }
    }

    fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async { SecretValue::new(b"fixture-token".to_vec()) })
    }
}

#[tokio::test]
async fn authenticated_private_https_mcp_json_round_trip() {
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
        .expect("bind HTTPS MCP fixture");
    let address = listener.local_addr().expect("HTTPS MCP fixture address");
    let server = tokio::spawn(serve_https_mcp(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        4,
    ));

    let reference = SecretReference::new("integration/https-mcp").expect("reference");
    let config = HttpsJsonMcpConfig::new(
        format!("https://127.0.0.1:{}/mcp", address.port()),
        reference,
    )
    .expect("base config")
    .with_limits(Duration::from_secs(5), Duration::from_secs(2), 1_048_576)
    .expect("bounded config")
    .with_exclusive_root_certificates_pem(ca.pem().into_bytes())
    .expect("private CA");
    let client = HttpsJsonMcpClient::new(config, Arc::new(FixedSecret)).expect("HTTPS MCP client");

    let tools = client.list_tools().await.expect("remote tool catalog");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "echo");
    let output = client
        .call_tool("echo", json!({"text": "remote"}))
        .await
        .expect("remote MCP tool");
    assert_eq!(output, json!({"text": "remote"}));
    client.shutdown().await.expect("shutdown HTTPS MCP");

    let methods = timeout(Duration::from_secs(5), server)
        .await
        .expect("HTTPS MCP fixture settlement")
        .expect("HTTPS MCP fixture task");
    assert_eq!(
        methods,
        [
            "initialize",
            "notifications/initialized",
            "tools/list",
            "tools/call"
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn service_doctor_registers_exact_remote_https_mcp_tools() {
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
        .expect("bind HTTPS MCP fixture");
    let address = listener.local_addr().expect("HTTPS MCP fixture address");
    let server = tokio::spawn(serve_https_mcp(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        3,
    ));

    let project = isolated_project();
    std::fs::create_dir_all(&project).expect("create service project");
    std::fs::write(project.join("mcp-ca.pem"), ca.pem()).expect("write MCP CA");
    let config = project.join("y-harness.json");
    std::fs::write(
        &config,
        format!(
            r#"{{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {{"type": "demo"}},
              "https_mcp_servers": [{{
                "id": "remote",
                "endpoint": "https://127.0.0.1:{}/mcp",
                "bearer_secret_reference": "mcp/remote",
                "bearer_environment": "YH_MCP_TOKEN",
                "request_timeout_ms": 5000,
                "connect_timeout_ms": 2000,
                "max_response_bytes": 1048576,
                "exclusive_root_ca_pem_path": "mcp-ca.pem",
                "tools": {{
                  "namespace": "remote",
                  "allow": ["echo"]
                }}
              }}]
            }}"#,
            address.port()
        ),
    )
    .expect("write service config");
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .env("YH_MCP_TOKEN", "fixture-token")
        .output()
        .expect("run doctor");
    assert!(
        output.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout).expect("doctor output");
    assert!(report.contains("tools: 2"));
    assert!(report.contains("mcp servers: 1 enabled / 1 configured"));
    assert!(report.contains("mcp command locks: 0 / 0 stdio enabled"));
    assert!(report.contains("status: ok"));

    let methods = timeout(Duration::from_secs(5), server)
        .await
        .expect("HTTPS MCP fixture settlement")
        .expect("HTTPS MCP fixture task");
    assert_eq!(
        methods,
        ["initialize", "notifications/initialized", "tools/list"]
    );
    std::fs::remove_dir_all(project).expect("remove service project");
}

async fn serve_https_mcp(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    expected_requests: usize,
) -> Vec<String> {
    let mut methods = Vec::new();
    while methods.len() < expected_requests {
        let (stream, _) = listener.accept().await.expect("accept HTTPS MCP");
        let mut stream = acceptor.accept(stream).await.expect("TLS handshake");
        let request = read_http_request(&mut stream).await;
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("HTTP header boundary");
        let headers = String::from_utf8(request[..header_end].to_vec()).expect("HTTP headers");
        assert!(headers.starts_with("POST /mcp HTTP/1.1\r\n"));
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("\r\nauthorization: bearer fixture-token\r\n")
        );
        assert!(
            headers
                .to_ascii_lowercase()
                .contains("\r\naccept: application/json\r\n")
        );
        let body = &request[header_end + 4..];
        let request: Value = serde_json::from_slice(body).expect("MCP request JSON");
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .expect("MCP method")
            .to_owned();
        let (status, response) = fixture_response(&method, &request);
        methods.push(method);
        write_http_response(&mut stream, status, response.as_deref()).await;
    }
    methods
}

fn fixture_response(method: &str, request: &Value) -> (&'static str, Option<Vec<u8>>) {
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let response = match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": request["params"]["protocolVersion"],
                "capabilities": {},
                "serverInfo": {"name": "fixture", "version": "1.0.0"}
            }
        })),
        "notifications/initialized" => None,
        "tools/list" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [{
                    "name": "echo",
                    "description": "Return the supplied text",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}}
                    }
                }]
            }
        })),
        "tools/call" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "content": [{"type": "text", "text": "remote"}],
                "structuredContent": {"text": "remote"},
                "isError": false
            }
        })),
        other => panic!("unexpected MCP method {other}"),
    };
    match response {
        Some(response) => (
            "200 OK",
            Some(serde_json::to_vec(&response).expect("MCP response JSON")),
        ),
        None => ("202 Accepted", None),
    }
}

async fn read_http_request(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 8_192];
    loop {
        let read = stream.read(&mut buffer).await.expect("read MCP request");
        assert!(read > 0, "request ended before its complete body");
        request.extend_from_slice(&buffer[..read]);
        assert!(request.len() <= 2_097_152, "test request exceeded bound");
        if complete_http_request_len(&request).is_some_and(|length| request.len() >= length) {
            return request;
        }
    }
}

async fn write_http_response(
    stream: &mut tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
    status: &str,
    body: Option<&[u8]>,
) {
    let body = body.unwrap_or_default();
    let headers = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .expect("write MCP response headers");
    stream
        .write_all(body)
        .await
        .expect("write MCP response body");
    stream.shutdown().await.expect("close MCP response");
}

fn complete_http_request_len(request: &[u8]) -> Option<usize> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let headers = std::str::from_utf8(&request[..header_end]).ok()?;
    let content_length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse::<usize>().ok())
            .flatten()
    })?;
    header_end.checked_add(4)?.checked_add(content_length)
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

fn isolated_project() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "y-harness-https-mcp-service-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ))
}
