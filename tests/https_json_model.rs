#![cfg(feature = "https-model")]

use std::{
    collections::BTreeMap,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

#[cfg(feature = "tls-host")]
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
    KeyPair, KeyUsagePurpose,
};
#[cfg(feature = "tls-host")]
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
#[cfg(feature = "tls-host")]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
#[cfg(feature = "tls-host")]
use tokio_rustls::TlsAcceptor;
use y_harness::{
    AllowListPolicy, EnvironmentSecretProvider, HarnessRuntime, HttpsJsonModel,
    HttpsJsonModelConfig, LanguageModel, MemoryEventStore, ModelEventSink, ModelOutput,
    ModelRequest, ModelStreamEvent, SecretReference, StateEngine, ThreadId, ToolRegistry,
    TurnExecutionOptions, TurnId,
};
#[cfg(feature = "tls-host")]
use y_harness::{
    HarnessFuture, MODEL_GATEWAY_API_VERSION, ModelResponse, SECRET_API_VERSION, SecretProvider,
    SecretProviderDescriptor, SecretRequest, SecretValue,
};

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<ModelStreamEvent>>,
}

#[cfg(feature = "tls-host")]
struct FixedSecret;

#[cfg(feature = "tls-host")]
impl SecretProvider for FixedSecret {
    fn descriptor(&self) -> SecretProviderDescriptor {
        SecretProviderDescriptor {
            name: "fixed".to_owned(),
            description: "Private gateway integration credential".to_owned(),
            api_version: SECRET_API_VERSION,
        }
    }

    fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
        Box::pin(async { SecretValue::new(b"fixture-token".to_vec()) })
    }
}

impl ModelEventSink for RecordingSink {
    fn emit(&self, event: &ModelStreamEvent) -> Result<(), String> {
        self.events
            .lock()
            .map_err(|_| "event sink poisoned".to_owned())?
            .push(event.clone());
        Ok(())
    }
}

#[cfg(feature = "tls-host")]
#[tokio::test]
async fn exclusive_private_ca_https_round_trip() {
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
        .expect("bind private gateway");
    let address = listener.local_addr().expect("private gateway address");
    let response = serde_json::to_vec(&ModelResponse::from(ModelOutput::Message {
        content: "private gateway".to_owned(),
    }))
    .expect("response");
    let server = tokio::spawn(serve_one_https_request(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        response,
    ));

    let reference = SecretReference::new("integration/private-model-gateway").expect("reference");
    let config = HttpsJsonModelConfig::new(
        format!("https://127.0.0.1:{}/v1/complete", address.port()),
        reference,
    )
    .expect("base config")
    .with_exclusive_root_certificates_pem(ca.pem().into_bytes())
    .expect("private CA");
    let model = HttpsJsonModel::new("integration/private-gateway", config, Arc::new(FixedSecret))
        .expect("model");
    let response = model
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::generate(),
            turn_id: TurnId::generate(),
            authority: y_harness::AuthorityContext::local_process(),
            items: Vec::new(),
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("private CA completion");
    assert_eq!(
        response.output,
        ModelOutput::Message {
            content: "private gateway".to_owned()
        }
    );

    let (request, handshake_failures) = server.await.expect("gateway task");
    assert_eq!(handshake_failures, 0);
    let request = String::from_utf8(request).expect("HTTP request");
    assert!(request.starts_with("POST /v1/complete HTTP/1.1\r\n"));
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nauthorization: bearer fixture-token\r\n")
    );
}

#[cfg(feature = "tls-host")]
#[tokio::test]
async fn private_model_gateway_requires_and_accepts_client_identity() {
    let (ca, issuer) = certificate_authority();
    let (server_certificate, server_key) =
        end_entity(&issuer, "127.0.0.1", ExtendedKeyUsagePurpose::ServerAuth);
    let (client_certificate, client_key) =
        end_entity(&issuer, "client", ExtendedKeyUsagePurpose::ClientAuth);
    let mut client_roots = RootCertStore::empty();
    client_roots.add(ca.der().clone()).expect("client CA");
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .expect("client verifier");
    let server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![server_certificate.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
        )
        .expect("server identity");
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind private gateway");
    let address = listener.local_addr().expect("private gateway address");
    let response = serde_json::to_vec(&ModelResponse::from(ModelOutput::Message {
        content: "mutually authenticated".to_owned(),
    }))
    .expect("response");
    let server = tokio::spawn(serve_one_https_request(
        listener,
        TlsAcceptor::from(Arc::new(server_config)),
        response,
    ));

    let endpoint = format!("https://127.0.0.1:{}/v1/complete", address.port());
    let reference = SecretReference::new("integration/private-mtls-gateway").expect("reference");
    let config = HttpsJsonModelConfig::new(endpoint, reference)
        .expect("base config")
        .with_exclusive_root_certificates_pem(ca.pem().into_bytes())
        .expect("private CA");
    let unauthenticated = HttpsJsonModel::new(
        "integration/private-gateway-without-identity",
        config.clone(),
        Arc::new(FixedSecret),
    )
    .expect("unauthenticated model");
    assert!(
        unauthenticated
            .complete(ModelRequest {
                thread_id: ThreadId::generate(),
                turn_id: TurnId::generate(),
                authority: y_harness::AuthorityContext::local_process(),
                items: Vec::new(),
                context: Vec::new(),
                tools: Vec::new(),
            })
            .await
            .is_err()
    );

    let mut identity_pem = client_certificate.pem().into_bytes();
    identity_pem.extend_from_slice(client_key.serialize_pem().as_bytes());
    let authenticated = HttpsJsonModel::new_with_client_identity(
        "integration/private-mtls-gateway",
        config,
        Arc::new(FixedSecret),
        SecretValue::new(identity_pem).expect("bounded identity"),
    )
    .expect("mTLS model");
    let response = authenticated
        .complete(ModelRequest {
            thread_id: ThreadId::generate(),
            turn_id: TurnId::generate(),
            authority: y_harness::AuthorityContext::local_process(),
            items: Vec::new(),
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("mTLS completion");
    assert_eq!(
        response,
        ModelOutput::Message {
            content: "mutually authenticated".to_owned()
        }
    );
    let (_, handshake_failures) = server.await.expect("gateway task");
    assert_eq!(handshake_failures, 1);
}

#[tokio::test]
#[ignore = "requires YH_HTTPS_MODEL_ENDPOINT and YH_HTTPS_MODEL_TOKEN for a compatible TLS gateway"]
async fn authenticated_https_gateway_round_trip() {
    let model = gateway_model();

    let response = model
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::generate(),
            turn_id: TurnId::generate(),
            authority: y_harness::AuthorityContext::local_process(),
            items: Vec::new(),
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("gateway completion");

    match response.output {
        ModelOutput::Message { content } => assert!(!content.trim().is_empty()),
        ModelOutput::ToolCall { call_id, name, .. } => {
            assert!(!call_id.trim().is_empty());
            assert!(!name.trim().is_empty());
        }
        ModelOutput::ToolCalls { calls } => {
            assert!(calls.len() >= 2);
            assert!(
                calls.iter().all(|call| {
                    !call.call_id.trim().is_empty() && !call.name.trim().is_empty()
                })
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires a compatible TLS gateway that implements bounded NDJSON streaming"]
async fn authenticated_https_gateway_streaming_round_trip() {
    let sink = Arc::new(RecordingSink::default());
    let runtime = HarnessRuntime::new(
        Arc::new(gateway_model()),
        ToolRegistry::new(),
        Arc::new(AllowListPolicy::deny_by_default()),
        StateEngine::new(Arc::new(MemoryEventStore::new())),
    );
    let thread = runtime.create_thread().await.expect("create thread");
    let outcome = runtime
        .run_turn_with_options(
            &thread.id,
            "stream one short response",
            TurnExecutionOptions {
                timeout: Some(Duration::from_secs(30)),
                model_event_sink: Some(sink.clone()),
                ..TurnExecutionOptions::default()
            },
        )
        .await
        .expect("streaming gateway turn");
    assert!(!outcome.final_text.trim().is_empty());
    assert!(sink.events.lock().expect("events").iter().any(
        |event| matches!(event, ModelStreamEvent::TextDelta { delta, .. } if !delta.is_empty())
    ));
}

fn gateway_model() -> HttpsJsonModel {
    let endpoint = env::var("YH_HTTPS_MODEL_ENDPOINT").expect("YH_HTTPS_MODEL_ENDPOINT");
    let reference = SecretReference::new("integration/model-gateway").expect("reference");
    let secrets = EnvironmentSecretProvider::new(
        "integration-environment",
        BTreeMap::from([(reference.clone(), "YH_HTTPS_MODEL_TOKEN".to_owned())]),
    )
    .expect("secret provider");
    HttpsJsonModel::new(
        "integration/gateway",
        HttpsJsonModelConfig::new(endpoint, reference).expect("config"),
        Arc::new(secrets),
    )
    .expect("model")
}

#[cfg(feature = "tls-host")]
async fn serve_one_https_request(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    response: Vec<u8>,
) -> (Vec<u8>, usize) {
    let mut handshake_failures = 0;
    let mut stream = loop {
        let (stream, _) = listener.accept().await.expect("accept private gateway");
        match acceptor.accept(stream).await {
            Ok(stream) => break stream,
            Err(_) => handshake_failures += 1,
        }
    };
    let mut request = Vec::new();
    let mut buffer = [0_u8; 4_096];
    loop {
        let read = stream.read(&mut buffer).await.expect("read request");
        assert!(read > 0, "request ended before its complete body");
        request.extend_from_slice(&buffer[..read]);
        assert!(request.len() <= 1_048_576, "test request exceeded bound");
        if complete_http_request_len(&request).is_some_and(|length| request.len() >= length) {
            break;
        }
    }
    let headers = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-y-harness-model-api: {MODEL_GATEWAY_API_VERSION}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response.len()
    );
    stream
        .write_all(headers.as_bytes())
        .await
        .expect("write response headers");
    stream
        .write_all(&response)
        .await
        .expect("write response body");
    stream.shutdown().await.expect("close response");
    (request, handshake_failures)
}

#[cfg(feature = "tls-host")]
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
    end_entity(issuer, "127.0.0.1", ExtendedKeyUsagePurpose::ServerAuth)
}

#[cfg(feature = "tls-host")]
fn end_entity(
    issuer: &Issuer<'static, KeyPair>,
    name: &str,
    usage: ExtendedKeyUsagePurpose,
) -> (Certificate, KeyPair) {
    let mut parameters =
        CertificateParams::new(vec![name.to_owned()]).expect("end-entity parameters");
    parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    parameters.extended_key_usages = vec![usage];
    let key = KeyPair::generate().expect("server key");
    let certificate = parameters.signed_by(&key, issuer).expect("end entity");
    (certificate, key)
}
