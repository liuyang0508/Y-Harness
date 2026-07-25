//! Mutually authenticated TLS host for the existing typed JSONL protocol.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};
use tokio::{
    io::{AsyncReadExt, BufReader, split},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinError, JoinSet},
};
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

use crate::{
    CancellationToken, HarnessError, ProtocolHandler, ProtocolPrincipal,
    protocol::serve_jsonl_with_limits,
};

const MAX_TLS_FILE_BYTES: usize = 1_048_576;
const MAX_CERTIFICATES: usize = 64;
const MAX_TLS_CONNECTIONS: usize = 10_000;
const MAX_TLS_SESSION_FRAMES: usize = 1_000_000;
const MAX_TLS_TIMEOUT: Duration = Duration::from_secs(3_600);
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_OPERATION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_CONNECTIONS: usize = 256;
const DEFAULT_MAX_SESSION_FRAMES: usize = 10_000;

/// Validated operator configuration for a mutually authenticated TLS host.
#[derive(Clone, Debug)]
pub struct TlsJsonlServerConfig {
    /// TCP address selected by the embedding host.
    pub bind_address: SocketAddr,
    /// PEM certificate chain presented by the server.
    pub certificate_chain_pem: PathBuf,
    /// PEM private key corresponding to the server certificate.
    pub private_key_pem: PathBuf,
    /// PEM trust roots accepted for mandatory client certificates.
    pub client_ca_pem: PathBuf,
    /// Maximum simultaneous TLS sessions.
    pub max_connections: usize,
    /// Maximum TLS handshake duration.
    pub handshake_timeout: Duration,
    /// Maximum time between protocol frames.
    pub idle_timeout: Duration,
    /// Maximum protocol frames accepted before reconnect is required.
    pub max_session_frames: usize,
    /// Maximum graceful wait for accepted protocol Operations during shutdown.
    pub operation_shutdown_timeout: Duration,
}

impl TlsJsonlServerConfig {
    /// Creates a configuration with bounded production defaults.
    pub fn new(
        bind_address: SocketAddr,
        certificate_chain_pem: PathBuf,
        private_key_pem: PathBuf,
        client_ca_pem: PathBuf,
    ) -> Result<Self, HarnessError> {
        let config = Self {
            bind_address,
            certificate_chain_pem,
            private_key_pem,
            client_ca_pem,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_session_frames: DEFAULT_MAX_SESSION_FRAMES,
            operation_shutdown_timeout: DEFAULT_OPERATION_SHUTDOWN_TIMEOUT,
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces connection and timeout bounds.
    pub fn with_limits(
        mut self,
        max_connections: usize,
        handshake_timeout: Duration,
        idle_timeout: Duration,
        max_session_frames: usize,
    ) -> Result<Self, HarnessError> {
        self.max_connections = max_connections;
        self.handshake_timeout = handshake_timeout;
        self.idle_timeout = idle_timeout;
        self.max_session_frames = max_session_frames;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the graceful protocol Operation drain deadline.
    pub fn with_operation_shutdown_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, HarnessError> {
        self.operation_shutdown_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), HarnessError> {
        for (label, path) in [
            ("certificate chain", &self.certificate_chain_pem),
            ("private key", &self.private_key_pem),
            ("client CA", &self.client_ca_pem),
        ] {
            if !path.is_absolute() {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "TLS {label} path must be absolute"
                )));
            }
        }
        if !(1..=MAX_TLS_CONNECTIONS).contains(&self.max_connections) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "TLS max_connections must be 1-{MAX_TLS_CONNECTIONS}"
            )));
        }
        if !(1..=MAX_TLS_SESSION_FRAMES).contains(&self.max_session_frames) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "TLS max_session_frames must be 1-{MAX_TLS_SESSION_FRAMES}"
            )));
        }
        for (label, timeout) in [
            ("handshake", self.handshake_timeout),
            ("idle", self.idle_timeout),
            ("Operation shutdown", self.operation_shutdown_timeout),
        ] {
            if timeout < Duration::from_millis(1) || timeout > MAX_TLS_TIMEOUT {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "TLS {label} timeout must be 1 millisecond to {} seconds",
                    MAX_TLS_TIMEOUT.as_secs()
                )));
            }
        }
        Ok(())
    }
}

/// Content-free settlement counters returned when a TLS host shuts down.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TlsJsonlServerReport {
    /// TCP sockets accepted before shutdown.
    pub accepted_connections: u64,
    /// Sockets closed because the concurrency limit was full.
    pub capacity_rejections: u64,
    /// TLS handshakes rejected, failed, or timed out.
    pub handshake_failures: u64,
    /// Authenticated sessions that reached clean EOF.
    pub completed_sessions: u64,
    /// Authenticated sessions closed by framing, I/O, or idle failure.
    pub session_failures: u64,
    /// Connection tasks that panicked.
    pub task_panics: u64,
    /// Active sessions aborted during host shutdown.
    pub shutdown_aborts: u64,
    /// Running protocol Operations asked to cancel during host shutdown.
    pub operation_cancellations: u64,
    /// Cancelled Operations that reached a process-local terminal status.
    pub operation_settlements: u64,
    /// Operations still running when the configured drain deadline elapsed.
    pub operation_shutdown_timeouts: u64,
    /// Whether Runtime snapshot maintenance drained before the same deadline.
    pub background_work_drained: bool,
}

/// Bound mTLS listener that serves the same [`ProtocolHandler`] as stdio.
pub struct TlsJsonlServer {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    handler: Arc<ProtocolHandler>,
    connections: Arc<Semaphore>,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    max_session_frames: usize,
    operation_shutdown_timeout: Duration,
}

impl TlsJsonlServer {
    /// Loads bounded PEM material, configures mandatory client authentication,
    /// and binds the TCP listener.
    pub async fn bind(
        config: TlsJsonlServerConfig,
        handler: Arc<ProtocolHandler>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let tls = load_server_config(&config).await?;
        let listener = TcpListener::bind(config.bind_address)
            .await
            .map_err(|_| HarnessError::Protocol("failed to bind TLS listener".to_owned()))?;
        Ok(Self {
            listener,
            acceptor: TlsAcceptor::from(Arc::new(tls)),
            handler,
            connections: Arc::new(Semaphore::new(config.max_connections)),
            handshake_timeout: config.handshake_timeout,
            idle_timeout: config.idle_timeout,
            max_session_frames: config.max_session_frames,
            operation_shutdown_timeout: config.operation_shutdown_timeout,
        })
    }

    /// Returns the actual bound address, including an assigned ephemeral port.
    pub fn local_addr(&self) -> Result<SocketAddr, HarnessError> {
        self.listener
            .local_addr()
            .map_err(|_| HarnessError::Protocol("failed to read TLS listener address".to_owned()))
    }

    /// Accepts sessions until cooperative shutdown and then closes all active
    /// connections before returning content-free settlement counters.
    pub async fn serve(
        self,
        shutdown: CancellationToken,
    ) -> Result<TlsJsonlServerReport, HarnessError> {
        let mut tasks = JoinSet::new();
        let mut report = TlsJsonlServerReport::default();
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(joined) = joined {
                        record_connection_result(&mut report, joined);
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.map_err(|_| {
                        HarnessError::Protocol("TLS listener accept failed".to_owned())
                    })?;
                    report.accepted_connections =
                        report.accepted_connections.saturating_add(1);
                    let Ok(permit) = self.connections.clone().try_acquire_owned() else {
                        report.capacity_rejections =
                            report.capacity_rejections.saturating_add(1);
                        drop(stream);
                        continue;
                    };
                    let acceptor = self.acceptor.clone();
                    let handler = self.handler.clone();
                    let handshake_timeout = self.handshake_timeout;
                    let idle_timeout = self.idle_timeout;
                    let max_session_frames = self.max_session_frames;
                    tasks.spawn(async move {
                        let _permit = permit;
                        serve_connection(
                            acceptor,
                            handler,
                            stream,
                            handshake_timeout,
                            idle_timeout,
                            max_session_frames,
                        )
                        .await
                    });
                }
            }
        }
        while let Some(joined) = tasks.try_join_next() {
            record_connection_result(&mut report, joined);
        }
        tasks.abort_all();
        while let Some(joined) = tasks.join_next().await {
            if joined.as_ref().is_err_and(JoinError::is_cancelled) {
                continue;
            }
            record_connection_result(&mut report, joined);
        }
        let operation_report = self
            .handler
            .shutdown(self.operation_shutdown_timeout)
            .await?;
        report.operation_cancellations = operation_report.cancellation_requests;
        report.operation_settlements = operation_report.settled_operations;
        report.operation_shutdown_timeouts = operation_report.remaining_operations;
        report.background_work_drained = operation_report.background_work_drained;
        Ok(report)
    }
}

#[derive(Clone, Copy)]
enum ConnectionOutcome {
    Completed,
    HandshakeFailed,
    SessionFailed,
}

async fn serve_connection(
    acceptor: TlsAcceptor,
    handler: Arc<ProtocolHandler>,
    stream: TcpStream,
    handshake_timeout: Duration,
    idle_timeout: Duration,
    max_session_frames: usize,
) -> ConnectionOutcome {
    let tls = match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(_)) | Err(_) => return ConnectionOutcome::HandshakeFailed,
    };
    let Some(certificate) = tls
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|certificates| certificates.first())
    else {
        return ConnectionOutcome::HandshakeFailed;
    };
    let principal = ProtocolPrincipal::from_mtls_certificate(certificate.as_ref());
    let (reader, writer) = split(tls);
    match serve_jsonl_with_limits(
        &handler,
        &principal,
        BufReader::new(reader),
        writer,
        Some(idle_timeout),
        Some(max_session_frames),
    )
    .await
    {
        Ok(()) => ConnectionOutcome::Completed,
        Err(_) => ConnectionOutcome::SessionFailed,
    }
}

fn record_connection_result(
    report: &mut TlsJsonlServerReport,
    result: Result<ConnectionOutcome, JoinError>,
) {
    match result {
        Ok(ConnectionOutcome::Completed) => {
            report.completed_sessions = report.completed_sessions.saturating_add(1);
        }
        Ok(ConnectionOutcome::HandshakeFailed) => {
            report.handshake_failures = report.handshake_failures.saturating_add(1);
        }
        Ok(ConnectionOutcome::SessionFailed) => {
            report.session_failures = report.session_failures.saturating_add(1);
        }
        Err(error) if error.is_panic() => {
            report.task_panics = report.task_panics.saturating_add(1);
        }
        Err(error) if error.is_cancelled() => {
            report.shutdown_aborts = report.shutdown_aborts.saturating_add(1);
        }
        Err(_) => {}
    }
}

async fn load_server_config(config: &TlsJsonlServerConfig) -> Result<ServerConfig, HarnessError> {
    let certificate_chain =
        load_certificates(&config.certificate_chain_pem, "server certificate").await?;
    let client_ca = load_certificates(&config.client_ca_pem, "client CA").await?;
    let private_key = load_private_key(&config.private_key_pem).await?;

    let mut client_roots = RootCertStore::empty();
    for certificate in client_ca {
        client_roots.add(certificate).map_err(|_| {
            HarnessError::InvalidConfiguration("TLS client CA is invalid".to_owned())
        })?;
    }
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots))
        .build()
        .map_err(|_| {
            HarnessError::InvalidConfiguration("TLS client verifier is invalid".to_owned())
        })?;
    ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certificate_chain, private_key)
        .map_err(|_| {
            HarnessError::InvalidConfiguration(
                "TLS server certificate and private key do not match".to_owned(),
            )
        })
}

async fn load_certificates(
    path: &Path,
    label: &str,
) -> Result<Vec<CertificateDer<'static>>, HarnessError> {
    let bytes = read_bounded_tls_file(path, label).await?;
    let certificates = CertificateDer::pem_slice_iter(bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| HarnessError::InvalidConfiguration(format!("TLS {label} PEM is invalid")))?;
    if certificates.is_empty() || certificates.len() > MAX_CERTIFICATES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "TLS {label} must contain 1-{MAX_CERTIFICATES} certificates"
        )));
    }
    Ok(certificates)
}

async fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, HarnessError> {
    let bytes = read_bounded_tls_file(path, "private key").await?;
    PrivateKeyDer::from_pem_slice(bytes.as_slice()).map_err(|_| {
        HarnessError::InvalidConfiguration("TLS private key PEM is invalid or empty".to_owned())
    })
}

async fn read_bounded_tls_file(
    path: &Path,
    label: &str,
) -> Result<Zeroizing<Vec<u8>>, HarnessError> {
    let file = tokio::fs::File::open(path).await.map_err(|_| {
        HarnessError::InvalidConfiguration(format!("failed to read TLS {label} file"))
    })?;
    let mut reader = file.take((MAX_TLS_FILE_BYTES as u64).saturating_add(1));
    let mut bytes = Zeroizing::new(Vec::with_capacity(8_192));
    reader.read_to_end(&mut bytes).await.map_err(|_| {
        HarnessError::InvalidConfiguration(format!("failed to read TLS {label} file"))
    })?;
    if bytes.is_empty() || bytes.len() > MAX_TLS_FILE_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "TLS {label} file must be 1-{MAX_TLS_FILE_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write as _,
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use rcgen::{
        BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::TcpStream,
    };
    use tokio_rustls::TlsConnector;

    use super::{MAX_TLS_FILE_BYTES, TlsJsonlServer, TlsJsonlServerConfig, read_bounded_tls_file};
    use crate::{
        AllowListPolicy, CancellationToken, FingerprintProtocolAuthorizer, HarnessFuture,
        HarnessRuntime, LanguageModel, MemoryEventStore, ModelOutput, ModelRequest,
        PROTOCOL_VERSION, ProtocolCommand, ProtocolHandler, ProtocolPrincipal, ProtocolRequest,
        ProtocolResponse, ProtocolResponseBody, ProtocolResult, StateEngine, ToolRegistry,
    };

    struct ImmediateModel;
    static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    impl LanguageModel for ImmediateModel {
        fn id(&self) -> &str {
            "test/tls"
        }

        fn complete<'a>(&'a self, _request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async {
                Ok(ModelOutput::Message {
                    content: "done".to_owned(),
                })
            })
        }
    }

    #[tokio::test]
    async fn tls_material_is_bounded_during_the_read() {
        let directory = isolated_directory();
        fs::create_dir_all(&directory).expect("directory");
        let path = directory.join("oversized.pem");
        write_file(&path, &vec![b'x'; MAX_TLS_FILE_BYTES + 1]);
        assert!(
            read_bounded_tls_file(&path, "fixture").await.is_err(),
            "oversized TLS material must fail before an unbounded read"
        );
        fs::remove_dir_all(directory).expect("remove fixture");
    }

    #[tokio::test]
    async fn requires_client_certificate_and_reuses_typed_protocol() {
        let directory = isolated_directory();
        fs::create_dir_all(&directory).expect("directory");
        let (ca, issuer) = certificate_authority();
        let (server_certificate, server_key) =
            end_entity(&issuer, "localhost", ExtendedKeyUsagePurpose::ServerAuth);
        let (client_certificate, client_key) =
            end_entity(&issuer, "client", ExtendedKeyUsagePurpose::ClientAuth);
        let certificate_path = directory.join("server-cert.pem");
        let key_path = directory.join("server-key.pem");
        let ca_path = directory.join("client-ca.pem");
        write_file(&certificate_path, server_certificate.pem().as_bytes());
        write_file(&key_path, server_key.serialize_pem().as_bytes());
        write_file(&ca_path, ca.pem().as_bytes());

        let client_principal =
            ProtocolPrincipal::from_mtls_certificate(client_certificate.der().as_ref());
        let authorizer = FingerprintProtocolAuthorizer::allow_all(vec![
            client_principal
                .mtls_sha256()
                .expect("mTLS fingerprint")
                .to_owned(),
        ])
        .expect("authorizer");
        let handler = Arc::new(
            ProtocolHandler::new(Arc::new(HarnessRuntime::new(
                Arc::new(ImmediateModel),
                ToolRegistry::new(),
                Arc::new(AllowListPolicy::deny_by_default()),
                StateEngine::new(Arc::new(MemoryEventStore::new())),
            )))
            .with_authorizer(Arc::new(authorizer)),
        );
        let server = TlsJsonlServer::bind(
            TlsJsonlServerConfig::new(
                "127.0.0.1:0".parse().expect("address"),
                certificate_path,
                key_path,
                ca_path,
            )
            .expect("config"),
            handler,
        )
        .await
        .expect("bind");
        let address = server.local_addr().expect("local address");
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_task = tokio::spawn(server.serve(server_shutdown));

        let mut roots = RootCertStore::empty();
        roots.add(ca.der().clone()).expect("root");
        let unauthenticated = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots.clone())
                .with_no_client_auth(),
        );
        let rejected_stream = TcpStream::connect(address).await.expect("connect rejected");
        let rejected = TlsConnector::from(unauthenticated)
            .connect(
                ServerName::try_from("localhost").expect("server name"),
                rejected_stream,
            )
            .await;
        if let Ok(mut rejected) = rejected {
            let _ = rejected.write_all(b"unauthenticated\n").await;
            let mut byte = [0_u8; 1];
            let read = tokio::time::timeout(Duration::from_secs(1), rejected.read(&mut byte)).await;
            assert!(
                !matches!(read, Ok(Ok(1))),
                "unauthenticated client received application data"
            );
        }

        let client_config = Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    vec![client_certificate.der().clone()],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client_key.serialize_der())),
                )
                .expect("client identity"),
        );
        let stream = TcpStream::connect(address).await.expect("connect");
        let mut stream = TlsConnector::from(client_config)
            .connect(
                ServerName::try_from("localhost").expect("server name"),
                stream,
            )
            .await
            .expect("mTLS");
        let request = ProtocolRequest {
            id: "initialize".to_owned(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command: ProtocolCommand::Initialize {},
        };
        let mut encoded = serde_json::to_vec(&request).expect("request");
        encoded.push(b'\n');
        stream.write_all(&encoded).await.expect("write");
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .expect("response timeout")
            .expect("response");
        let response: ProtocolResponse = serde_json::from_str(&line).expect("response JSON");
        assert!(matches!(
            response.body,
            ProtocolResponseBody::Success {
                result: ProtocolResult::Initialized { .. }
            }
        ));
        drop(reader);

        shutdown.cancel();
        let report = server_task.await.expect("server task").expect("server");
        let _ = fs::remove_dir_all(&directory);
        assert!(report.accepted_connections >= 2);
        assert!(report.handshake_failures >= 1);
        assert_eq!(report.task_panics, 0);
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

    fn end_entity(
        issuer: &Issuer<'static, KeyPair>,
        name: &str,
        usage: ExtendedKeyUsagePurpose,
    ) -> (Certificate, KeyPair) {
        let mut parameters =
            CertificateParams::new(vec![name.to_owned()]).expect("certificate parameters");
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![usage];
        let key = KeyPair::generate().expect("key");
        let certificate = parameters.signed_by(&key, issuer).expect("certificate");
        (certificate, key)
    }

    fn isolated_directory() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let fixture_id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "y-harness-tls-{}-{nonce}-{fixture_id}",
            std::process::id()
        ))
    }

    fn write_file(path: &Path, contents: &[u8]) {
        let mut file = fs::File::create(path).expect("create fixture");
        file.write_all(contents).expect("write fixture");
        file.sync_all().expect("sync fixture");
    }
}
