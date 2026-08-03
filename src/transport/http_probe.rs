//! Bounded HTTP health probes derived from authoritative Protocol admission.

use std::{net::SocketAddr, str, sync::Arc, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::{JoinError, JoinSet},
    time::timeout,
};

use crate::{
    CancellationToken, HarnessError, HarnessFuture, ProtocolAdmissionState, ProtocolHandler,
    ProtocolServiceStatus,
};

const MAX_CONNECTIONS: usize = 10_000;
const MAX_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REQUEST_HEADER_BYTES: usize = 8_192;
const DEFAULT_MAX_CONNECTIONS: usize = 64;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_STATUS_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Supplies the content-free admission status consumed by deployment probes.
///
/// Implementations must report the same authority used to admit new work. A
/// probe adapter must never infer readiness by opening durable stores or
/// independently testing downstream providers.
pub trait ServiceStatusSource: Send + Sync {
    /// Returns one current content-free admission projection.
    fn service_status<'a>(&'a self) -> HarnessFuture<'a, ProtocolServiceStatus>;
}

impl ServiceStatusSource for ProtocolHandler {
    fn service_status<'a>(&'a self) -> HarnessFuture<'a, ProtocolServiceStatus> {
        Box::pin(async move { Ok(ProtocolHandler::service_status(self).await) })
    }
}

/// Validated limits for one unauthenticated, content-free probe listener.
#[derive(Clone, Copy, Debug)]
pub struct HttpProbeServerConfig {
    /// TCP address selected by the embedding host. Examples use loopback; a
    /// non-loopback address is an explicit deployment choice.
    pub bind_address: SocketAddr,
    /// Maximum simultaneous probe connections.
    pub max_connections: usize,
    /// Maximum time for each request read or response write.
    pub request_timeout: Duration,
    /// Maximum time to obtain the authoritative status projection.
    pub status_timeout: Duration,
    /// Maximum graceful wait for accepted probe connections during shutdown.
    pub shutdown_timeout: Duration,
}

impl HttpProbeServerConfig {
    /// Creates a configuration for one explicit address with bounded defaults.
    pub fn new(bind_address: SocketAddr) -> Result<Self, HarnessError> {
        let config = Self {
            bind_address,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            status_timeout: DEFAULT_STATUS_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces every resource and time bound.
    pub fn with_limits(
        mut self,
        max_connections: usize,
        request_timeout: Duration,
        status_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Result<Self, HarnessError> {
        self.max_connections = max_connections;
        self.request_timeout = request_timeout;
        self.status_timeout = status_timeout;
        self.shutdown_timeout = shutdown_timeout;
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if !(1..=MAX_CONNECTIONS).contains(&self.max_connections) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "HTTP probe max_connections must be 1-{MAX_CONNECTIONS}"
            )));
        }
        for (label, value) in [
            ("request", self.request_timeout),
            ("status", self.status_timeout),
            ("shutdown", self.shutdown_timeout),
        ] {
            if value < Duration::from_millis(1) || value > MAX_TIMEOUT {
                return Err(HarnessError::InvalidConfiguration(format!(
                    "HTTP probe {label} timeout must be 1 millisecond to {} seconds",
                    MAX_TIMEOUT.as_secs()
                )));
            }
        }
        Ok(())
    }
}

/// Content-free counters returned after a probe listener drains.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HttpProbeServerReport {
    /// TCP sockets accepted before shutdown.
    pub accepted_connections: u64,
    /// Sockets closed because the concurrency limit was full.
    pub capacity_rejections: u64,
    /// Successful liveness or readiness responses.
    pub successful_responses: u64,
    /// Valid readiness requests answered unavailable.
    pub unready_responses: u64,
    /// Requests rejected as malformed, unsupported, or unknown.
    pub invalid_requests: u64,
    /// Requests that timed out, lost I/O, or could not obtain status.
    pub failed_requests: u64,
    /// Connection tasks that panicked.
    pub task_panics: u64,
    /// Active connection tasks aborted after the shutdown deadline.
    pub shutdown_aborts: u64,
}

/// Bound HTTP listener for Kubernetes-style liveness and readiness probes.
///
/// This adapter serves only `GET /livez` and `GET /readyz`, closes every
/// connection after one response, and never exposes prompts, identifiers,
/// credentials, or downstream dependency diagnostics.
pub struct HttpProbeServer {
    listener: TcpListener,
    source: Arc<dyn ServiceStatusSource>,
    connections: Arc<Semaphore>,
    request_timeout: Duration,
    status_timeout: Duration,
    shutdown_timeout: Duration,
}

impl HttpProbeServer {
    /// Validates limits and binds the requested TCP listener.
    pub async fn bind(
        config: HttpProbeServerConfig,
        source: Arc<dyn ServiceStatusSource>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let listener = TcpListener::bind(config.bind_address)
            .await
            .map_err(|_| HarnessError::Protocol("failed to bind HTTP probe listener".to_owned()))?;
        Ok(Self {
            listener,
            source,
            connections: Arc::new(Semaphore::new(config.max_connections)),
            request_timeout: config.request_timeout,
            status_timeout: config.status_timeout,
            shutdown_timeout: config.shutdown_timeout,
        })
    }

    /// Returns the actual bound address, including an assigned ephemeral port.
    pub fn local_addr(&self) -> Result<SocketAddr, HarnessError> {
        self.listener.local_addr().map_err(|_| {
            HarnessError::Protocol("failed to read HTTP probe listener address".to_owned())
        })
    }

    /// Accepts bounded one-shot requests until cooperative shutdown.
    ///
    /// Shutting down the probe never shuts down the Engine. The embedding host
    /// owns Engine lifecycle and may keep the probe alive while the authoritative
    /// status transitions to `draining`.
    pub async fn serve(
        self,
        shutdown: CancellationToken,
    ) -> Result<HttpProbeServerReport, HarnessError> {
        let mut tasks = JoinSet::new();
        let mut report = HttpProbeServerReport::default();
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
                        HarnessError::Protocol("HTTP probe listener accept failed".to_owned())
                    })?;
                    report.accepted_connections =
                        report.accepted_connections.saturating_add(1);
                    let Ok(permit) = self.connections.clone().try_acquire_owned() else {
                        report.capacity_rejections =
                            report.capacity_rejections.saturating_add(1);
                        drop(stream);
                        continue;
                    };
                    let source = self.source.clone();
                    let request_timeout = self.request_timeout;
                    let status_timeout = self.status_timeout;
                    tasks.spawn(async move {
                        let _permit = permit;
                        serve_connection(stream, source, request_timeout, status_timeout).await
                    });
                }
            }
        }

        let drained = timeout(self.shutdown_timeout, async {
            while let Some(joined) = tasks.join_next().await {
                record_connection_result(&mut report, joined);
            }
        })
        .await
        .is_ok();
        if !drained {
            tasks.abort_all();
            while let Some(joined) = tasks.join_next().await {
                record_connection_result(&mut report, joined);
            }
        }
        Ok(report)
    }
}

#[derive(Clone, Copy)]
enum ConnectionOutcome {
    Successful,
    Unready,
    Invalid,
    Failed,
}

fn record_connection_result(
    report: &mut HttpProbeServerReport,
    result: Result<ConnectionOutcome, JoinError>,
) {
    match result {
        Ok(ConnectionOutcome::Successful) => {
            report.successful_responses = report.successful_responses.saturating_add(1);
        }
        Ok(ConnectionOutcome::Unready) => {
            report.unready_responses = report.unready_responses.saturating_add(1);
        }
        Ok(ConnectionOutcome::Invalid) => {
            report.invalid_requests = report.invalid_requests.saturating_add(1);
        }
        Ok(ConnectionOutcome::Failed) => {
            report.failed_requests = report.failed_requests.saturating_add(1);
        }
        Err(error) if error.is_panic() => {
            report.task_panics = report.task_panics.saturating_add(1);
        }
        Err(error) if error.is_cancelled() => {
            report.shutdown_aborts = report.shutdown_aborts.saturating_add(1);
        }
        Err(_) => {
            report.failed_requests = report.failed_requests.saturating_add(1);
        }
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    source: Arc<dyn ServiceStatusSource>,
    request_timeout: Duration,
    status_timeout: Duration,
) -> ConnectionOutcome {
    let request = match timeout(request_timeout, read_request(&mut stream)).await {
        Ok(Ok(request)) => request,
        Ok(Err(RequestError::TooLarge)) => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::request_header_too_large(),
                ConnectionOutcome::Invalid,
            )
            .await;
        }
        Ok(Err(RequestError::Malformed)) => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::bad_request(),
                ConnectionOutcome::Invalid,
            )
            .await;
        }
        Err(_) => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::request_timeout(),
                ConnectionOutcome::Failed,
            )
            .await;
        }
    };

    let endpoint = match request {
        Request::Live => ProbeEndpoint::Live,
        Request::Ready => ProbeEndpoint::Ready,
        Request::MethodNotAllowed => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::method_not_allowed(),
                ConnectionOutcome::Invalid,
            )
            .await;
        }
        Request::NotFound => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::not_found(),
                ConnectionOutcome::Invalid,
            )
            .await;
        }
    };

    let status = match timeout(status_timeout, source.service_status()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) | Err(_) => {
            return write_outcome(
                &mut stream,
                request_timeout,
                Response::service_unavailable("unavailable\n"),
                ConnectionOutcome::Failed,
            )
            .await;
        }
    };
    let (response, outcome) = match endpoint {
        ProbeEndpoint::Live => (Response::ok("live\n"), ConnectionOutcome::Successful),
        ProbeEndpoint::Ready => match status.admission {
            ProtocolAdmissionState::Ready => {
                (Response::ok("ready\n"), ConnectionOutcome::Successful)
            }
            ProtocolAdmissionState::AtCapacity => (
                Response::service_unavailable("at_capacity\n"),
                ConnectionOutcome::Unready,
            ),
            ProtocolAdmissionState::Draining => (
                Response::service_unavailable("draining\n"),
                ConnectionOutcome::Unready,
            ),
        },
    };
    write_outcome(&mut stream, request_timeout, response, outcome).await
}

async fn write_outcome(
    stream: &mut TcpStream,
    write_timeout: Duration,
    response: Response,
    outcome: ConnectionOutcome,
) -> ConnectionOutcome {
    match timeout(write_timeout, response.write(stream)).await {
        Ok(Ok(())) => outcome,
        Ok(Err(_)) | Err(_) => ConnectionOutcome::Failed,
    }
}

#[derive(Clone, Copy)]
enum ProbeEndpoint {
    Live,
    Ready,
}

enum Request {
    Live,
    Ready,
    MethodNotAllowed,
    NotFound,
}

enum RequestError {
    Malformed,
    TooLarge,
}

async fn read_request(stream: &mut TcpStream) -> Result<Request, RequestError> {
    let mut encoded = Vec::with_capacity(1_024);
    let mut chunk = [0_u8; 1_024];
    loop {
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| RequestError::Malformed)?;
        if read == 0 {
            return Err(RequestError::Malformed);
        }
        if encoded.len().saturating_add(read) > MAX_REQUEST_HEADER_BYTES {
            return Err(RequestError::TooLarge);
        }
        encoded.extend_from_slice(&chunk[..read]);
        if encoded.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    parse_request(&encoded)
}

fn parse_request(encoded: &[u8]) -> Result<Request, RequestError> {
    let end = encoded
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(RequestError::Malformed)?;
    let header = str::from_utf8(&encoded[..end]).map_err(|_| RequestError::Malformed)?;
    let mut lines = header.split("\r\n");
    let request_line = lines.next().ok_or(RequestError::Malformed)?;
    let mut fields = request_line.split(' ');
    let method = fields.next().ok_or(RequestError::Malformed)?;
    let target = fields.next().ok_or(RequestError::Malformed)?;
    let version = fields.next().ok_or(RequestError::Malformed)?;
    if fields.next().is_some()
        || method.is_empty()
        || target.is_empty()
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
    {
        return Err(RequestError::Malformed);
    }
    for line in lines {
        if line.starts_with([' ', '\t']) {
            return Err(RequestError::Malformed);
        }
        let (name, value) = line.split_once(':').ok_or(RequestError::Malformed)?;
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(RequestError::Malformed);
        }
        let value = value.trim();
        if name.eq_ignore_ascii_case("transfer-encoding")
            || (name.eq_ignore_ascii_case("content-length") && value != "0")
        {
            return Err(RequestError::Malformed);
        }
    }
    if method != "GET" {
        return Ok(Request::MethodNotAllowed);
    }
    Ok(match target {
        "/livez" => Request::Live,
        "/readyz" => Request::Ready,
        _ => Request::NotFound,
    })
}

struct Response {
    status: &'static str,
    body: &'static str,
    allow_get: bool,
}

impl Response {
    const fn ok(body: &'static str) -> Self {
        Self {
            status: "200 OK",
            body,
            allow_get: false,
        }
    }

    const fn bad_request() -> Self {
        Self {
            status: "400 Bad Request",
            body: "bad_request\n",
            allow_get: false,
        }
    }

    const fn not_found() -> Self {
        Self {
            status: "404 Not Found",
            body: "not_found\n",
            allow_get: false,
        }
    }

    const fn method_not_allowed() -> Self {
        Self {
            status: "405 Method Not Allowed",
            body: "method_not_allowed\n",
            allow_get: true,
        }
    }

    const fn request_timeout() -> Self {
        Self {
            status: "408 Request Timeout",
            body: "request_timeout\n",
            allow_get: false,
        }
    }

    const fn request_header_too_large() -> Self {
        Self {
            status: "431 Request Header Fields Too Large",
            body: "request_header_too_large\n",
            allow_get: false,
        }
    }

    const fn service_unavailable(body: &'static str) -> Self {
        Self {
            status: "503 Service Unavailable",
            body,
            allow_get: false,
        }
    }

    async fn write(self, stream: &mut TcpStream) -> std::io::Result<()> {
        let allow = if self.allow_get { "Allow: GET\r\n" } else { "" };
        let header = format!(
            "HTTP/1.1 {}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n{}Connection: close\r\n\r\n",
            self.status,
            self.body.len(),
            allow
        );
        stream.write_all(header.as_bytes()).await?;
        stream.write_all(self.body.as_bytes()).await?;
        stream.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicBool, AtomicU8, Ordering},
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Notify;

    use super::*;

    struct MutableStatus {
        admission: AtomicU8,
        fail: AtomicBool,
    }

    impl MutableStatus {
        fn new() -> Self {
            Self {
                admission: AtomicU8::new(0),
                fail: AtomicBool::new(false),
            }
        }

        fn set(&self, admission: ProtocolAdmissionState) {
            let value = match admission {
                ProtocolAdmissionState::Ready => 0,
                ProtocolAdmissionState::AtCapacity => 1,
                ProtocolAdmissionState::Draining => 2,
            };
            self.admission.store(value, Ordering::Release);
        }
    }

    impl ServiceStatusSource for MutableStatus {
        fn service_status<'a>(&'a self) -> HarnessFuture<'a, ProtocolServiceStatus> {
            Box::pin(async move {
                if self.fail.load(Ordering::Acquire) {
                    return Err(HarnessError::Protocol("fixture unavailable".to_owned()));
                }
                let admission = match self.admission.load(Ordering::Acquire) {
                    0 => ProtocolAdmissionState::Ready,
                    1 => ProtocolAdmissionState::AtCapacity,
                    _ => ProtocolAdmissionState::Draining,
                };
                Ok(ProtocolServiceStatus {
                    admission,
                    running_operations: 0,
                    retained_operations: 0,
                    operation_retention_limit: 64,
                })
            })
        }
    }

    struct BlockingStatus {
        entered: Notify,
    }

    impl ServiceStatusSource for BlockingStatus {
        fn service_status<'a>(&'a self) -> HarnessFuture<'a, ProtocolServiceStatus> {
            Box::pin(async move {
                self.entered.notify_one();
                pending::<Result<ProtocolServiceStatus, HarnessError>>().await
            })
        }
    }

    async fn request(address: SocketAddr, encoded: &[u8]) -> std::io::Result<String> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(encoded).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok(String::from_utf8_lossy(&response).into_owned())
    }

    #[tokio::test]
    async fn endpoints_follow_authoritative_admission_without_dependency_claims()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(MutableStatus::new());
        let server = HttpProbeServer::bind(
            HttpProbeServerConfig::new("127.0.0.1:0".parse()?)?,
            source.clone(),
        )
        .await?;
        let address = server.local_addr()?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));

        let ready = request(address, b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(ready.starts_with("HTTP/1.1 200 OK"));
        assert!(ready.ends_with("ready\n"));

        source.set(ProtocolAdmissionState::AtCapacity);
        let full = request(address, b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(full.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(full.ends_with("at_capacity\n"));
        let live = request(address, b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(live.starts_with("HTTP/1.1 200 OK"));
        assert!(live.ends_with("live\n"));

        source.set(ProtocolAdmissionState::Draining);
        let draining = request(address, b"GET /readyz HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(draining.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(draining.ends_with("draining\n"));

        shutdown.cancel();
        let report = task.await??;
        assert_eq!(report.accepted_connections, 4);
        assert_eq!(report.successful_responses, 2);
        assert_eq!(report.unready_responses, 2);
        assert_eq!(report.failed_requests, 0);
        Ok(())
    }

    #[tokio::test]
    async fn malformed_unknown_and_unavailable_requests_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(MutableStatus::new());
        let server = HttpProbeServer::bind(
            HttpProbeServerConfig::new("127.0.0.1:0".parse()?)?,
            source.clone(),
        )
        .await?;
        let address = server.local_addr()?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));

        let method = request(address, b"POST /livez HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(method.starts_with("HTTP/1.1 405 Method Not Allowed"));
        assert!(method.contains("Allow: GET\r\n"));
        let unknown = request(address, b"GET /status HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(unknown.starts_with("HTTP/1.1 404 Not Found"));
        let body = request(
            address,
            b"GET /livez HTTP/1.1\r\nContent-Length: 1\r\n\r\nx",
        )
        .await?;
        assert!(body.starts_with("HTTP/1.1 400 Bad Request"));

        source.fail.store(true, Ordering::Release);
        let unavailable =
            request(address, b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
        assert!(unavailable.starts_with("HTTP/1.1 503 Service Unavailable"));
        assert!(unavailable.ends_with("unavailable\n"));

        shutdown.cancel();
        let report = task.await??;
        assert_eq!(report.invalid_requests, 3);
        assert_eq!(report.failed_requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn request_headers_and_configuration_are_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let error = HttpProbeServerConfig::new("127.0.0.1:0".parse()?)?
            .with_limits(
                0,
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(1),
            )
            .expect_err("zero connections must fail");
        assert!(error.to_string().contains("max_connections"));

        let source = Arc::new(MutableStatus::new());
        let server =
            HttpProbeServer::bind(HttpProbeServerConfig::new("127.0.0.1:0".parse()?)?, source)
                .await?;
        let address = server.local_addr()?;
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(server.serve(shutdown.clone()));
        let mut oversized = b"GET /livez HTTP/1.1\r\nX-Fill: ".to_vec();
        oversized.extend(std::iter::repeat_n(b'x', MAX_REQUEST_HEADER_BYTES));
        let response = request(address, &oversized).await?;
        assert!(response.starts_with("HTTP/1.1 431 Request Header Fields Too Large"));
        shutdown.cancel();
        let report = task.await??;
        assert_eq!(report.invalid_requests, 1);
        Ok(())
    }

    #[tokio::test]
    async fn connection_capacity_and_shutdown_drain_are_finite()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = Arc::new(BlockingStatus {
            entered: Notify::new(),
        });
        let config = HttpProbeServerConfig::new("127.0.0.1:0".parse()?)?.with_limits(
            1,
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_millis(10),
        )?;
        let server = HttpProbeServer::bind(config, source.clone()).await?;
        let address = server.local_addr()?;
        let shutdown = CancellationToken::new();
        let server_task = tokio::spawn(server.serve(shutdown.clone()));

        let first = tokio::spawn(async move {
            let mut stream = TcpStream::connect(address).await?;
            stream
                .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<_, std::io::Error>(response)
        });
        source.entered.notified().await;

        let mut rejected = TcpStream::connect(address).await?;
        let mut rejected_response = Vec::new();
        timeout(
            Duration::from_secs(1),
            rejected.read_to_end(&mut rejected_response),
        )
        .await??;
        assert!(rejected_response.is_empty());

        shutdown.cancel();
        let report = server_task.await??;
        assert_eq!(report.accepted_connections, 2);
        assert_eq!(report.capacity_rejections, 1);
        assert_eq!(report.shutdown_aborts, 1);
        assert!(first.await??.is_empty());
        Ok(())
    }
}
