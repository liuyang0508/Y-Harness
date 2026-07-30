//! Bounded authenticated HTTPS transport for MCP JSON-response servers.

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Debug},
    str,
    sync::Arc,
    time::Duration,
};

use futures::stream::BoxStream;
use reqwest::{
    StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue},
};
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientJsonRpcMessage, ServerJsonRpcMessage},
    service::RunningService,
    transport::{
        StreamableHttpClientTransport,
        common::{
            client_side_sse::NeverRetry,
            http_header::{HEADER_SESSION_ID, JSON_MIME_TYPE},
        },
        streamable_http_client::{
            SseError, StreamableHttpClient, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
    },
};
use serde_json::Value;
use sse_stream::Sse;
use tokio::{sync::Mutex, time::timeout};
use zeroize::Zeroizing;

use super::mcp::{
    McpClient, McpToolDescriptor, list_tools_bounded, settle_cancelled_session, tool_result_value,
    validated_mcp_tool_arguments,
};
use crate::{
    CancellationToken, ExecutionPhase, HarnessError, HarnessFuture, SecretProvider,
    SecretReference, SecretRequest, SecretServiceUse, SecretUseContext,
};

const MAX_HTTPS_MCP_URL_BYTES: usize = 8_192;
const MAX_HTTPS_MCP_REQUEST_BYTES: usize = 2_097_152;
const MAX_HTTPS_MCP_RESPONSE_BYTES: usize = 16_777_216;
const MAX_HTTPS_MCP_TOOL_ARGUMENT_BYTES: usize = 1_048_576;
const MAX_HTTPS_MCP_ROOT_CA_BYTES: usize = 1_048_576;
const MAX_HTTPS_MCP_ROOT_CERTIFICATES: usize = 64;
const MAX_HTTPS_MCP_SESSION_ID_BYTES: usize = 4_096;
const MAX_HTTPS_MCP_TIMEOUT: Duration = Duration::from_secs(86_400);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 8_388_608;

/// Strict policy for one authenticated MCP JSON-response endpoint.
#[derive(Clone)]
pub struct HttpsJsonMcpConfig {
    endpoint: String,
    bearer_secret_reference: SecretReference,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_response_bytes: usize,
    exclusive_root_ca_pem: Option<Arc<[u8]>>,
}

impl HttpsJsonMcpConfig {
    /// Creates a configuration with conservative transport defaults.
    pub fn new(
        endpoint: impl Into<String>,
        bearer_secret_reference: SecretReference,
    ) -> Result<Self, HarnessError> {
        let config = Self {
            endpoint: endpoint.into(),
            bearer_secret_reference,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            exclusive_root_ca_pem: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces request/connect and retained-response bounds.
    pub fn with_limits(
        mut self,
        request_timeout: Duration,
        connect_timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, HarnessError> {
        self.request_timeout = request_timeout;
        self.connect_timeout = connect_timeout;
        self.max_response_bytes = max_response_bytes;
        self.validate()?;
        Ok(self)
    }

    /// Replaces WebPKI roots with one explicit bounded PEM bundle.
    pub fn with_exclusive_root_certificates_pem(
        mut self,
        pem: impl Into<Vec<u8>>,
    ) -> Result<Self, HarnessError> {
        self.exclusive_root_ca_pem = Some(Arc::from(pem.into()));
        self.validate()?;
        Ok(self)
    }

    /// Returns the exact credential-free endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if self.endpoint.len() > MAX_HTTPS_MCP_URL_BYTES {
            return Err(HarnessError::InvalidConfiguration(format!(
                "HTTPS MCP endpoint exceeds {MAX_HTTPS_MCP_URL_BYTES} bytes"
            )));
        }
        let endpoint = reqwest::Url::parse(&self.endpoint).map_err(|_| {
            HarnessError::InvalidConfiguration(
                "HTTPS MCP endpoint must be an absolute URL".to_owned(),
            )
        })?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(HarnessError::InvalidConfiguration(
                "MCP endpoint must use HTTPS with a host and no userinfo, query, or fragment"
                    .to_owned(),
            ));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > MAX_HTTPS_MCP_TIMEOUT
            || self.connect_timeout < Duration::from_millis(1)
            || self.connect_timeout > self.request_timeout
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "HTTPS MCP timeouts require 1 millisecond <= connect <= request <= {} seconds",
                MAX_HTTPS_MCP_TIMEOUT.as_secs()
            )));
        }
        if !(1..=MAX_HTTPS_MCP_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "HTTPS MCP response limit must be 1-{MAX_HTTPS_MCP_RESPONSE_BYTES} bytes"
            )));
        }
        if let Some(pem) = &self.exclusive_root_ca_pem {
            parse_exclusive_root_certificates(pem)?;
        }
        Ok(())
    }
}

impl Debug for HttpsJsonMcpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpsJsonMcpConfig")
            .field("endpoint", &self.endpoint)
            .field("bearer_secret_reference", &self.bearer_secret_reference)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field(
                "exclusive_root_certificates",
                &self.exclusive_root_ca_pem.is_some(),
            )
            .finish()
    }
}

/// Persistent MCP session over a bounded HTTPS JSON-response transport.
///
/// Servers must implement the stateless JSON-response subset of MCP Streamable
/// HTTP. SSE responses, redirects, ambient proxies, and transparent request
/// replay are rejected.
pub struct HttpsJsonMcpClient {
    config: HttpsJsonMcpConfig,
    secrets: Arc<dyn SecretProvider>,
    session: Mutex<Option<RunningService<RoleClient, ()>>>,
}

impl HttpsJsonMcpClient {
    /// Validates transport configuration and creates a lazy session.
    pub fn new(
        config: HttpsJsonMcpConfig,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        Ok(Self {
            config,
            secrets,
            session: Mutex::new(None),
        })
    }

    async fn connect(&self) -> Result<RunningService<RoleClient, ()>, HarnessError> {
        let transport_client = StrictHttpsJsonClient::new(&self.config, self.secrets.clone())?;
        let mut transport_config =
            StreamableHttpClientTransportConfig::with_uri(self.config.endpoint.clone())
                .reinit_on_expired_session(false);
        transport_config.retry_config = Arc::new(NeverRetry::default());
        transport_config.allow_stateless = true;
        let transport =
            StreamableHttpClientTransport::with_client(transport_client, transport_config);
        timeout(self.config.request_timeout, ().serve(transport))
            .await
            .map_err(|_| HarnessError::Mcp("HTTPS MCP initialization timed out".to_owned()))?
            .map_err(|_| HarnessError::Mcp("HTTPS MCP initialization failed".to_owned()))
    }

    async fn ensure_connected<'a>(
        &'a self,
        session: &'a mut Option<RunningService<RoleClient, ()>>,
    ) -> Result<&'a RunningService<RoleClient, ()>, HarnessError> {
        if session.as_ref().is_none_or(RunningService::is_closed) {
            *session = Some(self.connect().await?);
        }
        session
            .as_ref()
            .ok_or_else(|| HarnessError::Mcp("HTTPS MCP session was not initialized".to_owned()))
    }

    fn invalidate(session: &mut Option<RunningService<RoleClient, ()>>) {
        if let Some(service) = session.take() {
            service.cancellation_token().cancel();
        }
    }

    async fn call_tool_in_session(
        &self,
        session: &mut Option<RunningService<RoleClient, ()>>,
        name: &str,
        arguments: serde_json::Map<String, Value>,
    ) -> Result<Value, HarnessError> {
        let service = self.ensure_connected(session).await?;
        let result = timeout(
            self.config.request_timeout,
            service
                .call_tool(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments)),
        )
        .await;
        let result = match result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                Self::invalidate(session);
                return Err(HarnessError::Mcp(format!("HTTPS MCP tool {name} failed")));
            }
            Err(_) => {
                Self::invalidate(session);
                return Err(HarnessError::Mcp(format!(
                    "HTTPS MCP tool {name} timed out"
                )));
            }
        };
        tool_result_value(name, result)
    }

    /// Gracefully closes the active remote session.
    pub async fn shutdown(&self) -> Result<(), HarnessError> {
        let service = self.session.lock().await.take();
        if let Some(service) = service {
            timeout(self.config.request_timeout, service.cancel())
                .await
                .map_err(|_| HarnessError::Mcp("HTTPS MCP shutdown timed out".to_owned()))?
                .map_err(|_| HarnessError::Mcp("HTTPS MCP shutdown failed".to_owned()))?;
        }
        Ok(())
    }
}

impl McpClient for HttpsJsonMcpClient {
    fn list_tools<'a>(&'a self) -> HarnessFuture<'a, Vec<McpToolDescriptor>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let service = self.ensure_connected(&mut session).await?;
            let result = timeout(self.config.request_timeout, list_tools_bounded(service)).await;
            match result {
                Ok(Ok(tools)) => Ok(tools),
                Ok(Err(error)) => {
                    Self::invalidate(&mut session);
                    Err(error)
                }
                Err(_) => {
                    Self::invalidate(&mut session);
                    Err(HarnessError::Mcp(
                        "HTTPS MCP tools/list timed out".to_owned(),
                    ))
                }
            }
        })
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: Value) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            let arguments =
                validated_mcp_tool_arguments(name, arguments, MAX_HTTPS_MCP_TOOL_ARGUMENT_BYTES)?;
            let mut session = self.session.lock().await;
            self.call_tool_in_session(&mut session, name, arguments)
                .await
        })
    }

    fn call_tool_with_cancellation<'a>(
        &'a self,
        name: &'a str,
        arguments: Value,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(HarnessError::Cancelled {
                    phase: ExecutionPhase::Tool,
                });
            }
            let arguments =
                validated_mcp_tool_arguments(name, arguments, MAX_HTTPS_MCP_TOOL_ARGUMENT_BYTES)?;
            let mut session = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Err(HarnessError::Cancelled {
                    phase: ExecutionPhase::Tool,
                }),
                session = self.session.lock() => session,
            };
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => {}
                result = self.call_tool_in_session(&mut session, name, arguments) => return result,
            };
            settle_cancelled_session(&mut session, "HTTPS").await?;
            Err(HarnessError::Cancelled {
                phase: ExecutionPhase::Tool,
            })
        })
    }
}

#[derive(Clone)]
struct StrictHttpsJsonClient {
    client: reqwest::Client,
    secrets: Arc<dyn SecretProvider>,
    reference: SecretReference,
    max_response_bytes: usize,
}

impl StrictHttpsJsonClient {
    fn new(
        config: &HttpsJsonMcpConfig,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_proxy()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .user_agent(concat!("y-harness/", env!("CARGO_PKG_VERSION")));
        if let Some(pem) = &config.exclusive_root_ca_pem {
            builder = builder.tls_certs_only(parse_exclusive_root_certificates(pem)?);
        }
        let client = builder.build().map_err(|_| {
            HarnessError::InvalidConfiguration("failed to build HTTPS MCP transport".to_owned())
        })?;
        Ok(Self {
            client,
            secrets,
            reference: config.bearer_secret_reference.clone(),
            max_response_bytes: config.max_response_bytes,
        })
    }

    async fn bearer_token(&self) -> Result<Zeroizing<String>, StreamableHttpError<reqwest::Error>> {
        let credential = self
            .secrets
            .resolve(SecretRequest {
                reference: self.reference.clone(),
                consumer: "https-mcp".to_owned(),
                use_context: SecretUseContext::Service {
                    use_case: SecretServiceUse::TransportRequest,
                },
            })
            .await
            .map_err(|_| {
                StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                    "HTTPS MCP credential resolution failed",
                ))
            })?;
        let token = str::from_utf8(credential.expose_bytes()).map_err(|_| {
            StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                "HTTPS MCP credential is not valid UTF-8",
            ))
        })?;
        Ok(Zeroizing::new(token.to_owned()))
    }
}

impl StreamableHttpClient for StrictHttpsJsonClient {
    type Error = reqwest::Error;

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        if auth_header.is_some() || custom_headers.contains_key(&AUTHORIZATION) {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                Cow::Borrowed("HTTPS MCP has conflicting credential authority"),
            ));
        }
        let token = self.bearer_token().await?;
        let body = crate::json::to_bounded_json_vec(&message, MAX_HTTPS_MCP_REQUEST_BYTES)
            .map_err(|error| match error {
                crate::json::BoundedJsonError::LimitExceeded => {
                    StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                        "HTTPS MCP request exceeded its configured limit",
                    ))
                }
                crate::json::BoundedJsonError::CannotEncode => {
                    StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                        "HTTPS MCP request could not be encoded",
                    ))
                }
            })?;
        let mut request = self
            .client
            .post(uri.as_ref())
            .header(ACCEPT, JSON_MIME_TYPE)
            .header(CONTENT_TYPE, JSON_MIME_TYPE)
            .bearer_auth(token.as_str());
        for (name, value) in custom_headers {
            request = request.header(name, value);
        }
        let attached_session = session_id.is_some();
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let mut response = request
            .body(body)
            .send()
            .await
            .map_err(StreamableHttpError::Client)?;
        let status = response.status();
        if matches!(status, StatusCode::ACCEPTED | StatusCode::NO_CONTENT) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == StatusCode::NOT_FOUND && attached_session {
            return Err(StreamableHttpError::SessionExpired);
        }
        if !status.is_success() {
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!("HTTPS MCP returned HTTP status {status}"),
            )));
        }
        if !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(is_json_media_type)
        {
            return Err(StreamableHttpError::UnexpectedContentType(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
            ));
        }
        if response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                Cow::Borrowed("HTTPS MCP response exceeded its configured limit"),
            ));
        }
        let returned_session = response
            .headers()
            .get(HEADER_SESSION_ID)
            .map(validate_session_id)
            .transpose()?;
        let body = read_bounded_body(&mut response, self.max_response_bytes).await?;
        if body.is_empty()
            && matches!(
                message,
                ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_)
            )
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let message = serde_json::from_slice::<ServerJsonRpcMessage>(&body)
            .map_err(StreamableHttpError::Deserialize)?;
        Ok(StreamableHttpPostResponse::Json(message, returned_session))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_header: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        if auth_header.is_some() || custom_headers.contains_key(&AUTHORIZATION) {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                Cow::Borrowed("HTTPS MCP has conflicting credential authority"),
            ));
        }
        let token = self.bearer_token().await?;
        let mut request = self
            .client
            .delete(uri.as_ref())
            .header(HEADER_SESSION_ID, session_id.as_ref())
            .bearer_auth(token.as_str());
        for (name, value) in custom_headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(StreamableHttpError::Client)?;
        if response.status() == StatusCode::METHOD_NOT_ALLOWED {
            return Ok(());
        }
        if response.status().is_success() {
            Ok(())
        } else {
            Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!(
                    "HTTPS MCP delete returned HTTP status {}",
                    response.status()
                ),
            )))
        }
    }

    async fn get_stream(
        &self,
        _uri: Arc<str>,
        _session_id: Arc<str>,
        _last_event_id: Option<String>,
        _auth_header: Option<String>,
        _custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        Err(StreamableHttpError::ServerDoesNotSupportSse)
    }
}

async fn read_bounded_body(
    response: &mut reqwest::Response,
    maximum: usize,
) -> Result<Vec<u8>, StreamableHttpError<reqwest::Error>> {
    let mut body = Vec::with_capacity(maximum.min(8_192));
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(StreamableHttpError::Client)?;
        let Some(chunk) = chunk else {
            return Ok(body);
        };
        let next = body.len().checked_add(chunk.len()).ok_or_else(|| {
            StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                "HTTPS MCP response size overflow",
            ))
        })?;
        if next > maximum {
            return Err(StreamableHttpError::UnexpectedServerResponse(
                Cow::Borrowed("HTTPS MCP response exceeded its configured limit"),
            ));
        }
        body.extend_from_slice(&chunk);
    }
}

fn validate_session_id(value: &HeaderValue) -> Result<String, StreamableHttpError<reqwest::Error>> {
    let value = value.to_str().map_err(|_| {
        StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
            "HTTPS MCP session identity is not valid ASCII",
        ))
    })?;
    if value.is_empty()
        || value.len() > MAX_HTTPS_MCP_SESSION_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(StreamableHttpError::UnexpectedServerResponse(
            Cow::Borrowed("HTTPS MCP session identity is invalid"),
        ));
    }
    Ok(value.to_owned())
}

fn is_json_media_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case(JSON_MIME_TYPE))
}

fn parse_exclusive_root_certificates(
    pem: &[u8],
) -> Result<Vec<reqwest::Certificate>, HarnessError> {
    if pem.is_empty() || pem.len() > MAX_HTTPS_MCP_ROOT_CA_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive MCP root CA bundle must be 1-{MAX_HTTPS_MCP_ROOT_CA_BYTES} bytes"
        )));
    }
    let certificates = reqwest::Certificate::from_pem_bundle(pem).map_err(|_| {
        HarnessError::InvalidConfiguration(
            "exclusive MCP root CA bundle is not valid PEM".to_owned(),
        )
    })?;
    if certificates.is_empty() || certificates.len() > MAX_HTTPS_MCP_ROOT_CERTIFICATES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive MCP root CA bundle must contain 1-{MAX_HTTPS_MCP_ROOT_CERTIFICATES} certificates"
        )));
    }
    Ok(certificates)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::HttpsJsonMcpConfig;
    use crate::SecretReference;

    #[test]
    fn configuration_rejects_ambient_or_unbounded_endpoint_authority() {
        let reference = SecretReference::new("mcp/test").expect("reference");
        for endpoint in [
            "http://example.com/mcp",
            "https://user@example.com/mcp",
            "https://example.com/mcp?tenant=ambient",
            "https://example.com/mcp#fragment",
        ] {
            assert!(HttpsJsonMcpConfig::new(endpoint, reference.clone()).is_err());
        }
        assert!(
            HttpsJsonMcpConfig::new("https://example.com/mcp", reference)
                .expect("base config")
                .with_limits(Duration::ZERO, Duration::from_secs(1), 1)
                .is_err()
        );
    }

    #[test]
    fn configuration_debug_exposes_only_the_secret_reference() {
        let reference = SecretReference::new("mcp/test").expect("reference");
        let config = HttpsJsonMcpConfig::new("https://example.com/mcp", reference).expect("config");
        let debug = format!("{config:?}");
        assert!(debug.contains("mcp/test"));
        assert!(!debug.contains("SecretValue"));
    }
}
