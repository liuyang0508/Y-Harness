//! Production-facing model adapters kept outside the kernel contract.

mod anthropic_messages;
mod gemini_generate_content;
mod native_http;
mod openai_chat_completions;
mod openai_responses;

pub use anthropic_messages::{AnthropicMessagesModel, AnthropicMessagesModelConfig};
pub use gemini_generate_content::{GeminiGenerateContentModel, GeminiGenerateContentModelConfig};
pub use openai_chat_completions::{
    ChatCompletionTokenLimitField, OpenAiChatCompletionsModel, OpenAiChatCompletionsModelConfig,
};
pub use openai_responses::{OpenAiResponsesModel, OpenAiResponsesModelConfig};

use std::{fmt, sync::Arc, time::Duration};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use serde::Deserialize;
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use crate::{
    HarnessError, HarnessFuture, LanguageModel, MODEL_GATEWAY_API_VERSION, ModelOutput,
    ModelProviderFailure, ModelProviderFailureKind, ModelRequest, ModelResponse, ModelStream,
    SecretProvider, SecretReference, SecretRequest, SecretUseContext, SecretValue,
    kernel::validate_model_id,
};

const MAX_HTTP_MODEL_REQUEST_BYTES: usize = 16_777_216;
const MAX_HTTP_MODEL_RESPONSE_BYTES: usize = 16_777_216;
const MAX_HTTP_MODEL_CONCURRENCY: usize = 256;
const MAX_HTTP_MODEL_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_PROVIDER_REQUEST_ID_BYTES: usize = 256;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 2_097_152;
const DEFAULT_MAX_CONCURRENCY: usize = 16;
const MODEL_API_HEADER: &str = "x-y-harness-model-api";
const MODEL_STREAM_HEADER: &str = "x-y-harness-model-stream";
const MODEL_STREAM_MEDIA_TYPE: &str = "application/x-ndjson";
const MAX_MODEL_STREAM_FRAMES: usize = 4_096;
const MAX_MODEL_STREAM_DELTA_BYTES: usize = 4_096;
const MAX_ROOT_CA_PEM_BYTES: usize = 1_048_576;
const MAX_ROOT_CA_CERTIFICATES: usize = 64;

/// Validated configuration for the HTTPS JSON model-gateway contract.
#[derive(Clone)]
pub struct HttpsJsonModelConfig {
    endpoint: String,
    bearer_secret: SecretReference,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_response_bytes: usize,
    max_concurrency: usize,
    exclusive_root_ca_pem: Option<Arc<[u8]>>,
}

impl HttpsJsonModelConfig {
    /// Creates safe defaults for one authenticated HTTPS endpoint.
    pub fn new(
        endpoint: impl Into<String>,
        bearer_secret: SecretReference,
    ) -> Result<Self, HarnessError> {
        let config = Self {
            endpoint: endpoint.into(),
            bearer_secret,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            exclusive_root_ca_pem: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces time, response-retention, and concurrency bounds.
    pub fn with_limits(
        mut self,
        request_timeout: Duration,
        connect_timeout: Duration,
        max_response_bytes: usize,
        max_concurrency: usize,
    ) -> Result<Self, HarnessError> {
        self.request_timeout = request_timeout;
        self.connect_timeout = connect_timeout;
        self.max_response_bytes = max_response_bytes;
        self.max_concurrency = max_concurrency;
        self.validate()?;
        Ok(self)
    }

    /// Trusts only the supplied bounded PEM CA bundle for this endpoint.
    ///
    /// This disables ambient native and WebPKI roots. It is intended for a
    /// private enterprise gateway whose trust root is selected explicitly by
    /// the host.
    pub fn with_exclusive_root_certificates_pem(
        mut self,
        pem: impl Into<Vec<u8>>,
    ) -> Result<Self, HarnessError> {
        self.exclusive_root_ca_pem = Some(Arc::from(pem.into()));
        self.validate()?;
        Ok(self)
    }

    /// Returns the validated endpoint without credentials.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn validate(&self) -> Result<(), HarnessError> {
        let endpoint = reqwest::Url::parse(&self.endpoint).map_err(|_| {
            HarnessError::InvalidConfiguration(
                "HTTPS model endpoint must be an absolute URL".to_owned(),
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
                "model endpoint must use HTTPS with a host and no userinfo, query, or fragment"
                    .to_owned(),
            ));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > MAX_HTTP_MODEL_TIMEOUT
            || self.connect_timeout < Duration::from_millis(1)
            || self.connect_timeout > self.request_timeout
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "model timeouts must be at least 1 millisecond, connect must not exceed request, and request must not exceed {} seconds",
                MAX_HTTP_MODEL_TIMEOUT.as_secs()
            )));
        }
        if !(1..=MAX_HTTP_MODEL_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "model response limit must be 1-{MAX_HTTP_MODEL_RESPONSE_BYTES} bytes"
            )));
        }
        if !(1..=MAX_HTTP_MODEL_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "model concurrency must be 1-{MAX_HTTP_MODEL_CONCURRENCY}"
            )));
        }
        if let Some(pem) = &self.exclusive_root_ca_pem {
            parse_exclusive_root_certificates(pem)?;
        }
        Ok(())
    }
}

impl fmt::Debug for HttpsJsonModelConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpsJsonModelConfig")
            .field("endpoint", &self.endpoint)
            .field("bearer_secret", &self.bearer_secret)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_concurrency", &self.max_concurrency)
            .field(
                "exclusive_root_certificates",
                &self.exclusive_root_ca_pem.is_some(),
            )
            .finish()
    }
}

/// One authenticated, bounded request issued to a model gateway transport.
pub struct HttpModelRequest {
    /// Validated HTTPS endpoint.
    pub endpoint: String,
    /// Exact gateway protocol requested by the adapter.
    pub api_version: String,
    /// Short-lived bearer credential.
    pub bearer_secret: SecretValue,
    /// Serialized provider-neutral [`ModelRequest`].
    pub body: Vec<u8>,
    /// Total transport time bound.
    pub timeout: Duration,
    /// Maximum retained response body bytes.
    pub max_response_bytes: usize,
}

/// Bounded response returned by a model gateway transport.
pub struct HttpModelResponse {
    /// Numeric HTTP status.
    pub status: u16,
    /// Exact gateway protocol reported by the server.
    pub api_version: Option<String>,
    /// Response media type, when present.
    pub content_type: Option<String>,
    /// Optional opaque request identity from the response header.
    pub provider_request_id: Option<String>,
    /// Retained response body.
    pub body: Vec<u8>,
}

/// Replaceable HTTPS authority used by the JSON model adapter.
///
/// Custom implementations are trusted host components and must provide the
/// same TLS, no-retry, redirect, proxy, and response-bound guarantees as the
/// built-in transport.
pub trait HttpModelTransport: Send + Sync {
    /// Sends one already validated gateway request.
    fn send<'a>(&'a self, request: HttpModelRequest) -> HarnessFuture<'a, HttpModelResponse>;

    /// Sends one request that may emit provisional deltas before final response.
    ///
    /// The default preserves non-streaming custom transports.
    fn send_streaming<'a>(
        &'a self,
        request: HttpModelRequest,
        _stream: ModelStream,
    ) -> HarnessFuture<'a, HttpModelResponse> {
        self.send(request)
    }
}

/// Reqwest-backed HTTPS transport with fixed security and resource policy.
pub struct ReqwestHttpModelTransport {
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
}

impl ReqwestHttpModelTransport {
    /// Builds a reusable pooled client from validated model configuration.
    pub fn new(config: &HttpsJsonModelConfig) -> Result<Self, HarnessError> {
        Self::build(config, None)
    }

    /// Builds a pooled mTLS client from one non-serializable PEM identity.
    ///
    /// The PEM must contain exactly one private key and at least one
    /// certificate. Rebuild the transport when the client identity rotates.
    pub fn new_with_client_identity(
        config: &HttpsJsonModelConfig,
        identity_pem: SecretValue,
    ) -> Result<Self, HarnessError> {
        Self::build(config, Some(identity_pem))
    }

    fn build(
        config: &HttpsJsonModelConfig,
        identity_pem: Option<SecretValue>,
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
        if let Some(identity_pem) = identity_pem {
            let identity =
                reqwest::Identity::from_pem(identity_pem.expose_bytes()).map_err(|_| {
                    HarnessError::InvalidConfiguration(
                        "model client identity is not a valid PEM certificate and private key"
                            .to_owned(),
                    )
                })?;
            builder = builder.identity(identity);
        }
        let client = builder.build().map_err(|_| {
            HarnessError::InvalidConfiguration("failed to build HTTPS model transport".to_owned())
        })?;
        Ok(Self {
            client,
            concurrency: Arc::new(Semaphore::new(config.max_concurrency)),
        })
    }
}

fn parse_exclusive_root_certificates(
    pem: &[u8],
) -> Result<Vec<reqwest::Certificate>, HarnessError> {
    if pem.is_empty() || pem.len() > MAX_ROOT_CA_PEM_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive model root CA bundle must be 1-{MAX_ROOT_CA_PEM_BYTES} bytes"
        )));
    }
    let certificates = reqwest::Certificate::from_pem_bundle(pem).map_err(|_| {
        HarnessError::InvalidConfiguration(
            "exclusive model root CA bundle is not valid PEM".to_owned(),
        )
    })?;
    if certificates.is_empty() || certificates.len() > MAX_ROOT_CA_CERTIFICATES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive model root CA bundle must contain 1-{MAX_ROOT_CA_CERTIFICATES} certificates"
        )));
    }
    Ok(certificates)
}

impl HttpModelTransport for ReqwestHttpModelTransport {
    fn send<'a>(&'a self, request: HttpModelRequest) -> HarnessFuture<'a, HttpModelResponse> {
        Box::pin(async move {
            tokio::time::timeout(request.timeout, async {
                let _permit = self.concurrency.acquire().await.map_err(|_| {
                    provider_failure(
                        ModelProviderFailureKind::Transport,
                        "HTTPS model transport is closed",
                        None,
                        None,
                    )
                })?;
                execute_http_request(&self.client, request).await
            })
            .await
            .map_err(|_| {
                provider_failure(
                    ModelProviderFailureKind::Transport,
                    "HTTPS model request timed out",
                    None,
                    None,
                )
            })?
        })
    }

    fn send_streaming<'a>(
        &'a self,
        request: HttpModelRequest,
        stream: ModelStream,
    ) -> HarnessFuture<'a, HttpModelResponse> {
        Box::pin(async move {
            tokio::time::timeout(request.timeout, async {
                let _permit = self.concurrency.acquire().await.map_err(|_| {
                    provider_failure(
                        ModelProviderFailureKind::Transport,
                        "HTTPS model transport is closed",
                        None,
                        None,
                    )
                })?;
                execute_http_streaming_request(&self.client, request, stream).await
            })
            .await
            .map_err(|_| {
                provider_failure(
                    ModelProviderFailureKind::Transport,
                    "HTTPS model request timed out",
                    None,
                    None,
                )
            })?
        })
    }
}

/// Authenticated model adapter for the Y-Harness JSON gateway contract.
///
/// The request body is a [`ModelRequest`] and the successful response body is a
/// [`ModelResponse`]. It is intentionally a Harness gateway protocol rather
/// than a claim that vendor APIs share one schema.
pub struct HttpsJsonModel {
    id: String,
    config: HttpsJsonModelConfig,
    secrets: Arc<dyn SecretProvider>,
    transport: Arc<dyn HttpModelTransport>,
}

impl HttpsJsonModel {
    /// Creates an adapter over the built-in pooled Reqwest HTTPS transport.
    pub fn new(
        id: impl Into<String>,
        config: HttpsJsonModelConfig,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<Self, HarnessError> {
        let transport = Arc::new(ReqwestHttpModelTransport::new(&config)?);
        Self::with_transport(id, config, secrets, transport)
    }

    /// Creates an adapter whose pooled HTTPS transport presents an mTLS identity.
    ///
    /// The caller resolves the non-serializable identity at host startup and
    /// rebuilds this adapter when that identity rotates.
    pub fn new_with_client_identity(
        id: impl Into<String>,
        config: HttpsJsonModelConfig,
        secrets: Arc<dyn SecretProvider>,
        identity_pem: SecretValue,
    ) -> Result<Self, HarnessError> {
        let transport = Arc::new(ReqwestHttpModelTransport::new_with_client_identity(
            &config,
            identity_pem,
        )?);
        Self::with_transport(id, config, secrets, transport)
    }

    /// Creates an adapter over a host-supplied trusted HTTPS transport.
    pub fn with_transport(
        id: impl Into<String>,
        config: HttpsJsonModelConfig,
        secrets: Arc<dyn SecretProvider>,
        transport: Arc<dyn HttpModelTransport>,
    ) -> Result<Self, HarnessError> {
        let id = id.into();
        validate_model_id(&id)?;
        config.validate()?;
        Ok(Self {
            id,
            config,
            secrets,
            transport,
        })
    }

    async fn request(&self, request: ModelRequest) -> Result<ModelResponse, HarnessError> {
        tokio::time::timeout(self.config.request_timeout, self.request_inner(request))
            .await
            .map_err(|_| HarnessError::Model("HTTPS model operation timed out".to_owned()))?
    }

    async fn request_inner(&self, request: ModelRequest) -> Result<ModelResponse, HarnessError> {
        let http_request = self.prepare_http_request(&request).await?;
        let response = self
            .transport
            .send(http_request)
            .await
            .map_err(sanitize_transport_error)?;
        self.decode_response(response)
    }

    async fn request_streaming(
        &self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> Result<ModelResponse, HarnessError> {
        tokio::time::timeout(
            self.config.request_timeout,
            self.request_streaming_inner(request, stream),
        )
        .await
        .map_err(|_| HarnessError::Model("HTTPS model operation timed out".to_owned()))?
    }

    async fn request_streaming_inner(
        &self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> Result<ModelResponse, HarnessError> {
        let http_request = self.prepare_http_request(&request).await?;
        let response = self
            .transport
            .send_streaming(http_request, stream)
            .await
            .map_err(sanitize_transport_error)?;
        self.decode_response(response)
    }

    async fn prepare_http_request(
        &self,
        request: &ModelRequest,
    ) -> Result<HttpModelRequest, HarnessError> {
        crate::runtime::validate_model_request(request)?;
        let body = crate::json::to_bounded_json_vec(request, MAX_HTTP_MODEL_REQUEST_BYTES)
            .map_err(|error| match error {
                crate::json::BoundedJsonError::LimitExceeded => HarnessError::Model(format!(
                    "HTTPS model request exceeds {MAX_HTTP_MODEL_REQUEST_BYTES} bytes"
                )),
                crate::json::BoundedJsonError::CannotEncode => {
                    HarnessError::Model("cannot encode HTTPS model request".to_owned())
                }
            })?;
        let credential = self
            .secrets
            .resolve_as(
                SecretRequest {
                    reference: self.config.bearer_secret.clone(),
                    consumer: self.id.clone(),
                    use_context: SecretUseContext::AgentTurn {
                        thread_id: request.thread_id.clone(),
                        turn_id: request.turn_id.clone(),
                    },
                },
                &request.authority,
            )
            .await
            .map_err(|_| HarnessError::Model("model credential resolution failed".to_owned()))?;
        validate_bearer_secret(&credential)?;
        Ok(HttpModelRequest {
            endpoint: self.config.endpoint.clone(),
            api_version: MODEL_GATEWAY_API_VERSION.to_owned(),
            bearer_secret: credential,
            body,
            timeout: self.config.request_timeout,
            max_response_bytes: self.config.max_response_bytes,
        })
    }

    fn decode_response(&self, response: HttpModelResponse) -> Result<ModelResponse, HarnessError> {
        validate_http_response(&response, self.config.max_response_bytes)?;
        let mut response_body: ModelResponse =
            serde_json::from_slice(&response.body).map_err(|_| {
                provider_failure(
                    ModelProviderFailureKind::Protocol,
                    "HTTPS model returned invalid JSON",
                    None,
                    None,
                )
            })?;
        if response_body.provider_request_id.is_none() {
            response_body.provider_request_id = response.provider_request_id;
        }
        crate::runtime::validate_model_response(&response_body)
            .map_err(|error| protocol_failure(error.to_string()))?;
        Ok(response_body)
    }
}

impl LanguageModel for HttpsJsonModel {
    fn id(&self) -> &str {
        &self.id
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move { self.request(request).await.map(|response| response.output) })
    }

    fn complete_with_metadata<'a>(
        &'a self,
        request: ModelRequest,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move { self.request(request).await })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> HarnessFuture<'a, ModelResponse> {
        if stream.is_enabled() {
            Box::pin(async move { self.request_streaming(request, stream).await })
        } else {
            self.complete_with_metadata(request)
        }
    }
}

async fn execute_http_request(
    client: &reqwest::Client,
    request: HttpModelRequest,
) -> Result<HttpModelResponse, HarnessError> {
    let authorization = authorization_header(&request.bearer_secret)?;
    let mut response = client
        .post(request.endpoint)
        .header(AUTHORIZATION, authorization)
        .header(ACCEPT, "application/json")
        .header(CONTENT_TYPE, "application/json")
        .header(MODEL_API_HEADER, request.api_version)
        .body(request.body)
        .send()
        .await
        .map_err(map_reqwest_error)?;
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > request.max_response_bytes as u64
    {
        return Err(protocol_failure(
            "HTTPS model response declared an oversized body",
        ));
    }
    let status = response.status().as_u16();
    let api_version = response
        .headers()
        .get(MODEL_API_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let provider_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !(200..300).contains(&status) {
        return Ok(HttpModelResponse {
            status,
            api_version,
            content_type,
            provider_request_id,
            body: Vec::new(),
        });
    }
    let mut body = Vec::with_capacity(request.max_response_bytes.min(8_192));
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| protocol_failure("HTTPS model response size overflow"))?;
        if next > request.max_response_bytes {
            return Err(protocol_failure(
                "HTTPS model response exceeded its configured limit",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(HttpModelResponse {
        status,
        api_version,
        content_type,
        provider_request_id,
        body,
    })
}

fn authorization_header(secret: &SecretValue) -> Result<HeaderValue, HarnessError> {
    let mut authorization = Zeroizing::new(Vec::with_capacity(7 + secret.expose_bytes().len()));
    authorization.extend_from_slice(b"Bearer ");
    authorization.extend_from_slice(secret.expose_bytes());
    let mut authorization = HeaderValue::from_bytes(authorization.as_slice()).map_err(|_| {
        HarnessError::Model("model bearer credential is not a valid HTTP header".to_owned())
    })?;
    authorization.set_sensitive(true);
    Ok(authorization)
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ModelGatewayStreamFrame {
    TextDelta { delta: String },
    Response { response: ModelResponse },
}

struct ModelGatewayStreamDecoder {
    stream: ModelStream,
    max_bytes: usize,
    pending: Vec<u8>,
    scanned: usize,
    total_bytes: usize,
    frame_count: usize,
    final_response: Option<ModelResponse>,
}

impl ModelGatewayStreamDecoder {
    fn new(stream: ModelStream, max_bytes: usize) -> Self {
        Self {
            stream,
            max_bytes,
            pending: Vec::new(),
            scanned: 0,
            total_bytes: 0,
            frame_count: 0,
            final_response: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<(), HarnessError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(chunk.len())
            .ok_or_else(|| protocol_failure("HTTPS model stream size overflow"))?;
        if self.total_bytes > self.max_bytes {
            return Err(protocol_failure(
                "HTTPS model stream exceeded its configured limit",
            ));
        }
        self.pending.extend_from_slice(chunk);
        while let Some(relative) = self.pending[self.scanned..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            let newline = self.scanned + relative;
            let mut line = self.pending.drain(..=newline).collect::<Vec<_>>();
            line.pop();
            self.scanned = 0;
            parse_stream_frame(
                &line,
                &self.stream,
                &mut self.frame_count,
                &mut self.final_response,
            )?;
        }
        self.scanned = self.pending.len();
        Ok(())
    }

    fn finish(mut self) -> Result<ModelResponse, HarnessError> {
        if !self.pending.is_empty() {
            parse_stream_frame(
                &self.pending,
                &self.stream,
                &mut self.frame_count,
                &mut self.final_response,
            )?;
        }
        self.final_response
            .ok_or_else(|| protocol_failure("HTTPS model stream ended without a final response"))
    }
}

async fn execute_http_streaming_request(
    client: &reqwest::Client,
    request: HttpModelRequest,
    stream: ModelStream,
) -> Result<HttpModelResponse, HarnessError> {
    let authorization = authorization_header(&request.bearer_secret)?;
    let mut response = client
        .post(request.endpoint)
        .header(AUTHORIZATION, authorization)
        .header(ACCEPT, MODEL_STREAM_MEDIA_TYPE)
        .header(CONTENT_TYPE, "application/json")
        .header(MODEL_API_HEADER, request.api_version)
        .header(MODEL_STREAM_HEADER, "1")
        .body(request.body)
        .send()
        .await
        .map_err(map_reqwest_error)?;
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > request.max_response_bytes as u64
    {
        return Err(protocol_failure(
            "HTTPS model stream declared an oversized body",
        ));
    }
    let status = response.status().as_u16();
    let api_version = response
        .headers()
        .get(MODEL_API_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let provider_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !(200..300).contains(&status) {
        return Ok(HttpModelResponse {
            status,
            api_version,
            content_type,
            provider_request_id,
            body: Vec::new(),
        });
    }
    if api_version.as_deref() != Some(MODEL_GATEWAY_API_VERSION) {
        return Err(protocol_failure(format!(
            "HTTPS model gateway API mismatch; expected {MODEL_GATEWAY_API_VERSION}"
        )));
    }
    if !content_type
        .as_deref()
        .is_some_and(|value| is_media_type(value, MODEL_STREAM_MEDIA_TYPE))
    {
        return Err(protocol_failure(format!(
            "HTTPS model stream must use {MODEL_STREAM_MEDIA_TYPE}"
        )));
    }

    let mut decoder = ModelGatewayStreamDecoder::new(stream, request.max_response_bytes);
    while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
        decoder.push(&chunk)?;
    }
    let final_response = decoder.finish()?;
    let body = crate::json::to_bounded_json_vec(&final_response, request.max_response_bytes)
        .map_err(|error| match error {
            crate::json::BoundedJsonError::LimitExceeded => {
                protocol_failure("streamed final model response exceeds its configured limit")
            }
            crate::json::BoundedJsonError::CannotEncode => {
                HarnessError::Model("cannot normalize streamed model response".to_owned())
            }
        })?;
    Ok(HttpModelResponse {
        status,
        api_version,
        content_type: Some("application/json".to_owned()),
        provider_request_id,
        body,
    })
}

fn parse_stream_frame(
    line: &[u8],
    stream: &ModelStream,
    frame_count: &mut usize,
    final_response: &mut Option<ModelResponse>,
) -> Result<(), HarnessError> {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.is_empty() {
        return Err(protocol_failure(
            "HTTPS model stream contains an empty frame",
        ));
    }
    *frame_count = frame_count
        .checked_add(1)
        .ok_or_else(|| protocol_failure("HTTPS model stream frame overflow"))?;
    if *frame_count > MAX_MODEL_STREAM_FRAMES {
        return Err(protocol_failure(format!(
            "HTTPS model stream exceeds {MAX_MODEL_STREAM_FRAMES} frames"
        )));
    }
    if final_response.is_some() {
        return Err(protocol_failure(
            "HTTPS model stream contains frames after its final response",
        ));
    }
    let frame: ModelGatewayStreamFrame = serde_json::from_slice(line)
        .map_err(|_| protocol_failure("HTTPS model stream contains invalid JSON"))?;
    match frame {
        ModelGatewayStreamFrame::TextDelta { delta } => {
            if delta.is_empty() || delta.len() > MAX_MODEL_STREAM_DELTA_BYTES {
                return Err(protocol_failure(format!(
                    "HTTPS model stream delta must be 1-{MAX_MODEL_STREAM_DELTA_BYTES} bytes"
                )));
            }
            let _ = stream.emit_text_delta(delta);
        }
        ModelGatewayStreamFrame::Response { response } => {
            crate::runtime::validate_model_response(&response)
                .map_err(|error| protocol_failure(error.to_string()))?;
            *final_response = Some(response);
        }
    }
    Ok(())
}

fn validate_bearer_secret(secret: &SecretValue) -> Result<(), HarnessError> {
    if secret
        .expose_bytes()
        .iter()
        .any(|byte| !(0x21..=0x7e).contains(byte))
    {
        return Err(HarnessError::Model(
            "model bearer credential must contain visible ASCII bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_http_response(
    response: &HttpModelResponse,
    max_response_bytes: usize,
) -> Result<(), HarnessError> {
    if !(200..300).contains(&response.status) {
        return Err(provider_http_failure("HTTPS model", response.status, None));
    }
    if response.api_version.as_deref() != Some(MODEL_GATEWAY_API_VERSION) {
        return Err(provider_failure(
            ModelProviderFailureKind::Protocol,
            format!("HTTPS model gateway API mismatch; expected {MODEL_GATEWAY_API_VERSION}"),
            None,
            None,
        ));
    }
    let is_json = response
        .content_type
        .as_deref()
        .is_some_and(|content_type| is_media_type(content_type, "application/json"));
    if !is_json {
        return Err(provider_failure(
            ModelProviderFailureKind::Protocol,
            "HTTPS model response must use application/json",
            None,
            None,
        ));
    }
    if response.body.is_empty() || response.body.len() > max_response_bytes {
        return Err(provider_failure(
            ModelProviderFailureKind::Protocol,
            format!("HTTPS model response must be 1-{max_response_bytes} bytes"),
            None,
            None,
        ));
    }
    validate_provider_request_id(response.provider_request_id.as_deref())
}

fn is_media_type(content_type: &str, expected: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case(expected))
}

fn validate_provider_request_id(value: Option<&str>) -> Result<(), HarnessError> {
    if let Some(value) = value
        && (value.trim().is_empty()
            || value.len() > MAX_PROVIDER_REQUEST_ID_BYTES
            || value.chars().any(char::is_control))
    {
        return Err(protocol_failure(format!(
            "provider request id must be 1-{MAX_PROVIDER_REQUEST_ID_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn sanitize_transport_error(error: HarnessError) -> HarnessError {
    match error {
        HarnessError::Cancelled { .. }
        | HarnessError::TimedOut { .. }
        | HarnessError::ModelProvider(_) => error,
        _ => provider_failure(
            ModelProviderFailureKind::Transport,
            "HTTPS model transport failed",
            None,
            None,
        ),
    }
}

fn map_reqwest_error(error: reqwest::Error) -> HarnessError {
    let message = if error.is_timeout() {
        "HTTPS model transport timed out"
    } else if error.is_connect() {
        "HTTPS model transport connection failed"
    } else if error.is_body() || error.is_decode() {
        "HTTPS model transport body failed"
    } else {
        "HTTPS model transport request failed"
    };
    provider_failure(ModelProviderFailureKind::Transport, message, None, None)
}

fn protocol_failure(message: impl Into<String>) -> HarnessError {
    provider_failure(ModelProviderFailureKind::Protocol, message, None, None)
}

pub(super) fn provider_http_failure(
    provider: &str,
    status: u16,
    retry_after_ms: Option<u64>,
) -> HarnessError {
    let kind = match status {
        401 => ModelProviderFailureKind::Authentication,
        403 => ModelProviderFailureKind::Authorization,
        429 => ModelProviderFailureKind::RateLimited,
        529 => ModelProviderFailureKind::Overloaded,
        400..=499 => ModelProviderFailureKind::RequestRejected,
        500..=599 => ModelProviderFailureKind::Server,
        _ => ModelProviderFailureKind::Protocol,
    };
    provider_failure(
        kind,
        format!("{provider} returned HTTP status {status}"),
        Some(status),
        retry_after_ms,
    )
}

pub(super) fn provider_failure(
    kind: ModelProviderFailureKind,
    message: impl Into<String>,
    http_status: Option<u16>,
    retry_after_ms: Option<u64>,
) -> HarnessError {
    match ModelProviderFailure::new(kind, message, http_status, retry_after_ms) {
        Ok(failure) => HarnessError::ModelProvider(failure),
        Err(_) => {
            HarnessError::InvalidCapability("model Provider failure evidence is invalid".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::{
        HttpModelRequest, HttpModelResponse, HttpModelTransport, HttpsJsonModel,
        HttpsJsonModelConfig, MAX_ROOT_CA_PEM_BYTES, MODEL_GATEWAY_API_VERSION,
        ModelGatewayStreamDecoder, ReqwestHttpModelTransport, parse_stream_frame,
        provider_http_failure,
    };
    use crate::{
        HarnessError, HarnessFuture, LanguageModel, ModelEventSink, ModelOutput,
        ModelProviderFailureKind, ModelRequest, ModelResponse, ModelStream, ModelStreamEvent,
        SECRET_API_VERSION, SecretProvider, SecretProviderDescriptor, SecretReference,
        SecretRequest, SecretValue, ThreadId, TurnId,
    };

    #[test]
    fn http_status_mapping_preserves_facts_without_inventing_policy() {
        let cases = [
            (401, ModelProviderFailureKind::Authentication),
            (403, ModelProviderFailureKind::Authorization),
            (429, ModelProviderFailureKind::RateLimited),
            (503, ModelProviderFailureKind::Server),
            (529, ModelProviderFailureKind::Overloaded),
            (500, ModelProviderFailureKind::Server),
            (400, ModelProviderFailureKind::RequestRejected),
            (302, ModelProviderFailureKind::Protocol),
        ];
        for (status, expected) in cases {
            let error = provider_http_failure("fixture", status, Some(1_000));
            let HarnessError::ModelProvider(failure) = error else {
                panic!("expected typed Provider failure");
            };
            assert_eq!(failure.kind(), expected);
            assert_eq!(failure.http_status(), Some(status));
            assert_eq!(failure.retry_after_ms(), Some(1_000));
        }
    }

    struct FixedSecret;

    struct TenantSecret;

    impl SecretProvider for FixedSecret {
        fn descriptor(&self) -> SecretProviderDescriptor {
            SecretProviderDescriptor {
                name: "fixed".to_owned(),
                description: "Test credential".to_owned(),
                api_version: SECRET_API_VERSION,
            }
        }

        fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async { SecretValue::new(b"fixture-token".to_vec()) })
        }
    }

    impl SecretProvider for TenantSecret {
        fn descriptor(&self) -> SecretProviderDescriptor {
            SecretProviderDescriptor {
                name: "tenant".to_owned(),
                description: "Test tenant credential".to_owned(),
                api_version: SECRET_API_VERSION,
            }
        }

        fn resolve<'a>(&'a self, _request: SecretRequest) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async { Err(HarnessError::Secret("tenant authority required".to_owned())) })
        }

        fn resolve_as<'a>(
            &'a self,
            _request: SecretRequest,
            authority: &'a crate::AuthorityContext,
        ) -> HarnessFuture<'a, SecretValue> {
            Box::pin(async move {
                if authority.tenant_id() != Some("tenant-a") {
                    return Err(HarnessError::Secret(
                        "tenant credential unavailable".to_owned(),
                    ));
                }
                SecretValue::new(b"tenant-token".to_vec())
            })
        }
    }

    #[derive(Debug, PartialEq)]
    struct RecordedRequest {
        endpoint: String,
        api_version: String,
        bearer: Vec<u8>,
        body: ModelRequest,
    }

    struct RecordingTransport {
        recorded: Mutex<Vec<RecordedRequest>>,
        api_version: Option<String>,
        response_body: Vec<u8>,
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<ModelStreamEvent>>,
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

    struct StreamingTransport;

    struct FailureTransport;

    impl HttpModelTransport for FailureTransport {
        fn send<'a>(&'a self, _request: HttpModelRequest) -> HarnessFuture<'a, HttpModelResponse> {
            Box::pin(async {
                Ok(HttpModelResponse {
                    status: 429,
                    api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
                    content_type: Some("application/json".to_owned()),
                    provider_request_id: Some("failed-request".to_owned()),
                    body: br#"{"secret":"must-not-appear"}"#.to_vec(),
                })
            })
        }
    }

    impl HttpModelTransport for StreamingTransport {
        fn send<'a>(&'a self, _request: HttpModelRequest) -> HarnessFuture<'a, HttpModelResponse> {
            Box::pin(async {
                Err(HarnessError::Model(
                    "non-streaming transport path used".to_owned(),
                ))
            })
        }

        fn send_streaming<'a>(
            &'a self,
            _request: HttpModelRequest,
            stream: ModelStream,
        ) -> HarnessFuture<'a, HttpModelResponse> {
            Box::pin(async move {
                let _ = stream.emit_text_delta("hel");
                let _ = stream.emit_text_delta("lo");
                let response = ModelResponse::from(ModelOutput::Message {
                    content: "hello".to_owned(),
                });
                Ok(HttpModelResponse {
                    status: 200,
                    api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
                    content_type: Some("application/json".to_owned()),
                    provider_request_id: Some("stream-request".to_owned()),
                    body: serde_json::to_vec(&response).expect("response"),
                })
            })
        }
    }

    impl HttpModelTransport for RecordingTransport {
        fn send<'a>(&'a self, request: HttpModelRequest) -> HarnessFuture<'a, HttpModelResponse> {
            Box::pin(async move {
                self.recorded
                    .lock()
                    .expect("recorded")
                    .push(RecordedRequest {
                        endpoint: request.endpoint,
                        api_version: request.api_version,
                        bearer: request.bearer_secret.expose_bytes().to_vec(),
                        body: serde_json::from_slice(&request.body).expect("request body"),
                    });
                Ok(HttpModelResponse {
                    status: 200,
                    api_version: self.api_version.clone(),
                    content_type: Some("application/json; charset=utf-8".to_owned()),
                    provider_request_id: Some("gateway-request".to_owned()),
                    body: self.response_body.clone(),
                })
            })
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            thread_id: ThreadId::from_static("thread"),
            turn_id: TurnId::from_static("turn"),
            authority: crate::AuthorityContext::local_process(),
            items: Vec::new(),
            context: Vec::new(),
            tools: Vec::new(),
        }
    }

    #[tokio::test]
    async fn sends_bounded_authenticated_gateway_contract() {
        let mut response = ModelResponse::from(ModelOutput::Message {
            content: "done".to_owned(),
        });
        response.provider_model = Some("provider/model-v2".to_owned());
        let transport = Arc::new(RecordingTransport {
            recorded: Mutex::new(Vec::new()),
            api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
            response_body: serde_json::to_vec(&response).expect("response"),
        });
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/v1/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            transport.clone(),
        )
        .expect("model");
        let response = model
            .complete_with_metadata(request())
            .await
            .expect("completion");

        assert_eq!(
            response.output,
            ModelOutput::Message {
                content: "done".to_owned()
            }
        );
        assert_eq!(
            response.provider_request_id.as_deref(),
            Some("gateway-request")
        );
        assert_eq!(
            response.provider_model.as_deref(),
            Some("provider/model-v2")
        );
        let recorded = transport.recorded.lock().expect("recorded");
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].bearer, b"fixture-token");
        assert_eq!(recorded[0].api_version, MODEL_GATEWAY_API_VERSION);
        assert_eq!(
            recorded[0].endpoint,
            "https://models.example.test/v1/complete"
        );
        assert_eq!(recorded[0].body, request());
    }

    #[tokio::test]
    async fn direct_gateway_resolves_credentials_with_in_process_turn_authority() {
        let response = ModelResponse::from(ModelOutput::Message {
            content: "done".to_owned(),
        });
        let transport = Arc::new(RecordingTransport {
            recorded: Mutex::new(Vec::new()),
            api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
            response_body: serde_json::to_vec(&response).expect("response"),
        });
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/v1/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(TenantSecret),
            transport.clone(),
        )
        .expect("model");
        let mut request = request();
        request.authority = crate::AuthorityContext::new(
            crate::ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "model-caller".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("tenant authority");
        model.complete(request).await.expect("completion");

        let recorded = transport.recorded.lock().expect("recorded");
        assert_eq!(recorded[0].bearer, b"tenant-token");
        assert_eq!(
            recorded[0].body.authority,
            crate::AuthorityContext::local_process(),
            "trusted authority must not enter the serialized provider body"
        );
    }

    #[tokio::test]
    async fn gateway_status_becomes_typed_evidence_without_response_body() {
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/v1/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            Arc::new(FailureTransport),
        )
        .expect("model");

        let error = model
            .complete(request())
            .await
            .expect_err("provider status");

        let HarnessError::ModelProvider(failure) = error else {
            panic!("expected typed Provider failure");
        };
        assert_eq!(failure.kind(), ModelProviderFailureKind::RateLimited);
        assert_eq!(failure.http_status(), Some(429));
        assert!(!failure.message().contains("must-not-appear"));
    }

    #[tokio::test]
    async fn direct_gateway_adapter_rejects_deep_model_json() {
        let mut deeply_nested = serde_json::Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            deeply_nested = serde_json::Value::Array(vec![deeply_nested]);
        }
        let response = ModelResponse::from(ModelOutput::ToolCall {
            call_id: "call-deep".to_owned(),
            name: "fixture".to_owned(),
            input: deeply_nested,
        });
        let transport = Arc::new(RecordingTransport {
            recorded: Mutex::new(Vec::new()),
            api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
            response_body: serde_json::to_vec(&response).expect("response"),
        });
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/v1/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            transport,
        )
        .expect("model");

        let error = model
            .complete(request())
            .await
            .expect_err("deep model output");

        assert!(error.to_string().contains("depth or node count"));
    }

    #[tokio::test]
    async fn streaming_adapter_emits_provisional_deltas_and_keeps_final_authority() {
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/v1/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            Arc::new(StreamingTransport),
        )
        .expect("model");
        let sink = Arc::new(RecordingSink::default());
        let stream = ModelStream::new(sink.clone()).for_step(4);
        let response = model
            .complete_streaming(request(), stream)
            .await
            .expect("streaming completion");

        assert_eq!(
            response.output,
            ModelOutput::Message {
                content: "hello".to_owned()
            }
        );
        assert_eq!(
            sink.events.lock().expect("events").as_slice(),
            [
                ModelStreamEvent::TextDelta {
                    model_step: 4,
                    delta: "hel".to_owned(),
                },
                ModelStreamEvent::TextDelta {
                    model_step: 4,
                    delta: "lo".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn ndjson_frames_are_exact_bounded_and_final() {
        let sink = Arc::new(RecordingSink::default());
        let stream = ModelStream::new(sink.clone()).for_step(2);
        let mut frame_count = 0;
        let mut final_response = None;
        parse_stream_frame(
            br#"{"type":"text_delta","delta":"piece"}"#,
            &stream,
            &mut frame_count,
            &mut final_response,
        )
        .expect("delta");
        let response = ModelResponse::from(ModelOutput::Message {
            content: "final".to_owned(),
        });
        let final_frame = serde_json::to_vec(&json!({
            "type": "response",
            "response": response
        }))
        .expect("frame");
        parse_stream_frame(&final_frame, &stream, &mut frame_count, &mut final_response)
            .expect("final response");
        assert_eq!(frame_count, 2);
        assert_eq!(
            final_response.expect("response").output,
            ModelOutput::Message {
                content: "final".to_owned()
            }
        );
        assert_eq!(
            sink.events.lock().expect("events").as_slice(),
            [ModelStreamEvent::TextDelta {
                model_step: 2,
                delta: "piece".to_owned(),
            }]
        );
        assert!(
            parse_stream_frame(
                br#"{"type":"text_delta","delta":"late"}"#,
                &stream,
                &mut frame_count,
                &mut Some(ModelResponse::from(ModelOutput::Message {
                    content: "done".to_owned(),
                })),
            )
            .is_err()
        );
        assert!(
            parse_stream_frame(
                br#"{"type":"text_delta","delta":"x","extra":true}"#,
                &stream,
                &mut 0,
                &mut None,
            )
            .is_err()
        );

        let mut invalid_model = ModelResponse::from(ModelOutput::Message {
            content: "untrusted".to_owned(),
        });
        invalid_model.provider_model = Some("\n".to_owned());
        let invalid_model_frame = serde_json::to_vec(&json!({
            "type": "response",
            "response": invalid_model
        }))
        .expect("frame");
        assert!(parse_stream_frame(&invalid_model_frame, &stream, &mut 0, &mut None,).is_err());
    }

    #[test]
    fn ndjson_decoder_handles_fragmented_utf8_and_requires_final_response() {
        let response = ModelResponse::from(ModelOutput::Message {
            content: "完成".to_owned(),
        });
        let wire = [
            serde_json::to_string(&json!({
                "type": "text_delta",
                "delta": "你"
            }))
            .expect("delta"),
            serde_json::to_string(&json!({
                "type": "response",
                "response": response
            }))
            .expect("response"),
        ]
        .join("\n")
            + "\n";
        let sink = Arc::new(RecordingSink::default());
        let mut decoder =
            ModelGatewayStreamDecoder::new(ModelStream::new(sink.clone()).for_step(1), wire.len());
        for chunk in wire.as_bytes().chunks(3) {
            decoder.push(chunk).expect("fragment");
        }
        assert_eq!(
            decoder.finish().expect("final").output,
            ModelOutput::Message {
                content: "完成".to_owned()
            }
        );
        assert_eq!(
            sink.events.lock().expect("events").as_slice(),
            [ModelStreamEvent::TextDelta {
                model_step: 1,
                delta: "你".to_owned(),
            }]
        );

        let mut missing = ModelGatewayStreamDecoder::new(ModelStream::disabled(), 128);
        missing
            .push(br#"{"type":"text_delta","delta":"only"}"#)
            .expect("delta");
        assert!(missing.finish().is_err());
        let mut oversized = ModelGatewayStreamDecoder::new(ModelStream::disabled(), 1);
        assert!(oversized.push(b"{}").is_err());
    }

    #[test]
    fn rejects_credential_leaking_or_unencrypted_endpoint_shapes() {
        let reference = SecretReference::new("model/gateway").expect("reference");
        for endpoint in [
            "http://models.example.test/complete",
            "https://user:pass@models.example.test/complete",
            "https://models.example.test/complete?target=other",
            "https://models.example.test/complete#fragment",
        ] {
            assert!(
                HttpsJsonModelConfig::new(endpoint, reference.clone()).is_err(),
                "{endpoint}"
            );
        }
        let config = HttpsJsonModelConfig::new("https://models.example.test/complete", reference)
            .expect("valid config");
        HttpsJsonModel::new("gateway/model", config, Arc::new(FixedSecret))
            .expect("built-in HTTPS model transport");
    }

    #[test]
    fn validates_and_redacts_exclusive_enterprise_roots() {
        let reference = SecretReference::new("model/gateway").expect("reference");
        let base =
            HttpsJsonModelConfig::new("https://models.example.test/complete", reference.clone())
                .expect("base config");
        assert!(
            base.clone()
                .with_exclusive_root_certificates_pem(Vec::new())
                .is_err()
        );
        assert!(
            base.clone()
                .with_exclusive_root_certificates_pem(b"not a PEM certificate".to_vec())
                .is_err()
        );
        assert!(
            base.with_exclusive_root_certificates_pem(vec![b'x'; MAX_ROOT_CA_PEM_BYTES + 1])
                .is_err()
        );

        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("test certificate");
        let pem = certified.cert.pem();
        let config = HttpsJsonModelConfig::new("https://models.example.test/complete", reference)
            .expect("base config")
            .with_exclusive_root_certificates_pem(pem.as_bytes().to_vec())
            .expect("exclusive root");
        let debug = format!("{config:?}");
        assert!(debug.contains("exclusive_root_certificates: true"));
        assert!(!debug.contains("BEGIN CERTIFICATE"));
        assert!(
            ReqwestHttpModelTransport::new_with_client_identity(
                &config,
                SecretValue::new(b"not an identity".to_vec()).expect("bounded secret"),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn rejects_malformed_gateway_response_without_echoing_body() {
        let transport = Arc::new(RecordingTransport {
            recorded: Mutex::new(Vec::new()),
            api_version: Some(MODEL_GATEWAY_API_VERSION.to_owned()),
            response_body: serde_json::to_vec(&json!({
                "secret": "must-not-appear",
                "invalid": true
            }))
            .expect("response"),
        });
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            transport,
        )
        .expect("model");
        let error = model
            .complete(request())
            .await
            .expect_err("invalid response");
        assert!(matches!(
            error,
            HarnessError::ModelProvider(ref failure)
                if failure.kind() == ModelProviderFailureKind::Protocol
        ));
        assert!(
            error
                .to_string()
                .contains("HTTPS model returned invalid JSON")
        );
        assert!(!error.to_string().contains("must-not-appear"));
    }

    #[tokio::test]
    async fn rejects_gateway_protocol_mismatch_before_body_use() {
        let response = ModelResponse::from(ModelOutput::Message {
            content: "must-not-be-used".to_owned(),
        });
        let transport = Arc::new(RecordingTransport {
            recorded: Mutex::new(Vec::new()),
            api_version: Some("999".to_owned()),
            response_body: serde_json::to_vec(&response).expect("response"),
        });
        let config = HttpsJsonModelConfig::new(
            "https://models.example.test/complete",
            SecretReference::new("model/gateway").expect("reference"),
        )
        .expect("config");
        let model = HttpsJsonModel::with_transport(
            "gateway/model",
            config,
            Arc::new(FixedSecret),
            transport,
        )
        .expect("model");

        let error = model.complete(request()).await.expect_err("mismatch");
        assert!(matches!(
            error,
            HarnessError::ModelProvider(ref failure)
                if failure.kind() == ModelProviderFailureKind::Protocol
        ));
        assert!(error.to_string().contains(&format!(
            "HTTPS model gateway API mismatch; expected {MODEL_GATEWAY_API_VERSION}"
        )));
    }
}
