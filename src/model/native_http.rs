//! Shared transport policy for first-party native HTTPS model adapters.

use std::{future::Future, net::IpAddr, sync::Arc, time::Duration};

use reqwest::{
    Response, Url,
    header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER},
};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use super::{provider_failure, provider_http_failure};
use crate::{HarnessError, ModelProviderFailureKind, SecretValue};

pub(super) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
pub(super) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const DEFAULT_MAX_RESPONSE_BYTES: usize = 4_194_304;
pub(super) const DEFAULT_MAX_CONCURRENCY: usize = 16;
pub(super) const MAX_RESPONSE_BYTES: usize = 16_777_216;
pub(super) const MAX_CONCURRENCY: usize = 256;
pub(super) const MAX_TIMEOUT: Duration = Duration::from_secs(86_400);
pub(super) const MAX_MODEL_NAME_BYTES: usize = 256;
pub(super) const MAX_ENDPOINT_BYTES: usize = 2_048;
pub(super) const MAX_REQUEST_BYTES: usize = 16_777_216;
pub(super) const MAX_STREAM_EVENTS: usize = 4_096;

#[derive(Clone, Debug)]
pub(super) struct NativeHttpSettings {
    pub endpoint: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_response_bytes: usize,
    pub max_concurrency: usize,
    pub allow_loopback_http: bool,
}

impl NativeHttpSettings {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            allow_loopback_http: false,
        }
    }

    pub fn with_limits(
        mut self,
        request_timeout: Duration,
        connect_timeout: Duration,
        max_response_bytes: usize,
        max_concurrency: usize,
    ) -> Self {
        self.request_timeout = request_timeout;
        self.connect_timeout = connect_timeout;
        self.max_response_bytes = max_response_bytes;
        self.max_concurrency = max_concurrency;
        self
    }

    pub fn with_loopback_http(mut self, allow: bool) -> Self {
        self.allow_loopback_http = allow;
        self
    }

    pub fn validate(&self, provider: &str) -> Result<Url, HarnessError> {
        if self.endpoint.is_empty()
            || self.endpoint.len() > MAX_ENDPOINT_BYTES
            || self.endpoint.chars().any(char::is_control)
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "{provider} endpoint must contain 1..={MAX_ENDPOINT_BYTES} non-control bytes"
            )));
        }
        let endpoint = Url::parse(&self.endpoint).map_err(|_| {
            HarnessError::InvalidConfiguration(format!(
                "{provider} endpoint must be an absolute URL"
            ))
        })?;
        let loopback_http = self.allow_loopback_http
            && endpoint.scheme() == "http"
            && endpoint
                .host_str()
                .and_then(|host| host.parse::<IpAddr>().ok())
                .is_some_and(|address| address.is_loopback());
        if (endpoint.scheme() != "https" && !loopback_http)
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "{provider} endpoint must use HTTPS, or explicitly allowed HTTP on a literal loopback IP, with no userinfo, query, or fragment"
            )));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > MAX_TIMEOUT
            || self.connect_timeout < Duration::from_millis(1)
            || self.connect_timeout > self.request_timeout
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "{provider} timeouts must be at least 1 millisecond, connect must not exceed request, and request must not exceed {} seconds",
                MAX_TIMEOUT.as_secs()
            )));
        }
        if !(1..=MAX_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "{provider} response limit must be 1-{MAX_RESPONSE_BYTES} bytes"
            )));
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "{provider} concurrency must be 1-{MAX_CONCURRENCY}"
            )));
        }
        Ok(endpoint)
    }
}

pub(super) fn validate_vendor_model(provider: &str, model: &str) -> Result<(), HarnessError> {
    if model.trim().is_empty()
        || model.len() > MAX_MODEL_NAME_BYTES
        || model.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{provider} model must be 1-{MAX_MODEL_NAME_BYTES} trimmed non-control bytes"
        )));
    }
    Ok(())
}

pub(super) struct NativeHttpClient {
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
    request_timeout: Duration,
}

impl NativeHttpClient {
    pub fn new(provider: &str, settings: &NativeHttpSettings) -> Result<Self, HarnessError> {
        settings.validate(provider)?;
        let client = reqwest::Client::builder()
            .https_only(!settings.allow_loopback_http)
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_proxy()
            .connect_timeout(settings.connect_timeout)
            .timeout(settings.request_timeout)
            .user_agent(concat!("y-harness/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                HarnessError::InvalidConfiguration(format!(
                    "failed to build {provider} HTTPS transport"
                ))
            })?;
        Ok(Self {
            client,
            concurrency: Arc::new(Semaphore::new(settings.max_concurrency)),
            request_timeout: settings.request_timeout,
        })
    }

    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub async fn run<T>(
        &self,
        provider: &str,
        operation: impl Future<Output = Result<T, HarnessError>>,
    ) -> Result<T, HarnessError> {
        tokio::time::timeout(self.request_timeout, async {
            let _permit = self.concurrency.acquire().await.map_err(|_| {
                HarnessError::Model(format!("{provider} model transport is closed"))
            })?;
            operation.await
        })
        .await
        .map_err(|_| {
            provider_failure(
                ModelProviderFailureKind::Transport,
                format!("{provider} model operation timed out"),
                None,
                None,
            )
        })?
    }
}

pub(super) fn secret_header(
    provider: &str,
    secret: &SecretValue,
) -> Result<HeaderValue, HarnessError> {
    let encoded = Zeroizing::new(secret.expose_bytes().to_vec());
    let mut value = HeaderValue::from_bytes(encoded.as_slice()).map_err(|_| {
        HarnessError::Model(format!("{provider} credential is not a valid HTTP header"))
    })?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn validate_response_head(
    provider: &str,
    response: &Response,
    maximum: usize,
    expected_content_type: &str,
) -> Result<(), HarnessError> {
    if !response.status().is_success() {
        return Err(provider_http_failure(
            provider,
            response.status().as_u16(),
            retry_after_ms(response.headers()),
        ));
    }
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > maximum as u64
    {
        return Err(protocol_failure(
            provider,
            "response declared an oversized body",
        ));
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected_content_type))
    {
        return Err(protocol_failure(
            provider,
            "returned an unexpected content type",
        ));
    }
    Ok(())
}

pub(super) async fn read_bounded_body(
    provider: &str,
    mut response: Response,
    maximum: usize,
) -> Result<Vec<u8>, HarnessError> {
    let mut body = Vec::with_capacity(maximum.min(8_192));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| map_transport_error(provider, error))?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| protocol_failure(provider, "response size overflow"))?;
        if next > maximum {
            return Err(protocol_failure(
                provider,
                "response exceeded its configured limit",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(super) fn provider_request_id(
    provider: &str,
    headers: &HeaderMap,
    names: &[&str],
) -> Result<Option<String>, HarnessError> {
    for name in names {
        if let Some(value) = headers.get(*name) {
            let value = value
                .to_str()
                .map_err(|_| protocol_failure(provider, "returned an invalid request id"))?;
            if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return Err(protocol_failure(provider, "returned an invalid request id"));
            }
            return Ok(Some(value.to_owned()));
        }
    }
    Ok(None)
}

pub(super) fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let seconds = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()?;
    seconds
        .checked_mul(1_000)
        .filter(|value| *value > 0 && *value <= crate::MAX_MODEL_PROVIDER_RETRY_AFTER_MS)
}

pub(super) fn map_transport_error(provider: &str, error: reqwest::Error) -> HarnessError {
    let message = if error.is_timeout() {
        "transport timed out"
    } else if error.is_connect() {
        "transport connection failed"
    } else if error.is_body() || error.is_decode() {
        "transport body failed"
    } else {
        "transport request failed"
    };
    provider_failure(
        ModelProviderFailureKind::Transport,
        format!("{provider} {message}"),
        None,
        None,
    )
}

pub(super) fn protocol_failure(provider: &str, message: impl AsRef<str>) -> HarnessError {
    provider_failure(
        ModelProviderFailureKind::Protocol,
        format!("{provider} {}", message.as_ref()),
        None,
        None,
    )
}
