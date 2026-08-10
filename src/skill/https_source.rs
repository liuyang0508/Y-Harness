//! Pinned, bounded HTTPS acquisition for signed declarative Skill packages.

use std::{fmt, sync::Arc, time::Duration};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

use super::{SignedSkillPackage, SkillId, SkillRegistry, SkillTrustStore, validate_package};
use crate::{
    CapabilityOrigin, HarnessError, HarnessFuture, SecretValue, kernel::validate_capability_name,
};

const MAX_HTTPS_SKILL_URL_BYTES: usize = 8_192;
const MAX_HTTPS_SKILL_RESPONSE_BYTES: usize = 16_777_216;
const MAX_HTTPS_SKILL_CONCURRENCY: usize = 64;
const MAX_HTTPS_SKILL_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_ROOT_CA_PEM_BYTES: usize = 1_048_576;
const MAX_ROOT_CA_CERTIFICATES: usize = 64;
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 16_777_216;
const DEFAULT_MAX_CONCURRENCY: usize = 8;

/// Validated policy for one exact remote signed-package URL.
#[derive(Clone)]
pub struct HttpsSkillSourceConfig {
    endpoint: String,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_response_bytes: usize,
    max_concurrency: usize,
    exclusive_root_ca_pem: Option<Arc<[u8]>>,
}

impl HttpsSkillSourceConfig {
    /// Creates conservative defaults for one operator-configured HTTPS URL.
    pub fn new(endpoint: impl Into<String>) -> Result<Self, HarnessError> {
        let config = Self {
            endpoint: endpoint.into(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            exclusive_root_ca_pem: None,
        };
        config.validate()?;
        Ok(config)
    }

    /// Replaces transport time, response-retention, and concurrency bounds.
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

    /// Trusts only the supplied bounded PEM CA bundle for this source.
    ///
    /// This is intended for an operator-configured private Registry. Ambient
    /// native and WebPKI roots are disabled when this option is present.
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
        if self.endpoint.len() > MAX_HTTPS_SKILL_URL_BYTES {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Skill source URL exceeds {MAX_HTTPS_SKILL_URL_BYTES} bytes"
            )));
        }
        let endpoint = reqwest::Url::parse(&self.endpoint).map_err(|_| {
            HarnessError::InvalidConfiguration(
                "HTTPS Skill source must be an absolute URL".to_owned(),
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
                "Skill source must use HTTPS with a host and no userinfo, query, or fragment"
                    .to_owned(),
            ));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > MAX_HTTPS_SKILL_TIMEOUT
            || self.connect_timeout < Duration::from_millis(1)
            || self.connect_timeout > self.request_timeout
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Skill source timeouts require 1 millisecond <= connect <= request <= {} seconds",
                MAX_HTTPS_SKILL_TIMEOUT.as_secs()
            )));
        }
        if !(1..=MAX_HTTPS_SKILL_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Skill response limit must be 1-{MAX_HTTPS_SKILL_RESPONSE_BYTES} bytes"
            )));
        }
        if !(1..=MAX_HTTPS_SKILL_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "Skill source concurrency must be 1-{MAX_HTTPS_SKILL_CONCURRENCY}"
            )));
        }
        if let Some(pem) = &self.exclusive_root_ca_pem {
            let _certificates = parse_exclusive_root_certificates(pem)?;
        }
        Ok(())
    }
}

impl fmt::Debug for HttpsSkillSourceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpsSkillSourceConfig")
            .field("endpoint", &self.endpoint)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_concurrency", &self.max_concurrency)
            .field(
                "exclusive_root_ca_pem",
                &self.exclusive_root_ca_pem.is_some(),
            )
            .finish()
    }
}

/// One request-scoped credential for a pinned Skill source.
///
/// The value is intentionally neither cloneable nor serializable. Callers
/// should resolve it immediately before one request so rotation and tenant
/// authority are re-evaluated for every network operation.
pub enum HttpSkillAuthorization {
    /// RFC 6750-style Bearer authorization.
    Bearer(SecretValue),
}

/// One bounded GET issued to a trusted HTTPS transport implementation.
pub struct HttpSkillRequest {
    /// Exact validated package URL.
    pub endpoint: String,
    /// Total operation bound.
    pub timeout: Duration,
    /// Maximum retained response bytes.
    pub max_response_bytes: usize,
    /// Optional request-scoped authorization selected by the trusted host.
    pub authorization: Option<HttpSkillAuthorization>,
}

/// Content-free metadata plus retained successful response bytes.
pub struct HttpSkillResponse {
    /// Numeric HTTP status.
    pub status: u16,
    /// Response media type when present.
    pub content_type: Option<String>,
    /// Retained body; transports leave this empty for non-success statuses.
    pub body: Vec<u8>,
}

/// Replaceable transport authority for pinned remote Skill acquisition.
///
/// Custom implementations are trusted host components and must provide the
/// same TLS, no-redirect, no-retry, no-proxy, timeout, and body-bound
/// guarantees as the built-in implementation.
pub trait HttpSkillTransport: Send + Sync {
    /// Fetches one exact URL without following indirection.
    fn fetch<'a>(&'a self, request: HttpSkillRequest) -> HarnessFuture<'a, HttpSkillResponse>;
}

/// Reqwest transport with pooled HTTPS and fixed ambient-authority policy.
pub struct ReqwestHttpSkillTransport {
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
}

impl ReqwestHttpSkillTransport {
    /// Builds a reusable client from one validated source configuration.
    pub fn new(config: &HttpsSkillSourceConfig) -> Result<Self, HarnessError> {
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
            HarnessError::InvalidConfiguration("failed to build HTTPS Skill transport".to_owned())
        })?;
        Ok(Self {
            client,
            concurrency: Arc::new(Semaphore::new(config.max_concurrency)),
        })
    }
}

impl HttpSkillTransport for ReqwestHttpSkillTransport {
    fn fetch<'a>(&'a self, request: HttpSkillRequest) -> HarnessFuture<'a, HttpSkillResponse> {
        Box::pin(async move {
            tokio::time::timeout(request.timeout, async {
                let _permit = self.concurrency.acquire().await.map_err(|_| {
                    HarnessError::Skill("HTTPS Skill transport is closed".to_owned())
                })?;
                execute_http_fetch(&self.client, request).await
            })
            .await
            .map_err(|_| HarnessError::Skill("HTTPS Skill request timed out".to_owned()))?
        })
    }
}

/// Exact-identity and digest-pinned remote signed-package source.
pub struct HttpsSkillSource {
    config: HttpsSkillSourceConfig,
    transport: Arc<dyn HttpSkillTransport>,
}

impl HttpsSkillSource {
    /// Creates a source over the built-in pooled Reqwest transport.
    pub fn new(config: HttpsSkillSourceConfig) -> Result<Self, HarnessError> {
        let transport = Arc::new(ReqwestHttpSkillTransport::new(&config)?);
        Self::with_transport(config, transport)
    }

    /// Creates a source over a host-supplied trusted transport.
    pub fn with_transport(
        config: HttpsSkillSourceConfig,
        transport: Arc<dyn HttpSkillTransport>,
    ) -> Result<Self, HarnessError> {
        config.validate()?;
        Ok(Self { config, transport })
    }

    /// Fetches and validates one untrusted signed package against exact pins.
    ///
    /// This checks package structure, identity, and content digest. Publisher
    /// and transparency trust are checked only by `SkillRegistry`.
    pub async fn fetch(
        &self,
        expected: &SkillId,
        expected_sha256: &str,
    ) -> Result<SignedSkillPackage, HarnessError> {
        self.fetch_with_authorization(expected, expected_sha256, None)
            .await
    }

    /// Fetches one exact package with a request-scoped Bearer credential.
    pub async fn fetch_with_bearer(
        &self,
        expected: &SkillId,
        expected_sha256: &str,
        credential: SecretValue,
    ) -> Result<SignedSkillPackage, HarnessError> {
        self.fetch_with_authorization(
            expected,
            expected_sha256,
            Some(HttpSkillAuthorization::Bearer(credential)),
        )
        .await
    }

    async fn fetch_with_authorization(
        &self,
        expected: &SkillId,
        expected_sha256: &str,
        authorization: Option<HttpSkillAuthorization>,
    ) -> Result<SignedSkillPackage, HarnessError> {
        validate_expected_pin(expected, expected_sha256)?;
        tokio::time::timeout(
            self.config.request_timeout,
            self.fetch_inner(expected, expected_sha256, authorization),
        )
        .await
        .map_err(|_| HarnessError::Skill("HTTPS Skill operation timed out".to_owned()))?
    }

    /// Fetches, pin-checks, verifies, and atomically registers one external Skill.
    pub async fn fetch_and_register(
        &self,
        registry: &mut SkillRegistry,
        origin: CapabilityOrigin,
        expected: &SkillId,
        expected_sha256: &str,
        trust: &SkillTrustStore,
    ) -> Result<(), HarnessError> {
        let signed = self.fetch(expected, expected_sha256).await?;
        registry.register_signed(origin, signed, trust)
    }

    async fn fetch_inner(
        &self,
        expected: &SkillId,
        expected_sha256: &str,
        authorization: Option<HttpSkillAuthorization>,
    ) -> Result<SignedSkillPackage, HarnessError> {
        let response = self
            .transport
            .fetch(HttpSkillRequest {
                endpoint: self.config.endpoint.clone(),
                timeout: self.config.request_timeout,
                max_response_bytes: self.config.max_response_bytes,
                authorization,
            })
            .await
            .map_err(|_| HarnessError::Skill("remote Skill transport failed".to_owned()))?;
        if !(200..300).contains(&response.status) {
            return Err(HarnessError::Skill(format!(
                "remote Skill source returned HTTP status {}",
                response.status
            )));
        }
        if response.body.len() > self.config.max_response_bytes {
            return Err(HarnessError::Skill(
                "remote Skill response exceeded its configured limit".to_owned(),
            ));
        }
        if !response
            .content_type
            .as_deref()
            .is_some_and(is_json_media_type)
        {
            return Err(HarnessError::Skill(
                "remote Skill response must be application/json".to_owned(),
            ));
        }
        let signed: SignedSkillPackage = serde_json::from_slice(&response.body)
            .map_err(|_| HarnessError::Skill("remote Skill response is malformed".to_owned()))?;
        validate_package(&signed.package)?;
        let actual = SkillId {
            name: signed.package.manifest.name.clone(),
            version: signed.package.manifest.version.clone(),
        };
        if &actual != expected {
            return Err(HarnessError::Skill(format!(
                "remote Skill identity does not match expected {}@{}",
                expected.name, expected.version
            )));
        }
        if signed.package.content_sha256 != expected_sha256 {
            return Err(HarnessError::Skill(format!(
                "remote Skill {}@{} does not match its expected content pin",
                expected.name, expected.version
            )));
        }
        Ok(signed)
    }
}

async fn execute_http_fetch(
    client: &reqwest::Client,
    request: HttpSkillRequest,
) -> Result<HttpSkillResponse, HarnessError> {
    let HttpSkillRequest {
        endpoint,
        timeout: _,
        max_response_bytes,
        authorization,
    } = request;
    let mut builder = client.get(&endpoint).header(ACCEPT, "application/json");
    if let Some(HttpSkillAuthorization::Bearer(secret)) = authorization {
        builder = builder.header(AUTHORIZATION, bearer_header(secret)?);
    }
    let mut response = builder
        .send()
        .await
        .map_err(|_| HarnessError::Skill("HTTPS Skill request failed".to_owned()))?;
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > max_response_bytes as u64
    {
        return Err(HarnessError::Skill(
            "HTTPS Skill response declared an oversized body".to_owned(),
        ));
    }
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !(200..300).contains(&status) {
        return Ok(HttpSkillResponse {
            status,
            content_type,
            body: Vec::new(),
        });
    }

    let mut body = Vec::with_capacity(max_response_bytes.min(8_192));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| HarnessError::Skill("HTTPS Skill response read failed".to_owned()))?
    {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| HarnessError::Skill("HTTPS Skill response size overflow".to_owned()))?;
        if next > max_response_bytes {
            return Err(HarnessError::Skill(
                "HTTPS Skill response exceeded its configured limit".to_owned(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(HttpSkillResponse {
        status,
        content_type,
        body,
    })
}

fn bearer_header(secret: SecretValue) -> Result<HeaderValue, HarnessError> {
    let mut encoded = Zeroizing::new(Vec::with_capacity(
        "Bearer ".len().saturating_add(secret.expose_bytes().len()),
    ));
    encoded.extend_from_slice(b"Bearer ");
    encoded.extend_from_slice(secret.expose_bytes());
    let mut value = HeaderValue::from_bytes(encoded.as_slice()).map_err(|_| {
        HarnessError::Skill("private Skill Registry credential is not a valid HTTP header".into())
    })?;
    value.set_sensitive(true);
    Ok(value)
}

fn parse_exclusive_root_certificates(
    pem: &[u8],
) -> Result<Vec<reqwest::Certificate>, HarnessError> {
    if pem.is_empty() || pem.len() > MAX_ROOT_CA_PEM_BYTES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive Skill source root CA bundle must be 1-{MAX_ROOT_CA_PEM_BYTES} bytes"
        )));
    }
    let certificates = reqwest::Certificate::from_pem_bundle(pem).map_err(|_| {
        HarnessError::InvalidConfiguration(
            "exclusive Skill source root CA bundle is not valid PEM".to_owned(),
        )
    })?;
    if certificates.is_empty() || certificates.len() > MAX_ROOT_CA_CERTIFICATES {
        return Err(HarnessError::InvalidConfiguration(format!(
            "exclusive Skill source root CA bundle must contain 1-{MAX_ROOT_CA_CERTIFICATES} certificates"
        )));
    }
    Ok(certificates)
}

fn validate_expected_pin(expected: &SkillId, digest: &str) -> Result<(), HarnessError> {
    validate_capability_name("expected Skill", &expected.name)?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(HarnessError::Skill(
            "expected Skill digest must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

fn is_json_media_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
        time::Duration,
    };

    use ed25519_dalek::{Signer, SigningKey};
    use semver::Version;

    use super::{
        HttpSkillAuthorization, HttpSkillRequest, HttpSkillResponse, HttpSkillTransport,
        HttpsSkillSource, HttpsSkillSourceConfig,
    };
    use crate::{
        CapabilityOrigin, HarnessError, HarnessFuture, SKILL_API_VERSION, SecretValue,
        SignedSkillPackage, SkillId, SkillManifest, SkillPackage, SkillRegistry, SkillSignature,
        SkillTrustStore,
    };

    struct StubTransport {
        status: u16,
        content_type: Option<String>,
        body: Vec<u8>,
        requests: Mutex<Vec<HttpSkillRequest>>,
        fail_with: Option<String>,
    }

    impl HttpSkillTransport for StubTransport {
        fn fetch<'a>(&'a self, request: HttpSkillRequest) -> HarnessFuture<'a, HttpSkillResponse> {
            self.requests.lock().expect("requests").push(request);
            let status = self.status;
            let content_type = self.content_type.clone();
            let body = self.body.clone();
            let failure = self.fail_with.clone();
            Box::pin(async move {
                if let Some(failure) = failure {
                    return Err(HarnessError::Skill(failure));
                }
                Ok(HttpSkillResponse {
                    status,
                    content_type,
                    body,
                })
            })
        }
    }

    fn signed_fixture() -> (SignedSkillPackage, SigningKey) {
        let package = SkillPackage::seal(
            SkillManifest {
                api_version: SKILL_API_VERSION.to_owned(),
                name: "remote".to_owned(),
                version: Version::parse("1.2.3").expect("version"),
                description: "remote fixture".to_owned(),
                estimated_tokens: 10,
                dependencies: Vec::new(),
                required_tools: BTreeSet::new(),
            },
            "remote instructions".to_owned(),
            BTreeMap::new(),
        )
        .expect("seal");
        let signing_key = SigningKey::from_bytes(&[31_u8; 32]);
        let signature =
            signing_key.sign(&package.publisher_signing_bytes().expect("signing material"));
        (
            SignedSkillPackage {
                package,
                signature: SkillSignature {
                    key_id: "publisher".to_owned(),
                    ed25519: signature.to_bytes().to_vec(),
                },
                transparency: None,
            },
            signing_key,
        )
    }

    #[test]
    fn source_config_rejects_ambient_or_unbounded_authority() {
        assert!(HttpsSkillSourceConfig::new("http://example.test/skill.json").is_err());
        assert!(HttpsSkillSourceConfig::new("https://user@example.test/skill.json").is_err());
        assert!(HttpsSkillSourceConfig::new("https://example.test/skill.json?token=x").is_err());
        let config =
            HttpsSkillSourceConfig::new("https://example.test/skill.json").expect("valid source");
        assert!(
            config
                .clone()
                .with_limits(Duration::ZERO, Duration::ZERO, 1, 1)
                .is_err()
        );
        assert!(
            config
                .with_limits(Duration::from_secs(1), Duration::from_secs(1), 1, 65)
                .is_err()
        );
        assert!(
            HttpsSkillSourceConfig::new("https://example.test/skill.json")
                .expect("base source")
                .with_exclusive_root_certificates_pem(Vec::new())
                .is_err()
        );
        assert!(
            HttpsSkillSourceConfig::new("https://example.test/skill.json")
                .expect("base source")
                .with_exclusive_root_certificates_pem(b"not a PEM certificate".to_vec())
                .is_err()
        );
    }

    #[tokio::test]
    async fn fetches_exact_pin_and_registers_only_after_trust_verification() {
        let (signed, signing_key) = signed_fixture();
        let expected = SkillId {
            name: signed.package.manifest.name.clone(),
            version: signed.package.manifest.version.clone(),
        };
        let digest = signed.package.content_sha256.clone();
        let body = serde_json::to_vec(&signed).expect("encode package");
        let transport = Arc::new(StubTransport {
            status: 200,
            content_type: Some("application/json; charset=utf-8".to_owned()),
            body,
            requests: Mutex::new(Vec::new()),
            fail_with: None,
        });
        let source = HttpsSkillSource::with_transport(
            HttpsSkillSourceConfig::new("https://example.test/skill.json").expect("source config"),
            transport.clone(),
        )
        .expect("source");
        let trust = SkillTrustStore::new();
        trust
            .trust("publisher", signing_key.verifying_key().to_bytes())
            .expect("trust publisher");
        let mut registry = SkillRegistry::new();
        source
            .fetch_and_register(
                &mut registry,
                CapabilityOrigin::External {
                    id: "remote-source".to_owned(),
                },
                &expected,
                &digest,
                &trust,
            )
            .await
            .expect("fetch and register");
        assert!(registry.get(&expected).is_some());
        let requests = transport.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].endpoint, "https://example.test/skill.json");
        assert!(requests[0].authorization.is_none());
    }

    #[tokio::test]
    async fn bearer_authorization_is_request_scoped_and_not_part_of_source_config() {
        let (signed, _) = signed_fixture();
        let expected = SkillId {
            name: signed.package.manifest.name.clone(),
            version: signed.package.manifest.version.clone(),
        };
        let digest = signed.package.content_sha256.clone();
        let transport = Arc::new(StubTransport {
            status: 200,
            content_type: Some("application/json".to_owned()),
            body: serde_json::to_vec(&signed).expect("encode package"),
            requests: Mutex::new(Vec::new()),
            fail_with: None,
        });
        let source = HttpsSkillSource::with_transport(
            HttpsSkillSourceConfig::new("https://registry.example.test/skill.json")
                .expect("source config"),
            transport.clone(),
        )
        .expect("source");
        source
            .fetch_with_bearer(
                &expected,
                &digest,
                SecretValue::new(b"short-lived-token".to_vec()).expect("credential"),
            )
            .await
            .expect("authenticated fetch");

        let requests = transport.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            requests[0].authorization,
            Some(HttpSkillAuthorization::Bearer(_))
        ));
        assert!(!format!("{:?}", source.config).contains("short-lived-token"));
    }

    #[tokio::test]
    async fn rejects_mismatch_and_sanitizes_transport_errors() {
        let (signed, _) = signed_fixture();
        let expected = SkillId {
            name: signed.package.manifest.name.clone(),
            version: signed.package.manifest.version.clone(),
        };
        let body = serde_json::to_vec(&signed).expect("encode package");
        let source = HttpsSkillSource::with_transport(
            HttpsSkillSourceConfig::new("https://example.test/skill.json").expect("source config"),
            Arc::new(StubTransport {
                status: 200,
                content_type: Some("application/json".to_owned()),
                body,
                requests: Mutex::new(Vec::new()),
                fail_with: None,
            }),
        )
        .expect("source");
        let wrong_digest = "0".repeat(64);
        let error = source
            .fetch(&expected, &wrong_digest)
            .await
            .expect_err("pin mismatch");
        assert!(error.to_string().contains("content pin"));

        let source = HttpsSkillSource::with_transport(
            HttpsSkillSourceConfig::new("https://example.test/skill.json").expect("source config"),
            Arc::new(StubTransport {
                status: 0,
                content_type: None,
                body: Vec::new(),
                requests: Mutex::new(Vec::new()),
                fail_with: Some("secret response body".to_owned()),
            }),
        )
        .expect("source");
        let error = source
            .fetch(&expected, &signed.package.content_sha256)
            .await
            .expect_err("transport failure");
        assert!(!error.to_string().contains("secret response body"));
    }
}
