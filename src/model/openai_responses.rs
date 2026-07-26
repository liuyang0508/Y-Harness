//! Optional direct adapter for OpenAI's Responses API.

use std::{fmt, sync::Arc, time::Duration};

use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;

use super::authorization_header;
use crate::{
    HarnessError, HarnessFuture, ItemKind, LanguageModel, ModelContinuation, ModelOutput,
    ModelRequest, ModelResponse, ModelStream, ModelUsage, SecretProvider, SecretReference,
    SecretRequest, kernel::validate_model_id,
};

const OPENAI_RESPONSES_ENDPOINT: &str = "https://api.openai.com/v1/responses";
const OPENAI_REASONING_CONTINUATION_FORMAT: &str = "openai.responses.reasoning.v1";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_RESPONSE_BYTES: usize = 4_194_304;
const DEFAULT_MAX_CONCURRENCY: usize = 16;
const MAX_RESPONSE_BYTES: usize = 16_777_216;
const MAX_CONCURRENCY: usize = 256;
const MAX_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_MODEL_NAME_BYTES: usize = 256;
const MAX_REQUEST_BYTES: usize = 16_777_216;
const MAX_STREAM_EVENTS: usize = 4_096;

/// Validated direct OpenAI Responses API configuration.
#[derive(Clone)]
pub struct OpenAiResponsesModelConfig {
    model: String,
    api_key: SecretReference,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_response_bytes: usize,
    max_concurrency: usize,
}

impl OpenAiResponsesModelConfig {
    /// Creates a configuration for one explicit OpenAI model identity.
    pub fn new(model: impl Into<String>, api_key: SecretReference) -> Result<Self, HarnessError> {
        let config = Self {
            model: model.into(),
            api_key,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
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

    /// Returns the configured vendor model string.
    #[must_use]
    pub fn model(&self) -> &str {
        &self.model
    }

    fn validate(&self) -> Result<(), HarnessError> {
        if self.model.trim().is_empty()
            || self.model.len() > MAX_MODEL_NAME_BYTES
            || self.model.chars().any(char::is_control)
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "OpenAI model must be 1-{MAX_MODEL_NAME_BYTES} non-control bytes"
            )));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > MAX_TIMEOUT
            || self.connect_timeout < Duration::from_millis(1)
            || self.connect_timeout > self.request_timeout
        {
            return Err(HarnessError::InvalidConfiguration(format!(
                "OpenAI timeouts must be at least 1 millisecond, connect must not exceed request, and request must not exceed {} seconds",
                MAX_TIMEOUT.as_secs()
            )));
        }
        if !(1..=MAX_RESPONSE_BYTES).contains(&self.max_response_bytes) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "OpenAI response limit must be 1-{MAX_RESPONSE_BYTES} bytes"
            )));
        }
        if !(1..=MAX_CONCURRENCY).contains(&self.max_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "OpenAI concurrency must be 1-{MAX_CONCURRENCY}"
            )));
        }
        Ok(())
    }
}

impl fmt::Debug for OpenAiResponsesModelConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiResponsesModelConfig")
            .field("model", &self.model)
            .field("api_key", &self.api_key)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_concurrency", &self.max_concurrency)
            .finish()
    }
}

/// Direct, pooled OpenAI Responses API model capability.
///
/// The adapter disables vendor-side parallel tool calls so the Harness remains
/// the sole owner of Tool scheduling, Policy, State, and retry semantics.
pub struct OpenAiResponsesModel {
    id: String,
    config: OpenAiResponsesModelConfig,
    secrets: Arc<dyn SecretProvider>,
    client: reqwest::Client,
    concurrency: Arc<Semaphore>,
}

impl OpenAiResponsesModel {
    /// Builds one adapter over the official HTTPS endpoint.
    pub fn new(
        id: impl Into<String>,
        config: OpenAiResponsesModelConfig,
        secrets: Arc<dyn SecretProvider>,
    ) -> Result<Self, HarnessError> {
        let id = id.into();
        validate_model_id(&id)?;
        config.validate()?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .referer(false)
            .no_proxy()
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout)
            .user_agent(concat!("y-harness/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| {
                HarnessError::InvalidConfiguration(
                    "failed to build OpenAI HTTPS transport".to_owned(),
                )
            })?;
        let max_concurrency = config.max_concurrency;
        Ok(Self {
            id,
            config,
            secrets,
            client,
            concurrency: Arc::new(Semaphore::new(max_concurrency)),
        })
    }

    async fn request(
        &self,
        request: ModelRequest,
        stream: Option<ModelStream>,
    ) -> Result<ModelResponse, HarnessError> {
        crate::runtime::validate_model_request(&request)?;
        let body = build_request_body(&self.config.model, &request, stream.is_some())?;
        let credential = self
            .secrets
            .resolve(SecretRequest {
                reference: self.config.api_key.clone(),
                consumer: self.id.clone(),
                thread_id: request.thread_id,
                turn_id: request.turn_id,
            })
            .await
            .map_err(|_| HarnessError::Model("OpenAI credential resolution failed".to_owned()))?;
        let authorization = authorization_header(&credential)?;

        tokio::time::timeout(self.config.request_timeout, async {
            let _permit =
                self.concurrency.acquire().await.map_err(|_| {
                    HarnessError::Model("OpenAI model transport is closed".to_owned())
                })?;
            let response = self
                .client
                .post(OPENAI_RESPONSES_ENDPOINT)
                .header(AUTHORIZATION, authorization)
                .header(
                    ACCEPT,
                    if stream.is_some() {
                        "text/event-stream"
                    } else {
                        "application/json"
                    },
                )
                .header(CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .map_err(map_transport_error)?;
            match stream {
                Some(stream) => {
                    decode_streaming_http_response(response, self.config.max_response_bytes, stream)
                        .await
                }
                None => decode_http_response(response, self.config.max_response_bytes).await,
            }
        })
        .await
        .map_err(|_| HarnessError::Model("OpenAI model operation timed out".to_owned()))?
    }
}

impl LanguageModel for OpenAiResponsesModel {
    fn id(&self) -> &str {
        &self.id
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move {
            self.request(request, None)
                .await
                .map(|response| response.output)
        })
    }

    fn complete_with_metadata<'a>(
        &'a self,
        request: ModelRequest,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move { self.request(request, None).await })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> HarnessFuture<'a, ModelResponse> {
        if stream.is_enabled() {
            Box::pin(async move { self.request(request, Some(stream)).await })
        } else {
            self.complete_with_metadata(request)
        }
    }
}

fn build_request_body(
    model: &str,
    request: &ModelRequest,
    streaming: bool,
) -> Result<Vec<u8>, HarnessError> {
    let mut input = Vec::new();
    for item in &request.items {
        match &item.kind {
            ItemKind::UserMessage { content } => {
                input.push(json!({"role": "user", "content": content}));
            }
            ItemKind::AssistantMessage { content, .. } => {
                input.push(json!({"role": "assistant", "content": content}));
            }
            ItemKind::ProviderContinuation { continuation, .. } => {
                append_reasoning_continuation(&mut input, continuation)?;
            }
            ItemKind::ToolCall {
                call_id,
                name,
                input: arguments,
                ..
            } => {
                input.push(json!({
                    "type": "function_call",
                    "call_id": call_id,
                    "name": name,
                    "arguments": serde_json::to_string(arguments).map_err(|_| {
                        HarnessError::Model("cannot encode OpenAI function arguments".to_owned())
                    })?
                }));
            }
            ItemKind::ToolResult {
                call_id,
                output,
                is_error,
            } => {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": serde_json::to_string(&json!({
                        "is_error": is_error,
                        "output": output
                    }))
                    .map_err(|_| {
                        HarnessError::Model("cannot encode OpenAI function output".to_owned())
                    })?
                }));
            }
            ItemKind::VerificationResult { verifier, outcome } => {
                input.push(json!({
                    "role": "developer",
                    "content": format!(
                        "Y-Harness verifier {verifier} returned: {}",
                        serde_json::to_string(outcome).map_err(|_| {
                            HarnessError::Model(
                                "cannot encode OpenAI verification feedback".to_owned()
                            )
                        })?
                    )
                }));
            }
            _ => {}
        }
    }

    let tools = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "strict": false
            })
        })
        .collect::<Vec<_>>();
    let mut root = Map::from_iter([
        ("model".to_owned(), Value::String(model.to_owned())),
        ("input".to_owned(), Value::Array(input)),
        ("include".to_owned(), json!(["reasoning.encrypted_content"])),
        ("store".to_owned(), Value::Bool(false)),
        ("parallel_tool_calls".to_owned(), Value::Bool(false)),
        ("stream".to_owned(), Value::Bool(streaming)),
        ("tools".to_owned(), Value::Array(tools)),
    ]);
    if !request.context.is_empty() {
        root.insert(
            "instructions".to_owned(),
            Value::String(
                request
                    .context
                    .iter()
                    .map(|block| block.text.as_str())
                    .collect::<Vec<_>>()
                    .join("\n\n---\n\n"),
            ),
        );
    }
    crate::json::to_bounded_json_vec(&Value::Object(root), MAX_REQUEST_BYTES).map_err(|error| {
        match error {
            crate::json::BoundedJsonError::LimitExceeded => {
                HarnessError::Model(format!("OpenAI request exceeds {MAX_REQUEST_BYTES} bytes"))
            }
            crate::json::BoundedJsonError::CannotEncode => {
                HarnessError::Model("cannot encode OpenAI request".to_owned())
            }
        }
    })
}

async fn decode_streaming_http_response(
    mut response: reqwest::Response,
    maximum: usize,
    stream: ModelStream,
) -> Result<ModelResponse, HarnessError> {
    validate_response_head(&response, maximum, "text/event-stream")?;
    let provider_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut decoder = OpenAiSseDecoder::new(stream, maximum, provider_request_id);
    while let Some(chunk) = response.chunk().await.map_err(map_transport_error)? {
        decoder.push(&chunk)?;
    }
    decoder.finish()
}

async fn decode_http_response(
    mut response: reqwest::Response,
    maximum: usize,
) -> Result<ModelResponse, HarnessError> {
    validate_response_head(&response, maximum, "application/json")?;
    let provider_request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut body = Vec::with_capacity(maximum.min(8_192));
    while let Some(chunk) = response.chunk().await.map_err(map_transport_error)? {
        let next = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| HarnessError::Model("OpenAI response size overflow".to_owned()))?;
        if next > maximum {
            return Err(HarnessError::Model(
                "OpenAI response exceeded its configured limit".to_owned(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    decode_response(&body, provider_request_id)
}

fn validate_response_head(
    response: &reqwest::Response,
    maximum: usize,
    expected_content_type: &str,
) -> Result<(), HarnessError> {
    if let Some(length) = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > maximum as u64
    {
        return Err(HarnessError::Model(
            "OpenAI response declared an oversized body".to_owned(),
        ));
    }
    if !response.status().is_success() {
        return Err(HarnessError::Model(format!(
            "OpenAI returned HTTP status {}",
            response.status().as_u16()
        )));
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
        return Err(HarnessError::Model(
            "OpenAI returned an unexpected content type".to_owned(),
        ));
    }
    Ok(())
}

fn decode_response(
    body: &[u8],
    provider_request_id: Option<String>,
) -> Result<ModelResponse, HarnessError> {
    let root: Value = serde_json::from_slice(body)
        .map_err(|_| HarnessError::Model("OpenAI returned invalid JSON".to_owned()))?;
    decode_response_value(root, provider_request_id)
}

fn decode_response_value(
    root: Value,
    provider_request_id: Option<String>,
) -> Result<ModelResponse, HarnessError> {
    crate::json::validate_value_shape(&root)
        .map_err(|_| HarnessError::Model("OpenAI response JSON is too complex".to_owned()))?;
    let object = root
        .as_object()
        .ok_or_else(|| HarnessError::Model("OpenAI response must be an object".to_owned()))?;
    if object.get("status").and_then(Value::as_str) != Some("completed") {
        return Err(HarnessError::Model(
            "OpenAI response did not complete".to_owned(),
        ));
    }
    let output = object
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| HarnessError::Model("OpenAI response has no output array".to_owned()))?;
    let mut text = String::new();
    let mut function_call = None;
    let mut continuation_items = Vec::new();
    let mut has_unreplayable_reasoning = false;
    for item in output {
        let Some(item) = item.as_object() else {
            return Err(HarnessError::Model(
                "OpenAI output item must be an object".to_owned(),
            ));
        };
        match item.get("type").and_then(Value::as_str) {
            Some("message") => collect_message_text(item, &mut text)?,
            Some("function_call") => {
                if function_call.is_some() {
                    return Err(HarnessError::Model(
                        "OpenAI returned multiple function calls while parallel calls are disabled"
                            .to_owned(),
                    ));
                }
                let call_id = required_string(item, "call_id", "OpenAI function call")?;
                let name = required_string(item, "name", "OpenAI function call")?;
                let arguments = required_string(item, "arguments", "OpenAI function call")?;
                let input = serde_json::from_str(&arguments).map_err(|_| {
                    HarnessError::Model("OpenAI function arguments are not valid JSON".to_owned())
                })?;
                function_call = Some(ModelOutput::ToolCall {
                    call_id,
                    name,
                    input,
                });
            }
            Some("reasoning") => {
                if item
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some_and(|content| !content.is_empty())
                {
                    continuation_items.push(Value::Object(item.clone()));
                } else {
                    has_unreplayable_reasoning = true;
                }
            }
            _ => {}
        }
    }
    if function_call.is_some() && has_unreplayable_reasoning {
        return Err(HarnessError::Model(
            "OpenAI function call contains reasoning that cannot be replayed with store disabled"
                .to_owned(),
        ));
    }
    let output = match (function_call, text.is_empty()) {
        (Some(output), true) => output,
        (Some(_), false) => {
            return Err(HarnessError::Model(
                "OpenAI returned both assistant text and a function call".to_owned(),
            ));
        }
        (None, false) => ModelOutput::Message { content: text },
        (None, true) => {
            return Err(HarnessError::Model(
                "OpenAI response has no assistant text or function call".to_owned(),
            ));
        }
    };
    let continuation = if continuation_items.is_empty() {
        None
    } else {
        Some(ModelContinuation::new(
            OPENAI_REASONING_CONTINUATION_FORMAT,
            continuation_items,
        )?)
    };
    let response = ModelResponse {
        output,
        usage: decode_usage(object.get("usage")),
        provider_model: Some(required_string(object, "model", "OpenAI response")?),
        provider_request_id,
        continuation,
    };
    crate::runtime::validate_model_response(&response)?;
    Ok(response)
}

fn append_reasoning_continuation(
    input: &mut Vec<Value>,
    continuation: &ModelContinuation,
) -> Result<(), HarnessError> {
    if continuation.format() != OPENAI_REASONING_CONTINUATION_FORMAT {
        return Err(HarnessError::Model(format!(
            "OpenAI adapter cannot replay continuation format {}",
            continuation.format()
        )));
    }
    for item in continuation.items() {
        let object = item.as_object().ok_or_else(|| {
            HarnessError::Model("OpenAI reasoning continuation item must be an object".to_owned())
        })?;
        if object.get("type").and_then(Value::as_str) != Some("reasoning")
            || object
                .get("encrypted_content")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            return Err(HarnessError::Model(
                "OpenAI reasoning continuation item is not replayable".to_owned(),
            ));
        }
        input.push(item.clone());
    }
    Ok(())
}

struct OpenAiSseDecoder {
    stream: ModelStream,
    maximum: usize,
    provider_request_id: Option<String>,
    pending: Vec<u8>,
    scanned: usize,
    event_data: Vec<u8>,
    total_bytes: usize,
    event_count: usize,
    final_response: Option<ModelResponse>,
}

impl OpenAiSseDecoder {
    fn new(stream: ModelStream, maximum: usize, provider_request_id: Option<String>) -> Self {
        Self {
            stream,
            maximum,
            provider_request_id,
            pending: Vec::new(),
            scanned: 0,
            event_data: Vec::new(),
            total_bytes: 0,
            event_count: 0,
            final_response: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<(), HarnessError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(chunk.len())
            .ok_or_else(|| HarnessError::Model("OpenAI stream size overflow".to_owned()))?;
        if self.total_bytes > self.maximum {
            return Err(HarnessError::Model(
                "OpenAI stream exceeded its configured limit".to_owned(),
            ));
        }
        self.pending.extend_from_slice(chunk);
        while let Some(relative) = self.pending[self.scanned..]
            .iter()
            .position(|byte| *byte == b'\n')
        {
            let newline = self.scanned + relative;
            let mut line = self.pending.drain(..=newline).collect::<Vec<_>>();
            self.scanned = 0;
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.consume_line(&line)?;
        }
        self.scanned = self.pending.len();
        Ok(())
    }

    fn consume_line(&mut self, line: &[u8]) -> Result<(), HarnessError> {
        if line.is_empty() {
            return self.finish_event();
        }
        let Some(data) = line.strip_prefix(b"data:") else {
            return Ok(());
        };
        let data = data.strip_prefix(b" ").unwrap_or(data);
        if !self.event_data.is_empty() {
            self.event_data.push(b'\n');
        }
        self.event_data.extend_from_slice(data);
        if self.event_data.len() > self.maximum {
            return Err(HarnessError::Model(
                "OpenAI stream event exceeded its configured limit".to_owned(),
            ));
        }
        Ok(())
    }

    fn finish_event(&mut self) -> Result<(), HarnessError> {
        if self.event_data.is_empty() {
            return Ok(());
        }
        self.event_count = self
            .event_count
            .checked_add(1)
            .ok_or_else(|| HarnessError::Model("OpenAI stream event count overflow".to_owned()))?;
        if self.event_count > MAX_STREAM_EVENTS {
            return Err(HarnessError::Model(format!(
                "OpenAI stream exceeds {MAX_STREAM_EVENTS} events"
            )));
        }
        let event: Value = serde_json::from_slice(&self.event_data)
            .map_err(|_| HarnessError::Model("OpenAI stream returned invalid JSON".to_owned()))?;
        self.event_data.clear();
        crate::json::validate_value_shape(&event)
            .map_err(|_| HarnessError::Model("OpenAI stream JSON is too complex".to_owned()))?;
        let event = event.as_object().ok_or_else(|| {
            HarnessError::Model("OpenAI stream event must be an object".to_owned())
        })?;
        if self.final_response.is_some() {
            return Err(HarnessError::Model(
                "OpenAI stream continued after its completed response".to_owned(),
            ));
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                let delta = required_string(event, "delta", "OpenAI stream text delta")?;
                emit_bounded_delta(&self.stream, &delta);
            }
            Some("response.completed") => {
                let response = event.get("response").cloned().ok_or_else(|| {
                    HarnessError::Model("OpenAI completed event has no response".to_owned())
                })?;
                self.final_response = Some(decode_response_value(
                    response,
                    self.provider_request_id.clone(),
                )?);
            }
            Some("response.failed" | "response.incomplete") => {
                return Err(HarnessError::Model(
                    "OpenAI streaming response did not complete".to_owned(),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ModelResponse, HarnessError> {
        if !self.pending.is_empty() {
            let line = std::mem::take(&mut self.pending);
            self.consume_line(line.strip_suffix(b"\r").unwrap_or(&line))?;
        }
        self.finish_event()?;
        self.final_response.ok_or_else(|| {
            HarnessError::Model("OpenAI stream ended without a completed response".to_owned())
        })
    }
}

fn emit_bounded_delta(stream: &ModelStream, delta: &str) {
    let mut start = 0;
    for (index, character) in delta.char_indices() {
        if index
            .saturating_add(character.len_utf8())
            .saturating_sub(start)
            > 4_096
        {
            let _ = stream.emit_text_delta(&delta[start..index]);
            start = index;
        }
    }
    if start < delta.len() {
        let _ = stream.emit_text_delta(&delta[start..]);
    }
}

fn collect_message_text(
    item: &Map<String, Value>,
    target: &mut String,
) -> Result<(), HarnessError> {
    let content = item
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| HarnessError::Model("OpenAI message has no content array".to_owned()))?;
    for part in content {
        let Some(part) = part.as_object() else {
            return Err(HarnessError::Model(
                "OpenAI message content must be an object".to_owned(),
            ));
        };
        if matches!(
            part.get("type").and_then(Value::as_str),
            Some("output_text" | "refusal")
        ) {
            let fragment = required_string(part, "text", "OpenAI message content")
                .or_else(|_| required_string(part, "refusal", "OpenAI message content"))?;
            target.push_str(&fragment);
        }
    }
    Ok(())
}

fn decode_usage(value: Option<&Value>) -> Option<ModelUsage> {
    let usage = value?.as_object()?;
    Some(ModelUsage {
        input_tokens: usage.get("input_tokens").and_then(Value::as_u64)?,
        output_tokens: usage.get("output_tokens").and_then(Value::as_u64)?,
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .and_then(Value::as_object)
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cost_usd_ticks: None,
    })
}

fn required_string(
    object: &Map<String, Value>,
    field: &str,
    kind: &str,
) -> Result<String, HarnessError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| HarnessError::Model(format!("{kind} has no {field}")))
}

fn map_transport_error(error: reqwest::Error) -> HarnessError {
    if error.is_timeout() {
        HarnessError::Model("OpenAI transport timed out".to_owned())
    } else if error.is_connect() {
        HarnessError::Model("OpenAI connection failed".to_owned())
    } else {
        HarnessError::Model("OpenAI transport failed".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};

    use super::{OpenAiSseDecoder, build_request_body, decode_response};
    use crate::{
        CapabilityOrigin, ContextBlock, ContextSource, Item, ItemKind, MemoryView,
        ModelContinuation, ModelEventSink, ModelOutput, ModelRequest, ModelStream,
        ModelStreamEvent, ThreadId, ToolDescriptor, TurnId,
    };

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<ModelStreamEvent>>);

    impl ModelEventSink for RecordingSink {
        fn emit(&self, event: &ModelStreamEvent) -> Result<(), String> {
            self.0
                .lock()
                .map_err(|_| "recording sink poisoned".to_owned())?
                .push(event.clone());
            Ok(())
        }
    }

    fn request() -> ModelRequest {
        ModelRequest {
            thread_id: ThreadId::from_static("thread-openai"),
            turn_id: TurnId::from_static("turn-openai"),
            items: vec![
                Item::new(ItemKind::UserMessage {
                    content: "weather?".to_owned(),
                }),
                Item::new(ItemKind::ProviderContinuation {
                    model_id: "openai/test".to_owned(),
                    model_origin: CapabilityOrigin::BuiltIn,
                    continuation: ModelContinuation::new(
                        super::OPENAI_REASONING_CONTINUATION_FORMAT,
                        vec![json!({
                            "type": "reasoning",
                            "id": "reasoning-1",
                            "encrypted_content": "opaque"
                        })],
                    )
                    .expect("continuation"),
                }),
                Item::new(ItemKind::ToolCall {
                    model_id: Some("openai/test".to_owned()),
                    model_origin: Some(CapabilityOrigin::BuiltIn),
                    call_id: "call-1".to_owned(),
                    name: "weather".to_owned(),
                    input: json!({"city": "Shanghai"}),
                }),
                Item::new(ItemKind::ToolResult {
                    call_id: "call-1".to_owned(),
                    output: json!({"temperature": 31}),
                    is_error: false,
                }),
            ],
            context: vec![ContextBlock {
                source: ContextSource::Memory {
                    provider: "fixture".to_owned(),
                    reference: "memory-1".to_owned(),
                    selected_view: MemoryView::Detail,
                    detail_uri: None,
                },
                text: "Be concise.".to_owned(),
                estimated_tokens: 3,
            }],
            tools: vec![ToolDescriptor {
                name: "weather".to_owned(),
                description: "Read weather".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {"city": {"type": "string"}}
                }),
            }],
        }
    }

    #[test]
    fn request_keeps_harness_in_charge_of_tools_and_storage() {
        let encoded = build_request_body("model-explicit", &request(), false).expect("request");
        let root: Value = serde_json::from_slice(&encoded).expect("json");
        assert_eq!(root["model"], "model-explicit");
        assert_eq!(root["include"], json!(["reasoning.encrypted_content"]));
        assert_eq!(root["store"], false);
        assert_eq!(root["parallel_tool_calls"], false);
        assert_eq!(root["stream"], false);
        assert_eq!(root["instructions"], "Be concise.");
        assert_eq!(root["tools"][0]["name"], "weather");
        assert_eq!(root["input"][1]["type"], "reasoning");
        assert_eq!(root["input"][1]["encrypted_content"], "opaque");
        assert_eq!(root["input"][2]["type"], "function_call");
        assert_eq!(root["input"][3]["type"], "function_call_output");
    }

    #[test]
    fn response_decodes_text_usage_and_request_identity() {
        let response = decode_response(
            serde_json::to_vec(&json!({
                "status": "completed",
                "model": "gpt-test-2026-01-01",
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "hello"}]
                }],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 4,
                    "input_tokens_details": {"cached_tokens": 3},
                    "output_tokens_details": {"reasoning_tokens": 2}
                }
            }))
            .expect("encode")
            .as_slice(),
            Some("request-1".to_owned()),
        )
        .expect("response");
        assert_eq!(
            response.output,
            ModelOutput::Message {
                content: "hello".to_owned()
            }
        );
        let usage = response.usage.expect("usage");
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.cached_input_tokens, 3);
        assert_eq!(usage.reasoning_tokens, 2);
        assert_eq!(
            response.provider_model.as_deref(),
            Some("gpt-test-2026-01-01")
        );
        assert_eq!(response.provider_request_id.as_deref(), Some("request-1"));
        assert!(response.continuation.is_none());

        let error = decode_response(
            &serde_json::to_vec(&json!({
                "status": "completed",
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": "hello"}]
                }]
            }))
            .expect("encode"),
            None,
        )
        .expect_err("completed response must report its model");
        assert!(error.to_string().contains("no model"));
    }

    #[test]
    fn response_decodes_one_function_call_and_rejects_multiple() {
        let call = json!({
            "type": "function_call",
            "call_id": "call-1",
            "name": "weather",
            "arguments": "{\"city\":\"Shanghai\"}"
        });
        let response = decode_response(
            &serde_json::to_vec(&json!({
                "status": "completed",
                "model": "gpt-test-2026-01-01",
                "output": [call.clone()]
            }))
            .expect("encode"),
            None,
        )
        .expect("response");
        assert_eq!(
            response.output,
            ModelOutput::ToolCall {
                call_id: "call-1".to_owned(),
                name: "weather".to_owned(),
                input: json!({"city": "Shanghai"}),
            }
        );
        let error = decode_response(
            &serde_json::to_vec(&json!({
                "status": "completed",
                "model": "gpt-test-2026-01-01",
                "output": [call.clone(), call]
            }))
            .expect("encode"),
            None,
        )
        .expect_err("reject parallel calls");
        assert!(error.to_string().contains("multiple function calls"));

        let response = decode_response(
            &serde_json::to_vec(&json!({
                "status": "completed",
                "model": "gpt-test-2026-01-01",
                "output": [
                    {
                        "type": "reasoning",
                        "id": "reasoning-2",
                        "encrypted_content": "opaque"
                    },
                    {
                        "type": "function_call",
                        "call_id": "call-2",
                        "name": "weather",
                        "arguments": "{}"
                    }
                ]
            }))
            .expect("encode"),
            None,
        )
        .expect("preserve reasoning continuation");
        let continuation = response.continuation.expect("continuation");
        assert_eq!(
            continuation.format(),
            super::OPENAI_REASONING_CONTINUATION_FORMAT
        );
        assert_eq!(continuation.items()[0]["encrypted_content"], "opaque");

        let error = decode_response(
            &serde_json::to_vec(&json!({
                "status": "completed",
                "model": "gpt-test-2026-01-01",
                "output": [
                    {"type": "reasoning", "encrypted_content": null},
                    {
                        "type": "function_call",
                        "call_id": "call-3",
                        "name": "weather",
                        "arguments": "{}"
                    }
                ]
            }))
            .expect("encode"),
            None,
        )
        .expect_err("reject unreplayable reasoning");
        assert!(error.to_string().contains("cannot be replayed"));
    }

    #[test]
    fn streaming_decoder_emits_delta_and_requires_completed_response() {
        let sink = Arc::new(RecordingSink::default());
        let stream = ModelStream::new(sink.clone()).for_step(1);
        let mut decoder = OpenAiSseDecoder::new(stream, 16_384, Some("request-2".to_owned()));
        decoder
            .push(
                b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hel",
            )
            .expect("first chunk");
        decoder
            .push(
                b"lo\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":\"gpt-test-2026-01-01\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}]}}\n\n",
            )
            .expect("second chunk");
        let response = decoder.finish().expect("final response");
        assert_eq!(
            response.output,
            ModelOutput::Message {
                content: "hello".to_owned()
            }
        );
        assert_eq!(
            response.provider_model.as_deref(),
            Some("gpt-test-2026-01-01")
        );
        assert_eq!(response.provider_request_id.as_deref(), Some("request-2"));
        assert_eq!(
            sink.0.lock().expect("events").as_slice(),
            &[ModelStreamEvent::TextDelta {
                model_step: 1,
                delta: "hello".to_owned(),
            }]
        );
    }
}
