//! Agent Memory Hub's MCP mapping onto the provider-neutral Memory contract.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use serde_json::{Map, Value, json};

use crate::{
    HarnessError, HarnessFuture, MEMORY_API_VERSION, McpClient, MemoryBriefRequest,
    MemoryBriefResponse, MemoryContextPack, MemoryHealth, MemoryHealthStatus, MemoryOperation,
    MemoryProvenance, MemoryProvider, MemoryProviderDescriptor, MemoryReadRequest,
    MemoryReadResponse, MemoryReference, MemorySearchRequest, MemorySearchResponse, MemoryView,
    MemoryWriteRequest, MemoryWriteResponse,
};

const PROVIDER_NAME: &str = "agent-memory-hub";
const REQUIRED_TOOLS: &[&str] = &[
    "search_memory",
    "read_memory",
    "write_memory",
    "brief_memory",
    "brain_stats",
];

/// First-party adapter from Agent Memory Hub MCP tools to Memory Provider v1.
pub struct AgentMemoryHubProvider {
    client: Arc<dyn McpClient>,
}

impl AgentMemoryHubProvider {
    #[must_use]
    /// Creates an adapter over an initialized-on-demand MCP client.
    pub fn new(client: Arc<dyn McpClient>) -> Self {
        Self { client }
    }
}

impl MemoryProvider for AgentMemoryHubProvider {
    fn descriptor(&self) -> MemoryProviderDescriptor {
        MemoryProviderDescriptor {
            name: PROVIDER_NAME.to_owned(),
            description: "Governed long-term memory through Agent Memory Hub MCP".to_owned(),
            api_version: MEMORY_API_VERSION,
            operations: BTreeSet::from([
                MemoryOperation::Search,
                MemoryOperation::Read,
                MemoryOperation::Write,
                MemoryOperation::Brief,
                MemoryOperation::Health,
            ]),
        }
    }

    fn search<'a>(
        &'a self,
        request: MemorySearchRequest,
    ) -> HarnessFuture<'a, MemorySearchResponse> {
        Box::pin(async move {
            let mut arguments = Map::from_iter([
                ("query".to_owned(), Value::String(request.query)),
                ("top_k".to_owned(), json!(request.top_k)),
                ("verbosity".to_owned(), Value::String("auto".to_owned())),
            ]);
            insert_scope(&mut arguments, request.scope);
            let value = self
                .client
                .call_tool("search_memory", Value::Object(arguments))
                .await?;
            let rows = result_array(value, "search_memory")?;
            let packs = rows
                .into_iter()
                .map(parse_search_pack)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(MemorySearchResponse {
                packs,
                warnings: Vec::new(),
            })
        })
    }

    fn read<'a>(&'a self, request: MemoryReadRequest) -> HarnessFuture<'a, MemoryReadResponse> {
        Box::pin(async move {
            let mut arguments = Map::from_iter([
                (
                    "item_id".to_owned(),
                    Value::String(request.reference.as_str().to_owned()),
                ),
                (
                    "view".to_owned(),
                    Value::String(memory_view_name(&request.view).to_owned()),
                ),
            ]);
            if let Some(head) = request.head_chars {
                arguments.insert("head".to_owned(), json!(head));
            }
            let value = self
                .client
                .call_tool("read_memory", Value::Object(arguments))
                .await?;
            let object = result_object(value, "read_memory")?;
            let text =
                first_string(&object, &["body", "overview", "locator"]).ok_or_else(|| {
                    HarnessError::Memory("Agent Memory Hub read returned no text view".to_owned())
                })?;
            let provenance = object
                .get("frontmatter")
                .and_then(Value::as_object)
                .map(provenance_from_frontmatter)
                .unwrap_or_default();
            Ok(MemoryReadResponse {
                reference: request.reference,
                text,
                truncated: object
                    .get("body_truncated")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                provenance,
            })
        })
    }

    fn write<'a>(&'a self, request: MemoryWriteRequest) -> HarnessFuture<'a, MemoryWriteResponse> {
        Box::pin(async move {
            if request.idempotency_key.trim().is_empty() {
                return Err(HarnessError::Memory(
                    "memory write idempotency key must not be empty".to_owned(),
                ));
            }
            let mut arguments = Map::from_iter([
                ("type".to_owned(), Value::String(request.kind)),
                ("title".to_owned(), Value::String(request.title)),
                ("summary".to_owned(), Value::String(request.summary)),
                ("body".to_owned(), Value::String(request.body)),
            ]);
            insert_scope(&mut arguments, request.scope);
            insert_provenance_refs(&mut arguments, request.provenance);
            let value = self
                .client
                .call_tool("write_memory", Value::Object(arguments))
                .await?;
            let object = result_object(value, "write_memory")?;
            if object.get("status").and_then(Value::as_str) == Some("blocked") {
                return Err(HarnessError::Memory(
                    object
                        .get("reason")
                        .and_then(Value::as_str)
                        .unwrap_or("Agent Memory Hub blocked the write")
                        .to_owned(),
                ));
            }
            let reference = object
                .get("id")
                .and_then(Value::as_str)
                .map(MemoryReference::new);
            if reference.is_none() {
                return Err(HarnessError::Memory(
                    "Agent Memory Hub write returned no item id".to_owned(),
                ));
            }
            let mut warnings = string_array(object.get("warnings"));
            warnings.push(
                "Agent Memory Hub MCP write does not currently settle the caller idempotency key"
                    .to_owned(),
            );
            Ok(MemoryWriteResponse {
                reference,
                warnings,
                degraded: string_array(object.get("degraded")),
            })
        })
    }

    fn brief<'a>(&'a self, request: MemoryBriefRequest) -> HarnessFuture<'a, MemoryBriefResponse> {
        Box::pin(async move {
            if request.scope.tenant_id.is_some() || !request.scope.tags.is_empty() {
                return Err(HarnessError::Memory(
                    "Agent Memory Hub brief MCP tool does not support tenant or tag scope"
                        .to_owned(),
                ));
            }
            let mut arguments =
                Map::from_iter([("budget_tokens".to_owned(), json!(request.budget_tokens))]);
            if let Some(project) = request.scope.project {
                arguments.insert("project".to_owned(), Value::String(project));
            }
            if let Some(query) = request.query {
                arguments.insert("query".to_owned(), Value::String(query));
            }
            let value = self
                .client
                .call_tool("brief_memory", Value::Object(arguments))
                .await?;
            parse_brief(value, request.budget_tokens)
        })
    }

    fn health<'a>(&'a self) -> HarnessFuture<'a, MemoryHealth> {
        Box::pin(async move {
            let tools = self.client.list_tools().await?;
            let available = tools
                .into_iter()
                .map(|tool| tool.name)
                .collect::<HashSet<_>>();
            let missing = REQUIRED_TOOLS
                .iter()
                .copied()
                .filter(|tool| !available.contains(*tool))
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                return Ok(MemoryHealth {
                    status: MemoryHealthStatus::Unavailable,
                    message: Some(format!("missing MCP tools: {}", missing.join(", "))),
                });
            }
            self.client.call_tool("brain_stats", json!({})).await?;
            Ok(MemoryHealth {
                status: MemoryHealthStatus::Healthy,
                message: None,
            })
        })
    }
}

fn insert_scope(arguments: &mut Map<String, Value>, scope: crate::MemoryScope) {
    if let Some(project) = scope.project {
        arguments.insert("project".to_owned(), Value::String(project));
    }
    if let Some(tenant_id) = scope.tenant_id {
        arguments.insert("tenant_id".to_owned(), Value::String(tenant_id));
    }
    if !scope.tags.is_empty() {
        arguments.insert("tags".to_owned(), json!(scope.tags));
    }
}

fn parse_search_pack(value: Value) -> Result<MemoryContextPack, HarnessError> {
    let object = value.as_object().ok_or_else(|| {
        HarnessError::Memory("Agent Memory Hub search item must be an object".to_owned())
    })?;
    let reference = required_string(object, "id", "search item")?;
    let context = object
        .get("context_pack")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            HarnessError::Memory(format!(
                "Agent Memory Hub search item {reference} has no context_pack"
            ))
        })?;
    let text = required_string(context, "text", "context pack")?;
    let packed_tokens = required_usize(context, "packed_tokens", "context pack")?;
    let selected_view = match required_string(context, "selected_view", "context pack")?.as_str() {
        "locator" => MemoryView::Locator,
        "overview" => MemoryView::Overview,
        "detail" => MemoryView::Detail,
        other => {
            return Err(HarnessError::Memory(format!(
                "Agent Memory Hub returned unknown context view {other:?}"
            )));
        }
    };
    let detail_uri = context
        .get("detail_uri")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let provenance = detail_uri
        .iter()
        .map(|uri| MemoryProvenance {
            kind: "detail_uri".to_owned(),
            reference: uri.clone(),
        })
        .collect();
    Ok(MemoryContextPack {
        reference: MemoryReference::new(reference),
        title: object
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_owned),
        text,
        selected_view,
        detail_uri,
        packed_tokens,
        provenance,
    })
}

fn parse_brief(value: Value, budget_tokens: usize) -> Result<MemoryBriefResponse, HarnessError> {
    let object = result_object(value, "brief_memory")?;
    let tiers = object
        .get("tiers")
        .and_then(Value::as_array)
        .ok_or_else(|| HarnessError::Memory("Agent Memory Hub brief has no tiers".to_owned()))?;
    let mut packs = Vec::new();
    let mut used_tokens = 0usize;
    let mut withheld = object
        .get("total_withheld")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    for tier in tiers {
        let Some(items) = tier
            .as_object()
            .and_then(|tier| tier.get("items"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for item in items {
            let item = item.as_object().ok_or_else(|| {
                HarnessError::Memory("Agent Memory Hub brief item must be an object".to_owned())
            })?;
            let reference = required_string(item, "id", "brief item")?;
            let title = required_string(item, "title", "brief item")?;
            let summary = required_string(item, "summary", "brief item")?;
            let kind = item.get("type").and_then(Value::as_str).unwrap_or("memory");
            let text = format!("[{kind}] {title} — {summary}");
            let packed_tokens = estimate_tokens(&text);
            if used_tokens.saturating_add(packed_tokens) > budget_tokens {
                withheld = withheld.saturating_add(1);
                continue;
            }
            used_tokens += packed_tokens;
            packs.push(MemoryContextPack {
                reference: MemoryReference::new(reference.clone()),
                title: Some(title),
                text,
                selected_view: MemoryView::Locator,
                detail_uri: Some(format!("memory://items/{reference}/body")),
                packed_tokens,
                provenance: Vec::new(),
            });
        }
    }
    Ok(MemoryBriefResponse { packs, withheld })
}

fn insert_provenance_refs(arguments: &mut Map<String, Value>, provenance: Vec<MemoryProvenance>) {
    let mappings = [
        ("file", "ref_files"),
        ("url", "ref_urls"),
        ("memory", "ref_mems"),
        ("commit", "ref_commits"),
        ("resource", "ref_resources"),
        ("extraction", "ref_extractions"),
    ];
    for (kind, argument) in mappings {
        let values = provenance
            .iter()
            .filter(|reference| reference.kind == kind)
            .map(|reference| reference.reference.clone())
            .collect::<Vec<_>>();
        if !values.is_empty() {
            arguments.insert(argument.to_owned(), json!(values));
        }
    }
}

fn provenance_from_frontmatter(frontmatter: &Map<String, Value>) -> Vec<MemoryProvenance> {
    let Some(refs) = frontmatter.get("refs").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mappings = [
        ("files", "file"),
        ("urls", "url"),
        ("mems", "memory"),
        ("commits", "commit"),
        ("resources", "resource"),
        ("extractions", "extraction"),
    ];
    let mut provenance = Vec::new();
    for (field, kind) in mappings {
        for reference in string_array(refs.get(field)) {
            provenance.push(MemoryProvenance {
                kind: kind.to_owned(),
                reference,
            });
        }
    }
    provenance
}

fn result_array(value: Value, tool: &str) -> Result<Vec<Value>, HarnessError> {
    match unwrap_result(value) {
        Value::Array(rows) => Ok(rows),
        Value::Object(object)
            if object.contains_key("id") || object.contains_key("context_pack") =>
        {
            Ok(vec![Value::Object(object)])
        }
        Value::Null => Ok(Vec::new()),
        value => Err(HarnessError::Memory(format!(
            "Agent Memory Hub {tool} result must be an array, got {}",
            value_shape(&value)
        ))),
    }
}

fn result_object(value: Value, tool: &str) -> Result<Map<String, Value>, HarnessError> {
    match unwrap_result(value) {
        Value::Object(object) => Ok(object),
        value => Err(HarnessError::Memory(format!(
            "Agent Memory Hub {tool} result must be an object, got {}",
            value_shape(&value)
        ))),
    }
}

fn unwrap_result(value: Value) -> Value {
    match value {
        Value::Object(mut object) if object.len() == 1 && object.contains_key("result") => {
            object.remove("result").unwrap_or(Value::Null)
        }
        value => value,
    }
}

fn value_shape(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "boolean".to_owned(),
        Value::Number(_) => "number".to_owned(),
        Value::String(_) => "string".to_owned(),
        Value::Array(values) => format!("array(len={})", values.len()),
        Value::Object(object) => {
            let fields = object
                .iter()
                .map(|(key, value)| format!("{key}:{}", value_shape(value)))
                .collect::<Vec<_>>()
                .join(",");
            format!("object{{{fields}}}")
        }
    }
}

fn required_string(
    object: &Map<String, Value>,
    field: &str,
    subject: &str,
) -> Result<String, HarnessError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| HarnessError::Memory(format!("{subject} has no string field {field}")))
}

fn required_usize(
    object: &Map<String, Value>,
    field: &str,
    subject: &str,
) -> Result<usize, HarnessError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| HarnessError::Memory(format!("{subject} has no integer field {field}")))
}

fn first_string(object: &Map<String, Value>, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        object
            .get(*field)
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn memory_view_name(view: &MemoryView) -> &'static str {
    match view {
        MemoryView::Locator => "locator",
        MemoryView::Overview => "overview",
        MemoryView::Detail => "detail",
    }
}

fn estimate_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4).max(1)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};

    use super::{AgentMemoryHubProvider, result_array};
    use crate::{
        HarnessError, HarnessFuture, McpClient, McpToolDescriptor, MemoryOperation, MemoryProvider,
        MemoryScope, MemorySearchRequest, MemoryWriteRequest,
    };

    struct FakeMcp {
        calls: Mutex<Vec<String>>,
    }

    impl McpClient for FakeMcp {
        fn list_tools<'a>(&'a self) -> HarnessFuture<'a, Vec<McpToolDescriptor>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn call_tool<'a>(&'a self, name: &'a str, _arguments: Value) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|_| HarnessError::Mcp("test lock poisoned".to_owned()))?
                    .push(name.to_owned());
                match name {
                    "search_memory" => Ok(json!({
                        "result": [{
                            "id": "mem-1",
                            "title": "Decision",
                            "context_pack": {
                                "text": "Use MCP",
                                "selected_view": "overview",
                                "detail_uri": "memory://items/mem-1/body",
                                "packed_tokens": 2
                            }
                        }]
                    })),
                    "write_memory" => Ok(json!({"id": "mem-2", "path": "/brain/item.md"})),
                    _ => Err(HarnessError::Mcp("unexpected tool".to_owned())),
                }
            })
        }
    }

    fn provider() -> AgentMemoryHubProvider {
        AgentMemoryHubProvider::new(Arc::new(FakeMcp {
            calls: Mutex::new(Vec::new()),
        }))
    }

    #[tokio::test]
    async fn maps_search_context_pack_without_flattening_it() {
        let response = provider()
            .search(MemorySearchRequest {
                query: "decision".to_owned(),
                scope: MemoryScope::default(),
                top_k: 5,
                budget_tokens: 100,
            })
            .await
            .expect("search");

        assert_eq!(response.packs.len(), 1);
        assert_eq!(response.packs[0].reference.as_str(), "mem-1");
        assert_eq!(response.packs[0].text, "Use MCP");
        assert_eq!(
            response.packs[0].detail_uri.as_deref(),
            Some("memory://items/mem-1/body")
        );
    }

    #[test]
    fn accepts_fastmcp_singleton_and_empty_list_shapes() {
        let singleton = result_array(
            json!({"result": {"id": "mem-1", "context_pack": {}}}),
            "search_memory",
        )
        .expect("singleton");
        assert_eq!(singleton.len(), 1);

        let empty = result_array(json!({"result": null}), "search_memory").expect("empty result");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn exposes_real_operation_surface_and_write_limitation() {
        let provider = provider();
        let descriptor = provider.descriptor();
        assert!(descriptor.supports(&MemoryOperation::Search));
        assert!(!descriptor.supports(&MemoryOperation::Feedback));

        let response = provider
            .write(MemoryWriteRequest {
                idempotency_key: "write-1".to_owned(),
                kind: "fact".to_owned(),
                title: "Title".to_owned(),
                summary: "Summary".to_owned(),
                body: "Body".to_owned(),
                scope: MemoryScope::default(),
                provenance: Vec::new(),
            })
            .await
            .expect("write");
        assert_eq!(
            response.reference.as_ref().map(|value| value.as_str()),
            Some("mem-2")
        );
        assert!(response.warnings[0].contains("idempotency"));
    }
}
