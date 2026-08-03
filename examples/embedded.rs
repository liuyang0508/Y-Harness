//! Minimal host that embeds Y-Harness through public contracts only.

use std::{error::Error, io, sync::Arc};

use serde_json::{Value, json};
use y_harness::{
    AllowListPolicy, CapabilityOrigin, HarnessError, HarnessFuture, HarnessRuntime, ItemKind,
    LanguageModel, MemoryEventStore, ModelOutput, ModelRegistry, ModelRequest, StateEngine, Tool,
    ToolContext, ToolDescriptor, ToolRegistry,
};

struct HostModel;

impl LanguageModel for HostModel {
    fn id(&self) -> &str {
        "example/local"
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move {
            if let Some(text) = request
                .items
                .iter()
                .rev()
                .find_map(|item| match &item.kind {
                    ItemKind::ToolResult { output, .. } => {
                        output.get("text").and_then(Value::as_str)
                    }
                    _ => None,
                })
            {
                return Ok(ModelOutput::Message {
                    content: format!("embedded: {text}"),
                });
            }
            let prompt = request
                .items
                .iter()
                .find_map(|item| match &item.kind {
                    ItemKind::UserMessage { content } => Some(content.clone()),
                    _ => None,
                })
                .ok_or_else(|| HarnessError::Model("missing user input".to_owned()))?;
            Ok(ModelOutput::ToolCall {
                call_id: "example-call".to_owned(),
                name: "echo".to_owned(),
                input: json!({ "text": prompt }),
            })
        })
    }
}

struct EchoTool;

impl Tool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "echo".to_owned(),
            description: "Returns the supplied text".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"]
            }),
        }
    }

    fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            if input.get("text").and_then(Value::as_str).is_none() {
                return Err(HarnessError::Tool(
                    "echo requires a string field named text".to_owned(),
                ));
            }
            Ok(input)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let origin = CapabilityOrigin::TrustedExtension {
        id: "example-host".to_owned(),
    };
    let mut models = ModelRegistry::new();
    models.register(origin.clone(), Arc::new(HostModel))?;
    let mut tools = ToolRegistry::new();
    tools.register(origin.clone(), Arc::new(EchoTool))?;

    let runtime = HarnessRuntime::from_model_registry(
        &models,
        "example/local",
        tools,
        Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
        StateEngine::new(Arc::new(MemoryEventStore::new())),
    )?
    .with_model_visible_tools(["echo"])?;
    let thread = runtime.create_thread().await?;
    let outcome = runtime.run_turn(&thread.id, "hello Harness").await?;
    let tool_completed = outcome.turn.items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::ToolResult {
                output,
                is_error: false,
                ..
            } if output == &json!({ "text": "hello Harness" })
        )
    });
    let policy_bound_origin = outcome.turn.items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::PolicyDecision {
                tool_origin: Some(tool_origin),
                ..
            } if tool_origin == &origin
        )
    });
    if outcome.final_text != "embedded: hello Harness" || !tool_completed || !policy_bound_origin {
        return Err(io::Error::other("embedded Agent Loop contract regressed").into());
    }

    println!("{}", outcome.final_text);
    println!("thread: {}", thread.id);
    Ok(())
}
