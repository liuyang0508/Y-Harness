//! Local reference hosts and clients for exercising the public Runtime surface.

mod eval_smoke;
mod service;

use std::{error::Error, path::PathBuf, sync::Arc, time::Duration};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    time::sleep,
};
use y_harness::{
    AllowListPolicy, ApprovalMigrationStatus, CapabilityOrigin, HarnessError, HarnessFuture,
    HarnessRuntime, ItemKind, LanguageModel, MemoryScope, MemoryTaskCoordinator, ModelOutput,
    ModelRequest, ModelResponse, ModelStream, ModelStreamEvent, OperationId, OperationStatus,
    PROTOCOL_VERSION, ProtocolCommand, ProtocolHandler, ProtocolRequest, ProtocolResponseBody,
    ProtocolResult, SqliteApprovalInbox, SqliteEventStore, StateEngine, StateMigrationStatus,
    ThreadId, Tool, ToolContext, ToolDescriptor, ToolRegistry, export_jsonl, serve_stdio,
};

type CliResult<T> = Result<T, Box<dyn Error>>;

struct DemoModel;

impl LanguageModel for DemoModel {
    fn id(&self) -> &str {
        "local/demo"
    }

    fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
        Box::pin(async move {
            if let Some(output) = request
                .items
                .iter()
                .rev()
                .find_map(|item| match &item.kind {
                    ItemKind::ToolResult { output, .. } => Some(output.clone()),
                    _ => None,
                })
            {
                return Ok(ModelOutput::Message {
                    content: format!("Y-Harness observed tool output: {output}"),
                });
            }

            let prompt = request
                .items
                .iter()
                .find_map(|item| match &item.kind {
                    ItemKind::UserMessage { content } => Some(content.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            Ok(ModelOutput::ToolCall {
                call_id: "demo-call".to_owned(),
                name: "echo".to_owned(),
                input: json!({ "text": prompt }),
            })
        })
    }

    fn complete_streaming<'a>(
        &'a self,
        request: ModelRequest,
        stream: ModelStream,
    ) -> HarnessFuture<'a, ModelResponse> {
        Box::pin(async move {
            let output = self.complete(request).await?;
            if let ModelOutput::Message { content } = &output {
                stream.emit_text_delta(content.clone());
            }
            Ok(ModelResponse::from(output))
        })
    }
}

struct EchoTool;

impl Tool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            name: "echo".to_owned(),
            description: "Return the supplied text".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": { "text": { "type": "string" } },
                "required": ["text"]
            }),
        }
    }

    fn execute<'a>(&'a self, input: Value, _context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            let text = input.get("text").and_then(Value::as_str).ok_or_else(|| {
                HarnessError::Tool("echo requires a string field named text".to_owned())
            })?;
            Ok(json!({ "text": text }))
        })
    }
}

/// Runs the direct embedded Runtime demonstration.
pub async fn run_demo(words: Vec<String>) -> CliResult<()> {
    let prompt = if words.is_empty() {
        "hello Y-Harness".to_owned()
    } else {
        words.join(" ")
    };

    let (runtime, state) = build_demo_runtime().await?;
    let thread = runtime.create_thread().await?;
    let outcome = runtime.run_turn(&thread.id, prompt).await?;
    state
        .create_checkpoint(
            &thread.id,
            Some(outcome.turn.id.clone()),
            Some("demo completed".to_owned()),
        )
        .await?;

    let trace_path = PathBuf::from(".y-harness")
        .join("traces")
        .join(format!("{}.jsonl", thread.id.as_str()));
    export_jsonl(&trace_path, &state.events(&thread.id).await?).await?;

    println!("{}", outcome.final_text);
    println!("thread: {}", thread.id);
    println!("trace: {}", trace_path.display());
    Ok(())
}

/// Runs the local typed JSONL protocol host over stdio.
pub async fn run_demo_server() -> CliResult<()> {
    let (runtime, _state) = build_demo_runtime().await?;
    let handler =
        ProtocolHandler::new(runtime).with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()));
    serve_stdio(handler).await?;
    Ok(())
}

/// Creates a no-clobber persistent service project.
pub fn run_init(directory: String) -> CliResult<()> {
    service::run_init(directory)
}

/// Validates one persistent service project without opening its databases.
pub async fn run_doctor(config: String) -> CliResult<()> {
    service::run_doctor(config).await
}

/// Runs one configured persistent JSONL service over stdio.
pub async fn run_service(config: String) -> CliResult<()> {
    service::run_service(config).await
}

/// Performs the explicit backup-first SQLite State migration.
pub async fn run_state_migrate(database: String, backup: String) -> CliResult<()> {
    let report = SqliteEventStore::migrate(&database, &backup).await?;
    match report.status {
        StateMigrationStatus::Migrated => {
            let Some(backup_path) = report.backup_path.as_deref() else {
                return Err("migration completed without a backup path".into());
            };
            println!(
                "migrated State schema {} -> {}; events: {}; required backup bytes: {}; available backup bytes: {}; backup: {}",
                report.from_event_schema,
                report.to_event_schema,
                report.historical_events,
                report.required_backup_bytes,
                report.available_backup_bytes,
                backup_path.display()
            );
        }
        StateMigrationStatus::AlreadyCurrent => {
            println!(
                "State schema {} is already current; events: {}",
                report.to_event_schema, report.historical_events
            );
        }
    }
    Ok(())
}

/// Performs the explicit backup-first SQLite Approval Inbox migration.
pub async fn run_approval_migrate(database: String, backup: String) -> CliResult<()> {
    let report = SqliteApprovalInbox::migrate(&database, &backup).await?;
    match report.status {
        ApprovalMigrationStatus::Migrated => {
            let Some(backup_path) = report.backup_path.as_deref() else {
                return Err("approval migration completed without a backup path".into());
            };
            println!(
                "migrated Approval Inbox schema {} -> {}; records: {}; orphaned pending: {}; required backup bytes: {}; available backup bytes: {}; backup: {}",
                report.from_record_schema,
                report.to_record_schema,
                report.historical_records,
                report.orphaned_pending_records,
                report.required_backup_bytes,
                report.available_backup_bytes,
                backup_path.display()
            );
        }
        ApprovalMigrationStatus::AlreadyCurrent => {
            println!(
                "Approval Inbox schema {} is already current; records: {}",
                report.to_record_schema, report.historical_records
            );
        }
    }
    Ok(())
}

/// Runs the deterministic, versioned Harness regression suite.
pub async fn run_eval_smoke() -> CliResult<()> {
    eval_smoke::run().await
}

/// Runs a dependency-free, line-oriented TUI through the typed protocol.
pub async fn run_tui_demo() -> CliResult<()> {
    let (runtime, _state) = build_demo_runtime().await?;
    let mut client = LocalProtocolClient::new(ProtocolHandler::new(runtime));
    let mut thread_id = client.create_thread().await?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    let mut output = tokio::io::stdout();

    output
        .write_all(
            format!(
                "Y-Harness TUI demo\nthread: {thread_id}\n\
                 commands: /new, /events, /help, /quit\n"
            )
            .as_bytes(),
        )
        .await?;
    loop {
        output
            .write_all(format!("yh[{}]> ", short_id(&thread_id)).as_bytes())
            .await?;
        output.flush().await?;
        let Some(line) = input.next_line().await? else {
            output.write_all(b"\n").await?;
            return Ok(());
        };
        let input = line.trim();
        match input {
            "" => {}
            "/quit" | "/exit" => return Ok(()),
            "/help" => {
                output
                    .write_all(b"/new creates a Thread; /events shows the first 20 events.\n")
                    .await?;
            }
            "/new" => {
                thread_id = client.create_thread().await?;
                output
                    .write_all(format!("created thread {thread_id}\n").as_bytes())
                    .await?;
            }
            "/events" => {
                let rendered = client.render_events(&thread_id).await?;
                output.write_all(rendered.as_bytes()).await?;
            }
            prompt if prompt.starts_with('/') => {
                output.write_all(b"unknown command; use /help\n").await?;
            }
            prompt => match client
                .run_turn(&thread_id, prompt.to_owned(), &mut output)
                .await
            {
                Ok(()) => {}
                Err(error) => {
                    output
                        .write_all(format!("error> {error}\n").as_bytes())
                        .await?;
                }
            },
        }
    }
}

/// Prints the reference binary command surface.
pub fn print_help() {
    println!(
        "Y-Harness\n\nUsage:\n  yh init [directory]\n  yh doctor [config]\n  yh serve [config]\n  yh demo [message]\n  yh tui-demo\n  yh serve-demo\n  yh eval-smoke\n  yh state-migrate <database> <backup>\n  yh approval-migrate <database> <backup>\n  yh --version\n  yh --help\n\n`init` creates a no-clobber local project; config defaults to y-harness.json.\n`doctor` validates config, model authority, credentials, and storage boundaries.\n`serve` opens durable State, Approval, and Task SQLite stores and speaks Protocol v10 JSONL over stdin/stdout.\nDemo and evaluation commands are local and perform no network requests.\nMigration commands require all corresponding writers to be stopped and never overwrite their backup."
    );
}

async fn build_demo_runtime() -> Result<(Arc<HarnessRuntime>, StateEngine), HarnessError> {
    let state = StateEngine::new(Arc::new(
        SqliteEventStore::open(".y-harness/state.db").await?,
    ));
    let runtime = runtime_with_demo_capabilities(state.clone())?;
    Ok((Arc::new(runtime), state))
}

fn runtime_with_demo_capabilities(state: StateEngine) -> Result<HarnessRuntime, HarnessError> {
    let mut tools = ToolRegistry::new();
    tools.register(CapabilityOrigin::BuiltIn, Arc::new(EchoTool))?;

    Ok(HarnessRuntime::new(
        Arc::new(DemoModel),
        tools,
        Arc::new(AllowListPolicy::deny_by_default().allow("echo")),
        state,
    ))
}

struct LocalProtocolClient {
    handler: ProtocolHandler,
    next_request_id: u64,
}

impl LocalProtocolClient {
    fn new(handler: ProtocolHandler) -> Self {
        Self {
            handler,
            next_request_id: 1,
        }
    }

    async fn create_thread(&mut self) -> CliResult<ThreadId> {
        match self.send(ProtocolCommand::CreateThread {}).await? {
            ProtocolResult::ThreadCreated { thread } => Ok(thread.id),
            result => Err(client_error(format!(
                "server returned unexpected create result: {result:?}"
            ))),
        }
    }

    async fn run_turn<W>(
        &mut self,
        thread_id: &ThreadId,
        prompt: String,
        output: &mut W,
    ) -> CliResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        let operation_id = match self
            .send(ProtocolCommand::StartTurn {
                thread_id: thread_id.to_string(),
                prompt,
                memory_scope: MemoryScope::default(),
                timeout_ms: Some(120_000),
            })
            .await?
        {
            ProtocolResult::TurnStarted { operation_id } => operation_id,
            result => {
                return Err(client_error(format!(
                    "server returned unexpected start result: {result:?}"
                )));
            }
        };

        let mut stream_cursor = 0;
        let mut streamed_text = String::new();
        let mut stream_started = false;
        loop {
            self.drain_operation_events(
                &operation_id,
                &mut stream_cursor,
                &mut streamed_text,
                &mut stream_started,
                output,
            )
            .await?;

            let status = match self
                .send(ProtocolCommand::GetOperation {
                    operation_id: operation_id.to_string(),
                })
                .await?
            {
                ProtocolResult::Operation { operation } => operation,
                result => {
                    return Err(client_error(format!(
                        "server returned unexpected operation result: {result:?}"
                    )));
                }
            };
            match status {
                OperationStatus::Running { .. } => sleep(Duration::from_millis(20)).await,
                OperationStatus::Completed { final_text, .. } => {
                    self.drain_operation_events(
                        &operation_id,
                        &mut stream_cursor,
                        &mut streamed_text,
                        &mut stream_started,
                        output,
                    )
                    .await?;
                    self.forget_operation(&operation_id).await?;
                    if stream_started {
                        output.write_all(b"\n").await?;
                        if streamed_text != final_text {
                            output
                                .write_all(format!("assistant(final)> {final_text}\n").as_bytes())
                                .await?;
                        }
                    } else {
                        output
                            .write_all(format!("assistant> {final_text}\n").as_bytes())
                            .await?;
                    }
                    return Ok(());
                }
                OperationStatus::Failed { error }
                | OperationStatus::Cancelled { error }
                | OperationStatus::TimedOut { error } => {
                    self.forget_operation(&operation_id).await?;
                    if stream_started {
                        output.write_all(b"\n").await?;
                    }
                    return Err(client_error(error));
                }
            }
        }
    }

    async fn drain_operation_events<W>(
        &mut self,
        operation_id: &OperationId,
        stream_cursor: &mut u64,
        streamed_text: &mut String,
        stream_started: &mut bool,
        output: &mut W,
    ) -> CliResult<()>
    where
        W: AsyncWrite + Unpin,
    {
        loop {
            let (events, next, has_more, dropped_through) = match self
                .send(ProtocolCommand::GetOperationEvents {
                    operation_id: operation_id.to_string(),
                    after_sequence: Some(*stream_cursor),
                    limit: Some(32),
                })
                .await?
            {
                ProtocolResult::OperationEvents {
                    events,
                    next_after_sequence,
                    has_more,
                    dropped_through_sequence,
                } => (
                    events,
                    next_after_sequence,
                    has_more,
                    dropped_through_sequence,
                ),
                result => {
                    return Err(client_error(format!(
                        "server returned unexpected stream result: {result:?}"
                    )));
                }
            };
            if let Some(dropped_through) = dropped_through
                && *stream_cursor < dropped_through
            {
                output
                    .write_all(format!("[stream gap through {dropped_through}] ").as_bytes())
                    .await?;
                *stream_cursor = dropped_through;
            }
            for event in events {
                match event.event {
                    ModelStreamEvent::TextDelta { delta, .. } => {
                        if !*stream_started {
                            output.write_all(b"assistant> ").await?;
                            *stream_started = true;
                        }
                        streamed_text.push_str(&delta);
                        output.write_all(delta.as_bytes()).await?;
                    }
                }
                *stream_cursor = event.sequence;
            }
            if let Some(next) = next {
                *stream_cursor = next;
            }
            output.flush().await?;
            if !has_more {
                return Ok(());
            }
        }
    }

    async fn render_events(&mut self, thread_id: &ThreadId) -> CliResult<String> {
        match self
            .send(ProtocolCommand::GetEvents {
                thread_id: thread_id.to_string(),
                after_sequence: None,
                limit: Some(20),
            })
            .await?
        {
            ProtocolResult::Events {
                events, has_more, ..
            } => {
                let mut rendered = String::new();
                for event in events {
                    rendered.push_str(&serde_json::to_string(&event)?);
                    rendered.push('\n');
                }
                if has_more {
                    rendered.push_str("… more events available through the protocol cursor\n");
                }
                if rendered.is_empty() {
                    rendered.push_str("no events\n");
                }
                Ok(rendered)
            }
            result => Err(client_error(format!(
                "server returned unexpected events result: {result:?}"
            ))),
        }
    }

    async fn forget_operation(&mut self, operation_id: &OperationId) -> CliResult<()> {
        match self
            .send(ProtocolCommand::ForgetOperation {
                operation_id: operation_id.to_string(),
            })
            .await?
        {
            ProtocolResult::OperationForgotten { .. } => Ok(()),
            result => Err(client_error(format!(
                "server returned unexpected forget result: {result:?}"
            ))),
        }
    }

    async fn send(&mut self, command: ProtocolCommand) -> CliResult<ProtocolResult> {
        let request_id = format!("tui-{}", self.next_request_id);
        self.next_request_id = self.next_request_id.saturating_add(1);
        let response = self
            .handler
            .handle(ProtocolRequest {
                id: request_id,
                protocol_version: PROTOCOL_VERSION.to_owned(),
                command,
            })
            .await;
        match response.body {
            ProtocolResponseBody::Success { result } => Ok(result),
            ProtocolResponseBody::Error { error } => {
                Err(client_error(format!("{}: {}", error.code, error.message)))
            }
        }
    }
}

fn short_id(thread_id: &ThreadId) -> &str {
    thread_id
        .as_str()
        .rsplit('-')
        .next()
        .unwrap_or(thread_id.as_str())
}

fn client_error(message: impl Into<String>) -> Box<dyn Error> {
    std::io::Error::other(message.into()).into()
}
