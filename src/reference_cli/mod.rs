//! Local reference hosts and clients for exercising the public Runtime surface.

mod effect_service;
mod eval_smoke;
mod service;
mod service_stdio;
mod temporal_service;

use std::{error::Error, path::PathBuf, sync::Arc};

use serde_json::{Value, json};
use y_harness::{
    AllowListPolicy, ApprovalMigrationStatus, CapabilityOrigin, HarnessError, HarnessFuture,
    HarnessRuntime, ItemKind, LanguageModel, MemoryTaskCoordinator, ModelOutput, ModelRequest,
    ModelResponse, ModelStream, ProtocolHandler, RuntimeCatalog, RuntimeModelCatalogEntry,
    SqliteApprovalInbox, SqliteEventStore, SqliteTaskCoordinator, StateEngine,
    StateMigrationStatus, TaskMigrationStatus, Tool, ToolBatchExecution, ToolContext,
    ToolDescriptor, ToolRegistry, export_jsonl, serve_stdio,
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

    fn batch_execution(&self) -> ToolBatchExecution {
        ToolBatchExecution::ParallelSafe
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
    let handler = ProtocolHandler::new(runtime)
        .with_task_coordinator(Arc::new(MemoryTaskCoordinator::new()))
        .with_runtime_catalog(RuntimeCatalog {
            configuration_sha256: "0".repeat(64),
            model_route: vec!["local/demo".to_owned()],
            models: vec![RuntimeModelCatalogEntry {
                id: "local/demo".to_owned(),
                adapter: "deterministic_demo".to_owned(),
                endpoint: None,
            }],
            tools: vec!["echo".to_owned()],
            skills: Vec::new(),
            skill_registries: Vec::new(),
            mcp_servers: Vec::new(),
            reload_strategy: "restart_boundary".to_owned(),
        });
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
                "migrated State schema {} -> {}; Agent Loop wait projection {:?} -> {}; events: {}; required backup bytes: {}; available backup bytes: {}; backup: {}",
                report.from_event_schema,
                report.to_event_schema,
                report.from_agent_loop_wait_projection_schema,
                report.to_agent_loop_wait_projection_schema,
                report.historical_events,
                report.required_backup_bytes,
                report.available_backup_bytes,
                backup_path.display()
            );
        }
        StateMigrationStatus::AlreadyCurrent => {
            println!(
                "State schema {} and Agent Loop wait projection {} are already current; events: {}",
                report.to_event_schema,
                report.to_agent_loop_wait_projection_schema,
                report.historical_events
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

/// Performs the explicit backup-first SQLite Task Graph migration.
pub async fn run_task_migrate(database: String, backup: String) -> CliResult<()> {
    let report = SqliteTaskCoordinator::migrate(&database, &backup).await?;
    match report.status {
        TaskMigrationStatus::Migrated => {
            let Some(backup_path) = report.backup_path.as_deref() else {
                return Err("Task Graph migration completed without a backup path".into());
            };
            println!(
                "migrated Task Graph schema {} -> {}; graphs: {}; required backup bytes: {}; available backup bytes: {}; backup: {}",
                report.from_graph_schema,
                report.to_graph_schema,
                report.historical_graphs,
                report.required_backup_bytes,
                report.available_backup_bytes,
                backup_path.display()
            );
        }
        TaskMigrationStatus::AlreadyCurrent => {
            println!(
                "Task Graph schema {} is already current; graphs: {}",
                report.to_graph_schema, report.historical_graphs
            );
        }
    }
    Ok(())
}

/// Installs one validated declarative Skill into a project-local store.
pub fn run_skill_install(package: String, config: String) -> CliResult<()> {
    service::run_skill_install(package, config)
}

/// Installs one signed Skill after checking configured publisher trust.
pub fn run_skill_install_external(package: String, config: String) -> CliResult<()> {
    service::run_skill_install_external(package, config)
}

/// Fetches one exact pinned signed Skill and installs it without activation.
pub async fn run_skill_install_https(
    endpoint: String,
    identity: String,
    expected_sha256: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_install_https(endpoint, identity, expected_sha256, config).await
}

/// Searches one exact digest-pinned remote declarative Skill catalog.
pub async fn run_skill_search_catalog(
    endpoint: String,
    expected_sha256: String,
    query: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_search_catalog(endpoint, expected_sha256, query, config).await
}

/// Installs one exact signed catalog dependency closure without activation.
pub async fn run_skill_install_catalog(
    endpoint: String,
    expected_sha256: String,
    identity: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_install_catalog(endpoint, expected_sha256, identity, config).await
}

/// Installs one exact signed catalog closure and activates its selected root.
pub async fn run_skill_upgrade_catalog(
    endpoint: String,
    expected_sha256: String,
    identity: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_upgrade_catalog(endpoint, expected_sha256, identity, config).await
}

/// Searches one configured authenticated or public Skill Registry.
pub async fn run_skill_search_registry(
    registry: String,
    expected_sha256: String,
    query: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_search_registry(registry, expected_sha256, query, config).await
}

/// Installs one exact closure from a configured Registry without activation.
pub async fn run_skill_install_registry(
    registry: String,
    expected_sha256: String,
    identity: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_install_registry(registry, expected_sha256, identity, config).await
}

/// Installs and activates one exact closure from a configured Registry.
pub async fn run_skill_upgrade_registry(
    registry: String,
    expected_sha256: String,
    identity: String,
    config: String,
) -> CliResult<()> {
    service::run_skill_upgrade_registry(registry, expected_sha256, identity, config).await
}

/// Lists validated declarative Skills in a project-local store.
pub fn run_skill_list(config: String) -> CliResult<()> {
    service::run_skill_list(config)
}

/// Verifies every declarative Skill in a project-local store.
pub fn run_skill_verify(config: String) -> CliResult<()> {
    service::run_skill_verify(config)
}

/// Moves one unreferenced project Skill into recoverable project trash.
pub async fn run_skill_remove(identity: String, config: String) -> CliResult<()> {
    service::run_skill_remove(identity, config).await
}

/// Atomically activates one installed Skill dependency graph.
pub async fn run_skill_activate(identity: String, config: String) -> CliResult<()> {
    service::run_skill_activate(identity, config).await
}

/// Atomically deactivates one exact root Skill.
pub async fn run_skill_deactivate(identity: String, config: String) -> CliResult<()> {
    service::run_skill_deactivate(identity, config).await
}

/// Lists immutable project configuration revisions.
pub fn run_skill_history(config: String) -> CliResult<()> {
    service::run_skill_history(config)
}

/// Restores one digest-addressed project configuration revision.
pub async fn run_skill_rollback(revision: String, config: String) -> CliResult<()> {
    service::run_skill_rollback(revision, config).await
}

/// Exports one terminal durable Thread to a no-clobber portable archive.
pub async fn run_thread_export(
    thread_id: String,
    archive: String,
    config: String,
) -> CliResult<()> {
    service::run_thread_export(thread_id, archive, config).await
}

/// Atomically imports one portable archive under a caller-chosen identity.
pub async fn run_thread_import(
    archive: String,
    target_thread_id: String,
    config: String,
) -> CliResult<()> {
    service::run_thread_import(archive, target_thread_id, config).await
}

/// Runs the deterministic, versioned Harness regression suite.
pub async fn run_eval_smoke() -> CliResult<()> {
    eval_smoke::run().await
}

/// Runs one configured Evaluation suite against an exact baseline.
pub async fn run_evaluation(suite: String, baseline: String, config: String) -> CliResult<()> {
    service::run_evaluation(suite, baseline, config).await
}

/// Prints the reference binary command surface.
pub fn print_help() {
    println!(
        "Y-Harness\n\nUsage:\n  yh init [directory]\n  yh doctor [config]\n  yh serve [config]\n  yh demo [message]\n  yh serve-demo\n  yh eval <suite> <baseline> [config]\n  yh eval-smoke\n  yh thread export <thread-id> <archive> [config]\n  yh thread import <archive> <target-thread-id> [config]\n  yh skill|package install <package> [config]\n  yh skill|package install-external <signed-package> [config]\n  yh skill|package install-https <url> <name@version> <sha256> [config]\n  yh skill|package search-https <catalog-url> <catalog-sha256> <query|*> [config]\n  yh skill|package install-catalog <catalog-url> <catalog-sha256> <name@version> [config]\n  yh skill|package upgrade-catalog <catalog-url> <catalog-sha256> <name@version> [config]\n  yh skill|package registry-search <registry> <catalog-sha256> <query|*> [config]\n  yh skill|package registry-install <registry> <catalog-sha256> <name@version> [config]\n  yh skill|package registry-upgrade <registry> <catalog-sha256> <name@version> [config]\n  yh skill|package activate <name@version> [config]\n  yh skill|package deactivate <name@version> [config]\n  yh skill|package list [config]\n  yh skill|package verify [config]\n  yh skill|package history [config]\n  yh skill|package rollback <config-sha256> [config]\n  yh skill|package remove <name@version> [config]\n  yh state-migrate <database> <backup>\n  yh approval-migrate <database> <backup>\n  yh task-migrate <database> <backup>\n  yh --version\n  yh --help\n\n`init` creates a no-clobber local project; config defaults to y-harness.json.\n`doctor` validates config, model authority, credentials, storage boundaries, configured Evaluation Graders, and existing durable stores without creating or migrating them.\n`serve` opens durable State, Approval, Task, Workflow, Human Handoff, and Effect SQLite stores and speaks the current typed JSONL Protocol over stdin/stdout.\n`eval` runs an isolated in-memory target with the configured Model, capabilities, and external Graders; configured process and network authority still applies.\nThread export accepts terminal histories and never overwrites an archive; import atomically creates the caller-named target.\nSkill and package are aliases for the governed declarative package lifecycle: installation never activates, activation preflights the complete configuration, and every mutation retains a digest-addressed rollback revision.\nHTTPS Skill, digest-pinned catalog, and configured Registry operations require the optional `https-skill` Cargo feature. Catalog and Registry installs resolve exact signed dependencies into an inactive local cache; upgrade operations activate only after the complete closure is verified. Registry credentials are resolved per request and are never accepted in URLs, catalog bytes, receipts, or command arguments.\nDemo and `eval-smoke` are local and perform no network requests.\nMigration commands require all corresponding writers to be stopped and never overwrite their backup.\nThe optional full-screen product is installed separately as `yh-tui`."
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
