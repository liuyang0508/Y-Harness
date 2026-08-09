use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::{
    io::{BufRead, BufReader},
    os::unix::fs::PermissionsExt,
};

use ed25519_dalek::{Signer, SigningKey};
use semver::Version;
#[cfg(unix)]
use sha2::{Digest, Sha256};
use y_harness::{
    APPROVAL_INBOX_SCHEMA_VERSION, EFFECT_DISPATCH_GOVERNOR_SCHEMA_VERSION,
    EFFECT_LEDGER_SCHEMA_VERSION, EffectCommandId, EffectCreateRequest, EffectOperation,
    HUMAN_HANDOFF_SCHEMA_VERSION, HumanHandoffCommandId, HumanHandoffCreateRequest,
    HumanHandoffSubject, Item, ItemKind, PROTOCOL_VERSION, ProtocolCommand, ProtocolRequest,
    ProtocolResponse, ProtocolResponseBody, ProtocolResult, SECRET_API_VERSION,
    STATE_EVENT_SCHEMA_VERSION, STATE_SNAPSHOT_SCHEMA_VERSION, SignedSkillPackage, SkillPackage,
    SkillSignature, SkillTransparencyReceipt, SqliteEventStore, SqliteTaskCoordinator,
    SqliteWorkflowCoordinator, StateEngine, TASK_GRAPH_SCHEMA_VERSION, TaskCoordinator,
    TaskDefinition, TaskGraph, TaskGraphId, TaskId, ThreadId, TurnStatus,
    WORKFLOW_RUN_SCHEMA_VERSION, WorkflowCommand, WorkflowCommandId, WorkflowCommandKind,
    WorkflowCreateRequest, WorkflowDefinition, WorkflowEngine, WorkflowRunId, WorkflowStatus,
    WorkflowTransitionKind, WorkflowWaitId, WorkspaceMode, decode_thread_archive,
};
#[cfg(unix)]
use y_harness::{
    CapabilityOrigin, EffectEngine, EffectId, EffectStatus, OperationStatus,
    SqliteEffectCoordinator,
};

const STATE_V1_FIXTURE: &str = include_str!("fixtures/state-v1.sql");

#[test]
fn init_is_no_clobber_and_doctor_validates_the_project() {
    let project = isolated_project("init");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(
        initialized.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let config_path = project.join("y-harness.json");
    let original = fs::read(&config_path).expect("read initialized config");
    assert!(project.join(".y-harness").is_dir());
    assert_eq!(
        fs::read_to_string(project.join(".gitignore")).expect("read gitignore"),
        ".y-harness/\n"
    );

    let repeated = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("repeat init");
    assert!(!repeated.status.success());
    assert_eq!(
        fs::read(&config_path).expect("reread initialized config"),
        original,
        "failed init must not replace configuration"
    );

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("run doctor");
    assert!(
        doctor.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report = String::from_utf8(doctor.stdout).expect("UTF-8 doctor report");
    assert!(report.contains(&format!("protocol: {PROTOCOL_VERSION}")));
    assert!(report.contains(&format!(
        "schemas: state={STATE_EVENT_SCHEMA_VERSION}/{STATE_SNAPSHOT_SCHEMA_VERSION} approval={APPROVAL_INBOX_SCHEMA_VERSION} task={TASK_GRAPH_SCHEMA_VERSION} workflow={WORKFLOW_RUN_SCHEMA_VERSION} handoff={HUMAN_HANDOFF_SCHEMA_VERSION} effect={EFFECT_LEDGER_SCHEMA_VERSION} effect-governance={EFFECT_DISPATCH_GOVERNOR_SCHEMA_VERSION} secret={SECRET_API_VERSION}"
    )));
    assert!(report.contains("model: local/demo"));
    assert!(report.contains("authority: local-process / unscoped"));
    assert!(report.contains("parallel tools: 1 safe / 4 maximum"));
    assert!(report.contains("verifiers: 0"));
    assert!(report.contains("evaluation graders: 0"));
    assert!(report.contains("mcp servers: 0 enabled / 0 configured"));
    assert!(report.contains("mcp command locks: 0 / 0 stdio enabled"));
    assert!(report.contains("skills: 0"));
    assert!(report.contains("conversation: 32 Turns / 65536 tokens / 65536 bytes"));
    assert!(report.contains("conversation compactor: disabled"));
    assert!(report.contains("temporal: disabled"));
    assert!(report.contains("effect consumer: disabled"));
    assert!(report.contains(
        "stores: state=will be created approval=will be created task=will be created \
         workflow=will be created handoff=will be created effect=will be created"
    ));
    assert!(report.contains("status: ok"));

    let initialized = serve(
        &project,
        vec![request("initialize", ProtocolCommand::Initialize {})],
    );
    assert!(matches!(
        initialized[0].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized { .. }
        }
    ));
    let current_doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("run current-store doctor");
    assert!(
        current_doctor.status.success(),
        "current-store doctor failed: {}",
        String::from_utf8_lossy(&current_doctor.stderr)
    );
    assert!(
        String::from_utf8(current_doctor.stdout)
            .expect("UTF-8 current-store doctor")
            .contains(
                "stores: state=ready approval=ready task=ready workflow=ready handoff=ready effect=ready"
            )
    );
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn doctor_rejects_legacy_state_before_provider_assembly_without_mutation() {
    let project = isolated_project("doctor-legacy-state");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let database = project.join(".y-harness/state.db");
    {
        let connection = rusqlite::Connection::open(&database).expect("open legacy State fixture");
        connection
            .execute_batch(STATE_V1_FIXTURE)
            .expect("install legacy State fixture");
    }
    let before = fs::read(&database).expect("read legacy State before doctor");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {
                "type": "json_command",
                "id": "external/must-not-start",
                "process": {
                    "command": project.join("missing-model"),
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }
        }))
        .expect("encode legacy doctor config"),
    )
    .expect("write legacy doctor config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg("y-harness.json")
        .current_dir(&project)
        .output()
        .expect("run legacy State doctor");
    assert!(!doctor.status.success());
    let diagnostic = String::from_utf8_lossy(&doctor.stderr);
    assert!(
        diagnostic.contains("SQLite State schema migration required"),
        "unexpected doctor diagnostic: {diagnostic}"
    );
    assert!(diagnostic.contains("yh state-migrate"));
    assert!(!diagnostic.contains("missing-model"));
    assert_eq!(
        fs::read(&database).expect("read legacy State after doctor"),
        before,
        "doctor must not mutate a legacy database"
    );
    assert!(
        fs::read_dir(project.join(".y-harness"))
            .expect("read data directory")
            .all(|entry| !entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .contains("backup")),
        "doctor must not create a migration backup"
    );

    let service = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("serve")
        .arg("y-harness.json")
        .current_dir(&project)
        .stdin(Stdio::null())
        .output()
        .expect("run legacy State service");
    assert!(!service.status.success());
    let service_diagnostic = String::from_utf8_lossy(&service.stderr);
    assert!(
        service_diagnostic.contains("SQLite State schema migration required"),
        "unexpected service diagnostic: {service_diagnostic}"
    );
    assert!(service_diagnostic.contains("yh state-migrate"));
    assert!(!service_diagnostic.contains("missing-model"));
    assert_eq!(
        fs::read(&database).expect("read legacy State after service preflight"),
        before,
        "service preflight must not mutate a legacy database"
    );
    fs::remove_dir_all(project).expect("remove legacy doctor fixture");
}

#[test]
fn doctor_rejects_a_partial_workflow_store() {
    let project = isolated_project("doctor-partial-workflow");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let database = project.join(".y-harness/workflows.db");
    {
        let connection =
            rusqlite::Connection::open(&database).expect("open partial Workflow fixture");
        connection
            .execute_batch(
                "CREATE TABLE workflow_store_meta (
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );",
            )
            .expect("install partial Workflow fixture");
    }

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg("y-harness.json")
        .current_dir(&project)
        .output()
        .expect("run partial Workflow doctor");
    assert!(!doctor.status.success());
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("SQLite Workflow store is partial"));
    fs::remove_dir_all(project).expect("remove partial Workflow fixture");
}

#[test]
fn doctor_and_service_reject_a_partial_effect_store_before_model_construction() {
    let project = isolated_project("doctor-partial-effect");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let database = project.join(".y-harness/effects.db");
    {
        let connection =
            rusqlite::Connection::open(&database).expect("open partial Effect fixture");
        connection
            .execute_batch(
                "CREATE TABLE effect_store_meta (
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );",
            )
            .expect("install partial Effect fixture");
    }
    let before = fs::read(&database).expect("read partial Effect fixture");

    for command in ["doctor", "serve"] {
        let output = Command::new(env!("CARGO_BIN_EXE_yh"))
            .arg(command)
            .arg("y-harness.json")
            .current_dir(&project)
            .stdin(Stdio::null())
            .output()
            .expect("run partial Effect preflight");
        assert!(!output.status.success());
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(
            diagnostic.contains("SQLite Effect Ledger is partial"),
            "unexpected {command} diagnostic: {diagnostic}"
        );
        assert!(
            !diagnostic.contains("missing-model"),
            "external Model construction ran before Effect preflight"
        );
        assert_eq!(
            fs::read(&database).expect("read partial Effect after preflight"),
            before,
            "{command} preflight mutated the partial Effect store"
        );
    }
    fs::remove_dir_all(project).expect("remove partial Effect fixture");
}

#[test]
fn doctor_and_service_reject_a_partial_effect_governor_store_without_mutation() {
    let project = isolated_project("doctor-partial-effect-governor");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let database = project.join(".y-harness/effect-governance.db");
    rusqlite::Connection::open(&database)
        .expect("open partial governor fixture")
        .execute_batch(
            "CREATE TABLE effect_dispatch_governor_meta (
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL
            );",
        )
        .expect("install partial governor fixture");
    let before = fs::read(&database).expect("read partial governor fixture");

    for command in ["doctor", "serve"] {
        let output = Command::new(env!("CARGO_BIN_EXE_yh"))
            .arg(command)
            .arg("y-harness.json")
            .current_dir(&project)
            .stdin(Stdio::null())
            .output()
            .expect("run partial governor preflight");
        assert!(!output.status.success());
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(
            diagnostic.contains("Effect dispatch governor: SQLite store is partial"),
            "unexpected {command} diagnostic: {diagnostic}"
        );
        assert_eq!(
            fs::read(&database).expect("read partial governor after preflight"),
            before,
            "{command} preflight mutated the partial governor store"
        );
    }
    fs::remove_dir_all(project).expect("remove partial governor fixture");
}

#[test]
fn configured_temporal_service_advances_durable_wait_and_stops_cleanly() {
    let project = isolated_project("temporal-service");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(
        initialized.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );
    let config_path = project.join("y-harness.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&config_path).expect("read initialized Temporal config"))
            .expect("decode initialized Temporal config");
    config["temporal"] = serde_json::json!({
        "poll_interval_ms": 100,
        "scan_limit": 16
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("encode Temporal config"),
    )
    .expect("write Temporal config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("run Temporal doctor");
    assert!(
        doctor.status.success(),
        "Temporal doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert!(
        String::from_utf8(doctor.stdout)
            .expect("UTF-8 Temporal doctor")
            .contains("temporal: enabled (100 ms / 16 identities per source)")
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build Temporal test Runtime");
    let now_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_millis(),
    )
    .expect("Unix milliseconds");
    let workflow = runtime.block_on(async {
        let tasks = Arc::new(
            SqliteTaskCoordinator::open(project.join(".y-harness/tasks.db"))
                .await
                .expect("open Task Coordinator"),
        );
        let graph_id = TaskGraphId::from_static("temporal-service-graph");
        tasks
            .create(
                graph_id.clone(),
                TaskGraph::new(vec![TaskDefinition {
                    id: TaskId::from_static("work"),
                    description: "durable Temporal service fixture".to_owned(),
                    dependencies: Default::default(),
                    priority: 0,
                    workspace: WorkspaceMode::None,
                    required_capabilities: Default::default(),
                }])
                .expect("build Task Graph"),
            )
            .await
            .expect("create Task Graph");
        let workflows = Arc::new(
            SqliteWorkflowCoordinator::open(project.join(".y-harness/workflows.db"))
                .await
                .expect("open Workflow Coordinator"),
        );
        let workflow = WorkflowEngine::new(workflows, tasks);
        let run_id = WorkflowRunId::from_static("temporal-service-run");
        workflow
            .create(
                run_id.clone(),
                WorkflowCreateRequest {
                    command_id: WorkflowCommandId::from_static("create"),
                    definition: WorkflowDefinition {
                        name: "test.temporal-service".to_owned(),
                        version: Version::new(1, 0, 0),
                        content_sha256: "a".repeat(64),
                    },
                    task_graph_id: graph_id,
                },
                now_ms,
            )
            .await
            .expect("create Workflow Run");
        workflow
            .apply(
                &run_id,
                1,
                WorkflowCommand {
                    id: WorkflowCommandId::from_static("wait"),
                    kind: WorkflowCommandKind::WaitUntil {
                        wait_id: WorkflowWaitId::from_static("timer"),
                        due_at_ms: now_ms + 250,
                    },
                },
                now_ms + 1,
            )
            .await
            .expect("start durable wait");
        (workflow, run_id)
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn Temporal service");
    let input = child.stdin.take().expect("Temporal service stdin");
    let (workflow, run_id) = workflow;
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let run = workflow
                    .load(&run_id)
                    .await
                    .expect("load Workflow Run")
                    .expect("Workflow Run exists");
                if matches!(run.run().status(), WorkflowStatus::Running)
                    && matches!(
                        run.run()
                            .transitions()
                            .last()
                            .map(|transition| &transition.kind),
                        Some(WorkflowTransitionKind::WaitDue { .. })
                    )
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Temporal service advances due Workflow");
    });

    drop(input);
    let settled = child.wait_with_output().expect("settle Temporal service");
    assert!(
        settled.status.success(),
        "Temporal service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    assert!(
        settled.stdout.is_empty(),
        "idle Temporal service must preserve protocol stdout purity"
    );
    assert!(
        settled.stderr.is_empty(),
        "healthy Temporal service emitted diagnostics: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    fs::remove_dir_all(project).expect("remove Temporal service fixture");
}

#[cfg(unix)]
#[test]
fn configured_effect_consumer_degrades_recovers_stops_and_does_not_replay_terminal_effects() {
    const HOST_SECRET_NAME: &str = "YH_EFFECT_TEST_TOKEN";
    const SECRET_VALUE: &str = "effect-e2e-secret-token";
    let project = isolated_project("effect-consumer");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("init Effect consumer project");
    assert!(
        initialized.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&initialized.stderr)
    );

    let execution = project.join("effect-execute");
    let reconciliation = project.join("effect-reconcile");
    write_executable(
        &execution,
        "#!/bin/sh\n\
         if [ -z \"${EFFECT_TOKEN:-}\" ]; then exit 23; fi\n\
         read -r request\n\
         printf '%s\\n' \"$request\" >> execution-requests.jsonl\n\
         printf '%s\\n' \
         '{\"protocol_version\":1,\"outcome\":{\"outcome\":\"unknown\",\
         \"reason_code\":\"target.uncertain\"}}'\n",
    );
    let reconciliation_content = "#!/bin/sh\n\
         if [ -z \"${EFFECT_TOKEN:-}\" ]; then exit 23; fi\n\
         read -r request\n\
         printf '%s\\n' \"$request\" >> reconciliation-requests.jsonl\n\
         if [ -f reconciliation-ready ]; then\n\
           printf '%s\\n' \
         '{\"protocol_version\":1,\"outcome\":{\"outcome\":\"applied\",\"receipt\":{\
         \"source\":\"test-target\",\"external_id\":\"message-42\",\"observed_at_ms\":1,\
         \"response_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}}}'\n\
         else\n\
           printf '%s\\n' '{\"protocol_version\":1,\"malformed\":true}'\n\
         fi\n";
    write_executable(&reconciliation, reconciliation_content);

    let config_path = project.join("y-harness.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(&fs::read(&config_path).expect("read Effect consumer config"))
            .expect("decode Effect consumer config");
    let process = |command: &Path| {
        serde_json::json!({
            "command": command,
            "command_sha256": sha256_file(command),
            "current_directory": ".",
            "secret_environment": {
                "EFFECT_TOKEN": {
                    "reference": "effect/notification-test",
                    "host_environment": HOST_SECRET_NAME
                }
            },
            "timeout_ms": 5_000,
            "max_output_bytes": 65_536,
            "launch": {
                "type": "unrestricted",
                "max_concurrency": 1
            }
        })
    };
    config["effect_consumer"] = serde_json::json!({
        "execution": {
            "poll_interval_ms": 100,
            "failure_backoff_ms": 100,
            "executor": {
                "scan_limit": 16,
                "max_concurrency": 2,
                "policy_timeout_ms": 1_000,
                "governor_timeout_ms": 1_000,
                "governor_retry_after_ms": 100,
                "execution_timeout_ms": 10_000,
                "settlement_reserve_ms": 5_000,
                "lease_duration_ms": 20_000
            },
            "governor": {
                "policy_id": "notification-test-v1",
                "max_dispatches_per_window": 16,
                "window_ms": 1_000,
                "failure_threshold": 2,
                "open_duration_ms": 1_000,
                "probe_lease_ms": 500,
                "admission_retention_ms": 604_800_000
            },
            "allow": [{
                "capability": "notification.test",
                "operation": "send"
            }],
            "connectors": [{
                "origin_id": "test/effect-execution",
                "capability": "notification.test",
                "operations": ["send"],
                "idempotency": "target_enforced",
                "process": process(&execution)
            }]
        },
        "reconciliation": {
            "poll_interval_ms": 100,
            "failure_backoff_ms": 100,
            "reconciler": {
                "scan_limit": 16,
                "max_concurrency": 2,
                "policy_timeout_ms": 1_000,
                "lookup_timeout_ms": 10_000
            },
            "allow": [{
                "capability": "notification.test",
                "operation": "send"
            }],
            "connectors": [{
                "origin_id": "test/effect-reconciliation",
                "capability": "notification.test",
                "operations": ["send"],
                "contract": "authoritative_read_only",
                "process": process(&reconciliation)
            }]
        }
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("encode Effect consumer config"),
    )
    .expect("write Effect consumer config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .env(HOST_SECRET_NAME, SECRET_VALUE)
        .output()
        .expect("run Effect consumer doctor");
    assert!(
        doctor.status.success(),
        "Effect consumer doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert!(
        String::from_utf8(doctor.stdout)
            .expect("UTF-8 Effect consumer doctor")
            .contains(
                "effect consumer: enabled (execution 1 dispatch-locked connector(s) / \
             1 credential-scoped / 1 secret variable(s) / 1 allow(s) / governor \
             notification-test-v1: 16/1000 ms, 2 failures/1000 ms, 500 ms probe / \
             100 ms poll / 100 ms backoff; reconciliation 1 dispatch-locked connector(s) / \
             1 credential-scoped / 1 secret variable(s) / 1 allow(s) / 100 ms poll / \
             100 ms backoff)"
            )
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build Effect consumer test Runtime");
    let effect_id = EffectId::from_static("service-effect-consumer");
    let effect_engine = runtime.block_on(async {
        let effects = Arc::new(
            SqliteEffectCoordinator::open(project.join(".y-harness/effects.db"))
                .await
                .expect("open Effect Coordinator"),
        );
        let engine = EffectEngine::new(effects);
        engine
            .create(
                effect_id.clone(),
                EffectCreateRequest {
                    command_id: EffectCommandId::from_static("create-service-effect"),
                    operation: EffectOperation {
                        capability: "notification.test".to_owned(),
                        operation: "send".to_owned(),
                    },
                    idempotency_key: "service-effect-idempotency".to_owned(),
                    input: serde_json::json!({"artifact_ref":"message-42"}),
                    not_before_ms: 1,
                },
                1,
            )
            .await
            .expect("create pending service Effect");
        engine
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .env(HOST_SECRET_NAME, SECRET_VALUE)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn Effect consumer service");
    let input = child.stdin.take().expect("Effect consumer service stdin");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let effect = effect_engine
                    .load(&effect_id)
                    .await
                    .expect("load uncertain Effect")
                    .expect("Effect exists");
                if matches!(effect.effect().status(), EffectStatus::Unknown { .. })
                    && project.join("execution-requests.jsonl").is_file()
                    && project.join("reconciliation-requests.jsonl").is_file()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Effect consumer reaches degraded reconciliation");
    });

    write_executable(
        &reconciliation,
        "#!/bin/sh\n\
         touch tampered-connector-ran\n\
         printf '%s\\n' \
         '{\"protocol_version\":1,\"outcome\":{\"outcome\":\"applied\",\"receipt\":{\
         \"source\":\"tampered-target\",\"external_id\":\"forged\",\"observed_at_ms\":1,\
         \"response_sha256\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"}}}'\n",
    );
    std::thread::sleep(Duration::from_millis(150));
    let reconciliation_during_drift =
        fs::read(project.join("reconciliation-requests.jsonl")).expect("read drift requests");
    std::thread::sleep(Duration::from_millis(350));
    assert!(
        !project.join("tampered-connector-ran").exists(),
        "dispatch-time digest lock allowed a drifted Connector to start"
    );
    assert_eq!(
        fs::read(project.join("reconciliation-requests.jsonl"))
            .expect("read requests during drift"),
        reconciliation_during_drift,
        "a drifted Connector received an Effect request"
    );
    let still_unknown = runtime
        .block_on(effect_engine.load(&effect_id))
        .expect("load Effect after Connector drift")
        .expect("Effect exists");
    assert!(matches!(
        still_unknown.effect().status(),
        EffectStatus::Unknown { .. }
    ));

    write_executable(&reconciliation, reconciliation_content);
    fs::write(project.join("reconciliation-ready"), b"ready")
        .expect("make authoritative target result visible");
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let effect = effect_engine
                    .load(&effect_id)
                    .await
                    .expect("load converged Effect")
                    .expect("Effect exists");
                if matches!(effect.effect().status(), EffectStatus::Applied { .. }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Effect consumer recovers and converges");
    });

    drop(input);
    let settled = child
        .wait_with_output()
        .expect("settle Effect consumer service");
    assert!(
        settled.status.success(),
        "Effect consumer service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    assert!(
        settled.stdout.is_empty(),
        "idle Effect consumer service must preserve protocol stdout purity"
    );
    let diagnostics = String::from_utf8(settled.stderr).expect("UTF-8 Effect diagnostics");
    assert!(!diagnostics.contains(SECRET_VALUE));
    assert!(
        !fs::read_to_string(&config_path)
            .expect("read Secret-reference config")
            .contains(SECRET_VALUE)
    );
    assert!(
        !String::from_utf8_lossy(
            &fs::read(project.join("execution-requests.jsonl"))
                .expect("read execution requests for Secret leak")
        )
        .contains(SECRET_VALUE)
    );
    assert!(
        !String::from_utf8_lossy(
            &fs::read(project.join("reconciliation-requests.jsonl"))
                .expect("read reconciliation requests for Secret leak")
        )
        .contains(SECRET_VALUE)
    );
    assert!(
        diagnostics.contains("Y-Harness Effect reconciliation degraded: 1 attempt(s) unavailable")
    );
    assert!(diagnostics.contains("Y-Harness Effect reconciliation recovered"));
    assert!(
        project.join(".y-harness/effect-governance.db").is_file(),
        "configured durable Effect governor store was not created"
    );

    let execution_before_restart =
        fs::read(project.join("execution-requests.jsonl")).expect("read execution requests");
    let reconciliation_before_restart = fs::read(project.join("reconciliation-requests.jsonl"))
        .expect("read reconciliation requests");
    let mut restarted = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .env(HOST_SECRET_NAME, SECRET_VALUE)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("restart Effect consumer service");
    let restarted_input = restarted.stdin.take().expect("restarted service stdin");
    std::thread::sleep(Duration::from_millis(350));
    drop(restarted_input);
    let restarted = restarted
        .wait_with_output()
        .expect("settle restarted Effect consumer service");
    assert!(
        restarted.status.success(),
        "restarted Effect consumer failed: {}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    assert!(restarted.stdout.is_empty());
    assert!(restarted.stderr.is_empty());
    assert_eq!(
        fs::read(project.join("execution-requests.jsonl")).expect("reread execution requests"),
        execution_before_restart,
        "terminal Effect was executed after restart"
    );
    assert_eq!(
        fs::read(project.join("reconciliation-requests.jsonl"))
            .expect("reread reconciliation requests"),
        reconciliation_before_restart,
        "terminal Effect was reconciled after restart"
    );
    fs::remove_dir_all(project).expect("remove Effect consumer fixture");
}

#[test]
fn thread_archive_cli_round_trips_without_clobber_or_partial_import() {
    let project = isolated_project("thread-archive");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let config = project.join("y-harness.json");
    let database = project.join(".y-harness/state.db");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build test Runtime");
    let source_id = runtime.block_on(async {
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&database)
                .await
                .expect("open State database"),
        ));
        let source = state.create_thread().await.expect("create source");
        state
            .set_thread_name(&source.id, Some("CLI portable".to_owned()))
            .await
            .expect("name source");
        let turn = state.start_turn(&source.id).await.expect("start Turn");
        state
            .append_item(
                &turn,
                Item::new(ItemKind::UserMessage {
                    content: "portable".to_owned(),
                }),
            )
            .await
            .expect("append Item");
        state
            .finish_turn(&turn, TurnStatus::Completed)
            .await
            .expect("finish Turn");
        source.id
    });

    let archive_path = project.join("source.yh-thread.json");
    let exported = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["thread", "export"])
        .arg(source_id.as_str())
        .arg(&archive_path)
        .arg(&config)
        .output()
        .expect("export archive");
    assert!(
        exported.status.success(),
        "export failed: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let original = fs::read(&archive_path).expect("read archive");
    let archive = decode_thread_archive(&original).expect("decode archive");
    assert_eq!(archive.source_thread_id, source_id);

    let repeated_export = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["thread", "export"])
        .arg(source_id.as_str())
        .arg(&archive_path)
        .arg(&config)
        .output()
        .expect("repeat export");
    assert!(!repeated_export.status.success());
    assert_eq!(fs::read(&archive_path).expect("reread archive"), original);

    let target_id = ThreadId::from_static("cli-import-target");
    for _ in 0..2 {
        let imported = Command::new(env!("CARGO_BIN_EXE_yh"))
            .args(["thread", "import"])
            .arg(&archive_path)
            .arg(target_id.as_str())
            .arg(&config)
            .output()
            .expect("import archive");
        assert!(
            imported.status.success(),
            "import failed: {}",
            String::from_utf8_lossy(&imported.stderr)
        );
    }

    let mut tampered: serde_json::Value =
        serde_json::from_slice(&original).expect("decode archive JSON");
    tampered["source_events_sha256"] = serde_json::Value::String("0".repeat(64));
    let tampered_path = project.join("tampered.yh-thread.json");
    fs::write(
        &tampered_path,
        serde_json::to_vec(&tampered).expect("encode tampered archive"),
    )
    .expect("write tampered archive");
    let rejected = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["thread", "import"])
        .arg(&tampered_path)
        .arg("cli-tampered-target")
        .arg(&config)
        .output()
        .expect("reject tampered archive");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("digest mismatch"));

    runtime.block_on(async {
        let state = StateEngine::new(Arc::new(
            SqliteEventStore::open(&database)
                .await
                .expect("reopen State database"),
        ));
        let imported = state
            .load_thread(&target_id)
            .await
            .expect("load target")
            .expect("target Thread");
        assert_eq!(imported.name.as_deref(), Some("CLI portable"));
        assert_eq!(imported.turns.len(), 1);
        assert_eq!(
            imported
                .import_origin
                .as_ref()
                .expect("import provenance")
                .source_thread_id,
            source_id
        );
        assert!(
            state
                .load_thread(&ThreadId::from_static("cli-tampered-target"))
                .await
                .expect("load rejected target")
                .is_none()
        );
    });
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn doctor_loads_exact_project_skills_and_rejects_content_tampering() {
    let project = isolated_project("project-skill");
    fs::create_dir_all(project.join("skills")).expect("create Skill directory");
    let config_path = project.join("y-harness.json");
    let package_path = project.join("skills/concise-assistant.skill.json");
    fs::write(
        &config_path,
        include_bytes!("../config/y-harness.skill.example.json"),
    )
    .expect("write Skill config");
    fs::write(
        &package_path,
        include_bytes!("../examples/skills/concise-assistant.skill.json"),
    )
    .expect("write Skill package");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("run Skill doctor");
    assert!(
        doctor.status.success(),
        "Skill doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report = String::from_utf8(doctor.stdout).expect("UTF-8 doctor report");
    assert!(report.contains("skills: 1"));
    assert!(report.contains("skill lock: concise-assistant@1.0.0 0ddd1d0a"));
    assert!(report.contains("status: ok"));

    let tampered = fs::read_to_string(&package_path)
        .expect("read Skill")
        .replace("Answer clearly", "Answer vaguely");
    fs::write(&package_path, tampered).expect("tamper Skill package");
    let rejected = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("run tampered Skill doctor");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("digest"));

    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn skill_cli_installs_verifies_and_recoverably_removes_exact_packages() {
    let project = isolated_project("skill-lifecycle");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let config_path = project.join("y-harness.json");
    let default_config = fs::read(&config_path).expect("read default config");
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/skills/concise-assistant.skill.json");
    let digest = "0ddd1d0af09a2ea5bc0166943f62b579eb3335a9c3919824ecf43864881fa460";
    let installed_path = project.join(format!("skills/{digest}.skill.json"));

    let installed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("skill")
        .arg("install")
        .arg(&source)
        .arg(&config_path)
        .output()
        .expect("install Skill");
    assert!(
        installed.status.success(),
        "Skill install failed: {}",
        String::from_utf8_lossy(&installed.stderr)
    );
    let report = String::from_utf8(installed.stdout).expect("UTF-8 install report");
    assert!(report.contains("skill lock: concise-assistant@1.0.0 0ddd1d0a"));
    assert!(report.contains("activation required:"));
    assert!(installed_path.is_file());

    let repeated = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "install"])
        .arg(&source)
        .arg(&config_path)
        .output()
        .expect("repeat Skill install");
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("already installed:"));

    let listed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "list"])
        .arg(&config_path)
        .output()
        .expect("list Skills");
    assert!(listed.status.success());
    let report = String::from_utf8(listed.stdout).expect("UTF-8 Skill list");
    assert!(report.contains("installed skills: 1"));
    assert!(report.contains("skill: concise-assistant@1.0.0 0ddd1d0a"));

    let verified = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "verify"])
        .arg(&config_path)
        .output()
        .expect("verify Skills");
    assert!(verified.status.success());
    let report = String::from_utf8(verified.stdout).expect("UTF-8 Skill verification");
    assert!(report.contains("verified skills: 1"));
    assert!(report.contains("status: ok"));

    fs::write(
        &config_path,
        format!(
            r#"{{
              "schema_version": 1,
              "data_directory": ".y-harness",
              "model": {{"type": "demo"}},
              "skills": {{
                "package_files": ["skills/{digest}.skill.json"],
                "activate": [{{"name": "concise-assistant", "version": "1.0.0"}}],
                "budget_tokens": 256
              }}
            }}"#
        ),
    )
    .expect("configure installed Skill");
    let active_removal = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "remove", "concise-assistant@1.0.0"])
        .arg(&config_path)
        .output()
        .expect("reject active Skill removal");
    assert!(!active_removal.status.success());
    assert!(String::from_utf8_lossy(&active_removal.stderr).contains("active Skill"));
    assert!(installed_path.is_file());

    fs::write(&config_path, default_config).expect("restore default config");
    let removed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "remove", "concise-assistant@1.0.0"])
        .arg(&config_path)
        .output()
        .expect("remove Skill");
    assert!(
        removed.status.success(),
        "Skill removal failed: {}",
        String::from_utf8_lossy(&removed.stderr)
    );
    let report = String::from_utf8(removed.stdout).expect("UTF-8 removal report");
    assert!(report.contains("recoverable:"));
    assert!(!installed_path.exists());
    assert_eq!(
        fs::read_dir(project.join(".y-harness/skill-trash"))
            .expect("read Skill trash")
            .count(),
        1
    );

    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn signed_skill_cli_preserves_external_trust_and_live_revocation() {
    let project = isolated_project("external-skill-lifecycle");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());
    let config_path = project.join("y-harness.json");
    let source_path = project.join("downloaded.signed-skill.json");
    let package: SkillPackage = serde_json::from_slice(include_bytes!(
        "../examples/skills/concise-assistant.skill.json"
    ))
    .expect("Skill fixture");
    let signing_key = SigningKey::from_bytes(&[7_u8; 32]);
    let signature = signing_key.sign(
        &package
            .publisher_signing_bytes()
            .expect("publisher signing bytes"),
    );
    let mut signed = SignedSkillPackage {
        package: package.clone(),
        signature: SkillSignature {
            key_id: "test-publisher".to_owned(),
            ed25519: signature.to_bytes().to_vec(),
        },
        transparency: None,
    };
    fs::write(
        &source_path,
        serde_json::to_vec_pretty(&signed).expect("encode signed Skill"),
    )
    .expect("write signed Skill");
    let public_key = lower_hex(&signing_key.verifying_key().to_bytes());
    let log_key = SigningKey::from_bytes(&[8_u8; 32]);
    let log_public_key = lower_hex(&log_key.verifying_key().to_bytes());
    let trust_config = format!(
        r#"{{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "model": {{"type": "demo"}},
          "skills": {{
            "package_files": [],
            "external_package_files": [],
            "activate": [],
            "trust": {{
              "publishers": [{{
                "key_id": "test-publisher",
                "public_key_hex": "{public_key}",
                "transparency": "required"
              }}],
              "transparency_logs": [{{
                "log_id": "test-log",
                "public_key_hex": "{log_public_key}"
              }}]
            }}
          }}
        }}"#
    );
    fs::write(&config_path, &trust_config).expect("configure publisher trust");
    let installed_path = project.join(format!(
        "skills/{}.signed-skill.json",
        package.content_sha256
    ));

    let rejected = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "install-external"])
        .arg(&source_path)
        .arg(&config_path)
        .output()
        .expect("reject unsigned transparency");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("requires a transparency receipt"));
    assert!(!installed_path.exists());

    let integrated_at_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_millis(),
    )
    .expect("millisecond timestamp");
    signed.transparency = Some(SkillTransparencyReceipt {
        log_id: "test-log".to_owned(),
        entry_id: "entry-1".to_owned(),
        integrated_at_ms,
        ed25519: Vec::new(),
    });
    let log_signature = log_key.sign(
        &signed
            .transparency_signing_bytes()
            .expect("transparency signing bytes"),
    );
    signed
        .transparency
        .as_mut()
        .expect("transparency receipt")
        .ed25519 = log_signature.to_bytes().to_vec();
    fs::write(
        &source_path,
        serde_json::to_vec_pretty(&signed).expect("encode transparent Skill"),
    )
    .expect("write transparent Skill");

    let installed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "install-external"])
        .arg(&source_path)
        .arg(&config_path)
        .output()
        .expect("install signed Skill");
    assert!(
        installed.status.success(),
        "signed Skill install failed: {}",
        String::from_utf8_lossy(&installed.stderr)
    );
    assert!(installed_path.is_file());
    let report = String::from_utf8(installed.stdout).expect("UTF-8 install report");
    assert!(report.contains("external"));
    assert!(report.contains("skills.external_package_files"));

    let repeated = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "install-external"])
        .arg(&source_path)
        .arg(&config_path)
        .output()
        .expect("repeat signed Skill install");
    assert!(repeated.status.success());
    assert!(String::from_utf8_lossy(&repeated.stdout).contains("already installed:"));

    for command in ["list", "verify"] {
        let output = Command::new(env!("CARGO_BIN_EXE_yh"))
            .args(["skill", command])
            .arg(&config_path)
            .output()
            .expect("inspect signed Skills");
        assert!(
            output.status.success(),
            "signed Skill {command} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("external"));
    }

    let active = format!(
        r#"{{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "model": {{"type": "demo"}},
          "skills": {{
            "package_files": [],
            "external_package_files": ["skills/{}.signed-skill.json"],
            "activate": [{{"name": "concise-assistant", "version": "1.0.0"}}],
            "budget_tokens": 256,
            "trust": {{
              "publishers": [{{
                "key_id": "test-publisher",
                "public_key_hex": "{public_key}",
                "transparency": "required"
              }}],
              "transparency_logs": [{{
                "log_id": "test-log",
                "public_key_hex": "{log_public_key}"
              }}]
            }}
          }}
        }}"#,
        package.content_sha256
    );
    fs::write(&config_path, &active).expect("activate external Skill");
    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .output()
        .expect("doctor external Skill");
    assert!(
        doctor.status.success(),
        "external Skill doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert!(String::from_utf8_lossy(&doctor.stdout).contains("skills: 1"));
    assert!(String::from_utf8_lossy(&doctor.stdout).contains("publisher=test-publisher"));
    assert!(String::from_utf8_lossy(&doctor.stdout).contains("transparency=test-log@entry-1"));

    let active_removal = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "remove", "concise-assistant@1.0.0"])
        .arg(&config_path)
        .output()
        .expect("reject active external Skill removal");
    assert!(!active_removal.status.success());
    assert!(String::from_utf8_lossy(&active_removal.stderr).contains("active Skill"));

    let revoked = trust_config.replace(
        r#""public_key_hex": ""#,
        r#""revocation": {"revoked_at_ms": 1, "reason_code": "compromised"},
                "public_key_hex": ""#,
    );
    fs::write(&config_path, &revoked).expect("revoke publisher");
    let listed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "list"])
        .arg(&config_path)
        .output()
        .expect("reject revoked signed Skill");
    assert!(!listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stderr).contains("revoked"));

    let removed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["skill", "remove", "concise-assistant@1.0.0"])
        .arg(&config_path)
        .output()
        .expect("remove revoked signed Skill");
    assert!(
        removed.status.success(),
        "revoked signed Skill removal failed: {}",
        String::from_utf8_lossy(&removed.stderr)
    );
    assert!(!installed_path.exists());
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[cfg(not(feature = "https-skill"))]
#[test]
fn https_skill_install_reports_the_missing_optional_feature() {
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args([
            "skill",
            "install-https",
            "https://example.test/skill.json",
            "example@1.0.0",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ])
        .output()
        .expect("run unavailable HTTPS Skill install");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("`https-skill` Cargo feature"));
}

#[cfg(not(feature = "https-mcp"))]
#[test]
fn enabled_https_mcp_reports_the_missing_optional_feature_before_secret_access() {
    let project = isolated_project("https-mcp-feature");
    fs::create_dir_all(&project).expect("create HTTPS MCP project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        r#"{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "model": {"type": "demo"},
          "https_mcp_servers": [{
            "id": "remote",
            "endpoint": "https://example.test/mcp",
            "bearer_secret_reference": "mcp/remote",
            "bearer_environment": "MISSING_MCP_SECRET"
          }]
        }"#,
    )
    .expect("write HTTPS MCP config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("run unavailable HTTPS MCP");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("`--features https-mcp`"));
    assert!(!project.join(".y-harness").exists());
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn disabled_https_mcp_acquires_no_feature_secret_or_network_authority() {
    let project = isolated_project("disabled-https-mcp");
    fs::create_dir_all(&project).expect("create disabled HTTPS MCP project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        r#"{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "model": {"type": "demo"},
          "https_mcp_servers": [{
            "id": "remote",
            "enabled": false,
            "endpoint": "https://unreachable.invalid/mcp",
            "bearer_secret_reference": "mcp/remote",
            "bearer_environment": "MISSING_MCP_SECRET"
          }]
        }"#,
    )
    .expect("write disabled HTTPS MCP config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose disabled HTTPS MCP");
    assert!(
        output.status.success(),
        "disabled HTTPS MCP acquired authority: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = String::from_utf8(output.stdout).expect("UTF-8 doctor report");
    assert!(report.contains("mcp servers: 0 enabled / 1 configured"));
    assert!(report.contains("mcp command locks: 0 / 0 stdio enabled"));
    assert!(report.contains("status: ok"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[cfg(feature = "https-mcp")]
#[test]
fn invalid_https_mcp_endpoint_is_rejected_before_secret_access() {
    let project = isolated_project("invalid-https-mcp");
    fs::create_dir_all(&project).expect("create invalid HTTPS MCP project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        r#"{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "model": {"type": "demo"},
          "https_mcp_servers": [{
            "id": "remote",
            "endpoint": "http://example.test/mcp",
            "bearer_secret_reference": "mcp/remote",
            "bearer_environment": "MISSING_MCP_SECRET"
          }]
        }"#,
    )
    .expect("write invalid HTTPS MCP config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose invalid HTTPS MCP");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 doctor error");
    assert!(error.contains("MCP endpoint must use HTTPS"));
    assert!(!error.contains("MISSING_MCP_SECRET"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn invalid_json_command_model_is_rejected_before_environment_access() {
    let project = isolated_project("invalid-json-command-model");
    fs::create_dir_all(&project).expect("create invalid JSON command Model project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {
                "type": "json_command",
                "id": "external/invalid",
                "process": {
                    "command": env!("CARGO_BIN_EXE_yh"),
                    "current_directory": ".",
                    "environment_from_host": {
                        "PROVIDER_API_KEY": "MISSING_JSON_MODEL_SECRET"
                    },
                    "timeout_ms": 0,
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }
        }))
        .expect("encode invalid JSON command Model config"),
    )
    .expect("write invalid JSON command Model config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose invalid JSON command Model");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 doctor error");
    assert!(error.contains("process timeout must be"));
    assert!(!error.contains("MISSING_JSON_MODEL_SECRET"));
    fs::remove_dir_all(project).expect("remove invalid JSON command Model project");
}

#[test]
fn invalid_json_command_compactor_is_rejected_before_environment_access() {
    let project = isolated_project("invalid-json-command-compactor");
    fs::create_dir_all(&project).expect("create invalid JSON command compactor project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {"type": "demo"},
            "conversation": {
                "max_turns": 1,
                "compaction": {
                    "name": "external.fixture-summary",
                    "description": "Fixture summary command",
                    "process": {
                        "command": env!("CARGO_BIN_EXE_yh"),
                        "current_directory": ".",
                        "environment_from_host": {
                            "PROVIDER_API_KEY": "MISSING_COMPACTOR_SECRET"
                        },
                        "timeout_ms": 0,
                        "launch": {
                            "type": "unrestricted",
                            "max_concurrency": 1
                        }
                    }
                }
            }
        }))
        .expect("encode invalid JSON command compactor config"),
    )
    .expect("write invalid JSON command compactor config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose invalid JSON command compactor");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 doctor error");
    assert!(error.contains("process timeout must be"));
    assert!(!error.contains("MISSING_COMPACTOR_SECRET"));
    fs::remove_dir_all(project).expect("remove invalid JSON command compactor project");
}

#[test]
fn invalid_json_command_verifier_is_rejected_before_environment_access() {
    let project = isolated_project("invalid-json-command-verifier");
    fs::create_dir_all(&project).expect("create invalid JSON command verifier project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {"type": "demo"},
            "verifiers": [{
                "name": "external.fixture-verifier",
                "description": "Fixture completion verifier",
                "process": {
                    "command": env!("CARGO_BIN_EXE_yh"),
                    "current_directory": ".",
                    "environment_from_host": {
                        "VERIFIER_API_KEY": "MISSING_VERIFIER_SECRET"
                    },
                    "timeout_ms": 0,
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }]
        }))
        .expect("encode invalid JSON command verifier config"),
    )
    .expect("write invalid JSON command verifier config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose invalid JSON command verifier");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 doctor error");
    assert!(error.contains("process timeout must be"));
    assert!(!error.contains("MISSING_VERIFIER_SECRET"));
    fs::remove_dir_all(project).expect("remove invalid JSON command verifier project");
}

#[test]
fn invalid_json_command_grader_is_rejected_before_environment_access() {
    let project = isolated_project("invalid-json-command-grader");
    fs::create_dir_all(&project).expect("create invalid JSON command grader project");
    let config = project.join("y-harness.json");
    fs::write(
        &config,
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {"type": "demo"},
            "evaluation": {
                "grader_timeout_ms": 0,
                "graders": [{
                    "name": "external.fixture-grader",
                    "description": "Fixture Evaluation Grader",
                    "process": {
                        "command": env!("CARGO_BIN_EXE_yh"),
                        "current_directory": ".",
                        "environment_from_host": {
                            "GRADER_API_KEY": "MISSING_GRADER_SECRET"
                        },
                        "launch": {
                            "type": "unrestricted",
                            "max_concurrency": 1
                        }
                    }
                }]
            }
        }))
        .expect("encode invalid JSON command grader config"),
    )
    .expect("write invalid JSON command grader config");
    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config)
        .output()
        .expect("diagnose invalid JSON command grader");
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("UTF-8 doctor error");
    assert!(error.contains("grader timeout must be"));
    assert!(!error.contains("MISSING_GRADER_SECRET"));
    fs::remove_dir_all(project).expect("remove invalid JSON command grader project");
}

#[cfg(feature = "https-model")]
#[test]
fn doctor_accepts_the_checked_in_https_gateway_template() {
    let project = isolated_project("https-template");
    fs::create_dir_all(&project).expect("create HTTPS template project");
    let config_path = project.join("y-harness.json");
    let mut config: serde_json::Value =
        serde_json::from_slice(include_bytes!("../config/y-harness.https.example.json"))
            .expect("decode checked-in HTTPS template");
    config["authority"] = serde_json::json!({
        "type": "local_process_tenant",
        "tenant_id": "tenant-https"
    });
    fs::write(
        &config_path,
        serde_json::to_vec_pretty(&config).expect("encode tenant HTTPS template"),
    )
    .expect("write checked-in HTTPS template");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .env("YH_MODEL_TOKEN", "doctor-placeholder")
        .output()
        .expect("run HTTPS template doctor");
    assert!(
        doctor.status.success(),
        "HTTPS template doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report = String::from_utf8(doctor.stdout).expect("UTF-8 doctor report");
    assert!(report.contains("model: gateway/default"));
    assert!(report.contains("authority: local-process / tenant-https"));
    assert!(report.contains("status: ok"));
    assert!(!report.contains("doctor-placeholder"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[cfg(feature = "https-model")]
#[test]
fn doctor_validates_an_explicit_ordered_model_catalog() {
    let project = isolated_project("model-route");
    fs::create_dir_all(&project).expect("create Model route project");
    let config_path = project.join("y-harness.json");
    fs::write(
        &config_path,
        r#"{
          "schema_version": 1,
          "data_directory": ".y-harness",
          "models": [
            {
              "type": "https_json_gateway",
              "id": "gateway/primary",
              "endpoint": "https://primary.example.com/v1/complete",
              "bearer_secret_reference": "gateway/primary",
              "bearer_environment": "YH_MODEL_TOKEN"
            },
            {
              "type": "https_json_gateway",
              "id": "gateway/fallback",
              "endpoint": "https://fallback.example.com/v1/complete",
              "bearer_secret_reference": "gateway/fallback",
              "bearer_environment": "YH_MODEL_TOKEN"
            }
          ],
          "model_route": {
            "models": ["gateway/primary", "gateway/fallback"],
            "attempt_timeout_ms": 25000,
            "timeout_cooldown_ms": 45000,
            "retry": {
              "max_retries": 3,
              "initial_delay_ms": 125,
              "max_delay_ms": 4000
            }
          }
        }"#,
    )
    .expect("write Model route config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("doctor")
        .arg(&config_path)
        .env("YH_MODEL_TOKEN", "doctor-placeholder")
        .output()
        .expect("run Model route doctor");
    assert!(
        doctor.status.success(),
        "Model route doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report = String::from_utf8(doctor.stdout).expect("UTF-8 doctor report");
    assert!(report.contains("models: 2"));
    assert!(report.contains("model route: gateway/primary -> gateway/fallback"));
    assert!(report.contains("model timeout cooldown: 45000 ms"));
    assert!(report.contains("model retries: 3 (125-4000 ms)"));
    assert!(report.contains("status: ok"));
    assert!(!report.contains("doctor-placeholder"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[cfg(unix)]
#[test]
fn configured_json_command_model_runs_a_real_service_turn() {
    use std::os::unix::fs::PermissionsExt;

    let project = isolated_project("json-command-model");
    fs::create_dir_all(&project).expect("create JSON command Model project");
    let adapter = project.join("model-adapter");
    fs::write(
        &adapter,
        br#"#!/bin/sh
cat >/dev/null
printf '%s' '{"type":"message","content":"configured command model"}'
"#,
    )
    .expect("write model adapter");
    let mut permissions = fs::metadata(&adapter)
        .expect("model adapter metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&adapter, permissions).expect("make model adapter executable");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {
                "type": "json_command",
                "id": "external/fixture",
                "process": {
                    "command": adapter,
                    "current_directory": ".",
                    "timeout_ms": 5_000,
                    "max_output_bytes": 1_048_576,
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }
        }))
        .expect("encode JSON command Model config"),
    )
    .expect("write JSON command Model config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JSON command Model service");
    let mut input = child.stdin.take().expect("service stdin");
    let mut output = BufReader::new(child.stdout.take().expect("service stdout"));

    let initialized = exchange(
        &mut input,
        &mut output,
        request("initialize", ProtocolCommand::Initialize {}),
    );
    assert!(matches!(
        initialized.body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized { .. }
        }
    ));
    let created = exchange(
        &mut input,
        &mut output,
        request("create-thread", ProtocolCommand::CreateThread {}),
    );
    let thread_id = match created.body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => thread.id,
        other => panic!("unexpected create Thread response: {other:?}"),
    };
    let final_text = run_turn_to_completion(
        &mut input,
        &mut output,
        &thread_id,
        "use configured adapter",
        "command-model-turn",
    );
    assert_eq!(final_text, "configured command model");

    drop(input);
    let settled = child.wait_with_output().expect("settle service");
    assert!(
        settled.status.success(),
        "JSON command Model service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build provenance test Runtime");
    let thread = runtime.block_on(async {
        StateEngine::new(Arc::new(
            SqliteEventStore::open(project.join(".y-harness/state.db"))
                .await
                .expect("open JSON command Model State"),
        ))
        .load_thread(&thread_id)
        .await
        .expect("load JSON command Model Thread")
        .expect("persisted JSON command Model Thread")
    });
    assert!(
        thread
            .turns
            .iter()
            .flat_map(|turn| &turn.items)
            .any(|item| {
                matches!(
                    &item.kind,
                    ItemKind::AssistantMessage {
                        model_id: Some(model_id),
                        model_origin:
                            Some(CapabilityOrigin::External {
                                id: origin_id
                            }),
                        content,
                    } if model_id == "external/fixture"
                        && origin_id == "json-command-model/external/fixture"
                        && content == "configured command model"
                )
            })
    );
    fs::remove_dir_all(project).expect("remove JSON command Model project");
}

#[cfg(unix)]
#[test]
fn configured_json_model_settlement_drives_typed_runtime_retry() {
    use std::os::unix::fs::PermissionsExt;

    let project = isolated_project("json-command-model-settlement");
    fs::create_dir_all(&project).expect("create settlement Model project");
    let adapter = project.join("model-adapter");
    fs::write(
        &adapter,
        br#"#!/bin/sh
cat >/dev/null
attempt=1
if [ -f model-attempts ]; then
  previous=$(cat model-attempts)
  attempt=$((previous + 1))
fi
printf '%s' "$attempt" > model-attempts
if [ "$attempt" -eq 1 ]; then
  printf '%s' '{"status":"failed","kind":"rate_limited","message":"fixture rate limit","http_status":429,"retry_after_ms":1}'
else
  printf '%s' '{"status":"completed","output":{"type":"message","content":"settlement retry completed"},"usage":{"input_tokens":9,"output_tokens":4,"cached_input_tokens":0,"reasoning_tokens":0,"cost_usd_ticks":7},"provider_model":"provider/fixture-v2","provider_request_id":"fixture-request-2"}'
fi
"#,
    )
    .expect("write settlement Model adapter");
    let mut permissions = fs::metadata(&adapter)
        .expect("settlement Model adapter metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&adapter, permissions).expect("make settlement Model adapter executable");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "models": [{
                "type": "json_command",
                "id": "external/settlement-fixture",
                "protocol": "settlement_v1",
                "process": {
                    "command": adapter,
                    "current_directory": ".",
                    "timeout_ms": 5_000,
                    "max_output_bytes": 1_048_576,
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }],
            "model_route": {
                "models": ["external/settlement-fixture"],
                "attempt_timeout_ms": 5_000,
                "retry": {
                    "max_retries": 1,
                    "initial_delay_ms": 1,
                    "max_delay_ms": 1
                }
            }
        }))
        .expect("encode settlement Model config"),
    )
    .expect("write settlement Model config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn settlement Model service");
    let mut input = child.stdin.take().expect("service stdin");
    let mut output = BufReader::new(child.stdout.take().expect("service stdout"));
    let initialized = exchange(
        &mut input,
        &mut output,
        request("initialize", ProtocolCommand::Initialize {}),
    );
    assert!(matches!(
        initialized.body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized { .. }
        }
    ));
    let created = exchange(
        &mut input,
        &mut output,
        request("create-thread", ProtocolCommand::CreateThread {}),
    );
    let thread_id = match created.body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => thread.id,
        other => panic!("unexpected create Thread response: {other:?}"),
    };
    let final_text = run_turn_to_completion(
        &mut input,
        &mut output,
        &thread_id,
        "exercise typed Provider retry",
        "settlement-model-turn",
    );
    assert_eq!(final_text, "settlement retry completed");

    drop(input);
    let settled = child.wait_with_output().expect("settle Model service");
    assert!(
        settled.status.success(),
        "settlement Model service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    assert_eq!(
        fs::read_to_string(project.join("model-attempts")).expect("read Model attempt count"),
        "2"
    );
    fs::remove_dir_all(project).expect("remove settlement Model project");
}

#[cfg(unix)]
#[test]
fn configured_json_command_compactor_records_real_service_summary_evidence() {
    use std::os::unix::fs::PermissionsExt;

    let project = isolated_project("json-command-compactor");
    fs::create_dir_all(&project).expect("create JSON command compactor project");
    let adapter = project.join("conversation-compactor");
    fs::write(
        &adapter,
        br#"#!/bin/sh
cat >/dev/null
printf '%s' '{"summary":"configured semantic summary"}'
"#,
    )
    .expect("write conversation compactor");
    let mut permissions = fs::metadata(&adapter)
        .expect("conversation compactor metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&adapter, permissions).expect("make conversation compactor executable");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {"type": "demo"},
            "conversation": {
                "max_turns": 1,
                "budget_tokens": 65_536,
                "budget_bytes": 65_536,
                "compaction": {
                    "name": "external.fixture-summary",
                    "description": "Fixture semantic summary command",
                    "max_input_turns": 2,
                    "input_budget_bytes": 65_536,
                    "output_budget_tokens": 1_024,
                    "output_budget_bytes": 4_096,
                    "process": {
                        "command": adapter,
                        "current_directory": ".",
                        "timeout_ms": 5_000,
                        "max_output_bytes": 4_096,
                        "launch": {
                            "type": "unrestricted",
                            "max_concurrency": 1
                        }
                    }
                }
            }
        }))
        .expect("encode JSON command compactor config"),
    )
    .expect("write JSON command compactor config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["doctor", "y-harness.json"])
        .current_dir(&project)
        .output()
        .expect("diagnose JSON command compactor service");
    assert!(
        doctor.status.success(),
        "JSON command compactor doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    let report = String::from_utf8(doctor.stdout).expect("UTF-8 compactor doctor report");
    assert!(report.contains("conversation: 1 Turns / 65536 tokens / 65536 bytes"));
    assert!(report.contains("conversation compactor: external.fixture-summary"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JSON command compactor service");
    let mut input = child.stdin.take().expect("service stdin");
    let mut output = BufReader::new(child.stdout.take().expect("service stdout"));

    let initialized = exchange(
        &mut input,
        &mut output,
        request("initialize", ProtocolCommand::Initialize {}),
    );
    assert!(matches!(
        initialized.body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized { .. }
        }
    ));
    let created = exchange(
        &mut input,
        &mut output,
        request("create-thread", ProtocolCommand::CreateThread {}),
    );
    let thread_id = match created.body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => thread.id,
        other => panic!("unexpected create Thread response: {other:?}"),
    };
    for (index, prompt) in ["first", "second", "third"].into_iter().enumerate() {
        run_turn_to_completion(
            &mut input,
            &mut output,
            &thread_id,
            prompt,
            &format!("compaction-turn-{index}"),
        );
    }

    drop(input);
    let settled = child.wait_with_output().expect("settle service");
    assert!(
        settled.status.success(),
        "JSON command compactor service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build compactor provenance Runtime");
    let thread = runtime.block_on(async {
        StateEngine::new(Arc::new(
            SqliteEventStore::open(project.join(".y-harness/state.db"))
                .await
                .expect("open JSON command compactor State"),
        ))
        .load_thread(&thread_id)
        .await
        .expect("load JSON command compactor Thread")
        .expect("persisted JSON command compactor Thread")
    });
    assert_eq!(thread.turns.len(), 3);
    assert!(thread.turns[0].items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::UserMessage { content } if content == "first"
        )
    }));
    assert!(thread.turns[2].items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::ConversationSummary {
                compactor,
                covered_turns,
                older_omitted_turns: 0,
                source_sha256,
                content_sha256,
                estimated_tokens,
                serialized_bytes,
            } if compactor == "external.fixture-summary"
                && covered_turns == &[thread.turns[0].id.clone()]
                && source_sha256.len() == 64
                && content_sha256.len() == 64
                && *estimated_tokens > 0
                && *serialized_bytes > 0
        )
    }));
    fs::remove_dir_all(project).expect("remove JSON command compactor project");
}

#[cfg(unix)]
#[test]
fn configured_json_command_verifier_gates_a_real_service_turn() {
    use std::os::unix::fs::PermissionsExt;

    let project = isolated_project("json-command-verifier");
    fs::create_dir_all(&project).expect("create JSON command verifier project");
    let adapter = project.join("completion-verifier");
    fs::write(
        &adapter,
        br#"#!/bin/sh
cat >/dev/null
printf '%s' '{"status":"passed","summary":"configured verification passed"}'
"#,
    )
    .expect("write completion verifier");
    let mut permissions = fs::metadata(&adapter)
        .expect("completion verifier metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&adapter, permissions).expect("make completion verifier executable");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "model": {"type": "demo"},
            "verifiers": [{
                "name": "external.fixture-verifier",
                "description": "Fixture completion verifier",
                "process": {
                    "command": adapter,
                    "current_directory": ".",
                    "timeout_ms": 5_000,
                    "max_output_bytes": 4_096,
                    "launch": {
                        "type": "unrestricted",
                        "max_concurrency": 1
                    }
                }
            }]
        }))
        .expect("encode JSON command verifier config"),
    )
    .expect("write JSON command verifier config");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["doctor", "y-harness.json"])
        .current_dir(&project)
        .output()
        .expect("diagnose JSON command verifier service");
    assert!(
        doctor.status.success(),
        "JSON command verifier doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert!(
        String::from_utf8(doctor.stdout)
            .expect("UTF-8 verifier doctor report")
            .contains("verifiers: 1")
    );

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["serve", "y-harness.json"])
        .current_dir(&project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn JSON command verifier service");
    let mut input = child.stdin.take().expect("service stdin");
    let mut output = BufReader::new(child.stdout.take().expect("service stdout"));
    let initialized = exchange(
        &mut input,
        &mut output,
        request("initialize", ProtocolCommand::Initialize {}),
    );
    assert!(matches!(
        initialized.body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized { .. }
        }
    ));
    let created = exchange(
        &mut input,
        &mut output,
        request("create-thread", ProtocolCommand::CreateThread {}),
    );
    let thread_id = match created.body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => thread.id,
        other => panic!("unexpected create Thread response: {other:?}"),
    };
    run_turn_to_completion(
        &mut input,
        &mut output,
        &thread_id,
        "verify this candidate",
        "verifier-turn",
    );

    drop(input);
    let settled = child.wait_with_output().expect("settle service");
    assert!(
        settled.status.success(),
        "JSON command verifier service failed: {}",
        String::from_utf8_lossy(&settled.stderr)
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build verifier evidence Runtime");
    let thread = runtime.block_on(async {
        StateEngine::new(Arc::new(
            SqliteEventStore::open(project.join(".y-harness/state.db"))
                .await
                .expect("open JSON command verifier State"),
        ))
        .load_thread(&thread_id)
        .await
        .expect("load JSON command verifier Thread")
        .expect("persisted JSON command verifier Thread")
    });
    assert!(thread.turns[0].items.iter().any(|item| {
        matches!(
            &item.kind,
            ItemKind::VerificationResult {
                verifier,
                outcome: y_harness::VerificationOutcome::Passed {
                    summary: Some(summary)
                }
            } if verifier == "external.fixture-verifier"
                && summary == "configured verification passed"
        )
    }));
    fs::remove_dir_all(project).expect("remove JSON command verifier project");
}

#[cfg(unix)]
#[test]
fn configured_json_command_grader_runs_an_isolated_real_evaluation() {
    use std::os::unix::fs::PermissionsExt;

    let project = isolated_project("json-command-grader");
    fs::create_dir_all(&project).expect("create JSON command grader project");
    let adapter = project.join("evaluation-grader");
    fs::write(
        &adapter,
        br#"#!/bin/sh
cat >/dev/null
printf '%s' '{"score":1.0,"passed":true,"rationale":"configured grade passed"}'
"#,
    )
    .expect("write Evaluation Grader");
    let mut permissions = fs::metadata(&adapter)
        .expect("Evaluation Grader metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(&adapter, permissions).expect("make Evaluation Grader executable");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "authority": {
                "type": "local_process_tenant",
                "tenant_id": "tenant-eval"
            },
            "model": {"type": "demo"},
            "evaluation": {
                "case_concurrency": 2,
                "grader_concurrency": 2,
                "default_case_timeout_ms": 5_000,
                "grader_timeout_ms": 5_000,
                "graders": [{
                    "name": "external.fixture-grader",
                    "description": "Fixture Evaluation Grader",
                    "process": {
                        "command": adapter,
                        "current_directory": ".",
                        "timeout_ms": 5_000,
                        "max_output_bytes": 4_096,
                        "launch": {
                            "type": "unrestricted",
                            "max_concurrency": 1
                        }
                    }
                }]
            }
        }))
        .expect("encode JSON command grader config"),
    )
    .expect("write JSON command grader config");
    fs::write(
        project.join("suite.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 2,
            "name": "configured-grader-test",
            "cases": [{
                "id": "configured-case",
                "prompt": "grade this candidate",
                "memory_scope": {
                    "project": "configured-grader-test",
                    "tenant_id": "tenant-eval",
                    "tags": ["isolated"]
                },
                "timeout_ms": 5_000,
                "metadata": {"criterion": "must complete"}
            }]
        }))
        .expect("encode configured Evaluation suite"),
    )
    .expect("write configured Evaluation suite");
    fs::write(
        project.join("baseline.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 2,
            "requirements": [{
                "case_id": "configured-case",
                "grader": "external.fixture-grader",
                "grader_origin": {
                    "kind": "external",
                    "id": "json-command-grader/external.fixture-grader"
                },
                "minimum_score": 1.0,
                "must_pass": true
            }]
        }))
        .expect("encode configured Evaluation baseline"),
    )
    .expect("write configured Evaluation baseline");

    let doctor = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["doctor", "y-harness.json"])
        .current_dir(&project)
        .output()
        .expect("diagnose JSON command grader");
    assert!(
        doctor.status.success(),
        "JSON command grader doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );
    assert!({
        let report = String::from_utf8(doctor.stdout).expect("UTF-8 grader doctor report");
        report.contains("evaluation graders: 1")
            && report.contains("authority: local-process / tenant-eval")
    });

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["eval", "suite.json", "baseline.json", "y-harness.json"])
        .current_dir(&project)
        .output()
        .expect("run configured Evaluation");
    assert!(
        output.status.success(),
        "configured Evaluation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("configured Evaluation JSON");
    assert_eq!(
        report
            .pointer("/comparison/passed")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        report
            .pointer("/report/cases/0/grades/0/grader_origin/id")
            .and_then(serde_json::Value::as_str),
        Some("json-command-grader/external.fixture-grader")
    );
    assert_eq!(
        report
            .pointer("/report/cases/0/grades/0/outcome/rationale")
            .and_then(serde_json::Value::as_str),
        Some("configured grade passed")
    );
    fs::write(
        project.join("regressed-baseline.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format_version": 2,
            "requirements": [{
                "case_id": "configured-case",
                "grader": "missing.required-grader",
                "grader_origin": {
                    "kind": "external",
                    "id": "json-command-grader/missing.required-grader"
                },
                "minimum_score": 1.0,
                "must_pass": true
            }]
        }))
        .expect("encode regressed Evaluation baseline"),
    )
    .expect("write regressed Evaluation baseline");
    let regressed = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args([
            "eval",
            "suite.json",
            "regressed-baseline.json",
            "y-harness.json",
        ])
        .current_dir(&project)
        .output()
        .expect("run regressed configured Evaluation");
    assert!(!regressed.status.success());
    let regressed_report: serde_json::Value =
        serde_json::from_slice(&regressed.stdout).expect("regressed Evaluation JSON");
    assert_eq!(
        regressed_report
            .pointer("/comparison/passed")
            .and_then(serde_json::Value::as_bool),
        Some(false)
    );
    assert!(
        !project.join(".y-harness").exists(),
        "configured Evaluation must not open persistent service State"
    );
    fs::remove_dir_all(project).expect("remove JSON command grader project");
}

#[test]
fn persistent_service_recovers_threads_tasks_workflows_handoffs_and_effects_after_restart() {
    let project = isolated_project("persistence");
    let initialized = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("init")
        .arg(&project)
        .output()
        .expect("run init");
    assert!(initialized.status.success());

    let first = serve(
        &project,
        vec![
            request("initialize", ProtocolCommand::Initialize {}),
            request("create-thread", ProtocolCommand::CreateThread {}),
            request(
                "create-graph",
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "persistent-graph".to_owned(),
                    definitions: vec![TaskDefinition {
                        id: TaskId::from_static("persistent-task"),
                        description: "survive service restart".to_owned(),
                        dependencies: Default::default(),
                        priority: 0,
                        workspace: WorkspaceMode::None,
                        required_capabilities: Default::default(),
                    }],
                },
            ),
            request(
                "create-workflow",
                ProtocolCommand::CreateWorkflowRun {
                    run_id: "persistent-workflow".to_owned(),
                    request: WorkflowCreateRequest {
                        command_id: WorkflowCommandId::from_static("create-persistent-workflow"),
                        definition: WorkflowDefinition {
                            name: "persistent.workflow".to_owned(),
                            version: Version::new(1, 0, 0),
                            content_sha256: "a".repeat(64),
                        },
                        task_graph_id: TaskGraphId::from_static("persistent-graph"),
                    },
                },
            ),
            request(
                "create-human-handoff",
                ProtocolCommand::CreateHumanHandoff {
                    handoff_id: "persistent-handoff".to_owned(),
                    request: HumanHandoffCreateRequest {
                        command_id: HumanHandoffCommandId::from_static("create-persistent-handoff"),
                        subject: HumanHandoffSubject::WorkflowRun {
                            run_id: WorkflowRunId::from_static("persistent-workflow"),
                        },
                        queue: "support.primary".to_owned(),
                        reason_code: "agent.escalation".to_owned(),
                        priority: 7,
                    },
                },
            ),
            request(
                "create-effect",
                ProtocolCommand::CreateEffect {
                    effect_id: "persistent-effect".to_owned(),
                    request: EffectCreateRequest {
                        command_id: EffectCommandId::from_static("create-persistent-effect"),
                        operation: EffectOperation {
                            capability: "channel.email".to_owned(),
                            operation: "send".to_owned(),
                        },
                        idempotency_key: "persistent-message".to_owned(),
                        input: serde_json::json!({"artifact_ref":"message"}),
                        not_before_ms: 1,
                    },
                },
            ),
        ],
    );
    let thread_id = match &first[1].body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => thread.id.to_string(),
        other => panic!("unexpected create Thread response: {other:?}"),
    };
    assert!(matches!(
        first[2].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraphCreated { .. }
        }
    ));
    assert!(matches!(
        first[3].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::WorkflowRunCreated { ref run }
        } if run.revision == 1
            && run.task_graph_id == TaskGraphId::from_static("persistent-graph")
    ));
    assert!(matches!(
        first[4].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::HumanHandoffCreated {
                ref handoff
            }
        } if handoff.revision == 1
            && handoff.queue == "support.primary"
            && matches!(handoff.status, y_harness::HumanHandoffStatus::Queued)
    ));
    assert!(matches!(
        first[5].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::EffectCreated {
                ref effect
            }
        } if effect.revision == 1
            && effect.effect_id == y_harness::EffectId::from_static("persistent-effect")
    ));

    let second = serve(
        &project,
        vec![
            request(
                "list-threads",
                ProtocolCommand::ListThreads {
                    before_sequence: None,
                    limit: Some(16),
                },
            ),
            request(
                "get-thread",
                ProtocolCommand::GetThread {
                    thread_id: thread_id.clone(),
                },
            ),
            request(
                "get-graph",
                ProtocolCommand::GetTaskGraph {
                    graph_id: "persistent-graph".to_owned(),
                },
            ),
            request(
                "get-workflow",
                ProtocolCommand::GetWorkflowRun {
                    run_id: "persistent-workflow".to_owned(),
                },
            ),
            request(
                "get-human-handoff",
                ProtocolCommand::GetHumanHandoff {
                    handoff_id: "persistent-handoff".to_owned(),
                },
            ),
            request(
                "get-effect",
                ProtocolCommand::GetEffect {
                    effect_id: "persistent-effect".to_owned(),
                },
            ),
        ],
    );
    assert!(matches!(
        &second[0].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Threads {
                threads,
                has_more: false,
                ..
            }
        } if threads.len() == 1 && threads[0].thread_id.to_string() == thread_id
    ));
    assert!(matches!(
        &second[1].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Thread {
                thread: Some(thread)
            }
        } if thread.id.to_string() == thread_id
    ));
    assert!(matches!(
        second[2].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraph {
                graph: Some(ref graph)
            }
        } if graph.revision == 1 && graph.task_count == 1
    ));
    assert!(matches!(
        second[3].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::WorkflowRun {
                run: Some(ref run)
            }
        } if run.revision == 1
            && run.definition.name == "persistent.workflow"
            && run.task_graph_id == TaskGraphId::from_static("persistent-graph")
    ));
    assert!(matches!(
        second[4].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::HumanHandoff {
                handoff: Some(ref handoff)
            }
        } if handoff.revision == 1
            && handoff.queue == "support.primary"
            && matches!(
                handoff.subject,
                HumanHandoffSubject::WorkflowRun { .. }
            )
    ));
    assert!(matches!(
        second[5].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Effect {
                effect: Some(ref effect)
            }
        } if effect.revision == 1
            && effect.operation.capability == "channel.email"
    ));
    for database in [
        "state.db",
        "approvals.db",
        "tasks.db",
        "workflows.db",
        "human-handoffs.db",
        "effects.db",
    ] {
        assert!(project.join(".y-harness").join(database).is_file());
    }
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn fixed_tenant_service_binds_protocol_state_tasks_and_archives() {
    let project = isolated_project("fixed-tenant");
    fs::create_dir_all(&project).expect("create fixed-tenant project");
    fs::write(
        project.join("y-harness.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "schema_version": 1,
            "data_directory": ".y-harness",
            "authority": {
                "type": "local_process_tenant",
                "tenant_id": "tenant-service"
            },
            "model": {"type": "demo"}
        }))
        .expect("encode fixed-tenant config"),
    )
    .expect("write fixed-tenant config");

    let first = serve(
        &project,
        vec![
            request("initialize", ProtocolCommand::Initialize {}),
            request("create-thread", ProtocolCommand::CreateThread {}),
            request(
                "create-graph",
                ProtocolCommand::CreateTaskGraph {
                    graph_id: "tenant-graph".to_owned(),
                    definitions: vec![TaskDefinition {
                        id: TaskId::from_static("tenant-task"),
                        description: "remain inside the configured tenant".to_owned(),
                        dependencies: Default::default(),
                        priority: 0,
                        workspace: WorkspaceMode::None,
                        required_capabilities: Default::default(),
                    }],
                },
            ),
        ],
    );
    let thread_id = match &first[1].body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { thread },
        } => {
            assert_eq!(thread.tenant_id(), Some("tenant-service"));
            thread.id.to_string()
        }
        other => panic!("unexpected tenant Thread response: {other:?}"),
    };
    assert!(matches!(
        &first[2].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraphCreated { graph }
        } if graph.tenant_id.as_deref() == Some("tenant-service")
    ));

    let archive_path = project.join("tenant-thread.yh-thread.json");
    let exported = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["thread", "export", &thread_id])
        .arg(&archive_path)
        .arg("y-harness.json")
        .current_dir(&project)
        .output()
        .expect("export fixed-tenant Thread");
    assert!(
        exported.status.success(),
        "fixed-tenant export failed: {}",
        String::from_utf8_lossy(&exported.stderr)
    );
    let imported_id = "tenant-imported";
    let imported = Command::new(env!("CARGO_BIN_EXE_yh"))
        .args(["thread", "import"])
        .arg(&archive_path)
        .arg(imported_id)
        .arg("y-harness.json")
        .current_dir(&project)
        .output()
        .expect("import fixed-tenant Thread");
    assert!(
        imported.status.success(),
        "fixed-tenant import failed: {}",
        String::from_utf8_lossy(&imported.stderr)
    );

    let recovered = serve(
        &project,
        vec![
            request(
                "get-thread",
                ProtocolCommand::GetThread {
                    thread_id: thread_id.clone(),
                },
            ),
            request(
                "get-imported",
                ProtocolCommand::GetThread {
                    thread_id: imported_id.to_owned(),
                },
            ),
            request(
                "get-graph",
                ProtocolCommand::GetTaskGraph {
                    graph_id: "tenant-graph".to_owned(),
                },
            ),
        ],
    );
    assert!(matches!(
        &recovered[0].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Thread {
                thread: Some(thread)
            }
        } if thread.tenant_id() == Some("tenant-service")
    ));
    assert!(matches!(
        &recovered[1].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Thread {
                thread: Some(thread)
            }
        } if thread.tenant_id() == Some("tenant-service")
    ));
    assert!(matches!(
        &recovered[2].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraph {
                graph: Some(graph)
            }
        } if graph.tenant_id.as_deref() == Some("tenant-service")
    ));
    fs::remove_dir_all(project).expect("remove fixed-tenant project");
}

fn request(id: &str, command: ProtocolCommand) -> ProtocolRequest {
    ProtocolRequest {
        id: id.to_owned(),
        protocol_version: PROTOCOL_VERSION.to_owned(),
        command,
    }
}

#[cfg(unix)]
fn exchange(
    input: &mut impl Write,
    output: &mut impl BufRead,
    request: ProtocolRequest,
) -> ProtocolResponse {
    serde_json::to_writer(&mut *input, &request).expect("encode protocol request");
    input.write_all(b"\n").expect("write request delimiter");
    input.flush().expect("flush protocol request");
    let mut line = String::new();
    let read = output.read_line(&mut line).expect("read protocol response");
    assert!(read > 0, "service ended before responding");
    serde_json::from_str(&line).expect("decode protocol response")
}

#[cfg(unix)]
fn run_turn_to_completion(
    input: &mut impl Write,
    output: &mut impl BufRead,
    thread_id: &ThreadId,
    prompt: &str,
    request_prefix: &str,
) -> String {
    let started = exchange(
        input,
        output,
        request(
            &format!("{request_prefix}-start"),
            ProtocolCommand::StartTurn {
                thread_id: thread_id.to_string(),
                prompt: prompt.to_owned(),
                memory_scope: Default::default(),
                context: Vec::new(),
                timeout_ms: Some(5_000),
            },
        ),
    );
    let operation_id = match started.body {
        ProtocolResponseBody::Success {
            result: ProtocolResult::TurnStarted { operation_id },
        } => operation_id,
        other => panic!("unexpected start Turn response: {other:?}"),
    };
    for attempt in 0..1_000 {
        let polled = exchange(
            input,
            output,
            request(
                &format!("{request_prefix}-poll-{attempt}"),
                ProtocolCommand::GetOperation {
                    operation_id: operation_id.to_string(),
                },
            ),
        );
        match polled.body {
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Operation {
                        operation: OperationStatus::Running { .. },
                    },
            } => std::thread::sleep(Duration::from_millis(5)),
            ProtocolResponseBody::Success {
                result:
                    ProtocolResult::Operation {
                        operation: OperationStatus::Completed { final_text, .. },
                    },
            } => return final_text,
            other => panic!("unexpected operation response: {other:?}"),
        }
    }
    panic!("Turn did not settle")
}

fn serve(project: &Path, requests: Vec<ProtocolRequest>) -> Vec<ProtocolResponse> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("serve")
        .arg("y-harness.json")
        .current_dir(project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn persistent service");
    {
        let mut input = child.stdin.take().expect("service stdin");
        for request in requests {
            serde_json::to_writer(&mut input, &request).expect("encode request");
            input.write_all(b"\n").expect("write request delimiter");
        }
    }
    let output = child
        .wait_with_output()
        .expect("wait for persistent service");
    assert!(
        output.status.success(),
        "service failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("UTF-8 service output")
        .lines()
        .map(|line| serde_json::from_str(line).expect("decode service response"))
        .collect()
}

#[cfg(unix)]
fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).expect("write executable fixture");
    let mut permissions = fs::metadata(path)
        .expect("read executable fixture metadata")
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions).expect("make fixture executable");
}

#[cfg(unix)]
fn sha256_file(path: &Path) -> String {
    let digest = Sha256::digest(fs::read(path).expect("read digest fixture"));
    digest
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            use std::fmt::Write as _;
            write!(encoded, "{byte:02x}").expect("encode digest fixture");
            encoded
        })
}

fn isolated_project(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "y-harness-service-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}
