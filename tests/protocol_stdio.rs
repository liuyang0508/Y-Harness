use std::{
    fs,
    io::Write,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use y_harness::{
    ApprovalId, ApprovalInbox, ApprovalRecordStatus, PROTOCOL_VERSION, ProtocolCommand,
    ProtocolRequest, ProtocolResponse, ProtocolResponseBody, ProtocolResult, SqliteApprovalInbox,
    SqliteTaskCoordinator, TaskCoordinator, TaskDefinition, TaskGraphId, TaskId, WorkspaceMode,
};

const STATE_V1_FIXTURE: &str = include_str!("fixtures/state-v1.sql");
const APPROVAL_V1_FIXTURE: &str = include_str!("fixtures/approval-v1.sql");
const APPROVAL_V2_FIXTURE: &str = include_str!("fixtures/approval-v2.sql");
const TASK_V1_FIXTURE: &str = include_str!("fixtures/task-v1.sql");

#[test]
fn stdio_server_preserves_one_response_per_request_and_stdout_purity() {
    let working_directory = isolated_working_directory("stdio");
    fs::create_dir_all(&working_directory).expect("create isolated working directory");

    let mut child = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("serve-demo")
        .current_dir(&working_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn stdio server");

    let requests = [
        ProtocolRequest {
            id: "initialize".to_owned(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command: ProtocolCommand::Initialize {},
        },
        ProtocolRequest {
            id: "create".to_owned(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command: ProtocolCommand::CreateThread {},
        },
        ProtocolRequest {
            id: "create-task-graph".to_owned(),
            protocol_version: PROTOCOL_VERSION.to_owned(),
            command: ProtocolCommand::CreateTaskGraph {
                graph_id: "stdio-task-graph".to_owned(),
                definitions: vec![TaskDefinition {
                    id: TaskId::from_static("stdio-task"),
                    description: "process-level Task protocol evidence".to_owned(),
                    dependencies: Default::default(),
                    priority: 0,
                    workspace: WorkspaceMode::None,
                    required_capabilities: Default::default(),
                }],
            },
        },
    ];
    {
        let mut input = child.stdin.take().expect("child stdin");
        for request in requests {
            serde_json::to_writer(&mut input, &request).expect("encode request");
            input.write_all(b"\n").expect("write frame delimiter");
        }
    }

    let output = child.wait_with_output().expect("wait for stdio server");
    let _ = fs::remove_dir_all(&working_directory);
    assert!(
        output.status.success(),
        "server failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    let responses = stdout
        .lines()
        .map(|line| serde_json::from_str::<ProtocolResponse>(line).expect("protocol response"))
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), 3, "stdout must contain only three frames");
    assert!(matches!(
        responses[0].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Initialized {
                ref capabilities,
                ..
            }
        } if capabilities.contains(&"task.worker.claim".to_owned())
    ));
    assert!(matches!(
        responses[1].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::ThreadCreated { .. }
        }
    ));
    assert!(matches!(
        responses[2].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraphCreated { .. }
        }
    ));
}

#[test]
fn evaluation_smoke_cli_emits_a_passing_machine_readable_report() {
    let working_directory = isolated_working_directory("evaluation");
    fs::create_dir_all(&working_directory).expect("create isolated working directory");

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("eval-smoke")
        .current_dir(&working_directory)
        .output()
        .expect("run evaluation smoke gate");
    assert!(
        output.status.success(),
        "evaluation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("machine-readable evaluation report");
    assert_eq!(report["schema_version"], 2);
    assert_eq!(report["report"]["suite"], "harness-smoke-v1");
    assert_eq!(
        report["report"]["cases"]
            .as_array()
            .expect("evaluation cases")
            .len(),
        2
    );
    assert_eq!(report["comparison"]["passed"], true);
    assert!(
        fs::read_dir(&working_directory)
            .expect("read isolated directory")
            .next()
            .is_none(),
        "evaluation must not create ambient files"
    );
    let _ = fs::remove_dir_all(&working_directory);
}

#[test]
fn state_migration_cli_creates_a_no_clobber_backup() {
    let working_directory = isolated_working_directory("migration");
    fs::create_dir_all(&working_directory).expect("create isolated working directory");
    let source = working_directory.join("state-v1.db");
    let backup = working_directory.join("state-v1.backup.db");
    rusqlite::Connection::open(&source)
        .expect("create v1 database")
        .execute_batch(STATE_V1_FIXTURE)
        .expect("apply v1 fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("state-migrate")
        .arg(&source)
        .arg(&backup)
        .output()
        .expect("run state migration");
    assert!(
        output.status.success(),
        "migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(backup.is_file());
    assert!(String::from_utf8_lossy(&output.stdout).contains("State schema 1 -> 16"));

    let second = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("state-migrate")
        .arg(&source)
        .arg(&backup)
        .output()
        .expect("rerun state migration");
    assert!(second.status.success());
    assert!(String::from_utf8_lossy(&second.stdout).contains("already current"));
    let _ = fs::remove_dir_all(&working_directory);
}

#[tokio::test]
async fn approval_migration_cli_orphans_unattributed_pending_requests() {
    let working_directory = isolated_working_directory("approval-migration");
    fs::create_dir_all(&working_directory).expect("create isolated working directory");
    let source = working_directory.join("approval-v1.db");
    let backup = working_directory.join("approval-v1.backup.db");
    rusqlite::Connection::open(&source)
        .expect("create v1 approval database")
        .execute_batch(APPROVAL_V1_FIXTURE)
        .expect("apply approval v1 fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("approval-migrate")
        .arg(&source)
        .arg(&backup)
        .output()
        .expect("run approval migration");
    assert!(
        output.status.success(),
        "approval migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(backup.is_file());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Approval Inbox schema 1 -> 3"));
    let inbox = SqliteApprovalInbox::open(&source)
        .await
        .expect("open migrated inbox");
    let record = inbox
        .get(&ApprovalId::from_string("approval-v1".to_owned()))
        .await
        .expect("read migrated approval")
        .expect("approval");
    assert!(matches!(
        record.status,
        ApprovalRecordStatus::Orphaned { .. }
    ));

    let schema_two = working_directory.join("approval-v2.db");
    let schema_two_backup = working_directory.join("approval-v2.backup.db");
    rusqlite::Connection::open(&schema_two)
        .expect("create v2 approval database")
        .execute_batch(APPROVAL_V2_FIXTURE)
        .expect("apply approval v2 fixture");
    let report = SqliteApprovalInbox::migrate(&schema_two, &schema_two_backup)
        .await
        .expect("migrate explicit v2 fixture");
    assert_eq!(report.from_record_schema, 2);
    assert_eq!(report.to_record_schema, 3);
    SqliteApprovalInbox::open(&schema_two)
        .await
        .expect("open migrated v2 fixture");
    let _ = fs::remove_dir_all(&working_directory);
}

#[tokio::test]
async fn task_migration_cli_preserves_legacy_graphs_as_unscoped() {
    let working_directory = isolated_working_directory("task-migration");
    fs::create_dir_all(&working_directory).expect("create isolated working directory");
    let source = working_directory.join("task-v1.db");
    let backup = working_directory.join("task-v1.backup.db");
    rusqlite::Connection::open(&source)
        .expect("create v1 Task database")
        .execute_batch(TASK_V1_FIXTURE)
        .expect("apply Task v1 fixture");

    let output = Command::new(env!("CARGO_BIN_EXE_yh"))
        .arg("task-migrate")
        .arg(&source)
        .arg(&backup)
        .output()
        .expect("run Task migration");
    assert!(
        output.status.success(),
        "Task migration failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(backup.is_file());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Task Graph schema 1 -> 4"));

    let coordinator = SqliteTaskCoordinator::open(&source)
        .await
        .expect("open migrated Task store");
    let snapshot = coordinator
        .load(&TaskGraphId::from_static("task-graph-v1"))
        .await
        .expect("load migrated graph")
        .expect("legacy graph");
    assert_eq!(snapshot.tenant_id(), None);
    assert_eq!(snapshot.revision(), 4);
    let _ = fs::remove_dir_all(&working_directory);
}

fn isolated_working_directory(label: &str) -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("y-harness-{label}-{}-{nonce}", std::process::id()))
}
