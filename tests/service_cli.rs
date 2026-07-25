use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use y_harness::{
    PROTOCOL_VERSION, ProtocolCommand, ProtocolRequest, ProtocolResponse, ProtocolResponseBody,
    ProtocolResult, TaskDefinition, TaskId, WorkspaceMode,
};

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
    assert!(report.contains("protocol: 10"));
    assert!(report.contains("model: local/demo"));
    assert!(report.contains("status: ok"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[cfg(feature = "https-model")]
#[test]
fn doctor_accepts_the_checked_in_https_gateway_template() {
    let project = isolated_project("https-template");
    fs::create_dir_all(&project).expect("create HTTPS template project");
    let config_path = project.join("y-harness.json");
    fs::write(
        &config_path,
        include_bytes!("../config/y-harness.https.example.json"),
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
    assert!(report.contains("status: ok"));
    assert!(!report.contains("doctor-placeholder"));
    fs::remove_dir_all(project).expect("remove isolated project");
}

#[test]
fn persistent_service_recovers_threads_and_task_graphs_after_restart() {
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
                    }],
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

    let second = serve(
        &project,
        vec![
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
        ],
    );
    assert!(matches!(
        &second[0].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::Thread {
                thread: Some(thread)
            }
        } if thread.id.to_string() == thread_id
    ));
    assert!(matches!(
        second[1].body,
        ProtocolResponseBody::Success {
            result: ProtocolResult::TaskGraph {
                graph: Some(ref graph)
            }
        } if graph.revision == 1 && graph.task_count == 1
    ));
    for database in ["state.db", "approvals.db", "tasks.db"] {
        assert!(project.join(".y-harness").join(database).is_file());
    }
    fs::remove_dir_all(project).expect("remove isolated project");
}

fn request(id: &str, command: ProtocolCommand) -> ProtocolRequest {
    ProtocolRequest {
        id: id.to_owned(),
        protocol_version: PROTOCOL_VERSION.to_owned(),
        command,
    }
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
