use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use y_harness::{
    CancellationToken, ExecutionPhase, HarnessError, McpClient, StdioMcpClient, StdioMcpConfig,
    StdioMcpLaunchAuthority,
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn fixture_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_yh-fault-fixture"))
}

fn temp_fixture(case: &str) -> (PathBuf, PathBuf) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let directory = env::temp_dir().join(format!(
        "yh-fault-fixture-{}-{nonce}-{sequence}",
        std::process::id()
    ));
    fs::create_dir(&directory).expect("create fixture directory");
    let spec_path = directory.join("fixture.json");
    let journal_path = directory.join("journal.jsonl");
    let spec = json!({
        "format_version": 1,
        "fixture_id": format!("cf-tool-{case}-001"),
        "case": case,
        "expected_fixture_executable_sha256": sha256_file(&fixture_binary()),
        "journal": journal_path,
        "operation_id": "effect-001",
        "expected_payload_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    });
    fs::write(
        &spec_path,
        serde_json::to_vec_pretty(&spec).expect("encode spec"),
    )
    .expect("write spec");
    (directory, spec_path)
}

fn sha256_file(path: &Path) -> String {
    let bytes = fs::read(path).expect("read fixture executable");
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn run_fixture(command: &str, spec_path: &Path) -> Value {
    let output = Command::new(fixture_binary())
        .arg(command)
        .arg(spec_path)
        .env_clear()
        .output()
        .expect("run fixture command");
    assert!(
        output.status.success(),
        "fixture {command} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("fixture JSON report")
}

fn client(spec_path: &Path, current_dir: &Path) -> StdioMcpClient {
    StdioMcpClient::new(
        StdioMcpConfig {
            command: fixture_binary(),
            args: vec!["serve".to_owned(), spec_path.to_string_lossy().into_owned()],
            env: BTreeMap::new(),
            current_dir: current_dir.to_path_buf(),
            request_timeout: Duration::from_secs(5),
        },
        StdioMcpLaunchAuthority::unrestricted(1).expect("fixture process authority"),
    )
    .expect("fixture client")
}

#[tokio::test]
async fn real_mcp_crash_is_durable_and_never_implicitly_retried() {
    let (directory, spec_path) = temp_fixture("crash_after_first_effect");
    let prepared = run_fixture("prepare", &spec_path);
    assert_eq!(prepared["format_version"], 1);
    let repeated_prepare = Command::new(fixture_binary())
        .arg("prepare")
        .arg(&spec_path)
        .env_clear()
        .output()
        .expect("repeat fixture prepare");
    assert!(!repeated_prepare.status.success());

    let client = client(&spec_path, &directory);
    let tools = client.list_tools().await.expect("fixture tool catalog");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "commit_effect");
    let arguments = json!({
        "operation_id": "effect-001",
        "payload_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    });
    let error = client
        .call_tool("commit_effect", arguments.clone())
        .await
        .expect_err("first call crashes after effect");
    assert!(error.to_string().contains("failed"));

    let first = run_fixture("inspect", &spec_path);
    assert_eq!(first["invocation_count"], 1);
    assert_eq!(first["effect_count"], 1);
    assert_eq!(first["oracle"]["passed"], true);
    assert_eq!(
        first["oracle"]["classification"],
        "uncertain_effect_not_replayed"
    );

    let second = client
        .call_tool("commit_effect", arguments)
        .await
        .expect("an explicit second call reconnects");
    assert_eq!(second["effect_ordinal"], 2);
    client.shutdown().await.expect("fixture shutdown");

    let duplicated = run_fixture("inspect", &spec_path);
    assert_eq!(duplicated["invocation_count"], 2);
    assert_eq!(duplicated["effect_count"], 2);
    assert_eq!(duplicated["oracle"]["passed"], false);
    assert_eq!(duplicated["oracle"]["classification"], "duplicate_effect");

    fs::remove_dir_all(&directory).expect("remove isolated fixture directory");
}

#[tokio::test]
async fn cancelling_a_live_stdio_mcp_call_invalidates_its_session() {
    let (directory, spec_path) = temp_fixture("hold_after_first_effect");
    run_fixture("prepare", &spec_path);
    let client = client(&spec_path, &directory);
    client.list_tools().await.expect("fixture tool catalog");
    let arguments = json!({
        "operation_id": "effect-001",
        "payload_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    });
    let cancellation = CancellationToken::new();
    let cancel_from_thread = cancellation.clone();
    let journal_path = directory.join("journal.jsonl");
    let canceller = std::thread::spawn(move || {
        for _ in 0..500 {
            let committed = fs::read_to_string(&journal_path)
                .is_ok_and(|journal| journal.contains("\"type\":\"effect_committed\""));
            if committed {
                cancel_from_thread.cancel();
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        cancel_from_thread.cancel();
        false
    });

    let error = client
        .call_tool_with_cancellation("commit_effect", arguments.clone(), cancellation)
        .await
        .expect_err("live MCP call should be cancelled");
    assert_eq!(
        error,
        HarnessError::Cancelled {
            phase: ExecutionPhase::Tool
        }
    );
    assert!(canceller.join().expect("canceller"));

    let after_cancel = run_fixture("inspect", &spec_path);
    assert_eq!(after_cancel["invocation_count"], 1);
    assert_eq!(after_cancel["effect_count"], 1);
    assert_eq!(
        after_cancel["oracle"]["classification"],
        "uncertain_effect_not_replayed"
    );

    let second = client
        .call_tool("commit_effect", arguments)
        .await
        .expect("an explicit second call reconnects");
    assert_eq!(second["effect_ordinal"], 2);
    client.shutdown().await.expect("fixture shutdown");

    fs::remove_dir_all(&directory).expect("remove isolated fixture directory");
}
