use std::{
    collections::BTreeMap,
    env,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(target_os = "macos")]
use y_harness::NetworkAccess;
use y_harness::{
    AgentMemoryHubProvider, CapabilityOrigin, McpClient, MemoryHealthStatus, MemoryProvider,
    MemoryReadRequest, MemoryScope, MemorySearchRequest, MemoryView, MemoryWriteRequest,
    StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority, ToolRegistry, register_mcp_tools,
};

struct IsolatedBrain(PathBuf);

impl IsolatedBrain {
    fn create() -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = env::temp_dir().join(format!("y-harness-amh-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&path).expect("create isolated brain");
        Self(path)
    }
}

impl Drop for IsolatedBrain {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
#[ignore = "requires YH_AMH_SERVER pointing to an Agent Memory Hub MCP launcher"]
async fn agent_memory_hub_stdio_round_trip() {
    let server =
        PathBuf::from(env::var_os("YH_AMH_SERVER").expect("set YH_AMH_SERVER to AMH server.sh"));
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let brain_dir = IsolatedBrain::create();
    let server_dir = server
        .parent()
        .expect("YH_AMH_SERVER must have a parent directory")
        .to_path_buf();
    let mut child_environment = BTreeMap::from([(
        "BRAIN_DIR".to_owned(),
        brain_dir.0.to_string_lossy().into_owned(),
    )]);
    for name in ["PATH", "LANG", "LC_ALL"] {
        if let Ok(value) = env::var(name) {
            child_environment.insert(name.to_owned(), value);
        }
    }
    child_environment.insert("MEMORY_HUB_EMBEDDING_OFFLINE".to_owned(), "1".to_owned());
    child_environment.insert("PYTHONDONTWRITEBYTECODE".to_owned(), "1".to_owned());
    child_environment.insert(
        "TMPDIR".to_owned(),
        brain_dir.0.to_string_lossy().into_owned(),
    );

    #[cfg(target_os = "macos")]
    let launch_authority = StdioMcpLaunchAuthority::macos_seatbelt(
        1,
        vec![brain_dir.0.clone(), PathBuf::from("/tmp")],
        NetworkAccess::Deny,
    )
    .expect("sandboxed MCP authority");
    #[cfg(not(target_os = "macos"))]
    let launch_authority =
        StdioMcpLaunchAuthority::unrestricted(1).expect("bounded unrestricted MCP authority");

    let client = Arc::new(
        StdioMcpClient::new(
            StdioMcpConfig {
                command: server,
                args: Vec::new(),
                env: child_environment,
                current_dir: server_dir,
                request_timeout: Duration::from_secs(45),
            },
            launch_authority,
        )
        .expect("MCP client"),
    );
    let provider = AgentMemoryHubProvider::new(client.clone() as Arc<dyn McpClient>);

    let health = provider.health().await.expect("health");
    assert_eq!(health.status, MemoryHealthStatus::Healthy, "{health:?}");

    let title = format!("Y-Harness MCP integration {unique}");
    let written = provider
        .write(MemoryWriteRequest {
            idempotency_key: format!("integration-{unique}"),
            kind: "fact".to_owned(),
            title: title.clone(),
            summary: "An isolated integration test record".to_owned(),
            body: "**事实** MCP round trip works.\n\n**来源** Y-Harness integration test."
                .to_owned(),
            scope: MemoryScope {
                project: Some("y-harness-integration".to_owned()),
                tenant_id: None,
                tags: vec!["y-harness".to_owned(), "integration".to_owned()],
            },
            provenance: Vec::new(),
        })
        .await
        .expect("write");
    let reference = written.reference.expect("written id");

    let read = provider
        .read(MemoryReadRequest {
            reference: reference.clone(),
            view: MemoryView::Detail,
            head_chars: Some(2_000),
        })
        .await
        .expect("read");
    assert!(read.text.contains("MCP round trip works"));

    let searched = provider
        .search(MemorySearchRequest {
            query: title,
            scope: MemoryScope {
                project: Some("y-harness-integration".to_owned()),
                tenant_id: None,
                tags: Vec::new(),
            },
            top_k: 5,
            budget_tokens: 500,
        })
        .await
        .expect("search");
    assert!(
        searched
            .packs
            .iter()
            .any(|pack| pack.reference == reference),
        "{searched:?}"
    );

    let mut tools = ToolRegistry::new();
    let registered = register_mcp_tools(
        &mut tools,
        CapabilityOrigin::External {
            id: "agent-memory-hub".to_owned(),
        },
        "amh",
        client.clone() as Arc<dyn McpClient>,
    )
    .await
    .expect("register real MCP catalog");
    assert!(registered.contains(&"amh.search_memory".to_owned()));
    assert!(registered.contains(&"amh.brain_stats".to_owned()));

    drop(provider);
    client.shutdown().await.expect("shutdown");
}
