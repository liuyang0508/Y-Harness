//! MCP client contracts and a supervised, persistent stdio implementation.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug},
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult, PaginatedRequestParams, Tool as RmcpTool},
    service::RunningService,
    transport::{Transport, async_rw::AsyncRwTransport},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncRead, ReadBuf},
    process::{Child, ChildStdin, ChildStdout, Command},
    runtime::Handle,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

use crate::{
    CapabilityOrigin, ExecutionPhase, HarnessError, HarnessFuture, NetworkAccess,
    ProcessBrokerDescriptor, ProcessIsolation, Tool, ToolContext, ToolDescriptor, ToolRegistry,
    execution::{ChildProcessGroup, MacOsSeatbeltPolicy, configure_process_group},
    kernel::{validate_capability_name, validate_capability_origin, validate_registry_growth},
};

const MAX_MCP_ARGUMENTS: usize = 256;
const MAX_MCP_ARGUMENT_BYTES: usize = 16_384;
const MAX_MCP_ENVIRONMENT_ENTRIES: usize = 256;
const MAX_MCP_ENVIRONMENT_BYTES: usize = 65_536;
const MAX_MCP_FRAME_BYTES: usize = 8_388_608;
const MAX_MCP_TOOL_ARGUMENT_BYTES: usize = 1_048_576;
const MAX_MCP_TOOL_RESULT_BYTES: usize = 1_048_576;
const MAX_MCP_TOOL_CATALOG_BYTES: usize = 16_777_216;
const MAX_MCP_TOOLS: usize = 4_096;
const MAX_MCP_TOOL_PAGES: usize = 256;
const MAX_MCP_TOOL_NAME_BYTES: usize = 256;
const MAX_MCP_TOOL_DESCRIPTION_BYTES: usize = 65_536;
const MAX_MCP_CURSOR_BYTES: usize = 4_096;
const MAX_MCP_TIMEOUT: Duration = Duration::from_secs(86_400);
const MAX_MCP_PROCESS_CONCURRENCY: usize = 4_096;
const MCP_CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Provider-neutral view of an MCP tool declaration.
pub struct McpToolDescriptor {
    /// Protocol tool name.
    pub name: String,
    /// Optional server description.
    pub description: Option<String>,
    /// Tool input JSON Schema.
    pub input_schema: Value,
}

/// Minimal MCP client port used by capability adapters.
pub trait McpClient: Send + Sync {
    /// Lists every tool after MCP lifecycle initialization and pagination.
    fn list_tools<'a>(&'a self) -> HarnessFuture<'a, Vec<McpToolDescriptor>>;
    /// Calls one tool with JSON-object arguments.
    fn call_tool<'a>(&'a self, name: &'a str, arguments: Value) -> HarnessFuture<'a, Value>;
}

struct McpToolAdapter {
    client: Arc<dyn McpClient>,
    remote_name: String,
    descriptor: ToolDescriptor,
}

impl Tool for McpToolAdapter {
    fn descriptor(&self) -> ToolDescriptor {
        self.descriptor.clone()
    }

    fn execute<'a>(&'a self, input: Value, context: ToolContext) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            if context.cancellation.is_cancelled() {
                return Err(HarnessError::Cancelled {
                    phase: ExecutionPhase::Tool,
                });
            }
            self.client.call_tool(&self.remote_name, input).await
        })
    }
}

/// Discovers one MCP catalog and atomically registers namespaced Runtime tools.
///
/// The namespace and every resulting `<namespace>.<remote-name>` must satisfy
/// the Kernel's portable tool-name contract. Registration never rewrites
/// server names and never leaves a partially registered catalog.
pub async fn register_mcp_tools(
    registry: &mut ToolRegistry,
    origin: CapabilityOrigin,
    namespace: &str,
    client: Arc<dyn McpClient>,
) -> Result<Vec<String>, HarnessError> {
    register_mcp_catalog(registry, origin, namespace, client, None).await
}

/// Discovers one MCP catalog and registers only an explicit remote-name set.
///
/// Every requested name must exist. Selection and registration are atomic, so
/// a stale or partially matching catalog never grants a subset accidentally.
pub async fn register_selected_mcp_tools(
    registry: &mut ToolRegistry,
    origin: CapabilityOrigin,
    namespace: &str,
    client: Arc<dyn McpClient>,
    remote_names: &[String],
) -> Result<Vec<String>, HarnessError> {
    validate_registry_growth("selected MCP tool", 0, remote_names.len())?;
    let mut selected = BTreeSet::new();
    for name in remote_names {
        validate_mcp_tool_name(name)?;
        if !selected.insert(name.clone()) {
            return Err(HarnessError::Mcp(format!(
                "selected MCP tool {name} is duplicated"
            )));
        }
    }
    register_mcp_catalog(registry, origin, namespace, client, Some(&selected)).await
}

async fn register_mcp_catalog(
    registry: &mut ToolRegistry,
    origin: CapabilityOrigin,
    namespace: &str,
    client: Arc<dyn McpClient>,
    selected: Option<&BTreeSet<String>>,
) -> Result<Vec<String>, HarnessError> {
    validate_capability_origin(&origin)?;
    validate_capability_name("MCP namespace", namespace)?;
    let discovered = client.list_tools().await?;
    validate_registry_growth("MCP tool", 0, discovered.len())?;
    let mut staged = Vec::with_capacity(discovered.len());
    let mut names = Vec::with_capacity(discovered.len());
    let mut matched = BTreeSet::new();
    for remote in discovered {
        if selected.is_some_and(|selected| !selected.contains(&remote.name)) {
            continue;
        }
        matched.insert(remote.name.clone());
        crate::json::validate_value_shape(&remote.input_schema).map_err(|_| {
            HarnessError::Mcp(
                "MCP tool schema exceeds the supported JSON depth or node count".to_owned(),
            )
        })?;
        crate::json::bounded_serialized_size(&remote, MAX_MCP_TOOL_CATALOG_BYTES).map_err(
            |error| match error {
                crate::json::BoundedJsonError::LimitExceeded => HarnessError::Mcp(format!(
                    "MCP tool descriptor exceeds {MAX_MCP_TOOL_CATALOG_BYTES} bytes"
                )),
                crate::json::BoundedJsonError::CannotEncode => {
                    HarnessError::Mcp("cannot encode MCP tool descriptor".to_owned())
                }
            },
        )?;
        let registry_name = format!("{namespace}.{}", remote.name);
        validate_capability_name("namespaced MCP tool", &registry_name)?;
        let description = remote
            .description
            .filter(|description| !description.trim().is_empty())
            .unwrap_or_else(|| format!("MCP tool {}", remote.name));
        let descriptor = ToolDescriptor {
            name: registry_name.clone(),
            description,
            input_schema: remote.input_schema,
        };
        let tool: Arc<dyn Tool> = Arc::new(McpToolAdapter {
            client: client.clone(),
            remote_name: remote.name,
            descriptor,
        });
        staged.push((origin.clone(), tool));
        names.push(registry_name);
    }
    if let Some(selected) = selected {
        let missing = selected.difference(&matched).cloned().collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(HarnessError::Mcp(format!(
                "selected MCP tools are missing: {}",
                missing.join(", ")
            )));
        }
    }
    names.sort();
    registry.register_batch(staged)?;
    Ok(names)
}

#[derive(Clone)]
/// Direct-process configuration for a persistent stdio MCP session.
pub struct StdioMcpConfig {
    /// Absolute executable path; no shell or `PATH` lookup is performed.
    pub command: PathBuf,
    /// Ordered executable arguments.
    pub args: Vec<String>,
    /// Exact child environment after inherited variables are cleared.
    pub env: BTreeMap<String, String>,
    /// Exact absolute child working directory.
    pub current_dir: PathBuf,
    /// Bound applied independently to initialization, calls, listing, and shutdown.
    pub request_timeout: Duration,
}

impl Debug for StdioMcpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StdioMcpConfig")
            .field("command", &self.command)
            .field("argument_count", &self.args.len())
            .field("environment_names", &self.env.keys().collect::<Vec<_>>())
            .field("current_dir", &self.current_dir)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

/// Explicit authority for launching persistent stdio MCP server processes.
///
/// The default denies execution. An unrestricted authority is an operator
/// decision: it bounds concurrent child sessions but does not restrict their
/// filesystem, network, credential, or syscall access.
#[derive(Clone)]
pub struct StdioMcpLaunchAuthority {
    descriptor: ProcessBrokerDescriptor,
    concurrency: Option<Arc<Semaphore>>,
    seatbelt: Option<MacOsSeatbeltPolicy>,
}

impl StdioMcpLaunchAuthority {
    /// Creates the secure authority that rejects every MCP process launch.
    #[must_use]
    pub fn denied() -> Self {
        Self {
            descriptor: ProcessBrokerDescriptor {
                name: "stdio-mcp-deny".to_owned(),
                isolation: ProcessIsolation::Denied,
            },
            concurrency: None,
            seatbelt: None,
        }
    }

    /// Explicitly allows bounded MCP child processes with Runtime-user authority.
    pub fn unrestricted(maximum_concurrency: usize) -> Result<Self, HarnessError> {
        if !(1..=MAX_MCP_PROCESS_CONCURRENCY).contains(&maximum_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "stdio MCP maximum_concurrency must be 1-{MAX_MCP_PROCESS_CONCURRENCY}"
            )));
        }
        Ok(Self {
            descriptor: ProcessBrokerDescriptor {
                name: format!("stdio-mcp-local-unrestricted-{maximum_concurrency}"),
                isolation: ProcessIsolation::Unrestricted,
            },
            concurrency: Some(Arc::new(Semaphore::new(maximum_concurrency))),
            seatbelt: None,
        })
    }

    /// Allows bounded MCP child processes under the scoped macOS Seatbelt policy.
    pub fn macos_seatbelt(
        maximum_concurrency: usize,
        writable_roots: Vec<PathBuf>,
        network_access: NetworkAccess,
    ) -> Result<Self, HarnessError> {
        if !(1..=MAX_MCP_PROCESS_CONCURRENCY).contains(&maximum_concurrency) {
            return Err(HarnessError::InvalidConfiguration(format!(
                "stdio MCP maximum_concurrency must be 1-{MAX_MCP_PROCESS_CONCURRENCY}"
            )));
        }
        Ok(Self {
            descriptor: ProcessBrokerDescriptor {
                name: format!("stdio-mcp-macos-seatbelt-{maximum_concurrency}"),
                isolation: ProcessIsolation::Sandboxed {
                    mechanism: "macos-seatbelt-write-network".to_owned(),
                },
            },
            concurrency: Some(Arc::new(Semaphore::new(maximum_concurrency))),
            seatbelt: Some(MacOsSeatbeltPolicy::new(writable_roots, network_access)?),
        })
    }

    /// Reports the exact isolation strength enforced by this authority.
    #[must_use]
    pub fn descriptor(&self) -> ProcessBrokerDescriptor {
        self.descriptor.clone()
    }

    async fn acquire(&self, wait: Duration) -> Result<OwnedSemaphorePermit, HarnessError> {
        let Some(concurrency) = &self.concurrency else {
            return Err(HarnessError::Mcp(
                "stdio MCP process execution is disabled".to_owned(),
            ));
        };
        timeout(wait, concurrency.clone().acquire_owned())
            .await
            .map_err(|_| HarnessError::Mcp("stdio MCP process launch queue timed out".to_owned()))?
            .map_err(|_| HarnessError::Mcp("stdio MCP launch authority is closed".to_owned()))
    }

    fn prepare_command(
        &self,
        config: &StdioMcpConfig,
    ) -> Result<(PathBuf, Vec<String>), HarnessError> {
        match &self.seatbelt {
            Some(policy) => policy.wrap_command(&config.command, config.args.clone()),
            None => Ok((config.command.clone(), config.args.clone())),
        }
    }
}

impl Default for StdioMcpLaunchAuthority {
    fn default() -> Self {
        Self::denied()
    }
}

impl Debug for StdioMcpLaunchAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StdioMcpLaunchAuthority")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

/// Persistent, serialized stdio MCP client using the official Rust SDK.
///
/// A failed session is invalidated and the next operation reconnects. Tool
/// calls are not automatically retried because their side effects may be
/// non-idempotent.
pub struct StdioMcpClient {
    config: StdioMcpConfig,
    launch_authority: StdioMcpLaunchAuthority,
    session: Mutex<Option<RunningService<RoleClient, ()>>>,
}

struct BoundedLineReader<R> {
    inner: R,
    line_bytes: usize,
    max_line_bytes: usize,
    failed: bool,
}

impl<R> BoundedLineReader<R> {
    fn new(inner: R, max_line_bytes: usize) -> Self {
        Self {
            inner,
            line_bytes: 0,
            max_line_bytes,
            failed: false,
        }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for BoundedLineReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "MCP frame exceeds configured limit",
            )));
        }
        if buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let current_line_capacity = this
            .max_line_bytes
            .saturating_sub(this.line_bytes)
            .saturating_add(1);
        let mut scratch = [0_u8; 8_192];
        let read_capacity = buffer
            .remaining()
            .min(scratch.len())
            .min(current_line_capacity);
        let mut scratch_buffer = ReadBuf::new(&mut scratch[..read_capacity]);
        match Pin::new(&mut this.inner).poll_read(context, &mut scratch_buffer) {
            Poll::Ready(Ok(())) => {
                for byte in scratch_buffer.filled() {
                    if *byte == b'\n' {
                        this.line_bytes = 0;
                    } else {
                        this.line_bytes = match this.line_bytes.checked_add(1) {
                            Some(bytes) => bytes,
                            None => {
                                this.failed = true;
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "MCP frame size overflow",
                                )));
                            }
                        };
                        if this.line_bytes > this.max_line_bytes {
                            this.failed = true;
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "MCP frame exceeds configured limit",
                            )));
                        }
                    }
                }
                buffer.put_slice(scratch_buffer.filled());
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

struct BoundedChildTransport {
    transport: AsyncRwTransport<RoleClient, BoundedLineReader<ChildStdout>, ChildStdin>,
    child: Option<Child>,
    process_group: ChildProcessGroup,
    _launch_permit: OwnedSemaphorePermit,
}

impl BoundedChildTransport {
    fn spawn(
        config: &StdioMcpConfig,
        program: PathBuf,
        args: Vec<String>,
        launch_permit: OwnedSemaphorePermit,
    ) -> io::Result<Self> {
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(&config.current_dir)
            .env_clear()
            .envs(&config.env)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        configure_process_group(&mut command);
        let mut child = command.spawn()?;
        let process_group = ChildProcessGroup::for_child(&child)
            .map_err(|error| io::Error::other(error.to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("MCP child stdout was not configured"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("MCP child stdin was not configured"))?;
        Ok(Self {
            transport: AsyncRwTransport::new(
                BoundedLineReader::new(stdout, MAX_MCP_FRAME_BYTES),
                stdin,
            ),
            child: Some(child),
            process_group,
            _launch_permit: launch_permit,
        })
    }
}

impl Transport<RoleClient> for BoundedChildTransport {
    type Error = io::Error;

    fn send(
        &mut self,
        item: rmcp::service::TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.transport.send(item)
    }

    fn receive(
        &mut self,
    ) -> impl Future<Output = Option<rmcp::service::RxJsonRpcMessage<RoleClient>>> + Send {
        self.transport.receive()
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.transport.close().await?;
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        match timeout(MCP_CHILD_SHUTDOWN_TIMEOUT, child.wait()).await {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                self.process_group.request_kill();
                child.start_kill()?;
                let _ = child.wait().await;
            }
        }
        if !self.process_group.settle_remaining().await {
            return Err(io::Error::other(
                "MCP child process group did not settle within termination grace",
            ));
        }
        Ok(())
    }
}

impl Drop for BoundedChildTransport {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let _ = child.start_kill();
        if let Ok(runtime) = Handle::try_current() {
            runtime.spawn(async move {
                let _ = child.wait().await;
            });
        }
    }
}

impl StdioMcpClient {
    /// Validates configuration and creates a lazily connected client.
    ///
    /// Launch authority is mandatory so an unrestricted persistent process is
    /// never enabled by constructor default.
    pub fn new(
        config: StdioMcpConfig,
        launch_authority: StdioMcpLaunchAuthority,
    ) -> Result<Self, HarnessError> {
        validate_stdio_config(&config)?;
        Ok(Self {
            config,
            launch_authority,
            session: Mutex::new(None),
        })
    }

    /// Reports the exact process isolation selected by the embedding host.
    #[must_use]
    pub fn launch_descriptor(&self) -> ProcessBrokerDescriptor {
        self.launch_authority.descriptor()
    }

    async fn connect(&self) -> Result<RunningService<RoleClient, ()>, HarnessError> {
        let launch_permit = self
            .launch_authority
            .acquire(self.config.request_timeout)
            .await?;
        let (program, args) = self.launch_authority.prepare_command(&self.config)?;
        let transport = BoundedChildTransport::spawn(&self.config, program, args, launch_permit)
            .map_err(|error| HarnessError::Mcp(format!("failed to start MCP server: {error}")))?;
        timeout(self.config.request_timeout, ().serve(transport))
            .await
            .map_err(|_| HarnessError::Mcp("MCP initialization timed out".to_owned()))?
            .map_err(|_| HarnessError::Mcp("MCP initialization failed".to_owned()))
    }

    async fn ensure_connected<'a>(
        &'a self,
        session: &'a mut Option<RunningService<RoleClient, ()>>,
    ) -> Result<&'a RunningService<RoleClient, ()>, HarnessError> {
        if session.as_ref().is_none_or(RunningService::is_closed) {
            *session = Some(self.connect().await?);
        }
        session
            .as_ref()
            .ok_or_else(|| HarnessError::Mcp("MCP session was not initialized".to_owned()))
    }

    fn invalidate(session: &mut Option<RunningService<RoleClient, ()>>) {
        if let Some(service) = session.take() {
            service.cancellation_token().cancel();
        }
    }

    /// Gracefully closes the active MCP session within the configured timeout.
    pub async fn shutdown(&self) -> Result<(), HarnessError> {
        let service = self.session.lock().await.take();
        if let Some(service) = service {
            timeout(self.config.request_timeout, service.cancel())
                .await
                .map_err(|_| HarnessError::Mcp("MCP shutdown timed out".to_owned()))?
                .map_err(|_| HarnessError::Mcp("MCP shutdown failed".to_owned()))?;
        }
        Ok(())
    }
}

impl McpClient for StdioMcpClient {
    fn list_tools<'a>(&'a self) -> HarnessFuture<'a, Vec<McpToolDescriptor>> {
        Box::pin(async move {
            let mut session = self.session.lock().await;
            let service = self.ensure_connected(&mut session).await?;
            let result = timeout(self.config.request_timeout, list_tools_bounded(service)).await;
            let tools = match result {
                Ok(Ok(tools)) => tools,
                Ok(Err(error)) => {
                    Self::invalidate(&mut session);
                    return Err(error);
                }
                Err(_) => {
                    Self::invalidate(&mut session);
                    return Err(HarnessError::Mcp("MCP tools/list timed out".to_owned()));
                }
            };
            Ok(tools)
        })
    }

    fn call_tool<'a>(&'a self, name: &'a str, arguments: Value) -> HarnessFuture<'a, Value> {
        Box::pin(async move {
            validate_mcp_tool_name(name)?;
            crate::json::validate_value_shape(&arguments).map_err(|_| {
                HarnessError::Mcp(
                    "MCP tool arguments exceed the supported JSON depth or node count".to_owned(),
                )
            })?;
            let arguments = match arguments {
                Value::Object(arguments) => {
                    crate::json::bounded_serialized_size(&arguments, MAX_MCP_TOOL_ARGUMENT_BYTES)
                        .map_err(|error| match error {
                        crate::json::BoundedJsonError::LimitExceeded => HarnessError::Mcp(format!(
                            "MCP tool arguments exceed {MAX_MCP_TOOL_ARGUMENT_BYTES} bytes"
                        )),
                        crate::json::BoundedJsonError::CannotEncode => {
                            HarnessError::Mcp("cannot encode MCP tool arguments".to_owned())
                        }
                    })?;
                    arguments
                }
                _ => {
                    return Err(HarnessError::Mcp(
                        "MCP tool arguments must be a JSON object".to_owned(),
                    ));
                }
            };
            let mut session = self.session.lock().await;
            let service = self.ensure_connected(&mut session).await?;
            let result = timeout(
                self.config.request_timeout,
                service.call_tool(
                    CallToolRequestParams::new(name.to_owned()).with_arguments(arguments),
                ),
            )
            .await;
            let result = match result {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => {
                    Self::invalidate(&mut session);
                    return Err(HarnessError::Mcp(format!("MCP tool {name} failed")));
                }
                Err(_) => {
                    Self::invalidate(&mut session);
                    return Err(HarnessError::Mcp(format!("MCP tool {name} timed out")));
                }
            };
            tool_result_value(name, result)
        })
    }
}

pub(super) async fn list_tools_bounded(
    service: &RunningService<RoleClient, ()>,
) -> Result<Vec<McpToolDescriptor>, HarnessError> {
    let mut tools = Vec::new();
    let mut names = BTreeSet::new();
    let mut cursors = BTreeSet::new();
    let mut cursor = None;
    let mut retained_bytes = 0_usize;

    for _ in 0..MAX_MCP_TOOL_PAGES {
        let result = service
            .list_tools(Some(PaginatedRequestParams::default().with_cursor(cursor)))
            .await
            .map_err(|_| HarnessError::Mcp("MCP tools/list failed".to_owned()))?;
        if tools.len().saturating_add(result.tools.len()) > MAX_MCP_TOOLS {
            return Err(HarnessError::Mcp(format!(
                "MCP tool catalog exceeds {MAX_MCP_TOOLS} tools"
            )));
        }
        for tool in result.tools {
            let descriptor = mcp_tool_descriptor(tool)?;
            if !names.insert(descriptor.name.clone()) {
                return Err(HarnessError::Mcp(format!(
                    "MCP tool catalog contains duplicate {}",
                    descriptor.name
                )));
            }
            let encoded =
                crate::json::bounded_serialized_size(&descriptor, MAX_MCP_TOOL_CATALOG_BYTES)
                    .map_err(|error| match error {
                        crate::json::BoundedJsonError::LimitExceeded => HarnessError::Mcp(format!(
                            "MCP tool descriptor exceeds {MAX_MCP_TOOL_CATALOG_BYTES} bytes"
                        )),
                        crate::json::BoundedJsonError::CannotEncode => {
                            HarnessError::Mcp("cannot encode MCP tool descriptor".to_owned())
                        }
                    })?;
            retained_bytes = retained_bytes
                .checked_add(encoded)
                .ok_or_else(|| HarnessError::Mcp("MCP tool catalog size overflow".to_owned()))?;
            if retained_bytes > MAX_MCP_TOOL_CATALOG_BYTES {
                return Err(HarnessError::Mcp(format!(
                    "MCP tool catalog exceeds {MAX_MCP_TOOL_CATALOG_BYTES} bytes"
                )));
            }
            tools.push(descriptor);
        }

        let Some(next_cursor) = result.next_cursor else {
            return Ok(tools);
        };
        validate_mcp_cursor(&next_cursor)?;
        if !cursors.insert(next_cursor.clone()) {
            return Err(HarnessError::Mcp(
                "MCP tools/list repeated a pagination cursor".to_owned(),
            ));
        }
        cursor = Some(next_cursor);
    }
    Err(HarnessError::Mcp(format!(
        "MCP tools/list exceeds {MAX_MCP_TOOL_PAGES} pages"
    )))
}

fn mcp_tool_descriptor(tool: RmcpTool) -> Result<McpToolDescriptor, HarnessError> {
    let name = tool.name.into_owned();
    validate_mcp_tool_name(&name)?;
    let description = tool.description.map(|value| value.into_owned());
    if description
        .as_deref()
        .is_some_and(|value| value.len() > MAX_MCP_TOOL_DESCRIPTION_BYTES)
    {
        return Err(HarnessError::Mcp(format!(
            "MCP tool description exceeds {MAX_MCP_TOOL_DESCRIPTION_BYTES} bytes"
        )));
    }
    let descriptor = McpToolDescriptor {
        name,
        description,
        input_schema: Value::Object((*tool.input_schema).clone()),
    };
    crate::json::validate_value_shape(&descriptor.input_schema).map_err(|_| {
        HarnessError::Mcp(
            "MCP tool schema exceeds the supported JSON depth or node count".to_owned(),
        )
    })?;
    Ok(descriptor)
}

pub(super) fn tool_result_value(name: &str, result: CallToolResult) -> Result<Value, HarnessError> {
    let is_error = result.is_error.unwrap_or(false);
    if is_error {
        return Err(HarnessError::Mcp(format!(
            "MCP tool {name} returned an execution error"
        )));
    }
    let value = if let Some(structured) = result.structured_content {
        structured
    } else {
        let texts = result
            .content
            .iter()
            .filter_map(|content| content.as_text().map(|text| text.text.as_str()))
            .collect::<Vec<_>>();
        match texts.as_slice() {
            [] => Value::Null,
            [text] => {
                serde_json::from_str(text).unwrap_or_else(|_| Value::String((*text).to_owned()))
            }
            _ => Value::Array(
                texts
                    .into_iter()
                    .map(|text| Value::String(text.to_owned()))
                    .collect(),
            ),
        }
    };
    crate::json::validate_value_shape(&value).map_err(|_| {
        HarnessError::Mcp(
            "MCP tool result exceeds the supported JSON depth or node count".to_owned(),
        )
    })?;
    crate::json::bounded_serialized_size(&value, MAX_MCP_TOOL_RESULT_BYTES).map_err(|error| {
        match error {
            crate::json::BoundedJsonError::LimitExceeded => HarnessError::Mcp(format!(
                "MCP tool result exceeds {MAX_MCP_TOOL_RESULT_BYTES} bytes"
            )),
            crate::json::BoundedJsonError::CannotEncode => {
                HarnessError::Mcp("cannot encode MCP tool result".to_owned())
            }
        }
    })?;
    Ok(value)
}

fn validate_stdio_config(config: &StdioMcpConfig) -> Result<(), HarnessError> {
    if !config.command.is_absolute() {
        return Err(HarnessError::Mcp(
            "stdio MCP command must be an absolute path".to_owned(),
        ));
    }
    if !config.current_dir.is_absolute() {
        return Err(HarnessError::Mcp(
            "stdio MCP working directory must be absolute".to_owned(),
        ));
    }
    if config.args.len() > MAX_MCP_ARGUMENTS
        || config
            .args
            .iter()
            .any(|argument| argument.len() > MAX_MCP_ARGUMENT_BYTES || argument.contains('\0'))
    {
        return Err(HarnessError::Mcp(format!(
            "stdio MCP args must contain at most {MAX_MCP_ARGUMENTS} values of at most {MAX_MCP_ARGUMENT_BYTES} bytes without NUL"
        )));
    }
    if config.env.len() > MAX_MCP_ENVIRONMENT_ENTRIES {
        return Err(HarnessError::Mcp(format!(
            "stdio MCP environment exceeds {MAX_MCP_ENVIRONMENT_ENTRIES} entries"
        )));
    }
    let environment_bytes = config
        .env
        .iter()
        .try_fold(0_usize, |total, (name, value)| {
            if !valid_environment_name(name) || value.contains('\0') {
                return Err(HarnessError::Mcp(format!(
                    "invalid stdio MCP environment entry {name:?}"
                )));
            }
            total
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| HarnessError::Mcp("stdio MCP environment size overflow".to_owned()))
        })?;
    if environment_bytes > MAX_MCP_ENVIRONMENT_BYTES {
        return Err(HarnessError::Mcp(format!(
            "stdio MCP environment exceeds {MAX_MCP_ENVIRONMENT_BYTES} bytes"
        )));
    }
    if config.request_timeout < Duration::from_millis(1) || config.request_timeout > MAX_MCP_TIMEOUT
    {
        return Err(HarnessError::Mcp(format!(
            "stdio MCP request timeout must be 1 millisecond to {} seconds",
            MAX_MCP_TIMEOUT.as_secs()
        )));
    }
    Ok(())
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

pub(super) fn validate_mcp_tool_name(name: &str) -> Result<(), HarnessError> {
    if name.is_empty() || name.len() > MAX_MCP_TOOL_NAME_BYTES || name.chars().any(char::is_control)
    {
        return Err(HarnessError::Mcp(format!(
            "MCP tool name must be 1-{MAX_MCP_TOOL_NAME_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn validate_mcp_cursor(cursor: &str) -> Result<(), HarnessError> {
    if cursor.is_empty()
        || cursor.len() > MAX_MCP_CURSOR_BYTES
        || cursor.chars().any(char::is_control)
    {
        return Err(HarnessError::Mcp(format!(
            "MCP cursor must be 1-{MAX_MCP_CURSOR_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

#[must_use]
/// Boxes a concrete client behind the adapter-facing MCP port.
pub fn mcp_client(client: impl McpClient + 'static) -> Arc<dyn McpClient> {
    Arc::new(client)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use rmcp::model::{CallToolResult, ContentBlock};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{
        BoundedLineReader, MAX_MCP_PROCESS_CONCURRENCY, McpClient, McpToolDescriptor,
        StdioMcpClient, StdioMcpConfig, StdioMcpLaunchAuthority, register_mcp_tools,
        register_selected_mcp_tools, tool_result_value,
    };
    use crate::{
        AllowListPolicy, CapabilityOrigin, HarnessError, HarnessFuture, HarnessRuntime,
        LanguageModel, MemoryEventStore, ModelOutput, ModelRequest, StateEngine, ToolRegistry,
    };

    struct FakeMcpClient {
        tools: Vec<McpToolDescriptor>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl McpClient for FakeMcpClient {
        fn list_tools<'a>(&'a self) -> HarnessFuture<'a, Vec<McpToolDescriptor>> {
            Box::pin(async move { Ok(self.tools.clone()) })
        }

        fn call_tool<'a>(&'a self, name: &'a str, arguments: Value) -> HarnessFuture<'a, Value> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|_| HarnessError::Mcp("call recorder poisoned".to_owned()))?
                    .push(name.to_owned());
                Ok(arguments)
            })
        }
    }

    struct McpCallingModel;

    impl LanguageModel for McpCallingModel {
        fn id(&self) -> &str {
            "test/mcp-calling"
        }

        fn complete<'a>(&'a self, request: ModelRequest) -> HarnessFuture<'a, ModelOutput> {
            Box::pin(async move {
                if request
                    .items
                    .iter()
                    .any(|item| matches!(&item.kind, crate::ItemKind::ToolResult { .. }))
                {
                    Ok(ModelOutput::Message {
                        content: "MCP result observed".to_owned(),
                    })
                } else {
                    Ok(ModelOutput::ToolCall {
                        call_id: "call-mcp".to_owned(),
                        name: "demo.echo".to_owned(),
                        input: json!({"text": "hello"}),
                    })
                }
            })
        }
    }

    fn config(command: PathBuf) -> StdioMcpConfig {
        StdioMcpConfig {
            command,
            args: Vec::new(),
            env: BTreeMap::new(),
            current_dir: std::env::temp_dir(),
            request_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn stdio_config_requires_explicit_bounded_process_authority() {
        assert!(
            StdioMcpClient::new(
                config(PathBuf::from("mcp-server")),
                StdioMcpLaunchAuthority::denied(),
            )
            .is_err()
        );

        let mut invalid = config(std::env::temp_dir().join("mcp-server"));
        invalid
            .env
            .insert("BAD=NAME".to_owned(), "value".to_owned());
        assert!(StdioMcpClient::new(invalid, StdioMcpLaunchAuthority::denied()).is_err());

        let mut invalid = config(std::env::temp_dir().join("mcp-server"));
        invalid.request_timeout = Duration::MAX;
        assert!(StdioMcpClient::new(invalid, StdioMcpLaunchAuthority::denied()).is_err());

        let mut invalid = config(std::env::temp_dir().join("mcp-server"));
        invalid.current_dir = PathBuf::from("relative");
        assert!(StdioMcpClient::new(invalid, StdioMcpLaunchAuthority::denied()).is_err());
    }

    #[test]
    fn stdio_launch_authority_defaults_to_deny_and_bounds_unrestricted_mode() {
        assert_eq!(
            StdioMcpLaunchAuthority::default().descriptor().isolation,
            crate::ProcessIsolation::Denied
        );
        assert!(StdioMcpLaunchAuthority::unrestricted(0).is_err());
        assert!(StdioMcpLaunchAuthority::unrestricted(MAX_MCP_PROCESS_CONCURRENCY + 1).is_err());
        assert_eq!(
            StdioMcpLaunchAuthority::unrestricted(2)
                .expect("bounded unrestricted authority")
                .descriptor()
                .isolation,
            crate::ProcessIsolation::Unrestricted
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn stdio_launch_authority_reuses_the_scoped_seatbelt_policy() {
        let authority = StdioMcpLaunchAuthority::macos_seatbelt(
            1,
            vec![std::env::temp_dir()],
            crate::NetworkAccess::Deny,
        )
        .expect("Seatbelt authority");
        assert_eq!(
            authority.descriptor().isolation,
            crate::ProcessIsolation::Sandboxed {
                mechanism: "macos-seatbelt-write-network".to_owned(),
            }
        );
        let (program, args) = authority
            .prepare_command(&config(PathBuf::from("/usr/bin/true")))
            .expect("wrapped command");
        assert_eq!(program, PathBuf::from("/usr/bin/sandbox-exec"));
        let profile = args
            .windows(2)
            .find(|pair| pair[0] == "-p")
            .map(|pair| pair[1].as_str())
            .expect("Seatbelt profile");
        assert!(profile.contains("(deny network*)"));
        assert!(profile.contains("(literal \"/dev/null\")"));
        assert!(profile.contains("(subpath (param \"YH_WRITE_0\"))"));
        assert_eq!(args.last().map(String::as_str), Some("/usr/bin/true"));
    }

    #[tokio::test]
    async fn stdio_launch_authority_holds_a_permit_for_each_live_session() {
        let authority =
            StdioMcpLaunchAuthority::unrestricted(1).expect("bounded unrestricted authority");
        let permit = authority
            .acquire(Duration::from_secs(1))
            .await
            .expect("first permit");
        let error = authority
            .acquire(Duration::from_millis(1))
            .await
            .expect_err("second permit must be bounded");
        assert!(error.to_string().contains("queue timed out"));
        drop(permit);
        let _released = authority
            .acquire(Duration::from_secs(1))
            .await
            .expect("released permit");
    }

    #[tokio::test]
    async fn denied_stdio_authority_fails_before_process_start() {
        let client = StdioMcpClient::new(
            config(std::env::temp_dir().join("nonexistent-mcp-server")),
            StdioMcpLaunchAuthority::denied(),
        )
        .expect("valid denied client");
        assert_eq!(
            client.launch_descriptor().isolation,
            crate::ProcessIsolation::Denied
        );

        let error = client
            .call_tool("fixture", json!({}))
            .await
            .expect_err("launch must be denied");

        assert!(error.to_string().contains("execution is disabled"));
        assert!(!error.to_string().contains("failed to start"));
    }

    #[test]
    fn stdio_config_debug_redacts_arguments_and_environment_values() {
        let mut value = config(std::env::temp_dir().join("mcp-server"));
        value.args.push("sensitive-argument".to_owned());
        value
            .env
            .insert("ACCESS_TOKEN".to_owned(), "sensitive-value".to_owned());
        let debug = format!("{value:?}");
        assert!(debug.contains("ACCESS_TOKEN"));
        assert!(!debug.contains("sensitive-argument"));
        assert!(!debug.contains("sensitive-value"));
    }

    #[tokio::test]
    async fn stdio_client_rejects_deep_arguments_before_process_start() {
        #[cfg(windows)]
        let command = PathBuf::from(r"C:\nonexistent\y-harness-mcp-server.exe");
        #[cfg(not(windows))]
        let command = PathBuf::from("/nonexistent/y-harness-mcp-server");
        let client = StdioMcpClient::new(config(command), StdioMcpLaunchAuthority::denied())
            .expect("valid bounded config");
        let mut nested = Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            nested = Value::Array(vec![nested]);
        }
        let error = client
            .call_tool("fixture", json!({"nested": nested}))
            .await
            .expect_err("deep arguments");
        assert!(error.to_string().contains("depth or node count"));
        assert!(!error.to_string().contains("nonexistent"));
    }

    #[tokio::test]
    async fn line_reader_rejects_oversize_and_resets_at_newline() {
        let (mut writer, reader) = tokio::io::duplex(64);
        writer
            .write_all(b"1234\n1234\n")
            .await
            .expect("write bounded lines");
        drop(writer);
        let mut bounded = BoundedLineReader::new(reader, 4);
        let mut bytes = Vec::new();
        bounded
            .read_to_end(&mut bytes)
            .await
            .expect("two bounded lines");
        assert_eq!(bytes, b"1234\n1234\n");

        let (mut writer, reader) = tokio::io::duplex(64);
        writer
            .write_all(b"12345\n")
            .await
            .expect("write oversized line");
        drop(writer);
        let mut bounded = BoundedLineReader::new(reader, 4);
        let mut bytes = Vec::new();
        let error = bounded
            .read_to_end(&mut bytes)
            .await
            .expect_err("oversized line");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn tool_error_content_never_enters_client_error() {
        let secret = "sensitive-provider-detail";
        let error = tool_result_value(
            "fixture",
            CallToolResult::error(vec![ContentBlock::text(secret)]),
        )
        .expect_err("tool error");
        assert!(!error.to_string().contains(secret));
        assert!(error.to_string().contains("execution error"));
    }

    #[tokio::test]
    async fn discovered_mcp_tools_register_atomically_and_run_through_the_agent_loop() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let client: Arc<dyn McpClient> = Arc::new(FakeMcpClient {
            tools: vec![McpToolDescriptor {
                name: "echo".to_owned(),
                description: Some("Return its input".to_owned()),
                input_schema: json!({"type": "object"}),
            }],
            calls: calls.clone(),
        });
        let origin = CapabilityOrigin::External {
            id: "mcp/demo".to_owned(),
        };
        let mut tools = ToolRegistry::new();
        assert_eq!(
            register_mcp_tools(&mut tools, origin.clone(), "demo", client)
                .await
                .expect("register MCP catalog"),
            ["demo.echo"]
        );
        assert_eq!(
            tools.get("demo.echo").map(|tool| &tool.origin),
            Some(&origin)
        );

        let runtime = HarnessRuntime::new(
            Arc::new(McpCallingModel),
            tools,
            Arc::new(AllowListPolicy::deny_by_default().allow("demo.echo")),
            StateEngine::new(Arc::new(MemoryEventStore::new())),
        );
        let thread = runtime.create_thread().await.expect("thread");
        let outcome = runtime
            .run_turn(&thread.id, "use MCP")
            .await
            .expect("MCP-backed Turn");
        assert_eq!(outcome.final_text, "MCP result observed");
        assert_eq!(calls.lock().expect("recorded calls").as_slice(), ["echo"]);

        let invalid: Arc<dyn McpClient> = Arc::new(FakeMcpClient {
            tools: vec![
                McpToolDescriptor {
                    name: "valid".to_owned(),
                    description: None,
                    input_schema: json!({"type": "object"}),
                },
                McpToolDescriptor {
                    name: "INVALID".to_owned(),
                    description: None,
                    input_schema: json!({"type": "object"}),
                },
            ],
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let mut empty = ToolRegistry::new();
        assert!(matches!(
            register_mcp_tools(&mut empty, origin, "demo", invalid).await,
            Err(HarnessError::InvalidCapability(_))
        ));
        assert!(empty.descriptors().is_empty());
    }

    #[tokio::test]
    async fn direct_mcp_catalog_rejects_deep_schema_before_registration() {
        let mut deeply_nested = Value::Null;
        for _ in 0..=crate::json::MAX_JSON_DEPTH {
            deeply_nested = Value::Array(vec![deeply_nested]);
        }
        let client: Arc<dyn McpClient> = Arc::new(FakeMcpClient {
            tools: vec![McpToolDescriptor {
                name: "deep".to_owned(),
                description: Some("Invalid deep schema".to_owned()),
                input_schema: json!({"nested": deeply_nested}),
            }],
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let mut tools = ToolRegistry::new();

        let error = register_mcp_tools(&mut tools, CapabilityOrigin::BuiltIn, "fixture", client)
            .await
            .expect_err("deep MCP schema");

        assert!(error.to_string().contains("depth or node count"));
        assert!(tools.descriptors().is_empty());
    }

    #[tokio::test]
    async fn selected_mcp_registration_is_exact_and_atomic() {
        let client: Arc<dyn McpClient> = Arc::new(FakeMcpClient {
            tools: vec![
                McpToolDescriptor {
                    name: "read".to_owned(),
                    description: Some("Read".to_owned()),
                    input_schema: json!({"type": "object"}),
                },
                McpToolDescriptor {
                    name: "write".to_owned(),
                    description: Some("Write".to_owned()),
                    input_schema: json!({"type": "object"}),
                },
            ],
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let mut tools = ToolRegistry::new();
        assert_eq!(
            register_selected_mcp_tools(
                &mut tools,
                CapabilityOrigin::BuiltIn,
                "files",
                client,
                &["read".to_owned()],
            )
            .await
            .expect("selected tools"),
            ["files.read"]
        );
        assert!(tools.get("files.write").is_none());

        let missing: Arc<dyn McpClient> = Arc::new(FakeMcpClient {
            tools: vec![McpToolDescriptor {
                name: "read".to_owned(),
                description: Some("Read".to_owned()),
                input_schema: json!({"type": "object"}),
            }],
            calls: Arc::new(Mutex::new(Vec::new())),
        });
        let mut empty = ToolRegistry::new();
        assert!(
            register_selected_mcp_tools(
                &mut empty,
                CapabilityOrigin::BuiltIn,
                "files",
                missing,
                &["missing".to_owned()],
            )
            .await
            .is_err()
        );
        assert!(empty.descriptors().is_empty());
    }
}
