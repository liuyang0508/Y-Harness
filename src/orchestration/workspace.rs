//! Governed provisioning and cleanup of Task workspaces.

use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs;

use super::WorkspaceMode;
use crate::{
    CancellationToken, EventId, ExecutionPhase, HarnessError, HarnessFuture, ProcessBroker,
    ProcessIsolation, ProcessRequest, TaskGraphId, TaskId, TaskLeaseId,
    kernel::{capture_capability_metadata, validate_capability_name},
};

/// Exact embedded Workspace Provider contract version.
pub const WORKSPACE_PROVIDER_API_VERSION: &str = "1";

const LOCAL_PROVIDER_NAME: &str = "local-directory";
const GIT_PROVIDER_NAME: &str = "git-worktree";
const DENY_PROVIDER_NAME: &str = "deny-workspace";
const MAX_CLEANUP_TOKEN_BYTES: usize = 4_096;
const MAX_GIT_REVISION_BYTES: usize = 64;
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(60);
const GIT_OUTPUT_BYTES: usize = 65_536;
const WORKSPACE_MARKER: &str = ".y-harness-workspace";

/// Honest provisioning mechanism reported by a Workspace Provider.
///
/// This describes path provisioning, not operating-system sandbox strength.
/// A Process Broker must separately enforce write and network authority for an
/// untrusted executor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceProvisioning {
    /// Filesystem workspace requests are rejected.
    Denied,
    /// A provider allocates a dedicated directory.
    Directory,
    /// A provider allocates a detached Git Worktree.
    GitWorktree,
}

/// Frozen identity and behavior class of a Workspace Provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkspaceProviderDescriptor {
    /// Stable capability identity.
    pub name: String,
    /// Concrete path-provisioning mechanism.
    pub provisioning: WorkspaceProvisioning,
}

/// Exact Task attempt presented to a Workspace Provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceRequest {
    /// Owning Task Graph.
    pub graph_id: TaskGraphId,
    /// Task receiving the workspace.
    pub task_id: TaskId,
    /// Exact fencing lease.
    pub lease_id: TaskLeaseId,
    /// Monotonic Task attempt.
    pub attempt: u32,
    /// Requested filesystem mode.
    pub mode: WorkspaceMode,
}

/// Read-only workspace view supplied to a Task executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskWorkspace {
    provider: String,
    mode: WorkspaceMode,
    root: Option<PathBuf>,
}

impl TaskWorkspace {
    /// Returns the frozen Workspace Provider identity.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the requested access mode.
    #[must_use]
    pub fn mode(&self) -> WorkspaceMode {
        self.mode
    }

    /// Returns the canonical workspace root, when a filesystem path exists.
    #[must_use]
    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }
}

/// Terminal reason supplied to idempotent workspace cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceDisposition {
    /// Executor returned a valid completion.
    Completed,
    /// Executor or workspace preparation failed.
    Failed,
    /// Scheduler cancellation or fencing stopped execution.
    Cancelled,
    /// The total prepare-and-execute deadline elapsed.
    TimedOut,
}

/// Provider-owned allocation retained by the Orchestrator until cleanup.
///
/// The executor receives only [`TaskWorkspace`], so it cannot forge or replay a
/// cleanup request. External providers construct a lease with [`Self::new`];
/// the Orchestrator validates it against the frozen provider descriptor and
/// exact Task attempt before execution.
pub struct WorkspaceLease {
    request: WorkspaceRequest,
    provider: String,
    root: Option<PathBuf>,
    cleanup_token: String,
}

impl WorkspaceLease {
    /// Constructs a provider allocation with one bounded opaque cleanup token.
    pub fn new(
        request: WorkspaceRequest,
        provider: impl Into<String>,
        root: Option<PathBuf>,
        cleanup_token: impl Into<String>,
    ) -> Result<Self, HarnessError> {
        let provider = provider.into();
        validate_capability_name("workspace provider", &provider)?;
        let cleanup_token = cleanup_token.into();
        validate_cleanup_token(&cleanup_token)?;
        match (&request.mode, &root) {
            (WorkspaceMode::None, None) => {}
            (WorkspaceMode::None, Some(_)) => {
                return Err(HarnessError::InvalidCapability(
                    "WorkspaceMode::None cannot carry a filesystem root".to_owned(),
                ));
            }
            (WorkspaceMode::Isolated | WorkspaceMode::SharedReadOnly, Some(path))
                if path.is_absolute() => {}
            (WorkspaceMode::Isolated | WorkspaceMode::SharedReadOnly, Some(_)) => {
                return Err(HarnessError::InvalidCapability(
                    "workspace root must be absolute".to_owned(),
                ));
            }
            (WorkspaceMode::Isolated | WorkspaceMode::SharedReadOnly, None) => {
                return Err(HarnessError::InvalidCapability(
                    "filesystem workspace request requires a root".to_owned(),
                ));
            }
        }
        Ok(Self {
            request,
            provider,
            root,
            cleanup_token,
        })
    }

    /// Returns the exact Task attempt that owns this allocation.
    #[must_use]
    pub fn request(&self) -> &WorkspaceRequest {
        &self.request
    }

    /// Returns the provider identity that created this allocation.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the allocated root, when present.
    #[must_use]
    pub fn root(&self) -> Option<&Path> {
        self.root.as_deref()
    }

    /// Returns the opaque token to the provider during cleanup.
    #[must_use]
    pub fn cleanup_token(&self) -> &str {
        &self.cleanup_token
    }

    pub(crate) fn executor_view(&self) -> TaskWorkspace {
        TaskWorkspace {
            provider: self.provider.clone(),
            mode: self.request.mode,
            root: self.root.clone(),
        }
    }
}

impl fmt::Debug for WorkspaceLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceLease")
            .field("request", &self.request)
            .field("provider", &self.provider)
            .field("root", &self.root)
            .field("cleanup_token", &"[REDACTED]")
            .finish()
    }
}

/// Replaceable Task workspace lifecycle boundary.
///
/// `release` must be idempotent for the same lease. Providers must clean up
/// partial allocations if `prepare` returns an error. The Runtime panic-isolates
/// both returned Futures, but provider code remains a trusted extension.
pub trait WorkspaceProvider: Send + Sync {
    /// Reports stable metadata captured when the provider is installed.
    fn descriptor(&self) -> WorkspaceProviderDescriptor;

    /// Provisions one exact Task attempt.
    fn prepare<'a>(
        &'a self,
        request: WorkspaceRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, WorkspaceLease>;

    /// Releases one allocation without depending on executor cooperation.
    fn release<'a>(
        &'a self,
        lease: WorkspaceLease,
        disposition: WorkspaceDisposition,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ()>;
}

/// Secure default that permits only Tasks requesting no filesystem workspace.
pub struct DenyWorkspaceProvider;

impl WorkspaceProvider for DenyWorkspaceProvider {
    fn descriptor(&self) -> WorkspaceProviderDescriptor {
        WorkspaceProviderDescriptor {
            name: DENY_PROVIDER_NAME.to_owned(),
            provisioning: WorkspaceProvisioning::Denied,
        }
    }

    fn prepare<'a>(
        &'a self,
        request: WorkspaceRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, WorkspaceLease> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            if request.mode != WorkspaceMode::None {
                return Err(HarnessError::Execution(
                    "filesystem workspace provisioning is disabled".to_owned(),
                ));
            }
            WorkspaceLease::new(request, DENY_PROVIDER_NAME, None, "none")
        })
    }

    fn release<'a>(
        &'a self,
        lease: WorkspaceLease,
        _disposition: WorkspaceDisposition,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            require_provider_lease(&lease, DENY_PROVIDER_NAME)?;
            Ok(())
        })
    }
}

/// Local ephemeral directories for trusted in-process or sandboxed executors.
///
/// Isolated allocations are direct children of one canonical managed root and
/// carry a private marker checked before recursive cleanup. An optional shared
/// root can satisfy `SharedReadOnly`; the provider does not itself prevent a
/// trusted executor from writing, so an untrusted process still requires a
/// read-only sandbox policy.
pub struct LocalDirectoryWorkspaceProvider {
    root: PathBuf,
    shared_read_only: Option<PathBuf>,
}

impl LocalDirectoryWorkspaceProvider {
    /// Creates or opens one managed workspace root.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, HarnessError> {
        Ok(Self {
            root: prepare_managed_root(root.into(), "local workspace root")?,
            shared_read_only: None,
        })
    }

    /// Adds one existing canonical directory for shared read-only Tasks.
    pub fn with_shared_read_only(mut self, root: impl Into<PathBuf>) -> Result<Self, HarnessError> {
        self.shared_read_only = Some(require_existing_directory(
            root.into(),
            "shared read-only workspace root",
        )?);
        Ok(self)
    }

    /// Returns the canonical managed root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl WorkspaceProvider for LocalDirectoryWorkspaceProvider {
    fn descriptor(&self) -> WorkspaceProviderDescriptor {
        WorkspaceProviderDescriptor {
            name: LOCAL_PROVIDER_NAME.to_owned(),
            provisioning: WorkspaceProvisioning::Directory,
        }
    }

    fn prepare<'a>(
        &'a self,
        request: WorkspaceRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, WorkspaceLease> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            match request.mode {
                WorkspaceMode::None => {
                    WorkspaceLease::new(request, LOCAL_PROVIDER_NAME, None, "none")
                }
                WorkspaceMode::SharedReadOnly => {
                    let root = self.shared_read_only.clone().ok_or_else(|| {
                        HarnessError::Execution(
                            "shared read-only workspace root is not configured".to_owned(),
                        )
                    })?;
                    WorkspaceLease::new(request, LOCAL_PROVIDER_NAME, Some(root), "shared")
                }
                WorkspaceMode::Isolated => {
                    let cleanup_token = allocation_token(&request)?;
                    let name = allocation_name("yh-dir", &cleanup_token);
                    let container = self.root.join(&name);
                    if fs::symlink_metadata(&container).await.is_ok() {
                        return Err(HarnessError::Execution(
                            "workspace allocation identity already exists".to_owned(),
                        ));
                    }
                    fs::create_dir(&container).await.map_err(|error| {
                        HarnessError::Execution(format!(
                            "failed to create isolated workspace: {error}"
                        ))
                    })?;
                    let mut allocation = LocalAllocationGuard::new(container.clone());
                    let path = container.join("workspace");
                    initialize_managed_container(&container, &path, &cleanup_token, &cancellation)
                        .await?;
                    let lease = WorkspaceLease::new(
                        request,
                        LOCAL_PROVIDER_NAME,
                        Some(path),
                        cleanup_token,
                    )?;
                    allocation.disarm();
                    Ok(lease)
                }
            }
        })
    }

    fn release<'a>(
        &'a self,
        lease: WorkspaceLease,
        _disposition: WorkspaceDisposition,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            require_provider_lease(&lease, LOCAL_PROVIDER_NAME)?;
            if lease.request.mode != WorkspaceMode::Isolated {
                return Ok(());
            }
            let path = lease.root.as_deref().ok_or_else(|| {
                HarnessError::Execution("isolated workspace lease lost its root".to_owned())
            })?;
            let container = require_managed_nested_child(&self.root, path, "yh-dir-", "workspace")?;
            let metadata = match fs::symlink_metadata(container).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => {
                    return Err(HarnessError::Execution(format!(
                        "failed to inspect isolated workspace: {error}"
                    )));
                }
            };
            if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                return Err(HarnessError::Execution(
                    "isolated workspace was replaced before cleanup".to_owned(),
                ));
            }
            let marker = read_bounded_marker(&container.join(WORKSPACE_MARKER)).await?;
            if marker != lease.cleanup_token {
                return Err(HarnessError::Execution(
                    "isolated workspace marker does not match its cleanup lease".to_owned(),
                ));
            }
            require_not_cancelled(&cancellation)?;
            fs::remove_dir_all(container).await.map_err(|error| {
                HarnessError::Execution(format!("failed to remove isolated workspace: {error}"))
            })
        })
    }
}

struct LocalAllocationGuard {
    path: PathBuf,
    armed: bool,
}

impl LocalAllocationGuard {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LocalAllocationGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

/// Detached Git Worktrees provisioned through an explicit Process Broker.
///
/// The revision must be a full 40- or 64-character hexadecimal object ID. No
/// branch is created, inherited environment is cleared by the broker, and no
/// shell is invoked.
pub struct GitWorktreeWorkspaceProvider {
    repository_root: PathBuf,
    workspace_root: PathBuf,
    revision: String,
    git_program: PathBuf,
    broker: Arc<dyn ProcessBroker>,
}

impl GitWorktreeWorkspaceProvider {
    /// Configures pinned detached Worktrees under one managed root.
    pub fn new(
        repository_root: impl Into<PathBuf>,
        workspace_root: impl Into<PathBuf>,
        revision: impl Into<String>,
        git_program: impl Into<PathBuf>,
        broker: Arc<dyn ProcessBroker>,
    ) -> Result<Self, HarnessError> {
        let repository_root =
            require_existing_directory(repository_root.into(), "Git repository root")?;
        let workspace_root = prepare_managed_root(workspace_root.into(), "Git Worktree root")?;
        if repository_root.starts_with(&workspace_root)
            || workspace_root.starts_with(&repository_root)
        {
            return Err(HarnessError::InvalidConfiguration(
                "Git repository and Worktree roots must not contain one another".to_owned(),
            ));
        }
        let revision = revision.into();
        if !matches!(revision.len(), 40 | MAX_GIT_REVISION_BYTES)
            || !revision.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(HarnessError::InvalidConfiguration(
                "Git Worktree revision must be a full 40- or 64-character hexadecimal object ID"
                    .to_owned(),
            ));
        }
        let git_program = require_existing_file(git_program.into(), "Git executable")?;
        if git_program.to_str().is_none()
            || repository_root.to_str().is_none()
            || workspace_root.to_str().is_none()
        {
            return Err(HarnessError::InvalidConfiguration(
                "Git Workspace paths must be valid UTF-8".to_owned(),
            ));
        }
        let broker_descriptor =
            capture_capability_metadata("process broker descriptor", || broker.descriptor())?;
        validate_capability_name("process broker", &broker_descriptor.name)?;
        if broker_descriptor.isolation == ProcessIsolation::Denied {
            return Err(HarnessError::InvalidConfiguration(
                "Git Worktree provider requires an enabled Process Broker".to_owned(),
            ));
        }
        Ok(Self {
            repository_root,
            workspace_root,
            revision: revision.to_ascii_lowercase(),
            git_program,
            broker,
        })
    }

    /// Returns the canonical source repository root.
    #[must_use]
    pub fn repository_root(&self) -> &Path {
        &self.repository_root
    }

    /// Returns the canonical managed Worktree root.
    #[must_use]
    pub fn workspace_root(&self) -> &Path {
        &self.workspace_root
    }

    async fn git(
        &self,
        args: Vec<String>,
        cancellation: CancellationToken,
    ) -> Result<(), HarnessError> {
        let output = self
            .broker
            .execute(
                ProcessRequest {
                    program: self.git_program.clone(),
                    args,
                    current_dir: self.repository_root.clone(),
                    environment: BTreeMap::new(),
                    stdin: Vec::new(),
                    timeout: GIT_COMMAND_TIMEOUT,
                    max_output_bytes: GIT_OUTPUT_BYTES,
                    cancellation_phase: ExecutionPhase::Tool,
                },
                cancellation,
            )
            .await
            .map_err(|error| {
                HarnessError::Execution(format!("Git workspace command failed: {error}"))
            })?;
        if output.stdout_truncated || output.stderr_truncated {
            return Err(HarnessError::Execution(
                "Git workspace command exceeded its output boundary".to_owned(),
            ));
        }
        if !output.success {
            return Err(HarnessError::Execution(match output.code {
                Some(code) => format!("Git workspace command exited with code {code}"),
                None => "Git workspace command terminated without an exit code".to_owned(),
            }));
        }
        Ok(())
    }

    fn add_args(&self, path: &Path) -> Result<Vec<String>, HarnessError> {
        Ok(vec![
            "-C".to_owned(),
            path_text(&self.repository_root)?.to_owned(),
            "worktree".to_owned(),
            "add".to_owned(),
            "--detach".to_owned(),
            path_text(path)?.to_owned(),
            self.revision.clone(),
        ])
    }

    fn remove_args(&self, path: &Path) -> Result<Vec<String>, HarnessError> {
        Ok(vec![
            "-C".to_owned(),
            path_text(&self.repository_root)?.to_owned(),
            "worktree".to_owned(),
            "remove".to_owned(),
            "--force".to_owned(),
            path_text(path)?.to_owned(),
        ])
    }
}

impl WorkspaceProvider for GitWorktreeWorkspaceProvider {
    fn descriptor(&self) -> WorkspaceProviderDescriptor {
        WorkspaceProviderDescriptor {
            name: GIT_PROVIDER_NAME.to_owned(),
            provisioning: WorkspaceProvisioning::GitWorktree,
        }
    }

    fn prepare<'a>(
        &'a self,
        request: WorkspaceRequest,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, WorkspaceLease> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            match request.mode {
                WorkspaceMode::None => {
                    WorkspaceLease::new(request, GIT_PROVIDER_NAME, None, "none")
                }
                WorkspaceMode::SharedReadOnly => WorkspaceLease::new(
                    request,
                    GIT_PROVIDER_NAME,
                    Some(self.repository_root.clone()),
                    "shared",
                ),
                WorkspaceMode::Isolated => {
                    let cleanup_token = allocation_token(&request)?;
                    let name = allocation_name("yh-git", &cleanup_token);
                    let container = self.workspace_root.join(name);
                    if fs::symlink_metadata(&container).await.is_ok() {
                        return Err(HarnessError::Execution(
                            "Git Worktree allocation identity already exists".to_owned(),
                        ));
                    }
                    fs::create_dir(&container).await.map_err(|error| {
                        HarnessError::Execution(format!(
                            "failed to create Git Worktree container: {error}"
                        ))
                    })?;
                    let mut allocation = LocalAllocationGuard::new(container.clone());
                    let path = container.join("worktree");
                    initialize_managed_container(&container, &path, &cleanup_token, &cancellation)
                        .await?;
                    fs::remove_dir(&path).await.map_err(|error| {
                        HarnessError::Execution(format!(
                            "failed to prepare empty Git Worktree target: {error}"
                        ))
                    })?;
                    let result = self.git(self.add_args(&path)?, cancellation.clone()).await;
                    if let Err(error) = result {
                        if fs::symlink_metadata(&path).await.is_ok() {
                            let _ = self
                                .git(self.remove_args(&path)?, CancellationToken::new())
                                .await;
                        }
                        return Err(error);
                    }
                    let canonical = fs::canonicalize(&path).await.map_err(|error| {
                        HarnessError::Execution(format!(
                            "cannot canonicalize prepared Git Worktree: {error}"
                        ))
                    })?;
                    if canonical != path {
                        let _ = self
                            .git(self.remove_args(&path)?, CancellationToken::new())
                            .await;
                        return Err(HarnessError::Execution(
                            "prepared Git Worktree escaped its managed root".to_owned(),
                        ));
                    }
                    let lease =
                        WorkspaceLease::new(request, GIT_PROVIDER_NAME, Some(path), cleanup_token)?;
                    allocation.disarm();
                    Ok(lease)
                }
            }
        })
    }

    fn release<'a>(
        &'a self,
        lease: WorkspaceLease,
        _disposition: WorkspaceDisposition,
        cancellation: CancellationToken,
    ) -> HarnessFuture<'a, ()> {
        Box::pin(async move {
            require_not_cancelled(&cancellation)?;
            require_provider_lease(&lease, GIT_PROVIDER_NAME)?;
            if lease.request.mode != WorkspaceMode::Isolated {
                return Ok(());
            }
            let path = lease.root.as_deref().ok_or_else(|| {
                HarnessError::Execution("Git Worktree lease lost its root".to_owned())
            })?;
            let container =
                require_managed_nested_child(&self.workspace_root, path, "yh-git-", "worktree")?;
            let metadata = match fs::symlink_metadata(container).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(error) => {
                    return Err(HarnessError::Execution(format!(
                        "failed to inspect Git Worktree container: {error}"
                    )));
                }
            };
            if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
                return Err(HarnessError::Execution(
                    "Git Worktree container was replaced before cleanup".to_owned(),
                ));
            }
            let marker = read_bounded_marker(&container.join(WORKSPACE_MARKER)).await?;
            if marker != lease.cleanup_token {
                return Err(HarnessError::Execution(
                    "Git Worktree marker does not match its cleanup lease".to_owned(),
                ));
            }
            match fs::symlink_metadata(path).await {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(HarnessError::Execution(format!(
                        "failed to inspect Git Worktree before cleanup: {error}"
                    )));
                }
            }
            if fs::symlink_metadata(path).await.is_ok() {
                self.git(self.remove_args(path)?, cancellation).await?;
            }
            if fs::symlink_metadata(path).await.is_ok() {
                return Err(HarnessError::Execution(
                    "Git Worktree path remained after successful cleanup".to_owned(),
                ));
            }
            fs::remove_dir_all(container).await.map_err(|error| {
                HarnessError::Execution(format!("failed to remove Git Worktree container: {error}"))
            })
        })
    }
}

pub(crate) fn validate_provider_descriptor(
    descriptor: &WorkspaceProviderDescriptor,
) -> Result<(), HarnessError> {
    validate_capability_name("workspace provider", &descriptor.name)
}

pub(crate) async fn validate_workspace_lease(
    lease: &WorkspaceLease,
    request: &WorkspaceRequest,
    descriptor: &WorkspaceProviderDescriptor,
) -> Result<TaskWorkspace, HarnessError> {
    if lease.request != *request || lease.provider != descriptor.name {
        return Err(HarnessError::InvalidCapability(
            "Workspace Provider returned a lease for a different Task attempt or identity"
                .to_owned(),
        ));
    }
    if descriptor.provisioning == WorkspaceProvisioning::Denied
        && request.mode != WorkspaceMode::None
    {
        return Err(HarnessError::InvalidCapability(
            "denied Workspace Provider returned a filesystem allocation".to_owned(),
        ));
    }
    if let Some(path) = lease.root() {
        let metadata = fs::symlink_metadata(path).await.map_err(|error| {
            HarnessError::InvalidCapability(format!(
                "Workspace Provider returned an inaccessible root: {error}"
            ))
        })?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(HarnessError::InvalidCapability(
                "Workspace Provider root must be a real directory".to_owned(),
            ));
        }
        let canonical = fs::canonicalize(path).await.map_err(|error| {
            HarnessError::InvalidCapability(format!(
                "Workspace Provider root cannot be canonicalized: {error}"
            ))
        })?;
        if canonical != path {
            return Err(HarnessError::InvalidCapability(
                "Workspace Provider root must already be canonical".to_owned(),
            ));
        }
    }
    Ok(lease.executor_view())
}

fn allocation_token(request: &WorkspaceRequest) -> Result<String, HarnessError> {
    let token = format!(
        "{}:{}:{}:{}:{}",
        request.graph_id,
        request.task_id,
        request.lease_id,
        request.attempt,
        EventId::generate()
    );
    validate_cleanup_token(&token)?;
    Ok(token)
}

fn allocation_name(prefix: &str, token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut encoded = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    format!("{prefix}-{encoded}")
}

async fn initialize_managed_container(
    container: &Path,
    executor_root: &Path,
    cleanup_token: &str,
    cancellation: &CancellationToken,
) -> Result<(), HarnessError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(container, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|error| {
                HarnessError::Execution(format!(
                    "failed to restrict workspace container permissions: {error}"
                ))
            })?;
    }
    require_not_cancelled(cancellation)?;
    let marker = container.join(WORKSPACE_MARKER);
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(&marker).await.map_err(|error| {
        HarnessError::Execution(format!(
            "failed to create isolated workspace marker: {error}"
        ))
    })?;
    use tokio::io::AsyncWriteExt as _;
    file.write_all(cleanup_token.as_bytes())
        .await
        .map_err(|error| {
            HarnessError::Execution(format!(
                "failed to write isolated workspace marker: {error}"
            ))
        })?;
    file.sync_all().await.map_err(|error| {
        HarnessError::Execution(format!("failed to sync isolated workspace marker: {error}"))
    })?;
    require_not_cancelled(cancellation)?;
    fs::create_dir(executor_root).await.map_err(|error| {
        HarnessError::Execution(format!("failed to create executor workspace root: {error}"))
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(executor_root, std::fs::Permissions::from_mode(0o700))
            .await
            .map_err(|error| {
                HarnessError::Execution(format!(
                    "failed to restrict executor workspace permissions: {error}"
                ))
            })?;
    }
    require_not_cancelled(cancellation)
}

async fn read_bounded_marker(path: &Path) -> Result<String, HarnessError> {
    use tokio::io::AsyncReadExt as _;

    let file = fs::File::open(path).await.map_err(|error| {
        HarnessError::Execution(format!("failed to open isolated workspace marker: {error}"))
    })?;
    let mut bytes = Vec::with_capacity(MAX_CLEANUP_TOKEN_BYTES);
    file.take((MAX_CLEANUP_TOKEN_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| {
            HarnessError::Execution(format!("failed to read isolated workspace marker: {error}"))
        })?;
    if bytes.len() > MAX_CLEANUP_TOKEN_BYTES {
        return Err(HarnessError::Execution(
            "isolated workspace marker exceeds its byte boundary".to_owned(),
        ));
    }
    String::from_utf8(bytes)
        .map_err(|_| HarnessError::Execution("isolated workspace marker is not UTF-8".to_owned()))
}

fn prepare_managed_root(path: PathBuf, label: &str) -> Result<PathBuf, HarnessError> {
    std::fs::create_dir_all(&path).map_err(|error| {
        HarnessError::InvalidConfiguration(format!("cannot create {label}: {error}"))
    })?;
    let canonical = require_existing_directory(path, label)?;
    if canonical.parent().is_none() {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{label} cannot be a filesystem root"
        )));
    }
    Ok(canonical)
}

fn require_existing_directory(path: PathBuf, label: &str) -> Result<PathBuf, HarnessError> {
    let canonical = std::fs::canonicalize(&path).map_err(|error| {
        HarnessError::InvalidConfiguration(format!("cannot canonicalize {label}: {error}"))
    })?;
    if !canonical.is_dir() {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{label} must be a directory"
        )));
    }
    Ok(canonical)
}

fn require_existing_file(path: PathBuf, label: &str) -> Result<PathBuf, HarnessError> {
    let canonical = std::fs::canonicalize(&path).map_err(|error| {
        HarnessError::InvalidConfiguration(format!("cannot canonicalize {label}: {error}"))
    })?;
    if !canonical.is_file() {
        return Err(HarnessError::InvalidConfiguration(format!(
            "{label} must be a file"
        )));
    }
    Ok(canonical)
}

fn require_provider_lease(lease: &WorkspaceLease, provider: &str) -> Result<(), HarnessError> {
    if lease.provider != provider {
        return Err(HarnessError::Execution(
            "workspace cleanup lease belongs to another provider".to_owned(),
        ));
    }
    Ok(())
}

fn validate_cleanup_token(token: &str) -> Result<(), HarnessError> {
    if token.is_empty()
        || token.len() > MAX_CLEANUP_TOKEN_BYTES
        || token.chars().any(char::is_control)
    {
        return Err(HarnessError::InvalidCapability(format!(
            "workspace cleanup token must be 1-{MAX_CLEANUP_TOKEN_BYTES} non-control bytes"
        )));
    }
    Ok(())
}

fn require_managed_nested_child<'a>(
    root: &Path,
    path: &'a Path,
    prefix: &str,
    leaf: &str,
) -> Result<&'a Path, HarnessError> {
    let container = path.parent();
    if path.file_name() != Some(std::ffi::OsStr::new(leaf))
        || container.and_then(Path::parent) != Some(root)
        || !container
            .and_then(Path::file_name)
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|name| name.starts_with(prefix))
    {
        return Err(HarnessError::Execution(
            "workspace cleanup path escaped its managed root".to_owned(),
        ));
    }
    container.ok_or_else(|| {
        HarnessError::Execution("workspace cleanup path lost its managed container".to_owned())
    })
}

fn require_not_cancelled(cancellation: &CancellationToken) -> Result<(), HarnessError> {
    if cancellation.is_cancelled() {
        Err(HarnessError::Execution(
            "workspace operation was cancelled".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn path_text(path: &Path) -> Result<&str, HarnessError> {
    path.to_str().ok_or_else(|| {
        HarnessError::InvalidConfiguration("Git Workspace path must be valid UTF-8".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        GitWorktreeWorkspaceProvider, LocalDirectoryWorkspaceProvider, WorkspaceDisposition,
        WorkspaceProvider, WorkspaceRequest,
    };
    use crate::{
        CancellationToken, LocalProcessBroker, TaskGraphId, TaskId, TaskLeaseId, WorkspaceMode,
    };

    fn request(mode: WorkspaceMode) -> WorkspaceRequest {
        WorkspaceRequest {
            graph_id: TaskGraphId::from_static("graph-workspace"),
            task_id: TaskId::from_static("task-workspace"),
            lease_id: TaskLeaseId::from_static("lease-workspace"),
            attempt: 1,
            mode,
        }
    }

    fn temporary_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("y-harness-{label}-{}", crate::EventId::generate()))
    }

    fn git_executable() -> std::path::PathBuf {
        let names: &[&str] = if cfg!(windows) {
            &["git.exe", "git"]
        } else {
            &["git"]
        };
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .flat_map(|directory| names.iter().map(move |name| directory.join(name)))
            .find(|path| path.is_file())
            .and_then(|path| std::fs::canonicalize(path).ok())
            .expect("Git executable required by the test environment")
    }

    #[tokio::test]
    async fn local_directory_is_unique_marked_and_idempotently_removed() {
        let root = temporary_root("local-workspaces");
        let provider =
            LocalDirectoryWorkspaceProvider::new(&root).expect("local workspace provider");
        let first = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("first workspace");
        let second = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("second workspace");
        assert_ne!(first.root(), second.root());
        assert!(first.root().expect("first root").is_dir());
        assert!(second.root().expect("second root").is_dir());
        let first_path = first.root().expect("first root").to_owned();
        provider
            .release(
                first,
                WorkspaceDisposition::Completed,
                CancellationToken::new(),
            )
            .await
            .expect("release first");
        assert!(!first_path.exists());
        provider
            .release(
                second,
                WorkspaceDisposition::Failed,
                CancellationToken::new(),
            )
            .await
            .expect("release second");
        std::fs::remove_dir(&root).expect("remove empty managed root");
    }

    #[tokio::test]
    async fn local_directory_never_removes_a_replaced_allocation() {
        let root = temporary_root("replaced-workspace");
        let provider =
            LocalDirectoryWorkspaceProvider::new(&root).expect("local workspace provider");
        let lease = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("workspace");
        let path = lease.root().expect("root").to_owned();
        let container = path.parent().expect("allocation container").to_owned();
        std::fs::remove_dir_all(&container).expect("remove allocation");
        std::fs::create_dir(&container).expect("replace container");
        std::fs::create_dir(&path).expect("replace executor root");

        assert!(
            provider
                .release(
                    lease,
                    WorkspaceDisposition::Cancelled,
                    CancellationToken::new(),
                )
                .await
                .expect_err("marker mismatch")
                .to_string()
                .contains("marker")
        );
        std::fs::remove_dir_all(&container).expect("remove replacement");
        std::fs::remove_dir(&root).expect("remove root");
    }

    #[tokio::test]
    async fn local_directory_bounds_untrusted_marker_reads() {
        let root = temporary_root("oversized-marker");
        let provider =
            LocalDirectoryWorkspaceProvider::new(&root).expect("local workspace provider");
        let lease = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("workspace");
        let path = lease.root().expect("root").to_owned();
        std::fs::write(
            path.parent()
                .expect("allocation container")
                .join(super::WORKSPACE_MARKER),
            vec![b'x'; super::MAX_CLEANUP_TOKEN_BYTES + 1],
        )
        .expect("replace marker");

        assert!(
            provider
                .release(
                    lease,
                    WorkspaceDisposition::Failed,
                    CancellationToken::new(),
                )
                .await
                .expect_err("oversized marker")
                .to_string()
                .contains("byte boundary")
        );
        std::fs::remove_dir_all(path.parent().expect("allocation container"))
            .expect("remove allocation");
        std::fs::remove_dir(&root).expect("remove root");
    }

    #[tokio::test]
    async fn local_directory_cleanup_survives_executor_root_removal() {
        let root = temporary_root("removed-executor-root");
        let provider =
            LocalDirectoryWorkspaceProvider::new(&root).expect("local workspace provider");
        let lease = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("workspace");
        let path = lease.root().expect("root").to_owned();
        std::fs::remove_dir_all(&path).expect("executor clears its root");

        provider
            .release(
                lease,
                WorkspaceDisposition::Completed,
                CancellationToken::new(),
            )
            .await
            .expect("release allocation container");
        assert!(!path.exists());
        std::fs::remove_dir(&root).expect("remove root");
    }

    #[tokio::test]
    async fn git_worktree_is_detached_at_a_pinned_revision_and_removed() {
        let git = git_executable();
        let repository = temporary_root("git-source");
        let workspaces = temporary_root("git-worktrees");
        std::fs::create_dir(&repository).expect("repository directory");
        let run = |args: &[&str]| {
            let output = std::process::Command::new(&git)
                .args(args)
                .current_dir(&repository)
                .env_clear()
                .output()
                .expect("run Git fixture command");
            assert!(
                output.status.success(),
                "Git fixture command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        run(&["init", "-q"]);
        run(&["config", "user.name", "Y Harness"]);
        run(&["config", "user.email", "y-harness@example.invalid"]);
        std::fs::write(repository.join("fixture.txt"), "workspace fixture\n")
            .expect("fixture file");
        run(&["add", "fixture.txt"]);
        run(&["commit", "-q", "-m", "fixture"]);
        let output = std::process::Command::new(&git)
            .args(["rev-parse", "HEAD"])
            .current_dir(&repository)
            .env_clear()
            .output()
            .expect("resolve revision");
        assert!(output.status.success());
        let revision = String::from_utf8(output.stdout)
            .expect("revision UTF-8")
            .trim()
            .to_owned();
        let provider = GitWorktreeWorkspaceProvider::new(
            &repository,
            &workspaces,
            revision,
            &git,
            Arc::new(LocalProcessBroker::new(1).expect("process broker")),
        )
        .expect("Git provider");
        let lease = provider
            .prepare(request(WorkspaceMode::Isolated), CancellationToken::new())
            .await
            .expect("Git Worktree");
        let path = lease.root().expect("Worktree root").to_owned();
        assert_eq!(
            std::fs::read_to_string(path.join("fixture.txt")).expect("fixture"),
            "workspace fixture\n"
        );
        provider
            .release(
                lease,
                WorkspaceDisposition::Completed,
                CancellationToken::new(),
            )
            .await
            .expect("remove Worktree");
        assert!(!path.exists());
        std::fs::remove_dir(&workspaces).expect("remove Worktree root");
        std::fs::remove_dir_all(&repository).expect("remove repository");
    }
}
