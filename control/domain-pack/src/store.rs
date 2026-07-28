use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    future::Future,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex as StdMutex},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};
use y_harness::{ActorIdentity, AuthorityContext, ExecutionBinding};

use crate::{
    DomainPackComponentKind, DomainPackReleaseId, DomainPackSnapshot, VerifiedDomainPack,
    model::{bounded_json_size, validate_digest, validate_release_id},
};

/// Current durable Domain Pack control-plane schema.
pub const DOMAIN_PACK_STORE_SCHEMA_VERSION: u32 = 1;

const MAX_RELEASES_PER_PACK: usize = 256;
const MAX_ROLLBACK_HISTORY: usize = 32;
const MAX_RELEASE_RECORD_BYTES: usize = 1_310_720;
const MAX_ACTIVATION_RECORD_BYTES: usize = 65_536;
const METADATA_TABLE: &str = "domain_pack_metadata";
const RELEASE_TABLE: &str = "domain_pack_releases";
const ACTIVATION_TABLE: &str = "domain_pack_activations";

/// Boxed asynchronous result used by Domain Pack store implementations.
pub type DomainPackFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, DomainPackError>> + Send + 'a>>;
type ReleaseKey = (Option<String>, DomainPackReleaseId);
type ActivationKey = (Option<String>, String);

/// Bounded Domain Pack control-plane failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomainPackError {
    /// Snapshot, evidence, identity, lifecycle, or bounds are invalid.
    Invalid(String),
    /// The exact release or activation does not exist in this tenant.
    NotFound(String),
    /// An optimistic lifecycle revision changed.
    Conflict {
        /// Contended Pack identity.
        pack: String,
        /// Revision supplied by the caller.
        expected: u64,
        /// Revision found atomically by the store.
        actual: u64,
    },
    /// Durable storage failed without exposing provider-controlled details.
    Storage(String),
}

impl fmt::Display for DomainPackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "invalid Domain Pack: {message}"),
            Self::NotFound(pack) => write!(formatter, "Domain Pack {pack} does not exist"),
            Self::Conflict {
                pack,
                expected,
                actual,
            } => write!(
                formatter,
                "Domain Pack conflict on {pack}: expected revision {expected}, found {actual}"
            ),
            Self::Storage(message) => write!(formatter, "Domain Pack storage error: {message}"),
        }
    }
}

impl Error for DomainPackError {}

/// Immutable completed evaluation evidence for one release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackEvaluation {
    /// Digest of the exact Evaluation suite and baseline inputs.
    pub suite_sha256: String,
    /// Digest of the complete machine-readable Evaluation report.
    pub report_sha256: String,
    /// Whether every configured promotion requirement passed.
    pub passed: bool,
    /// Trusted actor that submitted the completed evidence.
    pub evaluated_by: ActorIdentity,
    /// Server-clock settlement time.
    pub evaluated_at_ms: u64,
}

/// Immutable independent approval evidence for one evaluated release.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackApproval {
    /// Digest of the external approval record or signed decision receipt.
    pub evidence_sha256: String,
    /// Trusted actor that approved promotion.
    pub approved_by: ActorIdentity,
    /// Server-clock approval time.
    pub approved_at_ms: u64,
}

/// Monotonic release promotion state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case", deny_unknown_fields)]
pub enum DomainPackReleaseStage {
    /// Snapshot is installed but has no completed evaluation.
    Installed,
    /// One immutable evaluation settled; failed releases cannot be approved.
    Evaluated {
        /// Exact evaluation evidence.
        evaluation: DomainPackEvaluation,
    },
    /// A passing evaluation received an independent approval.
    Approved {
        /// Exact passing evaluation.
        evaluation: DomainPackEvaluation,
        /// Independent approval evidence.
        approval: DomainPackApproval,
    },
}

/// Revisioned release record within one trusted tenant partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackRelease {
    /// Durable record schema.
    pub schema_version: u32,
    /// Immutable tenant owner, absent only for an explicitly unscoped host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    /// Digest-bound Pack snapshot.
    pub snapshot: DomainPackSnapshot,
    /// Current monotonic promotion stage.
    pub stage: DomainPackReleaseStage,
    /// Optimistic revision beginning at one.
    pub revision: u64,
    /// Trusted installing actor.
    pub installed_by: ActorIdentity,
    /// Server-clock installation time.
    pub installed_at_ms: u64,
    /// Server-clock latest transition time.
    pub updated_at_ms: u64,
}

impl DomainPackRelease {
    /// Returns the immutable tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}

/// Revisioned active release and bounded rollback lineage for one Pack name.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPackActivation {
    /// Durable record schema.
    pub schema_version: u32,
    /// Immutable tenant owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    /// Stable Pack name.
    pub name: String,
    /// Exact active release, or none after explicit deactivation.
    pub active: Option<DomainPackReleaseId>,
    /// Newest-last bounded prior active releases.
    pub rollback: Vec<DomainPackReleaseId>,
    /// Inventory digest verified for the active release.
    pub inventory_sha256: Option<String>,
    /// Optimistic activation revision beginning at one.
    pub revision: u64,
    /// Trusted actor that performed the latest transition.
    pub changed_by: ActorIdentity,
    /// Server-clock latest transition time.
    pub changed_at_ms: u64,
}

impl DomainPackActivation {
    /// Returns the immutable tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}

/// Constructor-only proof that an approved active release still matches the
/// exact inventory and activation revision observed before execution.
#[derive(Clone, Debug)]
pub struct DomainPackExecutionBinding {
    tenant_id: Option<String>,
    snapshot: DomainPackSnapshot,
    activation_revision: u64,
    inventory_sha256: String,
}

impl DomainPackExecutionBinding {
    /// Returns the immutable tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the approved active snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &DomainPackSnapshot {
        &self.snapshot
    }

    /// Returns the exact activation revision observed by this binding.
    #[must_use]
    pub fn activation_revision(&self) -> u64 {
        self.activation_revision
    }

    /// Returns the complete inventory digest observed by this binding.
    #[must_use]
    pub fn inventory_sha256(&self) -> &str {
        &self.inventory_sha256
    }

    /// Converts this governed proof into Engine-owned durable Turn evidence.
    pub fn to_execution_binding(&self) -> Result<ExecutionBinding, DomainPackError> {
        ExecutionBinding::new(
            "domain-pack",
            self.snapshot.release.name.clone(),
            self.snapshot.release.version.to_string(),
            self.snapshot.content_sha256.clone(),
            self.inventory_sha256.clone(),
            self.activation_revision,
            self.tenant_id.clone(),
        )
        .map_err(|_| {
            DomainPackError::Invalid(
                "governed Domain Pack cannot be represented as an Engine execution binding"
                    .to_owned(),
            )
        })
    }
}

/// Tenant-fenced Domain Pack release and activation persistence.
pub trait DomainPackStore: Send + Sync {
    /// Idempotently installs one immutable snapshot in the trusted partition.
    fn install<'a>(
        &'a self,
        snapshot: DomainPackSnapshot,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease>;

    /// Loads one exact release only in the trusted partition.
    fn get<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackRelease>>;

    /// Atomically records one terminal evaluation at the observed revision.
    fn evaluate<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        suite_sha256: String,
        report_sha256: String,
        passed: bool,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease>;

    /// Atomically records independent approval of one passing evaluation.
    fn approve<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        evidence_sha256: String,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease>;

    /// Returns the current activation record for one Pack name.
    fn activation<'a>(
        &'a self,
        name: &'a str,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackActivation>>;

    /// Binds one active revision to the still-identical verified inventory.
    fn bind<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_activation_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackExecutionBinding>;

    /// Activates one approved and inventory-verified release by activation CAS.
    ///
    /// Revision zero means no activation record has been created.
    fn activate<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation>;

    /// Deactivates one currently active Pack while preserving rollback lineage.
    fn deactivate<'a>(
        &'a self,
        name: &'a str,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation>;

    /// Reactivates the newest rollback release after exact inventory verification.
    fn rollback<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation>;
}

/// In-memory implementation with the same transition and tenant semantics.
#[derive(Default)]
pub struct MemoryDomainPackStore {
    state: Mutex<CatalogState>,
}

#[derive(Default)]
struct CatalogState {
    releases: BTreeMap<ReleaseKey, DomainPackRelease>,
    activations: BTreeMap<ActivationKey, DomainPackActivation>,
}

impl MemoryDomainPackStore {
    /// Creates an empty control-plane store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl DomainPackStore for MemoryDomainPackStore {
    fn install<'a>(
        &'a self,
        snapshot: DomainPackSnapshot,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            snapshot.validate()?;
            let key = (tenant.clone(), snapshot.release.clone());
            let mut state = self.state.lock().await;
            if let Some(existing) = state.releases.get(&key) {
                return same_install(existing, &snapshot);
            }
            enforce_release_count(&state.releases, tenant.as_deref(), &snapshot.release.name)?;
            let record = installed_release(snapshot, tenant, authority)?;
            state.releases.insert(key, record.clone());
            Ok(record)
        })
    }

    fn get<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackRelease>> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            validate_release_id(release)?;
            let state = self.state.lock().await;
            Ok(state.releases.get(&(tenant, release.clone())).cloned())
        })
    }

    fn evaluate<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        suite_sha256: String,
        report_sha256: String,
        passed: bool,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let mut state = self.state.lock().await;
            let mut candidate = state
                .releases
                .get(&(tenant.clone(), release.clone()))
                .cloned()
                .ok_or_else(|| DomainPackError::NotFound(release_label(release)))?;
            let record = evaluate_release(
                &mut candidate,
                expected_revision,
                suite_sha256,
                report_sha256,
                passed,
                authority,
            )?;
            state
                .releases
                .insert((tenant, release.clone()), record.clone());
            Ok(record)
        })
    }

    fn approve<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        evidence_sha256: String,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let mut state = self.state.lock().await;
            let mut candidate = state
                .releases
                .get(&(tenant.clone(), release.clone()))
                .cloned()
                .ok_or_else(|| DomainPackError::NotFound(release_label(release)))?;
            let record = approve_release(
                &mut candidate,
                expected_revision,
                evidence_sha256,
                authority,
            )?;
            state
                .releases
                .insert((tenant, release.clone()), record.clone());
            Ok(record)
        })
    }

    fn activation<'a>(
        &'a self,
        name: &'a str,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackActivation>> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            validate_pack_name(name)?;
            let state = self.state.lock().await;
            Ok(state.activations.get(&(tenant, name.to_owned())).cloned())
        })
    }

    fn bind<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_activation_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackExecutionBinding> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let release_id = verified.snapshot().release.clone();
            let state = self.state.lock().await;
            let release = state
                .releases
                .get(&(tenant.clone(), release_id.clone()))
                .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
            let activation = state
                .activations
                .get(&(tenant.clone(), release_id.name.clone()))
                .ok_or_else(|| DomainPackError::NotFound(release_id.name.clone()))?;
            bind_execution(
                release,
                activation,
                verified,
                expected_activation_revision,
                tenant,
            )
        })
    }

    fn activate<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let release_id = verified.snapshot().release.clone();
            let mut state = self.state.lock().await;
            let release = state
                .releases
                .get(&(tenant.clone(), release_id.clone()))
                .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
            require_activatable(release, &verified)?;
            let key = (tenant.clone(), release_id.name.clone());
            let current = state.activations.get(&key).cloned();
            let activation =
                activate_release(current, verified, expected_revision, tenant, authority)?;
            state.activations.insert(key, activation.clone());
            Ok(activation)
        })
    }

    fn deactivate<'a>(
        &'a self,
        name: &'a str,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let mut state = self.state.lock().await;
            let key = (tenant, name.to_owned());
            let current = state
                .activations
                .get(&key)
                .cloned()
                .ok_or_else(|| DomainPackError::NotFound(name.to_owned()))?;
            let activation = deactivate_release(current, expected_revision, authority)?;
            state.activations.insert(key, activation.clone());
            Ok(activation)
        })
    }

    fn rollback<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            let release_id = verified.snapshot().release.clone();
            let mut state = self.state.lock().await;
            let release = state
                .releases
                .get(&(tenant.clone(), release_id.clone()))
                .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
            require_activatable(release, &verified)?;
            let key = (tenant, release_id.name.clone());
            let current = state
                .activations
                .get(&key)
                .cloned()
                .ok_or_else(|| DomainPackError::NotFound(release_id.name.clone()))?;
            let activation = rollback_release(current, verified, expected_revision, authority)?;
            state.activations.insert(key, activation.clone());
            Ok(activation)
        })
    }
}

/// Single-host durable SQLite Domain Pack store with cross-process CAS.
pub struct SqliteDomainPackStore {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteDomainPackStore {
    /// Opens or initializes an exact schema-1 SQLite control-plane store.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DomainPackError> {
        let mut connection = Connection::open(path).map_err(|_| storage_error())?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(|_| storage_error())?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|_| storage_error())?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(|_| storage_error())?;
        initialize_schema(&mut connection)?;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }

    async fn run<T, F>(&self, operation: F) -> Result<T, DomainPackError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, DomainPackError> + Send + 'static,
    {
        let connection = self.connection.clone();
        task::spawn_blocking(move || {
            let mut connection = connection.lock().map_err(|_| storage_error())?;
            operation(&mut connection)
        })
        .await
        .map_err(|_| storage_error())?
    }
}

impl DomainPackStore for SqliteDomainPackStore {
    fn install<'a>(
        &'a self,
        snapshot: DomainPackSnapshot,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            snapshot.validate()?;
            let actor = authority.actor().clone();
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|_| storage_error())?;
                if let Some(existing) =
                    load_release(&transaction, tenant.as_deref(), &snapshot.release)?
                {
                    return same_install(&existing, &snapshot);
                }
                enforce_sqlite_release_count(
                    &transaction,
                    tenant.as_deref(),
                    &snapshot.release.name,
                )?;
                let authority = authority_from_parts(actor, tenant.clone())?;
                let record = installed_release(snapshot, tenant.clone(), &authority)?;
                insert_release(&transaction, &record)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(record)
            })
            .await
        })
    }

    fn get<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackRelease>> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            validate_release_id(release)?;
            let release = release.clone();
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(|_| storage_error())?;
                let record = load_release(&transaction, tenant.as_deref(), &release)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(record)
            })
            .await
        })
    }

    fn evaluate<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        suite_sha256: String,
        report_sha256: String,
        passed: bool,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        let release = release.clone();
        let actor = authority.actor().clone();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                mutate_release(
                    connection,
                    tenant,
                    release,
                    expected_revision,
                    actor,
                    move |record, authority| {
                        evaluate_release(
                            record,
                            expected_revision,
                            suite_sha256,
                            report_sha256,
                            passed,
                            authority,
                        )
                    },
                )
            })
            .await
        })
    }

    fn approve<'a>(
        &'a self,
        release: &'a DomainPackReleaseId,
        expected_revision: u64,
        evidence_sha256: String,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackRelease> {
        let release = release.clone();
        let actor = authority.actor().clone();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                mutate_release(
                    connection,
                    tenant,
                    release,
                    expected_revision,
                    actor,
                    move |record, authority| {
                        approve_release(record, expected_revision, evidence_sha256, authority)
                    },
                )
            })
            .await
        })
    }

    fn activation<'a>(
        &'a self,
        name: &'a str,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, Option<DomainPackActivation>> {
        let name = name.to_owned();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            validate_pack_name(&name)?;
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(|_| storage_error())?;
                let activation = load_activation(&transaction, tenant.as_deref(), &name)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(activation)
            })
            .await
        })
    }

    fn bind<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_activation_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackExecutionBinding> {
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Deferred)
                    .map_err(|_| storage_error())?;
                let release_id = verified.snapshot().release.clone();
                let release = load_release(&transaction, tenant.as_deref(), &release_id)?
                    .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
                let activation =
                    load_activation(&transaction, tenant.as_deref(), &release_id.name)?
                        .ok_or_else(|| DomainPackError::NotFound(release_id.name.clone()))?;
                let binding = bind_execution(
                    &release,
                    &activation,
                    verified,
                    expected_activation_revision,
                    tenant,
                )?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(binding)
            })
            .await
        })
    }

    fn activate<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let actor = authority.actor().clone();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|_| storage_error())?;
                let release_id = verified.snapshot().release.clone();
                let release = load_release(&transaction, tenant.as_deref(), &release_id)?
                    .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
                require_activatable(&release, &verified)?;
                let current = load_activation(&transaction, tenant.as_deref(), &release_id.name)?;
                let authority = authority_from_parts(actor, tenant.clone())?;
                let activation =
                    activate_release(current, verified, expected_revision, tenant, &authority)?;
                write_activation(&transaction, &activation)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(activation)
            })
            .await
        })
    }

    fn deactivate<'a>(
        &'a self,
        name: &'a str,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let name = name.to_owned();
        let actor = authority.actor().clone();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|_| storage_error())?;
                let current = load_activation(&transaction, tenant.as_deref(), &name)?
                    .ok_or_else(|| DomainPackError::NotFound(name.clone()))?;
                let authority = authority_from_parts(actor, tenant)?;
                let activation = deactivate_release(current, expected_revision, &authority)?;
                write_activation(&transaction, &activation)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(activation)
            })
            .await
        })
    }

    fn rollback<'a>(
        &'a self,
        verified: VerifiedDomainPack,
        expected_revision: u64,
        authority: &'a AuthorityContext,
    ) -> DomainPackFuture<'a, DomainPackActivation> {
        let actor = authority.actor().clone();
        Box::pin(async move {
            let tenant = validated_tenant(authority)?;
            self.run(move |connection| {
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(|_| storage_error())?;
                let release_id = verified.snapshot().release.clone();
                let release = load_release(&transaction, tenant.as_deref(), &release_id)?
                    .ok_or_else(|| DomainPackError::NotFound(release_label(&release_id)))?;
                require_activatable(&release, &verified)?;
                let current = load_activation(&transaction, tenant.as_deref(), &release_id.name)?
                    .ok_or_else(|| DomainPackError::NotFound(release_id.name.clone()))?;
                let authority = authority_from_parts(actor, tenant)?;
                let activation =
                    rollback_release(current, verified, expected_revision, &authority)?;
                write_activation(&transaction, &activation)?;
                transaction.commit().map_err(|_| storage_error())?;
                Ok(activation)
            })
            .await
        })
    }
}

fn installed_release(
    snapshot: DomainPackSnapshot,
    tenant_id: Option<String>,
    authority: &AuthorityContext,
) -> Result<DomainPackRelease, DomainPackError> {
    let now = now_ms()?;
    let record = DomainPackRelease {
        schema_version: DOMAIN_PACK_STORE_SCHEMA_VERSION,
        tenant_id,
        snapshot,
        stage: DomainPackReleaseStage::Installed,
        revision: 1,
        installed_by: authority.actor().clone(),
        installed_at_ms: now,
        updated_at_ms: now,
    };
    validate_release(&record)?;
    Ok(record)
}

fn same_install(
    existing: &DomainPackRelease,
    snapshot: &DomainPackSnapshot,
) -> Result<DomainPackRelease, DomainPackError> {
    if &existing.snapshot == snapshot {
        Ok(existing.clone())
    } else {
        Err(DomainPackError::Invalid(format!(
            "{} is already installed with different immutable content",
            release_label(&snapshot.release)
        )))
    }
}

fn evaluate_release(
    record: &mut DomainPackRelease,
    expected_revision: u64,
    suite_sha256: String,
    report_sha256: String,
    passed: bool,
    authority: &AuthorityContext,
) -> Result<DomainPackRelease, DomainPackError> {
    require_revision(
        &release_label(&record.snapshot.release),
        record.revision,
        expected_revision,
    )?;
    if !matches!(record.stage, DomainPackReleaseStage::Installed) {
        return Err(DomainPackError::Invalid(
            "Domain Pack evaluation is immutable once recorded".to_owned(),
        ));
    }
    validate_digest("Domain Pack evaluation suite", &suite_sha256)?;
    validate_digest("Domain Pack evaluation report", &report_sha256)?;
    if !record.snapshot.components.iter().any(|component| {
        component.kind == DomainPackComponentKind::Evaluation
            && component.content_sha256 == suite_sha256
    }) {
        return Err(DomainPackError::Invalid(
            "Domain Pack evaluation evidence does not match a pinned suite".to_owned(),
        ));
    }
    let now = now_ms()?;
    record.stage = DomainPackReleaseStage::Evaluated {
        evaluation: DomainPackEvaluation {
            suite_sha256,
            report_sha256,
            passed,
            evaluated_by: authority.actor().clone(),
            evaluated_at_ms: now,
        },
    };
    record.revision = next_revision(record.revision)?;
    record.updated_at_ms = now;
    validate_release(record)?;
    Ok(record.clone())
}

fn approve_release(
    record: &mut DomainPackRelease,
    expected_revision: u64,
    evidence_sha256: String,
    authority: &AuthorityContext,
) -> Result<DomainPackRelease, DomainPackError> {
    require_revision(
        &release_label(&record.snapshot.release),
        record.revision,
        expected_revision,
    )?;
    validate_digest("Domain Pack approval evidence", &evidence_sha256)?;
    let DomainPackReleaseStage::Evaluated { evaluation } = &record.stage else {
        return Err(DomainPackError::Invalid(
            "only one evaluated Domain Pack release can be approved".to_owned(),
        ));
    };
    if !evaluation.passed {
        return Err(DomainPackError::Invalid(
            "a failed Domain Pack evaluation cannot be approved".to_owned(),
        ));
    }
    if &evaluation.evaluated_by == authority.actor() {
        return Err(DomainPackError::Invalid(
            "Domain Pack evaluator cannot approve the same release".to_owned(),
        ));
    }
    let evaluation = evaluation.clone();
    let now = now_ms()?;
    record.stage = DomainPackReleaseStage::Approved {
        evaluation,
        approval: DomainPackApproval {
            evidence_sha256,
            approved_by: authority.actor().clone(),
            approved_at_ms: now,
        },
    };
    record.revision = next_revision(record.revision)?;
    record.updated_at_ms = now;
    validate_release(record)?;
    Ok(record.clone())
}

fn require_activatable(
    record: &DomainPackRelease,
    verified: &VerifiedDomainPack,
) -> Result<(), DomainPackError> {
    validate_release(record)?;
    if &record.snapshot != verified.snapshot() {
        return Err(DomainPackError::Invalid(
            "verified Domain Pack does not match the installed release".to_owned(),
        ));
    }
    if !matches!(record.stage, DomainPackReleaseStage::Approved { .. }) {
        return Err(DomainPackError::Invalid(
            "only an approved Domain Pack release can be activated".to_owned(),
        ));
    }
    validate_digest(
        "Domain Pack activation inventory",
        verified.inventory_sha256(),
    )
}

fn bind_execution(
    release: &DomainPackRelease,
    activation: &DomainPackActivation,
    verified: VerifiedDomainPack,
    expected_activation_revision: u64,
    tenant_id: Option<String>,
) -> Result<DomainPackExecutionBinding, DomainPackError> {
    require_activatable(release, &verified)?;
    require_revision(
        &activation.name,
        activation.revision,
        expected_activation_revision,
    )?;
    if activation.tenant_id != tenant_id
        || activation.active.as_ref() != Some(&verified.snapshot().release)
        || activation.inventory_sha256.as_deref() != Some(verified.inventory_sha256())
    {
        return Err(DomainPackError::Invalid(
            "active Domain Pack does not match the verified execution inventory".to_owned(),
        ));
    }
    let snapshot = verified.snapshot().clone();
    let inventory_sha256 = verified.inventory_sha256().to_owned();
    Ok(DomainPackExecutionBinding {
        tenant_id,
        snapshot,
        activation_revision: activation.revision,
        inventory_sha256,
    })
}

fn activate_release(
    current: Option<DomainPackActivation>,
    verified: VerifiedDomainPack,
    expected_revision: u64,
    tenant_id: Option<String>,
    authority: &AuthorityContext,
) -> Result<DomainPackActivation, DomainPackError> {
    let target = verified.snapshot().release.clone();
    let actual = current.as_ref().map_or(0, |value| value.revision);
    require_revision(&target.name, actual, expected_revision)?;
    if let Some(existing) = &current
        && existing.active.as_ref() == Some(&target)
        && existing.inventory_sha256.as_deref() == Some(verified.inventory_sha256())
    {
        return Ok(existing.clone());
    }
    let mut rollback = current
        .as_ref()
        .map_or_else(Vec::new, |value| value.rollback.clone());
    if let Some(active) = current.and_then(|value| value.active)
        && active != target
    {
        push_rollback(&mut rollback, active)?;
    }
    let activation = DomainPackActivation {
        schema_version: DOMAIN_PACK_STORE_SCHEMA_VERSION,
        tenant_id,
        name: target.name.clone(),
        active: Some(target),
        rollback,
        inventory_sha256: Some(verified.inventory_sha256().to_owned()),
        revision: next_revision(actual)?,
        changed_by: authority.actor().clone(),
        changed_at_ms: now_ms()?,
    };
    validate_activation(&activation)?;
    Ok(activation)
}

fn deactivate_release(
    mut current: DomainPackActivation,
    expected_revision: u64,
    authority: &AuthorityContext,
) -> Result<DomainPackActivation, DomainPackError> {
    require_revision(&current.name, current.revision, expected_revision)?;
    let active = current.active.take().ok_or_else(|| {
        DomainPackError::Invalid(format!("Domain Pack {} is already inactive", current.name))
    })?;
    push_rollback(&mut current.rollback, active)?;
    current.inventory_sha256 = None;
    current.revision = next_revision(current.revision)?;
    current.changed_by = authority.actor().clone();
    current.changed_at_ms = now_ms()?;
    validate_activation(&current)?;
    Ok(current)
}

fn rollback_release(
    mut current: DomainPackActivation,
    verified: VerifiedDomainPack,
    expected_revision: u64,
    authority: &AuthorityContext,
) -> Result<DomainPackActivation, DomainPackError> {
    require_revision(&current.name, current.revision, expected_revision)?;
    let target = verified.snapshot().release.clone();
    let expected_target = current.rollback.pop().ok_or_else(|| {
        DomainPackError::Invalid(format!(
            "Domain Pack {} has no rollback release",
            current.name
        ))
    })?;
    if target != expected_target {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack {} rollback target does not match bounded activation history",
            current.name
        )));
    }
    if let Some(active) = current.active.take() {
        push_rollback(&mut current.rollback, active)?;
    }
    current.active = Some(target);
    current.inventory_sha256 = Some(verified.inventory_sha256().to_owned());
    current.revision = next_revision(current.revision)?;
    current.changed_by = authority.actor().clone();
    current.changed_at_ms = now_ms()?;
    validate_activation(&current)?;
    Ok(current)
}

fn push_rollback(
    rollback: &mut Vec<DomainPackReleaseId>,
    release: DomainPackReleaseId,
) -> Result<(), DomainPackError> {
    if rollback.len() > MAX_ROLLBACK_HISTORY {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack rollback history exceeds {MAX_ROLLBACK_HISTORY} releases"
        )));
    }
    if rollback.len() == MAX_ROLLBACK_HISTORY {
        rollback.remove(0);
    }
    rollback.push(release);
    Ok(())
}

fn validate_release(record: &DomainPackRelease) -> Result<(), DomainPackError> {
    if record.schema_version != DOMAIN_PACK_STORE_SCHEMA_VERSION {
        return Err(DomainPackError::Invalid(format!(
            "unsupported Domain Pack release schema {}; expected {DOMAIN_PACK_STORE_SCHEMA_VERSION}",
            record.schema_version
        )));
    }
    validate_optional_tenant(record.tenant_id.as_deref())?;
    record.snapshot.validate()?;
    validate_actor(&record.installed_by)?;
    if record.revision == 0 || record.installed_at_ms == 0 || record.updated_at_ms == 0 {
        return Err(DomainPackError::Invalid(
            "Domain Pack release revision and timestamps must be non-zero".to_owned(),
        ));
    }
    if record.updated_at_ms < record.installed_at_ms {
        return Err(DomainPackError::Invalid(
            "Domain Pack release time precedes installation".to_owned(),
        ));
    }
    match &record.stage {
        DomainPackReleaseStage::Installed => {}
        DomainPackReleaseStage::Evaluated { evaluation } => {
            validate_evaluation(evaluation, record.installed_at_ms)?;
        }
        DomainPackReleaseStage::Approved {
            evaluation,
            approval,
        } => {
            validate_evaluation(evaluation, record.installed_at_ms)?;
            if !evaluation.passed {
                return Err(DomainPackError::Invalid(
                    "approved Domain Pack contains a failed evaluation".to_owned(),
                ));
            }
            validate_digest("Domain Pack approval evidence", &approval.evidence_sha256)?;
            validate_actor(&approval.approved_by)?;
            if approval.approved_by == evaluation.evaluated_by {
                return Err(DomainPackError::Invalid(
                    "Domain Pack evaluator and approver are identical".to_owned(),
                ));
            }
            if approval.approved_at_ms < evaluation.evaluated_at_ms {
                return Err(DomainPackError::Invalid(
                    "Domain Pack approval precedes evaluation".to_owned(),
                ));
            }
        }
    }
    bounded_json_size(record, MAX_RELEASE_RECORD_BYTES, "Domain Pack release")?;
    Ok(())
}

fn validate_evaluation(
    evaluation: &DomainPackEvaluation,
    installed_at_ms: u64,
) -> Result<(), DomainPackError> {
    validate_digest("Domain Pack evaluation suite", &evaluation.suite_sha256)?;
    validate_digest("Domain Pack evaluation report", &evaluation.report_sha256)?;
    validate_actor(&evaluation.evaluated_by)?;
    if evaluation.evaluated_at_ms < installed_at_ms {
        return Err(DomainPackError::Invalid(
            "Domain Pack evaluation precedes installation".to_owned(),
        ));
    }
    Ok(())
}

fn validate_activation(activation: &DomainPackActivation) -> Result<(), DomainPackError> {
    if activation.schema_version != DOMAIN_PACK_STORE_SCHEMA_VERSION {
        return Err(DomainPackError::Invalid(format!(
            "unsupported Domain Pack activation schema {}; expected {DOMAIN_PACK_STORE_SCHEMA_VERSION}",
            activation.schema_version
        )));
    }
    validate_optional_tenant(activation.tenant_id.as_deref())?;
    validate_pack_name(&activation.name)?;
    if activation.revision == 0 || activation.changed_at_ms == 0 {
        return Err(DomainPackError::Invalid(
            "Domain Pack activation revision and timestamp must be non-zero".to_owned(),
        ));
    }
    validate_actor(&activation.changed_by)?;
    if activation.rollback.len() > MAX_ROLLBACK_HISTORY {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack rollback history exceeds {MAX_ROLLBACK_HISTORY} releases"
        )));
    }
    for release in activation.active.iter().chain(&activation.rollback) {
        validate_release_id(release)?;
        if release.name != activation.name {
            return Err(DomainPackError::Invalid(
                "Domain Pack activation mixes different Pack names".to_owned(),
            ));
        }
    }
    match (&activation.active, &activation.inventory_sha256) {
        (Some(_), Some(digest)) => validate_digest("Domain Pack inventory", digest)?,
        (None, None) => {}
        _ => {
            return Err(DomainPackError::Invalid(
                "Domain Pack active release and inventory evidence disagree".to_owned(),
            ));
        }
    }
    bounded_json_size(
        activation,
        MAX_ACTIVATION_RECORD_BYTES,
        "Domain Pack activation",
    )?;
    Ok(())
}

fn validated_tenant(authority: &AuthorityContext) -> Result<Option<String>, DomainPackError> {
    let tenant = authority.tenant_id().map(str::to_owned);
    authority_from_parts(authority.actor().clone(), tenant.clone())?;
    Ok(tenant)
}

fn authority_from_parts(
    actor: ActorIdentity,
    tenant: Option<String>,
) -> Result<AuthorityContext, DomainPackError> {
    AuthorityContext::new(actor, tenant)
        .map_err(|_| DomainPackError::Invalid("trusted authority is invalid".to_owned()))
}

fn validate_actor(actor: &ActorIdentity) -> Result<(), DomainPackError> {
    authority_from_parts(actor.clone(), None).map(|_| ())
}

fn validate_optional_tenant(tenant: Option<&str>) -> Result<(), DomainPackError> {
    authority_from_parts(ActorIdentity::LocalProcess, tenant.map(str::to_owned)).map(|_| ())
}

fn validate_pack_name(name: &str) -> Result<(), DomainPackError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'));
    if !valid {
        return Err(DomainPackError::Invalid(
            "Domain Pack name must be 1-128 portable ASCII bytes".to_owned(),
        ));
    }
    Ok(())
}

fn enforce_release_count(
    releases: &BTreeMap<ReleaseKey, DomainPackRelease>,
    tenant: Option<&str>,
    name: &str,
) -> Result<(), DomainPackError> {
    let count = releases
        .keys()
        .filter(|(candidate_tenant, release)| {
            candidate_tenant.as_deref() == tenant && release.name == name
        })
        .count();
    if count >= MAX_RELEASES_PER_PACK {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack {name} exceeds {MAX_RELEASES_PER_PACK} installed releases"
        )));
    }
    Ok(())
}

fn require_revision(pack: &str, actual: u64, expected: u64) -> Result<(), DomainPackError> {
    if actual != expected {
        return Err(DomainPackError::Conflict {
            pack: pack.to_owned(),
            expected,
            actual,
        });
    }
    Ok(())
}

fn next_revision(current: u64) -> Result<u64, DomainPackError> {
    let next = current
        .checked_add(1)
        .ok_or_else(|| DomainPackError::Invalid("Domain Pack revision overflow".to_owned()))?;
    if next > i64::MAX as u64 {
        return Err(DomainPackError::Invalid(
            "Domain Pack revision exceeds the durable range".to_owned(),
        ));
    }
    Ok(next)
}

fn release_label(release: &DomainPackReleaseId) -> String {
    format!("{}@{}", release.name, release.version)
}

fn now_ms() -> Result<u64, DomainPackError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DomainPackError::Storage("system clock precedes Unix epoch".to_owned()))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| DomainPackError::Storage("system clock is outside supported range".to_owned()))
}

fn initialize_schema(connection: &mut Connection) -> Result<(), DomainPackError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| storage_error())?;
    let present: i64 = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master \
             WHERE type = 'table' AND name IN (?1, ?2, ?3)",
            params![METADATA_TABLE, RELEASE_TABLE, ACTIVATION_TABLE],
            |row| row.get(0),
        )
        .map_err(|_| storage_error())?;
    if present == 0 {
        transaction
            .execute_batch(
                "CREATE TABLE domain_pack_metadata (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL
                 );
                 CREATE TABLE domain_pack_releases (
                    tenant_key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    body_json TEXT NOT NULL,
                    PRIMARY KEY (tenant_key, name, version)
                 );
                 CREATE TABLE domain_pack_activations (
                    tenant_key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    body_json TEXT NOT NULL,
                    PRIMARY KEY (tenant_key, name)
                 );",
            )
            .map_err(|_| storage_error())?;
        transaction
            .execute(
                "INSERT INTO domain_pack_metadata (singleton, schema_version) VALUES (1, ?1)",
                params![DOMAIN_PACK_STORE_SCHEMA_VERSION],
            )
            .map_err(|_| storage_error())?;
    } else if present != 3 {
        return Err(DomainPackError::Storage(
            "SQLite Domain Pack store is partial".to_owned(),
        ));
    }
    let schema: u32 = transaction
        .query_row(
            "SELECT schema_version FROM domain_pack_metadata WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| storage_error())?;
    if schema != DOMAIN_PACK_STORE_SCHEMA_VERSION {
        return Err(DomainPackError::Storage(format!(
            "SQLite Domain Pack schema {schema} is unsupported; expected {DOMAIN_PACK_STORE_SCHEMA_VERSION}"
        )));
    }
    transaction.commit().map_err(|_| storage_error())
}

fn mutate_release<F>(
    connection: &mut Connection,
    tenant: Option<String>,
    release: DomainPackReleaseId,
    expected_revision: u64,
    actor: ActorIdentity,
    mutation: F,
) -> Result<DomainPackRelease, DomainPackError>
where
    F: FnOnce(
        &mut DomainPackRelease,
        &AuthorityContext,
    ) -> Result<DomainPackRelease, DomainPackError>,
{
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| storage_error())?;
    let mut record = load_release(&transaction, tenant.as_deref(), &release)?
        .ok_or_else(|| DomainPackError::NotFound(release_label(&release)))?;
    require_revision(&release_label(&release), record.revision, expected_revision)?;
    let previous_revision = record.revision;
    let authority = authority_from_parts(actor, tenant)?;
    let record = mutation(&mut record, &authority)?;
    let body = encode_release(&record)?;
    let revision = revision_to_sql(record.revision)?;
    let previous_revision_sql = revision_to_sql(previous_revision)?;
    let changed = transaction
        .execute(
            "UPDATE domain_pack_releases SET revision = ?1, body_json = ?2 \
             WHERE tenant_key = ?3 AND name = ?4 AND version = ?5 AND revision = ?6",
            params![
                revision,
                body,
                tenant_key(record.tenant_id.as_deref()),
                record.snapshot.release.name,
                record.snapshot.release.version.to_string(),
                previous_revision_sql
            ],
        )
        .map_err(|_| storage_error())?;
    if changed != 1 {
        return Err(DomainPackError::Conflict {
            pack: release_label(&release),
            expected: previous_revision,
            actual: load_release(&transaction, record.tenant_id.as_deref(), &release)?
                .map_or(0, |value| value.revision),
        });
    }
    transaction.commit().map_err(|_| storage_error())?;
    Ok(record)
}

fn load_release(
    connection: &Connection,
    tenant: Option<&str>,
    release: &DomainPackReleaseId,
) -> Result<Option<DomainPackRelease>, DomainPackError> {
    let key = tenant_key(tenant);
    let version = release.version.to_string();
    let length: Option<i64> = connection
        .query_row(
            "SELECT length(CAST(body_json AS BLOB)) FROM domain_pack_releases \
             WHERE tenant_key = ?1 AND name = ?2 AND version = ?3",
            params![key, release.name, version],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| storage_error())?;
    let Some(length) = length else {
        return Ok(None);
    };
    if length < 0 || usize::try_from(length).map_or(true, |value| value > MAX_RELEASE_RECORD_BYTES)
    {
        return Err(DomainPackError::Storage(
            "SQLite Domain Pack release body exceeds its bound".to_owned(),
        ));
    }
    let (projected_revision, body): (i64, String) = connection
        .query_row(
            "SELECT revision, body_json FROM domain_pack_releases \
             WHERE tenant_key = ?1 AND name = ?2 AND version = ?3",
            params![key, release.name, version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|_| storage_error())?;
    let projected_revision = revision_from_sql(projected_revision, "release")?;
    let record: DomainPackRelease = serde_json::from_str(&body).map_err(|_| corrupted_release())?;
    validate_release(&record).map_err(|_| corrupted_release())?;
    if record.tenant_id.as_deref() != tenant
        || &record.snapshot.release != release
        || record.revision != projected_revision
    {
        return Err(corrupted_release());
    }
    Ok(Some(record))
}

fn insert_release(
    connection: &Connection,
    record: &DomainPackRelease,
) -> Result<(), DomainPackError> {
    validate_release(record)?;
    let body = encode_release(record)?;
    let revision = revision_to_sql(record.revision)?;
    connection
        .execute(
            "INSERT INTO domain_pack_releases \
             (tenant_key, name, version, revision, body_json) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                tenant_key(record.tenant_id.as_deref()),
                record.snapshot.release.name,
                record.snapshot.release.version.to_string(),
                revision,
                body
            ],
        )
        .map_err(|_| storage_error())?;
    Ok(())
}

fn enforce_sqlite_release_count(
    connection: &Connection,
    tenant: Option<&str>,
    name: &str,
) -> Result<(), DomainPackError> {
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM domain_pack_releases WHERE tenant_key = ?1 AND name = ?2",
            params![tenant_key(tenant), name],
            |row| row.get(0),
        )
        .map_err(|_| storage_error())?;
    if count < 0 || usize::try_from(count).map_or(true, |value| value >= MAX_RELEASES_PER_PACK) {
        return Err(DomainPackError::Invalid(format!(
            "Domain Pack {name} exceeds {MAX_RELEASES_PER_PACK} installed releases"
        )));
    }
    Ok(())
}

fn encode_release(record: &DomainPackRelease) -> Result<String, DomainPackError> {
    validate_release(record)?;
    encode_bounded(record, MAX_RELEASE_RECORD_BYTES, "Domain Pack release")
}

fn load_activation(
    connection: &Connection,
    tenant: Option<&str>,
    name: &str,
) -> Result<Option<DomainPackActivation>, DomainPackError> {
    let key = tenant_key(tenant);
    let length: Option<i64> = connection
        .query_row(
            "SELECT length(CAST(body_json AS BLOB)) FROM domain_pack_activations \
             WHERE tenant_key = ?1 AND name = ?2",
            params![key, name],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| storage_error())?;
    let Some(length) = length else {
        return Ok(None);
    };
    if length < 0
        || usize::try_from(length).map_or(true, |value| value > MAX_ACTIVATION_RECORD_BYTES)
    {
        return Err(DomainPackError::Storage(
            "SQLite Domain Pack activation body exceeds its bound".to_owned(),
        ));
    }
    let (projected_revision, body): (i64, String) = connection
        .query_row(
            "SELECT revision, body_json FROM domain_pack_activations \
             WHERE tenant_key = ?1 AND name = ?2",
            params![key, name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|_| storage_error())?;
    let projected_revision = revision_from_sql(projected_revision, "activation")?;
    let activation: DomainPackActivation =
        serde_json::from_str(&body).map_err(|_| corrupted_activation())?;
    validate_activation(&activation).map_err(|_| corrupted_activation())?;
    if activation.tenant_id.as_deref() != tenant
        || activation.name != name
        || activation.revision != projected_revision
    {
        return Err(corrupted_activation());
    }
    Ok(Some(activation))
}

fn write_activation(
    connection: &Connection,
    activation: &DomainPackActivation,
) -> Result<(), DomainPackError> {
    validate_activation(activation)?;
    let body = encode_bounded(
        activation,
        MAX_ACTIVATION_RECORD_BYTES,
        "Domain Pack activation",
    )?;
    let revision = revision_to_sql(activation.revision)?;
    let changed = connection
        .execute(
            "INSERT INTO domain_pack_activations \
             (tenant_key, name, revision, body_json) VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(tenant_key, name) DO UPDATE SET \
             revision = excluded.revision, body_json = excluded.body_json \
             WHERE domain_pack_activations.revision + 1 = excluded.revision \
                OR (domain_pack_activations.revision = excluded.revision \
                    AND domain_pack_activations.body_json = excluded.body_json)",
            params![
                tenant_key(activation.tenant_id.as_deref()),
                activation.name,
                revision,
                body
            ],
        )
        .map_err(|_| storage_error())?;
    if changed != 1 {
        return Err(DomainPackError::Conflict {
            pack: activation.name.clone(),
            expected: activation.revision.saturating_sub(1),
            actual: load_activation(
                connection,
                activation.tenant_id.as_deref(),
                &activation.name,
            )?
            .map_or(0, |value| value.revision),
        });
    }
    Ok(())
}

fn encode_bounded<T: Serialize>(
    value: &T,
    maximum: usize,
    kind: &str,
) -> Result<String, DomainPackError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| DomainPackError::Invalid(format!("cannot encode {kind}")))?;
    if bytes.len() > maximum {
        return Err(DomainPackError::Invalid(format!(
            "{kind} exceeds {maximum} bytes"
        )));
    }
    String::from_utf8(bytes)
        .map_err(|_| DomainPackError::Invalid(format!("cannot encode {kind} as UTF-8")))
}

fn tenant_key(tenant: Option<&str>) -> &str {
    tenant.unwrap_or("")
}

fn revision_to_sql(revision: u64) -> Result<i64, DomainPackError> {
    i64::try_from(revision).map_err(|_| {
        DomainPackError::Invalid("Domain Pack revision exceeds the durable range".to_owned())
    })
}

fn revision_from_sql(revision: i64, kind: &str) -> Result<u64, DomainPackError> {
    u64::try_from(revision).map_err(|_| {
        DomainPackError::Storage(format!("SQLite Domain Pack {kind} revision is corrupt"))
    })
}

fn storage_error() -> DomainPackError {
    DomainPackError::Storage("SQLite operation failed".to_owned())
}

fn corrupted_release() -> DomainPackError {
    DomainPackError::Storage("SQLite Domain Pack release is corrupt".to_owned())
}

fn corrupted_activation() -> DomainPackError {
    DomainPackError::Storage("SQLite Domain Pack activation is corrupt".to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        path::{Path, PathBuf},
        sync::{Arc, Barrier},
        thread,
    };

    use semver::Version;

    use super::*;
    use crate::{DomainPackComponentPin, DomainPackInventory};

    #[tokio::test]
    async fn memory_store_enforces_the_complete_governance_contract() {
        let store = MemoryDomainPackStore::new();
        exercise_governance_contract(&store).await;
    }

    #[tokio::test]
    async fn sqlite_store_persists_the_complete_governance_contract() {
        let path = temporary_database_path("contract");
        let store = SqliteDomainPackStore::open(&path).expect("open store");
        let expected = exercise_governance_contract(&store).await;
        drop(store);

        let reopened = SqliteDomainPackStore::open(&path).expect("reopen store");
        let actual = reopened
            .activation("assistant", &authority("tenant-a", "reader"))
            .await
            .expect("load activation")
            .expect("persisted activation");
        assert_eq!(actual, expected);
        drop(reopened);
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn failed_evaluation_is_terminal_and_cannot_be_approved() {
        let store = MemoryDomainPackStore::new();
        let snapshot = snapshot(3, '3', 'c');
        let release = snapshot.release.clone();
        let tenant = authority("tenant-a", "installer");
        store
            .install(snapshot, &tenant)
            .await
            .expect("install release");
        let evaluated = store
            .evaluate(
                &release,
                1,
                digest('3'),
                digest('d'),
                false,
                &authority("tenant-a", "evaluator"),
            )
            .await
            .expect("record failed evaluation");
        assert_eq!(evaluated.revision, 2);

        let error = store
            .approve(&release, 2, digest('e'), &authority("tenant-a", "approver"))
            .await
            .expect_err("failed evaluation cannot pass promotion");
        assert!(matches!(error, DomainPackError::Invalid(_)));
        let repeat = store
            .evaluate(
                &release,
                2,
                digest('3'),
                digest('f'),
                true,
                &authority("tenant-a", "evaluator"),
            )
            .await
            .expect_err("terminal evaluation cannot be replaced");
        assert!(matches!(repeat, DomainPackError::Invalid(_)));
    }

    #[test]
    fn rollback_history_retains_the_newest_bounded_window() {
        let mut rollback = Vec::new();
        for major in 0..=MAX_ROLLBACK_HISTORY {
            push_rollback(
                &mut rollback,
                DomainPackReleaseId {
                    name: "assistant".to_owned(),
                    version: Version::new(u64::try_from(major).expect("bounded major"), 0, 0),
                },
            )
            .expect("append rollback release");
        }
        assert_eq!(rollback.len(), MAX_ROLLBACK_HISTORY);
        assert_eq!(rollback.first().map(|id| id.version.major), Some(1));
        assert_eq!(
            rollback.last().map(|id| id.version.major),
            Some(u64::try_from(MAX_ROLLBACK_HISTORY).expect("bounded history"))
        );
    }

    #[tokio::test]
    async fn sqlite_compare_and_swap_has_one_winner_across_connections() {
        let path = temporary_database_path("cas");
        let setup = SqliteDomainPackStore::open(&path).expect("open setup store");
        let snapshot = snapshot(1, '1', 'a');
        let verified = approve(&setup, snapshot, "tenant-a").await;
        drop(setup);

        let first = SqliteDomainPackStore::open(&path).expect("open first store");
        let second = SqliteDomainPackStore::open(&path).expect("open second store");
        let barrier = Arc::new(Barrier::new(2));
        let first_barrier = barrier.clone();
        let first_verified = verified.clone();
        let first_thread = thread::spawn(move || {
            first_barrier.wait();
            runtime().block_on(first.activate(
                first_verified,
                0,
                &authority("tenant-a", "operator-a"),
            ))
        });
        let second_barrier = barrier;
        let second_thread = thread::spawn(move || {
            second_barrier.wait();
            runtime().block_on(second.activate(verified, 0, &authority("tenant-a", "operator-b")))
        });
        let first_result = first_thread.join().expect("join first activation");
        let second_result = second_thread.join().expect("join second activation");
        assert_eq!(
            usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
            1
        );
        let failure = if let Err(error) = first_result {
            error
        } else {
            second_result.expect_err("second activation must lose")
        };
        assert!(matches!(
            failure,
            DomainPackError::Conflict {
                expected: 0,
                actual: 1,
                ..
            }
        ));
        remove_database_files(&path);
    }

    #[tokio::test]
    async fn sqlite_rejects_projection_drift_and_partial_schema() {
        let corrupt_path = temporary_database_path("corrupt");
        let store = SqliteDomainPackStore::open(&corrupt_path).expect("open store");
        let snapshot = snapshot(1, '1', 'a');
        let release = snapshot.release.clone();
        store
            .install(snapshot, &authority("tenant-a", "installer"))
            .await
            .expect("install release");
        drop(store);
        let connection = Connection::open(&corrupt_path).expect("open raw database");
        connection
            .execute(
                "UPDATE domain_pack_releases SET revision = -1 \
                 WHERE tenant_key = 'tenant-a' AND name = 'assistant'",
                [],
            )
            .expect("corrupt projection");
        drop(connection);
        let store = SqliteDomainPackStore::open(&corrupt_path).expect("reopen corrupt store");
        let error = store
            .get(&release, &authority("tenant-a", "reader"))
            .await
            .expect_err("projection drift must fail closed");
        assert!(matches!(error, DomainPackError::Storage(_)));
        drop(store);
        remove_database_files(&corrupt_path);

        let partial_path = temporary_database_path("partial");
        let connection = Connection::open(&partial_path).expect("open partial database");
        connection
            .execute_batch(
                "CREATE TABLE domain_pack_metadata (
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                 );",
            )
            .expect("create partial schema");
        drop(connection);
        let error = SqliteDomainPackStore::open(&partial_path)
            .err()
            .expect("partial schema must fail closed");
        assert!(matches!(error, DomainPackError::Storage(_)));
        remove_database_files(&partial_path);

        let unknown_path = temporary_database_path("unknown-schema");
        let store = SqliteDomainPackStore::open(&unknown_path).expect("initialize schema");
        drop(store);
        let connection = Connection::open(&unknown_path).expect("open schema database");
        connection
            .execute(
                "UPDATE domain_pack_metadata SET schema_version = 2 \
                 WHERE singleton = 1",
                [],
            )
            .expect("change schema coordinate");
        drop(connection);
        let error = SqliteDomainPackStore::open(&unknown_path)
            .err()
            .expect("unknown schema must fail closed");
        assert!(matches!(error, DomainPackError::Storage(_)));
        remove_database_files(&unknown_path);
    }

    async fn exercise_governance_contract(store: &dyn DomainPackStore) -> DomainPackActivation {
        let first_snapshot = snapshot(1, '1', 'a');
        let first_release = first_snapshot.release.clone();
        let first_verified = approve(store, first_snapshot.clone(), "tenant-a").await;

        let duplicate = store
            .install(
                first_snapshot.clone(),
                &authority("tenant-a", "different-installer"),
            )
            .await
            .expect("idempotent install");
        assert_eq!(duplicate.revision, 3);
        assert_eq!(
            duplicate.installed_by,
            authority("tenant-a", "installer").actor().clone()
        );
        assert!(
            store
                .get(&first_release, &authority("tenant-b", "reader"))
                .await
                .expect("cross-tenant read")
                .is_none()
        );

        let first_activation = store
            .activate(
                first_verified.clone(),
                0,
                &authority("tenant-a", "operator"),
            )
            .await
            .expect("activate first release");
        assert_eq!(first_activation.revision, 1);
        let idempotent = store
            .activate(
                first_verified.clone(),
                1,
                &authority("tenant-a", "different-operator"),
            )
            .await
            .expect("idempotent activation");
        assert_eq!(idempotent, first_activation);
        let binding = store
            .bind(
                first_verified.clone(),
                1,
                &authority("tenant-a", "executor"),
            )
            .await
            .expect("bind active inventory");
        assert_eq!(binding.tenant_id(), Some("tenant-a"));
        assert_eq!(binding.snapshot(), &first_snapshot);
        assert_eq!(binding.activation_revision(), 1);
        assert_eq!(
            binding.inventory_sha256(),
            first_verified.inventory_sha256()
        );
        let engine_binding = binding
            .to_execution_binding()
            .expect("convert governed binding");
        assert_eq!(engine_binding.issuer(), "domain-pack");
        assert_eq!(engine_binding.name(), first_snapshot.release.name);
        assert_eq!(
            engine_binding.version(),
            first_snapshot.release.version.to_string()
        );
        assert_eq!(
            engine_binding.configuration_sha256(),
            first_snapshot.content_sha256
        );
        assert_eq!(
            engine_binding.environment_sha256(),
            first_verified.inventory_sha256()
        );
        assert_eq!(engine_binding.revision(), 1);
        assert_eq!(engine_binding.tenant_id(), Some("tenant-a"));

        let mut drifted_inventory = first_snapshot.components.clone();
        drifted_inventory.push(DomainPackComponentPin {
            kind: DomainPackComponentKind::Skill,
            name: "unrelated-drift".to_owned(),
            version: "skill:v1".to_owned(),
            content_sha256: digest('b'),
        });
        let drifted = first_snapshot
            .verify(&DomainPackInventory::new(drifted_inventory).expect("drifted inventory"))
            .expect("required pins still match");
        let drift_error = store
            .bind(drifted, 1, &authority("tenant-a", "executor"))
            .await
            .expect_err("complete inventory drift must invalidate the binding");
        assert!(matches!(drift_error, DomainPackError::Invalid(_)));

        let stale = store
            .activate(
                first_verified.clone(),
                0,
                &authority("tenant-a", "operator"),
            )
            .await
            .expect_err("stale activation revision");
        assert!(matches!(
            stale,
            DomainPackError::Conflict {
                expected: 0,
                actual: 1,
                ..
            }
        ));

        let second_snapshot = snapshot(2, '2', 'b');
        let second_verified = approve(store, second_snapshot, "tenant-a").await;
        let second_activation = store
            .activate(second_verified, 1, &authority("tenant-a", "operator"))
            .await
            .expect("activate second release");
        assert_eq!(
            second_activation.active.as_ref().map(|id| &id.version),
            Some(&Version::new(2, 0, 0))
        );
        assert_eq!(second_activation.rollback, vec![first_release.clone()]);

        let rolled_back = store
            .rollback(first_verified, 2, &authority("tenant-a", "operator"))
            .await
            .expect("roll back to first release");
        assert_eq!(rolled_back.active, Some(first_release));
        assert_eq!(rolled_back.revision, 3);

        let deactivated = store
            .deactivate("assistant", 3, &authority("tenant-a", "operator"))
            .await
            .expect("deactivate release");
        assert!(deactivated.active.is_none());
        assert!(deactivated.inventory_sha256.is_none());
        assert_eq!(deactivated.revision, 4);
        assert!(
            store
                .activation("assistant", &authority("tenant-b", "reader"))
                .await
                .expect("cross-tenant activation read")
                .is_none()
        );

        let tenant_b = store
            .install(first_snapshot, &authority("tenant-b", "tenant-b-installer"))
            .await
            .expect("same release identity in another tenant");
        assert_eq!(tenant_b.tenant_id(), Some("tenant-b"));
        deactivated
    }

    async fn approve(
        store: &dyn DomainPackStore,
        snapshot: DomainPackSnapshot,
        tenant: &str,
    ) -> VerifiedDomainPack {
        let release = snapshot.release.clone();
        let suite_digest = snapshot
            .components
            .iter()
            .find(|component| component.kind == DomainPackComponentKind::Evaluation)
            .expect("evaluation component")
            .content_sha256
            .clone();
        let inventory =
            DomainPackInventory::new(snapshot.components.clone()).expect("exact inventory");
        let verified = snapshot.verify(&inventory).expect("verify inventory");
        let installed = store
            .install(snapshot, &authority(tenant, "installer"))
            .await
            .expect("install release");
        assert_eq!(installed.revision, 1);
        let evaluated = store
            .evaluate(
                &release,
                1,
                suite_digest,
                digest('d'),
                true,
                &authority(tenant, "evaluator"),
            )
            .await
            .expect("evaluate release");
        assert_eq!(evaluated.revision, 2);
        let same_actor = store
            .approve(&release, 2, digest('e'), &authority(tenant, "evaluator"))
            .await
            .expect_err("evaluator cannot self-approve");
        assert!(matches!(same_actor, DomainPackError::Invalid(_)));
        let approved = store
            .approve(&release, 2, digest('f'), &authority(tenant, "approver"))
            .await
            .expect("approve release");
        assert_eq!(approved.revision, 3);
        verified
    }

    fn snapshot(major: u64, evaluation_digest: char, tool_digest: char) -> DomainPackSnapshot {
        DomainPackSnapshot::seal(
            DomainPackReleaseId {
                name: "assistant".to_owned(),
                version: Version::new(major, 0, 0),
            },
            format!("Enterprise assistant release {major}"),
            vec![
                DomainPackComponentPin {
                    kind: DomainPackComponentKind::Evaluation,
                    name: "promotion".to_owned(),
                    version: format!("eval:v{major}"),
                    content_sha256: digest(evaluation_digest),
                },
                DomainPackComponentPin {
                    kind: DomainPackComponentKind::Tool,
                    name: "orders.read".to_owned(),
                    version: format!("tool:v{major}"),
                    content_sha256: digest(tool_digest),
                },
            ],
        )
        .expect("seal snapshot")
    }

    fn authority(tenant: &str, subject: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "domain-pack-tests".to_owned(),
                subject: subject.to_owned(),
            },
            Some(tenant.to_owned()),
        )
        .expect("test authority")
    }

    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime")
    }

    fn temporary_database_path(label: &str) -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "y-harness-domain-pack-{label}-{}-{timestamp}.db",
            std::process::id()
        ))
    }

    fn remove_database_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
