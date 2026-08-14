//! Atomic persistence, discovery, and compare-and-swap ports for Effects.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task};

use super::{
    Effect, EffectApplyOutcome, EffectCommand, EffectCreateRequest, EffectPersistenceProtocol,
    EffectStatus, EffectStoredRecord, EffectStoredRecordParts, MAX_EFFECT_IDENTITY_BYTES,
    MAX_EFFECT_JSON_BYTES, validate_application_time, validate_identity,
};
use crate::{
    AuthorityContext, EffectId, EffectLeaseId, HarnessError, HarnessFuture,
    sqlite::{bounded_text, open_read_only},
};

/// Current durable Effect Ledger schema.
pub const EFFECT_LEDGER_SCHEMA_VERSION: u32 = 1;
const MAX_EFFECT_PAGE: usize = 256;
const MAX_EFFECT_STATUS_BYTES: usize = 9;
const MAX_EFFECT_CAPABILITY_BYTES: usize = 128;

/// Immutable revisioned Effect projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectSnapshot {
    pub(super) id: EffectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tenant_id: Option<String>,
    pub(super) revision: u64,
    pub(super) effect: Effect,
}

impl EffectSnapshot {
    /// Returns the stable Effect identity.
    #[must_use]
    pub fn id(&self) -> &EffectId {
        &self.id
    }

    /// Returns the immutable tenant boundary.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }

    /// Returns the positive optimistic-concurrency revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Returns the validated Effect aggregate.
    #[must_use]
    pub fn effect(&self) -> &Effect {
        &self.effect
    }
}

/// Stable identity cursor for lifecycle pages.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectPageCursor {
    /// Last returned Effect identity.
    pub effect_id: EffectId,
}

/// One bounded identity-ordered Effect page.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectPage {
    /// Effects in stable identity order.
    pub effects: Vec<EffectSnapshot>,
    /// Cursor for a later page.
    pub next_cursor: Option<EffectPageCursor>,
    /// Whether another matching Effect exists.
    pub has_more: bool,
}

/// Result of one idempotent Effect command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectCommandResult {
    /// Current durable snapshot after application or duplicate recognition.
    pub snapshot: EffectSnapshot,
    /// Whether the command changed the durable revision.
    pub outcome: EffectApplyOutcome,
}

/// One expired execution lease discovered from authoritative Effect state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectDueLease {
    /// Stable Effect identity.
    pub effect_id: EffectId,
    /// Immutable tenant boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// Revision observed with the lease.
    pub revision: u64,
    /// Exact execution fence.
    pub lease_id: EffectLeaseId,
    /// Positive attempt owned by the lease.
    pub attempt: u32,
    /// Exclusive expiration boundary.
    pub expires_at_ms: u64,
}

/// One bounded identity sweep over Effect records.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EffectDueScanPage {
    /// Expired leases among visited Effects.
    pub due: Vec<EffectDueLease>,
    /// Last visited Effect identity.
    pub next_after_effect_id: Option<EffectId>,
    /// Whether another Effect remains in the tenant sweep.
    pub has_more: bool,
    /// Number of authoritative Effects visited.
    pub scanned: usize,
}

/// Atomic persistence, discovery, and mutation boundary for durable Effects.
pub trait EffectCoordinator: Send + Sync {
    /// Creates or recognizes one unscoped Effect.
    fn create<'a>(
        &'a self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, EffectSnapshot> {
        Box::pin(async move {
            self.create_as(
                effect_id,
                request,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Creates or recognizes one intent under trusted tenant authority.
    ///
    /// The `(tenant, capability, operation, idempotency_key)` tuple is unique.
    /// Exact duplicate creation returns the already committed canonical Effect,
    /// even when the caller repeats it with a different proposed `effect_id`.
    fn create_as<'a>(
        &'a self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectSnapshot>;

    /// Loads one unscoped Effect.
    fn load<'a>(&'a self, effect_id: &'a EffectId) -> HarnessFuture<'a, Option<EffectSnapshot>> {
        Box::pin(async move {
            self.load_as(effect_id, &AuthorityContext::local_process())
                .await
        })
    }

    /// Loads one Effect inside the exact trusted tenant boundary.
    fn load_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<EffectSnapshot>>;

    /// Lists one bounded unscoped lifecycle page.
    fn list<'a>(
        &'a self,
        status: Option<&'a str>,
        after: Option<&'a EffectPageCursor>,
        limit: usize,
    ) -> HarnessFuture<'a, EffectPage> {
        Box::pin(async move {
            self.list_as(status, after, limit, &AuthorityContext::local_process())
                .await
        })
    }

    /// Lists one bounded page inside the exact trusted tenant boundary.
    fn list_as<'a>(
        &'a self,
        status: Option<&'a str>,
        after: Option<&'a EffectPageCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectPage>;

    /// Scans one bounded unscoped page for expired leases.
    fn scan_due<'a>(
        &'a self,
        at_ms: u64,
        after_effect_id: Option<&'a EffectId>,
        scan_limit: usize,
    ) -> HarnessFuture<'a, EffectDueScanPage> {
        Box::pin(async move {
            self.scan_due_as(
                at_ms,
                after_effect_id,
                scan_limit,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Scans expired leases inside the exact trusted tenant boundary.
    fn scan_due_as<'a>(
        &'a self,
        at_ms: u64,
        after_effect_id: Option<&'a EffectId>,
        scan_limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDueScanPage>;

    /// Applies one unscoped actor-bound command.
    fn apply<'a>(
        &'a self,
        effect_id: &'a EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
    ) -> HarnessFuture<'a, EffectCommandResult> {
        Box::pin(async move {
            self.apply_as(
                effect_id,
                expected_revision,
                command,
                applied_at_ms,
                &AuthorityContext::local_process(),
            )
            .await
        })
    }

    /// Applies one command with exact revision and tenant fencing.
    ///
    /// An exact duplicate is recognized before the revision comparison.
    fn apply_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectCommandResult>;
}

#[derive(Default)]
struct MemoryEffectState {
    effects: BTreeMap<(String, EffectId), EffectSnapshot>,
    idempotency: BTreeMap<(String, String, String, String), EffectId>,
}

/// Process-local durable-semantics Effect Coordinator.
#[derive(Default)]
pub struct MemoryEffectCoordinator {
    state: Mutex<MemoryEffectState>,
}

impl MemoryEffectCoordinator {
    /// Creates an empty in-memory ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl EffectCoordinator for MemoryEffectCoordinator {
    fn create_as<'a>(
        &'a self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectSnapshot> {
        Box::pin(async move {
            let prepared = EffectPersistenceProtocol::prepare_create(
                effect_id.clone(),
                request.clone(),
                applied_at_ms,
                authority,
            )?;
            let tenant = tenant_storage_key(authority.tenant_id()).to_owned();
            let identity = (tenant.clone(), effect_id.clone());
            let idempotency = idempotency_coordinate(&tenant, &request);
            let mut state = self.state.lock().await;
            if let Some(existing) = state.effects.get(&identity) {
                if EffectPersistenceProtocol::create_matches(existing, &request, authority)? {
                    return Ok(existing.clone());
                }
                return Err(HarnessError::Effect(format!(
                    "Effect {effect_id} already exists with different creation content"
                )));
            }
            if let Some(existing_id) = state.idempotency.get(&idempotency) {
                let existing = state
                    .effects
                    .get(&(tenant.clone(), existing_id.clone()))
                    .ok_or_else(|| {
                        HarnessError::Effect(
                            "Effect idempotency index points to missing state".to_owned(),
                        )
                    })?;
                if EffectPersistenceProtocol::create_matches(existing, &request, authority)? {
                    return Ok(existing.clone());
                }
                return Err(HarnessError::Effect(
                    "Effect idempotency key is already bound to different creation content"
                        .to_owned(),
                ));
            }
            let snapshot = prepared.into_committed_snapshot();
            state.idempotency.insert(idempotency, effect_id);
            state.effects.insert(identity, snapshot.clone());
            Ok(snapshot)
        })
    }

    fn load_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<EffectSnapshot>> {
        Box::pin(async move {
            validate_access(effect_id, authority)?;
            let state = self.state.lock().await;
            Ok(state
                .effects
                .get(&(
                    tenant_storage_key(authority.tenant_id()).to_owned(),
                    effect_id.clone(),
                ))
                .cloned())
        })
    }

    fn list_as<'a>(
        &'a self,
        status: Option<&'a str>,
        after: Option<&'a EffectPageCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectPage> {
        Box::pin(async move {
            validate_list(status, after, limit, authority)?;
            let tenant = tenant_storage_key(authority.tenant_id());
            let after_id = after.map(|cursor| cursor.effect_id.as_str());
            let state = self.state.lock().await;
            let candidates = state
                .effects
                .iter()
                .filter(|((candidate_tenant, id), snapshot)| {
                    candidate_tenant == tenant
                        && after_id.is_none_or(|after| id.as_str() > after)
                        && status
                            .is_none_or(|status| status_name(snapshot.effect.status()) == status)
                })
                .map(|(_, snapshot)| snapshot.clone())
                .take(limit.saturating_add(1))
                .collect();
            Ok(page_from_candidates(candidates, limit))
        })
    }

    fn scan_due_as<'a>(
        &'a self,
        at_ms: u64,
        after_effect_id: Option<&'a EffectId>,
        scan_limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDueScanPage> {
        Box::pin(async move {
            validate_due_scan(at_ms, after_effect_id, scan_limit, authority)?;
            let tenant = tenant_storage_key(authority.tenant_id());
            let after = after_effect_id.map(EffectId::as_str);
            let state = self.state.lock().await;
            let mut visited = state
                .effects
                .iter()
                .filter(|((candidate_tenant, id), _)| {
                    candidate_tenant == tenant && after.is_none_or(|after| id.as_str() > after)
                })
                .map(|(_, snapshot)| snapshot.clone())
                .take(scan_limit.saturating_add(1))
                .collect::<Vec<_>>();
            Ok(due_page_from_scan(&mut visited, scan_limit, at_ms))
        })
    }

    fn apply_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectCommandResult> {
        Box::pin(async move {
            validate_access(effect_id, authority)?;
            validate_expected_revision(expected_revision)?;
            let key = (
                tenant_storage_key(authority.tenant_id()).to_owned(),
                effect_id.clone(),
            );
            let mut state = self.state.lock().await;
            let current = state
                .effects
                .get(&key)
                .cloned()
                .ok_or_else(|| missing_effect(effect_id))?;
            let prepared = EffectPersistenceProtocol::prepare_command(
                &current,
                expected_revision,
                command,
                applied_at_ms,
                authority,
            )?;
            if prepared.changes_record() {
                state.effects.insert(key, prepared.snapshot().clone());
            }
            Ok(prepared.into_committed_result())
        })
    }
}

/// SQLite-backed Effect Coordinator for restart-safe local service operation.
pub struct SqliteEffectCoordinator {
    connection: Arc<StdMutex<Connection>>,
}

impl SqliteEffectCoordinator {
    /// Opens or creates a current Effect Ledger.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, HarnessError> {
        let path = path.as_ref().to_path_buf();
        let connection = task::spawn_blocking(move || {
            let mut connection = Connection::open(path).map_err(sql_error)?;
            configure_connection(&connection)?;
            initialize_or_validate(&mut connection)?;
            Ok::<_, HarnessError>(connection)
        })
        .await
        .map_err(|error| HarnessError::Effect(format!("Effect open task failed: {error}")))??;
        Ok(Self {
            connection: Arc::new(StdMutex::new(connection)),
        })
    }

    /// Validates an existing store read-only without creating or changing it.
    pub async fn validate_existing(path: impl AsRef<Path>) -> Result<(), HarnessError> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Ok(());
        }
        task::spawn_blocking(move || {
            let connection = open_read_only(&path).map_err(sql_error)?;
            validate_existing_store(&connection)
        })
        .await
        .map_err(|error| HarnessError::Effect(format!("Effect validation task failed: {error}")))?
    }
}

impl EffectCoordinator for SqliteEffectCoordinator {
    fn create_as<'a>(
        &'a self,
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectSnapshot> {
        let connection = self.connection.clone();
        let authority = authority.clone();
        Box::pin(async move {
            let prepared = EffectPersistenceProtocol::prepare_create(
                effect_id.clone(),
                request.clone(),
                applied_at_ms,
                &authority,
            )?;
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Effect("Effect lock poisoned".to_owned()))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                if let Some(existing) =
                    load_snapshot(&transaction, &effect_id, authority.tenant_id())?
                {
                    if EffectPersistenceProtocol::create_matches(&existing, &request, &authority)? {
                        transaction.commit().map_err(sql_error)?;
                        return Ok(existing);
                    }
                    return Err(HarnessError::Effect(format!(
                        "Effect {effect_id} already exists with different creation content"
                    )));
                }
                if let Some(existing) =
                    load_by_idempotency(&transaction, authority.tenant_id(), &request)?
                {
                    if EffectPersistenceProtocol::create_matches(&existing, &request, &authority)? {
                        transaction.commit().map_err(sql_error)?;
                        return Ok(existing);
                    }
                    return Err(HarnessError::Effect(
                        "Effect idempotency key is already bound to different creation content"
                            .to_owned(),
                    ));
                }
                let record = prepared.record();
                let changed = transaction
                    .execute(
                        "INSERT INTO effects
                         (tenant_id, effect_id, schema_version, revision, capability,
                          operation, idempotency_key, created_at_ms, status, effect_json)
                         VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            record.effect_id(),
                            record.schema_version(),
                            record.capability(),
                            record.operation(),
                            record.idempotency_key(),
                            sql_time(record.created_at_ms())?,
                            record.status(),
                            record.effect_json()
                        ],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(HarnessError::Effect(
                        "Effect creation changed an unexpected row count".to_owned(),
                    ));
                }
                transaction.commit().map_err(sql_error)?;
                Ok(prepared.into_committed_snapshot())
            })
            .await
            .map_err(|error| {
                HarnessError::Effect(format!("Effect creation task failed: {error}"))
            })?
        })
    }

    fn load_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, Option<EffectSnapshot>> {
        let connection = self.connection.clone();
        let effect_id = effect_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_access(&effect_id, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Effect("Effect lock poisoned".to_owned()))?;
                load_snapshot(&connection, &effect_id, authority.tenant_id())
            })
            .await
            .map_err(|error| HarnessError::Effect(format!("Effect load task failed: {error}")))?
        })
    }

    fn list_as<'a>(
        &'a self,
        status: Option<&'a str>,
        after: Option<&'a EffectPageCursor>,
        limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectPage> {
        let connection = self.connection.clone();
        let status = status.map(str::to_owned);
        let after = after.cloned();
        let authority = authority.clone();
        Box::pin(async move {
            validate_list(status.as_deref(), after.as_ref(), limit, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Effect("Effect lock poisoned".to_owned()))?;
                let fetch = limit
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::Effect("Effect page limit overflow".to_owned()))?;
                let mut statement = connection
                    .prepare(
                        "SELECT length(CAST(effect_id AS BLOB)), effect_id,
                                schema_version, revision,
                                length(CAST(capability AS BLOB)), capability,
                                length(CAST(operation AS BLOB)), operation,
                                length(CAST(idempotency_key AS BLOB)), idempotency_key,
                                created_at_ms,
                                length(CAST(status AS BLOB)), status,
                                length(CAST(effect_json AS BLOB)), effect_json
                         FROM effects
                         WHERE tenant_id = ?1 AND effect_id > ?2
                           AND (?3 IS NULL OR status = ?3)
                         ORDER BY effect_id ASC
                         LIMIT ?4",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            after
                                .as_ref()
                                .map_or("", |cursor| cursor.effect_id.as_str()),
                            status,
                            i64::try_from(fetch).map_err(|_| {
                                HarnessError::Effect("Effect page limit exceeds SQLite".to_owned())
                            })?
                        ],
                        read_row,
                    )
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                let candidates = rows
                    .into_iter()
                    .map(|row| decode_row(authority.tenant_id(), row))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(page_from_candidates(candidates, limit))
            })
            .await
            .map_err(|error| HarnessError::Effect(format!("Effect list task failed: {error}")))?
        })
    }

    fn scan_due_as<'a>(
        &'a self,
        at_ms: u64,
        after_effect_id: Option<&'a EffectId>,
        scan_limit: usize,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectDueScanPage> {
        let connection = self.connection.clone();
        let after_effect_id = after_effect_id.cloned();
        let authority = authority.clone();
        Box::pin(async move {
            validate_due_scan(at_ms, after_effect_id.as_ref(), scan_limit, &authority)?;
            task::spawn_blocking(move || {
                let connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Effect("Effect lock poisoned".to_owned()))?;
                let fetch = scan_limit
                    .checked_add(1)
                    .ok_or_else(|| HarnessError::Effect("Effect scan limit overflow".to_owned()))?;
                let mut statement = connection
                    .prepare(
                        "SELECT length(CAST(effect_id AS BLOB)), effect_id,
                                schema_version, revision,
                                length(CAST(capability AS BLOB)), capability,
                                length(CAST(operation AS BLOB)), operation,
                                length(CAST(idempotency_key AS BLOB)), idempotency_key,
                                created_at_ms,
                                length(CAST(status AS BLOB)), status,
                                length(CAST(effect_json AS BLOB)), effect_json
                         FROM effects
                         WHERE tenant_id = ?1 AND effect_id > ?2
                         ORDER BY effect_id ASC
                         LIMIT ?3",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            tenant_storage_key(authority.tenant_id()),
                            after_effect_id.as_ref().map_or("", EffectId::as_str),
                            i64::try_from(fetch).map_err(|_| {
                                HarnessError::Effect("Effect scan limit exceeds SQLite".to_owned())
                            })?
                        ],
                        read_row,
                    )
                    .map_err(sql_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(sql_error)?;
                let mut visited = rows
                    .into_iter()
                    .map(|row| decode_row(authority.tenant_id(), row))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(due_page_from_scan(&mut visited, scan_limit, at_ms))
            })
            .await
            .map_err(|error| HarnessError::Effect(format!("Effect scan task failed: {error}")))?
        })
    }

    fn apply_as<'a>(
        &'a self,
        effect_id: &'a EffectId,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &'a AuthorityContext,
    ) -> HarnessFuture<'a, EffectCommandResult> {
        let connection = self.connection.clone();
        let effect_id = effect_id.clone();
        let authority = authority.clone();
        Box::pin(async move {
            validate_access(&effect_id, &authority)?;
            validate_expected_revision(expected_revision)?;
            task::spawn_blocking(move || {
                let mut connection = connection
                    .lock()
                    .map_err(|_| HarnessError::Effect("Effect lock poisoned".to_owned()))?;
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                let current = load_snapshot(&transaction, &effect_id, authority.tenant_id())?
                    .ok_or_else(|| missing_effect(&effect_id))?;
                let prepared = EffectPersistenceProtocol::prepare_command(
                    &current,
                    expected_revision,
                    command,
                    applied_at_ms,
                    &authority,
                )?;
                if !prepared.changes_record() {
                    transaction.commit().map_err(sql_error)?;
                    return Ok(prepared.into_committed_result());
                }
                let record = prepared.record();
                let changed = transaction
                    .execute(
                        "UPDATE effects
                         SET revision = ?1, status = ?2, effect_json = ?3
                         WHERE tenant_id = ?4 AND effect_id = ?5 AND revision = ?6",
                        params![
                            sql_revision(record.revision())?,
                            record.status(),
                            record.effect_json(),
                            tenant_storage_key(authority.tenant_id()),
                            effect_id.as_str(),
                            sql_revision(current.revision)?
                        ],
                    )
                    .map_err(sql_error)?;
                if changed != 1 {
                    return Err(HarnessError::Effect(
                        "Effect atomic update changed an unexpected row count".to_owned(),
                    ));
                }
                transaction.commit().map_err(sql_error)?;
                Ok(prepared.into_committed_result())
            })
            .await
            .map_err(|error| HarnessError::Effect(format!("Effect command task failed: {error}")))?
        })
    }
}

fn configure_connection(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(sql_error)?;
    let mode: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(sql_error)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(HarnessError::Effect(format!(
            "SQLite refused WAL mode and selected {mode}"
        )));
    }
    connection
        .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(sql_error)
}

fn initialize_or_validate(connection: &mut Connection) -> Result<(), HarnessError> {
    let has_meta = table_exists(connection, "effect_store_meta")?;
    let has_effects = table_exists(connection, "effects")?;
    match (has_meta, has_effects) {
        (false, false) => {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            transaction
                .execute_batch(
                    "
                    CREATE TABLE effect_store_meta (
                        singleton      INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL
                    );
                    INSERT INTO effect_store_meta
                        (singleton, schema_version) VALUES (1, 1);
                    CREATE TABLE effects (
                        tenant_id       TEXT NOT NULL,
                        effect_id       TEXT NOT NULL,
                        schema_version  INTEGER NOT NULL,
                        revision        INTEGER NOT NULL CHECK(revision > 0),
                        capability      TEXT NOT NULL,
                        operation       TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        created_at_ms   INTEGER NOT NULL CHECK(created_at_ms > 0),
                        status          TEXT NOT NULL,
                        effect_json     TEXT NOT NULL,
                        PRIMARY KEY (tenant_id, effect_id),
                        UNIQUE (tenant_id, capability, operation, idempotency_key)
                    );
                    CREATE INDEX effects_lifecycle
                        ON effects (tenant_id, status, effect_id);
                    ",
                )
                .map_err(sql_error)?;
            transaction.commit().map_err(sql_error)
        }
        (true, true) => validate_store(connection),
        _ => Err(HarnessError::Effect(
            "SQLite Effect Ledger is partial".to_owned(),
        )),
    }
}

fn validate_existing_store(connection: &Connection) -> Result<(), HarnessError> {
    let has_meta = table_exists(connection, "effect_store_meta")?;
    let has_effects = table_exists(connection, "effects")?;
    match (has_meta, has_effects) {
        (false, false) => Ok(()),
        (true, true) => validate_store(connection),
        _ => Err(HarnessError::Effect(
            "SQLite Effect Ledger is partial".to_owned(),
        )),
    }
}

fn validate_store(connection: &Connection) -> Result<(), HarnessError> {
    let versions = connection
        .prepare("SELECT singleton, schema_version FROM effect_store_meta")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(sql_error)?;
    if versions != vec![(1, i64::from(EFFECT_LEDGER_SCHEMA_VERSION))] {
        return Err(HarnessError::Effect(
            "SQLite Effect Ledger schema is unknown or malformed".to_owned(),
        ));
    }
    let invalid_rows: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM effects
             WHERE schema_version != ?1 OR revision <= 0
                OR length(CAST(tenant_id AS BLOB)) > 128
                OR length(CAST(effect_id AS BLOB)) = 0
                OR length(CAST(effect_id AS BLOB)) > ?2
                OR length(CAST(capability AS BLOB)) = 0
                OR length(CAST(capability AS BLOB)) > ?3
                OR length(CAST(operation AS BLOB)) = 0
                OR length(CAST(operation AS BLOB)) > ?3
                OR length(CAST(idempotency_key AS BLOB)) = 0
                OR length(CAST(idempotency_key AS BLOB)) > ?2
                OR created_at_ms <= 0
                OR status NOT IN
                   ('pending', 'claimed', 'unknown', 'applied', 'rejected', 'cancelled')
                OR length(CAST(effect_json AS BLOB)) > ?4",
            params![
                EFFECT_LEDGER_SCHEMA_VERSION,
                i64::try_from(MAX_EFFECT_IDENTITY_BYTES).map_err(|_| HarnessError::Effect(
                    "Effect identity size overflow".to_owned()
                ))?,
                i64::try_from(MAX_EFFECT_CAPABILITY_BYTES).map_err(|_| {
                    HarnessError::Effect("Effect capability size overflow".to_owned())
                })?,
                i64::try_from(MAX_EFFECT_JSON_BYTES)
                    .map_err(|_| HarnessError::Effect("Effect size overflow".to_owned()))?
            ],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if invalid_rows != 0 {
        return Err(HarnessError::Effect(
            "SQLite Effect Ledger contains invalid row metadata".to_owned(),
        ));
    }
    Ok(())
}

type EffectRow = (
    String,
    i64,
    i64,
    String,
    String,
    String,
    i64,
    String,
    String,
);

fn read_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EffectRow> {
    Ok((
        bounded_text(row, 0, 1, MAX_EFFECT_IDENTITY_BYTES, "Effect identity")?,
        row.get(2)?,
        row.get(3)?,
        bounded_text(row, 4, 5, MAX_EFFECT_CAPABILITY_BYTES, "Effect capability")?,
        bounded_text(row, 6, 7, MAX_EFFECT_CAPABILITY_BYTES, "Effect operation")?,
        bounded_text(
            row,
            8,
            9,
            MAX_EFFECT_IDENTITY_BYTES,
            "Effect idempotency key",
        )?,
        row.get(10)?,
        bounded_text(row, 11, 12, MAX_EFFECT_STATUS_BYTES, "Effect status")?,
        bounded_text(row, 13, 14, MAX_EFFECT_JSON_BYTES, "Effect aggregate")?,
    ))
}

fn load_snapshot(
    connection: &Connection,
    effect_id: &EffectId,
    tenant_id: Option<&str>,
) -> Result<Option<EffectSnapshot>, HarnessError> {
    connection
        .query_row(
            "SELECT length(CAST(effect_id AS BLOB)), effect_id,
                    schema_version, revision,
                    length(CAST(capability AS BLOB)), capability,
                    length(CAST(operation AS BLOB)), operation,
                    length(CAST(idempotency_key AS BLOB)), idempotency_key,
                    created_at_ms,
                    length(CAST(status AS BLOB)), status,
                    length(CAST(effect_json AS BLOB)), effect_json
             FROM effects WHERE tenant_id = ?1 AND effect_id = ?2",
            params![tenant_storage_key(tenant_id), effect_id.as_str()],
            read_row,
        )
        .optional()
        .map_err(sql_error)?
        .map(|row| decode_row(tenant_id, row))
        .transpose()
}

fn load_by_idempotency(
    connection: &Connection,
    tenant_id: Option<&str>,
    request: &EffectCreateRequest,
) -> Result<Option<EffectSnapshot>, HarnessError> {
    connection
        .query_row(
            "SELECT length(CAST(effect_id AS BLOB)), effect_id,
                    schema_version, revision,
                    length(CAST(capability AS BLOB)), capability,
                    length(CAST(operation AS BLOB)), operation,
                    length(CAST(idempotency_key AS BLOB)), idempotency_key,
                    created_at_ms,
                    length(CAST(status AS BLOB)), status,
                    length(CAST(effect_json AS BLOB)), effect_json
             FROM effects
             WHERE tenant_id = ?1 AND capability = ?2 AND operation = ?3
               AND idempotency_key = ?4",
            params![
                tenant_storage_key(tenant_id),
                request.operation.capability,
                request.operation.operation,
                request.idempotency_key
            ],
            read_row,
        )
        .optional()
        .map_err(sql_error)?
        .map(|row| decode_row(tenant_id, row))
        .transpose()
}

fn decode_row(tenant_id: Option<&str>, row: EffectRow) -> Result<EffectSnapshot, HarnessError> {
    let (
        effect_id,
        schema,
        revision,
        capability,
        operation,
        idempotency_key,
        created_at_ms,
        status,
        encoded,
    ) = row;
    let schema_version = u32::try_from(schema)
        .map_err(|_| HarnessError::Effect("invalid Effect schema version".to_owned()))?;
    let revision = u64::try_from(revision)
        .map_err(|_| HarnessError::Effect("invalid Effect revision".to_owned()))?;
    validate_expected_revision(revision)?;
    let created_at_ms = u64::try_from(created_at_ms)
        .map_err(|_| HarnessError::Effect("invalid Effect creation time".to_owned()))?;
    let record = EffectStoredRecord::try_from_parts(EffectStoredRecordParts {
        schema_version,
        effect_id,
        tenant_storage_key: tenant_storage_key(tenant_id).to_owned(),
        revision,
        capability,
        operation,
        idempotency_key,
        created_at_ms,
        status,
        effect_json: encoded,
    })?;
    EffectPersistenceProtocol::restore_in_scope(record, tenant_id)
}

fn validate_access(effect_id: &EffectId, authority: &AuthorityContext) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect Coordinator authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_identity("Effect", effect_id.as_str())
}

pub(super) fn validate_list(
    status: Option<&str>,
    after: Option<&EffectPageCursor>,
    limit: usize,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect list authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    if let Some(status) = status {
        validate_status_filter(status)?;
    }
    if let Some(cursor) = after {
        validate_identity("Effect page cursor", cursor.effect_id.as_str())?;
    }
    if !(1..=MAX_EFFECT_PAGE).contains(&limit) {
        return Err(HarnessError::Effect(format!(
            "Effect page limit must be 1-{MAX_EFFECT_PAGE}"
        )));
    }
    Ok(())
}

pub(super) fn validate_due_scan(
    at_ms: u64,
    after_effect_id: Option<&EffectId>,
    scan_limit: usize,
    authority: &AuthorityContext,
) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect due-scan authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_application_time(at_ms)?;
    if let Some(effect_id) = after_effect_id {
        validate_identity("Effect scan cursor", effect_id.as_str())?;
    }
    if !(1..=MAX_EFFECT_PAGE).contains(&scan_limit) {
        return Err(HarnessError::Effect(format!(
            "Effect scan limit must be 1-{MAX_EFFECT_PAGE}"
        )));
    }
    Ok(())
}

fn validate_expected_revision(revision: u64) -> Result<(), HarnessError> {
    if revision == 0 {
        Err(HarnessError::Effect(
            "Effect revision must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn validate_status_filter(status: &str) -> Result<(), HarnessError> {
    if matches!(
        status,
        "pending" | "claimed" | "unknown" | "applied" | "rejected" | "cancelled"
    ) {
        Ok(())
    } else {
        Err(HarnessError::Effect(
            "Effect status filter is unknown".to_owned(),
        ))
    }
}

pub(super) fn page_from_candidates(
    mut candidates: Vec<EffectSnapshot>,
    limit: usize,
) -> EffectPage {
    let has_more = candidates.len() > limit;
    candidates.truncate(limit);
    let next_cursor = candidates.last().map(|snapshot| EffectPageCursor {
        effect_id: snapshot.id.clone(),
    });
    EffectPage {
        effects: candidates,
        next_cursor,
        has_more,
    }
}

pub(super) fn due_page_from_scan(
    visited: &mut Vec<EffectSnapshot>,
    scan_limit: usize,
    at_ms: u64,
) -> EffectDueScanPage {
    let has_more = visited.len() > scan_limit;
    visited.truncate(scan_limit);
    let next_after_effect_id = visited.last().map(|snapshot| snapshot.id.clone());
    let due = visited
        .iter()
        .filter_map(|snapshot| {
            let EffectStatus::Claimed { lease } = snapshot.effect.status() else {
                return None;
            };
            (lease.expires_at_ms <= at_ms).then(|| EffectDueLease {
                effect_id: snapshot.id.clone(),
                tenant_id: snapshot.tenant_id.clone(),
                revision: snapshot.revision,
                lease_id: lease.id.clone(),
                attempt: lease.attempt,
                expires_at_ms: lease.expires_at_ms,
            })
        })
        .collect();
    EffectDueScanPage {
        due,
        next_after_effect_id,
        has_more,
        scanned: visited.len(),
    }
}

fn idempotency_coordinate(
    tenant: &str,
    request: &EffectCreateRequest,
) -> (String, String, String, String) {
    (
        tenant.to_owned(),
        request.operation.capability.clone(),
        request.operation.operation.clone(),
        request.idempotency_key.clone(),
    )
}

fn status_name(status: &EffectStatus) -> &'static str {
    match status {
        EffectStatus::Pending { .. } => "pending",
        EffectStatus::Claimed { .. } => "claimed",
        EffectStatus::Unknown { .. } => "unknown",
        EffectStatus::Applied { .. } => "applied",
        EffectStatus::Rejected { .. } => "rejected",
        EffectStatus::Cancelled { .. } => "cancelled",
    }
}

fn tenant_storage_key(tenant_id: Option<&str>) -> &str {
    tenant_id.unwrap_or("")
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
             )",
            [table],
            |row| row.get::<_, i64>(0),
        )
        .map(|exists| exists != 0)
        .map_err(sql_error)
}

fn sql_revision(value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value)
        .map_err(|_| HarnessError::Effect("Effect revision exceeds SQLite".to_owned()))
}

fn sql_time(value: u64) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| HarnessError::Effect("Effect time exceeds SQLite".to_owned()))
}

fn missing_effect(effect_id: &EffectId) -> HarnessError {
    HarnessError::Effect(format!("Effect {effect_id} does not exist"))
}

fn sql_error(error: rusqlite::Error) -> HarnessError {
    HarnessError::Effect(format!("SQLite Effect Ledger: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActorIdentity, EffectCommandId, EffectCommandKind, EffectLeaseId, EffectOperation,
    };

    fn authority(tenant: &str, subject: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: subject.to_owned(),
            },
            Some(tenant.to_owned()),
        )
        .expect("authority")
    }

    fn request(key: &str) -> EffectCreateRequest {
        EffectCreateRequest {
            command_id: EffectCommandId::from_static("create"),
            operation: EffectOperation {
                capability: "channel.email".to_owned(),
                operation: "send".to_owned(),
            },
            idempotency_key: key.to_owned(),
            input: serde_json::json!({"artifact":"message"}),
            not_before_ms: 10,
        }
    }

    fn command(id: &str, kind: EffectCommandKind) -> EffectCommand {
        EffectCommand {
            id: EffectCommandId::from_string(id.to_owned()),
            kind,
        }
    }

    async fn exercise(coordinator: &dyn EffectCoordinator) {
        let tenant = authority("tenant-a", "worker");
        let other = authority("tenant-b", "worker");
        let canonical = coordinator
            .create_as(
                EffectId::from_static("effect-a"),
                request("key"),
                10,
                &tenant,
            )
            .await
            .expect("create");
        let duplicate = coordinator
            .create_as(
                EffectId::from_static("effect-alias"),
                request("key"),
                11,
                &tenant,
            )
            .await
            .expect("idempotency duplicate");
        assert_eq!(duplicate.id(), canonical.id());
        assert!(
            coordinator
                .load_as(canonical.id(), &other)
                .await
                .expect("other tenant load")
                .is_none()
        );
        let claimed = coordinator
            .apply_as(
                canonical.id(),
                1,
                command(
                    "claim",
                    EffectCommandKind::Claim {
                        lease_id: EffectLeaseId::from_static("lease"),
                        lease_duration_ms: 1_000,
                    },
                ),
                20,
                &tenant,
            )
            .await
            .expect("claim");
        assert_eq!(claimed.snapshot.revision(), 2);
        let due = coordinator
            .scan_due_as(1_020, None, 8, &tenant)
            .await
            .expect("due scan");
        assert_eq!(due.due.len(), 1);
        assert_eq!(due.due[0].effect_id, *canonical.id());
    }

    fn temporary_path(label: &str) -> std::path::PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "y-harness-effect-{label}-{}-{stamp}.db",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn memory_coordinator_is_idempotent_tenant_fenced_and_due_scannable() {
        exercise(&MemoryEffectCoordinator::new()).await;
    }

    #[tokio::test]
    async fn sqlite_coordinator_reopens_and_preserves_effect_state() {
        let path = temporary_path("reopen");
        let coordinator = SqliteEffectCoordinator::open(&path).await.expect("open");
        exercise(&coordinator).await;
        drop(coordinator);
        SqliteEffectCoordinator::validate_existing(&path)
            .await
            .expect("validate");
        let reopened = SqliteEffectCoordinator::open(&path).await.expect("reopen");
        let loaded = reopened
            .load_as(
                &EffectId::from_static("effect-a"),
                &authority("tenant-a", "reader"),
            )
            .await
            .expect("load")
            .expect("present");
        assert_eq!(loaded.revision(), 2);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_two_connection_claim_race_has_one_revision_cas_winner() {
        let path = temporary_path("claim-race");
        let first = SqliteEffectCoordinator::open(&path)
            .await
            .expect("open first");
        let second = SqliteEffectCoordinator::open(&path)
            .await
            .expect("open second");
        let actor = authority("tenant-a", "worker");
        let effect_id = EffectId::from_static("effect-race");
        first
            .create_as(effect_id.clone(), request("race-key"), 10, &actor)
            .await
            .expect("create");

        let left = first.apply_as(
            &effect_id,
            1,
            command(
                "claim-left",
                EffectCommandKind::Claim {
                    lease_id: EffectLeaseId::from_static("lease-left"),
                    lease_duration_ms: 1_000,
                },
            ),
            20,
            &actor,
        );
        let right = second.apply_as(
            &effect_id,
            1,
            command(
                "claim-right",
                EffectCommandKind::Claim {
                    lease_id: EffectLeaseId::from_static("lease-right"),
                    lease_duration_ms: 1_000,
                },
            ),
            20,
            &actor,
        );
        let (left, right) = tokio::join!(left, right);
        let successes = usize::from(left.is_ok()) + usize::from(right.is_ok());
        assert_eq!(successes, 1, "exactly one revision-1 claim must commit");
        let conflict = if let Err(error) = left {
            error
        } else {
            right.expect_err("losing claim must conflict")
        };
        assert!(matches!(
            conflict,
            HarnessError::EffectConflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        let settled = first
            .load_as(&effect_id, &actor)
            .await
            .expect("load")
            .expect("present");
        assert_eq!(settled.revision(), 2);
        assert!(matches!(
            settled.effect().status(),
            EffectStatus::Claimed { .. }
        ));
        drop(first);
        drop(second);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_rejects_projection_drift_on_read() {
        let path = temporary_path("projection-drift");
        let coordinator = SqliteEffectCoordinator::open(&path).await.expect("open");
        let actor = authority("tenant-a", "worker");
        let effect_id = EffectId::from_static("effect-drift");
        coordinator
            .create_as(effect_id.clone(), request("drift-key"), 10, &actor)
            .await
            .expect("create");
        let connection = Connection::open(&path).expect("open tamper connection");
        connection
            .execute(
                "UPDATE effects SET status = 'applied'
                 WHERE tenant_id = 'tenant-a' AND effect_id = 'effect-drift'",
                [],
            )
            .expect("tamper status projection");
        drop(connection);

        let error = coordinator
            .load_as(&effect_id, &actor)
            .await
            .expect_err("projection drift must fail closed");
        assert!(
            error
                .to_string()
                .contains("projection differs from aggregate")
        );
        drop(coordinator);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_rejects_revision_and_tenant_projection_drift() {
        let path = temporary_path("authority-drift");
        let coordinator = SqliteEffectCoordinator::open(&path).await.expect("open");
        let tenant_a = authority("tenant-a", "worker");
        let tenant_b = authority("tenant-b", "worker");
        let effect_id = EffectId::from_static("effect-authority-drift");
        coordinator
            .create_as(
                effect_id.clone(),
                request("authority-drift-key"),
                10,
                &tenant_a,
            )
            .await
            .expect("create");
        let connection = Connection::open(&path).expect("open tamper connection");
        connection
            .execute(
                "UPDATE effects SET revision = 2
                 WHERE tenant_id = 'tenant-a' AND effect_id = 'effect-authority-drift'",
                [],
            )
            .expect("tamper revision projection");
        drop(connection);
        let revision_error = coordinator
            .load_as(&effect_id, &tenant_a)
            .await
            .expect_err("revision drift must fail closed");
        assert!(
            revision_error
                .to_string()
                .contains("projection differs from aggregate")
        );

        let connection = Connection::open(&path).expect("reopen tamper connection");
        connection
            .execute(
                "UPDATE effects SET revision = 1, tenant_id = 'tenant-b'
                 WHERE tenant_id = 'tenant-a' AND effect_id = 'effect-authority-drift'",
                [],
            )
            .expect("tamper tenant projection");
        drop(connection);
        assert!(
            coordinator
                .load_as(&effect_id, &tenant_a)
                .await
                .expect("original tenant lookup")
                .is_none()
        );
        let tenant_error = coordinator
            .load_as(&effect_id, &tenant_b)
            .await
            .expect_err("tenant drift must fail closed");
        assert!(
            tenant_error
                .to_string()
                .contains("projection differs from aggregate")
        );
        drop(coordinator);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_partial_store_fails_closed() {
        let path = temporary_path("partial");
        let connection = Connection::open(&path).expect("open");
        connection
            .execute_batch(
                "CREATE TABLE effect_store_meta (
                    singleton INTEGER PRIMARY KEY,
                    schema_version INTEGER NOT NULL
                );",
            )
            .expect("partial");
        drop(connection);
        let error = match SqliteEffectCoordinator::open(&path).await {
            Ok(_) => panic!("partial store unexpectedly opened"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("partial"));
        let _ = std::fs::remove_file(path);
    }
}
