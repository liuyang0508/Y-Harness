//! Database-neutral preparation and strict restoration for durable Effects.
//!
//! A host remains responsible for its own atomic SQL transaction, unique
//! constraints, paging query, and compare-and-swap update. This module keeps
//! Effect construction, command application, and stored-state validation in
//! Y-Harness so a PostgreSQL host does not have to copy the lifecycle rules.

use std::fmt;

use super::{
    EFFECT_LEDGER_SCHEMA_VERSION, Effect, EffectApplyOutcome, EffectCommand, EffectCommandResult,
    EffectCreateRequest, EffectPage, EffectPageCursor, EffectSnapshot, EffectStatus,
    MAX_EFFECT_IDENTITY_BYTES, MAX_EFFECT_JSON_BYTES,
    coordinator::{due_page_from_scan, page_from_candidates},
    validate_application_time, validate_identity,
};
use crate::{AuthorityContext, EffectId, HarnessError};

const MAX_EFFECT_CAPABILITY_BYTES: usize = 128;
const MAX_EFFECT_STATUS_BYTES: usize = 9;

/// Raw bounded fields read from or written to a host-owned Effect table.
///
/// The type deliberately has no `Debug` implementation because `effect_json`
/// may contain connector input. Constructing this value does not certify that
/// it was persisted; [`EffectPersistenceProtocol::restore`] performs the
/// authoritative structural validation.
#[derive(Clone, Eq, PartialEq)]
pub struct EffectStoredRecordParts {
    /// Stored schema version.
    pub schema_version: u32,
    /// Stable Effect identity.
    pub effect_id: String,
    /// Non-null storage partition; empty text represents a local-process Effect.
    pub tenant_storage_key: String,
    /// Positive optimistic-concurrency revision.
    pub revision: u64,
    /// Indexed connector capability projection.
    pub capability: String,
    /// Indexed connector operation projection.
    pub operation: String,
    /// Indexed target-system idempotency key.
    pub idempotency_key: String,
    /// Trusted creation time.
    pub created_at_ms: u64,
    /// Indexed lifecycle status projection.
    pub status: String,
    /// Complete validated Effect aggregate JSON.
    pub effect_json: String,
}

/// One byte-bounded Effect record ready for a host-owned durable store.
#[derive(Clone, Eq, PartialEq)]
pub struct EffectStoredRecord {
    parts: EffectStoredRecordParts,
}

impl EffectStoredRecord {
    /// Accepts one raw database row after enforcing all outer byte bounds.
    pub fn try_from_parts(parts: EffectStoredRecordParts) -> Result<Self, HarnessError> {
        validate_record_bounds(&parts)?;
        Ok(Self { parts })
    }

    /// Returns the schema version stored with this record.
    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.parts.schema_version
    }

    /// Returns the stable Effect identity.
    #[must_use]
    pub fn effect_id(&self) -> &str {
        &self.parts.effect_id
    }

    /// Returns the immutable tenant projection.
    #[must_use]
    pub fn tenant_id(&self) -> Option<&str> {
        tenant_from_storage_key(&self.parts.tenant_storage_key)
    }

    /// Returns the non-null tenant partition used by database keys and indexes.
    #[must_use]
    pub fn tenant_storage_key(&self) -> &str {
        &self.parts.tenant_storage_key
    }

    /// Returns the positive revision.
    #[must_use]
    pub fn revision(&self) -> u64 {
        self.parts.revision
    }

    /// Returns the indexed connector capability.
    #[must_use]
    pub fn capability(&self) -> &str {
        &self.parts.capability
    }

    /// Returns the indexed connector operation.
    #[must_use]
    pub fn operation(&self) -> &str {
        &self.parts.operation
    }

    /// Returns the indexed target-system idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.parts.idempotency_key
    }

    /// Returns the trusted creation time.
    #[must_use]
    pub fn created_at_ms(&self) -> u64 {
        self.parts.created_at_ms
    }

    /// Returns the indexed lifecycle status.
    #[must_use]
    pub fn status(&self) -> &str {
        &self.parts.status
    }

    /// Returns the complete Effect aggregate JSON.
    #[must_use]
    pub fn effect_json(&self) -> &str {
        &self.parts.effect_json
    }

    /// Returns the raw fields for adapters that need owned SQL parameters.
    #[must_use]
    pub fn into_parts(self) -> EffectStoredRecordParts {
        self.parts
    }
}

impl fmt::Debug for EffectStoredRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EffectStoredRecord(<redacted>)")
    }
}

/// Unique target-system coordinate used by the host's atomic create query.
#[derive(Clone, Eq, PartialEq)]
pub struct EffectIdempotencyCoordinate {
    tenant_storage_key: String,
    capability: String,
    operation: String,
    idempotency_key: String,
}

impl EffectIdempotencyCoordinate {
    /// Returns the non-null tenant partition used by the unique constraint.
    #[must_use]
    pub fn tenant_storage_key(&self) -> &str {
        &self.tenant_storage_key
    }

    /// Returns the exact connector capability.
    #[must_use]
    pub fn capability(&self) -> &str {
        &self.capability
    }

    /// Returns the exact connector operation.
    #[must_use]
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Returns the exact target-system idempotency key.
    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
}

impl fmt::Debug for EffectIdempotencyCoordinate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EffectIdempotencyCoordinate(<redacted>)")
    }
}

/// Candidate returned before the host atomically inserts or recovers a row.
pub struct EffectPreparedCreate {
    snapshot: EffectSnapshot,
    record: EffectStoredRecord,
    idempotency: EffectIdempotencyCoordinate,
}

impl EffectPreparedCreate {
    /// Returns the exact record that may be inserted.
    #[must_use]
    pub fn record(&self) -> &EffectStoredRecord {
        &self.record
    }

    /// Returns the independent unique coordinate that must share the insert transaction.
    #[must_use]
    pub fn idempotency(&self) -> &EffectIdempotencyCoordinate {
        &self.idempotency
    }

    /// Consumes the candidate after the host confirms the atomic insert.
    #[must_use]
    pub fn into_committed_snapshot(self) -> EffectSnapshot {
        self.snapshot
    }
}

impl fmt::Debug for EffectPreparedCreate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EffectPreparedCreate(<redacted>)")
    }
}

/// Candidate returned before the host commits one revision compare-and-swap.
pub struct EffectPreparedCommand {
    result: EffectCommandResult,
    record: EffectStoredRecord,
}

impl EffectPreparedCommand {
    /// Returns the exact next record, or the current record for a duplicate.
    #[must_use]
    pub fn record(&self) -> &EffectStoredRecord {
        &self.record
    }

    /// Returns whether this command requires a durable row change.
    #[must_use]
    pub fn changes_record(&self) -> bool {
        self.result.outcome == EffectApplyOutcome::Applied
    }

    /// Returns the exact snapshot that must be persisted by the host CAS.
    #[must_use]
    pub fn snapshot(&self) -> &EffectSnapshot {
        &self.result.snapshot
    }

    /// Consumes the candidate after a successful CAS or duplicate recognition.
    #[must_use]
    pub fn into_committed_result(self) -> EffectCommandResult {
        self.result
    }
}

impl fmt::Debug for EffectPreparedCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EffectPreparedCommand(<redacted>)")
    }
}

/// Database-neutral Effect construction, replay, command, and hydration rules.
pub struct EffectPersistenceProtocol;

impl EffectPersistenceProtocol {
    /// Prepares one validated initial Effect without claiming it was persisted.
    pub fn prepare_create(
        effect_id: EffectId,
        request: EffectCreateRequest,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EffectPreparedCreate, HarnessError> {
        validate_access(&effect_id, authority)?;
        let effect = Effect::new(request, applied_at_ms, authority)?;
        let snapshot = EffectSnapshot {
            id: effect_id,
            tenant_id: authority.tenant_id().map(str::to_owned),
            revision: 1,
            effect,
        };
        let record = encode_snapshot(&snapshot)?;
        let idempotency = EffectIdempotencyCoordinate {
            tenant_storage_key: tenant_storage_key(snapshot.tenant_id.as_deref()).to_owned(),
            capability: snapshot.effect.operation().capability.clone(),
            operation: snapshot.effect.operation().operation.clone(),
            idempotency_key: snapshot.effect.idempotency_key().to_owned(),
        };
        Ok(EffectPreparedCreate {
            snapshot,
            record,
            idempotency,
        })
    }

    /// Strictly restores one stored record in the exact authority boundary.
    pub fn restore(
        record: EffectStoredRecord,
        authority: &AuthorityContext,
    ) -> Result<EffectSnapshot, HarnessError> {
        let effect_id = EffectId::from_string(record.parts.effect_id.clone());
        validate_access(&effect_id, authority)?;
        restore_in_scope(record, authority.tenant_id())
    }

    /// Restores one record after an internal Coordinator already checked authority.
    pub(super) fn restore_in_scope(
        record: EffectStoredRecord,
        tenant_id: Option<&str>,
    ) -> Result<EffectSnapshot, HarnessError> {
        restore_in_scope(record, tenant_id)
    }

    /// Checks whether one snapshot belongs to the supplied authority.
    fn validate_snapshot_authority(
        snapshot: &EffectSnapshot,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        validate_access(snapshot.id(), authority)?;
        if snapshot.tenant_id() != authority.tenant_id() {
            return Err(HarnessError::Effect(
                "Effect snapshot tenant does not match authority".to_owned(),
            ));
        }
        Ok(())
    }

    /// Checks whether an existing row is the exact create replay.
    pub fn create_matches(
        snapshot: &EffectSnapshot,
        request: &EffectCreateRequest,
        authority: &AuthorityContext,
    ) -> Result<bool, HarnessError> {
        Self::validate_snapshot_authority(snapshot, authority)?;
        snapshot.effect.create_matches(request, authority)
    }

    /// Applies or recognizes one command without claiming the CAS succeeded.
    pub fn prepare_command(
        snapshot: &EffectSnapshot,
        expected_revision: u64,
        command: EffectCommand,
        applied_at_ms: u64,
        authority: &AuthorityContext,
    ) -> Result<EffectPreparedCommand, HarnessError> {
        Self::validate_snapshot_authority(snapshot, authority)?;
        validate_revision(expected_revision)?;
        if snapshot.effect.recognizes_command(&command, authority)? {
            return Ok(EffectPreparedCommand {
                result: EffectCommandResult {
                    snapshot: snapshot.clone(),
                    outcome: EffectApplyOutcome::Duplicate,
                },
                record: encode_snapshot(snapshot)?,
            });
        }
        if snapshot.revision != expected_revision {
            return Err(HarnessError::EffectConflict {
                effect_id: snapshot.id.clone(),
                expected: expected_revision,
                actual: snapshot.revision,
            });
        }
        let mut effect = snapshot.effect.clone();
        let outcome = effect.apply(command, applied_at_ms, authority)?;
        let revision = snapshot
            .revision
            .checked_add(1)
            .ok_or_else(|| HarnessError::Effect("Effect revision overflow".to_owned()))?;
        let next = EffectSnapshot {
            id: snapshot.id.clone(),
            tenant_id: snapshot.tenant_id.clone(),
            revision,
            effect,
        };
        let record = encode_snapshot(&next)?;
        Ok(EffectPreparedCommand {
            result: EffectCommandResult {
                snapshot: next,
                outcome,
            },
            record,
        })
    }

    /// Encodes a validated snapshot for an atomic host write.
    pub fn encode(snapshot: &EffectSnapshot) -> Result<EffectStoredRecord, HarnessError> {
        encode_snapshot(snapshot)
    }

    /// Validates a list request before the host executes its bounded query.
    pub fn validate_list_request(
        status: Option<&str>,
        after: Option<&EffectPageCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        super::coordinator::validate_list(status, after, limit, authority)
    }

    /// Restores and verifies one bounded, ordered page returned by the host.
    pub fn restore_page(
        records: Vec<EffectStoredRecord>,
        status: Option<&str>,
        after: Option<&EffectPageCursor>,
        limit: usize,
        authority: &AuthorityContext,
    ) -> Result<EffectPage, HarnessError> {
        Self::validate_list_request(status, after, limit, authority)?;
        validate_query_count(records.len(), limit)?;
        let snapshots = restore_ordered(
            records,
            after.map(|cursor| cursor.effect_id.as_str()),
            authority,
        )?;
        if snapshots.iter().any(|snapshot| {
            status.is_some_and(|expected| status_name(snapshot.effect.status()) != expected)
        }) {
            return Err(HarnessError::Effect(
                "stored Effect page does not match its requested status".to_owned(),
            ));
        }
        Ok(page_from_candidates(snapshots, limit))
    }

    /// Validates a due-scan request before the host executes its bounded query.
    pub fn validate_due_scan_request(
        at_ms: u64,
        after_effect_id: Option<&EffectId>,
        scan_limit: usize,
        authority: &AuthorityContext,
    ) -> Result<(), HarnessError> {
        super::coordinator::validate_due_scan(at_ms, after_effect_id, scan_limit, authority)
    }

    /// Restores one bounded identity sweep and derives due leases itself.
    pub fn restore_due_scan(
        records: Vec<EffectStoredRecord>,
        at_ms: u64,
        after_effect_id: Option<&EffectId>,
        scan_limit: usize,
        authority: &AuthorityContext,
    ) -> Result<super::EffectDueScanPage, HarnessError> {
        Self::validate_due_scan_request(at_ms, after_effect_id, scan_limit, authority)?;
        validate_query_count(records.len(), scan_limit)?;
        let mut snapshots =
            restore_ordered(records, after_effect_id.map(EffectId::as_str), authority)?;
        Ok(due_page_from_scan(&mut snapshots, scan_limit, at_ms))
    }
}

fn restore_in_scope(
    record: EffectStoredRecord,
    tenant_id: Option<&str>,
) -> Result<EffectSnapshot, HarnessError> {
    let effect_id = EffectId::from_string(record.parts.effect_id.clone());
    let stored_tenant = tenant_from_storage_key(&record.parts.tenant_storage_key);
    if stored_tenant != tenant_id {
        return Err(HarnessError::Effect(
            "stored Effect tenant does not match expected scope".to_owned(),
        ));
    }
    if record.parts.schema_version != EFFECT_LEDGER_SCHEMA_VERSION {
        return Err(HarnessError::Effect(format!(
            "Effect {effect_id} uses unsupported schema {}",
            record.parts.schema_version
        )));
    }
    validate_revision(record.parts.revision)?;
    let effect: Effect = serde_json::from_str(&record.parts.effect_json)
        .map_err(|error| HarnessError::Effect(format!("decode Effect: {error}")))?;
    effect.validate()?;
    let transition_count = u64::try_from(effect.transition_count())
        .map_err(|_| HarnessError::Effect("Effect transition count overflow".to_owned()))?;
    if record.parts.revision != transition_count
        || effect.tenant_id() != stored_tenant
        || effect.operation().capability != record.parts.capability
        || effect.operation().operation != record.parts.operation
        || effect.idempotency_key() != record.parts.idempotency_key
        || effect.created_at_ms() != record.parts.created_at_ms
        || status_name(effect.status()) != record.parts.status
    {
        return Err(HarnessError::Effect(
            "stored Effect projection differs from aggregate".to_owned(),
        ));
    }
    Ok(EffectSnapshot {
        id: effect_id,
        tenant_id: stored_tenant.map(str::to_owned),
        revision: record.parts.revision,
        effect,
    })
}

fn encode_snapshot(snapshot: &EffectSnapshot) -> Result<EffectStoredRecord, HarnessError> {
    snapshot.effect.validate()?;
    validate_revision(snapshot.revision)?;
    let transition_count = u64::try_from(snapshot.effect.transition_count())
        .map_err(|_| HarnessError::Effect("Effect transition count overflow".to_owned()))?;
    if transition_count != snapshot.revision
        || snapshot.effect.tenant_id() != snapshot.tenant_id.as_deref()
    {
        return Err(HarnessError::Effect(
            "Effect snapshot projection differs from aggregate".to_owned(),
        ));
    }
    let effect_json = serde_json::to_string(&snapshot.effect)
        .map_err(|_| HarnessError::Effect("cannot encode Effect".to_owned()))?;
    EffectStoredRecord::try_from_parts(EffectStoredRecordParts {
        schema_version: EFFECT_LEDGER_SCHEMA_VERSION,
        effect_id: snapshot.id.as_str().to_owned(),
        tenant_storage_key: tenant_storage_key(snapshot.tenant_id.as_deref()).to_owned(),
        revision: snapshot.revision,
        capability: snapshot.effect.operation().capability.clone(),
        operation: snapshot.effect.operation().operation.clone(),
        idempotency_key: snapshot.effect.idempotency_key().to_owned(),
        created_at_ms: snapshot.effect.created_at_ms(),
        status: status_name(snapshot.effect.status()).to_owned(),
        effect_json,
    })
}

fn restore_ordered(
    records: Vec<EffectStoredRecord>,
    after: Option<&str>,
    authority: &AuthorityContext,
) -> Result<Vec<EffectSnapshot>, HarnessError> {
    let mut previous = after.map(str::to_owned);
    let mut snapshots = Vec::with_capacity(records.len());
    for record in records {
        if previous
            .as_deref()
            .is_some_and(|previous| record.effect_id() <= previous)
        {
            return Err(HarnessError::Effect(
                "stored Effect page is not in strict identity order".to_owned(),
            ));
        }
        let snapshot = EffectPersistenceProtocol::restore(record, authority)?;
        previous = Some(snapshot.id().as_str().to_owned());
        snapshots.push(snapshot);
    }
    Ok(snapshots)
}

fn validate_record_bounds(parts: &EffectStoredRecordParts) -> Result<(), HarnessError> {
    validate_identity("stored Effect", &parts.effect_id)?;
    if let Some(tenant_id) = tenant_from_storage_key(&parts.tenant_storage_key) {
        AuthorityContext::validate_tenant(tenant_id)
            .map_err(|_error| HarnessError::Effect("stored Effect tenant is invalid".to_owned()))?;
    }
    validate_revision(parts.revision)?;
    validate_text_bound(
        "stored Effect capability",
        &parts.capability,
        MAX_EFFECT_CAPABILITY_BYTES,
    )?;
    validate_text_bound(
        "stored Effect operation",
        &parts.operation,
        MAX_EFFECT_CAPABILITY_BYTES,
    )?;
    validate_text_bound(
        "stored Effect idempotency key",
        &parts.idempotency_key,
        MAX_EFFECT_IDENTITY_BYTES,
    )?;
    validate_application_time(parts.created_at_ms)?;
    validate_text_bound(
        "stored Effect status",
        &parts.status,
        MAX_EFFECT_STATUS_BYTES,
    )?;
    if parts.effect_json.len() > MAX_EFFECT_JSON_BYTES {
        return Err(HarnessError::Effect(format!(
            "stored Effect exceeds {MAX_EFFECT_JSON_BYTES} encoded bytes"
        )));
    }
    Ok(())
}

fn validate_text_bound(kind: &str, value: &str, maximum: usize) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > maximum {
        return Err(HarnessError::Effect(format!(
            "{kind} must be 1-{maximum} bytes"
        )));
    }
    Ok(())
}

fn validate_revision(revision: u64) -> Result<(), HarnessError> {
    if revision == 0 {
        Err(HarnessError::Effect(
            "Effect revision must be positive".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn validate_access(effect_id: &EffectId, authority: &AuthorityContext) -> Result<(), HarnessError> {
    authority
        .validate_current("Effect persistence authority")
        .map_err(|error| HarnessError::Effect(error.to_string()))?;
    validate_identity("Effect", effect_id.as_str())
}

fn validate_query_count(actual: usize, limit: usize) -> Result<(), HarnessError> {
    let maximum = limit
        .checked_add(1)
        .ok_or_else(|| HarnessError::Effect("Effect page limit overflow".to_owned()))?;
    if actual > maximum {
        Err(HarnessError::Effect(
            "Effect store returned more rows than requested".to_owned(),
        ))
    } else {
        Ok(())
    }
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

fn tenant_from_storage_key(stored: &str) -> Option<&str> {
    (!stored.is_empty()).then_some(stored)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ActorIdentity, EffectCommandId, EffectCommandKind, EffectLeaseId, EffectOperation,
    };

    fn authority() -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "portable-store-test".to_owned(),
                subject: "worker-a".to_owned(),
            },
            Some("tenant-a".to_owned()),
        )
        .expect("authority")
    }

    fn request() -> EffectCreateRequest {
        EffectCreateRequest {
            command_id: EffectCommandId::from_static("create-a"),
            operation: EffectOperation {
                capability: "channel.wechat".to_owned(),
                operation: "send".to_owned(),
            },
            idempotency_key: "send-a".to_owned(),
            input: serde_json::json!({"protected_ref":"message-a"}),
            not_before_ms: 10,
        }
    }

    #[test]
    fn portable_protocol_round_trips_commands_and_rejects_projection_tampering() {
        let authority = authority();
        let request = request();
        let prepared = EffectPersistenceProtocol::prepare_create(
            EffectId::from_static("effect-a"),
            request.clone(),
            10,
            &authority,
        )
        .expect("prepare create");
        let record = prepared.record().clone();
        assert_eq!(prepared.idempotency().idempotency_key(), "send-a");
        let local = EffectPersistenceProtocol::prepare_create(
            EffectId::from_static("effect-local"),
            request.clone(),
            10,
            &AuthorityContext::local_process(),
        )
        .expect("prepare local create");
        assert_eq!(local.record().tenant_storage_key(), "");
        assert_eq!(local.idempotency().tenant_storage_key(), "");
        let restored =
            EffectPersistenceProtocol::restore(record.clone(), &authority).expect("restore create");
        assert!(
            EffectPersistenceProtocol::create_matches(&restored, &request, &authority)
                .expect("match create")
        );
        let page = EffectPersistenceProtocol::restore_page(
            vec![record.clone()],
            Some("pending"),
            None,
            1,
            &authority,
        )
        .expect("restore page");
        assert_eq!(page.effects, vec![restored.clone()]);

        let applied = EffectPersistenceProtocol::prepare_command(
            &restored,
            1,
            EffectCommand {
                id: EffectCommandId::from_static("claim-a"),
                kind: EffectCommandKind::Claim {
                    lease_id: EffectLeaseId::from_static("lease-a"),
                    lease_duration_ms: 1_000,
                },
            },
            10,
            &authority,
        )
        .expect("prepare command");
        assert!(applied.changes_record());
        let due = EffectPersistenceProtocol::restore_due_scan(
            vec![applied.record().clone()],
            1_010,
            None,
            1,
            &authority,
        )
        .expect("restore due scan");
        assert_eq!(due.due.len(), 1);
        let claimed = EffectPersistenceProtocol::restore(applied.record().clone(), &authority)
            .expect("restore command");
        assert_eq!(claimed.revision(), 2);
        let other_authority = AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "portable-store-test".to_owned(),
                subject: "worker-b".to_owned(),
            },
            Some("tenant-b".to_owned()),
        )
        .expect("other authority");
        assert!(
            EffectPersistenceProtocol::prepare_command(
                &claimed,
                2,
                EffectCommand {
                    id: EffectCommandId::from_static("foreign-command"),
                    kind: EffectCommandKind::Cancel {
                        reason_code: "foreign".to_owned(),
                    },
                },
                11,
                &other_authority,
            )
            .is_err()
        );

        let mut tampered = record.into_parts();
        tampered.status = "applied".to_owned();
        let tampered = EffectStoredRecord::try_from_parts(tampered).expect("bounded row");
        assert!(EffectPersistenceProtocol::restore(tampered, &authority).is_err());
        assert_eq!(
            format!("{:?}", applied),
            "EffectPreparedCommand(<redacted>)"
        );
    }
}
