# ADR 0118: Bind durable Approval records to trusted tenant authority

Status: accepted

## Context

ADR 0117 made Thread and Operation tenant ownership authoritative, but the
Approval Inbox still stored only actor attribution. Runtime therefore rejected
tenant-scoped `PolicyDecision::Ask`, and Protocol omitted all Approval
capabilities for tenant-scoped authorities. Inferring Approval ownership from
an actor, Thread identifier, database path, or current deployment would make
historical migration and independent approver roles ambiguous.

## Decision

- Approval Inbox schema 3 adds immutable optional tenant ownership to each
  `ApprovalRecord`.
- Runtime passes the same trusted `AuthorityContext` used by Thread, Policy,
  Memory, and Tool execution to Approval submission, polling, abandonment, and
  restart continuation.
- `ApprovalInbox` keeps its unscoped embedding methods and adds tenant-aware
  `_as` methods. Unscoped methods can see only records whose tenant is absent;
  tenant-aware methods require exact case-sensitive equality.
- The SQLite `approval_records.tenant_id` column is a lookup projection and is
  validated against the serialized record body on every read. The body remains
  authoritative.
- Protocol Approval list, get, and settlement use only the
  transport-resolved authority. Commands contain no tenant selector. A
  different tenant observes absence, while same-tenant settlement still
  requires a distinct authenticated actor.
- Schema-2 records migrate as explicitly unscoped. Their tenant is not guessed
  from the owning Thread or requester. Schema-1 migration retains its existing
  behavior: unattributed pending requests are orphaned before becoming
  schema-3 records.
- Protocol advances to v21 because the serialized `ApprovalRecord` gains an
  optional tenant field and tenant-scoped capability discovery changes.
- Task Graphs remain unpartitioned. Tenant-scoped Task discovery and access
  continue to fail closed.

## Boundary

The `ProtocolAuthorizer` or trusted embedded host establishes
`AuthorityContext`. An Approval requester, Tool input, protocol command, or
database location is not tenant authority.

`ApprovalInbox` is the persistence boundary. Custom tenant-aware handlers must
override the authority-aware methods and durably preserve tenant ownership;
the default `ApprovalHandler` implementation rejects tenant-scoped use.

This ADR does not add role policy, quorum approval, signed receipts, delegation,
tenant transfer, retention, or Task/Artifact/Secret ownership.

## Consequences

- Tenant-scoped protected Tool execution can wait, restart, settle, and resume
  without losing its tenant boundary.
- Wrong-tenant reads and mutations cannot reveal record contents or ownership.
- Historical schema-2 approvals remain available only through the unscoped
  trusted boundary until an explicit transfer workflow exists.
- Restoring a schema-2 backup after schema-3 writes discards newer approval
  ownership and settlement evidence and is not a rolling downgrade.

## Verification

- `approval::tests::tenant_ownership_fences_memory_and_sqlite_approval_access`
- `approval::tests::sqlite_rejects_approval_tenant_projection_drift`
- `approval::migration::tests::migration_preserves_schema_two_records_as_explicitly_unscoped`
- `approval::migration::tests::schema_two_migration_restarts_after_every_mutating_phase`
- `runtime::tests::tenant_scoped_approval_is_durable_and_executes_only_after_settlement`
- `runtime::tests::tenant_scoped_sqlite_approval_wait_resumes_after_store_reopen`
- `runtime::tests::tenant_recovery_orphans_only_its_durable_approval`
- `protocol::tests::protocol_tenant_fencing_hides_threads_operations_approvals_and_tasks`
- `protocol::tests::protocol_twenty_one_wire_envelopes_state_provenance_and_permissions_are_stable`
