# ADR 0117: Make Thread tenant ownership durable and authoritative

Status: accepted

## Context

ADR 0116 carried a transport-resolved `AuthorityContext` through Memory,
Policy, and Tool execution, but Threads and Operations were still globally
addressable by opaque ID. An authenticated tenant could therefore not be
offered an isolation claim. Inferring ownership from a request, actor, memory
scope, database file, or current deployment would also make recovery and
migration ambiguous.

## Decision

- State schema 12 adds an optional tenant identity to the authoritative
  `thread_created` event and projected `Thread`.
- The SQLite `streams.tenant_id` value and the in-memory equivalent are
  disposable lookup projections. They are written atomically with creation and
  validated against the first event; the journal remains authoritative.
- Schema-1 through schema-11 Threads migrate explicitly as unscoped. Migration
  never guesses a tenant from an actor, path, configuration, or other resource.
- Thread reads, lists, mutations, Turn execution, recovery, forks, handoff
  preparation, exports, and protocol Operations require exact tenant equality.
  A different tenant observes absence rather than ownership metadata.
- Tenant is the resource partition. Actors within that tenant remain subject
  to Protocol authorization and Policy; this ADR does not make one actor the
  permanent Thread owner.
- Forked Threads inherit the trusted caller tenant. Archive imports create a
  new target owned by the importing tenant; source tenant data is never trusted
  as target authority.
- Portable Thread archives advance to format 2, and Protocol advances to v20,
  because `Thread`, `ThreadSummary`, and `thread_created` now expose the
  optional tenant field. Older peers must fail exact negotiation.
- Approval and Task records remain unpartitioned in this slice. Tenant-scoped
  Protocol sessions do not advertise their capabilities, and direct commands
  fail before resource access until those stores gain durable ownership.

## Boundary

The `ProtocolAuthorizer` or trusted embedded host supplies the authority.
Protocol requests contain no caller-authored tenant selector.

`EventStore` remains a trusted storage adapter below `StateEngine`. A custom
store must preserve atomic events and projections; application code uses the
tenant-aware State/Runtime/Protocol entry points rather than raw store methods.

This is durable Thread and Operation isolation, not complete enterprise
multi-tenancy. Approval, Task, Secret, Artifact, Domain Pack activation,
quotas, retention, and distributed ownership still require their own
authoritative schemas and cross-tenant tests.

## Consequences

- Restart and recovery preserve Thread ownership without ambient deployment
  assumptions.
- Legacy data stays reachable only from the unscoped local boundary until an
  explicit future ownership-transfer workflow exists.
- Cross-tenant IDs cannot be used to read State, mutate a Turn, steer, recover,
  cancel, poll, or forget another tenant's Operation.
- Exact tenant filtering adds one bounded projection lookup to protected State
  access. Performance must be measured before adding a cache.
- Restoring a pre-v12 backup after a schema-12 write would discard
  authoritative ownership evidence and is unsupported.

## Verification

- `state::tests::thread_tenant_ownership_fences_reads_mutations_and_reopen`
- `state::tests::thread_tenant_evidence_requires_schema_twelve`
- `state::migration::tests::migration_advances_metadata_schemas_without_rewriting_history`
- `protocol::tests::protocol_tenant_fencing_hides_threads_operations_and_pending_resources`
- `protocol::tests::protocol_twenty_wire_envelopes_state_provenance_and_permissions_are_stable`
