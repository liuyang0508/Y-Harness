# ADR 0119: Durable Task Graph tenant ownership

- Status: accepted
- Date: 2026-07-29

## Context

Protocol v21 omitted Task capabilities for tenant-scoped authorities because
Task Graph schema 1 had no durable owner. Filtering only in the protocol would
not protect embedded callers, reopened SQLite stores, worker leases, messages,
or compare-and-swap mutations. A global caller-selected `graph_id` would also
let one tenant reserve another tenant's identifier.

Historical schema-1 rows contain no trustworthy tenant fact. A Thread,
principal, database path, or deployment convention is not evidence of
ownership.

## Decision

- Advance the durable Task Graph schema from 1 to 2.
- Bind one immutable optional tenant to the complete Task Graph aggregate.
  `None` is the explicit unscoped partition; it is not a wildcard.
- Make `(tenant, graph_id)` the persistence identity so different tenants may
  use the same caller-selected Graph ID.
- Add authority-aware create, load, and CAS operations to `TaskCoordinator`.
  Existing unscoped methods are compatibility wrappers over trusted
  local-process authority.
- Require exact tenant equality for graph administration, worker claims,
  heartbeat, completion, failure, message access, cancellation, and every CAS.
  Cross-tenant reads return absence; mutations cannot distinguish the target
  from a missing Graph.
- Store tenant ownership both in the SQLite lookup projection and the bounded
  JSON aggregate envelope. Every read validates equality before returning the
  Graph.
- Resolve tenant only from the transport-authenticated `AuthorityContext`.
  Task commands gain no tenant selector.
- Advance the client protocol to v22. Task capabilities are discoverable for
  tenant-scoped authorities when a Coordinator is configured, and Task Graph
  summaries expose the immutable optional tenant.
- Migrate schema-1 SQLite stores offline with a no-clobber,
  source-fingerprinted rollback backup. Every historical Graph becomes
  explicitly unscoped; migration never infers ownership.

## Consequences

- Memory and SQLite coordinators have the same tenant namespace and fencing
  semantics.
- Tenant-scoped workers can use the complete existing lifecycle without
  weakening lease fencing or moving execution into the protocol.
- Old populated stores fail with an explicit `yh task-migrate` instruction.
  Schema-1 and schema-2 writers must never run concurrently.
- Migrated unscoped Graphs are intentionally invisible to tenant-scoped
  authorities. Reassignment or tenant transfer requires a separately governed
  future operation; it is not part of migration.
- Protocol v22 clients must not downgrade Task ownership to v21 behavior.
  State schema 12 and Approval Inbox schema 3 are unchanged.

## Rejected alternatives

- Protocol-only filtering: embedded access and direct Coordinator calls would
  remain unfenced.
- A nullable tenant beside a globally unique `graph_id`: this leaks and
  reserves caller-selected identities across tenants.
- Inferring tenant from Thread, actor, path, or deployment: none is durable
  ownership evidence in schema 1.
- Per-Task tenant fields: the Graph is the transaction, capacity, ordering,
  lease, and message boundary; duplicating ownership per child adds drift
  without adding isolation.
