# ADR 0160: Database-neutral Effect persistence protocol

- Status: accepted
- Date: 2026-08-14

## Context

The Effect Ledger originally shipped with Memory and SQLite Coordinators. A
multi-instance host may already own a PostgreSQL pool, migrations, transaction
policy, and operational controls. Reimplementing `Effect` creation, replay,
strict hydration, command application, paging, and due-lease derivation in
that host would duplicate the lifecycle state machine. Adding every database
driver and pool to Core would instead couple Y-Harness to host infrastructure.

## Decision

- Add `EffectPersistenceProtocol` as a database-neutral Rust boundary.
- Prepare creation and command candidates inside Core. A prepared value is not
  proof of persistence; the host returns its snapshot only after an atomic
  insert, exact replay, or revision compare-and-swap succeeds.
- Expose one bounded `EffectStoredRecord` with explicit indexed projections and
  complete aggregate JSON. Its ordinary `Debug` output is redacted.
- Strict restoration checks schema, byte bounds, authority scope, positive
  revision, transition count, capability, operation, idempotency key, creation
  time, status, and the complete aggregate invariants.
- Expose the exact tenant/capability/operation/idempotency coordinate required
  by the host's unique constraint.
- Validate and reconstruct bounded ordered list and due-scan results in Core,
  including cursor order, status projection, and authoritative lease expiry.
- Refactor Memory and SQLite creation/application through the same preparation
  rules so host adapters do not follow a second lifecycle implementation.

## Host responsibilities

The host must still provide one atomic create-or-recover transaction, the exact
unique constraint, revision compare-and-swap, bounded ordered SQL queries,
trusted server time, connection pooling, migrations, backup, and monitoring.
The tenant partition column is non-null: empty text is the only representation
of an unscoped local-process Effect, preventing SQL `NULL` uniqueness gaps.
Identity paging must use bytewise stable ordering rather than a locale-dependent
database collation.
The protocol does not make a sequence of host calls atomic and does not certify
that a prepared value was written. A PostgreSQL adapter must fail closed on an
unexpected row count, conflicting identity, corrupt projection, or lost CAS.

## Compatibility

This is an additive embedded Rust API. It does not change Effect schema 1,
Protocol 29, Effect command semantics, wire framing, or SQLite storage. The
public crate is pre-1.0; incompatible Rust API changes continue to follow the
existing exact-version policy.

## Rejected alternatives

- Copy the Effect state machine into each product repository: this creates
  divergent replay, lease, and corruption behavior.
- Add a PostgreSQL driver and pool directly to Core: hosts already own those
  dependencies and deployment policies.
- Use SQLite as a substitute for a multi-instance production ledger: local
  restart durability is not distributed high availability.
