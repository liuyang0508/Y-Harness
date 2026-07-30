# ADR 0131: Read-only service-store preflight

- Status: accepted
- Date: 2026-07-30

## Context

`yh doctor` previously validated configuration, provider construction, and the
data-directory boundary without opening existing authoritative databases. An
operator could therefore receive `status: ok` and only discover a legacy,
partial, or unknown SQLite schema when `yh serve` attempted its authoritative
open. Provider and MCP construction could also happen before that deterministic
storage incompatibility was reported.

Automatically migrating during diagnosis or service startup is unsafe. State,
Approval, and Task migration require every writer to be stopped, a distinct
operator-selected no-clobber backup path, and an explicit rollback decision.

## Decision

- The five concrete SQLite adapters expose additive asynchronous
  `validate_existing` functions. They open an already existing database with
  read-only SQLite authority and `query_only`, apply the production busy
  timeout, and reuse the same complete schema, metadata, projection, and
  bounded-record validation as their authoritative open.
- Validation does not create a file, bootstrap a schema, migrate data, publish
  a backup, or acquire provider/process/network authority. A missing path is an
  adapter error; an existing empty SQLite database is valid first-bootstrap
  input.
- The reference host first resolves the contained data directory. For each
  exact State, Approval, Task, Workflow, and Human Handoff path, absence means
  `will be created`; an existing regular non-symlink file must pass the
  adapter's read-only validation. Other filesystem object types fail closed.
- `yh doctor` performs this store preflight before constructing Models, Tools,
  MCP clients, Memory, Verifiers, or Evaluation Graders and reports each store
  as `ready` or `will be created`.
- `yh serve` performs the same preflight before capability construction. Its
  later authoritative store open repeats validation and remains the actual
  concurrency and startup boundary; the preflight is not a TOCTOU guarantee.
- Legacy State, Approval, or Task schemas return their existing actionable
  backup-first migration diagnostic. Partial, mixed, malformed, or unknown
  stores fail closed. Workflow and Human Handoff schema 1 have no inferred
  legacy migration path.
- Service configuration remains schema 1, Protocol remains v28, and all five
  durable schema coordinates remain unchanged. This is an additive Rust API
  and host-ordering correction.

## Operational boundary

`doctor` and `serve` never auto-migrate. The operator must stop every writer,
choose a fresh rollback path, run the corresponding `state-migrate`,
`approval-migrate`, or `task-migrate` command, verify success, and rerun
`doctor`. An existing database can change between preflight and authoritative
open; the second validation must reject that race rather than trusting the
earlier report.

## Rejected alternatives

- Validate only in `serve`: preserves the misleading successful diagnostic and
  may start unrelated external capabilities first.
- Auto-migrate on startup: removes the explicit backup and rollback authority
  and cannot prove that all other writers are stopped.
- Duplicate schema parsing in the CLI: risks drift from the concrete adapters'
  production invariants.
- Treat any existing path as ready: follows symlinks and delays corruption or
  partial-store discovery until mutation-capable startup.
