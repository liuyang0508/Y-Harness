# ADR 0130: Optional reference-service Temporal lifecycle

- Status: accepted
- Date: 2026-07-30

## Context

ADR 0129 deliberately made Temporal Driver API 1 a bounded embedded primitive:
Core owns no clock, polling task, or scheduler database. A long-running host
still needs an operational way to advance durable Workflow waits and expired
Human Handoff claims without recreating lifecycle policy in every deployment.

Starting that loop unconditionally in `yh serve` would surprise existing
embedders and operators. Moving it into Core would give a library hidden
process lifetime. Persisting its cursor would incorrectly promote an
acceleration hint into scheduling authority.

## Decision

- Add an optional `temporal` object to strict reference-service configuration
  schema 1. Omission is the backward-compatible disabled state. Presence
  enables the lifecycle with default `poll_interval_ms: 1000` and
  `scan_limit: 64`.
- Bound the polling interval to 100–86,400,000 milliseconds and the per-source
  scan to the public Temporal Driver bound of 1–256 identities. `yh doctor`
  reports the exact enabled cadence and scan bound.
- Compose the same SQLite Workflow and Human Handoff Engines already exposed
  by the service. Supply the exact fixed service `AuthorityContext` and host
  Unix time to every tick; request data cannot choose either value.
- Run an immediate first tick, then a Tokio interval with missed-tick behavior
  set to `Skip`. A slow tick therefore never creates a catch-up burst or
  concurrent tick inside one service process.
- Keep the identity cursor only in process memory. A successful scan advances
  it even when an individual fenced command reports `failed`; a scan failure
  retains the prior cursor. Restart or cursor loss begins a new bounded sweep.
- Treat duplicate and fenced outcomes as expected concurrency settlement.
  A scan error or content-free failed attempt moves the host into a degraded
  state. The reference host writes only health transitions to stderr, bounds
  diagnostics to 1,024 single-line characters, and reports recovery after a
  clean tick. Protocol stdout remains pure JSONL.
- On stdio completion, stop Temporal admission first and wait up to 30 seconds
  for an in-flight tick. Then drain Protocol Operations and Runtime background
  work, and finally stop MCP clients. A Temporal timeout aborts its
  process-local task and makes service shutdown fail; durable aggregate CAS and
  deterministic command identity remain the recovery authority.
- Keep Protocol v28, service configuration schema 1, Workflow/Handoff schema 1,
  and Temporal Driver API 1. The new field is an additive host policy, not a
  wire capability or durable semantic coordinate.

## Concurrency and availability

Multiple `yh serve` processes may poll the same fixed tenant. This can increase
read and CAS load, but exact command replay and stale revision fencing prevent
two successful transitions for one wait or claim. This is not leader election,
distributed scheduling, or a multi-node availability guarantee.

The interval bounds control one process, not aggregate deployment load.
Operators running several replicas must size the cadence and scan limit for
their authoritative SQLite coordination topology.

## Non-claims

This lifecycle does not:

- execute a retried Task, Tool, compensation, or business Workflow step;
- route a Human Handoff, notify a person, or wake a conversation;
- provide a durable outbox, Webhook delivery, or cross-database transaction;
- make the disposable cursor authoritative or guarantee real-time latency;
- partition one service process across several request-selected tenants;
- conceal Coordinator failure as successful advancement.

## Rejected alternatives

- Start polling inside Core: violates the embeddable Engine lifecycle boundary.
- Enable polling by default: changes existing service behavior without an
  operator decision.
- Replay every missed interval: creates an avoidable load burst while durable
  due state already guarantees later discovery.
- Persist the scan cursor: creates a second recovery contract with no semantic
  value and risks skipping work if corrupted.
- Add a Protocol `tick` command: lets remote callers influence time and cadence
  without adding useful domain authority.
