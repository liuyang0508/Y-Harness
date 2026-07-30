# ADR 0133: Durable fail-closed external Effect Ledger

- Status: accepted
- Date: 2026-07-30

## Context

Turn `ToolResult` proves how one in-process Tool call settled before the Turn
continued. Task leases prove who owns executable work. Workflow retry waits
prove when a caller-selected retry may become eligible. None of those records
answers the crash-boundary question for a cross-time external side effect:
the request may have reached the external system even though the worker died
before receiving or persisting its response.

Automatically returning an expired execution lease to a work queue would
therefore permit duplicate messages, payments, mutations, or notifications.
Storing an `effect_completed` boolean in Workflow would conflate executable
work, external-system evidence, worker ownership, and reconciliation.

## Decision

- Add an independent tenant-fenced `Effect` aggregate. It durably captures the
  immutable capability/operation coordinate, target-system idempotency key,
  bounded JSON request, request SHA-256, trusted creation time, lifecycle, and
  immutable actor-attributed transitions before execution begins.
- Make `(tenant, capability, operation, idempotency_key)` unique. Exact
  actor/content replay returns the canonical existing Effect even when a
  caller repeats creation with a different proposed Effect identity. A key
  collision with different content fails closed.
- Model `pending`, `claimed`, `unknown`, `applied`, `rejected`, and `cancelled`
  states. A claim owns one positive attempt under a never-reused finite lease.
- Permit an active owner to renew, report applied, report authoritatively not
  applied, or report an unknown outcome. Exact lease, owner, and unexpired
  server-clock boundary are revalidated for worker settlements.
- An expired lease becomes `unknown`, never `pending`. Reconciliation must name
  the exact uncertain attempt and lease and may confirm applied or
  authoritatively not applied. Only the latter may explicitly schedule the
  next attempt.
- Store content-free external receipts: source, external identity, observation
  time, and a digest of normalized response/proof. Credentials and arbitrary
  response bodies are not receipts.
- Bind creation and every command to the trusted actor, tenant boundary, and
  complete typed content with SHA-256. Exact replay is idempotent before
  revision comparison; authority/content collision fails closed.
- Reconstruct the complete current projection, command digests, lease
  ownership, attempt order, and receipt constraints from transition history
  during deserialization. Cached SQLite columns are validated against the
  aggregate.
- Provide Memory and SQLite Coordinators with revision CAS, exact tenant
  partitioning, bounded paging, expired-lease scans, WAL, `synchronous=FULL`,
  and fail-closed partial/unknown layout validation.
- Advance Protocol 28 to 29. The optional Effect surface uses separate create,
  read/list, worker claim/renew/settle, lease-management, reconciliation, and
  cancellation permissions. The reference service stores it in `effects.db`.
- Keep list pages content-light: they expose the request digest but omit
  request input and target-system idempotency identity. One exact `effect.get`
  authorization is required before a worker can materialize the request.
- Advance embedded Temporal Driver API 1 to 2 and optionally compose the Effect
  Engine. A host-driven tick converts expired exact leases to `unknown` using
  the existing command/CAS boundary. Core still starts no timer task.

## Bounds and recovery

- One Effect retains at most 4,096 transitions and 16 MiB of encoded state.
- Input and actor-bound commands are limited to 128 KiB.
- Identifiers and idempotency keys are limited to 256 non-control bytes.
- Leases range from 1 second through 7 days; attempts are positive and bounded.
- Coordinator scans visit 1–256 authoritative Effects per source tick.
- Protocol transition pages contain 1–64 records and at most 4 MiB.
- SQLite schema 1 is the first Effect store. Unknown or partial layouts are not
  initialized or migrated in place.

## Authority boundaries and non-claims

The Effect Ledger is a source of execution intent and settlement evidence, not
permission to perform an action. Policy/Approval must authorize creation and
worker access at the embedding boundary. The registered worker or Connector,
not Core, interprets the operation and request.

The ledger does not execute Tools, replay unknown work, infer idempotency from
Provider error strings, verify arbitrary receipt truth, encrypt request bodies,
route Channels, or compensate applied effects. A later Effect Executor/Outbox
host may claim pending intents and call registered capabilities, but it must
use this lease and settlement contract. Artifact authorization, retention,
legal hold, distributed scheduling, and multi-node consensus remain separate
governance work.

## Rejected alternatives

- Requeue on lease expiration: worker death does not prove the side effect was
  absent.
- Reuse Task status: Task completion and external effect settlement have
  different retry and evidence semantics.
- Put effect fields in Workflow: Workflow coordinates process state and must
  not become a second Tool executor or external-system ledger.
- Treat an idempotency key as sufficient proof: external systems vary, and a
  key without a durable request binding can be reused with different content.
- Store credentials or full Provider responses as receipts: this widens
  retention and secret authority without improving lifecycle correctness.
