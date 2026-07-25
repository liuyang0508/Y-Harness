# ADR 0031: Queryable Thread capacity pressure

- Status: Accepted
- Date: 2026-07-25
- Superseded in part by: ADR 0046 (recovery-byte capacity and protocol v3)

## Context

State recovery has a hard one-million-event boundary so loss of a disposable
snapshot cannot cause unbounded replay. Rejecting only the first event beyond
that boundary is fail-safe but gives operators no planning interval for
archival or workload rollover.

The signal must describe the enforced event-count invariant. It must not be
misrepresented as remaining disk space, elapsed lifetime, or a durability SLA.

## Decision

- Publish the exact `STATE_THREAD_EVENT_LIMIT` and reserve the final event slot
  exclusively for `TurnFinished`, so a valid running Turn cannot be stranded
  by ordinary State growth.
- Expose a read-only `StateCapacity` projection with used, total remaining,
  general-purpose remaining, terminal reserve, and level.
- Classify less than 80% as `healthy`, 80% through less than 95% as `warning`,
  95% through the penultimate general slot as `critical`, the final reserved
  slot as `terminal_only`, and the boundary as `exhausted`.
- Derive capacity from the validated authoritative stream version. Missing
  Threads return explicit absence in Core rather than a synthetic zero.
- Expose the projection through Core and Runtime APIs.
- Require four general-purpose event slots before Runtime starts a new Turn:
  start, user input, compiled-conversation evidence, and either a first result
  or failure evidence. Longer Turns can still consume the remaining budget.
- If a normal Runtime Item append fails after Turn start, immediately attempt a
  terminal `failed` transition using the reserved slot. If settlement evidence
  itself cannot be appended, settle with the intended cancelled, timed-out, or
  failed status without inventing that missing Item.
- Add an opt-in `GetThreadCapacity` protocol command with its own
  `thread.capacity` permission. Advertise it during `Initialize` only when the
  authenticated principal is authorized.
- Keep protocol coordinate `1`: the command and result are additive, existing
  shapes are unchanged, and old clients never receive the result unless they
  request it.

## Consequences

Embedded and remote operators can poll one small stable projection and alert
before writes are rejected. The terminal reserve prevents an accepted Turn
from consuming its own settlement slot. Runtime preflight prevents a Turn that
cannot record its minimum viable evidence, while append-failure tests prove
that a later State error still attempts durable terminal settlement. The query
is subject to ordinary concurrent-write staleness immediately after it returns;
callers must not treat general remaining events as a reservation.

This signal does not delete, archive, compact, or move journal events. It does
not estimate event byte size or available storage. Those actions require
separate authorization, storage contracts, and crash-tested migration tooling.

## Rejected alternatives

- Emit warnings only in logs: embedded hosts and typed clients could not
  consume them reliably.
- Add capacity fields to `GetThread`: this would change an existing protocol
  result shape.
- Base levels on snapshot age: snapshots do not change the authoritative event
  boundary.
- Automatically archive at a threshold: destructive lifecycle policy requires
  explicit operator authority.
