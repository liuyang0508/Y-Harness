# ADR 0003: Append-only state journal

- Status: Accepted
- Date: 2026-07-25

## Decision

The State Engine uses an append-only event journal as the single source of
truth for Thread, Turn, Item, and Checkpoint state. SQLite is the first durable
store; an in-memory store implements the same contract for tests.

Each event has a caller-generated unique ID. Re-appending the same ID and
content returns the existing record. Reusing an ID with different content is
an error. State is reconstructed by a deterministic projector that rejects
invalid histories.

Every append also carries the stream version observed by State Engine. The
Event Store compares that version and appends inside the same atomic critical
section or SQLite immediate transaction. A competing writer receives a typed
`StateConflict`; it cannot append a transition validated against stale state.

SQLite uses WAL mode, `synchronous=FULL`, a busy timeout, and an immediate
transaction for append settlement. JSONL traces are exports from stored events,
not a second write target.

SQLite transactionally maintains a compact `streams` head table so version
comparison does not count the full event stream. State Engine caches only
lightweight validated stream heads (version, last sequence, known Turns, and
running Turn), never the authoritative event bodies. A stale cache can only
produce a typed conflict; it cannot bypass the store comparison.

On recovery, a persisted running turn becomes `interrupted`. The runtime does
not automatically retry or replay its tool calls because their idempotency is
unknown.

## Rationale

Writing state and observability traces independently creates an ambiguous
failure mode: either side can commit without the other. A single ordered
journal makes recovery, replay, trace export, and later evaluation consume the
same evidence.

Idempotent event IDs make retry after an uncertain acknowledgement safe.
Projection validation keeps corrupted or semantically impossible histories
from silently becoming live state.

## Consequences

- Event schema evolution and migration must remain explicit.
- Large artifacts belong in a blob/artifact store and are referenced by state
  events rather than embedded indefinitely.
- External side effects still require their own idempotency keys or recovery
  policy; journal idempotency alone cannot make an external tool safe.
- Optimistic stream concurrency prevents conflicting multi-process
  transitions. Distributed ownership, leases, and automatic conflict retry are
  still deferred until orchestration requires them.
