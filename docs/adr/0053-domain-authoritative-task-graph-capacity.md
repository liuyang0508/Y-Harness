# ADR 0053: Domain-authoritative Task Graph capacity

- Status: Accepted
- Date: 2026-07-25

## Context

The Task Coordinator rejected a graph above 64 MiB while encoding a create or
compare-and-swap operation. `TaskGraph` itself enforced per-field and count
limits but not their aggregate. A sequence of otherwise valid messages or
large Task completions could therefore succeed in memory and only later become
impossible to persist.

Re-serializing the complete graph after every domain mutation would close the
correctness gap but turn recurring messages, heartbeats, and settlements into
an avoidable whole-graph operation.

## Decision

- Make the 64 MiB materialization boundary a Task Graph domain invariant shared
  with the Coordinator.
- Track a conservative in-memory charge: fixed graph overhead, serialized Task
  entry fragments, serialized messages, and a 1 KiB terminal reserve for each
  pending or running Task.
- Calculate the initial charge at graph construction. Rebuild it during custom
  deserialization and compare it during integrity validation.
- Do not serialize the charge. The Task Coordinator v1 JSON wire shape remains
  unchanged.
- Precompute the complete charge delta for a batch of Task mutations, reject
  overflow before changing any record, then publish the whole mutation batch.
- Charge a message before advancing its sequence or appending it.
- Keep the Coordinator's final exact full-JSON check as defense in depth.
- Expose current and remaining charge as read-only graph metrics.

## Consequences

Every successful public Task Graph mutation remains representable by the
built-in durable Coordinator. Capacity rejection is failure-atomic, including
transitive blocked-state propagation. Incremental mutations serialize only
their affected record fragments; full-graph encoding remains confined to
construction/deserialization validation and actual persistence.

The charge is deliberately conservative. Tuple-shaped Task fragments dominate
map-entry syntax, fixed overhead dominates graph framing, and the active-Task
reserve covers the worst escaped 256-byte dependency identity in a generated
blocked reason. Some graphs may therefore reject before their exact JSON reaches
64 MiB; this buys terminal liveness and simple auditability.

Changing the reserve, field limits, or serialized Task shapes requires updating
the dominance and exact-rebuild regression tests.

## Rejected alternatives

- Enforce capacity only in the Coordinator: domain success could produce an
  unpersistable aggregate.
- Serialize the entire graph on every mutation: correct but unnecessarily
  expensive for hot orchestration paths.
- Persist the charge as authority: a stale or tampered metadata field could
  bypass the real content calculation and would change the v1 wire schema.
