# ADR 0068: Durable Policy-to-Tool origin provenance

- Status: Accepted
- Date: 2026-07-25

## Context

Y-Harness registers every Tool with a trust-bearing `CapabilityOrigin`, and
Policy evaluates a `ToolAuthorization` containing that origin. Before this
decision, ordinary State retained the requested Tool name and Policy outcome
but discarded the evaluated origin. Only the narrower schema-3
`ApprovalRequested` continuation boundary retained Tool origin.

Consequently, an operator could reconstruct that Policy allowed, denied, or
deferred a named Tool, but not which built-in, trusted-extension, or external
registration Policy actually evaluated. A later registry entry using the same
name could make historical audit ambiguous. Recording origin only in
`ToolResult` would not solve this: denied calls, approval waits, pre-execution
failures, and uncertain effects may have no result.

## Decision

- Add optional `tool_origin` decoding to `ItemKind::PolicyDecision`, but require
  it on every newly written event.
- Advance authoritative State events from schema 3 to schema 4. Schema-4
  `PolicyDecision` Items require a valid Tool origin; schema-1 through schema-3
  events reject the field so old event labels cannot claim new evidence.
- Record the frozen registered Tool origin at the Policy settlement point for
  `allow`, `deny`, and `ask`.
- During approval continuation, require Policy and `ApprovalRequested` origins
  to match when the Policy evidence is schema 4. Continue accepting absent
  Policy origin in immutable schema-3 history so its already fingerprinted
  approval boundary remains resumable.
- Keep `ToolResult` free of duplicate origin. Its call ID joins to the ordered
  Tool call and Policy settlement; Policy is the semantic authority for what
  registration was authorized.
- Advance disposable State snapshots from schema 3 to schema 4 because they
  project the new Item shape.
- Extend the existing offline, backup-first migration to accept schema-1,
  schema-2, and schema-3 sources. Historical event JSON and schema labels stay
  immutable; event and snapshot writer coordinates advance atomically.
- Bind backup reuse to both source and destination event/snapshot coordinates,
  as well as the complete authoritative-event SHA-256 fingerprint.
- Advance the exact client protocol from 8 to 9 because initialization exposes
  the new coordinates and `GetEvents` can return the new durable shape.

## Consequences

Every current Policy outcome answers which trust domain it evaluated, whether
or not Tool execution began or completed. External and trusted-extension Tool
registrations are auditable through embedded State and the same typed service
protocol.

The Rust `ItemKind` construction surface and wire shape change before 1.0.
Protocol-v8 clients fail exact negotiation instead of permissively decoding
schema-4 events. Populated SQLite State stores require an offline migration
before the schema-4 writer opens them. Old snapshots are discarded and rebuilt
from authoritative history.

This origin identifies the operator registration, not a binary digest or
remote executable attestation. Supply-chain verification and deployment
integrity remain separate controls.

## Rejected alternatives

- Put origin only on `ToolResult`: calls with deny, ask, pre-execution failure,
  or unknown effects may never produce a result.
- Infer origin from the current registry: registration names can outlive or be
  reused across deployments, so current state is not historical evidence.
- Add the field without a schema/protocol advance: optional decoding would
  silently change the meaning of schema-3 events and expose an unnegotiated
  wire shape.
- Duplicate origin on `ToolCall`, `PolicyDecision`, and `ToolResult`: the Model
  requests a name but does not select a registration, while the result already
  correlates to the authoritative Policy settlement.
