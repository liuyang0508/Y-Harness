# ADR 0093: Atomic terminal-boundary Thread fork and lineage

- Status: accepted
- Date: 2026-07-28

## Context

Pi can extract a root-to-leaf session path into a new JSONL file while
preserving entry identities and recording the parent session. Codex exposes a
typed `thread/fork` operation with an optional Turn boundary and a new Thread
identity. Those are useful product semantics, but Y-Harness State is an
append-only recovery authority rather than a session-file convenience layer.

Implementing fork as repeated ordinary appends would allow a crash or unique
identity failure to leave a visible half-child. Replaying historical Tool or
approval records as new effects would also be incorrect. A lineage pointer
without locally materialized history would make child recovery depend on the
continued availability and retention policy of every ancestor.

## Decision

- Add schema-9 `ThreadForked { lineage }` immediately after the child's
  `ThreadCreated` event. Lineage contains the direct parent Thread, exact
  parent global sequence and stream-version boundary, and SHA-256 of the
  ordered parent event prefix.
- Accept only an empty settled parent or a boundary ending in a terminal Turn.
  An omitted boundary means the complete parent prefix observed by the Engine
  and is rejected when that observed prefix ends in a running Turn.
- Require the caller to choose the child `ThreadId`. It is the durable retry
  identity. A retry returns an existing child only when direct lineage and
  inherited Turn evidence match.
- Materialize the complete child in one Event Store operation. The Memory
  store holds one lock; SQLite uses one immediate transaction. Any failure
  leaves no child stream, event rows, recovery accounting, name projection, or
  snapshot.
- Preserve historical Turn, Item, Tool-call batch, Approval, Steering, and
  provider-correlation identities. They denote the same already-observed
  evidence and are never re-executed by fork.
- Assign new journal `EventId` values to child rows because Event identities
  are globally unique. Preserve each copied event's original supported schema
  coordinate and recording timestamp.
- Do not copy Thread names, Checkpoints, `ThreadCreated`, or ancestor
  `ThreadForked` events. Names are child-local operator metadata; Checkpoints
  contain source-journal recovery positions; direct lineage is sufficient and
  ancestors can be traversed through their own Threads.
- Keep fork in State/Runtime and expose it through Protocol 15. The optional
  TUI remains a client and invokes `/fork [terminal-turn-id]` without opening
  SQLite or owning branch state.

## Consequences

The parent and child recover independently and can accept new Turns without
mutating each other. Child recovery has the same finite event and byte limits
as any Thread and does not require ancestor reads. Large forks duplicate
bounded history by design; archival/blob sharing remains a separate future
contract rather than hidden cross-stream coupling.

Forking a currently running latest Turn does not manufacture an interruption
event in the source. A caller must select an earlier terminal Turn or settle
the active Turn first. This is stricter than Codex's active-latest behavior and
preserves Y-Harness's rule that only the runtime owner may settle active work.

This decision does not implement in-place session trees, branch
summarization, export/import, archival, or effect rollback.

## Rejected alternatives

- Repeated `append` calls: failure is not atomic and exposes half-children.
- Lineage-only child with inherited reads: recovery and retention become
  transitively dependent on ancestors.
- Copying Checkpoints: their source global sequence and Thread identity are
  invalid in the child journal.
- Generating new Turn/Item/correlation identities: this falsely represents
  historical evidence as newly executed work and breaks provenance.
- Forking a running latest Turn by interrupting the parent: fork authority
  would cross the runtime ownership and fencing boundary.

## Evidence

- Memory and SQLite share the same bounded fork validation.
- Tests cover terminal boundaries, active-latest rejection, earlier-boundary
  fork during newer parent work, retry identity, independent continuation,
  snapshots, SQLite reopen, and injected mid-transaction uniqueness failure.
- Protocol wire tests cover the schema-9 lineage shape, method tag,
  permission, conditional capability, and idempotent child result.
- The release State benchmark measures complete SQLite fork latency in
  addition to append, projection, and snapshot paths.

## Sources

- [Pi root-to-leaf session materialization at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/core/session-manager.ts)
- [Codex typed Thread fork boundary at `61a4488`](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/app-server/README.md)
