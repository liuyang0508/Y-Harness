# ADR 0094: Lineage-aware bounded Thread navigation

- Status: accepted
- Date: 2026-07-28

## Context

Pi exposes an in-file entry tree: every session entry points at a parent entry,
and `getTree()` defensively assembles roots and timestamp-ordered children.
Y-Harness deliberately uses a different State model. A fork materializes an
independently recoverable child Thread and records one immutable direct parent
boundary; it does not turn one Thread journal into a mutable-leaf DAG.

Protocol 15 returned lineage on a complete Thread but omitted it from bounded
recent-Thread summaries. A client therefore had to load every visible Thread's
full bounded history to show parent/child relationships. That is unnecessary
work and makes an optional product reconstruct an Engine fact through N full
projections.

## Decision

- Add optional direct `ThreadLineage` to `ThreadSummary`. Root Threads omit it.
  Fork summaries contain the exact same immutable value projected by
  `ThreadForked`; no alternative client-owned ancestry is introduced.
- Keep State event and snapshot schema at 9. Lineage already exists in the
  authoritative journal, so this change adds no durable fact and needs no
  SQLite migration.
- Maintain a disposable lineage index in the Memory Event Store. SQLite reads
  the second event of each selected recent stream through the existing
  `(thread_id, sequence)` index, decodes it with normal State bounds and schema
  validation, and returns lineage only when it is `ThreadForked`.
- Preserve recent-update ordering and the existing 64-summary page bound.
  Clients can construct a forest from the returned page. A parent outside that
  page remains an opaque parent identity; the Engine does not silently perform
  unbounded ancestor closure.
- Advance the typed client protocol to 16 because the observable
  `ThreadSummary` wire shape changed. State compatibility coordinates remain
  schema 9.
- Render direct parent identity and parent stream version in the optional TUI
  Sessions panel. The TUI consumes Protocol only and owns no lineage state.

## Consequences

Recent session navigation can expose authoritative branches without loading
message content or duplicating the State projector. Memory and SQLite return
the same typed summary. The SQLite query adds one indexed second-event lookup
per bounded result, not an unbounded history projection.

This is a recent-page Thread forest, not Pi's in-place entry DAG. It does not
implement entry-level leaf switching, recursive ancestor inclusion, branch
summarization, export/import, or archival. Those remain separate contracts and
must not be inferred from the display.

## Rejected alternatives

- Put parent columns in `streams`: this would require a schema migration for a
  disposable projection of an existing event.
- Have the TUI call `get_thread` for every row: it couples product navigation
  cost to full history size and duplicates Engine projection work.
- Recursively include every ancestor: it defeats the bounded page contract and
  can exceed response limits.
- Claim equivalence with Pi's entry tree: the persistence and navigation models
  are intentionally different.

## Evidence

- Memory and SQLite fork tests assert identical lineage-bearing summaries.
- Protocol wire and functional tests cover the optional summary shape.
- TUI render and real-PTY fork smoke tests exercise the client projection.
- Summary validation rejects self-parenting, invalid hashes, and parent
  boundaries that cannot precede the child's latest sequence.

## Sources

- [Pi defensive session-entry tree at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/core/session-manager.ts)
- [ADR 0093: atomic terminal-boundary Thread fork and lineage](0093-atomic-thread-fork-and-lineage.md)
