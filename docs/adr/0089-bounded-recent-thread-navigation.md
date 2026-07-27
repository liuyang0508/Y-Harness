# ADR 0089: Bounded recent-Thread navigation

- Status: Accepted; amended by ADRs 0092, 0093, and 0094
- Date: 2026-07-27

## Context

The Engine could create and load an exact Thread, and the TUI could resume one
when the operator already knew its opaque ID. Neither the State port nor
Protocol exposed a bounded Thread index, so a product client could not offer a
real recent-session list without opening Engine databases or maintaining a
second authority.

Loading every Thread projection to build a list would scale with complete
conversation history and duplicate the Event Store's index responsibility.

## Decision

- Add an optional `EventStore` recent-Thread listing capability. Stores must
  declare support; unsupported stores do not advertise `thread.list`.
- Return content-free summaries containing the Thread ID, latest global event
  sequence and timestamp, and current stream version.
- Order summaries by latest global sequence descending. Page with an exclusive
  `before_sequence` cursor and a hard 64-entry client page.
- Implement the capability in the in-memory and SQLite stores. SQLite queries
  only the latest event and stream metadata per Thread; it does not materialize
  event bodies or full projections.
- Validate store output for bounds, identity, ordering, uniqueness, nonzero
  sequence/version, and cursor compliance before returning it.
- Expose the same page through Runtime and capability-gated Protocol v13.
  This is an additive negotiated command, so the protocol coordinate does not
  change.
- Let the optional TUI render the latest 64 Threads and resume a selected
  Thread through ordinary `get_thread`. The client never opens SQLite.

## Consequences

Reference-service users can discover and resume recent durable Threads after a
restart. Embedded hosts with custom Event Stores remain honest: they must opt
in and implement the bounded index before clients see the capability.

The list is a live cursor view, not a snapshot transaction. Concurrent updates
may move a Thread to a newer page, so products refresh from the first page.
This decision itself adds no Thread names, archive/delete operations,
branch/fork/clone, import/export, or lineage semantics. ADR 0092 later adds
explicit Engine-owned names; ADR 0093 later adds atomic fork and direct
lineage; ADR 0094 projects that lineage into the same bounded summaries.
Archive/delete, import/export, and entry-level in-place trees remain separate.

## Rejected alternatives

- Scan Engine SQLite from the TUI: violates the independent-client boundary.
- Cache IDs only in the TUI: loses restart and multi-client authority.
- Project every Thread to derive titles: unbounded history work for a list.
- Add names and branching in the same change: they require distinct durable
  metadata and lineage contracts.

## Evidence

- `state::tests::recent_thread_pages_are_bounded_and_store_consistent`
- `protocol::tests::recent_threads_are_capability_gated_and_cursor_bounded`
- `persistent_service_recovers_threads_and_task_graphs_after_restart`
- `ui::tests::session_panel_renders_authoritative_thread_summaries`
