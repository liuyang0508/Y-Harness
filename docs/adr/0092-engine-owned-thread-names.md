# ADR 0092: Engine-owned durable Thread names

- Status: Accepted
- Date: 2026-07-28

## Context

Recent-Thread navigation exposed durable identities but no human-readable
metadata. Letting each TUI, Web, or Desktop client keep its own title database
would create conflicting authorities. Deriving titles from conversation
content would add model calls, privacy policy, provenance, and non-determinism
to a basic State concern.

Recent listing must remain bounded and must not replay full Thread histories.
Any SQLite index used for the list therefore has to be a disposable projection
whose consistency is tied to the authoritative journal.

## Decision

- Add schema-8 `ThreadNamed { name: Option<String> }`. A value sets the name;
  `None` clears it. Project the latest value into `Thread.name`.
- Accept only explicit 1–256-byte, trimmed, non-control UTF-8 names. Do not
  infer, summarize, or normalize conversation content.
- Maintain the in-memory name map and SQLite `streams.name` in the same append
  critical section or transaction as the event and stream version.
- Retain ordered naming events inside disposable schema-8 snapshots so snapshot
  validation can reconstruct exact event counts and recovery-byte charges even
  after repeated rename/clear transitions.
- Validate SQLite's name projection against the latest schema-8 naming event
  when opening. Drift fails closed instead of leaking a second authority into
  product clients.
- Add `thread.name`, `set_thread_name`, and `thread_named` in exact Protocol
  v14. Surface the optional name in both `Thread` and `ThreadSummary`.
- Migrate schema-1 through schema-7 stores backup-first. Add the nullable
  projection column, discard old disposable snapshots, advance State event and
  snapshot coordinates to 8, and never rewrite historical events.
- Let the optional TUI expose `/name [title]`; clients still use only Protocol.

## Consequences

Every client observes one durable name, and recent lists stay independent of
conversation size. Naming consumes one ordinary State event and follows the
existing per-Thread capacity and compare-and-swap rules.

There is no automatic title generation, alias search, rename history UI,
archive/delete lifecycle, or Thread lineage in this decision. Those require
separate evidence and contracts.

## Rejected alternatives

- TUI-local title storage: breaks multi-client and restart authority.
- Derive a title from the first prompt: leaks conversation content into an
  index and makes implicit content mutation authoritative.
- Load every Thread when listing: scales with full history instead of the page
  bound.
- Make the SQLite column authoritative: recovery would depend on a mutable
  cache rather than the append-only journal.
- Add a new metadata database: duplicates the existing transactional stream
  index without gaining an independent invariant.

## Evidence

- `state::tests::recent_thread_pages_are_bounded_and_store_consistent`
- `state::tests::sqlite_rejects_thread_name_projection_drift`
- `state::migration::tests::migration_advances_metadata_schemas_without_rewriting_history`
- `protocol::tests::thread_names_are_authorized_durable_and_listed`
- `ui::tests::session_panel_renders_authoritative_thread_summaries`

## Related decisions

- [ADR 0061](0061-backup-first-immutable-history-state-schema-migration.md)
- [ADR 0089](0089-bounded-recent-thread-navigation.md)
