# ADR 0025: Explicit pre-1.0 compatibility coordinates

- Status: Accepted
- Date: 2026-07-25

## Decision

Y-Harness publishes independent exact coordinates for protocol, State events,
State snapshots, Approval Inbox records, Task Coordinator graphs, Memory
Provider API, Token Counter API, Conversation Compactor API, Secret Provider
API, Skill package API, and HTTPS model-gateway API. Protocol initialization
advertises these coordinates together with the Cargo engine version.

Until 1.0, patch releases preserve a `0.y` line; minor releases may break only
with explicit migration notes. Unknown authoritative schemas fail closed,
snapshots remain discardable, mixed-version SQLite writers are unsupported,
and no downgrade compatibility is implied.

The first actual durable schema change must add fixture-based, crash-tested,
forward-only migration tooling before the writer is enabled.

## Rationale

One global version cannot describe independent extension and persistence
surfaces. Conversely, undocumented “best effort” compatibility turns stored
agent evidence into an experiment. Exact advertised coordinates make current
limits machine-readable without promising migration code that has not yet been
needed or tested.

## Consequences

The current schema set is the `0.1.0` baseline. This decision declares policy,
not rolling-upgrade support. Release readiness still blocks on migration
fixtures/tooling when the first schema change is proposed.
