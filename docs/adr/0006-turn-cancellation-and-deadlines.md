# ADR 0006: Cooperative Turn cancellation and deterministic deadlines

- Status: Accepted
- Date: 2026-07-25

## Decision

Every explicitly controlled Turn may receive one cloneable, monotonic
`CancellationToken` and one optional absolute execution deadline. The runtime
observes both while awaiting Context, Model, Policy, and Tool capabilities.
Invoked tools receive the same token for cooperative cleanup.

Cancellation and timeout are distinct terminal outcomes:

- the journal records a typed `TurnStopped` item with reason and active phase;
- the Turn settles as `cancelled` or `timed_out`, not as a generic failure;
- recovery reserves `interrupted` for a running Turn abandoned without an
  orderly settlement.

State writes and terminal settlement are not interrupted by the Turn deadline.
They may finish after the deadline so the authoritative journal does not trade
consistency for a tighter wall-clock return.

## Side-effect boundary

Stopping an await abandons the runtime's wait; it does not prove that an
external side effect did not begin. A cancelled or timed-out tool is never
automatically replayed. Capability implementations should observe the supplied
token where they can safely stop their own work.

Dropping the caller's `run_turn` future is not an explicit cancellation signal.
If no terminal event was persisted, normal recovery marks the Turn
`interrupted`.

## Failure settlement

Model, Context, and Policy provider failures settle the active Turn as
`failed`. Ordinary tool errors remain structured `ToolResult` items so the
model can inspect and correct them. Cancellation and timeout during Tool
execution bypass that correction loop and settle the Turn immediately.

If writing the terminal evidence itself fails, the State error supersedes the
original operation error because durable completion is then uncertain.

## Rationale

The distinction among failure, cancellation, deadline, and process
interruption is required for safe retry policy, operator UX, recovery, and
evaluation. A single generic error cannot express whether a side effect may be
in flight or whether an unfinished Turn needs recovery.
