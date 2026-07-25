# ADR 0047: Bounded protocol Operation drain

- Status: Accepted
- Date: 2026-07-25

## Context

Protocol Operations run independently from the connection that started them.
The mTLS server already stopped accepting sockets and aborted active sessions
on host shutdown, but its accepted Turns could continue invoking models or
tools afterward. Stdio EOF had the same gap. A host could therefore report
shutdown while process-local work and external side effects were still active.

Blind task abortion is not a safe substitute. State append and terminal
settlement are deliberately outside interruptible waits; aborting during one
of those phases can leave durable truth different from the process-local
Operation record.

## Decision

- Give every `ProtocolHandler` one shared, irreversible Operation lifecycle.
- `shutdown(timeout)` first stops accepting new `StartTurn` commands, then
  requests cooperative cancellation of every retained running Operation.
- Wait with a validated 1 ms to 1 hour deadline and the standard
  notification-plus-recheck pattern so settlement notifications cannot be
  lost.
- Return only content-free cancellation, settlement, remaining counts, and a
  separate Runtime-background-drained flag. Repeated shutdown is safe and the
  handler never re-enters accepting state.
- Do not forcibly abort workers at the deadline. A remaining Operation is
  reported honestly and requires the host to retain the Runtime or later prove
  exclusive Thread ownership before recovery.
- Notify drain waiters only after the supervisor has installed a terminal
  Operation status.
- After every Operation settles, spend the remainder of the same deadline on
  Runtime-owned background work. The current Runtime implementation drains
  accepted automatic State snapshot workers. If any Operation remains, do not
  claim background work drained because that Operation can still schedule
  maintenance while settling.
- Stdio performs the default 30-second drain after EOF or framing failure and
  fails shutdown when an Operation remainder persists or Runtime background
  work does not drain.
- The mTLS host exposes a validated configurable drain deadline and includes
  cancellation, settlement, timeout counts, and the background-drained flag in
  its shutdown report after all connection tasks have stopped.

## Consequences

New background work cannot race into the registry after shutdown begins.
Cooperative model, Tool, Context, Policy, and approval waits settle through the
ordinary Agent Loop and durable terminal State path. A host receives explicit
evidence when non-interruptible persistence did not finish before its deadline
instead of a false clean-shutdown claim.

Protocol hosts now have one deadline and one shutdown entry point for accepted
Operations plus current Runtime-owned snapshot maintenance; they do not need a
second State Engine handle merely to drain snapshots. Any future Runtime
maintenance subsystem must join `drain_background_work` before it can be
enabled by these hosts. Process termination with either a reported Operation
remainder or an undrained-background flag still needs explicit takeover
recovery and does not make uncertain external side effects replay-safe.

## Rejected alternatives

- Tie Operation lifetime to one client connection: reconnects would cancel
  useful work and conflate transport with Runtime ownership.
- Abort all worker tasks immediately: State writes and external effects may
  already be in progress.
- Keep accepting Turns while draining: a concurrent connection can prevent
  convergence indefinitely.
- Wait forever: host shutdown itself needs a finite availability boundary.
