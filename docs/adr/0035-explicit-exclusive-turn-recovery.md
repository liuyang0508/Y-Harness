# ADR 0035: Explicit exclusive Turn recovery

- Status: Accepted
- Date: 2026-07-25
- Amended by: ADR 0113 (permissioned, exact-Turn protocol takeover)

## Context

Runtime Turn startup previously called `recover_thread` before starting new
work. That is convenient after a process crash, but unsafe when independent
Runtime instances share an Event Store: the second Runtime cannot distinguish
an abandoned Turn from one still executing in the first process. It could win
the State compare-and-append, mark healthy work `interrupted`, and then start a
new Turn on the same Thread.

Optimistic State concurrency prevents malformed overlapping journal events; it
does not prove that a worker is dead or grant takeover authority.

## Decision

- Normal `run_turn` startup only loads authoritative Thread state. It never
  performs recovery.
- If the Thread already has a running Turn, `StateEngine::start_turn` rejects
  the new Turn. The competing Runtime makes no State mutation.
- Keep `recover_thread` as an explicit host API. Its contract requires the
  caller to establish exclusive Thread ownership and confirm the previous
  worker stopped before marking the Turn `interrupted`.
- Recovery continues to orphan any durable approval that the abandoned Turn
  can no longer consume. It never replays Tool work.
- Do not expose recovery through the current remote client protocol. Adding a
  takeover command would first require a separately authenticated ownership or
  fencing authority.

ADR 0113 later satisfies the serviceability part of this condition with a
separate `thread.recover` permission, required exact-Turn fencing, same-host
live-Operation refusal, and an explicit caller-owned takeover contract. It
does not claim distributed lease or failure-detection semantics.

## Consequences

Independent Runtime instances sharing SQLite can safely contend for a Thread:
one live Turn remains authoritative and later starts fail closed. Restart
recovery is no longer an implicit convenience; service hosts must perform it
during a controlled ownership transition.

This change does not claim distributed liveness. Detecting a dead remote worker
requires a lease/fencing design with a trustworthy clock and durable ownership
record. Until that exists, a host-provided exclusive startup procedure is the
honest recovery boundary.

## Rejected alternatives

- Recover automatically whenever a running Turn is observed: can terminate
  healthy work and permit overlapping external side effects.
- Assume State CAS proves liveness: it only orders writes.
- Use a fixed age threshold: long model, approval, or Tool waits can be healthy,
  and wall-clock age alone is not ownership evidence.
- Retry after a conflict: can replay uncertain provider or Tool side effects.
- Add a remote force-recover command now: exposes a destructive takeover
  operation without a proven ownership authority.
