# ADR 0071: Bounded fenced Task Orchestrator

- Status: Accepted
- Date: 2026-07-25

## Context

`TaskGraph` and `TaskCoordinator` already defined dependencies, ordered
messages, Artifacts, workspace requirements, expiring leases, fencing tokens,
and durable compare-and-swap. They did not execute a claim. Every embedding
host would otherwise have to rebuild concurrency admission, timeout,
cancellation, panic isolation, conflict handling, and stale-result rejection.

A scheduler must not weaken the existing external-effect boundary. A Task may
have completed real work before its coordinator settlement races another
worker, so retries cannot be inferred merely from a missing completion.

## Decision

- Add a public `TaskExecutor` port. One request contains the graph identity,
  immutable Task claim, current fencing lease, workspace requirement, and a
  cooperative cancellation token.
- Add a public `Orchestrator` that consumes any `TaskCoordinator` and one
  operator-assigned worker identity. It is a Harness primitive, not a business
  Agent implementation.
- Execute at most eight claims concurrently by default, configurable from 1 to
  64. Claims still use the Task Graph's independent 64-item batch ceiling.
- Apply a four-minute Task timeout and five-minute lease by default.
  Task timeout is bounded to 1 millisecond–24 hours, lease duration to
  1 millisecond–7 days, and the persisted millisecond lease must outlive the
  Task timeout. Coordinator polling is bounded to 1 millisecond–60 seconds.
- Reuse the Runtime's capability-Future isolation for constructor, poll, and
  destructor panics. Cancel the Task signal before releasing its Future on
  completion, timeout, fencing, or scheduler stop.
- Materialize executor errors, panics, invalid completions, and timeouts as
  bounded Task failures. Independent ready Tasks continue; the Task Graph
  propagates deterministic dependency blocking.
- Reload the current snapshot before every settlement and require the exact
  lease ID, owner, attempt, and unexpired deadline. Discard a stale result
  without mutation.
- Recompute claims and exact-lease settlements after coordinator CAS conflicts,
  with a finite 64-attempt contention window. Never replay an executor merely
  because persistence conflicted.
- When another coordinator mutation cancels or replaces a local claim, cancel
  the old executor. Scheduler-wide cancellation stops local Futures but leaves
  durable leases in place until their normal expiry, preventing immediate
  overlap with uncertain work.
- Keep process isolation behind the executor's governed execution environment.
  Workspace preparation was initially deferred; it is now supplied through the
  separate provider lifecycle in
  [ADR 0073](0073-governed-task-workspaces-and-pinned-git-worktrees.md). An
  in-process executor is trusted; an untrusted sub-Agent must use a Process
  Broker.

## Consequences

Y-Harness now has an executable Orchestration loop over the same durable DAG
and fencing contracts used by embedded and SQLite coordinators. Dependency
ordering, bounded fan-out, failure isolation, cancellation, and stale-worker
rejection are shared behavior instead of host boilerplate.

The scheduler deliberately has no automatic retry policy for a failed or
timed-out Task. The domain cannot know whether an executor produced an external
effect. Lease expiry remains recovery for a lost worker, and a host may model
safe retries as new explicit Tasks or use an idempotent executor.

Task Graph schema 1 and Protocol 9 remain unchanged. The Orchestrator is an
embedded capability; remote orchestration commands, multi-node consensus,
distributed clocks, durable cross-host workspace ownership, and executor
discovery remain separate host or future protocol concerns.

## Rejected alternatives

- Let executors mutate snapshots directly: bypasses coordinator CAS and
  fencing.
- Settle against the snapshot used to claim: unrelated durable mutations would
  be overwritten or spuriously rejected.
- Automatically rerun every error, panic, or timeout: may duplicate uncertain
  external effects.
- Spawn every ready Task: turns graph size into unbounded process-local fan-out.
- Treat a lease token as multi-node consensus: fencing orders settlements only
  through the authority of the selected coordinator.
