# ADR 0030: Failure-isolated automatic snapshot maintenance

- Status: Accepted
- Date: 2026-07-25

## Context

Explicit snapshots reduce recovery cost, but relying on every host to remember
when to create them makes latency depend on application code. Snapshot work is
linear in retained Thread history and must not lengthen or reverse a successful
journal settlement. Unbounded detached work would create a different
availability failure.

## Decision

- Keep automatic maintenance opt-in through a validated
  `SnapshotMaintenanceConfig`. The host selects an event cadence and a global
  worker-concurrency limit of at most 64.
- Consider maintenance only after a Turn reaches a durable terminal state.
  Ordinary Item appends do not create background work.
- Track a process-local per-Thread event watermark. A Thread gets at most one
  active worker, and a failed attempt is not retried until another configured
  event interval has accumulated.
- Acquire a worker permit without queueing. When capacity is occupied, skip the
  attempt and reconsider at a later terminal Turn instead of retaining an
  unbounded work queue.
- Before rebuilding, accept a valid store snapshot when it is already within
  the configured event interval. Otherwise project the authoritative journal
  and atomically retain the newest snapshot using the existing Event Store
  contract.
- Run provider work in an isolated Tokio task. Store errors, validation errors,
  worker panic, and worker cancellation update content-free counters but never
  change the already committed Turn result.
- Expose scheduled, created, already-current, failure, cadence, in-flight,
  capacity, active-worker, timestamp, and stable failure-class statistics.
  Provider error strings and journal content are not copied into metrics.
- Provide a bounded drain operation for graceful host shutdown.
- Continue retaining exactly one newest disposable snapshot per Thread in the
  built-in stores. Automatic maintenance does not delete journal events.

## Consequences

Configured runtimes maintain recovery caches without adding snapshot I/O to the
State commit latency path. A saturated host sheds cache work rather than
execution work. Operators can alert on failures and capacity skips, then run
the explicit `create_snapshot` operation when detailed diagnostics are needed.

Maintenance statistics and watermarks are process-local. After restart, the
first eligible terminal Turn checks the persisted snapshot and converges
without requiring a second scheduler database. If the host does not drain
before terminating its Tokio runtime, accepted cache work may be cancelled;
the journal remains authoritative.

This policy does not archive events, hold historical snapshots, offload blobs,
or provide legal-hold semantics. Those responsibilities need separate storage
contracts and cannot be inferred from a disposable projection cache.

## Rejected alternatives

- Snapshot synchronously in `finish_turn`: cache failure or latency would
  contaminate authoritative settlement.
- Spawn on every append: large Turns could create needless scheduling and lock
  churn.
- Queue every eligible Thread: an outage in snapshot storage could grow memory
  without bound.
- Retry immediately after failure: persistent provider faults would form a
  tight background loop.
- Hide failures completely: operators could not distinguish a healthy cache
  from silent maintenance loss.
