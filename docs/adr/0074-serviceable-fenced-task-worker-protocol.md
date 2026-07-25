# ADR 0074: Serviceable fenced Task worker protocol

- Status: Accepted
- Date: 2026-07-25

## Context

The Task DAG, revisioned Coordinator, fenced leases, Mailbox, Orchestrator, and
Workspace Providers were public embedded Rust contracts. The client protocol
could serve Threads, Turns, State, Operations, and Approvals, but an external
worker host could not safely coordinate Tasks without linking the engine.

Exposing the entire `TaskGraph` would make a potentially 64 MiB aggregate a
normal response and invite clients to submit forged mutations. Accepting a
worker name or timestamps from the request would let one client impersonate
another or extend a lease against an untrusted clock. Serving an arbitrary
`TaskExecutor` command would also merge process authority into coordination.

## Decision

- Advance the exact client protocol to version 10.
- Keep Task execution outside the wire contract. The protocol exposes durable
  coordination only; a host chooses and governs its executor separately.
- Add optional graph creation, summary, record paging, and explicit-revision
  cancellation commands. Capabilities are advertised only when the handler has
  a `TaskCoordinator`.
- Add worker claim, heartbeat, completion, failure, inbox, and send commands.
  The worker identity is derived only from the trusted transport:
  `local-process` for stdio or the exact lowercase SHA-256 mTLS leaf
  fingerprint. A request cannot provide an owner.
- Use the server clock for claims, heartbeats, and message timestamps. Every
  worker command revalidates the exact current Task, fencing token, owner, and
  unexpired lease.
- Retry at most 64 internal Coordinator compare-and-swap conflicts for claims
  and worker mutations. Reload and revalidate authority before every retry.
  Do not replay ordinary failures.
- Panic-isolate protocol command construction, polling, and Future drop.
  Provider panic payloads never enter the client response.
- Require a positive caller-observed graph revision for operator cancellation.
  A stale revision remains an explicit retryable orchestration conflict.
- Return bounded `TaskGraphSummary` metadata instead of the full graph. Page
  Task records by Task identity with count and encoded-byte limits.
- Default claims to one and cap the protocol batch at 16. Check encoded claims
  against the response budget before committing their leases.
- Preserve the domain's count-and-byte-bounded Task message pages and the
  protocol's 2 MiB request and 16 MiB response frames.
- Add Workspace Provider API version 1 to `Initialize`, because protocol Task
  definitions expose workspace modes even though provider installation remains
  a host responsibility.

## Consequences

Language-neutral worker hosts can now recover, claim, communicate, heartbeat,
and settle durable Tasks without embedding the Rust crate. A leaked lease token
alone is insufficient for a different authenticated principal to mutate its
Task. Lost create responses are reconciled through the caller-chosen graph
identity and `get_task_graph`; duplicate creation does not replace state.

The protocol does not launch commands, distribute secrets, provision worker
machines, or claim multi-node consensus. SQLite remains single-host durable
coordination with multi-process CAS. Workspace enforcement and executor
containment remain explicit host responsibilities.

## Rejected alternatives

- Return the complete graph on every read: violates ordinary response and
  allocation boundaries.
- Accept `worker`, `owner`, or `now_ms` in requests: enables identity or clock
  forgery.
- Let worker commands submit an expected graph revision: forces avoidable
  client retry races while still requiring server-side lease revalidation.
- Retry every coordinator error: can replay non-conflict failures and conceal
  provider defects.
- Expose arbitrary executor commands through the protocol: conflates
  coordination with Tool/Process authority and bypasses host policy.
- Advertise Task methods without a configured Coordinator: discovery would
  promise a capability that cannot execute.
