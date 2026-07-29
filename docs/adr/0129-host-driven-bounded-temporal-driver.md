# ADR 0129: Host-driven bounded Temporal Driver

- Status: accepted
- Date: 2026-07-29

## Context

Workflow Runs durably retain timer, retry, and optional signal-timeout waits.
Human Handoffs durably retain finite claim expirations. Before this decision,
an authorized caller could advance either fence explicitly, but the Engine
provided no shared, bounded discovery and advancement primitive for a hosting
service.

Putting an implicit infinite polling task inside Core would give an embedded
library hidden process lifetime, clock, retry, shutdown, and failure policy.
Creating a second authoritative scheduler database would duplicate Workflow
and Handoff truth and require atomic cross-database writes to avoid lost or
phantom timers.

## Decision

- Add embedded Temporal Driver API 1 as an optional composition module. It
  starts no task, sleeps on no interval, and owns no clock. A host supplies
  trusted Unix time and invokes one bounded `tick` when its lifecycle permits.
- Extend Workflow and Human Handoff Coordinator contracts with default
  fail-closed temporal-scan methods. Existing custom Coordinator
  implementations remain source-compatible but do not claim discovery until
  they implement the method.
- Built-in Memory and SQLite Coordinators scan authoritative aggregates in
  tenant-local identity order. One call visits 1–256 aggregates. The result
  reports visited count, due fences, last visited identity, and whether the
  current sweep has another page.
- Treat every Coordinator page as extension output. Before the first mutation,
  the driver revalidates its count bounds, continuation presence and progress,
  due-identity order, exact tenant, positive revision, fence identities, and
  current time eligibility. A malformed page aborts the complete tick.
- A Workflow due item contains the exact Run revision, wait fence, and
  inclusive boundary. Timer waits, retry waits, and signal waits with an
  expired timeout are eligible. Signal waits without a timeout are not.
- A Human Handoff due item contains the exact case revision, claim fence, and
  exclusive expiration. At the exact boundary the claim is expired.
- The scan cursor is disposable acceleration state, not authority. Each source
  resets independently after its identity sweep. Losing a cursor repeats part
  of a sweep and may increase latency, but the due fact remains in the
  authoritative aggregate until a fenced transition commits.
- Complete both configured source scans and derive every command identity
  before the first mutation. This prevents a second-source discovery failure
  from following already committed first-source effects.
- Derive each maintenance command identity from the action kind, trusted
  actor, aggregate identity, and exact wait/claim fence. A retry by the same
  worker is stable; another actor receives a different identity.
- Advance state only through existing commands:
  `WorkflowCommandKind::WakeDue` and
  `HumanHandoffCommandKind::ExpireClaim`. Existing aggregate revision CAS,
  wait/claim fences, server-time checks, command idempotency, and immutable
  transitions remain the only settlement authority.
- Settle every attempted item independently as applied, duplicate, fenced by
  a newer revision, or content-free failed. Once mutation begins, a later
  failure never relabels or hides an earlier committed transition.
- Keep Protocol at v28 and Workflow/Handoff stores at schema 1. No new wire
  command or durable projection is introduced. SQLite reads only a bounded
  identity page and validates every selected aggregate using its existing
  decoder.

## Bounds and concurrency

- `scan_limit` is 1–256 per configured source and per tick.
- A tick therefore attempts at most 512 transitions.
- Workflow attempts are reported before Human Handoff attempts, and each
  source is ordered by aggregate identity.
- Multiple hosts may scan the same due fence. One may commit; exact retries
  become duplicate and stale competing mutations become fenced. This is
  single-store CAS safety, not leader election or distributed consensus.
- Identity-order scanning deliberately avoids an unversioned due-time index.
  Poll latency is bounded by the host's interval, scan limit, and complete
  identity-sweep size rather than claimed as a real-time deadline.
- Memory Coordinators seek directly to the `(tenant, cursor)` B-tree range;
  SQLite uses the tenant/identity primary key and bounds every selected text
  field before allocation. Other tenants do not consume the declared page.

## Authority and lifecycle

The host owns authentication, tenant selection, wall-clock source, polling
interval, error observation, shutdown, and restart. A different actor changes
the deterministic command identity; deployments should therefore use one
stable maintenance principal per tenant when duplicate recognition across
restarts is required.

`TemporalAttemptOutcome::Failed` is intentionally content-free. Coordinator
and database diagnostics must be captured inside the hosting/observability
boundary rather than copied into a cross-tenant report body.

## Non-claims

This slice does not:

- start a background worker in `yh serve`;
- guarantee real-time timer latency or monotonic operating-system wall clocks;
- execute a retried Task, Tool, compensation, or business action;
- route a Human Handoff channel or notify an operator;
- create a durable outbox, Webhook delivery, or atomic cross-database event;
- persist scan cursors or elect a poller leader;
- provide multi-node availability or distributed consensus.

Those capabilities may compose around the deterministic tick, but they do not
enter the semantic Core implicitly.

## Rejected alternatives

- Spawn a Core-owned interval: hidden lifetime and shutdown policy violate the
  embeddable headless boundary.
- Store duplicate timers in a scheduler database: without an atomic
  cross-store commit it creates lost/phantom work and two sources of truth.
- Scan every row in one call: storage size would become an unbounded latency
  and allocation input.
- Order directly by JSON-extracted due time: it relies on storage-specific JSON
  behavior, weakens bounded decode validation, and silently introduces an
  unversioned projection.
- Treat every race as an error: concurrent pollers are expected; existing CAS
  and idempotency distinguish committed, duplicate, and fenced outcomes.
