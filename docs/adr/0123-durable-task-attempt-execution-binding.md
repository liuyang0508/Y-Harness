# ADR 0123: Durable tenant-exact Task-attempt execution binding

- Status: accepted
- Date: 2026-07-29

## Context

A governed Turn binding does not prove which deployment executed a durable
Task attempt. Task work may outlive a process, lose a lease, retry under a new
release, or complete without creating a Turn. Binding only the current
`TaskLease` is insufficient because terminal settlement and expiry remove that
lease from the current Task projection.

Core must retain a provider-neutral execution coordinate without importing
Domain Pack lifecycle types. The coordinate must be persisted before Workspace
or executor side effects and must remain attributable after retry and
settlement.

## Decision

- Reuse the generic `ExecutionBinding` value introduced by ADR 0122.
- Add append-only `TaskAttemptBinding` evidence to the Task Graph. Each record
  binds the exact Task, lease fencing token, monotonic attempt, worker, claim
  time, and execution coordinate.
- Add `TaskGraph::claim_ready_with_binding`; retain `claim_ready` for ungoverned
  graphs. Once a Task has a bound attempt, reject every later unbound retry.
- Return the exact optional binding in `TaskClaim`, so the executor observes
  the same coordinate that was committed with its lease.
- Let a trusted embedded host configure one `AuthorityContext` and optional
  binding on `Orchestrator`. Persist the claim by tenant-aware Coordinator CAS
  before Workspace preparation or executor entry.
- Preserve all prior binding evidence when a lease expires or a Task reaches a
  terminal state. Exact lease fencing continues to settle the current attempt.
- Require every stored binding tenant to equal the immutable Task Graph tenant.
  Memory and SQLite create, load, and CAS paths validate this projection.
- Keep Protocol claim input free of execution-binding fields. Current
  protocol-owned claims are unbound and cannot take over a Task that has
  entered bound mode.
- Advance Task Graph schema 2 to 3 and Client Protocol 24 to 25. The explicit,
  backup-first migration accepts schema 1 and schema 2, preserves schema-2
  tenant ownership, and rejects schema-2 rows that claim schema-3 evidence.

## Consequences

- Incident review can identify the exact deployment used by every governed
  attempt, including expired and successful attempts.
- A retry may deliberately use a newer binding, but it cannot silently become
  ungoverned after the Task enters bound mode.
- A crash after claim persistence and before executor entry leaves an auditable
  attempt and a fenced lease; normal expiry recovery creates a new attempt.
- Binding evidence consumes the existing bounded Graph materialization budget
  and has an explicit count ceiling. Capacity failure happens before Graph
  mutation.
- Protocol v25 changes the advertised Task schema coordinate. Trusted binding
  authorship and detailed binding-evidence inspection remain embedded-only.
- This Task-binding decision does not implement remote control-plane
  activation, canary rollout, Workflow timers, multi-node consensus, or
  Artifact blob authorization. ADR 0124 separately adds optional Domain Pack
  role authorization without changing Task schema or Protocol authorship.

## Rejected alternatives

- Store the binding only on `TaskLease`: terminal settlement and expiry would
  erase historical evidence.
- Store one binding on the whole Graph or Task definition: retries may execute
  under different immutable releases.
- Attach evidence only at completion: side effects would occur before the
  authoritative binding exists, and crashes would leave no proof.
- Accept a Protocol-authored binding: authenticated worker identity does not
  make the worker a trusted deployment control plane.
- Infer a missing retry binding from the latest activation: that rewrites
  historical execution truth and creates a time-of-check/time-of-use gap.
