# ADR 0024: Durable revisioned Approval Inbox

- Status: Accepted
- Date: 2026-07-25

## Context

A synchronous `ApprovalHandler` is sufficient for an in-process prompt, but it
does not provide an operator inbox, cross-process settlement, durable audit
state, or protection against two approvers racing. Persisting only the final
decision is also insufficient: a crash while waiting would leave no durable
request identity in the State journal.

Durable approval storage must not imply that an interrupted Agent Loop can
safely replay a Tool call. Continuation recovery is a separate state-machine
problem.

## Decision

- Record `ApprovalRequested` in authoritative Turn State before waiting. It
  carries approval ID, call ID, Tool identity, bounded reason, and risk class;
  Tool input remains in the preceding `ToolCall`.
- Define a provider-neutral `ApprovalInbox` with idempotent submit, get, bounded
  pending-page read, revision-CAS settlement, and Turn orphaning.
- Provide in-memory and SQLite implementations with identical semantics.
- SQLite uses WAL, `synchronous=FULL`, immediate settlement transactions,
  512 KiB records, durable status indexes, and index/body consistency checks.
- A request is immutable after submission. Reusing its ID with different
  content fails. A settlement is immutable and only succeeds at the observed
  pending revision.
- `InboxApprovalHandler` submits a request and polls at an explicitly bounded
  interval. Runtime cancellation and deadline controls remain outside the
  handler.
- Runtime recovery marks pending approvals from interrupted Turns `orphaned`.
  Approval wait failure also attempts orphaning after the Turn is durably
  settled, so cleanup failure cannot leave the Turn running.
- An orphaned approval cannot receive a late settlement.
- Policy and approval reasons are validated to 1–4,096 non-control bytes before
  entering State or the inbox.

## Consequences

Operators can durably list and settle approval requests from another process.
Competing settlers receive a typed `ApprovalConflict`. State and inbox evidence
share Thread, Turn, approval, and Tool-call correlation identities.

This slice is deliberately fail-safe across process loss: recovering a running
Turn interrupts it and orphans its pending approval, so a later click cannot
execute a Tool unexpectedly. It does not resume the original Agent Loop stack.
Exact continuation requires a durable continuation capsule, paused Turn state,
recompilable Context scope, and idempotent Tool-start protocol.

The typed protocol exposes capability-gated pending/get/settle commands and
maps stale revisions to `approval_conflict`. ADR 0063 later adds
authority-scoped requester/settler attribution, exact-actor separation of duty,
and backup-first schema migration. Subject/SAN-to-human mapping, signatures,
notifications, retention, and tenant/role ownership remain subsequent work.
ADRs 0028 and 0029 add mTLS plus exact certificate-fingerprint command grants;
ADR 0063 uses that fingerprint as a certificate subject but still does not
claim it is a human or organizational role.

## Rejected alternatives

- Store only the final decision: crashes would lose pending intent and
  correlation.
- Last-write-wins settlement: two operators could unknowingly override one
  another.
- Automatically rerun an interrupted Turn after approval: model output and Tool
  side effects are not generally replay-safe.
- Leave a request pending after its Turn is interrupted: the operator could be
  shown an actionable approval that no execution can safely consume.
