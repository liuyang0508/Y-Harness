# ADR 0065: Fingerprinted pre-Tool approval resumption

- Status: Accepted
- Date: 2026-07-25

## Context

An Approval Inbox can survive process loss, but durable approval storage alone
does not prove that an Agent Loop may safely continue. Reconstructing a Tool
call under changed Context, Memory, Model identity, Tool registration, or
requester authority could apply an approval to work the approver did not
review. Replaying after an `ApprovalDecision` is even less safe: the Tool may
already have produced an external effect even when no `ToolResult` reached
State.

Generic automatic Turn recovery therefore interrupted running Turns and
orphaned their approvals. That is safe, but unnecessarily discards the one
continuation boundary where State proves that Tool execution has not started:
the last durable Items are exactly `ToolCall`, `PolicyDecision::Ask`, and
`ApprovalRequested`.

## Decision

- Advance authoritative State events from schema 2 to schema 3. A schema-3
  `ApprovalRequested` records the authenticated requester, Tool origin, and
  lowercase SHA-256 of the exact serialized `ModelRequest` that produced the
  pending `ToolCall`. Schema-1 and schema-2 approval events remain readable but
  are not resumable.
- Advance the exact client protocol from 7 to 8 because `GetEvents` may expose
  the new State Item shape and initialization advertises State schema 3.
- Provide an embedded Runtime API for explicitly resuming one named running
  Turn. The host must first prove exclusive Thread ownership and that the old
  worker has stopped. The Runtime's in-process guard is not a distributed
  lease.
- Resume only when the final three Items are exactly the correlated
  `ToolCall`, `PolicyDecision::Ask`, and `ApprovalRequested`. Require the same
  Model identity and origin, requester actor, Tool origin, call identity,
  Policy rationale, and risk.
- Recompile prior conversation, semantic summary, Memory Context, Context
  blocks, and current Tool descriptors using the supplied execution options.
  Reconstruct the pre-call `ModelRequest` and compare its allocation-bounded
  SHA-256 before consulting the Approval Inbox or invoking the Tool.
- Re-submit the identical Approval request to the idempotent Inbox handler. A
  pending request continues waiting and an already settled request returns its
  durable decision. Persist `ApprovalDecision` before Tool execution, as in
  ordinary execution.
- Refuse continuation after any later State Item. In particular, never replay a
  Tool when `ApprovalDecision` exists without `ToolResult`; that boundary is
  intentionally classified as an unknown external-effect state.
- Keep generic `recover_thread` semantics unchanged: it interrupts abandoned
  running Turns and orphans pending approvals.
- Do not expose remote resume/takeover in protocol 8. A service command requires
  a lease/fencing authority that can prove exclusive ownership across hosts.

State snapshot schema remains 3. Snapshots are disposable caches, the new Item
fields are optional for decoding prior projections, and State event schema 3 is
the authoritative compatibility boundary.

## Consequences

A worker can restart at a durably waiting approval without another Model call
or Tool execution, provided all replay inputs are identical. Context, Memory,
Tool descriptor, origin, actor, and Model drift fail closed while the Turn
remains running and the Tool remains untouched.

The fingerprint proves the exact serialized request and registered metadata. It
does not attest to the binary identity or internal behavior of a trusted
in-process Tool whose implementation changes without changing its descriptor
and origin. Hosts remain responsible for deployment integrity and exclusive
ownership.

Crashes after `ApprovalDecision` or during Tool execution still require
Tool-specific idempotency/status reconciliation, explicit compensation, or
operator interruption. Y-Harness makes no generic exactly-once claim for
external effects.

Schema-1 and schema-2 SQLite State stores require the existing offline,
backup-first migration command before the schema-3 writer opens them.
Historical event JSON and schema labels remain immutable; migration advances
only writer metadata.

## Rejected alternatives

- Resume from the Approval Inbox alone: it does not bind current Model request,
  Context, Tool metadata, or requester authority.
- Re-run the Model and compare only its new Tool call: Model output is
  nondeterministic and the new call was not the approved request.
- Persist the complete Model request again in `ApprovalRequested`: duplicates
  bounded but potentially sensitive Context in authoritative State.
- Replay whenever an approval is settled and no `ToolResult` exists: absence of
  a result does not prove absence of an external effect.
- Add a protocol takeover command without fencing: two live workers could both
  pass local checks and execute the same Tool.
