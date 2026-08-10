# ADR 0127: Durable fenced Workflow Runs above Task execution

- Status: accepted
- Date: 2026-07-29

## Context

The durable Task Graph already owns executable DAG dependencies, bounded
claims, leases, fencing, workspaces, messages, and terminal Task settlement.
It cannot represent a process that waits across hours or days for an external
event, wakes at a timer boundary, delays an explicitly safe retry, or moves to
a newer Workflow implementation after restart.

Adding those states to `TaskStatus` would create a second scheduler inside the
Task aggregate. Treating Approval, Human Handoff, and Tool compensation as
Workflow wait variants would also merge distinct authority and effect models.

## Decision

- Add an independent `WorkflowRun` aggregate above one existing same-tenant
  Task Graph. The Workflow owns time and external-event lifecycle; the Task
  Graph remains the only executable DAG authority.
- Represent the current state as `running`, one exact `waiting` condition, or
  a terminal `completed`, `failed`, or `cancelled` state.
- Support three fenced waits:
  - an exact signal name and source with an optional timeout;
  - an absolute server-clock timer;
  - an explicit retry activity, positive attempt, due time, and effect-scoped
    idempotency key.
- Give every wait an immutable `WorkflowWaitId`. Signal and timer workers must
  settle the exact current wait, so a late delivery cannot wake a later wait.
  At the exact signal-expiration boundary, timeout wins.
- Apply mutations through a stable `WorkflowCommandId`. The aggregate stores a
  SHA-256 of the exact typed command. Repeating the same identity and digest is
  idempotent even after the revision advances; reusing an identity with
  different content fails closed.
- Retain an immutable, contiguous transition history with trusted actor
  attribution, monotonic server application time, command digest, and bounded
  materialization. Deserialization reconstructs the current projection and
  every command digest instead of trusting cached fields.
- Treat 4,096 transitions and 16 MiB of encoded state as work-admission
  ceilings. Starting a wait, scheduling a retry, and migrating a definition
  consume work capacity. Reserve two additional transitions and 278,528
  additional encoded bytes exclusively for signal/timer recovery and terminal
  settlement. This lets a Run use its final work slot to enter `waiting`, then
  wake and still fail, complete, or cancel without making either reserve
  available to further expansion.
- Keep the reserve finite: the absolute hard boundaries are 4,098 transitions
  and 17,055,744 encoded bytes. Exact duplicate recognition still precedes
  admission so an uncertain response remains retryable at either boundary.
  Deserialization rejects work transitions placed in the reserved transition
  window. These rules change no serialized field or enum representation.
- Permit definition migration only at a durable wait boundary, and only to the
  same name with a strictly newer semantic version and different immutable
  digest. Migration does not rewrite historical transitions or mutate the
  linked Task Graph.
- Persist Runs through a revisioned, tenant-partitioned Coordinator. Memory and
  SQLite implementations share create/load/apply semantics. SQLite store
  schema 1 uses WAL, `synchronous=FULL`, immediate transactions, an explicit
  metadata table, bounded JSON, and fail-closed partial/unknown layout checks.
- Compose persistence and Task authority in `WorkflowEngine`. Creation requires
  the linked Task Graph to exist in the exact authority boundary. Successful
  completion requires every linked Task to be durably completed; failures and
  cancellations remain explicit Workflow decisions and do not silently mutate
  Task state.
- Advance the client protocol from 26 to 27. A host advertises Workflow
  capabilities only when it installs a `WorkflowEngine`. Protocol commands
  create, read, page transitions, and apply typed commands. Signal delivery and
  timer waking have permissions distinct from ordinary Run mutation.
- The reference service stores Runs in `workflows.db` and uses the same fixed
  authority and Task Coordinator as all other service capabilities.

## Bounds and recovery

- One Run admits ordinary work through 4,096 transitions and 16 MiB of encoded
  state. Its recovery/settlement-only hard limits are 4,098 transitions and
  17,055,744 encoded bytes.
- One command is limited to 128 KiB; summaries, failure reasons, and
  cancellation reasons are limited to 64 KiB.
- Protocol transition pages contain 1–64 records and at most 4 MiB.
- A new command requires the exact current revision. An exact duplicate is
  recognized before revision comparison, enabling recovery after an uncertain
  response without replaying a transition.
- There is no migration from an older Workflow database because schema 1 is the
  first store. Unknown or partial stores are not initialized in place.

## Authority boundaries and non-claims

Workflow signals are content-free routing evidence: identity, name, source,
and idempotency key. Business facts remain in governed Connector results and
systems of record; a signal does not become business authority merely by
waking a Run.

This slice does not add a background timer poller, distributed clock,
multi-node consensus, automatic Task retry, arbitrary signal payload storage,
Human Handoff, or a second compensation executor. A host or later durable
timer service calls `wake_due`; safe effect retry still depends on the
declared idempotency contract. Existing `CompensationTool` remains the
authorized Tool-effect reversal mechanism. Human ownership transfer remains a
separate next state machine.

## Rejected alternatives

- Extend `TaskStatus` with signals and timers: this mixes executable work with
  process waits and invalidates existing ready/terminal invariants.
- Store timers only in memory: restart loses the exact wait identity and may
  deliver a stale wake to newer state.
- Treat every retryable error as an automatic retry: the Engine cannot infer
  whether an external effect occurred.
- Accept client-authored application timestamps or actors: transport and host
  authority, not JSON strings, own attribution and the settlement clock.
- Add signal payloads to Core: arbitrary business data would bypass the
  Connector authority and retention boundaries.
