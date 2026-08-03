# ADR 0158: durable Agent Loop waiting and exact resume

- Status: accepted; Phase 1 and the bounded Approval release/control-plane
  subset of Phase 2 are implemented
- Date: 2026-08-03

## Context

The Runtime historically waited for a pre-Tool Approval by retaining a worker
that polled the Approval handler while the authoritative Turn remained
`Running`. ADR 0065 permitted an explicitly owned restart at one fingerprinted
pre-effect boundary, but did not define a general suspension contract.

State/snapshot schema 16 and Protocol 37 now implement the first durable
release slice: an opted-in Turn may release its worker at one non-batch Tool
call whose Policy result is `ask`. State journals the complete wait envelope,
Approval settlement, resume, claim, cancellation/timeout closure, or denial on
the owning Turn stream. Protocol and the TUI expose bounded discovery, exact
resume, and exact cancellation. The legacy blocking path remains for Turns
that omit the durable-wait TTL and for multi-Tool batches.

A durable wait must release the worker, survive process loss, preserve the
request's authority and execution generation, stop charging active execution
time, and reject stale or duplicated answers. It must also share the Turn's
terminal race boundary: a cancellation, timeout, resume, worker claim, and
completion cannot each believe that it won the same revision.

Adding `Waiting` directly to `TurnStatus` would mix two different facts. Turn
status answers whether the Turn is still live or how it terminated. Execution
coordination answers whether live work is executing, awaiting evidence, ready
for a worker, or blocked on an unknown effect. Those facts evolve at different
rates and need different fencing identities.

The durable coordinates introduced here are State/snapshot schema 16, Thread
archive format 6, and Protocol 37. ADR 0159 subsequently adds independent wait
projection schema 1 and Temporal Driver API 3 without changing those journal
or wire coordinates. This ADR records both the implemented Approval slice and
the still-unimplemented general lifecycle; target-only states or guarantees
are called out explicitly below.

## Decision

### 1. Keep Turn outcome separate from execution coordination

`TurnStatus` retains its current `Running` plus terminal states. It does not
gain a `Waiting` variant. The target one-per-Turn `AgentLoopExecution`
projection separates execution coordination from Turn outcome:

| Execution state | Meaning |
|---|---|
| `Executing` | one authorized Runtime worker may advance the Agent Loop |
| `Waiting` | a durable `WaitEnvelope` exists; no worker is retained |
| `Ready` | exact resume evidence was accepted and work awaits a claim |
| `NeedsReconciliation` | an external effect may have started and generic replay is forbidden |
| `Closed` | the owning Turn is terminal and can never be resumed or claimed |

The schema-16 implementation serializes `Waiting`, `Ready`, and `Executing`.
It creates the projection at `WaitStarted`; ordinary execution before that
boundary has no separate coordination record. A terminal transition removes
the live projection in the same State CAS. `NeedsReconciliation`, an explicit
serialized `Closed` state, and pre-wait worker ownership are target work, not
current wire shapes.

`AgentLoopExecution` is not a new database or eventually consistent side
aggregate. Its events live in the same authoritative State journal as the Turn
and use the same stream compare-and-swap. The projection may have a logical
execution revision, but State advances that revision and the journal head in
one transaction.

The implemented Approval lifecycle is:

```text
no wait   --WaitStarted----------> Waiting
Waiting   --WaitAccepted---------> Ready
Ready     --ExecutionClaimed-----> Executing
Waiting/Ready --cancel or observed expiry--> terminal Turn
Waiting/Ready --Approval denial------------> failed Turn
Executing --ToolResult / further Loop work-> terminal or next decision
```

The target superset adds
`Executing --EffectOutcomeUnknown--> NeedsReconciliation`, authoritative
reconciliation, and an explicit closed/lease-fenced ownership model.

A finite cross-worker lease and renew/expiry protocol will later fence an
`ExecutionClaimed` ownership epoch. Until that phase lands, a claim is only a
same-State-CAS single-host coordination fact and does not authorize remote
takeover.

### 2. Every lifecycle choice shares one revision fence

`WaitStarted`, `WaitAccepted`, `ExecutionClaimed`, and every Turn-terminal
transition require the exact current execution revision and State stream head.
They are mutually exclusive choices at a given source revision: one committed
choice advances the revision, and all competitors must reload the complete
projection. They may of course occur sequentially at different revisions in
one valid lifecycle.

In particular:

- a wait cannot start after cancellation or terminal completion wins;
- resume acceptance cannot win after wait expiry, cancellation, or another
  accepted response advances the waiting revision;
- two workers cannot claim the same `Ready` revision; and
- a stale worker cannot complete a Turn after a wait, resume, reconciliation,
  or newer claim changed the execution revision.

Successful closure remains the atomic `TurnCompleted` boundary from ADR 0157.
Schema-16 execution-transition Items are part of the ordered Turn evidence
validated before completion, but format-1 `CompletionReceipt` was not silently
reinterpreted to add dedicated final-revision or claim-fence fields. A stronger
receipt that names those coordinates requires a new receipt format and remains
future work.

### 3. Revision, wait, command, and claim identities are different

| Coordinate | Stability and purpose |
|---|---|
| `revision` | monotonic version of one `AgentLoopExecution`; it is the optimistic concurrency fence and changes on each lifecycle transition |
| `wait_id` | stable identity of one wait episode; it survives reads, delivery retries, and exact response replay, but never authorizes resume by itself |
| `command_id` | stable idempotency identity of one transition; Runtime deterministically derives Approval resume/denial identities from exact evidence, while Protocol cancellation requires a caller-stable ID; changed reuse fails closed |
| `claim_id` | identity and fencing token of one worker ownership epoch; every re-claim uses a new value, and a stale value cannot append execution or terminal evidence |

An Event Store stream head may also advance for ordinary Turn evidence. It is
not a substitute for the execution revision: State validates both the complete
Turn projection and the expected execution lifecycle before committing a
transition.

### 4. A wait is a complete, authority-bound envelope

The versioned, bounded `WaitEnvelope` contains at least:

```text
wait_id
revision
kind
thread_id + turn_id + tenant_id
original_authority
started_at_server + expires_at_server
remaining_active_budget
execution_generation_digest
```

`kind` is a closed, versioned capability kind, not free-form Model text. The
first enabled kinds are `Approval` and then `HumanInput`; later external-signal
kinds require their own responder-evidence validators.

Thread, Turn, and tenant identities must match authoritative State. The
original trusted Authority Context is frozen in the envelope and remains the
authority of the resumed execution. A responder proves permission to answer a
particular wait; the responder does not replace or acquire the original
caller's execution authority.

`started_at_server` and `expires_at_server` come from trusted server time, never
caller JSON. The expiry is a wall-clock liveness boundary for the wait. The
execution-generation digest covers the complete immutable resume manifest,
including the selected Model route and origins, disclosed Tool/Skill/MCP
capability view, Policy and wait-kind contract, Context/Memory/compaction
coordinates, Verifier manifest, progress and budget policy, trusted authority,
and applicable execution binding. A partial approval-only fingerprint is not
sufficient. Digest equality proves equality with that manifest; it is not
binary, deployment, or remote-service attestation.

Schema 16 implements a self-digested `TurnWaitEnvelope` containing the exact
Approval request, Model-request SHA-256, trusted requester/tenant, server
start/expiry, remaining active timeout, and frozen `CompletionGeneration`.
Resume reconstructs and compares the original Model request. The caller must
still repeat the original `memory_scope` and non-authoritative `context` over
Protocol 37; a self-contained frozen Context capsule that removes that caller
replay dependency has not landed.

### 5. Waiting stops active-budget charging

Immediately before `WaitStarted`, Runtime deducts only active execution already
consumed and stores the exact remainder. Model-step, Model-attempt, Tool-call,
token/cost, and other discrete counters are also preserved without receiving a
new allowance. Active execution time is charged only in `Executing`; it is
frozen in `Waiting`, `Ready`, and `NeedsReconciliation`.

The wall-clock wait expiry still advances while active time is frozen. At the
exact server-clock expiry boundary, a resume cannot be accepted. The current
resume path observes expiry and settles the Turn as `TimedOut` through the
same State CAS. ADR 0159 adds an independent schema-1 due projection; an
embedding host or explicitly configured reference-service Temporal loop can
discover and close an unattended expired wait with the same exact stream CAS.
Core still starts no timer or sweeper of its own. A restart
reconstructs the stored remainder; it does not subtract process downtime or
time spent waiting. On a valid claim, Runtime derives a new in-process active
deadline from server time plus that exact remainder.

This separation prevents a human or external system from consuming execution
time while also preventing an unbounded wait with no durable expiry.

### 6. Resume is an evidence transition, not stack restoration

A bounded `ResumeEvidence` contains at least:

```text
command_id
digest
wait_id
source_revision
responder_evidence
```

The digest canonically binds the complete typed command, wait identity,
source revision, responder evidence, and trusted responder attribution.
Responder evidence is kind-specific: for Approval it binds the immutable
Approval request, decision, Inbox revision, and deciding authority; for
HumanInput it binds the declared capability, bounded answer Item or content
digest, and answering authority. Later signal kinds must name the authoritative
source and source revision or receipt.

State accepts resume only after it reloads and validates all of the following:

1. the exact accessible Thread, running Turn, tenant, and current `Waiting`
   projection;
2. matching `wait_id` and `source_revision` before the server-clock expiry;
3. exact command replay or a fresh `command_id` with a non-conflicting digest;
4. kind-specific responder permission and evidence;
5. unchanged original Authority Context; and
6. an exact recomputation of the full execution-generation digest.

Acceptance appends `WaitAccepted` and moves the execution to `Ready`. It does
not run a Model or Tool inside the resume transaction. A worker later reloads
the journal, obtains an exact claim, reconstructs Context from durable evidence,
and resumes with the stored active budget. No Rust Future, closure, provider
continuation held only in memory, or serialized call stack is treated as
recovery authority.

Generation or authority drift leaves the wait unchanged and returns a typed
failure. The caller may retry after restoring the exact generation or cancel
the Turn; it may not update the envelope in place to make stale evidence fit.

### 7. Approval is the first non-blocking wait kind

The implemented Runtime integration replaces Approval-handler polling with
durable suspension for one non-batch Tool call when the Turn opts in with a
finite wait lifetime:

```text
persist ToolCall + Policy Ask + ApprovalRequested + WaitStarted in State
  -> idempotently submit the immutable request to Approval Inbox
  -> release the worker
  -> accept an Inbox decision as typed ResumeEvidence
  -> move Waiting to Ready through State CAS
  -> claim, rehydrate, and continue before Tool execution
```

The pre-Tool boundary proves that the governed Tool effect has not started.
Approval denial is copied into State and fails the Turn in one terminal CAS;
it never accepts-then-claims a worker and never fabricates a successful Tool
result. Approval Inbox submission/read/orphan cleanup is independently bounded,
and State commits precede best-effort cleanup.

This release path is deliberately not used for a same-response multi-Tool
batch. Those requests retain the legacy blocking Approval path because partial
batch release would make dependency and effect ordering ambiguous.

The target `HumanInput` design is a separate explicit capability kind in the
frozen Capability View. It is not inferred from assistant prose, represented
as Approval, or treated as a generic Tool effect. The first version accepts exactly one
`HumanInput` proposal in a Model decision and rejects a same-response batch
that mixes it with Tool calls, Approval waits, another HumanInput request, or
another wait kind. This avoids partially executing a batch whose remaining
members depend on an answer that does not yet exist. It is not implemented in
schema 16 or Protocol 37.

### 8. State and Approval repair is idempotent, not atomic

State and Approval Inbox are separate transaction domains. The design does not
claim a transaction, distributed lock, or exactly-once write spanning both.
State is authoritative for whether the Turn may resume; the Inbox is
authoritative for the Approval request and decision. Stable IDs and digests
make the following crash gaps repairable:

| Observed gap | Idempotent repair |
|---|---|
| current State Approval wait, Inbox request absent | submit the exact immutable request |
| Inbox request exists, but State has no matching current wait | orphan or close the Inbox request; never create a wait from the Inbox alone |
| Inbox decision is terminal, State is still at its matching wait revision | build the same ResumeEvidence and attempt `WaitAccepted` |
| State is terminal while Inbox is pending or settled | orphan/close where supported and retain the unused decision as audit evidence; never reopen the Turn |

Repair always reloads both authorities and may be repeated after another
crash. A State terminal transition racing an Inbox settlement determines Turn
truth through State CAS; it cannot roll back the independently committed Inbox
decision. Operational metrics must expose unrepaired pairs and retry age.

The current Runtime performs bounded idempotent submit/read repair when a wait
is created or resumed and performs bounded best-effort orphan cleanup after
State closure. It does not yet persist an Inbox-repair outbox, create an
authoritative orphan tombstone before late submission, scan unresolved pairs,
or expose retry-age operations. A cleanup failure therefore remains an
operator-visible gap rather than a durable queued repair job.

### 9. Target: unknown effects never use the generic resume path

An `ApprovalDecision` without a proved `ToolResult`, a worker loss after effect
dispatch, or any other boundary where an external mutation may have started is
not `Waiting`. It enters `NeedsReconciliation` with the exact effect command,
generation, claim, and known transition evidence.

Generic `ResumeEvidence`, a repeated approval, worker lease expiry, or a human
instruction cannot authorize replay. Only an authoritative capability-specific
status or Effect Ledger reconciliation may establish `not_started`, `applied`,
or `rejected`. `not_started` may make the execution `Ready` for a newly fenced
attempt; `applied` records the real observation and continues without replay;
`rejected` follows its explicit failure policy. `unknown` remains
`NeedsReconciliation` or settles explicitly without re-execution.

Schema 16 prevents blind generic replay once an execution is `Executing`, but
it does not serialize `NeedsReconciliation` or provide a capability-specific
effect-status transition. Crash ambiguity after claim/effect dispatch therefore
remains a production-hardening gap; callers must not infer `not_started` from
the absence of a ToolResult.

## Required ordering

```text
load exact Turn + AgentLoopExecution projection
  -> validate current revision, authority, generation, budget, and server time
  -> commit exactly one wait / accept / claim / terminal State transition
  -> perform or repair any cross-domain Inbox operation idempotently
  -> only an exact current claim may issue the next Model or Tool operation
  -> bind the final execution-transition evidence into terminal completion
```

## Phased implementation and migration

### Phase 0: design acceptance — complete

The separation between Turn outcome and execution coordination, one-State-CAS
race boundary, frozen original authority, server clock, and pre-effect
Approval boundary is accepted and now implemented for the bounded slice below.

### Phase 1: same-journal execution foundation — implemented

- State/snapshot schema 16 implements the bounded `AgentLoopExecution`
  reducer, transition evidence, Memory/SQLite parity, and same-stream CAS.
- Waiting/Ready closure, denial, ordinary terminal settlement, and successful
  completion cannot leave a live execution projection on a terminal Turn.
- Thread archive format 6 preserves schema-16 wait evidence.
- The backup-first schema-1-through-schema-15 to schema-16 migration preserves
  immutable event JSON, discards disposable old snapshots, and never invents
  wait evidence for a legacy Running Turn.
- Exact Client Protocol 37 advertises the new coordinates and adds bounded
  wait discovery, resume, cancellation, and Waiting Operation projection. Old
  Protocol clients fail exact negotiation.

The planned dedicated final execution-revision/claim fields were not added to
CompletionReceipt format 1. A widened completion proof requires a new receipt
format and remains open.

Migration advances writer metadata and disposable snapshots; it never rewrites
historical event JSON or invents historical Wait, Resume, Claim, budget, or
generation evidence. A terminal legacy Turn remains terminal evidence with its
existing receipt semantics. A legacy `Running` Turn with no
`AgentLoopExecution` is not inferred to be `Waiting` from its last Items, a
pending Approval record, or process absence. It is ineligible for wait resume
or worker claim and must use the existing explicit exclusive recovery path to
settle `Interrupted`. No migration synthesizes a safe pre-effect boundary.

All old writers must be stopped. Old readers/new writers and mixed schema-15/
schema-16 writers are unsupported. Rollback means restoring the validated
backup before any schema-16 event is written.

### Phase 2: release workers for Approval — implemented subset

Implemented now:

- an opted-in single non-batch Tool `ask` persists State first, performs
  independently bounded Inbox submission/read, and releases the worker;
- exact Approval settlement is copied into State, `Ready` is claimed once by a
  unique process-local worker identity, and the Tool runs only after claim;
- denial atomically fails the Turn without a claim;
- cancellation, resume-observed expiry, and bounded Temporal maintenance close
  `Waiting`/`Ready` through the same stream CAS; accepted denial is projected
  as immediately due and converges to `DenyWait`, never `TimedOut`; and
- Protocol 37 and the TUI expose exact discovery, `/resume`, `/cancelwait`, and
  restart recovery from durable State without exposing Tool input or Model
  Context. They do not reattach a lost process Operation.

Still open in Phase 2:

- durable Inbox repair outbox, orphan tombstone, retry-age projection, and
  repair worker;
- explicit `HumanInput` and its single, non-mixed decision rule;
- worker release for multi-Tool batches;
- a frozen self-contained Context capsule; and
- a durable cross-process resume-result receipt for response-loss
  convergence.

### Phase 3: reconciliation and finite worker leases — future

- Add server-time finite claim leases, renewal, expiry, release, and a new
  `claim_id` for every ownership epoch.
- Fence every post-claim State write, Provider continuation, Tool dispatch, and
  completion transition by the active claim.
- Add a bounded dispatcher/reaper and authenticated worker capability matching
  before claiming distributed or multiprocess takeover.
- Add explicit `NeedsReconciliation` and capability-specific Effect Ledger
  proof before any ambiguous post-dispatch work can become claimable again.

No earlier phase may describe process-local ownership as a distributed lease.

## Required test matrix

This is the acceptance matrix for the full target, not a claim that every row
passes today. Current evidence covers schema-16 projection/validation,
Memory/SQLite replay and migration, wait/resume/claim/close/deny races and
exact retries, active-budget freezing, authority/generation checks, bounded
Inbox calls, single-call Runtime restart, Protocol 37, and TUI recovery.
ADR 0159 now covers bounded indexed expiry discovery and timeout/accepted-denial
convergence. `HumanInput`, durable repair queues, `NeedsReconciliation`, claim
reaping/leases, widened completion receipts, distributed scheduler ownership,
and maximum-cardinality repair scheduling remain future rows.

| Area | Required cases |
|---|---|
| Projection | every legal transition; every illegal source state; one execution per Turn; terminal/Closed parity; bounded decode and projection replay |
| CAS races | wait-start vs terminal; response vs expiry/cancel; duplicate responses; competing claims; stale claim vs wait/reconciliation/completion; only one winner at one source revision |
| Identity/idempotency | exact `command_id` replay; changed command digest rejection; wrong `wait_id`; stale/future revision; new `claim_id` on every re-claim; stale claim rejection |
| Budget and time | active budget deducted before wait; no charge during Waiting/Ready/reconciliation or restart; exact-expiry rejection; resume derives only the stored remainder; no caller-authored clock |
| Authority and generation | cross-Thread/Turn/tenant denial; responder-role denial; original-authority substitution denial; drift of every generation component fails before claim or effect |
| Approval repair | crash after State wait/before Inbox submit; after Inbox submit; after decision/before State accept; terminal races; repeated repair; Memory/SQLite and two-connection parity |
| HumanInput | explicit capability disclosure; one request accepted; prose is not a wait; mixed Tool/wait and multiple-input batches fail before partial execution |
| Unknown effects | loss before dispatch may resume only with `not_started` proof; loss after/around dispatch never generically replays; `applied`, `rejected`, and persistent `unknown` project distinctly |
| Completion | receipt transition-digest and revision tamper; stale claim receipt; completion/resume/terminal race; fork/archive/import retain source proof without claiming target execution |
| Migration/protocol | schema 1-15 backup-first fixtures; legacy Running never inferred as Waiting; no synthetic receipts or budgets; crash at each migration phase; mixed-writer/downgrade refusal; exact Protocol 36/37 mismatch |
| Restart/load | no worker held while Waiting; repeated service restart; bounded wait pages and repair queues; expiry/claim reaping at supported maximum cardinality |

## Consequences

- A live Turn can release compute while preserving exact resume authority and
  remaining execution budget.
- Turn outcome compatibility is not expanded with an ever-growing flat status
  enum; Protocol 37 clients inspect a separate execution projection.
- State CAS gives wait, resume, claim, and terminal transitions one truth
  boundary. Approval Inbox convergence remains explicit and observable rather
  than falsely described as atomic.
- Resume becomes deterministic reconstruction from durable evidence, not a
  promise that arbitrary in-memory provider or Tool stacks can be restored.
- The additional journal evidence, migration, repair work, and future lease
  renewal increase implementation and operational complexity.

## Rejected alternatives

- Add `Waiting`, `WaitingForApproval`, and `WaitingForHuman` to `TurnStatus`:
  this mixes Turn outcome with worker coordination and creates a flat enum for
  every future wait kind.
- Store execution coordination in a separate database: terminal, wait, resume,
  and claim races would cross transaction domains and lose the shared CAS.
- Keep polling the Approval Inbox: a durable record would exist, but the worker
  and active deadline would still be consumed.
- Serialize the async stack or provider object: implementation memory is not a
  portable, bounded, generation-fenced recovery contract.
- Resume by `wait_id` alone or reuse one identity for revisions, waits, and
  claims: stale answers and stale workers would be indistinguishable from the
  current owner.
- Infer a wait from legacy `Running`, final Item shape, or a pending Approval:
  none proves the missing budget, authority, generation, or absence of an
  external effect.
- Treat State plus Approval as one atomic commit: the current stores have no
  shared transaction manager, and saying otherwise would hide real crash gaps.
- Convert every request for user text into Approval or allow a mixed batch:
  authorization and missing task information have different evidence, and
  partial batch execution would be ambiguous.
- Restart an unknown effect after approval, response, or lease expiry: absence
  of a durable result is not proof that the mutation did not occur.
- Charge one absolute execution deadline across human wait time, or omit a wait
  expiry: the former consumes compute budget while no compute runs; the latter
  creates an unbounded unowned lifecycle.

## Non-claims

- State 16 and Protocol 37 implement only the single non-batch Approval slice;
  they do not implement generalized Waiting, batch worker release, or
  `HumanInput`.
- Wait expiry is enforced when observed by resume/close or by the bounded
  schema-1 due projection through Temporal Driver API 3. Core itself starts no
  scheduler; reference-host polling is explicit configuration.
- Inbox calls are bounded and retryable, but there is no durable repair
  outbox, orphan tombstone, pair scanner, or repair-age service level.
- `Executing` rejects blind generic replay, but there is no finite worker
  lease, automatic takeover, or serialized `NeedsReconciliation` lifecycle.
- Protocol resume still receives the original non-authoritative Context inputs
  and has no durable cross-process result receipt; State remains the recovery
  source after a lost process Operation.
- The accepted lifecycle is not exactly-once Model, Tool, Effect, Inbox, or
  channel delivery. It provides fenced State transitions and idempotent repair
  where explicitly specified.
- Phase 1 and Phase 2 do not provide distributed worker ownership, multi-node
  consensus, fleet placement, fairness, or automatic failover.
- Server time is a trusted host/store input, not proof of globally synchronized
  clocks. Multi-node leases require an authoritative clock/lease service.
- `HumanInput` does not replace Workflow signals, Human Handoff case ownership,
  Approval authorization, notifications, or a channel outbox.
- The execution-generation digest proves equality to measured coordinates, not
  implementation integrity or remote-service attestation.
- `NeedsReconciliation` does not make an unknown effect safe to replay, and the
  design does not invent a generic effect-status API where none exists.

This decision refines the planned Waiting hierarchy in
[ADR 0155](0155-hierarchical-agent-loop-governance.md), preserves the
pre-effect resume fence from
[ADR 0065](0065-fingerprinted-pre-tool-approval-resumption.md), and extends the
terminal proof boundary from
[ADR 0157](0157-generation-bound-completion-receipt.md).
