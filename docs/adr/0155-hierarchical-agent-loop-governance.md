# ADR 0155: hierarchical Agent Loop governance

- Status: accepted
- Date: 2026-08-03

## Context

An Agent Loop is often described as a `while` loop around a Model call. That
description omits the properties that matter in a production Harness: exact
execution authority, durable suspension, bounded recovery, completion proof,
and evidence explaining why execution continued or stopped.

The eleven principles reviewed for this decision are useful design input, not
competitor benchmark evidence or proof of an implementation. Y-Harness adopts
their sound boundaries while retaining deterministic authority in the Runtime
and avoiding one open-ended callback bus or one flat, ever-growing state enum.

## Decision

### 1. The Loop is a governed controller inside the Runtime

The Runtime owns Sessions/Threads, Context, Models, Tools, MCP, Skills, Memory,
Policy, State, Verification, Observability, and cancellation. The Agent Loop is
the controller that repeatedly obtains a Model proposal, validates it, executes
an authorized action, records the real observation, and decides whether another
Model step is allowed.

The target control hierarchy is:

```text
Request admission
  ├─ reject through deterministic Policy
  ├─ direct text Turn with an empty Tool view
  └─ execute
       ├─ Prepare
       ├─ Decide (Model proposal)
       ├─ Authorize
       ├─ Execute
       ├─ Observe
       ├─ Verify / Repair / Compact
       ├─ Waiting (approval, human input, external signal)
       └─ Terminal (completed, failed, cancelled, timed out, interrupted)
```

Admission may optimize whether a Tool-capable Loop is needed, but it cannot
grant authority. A Model-based coordinator may propose `direct` or `execute`;
only deterministic Policy may forbid an action. A direct reply is an ordinary
Turn with a text-only Capability View, not a bypass around State, budgets,
verification, or observability.

### 2. Model output is a proposal, never an effect

Only Runtime adapters may access files, processes, networks, MCP servers,
credentials, or external systems. Structured Model Tool calls pass the frozen
Capability View, Registry resolution, Policy, Approval, Authority, cancellation,
and executor boundaries before any effect. Tool results are durable observations
fed into a later Model step; the Runtime never fabricates success on behalf of a
Tool.

### 3. Durable state records recovery facts, not every transient phase

`Prepare`, `Decide`, and `Authorize` are observable execution phases. They do
not automatically become durable top-level Turn statuses. Durable state is
reserved for facts required to recover or coordinate: ordered decisions and
observations, approval/human requests, resumable fingerprints, externally
visible effects, terminal settlement, and generation identity.

The long-term lifecycle is hierarchical: `Active`, `Waiting`, and `Terminal`
are durable classes; fine-grained phases remain typed evidence underneath them.
This avoids a flat enum that requires a schema migration for every internal
implementation step.

### 4. Retry requires a justified state transition

A retry is legal only when the Runtime can name the new information or changed
condition: elapsed backoff, a new Provider candidate, a user decision, compacted
Context, repaired syntax, reconciled external state, or a changed capability
generation. Repeating an equivalent failing action is loss of progress, not
recovery.

The first Progress Governor slice compares bounded exact fingerprints of
structured Tool proposals and observations, detects short repeated
failure-bearing cycles, and settles at a pre-Model or terminal-budget safe
boundary. It never
inspects or persists hidden chain-of-thought. Trusted semantic failure classes,
advisories, strategy changes, and Turn-local capability quarantine remain later
extensions; see ADR 0156.

### 5. Completion is verified settlement

A Model message is a proposed answer, not proof that the task is complete.
Completion requires a bounded receipt that binds the candidate answer to the
Turn, required Turn-internal verifier outcomes, and the active execution
generation. Retryable verification failure re-enters decision-making;
non-retryable failure settles explicitly. `Completed` is written only through
the atomic receipt-bearing transition specified by ADR 0157.

Format 1 does not prove cross-aggregate Artifact, Effect, or business-delivery
requirements. Those requirements must be explicitly outside the contract,
keep the Turn non-terminal, or fail closed until a later authority-fenced proof
contract exists. Channel delivery and derived post-terminal work are separate
aggregates and cannot retroactively change the receipt.

### 6. Extensions are typed and constrained

Y-Harness will not expose an unrestricted Hook that can mutate arbitrary Loop
state. Stable extension needs are represented by small versioned contracts for
specific phases such as pre-authorization guard, post-observation enrichment,
pre-compaction, completion verification, audit export, and terminal cleanup.
Each contract declares ordering, timeout, payload bounds, failure mode,
authority, and evidence rules. Business Workflow remains outside the generic
Loop.

## Adoption matrix

| Principle | Current evidence | Decision and required enhancement | Priority |
|---|---|---|---|
| 1. Model decides; Runtime executes | `LanguageModel`, `ToolRegistry`, `PolicyEngine`, `ApprovalHandler`, Tool invocation, and format-1 CompletionReceipt construction are separated in `src/kernel`, `src/runtime`, and `src/completion.rs` | Retain. Format 1 binds measured Model-route/Tool-view/Verifier/Runtime-governance coordinates and an optional execution binding; complete all remaining capability-generation inputs without giving Provider adapters ambient execution authority | P1 |
| 2. Decisions follow real Tool results | ordered `ToolCall`, `PolicyDecision`, `ToolResult`, and later Model steps are persisted by `HarnessRuntime` | Retain. Extract the controller from the composition root without changing event ordering | P1 |
| 3. Progressive capability disclosure | ADR 0154 adds a frozen exact Tool Capability View and view-bound execution fence | Extend with intent-aware, per-step projection, Tool/Skill search, on-demand MCP schema acquisition, cache expiry, and one complete `CapabilityGeneration` digest | P1 |
| 4. Runtime contains the Loop | `HarnessRuntime` composes Context, Model route, Tool/Policy, State, approvals, verification, and observability | Make `AgentLoop` an internal controller with narrow ports; keep host composition and transport outside it | P1 |
| 5. Gateways isolate implementations | versioned capability traits and registries isolate Models, Tools, Memory, MCP, compaction, verification, and State | Retain. Bind every selected adapter/origin/configuration to the execution generation and conformance tests | P1 |
| 6. Safety is deterministic and layered | Capability View, Registry origin, Policy, durable Approval, authority, cancellation/deadline, sandboxed adapters, Effect governance, and State-revalidated Turn completion receipts are code-enforced | Retain. Add typed phase guards, per-tenant quotas, and later cross-aggregate receipt verification; Prompt rules remain advisory only | P1 |
| 7. Parallelism is only for independent calls | only `ToolBatchExecution::ParallelSafe` calls may overlap; limits, cancellation, ordered settlement, and sequential fences are tested | Retain fail-closed declaration. Future dependency graphs must be explicit; never infer write independence from Tool names or Model prose | P0 |
| 8. Context is budgeted | bounded prompts/results, Conversation Context, optional compactor, Memory Context, recent history, and durable summary provenance exist | Add adaptive in-Turn pressure stages, evidence-aware Tool-result reduction, cache-aware capability projection, and compaction quality gates | P1 |
| 9. Lifecycle extension points | no arbitrary Runtime Hook bus currently exists | Add only typed, versioned, bounded interceptors with deterministic ordering and auditable failure semantics | P1 |
| 10. Failure is recoverable, not blindly retried | typed Provider failure, bounded retry/backoff/failover, deadlines, cancellation, panic isolation, interrupted-Turn recovery, durable approval resume, Effect reconciliation, and ADR 0156's exact failure-cycle Progress Governor exist | Add trusted failure dispositions/advisories, partial structured-output repair, Turn-local capability quarantine, and stronger unknown-effect receipt verification | P0/P1 |
| 11. Human input is a normal state | approval requests and decisions are durable and approval continuation survives worker loss | Generalize to durable `WaitingForApproval`, `WaitingForHumanInput`, and `WaitingForExternalSignal`; suspend active execution deadlines while waiting and resume by exact authority/generation token | P0 |

## Enhancements beyond the eleven principles

1. **Execution generation:** bind Model route, Tool View and origins, Policy,
   Skills, MCP schemas, Memory provider, verifier set, and relevant configuration
   into one immutable digest used by resume, audit, and evaluation.
2. **Effect truth:** distinguish `not_started`, `applied`, `rejected`, and
   `unknown`; never retry an externally visible mutation until idempotency or
   reconciliation proves it safe.
3. **Completion receipt:** format 1 makes the answer and Turn-internal verifier
   evidence independently inspectable; later versions need authority-fenced
   cross-aggregate references before they may claim Artifact, Effect, or
   business-delivery truth.
4. **Progress evidence:** detect semantic stasis without storing private
   reasoning, and require every recovery transition to identify its novelty.
5. **Enterprise waiting:** tenant-fenced ownership, expiry, reassignment,
   escalation, channel delivery, and restart-safe resume for all human/external
   waits.
6. **Evaluation coupling:** add regression cases for loop termination,
   unsupported claims, repeated failure, unsafe concurrency, recovery, Context
   pressure, and completion truth—not only final-answer quality.

## Implementation order

1. Frozen Tool Capability View and execution fence — implemented by ADR 0154.
2. Bounded exact Progress Governor — implemented by ADR 0156.
3. Generation-bound CompletionReceipt — implemented by ADR 0157. General
   durable Waiting envelope and exact resume tokens remain pending.
4. Internal `AgentLoop` extraction plus typed lifecycle interceptors.
5. Dynamic Capability Projector and complete execution-generation digest.
6. Adaptive Context pressure governance and corresponding evaluations.

## Non-claims

- The current `TurnStatus::Running` plus durable Approval Inbox is not yet the
  generalized Waiting state described here.
- Format-1 completion is portable and generation-bound only for evidence inside
  the owning Turn. It does not yet prove cross-aggregate Artifact, Effect,
  business delivery, channel acknowledgement, or post-terminal derived work.
- Current Tool disclosure is frozen for one Runtime generation, not yet
  per-intent or per-step discovery.
- Y-Harness does not currently have a generic Coordinator or Hook subsystem;
  this ADR defines their safe boundaries rather than claiming them complete.
- This ADR records adopted engineering principles. It is not evidence that
  Y-Harness or the referenced design input outperforms another product.

The implemented completion boundary and its exact non-claims are specified in
[ADR 0157](0157-generation-bound-completion-receipt.md).
