# ADR 0156: bounded durable Progress Governor

- Status: accepted
- Date: 2026-08-03

## Context

The Agent Loop already has a hard Model-step ceiling, but a Model can consume
that entire budget by issuing the same failed action under fresh call IDs. A
retry that introduces no new observation is not recovery. Detecting this cannot
be delegated to Prompt wording or hidden model reasoning because termination
authority belongs to deterministic Runtime code.

A naive repeated-hash breaker is also unsafe. Repeated successful reads,
idempotency checks, and polling may be intentional. User steering can arrive
while a Tool is running, so a breaker evaluated immediately after `ToolResult`
could terminate before already-durable new user information is applied.

## Decision

1. `src/runtime/progress.rs` implements a pure incremental reducer over the
   authoritative ordered Turn Items. It retains bounded SHA-256 digests plus
   bounded call/batch correlation metadata while a decision is incomplete,
   never raw Tool payloads, assistant text, private reasoning, timestamps, or
   Provider identifiers. Correlation identities are excluded from the completed
   cycle fingerprint.
2. One progress observation is one completely settled Model Tool decision:
   either a single call/result pair or one ordered same-response batch. Call
   IDs, batch IDs, Item IDs, and timestamps are excluded. The digest binds each
   Tool name and exact JSON input to its exact JSON result and `is_error` flag.
3. The reducer examines exact repeated suffix cycles of period one through four.
   The default stops after five complete repetitions; embedded hosts may select
   a bound from two through sixteen with `ProgressPolicy`.
4. Only a cycle containing at least one failed Tool result is eligible. A fully
   successful cycle clears failure history. A changed proposal or observation
   changes the digest. User input, applied steering, an assistant candidate, or
   verification evidence also starts a new progress epoch. Approval settlement
   does not: authorization is not problem-solving evidence.
5. A stop is a verdict after the complete new Item slice has been consumed, not
   a reducer error. This advances the reducer cursor exactly once and permits a
   later valid external-input boundary to reset the verdict without replaying
   Items.
6. The Runtime evaluates the verdict at each safe boundary before another Model
   request and once more before terminal `MaxSteps` settlement. Under the
   Turn-control lock it first applies every pending durable steering input. If
   the cycle still qualifies, it seals further steering, releases the lock,
   records the existing bounded `RuntimeError`, and settles the Turn as `Failed`
   with `HarnessError::NoProgress`. At the terminal boundary, `Continue` also
   seals steering under the same lock before `MaxSteps` settlement, so no
   accepted input can be stranded between the two decisions.
7. No State Item, State schema, snapshot/archive format, or Protocol shape is
   added. A running Turn can reconstruct the reducer from existing Items after
   worker loss. Protocol 35 continues to expose the ordinary failed Operation
   and existing journal evidence.

## Required ordering

```text
persist complete ToolResult set
  → reach a pre-Model or terminal-step-budget safe boundary
  → lock exact active Turn control
  → apply all pending durable Steering
  → consume new Items exactly once
  → Continue, or seal and settle NoProgress
  → only non-terminal Continue may issue another Model request
  → terminal Continue seals steering and settles MaxSteps
```

Provider retries and route failover happen inside one Model step and produce no
Tool observation until a durable Model decision is accepted. Policy denial,
approval denial, cancellation, timeout, and unknown external-effect settlement
retain their own earlier failure semantics.

## Consequences

- Fresh call IDs cannot hide an exact repeated failure cycle.
- Alternating loops such as `A/B/A/B` are caught without unbounded history.
- A fixed successful call cannot hide a fixed failed neighbor in an otherwise
  identical repeated batch; a fully successful batch is never stopped by this
  governor and remains bounded by ordinary Turn limits.
- Durable steering queued during Tool execution is applied before a possible
  stop and resets the old failure epoch.
- The reducer stores at most `4 × configured repetitions` cycle digests plus
  one bounded incomplete decision.

## Non-claims

- Exact JSON fingerprints are a low-false-positive baseline, not semantic
  equivalence. A Model can change irrelevant parameters, and dynamic timestamps
  or nonces in errors can cause false negatives.
- The Runtime does not guess whether an error means `stable_failure`,
  `awaiting_external`, or `unknown`. A future version may add a trusted typed
  Tool-result disposition and an auditable advisory before the hard stop.
- The governor is not effect idempotency, exactly-once execution, rollback, or
  reconciliation. Externally uncertain mutations still require the Effect
  Ledger and authoritative reconciliation.
- It does not auto-disable Tools, inject reflection prompts, inspect chain of
  thought, or create a cross-Turn circuit breaker.
- `ProgressPolicy` is not yet bound into a complete execution-generation digest;
  an embedding host must keep it stable while resuming a running Turn.
- Long waits should use Workflow/Temporal or an explicitly supported durable
  Waiting boundary. The current Approval-only wait is intentionally narrower
  than a general suspension primitive; neither case should use a tight
  Tool-error polling loop.
