# ADR 0056: Explicit State-backed Tool compensation

- Status: Accepted
- Date: 2026-07-25

## Context

Verification can show that a candidate is incomplete or incorrect, but it
cannot safely infer how to reverse an arbitrary external side effect.
Cancellation is equally ambiguous: a provider may have committed an effect
before the Runtime observed cancellation or failure. Treating either event as
automatic rollback would create a second, potentially unauthorized side effect.

Compensation also has an uncertain-settlement problem. If a provider completes
but its response is lost, retrying with a new identity can repeat the reversal.
Trusting the Model to repeat the original Tool input or output would let
untrusted proposal data redefine the effect being reversed.

## Decision

- Model compensation as an ordinary, separately registered Tool adapter around
  a Tool-specific `ToolCompensator`.
- Declare exactly one target Tool in frozen, validated compensation metadata.
- Resolve the original call input and successful result from authoritative
  same-Thread State; do not accept copies in the compensation request.
- Require unambiguous, ordered authorization evidence for both the original
  effect and current compensation call. Policy `ask` additionally requires the
  matching recorded approval request and approval settlement.
- Require a stable provider-scoped idempotency key for one target Turn and call.
- Return a prior successful compensation result from State without another
  provider invocation.
- Permit retry after an authorized absent or failed settlement only with the
  same idempotency key. Reject a different key for the same target.
- Reject failed original Tool results as generic compensation targets.
- Reject repeated Model Tool-call IDs within one Turn before another Tool
  resolution, authorization, or execution.
- Do not change State or client-protocol schemas: compensation uses existing
  Tool call, Policy, approval, and Tool-result Items.

## Consequences

Compensation has the same Policy, approval, cancellation, output-bound,
observability, and durable-evidence path as every other Tool. Hosts retain
control over which effects are reversible and must implement provider-side
idempotency. Verification and cancellation remain observational rather than
silently gaining mutation authority.

The engine can prevent a known duplicate after a successful durable settlement
and can fence retries to one key, but it cannot prove that an arbitrary external
provider honors idempotency. A provider error or missing result remains an
uncertain outcome and must not be described as a completed rollback.

## Rejected alternatives

- Automatically compensate every Tool after failed Verification: no generic
  inverse exists, and Verification has no mutation authority.
- Run compensators outside `ToolRegistry`: that would bypass ordinary Policy,
  approval, State ordering, and observability.
- Accept original input/output in the Model request: proposal data is not
  authoritative evidence.
- Generate a new key for each retry: an uncertain provider settlement could be
  applied twice.
- Add dedicated compensation State events now: existing Tool evidence expresses
  the contract without a schema migration or parallel execution path.
