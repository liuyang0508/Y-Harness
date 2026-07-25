# ADR 0008: Verification gates Turn completion

- Status: Accepted
- Date: 2026-07-25

## Decision

An assistant message is a completion candidate, not automatically a completed
Turn. Every registered `Verifier` receives the same immutable candidate
snapshot in deterministic registry-name order.

- all verifiers pass: the Turn may complete;
- a retryable failure: its result is appended to the Turn and the Agent Loop
  receives another model step;
- a non-retryable failure: the result is appended and the Turn fails;
- a verifier error: the Turn fails with the verifier identity retained;
- cancellation or deadline: the Turn settles with the distinct
  `verification` stop phase.

An empty registry adds no completion condition and preserves the default Agent
Loop behavior. Verifier names are validated, collisions are rejected, and
outcome messages are non-empty and bounded to 4096 bytes before entering State.

## Evidence ordering

The candidate assistant Item is recorded before verification. Each verifier
settlement is then recorded before the runtime decides to complete, retry, or
fail:

```text
Assistant candidate → VerificationResult(s)
                    ├─ all passed → completed
                    ├─ retryable failure → next model step
                    └─ hard failure → RuntimeError → failed
```

Every verifier in one pass sees the same snapshot. It does not observe earlier
verifiers' results from that pass, so registry order cannot alter another
verifier's input.

## Rollback boundary

Verification does not claim to roll back Tool side effects. Generic rollback is
unsafe because an external effect may be irreversible, partially applied, or
concurrently observed. Reversal requires a Tool-specific compensation contract,
idempotency identity, pre/postcondition evidence, and Policy authorization.

Until that separate contract exists, a failed verification preserves the
ordered Tool evidence and reports failure honestly.

## Rationale

Verification is a runtime completion condition. Evaluation compares behavior
across datasets and versions and therefore remains a separate layer. Keeping
the contracts separate prevents an offline score or judge from silently
changing live execution.
