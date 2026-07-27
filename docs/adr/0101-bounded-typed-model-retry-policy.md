# ADR 0101: Bounded typed Model retry policy

- Status: accepted
- Date: 2026-07-28

## Context

ADR 0100 made remote Provider failures structured evidence without turning
that evidence into an automatic recovery command. A retry policy still needs
to answer independently:

- which facts are transient enough to retry;
- whether the same Model or the whole Turn is replayed;
- how retries interact with Route failover, provisional streaming, Provider
  Continuation, cancellation, and deadlines;
- whether a Provider delay may exceed operator limits; and
- which attempts remain observable without adding conversation authority.

Source-pinned reference review found useful but non-identical mechanisms:
Codex preserves typed retryable API failures and retry delays, OpenCode
normalizes status and retryability, Pi performs abortable Provider retries,
and Hermes uses a broad capped-jitter recovery taxonomy. Their product- and
Provider-specific classifications are not themselves a generic Harness
contract.

## Decision

- Keep same-Model retry disabled by default. Hosts opt in through
  `ModelRetryPolicy` or service `model_route.retry`.
- A policy permits 1–8 additional calls after the initial call. Its initial
  and maximum fallback delays are each 1–60,000 milliseconds, and the initial
  delay cannot exceed the maximum. The service defaults those two values to
  250 and 5,000 milliseconds only when a `retry` object is present.
- Retry only a validated `ModelProviderFailure` classified as rate limited,
  overloaded, server, or transport. Authentication, authorization, quota
  exhaustion, request rejection, model unavailability, content policy,
  protocol, legacy `Model(String)`, Runtime timeout, and every other error are
  not retried by this policy.
- Scope retry to one Model call in the current Agent step. Do not replay a
  Turn, a completed Tool effect, verification, or State settlement.
- Create the candidate attempt deadline once and share it across the initial
  call, retry waits, and additional calls. The total Turn deadline remains
  authoritative. A retry delay that cannot finish before the applicable
  deadline yields to the next Route candidate, or returns the current failure
  when no candidate remains. Reaching a candidate budget while waiting does
  not open ADR 0099's timeout cooldown because no Provider call timed out.
- Wait cooperatively and make cancellation interrupt the backoff before a new
  Provider call starts. Every call gets a fresh Model-stream cancellation
  scope.
- Honor a Provider retry delay exactly when it is within the policy maximum
  and the remaining deadline. Do not silently shorten an excessive Provider
  delay. Without a Provider hint, use bounded equal-jitter exponential
  backoff: retry `n` has a capped ceiling of
  `initial_delay × 2^(n-1)` and waits between half that ceiling and the full
  ceiling. Derive the jitter deterministically from Thread, Turn, Model, and
  retry identity so tests and local evidence are reproducible while distinct
  operations are dispersed.
- Retry only when the failed call delivered zero provisional stream events.
  Once content was delivered, suppress both retry and Route failover under the
  existing streaming safety rule.
- Preserve exact Route order and Provider Continuation affinity. Exhausting or
  declining same-Model retries resumes ordinary pre-output failover; a retry
  success records that exact Model as the settled provider.
- Emit one content-free `PhaseObservation` for every invoked call. A
  zero-based `model_retry_index` is `0` for the initial call and increases for
  same-Model retries. Route candidates skipped by cooldown have no retry
  index. Failure class, status, and retry hint retain ADR 0100's privacy
  boundary.
- Do not change State, Protocol, snapshot, Evaluation, or Model Gateway
  coordinates. The optional service configuration field is additive under
  configuration schema 1, and the observation field is best-effort
  non-authoritative evidence.

## Consequences

Transient, structured Provider failures can recover before switching Models
without parsing diagnostics or weakening the exact Route. Retries cannot
duplicate Y-Harness Tool effects because they happen only before a Model
output is accepted in the current step.

Each retry can still incur Provider cost, repeat opaque Provider-side work,
and increase latency. A host must leave the policy disabled for a
`LanguageModel` implementation whose call hides non-idempotent side effects.
The policy is process-local and does not coordinate retry pressure across
Runtime replicas.

The Runtime clones the bounded Model request only when retry is enabled.
Attempts are traceable but are not durable conversation items; recovery after
a process crash still follows the existing exclusive Turn-recovery contract.

## Rejected alternatives

- Retry every 429-like diagnostic string: localized text and extension-owned
  messages are not routing authority.
- Retry the complete Turn: previously completed Tool effects could be
  duplicated.
- Give every retry a new full attempt timeout: retry count would multiply the
  configured Route latency bound.
- Retry or fail over after provisional output: clients cannot safely retract
  an already delivered fragment.
- Clamp an excessive `Retry-After` to the policy maximum: this violates the
  Provider's explicit back-pressure instruction.
- Retry authentication, quota, content-policy, request, model-availability, or
  protocol failures: the typed fact does not prove that immediate repetition
  can succeed.
- Add a general circuit breaker in the same decision: cross-Turn health,
  distributed ownership, and probing require a separate contract.

## Evidence

- `runtime::tests::retryable_provider_failure_retries_same_model_with_trace_indices`
- `runtime::tests::model_retry_exhaustion_is_exact_and_returns_typed_failure`
- `runtime::tests::non_retryable_provider_failure_falls_through_without_same_model_retry`
- `runtime::tests::retry_backoff_that_cannot_fit_candidate_budget_yields_without_cooldown`
- `runtime::tests::provider_retry_hint_above_policy_max_is_not_shortened`
- `runtime::tests::provisional_output_suppresses_typed_retry_and_route_failover`
- `runtime::tests::cancellation_interrupts_retry_backoff_before_another_provider_call`
- `reference_cli::service::tests::model_catalog_validation_precedes_provider_construction`
- all-feature and zero-default workspace test gates

## Related decisions

- [ADR 0035: explicit exclusive Turn recovery](0035-explicit-exclusive-turn-recovery.md)
- [ADR 0070: explicit bounded Model failover](0070-explicit-bounded-model-failover.md)
- [ADR 0077: origin-bound Provider Continuation](0077-origin-bound-provider-continuation.md)
- [ADR 0099: observable Model attempt-timeout cooldown](0099-observable-model-attempt-timeout-cooldown.md)
- [ADR 0100: typed Model Provider failure evidence](0100-typed-model-provider-failure-evidence.md)
