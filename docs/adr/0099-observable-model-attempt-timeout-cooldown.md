# ADR 0099: Observable Model attempt-timeout cooldown

- Status: accepted
- Date: 2026-07-28

## Context

ADR 0070 established an exact ordered Model route with an independent attempt
timeout. A persistently slow primary can therefore consume the full attempt
budget on every Agent step and every new Turn before the same fallback
succeeds.

The current provider boundary reports ordinary failures as
`HarnessError::Model(String)`. That is deliberately sanitized, but it cannot
reliably distinguish authentication, invalid request, rate limiting, protocol,
transport, or permanent failure. Treating every string error as a cross-Turn
health signal would turn an unproven guess into routing authority.

## Decision

- Add an opt-in Runtime Model attempt-timeout cooldown. It is valid only for a
  multi-Model route, from 1 millisecond through 24 hours, and is disabled by
  default.
- Open cooldown only when the Runtime's own attempt deadline wins. Ordinary
  Provider errors, Provider-local strings, caller cancellation, and the total
  Turn deadline do not open it.
- Keep cooldown state process-local, monotonic, bounded by the route's 16 exact
  identities, and non-authoritative. It is neither State history nor a
  cross-process health claim.
- On each Model step, try non-cooling candidates first in their original Route
  order. Cooling candidates remain last-resort fallbacks in their original
  order; the mechanism must never remove the complete configured Route.
- When a non-cooling candidate succeeds, emit one content-free Model
  `ObservationOutcome::Skipped` record for each cooling fallback that was not
  invoked. If every non-cooling candidate fails before provisional output,
  invoke cooling fallbacks and record their ordinary attempt outcomes instead.
  Existing cancellation, total-deadline, and provisional-output suppression
  rules remain authoritative.
- Expired entries re-enter their original position. A successful attempt
  clears its entry.
- Provider Continuation affinity bypasses cooldown selection because the
  recorded Model identity and origin are the only safe consumer of that
  unfinished private state.
- Expose the same opt-in through service
  `model_route.timeout_cooldown_ms`; zero means disabled. Validate the complete
  range and multi-Model requirement before Provider construction, and report
  the exact value through `yh doctor`.
- Do not change Protocol, State, snapshot, or Model Gateway coordinates.
  `Skipped` is additive embedded Observability evidence; no conversation or
  durable authority changes.

## Consequences

Repeated Runtime-proven timeouts no longer impose the same wait before a
successful fallback on every step. The explicit Route remains authoritative,
and cooldown fails open when the non-cooling subset cannot settle the request.

This is intentionally not a general circuit breaker, load balancer, or health
score. Concurrent Turns already in flight are not revoked, cooldown is not
shared across processes or restarts, and multiple probes may occur after
expiry. ADR 0100 supplies a typed Provider failure evidence boundary, and
ADR 0101 uses four transient classes only for bounded same-Model retry. It does
not open this cross-Turn cooldown. General error cooldown still requires a
separate health, ownership, and persistence policy. Price/load routing
requires separate explicit policy and cost contracts.

## Rejected alternatives

- Cool every `Model(String)` failure: sanitized text is not a safe failure
  classifier.
- Permanently remove a timed-out Model: violates the exact configured Route
  and prevents recovery.
- Hard-skip cooling Models even when non-cooling candidates fail: makes the
  availability mechanism reduce availability.
- Persist cooldown in Thread State: operational process health is not
  conversation authority and needs separate distributed ownership semantics.
- Hedge Model calls: multiplies cost and needs a distinct winner, cancellation,
  and streaming arbitration contract.

## Evidence

- `runtime::tests::model_timeout_cooldown_skips_repeated_wait_but_keeps_trace_evidence`
- `runtime::tests::model_timeout_cooldown_fails_open_after_ready_candidates_fail`
- `runtime::tests::model_timeout_cooldown_expires_and_never_removes_the_complete_route`
- `runtime::tests::ordinary_model_failure_does_not_open_timeout_cooldown`
- `reference_cli::service::tests::model_catalog_validation_precedes_provider_construction`
- `doctor_validates_an_explicit_ordered_model_catalog`

## Related decisions

- [ADR 0070: explicit bounded Model failover](0070-explicit-bounded-model-failover.md)
- [ADR 0077: origin-bound Provider Continuation](0077-origin-bound-provider-continuation.md)
- [ADR 0087: explicit configured Model catalog and route](0087-explicit-configured-model-catalog-and-route.md)
- [ADR 0100: typed Model Provider failure evidence](0100-typed-model-provider-failure-evidence.md)
- [ADR 0101: bounded typed Model retry policy](0101-bounded-typed-model-retry-policy.md)
