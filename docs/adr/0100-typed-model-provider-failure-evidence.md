# ADR 0100: Typed Model Provider failure evidence

- Status: accepted
- Date: 2026-07-28

## Context

The Model capability historically returned every provider or model-contract
failure through `HarnessError::Model(String)`. The string is bounded before
durable settlement, but it cannot safely drive routing, retry, cooldown,
operator alerts, or comparative failure analysis.

Source-pinned reference review found the same underlying need with different
trade-offs:

- Codex carries typed API and protocol error variants plus optional retry
  delays.
- OpenCode preserves status, retryability, headers, and provider metadata, with
  string inference only as a compatibility fallback.
- Pi preserves status and headers at its provider boundary, while its outer
  retry layer still demonstrates the ambiguity of string matching.
- Hermes has a broad recovery taxonomy, but much of it is provider- and
  product-specific pattern matching that does not belong in a microkernel.

Y-Harness also uses `Model(String)` for its own request construction, response
validation, and process-adapter contracts. Replacing every occurrence would
mislabel Harness failures as remote-provider facts.

## Decision

- Retain `HarnessError::Model(String)` as the compatible representation for
  Harness-owned model-contract failures and unclassified legacy provider
  errors.
- Add `HarnessError::ModelProvider(ModelProviderFailure)` for adapters that can
  report structured facts. The public failure kinds are authentication,
  authorization, rate limit, quota exhaustion, request rejection, model
  unavailability, content policy, overload, server, transport, and protocol.
  There is no typed `unknown`; unclassified evidence stays on the legacy path.
- Require an adapter-sanitized 1–4,096-byte non-control diagnostic, optional
  HTTP status 100–599, and optional explicit retry delay from 1 millisecond
  through 24 hours. Adapters must not copy response bodies, credentials, or
  secrets into the diagnostic. The constructor validates structural bounds,
  and Runtime revalidates the value at the executable Model boundary; neither
  layer can identify secret material by inspection.
- Treat the diagnostic as an actionable returned error, not telemetry.
  `PhaseObservation` receives only content-free
  `provider_failure_kind`, `provider_status_code`, and
  `provider_retry_after_ms`.
- Built-in HTTP adapters classify only facts supported by the boundary:
  401 is authentication, 403 authorization, 429 rate limit, 529 overload,
  remaining 4xx request rejection, remaining 5xx server failure, and other
  non-success statuses protocol failure. Transport failures and successful
  HTTP responses that violate the selected wire contract receive their exact
  classes. The adapters do not inspect arbitrary response text to invent
  quota, content-policy, overload, or model-unavailable evidence.
- The direct OpenAI adapter retains a positive bounded numeric
  `retry-after-ms`, or a positive bounded numeric `Retry-After` expressed in
  seconds. It does not guess an HTTP-date delay or cap an invalid value.
- Do not change failover, retry, cooldown, State settlement, or client protocol
  policy in this decision. Existing pre-output failover remains unchanged.
  ADR 0099 cooldown remains open only for a Runtime-owned attempt timeout.
- Do not change State, Protocol, Model Gateway, or snapshot coordinates. The
  typed Rust error and additive best-effort observation fields carry no new
  durable authority.

## Consequences

Hosts can distinguish provider facts without parsing diagnostics, and
Observability can aggregate failure classes without receiving prompts,
provider bodies, or error messages. Existing `LanguageModel` implementations
continue to compile unless they exhaustively match the public pre-1.0
`HarnessError` enum.

Typed evidence is deliberately not a recovery command. For example, a rate
limit with a retry hint does not authorize replay of an entire Turn after Tool
effects, and an authentication failure does not automatically remove a Model
from its exact configured route. ADR 0101 defines a separate default-disabled
same-Model retry policy for four transient classes; broader retry or health
policy must still define its own ownership, idempotency, concurrency,
streaming, and persistence semantics.

Provider-specific adapters may add stronger classifications when they possess
structured vendor evidence. They must not infer those facts from localized
human-readable strings.

## Rejected alternatives

- Replace every `Model(String)` with a typed Provider failure: this falsely
  attributes Harness validation and encoding failures to a remote provider.
- Parse diagnostic strings in Runtime: wording is unstable, localized, and
  controlled by extensions or remote systems.
- Copy the complete Hermes recovery taxonomy into Core: it couples generic
  failure evidence to provider quirks and product recovery actions.
- Put response bodies or messages into `PhaseObservation`: this violates the
  content-free Observability boundary.
- Immediately retry every typed transient failure: a whole-Turn retry can
  duplicate already-started external effects and requires a separate policy.

## Evidence

- `kernel::types::tests::model_provider_failure_is_typed_bounded_evidence`
- `model::tests::http_status_mapping_preserves_facts_without_inventing_policy`
- `model::tests::gateway_status_becomes_typed_evidence_without_response_body`
- `runtime::tests::typed_provider_failure_reaches_trace_without_diagnostic_content`
- `runtime::tests::typed_provider_failure_does_not_open_timeout_cooldown`
- `observability::tests::observation_metadata_is_rejected_before_retention_or_delivery`
- all-feature and zero-default workspace test gates

## Related decisions

- [ADR 0017: failure-isolated content-free Observability](0017-failure-isolated-content-free-observability.md)
- [ADR 0070: explicit bounded Model failover](0070-explicit-bounded-model-failover.md)
- [ADR 0084: Provider-reported Model evidence](0084-provider-reported-model-evidence.md)
- [ADR 0099: observable Model attempt-timeout cooldown](0099-observable-model-attempt-timeout-cooldown.md)
- [ADR 0101: bounded typed Model retry policy](0101-bounded-typed-model-retry-policy.md)
