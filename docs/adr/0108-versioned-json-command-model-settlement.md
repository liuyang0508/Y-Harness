# ADR 0108: Versioned JSON-command Model settlement

- Status: accepted
- Date: 2026-07-28

## Context

Configured JSON-command Models made arbitrary-language Provider bridges
installable without changing Rust, but their original stdout contract was one
bare `ModelOutput`. It could not carry Provider-reported usage, exact cost,
settled Model, request identity, continuation, or typed failure facts. Those
facts therefore could not enter the Runtime's existing observability,
continuation, retry, and failover contracts.

Changing the meaning of the original stdout object would silently break
installed bridges. Inferring failure classes from stderr, exit codes, or
diagnostic text would also turn unversioned prose into retry authority.

## Decision

- Preserve `output_v1` as the default JSON-command Model protocol. Missing
  configuration continues to mean one bare `ModelOutput`.
- Add the explicitly selected `settlement_v1` protocol. The process still
  receives one bounded `ModelRequest` on stdin and returns exactly one bounded
  JSON object on stdout.
- Define a strict terminal `JsonModelSettlement`:
  - `completed` carries `ModelOutput` plus optional `ModelUsage`,
    Provider-reported Model, request identity, and `ModelContinuation`;
  - `failed` carries one bounded `ModelProviderFailureKind`, sanitized message,
    optional HTTP status, and optional retry delay.
- Reject unknown settlement fields, malformed values, unbounded Provider
  evidence, and invalid continuation before the response reaches the Agent
  Loop. A malformed failure never gains typed retry authority.
- Feed valid completed evidence through the existing
  `validate_model_response` boundary. Feed valid failed evidence through
  `ModelProviderFailure`, so existing Runtime policy—not the bridge—decides
  retry, failover, cooldown, and final settlement.
- Keep the configured Model's registered identity and External origin
  authoritative. `provider_model` is evidence and never changes routing,
  Policy, continuation ownership, or State provenance.
- Keep provisional output unsupported. A future streaming command protocol
  requires separately bounded framing, ordering, invalidation, and terminal
  settlement semantics; it is not inferred from multiple stdout values.
- Keep service schema 1, State schema 11, Protocol v18, and Model Gateway API
  v7. The new protocol selector is additive, and existing durable/wire
  artifacts do not change.

## Consequences

An arbitrary-language Provider bridge can now preserve truthful accounting and
failure facts without a native Rust adapter. Typed transient failures can use
the same bounded Runtime retry path as HTTPS Providers, while unavailable
evidence remains absent.

The bridge is responsible for removing secrets and response bodies from its
diagnostic before returning it. Y-Harness validates structure and bounds but
cannot recognize secret material. Selecting `settlement_v1` is an exact
configuration contract; returning a legacy output under that selector fails
closed.

## Rejected alternatives

- Replace `output_v1`: breaks existing adapters without negotiation.
- Auto-detect the response shape: makes a field collision or typo change
  semantics and weakens configuration review.
- Parse stderr or nonzero exits into Provider failures: diagnostic text and
  process status are not stable Provider facts.
- Let the bridge choose retry behavior: recovery policy belongs to the
  Runtime, not the evidence adapter.
- Accept JSONL streaming under the same selector: omits ordering, byte,
  cancellation, invalidation, and final-response contracts.

## Evidence

- `execution::tests::json_model_settlement_preserves_provider_evidence`
- `execution::tests::json_model_settlement_is_strict_and_returns_typed_failure`
- `configured_json_model_settlement_drives_typed_runtime_retry`
- legacy `configured_json_command_model_runs_a_real_service_turn`
- strict checked-in `y-harness.command-model.example.json`
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0037: propagate Turn cancellation through the model-step handle](0037-model-step-cancellation-propagation.md)
- [ADR 0070: explicit bounded Model failover](0070-explicit-bounded-model-failover.md)
- [ADR 0083: exact Provider Model cost ticks](0083-exact-provider-model-cost-ticks.md)
- [ADR 0084: Provider-reported Model evidence](0084-provider-reported-model-evidence.md)
- [ADR 0100: typed Model Provider failure evidence](0100-typed-model-provider-failure-evidence.md)
- [ADR 0101: bounded typed Model retry policy](0101-bounded-typed-model-retry-policy.md)
- [ADR 0104: configured brokered JSON-command Models](0104-configured-brokered-json-command-models.md)
