# ADR 0104: Configured brokered JSON-command Models

- Status: accepted
- Date: 2026-07-28

## Context

The embeddable engine already exposed `JsonCommandModel`: any executable can
receive a bounded typed `ModelRequest` on stdin and return one validated
`ModelOutput` on stdout through a `ProcessBroker`. The reference service did
not expose that existing port in its strict Model configuration. Operators
could configure built-in OpenAI Responses or a versioned HTTPS gateway, but a
new provider bridge still required Rust host code.

Creating another plugin ABI would duplicate the existing model port,
subprocess lifecycle, cancellation, sandbox selection, and Model registry.
Leaving the service gap open would make the claim that model capabilities are
configuration-extensible incomplete.

## Decision

- Add a `json_command` variant to both the compatible single `model` form and
  the explicit `models` catalog. It participates in the same ordered
  `model_route`, attempt deadline, timeout cooldown, and Runtime selection as
  every other registered Model.
- Reuse `ServiceJsonProcessConfig` and one shared service constructor for JSON
  Tool and Model processes. Require an existing absolute executable, a
  canonical working directory, an exact host-to-child environment map, finite
  timeout/output/concurrency bounds, and an explicit `unrestricted` or
  `macos_seatbelt` launch authority.
- Preserve the public `JsonCommandModel` wire contract: stdin is one canonical
  JSON `ModelRequest`; stdout is exactly one `ModelOutput` (`message`,
  `tool_call`, or ordered `tool_calls`). Validate request/output JSON and Model
  decisions through the existing Runtime boundaries.
- Register the configured model as
  `CapabilityOrigin::External { id: "json-command-model/<model-id>" }`.
  Configuration or process launch never upgrades third-party code to
  first-party trust.
- Propagate the Runtime's exact Turn cancellation token to the brokered Model
  process and retain existing deadline, future-drop, pipe-settlement, and Unix
  process-group cleanup behavior.
- Keep service configuration schema 1 because the new tagged variant is
  additive and no existing field changes meaning.
- Do not infer Provider-specific usage, cost, request identity, settled Model,
  continuation, typed HTTP failure, or provisional streaming from the default
  stdout contract. `output_v1` returns `ModelOutput`, not `ModelResponse`.

## Consequences

An operator can write a model bridge in any language and add it to a
single-Model configuration or failover route without modifying or compiling
Y-Harness. The bridge converts its vendor protocol at the edge; the Harness
continues to own Context, Tool exposure, Policy, Agent Loop, State, retries,
and completion.

The executable is still code with the authority granted by its selected
`ProcessBroker`. `unrestricted` is not a sandbox. Environment inheritance is
cleared, but explicitly mapped credentials are visible to the bridge.

Provider-rich metadata and typed failure facts are now available through the
separately selected settlement-v1 contract in
[ADR 0108](0108-versioned-json-command-model-settlement.md). Provisional
streaming still requires a native `LanguageModel` or versioned HTTPS adapter.
The original stdout shape remains unchanged.

## Rejected alternatives

- Add a dynamic Rust plugin ABI: increases supply-chain and memory-safety
  surface while duplicating an existing language-neutral boundary.
- Launch through a shell command string: reintroduces interpolation and
  injection.
- Treat configured command Models as trusted extensions: configuration grants
  authority to run, not provenance.
- Parse stderr or arbitrary failure text into typed retry authority: diagnostic
  text is not a stable provider contract.
- Accept multiple stdout messages: would require a separate bounded streaming
  protocol and final-response settlement contract.

## Evidence

- `configured_json_command_model_runs_a_real_service_turn`
- `invalid_json_command_model_is_rejected_before_environment_access`
- `execution::tests::json_command_adapters_use_phase_specific_broker_requests`
- `execution::tests::json_model_propagates_runtime_cancellation_to_its_broker`
- strict checked-in `y-harness.command-model.example.json`
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0013: external execution is a brokered authority](0013-external-execution-broker.md)
- [ADR 0037: propagate Turn cancellation through the model-step handle](0037-model-step-cancellation-propagation.md)
- [ADR 0070: explicit bounded Model failover](0070-explicit-bounded-model-failover.md)
- [ADR 0087: explicit configured Model catalog and route](0087-explicit-configured-model-catalog-and-route.md)
- [ADR 0108: versioned JSON-command Model settlement](0108-versioned-json-command-model-settlement.md)
