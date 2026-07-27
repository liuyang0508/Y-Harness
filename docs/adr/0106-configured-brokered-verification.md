# ADR 0106: Configured brokered verification

- Status: accepted
- Date: 2026-07-28

## Context

The Runtime already supported a deterministic `VerificationRegistry`.
Registered Verifiers receive one immutable candidate snapshot after Model
settlement. A retryable failure returns to the Agent Loop, a hard failure fails
the Turn, and every bounded outcome is journaled before completion.

The reference service always installed an empty registry. Users could not add
project completion conditions without writing a Rust host even when a checker
already existed in another language. The `VerificationRequest` also lacked the
Turn cancellation token, so a process adapter would have depended only on
future drop rather than cooperative `ProcessBroker` cancellation.

## Decision

- Add the exact cooperative `CancellationToken` to the public pre-1.0
  `VerificationRequest`. Runtime-created requests clone the active Turn token.
- Remove `VerificationRequest: PartialEq`; equality that ignores cancellation
  identity would be misleading, while `CancellationToken` intentionally has no
  value equality.
- Add `JsonCommandVerifier`, reusing `JsonProcessConfig` and `ProcessBroker`.
- Send one strict, cancellation-free `JsonVerificationRequest` containing
  Thread ID, Turn ID, ordered candidate snapshot Items, and candidate text.
  The active cancellation token is passed separately to the broker with
  `ExecutionPhase::Verification`.
- Require one strict internally tagged `JsonVerificationOutcome`:
  `{"status":"passed","summary":...}` or
  `{"status":"failed","reason":"...","retryable":...}`. Reject unknown fields,
  malformed output, truncation, and unsuccessful exit.
- Validate nested Tool input/result JSON before process execution and serialize
  the complete request through the shared 1 MiB allocation-bounded JSON-command
  ceiling.
- Leave semantic authority in the Runtime. Existing `validate_outcome` enforces
  non-empty and 4 KiB-bounded explanations; the Runtime alone decides whether
  to retry, fail, or complete and records the existing `VerificationResult`.
- Add a strict root `verifiers` list to service configuration schema 1. Each
  entry has a stable name, description, process config, and explicit launch
  authority. Duplicate or invalid descriptors fail before any Verifier process
  environment mapping.
- Register every command as
  `CapabilityOrigin::External { id:
  "json-command-verifier/<verifier-name>" }`. Registry execution remains
  deterministic by stable verifier name, independent of configuration order.
- Keep State schema 11, Protocol v18, and Model Gateway API v7 unchanged. The
  cancellation token is process-local, the service field is additive, and the
  existing result shape is reused.

## Consequences

Operators can implement completion checks in any language and compose multiple
checks through configuration. A checker may recommend another Model step but
cannot execute it, settle the Turn, write State, or bypass Policy.

`unrestricted` still grants the executable the Runtime user's operating-system
authority. Environment clearing and JSON framing are not sandboxing.

Adding the cancellation field is a source update for pre-1.0 Rust hosts that
construct or exhaustively destructure `VerificationRequest`. Implementations
that only read existing fields continue to work after recompilation.

## Rejected alternatives

- Let the process return a final assistant response: confuses verification with
  Model authority and bypasses the Agent Loop.
- Treat exit code alone as pass/fail: cannot express bounded actionable reasons
  or explicit retryability.
- Infer retryability from stderr or text: diagnostic prose is not authority.
- Depend only on future drop for cancellation: insufficient for a replaceable
  cooperative Process Broker.
- Add another plugin ABI: duplicates the existing language-neutral process
  boundary and its isolation lifecycle.
- Preserve `PartialEq` by ignoring cancellation: two requests with different
  stop authority are not semantically identical.

## Evidence

- `execution::tests::json_command_adapters_use_phase_specific_broker_requests`
- `execution::tests::json_verifier_propagates_runtime_cancellation_to_its_broker`
- `execution::tests::json_verifier_rejects_unknown_response_fields`
- `execution::tests::command_adapters_reject_deep_json_before_broker_execution`
- `configured_json_command_verifier_gates_a_real_service_turn`
- `configured_json_verifier_retains_external_registry_origin`
- `invalid_json_command_verifier_is_rejected_before_environment_access`
- strict checked-in `y-harness.verifier.example.json`
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0008: verification gates before completion](0008-verification-before-completion.md)
- [ADR 0013: external execution is a brokered authority](0013-external-execution-broker.md)
- [ADR 0038: panic-isolate runtime capability futures](0038-panic-isolated-runtime-capabilities.md)
- [ADR 0064: allocation-time bounded JSON authority](0064-allocation-time-bounded-json-authority.md)
- [ADR 0076: governed reference-service capability assembly](0076-governed-service-capability-assembly.md)
