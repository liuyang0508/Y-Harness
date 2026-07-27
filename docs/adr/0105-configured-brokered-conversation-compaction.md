# ADR 0105: Configured brokered conversation compaction

- Status: accepted
- Date: 2026-07-28

## Context

ADR 0060 added a versioned asynchronous `ConversationCompactor` port to the
Context Engine. It already selects a bounded newest slice of omitted whole
Turns, preserves retained raw Turns, runs under the Runtime's Context deadline,
cancellation, observation, and panic boundary, validates independent output
budgets, and records content-free summary provenance without replacing
authoritative history.

The reference service could not configure that port. An embedding host had to
write Rust even when it already had a summarizer in Python, TypeScript, or
another executable. Adding a new plugin ABI would duplicate the existing
Process Broker and JSON-command lifecycle used by Tools and Models.

`TokenCounter` is intentionally synchronous because it is called repeatedly
while allocating model-request Context. Turning it into a child-process call
would either block an async Runtime thread or require a different contract.
Configuration extensibility does not justify lying about that boundary.

## Decision

- Add `JsonCommandConversationCompactor`, backed by the existing
  `JsonProcessConfig` and `ProcessBroker`.
- Send exactly one `JsonConversationCompactionRequest` to stdin. It contains
  Thread identity, bounded omitted Turns, uncovered count, retained Turn IDs,
  current prompt, and output token/byte budgets. It deliberately excludes the
  in-process cancellation token.
- Expect exactly one strict `JsonConversationCompactionResponse` shaped as
  `{"summary":"..."}` on stdout. Unknown fields, malformed JSON, truncation,
  and unsuccessful exit fail the capability.
- Propagate the exact Turn cancellation token separately to the broker and
  mark the request `ExecutionPhase::Context`.
- Reuse the JSON-command 1 MiB stdin ceiling. The service additionally rejects
  a configured omitted-history `input_budget_bytes` above that ceiling; the
  complete envelope remains authoritative and may be larger than the history
  slice because it also contains the prompt and metadata.
- Validate nested Tool input/result JSON shape before serialization and use
  allocation-bounded JSON encoding.
- Add one optional strict root `conversation` object to service schema 1. It
  configures the existing whole-Turn window and may contain one `compaction`
  command with semantic budgets, descriptor, process settings, and explicit
  launch authority.
- Validate the conversation policy, compactor descriptor, semantic budgets,
  command path, process bounds, working directory, and launch authority before
  reading the compactor process's configured host environment values.
- Register the command as
  `CapabilityOrigin::External { id:
  "json-command-compactor/<compactor-name>" }`.
- Keep semantic settlement in the existing Context Engine: it adds the
  non-authoritative header, rejects empty or over-budget summaries, computes
  exact source/content SHA-256 values, and never persists summary text.
- Keep `TokenCounter` native-only in this slice. A future configurable
  tokenizer requires a non-blocking, batch-aware contract rather than a
  subprocess hidden behind the current synchronous trait.
- Keep service configuration schema 1, State schemas, Protocol v18, and Model
  Gateway API v7 unchanged. The field is additive and reuses existing Context
  and State shapes.

## Consequences

Operators can add or replace a semantic summarizer in any language by editing
configuration and restarting the reference service. The executable receives
only the bounded history selected by the engine and the explicitly mapped
environment values; it never owns Thread history, State, or cancellation.

`unrestricted` still carries the Runtime user's filesystem and network
authority. Environment clearing is not a sandbox, and the service does not pin
an interpreter script or its transitive dependencies merely because the
configured executable path is absolute.

The 1 MiB complete command envelope is stricter than the embeddable compactor
port's 8 MiB semantic input maximum. Native hosts remain free to register a
different compactor under the larger core contract.

## Rejected alternatives

- Add a dynamic Rust or WebAssembly plugin ABI: duplicates a sufficient
  language-neutral boundary and expands supply-chain/runtime surface.
- Call a shell command string: reintroduces interpolation and ambiguous
  authority.
- Persist generated summary text: creates a second mutable conversation
  authority and retains derived potentially sensitive content.
- Fall back to raw omission when the command fails: silently changes declared
  coverage and can present incomplete context as complete.
- Expose a child-process `TokenCounter`: violates the current synchronous hot
  path and encourages blocking execution.
- Raise the global process stdin ceiling to the core's 8 MiB maximum: widens
  every JSON Model and Tool command without evidence that they need it.

## Evidence

- `execution::tests::json_command_adapters_use_phase_specific_broker_requests`
- `execution::tests::json_compactor_propagates_runtime_cancellation_to_its_broker`
- `execution::tests::json_compactor_rejects_oversized_input_before_broker_execution`
- `execution::tests::json_compactor_rejects_unknown_response_fields`
- `execution::tests::command_adapters_reject_deep_json_before_broker_execution`
- `configured_json_command_compactor_records_real_service_summary_evidence`
- `invalid_json_command_compactor_is_rejected_before_environment_access`
- strict checked-in `y-harness.command-compactor.example.json`
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0013: external execution is a brokered authority](0013-external-execution-broker.md)
- [ADR 0037: propagate Turn cancellation through the model-step handle](0037-model-step-cancellation-propagation.md)
- [ADR 0060: bounded non-authoritative semantic conversation compaction](0060-bounded-non-authoritative-semantic-conversation-compaction.md)
- [ADR 0064: allocation-time bounded JSON authority](0064-allocation-time-bounded-json-authority.md)
- [ADR 0076: governed reference-service capability assembly](0076-governed-service-capability-assembly.md)
