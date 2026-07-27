# ADR 0107: Configured brokered Evaluation Graders

- Status: accepted
- Date: 2026-07-28

## Context

The Evaluation Engine already owned bounded case/Grader concurrency, immutable
samples, deterministic reports, isolated failures, and exact origin-bound
baselines. The reference product exposed only the fixed, dependency-free
`eval-smoke` gate. Users could implement a Rust `Grader`, but could not register
an existing checker in another language or run it against a configured Harness
without modifying the host.

The `Grader` contract also had only future-drop timeout behavior. Unlike
`EvaluationTarget`, it received no cooperative cancellation signal, so a
replaceable Process Broker could not observe the exact per-grade timeout.

## Decision

- Extend the public pre-1.0 `Grader::grade` method with one engine-owned
  `CancellationToken`.
- Give every grade task a distinct token. On timeout the Evaluation Engine
  cancels first, permits the existing bounded cleanup grace, and then records a
  normalized `grader timed out` outcome.
- Add `ExecutionPhase::Evaluation` for external grading. Evaluation remains
  outside Turn stop evidence and the live Agent Loop.
- Add `JsonCommandGrader`, reusing `JsonProcessConfig` and `ProcessBroker`.
  Send a strict cancellation-free `JsonGradeRequest` containing the validated
  case and captured target execution. Pass cancellation separately to the
  broker.
- Cap the complete Grader stdin at 4 MiB. This accommodates the existing
  bounded case and target-execution contracts while retaining an independent
  hard allocation ceiling. Validate nested case metadata and Tool JSON before
  process execution.
- Require one strict `JsonGradeResponse` containing `score`, `passed`, and an
  optional `rationale`. Reject unknown fields, malformed output, truncation,
  and unsuccessful exit. The Evaluation Engine still owns score/rationale
  normalization and baseline comparison.
- Add an optional strict `evaluation` object to service configuration schema 1.
  It contains independent case/Grader concurrency, timeouts, and named process
  Graders. Register each as
  `CapabilityOrigin::External { id:
  "json-command-grader/<grader-name>" }`.
- `yh doctor` validates and reports Evaluation Graders. Ordinary `yh serve`
  does not construct them or acquire their environment/process authority.
- Add `yh eval <suite> <baseline> [config]`. It assembles the configured Model,
  Tools, Context, Verification, Skills, and Memory against in-memory State,
  runs configured Graders, emits the existing format-2 report/comparison, and
  exits nonzero on regression. It never opens persistent service State,
  Approval, or Task databases.
- Keep State schema 11, Protocol v18, Evaluation format 2, and Model Gateway API
  v7 unchanged. Existing serialized artifacts and the fixed `eval-smoke` gate
  retain their meanings.

## Consequences

Evaluation Targets and Graders are now independently extensible through public
Rust contracts, while project Graders are installable by strict configuration
without changing Rust. A Grader can consume the full bounded sample but cannot
mutate Runtime State, call Tools, settle a Turn, or become a live Verifier.

Configured Models, MCP, Memory, and Grader processes retain exactly the
authority declared in the project configuration. In-memory Evaluation State
prevents production journal pollution; it is not a sandbox.

The `Grader::grade` signature and `ExecutionPhase` enum variant are source
updates for pre-1.0 Rust hosts. Implementations must accept the new token, and
exhaustive phase matches must handle `Evaluation`.

## Rejected alternatives

- Reuse `Verification` as the process phase: inaccurate because grading cannot
  influence live completion.
- Run configured Graders inside `serve`: acquires unused credentials and
  process authority on the production service path.
- Treat future drop as cancellation: hides timeout settlement from cooperative
  brokers and weakens cleanup guarantees.
- Let Graders write State or call Tools: conflates measurement with the system
  under measurement.
- Replace `eval-smoke`: removes the deterministic zero-network release gate.
- Add a distributed scheduler to Evaluation: Orchestration already owns that
  concern; format-2 materialized batches remain locally bounded to 64 cases.

## Evidence

- `evaluation::tests::bounds_target_and_grader_duration`
- `execution::tests::json_command_adapters_use_phase_specific_broker_requests`
- `execution::tests::process_request_reserves_the_larger_stdin_budget_for_evaluation`
- `execution::tests::json_grader_propagates_evaluation_cancellation_to_its_broker`
- `execution::tests::json_grader_rejects_unknown_response_fields`
- `execution::tests::command_adapters_reject_deep_json_before_broker_execution`
- `configured_json_command_grader_runs_an_isolated_real_evaluation`
- `configured_json_grader_retains_external_registry_origin`
- `invalid_json_command_grader_is_rejected_before_environment_access`
- strict checked-in config, suite, and baseline examples
- zero-default and all-feature workspace gates

## Related decisions

- [ADR 0010: Evaluation is not Verification](0010-evaluation-is-not-verification.md)
- [ADR 0013: external execution is a brokered authority](0013-external-execution-broker.md)
- [ADR 0026: bounded parallel Evaluation](0026-bounded-parallel-evaluation.md)
- [ADR 0064: allocation-time bounded JSON authority](0064-allocation-time-bounded-json-authority.md)
- [ADR 0067: versioned Harness smoke evaluation gate](0067-versioned-harness-smoke-evaluation-gate.md)
- [ADR 0069: origin-bound versioned Evaluation artifacts](0069-origin-bound-versioned-evaluation-artifacts.md)
- [ADR 0076: governed reference-service capability assembly](0076-governed-service-capability-assembly.md)
