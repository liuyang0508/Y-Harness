# ADR 0114: Bound Runtime Model attempts per Agent Loop step

Status: accepted

## Context

`max_steps` bounds settled Agent Loop decisions, while one step may invoke
several Providers through same-Model retry and ordered Route failover. The
largest supported configuration previously permitted 16 Route entries and
nine calls per entry without one independent shared ceiling.

Released-product control preflights also showed that matching a requested main
model does not prove whole-Turn model parity: a product may make auxiliary
title, compaction, planning, or verification calls outside its main response.
Y-Harness must bound the calls it actually governs without claiming visibility
inside an arbitrary extension implementation.

## Decision

- Add one Runtime-owned `max_model_attempts_per_step` budget shared by retries
  and Route candidates. Check the budget before every
  `LanguageModel::complete_streaming` invocation.
- Default the budget to 16, which preserves a complete no-retry traversal of
  the largest supported Route. Accept explicit values from 1 through 144, the
  existing maximum of 16 candidates times nine calls.
- Expose the same defaulted field in service configuration and reject invalid
  values during configuration loading. `yh doctor` reports the effective
  per-step budget.
- Return `HarnessError::MaxModelAttempts(limit)` before an over-budget Provider
  call. Attempts already invoked remain visible through ordinary
  `PhaseObservation` records.
- Define the Runtime-managed whole-Turn upper bound as
  `max_steps × max_model_attempts_per_step`. Each `for`-loop step consumes its
  budget even when Steering invalidates its response; approval continuation
  resumes from the durable count of already settled steps.

## Boundary

The budget counts only calls crossing the registered `LanguageModel` boundary.
A Conversation Compactor, Verifier, Tool, MCP server, or other extension may
internally call a model, but the Runtime cannot truthfully count that hidden
work under the current capability contracts. Such extensions need their own
bounded configuration and observations or a future shared call-budget
contract. They are not silently relabeled as main Agent Model calls.

This change does not add State Items, alter Client Protocol or Model Gateway
formats, or require State migration. Exact per-attempt trace records remain
process observations; the durable safety invariant is the product of the
existing step bound and this independent per-step ceiling.

## Consequences

- Retry and failover can no longer multiply one logical step beyond an
  operator-visible limit.
- A deliberately small budget may stop before a healthy fallback. That is an
  explicit governance decision and fails with a typed error.
- Raising either bound raises the possible Turn total; hosts remain
  responsible for choosing budgets appropriate to latency and cost policy.
- Whole-product benchmark reports must separately disclose auxiliary model
  calls that bypass the compared main Agent Model boundary.

## Verification

- `runtime::tests::model_attempt_budget_is_bounded_and_exposes_the_turn_ceiling`
- `runtime::tests::model_attempt_budget_stops_retry_before_an_extra_provider_call`
- `runtime::tests::model_attempt_budget_stops_failover_before_an_extra_provider_call`
- `runtime::tests::model_attempt_budget_resets_for_each_agent_loop_step`
- `reference_cli::service::tests::config_rejects_an_unbounded_model_attempt_limit`
