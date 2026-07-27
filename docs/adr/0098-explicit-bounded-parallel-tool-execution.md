# ADR 0098: Explicit bounded parallel Tool execution

- Status: accepted
- Date: 2026-07-28

## Context

ADR 0086 made a Model's same-response Tool calls one atomic ordered decision
and deliberately executed them sequentially. Pi demonstrates the useful small
loop property of selecting sequential or parallel batch execution and then
restoring results to source order. Claude Code also separates concurrent and
exclusive Tools.

Neither a Provider's `parallel_tool_calls` setting nor Rust `Send + Sync`
proves that two real-world effects are semantically safe to overlap.
Idempotency alone is also insufficient: two idempotent operations can still
race on shared state. Y-Harness therefore needs an affirmative Tool-owned
contract, a finite Runtime bound, deterministic evidence, and fail-safe
defaults.

## Decision

- Add `ToolBatchExecution::{Sequential, ParallelSafe}` to the embedded Tool
  contract. `Sequential` is the default.
- `ParallelSafe` is a strong semantic guarantee: a call may overlap any other
  eligible call in the same Model decision, including another call to the same
  Tool. The Tool implementation or trusted operator owns the truth of this
  declaration.
- Capture the declaration once during Tool registration using the existing
  panic-isolated metadata boundary. Runtime scheduling uses the validated
  frozen value rather than calling extension metadata again.
- Keep whole-batch resolution, Policy, and approval before every effect. A
  denial, unknown Tool, invalid Policy result, or unresolved approval still
  prevents an earlier call in that Model decision from executing.
- Execute each maximal contiguous run of `ParallelSafe` calls concurrently.
  Every `Sequential` call is a fence: prior safe work settles before it starts,
  and later safe work starts only after it settles.
- Limit a same-batch run with a Runtime semaphore. The default is 4, the
  accepted range is 1–64, and 1 forces source-order execution without a second
  code path.
- Join every launched call and append `ToolResult` Items in original source
  order, independent of completion order. Ordinary Tool errors remain ordered
  error results and do not erase sibling evidence.
- Check durable steering before each safe run or sequential call. Work already
  in flight reaches its controlled cancellation/deadline boundary; steering is
  then applied at the next safe boundary.
- On cancellation or deadline, retain every completed sibling result in source
  order and terminally settle the Turn. Do not retry. A process crash after an
  effect remains an unknown-effect boundary, and normal recovery continues to
  refuse replay.
- Expose `max_parallel_tool_calls` and an explicit JSON Command Tool
  `batch_execution` field in project configuration. MCP Tools remain
  `Sequential` because MCP has no standard semantic effect-safety declaration.
- Do not change Protocol, State, snapshot, or Model Gateway compatibility
  coordinates. The durable batch and result shapes already encode the
  authoritative decision and deterministic settlement order; scheduling mode
  is host registration/configuration metadata.

## Consequences

Independent, explicitly safe calls can reduce wall-clock latency without
allowing a Model or Provider to grant execution authority. Sequential Tools
retain the prior behavior, and mixed batches have understandable fence
semantics.

This is not a transaction, rollback mechanism, or generic read-only inference.
A false `ParallelSafe` declaration can still create application races. The
project configuration therefore makes opt-in visible, `yh doctor` reports the
safe-Tool count and Runtime ceiling, and comparative latency remains a
benchmark question rather than an architectural claim.

## Rejected alternatives

- Trust the Provider's parallel-call preference: it describes sampling, not
  Tool effects or Harness authority.
- Infer safety from risk, idempotency, Tool name, schema, or origin: none proves
  conflict freedom.
- Run every accepted call concurrently: breaks sequential effect ordering and
  approval/recovery expectations.
- Add a durable scheduler event: unnecessary because the authoritative Model
  decision and ordered Tool results are already durable; replay after an
  uncertain effect remains forbidden.
- Add dependency graphs or lock keys to the first implementation: useful only
  after real Tools demonstrate that the two-state contract is insufficient.

## Evidence

- `kernel::registry::tests::tool_batch_execution_is_frozen_and_panic_isolated`
- `runtime::tests::explicitly_safe_tool_calls_overlap_but_settle_in_source_order`
- `runtime::tests::sequential_tool_fences_neighboring_parallel_safe_runs`
- `runtime::tests::parallel_safe_calls_respect_a_runtime_limit_of_one`
- `runtime::tests::parallel_batch_timeout_keeps_completed_effect_evidence_and_stops`
- `runtime::tests::parallel_batch_cancellation_keeps_completed_effect_evidence_and_stops`
- `runtime::tests::durable_batch_approval_resume_executes_every_call_once_in_source_order`
- Reference-service strict configuration and `yh doctor` process tests.

## Sources

- [Pi Agent loop at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/agent-loop.ts)
- [ADR 0086: atomic ordered multi-Tool decisions](0086-atomic-ordered-multi-tool-decisions.md)
