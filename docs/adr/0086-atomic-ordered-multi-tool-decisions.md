# ADR 0086: Atomic ordered multi-Tool decisions

- Status: Accepted
- Date: 2026-07-27

## Context

A Model response may propose more than one Tool call. Treating every call as an
unrelated Agent step loses the fact that they came from one decision; appending
them one at a time also permits a crash to preserve only a prefix. Starting
with unconstrained parallel execution would add effect races before State,
Policy, approval recovery, steering, and provider replay had one exact batch
contract.

Pi's current loop usefully separates batch preparation from sequential or
parallel execution and restores results to source order. Y-Harness needs the
same small-loop property without inheriting ambient extension authority or
making in-memory messages authoritative.

## Decision

- Add `ModelOutput::ToolCalls` for 2–64 calls in provider source order. A
  one-call response retains the existing `ToolCall` shape.
- Validate every call identity, Tool name, JSON input, duplicate correlation,
  per-call size, and aggregate 4 MiB batch bound before State growth.
- Persist the complete decision in one schema-7 `ToolCallsAppended` event. Each
  Tool-call Item records one generated batch identity, zero-based index, and
  exact batch size. Singular appends cannot carry batch metadata.
- Record the provider continuation before the decision, then append the batch
  atomically. Snapshot reconstruction preserves the one-event batch charge and
  rejects truncated, reordered, or reused batches.
- Resolve and authorize every call before any Tool effect begins. Deny,
  unknown Tool, invalid Policy output, or unsettled approval fails closed
  without executing an earlier call from the same decision.
- Initially execute the accepted batch sequentially in source order. Tool
  results are journaled in that order and the next Model request sees the
  complete ordered result set.
- If steering is pending before an effect, append synthetic error results for
  that call and every later unexecuted call before applying steering. Never
  leave a Model-visible call without a result.
- Approval recovery reconstructs the exact durable batch, revalidates the
  original Model-request fingerprint and registered origins, authorizes calls
  after the paused position, and executes every call once from the beginning.
  Recovery remains forbidden after an effect may already have started.
- Advance disposable snapshots to schema 7, the exact client protocol to 13,
  and HTTPS Model Gateway API to 6 because Thread/Event results and gateway
  Model responses expose the new typed shapes.
- Let bounded parallel execution reuse this batch contract only after an
  explicit Tool execution-safety declaration and deterministic settlement; it
  is not inferred from a Provider's request. ADR 0098 implements that follow-up
  without changing the durable shape.

## Consequences

Provider adapters can preserve same-response multi-call intent without
delegating scheduling or Tool authority to the Provider. A crash cannot expose
half of a Model decision, approval restart has enough evidence to reconstruct
the boundary, and sequential execution gives a deterministic first
implementation.

ADR 0098 subsequently permits finite concurrency only for Tools that make the
strong `ParallelSafe` guarantee. The default and every undeclared Tool remain
sequential.

## Rejected alternatives

- Append each call with `ItemAppended`: permits partial same-response decisions.
- Execute while authorizing later calls: an early effect could occur before a
  later denial or approval requirement is known.
- Trust `parallel_tool_calls` as execution permission: Provider sampling
  preference is not Harness Policy or Tool effect metadata.
- Disable Provider multi-call generation: hides a useful Model capability and
  prevents exact alignment with common Provider protocols.
- Launch every call concurrently: effect safety must be explicit and
  sequential calls must remain fences.

## Evidence

- `runtime::tests::runs_same_response_tool_calls_as_one_ordered_durable_batch`
- `runtime::tests::durable_batch_approval_resume_executes_every_call_once_in_source_order`
- `runtime::tests::model_tool_call_batch_rejects_duplicate_correlations`
- `state::tests::state_atomically_projects_one_ordered_tool_call_batch`
- `state::tests::atomic_tool_call_batch_requires_schema_seven`
- `model::openai_responses::tests::response_decodes_ordered_function_call_batches`

## Audited source

- [Pi Agent loop at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/agent-loop.ts)
