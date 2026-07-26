# ADR 0078: Durable safe-boundary Turn steering

- Status: Accepted
- Date: 2026-07-26

## Context

An interactive client must be able to correct or extend one Turn while its
Model or Tool loop is still running. A UI-only input queue is insufficient:
the message can be lost with the client, attached to the wrong Turn, inserted
between a Tool call and its result, or race with a Model response that was
sampled from older context. Treating a process-local callback queue as
authoritative also prevents State inspection from explaining why the next
Model step changed.

The audited references contain complementary ideas:

- Pi exposes small steering and follow-up queues at explicit loop boundaries.
- The Claude Code reconstruction separates priority/scoped input queues and
  avoids inserting ordinary input before a pending Tool result.
- Codex requires an exact active Turn identity when steering and separates
  current-step input from later mail.
- Hermes discards a response crossed by new steering and restarts the loop.
- OpenCode keeps ordinary queued prompts editable in the client runtime, which
  is useful product behavior but not Runtime steering authority.

None of those observations alone defines Y-Harness State, recovery, Policy, or
client contracts. The design therefore adopts their mechanisms only after
translating them into engine-owned invariants.

## Decision

- Add `SteeringQueued` and `SteeringApplied` State Items. Acceptance is durable
  before acknowledgement. Only `SteeringApplied` projects into Model context,
  as a user message with the exact queued content.
- Require the caller's exact observed `TurnId`. Reject a missing, mismatched,
  sealed, or non-local active Turn without writing.
- Serialize normal Turn recording and steering acceptance through one
  per-active-Turn Runtime control lock. State compare-and-swap remains the
  durable concurrency authority.
- Bound one Turn to 64 pending steering messages and 1 MiB of pending content.
  Duplicate identities, non-FIFO application, changed content, application
  without a queue record, and completion with pending input fail closed.
- Apply pending input only at Agent Loop safe boundaries. A final Model message
  or Tool call sampled before steering arrived is stale: invalidate its
  provisional stream step, discard it, apply steering, and sample again.
- Never execute a stale Tool call. If steering is already pending before the
  Tool-effect boundary, journal a synthetic error Tool result for structural
  call/result integrity, apply steering, and continue without invoking the
  Tool. Steering accepted after an effect began waits until its Tool result is
  durably recorded.
- Seal steering under the active-Turn control lock before final-response
  settlement when the pending queue is empty. State completion independently
  checks durable pending input and fails closed. Failed, cancelled, timed-out,
  or interrupted Turns may retain unapplied queue evidence; completed Turns
  may not.
- Advance State events and disposable snapshots to schema 6 and the exact
  client protocol to 12. Protocol 12 adds `steer_turn`, the `turn.steer`
  permission, `turn_steered`, and the provisional `step_invalidated` event.
- Keep the feature in `HarnessRuntime` and the typed protocol. TUI, Web,
  Desktop, IM, and other clients may expose different queue UX, but they do not
  reimplement its semantics.

## Consequences

Steering is now inspectable, actor-attributed, bounded, and correlated with the
exact Turn. A client crash after acknowledgement does not erase acceptance, and
a late Model response cannot silently overwrite newer user intent. Tool
call/result ordering remains provider-safe while Policy and Tool execution
retain their existing authority boundaries.

The active control lock is process-local by design. This implementation does
not claim remote Turn takeover: another Runtime cannot steer a Turn it does not
own. Durable queued evidence may remain after interruption, but generic
recovery does not guess whether or how to resume it without an external
ownership and fencing protocol.

Every successful completion performs an authoritative pending-input check.
That extra read is accepted for the correctness boundary and must remain in
State performance baselines. A future cached counter may replace it only if
reopen, conflict, and corruption tests prove equivalent authority.

This decision does not add arbitrary mid-effect interruption, Tool rollback,
cross-process steering, client-side idempotency keys, priority queues, or a
comparative product-effect result.

## Evidence

- `runtime::tests::steering_is_durable_fenced_and_invalidates_a_crossed_model_response`
- `runtime::tests::steering_crossing_model_inference_discards_a_stale_tool_call`
- `runtime::tests::steering_before_the_tool_effect_preserves_call_result_structure_without_execution`
- `runtime::tests::steering_pending_count_and_bytes_are_bounded_before_durable_acceptance`
- `runtime::tests::steering_remains_open_across_a_retryable_verification_gate`
- `runtime::tests::failed_steering_application_preserves_the_pending_runtime_projection`
- `state::tests::state_authority_enforces_steering_correlation_order_and_completion_fence`
- `context::tests::only_applied_steering_becomes_model_visible_user_input`
- `protocol::tests::steering_protocol_requires_the_exact_running_turn_and_persists_acceptance`
- `protocol::tests::protocol_twelve_wire_envelopes_state_provenance_and_permissions_are_stable`

## Audited sources

- [Pi Agent loop](https://github.com/earendil-works/pi/blob/5bc1c2c0a6f07e00e8c240304182f213ab8d311f/packages/agent/src/agent-loop.ts)
- [Claude Code reconstructed query loop](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/query.ts)
- [Claude Code reconstructed message queue](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/utils/messageQueueManager.ts)
- [Codex input queue](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/core/src/session/input_queue.rs)
- [Codex Turn processor](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/app-server/src/request_processors/turn_processor.rs)
- [Hermes conversation loop](https://github.com/NousResearch/hermes-agent/blob/6ab5d2df2a5748f23ba7557ec527fac628720a22/agent/conversation_loop.py)
- [OpenCode client runtime queue](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/cli/cmd/run/runtime.queue.ts)
