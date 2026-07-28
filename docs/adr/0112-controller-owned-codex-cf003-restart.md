# ADR 0112: CF-003 restart uses persisted Thread identity and an external effect oracle

- Status: Accepted
- Date: 2026-07-28

## Context

Format 7 proves that one released Codex process observes a failed MCP transport
without replaying the fixture effect. It cannot answer what happens when the
product itself stops after the effect but before recording a Tool result.

Codex `0.145.0` persists each appended conversation item and flushes the
rollout. On history normalization, a `function_call` without a matching output
receives `FunctionCallOutputPayload::from_text("aborted")`. Its headless CLI
can resume an exact persisted Thread and start a new Turn. These source
properties make a deterministic restart cell possible, but they do not prove
the released binary or runtime behavior.

The first real probe also showed that Codex re-groups the configured MCP child.
Cancelling the outer Codex process group therefore settles Codex but leaves the
held fixture alive. Treating that as complete process-tree cleanup would be
false.

## Decision

- Add format 8 and command `codex-cf003-restart` to the independent benchmark
  runner. Do not add Codex behavior to Harness Core.
- Add a `hold_after_first_effect` fixture case. It synchronizes the same
  invocation/effect records as format 7, returns no Tool result, and accepts
  only a bounded release marker containing its exact fixture identity.
- Use a persistent empty `CODEX_HOME`. The first Provider selects deferred
  Tool search and the pinned MCP call, then accepts no third request.
- Poll the controller-owned journal to the exact one-effect boundary. Cancel
  the first Codex process through the Process Broker, create and synchronize
  the fixture release marker, and require the journal lock to settle.
- Discover exactly one bounded rollout beneath `CODEX_HOME/sessions`. Reject
  symlinks, excessive depth/count/bytes, malformed JSONL, a noncanonical Thread
  UUID, a missing or mismatched function call, or any matching persisted Tool
  output.
- Start a second released Codex process with `exec resume <thread-id>`. Require
  the loopback Provider to observe the original exact call, the exact
  source-defined string output `aborted`, and the new resume user message.
  Return a fixed final assistant message without selecting a Tool.
- Pass only when the resumed JSONL uses the same Thread, the same rollout grows
  and changes digest, no resumed MCP Tool event appears, and independent
  observations before and after resume both retain exactly one invocation and
  one effect.
- Record rather than hide the boundaries: the MCP child needed controller
  release, recovery starts a new Turn on the same Thread, the Provider is
  deterministic, the product binary is not reproducibly linked to the source
  commit, outer isolation is unrestricted, and the result is
  `claim_eligible: false`.
- Reject non-Unix execution in this version because the product-cancellation
  evidence relies on the Process Broker's Unix process-group semantics. The
  release marker is coordination for the detached fixture, not a containment
  substitute.

## Consequences

Y-Harness now has real evidence for a released Codex restart across an
uncertain non-idempotent effect. The evidence distinguishes four authorities:
Codex owns Thread persistence and normalization; the loopback Provider observes
the Model wire contract; the controller owns process sequencing; and the
fixture journal is the effect oracle.

The result does not prove continuation of an interrupted Turn stack, generic
descendant cleanup, Linux/Windows parity, other products, real-Model reasoning
quality, or competitive superiority. Those remain separate cells.

## Evidence

- Official source at the pinned commit shows response-item rollout persistence
  in
  [`core/src/session/mod.rs`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/core/src/session/mod.rs),
  append-and-flush behavior in
  [`thread-store/src/local/live_writer.rs`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/thread-store/src/local/live_writer.rs),
  missing-output normalization in
  [`core/src/context_manager/normalize.rs`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/core/src/context_manager/normalize.rs),
  and resume parameter construction in
  [`exec/src/lib.rs`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/exec/src/lib.rs).
- Unit tests close restart coordinates, persistent resume arguments, exact
  original call, synthetic `aborted` output, resume user message, identity-bound
  release, and checked evidence hashes.
- The checked record under
  `tools/benchmark-runner/evidence/2026-07-28-codex-cf003-restart` used released
  Codex `0.145.0`, resumed Thread
  `019fa611-48a1-7042-8ebe-5c7ec7e3312d`, emitted no resumed MCP Tool call, and
  retained one invocation and one effect before and after resume.
