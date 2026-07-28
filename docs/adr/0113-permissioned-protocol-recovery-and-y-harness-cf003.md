# ADR 0113: Protocol recovery is permissioned, exact-Turn fenced, and process-proven

- Status: Accepted
- Date: 2026-07-28

## Context

ADR 0035 correctly removed implicit recovery from normal Turn startup. A
Runtime sharing an Event Store cannot infer that another worker is dead, so
observing a `running` Turn is not authority to mark it `interrupted`.

The first Y-Harness CF-003 process probe exposed the other half of that
boundary. The public Runtime had an explicit `recover_thread` takeover API, but
the headless Protocol did not. After the controller killed the service at the
exact post-effect/pre-result boundary, a restarted `yh serve` truthfully
projected the abandoned Turn as `running` and had no serviceable way to perform
the already-defined explicit takeover.

A generic force-recover command would be too broad. A stale controller could
otherwise interrupt a different newer Turn, and a command name alone cannot
prove distributed ownership.

## Decision

- Advance the exact client Protocol to version 19.
- Add `recover_thread` behind the distinct `thread.recover` permission. It
  requires both `thread_id` and the exact observed `expected_turn_id`.
- Refuse recovery when the same Protocol host still owns a running Operation
  for that Thread.
- Require the expected Turn to be the one running Turn in the authoritative
  projection, then recheck that identity at the State optimistic-commit
  boundary. A stale or unrelated Turn identity fails closed without
  interrupting a newer Turn.
- Make retry after successful recovery idempotent only when that exact Turn is
  already `interrupted` and no newer Turn is running.
- Preserve ADR 0035's core rule: loading a Thread, restarting a service, and
  starting a normal Turn never recover implicitly. A network host should grant
  `thread.recover` only to a principal whose control plane established worker
  death and exclusive takeover. The permission and expected-Turn fence do not
  claim a distributed lease.
- Add format 9 and command `y-harness-cf003-restart` to the independent
  benchmark runner. It drives real `yh serve` processes over the typed stdio
  Protocol, a spec-bound JSON-command Model, the stdio MCP fixture, SQLite
  State, controller kill/restart, explicit recovery, and a new independent
  Turn.
- Pass only when the first Turn retains one exact Tool call and no Tool result,
  restart first observes it still `running`, explicit takeover changes that
  same Turn to `interrupted`, a new Turn completes without a Tool, and the
  external journal remains exactly one invocation and one effect both before
  and after restart.
- Keep the result `claim_eligible: false`. It measures deterministic recovery
  semantics, not Model reasoning quality or comparative product quality.

## Consequences

Headless clients can now perform the same controlled takeover already
available to embedded hosts without weakening shared-store safety into
automatic recovery. The exact expected Turn is enforced by State compare-and-
append and closes the stale-controller race that a Thread-only force command
would leave open, while the local live Operation check rejects the most direct
self-interruption error.

This does not provide multi-node failure detection, a durable ownership lease,
or fencing against a malicious or incorrectly authorized remote host. Those
remain orchestration/control-plane work. Recovery starts a new Turn after
interrupting the abandoned one; it does not continue an in-memory stack.

## Evidence

- Protocol wire, authorization, stale-Turn, idempotency, and live-operation
  refusal tests are in `src/protocol/mod.rs`; the newer-Turn race regression is
  in `src/state/mod.rs`.
- The spec-bound Model mode and identity-bound release oracle are in
  `tools/fault-fixture`.
- The shell-free process controller and format-9 report are in
  `tools/benchmark-runner/src/y_harness_fault.rs`.
- The checked non-claim record is under
  `tools/benchmark-runner/evidence/2026-07-28-y-harness-cf003-restart`.
