# ADR 0040: Bounded Runtime Turn admission

- Status: Accepted
- Date: 2026-07-25

## Context

Thread-local ownership prevented two active Turns on one Thread, but one
`HarnessRuntime` could still execute an unbounded number of Turns across
different Threads. Protocol registry capacity bounded retained handles, not
simultaneous model, Context, Tool, or Policy work. Embedded callers bypass the
protocol entirely, so limiting only a service adapter would leave Core
availability dependent on every host implementing its own admission control.

## Decision

- Give every Runtime a concurrent Turn limit. The safe default is 32.
- Allow hosts to configure an exact value from 1 through 4,096 with
  `with_turn_concurrency_limit`; reject invalid configuration instead of
  silently accepting zero or an unreasonably large value.
- Atomically claim both the Thread identity and one Runtime admission slot under
  the existing active-Thread lock.
- Return `HarnessError::RuntimeOverloaded { limit }` when full. The protocol
  classifies this error as retryable.
- Perform admission before Thread load, capacity checks, `TurnStarted`, or any
  other State mutation.
- Release admission through the existing RAII active-Thread guard on every
  return and unwind path.

## Consequences

Embedded and service-hosted execution now have the same finite Core backpressure
boundary. An overload attempt consumes no durable event capacity and cannot
leave an unfinished Turn. The lock operation is a bounded in-memory set lookup
and insertion, with no semaphore queue or fairness policy.

The limit applies to one Runtime instance. Multiple Runtime instances or
multiple hosts need a higher-level coordinator and infrastructure capacity
policy; this change does not claim distributed admission control.

## Rejected alternatives

- Limit only protocol Operations: embedded callers and other future transports
  would remain unbounded.
- Queue indefinitely: queued prompts retain attacker-controlled memory and can
  exceed caller deadlines before execution begins.
- Use only provider connection-pool limits: Context, Policy, Tool, and local
  model work would still be unbounded.
- Silently clamp invalid configuration: operators could believe a capacity
  value was applied when it was not.
