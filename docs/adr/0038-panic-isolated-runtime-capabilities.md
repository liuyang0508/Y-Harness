# ADR 0038: Panic-isolated Runtime capabilities

- Status: Accepted
- Date: 2026-07-25

## Context

The Runtime bounded cancellation, deadlines, and ordinary provider errors, but
a panic while a Context, Model, Policy, Approval, Tool, or Verification future
was constructed or polled could unwind past the Agent Loop. The active Turn
would remain `running` until explicit recovery, and embedded hosts could lose
the task that was responsible for settlement.

A future can also panic in `Drop` after completion or when cancellation/timeout
drops pending work, so catching only the `poll` call is incomplete.

## Decision

- Make the controlled external-operation boundary accept a future factory. Run
  both factory invocation and every future poll inside `catch_unwind` with an
  explicit `AssertUnwindSafe` host boundary.
- Store the pinned future in an `Option` and drop it inside the same isolation
  policy on completion, poll panic, cancellation, timeout, and wrapper drop.
- Convert every caught panic into
  `HarnessError::CapabilityPanicked { phase }`. Never copy, format, inspect, or
  persist the panic payload.
- Route the typed error through the existing Runtime settlement logic:
  Model/Context/Policy/Approval/Verification panics fail the Turn; a Tool panic
  becomes a bounded error result that the Agent Loop may inspect, consistent
  with other Tool execution failures.
- Preserve content-free Observability outcome `error` and the ordinary durable
  RuntimeError/TurnFinished ordering.

## Consequences

An extension panic no longer escapes the Agent Loop or strands the Turn in the
normal execution path. The isolation layer adds one boxed future per external
phase, which is small relative to network/process/model operations and buys a
single uniform settlement boundary.

Rust invokes the process-global panic hook before `catch_unwind` returns. The
engine prevents payloads from entering State, protocol, and Observability, but
cannot promise that a host-installed panic hook or process stderr will omit
them. Production hosts must govern their global panic hook and stderr sink.

This boundary does not catch panics in State authority code or arbitrary host
work outside controlled capabilities. Built-in code remains subject to tests,
and State/provider adapters require their own trust and validation boundaries.
`panic=abort` builds cannot unwind and therefore cannot use this recovery
mechanism; such builds need process supervision and must not claim in-process
panic settlement.

## Rejected alternatives

- Let panics unwind: strands durable work and makes one extension a host-wide
  failure authority.
- Catch only around `await`: synchronous future construction can already panic.
- Catch only `poll`: a future destructor can panic on every settlement path.
- Persist the panic payload for diagnostics: provider-controlled or secret
  content would cross State and protocol data-governance boundaries.
- Spawn every call as a Tokio task: borrowed provider futures are not
  necessarily `'static`, and task spawning is not required for isolation.
