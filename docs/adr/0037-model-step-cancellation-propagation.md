# ADR 0037: Propagate Turn cancellation through the model-step handle

- Status: Accepted
- Date: 2026-07-25

## Context

Tools receive the Runtime's Turn cancellation token in `ToolContext`, but the
provider-neutral Model contract previously exposed only the request and
optional provisional stream. `JsonCommandModel` therefore created a fresh
token for `ProcessBroker`. The built-in local broker still killed its child
when the outer Model future was dropped, but a custom broker could not observe
cooperative cancellation and might leave remote or detached work running.

## Decision

- Attach the exact Turn-level `CancellationToken` to the kernel-owned
  `ModelStream` step handle, including when no provisional sink is installed.
- Expose a cloned token to model providers through
  `ModelStream::cancellation_token`.
- Make Runtime install the caller's token once on the Turn-level handle and
  preserve it across every model step.
- Override `JsonCommandModel::complete_streaming` so the Process Broker receives
  that token. Direct calls to the simpler `complete` method retain an
  independent token because no caller cancellation context exists there.
- Retain future-drop cleanup as a second fence. A deadline does not mutate the
  caller's cancellation token, so providers must remain safe when their future
  is dropped.

## Consequences

Runtime cancellation now reaches built-in and custom external Model brokers
through an explicit provider contract instead of depending on one broker's drop
implementation. Provisional output and cancellation share one step-lifetime
handle, while the final `ModelResponse` remains authoritative.

This does not turn cancellation into rollback. A provider request may already
have incurred cost or side effects, and no automatic retry follows a cancelled
or dropped Model future.

## Rejected alternatives

- Create a fresh token in each adapter: disconnects the provider from Runtime
  control.
- Rely only on dropping the future: custom or detached work may outlive it.
- Cancel the caller token on deadline: changes caller-owned control state and
  conflates explicit cancellation with timeout.
- Add cancellation to serializable `ModelRequest`: runtime-only synchronization
  state must not cross provider JSON or durable State boundaries.
