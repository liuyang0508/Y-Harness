# ADR 0012: Versioned asynchronous Runtime protocol

- Status: Accepted
- Date: 2026-07-25
- Superseded in part by: ADR 0041 (Operation retention) and ADR 0042
  (protocol-v2 framing and event pagination)

## Context

Embedded callers, CLI/TUI clients, and future Web/Desktop hosts must control the
same Runtime without reimplementing Agent Loop semantics. A Turn can outlive one
request/response exchange, and its cancellation or timeout must remain distinct
from a transport disconnect.

An unbounded line protocol, unbounded operation registry, or whole-history event
read would make the reference service vulnerable to memory exhaustion even when
the Core itself is correct.

## Decision

The current language-neutral wire contract is maintained in
[`protocol.md`](../protocol.md); this ADR records why that contract is
versioned and asynchronous.

The initial slice exposed protocol version `1` as typed Serde contracts
independent of any transport. The first transport was newline-delimited JSON
over stdio. The current exact coordinate and accumulated wire contract are
recorded in [`protocol.md`](../protocol.md) and
[`compatibility.md`](../compatibility.md).

[ADR 0028](0028-mutually-authenticated-tls-protocol-host.md) later adds a
mandatory-mTLS network host over the same bounded JSONL framing and
`ProtocolHandler`; it does not alter Agent Loop or command semantics.

- Every request has a validated correlation ID and exact protocol version.
- Long-running Turns return an `OperationId`; clients poll or cancel the
  process-local operation independently of the durable Thread/Turn state.
- Terminal operation records must be explicitly forgotten. Retention has a hard
  capacity of 4,096 records, so it cannot grow without bound.
- Initial v1 input and output frames were each limited to 1 MiB. ADR 0042
  replaced that symmetric limit with the current asymmetric request/response
  bounds. Oversized output still becomes a small correlated
  `response_too_large` error rather than corrupting framing.
- Prompts and opaque identifiers have independent bounds.
- Thread events are cursor-paginated and the limit is enforced in the Event
  Store query, including SQLite.
- The stdio service writes protocol frames only to stdout. Diagnostic output
  belongs on stderr or in an Observability exporter.
- Protocol commands translate into calls on the same `HarnessRuntime` used by
  embedded applications; there is no second Agent Loop.

Operation retention is deliberately not authoritative. State events remain the
source of truth. If a service process exits while work is running, recovery
settles the durable running Turn as `interrupted`; the old process-local
`OperationId` is not resurrected.

## Consequences

Local clients now have a bounded, black-box-tested service surface suitable for
the reference CLI/TUI. Other transports can reuse `ProtocolHandler` without
changing Core execution semantics.

Clients must distinguish protocol correlation, process-local operation
identity, and durable Thread/Turn identity. They must page history, release
terminal operations, and reconcile durable state after server restart.

Later API-1 additions provide bounded provisional deltas through
[ADR 0020](0020-bounded-provisional-model-streams.md) and authenticated remote
transport through
[ADR 0028](0028-mutually-authenticated-tls-protocol-host.md) plus
[ADR 0029](0029-certificate-principal-protocol-authorization.md). Operation
records remain process-local and deliberately do not become a durable replay
coordinator.

## Rejected alternatives

- A synchronous `run_turn` protocol call: prevents independent polling and
  cooperative cancellation.
- A transport-specific Agent Loop: creates two semantic implementations and
  eventually divergent policy/state behavior.
- Automatic eviction of terminal operations: races clients that have not yet
  observed completion.
- Automatic replay after restart: unsafe for uncertain non-idempotent side
  effects.
