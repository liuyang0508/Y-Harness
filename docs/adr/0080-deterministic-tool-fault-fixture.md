# ADR 0080: Fault injection uses a controller-owned MCP fixture

- Status: Accepted
- Date: 2026-07-26

## Context

Unit tests can prove Y-Harness invariants but cannot expose the same failure to
released products. Embedding vendor-specific fault hooks in the Harness Core
would make the comparison circular. A realistic uncertain-effect case also
must distinguish the durable Tool effect from delivery of the Tool result.

## Decision

- Keep deterministic failure processes in the independent
  `y-harness-fault-fixture` workspace package, outside the semantic Core and
  product adapters.
- Start with one narrow stdio MCP fixture whose journal append is the synthetic
  non-idempotent effect. It synchronizes the effect and deliberately exits
  before responding on the first valid call.
- Pin the exact fixture executable in a strict spec. Use a create-new,
  append-only, locked, count-and-byte-bounded JSONL journal with contiguous
  sequence and call ordinals.
- Keep controller operations separate: `prepare` initializes evidence, `serve`
  exposes only the benchmark Tool, and `inspect` validates the settled journal.
  The Agent cannot invoke reset or inspection through MCP.
- Implement only the small MCP JSON-RPC surface required by the fixture and
  prove interoperability through the existing official Rust-SDK client. Do
  not turn the fixture into a second general MCP runtime.
- Treat one invocation and one effect as safe uncertain-effect handling.
  Classify another invocation and another committed effect separately; neither
  can be hidden by a later successful Tool result.
- Keep fixture observations ineligible for comparative claims until a pinned
  product run and restart trace are correlated with them.

## Consequences

The same deterministic crash can be presented to any released product that
supports stdio MCP, without granting its adapter control over the oracle. The
fixture is dependency-light, inspectable, and able to survive its own intended
process death.

The initial fixture does not prove a complete State-recovery matrix. Product
restart drivers, other fault modes, containment parity, and aggregated
comparison remain future work.

## Evidence

- Unit tests reject partial, mismatched, and reordered journal evidence and
  distinguish one effect from replay.
- A real process integration test uses the official MCP client to observe the
  crash, validate the safe one-effect state, reconnect explicitly, and prove
  the resulting duplicate fails the oracle.
