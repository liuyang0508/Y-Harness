# ADR 0075: Independent optional product clients

- Status: Accepted
- Date: 2026-07-25

## Context

Y-Harness is a reusable Harness engine, while TUI, Desktop, Web, and IM are
different product surfaces. Compiling a product UI into the engine would couple
release cadence, dependencies, lifecycle, and presentation policy to kernel
semantics. Letting a client read SQLite or call providers directly would also
create a second authority path around State and Policy.

## Decision

Keep `y-harness` headless. Ship every product surface as an independent,
optional package that controls the engine through a versioned public contract.

The first product client is the `y-harness-tui` workspace package under
`clients/tui`, installed as `yh-tui`. It starts `yh serve` or `yh serve-demo`
as a supervised child and exchanges bounded Protocol v10 JSONL frames. It may
reuse public wire DTO types at compile time, but it does not construct
`HarnessRuntime`, call Model/Tool/Policy implementations, or open Engine
databases.

The TUI treats durable Thread projection as authoritative. Provisional stream
deltas are visibly provisional, operation records are explicitly forgotten,
and cancellation uses the protocol. Approval inspection is read-only for a
local-process client because the same principal must not approve its own
request. Task inspection attaches to an explicitly named Graph and does not
perform worker duties.

Future Desktop, Web, IM, and SDK products use the same rule. Their language,
framework, process topology, and release cadence may differ without changing
Core semantics.

## Consequences

- Installing or removing a client cannot change Engine execution behavior.
- Engine releases and product-client releases can be packaged independently.
- Client compatibility is an explicit protocol-version concern.
- Product-only dependencies such as terminal rendering stay out of the Engine
  package.
- Remote approval settlement requires an independently authenticated principal;
  a UI cannot manufacture separation of duty.
- New presentation needs may motivate protocol additions, but a client-specific
  shortcut into storage or Runtime internals is rejected.

## Rejected alternatives

- Embed a full-screen TUI in `yh`: couples the headless engine to terminal
  dependencies and product behavior.
- Share SQLite directly: bypasses State invariants, authorization, migrations,
  and transport compatibility.
- Reimplement an Agent Loop per product: creates divergent execution semantics.
- Bundle every client by default: removes operator choice and expands the attack
  and dependency surface.
