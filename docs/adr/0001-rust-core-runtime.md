# ADR 0001: Rust for the Core and Runtime

- Status: Accepted
- Date: 2026-07-25

## Context

Y-Harness Engineering must produce a Core that is usable as an embedded
library and as a long-running Runtime. It must coordinate concurrent model and
tool work, enforce policy before side effects, persist recoverable state,
expose a stable typed protocol, and ship as a small cross-platform executable.

The extension ecosystem must not be restricted to the implementation language
of the kernel.

## Decision

Use Rust as the primary language for Core, Runtime, engine CLI, and the first
independent reference product client (`y-harness-tui`).

Keep third-party capability contracts language-neutral. Trusted built-ins may
run in process. Untrusted executable extensions will use a supervised
out-of-process protocol. MCP remains an adapter, not the internal object model.
Language SDKs can be generated from the runtime protocol after that protocol
stabilizes.

At dynamic Rust boundaries, public async contracts return boxed futures.
Native `async fn` in traits is not currently dyn-compatible, while capability
registries require trait objects. See the
[Rust Reference](https://doc.rust-lang.org/stable/reference/items/traits.html#dyn-compatibility).

## Why

- Algebraic data types make state transitions and wire events explicit.
- Ownership and `Send`/`Sync` constraints expose unsafe concurrency designs
  during compilation.
- Rust supports a single deployable runtime without a language VM.
- It has mature async, terminal, SQLite, WASI, and protocol ecosystems.
- The Codex runtime demonstrates that Rust is practical for a production agent
  runtime, while Pi and OpenCode demonstrate why extension ergonomics must
  remain language-neutral. The broader primary-source comparison is recorded
  in [`../reference-analysis.md`](../reference-analysis.md).

## Alternatives considered

- TypeScript offers excellent extension ergonomics but makes strong process
  isolation and a small standalone runtime harder to treat as defaults.
- Go offers simple deployment and concurrency, but Rust's enums and ownership
  model better fit the state machine and protocol core.
- Python offers the broadest AI ecosystem but is better suited to providers,
  evaluators, and application extensions than the systems kernel.

## Consequences

- Native Rust plugins are not the default third-party extension mechanism.
- Protocol and schema compatibility become first-class release concerns.
- Unsafe Rust is forbidden unless a future ADR documents the boundary and its
  verification strategy.
