# ADR 0005: Official MCP SDK behind an internal transport port

- Status: Accepted
- Date: 2026-07-25

## Decision

Y-Harness uses the official Rust MCP SDK for protocol lifecycle, capability
negotiation, JSON-RPC types, and asynchronous read/write transport semantics.
The dependency is pinned to stable `rmcp 2.2.0` and hidden behind the Y-Harness
`McpClient` port.

The crate enables only the SDK's client and asynchronous read/write features.
Y-Harness does not compile the unused MCP server, generic child-process helper,
or HTTP transport surfaces.

The selected SDK source uses let-chain syntax stabilized in Rust 1.88, so
Y-Harness declares Rust 1.88 as its tested minimum supported toolchain.

The stdio client maintains one initialized session, serializes calls on that
session, applies bounded initialization and request timeouts, invalidates a
failed session, and reconnects on the next operation. A Y-Harness-owned process
wrapper spawns an absolute command directly without a shell, clears inherited
environment, bounds raw input lines before the SDK allocates complete JSON,
and reaps the child on close/drop.

## Rationale

MCP lifecycle and framing are protocol infrastructure with multiple negotiated
versions and evolving edge cases. Reimplementing them locally would add risk
without differentiating Y-Harness.

The SDK's current generic async reader uses an unbounded `read_until` line
buffer, and its generic child helper inherits the parent environment. Those
host-policy choices are outside protocol correctness, so Y-Harness wraps the
SDK transport with its own byte-bounded reader and explicit process authority
instead of silently inheriting them.

The internal port prevents SDK types from leaking into Memory, Tool Runtime, or
kernel contracts. It also permits later conformance-tested upgrades or another
transport implementation without rewriting providers.

## Agent Memory Hub compatibility

The first real adapter uses Agent Memory Hub's stdio launcher and validates the
actual tool surface. It currently declares `search`, `read`, `write`, `brief`,
and `health`.

Agent Memory Hub does not currently expose adopted/rejected/ignored injection
feedback through MCP, so the adapter does not declare `feedback`. Its MCP write
tool also does not settle the Y-Harness idempotency key; the adapter reports
that limitation and never automatically retries an uncertain write.

FastMCP may encode a one-item list result as a singleton object. The adapter
accepts both array and singleton-object search shapes and tests both, while
rejecting unrelated malformed objects. Error diagnostics report JSON shape
only, not memory content.

## Upgrade rule

An SDK or protocol upgrade requires:

- unit tests for framing-independent mappings;
- stdio lifecycle, raw-frame, environment, pagination, result, and timeout
  tests;
- the real isolated Agent Memory Hub round trip;
- review of negotiated protocol version and published SDK migration notes.
