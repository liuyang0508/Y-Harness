# ADR 0036: Bounded explicit stdio MCP authority

- Status: Accepted
- Date: 2026-07-25

## Context

The initial stdio MCP client delegated child spawning and reads to the official
Rust SDK. Protocol lifecycle was correct, but two host invariants were missing:
the generic child inherited the Runtime's complete environment, and the current
async reader accumulated a newline-delimited frame with `read_until` without a
line ceiling. A misconfigured or hostile MCP process could therefore receive
unrelated secrets or force unbounded memory growth before decoded-value checks.

SDK convenience pagination also materializes every page without a page, cursor,
tool-count, or aggregate catalog limit.

## Decision

- Continue using pinned `rmcp 2.2.0` for MCP lifecycle, negotiation, JSON-RPC
  types, and async transport behavior.
- Replace only its generic child wrapper with a Y-Harness-owned transport:
  require an absolute executable and optional absolute working directory,
  execute without a shell or `PATH` lookup, clear inherited environment, pass
  only the configured map, inherit stderr for diagnostics, and kill/reap the
  child on close or drop.
- Wrap child stdout in an `AsyncRead` implementation that accepts at most
  8 MiB before each newline. It reads through one fixed 8 KiB scratch block,
  validates bytes first, and only then commits them to the SDK reader.
- Bound static args to 256 values of 16 KiB, environment to 256 entries and
  64 KiB, and request timeouts to 1 millisecond–24 hours.
- Redact all argument and environment values from configuration `Debug`
  output; only counts and environment names remain visible.
- Replace unbounded tool enumeration with at most 256 unique-cursor pages,
  4,096 unique tools, and 16 MiB of validated descriptors. Reject duplicate
  names and invalid cursors.
- Bound tool-call argument and result JSON to 1 MiB. Do not copy MCP
  tool-error content or protocol error data into engine errors.
- Read TLS certificate/key material through a maximum-plus-one limited reader
  so a later size check never follows an unbounded allocation.

## Consequences

The Agent Memory Hub integration retains a persistent official-SDK MCP session
while receiving an explicit process and resource boundary. Hosts that need
`PATH`, locale, vault-agent sockets, or other variables must opt them in by
name. A real isolated Agent Memory Hub round trip verifies that this stricter
environment still works.

The 8 MiB frame ceiling is larger than the 1 MiB decoded tool-result ceiling to
allow JSON escaping and envelope overhead. It is not a license for providers to
return 8 MiB values. Parsed values are checked again at the Y-Harness port.

This is process and memory hygiene, not an OS sandbox. The stdio server retains
the Runtime user's filesystem/network authority unless separately launched
inside a concrete broker or platform sandbox. Remote MCP, OAuth, secret
references for child environment values, and per-tool network policy remain
separate contracts.

## Rejected alternatives

- Trust inherited environment: leaks unrelated deployment secrets by default.
- Check a parsed result only: the unbounded frame allocation has already
  happened.
- Fork the SDK protocol implementation: duplicates negotiation and lifecycle
  code without fixing a protocol problem.
- Keep `list_all_tools`: a repeated cursor or endless catalog can consume the
  entire timeout and memory budget.
- Echo provider errors: may persist secrets or arbitrary server-controlled
  content into State and protocol responses.
