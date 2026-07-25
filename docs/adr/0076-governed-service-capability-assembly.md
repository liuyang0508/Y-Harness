# ADR 0076: Governed service capability assembly

- Status: Accepted
- Date: 2026-07-25
- Superseded in part by: [ADR 0077](0077-origin-bound-provider-continuation.md)

## Context

The Core already exposed typed Model, Tool, MCP, Memory, Secret, Process
Broker, Context, Policy, and State contracts, but the reference persistent
service assembled only `local/demo` plus `echo`, or an otherwise tool-less
custom HTTPS Model Gateway. A full-screen client could therefore prove the
Protocol and State path while still presenting a deterministic demo as if it
were the usable product path.

The service needs one real vertical assembly without giving a product client,
model vendor, MCP catalog, or child process a second authority path.

## Decision

- Add an optional first-party OpenAI Responses adapter outside the
  Microkernel. The service requires an explicit vendor model string and an
  environment-backed Secret reference; it never selects a moving default.
- Fix the adapter to OpenAI's official HTTPS endpoint. Disable redirects,
  ambient proxies, retries, referers, response storage, and provider-side
  parallel function calls.
- Map provider function calls onto one ordinary `ModelOutput::ToolCall`.
  Y-Harness remains the sole owner of Tool scheduling, Policy, Approval,
  State, retry, and completion.
- Decode JSON and SSE incrementally under explicit response-byte and event
  ceilings. Streamed text is provisional; only the completed Response is
  authoritative.
- Fail closed before Tool execution when a function-call response also carries
  opaque reasoning continuation. With `store: false`, silently discarding that
  vendor item would make the next model step semantically incomplete; the
  current generic State schema has no origin-bound field for it.
- Allow the service configuration to install shell-free JSON-command Tools
  through an explicit bounded Process Broker.
- Allow persistent stdio MCP servers only with an explicit launch authority,
  absolute executable, exact working directory, cleared environment, and
  host-environment values copied by configured name.
- Add exact-selected MCP Tool registration. Every selected remote name must
  exist and the entire selection registers atomically. A discovered catalog
  grants no authority by itself.
- Permit one selected MCP session to back the first-party Agent Memory Hub
  provider. Service startup and `doctor` perform a real health probe and close
  sessions on completion.
- Treat configured JSON Tools and selected MCP Tools as the service's explicit
  allow-list. They still pass through the ordinary Runtime Policy and State
  path.

## Consequences

The product path can now be:

```text
TUI
  → Protocol v11
  → Runtime
  → OpenAI Responses
  → governed JSON/MCP Tool
  → Agent Memory Hub Context
  → authoritative State
```

The demo remains deterministic and zero-network, but documentation must label
it honestly. A real OpenAI call still requires operator credentials, an
explicit available model ID, and the ignored live integration gate; local
schema and transport tests do not substitute for that external evidence.
The reasoning-model Tool limitation recorded by this decision was subsequently
resolved by ADR 0077 through an origin-bound, bounded, durable continuation
contract. Unreplayable provider state still fails before side effects.

This ADR does not add OpenAI hosted tools, vendor-side conversation ownership,
automatic MCP catalog trust, shell strings, inherited child environments,
additional vendor adapters, or a second Agent Loop.

## Sources

- [OpenAI Responses API](https://api.openai.com/v1/responses)
- [OpenAI function-calling flow](https://developers.openai.com/api/docs/guides/function-calling#the-tool-calling-flow)
- [OpenAI function-call handling](https://developers.openai.com/api/docs/guides/function-calling#handling-function-calls)
- [OpenAI model guidance for manually managed history](https://developers.openai.com/api/docs/guides/latest-model)
