# ADR 0115: Bound MCP Tool cancellation settlement

Status: accepted

## Context

`ToolContext` carried a Turn cancellation token, but the MCP Tool adapter
checked it only before invoking `McpClient::call_tool`. Cancellation or Turn
deadline expiry after a remote call started caused the Runtime to drop the Tool
Future. A persistent stdio or HTTPS MCP session could therefore remain live,
and a later call could reuse a session whose previous Tool effect was
ambiguous.

Dropping a local Future is not evidence that a remote side effect rolled back
or that a child process settled. Waiting without a limit is also unsafe because
an arbitrary Tool can ignore cancellation indefinitely.

## Decision

- Add the defaulted `Tool::cancellation_settlement_timeout` declaration. Freeze
  it with other Tool metadata at registration, panic-isolate it, and reject
  values above ten seconds. The default is zero.
- Give every Tool call a dedicated Runtime stop token. Explicit Turn
  cancellation and deadline expiry both cancel this token. When a Tool reserved
  a grace, poll it for only that duration so it can settle; preserve the
  original cancelled/timed-out outcome after successful cleanup. Close the
  token on every ordinary Tool settlement so a detached clone cannot outlive
  its call authority.
- Preserve a cleanup error or panic instead of relabeling it as successful
  cancellation or timeout.
- Add the defaulted `McpClient::call_tool_with_cancellation` entry so existing
  client implementations remain source-compatible. Its fallback races the
  token with `call_tool` and drops the losing call Future.
- Route every namespaced MCP Tool through the cancellation-aware entry. The
  built-in stdio and HTTPS clients override it, acquire the session
  cooperatively, and remove and boundedly close the session when cancellation
  wins after call admission.
- Return immediately when cancellation wins before session acquisition. This
  avoids disturbing an unrelated call from another concurrent Turn.
- Never retry the cancelled Tool call. A later explicit call may establish a
  new session.

The built-in session close has a nine-second limit inside the Tool's ten-second
Runtime grace. Stdio transport close retains its existing bounded direct-child
and Unix process-group settlement. Failure to finish session cleanup is an MCP
error.

## Boundary

Cancellation is not rollback. The provider may have committed an effect before
the engine observed the stop signal. Y-Harness records no synthetic Tool
result, sends no automatic retry, and makes no claim that an arbitrary
third-party `McpClient` cancels detached work. Stateful third-party clients must
override the new method to provide that guarantee.

The change adds no State Item, service configuration field, Client Protocol
command, or MCP wire extension. In particular it does not claim protocol-level
Tool cancellation acknowledgement from the server.

## Consequences

- Cancelled or timed-out built-in MCP calls cannot silently leave their
  persistent session eligible for reuse.
- Ordinary Tools retain immediate cancellation unless they explicitly reserve
  bounded cleanup time.
- A non-cooperative Tool can delay settlement only by its declared, validated
  grace; expiration still drops its Future.
- `RegisteredTool` gains a public frozen metadata field before the first stable
  release. Existing `Tool` and `McpClient` implementations keep safe defaults.

## Verification

- `transport::mcp::tests::turn_cancellation_stops_an_in_flight_mcp_tool`
- `transport::mcp::tests::turn_deadline_settles_an_in_flight_mcp_tool_before_timeout`
- `kernel::registry::tests::tool_cancellation_settlement_timeout_is_frozen_bounded_and_panic_isolated`
- `tools/fault-fixture/tests/mcp.rs::cancelling_a_live_stdio_mcp_call_invalidates_its_session`
