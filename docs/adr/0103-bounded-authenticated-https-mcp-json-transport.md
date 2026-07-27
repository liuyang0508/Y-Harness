# ADR 0103: Bounded authenticated HTTPS MCP JSON transport

- Status: accepted
- Date: 2026-07-28

## Context

Y-Harness exposed a provider-neutral `McpClient` and a governed persistent
stdio implementation, but the reference service could not connect directly to
an operator-supplied remote MCP endpoint. Requiring a local bridge process for
every remote server weakens deployment ergonomics and makes the answer to
"append an MCP endpoint without changing Rust" incomplete.

The official MCP Streamable HTTP client also contains policies that a Harness
must not inherit accidentally. Its default SSE reconnection is unbounded, and
expired-session recovery may replay the in-flight request. That is unsafe for
an arbitrary Tool call whose first effect may have occurred before the
transport failure. Generic response materialization also does not establish
Y-Harness's independent byte ceiling.

## Decision

- Add an optional `https-mcp` Cargo feature and public
  `HttpsJsonMcpConfig`/`HttpsJsonMcpClient`. The client implements the existing
  provider-neutral `McpClient`; Tool, Memory, Policy, State, and Agent Loop
  contracts do not change.
- Support the stateless JSON-response subset of MCP Streamable HTTP first.
  Use the official Rust MCP SDK for initialization, lifecycle, JSON-RPC
  correlation, catalog pagination, and Tool result decoding. Reject SSE
  responses rather than claiming an unbounded stream implementation.
- Require an exact HTTPS endpoint with a host and no URL userinfo, query, or
  fragment. Disable redirects, ambient proxies, automatic HTTP retry, Referer,
  and TLS below 1.2. Allow an optional project-contained exclusive root-CA PEM
  bundle.
- Resolve one explicit `SecretReference` through a `SecretProvider` for each
  HTTP operation. The reference and environment-variable name are
  configuration; credential bytes never enter serialized config, Debug,
  diagnostics, or errors. Keep the temporary UTF-8 bearer copy zeroizing.
- Bound encoded JSON requests to 2 MiB, responses to an operator-selected
  1–16 MiB, session IDs to 4 KiB, Tool arguments/results and catalog pages by
  the existing MCP limits, and every lifecycle/call by explicit connect and
  request timeouts.
- Disable SDK SSE reconnect and expired-session reinitialization. A failed
  session is invalidated so a later independent operation may reconnect, but
  the failed Tool call is never resubmitted automatically.
- Add a separate additive `https_mcp_servers` service list. Preserve the
  existing `mcp_servers` stdio shape. IDs must be unique across both lists.
  Disabled entries perform no secret lookup, network access, discovery,
  Policy registration, or Memory wiring.
- Reuse the same exact `tools.namespace` and non-empty `tools.allow` selection.
  Missing requested tools reject the complete registration. Agent Memory Hub
  or another `MemoryProvider` adapter may reference either transport through
  the same MCP identity map.
- Include `https-mcp` in the operator install and release binary while keeping
  it optional for library embeddings. Keep service configuration schema 1:
  the new root list is optional and no existing field changes meaning.
- Do not add OAuth discovery, arbitrary headers, cookies, SSE, redirects,
  proxies, transparent Tool retry, load balancing, hot reload, or remote
  server catalog discovery in this slice.

## Consequences

An operator can add an authenticated remote MCP Tool endpoint and exact Tool
allow-list without writing or compiling Rust. Remote Tools still enter the
ordinary registry and execute only after normal Policy/Approval checks.

Compatibility is intentionally narrower than the complete Streamable HTTP
ecosystem: a server that returns `text/event-stream` is rejected. A future SSE
implementation must bound bytes before event allocation, define reconnect
semantics that cannot replay effects, and add its own fault tests before this
limitation is removed.

Environment-backed bearer authentication is the only shipped remote
credential flow. OAuth and managed identity require separate `SecretProvider`
and transport decisions; they are not guessed from a `WWW-Authenticate`
response.

## Rejected alternatives

- Launch a local remote-MCP bridge: keeps deployment dependent on an ambient
  executable and does not provide a native endpoint contract.
- Enable the SDK defaults unchanged: expired-session recovery may replay an
  uncertain Tool effect, and SSE reconnection has no Y-Harness policy bound.
- Accept HTTP for local development: config drift could silently move a
  credential-bearing endpoint to plaintext.
- Reuse the stdio object with mutually exclusive fields: this would make the
  existing strict configuration shape ambiguous. A separate list is additive
  and keeps each transport's authority explicit.
- Cache a raw API key in service configuration: violates the Secret Provider
  custody boundary.

## Evidence

- `transport::mcp_https::tests::configuration_rejects_ambient_or_unbounded_endpoint_authority`
- `authenticated_private_https_mcp_json_round_trip`
- `service_doctor_registers_exact_remote_https_mcp_tools`
- `enabled_https_mcp_reports_the_missing_optional_feature_before_secret_access`
- `disabled_https_mcp_acquires_no_feature_secret_or_network_authority`
- `invalid_https_mcp_endpoint_is_rejected_before_secret_access`
- isolated `https-mcp`, all-feature, and zero-default workspace gates

## Related decisions

- [ADR 0043: atomic namespaced MCP Tool registration](0043-atomic-namespaced-mcp-tool-registration.md)
- [ADR 0054: bounded process settlement](0054-bounded-process-settlement.md)
- [ADR 0076: governed service capability assembly](0076-governed-service-capability-assembly.md)
- [ADR 0088: explicit MCP activation and extension locks](0088-explicit-mcp-activation-and-extension-locks.md)
