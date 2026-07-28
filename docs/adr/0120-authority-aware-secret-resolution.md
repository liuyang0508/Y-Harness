# ADR 0120: Authority-aware Secret resolution and MCP session fencing

- Status: accepted
- Date: 2026-07-29

## Context

`AuthorityContext` already followed a Turn through State, Memory, Policy, Tool,
Approval, and Task execution. Secret Provider API 1 did not receive that
authority. Direct Model adapters therefore resolved one deployment-wide
credential, even when the owning Turn was tenant-scoped.

MCP Tool execution had a related boundary problem. `ToolContext` carried the
trusted authority, but `McpToolAdapter` discarded it and called a client method
that accepted only Tool arguments and cancellation. Allowing a tenant-scoped
call through a shared authenticated MCP session could reuse credentials or
server-side session state across tenants.

Adding tenant fields to a serialized `ModelRequest`, Secret reference, Tool
arguments, or Protocol command would make caller/provider data an identity
authority. Silently treating an API-1 Provider or shared MCP session as
tenant-safe would make an unproved isolation claim.

## Decision

- Advance the Secret Provider API from 1 to 2.
- Add `SecretProvider::resolve_as(request, authority)`. Its default validates
  the trusted authority, delegates to the existing `resolve` method only for
  unscoped operations, and fails closed for every tenant-scoped request.
- Keep `resolve` as the required compatibility method. Existing providers
  remain usable for unscoped embedded hosts without being relabeled
  tenant-aware.
- Add `TenantEnvironmentSecretProvider` for embedded hosts. It requires an
  exact trusted tenant and an explicit `(tenant, SecretReference)` mapping.
  It has no unscoped, cross-tenant, ambient-name, or deployment-wide fallback.
- Carry `AuthorityContext` in the in-process `ModelRequest`, validate it before
  Model execution, and pass it to direct HTTPS gateway and OpenAI Responses
  Secret resolution.
- Mark the authority field `serde(skip)`. It is not Model input, is absent
  from gateway/JSON-command payloads and request digests, and decodes to
  unscoped local-process authority outside the trusted Runtime boundary.
  Thread tenant fencing and exact requester evidence independently protect
  deferred approval continuation.
- Add `McpClient::call_tool_with_context`. Its default accepts unscoped calls
  through the existing cancellation method and rejects tenant-scoped calls
  before any client invocation. A client may override it only when its host
  actually partitions credentials and session state by the supplied trusted
  authority.
- Advance the exact client protocol to v23 because `initialize` advertises
  Secret Provider API 2. Protocol commands and Model Gateway API 7 remain
  unchanged.

## Consequences

- Direct Model credentials can be resolved from the same trusted tenant that
  owns the Turn without disclosing actor or tenant data to a Model provider.
- API-1-style custom Secret Providers and current shared stdio/HTTPS MCP
  clients remain source-compatible for unscoped use and fail closed for
  tenant-scoped use.
- The reference service's existing environment mappings remain unscoped.
  Service-configuration syntax for tenant credential maps is not introduced
  by this ADR.
- Tenant-partitioned remote MCP connection pools, OAuth token lifecycle,
  revocation, Secret-manager integrations, and per-tenant service assembly
  remain explicit future work. Protocol v23 is not a complete multi-tenant
  deployment claim.
- `ModelRequest` is a public pre-1.0 Rust struct, so external struct literals
  must initialize `authority`. Serialized Model request formats do not gain a
  field.

## Rejected alternatives

- Put `tenant_id` in `SecretRequest` or provider JSON: a caller-authored or
  serialized value is not trusted transport authority.
- Let legacy Providers resolve tenant requests: this silently converts one
  global credential into a tenant credential.
- Mutate one shared MCP session's credential before each call: concurrent
  calls, reconnects, and server-held session state make that boundary unsafe.
- Open a new MCP session for every tenant call in the generic adapter: the
  current client contract cannot prove partitioning, cleanup, or authentication
  lifecycle, and speculative pooling would add complexity without evidence.
- Serialize `ModelRequest.authority`: Models do not need execution identity,
  and doing so would leak internal principal and tenant metadata.
