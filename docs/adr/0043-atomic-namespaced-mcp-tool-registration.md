# ADR 0043: Atomic namespaced MCP Tool registration

- Status: Accepted
- Date: 2026-07-25

## Context

Y-Harness had a bounded, persistent MCP client and used it for the first-party
Agent Memory Hub adapter, but a general MCP server's discovered tools could not
enter the Kernel `ToolRegistry`. A host would have needed bespoke wrappers,
which risks bypassing common naming, origin, Policy, approval, State, and Agent
Loop behavior.

Registering discovery results one by one is also unsafe. A late invalid
descriptor or collision would leave a partial server catalog active. Calling
extension-provided synchronous metadata methods without a panic boundary could
unwind the host during registration.

## Decision

- Add `register_mcp_tools`, which discovers a complete MCP catalog through the
  existing bounded `McpClient`.
- Require an operator-selected portable namespace and form exact registry names
  as `<namespace>.<remote-name>`. Never lowercase, escape, truncate, or otherwise
  rewrite a server's tool name.
- Require every final name to satisfy the Kernel's 64-byte portable Tool
  identity contract. Use a deterministic fallback description only when the
  server omits or blanks it.
- Stage the entire catalog and commit it through
  `ToolRegistry::register_batch`. Any invalid schema/name, in-batch duplicate,
  existing collision, or descriptor panic rejects the batch without mutation.
- Retain the caller-supplied `CapabilityOrigin` for every registered adapter.
- Execute calls through the ordinary `Tool` contract. Model proposal, Policy,
  approval, cancellation/deadline, result bounds, State evidence, and
  Verification remain owned by the Runtime.
- Catch synchronous model identity and Tool descriptor panics at registration
  and return content-free `InvalidCapability` errors.

## Consequences

MCP is now an extensible transport for normal Kernel Tools, not a parallel
execution authority. Multiple servers can expose the same remote name under
different namespaces, while collisions stay explicit. Skill manifests may
depend on the resulting stable names exactly like built-in or process-backed
Tools.

Registration captures one catalog snapshot. Automatic refresh, removal, and
hot replacement are not implied; those require an explicit versioned lifecycle
and in-flight-call policy.

## Rejected alternatives

- Expose raw MCP names globally: independent servers would collide and lose
  provenance.
- Silently normalize invalid names: the model-visible identity would diverge
  from auditable server metadata.
- Register incrementally: partial catalogs create nondeterministic capability
  sets after a failed startup.
- Let MCP calls bypass `ToolRegistry` or Policy: transport choice must not grant
  execution authority.
