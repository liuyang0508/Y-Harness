# ADR 0116: Carry trusted Turn authority to Policy and Tool execution

Status: accepted

## Context

The protocol authenticated a transport principal and attributed approval,
steering, and invocation Context records to its actor identity. `MemoryScope`
also accepted an optional `tenant_id`, but the Runtime did not bind that value
to trusted caller authority. A remote authenticated caller could therefore
select an arbitrary memory tenant even though Y-Harness did not implement
tenant-scoped State.

Certificate identity, human identity, and tenant membership are different
facts. Treating a certificate fingerprint or request field as all three would
create a false multi-tenant security claim.

## Decision

- Add one provider-neutral `AuthorityContext` containing the existing
  `ActorIdentity` and an optional validated tenant identity.
- Let the existing `ProtocolAuthorizer` resolve a trusted `AuthorityContext`
  after it grants a command. Its default preserves the transport actor without
  adding a tenant. Resolution is synchronous, panic-isolated, validated, and
  fails before command execution.
- Replace the Turn option dedicated only to approval attribution with the
  complete authority context.
- Before creating Turn State, bind Memory scope to the trusted tenant:
  inject an absent matching tenant, reject a mismatch, and reject tenant
  selection by an unscoped remote authenticated actor. The trusted embedded
  local-process boundary retains its explicit scope behavior.
- Pass the same authority to Policy evaluation and Tool execution. Compensation
  adapters preserve it when delegating an approved reversal.
- Keep existing durable State and Approval schemas unchanged in this slice.
  Tenant-scoped `PolicyDecision::Ask` and tenant-scoped approval recovery fail
  closed because their durable evidence does not yet bind a tenant. ADR 0118
  later adds that durable Approval boundary.

## Boundary

This is an authority-propagation foundation, not multi-tenant isolation.
Threads, Approval records, Task Graphs, Artifacts, Secrets, and protocol reads
are not yet partitioned by tenant. A host must not expose a tenant-scoped
network service as isolated until those durable resources are fenced and
migrated.

`AuthorityContext` carries identity and tenant attribution. It does not embed
RBAC rules; Policy remains the authority that decides what the resolved
principal may do.

The Client Protocol wire remains v19 because requests gain no caller-authored
identity fields. The Rust pre-1.0 `PolicyEngine` method gains the trusted
authority argument, and `ToolContext` gains the same value.

## Consequences

- Remote callers can no longer select an arbitrary Memory tenant without a
  trusted authority mapping.
- Policy and Tool implementations can enforce the same tenant and actor
  without reading transport-specific certificate data.
- Approval and durable State tenant fencing remain an explicit next migration
  instead of being implied by an optional Memory field.
- Existing custom protocol authorizers remain source-compatible through the
  default resolver; custom Policy implementations must accept the new
  authority argument.

## Verification

- `runtime::control::tests::trusted_tenant_is_injected_and_cannot_be_overridden`
- `runtime::control::tests::unscoped_remote_actor_cannot_select_a_memory_tenant`
- `runtime::tests::trusted_authority_reaches_policy_and_tool_execution`
- `runtime::tests::tenant_scoped_approval_is_durable_and_executes_only_after_settlement`
- `protocol::tests::protocol_authorizer_can_resolve_a_scoped_runtime_authority`
- `protocol::tests::authority_resolution_panic_fails_closed_before_command_execution`
- `protocol::tests::start_turn_carries_resolved_actor_into_durable_attribution`
