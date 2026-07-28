# ADR 0125: Fixed-tenant reference-service authority

- Status: accepted
- Date: 2026-07-29

## Context

ADR 0120 added trusted tenant-aware Secret resolution, but the reference
stdio service still assembled every operation as unscoped local-process
authority. Operators could not bind one service deployment, its durable
State, or its direct Model credentials to a tenant through configuration.

The stdio transport authenticates only the process boundary. It cannot
truthfully select among multiple remote principals or tenants. Current stdio
and HTTPS MCP clients also share a configured session and therefore cannot
prove tenant isolation.

## Decision

- Add an optional additive service-schema-1 authority:

  ```json
  {
    "authority": {
      "type": "local_process_tenant",
      "tenant_id": "tenant-a"
    }
  }
  ```

- Interpret this as one trusted local process serving exactly one validated
  tenant. It is a deployment boundary, not user authentication, delegated
  identity, or a general multi-tenant routing table.
- Resolve local Protocol requests to that exact `AuthorityContext`. State,
  retained Operations, Approval, and Task access therefore use their existing
  tenant fences without accepting a tenant selector from a request.
- Run configured Evaluation cases under the same authority. A case's Memory
  scope must agree with the configured tenant through the existing Runtime
  control boundary.
- Use the authority-aware State archive methods for CLI export and import.
- Assemble direct OpenAI and HTTPS-gateway environment credentials through an
  exact one-tenant `TenantEnvironmentSecretProvider` and probe them with
  `resolve_as`. Unscoped configurations preserve the existing global
  environment allow-list provider.
- Reject any enabled configured MCP server when fixed-tenant authority is
  active. The current shared clients do not prove tenant-partitioned
  credentials or session state; disabled entries acquire no authority.
- Keep State, Approval, Task, Secret, Model Gateway, Protocol, archive, and
  service-configuration coordinates unchanged. The optional field is additive
  and omitted configurations retain their prior unscoped meaning.

## Consequences

- A separate reference-service process and data directory can now be deployed
  per tenant using only configuration and environment-variable mapping.
- Protocol-created Threads and Task Graphs, configured Evaluation, direct
  Model Secret resolution, and archive operations share one explicit
  authority instead of drifting between unscoped helper paths.
- Changing an existing unscoped deployment to fixed-tenant mode does not
  migrate or infer ownership. Existing unscoped records remain inaccessible
  under the tenant fence; operators must use a fresh data directory until an
  explicit ownership-transfer workflow exists.
- A fixed-tenant process still trusts every local caller that can write its
  stdin. Multi-principal authentication, principal-to-tenant mapping, general
  Secret-manager backends, tenant-partitioned MCP pools, credential rotation,
  and remote policy reload remain open.

## Rejected alternatives

- Add `tenant_id` to Protocol commands: request data is not transport
  authority and would enable caller-selected ownership.
- Infer tenant ownership from the data directory, environment-variable name,
  or Model configuration: none is authenticated identity evidence.
- Accept a tenant credential map in stdio mode: without a trusted
  multi-principal transport selector, unused map entries create the appearance
  of isolation without an enforceable route.
- Allow existing shared MCP clients because their Tool calls are serialized:
  serialization does not partition credentials, reconnect state, or remote
  session state.
- Rewrite existing unscoped rows during startup: ownership must never be
  inferred as a convenience migration.

## Evidence

- `reference_cli::service::tests::fixed_tenant_authority_is_exact_and_rejects_shared_mcp_sessions`
- `configured_json_command_grader_runs_an_isolated_real_evaluation`
- `fixed_tenant_service_binds_protocol_state_tasks_and_archives`
- `doctor_accepts_the_checked_in_https_gateway_template`
