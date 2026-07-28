# ADR 0122: Durable tenant-exact Turn execution binding

- Status: accepted
- Date: 2026-07-29

## Context

A Domain Pack control plane can prove that an approved release and complete
installed inventory matched immediately before execution. That proof is not
useful after restart, approval pause, archive export, or incident review if the
Runtime does not bind it to the authoritative Turn.

Core must not depend on Domain Pack lifecycle types. Other deployment systems
also need to identify an immutable configuration and verified environment.
The binding must not become Model instructions, a caller-authored protocol
field, or a bearer credential.

## Decision

- Add a generic public `ExecutionBinding` with bounded issuer, deployment
  name/version, lowercase configuration and environment SHA-256 digests,
  non-zero issuer revision, and optional tenant.
- Accept it only through trusted in-process `TurnExecutionOptions`. Protocol
  `start_turn` intentionally has no corresponding input field.
- Validate the binding and require exact tenant equality with the Turn's
  trusted `AuthorityContext` before any Turn State is created.
- Advance State event and snapshot schemas to 13. Record at most one
  actor-attributed `ExecutionBinding` Item per Turn and reject duplicate or
  tenant-inconsistent projections.
- Treat the Item as content-free audit evidence. Context compilation and
  Model requests exclude it.
- Require the caller to present the exact recorded binding when resuming a
  pre-Tool approval boundary. Missing or substituted evidence fails before
  approval consumption or Tool execution.
- Advance Thread archive format to 3. Preserve bindings exactly and reject
  archive import into a different tenant when bound evidence exists. Unbound
  history retains the existing explicit target-tenant rebind behavior.
- Advance Client Protocol to v24 because `initialize` advertises State event
  and snapshot schema 13. Protocol clients may observe the new State Item but
  cannot author it.
- Let `DomainPackExecutionBinding::to_execution_binding` perform the optional
  control-plane conversion. Core does not import Domain Pack types or rules.
- Defer Task-attempt binding to a separate Task Graph schema migration. A Turn
  binding does not imply that every orchestrated Task attempt is bound.

## Consequences

- Incident review, SQLite recovery, snapshots, archives, and approval
  continuation retain the exact deployment and environment evidence observed
  at Turn start.
- A later Domain Pack activation does not mutate an in-flight Turn. The host
  still decides whether pinned work may finish and must preserve the
  corresponding immutable components.
- Tenant-bound execution evidence cannot be silently rewritten by portable
  archive import.
- External clients cannot forge governance evidence through the current wire
  contract.
- Existing schema-1 through schema-12 State stores require the existing
  backup-first offline migration. Historical events are not rewritten and
  disposable snapshots are rebuilt.
- This does not implement control-plane roles, distributed activation fencing,
  component locking, canary rollout, Workflow deployment, or Task-attempt
  binding.

## Rejected alternatives

- Store the Domain Pack object directly in Core: this would couple the semantic
  Runtime to one optional deployment product.
- Put binding data in `InvocationContext`: that evidence describes ephemeral
  reference input and is intentionally Model-visible through a separate
  channel.
- Put binding data in `TaskArtifact`: Artifacts are output references, not the
  execution environment authority for a Turn.
- Allow protocol callers to submit a binding: remote data is not trusted
  deployment-control evidence.
- Rewrite the binding tenant during archive import: this destroys historical
  truth and enables audit laundering.
- Infer a missing binding during approval resume: the active deployment may
  have changed after the original worker stopped.
