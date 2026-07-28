# ADR 0121: Domain Pack control plane above semantic Core

- Status: accepted
- Date: 2026-07-29

## Context

Y-Harness needs one enterprise execution foundation that can support customer
service, tutoring, research, coding, and other applications without embedding
those domains into the Agent Loop. Workflow, Skill, Tool, Policy, Evaluation,
and Schema selections must move together as an auditable release.

Independent extension registries are necessary but insufficient. A deployment
could otherwise combine versions that were never evaluated together, allow
the evaluator to approve its own evidence, activate a release in the wrong
tenant, lose rollback provenance, or execute after the installed inventory
drifted.

Putting this lifecycle into Core would couple every embedded runtime to an
enterprise deployment product. Putting it into a GUI, TUI, or configuration
file would make a client or mutable file authoritative.

## Decision

- Add optional workspace crate `y-harness-domain-pack` under `control/`.
  It depends on public Y-Harness authority types but is excluded from the
  packaged Core crate and adds no client protocol command.
- Define format-1 immutable snapshots with a stable Pack name, exact semantic
  version, canonical digest, and bounded unique component pins. Require at
  least one pinned Evaluation suite.
- Verify snapshots against a digest-bound complete installed inventory.
  Required component pins must match exactly; unrelated installed components
  may exist but remain part of the complete inventory digest.
- Define store-schema-1 release promotion as
  `installed → evaluated → approved`. Evaluation is terminal, failed evidence
  cannot advance, the suite digest must be pinned, and evaluator/approver
  identities must differ.
- Derive the optional tenant solely from trusted `AuthorityContext`. Partition
  release and activation keys by that tenant and expose no tenant selector in
  store methods.
- Define revision-CAS activation, explicit deactivation, and newest-entry-only
  rollback over the newest 32 retained releases. When full, history evicts only
  its oldest entry. Every activation and rollback rechecks an approved exact
  snapshot and verified inventory.
- Provide matching in-memory and SQLite stores. SQLite uses immediate write
  transactions, WAL plus `synchronous=FULL`, bounded bodies, projection/body
  consistency checks, and cross-process revision CAS.
- Add `DomainPackStore::bind`. It returns a constructor-only in-process
  execution binding only when tenant, approved active release, activation
  revision, and complete inventory digest still agree.
- Keep authorization outside the persistence primitive. The embedding control
  service must apply Policy roles before calling lifecycle methods; trusted
  attribution is not equivalent to permission.

## Consequences

- Domain specialization has an immutable, tenant-fenced promotion and rollback
  unit without adding domain behavior or deployment policy to Core.
- A Pack cannot claim evaluation against an unpinned suite, and inventory drift
  cannot silently reuse an old activation for a newly bound execution.
- The same Pack identity can exist independently in different tenants.
- A returned binding proves an observed in-process control-plane state. The
  host still must fence activation changes as required and lock or otherwise
  preserve the corresponding component inventory for the execution lifetime.
- Store schema 1 is a single-host control-plane baseline, not a multi-node
  consensus system.
- Protocol/CLI/service integration, external identity claims, registries,
  canary rollout, quotas, retention, durable Workflow waits, and
  domain-specific suites remain separate future work. ADR 0124 adds an
  optional exact-actor/tenant RBAC adapter without moving authorization into
  persistence or Core.

## Rejected alternatives

- Put Domain Pack lifecycle in Core: every embedding would inherit deployment
  governance and storage it may not use.
- Put lifecycle in each product client: clients would duplicate semantics and
  become authoritative.
- Treat a mutable directory or configuration file as a release: it cannot
  prove exact evaluated component content or atomic promotion.
- Record only required component digests at activation: unrelated inventory
  drift would remain invisible at execution binding.
- Let evaluation imply approval: enterprise separation of duty would be lost.
- Infer tenant from Pack name, filesystem path, or actor string: none is the
  trusted tenant authority.
