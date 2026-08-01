# ADR 0142: Typed Task execution capability matching at the claim boundary

- Status: accepted
- Date: 2026-08-01

## Context

The Task Graph already provides durable dependency state, revision CAS,
finite Worker leases, never-reused fencing tokens, heartbeat, retry, mailbox,
workspace isolation requests, and exact execution-binding evidence. Multiple
processes can safely compete for Tasks through the same SQLite coordinator.
Adding a coarse leader would serialize useful concurrency without answering a
more basic question: whether a Worker is permitted and equipped to execute a
particular Task.

Task descriptions, workspace modes, Worker names, and protocol request fields
are not trustworthy capability evidence. Treating any of them as such would
allow accidental or malicious privilege assertion. Conversely, making every
Task globally executable prevents a host from expressing that work requires a
specific governed facility such as Rust execution or browser access.

The claim path also performed lease-expiry and dependency-block maintenance in
memory but previously skipped coordinator CAS when no new claim was returned.
That could lose a valid maintenance-only transition.

## Decision

- Add `TaskCapabilitySet`, a canonical set of at most 64 exact validated names.
  Duplicate, malformed, oversized, and unknown-field input fails closed.
- Add optional `required_capabilities` to each immutable `TaskDefinition`.
  Omission is an empty set and therefore preserves universally executable
  Tasks. Matching requires the trusted Worker set to contain every Task
  requirement; there is no wildcard, prefix, hierarchy, implication, or text
  inference.
- Add explicit capability-aware claim APIs and
  `Orchestrator::with_worker_capabilities`. The embedding host is responsible
  for deriving this set from trusted deployment configuration or registry
  evidence.
- Keep existing claim APIs source-compatible but assign them an empty Worker
  set. They may claim universal Tasks only and cannot bypass a specialized
  requirement.
- Do not add a capability field to `claim_tasks`. Protocol Workers may not
  self-assert capabilities. Until a server-side authenticated Worker Registry
  exists, protocol claims are deliberately limited to Tasks with empty
  requirements.
- Return an internal mutation fact from the governed claim operation. Both the
  embedded Orchestrator and protocol service persist CAS whenever expiry or
  dependency propagation changed the Graph, even if the claim list is empty.
- Advance Task Graph schema 3 to 4 and client Protocol 30 to 31. The explicit
  backup-first migration accepts schema 1, 2, or 3. It preserves tenant and
  schema-3 attempt-binding evidence exactly, assigns empty requirements
  without inference, and rejects capability data carried under an older
  schema coordinate.

## Consequences and non-claims

- A Task can be durably declared incompatible with an unqualified Worker, and
  claim selection remains deterministic by priority and identity among the
  compatible ready set.
- Capability names are exact deployment contracts, not business roles or
  Policy decisions. A capability match does not bypass Policy, approval,
  sandbox, Secret, Tool, workspace, or execution-binding governance.
- Current protocol clients can create and inspect specialized Tasks but cannot
  execute them through `claim_tasks`. This is intentional fail-closed behavior,
  not silent fallback.
- The design does not claim a durable Worker Registry, remote attestation,
  heartbeat-based Worker discovery, fleet liveness, quotas, fairness,
  cross-Graph scheduling, placement optimization, leader election, consensus,
  or multi-node high availability.
- SQLite continues to provide per-Graph multi-process serialization on one
  shared filesystem. A future distributed scheduler must preserve the same
  Task lease, revision, capability, and execution-binding invariants rather
  than replace them with a coarse leadership assumption.

## Rejected alternatives

- Infer capabilities from Task descriptions or workspace mode: untyped,
  ambiguous, and spoofable.
- Accept capabilities in each protocol claim request: lets the claimant grant
  itself execution authority.
- Let legacy claim APIs ignore requirements: preserves source compatibility by
  creating a security bypass.
- Add a process leader before capability matching: reduces concurrency and
  still cannot decide which Worker is suitable.
- Store requirements outside the Task Graph: allows definition and scheduler
  truth to drift across restart.

## Evidence

- `orchestration::tests::capability_sets_are_canonical_bounded_and_strict`
- `orchestration::tests::claims_require_every_trusted_worker_capability`
- `orchestration::tests::maintenance_only_claim_reports_a_durable_graph_change`
- `orchestration::runner::tests::orchestrator_claims_only_tasks_supported_by_its_trusted_capabilities`
- `orchestration::runner::tests::orchestrator_never_claims_an_unsupported_task`
- `orchestration::coordinator::tests::task_capability_requirements_survive_sqlite_reopen`
- `orchestration::migration::tests::migration_preserves_schema_three_attempt_bindings`
- `orchestration::migration::tests::schema_three_migration_restarts_after_every_mutating_phase`
- `orchestration::migration::tests::schema_three_cannot_smuggle_schema_four_capability_requirements`
- `protocol::tests::protocol_workers_cannot_self_assert_capabilities_and_persist_maintenance_only_claims`
