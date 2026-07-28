# ADR 0124: Exact Domain Pack role authorization

- Status: accepted
- Date: 2026-07-29

## Context

ADR 0121 deliberately kept Domain Pack authorization outside both semantic
Core and the persistence primitive. Trusted `AuthorityContext` records who
acted and in which tenant, but attribution alone does not grant permission.
Leaving every embedding service to remember a separate check before each
store method would make omissions possible, especially for reads and
execution binding.

The control plane needs a safe reference policy without assuming that one
built-in RBAC model can replace enterprise IAM. Authorization must not reveal
whether a release exists, mutate persistence before denial, trust roles sent by
a client, or weaken the store's independent promotion invariants.

## Decision

- Add `DomainPackAction` and constructor-only `DomainPackAuthorization` as the
  exact actor, optional tenant, action, Pack, and optional release input to a
  pluggable `DomainPackAuthorizer`.
- Keep the authorizer synchronous and non-blocking. Catch policy panics and
  fail closed before invoking persistence.
- Add `AuthorizedDomainPackStore`, a generic adapter over any
  `DomainPackStore`. It authorizes all nine methods, including release and
  activation reads, before delegation. The wrapped store is not exposed
  through a bypass accessor.
- Add a bounded reference `DomainPackRoleAuthorizer`. Its grants match an
  exact current actor and exact optional tenant. It has no wildcard, tenant
  fallback, role inheritance, or implicit local-process privilege.
- Define narrow auditor, installer, evaluator, approver, operator, and
  executor roles plus an explicit administrator role. Multiple roles may be
  assigned to one exact principal.
- Retain evaluator/approver separation, revision CAS, tenant fencing, and all
  other promotion rules in the store. Authorization cannot relax lifecycle
  truth, including for administrators.
- Return a bounded `Forbidden` error containing only the requested action and
  caller-supplied Pack identity. It does not disclose stored release or
  activation content.
- Keep Domain Pack format schema 1, store schema 1, State schema 13, Task
  schema 3, and Protocol 25 unchanged; this adapter adds no durable or wire
  field.

## Consequences

- A host can wrap Memory, SQLite, or a future store once and cannot
  accidentally omit authorization for one lifecycle method through that
  handle.
- Exact tenant matching prevents a role granted in one tenant from becoming a
  fallback grant in another tenant.
- External IAM and policy engines can implement the same port, including
  through a shared trait object, without changing Domain Pack storage or
  semantic Core.
- Authentication, identity-to-role synchronization, signed policy receipts,
  remote control-plane exposure, and distributed policy availability remain
  host or future control-service responsibilities.
- A process that deliberately retains and calls the unwrapped store still owns
  that authority. Rust library composition cannot replace process isolation
  or a properly constructed service boundary.

## Rejected alternatives

- Add roles to `AuthorityContext`: this would make a deployment concern part
  of every Engine operation and would trust stale or caller-authored claims.
- Put role checks inside each persistence implementation: Memory, SQLite, and
  future stores could drift, and external policy integration would be coupled
  to storage.
- Authorize only mutations: release and activation reads can disclose tenant
  control-plane state.
- Treat administrator as bypassing promotion invariants: permission to request
  an operation is not permission to falsify evaluation or separation-of-duty
  evidence.
- Allow wildcard actors or tenant fallback in the reference policy: concise
  configuration would come at the cost of ambiguous cross-tenant authority.

## Evidence

- `authorization::tests::exact_roles_cover_the_complete_lifecycle`
- `authorization::tests::exact_tenant_grants_have_no_fallback_and_denial_does_not_mutate`
- `authorization::tests::authorizer_panic_fails_closed_before_store_mutation`
- `authorization::tests::store_separation_of_duty_still_applies_to_administrators`
- `authorization::tests::role_grants_reject_empty_invalid_and_duplicate_entries`
