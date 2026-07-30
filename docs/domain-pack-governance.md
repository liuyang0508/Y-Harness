# Domain Pack governance

`y-harness-domain-pack` is an optional control-plane library above the
Y-Harness semantic Core. It governs how a domain specialization is described,
promoted, activated, and bound to an execution. It does not add business
behavior to the Agent Loop and is not an Engine client.

Run the zero-network public lifecycle example from the repository root:

```bash
cargo run -p y-harness-domain-pack --example governed_release
```

## Immutable release

A format-1 `DomainPackSnapshot` contains:

- one stable name and exact semantic release version;
- a bounded human-readable description;
- sorted, unique pins for Workflow, Skill, Tool, Policy, Evaluation, and
  Schema components;
- the exact version coordinate and lowercase SHA-256 of each component;
- a digest over the canonical snapshot.

Every Pack must pin at least one Evaluation suite. Sealing sorts the component
list, rejects duplicate kind/name identities, validates all bounds, and
computes the snapshot digest. Deserialized snapshots are never trusted without
`validate`.

`DomainPackInventory` describes the components actually installed by the
embedding host. `DomainPackSnapshot::verify` requires every Pack pin to match
exactly. Extra host components are allowed, but the digest of the complete
inventory is retained so later drift—including unrelated additions or
removals—invalidates an existing execution binding.

## Promotion state machine

```text
install ──> evaluated(pass) ──> approved ──> active
   │                │               │          │
   │                └─ failed: terminal        ├─ deactivate
   └─ immutable release identity               └─ bounded rollback
```

- Install is idempotent only for equivalent immutable content.
- Evaluation is terminal and its suite digest must match a pinned Evaluation
  component.
- Failed evaluation cannot be replaced or approved.
- The same actor cannot both evaluate and approve a release.
- Activation requires an installed, approved, inventory-verified release.
- Release and activation transitions use explicit revision compare-and-swap.
- Rollback can target only the newest activation-history entry and requires
  that target's current inventory to be verified again. The newest 32 prior
  releases are retained; adding another discards only the oldest entry.
- Repeating activation of the same release and identical complete inventory is
  idempotent.

All records are partitioned by the trusted optional tenant from
`AuthorityContext`; public methods accept no caller-authored tenant selector.
The same Pack name and version may safely exist in different tenants.

## Authorization

Attribution is not permission. `AuthorizedDomainPackStore` wraps any
`DomainPackStore` and authorizes every read and lifecycle transition before
the persistence implementation is called. Its `DomainPackAuthorizer` port is
synchronous and non-blocking; a denial or authorizer panic fails closed
without reading or mutating Pack state.

`DomainPackRoleAuthorizer` is the bounded reference policy. Grants match one
exact authenticated actor and one exact optional tenant—there are no wildcard,
cross-tenant fallback, or implicit local-process privileges. Its roles are:

| Role | Exact actions |
|---|---|
| `auditor` | inspect release and activation |
| `installer` | install |
| `evaluator` | evaluate |
| `approver` | approve |
| `operator` | activate, deactivate, rollback |
| `executor` | bind |
| `administrator` | all current actions |

Roles control permission, while the store retains lifecycle truth. In
particular, even an administrator cannot approve a release evaluated by the
same actor. An embedding service can replace the reference RBAC policy with an
external IAM or policy engine without changing storage or Core.

## Execution binding

Activation-time verification alone is vulnerable to component drift before
execution. `DomainPackStore::bind` therefore checks, in one store read:

1. the exact release remains installed and approved in the trusted tenant;
2. it is the active release;
3. the caller observed the current activation revision;
4. the complete inventory digest still equals the activated digest.

Only then does it return a constructor-only `DomainPackExecutionBinding`.
`to_execution_binding` converts that proof into the generic Engine
`ExecutionBinding`: snapshot digest becomes configuration identity, complete
inventory digest becomes environment identity, and activation revision and
tenant remain exact. An embedding host supplies it through
`TurnExecutionOptions`; State schema 13 records it once, keeps it out of Model
Context, and requires the same value for approval continuation. The binding is
not a remote bearer token or a substitute for the host's extension and
activation fencing. A later activation does not mutate an already issued
binding; the host decides whether an in-flight execution may finish its pinned
release.

## Storage

`MemoryDomainPackStore` and `SqliteDomainPackStore` implement identical
lifecycle rules. SQLite store schema 1 uses WAL, `synchronous=FULL`, immediate
write transactions, bounded JSON records, validated lookup projections, and
revision CAS. It supports single-host multi-process contention, not
distributed consensus.

Schema 1 is the first durable Domain Pack schema, so no migration from an older
Domain Pack database exists. A partial or unknown schema fails closed.

## Host responsibilities and current limits

`AuthorityContext` supplies trusted identity and tenant attribution; it does
not itself grant a role or authenticate caller-authored strings. The embedding
control service must authenticate the actor and tenant, then use
`AuthorizedDomainPackStore` with the reference RBAC authorizer or its own
`DomainPackAuthorizer`. It must also produce truthful component inventories
and keep installed components immutable for the binding lifetime.

The current package is a Rust library, not a Protocol v30 command, CLI, remote
control-plane service, identity provider, registry, or automatic config
mutator. It does not
implement canary rollout, distributed fencing, quotas, retention, background
Workflow/Handoff/Effect polling, automatic Human Handoff routing or Effect
execution, or domain-specific Evaluation content. Generic Temporal Driver API
2 can advance due Workflow waits, expired Handoff claims, and expired Effect
leases when an embedding host invokes it, but a
Domain Pack neither activates that driver nor acquires scheduling, ownership,
or channel authority merely by referencing those capabilities.

Task `Artifact` reference metadata is a separate boundary. It inherits the
tenant of its durable Task Graph, but Y-Harness does not yet store or authorize
the external blob named by the `uri`.

Turn and embedded Task-attempt execution binding are implemented in generic
Core types. This package only converts its exact activation proof into
`ExecutionBinding`; the embedding host must pass that value with trusted
authority to the Runtime or `Orchestrator`. Remote Protocol binding control
and detailed Task-binding evidence inspection are not claimed.
