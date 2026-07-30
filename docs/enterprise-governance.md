# Enterprise governance roadmap

This roadmap keeps Y-Harness domain-neutral. Customer service, tutoring,
research, coding, and other applications belong in independently governed
Domain Packs; none becomes a Core subsystem.

## Product and control boundaries

```text
Domain Pack control plane ── verifies/promotes/activates immutable snapshots
                                      │
                                      v
clients ── Typed Protocol ── Y-Harness Engine ── governed capability ports
                                      │
                                      └── authoritative systems through Tools
```

Client products are independent adapters over the same protocol:

```text
CLI
TUI
GUI                 Web · Desktop · Mobile
Conversational/LUI  Web Chat · Desktop Chat · IM
Voice/VUI
IDE Extension
API · SDK · Webhook
```

Interaction modes and delivery platforms may overlap. For example, a Web
product may expose both GUI and LUI. The list is a capability matrix, not a
reason to duplicate Runtime or State.

## Current truth

| Existing primitive | It does not yet prove |
|---|---|
| optional immutable Domain Pack snapshots plus exact actor/tenant role authorization, tenant-fenced promotion, activation, rollback, schema-13 Turn binding, schema-14 Runtime-bound Connector evidence, and schema-3 embedded Task-attempt binding | remote control-plane/binding-evidence exposure, external IAM integration, canary rollout, or multi-node control-plane HA |
| transport Principal plus durable Thread/Operation/Approval/Task and optional Domain Pack ownership | external Artifact blob authorization, quota, retention, or tenant-partitioned MCP sessions |
| authority-aware Secret Provider API, exact embedded tenant/reference adapter, and fixed one-process/one-tenant reference-service assembly | multi-principal tenant credential routing or a general Secret-manager backend |
| durable Task DAG and fenced workers plus schema-1 Workflow signal/timer/retry waits, safe-boundary definition migration, and a bounded host-driven due-wait tick | a resident timer service, automatic Task execution/retry, or durable compensation plan |
| schema-1 Human Handoff with same-tenant subject validation, priority queue, actor-bound commands, finite lease, unique claim fence, Memory/SQLite CAS, and bounded host-driven claim expiry | automatic channel routing, Turn suspension, Workflow wakeup, a resident expiry service, outbox delivery, or proof that a local-process actor is a person |
| durable Approval Inbox | ownership transfer; approval authorizes a decision but does not claim conversational work |
| digest-bound Thread handoff input | human ownership; the format prepares bounded summarizer input only |
| reproducible Evaluation runner and digest-bound Pack evaluation evidence | domain-specific suites, canary evidence, or automatic promotion policy |
| SQLite recovery and multi-process CAS | multi-node high availability |

## Ordered implementation

1. **Authority and tenant fencing**
   - Resolve transport identity into trusted actor and tenant authority.
   - Bind State, Approval, Task, Memory, Secret, Artifact, and recovery access
     to that authority.
   - Prove cross-tenant denial for every read and mutation.
2. **Domain Pack lifecycle**
   - Compose exact Workflow, Skill, Tool, Policy, Eval, and Schema coordinates.
   - Keep acquire, verify, install, evaluate, approve, activate, deactivate,
     and rollback distinct.
   - Pin an immutable Pack identity and digest to every execution.
3. **Authoritative data**
   - Access systems of record through governed Connector Tools.
   - Carry source, resource, version, freshness, actor, tenant, and idempotency
     evidence without turning Memory or RAG into business authority.
4. **Persistent Workflow and Human Handoff**
   - Add durable signals, timers, waits, retries, compensation, and migration
     above the existing Task execution layer.
   - Model approval and ownership transfer as different state machines.
   - Keep channel routing, artificial work suspension, and business actions
     behind explicit host adapters rather than implicit Handoff side effects.
5. **Evaluation and promotion**
   - Gate Pack releases with Core conformance, domain scenarios, fault and
     recovery tests, tenant-isolation tests, approval, canary, and rollback.
6. **High availability and product adapters**
   - Add distributed fencing, durable outbox/webhooks, quotas, retention, and
     disaster recovery before claiming multi-node availability.
   - Prefer API/SDK/Webhook before new GUI products so remote authority,
     resumability, idempotency, and event delivery are exercised first.

## Current authority and Domain Pack slice

ADR 0116 introduces trusted Turn authority and binds remote Memory scope,
Policy, and Tool execution to it. ADR 0117 adds authoritative schema-12 tenant
ownership and exact access fencing for Thread, Turn, recovery, archive,
handoff, and Protocol Operation state across Memory and SQLite stores. ADR
0118 adds schema-3 durable Approval ownership, exact tenant list/get/settle,
restart continuation, and non-inferential schema-2 migration. ADR 0119 adds
schema-2 Task Graph ownership across Tasks, leases, messages, and Artifact
reference metadata. ADR 0120 carries trusted authority into Secret resolution
and fails closed for tenant-scoped legacy Providers and shared MCP sessions.
ADR 0122 records an exact generic execution coordinate on each governed Turn.
ADR 0123 advances Task Graphs to schema 3 and persists the same coordinate per
exact Task attempt before executor entry, retaining governed retries and
preventing downgrade to unbound execution.
ADR 0124 adds a fail-closed authorization adapter around the optional Domain
Pack store. Its reference RBAC policy matches exact actor, tenant, and action;
all reads and transitions are denied before persistence on mismatch or policy
panic. The store still owns promotion rules and evaluator/approver separation.
ADR 0125 lets one reference-service process declare one exact local-process
tenant. Protocol State/Approval/Task access, configured Evaluation, archives,
and direct Model environment Secrets share that authority. Enabled shared MCP
configuration fails before launch. This is deployment partitioning, not
multi-principal authentication or tenant routing.

ADR 0127 adds a separate Workflow Run aggregate above Task execution. It
persists exact signal, timer, and explicit retry waits; fences every wake to
the current wait identity; binds idempotent command identities to typed-command
digests; allows same-name monotonic definition migration only at a durable
wait; and exposes the tenant-fenced lifecycle through Protocol v27. The
reference service stores Runs independently in `workflows.db`.

ADR 0128 adds a separate Human Handoff aggregate and Protocol v28 surface.
Creation verifies an existing same-tenant Thread or Workflow Run; queue reads
are bounded and stable; claims are owned by the trusted actor, expire at an
exclusive server-time boundary, and use a never-reused claim identity.
Revision CAS, actor-and-content-bound command identities, immutable transition
digests, projection validation, and tenant-partitioned Memory/SQLite storage
make uncertain retries and competing operators explicit. The reference
service stores Handoffs independently in `human-handoffs.db`. It deliberately
does not route a channel, pause a Turn, wake a Workflow, execute business
actions, or claim that `LocalProcess` authenticates a human. The optional
reference-host Temporal loop can only expire the existing claim fence.

ADR 0129 added embedded Temporal Driver API 1. An embedding host supplies one
trusted authority and Unix time to a bounded tick. The driver scans
authoritative Workflow and Handoff rows by tenant-local identity, computes all
command identities before mutation, and advances due fences only through the
existing CAS commands. Cursors are disposable; losing one restarts discovery
without losing durable time state. The driver owns no scheduler database,
background lifecycle, Task execution, channel route, or outbox.

ADR 0130 adds the host lifecycle without weakening that Core boundary.
Reference-service config schema 1 accepts an optional strict `temporal`
policy; omission stays disabled. When enabled, the service supplies its fixed
Authority and wall clock, skips missed cadence ticks, retains only a
process-local cursor, emits bounded degraded/recovered diagnostics, and stops
maintenance before Protocol Operations and MCP clients. Protocol callers
cannot choose time, cursor, or cadence.

ADR 0133 adds a separate schema-1 Effect Ledger and advances Protocol to v29.
One immutable tenant/capability/operation/idempotency coordinate and bounded
request is committed before execution. A finite worker lease owns one exact
attempt; expiration becomes `unknown`, never a blind retry. Only a receipt or
explicit reconciliation of the exact attempt/lease can settle it as applied
or authoritatively not applied. Memory/SQLite parity, revision CAS,
actor/content-bound commands, bounded scans, read-only service preflight, and
restart recovery are executable. Temporal Driver API 2 optionally advances
expired Effect leases through the same fenced command boundary. No Core loop
executes pending Effects or verifies external receipt truth.

ADR 0134 adds optional embedded Governed Effect Executor API 1 without moving
deployment lifecycle into Core. A host registers exact-versioned Connectors
with frozen operation and idempotency contracts, installs an explicit
pre-Claim Policy, supplies trusted time and Authority, and invokes bounded
pending sweeps. Exact Claim replay never authorizes a second Connector entry.
Once dispatch begins, error, panic, timeout, and cancellation become durable
`unknown` outcomes. Reports omit request, idempotency, receipt, and Provider
content. The API neither polls autonomously nor verifies Connector truth;
operators still own credentials, containment, reconciliation, and service
availability.

ADR 0135 adds optional embedded Governed Effect Reconciler API 1 without
weakening the Ledger's fail-closed uncertainty. A host registers exact
authoritative read-only lookup Connectors, installs a default-deny pre-query
Policy, supplies trusted time and Authority, and invokes bounded unknown
sweeps. Valid target evidence settles through the existing
revision/attempt/lease CAS. Missing, malformed, failed, panicked, timed-out,
cancelled, or still-unknown observations perform no mutation. Duplicate
cross-host queries are explicitly allowed only under the read-only contract;
the host still owns credentials, containment, cadence, receipt truth, and
service availability.

ADR 0136 adds exact JSON Effect Connector protocol 1 and separate brokered
execution/read-only-reconciliation adapters. Any language may implement the
strict bounded stdin/stdout contract, while Process Broker still owns
shell-free launch, cleared environment, isolation metadata, cancellation, and
resource bounds. The adapters return into the existing Policy and Ledger
boundaries; they do not install reference-service configuration, polling,
credentials, or trust in Connector assertions.

ADR 0121 adds the independent `y-harness-domain-pack` control-plane crate.
Format-1 snapshots pin exact components and a mandatory Evaluation suite.
Store schema 1 makes release and activation identity tenant-partitioned,
records terminal evaluation plus independent approval, and uses cross-process
SQLite CAS for activation, deactivation, and bounded rollback. A
constructor-only execution binding is issued only when the approved active
release, full inventory digest, activation revision, and tenant still agree.

ADR 0122 adds a Domain-Pack-neutral Engine `ExecutionBinding`. The control
plane converts its proof into that content-free record; Runtime persists it
once per Turn, excludes it from Model Context, checks tenant equality, and
requires exact evidence on approval resume. This closes the Turn side of
ordered item 2. ADR 0123 closes the embedded Task side: the same generic
binding is committed with the exact lease before Workspace/executor entry,
retained after expiry and settlement, and required on every later retry once
governance begins.

Domain Pack lifecycle remains deliberately outside Core and the v29 client
protocol. The embedding control service must authenticate the trusted actor
and tenant, select the reference RBAC policy or provide an external
authorizer, collect truthful component inventories, and keep the binding valid
for the assembled execution. No customer-service, tutoring, coding, or other
business behavior is present.

This is not a complete multi-tenant claim. Task `Artifact` records contain
only bounded reference metadata (`uri`, digest, media type, and size) inside a
tenant-fenced Graph; Y-Harness does not yet store or authorize the external
blob addressed by that URI. Multi-principal tenant routing, general
Secret-manager integration, tenant-partitioned MCP sessions, a configured
reference-service Effect execution/reconciliation consumer, receipt
verification, reconciliation cadence/backoff, Workflow compensation planning,
automatic Human Handoff
channel routing/outbox delivery, quota, retention, canary rollout, and
multi-node control-plane availability remain open.
