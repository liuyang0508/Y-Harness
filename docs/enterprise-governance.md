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
| durable Task DAG and fenced workers | event/timer/human-wait Workflow |
| durable Approval Inbox | ownership transfer or Human Handoff |
| digest-bound Thread handoff input | Human Handoff |
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

Domain Pack lifecycle remains deliberately outside Core and the v26 client
protocol. The embedding control service must authenticate the trusted actor
and tenant, select the reference RBAC policy or provide an external
authorizer, collect truthful component inventories, and keep the binding valid
for the assembled execution. No customer-service, tutoring, coding, or other
business behavior is present.

This is not a complete multi-tenant claim. Task `Artifact` records contain
only bounded reference metadata (`uri`, digest, media type, and size) inside a
tenant-fenced Graph; Y-Harness does not yet store or authorize the external
blob addressed by that URI. Multi-principal tenant routing, general
Secret-manager integration, tenant-partitioned MCP sessions, durable Workflow
waits/timers, Human Handoff, quota, retention, canary rollout, and multi-node
control-plane availability remain open.
