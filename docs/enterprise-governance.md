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
| signed, revocable Skill lifecycle | atomic Domain Pack promotion and rollback |
| transport Principal plus durable Thread/Operation/Approval tenant ownership | tenant-partitioned Task, Secret, Artifact, and Domain Pack resources |
| durable Task DAG and fenced workers | event/timer/human-wait Workflow |
| durable Approval Inbox | ownership transfer or Human Handoff |
| digest-bound Thread handoff input | Human Handoff |
| reproducible Evaluation runner | Domain Pack release promotion |
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

## Current authority slice

ADR 0116 introduces trusted Turn authority and binds remote Memory scope,
Policy, and Tool execution to it. ADR 0117 adds authoritative schema-12 tenant
ownership and exact access fencing for Thread, Turn, recovery, archive,
handoff, and Protocol Operation state across Memory and SQLite stores. ADR
0118 adds schema-3 durable Approval ownership, exact tenant list/get/settle,
restart continuation, and non-inferential schema-2 migration.

This is not a complete multi-tenant claim. Tenant-scoped Task protocol
capabilities still fail closed and are not advertised until Task Graphs bind
ownership. Secret, Artifact, Domain Pack activation, quota, and retention
boundaries are also still open. The next compatible slice is Task Graph and
worker ownership; no tenant value will be inferred for legacy records.
