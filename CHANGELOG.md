# Changelog

All notable changes to Y-Harness are documented in this file.

## Unreleased

- added dispatch-time executable SHA-256 locks for one-shot Process Brokers,
  with frozen measured/unmeasured integrity evidence, regular-file and 256 MiB
  bounds, exact-path enforcement, per-request cancellation/deadline accounting,
  drift rejection before child entry, and restoration recovery; reference
  Effect execution and reconciliation now require the lock while atomic OS exec
  binding, transitive dependency integrity, and credential custody remain open;
- added an opt-in reference-service Effect consumer lifecycle with independently
  configurable execution and reconciliation loops, separate exact Connector
  registries and non-empty allowlists, explicit trust origins and frozen
  contracts, bounded cadence/backoff/concurrency/timeouts, disposable cursors,
  content-free health transitions, ordered shutdown, real subprocess
  degrade/recover coverage, and restart proof that terminal Effects are not
  replayed; Core remains task-free and the Effect Ledger remains authoritative;
- added embedded Governed Effect Executor API 1: exact-versioned Connector
  registration, frozen operation/idempotency contracts, default-deny
  pre-Claim Policy, bounded host-driven pending sweeps, deterministic
  actor/tenant-bound Claim identities, finite execution deadlines,
  panic/cancellation isolation, post-dispatch fail-closed `unknown`
  settlement, source-ordered bounded concurrency, and content-free reports;
  Core still owns no polling lifecycle, Channel implementation, credential
  store, receipt verifier, or automatic reconciliation;
- added independent Effect Ledger schema 1 and Protocol v29 for durable,
  tenant-scoped external side-effect intent: immutable bounded requests,
  operation/idempotency uniqueness, finite fenced worker leases, fail-closed
  `unknown` outcomes, explicit authoritative reconciliation, content-free
  receipts, Memory/SQLite parity, read-only preflight, and restart recovery
  through `effects.db`;
- advanced embedded Temporal Driver API to 2 with optional bounded Effect
  lease expiry. Exact expiration becomes `unknown`; the driver neither
  executes nor blindly retries an external effect;
- added embedded Temporal Driver API 1 with host-supplied time, bounded
  tenant-local identity scans over authoritative Workflow/Handoff aggregates,
  disposable cursors, fail-closed extension-page validation, deterministic
  actor-and-fence command identities, exact-boundary wake/expiry, and
  content-free applied/duplicate/fenced/failed settlement; no background
  thread, second scheduler database, Protocol, or durable-schema change;
- added independent Human Handoff schema 1 and Protocol v28 for durable
  ownership transfer over existing same-tenant Threads or Workflow Runs:
  actor-and-content-bound idempotency, revision CAS, stable priority queue,
  finite authenticated-owner leases, never-reused claim fences,
  tenant-partitioned Memory/SQLite coordination, conditional permissions,
  projection/digest validation, and restart recovery through
  `human-handoffs.db`;
- added independent Workflow Run schema 1 and Protocol v27 above the durable
  Task Graph: revision-CAS commands with content-bound idempotency, fenced
  signal/timer waits, explicit retry waits, safe-boundary definition migration,
  tenant-partitioned Memory/SQLite coordination, Task-completion proof,
  conditional protocol capabilities, bounded transition evidence, and
  restart recovery through `workflows.db`;
- added State/snapshot schema 14 and Thread archive format 4 with bounded
  Runtime-bound Connector evidence: registered Tool/origin, trusted
  actor/tenant, exact output SHA-256, atomic ToolResult persistence,
  recovery-time provenance validation, Model-hidden projection, backup-first
  schema-1 through schema-13 migration, and Protocol v26;
- added Task Graph schema 3 with append-only tenant-exact execution-binding
  evidence per Task attempt, trusted Orchestrator authority, persistence before
  Workspace/executor entry, retry anti-downgrade, exact claim propagation,
  backup-first schema-1/schema-2 migration, and Protocol v25;
- added Secret Provider API 2 with trusted per-Turn authority, exact
  tenant/reference environment resolution, non-serialized Model authority,
  direct HTTPS/OpenAI credential fencing, fail-closed legacy Providers and
  shared MCP sessions, and Protocol v23;
- added Task Graph schema 2 with immutable trusted tenant ownership,
  tenant-partitioned Graph identities, exact-tenant Memory/SQLite and Protocol
  fencing for the complete worker/lease/mailbox lifecycle, validated SQLite
  lookup projections, backup-first schema-1 migration without inferred
  ownership, and Protocol v22;
- added Approval Inbox schema 3 with immutable trusted tenant ownership,
  exact-tenant Memory/SQLite and Protocol fencing, tenant-aware Runtime
  approval/recovery flow, validated SQLite lookup projections, backup-first
  schema-2 migration without inferred ownership, and Protocol v21;
- added a bounded Codex `exec --json` adapter to the independent external
  benchmark runner, preserving JSONL evidence while recording unavailable
  product metrics as unavailable rather than inferred;
- added Grok Build's official Rust Agent/Harness source as an audited baseline,
  while keeping the Grok 4.5 Model coordinate distinct from supporting xAI
  weights, prompt, SDK, and protocol evidence;
- added a bounded Grok Build headless JSON adapter with isolated bare homes,
  private prompt-file cleanup, exact Model/effort/Turn controls, and truthful
  observed-Model and complete-cost evidence.

## 0.1.0 - 2026-07-25

Initial public baseline:

- eleven-layer, provider-neutral Agent Harness Runtime;
- bounded Agent Loop with durable State, policy, approval, verification,
  observation, evaluation, cancellation, recovery, and model failover;
- typed Model, Tool, MCP, Memory, Skill, Secret, Workspace, Grader, Verifier,
  and Process Broker extension contracts;
- Protocol v10 stdio and mandatory-mTLS hosts;
- durable Task Graph coordination with leases, fencing, Mailbox, workspaces,
  and authenticated worker commands;
- `yh init`, `yh doctor`, `yh serve`, deterministic demo, backup-first
  migration commands, and regression evaluation;
- independently installable `yh-tui` full-screen product client over Protocol
  v10, with authoritative State projection, streaming, cancellation, and
  read-only Approval/Task inspection, config preflight, and bounded Engine
  startup diagnostics;
- governed persistent-service assembly for direct OpenAI Responses, shell-free
  JSON-command Tools, exact-selected MCP Tools, and Agent Memory Hub Context,
  preserving Runtime-owned Policy, State, and sequential Tool scheduling;
- MIT OR Apache-2.0 licensing.

Supported evidence is limited to the exact release commit and CI platforms.
Linux and Windows do not yet have strong OS sandbox brokers. OpenAI Responses
is the only direct vendor adapter; other vendors use the exact HTTPS Model
Gateway v2 contract or an embedded `LanguageModel`. SQLite coordination is
single-host, not distributed consensus. See
[release readiness](docs/release-readiness.md) for the complete boundary.
