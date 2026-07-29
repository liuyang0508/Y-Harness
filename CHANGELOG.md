# Changelog

All notable changes to Y-Harness are documented in this file.

## Unreleased

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
