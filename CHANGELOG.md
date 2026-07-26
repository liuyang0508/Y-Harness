# Changelog

All notable changes to Y-Harness are documented in this file.

## Unreleased

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
