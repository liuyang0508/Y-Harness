# Changelog

All notable changes to Y-Harness are documented in this file.

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
  read-only Approval/Task inspection;
- MIT OR Apache-2.0 licensing.

Supported evidence is limited to the exact release commit and CI platforms.
Linux and Windows do not yet have strong OS sandbox brokers. Direct vendor
model adapters are outside this release; use the exact HTTPS Model Gateway v2
contract or embed a `LanguageModel`. SQLite coordination is single-host, not
distributed consensus. See [release readiness](docs/release-readiness.md) for
the complete boundary.
