# Release readiness

This document separates executable evidence from roadmap claims. Y-Harness is
not release-ready while any blocking row remains open.

## Current evidence

| Gate | Evidence | Current result |
|---|---|---|
| Minimum compiler | Rust 1.88 `check`, Clippy, tests, docs | passing |
| Feature isolation | zero-default core, each optional feature, and all features | passing |
| Deterministic tests | 264 library, 2 CLI, 7 Engine process/service, 10 TUI unit/render, and 2 private-gateway TLS integration tests | passing |
| Full-screen TUI PTY | demo and configured Engine modes; real Turn, durable State, alternate screen and bracketed-paste restoration | debug and release binaries passing |
| Installed operator path | isolated-prefix Engine and TUI installs; version, init, doctor, persistent service, demo, Task DAG and Mailbox | passing; TUI install contains only `yh-tui`; Task Graph terminal at revision 6 |
| Distribution package | `cargo package --locked -p y-harness`, clean-room crate verification | current candidate passing locally; exact clean-tree package rechecked before commit handoff |
| Real memory integration | Agent Memory Hub stdio MCP round trip under macOS Seatbelt, network denied, offline embeddings | passing |
| Dependency security | `cargo-audit 0.22.2 --deny warnings` over 286 locked crates | passing |
| State performance | 1,000 events, 5 samples, SQLite WAL + FULL | 71.97 ms append; 2.64 ms full projection; 2.22 ms snapshot load |
| State migration | schemas 1/2/3/4 → 5, immutable-history backup-first path | schema-1/2/3/4 sources, coordinate- and SHA-256-bound, restartable; prior maximum-size schema-4 measurement retained as historical evidence |
| Approval migration | schema 1 → 2, 256 records / 133,038,080 record bytes | 844.781 ms; backup-first, SHA-256-bound, restartable |
| JSON authority | 64-level / 65,536-node structural guards plus bounded streaming serialization | passing across embedded, durable, process, MCP, model, evaluation, and trace paths |
| Secret hygiene | bounded source-tree pattern scan | no matches |
| Source hygiene | no source artifact above 1 MiB; crate forbids `unsafe` | passing |
| macOS process isolation | real Seatbelt allow/deny test | passing |
| Persistent stdio MCP launch | default-deny authority, bounded unrestricted mode, macOS Seatbelt option, absolute working directory, Unix process-group settlement | sandboxed Agent Memory Hub round trip passing locally |
| Encrypted network host | generated CA/server/client mTLS round trip and unauthenticated rejection | passing |
| Private model gateway trust | generated private CA plus authenticated and mTLS HTTPS round trips | passing |
| Direct OpenAI Responses adapter | fixed official HTTPS endpoint, environment Secret, `store: false`, explicit encrypted-reasoning inclusion, sequential function calls, bounded JSON/SSE, usage/request provenance, origin-bound continuation | local mapping, streaming, persistence, replay, completed-chain release, tamper, and cross-model-failover suppression tests passing; live API environment-gated |
| Configured capability assembly | shell-free JSON Tools, exact-selected MCP Tools, explicit process authority, Agent Memory Hub health/Context wiring | local adapter tests and real sandboxed Agent Memory Hub service health pass |
| Model provider failover | explicit 1–16 identity route, bounded per-attempt timeout with cancel-before-drop cleanup, per-attempt Trace, settled-provider State provenance, stream and total-deadline safety | passing |
| Task orchestration execution | bounded public TaskExecutor scheduler, dependency progress, timeout/panic isolation, exact-lease settlement, stale-result cancellation, fenced paged Mailbox | passing with memory and SQLite coordinator contracts |
| Task workspace lifecycle | default deny, exact-attempt Provider lease, bounded prepare/release, cleanup-before-settlement, concurrent local isolation, marker/path replacement guards, detached pinned Git Worktree through Process Broker | passing with local directories and real local Git |
| Task worker protocol | protocol-v11 conditional discovery, bounded graph/record/claim surfaces, principal-derived ownership, server-clock leases, cross-principal fencing, messaging, CAS recovery, explicit-revision cancellation | passing with authenticated lifecycle and conflict tests |
| Harness regression evaluation | format-2, origin-bound 2-case × 2-grader end-to-end Runtime suite and exact baseline | `yh eval-smoke` passing; machine-readable and nonzero on regression |
| Public API embedding | standalone zero-network hosts run a Policy-controlled Model/Tool loop and a durable fenced Task DAG through public contracts | `embedded` and `orchestrated` examples passing |

The local performance figures were measured on 2026-07-26 and are environment
specific. CI uses deliberately wider smoke thresholds to detect catastrophic
regression without pretending hosted runners are stable benchmark machines.

## CI evidence that requires a remote run

The checked-in workflow runs the zero-default core, each isolated optional
feature, all-feature minimum compiler gates and the versioned Harness
regression evaluation on Ubuntu, all-feature tests on macOS and Windows,
audits Rust dependencies, and runs a release-mode SQLite performance smoke
workload. It also runs the TUI render/unit suite plus demo and configured real
PTY smoke gates. The tag workflow builds separate native `yh` and `yh-tui`
archives and SHA-256 files on Ubuntu, macOS, and Windows using only pinned
checkout plus platform tools. A checked-in workflow is configuration, not
proof that GitHub has executed it. Its status must be green on the release
commit.

## Open release blockers

- The local candidate has no configured Git remote. GitHub App access identifies
  `liuyang0508` as the intended owner and confirms that `Y-Harness` does not
  already exist in the accessible repository set, but GitHub CLI has no
  authenticated session. Therefore the remote repository cannot yet be created,
  pushed, tagged, or used to produce remote Ubuntu/macOS/Windows CI evidence.

## Declared non-blocking scope limitations

The following capabilities are outside the supported v0.1 baseline. Their
absence is not silently relabeled as support; the safe fallback and evidence
boundary remain explicit.

- Linux and Windows have a safe deny-by-default Process Broker path but no
  concrete strong OS sandbox with platform integration tests.
- Local Process Broker timeout/cancellation use a bounded cleanup grace. Unix
  executions now use and integration-test private process-group settlement for
  ordinary descendants. Persistent stdio MCP now requires an explicit,
  concurrency-bounded launch authority, reuses that group settlement on Unix,
  and can reuse the tested macOS Seatbelt write/network policy; unrestricted
  mode is not a sandbox. A hostile process can escape with a new session/group.
  Escape-resistant Linux resource/process containment, Linux/Windows
  sandboxed persistent MCP launch, and Windows Job Object containment are not
  implemented or integration-tested.
- An authenticated, exact-versioned HTTPS JSON model-gateway adapter and
  zeroizing credential-reference surface exist. Its opt-in NDJSON mode decodes
  bounded provisional deltas and requires a final typed response. A direct
  OpenAI Responses adapter additionally provides bounded JSON/SSE,
  Harness-owned sequential function calling, and schema-5 origin-bound replay
  of encrypted reasoning items under `store: false`. Other direct vendor
  adapters are not implemented. Ignored environment-gated Gateway and OpenAI
  tests define the remaining external evidence; local schema tests are not
  relabeled as a live vendor pass.
- SQLite coordination provides single-host recovery and multi-process CAS, not
  multi-node consensus or distributed availability. Normal Turn startup is
  fenced from interrupting another Runtime's live Turn; explicit recovery still
  requires the host to prove exclusive ownership before takeover.
- Workspace Provider release is exact, bounded, and idempotent while its
  process-local lease is retained. A power loss, forced process termination,
  hostile provider, or cleanup timeout can still orphan a directory or Git
  Worktree. No durable cross-host allocation journal or automatic orphan
  reaper is claimed. Managed roots require bounded storage and
  operator-controlled reconciliation. Directory uniqueness and Git Worktrees
  are not OS sandboxes; untrusted executors and `SharedReadOnly` require
  independent Process Broker or mount enforcement.
- A durable Approval Inbox survives restart, fences competing settlement,
  records the authority-scoped requester and deciding actor, and rejects exact
  actor self-approval inside the CAS boundary. mTLS uses the client leaf
  certificate fingerprint as the authenticated subject, and schema-1 migration
  is backup-first and explicitly orphans unattributed pending work. An embedded
  host with exclusive ownership can resume a schema-3, fingerprint-matched
  pre-Tool approval wait across Memory or reopened SQLite stores. Post-decision
  unknown Tool effects remain fail-closed. Mapping certificate Subject/SAN to
  people, tenant/role policy, proving that two certificates belong to
  independent humans, signed decision receipts, retention, notifications, and
  lease/fenced remote continuation are not implemented.
- Publisher and transparency-log keys now support live validity and immutable
  revocation, and signed receipts bind log/entry/time metadata to the exact
  package and publisher signature. Exact pin-bound public HTTPS acquisition is
  available behind `https-skill`. Catalog discovery, authenticated private
  registries, dependency acquisition, caching/offline mirrors, a live external
  source pass, threshold signatures, durable trust-policy distribution, and
  append-only transparency-log inclusion/consistency/gossip are not
  implemented.
- Conversation history preserves a deterministic raw whole-Turn suffix and can
  invoke an explicitly registered, bounded semantic Compactor for a newest
  omitted slice. Derived blocks carry coverage and source/content fingerprints,
  remain non-authoritative, and never replace State history. State schema 2
  introduced bounded content-free compactor/coverage/digest/size evidence and
  schema 5 retains it after crash-tested backup-first migration. Summary-body
  persistence, exact replay/caching, and generic semantic-faithfulness
  verification are not implemented.
- Journal-anchored snapshots accelerate recovery, opt-in automatic maintenance
  is bounded, failure-isolated, observable, drainable, and latest-only, and
  Threads expose count-and-recovery-byte warning/critical pressure before
  finite boundaries. Store-authoritative atomic recovery accounting, bounded
  pages, and terminal byte reserve prevent an accepted stream from becoming
  unrecoverable merely through aggregate event size.
  Archival, historical-snapshot retention, legal-hold policy, and blob
  offloading are not implemented.

## Release rule

A release candidate requires:

1. every local required gate passing from a clean checkout;
2. every CI job green on the exact candidate commit;
3. zero known critical or high-severity correctness/security defects;
4. a deliberate owner decision for license and distribution;
5. versioned migration and compatibility notes;
6. explicit documentation of every unsupported platform or capability.

Absolute permanent “zero bugs” is not a provable property. Release claims must
name the evidence and date used.
