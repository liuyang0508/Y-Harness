# Release readiness

This document separates executable evidence from roadmap claims. Y-Harness is
not release-ready while any blocking row remains open.

## Current evidence

| Gate | Evidence | Current result |
|---|---|---|
| Minimum compiler | Rust 1.88 `check`, Clippy, tests, docs | passing |
| Feature isolation | zero-default core, each optional feature, and all features | passing |
| Deterministic tests | 344 library, 10 CLI, 23 Engine process/service, 11 TUI unit/render, 2 private-gateway TLS integration, 2 private-MCP TLS integration, 42 product/Engine evidence-adapter, and 6 fault-fixture tests | passing locally: 440 total plus 7 explicitly ignored fixtures |
| Full-screen TUI PTY | demo and configured Engine modes; real Turn, atomic Thread fork, durable State, alternate screen and bracketed-paste restoration | debug and release binaries passing |
| Installed operator path | isolated-prefix Engine and TUI installs; version, init, doctor, persistent service, demo, Task DAG and Mailbox | passing; TUI install contains only `yh-tui`; Task Graph terminal at revision 6 |
| Distribution package | `cargo package --locked -p y-harness`, 224-file clean-room crate verification | passing locally from the committed clean tree |
| Real memory integration | Agent Memory Hub stdio MCP round trip under macOS Seatbelt, network denied, offline embeddings | passing |
| Dependency security | `cargo-audit 0.22.2 --deny warnings` over 289 locked crates | passing |
| State performance | 1,000 events plus 64-Thread lineage page, 5 samples, SQLite WAL + FULL | 71.841 ms append; 2.957 ms full projection; 11.116 ms atomic fork; 0.288 ms Thread list; 2.454 ms snapshot load |
| State migration | schemas 1/2/3/4/5/6/7/8/9/10 → 11, immutable-history backup-first path | schema-1 through schema-10 sources, coordinate- and SHA-256-bound, restartable; prior maximum-size schema-4 measurement retained as historical evidence |
| Atomic Thread fork | terminal Turn boundary, caller-owned child retry identity, direct lineage, Memory/SQLite atomic creation | boundary/idempotency/reopen tests passing; injected SQLite uniqueness failure leaves no child stream or projection |
| Portable Thread archive | bounded format-1 source journal and SHA-256, terminal-only export, no-clobber CLI file adapter, schema-10 import provenance, caller-owned idempotency identity, Memory/SQLite atomic import | round-trip/tamper/unknown-field/conflict/reopen/snapshot tests passing; latest schema-11 recheck at 1,000-Item export/import medians 4.858/8.080 ms on the reference host |
| Per-Turn reference Context | 64-block/1 MiB pre-State bounds, caller attribution, source/body SHA-256, schema-11 content-free evidence, approval-resume request binding, Skill-only provider instruction authority | Context/Runtime/State/Protocol/OpenAI mapping tests passing; branch-summary synthesis remains an optional-module gap |
| Approval migration | schema 1 → 2, 256 records / 133,038,080 record bytes | 844.781 ms; backup-first, SHA-256-bound, restartable |
| JSON authority | 64-level / 65,536-node structural guards plus bounded streaming serialization | passing across embedded, durable, process, MCP, model, evaluation, and trace paths |
| Secret hygiene | bounded source-tree pattern scan | no matches |
| Source hygiene | no source artifact above 1 MiB; crate forbids `unsafe` | passing |
| macOS process isolation | real Seatbelt allow/deny test | passing |
| Persistent stdio MCP launch | default-deny authority, bounded unrestricted mode, macOS Seatbelt option, absolute working directory, Unix process-group settlement | sandboxed Agent Memory Hub round trip passing locally |
| Authenticated HTTPS MCP | optional exact HTTPS JSON-response transport, per-request environment Secret, no redirect/proxy/retry/replay, bounded request/response/session/time, exclusive CA | direct Tool call and reference-service `doctor` assembly passing against generated private TLS |
| Encrypted network host | generated CA/server/client mTLS round trip and unauthenticated rejection | passing |
| Private model gateway trust | generated private CA plus authenticated and mTLS HTTPS round trips | passing |
| Direct OpenAI Responses adapter | fixed official HTTPS endpoint, environment Secret, `store: false`, explicit encrypted-reasoning inclusion, same-response multi-call decoding, Harness-owned effect-safe bounded scheduling, bounded JSON/SSE, Provider-reported Model/usage/request provenance, origin-bound continuation | local mapping, ordered/safe-parallel/fenced batch, approval-restart, streaming, persistence, replay, completed-chain release, tamper, and cross-model-failover suppression tests passing; live API environment-gated |
| Brokered JSON-command Model | additive single/catalog configuration, compatible output-v1 plus explicit strict settlement-v1, typed bounded stdin/stdout, Provider evidence/failure facts, explicit Process Broker authority, cleared mapped environment, cancellation and settlement, External provenance | real compatible service Turn, durable State-origin assertion, and typed transient retry passing on Unix; core broker phase/cancellation/metadata/failure/strictness tests passing cross-platform; provisional streaming not claimed |
| Brokered JSON-command Conversation Compactor | additive strict `conversation` configuration, bounded semantic request/response, Context-phase cancellation, explicit Process Broker authority, independent input/output budgets, immutable source history, content-free State provenance | real three-Turn service process invokes the command and records exact covered Turn/digests; invalid static process configuration precedes environment access; core phase/cancellation/deep-JSON/size gates passing |
| Brokered JSON-command Verifier | strict multi-Verifier configuration, immutable bounded candidate snapshot, strict pass/fail wire outcome, Verification-phase cancellation, explicit Process Broker authority, Runtime-owned outcome validation/retry/final settlement | real service Turn passes the configured gate and records durable `VerificationResult`; invalid static process configuration precedes mapped environment access; core phase/cancellation/deep-JSON/unknown-field gates passing |
| Brokered JSON-command Evaluation Grader | additive `evaluation` configuration, isolated in-memory target State, strict 4 MiB sample/response contract, independent case/Grader concurrency and cancellation, External origin, exact format-2 baseline | real configured CLI Evaluation and origin assertion passing; `serve` acquires no Grader authority; invalid static timeout precedes environment access; full workspace passing |
| Configured capability assembly | shell-free JSON Models/Tools/Conversation Compactors/Verifiers, exact-selected stdio/HTTPS MCP Tools, explicit process/network authority, Agent Memory Hub health/Context wiring, operator-trusted and signed-External Skill activation with publisher/log locks | real command-Model, command-compactor, and command-Verifier Turns, local/remote MCP adapter tests, signed Skill lifecycle/revocation/transparency process test, and real sandboxed Agent Memory Hub service health pass |
| Model provider failover | strict service-configured Model catalog, per-Model environment Secret mapping, explicit 1–16 identity route, bounded per-attempt timeout with cancel-before-drop cleanup, opt-in timeout-only cooldown with last-resort fail-open and skipped Trace evidence, settled-provider State provenance, stream and total-deadline safety | Runtime expiry/skip/fail-open/non-misclassification tests plus multi-gateway `yh doctor` route test passing |
| Model Provider failures and retry | additive bounded public taxonomy, legacy `Model(String)` compatibility, high-confidence HTTP/transport/protocol mapping, numeric retry hints, content-free Trace evidence, default-disabled 1–8 same-Model retries for four typed transient classes, shared attempt budget, cancellable bounded jitter, retry indices, and provisional-output fencing | constructor/status/trace/privacy plus retry exhaustion/classification/deadline/cancellation/stream/config tests passing; vendor-specific structured quota/content/model-unavailable mapping and distributed recovery remain open |
| Task orchestration execution | bounded public TaskExecutor scheduler, dependency progress, timeout/panic isolation, exact-lease settlement, stale-result cancellation, fenced paged Mailbox | passing with memory and SQLite coordinator contracts |
| Task workspace lifecycle | default deny, exact-attempt Provider lease, bounded prepare/release, cleanup-before-settlement, concurrent local isolation, marker/path replacement guards, detached pinned Git Worktree through Process Broker | passing with local directories and real local Git |
| Task worker protocol | protocol-v19 conditional discovery, bounded graph/record/claim surfaces, principal-derived ownership, server-clock leases, cross-principal fencing, messaging, CAS recovery, explicit-revision cancellation | passing with authenticated lifecycle and conflict tests |
| Harness regression evaluation | format-2, origin-bound 2-case × 2-grader end-to-end Runtime suite and exact baseline, plus configured external Grader path | `yh eval-smoke` and real isolated `yh eval` process test passing; machine-readable and nonzero on regression |
| Product and Engine evidence tools | external-run formats 1–6, Codex CF-003 formats 7/8, and Y-Harness CF-003 format 9; exact adapter/product/source/fixture/rollout/config hashes; bounded Claude Code JSON, Codex JSONL, Grok Build headless JSON, Pi JSONL, OpenCode JSONL, Hermes one-shot/usage, and typed Y-Harness stdio processes; deterministic loopback Responses Provider or spec-bound JSON-command Model; unavailable facts preserved as unavailable; exact-pinned MCP crash/hold-after-effect fixture with durable oracle | adapter/fixture tests plus real Claude Code 2.1.143 and Pi 0.82.1 fixed-output, Codex 0.145.0 single-process/same-Thread restart, and Y-Harness explicit-recovery restart CF-003 records passing; all explicitly ineligible for comparison claims, with an identity-bound detached-MCP release marker, new-Turn recovery, no implicit takeover, and no descendant-exit claim recorded, while Grok Build, OpenCode, and Hermes remain contract-only |
| Public API embedding | standalone zero-network hosts run a Policy-controlled Model/Tool loop and a durable fenced Task DAG through public contracts | `embedded` and `orchestrated` examples passing |

The latest local performance figures were measured on 2026-07-28 and are environment
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

- The clean local candidate has no configured Git remote. A read-only check on
  2026-07-28 confirms that GitHub CLI is authenticated as `liuyang0508` and
  `liuyang0508/Y-Harness` does not exist. Creating that external repository,
  choosing its visibility, pushing, and tagging are publication actions rather
  than local build steps; they have not been performed without explicit
  publication authorization. Consequently no remote Ubuntu/macOS/Windows CI or
  release-archive evidence exists yet.

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
- Remote MCP supports the stateless JSON-response subset over exact
  authenticated HTTPS. SSE, OAuth/managed identity, arbitrary headers,
  cookies, stateful remote sessions, redirects, proxies, and transparent
  session recovery are not implemented. A failed Tool request is never
  automatically replayed; a later independent operation may establish a new
  session. JSON response bounds are not evidence for the still-open
  pre-allocation SSE framing problem.
- An authenticated, exact-versioned HTTPS JSON model-gateway adapter and
  zeroizing credential-reference surface exist. Its opt-in NDJSON mode decodes
  bounded provisional deltas and requires a final typed response. A direct
  OpenAI Responses adapter additionally provides bounded JSON/SSE,
  schema-7 atomic same-response Tool-call decisions with Harness-owned bounded
  explicitly safe scheduling, source-ordered settlement, schema-5 origin-bound
  replay, and schema-6 safe-boundary steering
  of encrypted reasoning items under `store: false`. Other direct vendor
  adapters are not implemented. Ignored environment-gated Gateway and OpenAI
  tests define the remaining external evidence; local schema tests are not
  relabeled as a live vendor pass.
- A configured JSON-command Model makes a language-neutral brokered Model port
  available to the reference service and route. The compatible `output_v1`
  carries only `ModelOutput`; explicitly selected `settlement_v1` can carry
  validated Provider usage/cost/request/model/continuation evidence or typed
  failure facts. Neither is a native vendor protocol or provisional stream.
  An unrestricted broker grants the child the Runtime user's OS authority and
  is not a sandbox.
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
  available behind `https-skill`. The reference service explicitly separates
  operator-trusted project files from signed External files, configures
  publisher/log trust, preserves External provenance, and rechecks trust during
  governed use. The CLI validates and canonically installs local, offline
  signed, or exact public-HTTPS declarative packages without silently
  activating them. Automatic config mutation/update, dependency acquisition,
  directory auto-activation, hot reload, catalog discovery, authenticated
  private registries, caching/offline mirrors, a live external source pass,
  threshold signatures, durable trust-policy distribution, and append-only
  transparency-log inclusion/consistency/gossip are not implemented.
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
