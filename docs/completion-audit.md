# Harness completion audit

This audit maps the `Agent = LLM × Harness` scope to implementation and
executable evidence. It is not a release declaration. The authoritative open
blockers remain in [`release-readiness.md`](release-readiness.md).

## Eleven-layer evidence matrix

| Layer | Implemented baseline | Primary evidence | Status |
|---|---|---|---|
| Context Engine | deterministic blocks, whole-Turn history, memory packs, registered Token Counters and semantic Compactors, configured brokered JSON-command compaction, independent byte/token budgets, derived-summary provenance, attributed per-Turn Context, and format-1 digest-bound Thread-handoff preparation | `src/context`, `src/execution`, real service-process compaction, Context/Runtime compaction, invocation-context, and handoff tests, ADRs 0021/0059/0060/0061/0064/0096/0097/0105 | local baseline passing |
| Agent Loop | bounded model/tool/verification loop, atomic same-response multi-Tool decisions with explicit bounded safe-run concurrency, sequential fences, source-ordered settlement, explicit ordered and attempt-deadlined Model failover, observable timeout-only cooldown with last-resort fail-open, bounded typed Provider failure evidence, default-disabled typed same-Model retries with shared deadlines and provisional-output fencing, cancellation, deadlines, concurrency admission, explicit recovery, fingerprinted pre-Tool and batch approval resume | `src/runtime`, typed-failure/retry/trace, safe-parallel/fence/timeout, cooldown/trace/fail-open, ordered-batch, approval-restart, failover/provenance, and Memory/SQLite restart tests, ADRs 0065/0070/0086/0098/0099/0100/0101 | local baseline passing; cross-Turn/distributed recovery policy and remote takeover remain explicit open contracts |
| Tool Runtime | typed registry, configured shell-free JSON Tools, exact-selected stdio or authenticated HTTPS JSON-response MCP Tools, explicit activation and optional command lock, default-deny bounded MCP launch, Unix process-group settlement, reusable macOS sandbox, explicit compensation | `src/execution`, `src/transport`, ADRs 0013/0036/0043/0054/0056/0062/0066/0076/0088/0103 | local and private-TLS remote baselines passing; bounded SSE/OAuth, escape-resistant Linux, and Linux/Windows persistent containment open |
| State Engine | typed journal, memory/SQLite stores, CAS, checkpoints, snapshots, capacity/recovery bounds, schema-1 through schema-10 to schema-11 backup-first migration, origin-bound Provider Continuation, durable safe-boundary Steering, atomic ordered Tool-call batches, explicit Thread names, terminal-boundary atomic forks, lineage-aware bounded summaries, portable integrity-bound Thread archives, and caller-attributed content-free invocation Context | `src/state`, schema-1 through schema-10 migration fixtures, invocation-context provenance tests, archive tamper/no-clobber/idempotency/reopen tests, fork rollback/reopen/idempotency/summary, name drift, batch, continuation, and Steering fault tests, ADRs 0061/0065/0068/0077/0078/0086/0092/0093/0094/0095/0096 | local baseline passing; destructive archival/offload open |
| Memory Engine | versioned provider port, scoped provenance, Agent Memory Hub MCP adapter and configured Context assembly | `src/memory`, service host, unit tests and environment-gated real MCP test | adapter, service health probe, and sandboxed local round trip passing; remote CI environment-gated |
| Skill Engine | exact dependency graph, budgets, signatures, live revocation, transparency receipts, pinned HTTPS source, bounded trusted and signed-External install/list/verify/recoverable-remove lifecycle, explicit project-configured activation and publisher/log diagnostic locks | `src/skill`, service host, ADRs 0009/0014/0032/0033/0085/0088/0091/0102 | local governed lifecycle and public-HTTPS install path passing by composed contract; automatic update, dependency acquisition, catalog/private registry, and live public fixture remain open |
| Policy Engine | deny/allow/ask, risk class, attributed durable approval, restart-safe inbox, CAS, exact-actor separation of duty, fingerprinted continuation, backup-first schema migration | `src/runtime/policy.rs`, `src/approval`, restart/drift tests, ADRs 0007/0024/0049–0051/0063/0065 | local baseline passing; human/tenant roles and signed receipts open |
| Orchestration | bounded DAG, executable TaskExecutor scheduler, dependency concurrency, timeout/panic isolation, leases/fencing, paged TaskMailbox messaging, default-deny Workspace Provider lifecycle, isolated local directories, pinned detached Git Worktrees, Artifacts, memory and SQLite coordination, serviceable authenticated worker protocol | `src/orchestration`, `src/protocol/task.rs`, `examples/orchestrated.rs`, ADRs 0011/0019/0052/0053/0071/0072/0073/0074 | embedded and protocol worker lifecycles passing for single-host/multiprocess coordination; multi-node consensus and durable orphan reconciliation open |
| Verification | typed completion gates, retryable correction, hard failure settlement, exact Turn cancellation, and configured brokered JSON-command Verifiers | `src/verification`, `src/execution`, real service-process verification, Runtime verification tests, ADRs 0008/0106 | local and configured-process baselines passing |
| Observability | content-free phase records, latency/outcome/accounting, distinct registered and Provider-reported Model identities, exact integer provider-cost evidence, typed Provider failure class/status/retry evidence, invoked Model retry indices, panic isolation, bounded collector, allocation-bounded JSONL | `src/observability`, ADRs 0017/0064/0083/0084/0100/0101 | local baseline passing |
| Evaluation | bounded parallel cases/graders, exact per-Grader cancellation, configured brokered JSON-command Graders, isolated in-memory `yh eval`, format-2 root validation, origin-bound exact baselines, required-pass gates, versioned end-to-end smoke suite, external-run formats 1/2/3/4, controller-owned deterministic Tool fault oracle | `src/evaluation`, `src/execution`, `evals`, `yh eval`, `yh eval-smoke`, `tools/benchmark-runner`, ADRs 0010/0026/0064/0067/0069/0079/0080/0081/0082/0090/0107 | executable built-in and configured-process baselines, bounded Claude Code, Codex, Grok Build, and Pi adapter contracts, one real non-claim Claude Code conformance record, and one non-claim crash-after-effect fixture |

## Product and integration boundary

| Deliverable | Evidence | Status |
|---|---|---|
| Headless embeddable Rust core | public contracts in `src/lib.rs`; zero-default build; external-view examples execute a Policy-controlled Model/Tool loop and a fenced Task DAG | compiled and run locally; CI-gated |
| Reference-project derivation | primary-source comparison, immutable open-source snapshots, adopted/rejected decisions, code/ADR mapping in `reference-analysis.md` | documented and link-checked locally |
| Serviceable typed protocol | language-neutral v18 specification, exact envelope and schema-11/API-7 compatibility coordinates, bounded per-Turn Context, lineage-aware Thread summaries, atomic retry-identified Thread forks, durable Thread names/import provenance, async Turn operations, exact-ID Steering, conditional Task Graph discovery, authenticated fenced worker lifecycle, bounded paging, cancellation, shutdown | `docs/protocol.md`, `src/protocol`, process/TLS tests; passing locally |
| Engine CLI | `yh init/doctor/serve/eval`, no-clobber `yh thread export` and atomic `yh thread import`, trusted/signed/HTTPS `yh skill install*` plus list/verify/remove, durable State/Approval/Task databases, deterministic demo, strict configured Model catalog/route, direct OpenAI Responses, HTTPS Gateways, versioned brokered JSON-command Models, semantic Conversation Compactors, completion Verifiers, and Evaluation Graders, JSON/MCP Tool, project Skill and Agent Memory Hub assembly, `src/reference_cli`, process tests | archive round-trip/tamper/no-clobber, multi-Provider route diagnostics, real compatible command-Model Turn with External State provenance, real settlement-v1 typed retry, real command-compactor Turn with immutable source history and durable summary provenance, real command-Verifier completion gate and durable result, isolated command-Grader Evaluation with exact baseline, installed-binary, signed External Skill trust/revocation/transparency lifecycle, project Skill integrity, and restart tests passing locally; live OpenAI and public Skill endpoints environment-gated |
| Optional full-screen TUI | separate `y-harness-tui` package and `yh-tui` binary in `clients/tui`; Protocol-v18-only child transport, bounded lineage-aware recent-Thread navigation/resume, `/fork [terminal-turn-id]`, exact-ID active-Turn Steering, invalidated provisional-output handling, content-free continuation and ordered batch rendering, TestBackend render tests, real PTY Turn and fork | independently installable; local unit/lint/PTY gates passing; Pi-style entry-level in-place navigation is intentionally outside the Engine model |
| Competitive benchmark tools | independent `y-harness-benchmark-runner` and `y-harness-fault-fixture` packages; bounded shell-free Claude Code JSON, Codex JSONL, Grok Build headless JSON, and Pi JSONL adapters; exact binary coordinates; external-run formats 1/2/3/4; deterministic stdio MCP crash-after-effect fixture and durable oracle | 23 adapter/fixture tests plus one real `claim_eligible: false` Claude conformance result; Codex, Grok Build, and Pi have no live records and no comparative case exists |
| Install and operator path | Cargo-backed no-side-effect install script, strict config template, Chinese quick start, real language-neutral Task Worker, acceptance checklist | clean-prefix install and revision-6 worker lifecycle passing locally |
| MCP tools | official SDK stdio plus optional authenticated HTTPS JSON-response clients, atomic namespaced Tool registration, explicit activation, optional command-file lock | stdio process and private-TLS remote service assembly passing; SSE/OAuth not claimed |
| Agent Memory Hub | first-party provider adapter over persistent stdio MCP | live local round trip passing; CI environment-gated |
| External model provider | exact-versioned HTTPS JSON/NDJSON gateway, direct OpenAI Responses JSON/SSE, and brokered language-neutral JSON-command output-v1/settlement-v1; secret references, Provider-reported usage/settled Model/request evidence where supported, exclusive private-CA trust and mTLS gateway identity, bounded origin-bound continuation, typed failure facts | local private-gateway TLS, OpenAI mapping/stream/persistence/replay/tamper, compatible command-Model service Turn, settlement evidence, and typed retry passing; command Models deliberately do not claim provisional streaming; live gateway and OpenAI API environment-gated |
| External executable capabilities | deny-by-default Process Broker and bounded JSON adapters | passing locally |
| Desktop/Web/IM | intentionally independent optional products over the same protocol | future clients, never duplicate runtimes |

## Local gate evidence

The following commands passed on 2026-07-28 with Rust 1.88:

```bash
git diff --check
cargo fmt --all -- --check
cargo check --locked --no-default-features
cargo check --locked --no-default-features --features https-model
cargo check --locked --no-default-features --features https-mcp
cargo check --locked --no-default-features --features https-skill
cargo check --locked --no-default-features --features tls-host
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --no-default-features
cargo test --locked --workspace --all-targets --all-features
cargo test --locked -p y-harness-tui
cargo clippy --locked -p y-harness-tui --all-targets -- -D warnings
cargo run --locked --example embedded
cargo run --locked --example orchestrated
cargo run --locked -- eval-smoke
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
cargo audit --deny warnings
cargo package --locked -p y-harness
./scripts/install.sh --root /isolated/prefix
./scripts/install-tui.sh --root /isolated/tui-prefix
python3 scripts/smoke-tui.py
python3 scripts/smoke-tui.py --configured
python3 scripts/smoke-tui.py --tui target/release/yh-tui --engine target/release/yh
python3 scripts/smoke-tui.py --configured \
  --tui target/release/yh-tui --engine target/release/yh
YH_BIN=/isolated/prefix/bin/yh python3 examples/task_worker_client.py \
  /isolated/project/y-harness.json
```

The all-feature workspace run contains 341 passing library tests plus
2 manual size tests, 10 CLI configuration tests, 23 Engine process/service tests, 11 TUI
unit/render tests, 2 local private-gateway TLS integration tests, 2 local
private-MCP TLS integration tests, 19 released-product adapter tests, and 4
deterministic fault-fixture tests: 412 passing plus 7 explicitly ignored
fixtures in total. The no-default-feature workspace run contains 383 passing tests plus 3 ignored
manual/environment fixtures. The demo and configured PTY
smoke gates submit real Turns, create atomic child Threads, verify parent/child
history plus durable lineage in State, and check alternate-screen and
bracketed-paste restoration. One additional 64 MiB migration test and one
126.9 MiB Approval Inbox
migration test are deliberately manual.
The 219-file package archive verifies from the committed clean-tree candidate
without Cargo's `--allow-dirty` escape hatch.
Five integration tests are ignored by the ordinary suite unless their explicit
external fixtures are configured:
Agent Memory Hub, ordinary and streaming HTTPS model gateway, direct OpenAI,
and pinned HTTPS Skill acquisition. The Agent Memory Hub test passed against the local
first-party launcher under macOS Seatbelt with network denied and offline
hashing embeddings on 2026-07-25; local success is not evidence for the
remaining external systems.

A separate production-path lint also rejects panic and unwrap/expect use in the
library:

```bash
cargo clippy --locked --lib --all-features -- \
  -D clippy::unwrap_used \
  -D clippy::expect_used \
  -D clippy::panic \
  -D warnings
```

## Completion boundary

The engine baseline covers every one of the eleven semantic layers and keeps
business applications outside the Harness. MIT OR Apache-2.0 distribution,
crate packaging, installed-binary operation, release notes, and native release
automation are complete locally. A public release still requires green remote
CI on the exact commit and the explicitly scoped platform/external-integration
limitations listed in [`release-readiness.md`](release-readiness.md).

This completion audit is an internal contract-coverage statement, not evidence
that Y-Harness produces better results than another Harness or product. Such a
claim additionally requires the controlled, source-pinned protocol in
[`competitive-benchmark.md`](competitive-benchmark.md); no comparative result
has been produced yet.

“No bugs” is not a verifiable permanent state. The enforceable completion rule
is zero known critical/high defects plus named, reproducible evidence for every
supported claim.
