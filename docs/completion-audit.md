# Harness completion audit

This audit maps the `Agent = LLM × Harness` scope to implementation and
executable evidence. It is not a release declaration. The authoritative open
blockers remain in [`release-readiness.md`](release-readiness.md).

## Eleven-layer evidence matrix

| Layer | Implemented baseline | Primary evidence | Status |
|---|---|---|---|
| Context Engine | deterministic blocks, whole-Turn history, memory packs, registered Token Counters and semantic Compactors, independent byte/token budgets, derived-summary provenance and failure modes | `src/context`, Context and Runtime compaction tests, ADRs 0021/0059/0060/0061/0064 | local baseline passing |
| Agent Loop | bounded model/tool/verification loop, explicit ordered and attempt-deadlined Model failover, cancellation, deadlines, concurrency admission, explicit recovery, fingerprinted pre-Tool approval resume | `src/runtime`, failover/provenance and Memory/SQLite restart tests, ADRs 0065/0070 | local baseline passing; remote takeover awaits fencing |
| Tool Runtime | typed registry, configured shell-free JSON Tools, exact-selected MCP Tools, default-deny bounded MCP launch, Unix process-group settlement, reusable macOS sandbox, explicit compensation | `src/execution`, `src/transport/mcp.rs`, ADRs 0013/0036/0043/0054/0056/0062/0066/0076 | local baseline passing; escape-resistant Linux plus Linux/Windows persistent containment open |
| State Engine | typed journal, memory/SQLite stores, CAS, checkpoints, snapshots, capacity/recovery bounds, schema-1/2/3/4/5 to schema-6 backup-first migration, origin-bound Provider Continuation, durable safe-boundary Steering | `src/state`, schema-1/2/3/4/5 migration fixtures, continuation and Steering fault tests, ADRs 0061/0065/0068/0077/0078 | local baseline passing; archival/offload open |
| Memory Engine | versioned provider port, scoped provenance, Agent Memory Hub MCP adapter and configured Context assembly | `src/memory`, service host, unit tests and environment-gated real MCP test | adapter, service health probe, and sandboxed local round trip passing; remote CI environment-gated |
| Skill Engine | exact dependency graph, budgets, signatures, live revocation, transparency receipts, pinned HTTPS source | `src/skill`, ADRs 0009/0014/0032/0033 | local baseline passing; catalog/private registry open |
| Policy Engine | deny/allow/ask, risk class, attributed durable approval, restart-safe inbox, CAS, exact-actor separation of duty, fingerprinted continuation, backup-first schema migration | `src/runtime/policy.rs`, `src/approval`, restart/drift tests, ADRs 0007/0024/0049–0051/0063/0065 | local baseline passing; human/tenant roles and signed receipts open |
| Orchestration | bounded DAG, executable TaskExecutor scheduler, dependency concurrency, timeout/panic isolation, leases/fencing, paged TaskMailbox messaging, default-deny Workspace Provider lifecycle, isolated local directories, pinned detached Git Worktrees, Artifacts, memory and SQLite coordination, serviceable authenticated worker protocol | `src/orchestration`, `src/protocol/task.rs`, `examples/orchestrated.rs`, ADRs 0011/0019/0052/0053/0071/0072/0073/0074 | embedded and protocol worker lifecycles passing for single-host/multiprocess coordination; multi-node consensus and durable orphan reconciliation open |
| Verification | typed completion gates, retryable correction, hard failure settlement | `src/verification`, Runtime verification tests, ADR 0008 | local baseline passing |
| Observability | content-free phase records, latency/outcome/accounting, panic isolation, bounded collector, allocation-bounded JSONL | `src/observability`, ADRs 0017/0064 | local baseline passing |
| Evaluation | bounded parallel cases/graders, isolated failures, format-2 root validation, origin-bound exact baselines, required-pass gates, versioned end-to-end smoke suite, external-run format 1 | `src/evaluation`, `evals`, `yh eval-smoke`, `tools/benchmark-runner`, ADRs 0010/0026/0064/0067/0069/0079 | executable local baseline plus one real non-claim Claude Code adapter-conformance record |

## Product and integration boundary

| Deliverable | Evidence | Status |
|---|---|---|
| Headless embeddable Rust core | public contracts in `src/lib.rs`; zero-default build; external-view examples execute a Policy-controlled Model/Tool loop and a fenced Task DAG | compiled and run locally; CI-gated |
| Reference-project derivation | primary-source comparison, immutable open-source snapshots, adopted/rejected decisions, code/ADR mapping in `reference-analysis.md` | documented and link-checked locally |
| Serviceable typed protocol | language-neutral v12 specification, exact envelope and schema-6 compatibility coordinates, negotiation, async Turn operations, exact-ID Steering, conditional Task Graph discovery, authenticated fenced worker lifecycle, bounded paging, cancellation, shutdown | `docs/protocol.md`, `src/protocol`, process/TLS tests; passing locally |
| Engine CLI | `yh init/doctor/serve`, durable State/Approval/Task databases, deterministic demo, direct OpenAI Responses, JSON/MCP Tool and Agent Memory Hub assembly, `src/reference_cli`, process tests | installed-binary and restart tests passing locally; live OpenAI API environment-gated |
| Optional full-screen TUI | separate `y-harness-tui` package and `yh-tui` binary in `clients/tui`; Protocol-v12-only child transport, exact-ID active-Turn Steering, invalidated provisional-output handling, content-free continuation rendering, TestBackend render tests, real PTY Turn | independently installable; local unit/lint/PTY gates passing |
| Competitive benchmark adapter | independent `y-harness-benchmark-runner` package; bounded shell-free Claude Code JSON adapter; exact binary coordinates; external-run format 1 | 4 adapter tests plus one real `claim_eligible: false` conformance result; no comparative case result |
| Install and operator path | Cargo-backed no-side-effect install script, strict config template, Chinese quick start, real language-neutral Task Worker, acceptance checklist | clean-prefix install and revision-6 worker lifecycle passing locally |
| MCP tools | official SDK client plus atomic namespaced Tool registration | passing locally |
| Agent Memory Hub | first-party provider adapter over persistent stdio MCP | live local round trip passing; CI environment-gated |
| External model provider | exact-versioned HTTPS JSON/NDJSON gateway plus direct OpenAI Responses JSON/SSE, secret references, exclusive private-CA trust and mTLS gateway identity, bounded origin-bound reasoning continuation | local private-gateway TLS plus OpenAI mapping/stream/persistence/replay/tamper tests passing; live gateway and OpenAI API environment-gated |
| External executable capabilities | deny-by-default Process Broker and bounded JSON adapters | passing locally |
| Desktop/Web/IM | intentionally independent optional products over the same protocol | future clients, never duplicate runtimes |

## Local gate evidence

The following commands passed on 2026-07-26 with Rust 1.88:

```bash
git diff --check
cargo fmt --all -- --check
cargo check --locked --no-default-features
cargo check --locked --no-default-features --features https-model
cargo check --locked --no-default-features --features https-skill
cargo check --locked --no-default-features --features tls-host
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --no-default-features
cargo test --locked --all-features
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
YH_BIN=/isolated/prefix/bin/yh python3 examples/task_worker_client.py \
  /isolated/project/y-harness.json
```

The all-feature workspace run contains 274 passing library tests, 2 CLI
configuration tests, 7 Engine process/service tests, 10 TUI unit/render tests,
and 2 local private-gateway TLS integration tests. The demo and configured
PTY smoke gates submit real Turns, verify durable State, and check
alternate-screen and bracketed-paste restoration. One additional 64 MiB
migration test and one
126.9 MiB Approval Inbox
migration test are deliberately manual.
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
