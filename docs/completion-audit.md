# Harness completion audit

This audit maps the `Agent = LLM × Harness` scope to implementation and
executable evidence. It is not a release declaration. The authoritative open
blockers remain in [`release-readiness.md`](release-readiness.md).

## Eleven-layer evidence matrix

| Layer | Implemented baseline | Primary evidence | Status |
|---|---|---|---|
| Context Engine | deterministic blocks, whole-Turn history, memory packs, registered Token Counters and semantic Compactors, configured brokered JSON-command compaction, independent byte/token budgets, derived-summary provenance, attributed per-Turn Context, and format-1 digest-bound Thread-handoff preparation | `src/context`, `src/execution`, real service-process compaction, Context/Runtime compaction, invocation-context, and handoff tests, ADRs 0021/0059/0060/0061/0064/0096/0097/0105 | local baseline passing |
| Agent Loop | bounded model/tool/verification loop, atomic same-response multi-Tool decisions with explicit bounded safe-run concurrency, sequential fences, source-ordered settlement, explicit ordered and attempt-deadlined Model failover, observable timeout-only cooldown with last-resort fail-open, bounded typed Provider failure evidence, default-disabled typed same-Model retries with shared deadlines and provisional-output fencing, independent retry/failover attempt budget per step and derived whole-Turn ceiling, cancellation, deadlines, concurrency admission, permissioned exact-Turn recovery, fingerprinted pre-Tool and batch approval resume | `src/runtime`, typed-failure/retry/trace, shared attempt-budget retry/failover tests, safe-parallel/fence/timeout, cooldown/trace/fail-open, ordered-batch, approval-restart, failover/provenance, Memory/SQLite restart tests, and process-level CF-003 recovery, ADRs 0065/0070/0086/0098/0099/0100/0101/0113/0114 | local baseline passing; calls hidden inside arbitrary extension implementations and distributed lease/fencing ownership remain explicit open contracts |
| Tool Runtime | typed registry, configured shell-free JSON Tools, exact-selected stdio or authenticated HTTPS JSON-response MCP Tools, explicit activation and optional command lock, default-deny bounded MCP launch, bounded cancellation/deadline session settlement, tenant-scoped shared-session denial, Unix process-group settlement, reusable macOS sandbox, explicit compensation | `src/execution`, `src/transport`, Runtime cancellation/deadline and tenant-admission tests, real hold-after-effect stdio fixture, ADRs 0013/0036/0043/0054/0056/0062/0066/0076/0088/0103/0115/0120 | local and private-TLS remote unscoped baselines passing; tenant-partitioned sessions, bounded SSE/OAuth, escape-resistant Linux, and Linux/Windows persistent containment open |
| State Engine | typed journal, memory/SQLite stores, CAS, checkpoints, snapshots, capacity/recovery bounds, schema-1 through schema-12 to schema-13 backup-first migration, authoritative optional Thread tenant ownership with validated lookup projections, immutable content-free Turn execution binding, origin-bound Provider Continuation, durable safe-boundary Steering, atomic ordered Tool-call batches, explicit Thread names, terminal-boundary atomic forks, lineage-aware bounded summaries, portable integrity-bound Thread archives, and caller-attributed content-free invocation Context | `src/state`, schema-1 through schema-12 migration fixtures, execution-binding tenant/duplicate/reopen/snapshot/archive tests, invocation-context provenance tests, archive tamper/no-clobber/idempotency/reopen tests, fork rollback/reopen/idempotency/summary, name drift, batch, continuation, and Steering fault tests, ADRs 0061/0065/0068/0077/0078/0086/0092/0093/0094/0095/0096/0117/0122 | local baseline passing; external Artifact blob storage/authorization and destructive archival/offload open |
| Memory Engine | versioned provider port, scoped provenance, Agent Memory Hub MCP adapter and configured Context assembly | `src/memory`, service host, unit tests and environment-gated real MCP test | adapter, service health probe, and sandboxed local round trip passing; remote CI environment-gated |
| Skill Engine | exact dependency graph, budgets, signatures, live revocation, transparency receipts, pinned HTTPS source, bounded trusted and signed-External install/list/verify/recoverable-remove lifecycle, explicit project-configured activation and publisher/log diagnostic locks | `src/skill`, service host, ADRs 0009/0014/0032/0033/0085/0088/0091/0102 | local governed lifecycle and public-HTTPS install path passing by composed contract; automatic update, dependency acquisition, catalog/private registry, and live public fixture remain open |
| Policy Engine | deny/allow/ask, risk class, trusted per-Turn actor/tenant Authority Context, attributed durable approval, exact-tenant Memory/SQLite fencing, restart-safe inbox and continuation, CAS, exact-actor separation of duty, validated lookup projection, backup-first schema-1/schema-2 migration | `src/runtime/policy.rs`, `src/approval`, Authority propagation, tenant-fencing, migration, restart, and drift tests, ADRs 0007/0024/0049–0051/0063/0065/0116–0118 | local baseline passing; role/quorum policy, signed receipts, delegation, and tenant transfer open |
| Orchestration | bounded DAG, executable TaskExecutor scheduler, dependency concurrency, timeout/panic isolation, leases/fencing, paged TaskMailbox messaging, default-deny Workspace Provider lifecycle, isolated local directories, pinned detached Git Worktrees, Artifacts, memory and SQLite coordination, durable optional Task Graph tenant ownership, append-only exact-attempt execution binding with retry anti-downgrade, and serviceable authenticated worker protocol | `src/orchestration`, `src/protocol/task.rs`, schema-1/schema-2 migration/restart/evidence-smuggling/projection-drift/tenant-fencing tests, `examples/orchestrated.rs`, ADRs 0011/0019/0052/0053/0071/0072/0073/0074/0119/0123 | embedded governed and protocol unbound tenant-fenced worker lifecycles passing for single-host/multiprocess coordination; remote binding control/evidence inspection, multi-node consensus, tenant transfer, and durable orphan reconciliation open |
| Verification | typed completion gates, retryable correction, hard failure settlement, exact Turn cancellation, and configured brokered JSON-command Verifiers | `src/verification`, `src/execution`, real service-process verification, Runtime verification tests, ADRs 0008/0106 | local and configured-process baselines passing |
| Observability | content-free phase records, latency/outcome/accounting, distinct registered and Provider-reported Model identities, exact integer provider-cost evidence, typed Provider failure class/status/retry evidence, invoked Model retry indices, panic isolation, bounded collector, allocation-bounded JSONL | `src/observability`, ADRs 0017/0064/0083/0084/0100/0101 | local baseline passing |
| Evaluation | bounded parallel cases/graders, exact per-Grader cancellation, configured brokered JSON-command Graders, isolated in-memory `yh eval`, format-2 root validation, origin-bound exact baselines, required-pass gates, versioned end-to-end smoke suite, external-run formats 1/2/3/4/5/6, Codex CF-003 formats 7/8, Y-Harness process-restart format 9, controller-owned deterministic Tool fault oracle | `src/evaluation`, `src/execution`, `evals`, `yh eval`, `yh eval-smoke`, `tools/benchmark-runner`, ADRs 0010/0026/0064/0067/0069/0079/0080/0081/0082/0090/0107/0109/0110/0111/0112/0113 | executable built-in and configured-process baselines, bounded Claude Code, Codex, Grok Build, Pi, OpenCode, and Hermes Agent adapter contracts, real non-claim Claude/Grok/Pi/OpenCode/Hermes/Codex records, and a real non-claim Y-Harness explicit-recovery/non-replay record |

## Product and integration boundary

| Deliverable | Evidence | Status |
|---|---|---|
| Headless embeddable Rust core | public contracts in `src/lib.rs`; zero-default build; external-view examples execute a Policy-controlled Model/Tool loop and a fenced Task DAG | compiled and run locally; CI-gated |
| Reference-project derivation | primary-source comparison, immutable open-source snapshots, adopted/rejected decisions, code/ADR mapping in `reference-analysis.md` | documented and link-checked locally |
| Serviceable typed protocol | language-neutral v25 specification, exact envelope and State-13/Approval-3/Task-3/Secret-2/Model-Gateway-7 compatibility coordinates, schema-13 Turn-binding observation without remote authorship, embedded-only Task-attempt binding coordinate, panic-isolated trusted Authority resolution, exact Thread/Operation/Approval/Task tenant fencing, permissioned expected-Turn recovery takeover, bounded per-Turn Context, lineage-aware Thread summaries, atomic retry-identified Thread forks, durable Thread names/import provenance, async Turn operations, exact-ID Steering, conditional Task Graph discovery, authenticated fenced worker lifecycle, bounded paging, cancellation, shutdown | `docs/protocol.md`, `src/protocol`, process/TLS/fault tests; passing locally |
| Engine CLI | `yh init/doctor/serve/eval`, no-clobber `yh thread export` and atomic `yh thread import`, trusted/signed/HTTPS `yh skill install*` plus list/verify/remove, durable State/Approval/Task databases, deterministic demo, strict configured Model catalog/route, direct OpenAI Responses, HTTPS Gateways, versioned brokered JSON-command Models, semantic Conversation Compactors, completion Verifiers, and Evaluation Graders, JSON/MCP Tool, project Skill and Agent Memory Hub assembly, `src/reference_cli`, process tests | archive round-trip/tamper/no-clobber, multi-Provider route diagnostics, real compatible command-Model Turn with External State provenance, real settlement-v1 typed retry, real command-compactor Turn with immutable source history and durable summary provenance, real command-Verifier completion gate and durable result, isolated command-Grader Evaluation with exact baseline, installed-binary, signed External Skill trust/revocation/transparency lifecycle, project Skill integrity, and restart tests passing locally; live OpenAI and public Skill endpoints environment-gated |
| Optional full-screen TUI | separate `y-harness-tui` package and `yh-tui` binary in `clients/tui`; Protocol-v24-only child transport, execution-binding audit rendering, bounded tenant/lineage-aware recent-Thread navigation/resume, `/fork [terminal-turn-id]`, exact-ID active-Turn Steering, invalidated provisional-output handling, content-free continuation and ordered batch rendering, TestBackend render tests, real PTY Turn and fork | independently installable; local unit/lint/PTY gates passing; Pi-style entry-level in-place navigation is intentionally outside the Engine model |
| Optional Domain Pack control plane | separate `y-harness-domain-pack` library in `control/domain-pack`; format/store schema 1, immutable exact component pins, pinned-suite evaluation, independent approval, tenant-fenced Memory/SQLite activation and bounded rollback, execution-time inventory/revision proof converted to generic Engine Turn binding | public zero-network lifecycle example plus canonical/tamper, lifecycle, cross-tenant, reopen, projection-drift, failed-evaluation, inventory-drift, conversion, and two-connection CAS tests passing; no Protocol/CLI lifecycle integration, Task-attempt binding, or business behavior in Core |
| Competitive benchmark tools | independent `y-harness-benchmark-runner` and `y-harness-fault-fixture` packages; bounded shell-free Claude Code JSON, Codex JSONL, Grok Build headless JSON, Pi JSONL, OpenCode JSONL, Hermes one-shot/usage, and Y-Harness service-restart adapters; exact binary coordinates; external-run formats 1–6, Codex CF-003 formats 7/8, and Y-Harness CF-003 format 9; deterministic loopback Messages/Responses Providers, stdio MCP crash/hold-after-effect fixture, spec-bound JSON-command Model, and durable oracle | contract tests plus real `claim_eligible: false` Claude, Codex, Grok, Pi, OpenCode, and Hermes fixed-output, Codex single-process/restart and Y-Harness explicit-recovery CF-003 records; no comparative case exists |
| Install and operator path | Cargo-backed no-side-effect install script, strict config template, Chinese quick start, real language-neutral Task Worker, acceptance checklist | clean-prefix install and revision-6 worker lifecycle passing locally |
| MCP tools | official SDK stdio plus optional authenticated HTTPS JSON-response clients, atomic namespaced Tool registration, explicit activation, optional command-file lock, bounded cooperative cancellation and session settlement, default tenant-scoped shared-session denial | Runtime cancellation/deadline/tenant-admission, real stdio hold-after-effect/process settlement, and private-TLS unscoped remote service assembly passing; tenant-partitioned sessions, SSE/OAuth, and protocol-level rollback acknowledgement not claimed |
| Agent Memory Hub | first-party provider adapter over persistent stdio MCP | live local round trip passing; CI environment-gated |
| External model provider | exact-versioned HTTPS JSON/NDJSON gateway, direct OpenAI Responses JSON/SSE, and brokered language-neutral JSON-command output-v1/settlement-v1; secret references, Provider-reported usage/settled Model/request evidence where supported, exclusive private-CA trust and mTLS gateway identity, bounded origin-bound continuation, typed failure facts | local private-gateway TLS, OpenAI mapping/stream/persistence/replay/tamper, compatible command-Model service Turn, settlement evidence, and typed retry passing; command Models deliberately do not claim provisional streaming; live gateway and OpenAI API environment-gated |
| External executable capabilities | deny-by-default Process Broker and bounded JSON adapters | passing locally |
| GUI/LUI/VUI/IDE/API products | Web/Desktop/Mobile, Web Chat/Desktop Chat/IM, Voice, IDE, API/SDK/Webhook remain independent optional adapters over the same protocol | future clients, never duplicate runtimes |

## Local gate evidence

The following commands passed on 2026-07-29 with Rust 1.88:

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
cargo run --locked -p y-harness-domain-pack --example governed_release
cargo run --locked -- eval-smoke
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
cargo audit --deny warnings
cargo package --locked -p y-harness
./scripts/install.sh --root /isolated/prefix
./scripts/install-tui.sh --root /isolated/tui-prefix
cargo build --locked -p y-harness -p y-harness-tui
python3 scripts/smoke-tui.py
python3 scripts/smoke-tui.py --configured
cargo build --locked --release -p y-harness -p y-harness-tui
python3 scripts/smoke-tui.py --tui target/release/yh-tui --engine target/release/yh
python3 scripts/smoke-tui.py --configured \
  --tui target/release/yh-tui --engine target/release/yh
YH_BIN=/isolated/prefix/bin/yh python3 examples/task_worker_client.py \
  /isolated/project/y-harness.json
```

The all-feature workspace run contains 385 passing Core library tests plus
3 manual size tests, 8 Domain Pack control-plane tests, 11 CLI configuration
tests, 24 Engine process/service tests, 11 TUI unit/render tests, 2 local
private-gateway TLS integration tests, 2 local private-MCP TLS integration
tests, 53 product/Engine evidence-adapter tests, and 7 deterministic
fault-fixture tests: 509 passing plus 8 explicitly ignored fixtures in total.
The no-default-feature workspace run contains 479 passing tests plus 4 ignored
manual/environment fixtures. The demo and configured PTY
smoke gates submit real Turns, create atomic child Threads, verify parent/child
history plus durable lineage in State, and check alternate-screen and
bracketed-paste restoration. The 64 MiB State migration, 126.9 MiB Approval
Inbox migration, and near-limit 1,000-Task Graph migration tests are
deliberately manual.
The 238-file / 3.3 MiB Core package (721.2 KiB compressed) verifies from the
current State-13/Approval-3/Task-3/Secret-2/Protocol-25 candidate; Cargo's
`--allow-dirty` escape hatch is not used.
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
cargo clippy --locked -p y-harness-domain-pack --lib -- \
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
has been produced yet. The first shared-Provider Claude Code/Codex preflight
completed but correctly returned `not_comparable`: the common requested Model
identifier triggered Codex fallback metadata, and protocol, Tool, reasoning,
sandbox, Context, and settlement controls remained unequal.

The follow-up Codex/Grok Build preflight aligned the main protocol and
`gpt-5.4` identifier but remained `not_comparable`: Grok attempted a rejected
`grok-4.5` title call, while Tool, Context, reasoning-summary, permission,
call-count, and identity-settlement controls still differed.

“No bugs” is not a verifiable permanent state. The enforceable completion rule
is zero known critical/high defects plus named, reproducible evidence for every
supported claim.
