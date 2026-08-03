# Harness completion audit

This audit maps the `Agent = LLM × Harness` scope to implementation and
executable evidence. It is not a release declaration. The authoritative open
blockers remain in [`release-readiness.md`](release-readiness.md).

## Eleven-layer evidence matrix

| Layer | Implemented baseline | Primary evidence | Status |
|---|---|---|---|
| Context Engine | deterministic blocks, whole-Turn history, memory packs, registered Token Counters and semantic Compactors, configured brokered JSON-command compaction, independent byte/token budgets, derived-summary provenance, attributed per-Turn Context, and format-1 digest-bound Thread-handoff preparation | `src/context`, `src/execution`, real service-process compaction, Context/Runtime compaction, invocation-context, and handoff tests, ADRs 0021/0059/0060/0061/0064/0096/0097/0105 | local baseline passing |
| Agent Loop | bounded model/tool/verification loop, atomic same-response multi-Tool decisions with explicit bounded safe-run concurrency, sequential fences, source-ordered settlement, explicit ordered and attempt-deadlined Model failover, observable timeout-only cooldown with last-resort fail-open, bounded typed Provider failure evidence, default-disabled typed same-Model retries with shared deadlines and provisional-output fencing, independent retry/failover attempt budget per step and derived whole-Turn ceiling, exact failure-cycle Progress Governor, cancellation, deadlines, concurrency admission, permissioned exact-Turn recovery, current-candidate/Steering fence, atomic generation-bound successful settlement, and schema-16 single non-batch Approval worker release with exact wait/resume/claim/cancel/timeout/deny State fencing | `src/runtime`, `src/completion.rs`, typed-failure/retry/trace, shared attempt-budget retry/failover tests, safe-parallel/fence/timeout, cooldown/trace/fail-open, ordered-batch, durable Approval wait/restart/denial/due-index/capacity tests, failover/provenance, completion/Steering race tests, Memory/SQLite restart tests, Protocol-37/TUI wait tests, and process-level CF-003 recovery, ADRs 0065/0070/0086/0098/0099/0100/0101/0113/0114/0155/0156/0157/0158/0159 | implemented local Approval slice with bounded host-driven expiry; calls hidden inside arbitrary extension implementations, batch worker release, `HumanInput`, durable Inbox repair, finite worker leases, `NeedsReconciliation`, frozen Context capsules, and cross-process resume receipts remain open |
| Tool Runtime | typed registry, configured shell-free JSON Tools, exact-selected stdio or authenticated HTTPS JSON-response MCP Tools, additive evidence-aware Connector contract with Runtime-bound identity/origin/authority/output digest, explicit activation, optional startup command lock, optional one-shot per-dispatch SHA-256 drift lock with frozen integrity evidence, default-deny bounded MCP launch, bounded cancellation/deadline session settlement, tenant-scoped shared-session denial, Unix process-group settlement, reusable macOS sandbox, explicit compensation | `src/execution`, `src/transport`, Runtime Connector/cancellation/deadline and tenant-admission tests, digest drift/recovery tests, real hold-after-effect stdio fixture, ADRs 0013/0036/0043/0054/0056/0062/0066/0076/0088/0103/0115/0120/0126/0138 | local and private-TLS remote unscoped baselines passing; atomic executable-to-exec binding, transitive artifact integrity, tenant-partitioned sessions, bounded SSE/OAuth, escape-resistant Linux, and Linux/Windows persistent containment open |
| State Engine | typed journal, memory/SQLite stores, CAS, checkpoints, snapshots, capacity/recovery bounds, schema-1 through schema-15 to schema-16 backup-first migration, authoritative optional Thread tenant ownership with validated lookup projections, immutable content-free Turn execution binding, atomic digest-bound Connector evidence, origin-bound Provider Continuation, durable safe-boundary Steering, atomic ordered Tool-call batches, explicit Thread names, terminal-boundary atomic forks, lineage-aware bounded summaries, format-6 portable integrity-bound Thread archives, caller-attributed content-free invocation Context, atomic `TurnCompleted` plus deterministic receipt, and same-journal durable Approval wait/resume/claim/closure/denial evidence | `src/state`, `src/completion.rs`, schema-1 through schema-15 migration fixtures, schema-16 projection/CAS/capacity/snapshot/archive tests, CompletionReceipt validation/snapshot/fork/archive/import/legacy fixtures, Connector digest/origin/authority/archive tests, execution-binding tenant/duplicate/reopen/snapshot/archive tests, invocation-context provenance tests, archive tamper/no-clobber/idempotency/reopen tests, fork rollback/reopen/idempotency/summary, name drift, batch, continuation, and Steering fault tests, ADRs 0061/0065/0068/0077/0078/0086/0092/0093/0094/0095/0096/0117/0122/0126/0157/0158 | receipt-free schema-1 through schema-14 success remains readable only as legacy/unverified; legacy Running Turns never gain inferred waits; inherited fork/import evidence retains source proof rather than claiming target re-execution; active wait indexing/sweeping, repair outbox/tombstones, external Artifact blob storage/authorization, and destructive archival/offload remain open |
| Memory Engine | versioned provider port, scoped provenance, Agent Memory Hub MCP adapter and configured Context assembly | `src/memory`, service host, unit tests and environment-gated real MCP test | adapter, service health probe, and sandboxed local round trip passing; remote CI environment-gated |
| Skill Engine | exact dependency graph, budgets, signatures, live revocation, transparency receipts, pinned HTTPS source, digest-pinned Catalog resolution, named public/private Registries with request-scoped Bearer and exclusive private CA, bounded trusted and signed-External install/list/verify/recoverable-remove lifecycle, explicit project-configured activation and publisher/log diagnostic locks | `src/skill`, service host, ADRs 0009/0014/0032/0033/0085/0088/0091/0102/0145/0148 | local governed lifecycle, signed dependency acquisition, and private-TLS Registry CLI path passing; mirror federation, OAuth, npm/git/temporary sources, executable extensions, and a live public fixture remain open |
| Policy Engine | deny/allow/ask, risk class, trusted per-Turn actor/tenant Authority Context, attributed durable approval, exact-tenant Memory/SQLite fencing, restart-safe inbox and continuation, CAS, exact-actor separation of duty, validated lookup projection, backup-first schema-1/schema-2 migration | `src/runtime/policy.rs`, `src/approval`, Authority propagation, tenant-fencing, migration, restart, and drift tests, ADRs 0007/0024/0049–0051/0063/0065/0116–0118 | local baseline passing; role/quorum policy, signed receipts, delegation, and tenant transfer open |
| Orchestration | bounded DAG, executable TaskExecutor scheduler, dependency concurrency, timeout/panic isolation, leases/fencing, bounded exact Task execution requirements matched only against trusted embedded Worker capabilities, paged TaskMailbox messaging, default-deny Workspace Provider lifecycle, isolated local directories, pinned detached Git Worktrees, Artifacts, memory and SQLite coordination, durable optional Task Graph tenant ownership, append-only exact-attempt execution binding with retry anti-downgrade, fail-closed serviceable authenticated worker protocol, independent schema-1 Workflow and Human Handoff aggregates, independent schema-1 Effect Ledger with tenant-scoped idempotency uniqueness, finite attempt leases, fail-closed unknown outcomes, explicit reconciliation and receipts, embedded default-deny Governed Effect Executor API 1, independent durable Effect dispatch-governor API/schema 1 with tenant-exact fixed-window and epoch-fenced circuit state, embedded default-deny Governed Effect Reconciler API 1, exact brokered JSON Effect Connector protocol 1, typed per-dispatch Secret resolution into non-serializable process buffers after an integrity preflight and before a second Broker measurement, embedded Temporal Driver API 3 for bounded Workflow/Handoff/Effect/State due advancement, an opt-in reference-service Temporal lifecycle, and independently optional fixed-authority Effect execution/reconciliation consumer loops with separate exact registries/allowlists, mandatory per-dispatch command digest locks, optional persistent execution governance, and bounded cadence/backoff | `src/orchestration`, `src/workflow`, `src/human_handoff`, `src/effect`, `src/execution/effect.rs`, `src/temporal`, `src/reference_cli/{temporal_service,effect_service}.rs`, `src/protocol/{task,workflow,human_handoff,effect}.rs`, restart/tamper/conflict/tenant-fencing/capability-matching/temporal-race/service-lifecycle/degrade-recover/command-drift/Secret-gating/non-leakage/governor-rate-circuit tests, `examples/{orchestrated,effect_executor,effect_reconciler,json_effect_connector}.rs`, ADRs 0011/0013/0019/0052/0053/0071–0074/0119/0123/0127–0130/0133–0142/0159 | embedded governed and protocol tenant-fenced Task/Workflow/Handoff/Effect lifecycles, bounded host ticks including Agent Loop wait settlement, fixed-authority Temporal and Effect service polling, bounded policy-gated Connector execution, durable tenant/lane dispatch governance, bounded policy-gated authoritative read-only unknown convergence, and reference-backed per-dispatch Effect credential custody pass for single-host/multiprocess SQLite coordination; authenticated remote Worker Registry, fleet placement/fairness, atomic prepared-executable binding, raced post-preflight Provider issuance, OS/child-copy erasure, vault/rotation integration, compensation execution, channel routing, external receipt verification, distributed multi-region governance, multi-node query ownership, tenant transfer, and multi-node consensus remain open |
| Verification | typed completion gates, retryable correction, hard failure settlement, exact Turn cancellation, configured brokered JSON-command Verifiers, frozen origin/contract manifest, candidate-bound outcomes, and deterministic Turn-internal completion proof | `src/verification`, `src/completion.rs`, `src/execution`, real service-process verification, Runtime completion/current-candidate tests, ADRs 0008/0106/0157 | format-1 proof is bounded to the owning Turn; cross-aggregate Artifact, Effect, business delivery, channel delivery, and post-terminal jobs are not claimed |
| Observability | content-free phase records, latency/outcome/accounting, distinct registered and Provider-reported Model identities, exact integer provider-cost evidence, typed Provider failure class/status/retry evidence, invoked Model retry indices, panic isolation, bounded collector, allocation-bounded JSONL | `src/observability`, ADRs 0017/0064/0083/0084/0100/0101 | local baseline passing |
| Evaluation | bounded parallel cases/graders, exact per-Grader cancellation, configured brokered JSON-command Graders, isolated in-memory `yh eval`, format-2 root validation, origin-bound exact baselines, required-pass gates, versioned end-to-end smoke suite, external-run formats 1/2/3/4/5/6, Codex CF-003 formats 7/8, Y-Harness process-restart format 9, controller-owned deterministic Tool fault oracle | `src/evaluation`, `src/execution`, `evals`, `yh eval`, `yh eval-smoke`, `tools/benchmark-runner`, ADRs 0010/0026/0064/0067/0069/0079/0080/0081/0082/0090/0107/0109/0110/0111/0112/0113 | executable built-in and configured-process baselines, bounded Claude Code, Codex, Grok Build, Pi, OpenCode, and Hermes Agent adapter contracts, real non-claim Claude/Grok/Pi/OpenCode/Hermes/Codex records, and a real non-claim Y-Harness explicit-recovery/non-replay record |

## Product and integration boundary

| Deliverable | Evidence | Status |
|---|---|---|
| Headless embeddable Rust core | public contracts in `src/lib.rs`; zero-default build; external-view examples execute a Policy-controlled Model/Tool loop, a fenced Task DAG, a default-deny durable Effect Connector, authoritative read-only Effect reconciliation, and real shell-free JSON-command execution/reconciliation subprocesses; embedded Temporal Driver API 3, Effect Executor API 1, Effect Reconciler API 1, and JSON Effect Connector protocol 1 advance authoritative state without owning a host loop | compiled and run locally; CI-gated |
| Reference-project derivation | primary-source comparison, immutable open-source snapshots, adopted/rejected decisions, code/ADR mapping in `reference-analysis.md` | documented and link-checked locally |
| Serviceable typed protocol | language-neutral v37 specification, exact envelope and State-16/archive-6/Approval-3/Task-4/Workflow-1/Handoff-1/Effect-1/Secret-3/Model-Gateway-7 compatibility coordinates, atomic `turn_completed` projection, durable-wait discovery/resume/cancel and Waiting Operation status, terminal receipt digest, panic-isolated trusted Authority resolution, exact tenant fencing, conditional subsystem discovery, credential-free Runtime/Registry catalog, authoritative content-free ready/capacity/draining status, command-specific lifecycle authorization, bounded paging, cancellation, and shutdown | `docs/protocol.md`, `src/protocol`, durable-wait, process/TLS/fault tests; receipt digest correlates to authoritative State and is not an external-delivery proof |
| Engine CLI | `yh init/doctor/serve/eval`, read-only preflight of all existing authoritative stores before external capability construction, optional fixed local-process tenant authority, opt-in bounded Temporal lifecycle, opt-in separately authorized brokered Effect execution/reconciliation lifecycle with dispatch-locked commands and optional per-dispatch Secret references, host-owned SIGTERM/SIGINT/Ctrl-C drain with frame-atomic cooperative JSONL cancellation and bounded detached stdin buffering, no-clobber `yh thread export` and atomic `yh thread import`, trusted/signed/HTTPS `yh skill install*` plus list/verify/remove, durable State/Approval/Task/Workflow/Handoff/Effect databases, deterministic demo, strict configured Model catalog/route, direct OpenAI Responses, HTTPS Gateways, versioned brokered JSON-command Models, semantic Conversation Compactors, completion Verifiers, and Evaluation Graders, JSON/MCP Tool, project Skill and Agent Memory Hub assembly, `src/reference_cli`, process tests | fresh/current store status plus legacy/partial-store no-mutation failure, fixed-tenant Protocol/State/Task/Workflow/Handoff/Effect/archive, direct-Model Secret and Evaluation evidence; unscoped archive round-trip/tamper/no-clobber, multi-Provider route diagnostics, real compatible command-Model Turn with External State provenance, real settlement-v1 typed retry, real command-compactor Turn with immutable source history and durable summary provenance, real command-Verifier completion gate and durable result, isolated command-Grader Evaluation with exact baseline, real cross-process due-Workflow polling, real Effect command execution/invalid-reconciliation degradation/Secret injection and non-leakage/recovery/terminal restart non-replay, normal EOF plus live-pipe SIGTERM clean shutdown, installed-binary, signed External Skill trust/revocation/transparency lifecycle, project Skill integrity, and restart tests passing locally; multi-principal routing, tenant-partitioned MCP, live OpenAI, and public Skill endpoints remain open or environment-gated |
| Optional full-screen TUI | separate `y-harness-tui` package and `yh-tui` binary in `clients/tui`; Protocol-v37-only child transport with actionable coordinate mismatch diagnostics, credential-free Runtime/Registry inspector, authoritative admission rendering, durable-wait `WAITING`/`READY`/claimed status, restart discovery, `/resume` and `/cancelwait`, explicit durable `local/demo` labeling, exact capacity pressure, tenant/lineage-aware Thread navigation, fork, Steering, Tool Trace, and receipt rendering | independently installable and State-authoritative through Protocol only; durable release is limited to one non-batch Approval Tool call; Approval settlement remains a separate authenticated surface; Effect control UI is not yet implemented |
| Optional HTTP deployment probes | feature-isolated `src/transport/http_probe.rs` adapter and strict `http_probe` reference-host configuration; `GET /livez` and `GET /readyz` translate the same in-process Protocol-v37 admission source; one request per connection, finite header/connection/time/drain bounds, fixed content-free non-cacheable replies, ADR 0151 | library state/error/framing tests and real configured `yh serve` child pass; disabled by omission and rejected without the feature; no TLS/auth, dependency-health graph, metrics, Agent HTTP API, manifest, or multi-node load claim |
| Optional Domain Pack control plane | separate `y-harness-domain-pack` library in `control/domain-pack`; format/store schema 1, immutable exact component pins, exact actor/tenant/action authorization port plus bounded no-fallback RBAC reference, pinned-suite evaluation, independent approval, tenant-fenced Memory/SQLite activation and bounded rollback, execution-time proof converted to generic Engine Turn and Task-attempt binding | public zero-network lifecycle example plus canonical/tamper, complete authorized lifecycle, cross-tenant/no-fallback, panic-fail-closed/no-mutation, administrator separation-of-duty, reopen, projection-drift, failed-evaluation, inventory-drift, conversion, and two-connection CAS tests passing; no Protocol/CLI lifecycle integration, remote binding authorship/evidence surface, external IAM adapter, or business behavior in Core |
| Competitive benchmark tools | independent `y-harness-benchmark-runner` and `y-harness-fault-fixture` packages; bounded shell-free Claude Code JSON, Codex JSONL, Grok Build headless JSON, Pi JSONL, OpenCode JSONL, Hermes one-shot/usage, and Y-Harness service-restart adapters; exact binary coordinates; external-run formats 1–6, Codex CF-003 formats 7/8, and Y-Harness CF-003 format 9; deterministic loopback Messages/Responses Providers, stdio MCP crash/hold-after-effect fixture, spec-bound JSON-command Model, and durable oracle | contract tests plus real `claim_eligible: false` Claude, Codex, Grok, Pi, OpenCode, and Hermes fixed-output, Codex single-process/restart and Y-Harness explicit-recovery CF-003 records; no comparative case exists |
| Install and operator path | Cargo-backed no-side-effect install script, strict config template, Chinese quick start, real language-neutral Task Worker, acceptance checklist | clean-prefix install and revision-6 worker lifecycle passing locally |
| MCP tools | official SDK stdio plus optional authenticated HTTPS JSON-response clients, atomic namespaced Tool registration, explicit activation, optional command-file lock, bounded cooperative cancellation and session settlement, default tenant-scoped shared-session denial | Runtime cancellation/deadline/tenant-admission, real stdio hold-after-effect/process settlement, and private-TLS unscoped remote service assembly passing; tenant-partitioned sessions, SSE/OAuth, and protocol-level rollback acknowledgement not claimed |
| Agent Memory Hub | first-party provider adapter over persistent stdio MCP | live local round trip passing; CI environment-gated |
| External model provider | exact-versioned HTTPS JSON/NDJSON gateway, direct OpenAI Responses JSON/SSE, and brokered language-neutral JSON-command output-v1/settlement-v1; secret references, Provider-reported usage/settled Model/request evidence where supported, exclusive private-CA trust and mTLS gateway identity, bounded origin-bound continuation, typed failure facts | local private-gateway TLS, OpenAI mapping/stream/persistence/replay/tamper, compatible command-Model service Turn, settlement evidence, and typed retry passing; command Models deliberately do not claim provisional streaming; live gateway and OpenAI API environment-gated |
| External executable capabilities | deny-by-default Process Broker, bounded JSON adapters, and optional dispatch-time exact-path SHA-256 drift locks | matching/drift/restore/cancel/path/symlink tests passing; atomic OS exec binding and transitive dependency integrity not claimed |
| GUI/LUI/VUI/IDE/API products | Web/Desktop/Mobile, Web Chat/Desktop Chat/IM, Voice, IDE, API/SDK/Webhook remain independent optional adapters over the same protocol | future clients, never duplicate runtimes |

## Local gate evidence

The following commands passed on 2026-08-01 with Rust 1.88:

```bash
git diff --check
cargo fmt --all -- --check
cargo check --locked --no-default-features
cargo check --locked --no-default-features --features https-model
cargo check --locked --no-default-features --features https-mcp
cargo check --locked --no-default-features --features https-skill
cargo check --locked --no-default-features --features http-probe
cargo check --locked --no-default-features --features tls-host
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --no-default-features
cargo test --locked --workspace --all-targets --all-features
cargo test --locked -p y-harness-tui
cargo clippy --locked -p y-harness-tui --all-targets -- -D warnings
cargo run --locked --example embedded
cargo run --locked --example orchestrated
cargo run --locked --example temporal_driver
cargo run --locked -p y-harness-domain-pack --example governed_release
cargo run --locked -- eval-smoke
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
cargo audit --no-fetch --no-yanked --deny warnings
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

The all-feature workspace run contains 549 passing Core library tests plus
3 manual size tests, 13 Domain Pack control-plane tests, 11 aquaculture Domain
Pack tests, 23 CLI configuration tests, 30 Engine process/service tests, 22 TUI
unit/render tests, 2 local private-gateway TLS integration tests, 2 local
private-MCP TLS integration tests, 1 local private-Skill-Registry TLS test, 1
Skill Registry process test, 59 product/Engine evidence-adapter tests, and 7
deterministic fault-fixture tests: 725 passing plus 11 explicitly ignored
fixtures in total.
The no-default-feature workspace run contains 666 passing tests plus 4 ignored
manual/environment fixtures. The demo and configured PTY
smoke gates submit real Turns, create atomic child Threads, verify parent/child
history plus durable lineage in State, and check alternate-screen and
bracketed-paste restoration. The 64 MiB State migration, 126.9 MiB Approval
Inbox migration, and near-limit 1,000-Task Graph migration tests are
deliberately manual.
The local 1,173-entry RustSec advisory database reports no known vulnerability
across 290 locked crates. The live crates.io yanked-state lookup timed out on
2026-07-30, so that network-dependent check is not represented as locally
complete and remains part of the remote CI gate.
The previous clean Core package gate used Cargo without `--allow-dirty`. The
current Protocol-37 candidate is intentionally uncommitted and is therefore
not represented as clean package evidence; the same gate must run again on
the release commit.
Eight integration tests are ignored by the ordinary suite unless their explicit
external fixtures are configured: Agent Memory Hub, Anthropic Messages,
Gemini `generateContent`, ordinary and streaming HTTPS model gateway, OpenAI
Chat Completions, OpenAI Responses, and pinned public HTTPS Skill acquisition.
The Agent Memory Hub test passed against the local
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
