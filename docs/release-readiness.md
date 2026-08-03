# Release readiness

This document separates executable evidence from roadmap claims. Y-Harness is
not release-ready while any blocking row remains open.

## Current evidence

| Gate | Evidence | Current result |
|---|---|---|
| Minimum compiler | Rust 1.88 `check`, Clippy, tests, docs | passing |
| Read-only service-store preflight | `doctor` and `serve` validate existing State/Approval/Task/Workflow/Handoff/Effect/Effect-governance stores before external capability construction; concrete adapters use read-only `query_only`; missing stores remain bootstrap-eligible; migration remains explicit and backup-first | fresh/current-store status, legacy State no-mutation/provider-order, and partial Workflow/Effect/Effect-governance fail-closed tests passing; authoritative service open still revalidates |
| Feature isolation | zero-default core, each optional feature, and all features | passing |
| Deterministic tests | 549 Core library, 13 Domain Pack control-plane, 11 aquaculture Domain Pack, 23 CLI unit, 5 Protocol/CLI integration, 30 Engine process/service, 22 TUI unit/render, 2 private-gateway TLS, 2 private-MCP TLS, 1 private-Skill-Registry TLS, 1 Skill Registry process, 59 product/Engine evidence-adapter, and 7 fault-fixture tests | passing locally: 725 total plus 11 explicitly ignored manual/environment fixtures; zero-default workspace passes 666 plus 4 ignored |
| Full-screen TUI PTY | demo and configured Engine modes; real Turn, atomic Thread fork, durable State, alternate screen and bracketed-paste restoration; exact protocol mismatch reports both coordinates and same-checkout reinstall commands; empty/short conversation hierarchy, nonzero sub-percent capacity, separate Thread-count/global-sequence labels, and durable `local/demo` labeling are render-tested | debug and release binaries passing |
| Installed operator path | isolated-prefix Engine and TUI installs; version, init, doctor, persistent service, optional HTTP probes, demo, Task DAG, Mailbox, durable Workflow Run, Human Handoff, and Effect Ledger | passing; Engine install includes feature-gated probe support but opens no listener without configuration; installed `doctor` accepts the bounded example; TUI install contains only `yh-tui`; Task Graph terminal at revision 6 and Workflow/Handoff/Effect restart recovery are process-tested |
| Distribution package | `cargo package --locked -p y-harness`, clean-room Core crate verification | the prior clean package gate passed; the current Protocol-37 worktree is intentionally uncommitted and therefore is not claimed as clean package evidence. Rerun without `--allow-dirty` on the release commit; the optional control-plane crate remains a separate workspace package |
| Immutable release coordinate | clean worktree, exact tag-to-HEAD binding, matching Engine/TUI package versions, versioned notes, required distribution inputs, valid diff, and locked Cargo metadata | verifier passes in a synthetic clean exact-tag fixture and rejects the live dirty candidate plus missing tags; exact release-commit execution remains open |
| Atomic supply-chain publication | all local release gates repeated on the tag; Linux/macOS/Windows test and build jobs stage six archives; six-member CycloneDX 1.5 workspace SBOM; exact seven-archive `SHA256SUMS`; build-provenance attestation; public release created only after every dependency succeeds and never overwritten | workflow is pinned and `actionlint 1.7.12` passes; six SBOM documents were generated, parsed, counted, and archived locally with `cargo-cyclonedx 0.5.9`; no hosted attestation or release exists before the remote workflow runs |
| Real memory integration | Agent Memory Hub stdio MCP round trip under macOS Seatbelt, network denied, offline embeddings | passing |
| Dependency security | `cargo-audit 0.22.2` over 290 locked crates and a 1,173-advisory local RustSec database | no known vulnerability in the local scan; the latest live crates.io yanked-state recheck timed out and remains a remote-CI gate |
| State performance | schema-12 tenant-fenced 1,000 events plus 64-Thread lineage page, 5 samples, SQLite WAL + FULL | 93.103 ms append; 2.711 ms full projection; 9.175 ms atomic fork; 0.278 ms Thread list; 2.410 ms snapshot load |
| State migration | schemas 1–15 → 16 plus projection-only schema-16 → wait-projection-1, immutable-history backup-first path | legacy sources are coordinate- and SHA-256-bound and restartable; projection-only migration preserves current snapshots and streams only schema-16 wait lifecycle events to rebuild the disposable index; ownership, waits, and completion proof are never inferred from legacy history |
| Tenant-owned Thread State | schema-12 authoritative creation evidence plus disposable validated projections; exact Authority tenant on Thread, Turn, recovery, fork, handoff, archive, and retained Operation access | Memory/SQLite cross-tenant denial, archive rebind, protocol capability fail-closed, and SQLite reopen tests passing; external Artifact blob authorization and Domain Pack service integration remain open |
| Atomic Thread fork | terminal Turn boundary, caller-owned child retry identity, direct lineage, Memory/SQLite atomic creation | boundary/idempotency/reopen tests passing; injected SQLite uniqueness failure leaves no child stream or projection |
| Portable Thread archive | bounded format-6 source journal and SHA-256, terminal-only export, no-clobber CLI file adapter, schema-10 import provenance, schema-12 target-tenant rebind for unbound history, same-tenant preservation for schema-13 execution binding and schema-14 Connector authority evidence, byte-preserved schema-15 CompletionReceipt and schema-16 Agent Loop wait evidence, caller-owned idempotency identity, Memory/SQLite atomic import | round-trip/tamper/unknown-field/conflict/reopen/snapshot/cross-tenant, binding-rebind, Connector-rebind-denial, receipt- and wait-evidence-preservation paths are covered; inherited evidence never claims target re-execution; schema-12 1,000-Item export/import medians remain historical performance evidence |
| Per-Turn reference Context | 64-block/1 MiB pre-State bounds, caller attribution, source/body SHA-256, schema-11 content-free evidence, approval-resume request binding, Skill-only provider instruction authority | Context/Runtime/State/Protocol/OpenAI mapping tests passing; branch-summary synthesis remains an optional-module gap |
| Approval migration | schemas 1/2 → 3, up to 256 records / 133,038,080 record bytes | backup-first, SHA-256-bound, restartable; schema-2 lifecycle preserved as unscoped |
| Task Graph migration | schemas 1/2/3 → 4, including the earlier unversioned development layout and a near-limit 1,000-Task Graph | 931.316 ms release sample; backup-first, SHA-256-bound, restartable at every mutating phase; ownership and schema-3 attempt bindings remain exact, capability requirements are not inferred, and old-schema evidence smuggling fails before backup |
| JSON authority | 64-level / 65,536-node structural guards plus bounded streaming serialization | passing across embedded, durable, process, MCP, model, evaluation, and trace paths |
| Authority-aware Secrets | Secret Provider API 3; typed Agent-Turn, Governed-Effect, and bounded service-use contexts; trusted authority remains separate; exact tenant/reference environment mapping; fixed one-process/one-tenant reference-service assembly; non-serializable zeroizing values; shared-MCP tenant denial | direct gateway, Runtime propagation, typed context serialization, unscoped compatibility, cross-tenant denial, fixed-tenant `doctor`, Protocol/State/Task/archive, per-dispatch Effect resolution/cancellation/redaction/non-leakage, and configured Evaluation tests passing; OS/child copies, multi-principal tenant routing, vault/KMS/OAuth integration, rotation/revocation, and tenant-partitioned MCP sessions remain open |
| Durable Turn execution binding | schema-13 single content-free Item with trusted actor, exact configuration/environment SHA-256, revision, and tenant; Model-invisible, snapshot/reopen durable, archive-safe, exact on approval continuation | constructor/unknown-field/schema gate, pre-State tenant denial, duplicate denial, SQLite snapshot/reopen, Model invisibility, approval missing/substitution, Domain Pack conversion, and archive rebind-denial tests passing |
| Runtime-bound Connector evidence | schema-14 optional bounded source claim in the atomic ToolResult; Runtime-bound registered Tool/origin, trusted actor/tenant, and exact output SHA-256; Model-hidden and archive-safe | compatibility-default ordinary Tool, atomic persistence, digest/origin/authority tamper, schema gate, same/cross-tenant archive, and model-projection tests passing; remote Connector authoring is intentionally absent |
| Generation-bound Turn completion | schema-15 deterministic format-1 `CompletionReceipt`; exact candidate/Model-request/evidence prefix, frozen Model route/Tool View/Verifier manifest/Runtime governance, candidate-bound outcomes, trusted authority, optional execution binding, current-candidate Steering fence, and one atomic `TurnCompleted` transition | construction/tamper/order, Memory/SQLite projection, snapshot/reopen, race, idempotency, fork/archive/import, migration, and Protocol digest paths provide the implementation evidence; schema-1 through schema-14 receipt-free success remains legacy/unverified; format 1 explicitly does not prove cross-aggregate Artifact, Effect, business delivery, channel delivery, or post-terminal jobs |
| Durable Agent Loop Approval wait | schema-16 `Waiting`/`Ready`/`Executing`; independent wait-projection schema 1; one non-batch pre-Tool `ask`; frozen authority/generation/active timeout; atomic accept/claim/cancel/timeout/deny; bounded tenant-keyset expiry and denial convergence; State-level recovery fence; Protocol-37 discovery/resume/cancel and TUI restart recovery | Memory/SQLite projection, exact 1–2-event settlement, deterministic retry, reopen, denial-vs-timeout, and claim-vs-timeout CAS tests pass; Inbox repair outbox/tombstone, batch release, `HumanInput`, finite execution leases, `NeedsReconciliation`, self-contained Context capsule, and cross-process resume receipt remain release blockers for broader claims |
| Durable Task-attempt execution binding | schema-3 append-only exact Task/lease/attempt/worker/time/deployment evidence, persisted before Workspace/executor entry, tenant-exact, terminal/retry durable, and unbound-retry resistant | Graph retry/settlement/serde, Memory/SQLite tenant/reopen, Orchestrator executor-entry, protocol no-authorship, schema-1/schema-2 migration/restart, and old-schema evidence-smuggling tests passing; remote binding control and detailed protocol evidence inspection remain open |
| Typed Task execution capability matching | schema-4 immutable canonical requirements, at most 64 validated names, exact all-requirements matching against trusted embedded Worker capabilities, legacy/protocol empty-set fail-closed behavior, and maintenance-only lease recovery persistence | canonical/duplicate/bound tests, compatible/mismatched Orchestrator execution, SQLite reopen, schema-3 preservation/restart/smuggling migration, and protocol no-self-assertion plus maintenance-only CAS tests passing; authenticated remote Worker Registry, fleet discovery, quotas, fairness, and multi-node scheduling remain open |
| Durable Workflow Run | independent schema-1 aggregate above one same-tenant Task Graph; revision CAS, content-bound command identity, signal/timer wait fences, explicit retry waits, safe-boundary definition migration, immutable transitions, bounded Memory/SQLite persistence, conditional Protocol v27 surface, and Task-completion proof | domain projection/tamper/duplicate/collision/timeout tests, Memory/SQLite parity, restart/cross-tenant/conflict/partial-store tests, command-specific authorization, protocol conflict/paging, real service restart, host-driven due scans, and opt-in service polling pass; automatic effect-safe Task retry and durable compensation planning remain open |
| Durable Human Handoff | independent schema-1 aggregate over one same-tenant Thread or Workflow Run; actor/content-bound commands, revision CAS, stable priority/time/identity queue, finite authenticated-owner leases, never-reused claim fences, immutable transitions, bounded Memory/SQLite persistence, conditional Protocol v37 surface introduced in v28 | owner/expiry/reclaim/idempotency/projection/digest tests, Memory/SQLite queue parity, two-connection CAS, restart/cross-tenant/partial-store tests, command-specific authorization, bounded protocol paging, real service restart, and host-driven expiry advancement pass; the opt-in service composes the same tested Driver, while channel routing, Turn/Workflow side effects, retention/encryption policy, proof-of-human identity, and multi-node HA remain open |
| Durable Effect Ledger | independent schema-1 aggregate; immutable bounded request and digest, tenant/capability/operation/idempotency uniqueness, positive-attempt worker leases, applied/rejected/unknown states, fail-closed expiration, exact reconciliation, content-free receipts, actor-and-tenant-bound command digests, revision CAS, immutable transitions, Memory/SQLite parity, conditional Protocol v37 surface | aggregate projection/digest/owner/expiry/idempotency tests, tenant fencing, revision/tenant projection drift, SQLite reopen/partial-store validation, two-connection claim CAS, protocol capability/conflict/content-light list tests, service restart recovery, Temporal exact-boundary expiry, and configured reference-host terminal non-replay pass; compensation, receipt truth verification, encryption/retention, distributed scheduling, and channel delivery remain open |
| Governed Effect Executor | embedded API 1; exact-versioned frozen Connector capability/operation/idempotency descriptors, trust origin, default-deny pre-Claim Policy, complete pending-page validation, actor/tenant/cycle-bound deterministic Claim commands, duplicate-Claim execution suppression, bounded concurrency/deadlines, panic/error/timeout/cancellation isolation, post-dispatch `unknown`, exact settlement CAS, typed Secret-use context, optional durable dispatch-governor API/schema 1 with tenant-exact immutable lanes, fixed windows, epoch-fenced circuit state and one half-open probe, plus content-free reports | default denial, API/descriptor panic, success/retry, invalid evidence, Connector panic/timeout/cancellation, unexpected worker panic, same-cycle duplicate race, policy failure/timeout, pre-cancellation, Secret resolution/redaction/non-entry, stable order, concurrency bound, Memory/SQLite governor parity/reopen/tenant isolation/rate/circuit/stale-epoch/fail-closed/settlement-degradation, configured service execution, and a public external-view example pass; Connector containment, receipt truth verification, lease renewal, typed finer target coordinates, distributed multi-region governance, and multi-node scheduling remain open |
| Governed Effect Reconciler | embedded API 1; exact-versioned frozen authoritative read-only Connector capability/operation descriptors, trust origin, default-deny pre-query Policy, complete unknown-page revalidation, bounded duplicate-safe read-only queries, panic/error/timeout/cancellation isolation, evidence validation, actor/tenant/cycle/attempt/lease/evidence-bound settlement identity, exact reconciliation CAS, typed Secret-use context, and content-free reports | default denial, API/descriptor panic, applied/retry/rejected/still-unknown, invalid evidence, Connector panic/timeout/cancellation, policy panic/timeout, unexpected worker panic, same-cycle duplicate query and settlement replay, Secret phase/redaction, stable order, concurrency bound, configured service cadence/backoff/degrade/recover, and a public external-view example pass; Connector containment, receipt truth verification, durable query ownership, and multi-node scheduling remain open |
| JSON-command Effect adapters | process protocol 1; distinct strict execution/read-only-reconciliation envelopes, immutable Authority/input/digest/attempt/lease evidence, in-process cancellation, `ExecutionPhase::Effect`, frozen broker isolation and executable-integrity evidence, shell-free absolute command, cleared exact plain plus zeroizing Secret environment, mandatory Secret-gating integrity preflight plus Broker remeasurement under one deadline, bounded I/O/time/concurrency, strict response version, and content-free process/Provider errors | envelope/phase/isolation, registry composition, success, protocol mismatch, malformed/truncated output, broker descriptor panic, cancellation before Secret or process entry, typed execution/reconciliation Secret contexts, drift-before-Provider denial, oversized input, redaction, strict reference-service configuration/probe, required initial/per-dispatch SHA-256 measurement, drift rejection/restoration, and real service subprocess Secret execution/reconciliation/non-leakage pass; raced post-preflight Provider issuance, production Connector fixtures, OS/child-copy erasure, atomic executable-to-exec binding, transitive artifact integrity, and live target truth remain open |
| Host-driven Temporal Driver | embedded API 3; optional Workflow/Handoff/Effect/State composition, trusted host authority/time, 1–256-record tenant-local authoritative scans, disposable keyset cursors, complete extension-page revalidation, precomputed fence command identities, existing CAS transitions, and per-attempt applied/duplicate/fenced/failed settlement | exact-boundary Workflow/Handoff/Effect and Agent Loop timeout/denial advancement, sparse paging, Memory/SQLite reopen, deterministic duplicate, malformed-source fail-closed, and concurrent-fence tests pass; Core has no background task or scheduler database and claims no Task or Effect execution, leader election, or real-time latency |
| Optional reference Temporal host | strict config-schema-1 opt-in; fixed service Authority and Unix time; 100–86,400,000 ms skip-missed cadence; 1–256 identities per source; process-local cursor; bounded degraded/recovered stderr; Temporal-before-Protocol-before-MCP shutdown | configured host composes Workflow/Handoff/Effect and the same State wait source; config/default/bound/diagnostic tests plus real `yh serve` Workflow advancement and stdout/stderr purity pass; disabled by omission, no Protocol v37 Temporal lifecycle command, Task/Effect execution, channel routing, durable cursor, distributed lease, or multi-node scheduler claim |
| Optional reference Effect consumer | strict config-schema-1 opt-in; independent execution/reconciliation tasks; separate exact Connector registries and non-empty allowlists; mandatory dispatch-time command SHA-256 locks; optional reference-only Secret environments with fixed-authority startup probes and per-dispatch resolution; optional independent durable dispatch governor; explicit trust origin/idempotency/read-only contracts; bounded cadence/backoff/concurrency/timeouts; disposable cursors; content-free health transitions; Effect-before-Temporal-before-Protocol/MCP shutdown | strict/default/missing-lock/duplicate/unsupported/timeout/missing-Secret/governor-policy tests plus real `yh serve` commands using persistent dispatch governance, receiving a Secret only in the child environment, reaching Unknown, emitting invalid-reconciliation degradation, rejecting a drifted command before child entry, recovering after exact-byte restoration and target repair, converging to Applied, leaking no Secret into config/JSON/diagnostics, stopping cleanly, and proving no terminal replay after restart; disabled by omission, no Core task or Protocol v37 Effect-consumer lifecycle command, arbitrary Connector honesty, OS/child-copy erasure, atomic executable binding, target truth certification, distributed governor, or multi-node leader claim |
| Optional Domain Pack control plane | format/store schema 1, immutable exact component pins, mandatory pinned Evaluation suite, exact actor/tenant/action authorization port, bounded no-fallback reference RBAC, terminal evaluation, independent approval, tenant-partitioned release/activation records, SQLite CAS, bounded rollback, and execution-time inventory binding | canonical/tamper/inventory tests plus complete authorized lifecycle, cross-tenant/no-fallback, panic-fail-closed/no-mutation, administrator separation-of-duty, Memory/SQLite reopen, projection-drift, conversion to generic Engine execution evidence, and two-connection CAS tests passing; remote lifecycle integration, external IAM adapter, canary, and multi-node HA remain open |
| Secret hygiene | bounded source-tree pattern scan | no matches |
| Source hygiene | no source artifact above 1 MiB; crate forbids `unsafe` | passing |
| macOS process isolation | real Seatbelt allow/deny test | passing |
| Persistent stdio MCP launch | default-deny authority, bounded unrestricted mode, macOS Seatbelt option, absolute working directory, Unix process-group settlement | sandboxed Agent Memory Hub round trip passing locally |
| Authenticated HTTPS MCP | optional exact HTTPS JSON-response transport, per-request environment Secret, no redirect/proxy/retry/replay, bounded request/response/session/time, exclusive CA | direct Tool call and reference-service `doctor` assembly passing against generated private TLS |
| Authenticated Skill Registry | bounded named service configuration, exact digest-pinned Catalog and package identities, request-scoped Bearer resolution under trusted Authority, pre-credential package-origin allowlist, exclusive project-pinned CA, immutable cache/source receipt, inactive install, credential-free doctor/Protocol/TUI projection | generated private-CA TLS Registry receives authenticated Catalog and Package requests; CLI install verifies the signed package and scans the complete resulting project tree for credential leakage; origin-order and missing-credential fail-closed tests pass; mirror federation, OAuth, and live external service evidence remain open |
| Encrypted network host | generated CA/server/client mTLS round trip and unauthenticated rejection | passing |
| Authoritative service admission status | Protocol v37 `service.status`; content-free `ready`/`at_capacity`/`draining`; exact running, retained, and finite-limit counts derived under the same Handler lifecycle/Operation lock used by Turn admission | Handler transition and wire tests plus a real configured `yh serve` process pass; successful status establishes Protocol-process liveness and admission only, not external dependency health |
| Optional HTTP deployment probes | feature-isolated adapter over `ServiceStatusSource`; exact one-request `GET /livez` and `GET /readyz`; finite connection/header/read-write/status/shutdown bounds; fixed non-cacheable bodies; strict opt-in reference-service config; ADR 0151 | three-state mapping, malformed/method/path/body/source-failure/header/config bounds, shutdown reports, and a real configured child-process bind/live/ready/clean-stop pass; no TLS/auth, dependency-health, metrics, general Agent HTTP API, manifest, or multi-node claim |
| Signal-driven reference-service shutdown | Host-owned Unix SIGTERM/SIGINT and Windows Ctrl-C registration; cancellation-aware frame boundary; one detached reader with 32 KiB bounded async stdin buffering; existing one-way Handler and resource drain; ADR 0152 | duplex cancellation test and real Unix child SIGTERM with stdin deliberately left open pass; normal EOF path passes; SIGKILL, global cross-adapter deadline, and multi-process handover are not claimed |
| Private model gateway trust | generated private CA plus authenticated and mTLS HTTPS round trips | passing |
| Direct OpenAI Responses adapter | fixed official HTTPS endpoint, environment Secret, `store: false`, explicit encrypted-reasoning inclusion, same-response multi-call decoding, Harness-owned effect-safe bounded scheduling, bounded JSON/SSE, Provider-reported Model/usage/request provenance, origin-bound continuation | local mapping, ordered/safe-parallel/fenced batch, approval-restart, streaming, persistence, replay, completed-chain release, tamper, and cross-model-failover suppression tests passing; live API environment-gated |
| Brokered JSON-command Model | additive single/catalog configuration, compatible output-v1 plus explicit strict settlement-v1, typed bounded stdin/stdout, Provider evidence/failure facts, explicit Process Broker authority, cleared mapped environment, cancellation and settlement, External provenance | real compatible service Turn, durable State-origin assertion, and typed transient retry passing on Unix; core broker phase/cancellation/metadata/failure/strictness tests passing cross-platform; provisional streaming not claimed |
| Brokered JSON-command Conversation Compactor | additive strict `conversation` configuration, bounded semantic request/response, Context-phase cancellation, explicit Process Broker authority, independent input/output budgets, immutable source history, content-free State provenance | real three-Turn service process invokes the command and records exact covered Turn/digests; invalid static process configuration precedes environment access; core phase/cancellation/deep-JSON/size gates passing |
| Brokered JSON-command Verifier | strict multi-Verifier configuration, immutable bounded candidate snapshot, strict pass/fail wire outcome, Verification-phase cancellation, explicit Process Broker authority, Runtime-owned outcome validation/retry/final settlement | real service Turn passes the configured gate and records durable `VerificationResult`; invalid static process configuration precedes mapped environment access; core phase/cancellation/deep-JSON/unknown-field gates passing |
| Brokered JSON-command Evaluation Grader | additive `evaluation` configuration, isolated in-memory target State, strict 4 MiB sample/response contract, independent case/Grader concurrency and cancellation, External origin, exact format-2 baseline | real configured CLI Evaluation and origin assertion passing; `serve` acquires no Grader authority; invalid static timeout precedes environment access; full workspace passing |
| Configured capability assembly | shell-free JSON Models/Tools/Conversation Compactors/Verifiers/Effect Connectors, optional shared one-shot command SHA-256 locks with mandatory Effect use, exact-selected stdio/HTTPS MCP Tools, explicit process/network authority, Agent Memory Hub health/Context wiring, operator-trusted and signed-External Skill activation with publisher/log locks | real command-Model, command-compactor, command-Verifier, and drift-locked Effect Turns, local/remote MCP adapter tests, signed Skill lifecycle/revocation/transparency process test, and real sandboxed Agent Memory Hub service health pass |
| Model provider failover | strict service-configured Model catalog, per-Model environment Secret mapping, explicit 1–16 identity route, bounded per-attempt timeout with cancel-before-drop cleanup, opt-in timeout-only cooldown with last-resort fail-open and skipped Trace evidence, settled-provider State provenance, stream and total-deadline safety | Runtime expiry/skip/fail-open/non-misclassification tests plus multi-gateway `yh doctor` route test passing |
| Model Provider failures and retry | additive bounded public taxonomy, legacy `Model(String)` compatibility, high-confidence HTTP/transport/protocol mapping, numeric retry hints, content-free Trace evidence, default-disabled 1–8 same-Model retries for four typed transient classes, shared attempt budget, cancellable bounded jitter, retry indices, and provisional-output fencing | constructor/status/trace/privacy plus retry exhaustion/classification/deadline/cancellation/stream/config tests passing; vendor-specific structured quota/content/model-unavailable mapping and distributed recovery remain open |
| Agent Loop progress governance | ADR 0156 bounded replayable reducer over ordered single/batch Tool decisions and results; call/batch/time/Provider identities excluded; exact period-1–4 failure-bearing cycle detection; default five-repetition stop; fully successful cycles excluded; pending durable Steering applied under the Turn-control lock before verdict | period-1/2, changed fingerprints, success/Steering resets, repeated-success, mixed-batch/fresh-ID, cursor-consumption, final-step precedence, Runtime call-count/order, and Tool-in-flight Steering-race tests pass; trusted stable-failure/awaiting-external disposition, advisory, Tool quarantine, semantic equivalence, and cross-Turn circuit breaker remain open |
| Task orchestration execution | bounded public TaskExecutor scheduler, dependency progress, timeout/panic isolation, exact-lease settlement, stale-result cancellation, fenced paged Mailbox, trusted tenant authority, and pre-effect execution binding | passing with memory and SQLite coordinator contracts |
| Task workspace lifecycle | default deny, exact-attempt Provider lease, bounded prepare/release, cleanup-before-settlement, concurrent local isolation, marker/path replacement guards, detached pinned Git Worktree through Process Broker | passing with local directories and real local Git |
| Task worker protocol | protocol-v22 conditional discovery, durable tenant-partitioned Graph identity, bounded graph/record/claim surfaces, principal-derived worker ownership, server-clock leases, cross-principal fencing, messaging, CAS recovery, explicit-revision cancellation | Memory/SQLite exact-tenant access, same-ID tenant namespace, projection-drift, migration/restart, authenticated lifecycle, and conflict tests passing |
| Harness regression evaluation | format-2, origin-bound 2-case × 2-grader end-to-end Runtime suite and exact baseline, plus configured external Grader path | `yh eval-smoke` and real isolated `yh eval` process test passing; machine-readable and nonzero on regression |
| Product and Engine evidence tools | external-run formats 1–6, Codex CF-003 formats 7/8, and Y-Harness CF-003 format 9; exact adapter/product/source/fixture/rollout/config hashes; bounded Claude Code JSON, Codex JSONL, Grok Build headless JSON, Pi JSONL, OpenCode JSONL, Hermes one-shot/usage, and typed Y-Harness stdio processes; deterministic loopback Messages/Responses Providers or spec-bound JSON-command Model; unavailable facts preserved as unavailable; exact-pinned MCP crash/hold-after-effect fixture with durable oracle | adapter/fixture tests plus real Claude Code 2.1.143, Codex 0.145.0, Grok Build 0.2.112, Pi 0.82.1, OpenCode 1.18.5, and Hermes 0.19.0 fixed-output; shared-Provider Claude/Codex and shared-Responses Codex/Grok preflights that machine-reject comparison after exposing fallback/off-coordinate Model calls, protocol, Tool, reasoning, Context, permission, and sandbox differences; Codex single-process/same-Thread restart and Y-Harness explicit-recovery restart CF-003 records passing; all explicitly ineligible for comparison claims, with Claude's Provider probe/config state/product prompt blocks, Grok's rejected default-Model title call, Codex's visible built-ins, unavailable settled identity, identity-bound detached-MCP release marker, new-Turn recovery, no implicit takeover, and no descendant-exit claim recorded |
| Public API embedding | standalone zero-network hosts run a Policy-controlled Model/Tool loop and a durable fenced Task DAG through public contracts | `embedded` and `orchestrated` examples passing |

The latest local performance figures were measured on 2026-07-28 and are environment
specific. CI uses deliberately wider smoke thresholds to detect catastrophic
regression without pretending hosted runners are stable benchmark machines.

## CI evidence that requires a remote run

The checked-in CI workflow runs the zero-default core, each isolated optional
feature, all-feature minimum compiler gates and the versioned Harness
regression evaluation on Ubuntu, all-feature tests on macOS and Windows,
audits Rust dependencies, and runs a release-mode SQLite performance smoke
workload. It also runs the TUI render/unit suite plus demo and configured real
PTY smoke gates. The tag workflow independently repeats the release gates,
tests and stages separate native `yh` and `yh-tui` archives on Ubuntu, macOS,
and Windows, generates the complete workspace CycloneDX SBOM, verifies one
complete checksum manifest, attests every file, and only then creates an
immutable GitHub Release. Third-party Actions use exact commit pins and Rust
supply-chain tools use exact versions. A checked-in workflow is configuration,
not proof that GitHub has executed it. Both workflows must be green on the
exact release commit.

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
  certificate fingerprint as the authenticated subject. Schema-3 records,
  list/get/settle/orphan operations, and restart continuation are exactly
  tenant-fenced. Schema-1/schema-2 migration is backup-first, explicitly
  orphans unattributed schema-1 pending work, and keeps schema-2 records
  unscoped. An embedded host with exclusive ownership can resume a schema-3,
  fingerprint-matched
  pre-Tool approval wait across Memory or reopened SQLite stores. Post-decision
  unknown Tool effects remain fail-closed. Mapping certificate Subject/SAN to
  people, tenant/role policy, proving that two certificates belong to
  independent humans, signed decision receipts, retention, notifications, and
  lease/fenced remote continuation are not implemented.
- A trusted per-Turn Authority Context can map a transport principal to an
  actor and tenant, binds remote Memory scope, and reaches State, Policy, Tool,
  Task, Workflow, Human Handoff, direct Model Secret resolution, and MCP
  admission. Schema-12 Threads,
  State reads/mutations, recovery, retained Operations, schema-3 Approval
  records, schema-4 Task Graphs, schema-1 Workflow Runs, schema-1 Human
  Handoffs, and the optional Domain Pack control-plane store are tenant-fenced.
  Secret API 3 supports exact embedded tenant/reference mappings and typed
  Agent-Turn, Governed-Effect, and bounded service-use contexts; legacy
  Providers and current shared MCP sessions fail closed. Task Artifact
  reference metadata is fenced with its Graph, but the external URI target has
  no Y-Harness storage or authorization boundary. The reference service can
  bind one process to one exact tenant and its direct-Model plus per-dispatch
  Effect environment credentials. Multi-principal tenant routing, general
  Secret-manager integration, tenant-partitioned MCP sessions, external
  Artifact storage, quotas, and retention remain open, so this is not a
  complete multi-tenant isolation claim.
- State schema 15 proves only one exact Turn-internal completion generation.
  Its format-1 contract requires Artifact, Effect, and business-delivery
  obligations to be explicitly `not_required`; it does not turn Task Artifact
  references, Effect Ledger receipts, client/channel acknowledgement, Memory
  extraction, titles, suggestions, or Evaluation into completion evidence.
  Such obligations must keep the Turn non-terminal or fail closed until a
  later authority-fenced cross-aggregate contract exists. Receipt-free legacy
  success remains inspectable but unverified, and a fork/import retains source
  proof rather than claiming the target reran the gates. A durable delivery
  outbox and generalized Waiting/resume envelope remain open work.
- Workflow schema 1 provides durable, fenced Run state and evidence above a
  same-tenant Task Graph. The server clock and authenticated authority remain
  host-owned; the Workflow never leases work or repeats a Tool effect. The
  embedded Temporal Driver can advance bounded due waits when a host invokes
  it, and the reference service can opt into that lifecycle. Effect schema 1
  records and reconciles unknown external outcomes independently. Embedded
  Governed Effect Executor API 1 can perform a bounded, Policy-gated sweep,
  but the reference service does not configure a consumer and no component
  verifies arbitrary external receipts, retries Tasks, executes compensation
  plans, or routes Human Handoff. SQLite provides single-host/multiprocess
  CAS, not distributed scheduling or availability.
- Human Handoff schema 1 provides durable, lease-fenced ownership state over
  an existing same-tenant Thread or Workflow Run. It does not pause either
  subject, route a channel, grant business authority, start a resident expiry
  scanner, encrypt operator summaries, or authenticate `LocalProcess` as a
  person. An embedding host may use Temporal Driver API 3 for bounded expiry.
- Domain Pack format/store schema 1 governs immutable component snapshots,
  terminal pinned-suite evaluation, independent approval, activation,
  deactivation, rollback, and current-inventory execution binding in a separate
  optional crate above Core. `AuthorityContext` provides attribution and tenant
  fencing, not role permission; an embedding control service must authorize
  actions and lock the bound inventory. Protocol/CLI service integration,
  registries, signed remote Pack acquisition, canary rollout, retention, and
  distributed control-plane fencing are not implemented.
- Publisher and transparency-log keys now support live validity and immutable
  revocation, and signed receipts bind log/entry/time metadata to the exact
  package and publisher signature. Exact pin-bound public HTTPS acquisition is
  available behind `https-skill`. The reference service explicitly separates
  operator-trusted project files from signed External files, configures
  publisher/log trust, preserves External provenance, and rechecks trust during
  governed use. The CLI validates and canonically installs local, offline
  signed, exact public-HTTPS, or configured private-Registry declarative
  packages without silently activating them. Digest-pinned Catalog discovery,
  exact signed dependency acquisition, immutable caching/source receipts,
  explicit upgrade, restart-boundary reload, request-scoped Registry Bearer
  resolution, package-origin fencing, and exclusive private CA trust have local
  evidence. Directory auto-activation, Registry mirror federation, OAuth,
  npm/git/temporary sources, a live public source pass, threshold signatures,
  durable trust-policy distribution, and append-only transparency-log
  inclusion/consistency/gossip are not implemented.
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
6. explicit documentation of every unsupported platform or capability;
7. one complete checksum-verified and provenance-attested artifact set, with
   no prior or partially published release under the same coordinate.

The operator procedure and immutable-failure policy live in
[`release-process.md`](release-process.md); the design boundary is recorded in
[ADR 0149](adr/0149-atomic-attested-release-publication.md).

Absolute permanent “zero bugs” is not a provable property. Release claims must
name the evidence and date used.
