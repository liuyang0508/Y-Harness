# Reference architecture analysis

This document records how Y-Harness Engineering derives architectural lessons
from Pi Agent Harness, Claude Code, Codex, Hermes Agent, OpenCode, and Grok
Build. It is a design-input audit, not a feature-parity checklist and not an
endorsement of every implementation choice in those projects.

The baseline source snapshot was reviewed on 2026-07-25, the Turn-steering and
Grok Build deltas on 2026-07-26, and the current Pi capability delta on
2026-07-27. The released Hermes CLI adapter delta was reviewed on 2026-07-28.
The released Codex CF-003 and Grok Build adapter deltas were also reviewed on
2026-07-28. Every source observation below links to an immutable commit. The
repositories were inspected locally from shallow checkouts; their full test
suites were not executed as part of this audit.

Evidence has three levels:

- **official source**: code in the upstream public repository named by the
  project;
- **corroborating source**: code reconstructed from a distributed artifact,
  useful for understanding behavior but not authoritative for provenance,
  completeness, licensing, or current production behavior;
- **product documentation**: an official behavior claim without a public
  implementation claim.

The supplied `claude-code-source-code` repository is corroborating source, not
an Anthropic source repository. Its own README says it was extracted from the
Claude Code 2.1.88 npm package, is incomplete, is not directly compilable, has
no license file, and prohibits commercial use. Y-Harness may use it to form
behavioral hypotheses and choose clean-room tests. It must not copy code from
it or treat it as proof of Anthropic's private implementation.

## Audited snapshots

| Reference | Snapshot | Evidence status |
|---|---|---|
| Claude Code 2.1.88 source reconstruction | [`3da94d5`](https://github.com/liuyang0508/claude-code-source-code/tree/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f) | corroborating, incomplete, non-commercial notice, no executable upstream test evidence |
| Codex | [`4c43465`](https://github.com/openai/codex/tree/4c43465133428898aa84f0bfc02c306ed65fb66a) | official public source, Apache-2.0 |
| Pi Agent Harness | [`cee5ff7`](https://github.com/earendil-works/pi/tree/cee5ff7520d8828bed9955ef00419e995d1f91e0) | official public source, MIT |
| Hermes Agent | [`689b51b`](https://github.com/NousResearch/hermes-agent/tree/689b51bef68f9ec95b638121bb9c7fefa3703fb2) | official public source, MIT |
| OpenCode | [`7534d23`](https://github.com/anomalyco/opencode/tree/7534d23551f665e65080809975b4ca5c7d63807b) | official public source, MIT |
| Grok Build | [`47348d1`](https://github.com/xai-org/grok-build/tree/47348d13ec4508dcfe440e34c6d511bb02998fb2) | official public source periodically synced from the SpaceXAI monorepo, Apache-2.0 |

The 2026-07-26 delta additionally pins current Codex at
[`61a4488`](https://github.com/openai/codex/tree/61a44880a85d2fd0d8770908dea5733495e571c8)
and Hermes at
[`6ab5d2d`](https://github.com/NousResearch/hermes-agent/tree/6ab5d2df2a5748f23ba7557ec527fac628720a22).
These newer coordinates are used only for the delta findings below; they do not
silently relabel observations made against the earlier baseline.

The released Hermes adapter additionally pins official tag `v2026.7.20`,
package `0.19.0`, at
[`3ef6bbd`](https://github.com/NousResearch/hermes-agent/tree/3ef6bbd201263d354fd83ec55b3c306ded2eb72a).
That coordinate supports only the released-CLI findings and does not relabel
the earlier architectural observations.

The released Grok Build adapter additionally audits public snapshot
[`02d9359`](https://github.com/xai-org/grok-build/tree/02d9359435d0e9c20a20945679389cdce441e431),
whose `SOURCE_REV` is `1adcd1f477870e4a97bacbd6be78c8a3bfbac46d`.
The official `0.2.112` binary independently identifies its build as abbreviated
revision `9bbd559437aa`; no full public-source match is claimed.

The dedicated Codex fault driver additionally pins official tag
`rust-v0.145.0`, released CLI `0.145.0`, at
[`25af12f`](https://github.com/openai/codex/tree/25af12f7e61572b0bc18ddb1008be543b91519b0).
That coordinate supports only the deferred MCP exposure, JSONL lifecycle,
rollout flush/resume, missing-output normalization, and CF-003 findings in the
checked non-comparative records; it does not relabel the earlier architectural
observations or prove binary-to-source equivalence.

The Pi snapshot supersedes the earlier `5bc1c2c` audit coordinate after a
source-level revalidation. The old coordinate remains in historical ADR links
where it is evidence for the decision made at that time.

## 2026-07-27 Pi capability-alignment delta

Pi changed materially after the earlier audit. Current source contains a
larger `AgentHarness`, same-message multi-Tool execution, a broad generated
Provider catalog, custom model configuration, package management, tree-shaped
sessions, and mature interactive/RPC surfaces. Capability names alone do not
establish alignment, so this table binds each claim to a Y-Harness evidence
gate.

| Capability | Current Pi mechanism | Y-Harness evidence | Alignment and next gate |
|---|---|---|---|
| Agent Loop | One assistant message may contain several Tool calls; execution is parallel unless the loop or any selected Tool requires sequential execution. Preflight is ordered, execution may overlap, and final Tool-result messages retain assistant source order. | `ModelOutput::ToolCalls` accepts 2–64 calls, records one atomic ordered State decision, authorizes the complete batch before effects, and supports exact approval recovery. Frozen `ParallelSafe` declarations permit maximal contiguous safe runs under a 1–64 Runtime ceiling; sequential Tools fence runs, results settle in source order, and cancellation/deadline tests preserve completed sibling evidence. | **Aligned on the general Harness behavior with stricter effect authority.** Provider intent never grants concurrency and undeclared/MCP Tools remain sequential. Comparative latency and production fault diversity remain evidence gaps, not missing scheduler semantics. |
| Model and Provider | `packages/ai` registers a broad built-in catalog. `models.json` can add OpenAI Chat/Responses, Anthropic Messages, Google Generative, and compatible local or remote endpoints without changing Pi source. | The service accepts a strict configured Model catalog plus an explicit 1–16 identity route, per-Model environment Secret mapping, per-attempt timeouts, and opt-in observable timeout cooldown over direct OpenAI Responses, the provider-neutral HTTPS gateway, or an arbitrary-language brokered JSON-command Model. Command Models retain External provenance and the existing cancellation/process boundary; their explicit settlement-v1 wire preserves Provider usage/model/request/continuation evidence or typed failure facts without changing Rust. Cooling Models remain last-resort candidates; ordinary string errors never become inferred health. | **Partially aligned with stronger routing evidence.** Operators can add routed gateway- or command-backed vendors without Rust changes and avoid repeated proven timeout waits. Broad native vendor protocols, command-level provisional streaming, hot reload, load/price policy, and management UX remain open. |
| Tool, MCP, and isolation | Pi ships useful file/shell/image Tools and rich hooks, but intentionally has no built-in MCP, permission popups, or sandbox. Extensions run with the launching user's authority. | `src/execution`, `src/transport`, `src/runtime/policy.rs`, and `src/approval` provide governed Tool registration, stdio plus authenticated HTTPS JSON-response MCP, durable approval, and bounded process/network authority. | **Different strengths.** Preserve Y-Harness authority boundaries while adding multi-Tool ergonomics; bounded SSE/OAuth and broader Tool catalogs remain open, and ambient in-process extension authority stays rejected. |
| Packages, Skills, and extensions | `pi install/remove/list/update/config` manages npm, git, URL, local, user, project, and temporary sources. Packages may provide executable TypeScript extensions, Skills, prompts, and themes; `/reload` hot-loads resources. | `src/skill` verifies exact identity, dependencies, budgets, digests, publisher signatures, revocation, and transparency evidence. `yh skill install`, `install-external`, and `install-https` manage bounded declarative stores while preserving local versus signed-External trust and keeping activation separate. | **Partially aligned with stronger supply-chain authority.** Local and exact public-HTTPS installation are implemented without ambient execution. Dependency download, update/catalog/private-registry UX, hot reload, executable-extension isolation, and ecosystem breadth remain open. |
| Session and product surface | JSONL session trees support resume, tree navigation, fork, clone, compaction, import/export, and RPC. `getTree()` defensively assembles the entry DAG; `createBranchedSession` extracts the root-to-leaf path, preserves entry identities, re-chains retained entries, records `parentSession`, and writes the new file incrementally. `navigateTree()` may summarize the abandoned old-leaf suffix and append that derived text on the selected branch. | Authoritative SQLite Thread/Turn/Item State, recovery, checkpoints, bounded lineage-aware recent-Thread Protocol paging, explicit durable names, Protocol-backed TUI resume, schema-10 portable archives, schema-11 attributed per-Turn Context, and schema-12 durable Thread/Operation tenant fencing are implemented. Fork preserves historical evidence identity and exact tenant ownership; format-2 archive import is atomic and rebinds the target to the importing tenant. Format-1 Thread handoff preparation computes a bounded source-only Turn delta, binds it to source/target identities and a canonical digest, and converts any host-generated candidate into attributed content-free Context evidence. | **Partially aligned by a different State model.** Durable fork/clone-at-head, bounded recent-page forests, integrity-bound import/export, tenant isolation, and read-only Thread-handoff preparation are Engine-owned. Pi's entry-level mutable leaf and automatic in-session navigation are deliberately rejected as a second branch authority. Candidate synthesis remains provider/host-selected and its factual quality requires evaluation. JSON is a bounded interchange format, not authoritative State. |
| Evaluation | `packages/evals` contains an executable Pi harness adapter. OpenCode exposes source-tested `run --format json` step events. Hermes `0.19.0` exposes one-shot stdout plus a JSON usage sidecar. Grok Build exposes bounded headless JSON and an OpenAI-compatible custom-model endpoint. | External-run formats 3 through 6 drive released Grok Build, Pi, OpenCode, and Hermes CLIs. They pin binary/version, bound process evidence, support isolated bare state, and preserve only source-supported lifecycle/identity/cost facts. Hermes estimated cost remains distinct from actual cost. Released Grok Build `0.2.112`, Pi `0.82.1`, OpenCode `1.18.5`, and Hermes `0.19.0` runs completed against deterministic loopback Providers and are retained with their explicit unsupported controls. | **Partially aligned.** Contract tests pass, and Grok Build, Pi, OpenCode, and Hermes each have one real non-claim fixed-output record. A shared-Provider Claude/Codex preflight rejected fallback Model metadata and other control drift. A shared-Responses Codex/Grok preflight aligned the main `gpt-5.4` calls but proved Grok separately attempted a rejected `grok-4.5` title call, while Tool, Context, reasoning, and permission controls differed. No Harness-effect comparison exists. Superiority remains unverified. |

The adoption rule is therefore selective rather than superficial: take Pi's
small loop, provider normalization, ordered multi-Tool semantics, package UX,
branch-handoff intent, session navigation, and RPC lessons; retain
Y-Harness's stronger State, Policy, isolation, provenance, and
extension-supply-chain contracts. Session navigation composes Engine Threads
and per-Turn Context instead of importing Pi's mutable entry leaf.

## Grok Build: open Harness source; Grok 4.5: Model coordinate

[SpaceXAI's release announcement](https://x.ai/news/grok-build-open-source)
explicitly publishes Grok Build as its coding agent, TUI, and Harness. The
official repository contains the Rust CLI/TUI and agent runtime, including
context assembly, model sampling, Tool dispatch, workspace/checkpoint support,
Skills, Plugins, Hooks, MCP, and Subagents. Snapshot `47348d1` records
SpaceXAI monorepo source revision
`d02693a856a54f1030695b36b91d276e96b30b23` in `SOURCE_REV`.

The names must remain separate:

- **Grok Build** is the open Agent/Harness product and source baseline;
- **Grok 4.5** is the Model ID and the
  [checked-in default model](https://github.com/xai-org/grok-build/blob/47348d13ec4508dcfe440e34c6d511bb02998fb2/crates/codegen/xai-grok-models/default_models.json)
  for that product at the audited snapshot;
- neither fact makes the Grok 4.5 model weights open source or establishes
  that this repository contains the consumer Grok application's entire
  production stack.

The following official xAI snapshots remain useful supporting evidence rather
than substitutes for the Grok Build runtime:

| Source | Snapshot | What it establishes | What it does not establish |
|---|---|---|---|
| Grok-1 | [`7050ed2`](https://github.com/xai-org/grok-1/tree/7050ed204b8206bb8645c7b7bbef7252f79561b0) | Apache-2.0 JAX loading/sampling example and Grok-1 open weights; the README explicitly describes an intentionally inefficient correctness implementation | production Grok Agent Loop, State, Memory, Policy, Tool Runtime, orchestration, or clients |
| Grok prompts | [`a7c186f`](https://github.com/xai-org/grok-prompts/tree/a7c186f5ccac95875c0041aed60398f6ecb6d6c7) | AGPL-3.0 prompt snapshots for named Grok product/model surfaces | the code that compiles context, executes Tools, persists state, or validates completion |
| xAI Python SDK | [`4358bc2`](https://github.com/xai-org/xai-sdk-python/tree/4358bc235e8641ba5f0cb54599675d098385d4bf) | official synchronous/asynchronous gRPC client behavior | server implementation or production Harness internals |
| xAI protobufs | [`af1be87`](https://github.com/xai-org/xai-proto/tree/af1be87b733dc177c0857fbd624f1ff12128fbd2) | versioned public provider contracts for tools, remote MCP, continuation, encrypted content, bounded agentic turns, and multi-agent model requests | which component owns effects, recovery, authorization, or durable truth inside xAI |

Four useful design inputs follow without copying product semantics:

1. Grok Build is a source-level Harness baseline. Its Rust composition root,
   agent runtime, TUI, Tool, workspace, state, extension, and ACP boundaries are
   auditable mechanisms, not merely product claims.
2. Grok 4.5 is a controlled Model or product-default coordinate. It must not be
   conflated with the Grok Build Harness when attributing benchmark outcomes.
3. Grok-1 weights and public prompt snapshots are separate Model-side evidence,
   not substitutes for the current Grok Build Runtime.
4. Provider-managed Tools, remote MCP, opaque continuation, and multi-agent
   requests must be represented as provider-origin effects with explicit
   authority and evidence. They must never be mislabeled as Harness-executed
   Tools merely because the provider protocol uses similar nouns.

Y-Harness may add xAI as a direct Model provider and Grok 4.5 as a controlled
Model cell. Released Grok Build now has a bounded external adapter contract
and one real deterministic `0.2.112` conformance record. That record is not a
same-Model or quality comparison; superiority is still unproven until both
systems run the same versioned workloads under declared controls.

## 2026-07-26 Turn-steering delta

This delta applies “take the strengths, reject the liabilities” at mechanism
level. Similar names do not establish equivalent semantics.

| Source mechanism | Useful invariant extracted | Y-Harness adoption | Rejected or still open |
|---|---|---|---|
| Pi drains a small steering queue between assistant/tool steps and a follow-up queue when the loop would stop. | New input belongs at explicit loop boundaries rather than in arbitrary provider callbacks. | Pending steering is drained at Model and Tool safe boundaries. | Pi's process-local callback queue is not durable authority; Y-Harness does not inherit its session model or coding-product surface. |
| The Claude reconstruction has scoped/priority queues and preserves Tool-call/result adjacency before inserting ordinary input. | Provider history must never be malformed merely to make an interactive queue feel immediate. | A superseded Tool call receives a synthetic error result before steering becomes model-visible. | Reconstructed code is not copied. Priority tiers are deferred until a real engine-level requirement justifies their semantics and bounds. |
| Current Codex requires an exact active Turn ID and separates current-step input from later mail. | A stale client must not redirect whichever Turn happens to be running. | Protocol 15 preserves `thread_id` plus exact `expected_turn_id`; a mismatch writes nothing. | Codex's process-local queue is not treated as recoverable State, and its coding-agent product behavior is not a Core contract. |
| Current Hermes restarts after steering crosses an in-flight response, and its recovery code copies SQLite/WAL/SHM before canonical rebuild and integrity checks. | A response sampled from old context is stale; recovery must preserve evidence before repair. | Crossed Model messages and Tool calls are discarded, their provisional stream step is invalidated, and steering is applied before resampling. | This delta does not copy Hermes's large parent-object loop, mutable learned-skill behavior, or claim that Y-Harness already matches its recovery breadth. |
| OpenCode's CLI runtime serializes queued ordinary prompts and allows client editing/removal. | Product-side queue UX can remain independent from Runtime semantics. | The TUI maps input during an active Turn to the engine's typed steering command. | An editable client queue is not Runtime steering authority and cannot replace durable acceptance evidence. |

The resulting Y-Harness mechanism is not a union of six feature lists.
`SteeringQueued` records accepted actor-attributed input, while
`SteeringApplied` records the exact FIFO safe boundary at which it became Model
context. Completion is forbidden while queued input remains. A per-active-Turn
control lock serializes Runtime acceptance with ordinary recording, while the
append-only State compare-and-swap remains authoritative. See
[ADR 0078](adr/0078-durable-safe-boundary-turn-steering.md).

This is a source-supported architecture result, not a comparative effectiveness
result. Cross-product steering correctness, latency, answer quality, and
recovery are still unmeasured.

## Comparison

| Reference | Observed architecture | Adopted by Y-Harness | Deliberately not inherited |
|---|---|---|---|
| Pi Agent Harness | `agent-loop.ts` keeps the model/tool loop small, normalizes messages at the provider boundary, emits typed events, drains steering/follow-up queues, and executes ordered batches of Tool calls sequentially or concurrently. `AgentHarness` composes sessions, resources, hooks, compaction, tools, and model selection. The coding product adds Provider catalogs, packages, session trees, RPC, root-to-leaf fork extraction, and a real same-process evaluation adapter. | Embeddable runtime, explicit loop events, provider-neutral model contract, resource discovery, ordered multi-Tool semantics, one registration path for built-ins and extensions, and terminal-boundary Thread fork. | Package or extension installation is not authority to execute arbitrary code in the kernel. Session files and incremental copy loops are not the authoritative HA state model. Concurrency requires bounded scheduling plus per-effect Policy, State, cancellation, and recovery evidence. |
| Claude Code | The reconstructed 2.1.88 source shows an async-generator query loop, streaming tool execution with concurrent/exclusive scheduling, several compaction and recovery paths, layered permissions, MCP management, plugins, persistent tasks, and agent teams. Official docs independently describe instructions, skills, MCP, hooks, permissions, subagents, and worktrees. | Layered Context, discoverable Skills, MCP as a Tool adapter, typed lifecycle observations, capability-scoped sub-tasks, and declarative workspace requirements. Recovery regressions become clean-room fault-injection cases. | Reconstructed code is not copied or treated as authoritative. Product-specific instruction filenames, feature flags, shell-hook semantics, coding workflows, and UI behavior are not kernel contracts. Hooks cannot bypass Policy, bounds, provenance, or failure isolation. |
| Codex | `run_turn` owns the sampling/tool/follow-up loop and captures one step view for context and advertised tools. A Rust core supports TUI, exec, and a typed app-server protocol. The pinned app-server exposes `thread/fork`, optional inclusive `lastTurnId`, active-boundary rejection, interrupted latest-state handling, a new Thread ID, and `forkedFromId`. Tool routing records lifecycle outcomes, and the sandbox manager selects macOS Seatbelt, Linux seccomp/bubblewrap, or Windows restricted-token execution independently from approval policy. SQLite state, recovery, skills, plugins, memories, multi-agent paths, and OpenTelemetry are distinct crates or modules. | Rust semantic core, client/runtime separation, versioned asynchronous protocol, Thread/Turn/Item state, explicit approval, process-broker isolation, skills/MCP, worktree-aware orchestration, and typed terminal-boundary fork semantics. | Coding-agent behavior stays outside Core. Y-Harness does not mutate an active source to manufacture an interruption marker; clients select an earlier terminal boundary or settle the active Turn. Process-local operation state is not durable truth. Remote recovery cannot take over a Turn without an external lease and fencing authority. |
| Hermes Agent | One `AIAgent` serves CLI, gateway, ACP, batch, cron, and API entry points. The source has extensive provider normalization, retry/error classification, context compression, SQLite/FTS session storage, checkpoints, approvals, many execution environments, memory/context plugins, trajectories, and observer hooks. The extracted `conversation_loop.py` still drives a large parent-object state surface, showing useful production handling and a coupling cost at the same time. | General-purpose rather than coding-only scope, shared core across entry points, Memory and Context provider ports, profiles/scopes, provider neutrality, and trajectories as Evaluation evidence. Error taxonomies and provider quirks become adapter conformance cases. | Messaging adapters, provider configuration, mutable skill learning, and application commands do not belong in the microkernel. Import-time self-registration and ambient plugin authority are not accepted extension contracts. |
| OpenCode | The current tree separates Effect-based services, normalized `LLMEvent` streams, session processing, permissions, tools, providers, plugins, MCP, SQLite schemas/migrations, event-backed session projection, control-plane/worktree code, SDKs, TUI, web, and desktop clients. The permission service keeps pending approvals process-local, while the newer core state uses SQLite and explicit projection code. | Headless core plus typed service boundary, streaming events, provider/tool registries, permission checks, durable projection, and multiple clients over one semantic runtime. | Coding modes, local-server assumptions, and desktop/web product policy do not define Harness semantics. In-process plugins do not receive ambient engine authority by installation alone. Pending approvals and transient stream state cannot become authoritative recovery state. |
| Grok Build | A Rust composition root separates the full-screen TUI, agent runtime, Tools, workspace/checkpoints, model sampling, chat state, SQLite journal, MCP, Skills, Plugins, Hooks, Subagents, ACP, sandboxing, telemetry, and headless entry points. The public tree is a periodic monorepo sync rather than the complete monorepo. | Rust client/runtime separation, explicit product composition root, local-first provider configuration, ACP embedding, extension breadth, and concrete recovery/UX mechanisms become source-audited design inputs and benchmark cases. | Grok 4.5 defaults, coding workflows, TUI policy, hosted-service assumptions, and product extension semantics do not become Core contracts. A large generated crate closure is not copied as Y-Harness's module topology. |

## Source-level competitive findings

| Layer | Strongest observed lessons | Y-Harness position after this audit |
|---|---|---|
| Context Engine | Claude Code and Codex have multiple compaction/recovery paths shaped by real provider failures. OpenCode now models context epochs; Pi keeps compaction understandable; Hermes handles wide provider variance and prompt caching; Grok Build exposes concrete prompt, compaction, and recap paths. | Deterministic whole-Turn selection, independent byte/token bounds, and provenance are credible local strengths. Semantic faithfulness, provider cache behavior, media overflow recovery, and long-session quality are not competitively proven. |
| Agent Loop | Pi has the clearest small generic loop and explicit sequential/parallel multi-Tool execution. Codex captures a consistent per-step view. Claude Code has mature streaming fallback and orphan-result cleanup. Hermes has broad retry/failover classification. OpenCode converges runtimes on one event stream. Grok Build adds a substantial Rust product loop with interjection and long-running task behavior. | Durable settlement, deadlines, cancellation, exact failover, observable attempt-timeout cooldown, bounded typed Provider failure evidence, bounded same-Model transient retry, atomic ordered same-response decisions, and bounded explicitly safe Tool runs with sequential fences are implemented. Broader recovery, vendor-specific failure breadth, production fault diversity, and comparative latency remain unproven. |
| Tool Runtime | Codex has the strongest public cross-platform sandbox implementation. Claude Code shows mature concurrent/exclusive tool scheduling. Hermes has the broadest execution-environment catalog. OpenCode and Pi have low-friction extension paths. Grok Build combines local and hosted Tools with a broad Rust extension surface. | Policy-governed registration and fail-closed process authority are strong design choices. Linux/Windows containment, tool breadth, provider-hosted tools, and hostile-process tests remain material gaps. |
| State Engine | Codex, OpenCode, and Hermes all have substantial SQLite schemas, migrations, and recovery behavior; OpenCode now has event-backed session projection. Codex has typed Thread fork boundaries; Pi deliberately supports lighter JSONL/in-memory session stores, an entry tree, and root-to-leaf file extraction. | Typed append-only authority, CAS, bounded recovery, snapshots, backup-first migrations, atomic terminal-boundary fork with exact lineage, lineage-bearing bounded Thread summaries, portable integrity-bound terminal Thread archives, and content-free attributed invocation Context are implemented. Destructive archival/offload, blob separation, remote ownership, and long-running production evidence are open. Thread-handoff preparation is deliberately a read-only Context concern; entry-level in-place trees are intentionally outside the State model. |
| Memory Engine | Hermes exposes several provider plugins. Codex and Claude Code integrate product memory. | The Agent Memory Hub boundary is a genuine differentiator, but retrieval quality and end-to-end outcome gain require shared benchmarks, not architecture claims. |
| Skill Engine | Codex and Claude Code have marketplace/plugin product flows; OpenCode pulls remote skills; Pi packages skills/extensions; Hermes ships a large catalog; Grok Build exposes Skills, Plugins, Hooks, and marketplace code in one product runtime. | Signature, revocation, receipts, exact HTTPS pins, bounded resolution, and trusted/signed-External install/list/verify/recoverable-remove lifecycles are strong governance primitives. Network discovery, private registry, cache/mirror, dependency acquisition, update UX, executable-extension isolation, and ecosystem size lag. |
| Policy Engine | Codex separates approval from OS sandboxing and supports managed requirements. Claude Code has rich permission matching/classification. OpenCode has concise rule evaluation. Hermes covers approvals across CLI/gateway surfaces. | Durable attributed approvals and fingerprint-bound continuation are stronger than an in-memory prompt. Human/tenant identity, signed receipts, role policy, remote continuation, and cross-platform containment are open. |
| Orchestration | Codex and Claude Code expose mature multi-agent/product workflows. OpenCode has subagents and worktree/control-plane code. Hermes has delegation across many surfaces. Pi has simple steering/follow-up semantics. Grok Build exposes Subagents, workflows, goals, worktrees, and long-running task paths. | Task DAGs, leases, fencing, mailbox, and workspace lifecycle are architecturally substantial. Multi-node consensus, durable orphan reaping, remote executors, and comparative task success are not proven. |
| Verification | Claude Code has stop hooks and verification-oriented skills; Codex has review/hook paths; Hermes records verification evidence. | Verification is a first-class engine layer rather than only a prompt convention. Its real-world graders and false-completion rate still need competitive measurement. |
| Observability | Codex has OpenTelemetry modules and rich runtime events. Hermes has a versioned observer contract. OpenCode uses Effect/OTel. Claude Code and Pi expose extensive events. | Failure-isolated, content-free evidence now includes typed Provider failure class/status/retry facts without diagnostics. Exporter breadth, distributed traces, operator UX, and overhead comparisons are open. |
| Evaluation | Pi includes an executable harness adapter. Hermes records trajectories. All public projects have substantial tests, but their tests are not a controlled cross-Harness comparison. | Y-Harness has a versioned regression runner, configured origin-bound external Graders, real non-comparative released-Claude Code, Grok Build, Pi, OpenCode, Hermes, and Codex fixed-output cells, plus two real non-comparative Codex CF-003 cells: single-process and same-Thread restart. None is a competitive result; the fixed-output cells do not execute common Tools, Claude adds a Provider probe and product prompt/date blocks, Grok still exposes read/MCP meta-tools and an auxiliary title call, Codex keeps six built-in Tools visible and does not echo settled Provider/Model identity, and the fault cells do not prove in-place interrupted-Turn continuation. A shared-Provider Claude/Codex preflight proved that matching a requested Model string is insufficient because Codex used fallback metadata. A follow-up shared-Responses Codex/Grok preflight aligned the main `gpt-5.4` calls but exposed a rejected auxiliary `grok-4.5` title call plus unequal Tool, Context, reasoning, permission, call-count, and settlement controls. The required cross-Harness protocol is defined in [`competitive-benchmark.md`](competitive-benchmark.md). |

The architectural boundary is competitive; the product effect is not yet
competitive evidence. A typed abstraction, a passing unit test, or a larger
feature list cannot establish that Y-Harness produces better answers than
Codex or Claude Code.

## Cross-project synthesis

### One semantic core, multiple surfaces

Pi's embeddable SDK, Codex's core/app-server split, Hermes's shared agent, and
OpenCode's service/client split all support one conclusion: CLI, TUI, SDK,
Web, and Desktop are consumption surfaces, not separate Harness
implementations.

Y-Harness therefore has one `HarnessRuntime`, an embedded Rust API, and one
versioned protocol. Independent product clients call that protocol. A future
client must do the same instead of duplicating Agent Loop, Policy, or State
behavior. See [`architecture.md`](architecture.md) and
[ADR 0012](adr/0012-versioned-asynchronous-runtime-protocol.md).

### Explicit loop and authoritative evidence

Pi and OpenCode make model and tool progress observable as typed events.
Codex exposes asynchronous Turn lifecycle. Hermes documents a shared
conversation loop across entry points. Y-Harness adopts the explicit
`model → tool → observation → correction` loop, but makes the append-only State
journal authoritative rather than treating UI messages, callbacks, or
process-local operations as truth.

The implementation is in `src/runtime`, `src/kernel/types.rs`, and `src/state`.
The durable boundary is specified by
[ADR 0003](adr/0003-append-only-state-journal.md), while interruption,
approval continuation, and unknown Tool effects are constrained by
[ADR 0035](adr/0035-explicit-exclusive-turn-recovery.md) and
[ADR 0065](adr/0065-fingerprinted-pre-tool-approval-resumption.md).

### Provider evidence and recovery policy remain separate

Codex's `codex-api/src/error.rs` and protocol error mapping preserve semantic
variants and retry delays. OpenCode's provider normalization retains status and
retryability, while Pi retains HTTP status/headers before its outer
compatibility layer falls back to text matching. Hermes demonstrates both the
operational value and the coupling cost of a much broader, pattern-driven
recovery taxonomy.

Y-Harness adopts the common structured-evidence lesson without importing a
product-specific recovery table. `ModelProviderFailure` is bounded typed
evidence; legacy and Harness-owned failures remain `Model(String)`;
Observability receives no diagnostic content. A separate default-disabled
policy retries only typed rate-limit, overload, server, and transport failures
on the same Model, under the existing candidate deadline and provisional-output
fence. It does not parse strings, replay a Turn, infer cooldown, or install a
general Provider-specific recovery table. See
[ADR 0100](adr/0100-typed-model-provider-failure-evidence.md) and
[ADR 0101](adr/0101-bounded-typed-model-retry-policy.md).

### Context is compiled, not concatenated ad hoc

All six references expose some combination of instructions, session history,
compression, skills, memory, or provider conversion. Y-Harness turns those
inputs into a deterministic Context compilation phase with independent token
and byte limits, whole-Turn selection, explicit Memory provenance, and optional
semantic compaction that never rewrites authoritative history.

The implementation is in `src/context` and `src/memory`. See
[ADR 0021](adr/0021-bounded-whole-turn-conversation-context.md),
[ADR 0059](adr/0059-registered-token-counters-with-independent-byte-bounds.md),
and
[ADR 0060](adr/0060-bounded-non-authoritative-semantic-conversation-compaction.md).
Cross-Thread handoff reuses the same authority boundary through
[ADR 0096](adr/0096-attributed-per-turn-context.md) and
[ADR 0097](adr/0097-bounded-digest-bound-thread-handoff.md).

### MCP, CLI, and Skills are extension mechanisms, not trust shortcuts

The references show the value of discoverable tools and Skills. They also show
why discovery, loading, and execution authority must be separate. Y-Harness
uses typed registries for built-ins and third parties, namespaces MCP tools into
the ordinary Tool registry, models CLI/process execution behind a Process
Broker, and resolves declarative Skill packages by exact identity and digest.

An extension's origin affects trust even though its registration contract is
the same. Untrusted executable capabilities are supervised out of process and
still pass through Policy, State, bounds, cancellation, Observability, and
Verification. See `src/kernel/registry.rs`, `src/execution`,
`src/transport/mcp.rs`, `src/skill`, and
[ADR 0002](adr/0002-microkernel-boundary.md).

### Policy and isolation are independent controls

Codex most clearly documents the distinction between asking for approval and
constraining a command with an OS sandbox. Claude Code documents permission
rules and lifecycle hooks. Y-Harness retains both controls: Policy answers
whether an effect may be attempted, while a Process Broker constrains how an
external process may execute.

Approval is attributed, revisioned, durable, and fingerprint-bound before a
Tool effect. Process execution is deny-by-default and platform isolation claims
are limited to what the implementation proves. See `src/runtime/policy.rs`,
`src/approval`, `src/execution`, and
[ADR 0007](adr/0007-two-stage-policy-and-approval.md).

### Memory is a governed provider boundary

Hermes demonstrates the usefulness of persistent memory and replaceable memory
providers; Claude Code documents agent-scoped memory; the other references
also retain session or instruction context. Y-Harness keeps long-term Memory
separate from authoritative execution State and from ephemeral Context.

Agent Memory Hub is the first-party provider, accessed through the versioned
Memory port and MCP adapter. Retrieval is not automatic adoption, and memory
writes do not bypass Policy or provenance. See `src/memory` and
[ADR 0004](adr/0004-agent-memory-hub-provider.md).

### Orchestration is more than spawning subagents

Claude Code and Codex document isolated subagents and worktrees; Pi exposes
steering/follow-up queues; Hermes exposes delegation and profile isolation.
Y-Harness generalizes these product features into a bounded Task DAG with
dependencies, messages, Artifacts, workspace requirements, cancellation,
leases, and fencing. A public `Orchestrator` executes host-provided sub-Agent
capabilities with bounded concurrency, exact-lease settlement, and a fenced
durable Mailbox instead of mutable graph access. Workspace Provider API v1
turns each declarative workspace request into an exact-attempt lifecycle:
default deny, bounded prepare, canonical executor view, cancellation, cleanup,
then settlement. Local directories and full-object-ID-pinned detached Git
Worktrees are built-in adapters; neither is mislabeled as an OS sandbox.

The current scheduler and coordinator support single-host and shared-SQLite
multi-process execution. Multi-node consensus and remote takeover remain
explicit open work. See
`src/orchestration`,
[ADR 0011](adr/0011-fenced-task-orchestration.md), and
[ADR 0071](adr/0071-bounded-fenced-task-orchestrator.md) and
[ADR 0072](adr/0072-lease-fenced-task-mailbox.md), and
[ADR 0073](adr/0073-governed-task-workspaces-and-pinned-git-worktrees.md).

### Verification, Observability, and Evaluation remain separate

Callbacks, traces, trajectories, and tests appear across the references, but
they answer different questions. Y-Harness separates:

- Verification: may this Turn report completion?
- Observability: what bounded, content-free execution evidence occurred?
- Evaluation: how does behavior compare with a reproducible suite and baseline?

The corresponding implementations are `src/verification`,
`src/observability`, and `src/evaluation`. See
[ADR 0008](adr/0008-verification-completion-gates.md),
[ADR 0010](adr/0010-evaluation-is-not-verification.md), and
[ADR 0017](adr/0017-failure-isolated-content-free-observability.md).

## Non-copying rules

Y-Harness will not infer architectural validity from feature popularity.
Reference behavior is accepted only when it preserves the following
engine-owned invariants:

1. Business agents and client UX stay outside the Harness core.
2. State is durable, typed, bounded, and authoritative.
3. Discovered code has no ambient execution authority.
4. Policy, sandboxing, secret resolution, and extension trust are distinct.
5. Retrieval, observation, verification, and evaluation are distinct.
6. Cancellation does not imply rollback; uncertain effects are not replayed.
7. Protocol operations are process-local control, not a second state store.
8. Distributed continuation requires ownership and fencing evidence.
9. Every supported claim needs a named test, measurement, or explicit limit.

## Primary sources

### Pi Agent Harness

- [Agent loop and ordered multi-Tool execution at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/agent-loop.ts)
- [Composable Harness at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/harness/agent-harness.ts)
- [Built-in Provider registry at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/ai/src/providers/all.ts)
- [Custom model configuration at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/docs/models.md)
- [Package manager at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/core/package-manager.ts)
- [Package authority warning at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/docs/packages.md)
- [Session product contract at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/docs/sessions.md)
- [Explicit security boundary at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/docs/security.md)
- [Evaluation adapter at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/evals/src/pi-harness.ts)
- [Released CLI flags at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/cli/args.ts)
- [JSON print mode at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/modes/print-mode.ts)
- [Session event settlement at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/core/agent-session.ts)
- [Root-to-leaf fork materialization and lineage at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/coding-agent/src/core/session-manager.ts)

### Claude Code

- [Provenance, incompleteness, build, and use limitations of the supplied reconstruction at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/README.md)
- [Reconstructed query loop at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/query.ts)
- [Reconstructed scoped message queue at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/utils/messageQueueManager.ts)
- [Reconstructed streaming tool scheduler at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/services/tools/StreamingToolExecutor.ts)
- [Reconstructed tool permission/execution path at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/services/tools/toolExecution.ts)
- [Reconstructed compaction path at `3da94d5`](https://github.com/liuyang0508/claude-code-source-code/blob/3da94d5e5f2b99c9d82b0d8f09448b04775cd41f/src/services/compact/autoCompact.ts)
- [How Claude Code works](https://code.claude.com/docs/en/how-claude-code-works)
- [Hooks](https://code.claude.com/docs/en/hooks)
- [MCP](https://code.claude.com/docs/en/mcp)
- [Subagents](https://code.claude.com/docs/en/sub-agents)
- [Tools reference](https://code.claude.com/docs/en/tools-reference)

### Codex

- [Codex Rust core at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/core/README.md)
- [Turn loop at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/core/src/session/turn.rs)
- [Tool registry at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/core/src/tools/registry.rs)
- [Cross-platform sandbox manager at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/sandboxing/src/manager.rs)
- [SQLite migrations at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/state/src/migrations.rs)
- [Skill discovery at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/core-skills/src/loader/discovery.rs)
- [Codex app server at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/app-server/README.md)
- [Turn input queue at `61a4488`](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/core/src/session/input_queue.rs)
- [Exact-ID Turn steering at `61a4488`](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/app-server/src/request_processors/turn_processor.rs)
- [Bounded client recovery slots at `61a4488`](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/exec-server/src/client_recovery.rs)
- [Typed Thread fork boundaries at `61a4488`](https://github.com/openai/codex/blob/61a44880a85d2fd0d8770908dea5733495e571c8/codex-rs/app-server/README.md)
- [Deferred MCP Tool exposure at `25af12f`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/core/src/mcp_tool_exposure.rs)
- [Search Tool request behavior at `25af12f`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/core/tests/suite/search_tool.rs)
- [Exec JSONL lifecycle projection at `25af12f`](https://github.com/openai/codex/blob/25af12f7e61572b0bc18ddb1008be543b91519b0/codex-rs/exec/src/event_processor_with_jsonl_output.rs)
- [Official Codex documentation](https://developers.openai.com/codex/)

### Hermes Agent

- [Conversation loop at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/agent/conversation_loop.py)
- [`AIAgent` boundary at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/run_agent.py)
- [Context Engine at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/agent/context_engine.py)
- [SQLite state at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/hermes_state.py)
- [Approval flow at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/tools/approval.py)
- [Observer contract at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/docs/observability/README.md)
- [Architecture at `689b51b`](https://github.com/NousResearch/hermes-agent/blob/689b51bef68f9ec95b638121bb9c7fefa3703fb2/website/docs/developer-guide/architecture.md)
- [Crossed-response steering at `6ab5d2d`](https://github.com/NousResearch/hermes-agent/blob/6ab5d2df2a5748f23ba7557ec527fac628720a22/agent/conversation_loop.py)
- [Backup-first session recovery at `6ab5d2d`](https://github.com/NousResearch/hermes-agent/blob/6ab5d2df2a5748f23ba7557ec527fac628720a22/hermes_cli/session_recovery.py)

### OpenCode

- [Session loop and assembly at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/session/prompt.ts)
- [Normalized stream processor at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/session/processor.ts)
- [Tool registry at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/tool/registry.ts)
- [Permission service at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/permission/index.ts)
- [SQLite session schema at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/core/src/session/sql.ts)
- [Session projection at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/core/src/session/projector.ts)
- [Plugin runtime at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/plugin/index.ts)
- [Typed server at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/server/server.ts)
- [Serialized client prompt queue at `7534d23`](https://github.com/anomalyco/opencode/blob/7534d23551f665e65080809975b4ca5c7d63807b/packages/opencode/src/cli/cmd/run/runtime.queue.ts)
