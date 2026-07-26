# Reference architecture analysis

This document records how Y-Harness Engineering derives architectural lessons
from Pi Agent Harness, Claude Code, Codex, Hermes Agent, and OpenCode. It is a
design-input audit, not a feature-parity checklist and not an endorsement of
every implementation choice in those projects.

The baseline source snapshot was reviewed on 2026-07-25 and the Turn-steering
delta on 2026-07-26. Every source observation below links to an immutable
commit. The repositories were inspected locally from shallow checkouts; their
full test suites were not executed as part of this audit.

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
| Pi Agent Harness | [`5bc1c2c`](https://github.com/earendil-works/pi/tree/5bc1c2c0a6f07e00e8c240304182f213ab8d311f) | official public source supplied for this audit, MIT |
| Hermes Agent | [`689b51b`](https://github.com/NousResearch/hermes-agent/tree/689b51bef68f9ec95b638121bb9c7fefa3703fb2) | official public source, MIT |
| OpenCode | [`7534d23`](https://github.com/anomalyco/opencode/tree/7534d23551f665e65080809975b4ca5c7d63807b) | official public source, MIT |

The 2026-07-26 delta additionally pins current Codex at
[`61a4488`](https://github.com/openai/codex/tree/61a44880a85d2fd0d8770908dea5733495e571c8)
and Hermes at
[`6ab5d2d`](https://github.com/NousResearch/hermes-agent/tree/6ab5d2df2a5748f23ba7557ec527fac628720a22).
These newer coordinates are used only for the delta findings below; they do not
silently relabel observations made against the earlier baseline.

## 2026-07-26 Turn-steering delta

This delta applies “take the strengths, reject the liabilities” at mechanism
level. Similar names do not establish equivalent semantics.

| Source mechanism | Useful invariant extracted | Y-Harness adoption | Rejected or still open |
|---|---|---|---|
| Pi drains a small steering queue between assistant/tool steps and a follow-up queue when the loop would stop. | New input belongs at explicit loop boundaries rather than in arbitrary provider callbacks. | Pending steering is drained at Model and Tool safe boundaries. | Pi's process-local callback queue is not durable authority; Y-Harness does not inherit its session model or coding-product surface. |
| The Claude reconstruction has scoped/priority queues and preserves Tool-call/result adjacency before inserting ordinary input. | Provider history must never be malformed merely to make an interactive queue feel immediate. | A superseded Tool call receives a synthetic error result before steering becomes model-visible. | Reconstructed code is not copied. Priority tiers are deferred until a real engine-level requirement justifies their semantics and bounds. |
| Current Codex requires an exact active Turn ID and separates current-step input from later mail. | A stale client must not redirect whichever Turn happens to be running. | Protocol 12 requires `thread_id` plus exact `expected_turn_id`; a mismatch writes nothing. | Codex's process-local queue is not treated as recoverable State, and its coding-agent product behavior is not a Core contract. |
| Current Hermes restarts after steering crosses an in-flight response, and its recovery code copies SQLite/WAL/SHM before canonical rebuild and integrity checks. | A response sampled from old context is stale; recovery must preserve evidence before repair. | Crossed Model messages and Tool calls are discarded, their provisional stream step is invalidated, and steering is applied before resampling. | This delta does not copy Hermes's large parent-object loop, mutable learned-skill behavior, or claim that Y-Harness already matches its recovery breadth. |
| OpenCode's CLI runtime serializes queued ordinary prompts and allows client editing/removal. | Product-side queue UX can remain independent from Runtime semantics. | The TUI maps input during an active Turn to the engine's typed steering command. | An editable client queue is not Runtime steering authority and cannot replace durable acceptance evidence. |

The resulting Y-Harness mechanism is not a union of five feature lists.
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
| Pi Agent Harness | `agent-loop.ts` keeps the model/tool loop small, normalizes messages at the provider boundary, emits typed events, and supports steering/follow-up queues. `AgentHarness` composes sessions, resources, hooks, compaction, tools, and model selection. Pi also has a real same-process evaluation adapter. | Embeddable runtime, explicit loop events, provider-neutral model contract, resource discovery, and one registration path for built-ins and extensions. | Package or extension installation is not authority to execute arbitrary code in the kernel. Session files are not the authoritative HA state model. Model/tool parallelism is an explicit scheduler concern, not an implicit registry behavior. |
| Claude Code | The reconstructed 2.1.88 source shows an async-generator query loop, streaming tool execution with concurrent/exclusive scheduling, several compaction and recovery paths, layered permissions, MCP management, plugins, persistent tasks, and agent teams. Official docs independently describe instructions, skills, MCP, hooks, permissions, subagents, and worktrees. | Layered Context, discoverable Skills, MCP as a Tool adapter, typed lifecycle observations, capability-scoped sub-tasks, and declarative workspace requirements. Recovery regressions become clean-room fault-injection cases. | Reconstructed code is not copied or treated as authoritative. Product-specific instruction filenames, feature flags, shell-hook semantics, coding workflows, and UI behavior are not kernel contracts. Hooks cannot bypass Policy, bounds, provenance, or failure isolation. |
| Codex | `run_turn` owns the sampling/tool/follow-up loop and captures one step view for context and advertised tools. A Rust core supports TUI, exec, and a typed app-server protocol. Tool routing records lifecycle outcomes, and the sandbox manager selects macOS Seatbelt, Linux seccomp/bubblewrap, or Windows restricted-token execution independently from approval policy. SQLite state, recovery, skills, plugins, memories, multi-agent paths, and OpenTelemetry are distinct crates or modules. | Rust semantic core, client/runtime separation, versioned asynchronous protocol, Thread/Turn/Item state, explicit approval, process-broker isolation, skills/MCP, and worktree-aware orchestration. | Coding-agent behavior stays outside Core. Process-local operation state is not durable truth. Remote recovery cannot take over a Turn without an external lease and fencing authority. |
| Hermes Agent | One `AIAgent` serves CLI, gateway, ACP, batch, cron, and API entry points. The source has extensive provider normalization, retry/error classification, context compression, SQLite/FTS session storage, checkpoints, approvals, many execution environments, memory/context plugins, trajectories, and observer hooks. The extracted `conversation_loop.py` still drives a large parent-object state surface, showing useful production handling and a coupling cost at the same time. | General-purpose rather than coding-only scope, shared core across entry points, Memory and Context provider ports, profiles/scopes, provider neutrality, and trajectories as Evaluation evidence. Error taxonomies and provider quirks become adapter conformance cases. | Messaging adapters, provider configuration, mutable skill learning, and application commands do not belong in the microkernel. Import-time self-registration and ambient plugin authority are not accepted extension contracts. |
| OpenCode | The current tree separates Effect-based services, normalized `LLMEvent` streams, session processing, permissions, tools, providers, plugins, MCP, SQLite schemas/migrations, event-backed session projection, control-plane/worktree code, SDKs, TUI, web, and desktop clients. The permission service keeps pending approvals process-local, while the newer core state uses SQLite and explicit projection code. | Headless core plus typed service boundary, streaming events, provider/tool registries, permission checks, durable projection, and multiple clients over one semantic runtime. | Coding modes, local-server assumptions, and desktop/web product policy do not define Harness semantics. In-process plugins do not receive ambient engine authority by installation alone. Pending approvals and transient stream state cannot become authoritative recovery state. |

## Source-level competitive findings

| Layer | Strongest observed lessons | Y-Harness position after this audit |
|---|---|---|
| Context Engine | Claude Code and Codex have multiple compaction/recovery paths shaped by real provider failures. OpenCode now models context epochs; Pi keeps compaction understandable; Hermes handles wide provider variance and prompt caching. | Deterministic whole-Turn selection, independent byte/token bounds, and provenance are credible local strengths. Semantic faithfulness, provider cache behavior, media overflow recovery, and long-session quality are not competitively proven. |
| Agent Loop | Pi has the clearest small generic loop. Codex captures a consistent per-step view. Claude Code has mature streaming fallback and orphan-result cleanup. Hermes has broad retry/failover classification. OpenCode converges runtimes on one event stream. | Durable settlement, deadlines, cancellation, and explicit recovery are implemented, but the direct-provider path and production fault matrix are much narrower. |
| Tool Runtime | Codex has the strongest public cross-platform sandbox implementation. Claude Code shows mature concurrent/exclusive tool scheduling. Hermes has the broadest execution-environment catalog. OpenCode and Pi have low-friction extension paths. | Policy-governed registration and fail-closed process authority are strong design choices. Linux/Windows containment, tool breadth, provider-hosted tools, and hostile-process tests remain material gaps. |
| State Engine | Codex, OpenCode, and Hermes all have substantial SQLite schemas, migrations, and recovery behavior; OpenCode now has event-backed session projection. Pi deliberately supports lighter JSONL/in-memory session stores. | Typed append-only authority, CAS, bounded recovery, snapshots, and backup-first migrations are implemented. Archival, blob offload, remote ownership, and long-running production evidence are open. |
| Memory Engine | Hermes exposes several provider plugins. Codex and Claude Code integrate product memory. | The Agent Memory Hub boundary is a genuine differentiator, but retrieval quality and end-to-end outcome gain require shared benchmarks, not architecture claims. |
| Skill Engine | Codex and Claude Code have marketplace/plugin product flows; OpenCode pulls remote skills; Pi packages skills/extensions; Hermes ships a large catalog. | Signature, revocation, receipts, exact pins, and bounded resolution are strong governance primitives. Discovery UX, private registry, cache/mirror, dependency acquisition, and ecosystem size lag. |
| Policy Engine | Codex separates approval from OS sandboxing and supports managed requirements. Claude Code has rich permission matching/classification. OpenCode has concise rule evaluation. Hermes covers approvals across CLI/gateway surfaces. | Durable attributed approvals and fingerprint-bound continuation are stronger than an in-memory prompt. Human/tenant identity, signed receipts, role policy, remote continuation, and cross-platform containment are open. |
| Orchestration | Codex and Claude Code expose mature multi-agent/product workflows. OpenCode has subagents and worktree/control-plane code. Hermes has delegation across many surfaces. Pi has simple steering/follow-up semantics. | Task DAGs, leases, fencing, mailbox, and workspace lifecycle are architecturally substantial. Multi-node consensus, durable orphan reaping, remote executors, and comparative task success are not proven. |
| Verification | Claude Code has stop hooks and verification-oriented skills; Codex has review/hook paths; Hermes records verification evidence. | Verification is a first-class engine layer rather than only a prompt convention. Its real-world graders and false-completion rate still need competitive measurement. |
| Observability | Codex has OpenTelemetry modules and rich runtime events. Hermes has a versioned observer contract. OpenCode uses Effect/OTel. Claude Code and Pi expose extensive events. | Failure-isolated, content-free evidence is privacy-conscious and bounded. Exporter breadth, distributed traces, operator UX, and overhead comparisons are open. |
| Evaluation | Pi includes an executable harness adapter. Hermes records trajectories. All public projects have substantial tests, but their tests are not a controlled cross-Harness comparison. | Y-Harness has a versioned regression runner, not a competitive result. The required cross-Harness protocol is defined in [`competitive-benchmark.md`](competitive-benchmark.md). |

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

### Context is compiled, not concatenated ad hoc

All five references expose some combination of instructions, session history,
compression, skills, memory, or provider conversion. Y-Harness turns those
inputs into a deterministic Context compilation phase with independent token
and byte limits, whole-Turn selection, explicit Memory provenance, and optional
semantic compaction that never rewrites authoritative history.

The implementation is in `src/context` and `src/memory`. See
[ADR 0021](adr/0021-bounded-whole-turn-conversation-context.md),
[ADR 0059](adr/0059-registered-token-counters-with-independent-byte-bounds.md),
and
[ADR 0060](adr/0060-bounded-non-authoritative-semantic-conversation-compaction.md).

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

- [Agent loop at `5bc1c2c`](https://github.com/earendil-works/pi/blob/5bc1c2c0a6f07e00e8c240304182f213ab8d311f/packages/agent/src/agent-loop.ts)
- [Composable Harness at `5bc1c2c`](https://github.com/earendil-works/pi/blob/5bc1c2c0a6f07e00e8c240304182f213ab8d311f/packages/agent/src/harness/agent-harness.ts)
- [Session contract at `5bc1c2c`](https://github.com/earendil-works/pi/blob/5bc1c2c0a6f07e00e8c240304182f213ab8d311f/packages/agent/src/harness/session/session.ts)
- [Evaluation adapter at `5bc1c2c`](https://github.com/earendil-works/pi/blob/5bc1c2c0a6f07e00e8c240304182f213ab8d311f/packages/evals/src/pi-harness.ts)

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
