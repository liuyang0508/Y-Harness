# Reference architecture analysis

This document records how Y-Harness Engineering derives architectural lessons
from Pi Agent Harness, Claude Code, Codex, Hermes Agent, and OpenCode. It is a
design-input audit, not a feature-parity checklist and not an endorsement of
every implementation choice in those projects.

The source snapshot was reviewed on 2026-07-25. Open-source observations below
link to immutable commits. Claude Code observations are limited to its public
product documentation; they do not claim knowledge of its private
implementation.

## Comparison

| Reference | Observed architecture | Adopted by Y-Harness | Deliberately not inherited |
|---|---|---|---|
| Pi Agent Harness | A small stateful agent core exposes an event stream and explicit model/tool loop. `AgentMessage` is normalized before provider input. The coding-agent SDK composes sessions, resource loading, tools, extensions, skills, and persistence. | Embeddable runtime, explicit loop events, provider-neutral model contract, resource discovery, and one registration path for built-ins and extensions. | Package or extension installation is not authority to execute arbitrary code in the kernel. Session files are not the authoritative HA state model. Model/tool parallelism is an explicit scheduler concern, not an implicit registry behavior. |
| Claude Code | Public docs describe layered instructions, on-demand skills, MCP tools, lifecycle hooks, permissions, isolated subagents, and worktree isolation. | Layered Context, discoverable Skills, MCP as a Tool adapter, typed lifecycle observations, capability-scoped sub-tasks, and declarative workspace requirements. | Product-specific instruction filenames, shell-hook semantics, coding workflows, and UI behavior are not kernel contracts. Hooks cannot bypass Policy, bounds, provenance, or failure isolation. |
| Codex | A Rust core is consumed by multiple interfaces. The app server exposes typed thread/turn operations and asynchronous notifications. Commands run under an OS sandbox with approval configured as a separate control. | Rust semantic core, client/runtime separation, versioned asynchronous protocol, Thread/Turn/Item state, explicit approval, process-broker isolation, skills/MCP, and worktree-aware orchestration. | Coding-agent behavior stays outside Core. Process-local operation state is not durable truth. Remote recovery cannot take over a Turn without an external lease and fencing authority. |
| Hermes Agent | One platform-agnostic agent serves CLI, gateway, ACP, batch, and API entry points. Its documented subsystems include context compression, provider resolution, tool registry, SQLite sessions, memory providers, skills, plugins, profiles, and trajectory generation. | General-purpose rather than coding-only scope, shared core across entry points, Memory and Context provider ports, profiles/scopes, provider neutrality, and trajectories as Evaluation evidence. | Messaging adapters, provider configuration, mutable skill learning, and application commands do not belong in the microkernel. Import-time self-registration and ambient plugin authority are not accepted extension contracts. |
| OpenCode | The repository separates core packages, a typed local server, session processing, permissions, tool registration, SDKs, TUI, web, and desktop clients. Session processing consumes streaming model events and materializes tool-call states. | Headless core plus typed service boundary, streaming events, provider/tool registries, permission checks, and multiple clients over one semantic runtime. | Project singleton state, coding modes, local-server assumptions, and desktop/web product policy do not define Harness semantics. A client or transient stream processor cannot become the authoritative state owner. |

## Cross-project synthesis

### One semantic core, multiple surfaces

Pi's embeddable SDK, Codex's core/app-server split, Hermes's shared agent, and
OpenCode's service/client split all support one conclusion: CLI, TUI, SDK,
Web, and Desktop are consumption surfaces, not separate Harness
implementations.

Y-Harness therefore has one `HarnessRuntime`, an embedded Rust API, and one
versioned protocol. The reference CLI/TUI calls that protocol. A future client
must do the same instead of duplicating Agent Loop, Policy, or State behavior.
See [`architecture.md`](architecture.md) and
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

- [Agent core at `8eef62e`](https://github.com/earendil-works/pi/blob/8eef62ed3ea62d646a7fad92fa583fc8d71fec17/packages/agent/README.md)
- [Coding-agent SDK at `8eef62e`](https://github.com/earendil-works/pi/blob/8eef62ed3ea62d646a7fad92fa583fc8d71fec17/packages/coding-agent/docs/sdk.md)
- [Extensions at `8eef62e`](https://github.com/earendil-works/pi/blob/8eef62ed3ea62d646a7fad92fa583fc8d71fec17/packages/coding-agent/docs/extensions.md)
- [Packages and trust implications at `8eef62e`](https://github.com/earendil-works/pi/blob/8eef62ed3ea62d646a7fad92fa583fc8d71fec17/packages/coding-agent/docs/packages.md)

### Claude Code

- [How Claude Code works](https://code.claude.com/docs/en/how-claude-code-works)
- [Hooks](https://code.claude.com/docs/en/hooks)
- [MCP](https://code.claude.com/docs/en/mcp)
- [Subagents](https://code.claude.com/docs/en/sub-agents)
- [Tools reference](https://code.claude.com/docs/en/tools-reference)

### Codex

- [Codex Rust core at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/core/README.md)
- [Codex app server at `4c43465`](https://github.com/openai/codex/blob/4c43465133428898aa84f0bfc02c306ed65fb66a/codex-rs/app-server/README.md)
- [Official Codex documentation](https://developers.openai.com/codex/)

### Hermes Agent

- [Architecture at `6668242`](https://github.com/NousResearch/hermes-agent/blob/666824261a017d62d82e2a7e646b4599c1fc830e/website/docs/developer-guide/architecture.md)
- [Repository overview at `6668242`](https://github.com/NousResearch/hermes-agent/blob/666824261a017d62d82e2a7e646b4599c1fc830e/README.md)

### OpenCode

- [Repository overview at `5e2a625`](https://github.com/anomalyco/opencode/blob/5e2a6257b22c0141a20c281f4c2a641311afe5a5/README.md)
- [Typed server at `5e2a625`](https://github.com/anomalyco/opencode/blob/5e2a6257b22c0141a20c281f4c2a641311afe5a5/packages/opencode/src/server/server.ts)
- [Session stream processor at `5e2a625`](https://github.com/anomalyco/opencode/blob/5e2a6257b22c0141a20c281f4c2a641311afe5a5/packages/opencode/src/session/processor.ts)
- [Tool registry at `5e2a625`](https://github.com/anomalyco/opencode/blob/5e2a6257b22c0141a20c281f4c2a641311afe5a5/packages/opencode/src/tool/registry.ts)
