# Y-Harness Engineering

**Agent = LLM × Harness = X × Y**

Y-Harness Engineering builds a headless, embeddable, and serviceable Agent
Harness Runtime. It is infrastructure for building agents, not a coding agent
or another application-specific assistant.

## Quick start

Install from a local checkout with Rust 1.88 or newer:

```bash
./scripts/install.sh
yh --version
```

Run the deterministic, zero-network Agent Loop:

```bash
yh demo "hello Y-Harness"
```

Install and run the optional full-screen terminal product:

```bash
./scripts/install-tui.sh
yh-tui --demo
```

The checkout also includes a local demo `y-harness.json`, so
`yh-tui --config y-harness.json` works directly from the repository root.
That file deliberately uses the deterministic `local/demo` model and makes no
network request.

Run the same TUI against a real OpenAI Responses model:

```bash
cp config/y-harness.openai.example.json y-harness.local.json
# Set one model ID available to your OpenAI project in y-harness.local.json.
export OPENAI_API_KEY='...'
yh doctor y-harness.local.json
yh-tui --config y-harness.local.json
```

The API key is resolved from the named environment variable and never stored
in configuration or State. The adapter sends `store: false`, disables
provider-side parallel function calls, and keeps Tool execution, Policy, and
State inside Y-Harness. Configured shell-free JSON Tools, selected MCP Tools,
and Agent Memory Hub remain optional; see the
[Chinese quick start](docs/quickstart.zh-CN.md).

Create and validate a persistent Harness service:

```bash
yh init my-harness
cd my-harness
yh doctor
yh serve
```

`yh serve` is a headless Protocol v12 JSONL service over stdin/stdout. It
persists State, approvals, and Task coordination under `.y-harness/`. A
language-neutral Task Worker example is included:

```bash
YH_BIN="$(command -v yh)" \
python3 /path/to/Y-Harness/examples/task_worker_client.py y-harness.json
```

See the [Chinese quick start](docs/quickstart.zh-CN.md), the
[acceptance checklist](docs/acceptance-checklist.zh-CN.md), and the
[protocol specification](docs/protocol.md) for the complete operator path.

## Product boundary

```text
optional products: TUI · Desktop · Web · IM · SDK hosts
                           │
                    versioned protocol
                           │
                 headless Y-Harness Runtime
                           │
                      Harness Core
```

Core and Runtime own execution semantics. Every product surface is an
independent, replaceable client module that renders and controls the same
engine through its public contract. Clients do not open engine databases,
construct providers, own authoritative Agent state, or bypass Policy.

The repository currently ships two separately installable runtime packages
and two non-runtime benchmark tools:

| Package | Binary | Role |
|---|---|---|
| `y-harness` | `yh` | headless engine, service, diagnostics, migrations |
| `y-harness-tui` | `yh-tui` | full-screen terminal client over Protocol v12 |
| `y-harness-benchmark-runner` | `yh-bench` | released-product evidence adapter outside the semantic Core |
| `y-harness-fault-fixture` | `yh-fault-fixture` | deterministic Tool fault process and oracle outside the semantic Core |

## Architecture

The target architecture has eleven layers:

1. Context Engine
2. Agent Loop
3. Tool Runtime
4. State Engine
5. Memory Engine
6. Skill Engine
7. Policy Engine
8. Orchestration
9. Verification
10. Observability
11. Evaluation

The microkernel owns identity, lifecycle, typed registries, state transitions,
policy enforcement, cancellation, budgets, trace ordering, and protocol
version negotiation. Models, tools, MCP servers, CLI adapters, skills, memory
providers, and evaluators are typed capabilities around that kernel.

Built-ins and extensions use the same public registration contracts. Equal
contracts do not imply equal trust: untrusted executable extensions will run
out of process.

Model, Tool, Memory, Token Counter, Conversation Compactor, Skill, Verifier,
Grader, and Observer capabilities are registered by stable identity with
collision rejection and operator-assigned origin. The Runtime selects either
one exact Model or an explicit ordered failover route of at most 16 registered
identities; the Registry never silently replaces or switches a provider.
Multi-model routes apply a 30-second deadline to each attempt by default,
configurable from 1 millisecond to 24 hours; an earlier total Turn deadline
always wins. Attempt timeout cancels the provider before its Future is
released. Cancellation, the Turn deadline, and successfully delivered
provisional output stop fallback. Model-produced State retains the actual
successful Model identity and origin for durable provenance, while
Observability records every attempt.
Registry-selected identity is never re-queried from provider code. The
compatibility constructor captures, panic-isolates, validates, and freezes
`LanguageModel::id()` exactly once; a bad identity rejects execution before
`TurnStarted`.
See [ADR 0018](docs/adr/0018-model-registry-and-provenance.md) and
[ADR 0070](docs/adr/0070-explicit-bounded-model-failover.md).

Executable extension metadata is treated as code, not inert configuration.
Model, Tool, Memory, Token Counter, Conversation Compactor, Secret, Verifier,
Grader, and Process Broker metadata is captured through one content-free panic
boundary and retained after validation; later execution and evidence paths
consume that frozen snapshot.

Mutable capability registries have a shared 4,096-entry ceiling (Evaluation
graders retain their tighter 64-entry limit), and extension-origin identities
are bounded before provider metadata is invoked. Tool descriptors are capped at
1 MiB each and 8 MiB per registry, with batch registration remaining atomic.
Skill registries additionally cap aggregate package content at 64 MiB. MCP
discovery checks its catalog count before allocating staging collections.

See [Architecture](docs/architecture.md) and the
[architecture decisions](docs/adr/). Engineering acceptance criteria live in
[Engineering standards](docs/engineering-standards.md); measured runtime
evidence lives in the [performance baseline](docs/performance-baseline.md).
The language-neutral wire contract lives in the
[client protocol v11 specification](docs/protocol.md).
The observed lessons, rejected assumptions, immutable source snapshots, and
code/ADR traceability for Pi Agent Harness, Claude Code, Codex, Hermes Agent,
and OpenCode live in the
[reference architecture analysis](docs/reference-analysis.md).
The controlled same-model and product-default rules required for any
“outperforms” claim live in the
[competitive Harness benchmark](docs/competitive-benchmark.md).
Current proof and open blockers are tracked in
[Release readiness](docs/release-readiness.md). Exact pre-1.0 wire, persistence,
API, and migration rules live in the
[compatibility policy](docs/compatibility.md).

## License

Y-Harness is dual-licensed under
[MIT](LICENSE-MIT) or [Apache License 2.0](LICENSE-APACHE), at your option.

## Current executable baseline

The baseline proves the core end-to-end invariant:

```text
Thread → Turn → Item → Model → Tool → Policy → Event Journal → Model → Result
```

Run it:

```bash
cargo run -- demo "hello Y-Harness"
```

Run the versioned behavioral regression gate:

```bash
cargo run --locked -- eval-smoke
```

Run a minimal embedding host that uses only the public crate API:

```bash
cargo run --locked --example embedded
```

The [embedded example](examples/embedded.rs) registers a host-defined Model and
Tool with explicit extension provenance, uses in-memory State, and executes the
same Policy-controlled Agent Loop without a protocol transport, network, or
ambient files.

Run the public Task orchestration surface:

```bash
cargo run --locked --example orchestrated
```

The [orchestrated example](examples/orchestrated.rs) executes a dependency DAG
through a host-defined `TaskExecutor`, durable coordinator, bounded scheduler,
lease-fenced Mailbox, explicit local Workspace Provider, cleanup-before-
settlement lifecycle, and fenced completion without embedding business-agent
behavior in Core.

The demo uses a deterministic local model and an `echo` tool. It performs no
network requests. SQLite is the authoritative append-only state and evidence
journal; the CLI exports a derived JSONL trace under `.y-harness/traces/`.
Atomic stream-version comparison prevents competing runtimes from committing
transitions validated against stale state. A lightweight head cache accelerates
the common path but cannot override the transactional comparison.

State independently bounds identities, encoded events, event pages, aggregate
Thread recovery charge, and checkpoint labels before persistence. Event Stores
atomically compare both stream version and recovery bytes, while append/read
results are revalidated before caching, projection, or exposure. A replacement
store therefore cannot silently cross Thread boundaries, inject malformed
ordering, or create a stream that built-in recovery cannot materialize. SQLite
also checks stored byte lengths before materializing event and snapshot text,
then validates durable envelopes while decoding.

Explicit State snapshots accelerate long-Thread recovery without replacing the
journal. A snapshot carries a projection digest and journal anchor; State
revalidates both, replays only the bounded authoritative tail, and falls back to
full replay if the cache is absent or invalid. Checkpoints remain marker events.
A hard one-million-event and 64 MiB recovery-charge boundary keeps both
fallback and tail recovery finite until archival policy is implemented. Reads
use count-plus-byte pages rather than asking a store to materialize the stream
first. The final event slot and 4 KiB recovery charge are reserved for terminal
Turn settlement so accepted work cannot strand itself at either limit. A
read-only capacity projection reports exact used, general remaining, and
terminal-reserve values for both dimensions, with the worst pressure becoming
healthy, warning, critical, terminal-only, or exhausted; remote callers require
the separate `thread.capacity` permission. The signal does not claim remaining
disk or process memory. Runtime refuses to start without four general event
slots for its minimum viable evidence, and any later Item persistence failure
immediately uses the terminal reserve to attempt a durable failed settlement.

Hosts may opt into automatic snapshot maintenance with a bounded event cadence
and worker limit. Maintenance runs only after terminal Turns, never reverses a
successful journal settlement, sheds work instead of queueing when saturated,
and exposes content-free success/failure/capacity counters plus a graceful
shutdown drain. Built-in stores atomically retain only the newest disposable
snapshot per Thread.

An abandoned running Turn is marked `interrupted` only through explicit
recovery after the host has established exclusive Thread ownership and
confirmed the previous worker stopped. Normal execution never performs
takeover: a second Runtime sees the durable running Turn and is rejected.
Y-Harness does not automatically replay potentially non-idempotent tool calls.

Callers can also supply a monotonic cancellation token and a Turn deadline.
Context, Model, Policy, and Tool waits are bounded; explicit cancellation and
timeout become distinct, durably recorded terminal states. State settlement is
allowed to finish after the deadline so the journal remains authoritative.
Each Runtime admits at most 32 concurrent Turns by default; hosts can select a
validated limit from 1 through 4,096. Excess work fails fast with a retryable
typed error before creating a Turn or consuming journal capacity.
Capability future construction, polling, and cleanup are panic-isolated;
provider panic payloads never enter State or client errors, and the Turn follows
its ordinary failed settlement path.

Each new Turn receives a deterministic, newest-first-selected suffix of prior
whole Turns, restored to chronological order before inference. Only
model-visible conversation, Tool, and Verification Items cross that boundary;
internal Policy, approval, memory, and stop evidence remain in State. Included
Turn IDs, omitted count, and conservative budget are journaled. Prompt, Context
blocks, aggregate Context, Tool output, complete Model request, errors, and
Agent Loop steps all have transport-independent hard bounds.

JSON bounds are enforced while work is performed, not after an unbounded
temporary encoding exists. Caller/provider `Value` trees are iteratively
limited to 64 nested container levels and 65,536 nodes before serialization;
counting and materializing writers stop at each subsystem's byte ceiling. The
same rule covers embedded APIs as well as Approval, Context, Evaluation,
Model/Tool adapters, MCP, State, and trace export. Raw transport frames,
HTTP/process bodies, and SQLite text keep independent ingress limits.

Context may select an exact registered Token Counter for the active provider.
The counter is versioned, collision-safe, origin-bearing, metadata-frozen, and
panic-isolated. It recounts conversation segments, Memory packs, and Skill
blocks instead of trusting provider estimates. Token limits and serialized-byte
limits are independent; a counter can improve allocation accuracy but can
never weaken the byte ceilings. Without a selected counter, the deterministic
serialized-byte estimate remains the fallback.

Hosts may also explicitly select a versioned semantic Conversation Compactor.
It receives only a bounded newest slice of omitted whole Turns, plus the count
of still-older omissions, and runs under the Turn's Context deadline,
cancellation, observation, and panic boundary. The final block has independent
token/byte limits, exact covered-Turn IDs, source/content SHA-256 fingerprints,
and an engine-owned non-authoritative warning. Compactor failure fails the Turn
instead of silently presenting a partial summary as complete. Original Items
remain untouched in authoritative State; summary text is ephemeral derived
Context rather than a replacement conversation record. State schema 2
introduced bounded content-free evidence, and the current schema-6 writer
preserves it: compactor identity, exact coverage, source/content fingerprints,
and token/byte charges.

Populated schema-1, schema-2, schema-3, schema-4, and schema-5 SQLite databases
require an offline, backup-first migration before the current Runtime opens
them:

```bash
cargo run -- state-migrate /absolute/path/state.db /absolute/path/state-pre-v6.rollback.db
```

Stop all writers first. The command never overwrites the backup path and never
rewrites historical events. See the
[migration runbook](docs/state-migration.md) for retry and rollback boundaries.

Tool authorization supports `allow`, `deny`, and `ask`. An `ask` produces a
separately correlated approval request with a risk class; both the policy
decision and approval settlement are journaled before tool execution. The
runtime denies `ask` by default when no approval handler is installed. State
schema 4 binds every Policy decision—including deny and ask—to the exact
trust-bearing origin of the registered Tool, so authorization provenance does
not disappear when execution is refused, deferred, or fails.

State schema 5 additionally records bounded, non-executable Provider
Continuation Items. Runtime binds each capsule to the Model registration that
actually produced it, filters capsules by exact Model identity and origin, and
keeps an unfinished Tool chain on that Model instead of unsafe cross-provider
failover. The direct OpenAI adapter uses this path to replay encrypted
reasoning items with `store: false`.

State schema 6 records accepted Turn steering separately from its safe-boundary
application. The caller must name the exact active Turn; acceptance is durable
and actor-attributed before acknowledgement. If steering crosses Model
inference, Runtime invalidates provisional output, discards the stale response,
applies the queued input FIFO, and samples again. A stale Tool call never
executes, and a Turn cannot complete with accepted input still pending.

For asynchronous human workflows, a provider-neutral Approval Inbox supports
idempotent submission, a deterministic oldest-first 16-record pending window,
revision-CAS settlement, and orphaning. Its SQLite implementation survives
process restart and fences competing approvers. Schema 2 records the
authority-scoped Turn requester and deciding actor; settlement atomically
rejects self-approval without changing the pending revision. mTLS callers use
the exact client leaf-certificate SHA-256 fingerprint as their subject.
`LocalProcess` is deliberately one actor and cannot impersonate two local
roles. Stored status, identities, and record bodies are byte-checked before
Rust text allocation and revalidated after decoding. Pending admission reserves
enough record capacity for every supported terminal decision, and in-memory
transitions commit only after the complete candidate validates. Runtime records
`ApprovalRequested` before waiting. State schema 3 binds that boundary to the
requester, Tool origin, and SHA-256 of the exact Model request. After proving
exclusive Thread ownership, an embedded host may resume only when
`ToolCall → PolicyDecision::Ask → ApprovalRequested` are the final durable
Items and reconstructed Context, Memory, Model, actor, and Tool metadata match.
If `ApprovalDecision` exists without `ToolResult`, Tool execution is uncertain
and Y-Harness refuses generic replay. Generic recovery still interrupts and
orphans abandoned work. Protocol 9 intentionally exposes no remote takeover
without cross-host lease/fencing. Hosts that install the inbox also expose
typed pending/get/settle commands.

Populated schema-1 Approval Inbox databases require a separate offline
migration:

```bash
cargo run -- approval-migrate \
  /absolute/path/approval.db \
  /absolute/path/approval-v1.rollback.db
```

The backup is no-clobber and SHA-256-bound to the source. Because schema 1 had
no actor evidence, pending requests are orphaned and terminal identities are
marked explicitly unattributed. See the
[Approval migration runbook](docs/approval-migration.md).

Interrupted-Turn recovery keeps durable orphaning atomic without bulk-loading
every approval body: SQLite selects a bounded identity set, then validates and
updates one record at a time inside the same immediate transaction.

Completion verifiers are registered through a typed, collision-safe registry.
All must pass before an assistant candidate completes the Turn. Retryable
failures return structured feedback to the Agent Loop; hard failures terminate
the Turn. Verification records evidence but never pretends to generically roll
back external side effects. Reversal is explicit: a host may register a
Tool-specific `CompensationTool`, which resolves the original successful effect
from authoritative State and then follows the ordinary Policy, approval,
execution, and Tool-result path. Retries must reuse one stable idempotency key;
a prior successful settlement is returned from State without repeating the
provider effect.

Skill packages are declarative, exact-versioned, and SHA-256 pinned. The Skill
Engine resolves dependencies deterministically, detects cycles, verifies
required Tools and whole-package token budgets, loads instructions into Context
in dependency order, and leaves resources on demand. Skills do not execute code
or bypass Tool Policy. External Skill origins additionally require a strict
Ed25519 signature from an explicitly configured publisher trust root; digest
integrity alone is not treated as authenticity.

Publisher trust is live rather than install-time-only: keys have optional
validity windows and immutable effective revocations, supplied transparency
receipts are verified against separately trusted Ed25519 log roots, and a
publisher may require them. Resolution, resource reads, and every Context
compile recheck publisher and log status, so a later revocation stops further
governed use. Verified log/entry/time metadata stays with Skill provenance.
Signed receipts are attestations, not yet Merkle inclusion or cross-log
consistency proofs.

The optional `https-skill` feature acquires one signed package from one exact
operator-configured public HTTPS URL. Every call supplies an exact Skill
identity and digest pin; redirects, retries, ambient proxies, Referer, URL
credentials/query/fragment, non-JSON responses, and oversized bodies are
rejected. Package content is capped at 2 MiB raw and 16 MiB encoded, and the
registry is capped at 64 MiB of aggregate package content. The safe
fetch-and-register path performs all live trust checks before mutation.
Catalog discovery, authenticated private registries, caching, and recursive
dependency fetching are not implied.

Evaluation is a separate comparison layer: validated cases run through an
`EvaluationTarget` with engine-owned cancellation and deadlines. Cases and
graders execute with independently bounded concurrency; panics, timeouts, and
ordinary failures become isolated report outcomes. Immutable samples are
shared without copying full Turn outcomes, output order is deterministic, and
exact case/grader baselines detect missing results, errors, score regressions,
required-pass failures, and same-name Grader origin changes. Format-2 suites,
baselines, and reports are self-describing; every grade and requirement retains
the registered Grader's trust-bearing origin. Graders cannot alter live
Verification or Agent Loop control. The materialized report API admits at most
64 cases per batch; larger datasets must be chunked by the caller. Deserialized
suites, baselines, and reports are revalidated at their execution/comparison
boundaries.

The checked-in `evals/harness-smoke-suite.json` and
`evals/harness-smoke-baseline.json` form the first executable regression
baseline. `cargo run --locked -- eval-smoke` runs two isolated end-to-end
Tool-loop cases through the real reference Runtime, emits format-2 JSON
report, creates no ambient files, and exits nonzero on regression. It is a
narrow Harness contract smoke gate, not a claim about hosted-model quality or
application usefulness. See
[ADR 0067](docs/adr/0067-versioned-harness-smoke-evaluation-gate.md) and
[ADR 0069](docs/adr/0069-origin-bound-versioned-evaluation-artifacts.md).

Orchestration Core provides validated Task DAGs, deterministic priority
scheduling, expiring leases with fencing tokens, transitive failure blocking,
ordered messages, Artifact references, and declarative workspace isolation.
The graph persists through a revisioned coordinator contract. Its SQLite
implementation uses atomic compare-and-swap, durable WAL settings, bounded
validated snapshots, allocation-time stored-text limits, and rejects stale
writers across independent connections. This provides single-host recovery and
coordination; multi-node consensus and distributed availability are not
claimed.

The public `Orchestrator` executes host-provided `TaskExecutor` capabilities
with bounded concurrency, per-Task timeout, cooperative cancellation, panic
isolation, dependency progress, and exact-lease settlement. It cancels a local
executor when another coordinator mutation fences its claim and discards stale
results. Each execution also receives a lease-fenced `TaskMailbox` for durable
CAS-safe sends and count/byte-bounded cursor inbox pages; completed or stale
attempts cannot publish late messages. Schedulers claim at most 64 ready Tasks
per call. Claim inputs, lease deadline, and attempt capacity are preflighted
before expired-lease release or dependency propagation, so a returned claim
error leaves the graph unchanged.

Before executor entry, the Orchestrator now fulfills the declared workspace
mode through Workspace Provider API v1. Filesystem access is denied by default;
hosts may explicitly install bounded local-directory provisioning or detached
Git Worktrees pinned to a full object ID and launched through a Process Broker.
The executor receives only a canonical `TaskWorkspace` view, while the
Orchestrator retains cleanup authority, cancels before release, and releases
before Task settlement. Directory or Worktree isolation is not an OS sandbox;
untrusted executors still require a Process Broker with exact filesystem and
network authority. See
[ADR 0071](docs/adr/0071-bounded-fenced-task-orchestrator.md) and
[ADR 0072](docs/adr/0072-lease-fenced-task-mailbox.md), and
[ADR 0073](docs/adr/0073-governed-task-workspaces-and-pinned-git-worktrees.md).

Task Graphs also own a conservative 64 MiB materialization charge shared with
the Coordinator. Construction and deserialization establish it; Task status
and message mutations update it incrementally before commit. Current and
remaining capacity are inspectable, while the persisted v1 JSON shape stays
unchanged and the Coordinator still performs an exact final encoding check.

The same Runtime is available through an exactly versioned, typed command
protocol. Protocol v12 preserves the 2 MiB request and 16 MiB response ceilings,
byte-authoritative Thread capacity, Token Counter and Conversation Compactor
coordinates, attributed approvals, schema-3 approval continuation evidence,
schema-4 Policy-to-Tool-origin provenance, schema-5 Provider Continuation, and
schema-6 durable safe-boundary Turn steering. Steering requires the exact
active Turn, invalidates crossed provisional Model output, and never executes
a Tool call sampled from older context. When a host installs a Task
Coordinator, it also exposes bounded graph administration and
transport-authenticated worker claim, heartbeat, messaging, completion, and
failure commands. Worker ownership is derived from the local-process boundary
or exact mTLS leaf fingerprint, lease time uses the server clock, and every
mutation revalidates the current fencing token before bounded CAS retry. See
[ADR 0074](docs/adr/0074-serviceable-fenced-task-worker-protocol.md).
Response JSON is stopped during serialization, not after an unbounded temporary
allocation.
Turns are asynchronous and
independently pollable/cancellable; operation retention, prompts, identifiers,
frames, and count-plus-byte event pages all have hard bounds. A handler retains
at most 64 running plus terminal Operations by default; hosts may configure 1
through 4,096, and clients release terminal capacity explicitly with
`operation.forget`. Each background Turn task is supervised: an unexpected task
panic or premature stop becomes a content-free terminal Operation failure
instead of leaving a permanently running process-local record. State remains
authoritative across process failure, while process-local operation IDs are
intentionally disposable.

Protocol hosts also have an explicit bounded drain. Shutdown permanently
rejects new Turns, requests cooperative cancellation for every running
Operation, waits for process-local terminal status, then uses the remainder of
the same deadline to drain Runtime-owned automatic snapshot work. Stdio invokes
the default 30-second drain on EOF; the mTLS host uses a validated configurable
deadline and reports cancellations, settlements, remaining Operations, and a
separate background-work result. Uninterruptible State persistence or
maintenance is never relabeled as successful cancellation: a non-zero
remainder or false background result is an explicit recovery signal.

The `tls-host` feature serves those same JSONL frames over a mandatory-mTLS
listener. Server identity and client trust roots are operator-supplied PEM
files; TLS handshakes, connections, idle time, frames per session, and shutdown
are bounded. The client leaf certificate becomes a SHA-256 principal, and an
exact allow-list gates every protocol capability before execution;
`Initialize` advertises only granted capabilities. Subject/SAN identity,
tenant/role mapping, revocation, and hot policy reload are intentionally not
yet claimed.

Streaming model providers can emit provisional text through a kernel-owned
failure-isolated handle. Deltas and total Turn output are byte-bounded; protocol
Operations retain a bounded cursor-readable ring and report any evicted
sequence gap. Step handles close on success, failure, cancellation, or timeout,
so late provider emissions are rejected. Final model output, Verification, and
State remain authoritative.

Production model access can use the exact-versioned HTTPS JSON gateway adapter.
It is enabled by the `https-model` Cargo feature so headless Core embeddings do
not compile an unused HTTP/TLS stack.
It requires TLS 1.2+, bearer credentials resolved on demand from an opaque
`SecretReference`, disables redirects, ambient proxies, referers, cookies, and
automatic retries, reuses a bounded connection pool, and incrementally rejects
oversized responses. Secret values are non-serializable, debug-redacted, and
zeroized on drop. The built-in environment resolver reads only an explicit
reference-to-variable allow-list; secrets never enter State, Trace, protocol
frames, or model configuration. Private enterprise gateways can supply a
bounded PEM CA bundle in exclusive-root mode; this disables ambient native and
WebPKI roots instead of silently widening trust. CA bytes are validated during
configuration and omitted from debug output. Private gateways may additionally
require a bounded, non-serializable `SecretValue` containing the client
certificate chain and private key. The pooled transport parses it once, never
places it in model configuration, and must be rebuilt for identity rotation.

When a caller installs a provisional-event sink, the same adapter requests the
gateway's exact NDJSON stream mode. Text deltas are bounded provisional frames;
one final typed `ModelResponse` is mandatory and remains authoritative. Without
a sink, the adapter preserves the ordinary JSON request/response contract.

Run the local reference stdio service:

```bash
cargo run -- serve-demo
```

It accepts one JSON request per line and emits only JSON responses on stdout.
`serve-demo` uses the deterministic local model and `echo` tool; it is a
protocol reference host, not a production provider configuration.

Run the independently installable full-screen TUI:

```bash
./scripts/install-tui.sh
yh-tui --demo
```

The TUI supervises `yh serve` or `yh serve-demo`, then creates/loads Threads,
streams Turns, polls and forgets Operations, and projects paginated State,
Approval, and Task views only through Protocol v12. Input submitted during an
active Turn uses the engine's exact-ID steering command rather than a TUI-owned
execution queue. It is implemented in
[`clients/tui`](clients/tui) and can be omitted without changing the engine.

Agent Memory Hub is the first-party reference integration for governed
long-term memory. Y-Harness owns when and under which policy a memory provider
is called; Agent Memory Hub owns durable knowledge, evidence, retrieval,
context packing, feedback, and memory governance. The integration boundary is
MCP rather than Agent Memory Hub's internal Python or filesystem layout.

The current runtime exposes a versioned Memory Provider capability with
`search`, `read`, `write`, `brief`, `feedback`, `health`, and optional evidence
ingestion declarations. The first executable Context Engine path validates and
budgets complete provider context packs, passes them separately from
conversation items to the model, and records loaded or degraded memory context
in the state journal.

The first-party Agent Memory Hub adapter runs over a persistent, supervised
stdio MCP session built on the official Rust MCP SDK. It maps `search`, `read`,
`write`, `brief`, and `health`; it deliberately does not advertise `feedback`
because Agent Memory Hub does not currently expose feedback settlement through
MCP. Calls have bounded timeouts, a failed session is discarded for later
reconnection, and side-effecting calls are never retried automatically. The
child executable and working directory must be absolute, the inherited
environment is cleared, and child stderr is discarded. Construction requires a
`StdioMcpLaunchAuthority`: its default denies execution, while unrestricted
execution requires an explicit 1–4,096 concurrency limit and reports that it
does not restrict OS authority. One permit is held for the complete live
session. On macOS the same authority can instead reuse the tested Seatbelt
policy to deny network and restrict writes to canonical operator roots. Unix
children use the same ordinary process-group settlement boundary as other
external execution. Configuration debug output redacts argument/environment
values, raw JSONL frames are capped at 8 MiB, and tool pagination/catalog/result
sizes are independently bounded.

General MCP tool catalogs use the same client boundary. A host supplies an
operator-approved namespace and trust-bearing `CapabilityOrigin`; discovery
atomically registers `<namespace>.<remote-name>` adapters in `ToolRegistry`
without rewriting server names. Any invalid descriptor or collision rejects the
whole catalog. Calls then follow the ordinary model proposal, Policy/approval,
Tool execution, State evidence, cancellation, and Verification path rather than
receiving a transport-specific bypass.

Executable extensions use a replaceable Process Broker. The default broker
denies execution. The opt-in local broker clears inherited environment, never
uses a shell, bounds input/output/time and concurrency (1–4096), keeps
cooperative cancellation active through post-exit pipe settlement, and gives
termination a bounded five-second cleanup grace. On Unix each child leads a
private process group; ordinary descendants remaining in that group are killed
on completion, cancellation, timeout, or future drop. The broker truthfully
reports itself as unrestricted because process groups do not remove filesystem,
network, credential, or syscall authority and can be escaped with a new
session/group. Windows still has direct-child-only cleanup. JSON command
adapters make the same boundary usable
for external Tools and Models; Tools still pass through normal Policy and
evidence ordering. Runtime-driven external Models receive the same Turn
cancellation token through their model-step handle, so a custom broker can
cooperatively stop work instead of relying only on future-drop cleanup.

On macOS, the first concrete sandbox broker uses Seatbelt to deny network by
default and restrict writes to canonical operator-approved roots. Reads remain
available, and the broker reports that scoped guarantee rather than claiming
full filesystem isolation.

Runtime phase observations expose content-free timing and settlement classes.
Model providers may additionally report token usage, cost, and an opaque
request ID; the Runtime never invents missing accounting data. Observer errors
and panics are isolated from Turn settlement, and the reference collector has a
hard capacity with explicit drop counters.

Verify it:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --no-default-features
cargo test --all-targets --all-features
cargo run --locked --example embedded
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

YH_AMH_SERVER=/path/to/agent-memory-hub/agent_runtime_kit/mcp/server.sh \
  cargo test --test agent_memory_hub_mcp -- --ignored

YH_HTTPS_MODEL_ENDPOINT=https://gateway.example/v1/complete \
YH_HTTPS_MODEL_TOKEN=<token> \
  cargo test --all-features --test https_json_model -- --ignored
```

The provider integration test creates and removes an isolated memory root; it
does not modify the operator's real brain. The
[completion audit](docs/completion-audit.md) maps every Harness layer to code
and executable evidence. The exact remaining release blockers are maintained
in [release readiness](docs/release-readiness.md); neither document substitutes
for green CI on the release commit.
