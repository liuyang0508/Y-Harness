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
provider-side storage, accepts ordered multi-call proposals, and keeps Tool
execution, Policy, and State inside Y-Harness. Configured shell-free JSON
Models/Tools, selected MCP Tools, Agent Memory Hub, and Evaluation Graders
remain optional; see the [Chinese quick start](docs/quickstart.zh-CN.md).

Create and validate a persistent Harness service:

```bash
yh init my-harness
cd my-harness
yh doctor
yh serve
```

`yh doctor` performs a read-only preflight of every existing authoritative
SQLite store before it constructs external capabilities. It reports each store
as `ready` or `will be created`; it never creates, bootstraps, or migrates a
database. A legacy store fails with the exact backup-first migration command
that must be run after all writers are stopped.

`yh serve` is a headless Protocol v30 JSONL service over stdin/stdout. It
persists State, approvals, Task coordination, Workflow Runs, and Human
Handoffs, and durable external Effects under `.y-harness/`. A
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
optional products: CLI · TUI · GUI · LUI · VUI · IDE · API/SDK/Webhook
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

The repository currently ships two separately installable runtime products,
one optional control-plane library, and two non-runtime evidence tools:

| Package | Binary | Role |
|---|---|---|
| `y-harness` | `yh` | headless engine, service, diagnostics, migrations |
| `y-harness-tui` | `yh-tui` | full-screen terminal client over Protocol v30 |
| `y-harness-domain-pack` | — | optional tenant-fenced Domain Pack promotion and execution-binding control plane |
| `y-harness-benchmark-runner` | `yh-bench` | released-product evidence adapters outside the semantic Core |
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
provisional output stop fallback. An optional 1-millisecond to 24-hour
attempt-timeout cooldown tries non-cooling Models first on later steps while
retaining cooling Models as last-resort fallbacks. Only a Runtime-owned attempt
timeout opens cooldown; Provider errors never masquerade as that signal.
Providers may return bounded typed authentication,
authorization, rate-limit, quota, request, availability, policy, overload,
server, transport, or protocol evidence. Observability records only its
content-free class, HTTP status, and explicit retry delay; it never receives
the diagnostic or response body. An independent default-disabled policy may
retry only typed rate-limit, overload, server, and transport failures on the
same Model, at most eight times with bounded cancellable backoff. Retries share
the candidate attempt deadline, stop after provisional output, and never
replay a Turn or Tool effect. Model-produced State retains the actual successful
Model identity and origin for durable provenance, while Observability records
every invoked retry index and explicit cooldown skips.
Retry and Route calls also share an independent per-Agent-Loop-step budget:
16 by default, configurable from 1 through 144. The Runtime-managed whole-Turn
ceiling is `max_steps × max_model_attempts_per_step` (512 under both defaults);
the budget fails before an excess Provider call. It does not pretend to count
model calls hidden inside arbitrary Compactor, Verifier, Tool, or MCP
implementations.
Registry-selected identity is never re-queried from provider code. The
compatibility constructor captures, panic-isolates, validates, and freezes
`LanguageModel::id()` exactly once; a bad identity rejects execution before
`TurnStarted`.
See [ADR 0018](docs/adr/0018-model-registry-and-provenance.md) and
[ADR 0070](docs/adr/0070-explicit-bounded-model-failover.md),
[ADR 0099](docs/adr/0099-observable-model-attempt-timeout-cooldown.md), plus
[ADR 0100](docs/adr/0100-typed-model-provider-failure-evidence.md) and
[ADR 0101](docs/adr/0101-bounded-typed-model-retry-policy.md), and
[ADR 0114](docs/adr/0114-bounded-runtime-model-attempts-per-step.md).
The reference service exposes the same contract through a mutually exclusive
`models` catalog plus `model_route`; configured IDs are stable operator aliases,
each Model keeps its own environment-backed Secret reference, and `yh doctor`
rejects duplicates, unknown route entries, invalid timeouts, and invalid retry
bounds before Provider construction. See
[ADR 0087](docs/adr/0087-explicit-configured-model-catalog-and-route.md).

One reference-service process may also be bound to one exact tenant without
changing Rust code:

```json
{
  "schema_version": 1,
  "data_directory": ".y-harness",
  "authority": {
    "type": "local_process_tenant",
    "tenant_id": "tenant-a"
  },
  "model": {
    "type": "open_ai_responses",
    "id": "openai/default",
    "model": "replace-with-an-available-model-id",
    "api_key_secret_reference": "openai/default",
    "api_key_environment": "TENANT_A_OPENAI_API_KEY"
  }
}
```

This is a fixed single-tenant deployment boundary: Protocol State, Approval,
Task, Evaluation, archive access, and direct Model Secret resolution share the
same trusted tenant. It is not multi-user authentication. Enabled configured
MCP servers are rejected because the current clients share sessions; run an
unscoped service or provide a genuinely tenant-partitioned embedded client.
Existing unscoped data is never assigned to a tenant by changing this field.
See
[ADR 0125](docs/adr/0125-fixed-tenant-reference-service-authority.md).

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
[client protocol v30 specification](docs/protocol.md).
The observed lessons, rejected assumptions, immutable source snapshots, and
code/ADR traceability for Pi Agent Harness, Claude Code, Codex, Hermes Agent,
OpenCode, and Grok Build live in the
[reference architecture analysis](docs/reference-analysis.md).
That analysis keeps Grok Build, the open Agent/Harness product, distinct from
its current Grok 4.5 default Model and from supporting xAI Model, prompt, SDK,
and protocol snapshots.
The controlled same-model and product-default rules required for any
“outperforms” claim live in the
[competitive Harness benchmark](docs/competitive-benchmark.md).
Checked execution evidence currently includes one Claude Code fixed-output
probe, Codex `0.145.0` single-process and same-Thread restart CF-003 probes,
and one Y-Harness service-process explicit-recovery CF-003 probe. All are
explicitly claim-ineligible; none is presented as a cross-product comparison.
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
introduced bounded content-free evidence, and the current schema-14 writer
preserves it: compactor identity, exact coverage, source/content fingerprints,
and token/byte charges.

Populated schema-1 through schema-13 SQLite
databases require an offline, backup-first migration before the current
Runtime opens them. `yh doctor` detects this incompatibility without modifying
the database:

```bash
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v14.rollback.db
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

State schema 7 records every same-response multi-Tool decision as one atomic
ordered event. Runtime validates the whole 2–64-call batch, resolves and
authorizes every call before any effect, then executes undeclared Tools
sequentially. A Tool may explicitly guarantee `parallel_safe`; Runtime overlaps
only maximal contiguous safe runs under a configurable 1–64 ceiling and still
journals results in source order. Sequential Tools fence neighboring runs.
Batch identity and positions survive snapshots and approval restart; a crash
cannot expose a partially appended decision. Provider `parallel_tool_calls`
enables multi-call proposals but never grants parallel execution authority.
See [ADR 0098](docs/adr/0098-explicit-bounded-parallel-tool-execution.md).

State schema 8 records explicit operator-authored Thread names as durable,
clearable events. Names are bounded canonical metadata, never inferred from
conversation content. Memory and SQLite recent-Thread indexes are updated in
the same append transaction; SQLite validates that its name projection matches
the authoritative journal on open. Schema migration adds the nullable index
column and discards old disposable snapshots without rewriting history. See
[ADR 0092](docs/adr/0092-engine-owned-thread-names.md).

State schema 9 makes Thread fork/clone a first-class Engine transition rather
than a client-side replay. A caller supplies the child Thread ID as a retry
identity and may select one terminal parent Turn boundary. Memory and SQLite
create the complete child journal atomically; a failed insert leaves no child.
The child preserves immutable historical Turn/Item/correlation identities
without re-executing Tool or approval effects, omits parent names and
Checkpoints, and records direct parent sequence/version plus an exact event
prefix SHA-256. Parent and child then continue independently. See
[ADR 0093](docs/adr/0093-atomic-thread-fork-and-lineage.md).
Protocol 16 also projects this same direct lineage into content-free bounded
Thread summaries, so clients can render a recent-page branch forest without
loading every conversation history. Parents outside the page remain opaque;
this is not an entry-level mutable session DAG. See
[ADR 0094](docs/adr/0094-lineage-aware-bounded-thread-navigation.md).

State schema 10 adds portable, integrity-bound Thread archives. Export binds a
complete terminal source journal to its identity, version, last sequence, and
SHA-256. Import uses a caller-owned retry identity and atomically materializes
a new local Thread without replaying effects; historical correlation
identities and Thread-name transitions survive, recovery-only Checkpoints do
not, and every Event receives a fresh globally unique identity. Source fork
lineage is retained as evidence rather than asserted as navigable local
ancestry. The embedded API exposes the same contract as:

```bash
yh thread export <thread-id> <archive> [config]
yh thread import <archive> <target-thread-id> [config]
```

Export never overwrites its destination, import rejects altered or oversized
archives before mutation, and a running Turn is not exportable. The current
archive root is format 4 so older readers cannot silently discard schema-14
Connector evidence. Tenant-bound execution or Connector evidence can be
imported only into the same tenant; unbound histories retain the existing
explicit target-tenant rebind behavior. See
[ADR 0095](docs/adr/0095-portable-integrity-bound-thread-archives.md).

State schema 11 adds optional per-Turn reference Context without adding another
conversation or branch authority. An authorized embedding or Protocol caller
may supply up to 64 unique `TurnContextInput` blocks for RAG, branch handoff,
or workflow context. Runtime validates them before State mutation, prefixes
them as non-authoritative data, recounts them, and journals only the
authenticated actor, source/reference, double SHA-256 provenance, and
byte/token charges. The body is ephemeral and never becomes a user/assistant
Item. Direct OpenAI requests reserve provider `instructions` for verified Skill
blocks; all other Context remains user-level reference data. See
[ADR 0096](docs/adr/0096-attributed-per-turn-context.md).

State schema 12 makes optional Thread tenant ownership authoritative at
creation. Memory and SQLite stores fence every Thread/Turn read and mutation by
exact trusted tenant, and Protocol v20 applies the same boundary to retained
Operations. Forks inherit the caller tenant; archive imports rebind a new
target to the importing tenant. Legacy Threads migrate as unscoped rather than
receiving inferred ownership. Task ownership is independently bound by Task
Graph schema 2 rather than inferred from a Thread. See
[ADR 0117](docs/adr/0117-durable-thread-tenant-ownership.md).

State schema 13 adds one immutable, content-free `ExecutionBinding` Item to a
Turn. A trusted embedding host may pin an issuer, deployment name/version,
configuration digest, complete environment digest, activation revision, and
exact tenant before execution. Runtime records it once, excludes it from Model
Context, preserves it through SQLite snapshots and archives, and requires an
exact match on approval restart. Protocol clients cannot author this trusted
field. See [ADR 0122](docs/adr/0122-durable-turn-execution-binding.md).

State schema 14 adds optional Connector evidence to the same atomic
`ToolResult` event. A Connector may report bounded source, resource, revision,
observation/freshness, and idempotency claims, but those claims are not
authority by themselves. Runtime binds them to the exact registered Tool
identity and origin, trusted actor/tenant, and SHA-256 of the exact output.
State revalidates the digest and ToolCall→Policy→ToolResult provenance on
append, recovery, snapshot, and archive import. Failed results retain no
evidence, and model-visible replay strips it to prevent privileged metadata
from becoming instructions. See
[ADR 0126](docs/adr/0126-runtime-bound-connector-evidence.md).

For optional cross-Thread handoff, `ThreadHandoffRequest::prepare` computes the
longest identical Turn prefix and selects a bounded newest source-only delta.
Its format-1 digest binds the exact summarizer input and both Thread identities.
The Engine does not choose or invoke a summarizer: any host-selected provider
may return a candidate, and `to_context` converts it into the same attributed,
non-authoritative `TurnContextInput` path. The read-only
`HarnessRuntime::prepare_thread_handoff` convenience writes no State and does
not introduce Pi-style mutable entry navigation. See
[ADR 0097](docs/adr/0097-bounded-digest-bound-thread-handoff.md).

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

Approval Inbox schema 3 binds every current record to the same optional trusted
tenant as its owning Turn. Memory and SQLite access require exact tenant
equality, SQLite validates its tenant lookup projection against the record
body, and Protocol v21 exposes tenant-scoped Approval list/get/settle without a
caller-authored tenant selector. Same-tenant settlement still requires an
independent actor. Schema-2 records migrate as explicitly unscoped; ownership
is never inferred from a Thread, actor, or database path. See
[ADR 0118](docs/adr/0118-durable-approval-tenant-ownership.md).

Populated schema-1 and schema-2 Approval Inbox databases require a separate
offline migration:

```bash
cargo run -- approval-migrate \
  /absolute/path/approval.db \
  /absolute/path/approval-v1.rollback.db
```

The backup is no-clobber and SHA-256-bound to the source. Because schema 1 had
no actor evidence, pending requests are orphaned and terminal identities are
marked explicitly unattributed. Schema-2 records retain their lifecycle and
become explicitly unscoped. See the
[Approval migration runbook](docs/approval-migration.md).

Interrupted-Turn recovery keeps durable orphaning atomic without bulk-loading
every approval body: SQLite selects a bounded identity set, then validates and
updates one record at a time inside the same immediate transaction.

Task Graph schema 3 binds the complete Graph aggregate—including leases,
messages, Artifacts, and append-only governed attempt evidence—to the trusted
optional tenant. Tenant is part of the Memory/SQLite Graph key, so the same
caller-selected Graph ID may exist in different tenant namespaces. Every
protocol lifecycle operation uses exact tenant equality. Existing schema-1 or
schema-2 databases require an offline, backup-first migration; schema-1
Graphs remain explicitly unscoped and schema-2 ownership is preserved:

```bash
yh task-migrate \
  /absolute/path/tasks.db \
  /absolute/path/tasks-v2.rollback.db
```

See the [Task migration runbook](docs/task-migration.md) and
[ADR 0119](docs/adr/0119-durable-task-graph-tenant-ownership.md).

Secret Provider API 3 receives the same trusted authority used by State,
Policy, Tool, Approval, Task, and Effect execution. `SecretUseContext`
distinguishes an exact Agent Turn, Governed Effect attempt, and bounded service
operation without inventing Thread/Turn identities. Direct HTTPS gateway and
OpenAI adapters resolve credentials against Turn authority while keeping it
out of serialized Model payloads. Effect Connectors may resolve opaque
references per dispatch into non-cloneable zeroizing process buffers; values
never enter the JSON Connector envelope. Existing Secret Providers still serve
unscoped hosts; tenant-scoped requests fail closed until a Provider implements
authority-aware resolution. The embedded tenant environment Provider requires
an exact tenant/reference mapping and never falls back to a global mapping.
Current shared stdio and HTTPS MCP sessions likewise reject tenant-scoped Tool
calls before remote invocation unless an embedding host supplies a genuinely
tenant-partitioned client. See
[ADR 0120](docs/adr/0120-authority-aware-secret-resolution.md). The reference
service can now select one exact local-process tenant; its direct Model
environment mappings are assembled through the tenant provider, while enabled
shared MCP configuration fails before launch. See
[ADR 0125](docs/adr/0125-fixed-tenant-reference-service-authority.md).
Effect credential custody and its OS/child-copy non-claims are fixed by
[ADR 0139](docs/adr/0139-typed-secret-use-and-effect-credential-custody.md).

The optional `y-harness-domain-pack` crate sits above the semantic Core. Its
format-1 immutable snapshots pin exact Workflow, Skill, Tool, Policy,
Evaluation, and Schema coordinates and require a pinned Evaluation suite.
Store schema 1 provides tenant-partitioned install, terminal evaluation,
independent approval, activation, deactivation, and bounded rollback with
revision CAS in memory or SQLite. Activation requires an exact installed
inventory; execution binding rechecks the active release, complete inventory
digest, and activation revision so extension drift fails closed. Domain
behavior does not enter the Agent Loop, and the current Engine service does not
implicitly activate Packs. `AuthorizedDomainPackStore` denies every read or
transition before persistence unless a pluggable authorizer allows the exact
actor, tenant, action, and Pack. The bounded reference RBAC policy has no
wildcards or tenant fallback; an embedding service may replace it with
external IAM. The returned Domain Pack proof converts directly into the
generic Engine binding; the Engine remains unaware of Domain Pack lifecycle
rules.
See the [Domain Pack governance guide](docs/domain-pack-governance.md),
[ADR 0121](docs/adr/0121-domain-pack-control-plane.md), and
[ADR 0122](docs/adr/0122-durable-turn-execution-binding.md), and
[ADR 0124](docs/adr/0124-exact-domain-pack-role-authorization.md).

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
The reference operator binary includes this feature; headless library users
may still exclude it. Catalog discovery, authenticated private registries,
caching, and recursive dependency fetching are not implied.

The reference service also accepts explicitly listed project-local Skill
package files, signed External package files, and exact activation identities.
Each path must remain below the configuration project root, every package is
digest-verified, dependencies and required Tools resolve before startup, and
the resolved instructions enter the ordinary Context Engine. External files
are registered with configured publisher/log keys, validity, transparency, and
immutable revocation policy; they remain `External` and retain live trust
checks. `yh doctor` emits each resolved exact identity, content digest,
publisher, and transparency receipt as applicable. Unsigned project files are
operator-trusted inputs rather than third-party publisher attestations.

`yh skill install <package> [config]` validates and canonically stores a local
declarative package under the configuration project without activating it.
`install-external <signed-package> [config]` verifies configured publisher
trust before storing the complete signed envelope, while
`install-https <url> <name@version> <sha256> [config]` adds ADR 0033's exact
network pins. `list` and `verify` revalidate the bounded store and all External
trust; `remove <name@version>` refuses configured or active packages and moves
an unreferenced package into project-local recoverable trash even when its key
has since been revoked. Installation never edits activation authority: the
operator must still add the printed path to `skills.package_files` or
`skills.external_package_files`, add the exact identity to `skills.activate`,
and restart the service. See
[`y-harness.skill.example.json`](config/y-harness.skill.example.json) and
[ADRs 0085](docs/adr/0085-project-configured-declarative-skills.md) and
[0088](docs/adr/0088-explicit-mcp-activation-and-extension-locks.md), and
[0091](docs/adr/0091-governed-project-skill-lifecycle.md), plus
[0102](docs/adr/0102-governed-signed-external-skill-lifecycle.md).

Evaluation is a separate comparison layer: validated cases run through an
`EvaluationTarget` with engine-owned cancellation and deadlines. Cases and
graders execute with independently bounded concurrency and cancellation;
panics, timeouts, and ordinary failures become isolated report outcomes.
Immutable samples are shared without copying full Turn outcomes, output order
is deterministic, and exact case/grader baselines detect missing results,
errors, score regressions, required-pass failures, and same-name Grader origin
changes. Format-2 suites, baselines, and reports are self-describing; every
grade and requirement retains the registered Grader's trust-bearing origin.
Graders cannot alter live Verification or Agent Loop control. The materialized
report API admits at most 64 cases per batch; larger datasets must be chunked
by the caller. Deserialized suites, baselines, and reports are revalidated at
their execution/comparison boundaries.

The checked-in `evals/harness-smoke-suite.json` and
`evals/harness-smoke-baseline.json` form the first executable regression
baseline. `cargo run --locked -- eval-smoke` runs two isolated end-to-end
Tool-loop cases through the real reference Runtime, emits format-2 JSON
report, creates no ambient files, and exits nonzero on regression. It is a
narrow Harness contract smoke gate, not a claim about hosted-model quality or
application usefulness. See
[ADR 0067](docs/adr/0067-versioned-harness-smoke-evaluation-gate.md) and
[ADR 0069](docs/adr/0069-origin-bound-versioned-evaluation-artifacts.md).

Projects may additionally configure external JSON-command Graders. After
replacing the template's executable and any environment mapping, run:

```bash
yh eval evals/configured-example-suite.json \
  evals/configured-example-baseline.json \
  config/y-harness.eval.example.json
```

Each Grader receives one strict, immutable case/execution sample and returns a
strict score/pass/rationale object. The complete input is capped at 4 MiB; the
Evaluation Engine owns per-grade cancellation, normalization, deterministic
ordering, baseline comparison, and exit status. Configured evaluation uses
in-memory State and never opens the production State, Approval, or Task
databases. `yh serve` does not construct configured Graders. See
[the example configuration](config/y-harness.eval.example.json) and
[ADR 0107](docs/adr/0107-configured-brokered-evaluation-graders.md).

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
remaining capacity are inspectable. Schema 3 wraps the persisted Graph in an
immutable optional tenant envelope, duplicates that owner in the SQLite lookup
key, and retains append-only exact Task-attempt execution bindings across
expiry, retry, and settlement; reads validate every representation before
returning data.

An independent `WorkflowRun` aggregate coordinates one existing same-tenant
Task Graph across time without becoming a second executor. Schema 1 owns
revision-CAS commands, exact command-content idempotency, fenced signal/timer
waits, explicit retry waits, safe-boundary definition migration, and bounded
immutable transition evidence. Memory and SQLite coordinators have equivalent
contracts, and successful Run completion requires every linked Task to be
durably complete. The reference service persists Runs in `workflows.db`; its
explicitly enabled Temporal lifecycle can wake due waits without executing the
next business step. Automatic effect-safe retry, compensation planning, and
automatic Human Handoff routing remain separate future services. See
[ADR 0127](docs/adr/0127-durable-fenced-workflow-runs.md).

Human ownership transfer is a separate durable aggregate rather than an
Approval or Workflow status. Human Handoff schema 1 accepts a same-tenant
Thread or Workflow Run subject, queues by bounded priority and request time,
and uses an authenticated-owner claim with a finite lease and a unique claim
fence. Claim, renewal, release, expiration, resolution, and cancellation are
revision-CAS commands bound to the trusted actor and complete typed payload.
Memory and SQLite implementations share the same queue/cursor contract; the
reference service persists them in `human-handoffs.db`. Creating a Handoff
does not pause a Turn, route an IM conversation, wake a Workflow, authorize a
business action, or prove that `LocalProcess` is a person. An expired claim can
be returned to the queue by the same optional Temporal host lifecycle. See
[ADR 0128](docs/adr/0128-durable-lease-fenced-human-handoff.md).

Temporal Driver API 2 optionally composes Workflow, Handoff, and expired
Effect-lease advancement.
One host-driven `tick` scans at most 256 authoritative Workflow Runs and 256
Human Handoffs, then advances exact due wait/claim fences through the existing
CAS commands. Coordinator pages are revalidated before mutation. Its identity
cursor is disposable: losing it repeats a bounded part of the sweep but cannot
lose a durable timer or expiration. Core starts no interval task and owns no
second scheduler database. The embedding product still owns wall-clock source,
polling interval, shutdown, and failure observation. The reference `yh serve`
host takes that responsibility only when the strict `temporal` object is
present:

```json
{
  "temporal": {
    "poll_interval_ms": 1000,
    "scan_limit": 64
  }
}
```

Omission keeps polling disabled. Enabled service polling uses the same fixed
Authority as Protocol commands, skips missed cadence ticks, emits only bounded
health transitions to stderr, and stops before Protocol/MCP shutdown. It does
not execute Tasks, route Handoffs, or add a Protocol command. See
[ADR 0129](docs/adr/0129-host-driven-bounded-temporal-driver.md),
[ADR 0130](docs/adr/0130-optional-reference-service-temporal-lifecycle.md), and the
[`temporal_driver`](examples/temporal_driver.rs) public-API example:

```bash
cargo run --locked --example temporal_driver
```

Hosts that need to consume pending Effects may compose embedded Governed
Effect Executor API 1. Connectors register one exact capability, an explicit
operation set, trust origin, API coordinate, and target- or
Connector-enforced idempotency contract. Execution is default-deny: Policy is
evaluated before the durable Claim, duplicate Claim callers never re-enter a
Connector, and every panic, error, timeout, or cancellation after dispatch is
settled as `unknown`, never blindly retried. Each call scans a bounded page and
uses bounded concurrency; Core still starts no consumer loop and owns no
Channel, credential store, receipt verifier, or reconciliation policy. See
[ADR 0134](docs/adr/0134-host-driven-governed-effect-executor.md) and the
[`effect_executor`](examples/effect_executor.rs) public-API example:

```bash
cargo run --locked --example effect_executor
```

Unknown Effects may be converged by optional Governed Effect Reconciler API 1.
Reconciliation Connectors register one exact capability and operation set plus
an explicit authoritative read-only contract. A default-deny Policy runs before
lookup; valid `Applied` or `NotApplied` evidence settles through the existing
revision/attempt/lease CAS, while error, panic, timeout, cancellation,
`StillUnknown`, or malformed evidence leaves the Effect unchanged. Duplicate
cross-host queries are permitted only because the Connector contract forbids
external mutation; settlement remains idempotent and fenced. Core owns no
poller, query lease, credential store, or target truth model. See
[ADR 0135](docs/adr/0135-host-driven-authoritative-effect-reconciliation.md)
and the
[`effect_reconciler`](examples/effect_reconciler.rs) public-API example:

```bash
cargo run --locked --example effect_reconciler
```

Hosts may implement both Connector contracts in any language through exact
JSON Effect Connector protocol 1. `JsonCommandEffectConnector` and
`JsonCommandEffectReconciliationConnector` run one absolute executable through
the selected `ProcessBroker`: no shell, no inherited environment, bounded
input/output/time/concurrency, exact protocol validation, and typed Effect
cancellation. The adapter does not bypass either default-deny Policy or Ledger
CAS, and it does not make an unrestricted child process safe.
See [ADR 0136](docs/adr/0136-versioned-brokered-json-effect-connectors.md) and
the self-hosting public example:

```bash
cargo run --locked --example json_effect_connector
```

The reference `yh serve` host can optionally own resident Effect consumption.
Execution and reconciliation are configured independently under
`effect_consumer`; each requires a non-empty exact allowlist and a separate
Connector registry. Registration does not grant authority, and omission starts
no background work. Commands are preflighted by `yh doctor`, missed ticks are
skipped, unavailable pages use bounded process-local backoff, diagnostics are
content-free health transitions, and both loops stop before Protocol/MCP
shutdown. Every Effect command requires a lowercase `command_sha256`; the
selected Broker verifies it at assembly and again before every dispatch within
the existing cancellation and timeout budget. Drift prevents child entry, but
this remains non-atomic command-file measurement rather than a sandbox or
transitive dependency lock. The Ledger remains the only durable recovery
authority, so restart may repeat safe scans but cannot replay a terminal
Effect:

Execution may additionally install dispatch-governor API 1. Its independent
schema-1 `effect-governance.db` atomically enforces a fixed-window limit,
consecutive-failure circuit, monotonic circuit epoch, and one leased half-open
probe for the trusted `(tenant, capability, operation, policy_id)` lane. It
runs after durable Claim and before Connector entry; denials therefore settle
as safe retryable `NotApplied` results. It never infers a business target from
Effect input or a failure from `reason_code`, and it does not gate authoritative
read-only reconciliation. See
[ADR 0141](docs/adr/0141-durable-effect-dispatch-governance.md).

```json
{
  "effect_consumer": {
    "execution": {
      "poll_interval_ms": 1000,
      "failure_backoff_ms": 5000,
      "executor": {
        "scan_limit": 64,
        "max_concurrency": 8,
        "policy_timeout_ms": 5000,
        "governor_timeout_ms": 5000,
        "governor_retry_after_ms": 5000,
        "execution_timeout_ms": 240000,
        "settlement_reserve_ms": 30000,
        "lease_duration_ms": 300000
      },
      "governor": {
        "policy_id": "notification-command-v1",
        "max_dispatches_per_window": 1000,
        "window_ms": 60000,
        "failure_threshold": 5,
        "open_duration_ms": 30000,
        "probe_lease_ms": 10000,
        "admission_retention_ms": 604800000
      },
      "allow": [
        {"capability": "notification.command", "operation": "send"}
      ],
      "connectors": [
        {
          "origin_id": "deployment/notification-command-execution",
          "capability": "notification.command",
          "operations": ["send"],
          "idempotency": "target_enforced",
          "process": {
            "command": "/absolute/path/to/effect-connector",
            "command_sha256": "0000000000000000000000000000000000000000000000000000000000000000",
            "args": ["--execute"],
            "current_directory": ".",
            "secret_environment": {
              "TARGET_API_TOKEN": {
                "reference": "effect/notification-primary",
                "host_environment": "NOTIFICATION_API_TOKEN"
              }
            },
            "timeout_ms": 240000,
            "max_output_bytes": 65536,
            "launch": {"type": "unrestricted", "max_concurrency": 8}
          }
        }
      ]
    }
  }
}
```

Reconciliation uses the same lifecycle fields plus `reconciler`,
`contract: "authoritative_read_only"`, and its own process. See the complete
[Effect consumer example](config/y-harness.effect-consumer.example.json) and
[ADR 0137](docs/adr/0137-optional-reference-service-effect-consumer.md). Replace
the placeholder digest with the exact command-file SHA-256. Integrity semantics
and non-claims are fixed by
[ADR 0138](docs/adr/0138-dispatch-time-effect-command-digest-locks.md).
`secret_environment` is optional, contains references and host variable names
only, is probed by `yh doctor`, and is resolved again under the exact Effect
authority before every dispatch. It is not serialized to the Connector and is
accepted only with dispatch SHA-256 integrity. The adapter preflights that
digest before Provider resolution; the Broker measures again before child
entry. A racing change may still cause issuance before the second measurement
rejects it, so this is not an atomic executable-to-`exec` claim. See
[ADR 0139](docs/adr/0139-typed-secret-use-and-effect-credential-custody.md) and
[ADR 0140](docs/adr/0140-secret-gated-effect-command-integrity-preflight.md).
Dispatch-governor semantics and their cross-store non-claims are fixed by
[ADR 0141](docs/adr/0141-durable-effect-dispatch-governance.md).

The same Runtime is available through an exactly versioned, typed command
protocol. Protocol v30 preserves Protocol v29's 2 MiB request and 16 MiB
response ceilings, byte-authoritative Thread capacity, Token Counter and
Conversation Compactor
coordinates, attributed approvals, schema-3 approval continuation evidence,
schema-4 Policy-to-Tool-origin provenance, schema-5 Provider Continuation, and
schema-6 durable safe-boundary Turn steering, schema-7 atomic ordered
Tool-call batches, schema-8 Engine-owned Thread names, and schema-9 atomic
Thread forks with immutable direct lineage, schema-10 integrity-bound Thread
import provenance, schema-11 attributed invocation Context, and schema-12
durable Thread tenant ownership. Protocol 16
added lineage to bounded content-free Thread summaries without changing State
schema; Protocol 17 admitted import provenance, and Protocol 18 adds optional
bounded `start_turn.context`. Protocol 19 adds explicit permissioned recovery
of one exact abandoned Turn without automatic replay. Protocol 20 adds exact
Thread and Operation tenant fencing. Protocol 21 adds schema-3 durable
Approval tenant ownership and tenant-scoped Approval capabilities. Protocol
22 adds schema-2 durable Task Graph ownership, tenant-partitioned Graph
identity, and the complete tenant-scoped worker lifecycle. Protocol 23 adds
Secret Provider API 2 with trusted-authority credential resolution and
fail-closed shared MCP session fencing. Protocol 24 advertises State and
snapshot schema 13 for durable Turn execution bindings; Thread archive format
3 advances independently. Remote protocol callers still cannot author
bindings. Protocol 25 advertises Task Graph schema 3; a trusted embedded
Orchestrator commits exact Task/lease/attempt/worker binding evidence before
Workspace or executor entry, retains it after retry and settlement, and
rejects an unbound retry after governance begins. Protocol workers cannot
author that evidence or take over a bound Task. Protocol 26 advertises State
and snapshot schema 14; Thread archive format 4 advances independently for
Runtime-bound Connector evidence. Protocol clients can observe the durable
record but cannot author or elevate a Tool into a trusted Connector. Protocol
27 added an optional schema-1 durable Workflow Run surface above one
same-tenant Task Graph. Revision-CAS commands, stable command digests, fenced
signal/timer waits, explicit retry waits, and safe-boundary definition
migration remain separate from Task lease/effect authority. Protocol 28 adds
an optional schema-1 Human Handoff surface with command-specific permissions,
actor-bound idempotency, exact claim ownership, and bounded queue/transition
paging. Protocol 29 adds an optional schema-1 Effect Ledger with
tenant-scoped idempotency uniqueness, finite worker leases, fail-closed
unknown outcomes, explicit reconciliation, and content-free external receipts.
Protocol 30 advertises Secret Provider API 3 and its typed Turn, Effect, and
service use contexts; it adds no Secret-bearing client command or durable
schema.
Temporal Driver API 2 may convert expired exact leases to `unknown`; it never
requeues or executes the effect. Steering requires the exact active Turn,
invalidates crossed provisional Model output, and never executes
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
`Initialize` advertises only granted capabilities. Subject/SAN-to-role mapping,
revocation, and hot policy reload are intentionally not yet claimed. A host
authorizer may resolve a transport principal to a validated per-Turn
actor/tenant Authority Context. That tenant now fences durable Thread,
Operation, Approval, Task, and optional Domain Pack control-plane records and
reaches Memory, Policy, Tool, Model Secret resolution, and MCP admission. Task
Artifact reference metadata inherits its owning Graph's tenant fence, but the
external blob named by its URI has no Y-Harness storage or authorization
layer. The reference service supports a fixed one-process/one-tenant authority
and exact direct-Model environment mapping. Multi-principal tenant routing,
general Secret-manager integration, quotas, retention, tenant-partitioned MCP
sessions, and external Artifact storage remain outside the current claim.

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
lists and resumes the latest authoritative Threads, streams Turns, polls and
forgets Operations, and projects paginated State, Approval, and Task views only
through Protocol v30. The Sessions panel shows direct fork ancestry from
content-free Engine summaries. `/name [title]` changes or clears Engine-owned
Thread metadata; `/fork [terminal-turn-id]` creates and switches to an
independent child through the same typed protocol. Input submitted during an active Turn uses the engine's
exact-ID steering command rather than a TUI-owned execution queue. It is implemented in
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

MCP Tool calls observe a per-call Runtime stop signal for both explicit
cancellation and Turn deadline expiry. MCP adapters reserve a
registration-time-frozen cleanup grace capped at ten seconds. The built-in
stdio and HTTPS transports remove and boundedly close the affected persistent
session before the Turn becomes terminal; cleanup failure is reported as a
failure, not successful cancellation. The stdio boundary also waits for its
bounded child/process-group settlement. Cancellation never claims rollback and
never triggers an automatic Tool retry; only a later explicit call may create a
new session.

The optional `https-mcp` feature adds an authenticated remote transport for
the bounded stateless JSON-response subset of MCP Streamable HTTP. It requires
an exact HTTPS URL and environment-backed Secret reference, disables redirects,
ambient proxies, automatic HTTP/Tool retry, SSE reconnect, and expired-session
request replay, and independently bounds requests, responses, session IDs, and
timeouts. A project-contained exclusive CA bundle is supported. SSE, OAuth,
arbitrary headers, and stateful remote sessions are explicitly not claimed.
The operator install and release binary include this feature; library hosts may
exclude it. See
[ADR 0103](docs/adr/0103-bounded-authenticated-https-mcp-json-transport.md) and
[ADR 0115](docs/adr/0115-bounded-mcp-tool-cancellation-settlement.md).

Reference-service stdio and `https_mcp_servers` entries are explicitly
activatable. A disabled entry
starts no process, discovers no catalog, grants no Tool Policy, and cannot
satisfy a Memory dependency. Enabled stdio entries may pin the exact command
file with a lowercase SHA-256 digest; `yh doctor` reports enabled/configured and
locked/enabled-stdio counts. Remote IDs share the same collision domain and
exact Tool allow-list, but command locks do not apply to URLs. This is startup
drift detection for that file, not a sandbox, dependency lock, or atomic
executable measurement. See
[ADR 0088](docs/adr/0088-explicit-mcp-activation-and-extension-locks.md).

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
adapters make the same boundary usable for external Tools, Models,
Conversation Compactors, Verifiers, and Evaluation Graders; Tools still pass
through normal Policy and evidence ordering. JSON Tools default to sequential
execution; an operator may declare `batch_execution: "parallel_safe"` only when
the Tool is semantically safe against every other eligible same-response call, while
`max_parallel_tool_calls` bounds the Runtime to 1–64 concurrent calls.
Runtime-driven external Models receive the same Turn
cancellation token through their model-step handle, so a custom broker can
cooperatively stop work instead of relying only on future-drop cleanup.
Embedding hosts may wrap a one-shot Broker in `DigestLockedProcessBroker`.
Its descriptor freezes `dispatch_sha256` evidence and it remeasures one exact
regular command file before each dispatch under the same cancellation and total
timeout. This detects resident-service drift; it is explicitly not an atomic
OS exec measurement and does not cover interpreters, arguments, libraries, or
same-authority filesystem races.

The reference service exposes the existing Model adapter as
`model.type = "json_command"` in either the compatible single-Model form or the
explicit routed catalog. The executable receives one bounded `ModelRequest` on
stdin. The backward-compatible `output_v1` default returns one validated
`ModelOutput`. Explicit `protocol: "settlement_v1"` instead returns one strict
completed/failed envelope: completed results may preserve Provider-reported
usage, exact cost, settled Model, request identity, and continuation; failures
carry bounded typed facts that enter the existing Runtime retry/failover path.
The registered Model identity and External provenance remain authoritative.
Both protocols reuse the same explicit launch, environment, timeout, output,
cancellation, and cleanup boundaries as JSON Tools. Neither protocol claims
provisional streaming. See
[the example configuration](config/y-harness.command-model.example.json) and
[ADR 0104](docs/adr/0104-configured-brokered-json-command-models.md), plus
[ADR 0108](docs/adr/0108-versioned-json-command-model-settlement.md).

The reference service can also configure the Context Engine's existing
semantic compaction port under `conversation.compaction`. A shell-free JSON
command receives one bounded, cancellation-free
`JsonConversationCompactionRequest` on stdin and returns exactly
`{"summary":"..."}` on stdout; the Turn cancellation signal stays inside the
engine and is passed separately to the Process Broker. The engine adds the
non-authoritative marker, validates independent token/byte ceilings, preserves
all original Items, and journals only content-free coverage and digest
evidence. The complete command envelope is capped at 1 MiB. A configured
failure fails the Turn rather than silently claiming complete history. See
[the example configuration](config/y-harness.command-compactor.example.json)
and
[ADR 0105](docs/adr/0105-configured-brokered-conversation-compaction.md).

Completion gates are independently configurable through `verifiers`. Each
shell-free JSON command receives one immutable candidate snapshot on stdin and
returns a strict `passed` or `failed` outcome. Retryable failures send the Agent
Loop back to the Model; hard failures fail the Turn; every settlement is
journaled through the existing `VerificationResult` shape. The exact Turn
cancellation token stays in-process and reaches the selected Process Broker
separately. Verifiers never gain Tool, Policy, State, or completion authority:
the Runtime validates their bounded outcome and owns final settlement. See
[the example configuration](config/y-harness.verifier.example.json) and
[ADR 0106](docs/adr/0106-configured-brokered-verification.md).

Evaluation Graders use the same explicit process authority without becoming
Runtime capabilities. `evaluation.graders` is loaded only by `doctor` and
`eval`; each process receives a cancellation-free sample on stdin while its
exact per-grade token reaches the Process Broker separately under the
`evaluation` phase. Unknown response fields and unbounded/deep samples fail
closed. The Evaluation Engine—not the process—owns score validation, report
provenance, baseline comparison, and command success. See
[ADR 0107](docs/adr/0107-configured-brokered-evaluation-graders.md).

On macOS, the first concrete sandbox broker uses Seatbelt to deny network by
default and restrict writes to canonical operator-approved roots. Reads remain
available, and the broker reports that scoped guarantee rather than claiming
full filesystem isolation.

Runtime phase observations expose content-free timing and settlement classes.
Model providers may additionally report token usage, exact integer cost at ten
billion USD ticks per dollar, the Provider-reported settled Model, and an
opaque request ID; unavailable, partial, or inexact evidence stays absent. The
registered Model remains routing and continuation authority, and the Runtime
never invents missing accounting data. Observer errors and panics are isolated
from Turn settlement, and the reference collector has a hard capacity with
explicit drop counters.

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
