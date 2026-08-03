# Y-Harness Engineering v0.1.0

Y-Harness v0.1.0 is the first general-purpose, headless Agent Harness baseline
built around:

```text
Agent = LLM × Harness = X × Y
```

It ships an embeddable Rust Core/Runtime, Protocol v37 service, thin engine CLI,
an independently installable full-screen TUI, an optional Domain Pack
control-plane library, durable SQLite State/Approval/Task coordination,
governed extension contracts, evaluation gates, and executable examples.
The persistent service can assemble an optional direct OpenAI Responses
Provider, brokered shell-free JSON-command Models and Tools, exact-selected MCP
Tools, and Agent Memory Hub Context without moving Policy or State authority
into a client or provider. A configured command Model accepts one bounded
`ModelRequest`; compatible `output_v1` returns one validated `ModelOutput`,
while explicit strict `settlement_v1` may preserve Provider usage, settled
Model/request identity, continuation, or typed failure facts. Both keep
External provenance and participate in the same catalog/route without
claiming provisional streaming. Schema 5 adds bounded, origin-bound Provider Continuation so
stateless OpenAI reasoning Tool loops can replay encrypted reasoning state
without transferring Tool authority to the vendor.
Schema 6 adds durable, actor-attributed, exact-Turn steering with crossed
response invalidation and safe Tool boundaries.
Schema 7 adds atomic ordered same-response Tool-call batches, whole-batch
Policy/Approval before effects, restart-safe pre-effect approval continuation,
and Harness-owned scheduling. Tools are sequential by default; explicitly
`ParallelSafe` contiguous runs use bounded concurrency with sequential fences
and source-ordered durable settlement.
Schema 8 adds bounded, explicit, clearable Engine-owned Thread names and a
validated same-transaction recent-list projection.
Schema 9 adds atomic terminal-boundary Thread fork/clone with caller-owned
child retry identity, direct immutable lineage, exact parent-prefix SHA-256,
SQLite rollback on any partial failure, and no replay of historical effects.
Schema 10 adds bounded portable Thread archives, terminal-only export, exact
source-event SHA-256, atomic caller-identified import, fresh Event identities,
and immutable source provenance without replaying effects.
Schema 11 adds bounded per-Turn reference Context with authenticated caller
attribution, source/body SHA-256 provenance, content-free State evidence, and
no synthetic conversation or branch authority. Direct OpenAI maps only
digest-pinned Skill Context to provider instructions; other Context remains
user-level reference data.
Schema 12 adds durable optional Thread tenant ownership with validated
Memory/SQLite projections and exact State access fencing. Schema-1 through
schema-11 Threads migrate as unscoped rather than receiving inferred
ownership. Forks inherit the trusted tenant; format-3 archive imports rebind
unbound history to the importing tenant.
Schema 13 adds a single immutable, content-free per-Turn execution binding
with exact configuration/environment digests, revision, actor, and tenant.
Runtime excludes it from Model Context, validates it across SQLite
snapshot/reopen, and requires an exact match on approval restart. Thread
archive format 3 preserves this evidence and refuses cross-tenant rebinding of
bound history.
Protocol 19 adds permissioned recovery of one exact abandoned Turn without
automatic replay. The embedded Runtime also carries a validated per-Turn
Authority Context from a trusted host or protocol authorizer into Memory scope,
Policy, and Tool execution. Protocol 20 applies the same trusted tenant to
Thread, Turn, recovery, handoff, archive, and retained Operation access.
Approval Inbox schema 3 and Protocol 21 additionally bind Approval records,
discovery, and settlement to that tenant while preserving independent-actor
settlement. Task Graph schema 2 and Protocol 22 bind the complete Graph,
worker lease, and mailbox lifecycle to that same trusted tenant, while
partitioning caller-selected Graph IDs by tenant. Historical schema-1 Graphs
migrate as explicitly unscoped.
Secret Provider API 2 and Protocol 23 carry trusted Turn authority into direct
Model credential resolution without serializing actor or tenant data to the
Provider. Exact tenant/reference environment mappings have no global fallback;
legacy Secret Providers and shared MCP sessions fail closed for tenant-scoped
operations. An additive fixed local-process tenant configuration now binds one
reference-service deployment's Protocol State/Approval/Task access,
Evaluation, archives, and direct Model environment Secrets to the same exact
tenant. Enabled shared MCP configuration fails before launch. This does not
claim multi-principal tenant routing, a general Secret manager, or
tenant-partitioned MCP sessions.
Protocol 24 advertises State/snapshot schema 13; Thread archive format 3
advances independently, and remote clients cannot author trusted execution
bindings.
Task Graph schema 3 and Protocol 25 add append-only exact-attempt execution
bindings for trusted embedded Orchestrators. Evidence is committed before
Workspace or executor entry, survives retry and settlement, and is
tenant-exact. Once a Task enters bound mode, an unbound retry fails closed.
Schema-1/schema-2 stores migrate backup-first; schema-2 tenant ownership is
preserved and old rows cannot claim new evidence.
Task Graph schema 4 and Protocol 31 add bounded canonical execution-capability
requirements to immutable Task definitions. A trusted embedded Orchestrator
claims only when its exact capability set contains every requirement. Legacy
claim paths and protocol Workers use an empty capability set, so they cannot
bypass specialized Tasks or self-assert authority. Schema-1/schema-2/schema-3
stores migrate backup-first; historical ownership and attempt bindings are
preserved, requirements remain empty without inference, and old-schema
capability smuggling fails closed.
State/snapshot schema 14 and Protocol 26 add optional Runtime-bound Connector
evidence. Evidence-aware in-process Tools report only bounded source claims;
Runtime supplies registered Tool/origin, trusted actor/tenant, and the exact
output SHA-256. State stores output and evidence atomically, revalidates the
ToolCall→Policy→ToolResult chain, strips evidence from Model Context, and
rejects failed-result, digest, origin, authority, and cross-tenant archive
tampering. Ordinary Tool, MCP, and JSON-command adapters retain their
non-authoritative compatibility path. Thread archive format advances to 4.
Workflow Run schema 1 and Protocol 27 add an optional durable control plane
above one existing same-tenant Task Graph. Stable content-bound command
identity, revision CAS, fenced signal/timer waits, explicit retry waits, and
safe-boundary definition migration survive restart without moving Task lease,
effect, or worker authority into Workflow. Successful completion requires
every linked Task to be complete. The reference service persists this
independent aggregate in `workflows.db`.
Human Handoff schema 1 and Protocol 28 add an optional ownership-transfer
surface over an existing same-tenant Thread or Workflow Run. A bounded stable
queue leads to a finite authenticated-owner claim with a unique fence.
Actor-and-content-bound command identities, revision CAS, exact expiry,
immutable transitions, tenant-partitioned Memory/SQLite coordination, and
projection validation survive restart. The reference service persists the
aggregate in `human-handoffs.db`. The aggregate itself does not implicitly
approve a decision, pause a Turn, route a channel, wake a Workflow, execute
business actions, own a polling loop, or prove that `LocalProcess` is a human.
Effect Ledger schema 1 and Protocol 29 add an independent durable external
side-effect intent boundary. One immutable tenant/capability/operation/
idempotency coordinate is committed before execution. A finite worker lease
owns one exact attempt; expiry becomes `unknown` and cannot be retried without
authoritative reconciliation. Content-free receipts, actor/content-bound
commands, revision CAS, Memory/SQLite parity, bounded paging, and restart
recovery live in `effects.db`. Temporal Driver API 3 may expire the exact lease
to `unknown`, but it never performs the external operation.
Embedded Governed Effect Executor API 1 optionally consumes those pending
intents through exact-versioned registered Connectors. It freezes explicit
operation and idempotency contracts, denies execution until Policy permits a
pre-Claim request, bounds concurrent Connector calls and deadlines, skips
duplicate Claim callers, and converts every post-dispatch failure into durable
uncertainty. It is host-driven and adds no Protocol command, polling thread,
credential store, or receipt/reconciliation authority.
Embedded Governed Effect Reconciler API 1 optionally converges `unknown`
attempts through exact registered authoritative read-only lookup Connectors.
It runs default-deny Policy before lookup, bounds concurrency and deadlines,
isolates failure, validates evidence, and settles only through the existing
revision/attempt/lease CAS. Duplicate lookup is permitted because the contract
forbids external mutation; opaque failure and `StillUnknown` leave durable
state unchanged. It adds no Effect schema, Protocol command, query lease,
poller, credential store, or target truth model.
JSON Effect Connector protocol 1 adds strict versioned execution and
reconciliation process envelopes plus shell-free `ProcessBroker` adapters.
Requests retain Authority and immutable attempt/lease evidence while keeping
cancellation in-process. Typed `ExecutionPhase::Effect`, bounded I/O and
deadlines, cleared environment, exact response decoding, and diagnostic
redaction preserve the existing Executor/Reconciler failure boundary. This
adds no durable schema, client Protocol command, service configuration, or
resident consumer.
The optional reference-service Effect consumer supplies that resident
lifecycle outside Core. Execution and reconciliation remain separate,
default-deny, bounded loops over the authoritative Ledger. Every Connector
uses a per-dispatch command-file SHA-256 drift lock. Optional
`secret_environment` entries contain only opaque references and host variable
names; Secret Provider API 3 resolves them under the exact typed Effect
context and trusted Authority immediately before each dispatch. Such
Connectors require dispatch SHA-256 evidence: the adapter preflights before
Provider resolution and the Broker measures again before child entry under one
deadline. Protocol 30 advertises API 3 without adding a Secret-bearing command.
Values do not enter Effect input, JSON Connector envelopes, State, or
diagnostics; racing post-preflight issuance, atomic executable binding, and
operating-system/child-process copies remain outside the claim.
Embedded Temporal Driver API 3 composes optional Workflow, Handoff, Effect,
and State Engines behind one bounded host-driven tick. It uses trusted host
time and authority, tenant-local 1–256-record keyset scans per source,
disposable cursors, deterministic fence command identities, and existing CAS
transitions to settle each attempt as applied, duplicate, fenced, or failed.
Agent Loop maintenance reads only its due projection and one or two exact
lifecycle events. It starts no background task and adds no scheduler database.
Reference-service config schema 1 additively accepts an optional `temporal`
policy. Omission remains disabled; opt-in polling uses the fixed service
Authority, skip-missed cadence, bounded health diagnostics, and
Temporal-before-Protocol-before-MCP shutdown.
The reference service now also converts Unix SIGTERM/SIGINT and Windows Ctrl-C
into its existing one-way drain. Public cancellation-aware JSONL helpers stop
only between frames, and a bounded detached stdin bridge prevents an open pipe
from pinning Tokio's blocking pool during process shutdown (ADR 0152).
The optional `y-harness-domain-pack` crate remains above Core and outside
Protocol v36. Format/store schema 1 pins immutable component snapshots and a
mandatory Evaluation suite, records terminal evaluation and independent
approval, tenant-fences release/activation identity, and supports SQLite CAS,
bounded rollback, and execution-time inventory/revision binding. Its
fail-closed store adapter authorizes every read and transition before
persistence; the bounded reference RBAC policy matches exact actor, tenant,
and action without wildcard or fallback, while external authorizers remain
pluggable. Its proof converts into the generic Engine Turn binding. The
embedding control service remains responsible for authentication and
component locking; no business workflow is added to the Agent Loop.
An additive format-1 `ThreadHandoffRequest` prepares a bounded source-only
whole-Turn delta against another terminal Thread and binds the exact summarizer
input to both Thread identities. Summary synthesis remains host-selected; the
candidate re-enters the governed per-Turn Context path and preparation writes
no State.
The service may use the compatible single-Model form or a strict Model catalog
with an explicit 1–16 identity ordered route and per-Model environment Secret
mapping; the forms are never combined or inferred. Multi-Model routes may
enable a process-local timeout-only cooldown: ready candidates run first,
cooling candidates remain last-resort fallbacks, and uncalled candidates emit
content-free `Skipped` observations.
Model Providers may additionally return bounded typed failure evidence.
Authentication, authorization, rate limit, quota, request, availability,
content policy, overload, server, transport, and protocol classes remain
separate from recovery authority; Trace retains only class, HTTP status, and
explicit retry delay. An independent default-disabled route policy can retry
only rate-limit, overload, server, and transport failures on the same Model,
using 1–8 additional calls, shared candidate deadlines, cancellable bounded
backoff, provisional-output fencing, and content-free retry indices. Legacy
`Model(String)` implementations remain supported and are never retried by this
policy.
Configured stdio MCP entries now support explicit disablement and optional
SHA-256 command-file drift detection. `yh doctor` reports MCP activation/lock
counts and every resolved exact Skill identity/content digest.
An optional authenticated HTTPS MCP client now serves the stateless
JSON-response subset through the same `McpClient`, Tool, Policy, and Memory
boundaries. Exact HTTPS endpoints, environment Secrets, response/time limits,
exclusive CAs, disabled redirects/proxies/retries, and private-TLS service
assembly are tested. SSE, OAuth, arbitrary headers, stateful remote sessions,
and automatic Tool replay remain unsupported.
Signed third-party Skills now have a complete project lifecycle:
`install-external` and exact-pinned `install-https` verify configured publisher
validity/revocation and optional required transparency before create-new
storage; `external_package_files` preserves the signed envelope and External
origin; startup and Context keep live trust checks; doctor reports publisher
and log provenance; revoked packages remain recoverably removable after
deactivation.
Digest-pinned format-1 Catalog commands now search, recursively acquire, and
explicitly upgrade exact signed Skill dependency closures while retaining
immutable Catalog bytes and source receipts. Named private Registries add an
exact Catalog endpoint, package-origin allowlist, request-scoped Bearer Secret
resolution, and optional exclusive project-pinned CA. The real private-TLS CLI
test proves both requests are authenticated, installation remains inactive,
and the credential does not enter the resulting project tree. Protocol v33
adds only credential-free Registry metadata to Runtime Catalog; Registry
federation, mirrors, OAuth, npm/git sources, and executable extensions remain
outside this release.
Protocol v34 adds an independently authorized, content-free service admission
projection. It reports `ready`, exact Operation `at_capacity`, or one-way
`draining` plus bounded running/retained counts from the same Handler state
that gates new Turns. A successful response proves Protocol-process liveness;
it does not claim external dependency health.
Protocol v35 adds bounded Model Tool Trace attempt events and credential-free
MCP registration projections. They improve client diagnostics without moving
prompts, Tool inputs/results, schema bodies, credentials, or execution authority
out of the Runtime.
State/snapshot schema 15 and Protocol v36 add deterministic format-1
`CompletionReceipt` settlement. A Model text message is only a candidate;
Runtime records its exact Model-request digest, runs the frozen candidate-bound
Verifier manifest, fences pending Steering, and asks State to validate and
atomically commit `TurnCompleted { turn_id, receipt }` against the exact
running projection. The receipt binds the candidate and evidence prefix,
Model route, Tool Capability View, Verifier identities/origins/contracts and
outcomes, Runtime governance, trusted authority, and an optional
`ExecutionBinding`. A successful terminal Operation exposes only the SHA-256
of that durable receipt for correlation.

Receipt-free completed Turns from schema 1 through 14 remain readable as
`legacy/unverified`; migration does not synthesize proof. Thread archive format
5 preserves the original receipt bytes. Fork/import lineage proves that the
target inherited source history, while `source_thread_id` continues to identify
the Thread in which completion was actually performed. It does not claim the
target reran completion gates. Format 1 requires Artifact, Effect, and business
delivery to be explicitly `not_required`; it does not prove cross-aggregate
Artifact contents, Effect truth, channel delivery, or post-terminal derived
jobs. See [ADR 0157](adr/0157-generation-bound-completion-receipt.md).
State/snapshot schema 16, Thread archive format 6, and Protocol v37 add the
first durable Agent Loop wait slice. An opted-in single non-batch Tool `ask`
persists a complete authority/generation/active-time envelope and releases its
worker. Exact Approval evidence advances `Waiting` to `Ready`; one State-CAS
claim advances to `Executing`. Denial fails the Turn atomically without a
claim, while exact cancellation and deterministic timeout share the terminal
State boundary. Independent wait-projection schema 1 transactionally indexes
expiring waits and immediately due accepted denials; Temporal API 3 provides
bounded host-driven convergence. Protocol and the TUI support bounded
discovery, resume, cancel, and restart recovery. Batch release, `HumanInput`,
Inbox repair outbox/tombstone, finite leases, `NeedsReconciliation`, frozen
Context capsules, and cross-process resume receipts remain out of scope. See
[ADR 0158](adr/0158-durable-agent-loop-waiting-and-resume.md) and
[ADR 0159](adr/0159-bounded-indexed-durable-agent-loop-wait-expiry.md).
The explicit backup-first State migration now accepts schema 1 through 15 and
projection-less schema 16, retains every historical event and schema label,
preserves current snapshots during projection-only migration, never infers a
wait from legacy Running state, and advances writers to State/snapshot schema
16 plus wait-projection schema 1.
The embedded Runtime also adds ADR 0156's bounded Progress Governor. It rebuilds
from existing Turn Items, ignores fresh call and batch identities, detects exact
failure-bearing cycles of period one through four, and by default stops after
five repetitions. Pending durable Steering is applied under the Turn-control
lock before the stop verdict. This adds `ProgressPolicy` and
`HarnessError::NoProgress`, but no State, archive, service-config, or Protocol
shape.
An optional `http-probe` feature now exposes bounded Kubernetes-style
`/livez` and `/readyz` routes from that same in-process status. The reference
service accepts a strict opt-in configuration, while zero-default binaries
reject it explicitly. The adapter has no durable state, dependency-health
claim, TLS, or general Agent HTTP API.
Stores that implement the bounded recent-Thread index advertise
`thread.list`; the TUI uses it to list and resume the latest 64 authoritative
Threads without opening Engine storage. Protocol 16 includes optional direct
lineage in those content-free summaries, and the Sessions panel renders the
parent identity/version without projecting full histories.
The TUI exposes `/name [title]` and `/fork [terminal-turn-id]` through the
protocol; it keeps no title or branch store. If independently installed Engine
and TUI binaries drift, startup now reports both Protocol coordinates and the
same-checkout reinstall commands instead of two unexplained numbers. The TUI
also gives empty Threads an explicit first-Turn path, keeps short transcripts
near the Composer, renders exact/sub-percent State pressure without false
`0%`, labels Header capacity as per-Thread events, labels Activity identifiers
as global event sequences, and marks durable `local/demo` decisions as
deterministic/no-network.
Header identity is explicitly `LAST MODEL`: it is derived only from Protocol
State Items and never predicts the next Engine-owned Route. The client still
does not parse Engine configuration.
The independent benchmark runner emits exact non-claim external-run formats
for released Claude Code, Codex, Grok Build, Pi, OpenCode, and Hermes Agent
CLIs. Real released Claude Code `2.1.143`, Codex `0.145.0`, Grok Build
`0.2.112`, Pi `0.82.1`, OpenCode `1.18.5`, and Hermes `0.19.0` fixed-output
records preserve formats 1–6 with deterministic loopback Providers and
explicit unsupported controls. Claude's sidecar records one Provider probe,
one Model request, exact settled Model/usage, projected product cost, on-wire
thinking, product prompt/date blocks, and isolated config writes. Grok's
request sidecar records a separate session-title call outside the one returned
main-agent Turn.
Codex's sidecar records one request, six visible built-in Tools, absent
automatic Skill/App instructions, and unavailable settled Provider/Model
identity. These are adapter conformance records, not comparisons. Format 5 uses
OpenCode's source-tested run JSONL surface. Format 6 uses Hermes one-shot stdout
plus its strict bounded usage sidecar, an isolated bare home, static empty Tool
set, exact Provider/Model identity, and estimated-cost preservation without
relabeling it as actual cost. Hermes's argv prompt exposure and missing
system-role/workspace-rule parity remain explicit limitations. Dedicated
format 7 drives released Codex `0.145.0`
through a source-pinned deferred Tool-search and MCP crash-after-effect path,
then validates the durable fixture journal independently. Its checked CF-003
record is non-comparative and claim-ineligible. Format 8 additionally cancels
the product after the held effect, resumes the exact persisted Thread,
requires Codex's synthetic `aborted` Tool output, and proves that the durable
effect remains singular. It records Codex's detached MCP child release and
new-Turn recovery instead of claiming descendant cleanup or in-place Turn
continuation. Binary-to-source equivalence and same-Model parity remain
unproven. Format 9 drives real `yh serve` processes through a spec-bound
JSON-command Model, stdio MCP, SQLite restart, permissioned exact-Turn
recovery, and a Tool-free audit Turn. State rechecks the expected Turn at the
optimistic-commit boundary, while the report explicitly declines descendant
cleanup, in-place continuation, reasoning-quality, and comparative claims.
A shared-Provider Claude Code/Codex preflight also completed with the same
requested Model identifier, prompts, effort label, timeout, empty-workspace
class, and fixed response. Its machine verdict is `not_comparable`: Codex
reported fallback Model metadata, the products used different protocols,
Tools, reasoning representations, Context, and sandboxes, and identical Model
implementation was not settled. No cross-product comparative result is
claimed. A follow-up Codex/Grok Build preflight aligned both main calls on the
same Provider process, Responses protocol, `gpt-5.4`, prompts, effort, and
read-only sandbox requests. It still returned `not_comparable`: Grok attempted
a `grok-4.5` title call that the fixture rejected with HTTP 422 and then
silently continued; Tool schemas, Context, reasoning summaries, permission
modes, call counts, and Codex identity settlement also remained unequal.

## Start

```bash
./scripts/install.sh
./scripts/install-tui.sh
yh demo "hello Y-Harness"
yh-tui --demo
yh init my-harness
cd my-harness
yh doctor
yh serve
```

`yh doctor` and `yh serve` now preflight existing State, Approval, Task,
Workflow, and Human Handoff SQLite stores through the concrete adapters before
constructing external capabilities. Diagnosis is read-only, reports each store
as ready or eligible for creation, never auto-migrates, and preserves the
explicit backup-first operator boundary. Service open repeats authoritative
validation. This changes no durable or Protocol coordinate.

## Compatibility

- Rust crate: `0.1.0`
- optional TUI package: `0.1.0`
- optional Domain Pack control-plane package: `0.1.0`
- service configuration: `1`
- client protocol: `37`
- State event/snapshot schema: `16` / `16`
- Agent Loop wait projection schema: `1`
- Thread archive format: `6`
- Approval Inbox schema: `3`
- Task Coordinator schema: `4`
- Workflow Coordinator schema: `1`
- Human Handoff Coordinator schema: `1`
- Effect Ledger schema: `1`
- Governed Effect Executor/Reconciler API: `1` / `1` (embedded only)
- JSON Effect Connector protocol: `1`
- Temporal Driver API: `3` (embedded only)
- Secret Provider API: `3`
- Domain Pack format/store schema: `1` / `1`
- HTTPS Model Gateway API: `7`

Before upgrading older State or Approval databases, stop all writers and use
the documented backup-first migration commands.

## Explicit limitations

- Linux and Windows deny external execution by default but do not yet include a
  tested strong OS sandbox broker.
- Network protocol exposure requires the mandatory-mTLS host; the stdio JSONL
  service is not a raw Internet server.
- Native OpenAI Responses and Chat Completions, Anthropic Messages, and Gemini
  `generateContent` adapters are included. Their deterministic mapping and
  transport tests are local; live API passes remain environment-gated.
  Schema-5 origin-bound continuation preserves replay-required provider state;
  a function call whose required continuation is unavailable still fails
  before Tool execution.
- SQLite offers single-host durability and multi-process CAS, not multi-node
  consensus or distributed high availability.
- Workspace cleanup cannot guarantee recovery after power loss or hostile
  provider behavior.

Release claims apply only to the tagged commit and its recorded local/remote
evidence. Permanent zero-defect software is not a provable claim.
