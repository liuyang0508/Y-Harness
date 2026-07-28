# Y-Harness Engineering v0.1.0

Y-Harness v0.1.0 is the first general-purpose, headless Agent Harness baseline
built around:

```text
Agent = LLM × Harness = X × Y
```

It ships an embeddable Rust Core/Runtime, Protocol v19 service, thin engine CLI,
an independently installable full-screen TUI, durable SQLite
State/Approval/Task coordination, governed extension contracts, evaluation
gates, and executable examples.
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
Protocol 19 adds permissioned recovery of one exact abandoned Turn without
automatic replay. The embedded Runtime also carries a validated per-Turn
Authority Context from a trusted host or protocol authorizer into Memory scope,
Policy, and Tool execution. Durable tenant ownership is not claimed, and
tenant-scoped approvals fail closed until a later schema binds tenant evidence.
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
Stores that implement the bounded recent-Thread index advertise
`thread.list`; the TUI uses it to list and resume the latest 64 authoritative
Threads without opening Engine storage. Protocol 16 includes optional direct
lineage in those content-free summaries, and the Sessions panel renders the
parent identity/version without projecting full histories.
The TUI exposes `/name [title]` and `/fork [terminal-turn-id]` through the
protocol; it keeps no title or branch store.
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

## Compatibility

- Rust crate: `0.1.0`
- optional TUI package: `0.1.0`
- service configuration: `1`
- client protocol: `19`
- State event/snapshot schema: `11` / `11`
- Approval Inbox schema: `2`
- Task Coordinator schema: `1`
- HTTPS Model Gateway API: `7`

Before upgrading older State or Approval databases, stop all writers and use
the documented backup-first migration commands.

## Explicit limitations

- Linux and Windows deny external execution by default but do not yet include a
  tested strong OS sandbox broker.
- Network protocol exposure requires the mandatory-mTLS host; the stdio JSONL
  service is not a raw Internet server.
- OpenAI Responses is the only direct vendor model adapter. Its mapping and
  transport tests are local; a live API pass remains environment-gated.
  Schema-5 origin-bound continuation handles replayable encrypted reasoning;
  a function call whose reasoning state is not replayable still fails before
  Tool execution.
- SQLite offers single-host durability and multi-process CAS, not multi-node
  consensus or distributed high availability.
- Workspace cleanup cannot guarantee recovery after power loss or hostile
  provider behavior.

Release claims apply only to the tagged commit and its recorded local/remote
evidence. Permanent zero-defect software is not a provable claim.
