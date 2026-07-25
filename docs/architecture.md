# Y-Harness Engineering Architecture

## System shape

Y-Harness has one semantic core with two consumption modes:

```text
embedded application ──────── Core API
                                      \
CLI/TUI/Web/Desktop ─ typed protocol ─ Runtime ─ Core
```

The service mode must not implement a second agent loop. It hosts the same
Core and translates protocol commands and events.

This shape is derived from a primary-source comparison of Pi Agent Harness,
Claude Code, Codex, Hermes Agent, and OpenCode. The observations, decisions,
and non-copying rules are recorded in
[`reference-analysis.md`](reference-analysis.md).

## Eleven layers

| Layer | Kernel-owned invariant | Extension points |
|---|---|---|
| Context Engine | deterministic compilation and token budget | sources, retrievers, compactors |
| Agent Loop | state machine, stop and recovery semantics | model providers, loop policies |
| Tool Runtime | validated calls and result settlement | tools, MCP, CLI/process adapters |
| State Engine | authoritative typed state transitions | stores, blob backends |
| Memory Engine | scoped reads/writes and provenance | memory providers, embedders |
| Skill Engine | discovery, resolution, trust, versioning | skill sources and packages |
| Policy Engine | deny/ask/allow before side effects | policy providers, approvers |
| Orchestration | ownership, dependencies, cancellation | schedulers, execution environments |
| Verification | run completion conditions | verifiers |
| Observability | ordered evidence and correlation | exporters and read-only observers |
| Evaluation | reproducible comparison and baselines | suites, graders, judges, reporters |

## Current vertical slice

The current code implements the smallest durable path that validates kernel
direction:

- typed Thread, Turn, Item, Checkpoint, model, tool, policy, and state-event
  contracts;
- a deterministic Model registry with validated identities, collision
  rejection, trust-bearing origins, explicit Runtime selection, and durable
  provenance on model-produced State;
- construction-time frozen model identity, with synchronous metadata panic
  isolation and no provider re-entry while recording State or Observability;
- a versioned Secret Provider registry with opaque serializable references,
  non-serializable zeroizing values, debug redaction, and an explicit
  environment-variable allow-list adapter;
- an exact-versioned HTTPS JSON model-gateway adapter with TLS 1.2+, on-demand
  bearer resolution, disabled redirects/proxies/retries/referers, bounded
  concurrency/time/body retention, pooled connections, client-safe errors, and
  an exclusive bounded enterprise-CA trust mode with no ambient roots plus an
  optional non-serializable mTLS client identity;
- opt-in exact NDJSON model-gateway streaming with incremental linear decoding,
  bounded frames/deltas/total bytes, exactly one mandatory final typed response,
  and no behavior change for requests without a provisional-event sink;
- one registry path for built-in and extension tools;
- one panic-isolated metadata capture boundary for Model, Tool, Memory, Token
  Counter, Conversation Compactor, Secret, Verifier, Grader, Process Broker,
  and Workspace Provider extensions, with validated snapshots;
- bounded capability origins and finite registries, including a 4,096-entry
  shared ceiling, tighter Evaluation limits, a 1 MiB per-Tool/8 MiB aggregate
  Tool metadata budget, and a 64 MiB aggregate Skill content budget;
- collision rejection and deny-by-default policy;
- an asynchronous model/tool loop with a hard step budget;
- per-Runtime concurrent Turn admission with a safe default, validated operator
  limit, retryable overload result, and no pre-admission State mutation;
- cooperative Turn cancellation and an absolute external-work deadline, with
  distinct cancelled, timed-out, failed, and interrupted terminal states;
- panic isolation around external capability future construction, polling, and
  drop, with content-free typed errors and ordinary durable failed settlement;
- explicit takeover-only recovery: normal execution never interrupts a running
  Turn owned by another Runtime, while a confirmed abandoned worker can be
  settled only after the host establishes exclusive Thread ownership;
- two-stage allow/deny/ask Policy and approve/deny settlement with risk class,
  correlation identity, ordered evidence, and a safe default denial;
- a revisioned durable Approval Inbox with memory/SQLite parity, idempotent
  requests, a deterministic 16-record oldest-first working window,
  cross-process CAS settlement, and fail-safe orphaning when the consuming Turn
  is interrupted, failure-atomic in-memory transitions, terminal-capacity
  reservation, identity-first bounded SQLite recovery, plus capability-gated
  pending/get/settle protocol commands;
- one shared SQLite `TEXT` boundary that checks encoded byte length before Rust
  allocation for State, Approval, and Task Coordinator durable payloads, while
  retaining each subsystem's post-decode invariant validation;
- deterministic completion verifiers whose retryable failures re-enter the
  Agent Loop and whose hard failures prevent false completion;
- an append-only SQLite event journal with in-memory parity for tests;
- deterministic projection, idempotent event append, checkpoint persistence,
  and interrupted-turn recovery;
- atomic optimistic stream concurrency across independent SQLite connections,
  plus a non-authoritative validated head cache;
- Event Store authority bounds that validate pending mutations and revalidate
  append/read results, identities, ordering, Thread ownership, encoded size,
  checkpoint labels, atomic stream recovery charge, and requested count-plus-
  byte page capacity;
- optional journal-anchored State snapshots with projection digests, bounded
  tail paging, corruption fallback, SQLite persistence, and finite Thread
  event-count plus recovery-byte boundaries;
- opt-in terminal-Turn snapshot maintenance with per-Thread deduplication,
  bounded global concurrency, failure/panic isolation, latest-only retention,
  content-free health counters, and graceful shutdown draining;
- an authoritative count-and-recovery-byte capacity projection with 80%
  warning, 95% critical, and terminal-only levels; the last event plus a
  dedicated byte budget are reserved for durable Turn settlement and a
  separately authorized protocol capability exposes the worst pressure without
  pretending to report host storage;
- JSONL trace export as a derived view of the authoritative event journal;
- a versioned Memory Provider registry with declared operations and collision
  rejection;
- Context Engine compilation that preserves provider context packs, enforces a
  run-wide memory budget, and supports fail-turn or explicit degraded behavior;
- a deterministic whole-Turn conversation suffix with model-visible Item
  filtering, optional provider-specific Token Counter budgeting, an independent
  serialized-byte ceiling, and journaled selection evidence;
- a versioned, bounded Token Counter registry with exact selection,
  trust-bearing origins, frozen metadata, sanitized failures, and
  Context-phase panic isolation;
- a versioned semantic Conversation Compactor registry with explicit selection,
  bounded newest-omitted whole-Turn input, independent output token/byte
  ceilings, exact coverage plus source/content fingerprints, an engine-owned
  non-authoritative marker, Context-phase cancellation/deadline/panic isolation,
  content-free audit evidence introduced by schema 2 and retained by the
  current schema-4 writer, and no mutation of authoritative history or
  persistence of generated summary bodies;
- transport-independent prompt, Context block/aggregate, Tool output, Model
  request, error, and Agent Loop hard bounds;
- one allocation-time JSON authority shared by Approval, Context, Evaluation,
  Model/Tool adapters, MCP, State, and trace export: caller/provider `Value`
  trees are iteratively limited to 64 levels and 65,536 nodes, while counting
  and materializing serializers stop at each subsystem's byte ceiling rather
  than checking a complete temporary buffer afterward;
- distinct model context blocks and journaled memory-context observations;
- an explicit ordered route of 1–16 registered Models that retries only
  ordinary pre-output failures, applies a configurable per-attempt deadline
  (30 seconds by default for multi-model routes), cancels before provider
  release, records every attempt in Observability, writes the settled Model
  identity/origin to State, and never crosses cancellation, the Turn deadline,
  or successfully delivered provisional output;
- a persistent stdio MCP transport behind a provider-neutral client port, with
  a mandatory default-deny launch authority, explicit bounded unrestricted
  opt-in, reusable macOS Seatbelt write/network isolation, an exact absolute
  working directory, cleared child environments, discarded child stderr, Unix
  process-group settlement, bounded raw frames, finite tool
  pagination/catalog/results, bounded lifecycle/call timeouts, sanitized
  failures, and reconnect-after-failure behavior;
- atomic namespaced MCP catalog registration into the ordinary Tool registry,
  preserving external origin and all Policy/approval/State boundaries;
- a first-party Agent Memory Hub adapter for search, bounded read, governed
  write, resume brief, and health;
- an isolated real-process integration test covering MCP negotiation, tool
  discovery, write, read, retrieval, and graceful shutdown;
- a release-mode State benchmark with optional calibrated regression
  thresholds;
- a declarative Skill registry with exact SemVer identities, SHA-256 integrity,
  dependency/cycle/tool checks, whole-block budgeting, and on-demand resources;
- bounded live Skill publisher/log trust with validity windows, immutable
  effective revocation, policy-required signed transparency receipts, preserved
  receipt provenance, and trust rechecks at resolution/resource/Context use;
- optional exact-identity/digest-pinned public HTTPS Skill acquisition with
  TLS/no-redirect/no-proxy/no-retry policy, bounded streaming reads, aggregate
  package limits, and verify-before-register ordering;
- Context Engine loading of resolved Skill instructions in dependency order;
- an Evaluation target/runner with two-level bounded concurrency, engine-owned
  case deadlines and cancellation, grader timeouts, panic isolation,
  deterministic report ordering, root-boundary revalidation, bounded
  materialized batches, format-2 self-describing artifacts, Grader-origin-bound
  case/grader regression baselines, and a versioned end-to-end `eval-smoke`
  gate;
- a serializable Task DAG with deterministic ready ordering, fenced leases,
  failure propagation, messages, Artifacts, workspace requirements, and
  preflighted 64-Task claim windows, plus a domain-authoritative conservative
  materialization charge under the durable 64 MiB boundary;
- a revisioned Task Coordinator with in-memory parity and durable SQLite CAS,
  restart recovery, cross-connection conflict detection, invariant validation,
  and stale-worker fencing;
- a public bounded Task Orchestrator that executes host-provided sub-Agent
  capabilities, advances dependencies, isolates timeout/panic failure, cancels
  fenced workers, and settles only an exact current lease;
- lease-fenced Task Mailboxes with CAS-safe durable sends and 1–256-item,
  2 MiB cursor inbox pages, so executors communicate without mutable graph
  access or unbounded history cloning;
- Workspace Provider API v1 with default-deny admission, exact-attempt
  allocation leases, executor-safe canonical views, bounded prepare/release
  cleanup, concurrent local-directory isolation, and detached full-object-ID-
  pinned Git Worktrees through an explicit Process Broker; provisioning does
  not claim OS sandbox strength;
- a transport-neutral, exactly versioned command protocol with asynchronous
  Turn operations, cooperative cancellation, bounded retention, normalized
  errors, content-free task-panic supervision, and optional durable Task Graph
  administration plus authenticated fenced worker coordination;
- a one-way bounded protocol drain that rejects new Turns, cancels accepted
  Operations, waits for terminal settlement, spends the same remaining
  deadline on Runtime-owned automatic snapshot work, and reports Operation and
  background completion independently without forced-success relabeling; stdio
  and mTLS hosts invoke it during shutdown;
- protocol-v10 negotiation with protocol-v2's asymmetric 2 MiB request/16 MiB
  response ceilings, allocation-time bounded JSON serialization, count-plus-
  byte State event cursor pages, byte-authoritative Thread capacity, and an
  explicit Token Counter and Conversation Compactor API coordinate; protocol
  10 retains schema-4 Policy-to-Tool-origin evidence and attributed approvals,
  while adding bounded Task record/claim pages, server-clock leases, principal-
  derived worker ownership, exact fencing, and conflict-only CAS retries;
- initialization-time compatibility coordinates for engine, State event,
  snapshot, Approval Inbox, Task Coordinator, Memory API, Token Counter API,
  Conversation Compactor API, Secret API, Skill API, model-gateway API, and
  Workspace Provider API versions;
- bounded provisional model streams with step correlation, late-emission
  fencing, failure isolation, cursor paging, and explicit eviction gaps;
- bounded JSONL stdio serving, cursor-paginated Event Store reads, and a
  process-level stdout-purity test;
- an optional mandatory-mTLS JSONL host that reuses `ProtocolHandler`, bounds
  handshakes/connections/idle time/session frames, rejects clients outside an
  operator trust root, and shuts active sessions down cooperatively;
- fail-closed protocol authorization that derives an mTLS leaf-certificate
  fingerprint principal, checks exact per-command grants before execution,
  filters advertised capabilities, and trusts only local-process callers by
  default;
- a dependency-free reference CLI/TUI that controls the Runtime through typed
  commands rather than duplicating Agent Loop behavior, plus strict project
  initialization, diagnostic, and persistent stdio service commands;
- a deny-by-default external Process Broker, an explicitly unrestricted bounded
  local broker, a scoped macOS Seatbelt write/network sandbox, and JSON command
  adapters for Tools and Models with the Runtime's exact Turn cancellation
  signal propagated into external Model execution; local execution permits
  1–4096 concurrent direct children, remains cancellable during pipe settlement,
  bounds termination cleanup to five seconds, and on Unix settles ordinary
  descendants that remain in a private per-execution process group without
  claiming escape-resistant sandbox containment;
- trusted Ed25519 publisher roots and strict detached signatures for externally
  sourced Skill packages;
- explicit Tool-specific compensation adapters that reconstruct the original
  successful effect and authorization from State, require ordinary
  Policy/approval authorization, and settle uncertain retries through one
  stable idempotency key;
- content-free Runtime phase observations with monotonic latency, settlement
  class, provider-reported model accounting, failure isolation, and bounded
  local collection capped at 65,536 records; oversized observation identities
  are rejected before observer delivery or retention;
- deterministic CLI demonstration with no provider credentials.

Snapshot archival, distributed orchestration coordination, lease/fenced remote
approval continuation, unknown Tool-effect reconciliation, Model load
balancing/circuit breaking, direct vendor model adapters, Linux/Windows
sandbox brokers, Skill catalogs/private registry authentication and append-only
transparency-log consistency,
streaming large-dataset Evaluation reports, and certificate subject/SAN
identity, tenant/role attribution, revocation, and policy hot reload remain
explicit subsequent slices.

## Client protocol boundary

The complete language-neutral wire contract is
[`protocol.md`](protocol.md). Compatibility and migration rules are separate
in [`compatibility.md`](compatibility.md).

Protocol correlation IDs, operation IDs, and durable State IDs are deliberately
different:

```text
request id ─────── correlate one request/response
operation id ───── poll/cancel/forget process-local execution
thread + turn id ─ recover authoritative durable state
```

The stdio reference transport and optional mTLS host use exact protocol
negotiation and one bounded JSON object per line. The mTLS host authenticates
the connection before any application frame and adds finite handshake,
connection, idle, and session-frame limits. A Turn starts asynchronously.
Terminal operations remain observable until the client forgets them, subject
to a validated registry capacity: 64 by default and 4,096 at the supported
maximum. A dedicated supervisor converts unexpected background task
panic/cancellation into a terminal Operation failure without copying the panic
payload. Event history is cursor-paginated at the Event Store rather than
loaded in full.

Operations are not a second persistence layer. After service restart, clients
reconcile Thread events; recovery marks unfinished Turns `interrupted` and never
replays uncertain side effects.

## State, context, and memory boundary

These layers deliberately do not share a storage abstraction:

| Concern | Owner | Lifetime |
|---|---|---|
| Thread, Turn, Item, Checkpoint, ordered runtime evidence | State Engine | authoritative run history |
| Prompt assembly, retrieved views, token allocation | Context Engine | compiled per model step |
| Durable facts, decisions, episodes, skills, provenance, feedback | Memory Engine/provider | cross-thread and cross-agent |

The State Engine may record that a memory operation occurred and retain its
opaque references. It does not become a knowledge store. The Memory Engine may
use runtime evidence as provenance, but it does not own the Agent Loop state
machine.

Agent Memory Hub is the first-party reference provider for governed long-term
memory. It remains an external system behind a versioned provider contract:

```text
Agent Loop → Context Engine → Memory Provider port → MCP → Agent Memory Hub
                    │                                  ├─ MemoryItem + evidence
                    │                                  ├─ retrieval + firewall
                    └─ final run-wide token budget     └─ context pack + feedback
```

Y-Harness owns call timing, run identity, authorization, budgets, cancellation,
failure behavior, and ordered observations. Agent Memory Hub owns its durable
schema, write funnel, indexes, ranking, memory-specific injection firewall,
feedback, governance, and memory benchmarks. Y-Harness does not import Agent
Memory Hub's Python modules or depend on its on-disk Markdown/index layout.

Retrieval is not adoption. The runtime records which opaque memory references
were loaded, but sends no positive or negative feedback merely because a pack
was retrieved or included. Feedback requires later explicit outcome evidence.
The current Agent Memory Hub MCP surface has no feedback tool, so its adapter
does not claim that operation. Likewise, its MCP write does not settle the
caller's idempotency key; Y-Harness reports that limitation and will not retry
an uncertain write.

## Cancellation and deadline boundary

The runtime checks one shared cancellation signal and absolute deadline while
awaiting Context, Model, Policy, and Tool capabilities. It then records the
reason and active phase before settling the Turn. State append and terminal
settlement are deliberately outside interruptible waits.

Cancellation is not rollback. In particular, a Tool stop may occur after an
external side effect started. The runtime neither reports that as an ordinary
Tool error nor retries it. A Tool can observe the shared cancellation token to
perform capability-specific cleanup.

Worker-loss continuation is narrower than generic recovery. An embedded host
that has independently proven exclusive Thread ownership may resume only when
the final durable boundary is exactly `ToolCall → PolicyDecision::Ask →
ApprovalRequested`. The Runtime reconstructs and hashes the original Model
request and requires unchanged Context, Memory scope, Model identity/origin,
requester actor, Policy-bound Tool origin, and Tool descriptors before consuming the
approval. `ApprovalDecision` without `ToolResult` is an unknown-effect boundary
and is never replayed generically. Protocol 9 exposes no remote takeover command
without a cross-host lease and fencing authority.

## Explicit compensation boundary

Compensation is an ordinary, separately registered Tool, never an automatic
reaction to cancellation or Verification failure. Its request identifies one
same-Thread target Turn, Tool call, and stable provider idempotency key. Before
calling a `ToolCompensator`, the adapter reconstructs from authoritative State:

- the current compensation call and its ordered Policy/approval authorization;
- one unambiguous original call for the declared target Tool;
- the original call's ordered authorization and successful result; and
- prior authorized compensation attempts for the same target.

A prior successful attempt returns its recorded result without another
provider call. An authorized attempt with an absent or failed result may be
retried only with the same key because the external outcome can be uncertain.
A different key fails closed. Failed original Tool results are not treated as
generic reversible effects.

This contract adds no State event or client-protocol shape. Calls, decisions,
approvals, and results retain the existing State schema and ordering.

## Required ordering

Every side effect follows:

```text
validate → resolve capability → authorize → record decision → execute
         → record result → verify/continue
```

No client, plugin, or model provider may bypass this ordering.

For a Policy `ask`, authorization expands without changing the invariant:

```text
validate → policy ask → record policy → record approval request
         → obtain approval → record approval settlement → execute
         → record result
```

Policy and approval are different authorities. A missing approval handler
denies the request.

Verification runs after an assistant candidate is recorded and before the Turn
can complete. It is observational and cannot erase prior Tool evidence.
Rollback requires a separately authorized, Tool-specific compensation
capability; the runtime does not infer one from a failed verifier.
