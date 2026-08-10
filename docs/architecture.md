# Y-Harness Engineering Architecture

## System shape

Y-Harness has one semantic core with two consumption modes and replaceable
product clients:

```text
embedded host ───────────────────────────── Core API
                                                   \
CLI · TUI · GUI · LUI · VUI · IDE · API ─ protocol ─ Runtime ─ Core
        independent optional products

optional Domain Pack control plane ─ verified execution binding ─┘
```

The service mode must not implement a second agent loop. It hosts the same
Core and translates protocol commands and events. Product clients never import
provider implementations, open Runtime storage, or become authoritative state
owners.

GUI may target Web, Desktop, or Mobile; Conversational/LUI may target Web Chat,
Desktop Chat, or IM. Voice/VUI, IDE extensions, SDKs, APIs, and Webhooks remain
independent adapters over the same contract. The governance and Domain Pack
boundary is specified in
[`enterprise-governance.md`](enterprise-governance.md).

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
- a versioned Secret Provider API 3 registry with opaque serializable
  references, non-serializable zeroizing values, debug redaction, typed
  Agent-Turn/Governed-Effect/service-use contexts, trusted-authority
  resolution, a global environment allow-list adapter for unscoped hosts, and
  an exact tenant/reference adapter with no fallback; credential-bearing JSON
  Effect commands require frozen dispatch SHA-256 evidence, preflight before
  Provider resolution, and Broker remeasurement before child entry;
- an optional control-plane crate above Core with immutable digest-bound
  Domain Pack snapshots, exact component inventories, tenant-partitioned
  Memory/SQLite lifecycle records, independent evaluation approval,
  revision-CAS activation/rollback, and current-inventory execution binding;
- an exact-versioned HTTPS JSON model-gateway adapter with TLS 1.2+, on-demand
  bearer resolution, disabled redirects/proxies/retries/referers, bounded
  concurrency/time/body retention, pooled connections, client-safe errors, and
  an exclusive bounded enterprise-CA trust mode with no ambient roots plus an
  optional non-serializable mTLS client identity;
- an optional direct OpenAI Responses adapter with environment-backed API-key
  resolution, pooled HTTPS, `store: false`, bounded JSON/SSE decoding, exact
  function-call translation, provider usage evidence, ordered multi-call
  proposals, and Harness-owned effect-safe Tool scheduling;
- an additive configured JSON-command Model that reuses the bounded
  language-neutral `ModelRequest`/`ModelOutput` contract, explicit Process
  Broker authority, exact environment mapping, cancellation, route selection,
  and External provenance without fabricating provider metadata or streaming;
- opt-in exact NDJSON model-gateway streaming with incremental linear decoding,
  bounded frames/deltas/total bytes, exactly one mandatory final typed response,
  and no behavior change for requests without a provisional-event sink;
- one registry path for built-in and extension tools;
- a frozen Runtime-generation Tool Capability View that defaults to all
  registered descriptors, permits an explicit embedded-host subset, rejects
  unknown/duplicate selection at configuration time, and fences every single,
  batch, parallel, and approval-resumed Tool call against the exact disclosed
  set before Policy or execution;
- a frozen fail-safe Tool batch declaration, whole-batch authorization,
  maximal explicitly safe concurrent runs with a 1–64 Runtime ceiling,
  sequential fences, and source-ordered durable result settlement;
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
- deterministic format-1 Completion Receipts that bind one exact Assistant
  candidate, Model request, Turn evidence prefix, frozen Model route, Tool
  Capability View, Verifier manifest/outcomes, Runtime governance, trusted
  authority, and optional execution binding; State validates the receipt
  against the exact running projection and atomically commits successful
  terminal status with it;
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
- an optional bounded HTTP deployment adapter whose liveness/readiness routes
  translate the authoritative Protocol-v37 admission projection without
  opening stores or probing downstream dependencies;
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
  current schema-16 writer, and no mutation of authoritative history or
  persistence of generated summary bodies;
- optional 1–64-block per-Turn reference Context with pre-State count/byte/
  identity validation, fixed non-authoritative labeling, provider-specific
  recounting, source/body SHA-256, authenticated caller attribution, and
  schema-11 content-free evidence; callers cannot forge Skill, Memory, or
  conversation-summary provenance;
- a format-1, read-only Thread-handoff request compiler that finds the longest
  identical Turn prefix, selects a whole-Turn source delta within explicit
  count/byte bounds, binds it to both Thread identities and a canonical digest,
  and delegates candidate synthesis to any host-selected summarizer before
  re-entering the governed per-Turn Context path;
- transport-independent prompt, Context block/aggregate, Tool output, Model
  request, error, and Agent Loop hard bounds;
- one allocation-time JSON authority shared by Approval, Context, Evaluation,
  Model/Tool adapters, MCP, State, and trace export: caller/provider `Value`
  trees are iteratively limited to 64 levels and 65,536 nodes, while counting
  and materializing serializers stop at each subsystem's byte ceiling rather
  than checking a complete temporary buffer afterward;
- distinct model context blocks and journaled memory-context observations;
- an explicit ordered route of 1–16 registered Models that falls through only
  on ordinary pre-output failures, applies a configurable per-attempt deadline
  (30 seconds by default for multi-model routes), cancels before provider
  release, records every attempt in Observability, writes the settled Model
  identity/origin to State, and never crosses cancellation, the Turn deadline,
  or successfully delivered provisional output;
- an optional process-local attempt-timeout cooldown that reorders only
  Runtime-proven timed-out Models behind ready Route candidates, retains them
  as last-resort fallbacks, preserves Provider Continuation affinity, and emits
  explicit content-free skipped observations without inferring health from
  ordinary Provider strings;
- a bounded typed Model Provider failure contract that keeps Harness-owned and
  legacy errors on `Model(String)`, preserves only proved remote facts in
  `ModelProviderFailure`, and exposes content-free class/status/retry evidence
  independently from failover, cooldown, and durable State policy;
- an explicit default-disabled same-Model retry policy that accepts only typed
  rate-limit, overload, server, and transport failures, shares one candidate
  attempt deadline, honors only in-bound Provider delays, otherwise applies
  bounded equal-jitter exponential backoff, remains cancellable, stops after
  provisional output, and records every invoked retry index without replaying
  a Turn or Tool effect;
- an independent 1–144 Runtime Model-attempt budget shared by every retry and
  Route candidate in one Agent Loop step (16 by default), yielding the hard
  Runtime-managed Turn bound `max_steps × max_model_attempts_per_step` without
  claiming visibility into model calls hidden inside extension providers;
- a strict service-configured Model catalog whose operator-owned IDs are stable
  aliases, whose credential references remain per Model, and whose explicit
  route is validated before Provider construction without adding a second
  router or implicit fallback;
- a bounded, non-executable Provider Continuation contract that Runtime binds
  to the settled Model identity/origin, persists before the corresponding
  decision, filters per model attempt, and uses to suppress unsafe failover
  inside an unfinished Tool chain;
- schema-7 atomic ordered 2–64-call Model decisions with exact batch
  identity/position evidence, whole-batch validation and authorization before
  effects, bounded explicitly safe execution with sequential fences and
  source-order settlement, steering-safe synthetic results, and restart-safe
  pre-effect approval continuation;
- schema-8 explicit, clearable Thread names with bounded canonical input,
  journal authority, same-transaction recent-list projection, and startup
  drift validation;
- schema-9 atomic terminal-boundary Thread forks with caller-owned retry
  identity, immutable direct lineage, exact parent-prefix SHA-256, preserved
  historical evidence identity, omitted recovery-only Checkpoints, and no
  replay of Tool or approval effects;
- schema-10 bounded portable Thread archives with an exact source-journal
  digest, terminal export boundary, caller-owned import retry identity,
  atomic target materialization, fresh Event identities, durable source
  provenance, and no replay of Tool or approval effects;
- schema-11 attributed invocation Context with ephemeral bodies, ordered
  source/reference provenance, independent byte/token bounds, and no mutation
  of conversation or branch authority;
- schema-12 authoritative optional Thread tenant ownership with exact
  Thread/Turn/Operation fencing, validated disposable Memory/SQLite lookup
  projections, unscoped legacy migration, inherited fork ownership, and
  importing-tenant archive rebind;
- schema-13 immutable content-free Turn execution binding with exact
  configuration/environment digests, revision, trusted actor, and tenant;
  single-binding projection validation, Model invisibility, SQLite
  snapshot/reopen persistence, exact approval-resume matching, and
  archive-format-3 preservation without cross-tenant evidence rewriting;
- schema-14 optional Connector evidence stored atomically with Tool output,
  with Runtime-bound registered Tool/origin, trusted actor/tenant, exact output
  SHA-256, recovery-time execution-chain validation, Model invisibility, and
  archive-format-4 preservation without cross-tenant authority rewriting;
- schema-15 atomic `TurnCompleted` settlement with a deterministic bounded
  CompletionReceipt; new writers reject receipt-free success, while migrated
  receipt-free schema-1 through schema-14 completion remains explicitly
  legacy/unverified; archive format 5 preserves receipt bytes, so fork/import
  provenance establishes inherited source proof without claiming that the
  target Thread reran completion gates;
- schema-16 same-journal `AgentLoopExecution` coordination for one durable
  Approval wait, with immutable authority/generation/active-budget envelope,
  revision-fenced `Waiting`/`Ready`/`Executing`, atomic cancellation and
  timeout closure, atomic denial without a worker claim, and archive-format-6
  preservation; independently versioned live-wait projection schema 1 is
  updated in the journal transaction, indexes `Waiting`/`ReadyAllow` expiry and
  immediately due `ReadyDeny`, and supports tenant-local keyset discovery plus
  exact 1–2-event maintenance without complete Thread recovery;
- Approval Inbox schema 3 immutable optional tenant ownership with exact
  Memory/SQLite list/get/settle/orphan fencing, validated lookup projection,
  attributed same-tenant separation of duty, restart continuation, and
  explicitly unscoped schema-2 migration;
- Task Graph schema 4 immutable optional tenant ownership over the whole Graph,
  including leases, messages, Artifacts, append-only Task-attempt execution
  bindings, and bounded canonical execution-capability requirements; exact
  Task/lease/attempt/worker evidence, governed-retry anti-downgrade,
  all-requirements capability matching, tenant-partitioned identity, exact
  Memory/SQLite CAS fencing, trusted Orchestrator binding before executor
  entry, validated lookup projections, explicit old-schema migration without
  inferred ownership or capabilities, and protocol Worker fail-closed behavior
  until capabilities can be bound to trusted registration evidence;
- content-free bounded Thread summaries that project the same direct lineage
  for Protocol clients without loading full histories or creating a second
  branch authority;
- a persistent stdio MCP transport behind a provider-neutral client port, with
  a mandatory default-deny launch authority, explicit bounded unrestricted
  opt-in, reusable macOS Seatbelt write/network isolation, an exact absolute
  working directory, cleared child environments, discarded child stderr, Unix
  process-group settlement, bounded raw frames, finite tool
  pagination/catalog/results, bounded lifecycle/call timeouts, sanitized
  failures, reconnect-after-failure behavior, explicit enablement, and optional
  startup SHA-256 command-file drift detection;
- an optional authenticated HTTPS MCP transport for the stateless
  JSON-response subset, with exact credential-free URLs, per-request Secret
  resolution, exclusive-CA support, no redirects/proxies/retries, bounded
  JSON bodies and session IDs, and explicit rejection of SSE or
  expired-session request replay;
- atomic namespaced MCP catalog registration into the ordinary Tool registry,
  preserving external origin and all Policy/approval/State boundaries, with
  cooperative in-flight cancellation and bounded session settlement before a
  cancelled or timed-out Turn becomes terminal; shared clients reject
  tenant-scoped calls before remote invocation unless an implementation
  explicitly partitions credentials and sessions by trusted authority;
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
- a project lifecycle for signed External Skills that configures exact
  publisher/log roots, validity/transparency/revocation policy, verifies before
  create-new storage, preserves the complete signed envelope and External
  origin, keeps activation separate, rechecks trust at startup and governed
  Context use, and leaves revoked packages recoverably removable;
- Context Engine loading of resolved Skill instructions in dependency order;
- an Evaluation target/runner with two-level bounded concurrency, engine-owned
  case and per-Grader deadlines/cancellation, panic isolation,
  deterministic report ordering, root-boundary revalidation, bounded
  materialized batches, format-2 self-describing artifacts, Grader-origin-bound
  case/grader regression baselines, a configured JSON-command Grader path, and
  a versioned end-to-end `eval-smoke` gate;
- a serializable Task DAG with deterministic ready ordering, fenced leases,
  failure propagation, messages, Artifacts, workspace requirements, and
  preflighted 64-Task claim windows, plus a domain-authoritative conservative
  materialization charge under the durable 64 MiB boundary;
- a revisioned Task Coordinator with in-memory parity and durable SQLite CAS,
  restart recovery, cross-connection conflict detection, invariant validation,
  and stale-worker fencing;
- an independent revisioned Workflow Run aggregate above one same-tenant Task
  Graph, with content-bound command idempotency, signal/timer wait fencing,
  explicit retry waits, safe-boundary definition migration, bounded immutable
  transitions, Memory/SQLite parity, restart recovery, and Task-completion
  proof without taking over Task lease or effect authority;
- an independent revisioned Human Handoff aggregate bound to one existing
  same-tenant Thread or Workflow Run, with actor-and-content-bound command
  idempotency, stable priority/time/identity queue ordering, finite
  authenticated-owner claim leases, never-reused claim fences, immutable
  transitions, Memory/SQLite parity, projection validation, and restart
  recovery without implicitly pausing, routing, approving, or executing the
  subject;
- an independent revisioned Effect Ledger that commits bounded external
  intent before execution, tenant-scopes operation/idempotency identity,
  fences one finite worker attempt, treats lease expiry as `unknown`, and
  requires exact content-free receipt or authoritative reconciliation before
  retry or terminal settlement;
- embedded Governed Effect Executor API 1 with frozen exact-versioned
  Connector descriptors, explicit target/Connector idempotency contracts,
  default-deny pre-Claim Policy, complete pending-page revalidation,
  deterministic actor/tenant/cycle-bound Claim identity, bounded concurrent
  Connector entry, panic/timeout/cancellation isolation, and fail-closed
  post-dispatch uncertainty; one host call performs one sweep and Core owns no
  background consumer, Channel, credential, or reconciliation lifecycle;
- embedded Governed Effect Reconciler API 1 with frozen exact-versioned
  authoritative read-only lookup descriptors, default-deny pre-query Policy,
  complete unknown-page revalidation, bounded concurrent lookup isolation,
  evidence validation, deterministic actor/tenant/cycle/evidence-bound
  settlement identity, and exact revision/attempt/lease CAS; duplicate
  cross-host lookups may occur but Core owns no poller, query lease,
  credential, target truth model, or external mutation authority;
- exact JSON Effect Connector protocol 1 with separate execution and
  authoritative read-only reconciliation envelopes, cancellation-free process
  requests, typed Effect-phase cancellation, strict version settlement, and
  shell-free Process Broker execution; optional Secret references resolve
  under exact Effect authority per dispatch into zeroizing, non-serialized
  process environment buffers; adapter failure returns to the existing
  Executor/Reconciler uncertainty rules;
- an opt-in reference-service Effect consumer with independently configurable
  execution and reconciliation loops, separate exact Connector registries and
  non-empty allowlists, mandatory per-dispatch command-file SHA-256 locks,
  optional content-free credential probes and reference-only Secret
  environments,
  bounded cadence/backoff/concurrency/timeouts, disposable process-local
  cursors, content-free health transitions, and
  Effect-before-Temporal-before-Protocol/MCP shutdown while leaving Core
  task-free and the Ledger authoritative;
- embedded Temporal Driver API 3 with host-supplied time, optional
  Workflow/Handoff/Effect/State composition, 1–256-record tenant-local keyset
  scans per source, disposable cursors, fail-closed extension-page
  revalidation, deterministic fence-bound command identity, exact-boundary
  advancement through existing CAS commands, and independent
  applied/duplicate/fenced/failed settlement; it starts no background task and
  introduces no scheduler database or second time authority;
- an opt-in reference-service Temporal lifecycle that supplies the fixed
  service Authority and Unix time, bounds cadence and scan size, skips missed
  ticks, retains only a disposable process cursor, emits bounded health
  transitions, advances the same bounded Agent Loop wait projection, and stops
  before Protocol/MCP shutdown while leaving Core task-free;
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
- cancellation-aware JSONL framing that observes Host shutdown only between
  request frames, preserving every accepted response; the reference service
  maps SIGTERM/SIGINT or Ctrl-C into that boundary and uses a bounded detached
  stdin bridge so an open supervisor pipe cannot pin Tokio Runtime shutdown;
- protocol-v37 negotiation with the asymmetric 2 MiB request/16 MiB
  response ceilings, allocation-time bounded JSON serialization, count-plus-
  byte State event cursor pages, byte-authoritative Thread capacity, and an
  explicit Token Counter and Conversation Compactor API coordinate; protocol
  37 advertises State/snapshot schema 16 and Thread archive format 6, adds
  opt-in durable Approval release, exact wait discovery/resume/cancellation,
  and a bounded Waiting Operation projection; protocol 36 historically added
  schema-15 CompletionReceipt projection and terminal receipt digests;
  protocol 35 added bounded Tool Trace
  events and credential-free MCP registration projections; protocol 34 added
  independently authorized, content-free `ready`, `at_capacity`, and
  `draining` admission derived from the same finite Operation registry and
  one-way lifecycle that gate Turns; protocol 33 added credential-free private
  Skill Registry projections to the protocol-32 Runtime Catalog; protocol 32
  introduced that immutable active-generation catalog; protocol 31
  advertises Task Graph schema 4 and its exact Worker capability-matching
  boundary without accepting remote self-assertion; protocol 30 historically
  advertised Secret Provider API 3 and its typed use contexts without adding a
  Secret-bearing wire command; protocol 29 historically
  conditionally advertises Effect Ledger schema 1 and command-specific worker,
  reconciliation, and cancellation permissions; protocol 28 historically
  introduced Human Handoff schema 1 and its command-specific
  permissions when the host composes a Human Handoff Engine, protocol 27
  conditionally advertises Workflow Run schema 1 and its command-specific
  permissions when the host composes Workflow and Task coordinators, protocol
  26 advertises State/snapshot schema 14 with Runtime-bound Connector evidence,
  while protocol 25
  advertises Task Graph schema 3 for embedded-only governed attempt binding,
  protocol 24 advertises schema-13 Turn execution binding evidence while
  keeping binding authorship embedded-only, protocol 23 advertises Secret Provider API 2 and
  fail-closed MCP session fencing,
  protocol 22
  adds schema-2 durable Task Graph tenant ownership and the tenant-scoped
  worker lifecycle, protocol 21 adds schema-3 Approval tenant fencing, and
  protocol 20 adds authoritative schema-12
  Thread/Operation tenant fencing, protocol 19
  adds permissioned exact-Turn recovery takeover without automatic replay,
  protocol 18 adds bounded per-Turn Context and schema-11 attribution, protocol 17
  admits schema-10 Thread import provenance, protocol 16 adds direct lineage
  to bounded content-free Thread summaries, while
  protocol 15 retains protocol 14's bounded Task record/claim pages, server-clock
  leases, principal-derived worker ownership, exact fencing, conflict-only CAS
  retries, schema-5 Provider Continuation evidence, exact-ID actor-attributed
  schema-6 safe-boundary Turn steering, provisional-step invalidation, and
  schema-7 atomic ordered Tool-call batches, schema-8 Engine-owned Thread
  names, and schema-9 atomic Thread forks;
- initialization-time compatibility coordinates for engine, State event,
  snapshot, Thread archive, Approval Inbox, Task Coordinator, Memory API, Token Counter API,
  Conversation Compactor API, Secret API, Skill API, model-gateway API,
  Workflow Coordinator, Human Handoff Coordinator, and Workspace Provider API
  versions;
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
- a validated per-Turn Authority Context resolved by the existing protocol
  authorizer, with panic-isolated fail-closed mapping, trusted tenant-to-Memory
  scope binding, durable Thread/Operation/Approval/Task tenant ownership, and
  the same actor/tenant authority passed to State, Policy, Tool, direct Model
  Secret resolution, and MCP admission without caller-authored identity fields
  or serialized Provider identity fields;
- a thin engine CLI with strict project initialization, diagnostic, migration,
  deterministic demo, persistent stdio service and isolated configured
  Evaluation commands, explicitly launched shell-free JSON Models, Tools,
  semantic Conversation Compactors, completion Verifiers, and Evaluation
  Graders,
  exact-selected MCP Tools, explicitly activated digest-verified project
  Skills, Agent Memory Hub Context assembly, and an optional fixed
  local-process tenant shared by Protocol State/Approval/Task/Workflow/Human
  Handoff,
  configured Evaluation, archives, direct Model Secret resolution, and
  per-dispatch Effect Connector Secret resolution;
- read-only preflight of every existing authoritative service store before
  external capability construction, with exact ready/create reporting,
  actionable explicit migration diagnostics, and repeated validation during
  the later mutation-capable open;
- an independently installable full-screen Rust TUI under `clients/tui` that
  supervises the engine process and controls it exclusively through Protocol
  v36, with bounded tenant-fenced recent-Thread navigation, authoritative
  Thread projection,
  bounded provisional streaming, cancellation, event paging, and read-only
  Approval/Task inspection;
- a deny-by-default external Process Broker, an explicitly unrestricted bounded
  local broker, a scoped macOS Seatbelt write/network sandbox, an optional
  exact-path dispatch-time SHA-256 drift-lock wrapper with frozen integrity
  evidence, a separate non-cloneable zeroizing Secret environment on process
  requests, and JSON command adapters for Tools, Models, semantic Conversation
  Compactors, completion Verifiers, and Evaluation Graders; Models preserve the compatible bare-output
  wire or explicitly select a strict Provider-evidence/failure settlement,
  while exact owner cancellation propagates into external Model, Context,
  Verification, and Evaluation execution; local execution permits 1–4096
  concurrent direct children, remains cancellable during pipe settlement,
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
  class, distinct registered and Provider-reported Model identities, model
  accounting, failure isolation, and bounded local collection capped at 65,536
  records; oversized observation identities are rejected before observer
  delivery or retention;
- deterministic CLI demonstration with no provider credentials.

Snapshot archival, distributed orchestration coordination, lease/fenced remote
approval continuation, unknown Tool-effect reconciliation, Model load
balancing/circuit breaking, additional direct vendor model adapters,
Linux/Windows sandbox brokers, Skill Registry mirror federation/OAuth and
append-only transparency-log consistency,
streaming large-dataset Evaluation reports, certificate subject/SAN identity,
multi-principal reference-service tenant routing, general Secret-manager
integration, tenant-partitioned MCP sessions, external Artifact storage and
authorization, automatic effect-safe Task retry, durable compensation
planning, automatic Human Handoff channel routing and outbox delivery, Domain
Pack service/protocol integration, role and delegation claims, revocation,
and policy hot reload remain explicit subsequent slices.
Task Artifact reference metadata is already part of its tenant-fenced Task
Graph; this does not authorize or store the referenced external content.

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
Stores may additionally expose a content-free recent-Thread index through an
exclusive descending sequence cursor. Protocol advertises `thread.list` only
when the configured store implements that bounded capability; clients still
load one exact authoritative Thread before use.

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
awaiting Context, Model, Policy, and Tool capabilities. A Tool receives a
per-call stop signal derived from both controls. Tools default to no cleanup
grace; an implementation may declare a registration-time-frozen grace of at
most ten seconds. The runtime waits only that bounded interval before recording
the reason and active phase. State append and terminal settlement are
deliberately outside interruptible waits.

Cancellation is not rollback. In particular, a Tool stop may occur after an
external side effect started. The runtime neither reports that as an ordinary
Tool error nor retries it. A Tool can observe the shared cancellation token to
perform capability-specific cleanup. MCP adapters reserve the bounded grace,
discard and settle the affected built-in session, and reconnect only for a
later explicit call. A cleanup failure remains a failure rather than being
relabeled successful cancellation.

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
freeze/compile capability view → advertise exact Model request
         → validate proposed call belongs to that view
         → resolve registered capability → authorize → record decision
         → execute → record result → verify/continue
```

No client, plugin, or model provider may bypass this ordering.
Registry membership alone is never evidence that a Tool was disclosed to the
Model. Capability View membership gates entry into Policy and execution; Policy
and Approval remain independent authorities after that gate.

The Loop is governed as a hierarchical controller inside the Runtime rather
than as a flat `while` loop or a container for every subsystem. Admission,
transient execution phases, durable Waiting, terminal settlement, progress
governance, completion receipts, and typed extension boundaries are specified
in [ADR 0155](adr/0155-hierarchical-agent-loop-governance.md). That ADR is also
the honest current-state matrix for the eleven reviewed Loop principles.
Format-1 completion settlement is implemented by
[ADR 0157](adr/0157-generation-bound-completion-receipt.md). The durable
Waiting and exact-resume contract is specified in
[ADR 0158](adr/0158-durable-agent-loop-waiting-and-resume.md): `TurnStatus`
remains `Running` or terminal, while a same-State-journal, same-CAS
`AgentLoopExecution` now distinguishes `Waiting`, `Ready`, and `Executing` for
the implemented single non-batch pre-Tool Approval slice. State 16 and Protocol
37 release the worker, preserve the original authority/generation/remaining
active timeout, expose exact discovery/resume/cancellation, and settle denial
atomically without a claim. ADR 0159 adds the same-transaction live-wait due
index and bounded Temporal timeout/denial convergence without complete Thread
recovery. Generalized batch waiting, explicit non-mixed `HumanInput`, Inbox
repair outbox/tombstone, finite worker leases, `NeedsReconciliation`, a frozen
Context capsule independent of caller replay, and cross-process resume
receipts remain future work. Dynamic capability projection and typed
interceptors are likewise planned rather than represented as implemented
features.

ADR 0156 implements the first liveness guard beneath that hierarchy: a bounded,
replayable Progress Governor over exact failure-bearing Tool cycles. Its stop is
evaluated only after pending durable Steering is applied at a pre-Model or
terminal-step-budget safe boundary. Terminal continuation seals Steering under
the same lock before `MaxSteps` settlement. It adds no State or Protocol shape
and is not represented as semantic failure classification or external-effect
deduplication.

Successful completion follows a separate evidence boundary after the last
Model proposal:

```text
persist candidate + Model-request digest
  → run frozen candidate-bound Verifiers
  → apply accepted Steering before every terminal decision
  → build and revalidate deterministic CompletionReceipt
  → atomically commit TurnCompleted(receipt)
  → expose receipt digest to the Operation/client
  → run channel delivery and derived post-terminal jobs separately
```

Format 1 proves only evidence inside the owning Turn. Artifact, Effect, and
business-delivery obligations must be explicitly `not_required`; the receipt
does not certify cross-aggregate Artifact content, external Effect truth,
channel acknowledgement, or Memory/title/suggestion/Evaluation jobs.

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
