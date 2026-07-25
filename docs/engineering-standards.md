# Engineering standards

Y-Harness Engineering treats quality as evidence, not as an adjective. No
change is described as complete while a required gate is failing or a known
critical defect remains open.

## Architecture rules

The dependency direction is:

```text
clients → Runtime → Kernel contracts
                 ↘ Context / State / capability ports
providers → capability adapters → Transport
Observability reads State evidence; it does not mutate execution
```

- Kernel contracts do not import provider implementations.
- Protocol transports do not contain Agent Loop or provider semantics.
- Providers map external capabilities onto provider-neutral ports.
- State is the authoritative execution record; JSONL and later telemetry are
  derived views.
- Built-in and external capabilities use the same typed registration rules.
- Provider selection is exact and explicit; a registry never silently replaces
  an identity or performs implicit failover. An operator-configured Model route
  is bounded, ordered, attempt-deadlined, and records the actual settled
  identity and origin.
- Untrusted executable extensions will not run in process.
- Raw evidence, runtime state, compiled context, and durable knowledge remain
  distinct data classes.

## Source layout

```text
clients/
└── tui/             optional full-screen Protocol client; no Runtime internals
src/
├── approval/        durable inbox, CAS settlement, and handler adapter
├── context/         context compilation and token allocation
├── evaluation/      suites, graders, reports, and regression baselines
├── execution/       Process Broker, sandbox, CLI Tool, and Model adapters
├── isolation.rs     shared capability Future panic/settlement boundary
├── kernel/          identities, capability contracts, and typed registries
├── memory/          provider-neutral memory port and provider adapters
├── model/           authenticated network model adapters
├── observability/   content-free phase observations and evidence exports
├── orchestration/   Task DAGs, bounded scheduling, leases, messages, Artifacts
├── protocol/        transport-neutral client commands and bounded stdio
├── reference_cli/   demo plus strict project and persistent service host
├── runtime/         Agent Loop and policy/tool settlement
├── secret/          opaque references, zeroizing values, resolver registry
├── skill/           package integrity, resolution, and context loading
├── json.rs          shared allocation-time JSON shape and byte guards
├── sqlite.rs        shared durable TEXT allocation guard
├── state/           event store, projection, recovery, checkpoints
├── transport/       MCP, stdio framing, and mandatory-mTLS hosting
├── verification/    completion conditions and verifier registration
├── lib.rs           intentionally small public facade
└── main.rs          thin CLI host
```

New directories are added when a real layer obtains code. Empty architectural
placeholders are not created.

Product clients are separate workspace packages and release assets. They may
depend on public wire DTOs for compile-time type safety, but execute only
against the versioned protocol boundary. No client may call Runtime/provider
internals or read Engine storage directly.

## Code and comments

- Names express domain meaning; comments explain invariants, safety,
  concurrency, protocol quirks, and non-obvious trade-offs.
- Public contracts document ownership and failure behavior.
- Comments must not merely narrate the next line of code.
- `unsafe` Rust is forbidden at the crate level.
- Errors are typed at subsystem boundaries and retain actionable context
  without dumping secrets or memory bodies.
- Durable configuration stores only `SecretReference`; resolved values are
  non-serializable, debug-redacted, short-lived, and zeroized on drop.
- Authenticated HTTP adapters disable redirects, ambient proxies, automatic
  retries, referers, and cookies unless a separately reviewed contract requires
  them. Sensitive headers are marked as such and response bodies are read
  incrementally under an authority-owned byte ceiling.
- Network protocol hosts must authenticate and encrypt before passing streams
  to transport-neutral JSONL framing. TLS handshakes, live connections, idle
  duration, and frames per session all require independent hard bounds.
- Request and response frames have separately justified limits. Serialization
  must stop at the response ceiling during allocation, and every accepted
  single State event must be retrievable within one response.
- Transport-derived principals are authorized against exact command
  permissions before execution. Default protocol authority is local-process
  only; authorization panics fail closed, and `Initialize` must not advertise
  capabilities the principal cannot invoke.
- Remote Task worker identity comes only from the authenticated transport,
  never a request body. Claims and heartbeats use the server clock; every
  worker mutation revalidates the current unexpired owner and fencing token
  after each conflict reload. Result pages and claim batches must fit their
  response budget before durable mutation is committed.
- Protocol command Futures are panic-isolated during construction, polling,
  and drop. Panic payloads never enter a response.
- External JSON is validated before it enters kernel state.
- Caller- or provider-controlled `serde_json::Value` trees are iteratively
  limited to 64 container levels and 65,536 nodes before serialization.
  Authority-bound size checks use a counting or bounded streaming writer;
  `to_vec` followed by `len()` is not an acceptable limit at that boundary.
- Raw frames, HTTP/process bodies, and durable text are independently
  byte-bounded before their first engine-owned materialization. Structural
  checks do not replace those ingress limits, and ingress limits do not replace
  pre-serialization structural checks.
- Transport bounds are repeated at the Runtime authority boundary; embedded
  callers do not receive a less safe path.
- Conversation history admits whole Turns in chronological order and never
  exposes internal Policy, approval, memory, or stop evidence as dialogue.
- Provider token estimates never replace hard Context and Model-request byte
  ceilings.
- A selected provider Token Counter recounts bounded conversation segments,
  Memory packs, and Skill blocks. Counter metadata is frozen at registration;
  invocation errors and panics fail closed without provider payloads.
- Conversation token and serialized-byte budgets are independent. The durable
  schema-1 `ConversationContext.estimated_tokens` field retains its original
  conservative byte charge; later schemas do not relabel that historical
  meaning.
- Semantic compaction is opt-in and registry-selected. Its input is a bounded
  newest slice of omitted whole Turns; retained raw Turns remain model-visible
  and every original Item remains authoritative State.
- Summary output is explicitly marked derived and non-authoritative, carries
  exact covered Turn IDs plus uncovered count and source/content SHA-256, and
  is independently token/byte bounded. A configured compactor failure fails
  the Turn; it never silently invents complete coverage.
- Compactor execution uses the ordinary Context deadline, cancellation,
  construction/poll/drop panic isolation, content-free observation, and durable
  failed settlement. State schema 2 introduced bounded content-free provenance,
  retained by schema 4, but never the generated summary body.
- A populated legacy State store never migrates during ordinary open. Explicit
  migration must validate exact versions and capacity, create a durable
  no-clobber backup, remain restartable, preserve historical event bytes and
  labels, and declare the last safe downgrade point.
- A populated schema-1 Approval Inbox also fails ordinary open. Its explicit
  backup-first migration fingerprints every indexed record, orphans
  unattributed pending work, preserves terminal decisions without inventing
  identities, and publishes schema-2 writer metadata atomically.
- Current durable approvals carry authority-scoped requester and settler
  actors. The Inbox CAS boundary rejects equal actors before mutation;
  transport-only logging is not a substitute for this invariant.
- A worker-loss continuation may execute a Tool only from the exact final
  `ToolCall → PolicyDecision::Ask → ApprovalRequested` boundary after actor,
  Model, Tool origin, and reconstructed Model-request fingerprint validation.
  `ApprovalDecision` without `ToolResult` is an unknown external-effect state
  and must never be replayed by the generic Runtime.
- Every current Policy decision must durably retain the trust-bearing origin of
  the registered Tool it evaluated. Denial, approval wait, execution failure,
  and success all preserve the same authorization provenance.
- Remote takeover requires a host-authoritative lease and fencing token.
  In-process active-Thread guards and operator assertions do not prove
  exclusivity across processes or machines.
- Provider results retain opaque references, provenance, and reversible
  context views.
- New model-produced State retains the registered model identity and
  operator-assigned origin; legacy missing provenance remains explicitly
  absent rather than being relabeled as trusted.
- A Runtime freezes model identity once. Invalid or panicking synchronous
  metadata rejects execution before Turn state; State and Observability never
  re-enter provider `id()` methods.
- Every executable extension metadata callback is panic-isolated before
  registry or adapter mutation. Validated descriptors are frozen; error text
  never includes a panic payload.
- Mutable capability registries reject unbounded origin identities and growth
  before invoking provider metadata where possible. Batch registration is
  failure-atomic. Metadata that is copied into recurring Model requests has
  per-entry, aggregate-byte, and iterative structural-complexity ceilings.
- Collection constructors validate an upper bound before reserving caller-sized
  capacity. Remote discovery is count-preflighted before secondary staging
  allocation.
- Verifier messages are validated and bounded before being appended to State.
- Skill package digests prove pinned content integrity. External origins also
  require a strict signature from an explicitly trusted publisher key;
  executable work remains behind Tool/Policy boundaries.
- A child process is not called a sandbox unless a concrete broker enforces and
  reports an OS restriction set.
- Local Process Broker concurrency is finite and configuration-bounded.
  Cancellation remains active while child I/O is being settled. Timeout or
  cancellation aborts I/O work before bounded terminate-and-wait cleanup. Unix
  children lead private process groups; all remaining members are signalled and
  observed gone before ordinary settlement. This must not be described as a
  sandbox or escape-resistant containment because a process can change
  session/group. Non-Unix brokers state their direct-child boundary explicitly.
- Stdio capability processes use absolute executable paths, clear inherited
  environment, redact configured arguments/values from debug output, bound raw
  frames before JSON allocation, and independently bound pagination plus
  decoded results. External tool-error bodies never become engine errors.
- Persistent stdio MCP additionally requires an explicit launch authority and
  absolute working directory. Denied mode must fail before spawn; unrestricted
  mode has a finite shared session semaphore and reports its true isolation.
  Child stderr is discarded. Unix children reuse bounded process-group
  settlement without claiming escape-resistant containment.
- macOS persistent MCP sandbox mode must reuse the same canonical-root
  Seatbelt policy as one-shot external execution. Provider integration tests
  run with network denied and must explicitly select an offline provider mode;
  a timeout caused by a blocked model download is not sandbox success.
- MCP discovery never mutates Tool registration incrementally. The complete
  namespaced catalog is validated for portable names, schemas, collisions, and
  metadata panics before one atomic registry commit.
- Configuration-file size checks must be enforced while reading, not only
  after a whole file has already been allocated.
- Durable SQLite `TEXT` from data-bearing tables must select its BLOB byte
  length and reject an over-limit value before converting it to Rust `String`.
  Decoded schema and domain validation remain mandatory.
- Default phase observations contain no prompt, context, model content, or Tool
  payload. Model usage and cost are reported only when supplied by a provider.
- Observer errors, panics, backpressure, and capacity loss cannot alter Agent
  Loop settlement and must be exposed through explicit drop counters.
- Provisional model streams are application content, never Observability
  payloads. They are byte-bounded, non-blocking, cursor-readable, and explicitly
  report eviction gaps; final State remains authoritative.
- A model-step stream is closed on every settlement path so a provider cannot
  publish late content after success, failure, cancellation, or timeout.
- Each Model attempt receives a cooperative cancellation token. The Runtime
  cancels it before releasing the provider Future on success, failure,
  cancellation, or timeout; external Model brokers must observe it or
  guarantee cancellation-safe cleanup when their Future is dropped.
- Network model streams use an exact media type and frame schema, decode
  incrementally across arbitrary transport chunks, reject empty/unknown/late
  frames, and require exactly one final typed response. A provisional sink
  failure may drop deltas but cannot replace or invalidate that final response.
- Private model-gateway trust roots are explicit, byte/count bounded, parsed
  before client construction, absent from debug output, and exclusive: native
  or WebPKI roots must not be merged back into that client.
- Model-gateway mTLS identity material enters only as a bounded
  non-serializable `SecretValue`, is parsed while constructing the pooled
  client, and never enters State, protocol, Model configuration, or debug
  output. Rotation constructs a new client rather than mutating live trust.
- Evaluation targets receive an engine-owned cancellation signal. Case and
  grader work has independent concurrency and time bounds; task panics become
  content-free per-item errors, never batch failure or leaked panic payload.
- Materialized Evaluation batches bound case count, encoded input, captured
  execution size, grader count, and concurrency. Result ordering follows
  stable identities rather than nondeterministic task completion.
- Evaluation suite, baseline, and report values are revalidated at execution
  and comparison boundaries even when their public Rust type was deserialized
  or assembled without a validating constructor.
- Evaluation artifact roots carry the exact current format coordinate. Every
  Grade and baseline requirement binds both Grader name and trust-bearing
  origin; missing legacy origin is never inferred as built-in trust.
- `yh eval-smoke` is the minimum executable behavioral regression gate. Its
  versioned suite and baseline must change in the same review as an intentional
  contract change; weakening a threshold solely to accept a regression is not
  a fix.

## Required change gates

Every implementation change must pass:

```bash
cargo +1.88.0 fmt --all --check
cargo +1.88.0 clippy --locked --all-targets --all-features -- -D warnings
cargo +1.88.0 test --locked --all-targets --no-default-features
cargo +1.88.0 test --locked --all-targets --all-features
cargo +1.88.0 run --locked --example embedded
cargo +1.88.0 run --locked --example orchestrated
cargo +1.88.0 run --locked -- eval-smoke
RUSTDOCFLAGS="-D warnings" cargo +1.88.0 doc --locked --no-deps --all-features
```

The declared MSRV is an executable gate. Dependency metadata is not accepted as
proof that a transitive build script works on the minimum compiler.

Changes to a real provider or transport also require its isolated integration
test. Agent Memory Hub uses:

```bash
YH_AMH_SERVER=/path/to/agent-memory-hub/agent_runtime_kit/mcp/server.sh \
  cargo test --test agent_memory_hub_mcp -- --ignored
```

Integration tests must use isolated data roots and clean them on success,
failure, or panic.

Pinned public Skill acquisition has a separate ignored live gate:

```bash
YH_HTTPS_SKILL_ENDPOINT=https://registry.example/skill.json \
YH_HTTPS_SKILL_NAME=example \
YH_HTTPS_SKILL_VERSION=1.0.0 \
YH_HTTPS_SKILL_SHA256=<64-lowercase-hex> \
YH_HTTPS_SKILL_PUBLISHER_KEY_ID=publisher \
YH_HTTPS_SKILL_PUBLISHER_KEY_HEX=<32-byte-public-key-hex> \
cargo test --all-features --test https_skill_source -- --ignored
```

The HTTPS model gateway has ordinary JSON and bounded NDJSON live gates:

```bash
YH_HTTPS_MODEL_ENDPOINT=https://gateway.example/v1/complete \
YH_HTTPS_MODEL_TOKEN=<token> \
cargo test --all-features --test https_json_model -- --ignored
```

Any durable schema change additionally requires the fixture, crash, migration,
backup, rollback, mixed-version, and maximum-size evidence defined in
[`compatibility.md`](compatibility.md). Until that evidence exists, the writer
schema must not advance.

## Reliability and availability

- Every external operation has a bounded timeout.
- A Runtime has finite concurrent Turn admission. Overload is rejected before
  `TurnStarted` and is a distinct retryable error, not a provider failure.
- External capability future construction, polling, and drop are panic-isolated.
  Panic payloads never become State, protocol, or Observability content; the
  phase becomes a typed failed outcome.
- Every detached protocol Turn has a task supervisor. A worker panic or
  premature task stop must replace `running` with a content-free terminal
  failure so clients can observe and release the Operation.
- Protocol host shutdown is a one-way drain: reject new Turns, cancel accepted
  Operations, wait within an explicit deadline, then spend only its remainder
  on Runtime-owned maintenance. Report Operation remainder and background
  completion independently. Never call a forced task stop successful
  cancellation; unresolved durable running Turns require exclusive recovery
  evidence.
- Process-local Operation retention has a safe finite default and a validated
  operator maximum. Capacity never silently evicts running or unobserved
  terminal results; clients explicitly forget terminal records.
- Durable event pages are bounded by count and encoded bytes. When the byte
  ceiling wins, `has_more` and `next_after_sequence` preserve lossless cursor
  progress.
- Connection failure, execution failure, provider-declared error, malformed
  response, and degraded result are different outcomes.
- An uncertain side effect is not retried unless the external contract supplies
  an idempotency mechanism.
- Explicit cancellation, deadline expiry, capability failure, and abandoned
  process recovery remain distinct terminal outcomes.
- Policy evaluation and approval settlement are separately correlated and
  persisted; `ask` is denied when no approval handler is configured.
- Durable approval requests use optimistic revision settlement. Recovery
  orphans requests from interrupted Turns; it never treats a late approval as
  authority to replay an uncertain Tool continuation.
- Pending approval admission reserves enough encoded capacity for every
  supported terminal form. In-memory transitions validate a candidate copy and
  publish it atomically; a returned error must leave the prior record intact.
- Pending approval reads expose only the deterministic oldest 16-record
  working window. Implementations bound record materialization before returning
  or serializing the page; a count-only limit applied after cloning is
  insufficient.
- Multi-record approval recovery selects a bounded identity set first and
  materializes only one record body at a time inside the same atomic
  transaction. A per-Turn count ceiling does not justify aggregate body
  allocation.
- State append and terminal settlement are not cancelled merely to satisfy a
  Turn deadline; durable truth takes precedence over early return.
- Interrupted Turns are recovered explicitly and non-idempotent tools are not
  replayed automatically.
- Recovery is a takeover operation. A host must establish exclusive Thread
  ownership and confirm the former worker stopped; ordinary Turn startup must
  reject a durable running Turn instead of declaring it interrupted.
- Provider degradation is recorded in State and follows configured fail-open or
  fail-turn behavior.
- Process-local ownership is not represented as distributed high availability;
  leases and multi-process coordination require their own proven slice.
- A Task lease is never treated as safe ownership without a unique fencing
  token and atomic coordinator mutation.
- Task executors receive cooperative cancellation and cannot settle their own
  claims. The Orchestrator cancels before Future release, reloads the current
  graph, and requires the exact unexpired lease before persistence.
- Task executor messaging goes through a coordinator-backed Mailbox. Inbox
  reads are cursor-, count-, and byte-bounded; sends revalidate the exact
  current lease and retry only explicit CAS conflicts.
- A filesystem Task never enters its executor under the default Workspace
  Provider. Installed providers are metadata- and Future-panic-isolated,
  return a lease for the exact graph/task/fencing attempt, and expose no
  cleanup token to the executor.
- Workspace preparation is part of the Task deadline. Cancellation gives
  partial preparation a bounded drain; release is separately bounded, runs
  after executor cancellation and before Task settlement, and may replace
  apparent executor success with a durable failure.
- Recursive workspace cleanup is limited to a canonical managed direct-child
  container with provider-owned identity evidence outside the nested executor
  root. Symlinks, replacement, escaped paths, missing/mismatched markers, and
  oversized marker bodies fail closed.
- Directory and Git Worktree provisioning are not called sandboxes.
  `SharedReadOnly` and untrusted isolated execution require independent
  Process Broker or mount enforcement. Git Worktrees use a full immutable
  object ID, an absolute executable, shell-free arguments, bounded output and
  time, and explicit repository/worktree write authority.
- Orchestration concurrency, Task timeout, cleanup budget, lease duration,
  polling, claim batches, and CAS retries are finite. The lease must outlive
  the Task timeout plus cleanup budget. A persistence conflict may recompute
  settlement against the same lease but must never re-invoke uncertain work.
- Task claims use a finite batch window and validate every fallible input,
  deadline, and attempt-capacity condition before releasing leases or
  propagating graph state. A returned preflight error leaves the graph
  unchanged.
- Task Graph persistence uses an observed revision compare-and-swap. A conflict
  is returned to the caller for reload and semantic recomputation; stale
  mutations are never retried blindly.
- The Task Graph domain, not only its store, owns aggregate durable capacity.
  Construction and deserialization establish a conservative charge; every
  status/message mutation preflights its complete delta and publishes only a
  persistable result. The Coordinator retains an exact final encoding check.
- Process-shared SQLite coordination is not described as multi-node consensus
  or distributed high availability.
- Approval and Task Graph SQLite reads apply their declared payload and
  identity bounds before text materialization; index/body and graph invariants
  are still revalidated after decoding.
- Every State transition uses an Event Store compare-and-append against the
  observed stream version and recovery charge. Both metadata values and the
  event must change atomically; a stale writer must fail closed.
- State validates mutation identities and encoded event size before calling an
  Event Store, then revalidates append/read results, ordering, Thread ownership,
  and requested page capacity before trusting them.
- State snapshots are disposable caches: validate their digest and projected
  invariants, reread their journal anchor, and replay the authoritative tail.
  Invalid snapshots fall back to bounded full replay.
- Automatic snapshot work starts only from terminal Turn settlement, has an
  explicit cadence and concurrency ceiling, never queues when saturated, and
  cannot reverse an authoritative append. Hosts inspect content-free
  maintenance counters; protocol hosts drain accepted workers through the
  Runtime-wide shutdown contract rather than a parallel lifecycle.
- State capacity reports are derived from validated journal count and exact
  serialized-plus-overhead recovery charge. Warning levels are planning
  signals, never estimates of process heap, disk capacity, or authority to
  archive data.
- The final Thread event slot is terminal-settlement-only. A non-terminal
  append must never consume the last durable slot needed to close a running
  Turn.
- The final Thread recovery-byte budget is terminal-settlement-only. Store
  reads must page before aggregate materialization, and a non-terminal append
  must not consume the bytes needed for the largest valid terminal envelope.
- Runtime checks its minimum viable event budget before `TurnStarted`. If a
  later Item append fails, it attempts terminal settlement immediately; a
  State recording error must not silently leave a Turn running.
- A Model-generated Tool call identity is unique within one Turn. Reuse fails
  the Turn before registry resolution, Policy, or another external effect.
- Compensation is explicit and Tool-specific. It reconstructs both the
  original successful effect and the current authorization chain from
  authoritative State, never trusts model-supplied copies of effect data, and
  never runs as an implicit verifier or cancellation hook.
- Every retry of one compensation target uses the same provider idempotency
  key. A successful durable settlement is replayed from State; a different key
  for the same target fails closed.
- Observability is not a control-plane dependency. Production exporters require
  bounded queues and failure isolation before they may be registered.

## Performance

- Persistent connections are preferred for recurring provider calls.
- Network model connections are pooled behind a hard concurrency bound; DNS,
  connect, credential resolution, send, and response reads remain inside one
  total operation deadline.
- Blocking SQLite work runs outside Tokio's async executor.
- Evaluation shares immutable samples by `Arc`; parallel graders do not clone
  captured Turn histories.
- Context budgeting operates on provider-reported packs without copying or
  truncating canonical bodies.
- Conversation budgeting drops complete oldest Turns; partial Item truncation
  is forbidden because it can separate calls, results, candidates, and
  Verification feedback.
- External Skill publisher and transparency-log trust is checked at
  registration and every governed use. Effective revocations are immutable,
  supplied receipts are always signature-verified, and copied instructions are
  never treated as live after their trust check fails.
- Remote Skill acquisition requires an operator URL plus exact identity and
  digest pins, keeps network indirection disabled, bounds response and decoded
  package memory independently, and completes all trust checks before Registry
  mutation.
- Performance claims require a reproducible benchmark, workload, baseline, and
  regression threshold. No unmeasured “high performance” claim is accepted.

The first reproducible State workload is:

```bash
cargo run --release --bin yh-state-bench
```

`YH_BENCH_EVENTS` and `YH_BENCH_SAMPLES` control its bounded workload. It uses
isolated SQLite databases with production durability settings and reports the
median append throughput and full projection latency as JSON.
`YH_BENCH_MAX_APPEND_MS` and `YH_BENCH_MAX_PROJECT_MS` make a calibrated
environment fail on regression. `YH_BENCH_MAX_SNAPSHOT_LOAD_MS` independently
gates journal-anchored snapshot recovery.

## Defect policy

“Zero bugs” is not a credible permanent software property. The enforceable
release condition is:

- no known critical or high-severity correctness/security defect;
- all required deterministic and integration gates pass;
- limitations and unverified paths are explicit;
- regressions receive a reproducing test before the fix is accepted;
- completion claims include the verification evidence used.
