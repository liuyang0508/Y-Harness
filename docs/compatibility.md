# Compatibility and migration policy

This policy describes the current pre-1.0 contract. It is intentionally strict:
Y-Harness fails closed on unknown durable schemas and does not claim rolling
upgrade support that has not been tested.

## Version coordinates

| Surface | Current coordinate | Negotiation |
|---|---:|---|
| Rust crate | Cargo `0.1.0` | Cargo SemVer |
| Optional TUI package | Cargo `0.1.0` | Cargo SemVer plus exact client protocol |
| Service configuration | `1` | strict root `schema_version`; no permissive fallback |
| Client protocol | `"30"` | exact `Initialize` request/response |
| State events | `14` | per-event durable envelope; reads schemas 1 through 14 |
| State snapshots | `14` | cache body; incompatible caches are discarded |
| Approval Inbox | `3` | per-record durable body after explicit migration |
| Task Coordinator | `3` | per-graph SQLite schema column |
| Workflow Coordinator | `1` | store metadata plus per-Run SQLite schema column |
| Human Handoff Coordinator | `1` | store metadata plus per-Handoff SQLite schema column |
| Effect Ledger | `1` | store metadata plus per-Effect SQLite schema column |
| Governed Effect Executor API | `1` | exact embedded Connector descriptor; not a durable or client-protocol coordinate |
| Governed Effect Reconciler API | `1` | exact embedded read-only Connector descriptor; not a durable or client-protocol coordinate |
| JSON Effect Connector protocol | `1` | exact stdin/stdout process envelopes; no negotiation or fallback |
| Temporal Driver API | `2` | exact embedded Rust API; not a durable or client-protocol coordinate |
| Memory Provider API | `1` | exact descriptor registration |
| Token Counter API | `1` | exact descriptor registration |
| Conversation Compactor API | `1` | exact descriptor registration |
| Thread handoff request format | `1` | canonical bounded summarizer-input envelope |
| Thread archive format | `4` | exact bounded archive root plus digest |
| Evaluation artifacts | `2` | exact self-described suite/baseline/report roots; not a client-protocol surface |
| Workspace Provider API | `"1"` | exact embedded provider installation and `Initialize` coordinate |
| Secret Provider API | `3` | exact descriptor registration, typed use context, and trusted-authority resolution |
| Skill package API | `"1"` | exact manifest validation |
| HTTPS model gateway API | `"7"` | exact request/response header |

`Initialize` advertises the engine version and Runtime-facing durable/API
coordinates above, including the Workspace Provider API implemented by
orchestration hosts. The embedded-only Temporal Driver API and self-described
Evaluation artifacts are not client-protocol surfaces. Capabilities are
separately negotiated; a disabled capability is not implied by its schema
coordinate.

Service configuration schema 1 is bounded to 65,536 bytes, rejects unknown
fields, and keeps credentials as environment-backed secret references. Its
optional service-assembly fields add direct OpenAI Responses, brokered
shell-free JSON Models and Tools, exact-selected stdio or authenticated HTTPS
JSON-response MCP Tools, Agent Memory Hub Context, and explicitly activated
project-local declarative Skills. The `json_command` Model variant is additive,
uses the existing process shape, and participates in the existing single
Model or catalog/route forms without changing their meaning. The additive
`https_mcp_servers` list preserves the existing
`mcp_servers` stdio shape; identities collide across both lists and disabled
entries acquire no process, network, Secret, Tool, or Memory authority. Skills
may add separate signed
`external_package_files` plus publisher/log public keys, validity,
transparency, and immutable revocation policy; the existing unsigned
`package_files` keep their operator-trusted meaning. A trust-only staged object
may leave package and activation lists empty. The configuration also supports
a mutually exclusive `models` catalog plus explicit `model_route` alternative
to the existing single `model`; no existing field changes meaning. MCP entries
may be explicitly disabled and may pin the configured command file by SHA-256.
Multi-Model routes may add a default-disabled `timeout_cooldown_ms` from
1–86,400,000 milliseconds; Runtime-proven attempt timeouts are tried after
ready candidates while cooling Models remain last-resort fallbacks.
They may independently add a default-disabled `retry` object with 1–8
additional calls and 1–60,000 millisecond initial/maximum delays. Only typed
rate-limit, overload, server, and transport failures are eligible, and retries
share the existing candidate deadline.
The additive defaulted `max_model_attempts_per_step` service field accepts
1–144 and defaults to 16. It bounds all Runtime `LanguageModel` invocations in
one Agent Loop step across retry and Route fallback. The Rust API additively
exposes the same builder, constants, derived Turn-bound query, and
`HarnessError::MaxModelAttempts`; exhaustive matches on the public pre-1.0
error enum require a source update. State, Client Protocol, snapshots, and
Model Gateway formats do not change. See
[ADR 0114](adr/0114-bounded-runtime-model-attempts-per-step.md).
The Rust API additively exposes `HarnessError::ModelProvider` and bounded
`ModelProviderFailure` evidence, plus `ModelRetryPolicy`. Existing providers
may continue returning `HarnessError::Model(String)`; only exhaustive matches
on the public pre-1.0 error enum require a source update. Existing Observer
implementations only read the expanded `PhaseObservation`; external code that
directly constructs that public pre-1.0 struct must initialize its four new
optional Provider/retry fields. Those fields use Serde defaults and do not
advance State, Protocol, Model Gateway, snapshot, or service-configuration
coordinates. See
[ADR 0100](adr/0100-typed-model-provider-failure-evidence.md) and
[ADR 0101](adr/0101-bounded-typed-model-retry-policy.md), plus
[ADR 0103](adr/0103-bounded-authenticated-https-mcp-json-transport.md) and
[ADR 0104](adr/0104-configured-brokered-json-command-models.md). JSON-command
Models still default to the original bare `output_v1` wire. The additive
`protocol: "settlement_v1"` selector requires the strict completed/failed
envelope and can preserve Provider evidence or typed failure facts; it is never
auto-detected. This changes no existing configuration meaning, State,
Protocol, or Model Gateway coordinate. See
[ADR 0108](adr/0108-versioned-json-command-model-settlement.md). The optional
strict `conversation` object and its brokered JSON-command compactor are also
additive service-schema-1 configuration; no State, Protocol, or Model Gateway
shape changes. See
[ADR 0105](adr/0105-configured-brokered-conversation-compaction.md).
The optional `verifiers` list is another additive service-schema-1 field and
reuses the existing `VerificationResult` State/Protocol shape. The public
pre-1.0 `VerificationRequest` now carries the exact in-process
`CancellationToken`; external implementations that construct or destructure
that struct must add the field, and its previous `PartialEq` implementation was
removed because comparing requests while ignoring cancellation identity would
be misleading. The JSON-command wire uses a separate cancellation-free type.
See [ADR 0106](adr/0106-configured-brokered-verification.md).
The optional strict `evaluation` object adds process Graders plus independent
case/Grader concurrency and timeout controls to service schema 1. It is
constructed by `doctor` and `eval`, not ordinary `serve`. The public pre-1.0
`Grader::grade` contract now receives a distinct engine-owned
`CancellationToken`, and `ExecutionPhase` adds `Evaluation`; external Grader
implementations and exhaustive phase matches require a source update. The
format-2 suite, baseline, report, State, Protocol, and Model Gateway shapes do
not change. Evaluation-phase cancellation is process-local and never becomes
Turn stop evidence. See
[ADR 0107](adr/0107-configured-brokered-evaluation-graders.md).
The defaulted `max_parallel_tool_calls` field is bounded to 1–64, and JSON
Tools may explicitly opt into `parallel_safe`; absent declarations remain
sequential.
The public pre-1.0 `Tool` trait adds the defaulted
`cancellation_settlement_timeout` method, and `RegisteredTool` exposes its
registration-time-frozen value. Existing `Tool` implementations require no
method addition; external `RegisteredTool` struct literals must add the field.
The limit is ten seconds and the default is zero. `ToolContext::cancellation`
is now a per-call signal derived from both Turn cancellation and deadline, and
is closed when that Tool Future settles; implementations must not treat its
identity as a Turn-global token. `McpClient` likewise adds the defaulted
`call_tool_with_cancellation` method; existing implementations remain
source-compatible, while stateful implementations should override it for
session cleanup. These changes do not alter State, Client Protocol, service
configuration, or MCP wire coordinates. See
[ADR 0115](adr/0115-bounded-mcp-tool-cancellation-settlement.md).
The public pre-1.0 `TurnExecutionOptions` replaces its approval-only actor
field with `AuthorityContext`; `PolicyEngine::authorize` receives that trusted
authority, and `ToolContext` carries it into execution. Existing custom Policy
implementations and direct options/context struct literals require source
updates. `ProtocolAuthorizer` gains a defaulted resolver, so existing
implementations preserve their transport actor without a tenant. No
caller-authored identity field, State event, Approval record, Client Protocol,
or service configuration coordinate changes. Tenant-scoped approvals fail
closed until a later durable schema binds their tenant evidence. See
[ADR 0116](adr/0116-trusted-turn-authority-context.md).
State schema 12 now binds optional tenant ownership to the authoritative
Thread creation event, Protocol v20 fences Thread and Operation access by the
resolved authority, and Thread archive format 2 preserves the new projection.
Legacy Threads migrate as unscoped rather than receiving guessed ownership.
At Protocol v20, Approval and Task protocol surfaces still fail closed for
tenant-scoped authorities until their own durable schemas advance. See
[ADR 0117](adr/0117-durable-thread-tenant-ownership.md).
The Rust pre-1.0 API adds tenant-aware State/Runtime methods and
`EventStore::thread_accessible`; custom stores that override
`thread_summaries_page` must accept the tenant filter. `Thread` construction
now keeps ownership engine-controlled and exposes it through `tenant_id()`,
so external direct struct literals require a source update.
Approval Inbox schema 3 binds immutable optional tenant ownership to every
record, and Protocol v21 enables exact-tenant Approval discovery and
settlement while tenant-scoped Task access remains fail-closed. Schema-2
records migrate as unscoped; no Thread or actor relationship is used to infer
ownership. The pre-1.0 `ApprovalInbox` and `ApprovalHandler` APIs add
authority-aware methods, and custom tenant-aware handlers must durably
preserve the boundary. See
[ADR 0118](adr/0118-durable-approval-tenant-ownership.md).
Task Graph schema 2 binds immutable optional tenant ownership to the complete
Graph aggregate, and Protocol v22 enables exact-tenant Graph administration
and worker lifecycle commands. The `(tenant, graph_id)` key gives each tenant
an independent caller-selected identity namespace. Schema-1 Graphs migrate as
unscoped; no Thread, worker, path, or deployment relationship is used to infer
ownership. The pre-1.0 `TaskCoordinator` API adds authority-aware methods. See
[ADR 0119](adr/0119-durable-task-graph-tenant-ownership.md).
Secret Provider API 2 adds trusted-authority resolution. The default method
preserves existing unscoped providers but rejects tenant-scoped resolution
until the provider explicitly implements it. The built-in tenant environment
provider requires an exact tenant/reference mapping and has no global
fallback. `ModelRequest` now carries trusted authority in process, so external
Rust struct literals must initialize it, but Serde excludes it from Model
Provider and JSON-command payloads. Direct HTTPS gateway and OpenAI adapters
resolve credentials with the Turn authority. `McpClient` similarly adds a
defaulted context-aware call method; current shared sessions reject
tenant-scoped calls unless a custom implementation proves session partitioning.
Protocol v23 advertises Secret API 2; State, Approval, Task, service
configuration, and Model Gateway coordinates are unchanged. See
[ADR 0120](adr/0120-authority-aware-secret-resolution.md).
Service schema 1 now additively accepts an optional
`authority.type = "local_process_tenant"` with one validated `tenant_id`.
Omission retains the prior unscoped service meaning. The configured tenant is
applied to local stdio Protocol authority, configured Evaluation, State
archive commands, and direct OpenAI/HTTPS Model environment Secret resolution.
Enabled configured MCP servers fail validation in this mode because their
shared sessions are not tenant-partitioned. Changing this field does not
migrate existing unscoped State, Approval, or Task ownership; such records
remain inaccessible under the exact tenant fence. No durable, Protocol,
Model-Gateway, archive, or service-schema coordinate advances. See
[ADR 0125](adr/0125-fixed-tenant-reference-service-authority.md).
The additive `Tool::execute_with_evidence` compatibility method lets an
in-process Connector return bounded source-system claims while ordinary Tools
retain their existing implementation. Runtime binds each claim to the exact
registered Tool/origin, trusted actor/tenant, and output SHA-256; State schema
14 stores it atomically with `ToolResult`, revalidates its execution chain, and
excludes it from Model Context. Failed results cannot retain evidence. Thread
archive format 4 preserves the record and refuses cross-tenant authority
rebinding. Protocol v26 advertises State/snapshot schema 14 but exposes no
Connector-evidence authoring command. See
[ADR 0126](adr/0126-runtime-bound-connector-evidence.md).
Protocol v27 adds the optional Workflow Engine surface and advertises Workflow
store schema 1. The additive Rust contract separates a revisioned Workflow Run
from its linked Task Graph, uses digest-bound idempotent commands, exact
signal/timer wait fencing, trusted server application time, safe-boundary
definition migration, and tenant-partitioned Memory/SQLite persistence.
`WorkflowEngine` verifies the linked same-tenant Task Graph at creation and
requires every Task to be complete before successful Workflow completion.
Schema 1 is the first Workflow store, so there is no legacy migration or
rolling mixed-version writer support. See
[ADR 0127](adr/0127-durable-fenced-workflow-runs.md).
Protocol v28 adds the optional Human Handoff surface and advertises Human
Handoff store schema 1. A Handoff refers to one same-tenant Thread or Workflow
Run, but owns an independent revisioned lifecycle: queued, lease-fenced claim,
resolved, or cancelled. Creation and mutation identities are bound to the
trusted actor and complete typed payload. Queue order and its cursor are
priority-descending, request-time-ascending, then identity-ascending.
Memory/SQLite implementations tenant-partition identity and validate persisted
projections against the aggregate. Schema 1 is the first Handoff store, so no
legacy migration or mixed-version writer support exists. Protocol permissions
are command-specific; composing the schema coordinate alone does not enable
the surface. See
[ADR 0128](adr/0128-durable-lease-fenced-human-handoff.md).
Temporal Driver API 1 additively composed installed Workflow and Human
Handoff Engines behind one bounded host-invoked tick. The host supplies trusted
Unix time and exact authority; each source visits at most 1–256 authoritative
tenant-local identities, returns a disposable continuation, and applies only
the existing revision- and fence-checked `wake_due` or `expire_claim`
commands. Custom coordinators remain source-compatible because their additive
due-scan methods fail closed by default; implemented pages are revalidated for
bounds, cursor progress, ordering, tenant, revision, fence, and eligibility
before mutation. `MAX_TEMPORAL_SCAN_LIMIT` exposes the same 256-record bound to
hosts. The public pre-1.0 `HarnessError` enum adds `Temporal`, so exhaustive
external matches require a source update.
This API adds no Core background task, scheduler database, durable schema,
`Initialize` coordinate, or Protocol v28 command. Reference-service config
schema 1 now accepts an optional strict `temporal` host policy; omission
preserves disabled behavior. See
[ADR 0129](adr/0129-host-driven-bounded-temporal-driver.md).
Reference-host lifecycle semantics are specified by
[ADR 0130](adr/0130-optional-reference-service-temporal-lifecycle.md).
Client protocol `30` preserves protocol-v29 commands, framing, authority,
permissions, durable coordinates, and request/response ceilings. It advances
the advertised Secret Provider coordinate to API 3 for typed Agent-Turn,
Governed-Effect, and bounded service-use contexts. No Secret value or new
Secret-bearing protocol command is introduced. Protocol-v29 clients fail exact
initialization rather than assume every credential belongs to a Turn.

Protocol v29 adds the optional schema-1 Effect Ledger surface. One immutable
intent binds tenant, capability, operation, idempotency key, bounded input,
request digest, actor, and server time before execution. A finite worker lease
owns one exact positive attempt. Applied, authoritatively not-applied, unknown,
and reconciliation settlements are actor/content bound and revision fenced.
Lease expiration moves to `unknown`, never back to pending; only explicit
not-applied evidence may select a later attempt. Memory and SQLite
Coordinators share exact idempotency uniqueness, tenant fencing, bounded
identity paging, and expired-lease scans. Effect schema 1 is a new independent
store with no inferred migration or mixed-version writer support.

Temporal Driver API 2 additively composes an optional Effect Engine. It scans
the same bounded tenant-local identity space and applies only the existing
`expire_lease` command after complete cross-source validation. The cursor and
report add an Effect coordinate, so exhaustive Rust construction requires a
source update. The API still owns no background task or scheduler database.
See [ADR 0133](adr/0133-durable-effect-ledger.md).

Governed Effect Executor API 1 is an additive embedded Rust surface over the
same schema-1 Ledger. Connector descriptors must declare the exact API
coordinate, capability, explicit operation set, and idempotency contract.
The public pre-1.0 crate adds Executor, Connector, Policy, Clock, request,
outcome, configuration, and report types, so wildcard imports or exhaustive
external construction may require a source update. It adds no durable schema,
Protocol v29 command, configuration field, or Core background task. See
[ADR 0134](adr/0134-host-driven-governed-effect-executor.md).

Governed Effect Reconciler API 1 is another additive embedded Rust surface over
schema-1 Ledger reconciliation commands. Its distinct Connector descriptor
requires an exact capability, explicit operation set, API coordinate, trust
origin, and authoritative read-only lookup contract. The public pre-1.0 crate
adds Reconciler, Connector, Policy, Clock, request, outcome, configuration, and
report types. It adds no Effect schema, Protocol v29 command, configuration
field, durable query lease, or Core background task. See
[ADR 0135](adr/0135-host-driven-authoritative-effect-reconciliation.md).

JSON Effect Connector protocol 1 adds strict cancellation-free process request
and response envelopes plus brokered execution/reconciliation adapters.
Requests carry immutable Effect/Authority/attempt/lease evidence; every
response carries the same exact protocol coordinate. `ExecutionPhase` adds
`Effect`, so exhaustive external matches require a source update. This process
wire adds no Effect schema, client Protocol v29 command, service configuration
field, or Core background task. See
[ADR 0136](adr/0136-versioned-brokered-json-effect-connectors.md).

Reference-service config schema 1 additively accepts an optional strict
`effect_consumer` object. Execution and reconciliation are independently
optional and require separate exact Connector registries, explicit non-empty
capability/operation allowlists, bounded cadence/backoff, and their existing
bounded Executor/Reconciler policies. Every Connector adds a required
trust-bearing `origin_id`; execution additionally declares idempotency, while
reconciliation declares the authoritative read-only contract. Omission retains
the previous no-consumer behavior. This adds no Effect schema, Protocol v29
command, or Core background task. See
[ADR 0137](adr/0137-optional-reference-service-effect-consumer.md).

The public pre-1.0 `ProcessBrokerDescriptor` adds
`executable_integrity`, with serde-default `unmeasured` compatibility for
historical serialized descriptors; external Rust struct literals must add the
field. `DigestLockedProcessBroker` reports `dispatch_sha256` and remeasures one
exact regular command path before each dispatch under the request's existing
cancellation and total timeout. Shared one-shot service process configuration
additively accepts `command_sha256`; Effect consumer Connectors now require it.
This changes no durable schema, Protocol v29, or JSON Effect wire coordinate
and does not claim an atomic OS exec measurement. See
[ADR 0138](adr/0138-dispatch-time-effect-command-digest-locks.md).

Secret Provider API 3 replaces the mandatory Thread/Turn fields in the public
pre-1.0 `SecretRequest` with a typed `SecretUseContext` covering Agent Turns,
Governed Effect attempts, and bounded service operations. External Provider
implementations that construct or inspect requests must migrate and advertise
API 3; `resolve_as` remains the only actor/tenant authority boundary. The exact
client protocol advances from v29 to v30 because `initialize` now advertises
Secret API 3. No Secret value, new command, durable schema, Model Gateway
payload, or JSON Effect envelope is added.

The public pre-1.0 `ProcessRequest` adds `secret_environment`; external struct
literals must initialize it. Values are non-cloneable `SecretValue` buffers and
the request remains non-serializable. Service-schema-1 JSON process config
additively accepts `secret_environment`, but only Effect Connectors admit it;
other adapters fail closed rather than inventing an authority context.
Omission preserves the previous behavior. See
[ADR 0139](adr/0139-typed-secret-use-and-effect-credential-custody.md).

Credential-bearing JSON Effect adapters now require their frozen Broker
descriptor to advertise `dispatch_sha256`. They preflight the exact command
before Provider resolution, then retain the Broker's second measurement before
child entry; both checks, Provider work, queueing, and execution share the
configured process deadline. An external pre-1.0 Rust caller that attaches
`EffectSecretEnvironment` to an unmeasured Broker now fails configuration.
Credential-free adapters are unchanged. This adds no durable schema, client
command, Protocol-v30 field, JSON Effect envelope, or service-schema
coordinate, and it does not claim atomic executable-to-`exec` binding. See
[ADR 0140](adr/0140-secret-gated-effect-command-integrity-preflight.md).

The public pre-1.0 `TurnExecutionOptions` now adds an optional trusted
`ExecutionBinding`. State schema 13 records at most one binding per Turn,
requires its tenant to equal authoritative Thread ownership, excludes it from
Model Context, and requires an exact match on approval resume. Thread archive
format 3 preserves the evidence and refuses cross-tenant rebinding when any
binding is present. Protocol v24 advertises the new State/snapshot coordinates
but does not expose a binding-authoring command; archive format is a separate
Rust/CLI coordinate. See
[ADR 0122](adr/0122-durable-turn-execution-binding.md).
Task Graph schema 3 adds append-only `TaskAttemptBinding` evidence and the
optional exact binding on `TaskClaim`. `Orchestrator::with_execution_context`
adds trusted tenant-aware execution; current Protocol workers cannot author a
binding or take over a Task after it enters bound mode. Protocol v25
advertises Task schema 3. Schema-1/schema-2 SQLite stores require the explicit
backup-first migration; schema-2 ownership is preserved without inventing
historical attempt evidence. See
[ADR 0123](adr/0123-durable-task-attempt-execution-binding.md).
The optional Domain Pack crate adds an authorization port, exact action and
request types, bounded reference roles, an exact actor/tenant grant model, an
authorized Store adapter, and a `Forbidden` error. These are additive Rust
control-plane APIs. They do not alter Pack format/store schema 1, State schema
14, Task schema 3, Workflow schema 1, Human Handoff schema 1, or Protocol v28.
See
[ADR 0124](adr/0124-exact-domain-pack-role-authorization.md).
External process launch is explicit, child environments are
copied only by configured host-variable name, and MCP catalog discovery alone
grants no Tool authority. `data_directory`, project Skill package files, and an
optional exclusive CA file must remain inside the configuration project root.
A future incompatible configuration shape requires a new root schema coordinate
and an explicit operator migration; it is never guessed from fields.

Skill API `1` keeps the original package digest and publisher-signature bytes.
Signed packages may add an optional signed transparency receipt; publisher
policy decides whether absence is accepted. This does not change how an
existing receipt-free package is decoded or how its publisher signature is
verified. External project storage preserves that existing signed envelope
verbatim in canonical JSON; no Skill API coordinate changes. See
[ADR 0102](adr/0102-governed-signed-external-skill-lifecycle.md).

HTTPS model gateway API `7` preserves API 6's transport rules and adds the
`invocation` Context-source shape with source/reference and exact source/body
digests. Gateways must keep this shape, Memory, and conversation summaries at
ordinary caller/evidence authority; only digest-pinned Skill blocks are
Harness instructions. API-6 peers must fail exact negotiation rather than
silently elevate caller context.

HTTPS model gateway API `6` preserves API 5's transport rules and adds the
ordered `tool_calls` Model output plus optional Tool-call batch metadata on
replayed request Items. API-5 peers must fail exact header negotiation rather
than flatten or reject a decision after partial interpretation.

HTTPS model gateway API `5` preserves API 4's transport rules and adds optional
bounded `provider_model` evidence to `ModelResponse`. Gateways must preserve a
Provider-reported settled Model when available and must not copy the requested
or registered Model identity into this field.

HTTPS model gateway API `4` preserves API 3's transport rules and replaces
`ModelUsage.cost_microusd` with exact integer `cost_usd_ticks` at ten billion
ticks per USD. Gateways must omit unavailable, partial, or inexact cost rather
than round it, and API-3 bodies are not reinterpreted under the new unit.

HTTPS model gateway API `3` preserves API 2's transport rules and adds the
optional bounded `continuation` field to `ModelResponse` plus the
`provider_continuation` request Item. Gateways must treat the capsule as
provider-formatted non-executable data and return only formats they can safely
replay.

HTTPS model gateway API `2` preserves API 1's JSON/NDJSON settlement rules and
adds the opt-in `conversation_summary` Context-source shape. A gateway sees that
shape only when its host selected a semantic compactor, but the exact request
contract still advances rather than relying on permissive enum decoding.

HTTPS model gateway API `1` preserved the original JSON response whenever the
request omits `x-y-harness-model-stream`. A request with that header set to
`1` explicitly negotiates the API-1 NDJSON media mode; peers that do not support
it fail the exact content-type check rather than silently changing semantics.

Client protocol `28` preserves protocol-v27 framing, authority, ceilings, and
existing State/Approval/Task/Workflow commands. It adds the optional Human
Handoff schema-1 coordinate and command-specific lifecycle surface. A v28
client must still check the advertised capabilities; the coordinate does not
imply that a host composed a Handoff Engine. Protocol-v27 clients fail exact
initialization rather than silently ignore ownership-transfer state.

Client protocol `27` preserves protocol-v26 framing, authority, ceilings, and
existing State/Approval/Task commands. It adds the optional Workflow schema-1
coordinate and command-specific lifecycle surface.

Client protocol `26` preserves protocol-v25 commands, framing, authority,
permissions, request/response ceilings, and Task Graph schema 3. It advances
advertised State event/snapshot coordinates to 14. Thread archive format
independently advances to 4. Connector evidence is durable output produced
only by an in-process evidence-aware Tool and bound by Runtime; protocol
callers can observe it but cannot author it. Protocol-v25 clients must fail
exact initialization rather than assume schema-13 projections can preserve
schema-14 Items.

Client protocol `24` preserves protocol-v23 commands, framing, authority,
permissions, and request/response ceilings. It advances advertised State event
and snapshot coordinates to 13. Thread archive format independently advances
to 3. The new execution binding is trusted embedded-host input and durable
output evidence; protocol callers can observe State but cannot author that field.
Protocol-v23 clients must fail exact initialization rather than assume
schema-12 projections can preserve schema-13 Items.

Client protocol `23` preserves protocol-v22 commands, framing, authority,
tenant-owned State/Approval/Task projections, and Model Gateway API 7. It
advertises Secret Provider API 2. Trusted Model authority remains in process
and is absent from provider JSON; legacy Secret Providers and shared MCP
sessions fail closed for tenant-scoped use. Protocol-v22 clients must fail
exact initialization rather than assume Secret API 1 behavior.

Client protocol `22` preserves protocol-v21 commands, framing, authority,
State schema 12, and Approval Inbox schema 3. It advances Task Graphs to
schema 2, carries immutable optional tenant ownership in summaries, and enables
exact-tenant Graph administration plus the complete worker/lease/mailbox
lifecycle. Protocol-v21 clients must fail exact initialization rather than
discard Task ownership.

Client protocol `21` preserves protocol-v20 commands, framing, Thread and
Operation tenant fencing, and Task schema 1 behavior. It advances Approval
Inbox records to schema 3, projects immutable optional tenant ownership, and
enables exact-tenant Approval discovery and settlement. Protocol-v20 clients
must fail exact initialization rather than discard Approval ownership.

Client protocol `20` preserves protocol-v19 commands, framing, paging,
authorization, and recovery semantics. It advances advertised State
coordinates to schema 12 and carries optional tenant ownership on Thread,
Thread-summary, and `thread_created` projections. Every Thread and retained
Operation access is fenced by the trusted authority's exact tenant; Protocol
requests contain no tenant selector. Tenant-scoped sessions omit Approval and
Task capabilities and reject those commands until their durable stores gain
tenant ownership. Protocol-v19 clients must fail exact initialization rather
than silently discard ownership evidence.

Client protocol `19` preserves protocol-v18 framing, paging, State schema-11
projections, and Model-gateway API 7 coordinates. It adds the explicitly
permissioned `recover_thread` takeover mutation, required
`expected_turn_id` fencing, and the `thread_recovered` settlement. Recovery
remains caller-authorized and never automatic; protocol-v18 clients must fail
exact initialization rather than start new work on an abandoned running Turn.

Client protocol `18` preserves protocol-v17 commands, framing, authority,
lineage, and Thread archive projections. It adds optional bounded
`TurnContextInput` records to `start_turn`, advances advertised State
coordinates to schema 11, and carries the new model-gateway API 7 coordinate.
Runtime derives attribution from the authenticated principal and journals only
content-free context provenance. Protocol-v17 clients must fail exact
initialization rather than omit recovery-critical context.

Client protocol `17` preserves protocol-v16 commands, framing, authority, and
lineage-aware bounded summaries. It advances the advertised State coordinates
to schema 10 and admits optional immutable import provenance on full Thread and
State-event projections. Portable archive files are an embedded/CLI adapter
surface, not a protocol file-transfer command. Protocol-v16 clients must fail
exact initialization rather than silently discard the new durable shape.

Client protocol `16` preserves protocol-v15 commands, framing, authority,
State schema-9 coordinates, and atomic fork semantics. It adds the already
durable direct `lineage` object to bounded Thread summaries so clients can
construct a recent-page branch forest without loading message histories.
Root summaries omit the field. This is a wire-shape change, not a new State
event or SQLite schema. Protocol-v15 clients must fail exact initialization
rather than silently discard ancestry from navigation results.

Client protocol `15` preserves protocol-v14 framing, paging, authority,
Thread-name semantics, and every prior command. It adds `fork_thread`, the
conditional `thread.fork` permission, `thread_forked` settlement, and direct
immutable lineage on Thread projections. The caller-chosen child Thread ID is
the durable retry identity. It advances the advertised State event/snapshot
coordinates to schema 9. Protocol-v14 clients must fail exact initialization
rather than ignore the new command or durable shape.

Client protocol `14` preserves protocol-v13 framing, paging, authority,
provisional-stream semantics, and schema-7 Tool-call batches. It adds
`set_thread_name`, the `thread.name` permission, `thread_named` settlement,
and optional names on Thread navigation projections. It advances the
advertised State event/snapshot coordinates to schema 8. Protocol-v13 clients
must fail exact initialization rather than silently ignore the new command or
durable shape.

Client protocol `13` preserves all protocol-v12 commands, framing, paging,
authority, and provisional-stream semantics. It advances the advertised State
event/snapshot coordinates and permits Thread/Event results to carry one
schema-7 atomic ordered Tool-call batch from a single Model response.
Protocol-v12 clients must fail exact initialization rather than silently
flatten or partially decode the new durable event and Item metadata.
ADR 0098 later adds host-owned bounded execution of explicitly `ParallelSafe`
Tools without changing this wire or State shape: Model decisions and
`ToolResult` Items remain source ordered, while scheduling declarations stay
in Tool registration and project configuration.
Within protocol 13, `thread.list` is an optional additive capability: new
clients must inspect `Initialize`, and older servers remain compatible by not
advertising or accepting the command.

Client protocol `12` preserves all protocol-v11 framing, paging, Task,
Approval, and Operation authority semantics. It adds exact-ID `steer_turn`,
the `turn.steer` permission, durable `turn_steered` acknowledgement, and
provisional `step_invalidated` events. It advances the advertised State
event/snapshot coordinates and permits Thread/Event results to carry schema-6
Steering Items. Protocol-v11 clients must fail exact initialization rather than
silently ignore the new command, stream event, or durable shapes.

Client protocol `11` preserves all protocol-v10 commands, framing, paging, and
authority semantics. It advances the advertised State event/snapshot and Model
Gateway coordinates and permits `GetEvents` and Thread results to carry
schema-5 origin-bound Provider Continuation Items. Protocol-v10 clients must
fail exact initialization rather than decode opaque provider state
permissively.

Client protocol `10` preserves protocol-v9 Turn, State, Operation, and Approval
commands. It adds optional durable Task Graph administration plus
transport-authenticated, principal-derived worker claim, heartbeat,
completion, failure, messaging, and fenced settlement. It also adds the
Workspace Provider API coordinate to `Initialize`. Protocol-v9 clients must
fail exact initialization rather than silently ignore the new command and
compatibility surfaces.

Client protocol `9` preserves protocol-v8 commands, framing, paging, and
operation semantics. It advances the advertised State event and snapshot
coordinates and permits `GetEvents` to return schema-4 Policy decisions bound
to registered Tool origins. Protocol-v8 clients must fail exact initialization
rather than decode the new durable shape permissively.

Client protocol `8` preserves protocol-v7 commands, framing, paging, and
operation semantics. It advances the advertised State event coordinate and
permits `GetEvents` to return schema-3 approval continuation evidence. It does
not add remote Turn takeover: continuation remains an embedded, host-fenced
Runtime operation. Protocol-v7 clients must fail exact initialization rather
than decode the new durable shape permissively.

Client protocol `7` preserves protocol-v6 commands, framing, paging, and
operation semantics. It changes existing Approval record results to include
the authority-scoped Turn requester and deciding actor, advertises Approval
Inbox schema 2, and attributes `StartTurn` to the transport-authenticated
principal. Protocol-v6 clients must fail exact initialization rather than
decode the new approval shapes permissively.

Client protocol `6` preserves protocol-v5 commands, framing, paging, and
operation semantics. It advances the advertised State coordinates and permits
`GetEvents` to return the schema-2 content-free `conversation_summary` evidence
Item. Protocol-v5 clients must fail exact initialization rather than decode a
new durable shape permissively.

Client protocol `5` preserves protocol-v4 commands, frame sizes, retrieval
semantics, and durable State shapes. It adds `conversation_compactor_api` to
the initialization compatibility manifest. State event schema remains `1`;
semantic summaries are ephemeral derived Context and never replace journaled
conversation.

Client protocol `4` preserves protocol-v3 commands, frame sizes, retrieval
semantics, and State shapes. It adds `token_counter_api` to the initialization
compatibility manifest; strict v3 clients must fail exact negotiation instead
of receiving an unrecognized manifest.

Client protocol `3` added exact recovery-byte fields to the protocol-v2
Thread-capacity result. Protocol versions remain exact rather than rolling
field negotiation.

State event schema `13` adds one immutable content-free execution binding to a
Turn. The binding carries trusted actor, issuer/name/version, exact
configuration and environment SHA-256 values, revision, and optional tenant.
The tenant must equal authoritative Thread ownership, projection permits at
most one binding, and Runtime excludes it from Model Context. Schema-13
readers accept immutable schema-1 through schema-12 history after explicit
migration, and the schema-13 writer emits only schema 13.

State event schema `14` adds bounded Connector evidence to a successful
`ToolResult`. Runtime—not the Connector—binds registered Tool identity/origin,
trusted actor/tenant, and the exact output SHA-256. State validates the claim
shape, digest, tenant, and preceding ToolCall/Policy origin during append and
every projection. Schema-14 readers accept immutable schema-1 through
schema-13 history after explicit migration, and the schema-14 writer emits
only schema 14.

State event schema `12` adds optional tenant ownership to the authoritative
`thread_created` event. SQLite persists a nullable same-transaction
`streams.tenant_id` lookup projection and validates it against the journal on
open. Exact tenant equality gates State reads and mutations. Schema-12 readers
accept immutable schema-1 through schema-11 history after explicit migration,
and the schema-12 writer emits only schema 12.

State event schema `11` adds `invocation_context`, a content-free Item carrying
the authenticated Turn actor plus 1–64 ordered source/reference pairs, exact
source/model-visible SHA-256 values, and bounded byte/token charges. The body
is ephemeral and never becomes conversation history. Schema-11 readers accept
immutable schema-1 through schema-10 history, and the schema-11 writer emits
only schema 11.

State event schema `10` adds `thread_imported` with the exact source Thread
identity, source stream version/last sequence, source-event SHA-256, and
optional source fork lineage. A caller-named target stream is created
atomically with fresh Event identities, preserved historical correlations,
copied name transitions, and no recovery-only Checkpoints or replayed effects.
Schema-10 readers accept immutable schema-1 through schema-9 history, and the
schema-10 writer emits only schema 10.

State event schema `9` adds `thread_forked` with direct parent identity, exact
parent sequence/version boundary, and parent-prefix SHA-256. A fork stream is
created atomically under a caller-chosen child identity; it preserves
historical Turn/Item/correlation identities without replaying effects and does
not copy names or Checkpoints. Schema-9 readers accept immutable schema-1
through schema-8 history, and the schema-9 writer emits only schema 9.

State event schema `8` adds the explicit `thread_named` event. The optional
name is 1–256 trimmed non-control UTF-8 bytes or null to clear it. Schema-8
readers accept immutable schema-1 through schema-7 history, and the schema-8
writer emits only schema 8. SQLite stores a same-transaction nullable
projection for recent listing and validates it against authoritative events
when opening.

State event schema `7` adds the atomic `tool_calls_appended` event and batch
identity/index/size metadata on its ordered Tool-call Items. A singular
`item_appended` event cannot carry batch metadata. Schema-7 readers accept
immutable schema-1/2/3/4/5/6 history, and the schema-7 writer emits only
schema 7.

State event schema `6` adds actor-attributed `steering_queued` and exactly
correlated `steering_applied` Items. Queue records are durable acceptance
evidence but are not Model-visible; application records project as user input
only at Runtime safe boundaries. Schema-6 readers accept immutable
schema-1/2/3/4/5 history, and the schema-6 writer emits only schema 6.

State event schema `5` adds the bounded `provider_continuation` Item. The
Runtime binds it to the exact registered Model identity and origin that
settled the response; provider adapters own format-specific replay. Schema-5
readers accept immutable schema-1/2/3/4 history, and the schema-5 writer emits
only schema 5.

State event schema `4` adds the registered Tool origin to every
`policy_decision`, including allow, deny, and ask. This records what Policy
actually evaluated even when Tool execution never begins. Schema-4 readers
accept immutable schema-1, schema-2, and schema-3 history; the schema-4 writer
emits only schema 4.

State event schema `3` adds requester, Tool-origin, and exact Model-request
SHA-256 evidence to `approval_requested`. This permits only the fingerprinted
pre-Tool approval boundary to resume after worker loss. Schema-3 readers accept
immutable schema-1 and schema-2 history; the schema-3 writer emits only schema
3.

State event schema `2` added content-free `conversation_summary` evidence. Its
body remains ephemeral Context; original conversation Items remain immutable
schema-1 or schema-2 events.

State snapshot schema `12` admits authoritative Thread tenant ownership.
State snapshot schema `11` admits invocation-context evidence.
State snapshot schema `10` admits immutable Thread import provenance. Older
snapshots are disposable and are ignored. State snapshot schema `9` admits
fork provenance, while schema `8` admits Thread names. State snapshot schema
`7` admits atomic ordered Tool-call batches. Older
snapshots are disposable and are ignored. State snapshot schema `6` admits
Steering evidence. Older snapshots are
disposable and are ignored. State snapshot schema `5` admits Provider
Continuation evidence. Older
snapshots are disposable and are ignored. State snapshot schema `4` admits
Policy-to-Tool-origin evidence. Older
snapshots are disposable and are ignored. State snapshot schema `3` admitted
schema-3 approval continuation evidence. State snapshot schema `2` was the
first coordinate to include the exact recovery charge of its journal prefix.

Approval Inbox schema `2` adds `requested_by` to each request and `decided_by`
to settled records. It writes only schema 2. Populated schema-1 SQLite inboxes
must migrate before opening; pending legacy requests become orphaned and
unrecoverable historical actors remain explicitly `unattributed_legacy`.

Evaluation artifact format `2` makes suite, baseline, and report roots
self-describing. Grade results and baseline requirements bind the registered
Grader name and trust-bearing origin. Format-1 artifacts had no root coordinate
or Grader origin and cannot be upgraded automatically without an owner choosing
the intended origin; missing provenance is never inferred as built-in trust.

## Pre-1.0 support promise

- Patch releases in one `0.y` line preserve documented public Rust contracts,
  protocol behavior, and readable durable schemas except for a critical
  security fix that cannot be made safely without a break.
- A minor `0.y` release may break Rust APIs or durable/wire schemas, but release
  notes must identify every break and the required operator action.
- Y-Harness writes only its current schema. It never silently downgrades,
  truncates, or relabels unknown data.
- Unknown authoritative State or Approval schemas fail closed. Unknown or
  corrupt State snapshots are disposable and fall back to the journal.
- Downgrade after a writer has emitted a newer schema is unsupported unless the
  target release explicitly advertises read compatibility.
- Mixed-version writers against one SQLite database are unsupported. Stop old
  writers before enabling a release that changes a durable write schema.

## What is additive

Within one client protocol version, a new opt-in command or capability may be
added when:

1. old clients never receive its result unless they request it;
2. existing command and result shapes do not change;
3. `Initialize.capabilities` advertises availability; and
4. frame and authority bounds remain unchanged or become stricter only in a
   new protocol version.

Changing an existing command/result, error settlement, identifier meaning,
State event exposed by `GetEvents`, or stream cursor semantics requires a new
protocol coordinate.

Adding an optional Rust trait method with a safe default may remain compatible.
Adding a required method, changing ownership/lifetime behavior, or weakening a
kernel invariant requires the next minor release before 1.0.

The current unreleased `0.1.0` source requires a
`StdioMcpLaunchAuthority` when constructing `StdioMcpClient` and makes
`StdioMcpConfig.current_dir` an exact absolute path. This pre-publication
security correction removes implicit unrestricted MCP process execution. It
does not change client protocol or durable data.

## Read-only store preflight

The concrete SQLite State, Approval, Task, Workflow, Human Handoff, and Effect adapters
add public asynchronous `validate_existing` functions. They validate an
existing regular database through a read-only, `query_only` connection and
perform no creation, bootstrap, migration, or backup publication.

The reference `doctor` and `serve` hosts call these validators before external
capability construction. Missing stores remain eligible for first creation;
current or empty stores are accepted; legacy stores return the existing
operator migration diagnostic; partial, mixed, malformed, and unknown stores
fail closed. `serve` still repeats authoritative validation while opening each
store, so preflight is not treated as race protection.

This is an additive pre-1.0 Rust API and host-ordering change. It does not
advance service configuration schema 1, Protocol v28, State 14, Approval 3,
Task 3, Workflow 1, or Human Handoff 1. See
[ADR 0131](adr/0131-read-only-service-store-preflight.md).

## Migration discipline

State schemas 1 through 13 are supported migration sources. Populated SQLite
stores at any of those coordinates require the explicit backup-first command
below before a schema-14 Runtime can open them:

```bash
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v14.rollback.db
```

All writers must be stopped. The migration checks exact source versions and
disk space, creates and validates a no-clobber SQLite backup, conditionally
adds the nullable Thread-name projection for pre-v8 sources, adds the nullable
Thread-tenant projection without inferring legacy ownership, drops disposable
old snapshots, and advances
event/snapshot writer metadata in one immediate transaction. Historical event
JSON and schema labels are never rewritten. An interrupted schema-1 through
schema-13 run can reuse its validated backup.

The schema-14 reader/new writer decision is asymmetric:

- new reader + old data: supported only after explicit migration; historical
  schema-1 through schema-13 events remain readable;
- old reader + new writer: unsupported and fails on schema-14 metadata or
  events;
- old and new writers together: unsupported; and
- downgrade: supported only by restoring the backup before any schema-14 event
  is written.

See the [State migration runbook](state-migration.md) and
[ADR 0061](adr/0061-backup-first-immutable-history-state-schema-migration.md)
for the immutable-history migration design, plus
[ADR 0065](adr/0065-fingerprinted-pre-tool-approval-resumption.md) for the
schema-3 continuation boundary and
[ADR 0068](adr/0068-durable-policy-tool-origin-provenance.md) for schema-4
authorization provenance, plus
[ADR 0077](adr/0077-origin-bound-provider-continuation.md) for schema-5
Provider Continuation, plus
[ADR 0078](adr/0078-durable-safe-boundary-turn-steering.md) for schema-6
safe-boundary steering, plus
[ADR 0086](adr/0086-atomic-ordered-multi-tool-decisions.md) for schema-7
atomic ordered Tool-call decisions and
[ADR 0098](adr/0098-explicit-bounded-parallel-tool-execution.md) for its
host-owned execution policy, plus
[ADR 0099](adr/0099-observable-model-attempt-timeout-cooldown.md) for
process-local observable Model timeout cooldown, plus
[ADR 0100](adr/0100-typed-model-provider-failure-evidence.md) for the additive
typed Provider failure evidence boundary, plus
[ADR 0101](adr/0101-bounded-typed-model-retry-policy.md) for explicit bounded
same-Model retries, plus
[ADR 0102](adr/0102-governed-signed-external-skill-lifecycle.md) for governed
signed External Skill storage and activation, plus
[ADR 0092](adr/0092-engine-owned-thread-names.md) for schema-8 Thread names,
plus [ADR 0093](adr/0093-atomic-thread-fork-and-lineage.md) for schema-9
atomic fork lineage, and
[ADR 0094](adr/0094-lineage-aware-bounded-thread-navigation.md) for the
Protocol-16 lineage-aware summary projection, plus
[ADR 0095](adr/0095-portable-integrity-bound-thread-archives.md) for schema-10
portable Thread archives and import provenance, plus
[ADR 0096](adr/0096-attributed-per-turn-context.md) for schema-11 attributed
per-Turn context, plus
[ADR 0117](adr/0117-durable-thread-tenant-ownership.md) for schema-12 Thread
tenant ownership, Protocol v20 fencing, and archive format 2, plus
[ADR 0118](adr/0118-durable-approval-tenant-ownership.md) for Approval Inbox
schema 3 and Protocol v21 tenant fencing, plus
[ADR 0119](adr/0119-durable-task-graph-tenant-ownership.md) for Task Graph
schema 2 and Protocol v22 tenant fencing, plus
[ADR 0120](adr/0120-authority-aware-secret-resolution.md) for Secret Provider
API 2, in-process Model authority, MCP session fencing, and Protocol v23, plus
[ADR 0122](adr/0122-durable-turn-execution-binding.md) for schema-13 Turn
execution evidence, archive format 3, and Protocol v24, plus
[ADR 0123](adr/0123-durable-task-attempt-execution-binding.md) for Task Graph
schema 3 governed attempt evidence and Protocol v25.
See [ADR 0126](adr/0126-runtime-bound-connector-evidence.md) for schema-14
Runtime-bound Connector evidence, archive format 4, and Protocol v26.
See [ADR 0127](adr/0127-durable-fenced-workflow-runs.md) for Workflow Run
schema 1 and Protocol v27.
See [ADR 0128](adr/0128-durable-lease-fenced-human-handoff.md) for Human
Handoff schema 1 and Protocol v28.

Approval Inbox schema 1 or schema 2 is independently migrated with:

```bash
yh approval-migrate \
  /absolute/path/approval.db \
  /absolute/path/approval-v1.rollback.db
```

All Approval Inbox writers must be stopped. The migration validates and
fingerprints the complete indexed record set, creates a no-clobber backup,
orphans schema-1 pending records whose requester cannot be reconstructed,
preserves terminal evidence with explicit unattributed actors, retains
schema-2 lifecycles as unscoped, and commits schema-3 writer metadata and the
tenant projection atomically. New reader + old populated data is supported
only after this migration; old readers/writers must not access the migrated
source. Rollback is a backup restore before any schema-3 write, or an
explicitly data-losing recovery afterward.

See the [Approval migration runbook](approval-migration.md) and
[ADR 0063](adr/0063-attributed-separation-of-duty-approvals.md).

Task Graph schema 1 or schema 2 is independently migrated with:

```bash
yh task-migrate \
  /absolute/path/tasks.db \
  /absolute/path/tasks-v2.rollback.db
```

All Task Coordinator writers must be stopped. The migration validates and
fingerprints every bounded Graph, creates a no-clobber backup, retains every
lifecycle, lease, message, and artifact, and writes schema-3 aggregates.
Schema-1 Graphs become explicitly unscoped; schema-2 tenant ownership remains
exact. Neither path invents historical Task-attempt binding evidence. A
schema-2 body that claims schema-3 binding evidence fails closed. The earlier
unreleased table without a version column is accepted as the same schema-1
source shape. Old writers must not access a migrated store, and rollback is
supported only by restoring the backup before any schema-3 write. See the
[Task migration runbook](task-migration.md).

Workflow Run schema 1 is a new independent store. `yh serve` creates
`workflows.db` only when neither metadata nor Run tables exist. A partial
layout, unknown metadata version, or row that claims another schema fails
closed; it is never guessed or migrated in place. There is no downgrade or
rolling old/new writer claim for this first schema.

Human Handoff schema 1 is another independent store. `yh serve` creates
`human-handoffs.db` only when neither metadata nor Handoff tables exist. A
partial layout, unknown metadata version, oversized row, projection/body
drift, or actor-bound command-digest mismatch fails closed. There is no
migration, downgrade, or rolling mixed-writer claim for this first schema.

The first durable schema change must ship with:

- an ADR and old/new schema fixtures;
- a bounded, restartable, forward-only migration;
- preflight disk-space and version checks;
- transactional or checkpointed progress;
- backup/restore instructions;
- crash-at-each-phase tests;
- old-reader/new-writer and new-reader/old-writer decisions;
- an explicit rollback boundary; and
- performance evidence at the largest supported fixture.

Destructive archival, retention, or blob migration requires separate operator
authorization. A snapshot never counts as a backup.

## Deprecation and support

Before 1.0, a deprecated public API is retained for at least one subsequent
minor release when doing so does not preserve a security defect. Protocol and
durable schema support windows are declared per release. State schema 14 reads
immutable schema-1 through schema-13 history after explicit store migration.
Approval reads only schema 3 in normal operation; its migration tool alone
reads schema 1 and schema 2. Task coordination reads only schema 3 in normal
operation; `task-migrate` alone reads schema 1, schema 2, and the earlier
unversioned development layout. Workflow coordination reads only its first
schema 1.

The MSRV is Rust 1.88.0 and is enforced in CI. Platform support is proven only
where the exact release commit has green jobs; configuration alone is not
evidence.
