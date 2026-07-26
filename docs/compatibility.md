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
| Client protocol | `"12"` | exact `Initialize` request/response |
| State events | `6` | per-event durable envelope; reads schemas 1 through 6 |
| State snapshots | `6` | cache body; incompatible caches are discarded |
| Approval Inbox | `2` | per-record durable body after explicit migration |
| Task Coordinator | `1` | per-graph SQLite schema column |
| Memory Provider API | `1` | exact descriptor registration |
| Token Counter API | `1` | exact descriptor registration |
| Conversation Compactor API | `1` | exact descriptor registration |
| Evaluation artifacts | `2` | exact self-described suite/baseline/report roots; not a client-protocol surface |
| Workspace Provider API | `"1"` | exact embedded provider installation and `Initialize` coordinate |
| Secret Provider API | `1` | exact descriptor registration |
| Skill package API | `"1"` | exact manifest validation |
| HTTPS model gateway API | `"4"` | exact request/response header |

`Initialize` advertises the engine version and Runtime-facing durable/API
coordinates above, including the Workspace Provider API implemented by
orchestration hosts. Evaluation artifacts remain self-described and are not a
client-protocol surface. Capabilities are separately negotiated; a disabled
capability is not implied by its schema coordinate.

Service configuration schema 1 is bounded to 65,536 bytes, rejects unknown
fields, and keeps credentials as environment-backed secret references. Its
optional service-assembly fields add direct OpenAI Responses, shell-free JSON
Tools, exact-selected stdio MCP Tools, and Agent Memory Hub Context without
changing the meaning of an existing field. External process launch is explicit,
child environments are copied only by configured host-variable name, and MCP
catalog discovery alone grants no Tool authority. `data_directory` and an
optional exclusive CA file must remain inside the configuration project root.
A future incompatible configuration shape requires a new root schema
coordinate and an explicit operator migration; it is never guessed from
fields.

Skill API `1` keeps the original package digest and publisher-signature bytes.
Signed packages may add an optional signed transparency receipt; publisher
policy decides whether absence is accepted. This does not change how an
existing receipt-free package is decoded or how its publisher signature is
verified.

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

State snapshot schema `6` admits Steering evidence. Older snapshots are
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

## Migration discipline

State schemas 1, 2, 3, 4, and 5 are supported migration sources. Populated
SQLite stores at any of those coordinates require the explicit backup-first
command below before a schema-6 Runtime can open them:

```bash
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v6.rollback.db
```

All writers must be stopped. The migration checks exact source versions and
disk space, creates and validates a no-clobber SQLite backup, then adds or
advances event and disposable-snapshot writer metadata in one immediate
transaction. Historical event JSON and schema labels are never rewritten. An
interrupted schema-1, schema-2, schema-3, schema-4, or schema-5 run can reuse
its validated backup.

The schema-6 reader/new writer decision is asymmetric:

- new reader + old data: supported only after explicit migration; historical
  schema-1, schema-2, schema-3, schema-4, and schema-5 events remain readable;
- old reader + new writer: unsupported and fails on schema-6 metadata or Items;
- old and new writers together: unsupported; and
- downgrade: supported only by restoring the backup before any schema-6 event
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
safe-boundary steering.

Approval Inbox schema 1 is independently migrated with:

```bash
yh approval-migrate \
  /absolute/path/approval.db \
  /absolute/path/approval-v1.rollback.db
```

All Approval Inbox writers must be stopped. The migration validates and
fingerprints the complete indexed record set, creates a no-clobber backup,
orphans pending records whose requester cannot be reconstructed, preserves
terminal evidence with explicit unattributed actors, and commits schema-2
writer metadata atomically. New reader + old populated data is supported only
after this migration; old readers/writers must not access the migrated source.
Rollback is a backup restore before any schema-2 write, or an explicitly
data-losing recovery afterward.

See the [Approval migration runbook](approval-migration.md) and
[ADR 0063](adr/0063-attributed-separation-of-duty-approvals.md).

The Task Coordinator recognizes its earlier unreleased development table and
adds the version-1 column with a constant default; this path is covered by a
fail-closed schema test and is not a public support window.

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
durable schema support windows are declared per release. State schema 6 reads
immutable schema-1, schema-2, schema-3, schema-4, and schema-5 history after
explicit store migration.
Approval reads only schema 2 in normal operation; its migration tool alone
reads schema 1.

The MSRV is Rust 1.88.0 and is enforced in CI. Platform support is proven only
where the exact release commit has green jobs; configuration alone is not
evidence.
