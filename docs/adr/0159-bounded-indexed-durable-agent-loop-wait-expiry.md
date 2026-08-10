# ADR 0159: bounded indexed durable Agent Loop wait expiry

- Status: accepted; bounded Memory/SQLite projection, backup-first migration,
  exact settlement, Temporal API 3 composition, and reference-service lifecycle
  are implemented
- Date: 2026-08-03

## Context

ADR 0158 makes one pre-effect Approval wait durable. A Turn may be
`Waiting`, become `Ready` after an Approval settlement, or be claimed by a
worker. Cancellation, timeout, denial, claim, and completion share the same
State stream compare-and-swap boundary.

The current exact resume path can observe an expired wait, but a wait with no
caller may remain live indefinitely. A process-local timer heap loses work on
restart. Scanning or replaying every retained Thread on every tick makes
latency and allocation proportional to all State. A second scheduler database
creates a crash gap between timer truth and the owning Turn journal.

There is a second, more urgent convergence case. Approval denial is accepted
as a durable `Ready` transition before Runtime appends the atomic terminal
`DenyWait`. If that process stops between those events, the Turn must finish
the accepted denial immediately. Relabeling it `TimedOut` would destroy the
real Approval decision and produce false audit evidence.

This decision defines a metadata-only live projection and bounded host-driven
settlement. The journal remains authoritative. The projection locates exact
lifecycle events; it never authorizes a transition by itself.

## Decision

### 1. Materialize three non-effecting live phases

Maintain at most one `agent_loop_wait_projection` row per Thread in the same
State store as its journal. The row has one of three phases:

| Phase | Due coordinate | Required maintenance action |
|---|---|---|
| `Waiting` | immutable envelope expiry | `WaitClosed(TimedOut)` |
| `ReadyAllow` | the same immutable envelope expiry | `WaitClosed(TimedOut)` if no worker claimed first |
| `ReadyDeny` | Approval acceptance time | immediate `DenyWait` with the accepted denial |

`ReadyAllow` remains discoverable because an accepted approval is not a worker
claim. If no worker claims it before the original finite wait boundary, the
Turn times out. `ReadyDeny` is immediately due and can never take the timeout
path. It converges the already accepted denial through `DenyWait`.

The live projection stores only bounded coordinates:

```text
tenant_key
thread_id + turn_id + wait_id
revision + phase + due_at_ms
approval_id + envelope_sha256
wait_started_event_id
current_transition_event_id
resume_command_id?       # present for ReadyAllow/ReadyDeny
```

It stores no event JSON, Approval reason, Tool input, Model request, Context,
Memory, secret, credential, or serialized `WaitEnvelope`. `Executing` and
terminal Turns have no row.

The pure lifecycle reducer updates the row as follows:

```text
WaitStarted                         -> insert Waiting
AcceptResume(Approve)               -> Waiting to ReadyAllow
AcceptResume(Deny)                  -> Waiting to ReadyDeny, due immediately
ClaimReady from ReadyAllow          -> delete
WaitClosed from Waiting/ReadyAllow  -> delete
DenyWait from Waiting/ReadyDeny     -> delete
```

An ordinary terminal event is not allowed to bypass a live row. A phase,
revision, wait identity, transition identity, or resume-command mismatch fails
closed instead of repairing the row heuristically.

### 2. Journal, stream fences, and projection change atomically

SQLite applies the journal insert, stream version and recovery-byte update,
and projection insert/update/delete in one `IMMEDIATE` transaction. Projection
updates use the previous phase, revision, wait identity, and transition event
as their own row CAS. A commit exposes either the complete old lifecycle or
the complete new lifecycle; a crash cannot expose a terminal journal event
with a stale due row or a due row without its owning journal event.

Memory State applies the same pure reducer while holding its single State
mutation boundary and maintains an ordered due-key set. Other Event Store
implementations either provide the same atomic contract or report the feature
unsupported. No adapter may update the due projection asynchronously.

The journal is still the source of truth. Projection schema and reducer
validation are additional fail-closed invariants, not a second aggregate and
not permission to invent a missing wait.

### 3. Scan a metadata-only partial due index

SQLite uses a partial covering index over:

```sql
(tenant_key, due_at_ms, thread_id, turn_id, wait_id)
WHERE due_at_ms IS NOT NULL
```

Trusted `at_ms` is a query parameter, never a dynamic index expression. One
scan is scoped to exactly one tenant and selects
`due_at_ms <= at_ms` in strict keyset order:

```text
(due_at_ms, thread_id, turn_id, wait_id)
```

The limit is 1 through 256. Stores query at most `limit + 1`, return at most
`limit`, and use the extra row only for `has_more`. Offset pagination,
unbounded sorting, wildcard-tenant scans, and full Thread enumeration are
forbidden. The disposable cursor is the last returned key. Losing it can
repeat a bounded page but cannot lose authoritative work.

The index read returns only the metadata above plus the current stream version
and recovery-byte fence obtained in the same read boundary. It does not decode
event bodies and does not expose wait payload to the Temporal host.

### 4. Read exactly one or two lifecycle events, not the Thread

Before settlement, State resolves projection event references through the
unique event-ID index:

- `Waiting` reads the one `WaitStarted` event; its current-transition identity
  must equal the wait-start identity.
- `ReadyAllow` and `ReadyDeny` read `WaitStarted` plus the current
  `AcceptResume` event.

Those one or two bounded events are enough to reconstruct and validate the
complete immutable wait envelope and, for a ready phase, the exact accepted
resume evidence. State checks event schema, Thread/Turn/wait coordinates,
revision chain, phase/decision correlation, Approval identity, envelope
digest, resume command, event identities, recorded times, tenant, and the
stream/recovery fences returned by the due query.

The due path does not load a State snapshot, replay a complete Thread, page
all Thread events, or scan for a matching denial. Missing, duplicated,
oversized, malformed, cross-Thread, or contradictory referenced events make
the candidate fail closed. The row is never used to synthesize absent event
content.

This minimal reconstruction is specific to maintenance at a proved
non-effecting boundary. It is not a general shortcut for Agent Loop resume,
worker claim, completion, or unknown-effect reconciliation.

### 5. Settle each phase through its exact existing transition

For `Waiting` and `ReadyAllow`, maintenance applies the existing exact
`WaitClosed(TimedOut)` transition. State rechecks:

- current row phase is `Waiting` or `ReadyAllow`;
- exact wait identity and lifecycle revision;
- envelope expiry equals the row due coordinate and is at or before trusted
  scan time;
- stream version and recovery-byte CAS; and
- no accepted denial exists.

For `ReadyDeny`, maintenance reconstructs the complete accepted denial from
`AcceptResume` and applies the existing atomic `DenyWait`. It copies the real
Approval settlement and deciding authority into terminal evidence. It does
not create a worker claim, invoke a Tool, wait until envelope expiry, or emit a
timeout reason.

`DenyWait` remains available directly from `Waiting` when Runtime already has
an authoritative denial. The due projection does not manufacture such a
denial: only `ReadyDeny` carries a referenced accepted settlement suitable for
maintenance reconstruction.

Resume, claim, cancellation, denial, expiry, completion, and another
maintenance process all compete on the same stream and projection fences.
Exactly one source-revision transition may commit. A loser reloads and reports
`duplicate` or `fenced`; it never overwrites the winner.

### 6. Use deterministic logical time and command identities

Maintenance event content must not depend on which process observed a due row
or how late its scan ran. The lifecycle's logical settlement time is:

| Phase | Logical event time |
|---|---|
| `Waiting` / `ReadyAllow` | immutable envelope expiry (`due_at_ms`) |
| `ReadyDeny` | immutable resume acceptance time (`due_at_ms`) |

The host's later observation time is an operational latency metric, not
durable lifecycle evidence. The logical time is used for transition evidence
and journal `recorded_at_ms`, subject to the event's normal monotonic and bound
validation.

Timeout and denial use distinct domain-separated SHA-256 command identities.
Canonical length-prefixed hashing binds:

```text
command domain + phase + tenant presence/value
thread + turn + wait + lifecycle revision
due_at_ms + immutable envelope_sha256
```

It deliberately excludes host identity, process ID, scan cursor, observation
time, and ephemeral maintenance credentials. Consequently two authorized
processes derive the same logical command and response-loss retry converges on
the same event. A timeout command rejects `ReadyDeny`; a denial command rejects
every other phase. Reusing an identity with changed typed content fails closed.

The latest stream version and recovery bytes remain separate CAS inputs. They
are reloaded for each attempt and do not destabilize logical command identity.

### 7. Separate maintenance authority from frozen requester authority

Due discovery and settlement require a trusted embedding host to supply a
validated maintenance principal with the exact owning tenant. `StateEngine`
enforces identity validity and the tenant fence; `AuthorityContext` does not
carry a permission set, so the host must authorize its maintenance role before
constructing or invoking the embedded driver. The fixed-authority reference
service obtains that authority from operator configuration. An unscoped actor
cannot scan a tenant-owned row, a tenant-scoped actor cannot scan or mutate
another tenant, and the index is not a tenant-discovery API. A future remote
administration surface must add its own typed protocol permission rather than
claim that this embedded port already has one.

The maintenance principal authorizes only discovery and the phase-specific
terminal transition. It does not become the original Agent requester and gains
no authority to inspect Context, answer Approval, resume execution, claim a
worker, or invoke a Tool.

The `WaitEnvelope` retains the frozen requester Authority Context that
authorized the original execution generation. Minimal reconstruction validates
that frozen requester against the wait event and generation evidence; it is
never replaced by the sweeper principal. `ReadyDeny` also retains the distinct
Approval deciding authority from the accepted settlement. Thus one convergence
event may have three deliberately different roles:

```text
frozen requester       -> owns the original Agent execution
Approval decision maker -> supplied the authoritative Deny evidence
maintenance principal  -> is allowed to converge an already-due lifecycle
```

Audit records preserve those roles rather than attributing the denial decision
to the sweeper. Stable logical command identity allows several processes to use
the same configured maintenance role without pretending that they share the
requester's credentials.

### 8. Compose an opt-in bounded Temporal source

This implementation advances embedded Temporal Driver API 2 to 3 and adds
Agent Loop due settlement as an explicitly optional host source. Core starts
no thread, timer, interval, or background task. The host supplies trusted time,
one tenant authority, a per-source page bound, cadence, cancellation, and drain
behavior.

One tick validates a complete metadata page before its first mutation and
reports each candidate independently as `applied`, `duplicate`, `fenced`, or
content-free `failed`. The first implementation is sequential and visits one
1–256-row page per installed source per tick. Reaching that bound returns
`has_more`; it does not expand the limit or monopolize SQLite. The reference
service skips missed cadence ticks and bounds shutdown waiting, but API 3 does
not yet expose an independent per-tick wall-time, page-count, or attempt budget.

Reference-service polling is disabled by default and requires explicit tenant,
authority, cadence, and bounds. A public Protocol administration surface is a
separate versioned decision; the embedded driver is not silently exposed by
Protocol 37.

The bounded API report exposes per-source scanned/due/`has_more` counts and
content-free applied/duplicate/fenced/failed attempts. Production operations
metrics for phase-specific live counts, oldest-due age, settlement/query
latency, backlog, projection health, and migration state remain follow-up
evidence. User identifiers, Approval content, Context, Tool input, and secrets
must not become metric labels or error report bodies.

### 9. Version and migrate the projection independently

The installed layout has an independent exact coordinate:

```text
agent_loop_wait_projection_schema = 1
```

It is not silently folded into an already advertised State-event schema. State
event/snapshot, Thread archive, Protocol, and Temporal coordinates change only
when their own contracts change. Store open validates the complete table,
columns, constraints, foreign keys, partial index, and exact projection schema
metadata. Unknown, partial, or newer layouts fail closed.

Migration is backup-first, exclusive, and offline with old writers stopped:

1. validate the existing State database and backup;
2. create a shadow schema-1 projection and partial index;
3. page authoritative journal events in stable order through the pure
   lifecycle reducer;
4. validate every resulting live row against its one or two referenced events,
   stream tenant, version, and recovery metadata;
5. atomically install the completed projection and write the independent
   schema/readiness coordinate last; and
6. reopen through the normal strict validator before admitting writers or
   Temporal scans.

A migration crash leaves scanning disabled; it does not fall back to a full
Thread scan. Rebuild is an explicit backup/exclusive-writer operation using the
same reducer. It may recreate derived rows but never rewrite journal events,
invent Approval evidence, change due time, or infer Waiting from a legacy
`Running` Turn. Downgrade requires restoration of the validated pre-migration
backup before projection-schema-1 writes are admitted.

Fork and import streams need one additional historical boundary. Their copied
wait envelopes and Approval settlement evidence retain the immutable source
Thread identity while the outer `StoredEvent` is rebound to the target Thread.
Backfill accepts that mismatch only when the target stream has exactly one
`ThreadForked` or `ThreadImported` provenance event in the second event
position, every copied lifecycle transition agrees on the same source Thread,
and each rebound wait history reaches a terminal event. Such copied terminal
history is audit evidence and never becomes a live due row in the target
Thread. Ordinary appends do not receive this migration-only exception. This
structural proof does not attest the complete ancestry chain: a valid
multi-level fork/import can legitimately retain a source Thread older than the
immediate provenance marker. Cryptographically proving that ancestry requires
a separately versioned lineage-chain contract.

### 10. Preserve the Approval Inbox transaction boundary

State and Approval Inbox remain separate databases. A timeout or converged
denial commits to State first. Inbox close/orphan cleanup is a separately
bounded idempotent action or future repair-outbox job. This ADR does not claim
an atomic cross-database transaction.

A crash after State settlement can leave an Inbox record pending or settled.
Repair may close or retain it as audit evidence, but it can never reopen the
terminal Turn. Inbox absence never suppresses settlement of an authoritative
State due row.

## Crash and concurrency boundaries

| Boundary | Required result |
|---|---|
| wait/resume journal transaction crashes | journal, stream fences, and projection expose complete old or new state together |
| scan crashes before mutation | no State change; disposable cursor may repeat the candidate |
| timeout/denial transaction crashes | old live phase or complete terminal state, never a terminal event plus live row |
| commit response is lost | deterministic command/event retry returns the exact committed outcome |
| two processes settle one row | one stream/projection CAS winner; the other is duplicate or fenced |
| Approval Deny accepted, Runtime dies | row is `ReadyDeny`, immediately due, and later settles only through `DenyWait` |
| claim races `ReadyAllow` expiry | claim or TimedOut wins; no Tool starts from the losing revision |
| old candidate meets a later wait | wait/revision/event/digest fences reject it |
| migration/rebuild crashes | projection remains disabled until schema-1 validation and readiness commit |
| State settlement/Inbox cleanup crashes | State remains terminal; separate repair cannot reopen it |

Multi-process safety comes from SQLite transactions, stream and row CAS,
deterministic idempotency, and exact indexed event reads. It is not leader
election, distributed consensus, exactly-once polling, or a globally monotonic
clock.

## Required test matrix

This matrix separates the implemented safety contract from the remaining
production-scale and operational evidence. Passing the current unit and
integration subset is not a latency, backlog-capacity, or multi-node claim.

| Area | Required cases |
|---|---|
| Reducer | every legal three-phase transition; wrong phase/revision/event/decision; ordinary terminal bypass; Memory/SQLite parity |
| Atomicity | fault injection around event, stream version, recovery bytes, row insert/update/delete, and commit; reopen sees only old/new complete state |
| Due scan | exact inclusive boundary; ReadyDeny immediate; partial-index query plan; total keyset order; cursor tamper/no progress; 1/256 and maximum-cardinality bounds |
| Minimal reads | Waiting reads exactly one event; Ready phases read exactly two; no snapshot/full Thread replay; missing/wrong/cross-Thread/oversized event fails closed |
| Actions | Waiting/ReadyAllow only TimedOut; ReadyDeny only DenyWait; real denial reason and deciding authority preserved; no claim or Tool effect on denial |
| Idempotency/time | stable IDs and logical timestamps across delay/restart/process; domain separation; length-prefix collision tests; changed-content reuse rejection |
| CAS races | timeout/denial vs resume/cancel/claim/completion/recovery and another sweeper on two SQLite connections/processes; exactly one winner |
| Authority | exact tenant; unscoped/wrong tenant denied; maintenance/requester/decision-maker role substitution denied; no payload leakage |
| Migration | every supported fixture; backup; crash at each shadow/install phase; partial/unknown layout refusal; rebuild equivalence; no legacy synthetic rows; downgrade refusal |
| Temporal/limits | source disabled/enabled; page/attempt/time exhaustion; cancellation/drain; large backlog and foreground-write latency; API 2/3 mismatch |
| Inbox gap | timeout and denial before/after Inbox settlement; cleanup failure/response loss; late decision never resumes State |
| Observability | phase/result/lag metrics, bounded tenant-safe labels, no Approval/Context/Tool/secret leakage |

Performance acceptance includes maximum supported live-row and due-backlog
cardinality with SQLite `EXPLAIN QUERY PLAN` evidence that the partial index is
used. Unit correctness alone is not a production latency claim.

## Consequences

- Expired waits and stranded accepted denials become discoverable without
  retained workers or complete Thread replay.
- `ReadyDeny` preserves the actual business decision instead of converting a
  process crash into a false timeout.
- Metadata scanning stays bounded while exact event evidence and CAS retain the
  authoritative safety boundary.
- An independent projection schema, migration, host lifecycle, health checks,
  and backlog operations add implementation and operational complexity.
- Settlement latency depends on host cadence, backlog, budgets, SQLite
  availability, and trusted-clock quality; safety does not imply real-time SLA.

## Rejected alternatives

- Index only `Waiting`: it loses unclaimed approvals and can strand an accepted
  denial after Runtime loss.
- Mark `ReadyDeny` TimedOut: it discards authoritative Approval evidence and
  makes audit history false.
- Replay the complete Thread per due row: cost and allocation grow with
  unrelated retained history despite exact lifecycle event identities.
- Trust projection metadata without reading events: a stale or corrupt row is
  not a complete wait or denial proof.
- Store full envelopes or decisions in the index: it duplicates sensitive
  payload, widens query exposure, and creates two content authorities.
- Scan every Thread, use offsets, or sort JSON expiry: each creates unbounded
  work, unstable pagination, or storage-specific untyped behavior.
- Store timers in memory or a scheduler database: the former loses restart
  state; the latter introduces a cross-store truth gap.
- Derive IDs or timestamps from scan/host time: two processes and response-loss
  retries would produce different logical transitions for one due fence.
- Bind command identity to process credentials: fleet restart or failover would
  defeat deterministic convergence; permission is checked separately.
- Let maintenance impersonate the requester: expiry authority is not execution
  authority and must not unlock Context, resume, claim, or Tool use.
- Run Inbox cleanup before State settlement: an unavailable second database
  would block authoritative convergence without providing atomicity.
- Hide the table under State schema 16 or Temporal API 2: older binaries could
  accept a partial layout or claim semantics they do not implement.

## Non-claims

- The accepted implementation includes the projection, migration, Temporal
  Driver API 3, and opt-in reference-service loop. It does not claim production
  metrics, real-time expiry, or completion of every scale/operations row above.
- It provides bounded due discovery and fenced State settlement, not real-time
  expiry, exactly-once scheduling, leader election, distributed consensus, or
  a globally synchronized clock.
- It does not add finite worker leases, `NeedsReconciliation`, automatic Tool
  replay, HumanInput, multi-Tool wait release, or a frozen Context capsule.
- It does not make State and Approval Inbox atomic or complete the Inbox repair
  outbox/tombstone/scanner left open by ADR 0158.
- It does not expose event payload, Approval content, Context, Tool input,
  credentials, or secrets through the scan or metrics.
- It does not infer Waiting from legacy `Running`, process absence, a pending
  Inbox Approval, or a projection row lacking exact event evidence.
- It binds every copied wait lifecycle to one exact embedded source Thread but
  does not cryptographically attest that source against a complete multi-level
  fork/import ancestry chain.
- It does not define a public Protocol command. Remote maintenance or durable
  operation receipts require separate versioned decisions.

This decision extends the host-driven temporal boundary from
[ADR 0129](0129-host-driven-bounded-temporal-driver.md), preserves the exact
wait and State/Inbox boundaries from
[ADR 0158](0158-durable-agent-loop-waiting-and-resume.md), and follows the
fail-closed expiry precedent in
[ADR 0133](0133-durable-effect-ledger.md).
