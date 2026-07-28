# Client protocol v19

This document is the language-neutral wire specification for the current
Y-Harness client protocol. The protocol controls one headless Runtime; it does
not duplicate Agent Loop, State, Policy, or approval semantics in a client.

Protocol version `"19"` is exact. Every request carries that value, and a peer
using another value receives `unsupported_version`. Version evolution and
durable schema support are defined in
[`compatibility.md`](compatibility.md).

## Transport and framing

The application framing is newline-delimited UTF-8 JSON:

- one request object per input frame;
- one response object per request frame;
- a normal frame ends with `\n`; `\r\n` is accepted;
- a final non-empty frame at clean EOF is also processed;
- request content before the delimiter is limited to 2,097,152 bytes;
- encoded response content is limited to 16,777,216 bytes;
- stdout of a stdio host contains protocol frames only.

An oversized or malformed request still receives one error response. Its
correlation `id` is `null` when the frame could not be decoded. Response
serialization is bounded while writing; it does not first create an unbounded
JSON buffer.

Framing does not authenticate or encrypt. `serve_stdio` and the reference
`serve-demo` host use a trusted local-process principal. A network host must
authenticate before handing streams to the protocol. The optional Y-Harness
TLS host requires mutual TLS and derives the principal from the SHA-256
fingerprint of the exact client leaf certificate.

## Request and response envelopes

A request has exactly three top-level fields:

```json
{
  "id": "init-1",
  "protocol_version": "19",
  "command": {
    "method": "initialize"
  }
}
```

`id` is chosen by the client. It must contain 1–128 ASCII letters, digits,
periods, underscores, or hyphens. It correlates one response only; it is not a
durable or idempotency identity. Unknown top-level request fields are rejected.
Clients must send only the command fields specified below.

A successful response nests a typed result:

```json
{
  "id": "request-1",
  "protocol_version": "19",
  "body": {
    "status": "success",
    "result": {
      "type": "cancellation",
      "operation_id": "operation-fixture",
      "accepted": true
    }
  }
}
```

An error response has the same correlation envelope:

```json
{
  "id": "request-1",
  "protocol_version": "19",
  "body": {
    "status": "error",
    "error": {
      "code": "invalid_request",
      "message": "bounded client-safe explanation",
      "retryable": false
    }
  }
}
```

Object member order is not significant. Integer fields are non-negative JSON
integers. Opaque Thread, Turn, Operation, Approval, and event identities must
not be parsed for meaning.

## Initialization

Clients should call `initialize` before using other methods. The server checks
the exact protocol version independently on every request; initialization does
not create hidden session state.

```json
{
  "id": "init-1",
  "protocol_version": "19",
  "command": {
    "method": "initialize"
  }
}
```

The result type is `initialized`:

```json
{
  "id": "init-1",
  "protocol_version": "19",
  "body": {
    "status": "success",
    "result": {
      "type": "initialized",
      "server": "Y-Harness Engineering",
      "capabilities": [
        "operation.cancel",
        "operation.events",
        "operation.forget",
        "operation.get",
        "thread.capacity",
        "thread.create",
        "thread.events",
        "thread.fork",
        "thread.get",
        "thread.list",
        "thread.name",
        "thread.recover",
        "turn.start",
        "turn.steer"
      ],
      "compatibility": {
        "engine_version": "0.1.0",
        "state_event_schema": 11,
        "state_snapshot_schema": 11,
        "approval_inbox_schema": 2,
        "task_graph_schema": 1,
        "memory_api": 1,
        "token_counter_api": 1,
        "conversation_compactor_api": 1,
        "secret_api": 1,
        "skill_api": "1",
        "model_gateway_api": "7",
        "workspace_provider_api": "1"
      }
    }
  }
}
```

`capabilities` contains only permissions granted to the authenticated
principal. Approval permissions appear only when a durable Approval Inbox is
configured; Task permissions appear only when a durable Task Coordinator is
configured. A client must not infer a permission from a compatibility
coordinate.

## Methods

Optional JSON fields may be omitted or sent as `null`. Cursor values are
exclusive: event items have a sequence strictly greater than
`after_sequence`; recent Thread summaries have a latest sequence strictly less
than `before_sequence`.

| `method` | Command fields | Required permission | Success result `type` |
|---|---|---|---|
| `initialize` | none | `initialize` | `initialized` |
| `create_thread` | none | `thread.create` | `thread_created` |
| `fork_thread` | `parent_thread_id`, `child_thread_id`, optional `through_turn_id` | `thread.fork` | `thread_forked` |
| `list_threads` | optional `before_sequence`, optional `limit` | `thread.list` | `threads` |
| `set_thread_name` | `thread_id`, optional `name` | `thread.name` | `thread_named` |
| `get_thread` | `thread_id` | `thread.get` | `thread` |
| `recover_thread` | `thread_id`, `expected_turn_id` | `thread.recover` | `thread_recovered` |
| `get_thread_capacity` | `thread_id` | `thread.capacity` | `thread_capacity` |
| `start_turn` | `thread_id`, `prompt`, optional `memory_scope`, optional `context`, optional `timeout_ms` | `turn.start` | `turn_started` |
| `steer_turn` | `thread_id`, `expected_turn_id`, `content` | `turn.steer` | `turn_steered` |
| `get_operation` | `operation_id` | `operation.get` | `operation` |
| `get_operation_events` | `operation_id`, optional `after_sequence`, optional `limit` | `operation.events` | `operation_events` |
| `cancel_operation` | `operation_id` | `operation.cancel` | `cancellation` |
| `forget_operation` | `operation_id` | `operation.forget` | `operation_forgotten` |
| `get_events` | `thread_id`, optional `after_sequence`, optional `limit` | `thread.events` | `events` |
| `get_pending_approvals` | optional `limit` | `approval.pending` | `pending_approvals` |
| `get_approval` | `approval_id` | `approval.get` | `approval` |
| `settle_approval` | `approval_id`, `expected_revision`, `decision` | `approval.settle` | `approval_settled` |
| `create_task_graph` | `graph_id`, `definitions` | `task.graph.create` | `task_graph_created` |
| `get_task_graph` | `graph_id` | `task.graph.get` | `task_graph` |
| `get_task_records` | `graph_id`, optional `after_task_id`, optional `limit` | `task.graph.get` | `task_records` |
| `cancel_task` | `graph_id`, `task_id`, `expected_revision`, `reason` | `task.graph.cancel` | `task_cancelled` |
| `claim_tasks` | `graph_id`, `lease_duration_ms`, optional `maximum` | `task.worker.claim` | `tasks_claimed` |
| `heartbeat_task` | `graph_id`, `task_id`, `lease_id`, `lease_duration_ms` | `task.worker.heartbeat` | `task_heartbeat` |
| `complete_task` | `graph_id`, `task_id`, `lease_id`, `completion` | `task.worker.complete` | `task_completed` |
| `fail_task` | `graph_id`, `task_id`, `lease_id`, `reason` | `task.worker.fail` | `task_failed` |
| `get_task_messages` | `graph_id`, `task_id`, `lease_id`, optional `after_sequence`, optional `limit` | `task.worker.messages.read` | `task_messages` |
| `send_task_message` | `graph_id`, `task_id`, `lease_id`, `to`, `body` | `task.worker.messages.send` | `task_message_sent` |

`memory_scope` has this shape and defaults to an empty scope:

```json
{
  "project": "optional-project",
  "tenant_id": "optional-tenant",
  "tags": ["optional", "constraints"]
}
```

For the trusted local-process boundary, `tenant_id` remains an explicit
embedding-host scope. For an authenticated remote principal, the protocol
authorizer must resolve the trusted tenant: Runtime injects an omitted matching
tenant, rejects a mismatch, and rejects tenant selection by an unscoped
authenticated actor before creating Turn State. This is Memory-scope binding,
not evidence that Thread or other durable resources are tenant-partitioned.

`prompt` must contain 1–1,048,576 UTF-8 bytes after rejecting all-whitespace
input. `timeout_ms`, when present, must be greater than zero and fit the host
Runtime clock. A timeout is a total external-work deadline, not a guarantee
that non-cooperative persistence can be forcibly aborted.

`context` defaults to an empty list. Each entry is non-authoritative reference
data supplied by the authenticated Turn caller:

```json
[
  {
    "source": "branch-handoff",
    "reference": "thread:source/turn:terminal",
    "text": "Bounded derived handoff text."
  }
]
```

`source` is a validated capability-style name; `reference` is an opaque
1–4,096-byte non-control locator. A request may contain at most 64 unique
source/reference pairs and 1,048,576 aggregate source-text bytes, within the
2 MiB request-frame ceiling. Runtime prefixes and recounts every block,
computes source and model-visible SHA-256 values, and records only hashes,
charges, source/reference, and the authenticated actor in schema-11 State.
The text never becomes a user/assistant history Item or durable State body.
Changing or omitting it during deferred approval recovery changes the complete
Model request and fails closed before Tool execution.

`steer_turn` supplies additional input to one exact running Turn. The caller
must first observe its `TurnId` through `get_thread`; a stale or already sealed
identity fails without writing. A successful result acknowledges the durable
queue record:

```json
{
  "type": "turn_steered",
  "steering_id": "steering-...",
  "turn_id": "turn-..."
}
```

Acceptance does not mean immediate Model visibility. Runtime applies queued
input FIFO only at a safe Agent Loop boundary. It invalidates and resamples a
Model response crossed by newer steering, never executes a stale Tool call,
and will not complete a Turn while accepted input remains unapplied.

`set_thread_name` records an explicit operator-authored display name in the
State journal. `name` is either `null` to clear it or 1–256 trimmed,
non-control UTF-8 bytes. The result echoes only the accepted value:

```json
{
  "type": "thread_named",
  "name": "Harness design"
}
```

Names are not generated from conversation content. They appear in `thread`
and bounded `threads` projections so every client observes the same durable
metadata.

`fork_thread` creates an independent child from an exact terminal parent
boundary. `child_thread_id` is caller-chosen durable retry identity. Repeating
the same request returns the matching child; reusing it for different
provenance fails. If `through_turn_id` is absent, the fork uses the complete
settled parent prefix currently observed and rejects a running latest Turn.
When supplied, it must identify a terminal Turn and the child includes history
through that Turn only.

The Event Store commits the complete child stream atomically or leaves no
child. Historical Turn, Item, Tool-call, Approval, and Steering identities are
preserved because they denote the same already-observed evidence; no Tool or
approval effect is replayed. Thread names, Checkpoints, and an ancestor's own
lineage events are not copied. The returned `thread.lineage` records the direct
parent, parent sequence/version boundary, and SHA-256 of the exact parent event
prefix. A child can immediately continue with new Turns without mutating its
parent.

An approval decision is immutable:

```json
{ "action": "approve" }
```

or:

```json
{
  "action": "deny",
  "reason": "bounded explanation"
}
```

`expected_revision` begins at one. Settlement uses compare-and-swap; a stale
revision returns `approval_conflict`. The authenticated transport principal,
not a request body field, becomes the deciding actor. The immutable requester
cannot settle its own request.

`recover_thread` is an explicit takeover mutation, never a read-side or
startup side effect. The caller must first establish that the previous worker
has stopped and that it owns the Thread exclusively. `expected_turn_id` fences
the exact abandoned running Turn the caller observed and is rechecked at the
State optimistic-commit boundary. A stale identity, a terminal status other
than `interrupted`, or a live operation in the same Protocol host fails without
mutation. Retrying the same request after that Turn is already `interrupted`
is idempotent while no newer Turn is running.

Recovery appends an `interrupted` terminal event and abandons approvals the old
Turn can no longer consume. It does not resume the interrupted stack,
synthesize a Tool result, or replay Model/Tool work. Starting a replacement
Turn remains a separate `start_turn` request. Network hosts should grant
`thread.recover` only to a principal whose surrounding control plane can prove
the exclusive takeover condition; the permission itself is not a distributed
lease.

## Thread and operation lifecycle

The normal client sequence is:

```text
initialize
  → create_thread
  → optional set_thread_name
  → start_turn
  → optional steer_turn while running
  → get_operation_events / get_operation
  → get_events
  → forget_operation
```

When the Event Store advertises `thread.list`, `list_threads` returns at most
64 bounded summaries ordered by the latest global State sequence,
newest first:

```json
{
  "type": "threads",
  "threads": [{
    "thread_id": "thread-...",
    "name": "Harness design",
    "lineage": {
      "parent_thread_id": "thread-parent",
      "parent_through_sequence": 42,
      "parent_stream_version": 7,
      "parent_events_sha256": "lowercase-64-character-sha256"
    },
    "last_sequence": 50,
    "updated_at_ms": 1785142800000,
    "stream_version": 9
  }],
  "next_before_sequence": 50,
  "has_more": true
}
```

Pass the returned `next_before_sequence` as `before_sequence` to request the
next older page. Paging is a live view, not a snapshot transaction: concurrent
Thread updates can move entries toward the front. A client should restart at
the first page when refreshing. Summaries contain no message content. Protocol
16 adds the optional direct `lineage` already present on forked Thread
projections, allowing a client to build a forest from the bounded page without
loading full histories. A root Thread omits it; an ancestor outside the current
page remains an opaque parent identity. Summaries do not replace `get_thread`.

`start_turn` returns immediately with a process-local `operation_id`. It does
not return the terminal Turn:

```json
{
  "type": "turn_started",
  "operation_id": "operation-..."
}
```

`get_operation` returns one of these tagged states:

- `{"status":"running","thread_id":"thread-..."}`;
- `{"status":"completed","thread_id":"thread-...","turn_id":"turn-...","final_text":"..."}`;
- `{"status":"failed","error":"..."}`;
- `{"status":"cancelled","error":"..."}`;
- `{"status":"timed_out","error":"..."}`.

`cancel_operation` requests cooperative cancellation. Its `accepted` field is
`true` only when the operation was running when checked. Clients must continue
polling until a terminal status. `forget_operation` succeeds only for a
terminal operation and releases process-local retention capacity.

Operation IDs and provisional model events are disposable. They do not survive
service restart. Thread events are authoritative: after reconnect or restart,
a client reconciles with `get_thread` and `get_events`. Recovery marks
unfinished work interrupted and never generically replays an uncertain Tool
effect.

If `get_operation_events` returns
`{"type":"step_invalidated","model_step":N}`, clients must discard provisional
text previously emitted for model step `N`. Authoritative State and the
terminal operation result remain the reconciliation source.

## Task Graph and worker lifecycle

Task orchestration is optional and appears only when the host installs a
durable `TaskCoordinator`. The protocol exposes coordination, not arbitrary
remote process launch. A worker executes a returned Task through its own
host-controlled executor and settles only through the fenced commands below.

The normal sequence is:

```text
create_task_graph
  → claim_tasks
  → get_task_messages / send_task_message / heartbeat_task
  → complete_task or fail_task
  → get_task_graph / get_task_records
```

`create_task_graph` accepts the complete DAG and a caller-selected stable
`graph_id`. Creation begins at revision one and never replaces an existing
identity. If a response is lost, reconcile with `get_task_graph`; repeating
creation is not treated as a successful idempotent replay.

A Task definition has this public domain shape:

```json
{
  "id": "compile",
  "description": "Compile the workspace",
  "dependencies": ["prepare"],
  "priority": 10,
  "workspace": "isolated"
}
```

`workspace` is `none`, `isolated`, or `shared_read_only`. The graph validates
identity uniqueness, dependency existence, and acyclicity before it is made
durable. `get_task_graph` returns either `null` or bounded metadata:

```json
{
  "type": "task_graph",
  "graph": {
    "graph_id": "build-42",
    "revision": 7,
    "task_count": 2,
    "terminal": true,
    "materialization_charge_bytes": 4096,
    "remaining_materialization_bytes": 67104768
  }
}
```

`get_task_records` returns identity-ordered records, an optional
`next_after_task_id`, and `has_more`. Its cursor is exclusive. Every record
contains its immutable `definition`, `attempts`, and one tagged `status`:
`pending`, `running`, `completed`, `failed`, `cancelled`, or `blocked`.

`claim_tasks` derives the lease owner from the authenticated transport
principal. It is always `local-process` for trusted stdio or the exact
lowercase SHA-256 client-certificate fingerprint for mTLS. No request field can
select or override that worker identity. A successful result includes the
durable graph revision, derived worker, and fenced claims:

```json
{
  "type": "tasks_claimed",
  "graph_id": "build-42",
  "revision": 2,
  "worker": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "claims": [{
    "task": {
      "id": "prepare",
      "description": "Prepare inputs",
      "dependencies": [],
      "priority": 0,
      "workspace": "none"
    },
    "lease": {
      "id": "lease-...",
      "owner": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "attempt": 1,
      "expires_at_ms": 1780000000000
    }
  }]
}
```

Lease expiration is computed only from the server clock. `heartbeat_task`,
`complete_task`, `fail_task`, `get_task_messages`, and `send_task_message`
revalidate the exact current Task, fencing token, authenticated owner, and
unexpired deadline. A stale, expired, or different principal cannot reuse a
lease. Worker mutations retry bounded internal compare-and-swap conflicts only
after reloading and revalidating the lease; other errors are not replayed.

`cancel_task` is an operator command, not a worker lease command. It requires
the positive graph revision observed by the caller. A stale revision returns
the retryable `orchestration_conflict`; the client must reload before deciding
whether cancellation is still appropriate.

## Paging

`get_operation_events` returns:

```json
{
  "type": "operation_events",
  "events": [
    {
      "sequence": 8,
      "event": {
        "type": "text_delta",
        "model_step": 1,
        "delta": "partial text"
      }
    }
  ],
  "next_after_sequence": 8,
  "has_more": false,
  "dropped_through_sequence": null
}
```

Operation event sequence numbers are local to one retained operation. The
buffer retains at most 4,096 events and 1,048,576 delta bytes. When older
events were evicted, `dropped_through_sequence` is the highest lost sequence;
the terminal operation result and authoritative State remain valid.

The default/max operation-event page is 16/32. The default/max State-event page
is 16/32. `get_events` uses durable global event sequences and also stops
before the response byte budget. Both results return
`next_after_sequence` and `has_more`.

The default/max pending-approval page is 8/16. Pending approvals are ordered
oldest first. Approval records are durable and revisioned; operation events
are not.

The default/max Task-record page is 16/64 and is also bounded by the response
byte ceiling. The default/max Task claim count is 1/16; the encoded claims are
checked before the graph mutation is committed. A Task lease duration must be
1–604,800,000 milliseconds. The default/max Task-message page is 32/256 and
the domain page is capped at 2,097,152 encoded bytes.

## Domain payloads

The protocol serializes the same public domain records used by an embedded
host:

- a Thread is `{id, name?, lineage?, created_at_ms, turns, checkpoints}`;
- a Thread summary is
  `{thread_id, name?, lineage?, last_sequence, updated_at_ms, stream_version}`;
- a Turn is `{id, thread_id, status, items}`;
- an Item is tagged by `type`;
- a Stored Event carries
  `{schema_version, sequence, event_id, thread_id, recorded_at_ms, ...event}`;
- a State capacity result reports used/remaining event and recovery-byte
  budgets plus `healthy`, `warning`, `critical`, `terminal_only`, or
  `exhausted`;
- an Approval record carries
  `{schema_version, request, status, revision, requested_at_ms, settled_at_ms}`.
- a Task Graph summary carries revision, count, terminal state, and bounded
  materialization accounting;
- a Task claim carries the immutable Task definition and current fenced lease;
- a Task message carries graph-local sequence, sender, recipient, body, and
  server-clock creation time.

State event schema 4 binds every `policy_decision` to the exact trust-bearing
origin of the registered Tool evaluated by Policy:

```json
{
  "type": "policy_decision",
  "call_id": "call-...",
  "tool_origin": {
    "kind": "external",
    "id": "operator-registration"
  },
  "decision": {
    "action": "allow"
  }
}
```

State event schema 5 adds a bounded, non-executable Provider Continuation Item
bound to the exact registered Model identity and origin:

```json
{
  "type": "provider_continuation",
  "model_id": "openai/default",
  "model_origin": {
    "kind": "built_in"
  },
  "continuation": {
    "format": "openai.responses.reasoning.v1",
    "items": [
      {
        "type": "reasoning",
        "encrypted_content": "<opaque>"
      }
    ]
  }
}
```

Clients must treat `continuation.items` as opaque provider state. Product UIs
should not render its body. Runtime routing and provider adapters, rather than
clients, validate and replay it.

State event schema 6 adds two correlated steering Items:

```json
{
  "type": "steering_queued",
  "steering_id": "steering-...",
  "submitted_by": {
    "kind": "authenticated",
    "authority": "mtls-certificate-sha256",
    "subject": "<client-leaf-fingerprint>"
  },
  "content": "correct course"
}
```

```json
{
  "type": "steering_applied",
  "steering_id": "steering-...",
  "content": "correct course"
}
```

`steering_queued` is evidence of acceptance and is not Model-visible.
`steering_applied` must match the oldest pending identity and exact content; it
projects as user input for the following Model step. A completed Turn cannot
contain unapplied steering.

State event schema 7 adds one atomic event for all Tool calls proposed by the
same Model response:

```json
{
  "type": "tool_calls_appended",
  "turn_id": "turn-...",
  "calls": [
    {
      "id": "item-...",
      "created_at_ms": 1785081600000,
      "type": "tool_call",
      "model_id": "openai/default",
      "model_origin": {
        "kind": "built_in"
      },
      "call_id": "call-1",
      "name": "read",
      "input": {
        "path": "README.md"
      },
      "batch": {
        "id": "tool-batch-...",
        "index": 0,
        "size": 2
      }
    }
  ]
}
```

The actual `calls` array contains exactly `size` Items in source order, with
indexes zero through `size - 1`, one shared batch identity, and unique call correlations.
The event contains 2–64 calls. `item_appended` cannot carry batch metadata, so
a client must not flatten or partially apply this event.

State event schema 8 adds the explicit Thread-name transition:

```json
{
  "type": "thread_named",
  "name": "Harness design"
}
```

`name: null` clears the name. The authoritative event is projected into
`Thread.name`; SQLite maintains `streams.name` only as a transactionally
consistent recent-list index and fails closed when it drifts from the journal.

State event schema 9 adds direct immutable fork provenance:

```json
{
  "type": "thread_forked",
  "lineage": {
    "parent_thread_id": "thread-parent",
    "parent_through_sequence": 42,
    "parent_stream_version": 7,
    "parent_events_sha256": "lowercase-64-character-sha256"
  }
}
```

It must immediately follow the child's `thread_created` event. Fork snapshots
reconstruct this event exactly; copied parent Checkpoints never enter the
child.

State event schema 10 adds immutable import provenance:

```json
{
  "type": "thread_imported",
  "origin": {
    "source_thread_id": "thread-source",
    "source_stream_version": 7,
    "source_last_sequence": 42,
    "source_events_sha256": "lowercase-64-character-sha256"
  }
}
```

It must immediately follow the target `thread_created` event. Optional
`source_lineage` preserves source fork evidence but does not populate the
target's local `lineage`. Portable archive encoding and file transfer remain
embedded/CLI concerns; Protocol 18 only transports the resulting Thread and
State-event projections.

State event schema 11 adds content-free invocation-context evidence:

```json
{
  "type": "invocation_context",
  "submitted_by": { "kind": "local_process" },
  "blocks": [
    {
      "source": "branch-handoff",
      "reference": "thread:source/turn:terminal",
      "source_sha256": "lowercase-64-character-sha256",
      "content_sha256": "lowercase-64-character-sha256",
      "estimated_tokens": 42,
      "serialized_bytes": 168
    }
  ]
}
```

This evidence is excluded from model-visible conversation replay. The typed
Model Context block carries `source.type = "invocation"` so gateways keep it
at ordinary caller/evidence authority rather than treating it as Skill
instructions.

`tool_origin` is also present for `deny` and `ask`, so authorization provenance
does not depend on Tool execution succeeding. It may be absent only in
immutable schema-1, schema-2, or schema-3 history.

The exact current definitions are the serialized public contracts in
[`kernel/types.rs`](../src/kernel/types.rs),
[`state/mod.rs`](../src/state/mod.rs), and
[`approval/mod.rs`](../src/approval/mod.rs), and
[`orchestration/mod.rs`](../src/orchestration/mod.rs). A change that alters a domain
record observable through this protocol requires the compatibility action
defined in [`compatibility.md`](compatibility.md).

## Bounds and retention

| Boundary | Protocol v19 value |
|---|---:|
| Request frame | 2,097,152 bytes |
| Response frame | 16,777,216 bytes |
| Request `id` | 1–128 restricted ASCII bytes |
| Opaque command identity | 1–256 bytes |
| Thread name | 1–256 trimmed non-control bytes, or null |
| Prompt | 1–1,048,576 bytes |
| Per-Turn context | 64 blocks; 1,048,576 source bytes and tokens; 4,096-byte reference |
| Retained Operations | 64 default; 4,096 configurable maximum |
| Operation stream | 4,096 events and 1,048,576 delta bytes |
| Operation-event page | 16 default; 32 maximum |
| State-event page | 16 default; 32 maximum plus response-byte ceiling |
| Pending-approval page | 8 default; 16 maximum |
| Task-record page | 16 default; 64 maximum plus response-byte ceiling |
| Task claims | 1 default; 16 maximum plus pre-commit response-byte ceiling |
| Task lease | 1–604,800,000 milliseconds, server-clock deadline |
| Task-message page | 32 default; 256 maximum and 2,097,152 encoded bytes |
| Client-safe error | 4,096 Unicode scalar values plus optional ellipsis |
| Host shutdown drain | 30 seconds default; 1 hour configurable maximum |

Reaching Operation retention capacity rejects a new Turn before execution.
Clients release terminal capacity explicitly. Shutdown is one-way: it rejects
new Turns, asks running operations to cancel, waits within one deadline, and
then drains Runtime snapshot maintenance with the time that remains.

## Errors and retry

| `code` | Meaning |
|---|---|
| `invalid_json` | Frame is not a decodable request object |
| `frame_too_large` | Request exceeds the input frame limit |
| `response_too_large` | Result could not fit the output frame limit |
| `unsupported_version` | Request protocol is not exactly `"19"` |
| `invalid_request_id` | Correlation ID violates its syntax or bound |
| `forbidden` | Principal lacks the exact command permission |
| `invalid_request` | Command fields, lifecycle, identity, or target are invalid |
| `runtime_overloaded` | Runtime admission is at capacity |
| `state_conflict` | Authoritative State compare-and-swap lost |
| `orchestration_conflict` | Task Coordinator revision/fencing conflict |
| `approval_conflict` | Approval revision or settlement conflict |
| `runtime_error` | Other bounded provider or Runtime failure |

Clients use the response's `retryable` boolean as the authority. Overload and
conflict categories are retryable; validation, authorization, framing, and
version failures are not. A retryable response does not make a side effect
safe to repeat: clients must reconcile durable State before retrying work whose
Tool-effect status is uncertain.

## Conformance evidence

The protocol module contains wire-shape regression tests for both envelopes,
schema-11 invocation-context evidence, schema-10 Thread-import evidence,
schema-9 Thread-fork evidence, schema-8
Thread-name evidence, schema-7 Tool-call batch evidence, schema-6
Steering evidence, schema-5 Provider Continuation evidence, schema-4
Tool-origin evidence, all method tags, and their permission mapping. Turn
integration tests prove exact-ID durable steering acceptance. Task integration
tests prove conditional discovery, bounded cursor
paging, principal-derived ownership, cross-principal fencing, server-clock
heartbeat, messaging, terminal recovery, and explicit-revision cancellation.
Process tests additionally prove stdout purity, one response per request,
and asynchronous Turn control. The independent `y-harness-tui` package has
TestBackend rendering tests and a real-PTY smoke gate against `yh serve-demo`;
it does not call Runtime internals or open State storage. The optional TLS
integration tests prove that the same handler requires and authenticates client
certificates.
