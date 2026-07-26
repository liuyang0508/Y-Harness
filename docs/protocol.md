# Client protocol v12

This document is the language-neutral wire specification for the current
Y-Harness client protocol. The protocol controls one headless Runtime; it does
not duplicate Agent Loop, State, Policy, or approval semantics in a client.

Protocol version `"12"` is exact. Every request carries that value, and a peer
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
  "protocol_version": "12",
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
  "protocol_version": "12",
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
  "protocol_version": "12",
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
  "protocol_version": "12",
  "command": {
    "method": "initialize"
  }
}
```

The result type is `initialized`:

```json
{
  "id": "init-1",
  "protocol_version": "12",
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
        "thread.get",
        "turn.start",
        "turn.steer"
      ],
      "compatibility": {
        "engine_version": "0.1.0",
        "state_event_schema": 6,
        "state_snapshot_schema": 6,
        "approval_inbox_schema": 2,
        "task_graph_schema": 1,
        "memory_api": 1,
        "token_counter_api": 1,
        "conversation_compactor_api": 1,
        "secret_api": 1,
        "skill_api": "1",
        "model_gateway_api": "5",
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
exclusive: a returned item must have a sequence strictly greater than
`after_sequence`.

| `method` | Command fields | Required permission | Success result `type` |
|---|---|---|---|
| `initialize` | none | `initialize` | `initialized` |
| `create_thread` | none | `thread.create` | `thread_created` |
| `get_thread` | `thread_id` | `thread.get` | `thread` |
| `get_thread_capacity` | `thread_id` | `thread.capacity` | `thread_capacity` |
| `start_turn` | `thread_id`, `prompt`, optional `memory_scope`, optional `timeout_ms` | `turn.start` | `turn_started` |
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

`prompt` must contain 1–1,048,576 UTF-8 bytes after rejecting all-whitespace
input. `timeout_ms`, when present, must be greater than zero and fit the host
Runtime clock. A timeout is a total external-work deadline, not a guarantee
that non-cooperative persistence can be forcibly aborted.

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

## Thread and operation lifecycle

The normal client sequence is:

```text
initialize
  → create_thread
  → start_turn
  → optional steer_turn while running
  → get_operation_events / get_operation
  → get_events
  → forget_operation
```

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

- a Thread is `{id, created_at_ms, turns, checkpoints}`;
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

| Boundary | Protocol v12 value |
|---|---:|
| Request frame | 2,097,152 bytes |
| Response frame | 16,777,216 bytes |
| Request `id` | 1–128 restricted ASCII bytes |
| Opaque command identity | 1–256 bytes |
| Prompt | 1–1,048,576 bytes |
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
| `unsupported_version` | Request protocol is not exactly `"12"` |
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
schema-6 Steering evidence, schema-5 Provider Continuation evidence, schema-4
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
