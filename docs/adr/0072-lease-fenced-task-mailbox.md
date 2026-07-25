# ADR 0072: Lease-fenced Task Mailbox

- Status: Accepted
- Date: 2026-07-25

## Context

`TaskGraph` stored bounded, globally ordered messages, but a running
`TaskExecutor` had no governed way to read or send them. A host could retain a
coordinator beside its executor and mutate the graph directly, but that would
duplicate CAS retry logic and could allow a fenced or completed attempt to
publish late messages.

Cloning an entire receiver history into `TaskExecutionRequest` is also unsafe.
One graph may contain 100,000 messages, and message bodies may be 64 KiB each.

## Decision

- Add `TaskMessagePage` and `TaskGraph::messages_page_for`. One page contains
  1–256 receiver messages, is capped at 2 MiB of encoded message charge, follows
  graph-global sequence order, and reports its next cursor and whether more
  messages exist.
- Add a concrete `TaskMailbox` to every `TaskExecutionRequest`. It retains the
  coordinator, graph identity, exact claim, and the Task cancellation token;
  executor code does not receive direct mutable graph access.
- Before every inbox read or send, reject a cancelled execution and reload the
  authoritative graph. Require the exact lease ID, owner, attempt, and
  unexpired deadline.
- Persist sends as ordinary `TaskGraph::send_message` mutations through
  coordinator CAS. On an explicit CAS conflict, reload and recompute against
  the same exact lease, up to the shared 64-attempt contention limit.
- Do not retry after an ambiguous coordinator error. A successful message is
  durable immediately and may remain even if the sender later fails; messages
  are coordination evidence, not part of Task completion.
- Reuse the execution cancellation token. A Mailbox cloned into detached work
  rejects new access after executor completion, timeout, fencing, or scheduler
  cancellation.

## Consequences

Sub-Agents can exchange durable messages during actual scheduled execution
without bypassing Task Graph validation, aggregate capacity, optimistic
concurrency, or stale-worker fencing. Receivers page large inboxes instead of
materializing the graph's complete message history.

Mailbox operations are not a distributed transaction with arbitrary external
effects or final Task completion. They provide durable ordered delivery, not
exactly-once consumption, acknowledgements, deletion, broadcast, or secret
storage. Applications needing those semantics must build them explicitly over
message IDs and sequence cursors.

Task Graph schema 1 and Protocol 9 remain unchanged. `TaskMessage` already
existed in the durable graph; the page and Mailbox are embedded Rust API
surfaces.

## Rejected alternatives

- Clone every message into the execution request: violates bounded allocation.
- Give the executor a mutable snapshot: bypasses coordinator CAS and fencing.
- Buffer outbound messages until Task completion: loses useful failure
  evidence and couples communication to terminal success.
- Retry every transport/storage error: an error after an uncertain commit can
  duplicate a message.
