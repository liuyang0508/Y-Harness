# ADR 0096: Attributed, bounded per-Turn context

- Status: accepted
- Date: 2026-07-28

## Context

Pi's coding product stores an entry DAG inside one session. Navigating to a
different leaf can summarize entries abandoned between the old leaf and the
common ancestor, then append that summary to the selected branch. The behavior
is useful, but its mutable leaf and entry-parent graph are part of Pi's product
session model.

Y-Harness instead makes each branch an independently recoverable Thread.
`ThreadForked` is the only branch authority, and a client selects a Thread
rather than mutating an in-Thread leaf pointer. Copying Pi's navigation model
would introduce a second branch truth. Treating a branch summary as a user or
assistant message would also misstate who said it.

The reusable Harness requirement is lower-level: an embedding host,
orchestrator, RAG adapter, or optional client needs to supply bounded reference
context to one Turn without impersonating Skill instructions, Memory, or
conversation history.

## Decision

- Add `TurnContextInput { source, reference, text }` to
  `TurnExecutionOptions` and protocol `start_turn`. The field is optional and
  defaults to an empty list.
- Accept at most 64 unique source/reference pairs and 1 MiB of source text.
  Validate identity, metadata, count, and bytes before claiming the Thread or
  writing Turn State.
- Context Engine prefixes every block with a fixed non-authoritative-data
  warning, recounts it with the selected Token Counter, enforces an independent
  aggregate token bound, and computes SHA-256 for both the source text and the
  exact model-visible block.
- Add `ContextSource::Invocation` so a Model gateway can distinguish this data
  from digest-pinned Skill instructions. Advance the exact model-gateway
  contract to API 7.
- Record one schema-11 `invocation_context` Item containing the authenticated
  Turn actor, ordered source/reference pairs, hashes, and byte/token charges.
  The source text is not journaled and the evidence Item is never replayed as
  conversation.
- Advance the exact client protocol to 18. A service caller needs only its
  existing `turn.start` permission; attribution comes from the authenticated
  protocol principal rather than a caller-controlled actor field.
- In the direct OpenAI adapter, only Skill blocks populate provider
  `instructions`. Memory, conversation summaries, and invocation context are
  emitted as explicitly marked user-level reference data. A custom gateway
  must preserve the same authority distinction using the typed Context source.
- Approval recovery requires the caller to resupply byte-identical context.
  Runtime recompiles it and compares the complete Model-request SHA-256 before
  any deferred Tool effect. A missing or changed block fails closed.

## Consequences

Branch handoff becomes one application of a general Context primitive. A
client may summarize an abandoned Thread suffix and submit the result with
`source = "branch-handoff"` and an opaque source boundary reference. The
Engine does not infer navigation intent, move a leaf, or make the derived text
authoritative.

The context body is deliberately ephemeral. Durable knowledge belongs in a
Memory Provider; durable conversation belongs in Thread State. Hashes make the
exact ephemeral input auditable without placing potentially private retrieved
material in the journal.

This decision does not synthesize branch summaries, implement entry-level
navigation, or add another compactor contract. The existing Conversation
Compactor remains responsible only for bounded omitted history within one
Thread. ADR 0097 subsequently adds a read-only, digest-bound Thread-delta
preparation API; a host-selected summarizer still produces the optional
candidate before this invocation-context contract is used.

## Rejected alternatives

- Add Pi's mutable entry DAG inside a Thread: duplicates branch authority and
  weakens independent recovery.
- Persist summary text as a synthetic user or assistant Item: invents a
  speaker and contaminates authoritative conversation.
- Let callers submit raw `ContextBlock`: they could forge Skill, Memory, or
  conversation-summary provenance.
- Put caller context in provider instructions: it elevates ordinary
  `turn.start` input above its authenticated authority.
- Store the full context body: it duplicates external sources, increases State
  retention, and can persist private RAG material unexpectedly.

## Evidence

- Context tests cover prefixing, double-digest provenance, uniqueness, and
  bounds.
- Runtime tests prove model visibility, actor-attributed content-free State,
  and rejection before Turn creation.
- State tests reject invocation-context evidence before schema 11.
- OpenAI adapter tests prove only Skill blocks receive instruction authority.
- Protocol wire tests cover the optional typed input.

## Sources

- [Pi branch summarization at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/harness/compaction/branch-summarization.ts)
- [Pi tree navigation at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/harness/agent-harness.ts)
- [ADR 0093: atomic Thread fork and lineage](0093-atomic-thread-fork-and-lineage.md)
- [ADR 0094: lineage-aware Thread navigation](0094-lineage-aware-bounded-thread-navigation.md)
- [ADR 0097: bounded Thread handoff preparation](0097-bounded-digest-bound-thread-handoff.md)
