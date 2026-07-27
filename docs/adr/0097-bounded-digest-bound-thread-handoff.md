# ADR 0097: Bounded, digest-bound Thread handoff preparation

- Status: accepted
- Date: 2026-07-28

## Context

ADR 0096 established attributed per-Turn Context as the general substrate for
RAG, orchestration, and branch handoff. It deliberately did not copy Pi's
mutable session-entry DAG or make generated branch summaries authoritative.

An embedding host could already construct a `TurnContextInput`, but doing so
left every host to rediscover the source/target divergence, whole-Turn
selection, byte bounds, and provenance reference. Moving summary generation
into the kernel would create a model dependency and another provider lifecycle
for an optional convenience. Reusing the Conversation Compactor would also
misstate its contract: that extension summarizes omitted history within one
Thread, while a handoff crosses two independently recoverable Threads.

## Decision

- Add format-1 `ThreadHandoffRequest` and `ThreadHandoffConfig` to the Context
  Engine API.
- `prepare(source, target, config)` requires stable terminal projections and
  computes the longest prefix whose Turn IDs, terminal status, and Items are
  identical. Owning Thread IDs are intentionally excluded because a fork
  materializes inherited Turns under the child Thread.
- Select only model-visible source Turns after that prefix. Keep the newest
  whole Turns within independently validated count and canonical-JSON byte
  bounds. Defaults are 64 Turns and 1 MiB; hard ceilings are 256 Turns and
  8 MiB.
- A zero-length shared prefix is valid. The request is a generic cross-Thread
  handoff and does not claim that two Threads share authoritative lineage.
  When the source has no model-visible delta, return `None`.
- Bind source and target identities, shared-prefix length, included-Turn count,
  omitted older-Turn count, and the exact bounded Turn input through SHA-256.
  The request remains serializable so any host-selected summarizer can consume
  it.
- `to_context(summary)` revalidates the request digest, rejects an empty or
  oversized result, adds an explicit derived/non-authoritative marker, and
  returns `source = "thread-handoff"` with a bounded canonical provenance
  reference.
- Add `HarnessRuntime::prepare_thread_handoff` as a read-only convenience that
  loads both authoritative Thread projections. It writes no State.
- Summary synthesis remains outside the kernel. The host may use any Model,
  process, or service and then submit the resulting `TurnContextInput` through
  normal Turn execution. Runtime independently prefixes, token-counts, hashes,
  attributes, and journals content-free invocation evidence.
- This additive embedded API changes no Protocol, State, snapshot, or Model
  Gateway compatibility coordinate.

## Consequences

Pi's useful abandoned-branch-summary behavior now has a general Harness
primitive without adding an entry leaf, navigation state, or a special
summarizer registry. The same request works for sibling branches, ancestor
handoff, or unrelated Threads; only actual shared Turn evidence is called a
shared prefix.

The engine proves which bounded source material a summary candidate was based
on, not that the candidate is factually correct. Consequential claims still
require Verification or authoritative sources. Preparation does not
automatically navigate, start a Turn, call a Model, or persist the summary.

## Rejected alternatives

- Add Pi's session entry DAG: duplicates Y-Harness Thread authority.
- Reuse `ConversationCompactor`: conflates within-Thread omitted history with
  cross-Thread handoff semantics.
- Add a dedicated summarizer registry: unnecessary until independent
  lifecycle, trust, or routing requirements are demonstrated.
- Persist the generated summary as a user or assistant Item: invents a speaker
  and contaminates authoritative conversation.
- Require a non-empty common prefix: prevents legitimate root-level and
  unrelated-Thread handoff without adding safety; the output is already
  explicitly non-authoritative.

## Evidence

- Context tests cover shared-prefix extraction, unrelated Threads, newest-Turn
  truncation, omission counts, digest binding, empty delta, unstable Threads,
  byte bounds, and tamper rejection.
- Runtime tests prove authoritative State loading and no mutation during
  preparation.
- Existing invocation-context tests cover provider visibility, attribution,
  content-free State, and approval-resume digest equality.

## Sources

- [Pi branch summarization at `cee5ff7`](https://github.com/earendil-works/pi/blob/cee5ff7520d8828bed9955ef00419e995d1f91e0/packages/agent/src/harness/compaction/branch-summarization.ts)
- [ADR 0093: atomic Thread fork and lineage](0093-atomic-thread-fork-and-lineage.md)
- [ADR 0096: attributed per-Turn context](0096-attributed-per-turn-context.md)
