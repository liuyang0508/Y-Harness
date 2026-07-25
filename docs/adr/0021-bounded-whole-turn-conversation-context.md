# ADR 0021: Bounded whole-Turn conversation context

- Status: Accepted
- Date: 2026-07-25

## Context

The initial Agent Loop sent only Items from the active Turn. A second Turn in
the same Thread therefore had durable history in State but no conversational
history in its `ModelRequest`. Protocol prompt limits also did not protect
embedded callers, provider Context, Tool output, or accumulated model requests.

## Decision

- Compile a deterministic suffix of previous whole Turns before starting a new
  Turn. The default window is at most 32 Turns and 65,536 conservative budget
  units.
- Charge serialized Item JSON bytes as an upper-budget token heuristic. This is
  intentionally conservative and does not pretend to be a provider tokenizer.
- Stop at the first older Turn that would exceed the Turn count or byte budget;
  never slice an Item or include an older Turn while dropping a newer one.
- Admit only User messages, Assistant messages, Tool calls/results, and
  Verification feedback to model-visible history. Policy, approval, memory
  observations, context-window observations, runtime errors, and stop evidence
  remain authoritative State but are not conversation.
- Journal the included Turn identities, dropped-Turn count, and estimated
  budget without duplicating conversation content.
- Keep long-term Memory blocks separate from conversation Items.
- Enforce hard Runtime boundaries independent of transport:
  - prompt: 1 MiB;
  - one Context block: 1 MiB;
  - all Context text: 8 MiB and at most 512 blocks;
  - one Tool output: 1 MiB;
  - complete serialized Model request: 16 MiB;
  - Runtime error evidence: 4,096 characters;
  - Agent Loop: at most 256 model steps.
- Bound memory selection configuration, scope metadata, candidate count,
  references, detail locators, provenance, and warnings before provider output
  enters compiled Context or State evidence.
- Convert an oversized Tool success into a bounded `ToolResult` error so the
  model can observe the failure without the payload entering State.

## Consequences

Multi-Turn Thread semantics now work through the same projected State used for
recovery. Context selection is reproducible and its omissions are visible in
the journal. A provider cannot bypass byte limits by under-reporting tokens.

Provider-specific token counting was added later by ADR 0059 while preserving
the hard byte ceilings. ADR 0060 adds opt-in bounded semantic compaction for a
newest slice of omitted whole Turns while keeping this raw suffix and the
original State history authoritative.

## Rejected alternatives

- Send the complete Thread forever: unbounded cost and eventual provider
  rejection.
- Select individual newest Items: can separate a Tool result from its call or a
  Verification failure from its candidate.
- Trust only provider token estimates: malicious or buggy sources could inject
  arbitrarily large text.
- Rely only on protocol frame limits: embedded callers and in-process
  capabilities bypass the transport.
- Persist an oversized Tool value and truncate later: State growth has already
  occurred and the retained JSON may misrepresent the actual result.
