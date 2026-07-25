# ADR 0060: Bounded non-authoritative semantic conversation compaction

- Status: Accepted; persistence and version coordinates superseded by ADR 0061
- Date: 2026-07-25

## Context

Whole-Turn suffix selection is deterministic and safe, but it discards older
model-visible history once the count, token, or byte boundary is reached.
Semantic summaries can preserve useful intent across a long Thread, but a
summary is fallible generated content: it can omit facts, amplify prompt
injection, drift between providers, fail, hang, or panic. It must never replace
the append-only State history or be presented as an authoritative fact source.

Persisting new summary evidence inside State schema 1 would silently change the
durable contract. The compatibility policy requires migration tooling before
the first State schema change.

ADR 0061 later supplied that migration evidence. It supersedes only this ADR's
ephemeral-provenance and version-coordinate decisions; the summary body remains
ephemeral and non-authoritative.

## Decision

- Add exact version-1 `ConversationCompactor`,
  `ConversationCompactorDescriptor`, and `ConversationCompactorRegistry`
  contracts with frozen metadata, trust-bearing origins, collision rejection,
  and the shared 4,096-capability ceiling.
- Require explicit compactor selection and explicit input-Turn, input-byte,
  output-token, and output-byte budgets. No compactor is selected implicitly
  from a model name.
- Preserve the newest raw whole-Turn suffix. Supply only the newest bounded
  slice of omitted whole Turns to the compactor, in chronological order, and
  report the number of still-older omitted Turns separately.
- Run the asynchronous compactor inside the Runtime's Context phase so the Turn
  deadline, cancellation signal, future construction/poll/drop panic boundary,
  content-free observation, and ordinary failed settlement all apply.
- Fail the Turn when a configured compactor fails or violates its contract.
  Silent degradation could make a partial summary appear complete.
- Prefix every model-visible result with an engine-owned statement that it is
  derived, non-authoritative context and that consequential claims require
  verification against retained conversation or authoritative State.
- Attach exact covered Turn IDs, the uncovered older-Turn count, a SHA-256 of
  the canonical covered input, and a SHA-256 of the exact model-visible summary
  to `ContextSource::ConversationSummary`.
- Recount the final header plus summary with the selected provider Token
  Counter and independently enforce the byte ceiling. Existing aggregate
  Context and complete Model-request byte bounds remain authoritative.
- Never modify, replace, or duplicate original conversation Items in State.
  Schema-1 `ConversationContext` continues to record the retained suffix and
  omitted count. This ADR originally kept summary text and provenance ephemeral;
  ADR 0061 now persists only content-free provenance under schema 2.
- This ADR originally advanced the exact client protocol from 4 to 5 and
  advertised
  `conversation_compactor_api = 1`. Advance the HTTPS model-gateway API from 1
  to 2 because its serialized `ContextSource` can now contain
  `conversation_summary`. It kept State event schema 1 and snapshot schema 2;
  ADR 0061 supersedes those three local coordinates.

## Consequences

Hosts can install an LLM-backed or deterministic summarizer without coupling
Core to one model vendor. The model sees a bounded, explicitly derived view;
State recovery and audit always retain the original inputs. Compactor identity,
duration, and settlement are available as content-free Context observations.

The engine verifies coverage, identity, digests, and budgets, not semantic
truth. Summary-body persistence, exact replay/caching, semantic faithfulness
verification, and cross-process reuse remain future work. In-process providers
can allocate before returning; untrusted executable providers therefore still
belong behind the existing out-of-process extension boundary.

Protocol-v4 clients and model-gateway-v1 peers must fail exact negotiation.
No durable migration is needed because authoritative State schemas did not
change.

ADR 0061 subsequently advances State to schema 2, snapshots to schema 3, and
the client protocol to 6. It persists only bounded content-free summary
provenance; it does not persist the summary body or alter original Items.

## Rejected alternatives

- Overwrite old State Items with a summary: destroys authoritative evidence and
  makes recovery provider-dependent.
- Persist a new schema-1 Item variant: silently changes an already declared
  durable coordinate without the required migration runner.
- Treat the summary as an Assistant message: erases its derived trust class and
  allows it to masquerade as prior model output.
- Summarize every omitted Turn without an input bound: makes long-Thread
  compilation unbounded.
- Continue raw truncation only: safe, but leaves no replaceable semantic
  compaction capability for long-running agents.
