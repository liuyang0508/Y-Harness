# ADR 0077: Origin-bound durable Provider Continuation

- Status: Accepted
- Date: 2026-07-25

## Context

The direct OpenAI Responses adapter uses `store: false`, leaving Y-Harness
responsible for conversation history. Reasoning models may return an encrypted
`reasoning` output item before a function call. OpenAI's stateless continuation
contract requires that item to be replayed with the function call and Tool
result. Dropping it can make the next model step semantically incomplete;
letting a different failover model consume it gives provider-private state to
the wrong authority.

The previous safe behavior rejected such responses before Tool execution.
That prevented silent corruption but also prevented a real reasoning-model
Tool loop.

## Decision

- Add a provider-neutral `ModelContinuation` to `ModelResponse`. It contains
  1–64 ordered JSON items, a 1–64 byte portable format coordinate, and at most
  1 MiB of bounded serialized data.
- Treat continuation data as non-executable. The provider adapter owns
  format-specific validation and replay; Tool execution remains exclusively
  under Runtime Policy, Approval, and State.
- Record every returned capsule as a separate `provider_continuation` Item
  before the associated assistant decision. Runtime, not the provider, binds
  the Item to the model identity and trust-bearing registration origin that
  actually settled the request.
- Include continuation Items in model-visible history, but filter them before
  each model attempt so a model sees only capsules matching both its current
  identity and origin.
- When a Tool result belongs to a chain containing a continuation, route the
  next model step only to that exact model registration. Suppress ordinary
  failover because another provider cannot safely interpret the capsule.
- Request `reasoning.encrypted_content`, preserve returned OpenAI `reasoning`
  items with non-empty encrypted content, and replay them before the normalized
  `function_call`. Reject an unreplayable reasoning function call before any
  Tool side effect.
- Advance State events and disposable snapshots to schema 5, the exact client
  protocol to 11, and the HTTPS model-gateway API to 3. Historical State events
  remain immutable; populated schema-1/2/3/4 stores require the existing
  backup-first offline migration.

## Consequences

Reasoning-model function calls can now complete a real stateless OpenAI Tool
loop without moving scheduling or authority into the provider. SQLite reopen
tests retain the capsule across an interrupted Turn; Runtime tests prove
ordered persistence, exact replay, and provenance-tamper rejection.

Continuation-bound model failure now fails the Turn instead of crossing to a
fallback model. This is deliberate: availability cannot override semantic
integrity. A new user Turn is not permanently pinned; only an unfinished Tool
chain containing provider-private state is constrained.

The capsule can contain provider-sensitive opaque data. Product clients expose
only its model and format metadata, not its body. Retention, encryption at
rest, and tenant-specific data policy remain host responsibilities shared with
the rest of authoritative State.

This decision does not add provider-side Tool execution, hosted Tool trust,
parallel Tool calls, cross-provider continuation translation, or evidence of a
live vendor pass.

## Sources

- [OpenAI model guidance for manually managed history](https://developers.openai.com/api/docs/guides/latest-model#update-api-and-model-parameters)
- [OpenAI Responses API](https://api.openai.com/v1/responses)
