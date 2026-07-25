# ADR 0059: Registered Token Counters with independent byte bounds

- Status: Accepted
- Date: 2026-07-25

## Context

Conversation selection charged serialized JSON bytes under a field named
`budget_tokens`. This was deterministic and safe but not a provider tokenizer.
Memory packs carried provider-reported token estimates, so a buggy or malicious
provider could distort which packs fit even though hard byte limits still
prevented unbounded allocation.

A provider tokenizer is extension code: its metadata may panic or drift, its
call may fail, and its output may be zero or pathological. Letting its estimate
replace transport-independent byte limits would weaken the existing trust
boundary.

## Decision

- Add version-1 `TokenCounter`, `TokenCounterDescriptor`, and
  `TokenCounterRegistry` public contracts.
- Register counters by portable exact identity with collision rejection,
  trust-bearing `CapabilityOrigin`, the shared 4,096-entry ceiling, frozen
  metadata, and exact API-version validation.
- Select one counter explicitly in `ContextEngine`; no implicit model or
  tokenizer substitution is allowed.
- Recount bounded serialized conversation Items, Memory pack text, and Skill
  block text when a counter is selected. Without one, preserve existing
  serialized-byte/provider-estimate behavior.
- Fail Context compilation when recounted Skill instructions exceed the
  manifest token budget already admitted by `SkillEngine`.
- Panic-isolate counter calls in the Context phase, sanitize returned errors,
  and reject counts outside 1–16,777,216.
- Split conversation configuration into independent token and serialized-byte
  budgets. A Turn is included only when both remain within bounds.
- Keep the complete Context and Model-request byte ceilings unchanged.
- Advertise `token_counter_api = 1` in `CompatibilityManifest` and advance the
  exact client protocol from 3 to 4.
- Keep State event schema 1 unchanged. Its legacy-named
  `ConversationContext.estimated_tokens` field continues to store the
  conservative serialized-byte charge; selected Turn IDs remain the
  authoritative selection evidence.

## Consequences

Hosts can install the tokenizer that matches an exact model/provider without
adding tokenizer dependencies to the headless core. Memory providers no longer
control final token accounting when a counter is selected. Counter failure
cannot silently degrade as a Memory failure and cannot leak its error or panic
payload.

Token counting remains an estimate for segmented inputs; the provider owns the
final request acceptance. Independent byte ceilings continue to bound memory,
serialization, and transport even if a counter under-reports.

The Rust `ConversationContextConfig` gains `budget_bytes` before the first
public release. Protocol v3 clients must fail exact negotiation with v4.

## Rejected alternatives

- Add one vendor tokenizer dependency to Core: it couples the microkernel to one
  provider and still leaves other models unsupported.
- Trust `MemoryContextPack.packed_tokens`: provider output is not the Context
  Engine's budgeting authority.
- Replace byte limits with tokenizer counts: count errors would become memory
  and transport authority.
- Change schema-1 `estimated_tokens` to provider-token semantics: that silently
  changes durable evidence meaning without a migration.
- Infer a counter from model names: model and tokenizer selection must remain
  exact operator configuration.
