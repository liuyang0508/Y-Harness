# ADR 0044: Frozen, panic-isolated model identity

- Status: Accepted
- Date: 2026-07-25

## Context

`ModelRegistry` validates provider identity during registration, but the
compatibility `HarnessRuntime::new` constructor accepts a model directly.
Runtime execution previously called `LanguageModel::id()` before a Turn and
again while recording assistant/tool-call State and Observability. A
synchronous provider panic could escape embedded execution, and a mutable or
nondeterministic implementation could give one Turn inconsistent provenance.

## Decision

- Capture model identity once when constructing a Runtime.
- For registry construction, reuse the identity already validated and retained
  by `RegisteredModel`; never call provider metadata again.
- For the compatibility constructor, invoke `id()` inside `catch_unwind`,
  validate the result against the portable model identity contract, and retain
  only `Valid`, `Invalid`, or `Panicked` state.
- Reject invalid identity with a content-free `InvalidCapability` and a panic
  with `CapabilityPanicked { phase: Model }`.
- Perform this check before Runtime admission, Thread loading, `TurnStarted`, or
  any other Turn mutation.
- Use the frozen identity for every model observation and model-produced State
  item in the Runtime lifetime.

## Consequences

One Runtime has one stable model provenance identity. Embedded and protocol
execution receive the same typed failure without a provider payload, and an
identity failure creates no Turn. Registry-selected extensions do not get a
second metadata callback after admission.

As with every caught Rust panic, the process-global panic hook runs first.
Production hosts remain responsible for their hook and stderr policy.

## Rejected alternatives

- Re-query identity at each use: provenance can diverge and synchronous
  extension code remains on sensitive State/Trace paths.
- Make the existing constructor fallible immediately: that is a larger public
  API break when deferred typed rejection preserves safety.
- Substitute a generated identity after failure: invented provenance is worse
  than failing closed.
