# ADR 0070: Explicit bounded Model failover

- Status: Accepted
- Date: 2026-07-25

## Context

The Runtime selected exactly one registered Model. That preserved provenance
but left an already configured secondary provider unused after an ordinary
primary failure. Hiding fallback inside `ModelRegistry` or a wrapper
`LanguageModel` would be incorrect: State would identify the wrapper rather
than the implementation that produced an assistant message or Tool call.

Streaming makes fallback more dangerous. If one Model delivers provisional
text and a second Model then starts at the same step, clients can observe a
single stream assembled from two independent decisions.

## Decision

- Keep `ModelRegistry` exact, collision-safe, and free of implicit selection.
- Add an explicit Runtime constructor for an ordered route of 1–16 exact
  registered Model identities.
- Try the first Model on every Agent Loop step. Continue to the next only
  after an ordinary failure before any provisional event was successfully
  delivered to the caller.
- Give every attempt in a multi-model route a 30-second default deadline,
  configurable from 1 millisecond to 24 hours. The total Turn deadline wins
  when it expires first.
- Treat an attempt-local timeout like an ordinary pre-output failure. Cancel
  the attempt's cooperative signal before releasing the provider Future so
  external work can stop deterministically.
- Never cross Turn cancellation or the Turn deadline.
- Give each attempt a separately closable streaming handle. Late events from a
  settled failed attempt cannot enter a later attempt.
- Synchronize attempt closure with any in-flight synchronous sink delivery
  before deciding whether fallback is still safe.
- Record one content-free Observability result per attempted Model.
- Persist the exact successful Model identity and `CapabilityOrigin` on the
  resulting assistant message or Tool call.
- When resuming a durable pre-Tool approval boundary, require the exact
  recorded Model identity and origin to remain present anywhere in the
  configured route.

## Consequences

An operator can opt into bounded provider availability without weakening
State provenance or merging incompatible streams. A single-model
configuration keeps its previous behavior, and no provider switch happens
after a valid Model response has entered the Agent Loop.

Requests are cloned only for routes that may need a later attempt. A failed
attempt can still incur provider cost. Model adapters remain responsible for
tighter connect/request timeouts and cleanup after cooperative cancellation;
the Runtime owns the route-attempt deadline and total Turn deadline. The
closure gate relies on the existing `ModelEventSink` contract that sink
callbacks are synchronous and non-blocking; it does not make blocking host
callbacks safe.
This baseline did not add load balancing, circuit breaking, health scoring, or
hedged requests. ADR 0099 later adds only an opt-in process-local cooldown for
Runtime-proven attempt timeouts; it does not infer health from ordinary
Provider errors. ADR 0101 separately adds default-disabled bounded retries for
four typed transient Provider failure classes; retries share this decision's
candidate deadline and provisional-output fence.

The existing State fields already carry the actual Model identity and origin,
and the wire event shape is unchanged. State schema 4 and Protocol 9 therefore
remain current.

## Rejected alternatives

- Put fallback inside `ModelRegistry`: registration and execution policy would
  become coupled, and selection would be implicit.
- Register a failover wrapper as one Model: durable provenance would identify
  the wrapper rather than the settled provider.
- Continue after delivered provisional text: clients could not distinguish or
  retract the failed Model's fragment.
- Run providers concurrently: hedging multiplies cost and needs an explicit
  winner, cancellation, and streaming arbitration contract.

## Related decisions

- [ADR 0099: observable Model attempt-timeout cooldown](0099-observable-model-attempt-timeout-cooldown.md)
- [ADR 0100: typed Model Provider failure evidence](0100-typed-model-provider-failure-evidence.md)
- [ADR 0101: bounded typed Model retry policy](0101-bounded-typed-model-retry-policy.md)
