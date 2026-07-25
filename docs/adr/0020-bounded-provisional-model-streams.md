# ADR 0020: Bounded provisional model streams

- Status: Accepted
- Date: 2026-07-25

## Context

Interactive clients need model text before a Turn finishes. Treating each delta
as authoritative State would create excessive journal traffic and make partial
provider output look completed. An unbounded async channel would let a slow or
disconnected client exhaust Runtime memory.

## Decision

- Extend the existing `LanguageModel` contract with an optional streaming
  method whose default preserves non-streaming providers.
- Pass providers a kernel-owned `ModelStream` handle, not an arbitrary channel.
  It accepts at most 4 KiB per delta and 1 MiB per Turn.
- Correlate every event to a one-based model step. Providers cannot choose the
  step number.
- Keep the sink synchronous and non-blocking. Bounds, sink errors, and sink
  panics reject an event, increment a drop counter, and never fail inference.
- Close each step handle immediately when inference succeeds, fails, times out,
  or is cancelled. Cloned provider handles cannot emit after settlement.
- Treat all deltas as provisional application content. The returned
  `ModelResponse`, Verification, and State settlement remain authoritative.
- Retain operation events in a process-local ring bounded by both 4,096 events
  and 1 MiB. Cursor reads are capped at 32 events.
- Evict oldest deltas under pressure and return
  `dropped_through_sequence` so clients can detect an irreversible gap.
- Remove the stream with its terminal Operation when the client explicitly
  forgets that Operation.

## Consequences

Embedded clients and protocol clients share one streaming path without a
second Agent Loop. A slow client cannot backpressure inference or grow memory
without limit. Clients must be prepared to reconcile a provisional stream with
the authoritative final response and to display explicit gaps.

Streaming content is intentionally outside content-free Observability.
Operational telemetry receives only the count of rejected stream events.
Operation streams do not survive process restart; durable recovery uses State.

## Rejected alternatives

- Journal every token: conflates transient presentation with durable execution
  truth and amplifies writes.
- Unbounded channels: turn client slowness into Runtime memory exhaustion.
- Await the client from inference: makes UI availability part of model
  settlement.
- Silently discard old deltas: clients could render incomplete text as if it
  were complete.
- Treat concatenated deltas as final output: providers may revise, suppress, or
  diverge from their final response.
