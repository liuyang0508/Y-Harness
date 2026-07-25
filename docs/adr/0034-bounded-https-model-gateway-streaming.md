# ADR 0034: Bounded HTTPS model-gateway streaming

- Status: Accepted
- Date: 2026-07-25

## Context

The kernel already exposes failure-isolated provisional model events, but the
built-in HTTPS adapter could return only a fully buffered JSON response.
Without a specified network stream, each gateway would invent framing,
completion, and resource semantics. Transport chunks also do not preserve JSON
or UTF-8 boundaries, so reparsing an ever-growing buffer would create avoidable
quadratic work.

## Decision

- Preserve the ordinary JSON request/response path when the caller supplies no
  provisional-event sink.
- When a sink exists, send `x-y-harness-model-stream: 1`, request
  `application/x-ndjson`, and retain the exact model-gateway API coordinate
  header `"1"`.
- Accept exactly two closed frame shapes:

  ```json
  {"type":"text_delta","delta":"..."}
  {"type":"response","response":{"output":{"type":"message","content":"..."}}}
  ```

  Unknown fields and variants fail closed.
- Decode incrementally across arbitrary HTTP chunks, including split UTF-8 and
  CRLF line endings. Scan each received byte at most once instead of reparsing
  the accumulated body.
- Bound the entire wire body by the configured response limit (default 2 MiB,
  maximum 16 MiB), frames to 4,096, each text delta to 4 KiB, and the
  kernel-accepted Turn delta total to 1 MiB.
- Reject blank frames, invalid JSON, a missing final response, multiple final
  responses, and every frame after the final response.
- Treat deltas as provisional application content. Sink rejection, panic, or
  capacity pressure drops an event and increments the kernel counter without
  failing inference. The mandatory final `ModelResponse`, subsequent
  Verification, and durable State remain authoritative.
- Keep TLS, authentication, redirect/proxy/retry, concurrency, timeout,
  response-size, and error-sanitization policy identical to the ordinary HTTPS
  gateway path.
- Give custom transports a default non-streaming method so existing trusted
  host implementations remain source-compatible. A transport overrides it
  only when it can preserve these invariants.

## Consequences

Embedded and protocol clients now receive real gateway deltas through the same
kernel stream contract; the Agent Loop still has one final-result path.
Streaming is opt-in per Turn through sink presence, so an existing API-1
gateway sees no changed request by default.

The decoder retains at most the configured total response bound plus the final
normalized JSON body. It does not provide backpressure to the remote producer:
the kernel event sink is deliberately synchronous and non-blocking, and
overflowed provisional content is dropped rather than delaying authoritative
completion.

This is a provider-neutral Y-Harness gateway protocol, not SSE compatibility
with vendor APIs. Direct vendor adapters, resume cursors, automatic replay,
multi-choice streams, binary framing, and a live configured gateway pass remain
separate work.

## Rejected alternatives

- Make streaming the default: breaks existing gateways and needlessly changes
  requests whose callers cannot consume deltas.
- Use SSE for the internal gateway: adds a vendor-shaped parser without
  improving the typed final-response contract.
- Treat concatenated deltas as the final answer: bypasses typed provider
  metadata, Tool calls, response validation, Verification, and State authority.
- Retry a broken stream: delivery may already have produced provider-side work,
  and provisional content cannot prove safe idempotency.
- Buffer the full body before parsing: delays useful deltas and weakens memory
  and complexity guarantees.
