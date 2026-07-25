# ADR 0042: Protocol v2 bounded, retrievable responses

- Status: Accepted
- Date: 2026-07-25
- Superseded in part by: ADR 0046 (protocol-v3 capacity shape)

## Context

Protocol v1 limited both request and response JSONL frames to one MiB. The
Runtime legitimately accepts a one-MiB model result, while State authority
accepts an encoded event up to eight MiB. JSON envelopes add bytes around those
payloads. A successful Turn could therefore be durable yet impossible to read
through either `operation.get` or `thread.events`.

The response writer also serialized an entire value into a `Vec` before
checking its length. Large Thread or event-page responses could allocate far
beyond the advertised frame ceiling and only then become
`response_too_large`. Count-only event pagination had the same aggregate-memory
problem.

## Decision

- Advance the exact client protocol coordinate from `1` to `2`.
- Limit request frames to 2 MiB. This admits the independently bounded one-MiB
  prompt plus its typed envelope without accepting arbitrarily large input.
- Limit response frames to 16 MiB. This admits every valid single eight-MiB
  State event and the Runtime's maximum model result with envelope headroom.
- Serialize through a writer that refuses growth beyond the response ceiling.
  On overflow, return the existing small correlated `response_too_large`
  result without first allocating the complete response.
- Limit requested State event pages to 32 and fetch at most two authoritative
  events at a time. Stop a page at a 16-MiB-minus-64-KiB content budget and
  preserve `has_more` plus the last included sequence.
- Reduce pending-approval pages to a default of 8 and maximum of 16 because an
  approval can contain a large governed Tool input.

## Consequences

All content accepted at the Runtime model boundary and every valid individual
State event can cross the reference protocol. Large histories remain cursor
paged. Response serialization uses at most the configured frame buffer instead
of duplicating an arbitrarily large result.

Clients must negotiate protocol `2`; protocol `1` requests fail closed. This is
an intentional pre-release compatibility break under the declared policy.
Increasing frame ceilings does not increase prompt, model, Tool, State-event,
or approval payload authority; those independent validation limits remain.

`thread.get` can still require a large in-memory State projection before the
bounded writer rejects its serialized response. Archival and scalable
projection remain separate State Engine work and are not claimed here.

## Rejected alternatives

- Keep v1 and reduce Runtime/State payload limits below one MiB: embedded users
  would lose valid capability solely for one transport.
- Raise the response ceiling without bounded serialization: the advertised
  wire bound would still permit transient allocation exhaustion.
- Return partial JSON: it corrupts framing and makes client state ambiguous.
- Split one State event across pages: that creates a second chunking protocol
  and weakens the event envelope's atomic meaning.
