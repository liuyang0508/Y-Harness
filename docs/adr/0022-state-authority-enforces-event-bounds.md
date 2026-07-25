# ADR 0022: State authority enforces event boundaries

- Status: Accepted
- Date: 2026-07-25

## Context

Runtime and protocol limits are not sufficient protection for an embeddable
engine. Direct State callers and replacement Event Stores can bypass those
layers. An Event Store is a persistence extension point, not a trusted source
of kernel truth.

The first event-size proposal was 2 MiB. That is too small for a valid 1 MiB
Runtime text field because JSON control-character escaping can expand one input
byte to six encoded bytes.

## Decision

- State validates every pending mutation before calling an Event Store.
- State revalidates every Event Store append result and read result before
  caching, projection, or returning it to a caller.
- Event, Thread, Turn, Item, and Checkpoint identities are 1–256 non-control
  bytes at this boundary.
- One encoded State event is at most 8 MiB. This covers the worst-case JSON
  expansion of the Runtime's 1 MiB text ceiling while retaining a hard
  allocation limit.
- Event pages contain 1–10,000 requested records. A store response must not
  exceed the request, cross Thread boundaries, duplicate event identities, or
  return non-increasing sequences.
- Checkpoint labels are optional; present labels are 1–4,096 bytes.
- SQLite rechecks persisted event size, schema, identities, and typed event
  metadata while decoding. Corrupt data fails closed.
- A rejected mutation does not advance the built-in store or State head cache.

## Consequences

Built-in stores and third-party implementations now meet the same kernel-owned
boundary. Validation adds serialization work, so the State performance
benchmark is rerun whenever these checks change.

This decision bounds individual mutations. ADR 0023 subsequently adds
journal-anchored snapshot acceleration and a finite Thread event boundary;
archival and retention policy remain separate.

## Rejected alternatives

- Trust Event Store implementations: extensions would be able to inject
  malformed or cross-Thread state.
- Keep the 2 MiB envelope: valid bounded Runtime text could fail only after a
  Turn had started.
- Truncate oversized events: truncation changes durable meaning and can split
  correlated model, Tool, Policy, or Verification evidence.
- Treat checkpoints as snapshots: current checkpoints are durable markers and
  contain no authoritative projection payload.
