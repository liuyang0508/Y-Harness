# ADR 0052: Preflighted, bounded Task claims

- Status: Accepted
- Date: 2026-07-25

## Context

`TaskGraph::claim_ready` accepted any positive batch size. A caller could clone
a large number of rich Task definitions into one claim result. The method also
released expired leases and propagated blocked dependencies before incrementing
attempt counters. If a claimable Task had exhausted its `u32` attempt counter,
the call returned an error after those maintenance mutations, so failure was
not state-preserving.

## Decision

- Bound one claim batch to 1 through 64 Tasks.
- Validate the owner, duration, batch size, deadline arithmetic, and attempt
  capacity before releasing leases or propagating dependency state.
- Treat an expired running Task as claimable during preflight when all of its
  dependencies are complete.
- Reject an exhausted claimable attempt counter before any graph mutation.
- Keep checked attempt increment in the mutation loop as defense in depth.

## Consequences

Invalid configuration and exhausted-attempt failures leave the Task Graph
byte-for-byte unchanged, including expired leases that the failed call was not
authorized to maintain. A scheduler can request another bounded batch after
persisting the previous one.

Sixty-four is a concurrency work window rather than a graph-size limit. The
graph may still contain up to its independent Task ceiling; callers iterate
claim batches.

## Rejected alternatives

- Allow arbitrary maximum values: graph size bounds do not bound result cloning
  or downstream worker fan-out.
- Roll back individual fields after an error: restoration logic is harder to
  keep complete as Task state evolves.
- Saturate the attempt counter: two distinct attempts would share one fencing
  generation and invalidate stale-worker reasoning.
