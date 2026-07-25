# ADR 0050: Bounded approval working window

- Status: Accepted
- Date: 2026-07-25

## Context

Approval records are individually bounded, but the pending API previously
accepted 1,000 records. At the pending lifecycle ceiling, one call could
materialize roughly 496 MiB before the protocol response allocator enforced its
own limit. The in-memory implementation was worse: it cloned every pending
record, sorted the full clone set, and only then truncated it to the requested
count.

Approval review is a working-window operation. It does not need an enormous
materialized snapshot, and returning a transport error after allocating one is
not a safe bound.

## Decision

- Limit one pending Approval page to 16 records.
- Keep maximum encoded pending record bodies below 8 MiB in aggregate; the
  protocol retains additional envelope headroom under its 16 MiB response
  limit.
- In the in-memory inbox, scan the map while retaining only the oldest
  requested window as references, ordered by `(requested_at_ms, approval_id)`.
  Clone only that final bounded window.
- Keep SQLite ordering and `LIMIT` at the query boundary.
- Reject a requested page of 17 or more rather than silently truncating the
  caller's contract.

## Consequences

Both inbox implementations materialize at most 16 Approval records per pending
call, with deterministic oldest-first parity. Hosts process or settle that
window and poll again to reveal later requests.

Scanning the in-memory map remains linear in record count because it is a
reference implementation without a second time index, but its additional
selection memory is bounded to 16 references and its record cloning is bounded
below 8 MiB. A future high-volume in-memory implementation may add an ordered
index behind the same trait.

## Rejected alternatives

- Rely on protocol serialization limits: embedded callers would remain
  exposed, and allocation would already have happened.
- Keep 1,000 and add an aggregate byte cutoff: without a cursor, an early
  oversized record could make page progress ambiguous.
- Clone all records before truncation: the returned count would be bounded but
  working memory would not.
