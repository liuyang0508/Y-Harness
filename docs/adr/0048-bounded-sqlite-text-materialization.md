# ADR 0048: Bound SQLite text before materialization

- Status: Accepted
- Date: 2026-07-25

## Context

State events and snapshots had explicit durable byte limits and recently
checked SQLite byte lengths before converting `TEXT` into Rust `String`.
Approval Inbox records and Task Coordinator graph snapshots also validated
decoded JSON size, but their SQLite reads materialized the complete stored text
first. A corrupted or externally modified database could therefore force an
allocation beyond the subsystem limit before receiving the intended error.

The same trust-boundary rule applies to indexed approval status and identity
columns. Index/body consistency checks do not replace allocation-time bounds.

## Decision

- Keep one crate-private SQLite text guard shared by State, Approval, and
  Orchestration instead of three subtly different implementations.
- Every guarded query selects `length(CAST(column AS BLOB))` before the text.
  The BLOB cast measures encoded UTF-8 bytes rather than Unicode characters.
- Reject a negative, unrepresentable, or over-limit length before calling
  `Row::get::<String>` for that column.
- Apply the guard to State event identities, events, and snapshots; Approval
  status, Thread identity, Turn identity, and record body; and Task Graph
  snapshot bodies.
- Preserve all post-materialization schema, JSON, identity, index/body,
  digest, ordering, and domain-invariant validation. The allocation guard is
  an additional boundary, not a substitute.

## Consequences

Built-in durable payloads cannot cross their declared Rust `String` allocation
ceiling merely because their SQLite database was damaged or modified by
another process. Error conversion remains content-free and does not echo the
stored value.

This does not impose a database-file quota, authenticate database pages, or
make SQLite itself a hostile multi-tenant service. Operators still own file
permissions, storage quotas, integrity monitoring, backup, and recovery.

## Rejected alternatives

- Check only after `String` allocation: it bounds later decoding, not the
  allocation attack surface.
- Depend only on table `CHECK` constraints: existing development schemas and
  out-of-band database modification can bypass write-path validation.
- Duplicate one helper per subsystem: repeated boundary code is easier to
  diverge and harder to audit.
