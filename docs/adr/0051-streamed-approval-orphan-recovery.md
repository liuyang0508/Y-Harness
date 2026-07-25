# ADR 0051: Streamed approval orphan recovery

- Status: Accepted
- Date: 2026-07-25

## Context

A Turn may create up to 256 approval records across the bounded Agent Loop.
SQLite orphan recovery previously selected and decoded every complete pending
record into a vector before applying updates. At the pending record ceiling,
one recovery transaction could therefore retain roughly 127 MiB of record
bodies even though it processes each record independently.

Lowering the per-Turn count would unnecessarily reduce Agent Loop capability.
The aggregation is an implementation artifact, not a domain requirement.

## Decision

- Inside one immediate SQLite transaction, first select only the ordered
  approval identities for the abandoned Thread and Turn.
- Bound every selected identity before Rust text allocation and retain at most
  the existing 256-per-Turn identity limit.
- After closing that query statement, load, validate, transform, encode, and
  update one complete record at a time.
- Keep the entire sequence in one transaction so any error rolls back all
  durable orphan updates.
- Preserve the existing failure-atomic candidate transition and index/body
  validation for every record.

## Consequences

SQLite orphan recovery retains at most one complete Approval record plus a
small bounded identity vector, instead of all possible record bodies. Durable
orphaning remains atomic across the Turn and preserves the 256-step Agent Loop
ceiling.

The operation still performs one bounded record query per approval. Recovery is
not the steady-state hot path, and predictable memory plus transactional truth
takes priority over a bulk projection.

## Rejected alternatives

- Reduce approvals per Turn: this couples persistence allocation to Agent Loop
  expressiveness.
- Keep the bulk record projection: count-bounded does not mean byte-safe when
  individual records are large.
- Commit one orphan at a time: a mid-run failure would expose a partially
  orphaned Turn to other processes.
