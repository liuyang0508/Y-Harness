# ADR 0046: Store-authoritative bounded State recovery

- Status: Accepted
- Date: 2026-07-25
- Supersedes in part: ADR 0023, ADR 0031, and ADR 0042

## Context

State limited each encoded event to eight MiB and each Thread to one million
events. Those independent ceilings still allowed a valid stream whose full
projection required terabytes. The old full-read Event Store method could
materialize that stream before State checked its count, and snapshot-tail
paging accumulated pages before applying an aggregate byte limit.

An aggregate limit enforced only by State Engine would also be racy: two
writers can both observe the same remaining capacity. Finally, consuming the
last bytes with a non-terminal event could leave a running Turn unable to
persist `TurnFinished`.

## Decision

- Charge each event its exact encoded UTF-8 State-event bytes plus 512 bytes of
  conservative per-event recovery overhead.
- Limit one Thread to 64 MiB of recovery charge as well as one million events.
- Extend `PendingEvent` with the expected recovery charge. Event Stores must
  compare version and charge and update both atomically with the append.
- Maintain the charge directly in the in-memory store and in a dedicated
  SQLite `stream_recovery` row. Opening an unreleased earlier development
  database transactionally backfills missing rows from authoritative events.
- Capture SQLite's event rowid immediately after the event insert, before
  auxiliary metadata-table writes can change `last_insert_rowid()`.
- Remove the unbounded Event Store read method. Require stores to implement
  cursor paging with both count and byte budgets; State uses 16 MiB maximum
  pages and rejects aggregate growth before extending its result vector.
- In SQLite reads, inspect authoritative BLOB byte lengths before converting
  identity, event, or snapshot TEXT into Rust strings. Corrupt oversized values
  therefore fail before their body crosses the FFI allocation boundary.
- Reserve one event and 4 KiB exclusively for terminal settlement. Tests encode
  a maximum-length, maximum-escaping Turn identity and prove its terminal event
  fits that byte reserve.
- Advance disposable State snapshots from schema `1` to `2`, record exact
  prefix recovery charge, and reconstruct that charge from the projection
  during validation. Old snapshots are discarded and the journal is replayed.
- Report count and byte usage, general capacity, terminal reserves, and the
  worst pressure level through `StateCapacity`.
- Advance the exact client protocol from `2` to `3` because the existing
  Thread-capacity result gains required fields. Protocol-v2 frame and retrieval
  limits remain unchanged.

## Consequences

Built-in stores cannot accept a stream that built-in recovery later rejects
solely because its aggregate event bodies are too large. Full replay and
snapshot tails allocate in bounded pages, and competing writers cannot
oversubscribe the same byte capacity. A running Turn retains enough journal
authority to settle at either boundary.

The 512-byte charge is a conservative accounting unit, not a claim about exact
allocator or Rust object size. The limit is per Thread; it does not cap the
number of Threads, total SQLite disk use, or total host memory. Archival, blob
offload, global tenant quotas, and historical retention remain separate
operator-controlled work.

This intentionally changes the still-unpublished Rust `EventStore` extension
contract. Third-party stores must implement byte-bounded paging and atomic
recovery-charge comparison before registration in a release build.

## Rejected alternatives

- Check total bytes after `events()` returns: the dangerous allocation has
  already happened.
- Keep count-only pages: a page of maximum-size events can still allocate tens
  of gigabytes.
- Track bytes only in the Runtime head cache: independent processes can race,
  and a cache is not authoritative.
- Query `SUM(length(event_json))` on every SQLite append: correct but makes
  append cost linear in Thread age.
- Use a byte limit without a terminal reserve: ordinary work can strand a
  running Turn at the boundary.
