# ADR 0023: Journal-anchored disposable State snapshots

- Status: Accepted
- Date: 2026-07-25

## Context

The event journal is the authoritative source of runtime truth, but replaying
every retained event makes recovery latency linear in Thread age. Existing
Checkpoints are business-visible markers; changing them into projection blobs
would overload their semantics.

A snapshot must accelerate projection without becoming a second source of
truth or hiding a corrupt journal tail.

## Decision

- Add an optional snapshot capability to the Event Store contract. Existing
  stores remain compatible through default no-snapshot methods.
- Keep snapshot creation explicit through `StateEngine::create_snapshot`.
  Optional automatic maintenance is layered on this primitive by ADR 0030;
  Checkpoints remain unchanged.
- A snapshot contains a projected Thread, snapshot schema version, included
  stream version, last included global sequence, anchor event identity,
  creation time, and SHA-256 of the serialized projection.
- Snapshot fields are private and values are normally constructed by State
  Engine; every deserialized value is still revalidated.
- Loading validates the 64 MiB snapshot envelope, digest, identities, projected
  event count, Turn/Item/Checkpoint uniqueness, lifecycle invariants, and the
  same per-event encodability rules as the journal.
- State rereads the anchor event from the authoritative journal. Only after the
  sequence and event identity match does it replay newer events in bounded
  1,000-event pages.
- A missing, malformed, stale, or unanchored snapshot is discarded and full
  journal replay is used. A malformed authoritative tail fails closed.
- Memory and SQLite stores retain only the newest observed stream snapshot.
  SQLite persists snapshots transactionally in a separate cache table.
- A Thread accepts at most 1,000,000 events. The same ceiling bounds full replay
  and snapshot-plus-tail recovery so a cached writer cannot create a stream
  that the recovery path refuses to load. ADR 0031 reserves the last slot for
  terminal Turn settlement. ADR 0046 subsequently adds an aggregate recovery-
  byte boundary and advances the disposable snapshot body to schema `2`.

## Consequences

The journal remains sufficient for reconstruction and can outlive every
snapshot. Snapshot creation has linear cost, but later recovery parses one
projection plus the journal tail rather than every historical event. Operators
must choose when to materialize snapshots until scheduling policy exists.

SHA-256 detects accidental or partial snapshot-body corruption; it is not a
signature against a storage administrator who can rewrite both body and
digest. The anchor binds the cache to an existing event identity, while normal
Event Store and projection validation continue to fail closed.

The one-million-event ceiling is a deliberate availability boundary, not
archival. Automatic scheduling is defined separately by ADR 0030. Retention
warnings, archival, legal-hold policy, and blob offloading remain separate
work.

## Rejected alternatives

- Make snapshots authoritative: journal and snapshot failures could disagree.
- Reuse Checkpoints: marker and materialized-projection lifecycles differ.
- Trust snapshot JSON after deserialization: silent corruption could seed the
  State head cache.
- Verify every snapshot by replaying its full prefix: correct but removes the
  recovery acceleration.
- Allow unlimited events behind a snapshot: a missing cache would make the
  authoritative fallback unbounded.
