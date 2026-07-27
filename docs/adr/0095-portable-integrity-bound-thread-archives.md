# ADR 0095: Portable integrity-bound Thread archives

## Status

Accepted.

## Context

An embedded host or operator must be able to move terminal Thread history
between State stores without replaying Tools, copying a SQLite database, or
making a client authoritative. Pi demonstrates useful session JSONL
import/export ergonomics, but copying a file into a session directory does not
provide Y-Harness's atomic State projection, bounded input, or durable source
evidence.

## Decision

- Add a bounded format-1 `ThreadArchive` containing one complete validated
  source `StoredEvent` journal, its source identity/version/last sequence, and
  SHA-256 over the exact ordered events.
- Export only terminal histories. A running Turn has an active owner and is not
  a portable completion boundary.
- Add schema-10 `ThreadImported` immediately after the target
  `ThreadCreated`. It records the source boundary, digest, and optional source
  fork lineage as evidence.
- Import under a caller-chosen target Thread identity. Memory and SQLite stores
  create the complete target stream atomically or leave it absent.
- Generate new globally unique Event identities while preserving historical
  Turn, Item, Tool-batch, Approval, Steering, and Provider correlation
  identities. Copy Thread-name transitions; omit source creation/provenance
  transitions and recovery-only Checkpoints.
- Source fork lineage remains import evidence. It is not exposed as local
  `Thread.lineage`, because the source parent may not exist in the target
  store.
- Reusing the target identity is idempotent only when its immutable import
  origin and inherited Turn prefix match.
- Keep archive files at the CLI/host adapter boundary. The State Engine owns
  archive semantics; the current service protocol does not stream files.

## Consequences

Imports never re-execute effects and cannot leave partial target histories.
Archive tampering, unknown root fields, invalid journals, unsupported formats,
and oversized inputs fail before State mutation. Imports have fresh target
creation time and global sequences, while provenance binds the exact source
journal. Protocol 17 advances because full Thread and State-event projections
can now contain schema-10 import provenance.
