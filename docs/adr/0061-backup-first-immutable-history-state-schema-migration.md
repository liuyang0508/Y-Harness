# ADR 0061: Backup-first immutable-history State schema migration

- Status: Accepted
- Date: 2026-07-25

## Context

State schema 1 cannot durably represent content-free semantic-summary evidence.
Adding that Item while continuing to label new events as schema 1 would make an
old reader accept a coordinate whose JSON shape it cannot decode. Rewriting
every historical event to a new label would also destroy the useful fact that
those bytes were written under the old contract and would turn a small schema
advance into an unnecessary bulk mutation.

The first authoritative schema change must be bounded, backup-first,
restartable, fail closed on unknown versions, and have an explicit downgrade
boundary. A snapshot is a disposable acceleration cache and cannot satisfy
those requirements.

## Decision

- Advance the State event writer from schema 1 to schema 2 and the disposable
  snapshot cache from schema 2 to schema 3. Advance the exact client protocol
  from 5 to 6 because `GetEvents` can expose the new Item shape.
- Schema 2 adds `ConversationSummary`, a content-free evidence Item containing
  the selected compactor, exact covered Turn IDs, uncovered older-Turn count,
  source/content SHA-256 fingerprints, token charge, and serialized-byte
  charge. The generated summary body remains ephemeral Context.
- Make the schema-2 reader accept immutable historical schema-1 events. Reject
  a `ConversationSummary` mislabeled as schema 1, unknown future versions, and
  invalid evidence bounds.
- Add a small SQLite metadata table that declares the active event writer and
  snapshot coordinates. A populated legacy database without that table cannot
  open normally; it requires explicit `yh state-migrate <database> <backup>`.
  A fresh or completely empty store can bootstrap current metadata.
- Require every writer to be stopped before migration. Mixed-version and
  rolling writers against one database remain unsupported.
- Preflight source/backup paths, exact legacy event versions, compact live-page
  backup size, exact stream-version/recovery-charge accounting, at least 1 MiB
  of backup working reserve, and at least 1 MiB of source-filesystem working
  space before mutation.
- Create the backup with SQLite `VACUUM INTO`, switch it to a fully settled
  rollback-journal mode, add a manifest binding the source event count and
  maximum sequence plus a streaming SHA-256 of every authoritative event row,
  sync the file, and publish it with a same-directory no-clobber hard link.
  Sync parent-directory entries on Unix. Never replace an existing backup path.
- Validate an existing backup with `integrity_check`, its manifest, and the
  source fingerprint so an interrupted migration can reuse it without another
  full copy.
- In one `BEGIN IMMEDIATE` transaction, recheck the full source fingerprint and
  legacy version set, then add current metadata. Do not rewrite event JSON,
  event schema labels, sequences, or snapshots.
- Treat the durable backup as the downgrade boundary. Before any schema-2 event
  is written, an operator may stop all writers and restore the backup. After a
  schema-2 event exists, downgrade is unsupported because an old reader cannot
  represent all committed history.
- Keep restore operator-controlled. Automatically replacing a database is a
  destructive action outside the migration command's authority.

## Consequences

Migration work is proportional to the compact SQLite backup and bounded
streaming event fingerprints; the source mutation itself remains constant-size.
Historical schema provenance remains honest and the current reader supports one
explicit predecessor coordinate. The backup contains one Y-Harness manifest
table in addition to the legacy tables; legacy Y-Harness code ignores that
extra table.

Process interruption after preflight, after backup publication, or before
metadata commit leaves either an untouched legacy source or a reusable verified
backup. A process or machine interruption can leave a uniquely named
`.partial-*` file; it is not authoritative and is cleaned only after the
operator verifies the final backup and source state.

This does not provide online migration, mixed-version operation, multi-node
coordination, automatic restore, archival, or downgrade after new writes.
Filesystem and SQLite durability still depend on the underlying storage honoring
their sync contracts.

## Rejected alternatives

- Relabel or re-encode every schema-1 event as schema 2: mutates authoritative
  history without changing its meaning and creates a larger failure surface.
- Let normal open migrate implicitly: removes the operator's backup-path and
  writer-quiescence decisions.
- Overwrite a conventional `.bak` path: can destroy the only valid rollback
  artifact on a retry or typo.
- Persist the generated summary body: expands sensitive durable content and
  still would not make semantic output authoritative.
- Support live old and new writers: the old writer cannot honor the metadata
  coordinate or schema-2 Item contract.
