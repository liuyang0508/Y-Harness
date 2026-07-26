# SQLite State migration runbook

This runbook applies to schema-1, schema-2, schema-3, schema-4, or schema-5
State stores moving to schema 6.
It is deliberately offline and backup-first.

## Preconditions

1. Stop every process that can read-write the database, including older
   Y-Harness binaries. Confirm the previous workers have exited.
2. Choose an existing backup directory on a durable filesystem. The final
   backup path must not already exist and must differ from the source path.
3. Ensure the backup filesystem can hold SQLite's compact live pages plus
   1 MiB, and the source filesystem has at least 1 MiB of working space. The
   command performs both checks again.
4. Preserve database ownership, permissions, encryption-at-rest controls, and
   any storage-level snapshot policy outside Y-Harness.

Run:

```bash
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v6.rollback.db
```

Success reports the source and destination event coordinates, immutable
historical event count, preflight space values, and the rollback backup path.
Normal runtime open refuses a populated schema-1, schema-2, schema-3, schema-4,
or schema-5 database until this command succeeds.

## What the command changes

The command creates a compact SQLite backup, records a migration manifest in
that backup, binds it to the complete authoritative event history with a
streaming SHA-256, validates it, and then adds schema-6 writer metadata for a
schema-1 source or advances schema-2/schema-3/schema-4/schema-5 event and
snapshot metadata inside one immediate transaction. It does not change
historical event JSON, schema labels, sequence numbers, streams, or snapshots.
New events are written as schema 6; schema-6 readers continue to validate
historical schema-1, schema-2, schema-3, schema-4, and schema-5 events.
Existing snapshots remain untouched and disposable; new snapshots use schema
6.

An existing backup path is never overwritten. A valid backup from an
interrupted attempt is reused; an unrelated, corrupt, or mismatched file fails
closed. The caller must use a new backup path after legitimate source history
changes.

## Interruption and retry

Rerun the same command with the same source and backup after a process
interruption:

- interruption before backup publication leaves the source unchanged;
- interruption after publication reuses the validated backup;
- interruption before source commit rolls back the metadata transaction; and
- rerunning after success reports `AlreadyCurrent` and performs no new write.

A hard interruption can leave a file named like
`state-pre-v6.rollback.db.partial-<id>`. It is not the final backup. Remove orphan
partials only after verifying that no migration process is active and that
either the final backup is valid or the source is still at its untouched
schema-1, schema-2, schema-3, schema-4, or schema-5 coordinate.

## Restore and downgrade boundary

Restoration is intentionally not automated because it replaces authoritative
State.

Before restoring:

1. Stop all Y-Harness writers.
2. Verify the rollback file with SQLite `PRAGMA integrity_check(1)` and inspect
   the single row in `y_harness_migration_backup`.
3. Preserve the current source separately for diagnosis.
4. Use operator-approved filesystem or storage tooling to replace the source
   atomically with the verified backup, retaining required ownership and
   permissions.
5. Start only the reader/writer version appropriate for the restored schema.

Rollback is supported only before any schema-6 event has been committed. Once a
schema-6 event exists, restoring the pre-v6 backup discards newer
authoritative history and is therefore not a supported downgrade.

## Mixed-version rule

There is no rolling-upgrade window. A new reader can read schema-1, schema-2,
schema-3, schema-4, and schema-5 history only after explicit migration. An old
reader/writer is unsupported against the migrated source and must fail on
schema-6 metadata or Items. Never run old and new writers concurrently against
one database.
