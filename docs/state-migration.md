# SQLite State migration runbook

This runbook applies to schema-1 through schema-12 State stores moving to
schema 13.
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
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v13.rollback.db
```

Success reports the source and destination event coordinates, immutable
historical event count, preflight space values, and the rollback backup path.
Normal runtime open refuses a populated schema-1 through schema-12 database
until this command succeeds.

## What the command changes

The command creates a compact SQLite backup, records a migration manifest in
that backup, binds it to the complete authoritative event history with a
streaming SHA-256, validates it, deletes disposable legacy snapshots, and
advances event and snapshot writer metadata inside one immediate transaction.
It does not change
historical event JSON, schema labels, sequence numbers, stream versions, or
recovery charges. For schema-1 through schema-7 sources, the transaction also
adds the nullable `streams.name` projection column. It adds nullable
`streams.tenant_id` to every schema-1 through schema-11 source. Existing
Threads remain explicitly unscoped; migration never infers ownership.
Schema-8 and newer names are validated against the journal before migration.
New events and snapshots use schema 13; schema-13 readers continue to validate
immutable schema-1 through schema-12 events. Schema-12 sources already contain
the tenant projection, so migration advances metadata and discards disposable
snapshots without altering authoritative history. Snapshots are rebuildable
caches, not authoritative history.

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
`state-pre-v13.rollback.db.partial-<id>`. It is not the final backup. Remove
orphan partials only after verifying that no migration process is active and
that either the final backup is valid or the source is still at its untouched
schema-1 through schema-12 coordinate.

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

Rollback is supported only before any schema-13 event has been committed. Once
a schema-13 event exists, restoring the pre-v13 backup discards newer
authoritative history and is therefore not a supported downgrade.

## Mixed-version rule

There is no rolling-upgrade window. A new reader can read schema-1 through
schema-12 history only after explicit migration. An old reader/writer is
unsupported against the migrated source and must fail on schema-13 metadata or
events. Never run old and new writers concurrently against one database.
