# SQLite State migration runbook

This runbook applies to schema-1 through schema-15 State stores moving to
schema 16, and to schema-16 stores that predate the independently versioned
Agent Loop wait projection schema 1.
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
yh state-migrate /absolute/path/state.db /absolute/path/state-pre-v16.rollback.db
```

Success reports the source and destination event and wait-projection
coordinates, immutable historical event count, preflight space values, and the
rollback backup path. Normal runtime open refuses a populated legacy database
until this command succeeds.

## What the command changes

The command creates a compact SQLite backup, records a migration manifest in
that backup, binds it to the complete authoritative event history with a
streaming SHA-256, validates it, installs and backfills the bounded live-wait
projection, and advances event, snapshot, and wait-projection writer metadata
inside one immediate transaction. Event-schema upgrades delete disposable
legacy snapshots; a projection-only schema-16 migration preserves already
current schema-16 snapshots. It does not change
historical event JSON, schema labels, sequence numbers, stream versions, or
recovery charges. For schema-1 through schema-7 sources, the transaction also
adds the nullable `streams.name` projection column. It adds nullable
`streams.tenant_id` to every schema-1 through schema-11 source. Existing
Threads remain explicitly unscoped; migration never infers ownership.
Schema-8 and newer names are validated against the journal before migration.
New events and snapshots use schema 16; schema-16 readers continue to validate
immutable schema-1 through schema-15 events. Schema-12 through schema-15
sources already contain the tenant projection, so migration advances metadata
and discards disposable snapshots without altering authoritative history.
Snapshots and the wait projection are rebuildable caches, not authoritative
history. Wait backfill streams only schema-16 lifecycle events and never
replays complete Thread aggregates.

Forked and imported streams preserve source-bound identities inside copied
wait and Approval evidence. Projection backfill permits that historical
source/target mismatch only for a structurally proven materialized stream:
creation first, one fork/import provenance event second, one consistent
embedded source Thread per wait lifecycle, and a terminal event for every
copied wait history. These terminal copied waits remain absent from the target
Thread's live projection. A missing, duplicated, late, lifecycle-inconsistent,
or unterminated provenance structure fails migration closed; normal Event
Store appends remain strictly target-bound. This check does not prove a
complete cryptographic ancestry chain because valid multi-level materialization
can retain a source older than its immediate provenance marker. That stronger
claim requires a future versioned lineage-chain format.

Migration does not rewrite a historical receipt-free `TurnFinished` success
into `TurnCompleted`, and it never synthesizes a `CompletionReceipt`.
Successful Turns written under schema 1 through 14 remain readable as
`legacy/unverified`; only a new schema-15 atomic `TurnCompleted { turn_id,
receipt }` event proves the format-1 completion conditions.

Migration also never infers `Waiting` from a legacy `Running` Turn, pending
Approval, final Item shape, or missing worker. It does not synthesize a wait
identity, remaining active timeout, execution generation, resume settlement,
or worker claim. Only new schema-16 transitions create
`AgentLoopExecution` evidence.

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
`state-pre-v16.rollback.db.partial-<id>`. It is not the final backup. Remove
orphan partials only after verifying that no migration process is active and
that either the final backup is valid or the source is still at its untouched
schema-1 through schema-15 coordinate.

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

Rollback is supported only before any schema-16 event has been committed. Once
a schema-16 event exists, restoring the pre-v16 backup discards newer
authoritative history and is therefore not a supported downgrade.

## Mixed-version rule

There is no rolling-upgrade window. A new reader can read schema-1 through
schema-15 history only after explicit migration. An old reader/writer is
unsupported against the migrated source and must fail on schema-16 metadata or
events. Never run old and new writers concurrently against one database.

See [ADR 0157](adr/0157-generation-bound-completion-receipt.md) for the
schema-15 successful-settlement and legacy-proof boundary, and
[ADR 0158](adr/0158-durable-agent-loop-waiting-and-resume.md) for schema-16
durable-wait evidence and its non-synthesis rule.
