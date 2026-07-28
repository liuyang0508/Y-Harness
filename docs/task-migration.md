# Task Graph schema migration

Task Graph schema 2 adds durable optional tenant ownership and a
tenant-partitioned Graph identity. Normal readers write and accept schema 2
only.

## Preconditions

1. Stop every process that can open the Task Coordinator database.
2. Choose a rollback path on a filesystem with sufficient free space.
3. Ensure the rollback path does not already name an unrelated file.

Run:

```bash
yh task-migrate \
  /absolute/path/tasks.db \
  /absolute/path/tasks-v1.rollback.db
```

The command validates every bounded schema-1 Graph, fingerprints its exact
identity, revision, and JSON, creates or verifies a no-clobber SQLite backup,
then replaces the table atomically. Historical Graphs receive `tenant=None`;
no tenant is inferred.

The older unreleased table without a `schema_version` column is accepted as
the same schema-1 source shape. Any other layout or schema fails closed.

## Verification

After migration:

```bash
sqlite3 /absolute/path/tasks.db \
  "pragma integrity_check; pragma table_info(task_graphs);"

yh doctor /absolute/path/y-harness.json
```

`integrity_check` must return `ok`. The table must contain `tenant_id`, and
`yh doctor` must report Task Graph schema 2.

## Rollback boundary

Before any schema-2 writer runs, stop all processes and restore the rollback
database as the source. After a schema-2 write, restoring the old backup
discards that newer work; there is no automatic backward conversion.

Never let schema-1 and schema-2 writers share a database. The backup is a
rollback artifact, not a live replica.
