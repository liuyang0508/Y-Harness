# Task Graph schema migration

Task Graph schema 4 adds bounded, canonical execution-capability requirements
to immutable Task definitions. Normal readers write and accept schema 4 only.

## Preconditions

1. Stop every process that can open the Task Coordinator database.
2. Choose a rollback path on a filesystem with sufficient free space.
3. Ensure the rollback path does not already name an unrelated file.

Run:

```bash
yh task-migrate \
  /absolute/path/tasks.db \
  /absolute/path/tasks-v3.rollback.db
```

The command accepts schema 1, schema 2, or schema 3, validates every bounded Graph,
fingerprints its exact tenant, identity, revision, and JSON, creates or
verifies a no-clobber SQLite backup, then replaces the table atomically.

- Schema-1 Graphs receive `tenant=None`; no tenant is inferred.
- Schema-2 Graph tenant ownership and every lifecycle field are preserved
  exactly. Historical Graphs have no attempt-binding evidence.
- Schema-3 ownership, lifecycle, and append-only exact-attempt execution
  bindings are preserved exactly.
- Every migrated historical Task receives an empty capability requirement.
  Migration does not infer capabilities from workspace mode, description,
  worker identity, execution binding, or other historical text.
- The older unreleased table without a `schema_version` column is accepted as
  the same schema-1 source shape.
- Mixed, unknown, or malformed stores fail before backup publication or source
  mutation. Schema-1/schema-2 rows carrying schema-3 binding evidence and any
  schema-1/schema-2/schema-3 row carrying schema-4 capability requirements
  also fail closed.

## Verification

After migration:

```bash
sqlite3 /absolute/path/tasks.db \
  "pragma integrity_check; pragma table_info(task_graphs);"

yh doctor /absolute/path/y-harness.json
```

`integrity_check` must return `ok`. The table must contain `tenant_id`, and
`yh doctor` must report Task Graph schema 4.

## Rollback boundary

Before any schema-4 writer runs, stop all processes and restore the rollback
database as the source. After a schema-4 write, restoring the old backup
discards that newer work; there is no automatic backward conversion.

Never let schema-1/schema-2/schema-3 and schema-4 writers share a database. The
backup is a rollback artifact, not a live replica.
