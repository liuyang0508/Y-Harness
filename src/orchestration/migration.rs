//! Explicit, backup-first SQLite Task Graph migration.

use std::{
    ffi::OsString,
    fs::{self, File},
    path::{Path, PathBuf},
    time::Duration,
};

use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use sha2::{Digest, Sha256};
use tokio::task;

use super::{
    MAX_TASK_GRAPH_JSON_BYTES, SqliteTaskCoordinator, TASK_GRAPH_SCHEMA_VERSION, TaskGraph,
    coordinator::encode_graph,
};
use crate::{HarnessError, TaskGraphId, sqlite::bounded_text};

const PREVIOUS_TASK_GRAPH_SCHEMA_VERSION: u32 = 1;
const MIN_MIGRATION_WORKING_BYTES: u64 = 1_048_576;
const MIGRATION_PAGE: usize = 16;
const BACKUP_MANIFEST_TABLE: &str = "y_harness_task_migration_backup";

/// Result category for one explicit SQLite Task Graph migration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskMigrationStatus {
    /// A legacy coordinator was backed up and advanced.
    Migrated,
    /// The coordinator already used the current schema.
    AlreadyCurrent,
}

/// Content-free evidence returned by one Task Graph migration attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskMigrationReport {
    /// Migration outcome.
    pub status: TaskMigrationStatus,
    /// Graph schema observed before migration.
    pub from_graph_schema: u32,
    /// Graph schema expected after migration.
    pub to_graph_schema: u32,
    /// Number of historical graphs migrated or observed.
    pub historical_graphs: u64,
    /// Additional backup bytes required by this attempt.
    pub required_backup_bytes: u64,
    /// Available bytes observed on the backup filesystem during preflight.
    pub available_backup_bytes: u64,
    /// Durable rollback backup, when migration ran.
    pub backup_path: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoreFingerprint {
    graph_count: u64,
    graphs_sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LegacyLayout {
    has_schema_version: bool,
}

impl SqliteTaskCoordinator {
    /// Migrates a schema-1 SQLite Task Graph store after creating or
    /// validating a complete rollback backup.
    ///
    /// Every old and new writer must be stopped. Historical graphs remain
    /// explicitly unscoped because tenant ownership cannot be inferred safely.
    pub async fn migrate(
        path: impl AsRef<Path>,
        backup_path: impl AsRef<Path>,
    ) -> Result<TaskMigrationReport, HarnessError> {
        let path = path.as_ref().to_owned();
        let backup_path = backup_path.as_ref().to_owned();
        task::spawn_blocking(move || migrate_sync(&path, &backup_path, MigrationStop::None))
            .await
            .map_err(|error| {
                HarnessError::Orchestration(format!(
                    "SQLite Task Graph migration task failed: {error}"
                ))
            })?
    }
}

fn migrate_sync(
    path: &Path,
    backup_path: &Path,
    stop: MigrationStop,
) -> Result<TaskMigrationReport, HarnessError> {
    validate_migration_paths(path, backup_path)?;
    let mut connection =
        Connection::open(path).map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    configure_connection(&connection)?;
    if !table_exists(&connection, "task_graphs")? {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph migration source has no task_graphs table".to_owned(),
        ));
    }
    let historical_graphs = graph_count(&connection)?;
    let Some(layout) = legacy_layout(&connection)? else {
        let fingerprint = current_store_fingerprint(&connection)?;
        return Ok(TaskMigrationReport {
            status: TaskMigrationStatus::AlreadyCurrent,
            from_graph_schema: TASK_GRAPH_SCHEMA_VERSION,
            to_graph_schema: TASK_GRAPH_SCHEMA_VERSION,
            historical_graphs: fingerprint.graph_count,
            required_backup_bytes: 0,
            available_backup_bytes: 0,
            backup_path: None,
        });
    };
    let fingerprint = legacy_store_fingerprint(&connection, layout)?;
    if fingerprint.graph_count != historical_graphs {
        return Err(HarnessError::Orchestration(
            "Task Graph count changed during migration preflight".to_owned(),
        ));
    }
    let (required_backup_bytes, available_backup_bytes) =
        migration_space_preflight(&connection, backup_path)?;
    if stop == MigrationStop::AfterPreflight {
        return Err(injected_stop("after preflight"));
    }
    create_or_validate_backup(path, backup_path, layout, fingerprint)?;
    if stop == MigrationStop::AfterBackup {
        return Err(injected_stop("after backup"));
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if legacy_layout(&transaction)? != Some(layout)
        || legacy_store_fingerprint(&transaction, layout)? != fingerprint
    {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph store changed after migration backup; stop all writers and retry with a new backup"
                .to_owned(),
        ));
    }
    transaction
        .execute_batch(
            "
            CREATE TABLE task_graphs_v2 (
                tenant_id      TEXT NOT NULL,
                graph_id       TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                revision       INTEGER NOT NULL CHECK(revision > 0),
                graph_json     TEXT NOT NULL,
                PRIMARY KEY (tenant_id, graph_id)
            );
            ",
        )
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    migrate_legacy_graphs(&transaction, layout)?;
    transaction
        .execute_batch(
            "
            DROP TABLE task_graphs;
            ALTER TABLE task_graphs_v2 RENAME TO task_graphs;
            ",
        )
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if stop == MigrationStop::BeforeCommit {
        return Err(injected_stop("before commit"));
    }
    transaction
        .commit()
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;

    Ok(TaskMigrationReport {
        status: TaskMigrationStatus::Migrated,
        from_graph_schema: PREVIOUS_TASK_GRAPH_SCHEMA_VERSION,
        to_graph_schema: TASK_GRAPH_SCHEMA_VERSION,
        historical_graphs,
        required_backup_bytes,
        available_backup_bytes,
        backup_path: Some(backup_path.to_owned()),
    })
}

fn migrate_legacy_graphs(
    transaction: &Transaction<'_>,
    layout: LegacyLayout,
) -> Result<(), HarnessError> {
    let mut after_id = String::new();
    loop {
        let page = legacy_page(transaction, layout, &after_id)?;
        if page.is_empty() {
            return Ok(());
        }
        for (graph_id, revision, encoded) in page {
            let graph = decode_legacy_graph(&encoded)?;
            let current = encode_graph(&graph, None)?;
            let changed = transaction
                .execute(
                    "INSERT INTO task_graphs_v2
                        (tenant_id, graph_id, schema_version, revision, graph_json)
                     VALUES ('', ?1, ?2, ?3, ?4)",
                    params![
                        graph_id,
                        i64::from(TASK_GRAPH_SCHEMA_VERSION),
                        revision,
                        current
                    ],
                )
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            if changed != 1 {
                return Err(HarnessError::Orchestration(
                    "legacy Task Graph migration changed an unexpected row count".to_owned(),
                ));
            }
            after_id = graph_id;
        }
    }
}

fn legacy_page(
    connection: &Connection,
    layout: LegacyLayout,
    after_id: &str,
) -> Result<Vec<(String, i64, String)>, HarnessError> {
    let schema_filter = if layout.has_schema_version {
        "AND schema_version = ?3"
    } else {
        ""
    };
    let sql = format!(
        "SELECT length(CAST(graph_id AS BLOB)), graph_id, revision,
                length(CAST(graph_json AS BLOB)), graph_json
         FROM task_graphs
         WHERE graph_id > ?1 {schema_filter}
         ORDER BY graph_id
         LIMIT ?2"
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let page_size = i64::try_from(MIGRATION_PAGE).unwrap_or(i64::MAX);
    let mut rows = if layout.has_schema_version {
        statement
            .query(params![
                after_id,
                page_size,
                i64::from(PREVIOUS_TASK_GRAPH_SCHEMA_VERSION)
            ])
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?
    } else {
        statement
            .query(params![after_id, page_size])
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?
    };
    let mut page = Vec::with_capacity(MIGRATION_PAGE);
    while let Some(row) = rows
        .next()
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?
    {
        let graph_id = bounded_text(row, 0, 1, 256, "legacy Task Graph identity")
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        validate_graph_id(&graph_id)?;
        let revision = row
            .get::<_, i64>(2)
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        validate_revision(revision)?;
        let encoded = bounded_text(
            row,
            3,
            4,
            MAX_TASK_GRAPH_JSON_BYTES,
            "legacy Task Graph snapshot",
        )
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        decode_legacy_graph(&encoded)?;
        page.push((graph_id, revision, encoded));
    }
    Ok(page)
}

fn decode_legacy_graph(encoded: &str) -> Result<TaskGraph, HarnessError> {
    if encoded.len() > MAX_TASK_GRAPH_JSON_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "legacy Task Graph exceeds {MAX_TASK_GRAPH_JSON_BYTES} bytes"
        )));
    }
    let graph: TaskGraph = serde_json::from_str(encoded).map_err(|error| {
        HarnessError::Orchestration(format!("decode legacy Task Graph: {error}"))
    })?;
    graph.validate_integrity()?;
    Ok(graph)
}

fn legacy_store_fingerprint(
    connection: &Connection,
    layout: LegacyLayout,
) -> Result<StoreFingerprint, HarnessError> {
    let mut after_id = String::new();
    let mut hasher = Sha256::new();
    let mut count = 0_u64;
    loop {
        let page = legacy_page(connection, layout, &after_id)?;
        if page.is_empty() {
            break;
        }
        for (graph_id, revision, encoded) in page {
            update_fingerprint_text(&mut hasher, &graph_id)?;
            hasher.update(revision.to_le_bytes());
            update_fingerprint_text(&mut hasher, &encoded)?;
            count = count.checked_add(1).ok_or_else(|| {
                HarnessError::Orchestration("Task Graph count overflow".to_owned())
            })?;
            after_id = graph_id;
        }
    }
    hasher.update(count.to_le_bytes());
    Ok(StoreFingerprint {
        graph_count: count,
        graphs_sha256: hasher.finalize().into(),
    })
}

fn current_store_fingerprint(connection: &Connection) -> Result<StoreFingerprint, HarnessError> {
    let mut statement = connection
        .prepare(
            "SELECT length(CAST(tenant_id AS BLOB)), tenant_id,
                    length(CAST(graph_id AS BLOB)), graph_id, schema_version, revision,
                    length(CAST(graph_json AS BLOB)), graph_json
             FROM task_graphs
             ORDER BY tenant_id, graph_id",
        )
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                bounded_text(row, 0, 1, 256, "Task tenant")?,
                bounded_text(row, 2, 3, 256, "Task Graph identity")?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                bounded_text(row, 6, 7, MAX_TASK_GRAPH_JSON_BYTES, "Task Graph snapshot")?,
            ))
        })
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let mut hasher = Sha256::new();
    let mut count = 0_u64;
    for row in rows {
        let (tenant, graph_id, schema, revision, encoded) =
            row.map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        if schema != i64::from(TASK_GRAPH_SCHEMA_VERSION) {
            return Err(HarnessError::Orchestration(format!(
                "unsupported Task Graph schema version {schema}"
            )));
        }
        validate_graph_id(&graph_id)?;
        validate_revision(revision)?;
        super::coordinator::decode_snapshot(
            TaskGraphId::from_string(graph_id.clone()),
            &tenant,
            schema,
            revision,
            &encoded,
        )?;
        for value in [&tenant, &graph_id, &encoded] {
            update_fingerprint_text(&mut hasher, value)?;
        }
        hasher.update(revision.to_le_bytes());
        count = count
            .checked_add(1)
            .ok_or_else(|| HarnessError::Orchestration("Task Graph count overflow".to_owned()))?;
    }
    hasher.update(count.to_le_bytes());
    Ok(StoreFingerprint {
        graph_count: count,
        graphs_sha256: hasher.finalize().into(),
    })
}

fn legacy_layout(connection: &Connection) -> Result<Option<LegacyLayout>, HarnessError> {
    let columns = table_columns(connection)?;
    let has_tenant = columns.iter().any(|(name, _)| name == "tenant_id");
    let has_schema = columns.iter().any(|(name, _)| name == "schema_version");
    let has_revision = columns.iter().any(|(name, _)| name == "revision");
    let has_graph_json = columns.iter().any(|(name, _)| name == "graph_json");
    let tenant_pk = columns
        .iter()
        .find_map(|(name, pk)| (name == "tenant_id").then_some(*pk))
        .unwrap_or(0);
    let graph_pk = columns
        .iter()
        .find_map(|(name, pk)| (name == "graph_id").then_some(*pk))
        .unwrap_or(0);
    if columns.len() == 5
        && has_tenant
        && has_schema
        && has_revision
        && has_graph_json
        && tenant_pk == 1
        && graph_pk == 2
    {
        let unsupported: Option<i64> = connection
            .query_row(
                "SELECT schema_version FROM task_graphs
                 WHERE schema_version != ?1 LIMIT 1",
                [i64::from(TASK_GRAPH_SCHEMA_VERSION)],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
        if let Some(schema) = unsupported {
            return Err(HarnessError::Orchestration(format!(
                "unsupported Task Graph schema version {schema}"
            )));
        }
        return Ok(None);
    }
    let expected_legacy_columns = if has_schema { 4 } else { 3 };
    if columns.len() == expected_legacy_columns
        && !has_tenant
        && has_revision
        && has_graph_json
        && graph_pk == 1
    {
        if has_schema {
            let unsupported: Option<i64> = connection
                .query_row(
                    "SELECT schema_version FROM task_graphs
                     WHERE schema_version != ?1 LIMIT 1",
                    [i64::from(PREVIOUS_TASK_GRAPH_SCHEMA_VERSION)],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
            if let Some(schema) = unsupported {
                return Err(HarnessError::Orchestration(format!(
                    "unsupported legacy Task Graph schema version {schema}"
                )));
            }
        }
        return Ok(Some(LegacyLayout {
            has_schema_version: has_schema,
        }));
    }
    Err(HarnessError::Orchestration(
        "unsupported SQLite Task Graph table layout".to_owned(),
    ))
}

fn table_columns(connection: &Connection) -> Result<Vec<(String, i64)>, HarnessError> {
    let mut statement = connection
        .prepare("PRAGMA table_info(task_graphs)")
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    rows.map(|row| row.map_err(|error| HarnessError::Orchestration(error.to_string())))
        .collect()
}

fn graph_count(connection: &Connection) -> Result<u64, HarnessError> {
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM task_graphs", [], |row| row.get(0))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    u64::try_from(count)
        .map_err(|_| HarnessError::Orchestration("Task Graph count is invalid".to_owned()))
}

fn validate_graph_id(value: &str) -> Result<(), HarnessError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(HarnessError::Orchestration(
            "legacy Task Graph identity must be 1-256 non-control bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_revision(value: i64) -> Result<(), HarnessError> {
    if value <= 0 {
        return Err(HarnessError::Orchestration(
            "legacy Task Graph revision is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_migration_paths(path: &Path, backup_path: &Path) -> Result<(), HarnessError> {
    if !path.is_file() {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph migration source must be an existing file".to_owned(),
        ));
    }
    if path == backup_path {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph migration backup must differ from the source".to_owned(),
        ));
    }
    if backup_path.to_str().is_none() {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph backup path must be valid UTF-8".to_owned(),
        ));
    }
    let parent = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if parent.is_some_and(|parent| !parent.is_dir()) {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph backup parent must already exist".to_owned(),
        ));
    }
    Ok(())
}

fn configure_connection(connection: &Connection) -> Result<(), HarnessError> {
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    connection
        .execute_batch("PRAGMA synchronous = FULL;")
        .map_err(|error| HarnessError::Orchestration(error.to_string()))
}

fn migration_space_preflight(
    connection: &Connection,
    backup_path: &Path,
) -> Result<(u64, u64), HarnessError> {
    let used_pages = pragma_u64(connection, "page_count")?
        .saturating_sub(pragma_u64(connection, "freelist_count")?);
    let required_backup_bytes = if backup_path.exists() {
        0
    } else {
        used_pages
            .checked_mul(pragma_u64(connection, "page_size")?)
            .and_then(|bytes| bytes.checked_add(MIN_MIGRATION_WORKING_BYTES))
            .ok_or_else(|| {
                HarnessError::Orchestration(
                    "Task Graph migration disk requirement overflow".to_owned(),
                )
            })?
    };
    let backup_probe = backup_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let available_backup_bytes = fs2::available_space(backup_probe)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if available_backup_bytes < required_backup_bytes {
        return Err(HarnessError::Orchestration(format!(
            "SQLite Task Graph migration requires {required_backup_bytes} backup bytes, found {available_backup_bytes}"
        )));
    }
    let source_probe = connection
        .path()
        .map(Path::new)
        .and_then(Path::parent)
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let source_available = fs2::available_space(source_probe)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if source_available < MIN_MIGRATION_WORKING_BYTES {
        return Err(HarnessError::Orchestration(format!(
            "SQLite Task Graph migration requires {MIN_MIGRATION_WORKING_BYTES} working bytes on the source filesystem, found {source_available}"
        )));
    }
    Ok((required_backup_bytes, available_backup_bytes))
}

fn create_or_validate_backup(
    source_path: &Path,
    backup_path: &Path,
    layout: LegacyLayout,
    fingerprint: StoreFingerprint,
) -> Result<(), HarnessError> {
    if backup_path.exists() {
        return validate_backup(backup_path, layout, fingerprint);
    }
    let partial_path = partial_backup_path(backup_path);
    let source = Connection::open(source_path)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    configure_connection(&source)?;
    let partial_text = partial_path.to_str().ok_or_else(|| {
        HarnessError::Orchestration("Task Graph migration partial path is not UTF-8".to_owned())
    })?;
    source
        .execute("VACUUM INTO ?1", [partial_text])
        .map_err(|error| {
            HarnessError::Orchestration(format!("cannot create Task Graph backup: {error}"))
        })?;
    drop(source);

    let backup = Connection::open(&partial_path)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    backup
        .execute_batch("PRAGMA journal_mode = DELETE; PRAGMA synchronous = FULL;")
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    backup
        .execute_batch(&format!(
            "CREATE TABLE {BACKUP_MANIFEST_TABLE} (
                id                INTEGER PRIMARY KEY CHECK(id = 1),
                from_graph_schema INTEGER NOT NULL,
                to_graph_schema   INTEGER NOT NULL,
                graph_count       INTEGER NOT NULL,
                graphs_sha256     TEXT NOT NULL CHECK(length(graphs_sha256) = 64),
                had_schema_column INTEGER NOT NULL CHECK(had_schema_column IN (0, 1))
            );"
        ))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    backup
        .execute(
            &format!(
                "INSERT INTO {BACKUP_MANIFEST_TABLE}
                    (id, from_graph_schema, to_graph_schema, graph_count,
                     graphs_sha256, had_schema_column)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5)"
            ),
            params![
                i64::from(PREVIOUS_TASK_GRAPH_SCHEMA_VERSION),
                i64::from(TASK_GRAPH_SCHEMA_VERSION),
                to_i64(fingerprint.graph_count, "Task Graph count")?,
                fingerprint_hex(&fingerprint.graphs_sha256),
                i64::from(layout.has_schema_version),
            ],
        )
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    drop(backup);
    File::open(&partial_path)
        .and_then(|file| file.sync_all())
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    fs::hard_link(&partial_path, backup_path).map_err(|error| {
        HarnessError::Orchestration(format!(
            "cannot publish Task Graph backup without overwriting an existing path: {error}"
        ))
    })?;
    sync_parent_directory(backup_path)?;
    fs::remove_file(&partial_path)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    sync_parent_directory(backup_path)?;
    validate_backup(backup_path, layout, fingerprint)
}

fn validate_backup(
    backup_path: &Path,
    layout: LegacyLayout,
    expected: StoreFingerprint,
) -> Result<(), HarnessError> {
    if !backup_path.is_file() {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph backup is not a regular file".to_owned(),
        ));
    }
    let backup = Connection::open_with_flags(backup_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    let integrity: String = backup
        .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    if integrity != "ok" {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph backup failed integrity_check".to_owned(),
        ));
    }
    let manifest = backup
        .query_row(
            &format!(
                "SELECT from_graph_schema, to_graph_schema, graph_count,
                        length(CAST(graphs_sha256 AS BLOB)), graphs_sha256,
                        had_schema_column
                 FROM {BACKUP_MANIFEST_TABLE} WHERE id = 1"
            ),
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    bounded_text(row, 3, 4, 64, "Task Graph backup fingerprint")?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?
        .ok_or_else(|| {
            HarnessError::Orchestration(
                "SQLite Task Graph backup has no migration manifest".to_owned(),
            )
        })?;
    let expected_hex = fingerprint_hex(&expected.graphs_sha256);
    if manifest.0 != i64::from(PREVIOUS_TASK_GRAPH_SCHEMA_VERSION)
        || manifest.1 != i64::from(TASK_GRAPH_SCHEMA_VERSION)
        || manifest.2 != to_i64(expected.graph_count, "Task Graph count")?
        || manifest.3 != expected_hex
        || manifest.4 != i64::from(layout.has_schema_version)
        || legacy_layout(&backup)? != Some(layout)
        || legacy_store_fingerprint(&backup, layout)? != expected
    {
        return Err(HarnessError::Orchestration(
            "SQLite Task Graph backup does not match the migration source".to_owned(),
        ));
    }
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, HarnessError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|value| value.is_some())
        .map_err(|error| HarnessError::Orchestration(error.to_string()))
}

fn pragma_u64(connection: &Connection, pragma: &str) -> Result<u64, HarnessError> {
    let value: i64 = connection
        .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .map_err(|error| HarnessError::Orchestration(error.to_string()))?;
    u64::try_from(value)
        .map_err(|_| HarnessError::Orchestration(format!("invalid SQLite {pragma}")))
}

fn update_fingerprint_text(hasher: &mut Sha256, value: &str) -> Result<(), HarnessError> {
    let length = u64::try_from(value.len()).map_err(|_| {
        HarnessError::Orchestration("Task Graph fingerprint length overflow".to_owned())
    })?;
    hasher.update(length.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn fingerprint_hex(value: &[u8; 32]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn to_i64(value: u64, kind: &str) -> Result<i64, HarnessError> {
    i64::try_from(value).map_err(|_| HarnessError::Orchestration(format!("{kind} exceeds SQLite")))
}

fn partial_backup_path(backup_path: &Path) -> PathBuf {
    let mut name = backup_path
        .file_name()
        .map_or_else(|| OsString::from("task-backup"), OsString::from);
    name.push(format!(".{}.partial", std::process::id()));
    backup_path.with_file_name(name)
}

fn sync_parent_directory(path: &Path) -> Result<(), HarnessError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| HarnessError::Orchestration(error.to_string()))
}

fn injected_stop(phase: &str) -> HarnessError {
    HarnessError::Orchestration(format!("injected Task Graph migration stop {phase}"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MigrationStop {
    None,
    AfterPreflight,
    AfterBackup,
    BeforeCommit,
}

#[cfg(test)]
pub(super) fn migrate_with_stop(
    path: &Path,
    backup_path: &Path,
    phase: &str,
) -> Result<TaskMigrationReport, HarnessError> {
    let stop = match phase {
        "after_preflight" => MigrationStop::AfterPreflight,
        "after_backup" => MigrationStop::AfterBackup,
        "before_commit" => MigrationStop::BeforeCommit,
        _ => MigrationStop::None,
    };
    migrate_sync(path, backup_path, stop)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::{Path, PathBuf},
        time::Instant,
    };

    use rusqlite::{Connection, params};

    use super::{BACKUP_MANIFEST_TABLE, TaskMigrationStatus, migrate_with_stop, table_exists};
    use crate::{
        ActorIdentity, AuthorityContext, SqliteTaskCoordinator, TaskCoordinator, TaskDefinition,
        TaskGraph, TaskGraphId, TaskId, WorkspaceMode,
    };

    #[tokio::test]
    async fn migration_preserves_schema_one_graphs_as_explicitly_unscoped() {
        let source = temporary_database_path("source");
        let backup = temporary_database_path("backup");
        create_legacy_store(&source, true);

        let open_error = match SqliteTaskCoordinator::open(&source).await {
            Ok(_) => panic!("legacy store must require migration"),
            Err(error) => error,
        };
        assert!(open_error.to_string().contains("task-migrate"));

        let report = SqliteTaskCoordinator::migrate(&source, &backup)
            .await
            .expect("migrate");
        assert_eq!(report.status, TaskMigrationStatus::Migrated);
        assert_eq!(report.historical_graphs, 1);
        assert_eq!(report.backup_path.as_deref(), Some(backup.as_path()));

        let coordinator = SqliteTaskCoordinator::open(&source)
            .await
            .expect("open migrated store");
        let graph_id = TaskGraphId::from_static("legacy-graph");
        let unscoped = coordinator
            .load(&graph_id)
            .await
            .expect("load unscoped")
            .expect("legacy graph");
        assert_eq!(unscoped.tenant_id(), None);
        let tenant = authority("tenant-a");
        assert!(
            coordinator
                .load_as(&graph_id, &tenant)
                .await
                .expect("tenant lookup")
                .is_none()
        );

        let backup_db = Connection::open(&backup).expect("open backup");
        assert!(table_exists(&backup_db, BACKUP_MANIFEST_TABLE).expect("manifest"));
        let integrity: String = backup_db
            .query_row("PRAGMA integrity_check(1)", [], |row| row.get(0))
            .expect("integrity");
        assert_eq!(integrity, "ok");
        remove_database_files(&source);
        remove_database_files(&backup);
    }

    #[test]
    fn migration_restarts_after_every_mutating_phase() {
        for phase in ["after_preflight", "after_backup", "before_commit"] {
            let source = temporary_database_path(phase);
            let backup = temporary_database_path(&format!("{phase}-backup"));
            create_legacy_store(&source, true);

            migrate_with_stop(&source, &backup, phase).expect_err("injected stop");
            let report = migrate_with_stop(&source, &backup, "none").expect("resume migration");
            assert_eq!(report.status, TaskMigrationStatus::Migrated);
            let migrated = Connection::open(&source).expect("open migrated source");
            let tenant: String = migrated
                .query_row(
                    "SELECT tenant_id FROM task_graphs WHERE graph_id = 'legacy-graph'",
                    [],
                    |row| row.get(0),
                )
                .expect("tenant");
            assert!(tenant.is_empty());
            remove_database_files(&source);
            remove_database_files(&backup);
        }
    }

    #[tokio::test]
    async fn migration_accepts_the_pre_schema_dev_layout() {
        let source = temporary_database_path("dev-source");
        let backup = temporary_database_path("dev-backup");
        create_legacy_store(&source, false);
        let report = SqliteTaskCoordinator::migrate(&source, &backup)
            .await
            .expect("migrate dev layout");
        assert_eq!(report.status, TaskMigrationStatus::Migrated);
        SqliteTaskCoordinator::open(&source)
            .await
            .expect("open current store");
        remove_database_files(&source);
        remove_database_files(&backup);
    }

    #[tokio::test]
    async fn current_store_is_idempotent_without_creating_backup() {
        let source = temporary_database_path("current-source");
        let backup = temporary_database_path("current-backup");
        let coordinator = SqliteTaskCoordinator::open(&source)
            .await
            .expect("create current store");
        coordinator
            .create(TaskGraphId::from_static("current-graph"), graph())
            .await
            .expect("create current graph");
        drop(coordinator);

        let report = SqliteTaskCoordinator::migrate(&source, &backup)
            .await
            .expect("observe current store");
        assert_eq!(report.status, TaskMigrationStatus::AlreadyCurrent);
        assert_eq!(report.historical_graphs, 1);
        assert!(report.backup_path.is_none());
        assert!(!backup.exists());
        remove_database_files(&source);
    }

    #[tokio::test]
    async fn migration_rejects_unknown_schema_before_backup_creation() {
        let source = temporary_database_path("unknown-source");
        let backup = temporary_database_path("unknown-backup");
        create_legacy_store(&source, true);
        let connection = Connection::open(&source).expect("open legacy store");
        connection
            .execute("UPDATE task_graphs SET schema_version = 99", [])
            .expect("inject unknown schema");
        drop(connection);

        SqliteTaskCoordinator::migrate(&source, &backup)
            .await
            .expect_err("unknown schema");
        assert!(!backup.exists());
        remove_database_files(&source);
    }

    #[tokio::test]
    async fn migration_never_reuses_a_backup_from_different_history() {
        let first = temporary_database_path("first-source");
        let second = temporary_database_path("second-source");
        let backup = temporary_database_path("shared-backup");
        create_legacy_store_with_id(&first, true, "first-graph");
        create_legacy_store_with_id(&second, true, "second-graph");
        SqliteTaskCoordinator::migrate(&first, &backup)
            .await
            .expect("first migration");

        let error = SqliteTaskCoordinator::migrate(&second, &backup)
            .await
            .expect_err("mismatched backup");
        assert!(error.to_string().contains("does not match"));
        let second_db = Connection::open(&second).expect("open untouched second source");
        assert_eq!(
            super::legacy_layout(&second_db).expect("legacy layout"),
            Some(super::LegacyLayout {
                has_schema_version: true
            })
        );
        remove_database_files(&first);
        remove_database_files(&second);
        remove_database_files(&backup);
    }

    #[test]
    #[ignore = "manual maximum-size Task Graph migration performance evidence"]
    fn migrates_a_near_limit_task_graph_fixture() {
        let source = temporary_database_path("maximum-source");
        let backup = temporary_database_path("maximum-backup");
        let definitions = (0..1_000)
            .map(|index| TaskDefinition {
                id: TaskId::from_string(format!("task-{index:04}")),
                description: "x".repeat(65_536),
                dependencies: BTreeSet::new(),
                priority: 0,
                workspace: WorkspaceMode::Isolated,
            })
            .collect();
        let graph = TaskGraph::new(definitions).expect("near-limit graph");
        let encoded = serde_json::to_string(&graph).expect("encode near-limit graph");
        let connection = Connection::open(&source).expect("open legacy store");
        connection
            .execute_batch(
                "
                CREATE TABLE task_graphs (
                    graph_id       TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    revision       INTEGER NOT NULL CHECK(revision > 0),
                    graph_json     TEXT NOT NULL
                );
                ",
            )
            .expect("create legacy store");
        connection
            .execute(
                "INSERT INTO task_graphs
                    (graph_id, schema_version, revision, graph_json)
                 VALUES ('near-limit', 1, 1, ?1)",
                [encoded],
            )
            .expect("insert near-limit graph");
        drop(connection);

        let started = Instant::now();
        let report = migrate_with_stop(&source, &backup, "none").expect("migrate near-limit graph");
        assert_eq!(report.status, TaskMigrationStatus::Migrated);
        assert_eq!(report.historical_graphs, 1);
        println!(
            "migrated one near-limit 1,000-Task Graph in {:.3} ms",
            started.elapsed().as_secs_f64() * 1_000.0
        );
        remove_database_files(&source);
        remove_database_files(&backup);
    }

    fn create_legacy_store(path: &Path, with_schema: bool) {
        create_legacy_store_with_id(path, with_schema, "legacy-graph");
    }

    fn create_legacy_store_with_id(path: &Path, with_schema: bool, graph_id: &str) {
        let connection = Connection::open(path).expect("open legacy store");
        if with_schema {
            connection
                .execute_batch(
                    "
                    CREATE TABLE task_graphs (
                        graph_id       TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL,
                        revision       INTEGER NOT NULL CHECK(revision > 0),
                        graph_json     TEXT NOT NULL
                    );
                    ",
                )
                .expect("create schema-one table");
        } else {
            connection
                .execute_batch(
                    "
                    CREATE TABLE task_graphs (
                        graph_id   TEXT PRIMARY KEY,
                        revision   INTEGER NOT NULL CHECK(revision > 0),
                        graph_json TEXT NOT NULL
                    );
                    ",
                )
                .expect("create dev table");
        }
        let encoded = serde_json::to_string(&graph()).expect("encode graph");
        if with_schema {
            connection
                .execute(
                    "INSERT INTO task_graphs
                        (graph_id, schema_version, revision, graph_json)
                     VALUES (?1, 1, 7, ?2)",
                    params![graph_id, encoded],
                )
                .expect("insert graph");
        } else {
            connection
                .execute(
                    "INSERT INTO task_graphs (graph_id, revision, graph_json)
                     VALUES (?1, 7, ?2)",
                    params![graph_id, encoded],
                )
                .expect("insert graph");
        }
    }

    fn graph() -> TaskGraph {
        TaskGraph::new(vec![TaskDefinition {
            id: TaskId::from_static("task-a"),
            description: "legacy work".to_owned(),
            dependencies: BTreeSet::new(),
            priority: 0,
            workspace: WorkspaceMode::Isolated,
        }])
        .expect("graph")
    }

    fn authority(tenant: &str) -> AuthorityContext {
        AuthorityContext::new(
            ActorIdentity::Authenticated {
                authority: "test".to_owned(),
                subject: "migration-test".to_owned(),
            },
            Some(tenant.to_owned()),
        )
        .expect("authority")
    }

    fn temporary_database_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "y-harness-task-migration-{label}-{}.db",
            TaskGraphId::generate()
        ))
    }

    fn remove_database_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}
