//! Shared guards for values crossing the SQLite allocation boundary.

use std::path::Path;

use rusqlite::{Connection, OpenFlags, Row, types::Type};

/// Opens an existing SQLite database without create or write authority.
pub(crate) fn open_read_only(path: &Path) -> rusqlite::Result<Connection> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.pragma_update(None, "query_only", true)?;
    Ok(connection)
}

/// Reads one SQLite `TEXT` value only after its byte length is within policy.
///
/// Callers must select `length(CAST(column AS BLOB))` immediately before the
/// corresponding text column. The BLOB cast makes SQLite report UTF-8 bytes
/// rather than Unicode characters.
pub(crate) fn bounded_text(
    row: &Row<'_>,
    length_index: usize,
    text_index: usize,
    maximum_bytes: usize,
    kind: &'static str,
) -> rusqlite::Result<String> {
    let stored_bytes = row.get::<_, i64>(length_index)?;
    let valid = usize::try_from(stored_bytes)
        .ok()
        .is_some_and(|stored_bytes| stored_bytes <= maximum_bytes);
    if !valid {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            text_index,
            Type::Text,
            Box::new(std::io::Error::other(format!(
                "{kind} exceeds {maximum_bytes} bytes"
            ))),
        ));
    }
    row.get(text_index)
}

/// Reads one nullable SQLite `TEXT` value after enforcing its byte boundary.
///
/// Callers must select `length(CAST(column AS BLOB))` immediately before the
/// nullable text column.
pub(crate) fn bounded_optional_text(
    row: &Row<'_>,
    length_index: usize,
    text_index: usize,
    maximum_bytes: usize,
    kind: &'static str,
) -> rusqlite::Result<Option<String>> {
    let Some(stored_bytes) = row.get::<_, Option<i64>>(length_index)? else {
        return Ok(None);
    };
    let valid = usize::try_from(stored_bytes)
        .ok()
        .is_some_and(|stored_bytes| stored_bytes <= maximum_bytes);
    if !valid {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            text_index,
            Type::Text,
            Box::new(std::io::Error::other(format!(
                "{kind} exceeds {maximum_bytes} bytes"
            ))),
        ));
    }
    row.get(text_index)
}

#[cfg(test)]
mod tests {
    #[test]
    fn rejects_text_before_materializing_an_oversized_column() {
        let connection = rusqlite::Connection::open_in_memory().expect("open memory database");
        let error = connection
            .query_row("SELECT length(CAST(?1 AS BLOB)), ?1", ["four"], |row| {
                super::bounded_text(row, 0, 1, 3, "fixture")
            })
            .expect_err("oversized text");
        assert!(error.to_string().contains("fixture exceeds 3 bytes"));
    }

    #[test]
    fn reads_bounded_nullable_text() {
        let connection = rusqlite::Connection::open_in_memory().expect("open memory database");
        let present = connection
            .query_row("SELECT length(CAST(?1 AS BLOB)), ?1", ["name"], |row| {
                super::bounded_optional_text(row, 0, 1, 4, "fixture")
            })
            .expect("bounded optional text");
        assert_eq!(present.as_deref(), Some("name"));
        let absent = connection
            .query_row("SELECT NULL, NULL", [], |row| {
                super::bounded_optional_text(row, 0, 1, 4, "fixture")
            })
            .expect("null optional text");
        assert_eq!(absent, None);
    }
}
