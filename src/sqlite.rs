//! Shared guards for values crossing the SQLite allocation boundary.

use rusqlite::{Row, types::Type};

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
}
