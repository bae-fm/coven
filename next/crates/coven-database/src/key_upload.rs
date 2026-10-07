//! Fixed sealed copies published independently of an authored store-log entry.

use crate::{sqlite::DatabaseConnection, DbError};

pub(crate) fn prepare<F, E>(
    database: &DatabaseConnection,
    path: &str,
    seal: F,
) -> Result<Vec<u8>, E>
where
    F: FnOnce() -> Result<Vec<u8>, E>,
    E: From<DbError>,
{
    // The owner holds its writer lock through lookup, sealing and insertion.
    if let Some(bytes) = database
        .query(
            "SELECT bytes FROM coven_key_uploads WHERE path=?1",
            [path],
            |row| row.get(0),
        )?
        .into_iter()
        .next()
    {
        return Ok(bytes);
    }
    let bytes = seal()?;
    database.internal_execute(
        "INSERT INTO coven_key_uploads(path,bytes) VALUES(?1,?2)",
        (path, &bytes),
    )?;
    Ok(bytes)
}

pub(crate) fn complete(database: &DatabaseConnection, path: &str) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM coven_key_uploads WHERE path=?1", [path])?;
    Ok(())
}

#[cfg(test)]
#[path = "key_upload_tests.rs"]
mod tests;
