use std::path::Path;

use coven_core::database::DbError;
use rusqlite::Connection;

pub(crate) fn install_platform_connection_opener() {
    coven_core::database::register_platform_connection_opener(open_native_connection);
}

fn open_native_connection(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .map_err(DbError::from)?;
    Ok(conn)
}
