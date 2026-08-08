use super::*;

pub(crate) fn seed_from(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), DbError> {
    if let Some(stamp) = value {
        let floor = Timestamp::parse(&stamp)
            .ok_or_else(|| DbError::Message(format!("corrupt {context}: {stamp:?}")))?;
        hlc.seed(&floor);
    }
    Ok(())
}

/// Greatest `_updated_at` within the restart seed's honest future bound, scanned
/// across every synced table. A registered table that does not exist is a host
/// integration error and surfaces as `Err`.
pub(crate) fn scan_max_updated_at(
    conn: &Connection,
    synced_tables: &[SyncedTable],
    seed_bound_ms: u64,
) -> Result<Option<String>, DbError> {
    let mut overall: Option<String> = None;
    let seed_bound = format!("{seed_bound_ms:013}");
    for t in synced_tables {
        let sql = format!(
            "SELECT MAX(_updated_at) FROM {} WHERE substr(_updated_at, 1, 13) <= ?1",
            crate::quote_ident(t.name())
        );
        let value: Option<String> = conn
            .query_row(&sql, [&seed_bound], |r| r.get::<_, Option<String>>(0))
            .map_err(|e| {
                DbError::context(
                    format!("register-floor scan over synced table {}", t.name()),
                    e,
                )
            })?;
        if let Some(v) = value {
            overall = Some(match overall {
                Some(cur) if cur >= v => cur,
                _ => v,
            });
        }
    }
    Ok(overall)
}

/// Create a capture session and attach every synced table, so a journaled
/// transaction records changes to exactly those tables.
pub fn attach_session<'c>(
    conn: &'c Connection,
    synced_tables: &[SyncedTable],
) -> Result<rusqlite::session::Session<'c>, DbError> {
    let mut session = rusqlite::session::Session::new(conn)
        .map_err(|e| DbError::context("failed to create capture session", e))?;
    for t in synced_tables {
        session.attach(Some(t.name())).map_err(|e| {
            DbError::context(
                format!("failed to attach synced table {} to session", t.name()),
                e,
            )
        })?;
    }
    Ok(session)
}

/// Drain a journal session's recorded changes into a changeset. The caller drops
/// the session right after (it lives only for the span of one journaled
/// transaction), so there is nothing to reset.
pub fn capture_changeset(session: &mut rusqlite::session::Session<'_>) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    session
        .changeset_strm(&mut buf)
        .map(|()| buf)
        .map_err(DbError::from)
}

pub fn open_database_image(image: &[u8]) -> Result<Connection, DbError> {
    const SQLITE_DATABASE_HEADER: &[u8; 16] = b"SQLite format 3\0";
    if image.len() < 20 || &image[..SQLITE_DATABASE_HEADER.len()] != SQLITE_DATABASE_HEADER {
        return Err(DbError::Message(
            "database image is not a SQLite database".to_string(),
        ));
    }
    let mut image = image.to_vec();
    // sqlite3_deserialize cannot use an image whose header requests WAL. A
    // private in-memory copy has no WAL file, so it uses rollback journaling.
    image[18] = 1;
    image[19] = 1;
    let mut connection = Connection::open_in_memory().map_err(DbError::from)?;
    connection
        .deserialize_read_exact(rusqlite::MAIN_DB, image.as_slice(), image.len(), false)
        .map_err(DbError::from)?;
    Ok(connection)
}

pub(crate) fn open_connection(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    // WAL so a read-only connection in another process can read committed rows while
    // this writer commits. The mode is stored in the db header and persists, so a
    // later read-only open finds the db already in WAL.
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .map_err(DbError::from)?;
    // The operation journal is the durable side of the local-intent →
    // idempotent-remote-step bridge. Pin FULL rather than inheriting SQLite's
    // compiled default so a successful journal commit reaches the OS before an
    // external step can begin.
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(DbError::from)?;
    Ok(conn)
}

/// Open a `SQLITE_OPEN_READONLY` connection for [`Database::open_read_only`].
/// `NO_MUTEX` because coven serializes every access
/// on its one connection thread; the connection sets no journal mode (a read-only
/// connection cannot, and the writer already put the db in WAL).
pub(crate) fn open_connection_read_only(path: &Path) -> Result<Connection, DbError> {
    use rusqlite::OpenFlags;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;
    Connection::open_with_flags(path, flags).map_err(DbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writable_connection_pins_wal_commit_durability() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection =
            open_connection(&directory.path().join("store.db")).expect("open database");

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode");

        assert_eq!(synchronous, 2);
        assert_eq!(journal_mode, "wal");
    }
}
