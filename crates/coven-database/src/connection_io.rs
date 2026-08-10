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
pub(crate) fn attach_session<'c>(
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
pub(crate) fn capture_changeset(
    session: &mut rusqlite::session::Session<'_>,
) -> Result<Vec<u8>, DbError> {
    let mut buf = Vec::new();
    session
        .changeset_strm(&mut buf)
        .map(|()| buf)
        .map_err(DbError::from)
}

pub(crate) fn open_database_image(image: &[u8]) -> Result<Connection, DbError> {
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

pub(crate) fn serialize_database_image(connection: &Connection) -> Result<Vec<u8>, DbError> {
    connection
        .serialize(rusqlite::MAIN_DB)
        .map(|bytes| bytes.to_vec())
        .map_err(DbError::from)
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ConnectionDurability {
    Full,
    #[cfg(any(test, feature = "test-utils"))]
    Disabled,
}

pub(crate) fn configure_connection_durability(
    connection: &Connection,
    durability: ConnectionDurability,
) -> Result<(), DbError> {
    configure_connection_schema_durability(connection, None, durability)
}

pub(crate) fn configure_connection_schema_durability(
    connection: &Connection,
    schema: Option<&str>,
    durability: ConnectionDurability,
) -> Result<(), DbError> {
    let (journal_mode, synchronous) = match durability {
        ConnectionDurability::Full => ("DELETE", "FULL"),
        #[cfg(any(test, feature = "test-utils"))]
        ConnectionDurability::Disabled => ("MEMORY", "OFF"),
    };
    connection
        .pragma_update_and_check(schema, "journal_mode", journal_mode, |_| Ok(()))
        .map_err(DbError::from)?;
    connection
        .pragma_update(schema, "synchronous", synchronous)
        .map_err(DbError::from)
}

pub(crate) fn open_connection(
    path: &Path,
    durability: ConnectionDurability,
) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    // Rollback journaling lets one SQLite transaction commit the Store and an
    // attached operation journal through a super-journal. Production selects
    // DELETE + FULL so that cross-file commit is crash-atomic before an external
    // step begins. Tests select MEMORY + OFF: transaction rollback remains real,
    // while crash durability and its filesystem work are deliberately absent.
    configure_connection_durability(&conn, durability)?;
    Ok(conn)
}

/// Open a `SQLITE_OPEN_READONLY` connection for [`Database::open_read_only`].
/// `NO_MUTEX` because coven serializes every access
/// on its one connection thread; the connection sets no journal mode because a
/// read-only connection cannot change the writer-owned database setting.
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
    fn writable_connection_uses_crash_atomic_rollback_journaling() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection = open_connection(
            &directory.path().join("store.db"),
            ConnectionDurability::Full,
        )
        .expect("open database");

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode");

        assert_eq!(synchronous, 2);
        assert_eq!(journal_mode, "delete");
    }

    #[test]
    fn writable_connection_can_keep_test_transactions_out_of_durable_files() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection = open_connection(
            &directory.path().join("store.db"),
            ConnectionDurability::Disabled,
        )
        .expect("open database");

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode");

        assert_eq!(synchronous, 0);
        assert_eq!(journal_mode, "memory");
    }

    #[test]
    fn attached_test_journal_uses_the_store_connections_durability() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection = open_connection(
            &directory.path().join("store.db"),
            ConnectionDurability::Disabled,
        )
        .expect("open Store database");
        let pending_path = directory.path().join("pending.db");
        Connection::open(&pending_path).expect("create pending database");
        let pending_path = pending_path.to_string_lossy().into_owned();
        connection
            .execute("ATTACH DATABASE ?1 AS pending", [&pending_path])
            .expect("attach pending database");

        configure_connection_schema_durability(
            &connection,
            Some("pending"),
            ConnectionDurability::Disabled,
        )
        .expect("configure attached database");

        let synchronous: i64 = connection
            .query_row("PRAGMA pending.synchronous", [], |row| row.get(0))
            .expect("read attached synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA pending.journal_mode", [], |row| row.get(0))
            .expect("read attached journal mode");
        assert_eq!(synchronous, 0);
        assert_eq!(journal_mode, "memory");
    }

    #[test]
    fn rollback_writer_and_secondary_reader_observe_commits() {
        let directory = tempfile::tempdir().expect("create database directory");
        let path = directory.path().join("store.db");
        let writer = open_connection(&path, ConnectionDurability::Full).expect("open writer");
        writer
            .execute_batch("CREATE TABLE values_seen (value INTEGER NOT NULL);")
            .expect("create table");
        let reader = open_connection_read_only(&path).expect("open secondary reader");

        writer
            .execute("INSERT INTO values_seen VALUES (1)", [])
            .expect("commit writer row");
        let value: i64 = reader
            .query_row("SELECT value FROM values_seen", [], |row| row.get(0))
            .expect("secondary reader observes committed row");

        assert_eq!(value, 1);
    }

    #[test]
    fn bundled_sqlite_disables_global_memory_statistics() {
        let connection = Connection::open_in_memory().expect("open database");
        let disabled: bool = connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_compile_options
                     WHERE compile_options = 'DEFAULT_MEMSTATUS=0'
                 )",
                [],
                |row| row.get(0),
            )
            .expect("read SQLite compile options");

        assert!(
            disabled,
            "SQLite memory statistics serialize allocations from independent connections"
        );
    }
}
