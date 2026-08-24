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

pub(crate) fn deserialize_database_image_into(
    connection: &mut Connection,
    image: &[u8],
) -> Result<(), DbError> {
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
    connection
        .deserialize_read_exact(rusqlite::MAIN_DB, image.as_slice(), image.len(), false)
        .map_err(DbError::from)
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

/// Production selects WAL so a read-only connection on the same database keeps
/// reading committed rows while this writer commits more, instead of waiting
/// behind the exclusive lock a rollback journal takes at every commit. The mode
/// lives in the database header and persists, so a later read-only open finds
/// the database already in WAL. `FULL` is what puts a committed transaction on
/// the disk before an external step begins.
///
/// Tests select an in-memory journal with no physical sync: transaction
/// rollback stays real, while crash durability and the filesystem work it costs
/// are deliberately absent.
pub(crate) fn configure_connection_durability(
    connection: &Connection,
    durability: ConnectionDurability,
) -> Result<(), DbError> {
    let (journal_mode, synchronous) = match durability {
        ConnectionDurability::Full => ("WAL", "FULL"),
        #[cfg(any(test, feature = "test-utils"))]
        ConnectionDurability::Disabled => ("MEMORY", "OFF"),
    };
    connection
        .pragma_update_and_check(None, "journal_mode", journal_mode, |_| Ok(()))
        .map_err(DbError::from)?;
    connection
        .pragma_update(None, "synchronous", synchronous)
        .map_err(DbError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection_with_durability(path: &Path, durability: ConnectionDurability) -> Connection {
        let connection = Connection::open(path).expect("open database");
        configure_connection_durability(&connection, durability)
            .expect("configure connection durability");
        connection
    }

    #[test]
    fn writable_connection_pins_wal_commit_durability() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection = connection_with_durability(
            &directory.path().join("store.db"),
            ConnectionDurability::Full,
        );

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode");

        assert_eq!(synchronous, 2);
        assert_eq!(journal_mode, "wal");
    }

    #[test]
    fn writable_connection_can_keep_test_transactions_out_of_durable_files() {
        let directory = tempfile::tempdir().expect("create database directory");
        let connection = connection_with_durability(
            &directory.path().join("store.db"),
            ConnectionDurability::Disabled,
        );

        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("read synchronous setting");
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("read journal mode");

        assert_eq!(synchronous, 0);
        assert_eq!(journal_mode, "memory");
    }

    /// The point of WAL: a writer committing in a loop never blocks a reader on
    /// a second connection, and never blocks *on* one. Neither side waits, so
    /// both are run with no busy handler — under a rollback journal the writer's
    /// commit would need the exclusive lock the open read snapshot holds and
    /// fail here instead of quietly waiting.
    #[test]
    fn wal_writer_commits_continuously_while_a_reader_holds_a_snapshot() {
        let directory = tempfile::tempdir().expect("create database directory");
        let path = directory.path().join("store.db");
        let writer = connection_with_durability(&path, ConnectionDurability::Full);
        writer
            .execute_batch(
                "CREATE TABLE values_seen (value INTEGER NOT NULL);
                 INSERT INTO values_seen VALUES (1);",
            )
            .expect("seed the table");
        writer
            .busy_timeout(std::time::Duration::ZERO)
            .expect("writer waits for no lock");
        let reader = Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .expect("open secondary reader");
        reader
            .busy_timeout(std::time::Duration::ZERO)
            .expect("reader waits for no lock");

        let snapshot = reader
            .unchecked_transaction()
            .expect("begin the reader snapshot");
        let before: i64 = snapshot
            .query_row("SELECT value FROM values_seen", [], |row| row.get(0))
            .expect("reader opens its snapshot");
        for value in 2..=64 {
            writer
                .execute("UPDATE values_seen SET value = ?1", [value])
                .expect("writer commits while the reader snapshot stays open");
        }
        let during: i64 = snapshot
            .query_row("SELECT value FROM values_seen", [], |row| row.get(0))
            .expect("reader still reads its snapshot after 63 commits");
        drop(snapshot);
        let after: i64 = reader
            .query_row("SELECT value FROM values_seen", [], |row| row.get(0))
            .expect("reader observes committed rows once its snapshot closes");

        assert_eq!(before, 1);
        assert_eq!(during, 1);
        assert_eq!(after, 64);
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
