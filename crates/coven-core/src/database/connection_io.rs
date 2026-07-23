use super::*;

pub(super) fn seed_from(hlc: &Hlc, value: Option<String>, context: &str) -> Result<(), DbError> {
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
pub(super) fn scan_max_updated_at(
    conn: &Connection,
    synced_tables: &[SyncedTable],
    seed_bound_ms: u64,
) -> Result<Option<String>, DbError> {
    let mut overall: Option<String> = None;
    let seed_bound = format!("{seed_bound_ms:013}");
    for t in synced_tables {
        let sql = format!(
            "SELECT MAX(_updated_at) FROM {} WHERE substr(_updated_at, 1, 13) <= ?1",
            crate::sync::session::quote_ident(t.name())
        );
        let value: Option<String> = conn
            .query_row(&sql, [&seed_bound], |r| r.get::<_, Option<String>>(0))
            .map_err(|e| {
                DbError::Message(format!(
                    "register-floor scan over synced table {}: {e}",
                    t.name()
                ))
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
        .map_err(|e| DbError::Message(format!("failed to create capture session: {e}")))?;
    for t in synced_tables {
        session.attach(Some(t.name())).map_err(|e| {
            DbError::Message(format!(
                "failed to attach synced table {} to session: {e}",
                t.name()
            ))
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

pub(super) fn open_connection(path: &Path) -> Result<Connection, DbError> {
    let conn = Connection::open(path).map_err(DbError::from)?;
    // WAL so a read-only connection in another process can read committed rows while
    // this writer commits. The mode is stored in the db header and persists, so a
    // later read-only open finds the db already in WAL.
    conn.query_row("PRAGMA journal_mode = WAL", [], |_| Ok(()))
        .map_err(DbError::from)?;
    Ok(conn)
}

/// Open a `SQLITE_OPEN_READONLY` connection for [`Database::open_read_only`].
/// `NO_MUTEX` because coven serializes every access
/// on its one connection thread; the connection sets no journal mode (a read-only
/// connection cannot, and the writer already put the db in WAL).
pub(super) fn open_connection_read_only(path: &Path) -> Result<Connection, DbError> {
    use rusqlite::OpenFlags;
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_URI;
    Connection::open_with_flags(path, flags).map_err(DbError::from)
}
