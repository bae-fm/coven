use crate::{authorize_host_sql, DbError};

pub(crate) struct HostSqlAuthorization<'connection> {
    connection: &'connection rusqlite::Connection,
    authorizer_installed: bool,
}

impl<'connection> HostSqlAuthorization<'connection> {
    pub(crate) fn begin(connection: &'connection rusqlite::Connection) -> Result<Self, DbError> {
        crate::reset_host_sql_write_observation();
        connection
            .authorizer(Some(authorize_host_sql))
            .map_err(DbError::from)?;
        Ok(Self {
            connection,
            authorizer_installed: true,
        })
    }

    pub(crate) fn run<R>(mut self, f: impl FnOnce() -> R) -> R {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        self.remove_authorizer();
        match outcome {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    pub(crate) fn run_observing_write<R>(self, f: impl FnOnce() -> R) -> (R, bool) {
        let result = self.run(f);
        (result, crate::host_sql_write_was_observed())
    }

    fn remove_authorizer(&mut self) {
        self.authorizer_installed = false;
        if let Err(error) = self.connection.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        ) {
            panic!("failed to remove host SQL authorization: {error}");
        }
    }
}

impl Drop for HostSqlAuthorization<'_> {
    fn drop(&mut self) {
        if self.authorizer_installed {
            self.remove_authorizer();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn host_sql_authorizer_is_removed_after_success_error_and_panic() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "ATTACH ':memory:' AS coven_gate_empty; \
             CREATE TABLE coven_gate_empty.baseline (id TEXT PRIMARY KEY) STRICT; \
             INSERT INTO coven_gate_empty.baseline VALUES ('guarded');",
        )
        .expect("attach guarded schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");
        let assert_guard_removed = || {
            let id: String = tx
                .query_row("SELECT id FROM coven_gate_empty.baseline", [], |row| {
                    row.get(0)
                })
                .expect("internal SQL can address the baseline after host SQL");
            assert_eq!(id, "guarded");
        };

        HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| Ok::<_, DbError>(()))
            .expect("successful host SQL");
        assert_guard_removed();

        let error = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| Err::<(), _>(DbError::Message("host".into())));
        assert!(error.is_err());
        assert_guard_removed();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HostSqlAuthorization::begin(&tx)
                .expect("install authorizer")
                .run(|| -> Result<(), DbError> { panic!("host panic") })
                .expect("panicking host SQL closure never returns");
        }));
        assert!(panic.is_err());
        assert_guard_removed();
    }

    #[test]
    fn dropping_host_sql_authorization_removes_the_authorizer() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "ATTACH ':memory:' AS coven_gate_empty; \
             CREATE TABLE coven_gate_empty.baseline (id TEXT PRIMARY KEY) STRICT; \
             INSERT INTO coven_gate_empty.baseline VALUES ('guarded');",
        )
        .expect("attach guarded schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        drop(HostSqlAuthorization::begin(&tx).expect("install authorizer"));

        let id: String = tx
            .query_row("SELECT id FROM coven_gate_empty.baseline", [], |row| {
                row.get(0)
            })
            .expect("internal SQL can address the baseline after owner drop");
        assert_eq!(id, "guarded");
    }

    #[test]
    fn host_sql_cannot_read_or_mutate_coven_owned_tables() {
        let conn = Connection::open_in_memory().expect("open");
        crate::apply_coven_schema(&conn).expect("install Coven schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        let error = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES ('host-owned', 'no')",
                    [],
                )
                .map(|_| ())
                .map_err(DbError::from)
            })
            .expect_err("host SQL must not write Coven bookkeeping");
        assert!(error.to_string().contains("not authorized"));

        let error = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.query_row("SELECT COUNT(*) FROM protocol_state", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|_| ())
                .map_err(DbError::from)
            })
            .expect_err("host SQL must not read Coven bookkeeping");
        assert!(error.to_string().contains("not authorized"));

        let stored: bool = tx
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM protocol_state WHERE key = 'host-owned'
                )",
                [],
                |row| row.get(0),
            )
            .expect("query protocol state after refused host write");
        assert!(!stored);
        tx.rollback().expect("finish host SQL transaction test");

        let error = HostSqlAuthorization::begin(&conn)
            .expect("install host read authorizer")
            .run(|| {
                conn.query_row("SELECT COUNT(*) FROM protocol_state", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|_| ())
                .map_err(DbError::from)
            })
            .expect_err("host read API must not expose Coven bookkeeping");
        assert!(error.to_string().contains("not authorized"));
    }

    /// Coven's own entry points are documented to run inside the host's write
    /// closure (an external-blob registration or delete enqueue commits with
    /// the row change that motivated it), so the reserved-table writes they
    /// perform must pass the very authorizer that refuses the host's.
    #[test]
    fn coven_owned_writes_pass_the_host_sql_authorizer() {
        let conn = Connection::open_in_memory().expect("open");
        crate::apply_coven_schema(&conn).expect("install Coven schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                crate::with_coven_sql_authority(|| {
                    tx.execute(
                        "INSERT INTO protocol_state (key, value) VALUES ('coven-owned', 'yes')",
                        [],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
                })?;
                tx.execute(
                    "INSERT INTO protocol_state (key, value) VALUES ('host-owned', 'no')",
                    [],
                )
                .map(|_| ())
                .map_err(DbError::from)
            })
            .expect_err("host SQL after the Coven-owned write is still refused");

        let stored: bool = tx
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM protocol_state WHERE key = 'coven-owned'
                )",
                [],
                |row| row.get(0),
            )
            .expect("query protocol state after the authorized Coven write");
        assert!(stored, "the Coven-owned write commits");
    }

    #[test]
    fn write_observation_excludes_coven_owned_bookkeeping() {
        let conn = Connection::open_in_memory().expect("open");
        crate::apply_coven_schema(&conn).expect("install Coven schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        let (result, host_write_seen) = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run_observing_write(|| {
                crate::with_coven_sql_authority(|| {
                    tx.execute(
                        "INSERT INTO protocol_state (key, value) VALUES ('coven-owned', 'yes')",
                        [],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
                })
            });

        result.expect("Coven bookkeeping write");
        assert!(!host_write_seen);
    }

    #[test]
    fn coven_cleanup_guards_can_read_bookkeeping_but_host_sql_cannot_impersonate_them() {
        let conn = Connection::open_in_memory().expect("open");
        crate::apply_coven_schema(&conn).expect("install Coven schema");
        conn.execute_batch(
            "CREATE TABLE notes (id TEXT PRIMARY KEY) STRICT;
             CREATE TEMP TRIGGER coven_cleanup_guard_insert_notes
             BEFORE INSERT ON notes
             WHEN EXISTS(SELECT 1 FROM local_cleanup_intents WHERE blob_id = NEW.id)
             BEGIN SELECT RAISE(ABORT, 'cleanup in progress'); END;",
        )
        .expect("install Coven cleanup guard");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.execute("INSERT INTO notes (id) VALUES ('allowed')", [])
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .expect("Coven cleanup guard may inspect its bookkeeping");

        let direct_read = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.query_row("SELECT COUNT(*) FROM local_cleanup_intents", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|_| ())
                .map_err(DbError::from)
            });
        assert!(direct_read.is_err());

        let impersonation = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.execute_batch(
                    "CREATE TEMP TRIGGER coven_cleanup_guard_forged
                         BEFORE INSERT ON notes
                         BEGIN SELECT 1; END;",
                )
                .map_err(DbError::from)
            });
        assert!(impersonation.is_err());

        let removal = HostSqlAuthorization::begin(&tx)
            .expect("install authorizer")
            .run(|| {
                tx.execute_batch("DROP TRIGGER COVEN_CLEANUP_GUARD_INSERT_NOTES")
                    .map_err(DbError::from)
            });
        assert!(removal.is_err());
    }
}
