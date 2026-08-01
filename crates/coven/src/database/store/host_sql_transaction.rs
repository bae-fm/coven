use crate::database::{authorize_host_sql, DbError};

pub(super) struct HostSqlTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
    authorization: HostSqlAuthorization<'transaction>,
}

struct HostSqlAuthorization<'connection> {
    connection: &'connection rusqlite::Connection,
    authorizer_installed: bool,
}

impl<'connection> HostSqlAuthorization<'connection> {
    fn begin(connection: &'connection rusqlite::Connection) -> Result<Self, DbError> {
        connection
            .authorizer(Some(authorize_host_sql))
            .map_err(DbError::from)?;
        Ok(Self {
            connection,
            authorizer_installed: true,
        })
    }

    fn run<R>(mut self, f: impl FnOnce() -> R) -> R {
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        self.remove_authorizer();
        match outcome {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
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

pub(super) fn run_host_sql_read<R, E>(
    connection: &rusqlite::Connection,
    read: impl FnOnce(&rusqlite::Connection) -> Result<R, E>,
) -> Result<Result<R, E>, DbError> {
    let authorization = HostSqlAuthorization::begin(connection)?;
    Ok(authorization.run(|| read(connection)))
}

impl<'transaction, 'connection> HostSqlTransaction<'transaction, 'connection> {
    pub(super) fn begin(
        transaction: &'transaction rusqlite::Transaction<'connection>,
    ) -> Result<Self, DbError> {
        Ok(Self {
            transaction,
            authorization: HostSqlAuthorization::begin(transaction)?,
        })
    }

    pub(super) fn run<R, E>(
        self,
        f: impl FnOnce(&rusqlite::Transaction<'connection>) -> Result<R, E>,
    ) -> Result<R, E> {
        let transaction = self.transaction;
        self.authorization.run(|| f(transaction))
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

        HostSqlTransaction::begin(&tx)
            .expect("install authorizer")
            .run(|_| Ok::<_, DbError>(()))
            .expect("successful host SQL");
        assert_guard_removed();

        let error = HostSqlTransaction::begin(&tx)
            .expect("install authorizer")
            .run(|_| Err::<(), _>(DbError::Message("host".into())));
        assert!(error.is_err());
        assert_guard_removed();

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            HostSqlTransaction::begin(&tx)
                .expect("install authorizer")
                .run(|_| -> Result<(), DbError> { panic!("host panic") })
                .expect("panicking host SQL closure never returns");
        }));
        assert!(panic.is_err());
        assert_guard_removed();
    }

    #[test]
    fn dropping_host_sql_transaction_removes_the_authorizer() {
        let conn = Connection::open_in_memory().expect("open");
        conn.execute_batch(
            "ATTACH ':memory:' AS coven_gate_empty; \
             CREATE TABLE coven_gate_empty.baseline (id TEXT PRIMARY KEY) STRICT; \
             INSERT INTO coven_gate_empty.baseline VALUES ('guarded');",
        )
        .expect("attach guarded schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        drop(HostSqlTransaction::begin(&tx).expect("install authorizer"));

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
        crate::database::apply_coven_schema(&conn).expect("install Coven schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        let error = HostSqlTransaction::begin(&tx)
            .expect("install authorizer")
            .run(|transaction| {
                transaction
                    .execute(
                        "INSERT INTO protocol_state (key, value) VALUES ('host-owned', 'no')",
                        [],
                    )
                    .map(|_| ())
                    .map_err(DbError::from)
            })
            .expect_err("host SQL must not write Coven bookkeeping");
        assert!(error.to_string().contains("not authorized"));

        let error = HostSqlTransaction::begin(&tx)
            .expect("install authorizer")
            .run(|transaction| {
                transaction
                    .query_row("SELECT COUNT(*) FROM protocol_state", [], |row| {
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

        let error = run_host_sql_read(&conn, |connection| {
            connection
                .query_row("SELECT COUNT(*) FROM protocol_state", [], |row| {
                    row.get::<_, i64>(0)
                })
                .map(|_| ())
                .map_err(DbError::from)
        })
        .expect("install host read authorizer")
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
        crate::database::apply_coven_schema(&conn).expect("install Coven schema");
        let tx = conn.unchecked_transaction().expect("begin transaction");

        HostSqlTransaction::begin(&tx)
            .expect("install authorizer")
            .run(|transaction| {
                crate::database::with_coven_sql_authority(|| {
                    transaction
                        .execute(
                            "INSERT INTO protocol_state (key, value) VALUES ('coven-owned', 'yes')",
                            [],
                        )
                        .map(|_| ())
                        .map_err(DbError::from)
                })?;
                transaction
                    .execute(
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
}
