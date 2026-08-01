use crate::database::{authorize_host_sql, DbError};

pub(super) struct HostSqlTransaction<'transaction, 'connection> {
    transaction: &'transaction rusqlite::Transaction<'connection>,
    authorizer_installed: bool,
}

impl<'transaction, 'connection> HostSqlTransaction<'transaction, 'connection> {
    pub(super) fn begin(
        transaction: &'transaction rusqlite::Transaction<'connection>,
    ) -> Result<Self, DbError> {
        transaction
            .authorizer(Some(authorize_host_sql))
            .map_err(DbError::from)?;
        Ok(Self {
            transaction,
            authorizer_installed: true,
        })
    }

    pub(super) fn run<R, E>(
        mut self,
        f: impl FnOnce(&rusqlite::Transaction<'connection>) -> Result<R, E>,
    ) -> Result<R, E> {
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(self.transaction)));
        self.remove_authorizer();
        match outcome {
            Ok(result) => result,
            Err(panic) => std::panic::resume_unwind(panic),
        }
    }

    fn remove_authorizer(&mut self) {
        self.authorizer_installed = false;
        if let Err(error) = self.transaction.authorizer(
            None::<fn(rusqlite::hooks::AuthContext<'_>) -> rusqlite::hooks::Authorization>,
        ) {
            panic!("failed to remove host SQL gate-baseline guard: {error}");
        }
    }
}

impl Drop for HostSqlTransaction<'_, '_> {
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
    fn host_sql_cannot_mutate_coven_owned_tables() {
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
