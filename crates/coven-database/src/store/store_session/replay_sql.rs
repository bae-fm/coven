use crate::DbError;
use rusqlite::{config::DbConfig, Connection};

/// Install recorded row effects without running the host's triggers or foreign
/// key actions again. The caller validates the resulting foreign-key graph
/// before committing its transaction. SQLite keeps TEMP triggers enabled, so
/// the connection's Coven cleanup guards remain in force.
pub(super) struct ReplaySql<'connection> {
    connection: &'connection Connection,
    foreign_keys: bool,
    triggers: bool,
    deferred_foreign_keys: bool,
    active: bool,
}

impl<'connection> ReplaySql<'connection> {
    pub(super) fn begin(connection: &'connection Connection) -> Result<Self, DbError> {
        let guard = Self {
            connection,
            foreign_keys: connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)?,
            triggers: connection.db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)?,
            deferred_foreign_keys: connection.pragma_query_value(
                None,
                "defer_foreign_keys",
                |row| row.get(0),
            )?,
            active: true,
        };
        // Unlike PRAGMA foreign_keys, db_config applies within a transaction.
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, false)?;
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, false)?;
        Ok(guard)
    }

    pub(super) fn run<T>(
        mut self,
        operation: impl FnOnce() -> Result<T, DbError>,
    ) -> Result<T, DbError> {
        let outcome = operation();
        let restored = self.restore();
        match (outcome, restored) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(operation), Err(restoration)) => Err(DbError::context(
                format!("replay SQL restoration also failed: {restoration}"),
                operation,
            )),
        }
    }

    fn restore(&mut self) -> Result<(), DbError> {
        // Attempt every restoration even if one fails. The pragma can allocate
        // or be refused by an authorizer; its error must roll back the caller's
        // transaction after both connection-wide action settings are restored.
        let deferred =
            self.connection
                .pragma_update(None, "defer_foreign_keys", self.deferred_foreign_keys);
        let foreign_keys = self
            .connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, self.foreign_keys);
        let triggers = self
            .connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, self.triggers);
        self.active = false;
        if let Err(error) = foreign_keys.and(triggers) {
            // SQLite's fixed boolean db_config options cannot fail on a live
            // connection. A panic would be caught by the connection worker and
            // permit reuse of this invalid connection, so do not unwind here.
            tracing::error!(%error, "failed to restore replay SQL action settings");
            std::process::abort();
        }
        deferred.map_err(|error| DbError::context("restore replay foreign-key deferral", error))
    }
}

impl Drop for ReplaySql<'_> {
    fn drop(&mut self) {
        if self.active {
            if let Err(error) = self.restore() {
                panic!("failed to restore replay SQL configuration: {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_restores_exact_settings_after_success_error_and_panic() {
        let connection = Connection::open_in_memory().expect("open database");
        for (foreign_keys, triggers, deferred) in [
            (true, true, true),
            (false, false, true),
            (true, false, false),
            (false, true, false),
        ] {
            connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, foreign_keys)
                .expect("set foreign keys");
            connection
                .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER, triggers)
                .expect("set triggers");
            let transaction = connection
                .unchecked_transaction()
                .expect("begin transaction");
            transaction
                .pragma_update(None, "defer_foreign_keys", deferred)
                .expect("defer foreign keys");
            for exit in 0..3 {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    ReplaySql::begin(&transaction)?.run(|| {
                        // Native changeset apply resets this pragma before returning.
                        transaction.pragma_update(None, "defer_foreign_keys", false)?;
                        match exit {
                            0 => Ok(()),
                            1 => Err(DbError::Message("replay failed".into())),
                            _ => panic!("replay panicked"),
                        }
                    })
                }));
                match exit {
                    0 => outcome.expect("normal return").expect("replay succeeds"),
                    1 => assert!(outcome.expect("error return").is_err()),
                    _ => assert!(outcome.is_err()),
                }
                assert_eq!(
                    transaction
                        .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                        .expect("read foreign keys"),
                    foreign_keys,
                );
                assert_eq!(
                    transaction
                        .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                        .expect("read triggers"),
                    triggers,
                );
                assert_eq!(transaction
                    .pragma_query_value(None, "defer_foreign_keys", |row| row.get::<_, bool>(0),)
                    .expect("read deferred foreign keys"), deferred);
            }
            transaction.rollback().expect("roll back transaction");
        }
    }

    #[test]
    fn replay_suppresses_host_triggers_and_preserves_temporary_guards() {
        let connection = Connection::open_in_memory().expect("open database");
        connection
            .execute_batch(
                "CREATE TABLE notes (id TEXT PRIMARY KEY) STRICT;
             CREATE TRIGGER host_insert BEFORE INSERT ON notes
             BEGIN SELECT RAISE(ABORT, 'host trigger'); END;
             CREATE TEMP TRIGGER coven_cleanup_guard_insert_notes
             BEFORE INSERT ON notes WHEN NEW.id = 'guarded'
             BEGIN SELECT RAISE(ABORT, 'cleanup in progress'); END;",
            )
            .expect("create triggers");
        let transaction = connection
            .unchecked_transaction()
            .expect("begin transaction");
        ReplaySql::begin(&transaction)
            .expect("begin replay")
            .run(|| {
                transaction
                    .execute("INSERT INTO notes VALUES ('replayed')", [])
                    .expect("recorded effects do not invoke the host trigger");
                let error = transaction
                    .execute("INSERT INTO notes VALUES ('guarded')", [])
                    .expect_err("cleanup guard remains active");
                assert!(error.to_string().contains("cleanup in progress"));
                Ok(())
            })
            .expect("restore settings");
        let error = transaction
            .execute("INSERT INTO notes VALUES ('host')", [])
            .expect_err("host trigger is restored");
        assert!(error.to_string().contains("host trigger"));
        transaction.rollback().expect("roll back transaction");
    }

    #[test]
    fn deferral_restoration_failure_restores_actions_and_preserves_operation_error() {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

        let connection = Connection::open_in_memory().expect("open database");
        connection
            .execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY) STRICT;")
            .expect("create table");
        connection
            .set_db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY, true)
            .expect("enable foreign keys");
        for operation_fails in [false, true] {
            let transaction = connection
                .unchecked_transaction()
                .expect("begin transaction");
            let error = ReplaySql::begin(&transaction)
                .expect("begin replay")
                .run(|| {
                    transaction.execute("INSERT INTO notes VALUES ('replayed')", [])?;
                    transaction.authorizer(Some(|context: AuthContext<'_>| {
                        match context.action {
                            AuthAction::Pragma {
                                pragma_name: "defer_foreign_keys",
                                pragma_value: Some(_),
                            } => Authorization::Deny,
                            _ => Authorization::Allow,
                        }
                    }))?;
                    if operation_fails {
                        Err(DbError::Message("original replay failure".into()))
                    } else {
                        Ok(())
                    }
                })
                .expect_err("failed restoration must prevent commit");
            transaction
                .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
                .expect("remove injected authorizer");
            assert!(error
                .to_string()
                .contains("restore replay foreign-key deferral"));
            if operation_fails {
                let DbError::Context { source, .. } = error else {
                    panic!("both errors retain the original source");
                };
                assert!(
                    matches!(*source, DbError::Message(ref value) if value == "original replay failure")
                );
            }
            assert!(transaction
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_FKEY)
                .expect("foreign keys restored"));
            assert!(transaction
                .db_config(DbConfig::SQLITE_DBCONFIG_ENABLE_TRIGGER)
                .expect("triggers restored"));
            transaction.rollback().expect("roll back refused replay");
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM notes", [], |row| row.get::<_, i64>(0))
                    .expect("read rolled back rows"),
                0
            );
        }
    }
}
