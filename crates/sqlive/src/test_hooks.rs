//! Hooks a test uses to make a connection fail, panic or end the process
//! where it chooses.

use super::{HostSql, Policy};

impl<P: Policy> HostSql<P> {
    /// Fail each statement that fires `event` with `message`, through a
    /// trigger named `name`.
    pub fn fail_at(&self, name: &str, event: &str, message: &str) {
        self.batch(&format!(
            "CREATE TRIGGER {} {event} BEGIN SELECT RAISE(ABORT,'{}'); END",
            crate::sql::identifier(name),
            message.replace('\'', "''")
        ))
        .unwrap();
    }

    /// Set the most bytes a string or BLOB can hold on this connection,
    /// returning the limit it had.
    pub fn set_value_limit(&self, bytes: i32) -> i32 {
        self.connection.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH, bytes).unwrap()
    }

    /// End the process with status 86 as the next commit writes the WAL.
    pub fn crash_after_next_commit(&mut self) {
        self.connection
            .wal_hook(Some(|_: &mut rusqlite::hooks::Wal, _| std::process::exit(86)))
            .unwrap();
    }

    /// Panic in the next call of one of the host's SQL functions on this
    /// connection.
    pub fn panic_next_function(&self) {
        self.authorization.panic_next_function();
    }

    /// Panic in the next judgment of this connection's authorizer.
    pub fn panic_next_authorization(&self) {
        self.authorization.panic_next();
    }

    /// Panic in the next call of a write transaction's hook on this
    /// connection.
    pub fn panic_in_next_hook(&self) {
        self.hook_panics.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Have the authorizer refuse every COMMIT and ROLLBACK while `refuse`.
    pub fn refuse_transaction_end(&mut self, refuse: bool) {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization, TransactionOperation};
        let mut authorize = self.authorization.callback();
        self.connection
            .authorizer(Some(move |context: AuthContext<'_>| {
                if refuse
                    && matches!(
                        context.action,
                        AuthAction::Transaction { operation }
                            if !matches!(operation, TransactionOperation::Begin)
                    )
                {
                    Authorization::Deny
                } else {
                    authorize(context)
                }
            }))
            .unwrap();
    }

    /// End the process with status 86 after each `action` on `table`.
    pub fn crash_after(&self, table: &str, action: &str) {
        self.connection
            .create_scalar_function(
                "test_crash",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8
                    | rusqlite::functions::FunctionFlags::SQLITE_INNOCUOUS,
                |_| -> rusqlite::Result<i64> { std::process::exit(86) },
            )
            .unwrap();
        self.batch(&format!(
            "CREATE TRIGGER _coven_crash AFTER {action} ON {table} BEGIN SELECT test_crash(); END"
        ))
        .unwrap();
    }
}
