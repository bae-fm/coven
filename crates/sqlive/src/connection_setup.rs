//! What the host registers on a connection as it opens, before any statement
//! runs: SQL functions and collations.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSql;

use crate::authorization::{FunctionCall, HostFunctions, Policy, SqlAuthorization};
use crate::snapshot_values::{SnapshotValue, SnapshotValues};

/// What a connection's setup may register.
pub struct Setup<'a, P: Policy> {
    connection: &'a rusqlite::Connection,
    functions: HostFunctions<P>,
    /// The values the host's functions read as each transaction's snapshot
    /// saw them.
    values: RefCell<SnapshotValues>,
    /// Where the setup keeps the tables whose rows the host keeps in memory,
    /// once registered, for its connection to read.
    host_tables: &'a RefCell<Option<crate::HostTables>>,
}

impl<'a, P: Policy> Setup<'a, P> {
    pub(crate) fn new(
        connection: &'a rusqlite::Connection,
        authorization: &SqlAuthorization<P>,
        host_tables: &'a RefCell<Option<crate::HostTables>>,
    ) -> Self {
        Self {
            connection,
            functions: authorization.functions(),
            values: RefCell::new(SnapshotValues::new()),
            host_tables,
        }
    }

    /// The values registered, which the connection's transactions take.
    pub(crate) fn into_snapshot_values(self) -> SnapshotValues {
        self.values.into_inner()
    }

    /// Read `tables`, whose rows the host keeps in memory, on this
    /// connection, each as a virtual table of its name (`host_tables.rs`).
    /// Every connection to one database registers the same tables, once.
    pub fn host_tables(&self, tables: &crate::HostTables) -> rusqlite::Result<()> {
        let taken = self.snapshot_value(tables.now());
        crate::host_tables::register(tables, self.connection, &taken)?;
        let previous = self.host_tables.replace(Some(tables.clone()));
        assert!(previous.is_none(), "a connection registers its host tables once");
        Ok(())
    }

    /// Run `register` on `connection`, a connection a test opened itself,
    /// as the setup of a connection judged by `policy` would run it.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn on_test_connection(
        connection: &rusqlite::Connection,
        policy: P,
        register: impl FnOnce(&Setup<'_, P>) -> rusqlite::Result<()>,
    ) -> rusqlite::Result<()> {
        let authorization = SqlAuthorization::new(
            std::sync::Arc::new(crate::WholeViews::new([])),
            std::sync::Arc::new(policy),
        );
        register(&Setup::new(connection, &authorization, &RefCell::new(None)))
    }

    /// Register `function` as `name`. rusqlite turns a function's panic into
    /// an error that names no bug, so the authorizer catches the panic and
    /// records it as a bug: the statement fails with the bug's text, and the
    /// app's call fails with `Error::Bug` (E15).
    pub fn scalar_function<T: ToSql>(
        &self,
        name: &'static str,
        arguments: i32,
        flags: FunctionFlags,
        function: impl Fn(&FunctionCall<'_, P>) -> rusqlite::Result<T> + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection.create_scalar_function(
            name,
            arguments,
            flags,
            self.functions.function(name, function),
        )
    }

    /// Register `function` as `name`, a name only known as the connection
    /// opens. Nothing catches a panic in it: the function ends the process
    /// itself, or doesn't panic.
    pub fn named_function<T: ToSql>(
        &self,
        name: &str,
        arguments: i32,
        flags: FunctionFlags,
        function: impl Fn(&Context<'_>) -> rusqlite::Result<T> + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection
            .create_scalar_function(name, arguments, flags, move |context| function(context))
    }

    /// A value the host keeps in memory, which `now` reads, for this
    /// connection's functions to read as each transaction's snapshot saw it
    /// (`snapshot_values.rs`).
    pub fn snapshot_value<T: Clone + Send + 'static>(
        &self,
        now: impl Fn() -> T + Send + Sync + 'static,
    ) -> SnapshotValue<T> {
        self.values.borrow_mut().value(now)
    }

    /// Register `compare` as the order the collation `name` gives text.
    pub fn collation(
        &self,
        name: &str,
        compare: impl Fn(&str, &str) -> Ordering + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection.create_collation(name, compare)
    }

    /// The names of the functions registered so far, SQLite's own included,
    /// in lowercase.
    pub fn function_names(&self) -> rusqlite::Result<BTreeSet<String>> {
        self.connection
            .prepare("SELECT DISTINCT lower(name) FROM pragma_function_list")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect()
    }
}
