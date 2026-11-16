//! What the host registers on a connection as it opens, before any statement
//! runs: SQL functions and collations.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ToSql;

use crate::authorization::{FunctionCall, SqlAuthorization};

/// What a connection's setup may register.
pub(crate) struct Setup<'a> {
    connection: &'a rusqlite::Connection,
    authorization: &'a SqlAuthorization,
}

impl<'a> Setup<'a> {
    pub(crate) fn new(
        connection: &'a rusqlite::Connection,
        authorization: &'a SqlAuthorization,
    ) -> Self {
        Self { connection, authorization }
    }

    /// Register `function` as `name`. rusqlite turns a function's panic into
    /// an error that names no bug, so the authorizer keeps it: the statement
    /// fails with the bug's text, and the app's call with `DbError::Bug`
    /// (E15).
    pub(crate) fn scalar_function<T: ToSql>(
        &self,
        name: &'static str,
        arguments: i32,
        flags: FunctionFlags,
        function: impl Fn(&FunctionCall<'_>) -> rusqlite::Result<T> + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection.create_scalar_function(
            name,
            arguments,
            flags,
            self.authorization.function(name, function),
        )
    }

    /// Register `function` as `name`, a name only known as the connection
    /// opens. Nothing catches a panic in it: the function ends the process
    /// itself, or doesn't panic.
    pub(crate) fn named_function<T: ToSql>(
        &self,
        name: &str,
        arguments: i32,
        flags: FunctionFlags,
        function: impl Fn(&Context<'_>) -> rusqlite::Result<T> + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection
            .create_scalar_function(name, arguments, flags, move |context| function(context))
    }

    /// Register `compare` as the order the collation `name` gives text.
    pub(crate) fn collation(
        &self,
        name: &str,
        compare: impl Fn(&str, &str) -> Ordering + Send + 'static,
    ) -> rusqlite::Result<()> {
        self.connection.create_collation(name, compare)
    }

    /// The names of the functions registered so far, SQLite's own included,
    /// in lowercase.
    pub(crate) fn function_names(&self) -> rusqlite::Result<BTreeSet<String>> {
        self.connection
            .prepare("SELECT DISTINCT lower(name) FROM pragma_function_list")?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect()
    }
}
