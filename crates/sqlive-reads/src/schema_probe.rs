//! A database in memory that holds a schema and no rows, on which a build
//! tool prepares SQL and asks what SQLite and sqlive make of it: the columns
//! a statement returns and where each comes from, the parameters it reads,
//! and what a live query of it depends on (SPEC.md section 12). The compiler of
//! sqlive-compose is the tool that uses it, so that no crate but sqlive
//! opens SQLite.

use std::collections::BTreeSet;

use rusqlite::functions::FunctionFlags;
use rusqlite::Connection;

use crate::read_capture::WholeViews;
use crate::read_report::ReadReport;
use crate::scope_plan::ScopePlan;

/// A SQL function the schema's SQL names, which the probe registers so that
/// SQLite can prepare a statement that calls it. It returns NULL when called.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandInFunction {
    /// The function's name.
    pub name: String,
    /// How many arguments it takes, or -1 for any number.
    pub arguments: i32,
    /// Whether it gives the same result for the same arguments, as SQLite's
    /// planner assumes of an index expression's functions.
    pub deterministic: bool,
}

/// Where a result column of a statement comes from in a table, when it is
/// one of the table's columns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnOrigin {
    /// The table, as the schema names it.
    pub table: String,
    /// The type the table declares for the column, as written, when it
    /// declares one.
    pub declared_type: Option<String>,
    /// Whether the table declares the column `NOT NULL`.
    pub not_null: bool,
}

/// One result column of a prepared statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbedColumn {
    /// The name SQLite gives the column.
    pub name: String,
    /// The table column the result is, when it is one unchanged.
    pub origin: Option<ColumnOrigin>,
}

/// What SQLite made of a statement it prepared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbedStatement {
    /// The result columns, in order.
    pub columns: Vec<ProbedColumn>,
    /// The names of the parameters the statement reads, with their first
    /// character (`:name`), each once, in order of first use.
    pub parameters: Vec<String>,
    /// How many parameters the statement reads that have no name: `?` alone.
    pub unnamed_parameters: usize,
    /// Whether running the statement can change the database, as SQLite's
    /// `sqlite3_stmt_readonly` says: false for an INSERT, UPDATE, DELETE or
    /// REPLACE, and for a statement that changes the schema.
    pub writes: bool,
}

/// An in-memory database that holds a schema's tables, views and triggers.
pub struct SchemaProbe {
    connection: Connection,
}

impl SchemaProbe {
    /// A database that has run `schema`, a list of SQL texts, in order, after
    /// `functions` and `collations` were registered under the names the
    /// schema uses: SQLite refuses to prepare SQL that names a function or a
    /// collation it doesn't know. A collation compares text byte by byte.
    pub fn open(
        schema: &[String],
        functions: &[StandInFunction],
        collations: &[String],
    ) -> rusqlite::Result<Self> {
        let connection = Connection::open_in_memory()?;
        for function in functions {
            let mut flags = FunctionFlags::SQLITE_UTF8;
            if function.deterministic {
                flags |= FunctionFlags::SQLITE_DETERMINISTIC;
            }
            connection.create_scalar_function(
                function.name.as_str(),
                function.arguments,
                flags,
                |_| Ok(None::<i64>),
            )?;
        }
        for name in collations {
            connection.create_collation(name.as_str(), |left, right| left.cmp(right))?;
        }
        for sql in schema {
            connection.execute_batch(sql)?;
        }
        Ok(Self { connection })
    }

    /// Prepare `sql` and describe it. SQLite's own error says what it can't
    /// find.
    pub fn prepare(&self, sql: &str) -> rusqlite::Result<ProbedStatement> {
        let sql = crate::sql::up_to_nul(sql);
        let statement = self.connection.prepare(sql)?;
        let mut columns = Vec::new();
        for (position, metadata) in statement.columns_with_metadata().enumerate() {
            let origin = match statement.column_metadata(position)? {
                Some((_, table, _, declared, _, not_null, _, _))
                    if metadata.database_name() == Some("main") =>
                {
                    Some(ColumnOrigin {
                        table: table.to_string_lossy().into_owned(),
                        declared_type: declared
                            .map(|declared| declared.to_string_lossy().into_owned()),
                        not_null,
                    })
                }
                _ => None,
            };
            columns.push(ProbedColumn { name: metadata.name().to_owned(), origin });
        }
        let mut seen = BTreeSet::new();
        let mut parameters = Vec::new();
        let mut unnamed_parameters = 0;
        for position in 1..=statement.parameter_count() {
            match statement.parameter_name(position) {
                Some(name) => {
                    if seen.insert(name.to_owned()) {
                        parameters.push(name.to_owned());
                    }
                }
                None => unnamed_parameters += 1,
            }
        }
        Ok(ProbedStatement {
            columns,
            parameters,
            unnamed_parameters,
            writes: !statement.readonly(),
        })
    }

    /// What a live query of `sql` depends on in each table it reads, as its
    /// plan narrows them before a run binds its values (SPEC.md section 12). A
    /// view of the schema counts as sqlive reads it when no host names it a
    /// view to read whole.
    pub fn read_report(&self, sql: &str) -> rusqlite::Result<ReadReport> {
        let sql = crate::sql::up_to_nul(sql);
        let statement = self.connection.prepare(sql)?;
        let plan = ScopePlan::build(&self.connection, sql, &statement, &WholeViews::new([]))?;
        Ok(ReadReport::of(&plan))
    }
}

#[cfg(test)]
#[path = "schema_probe_tests.rs"]
mod tests;
