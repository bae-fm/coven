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
use crate::result_nulls::{statement_nulls, ColumnNulls, SelectNull, StatementNulls};
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

/// Where a result column of a statement comes from, when it is a column of a
/// table or of a table function.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnOrigin {
    /// The table, as the schema names it, or the table function, as SQLite
    /// names it.
    pub table: String,
    /// The column, as SQLite names it.
    pub column: String,
    /// The type the schema's table declares for the column, as written, when
    /// it declares one. A table function's column has none here.
    pub declared_type: Option<String>,
}

/// One result column of a prepared statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbedColumn {
    /// The name SQLite gives the column.
    pub name: String,
    /// The column of a table or table function that the result is, when it is
    /// one unchanged.
    pub origin: Option<ColumnOrigin>,
    /// Where the statement can give NULL in the column, or `None` when the
    /// probe can't read the statement, or can't tell which columns a SELECT
    /// gives: it knows then neither where the column can be NULL nor how many
    /// SELECTs give it values.
    pub nulls: Option<ColumnNulls>,
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

/// A column of a table that an INSERT without a column list fills.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableColumn {
    /// The column's name, as the table declares it.
    pub name: String,
    /// Whether the table declares the column `NOT NULL`.
    pub not_null: bool,
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
        let statement_nulls = statement_nulls(&self.connection, sql, statement.column_count())?;
        let mut columns = Vec::new();
        for (position, metadata) in statement.columns_with_metadata().enumerate() {
            let origin =
                match (metadata.database_name(), metadata.table_name(), metadata.origin_name()) {
                    (Some("main"), Some(table), Some(column)) => Some(ColumnOrigin {
                        table: table.to_owned(),
                        column: column.to_owned(),
                        declared_type: schema_column(&self.connection, table, column)?
                            .and_then(|column| column.declared_type),
                    }),
                    _ => None,
                };
            let nulls = match &statement_nulls {
                StatementNulls::Select(columns) => Some(columns[position].clone()),
                StatementNulls::Write(subqueries) => {
                    // SQLite names a subquery's column as the origin of the
                    // subquery, which gives NULL with no row.
                    let null = match subqueries[position] {
                        Some(null) => null,
                        None => match &origin {
                            Some(origin)
                                if table_column_never_null(
                                    &self.connection,
                                    &origin.table,
                                    &origin.column,
                                )? =>
                            {
                                SelectNull::Never
                            }
                            _ => SelectNull::Value,
                        },
                    };
                    Some(ColumnNulls { selects: vec![null], row_can_be_missing: false })
                }
                StatementNulls::Unknown => None,
            };
            columns.push(ProbedColumn { name: metadata.name().to_owned(), origin, nulls });
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

    /// The columns of `table` in the order of an INSERT with no column list,
    /// which leaves out the hidden and generated columns. A view's columns are
    /// never `NOT NULL`. A name that no table or view has gives no columns.
    pub fn table_columns(&self, table: &str) -> rusqlite::Result<Vec<TableColumn>> {
        let quoted = table.replace('"', "\"\"");
        let mut statement = self.connection.prepare(&format!("PRAGMA table_info(\"{quoted}\")"))?;
        let columns = statement.query_map([], |row| {
            Ok(TableColumn { name: row.get("name")?, not_null: row.get("notnull")? })
        })?;
        columns.collect()
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

/// What the main schema declares of a table's column.
struct SchemaColumn {
    declared_type: Option<String>,
    not_null: bool,
    primary_key: bool,
}

/// What the main schema declares of the column `column` of `table`, which may
/// be one of the rowid's names, or `None` when the schema holds no such table:
/// SQLite names a table function and its column as a column's origin too.
fn schema_column(
    connection: &Connection,
    table: &str,
    column: &str,
) -> rusqlite::Result<Option<SchemaColumn>> {
    if !connection.table_exists(Some("main"), table)? {
        return Ok(None);
    }
    let (declared, _, not_null, primary_key, _) =
        connection.column_metadata(Some("main"), table, column)?;
    let declared_type = declared.map(|declared| declared.to_string_lossy().into_owned());
    Ok(Some(SchemaColumn { declared_type, not_null, primary_key }))
}

/// Whether no row of `table` holds NULL in its column `column`, which may be
/// one of the rowid's names: the main schema's table declares it `NOT NULL`,
/// or it is the rowid. A table function's column can hold NULL.
pub(crate) fn table_column_never_null(
    connection: &Connection,
    table: &str,
    column: &str,
) -> rusqlite::Result<bool> {
    let Some(declared) = schema_column(connection, table, column)? else {
        return Ok(false);
    };
    Ok(declared.not_null || (declared.primary_key && is_rowid(connection, table, column)?))
}

/// Whether `column`, which SQLite says is part of the primary key of
/// `table`, is the table's rowid. SQLite says that of a name of the rowid
/// that no column of the table has. It gives every primary key an index,
/// except an `INTEGER PRIMARY KEY` that is the rowid itself.
fn is_rowid(connection: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(?1, 'main') WHERE name = ?2)
             OR NOT EXISTS (SELECT 1 FROM pragma_index_list(?1, 'main') WHERE origin = 'pk')",
        [table, column],
        |row| row.get(0),
    )
}

#[cfg(test)]
#[path = "schema_probe_tests.rs"]
mod tests;
