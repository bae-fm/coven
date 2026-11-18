//! The main schema's tables and views, as the scope planner looks them up
//! (§4): which object a name names, a table's columns, and a view's SELECT.

use rusqlite::Connection;
use sqlite3_parser::{
    ast::{Cmd, IndexedColumn, Name, Select, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// Every table of the main schema but SQLite's own, with its type: an
/// ordinary table, a virtual table, or the shadow table that holds a virtual
/// table's rows. A write can change each of them.
pub(crate) const MAIN_TABLES: &str = "SELECT name,type FROM pragma_table_list
     WHERE schema='main' AND type IN ('table','shadow','virtual') AND name NOT GLOB 'sqlite_*'";

/// The stored name and type, `table` or `view`, of the main schema's table
/// or view `name` names. Anything else, as a virtual table, is `None`.
pub(crate) fn main_object(
    connection: &Connection,
    name: &Name<'_>,
) -> rusqlite::Result<Option<(String, String)>> {
    let mut objects = connection.prepare_cached(
        "SELECT name,type FROM pragma_table_list WHERE schema='main' AND type IN ('table','view')",
    )?;
    let found = objects
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .find(|(object, _)| *name == object.as_str());
    Ok(found)
}

/// One column of a main schema table or view, as SQLite declares it.
pub(crate) struct DeclaredColumn {
    pub(crate) name: String,
    pub(crate) declared: String,
    pub(crate) hidden: bool,
}

/// The columns of the main schema's table or view `stored`, in order.
pub(crate) fn declared_columns(
    connection: &Connection,
    stored: &str,
) -> rusqlite::Result<Vec<DeclaredColumn>> {
    let mut metadata =
        connection.prepare_cached("SELECT name,type,hidden FROM pragma_table_xinfo(?1,'main')")?;
    let columns = metadata
        .query_map([stored], |row| {
            Ok(DeclaredColumn {
                name: row.get(0)?,
                declared: row.get(1)?,
                hidden: row.get::<_, i64>(2)? != 0,
            })
        })?
        .collect();
    columns
}

/// `read` run on the column names and the SELECT of the main schema's view
/// `stored`, as the parser reads its CREATE VIEW, or the parser's message
/// when it can't: SQLite and the parser can accept different language
/// versions.
pub(crate) fn read_view<T>(
    connection: &Connection,
    stored: &str,
    read: impl FnOnce(Option<&[IndexedColumn<'_>]>, &Select<'_>) -> rusqlite::Result<T>,
) -> rusqlite::Result<Result<T, String>> {
    let sql: String = connection.query_row(
        "SELECT sql FROM main.sqlite_schema WHERE type='view' AND name=?1",
        [stored],
        |row| row.get(0),
    )?;
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let result = match parser.next() {
        Ok(Some(Cmd::Stmt(Stmt::CreateView { columns, select, .. }))) => {
            read(columns, select).map(Ok)
        }
        Ok(_) => Ok(Err(format!("view {stored} holds no CREATE VIEW the parser reads"))),
        Err(error) => Ok(Err(format!("view {stored}: {error}"))),
    };
    result
}
