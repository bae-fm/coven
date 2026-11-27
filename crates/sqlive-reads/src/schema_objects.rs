//! The main schema's tables and views, as the scope planner looks them up
//! (SPEC.md section 8): which object a name names, a table's columns, and a
//! view's SELECT.

use std::collections::{BTreeSet, HashMap};

use crate::view_trees::ViewTrees;
use rusqlite::{ffi, Connection};
use sqlite3_parser::{
    ast::{
        Cmd, ColumnConstraint, CreateTableBody, Expr, IndexedColumn, Name, NamedColumnConstraint,
        Select, Stmt,
    },
    lexer::sql::Parser,
    Bump, FallibleIterator,
};

/// Every table of the main schema but SQLite's own, with its type: an
/// ordinary table, a virtual table, or the shadow table that holds a virtual
/// table's rows. A write can change each of them.
pub const MAIN_TABLES: &str = "SELECT name,type FROM pragma_table_list
     WHERE schema='main' AND type IN ('table','shadow','virtual') AND name NOT GLOB 'sqlite_*'";

/// The names SQLite gives a rowid table's rowid where no column has them.
pub(crate) const ROWID_NAMES: [&str; 3] = ["rowid", "oid", "_rowid_"];

/// Each contentful FTS5 table of the main schema, with the shadow table that
/// holds its rows: `<name>_content`, which keeps each row's rowid in `id` and
/// its columns in order in `c0`, `c1` and on, one column after `id` for each
/// of the table's own. A contentless table and one whose content is another
/// table have no such shadow table; one that keeps a locale for its values
/// has more columns in it.
pub const FTS5_CONTENT: &str = "SELECT v.name,c.name FROM pragma_table_list AS v
     JOIN pragma_table_list AS c ON c.schema='main' AND c.type='shadow'
       AND c.name=v.name||'_content'
     JOIN main.sqlite_schema AS s ON s.type='table' AND s.name=v.name
     WHERE v.schema='main' AND v.type='virtual' AND s.sql LIKE '% USING fts5%'
       AND c.ncol=(SELECT count(*) FROM pragma_table_xinfo(v.name,'main') WHERE hidden=0)+1";

/// The main schema as the scope planner looks it up, for one version of it:
/// the one the connection's schema cookie counted when `read` ran. Its holder
/// keeps it while the cookie stays, and reads another when the cookie changes.
///
/// `read` reads every table and view, with the SQL that made each, its
/// columns and its indexes, so no lookup asks SQLite again.
pub struct MainSchema {
    /// Each table and view, by its name, ASCII lowercased as SQLite compares
    /// names.
    objects: HashMap<String, MainObject>,
    /// The tables `MAIN_TABLES` lists, each by its stored name.
    main_tables: Vec<String>,
}

/// One table or view of the main schema.
struct MainObject {
    /// Its name as the schema stores it.
    stored: String,
    kind: ObjectKind,
    /// The SQL that made it, when `sqlite_schema` holds it.
    sql: Option<String>,
    /// Its columns, in order, or why SQLite can't list them, as for a view
    /// whose table was dropped, which only a lookup of those columns reports.
    columns: Result<Vec<DeclaredColumn>, Unlisted>,
}

enum ObjectKind {
    View,
    Table(MainTable),
}

/// What the schema holds of a table beyond what it holds of a view.
struct MainTable {
    form: TableForm,
    without_rowid: bool,
    indexes: Vec<DeclaredIndex>,
}

/// A table's type, as `pragma_table_list` gives it.
enum TableForm {
    Ordinary,
    /// A table that holds a virtual table's rows.
    Shadow,
    Virtual {
        /// Whether it is a contentful FTS5 table (`FTS5_CONTENT`).
        contentful: bool,
    },
}

/// SQLite's error for a table or view whose columns it can't list.
struct Unlisted {
    error: ffi::Error,
    message: Option<String>,
}

/// One column of a main schema table or view, as SQLite declares it.
pub(crate) struct DeclaredColumn {
    pub(crate) name: String,
    pub(crate) declared: String,
    pub(crate) hidden: bool,
    pub(crate) not_null: bool,
    pub(crate) primary_key: bool,
}

/// One index of a main schema table.
pub(crate) struct DeclaredIndex {
    /// Whether SQLite made it for the table's primary key.
    pub(crate) primary_key: bool,
    /// The columns of a unique index with no WHERE, each by its name with its
    /// collation, a name being `None` where the index is over an expression
    /// or the rowid; `None` for any other index.
    pub(crate) unique: Option<Vec<(Option<String>, String)>>,
}

impl MainSchema {
    /// The main schema as `connection` sees it now.
    pub fn read(connection: &Connection) -> rusqlite::Result<Self> {
        let contentful = connection
            .prepare_cached(FTS5_CONTENT)?
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        let mut made = connection
            .prepare_cached(
                "SELECT name,sql FROM main.sqlite_schema WHERE type IN ('table','view')",
            )?
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)))?
            .collect::<rusqlite::Result<HashMap<_, _>>>()?;
        let objects = connection
            .prepare_cached("SELECT name,type,wr FROM pragma_table_list WHERE schema='main'")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, bool>(2)?))
            })?
            .map(|listed| {
                let (stored, listed_type, without_rowid) = listed?;
                let columns = match declared_columns(connection, &stored) {
                    Ok(columns) => Ok(columns),
                    // SQLITE_ERROR says the schema's own SQL doesn't
                    // resolve; any other code is a failure of this read.
                    Err(rusqlite::Error::SqliteFailure(error, message))
                        if error.extended_code == ffi::SQLITE_ERROR =>
                    {
                        Err(Unlisted { error, message })
                    }
                    Err(error) => return Err(error),
                };
                let table = |form| -> rusqlite::Result<ObjectKind> {
                    let indexes = declared_indexes(connection, &stored)?;
                    Ok(ObjectKind::Table(MainTable { form, without_rowid, indexes }))
                };
                let kind = match listed_type.as_str() {
                    "view" => ObjectKind::View,
                    "table" => table(TableForm::Ordinary)?,
                    "shadow" => table(TableForm::Shadow)?,
                    "virtual" => {
                        table(TableForm::Virtual { contentful: contentful.contains(&stored) })?
                    }
                    other => unreachable!("pragma_table_list gives no type {other}"),
                };
                let object =
                    MainObject { columns, kind, sql: made.remove(&stored).flatten(), stored };
                Ok((object.stored.to_ascii_lowercase(), object))
            })
            .collect::<rusqlite::Result<_>>()?;
        let main_tables = connection
            .prepare_cached(MAIN_TABLES)?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(Self { objects, main_tables })
    }

    /// The table or view named `name`, in any ASCII letter case.
    fn named(&self, name: &str) -> Option<&MainObject> {
        self.objects.get(&name.to_ascii_lowercase())
    }

    /// The table or view named `name`, which the caller knows the schema
    /// holds.
    fn held(&self, name: &str) -> rusqlite::Result<&MainObject> {
        self.named(name).ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    /// The stored name and type of the object `name` names: `table` or
    /// `view`, or `virtual` for a contentful FTS5 table (`FTS5_CONTENT`),
    /// whose rows its content table holds. Anything else, as another virtual
    /// table, is `None`.
    pub(crate) fn object(&self, name: &Name<'_>) -> Option<(&str, &'static str)> {
        let object = self.named(&crate::sql::name(name.0))?;
        let kind = match &object.kind {
            ObjectKind::View => "view",
            ObjectKind::Table(table) => match table.form {
                TableForm::Ordinary => "table",
                TableForm::Virtual { contentful: true } => "virtual",
                TableForm::Virtual { contentful: false } | TableForm::Shadow => return None,
            },
        };
        Some((&object.stored, kind))
    }

    /// Every table of the main schema but SQLite's own (`MAIN_TABLES`).
    pub(crate) fn tables(&self) -> impl Iterator<Item = &str> {
        self.main_tables.iter().map(String::as_str)
    }

    /// Whether the schema holds a table or view named `name`.
    pub(crate) fn holds(&self, name: &str) -> bool {
        self.named(name).is_some()
    }

    /// The columns of the table or view `name`, in order.
    pub(crate) fn columns(&self, name: &str) -> rusqlite::Result<&[DeclaredColumn]> {
        match &self.held(name)?.columns {
            Ok(columns) => Ok(columns),
            Err(Unlisted { error, message }) => {
                Err(rusqlite::Error::SqliteFailure(*error, message.clone()))
            }
        }
    }

    /// The table `name`, which the caller knows is no view.
    fn table(&self, name: &str) -> rusqlite::Result<&MainTable> {
        match &self.held(name)?.kind {
            ObjectKind::Table(table) => Ok(table),
            ObjectKind::View => Err(rusqlite::Error::QueryReturnedNoRows),
        }
    }

    /// The indexes of the table `name`.
    pub(crate) fn indexes(&self, name: &str) -> rusqlite::Result<&[DeclaredIndex]> {
        Ok(&self.table(name)?.indexes)
    }

    /// Whether the table `name` is WITHOUT ROWID.
    pub(crate) fn without_rowid(&self, name: &str) -> rusqlite::Result<bool> {
        Ok(self.table(name)?.without_rowid)
    }

    /// The SQL that made the table or view `name`.
    pub(crate) fn sql(&self, name: &str) -> rusqlite::Result<&str> {
        self.held(name)?.sql.as_deref().ok_or(rusqlite::Error::QueryReturnedNoRows)
    }

    /// `read` run on the column names and the SELECT of the view `stored`,
    /// as the parser reads its CREATE VIEW, or the parser's message when it
    /// can't: SQLite and the parser can accept different language versions.
    pub(crate) fn read_view<T>(
        &self,
        stored: &str,
        read: impl FnOnce(Option<&[IndexedColumn<'_>]>, &Select<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<Result<T, String>> {
        let bump = Bump::new();
        let result = match ViewTrees::new(&bump).tree(self, stored)? {
            Ok(tree) => read(tree.columns, tree.select).map(Ok),
            Err(why) => Ok(Err(why)),
        };
        result
    }

    /// `read` run on the generated columns of the table `stored`, each by its
    /// name with its expression, as the parser reads its CREATE TABLE; or the
    /// parser's message when it can't read the CREATE TABLE.
    pub(crate) fn read_generated<T>(
        &self,
        stored: &str,
        read: impl FnOnce(&[(&Name<'_>, &Expr<'_>)]) -> T,
    ) -> rusqlite::Result<Result<T, String>> {
        let sql = self.sql(stored)?;
        let bump = Bump::new();
        let mut parser = Parser::new(&bump, sql.as_bytes());
        let result = match parser.next() {
            Ok(Some(Cmd::Stmt(Stmt::CreateTable {
                body: CreateTableBody::ColumnsAndConstraints { columns, .. },
                ..
            }))) => {
                let generated: Vec<(&Name<'_>, &Expr<'_>)> = columns
                    .iter()
                    .filter_map(|column| {
                        column.constraints.iter().find_map(
                            |NamedColumnConstraint { constraint, .. }| match constraint {
                                ColumnConstraint::Generated { expr, .. } => {
                                    Some((&column.col_name, expr))
                                }
                                _ => None,
                            },
                        )
                    })
                    .collect();
                Ok(read(&generated))
            }
            Ok(_) => Err(format!("table {stored} holds no CREATE TABLE the parser reads")),
            Err(error) => Err(format!("table {stored}: {error}")),
        };
        Ok(result)
    }
}

/// The columns SQLite gives `name`, a table, view or table function, in
/// order.
pub(crate) fn declared_columns(
    connection: &Connection,
    name: &str,
) -> rusqlite::Result<Vec<DeclaredColumn>> {
    let mut metadata = connection.prepare_cached(
        "SELECT name,type,hidden,\"notnull\",pk FROM pragma_table_xinfo(?1,'main')",
    )?;
    let columns = metadata
        .query_map([name], |row| {
            Ok(DeclaredColumn {
                name: row.get(0)?,
                declared: row.get(1)?,
                hidden: row.get::<_, i64>(2)? != 0,
                not_null: row.get(3)?,
                primary_key: row.get::<_, i64>(4)? > 0,
            })
        })?
        .collect();
    columns
}

/// The indexes of the table `table`.
fn declared_indexes(connection: &Connection, table: &str) -> rusqlite::Result<Vec<DeclaredIndex>> {
    let listed = connection
        .prepare_cached(
            "SELECT name, origin='pk', \"unique\" AND NOT partial
             FROM pragma_index_list(?1,'main')",
        )?
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?, row.get::<_, bool>(2)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    listed
        .into_iter()
        .map(|(index, primary_key, unique)| {
            let columns = || {
                connection
                    .prepare_cached(
                        "SELECT name, coll FROM pragma_index_xinfo(?1,'main')
                         WHERE key ORDER BY seqno",
                    )?
                    .query_map([&index], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()
            };
            Ok(DeclaredIndex { primary_key, unique: unique.then(columns).transpose()? })
        })
        .collect()
}

#[cfg(test)]
#[path = "schema_objects_tests.rs"]
mod tests;
