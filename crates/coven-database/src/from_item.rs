//! A FROM item of a statement's core, as the scope planner reads it: a
//! table, a table function, one of coven's views, or a subquery, CTE or
//! app view read as cores of its own (§4).

use crate::derived::{read_derived_item, Ctes, Derived};
use crate::scope_plan::{unquoted, Planner, Source};
use crate::scope_term::Affinity;
use crate::select_syntax::{parts, quoted, text, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{As, Name, QualifiedName, SelectTable};

/// One of a FROM item's columns: its name, lowercase, when coven knows the
/// name SQLite gives it, and its affinity, or `None` when a condition on it
/// can't narrow, as a generated column's, one whose collation isn't BINARY,
/// or one of coven's views'.
pub(crate) struct Column {
    pub(crate) name: Option<String>,
    pub(crate) affinity: Option<Affinity>,
}

/// A FROM item's columns.
pub(crate) enum Columns {
    /// Every column, in order.
    Listed(Vec<Column>),
    /// A table function's, which coven doesn't know.
    Unknown,
}

impl Columns {
    /// The column `name` names, lowercase: the first listed of that name,
    /// or the name itself when the columns aren't known.
    pub(crate) fn named(&self, name: &Name<'_>) -> Option<String> {
        match self {
            Self::Listed(columns) => columns
                .iter()
                .filter_map(|column| column.name.as_ref())
                .find(|column| *name == column.as_str())
                .cloned(),
            Self::Unknown => Some(unquoted(name.0).to_ascii_lowercase()),
        }
    }

    /// The affinity a condition on `column` narrows by.
    pub(crate) fn affinity(&self, column: &str) -> Option<Affinity> {
        match self {
            Self::Listed(columns) => {
                columns.iter().find(|listed| listed.name.as_deref() == Some(column))?.affinity
            }
            Self::Unknown => None,
        }
    }

    /// Whether a bare name none of them has is surely none of its columns.
    pub(crate) fn complete(&self) -> bool {
        match self {
            Self::Listed(columns) => columns.iter().all(|column| column.name.is_some()),
            Self::Unknown => false,
        }
    }
}

/// What the planner knows of a FROM item while it reads the conditions.
pub(crate) struct FromItem {
    pub(crate) source: Source,
    pub(crate) columns: Columns,
    /// The right side of a LEFT JOIN: only its own ON conditions narrow it.
    pub(crate) nullable: bool,
    /// The SELECT core whose FROM holds it.
    pub(crate) core: usize,
    /// A subquery's, CTE's or view's cores, which conditions on its rows
    /// narrow.
    pub(crate) derived: Option<Derived>,
}

/// The FROM item `table` of `core`, which sits in `parent`, where its name
/// can name `ctes`; `None` when coven can't read its rows.
pub(crate) fn read_item<'a>(
    connection: &Connection,
    planner: &mut Planner,
    table: &'a SelectTable<'a>,
    nullable: bool,
    core: usize,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<FromItem>> {
    let qualifier = |alias: &Option<As<'_>>, name: &Name<'_>| match alias {
        Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
        None => name.0.to_owned(),
    };
    let item = |source, columns, derived| FromItem { source, columns, nullable, core, derived };
    match table {
        SelectTable::Table(name, alias, _) => {
            let qualifier = qualifier(alias, &name.name);
            if name.db_name.is_none() {
                if let Some(cte) = ctes.named(&name.name) {
                    return read_derived_item(
                        connection,
                        planner,
                        crate::derived::Named::Cte(cte),
                        qualifier,
                        parent,
                        ctes,
                    )
                    .map(|found| {
                        found.map(|(source, columns, derived)| item(source, columns, Some(derived)))
                    });
                }
            }
            if !in_main(name) {
                return Ok(None);
            }
            let Some((stored, kind)) = object(connection, &name.name)? else {
                return Ok(None);
            };
            let ordinary = kind == "table";
            if !ordinary && !crate::internal_schema::app_readable(&stored) {
                let read = (
                    planner.items.len(),
                    planner.cores.len(),
                    planner.edges.len(),
                    planner.inlined.len(),
                );
                if let Some((source, columns, derived)) = read_derived_item(
                    connection,
                    planner,
                    crate::derived::Named::View(&stored),
                    qualifier.clone(),
                    parent,
                    ctes,
                )? {
                    planner.inlined.push(stored.to_ascii_lowercase());
                    return Ok(Some(item(source, columns, Some(derived))));
                }
                // A view coven can't read as cores of its own, as one with a
                // recursive CTE, is read for its values alone, and its tables
                // whole, as one of coven's are.
                planner.items.truncate(read.0);
                planner.cores.truncate(read.1);
                planner.edges.truncate(read.2);
                planner.inlined.truncate(read.3);
            }
            // An ordinary table, or a view read only for its values: one of
            // coven's shows what memory holds as well as its tables.
            Ok(Some(item(
                Source {
                    table: ordinary.then(|| stored.to_ascii_lowercase()),
                    from: Some(format!("main.{} AS {qualifier}", quoted(&stored))),
                    qualifier,
                    term: None,
                    memberships: Vec::new(),
                },
                columns(connection, &stored, ordinary)?,
                None,
            )))
        }
        SelectTable::TableCall(name, args, alias) => {
            if !in_main(name) {
                return Ok(None);
            }
            let qualifier = qualifier(alias, &name.name);
            let mut found = Vec::new();
            for arg in args.iter().flat_map(|args| args.iter()) {
                parts(arg, &mut found);
            }
            let alone = found.iter().all(|part| matches!(part, Part::Function(..)));
            let from = alone.then(|| match alias {
                Some(_) => text(table),
                None => format!("{} AS {qualifier}", text(table)),
            });
            Ok(Some(item(
                Source { table: None, from, qualifier, term: None, memberships: Vec::new() },
                Columns::Unknown,
                None,
            )))
        }
        SelectTable::Select(select, alias) => {
            let qualifier = match alias {
                Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
                // Nothing can name it; this name is coven's own.
                None => quoted("(subquery)"),
            };
            read_derived_item(
                connection,
                planner,
                crate::derived::Named::Subquery(select),
                qualifier,
                parent,
                ctes,
            )
            .map(|found| {
                found.map(|(source, columns, derived)| item(source, columns, Some(derived)))
            })
        }
        SelectTable::Sub(..) => Ok(None),
    }
}

fn in_main(name: &QualifiedName<'_>) -> bool {
    name.db_name.as_ref().is_none_or(|name| *name == "main")
}

/// The stored name and type of the table or view `name` names. Anything
/// else, as a virtual table, keeps what it reads whole.
fn object(connection: &Connection, name: &Name<'_>) -> rusqlite::Result<Option<(String, String)>> {
    let mut objects = connection.prepare_cached(
        "SELECT name,type FROM pragma_table_list WHERE schema='main' AND type IN ('table','view')",
    )?;
    let found = objects
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .find(|(table, _)| *name == table.as_str());
    Ok(found)
}

/// A table's or one of coven's views' columns, in order. A view's
/// comparisons happen inside SQLite, so coven gives none of them an
/// affinity to narrow by.
fn columns(connection: &Connection, table: &str, ordinary: bool) -> rusqlite::Result<Columns> {
    let mut metadata =
        connection.prepare_cached("SELECT name,type,hidden FROM pragma_table_xinfo(?1,'main')")?;
    let declared = metadata
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut columns = Vec::new();
    for (column, declared, hidden) in declared {
        let affinity = if ordinary && !hidden {
            let (_, collation, _, _, _) =
                connection.column_metadata(Some("main"), table, column.as_str())?;
            collation
                .is_some_and(|collation| collation.to_bytes().eq_ignore_ascii_case(b"BINARY"))
                .then(|| Affinity::declared(&declared))
        } else {
            None
        };
        columns.push(Column { name: Some(column.to_ascii_lowercase()), affinity });
    }
    Ok(Columns::Listed(columns))
}
