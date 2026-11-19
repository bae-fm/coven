//! A FROM item of a statement's core, as the scope planner reads it: a
//! table, a table function, a view read whole, or a subquery, CTE or app
//! view read as cores of its own (§4).

use crate::derived::{read_derived_item, Ctes, Derived};
use crate::schema_objects::{declared_columns, main_object, read_generated, DeclaredColumn};
use crate::scope_plan::{Planner, Source};
use crate::scope_row::{row_term, RowExpression};
use crate::scope_term::Affinity;
use crate::select_syntax::{parts, text, Functions, Part};
use rusqlite::Connection;
use sqlite3_parser::ast::{As, Expr, Name, QualifiedName, SelectTable};

/// One of a FROM item's columns: its name, lowercase, when sqlive knows the
/// name SQLite gives it, and what sqlive knows of its values.
pub(crate) struct Column {
    pub(crate) name: Option<String>,
    pub(crate) value: ColumnValue,
}

/// What sqlive knows of a column's values.
pub(crate) enum ColumnValue {
    /// Values sqlive compares as SQLite does, of this affinity, so a
    /// condition on the column narrows: a stored column whose collation is
    /// BINARY, or a derived item's column that passes such columns on.
    Compared(Affinity),
    /// A generated column's values, which its expression gives: one whose
    /// collation is BINARY and whose expression names only its table's
    /// stored columns of BINARY collation (§4).
    Generated(RowExpression),
    /// Values no condition narrows by: a column whose collation isn't
    /// BINARY, a generated column sqlive can't evaluate, or a column of a
    /// view read whole.
    Unknown,
}

/// A FROM item's columns.
pub(crate) enum Columns {
    /// Every column, in order.
    Listed(Vec<Column>),
    /// A table function's, which sqlive doesn't know.
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
            Self::Unknown => Some(crate::sql::name(name.0).to_ascii_lowercase()),
        }
    }

    /// What sqlive knows of the values of `column`.
    pub(crate) fn value(&self, column: &str) -> Option<&ColumnValue> {
        match self {
            Self::Listed(columns) => {
                Some(&columns.iter().find(|listed| listed.name.as_deref() == Some(column))?.value)
            }
            Self::Unknown => None,
        }
    }

    /// The affinity a condition on `column` narrows by.
    pub(crate) fn affinity(&self, column: &str) -> Option<Affinity> {
        match self.value(column)? {
            ColumnValue::Compared(affinity) => Some(*affinity),
            ColumnValue::Generated(_) | ColumnValue::Unknown => None,
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
    /// For the right side of a LEFT JOIN that no condition outside its ON
    /// narrows yet, what its ON says of the other sources, which narrows
    /// them once one does and so makes it an inner join's.
    pub(crate) left_on: Option<crate::scope_plan::Narrowing>,
    /// Whether a LEFT JOIN adds it, whatever makes the join inner later: a
    /// left row no right row joins gives NULLs in its place with no row of
    /// it, which no lookup finds.
    pub(crate) left: bool,
    /// The SELECT core whose FROM holds it.
    pub(crate) core: usize,
    /// A subquery's, CTE's or view's cores, which conditions on its rows
    /// narrow.
    pub(crate) derived: Option<Derived>,
    /// The columns, lowercase, a USING join compares with an item to its
    /// left, which a bare name and a `*` take from that item instead.
    pub(crate) using: Vec<String>,
    /// The CTE it reads, by its number (`derived::Declared`).
    pub(crate) cte: Option<usize>,
    /// The sides of each OR that narrows it (`scope_or.rs`).
    pub(crate) ors: Vec<Vec<crate::scope_or::Side>>,
}

impl FromItem {
    /// Whether every binding of its core holds a row of it: no LEFT JOIN
    /// adds it. A lookup finds every row such an item gives a binding, and
    /// none for the NULLs a LEFT JOIN's missing row gives.
    pub(crate) fn always_bound(&self) -> bool {
        !self.left
    }
}

/// The FROM item `table` of `core`, which sits in `parent`, where its name
/// can name `ctes`; `None` when sqlive can't read its rows.
pub(crate) fn read_item<'a>(
    connection: &Connection,
    planner: &mut Planner,
    table: &'a SelectTable<'a>,
    left: bool,
    core: usize,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Option<FromItem>> {
    let qualifier = |alias: &Option<As<'_>>, name: &Name<'_>| match alias {
        Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
        None => name.0.to_owned(),
    };
    let item = |source, columns, derived| FromItem {
        source,
        columns,
        left_on: left.then(crate::scope_plan::Narrowing::default),
        left,
        core,
        derived,
        using: Vec::new(),
        cte: None,
        ors: Vec::new(),
    };
    match table {
        SelectTable::Table(name, alias, _) => {
            let qualifier = qualifier(alias, &name.name);
            if name.db_name.is_none() {
                if let Some(declared) = ctes.named(&name.name) {
                    return read_derived_item(
                        connection,
                        planner,
                        crate::derived::Named::Cte(declared),
                        qualifier,
                        parent,
                        ctes,
                    )
                    .map(|found| {
                        found.map(|(source, columns, derived)| FromItem {
                            cte: Some(declared.number),
                            ..item(source, columns, Some(derived))
                        })
                    });
                }
            }
            if !in_main(name) {
                return Ok(None);
            }
            let Some((stored, kind)) = main_object(connection, &name.name)? else {
                return Ok(None);
            };
            let ordinary = kind == "table";
            if !ordinary && !planner.whole.contains(&stored) {
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
                // A view sqlive can't read as cores of its own, as one with a
                // recursive CTE, is read for its values alone, and its tables
                // whole, as a view read whole is.
                planner.items.truncate(read.0);
                planner.cores.truncate(read.1);
                planner.edges.truncate(read.2);
                planner.inlined.truncate(read.3);
            }
            // An ordinary table, or a view read only for its values: a view
            // read whole can show what the host's memory holds as well as
            // its tables.
            let columns = columns(connection, planner.functions, &qualifier, &stored, ordinary)?;
            Ok(Some(item(
                Source {
                    table: ordinary.then(|| stored.to_ascii_lowercase()),
                    from: Some(format!("main.{} AS {qualifier}", crate::sql::identifier(&stored))),
                    qualifier,
                    term: None,
                    carried: None,
                    either: Vec::new(),
                    memberships: Vec::new(),
                },
                columns,
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
            let alone = found.iter().all(|part| matches!(part, Part::Function(..) | Part::Clock));
            let from = alone.then(|| match alias {
                Some(_) => text(table),
                None => format!("{} AS {qualifier}", text(table)),
            });
            Ok(Some(item(
                Source {
                    table: None,
                    from,
                    qualifier,
                    term: None,
                    carried: None,
                    either: Vec::new(),
                    memberships: Vec::new(),
                },
                Columns::Unknown,
                None,
            )))
        }
        SelectTable::Select(select, alias) => {
            let qualifier = match alias {
                Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
                // Nothing can name it; this name is sqlive's own.
                None => crate::sql::identifier("(subquery)"),
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

/// A table's or a view's columns, in order, `ordinary` for a table, which the
/// statement names by `qualifier`. A view's comparisons happen inside SQLite,
/// so sqlive gives none of them an affinity to narrow by. The connection's
/// `functions` tell whether a generated column's calls repeat.
pub(crate) fn columns(
    connection: &Connection,
    functions: &Functions,
    qualifier: &str,
    table: &str,
    ordinary: bool,
) -> rusqlite::Result<Columns> {
    let mut columns = Vec::new();
    // A table's hidden columns are its generated ones.
    let mut generated = Vec::new();
    for DeclaredColumn { name: column, declared, hidden } in declared_columns(connection, table)? {
        let binary = ordinary && {
            let (_, collation, _, _, _) =
                connection.column_metadata(Some("main"), table, column.as_str())?;
            collation.is_some_and(|collation| collation.to_bytes().eq_ignore_ascii_case(b"BINARY"))
        };
        let affinity = Affinity::declared(&declared);
        if binary && hidden {
            generated.push((columns.len(), affinity));
        }
        let value =
            if binary && !hidden { ColumnValue::Compared(affinity) } else { ColumnValue::Unknown };
        columns.push(Column { name: Some(column.to_ascii_lowercase()), value });
    }
    if generated.is_empty() {
        return Ok(Columns::Listed(columns));
    }
    let read = read_generated(connection, table, |expressions| {
        let mut found = Vec::new();
        for (position, affinity) in generated {
            let name = columns[position].name.as_deref().expect("a table's columns are named");
            let expression = expressions.iter().find(|(named, _)| **named == name);
            if let Some(evaluated) = expression
                .and_then(|(_, expr)| evaluated(functions, qualifier, &columns, expr, affinity))
            {
                found.push((position, evaluated));
            }
        }
        found
    })?;
    let found = match read {
        Ok(found) => found,
        // A table the parser can't read keeps its generated columns unknown.
        Err(why) => {
            tracing::debug!(table, %why, "sqlive can't read a table's generated columns");
            Vec::new()
        }
    };
    for (position, generated) in found {
        columns[position].value = ColumnValue::Generated(generated);
    }
    Ok(Columns::Listed(columns))
}

/// The value of the generated column of affinity `affinity` that `expr`
/// gives, among the table's `columns`, when the expression names only
/// columns sqlive compares and holds no COLLATE, which would give a
/// comparison of its value another collation. SQLite allows a generated
/// column's expression only columns of its own row, by bare names.
fn evaluated(
    functions: &Functions,
    qualifier: &str,
    columns: &[Column],
    expr: &Expr<'_>,
    affinity: Affinity,
) -> Option<RowExpression> {
    let mut found = Vec::new();
    parts(expr, &mut found);
    if found.iter().any(|part| matches!(part, Part::Collation)) {
        return None;
    }
    let term = row_term(functions, qualifier, expr, &found, &|name| {
        let name = match name {
            Expr::Id(name) => Name(name.0),
            Expr::Name(name) => name.clone(),
            _ => return None,
        };
        let column = columns
            .iter()
            .find(|column| column.name.as_deref().is_some_and(|column| name == column))?;
        match column.value {
            ColumnValue::Compared(compared) => Some((column.name.clone()?, compared)),
            ColumnValue::Generated(_) | ColumnValue::Unknown => None,
        }
    })?;
    Some(RowExpression { term, affinity })
}
