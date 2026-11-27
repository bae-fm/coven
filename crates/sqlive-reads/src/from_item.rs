//! A FROM item of a statement's core, as the scope planner reads it: a table, a
//! contentful FTS5 table, a table function, a view read whole, or a subquery,
//! CTE or app view read as cores of its own (SPEC.md section 8); and a core's
//! FROM, whose parenthesized joins' tables are items of the core too.

use crate::derived::{read_derived_item, Ctes, Derived};
use crate::read_report::ReadPlace;
use crate::schema_objects::{DeclaredColumn, SchemaLookups};
use crate::scope_plan::{is_name, Object, Place, Planner, Source, Unread, Written};
use crate::scope_row::{row_term, RowExpression};
use crate::scope_term::Affinity;
use crate::select_syntax::{conjuncts, parts, text, Functions, Part};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{
    As, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, Name, Operator, QualifiedName,
    SelectTable,
};

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
    /// stored columns of BINARY collation (SPEC.md section 6).
    Generated(RowExpression),
    /// A stored column whose collation isn't BINARY, of this affinity and
    /// the collation it declares, or a derived item's column that passes one
    /// on, or passes on a generated column, whose collation is BINARY. No
    /// condition narrows by it, but an ORDER BY term that is the column
    /// bounds a LIMIT's rows, by a comparison SQLite runs in the term's
    /// collation (SPEC.md section 6).
    Collated(Affinity, String),
    /// Values no condition narrows by: a generated column sqlive can't
    /// evaluate, or a column of a view read whole.
    Unknown,
    /// A derived item's column whose expression gives another answer when
    /// evaluated again, as one that reads the clock: a lookup that reads it
    /// again at a write finds no row of the query's.
    Volatile,
    /// A contentful FTS5 table's hidden column, the one that bears the
    /// table's name or `rank`, whose values depend on rows besides its own:
    /// a statement that names one narrows nothing (`Planner::names_hidden`).
    Hidden,
}

/// A FROM item's columns.
pub(crate) enum Columns {
    /// Every column, in order.
    Listed(Vec<Column>),
    /// A table function's, or a SELECT's whose `*` spans one, which sqlive
    /// doesn't know. `volatile` says whether a SELECT's volatile
    /// (`Planner::select_volatile`), so each of its columns may be; a table
    /// function's is always false, whatever its arguments read.
    Unknown { volatile: bool },
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
            Self::Unknown { .. } => Some(crate::sql::name(name.0).to_ascii_lowercase()),
        }
    }

    /// The names of the columns sqlive knows them of, lowercase.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        let listed = match self {
            Self::Listed(columns) => columns.as_slice(),
            Self::Unknown { .. } => &[],
        };
        listed.iter().filter_map(|column| column.name.as_deref())
    }

    /// What sqlive knows of the values of `column`.
    pub(crate) fn value(&self, column: &str) -> Option<&ColumnValue> {
        match self {
            Self::Listed(columns) => {
                Some(&columns.iter().find(|listed| listed.name.as_deref() == Some(column))?.value)
            }
            Self::Unknown { .. } => None,
        }
    }

    /// Whether the values of `column` come from an expression that gives
    /// another answer when a write's lookup evaluates it again, or may.
    pub(crate) fn volatile(&self, column: &str) -> bool {
        match self {
            Self::Listed(_) => matches!(self.value(column), Some(ColumnValue::Volatile)),
            Self::Unknown { volatile } => *volatile,
        }
    }

    /// Whether any column is volatile (`volatile`).
    pub(crate) fn any_volatile(&self) -> bool {
        match self {
            Self::Listed(columns) => {
                columns.iter().any(|column| matches!(column.value, ColumnValue::Volatile))
            }
            Self::Unknown { volatile } => *volatile,
        }
    }

    /// The affinity a condition on `column` narrows by.
    pub(crate) fn affinity(&self, column: &str) -> Option<Affinity> {
        match self.value(column)? {
            ColumnValue::Compared(affinity) => Some(*affinity),
            ColumnValue::Generated(_)
            | ColumnValue::Collated(..)
            | ColumnValue::Unknown
            | ColumnValue::Volatile
            | ColumnValue::Hidden => None,
        }
    }

    /// Whether a bare name none of them has is surely none of its columns.
    pub(crate) fn complete(&self) -> bool {
        match self {
            Self::Listed(columns) => columns.iter().all(|column| column.name.is_some()),
            Self::Unknown { .. } => false,
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
    pub(crate) left_on: Option<crate::scope_narrowing::Narrowing>,
    /// Whether a parenthesized join of two items or more, after the first
    /// item of its FROM, holds it: SQLite reads such a join through a
    /// `SELECT *` of its own, which reads every column of its tables.
    pub(crate) grouped: bool,
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
    /// The address of the text of the table's or view's name in the syntax
    /// tree its FROM was read from (`table_places.rs`).
    pub(crate) named: Option<usize>,
}

impl FromItem {
    /// Whether sqlive can't tell that a lookup that reads its column `column`
    /// again at a write gets the query's answer: the column is volatile, or
    /// the item is a view read whole, which can show what the host's memory
    /// holds, or a clock, as well as its tables.
    pub(crate) fn column_unjudged(&self, column: &str) -> bool {
        self.columns.volatile(column) || self.is_view_read_whole()
    }

    /// Whether it is a view read whole.
    pub(crate) fn is_view_read_whole(&self) -> bool {
        matches!(self.source.object, Object::View { .. })
    }

    /// Whether every binding of its core holds a row of it: no LEFT JOIN
    /// adds it. A lookup finds every row such an item gives a binding, and
    /// none for the NULLs a LEFT JOIN's missing row gives.
    pub(crate) fn always_bound(&self) -> bool {
        !self.left
    }
}

/// The FROM item `table` of `core`, which sits in `parent`, where its name
/// can name `ctes`; or why sqlive can't read its rows.
pub(crate) fn read_item<'a>(
    connection: &Connection,
    planner: &mut Planner,
    table: &'a SelectTable<'a>,
    left: bool,
    core: usize,
    parent: Option<usize>,
    ctes: &Ctes<'a>,
) -> rusqlite::Result<Result<FromItem, Unread>> {
    let qualifier = |alias: &Option<As<'_>>, name: &Name<'_>| match alias {
        Some(As::As(alias) | As::Elided(alias)) => alias.0.to_owned(),
        None => name.0.to_owned(),
    };
    let item = |source, columns, derived| FromItem {
        source,
        columns,
        left_on: left.then(crate::scope_narrowing::Narrowing::default),
        left,
        core,
        derived,
        using: Vec::new(),
        cte: None,
        ors: Vec::new(),
        grouped: false,
        named: None,
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
                return Ok(Err(Unread("a table outside the main schema")));
            }
            let schema = planner.schema;
            let Some((stored, kind)) = schema.object(&name.name) else {
                return Ok(Err(Unread(
                    "a name that is no CTE, ordinary table, view or contentful FTS5 table",
                )));
            };
            let ordinary = kind == "table";
            let contentful = kind == "virtual";
            let stored = stored.to_owned();
            let written = name.name.0;
            let name = stored.to_ascii_lowercase();
            let object = if kind == "view" {
                let read = (
                    planner.items.len(),
                    planner.cores.len(),
                    planner.edges.len(),
                    planner.inlined.len(),
                );
                let why = match read_derived_item(
                    connection,
                    planner,
                    crate::derived::Named::View(&stored),
                    qualifier.clone(),
                    parent,
                    ctes,
                )? {
                    Ok((source, columns, derived)) => {
                        planner.inlined.push(name);
                        return Ok(Ok(item(source, columns, Some(derived))));
                    }
                    // A view sqlive can't read as cores of its own, as one
                    // with a recursive CTE, is read whole: for its values
                    // alone, and its tables whole.
                    Err(unread) => {
                        planner.items.truncate(read.0);
                        planner.cores.truncate(read.1);
                        planner.edges.truncate(read.2);
                        planner.inlined.truncate(read.3);
                        unread
                    }
                };
                // A read SQLite reports inside a view read whole names the
                // view or a CTE or view inside it, which the statement's own
                // CTEs and views may share.
                let found = crate::whole_view_names::whole_view_names(schema, &stored)?;
                let inside = found.as_ref().ok().cloned();
                planner.note_whole_view(found);
                Object::View { name, why: why.0.to_owned(), inside }
            } else {
                Object::Table { name, within: planner.within.clone() }
            };
            // An ordinary table, a contentful FTS5 table, whose rows sqlive
            // sees change as its content table's do, or a view read only for
            // its values.
            let stored_from = format!("main.{} AS {qualifier}", crate::sql::identifier(&stored));
            let (columns, from) = if contentful {
                let name = stored.to_ascii_lowercase();
                planner.fts5_shadows.extend(
                    ["content", "data", "idx", "docsize", "config"]
                        .map(|suffix| format!("{name}_{suffix}")),
                );
                let columns = fts5_columns(schema, &stored)?;
                let from = fts5_content_from(&stored, &qualifier, &columns);
                (Columns::Listed(columns), from)
            } else {
                (
                    columns(connection, schema, planner.functions, &qualifier, &stored, ordinary)?,
                    stored_from,
                )
            };
            let found = item(
                Source {
                    object,
                    from: Some(from),
                    qualifier,
                    term: None,
                    carried: None,
                    place: planner.within.clone().map(ReadPlace::Within),
                    unread: false,
                    columns: Default::default(),
                    either: Vec::new(),
                    memberships: Vec::new(),
                },
                columns,
                None,
            );
            Ok(Ok(FromItem { named: Some(written.as_ptr() as usize), ..found }))
        }
        SelectTable::TableCall(name, args, alias) => {
            if !in_main(name) {
                return Ok(Err(Unread("a table function outside the main schema")));
            }
            let qualifier = qualifier(alias, &name.name);
            let mut found = Vec::new();
            for arg in args.iter().flat_map(|args| args.iter()) {
                parts(arg, &mut found);
            }
            // A write's lookup asks the function again, so arguments that give
            // another answer then (a clock, `random()`) leave it unasked.
            let alone = found.iter().all(|part| part.answers_again(planner.functions));
            let from = alone.then(|| match alias {
                Some(_) => text(table),
                None => format!("{} AS {qualifier}", text(table)),
            });
            Ok(Ok(item(
                Source {
                    object: Object::Function,
                    from,
                    qualifier,
                    term: None,
                    carried: None,
                    place: None,
                    unread: false,
                    columns: Default::default(),
                    either: Vec::new(),
                    memberships: Vec::new(),
                },
                Columns::Unknown { volatile: false },
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
        SelectTable::Sub(..) => unreachable!("read_from reads a parenthesized join's tables"),
    }
}

/// Read the FROM `from` of `core`, which sits in `parent` where its names can
/// name `ctes`, into the core's items and the conditions of its joins, and
/// the parts of those conditions into `found`. For a parenthesized join's
/// tables, `group` says whether a LEFT JOIN adds the join, which then adds
/// its first table. A parenthesized join's tables are items of the core too,
/// joined one after another, where that gives SQLite's rows
/// (`Planner::group_flattens`). The reason for a shape that keeps every
/// table whole.
pub(crate) fn read_from<'a>(
    connection: &Connection,
    planner: &mut Planner,
    from: &'a FromClause<'a>,
    group: Option<bool>,
    (core, parent, ctes): (usize, Option<usize>, &Ctes<'a>),
    conditions: &mut Vec<(Written<'a>, Place)>,
    found: &mut Vec<Part<'a>>,
) -> rusqlite::Result<Result<(), Unread>> {
    let tables = from
        .select
        .iter()
        .map(|table| (*table, None))
        .chain(from.joins.iter().flatten().map(|join| (&join.table, Some(join))));
    for (table, join) in tables {
        let left = match join.map(|join| join.operator) {
            None => group.unwrap_or(false),
            Some(JoinOperator::Comma | JoinOperator::TypedJoin(None)) => false,
            Some(JoinOperator::TypedJoin(Some(kind))) => {
                if kind.intersects(JoinType::NATURAL | JoinType::RIGHT) {
                    return Ok(Err(Unread("a NATURAL or RIGHT join")));
                }
                kind.contains(JoinType::LEFT)
            }
        };
        let constraint = join.and_then(|join| join.constraint.as_ref());
        if let SelectTable::Sub(inner, alias) = table {
            // SQLite reads a parenthesized join under an alias as a
            // subquery's rows. One that starts with another, or that USING
            // joins, no reading here states.
            if alias.is_some()
                || matches!(inner.select, Some(SelectTable::Sub(..)))
                || matches!(constraint, Some(JoinConstraint::Using(_)))
            {
                return Ok(Err(Unread(
                    "a parenthesized join under an alias, starting with another, or joined by USING",
                )));
            }
            let before = planner.cores[core].items.len();
            let held = conditions.len();
            if let Err(unread) = read_from(
                connection,
                planner,
                inner,
                Some(left),
                (core, parent, ctes),
                conditions,
                found,
            )? {
                return Ok(Err(unread));
            }
            let mut on = Vec::new();
            if let Some(JoinConstraint::On(expr)) = constraint {
                parts(expr, found);
                conjuncts(expr, &mut on);
            }
            // The items the core gained: the tables in the parentheses. A
            // subquery among them has items of its own, in its own cores,
            // which the planner numbers among these.
            let group = &planner.cores[core].items[before..];
            let first = group[0];
            if !planner.group_flattens(core, group, left, &conditions[held..], &on) {
                return Ok(Err(Unread(
                    "a parenthesized join whose rows aren't its tables joined in turn",
                )));
            }
            if join.is_some() && planner.cores[core].items.len() - before > 1 {
                for position in before..planner.cores[core].items.len() {
                    let item = planner.cores[core].items[position];
                    planner.items[item].grouped = true;
                }
            }
            let place = if left { Place::LeftOn(first) } else { Place::Inner };
            conditions.extend(on.into_iter().map(|expr| (Written::Expr(expr), place)));
            continue;
        }
        if let SelectTable::TableCall(_, Some(args), _) = table {
            for arg in args.iter() {
                parts(arg, found);
            }
        }
        let item = match read_item(connection, planner, table, left, core, parent, ctes)? {
            Ok(item) => item,
            Err(unread) => return Ok(Err(unread)),
        };
        let place = if left { Place::LeftOn(planner.items.len()) } else { Place::Inner };
        planner.cores[core].items.push(planner.items.len());
        planner.items.push(item);
        match constraint {
            None => {}
            Some(JoinConstraint::On(expr)) => {
                parts(expr, found);
                let mut joined = Vec::new();
                conjuncts(expr, &mut joined);
                conditions.extend(joined.into_iter().map(|expr| (Written::Expr(expr), place)));
            }
            // Inside a parenthesized join, USING compares with the leftmost
            // table of the join, not of the core.
            Some(JoinConstraint::Using(_)) if group.is_some() => {
                return Ok(Err(Unread("USING inside a parenthesized join")))
            }
            Some(JoinConstraint::Using(names)) => {
                let right = planner.items.len() - 1;
                for name in names.iter() {
                    let column = crate::sql::name(name.0).to_ascii_lowercase();
                    let left = planner.using_left(core, right, &column);
                    let right_column = planner.items[right].columns.named(&Name(&column));
                    if let (Some(left), Some(right_column)) = (left, right_column) {
                        let using = Written::Using { left, right: (right, right_column) };
                        conditions.push((using, place));
                    }
                    planner.items[right].using.push(column);
                }
            }
        }
    }
    Ok(Ok(()))
}

impl Planner<'_, '_> {
    /// Whether a name among `found`, written in `core`, can name a
    /// contentful FTS5 table's hidden column (`ColumnValue::Hidden`): one
    /// that resolves to it, or one sqlive can't resolve that such a column
    /// of an item of `core`, or of a core it sits in, bears. `rank` and the
    /// functions that take the
    /// column bearing the table's name, as `bm25`, give a row values that
    /// depend on the table's other rows, and `MATCH` on that column is no
    /// test sqlive can ask of one row.
    pub(crate) fn names_hidden(&self, core: usize, found: &[Part<'_>]) -> bool {
        found.iter().any(|part| {
            let Part::Name(name) = part else { return false };
            if let Some((item, column)) = self.resolve(core, name) {
                return matches!(
                    self.items[item].columns.value(&column),
                    Some(ColumnValue::Hidden)
                );
            }
            let Some(column) = crate::sql::column_name(name).map(Name) else {
                return false;
            };
            self.scope_of(core).flat_map(|current| &self.cores[current].items).any(|item| {
                let columns = &self.items[*item].columns;
                columns
                    .named(&column)
                    .is_some_and(|named| matches!(columns.value(&named), Some(ColumnValue::Hidden)))
            })
        })
    }

    /// Whether `JOIN (...) ON on`, or a LEFT JOIN when `left`, of `group`,
    /// the items of `core` in the parentheses, whose own joins' conditions are
    /// `inner`, gives the rows its tables give joined one after another, the
    /// first by `on`.
    /// SQLite joins the parenthesized tables among themselves and keeps the
    /// rows `on` holds for; a LEFT JOIN gives a left row that no such row
    /// joins NULLs in every table of the join. An inner join's `on`, when it
    /// holds no subquery, is one more condition on the joined rows. A LEFT
    /// JOIN's is the ON of its first table when it names no other table of
    /// the join, and each later table is a LEFT JOIN's whose ON compares a
    /// column of a table before it in the join, other than by `IS`: such an
    /// ON can't hold where that table has no row, so a left row that no row
    /// of the join joins gets NULLs in each table, one after another.
    pub(crate) fn group_flattens(
        &self,
        core: usize,
        group: &[usize],
        left: bool,
        inner: &[(Written<'_>, Place)],
        on: &[&Expr<'_>],
    ) -> bool {
        let mut found = Vec::new();
        for expr in on {
            parts(expr, &mut found);
        }
        if found
            .iter()
            .any(|part| matches!(part, Part::Select(_) | Part::Subquery(_) | Part::Table(_)))
        {
            return false;
        }
        if !left {
            return true;
        }
        let later = &group[1..];
        let named_first = found.iter().all(|part| match part {
            Part::Name(name) => {
                self.resolve(core, name).is_some_and(|(item, _)| !later.contains(&item))
            }
            _ => true,
        });
        named_first
            && later.iter().enumerate().all(|(position, &item)| {
                self.items[item].left
                    && inner.iter().any(|(written, place)| {
                        *place == Place::LeftOn(item)
                            && matches!(written, Written::Expr(condition)
                                if self.rejects(core, condition, &group[..=position]))
                    })
            })
    }

    /// Whether `condition`, written in `core`, can't hold where an item of
    /// `items` has no row: a comparison other than `IS` and `IS NOT` with a
    /// column of one of them on a side.
    fn rejects(&self, core: usize, condition: &Expr<'_>, items: &[usize]) -> bool {
        let Expr::Binary(left, op, right) = unparenthesized(condition) else {
            return false;
        };
        matches!(
            op,
            Operator::Equals
                | Operator::NotEquals
                | Operator::Less
                | Operator::LessEquals
                | Operator::Greater
                | Operator::GreaterEquals
        ) && [left, right].into_iter().any(|side| {
            is_name(side) && self.resolve(core, side).is_some_and(|(item, _)| items.contains(&item))
        })
    }
}

fn in_main(name: &QualifiedName<'_>) -> bool {
    name.db_name.as_ref().is_none_or(|name| *name == "main")
}

/// A contentful FTS5 table's columns, in order: its own hold the values its
/// content table holds, of no affinity, which compare by BINARY; its hidden
/// ones, the one that bears the table's name and `rank`, give values that
/// depend on rows besides their own.
fn fts5_columns(schema: &SchemaLookups<'_>, table: &str) -> rusqlite::Result<Vec<Column>> {
    let columns = schema
        .columns(table)?
        .iter()
        .map(|DeclaredColumn { name, hidden, .. }| Column {
            name: Some(name.to_ascii_lowercase()),
            value: if *hidden {
                ColumnValue::Hidden
            } else {
                ColumnValue::Compared(Affinity::Blob)
            },
        })
        .collect();
    Ok(columns)
}

/// The FROM item a lookup reads a contentful FTS5 table's rows by, which
/// the statement names by `qualifier`: its content table, each of the
/// table's own `columns` under its name. The content table holds the same
/// rows, and an ordinary table's scan costs a lookup fewer of SQLite's steps
/// than FTS5's, which reads its index and its content table for each row.
fn fts5_content_from(table: &str, qualifier: &str, columns: &[Column]) -> String {
    let own: Vec<String> = columns
        .iter()
        .filter(|column| !matches!(column.value, ColumnValue::Hidden))
        .filter_map(|column| column.name.as_deref())
        .enumerate()
        .map(|(position, name)| format!("c{position} AS {}", crate::sql::identifier(name)))
        .collect();
    format!(
        "(SELECT {} FROM main.{}) AS {qualifier}",
        own.join(","),
        crate::sql::identifier(&format!("{table}_content"))
    )
}

/// A table's or a view's columns, in order, `ordinary` for a table, which the
/// statement names by `qualifier`. A view's comparisons happen inside SQLite,
/// so sqlive gives none of them an affinity to narrow by. The connection's
/// `functions` tell whether a generated column's calls repeat.
pub(crate) fn columns(
    connection: &Connection,
    schema: &SchemaLookups<'_>,
    functions: &Functions,
    qualifier: &str,
    table: &str,
    ordinary: bool,
) -> rusqlite::Result<Columns> {
    let mut columns = Vec::new();
    // A table's hidden columns are its generated ones.
    let mut generated = Vec::new();
    for DeclaredColumn { name: column, declared, hidden, .. } in schema.columns(table)? {
        let hidden = *hidden;
        let collation = if schema.is_host_table(table) {
            // A host table's values compare as BINARY, and SQLite keeps no
            // metadata of a virtual table's columns.
            Some("BINARY".to_owned())
        } else if ordinary {
            let (_, collation, _, _, _) =
                connection.column_metadata(Some("main"), table, column.as_str())?;
            collation.map(|collation| collation.to_string_lossy().into_owned())
        } else {
            None
        };
        let binary = collation.as_deref().is_some_and(|name| name.eq_ignore_ascii_case("BINARY"));
        let affinity = Affinity::declared(declared);
        if binary && hidden {
            generated.push((columns.len(), affinity));
        }
        let value = match collation {
            Some(_) if binary && !hidden => ColumnValue::Compared(affinity),
            Some(collation) if !binary && !hidden => ColumnValue::Collated(affinity, collation),
            _ => ColumnValue::Unknown,
        };
        columns.push(Column { name: Some(column.to_ascii_lowercase()), value });
    }
    if generated.is_empty() {
        return Ok(Columns::Listed(columns));
    }
    let read = schema.read_generated(table, |expressions| {
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
            _ => return None,
        };
        let column = columns
            .iter()
            .find(|column| column.name.as_deref().is_some_and(|column| name == column))?;
        match column.value {
            ColumnValue::Compared(compared) => Some((column.name.clone()?, compared)),
            ColumnValue::Generated(_)
            | ColumnValue::Collated(..)
            | ColumnValue::Unknown
            | ColumnValue::Volatile
            | ColumnValue::Hidden => None,
        }
    })?;
    Some(RowExpression { term, affinity })
}
