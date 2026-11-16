//! A statement that orders by columns of one table and has a LIMIT with no
//! OFFSET, whose full run depends only on that table's rows up to its last
//! row (§4).
//!
//! When a run returns as many rows as its LIMIT, a row that sorts after its
//! last row can't appear in the result: every result row it would give sorts
//! after the rows already there. So the run's scope for the table also requires
//! a row to sort at or before the last row, in the ORDER BY's directions, and
//! the next run sets that bound again from its own last row. A row equal to the
//! last one could sort before it, so it passes. A run that returned fewer rows
//! than its LIMIT holds every row its WHERE allows, and keeps its scope.
//!
//! coven compares the values as BINARY, as SQLite does for a stored column of
//! that collation, and takes the last row's values from the result columns
//! that return them. So each ORDER BY term is a column of one table of the
//! statement's only SELECT, written as a name with no COLLATE and no NULLS
//! FIRST or LAST, that the statement returns as a result column. The table
//! isn't the right side of a LEFT JOIN. There, when a write adds a row that
//! matches a left row that had no match, SQLite no longer returns the row of
//! NULLs it returned for that left row, and that row of NULLs can sort before
//! the last row. The SELECT has no GROUP BY and calls no aggregate or window
//! function in its result columns, since there one result row's values depend
//! on many table rows.

use std::cmp::Ordering;

use crate::row_scope::RowScope;
use crate::scope_plan::{is_name, Planner};
use crate::scope_term::{operand, Affinity, Operand};
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{As, Expr, Name, OneSelect, ResultColumn, Select, SortOrder};

/// A statement's order and LIMIT, which a full run's last row bounds.
pub(crate) struct SortedLimit {
    /// The source whose rows the order sorts.
    pub(crate) source: usize,
    /// The ORDER BY's terms, in order.
    pub(crate) terms: Vec<SortTerm>,
    /// The LIMIT's value.
    limit: Operand,
}

/// One ORDER BY term: a column of the sorted source.
pub(crate) struct SortTerm {
    /// The column, lowercase.
    pub(crate) column: String,
    /// Whether it sorts in descending order.
    pub(crate) descending: bool,
    /// The result column, from zero, that returns its value.
    position: usize,
}

/// Where a run finds the row its scope ends at.
pub struct LastRow {
    /// How many rows the run must return for its last row to bound its
    /// scope: the LIMIT.
    pub limit: usize,
    /// The result columns, from zero, that return the ORDER BY's values, in
    /// its order.
    positions: Vec<usize>,
}

impl LastRow {
    /// The ORDER BY's values in `row`, in its order.
    pub fn values(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<Vec<Value>> {
        self.positions.iter().map(|position| Ok(row.get_ref(*position)?.into())).collect()
    }
}

impl SortedLimit {
    /// Where a run bound to `parameters` finds its last row, or `None` when
    /// the LIMIT's value is not a positive integer.
    pub(crate) fn last_row(&self, parameters: &[Value]) -> Option<LastRow> {
        let Value::Integer(limit) = self.limit.value(parameters) else {
            return None;
        };
        Some(LastRow {
            limit: usize::try_from(*limit).ok().filter(|limit| *limit > 0)?,
            positions: self.terms.iter().map(|term| term.position).collect(),
        })
    }
}

/// The order and LIMIT of `select`, the statement whose only core is `core`,
/// when a full run's last row can bound its scope.
pub(crate) fn sorted_limit(
    connection: &Connection,
    planner: &Planner,
    select: &Select<'_>,
    core: usize,
) -> rusqlite::Result<Option<SortedLimit>> {
    let (Some(limit), Some(order_by)) = (select.limit, select.order_by) else {
        return Ok(None);
    };
    if limit.offset.is_some() || select.body.compounds.is_some() {
        return Ok(None);
    }
    let OneSelect::Select { columns, group_by: None, having: None, window_clause: None, .. } =
        &select.body.select
    else {
        return Ok(None);
    };
    let mut found = Vec::new();
    for column in columns.iter() {
        if let ResultColumn::Expr(expr, _) = column {
            crate::select_syntax::parts(expr, &mut found);
        }
    }
    let many_rows = found.iter().any(|part| {
        matches!(part, crate::select_syntax::Part::Function(name, window)
            if *window || planner.aggregates.contains(&name.to_ascii_lowercase()))
    });
    if many_rows {
        return Ok(None);
    }
    let Some(limit) = operand(connection, &limit.expr, Affinity::Blob)? else {
        return Ok(None);
    };
    let mut source = None;
    let mut terms = Vec::new();
    for term in order_by.iter() {
        if term.nulls.is_some() {
            return Ok(None);
        }
        let Some((position, item, column)) = result_column(planner, core, columns, &term.expr)
        else {
            return Ok(None);
        };
        if source.is_some_and(|source| source != item) {
            return Ok(None);
        }
        source = Some(item);
        terms.push(SortTerm { column, descending: term.order == Some(SortOrder::Desc), position });
    }
    let Some(source) = source else {
        return Ok(None);
    };
    let from = &planner.items[source];
    let sorted = from.core == core
        && from.source.table.is_some()
        && from.left_on.is_none()
        && terms.iter().all(|term| from.columns.affinity(&term.column).is_some());
    Ok(sorted.then_some(SortedLimit { source, terms, limit }))
}

/// The result column that returns the ORDER BY term `expr`, and the source
/// and column, lowercase, whose value it is, when `expr` names a column. A
/// bare name that matches a result column's alias means that result column,
/// as SQLite reads an ORDER BY; any other name means the column it resolves
/// to.
fn result_column(
    planner: &Planner,
    core: usize,
    columns: &[ResultColumn<'_>],
    expr: &Expr<'_>,
) -> Option<(usize, usize, String)> {
    let bare = match expr {
        Expr::Id(name) => Some(Name(name.0)),
        Expr::Name(name) => Some(name.clone()),
        Expr::Qualified(..) | Expr::DoublyQualified(..) => None,
        _ => return None,
    };
    // coven doesn't know how many columns a `*` adds, so it can't tell the
    // position of a column after one.
    let listed: Vec<&Expr<'_>> = columns
        .iter()
        .map_while(|column| match column {
            ResultColumn::Expr(expr, _) => Some(expr),
            ResultColumn::Star | ResultColumn::TableStar(_) => None,
        })
        .collect();
    let named: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            matches!((column, &bare), (ResultColumn::Expr(_, Some(As::As(alias) | As::Elided(alias))), Some(bare))
                if Name(alias.0) == *bare)
        })
        .map(|(position, _)| position)
        .collect();
    let (position, (item, column)) = match named[..] {
        [] => {
            let resolved = planner.resolve(core, expr)?;
            let position = listed.iter().position(|listed| {
                is_name(listed) && planner.resolve(core, listed).as_ref() == Some(&resolved)
            })?;
            (position, resolved)
        }
        [position] => {
            let listed = listed.get(position)?;
            if !is_name(listed) {
                return None;
            }
            (position, planner.resolve(core, listed)?)
        }
        _ => return None,
    };
    Some((position, item, column))
}

/// The rows that sort at or before `last`, the values of a run's last row in
/// the order `terms`: lexicographically, each term in its direction, NULL
/// first in ascending order and last in descending, as SQLite sorts. A row
/// equal to `last` passes.
pub(crate) fn sorted_at_or_before(terms: &[SortTerm], last: &[Value]) -> RowScope {
    let mut scope = RowScope::All;
    for (term, value) in terms.iter().zip(last).rev() {
        let column = || term.column.clone();
        let compare = |ordering| RowScope::Compare {
            column: column(),
            value: value.clone(),
            ordering,
            inclusive: false,
        };
        let null = |not| RowScope::Null { column: column(), not };
        let equal = match value {
            Value::Null => null(false),
            _ => compare(Ordering::Equal),
        };
        let before = match (term.descending, value) {
            (false, Value::Null) => RowScope::equal(column(), Vec::new()),
            (false, _) => null(false).or(compare(Ordering::Less)),
            (true, Value::Null) => null(true),
            (true, _) => compare(Ordering::Greater),
        };
        scope = before.or(equal.and(scope));
    }
    scope
}
