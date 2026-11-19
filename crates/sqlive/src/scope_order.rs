//! A statement that orders by columns of one table, or expressions over
//! them, and has a LIMIT with no OFFSET, whose full run depends only on that
//! table's rows up to its last row (§4).
//!
//! When a run returns as many rows as its LIMIT, a row that sorts after its
//! last row can't appear in the result: every result row it would give sorts
//! after the rows already there. So the run's scope for the table also requires
//! a row to sort at or before the last row, in the ORDER BY's directions, and
//! the next run sets that bound again from its own last row. A row equal to the
//! last one could sort before it, so it passes. A run that returned fewer rows
//! than its LIMIT holds every row its WHERE allows, and keeps its scope.
//!
//! sqlive takes the last row's values from the result columns that return
//! them, so each ORDER BY term is a result column of the statement's only
//! SELECT, written as the term writes it, with no COLLATE and no NULLS FIRST
//! or LAST. Each term reads one table of that SELECT: a stored column of
//! BINARY collation, whose values sqlive compares as SQLite does; or an
//! expression over such columns that SQLite can evaluate row by row, or a
//! generated column whose expression names only such columns, whose value in
//! a changed row a write asks SQLite to compare with the last row's
//! (`scope_row.rs`). The table isn't the right side of a LEFT JOIN. There,
//! when a write adds a row that matches a left row that had no match, SQLite
//! no longer returns the row of NULLs it returned for that left row, and that
//! row of NULLs can sort before the last row. The SELECT has no GROUP BY and
//! calls no aggregate or window function in its result columns, since there
//! one result row's values depend on many table rows.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::from_item::ColumnValue;
use crate::row_scope::RowScope;
use crate::scope_plan::{is_name, Planner};
use crate::scope_row::{row_term, RowCondition, RowTerm, RowTest};
use crate::scope_term::{operand, Affinity, Operand};
use crate::select_syntax::{order_term, parts, OrderTerm, Part};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, OneSelect, ResultColumn, Select, SortOrder};

/// A statement's order and LIMIT, which a full run's last row bounds.
pub(crate) struct SortedLimit {
    /// The source whose rows the order sorts.
    pub(crate) source: usize,
    /// The ORDER BY's terms, in order.
    pub(crate) terms: Vec<SortTerm>,
    /// The LIMIT's value.
    limit: Operand,
}

/// One ORDER BY term of the sorted source.
pub(crate) struct SortTerm {
    /// What it sorts the source's rows by.
    pub(crate) key: SortKey,
    /// Whether it sorts in descending order.
    pub(crate) descending: bool,
    /// The result column, from zero, that returns its value.
    position: usize,
}

/// What an ORDER BY term sorts the sorted source's rows by.
pub(crate) enum SortKey {
    /// A stored column, lowercase, whose values sqlive compares itself.
    Column(String),
    /// An expression over the source's stored columns, whose value in a
    /// changed row a write asks SQLite for, with the affinity SQLite gives
    /// that value: a generated column's own, or none for an expression the
    /// ORDER BY writes.
    Expression { term: Arc<RowTerm>, affinity: Affinity },
}

impl SortKey {
    /// The rows whose key is NULL, or isn't when `not`, for a run bound to
    /// `parameters`.
    fn null(&self, not: bool, parameters: &[Value]) -> RowScope {
        match self {
            Self::Column(column) => RowScope::Null { column: column.clone(), not },
            Self::Expression { term, .. } => row(term, parameters, RowTest::Null { not }),
        }
    }

    /// The rows whose key compares with `value`, which isn't NULL, as
    /// `ordering`, for a run bound to `parameters`.
    fn compare(&self, value: &Value, ordering: Ordering, parameters: &[Value]) -> RowScope {
        match self {
            Self::Column(column) => RowScope::Compare {
                column: column.clone(),
                value: value.clone(),
                ordering,
                inclusive: false,
            },
            Self::Expression { term, affinity } => row(
                term,
                parameters,
                RowTest::Compare { ordering, value: value.clone(), affinity: *affinity },
            ),
        }
    }
}

/// The rows a write asks SQLite whether `test` holds of `term` for.
fn row(term: &Arc<RowTerm>, parameters: &[Value], test: RowTest) -> RowScope {
    RowScope::Row(Arc::new(RowCondition {
        term: Arc::clone(term),
        parameters: parameters.to_vec(),
        test,
    }))
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
    if found.iter().any(|part| part.many_rows(planner.functions)) {
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
        let Some((position, item, key)) = sort_term(planner, core, columns, &term.expr) else {
            return Ok(None);
        };
        if source.is_some_and(|source| source != item) {
            return Ok(None);
        }
        source = Some(item);
        terms.push(SortTerm { key, descending: term.order == Some(SortOrder::Desc), position });
    }
    let Some(source) = source else {
        return Ok(None);
    };
    let from = &planner.items[source];
    let sorted = from.core == core && from.source.table.is_some() && from.left_on.is_none();
    Ok(sorted.then_some(SortedLimit { source, terms, limit }))
}

/// The result column that returns the ORDER BY term `expr`, the source whose
/// rows it sorts and what it sorts them by, when a run's last row can bound
/// them. A bare name that matches a result column's alias means that result
/// column, as SQLite reads an ORDER BY; any other name means the column it
/// resolves to, and any other expression the result column that writes it
/// the same way.
fn sort_term(
    planner: &Planner,
    core: usize,
    columns: &[ResultColumn<'_>],
    expr: &Expr<'_>,
) -> Option<(usize, usize, SortKey)> {
    let (position, sorted) = match order_term(columns, expr) {
        OrderTerm::Alias(position, listed) => (position, listed),
        OrderTerm::Expr(expr) => {
            let resolved = is_name(expr).then(|| planner.resolve(core, expr)).flatten();
            // sqlive doesn't know how many columns a `*` adds, so it can't
            // tell the position of a column after one.
            let position = columns
                .iter()
                .map_while(|column| match column {
                    ResultColumn::Expr(expr, _) => Some(expr),
                    ResultColumn::Star | ResultColumn::TableStar(_) => None,
                })
                .position(|listed| match &resolved {
                    Some(resolved) => {
                        is_name(listed) && planner.resolve(core, listed).as_ref() == Some(resolved)
                    }
                    None => unparenthesized(listed) == expr,
                })?;
            (position, expr)
        }
        OrderTerm::Position(_) | OrderTerm::Unknown => return None,
    };
    if is_name(sorted) {
        let (item, column) = planner.resolve(core, sorted)?;
        let from = &planner.items[item];
        let key = match from.columns.value(&column)? {
            ColumnValue::Compared(_) => SortKey::Column(column),
            ColumnValue::Generated(generated) => SortKey::Expression {
                term: Arc::new(RowTerm {
                    sql: generated.sql.clone(),
                    qualifier: from.source.qualifier.clone(),
                    columns: generated.columns.clone(),
                }),
                affinity: generated.affinity,
            },
            ColumnValue::Unknown => return None,
        };
        return Some((position, item, key));
    }
    let mut found = Vec::new();
    parts(sorted, &mut found);
    // A COLLATE gives the ORDER BY its collation, by which sqlive doesn't
    // compare the last row's value.
    if found.iter().any(|part| matches!(part, Part::Collation)) {
        return None;
    }
    let mut items = found.iter().filter_map(|part| match part {
        Part::Name(name) => Some(planner.resolve(core, name).map(|(item, _)| item)),
        _ => None,
    });
    let item = items.next()??;
    if !items.all(|other| other == Some(item)) {
        return None;
    }
    let term = row_term(planner, core, item, sorted, &found)?;
    Some((position, item, SortKey::Expression { term, affinity: Affinity::Blob }))
}

/// The rows that sort at or before `last`, the values of a run's last row in
/// the order `terms`, for a run bound to `parameters`: lexicographically,
/// each term in its direction, NULL first in ascending order and last in
/// descending, as SQLite sorts. A row equal to `last` passes.
pub(crate) fn sorted_at_or_before(
    terms: &[SortTerm],
    last: &[Value],
    parameters: &[Value],
) -> RowScope {
    let mut scope = RowScope::All;
    for (term, value) in terms.iter().zip(last).rev() {
        let key = &term.key;
        let equal = match value {
            Value::Null => key.null(false, parameters),
            _ => key.compare(value, Ordering::Equal, parameters),
        };
        // No value sorts before NULL in ascending order.
        let before = match (term.descending, value) {
            (false, Value::Null) => None,
            (false, _) => {
                Some(key.null(false, parameters).or(key.compare(value, Ordering::Less, parameters)))
            }
            (true, Value::Null) => Some(key.null(true, parameters)),
            (true, _) => Some(key.compare(value, Ordering::Greater, parameters)),
        };
        let tied = equal.and(scope);
        scope = match before {
            Some(before) => before.or(tied),
            None => tied,
        };
    }
    scope
}
