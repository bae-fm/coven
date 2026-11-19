//! A statement that orders by columns of its tables, or expressions over one
//! table's columns, and has a LIMIT with no OFFSET, whose full run depends
//! only on the rows that sort at or before its last row (§4).
//!
//! When a run returns as many rows as its LIMIT, a row that sorts after its
//! last row can't appear in the result: every result row it would give sorts
//! after the rows already there. So the run's scopes also require a row to
//! sort at or before the last row, in the ORDER BY's directions, and the next
//! run sets that bound again from its own last row. A row equal to the last
//! one could sort before it, so it passes. A run that returned fewer rows
//! than its LIMIT holds every row its WHERE allows, and keeps its scopes.
//!
//! The ORDER BY's terms fall into runs, each of consecutive terms over one
//! table. A result row sorts at or before the last row when its values of
//! the first run sort before the last row's, or equal them and the rest sort
//! at or before. So the first run's table takes the rows whose own terms sort
//! at or before the last row's values of that run. A later run's table takes
//! those rows too, and the rows that join, for some earlier run, a row of
//! that run's table that sorts before the last row's values of that run,
//! which a write looks up as it looks up a join (`scope_lookup.rs`). A table
//! that takes no value from each earlier run's table has no bound.
//!
//! sqlive takes the last row's values from the result columns that return
//! them, so each ORDER BY term is a result column of the statement's only
//! SELECT, written as the term writes it, with no COLLATE and no NULLS FIRST
//! or LAST. Each term reads one table of that SELECT: a stored column of
//! BINARY collation, whose values sqlive compares as SQLite does; or an
//! expression over such columns that SQLite can evaluate row by row, or a
//! generated column whose expression names only such columns, whose value in
//! a changed row a write asks SQLite to compare with the last row's
//! (`scope_row.rs`). No such table is the right side of a LEFT JOIN. There,
//! when a write adds a row that matches a left row that had no match, SQLite
//! no longer returns the row of NULLs it returned for that left row, and that
//! row of NULLs can sort before the last row. The SELECT has no GROUP BY and
//! calls no aggregate or window function in its result columns, since there
//! one result row's values depend on many table rows.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::from_item::ColumnValue;
use crate::row_scope::RowScope;
use crate::scope_plan::{is_name, Membership, Planner};
use crate::scope_row::{item_row_term, RowCondition, RowExpression, RowTerm, RowTest};
use crate::scope_term::{operand, Affinity, Operand};
use crate::select_syntax::{order_term, parts, Functions, OrderTerm, Part};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{
    Distinctness, Expr, OneSelect, ResultColumn, Select, SortOrder, SortedColumn,
};

/// A statement's order and LIMIT, which a full run's last row bounds.
pub(crate) struct SortedLimit {
    /// The ORDER BY's terms, in runs of consecutive terms over one source.
    runs: Vec<SortRun>,
    limit: RunLimit,
}

/// A LIMIT whose value a run's parameters can give.
#[derive(Clone, Debug)]
pub(crate) struct RunLimit(Operand);

impl RunLimit {
    /// The LIMIT's value for a run bound to `parameters`, or `None` when it
    /// is not a positive integer.
    pub(crate) fn value(&self, parameters: &[Value]) -> Option<usize> {
        let Value::Integer(limit) = self.0.value(parameters) else {
            return None;
        };
        usize::try_from(*limit).ok().filter(|limit| *limit > 0)
    }
}

/// One SELECT that keeps the first rows of its ORDER BY by a LIMIT with no
/// OFFSET, each of whose rows comes of one row of its tables: no GROUP BY,
/// HAVING or WINDOW clause, and no aggregate or window function in its
/// result columns.
pub(crate) struct OrderedLimit<'a> {
    pub(crate) columns: &'a [ResultColumn<'a>],
    pub(crate) where_clause: Option<&'a Expr<'a>>,
    pub(crate) distinct: bool,
    pub(crate) order_by: &'a [SortedColumn<'a>],
    pub(crate) limit: RunLimit,
}

/// `select` as an `OrderedLimit`, when it is one; the connection's
/// `functions` tell an aggregate call.
pub(crate) fn ordered_limit<'a>(
    connection: &Connection,
    functions: &Functions,
    select: &'a Select<'a>,
) -> rusqlite::Result<Option<OrderedLimit<'a>>> {
    let (Some(limit), Some(order_by)) = (select.limit, select.order_by) else {
        return Ok(None);
    };
    if limit.offset.is_some() || select.body.compounds.is_some() {
        return Ok(None);
    }
    let OneSelect::Select {
        distinctness,
        columns,
        where_clause,
        group_by: None,
        having: None,
        window_clause: None,
        ..
    } = &select.body.select
    else {
        return Ok(None);
    };
    let mut found = Vec::new();
    for column in columns.iter() {
        if let ResultColumn::Expr(expr, _) = column {
            crate::select_syntax::parts(expr, &mut found);
        }
    }
    if found.iter().any(|part| part.many_rows(functions)) {
        return Ok(None);
    }
    let Some(limit) = operand(connection, &limit.expr, Affinity::Blob)? else {
        return Ok(None);
    };
    Ok(Some(OrderedLimit {
        columns,
        where_clause: *where_clause,
        distinct: matches!(distinctness, Some(Distinctness::Distinct)),
        order_by,
        limit: RunLimit(limit),
    }))
}

/// Consecutive ORDER BY terms over one source.
struct SortRun {
    /// The source whose rows the terms sort.
    source: usize,
    terms: Vec<SortTerm>,
}

/// One ORDER BY term of a sorted source.
struct SortTerm {
    /// What it sorts the source's rows by.
    key: SortKey,
    /// Whether it sorts in descending order.
    descending: bool,
    /// The result column, from zero, that returns its value.
    position: usize,
}

/// What an ORDER BY term sorts its source's rows by.
enum SortKey {
    /// A stored column, lowercase, whose values sqlive compares itself.
    Column(String),
    /// An expression over the source's stored columns, whose value in a
    /// changed row a write asks SQLite for: a generated column's, or one the
    /// ORDER BY writes.
    Expression(RowExpression),
}

impl SortKey {
    /// The rows whose key is NULL, or isn't when `not`, for a run bound to
    /// `parameters`.
    fn null(&self, not: bool, parameters: &[Value]) -> RowScope {
        match self {
            Self::Column(column) => RowScope::Null { column: column.clone(), not },
            Self::Expression(RowExpression { term, .. }) => {
                row(term, parameters, RowTest::Null { not })
            }
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
            Self::Expression(RowExpression { term, affinity }) => row(
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
        Some(LastRow {
            limit: self.limit.value(parameters)?,
            positions: self
                .runs
                .iter()
                .flat_map(|run| &run.terms)
                .map(|term| term.position)
                .collect(),
        })
    }

    /// The bound of `source`'s rows for a run bound to `parameters` whose
    /// last row's ORDER BY values are `last`, when the order sorts it: its
    /// first run's terms at or before their values, or, for each earlier
    /// run, a row it joins of that run's source that sorts before that run's
    /// values. `memberships` are its own, and `looked_up` gives the rows
    /// whose membership finds a row of its partner that passes the given
    /// scope too. `None` when it takes no value from an earlier run's source.
    pub(crate) fn bound(
        &self,
        source: usize,
        last: &[Value],
        parameters: &[Value],
        memberships: &[Membership],
        looked_up: impl Fn(&Membership, RowScope) -> RowScope,
    ) -> Option<RowScope> {
        let mut earlier = Vec::new();
        let mut values = last;
        for run in &self.runs {
            let (own, rest) = values.split_at(run.terms.len());
            values = rest;
            if run.source == source {
                let at_or_before = sorted_before(&run.terms, own, parameters, Some(RowScope::All))
                    .expect("a tied row passes, so some row does");
                return Some(
                    earlier
                        .into_iter()
                        .rev()
                        .fold(at_or_before, |later, sooner: RowScope| sooner.or(later)),
                );
            }
            let membership = memberships.iter().find(|found| found.partner == run.source)?;
            if let Some(before) = sorted_before(&run.terms, own, parameters, None) {
                earlier.push(looked_up(membership, before));
            }
        }
        None
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
    let Some(OrderedLimit { columns, order_by, limit, .. }) =
        ordered_limit(connection, planner.functions, select)?
    else {
        return Ok(None);
    };
    let mut runs: Vec<SortRun> = Vec::new();
    for term in order_by.iter() {
        if term.nulls.is_some() {
            return Ok(None);
        }
        let Some((position, item, key)) = sort_term(planner, core, columns, &term.expr) else {
            return Ok(None);
        };
        let from = &planner.items[item];
        if from.core != core || from.source.table.is_none() || from.left_on.is_some() {
            return Ok(None);
        }
        let term = SortTerm { key, descending: term.order == Some(SortOrder::Desc), position };
        match runs.last_mut() {
            Some(run) if run.source == item => run.terms.push(term),
            _ => runs.push(SortRun { source: item, terms: vec![term] }),
        }
    }
    Ok((!runs.is_empty()).then_some(SortedLimit { runs, limit }))
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
            ColumnValue::Generated(generated) => SortKey::Expression(generated.clone()),
            ColumnValue::Unknown | ColumnValue::Hidden => return None,
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
    let term = item_row_term(planner, core, item, sorted, &found)?;
    Some((position, item, SortKey::Expression(RowExpression { term, affinity: Affinity::Blob })))
}

/// The rows that sort before `last`, the last row's values of the terms
/// `terms`, or equal it and pass `tied`, for a run bound to `parameters`:
/// lexicographically, each term in its direction, NULL first in ascending
/// order and last in descending, as SQLite sorts. `None`, for `tied` too,
/// passes no row.
fn sorted_before(
    terms: &[SortTerm],
    last: &[Value],
    parameters: &[Value],
    tied: Option<RowScope>,
) -> Option<RowScope> {
    let mut scope = tied;
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
        let tied = scope.map(|scope| equal.and(scope));
        scope = match (before, tied) {
            (Some(before), Some(tied)) => Some(before.or(tied)),
            (before, None) => before,
            (None, tied) => tied,
        };
    }
    scope
}
