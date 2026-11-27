//! A statement that orders by columns of its FROM items, or expressions over
//! one item's columns, and has a LIMIT with no OFFSET, whose full run depends
//! only on the rows that sort at or before its last row (SPEC.md section 6).
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
//! A run's source can be a subquery, CTE or view the statement reads as a
//! FROM item, such as a spread fragment. When it has one arm, with no LIMIT,
//! OFFSET or window function, and the arm passes the columns the run's first
//! terms read on from one of its own sources, that no LEFT JOIN adds, each
//! row of the item is made of a row of that source that sorts as the item's
//! does over those terms. So that source takes the first run's bound over
//! them too, with the item's joins read as its own, and so does a source
//! inside it where it is such an item (`derived::only_source`). This bound
//! narrows the source's table, and no lookup of another source.
//!
//! sqlive takes the last row's values from the result columns that return them,
//! so each ORDER BY term is a result column of the statement's only SELECT, one
//! it writes as the term does, with or without the COLLATE the term ends in, or
//! one a `*` lists, and with no NULLS FIRST or LAST. Each term reads one FROM
//! item of that SELECT: a
//! stored column of BINARY collation, whose values sqlive compares as SQLite
//! does; a stored column of another collation; or an expression over stored
//! BINARY columns that SQLite can evaluate row by row, or a generated column
//! whose expression names only such columns, whose value in a changed row a
//! write asks SQLite to compare with the last row's (`scope_row.rs`). The term
//! sorts text by its collation: the COLLATE it ends in, else its column's
//! declared one, else BINARY. A write asks SQLite to compare in that collation,
//! whether SQLite's own or one the app registered, so sqlive keeps no list of
//! collations (SPEC.md section 6). A COLLATE inside the term, anywhere but its
//! end, gives a comparison within it a collation of its own, and the term
//! bounds nothing. No such item is the right side of a LEFT JOIN. There, when
//! a write adds a row that matches a left row that had no match, SQLite no
//! longer returns the row of NULLs it returned for that left row, and that row
//! of NULLs can sort before the last row. The SELECT has no GROUP BY and calls
//! no aggregate or window function in its result columns, since there one
//! result row's values depend on many table rows.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::derived::{arm_columns, only_source, Output};
use crate::from_item::ColumnValue;
use crate::row_scope::RowScope;
use crate::scope_plan::{is_name, Membership, Planner, Source};
use crate::scope_row::{item_row_term, RowColumn, RowCondition, RowExpression, RowTerm, RowTest};
use crate::scope_term::{operand, Affinity, Operand, RunValues};
use crate::select_syntax::{order_term, parts, Functions, OrderTerm, Part};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{
    Distinctness, Expr, OneSelect, ResultColumn, Select, SortOrder, SortedColumn,
};

/// A statement's order and LIMIT, which a full run's last row bounds.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct SortedLimit {
    /// The ORDER BY's terms, in runs of consecutive terms over one source.
    runs: Vec<SortRun>,
    limit: RunLimit,
}

/// A LIMIT whose value a run's parameters can give.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct RunLimit(Operand);

impl RunLimit {
    /// The LIMIT's value for a run bound to `values`, or `None` when it is
    /// not a positive integer.
    pub(crate) fn value(&self, values: RunValues<'_>) -> Option<usize> {
        let Value::Integer(limit) = self.0.value(values, Affinity::Numeric) else {
            return None;
        };
        usize::try_from(limit).ok().filter(|limit| *limit > 0)
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
#[derive(serde::Serialize, serde::Deserialize)]
struct SortRun {
    /// The source whose rows the terms sort.
    source: usize,
    terms: Vec<SortTerm>,
    /// For the first run over a derived item, the sources inside it that its
    /// first terms pass down to, each inside the one before it.
    inner: Vec<InnerRun>,
}

/// A source inside a derived item that each row of the item is made from,
/// whose columns the first terms of the item's run read: the run's bound on
/// its rows is the item's, over those terms.
#[derive(serde::Serialize, serde::Deserialize)]
struct InnerRun {
    source: usize,
    /// How the statement names the source's rows.
    qualifier: String,
    /// This source's column that holds each column of the source around it
    /// that one of its columns passes to.
    columns: BTreeMap<String, String>,
    /// The run's first terms, over the source.
    terms: Vec<SortTerm>,
}

/// One ORDER BY term of a sorted source.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SortTerm {
    /// What it sorts the source's rows by.
    key: SortKey,
    /// Whether it sorts in descending order.
    descending: bool,
    /// The result column, from zero, that returns its value.
    position: usize,
}

/// What an ORDER BY term sorts its source's rows by.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
enum SortKey {
    /// A stored column of BINARY collation, lowercase, whose values sqlive
    /// compares itself.
    Column(String),
    /// An expression over the source's stored columns, whose value in a
    /// changed row a write asks SQLite for, and how it compares with the last
    /// row's, text by `collation` when it isn't BINARY: the COLLATE the term
    /// ends in, else its column's. A generated column's, a stored column of
    /// another collation, or one the ORDER BY writes.
    Expression { expression: RowExpression, collation: Option<String> },
}

impl SortKey {
    /// The rows whose key is NULL, or isn't when `not`, for a run bound to
    /// `parameters`.
    fn null(&self, not: bool, parameters: &[Value]) -> RowScope {
        match self {
            Self::Column(column) => RowScope::Null { column: column.clone(), not },
            Self::Expression { expression: RowExpression { term, .. }, .. } => {
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
            Self::Expression { expression: RowExpression { term, affinity }, collation } => row(
                term,
                parameters,
                RowTest::Compare {
                    ordering,
                    value: value.clone(),
                    affinity: *affinity,
                    collation: collation.clone(),
                },
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
    /// Values a last row could hold, for a plan bound to no run's.
    pub(crate) fn any_values(&self) -> Vec<Value> {
        self.positions.iter().map(|_| Affinity::Numeric.any_value()).collect()
    }

    /// The ORDER BY's values in `row`, in its order.
    pub fn values(&self, row: &rusqlite::Row<'_>) -> rusqlite::Result<Vec<Value>> {
        self.positions.iter().map(|position| Ok(row.get_ref(*position)?.into())).collect()
    }
}

impl SortedLimit {
    /// Where a run bound to `values` finds its last row, or `None` when the
    /// LIMIT's value is not a positive integer.
    pub(crate) fn last_row(&self, values: RunValues<'_>) -> Option<LastRow> {
        Some(LastRow {
            limit: self.limit.value(values)?,
            positions: self
                .runs
                .iter()
                .flat_map(|run| &run.terms)
                .map(|term| term.position)
                .collect(),
        })
    }

    /// The bound of `source`'s rows for a run bound to `parameters` whose
    /// last row's ORDER BY values are `last`, when a run sorts it: its first
    /// run's terms at or before their values, or, for each earlier run, a row
    /// it joins of that run's source that sorts before that run's values.
    /// `sources` are the plan's, whose memberships give each source's joins,
    /// and `looked_up` gives the rows whose membership finds a row of its
    /// partner that passes the given scope too. `None` when no run sorts it,
    /// or it takes no value from an earlier run's source.
    pub(crate) fn bound(
        &self,
        source: usize,
        last: &[Value],
        parameters: &[Value],
        sources: &[Source],
        looked_up: impl Fn(&Membership, RowScope) -> RowScope,
    ) -> Option<RowScope> {
        let at = self.runs.iter().position(|run| run.source == source)?;
        let join = |partner| {
            let own = &sources[source].memberships;
            own.iter().find(|found| found.partner == partner).cloned()
        };
        self.run_bound(at, &self.runs[at].terms, last, parameters, join, looked_up)
    }

    /// The bound of `source`'s rows when it is inside a run's derived item:
    /// that run's, over its first terms, with the item's joins read as joins
    /// of `source`'s rows. The arguments are `bound`'s. It narrows what a
    /// write must change in `source`'s table, and no other source looks rows
    /// up within it, so `looked_up` may read every source's scope.
    pub(crate) fn inner_bound(
        &self,
        source: usize,
        last: &[Value],
        parameters: &[Value],
        sources: &[Source],
        looked_up: impl Fn(&Membership, RowScope) -> RowScope,
    ) -> Option<RowScope> {
        let (at, run, inner) = self.runs.iter().enumerate().find_map(|(at, run)| {
            let inner = run.inner.iter().position(|inner| inner.source == source)?;
            Some((at, run, &run.inner[..=inner]))
        })?;
        let join = |partner| {
            let own = &sources[run.source].memberships;
            let found = own.iter().find(|found| found.partner == partner)?;
            // The derived item's join, read down through each source around
            // `source`.
            inner.iter().try_fold(found.clone(), |membership, inner| {
                let column = |column: &str| inner.columns.get(column).cloned();
                let member = membership.member.over(&inner.qualifier, &column)?;
                Some(Membership { member, ..membership })
            })
        };
        let terms = &inner.last()?.terms;
        self.run_bound(at, terms, last, parameters, join, looked_up)
    }

    /// The bound of the rows `terms` sort, the run's at `at` or its first
    /// ones read over a source inside it, where `join` gives their join with
    /// an earlier run's source.
    fn run_bound(
        &self,
        at: usize,
        terms: &[SortTerm],
        last: &[Value],
        parameters: &[Value],
        join: impl Fn(usize) -> Option<Membership>,
        looked_up: impl Fn(&Membership, RowScope) -> RowScope,
    ) -> Option<RowScope> {
        let mut earlier = Vec::new();
        let mut values = last;
        for run in &self.runs[..at] {
            let (own, rest) = values.split_at(run.terms.len());
            values = rest;
            let membership = join(run.source)?;
            if let Some(before) = sorted_before(&run.terms, own, parameters, None) {
                earlier.push(looked_up(&membership, before));
            }
        }
        let at_or_before =
            sorted_before(terms, &values[..terms.len()], parameters, Some(RowScope::All))
                .expect("a tied row passes, so some row does");
        Some(
            earlier
                .into_iter()
                .rev()
                .fold(at_or_before, |later, sooner: RowScope| sooner.or(later)),
        )
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
    // The result columns as SQLite lists them, each `*`'s one by one.
    let Some(outputs) = arm_columns(planner, core, &select.body.select) else {
        return Ok(None);
    };
    let mut runs: Vec<SortRun> = Vec::new();
    // The ORDER BY's terms of each run, with the COLLATE each ends in.
    let mut written: Vec<Vec<(Sorted, Option<String>)>> = Vec::new();
    for term in order_by.iter() {
        if term.nulls.is_some() {
            return Ok(None);
        }
        let Some(found) = sort_term(planner, core, columns, &outputs, &term.expr) else {
            return Ok(None);
        };
        let (position, item, sorted, explicit) = found;
        let from = &planner.items[item];
        if from.core != core || from.left_on.is_some() {
            return Ok(None);
        }
        let Some(key) = sort_key(planner, item, &sorted, explicit.as_deref()) else {
            return Ok(None);
        };
        let term = SortTerm { key, descending: term.order == Some(SortOrder::Desc), position };
        match (runs.last_mut(), written.last_mut()) {
            (Some(run), Some(terms)) if run.source == item => {
                run.terms.push(term);
                terms.push((sorted, explicit));
            }
            _ => {
                runs.push(SortRun { source: item, terms: vec![term], inner: Vec::new() });
                written.push(vec![(sorted, explicit)]);
            }
        }
    }
    // Only the first run over a source bounds its rows, and the sources
    // inside it.
    for (at, terms) in written.iter().enumerate() {
        let (before, rest) = runs.split_at_mut(at);
        let run = &mut rest[0];
        if before.iter().all(|earlier| earlier.source != run.source) {
            run.inner = inner_runs(planner, run.source, &run.terms, terms);
        }
    }
    Ok((!runs.is_empty()).then_some(SortedLimit { runs, limit }))
}

/// What an ORDER BY term reads of one source's rows.
#[derive(Clone)]
enum Sorted {
    /// One of its columns, lowercase.
    Column(String),
    /// An expression over its columns, as a row term of its rows.
    Expression(Arc<RowTerm>),
}

/// The result column, from zero, that returns the ORDER BY term `expr`, the
/// source whose rows it sorts, what it reads of them and the COLLATE it ends
/// in. A bare name that matches a result column's alias means that result
/// column, as SQLite reads an ORDER BY; any other name means the column it
/// resolves to, and any other expression the result column that writes it the
/// same way. A result column may end in a COLLATE too, which doesn't change
/// its value. `outputs` are the result columns `columns` list, each `*`'s one
/// by one.
fn sort_term<'a>(
    planner: &Planner,
    core: usize,
    columns: &'a [ResultColumn<'a>],
    outputs: &[Output<'a>],
    expr: &'a Expr<'a>,
) -> Option<(usize, usize, Sorted, Option<String>)> {
    let (term, explicit) = without_collation(expr);
    let (position, sorted) = match order_term(columns, term) {
        OrderTerm::Alias(listed) => {
            let position = outputs.iter().position(|output| {
                output.written().is_some_and(|written| std::ptr::eq(written, listed))
            })?;
            (position, listed)
        }
        OrderTerm::Expr(term) => {
            let resolved = is_name(term).then(|| planner.resolve(core, term)).flatten();
            let position = outputs.iter().position(|output| {
                let listed = output.written().map(|written| without_collation(written).0);
                match &resolved {
                    // A column a `*` lists has no expression of its own.
                    Some(resolved) => match listed {
                        Some(listed) => {
                            is_name(listed)
                                && planner.resolve(core, listed).as_ref() == Some(resolved)
                        }
                        None => output.passed().as_ref() == Some(resolved),
                    },
                    None => listed == Some(term),
                }
            })?;
            (position, term)
        }
        OrderTerm::Position(_) | OrderTerm::Unknown => return None,
    };
    if is_name(sorted) {
        let (item, column) = planner.resolve(core, sorted)?;
        return Some((position, item, Sorted::Column(column), explicit));
    }
    let mut found = Vec::new();
    parts(sorted, &mut found);
    // A COLLATE inside the term gives a comparison within it a collation of
    // its own, which sqlive doesn't know how to carry to the ORDER BY.
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
    Some((position, item, Sorted::Expression(term), explicit))
}

/// What a term that reads `sorted` of the rows of source `item` sorts them by,
/// ending in the COLLATE `explicit`, when a run's last row can bound them: a
/// stored column sorts in the collation it declares unless the term names
/// another, and any other term in BINARY.
fn sort_key(
    planner: &Planner,
    item: usize,
    sorted: &Sorted,
    explicit: Option<&str>,
) -> Option<SortKey> {
    let explicit = explicit.map(str::to_owned);
    let column = match sorted {
        Sorted::Column(column) => column,
        Sorted::Expression(term) => {
            let expression = RowExpression { term: Arc::clone(term), affinity: Affinity::Blob };
            let collation = explicit.and_then(not_binary);
            return Some(SortKey::Expression { expression, collation });
        }
    };
    let from = &planner.items[item];
    Some(match from.columns.value(column)? {
        ColumnValue::Compared(affinity) => match explicit.and_then(not_binary) {
            None => SortKey::Column(column.clone()),
            // SQLite compares by the term's collation, which sqlive asks it
            // for.
            collation => SortKey::Expression {
                expression: column_expression(from, column, *affinity),
                collation,
            },
        },
        ColumnValue::Generated(generated) => SortKey::Expression {
            expression: generated.clone(),
            collation: explicit.and_then(not_binary),
        },
        ColumnValue::Collated(affinity, declared) => SortKey::Expression {
            expression: column_expression(from, column, *affinity),
            collation: match explicit {
                Some(name) => not_binary(name),
                None => not_binary(declared.clone()),
            },
        },
        ColumnValue::Unknown | ColumnValue::Volatile | ColumnValue::Hidden => return None,
    })
}

/// The sources inside the derived item `item` that the first terms of its
/// run pass down to: `terms`, which read `written`, each with the COLLATE it
/// ends in. A term passes to a source when the item's one arm passes each
/// column it reads on from that source (`derived::only_source`), and so on
/// down through a derived item there. A source takes the run's first terms
/// that all pass to it.
fn inner_runs(
    planner: &Planner,
    item: usize,
    terms: &[SortTerm],
    written: &[(Sorted, Option<String>)],
) -> Vec<InnerRun> {
    let mut found = Vec::new();
    // The source the run's first term passes to, and each term over its rows.
    let mut source = item;
    let mut over: Vec<Sorted> = written.iter().map(|(sorted, _)| sorted.clone()).collect();
    loop {
        let Some(first) = over.first() else { return found };
        let Some(below) = reads(first)
            .first()
            .and_then(|column| only_source(planner, source, column))
            .map(|(below, _)| below)
        else {
            return found;
        };
        let passes = |column: &str| {
            let (source_below, passed) = only_source(planner, source, column)?;
            (source_below == below).then_some(passed)
        };
        let qualifier = planner.items[below].source.qualifier.clone();
        let passed: Vec<Sorted> = over
            .iter()
            .map_while(|sorted| match sorted {
                Sorted::Column(column) => passes(column).map(Sorted::Column),
                Sorted::Expression(term) => term.over(&qualifier, &passes).map(Sorted::Expression),
            })
            .collect();
        let keys: Vec<SortTerm> = passed
            .iter()
            .zip(written)
            .zip(terms)
            .map_while(|((sorted, (_, explicit)), term)| {
                let key = sort_key(planner, below, sorted, explicit.as_deref())?;
                Some(SortTerm { key, ..term.clone() })
            })
            .collect();
        if keys.is_empty() {
            return found;
        }
        let columns: BTreeMap<String, String> = planner.items[source]
            .columns
            .names()
            .filter_map(|column| Some((column.to_owned(), passes(column)?)))
            .collect();
        over = passed;
        over.truncate(keys.len());
        found.push(InnerRun { source: below, qualifier, columns, terms: keys });
        source = below;
    }
}

/// The columns of its source's rows that `sorted` reads.
fn reads(sorted: &Sorted) -> Vec<&str> {
    match sorted {
        Sorted::Column(column) => vec![column],
        Sorted::Expression(term) => term.columns.iter().map(|read| read.column.as_str()).collect(),
    }
}

/// `expr` without the COLLATE it ends in, and the collation that names.
fn without_collation<'a>(expr: &'a Expr<'a>) -> (&'a Expr<'a>, Option<String>) {
    match unparenthesized(expr) {
        Expr::Collate(inner, name) => (unparenthesized(inner), Some(crate::sql::name(name))),
        expr => (expr, None),
    }
}

/// `collation`, unless it is BINARY, which SQLite's comparison uses with no
/// COLLATE.
fn not_binary(collation: String) -> Option<String> {
    (!collation.eq_ignore_ascii_case("BINARY")).then_some(collation)
}

/// A stored column of `from`'s table as an expression a write asks SQLite
/// for, of `affinity`, which the column's values are typed by.
fn column_expression(
    from: &crate::from_item::FromItem,
    column: &str,
    affinity: Affinity,
) -> RowExpression {
    let term = RowTerm {
        sql: crate::sql::identifier(column),
        qualifier: from.source.qualifier.clone(),
        columns: vec![RowColumn::written(column.to_owned(), affinity)],
        passed_to: None,
    };
    RowExpression { term: Arc::new(term), affinity: Affinity::Blob }
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
