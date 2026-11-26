//! A subquery that keeps the first rows of each window, as a correlated
//! `ORDER BY ... LIMIT` does for each row around it (§4).
//!
//! A subquery that is one SELECT of one table, ordered by expressions over
//! that table's columns, with a LIMIT and no OFFSET, returns for each row of
//! the query around it the first rows of that row's window: the table's rows
//! that the subquery's own conditions pass and that hold, in each column an
//! equality compares with a column around the subquery, that row's value. A
//! row that sorts after the LIMIT-th row of its window changes nothing the
//! subquery returns, wherever it sorts in another row's window. So the
//! table's scope also requires a changed row to hold no NULL in those
//! columns, and fewer than LIMIT rows of its window to sort before it. A
//! write asks SQLite for the first LIMIT rows of the changed row's window,
//! as the subquery orders them, and whether the changed row sorts before the
//! last of them, in the database the write's lookups read; the run keeps no
//! value. Lean's `Rules/Window.lean` states this rule and `Windows.lean`
//! proves it.
//!
//! Each condition of the subquery is one of the two kinds, so the rows its
//! conditions keep for a row around it are exactly that row's window: a
//! condition that names only the table's columns, which SQLite can evaluate
//! row by row (`scope_row::item_row_term`), or an equality of the table's
//! column with a value around the subquery that the planner reads as an edge
//! (`scope_edge.rs`). Each ORDER BY term is an expression over the table's
//! columns, of the same kind, with no COLLATE and no NULLS FIRST or LAST, and
//! no term is a position: Lean models an ORDER BY that writes its terms. The
//! SELECT neither groups, calls an aggregate or window function, nor is
//! DISTINCT, since there one row depends on many. A comparison that takes the
//! subquery's value (`scope_subquery.rs`) takes it from a row the subquery
//! returns, which sits inside its window, so the window narrows the
//! subquery's table there too. The window narrows only the table's own
//! scope: the lookups of other sources read the table's rows as its
//! conditions narrow them.

use std::sync::Arc;

use crate::row_scope::{RowImage, RowScope};
use crate::scope_edge::MemberValue;
use crate::scope_order::{ordered_limit, OrderedLimit, RunLimit};
use crate::scope_plan::{is_name, Planner};
use crate::scope_row::{item_row_term, typed, RowTerm};
use crate::scope_term::RunValues;
use crate::select_syntax::{conjuncts, order_term, parts, OrderTerm, Part};
use crate::sql::unparenthesized;
use crate::sql_value::SqlValue as Value;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Operator, Select, SortOrder};

/// A subquery whose rows, for each row around it, are the first rows of
/// that row's window, before the run's values.
#[derive(Clone, Debug)]
pub(crate) struct Window {
    /// The table item the subquery's one core reads.
    pub(crate) item: usize,
    /// The table as sqlive's SQL names it, with its qualifier.
    from: String,
    /// How the statement names the table's rows.
    qualifier: String,
    /// The conditions on the table's row alone, as SQL.
    own: Vec<String>,
    /// The columns, lowercase, that an equality compares with a value
    /// around the subquery.
    partition: Vec<String>,
    /// The ORDER BY's terms, each descending when `true`.
    keys: Vec<(Arc<RowTerm>, bool)>,
    limit: RunLimit,
}

/// The window of `select`, a subquery whose one core is `core`, when its
/// rows for each row around it are the first rows of a window.
pub(crate) fn read_window(
    connection: &Connection,
    planner: &Planner,
    select: &Select<'_>,
    core: usize,
) -> rusqlite::Result<Option<Window>> {
    let Some(OrderedLimit { columns, where_clause, distinct, order_by, limit }) =
        ordered_limit(connection, planner.functions, select)?
    else {
        return Ok(None);
    };
    let [item] = planner.cores[core].items[..] else {
        return Ok(None);
    };
    let source = &planner.items[item].source;
    let (Some(_), Some(from)) = (source.table(), &source.from) else {
        return Ok(None);
    };
    let mut found = Vec::new();
    for term in order_by.iter() {
        parts(&term.expr, &mut found);
    }
    if distinct || found.iter().any(|part| part.many_rows(planner.functions)) {
        return Ok(None);
    }
    let mut keys = Vec::new();
    for term in order_by.iter() {
        let sorted = match order_term(columns, &term.expr) {
            OrderTerm::Alias(_, listed) => listed,
            OrderTerm::Expr(expr) => expr,
            OrderTerm::Position(_) | OrderTerm::Unknown => return Ok(None),
        };
        let mut found = Vec::new();
        parts(sorted, &mut found);
        // A COLLATE gives the ORDER BY its collation, which Lean's model
        // doesn't hold.
        if term.nulls.is_some() || found.iter().any(|part| matches!(part, Part::Collation)) {
            return Ok(None);
        }
        let Some(key) = own_term(planner, core, item, sorted, &found) else {
            return Ok(None);
        };
        keys.push((key, term.order == Some(SortOrder::Desc)));
    }
    let mut conditions = Vec::new();
    if let Some(predicate) = where_clause {
        conjuncts(predicate, &mut conditions);
    }
    let mut own = Vec::new();
    let mut partition = Vec::new();
    for condition in conditions {
        let mut found = Vec::new();
        parts(condition, &mut found);
        if let Some(term) = own_term(planner, core, item, condition, &found) {
            own.push(term.sql.clone());
        } else if let Some(column) = partition_column(planner, core, item, condition) {
            partition.push(column);
        } else {
            return Ok(None);
        }
    }
    Ok(Some(Window {
        item,
        from: from.clone(),
        qualifier: source.qualifier.clone(),
        own,
        partition,
        keys,
        limit,
    }))
}

/// The term `expr`, written in `core`, gives `item`'s rows when its every
/// name is a column of `item` that sqlive compares and SQLite can evaluate
/// it row by row; `found` are its parts.
fn own_term(
    planner: &Planner,
    core: usize,
    item: usize,
    expr: &Expr<'_>,
    found: &[Part<'_>],
) -> Option<Arc<RowTerm>> {
    let own = found.iter().all(|part| match part {
        Part::Name(name) => planner.resolve(core, name).is_some_and(|(named, _)| named == item),
        _ => true,
    });
    own.then(|| item_row_term(planner, core, item, expr, found)).flatten()
}

/// The column of `item` that `condition`, written in `core`, compares by `=`
/// with a value of a source around the core, when the planner reads the
/// equality as an edge of that column.
fn partition_column(
    planner: &Planner,
    core: usize,
    item: usize,
    condition: &Expr<'_>,
) -> Option<String> {
    let Expr::Binary(left, Operator::Equals, right) = unparenthesized(condition) else {
        return None;
    };
    [(left, right, false), (right, left, true)].into_iter().find_map(|(member, other, first)| {
        if !is_name(member) {
            return None;
        }
        let edge = planner.edge(core, member, other, first)?;
        match &edge.member.value {
            MemberValue::Column(column)
                if edge.member.item == item && planner.items[edge.partner].core != core =>
            {
                Some(column.clone())
            }
            _ => None,
        }
    })
}

impl Window {
    /// The scope this window gives its table for a run bound to `values`, or
    /// `None` when the LIMIT's value is not a positive integer.
    pub(crate) fn bind(&self, values: RunValues<'_>) -> Option<RowScope> {
        let limit = self.limit.value(values)?;
        Some(RowScope::Window(Arc::new(RowWindow {
            window: self.clone(),
            limit,
            parameters: values.parameters().to_vec(),
        })))
    }
}

/// A run's window: the rows that hold no NULL in the window's partition
/// columns and that fewer than `limit` rows of their window sort before.
#[derive(Debug)]
pub struct RowWindow {
    window: Window,
    limit: usize,
    /// The run's parameters, which the conditions and terms name.
    parameters: Vec<Value>,
}

/// Whether a changed row passes a window, or the question that tells.
pub(crate) enum WindowQuestion {
    Answered(bool),
    /// The SQL that finds a row when the row passes, and its parameters.
    Ask(String, Vec<Value>),
}

impl RowWindow {
    /// The columns, lowercase, whose values in a changed row decide whether
    /// it passes.
    pub(crate) fn columns(&self) -> impl Iterator<Item = &String> {
        let window = &self.window;
        window
            .partition
            .iter()
            .chain(window.keys.iter().flat_map(|(term, _)| term.columns.iter().map(|(c, _)| c)))
    }

    /// Whether the row `image` passes, or what to ask SQLite. A row with
    /// NULL in a partition column sits in no row's window, as `=` holds of
    /// no NULL. A row the image lacks a column of, or whose value no cast
    /// would keep as it is in its column's type, passes. The question
    /// orders the first LIMIT rows of the row's window with the row itself,
    /// which goes before the rows it ties with, and finds the row among the
    /// first LIMIT: then fewer than LIMIT rows sort before it.
    pub(crate) fn question(&self, image: &RowImage) -> WindowQuestion {
        let window = &self.window;
        let qualifier = &window.qualifier;
        let mut bound = self.parameters.clone();
        let mut conditions: Vec<String> = window.own.iter().map(|sql| format!("({sql})")).collect();
        for column in &window.partition {
            match image.get(column) {
                None => return WindowQuestion::Answered(true),
                Some(Value::Null) => return WindowQuestion::Answered(false),
                Some(value) => {
                    bound.push(value.clone());
                    conditions.push(format!(
                        "{qualifier}.{} = ?{}",
                        crate::sql::identifier(column),
                        bound.len()
                    ));
                }
            }
        }
        let mut row: Vec<String> = Vec::new();
        let mut named: Vec<&str> = Vec::new();
        for (term, _) in &window.keys {
            for (column, affinity) in &term.columns {
                if named.contains(&column.as_str()) {
                    continue;
                }
                let Some(value) = image.get(column) else {
                    return WindowQuestion::Answered(true);
                };
                let Some(typed) = typed(&format!("?{}", bound.len() + 1), *affinity, value) else {
                    return WindowQuestion::Answered(true);
                };
                bound.push(value.clone());
                row.push(format!("{typed} AS {}", crate::sql::identifier(column)));
                named.push(column);
            }
        }
        let keys: Vec<String> = window
            .keys
            .iter()
            .enumerate()
            .map(|(position, (term, _))| format!("({}) AS sqlive_key{position}", term.sql))
            .collect();
        let order: Vec<String> = window
            .keys
            .iter()
            .enumerate()
            .map(|(position, (_, descending))| {
                format!("sqlive_key{position}{}", if *descending { " DESC" } else { "" })
            })
            .collect();
        let (keys, order, limit) = (keys.join(", "), order.join(", "), self.limit);
        let condition =
            if conditions.is_empty() { "1".to_owned() } else { conditions.join(" AND ") };
        let sql = format!(
            "SELECT 1 FROM (\
             SELECT * FROM (SELECT 0 AS sqlive_changed, {keys} FROM {from} WHERE {condition} \
             ORDER BY {order} LIMIT {limit}) \
             UNION ALL SELECT 1, {keys} FROM (SELECT {row}) AS {qualifier} \
             ORDER BY {order}, sqlive_changed DESC LIMIT {limit}\
             ) WHERE sqlive_changed = 1",
            from = window.from,
            row = row.join(","),
        );
        WindowQuestion::Ask(sql, bound)
    }
}
