//! Rule 7 in a core that aggregates: a result column outside every GROUP BY
//! term takes its value from one row of its group, and `group_concat` joins
//! its values in the order SQLite reads its rows, unless its own ORDER BY
//! sorts by the rowid of each table, so such a core depends on every column
//! of its tables, and so do max of a value that can compare equal to another
//! and differ and a result column that shows a GROUP BY term that can hold
//! such values. SQLite reads `u` and `w` through their indexes on `a` and
//! `c`, which the statements don't always name. Count, and max and a GROUP
//! BY term of an INTEGER column, don't follow that order.

use super::{
    col, eq, item, out, pick, select, table, Core, Expr, Item, Join, Out, Query, Source, COLUMNS,
    TABLES,
};
use crate::random_scopes::Rng;

/// A `group_concat` of a column of `x`, in the order SQLite reads the rows,
/// in the order of the rowid of each item, or in an order with ties.
fn concat(rng: &mut Rng, from: &[Item], alias: Option<String>) -> Out {
    let value = col("x", pick(rng, &COLUMNS));
    let order = match rng.below(3) {
        0 => Vec::new(),
        1 => from.iter().map(|item| (col(&item.alias, "id"), rng.chance(50))).collect(),
        _ => vec![(col("x", pick(rng, &["a", "c"])), rng.chance(50))],
    };
    Out::Concat(value, order, alias)
}

/// A core that aggregates the rows of `x`, and of `y` joined to it.
fn core(rng: &mut Rng) -> Core {
    let mut from = vec![table("x", pick(rng, &TABLES))];
    if rng.chance(30) {
        let join = if rng.chance(50) { Join::Left } else { Join::Inner };
        let on = vec![eq(col("y", "a"), col("x", "id"))];
        from.push(item("y", Source::Table(pick(rng, &TABLES)), join, on));
    }
    let mut conditions = Vec::new();
    if rng.chance(50) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let group = match rng.below(4) {
        0 | 1 => Vec::new(),
        2 => vec![col("x", pick(rng, &COLUMNS))],
        _ => vec![Expr::Add(Box::new(col("x", "a")), Box::new(col("x", "c")))],
    };
    // Each result column has a name, which a statement around the core
    // reads it by.
    let mut names = (0..).map(|position| Some(format!("v{position}")));
    let mut select_ = Vec::new();
    if let Some(grouped) = group.first() {
        if rng.chance(70) {
            select_.push(Out::Expr(grouped.clone(), names.next().unwrap()));
        }
    }
    for _ in 0..1 + rng.below(2) {
        let name = names.next().unwrap();
        select_.push(match rng.below(8) {
            0 | 1 => Out::Expr(col("x", pick(rng, &COLUMNS)), name),
            2 => Out::Count(name),
            3 => Out::Max(col("x", pick(rng, &COLUMNS)), name),
            4 => Out::Max(Expr::Add(Box::new(col("x", "a")), Box::new(col("x", "c"))), name),
            // `max(a, b)` with two arguments is SQLite's scalar max, whose
            // columns sit in no aggregate.
            5 => Out::Expr(
                Expr::Greatest(
                    Box::new(col("x", pick(rng, &COLUMNS))),
                    Box::new(col("x", pick(rng, &COLUMNS))),
                ),
                name,
            ),
            _ => concat(rng, &from, name),
        });
    }
    if group.is_empty() && select_.iter().all(|column| matches!(column, Out::Expr(..))) {
        select_.push(Out::Count(names.next().unwrap()));
    }
    Core { from, conditions, select: select_, group, distinct: false }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let core = core(rng);
    if rng.chance(75) {
        return select(core);
    }
    // The core as a FROM subquery, whose rows the statement reads.
    let shown: Vec<Out> = (0..core.select.len())
        .map(|position| out(Expr::Col("d".to_owned(), ["v0", "v1", "v2", "v3"][position])))
        .collect();
    let from = vec![item("d", Source::Query(Box::new(select(core))), Join::Comma, Vec::new())];
    select(Core { from, conditions: Vec::new(), select: shown, group: Vec::new(), distinct: false })
}
