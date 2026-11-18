//! Rule 7 in a core that calls a window function: `row_number()` numbers the
//! rows of each partition in the order its ORDER BY sorts them, and among
//! those that sort the same in the order SQLite reads them, through the
//! indexes on `u.a` and `w.c` that the statements don't always name. So such
//! a core depends on every column of its tables, unless the window's ORDER
//! BY sorts by the rowid of each table. A condition on the core's rows passes
//! into it nowhere, and a LIMIT over it bounds nothing, since a row's number
//! depends on the core's other rows.

use super::{
    cmp, col, eq, item, lit, out, pick, select, table, Cond, Core, Item, Join, Limit, Out, Query,
    Source, TABLES,
};
use crate::random_scopes::Rng;

/// The columns the core returns before its windows' numbers, by name.
const SHOWN: [&str; 3] = ["c", "id", "a"];

/// A `row_number()` over the items `from`: with or without a partition, and
/// in no order, in the order of each item's rowid, or in an order with ties.
fn row_number(rng: &mut Rng, from: &[Item], name: Option<String>) -> Out {
    let partition = match rng.below(3) {
        0 => Vec::new(),
        _ => vec![col("x", pick(rng, &["a", "c"]))],
    };
    let order = match rng.below(4) {
        0 => Vec::new(),
        1 => from.iter().map(|item| (col(&item.alias, "id"), rng.chance(50))).collect(),
        2 => vec![(col("x", pick(rng, &["a", "c"])), rng.chance(50))],
        _ => vec![(col("x", "a"), false), (col("x", "id"), rng.chance(50))],
    };
    Out::RowNumber(partition, order, name)
}

/// A core over `x`, and `y` joined to it, that numbers its rows.
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
    let mut select_: Vec<Out> = SHOWN
        .iter()
        .map(|column| Out::Expr(col("x", column), Some((*column).to_owned())))
        .collect();
    for position in 0..1 + rng.below(2) {
        select_.push(row_number(rng, &from, Some(format!("r{position}"))));
    }
    Core { from, conditions, select: select_, group: Vec::new() }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let core = core(rng);
    match rng.below(4) {
        0 | 1 => select(core),
        2 => {
            // The core as a FROM subquery, with a condition on its rows.
            let k = rng.below(4) as i64;
            let condition = match rng.below(3) {
                0 => eq(col("d", pick(rng, &SHOWN)), lit(k)),
                1 => cmp("<=", col("d", "r0"), lit(1 + k)),
                _ => Cond::Null(col("d", "a"), false),
            };
            let shown = vec![out(col("d", "id")), out(col("d", "r0"))];
            let from =
                vec![item("d", Source::Query(Box::new(select(core))), Join::Comma, Vec::new())];
            select(Core { from, conditions: vec![condition], select: shown, group: Vec::new() })
        }
        _ => Query {
            // Ordered by the core's own columns with a LIMIT: a row after
            // the last one can still change the numbers before it.
            arms: vec![core],
            order: vec![(0, false), (1, false)],
            order_named: true,
            limit: Some(Limit { count: 1 + rng.below(3) as u32, offset: None }),
            ..Query::default()
        },
    }
}
