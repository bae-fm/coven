//! Rule 6: a statement ordered by its result columns of one table, with a
//! LIMIT, depends on that table's rows up to its last row; and the forms
//! that keep the WHERE's scope: an order by position, by a LEFT JOIN's right
//! side, or by another table's column too, and an OFFSET. Rule 7: an order
//! that doesn't sort by the rowid of each table, as one over a join, makes
//! every column of the tables count as read.

use super::{col, eq, item, out, pick, table, Core, Join, Limit, Query, Source, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let mut from = vec![table("x", first)];
    let mut conditions = Vec::new();
    if rng.chance(60) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let sorted = if rng.chance(15) { "y" } else { "x" };
    match rng.below(3) {
        0 => {}
        1 => from.push(item(
            "y",
            Source::Table(second),
            Join::Inner,
            vec![eq(col("y", "a"), col("x", "id"))],
        )),
        _ => from.push(item(
            "y",
            Source::Table(second),
            Join::Left,
            vec![eq(col("y", "a"), col("x", "id"))],
        )),
    }
    let sorted = if from.len() == 1 { "x" } else { sorted };
    // The result shows only the sorted item's columns, so rows that tie
    // on the order are equal.
    let select_ = vec![out(col(sorted, "c")), out(col(sorted, "id")), out(col(sorted, "a"))];
    let order = match rng.below(4) {
        0 => vec![(0, false)],
        1 => vec![(0, true), (1, false)],
        2 => vec![(2, false), (1, true)],
        _ => vec![(0, false), (1, false)],
    };
    Query {
        arms: vec![Core { from, conditions, select: select_, group: Vec::new() }],
        order,
        order_named: rng.chance(85),
        limit: Some(Limit {
            count: 1 + rng.below(4) as u32,
            offset: rng.chance(20).then(|| rng.below(3) as u32),
        }),
        ..Query::default()
    }
}
