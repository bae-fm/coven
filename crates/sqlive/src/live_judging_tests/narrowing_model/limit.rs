//! Rule 6: a statement ordered by its result columns of one table, columns
//! or expressions over them, with a LIMIT, depends on that table's rows up to
//! its last row; and the forms that keep the WHERE's scope: an order by
//! position, by a LEFT JOIN's right side, by another table's column too, or
//! by an expression over a column sqlive doesn't compare, and an OFFSET.
//! Rule 7: an order that doesn't sort by the rowid of each table, as one over
//! a join, makes every column of the tables count as read.

use super::{col, eq, item, lit, out, pick, table, Core, Expr, Join, Limit, Query, Source, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let mut from = vec![table("x", first)];
    let mut conditions = Vec::new();
    if rng.chance(60) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    if rng.chance(15) {
        // `max(a, c)` with two arguments is SQLite's scalar max, so the core
        // makes a row of each row of `x`, and the LIMIT keeps the rows SQLite
        // reads first.
        let greatest = Expr::Greatest(Box::new(col("x", "a")), Box::new(col("x", "c")));
        return Query {
            arms: vec![Core {
                from,
                conditions,
                select: vec![out(greatest)],
                group: Vec::new(),
                distinct: false,
            }],
            limit: super::limit(1 + rng.below(3) as u32),
            ..Query::default()
        };
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
    let other = (from.len() > 1).then(|| if sorted == "x" { "y" } else { "x" });
    // The result shows the sorted item's columns and an expression over
    // them, or over another item's column too.
    let select_ = vec![
        out(col(sorted, "c")),
        out(col(sorted, "id")),
        out(col(sorted, "a")),
        out(expression(rng, sorted, other)),
    ];
    let order = match rng.below(6) {
        0 => vec![(0, false)],
        1 => vec![(0, true), (1, false)],
        2 => vec![(2, false), (1, true)],
        3 => vec![(3, false), (1, false)],
        4 => vec![(3, true), (0, false), (1, true)],
        _ => vec![(0, false), (1, false)],
    };
    Query {
        arms: vec![Core { from, conditions, select: select_, group: Vec::new(), distinct: false }],
        order,
        order_named: rng.chance(85),
        limit: Some(Limit {
            count: 1 + rng.below(4) as u32,
            offset: rng.chance(20).then(|| rng.below(3) as u32),
        }),
        ..Query::default()
    }
}

/// An expression over `sorted`'s columns: of the INTEGER `a` and `c` and the
/// untyped `b`, whose values sqlive compares as they are, or of the NOCASE
/// `n` or of the `other` item's column too, which keep the WHERE's scope.
fn expression(rng: &mut Rng, sorted: &str, other: Option<&str>) -> Expr {
    let add = |left, right| Expr::Add(Box::new(left), Box::new(right));
    match rng.below(7) {
        0 => add(col(sorted, "a"), col(sorted, "c")),
        1 => Expr::Coalesce(Box::new(col(sorted, "a")), Box::new(lit(-1))),
        2 => Expr::Greatest(Box::new(col(sorted, "a")), Box::new(col(sorted, "c"))),
        3 => add(col(sorted, "b"), lit(1)),
        4 => Expr::Coalesce(Box::new(add(col(sorted, "a"), lit(2))), Box::new(col(sorted, "c"))),
        5 => add(col(sorted, "n"), lit(0)),
        _ => add(col(sorted, "a"), col(other.unwrap_or(sorted), "c")),
    }
}
