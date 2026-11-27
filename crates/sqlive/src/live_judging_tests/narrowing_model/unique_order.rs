//! Rule 7: a LIMIT whose ORDER BY and joins tell its rows apart, by a unique
//! key of each table and by FROM subqueries that return one row at most to
//! each, depends only on the columns it names. And the statements that leave
//! rows tied, which depend on every column of the tables they read: by part of
//! a key, by a key that can hold NULL, by an equality that converts a key's
//! values, or by a subquery that can return several rows.

use super::{
    cmp, col, eq, item, lit, out, pick, select, table, Cond, Core, Item, Join, Limit, Out, Query,
    Source, KEYED,
};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &[KEYED, KEYED, "t"]);
    let mut from = vec![table("x", first)];
    let mut conditions = Vec::new();
    if rng.chance(40) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    if rng.chance(15) {
        conditions.push(eq(col("x", "a"), lit(rng.below(4) as i64)));
    }
    let mut select_ =
        vec![out(col("x", "id")), out(col("x", "a")), out(col("x", "b")), out(col("x", "c"))];
    for alias in ["y", "z"].into_iter().take(rng.below(3)) {
        let before = if alias == "z" && rng.chance(50) { "y" } else { "x" };
        // A FROM subquery's columns are `id` and `a`.
        let before = if before == "y" && from.len() < 2 { "x" } else { before };
        let (joined, column) = join(rng, alias, before);
        from.push(joined);
        select_.push(out(col(alias, column)));
    }
    // Result column positions: x's id, a, b and c, then one of each other
    // item.
    let mut order: Vec<usize> = match rng.below(6) {
        0 => vec![0],
        1 => vec![1, 3],
        2 => vec![3, 1],
        3 => vec![2, 1],
        4 => vec![1, 3, 0],
        _ => vec![1],
    };
    if select_.len() > 4 && rng.chance(25) {
        order.push(4);
    }
    Query {
        arms: vec![Core { from, conditions, select: select_, group: Vec::new(), distinct: false }],
        order: order.into_iter().map(|position| (position, rng.chance(25))).collect(),
        order_named: rng.chance(85),
        limit: Some(Limit {
            count: 1 + rng.below(4) as u32,
            offset: rng.chance(15).then(|| rng.below(3) as u32),
        }),
        ..Query::default()
    }
}

/// An item named `alias` joined to `before`, which has the columns `id` and
/// `a`, and a column of it for the result.
fn join(rng: &mut Rng, alias: &str, before: &str) -> (Item, &'static str) {
    let kind = if rng.chance(60) { Join::Left } else { Join::Inner };
    // `before` can be a FROM subquery, so its columns here are `id` and `a`.
    let other = |column| col(before, column);
    let own = |column| col(alias, column);
    let (source, mut on, returned): (Source, Vec<Cond>, &'static str) = match rng.below(9) {
        // The whole of the key (a, c), by `c` of the first item.
        0 => {
            (Source::Table(KEYED), vec![eq(own("a"), other("a")), eq(own("c"), col("x", "c"))], "c")
        }
        // Part of it.
        1 => (Source::Table(KEYED), vec![eq(own("a"), other("a"))], "c"),
        // A rowid.
        2 => (Source::Table(pick(rng, &["t", "u", KEYED])), vec![eq(own("id"), other("a"))], "c"),
        // The key (b, a), whose `b` is untyped: an equality with a numeric
        // value converts its values, and one with the untyped `b` doesn't.
        3 => (
            Source::Table(KEYED),
            vec![
                eq(own("b"), if rng.chance(50) { col("x", "b") } else { other("a") }),
                eq(own("a"), other("id")),
            ],
            "c",
        ),
        // One row of each group.
        4 => (
            Source::Query(Box::new(select(Core {
                from: vec![table("g", pick(rng, &["t", KEYED]))],
                conditions: Vec::new(),
                select: vec![
                    Out::Expr(col("g", "a"), Some("id".to_owned())),
                    Out::Count(Some("a".to_owned())),
                ],
                group: vec![col("g", "a")],
                distinct: false,
            }))),
            vec![eq(own("id"), other("a"))],
            "a",
        ),
        // One row of each rowid, or by `a`, several.
        5 | 6 => (
            Source::Query(Box::new(select(Core {
                from: vec![table("g", "u")],
                conditions: Vec::new(),
                select: vec![
                    Out::Expr(col("g", "id"), Some("id".to_owned())),
                    Out::Expr(col("g", "a"), Some("a".to_owned())),
                ],
                group: Vec::new(),
                distinct: false,
            }))),
            vec![eq(own(if rng.chance(70) { "id" } else { "a" }), other("a"))],
            "a",
        ),
        // One row in all.
        7 => (
            Source::Query(Box::new(select(Core {
                from: vec![table("g", "w")],
                conditions: Vec::new(),
                select: vec![Out::Count(Some("id".to_owned())), Out::Count(Some("a".to_owned()))],
                group: Vec::new(),
                distinct: false,
            }))),
            vec![eq(own("id"), other("a"))],
            "a",
        ),
        // A key the WHERE or the ON names by a constant.
        _ => (
            Source::Table(KEYED),
            vec![eq(own("a"), lit(rng.below(4) as i64)), eq(own("c"), other("id"))],
            "c",
        ),
    };
    if rng.chance(20) {
        on.push(cmp("<", other("id"), lit(1 + rng.below(5) as i64)));
    }
    (item(alias, source, kind, on), returned)
}
