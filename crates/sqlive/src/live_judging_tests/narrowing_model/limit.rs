//! Rule 6: a statement ordered by its result columns, columns or expressions
//! over one table's columns, with a LIMIT, depends on each ordered table's
//! rows up to its last row: the table whose terms come first by its own
//! terms, and a table whose terms come later also by the rows of the tables
//! before it that it joins. And the forms that keep the WHERE's scope: an
//! order by position, by a LEFT JOIN's right side, or by an expression over
//! two tables or over a column sqlive doesn't compare, and an OFFSET.
//! Rule 7: an order that doesn't sort by the rowid of each table, as one over
//! a join, makes every column of the tables count as read.

use super::{
    col, columns, eq, item, lit, out, pick, table, Core, Expr, Join, Limit, Out, Query, Source,
    SEARCH, TABLES,
};
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
    // A condition on `y` decides which item the planner places first.
    if from.len() > 1 && rng.chance(25) {
        conditions.push(super::term::condition(rng, "y", 1));
    }
    let sorted = if from.len() == 1 { "x" } else { sorted };
    let other = (from.len() > 1).then(|| if sorted == "x" { "y" } else { "x" });
    // The result shows the sorted item's columns and an expression over
    // them, or over another item's column too, then the other item's
    // columns, by which an order across the join sorts.
    let mut select_ = vec![
        out(col(sorted, "c")),
        out(col(sorted, "id")),
        out(col(sorted, "a")),
        out(expression(rng, sorted, other)),
    ];
    if let Some(other) = other {
        select_.push(out(col(other, "c")));
        select_.push(out(col(other, "id")));
    }
    // The text column `s` of the sorted item, when its table has one, which
    // an ORDER BY term sorts by the collation `fold`.
    let sorted_table = if sorted == "x" { first } else { second };
    let text = (sorted_table != SEARCH).then(|| {
        select_.push(out(col(sorted, "s")));
        select_.len() - 1
    });
    let mut collate = Vec::new();
    let order = match rng.below(if other.is_some() { 11 } else { 6 }) {
        0 => vec![(0, false)],
        1 => vec![(0, true), (1, false)],
        2 => vec![(2, false), (1, true)],
        3 => vec![(3, false), (1, false)],
        4 => vec![(3, true), (0, false), (1, true)],
        5 => vec![(0, false), (1, false)],
        // Orders across the join: the other item's terms first, or the
        // sorted item's, or the other's on both sides of the sorted item's.
        6 => vec![(4, false), (0, false), (1, true)],
        7 => vec![(0, true), (4, false), (5, false)],
        8 => vec![(4, true), (5, false), (3, false), (1, false)],
        9 => vec![(2, false), (1, false), (4, true)],
        _ => vec![(4, false), (0, true), (5, false)],
    };
    let order = match text {
        // Orders by `s`, which declares `fold` or has it written, ascending
        // or descending, alone with the rowid or among the other terms.
        Some(text) if rng.chance(40) => {
            collate.push((text, rng.chance(50)));
            let mut order = match rng.below(4) {
                0 => vec![(text, false), (1, false)],
                1 => vec![(text, true), (1, false)],
                2 => vec![(2, false), (text, rng.chance(50)), (1, false)],
                _ => vec![(1, true), (text, false), (3, false)],
            };
            // An order across a join tells its rows apart by the other
            // item's rowid too.
            if other.is_some() {
                order.push((5, false));
            }
            order
        }
        _ => order,
    };
    // Either item read through FROM subqueries that pass its columns on, as
    // a spread fragment's, whose table takes the run's bound too.
    for (at, name) in [first, second].into_iter().enumerate().take(from.len()) {
        if rng.chance(30) {
            let mut source = through(rng, Source::Table(name), name);
            if rng.chance(30) {
                source = through(rng, source, name);
            }
            from[at].source = source;
        }
    }
    Query {
        arms: vec![Core { from, conditions, select: select_, group: Vec::new(), distinct: false }],
        order,
        collate,
        order_named: rng.chance(85),
        limit: Some(Limit {
            count: 1 + rng.below(4) as u32,
            offset: rng.chance(20).then(|| rng.below(3) as u32),
        }),
        ..Query::default()
    }
}

/// A FROM subquery that returns each column of `source`, which has the
/// columns of the table `name`: one that passes them all on, or one that
/// computes a column, groups its rows, adds a LEFT JOIN or keeps its first
/// rows by a LIMIT, from which a column or all of them pass no further.
fn through(rng: &mut Rng, source: Source, name: &str) -> Source {
    let shape = rng.below(10);
    let computed = (shape == 0).then(|| pick(rng, &["a", "c", "id"]));
    let select_ = columns(name)
        .iter()
        .map(|column| {
            let value = if computed == Some(*column) {
                Expr::Add(Box::new(col("g", column)), Box::new(lit(0)))
            } else {
                col("g", column)
            };
            Out::Expr(value, Some((*column).to_owned()))
        })
        .collect();
    let mut from = vec![item("g", source, Join::Comma, Vec::new())];
    if shape == 1 {
        let on = vec![eq(col("j", "id"), col("g", "a"))];
        from.push(item("j", Source::Table("w"), Join::Left, on));
    }
    let mut conditions = Vec::new();
    if rng.chance(25) {
        conditions.push(super::term::condition(rng, "g", 1));
    }
    let group = match shape {
        // One row of each rowid, each group one row, which passes on only
        // the columns the GROUP BY lists.
        2 => vec![col("g", "c"), col("g", "id")],
        3 => vec![col("g", "id")],
        _ => Vec::new(),
    };
    Source::Query(Box::new(Query {
        arms: vec![Core { from, conditions, select: select_, group, distinct: false }],
        // The first rows by the rowid.
        order: if shape == 4 { vec![(0, false)] } else { Vec::new() },
        limit: (shape == 4).then(|| super::limit(3)).flatten(),
        ..Query::default()
    }))
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
