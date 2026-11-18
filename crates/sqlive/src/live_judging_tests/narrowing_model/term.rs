//! Rule 1: conditions on one item's own columns, alone or beside other
//! items, on a LEFT JOIN's right side, and on an outer item inside a
//! subquery, where they narrow nothing.

use super::{
    cmp, col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    COLUMNS, TABLES,
};
use crate::random_scopes::Rng;

/// A condition on the columns of the item `alias`: the forms sqlive reads as
/// its own terms, and others it asks SQLite to judge row by row.
pub(super) fn condition(rng: &mut Rng, alias: &str, depth: usize) -> Cond {
    if depth < 2 && rng.chance(25) {
        let left = Box::new(condition(rng, alias, depth + 1));
        let right = Box::new(condition(rng, alias, depth + 1));
        return if rng.chance(50) { Cond::And(left, right) } else { Cond::Or(left, right) };
    }
    let column = col(alias, pick(rng, &COLUMNS));
    let k = rng.below(4) as i64;
    match rng.below(15) {
        0..=3 => cmp(pick(rng, &["=", "<", "<=", ">", ">="]), column, lit(k)),
        4 => cmp(pick(rng, &["=", "<", ">="]), lit(k), column),
        5 => Cond::InList(column, vec![lit(k), lit(rng.below(4) as i64)]),
        6 => Cond::Null(column, false),
        7 => Cond::Null(column, true),
        8 => cmp("IS", column, lit(k)),
        9 => cmp("<>", column, lit(k)),
        10 => cmp(">", Expr::Add(Box::new(column), Box::new(lit(1))), lit(k)),
        11 => eq(col(alias, "a"), col(alias, "c")),
        12 => Cond::Not(Box::new(eq(column, lit(k)))),
        13 => eq(column, Expr::Lit(None)),
        _ => cmp("IS NOT", column, lit(k)),
    }
}

/// One to two conditions on `alias`.
pub(super) fn conditions(rng: &mut Rng, alias: &str) -> Vec<Cond> {
    (0..1 + rng.below(2)).map(|_| condition(rng, alias, 0)).collect()
}

/// The columns `alias`'s rows show.
pub(super) fn columns(alias: &str) -> Vec<Out> {
    vec![out(col(alias, "id")), out(col(alias, "a")), out(col(alias, "b"))]
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    match rng.below(4) {
        0 => select(Core {
            from: vec![table("x", first)],
            conditions: conditions(rng, "x"),
            select: columns("x"),
            group: Vec::new(),
        }),
        1 => {
            let mut where_ = conditions(rng, "x");
            where_.extend(conditions(rng, "y"));
            let mut select_ = columns("x");
            select_.extend(columns("y"));
            select(Core {
                from: vec![table("x", first), table("y", second)],
                conditions: where_,
                select: select_,
                group: Vec::new(),
            })
        }
        2 => {
            // A LEFT JOIN: a term on its right side in its ON narrows that
            // side, and one in the WHERE makes the join inner unless a row
            // of NULLs can pass it.
            let mut on = vec![eq(col("y", "a"), col("x", "id"))];
            if rng.chance(40) {
                on.push(condition(rng, "y", 1));
            }
            let mut where_ = conditions(rng, "x");
            if rng.chance(60) {
                where_.push(condition(rng, "y", 1));
            }
            let mut select_ = columns("x");
            select_.push(out(col("y", "c")));
            select(Core {
                from: vec![table("x", first), item("y", Source::Table(second), Join::Left, on)],
                conditions: where_,
                select: select_,
                group: Vec::new(),
            })
        }
        _ => {
            // A condition on the outer item alone inside a subquery narrows
            // nothing; the subquery's own narrows its table.
            let inner = select(Core {
                from: vec![table("q", second)],
                conditions: vec![condition(rng, "x", 1), condition(rng, "q", 1)],
                select: vec![out(lit(1))],
                group: Vec::new(),
            });
            let mut where_ = conditions(rng, "x");
            where_.push(Cond::Exists(Box::new(inner), rng.chance(30)));
            select(Core {
                from: vec![table("x", first)],
                conditions: where_,
                select: columns("x"),
                group: Vec::new(),
            })
        }
    }
}
