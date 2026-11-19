//! A column equal to a value that is one of several partners' values:
//! COALESCE of another item's columns and expressions, a subquery's value
//! and constants, in an inner join's ON, the WHERE and a LEFT JOIN's ON; and
//! a column equal to or IN a compound whose arms return a column of their own
//! item or a constant. An argument or an arm of another shape, as a NULL,
//! the member's own column or an expression an arm returns, narrows nothing.

use super::{col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

fn core(from: Vec<super::Item>, conditions: Vec<Cond>, select_: Vec<super::Out>) -> Core {
    Core { from, conditions, select: select_, group: Vec::new(), distinct: false }
}

/// One argument of COALESCE beside the member `member`, taking values from
/// `partner`.
fn argument(rng: &mut Rng, member: &str, partner: &str) -> Expr {
    match rng.below(12) {
        0..=4 => super::edge::value(rng, partner, &COLUMNS),
        5 | 6 => lit(rng.below(4) as i64),
        7 | 8 => {
            let mut conditions = vec![super::term::condition(rng, "q", 1)];
            if rng.chance(30) {
                conditions.push(eq(col("q", "c"), col(member, "c")));
            }
            Expr::Sub(Box::new(select(core(
                vec![table("q", pick(rng, &TABLES))],
                conditions,
                vec![out(col("q", pick(rng, &COLUMNS)))],
            ))))
        }
        9 => Expr::Lit(None),
        10 => col(member, pick(rng, &COLUMNS)),
        _ => Expr::Coalesce(
            Box::new(super::edge::value(rng, partner, &COLUMNS)),
            Box::new(lit(rng.below(4) as i64)),
        ),
    }
}

/// `member.col = COALESCE(...)`, either side first.
fn coalesced(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let mut value = argument(rng, member, partner);
    for _ in 0..1 + rng.below(2) {
        value = Expr::Coalesce(Box::new(argument(rng, member, partner)), Box::new(value));
    }
    let column = col(member, pick(rng, &COLUMNS));
    if rng.chance(50) {
        eq(column, value)
    } else {
        eq(value, column)
    }
}

/// One arm of a compound.
fn arm(rng: &mut Rng) -> Core {
    match rng.below(8) {
        0 => core(Vec::new(), Vec::new(), vec![out(lit(rng.below(4) as i64))]),
        1 => core(
            vec![table("p", pick(rng, &TABLES))],
            Vec::new(),
            vec![out(Expr::Add(Box::new(col("p", "a")), Box::new(lit(1))))],
        ),
        _ => {
            let mut conditions = Vec::new();
            if rng.chance(70) {
                conditions.push(super::term::condition(rng, "p", 1));
            }
            core(
                vec![table("p", pick(rng, &TABLES))],
                conditions,
                vec![out(col("p", pick(rng, &COLUMNS)))],
            )
        }
    }
}

/// A column of `member` compared with a compound of two or three arms.
fn compound(rng: &mut Rng, member: &str) -> Cond {
    let mut query = Query { arms: vec![arm(rng)], ..Query::default() };
    for _ in 0..1 + rng.below(2) {
        query.arms.push(arm(rng));
        query.ops.push(pick(rng, &["UNION ALL", "UNION"]));
    }
    let column = col(member, pick(rng, &COLUMNS));
    match rng.below(4) {
        0 => eq(column, Expr::Sub(Box::new(query))),
        _ => Cond::InQuery(column, Box::new(query), false),
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let mut select_ = super::term::columns("x");
    select_.push(out(col("y", "c")));
    let mut where_ = Vec::new();
    if rng.chance(60) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    let (join, on) = match rng.below(4) {
        0 => (Join::Inner, vec![coalesced(rng, "y", "x")]),
        1 => (Join::Left, vec![coalesced(rng, "y", "x")]),
        2 => {
            where_.push(coalesced(rng, "y", "x"));
            (Join::Comma, Vec::new())
        }
        _ => {
            where_.push(compound(rng, "y"));
            (Join::Inner, vec![eq(col("y", "a"), col("x", "id"))])
        }
    };
    if join == Join::Left && rng.chance(40) {
        where_.push(super::term::condition(rng, "y", 1));
    }
    select(core(
        vec![table("x", first), item("y", Source::Table(second), join, on)],
        where_,
        select_,
    ))
}
