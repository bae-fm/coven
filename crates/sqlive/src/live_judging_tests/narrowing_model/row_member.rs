//! An expression over one item's own row compared with a value of another
//! item, which narrows the first to the rows whose expression's value some
//! row of the other gives: CASE, `iif`, COALESCE, scalar `max`, `IS NULL`,
//! arithmetic and the app's `nz`, in an inner join's ON, the WHERE, a LEFT
//! JOIN's ON, by `IS`, in a row value and inside a subquery. An expression
//! that can be other than NULL where its columns are narrows nothing over a
//! LEFT JOIN's item, and one over the NOCASE `n` narrows nothing.

use super::{col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

/// An expression over the columns of `alias`.
fn expression(rng: &mut Rng, alias: &str) -> Expr {
    let column = |rng: &mut Rng| Box::new(col(alias, pick(rng, &["id", "a", "b", "c"])));
    match rng.below(9) {
        0 => Expr::Iif { test: column(rng), yes: column(rng), no: column(rng), iif: false },
        1 => Expr::Iif { test: column(rng), yes: column(rng), no: column(rng), iif: true },
        2 => Expr::Coalesce(column(rng), column(rng)),
        3 => Expr::Greatest(column(rng), column(rng)),
        4 => Expr::IsNull(column(rng)),
        5 => Expr::App(column(rng)),
        6 => Expr::Add(Box::new(col(alias, "n")), Box::new(lit(0))),
        _ => Expr::Add(column(rng), Box::new(lit(rng.below(2) as i64))),
    }
}

/// A value of `partner`: a column, or a column plus one.
fn value(rng: &mut Rng, partner: &str) -> Expr {
    let column = col(partner, pick(rng, &COLUMNS));
    if rng.chance(25) {
        Expr::Add(Box::new(column), Box::new(lit(1)))
    } else {
        column
    }
}

/// An expression of `member`'s row compared with a value of `partner`:
/// by `=` or `IS`, either side first, or in a row value beside a column
/// pair.
fn comparing(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let own = expression(rng, member);
    let other = value(rng, partner);
    let (left, right) = if rng.chance(50) { (own, other) } else { (other, own) };
    match rng.below(5) {
        0 => Cond::Cmp("IS", left, right),
        1 => Cond::RowEq(
            vec![left, col(member, pick(rng, &COLUMNS))],
            vec![right, col(partner, pick(rng, &COLUMNS))],
        ),
        _ => eq(left, right),
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let mut select_ = super::term::columns("x");
    let mut where_ = Vec::new();
    if rng.chance(70) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    let mut from = vec![table("x", first)];
    match rng.below(4) {
        0 => {
            from.push(item(
                "y",
                Source::Table(second),
                Join::Inner,
                vec![comparing(rng, "y", "x")],
            ));
            select_.push(out(col("y", "c")));
        }
        1 => {
            from.push(item("y", Source::Table(second), Join::Left, vec![comparing(rng, "y", "x")]));
            if rng.chance(40) {
                where_.push(super::term::condition(rng, "y", 1));
            }
            select_.push(out(col("y", "c")));
        }
        2 => {
            from.push(table("y", second));
            where_.push(comparing(rng, "y", "x"));
            select_.push(out(col("y", "c")));
        }
        _ => {
            // Inside a subquery, the expression takes values from the item
            // around it.
            let inner = select(Core {
                from: vec![table("y", second)],
                conditions: vec![comparing(rng, "y", "x")],
                select: vec![out(lit(1))],
                group: Vec::new(),
                distinct: false,
            });
            where_.push(Cond::Exists(Box::new(inner), rng.chance(30)));
        }
    }
    select(Core { from, conditions: where_, select: select_, group: Vec::new(), distinct: false })
}
