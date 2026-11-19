//! An equality between a column of one item and an expression over a partner
//! that can be other than NULL where the partner's columns are NULL: a CASE,
//! `iif`, `IS NULL` as a value, COALESCE and an app function, alone or inside
//! a strict expression, beside strict expressions over the same partner. The
//! partner is a LEFT JOIN's right side, whose missing row gives such an
//! expression a value that no lookup finds, so the member reads its table
//! whole; or an inner join's, which every binding holds a row of, so the
//! member narrows.

use super::{col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

/// A value of `partner` that can be other than NULL where its columns are
/// NULL, or at times one that can't.
fn value(rng: &mut Rng, partner: &str) -> Expr {
    let column = |rng: &mut Rng| col(partner, pick(rng, &COLUMNS));
    let constant = |rng: &mut Rng| lit(rng.below(4) as i64);
    let choice = |rng: &mut Rng, test: Expr| {
        let yes = if rng.chance(30) { column(rng) } else { constant(rng) };
        Expr::Iif {
            test: Box::new(test),
            yes: Box::new(yes),
            no: Box::new(constant(rng)),
            iif: rng.chance(50),
        }
    };
    match rng.below(11) {
        0 | 1 => {
            let test = column(rng);
            choice(rng, test)
        }
        2 => {
            let test = Expr::Add(Box::new(column(rng)), Box::new(lit(-1)));
            choice(rng, test)
        }
        3 => Expr::IsNull(Box::new(column(rng))),
        4 => Expr::Add(Box::new(Expr::IsNull(Box::new(column(rng)))), Box::new(constant(rng))),
        5 => {
            let test = column(rng);
            Expr::Greatest(Box::new(choice(rng, test)), Box::new(lit(0)))
        }
        6 => Expr::Coalesce(Box::new(column(rng)), Box::new(constant(rng))),
        7 => Expr::App(Box::new(column(rng))),
        8 => Expr::Add(Box::new(Expr::App(Box::new(column(rng)))), Box::new(constant(rng))),
        _ => super::edge::value(rng, partner, &COLUMNS),
    }
}

/// An equality between a column of `member` and a value of `partner`,
/// either side first.
fn equality(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let column = col(member, pick(rng, &COLUMNS));
    let other = value(rng, partner);
    if rng.chance(50) {
        eq(column, other)
    } else {
        eq(other, column)
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let third = pick(rng, &TABLES);
    let mut select_ = super::term::columns("x");
    select_.push(out(col("y", "c")));
    let mut where_ = Vec::new();
    if rng.chance(60) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    let joined = vec![eq(col("y", pick(rng, &COLUMNS)), col("x", pick(rng, &COLUMNS)))];
    let partner = if rng.chance(50) { Join::Left } else { Join::Inner };
    let mut from = vec![table("x", first), item("y", Source::Table(second), partner, joined)];
    match rng.below(4) {
        0 => from.push(item("z", Source::Table(third), Join::Inner, vec![equality(rng, "z", "y")])),
        1 => from.push(item("z", Source::Table(third), Join::Left, vec![equality(rng, "z", "y")])),
        2 => {
            from.push(table("z", third));
            where_.push(equality(rng, "z", "y"));
        }
        _ => {
            // The member is the first item, which the partner joins.
            where_.push(equality(rng, "x", "y"));
            from.push(item(
                "z",
                Source::Table(third),
                Join::Inner,
                vec![eq(col("z", "a"), col("x", "id"))],
            ));
        }
    }
    select_.push(out(col("z", "a")));
    select(Core { from, conditions: where_, select: select_, group: Vec::new(), distinct: false })
}
