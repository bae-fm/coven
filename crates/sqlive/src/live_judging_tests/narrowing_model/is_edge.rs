//! `IS` between a column of one item and a value of another, which narrows
//! as the equality does and matches a NULL with a NULL: in an inner join's
//! ON, the WHERE and a LEFT JOIN's ON, beside an equality, with a partner a
//! LEFT JOIN adds, which gives no values, and `NOT` around it, which
//! narrows nothing.

use super::edge::value;
use super::{col, eq, item, out, pick, select, table, Cond, Core, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

/// `IS` between a column of `member` and a value of `partner`, either side
/// first.
fn is(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let column = col(member, pick(rng, &COLUMNS));
    let other = value(rng, partner, &COLUMNS);
    let compared = if rng.chance(50) {
        Cond::Cmp("IS", column, other)
    } else {
        Cond::Cmp("IS", other, column)
    };
    if rng.chance(10) {
        Cond::Not(Box::new(compared))
    } else {
        compared
    }
}

/// `IS` joining `member` to `partner`, and an equality beside it at times.
fn joining(rng: &mut Rng, member: &str, partner: &str) -> Vec<Cond> {
    let mut on = vec![is(rng, member, partner)];
    if rng.chance(30) {
        on.push(eq(col(member, pick(rng, &COLUMNS)), col(partner, pick(rng, &COLUMNS))));
    }
    on
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
    let mut from = vec![table("x", first)];
    match rng.below(4) {
        0 => from.push(item("y", Source::Table(second), Join::Inner, joining(rng, "y", "x"))),
        1 => {
            from.push(item("y", Source::Table(second), Join::Left, joining(rng, "y", "x")));
            if rng.chance(40) {
                where_.push(super::term::condition(rng, "y", 1));
            }
        }
        2 => {
            from.push(table("y", second));
            where_.extend(joining(rng, "x", "y"));
        }
        _ => {
            // `z` takes values by `IS` from `y`, which a LEFT JOIN adds.
            let on = vec![eq(col("y", pick(rng, &COLUMNS)), col("x", pick(rng, &COLUMNS)))];
            from.push(item("y", Source::Table(second), Join::Left, on));
            let third = pick(rng, &TABLES);
            from.push(item("z", Source::Table(third), Join::Inner, joining(rng, "z", "y")));
            select_.push(out(col("z", "a")));
        }
    }
    select(Core { from, conditions: where_, select: select_, group: Vec::new(), distinct: false })
}
