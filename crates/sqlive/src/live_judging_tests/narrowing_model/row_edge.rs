//! A row value equality between two items, which narrows as the
//! equalities of its pairs: in an inner join's ON, the WHERE and a LEFT
//! JOIN's ON, over two to four columns of any affinity or collation, a pair
//! that is an expression of one item or a constant, and `NOT` around it,
//! which narrows nothing.

use super::edge::value;
use super::{col, item, lit, out, pick, select, table, Cond, Core, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

/// A row value equality between columns of `member` and values of
/// `partner`, either side first.
fn equality(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let width = 2 + rng.below(3);
    let mut left = Vec::new();
    let mut right = Vec::new();
    for _ in 0..width {
        let column = col(member, pick(rng, &COLUMNS));
        let other = match rng.below(10) {
            0 => lit(rng.below(4) as i64),
            1 => col(member, pick(rng, &COLUMNS)),
            _ => value(rng, partner, &COLUMNS),
        };
        if rng.chance(50) {
            left.push(column);
            right.push(other);
        } else {
            left.push(other);
            right.push(column);
        }
    }
    let equality = Cond::RowEq(left, right);
    if rng.chance(10) {
        Cond::Not(Box::new(equality))
    } else {
        equality
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
    let (join, on) = match rng.below(3) {
        0 => (Join::Inner, vec![equality(rng, "y", "x")]),
        1 => (Join::Left, vec![equality(rng, "y", "x")]),
        _ => {
            where_.push(equality(rng, "x", "y"));
            (Join::Comma, Vec::new())
        }
    };
    if join == Join::Left && rng.chance(40) {
        where_.push(super::term::condition(rng, "y", 1));
    }
    select(Core {
        from: vec![table("x", first), item("y", Source::Table(second), join, on)],
        conditions: where_,
        select: select_,
        group: Vec::new(),
        distinct: false,
    })
}
