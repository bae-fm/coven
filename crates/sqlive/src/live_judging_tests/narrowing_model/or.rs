//! An OR whose sides narrow one item, each by a term, an equality with
//! another item, `IS`, or a comparison with a subquery's value, and an AND
//! of these in a side: in the WHERE, an inner join's ON and a LEFT JOIN's
//! ON. Sides that name only the other item, and `NOT` around the OR, narrow
//! nothing.

use super::{cmp, col, item, pick, select, table, Cond, Core, Join, Query, Source};
use super::{COLUMNS, TABLES};
use crate::random_scopes::Rng;

/// A condition that narrows `member` at times, and names `partner` at times.
fn side(rng: &mut Rng, member: &str, partner: &str, depth: usize) -> Cond {
    if depth < 1 && rng.chance(20) {
        let left = Box::new(side(rng, member, partner, depth + 1));
        let right = Box::new(side(rng, member, partner, depth + 1));
        return Cond::And(left, right);
    }
    match rng.below(10) {
        0..=2 => super::edge::equality(rng, member, partner),
        3 => cmp("IS", col(member, pick(rng, &COLUMNS)), col(partner, pick(rng, &COLUMNS))),
        4 | 5 => super::term::condition(rng, member, 1),
        6 | 7 => super::subquery_edge::comparison(rng, member),
        8 => super::term::condition(rng, partner, 1),
        _ => super::edge::equality(rng, partner, member),
    }
}

/// An OR of two or three sides.
fn or(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    let mut found = side(rng, member, partner, 0);
    for _ in 0..1 + rng.below(2) {
        found = Cond::Or(Box::new(found), Box::new(side(rng, member, partner, 0)));
    }
    if rng.chance(10) {
        Cond::Not(Box::new(found))
    } else {
        found
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let mut select_ = super::term::columns("x");
    select_.push(super::out(col("y", "c")));
    let mut where_ = Vec::new();
    if rng.chance(60) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    let (join, on) = match rng.below(3) {
        0 => (Join::Inner, vec![or(rng, "y", "x")]),
        1 => (Join::Left, vec![or(rng, "y", "x")]),
        _ => {
            where_.push(or(rng, "y", "x"));
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
