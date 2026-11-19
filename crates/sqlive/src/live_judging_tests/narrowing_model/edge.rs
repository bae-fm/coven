//! Rule 2: equalities between a column of one item and a value of another,
//! in an inner join's ON, the WHERE, a LEFT JOIN's ON, a self-join, and a
//! subquery correlated with the item around it. The columns' affinities and
//! collations differ, so some equalities narrow nothing.

use super::{
    col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Item, Join, Out, Query, Source,
    COLUMNS, TABLES,
};
use crate::random_scopes::Rng;

/// The value an equality takes from item `alias`, whose columns are
/// `columns`: a column of any affinity, or an expression over one.
pub(super) fn value(rng: &mut Rng, alias: &str, columns: &[&'static str]) -> Expr {
    let column = col(alias, pick(rng, columns));
    if rng.chance(20) {
        Expr::Add(Box::new(column), Box::new(lit(rng.below(2) as i64)))
    } else {
        column
    }
}

/// An equality between a column of `member` and a value `value` writes of
/// `partner`, either side first, each an alias with its columns.
pub(super) fn equality_of(
    rng: &mut Rng,
    member: (&str, &[&'static str]),
    partner: (&str, &[&'static str]),
    value: fn(&mut Rng, &str, &[&'static str]) -> Expr,
) -> Cond {
    let column = col(member.0, pick(rng, member.1));
    let other = value(rng, partner.0, partner.1);
    if rng.chance(50) {
        eq(column, other)
    } else {
        eq(other, column)
    }
}

/// An equality between a column of the table item `member` and a value of
/// the table item `partner`.
pub(super) fn equality(rng: &mut Rng, member: &str, partner: &str) -> Cond {
    equality_of(rng, (member, &COLUMNS), (partner, &COLUMNS), value)
}

fn maybe_terms(rng: &mut Rng, alias: &str) -> Vec<Cond> {
    if rng.chance(60) {
        vec![super::term::condition(rng, alias, 1)]
    } else {
        Vec::new()
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let third = pick(rng, &TABLES);
    let mut select_ = super::term::columns("x");
    select_.push(out(col("y", "c")));
    let core = |from: Vec<Item>, conditions: Vec<Cond>, select_: Vec<Out>| Core {
        from,
        conditions,
        select: select_,
        group: Vec::new(),
        distinct: false,
    };
    match rng.below(6) {
        0 => {
            let on = vec![equality(rng, "y", "x")];
            let mut where_ = maybe_terms(rng, "x");
            where_.extend(maybe_terms(rng, "y"));
            select(core(
                vec![table("x", first), item("y", Source::Table(second), Join::Inner, on)],
                where_,
                select_,
            ))
        }
        1 => {
            let mut where_ = vec![equality(rng, "x", "y")];
            where_.extend(maybe_terms(rng, "x"));
            select(core(vec![table("x", first), table("y", second)], where_, select_))
        }
        2 => {
            let on_y = vec![equality(rng, "y", "x")];
            let on_z = vec![equality(rng, "z", "y")];
            let mut where_ = maybe_terms(rng, "x");
            where_.extend(maybe_terms(rng, "z"));
            select_.push(out(col("z", "a")));
            select(core(
                vec![
                    table("x", first),
                    item("y", Source::Table(second), Join::Inner, on_y),
                    item("z", Source::Table(third), Join::Inner, on_z),
                ],
                where_,
                select_,
            ))
        }
        3 => {
            // A LEFT JOIN's ON narrows its right side; a WHERE condition
            // on that side that rejects NULLs makes it inner.
            let mut on = vec![equality(rng, "y", "x")];
            if rng.chance(30) {
                on.push(equality(rng, "x", "y"));
            }
            let mut where_ = maybe_terms(rng, "x");
            if rng.chance(50) {
                where_.push(super::term::condition(rng, "y", 1));
            }
            select(core(
                vec![table("x", first), item("y", Source::Table(second), Join::Left, on)],
                where_,
                select_,
            ))
        }
        4 => {
            // A table joined to itself.
            let on = vec![equality(rng, "y", "x")];
            select(core(
                vec![table("x", first), item("y", Source::Table(first), Join::Inner, on)],
                maybe_terms(rng, "x"),
                select_,
            ))
        }
        _ => {
            // A subquery's item takes values from the item around it.
            let inner = select(core(
                vec![table("q", second)],
                vec![equality(rng, "q", "x")].into_iter().chain(maybe_terms(rng, "q")).collect(),
                vec![out(lit(1))],
            ));
            let mut where_ = maybe_terms(rng, "x");
            where_.push(Cond::Exists(Box::new(inner), rng.chance(30)));
            select(core(vec![table("x", first)], where_, super::term::columns("x")))
        }
    }
}
