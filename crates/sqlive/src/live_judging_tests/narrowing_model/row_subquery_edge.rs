//! A row value compared with a subquery's row, by `=` either way round or by
//! IN, each pair narrowing as it would alone; and the forms that narrow
//! nothing: NOT IN, `NOT (... = ...)`, a pair whose subquery side is an
//! expression or an aggregate, or whose sides differ in affinity or
//! collation, a compound, and a constant in the row value.

use super::{
    col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    COLUMNS, TABLES,
};
use crate::random_scopes::Rng;

/// A subquery over the table `name` returning `width` columns, sometimes
/// correlated with the item `outer`.
fn subquery(rng: &mut Rng, name: &'static str, outer: &str, width: usize) -> Query {
    // Aggregates all, as a bare column beside one takes SQLite's pick of
    // row, which the model doesn't follow.
    let aggregates = rng.chance(10);
    let returned: Vec<Out> = (0..width)
        .map(|_| match rng.below(10) {
            _ if aggregates => Out::Max(col("q", pick(rng, &COLUMNS)), None),
            0 => out(Expr::Add(Box::new(col("q", "a")), Box::new(lit(1)))),
            _ => out(col("q", pick(rng, &COLUMNS))),
        })
        .collect();
    let mut conditions = Vec::new();
    if rng.chance(70) {
        conditions.push(super::term::condition(rng, "q", 1));
    }
    if rng.chance(30) {
        conditions.push(eq(col("q", "c"), col(outer, "c")));
    }
    let core =
        Core { from: vec![table("q", name)], conditions, select: returned, group: Vec::new() };
    if rng.chance(15) {
        let other = Core {
            from: vec![table("p", pick(rng, &TABLES))],
            conditions: Vec::new(),
            select: (0..width).map(|_| out(col("p", pick(rng, &COLUMNS)))).collect(),
            group: Vec::new(),
        };
        return Query { arms: vec![core, other], ops: vec!["UNION"], ..Query::default() };
    }
    let mut query = select(core);
    if rng.chance(20) {
        query.limit = Some(1);
    }
    query
}

/// A row value of the item `member` compared with a subquery's row.
pub(super) fn comparison(rng: &mut Rng, member: &str) -> Cond {
    let width = 2 + rng.below(2);
    let values: Vec<Expr> = (0..width)
        .map(|_| {
            if rng.chance(10) {
                lit(rng.below(4) as i64)
            } else {
                col(member, pick(rng, &COLUMNS))
            }
        })
        .collect();
    let name = pick(rng, &TABLES);
    let query = Box::new(subquery(rng, name, member, width));
    let (member_, not) = match rng.below(6) {
        0 | 1 => (false, false),
        2 | 3 => (true, false),
        4 => (true, true),
        _ => (false, true),
    };
    Cond::RowSub { values, query, member: member_, first: rng.chance(50), not }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let mut where_ = vec![comparison(rng, "x")];
    if rng.chance(40) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    if rng.chance(40) {
        // An item that takes values from the compared one.
        let second = pick(rng, &TABLES);
        let on = vec![eq(col("y", "a"), col("x", "id"))];
        let mut select_ = super::term::columns("x");
        select_.push(out(col("y", "c")));
        return select(Core {
            from: vec![table("x", first), item("y", Source::Table(second), Join::Inner, on)],
            conditions: where_,
            select: select_,
            group: Vec::new(),
        });
    }
    select(Core {
        from: vec![table("x", first)],
        conditions: where_,
        select: super::term::columns("x"),
        group: Vec::new(),
    })
}
