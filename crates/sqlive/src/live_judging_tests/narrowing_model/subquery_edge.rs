//! Rule 3: a column compared with the value a subquery returns, by `=`
//! either way round or by IN, and the forms that narrow nothing: NOT IN, a
//! returned expression or aggregate, a compound, a column of another
//! affinity.

use super::{
    col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    COLUMNS, TABLES,
};
use crate::random_scopes::Rng;

/// A subquery over the table `name` returning one column, sometimes
/// correlated with the item `outer`.
fn subquery(rng: &mut Rng, name: &'static str, outer: &str) -> Query {
    let returned = match rng.below(8) {
        0 => out(Expr::Add(Box::new(col("q", "a")), Box::new(lit(1)))),
        1 => Out::Max(col("q", "a"), None),
        _ => out(col("q", pick(rng, &COLUMNS))),
    };
    let mut conditions = Vec::new();
    if rng.chance(70) {
        conditions.push(super::term::condition(rng, "q", 1));
    }
    if rng.chance(30) {
        conditions.push(eq(col("q", "c"), col(outer, "c")));
    }
    let core = Core {
        from: vec![table("q", name)],
        conditions,
        select: vec![returned],
        group: Vec::new(),
    };
    if rng.chance(15) {
        let other = Core {
            from: vec![table("p", pick(rng, &TABLES))],
            conditions: Vec::new(),
            select: vec![out(col("p", "a"))],
            group: Vec::new(),
        };
        return Query { arms: vec![core, other], ops: vec!["UNION"], ..Query::default() };
    }
    let mut query = select(core);
    if rng.chance(20) {
        query.limit = super::limit(1);
    }
    query
}

/// A comparison of a column of `member` with a subquery's value.
pub(super) fn comparison(rng: &mut Rng, member: &str) -> Cond {
    let column = col(member, pick(rng, &COLUMNS));
    let name = pick(rng, &TABLES);
    let query = Box::new(subquery(rng, name, member));
    match rng.below(5) {
        0 => eq(column, Expr::Sub(query)),
        1 => eq(Expr::Sub(query), column),
        2 | 3 => Cond::InQuery(column, query, false),
        _ => Cond::InQuery(column, query, true),
    }
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
