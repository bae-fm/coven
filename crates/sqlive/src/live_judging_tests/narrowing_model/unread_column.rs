//! A column of a FROM subquery or CTE that holds a subquery over another
//! table, which the statement reads or not: through a `SELECT *` around the
//! subquery, in each arm of a UNION ALL, and where the subquery's rows
//! depend on the column, as with an ORDER BY, a DISTINCT or a UNION.

use super::push_down::{condition, derived, DERIVED};
use super::{
    col, eq, item, lit, out, pick, select, table, Core, Expr, Join, Out, Query, Source, TABLES,
};
use crate::random_scopes::Rng;

/// A SELECT of a table whose column `column` of `DERIVED` is the number of
/// another table's rows that its row joins.
fn counted(rng: &mut Rng, alias: &'static str, column: usize) -> Core {
    let mut counted = vec![eq(col("n", pick(rng, &["a", "c"])), col(alias, "c"))];
    if rng.chance(30) {
        counted.push(super::term::condition(rng, "n", 1));
    }
    let count = select(Core {
        from: vec![table("n", pick(rng, &TABLES))],
        conditions: counted,
        select: vec![Out::Count(None)],
        group: Vec::new(),
        distinct: false,
    });
    let mut selected: Vec<Out> = DERIVED.iter().map(|name| out(col(alias, name))).collect();
    selected[column] = Out::Expr(Expr::Sub(Box::new(count)), Some(DERIVED[column].to_owned()));
    let conditions =
        if rng.chance(30) { vec![super::term::condition(rng, alias, 1)] } else { Vec::new() };
    Core {
        from: vec![table(alias, pick(rng, &TABLES))],
        conditions,
        select: selected,
        group: Vec::new(),
        distinct: rng.chance(10),
    }
}

/// The columns `DERIVED` of `query`, read as the item `alias`.
fn around(alias: &str, query: Query) -> Query {
    select(Core {
        from: vec![item(alias, Source::Query(Box::new(query)), Join::Comma, Vec::new())],
        conditions: Vec::new(),
        select: DERIVED.iter().map(|name| out(col(alias, name))).collect(),
        group: Vec::new(),
        distinct: false,
    })
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let column = 1 + rng.below(3);
    let mut inner = select(counted(rng, "p", column));
    if rng.chance(30) {
        let mut second =
            if rng.chance(50) { select(counted(rng, "r", column)) } else { derived(rng) };
        if second.arms.len() == 1 && second.order.is_empty() && second.limit.is_none() {
            inner.arms.push(second.arms.remove(0));
            inner.ops.push(if rng.chance(75) { "UNION ALL" } else { "UNION" });
        }
    }
    if inner.arms.len() == 1 && rng.chance(10) {
        inner.order = vec![(column, false), (0, false)];
    }
    if rng.chance(40) {
        inner = around("w", inner);
    }
    // The statement reads some of the columns, the counted one among them
    // or not.
    let read: Vec<&'static str> = DERIVED.iter().copied().filter(|_| rng.chance(40)).collect();
    let mut selected: Vec<Out> = read.iter().map(|name| out(col("d", name))).collect();
    selected.push(out(col("d", "id")));
    let mut conditions = Vec::new();
    if rng.chance(40) {
        conditions.push(condition(rng, "d", &DERIVED[..column]));
    }
    let mut from = vec![item("d", Source::Query(Box::new(inner)), Join::Comma, Vec::new())];
    if rng.chance(50) {
        let join = if rng.chance(30) { Join::Left } else { Join::Inner };
        let on = vec![eq(col("y", pick(rng, &["a", "c"])), col("d", "id"))];
        from.push(item("y", Source::Table(pick(rng, &TABLES)), join, on));
    }
    if rng.chance(30) {
        conditions.push(eq(col("d", "id"), lit(rng.below(4) as i64)));
    }
    select(Core { from, conditions, select: selected, group: Vec::new(), distinct: false })
}
