//! Rule 7 in a DISTINCT core: DISTINCT keeps one of each set of rows that
//! compare equal, the one SQLite reads first, so a core that returns the
//! untyped `b`, the NOCASE `n` or an expression, which can hold values that
//! compare equal and differ, depends on every column of its tables. SQLite
//! reads `u` and `w` through their indexes on `a` and `c`, which the
//! statements don't always name. A DISTINCT core of INTEGER columns depends
//! only on the columns it names. Conditions on a DISTINCT FROM subquery's rows
//! pass into it.

use super::{
    col, eq, item, lit, out, pick, select, table, Core, Expr, Join, Out, Query, Source, COLUMNS,
    TABLES,
};
use crate::random_scopes::Rng;

/// A DISTINCT core over `x`, and `y` joined to it, returning one to three
/// columns named `v0`, `v1` and `v2`.
fn core(rng: &mut Rng) -> Core {
    let mut from = vec![table("x", pick(rng, &TABLES))];
    if rng.chance(30) {
        let join = if rng.chance(50) { Join::Left } else { Join::Inner };
        let on = vec![eq(col("y", "a"), col("x", "id"))];
        from.push(item("y", Source::Table(pick(rng, &TABLES)), join, on));
    }
    let mut conditions = Vec::new();
    if rng.chance(50) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let select_ = (0..1 + rng.below(3))
        .map(|position| {
            let value = match rng.below(6) {
                0 => Expr::Add(Box::new(col("x", "a")), Box::new(lit(1))),
                1 if from.len() > 1 => col("y", pick(rng, &COLUMNS)),
                _ => col("x", pick(rng, &COLUMNS)),
            };
            Out::Expr(value, Some(format!("v{position}")))
        })
        .collect();
    Core { from, conditions, select: select_, group: Vec::new(), distinct: true }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let core = core(rng);
    if rng.chance(60) {
        return select(core);
    }
    // The core as a FROM subquery, whose rows a condition narrows.
    let names = ["v0", "v1", "v2"];
    let shown: Vec<Out> =
        (0..core.select.len()).map(|position| out(col("d", names[position]))).collect();
    let condition = super::push_down::condition(rng, "d", &names[..core.select.len()]);
    let from = vec![item("d", Source::Query(Box::new(select(core))), Join::Comma, Vec::new())];
    select(Core {
        from,
        conditions: vec![condition],
        select: shown,
        group: Vec::new(),
        distinct: false,
    })
}
