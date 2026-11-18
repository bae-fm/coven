//! Statements that hold several rules at once: a derived item, a table that
//! takes values from it, a LEFT JOIN, a subquery's value and terms.

use super::{col, item, out, pick, select, table, Core, Join, Query, Source, COLUMNS, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    let derived = super::push_down::derived(rng);
    let mut from = vec![table("x", first)];
    from.push(item(
        "d",
        Source::Query(Box::new(derived)),
        Join::Inner,
        vec![super::edge::equality_of(rng, ("d", &super::push_down::DERIVED), ("x", &COLUMNS))],
    ));
    let partner: (&str, &[&'static str]) =
        if rng.chance(50) { ("x", &COLUMNS) } else { ("d", &super::push_down::DERIVED) };
    let on = vec![super::edge::equality_of(rng, ("y", &COLUMNS), partner)];
    let join = if rng.chance(50) { Join::Left } else { Join::Inner };
    from.push(item("y", Source::Table(second), join, on));
    let mut conditions = Vec::new();
    if rng.chance(60) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    if rng.chance(40) {
        conditions.push(super::push_down::condition(rng, "d", &super::push_down::DERIVED));
    }
    if rng.chance(40) {
        conditions.push(super::subquery_edge::comparison(rng, "x"));
    }
    if rng.chance(30) {
        conditions.push(super::term::condition(rng, "y", 1));
    }
    let select_ = vec![out(col("x", "id")), out(col("d", "a")), out(col("y", "c"))];
    select(Core { from, conditions, select: select_, group: Vec::new() })
}
