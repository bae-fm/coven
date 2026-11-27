//! The ORs of `or` with the tables they narrow behind FROM subqueries, as
//! `wrapped` puts them there.

use super::{col, columns, out, select, table, Core, Query, Source, SEARCH};
use crate::random_scopes::Rng;

/// The rows of the table `name`, as a query selecting each of its columns.
fn rows(name: &'static str) -> Query {
    select(Core {
        from: vec![table("zz", name)],
        conditions: Vec::new(),
        select: columns(name).iter().map(|column| out(col("zz", column))).collect(),
        group: Vec::new(),
        distinct: false,
    })
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let query = super::or::statement(rng);
    wrapped(rng, query)
}

/// `query` with tables of its first SELECT's FROM behind subqueries, the
/// first FROM item always and each other one half the time: one level deep or
/// two, and sometimes a UNION ALL of the table's rows twice.
pub(super) fn wrapped(rng: &mut Rng, mut query: Query) -> Query {
    for (position, item) in query.arms[0].from.iter_mut().enumerate() {
        let Source::Table(name) = item.source else { continue };
        if name == SEARCH || (position == 0 && rng.chance(50)) {
            continue;
        }
        let mut inner = rows(name);
        if rng.chance(20) {
            inner.arms.push(rows(name).arms.remove(0));
            inner.ops.push("UNION ALL");
        }
        if rng.chance(30) {
            let mut outer = rows(name);
            outer.arms[0].from[0].source = Source::Query(Box::new(inner));
            inner = outer;
        }
        item.source = Source::Query(Box::new(inner));
    }
    query
}
