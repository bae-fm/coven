//! The fallback: a NATURAL join makes sqlive read every table of the
//! statement whole, and a statement with no condition depends on every
//! row of each table it reads.

use super::{col, item, out, pick, select, table, Core, Join, Out, Query, Source, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    // sqlive reads every column of a contentful FTS5 table a NATURAL join
    // reads, where the model reads the columns the join compares. The
    // plain tables have the column `s` that `SEARCH` lacks, so the join's
    // two tables are both plain or both `SEARCH`.
    let second = if first == super::SEARCH {
        first
    } else {
        pick(
            rng,
            &TABLES.iter().copied().filter(|table| *table != super::SEARCH).collect::<Vec<_>>(),
        )
    };
    match rng.below(2) {
        0 => select(Core {
            from: vec![
                table("x", first),
                item(
                    "y",
                    Source::Table(second),
                    // The two tables have the same columns.
                    Join::Natural { columns: super::columns(first) },
                    Vec::new(),
                ),
            ],
            conditions: vec![super::term::condition(rng, "x", 1)],
            select: vec![out(col("x", "id")), out(col("y", "b"))],
            group: Vec::new(),
            distinct: false,
        }),
        _ => select(Core {
            from: vec![table("x", first)],
            conditions: Vec::new(),
            select: vec![Out::Count(None)],
            group: Vec::new(),
            distinct: false,
        }),
    }
}
