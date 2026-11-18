//! The fallback: a NATURAL join makes sqlive read every table of the
//! statement whole, and a statement with no condition depends on every
//! row of each table it reads.

use super::{col, eq, item, out, pick, select, table, Core, Join, Out, Query, Source, TABLES};
use crate::random_scopes::Rng;

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    match rng.below(2) {
        0 => select(Core {
            from: vec![
                table("x", first),
                item(
                    "y",
                    Source::Table(second),
                    Join::Natural,
                    ["id", "a", "b", "c", "n"]
                        .iter()
                        .map(|column| eq(col("y", column), col("x", column)))
                        .collect(),
                ),
            ],
            conditions: vec![super::term::condition(rng, "x", 1)],
            select: vec![out(col("x", "id")), out(col("y", "b"))],
            group: Vec::new(),
        }),
        _ => select(Core {
            from: vec![table("x", first)],
            conditions: Vec::new(),
            select: vec![Out::Count(None)],
            group: Vec::new(),
        }),
    }
}
