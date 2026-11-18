//! USING joins, which narrow as the equalities they stand for: of a table,
//! a FROM subquery or a CTE, on one or two columns of any affinity or
//! collation, by an inner or a LEFT JOIN, after items of which the leftmost
//! with the column isn't always the first, and a second USING join after
//! the first.

use super::push_down::{derived, DERIVED};
use super::{col, item, out, pick, select, table, Core, Join, Query, Source, TABLES};
use crate::random_scopes::Rng;

/// The columns of the subquery that sometimes stands first, without the
/// columns the item after it has.
const FIRST: [&str; 2] = ["id", "a"];

/// One column of `columns`, or two beside each other.
fn using(rng: &mut Rng, columns: &'static [&'static str]) -> &'static [&'static str] {
    let first = rng.below(columns.len());
    if rng.chance(30) && first + 1 < columns.len() {
        &columns[first..first + 2]
    } else {
        &columns[first..=first]
    }
}

/// A condition on the item `alias` whose columns are `columns`.
fn condition(rng: &mut Rng, alias: &str, columns: &[&'static str]) -> super::Cond {
    if columns.len() == super::COLUMNS.len() {
        super::term::condition(rng, alias, 1)
    } else {
        super::push_down::condition(rng, alias, columns)
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let mut from = Vec::new();
    let mut conditions = Vec::new();
    // The left items, each with its columns.
    let mut left: Vec<(&str, &'static [&'static str])> = Vec::new();
    if rng.chance(30) {
        // A subquery lacking most columns, then a table: the table is the
        // leftmost item with those.
        let core = Core {
            from: vec![table("p", pick(rng, &TABLES))],
            conditions: Vec::new(),
            select: FIRST.iter().map(|column| out(col("p", column))).collect(),
            group: Vec::new(),
        };
        from.push(item("x", Source::Query(Box::new(select(core))), Join::Comma, Vec::new()));
        from.push(table("z", pick(rng, &TABLES)));
        left.push(("x", &FIRST));
        left.push(("z", &super::COLUMNS));
    } else {
        from.push(table("x", pick(rng, &TABLES)));
        left.push(("x", &super::COLUMNS));
    }
    let mut with = Vec::new();
    let (source, columns): (Source, &'static [&'static str]) = match rng.below(4) {
        0 => {
            with.push(("e".to_owned(), derived(rng)));
            (Source::Cte("e".to_owned()), &DERIVED)
        }
        1 => (Source::Query(Box::new(derived(rng))), &DERIVED),
        _ => (Source::Table(pick(rng, &TABLES)), &super::COLUMNS),
    };
    let joined_left = rng.chance(40);
    from.push(item(
        "y",
        source,
        Join::Using { left: joined_left, columns: using(rng, columns) },
        Vec::new(),
    ));
    if rng.chance(25) {
        // A second USING join, whose columns the first items have.
        let columns = left[0].1;
        from.push(item(
            "v",
            Source::Table(pick(rng, &TABLES)),
            Join::Using { left: rng.chance(40), columns: using(rng, columns) },
            Vec::new(),
        ));
    }
    for (alias, item_columns) in &left {
        if rng.chance(50) {
            conditions.push(condition(rng, alias, item_columns));
        }
    }
    if rng.chance(30) {
        // On the right side: makes a LEFT JOIN inner, unless a row of NULLs
        // can pass it.
        conditions.push(condition(rng, "y", columns));
    }
    let shown = left[left.len() - 1].0;
    let mut query = select(Core {
        from,
        conditions,
        select: vec![out(col(shown, "id")), out(col("y", pick(rng, columns)))],
        group: Vec::new(),
    });
    query.with = with;
    query
}
