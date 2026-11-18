//! A FROM subquery or CTE read inside an expression subquery, which a
//! lookup reads alone when its SQL names no row outside it: one in the
//! statement's own WITH, one in a WITH inside the subquery, one in FROM
//! there, and one joined inside another CTE that the statement reads only
//! in a subquery; and the same correlated with the row around them, which
//! narrow by their own conditions only.

use super::push_down::{condition, derived, DERIVED};
use super::{
    col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    TABLES,
};
use crate::random_scopes::Rng;

/// A query of a derived item's shape, sometimes correlated with the item
/// `outer` around the subquery that reads it.
fn inner(rng: &mut Rng, outer: &str) -> Query {
    let mut query = derived(rng);
    if rng.chance(30) {
        let core = &mut query.arms[0];
        let alias = core.from[0].alias.clone();
        core.conditions.push(eq(col(&alias, "c"), col(outer, "c")));
    }
    query
}

/// The subquery that reads the derived item `d` beside a table it gives
/// values to, or that takes values from it.
fn reading(rng: &mut Rng, source: Source, outer: &str) -> Query {
    let table_ = pick(rng, &TABLES);
    let on = vec![if rng.chance(50) {
        eq(col("y", "a"), col("d", "c"))
    } else {
        eq(col("d", "a"), col("y", "c"))
    }];
    let mut conditions = Vec::new();
    if rng.chance(50) {
        conditions.push(condition(rng, "d", &DERIVED));
    }
    if rng.chance(40) {
        conditions.push(super::term::condition(rng, "y", 1));
    }
    if rng.chance(30) {
        conditions.push(eq(col("y", "b"), col(outer, "b")));
    }
    select(Core {
        from: vec![
            item("d", source, Join::Comma, Vec::new()),
            item("y", Source::Table(table_), Join::Inner, on),
        ],
        conditions,
        select: vec![Out::Count(None)],
        group: Vec::new(),
    })
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let mut where_ = Vec::new();
    if rng.chance(50) {
        where_.push(super::term::condition(rng, "x", 1));
    }
    let mut with = Vec::new();
    let subquery = match rng.below(5) {
        0 => {
            // A CTE of the statement's own WITH.
            with.push(("e".to_owned(), derived(rng)));
            reading(rng, Source::Cte("e".to_owned()), "x")
        }
        1 => {
            // A CTE of a WITH inside the subquery.
            let mut query = reading(rng, Source::Cte("e".to_owned()), "x");
            query.with.push(("e".to_owned(), inner(rng, "x")));
            query
        }
        2 => {
            let query = inner(rng, "x");
            reading(rng, Source::Query(Box::new(query)), "x")
        }
        3 => {
            // A column compared with a value the CTE gives.
            with.push(("e".to_owned(), derived(rng)));
            let mut conditions = vec![condition(rng, "d", &DERIVED)];
            if rng.chance(30) {
                conditions.push(eq(col("d", "b"), col("x", "b")));
            }
            let pick_ = select(Core {
                from: vec![item("d", Source::Cte("e".to_owned()), Join::Comma, Vec::new())],
                conditions,
                select: vec![out(col("d", pick(rng, &["id", "a", "c"])))],
                group: Vec::new(),
            });
            where_.push(Cond::InQuery(
                col("x", pick(rng, &["id", "a", "c"])),
                Box::new(pick_),
                false,
            ));
            return Query {
                with,
                ..select(Core {
                    from: vec![table("x", first)],
                    conditions: where_,
                    select: super::term::columns("x"),
                    group: Vec::new(),
                })
            };
        }
        _ => {
            // A CTE joined to a table inside another CTE, which the
            // statement reads only in a subquery with no FROM around it.
            with.push(("e".to_owned(), derived(rng)));
            let mut joined = Core {
                from: vec![
                    item("d", Source::Cte("e".to_owned()), Join::Comma, Vec::new()),
                    item(
                        "y",
                        Source::Table(pick(rng, &TABLES)),
                        Join::Inner,
                        vec![eq(col("y", "a"), col("d", "c"))],
                    ),
                ],
                conditions: Vec::new(),
                select: DERIVED.iter().map(|column| out(col("y", column))).collect(),
                group: Vec::new(),
            };
            if rng.chance(50) {
                joined.conditions.push(condition(rng, "d", &DERIVED));
            }
            with.push(("f".to_owned(), select(joined)));
            let count = select(Core {
                from: vec![item("g", Source::Cte("f".to_owned()), Join::Comma, Vec::new())],
                conditions: Vec::new(),
                select: vec![Out::Count(None)],
                group: Vec::new(),
            });
            return Query {
                with,
                ..select(Core {
                    from: Vec::new(),
                    conditions: Vec::new(),
                    select: vec![out(Expr::Sub(Box::new(count))), out(lit(1))],
                    group: Vec::new(),
                })
            };
        }
    };
    let mut select_ = super::term::columns("x");
    if rng.chance(50) {
        where_.push(Cond::Exists(Box::new(subquery), rng.chance(30)));
    } else {
        select_.push(out(Expr::Sub(Box::new(subquery))));
    }
    Query {
        with,
        ..select(Core {
            from: vec![table("x", first)],
            conditions: where_,
            select: select_,
            group: Vec::new(),
        })
    }
}
