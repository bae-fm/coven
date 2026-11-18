//! Rule 4: conditions on the rows of a FROM subquery or a CTE pass into it
//! when it neither groups, aggregates nor limits its rows: through a bare
//! column, into each arm of a compound, onto a LEFT JOIN's right side inside
//! it; and a derived item joined to tables, or read inside a subquery.

use super::{
    cmp, col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    TABLES,
};
use crate::random_scopes::Rng;

/// The columns each derived query here returns, so conditions on its rows
/// can name them.
pub(super) const DERIVED: [&str; 4] = ["id", "a", "b", "c"];

fn plain(rng: &mut Rng, alias: &str, name: &'static str) -> Core {
    let conditions =
        if rng.chance(50) { vec![super::term::condition(rng, alias, 1)] } else { Vec::new() };
    Core {
        from: vec![table(alias, name)],
        conditions,
        select: DERIVED.iter().map(|column| out(col(alias, column))).collect(),
        group: Vec::new(),
    }
}

/// A query a derived item reads, with columns `DERIVED`.
pub(super) fn derived(rng: &mut Rng) -> Query {
    let first = pick(rng, &TABLES);
    let second = pick(rng, &TABLES);
    match rng.below(7) {
        0 | 1 => select(plain(rng, "x", first)),
        2 => {
            // A column computed from the item's own passes nothing on.
            let mut core = plain(rng, "x", first);
            core.select[1] = Out::Expr(
                Expr::Add(Box::new(col("x", "a")), Box::new(lit(1))),
                Some("a".to_owned()),
            );
            select(core)
        }
        3 => Query {
            arms: vec![plain(rng, "x", first), plain(rng, "y", second)],
            ops: vec![pick(rng, &["UNION ALL", "UNION", "EXCEPT", "INTERSECT"])],
            ..Query::default()
        },
        4 => select(Core {
            from: vec![table("x", first)],
            conditions: Vec::new(),
            select: vec![
                Out::Expr(col("x", "a"), Some("id".to_owned())),
                Out::Count(Some("a".to_owned())),
                Out::Expr(col("x", "a"), Some("b".to_owned())),
                Out::Max(col("x", "c"), Some("c".to_owned())),
            ],
            group: vec![col("x", "a")],
        }),
        5 => {
            let mut query = select(plain(rng, "x", first));
            query.order = vec![(0, false)];
            query.order_named = true;
            query.limit = Some(3);
            query
        }
        _ => select(Core {
            from: vec![
                table("x", first),
                item(
                    "y",
                    Source::Table(second),
                    Join::Left,
                    vec![eq(col("y", "id"), col("x", "c"))],
                ),
            ],
            conditions: Vec::new(),
            select: vec![
                out(col("x", "id")),
                out(col("x", "a")),
                out(col("y", "b")),
                out(col("y", "c")),
            ],
            group: Vec::new(),
        }),
    }
}

/// A condition on a row of the derived item `alias`.
pub(super) fn condition(rng: &mut Rng, alias: &str, columns: &[&'static str]) -> Cond {
    let column = col(alias, pick(rng, columns));
    let k = rng.below(4) as i64;
    match rng.below(6) {
        0 | 1 => eq(column, lit(k)),
        2 => cmp(pick(rng, &["<", ">="]), column, lit(k)),
        3 => Cond::InList(column, vec![lit(k), lit(rng.below(4) as i64)]),
        4 => Cond::Null(column, rng.chance(50)),
        _ => cmp("<>", column, lit(k)),
    }
}

/// A statement reading `source` as the item `d`, whose columns are
/// `columns`.
pub(super) fn around(rng: &mut Rng, source: Source, columns: &[&'static str]) -> Query {
    let shown: Vec<Out> = columns.iter().map(|column| out(col("d", column))).collect();
    match rng.below(4) {
        0 => select(Core {
            from: vec![item("d", source, Join::Comma, Vec::new())],
            conditions: vec![condition(rng, "d", columns)],
            select: shown,
            group: Vec::new(),
        }),
        1 => {
            // The derived item and a table, joined either way.
            let table_ = pick(rng, &TABLES);
            let on = vec![if rng.chance(50) {
                eq(col("y", "a"), col("d", columns[0]))
            } else {
                eq(col("d", columns[0]), col("y", "a"))
            }];
            let mut where_ = Vec::new();
            if rng.chance(60) {
                where_.push(condition(rng, "d", columns));
            }
            if rng.chance(40) {
                where_.push(super::term::condition(rng, "y", 1));
            }
            let mut select_ = shown;
            select_.push(out(col("y", "c")));
            select(Core {
                from: vec![
                    item("d", source, Join::Comma, Vec::new()),
                    item("y", Source::Table(table_), Join::Inner, on),
                ],
                conditions: where_,
                select: select_,
                group: Vec::new(),
            })
        }
        2 => {
            // A table whose rows a condition narrows, with the derived item
            // joined to it.
            let table_ = pick(rng, &TABLES);
            let on = vec![eq(col("d", columns[0]), col("x", "c"))];
            let mut select_ = super::term::columns("x");
            select_.push(out(col("d", columns[columns.len() - 1])));
            select(Core {
                from: vec![table("x", table_), item("d", source, Join::Inner, on)],
                conditions: vec![super::term::condition(rng, "x", 1)],
                select: select_,
                group: Vec::new(),
            })
        }
        _ => {
            // Inside a subquery, a derived item takes values from the item
            // around it, but a lookup can't read it alone.
            let table_ = pick(rng, &TABLES);
            let inner = select(Core {
                from: vec![item("d", source, Join::Comma, Vec::new())],
                conditions: vec![
                    eq(col("d", columns[0]), col("x", "a")),
                    condition(rng, "d", columns),
                ],
                select: vec![out(lit(1))],
                group: Vec::new(),
            });
            select(Core {
                from: vec![table("x", table_)],
                conditions: vec![Cond::Exists(Box::new(inner), rng.chance(30))],
                select: super::term::columns("x"),
                group: Vec::new(),
            })
        }
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let query = derived(rng);
    if rng.chance(30) {
        // The same query as a CTE.
        let mut statement = around(rng, Source::Cte("e".to_owned()), &DERIVED);
        statement.with = vec![("e".to_owned(), query)];
        return statement;
    }
    around(rng, Source::Query(Box::new(query)), &DERIVED)
}
