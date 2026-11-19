//! Rule 4 into a query that groups its rows: a condition on a column the
//! GROUP BY lists passes into the group's item, through a FROM subquery, a
//! CTE or an arm of a compound beside a plain arm, and a LEFT JOIN onto the
//! grouped rows narrows them by its ON. A condition on an aggregate, on a
//! key computed from a column, or on a grouped query with no GROUP BY passes
//! nothing.

use super::push_down::{around, condition, DERIVED};
use super::{
    col, eq, item, lit, out, pick, select, table, Core, Expr, Join, Out, Query, Source, TABLES,
};
use crate::random_scopes::Rng;

/// A query with columns `DERIVED` that groups its rows by one or two of the
/// columns of `x`, a table, and shows each key or an aggregate.
fn grouped(rng: &mut Rng) -> Core {
    let table_ = pick(rng, &TABLES);
    let first = pick(rng, &["a", "c", "id"]);
    let second = pick(rng, &["c", "b", "a"]);
    let mut group = vec![col("x", first)];
    if rng.chance(50) {
        group.push(col("x", second));
    }
    let key = |position: usize, column: &'static str| {
        Out::Expr(col("x", column), Some(DERIVED[position].to_owned()))
    };
    let second_out = if group.len() == 2 {
        key(2, second)
    } else if rng.chance(50) {
        // A key computed from the column passes nothing on.
        Out::Expr(Expr::Add(Box::new(col("x", first)), Box::new(lit(1))), Some("b".to_owned()))
    } else {
        Out::Max(col("x", "c"), Some("b".to_owned()))
    };
    let mut conditions = Vec::new();
    if rng.chance(40) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let mut select_ = vec![
        key(0, first),
        Out::Count(Some("a".to_owned())),
        second_out,
        Out::Max(col("x", "id"), Some("c".to_owned())),
    ];
    let mut group_ = group;
    if rng.chance(15) {
        // An aggregate with no GROUP BY makes one row of every row.
        group_ = Vec::new();
        select_[0] = Out::Max(col("x", first), Some("id".to_owned()));
        select_[2] = Out::Count(Some("b".to_owned()));
    }
    Core {
        from: vec![table("x", table_)],
        conditions,
        select: select_,
        group: group_,
        distinct: false,
    }
}

/// A query a derived item reads: one grouped core, or a compound of it with
/// a plain core.
fn derived(rng: &mut Rng) -> Query {
    if rng.chance(25) {
        let plain = Core {
            from: vec![table("y", pick(rng, &TABLES))],
            conditions: Vec::new(),
            select: DERIVED.iter().map(|column| out(col("y", column))).collect(),
            group: Vec::new(),
            distinct: false,
        };
        return Query {
            arms: vec![grouped(rng), plain],
            ops: vec!["UNION ALL"],
            ..Query::default()
        };
    }
    select(grouped(rng))
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let query = derived(rng);
    let (source, with) = if rng.chance(30) {
        (Source::Cte("e".to_owned()), vec![("e".to_owned(), query)])
    } else {
        (Source::Query(Box::new(query)), Vec::new())
    };
    let mut statement = if rng.chance(40) {
        // A table whose rows a condition narrows, with the grouped rows
        // joined by their key, as a LEFT JOIN or an inner join.
        let join = if rng.chance(60) { Join::Left } else { Join::Inner };
        let mut conditions = vec![super::term::condition(rng, "x", 1)];
        if rng.chance(30) {
            conditions.push(condition(rng, "d", &DERIVED));
        }
        select(Core {
            from: vec![
                table("x", pick(rng, &TABLES)),
                item("d", source, join, vec![eq(col("d", "id"), col("x", "c"))]),
            ],
            conditions,
            select: vec![out(col("x", "id")), out(col("d", "a")), out(col("d", "b"))],
            group: Vec::new(),
            distinct: false,
        })
    } else {
        around(rng, source, &DERIVED)
    };
    statement.with = with;
    statement
}
