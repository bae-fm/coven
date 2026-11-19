//! Rule 8: an equality between two items carries a term on one side's
//! compared column to the other side's column, whichever item the planner
//! places first: beside the other item's own term, from a table's own term,
//! and from what a derived item's arms say of the column they pass on, under
//! a LIMIT, across a compound's arms, or from an arm that groups, which says
//! nothing. The columns' affinities and collations differ, and the equality
//! sometimes compares with an expression, so some equalities carry nothing.

use super::push_down::DERIVED;
use super::{
    cmp, col, eq, item, lit, out, pick, select, table, Cond, Core, Expr, Join, Out, Query, Source,
    COLUMNS, TABLES,
};
use crate::random_scopes::Rng;

/// A condition on the column `column` of the item `alias`.
fn on_column(rng: &mut Rng, alias: &str, column: &'static str) -> Cond {
    let tested = col(alias, column);
    let k = rng.below(4) as i64;
    match rng.below(8) {
        0..=2 => eq(tested, lit(k)),
        3 => cmp(pick(rng, &["<", ">="]), tested, lit(k)),
        4 => Cond::InList(tested, vec![lit(k), lit(rng.below(4) as i64)]),
        5 => Cond::Null(tested, rng.chance(50)),
        6 => cmp("IS", tested, lit(k)),
        _ => cmp("<>", tested, lit(k)),
    }
}

/// One arm of a derived item: a table whose condition tests the column it
/// passes on as `passed`, and sometimes another of its columns.
fn arm(rng: &mut Rng, alias: &str, passed: &'static str) -> Core {
    let mut conditions = vec![on_column(rng, alias, passed)];
    if rng.chance(30) {
        conditions.push(super::term::condition(rng, alias, 1));
    }
    Core {
        from: vec![table(alias, pick(rng, &TABLES))],
        conditions,
        select: DERIVED.iter().map(|column| out(col(alias, column))).collect(),
        group: Vec::new(),
        distinct: false,
    }
}

/// The partner `y`: what it reads, the CTE it reads, if any, the conditions
/// on its rows, its columns, and the column the equality compares.
struct Partner {
    source: Source,
    with: Option<Query>,
    conditions: Vec<Cond>,
    columns: &'static [&'static str],
    compared: &'static str,
}

/// A table with a condition on the compared column, or a derived item whose
/// arms test the column they pass on.
fn partner(rng: &mut Rng) -> Partner {
    if rng.chance(40) {
        let compared = pick(rng, &COLUMNS);
        return Partner {
            source: Source::Table(pick(rng, &TABLES)),
            with: None,
            conditions: vec![on_column(rng, "y", compared)],
            columns: &COLUMNS,
            compared,
        };
    }
    let compared = pick(rng, &DERIVED);
    let query = match rng.below(5) {
        0 | 1 => select(arm(rng, "p", compared)),
        2 => Query {
            // A LIMIT keeps only rows its arm gives.
            limit: super::limit(1 + rng.below(2) as u32),
            ..select(arm(rng, "p", compared))
        },
        3 => Query {
            arms: vec![arm(rng, "p", compared), arm(rng, "q", compared)],
            ops: vec![pick(rng, &["UNION ALL", "UNION"])],
            ..Query::default()
        },
        _ => {
            // An arm that groups passes on no row's own value.
            let mut core = arm(rng, "p", compared);
            core.group = vec![col("p", compared)];
            core.select = DERIVED
                .iter()
                .map(|column| {
                    let name = Some((*column).to_owned());
                    if *column == compared {
                        Out::Expr(col("p", column), name)
                    } else {
                        Out::Count(name)
                    }
                })
                .collect();
            select(core)
        }
    };
    let (source, with) = if rng.chance(50) {
        (Source::Cte("e".to_owned()), Some(query))
    } else {
        (Source::Query(Box::new(query)), None)
    };
    Partner { source, with, conditions: Vec::new(), columns: &DERIVED, compared }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let Partner { source, with, conditions: partner_conditions, columns, compared } = partner(rng);
    let member = pick(rng, &COLUMNS);
    let value = if rng.chance(15) {
        Expr::Add(Box::new(col("y", compared)), Box::new(lit(0)))
    } else {
        col("y", compared)
    };
    let equality =
        if rng.chance(50) { eq(col("x", member), value) } else { eq(value, col("x", member)) };
    // `x`'s own term, which makes it the item the planner places first.
    let mut conditions = Vec::new();
    if rng.chance(75) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let x = table("x", pick(rng, &TABLES));
    let from = if rng.chance(25) {
        // The partner first, the equality in the WHERE.
        conditions.push(equality);
        vec![item("y", source, Join::Comma, Vec::new()), x]
    } else {
        let join = if rng.chance(20) { Join::Left } else { Join::Inner };
        vec![x, item("y", source, join, vec![equality])]
    };
    conditions.extend(partner_conditions);
    let mut select_ = super::term::columns("x");
    select_.push(out(col("y", columns[columns.len() - 1])));
    Query {
        with: with.map(|query| ("e".to_owned(), query)).into_iter().collect(),
        ..select(Core { from, conditions, select: select_, group: Vec::new(), distinct: false })
    }
}
