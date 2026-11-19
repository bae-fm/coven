//! Rule 6 for each row around a subquery: a subquery of one table, ordered
//! by expressions over its columns, with a LIMIT, that its own conditions
//! and equalities with the row around it narrow, depends only on the rows of
//! each window up to its LIMIT-th row. Its value in a result column, EXISTS
//! and a comparison that takes no row's value from it narrow by the window.
//! And the forms that keep its conditions' scope: an OFFSET, an order by a
//! position or by a column sqlive doesn't compare, another condition on the
//! row around it, and a comparison that takes its value as a value of its
//! table. The writes move rows between windows, change a window's rows and
//! delete the rows around it.

use super::{
    cmp, col, eq, lit, out, pick, select, table, Cond, Core, Expr, Limit, Out, Query, TABLES,
};
use crate::random_scopes::Rng;

/// Where the statement reads the subquery.
enum Place {
    /// A result column.
    Column,
    /// EXISTS or NOT EXISTS in the WHERE.
    Exists,
    /// `x.a < (subquery)`, which takes no row's value.
    Less,
    /// `x.c = (subquery)` or `x.c IN (subquery)`, which take the value of
    /// the subquery's table.
    Taken,
}

/// The subquery over the table `name`, which the item `x` around it sees,
/// returning `returned` of its rows, and returning more columns than the
/// SQL lists only when `hidden`.
fn subquery(rng: &mut Rng, name: &'static str, returned: &'static str, hidden: bool) -> Query {
    let mut conditions = Vec::new();
    // Each window holds the rows whose `c`, or `c` and `a`, equal the row
    // around it.
    if rng.chance(85) {
        conditions.push(if rng.chance(50) {
            eq(col("q", "c"), col("x", "c"))
        } else {
            eq(col("x", "c"), col("q", "c"))
        });
    }
    if rng.chance(15) {
        conditions.push(eq(col("q", "a"), col("x", "a")));
    }
    if rng.chance(50) {
        conditions.push(super::term::condition(rng, "q", 1));
    }
    // A condition of neither kind keeps the subquery whole.
    if rng.chance(8) {
        conditions.push(cmp("<", col("q", "a"), col("x", "a")));
    }
    let mut select_ = vec![out(col("q", returned))];
    let mut order = Vec::new();
    if hidden {
        let keys = [
            col("q", "a"),
            col("q", "c"),
            col("q", "b"),
            Expr::Add(Box::new(col("q", "a")), Box::new(col("q", "c"))),
            Expr::Coalesce(Box::new(col("q", "a")), Box::new(lit(-1))),
            // NOCASE, which sqlive doesn't compare.
            col("q", "n"),
        ];
        for _ in 0..1 + rng.below(2) {
            order.push((select_.len(), rng.chance(40)));
            let choices = if rng.chance(90) { 5 } else { 6 };
            select_.push(Out::Hidden(keys[rng.below(choices)].clone()));
        }
        // Most orders tell every row apart by the rowid.
        if rng.chance(70) {
            order.push((select_.len(), rng.chance(30)));
            select_.push(Out::Hidden(col("q", "id")));
        }
    } else {
        // The subquery returns one column, which it sorts by.
        order.push((0, rng.chance(40)));
    }
    Query {
        arms: vec![Core {
            from: vec![table("q", name)],
            conditions,
            select: select_,
            group: Vec::new(),
            distinct: false,
        }],
        order,
        order_named: hidden || rng.chance(85),
        limit: Some(Limit {
            count: 1 + rng.below(3) as u32,
            offset: rng.chance(10).then(|| rng.below(2) as u32),
        }),
        ..Query::default()
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let outer = pick(rng, &TABLES);
    let inner = pick(rng, &TABLES);
    let place = match rng.below(8) {
        0..=2 => Place::Column,
        3 | 4 => Place::Exists,
        5 => Place::Less,
        _ => Place::Taken,
    };
    let mut conditions = Vec::new();
    if rng.chance(40) {
        conditions.push(super::term::condition(rng, "x", 1));
    }
    let mut select_ = vec![out(col("x", "id")), out(col("x", "a"))];
    match place {
        Place::Column => {
            let returned = pick(rng, &["a", "b", "c", "id"]);
            select_.push(out(Expr::Sub(Box::new(subquery(rng, inner, returned, true)))));
        }
        Place::Exists => {
            let query = Box::new(subquery(rng, inner, "id", true));
            conditions.push(Cond::Exists(query, rng.chance(30)));
        }
        Place::Less => {
            let query = Box::new(subquery(rng, inner, "a", true));
            conditions.push(cmp("<", col("x", "a"), Expr::Sub(query)));
        }
        Place::Taken => {
            let query = Box::new(subquery(rng, inner, "c", false));
            conditions.push(if rng.chance(50) {
                eq(col("x", "c"), Expr::Sub(query))
            } else {
                Cond::InQuery(col("x", "c"), query, false)
            });
        }
    }
    select(Core {
        from: vec![table("x", outer)],
        conditions,
        select: select_,
        group: Vec::new(),
        distinct: false,
    })
}
