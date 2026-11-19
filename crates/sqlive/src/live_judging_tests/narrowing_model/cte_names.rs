//! Two CTEs of one name: a WITH inside a FROM subquery, a CTE's query or an
//! expression subquery declares `e`, which the statement's own WITH declares
//! too. Each FROM item that names `e` reads the innermost `e` visible where
//! it is, and the statement's `f`, whose query names `e`, reads the outer
//! one wherever the statement reads `f`.

use super::nested_derived::reading;
use super::push_down::{around, condition, derived, DERIVED};
use super::{col, item, out, select, Cond, Core, Join, Query, Source};
use crate::random_scopes::Rng;

/// Every column of the CTE `name`, read as `alias`, under a condition
/// sometimes.
fn every(rng: &mut Rng, alias: &str, name: &str) -> Core {
    let mut conditions = Vec::new();
    if rng.chance(40) {
        conditions.push(condition(rng, alias, &DERIVED));
    }
    Core {
        from: vec![item(alias, Source::Cte(name.to_owned()), Join::Comma, Vec::new())],
        conditions,
        select: DERIVED.iter().map(|column| out(col(alias, column))).collect(),
        group: Vec::new(),
        distinct: false,
    }
}

/// A query with a WITH of its own that declares `e` again, which reads that
/// `e`, and `f` beside it sometimes.
fn shadowing(rng: &mut Rng) -> Query {
    let mut arms = vec![every(rng, "q", "e")];
    if rng.chance(50) {
        arms.push(every(rng, "r", "f"));
    }
    Query {
        with: vec![("e".to_owned(), derived(rng))],
        ops: if arms.len() > 1 { vec!["UNION ALL"] } else { Vec::new() },
        arms,
        ..Query::default()
    }
}

pub(super) fn statement(rng: &mut Rng) -> Query {
    let mut with =
        vec![("e".to_owned(), derived(rng)), ("f".to_owned(), select(every(rng, "p", "e")))];
    let mut statement = match rng.below(3) {
        0 => {
            let inner = shadowing(rng);
            around(rng, Source::Query(Box::new(inner)), &DERIVED)
        }
        1 => {
            let inner = shadowing(rng);
            with.push(("g".to_owned(), inner));
            around(rng, Source::Cte("g".to_owned()), &DERIVED)
        }
        _ => {
            // The outer `e` read in the statement's FROM, the inner one in a
            // subquery beside it.
            let mut inside = reading(rng, Source::Cte("e".to_owned()), "o");
            inside.with.push(("e".to_owned(), derived(rng)));
            let mut core = every(rng, "o", "e");
            core.conditions.push(Cond::Exists(Box::new(inside), rng.chance(30)));
            select(core)
        }
    };
    statement.with = with;
    statement
}
