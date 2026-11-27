//! The narrowing one condition of a core gives: terms on one source's rows,
//! and the edges of its joins and subqueries (SPEC.md sections 5, 7 and 8).

use crate::column_order::column_order;
use crate::scope_narrowing::Narrowing;
use crate::scope_plan::{Planner, Subqueries};
use crate::scope_row::item_row_term;
use crate::scope_subquery::compared_subquery;
use crate::scope_term::{term, ScopeTerm};
use crate::select_syntax::{parts, Part};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Operator};

/// The narrowing condition `condition` of core `core` gives: what it gives
/// as a whole, and, for a row value ordered against another, what its first
/// pair's comparison does too (`crate::row_order`).
pub(crate) fn read_condition<'a>(
    connection: &Connection,
    planner: &mut Planner,
    core: usize,
    condition: &'a Expr<'a>,
    subqueries: &Subqueries<'a>,
) -> rusqlite::Result<Narrowing> {
    let mut found = read_whole(connection, planner, core, condition, subqueries)?;
    if let Some(leading) = crate::row_order::leading_comparison(condition) {
        let mut read = Vec::new();
        parts(&leading, &mut read);
        if let Some(&[item]) = referenced_items(planner, core, &read).as_deref() {
            if planner.items[item].core == core {
                let term = item_term(connection, planner, core, item, &leading, &read)?;
                found.terms.extend(term.map(|term| (item, term)));
            }
        }
    }
    Ok(found)
}

/// The term `condition`, made of the parts `read`, gives `item`, the one
/// item of core `core` whose columns it names, when it names no subquery.
fn item_term(
    connection: &Connection,
    planner: &Planner,
    core: usize,
    item: usize,
    condition: &Expr<'_>,
    read: &[Part<'_>],
) -> rusqlite::Result<Option<ScopeTerm>> {
    let column = |expr: &Expr<'_>| {
        let (found, column) = planner.resolve(core, expr)?;
        let affinity =
            (found == item).then(|| planner.items[item].columns.affinity(&column)).flatten()?;
        Some((column, affinity))
    };
    // What `term` can't read, SQLite judges row by row (`scope_row.rs`).
    let narrowed = match term(connection, &column, condition)? {
        Some(term) => Some(term),
        None => match item_row_term(planner, core, item, condition, read) {
            Some(judged) => Some(ScopeTerm::Row(judged)),
            None => column_order(connection, planner, core, item, condition)?,
        },
    };
    Ok(narrowed)
}

/// The items whose columns a condition of core `core` made of `read` names,
/// or `None` when it holds a subquery or a name sqlive can't place, either of
/// which keeps it from narrowing.
fn referenced_items(planner: &Planner, core: usize, read: &[Part<'_>]) -> Option<Vec<usize>> {
    let mut referenced = Vec::new();
    for part in read {
        let name = match part {
            Part::Name(name) => *name,
            Part::Function(..) | Part::Clock => continue,
            // A COLLATE changes only how a comparison compares: `term` and
            // `edge` refuse the comparisons it reaches, and a NULL test
            // looks through it.
            Part::Collation => continue,
            Part::Select(_) | Part::Subquery(_) | Part::Table(_) => return None,
        };
        let (item, _) = planner.resolve(core, name)?;
        if !referenced.contains(&item) {
            referenced.push(item);
        }
    }
    Some(referenced)
}

fn read_whole<'a>(
    connection: &Connection,
    planner: &mut Planner,
    core: usize,
    condition: &'a Expr<'a>,
    subqueries: &Subqueries<'a>,
) -> rusqlite::Result<Narrowing> {
    if let Some((values, select)) = compared_subquery(condition) {
        let cores = subqueries.iter().find(|(read, _)| std::ptr::eq(*read, select));
        if let Some((_, cores)) = cores {
            // A compound's rows are its arms' rows (`scope_one_of.rs`).
            if let ([value], [_, _, ..]) = (&values[..], &cores[..]) {
                return planner.compound(connection, core, value, select, cores);
            }
            let width = values.len();
            let edges = values
                .into_iter()
                .enumerate()
                .filter_map(|(position, value)| {
                    planner.subquery_edge(core, value, (position, width), select, cores)
                })
                .collect();
            return Ok(Narrowing { edges, ..Narrowing::default() });
        }
    }
    // A column equal to COALESCE of values narrows by each of them.
    if let Expr::Binary(left, Operator::Equals, right) = crate::sql::unparenthesized(condition) {
        for (member, other) in [(left, right), (right, left)] {
            if let Some(found) = planner.coalesced(connection, core, member, other, subqueries)? {
                return Ok(found);
            }
        }
    }
    let mut read = Vec::new();
    parts(condition, &mut read);
    // An OR of one source's columns alone is a term on that source; any
    // other narrows by its sides.
    let or = crate::scope_or::is_or(condition);
    if or && read.iter().any(|part| matches!(part, Part::Select(_) | Part::Subquery(_))) {
        return crate::scope_or::read_or(connection, planner, core, condition, subqueries);
    }
    let Some(referenced) = referenced_items(planner, core, &read) else {
        return Ok(Narrowing::default());
    };
    let mut found = Narrowing::default();
    if let [item] = referenced[..] {
        if planner.items[item].core != core {
            return Ok(Narrowing::default());
        }
        let term = item_term(connection, planner, core, item, condition, &read)?;
        found.terms.extend(term.map(|term| (item, term)));
    } else if or {
        return crate::scope_or::read_or(connection, planner, core, condition, subqueries);
    } else if let Expr::Binary(left, Operator::Is, right) = condition {
        // `IS` is `=` that matches a NULL with a NULL.
        for (member, other, first) in [(left, right, false), (right, left, true)] {
            found.edges.extend(planner.is_edge(core, member, other, first));
        }
    } else if let Expr::Binary(left, Operator::Equals, right) = condition {
        // A row value equality has the truth of its pairs' equalities joined
        // by AND, NULLs included, so it narrows as they do.
        let pairs: Vec<(&Expr<'_>, &Expr<'_>)> =
            match (unparenthesized(left), unparenthesized(right)) {
                (Expr::Parenthesized(lefts), Expr::Parenthesized(rights))
                    if lefts.len() == rights.len() =>
                {
                    lefts.iter().zip(rights.iter()).collect()
                }
                _ => vec![(left, right)],
            };
        for (left, right) in pairs {
            for (member, other, first) in [(left, right, false), (right, left, true)] {
                found.edges.extend(planner.edge(core, member, other, first));
            }
        }
    }
    Ok(found)
}
