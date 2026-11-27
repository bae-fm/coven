//! An ordering of one column against a value, `column >= ?1`, where the
//! column is generated or of a collation other than BINARY. sqlive doesn't
//! compare such a column's values itself, so a write asks SQLite to compare
//! the changed row's, as it asks for an ORDER BY term that sorts by the
//! column (SPEC.md section 5).

use crate::from_item::ColumnValue;
use crate::scope_plan::{is_name, Planner};
use crate::scope_term::{operand, ScopeTerm};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Operator};
use std::cmp::Ordering;

/// The term `condition` of core `core` gives item `item`, when it orders such
/// a column of the item against a value, on either side.
pub(crate) fn column_order(
    connection: &Connection,
    planner: &Planner,
    core: usize,
    item: usize,
    condition: &Expr<'_>,
) -> rusqlite::Result<Option<ScopeTerm>> {
    let Expr::Binary(left, operator, right) = unparenthesized(condition) else {
        return Ok(None);
    };
    let (ordering, inclusive) = match operator {
        Operator::Less => (Ordering::Less, false),
        Operator::LessEquals => (Ordering::Less, true),
        Operator::Greater => (Ordering::Greater, false),
        Operator::GreaterEquals => (Ordering::Greater, true),
        _ => return Ok(None),
    };
    for (named, other, ordering) in [(left, right, ordering), (right, left, ordering.reverse())] {
        let found = is_name(named).then(|| planner.resolve(core, named)).flatten();
        let Some((_, column)) = found.filter(|(found, _)| *found == item) else {
            continue;
        };
        let affinity = match planner.items[item].columns.value(&column) {
            Some(ColumnValue::Collated(affinity, _)) => *affinity,
            Some(ColumnValue::Generated(generated)) => generated.affinity,
            _ => continue,
        };
        if let Some(operand) = operand(connection, other, affinity)? {
            let key = None;
            return Ok(Some(ScopeTerm::Ordered {
                column,
                affinity,
                operand,
                ordering,
                inclusive,
                key,
            }));
        }
    }
    Ok(None)
}
