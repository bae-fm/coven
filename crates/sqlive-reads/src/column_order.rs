//! An ordering of one column against a value, `column >= ?1`, where the
//! column is generated or of a collation other than BINARY. sqlive doesn't
//! compare such a column's values itself, so a write asks SQLite to compare
//! the changed row's, as it asks for an ORDER BY term that sorts by the
//! column (SPEC.md section 5).

use crate::from_item::ColumnValue;
use crate::scope_plan::{is_name, Planner};
use crate::scope_term::{column_comparison, operand, ScopeTerm};
use crate::sql::unparenthesized;
use rusqlite::Connection;
use sqlite3_parser::ast::{Expr, Operator};

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
    // An equality of such a column narrows nothing.
    if !matches!(
        operator,
        Operator::Less | Operator::LessEquals | Operator::Greater | Operator::GreaterEquals
    ) {
        return Ok(None);
    }
    // The side that names such a column of the item, and its affinity.
    let ordered = |named: &Expr<'_>| {
        let found = is_name(named).then(|| planner.resolve(core, named)).flatten();
        let (_, column) = found.filter(|(found, _)| *found == item)?;
        let affinity = match planner.items[item].columns.value(&column)? {
            ColumnValue::Collated(affinity, _) => *affinity,
            ColumnValue::Generated(generated) => generated.affinity,
            _ => return None,
        };
        Some((column, affinity))
    };
    let Some(((column, affinity), other, ordering, inclusive)) =
        column_comparison(*operator, left, right, ordered)
    else {
        return Ok(None);
    };
    Ok(operand(connection, other, affinity)?.map(|operand| ScopeTerm::Ordered {
        column,
        affinity,
        operand,
        ordering,
        inclusive,
        key: None,
    }))
}
