//! A row value ordered against another, `(a1, ..., an) >= (b1, ..., bn)`, or
//! by `>`, `<=` or `<`, is true only where its first pair orders the same way
//! or ties: `a1 >= b1`. SQLite compares that pair as it compares `a1 >= b1`
//! alone, and gives NULL where either value is NULL, so sqlive narrows by the
//! comparison too (SPEC.md section 5). It gives a window that starts at a key
//! the first term's bound there.

use crate::sql::unparenthesized;
use sqlite3_parser::ast::{Expr, Operator};

/// The comparison of its first pair that `condition` holds only with, when
/// it orders one row value against another.
pub(crate) fn leading_comparison<'a>(condition: &'a Expr<'a>) -> Option<Expr<'a>> {
    let Expr::Binary(left, operator, right) = unparenthesized(condition) else {
        return None;
    };
    let tied = match operator {
        Operator::Less | Operator::LessEquals => Operator::LessEquals,
        Operator::Greater | Operator::GreaterEquals => Operator::GreaterEquals,
        _ => return None,
    };
    let (Expr::Parenthesized(left), Expr::Parenthesized(right)) =
        (unparenthesized(left), unparenthesized(right))
    else {
        return None;
    };
    // A parenthesized expression of one value is that value.
    (left.len() > 1 && left.len() == right.len()).then(|| Expr::Binary(&left[0], tied, &right[0]))
}
