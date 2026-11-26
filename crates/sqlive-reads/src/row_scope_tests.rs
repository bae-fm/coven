//! Numbers compare as SQLite compares them.

use super::*;

#[test]
fn numbers_compare_exactly_as_sqlite_does() {
    use std::cmp::Ordering::{Equal, Greater, Less};
    let cases = [
        (Value::Integer(9_007_199_254_740_993), Value::Real(9_007_199_254_740_992.0), Greater),
        (Value::Real(-0.0), Value::Integer(0), Equal),
        (Value::Real(-0.0), Value::Real(0.0), Equal),
        (Value::Integer(i64::MAX), Value::Real(9.3e18), Less),
        (Value::Integer(i64::MIN), Value::Real(-9.3e18), Greater),
        (Value::Integer(i64::MIN), Value::Real(-9_223_372_036_854_775_808.0), Equal),
        (Value::Integer(-1), Value::Real(-1.5), Greater),
        (Value::Integer(1), Value::Real(1.5), Less),
        (Value::Integer(2), Value::Integer(10), Less),
        (Value::Real(1.5), Value::Real(-2.5), Greater),
    ];
    for (left, right, expected) in cases {
        assert_eq!(compare(&left, &right), Some(expected), "{left:?} against {right:?}");
        assert_eq!(compare(&right, &left), Some(expected.reverse()), "{right:?} against {left:?}");
    }
}

fn integers(column: &str, values: &[i64]) -> RowScope {
    RowScope::equal(column.to_owned(), values.iter().map(|value| Value::Integer(*value)).collect())
}

/// An OR holds only the values both sides give: a side that says nothing of
/// the column leaves the OR open to every value.
#[test]
fn an_or_gives_the_equal_values_of_both_its_sides() {
    let both = RowScope::Or(Box::new(integers("a", &[1])), Box::new(integers("a", &[2])));
    assert_eq!(both.equal_values("a"), Some(vec![Value::Integer(1), Value::Integer(2)]));
    let other_column = RowScope::Or(Box::new(integers("a", &[1])), Box::new(integers("b", &[2])));
    assert_eq!(other_column.equal_values("a"), None);
    let open = RowScope::Or(Box::new(integers("a", &[1])), Box::new(RowScope::All));
    assert_eq!(open.equal_values("a"), None);
}
