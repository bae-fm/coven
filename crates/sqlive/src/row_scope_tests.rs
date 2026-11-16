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
