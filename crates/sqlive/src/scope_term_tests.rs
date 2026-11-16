//! A term's binding agrees with SQLite: the bound scope holds exactly the
//! rows a SELECT keeps for the same condition.

use super::*;
use crate::row_scope::RowImage;
use crate::scope_plan::is_name;
use rusqlite::params_from_iter;
use sqlite3_parser::{
    ast::{Cmd, OneSelect, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};
use std::sync::Arc;

/// The values the column holds in some row, as SQL and as a value.
fn column_values() -> Vec<(&'static str, Value)> {
    vec![
        ("NULL", Value::Null),
        ("0", Value::Integer(0)),
        ("1", Value::Integer(1)),
        ("2", Value::Integer(2)),
        ("'a'", Value::Text(b"a".to_vec())),
        ("'b'", Value::Text(b"b".to_vec())),
        ("x'00'", Value::Blob(vec![0])),
    ]
}

/// The scope `where_sql` gives the column `x`, bound to `parameters`.
fn bound(
    connection: &Connection,
    where_sql: &str,
    parameters: &[Value],
) -> Option<Option<RowScope>> {
    let sql = format!("SELECT 1 FROM t WHERE {where_sql}");
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let Ok(Some(Cmd::Stmt(Stmt::Select(select)))) = parser.next() else {
        panic!("{sql} is a SELECT");
    };
    let OneSelect::Select { where_clause: Some(condition), .. } = &select.body.select else {
        panic!("{sql} has a WHERE");
    };
    let column = |expr: &Expr<'_>| {
        (is_name(expr) && expr.to_string() == "x").then(|| ("x".to_owned(), Affinity::Blob))
    };
    let term = term(connection, &column, condition).unwrap()?;
    Some(term.bind(parameters))
}

#[test]
fn a_conditions_truth_compared_with_a_value_binds_as_sqlite_compares() {
    let connection = Connection::open_in_memory().unwrap();
    connection.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x)", []).unwrap();
    for (id, (value, _)) in column_values().iter().enumerate() {
        connection.execute(&format!("INSERT INTO t VALUES ({id}, {value})"), []).unwrap();
    }
    let conditions: [(&str, Vec<Value>); 6] = [
        ("x IS NULL", vec![]),
        ("x IS NOT NULL", vec![]),
        ("x = 2", vec![]),
        ("x < 2", vec![]),
        ("x >= 'b'", vec![]),
        ("x = ?1", vec![Value::Null]),
    ];
    let compared = ["0", "1", "2", "0.0", "0.5", "'a'", "x'00'", "NULL"];
    for (condition, parameters) in conditions {
        for operator in ["=", "<", "<=", ">", ">=", "IS"] {
            for value in compared {
                let sql = format!("({condition}) {operator} {value}");
                let scope = bound(&connection, &sql, &parameters)
                    .unwrap_or_else(|| panic!("{sql} narrows"))
                    .unwrap_or_else(|| panic!("{sql} binds"));
                let mut statement = connection
                    .prepare(&format!("SELECT id FROM t WHERE {sql} ORDER BY id"))
                    .unwrap();
                let kept: Vec<usize> = statement
                    .query_map(params_from_iter(&parameters), |row| {
                        Ok(row.get::<_, i64>(0)? as usize)
                    })
                    .unwrap()
                    .map(Result::unwrap)
                    .collect();
                let held: Vec<usize> = column_values()
                    .into_iter()
                    .enumerate()
                    .filter(|(_, (_, stored))| {
                        let image =
                            RowImage::new(Arc::from(["x".to_owned()]), vec![Some(stored.clone())]);
                        scope.matches(&image, &mut |_| Ok(true)).unwrap()
                    })
                    .map(|(id, _)| id)
                    .collect();
                assert_eq!(held, kept, "{sql} with {parameters:?}");
            }
        }
    }
}

/// A condition on both sides of nothing coven narrows by stays whole: a
/// truth compared with an expression, an IN, or a condition of conditions.
#[test]
fn other_truths_narrow_nothing() {
    let connection = Connection::open_in_memory().unwrap();
    for sql in
        ["(x IN (1, 2)) = 1", "(x = 1 AND x < 3) = 1", "((x = 1) = 1) = 1", "(x = 1) = abs(1)"]
    {
        assert!(bound(&connection, sql, &[]).is_none(), "{sql}");
    }
}
