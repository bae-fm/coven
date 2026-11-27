//! A term's binding agrees with SQLite: the bound scope holds exactly the
//! rows a SELECT keeps for the same condition.

use super::*;
use crate::row_scope::RowImage;
use crate::scope_plan::is_name;
use crate::scope_row::RowColumn;
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
    Some(term.bind(RunValues::Run(parameters)))
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

/// coven does not narrow the scope when the true or false result of a
/// condition is compared with an expression, an IN list, or conditions
/// joined with AND.
#[test]
fn other_truths_narrow_nothing() {
    let connection = Connection::open_in_memory().unwrap();
    for sql in
        ["(x IN (1, 2)) = 1", "(x = 1 AND x < 3) = 1", "((x = 1) = 1) = 1", "(x = 1) = abs(1)"]
    {
        assert!(bound(&connection, sql, &[]).is_none(), "{sql}");
    }
}

/// The columns a term tests, each once, in the order it first tests them:
/// a column's own, the column of a condition whose truth it compares, and
/// those of a condition SQLite judges row by row.
#[test]
fn a_term_lists_each_column_it_tests_once() {
    let null = |column: &str| ScopeTerm::Null { column: column.to_owned(), not: false };
    let truth = |column: &str| ScopeTerm::Truth {
        condition: Truth::Null { column: column.to_owned(), not: false },
        ordering: Ordering::Equal,
        inclusive: false,
        is: false,
        operand: Operand::Constant(Value::Integer(1)),
    };
    let row = |columns: &[&str]| {
        ScopeTerm::Row(Arc::new(RowTerm {
            sql: String::new(),
            qualifier: "t".to_owned(),
            columns: columns
                .iter()
                .map(|column| RowColumn::written((*column).to_owned(), Affinity::Blob))
                .collect(),
            passed_to: None,
        }))
    };
    let term = null("a").and(truth("b")).and(truth("a")).and(row(&["c", "b", "d"]));
    let mut columns = Vec::new();
    term.columns(&mut columns);
    assert_eq!(columns, ["a", "b", "c", "d"]);
}
