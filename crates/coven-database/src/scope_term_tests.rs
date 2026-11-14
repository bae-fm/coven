//! A term's binding agrees with SQLite: the bound scope holds exactly the
//! rows a SELECT keeps for the same condition. And the truth of a condition
//! compared with a value narrows live queries, as the windows of a list
//! sorted by a nullable column read it (§4).

use super::*;
use crate::row_scope::RowImage;
use crate::scope_lookup::tests::follows;
use crate::scope_plan::is_name;
use crate::sql_value::SqlValue::{Integer, Null};
use crate::tests::{next, TestStore};
use crate::{Database, DbError};
use rusqlite::params_from_iter;
use sqlite3_parser::{
    ast::{Cmd, OneSelect, Stmt},
    lexer::sql::Parser,
    Bump, FallibleIterator,
};
use std::sync::atomic::{AtomicUsize, Ordering};
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
                        scope.matches(&image, &mut |_, _| Ok(true)).unwrap()
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

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

/// A row with a NULL column is outside `(b = 'g') = 0`, since the comparison
/// is NULL there, and inside `(b = 'g') IS NULL`.
#[tokio::test]
async fn a_conditions_truth_compared_with_a_value_narrows() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE items(id INTEGER PRIMARY KEY, b TEXT, value INTEGER);
INSERT INTO items VALUES (1,'g',0),(2,'g',0),(3,'h',0),(4,'i',0),(5,NULL,0)",
        )
        .await
        .unwrap();
    follows(
        &db,
        "SELECT value FROM items WHERE (b = 'g') = 1",
        vec![],
        &[
            ("UPDATE items SET value = value + 1 WHERE id IN (3, 4, 5)", false),
            ("UPDATE items SET value = value + 1 WHERE id = 1", true),
            ("UPDATE items SET b = 'g' WHERE id = 3", true),
            ("UPDATE items SET b = 'x' WHERE id = 1", true),
            ("UPDATE items SET b = NULL WHERE id = 3", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT value FROM items WHERE (b = 'g') = 0",
        vec![],
        &[
            ("UPDATE items SET value = value + 1 WHERE id IN (2, 5)", false),
            ("UPDATE items SET value = value + 1 WHERE id = 4", true),
            ("UPDATE items SET b = 'k' WHERE id = 5", true),
            ("UPDATE items SET b = 'g' WHERE id = 4", true),
            ("UPDATE items SET b = NULL WHERE id = 5", true),
        ],
    )
    .await;
    follows(
        &db,
        "SELECT value FROM items WHERE (b = 'g') IS NULL",
        vec![],
        &[
            ("UPDATE items SET value = value + 1 WHERE id IN (1, 2, 4)", false),
            ("UPDATE items SET value = value + 1 WHERE id = 5", true),
            ("UPDATE items SET b = 'z' WHERE id = 5", true),
            ("UPDATE items SET b = NULL WHERE id = 2", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// One window's seeks: each holds a prefix of the order's terms equal and
/// compares the next, with the values it is bound to.
type Seeks = &'static [(&'static str, &'static [Value])];

const ALBUMS_BY_YEAR: &str =
    "CREATE TABLE albums(id INTEGER PRIMARY KEY, year INTEGER, title TEXT);
INSERT INTO albums VALUES (1,1990,''),(2,1995,''),(3,2000,''),(4,2000,''),(5,2000,''),(6,2005,''),
 (7,2010,''),(8,2010,''),(9,2012,''),(10,NULL,''),(11,NULL,''),(12,NULL,''),(13,NULL,'')";

/// Read the window `seeks` of albums ordered by `year IS NULL, year, id` as
/// a live query. A write to an album outside it wakes nothing; one to an
/// album inside it, and each of `moves`, which put a row in or take one out,
/// does.
async fn window(seeks: Seeks, outside: &[i64], inside: &[i64], moves: &[&str]) {
    let store = TestStore::new();
    let db = store.schema(vec![], ALBUMS_BY_YEAR).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(move |db| {
        count.fetch_add(1, Ordering::SeqCst);
        let mut rows = Vec::new();
        for (seek, parameters) in seeks {
            let sql = format!("SELECT id, year, title FROM albums WHERE {seek}");
            rows.extend(db.query(&sql, rusqlite::params_from_iter(*parameters), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Option<i64>>(1)?, r.get::<_, String>(2)?))
            })?);
        }
        Ok(rows)
    });
    next(&mut query).await;
    for id in outside {
        write(&db, &format!("UPDATE albums SET title = title || '.' WHERE id = {id}"));
        assert!(!query.is_marked_for_rerun(), "outside: album {id}, {seeks:?}");
    }
    for id in inside {
        write(&db, &format!("UPDATE albums SET title = title || '.' WHERE id = {id}"));
        next(&mut query).await;
    }
    for sql in moves {
        write(&db, sql);
        next(&mut query).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1 + inside.len() + moves.len(), "{seeks:?}");
    db.close().await.unwrap();
}

/// Ben's album grid sorted by year lists the albums with no year last. A
/// window over the years, one across the boundary into the albums with no
/// year, and one over those alone, each as the seeks of a run of the order.
#[tokio::test]
async fn windows_of_a_list_sorted_by_a_nullable_column_narrow() {
    // Over the years, after (2000, 4) and before (2010, 8).
    window(
        &[
            (
                "(year IS NULL) = ?1 AND year = ?2 AND id > ?3",
                &[Integer(0), Integer(2000), Integer(4)],
            ),
            (
                "(year IS NULL) = ?1 AND year > ?2 AND year < ?3",
                &[Integer(0), Integer(2000), Integer(2010)],
            ),
            (
                "(year IS NULL) = ?1 AND year = ?2 AND id < ?3",
                &[Integer(0), Integer(2010), Integer(8)],
            ),
        ],
        &[1, 2, 3, 4, 8, 9, 10, 11, 12, 13],
        &[5, 6, 7],
        &[
            "UPDATE albums SET year = 2005 WHERE id = 10",
            "UPDATE albums SET year = 2003 WHERE id = 9",
            "UPDATE albums SET year = NULL WHERE id = 6",
            "UPDATE albums SET year = 1999 WHERE id = 5",
        ],
    )
    .await;
    // Across the boundary, after (2010, 7) and before (NULL, 12).
    window(
        &[
            (
                "(year IS NULL) = ?1 AND year = ?2 AND id > ?3",
                &[Integer(0), Integer(2010), Integer(7)],
            ),
            ("(year IS NULL) = ?1 AND year > ?2", &[Integer(0), Integer(2010)]),
            ("(year IS NULL) > ?1 AND (year IS NULL) < ?2", &[Integer(0), Integer(1)]),
            ("(year IS NULL) = ?1 AND year IS ?2 AND id < ?3", &[Integer(1), Null, Integer(12)]),
        ],
        &[1, 2, 3, 4, 5, 6, 7, 12, 13],
        &[8, 9, 10, 11],
        &[
            "UPDATE albums SET year = 2012 WHERE id = 12",
            "UPDATE albums SET year = 2011 WHERE id = 1",
            "UPDATE albums SET year = 1990 WHERE id = 9",
            "UPDATE albums SET year = 2001 WHERE id = 10",
        ],
    )
    .await;
    // Over the albums with no year alone, after id 10 and before id 13.
    window(
        &[(
            "(year IS NULL) = ?1 AND year IS ?2 AND id > ?3 AND id < ?4",
            &[Integer(1), Null, Integer(10), Integer(13)],
        )],
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13],
        &[11, 12],
        &[
            "UPDATE albums SET year = 2000 WHERE id = 11",
            "UPDATE albums SET year = NULL WHERE id = 11",
        ],
    )
    .await;
    // Descending, from the albums with no year into the years, after (NULL,
    // 11) and before (2010, 8).
    window(
        &[
            ("(year IS NULL) = ?1 AND year IS ?2 AND id < ?3", &[Integer(1), Null, Integer(11)]),
            ("(year IS NULL) < ?1 AND (year IS NULL) > ?2", &[Integer(1), Integer(0)]),
            ("(year IS NULL) = ?1 AND year > ?2", &[Integer(0), Integer(2010)]),
            (
                "(year IS NULL) = ?1 AND year = ?2 AND id > ?3",
                &[Integer(0), Integer(2010), Integer(8)],
            ),
        ],
        &[1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 13],
        &[9, 10],
        &[
            "UPDATE albums SET year = 2020 WHERE id = 7",
            "UPDATE albums SET year = 1999 WHERE id = 10",
        ],
    )
    .await;
}
