//! The truth of a condition compared with a value narrows live queries, as
//! the windows of a list sorted by a nullable column read it (§4).

use super::lookup::follows;
use crate::tests::{next, TestStore};
use crate::{Database, DbError};
use sqlive::SqlValue::{self as Value, Integer, Null};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
