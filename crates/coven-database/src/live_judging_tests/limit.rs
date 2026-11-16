//! A statement that orders by columns of one table and returns as many rows
//! as its LIMIT depends only on the rows that sort at or before its last
//! row, as a list window read as one live query does (§4).

use super::lookup::follows;
use crate::tests::TestStore;
use sqlive::SqlValue::{Integer, Text};

const NOTES: &str = "CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, body TEXT);
INSERT INTO notes VALUES (1,'b',''),(2,'d',''),(3,'f',''),(4,'h',''),(5,'j',''),(6,'l','')";

/// The window from `c` holds d, f and h. A change after h reruns nothing, and
/// a change inside the window reruns it. Each rerun moves the bound to the
/// window's new last row.
#[tokio::test]
async fn a_full_window_reruns_only_for_rows_up_to_its_last() {
    let store = TestStore::new();
    let db = store.schema(vec![], NOTES).await.unwrap();
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= ?1 ORDER BY title LIMIT ?2",
        vec![Text(b"c".to_vec()), Integer(3)],
        &[
            ("INSERT INTO notes VALUES (7,'k','')", false),
            ("INSERT INTO notes VALUES (8,'z','')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'j'", false),
            ("INSERT INTO notes VALUES (9,'a','')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'f'", true),
            // The window now holds d, e and f.
            ("INSERT INTO notes VALUES (10,'e','')", true),
            ("INSERT INTO notes VALUES (11,'g','')", false),
            ("UPDATE notes SET body = 'y' WHERE title = 'h'", false),
            // After deleting e, the window holds d, f and g.
            ("DELETE FROM notes WHERE title = 'e'", true),
            ("UPDATE notes SET body = 'y' WHERE title = 'g'", true),
            ("UPDATE notes SET body = 'z' WHERE title = 'h'", false),
            // A row equal to the last one could sort before it.
            ("INSERT INTO notes VALUES (12,'g','')", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A row that moves into the window from after it, or out of the window,
/// reruns it.
#[tokio::test]
async fn a_row_moving_into_or_out_of_a_full_window_reruns_it() {
    let store = TestStore::new();
    let db = store.schema(vec![], NOTES).await.unwrap();
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= 'c' ORDER BY title LIMIT 3",
        vec![],
        &[
            // From after the window into it: d, e and f.
            ("UPDATE notes SET title = 'e' WHERE id = 6", true),
            // Out of the window: e, f and h.
            ("UPDATE notes SET title = 'y' WHERE id = 2", true),
            ("UPDATE notes SET body = 'x' WHERE id = 2", false),
            ("UPDATE notes SET body = 'x' WHERE id = 4", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A window that returned fewer rows than its LIMIT already holds every row
/// after its start, so a row added anywhere after the start reruns it.
#[tokio::test]
async fn a_short_window_reruns_for_a_row_anywhere_after_its_start() {
    let store = TestStore::new();
    let db = store.schema(vec![], NOTES).await.unwrap();
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title >= 'i' ORDER BY title LIMIT 5",
        vec![],
        &[
            ("INSERT INTO notes VALUES (7,'a','')", false),
            ("INSERT INTO notes VALUES (8,'z','')", true),
            ("INSERT INTO notes VALUES (9,'zz','')", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A descending window ends at its smallest row, and sorts NULLs last.
#[tokio::test]
async fn a_descending_window_ends_at_its_smallest_row() {
    let store = TestStore::new();
    let db = store.schema(vec![], NOTES).await.unwrap();
    // j, h and f.
    follows(
        &db,
        "SELECT title, id, body FROM notes WHERE title <= 'k' OR title IS NULL ORDER BY title DESC LIMIT 3",
        vec![],
        &[
            ("INSERT INTO notes VALUES (7,'c','')", false),
            ("INSERT INTO notes VALUES (8,NULL,'')", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'd'", false),
            ("UPDATE notes SET body = 'x' WHERE title = 'h'", true),
            ("INSERT INTO notes VALUES (9,'g','')", true),
            ("INSERT INTO notes VALUES (10,'k','')", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// A window over two columns, whose last row ties with later rows on the
/// first column, ends at that row's second column. NULLs sort first.
#[tokio::test]
async fn a_window_over_two_columns_ends_within_a_tie() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE albums(id INTEGER PRIMARY KEY, year INTEGER, title TEXT);
INSERT INTO albums VALUES (10,2000,''),(20,2000,''),(30,2005,''),(40,2005,''),(50,2010,'')",
        )
        .await
        .unwrap();
    // 10, 20 and 30.
    follows(
        &db,
        "SELECT title, year, id FROM albums ORDER BY year, id LIMIT 3",
        vec![],
        &[
            ("INSERT INTO albums VALUES (35,2005,'')", false),
            ("UPDATE albums SET title = 'x' WHERE id = 40", false),
            ("UPDATE albums SET title = 'x' WHERE id = 30", true),
            // 10, 20 and 25.
            ("INSERT INTO albums VALUES (25,2005,'')", true),
            ("UPDATE albums SET title = 'y' WHERE id = 30", false),
            // 5, 10 and 20.
            ("INSERT INTO albums VALUES (5,NULL,'')", true),
            ("INSERT INTO albums VALUES (60,2000,'')", false),
            ("INSERT INTO albums VALUES (15,2000,'')", true),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// When coven narrows `tracks` by the releases the window joins them to, it
/// looks up only the releases up to the window's last row: a change to a track
/// of a release after the window reruns nothing.
#[tokio::test]
async fn a_join_looks_up_only_the_sorted_rows_up_to_the_last_row() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![],
            "CREATE TABLE releases(id TEXT NOT NULL PRIMARY KEY, title TEXT);
CREATE TABLE tracks(id INTEGER PRIMARY KEY, release TEXT, title TEXT);
INSERT INTO releases VALUES ('r1','a'),('r2','b'),('r3','c');
INSERT INTO tracks VALUES (1,'r1','one'),(2,'r2','two'),(3,'r3','three')",
        )
        .await
        .unwrap();
    // a's track and b's.
    follows(
        &db,
        "SELECT t.title, r.title, r.id FROM releases r JOIN tracks t ON t.release = r.id ORDER BY r.title, r.id LIMIT 2",
        vec![],
        &[
            ("INSERT INTO tracks VALUES (4,'r3','four')", false),
            ("UPDATE tracks SET title = 'tres' WHERE id = 3", false),
            ("UPDATE tracks SET title = 'dos' WHERE id = 2", true),
            // a's two tracks.
            ("INSERT INTO tracks VALUES (5,'r1','five')", true),
            ("UPDATE tracks SET title = 'two' WHERE id = 2", false),
        ],
    )
    .await;
    db.close().await.unwrap();
}

/// With an OFFSET, an ORDER BY column the statement doesn't return, an ORDER
/// BY expression or a COLLATE, the scope stays as the WHERE gives it, so a row
/// added after the window's last row reruns the window.
#[tokio::test]
async fn an_offset_an_unreturned_column_or_an_expression_narrows_nothing_more() {
    let store = TestStore::new();
    let db = store.schema(vec![], NOTES).await.unwrap();
    for sql in [
        "SELECT title, id FROM notes ORDER BY title LIMIT 2 OFFSET 1",
        "SELECT id FROM notes ORDER BY title LIMIT 2",
        "SELECT title, id FROM notes ORDER BY lower(title) LIMIT 2",
        "SELECT title, id FROM notes ORDER BY title COLLATE NOCASE LIMIT 2",
    ] {
        follows(&db, sql, vec![], &[("INSERT INTO notes VALUES (7,'z','')", true)]).await;
        db.commit_writer(|db, _| db.batch("DELETE FROM notes WHERE id = 7").unwrap());
    }
    db.close().await.unwrap();
}
