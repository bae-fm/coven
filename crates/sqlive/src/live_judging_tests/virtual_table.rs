//! A contentful FTS5 table, read as a table whose rows its content table
//! holds: a search narrows the tables beside it to the rows it finds, and
//! its own rows to those its conditions pass. A statement that names a
//! hidden column, as `rank`, and one that reads a contentless FTS5 table,
//! narrow nothing.

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue;

const SEARCH: &str = "CREATE TABLE albums(id TEXT PRIMARY KEY, year INTEGER);
    CREATE VIRTUAL TABLE album_search USING fts5(album_id UNINDEXED, title, tokenize='trigram');
    CREATE VIEW lit AS SELECT album_id FROM album_search WHERE title GLOB '*light*';
    INSERT INTO albums VALUES ('a1', 2001), ('a2', 2002), ('a3', 2003);
    INSERT INTO album_search(rowid, album_id, title)
        VALUES (1, 'a1', 'first light'), (2, 'a2', 'second wave'), (3, 'a3', 'third light')";

/// The library search: the albums whose search row's title matches the
/// pattern. A write to another album doesn't rerun it, and one to an album
/// it finds does. A search row the pattern matches reruns it as it comes,
/// changes or goes, and one it doesn't match doesn't. The same holds through
/// a view whose own condition searches the table.
#[tokio::test]
async fn a_virtual_table_gives_the_tables_beside_it_values() {
    for sql in [
        "SELECT a.year FROM albums a WHERE a.id IN (SELECT album_id FROM album_search WHERE title GLOB ?1)",
        "SELECT a.year FROM albums a WHERE a.id IN (SELECT album_id FROM lit WHERE ?1 IS NOT NULL)",
        "SELECT a.year FROM album_search s JOIN albums a ON a.id = s.album_id WHERE s.title GLOB ?1",
    ] {
        let db = TestDatabase::open(SEARCH);
        follows(&db, sql, vec![SqlValue::Text(b"*light*".to_vec())], &[
            ("UPDATE albums SET year = year + 10 WHERE id = 'a2'", false),
            ("UPDATE albums SET year = year + 10 WHERE id = 'a1'", true),
            ("INSERT INTO album_search(rowid, album_id, title) VALUES (9, 'a9', 'no match')", false),
            ("UPDATE album_search SET title = 'more light' WHERE rowid = 9", true),
            ("UPDATE album_search SET album_id = 'a2' WHERE rowid = 9", true),
            ("UPDATE albums SET year = year + 10 WHERE id = 'a2'", true),
            ("DELETE FROM album_search WHERE rowid = 9", true),
            ("UPDATE album_search SET title = 'second tide' WHERE rowid = 2", false),
        ])
        .await;
        db.close();
    }
}

/// A statement that names the search table's `rank`, or the column that
/// bears its name, as MATCH does, depends on every row of the table: a
/// search row the pattern doesn't match reruns it.
#[tokio::test]
async fn a_hidden_column_keeps_the_virtual_table_whole() {
    for (sql, pattern) in [
        (
            "SELECT a.year FROM album_search s JOIN albums a ON a.id = s.album_id WHERE s.title GLOB ?1 ORDER BY s.rank",
            "*light*",
        ),
        (
            "SELECT a.year FROM albums a WHERE a.id IN (SELECT album_id FROM album_search WHERE album_search MATCH ?1)",
            "light",
        ),
    ] {
        let db = TestDatabase::open(SEARCH);
        follows(&db, sql, vec![SqlValue::Text(pattern.as_bytes().to_vec())], &[(
            "INSERT INTO album_search(rowid, album_id, title) VALUES (9, 'a9', 'no match')",
            true,
        )])
        .await;
        db.close();
    }
}

/// A name sqlive can't resolve keeps a statement whole only where it can name
/// a hidden column: the result column `rank` the outer SELECT names doesn't
/// read the search table's `rank`, which its subquery reads in a core of its
/// own, so the statement still narrows `albums` and the search table.
#[tokio::test]
async fn a_hidden_column_name_outside_the_virtual_tables_scope_narrows() {
    let db = TestDatabase::open(SEARCH);
    follows(
        &db,
        "SELECT a.year AS rank FROM albums a WHERE a.id IN (SELECT album_id FROM album_search WHERE title GLOB ?1) ORDER BY rank",
        vec![SqlValue::Text(b"*light*".to_vec())],
        &[
            ("UPDATE albums SET year = year + 10 WHERE id = 'a2'", false),
            ("INSERT INTO album_search(rowid, album_id, title) VALUES (9, 'a9', 'no match')", false),
            ("UPDATE albums SET year = year + 10 WHERE id = 'a1'", true),
        ],
    )
    .await;
    db.close();
}

/// A contentless FTS5 table keeps no rows sqlive can see change, so a
/// statement that reads it narrows nothing.
#[tokio::test]
async fn a_contentless_virtual_table_keeps_the_statement_whole() {
    let db = TestDatabase::open(
        "CREATE TABLE albums(id INTEGER PRIMARY KEY, year INTEGER);
        CREATE VIRTUAL TABLE album_search USING fts5(title, content='');
        INSERT INTO albums VALUES (1, 2001), (2, 2002);
        INSERT INTO album_search(rowid, title) VALUES (1, 'first light')",
    );
    follows(
        &db,
        "SELECT a.year FROM albums a WHERE a.id IN (SELECT rowid FROM album_search WHERE album_search MATCH 'light')",
        vec![],
        &[
            ("UPDATE albums SET year = year + 10 WHERE id = 2", true),
            ("INSERT INTO album_search(rowid, title) VALUES (5, 'dark')", true),
        ],
    )
    .await;
    db.close();
}
