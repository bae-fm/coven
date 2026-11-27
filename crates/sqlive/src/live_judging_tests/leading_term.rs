//! A window ordered by a column of a table and then by expressions sqlive
//! can't bound narrows by its leading terms: to the rows that sort no later
//! than its last row on them, and from the key its WHERE starts it at
//! (SPEC.md section 6).

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue;

/// A list of albums by title, each with a cover. Titles b, d, f, h, j and l
/// are albums b to l.
const LIBRARY: &str = "
CREATE TABLE listing(id TEXT PRIMARY KEY, title TEXT NOT NULL, year INTEGER NOT NULL,
    seen INTEGER) STRICT;
CREATE INDEX listing_title ON listing(title, id);
CREATE TABLE covers(album TEXT PRIMARY KEY, version INTEGER NOT NULL) STRICT;
INSERT INTO listing(id, title, year) VALUES ('b', 'b', 1), ('d', 'd', 2), ('f', 'f', 3),
    ('h', 'h', 4), ('j', 'j', 5), ('l', 'l', 6);
INSERT INTO covers VALUES ('b', 1), ('d', 1), ('f', 1), ('h', 1), ('j', 1), ('l', 1)";

/// The window of two albums from title f: f and h.
const WINDOW: &str = "
    SELECT entry.title, entry.second, entry.id, card.version
    FROM (SELECT l.id, l.title,
                 CASE ?1 WHEN 'year' THEN l.year ELSE 0 END AS second
          FROM listing AS l) AS entry
    CROSS JOIN (SELECT c.album, c.version FROM covers AS c) AS card ON card.album = entry.id
    WHERE (entry.title, entry.second, entry.id) >= (?2, ?3, ?4)
    ORDER BY entry.title, entry.second, entry.id
    LIMIT ?5";

#[tokio::test]
async fn a_window_ends_where_its_leading_term_passes_its_last_row() {
    let db = TestDatabase::open(LIBRARY);
    follows(
        &db,
        WINDOW,
        vec![
            SqlValue::Text(b"year".to_vec()),
            SqlValue::Text(b"f".to_vec()),
            SqlValue::Integer(0),
            SqlValue::Text(Vec::new()),
            SqlValue::Integer(2),
        ],
        &[
            // Past the last row, h.
            ("UPDATE listing SET year = 60 WHERE id = 'j'", false),
            ("UPDATE covers SET version = 2 WHERE album = 'j'", false),
            ("INSERT INTO listing(id, title, year) VALUES ('k', 'k', 1)", false),
            ("DELETE FROM listing WHERE id = 'l'", false),
            // Before the starting key.
            ("UPDATE listing SET year = 20 WHERE id = 'd'", false),
            ("UPDATE covers SET version = 2 WHERE album = 'd'", false),
            ("INSERT INTO listing(id, title, year) VALUES ('c', 'c', 1)", false),
            // Inside.
            ("UPDATE covers SET version = 2 WHERE album = 'f'", true),
            ("UPDATE listing SET year = 40 WHERE id = 'h'", true),
            ("INSERT INTO listing(id, title, year) VALUES ('g', 'g', 1)", true),
        ],
    )
    .await;
    db.close();
}

/// A leading column in a collation of its own, or a generated one, sorts and
/// starts the window by what the ORDER BY compares: in NOCASE, `F` sorts with
/// `f`. The window runs down from `h`.
#[tokio::test]
async fn a_collated_or_generated_leading_term_bounds_a_window() {
    for title in [
        "title TEXT NOT NULL COLLATE NOCASE, name TEXT AS (title)",
        "name TEXT NOT NULL, title TEXT NOT NULL AS (upper(name))",
        "name TEXT NOT NULL, title TEXT NOT NULL COLLATE NOCASE AS (name)",
    ] {
        let named = if title.starts_with("name") { "name" } else { "title" };
        let schema = format!(
            "CREATE TABLE listing(id TEXT PRIMARY KEY, {title}, year INTEGER NOT NULL) STRICT;
             INSERT INTO listing(id, {named}, year) VALUES ('b', 'B', 1), ('d', 'd', 2),
                 ('f', 'F', 3), ('h', 'h', 4), ('j', 'J', 5)"
        );
        let db = TestDatabase::open(&schema);
        let insert = |id: &str, value: &str| {
            format!("INSERT INTO listing(id, {named}, year) VALUES ('{id}', '{value}', 1)")
        };
        let (past, before, inside) = (insert("c", "C"), insert("i", "I"), insert("g", "G"));
        follows(
            &db,
            "SELECT entry.title, entry.second, entry.id
             FROM (SELECT l.id, l.title, l.year + 0 AS second FROM listing AS l) AS entry
             WHERE (entry.title, entry.second, entry.id) <= (?1, ?2, ?3)
             ORDER BY entry.title DESC, entry.second DESC, entry.id DESC
             LIMIT 2",
            vec![SqlValue::Text(b"H".to_vec()), SqlValue::Integer(100), SqlValue::Text(Vec::new())],
            &[
                ("UPDATE listing SET year = 50 WHERE id = 'j'", false),
                (&before, false),
                ("UPDATE listing SET year = 20 WHERE id = 'd'", false),
                (&past, false),
                ("UPDATE listing SET year = 30 WHERE id = 'f'", true),
                (&inside, true),
            ],
        )
        .await;
        db.close();
    }
}
