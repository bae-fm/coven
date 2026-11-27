//! An ordered LIMIT whose sort columns sit anywhere in the result, in a `*`
//! too, and in a FROM subquery's, as a spread fragment's, narrows to the rows
//! that sort at or before its last row (SPEC.md section 6).

use super::lookup::follows;
use crate::tests::TestDatabase;

/// A queue of releases, each with its release and album. No statement below
/// names a `seen` column. Places 1 to 4 hold releases r1 to r4.
const QUEUE: &str = "
CREATE TABLE queue(
    position INTEGER PRIMARY KEY, release TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
    seen INTEGER) STRICT;
CREATE TABLE releases(id TEXT PRIMARY KEY, album TEXT NOT NULL, seen INTEGER) STRICT;
CREATE TABLE albums(id TEXT PRIMARY KEY, title TEXT NOT NULL, seen INTEGER) STRICT;
INSERT INTO queue(position, release, state) VALUES
    (1, 'r1', 'queued'), (2, 'r2', 'queued'), (3, 'r3', 'queued'), (4, 'r4', 'queued');
INSERT INTO releases(id, album) VALUES ('r1', 'a1'), ('r2', 'a2'), ('r3', 'a3'), ('r4', 'a4');
INSERT INTO albums(id, title) VALUES ('a1', 'one'), ('a2', 'two'), ('a3', 'three'),
    ('a4', 'four')";

/// Writes of the rows after the window's last, each to a column the window
/// names, rerun nothing, and writes inside it do.
const WRITES: &[(&str, bool)] = &[
    // The rows are told apart by the sort and the joins, so a column the
    // statement doesn't name reruns nothing, in the window too.
    ("UPDATE queue SET seen = 1 WHERE position = 1", false),
    ("UPDATE releases SET seen = 1 WHERE id = 'r1'", false),
    ("UPDATE albums SET seen = 1 WHERE id = 'a1'", false),
    ("UPDATE queue SET state = 'active' WHERE position = 3", false),
    ("UPDATE queue SET state = 'active' WHERE position = 4", false),
    ("UPDATE releases SET album = 'a1' WHERE id = 'r4'", false),
    ("UPDATE albums SET title = 'FOUR' WHERE id = 'a4'", false),
    ("INSERT INTO queue(position, release, state) VALUES (9, 'r9', 'queued')", false),
    ("UPDATE queue SET state = 'failed' WHERE position = 2", true),
    ("UPDATE albums SET title = 'ONE' WHERE id = 'a1'", true),
    ("DELETE FROM queue WHERE position = 1", true),
    // The window now ends at place 3.
    ("UPDATE queue SET state = 'queued' WHERE position = 3", true),
    ("UPDATE queue SET state = 'queued' WHERE position = 4", false),
];

/// The queue window as an app composes it from fragments: each spread a FROM
/// subquery selected by `*`, the sort column in the first one.
#[tokio::test]
async fn a_window_of_spread_fragments_ordered_by_a_fragments_column_ends_at_its_last_row() {
    let db = TestDatabase::open(QUEUE);
    follows(
        &db,
        "SELECT entry.*, release.*, album.* \
         FROM (SELECT kept.position, kept.release AS release_id, kept.state \
               FROM queue AS kept) AS entry \
         LEFT JOIN (SELECT r.id AS release_key, r.album FROM releases AS r) AS release \
                ON release.release_key = entry.release_id \
         LEFT JOIN (SELECT a.id AS album_key, a.title FROM albums AS a) AS album \
                ON album.album_key = release.album \
         WHERE entry.position >= ?1 \
         ORDER BY entry.position LIMIT ?2",
        vec![crate::SqlValue::Integer(0), crate::SqlValue::Integer(2)],
        WRITES,
    )
    .await;
    db.close();
}

/// A sort column after a `*`, in a `*`, or named by its alias or position
/// after one, bounds the window as one listed first does: of a table, and of
/// a fragment within a fragment.
#[tokio::test]
async fn a_window_whose_sort_column_sits_after_or_in_a_star_ends_at_its_last_row() {
    for sql in [
        "SELECT r.*, q.position, q.state FROM queue AS q \
         JOIN releases AS r ON r.id = q.release ORDER BY q.position LIMIT 2",
        "SELECT * FROM queue ORDER BY position LIMIT 2",
        "SELECT r.*, q.* FROM queue AS q JOIN releases AS r ON r.id = q.release \
         ORDER BY q.position LIMIT 2",
        "SELECT outer_entry.* FROM (SELECT inner_entry.* FROM \
             (SELECT kept.position, kept.release, kept.state FROM queue AS kept) AS inner_entry) \
         AS outer_entry ORDER BY outer_entry.position LIMIT 2",
        "WITH entry AS (SELECT * FROM queue) SELECT entry.* FROM entry \
         ORDER BY entry.position LIMIT 2",
    ] {
        let db = TestDatabase::open(QUEUE);
        follows(
            &db,
            sql,
            vec![],
            &[
                ("UPDATE queue SET state = 'active' WHERE position = 3", false),
                ("UPDATE queue SET state = 'active' WHERE position = 4", false),
                ("UPDATE queue SET state = 'failed' WHERE position = 2", true),
                ("DELETE FROM queue WHERE position = 1", true),
                ("UPDATE queue SET state = 'queued' WHERE position = 3", true),
                ("UPDATE queue SET state = 'queued' WHERE position = 4", false),
            ],
        )
        .await;
        db.close();
    }
}

/// Names whose order the collations decide, in a column that declares NOCASE
/// and in one the ORDER BY collates.
const NAMES: &str = "
CREATE TABLE names(id INTEGER PRIMARY KEY, name TEXT NOT NULL COLLATE NOCASE,
    plain TEXT NOT NULL, note INTEGER) STRICT;
INSERT INTO names VALUES (1, 'a', 'a', 0), (2, 'B', 'B', 0), (3, 'c', 'c', 0),
    (4, 'D', 'D', 0)";

/// A fragment's column sorts in the collation it passes on, or the one the
/// term writes, and an expression over its columns sorts as one over the
/// table's.
#[tokio::test]
async fn a_window_ordered_by_a_fragments_column_sorts_as_the_column_does() {
    for sql in [
        "SELECT item.* FROM (SELECT n.name, n.id, n.note FROM names AS n) AS item \
         ORDER BY item.name, item.id LIMIT 2",
        "SELECT item.* FROM (SELECT n.plain AS shown, n.id, n.note FROM names AS n) AS item \
         ORDER BY item.shown COLLATE NOCASE, item.id LIMIT 2",
        "SELECT item.* FROM (SELECT n.plain AS shown, n.id, n.note FROM names AS n) AS item \
         ORDER BY lower(item.shown), item.id LIMIT 2",
    ] {
        let db = TestDatabase::open(NAMES);
        let sql = if sql.contains("lower(") {
            sql.replace("SELECT item.* FROM", "SELECT item.*, lower(item.shown) FROM")
        } else {
            sql.to_owned()
        };
        follows(
            &db,
            sql.leak(),
            vec![],
            &[
                ("UPDATE names SET note = 1 WHERE id = 3", false),
                ("UPDATE names SET note = 1 WHERE id = 4", false),
                ("INSERT INTO names VALUES (5, 'E', 'E', 0)", false),
                ("INSERT INTO names VALUES (6, 'Ab', 'Ab', 0)", true),
                ("UPDATE names SET note = 1 WHERE id = 2", false),
                ("UPDATE names SET note = 2 WHERE id = 6", true),
            ],
        )
        .await;
        db.close();
    }
}

/// A window ordered by a header's columns, then an entry's, both from
/// fragments: the entries' table takes the entries at or before the last,
/// and those under an earlier header.
#[tokio::test]
async fn a_window_ordered_across_two_fragments_bounds_both_tables() {
    let db = TestDatabase::open(
        "CREATE TABLE heads(id INTEGER PRIMARY KEY, place INTEGER NOT NULL, note INTEGER);
         CREATE TABLE lines(id INTEGER PRIMARY KEY, head INTEGER NOT NULL, place INTEGER NOT NULL,
             note INTEGER);
         CREATE INDEX lines_head ON lines(head);
         INSERT INTO heads VALUES (1, 1, 0), (2, 2, 0), (3, 3, 0);
         INSERT INTO lines VALUES (1, 1, 1, 0), (2, 1, 2, 0), (3, 2, 1, 0), (4, 2, 2, 0),
             (5, 3, 1, 0)",
    );
    follows(
        &db,
        "SELECT head.*, line.* \
         FROM (SELECT h.id AS head_id, h.place AS head_place, h.note AS head_note \
               FROM heads AS h) AS head \
         JOIN (SELECT l.id AS line_id, l.head, l.place, l.note FROM lines AS l) AS line \
           ON line.head = head.head_id \
         ORDER BY head.head_place, head.head_id, line.place, line.line_id LIMIT 3",
        vec![],
        &[
            ("UPDATE heads SET note = 1 WHERE id = 3", false),
            ("UPDATE lines SET note = 1 WHERE id = 5", false),
            ("UPDATE lines SET note = 1 WHERE id = 4", false),
            ("UPDATE lines SET note = 1 WHERE id = 3", true),
            ("UPDATE lines SET note = 1 WHERE id = 2", true),
            ("INSERT INTO lines VALUES (6, 1, 3, 0)", true),
            ("UPDATE lines SET note = 2 WHERE id = 3", false),
        ],
    )
    .await;
    db.close();
}

/// A table a fragment's LEFT JOIN adds takes no bound from the fragment: a
/// row added to it can take a row of NULLs, which sorts first, out of the
/// window.
#[tokio::test]
async fn a_column_of_a_fragments_left_join_bounds_no_table_inside_it() {
    let db = TestDatabase::open(
        "CREATE TABLE a(id INTEGER PRIMARY KEY);
         CREATE TABLE b(id INTEGER PRIMARY KEY, a INTEGER NOT NULL, place INTEGER NOT NULL);
         INSERT INTO a VALUES (1), (2), (3);
         INSERT INTO b VALUES (2, 2, 5), (3, 3, 6)",
    );
    follows(
        &db,
        "SELECT item.* FROM (SELECT x.id, y.place FROM a AS x LEFT JOIN b AS y ON y.a = x.id) \
         AS item ORDER BY item.place, item.id LIMIT 2",
        vec![],
        &[("INSERT INTO b VALUES (1, 1, 100)", true)],
    )
    .await;
    db.close();
}
