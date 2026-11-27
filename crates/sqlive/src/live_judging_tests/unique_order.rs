//! A LIMIT whose ORDER BY and joins tell its rows apart depends only on the
//! columns it names: each FROM item is told apart by a unique key that the
//! ORDER BY sorts by or that equalities with the items already told apart
//! compare (SPEC.md section 6).

use super::lookup::follows;
use crate::tests::TestDatabase;

/// A list of releases under shelf headers, and the tables a row of it reads.
/// No statement below names a `seen` column. Release 11 shows under header
/// 10, and releases 21 and 22 under header 20.
const SHELF: &str = "
CREATE TABLE placement(
    entry INTEGER PRIMARY KEY, root BLOB NOT NULL, item TEXT NOT NULL,
    key BLOB NOT NULL, shelf BLOB, unit INTEGER NOT NULL, sort INTEGER NOT NULL,
    seen INTEGER NOT NULL DEFAULT 0, UNIQUE (root, item, key)) STRICT;
CREATE INDEX placement_units ON placement(sort, entry) WHERE unit = entry;
CREATE INDEX placement_under ON placement(unit, sort, entry);
CREATE TABLE candidate(
    root BLOB NOT NULL, path BLOB NOT NULL, hash TEXT, revision INTEGER NOT NULL,
    kind TEXT NOT NULL, seen INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (root, path)) STRICT;
CREATE TABLE state(hash TEXT PRIMARY KEY, revision INTEGER NOT NULL, seen INTEGER) STRICT;
CREATE TABLE origin(
    release TEXT NOT NULL PRIMARY KEY, hash TEXT NOT NULL UNIQUE, seen INTEGER) STRICT;
CREATE TABLE storage(release TEXT PRIMARY KEY, queued INTEGER NOT NULL, seen INTEGER) STRICT;
CREATE TABLE sending(id INTEGER PRIMARY KEY, release TEXT NOT NULL, seen INTEGER) STRICT;
CREATE TABLE image(
    root BLOB NOT NULL, path BLOB NOT NULL, position INTEGER NOT NULL,
    name TEXT NOT NULL, seen INTEGER, PRIMARY KEY (root, path, position)) STRICT;
CREATE TABLE cover(release TEXT PRIMARY KEY, version INTEGER NOT NULL, seen INTEGER) STRICT;
CREATE TABLE shelf_count(
    root BLOB NOT NULL, shelf BLOB NOT NULL, tab TEXT NOT NULL, count INTEGER NOT NULL,
    PRIMARY KEY (root, shelf, tab)) STRICT;

INSERT INTO placement(entry, root, item, key, shelf, unit, sort) VALUES
    (10, x'01', 'group', x'A0', NULL, 10, 1),
    (11, x'01', 'release', x'A1', x'A0', 10, 2),
    (20, x'01', 'group', x'B0', NULL, 20, 3),
    (21, x'01', 'release', x'B1', x'B0', 20, 4),
    (22, x'01', 'release', x'B2', x'B0', 20, 5);
INSERT INTO candidate(root, path, hash, revision, kind) VALUES
    (x'01', x'A1', 'h1', 1, 'invalid'), (x'01', x'B1', 'h2', 1, 'candidate'),
    (x'01', x'B2', 'h3', 1, 'candidate');
INSERT INTO state(hash, revision) VALUES ('h1', 1), ('h2', 1), ('h3', 1);
INSERT INTO origin(release, hash) VALUES ('r1', 'h1'), ('r3', 'h3');
INSERT INTO storage(release, queued) VALUES ('r1', 2), ('r3', 1);
INSERT INTO sending(id, release) VALUES (1, 'r1'), (2, 'r1');
INSERT INTO image(root, path, position, name) VALUES (x'01', x'A1', 0, 'front');
INSERT INTO cover(release, version) VALUES ('r1', 1), ('r3', 1);
INSERT INTO shelf_count VALUES (x'01', x'A0', 'pending', 1), (x'01', x'B0', 'pending', 2)";

/// The list's window: each row its header's and its own, with one row at most
/// of each joined table, FROM subquery and scalar subquery. Its first three
/// rows are 10, 11 and 20.
const WINDOW: &str = "
SELECT u.sort AS s0, u.entry AS k0, w.sort AS s1, w.entry AS k1,
       c.hash, s.revision, o.release, st.queued, sent.files, art.name,
       (SELECT v.version FROM cover AS v WHERE v.release = o.release) AS cover,
       (SELECT coalesce(sum(n.count), 0) FROM shelf_count AS n
        WHERE n.root = w.root AND n.shelf = h.key) AS entries
FROM placement AS u CROSS JOIN placement AS w ON w.unit = u.entry
LEFT JOIN placement AS h
    ON h.root = w.root AND h.item = 'group'
   AND h.key = CASE w.item WHEN 'group' THEN w.key ELSE w.shelf END
LEFT JOIN candidate AS c
    ON w.item <> 'group' AND c.root = w.root AND c.path = w.key
LEFT JOIN state AS s ON s.hash = c.hash AND s.revision = c.revision
LEFT JOIN origin AS o ON o.hash = c.hash
LEFT JOIN storage AS st ON st.release = o.release AND st.queued > 0
LEFT JOIN (SELECT sent.* FROM (SELECT x.release, count(*) AS files
                               FROM sending AS x GROUP BY x.release) AS sent) AS sent
    ON sent.release = st.release
LEFT JOIN image AS art
    ON c.kind = 'invalid' AND art.root = c.root AND art.path = c.path
   AND art.position = (SELECT min(i.position) FROM image AS i
                       WHERE i.root = c.root AND i.path = c.path AND i.name = 'front')
WHERE u.unit = u.entry
ORDER BY u.sort, u.entry, w.sort, w.entry LIMIT 3";

/// A write that only reconfirms what the window shows, through columns it
/// never names, reruns nothing in any table it joins.
#[tokio::test]
async fn a_window_whose_joins_add_one_row_each_depends_only_on_the_named_columns() {
    let db = TestDatabase::open(SHELF);
    follows(
        &db,
        WINDOW,
        vec![],
        &[
            ("UPDATE placement SET seen = 1 WHERE entry = 11", false),
            ("UPDATE candidate SET seen = 1 WHERE path = x'A1'", false),
            ("UPDATE state SET seen = 1 WHERE hash = 'h1'", false),
            ("UPDATE origin SET seen = 1 WHERE release = 'r1'", false),
            ("UPDATE storage SET seen = 1 WHERE release = 'r1'", false),
            ("UPDATE image SET seen = 1 WHERE path = x'A1'", false),
            ("UPDATE cover SET seen = 1 WHERE release = 'r1'", false),
            // A named column of a row the window shows reruns it, and a row
            // after the window's last doesn't.
            ("UPDATE state SET revision = 2 WHERE hash = 'h1'", true),
            ("UPDATE cover SET version = 2 WHERE release = 'r1'", true),
            ("UPDATE placement SET sort = 6 WHERE entry = 22", false),
        ],
    )
    .await;
    db.close();
}

/// A join that compares only part of a unique key can add several rows, which
/// the order SQLite reads them in then chooses among, so the window depends on
/// every column of each table it reads. So does a window ordered by terms that
/// no unique key covers.
#[tokio::test]
async fn a_window_whose_order_leaves_rows_tied_depends_on_every_column() {
    for sql in [
        // The header join leaves out `item`, so a header and a release of one
        // key would both join.
        WINDOW.replace("AND h.item = 'group'", "AND h.item <> 'release'"),
        // A header and a release could share a unit, a sort and a key.
        WINDOW
            .replace("w.sort, w.entry LIMIT", "w.sort, w.key LIMIT")
            .replace("w.entry AS k1", "w.key AS k1"),
        // A state's revision names no one state.
        WINDOW.replace("s.hash = c.hash AND ", ""),
    ] {
        let db = TestDatabase::open(SHELF);
        follows(
            &db,
            sql.leak(),
            vec![],
            &[("UPDATE candidate SET seen = 1 WHERE path = x'A1'", true)],
        )
        .await;
        db.close();
    }
}
