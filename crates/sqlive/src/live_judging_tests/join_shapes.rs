//! Joins other than one column equal to another, each narrowing the joined
//! table as the equality would: `IS` between two tables, an OR whose sides
//! each narrow the table, COALESCE of several partners' values, and an
//! expression over the joined table's own row; a parenthesized join, whose
//! tables narrow as their joins do; and joins on an expression that gives a
//! value where its table's row is missing, which narrow nothing.

use super::lookup::follows;
use crate::tests::TestDatabase;

/// A candidate's failures belong to its verdict by a pair of columns that
/// can hold NULL, joined by `IS`: a failure of another verdict wakes
/// nothing, and one of this verdict with a NULL run does.
#[tokio::test]
async fn an_is_join_narrows_and_matches_nulls() {
    let db = TestDatabase::open(
        "CREATE TABLE verdict(id INTEGER PRIMARY KEY, hash TEXT, run INTEGER);
        CREATE TABLE failure(id INTEGER PRIMARY KEY, hash TEXT, run INTEGER, message TEXT);
        INSERT INTO verdict VALUES (1,'h1',NULL),(2,'h2',1);
        INSERT INTO failure VALUES (1,'h1',NULL,'a'),(2,'h2',1,'b'),(3,'h1',2,'c')",
    );
    follows(&db, "SELECT f.message FROM verdict v JOIN failure f ON f.hash IS v.hash AND f.run IS v.run WHERE v.hash = 'h1' ORDER BY f.id", vec![], &[
        ("UPDATE failure SET message = 'bb' WHERE id = 2", false),
        ("UPDATE failure SET message = 'cc' WHERE id = 3", false),
        ("INSERT INTO failure VALUES (4,'h2',NULL,'d')", false),
        ("UPDATE failure SET message = 'aa' WHERE id = 1", true),
        ("UPDATE failure SET run = NULL WHERE id = 3", true),
        ("UPDATE failure SET hash = 'h2', run = 1 WHERE id = 3", true),
        ("UPDATE verdict SET run = 2 WHERE id = 1", true),
        ("UPDATE failure SET message = 'a' WHERE id = 1", false),
    ])
    .await;
    // A verdict a LEFT JOIN adds gives NULLs with no row when none joins,
    // which match a failure's NULL run: the failures narrow by their hash
    // alone.
    follows(&db, "SELECT f.message FROM failure f LEFT JOIN verdict v ON v.id = 9 WHERE f.run IS v.run AND f.hash = 'h1' ORDER BY f.id", vec![], &[
        ("UPDATE failure SET message = 'bbb' WHERE id = 2", false),
        ("UPDATE failure SET run = 5 WHERE id = 1", true),
    ])
    .await;
    db.close();
}

/// An artist the wanted row names by either of two equalities: an artist
/// neither side names wakes nothing, and one either side names does.
#[tokio::test]
async fn an_or_of_two_joins_narrows_by_either() {
    const ARTISTS: &str = "CREATE TABLE wanted(id INTEGER PRIMARY KEY, dg INTEGER, key TEXT);
        CREATE TABLE artists(id INTEGER PRIMARY KEY, discogs_id INTEGER, name TEXT, name_key TEXT);
        CREATE TABLE shown(id INTEGER PRIMARY KEY, artist INTEGER, key TEXT);
        INSERT INTO wanted VALUES (1,5,'k1'),(2,6,'k2');
        INSERT INTO artists VALUES (1,5,'one','k1'),(105,NULL,'two','k9'),(3,6,'three','k2'),(106,NULL,'four','k8');
        INSERT INTO shown VALUES (1,105,'k1'),(2,106,'k2')";
    let db = TestDatabase::open(ARTISTS);
    follows(&db, "SELECT a.name FROM wanted t JOIN artists a ON a.discogs_id = t.dg OR a.id = t.dg + 100 WHERE t.id = 1 ORDER BY a.id", vec![], &[
        ("UPDATE artists SET name = 'x' WHERE id = 3", false),
        ("UPDATE artists SET name = 'y' WHERE id = 106", false),
        ("UPDATE artists SET name = 'uno' WHERE id = 1", true),
        ("UPDATE artists SET name = 'dos' WHERE id = 105", true),
        ("UPDATE artists SET discogs_id = 5 WHERE id = 3", true),
        ("UPDATE wanted SET dg = 6 WHERE id = 1", true),
    ])
    .await;
    db.close();
    // A side that compares with a subquery's value takes it once the
    // subquery's source is placed.
    let db = TestDatabase::open(ARTISTS);
    follows(&db, "SELECT a.name FROM wanted z JOIN artists a ON a.name_key = z.key OR a.id IN (SELECT s.artist FROM shown s WHERE s.key = z.key) WHERE z.id = 1 ORDER BY a.id", vec![], &[
        ("UPDATE artists SET name = 'x' WHERE id = 3", false),
        ("UPDATE artists SET name = 'y' WHERE id = 106", false),
        ("UPDATE artists SET name = 'un' WHERE id = 1", true),
        ("UPDATE artists SET name = 'deux' WHERE id = 105", true),
        ("UPDATE shown SET artist = 3 WHERE id = 1", true),
        ("UPDATE artists SET name = 'trois' WHERE id = 3", true),
    ])
    .await;
    db.close();
}

/// An OR one of whose sides says nothing of a table narrows that table by
/// nothing: the admins narrow by their join to the operations alone.
#[tokio::test]
async fn an_or_with_a_side_naming_another_table_narrows_nothing_there() {
    let db = TestDatabase::open(
        "CREATE TABLE ops(id INTEGER PRIMARY KEY, kind TEXT, admin INTEGER, v TEXT);
        CREATE TABLE admins(id INTEGER PRIMARY KEY, member INTEGER);
        INSERT INTO ops VALUES (1,'set',1,'a'),(2,'x',2,'b');
        INSERT INTO admins VALUES (1,1),(2,2),(3,3)",
    );
    follows(&db, "SELECT o.v FROM ops o JOIN admins a ON a.id = o.admin WHERE o.kind <> 'x' OR (o.kind = 'set' AND a.member = 1) ORDER BY o.id", vec![], &[
        ("UPDATE admins SET member = 5 WHERE id = 3", false),
        ("UPDATE admins SET member = 1 WHERE id = 2", false),
        ("UPDATE admins SET member = 7 WHERE id = 1", true),
        ("UPDATE ops SET kind = 'y' WHERE id = 2", true),
    ])
    .await;
    db.close();
}

/// A cover joined to COALESCE of the scan row's hash and a subquery's: a
/// cover of a third hash wakes nothing, and one of either hash does.
#[tokio::test]
async fn a_coalesce_of_partners_narrows_by_each() {
    let db = TestDatabase::open(
        "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT, hash TEXT);
        CREATE TABLE covers(id INTEGER PRIMARY KEY, hash TEXT, url TEXT);
        INSERT INTO scan VALUES (1,'/a',NULL),(2,'/b','h2'),(3,'/c','h3');
        INSERT INTO covers VALUES (1,'h1','u1'),(2,'h2','u2'),(3,'h3','u3')",
    );
    follows(&db, "SELECT cv.url FROM scan c JOIN covers cv ON cv.hash = COALESCE(c.hash, (SELECT hash FROM scan WHERE path = '/b')) WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("UPDATE covers SET url = 'v3' WHERE id = 3", false),
        ("UPDATE covers SET url = 'v2' WHERE id = 2", true),
        ("UPDATE scan SET hash = 'h1' WHERE id = 1", true),
        ("UPDATE covers SET url = 'v1' WHERE id = 1", true),
        ("UPDATE covers SET url = 'w3' WHERE id = 3", false),
    ])
    .await;
    db.close();
}

/// A cover joined to an expression over the tag a LEFT JOIN adds, which gives
/// a value where the scan row has no tag: the cover of that value is in the
/// result, and no lookup of the tags finds it, so each such join reads the
/// covers whole. A join on `lower`, NULL where the tag is missing, narrows,
/// and so does the CASE over a tag an inner join adds, which every joined row
/// has.
#[tokio::test]
async fn a_join_on_a_value_of_a_missing_row_reads_the_table_whole() {
    let db = TestDatabase::open(
        "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE tag(id INTEGER PRIMARY KEY, scan INTEGER, kind TEXT);
        CREATE TABLE covers(id INTEGER PRIMARY KEY, kind TEXT, url TEXT);
        INSERT INTO scan VALUES (1,'/a'),(2,'/b');
        INSERT INTO tag VALUES (1,2,'front');
        INSERT INTO covers VALUES (1,'front','u1')",
    );
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = CASE WHEN t.kind IS NULL THEN 'none' ELSE t.kind END WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (2,'none','u2')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = iif(t.kind IS NULL, 'none', t.kind) WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (3,'none','u3')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = quote(t.kind) WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (4,'NULL','u4')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = concat(t.kind, 'back') WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (5,'back','u5')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = (t.kind IS NULL) || 'x' WHERE c.path = '/a' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (6,'1x','u6')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c LEFT JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = lower(t.kind) WHERE c.path = '/b' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (7,'none','u7')", false),
        ("INSERT INTO covers VALUES (8,'front','u8')", true),
    ])
    .await;
    follows(&db, "SELECT cv.url FROM scan c JOIN tag t ON t.scan = c.id JOIN covers cv ON cv.kind = CASE WHEN t.kind IS NULL THEN 'none' ELSE t.kind END WHERE c.path = '/b' ORDER BY cv.id", vec![], &[
        ("INSERT INTO covers VALUES (9,'none','u9')", false),
        ("INSERT INTO covers VALUES (10,'front','u10')", true),
    ])
    .await;
    db.close();
}

/// A record joins its owner by an expression over its own row, which picks
/// an album's own key or a release's album key: a record whose expression
/// gives another key wakes nothing, and one whose kind or key changes so
/// that the expression gives the owner's key does. The same holds when the
/// owner's key is an expression, under `IS`, and in a row value.
#[tokio::test]
async fn an_expression_of_the_row_narrows_by_its_value() {
    const RECORDS: &str = "CREATE TABLE owned(id INTEGER PRIMARY KEY, album_catalog TEXT, album_key TEXT);
        CREATE TABLE release_records(id INTEGER PRIMARY KEY, catalog TEXT, kind TEXT, key TEXT, album_key TEXT);
        INSERT INTO owned VALUES (1,'c','a1'),(2,'c','a2');
        INSERT INTO release_records VALUES (1,'c','album','a1',NULL),(2,'c','release','r1','a1'),(3,'c','album','a2',NULL),(4,'c','release','r2','a2')";
    let db = TestDatabase::open(RECORDS);
    for sql in [
        "SELECT rr.key FROM owned o JOIN release_records rr ON rr.catalog = o.album_catalog AND (CASE WHEN rr.kind = 'album' THEN rr.key ELSE rr.album_key END) = o.album_key WHERE o.id = 1 ORDER BY rr.id",
        "SELECT rr.key FROM owned o JOIN release_records rr ON o.album_key || '' = (CASE WHEN rr.kind = 'album' THEN rr.key ELSE rr.album_key END) WHERE o.id = 1 ORDER BY rr.id",
        "SELECT rr.key FROM owned o JOIN release_records rr ON (CASE WHEN rr.kind = 'album' THEN rr.key ELSE rr.album_key END) IS o.album_key WHERE o.id = 1 ORDER BY rr.id",
        "SELECT rr.key FROM owned o JOIN release_records rr ON (rr.catalog, CASE WHEN rr.kind = 'album' THEN rr.key ELSE rr.album_key END) = (o.album_catalog, o.album_key) WHERE o.id = 1 ORDER BY rr.id",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE release_records SET key = 'r9' WHERE id = 4", false),
            ("UPDATE release_records SET catalog = 'd' WHERE id = 3", false),
            ("UPDATE release_records SET key = 'r1.' WHERE id = 2", true),
            ("UPDATE release_records SET kind = 'album', key = 'a1' WHERE id = 4", true),
            ("UPDATE owned SET album_key = 'a2' WHERE id = 2", false),
        ])
        .await;
        db.commit("DELETE FROM release_records; DELETE FROM owned; INSERT INTO owned VALUES (1,'c','a1'),(2,'c','a2'); INSERT INTO release_records VALUES (1,'c','album','a1',NULL),(2,'c','release','r1','a1'),(3,'c','album','a2',NULL),(4,'c','release','r2','a2')");
    }
    db.close();
}

const PEOPLE: &str = "CREATE TABLE members(id INTEGER PRIMARY KEY, member TEXT);
CREATE TABLE people(id TEXT PRIMARY KEY, name TEXT, avatar_id INTEGER);
CREATE TABLE files(id INTEGER PRIMARY KEY, path TEXT);
INSERT INTO members VALUES (1,'p1'),(2,'p2');
INSERT INTO people VALUES ('p1','one',1),('p2','two',2),('p3','three',3);
INSERT INTO files VALUES (1,'a.png'),(2,'b.png'),(3,'c.png')";

/// A parenthesized join's tables narrow as their joins do: the ON around a
/// LEFT JOIN of the group narrows its first table, and the group's own ON
/// narrows the next by the first. A person not the member's, or a file not
/// that person's avatar, reruns nothing.
#[tokio::test]
async fn a_parenthesized_join_narrows_as_its_joins() {
    let db = TestDatabase::open(PEOPLE);
    for sql in [
        "SELECT m.id || ':' || coalesce(p.name, '') || ':' || coalesce(a.path, '') FROM members m LEFT JOIN (people p LEFT JOIN files a ON a.id = p.avatar_id) ON p.id = m.member WHERE m.id = 1",
        "SELECT m.id || ':' || p.name || ':' || coalesce(a.path, '') FROM members m JOIN (people p LEFT JOIN files a ON a.id = p.avatar_id) ON p.id = m.member WHERE m.id = 1",
    ] {
        follows(&db, sql, vec![], &[
            ("UPDATE people SET name = 'two2' WHERE id = 'p2'", false),
            ("UPDATE files SET path = 'b2.png' WHERE id = 2", false),
            ("UPDATE people SET name = 'one2' WHERE id = 'p1'", true),
            ("UPDATE files SET path = 'a2.png' WHERE id = 1", true),
            ("UPDATE people SET avatar_id = 3 WHERE id = 'p1'", true),
            ("UPDATE files SET path = 'c2.png' WHERE id = 3", true),
            ("UPDATE members SET member = 'p3' WHERE id = 1", true),
        ])
        .await;
        db.commit("UPDATE people SET name = 'one' WHERE id = 'p1'; UPDATE people SET name = 'two' WHERE id = 'p2'; UPDATE people SET avatar_id = 1 WHERE id = 'p1'; UPDATE files SET path = 'a.png' WHERE id = 1; UPDATE files SET path = 'b.png' WHERE id = 2; UPDATE files SET path = 'c.png' WHERE id = 3; UPDATE members SET member = 'p1' WHERE id = 1");
    }
    db.close();
}

/// A LEFT JOIN of a parenthesized join whose tables joined one after another
/// would give other rows keeps every table whole: one named by an alias, one
/// whose ON names a later table of the join, one with an inner join inside,
/// and one whose inner ON can hold where the first table has no row.
#[tokio::test]
async fn a_parenthesized_join_sqlite_reads_otherwise_keeps_its_tables_whole() {
    let db = TestDatabase::open(PEOPLE);
    for sql in [
        "SELECT count(g.name) FROM members m LEFT JOIN (people p LEFT JOIN files a ON a.id = p.avatar_id) AS g ON g.id = m.member WHERE m.id = 1",
        "SELECT count(p.name) FROM members m LEFT JOIN (people p LEFT JOIN files a ON a.id = p.avatar_id) ON p.id = m.member AND a.path IS NOT NULL WHERE m.id = 1",
        "SELECT count(p.name) FROM members m LEFT JOIN (people p JOIN files a ON a.id = p.avatar_id) ON p.id = m.member WHERE m.id = 1",
        "SELECT count(a.path) FROM members m LEFT JOIN (people p LEFT JOIN files a ON a.id IS p.avatar_id) ON p.id = m.member WHERE m.id = 1",
    ] {
        follows(&db, sql, vec![], &[("UPDATE people SET name = 'two2' WHERE id = 'p2'", true)]).await;
        db.commit("UPDATE people SET name = 'two' WHERE id = 'p2'");
    }
    db.close();
}

/// A column declared with no type has no affinity. SQLite compares it with
/// a TEXT column as it is, so the text '5' in `tags.k` joins `notes.k` and
/// the integer 6 doesn't, and gives it an INTEGER column's affinity, so the
/// text '7' joins `nums.k`'s 7. The lookup compares the same way.
#[tokio::test]
async fn a_column_of_no_affinity_gives_values_as_sqlite_compares_them() {
    let db = TestDatabase::open(
        "CREATE TABLE tags(id INTEGER PRIMARY KEY, k);
        CREATE TABLE notes(id INTEGER PRIMARY KEY, k TEXT, body TEXT);
        CREATE TABLE nums(id INTEGER PRIMARY KEY, k INTEGER, body TEXT);
        INSERT INTO tags VALUES (1, '5'), (2, 6), (3, '7');
        INSERT INTO notes VALUES (1, '5', 'a'), (2, '6', 'b');
        INSERT INTO nums VALUES (1, 7, 'a'), (2, 8, 'b')",
    );
    let text = [
        "SELECT n.body FROM tags t JOIN notes n ON n.k = t.k WHERE t.id = ?1",
        "SELECT n.body FROM tags t JOIN notes n ON t.k = n.k WHERE t.id = ?1",
        "SELECT body FROM notes WHERE k IN (SELECT k FROM tags WHERE id = ?1)",
        "SELECT body FROM notes WHERE k = (SELECT k FROM tags WHERE id = ?1)",
    ];
    for sql in text {
        follows(
            &db,
            sql,
            vec![crate::SqlValue::Integer(1)],
            &[
                ("UPDATE notes SET body = body || '.' WHERE id = 2", false),
                ("UPDATE notes SET body = body || '.' WHERE id = 1", true),
            ],
        )
        .await;
        follows(
            &db,
            sql,
            vec![crate::SqlValue::Integer(2)],
            &[("UPDATE notes SET body = body || '.' WHERE id = 2", false)],
        )
        .await;
    }
    for sql in [
        "SELECT m.body FROM tags t JOIN nums m ON m.k = t.k WHERE t.id = ?1",
        "SELECT body FROM nums WHERE k IN (SELECT k FROM tags WHERE id = ?1)",
    ] {
        follows(
            &db,
            sql,
            vec![crate::SqlValue::Integer(3)],
            &[
                ("UPDATE nums SET body = body || '.' WHERE id = 2", false),
                ("UPDATE nums SET body = body || '.' WHERE id = 1", true),
            ],
        )
        .await;
    }
    db.close();
}
