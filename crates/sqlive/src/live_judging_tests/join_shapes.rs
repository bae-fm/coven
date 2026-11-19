//! Joins other than one column equal to another, each narrowing the joined
//! table as the equality would: `IS` between two tables, an OR whose sides
//! each narrow the table, and COALESCE of several partners' values; and
//! joins on an expression that gives a value where its table's row is
//! missing, which narrow nothing.

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
