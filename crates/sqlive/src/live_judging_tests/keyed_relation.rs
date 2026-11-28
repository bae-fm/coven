//! A fragment that takes a relation of keys and reads each key's rows in a
//! subquery: the subquery hands a fragment a relation that names the key's
//! row, and that fragment joins the relation's rows to a table, then groups
//! them (SPEC.md section 8).

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::Text;

/// Two scanned folders, `/a` hashed `ha` and `/b` hashed `hb`, each crediting
/// one artist of its own.
const CREDITED: &str =
    "CREATE TABLE scan(path TEXT PRIMARY KEY, content_hash TEXT NOT NULL) STRICT;
CREATE TABLE credit(content_hash TEXT NOT NULL, position INTEGER NOT NULL, artist_id TEXT NOT NULL,
    PRIMARY KEY (content_hash, position)) STRICT;
CREATE TABLE artists(id TEXT PRIMARY KEY, name TEXT NOT NULL) STRICT;
INSERT INTO scan VALUES ('/a', 'ha'), ('/b', 'hb');
INSERT INTO credit VALUES ('ha', 0, 'x'), ('hb', 0, 'y');
INSERT INTO artists VALUES ('x', 'one'), ('y', 'two')";

/// The names of the artists each folder of the keys credits, as a fragment of
/// keys reads them: for each key, a subquery hands a standing fragment the
/// key's credits as a relation, and the fragment joins each credit to its
/// artist and groups the credits by artist.
const STANDINGS: &str = "SELECT field.* FROM (\
     WITH m__Field__keys (candidate_key) AS (SELECT ?1 AS candidate_key) \
     SELECT keys.candidate_key AS field_key, (\
         WITH m__Standing__raw (artist_id, position) AS (\
             SELECT credit.artist_id, credit.position \
             FROM (SELECT scan.path AS candidate_key, c.artist_id, c.position \
                   FROM scan AS scan JOIN credit AS c ON c.content_hash = scan.content_hash) \
                 AS credit \
             WHERE credit.candidate_key = keys.candidate_key), \
         counted AS (\
             SELECT assignment.artist_id, COUNT(*) AS total, MIN(picked.name) AS name \
             FROM m__Standing__raw AS assignment \
             LEFT JOIN artists AS picked ON picked.id = assignment.artist_id \
             GROUP BY assignment.artist_id) \
         SELECT group_concat(counted.name || counted.total) FROM counted\
     ) AS field \
     FROM m__Field__keys AS keys) AS field";

/// An artist another folder credits, or a credit of another folder, wakes
/// nothing, and an artist this folder credits does.
#[tokio::test]
async fn a_relation_naming_a_key_s_row_narrows_the_tables_a_grouping_fragment_joins_to_it() {
    let db = TestDatabase::open(CREDITED);
    follows(
        &db,
        STANDINGS,
        vec![Text("/a".into())],
        &[
            ("UPDATE artists SET name = 'other' WHERE id = 'y'", false),
            ("INSERT INTO artists VALUES ('z', 'three')", false),
            ("INSERT INTO credit VALUES ('hb', 1, 'z')", false),
            ("UPDATE artists SET name = 'ONE' WHERE id = 'x'", true),
            ("INSERT INTO credit VALUES ('ha', 1, 'y')", true),
        ],
    )
    .await;
    db.close();
}

/// The names of the artists each folder of the keys credits, as a fragment of
/// keys reads them through a standing fragment that takes a relation of keys
/// of its own: the subquery hands it the key of its row as the CTE
/// `m__Standing__keys` that `$keys` declares, and the standing fragment reads
/// the key's credits and joins each to its artist.
macro_rules! nested_standings {
    ($keys:literal) => {
        concat!(
            "SELECT field.* FROM (\
     WITH m__Field__keys (candidate_key) AS (SELECT ?1 AS candidate_key) \
     SELECT keys.candidate_key AS field_key, (\
         SELECT standing.names FROM (\
             WITH ",
            $keys,
            ", \
             m__Standing__raw (artist_id) AS (\
                 SELECT credit.artist_id \
                 FROM m__Standing__keys AS keys \
                 JOIN (SELECT scan.path AS candidate_key, c.artist_id \
                       FROM scan AS scan JOIN credit AS c ON c.content_hash = scan.content_hash) \
                     AS credit ON credit.candidate_key = keys.candidate_key) \
             SELECT group_concat(picked.name) AS names \
             FROM m__Standing__raw AS assignment \
             JOIN artists AS picked ON picked.id = assignment.artist_id) AS standing\
     ) AS field \
     FROM m__Field__keys AS keys) AS field"
        )
    };
}

/// A credit or an artist of another folder wakes nothing, and one of this
/// folder does, through a relation that hands on the key of the row around
/// it: one whose CTE names its column, and one that leaves the column
/// unnamed, which SQLite names for the column it returns.
#[tokio::test]
async fn a_relation_of_the_key_of_the_row_around_it_narrows_as_the_key_does() {
    for statement in [
        nested_standings!(
            "m__Standing__keys (candidate_key) AS (SELECT keys.candidate_key AS candidate_key)"
        ),
        nested_standings!("m__Standing__keys AS (SELECT keys.candidate_key)"),
    ] {
        let db = TestDatabase::open(CREDITED);
        follows(
            &db,
            statement,
            vec![Text("/a".into())],
            &[
                ("INSERT INTO scan VALUES ('/c', 'hc')", false),
                ("INSERT INTO credit VALUES ('hb', 1, 'z')", false),
                ("UPDATE artists SET name = 'other' WHERE id = 'y'", false),
                ("UPDATE artists SET name = 'ONE' WHERE id = 'x'", true),
                ("INSERT INTO credit VALUES ('ha', 1, 'y')", true),
            ],
        )
        .await;
        db.close();
    }
}

/// The answers stored for the names each folder of the keys credits, as a
/// standing fragment reads them: it joins each credit to the answer stored
/// for the credit's name, by the function `$key` of the credit's name.
macro_rules! answered {
    ($key:literal) => {
        concat!(
            "SELECT field.* FROM (\
             WITH m__Field__keys (candidate_key) AS (SELECT ?1 AS candidate_key) \
             SELECT keys.candidate_key AS field_key, (\
                 WITH m__Standing__raw (name) AS (\
                     SELECT credit.name \
                     FROM (SELECT scan.path AS candidate_key, c.artist_id AS name \
                           FROM scan AS scan JOIN credit AS c ON c.content_hash = scan.content_hash) \
                         AS credit \
                     WHERE credit.candidate_key = keys.candidate_key), \
                 m__Standing__answer (name_key, answer) AS (\
                     SELECT stored.name_key, stored.answer FROM answer AS stored) \
                 SELECT group_concat(given.answer) \
                 FROM m__Standing__raw AS assignment \
                 LEFT JOIN m__Standing__answer AS given ON given.name_key = ",
            $key,
            "(assignment.name)\
             ) AS field \
             FROM m__Field__keys AS keys) AS field"
        )
    };
}

/// Two scanned folders, `/a` and `/b`, each crediting one name of its own,
/// and a table of the answers stored for names.
const NAMED: &str = "CREATE TABLE scan(path TEXT PRIMARY KEY, content_hash TEXT NOT NULL) STRICT;
CREATE TABLE credit(content_hash TEXT NOT NULL, position INTEGER NOT NULL, artist_id TEXT NOT NULL,
    PRIMARY KEY (content_hash, position)) STRICT;
CREATE TABLE answer(name_key TEXT PRIMARY KEY, answer TEXT NOT NULL) STRICT;
INSERT INTO scan VALUES ('/a', 'ha'), ('/b', 'hb');
INSERT INTO credit VALUES ('ha', 0, 'x'), ('hb', 0, 'y')";

/// An answer stored for a name another folder credits wakes nothing, and one
/// for a name this folder credits does.
#[tokio::test]
async fn an_expression_of_a_relation_naming_a_key_s_row_narrows_the_table_joined_by_it() {
    let db = TestDatabase::open(NAMED);
    follows(
        &db,
        answered!("upper"),
        vec![Text("/a".into())],
        &[
            ("INSERT INTO answer VALUES ('Y', 'two')", false),
            ("INSERT INTO answer VALUES ('X', 'one')", true),
        ],
    )
    .await;
    db.close();
}

/// An app's function of the credit's name, which sqlive can't tell gives NULL
/// where the name is NULL, narrows the answers too: no LEFT JOIN adds the
/// credits, so every row the fragment joins has a credit's row.
#[tokio::test]
async fn an_app_function_of_a_relation_naming_a_key_s_row_narrows_the_table_joined_by_it() {
    let db = TestDatabase::open(NAMED);
    super::row::fold_on_every_connection(&db, true);
    follows(
        &db,
        answered!("fold"),
        vec![Text("/a".into())],
        &[
            ("INSERT INTO answer VALUES ('y', 'two')", false),
            ("INSERT INTO answer VALUES ('x', 'one')", true),
        ],
    )
    .await;
    db.close();
}
