//! A fragment that takes its rows through a relation, as sqlive-compose
//! writes one: the statement hands the fragment a set of rows, which the
//! fragment reads as a CTE, and the rows' SQL names a row of the statement
//! around the fragment. A table the fragment joins to the relation narrows to
//! the rows the relation's own table gives, in that table's scope (SPEC.md
//! sections 7 and 8).

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::Text;

/// Two folders' verdicts, each with its list of cards: verdict 1 of the
/// folder hashed `ha` holds list 10, and verdict 2 of `hb` list 20.
const CARDS: &str = "CREATE TABLE import_candidate_verdict(verdict_id INTEGER PRIMARY KEY,
    content_hash TEXT NOT NULL) STRICT;
CREATE TABLE import_candidate_card_list(list INTEGER PRIMARY KEY, verdict_id INTEGER NOT NULL,
    step TEXT) STRICT;
CREATE TABLE import_candidate_card(list INTEGER NOT NULL, card INTEGER NOT NULL,
    title TEXT NOT NULL, PRIMARY KEY (list, card)) STRICT;
INSERT INTO import_candidate_verdict VALUES (1, 'ha'), (2, 'hb');
INSERT INTO import_candidate_card_list VALUES (10, 1, NULL), (20, 2, NULL);
INSERT INTO import_candidate_card VALUES (10, 0, 'first'), (20, 0, 'second')";

/// The titles of the cards of the verdict of the folder hashed `?1`, as a
/// fragment of cards reads them from the list its relation `card_list` gives:
/// the relation's rows read the list of the verdict around the fragment. The
/// fragment compares the card's list with the relation's list in a subquery
/// and in a join.
const CARD_TITLES: [&str; 2] = [
    "SELECT verdict.verdict_id, (SELECT cards.titles FROM (\
         WITH cards__Cards__card_list (list) AS (\
             SELECT l.list FROM import_candidate_card_list AS l \
             WHERE l.verdict_id = verdict.verdict_id AND l.step IS NULL) \
         SELECT group_concat(c.title) AS titles FROM import_candidate_card AS c \
         WHERE c.list = (SELECT card_list.list FROM cards__Cards__card_list AS card_list)\
     ) AS cards) \
     FROM import_candidate_verdict AS verdict WHERE verdict.content_hash = ?1",
    "SELECT verdict.verdict_id, (SELECT cards.titles FROM (\
         WITH cards__Cards__card_list (list) AS (\
             SELECT l.list FROM import_candidate_card_list AS l \
             WHERE l.verdict_id = verdict.verdict_id AND l.step IS NULL) \
         SELECT group_concat(c.title) AS titles FROM import_candidate_card AS c \
         JOIN cards__Cards__card_list AS card_list ON card_list.list = c.list\
     ) AS cards) \
     FROM import_candidate_verdict AS verdict WHERE verdict.content_hash = ?1",
];

/// A card written to another folder's list wakes nothing, and one written to
/// this folder's list does.
#[tokio::test]
async fn a_card_of_another_verdict_s_list_leaves_the_fragment_s_read_unmarked() {
    for sql in CARD_TITLES {
        let db = TestDatabase::open(CARDS);
        follows(
            &db,
            sql,
            vec![Text("ha".into())],
            &[
                ("INSERT INTO import_candidate_card VALUES (20, 1, 'other')", false),
                ("UPDATE import_candidate_card SET title = 'moved' WHERE list = 20", false),
                ("INSERT INTO import_candidate_card VALUES (10, 1, 'more')", true),
                ("UPDATE import_candidate_card_list SET verdict_id = 1 WHERE list = 20", true),
            ],
        )
        .await;
        db.close();
    }
}

/// Two scanned folders, `/a` hashed `ha` and `/b` hashed `hb`, each with its
/// state and one card.
const OWNED: &str = "CREATE TABLE scan(root TEXT NOT NULL, path TEXT NOT NULL,
    content_hash TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY (root, path)) STRICT;
CREATE INDEX scan_path ON scan(path);
CREATE TABLE state(content_hash TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (content_hash, revision)) STRICT;
CREATE TABLE card(content_hash TEXT NOT NULL, position INTEGER NOT NULL, title TEXT NOT NULL,
    PRIMARY KEY (content_hash, position)) STRICT;
INSERT INTO scan VALUES ('/', '/a', 'ha', 1), ('/', '/b', 'hb', 1);
INSERT INTO state VALUES ('ha', 1), ('hb', 1);
INSERT INTO card VALUES ('ha', 0, 'first'), ('hb', 0, 'second');
CREATE TABLE verdict(verdict_id INTEGER PRIMARY KEY, content_hash TEXT UNIQUE,
    identify_run INTEGER UNIQUE) STRICT;
CREATE TABLE ledger(verdict_id INTEGER NOT NULL, step TEXT NOT NULL,
    PRIMARY KEY (verdict_id, step)) STRICT;
INSERT INTO verdict VALUES (1, 'ha', NULL), (2, 'hb', NULL);
INSERT INTO ledger VALUES (1, 'first'), (2, 'second');
CREATE TABLE card_list(list INTEGER PRIMARY KEY, verdict_id INTEGER NOT NULL,
    step TEXT NOT NULL) STRICT;
CREATE TABLE listed_card(list INTEGER NOT NULL, card INTEGER NOT NULL, title TEXT NOT NULL,
    PRIMARY KEY (list, card)) STRICT;
INSERT INTO card_list VALUES (10, 1, 'first'), (20, 2, 'second');
INSERT INTO listed_card VALUES (10, 0, 'first'), (20, 0, 'second')";

/// The owner of the folder at `?1`'s state, as the fragment that hands it to
/// the cards spreads it: with its keys in a WITH of its own, or narrowed by
/// the key itself.
const OWNERS: [&str; 2] = [
    "WITH m__Owner__keys (candidate_key) AS (SELECT ?1 AS candidate_key) \
     SELECT state.content_hash AS content_hash, NULL AS identify_run \
     FROM m__Owner__keys AS keys \
     JOIN (SELECT scan.path AS candidate_key, scan.content_hash, scan.revision \
         FROM scan AS scan) \
         AS scanned ON scanned.candidate_key = keys.candidate_key \
     JOIN state AS state ON state.content_hash = scanned.content_hash \
         AND state.revision = scanned.revision",
    "SELECT state.content_hash AS content_hash, NULL AS identify_run \
     FROM (SELECT scan.path AS candidate_key, scan.content_hash, scan.revision \
         FROM scan AS scan) \
         AS scanned \
     JOIN state AS state ON state.content_hash = scanned.content_hash \
         AND state.revision = scanned.revision \
     WHERE scanned.candidate_key = ?1",
];

/// The titles of the cards of the owner `{owner}` gives, as a fragment reads
/// them from its relation `owner`, comparing with IS as a fragment that
/// takes an owner of a hash or of a run does, directly and through a
/// fragment it hands the owner on to.
const OWNED_CARDS: [&str; 3] = [
    "SELECT cards.titles FROM (SELECT ?1 AS key) AS pane \
     LEFT JOIN (\
         WITH m__Cards__owner (content_hash, identify_run) AS ({owner}) \
         SELECT (SELECT group_concat(card.title) FROM card AS card \
                 WHERE card.content_hash IS owner.content_hash) AS titles \
         FROM m__Cards__owner AS owner\
     ) AS cards ON 1",
    "SELECT cards.titles FROM (SELECT ?1 AS key) AS pane \
     LEFT JOIN (\
         WITH m__Cards__owner (content_hash, identify_run) AS ({owner}) \
         SELECT handed.titles FROM (\
             WITH m__Titles__owner (content_hash, identify_run) AS (\
                 SELECT * FROM m__Cards__owner) \
             SELECT (SELECT group_concat(card.title) FROM card AS card \
                     WHERE card.content_hash IS owner.content_hash) AS titles \
             FROM m__Titles__owner AS owner) AS handed\
     ) AS cards ON 1",
    "SELECT cards.title FROM (SELECT ?1 AS key) AS pane \
     LEFT JOIN (\
         WITH m__Cards__owner (content_hash, identify_run) AS ({owner}), \
         m__Cards__statement (owner_row) AS (\
             SELECT COALESCE(owner.content_hash, 'run ' || owner.identify_run) \
             FROM m__Cards__owner AS owner) \
         SELECT card.title FROM m__Cards__statement AS statement \
         LEFT JOIN card AS card ON card.content_hash = statement.owner_row\
     ) AS cards ON 1 \
     ORDER BY cards.title",
];

/// A card of another folder wakes nothing, however the owner's keys reach
/// it, and a card of this folder, or a scan that gives this folder another
/// hash, does.
#[tokio::test]
async fn a_key_relation_nested_in_an_owner_narrows_the_owner_s_reads() {
    for (owner, parameter) in OWNERS.iter().flat_map(|owner| [(owner, "?1"), (owner, ":key")]) {
        for cards in OWNED_CARDS {
            let sql: &'static str = cards.replace("{owner}", owner).replace("?1", parameter).leak();
            let db = TestDatabase::open(OWNED);
            follows(
                &db,
                sql,
                vec![Text("/a".into())],
                &[
                    ("INSERT INTO card VALUES ('hb', 1, 'other')", false),
                    ("INSERT INTO state VALUES ('hb', 2)", false),
                    ("INSERT INTO scan VALUES ('/', '/c', 'hc', 1)", false),
                    ("INSERT INTO card VALUES ('ha', 1, 'more')", true),
                    ("UPDATE scan SET content_hash = 'hb' WHERE path = '/a'", true),
                ],
            )
            .await;
            db.close();
        }
    }
}

/// The steps of the ledger of the verdict of the owner `{owner}` gives, as a
/// fragment of a candidate's state reads them: the verdict of the owner's
/// hash or run, compared with IS, and the ledger rows of that verdict.
const OWNED_LEDGER: &str = "SELECT cards.title FROM (SELECT ?1 AS key) AS pane \
     LEFT JOIN (\
         WITH m__Cards__owner (content_hash, identify_run) AS ({owner}), \
         m__Cards__statement (owner_row, title) AS (\
             SELECT COALESCE(owner.content_hash, 'run ' || owner.identify_run), \
                 (SELECT group_concat(ledger.step) FROM ledger AS ledger \
                  WHERE ledger.verdict_id = verdict.verdict_id) \
             FROM m__Cards__owner AS owner \
             LEFT JOIN verdict AS verdict ON verdict.content_hash IS owner.content_hash \
                 AND verdict.identify_run IS owner.identify_run) \
         SELECT statement.title FROM m__Cards__statement AS statement\
     ) AS cards ON 1";

/// A ledger row of another folder's verdict, or another verdict, wakes
/// nothing, however the owner's keys reach the state, and a ledger row of
/// this folder's verdict, or a scan that gives this folder another hash,
/// does.
#[tokio::test]
async fn a_key_relation_nested_in_an_owner_narrows_the_lookups_through_it() {
    for (owner, parameter) in OWNERS.iter().flat_map(|owner| [(owner, "?1"), (owner, ":key")]) {
        let sql: &'static str =
            OWNED_LEDGER.replace("{owner}", owner).replace("?1", parameter).leak();
        let db = TestDatabase::open(OWNED);
        follows(
            &db,
            sql,
            vec![Text("/a".into())],
            &[
                ("INSERT INTO ledger VALUES (2, 'more')", false),
                ("INSERT INTO verdict VALUES (3, 'hc', NULL)", false),
                ("INSERT INTO ledger VALUES (1, 'more')", true),
                ("UPDATE scan SET content_hash = 'hb' WHERE path = '/a'", true),
            ],
        )
        .await;
        db.close();
    }
}

/// The titles of the cards of each ledger step of the verdict of the owner
/// `{owner}` gives, as a fragment of a candidate's state reads them: a cards
/// fragment takes its list through a relation whose rows name the ledger row
/// and, through it, the verdict around the fragment.
const OWNED_LEDGER_CARDS: &str = "SELECT cards.titles FROM (SELECT ?1 AS key) AS pane \
     LEFT JOIN (\
         WITH m__State__owner (content_hash, identify_run) AS ({owner}), \
         m__State__statement (owner_row, titles) AS (\
             SELECT COALESCE(owner.content_hash, 'run ' || owner.identify_run), \
                 (SELECT group_concat(by_step.titles) FROM (\
                     SELECT ledger.step, (SELECT listed.titles FROM (\
                         WITH m__Cards__card_list (list) AS (\
                             SELECT l.list FROM card_list AS l \
                             WHERE l.verdict_id = ledger.verdict_id AND l.step = ledger.step) \
                         SELECT group_concat(c.title) AS titles FROM listed_card AS c \
                         WHERE c.list = (SELECT card_list.list FROM m__Cards__card_list AS card_list)\
                     ) AS listed) AS titles \
                     FROM ledger AS ledger WHERE ledger.verdict_id = verdict.verdict_id\
                 ) AS by_step) \
             FROM m__State__owner AS owner \
             LEFT JOIN verdict AS verdict ON verdict.content_hash IS owner.content_hash \
                 AND verdict.identify_run IS owner.identify_run) \
         SELECT statement.titles FROM m__State__statement AS statement\
     ) AS cards ON 1";

/// A card of another verdict's list wakes nothing, however the owner's keys
/// reach the state, and a card of this folder's verdict's list does.
#[tokio::test]
async fn a_key_relation_nested_in_an_owner_narrows_a_relation_naming_rows_through_it() {
    for owner in OWNERS {
        let sql: &'static str = OWNED_LEDGER_CARDS.replace("{owner}", owner).leak();
        let db = TestDatabase::open(OWNED);
        follows(
            &db,
            sql,
            vec![Text("/a".into())],
            &[
                ("INSERT INTO listed_card VALUES (20, 1, 'other')", false),
                ("INSERT INTO card_list VALUES (30, 2, 'second')", false),
                (
                    "INSERT INTO verdict VALUES (3, 'hz', NULL); \
                     INSERT INTO ledger VALUES (3, 'third'); \
                     INSERT INTO card_list VALUES (40, 3, 'third'); \
                     INSERT INTO listed_card VALUES (40, 0, 'third')",
                    false,
                ),
                ("INSERT INTO listed_card VALUES (10, 1, 'more')", true),
            ],
        )
        .await;
        db.close();
    }
}

/// The owner of the folder at `?1` as `OWNERS` reads it, over a scan table
/// that no index serves by path, which a lookup reads row by row.
const UNINDEXED: &str = "CREATE TABLE scan(id INTEGER PRIMARY KEY, path TEXT NOT NULL,
    content_hash TEXT NOT NULL, revision INTEGER NOT NULL) STRICT;
CREATE TABLE state(content_hash TEXT NOT NULL, revision INTEGER NOT NULL,
    PRIMARY KEY (content_hash, revision)) STRICT;
CREATE TABLE verdict(verdict_id INTEGER PRIMARY KEY, content_hash TEXT UNIQUE,
    identify_run INTEGER UNIQUE) STRICT;
CREATE TABLE ledger(verdict_id INTEGER NOT NULL, step TEXT NOT NULL,
    PRIMARY KEY (verdict_id, step)) STRICT;
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 150)
    INSERT INTO scan SELECT i, '/' || i, 'h' || i, 1 FROM n;
INSERT INTO state VALUES ('h1', 1), ('h2', 1);
INSERT INTO verdict VALUES (1, 'h1', NULL), (2, 'h2', NULL);
INSERT INTO ledger VALUES (1, 'first'), (2, 'second');
CREATE TABLE card_list(list INTEGER PRIMARY KEY, verdict_id INTEGER NOT NULL,
    step TEXT NOT NULL) STRICT;
CREATE TABLE listed_card(list INTEGER NOT NULL, card INTEGER NOT NULL, title TEXT NOT NULL,
    PRIMARY KEY (list, card)) STRICT;
INSERT INTO card_list VALUES (10, 1, 'first'), (20, 2, 'second');
INSERT INTO listed_card VALUES (10, 0, 'first'), (20, 0, 'second')";

/// A lookup reads a partner that is a subquery once, however many of its
/// conditions name it. Each lookup a write to another verdict's card asks
/// passes the verdict's two IS comparisons with the owner, which name the
/// owner four times, and the owner reads a scan that no index serves: read
/// once for each, the owner would take the lookup past its steps, and the
/// write would rerun the query.
#[tokio::test]
async fn a_lookup_reads_a_subquery_partner_once() {
    for owner in OWNERS {
        let sql: &'static str = OWNED_LEDGER_CARDS.replace("{owner}", owner).leak();
        let db = TestDatabase::open(UNINDEXED);
        follows(
            &db,
            sql,
            vec![Text("/1".into())],
            &[
                ("INSERT INTO listed_card VALUES (20, 1, 'other')", false),
                ("INSERT INTO listed_card VALUES (10, 1, 'more')", true),
            ],
        )
        .await;
        db.close();
    }
}
