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
