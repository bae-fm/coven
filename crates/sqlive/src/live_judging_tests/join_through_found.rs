//! A read that reaches rows through a join narrows them by the join's
//! equalities, as an import pane's read of a cell's cards does: the rows of
//! a verdict's matches, and of the labels, media and links of those matches,
//! reached through the equalities of the join alone.

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::Integer;

/// Verdicts, each with the releases found for its ledger cells, the matches
/// of the verdict, and the labels of each match.
const LEDGER: &str =
    "CREATE TABLE verdict(verdict_id INTEGER PRIMARY KEY, name TEXT NOT NULL) STRICT;
CREATE TABLE found(verdict_id INTEGER NOT NULL, ordinal INTEGER NOT NULL,
    match_position INTEGER NOT NULL, PRIMARY KEY (verdict_id, ordinal)) STRICT;
CREATE TABLE matched(verdict_id INTEGER NOT NULL, position INTEGER NOT NULL,
    source TEXT NOT NULL, PRIMARY KEY (verdict_id, position)) STRICT;
CREATE TABLE label(verdict_id INTEGER NOT NULL, position INTEGER NOT NULL,
    name TEXT NOT NULL, PRIMARY KEY (verdict_id, position, name)) STRICT;
INSERT INTO verdict VALUES (1, 'one'), (2, 'two');
INSERT INTO found VALUES (1, 0, 0), (2, 0, 0);
INSERT INTO matched VALUES (1, 0, 'a'), (2, 0, 'b');
INSERT INTO label VALUES (1, 0, 'x'), (2, 0, 'y')";

/// The writes of other verdicts' rows wake nothing, and those of the
/// verdict's own rows do, whichever way the statement names the verdict.
async fn narrowed_to_verdict_one(sql: &'static str) {
    let db = TestDatabase::open(LEDGER);
    follows(
        &db,
        sql,
        vec![Integer(1)],
        &[
            ("UPDATE label SET name = 'z' WHERE verdict_id = 2", false),
            ("UPDATE matched SET source = 'c' WHERE verdict_id = 2", false),
            ("INSERT INTO label VALUES (2, 0, 'w')", false),
            ("INSERT INTO label VALUES (1, 0, 'v')", true),
            ("UPDATE label SET name = 'u' WHERE verdict_id = 1 AND name = 'v'", true),
            ("UPDATE matched SET source = 'd' WHERE verdict_id = 1", true),
        ],
    )
    .await;
    db.close();
}

/// The first table is narrowed by a value the run is bound to, and each
/// table after it is reached by an equality with the one before.
#[tokio::test]
async fn a_chain_of_joins_narrows_each_table_through_the_one_before() {
    narrowed_to_verdict_one(
        "SELECT l.name || m.source FROM found AS f \
         JOIN matched AS m ON m.verdict_id = f.verdict_id AND m.position = f.match_position \
         JOIN label AS l ON l.verdict_id = m.verdict_id AND l.position = m.position \
         WHERE f.verdict_id = ?1",
    )
    .await;
}

/// The first table is narrowed by a subquery that looks the verdict up by
/// its id, as the cards read does.
#[tokio::test]
async fn a_chain_of_joins_narrows_through_a_table_a_subquery_narrows() {
    narrowed_to_verdict_one(
        "SELECT l.name || m.source FROM found AS f \
         JOIN matched AS m ON m.verdict_id = f.verdict_id AND m.position = f.match_position \
         JOIN label AS l ON l.verdict_id = m.verdict_id AND l.position = m.position \
         WHERE f.verdict_id = (SELECT v.verdict_id FROM verdict AS v WHERE v.verdict_id = ?1)",
    )
    .await;
}

/// The same read as a scalar subquery of a statement over the verdict's own
/// row, which names the verdict by a column of the row around it.
#[tokio::test]
async fn a_subquery_around_a_row_narrows_through_the_chain() {
    narrowed_to_verdict_one(
        "SELECT (SELECT group_concat(l.name || m.source) FROM found AS f \
           JOIN matched AS m ON m.verdict_id = f.verdict_id AND m.position = f.match_position \
           JOIN label AS l ON l.verdict_id = m.verdict_id AND l.position = m.position \
           WHERE f.verdict_id = (SELECT w.verdict_id FROM verdict AS w \
                                 WHERE w.verdict_id = o.verdict_id)) \
         FROM verdict AS o WHERE o.verdict_id = ?1",
    )
    .await;
}
