//! An import pane reads the cards of one ledger cell through a lookup's
//! columns, so the rows of the match, its labels and its links narrow to the
//! cell's verdict by the joins alone (SPEC.md section 7). The cell's lookup
//! names its verdict, step, position and cell, each of which narrows the
//! cell's found releases by the same partner rows, and the lookup's own rows
//! are narrowed by a chain of tables from the pane's folder.

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::{Integer, Text};

/// A folder's scan names its state, the state its verdict, the verdict its
/// ledger rows and their lookups, and a lookup the releases it found, each of
/// which names a match with its labels. Two folders, `/a` and `/b`, each with
/// verdict 1 or 2.
const LEDGER: &str = "CREATE TABLE scan_candidate(path TEXT PRIMARY KEY, content_hash TEXT NOT NULL,
    file_edit_revision INTEGER NOT NULL) STRICT;
CREATE TABLE import_candidate_state(content_hash TEXT PRIMARY KEY, edit_revision INTEGER NOT NULL)
    STRICT;
CREATE TABLE import_candidate_verdict(verdict_id INTEGER PRIMARY KEY,
    content_hash TEXT NOT NULL) STRICT;
CREATE TABLE import_candidate_lookup_value(verdict_id INTEGER NOT NULL, step TEXT NOT NULL,
    position INTEGER NOT NULL, PRIMARY KEY (verdict_id, step, position)) STRICT;
CREATE TABLE import_candidate_lookup(verdict_id INTEGER NOT NULL, step TEXT NOT NULL,
    position INTEGER NOT NULL, cell INTEGER NOT NULL,
    PRIMARY KEY (verdict_id, step, position, cell)) STRICT;
CREATE TABLE import_candidate_lookup_release(verdict_id INTEGER NOT NULL, step TEXT NOT NULL,
    position INTEGER NOT NULL, cell INTEGER NOT NULL, ordinal INTEGER NOT NULL,
    match_position INTEGER NOT NULL, PRIMARY KEY (verdict_id, step, position, cell, ordinal))
    STRICT;
CREATE TABLE import_candidate_match(match_id INTEGER PRIMARY KEY, verdict_id INTEGER NOT NULL,
    position INTEGER NOT NULL, source TEXT NOT NULL, UNIQUE (verdict_id, position)) STRICT;
CREATE TABLE import_candidate_match_label(match_id INTEGER NOT NULL, name TEXT NOT NULL,
    PRIMARY KEY (match_id, name)) STRICT;
INSERT INTO scan_candidate VALUES ('/a', 'ha', 1), ('/b', 'hb', 1);
INSERT INTO import_candidate_state VALUES ('ha', 1), ('hb', 1);
INSERT INTO import_candidate_verdict VALUES (1, 'ha'), (2, 'hb');
INSERT INTO import_candidate_lookup_value VALUES
    (1, 'disc_id', 0), (1, 'catalog', 0), (1, 'catalog', 1),
    (2, 'disc_id', 0), (2, 'catalog', 0), (2, 'catalog', 1);
INSERT INTO import_candidate_lookup VALUES
    (1, 'disc_id', 0, 0), (1, 'catalog', 0, 0), (1, 'catalog', 0, 1), (1, 'catalog', 1, 0),
    (2, 'disc_id', 0, 0), (2, 'catalog', 0, 0), (2, 'catalog', 0, 1), (2, 'catalog', 1, 0);
INSERT INTO import_candidate_lookup_release VALUES
    (1, 'disc_id', 0, 0, 0, 0), (1, 'catalog', 0, 0, 0, 1), (1, 'catalog', 0, 1, 0, 2), (1, 'catalog', 1, 0, 0, 2),
    (2, 'disc_id', 0, 0, 0, 0), (2, 'catalog', 0, 0, 0, 1), (2, 'catalog', 0, 1, 0, 2), (2, 'catalog', 1, 0, 0, 2);
INSERT INTO import_candidate_match VALUES
    (10, 1, 0, 'a'), (11, 1, 1, 'b'), (12, 1, 2, 'e'), (20, 2, 0, 'c'), (21, 2, 1, 'd'),
    (22, 2, 2, 'f');
INSERT INTO import_candidate_match_label VALUES (10, 'x'), (11, 'y'), (20, 'z'), (21, 'w')";

/// The labels of the matches of every cell of the folder `?1`, read as the
/// pane reads a cell's cards: the cell's verdict looked up by its id, and the
/// found releases of the cell joined to the matches they name and to their
/// labels, with no condition of its own on the match's verdict.
const CELL_LABELS: &str = "SELECT (SELECT group_concat((SELECT group_concat(label.name) \
         FROM import_candidate_lookup_release AS found \
         JOIN import_candidate_match AS matched \
           ON matched.verdict_id = found.verdict_id AND matched.position = found.match_position \
         JOIN import_candidate_match_label AS label ON label.match_id = matched.match_id \
         WHERE found.verdict_id = (SELECT of_verdict.verdict_id \
                                   FROM import_candidate_verdict AS of_verdict \
                                   WHERE of_verdict.verdict_id = lookup.verdict_id) \
           AND found.step = lookup.step AND found.position = lookup.position \
           AND found.cell = lookup.cell)) \
       FROM import_candidate_lookup AS lookup \
       WHERE lookup.verdict_id = ledger_row.verdict_id AND lookup.step = ledger_row.step \
         AND lookup.position = ledger_row.position) \
     FROM scan_candidate AS scanned \
     JOIN import_candidate_state AS state ON state.content_hash = scanned.content_hash \
                                         AND state.edit_revision = scanned.file_edit_revision \
     JOIN import_candidate_verdict AS verdict ON verdict.content_hash = state.content_hash \
     JOIN import_candidate_lookup_value AS ledger_row ON ledger_row.verdict_id = verdict.verdict_id \
     WHERE scanned.path = ?1";

/// The labels of another folder's verdict wake nothing, and a label of the
/// pane's own verdict does, though each question about the changed row stays
/// within the step limit.
#[tokio::test]
async fn a_cell_s_cards_narrow_to_its_verdict_through_the_join() {
    let db = TestDatabase::open(LEDGER);
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    follows(
        &db,
        CELL_LABELS,
        vec![Text("/a".into())],
        &[
            ("INSERT INTO import_candidate_match_label VALUES (20, 'v')", false),
            ("UPDATE import_candidate_match SET source = 'e' WHERE verdict_id = 2", false),
            (
                "INSERT INTO import_candidate_lookup_release VALUES (2, 'catalog', 1, 1, 0, 0)",
                false,
            ),
            ("INSERT INTO import_candidate_match_label VALUES (10, 'u')", true),
            ("UPDATE import_candidate_match SET source = 'f' WHERE verdict_id = 1", true),
        ],
    )
    .await;
    assert_eq!(
        counter.take().stopped_questions,
        0,
        "no question about a changed row ran past its steps"
    );
    db.close();
}

/// The labels read through a subquery in FROM narrow as the table does. The
/// subquery waits with its group of joins, since only a table that waits for
/// the verdict's subquery can narrow the group, so the table it reads waits
/// for it too, and takes the values it takes.
#[tokio::test]
async fn a_cell_s_cards_narrow_a_table_read_through_a_subquery_in_from() {
    let db = TestDatabase::open(LEDGER);
    let sql = CELL_LABELS.replace(
        "JOIN import_candidate_match_label AS label",
        "JOIN (SELECT match_id, name FROM import_candidate_match_label) AS label",
    );
    follows(
        &db,
        // `follows` keeps the statement for as long as its query runs.
        sql.leak(),
        vec![Text("/a".into())],
        &[
            ("INSERT INTO import_candidate_match_label VALUES (20, 'v')", false),
            ("INSERT INTO import_candidate_match_label VALUES (10, 'u')", true),
        ],
    )
    .await;
    db.close();
}

/// The labels of the cards of the cells numbered `?2`, read with the statement
/// above and one more condition: the cell's number.
const CELL_ONLY_LABELS: &str = "SELECT (SELECT group_concat((SELECT group_concat(label.name) \
         FROM import_candidate_lookup_release AS found \
         JOIN import_candidate_match AS matched \
           ON matched.verdict_id = found.verdict_id AND matched.position = found.match_position \
         JOIN import_candidate_match_label AS label ON label.match_id = matched.match_id \
         WHERE found.verdict_id = (SELECT of_verdict.verdict_id \
                                   FROM import_candidate_verdict AS of_verdict \
                                   WHERE of_verdict.verdict_id = lookup.verdict_id) \
           AND found.step = lookup.step AND found.position = lookup.position \
           AND found.cell = lookup.cell)) \
       FROM import_candidate_lookup AS lookup \
       WHERE lookup.verdict_id = ledger_row.verdict_id AND lookup.step = ledger_row.step \
         AND lookup.position = ledger_row.position AND lookup.cell = ?2) \
     FROM scan_candidate AS scanned \
     JOIN import_candidate_state AS state ON state.content_hash = scanned.content_hash \
                                         AND state.edit_revision = scanned.file_edit_revision \
     JOIN import_candidate_verdict AS verdict ON verdict.content_hash = state.content_hash \
     JOIN import_candidate_lookup_value AS ledger_row ON ledger_row.verdict_id = verdict.verdict_id \
     WHERE scanned.path = ?1";

/// The labels of the matches of another cell of the same verdict wake nothing:
/// one row of `lookup` holds a found release's step, position and cell
/// together, not a row for each.
#[tokio::test]
async fn a_cell_s_cards_narrow_to_the_cell() {
    let db = TestDatabase::open(LEDGER);
    let counter = crate::ConnectionWorkCounter::new();
    db.track_work(&counter);
    follows(
        &db,
        CELL_ONLY_LABELS,
        vec![Text("/a".into()), Integer(1)],
        &[
            ("INSERT INTO import_candidate_match_label VALUES (11, 'v')", false),
            ("INSERT INTO import_candidate_match_label VALUES (22, 'u')", false),
            ("INSERT INTO import_candidate_match_label VALUES (12, 't')", true),
        ],
    )
    .await;
    assert_eq!(
        counter.take().stopped_questions,
        0,
        "no question about a changed row ran past its steps"
    );
    db.close();
}
