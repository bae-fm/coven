//! An equality between two sources carries what one side's conditions say
//! of its compared column to the other side, whichever source sqlive places
//! first: a source that a condition of its own narrows still depends only on
//! the rows the join can reach.

use super::lookup::follows;
use crate::tests::TestDatabase;
use crate::SqlValue::Blob;

/// Folders a scan found, keyed by their watched folder and path as bytes,
/// and the files of each, with what the scan read each file as.
const SCAN: &str =
    "CREATE TABLE folder(watched BLOB NOT NULL, path BLOB NOT NULL, name TEXT NOT NULL,
    PRIMARY KEY (watched, path)) STRICT;
CREATE TABLE folder_file(watched BLOB NOT NULL, folder_path BLOB NOT NULL, name TEXT NOT NULL,
    role TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (watched, folder_path, name)) STRICT;
INSERT INTO folder VALUES (x'01', x'0101', 'shown');
INSERT INTO folder_file VALUES (x'01', x'0101', 'a', 'audio', 3)";

/// Counting the shown folder's files by a condition on the file's own row
/// reruns for a file of the shown folder, and for one that moves into it,
/// but not for another folder or its file: the folder's path, which the
/// CTE or the joined table keys by the run's value, narrows the files too.
#[tokio::test]
async fn a_source_with_a_condition_of_its_own_keeps_its_join_partners_key() {
    let statements = [
        "WITH shown AS (SELECT watched, path FROM folder WHERE path = ?1 LIMIT 1) \
         SELECT count(*) FROM folder_file AS f \
         JOIN shown ON f.watched = shown.watched AND f.folder_path = shown.path \
         WHERE f.role = 'audio'",
        "WITH shown AS (SELECT watched, path FROM folder WHERE path = ?1 LIMIT 1) \
         SELECT count(*) FROM folder_file AS f \
         JOIN shown ON f.watched = shown.watched AND f.folder_path = shown.path \
         AND f.role = 'audio'",
        "WITH shown AS (SELECT watched, path FROM folder WHERE path = ?1 LIMIT 1) \
         SELECT count(*) FROM folder_file AS f \
         JOIN shown ON f.watched = shown.watched AND f.folder_path = shown.path \
         WHERE f.size > 0",
        "WITH shown AS (SELECT watched, path FROM folder WHERE path = ?1) \
         SELECT count(*) FROM folder_file AS f \
         JOIN shown ON f.watched = shown.watched AND f.folder_path = shown.path \
         WHERE f.role = 'audio'",
        "SELECT count(*) FROM folder_file AS f \
         JOIN folder AS c ON f.watched = c.watched AND f.folder_path = c.path \
         WHERE c.path = ?1 AND f.role = 'audio'",
    ];
    for sql in statements {
        let db = TestDatabase::open(SCAN);
        follows(
            &db,
            sql,
            vec![Blob(vec![1, 1])],
            &[
                (
                    "INSERT INTO folder VALUES (x'01', x'0102', 'other'); \
                     INSERT INTO folder_file VALUES (x'01', x'0102', 'b', 'audio', 4)",
                    false,
                ),
                ("UPDATE folder_file SET size = 5 WHERE name = 'b'", false),
                ("INSERT INTO folder_file VALUES (x'01', x'0101', 'c', 'audio', 5)", true),
                ("UPDATE folder_file SET folder_path = x'0101' WHERE name = 'b'", true),
            ],
        )
        .await;
        db.close();
    }
}
