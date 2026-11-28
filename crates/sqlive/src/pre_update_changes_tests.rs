use crate::tests::{next, no_values, rerun, TestDatabase, TestOptions};

/// An uncaptured queue table beside a captured notes table.
fn queue() -> TestDatabase {
    TestDatabase::open_with(
        "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
         CREATE TABLE queue(id INTEGER NOT NULL PRIMARY KEY, track TEXT NOT NULL, played INTEGER NOT NULL DEFAULT 0);
         INSERT INTO queue(id, track) VALUES (1,'a'),(2,'b')",
        TestOptions { captured: ["notes".to_owned()].into(), ..TestOptions::default() },
    )
}

/// A live query that reads a uncaptured table by column reruns for a
/// change to a column it reads, and not for one to a column it doesn't.
#[tokio::test]
async fn a_local_tables_columns_still_decide_a_rerun() {
    let db = queue();
    let mut tracks = db.subscribe_rows("SELECT track FROM queue ORDER BY id", no_values(), |r| {
        r.get::<_, String>(0)
    });
    assert_eq!(next(&mut tracks).await, ["a", "b"]);
    db.write("UPDATE queue SET played=1 WHERE id=1").unwrap();
    assert!(!tracks.is_marked_for_rerun());
    db.write("UPDATE queue SET track='c' WHERE id=2").unwrap();
    assert_eq!(next(&mut tracks).await, ["a", "c"]);
    db.write("INSERT INTO queue(id, track) VALUES (3,'d')").unwrap();
    assert_eq!(next(&mut tracks).await, ["a", "c", "d"]);
    db.write("DELETE FROM queue WHERE id=1").unwrap();
    assert_eq!(next(&mut tracks).await, ["c", "d"]);
    db.close();
}

/// A live query that reads one row of a uncaptured table by its key
/// reruns only for that row's changes: while it waits, writes keep that
/// table's keys.
#[tokio::test]
async fn a_key_scoped_query_of_a_local_table_reruns_only_for_its_row() {
    let db = queue();
    let mut first = db.subscribe_rows("SELECT track FROM queue WHERE id = 1", no_values(), |r| {
        r.get::<_, String>(0)
    });
    assert_eq!(next(&mut first).await, ["a"]);
    db.write("UPDATE queue SET track='z' WHERE id=2; INSERT INTO queue(id, track) VALUES (3,'d'); DELETE FROM queue WHERE id=3")
        .unwrap();
    assert!(!first.is_marked_for_rerun());
    db.write("UPDATE queue SET id=4 WHERE id=2").unwrap();
    assert!(!first.is_marked_for_rerun());
    db.write("UPDATE queue SET track='y' WHERE id=1").unwrap();
    assert_eq!(next(&mut first).await, ["y"]);
    db.write("DELETE FROM queue WHERE id=1").unwrap();
    assert!(next(&mut first).await.is_empty());
    db.close();
}

/// A write past the per-table bound of row changes keeps only that the
/// table changed: a narrowed query runs again, as for any whole-table
/// change, while a smaller write outside its range doesn't wake it.
#[tokio::test]
async fn a_write_past_the_row_bound_wakes_every_query_of_the_table() {
    let db = queue();
    let mut first =
        db.subscribe_rows("SELECT track FROM queue WHERE track = 'a'", no_values(), |r| {
            r.get::<_, String>(0)
        });
    assert_eq!(next(&mut first).await, ["a"]);
    let fill = |rows: usize| {
        format!(
            "WITH RECURSIVE n(i) AS (VALUES(100000) UNION ALL SELECT i+1 FROM n WHERE i<{}) INSERT INTO queue(id, track) SELECT i, 'other' FROM n",
            100_000 + rows - 1
        )
    };
    db.write(&fill(5)).unwrap();
    assert!(!first.is_marked_for_rerun());
    db.write("DELETE FROM queue WHERE id >= 100000").unwrap();
    assert!(!first.is_marked_for_rerun());
    db.write(&fill(crate::ROWS_PER_TABLE + 1)).unwrap();
    assert_eq!(rerun(&mut first), 2);
    db.close();
}

/// How many of the writer's statements in its last profiled commit read a
/// table's shape: its columns, or which tables are shadow tables.
fn shape_reads(db: &TestDatabase) -> usize {
    db.host(|writer| {
        writer
            .profiled_statements()
            .iter()
            .filter(|statement| {
                statement.sql.contains("pragma_table_xinfo")
                    || statement.sql.contains("pragma_table_list")
            })
            .count()
    })
}

/// The writer reads a table's shape once for each schema, not once for each
/// commit: after that read, an app's write, a bulk write and the host's own
/// transaction each read no shape, commit after commit.
#[tokio::test]
async fn commits_read_no_table_shape_their_schema_already_gave() {
    let db = queue();
    db.write("INSERT INTO notes VALUES('first','a'); UPDATE queue SET played=1").unwrap();
    let mut reads = Vec::new();
    for i in 0..3 {
        db.write(&format!(
            "INSERT INTO notes VALUES('n{i}','t'); UPDATE queue SET played=played+1 WHERE id=1"
        ))
        .unwrap();
        reads.push(shape_reads(&db));
        db.bulk("UPDATE queue SET track='x' WHERE id=2").unwrap();
        reads.push(shape_reads(&db));
        db.commit("UPDATE queue SET played=0");
        reads.push(shape_reads(&db));
    }
    assert_eq!(reads, [0; 9]);
    db.close();
}

/// A commit after the schema changed reads its tables by their new shape.
/// When the host adds a column after an earlier commit read the table, a
/// live query that reads only that column runs again when a commit changes
/// the column, whether a session or the hook on an app's write saw the
/// change.
#[tokio::test]
async fn a_commit_after_a_schema_change_reads_the_tables_new_shape() {
    let db = queue();
    db.write("UPDATE queue SET played=1 WHERE id=1").unwrap();
    db.commit("UPDATE queue SET played=2 WHERE id=1");
    db.commit("ALTER TABLE queue ADD COLUMN rating INTEGER");
    let mut ratings =
        db.subscribe_rows("SELECT rating FROM queue WHERE id = 1", no_values(), |r| {
            r.get::<_, Option<i64>>(0)
        });
    assert_eq!(next(&mut ratings).await, [None]);
    db.commit("UPDATE queue SET rating=5 WHERE id=1");
    assert!(ratings.is_marked_for_rerun());
    assert_eq!(next(&mut ratings).await, [Some(5)]);
    db.write("UPDATE queue SET rating=4 WHERE id=1").unwrap();
    assert!(ratings.is_marked_for_rerun());
    assert_eq!(next(&mut ratings).await, [Some(4)]);
    db.close();
}
