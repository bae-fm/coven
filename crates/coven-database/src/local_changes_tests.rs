use crate::tests::TestStore;
use crate::write::tests::sql;
use crate::{Migration, RowIdentity, SyncedTable};

/// A device-local queue beside a synced table.
async fn queue(store: &TestStore) -> crate::Database {
    store
        .builder(
            vec![SyncedTable::new("notes", RowIdentity::SharedKey)],
            vec![Migration::addition(
                1,
                "schema",
                "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT NOT NULL);
                 CREATE TABLE queue(id INTEGER NOT NULL PRIMARY KEY, track TEXT NOT NULL, played INTEGER NOT NULL DEFAULT 0);
                 INSERT INTO queue(id, track) VALUES (1,'a'),(2,'b')",
            )],
        )
        .open()
        .await
        .unwrap()
}

/// A live query that reads a device-local table by column reruns for a
/// change to a column it reads, and not for one to a column it doesn't.
#[tokio::test]
async fn a_local_tables_columns_still_decide_a_rerun() {
    let store = TestStore::new();
    let db = queue(&store).await;
    let mut tracks = db.subscribe(|sql| {
        Ok(sql.query("SELECT track FROM queue ORDER BY id", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert_eq!(
        tracks
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["a", "b"]
    );
    sql(&db, "UPDATE queue SET played=1 WHERE id=1")
        .await
        .unwrap();
    assert!(!tracks.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET track='c' WHERE id=2")
        .await
        .unwrap();
    assert_eq!(
        tracks
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["a", "c"]
    );
    sql(&db, "INSERT INTO queue(id, track) VALUES (3,'d')")
        .await
        .unwrap();
    assert_eq!(
        tracks
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["a", "c", "d"]
    );
    sql(&db, "DELETE FROM queue WHERE id=1").await.unwrap();
    assert_eq!(
        tracks
            .next()
            .await
            .expect("the live query answers")
            .unwrap(),
        ["c", "d"]
    );
    db.close().await.unwrap();
}

/// A live query that reads one row of a device-local table by its key
/// reruns only for that row's changes: while it waits, writes keep that
/// table's keys.
#[tokio::test]
async fn a_key_scoped_query_of_a_local_table_reruns_only_for_its_row() {
    let store = TestStore::new();
    let db = queue(&store).await;
    let mut first = db.subscribe(|sql| {
        Ok(sql.query("SELECT track FROM queue WHERE id = 1", [], |r| {
            r.get::<_, String>(0)
        })?)
    });
    assert_eq!(
        first.next().await.expect("the live query answers").unwrap(),
        ["a"]
    );
    sql(&db, "UPDATE queue SET track='z' WHERE id=2; INSERT INTO queue(id, track) VALUES (3,'d'); DELETE FROM queue WHERE id=3")
        .await
        .unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET id=4 WHERE id=2").await.unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET track='y' WHERE id=1")
        .await
        .unwrap();
    assert_eq!(
        first.next().await.expect("the live query answers").unwrap(),
        ["y"]
    );
    sql(&db, "DELETE FROM queue WHERE id=1").await.unwrap();
    assert!(first
        .next()
        .await
        .expect("the live query answers")
        .unwrap()
        .is_empty());
    db.close().await.unwrap();
}

/// A write past the per-table bound of row changes keeps only that the
/// table changed: a narrowed query runs again, as for any whole-table
/// change, while a smaller write outside its range doesn't wake it.
#[tokio::test]
async fn a_write_past_the_row_bound_wakes_every_query_of_the_table() {
    let store = TestStore::new();
    let db = queue(&store).await;
    let runs = std::sync::atomic::AtomicUsize::new(0);
    let mut first = db.subscribe(move |sql| {
        let run = runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let tracks = sql.query("SELECT track FROM queue WHERE track = 'a'", [], |r| {
            r.get::<_, String>(0)
        })?;
        Ok((run, tracks))
    });
    assert_eq!(
        first.next().await.expect("the live query answers").unwrap(),
        (0, vec!["a".to_string()])
    );
    let fill = |rows: usize| -> &'static str {
        Box::leak(
            format!(
                "WITH RECURSIVE n(i) AS (VALUES(100000) UNION ALL SELECT i+1 FROM n WHERE i<{}) INSERT INTO queue(id, track) SELECT i, 'other' FROM n",
                100_000 + rows - 1
            )
            .into_boxed_str(),
        )
    };
    sql(&db, fill(5)).await.unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, "DELETE FROM queue WHERE id >= 100000")
        .await
        .unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, fill(super::ROWS_PER_TABLE + 1)).await.unwrap();
    assert_eq!(
        first.next().await.expect("the live query answers").unwrap(),
        (1, vec!["a".to_string()])
    );
    db.close().await.unwrap();
}
