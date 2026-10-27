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
    assert_eq!(tracks.next().await.unwrap(), ["a", "b"]);
    sql(&db, "UPDATE queue SET played=1 WHERE id=1")
        .await
        .unwrap();
    assert!(!tracks.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET track='c' WHERE id=2")
        .await
        .unwrap();
    assert_eq!(tracks.next().await.unwrap(), ["a", "c"]);
    sql(&db, "INSERT INTO queue(id, track) VALUES (3,'d')")
        .await
        .unwrap();
    assert_eq!(tracks.next().await.unwrap(), ["a", "c", "d"]);
    sql(&db, "DELETE FROM queue WHERE id=1").await.unwrap();
    assert_eq!(tracks.next().await.unwrap(), ["c", "d"]);
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
    assert_eq!(first.next().await.unwrap(), ["a"]);
    sql(&db, "UPDATE queue SET track='z' WHERE id=2; INSERT INTO queue(id, track) VALUES (3,'d'); DELETE FROM queue WHERE id=3")
        .await
        .unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET id=4 WHERE id=2").await.unwrap();
    assert!(!first.is_marked_for_rerun());
    sql(&db, "UPDATE queue SET track='y' WHERE id=1")
        .await
        .unwrap();
    assert_eq!(first.next().await.unwrap(), ["y"]);
    sql(&db, "DELETE FROM queue WHERE id=1").await.unwrap();
    assert!(first.next().await.unwrap().is_empty());
    db.close().await.unwrap();
}
