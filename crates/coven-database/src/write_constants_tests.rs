use crate::tests::TestStore;
use crate::write::tests::count;
use crate::SyncedTable;

/// A local write reads what it can't change once, not once a row: the
/// schema version, each column's registered id and its own authored part.
#[tokio::test]
async fn a_write_reads_its_constants_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    crate::write::tests::sql(
        &db,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<50)
         INSERT INTO notes SELECT 'note ' || i, 'title', 'body' FROM n",
    )
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(runs("PRAGMA user_version"), 1);
    assert_eq!(runs("FROM _coven_columns WHERE table_name=?1 AND column_name=?2"), 3);
    assert_eq!(runs("FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2"), 1);
    db.close().await.unwrap();
}

/// A local write checks once, not once an app statement, that the schema
/// its kept statements were judged against is still the one in effect.
#[tokio::test]
async fn a_write_reads_the_schema_cookie_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)],
            "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        for i in 0..50 {
            sql.execute("INSERT INTO notes VALUES(?1,'title','body')", [format!("note {i}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql == text).count();
    assert_eq!(runs("PRAGMA main.schema_version"), 1);
    assert_eq!(runs("INSERT INTO notes VALUES(?1,'title','body')"), 50);
    db.close().await.unwrap();
}

/// A local write reads each foreign key's registered id once, not once for
/// each reference it stores.
#[tokio::test]
async fn a_write_reads_each_foreign_key_id_once() {
    let store = TestStore::new();
    let db = store
        .schema(
            vec![
                SyncedTable::new("parents", crate::RowIdentity::SharedKey),
                SyncedTable::new("children", crate::RowIdentity::SharedKey),
            ],
            "CREATE TABLE parents(id TEXT NOT NULL PRIMARY KEY);
             CREATE TABLE children(id TEXT NOT NULL PRIMARY KEY,
                 parent TEXT NOT NULL REFERENCES parents(id) ON DELETE CASCADE)",
        )
        .await
        .unwrap();
    db.write(|sql| {
        sql.execute("INSERT INTO parents VALUES('p')", [])?;
        for i in 0..20 {
            sql.execute("INSERT INTO children VALUES(?1,'p')", [format!("c{i}")])?;
        }
        Ok(())
    })
    .await
    .unwrap();
    let statements = db.inspect_writer(|writer| writer.profiled_statements());
    let runs = |text: &str| statements.iter().filter(|s| s.sql.contains(text)).count();
    assert_eq!(
        runs("SELECT (SELECT id FROM _coven_foreign_keys WHERE table_name=?1 AND identity=?2)"),
        1
    );
    assert_eq!(count(&db, "_coven_references"), 20);
    db.close().await.unwrap();
}

/// Applying another device's write reads what it can't change once, as a
/// local write does, not once for every row or cell it stores.
#[tokio::test]
async fn applying_a_write_reads_its_constants_once() {
    let ids = coven_foundation::id_source::SequentialIds::new();
    let (author, receiver) = (TestStore::with_ids(&ids), TestStore::with_ids(&ids));
    let tables = || vec![SyncedTable::new("notes", crate::RowIdentity::SharedKey)];
    const NOTES: &str = "CREATE TABLE notes(id TEXT NOT NULL PRIMARY KEY, title TEXT, body TEXT)";
    let a = author.schema(tables(), NOTES).await.unwrap();
    let b = receiver.schema(tables(), NOTES).await.unwrap();
    crate::write::tests::sql(
        &a,
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<50)
         INSERT INTO notes SELECT 'note ' || i, 'title', 'body' FROM n",
    )
    .await
    .unwrap();
    let counter = crate::test_utils::WorkCounter::default();
    b.test_track_work(counter.clone()).await.unwrap();
    crate::author_judgement::tests::deliver(&a, &b).await;
    // The schema version as the write arrives and again in the transaction
    // applying it, the schema cookie its materialized rows are judged
    // against, and the three columns' ids.
    assert_eq!(counter.take().constant_reads, 6);
    assert_eq!(count(&b, "notes"), 50);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
