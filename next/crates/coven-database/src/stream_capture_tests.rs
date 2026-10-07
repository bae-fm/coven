use crate::{tests::TestStore, DbError};

#[tokio::test]
async fn streamed_commits_observe_without_rowid_and_virtual_tables_and_discard_rollbacks() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID; CREATE VIRTUAL TABLE search USING fts5(value)").await.unwrap();
    let mut items = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?));
    let mut search = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM search", [], |r| r.get::<_, String>(0))?));
    assert!(items.next().await.unwrap().is_empty());
    assert!(search.next().await.unwrap().is_empty());
    db.inspect_writer(|writer| {
        assert!(writer
            .stream_transaction::<()>(|writer| {
                writer.batch(
                    "INSERT INTO items VALUES('one','failed'); INSERT INTO search VALUES('failed')",
                )?;
                Err(DbError::TransactionEnded)
            })
            .is_err());
    });
    assert!(!items.is_marked_for_rerun());
    assert!(!search.is_marked_for_rerun());
    db.inspect_writer(|writer| {
        writer
            .stream_transaction(|writer| {
                writer.batch(
                    "INSERT INTO items VALUES('one','stored'); INSERT INTO search VALUES('stored')",
                )
            })
            .unwrap()
    });
    assert_eq!(items.next().await.unwrap(), ["stored"]);
    assert_eq!(search.next().await.unwrap(), ["stored"]);
    db.inspect_writer(|writer| {
        writer
            .stream_transaction(|writer| writer.batch("DELETE FROM items; DELETE FROM search"))
            .unwrap()
    });
    assert!(items.next().await.unwrap().is_empty());
    assert!(search.next().await.unwrap().is_empty());
    db.close().await.unwrap();
}
