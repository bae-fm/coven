use crate::tests::{next, TestDatabase, TestError};

#[tokio::test]
async fn streamed_commits_observe_without_rowid_and_virtual_tables_and_discard_rollbacks() {
    let db = TestDatabase::open(
        "CREATE TABLE items(id TEXT PRIMARY KEY,value TEXT) WITHOUT ROWID; CREATE VIRTUAL TABLE search USING fts5(value)",
    );
    let mut items = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM items", [], |r| r.get::<_, String>(0))?));
    let mut search = db
        .subscribe(|sql| Ok(sql.query("SELECT value FROM search", [], |r| r.get::<_, String>(0))?));
    assert!(next(&mut items).await.is_empty());
    assert!(next(&mut search).await.is_empty());
    db.host(|writer| {
        assert!(writer
            .stream_transaction::<(), TestError>(|writer| {
                writer.batch(
                    "INSERT INTO items VALUES('one','failed'); INSERT INTO search VALUES('failed')",
                )?;
                Err(crate::Error::TransactionEnded)
            })
            .is_err());
    });
    assert!(!items.is_marked_for_rerun());
    assert!(!search.is_marked_for_rerun());
    db.host(|writer| {
        writer
            .stream_transaction(|writer| {
                writer.batch(
                    "INSERT INTO items VALUES('one','stored'); INSERT INTO search VALUES('stored')",
                )
            })
            .unwrap()
    });
    assert_eq!(next(&mut items).await, ["stored"]);
    assert_eq!(next(&mut search).await, ["stored"]);
    db.host(|writer| {
        writer
            .stream_transaction(|writer| writer.batch("DELETE FROM items; DELETE FROM search"))
            .unwrap()
    });
    assert!(next(&mut items).await.is_empty());
    assert!(next(&mut search).await.is_empty());
    db.close();
}
