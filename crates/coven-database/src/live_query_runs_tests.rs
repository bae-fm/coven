use crate::tests::next;
use crate::tests::TestStore;
use crate::Database;

fn write(db: &Database, sql: &str) {
    db.commit_writer(|db| db.batch(sql).unwrap());
}

/// A run a write causes counts as one; one whose result is the same counts
/// as equal too, and isn't returned. A write outside the query's scope runs
/// nothing.
#[tokio::test]
async fn counts_runs_writes_and_equal_results() {
    let store = TestStore::new();
    let db = store.schema(vec![], "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES (1, 0), (2, 0)").await.unwrap();
    let (ran, mut runs) = tokio::sync::mpsc::unbounded_channel();
    let mut query = db
        .subscribe(move |sql| {
            let read = sql.query_row("SELECT value > 0 FROM items WHERE id = 1", [], |r| {
                r.get::<_, bool>(0)
            })?;
            #[expect(
                clippy::let_underscore_must_use,
                reason = "a test that stopped listening has no use for it"
            )]
            let _ = ran.send(());
            Ok(read)
        })
        .process(Ok);
    assert!(!next(&mut query).await);
    write(&db, "UPDATE items SET value = 1 WHERE id = 2");
    write(&db, "UPDATE items SET value = 2 WHERE id = 1");
    assert!(next(&mut query).await);
    while runs.try_recv().is_ok() {}
    write(&db, "UPDATE items SET value = 3 WHERE id = 1");
    // The run this write causes finds the same result, and returns nothing.
    tokio::select! {
        result = query.next() => panic!("an equal result was returned: {result:?}"),
        read = runs.recv() => read.unwrap(),
    }
    write(&db, "UPDATE items SET value = 0 WHERE id = 1");
    assert!(!next(&mut query).await);
    let runs = query.runs();
    assert_eq!((runs.runs(), runs.writes(), runs.equal()), (4, 3, 1));
    db.close().await.unwrap();
}
