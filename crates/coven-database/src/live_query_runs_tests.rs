use crate::tests::TestStore;
use crate::Database;
use std::time::Duration;

async fn next(query: &mut crate::LiveQuery<bool>) -> bool {
    tokio::time::timeout(Duration::from_secs(5), query.next())
        .await
        .expect("the live query answers")
        .expect("the query is open")
        .unwrap()
}

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
    let mut query = db
        .subscribe(|sql| {
            Ok(
                sql.query_row("SELECT value > 0 FROM items WHERE id = 1", [], |r| {
                    r.get::<_, bool>(0)
                })?,
            )
        })
        .process(Ok);
    let runs = query.runs();
    assert!(!next(&mut query).await);
    write(&db, "UPDATE items SET value = 1 WHERE id = 2");
    write(&db, "UPDATE items SET value = 2 WHERE id = 1");
    assert!(next(&mut query).await);
    write(&db, "UPDATE items SET value = 3 WHERE id = 1");
    // The run this write causes finds the same result, and returns nothing.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), query.next())
            .await
            .is_err()
    );
    write(&db, "UPDATE items SET value = 0 WHERE id = 1");
    assert!(!next(&mut query).await);
    assert_eq!((runs.runs(), runs.writes(), runs.equal()), (4, 3, 1));
    db.close().await.unwrap();
}
