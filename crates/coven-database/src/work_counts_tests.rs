use super::WorkCounter;
use crate::test_utils::{notes_migrations, notes_tables};
use crate::tests::{next, TestStore};
use crate::DbError;

/// A live query's first run builds its statement's scope plan, which the
/// store's work counts with its size.
#[tokio::test]
async fn a_live_querys_first_run_counts_its_scope_plan() {
    let store = TestStore::new();
    let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
    let counter = WorkCounter::new();
    db.test_track_work(counter.clone()).await.unwrap();
    let mut query: crate::LiveQuery<_, DbError> = db.subscribe(|sql| {
        Ok(sql.query_row("SELECT count(*) FROM notes WHERE id = 'a'", [], |r| r.get::<_, i64>(0))?)
    });
    assert_eq!(next(&mut query).await, 0);
    let work = counter.take();
    assert_eq!(
        (
            work.scope_plans_built,
            work.scope_plan_sources,
            work.scope_plan_cores,
            work.scope_plan_edges
        ),
        (1, 1, 1, 0)
    );
    drop(query);
    db.close().await.unwrap();
}
