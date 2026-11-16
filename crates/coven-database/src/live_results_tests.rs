use crate::DbError;
use crate::{
    test_utils::{notes_migrations, notes_tables},
    tests::TestStore,
};

/// A skipped write leaves no blocking-pool thread waiting: with one blocking
/// thread, other blocking work still runs while `next_result` waits for the
/// change after the skip.
#[test]
fn next_result_after_a_skipped_write_waits_without_holding_a_pool_thread() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = TestStore::new();
        let db = store.builder(notes_tables(), notes_migrations()).open().await.unwrap();
        db.commit_writer(|sql, _| sql.batch("INSERT INTO notes VALUES ('a','Plan','')").unwrap());
        let (ran, mut runs) = tokio::sync::mpsc::unbounded_channel();
        let query: crate::LiveQuery<_, DbError> = db.subscribe(move |sql| {
            ran.send(()).unwrap();
            Ok(sql.query_row("SELECT count(*) > 0 FROM notes", [], |r| r.get::<_, bool>(0))?)
        });
        let (query, first) = crate::next_result(query).await;
        assert!(first.unwrap().unwrap());
        runs.recv().await.unwrap();
        let answer = tokio::spawn(crate::next_result(query));
        db.commit_writer(|sql, _| sql.batch("INSERT INTO notes VALUES ('b','Plan','')").unwrap());
        // The run that finds an equal result has started.
        runs.recv().await.unwrap();
        // Blocking work queued behind that run starts once it ends, which a
        // parked thread would prevent.
        coven_foundation::clock::within(tokio::task::spawn_blocking(|| ()))
            .await
            .expect("the pool's one thread is free")
            .unwrap();
        assert!(!answer.is_finished(), "next_result returned an equal result");
        db.commit_writer(|sql, _| sql.batch("DELETE FROM notes").unwrap());
        let (_query, second) = answer.await.unwrap();
        assert!(!second.unwrap().unwrap());
        db.close().await.unwrap();
    });
}
