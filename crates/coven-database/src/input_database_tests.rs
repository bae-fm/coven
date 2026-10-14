use super::*;
use crate::tests::TestStore;

async fn input(db: &Database, path: &str, version: u8, replaceable: bool) -> RetainedInput {
    db.observe_input(path.to_owned(), vec![version], vec![version], replaceable)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn ranges_facts_and_uncertainty_survive_reopen_and_explicit_eviction() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let first = input(&db, "write", 1, false).await;
    first.keep_range(0, vec![1, 2, 3, 4]).await.unwrap();
    first.keep_metadata(vec![9]).await.unwrap();
    first.keep_files(None).await.unwrap();
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let first = input(&db, "write", 1, false).await;
    let clipped = first.ranges(1, 3).await.unwrap();
    assert_eq!(
        (clipped[0].offset, clipped[0].bytes.as_slice()),
        (1, &[2, 3][..])
    );
    assert_eq!(first.metadata().await.unwrap(), Some(vec![9]));
    assert_eq!(first.files().await.unwrap(), Some(None));
    first.release_ranges().await.unwrap();
    assert_eq!(
        db.set_input_budget(3).await.unwrap(),
        InputBudget {
            maximum: 3,
            retained: 0,
            evicted: 4
        }
    );
    let first = input(&db, "write", 1, false).await;
    assert!(first.ranges(0, 4).await.unwrap().is_empty());
    assert_eq!(first.metadata().await.unwrap(), Some(vec![9]));
    assert_eq!(first.files().await.unwrap(), Some(None));
    first.keep_range(1, vec![2, 3]).await.unwrap();
    assert_eq!(db.input_budget().await.unwrap().retained, 2);
}

#[tokio::test]
async fn replacement_invalidates_only_its_path_and_immutable_changes_preserve_evidence() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let write = input(&db, "write", 1, false).await;
    write.keep_range(0, vec![1]).await.unwrap();
    assert!(db
        .observe_input("write".into(), vec![2], vec![2], false)
        .await
        .unwrap()
        .is_none());
    let peer = input(&db, "peer", 1, true).await;
    peer.keep_range(0, vec![2]).await.unwrap();
    peer.keep_metadata(vec![3]).await.unwrap();
    let peer = input(&db, "peer", 2, true).await;
    assert!(peer.ranges(0, 1).await.unwrap().is_empty());
    assert_eq!(peer.metadata().await.unwrap(), None);
    assert_eq!(write.ranges(0, 1).await.unwrap()[0].bytes, [1]);
    assert_eq!(db.input_budget().await.unwrap().retained, 1);
    assert_eq!(
        db.input_budget().await.unwrap().evicted,
        0,
        "a superseded revision is not eviction work"
    );
}

#[tokio::test]
async fn eviction_keeps_authenticated_file_references() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let input = input(&db, "snapshot", 1, false).await;
    let files = BTreeSet::from([(DeviceId(5), FileId(uuid::Uuid::from_u128(6)))]);
    input.keep_files(Some(files.clone())).await.unwrap();
    input.keep_range(0, vec![1, 2]).await.unwrap();
    input.release_ranges().await.unwrap();
    db.set_input_budget(0).await.unwrap();
    let input = self::input(&db, "snapshot", 1, false).await;
    assert_eq!(input.files().await.unwrap(), Some(Some(files)));
}

#[tokio::test]
async fn unchanged_observations_and_facts_take_no_writer_or_transaction() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let saved = input(&db, "write", 1, false).await;
    saved.keep_range(0, vec![1, 2]).await.unwrap();
    saved.keep_metadata(vec![3]).await.unwrap();
    saved.keep_files(None).await.unwrap();
    let work = crate::test_utils::WorkCounter::default();
    db.test_track_work(work.clone()).await.unwrap();
    for _ in 0..3 {
        let saved = input(&db, "write", 1, false).await;
        assert_eq!(saved.ranges(0, 2).await.unwrap()[0].bytes, [1, 2]);
        saved.keep_metadata(vec![3]).await.unwrap();
        saved.keep_files(None).await.unwrap();
    }
    assert_eq!(work.take(), crate::test_utils::WorkCounts::default());
}

#[tokio::test]
async fn a_failed_range_commit_preserves_prior_ranges_and_budget() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let input = input(&db, "write", 1, false).await;
    input.keep_range(0, vec![1, 2]).await.unwrap();
    db.call(|inner| inner.inputs.write(|db| db.batch("CREATE TRIGGER refuse_input AFTER INSERT ON _coven_input_ranges BEGIN SELECT RAISE(ABORT,'input failed'); END"))).await.unwrap();
    let before = db.input_budget().await.unwrap();
    assert!(input.keep_range(2, vec![3, 4]).await.is_err());
    assert_eq!(db.input_budget().await.unwrap(), before);
    assert_eq!(input.ranges(0, 4).await.unwrap()[0].bytes, [1, 2]);
    db.call(|inner| {
        inner
            .inputs
            .write(|db| db.batch("DROP TRIGGER refuse_input"))
    })
    .await
    .unwrap();
    input.keep_range(2, vec![3, 4]).await.unwrap();
    assert_eq!(db.input_budget().await.unwrap().retained, 4);
}

#[tokio::test]
async fn budget_exhaustion_preserves_pending_bytes_and_the_previous_limit() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let retained = input(&db, "waiting-write", 1, false).await;
    retained.keep_range(0, vec![1, 2, 3]).await.unwrap();
    drop(retained);
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let before = db.input_budget().await.unwrap();
    assert!(db.set_input_budget(2).await.is_err());
    assert_eq!(db.input_budget().await.unwrap(), before);
    assert_eq!(
        input(&db, "waiting-write", 1, false)
            .await
            .ranges(0, 3)
            .await
            .unwrap()[0]
            .bytes,
        [1, 2, 3]
    );
}

#[tokio::test]
async fn an_active_consumer_protects_previously_released_ranges() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let saved = input(&db, "write", 1, false).await;
    saved.keep_range(0, vec![1]).await.unwrap();
    saved.release_ranges().await.unwrap();
    let reading = input(&db, "write", 1, false).await;
    assert!(matches!(
        db.set_input_budget(0).await,
        Err(DbError::InputBudgetExceeded { .. })
    ));
    drop(reading);
    assert_eq!(db.set_input_budget(0).await.unwrap().evicted, 1);
}

#[tokio::test]
async fn range_queries_seek_past_preceding_chunks_and_preserve_gaps() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let input = input(&db, "write", 1, false).await;
    input.write(|db,id| {
        for start in (0..10_000).step_by(2) {
            db.internal_execute("INSERT INTO _coven_input_ranges(object,start,end,bytes) VALUES(?1,?2,?3,X'01')", (id,start,start+1))?;
        }
        for (query, bound) in [(RANGES, "object=? AND start>? AND start<?"), (RANGE_OVERLAP, "object=? AND start=?")] {
            let plans = db.query(&format!("EXPLAIN QUERY PLAN {query}"), (id,9998,10000), |r| r.get::<_,String>(3))?;
            assert!(plans.iter().any(|plan| plan.contains(bound)), "{plans:?}");
        }
        Ok(())
    }).await.unwrap();
    let ranges = input.ranges(9997, 10000).await.unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(
        (ranges[0].offset, ranges[0].bytes.as_slice()),
        (9998, &[1][..])
    );
    assert!(input.keep_range(9997, vec![2, 3]).await.is_err());
    input.keep_range(9997, vec![2]).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn input_commits_do_not_wait_for_the_application_writer() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    let input = input(&db, "write", 1, false).await;
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let (release, wait_release) = std::sync::mpsc::channel();
    let writer = db.clone();
    let writing = tokio::spawn(async move {
        writer
            .write(move |_| {
                entered.send(()).unwrap();
                wait_release
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
                Ok(())
            })
            .await
    });
    wait_entered.await.unwrap();
    let retained = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        input.keep_range(0, vec![1, 2]),
    )
    .await;
    release.send(()).unwrap();
    writing.await.unwrap().unwrap();
    retained
        .expect("input persistence waited for the application writer")
        .unwrap();
}
