use super::*;
use crate::tests::TestStore;
use std::collections::BTreeMap;

fn input(writer: u64, number: u64) -> LogInput {
    LogInput {
        writer: DeviceId(writer),
        number: number.try_into().unwrap(),
        observation: vec![number as u8],
        bytes: vec![number as u8, 0],
    }
}

#[tokio::test]
async fn cursor_never_crosses_a_fetch_ahead_gap_and_reopen_keeps_every_body() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.keep_log_inputs(LogKind::Entry, vec![input(42, 3), input(7, 1)])
            .await
            .unwrap(),
        BTreeMap::from([(DeviceId(7), 1), (DeviceId(42), 0)])
    );
    assert_eq!(
        db.keep_log_inputs(LogKind::Entry, vec![input(42, 1)])
            .await
            .unwrap(),
        BTreeMap::from([(DeviceId(42), 1)])
    );
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(
        db.log_inputs(LogKind::Entry, vec![(DeviceId(7), 0), (DeviceId(42), 0)])
            .await
            .unwrap(),
        [input(7, 1), input(42, 1), input(42, 3)]
    );
    // Each writer's rows are read past its own number only.
    assert_eq!(
        db.log_inputs(LogKind::Entry, vec![(DeviceId(42), 1)])
            .await
            .unwrap(),
        [input(42, 3)]
    );
    assert_eq!(
        db.log_downloads(LogKind::Entry).await.unwrap(),
        [(DeviceId(7), 1), (DeviceId(42), 1)]
    );
    assert_eq!(
        db.keep_log_inputs(LogKind::Entry, vec![input(42, 2)])
            .await
            .unwrap(),
        BTreeMap::from([(DeviceId(42), 3)])
    );
    assert!(db.log_downloads(LogKind::KeyCopy).await.unwrap().is_empty());
    assert!(db.store_log().await.unwrap().entries.is_empty());
}

#[tokio::test]
async fn cursor_commit_failure_rolls_back_every_input_and_retry_is_idempotent() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.inspect_writer(|sql| {
        sql.fail_at(
            "reject_cursor",
            "BEFORE INSERT ON _coven_log_downloads",
            "cursor failed",
        )
    });
    // A crash before the cursor commits retains neither body.
    assert!(db
        .keep_log_inputs(LogKind::KeyCopy, vec![input(42, 1), input(42, 2)])
        .await
        .is_err());
    assert!(db
        .log_inputs(LogKind::KeyCopy, vec![(DeviceId(42), 0)])
        .await
        .unwrap()
        .is_empty());
    assert!(db.log_downloads(LogKind::KeyCopy).await.unwrap().is_empty());
    db.inspect_writer(|sql| sql.batch("DROP TRIGGER reject_cursor").unwrap());
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            db.keep_log_inputs(LogKind::KeyCopy, vec![input(42, 1)])
                .await
                .unwrap(),
            BTreeMap::from([(DeviceId(42), 1)])
        );
    }
    let mut changed = input(42, 1);
    changed.bytes.push(2);
    assert!(db
        .keep_log_inputs(LogKind::KeyCopy, vec![changed])
        .await
        .is_err());
    assert_eq!(
        db.log_inputs(LogKind::KeyCopy, vec![(DeviceId(42), 0)])
            .await
            .unwrap(),
        [input(42, 1)]
    );
}

#[tokio::test]
async fn a_remembered_writer_has_no_received_input() {
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    db.remember_log_writers(vec![DeviceId(5)]).await.unwrap();
    db.remember_log_writers(vec![DeviceId(5)]).await.unwrap();
    for kind in [LogKind::Entry, LogKind::KeyCopy] {
        assert_eq!(db.log_downloads(kind).await.unwrap(), [(DeviceId(5), 0)]);
        assert!(db
            .log_inputs(kind, vec![(DeviceId(5), 0)])
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn the_return_checkpoint_only_moves_forward_and_survives_reopen() {
    use coven_format::value::StorageTime;
    let store = TestStore::new();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.return_checkpoint().await.unwrap(), None);
    let time = |seconds, nanos| StorageTime::new(seconds, nanos).unwrap();
    assert!(db.save_return_checkpoint(time(10, 5)).await.unwrap());
    assert!(!db.save_return_checkpoint(time(10, 5)).await.unwrap());
    assert!(!db.save_return_checkpoint(time(9, 999)).await.unwrap());
    db.close().await.unwrap();
    let db = store.builder(vec![], vec![]).open().await.unwrap();
    assert_eq!(db.return_checkpoint().await.unwrap(), Some(time(10, 5)));
    assert!(db.save_return_checkpoint(time(10, 6)).await.unwrap());
    assert_eq!(db.return_checkpoint().await.unwrap(), Some(time(10, 6)));
}
