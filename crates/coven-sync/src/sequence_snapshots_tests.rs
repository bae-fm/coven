use crate::sequence_sync::tests::*;
use coven_storage::Storage;

#[tokio::test]
async fn a_snapshot_is_uploaded_before_its_counted_announcement() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "recorded").await;
    ana.sync.pass().await.unwrap();
    let covered = ana.sync.database.applied_slot().await.unwrap();
    let announced = ana
        .sync
        .snapshot(coven_merge::Audience::Store)
        .await
        .unwrap();
    assert_eq!(announced.0, covered + 1);
    ben.sync.pass().await.unwrap();
    let session = ben.sync.session.lock().await;
    let announcement = &session.history.announcements[&announced];
    assert_eq!(announcement.covers, covered);
    let path = crate::snapshot_object::path(group.store, &announcement.snapshot).unwrap();
    assert!(group.storage.status(&path).await.unwrap().is_some());
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn encoded_part_growth_requests_a_snapshot_and_an_idle_pass_reuses_its_size() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let title = "x".repeat(1_048_577);
    ana.sync
        .database
        .write(move |sql| {
            sql.execute("INSERT INTO notes VALUES('large',?1)", [title])?;
            Ok(())
        })
        .await
        .unwrap();
    ana.sync.pass().await.unwrap();
    let session = ana.sync.session.lock().await;
    let (_, announcement) = session
        .history
        .announcements
        .last_key_value()
        .expect("growth requests a snapshot");
    assert_eq!(announcement.covers, 3);
    drop(session);
    assert_idle_work(&ana).await;
    ana.sync.database.close().await.unwrap();
}

#[tokio::test]
async fn aged_history_requests_missing_coverage_below_the_growth_threshold() {
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    insert(&ana.sync.database, "old", "retention needs this prefix").await;
    ana.sync.pass().await.unwrap();
    assert!(ana
        .sync
        .session
        .lock()
        .await
        .history
        .announcements
        .is_empty());
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync
            .session
            .lock()
            .await
            .history
            .announcements
            .last_key_value()
            .unwrap()
            .1
            .covers,
        3
    );
    ana.sync.database.close().await.unwrap();
}
