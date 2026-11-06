use crate::sequence_sync::tests::*;
use coven_storage::{test_utils::Faults, ObjectPath, Storage};

#[tokio::test]
async fn floors_above_the_request_limit_create_and_replace_atomically() {
    let mut group = Group::new();
    group.storage = coven_storage::test_utils::MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(256, 128)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let floor_path = ObjectPath::floor(group.store);
    let mut previous = None;
    for (index, id) in ["first", "second"].into_iter().enumerate() {
        insert(ana.database(), id, "retained in the snapshot").await;
        ana.sync.pass().await.unwrap();
        let covered = ana.database().applied_slot().await.unwrap();
        ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
        group.clock.set(
            std::time::UNIX_EPOCH
                + (super::GRACE + std::time::Duration::from_secs(1)) * (index as u32 + 1),
        );
        ana.sync.pass().await.unwrap();
        let floor = group.storage.status(&floor_path).await.unwrap().unwrap();
        assert!(floor.size > group.storage.single_request_limit());
        assert_ne!(previous.as_ref(), Some(&floor));
        assert!(group
            .storage
            .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
            .await
            .unwrap()
            .is_none());
        assert!(ana.database().operations().await.unwrap().is_empty());
        previous = Some(floor);
    }
    ana.database().close().await.unwrap();
}

/// A floor write that moves only the names keeps `first`, so an unchanged
/// floor with that same `first` does not show the write landed: the retry
/// publishes it.
#[tokio::test]
async fn an_interrupted_floor_write_that_moves_only_the_names_publishes_on_retry() {
    let mut group = Group::new();
    group.storage = coven_storage::test_utils::MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(256, 128)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let path = ObjectPath::floor(group.store);
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.sync.pass().await.unwrap();
    let before =
        group.storage.status(&path).await.unwrap().expect("a fresh snapshot is named at once");
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.storage.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
    ana.sync.pass().await.unwrap();
    assert_eq!(group.storage.status(&path).await.unwrap(), Some(before));
    let records = ana.database().operations().await.unwrap();
    let record = records.iter().find(|record| record.kind == "sequence-retention").unwrap();
    let super::Retention::Floor(artifact) = serde_json::from_slice(&record.data).unwrap() else {
        panic!("upload lost its artifact");
    };
    assert_eq!(artifact.capture.first, 1);
    let bytes = ana
        .sync
        .artifact_file(&artifact.capture.name)
        .unwrap()
        .open_reader()
        .unwrap()
        .read_at(0, artifact.size as usize)
        .unwrap();
    let operation = record.id;
    let ana = group.reopen(ana).await;
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert_eq!(group.storage.read(&path).await.unwrap(), bytes);
    assert!(group
        .storage
        .status(&ObjectPath::slot(group.store, 1.try_into().unwrap()))
        .await
        .unwrap()
        .is_some());
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}

#[tokio::test]
async fn interrupted_floor_parts_and_publication_keep_their_journal_across_reopening() {
    for replacement in [false, true] {
        for fault in [0, 1, 2] {
            let mut group = Group::new();
            group.storage = coven_storage::test_utils::MemoryStorage::builder()
                .clock(group.clock.clone())
                .transfer_limits(256, 128)
                .build()
                .unwrap();
            let ana = group.device(1).await;
            group.create(&ana).await;
            if replacement {
                ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
                group
                    .clock
                    .set(std::time::UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
                ana.sync.pass().await.unwrap();
            }
            insert(ana.database(), "held", "covered by the proposed floor").await;
            ana.sync.pass().await.unwrap();
            let covered = ObjectPath::slot(
                group.store,
                ana.database().applied_slot().await.unwrap().try_into().unwrap(),
            );
            ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
            group.clock.set(
                std::time::UNIX_EPOCH + (super::GRACE + std::time::Duration::from_secs(1)) * 2,
            );
            let path = ObjectPath::floor(group.store);
            let before = group.storage.status(&path).await.unwrap();
            ana.storage
                .set_faults(Faults {
                    drop_part: fault == 0,
                    lose_part_reply: fault == 1,
                    lose_completion_reply: fault == 2,
                    ..Faults::none()
                })
                .await;
            // Retention alone, so the fault hits the floor's upload: a whole
            // pass would first take a snapshot of the slots past grace.
            let mut session = ana.sync.session.lock().await;
            ana.sync
                .retain_sequence(&mut session, &mut group.ring.clone(), &ana.identity)
                .await
                .unwrap();
            drop(session);
            assert!(group.storage.status(&covered).await.unwrap().is_some());
            if fault != 2 {
                assert_eq!(group.storage.status(&path).await.unwrap(), before);
            }
            let records = ana.database().operations().await.unwrap();
            let record = records.iter().find(|record| record.kind == "sequence-retention").unwrap();
            let super::Retention::Floor(artifact) = serde_json::from_slice(&record.data).unwrap()
            else {
                panic!("upload lost its artifact");
            };
            assert!(artifact.session.is_some());
            let bytes = ana
                .sync
                .artifact_file(&artifact.capture.name)
                .unwrap()
                .open_reader()
                .unwrap()
                .read_at(0, artifact.size as usize)
                .unwrap();
            let operation = record.id;
            let ana = group.reopen(ana).await;
            ana.sync.retry_pending_operation(operation).await.unwrap();
            assert_eq!(group.storage.read(&path).await.unwrap(), bytes);
            assert!(group.storage.status(&covered).await.unwrap().is_none());
            assert!(ana.database().operation(operation).await.unwrap().is_none());
            assert!(ana.sync.artifact_file(&artifact.capture.name).unwrap().open_reader().is_err());
            ana.database().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_failed_floor_abort_keeps_the_record_and_prevents_deletion() {
    let mut group = Group::new();
    group.storage = coven_storage::test_utils::MemoryStorage::builder()
        .clock(group.clock.clone())
        .transfer_limits(256, 128)
        .build()
        .unwrap();
    let ana = group.device(1).await;
    group.create(&ana).await;
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + super::GRACE + std::time::Duration::from_secs(1));
    ana.sync.pass().await.unwrap();
    insert(ana.database(), "held", "covered").await;
    ana.sync.pass().await.unwrap();
    let covered = ObjectPath::slot(
        group.store,
        ana.database().applied_slot().await.unwrap().try_into().unwrap(),
    );
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    group.clock.set(std::time::UNIX_EPOCH + (super::GRACE + std::time::Duration::from_secs(1)) * 2);
    ana.storage
        .set_faults(Faults { refuse_replacements: 1, abort_failures: 1, ..Faults::none() })
        .await;
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&covered).await.unwrap().is_some());
    let records = ana.database().operations().await.unwrap();
    let record = records.iter().find(|record| record.kind == "sequence-retention").unwrap();
    let super::Retention::Floor(artifact) = serde_json::from_slice(&record.data).unwrap() else {
        panic!("cleanup lost its upload");
    };
    let operation = record.id;
    assert!(artifact.session.is_some());
    let ana = group.reopen(ana).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(group.storage.status(&covered).await.unwrap().is_none());
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}
