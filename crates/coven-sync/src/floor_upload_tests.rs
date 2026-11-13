use crate::sequence_sync::tests::*;
use coven_storage::{test_utils::Faults, ObjectPath, ObjectPrefix, Storage};

/// The floor bodies `ana` has in storage, by path.
async fn bodies(group: &Group, ana: &Device) -> Vec<coven_storage::StoredObject> {
    let folder = ObjectPrefix::device_floors(group.store, ana.sync.device);
    group.storage.list(&folder).await.unwrap()
}

/// Whether the journal holds only the deletion of the body the floor names.
async fn only_the_live_body_remains(ana: &Device) -> bool {
    let records = ana.database().operations().await.unwrap();
    records.iter().all(|record| {
        matches!(
            serde_json::from_slice::<super::Retention>(&record.data),
            Ok(super::Retention::LiveBody { .. })
        )
    })
}

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
    let mut written = Vec::new();
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
        assert!(floor.size <= group.storage.single_request_limit(), "the pointer is one request");
        assert_ne!(previous.as_ref(), Some(&floor));
        let stored = bodies(&group, &ana).await;
        let body = stored.iter().max_by_key(|body| body.path.floor_body_id()).unwrap();
        assert!(body.size > group.storage.single_request_limit(), "the body goes up in parts");
        written.push(body.path.clone());
        // The body the floor names stays; the one it superseded is gone.
        assert_eq!(stored.len(), 1, "{stored:?}");
        assert!(group
            .storage
            .status(&ObjectPath::slot(group.store, covered.try_into().unwrap()))
            .await
            .unwrap()
            .is_none());
        assert!(only_the_live_body_remains(&ana).await);
        previous = Some(floor);
    }
    assert_ne!(written[0], written[1], "each floor takes a body number of its own");
    // A delayed create at the name of the body that was deleted lands there
    // again. Nothing reads it: readers follow the pointer.
    group.storage.create(&written[0], b"a delayed create").await.unwrap();
    let floor = crate::sequence_sync::sequence_reload::tests::stored_floor(&ana).await;
    assert_eq!(floor.body().1.get(), written[1].floor_body_id().unwrap().1.get());
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
    let before_revision = before.revision.clone();
    ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
    ana.storage.set_faults(Faults { drop_part: true, ..Faults::none() }).await;
    ana.sync.pass().await.unwrap();
    assert_eq!(group.storage.status(&path).await.unwrap(), Some(before));
    let records = ana.database().operations().await.unwrap();
    let record = records.iter().find(|record| super::is_floor_write(record)).unwrap();
    let super::Retention::Floor(artifact) = serde_json::from_slice(&record.data).unwrap() else {
        panic!("upload lost its artifact");
    };
    assert_eq!(artifact.capture.first, coven_merge::Slot::FIRST);
    let bytes = ana
        .sync
        .artifact_file(&artifact.capture.name)
        .unwrap()
        .open_reader()
        .unwrap()
        .read_at(0, artifact.size as usize)
        .unwrap();
    let body = artifact.capture.body_path(group.store, ana.sync.device);
    let operation = record.id;
    let ana = group.reopen(ana).await;
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert_eq!(group.storage.read(&body).await.unwrap(), bytes);
    assert_ne!(group.storage.status(&path).await.unwrap().unwrap().revision, before_revision);
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
        for fault in [0, 1, 2, 3] {
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
                    lose_reply_at: if fault == 3 {
                        [ObjectPath::floor(group.store)].into()
                    } else {
                        Default::default()
                    },
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
            if fault < 2 {
                assert_eq!(group.storage.status(&path).await.unwrap(), before);
            }
            let records = ana.database().operations().await.unwrap();
            let record = records.iter().find(|record| super::is_floor_write(record)).unwrap();
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
            let body = artifact.capture.body_path(group.store, ana.sync.device);
            assert_eq!(group.storage.read(&body).await.unwrap(), bytes);
            assert_ne!(group.storage.status(&path).await.unwrap(), before);
            assert!(group.storage.status(&covered).await.unwrap().is_none());
            assert!(ana.database().operation(operation).await.unwrap().is_none());
            assert!(ana.sync.artifact_file(&artifact.capture.name).unwrap().open_reader().is_err());
            ana.database().close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn a_failed_body_deletion_keeps_the_record_and_prevents_deletion() {
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
    // The pointer's replacement is refused, as when another device's pointer
    // won the race, and the device's own body then cannot be deleted.
    let bodies = (1..40)
        .map(|number| {
            let body =
                ObjectPath::floor_body(group.store, ana.sync.device, number.try_into().unwrap());
            (body, coven_storage::StorageFailure::PermissionDenied)
        })
        .collect();
    ana.storage
        .set_faults(Faults { refuse_replacements: 1, delete_failures: bodies, ..Faults::none() })
        .await;
    ana.sync.pass().await.unwrap();
    assert!(group.storage.status(&covered).await.unwrap().is_some());
    let records = ana.database().operations().await.unwrap();
    let record = records.iter().find(|record| super::is_floor_write(record)).unwrap();
    let super::Retention::Floor(artifact) = serde_json::from_slice(&record.data).unwrap() else {
        panic!("cleanup lost its upload");
    };
    let operation = record.id;
    assert!(artifact.session.is_some());
    let ana = group.reopen(ana).await;
    ana.storage.set_faults(Faults::none()).await;
    ana.sync.retry_pending_operation(operation).await.unwrap();
    assert!(group.storage.status(&covered).await.unwrap().is_none());
    assert!(ana.database().operation(operation).await.unwrap().is_none());
    ana.database().close().await.unwrap();
}
