use crate::sequence_sync::tests::*;
use crate::*;
use coven_database::StagedDamage;
use coven_merge::Audience;

/// Ana writes a row and snapshots the store; Ben reads up to it.
async fn snapshotted() -> (Group, Device, Device) {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    insert(&ana.sync.database, "a", "in the snapshot").await;
    ana.sync.pass().await.unwrap();
    ana.sync.snapshot(Audience::Store).await.unwrap();
    ben.sync.pass().await.unwrap();
    (group, ana, ben)
}

/// Ben's reload fails here, refusing nothing, and the next one loads the
/// snapshot.
async fn reloads_after_local_damage(ana: &Device, ben: &Device) {
    let result = ben.sync.reload_from_snapshot().await;
    assert!(
        matches!(&result, Err(SyncError::Database(coven_database::DbError::DamagedDatabase))),
        "{result:?}"
    );
    ben.sync.reload_from_snapshot().await.unwrap();
    assert_eq!(rows(&ben.sync.database).await, rows(&ana.sync.database).await);
    assert_eq!(rows(&ben.sync.database).await.len(), 1);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}

/// Ben's staged copy of Ana's snapshot changes on his disk before his load
/// reads it. The snapshot is what Ana wrote, so it is not refused (§19.1).
#[tokio::test]
async fn a_staged_snapshot_damaged_before_its_load_is_not_refused() {
    let (_group, ana, ben) = snapshotted().await;
    ben.sync.database.damage_next_staged_read().await.unwrap();
    reloads_after_local_damage(&ana, &ben).await;
}

/// The same while his staging checks the writes it staged (§19.1).
#[tokio::test]
async fn a_staged_snapshot_damaged_while_it_stages_is_not_refused() {
    let (_group, ana, ben) = snapshotted().await;
    ben.sync.database.damage_staged_finishes(StagedDamage::Writes, 1).await.unwrap();
    reloads_after_local_damage(&ana, &ben).await;
}

/// Both stagings of the snapshot hold the same failing records, as every
/// staging of a snapshot whose own records fail does: the refusal stands,
/// as the staging's check or as the load's (§19.1).
#[tokio::test]
async fn a_snapshot_whose_stagings_agree_is_refused() {
    for damage in [StagedDamage::Writes, StagedDamage::Record] {
        let (_group, ana, ben) = snapshotted().await;
        ben.sync.database.damage_staged_finishes(damage, 2).await.unwrap();
        let result = ben.sync.reload_from_snapshot().await;
        let refused = match damage {
            StagedDamage::Writes => matches!(
                &result,
                Err(SyncError::Damaged(DamagedObject {
                    failure: Refusal::InvalidWrite { .. },
                    ..
                }))
            ),
            StagedDamage::Record => matches!(
                &result,
                Err(SyncError::Damaged(DamagedObject { failure: Refusal::Parse { .. }, .. }))
            ),
        };
        assert!(refused, "{damage:?}: {result:?}");
        ana.sync.database.close().await.unwrap();
        ben.sync.database.close().await.unwrap();
    }
}

/// The stored snapshot's last byte differs from its announcement: Ben's
/// read of it and a second read agree on that, which refuses it. That second
/// read is the confirmation, so no second staging streams it again (§19.1).
#[tokio::test]
async fn a_refusal_a_second_read_confirmed_is_not_staged_again() {
    use coven_storage::{test_utils::MemoryRequest, Storage};
    let (group, ana, ben) = snapshotted().await;
    let history = ben.sync.session.lock().await.history.clone();
    let announced = history.announcements.values().find(|a| a.snapshot.audience == Audience::Store);
    let path = crate::snapshot_object::path(group.store, &announced.unwrap().snapshot).unwrap();
    let size = group.storage.status(&path).await.unwrap().unwrap().size;
    group.storage.corrupt_byte(&path, size as usize - 1).await.unwrap();
    let before = ben.storage.request_trace().len();
    let result = ben.sync.reload_from_snapshot().await;
    assert!(
        matches!(
            &result,
            Err(SyncError::Damaged(DamagedObject { failure: Refusal::WrongIdentity { .. }, .. }))
        ),
        "{result:?}"
    );
    let streams = ben.storage.request_trace()[before..]
        .iter()
        .filter(|request| {
            matches!(request, MemoryRequest::Read(read, range)
            if *read == path && range.is_some_and(|range| range.end() == size))
        })
        .count();
    // The body's stream, and the second read that confirms its digest.
    assert_eq!(streams, 2, "{:?}", &ben.storage.request_trace()[before..]);
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}
