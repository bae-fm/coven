use crate::sequence_sync::tests::*;
use coven_storage::{ObjectPath, Storage};

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
    let announced = ana.sync.snapshot(coven_merge::Audience::Store).await.unwrap();
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
async fn sealed_slot_growth_requests_a_snapshot_and_an_idle_pass_reuses_its_size() {
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
    let (_, announcement) =
        session.history.announcements.last_key_value().expect("growth requests a snapshot");
    assert_eq!(announcement.covers, 3);
    drop(session);
    // The same pass wrote the floor naming the snapshot; the next pass reads
    // that floor's bytes once.
    ana.sync.pass().await.unwrap();
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
    assert!(ana.sync.session.lock().await.history.announcements.is_empty());
    group.clock.set(
        std::time::UNIX_EPOCH
            + crate::sequence_sync::sequence_retention::GRACE
            + std::time::Duration::from_secs(1),
    );
    ana.sync.pass().await.unwrap();
    assert_eq!(
        ana.sync.session.lock().await.history.announcements.last_key_value().unwrap().1.covers,
        3
    );
    ana.sync.database.close().await.unwrap();
}

/// The sealed bytes of the slots after `after` through `through`, as a new
/// device reads them.
async fn sealed(group: &Group, after: u64, through: u64) -> u64 {
    let mut bytes = 0;
    for slot in after + 1..=through {
        let path = ObjectPath::slot(group.store, slot.try_into().unwrap());
        bytes += group.storage.status(&path).await.unwrap().unwrap().size;
    }
    bytes
}

/// The newest announcement's slot, coverage and snapshot.
async fn newest(
    device: &Device,
) -> Option<(coven_merge::Slot, u64, coven_format::admin::SnapshotId)> {
    let session = device.sync.session.lock().await;
    let (slot, announcement) = session.history.announcements.last_key_value()?;
    Some((*slot, announcement.covers, announcement.snapshot.clone()))
}

/// Snapshots follow the slots written since the last one: k × S sealed
/// bytes, S being the last snapshot's size, however large S is. A store
/// whose snapshot is past 8 MiB, where a cap once stood, still waits for
/// k × S, so snapshot work stays a fixed share of what is written.
#[tokio::test]
async fn a_large_snapshot_is_followed_after_k_times_its_size_in_slots() {
    use super::SNAPSHOT_GAP_PERCENT;
    const OLD_CAP: u64 = 8 * 1024 * 1024;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    const NOTE: usize = 1024 * 1024;
    let mut written = 0usize;
    let write = |ana: &Device, index: usize| {
        let database = ana.sync.database.clone();
        async move {
            let title = "x".repeat(NOTE);
            database
                .write(move |sql| {
                    sql.execute("INSERT INTO notes VALUES(?1,?2)", (index.to_string(), title))?;
                    Ok(())
                })
                .await
                .unwrap();
        }
    };
    // Grow the store until its newest snapshot is well past the old cap.
    let (slot, covers, size) = loop {
        write(&ana, written).await;
        written += 1;
        ana.sync.pass().await.unwrap();
        if let Some((slot, covers, id)) = newest(&ana).await {
            let path = crate::snapshot_object::path(group.store, &id).unwrap();
            let size = group.storage.status(&path).await.unwrap().unwrap().size;
            if size > OLD_CAP + 2 * NOTE as u64 {
                break (slot, covers, size);
            }
        }
        assert!(written < 64, "no snapshot grew past the old cap");
    };
    let gap = size * SNAPSHOT_GAP_PERCENT / 100;
    let mut before;
    let next = loop {
        before = sealed(&group, covers, ana.sync.database.applied_slot().await.unwrap()).await;
        assert!(before < gap, "no snapshot after {before} bytes of slots");
        write(&ana, written).await;
        written += 1;
        ana.sync.pass().await.unwrap();
        match newest(&ana).await {
            Some((newer, next, _)) if newer != slot => break next,
            _ => {}
        }
    };
    let growth = sealed(&group, covers, next).await;
    assert!(
        growth >= gap,
        "a snapshot after {growth} bytes of slots, {before} before its pass, the last one {size} bytes"
    );
    ana.sync.database.close().await.unwrap();
}

/// A young store of small writes. Each write seals to more bytes than its
/// parts hold: its header, roots and signatures, and its slot's envelope. A
/// new device reads those sealed bytes, so they request the first snapshot:
/// this store's slots reach 1 MiB while its parts are still below it.
#[tokio::test]
async fn sealed_slot_bytes_request_the_first_snapshot_while_the_parts_are_below_it() {
    use super::FIRST_SNAPSHOT_BYTES;
    let group = Group::new();
    let ana = group.device(1).await;
    group.create(&ana).await;
    let mut written = 0;
    let mut before;
    let covers = loop {
        before = sealed(&group, 0, ana.sync.database.applied_slot().await.unwrap()).await;
        assert!(before < FIRST_SNAPSHOT_BYTES, "no snapshot after {before} bytes of slots");
        for _ in 0..250 {
            let id = written.to_string();
            ana.sync
                .database
                .write(move |sql| {
                    sql.execute("INSERT INTO notes VALUES(?1,?2)", (id, "x".repeat(900)))?;
                    Ok(())
                })
                .await
                .unwrap();
            written += 1;
        }
        ana.sync.pass().await.unwrap();
        if let Some((_, covers, _)) = newest(&ana).await {
            break covers;
        }
    };
    let covered = sealed(&group, 0, covers).await;
    assert!(
        covered >= FIRST_SNAPSHOT_BYTES,
        "a first snapshot covering {covered} bytes of slots, {before} before its pass"
    );
    // The rows themselves, about 0.9 KiB each, stay well below 1 MiB.
    assert!(written * 1000 < FIRST_SNAPSHOT_BYTES, "{written} writes");
    ana.sync.database.close().await.unwrap();
}
