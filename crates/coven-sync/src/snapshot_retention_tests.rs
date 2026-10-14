use super::*;
use coven_format::sealed_snapshot::SnapshotObjectPrefix;

pub(super) async fn post(device: &Device, storage: &Arc<MemoryStorage>) {
    assert!(crate::DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.custody.clone(),
        device.identity.clone(),
        device.clock.clone(),
        device.send.clone(),
    )
    .post_positions()
    .await
    .unwrap());
}

#[tokio::test]
async fn unsigned_positions_cannot_authorize_log_deletion() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let other = member(2);
    a.add(&other, MemberRole::Member).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    post(&a, &storage).await;
    let path = ObjectPath::positions(store(1), a.device().await);
    let bytes = storage.read(&path).await.unwrap();
    let unsigned = coven_format::sealed_single::SingleChunkObject::decode(&bytes)
        .unwrap()
        .signed_bytes()
        .unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&unsigned);
    let mut forged = unsigned.clone();
    forged.extend_from_slice(
        other
            .keys()
            .sign_object(path.as_str(), &hash.finish())
            .as_bytes(),
    );
    for damaged in [unsigned, forged] {
        storage.replace(&path, &damaged).await.unwrap();
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            1
        );
    }
    storage.replace(&path, &bytes).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn every_active_device_must_post_before_covered_logs_are_deleted() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    a.sync().await;
    b.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    b.sync.reload_from_snapshots().await.unwrap();
    c.sync.reload_from_snapshots().await.unwrap();
    post(&a, &storage).await;
    post(&b, &storage).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    post(&c, &storage).await;
    // Every device has read it, but only its uploader owns the deletion.
    b.sync.run_retention().await.unwrap();
    assert_eq!(
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len(),
        1
    );
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_removed_devices_member_can_delete_its_covered_logs() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    b.sync.reload_from_snapshots().await.unwrap();
    b.sync
        .make_and_upload_entry(StoreChange::RemoveDevice {
            device: a.device().await,
        })
        .await
        .unwrap();
    post(&b, &storage).await;
    b.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn removed_member_logs_belong_to_the_store_owner_and_stay_on_google_drive() {
    for google in [false, true] {
        let storage = if google {
            Arc::new(
                MemoryStorage::builder()
                    .provider(coven_storage::CloudProvider::GoogleDrive)
                    .build()
                    .unwrap(),
            )
        } else {
            snapshot_storage()
        };
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = device(storage.clone(), 2, member(2), store(1)).await;
        b.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        let mut c = device(storage.clone(), 3, member(3), store(1)).await;
        c.reopen(storage.clone(), notes_tables(), notes_migrations())
            .await;
        a.add(&b.member, MemberRole::Member).await;
        a.add(&c.member, MemberRole::Admin).await;
        add_device(&mut b).await;
        add_device(&mut c).await;
        a.sync().await;
        write_rows(&b, 0, 1, 17, Audience::Store).await;
        a.db.apply_downloaded::<crate::PendingRecord>(
            b.db.test_queued_writes().await.unwrap().remove(0).into(),
            std::time::UNIX_EPOCH,
        )
        .await
        .unwrap();
        upload(&b, &storage).await;
        a.sync.write_snapshot(Audience::Store).await.unwrap();
        a.sync
            .make_and_upload_entry(StoreChange::RemoveMember {
                member: b.member.member(),
            })
            .await
            .unwrap();
        c.sync().await;
        for device in [&a, &c] {
            device
                .clock
                .set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
        }
        c.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            1
        );
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs(store(1)))
                .await
                .unwrap()
                .len(),
            usize::from(google)
        );
    }
}

#[tokio::test]
async fn a_file_named_only_by_a_retained_log_write_stays_until_that_log_goes() {
    use coven_database::{CacheFill, FileDatabase, FileDecl, Provenance};
    let storage = snapshot_storage();
    let mut a = device(storage.clone(), 1, member(1), store(1)).await;
    a.db.close().await.unwrap();
    a.db = DatabaseBuilder::new(a.directory.clone())
        .synced_tables(vec![SyncedTable::new("files", RowIdentity::SharedKey)
            .carries_files(FileDecl::new(
                "files",
                Provenance::AppProvided,
                CacheFill::CacheLazy,
            ))])
        .migrations(vec![Migration::sql(
            1,
            "files",
            "CREATE TABLE files(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)",
        )])
        .clock(a.clock.clone())
        .open()
        .await
        .unwrap();
    a.sync.database = a.db.clone();
    a.create(key(1)).await;
    let files = crate::Files::new(
        FileDatabase::new(a.db.clone()),
        a.directory.clone(),
        Some(storage.clone()),
        a.clock.clone(),
        a.send.clone(),
        Arc::new(coven_foundation::id_source::UuidIds),
        crate::TransferLimits::default(),
    );
    a.db.write_with_files::<_, _, _, coven_database::DbError>(
        |batch| {
            batch.put_file("files", "one", vec![37; 65536]);
            Ok(())
        },
        |sql| {
            sql.execute("INSERT INTO files(id) VALUES('one')", [])?;
            Ok(())
        },
    )
    .await
    .unwrap();
    files.retry_uploads_now().await.unwrap();
    let uploaded = a.db.file_ref("files", "one").await.unwrap();
    let path = ObjectPath::file(store(1), uploaded.uploader(), uploaded.id());
    a.db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    upload(&a, &storage).await;
    assert!(a.db.retained_files().await.unwrap().references.is_empty());
    let objects = storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap();
    let start = storage.reads().await.len();
    a.sync.run_retention().await.unwrap();
    let reads = storage.reads().await;
    for object in objects {
        let bytes: u64 = reads[start..]
            .iter()
            .filter(|(path, _, _)| path == &object.path)
            .map(|(_, _, bytes)| bytes)
            .sum();
        assert!(
            bytes <= object.size,
            "retention must reuse the opened log header"
        );
    }
    assert!(storage.read(&path).await.is_ok());
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
        .is_empty());
    assert!(storage
        .list(&ObjectPrefix::files(store(1)))
        .await
        .unwrap()
        .is_empty());
    files.close().await;
}

#[tokio::test]
async fn a_snapshot_keeps_its_key_when_a_removal_is_dropped() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = device(storage.clone(), 2, member(2), store(1)).await;
    b.reopen(storage.clone(), notes_tables(), notes_migrations())
        .await;
    let mut c = device(storage.clone(), 3, member(3), store(1)).await;
    a.add(&b.member, MemberRole::Admin).await;
    a.add(&c.member, MemberRole::Member).await;
    add_device(&mut b).await;
    // Carol's device is registered, so others discover its key copies.
    add_device(&mut c).await;
    a.sync().await;
    // Ben receives Carol's registration, which Ana's write has read.
    b.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    assert_eq!(
        b.db.apply_downloaded::<crate::PendingRecord>(
            a.db.test_queued_writes().await.unwrap().remove(0).into(),
            std::time::UNIX_EPOCH
        )
        .await
        .unwrap(),
        coven_database::ApplyOutcome::Applied
    );
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    write_rows(&b, 1, 1, 17, Audience::Store).await;
    let write = b.db.test_queued_writes().await.unwrap()[0].header.position;
    upload(&b, &storage).await;
    // Ben's write is gone, as retention may delete it once his snapshot below
    // covers it: two covered writes against Ana's one make that snapshot the
    // current newest, which deletion requires (§15).
    let path = ObjectPath::device_log(
        store(1),
        write.device,
        NonZeroU64::new(write.number).unwrap(),
    );
    storage.delete(&path).await.unwrap();
    b.clock.set(UNIX_EPOCH + Duration::from_secs(3));
    b.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: a.member.member(),
        })
        .await
        .unwrap();
    // In Ben's view Ana is out, so his snapshot first rotates the store key,
    // sealing the replacement to himself and Carol only (§11).
    let selected = b.sync.write_snapshot(Audience::Store).await.unwrap();
    let replacement = b.newest(Audience::Store).await;
    assert_ne!(replacement, key(1));
    assert!(find_copy(
        storage.as_ref(),
        Audience::Store,
        replacement,
        &a.member.member()
    )
    .await
    .is_none());
    a.clock.set(UNIX_EPOCH + Duration::from_secs(2));
    a.sync
        .make_and_upload_entry(StoreChange::RemoveMember {
            member: b.member.member(),
        })
        .await
        .unwrap();
    // Ana's earlier removal wins. Carol holds the replacement and shares it
    // with Ana, now a current member who lacks it; the snapshot stays readable.
    c.sync().await;
    assert!(find_copy(
        storage.as_ref(),
        Audience::Store,
        replacement,
        &a.member.member()
    )
    .await
    .is_some());
    a.sync().await;
    let ring = a.custody.read().unwrap().unwrap();
    assert!(ring.store_key(replacement).is_ok());
    // Ben's rotation was authorized in his view, so its key stays a readable
    // snapshot key for Ana after his removal of her drops (§11).
    assert!(crate::key_introductions::needed(&a.log().await)
        .contains_key(&(Audience::Store, replacement)));
    a.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(
        selected_snapshot(&mut a).await.snapshot_id(),
        Some(selected)
    );
    assert_eq!(tables(&a).await, tables(&b).await);
}

/// `newer_snapshot_retention_counterexample` (storelog-data `PendingScans`):
/// Ana's snapshot covers her writes 1 to 4. Ben's covers her 1 to 3 and his
/// own 1 and 2, more writes, so his is the store's newest (§15). `registered`
/// is whether Ana has applied Ben's device registration. Ana's clock is past
/// 30 storage days, so coverage alone decides deletion. Returns Ana, the path
/// of Ben's snapshot and the paths of Ana's writes.
async fn bens_snapshot_is_newest(
    registered: bool,
) -> (Arc<MemoryStorage>, Device, ObjectPath, Vec<ObjectPath>) {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    for n in 0..4 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    let writes = a.db.test_queued_writes().await.unwrap();
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    if registered {
        a.sync().await;
    }
    for write in writes.iter().take(3) {
        b.db.apply_downloaded::<crate::PendingRecord>(write.clone().into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    write_rows(&b, 10, 1, 17, Audience::Store).await;
    write_rows(&b, 11, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    let id = b.sync.write_snapshot(Audience::Store).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    let ben = ObjectPath::snapshot(
        store(1),
        id.audience,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    let writes = writes
        .iter()
        .map(|write| crate::write_seal::path(store(1), write.header.position))
        .collect();
    (storage, a, ben, writes)
}

async fn retained(storage: &MemoryStorage, writes: &[ObjectPath]) -> Vec<bool> {
    let mut retained = Vec::new();
    for path in writes {
        retained.push(storage.status(path).await.unwrap().is_some());
    }
    retained
}

#[tokio::test]
async fn a_newer_format_snapshot_holds_log_retention_like_the_reload() {
    let (storage, mut a, ben, writes) = bens_snapshot_is_newest(true).await;
    let readable = storage.read(&ben).await.unwrap();
    let mut newer = readable.clone();
    newer[2] = 2;
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &newer).await.unwrap();
    // Ana's own snapshot covers her write 4, but Ben's, which she cannot
    // read, may be the newest: retention waits for the update, as her
    // reload does, and the snapshot's record says why.
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, [true; 4]);
    assert_eq!(
        snapshot_record(&a, &ben).await,
        Some(crate::PendingReason::UpdateRequired(
            crate::RequiredUpdate::CovenFormat { version: 2 }
        ))
    );
    // A version that reads Ben's snapshot selects it as the newest; it
    // covers Ana's writes 1 to 3 only, and the reload needs her write 4.
    // Ana's retained bytes are the newer format's, so a new device reads it.
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &readable).await.unwrap();
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(selected_snapshot(&mut c).await, ben);
    assert!(tables(&c).await.iter().any(|(id, _, _)| id == "3"));
}

#[tokio::test]
async fn a_snapshot_waiting_for_its_authors_registration_holds_log_retention() {
    let (storage, mut a, ben, writes) = bens_snapshot_is_newest(false).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, [true; 4]);
    assert_eq!(
        snapshot_record(&a, &ben).await,
        Some(crate::PendingReason::Waits(
            crate::Prerequisite::DeviceRegistration(ben.device().unwrap())
        ))
    );
    // With Ben's registration applied his prefix checks and is the newest.
    a.sync().await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(
        retained(&storage, &writes).await,
        [false, false, false, true]
    );
    assert_eq!(snapshot_record(&a, &ben).await, None);
}

/// The store of storelog-data `RefusedRetention`: Ana's snapshot covers her
/// writes 1 to 10 and Ben's 1 and 2; Ben's covers Ana 1 to 3 and his own 1 to
/// 20, more writes, so his is the store's newest (§15). Ana has applied Ben's
/// registration and her clock is past 30 storage days, so coverage alone
/// decides deletion. Returns Ana, the path of Ben's snapshot and the paths of
/// Ana's writes.
async fn ana_ten_ben_twenty() -> (Arc<MemoryStorage>, Device, ObjectPath, Vec<ObjectPath>) {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    for n in 0..3 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    for write in a.db.test_queued_writes().await.unwrap() {
        b.db.apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    for n in 100..120 {
        write_rows(&b, n, 1, 17, Audience::Store).await;
    }
    for write in b.db.test_queued_writes().await.unwrap().into_iter().take(2) {
        a.db.apply_downloaded::<crate::PendingRecord>(write.into(), std::time::UNIX_EPOCH)
            .await
            .unwrap();
    }
    upload(&b, &storage).await;
    for n in 3..10 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    let writes = a.db.test_queued_writes().await.unwrap();
    assert_eq!(writes.len(), 10);
    upload(&a, &storage).await;
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    let id = b.sync.write_snapshot(Audience::Store).await.unwrap();
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    let ben = ObjectPath::snapshot(
        store(1),
        id.audience,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    let writes = writes
        .iter()
        .map(|write| crate::write_seal::path(store(1), write.header.position))
        .collect();
    (storage, a, ben, writes)
}

/// Ana's writes 1 to 3 go and 4 to 10 stay: Ben's snapshot, the newest,
/// covers her only through 3.
const JUDGED_BY_BENS: [bool; 10] = [
    false, false, false, true, true, true, true, true, true, true,
];

/// A version that loads Ben's snapshot selects it as the newest and reads
/// Ana's write 4, which it does not cover, from her log.
async fn a_passing_version_needs_write_4(storage: &Arc<MemoryStorage>, ben: &ObjectPath) {
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(selected_snapshot(&mut c).await, *ben);
    assert!(tables(&c).await.iter().any(|(id, _, _)| id == "3"));
}

/// `refused_body_retention_counterexample` (storelog-data
/// `RefusedRetention`): Ana's version refuses Ben's body. His verified prefix
/// still makes his snapshot the newest, so retention keeps her write 4,
/// which it leaves out (`retention_ignores_bodies`).
#[tokio::test]
async fn a_refused_body_still_decides_log_retention_by_its_prefix() {
    let (storage, mut a, ben, writes) = ana_ten_ben_twenty().await;
    let readable = storage.read(&ben).await.unwrap();
    storage
        .corrupt_byte(&ben, readable.len() - 1)
        .await
        .unwrap();
    // Ana's reload refuses Ben's body and waits on his snapshot rather than
    // loading her own (storelog-data `missingDecision`).
    let result = a.sync.reload_from_snapshots().await;
    let Err(SyncError::Pending(waiting)) = result else {
        panic!("{result:?}")
    };
    assert_eq!(
        waiting.reason,
        crate::PendingReason::Waits(crate::Prerequisite::Object(ben.clone()))
    );
    assert!(matches!(
        snapshot_record(&a, &ben).await,
        Some(crate::PendingReason::Refused(_))
    ));
    // A body refusal also comes from reading it for file retention, with no
    // reload waiting; end this one so the retention below sees the refusal.
    let crate::PendingSubject::Operation { id, .. } = waiting.subject else {
        panic!("{waiting:?}")
    };
    a.db.finish_operation(id).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, JUDGED_BY_BENS);
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &readable).await.unwrap();
    a_passing_version_needs_write_4(&storage, &ben).await;
}

/// `missing_key_retention_counterexample` (storelog-data `RefusedRetention`):
/// Ben's verified prefix names a key no entry Ana has applied introduces, so
/// she cannot load it, and it still decides her log retention.
#[tokio::test]
async fn a_snapshot_whose_key_is_not_accepted_still_decides_log_retention() {
    let (storage, mut a, ben, writes) = ana_ten_ben_twenty().await;
    let readable = storage.read(&ben).await.unwrap();
    let length = SnapshotObjectPrefix::length(&readable).unwrap();
    let (mut prefix, _) = SnapshotObjectPrefix::decode_signed(&readable[..length + 64]).unwrap();
    prefix.key = key(2);
    let mut unsigned = prefix.encode().unwrap();
    unsigned.extend_from_slice(&readable[length + 64..readable.len() - 64]);
    // Every notes device belongs to member 1, Ben's member.
    let unaccepted = super::signatures::signed_snapshot(&unsigned, &ben, &a.member, &a.member);
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &unaccepted).await.unwrap();
    assert!(
        !crate::key_introductions::needed(&a.log().await).contains_key(&(Audience::Store, key(2)))
    );
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, JUDGED_BY_BENS);
    assert_eq!(snapshot_record(&a, &ben).await, None);
    // Ana's reload waits for that key rather than loading her own snapshot.
    let result = a.sync.reload_from_snapshots().await;
    assert!(
        matches!(&result, Err(SyncError::Pending(waiting)) if waiting.reason
        == crate::PendingReason::KeyUnavailable {
            audience: Audience::Store,
            key: key(2),
        }),
        "{result:?}"
    );
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &readable).await.unwrap();
    a_passing_version_needs_write_4(&storage, &ben).await;
}

/// `refused_prefix_retention_counterexample` (storelog-data
/// `RefusedRetention`): Ana's version refuses the signature on Ben's prefix.
/// A version whose check passes selects his snapshot as the newest and needs
/// her write 4, so while that refusal is listed Ana deletes no write of the
/// store (`refused_prefix_holds`, `holding_safe`). The hold ends once the
/// listing stops naming his snapshot (`holding_without_refusals`).
#[tokio::test]
async fn a_refused_snapshot_prefix_holds_log_retention_while_it_is_listed() {
    let (storage, mut a, ben, writes) = ana_ten_ben_twenty().await;
    let readable = storage.read(&ben).await.unwrap();
    let mut refused = readable.clone();
    refused[SnapshotObjectPrefix::length(&readable).unwrap()] ^= 1;
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &refused).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, [true; 10]);
    assert!(matches!(
        snapshot_record(&a, &ben).await,
        Some(crate::PendingReason::Refused(
            crate::Refusal::Signature { .. }
        ))
    ));
    storage.delete(&ben).await.unwrap();
    storage.create(&ben, &readable).await.unwrap();
    a_passing_version_needs_write_4(&storage, &ben).await;
    storage.delete(&ben).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert_eq!(snapshot_record(&a, &ben).await, None);
    assert_eq!(retained(&storage, &writes).await, [false; 10]);
}

/// §5: proving file absence validates another device's snapshot while holding
/// the database writer. A body stalled in storage must not keep an app write
/// waiting: validation reads only bytes already received.
#[tokio::test]
async fn a_stalled_snapshot_body_does_not_hold_the_database_writer() {
    let storage = snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    write_rows(&b, 0, 10, 20_000, Audience::Store).await;
    upload(&b, &storage).await;
    b.sync.write_snapshot(Audience::Store).await.unwrap();
    a.sync().await;
    let snapshot = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0);
    let at = snapshot.size - 100;
    assert!(at > 2 * coven_format::chunks::CHUNK_SIZE as u64);
    // This device owns an unreferenced upload, so retention must read every
    // retained snapshot's references before deleting it.
    let orphan = ObjectPath::file(
        store(1),
        a.device().await,
        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
    );
    storage.create(&orphan, b"orphan").await.unwrap();
    let (stalled, observed) = tokio::sync::oneshot::channel();
    let (resume, held) = tokio::sync::oneshot::channel();
    storage
        .hold_body_at(snapshot.path.clone(), at, stalled, held)
        .await;
    let db = a.db.clone();
    let app = async move {
        observed.await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            db.write(|sql| {
                sql.execute(
                    "INSERT INTO notes VALUES('local','store','while','downloading')",
                    [],
                )?;
                Ok(())
            }),
        )
        .await
        .expect("an app write waited for a stalled snapshot body")
        .unwrap();
        resume.send(()).unwrap();
    };
    let (retained, ()) = tokio::join!(a.sync.run_retention(), app);
    retained.unwrap();
    assert_eq!(
        storage.read(&orphan).await.unwrap_err().failure(),
        coven_storage::StorageFailure::NotFound
    );
    assert_eq!(
        a.db.read(|sql| Ok(sql.query_row(
            "SELECT count(*) FROM notes WHERE id='local'",
            [],
            |row| row.get::<_, i64>(0)
        )?))
        .await
        .unwrap(),
        1
    );
}
