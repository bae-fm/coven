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
    a.write_snapshot(Audience::Store).await.unwrap();
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
    a.write_snapshot(Audience::Store).await.unwrap();
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
async fn a_file_named_only_by_a_retained_log_write_stays_until_that_log_goes() {
    use coven_database::{CacheFill, FileDatabase, FileDecl, Provenance};
    let (storage, storage_clock) = timed_snapshot_storage();
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
    a.writes = a.writes();
    a.create(key(1)).await;
    let files = crate::Files::new(
        FileDatabase::new(a.db.clone()),
        a.directory.clone(),
        Some(storage.clone()),
        a.send.clone(),
        a.waits.clone(),
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
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    if !a.send.can_send() {
        a.catch_up().await;
    }
    a.write_snapshot(Audience::Store).await.unwrap();
    a.sync.run_retention().await.unwrap();
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
            coven_database::test_utils::published(coven_format::value::StorageTime::UNIX_EPOCH)
        )
        .await
        .unwrap(),
        coven_database::ApplyOutcome::Applied
    );
    // Ana's snapshot is sent once her write is stored (§15).
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
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
    let selected = b.write_snapshot(Audience::Store).await.unwrap();
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

/// Storage and its clock, Ana, the path of Ben's snapshot and Ana's writes,
/// thirty-one storage days after the writes were published.
type Fixture = (
    Arc<MemoryStorage>,
    Arc<FixedClock>,
    Device,
    ObjectPath,
    Vec<ObjectPath>,
);

/// Replace Ben's snapshot with `bytes` published at its original storage
/// time, as if he had published them: whether a prefix counts depends on
/// its storage time (§15).
async fn republish(storage: &MemoryStorage, clock: &FixedClock, path: &ObjectPath, bytes: &[u8]) {
    let now = coven_foundation::clock::Clock::now(clock);
    let published = storage.status(path).await.unwrap().unwrap().stored_at;
    clock.set(UNIX_EPOCH + published.elapsed_since_epoch());
    storage.delete(path).await.unwrap();
    storage.create(path, bytes).await.unwrap();
    clock.set(now);
}

/// Prepare `device`'s requested store snapshot after a fresh catch-up,
/// without sending it.
async fn prepare_requested(device: &mut Device) -> crate::OperationId {
    let data = Data::Snapshots(crate::snapshot_data::SnapshotTask {
        job: crate::snapshot_data::SnapshotJob::Write {
            audience: Audience::Store,
            device: device.device().await,
            trigger: crate::snapshot_data::SnapshotTrigger::Requested,
            attempt: None,
        },
        temporary: Vec::new(),
    });
    let id = device
        .db
        .start_operation(data.new_operation("coven").unwrap())
        .await
        .unwrap();
    let _pass = device.caught_up_pass().await;
    let record = device.operation(id).await;
    assert!(matches!(
        device
            .sync
            .operation_step(&record, Data::read(&record).unwrap())
            .await
            .unwrap(),
        Progress::Advanced
    ));
    id
}

/// Send a prepared snapshot and finish its task, with no store-log scan;
/// returns its identity.
async fn send_prepared(
    device: &mut Device,
    id: crate::OperationId,
) -> coven_format::store_log::SnapshotId {
    let record = device.operation(id).await;
    let Data::Snapshots(task) = Data::read(&record).unwrap() else {
        panic!("a snapshot task")
    };
    let published = task.published().unwrap();
    while let Some(record) = device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)
    {
        assert!(!matches!(
            device
                .sync
                .operation_step(&record, Data::read(&record).unwrap())
                .await
                .unwrap(),
            Progress::Waiting(_)
        ));
    }
    published
}

/// `newer_snapshot_retention_counterexample` (storelog-data `PendingScans`):
/// Ana's snapshot covers her writes 1 to 4. Ben's covers her 1 to 3 and his
/// own 1 and 2, more writes, so his is the store's newest (§15). Each is
/// prepared before the other lands: Ana captures her write 4 before it is
/// stored, Ben's catch-up misses it, and Ana's lands first.
/// `registered` is whether Ana has applied Ben's device registration.
/// Storage time is past 30 days after the writes, so coverage alone decides
/// deletion.
async fn bens_snapshot_is_newest(registered: bool) -> Fixture {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    for n in 0..3 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    upload(&a, &storage).await;
    write_rows(&a, 3, 1, 17, Audience::Store).await;
    let device = a.device().await;
    let writes: Vec<_> = (1..=4)
        .map(|number| crate::write_seal::path(store(1), coven_merge::WriteId { device, number }))
        .collect();
    let anas = prepare_requested(&mut a).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    if registered {
        a.sync().await;
    }
    write_rows(&b, 10, 1, 17, Audience::Store).await;
    write_rows(&b, 11, 1, 17, Audience::Store).await;
    upload(&b, &storage).await;
    let bens = prepare_requested(&mut b).await;
    upload(&a, &storage).await;
    send_prepared(&mut a, anas).await;
    let id = send_prepared(&mut b, bens).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    let ben = ObjectPath::snapshot(
        store(1),
        id.audience,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    (storage, storage_clock, a, ben, writes)
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
    let (storage, clock, mut a, ben, writes) = bens_snapshot_is_newest(true).await;
    let readable = storage.read(&ben).await.unwrap();
    let mut newer = readable.clone();
    newer[2] = 2;
    republish(&storage, &clock, &ben, &newer).await;
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
    // covers Ana's writes 1 to 3 only, so a new device reads her write 4
    // from her log, which retention kept.
    republish(&storage, &clock, &ben, &readable).await;
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(selected_snapshot(&mut c).await, ben);
    c.writes.download_writes().await.unwrap();
    assert!(tables(&c).await.iter().any(|(id, _, _)| id == "3"));
}

#[tokio::test]
async fn a_snapshot_waiting_for_its_authors_registration_holds_log_retention() {
    let (storage, _clock, mut a, ben, writes) = bens_snapshot_is_newest(false).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, [true; 4]);
    assert_eq!(
        snapshot_record(&a, &ben).await,
        // Its signed coverage names Ben's registration, which Ana has not
        // applied: the signer is looked up in that view (D9).
        Some(crate::PendingReason::Waits(crate::Prerequisite::Object(
            crate::store_log_object::path(
                store(1),
                coven_format::value::EntryId {
                    device: ben.device().unwrap(),
                    number: 1
                }
            )
        )))
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
/// registration and storage time is past 30 days after the writes, so
/// coverage alone decides deletion.
async fn ana_ten_ben_twenty() -> Fixture {
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    for n in 0..3 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    upload(&a, &storage).await;
    for n in 100..120 {
        write_rows(&b, n, 1, 17, Audience::Store).await;
    }
    upload(&b, &storage).await;
    // Each snapshot is prepared before the other lands, and Ana's lands
    // first. Ana's catch-up reads only Ben's writes 1 and 2, his later ones
    // not yet listed, and she captures her writes 4 to 10 before they are
    // stored; Ben's catch-up then reads only her writes 1 to 3.
    let ben_device = b.device().await;
    let mut hidden = Vec::new();
    for object in storage
        .list(&ObjectPrefix::device_logs(store(1)))
        .await
        .unwrap()
    {
        let id = object.path.write_id().unwrap();
        if id.device == ben_device && id.number > 2 {
            hidden.push((
                object.path.clone(),
                storage.read(&object.path).await.unwrap(),
            ));
            storage.delete(&object.path).await.unwrap();
        }
    }
    for n in 3..10 {
        write_rows(&a, n, 1, 17, Audience::Store).await;
    }
    let device = a.device().await;
    let writes: Vec<_> = (1..=10)
        .map(|number| crate::write_seal::path(store(1), coven_merge::WriteId { device, number }))
        .collect();
    let anas = prepare_requested(&mut a).await;
    for (path, bytes) in hidden {
        storage.create(&path, &bytes).await.unwrap();
    }
    let bens = prepare_requested(&mut b).await;
    upload(&a, &storage).await;
    send_prepared(&mut a, anas).await;
    let id = send_prepared(&mut b, bens).await;
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    let ben = ObjectPath::snapshot(
        store(1),
        id.audience,
        id.device,
        NonZeroU64::new(id.number).unwrap(),
    );
    (storage, storage_clock, a, ben, writes)
}

/// Ana's writes 1 to 3 go and 4 to 10 stay: Ben's snapshot, the newest,
/// covers her only through 3.
const JUDGED_BY_BENS: [bool; 10] = [
    false, false, false, true, true, true, true, true, true, true,
];

/// A version that loads Ben's snapshot selects it as the newest; the reload
/// stops at its target, and Ana's write 4, which it does not cover, comes
/// from her log.
async fn a_passing_version_needs_write_4(storage: &Arc<MemoryStorage>, ben: &ObjectPath) {
    let mut c = notes_device(storage.clone(), 3).await;
    add_device(&mut c).await;
    c.sync.reload_from_snapshots().await.unwrap();
    assert_eq!(selected_snapshot(&mut c).await, *ben);
    c.writes.download_writes().await.unwrap();
    assert!(tables(&c).await.iter().any(|(id, _, _)| id == "3"));
}

/// `refused_body_retention_counterexample` (storelog-data
/// `RefusedRetention`): Ana's version refuses Ben's body. His verified prefix
/// still makes his snapshot the newest, so retention keeps her write 4,
/// which it leaves out (`retention_ignores_bodies`).
#[tokio::test]
async fn a_refused_body_still_decides_log_retention_by_its_prefix() {
    let (storage, clock, mut a, ben, writes) = ana_ten_ben_twenty().await;
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
    republish(&storage, &clock, &ben, &readable).await;
    a_passing_version_needs_write_4(&storage, &ben).await;
}

/// `missing_key_retention_counterexample` (storelog-data `RefusedRetention`):
/// Ben's verified prefix names a key no entry Ana has applied introduces, so
/// she cannot load it, and it still decides her log retention.
#[tokio::test]
async fn a_snapshot_whose_key_is_not_accepted_still_decides_log_retention() {
    let (storage, clock, mut a, ben, writes) = ana_ten_ben_twenty().await;
    let readable = storage.read(&ben).await.unwrap();
    let length = SnapshotObjectPrefix::length(&readable).unwrap();
    let (mut prefix, _) = SnapshotObjectPrefix::decode_signed(&readable[..length + 64]).unwrap();
    prefix.key = key(2);
    let mut unsigned = prefix.encode().unwrap();
    unsigned.extend_from_slice(&readable[length + 64..readable.len() - 64]);
    // Every notes device belongs to member 1, Ben's member.
    let unaccepted = super::signatures::signed_snapshot(&unsigned, &ben, &a.member, &a.member);
    republish(&storage, &clock, &ben, &unaccepted).await;
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
    republish(&storage, &clock, &ben, &readable).await;
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
    let (storage, clock, mut a, ben, writes) = ana_ten_ben_twenty().await;
    let readable = storage.read(&ben).await.unwrap();
    let mut refused = readable.clone();
    refused[SnapshotObjectPrefix::length(&readable).unwrap()] ^= 1;
    republish(&storage, &clock, &ben, &refused).await;
    a.sync.run_retention().await.unwrap();
    assert_eq!(retained(&storage, &writes).await, [true; 10]);
    assert!(matches!(
        snapshot_record(&a, &ben).await,
        Some(crate::PendingReason::Refused(
            crate::Refusal::Signature { .. }
        ))
    ));
    republish(&storage, &clock, &ben, &readable).await;
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
    b.write_snapshot(Audience::Store).await.unwrap();
    a.sync().await;
    let snapshot = storage
        .list(&ObjectPrefix::snapshots(store(1)))
        .await
        .unwrap()
        .remove(0);
    let at = snapshot.size - 100;
    assert!(at > 2 * coven_format::chunks::CHUNK_SIZE as u64);
    // This device owns an upload whose last reference went, so retention
    // must read every retained snapshot's references before deleting it.
    let orphan = ObjectPath::file(
        store(1),
        a.device().await,
        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
    );
    storage.create(&orphan, b"orphan").await.unwrap();
    a.db.add_file_candidates(std::collections::BTreeSet::from([(
        orphan.device().unwrap(),
        orphan.file_id().unwrap(),
    )]))
    .await
    .unwrap();
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

/// §15: Ana's phone clock jumps ahead a year. A covered log Ben never read is
/// still one storage day old, so the jump cannot release it; a fresh
/// observation of storage time decides. Thirty storage days do.
#[tokio::test]
async fn a_device_clock_jump_never_ages_a_log() {
    let (storage, storage_clock) = timed_snapshot_storage();
    let day = |n: u64| UNIX_EPOCH + Duration::from_secs(n * 86400);
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    let mut b = notes_device(storage.clone(), 2).await;
    add_device(&mut b).await;
    a.sync().await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    a.write_snapshot(Audience::Store).await.unwrap();
    let logs = || async {
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
            .len()
    };
    assert_eq!(logs().await, 1);
    storage_clock.set(day(1));
    a.clock.set(day(366));
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs().await, 1);
    storage_clock.set(day(30));
    a.sync.run_retention().await.unwrap();
    assert_eq!(logs().await, 0);
}

/// §18.1: publication ends at confirmed upload. A failed deletion neither
/// fails nor holds the snapshot operation: it is the path's own
/// `Retention { path }` record, the object stays, and the next evaluation
/// derives the deletion again from the catalog and clears the record.
#[tokio::test]
async fn a_confirmed_snapshot_finishes_despite_a_failed_deletion() {
    use coven_storage::{
        test_utils::{Faults, MemoryRequest},
        StorageFailure,
    };
    let (storage, storage_clock) = timed_snapshot_storage();
    let mut a = notes_device(storage.clone(), 1).await;
    a.create(key(1)).await;
    write_rows(&a, 0, 1, 17, Audience::Store).await;
    upload(&a, &storage).await;
    let logs = || async {
        storage
            .list(&ObjectPrefix::device_logs(store(1)))
            .await
            .unwrap()
    };
    let log = logs().await.remove(0).path;
    // Day 31: the covered log may go by its storage age, but deleting it fails.
    a.clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage_clock.set(UNIX_EPOCH + Duration::from_secs(31 * 86400));
    storage
        .set_faults(Faults {
            delete_failures: [(log.clone(), StorageFailure::PermissionDenied)].into(),
            ..Faults::none()
        })
        .await;
    if !a.send.can_send() {
        a.catch_up().await;
    }
    let before = storage.request_trace().len();
    a.write_snapshot(Audience::Store).await.unwrap();
    assert!(!storage.request_trace()[before..]
        .iter()
        .any(|request| matches!(request, MemoryRequest::Delete(_))));
    assert!(a.db.operations().await.unwrap().is_empty());
    let retention = || async {
        a.db.pending::<crate::PendingRecord>()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| matches!(r.subject, crate::PendingSubject::Retention { .. }))
            .map(|r| (r.subject, r.reason))
            .collect::<Vec<_>>()
    };
    for _ in 0..2 {
        let error = a.sync.run_retention().await.unwrap_err();
        assert!(
            matches!(&error, SyncError::Storage(e) if e.failure() == StorageFailure::PermissionDenied),
            "{error:?}"
        );
        assert!(a.db.operations().await.unwrap().is_empty());
        assert_eq!(
            retention().await,
            vec![(
                crate::PendingSubject::Retention { path: log.clone() },
                crate::PendingReason::Storage {
                    provider: storage.config().provider(),
                    failure: StorageFailure::PermissionDenied,
                }
            )]
        );
        assert_eq!(logs().await.len(), 1);
    }
    storage.set_faults(Faults::none()).await;
    a.sync.run_retention().await.unwrap();
    assert!(logs().await.is_empty());
    assert!(retention().await.is_empty());
}
