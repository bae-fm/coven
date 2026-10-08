use super::*;

pub(super) async fn post(device: &Device, storage: &Arc<MemoryStorage>) {
    assert!(crate::DeviceLogSync::new(
        storage.clone(),
        device.db.clone(),
        device.custody.clone(),
        Arc::new(InMemoryCustody::new(device.member.clone())),
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
    let path = ObjectPath::positions(a.device().await);
    let bytes = storage.read(&path).await.unwrap();
    let unsigned = coven_format::sealed_single::SingleChunkObject::decode(&bytes)
        .unwrap()
        .signed_bytes()
        .unwrap();
    let mut hash = coven_crypto::ObjectHasher::new();
    hash.update(&unsigned);
    let mut forged = unsigned.clone();
    forged.extend_from_slice(other.sign_object(path.as_str(), &hash.finish()).as_bytes());
    for damaged in [unsigned, forged] {
        storage.replace(&path, &damaged).await.unwrap();
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs())
                .await
                .unwrap()
                .len(),
            1
        );
    }
    storage.replace(&path, &bytes).await.unwrap();
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs())
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
            .list(&ObjectPrefix::device_logs())
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
            .list(&ObjectPrefix::device_logs())
            .await
            .unwrap()
            .len(),
        1
    );
    a.sync.run_retention().await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs())
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
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn removed_member_logs_belong_to_the_store_owner_and_stay_on_google_drive() {
    for google in [false, true] {
        let storage = if google {
            Arc::new(
                MemoryStorage::new(
                    StorageConfig::GoogleDrive {
                        folder_id: "store".into(),
                    },
                    Arc::new(FixedClock::new(UNIX_EPOCH)),
                )
                .unwrap(),
            )
        } else {
            snapshot_storage()
        };
        let mut a = notes_device(storage.clone(), 1).await;
        a.create(key(1)).await;
        let mut b = device(storage.clone(), 2, member(2), store(1)).await;
        b.db.close().await.unwrap();
        b.db = open_notes(b.directory.clone(), b.clock.clone()).await;
        b.sync.database = b.db.clone();
        let mut c = device(storage.clone(), 3, member(3), store(1)).await;
        c.db.close().await.unwrap();
        c.db = open_notes(c.directory.clone(), c.clock.clone()).await;
        c.sync.database = c.db.clone();
        a.add(&b.member, MemberRole::Member).await;
        a.add(&c.member, MemberRole::Admin).await;
        add_device(&mut b).await;
        add_device(&mut c).await;
        a.sync().await;
        write_rows(&b, 0, 1, 17, Audience::Store).await;
        a.db.apply_downloaded(b.db.test_queued_writes().await.unwrap().remove(0).into())
            .await
            .unwrap();
        upload(&b, &storage).await;
        a.sync.write_snapshot(Audience::Store).await.unwrap();
        a.sync
            .make_and_upload_entry(StoreChange::RemoveMember {
                member: b.member.member_id(),
                key: key(2),
                circle_keys: Vec::new(),
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
                .list(&ObjectPrefix::device_logs())
                .await
                .unwrap()
                .len(),
            1
        );
        a.sync.run_retention().await.unwrap();
        assert_eq!(
            storage
                .list(&ObjectPrefix::device_logs())
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
    let uploaded =
        a.db.file_ref("files", "one")
            .await
            .unwrap()
            .uploaded()
            .unwrap()
            .unwrap();
    let path = ObjectPath::file(uploaded.device, uploaded.id);
    a.db.write(|sql| {
        sql.execute("DELETE FROM files", [])?;
        Ok(())
    })
    .await
    .unwrap();
    upload(&a, &storage).await;
    assert!(a.db.retained_files().await.unwrap().references.is_empty());
    let objects = storage.list(&ObjectPrefix::device_logs()).await.unwrap();
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
    a.sync.write_snapshot(Audience::Store).await.unwrap();
    assert!(storage
        .list(&ObjectPrefix::device_logs())
        .await
        .unwrap()
        .is_empty());
    assert!(storage
        .list(&ObjectPrefix::files())
        .await
        .unwrap()
        .is_empty());
    files.close().await;
}
