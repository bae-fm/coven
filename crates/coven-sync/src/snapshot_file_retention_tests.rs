use super::*;
use crate::StoreLogSync;
use coven_crypto::{
    custody::{InMemoryCustody, KeySession},
    MemberKeys, StoreKey, StoreKeyring,
};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, Storage};

async fn sync(f: &Fixture) -> (StoreLogSync, MemberKeys, crate::DeviceLogSync) {
    let member = MemberKeys::generate().unwrap();
    let key = KeyId(uuid::Uuid::from_u128(500));
    let store_keys = Arc::new(
        KeySession::store(Arc::new(InMemoryCustody::new(StoreKeyring::new(
            StoreKey::generate(key).unwrap(),
        ))))
        .unwrap(),
    );
    let member_keys =
        Arc::new(KeySession::member(Arc::new(InMemoryCustody::new(member.clone()))).unwrap());
    let writes = crate::DeviceLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        store_keys.clone(),
        member_keys.clone(),
        f.clock.clone(),
        f.send.clone(),
    );
    let mut sync = StoreLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        store_keys,
        member_keys,
        f.clock.clone(),
        f.send.clone(),
        f.ids.clone(),
        f.directory.clone(),
    );
    sync.make_and_upload_entry(StoreChange::CreateStore {
        access: coven_format::MemberAccess::ProviderAccount("owner".into()),
        store: f.directory.id(),
        name: f.directory.settings().unwrap().name,
        admin: MemberPublicKeys {
            signing: member.member_id(),
            sealing: member.sealing_public_key(),
        },
        key,
        key_hash: sync.test_key(coven_merge::Audience::Store, key),
        device_name: "Uploader".into(),
    })
    .await
    .unwrap();
    (sync, member, writes)
}

fn path(f: &Fixture, file: &FileRef) -> ObjectPath {
    f.path(file)
}

#[tokio::test]
async fn an_excluded_snapshot_prevents_proving_file_absence() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, author, mut writes) = sync(&f).await;
    let file = f.uploaded("old", vec![31; CHUNK]).await;
    sync.write_snapshot(Audience::Store).await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    writes.upload_writes().await.unwrap();
    let object = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap()
        .remove(0);
    let mut bytes = f.storage.read(&object.path).await.unwrap();
    use coven_format::sealed_snapshot::SnapshotObjectPrefix;
    let end = SnapshotObjectPrefix::length(&bytes).unwrap();
    let mut prefix = SnapshotObjectPrefix::decode(&bytes[..end]).unwrap();
    prefix.key = KeyId(uuid::Uuid::from_u128(501));
    let clear = prefix.encode().unwrap();
    let signature = author.sign_prefix(object.path.as_str(), &clear);
    bytes.splice(..end, clear);
    bytes[end..end + 64].copy_from_slice(signature.as_bytes());
    f.storage.delete(&object.path).await.unwrap();
    f.storage.create(&object.path, &bytes).await.unwrap();
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &file)).await.is_ok());
    f.close().await;
}

#[tokio::test]
async fn file_references_survive_in_waiting_writes_and_kept_snapshots() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _, mut writes) = sync(&f).await;
    let old = f.uploaded("old", vec![31; CHUNK]).await;
    let kept = f.uploaded("kept", vec![32; CHUNK]).await;
    sync.write_snapshot(Audience::Store).await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files WHERE id='old'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    assert!(f.storage.read(&path(&f, &kept)).await.is_ok());
    writes.upload_writes().await.unwrap();
    assert!(writes.post_positions().await.unwrap());
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    sync.write_snapshot(Audience::Store).await.unwrap();
    sync.run_retention().await.unwrap();
    let objects = f
        .storage
        .list(&ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].path, path(&f, &kept));
    f.close().await;
}

#[tokio::test]
async fn a_replaced_files_upload_stays_while_its_attaching_write_names_it() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _, mut writes) = sync(&f).await;
    let (old, new) = super::uploads::retargeted_upload(&f).await;
    // The waiting attaching write names A's fixed path.
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&f, &old)).await.is_ok());
    writes.upload_writes().await.unwrap();
    assert!(writes.post_positions().await.unwrap());
    sync.write_snapshot(Audience::Store).await.unwrap();
    sync.run_retention().await.unwrap();
    // Nothing retained names A any more; B still waits in the queue.
    assert!(f
        .storage
        .list(&ObjectPrefix::files(f.directory.id()))
        .await
        .unwrap()
        .is_empty());
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].file, new);
    f.close().await;
}

#[tokio::test]
async fn locally_protected_files_reuse_the_checked_snapshot_prefix() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _, _) = sync(&f).await;
    f.uploaded("kept", vec![32; CHUNK]).await;
    sync.write_snapshot(Audience::Store).await.unwrap();
    let object = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap()
        .remove(0);
    let start = f.storage.reads().await.len();
    sync.run_retention().await.unwrap();
    let reads = f.storage.reads().await;
    assert_eq!(
        reads[start..]
            .iter()
            .filter(|(path, _, _)| path == &object.path)
            .map(|(_, _, bytes)| bytes)
            .sum::<u64>(),
        0
    );
    f.close().await;
}

#[tokio::test]
async fn a_damaged_snapshot_cannot_prove_file_absence() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _, _) = sync(&f).await;
    let kept = f.uploaded("kept", vec![32; CHUNK]).await;
    sync.write_snapshot(Audience::Store).await.unwrap();
    let orphan = ObjectPath::file(
        f.directory.id(),
        kept.uploader(),
        coven_foundation::id_source::FileId(uuid::Uuid::from_u128(999)),
    );
    f.storage.create(&orphan, b"orphan").await.unwrap();
    let snapshot = f
        .storage
        .list(&ObjectPrefix::snapshots(f.directory.id()))
        .await
        .unwrap()
        .remove(0);
    f.storage
        .corrupt_byte(&snapshot.path, snapshot.size as usize - 1)
        .await
        .unwrap();
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&orphan).await.is_ok());
    f.close().await;
}
