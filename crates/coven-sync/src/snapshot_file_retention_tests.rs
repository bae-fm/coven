use super::*;
use crate::StoreLogSync;
use coven_crypto::{custody::InMemoryCustody, MemberKeys, StoreKey, StoreKeyring};
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::{ObjectPath, ObjectPrefix, Storage};

async fn sync(f: &Fixture) -> (StoreLogSync, MemberKeys) {
    let member = MemberKeys::generate().unwrap();
    let key = KeyId(uuid::Uuid::from_u128(500));
    let mut sync = StoreLogSync::new(
        f.storage.clone(),
        f.database.clone(),
        Arc::new(InMemoryCustody::new(StoreKeyring::new(
            StoreKey::generate(key).unwrap(),
        ))),
        Arc::new(InMemoryCustody::new(member.clone())),
        f.clock.clone(),
        f.ids.clone(),
        f.directory.clone(),
    );
    sync.make_and_upload_entry(StoreChange::CreateStore {
        access: coven_format::MemberAccess::ProviderAccount("owner".into()),
        store: f.directory.id(),
        name: "Files".into(),
        admin: MemberPublicKeys {
            signing: member.member_id(),
            sealing: member.sealing_public_key(),
        },
        key,
        device_name: "Uploader".into(),
    })
    .await
    .unwrap();
    (sync, member)
}

fn path(file: &FileRef) -> ObjectPath {
    let uploaded = file.uploaded().unwrap().unwrap();
    ObjectPath::file(uploaded.device, uploaded.id)
}

#[tokio::test]
async fn an_excluded_snapshot_prevents_proving_file_absence() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, author) = sync(&f).await;
    let file = f.uploaded("old", vec![31; CHUNK]).await;
    sync.write_snapshot(Audience::Store).await.unwrap();
    f.database
        .write(|sql| {
            sql.execute("DELETE FROM files", [])?;
            Ok(())
        })
        .await
        .unwrap();
    for write in f.database.test_queued_writes().await.unwrap() {
        f.database
            .test_acknowledge_write(write.header.position)
            .await
            .unwrap();
    }
    let object = f
        .storage
        .list(&ObjectPrefix::snapshots())
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
    assert!(f.storage.read(&path(&file)).await.is_ok());
    f.close().await;
}

#[tokio::test]
async fn file_references_survive_in_waiting_writes_and_kept_snapshots() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _) = sync(&f).await;
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
    assert!(f.storage.read(&path(&old)).await.is_ok());
    assert!(f.storage.read(&path(&kept)).await.is_ok());
    // Simulate successful device-log publication, whose storage copy is tested
    // separately: the queue no longer protects the old row's upload reference.
    for write in f.database.test_queued_writes().await.unwrap() {
        f.database
            .test_acknowledge_write(write.header.position)
            .await
            .unwrap();
    }
    sync.run_retention().await.unwrap();
    assert!(f.storage.read(&path(&old)).await.is_ok());
    sync.write_snapshot(Audience::Store).await.unwrap();
    sync.run_retention().await.unwrap();
    let objects = f.storage.list(&ObjectPrefix::files()).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].path, path(&kept));
    f.close().await;
}

#[tokio::test]
async fn deleting_an_unused_publication_retires_its_queue() {
    let f = Fixture::new(Provenance::AppProvided, CacheFill::CacheLazy).await;
    let (mut sync, _) = sync(&f).await;
    super::uploads::unused_upload(&f).await;
    sync.run_retention().await.unwrap();
    assert!(f
        .storage
        .list(&ObjectPrefix::files())
        .await
        .unwrap()
        .is_empty());
    let queue = f.files.inner.database.uploads().await.unwrap();
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue[0].file,
        f.database.file_ref("files", "old").await.unwrap()
    );
    assert!(!queue[0].stored && !queue[0].unused);
    f.close().await;
}
