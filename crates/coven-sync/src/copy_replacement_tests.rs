//! A provider-damaged key copy is replaced by its publisher; readers skip
//! it once its writer's kept entry marks it (§11, `CopyReplacement`).

use super::*;
use crate::device_log_sync::tests::{group, sql, STORE_ID};
use crate::store_log_sync::tests::Device;
use coven_database::EntryOutcome;
use coven_storage::{test_utils::MemoryStorage, Storage};

/// Seal the group's store key to its member at this device's next number and
/// confirm it, as sharing does.
async fn publish_copy(device: &Device, storage: &Arc<MemoryStorage>) -> ObjectPath {
    let ring = device.custody.read().unwrap().unwrap();
    let key = ring
        .store_key(KeyId(uuid::Uuid::from_u128(1)))
        .unwrap()
        .clone();
    let (id, sealing) = (
        device.member.member(),
        device.member.keys().public_keys().sealing,
    );
    let copy = device
        .db
        .prepare_key_upload(move |path| {
            crate::store_log_keys::store_copy(path, &key, &id, &sealing)
        })
        .await
        .unwrap();
    crate::store_log_object::upload_keys(
        STORE_ID,
        Some(storage.as_ref()),
        &device.db,
        &device.send,
    )
    .await
    .unwrap();
    copy.path(STORE_ID)
}

/// The provider replaces a stored object's bytes; §2 assumes it does not.
async fn corrupt(storage: &MemoryStorage, path: &ObjectPath) {
    storage.delete(path).await.unwrap();
    storage.create(path, b"not a sealed key").await.unwrap();
}

async fn replacements(device: &Device) -> Vec<coven_database::OperationRecord> {
    device
        .db
        .operations()
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.kind == "replace-key-copy")
        .collect()
}

async fn finish(device: &mut Device, id: coven_database::OperationId) {
    loop {
        match device.step(id).await.unwrap() {
            crate::operations::Progress::Advanced => (),
            crate::operations::Progress::Finished(crate::operations::Output::Unit) => return,
            _ => panic!("a copy replacement advances to its kept entry"),
        }
    }
}

fn prefix(bytes: &[u8]) -> coven_format::sealed_key::KeyCopyPrefix {
    coven_format::sealed_key::SealedKey::decode(bytes)
        .unwrap()
        .prefix()
        .clone()
}

/// The provider corrupts a key copy Ana's phone published before any device
/// read it. Her laptop's first phase waits on it, as on any damaged copy. Her
/// phone's confirmed bytes stand in for it, so its own phase completes and
/// keeps its id and unsent write (`own_record_matches`); it reseals the same
/// key to the same recipient under its next number and records the
/// replacement. The laptop then skips the damaged copy, reads the
/// replacement, and completes (`marked_is_damage`, `complete_once_marked`).
#[tokio::test]
async fn the_publisher_replaces_a_damaged_copy_and_readers_complete() {
    let storage = crate::device_log_sync::tests::storage();
    let mut devices = group(storage.clone(), 2).await;
    let mut phone = devices.remove(0);
    let laptop = devices.remove(0);
    let path = publish_copy(&phone, &storage).await;
    let original = storage.read(&path).await.unwrap();
    corrupt(&storage, &path).await;
    assert!(matches!(
        laptop.sync.scan_store_log_once(true).await,
        Err(SyncError::Damaged(damage)) if damage.path == path.as_str()
    ));
    sql(&phone.db, "INSERT INTO notes VALUES('one','title','body')").await;
    assert!(phone.sync.scan_store_log_once(true).await.unwrap());
    assert!(phone.sync.scan_store_log_once(true).await.unwrap());
    let scheduled = replacements(&phone).await;
    assert_eq!(scheduled.len(), 1, "one replacement however often it scans");
    assert_eq!(phone.send.stopped(), None);
    assert_eq!(phone.directory.device_reset().unwrap(), None);
    finish(&mut phone, scheduled[0].id).await;
    let log = phone.log().await;
    let entry = log.entries.last().unwrap();
    let (writer, damaged) = path.key_copy_position().unwrap();
    let StoreChange::ReplaceKeyCopy {
        damaged: named,
        replacement,
    } = entry.entry.change
    else {
        panic!("the replacement is recorded: {:?}", entry.entry.change);
    };
    assert_eq!(
        (entry.entry.position.device, named),
        (writer, damaged.get())
    );
    assert_eq!(
        log.replay.entries[&entry.entry.position],
        EntryOutcome::Kept
    );
    let resealed = storage
        .read(&ObjectPath::key_copy(
            STORE_ID,
            writer,
            replacement.try_into().unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(prefix(&resealed), prefix(&original));
    assert_ne!(resealed, original, "fresh randomness at a new path");
    // Every reader now skips the damaged copy, the publisher included.
    assert!(laptop.sync.scan_store_log_once(true).await.unwrap());
    assert!(phone.sync.scan_store_log_once(true).await.unwrap());
    assert!(replacements(&phone).await.is_empty());
    let observed = laptop.send.observation().unwrap();
    assert!(!observed.iter().any(|copy| copy.path == path));
    assert!(observed.iter().any(|copy| copy.path.key_copy_position()
        == Some((writer, replacement.try_into().unwrap()))
        && copy.prefix == prefix(&original)));
    let attempted = phone
        .db
        .read_oldest_upload(|upload| Ok::<_, coven_database::DbError>(upload.keys.is_some()))
        .await
        .unwrap();
    assert_eq!(attempted, Some(false), "the unsent write stays queued");
}

/// A replacement entry names numbers in its own writer's log only. Ana's
/// laptop records the replacement of its own copy 1; the provider then
/// corrupts her phone's copy 1, which the entry does not name, so a third
/// device still waits on it and the laptop, with no confirmed bytes for
/// another writer's copy, starts no replacement of it.
#[tokio::test]
async fn another_device_cannot_mark_a_copy() {
    let storage = crate::device_log_sync::tests::storage();
    let mut devices = group(storage.clone(), 3).await;
    let phone = devices.remove(0);
    let mut laptop = devices.remove(0);
    let tablet = devices.remove(0);
    let phone_copy = publish_copy(&phone, &storage).await;
    let (_, number) = phone_copy.key_copy_position().unwrap();
    // The laptop's copy takes the same number in its own log.
    let laptop_device = laptop.device().await;
    while laptop.db.next_key_copy_number().await.unwrap() <= number {
        publish_copy(&laptop, &storage).await;
    }
    laptop.catch_up().await;
    let next = laptop.db.next_key_copy_number().await.unwrap();
    laptop
        .sync
        .make_and_upload_entry(StoreChange::ReplaceKeyCopy {
            damaged: number.get(),
            replacement: next.get(),
        })
        .await
        .unwrap();
    assert_eq!(
        replaced_copies(&laptop.log().await),
        [(laptop_device, number)].into()
    );
    corrupt(&storage, &phone_copy).await;
    assert!(matches!(
        tablet.sync.scan_store_log_once(true).await,
        Err(SyncError::Damaged(damage)) if damage.path == phone_copy.as_str()
    ));
    // The laptop read the phone's copy before the damage and keeps it.
    assert!(laptop.sync.scan_store_log_once(true).await.unwrap());
    assert!(replacements(&laptop).await.is_empty());
}
