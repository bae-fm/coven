//! Saved restore codes on Android phones, in Block Store (§13.1). Each phone
//! keeps its own entries; a phone set up from another receives a copy.
#![cfg(feature = "test-utils")]

use coven::{
    restore_from_saved_code, BootstrapError, CloudProvider, Coven, CovenBuilder, CovenHandle,
    FixedClock, IdSourceRef, MemoryStorage, OperationOutcome, SavedRestoreCode, SavedRestoreEntry,
    SecretText, SequentialIds, Storage, StorageSetup, StorageSetupError, SyncError,
};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// The entries coven can restore from.
fn readable(entries: Vec<SavedRestoreEntry>) -> Vec<SavedRestoreCode> {
    entries
        .into_iter()
        .map(|entry| match entry {
            SavedRestoreEntry::Readable(saved) => saved,
            SavedRestoreEntry::Unreadable { store_id, reason } => {
                panic!("{store_id:?} is unreadable: {reason:?}")
            }
        })
        .collect()
}

/// Dan's phone, with Household set up on S3.
struct Phone {
    app: Coven,
    handle: CovenHandle,
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    _root: tempfile::TempDir,
}

fn storage(clock: &Arc<FixedClock>) -> Arc<MemoryStorage> {
    Arc::new(
        MemoryStorage::builder()
            .provider(CloudProvider::S3)
            .clock(clock.clone())
            .transfer_limits(1024 * 1024, 65536)
            .build()
            .unwrap(),
    )
}

fn builder(app: &Coven, clock: &Arc<FixedClock>, storage: Arc<MemoryStorage>) -> CovenBuilder {
    app.builder()
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .clock(clock.clone())
        .storage_connector(storage)
}

/// Set up Household on `app`'s phone, returning setup's own result.
/// `app`'s stores are in `root`.
async fn set_up(
    app: Coven,
    root: tempfile::TempDir,
) -> (Phone, Result<StorageSetup, StorageSetupError>) {
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let directory = app.create_closed_store("Household", ids.clone()).await.unwrap();
    let storage = storage(&clock);
    let handle = builder(&app, &clock, storage.clone())
        .id_source(ids)
        .open(directory.id(), |_| ())
        .await
        .unwrap();
    let setup = handle
        .setup_s3_storage(
            storage.config(),
            "Dan's phone",
            "dan-key".into(),
            SecretText::new("secret".into()),
            |_| {},
        )
        .await;
    let phone = Phone { app, handle, storage, clock, _root: root };
    (phone, setup)
}

async fn household() -> Phone {
    let root = tempfile::tempdir().unwrap();
    let (phone, setup) = set_up(Coven::with_block_store(root.path().into()), root).await;
    setup.unwrap();
    phone
}

impl Phone {
    /// Restore the listed store on `new`, a phone set up from this one.
    async fn restore_on(
        &self,
        new: &Coven,
        saved: &SavedRestoreCode,
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        let (_, cancel) = tokio::sync::watch::channel(false);
        restore_from_saved_code(
            builder(new, &self.clock, Arc::new(self.storage.for_device())),
            saved,
            None,
            "Dan's new phone",
            |_| {},
            &cancel,
        )
        .await
    }

    async fn close(self) {
        self.handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_phone_set_up_from_another_lists_the_store_and_restores_it() {
    let phone = household().await;
    // Another phone of Dan's, not set up from this one, has nothing.
    let other_dir = tempfile::tempdir().unwrap();
    let other = phone.app.another_device(other_dir.path().into());
    assert!(readable(other.saved_restore_codes().await.unwrap()).is_empty());

    let new_dir = tempfile::tempdir().unwrap();
    let new = phone.app.set_up_new_device(new_dir.path().into());
    let saved = readable(new.saved_restore_codes().await.unwrap());
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].name, "Household");
    assert_eq!(saved[0].store_id, phone.handle.store_id());
    // The S3 key came across beside the code, so Dan types nothing.
    assert!(saved[0].has_s3_credentials);
    let restored = phone.restore_on(&new, &saved[0]).await.unwrap().unwrap();
    assert_eq!(restored.store_name().await.unwrap(), "Household");
    restored.close().await.unwrap();
    phone.close().await;
}

/// Block Store doesn't sync: a rename reaches only the entry of a phone that
/// applies it.
#[tokio::test]
async fn a_rename_rewrites_only_this_phones_entry() {
    let phone = household().await;
    let tablet_dir = tempfile::tempdir().unwrap();
    let tablet = phone.app.set_up_new_device(tablet_dir.path().into());
    phone.handle.start_sync().await.unwrap();
    let rename = phone.handle.rename_store("Family").await.unwrap().unwrap();
    assert!(matches!(
        phone.handle.operation_outcome(rename).await.unwrap(),
        OperationOutcome::Landed(_)
    ));
    assert_eq!(readable(phone.app.saved_restore_codes().await.unwrap())[0].name, "Family");
    assert_eq!(readable(tablet.saved_restore_codes().await.unwrap())[0].name, "Household");
    phone.close().await;
}

#[tokio::test]
async fn forgetting_deletes_only_this_phones_entry() {
    let phone = household().await;
    let new_dir = tempfile::tempdir().unwrap();
    let new = phone.app.set_up_new_device(new_dir.path().into());
    let store = phone.handle.store_id();
    new.forget_saved_restore_code(store).await.unwrap();
    assert!(new.saved_restore_codes().await.unwrap().is_empty());
    assert_eq!(readable(phone.app.saved_restore_codes().await.unwrap())[0].store_id, store);
    phone.close().await;
}

/// Dan replaced his keys on his phone; his tablet's entry still holds the old
/// code, and a phone set up from the tablet can't restore with it.
#[tokio::test]
async fn a_stale_code_from_another_phone_fails_at_restore() {
    let phone = household().await;
    let tablet_dir = tempfile::tempdir().unwrap();
    let tablet = phone.app.set_up_new_device(tablet_dir.path().into());
    phone.handle.replace_member_keys().await.unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let new = tablet.set_up_new_device(new_dir.path().into());
    let saved = readable(new.saved_restore_codes().await.unwrap());
    let refused = phone.restore_on(&new, &saved[0]).await;
    assert!(
        matches!(&refused, Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { .. }))),
        "{refused:?}"
    );
    // The phone's own entry holds the new code, which restores.
    let current_dir = tempfile::tempdir().unwrap();
    let current = phone.app.set_up_new_device(current_dir.path().into());
    let saved = readable(current.saved_restore_codes().await.unwrap());
    let restored = phone.restore_on(&current, &saved[0]).await.unwrap().unwrap();
    restored.close().await.unwrap();
    phone.close().await;
}

/// The code holds Dan's private keys, so it reaches Google's cloud backup only
/// when that copy is end-to-end encrypted.
#[tokio::test]
async fn a_cloud_restore_receives_only_end_to_end_encrypted_entries() {
    let phone = household().await;
    phone.app.back_up_to_cloud();
    let restored_dir = tempfile::tempdir().unwrap();
    let restored = phone.app.restore_new_device_from_cloud(restored_dir.path().into());
    assert_eq!(readable(restored.saved_restore_codes().await.unwrap()).len(), 1);

    let app_dir = tempfile::tempdir().unwrap();
    let app = Coven::with_block_store(app_dir.path().into());
    app.set_end_to_end_encryption(false);
    let (unlocked, setup) = set_up(app, app_dir).await;
    setup.unwrap();
    unlocked.app.back_up_to_cloud();
    let restored_dir = tempfile::tempdir().unwrap();
    let restored = unlocked.app.restore_new_device_from_cloud(restored_dir.path().into());
    assert!(restored.saved_restore_codes().await.unwrap().is_empty());
    // Direct transfer still carries it.
    let transferred_dir = tempfile::tempdir().unwrap();
    let transferred = unlocked.app.set_up_new_device(transferred_dir.path().into());
    assert_eq!(readable(transferred.saved_restore_codes().await.unwrap()).len(), 1);
    unlocked.close().await;
    phone.close().await;
}

/// Dan saved Household before his phone had a screen lock, so Block Store
/// kept it out of the cloud backup. Once he sets one, the next opening stores
/// the entries again, and the next backup carries them.
#[tokio::test]
async fn opening_after_a_screen_lock_is_set_lets_the_entries_reach_the_cloud() {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::with_block_store(root.path().into());
    app.set_end_to_end_encryption(false);
    let (phone, setup) = set_up(app, root).await;
    setup.unwrap();
    let store = phone.handle.store_id();
    phone.handle.close().await.unwrap();

    phone.app.set_end_to_end_encryption(true);
    phone.app.back_up_to_cloud();
    let restored_dir = tempfile::tempdir().unwrap();
    assert!(phone
        .app
        .restore_new_device_from_cloud(restored_dir.path().into())
        .saved_restore_codes()
        .await
        .unwrap()
        .is_empty());

    let reopened =
        builder(&phone.app, &phone.clock, phone.storage.clone()).open(store, |_| ()).await.unwrap();
    phone.app.back_up_to_cloud();
    let restored = readable(
        phone
            .app
            .restore_new_device_from_cloud(restored_dir.path().into())
            .saved_restore_codes()
            .await
            .unwrap(),
    );
    assert_eq!(restored.len(), 1);
    assert!(restored[0].has_s3_credentials);
    reopened.close().await.unwrap();
}
