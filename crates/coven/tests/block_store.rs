//! Saved restore codes on Android phones, in Block Store (§13.1). Each phone
//! keeps its own entries; a phone set up from another receives a copy.
#![cfg(feature = "test-utils")]

use coven::{
    restore_from_saved_code, BootstrapError, CloudProvider, CovenBuilder, CovenHandle, FixedClock,
    IdSourceRef, MemoryStorage, SavedRestoreCode, SavedRestoreEntry, SecretText, SequentialIds,
    Storage, StorageSetup, StorageSetupError, StoreLayout, SyncError, TestCoven,
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
    app: TestCoven,
    handle: CovenHandle,
    storage: Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    root: tempfile::TempDir,
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

fn builder(
    app: &TestCoven,
    layout: StoreLayout,
    clock: &Arc<FixedClock>,
    storage: Arc<MemoryStorage>,
) -> CovenBuilder {
    app.builder(layout)
        .synced_tables(Vec::new())
        .migrations(Vec::new())
        .clock(clock.clone())
        .storage_connector(storage)
}

/// Set up Household on `app`'s phone, returning setup's own result.
async fn set_up(app: TestCoven) -> (Phone, Result<StorageSetup, StorageSetupError>) {
    let root = tempfile::tempdir().unwrap();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let ids: IdSourceRef = Arc::new(SequentialIds::new());
    let phone_dir = root.path().join("phone");
    std::fs::create_dir(&phone_dir).unwrap();
    let layout = StoreLayout::new(phone_dir);
    let directory = app
        .create_closed_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let storage = storage(&clock);
    let handle = builder(&app, layout, &clock, storage.clone())
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
        )
        .await;
    let phone = Phone {
        app,
        handle,
        storage,
        clock,
        root,
    };
    (phone, setup)
}

async fn household() -> Phone {
    let (phone, setup) = set_up(TestCoven::with_block_store()).await;
    setup.unwrap();
    phone
}

impl Phone {
    /// Restore the listed store on `new`, a phone set up from this one, in
    /// its own `folder`.
    async fn restore_on(
        &self,
        new: &TestCoven,
        folder: &str,
        saved: &SavedRestoreCode,
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        let (_, cancel) = tokio::sync::watch::channel(false);
        let dir = self.root.path().join(folder);
        std::fs::create_dir(&dir).unwrap();
        let layout = StoreLayout::new(dir);
        restore_from_saved_code(
            builder(
                new,
                layout,
                &self.clock,
                Arc::new(self.storage.for_device()),
            ),
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
    assert!(readable(
        phone
            .app
            .another_device()
            .saved_restore_codes()
            .await
            .unwrap()
    )
    .is_empty());

    let new = phone.app.set_up_new_device();
    let saved = readable(new.saved_restore_codes().await.unwrap());
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].name, "Household");
    assert_eq!(saved[0].store_id, phone.handle.store_id());
    // The S3 key came across beside the code, so Dan types nothing.
    assert!(saved[0].has_s3_credentials);
    let restored = phone
        .restore_on(&new, "new", &saved[0])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.store_name().await.unwrap(), "Household");
    restored.close().await.unwrap();
    phone.close().await;
}

/// Block Store doesn't sync: a rename reaches only the entry of a phone that
/// applies it.
#[tokio::test]
async fn a_rename_rewrites_only_this_phones_entry() {
    let phone = household().await;
    let tablet = phone.app.set_up_new_device();
    phone.handle.rename_store("Family").await.unwrap();
    assert_eq!(
        readable(phone.app.saved_restore_codes().await.unwrap())[0].name,
        "Family"
    );
    assert_eq!(
        readable(tablet.saved_restore_codes().await.unwrap())[0].name,
        "Household"
    );
    phone.close().await;
}

#[tokio::test]
async fn forgetting_deletes_only_this_phones_entry() {
    let phone = household().await;
    let new = phone.app.set_up_new_device();
    let store = phone.handle.store_id();
    new.forget_saved_restore_code(store).await.unwrap();
    assert!(new.saved_restore_codes().await.unwrap().is_empty());
    assert_eq!(
        readable(phone.app.saved_restore_codes().await.unwrap())[0].store_id,
        store
    );
    phone.close().await;
}

/// Dan replaced his keys on his phone; his tablet's entry still holds the old
/// code, and a phone set up from the tablet can't restore with it.
#[tokio::test]
async fn a_stale_code_from_another_phone_fails_at_restore() {
    let phone = household().await;
    let tablet = phone.app.set_up_new_device();
    phone.handle.replace_member_keys().await.unwrap();
    let new = tablet.set_up_new_device();
    let saved = readable(new.saved_restore_codes().await.unwrap());
    let refused = phone.restore_on(&new, "new", &saved[0]).await;
    assert!(
        matches!(
            &refused,
            Err(BootstrapError::Sync(SyncError::MemberKeysReplaced { .. }))
        ),
        "{refused:?}"
    );
    // The phone's own entry holds the new code, which restores.
    let current = phone.app.set_up_new_device();
    let saved = readable(current.saved_restore_codes().await.unwrap());
    let restored = phone
        .restore_on(&current, "current", &saved[0])
        .await
        .unwrap()
        .unwrap();
    restored.close().await.unwrap();
    phone.close().await;
}

/// The code holds Dan's private keys, so it reaches Google's cloud backup only
/// when that copy is end-to-end encrypted.
#[tokio::test]
async fn a_cloud_restore_receives_only_end_to_end_encrypted_entries() {
    let phone = household().await;
    phone.app.back_up_to_cloud();
    let restored = phone.app.restore_new_device_from_cloud();
    assert_eq!(
        readable(restored.saved_restore_codes().await.unwrap()).len(),
        1
    );

    let app = TestCoven::with_block_store();
    app.set_end_to_end_encryption(false);
    let (unlocked, setup) = set_up(app).await;
    setup.unwrap();
    unlocked.app.back_up_to_cloud();
    let restored = unlocked.app.restore_new_device_from_cloud();
    assert!(restored.saved_restore_codes().await.unwrap().is_empty());
    // Direct transfer still carries it.
    let transferred = unlocked.app.set_up_new_device();
    assert_eq!(
        readable(transferred.saved_restore_codes().await.unwrap()).len(),
        1
    );
    unlocked.close().await;
    phone.close().await;
}

/// Dan saved Household before his phone had a screen lock, so Block Store
/// kept it out of the cloud backup. Once he sets one, the next opening stores
/// the entries again, and the next backup carries them.
#[tokio::test]
async fn opening_after_a_screen_lock_is_set_lets_the_entries_reach_the_cloud() {
    let app = TestCoven::with_block_store();
    app.set_end_to_end_encryption(false);
    let (phone, setup) = set_up(app).await;
    setup.unwrap();
    let store = phone.handle.store_id();
    phone.handle.close().await.unwrap();

    phone.app.set_end_to_end_encryption(true);
    phone.app.back_up_to_cloud();
    assert!(phone
        .app
        .restore_new_device_from_cloud()
        .saved_restore_codes()
        .await
        .unwrap()
        .is_empty());

    let reopened = builder(
        &phone.app,
        StoreLayout::new(phone.root.path().join("phone")),
        &phone.clock,
        phone.storage.clone(),
    )
    .open(store, |_| ())
    .await
    .unwrap();
    phone.app.back_up_to_cloud();
    let restored = readable(
        phone
            .app
            .restore_new_device_from_cloud()
            .saved_restore_codes()
            .await
            .unwrap(),
    );
    assert_eq!(restored.len(), 1);
    assert!(restored[0].has_s3_credentials);
    reopened.close().await.unwrap();
}
