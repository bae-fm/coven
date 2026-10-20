//! Leaving a store deletes the person's restore code and S3 key from iCloud
//! Keychain, on every one of their devices; removing one device does not
//! (§13.1, §14.1).

use crate::tests::Network;
use crate::*;
use std::time::Duration;

async fn own_member(handle: &CovenHandle) -> MemberId {
    handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| member.is_self)
        .unwrap()
        .id
}

async fn wait_for(handle: &CovenHandle, reached: impl Fn(&SyncStatus) -> bool) {
    let mut status = handle.subscribe_sync_status();
    tokio::time::timeout(Duration::from_secs(15), status.wait_for(reached))
        .await
        .unwrap()
        .unwrap();
}

async fn stop(handle: &CovenHandle) {
    handle.stop_sync();
    wait_for(handle, |status| matches!(status, SyncStatus::Stopped)).await;
}

fn removed(status: &SyncStatus) -> bool {
    matches!(
        status,
        SyncStatus::Failed {
            error: SyncFailure::Removed
        }
    )
}

/// Ana removes Ben. Ben's phone deletes his restore code and S3 key when it
/// applies the removal, so his laptop stops offering Household. A refused
/// deletion holds the removal back until the deletion succeeds.
#[tokio::test]
async fn a_members_removal_deletes_their_synced_entries_before_it_applies() {
    let network = Network::new(2).await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop = ben.app.another_device();
    let saved = laptop.saved_restore_codes().await.unwrap();
    assert_eq!(saved.len(), 1);
    assert!(saved[0].has_s3_credentials);
    stop(&ben.handle).await;
    let applied = ben.handle.applied_slot().await;
    ana.handle
        .remove_member(&own_member(&ben.handle).await)
        .await
        .unwrap();
    ben.app.refuse_synced_keychain_operations(true);
    ben.handle.start_sync().await.unwrap();
    wait_for(&ben.handle, |status| {
        matches!(
            status,
            SyncStatus::Failed {
                error: SyncFailure::Other(_)
            }
        )
    })
    .await;
    assert_eq!(ben.handle.applied_slot().await, applied);
    assert_eq!(laptop.saved_restore_codes().await.unwrap(), saved);
    // Sync retries a custody failure on its own, under the shared backoff,
    // and applies the removal once the keychain answers (E5).
    ben.app.refuse_synced_keychain_operations(false);
    wait_for(&ben.handle, removed).await;
    assert!(ben.handle.applied_slot().await > applied);
    assert!(laptop.saved_restore_codes().await.unwrap().is_empty());
    assert!(ben
        .app
        .synced_keychain(ben.store)
        .s3_credentials()
        .unwrap()
        .is_none());
    // The store stays on Ben's phone, with the phone's own entries, until he
    // deletes it there.
    assert!(ben
        .app
        .store_keychain(ben.store)
        .device_id()
        .unwrap()
        .is_some());
    // Ana is still a member: her entries stay.
    assert_eq!(ana.app.saved_restore_codes().await.unwrap().len(), 1);
    network.close().await;
}

/// Ana renames Household. Ben's phone rewrites his saved code's name before
/// it applies the rename, so a refused rewrite holds the rename back until
/// the keychain answers, as a removal's deletion does.
#[tokio::test]
async fn a_rename_rewrites_the_saved_name_before_it_applies() {
    let network = Network::new(2).await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop = ben.app.another_device();
    stop(&ben.handle).await;
    let applied = ben.handle.applied_slot().await;
    ana.handle.rename_store("Family").await.unwrap();
    ben.app.refuse_synced_keychain_operations(true);
    ben.handle.start_sync().await.unwrap();
    wait_for(&ben.handle, |status| {
        matches!(
            status,
            SyncStatus::Failed {
                error: SyncFailure::Other(_)
            }
        )
    })
    .await;
    assert_eq!(ben.handle.applied_slot().await, applied);
    assert_eq!(ben.handle.store_name().await.unwrap(), "Household");
    assert_eq!(
        laptop.saved_restore_codes().await.unwrap()[0].name,
        "Household"
    );
    ben.app.refuse_synced_keychain_operations(false);
    wait_for(&ben.handle, |status| {
        matches!(status, SyncStatus::Synced { .. })
    })
    .await;
    assert_eq!(ben.handle.store_name().await.unwrap(), "Family");
    assert_eq!(
        laptop.saved_restore_codes().await.unwrap()[0].name,
        "Family"
    );
    network.close().await;
}

/// Once Ben's phone applies his removal, it refuses a new S3 key with
/// `Removed`, connected or not. Ben has no storage access left to change, and
/// saving the key would put it back in the iCloud Keychain his removal emptied.
#[tokio::test]
async fn a_removed_member_cannot_save_storage_credentials() {
    let network = Network::new(2).await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop = ben.app.another_device();
    ana.handle
        .remove_member(&own_member(&ben.handle).await)
        .await
        .unwrap();
    ben.handle.sync_now();
    wait_for(&ben.handle, removed).await;
    let held = |app: &TestCoven| {
        app.store_keychain(ben.store)
            .storage_credentials()
            .unwrap()
            .map(|bytes| bytes.as_bytes().to_vec())
    };
    let before = held(&ben.app);
    assert!(before.is_some());
    let key = || ("ben-new".to_owned(), SecretText::new("new secret".into()));
    let (id, secret) = key();
    let replaced = ben.handle.replace_access_key(id, secret).await;
    assert!(matches!(replaced, Err(SyncError::Removed)), "{replaced:?}");
    assert_eq!(held(&ben.app), before);
    for connected in [true, false] {
        if !connected {
            ben.handle.disconnect_storage().await.unwrap();
        }
        let (id, secret) = key();
        let result = ben.handle.update_credentials(id, secret).await;
        assert!(matches!(result, Err(SyncError::Removed)), "{result:?}");
        assert!(ben
            .app
            .synced_keychain(ben.store)
            .s3_credentials()
            .unwrap()
            .is_none());
        assert!(laptop.saved_restore_codes().await.unwrap().is_empty());
        // Disconnecting removed this device's own credentials; none return.
        assert_eq!(held(&ben.app), before.clone().filter(|_| connected));
    }
    network.close().await;
}

/// Ben, an admin, removes himself. His restore code and S3 key go with his
/// membership; Ana's stay.
#[tokio::test]
async fn leaving_the_store_deletes_the_persons_synced_entries() {
    for refused in [false, true] {
        leave(refused).await;
    }
}

async fn leave(refused: bool) {
    let mut network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    network
        .join_as(
            1,
            InviteTarget::NewMember {
                role: MemberRole::Admin,
            },
        )
        .await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop = ben.app.another_device();
    assert_eq!(laptop.saved_restore_codes().await.unwrap().len(), 1);
    let member = own_member(&ben.handle).await;
    if refused {
        // The removal is claimed, but this device applies it only once the
        // deletion succeeds; Ben's call reports the refusal.
        ben.app.refuse_synced_keychain_operations(true);
        let result = ben.handle.remove_member(&member).await;
        assert!(
            matches!(result, Err(SyncError::SecureStorage(_))),
            "{result:?}"
        );
        assert_eq!(laptop.saved_restore_codes().await.unwrap().len(), 1);
        ben.app.refuse_synced_keychain_operations(false);
    }
    ben.handle.remove_member(&member).await.unwrap();
    assert!(laptop.saved_restore_codes().await.unwrap().is_empty());
    assert!(ben
        .app
        .synced_keychain(ben.store)
        .s3_credentials()
        .unwrap()
        .is_none());
    ben.handle.sync_now();
    wait_for(&ben.handle, removed).await;
    assert_eq!(ana.app.saved_restore_codes().await.unwrap().len(), 1);
    network.close().await;
}

/// Ana's lost phone is removed from her other device. Only the phone stops;
/// Ana is still a member, so her restore code and S3 key stay.
#[tokio::test]
async fn removing_one_device_keeps_the_persons_synced_entries() {
    let network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let phone = &network.devices[0];
    let laptop = phone.app.another_device();
    let saved = laptop.saved_restore_codes().await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let (_, cancel) = tokio::sync::watch::channel(false);
    let restored = restore_from_keychain(
        laptop
            .builder(StoreLayout::new(root.path().into()))
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .clock(std::sync::Arc::new(crate::tests::PollingClock::new(
                network.clock.clone(),
            )))
            .storage_connector(std::sync::Arc::new(phone.storage.for_device())),
        &saved[0],
        None,
        "Ana's laptop",
        |_| {},
        &cancel,
    )
    .await
    .unwrap()
    .unwrap();
    restored
        .remove_device(phone.handle.device_id())
        .await
        .unwrap();
    phone.handle.sync_now();
    wait_for(&phone.handle, removed).await;
    assert_eq!(laptop.saved_restore_codes().await.unwrap(), saved);
    assert_eq!(phone.app.saved_restore_codes().await.unwrap(), saved);
    restored.close().await.unwrap();
    network.close().await;
}
