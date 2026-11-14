//! Leaving a store deletes the person's restore code and S3 key from iCloud
//! Keychain, on every one of their devices; removing one device does not
//! (§13.1, §14.1).

use crate::tests::Network;
use crate::*;

async fn own_member(handle: &CovenHandle) -> MemberId {
    handle.members().await.unwrap().into_iter().find(|member| member.is_self).unwrap().id
}

/// Wait until the last pass, as the app's SQL reads it, is one `reached`
/// accepts.
async fn wait_for(handle: &CovenHandle, reached: impl FnMut(&Option<LastPass>) -> bool) {
    handle.test_last_pass().wait_for(reached).await;
}

/// Wait until a pass ends with this device removed the way `removal` says, and
/// the state shows it.
async fn removed(handle: &CovenHandle, removal: coven_database::Removal) {
    wait_for(handle, |last| {
        matches!(
            last,
            Some(LastPass::Failed {
                failure: StatusFailure::Removed(stop),
                ..
            }) if *stop == removal
        )
    })
    .await;
    let status = handle.test_sync_status().latest().await;
    assert!(
        matches!(status.state, SyncState::Removed { removal: stop, .. } if stop == removal),
        "{status:?}"
    );
}

/// Ana removes Ben. Ben's phone deletes his restore code and S3 key when it
/// applies the removal, so his laptop stops offering Household. A refused
/// deletion holds the removal back until the deletion succeeds.
#[tokio::test]
async fn a_members_removal_deletes_their_synced_entries_before_it_applies() {
    let network = Network::new(2).await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = ben.app.another_device(laptop_dir.path().into());
    let saved = crate::tests::readable(laptop.saved_restore_codes().await.unwrap());
    assert_eq!(saved.len(), 1);
    assert!(saved[0].has_s3_credentials);
    ben.stop_sync().await;
    let applied = ben.handle.applied_slot().await;
    let removal = ana.handle.remove_member(&own_member(&ben.handle).await).await.unwrap();
    crate::tests::landed(&ana.handle, removal).await;
    ben.app.refuse_person_entry_operations(true);
    ben.handle.start_sync().await.unwrap();
    wait_for(&ben.handle, |last| {
        matches!(last, Some(LastPass::Failed { failure: StatusFailure::Other, .. }))
    })
    .await;
    assert_eq!(ben.handle.applied_slot().await, applied);
    assert_eq!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()), saved);
    // Sync retries a custody failure on its own, under the shared backoff,
    // and applies the removal once the keychain answers (E5). The backoff
    // passes as the test moves the clock to its end.
    ben.app.refuse_person_entry_operations(false);
    network.end_wait(crate::tests::connection_wait(&ben.handle, |_| true).await);
    removed(&ben.handle, coven_database::Removal::Member).await;
    assert!(ben.handle.applied_slot().await > applied);
    assert!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).is_empty());
    assert!(ben.app.person_entries(ben.store).s3_credentials().unwrap().is_none());
    // The store stays on Ben's phone, with the phone's own entries, until he
    // deletes it there.
    assert!(ben.app.store_keychain(ben.store).device_id().unwrap().is_some());
    // Ana is still a member: her entries stay.
    assert_eq!(crate::tests::readable(ana.app.saved_restore_codes().await.unwrap()).len(), 1);
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
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = ben.app.another_device(laptop_dir.path().into());
    ben.stop_sync().await;
    let applied = ben.handle.applied_slot().await;
    crate::tests::landed(&ana.handle, ana.handle.rename_store("Family").await.unwrap()).await;
    ben.app.refuse_person_entry_operations(true);
    ben.handle.start_sync().await.unwrap();
    wait_for(&ben.handle, |last| {
        matches!(last, Some(LastPass::Failed { failure: StatusFailure::Other, .. }))
    })
    .await;
    assert_eq!(ben.handle.applied_slot().await, applied);
    assert_eq!(ben.handle.store_name().await.unwrap(), "Household");
    assert_eq!(
        crate::tests::readable(laptop.saved_restore_codes().await.unwrap())[0].name,
        "Household"
    );
    ben.app.refuse_person_entry_operations(false);
    network.end_wait(crate::tests::connection_wait(&ben.handle, |_| true).await);
    wait_for(&ben.handle, |last| *last == Some(LastPass::Synced)).await;
    assert_eq!(ben.handle.store_name().await.unwrap(), "Family");
    assert_eq!(
        crate::tests::readable(laptop.saved_restore_codes().await.unwrap())[0].name,
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
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = ben.app.another_device(laptop_dir.path().into());
    let removal = ana.handle.remove_member(&own_member(&ben.handle).await).await.unwrap();
    crate::tests::landed(&ana.handle, removal).await;
    ben.handle.sync_now();
    removed(&ben.handle, coven_database::Removal::Member).await;
    let held = |app: &Coven| {
        app.store_keychain(ben.store)
            .storage_credentials()
            .unwrap()
            .map(|bytes| bytes.as_bytes().to_vec())
    };
    let before = held(&ben.app);
    assert!(before.is_some());
    let key = || ("ben-new".to_owned(), SecretText::new("new secret".into()));
    let (id, secret) = key();
    let replaced = ben.handle.use_access_key(id, secret).await;
    assert!(
        matches!(replaced, Err(SyncError::Removed(coven_database::Removal::Member))),
        "{replaced:?}"
    );
    assert_eq!(held(&ben.app), before);
    let laptop = &laptop;
    let refused = |handle: CovenHandle| async move {
        let (id, secret) = key();
        let result = handle.use_access_key(id, secret).await;
        assert!(
            matches!(result, Err(SyncError::Removed(coven_database::Removal::Member))),
            "{result:?}"
        );
        assert!(ben.app.person_entries(ben.store).s3_credentials().unwrap().is_none());
        assert!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).is_empty());
    };
    refused(ben.handle.clone()).await;
    assert_eq!(held(&ben.app), before);
    // Without a connection, and with this device's own credentials gone, as
    // when the person deletes the keychain entry: none return.
    ben.handle.close().await.unwrap();
    ben.app.lose_storage_credentials(ben.store).unwrap();
    let reopened = network.reopen(ben).open(ben.store, |_| ()).await.unwrap();
    refused(reopened.clone()).await;
    assert_eq!(held(&ben.app), None);
    reopened.close().await.unwrap();
    ana.handle.close().await.unwrap();
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
    network.join_as(1, MemberRole::Admin).await;
    let ana = &network.devices[0];
    let ben = &network.devices[1];
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = ben.app.another_device(laptop_dir.path().into());
    assert_eq!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).len(), 1);
    let member = own_member(&ben.handle).await;
    ben.app.refuse_person_entry_operations(refused);
    let removal = ben.handle.remove_member(&member).await.unwrap();
    if refused {
        // The removal is claimed, but this device applies it only once the
        // deletion succeeds: the pass fails with the refusal, and sync tries
        // again under the shared backoff.
        wait_for(&ben.handle, |last| {
            matches!(last, Some(LastPass::Failed { failure: StatusFailure::Other, .. }))
        })
        .await;
        assert_eq!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).len(), 1);
        ben.app.refuse_person_entry_operations(false);
        network.end_wait(crate::tests::connection_wait(&ben.handle, |_| true).await);
    }
    // The removal took effect, and its operation ended landed as this
    // device stopped.
    crate::tests::landed(&ben.handle, removal).await;
    assert!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()).is_empty());
    assert!(ben.app.person_entries(ben.store).s3_credentials().unwrap().is_none());
    removed(&ben.handle, coven_database::Removal::Member).await;
    assert_eq!(crate::tests::readable(ana.app.saved_restore_codes().await.unwrap()).len(), 1);
    network.close().await;
}

/// Ana's lost phone is removed from her other device. Only the phone stops;
/// Ana is still a member, so her restore code and S3 key stay.
#[tokio::test]
async fn removing_one_device_keeps_the_person_entries() {
    let network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let phone = &network.devices[0];
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = phone.app.another_device(laptop_dir.path().into());
    let saved = crate::tests::readable(laptop.saved_restore_codes().await.unwrap());
    let (_, cancel) = tokio::sync::watch::channel(false);
    let restored = restore_from_saved_code(
        laptop
            .builder()
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .clock(network.polling.clone())
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
    restored.start_sync().await.unwrap();
    let (removal, sign_out) = restored.remove_device(phone.handle.device_id()).await.unwrap();
    // On S3 the phone shares Ana's key, so nothing at the provider changes.
    assert_eq!(sign_out, None);
    crate::tests::landed(&restored, removal).await;
    phone.handle.sync_now();
    removed(&phone.handle, coven_database::Removal::Device).await;
    assert_eq!(crate::tests::readable(laptop.saved_restore_codes().await.unwrap()), saved);
    assert_eq!(crate::tests::readable(phone.app.saved_restore_codes().await.unwrap()), saved);
    restored.close().await.unwrap();
    network.close().await;
}

/// Ana restores the store on her laptop and closes it, then removes the
/// laptop from her phone at once, before the phone has read the laptop's
/// registration (§14.1).
#[tokio::test]
async fn a_device_is_removable_right_after_its_restore_finished() {
    let network = Network::with_schema(CloudProvider::S3, Vec::new(), Vec::new()).await;
    let phone = &network.devices[0];
    let laptop_dir = tempfile::tempdir().unwrap();
    let laptop = phone.app.another_device(laptop_dir.path().into());
    let saved = crate::tests::readable(laptop.saved_restore_codes().await.unwrap());
    let (_, cancel) = tokio::sync::watch::channel(false);
    let restored = restore_from_saved_code(
        laptop
            .builder()
            .synced_tables(Vec::new())
            .migrations(Vec::new())
            .clock(network.polling.clone())
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
    let device = restored.device_id();
    restored.close().await.unwrap();
    let (removal, _) = phone.handle.remove_device(device).await.unwrap();
    crate::tests::landed(&phone.handle, removal).await;
    // Once the phone has applied the removal, removing the laptop again is
    // decided at once: it journals nothing.
    assert_eq!(phone.handle.remove_device(device).await.unwrap().0, None);
    network.close().await;
}
