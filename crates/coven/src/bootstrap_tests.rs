use super::*;
use coven_crypto::custody::KeyringCustody;
use coven_database::{DatabaseBuilder, FileDatabase};
use coven_format::{
    store_log::{MemberPublicKeys, StoreChange},
    MemberAccess,
};
use coven_storage::{
    test_utils::{Faults, MemoryStorage},
    ObjectPath, ObjectPrefix, S3Credentials, Storage, StorageFailure,
};
use coven_sync::{DeviceLogSync, Files, Operations};
use std::{num::NonZeroUsize, time::UNIX_EPOCH};

// Include the installation fixtures in this test module so their retained
// database, storage and custody dependencies stay private.
include!("../tests/fixtures/bootstrap.rs");

#[tokio::test]
async fn restore_loads_snapshots_and_later_writes_with_identical_fingerprints() {
    let owner = Owner::new(CloudProvider::S3, true).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.layout.store_dir(&owner.directory.id());
    assert_eq!(
        handle.restore_code().await.unwrap(),
        owner.code.to_text().unwrap().as_str()
    );
    let synced = keychain_code(&install.keychain).unwrap().unwrap();
    assert_eq!(
        synced.to_text().unwrap().as_str(),
        handle.restore_code().await.unwrap()
    );
    handle.close().await.unwrap();
    assert_ne!(
        directory.settings().unwrap().device_id,
        owner.directory.settings().unwrap().device_id
    );
    let db = DatabaseBuilder::new(directory.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .open()
        .await
        .unwrap();
    let scoped = Arc::new(StoreKeychain::new(install.keychain.clone(), directory.id()));
    let keys = KeyringCustody::<StoreKeyring>::new(scoped.clone());
    assert_eq!(rows(&db).await, rows(&owner.db).await);
    assert_eq!(
        fingerprint(&db, &keys).await,
        fingerprint(&owner.db, owner.keys.as_ref()).await
    );
    let log = db.local_store_log().await.unwrap();
    assert_eq!(
        log.log.replay.state.devices[&log.device].member,
        owner.member.member_id()
    );
    assert_eq!(
        log.log.replay.state.devices[&log.device].name,
        "Ana’s laptop"
    );
    assert!(directory
        .owned_file(StoreFile::Bootstrap)
        .read_optional()
        .unwrap()
        .is_none());
    db.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn join_accepts_provider_access_and_approval_then_loads_the_store() {
    for provider in [
        CloudProvider::S3,
        CloudProvider::GoogleDrive,
        CloudProvider::Dropbox,
        CloudProvider::OneDrive,
        CloudProvider::CloudKit,
    ] {
        let owner = Owner::new(provider, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let mut requests = owner.operations.subscribe_join_requests();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let approve = async {
            let request = next_request(&mut requests).await;
            assert_eq!(request.device_name, "New phone");
            install.absent(owner.directory.id()).await;
            owner
                .operations
                .approve_join_request(&request)
                .await
                .unwrap();
            request.member
        };
        let (result, member) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(joining, approve)
        })
        .await
        .unwrap();
        let handle = result.unwrap().unwrap();
        let restored =
            coven_sync::read_restore_code(&handle.restore_code().await.unwrap()).unwrap();
        assert_eq!(restored.member_keys.member_id(), member);
        assert_ne!(member, owner.member.member_id());
        assert_eq!(
            handle
                .read(|sql| Ok(
                    sql.query("SELECT id,body FROM notes ORDER BY id", [], |r| Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?
                    )))?
                ))
                .await
                .unwrap(),
            rows(&owner.db).await
        );
        assert_eq!(
            handle
                .get_members()
                .await
                .unwrap()
                .iter()
                .find(|m| m.is_self)
                .unwrap()
                .devices
                .len(),
            1
        );
        handle.close().await.unwrap();
        owner.close().await;
    }
}

#[tokio::test]
async fn restart_while_waiting_reuses_the_member_device_and_request_bytes() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let mut joining = Box::pin(install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    ));
    let request = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::select! { result = &mut joining => panic!("join returned before approval: {result:?}"), request = next_request(&mut requests) => request }
    }).await.unwrap();
    let bytes = owner
        .storage
        .read(&ObjectPath::join_request(invite.id))
        .await
        .unwrap();
    drop(joining);
    install.absent(owner.directory.id()).await;
    let pending = install
        .layout
        .begin_bootstrap(owner.directory.id(), "Household", &UuidIds)
        .unwrap();
    let device = pending.directory().settings().unwrap().device_id;
    drop(pending);
    let resumed = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        assert_eq!(
            owner
                .storage
                .read(&ObjectPath::join_request(invite.id))
                .await
                .unwrap(),
            bytes
        );
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(resumed, approve)
    })
    .await
    .unwrap();
    let handle = result.unwrap().unwrap();
    let directory = install.layout.store_dir(&owner.directory.id());
    assert_eq!(directory.settings().unwrap().device_id, device);
    assert_eq!(
        keychain_code(&install.keychain)
            .unwrap()
            .unwrap()
            .member_keys
            .member_id(),
        request.member
    );
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn declined_and_expired_requests_finish_without_committing_custody() {
    for expiry in [false, true] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let mut requests = owner.operations.subscribe_join_requests();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let settle = async {
            let request = next_request(&mut requests).await;
            if expiry {
                owner.clock.set(invite.expires_at);
                owner.operations.blocked_operations().await.unwrap();
            } else {
                owner
                    .operations
                    .decline_join_request(&request)
                    .await
                    .unwrap();
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(joining, settle)
        })
        .await
        .unwrap();
        assert!(result.unwrap().is_none());
        install.absent(owner.directory.id()).await;
        assert!(!install
            .root
            .path()
            .join("stores")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

#[tokio::test]
async fn permission_revocation_while_joining_keeps_the_provider_failure() {
    let owner = Owner::new(CloudProvider::GoogleDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let revoke = async {
        let request = next_request(&mut requests).await;
        owner
            .operations
            .decline_join_request(&request)
            .await
            .unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, revoke)
    })
    .await
    .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::PermissionDenied)
    );
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn cancellation_while_waiting_removes_every_local_installation_value() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (stop, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let cancel_join = async {
        next_request(&mut requests).await;
        stop.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, cancel_join)
    })
    .await
    .unwrap();
    assert!(matches!(result, Err(BootstrapError::Cancelled)));
    install.absent(owner.directory.id()).await;
    assert!(!install
        .root
        .path()
        .join("stores")
        .join(owner.directory.id().to_string())
        .exists());
    owner.operations.cancel_invite(&invite.id).await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn cancellation_during_restore_and_before_publication_keeps_final_custody_empty() {
    for phase in [
        "Preparing this device",
        "Opening member keys and the store log",
        "Keeping keys and credentials",
    ] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let install = Installation::new();
        let (stop, cancel) = watch::channel(false);
        let result = install
            .run(
                &owner,
                Installation::restore_request(&owner),
                owner.storage.clone(),
                &cancel,
                |status| {
                    if status == phase {
                        stop.send(true).unwrap();
                    }
                },
            )
            .await;
        assert!(
            matches!(result, Err(BootstrapError::Cancelled)),
            "{phase}: {result:?}"
        );
        install.absent(owner.directory.id()).await;
        assert!(!install
            .root
            .path()
            .join("stores")
            .join(owner.directory.id().to_string())
            .exists());
        owner.close().await;
    }
}

#[tokio::test]
async fn storage_failure_retains_staging_for_retry_without_final_keys() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    owner
        .storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        install
            .run(
                &owner,
                Installation::restore_request(&owner),
                owner.storage.clone(),
                &cancel,
                |_| {}
            )
            .await,
        Err(BootstrapError::Sync(SyncError::Storage(_)))
    ));
    install.absent(owner.directory.id()).await;
    assert!(install
        .run(
            &owner,
            Installation::restore_request(&owner),
            owner.storage.clone(),
            &cancel,
            |_| {}
        )
        .await
        .unwrap()
        .is_some());
    owner.close().await;
}

#[tokio::test]
async fn restore_codes_track_s3_keys_and_oauth_credentials_in_synced_custody() {
    for provider in [CloudProvider::S3, CloudProvider::GoogleDrive] {
        let owner = Owner::new(provider, false).await;
        let install = Installation::new();
        let handle = install.restore(&owner).await;
        let directory = install.layout.store_dir(&owner.directory.id());
        let before = handle.restore_code().await.unwrap();
        let info = decode_code_info(&before).unwrap();
        assert_eq!(info.kind, CodeKind::Restore);
        assert_eq!(info.store_id, directory.id());
        assert_eq!(info.cloud_provider, provider);
        assert_eq!(info.needs_oauth, provider != CloudProvider::S3);
        let mut updated = coven_sync::read_restore_code(&before).unwrap();
        let mut data = RestoreStorage::decode(updated.storage.as_bytes()).unwrap();
        data.credentials = if provider == CloudProvider::S3 {
            StorageCredentials::S3(S3Credentials {
                access_key_id: "replacement".into(),
                secret_access_key: SecretText::new("new-secret".into()),
            })
        } else {
            StorageCredentials::OAuth(tokens("replacement"))
        };
        updated.storage = data.encode().unwrap();
        let after = updated.to_text().unwrap();
        if provider == CloudProvider::S3 {
            assert_eq!(
                handle
                    .replace_access_key("replacement".into(), SecretText::new("new-secret".into()))
                    .await
                    .unwrap(),
                after.as_str()
            );
            assert_eq!(
                owner.storage.s3_access_key_id().await.as_deref(),
                Some("replacement")
            );
        } else {
            handle.update_credentials(&after).await.unwrap();
        }
        assert_ne!(before, after.as_str());
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        assert_eq!(
            keychain_code(&install.keychain)
                .unwrap()
                .unwrap()
                .to_text()
                .unwrap()
                .as_str(),
            after.as_str()
        );
        let mut wrong_member = coven_sync::read_restore_code(&after).unwrap();
        wrong_member.member_keys = MemberKeys::generate().unwrap();
        assert!(matches!(
            handle
                .update_credentials(&wrong_member.to_text().unwrap())
                .await,
            Err(SyncError::WrongMember { .. })
        ));
        let mut wrong_store = coven_sync::read_restore_code(&after).unwrap();
        wrong_store.store = StoreId(UuidIds.new_id());
        assert!(matches!(
            handle
                .update_credentials(&wrong_store.to_text().unwrap())
                .await,
            Err(SyncError::WrongStore { .. })
        ));
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        handle.close().await.unwrap();
        let handle = Coven::builder(install.layout.clone())
            .with_keychain(install.keychain.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .clock(owner.clock.clone())
            .open(directory.id())
            .await
            .unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), after.as_str());
        handle.update_credentials(&before).await.unwrap();
        assert_eq!(handle.restore_code().await.unwrap(), before);
        handle.close().await.unwrap();
        assert!(matches!(
            handle.restore_code().await,
            Err(SyncError::Database(DbError::StoreClosed))
        ));
        owner.close().await;
    }
}

#[tokio::test]
async fn keychain_discovery_refuses_ambiguous_or_mismatched_store_ids() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    assert!(keychain_code(&install.keychain).unwrap().is_none());
    let scoped = StoreKeychain::new(install.keychain.clone(), owner.directory.id());
    scoped
        .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
        .unwrap();
    assert_eq!(
        keychain_code(&install.keychain).unwrap().unwrap().store,
        owner.directory.id()
    );
    let other = StoreKeychain::new(install.keychain.clone(), StoreId(UuidIds.new_id()));
    other
        .set_synced_restore_code(&SecretBytes::new(owner.code.to_bytes().unwrap().to_vec()))
        .unwrap();
    assert!(
        matches!(keychain_code(&install.keychain), Err(BootstrapError::MultipleStores(ids)) if ids.len() == 2)
    );
    scoped.delete_synced_restore_code().unwrap();
    assert!(matches!(
        keychain_code(&install.keychain),
        Err(BootstrapError::Code(CodeError::Invalid))
    ));
    owner.close().await;
}

#[tokio::test]
async fn a_device_removed_during_loading_receives_the_provider_refusal() {
    let owner = Owner::new(CloudProvider::GoogleDrive, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_listing(ObjectPrefix::device_logs(), listed, resumed)
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let remove = async {
        let request = next_request(&mut requests).await;
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
        listing.await.unwrap();
        owner.operations.sync_store_log().await.unwrap();
        let members = owner.operations.get_members().await.unwrap();
        let devices = &members
            .iter()
            .find(|m| m.id == request.member)
            .unwrap()
            .devices;
        assert_eq!(devices.len(), 1);
        assert!(matches!(
            owner.operations.remove_device(devices[0]).await.unwrap(),
            ProviderSignOut::RemoveAppAccess {
                provider: CloudProvider::GoogleDrive
            }
        ));
        // Cut off this account in the fake to model provider refusal on the removed device.
        owner
            .storage
            .revoke_access(&MemberAccess::ProviderAccount("join@example.com".into()))
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, remove)
    })
    .await
    .unwrap();
    assert!(
        matches!(result, Err(BootstrapError::Sync(SyncError::Storage(error))) if error.failure() == StorageFailure::PermissionDenied)
    );
    install.absent(owner.directory.id()).await;
    owner.close().await;
}

#[tokio::test]
async fn approval_during_a_log_listing_is_not_mistaken_for_decline() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let mut requests = owner.operations.subscribe_join_requests();
    let (listed, listing) = tokio::sync::oneshot::channel();
    let (resume, resumed) = tokio::sync::oneshot::channel();
    owner
        .storage
        .hold_next_listing(ObjectPrefix::store_logs(), listed, resumed)
        .await;
    let joining = install.run(
        &owner,
        Installation::join_request(&invite),
        owner.recipient(),
        &cancel,
        |_| {},
    );
    let approve = async {
        listing.await.unwrap();
        let request = next_request(&mut requests).await;
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
        resume.send(()).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, approve)
    })
    .await
    .unwrap();
    assert!(
        result.unwrap().is_some(),
        "approval deleted the request after this device took its log listing"
    );
    owner.close().await;
}

#[tokio::test]
async fn removal_lists_every_recorded_key_including_a_dropped_replacement() {
    for concurrent in [true, false] {
        let owner = Owner::new(CloudProvider::S3, false).await;
        let invite = owner.invite().await;
        let install = Installation::new();
        let (_, cancel) = watch::channel(false);
        let mut requests = owner.operations.subscribe_join_requests();
        let joining = install.run(
            &owner,
            Installation::join_request(&invite),
            owner.recipient(),
            &cancel,
            |_| {},
        );
        let approve = async {
            let request = next_request(&mut requests).await;
            owner
                .operations
                .approve_join_request(&request)
                .await
                .unwrap();
            request.member
        };
        let (result, member) = tokio::time::timeout(Duration::from_secs(20), async {
            tokio::join!(joining, approve)
        })
        .await
        .unwrap();
        let handle = result.unwrap().unwrap();
        let replace = || {
            handle.replace_access_key(
                "new-member-key".into(),
                SecretText::new("new-secret".into()),
            )
        };
        let expected_key = if concurrent {
            let (listed, listing) = tokio::sync::oneshot::channel();
            let (resume, resumed) = tokio::sync::oneshot::channel();
            owner
                .storage
                .hold_next_listing(ObjectPrefix::store_logs(), listed, resumed)
                .await;
            let replacement = async {
                listing.await.unwrap();
                let code = replace().await.unwrap();
                assert_eq!(
                    coven_sync::read_restore_code(&code)
                        .unwrap()
                        .member_keys
                        .member_id(),
                    member
                );
                resume.send(()).unwrap();
            };
            let (removal, ()) = tokio::time::timeout(Duration::from_secs(20), async {
                tokio::join!(owner.operations.remove_member(&member), replacement)
            })
            .await
            .unwrap();
            assert_eq!(
                removal.unwrap(),
                MemberRemoval::DeleteAccessKey {
                    access_key_id: "invited-key".into()
                }
            );
            "invited-key"
        } else {
            let code = replace().await.unwrap();
            assert_eq!(
                coven_sync::read_restore_code(&code)
                    .unwrap()
                    .member_keys
                    .member_id(),
                member
            );
            owner.operations.sync_store_log().await.unwrap();
            assert_eq!(
                owner.operations.remove_member(&member).await.unwrap(),
                MemberRemoval::DeleteAccessKey {
                    access_key_id: "new-member-key".into()
                }
            );
            "new-member-key"
        };
        let log = owner.db.local_store_log().await.unwrap().log;
        let entry = log
            .entries
            .iter()
            .find(|e| {
                e.entry.author == member
                    && matches!(
                        &e.entry.change,
                        StoreChange::SetAccess {
                            access: MemberAccess::S3AccessKey { access_key_id }
                        } if access_key_id == "new-member-key"
                    )
            })
            .unwrap();
        assert_eq!(
            matches!(
                log.replay.entries[&entry.entry.position],
                coven_database::EntryOutcome::Dropped(_)
            ),
            concurrent
        );
        assert_eq!(
            log.replay.state.members[&member].access,
            MemberAccess::S3AccessKey {
                access_key_id: expected_key.into()
            }
        );
        assert_eq!(
            owner.operations.access_keys_to_delete().await.unwrap(),
            ["invited-key", "new-member-key"].map(|key| AccessKeyToDelete {
                access_key_id: key.into(),
                member: Some(member.clone())
            })
        );
        handle.close().await.unwrap();
        owner.close().await;
    }
}

#[tokio::test]
async fn failed_access_publication_retains_credentials_and_retry_finishes_once() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let install = Installation::new();
    let handle = install.restore(&owner).await;
    let directory = install.layout.store_dir(&owner.directory.id());
    owner
        .storage
        .set_faults(Faults {
            fail_next: 1,
            ..Faults::none()
        })
        .await;
    assert!(matches!(
        handle
            .replace_access_key("replacement".into(), SecretText::new("secret".into()))
            .await,
        Err(SyncError::Storage(_))
    ));
    let retained = handle.restore_code().await.unwrap();
    assert_eq!(
        owner.storage.s3_access_key_id().await.as_deref(),
        Some("replacement")
    );
    assert_eq!(
        keychain_code(&install.keychain)
            .unwrap()
            .unwrap()
            .to_text()
            .unwrap()
            .as_str(),
        retained
    );
    handle.close().await.unwrap();
    let handle = install.handle(directory.clone(), &owner).await;
    for _ in 0..2 {
        assert_eq!(
            handle
                .replace_access_key("replacement".into(), SecretText::new("secret".into()))
                .await
                .unwrap(),
            retained
        );
    }
    owner.operations.sync_store_log().await.unwrap();
    let log = owner.db.local_store_log().await.unwrap().log;
    assert_eq!(
        log.entries
            .iter()
            .filter(|e| matches!(e.entry.change, StoreChange::SetAccess { .. }))
            .count(),
        1
    );
    handle.close().await.unwrap();
    let disconnected = Coven::builder(install.layout.clone())
        .with_keychain(install.keychain.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .open(directory.id())
        .await
        .unwrap();
    assert!(matches!(
        disconnected
            .replace_access_key("offline".into(), SecretText::new("secret".into()))
            .await,
        Err(SyncError::NoStorage)
    ));
    assert_eq!(disconnected.restore_code().await.unwrap(), retained);
    disconnected.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn restores_return_session_keys_and_builder_choices_without_reopening() {
    for from_keychain in [false, true] {
        for passphrase in [false, true] {
            let owner = Owner::new(CloudProvider::S3, false).await;
            let install = Installation::new();
            let (_, cancel) = watch::channel(false);
            let builder = install
                .builder(&owner, owner.storage.clone())
                .max_concurrent_uploads(NonZeroUsize::new(3).unwrap())
                .max_concurrent_downloads(NonZeroUsize::new(4).unwrap())
                .key_custody(if passphrase {
                    KeyCustody::Passphrase(Passphrase::new("store password".into()))
                } else {
                    KeyCustody::InMemory
                })
                .identity_custody(if passphrase {
                    IdentityCustody::Passphrase(Passphrase::new("identity password".into()))
                } else {
                    IdentityCustody::InMemory
                });
            let handle = if from_keychain {
                let scoped = StoreKeychain::new(install.keychain.clone(), owner.directory.id());
                scoped
                    .set_synced_restore_code(&SecretBytes::new(
                        owner.code.to_bytes().unwrap().to_vec(),
                    ))
                    .unwrap();
                restore_from_keychain(builder, "Laptop", None, |_| {}, &cancel)
                    .await
                    .unwrap()
                    .unwrap()
            } else {
                restore_from_code(
                    builder,
                    &owner.code.to_text().unwrap(),
                    "Laptop",
                    None,
                    |_| {},
                    &cancel,
                )
                .await
                .unwrap()
            };
            assert_eq!(handle.transfer_limits().uploads.get(), 3);
            assert_eq!(handle.transfer_limits().downloads.get(), 4);
            assert_eq!(
                handle.restore_code().await.unwrap(),
                owner.code.to_text().unwrap().as_str()
            );
            let sealed = handle
                .seal_app_data(b"session", b"notes/new")
                .await
                .unwrap();
            assert_eq!(
                handle.open_app_data(&sealed, b"notes/new").unwrap(),
                b"session"
            );
            handle
                .write(|sql| {
                    sql.execute(
                        "INSERT INTO notes VALUES('new',?1)",
                        params![b"new".to_vec()],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(install.layout.stores().await.unwrap().len(), 1);
            let directory = install.layout.store_dir(&owner.directory.id());
            assert!(matches!(
                directory.lock_exclusive(),
                Err(StoreLockError::AlreadyOpen(_))
            ));
            let member = coven_sync::read_restore_code(&handle.restore_code().await.unwrap())
                .unwrap()
                .member_keys
                .member_id();
            assert_eq!(member, owner.member.member_id());
            handle.close().await.unwrap();
            if passphrase {
                // The same files remain usable at their permanent paths after publication.
                let reopened = install
                    .builder(&owner, owner.storage.clone())
                    .key_custody(KeyCustody::Passphrase(Passphrase::new(
                        "store password".into(),
                    )))
                    .identity_custody(IdentityCustody::Passphrase(Passphrase::new(
                        "identity password".into(),
                    )))
                    .open(directory.id())
                    .await
                    .unwrap();
                assert_eq!(
                    reopened.open_app_data(&sealed, b"notes/new").unwrap(),
                    b"session"
                );
                reopened.close().await.unwrap();
            }
            owner.close().await;
        }
    }
}

#[tokio::test]
async fn joining_retains_the_approved_member_in_session_custody() {
    let owner = Owner::new(CloudProvider::S3, false).await;
    let invite = owner.invite().await;
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let builder = install
        .builder(&owner, owner.recipient())
        .key_custody(KeyCustody::InMemory)
        .identity_custody(IdentityCustody::InMemory);
    let joining = join_with_invite(builder, &invite.code, "New phone", None, |_| {}, &cancel);
    let mut requests = owner.operations.subscribe_join_requests();
    let approve = async {
        let request = next_request(&mut requests).await;
        install.absent(owner.directory.id()).await;
        owner
            .operations
            .approve_join_request(&request)
            .await
            .unwrap();
        request.member
    };
    let (handle, member) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(joining, approve)
    })
    .await
    .unwrap();
    let handle = handle.unwrap().unwrap();
    assert_eq!(
        coven_sync::read_restore_code(&handle.restore_code().await.unwrap())
            .unwrap()
            .member_keys
            .member_id(),
        member
    );
    let sealed = handle.seal_app_data(b"joined", b"notes/new").await.unwrap();
    assert_eq!(
        handle.open_app_data(&sealed, b"notes/new").unwrap(),
        b"joined"
    );
    handle.close().await.unwrap();
    owner.close().await;
}

#[tokio::test]
async fn missing_keychain_code_returns_none_without_creating_a_store() {
    let install = Installation::new();
    let (_, cancel) = watch::channel(false);
    let result = restore_from_keychain(
        Coven::builder(install.layout.clone()).with_keychain(install.keychain.clone()),
        "Laptop",
        None,
        |_| {},
        &cancel,
    )
    .await
    .unwrap();
    assert!(result.is_none());
    assert!(install.layout.stores().await.unwrap().is_empty());
}
