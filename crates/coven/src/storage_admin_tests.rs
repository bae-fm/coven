//! Admin access from the facade: obtaining it, minting an invite's key with
//! it, deleting that key on evidence, and guided console steps (§20.10, §14.3).
use crate::tests::Network;
use crate::*;
use coven_foundation::files::StoreDirAccess;
use coven_storage::Storage;

fn pending_keys(records: &[PendingRecord]) -> Vec<(OperationId, AccessKeyToDelete)> {
    records
        .iter()
        .filter_map(|record| match (&record.subject, &record.reason) {
            (PendingSubject::Operation { id, .. }, PendingReason::DeleteAccessKey(key)) => {
                Some((*id, key.clone()))
            }
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn admin_access_makes_an_invites_key_and_deletes_it_on_evidence() {
    let network = Network::with_schema(CloudProvider::S3, vec![], vec![]).await;
    let owner = &network.devices[0];
    let admin = owner
        .handle
        .admin_access(AdminAccessSource::Pasted(AdminCredential::AccessKey {
            access_key_id: "admin".into(),
            secret_access_key: SecretText::new("admin-secret".into()),
            session_token: None,
            iam_endpoint: None,
        }))
        .await
        .unwrap();
    let invite = owner
        .handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::S3AdminAccess(admin.clone()),
        )
        .await
        .unwrap();
    let made = owner.storage.key_admin();
    assert_eq!(made.calls(), ["mint", "probe"]);
    assert_eq!(made.users().len(), 1);
    owner.handle.cancel_invite(&invite.id).await.unwrap();
    let pending = pending_keys(&owner.handle.pending().await.unwrap());
    assert_eq!(pending.len(), 1);
    let (operation, key) = &pending[0];
    assert!(matches!(&key.principal, S3Principal::IamUser { .. }));
    // A failed deletion keeps the record and reports why.
    made.fail_next_delete();
    assert!(matches!(
        owner
            .handle
            .delete_access_key(*operation, admin.clone())
            .await,
        Err(SyncError::AccessKeyNotDeleted(_))
    ));
    assert_eq!(
        pending_keys(&owner.handle.pending().await.unwrap()).len(),
        1
    );
    owner
        .handle
        .delete_access_key(*operation, admin)
        .await
        .unwrap();
    assert!(pending_keys(&owner.handle.pending().await.unwrap()).is_empty());
    assert!(made.users().is_empty());
    network.close().await;
}

#[tokio::test]
async fn guided_steps_fill_the_member_policy_for_this_store() {
    let network = Network::with_schema(CloudProvider::S3, vec![], vec![]).await;
    let owner = &network.devices[0];
    let guided = owner.handle.guided_access_key().await.unwrap();
    let StorageConfig::S3 { bucket, .. } = owner.storage.config() else {
        panic!("S3 location")
    };
    assert_eq!(guided.bucket, bucket);
    assert!(guided.store_prefix.ends_with(&format!("{}/", owner.store)));
    let policy: serde_json::Value = serde_json::from_str(&guided.policy).unwrap();
    assert_eq!(
        policy["Statement"][1]["Condition"]["StringLike"]["s3:prefix"],
        format!("{}*", guided.store_prefix)
    );
    network.close().await;
}

#[tokio::test]
async fn an_admin_replaces_a_members_key_and_their_device_adopts_it() {
    let network = Network::new(2).await;
    let owner = &network.devices[0];
    let member = owner
        .handle
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| !member.is_self)
        .unwrap()
        .id;
    let admin = owner
        .handle
        .admin_access(AdminAccessSource::Pasted(AdminCredential::AccessKey {
            access_key_id: "admin".into(),
            secret_access_key: SecretText::new("admin-secret".into()),
            session_token: None,
            iam_endpoint: None,
        }))
        .await
        .unwrap();
    let replaced = owner
        .handle
        .replace_member_access_key(&member, admin)
        .await
        .unwrap();
    let new_key = replaced.key.credentials.access_key_id.clone();
    // The invite's key was made in the console; only the console deletes it.
    let OldKeyDeletion::Console { operation } = replaced.old_key else {
        panic!("console deletion: {:?}", replaced.old_key)
    };
    assert_eq!(
        pending_keys(&owner.handle.pending().await.unwrap())
            .iter()
            .find(|(id, _)| *id == operation)
            .unwrap()
            .1
            .access_key_id,
        "member-1"
    );
    let held = || {
        let device = &network.devices[1];
        let bytes = device
            .app
            .store_keychain(device.store)
            .storage_credentials()
            .unwrap()
            .unwrap();
        match coven_storage::StorageCredentials::decode(bytes.as_bytes()).unwrap() {
            coven_storage::StorageCredentials::S3(key) => key.access_key_id,
            _ => panic!("S3 device key"),
        }
    };
    assert_eq!(held(), "member-1");
    // The member's device picks up the delivered key on its next sync.
    network.sync(1).await;
    assert_eq!(held(), new_key);
    network.close().await;
}

/// The owner's desktop with one invite whose key admin access made.
async fn invited_with_made_key() -> (Network, coven_storage::test_utils::MemoryKeyAdmin, Invite) {
    let network = Network::new(1).await;
    let owner = &network.devices[0];
    let admin = owner
        .handle
        .admin_access(AdminAccessSource::Pasted(AdminCredential::AccessKey {
            access_key_id: "admin".into(),
            secret_access_key: SecretText::new("admin-secret".into()),
            session_token: None,
            iam_endpoint: None,
        }))
        .await
        .unwrap();
    let invite = owner
        .handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::S3AdminAccess(admin),
        )
        .await
        .unwrap();
    let made = owner.storage.key_admin();
    assert_eq!(made.users().len(), 1);
    (network, made, invite)
}

/// A database too damaged to read carries no journal; its unrecorded keys
/// remain subject to reclamation after the old device is retired (§14.3).
#[tokio::test]
async fn a_reset_of_an_unreadable_database_leaves_key_reclamation_to_admins() {
    for recorded in [false, true] {
        let (mut network, made, _invite) = invited_with_made_key().await;
        let device = network.devices.remove(0);
        device.handle.close().await.unwrap();
        let directory = device.layout.store_dir(&device.store);
        {
            let database = directory.database_path();
            for suffix in ["-wal", "-shm"] {
                let path = std::path::PathBuf::from(format!("{}{suffix}", database.display()));
                if path.exists() {
                    std::fs::remove_file(path).unwrap();
                }
            }
            std::fs::write(&database, b"not a database").unwrap();
        }
        // An interrupted reset already recorded its decision; otherwise the
        // reset records it itself.
        if recorded {
            directory
                .record_device_reset(&coven_foundation::files::DeviceReset {
                    old_device: directory.settings().unwrap().device_id,
                    new_device: DeviceId(9000),
                })
                .unwrap();
        }
        let handle = network
            .reopen(&device)
            .reset_device(device.store, "Reset desktop")
            .await
            .unwrap();
        assert!(pending_keys(&handle.pending().await.unwrap()).is_empty());
        assert_eq!(made.users().len(), 1);
        handle.close().await.unwrap();
        network.close().await;
    }
}

async fn reset_owner(network: &mut Network) {
    let owner = &network.devices[0];
    owner.handle.close().await.unwrap();
    let handle = network
        .reopen(owner)
        .reset_device(owner.store, "Reset desktop")
        .await
        .unwrap();
    network.devices[0].handle = handle;
}

#[tokio::test]
async fn reset_revokes_open_account_invites_and_deletes_their_requests() {
    let mut network = Network::with_schema(CloudProvider::CloudKit, vec![], vec![]).await;
    let owner = &network.devices[0];
    let invite = owner
        .handle
        .create_invite(
            InviteTarget::NewMember {
                role: MemberRole::Member,
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.test".into(),
            },
        )
        .await
        .unwrap();
    let code = coven_sync::read_invite_code(&invite.code).unwrap();
    let coven_storage::InviteStorage::Account(invitation) =
        coven_storage::InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!("account invite")
    };
    let carol = coven_storage::test_utils::MemoryStorage::for_recipient(
        &owner.storage,
        "carol@example.test",
    )
    .unwrap();
    carol.join(owner.store, &invitation).await.unwrap();
    let request = ObjectPath::join_request(owner.store, invite.id);
    owner.handle.stop_sync();
    owner
        .handle
        .subscribe_sync_status()
        .wait_for(|state| matches!(state, SyncStatus::Stopped))
        .await
        .unwrap();
    carol.create(&request, b"unapproved request").await.unwrap();
    reset_owner(&mut network).await;
    network.devices[0].handle.start_sync().await.unwrap();
    network.sync(0).await;
    let owner = &network.devices[0];
    assert!(owner.storage.status(&request).await.unwrap().is_none());
    assert!(matches!(carol.read(&request).await,
        Err(error) if error.failure() == coven_storage::StorageFailure::PermissionDenied));
    assert!(owner.handle.pending().await.unwrap().is_empty());
    network.close().await;
}

#[tokio::test]
async fn reset_carries_an_open_invites_minted_key_until_confirmed_deletion() {
    let (mut network, made, invite) = invited_with_made_key().await;
    let code = coven_sync::read_invite_code(&invite.code).unwrap();
    let coven_storage::InviteStorage::S3 { credentials, .. } =
        coven_storage::InviteStorage::decode(code.storage.as_bytes()).unwrap()
    else {
        panic!("S3 invite")
    };
    let request = ObjectPath::join_request(network.devices[0].store, invite.id);
    network.devices[0].handle.stop_sync();
    network.devices[0]
        .handle
        .subscribe_sync_status()
        .wait_for(|state| matches!(state, SyncStatus::Stopped))
        .await
        .unwrap();
    network.devices[0]
        .storage
        .create(&request, b"unapproved request")
        .await
        .unwrap();
    reset_owner(&mut network).await;
    network.devices[0].handle.start_sync().await.unwrap();
    network.sync(0).await;
    let owner = &network.devices[0];
    let keys = pending_keys(&owner.handle.pending().await.unwrap());
    assert_eq!(keys.len(), 1);
    let (operation, key) = &keys[0];
    assert_eq!(key.access_key_id, credentials.access_key_id);
    assert!(owner.storage.status(&request).await.unwrap().is_none());
    assert!(matches!(key.principal, S3Principal::IamUser { .. }));
    let admin = owner
        .handle
        .admin_access(AdminAccessSource::Pasted(AdminCredential::AccessKey {
            access_key_id: "admin".into(),
            secret_access_key: SecretText::new("admin-secret".into()),
            session_token: None,
            iam_endpoint: None,
        }))
        .await
        .unwrap();
    made.fail_next_delete();
    assert!(matches!(
        owner
            .handle
            .delete_access_key(*operation, admin.clone())
            .await,
        Err(SyncError::AccessKeyNotDeleted(_))
    ));
    assert_eq!(pending_keys(&owner.handle.pending().await.unwrap()), keys);
    owner
        .handle
        .delete_access_key(*operation, admin)
        .await
        .unwrap();
    assert!(made.users().is_empty());
    assert!(pending_keys(&owner.handle.pending().await.unwrap()).is_empty());
    network.close().await;
}
