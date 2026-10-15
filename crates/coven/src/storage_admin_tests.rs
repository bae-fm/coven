//! Admin access from the facade: obtaining it, minting an invite's key with
//! it, deleting that key on evidence, and guided console steps (§4, §13).
use crate::tests::Network;
use crate::*;
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
async fn invited_with_made_key() -> (Network, coven_storage::test_utils::MemoryKeyAdmin) {
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
    owner
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
    (network, made)
}

/// T203 (KeyDelivery `reset_keeps_accounted`): the owner's copy is reset as a
/// stale copy (§10) while its invite's made key is still only in its journal.
/// The replacement installation lists that key as pending deletion, and
/// deleting it with admin access revokes it.
#[tokio::test]
async fn a_reset_keeps_an_invites_made_key_pending_deletion() {
    let (mut network, made) = invited_with_made_key().await;
    let device = network.devices.remove(0);
    device.handle.close().await.unwrap();
    let directory = device.layout.store_dir(&device.store);
    directory
        .record_device_reset(&coven_foundation::files::DeviceReset {
            old_device: directory.settings().unwrap().device_id,
            new_device: DeviceId(9000),
            reason: coven_foundation::files::DeviceResetReason::SlotMismatch,
        })
        .unwrap();
    let handle = network
        .reopen(&device)
        .reset_device(device.store, "Reset desktop")
        .await
        .unwrap();
    let pending = pending_keys(&handle.pending().await.unwrap());
    assert_eq!(pending.len(), 1);
    let (operation, key) = &pending[0];
    assert_eq!(key.member, None);
    assert!(matches!(&key.principal, S3Principal::IamUser { .. }));
    let admin = handle
        .admin_access(AdminAccessSource::Pasted(AdminCredential::AccessKey {
            access_key_id: "admin".into(),
            secret_access_key: SecretText::new("admin-secret".into()),
            session_token: None,
            iam_endpoint: None,
        }))
        .await
        .unwrap();
    handle.delete_access_key(*operation, admin).await.unwrap();
    assert!(made.users().is_empty());
    assert!(pending_keys(&handle.pending().await.unwrap()).is_empty());
    handle.close().await.unwrap();
    network.close().await;
}

/// A database too damaged to read carries nothing, whatever the reset's
/// reason: the reset still completes, leaving its made key to an admin's
/// provider listing (§13).
#[tokio::test]
async fn an_unreadable_discarded_database_resets_without_carrying_keys() {
    for recorded in [false, true] {
        let (mut network, made) = invited_with_made_key().await;
        let device = network.devices.remove(0);
        device.handle.close().await.unwrap();
        let directory = device.layout.store_dir(&device.store);
        let database = directory.database_path();
        for suffix in ["-wal", "-shm"] {
            let path = std::path::PathBuf::from(format!("{}{suffix}", database.display()));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        std::fs::write(&database, b"not a database").unwrap();
        // Without a recorded stale-copy reset the reset records damage itself.
        if recorded {
            directory
                .record_device_reset(&coven_foundation::files::DeviceReset {
                    old_device: directory.settings().unwrap().device_id,
                    new_device: DeviceId(9000),
                    reason: coven_foundation::files::DeviceResetReason::SlotMismatch,
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
