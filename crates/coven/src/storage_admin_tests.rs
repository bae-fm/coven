//! Guided console steps from the facade, and what a reset carries of an
//! open invite (§20.10, §19.3).
use crate::tests::Network;
use crate::*;
use coven_storage::Storage;

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

async fn reset_owner(network: &mut Network) {
    let owner = &network.devices[0];
    owner.handle.close().await.unwrap();
    let handle = network
        .reopen(owner)
        .reset_device(owner.store, "Reset desktop", |_| ())
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
                name: "Carol".into(),
            },
            InviteAccess::ProviderAccount {
                email: "carol@example.test".into(),
            },
            crate::InviteDelivery::Sent {
                lifetime: crate::InviteLifetime::Day,
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
