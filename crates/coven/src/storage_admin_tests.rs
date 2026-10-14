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
        }))
        .await
        .unwrap();
    let invite = owner
        .handle
        .create_invite(
            MemberRole::Member,
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
