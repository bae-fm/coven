use crate::sequence_sync::tests::*;
use crate::*;
use coven_format::admin::StoreChange;
use coven_storage::Storage;
use std::sync::Arc;

// §13 and E9: a delivery is authenticated by the applied entry recording its key.
#[tokio::test]
async fn a_delivery_uses_the_signing_key_at_its_recorded_slot() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let replacement = ana
        .sync
        .replace_member_access_key(ben.identity.member(), Arc::new(group.storage.key_admin()))
        .await
        .unwrap();
    ana.sync.replace_member_keys().await.unwrap();
    ben.sync.pass().await.unwrap();
    let delivered = ben.sync.delivered_access_key(Some("ben")).await.unwrap();
    assert_eq!(
        delivered.map(|key| key.access_key_id),
        Some(replacement.key.credentials.access_key_id)
    );
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_admin_replacement_delivers_records_and_leaves_the_console_key_owed() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let admin = Arc::new(group.storage.key_admin());
    let replaced = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone())
        .await
        .unwrap();
    let OldKeyDeletion::Console { operation } = replaced.old_key else {
        panic!("a console key is deleted in the console");
    };
    assert_eq!(admin.calls(), ["mint", "probe"]);
    let deletion: crate::sequence_sync::access_keys::Deletion = serde_json::from_slice(
        &ana.sync
            .database
            .operation(operation)
            .await
            .unwrap()
            .unwrap()
            .data,
    )
    .unwrap();
    assert_eq!(deletion.key.access_key_id, "ben");
    ben.sync.pass().await.unwrap();
    let delivered = ben
        .sync
        .delivered_access_key(Some("ben"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        delivered.access_key_id,
        replaced.key.credentials.access_key_id
    );
    assert!(ben
        .sync
        .delivered_access_key(Some(&delivered.access_key_id))
        .await
        .unwrap()
        .is_none());
    for device in [&ana, &ben] {
        device.sync.database.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_replacement_invalidated_by_contention_undoes_its_key_and_delivery() {
    let group = Group::new();
    let ana = group.device(1).await;
    let ben = group.device(2).await;
    let carol = group.device(3).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    group.admit(&ana, &carol).await;
    carol.sync.pass().await.unwrap();
    let admin = Arc::new(group.storage.key_admin());
    let (delivery_started, delivery_observed) = tokio::sync::oneshot::channel();
    let (delivery_resume, delivery_held) = tokio::sync::oneshot::channel();
    group
        .storage
        .hold_next_creation(
            coven_storage::ObjectPrefix::all(group.store),
            delivery_started,
            delivery_held,
        )
        .await;
    let replace = ana
        .sync
        .replace_member_access_key(ben.identity.member(), admin.clone());
    let remove = async {
        delivery_observed.await.unwrap();
        let (claim_started, claim_observed) = tokio::sync::oneshot::channel();
        let (claim_resume, claim_held) = tokio::sync::oneshot::channel();
        group
            .storage
            .hold_next_creation(
                coven_storage::ObjectPrefix::all(group.store),
                claim_started,
                claim_held,
            )
            .await;
        delivery_resume.send(()).unwrap();
        claim_observed.await.unwrap();
        carol
            .sync
            .administer(StoreChange::RemoveMember {
                member: ben.identity.member(),
            })
            .await
            .unwrap();
        claim_resume.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(replace, remove);
    assert!(
        matches!(
            &result,
            Err(SyncError::Rejected(OperationRejection::TargetGone(_)))
        ),
        "{result:?}"
    );
    assert_eq!(admin.calls(), ["mint", "probe", "revoke", "key_exists"]);
    let deliveries = group
        .storage
        .list(&coven_storage::ObjectPrefix::access_keys(
            group.store,
            &ben.identity.member(),
        ))
        .await
        .unwrap();
    assert!(deliveries.is_empty(), "undo deletes its delivery");
    assert!(ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .iter()
        .all(|record| record.kind != "replace-access-key"));
    for device in [&ana, &ben, &carol] {
        device.sync.database.close().await.unwrap();
    }
}

use coven_database::Database;
use coven_foundation::id_source::StoreId;
use coven_storage::{
    test_utils::MemoryKeyAdmin, AccessKeyAdmin, ListedKey, MintScope, S3Credentials, S3MemberKey,
    StorageError, StorageFailure,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};

struct InterruptedMint {
    database: Database,
    provider: MemoryKeyAdmin,
    kind: &'static str,
    fail_undo: AtomicBool,
    observations: Mutex<Vec<(MintScope, MintScope)>>,
}

#[async_trait::async_trait]
impl AccessKeyAdmin for InterruptedMint {
    async fn mint(&self, scope: &MintScope) -> Result<S3MemberKey, StorageError> {
        let record = self
            .database
            .operations()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.kind == self.kind)
            .unwrap();
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        let recorded = if self.kind == "invite" {
            &data["access"]["Minting"]
        } else {
            &data["scope"]
        };
        self.observations.lock().unwrap().push((
            scope.clone(),
            serde_json::from_value(recorded.clone()).unwrap(),
        ));
        self.provider.mint(scope).await
    }
    async fn probe(&self, scope: &MintScope, key: &S3Credentials) -> Result<(), StorageError> {
        self.provider.probe(scope, key).await
    }
    async fn roll_back(&self, scope: &MintScope) -> Result<(), StorageError> {
        if self.fail_undo.swap(false, Ordering::SeqCst) {
            return Err(StorageFailure::PermissionDenied.into());
        }
        self.provider.roll_back(scope).await
    }
    async fn revoke(
        &self,
        key: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<(), StorageError> {
        self.provider.revoke(key, principal).await
    }
    async fn key_exists(
        &self,
        key: &str,
        principal: &coven_format::S3Principal,
    ) -> Result<bool, StorageError> {
        self.provider.key_exists(key, principal).await
    }
    async fn list_keys(&self, store: StoreId) -> Result<Vec<ListedKey>, StorageError> {
        self.provider.list_keys(store).await
    }
}

// §13, §18: record the exact artifact before creation; retry undoes that name.
#[tokio::test]
async fn uncertain_mints_are_recorded_before_creation_and_undone_after_reopening() {
    for kind in ["invite", "replace-access-key"] {
        interrupted_mint(kind, false).await;
    }
}

// §12.2, §18: cancellation cannot discard an uncertain grant.
#[tokio::test]
async fn cancelling_an_uncertain_invite_keeps_its_name_until_undo_succeeds() {
    interrupted_mint("invite", true).await;
}

async fn interrupted_mint(kind: &'static str, cancel: bool) {
    let group = Group::new();
    let mut ana = group.device(1).await;
    Arc::get_mut(&mut ana.sync).unwrap().ids = Arc::new(coven_foundation::id_source::UuidIds);
    let ben = group.device(2).await;
    group.create(&ana).await;
    group.admit(&ana, &ben).await;
    let provider = group.storage.key_admin();
    provider.fail_next_mint();
    let admin = Arc::new(InterruptedMint {
        database: ana.sync.database.clone(),
        provider: provider.clone(),
        kind,
        fail_undo: AtomicBool::new(true),
        observations: Mutex::new(Vec::new()),
    });
    let result = if kind == "invite" {
        ana.sync
            .create_invite(
                InviteTarget::NewMember {
                    role: coven_format::admin::MemberRole::Member,
                },
                InviteAccess::S3AdminAccess(admin.clone()),
            )
            .await
            .map(drop)
    } else {
        ana.sync
            .replace_member_access_key(ben.identity.member(), admin.clone())
            .await
            .map(drop)
    };
    assert!(
        matches!(result, Err(SyncError::Cleanup { .. })),
        "{result:?}"
    );
    let (minted, recorded) = admin.observations.lock().unwrap()[0].clone();
    assert_eq!(
        minted, recorded,
        "the provider must receive the journaled name"
    );
    assert_eq!(provider.users(), std::slice::from_ref(&minted.key_name));
    let record = ana
        .sync
        .database
        .operations()
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.kind == kind)
        .unwrap();
    let mut ana = group.reopen(ana).await;
    Arc::get_mut(&mut ana.sync).unwrap().ids = Arc::new(coven_foundation::id_source::UuidIds);
    if cancel {
        let data: serde_json::Value = serde_json::from_slice(&record.data).unwrap();
        let invite = serde_json::from_value(data["id"].clone()).unwrap();
        assert!(matches!(
            ana.sync.cancel_invite(invite).await,
            Err(SyncError::Pending(_))
        ));
        assert!(ana
            .sync
            .database
            .operation(record.id)
            .await
            .unwrap()
            .is_some());
    }
    let resumed = Arc::new(InterruptedMint {
        database: ana.sync.database.clone(),
        provider: provider.clone(),
        kind,
        fail_undo: AtomicBool::new(false),
        observations: Mutex::new(Vec::new()),
    });
    ana.sync
        .retry_with_admin_access(record.id, resumed.clone())
        .await
        .unwrap();
    assert!(
        !provider.users().contains(&minted.key_name),
        "undo must delete the recorded principal"
    );
    if cancel {
        assert!(provider.users().is_empty());
        assert!(ana
            .sync
            .database
            .operation(record.id)
            .await
            .unwrap()
            .is_none());
        assert!(resumed.observations.lock().unwrap().is_empty());
    } else {
        let observations = resumed.observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].0, observations[0].1);
        assert_ne!(
            observations[0].0, minted,
            "a retry mints under a fresh name"
        );
        assert_eq!(provider.users(), [observations[0].0.key_name.clone()]);
    }
    ana.sync.database.close().await.unwrap();
    ben.sync.database.close().await.unwrap();
}
