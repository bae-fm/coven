use super::*;
use crate::{sequence_sync::tests::Group, PendingRecord, SyncError};
use coven_database::{AccessKeyDeletion, AccessKeyToDelete, NewOperation};
use coven_format::S3Principal;
use coven_foundation::id_source::StoreId;
use coven_storage::{
    AccessKeyAdmin, ListedKey, MintScope, S3Credentials, S3MemberKey, StorageFailure,
};
use std::sync::Mutex;

struct Admin {
    deleted: Result<(), StorageFailure>,
    exists: Result<bool, StorageFailure>,
    calls: Mutex<Vec<(&'static str, String, S3Principal)>>,
}

#[async_trait::async_trait]
impl AccessKeyAdmin for Admin {
    async fn mint(&self, _: &MintScope) -> Result<S3MemberKey, StorageError> {
        panic!("deletion does not mint")
    }
    async fn probe(&self, _: &MintScope, _: &S3Credentials) -> Result<(), StorageError> {
        panic!("deletion does not probe")
    }
    async fn roll_back(&self, _: &MintScope) -> Result<(), StorageError> {
        panic!("deletion does not roll back a mint")
    }
    async fn list_keys(&self, _: StoreId) -> Result<Vec<ListedKey>, StorageError> {
        panic!("deletion reads only its exact key")
    }
    async fn revoke(&self, key: &str, principal: &S3Principal) -> Result<(), StorageError> {
        self.calls
            .lock()
            .unwrap()
            .push(("revoke", key.into(), principal.clone()));
        self.deleted.map_err(Into::into)
    }
    async fn key_exists(&self, key: &str, principal: &S3Principal) -> Result<bool, StorageError> {
        self.calls
            .lock()
            .unwrap()
            .push(("key_exists", key.into(), principal.clone()));
        self.exists.map_err(Into::into)
    }
}

#[tokio::test]
async fn only_a_successful_delete_and_absent_read_clear_the_durable_obligation() {
    for (deleted, exists) in [
        (Err(StorageFailure::PermissionDenied), Ok(false)),
        (Ok(()), Err(StorageFailure::Network)),
        (Ok(()), Ok(true)),
        (Ok(()), Ok(false)),
    ] {
        let group = Group::new();
        let ana = group.device(1).await;
        let key = AccessKeyToDelete {
            access_key_id: "old-key".into(),
            principal: S3Principal::IamUser {
                user: "old-user".into(),
                policy: coven_format::IamPolicy::Inline {
                    name: "member-policy".into(),
                },
            },
            member: Some(ana.identity.member()),
        };
        let record = || AccessKeyDeletion {
            operation: NewOperation {
                kind: "delete-access-key".into(),
                data: serde_json::to_vec(&Deletion { key: key.clone() }).unwrap(),
                started_by: "coven".into(),
                pauses_writes: false,
            },
            key: key.clone(),
        };
        ana.database()
            .record_access_key_deletions::<PendingRecord>(None, vec![record()])
            .await
            .unwrap();
        let id = ana.database().operations().await.unwrap()[0].id;
        let admin = Admin {
            deleted,
            exists,
            calls: Mutex::new(Vec::new()),
        };
        let result = ana.sync.delete_access_key(id, &admin).await;
        match (deleted, exists, result) {
            (
                Err(expected),
                _,
                Err(SyncError::AccessKeyNotDeleted(DeleteAccessKeyError::DeleteFailed(error))),
            )
            | (
                Ok(()),
                Err(expected),
                Err(SyncError::AccessKeyNotDeleted(DeleteAccessKeyError::ReadFailed(error))),
            ) => {
                assert_eq!(error.failure(), expected);
            }
            (
                Ok(()),
                Ok(true),
                Err(SyncError::AccessKeyNotDeleted(DeleteAccessKeyError::StillPresent)),
            )
            | (Ok(()), Ok(false), Ok(())) => (),
            other => panic!("unexpected deletion result: {other:?}"),
        }
        let mut expected = vec![("revoke", key.access_key_id.clone(), key.principal.clone())];
        if deleted.is_ok() {
            expected.push((
                "key_exists",
                key.access_key_id.clone(),
                key.principal.clone(),
            ));
        }
        assert_eq!(*admin.calls.lock().unwrap(), expected);
        let cleared = deleted.is_ok() && exists == Ok(false);
        assert_eq!(
            ana.database().operation(id).await.unwrap().is_none(),
            cleared
        );
        assert_eq!(
            ana.database()
                .pending::<PendingRecord>()
                .await
                .unwrap()
                .is_empty(),
            cleared
        );
        let ana = group.reopen(ana).await;
        ana.database()
            .record_access_key_deletions::<PendingRecord>(None, vec![record()])
            .await
            .unwrap();
        assert_eq!(
            ana.database().operation(id).await.unwrap().is_none(),
            cleared
        );
        assert_eq!(
            ana.database()
                .pending::<PendingRecord>()
                .await
                .unwrap()
                .is_empty(),
            cleared
        );
        if !cleared {
            let admin = Admin {
                deleted: Ok(()),
                exists: Ok(false),
                calls: Mutex::new(Vec::new()),
            };
            ana.sync.delete_access_key(id, &admin).await.unwrap();
            assert!(ana.database().operation(id).await.unwrap().is_none());
            assert!(ana
                .database()
                .pending::<PendingRecord>()
                .await
                .unwrap()
                .is_empty());
        }
        ana.database().close().await.unwrap();
    }
}
