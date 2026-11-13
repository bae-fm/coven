use crate::sequence_sync::sequence_operations::Request;
use crate::sequence_sync::tests::*;
use crate::*;
use coven_crypto::SecretText;
use coven_format::{admin::StoreChange, MemberAccess};
use coven_storage::{S3Credentials, Storage};

fn key(id: &str) -> S3Credentials {
    S3Credentials {
        access_key_id: id.into(),
        secret_access_key: SecretText::new(format!("{id}-secret")),
    }
}

async fn recorded(device: &Device) -> MemberAccess {
    let state = device.sync.database.store_state().await.unwrap();
    state.active_member(&device.identity.member()).unwrap().access.clone().unwrap()
}

async fn slots(device: &Device) -> u64 {
    device.sync.database.applied_slot().await.unwrap()
}

/// A device that holds `ana-key` and whose store records it.
async fn ana(group: &Group) -> Device {
    let ana = group.device(1).await;
    ana.storage.set_s3_credentials(key("ana")).await.unwrap();
    group.create(&ana).await;
    ana
}

/// A call whose key can't be taken ends its entry without claiming: the
/// store, the record and the journal stay as they were (§14.3).
#[tokio::test]
async fn a_key_that_cannot_be_taken_ends_its_entry_without_claiming() {
    let group = Group::new();
    let ana = ana(&group).await;
    let before = slots(&ana).await;
    let failed = ana
        .sync
        .use_access_key("new".into(), async {
            Err(coven_storage::StorageFailure::InvalidConfiguration.with_source("refused").into())
        })
        .await;
    assert!(failed.is_err());
    assert_eq!(slots(&ana).await, before);
    assert_eq!(recorded(&ana).await, group.access("ana"));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}

/// A call whose key is taken claims its entry with that key.
#[tokio::test]
async fn a_key_that_is_taken_is_claimed_and_recorded() {
    let group = Group::new();
    let ana = ana(&group).await;
    let before = slots(&ana).await;
    let storage = ana.storage.clone();
    ana.sync
        .use_access_key("new".into(), async move {
            storage.set_s3_credentials(key("new")).await.map_err(SyncError::from)
        })
        .await
        .unwrap();
    assert_eq!(slots(&ana).await, before + 1);
    assert_eq!(recorded(&ana).await, group.access("new"));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    // The record names the key now: another call takes the key and journals
    // nothing.
    let storage = ana.storage.clone();
    ana.sync
        .use_access_key("new".into(), async move {
            storage.set_s3_credentials(key("new")).await.map_err(SyncError::from)
        })
        .await
        .unwrap();
    assert_eq!(slots(&ana).await, before + 1);
    ana.sync.database.close().await.unwrap();
}

/// An entry a killed call left journaled, before its key was taken, ends
/// without claiming when a pass runs it: the connection still signs with the
/// old key. Once the key is taken, the same entry claims (§14.3).
#[tokio::test]
async fn a_left_entry_ends_until_its_key_is_taken_and_then_claims() {
    let group = Group::new();
    let ana = ana(&group).await;
    let before = slots(&ana).await;
    let entry = || {
        Request::Change(StoreChange::SetAccess {
            member: ana.identity.member(),
            access: group.access("new"),
        })
    };
    ana.sync.start_request_row(entry(), "operation").await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(slots(&ana).await, before);
    assert_eq!(recorded(&ana).await, group.access("ana"));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());

    ana.sync.start_request_row(entry(), "operation").await.unwrap();
    ana.storage.set_s3_credentials(key("new")).await.unwrap();
    ana.sync.pass().await.unwrap();
    assert_eq!(slots(&ana).await, before + 1);
    assert_eq!(recorded(&ana).await, group.access("new"));
    assert!(ana.sync.database.operations().await.unwrap().is_empty());
    ana.sync.database.close().await.unwrap();
}
