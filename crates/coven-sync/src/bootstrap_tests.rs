use crate::store_log_sync::tests::{device, key, member, public, store};
use crate::SyncError;
use coven_database::DbError;
use coven_format::store_log::StoreChange;
use coven_storage::{test_utils::MemoryStorage, ObjectPrefix, Storage};
use std::sync::Arc;

#[tokio::test]
async fn conflicting_creation_identity_reserves_and_publishes_nothing() {
    for wrong_id in [false, true] {
        let storage = Arc::new(MemoryStorage::builder().build().unwrap());
        let mut founder = device(storage.clone(), 1, member(1), store(1)).await;
        let change = StoreChange::CreateStore {
            store: if wrong_id { store(2) } else { store(1) },
            name: if wrong_id { "Store" } else { "Another name" }.into(),
            admin: public(&founder.member),
            key: key(1),
            key_hash: founder.sync.test_key(coven_merge::Audience::Store, key(1)),
            access: coven_format::MemberAccess::S3AccessKey {
                access_key_id: "owner".into(),
                principal: coven_format::S3Principal::Console,
            },
            device_name: "Phone".into(),
        };
        let error = founder
            .sync
            .make_and_upload_entry(change)
            .await
            .unwrap_err();
        if wrong_id {
            assert!(matches!(error, SyncError::WrongStore { .. }), "{error:?}");
        } else {
            assert!(
                matches!(&error, SyncError::Database(DbError::StoreSettingsMismatch { store: id, name })
                    if *id == store(1) && name == "Another name"),
                "{error:?}"
            );
        }
        let local = founder.db.local_store_log().await.unwrap();
        assert!(local.upload.is_none());
        assert!(local.log.entries.is_empty());
        assert!(founder.db.key_uploads().await.unwrap().is_empty());
        assert!(storage
            .list(&ObjectPrefix::all(store(1)))
            .await
            .unwrap()
            .is_empty());
    }
}

// Settings cannot name another store's id (reading them refuses that), so a
// joining device can disagree with the stored creation only about the name.
#[tokio::test]
async fn bootstrap_rejects_a_conflicting_name_without_applying_creation() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut founder = device(storage.clone(), 1, member(1), store(1)).await;
    founder.create(key(1)).await;
    let mut joining = device(storage.clone(), 2, member(1), store(1)).await;
    joining
        .custody
        .persist(&founder.custody.read().unwrap().unwrap())
        .unwrap();
    let mut settings = joining.directory.settings().unwrap();
    settings.name = "Another name".into();
    let path = joining
        .directory
        .database_path()
        .parent()
        .unwrap()
        .join("settings.json");
    std::fs::write(&path, serde_json::to_vec(&settings).unwrap()).unwrap();
    joining.reopen(storage.clone(), vec![], vec![]).await;
    let error = joining
        .sync
        .bootstrap_member(crate::BindingSource::Restore)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, SyncError::Database(DbError::StoreSettingsMismatch { name, .. }) if name == "Store"),
        "{error:?}"
    );
    let log = joining.log().await;
    assert!(log.entries.is_empty());
    assert!(log.replay.state.store.is_none());
}

/// An interrupted registration is this installation's one registration (§10,
/// `Writers.RegistersOnce`): retrying the load publishes the queued entry and
/// makes no second one, even though replay keeps the first.
#[tokio::test]
async fn a_retried_load_publishes_the_queued_registration_only() {
    let storage = Arc::new(MemoryStorage::builder().build().unwrap());
    let mut ana = device(storage.clone(), 1, member(1), store(1)).await;
    let mut ben = device(storage.clone(), 2, member(2), store(1)).await;
    ana.create(key(1)).await;
    ana.add(&ben.member, coven_format::store_log::MemberRole::Member)
        .await;
    ben.sync().await;
    ben.interrupt_entry(1).await;
    assert!(ben.sync.load_new_device("Ben", None).await.is_err());
    assert!(ben.db.local_store_log().await.unwrap().upload.is_some());
    ben.catch_up().await;
    ben.sync.load_new_device("Ben", None).await.unwrap();
    let id = ben.device().await;
    let log = ben.log().await;
    let registrations: Vec<_> = log
        .entries
        .iter()
        .filter(|applied| {
            matches!(&applied.entry.change,
                StoreChange::AddDevice { device, .. } if *device == id)
        })
        .map(|applied| applied.entry.position)
        .collect();
    assert_eq!(registrations.len(), 1, "{registrations:?}");
    assert_eq!(registrations[0].number, 1);
    assert!(log.replay.state.devices.contains_key(&id));
}
