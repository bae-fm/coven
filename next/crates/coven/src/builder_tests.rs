use crate::*;
use coven_crypto::custody::{InMemoryCustody, StoreKeyCustody};
use coven_database::DatabaseBuilder;
use coven_format::store_log::{MemberPublicKeys, StoreChange};
use coven_storage::test_utils::MemoryStorage;
use coven_sync::StoreLogSync;
use std::sync::Arc;
use std::{future::Future, task::Poll, time::UNIX_EPOCH};

fn tables() -> Vec<SyncedTable> {
    vec![
        SyncedTable::new("attachments", RowIdentity::SharedKey).carries_files(FileDecl::new(
            "attachments",
            Provenance::AppProvided,
            Uploads::WhenAsked,
            CacheFill::CacheLazy,
        )),
    ]
}

fn migrations() -> Vec<Migration> {
    vec![Migration::sql(
        1,
        "attachments",
        "CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,size INTEGER,hash BLOB,location TEXT)",
    )]
}

#[tokio::test]
async fn app_reopening_resumes_operations_and_files_using_one_storage_capability() {
    let root = tempfile::tempdir().unwrap();
    let layout = StoreLayout::new(root.path().into());
    let app = TestCoven::new();
    let ids = Arc::new(UuidIds);
    let directory = app
        .create_store(&layout, "Household", ids.clone())
        .await
        .unwrap();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH));
    let storage = Arc::new(
        MemoryStorage::new(
            StorageConfig::S3 {
                bucket: "test".into(),
                region: "us-east-1".into(),
                endpoint: None,
                prefix: "household".into(),
            },
            clock.clone(),
        )
        .unwrap()
        .with_transfer_limits(65536, 65536)
        .unwrap(),
    );
    let member = MemberKeys::generate().unwrap();
    let identity = Arc::new(InMemoryCustody::new(member.clone()));
    let store_key = KeyId(ids.new_id());
    let keys = Arc::new(InMemoryCustody::new(StoreKeyring::new(
        StoreKey::generate(store_key).unwrap(),
    )));
    let db = DatabaseBuilder::new(directory.clone())
        .synced_tables(tables())
        .migrations(migrations())
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .clock(clock.clone())
        .open()
        .await
        .unwrap();
    let mut sync = StoreLogSync::new(
        storage.clone(),
        db.clone(),
        keys.clone(),
        identity.clone(),
        clock.clone(),
        ids.clone(),
    );
    sync.make_and_upload_entry(StoreChange::CreateStore {
        store: directory.id(),
        name: "Household".into(),
        admin: MemberPublicKeys {
            signing: member.member_id(),
            sealing: member.sealing_public_key(),
        },
        access: coven_format::MemberAccess::S3AccessKey {
            access_key_id: "owner-key".into(),
        },
        device_name: "Owner".into(),
        key: store_key,
    })
    .await
    .unwrap();
    db.close().await.unwrap();
    drop(sync);
    let open = || {
        app.builder(directory.clone())
            .synced_tables(tables())
            .migrations(migrations())
            .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
            .key_custody(KeyCustody::Custom(keys.clone()))
            .identity_custody(IdentityCustody::Custom(identity.clone()))
            .clock(clock.clone())
    };
    let handle = open().open().await.unwrap();
    handle.set_uploads_paused(true);
    handle
        .write_with_files(
            |batch| {
                batch.put_file("attachments", "shared", vec![42; 200_000]);
                Ok(())
            },
            |sql| {
                sql.execute("INSERT INTO attachments(id) VALUES('shared')", [])?;
                Ok(())
            },
        )
        .await
        .unwrap();
    let local = handle.file_ref("attachments", "shared").await.unwrap();
    handle
        .upload_files(std::slice::from_ref(&local))
        .await
        .unwrap();
    let members = handle.get_members().await.unwrap();
    assert_eq!(members.len(), 1);
    assert!(members[0].is_self);
    let circles = handle.circles();
    let mut waiting = Box::pin(circles.create("While offline"));
    std::future::poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    handle.get_members().await.unwrap(); // The preceding call's intent has committed.
    drop(waiting);
    handle.close().await.unwrap();
    let handle = open().storage(storage.clone()).open().await.unwrap();
    let mut uploads = handle.subscribe_uploads();
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let queue = uploads.next().await.unwrap();
            if queue.files.is_empty() {
                break;
            }
            assert!(queue.files.iter().all(|file| file.last_failure.is_none()));
        }
    })
    .await
    .unwrap();
    let file = handle.file_ref("attachments", "shared").await.unwrap();
    assert_eq!(file.location(), FileLocation::Uploaded);
    assert_eq!(handle.read_file(&file).await.unwrap(), vec![42; 200_000]);
    let requests = storage.request_count();
    let stream = handle.open_file_stream(&file).await.unwrap();
    assert_eq!(stream.read_at(70_000, 37).await.unwrap(), vec![42; 37]);
    assert_eq!(storage.request_count(), requests);
    assert!(matches!(
        handle.upload_files(&[local]).await,
        Err(OperationError::Database(DbError::FileRefChanged { .. }))
    ));
    assert!(matches!(
        handle.retry_blocked_operation(OperationId(-1)).await,
        Err(OperationError::NotBlocked(OperationId(-1)))
    ));
    handle.get_members().await.unwrap();
    let circle = handle.circles().list().await.unwrap().remove(0);
    assert_eq!(circle.name, "While offline");
    handle.circles().rename(circle.id, "Family").await.unwrap();
    assert!(handle.circles().members(circle.id).await.unwrap()[0].is_self);
    let invite = handle
        .create_invite(
            MemberRole::Member,
            InviteAccess::S3AccessKey {
                access_key_id: "invited-key".into(),
                secret_access_key: SecretText::new("invited-secret".into()),
            },
        )
        .await
        .unwrap();
    let joins = handle.subscribe_join_requests();
    assert!(joins.borrow().is_empty());
    handle.cancel_invite(&invite.id).await.unwrap();
    handle
        .confirm_access_key_deleted("invited-key")
        .await
        .unwrap();
    assert!(matches!(
        handle
            .set_member_role(&member.member_id(), MemberRole::Member)
            .await,
        Err(SyncError::LastAdmin)
    ));
    assert!(keys
        .unlock()
        .unwrap()
        .unwrap()
        .circle_key_ids(circle.id)
        .next()
        .is_some());
    handle.circles().delete(circle.id).await.unwrap();
    assert!(handle.circles().list().await.unwrap().is_empty());
    handle.close().await.unwrap();
}
