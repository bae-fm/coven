use super::*;
fn config() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: None,
        prefix: "store".into(),
    }
}
#[tokio::test]
async fn memory_conforms() {
    Conformance::new(Arc::new(
        MemoryStorage::new(
            config(),
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap(),
    ))
    .run()
    .await
    .unwrap();
}
#[tokio::test]
async fn crash_after_accepted_part_and_dropped_part_resumes() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::file(coven_foundation::id_source::FileId(uuid::Uuid::from_bytes(
        [0xaa; 16],
    )));
    let mut session = provider.begin_upload(&path, 10).await.unwrap();
    provider.upload_part(&mut session, b"abcd").await.unwrap();
    let recorded = session.encode().unwrap();
    provider
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    assert!(provider.upload_part(&mut session, b"efgh").await.is_err());
    assert_eq!(session.confirmed_bytes(), 4);
    drop(session);
    let reopened = provider.clone();
    let mut session = UploadSession::decode(recorded.as_bytes()).unwrap();
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened
        .set_faults(Faults {
            drop_part: true,
            ..Faults::none()
        })
        .await;
    assert!(reopened.upload_part(&mut session, b"ij").await.is_err());
    reopened.resume_upload(&mut session).await.unwrap();
    assert_eq!(session.confirmed_bytes(), 8);
    reopened.upload_part(&mut session, b"ij").await.unwrap();
    let before_finish = session.encode().unwrap();
    reopened.finish_upload(&mut session).await.unwrap();
    let mut lost_finish = UploadSession::decode(before_finish.as_bytes()).unwrap();
    reopened.finish_upload(&mut lost_finish).await.unwrap();
    assert_eq!(reopened.read(&path).await.unwrap(), b"abcdefghij");
}
#[tokio::test]
async fn setup_refuses_other_store_and_retries_its_own_entry() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::store_log(
        coven_foundation::id_source::DeviceId(1),
        std::num::NonZeroU64::MIN,
    );
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    provider
        .setup(&path, b"encrypted first entry")
        .await
        .unwrap();
    assert_eq!(
        provider
            .setup(&path, b"other store")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
}
#[tokio::test(start_paused = true)]
async fn faults_are_counted_and_delay_is_awaited() {
    let provider = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    provider
        .set_faults(Faults {
            fail_next: 2,
            failure: StorageFailure::QuotaExceeded,
            delay: Duration::from_secs(3),
            ..Faults::none()
        })
        .await;
    let start = tokio::time::Instant::now();
    for _ in 0..2 {
        assert_eq!(
            provider
                .list(&ObjectPrefix::all())
                .await
                .unwrap_err()
                .failure(),
            StorageFailure::QuotaExceeded
        );
    }
    assert!(provider
        .list(&ObjectPrefix::all())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(start.elapsed(), Duration::from_secs(9));
}

#[tokio::test]
async fn expired_upload_restarts_with_a_new_recording_at_the_same_destination() {
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    let mut expired = storage.begin_upload(&path, 5).await.unwrap();
    storage.upload_part(&mut expired, b"abcd").await.unwrap();
    storage.abort_upload(&expired).await.unwrap();
    assert!(matches!(
        storage.resume_upload(&mut expired).await,
        Err(StorageError::SessionExpired)
    ));
    let replacement = storage.restart_upload(&expired).await.unwrap();
    assert_eq!(replacement.path(), &path);
    assert_eq!(replacement.total_bytes(), 5);
    assert_eq!(replacement.confirmed_bytes(), 0);
    assert_ne!(
        replacement.encode().unwrap().as_bytes(),
        expired.encode().unwrap().as_bytes()
    );
    let mut replacement = UploadSession::decode(replacement.encode().unwrap().as_bytes()).unwrap();
    storage
        .upload_part(&mut replacement, b"abcd")
        .await
        .unwrap();
    storage.upload_part(&mut replacement, b"e").await.unwrap();
    storage.finish_upload(&mut replacement).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), b"abcde");
    assert!(matches!(
        storage.restart_upload(&replacement).await,
        Err(StorageError::InvalidPart)
    ));
    let other = MemoryStorage::new(
        StorageConfig::Dropbox {
            namespace_id: "elsewhere".into(),
        },
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    assert!(matches!(
        other.restart_upload(&expired).await,
        Err(StorageError::SessionMismatch)
    ));
}

#[tokio::test]
async fn failed_automatic_upload_is_aborted_without_publishing() {
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    storage
        .set_faults(Faults {
            lose_part_reply: true,
            ..Faults::none()
        })
        .await;
    let path = ObjectPath::device_log(
        coven_foundation::id_source::DeviceId(31),
        std::num::NonZeroU64::MIN,
    );
    assert_eq!(
        storage.create(&path, &[1; 17]).await.unwrap_err().failure(),
        StorageFailure::Network
    );
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    assert!(storage.state.lock().await.uploads.is_empty());
    storage.create(&path, &[1; 17]).await.unwrap();
    assert_eq!(storage.read(&path).await.unwrap(), [1; 17]);
}

#[tokio::test]
async fn sharing_authority_belongs_to_the_adapters_account() {
    for config in [
        StorageConfig::GoogleDrive {
            folder_id: "folder".into(),
        },
        StorageConfig::Dropbox {
            namespace_id: "namespace".into(),
        },
        StorageConfig::OneDrive {
            drive_id: "drive".into(),
            folder_id: "folder".into(),
        },
        StorageConfig::CloudKit {
            container: "container".into(),
            owner: "owner".into(),
            zone: "zone".into(),
        },
    ] {
        let owner = MemoryStorage::new(
            config,
            Arc::new(coven_foundation::clock::FixedClock::new(
                std::time::SystemTime::UNIX_EPOCH,
            )),
        )
        .unwrap();
        owner.grant_access("kept@example.test").await.unwrap();
        let mut recipient = owner.clone();
        recipient.set_owner(false);
        assert!(matches!(
            recipient.grant_access("new@example.test").await,
            Err(StorageError::NotStoreOwner)
        ));
        assert!(matches!(
            recipient
                .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
                .await,
            Err(StorageError::NotStoreOwner)
        ));
        assert_eq!(
            owner
                .state
                .lock()
                .await
                .accounts
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            ["kept@example.test"]
        );
        owner
            .revoke_access(&MemberAccess::ProviderAccount("kept@example.test".into()))
            .await
            .unwrap();
        assert!(owner.state.lock().await.accounts.is_empty());
    }
    let mut s3 = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    s3.set_owner(false);
    assert!(matches!(
        s3.grant_access("member").await.unwrap(),
        AccessGrant::CreateAccessKey
    ));
    assert!(matches!(
        s3.revoke_access(&MemberAccess::S3AccessKey {
            access_key_id: "key".into()
        })
        .await
        .unwrap(),
        MemberRemoval::DeleteAccessKey { .. }
    ));
}

#[tokio::test]
async fn reconnect_accepts_its_first_entry_after_the_store_has_uploaded_more() {
    use coven_foundation::id_source::DeviceId;
    let storage = MemoryStorage::new(
        config(),
        Arc::new(coven_foundation::clock::FixedClock::new(
            std::time::SystemTime::UNIX_EPOCH,
        )),
    )
    .unwrap();
    let first = ObjectPath::store_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.setup(&first, b"first").await.unwrap();
    let later = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    storage.create(&later, b"later").await.unwrap();
    storage
        .replace(&ObjectPath::positions(DeviceId(32)), b"positions")
        .await
        .unwrap();
    assert_eq!(storage.setup(&first, b"first").await.unwrap(), config());
    assert_eq!(
        storage
            .setup(&first, b"different")
            .await
            .unwrap_err()
            .failure(),
        StorageSetupFailure::LocationOccupied
    );
    let other = ObjectPath::store_log(DeviceId(32), std::num::NonZeroU64::MIN);
    assert_eq!(
        storage.setup(&other, b"other").await.unwrap_err().failure(),
        StorageSetupFailure::LocationOccupied
    );
    assert_eq!(storage.read(&first).await.unwrap(), b"first");
    assert_eq!(storage.read(&later).await.unwrap(), b"later");
}

#[tokio::test]
async fn listing_records_publication_time_and_keeps_it_on_retry() {
    use coven_foundation::{clock::FixedClock, id_source::DeviceId};
    use std::time::{Duration, SystemTime};
    let started = SystemTime::UNIX_EPOCH + Duration::from_secs(10);
    let published = started + Duration::from_secs(20);
    let clock = Arc::new(FixedClock::new(started));
    let storage = MemoryStorage::new(config(), clock.clone()).unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let mut upload = storage.begin_upload(&path, 4).await.unwrap();
    storage.upload_part(&mut upload, b"data").await.unwrap();
    assert!(storage.list(&ObjectPrefix::all()).await.unwrap().is_empty());
    clock.set(published);
    storage.finish_upload(&mut upload).await.unwrap();
    clock.set(published + Duration::from_secs(30));
    storage.create_once(&path, b"data").await.unwrap();
    assert_eq!(
        storage.list(&ObjectPrefix::all()).await.unwrap(),
        [StoredObject {
            path,
            size: 4,
            stored_at: published
        }]
    );
    let positions = ObjectPath::positions(DeviceId(31));
    storage.replace(&positions, b"old").await.unwrap();
    clock.set(published + Duration::from_secs(40));
    storage.replace(&positions, b"new position").await.unwrap();
    let listed = storage.list(&ObjectPrefix::positions()).await.unwrap();
    assert_eq!(listed[0].stored_at, published + Duration::from_secs(40));
    assert_eq!(listed[0].size, 12);
}
