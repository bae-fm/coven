use super::*;
use crate::test_utils::MemoryStorage;
use coven_foundation::id_source::DeviceId;

#[tokio::test]
async fn wrong_location_and_invalid_part_keep_the_provider_refusal_order() {
    for (config, expected) in [
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "region".into(),
                endpoint: None,
                prefix: "store".into(),
            },
            StorageFailure::InvalidPart,
        ),
        (
            StorageConfig::GoogleDrive {
                folder_id: "folder".into(),
            },
            StorageFailure::InvalidPart,
        ),
        (
            StorageConfig::CloudKit {
                container: "container".into(),
                owner: "owner".into(),
                zone: "zone".into(),
            },
            StorageFailure::InvalidPart,
        ),
        (
            StorageConfig::Dropbox {
                namespace_id: "namespace".into(),
            },
            StorageFailure::SessionMismatch,
        ),
        (
            StorageConfig::OneDrive {
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            StorageFailure::SessionMismatch,
        ),
    ] {
        let storage = MemoryStorage::builder().location(config).build().unwrap();
        let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        session.location = StorageConfig::Dropbox {
            namespace_id: "another".into(),
        };
        let before = storage.request_count();
        assert_eq!(
            storage
                .upload_part(&mut session, &[])
                .await
                .unwrap_err()
                .failure(),
            expected
        );
        assert_eq!(storage.request_count(), before);
    }
}

#[tokio::test]
async fn guards_and_completed_sessions_do_not_issue_provider_calls() {
    let storage = MemoryStorage::builder()
        .provider(CloudProvider::Dropbox)
        .build()
        .unwrap();
    let path = ObjectPath::device_log(DeviceId(31), std::num::NonZeroU64::MIN);
    let positions = ObjectPath::positions(DeviceId(31));
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    let before = storage.request_count();
    for (error, expected) in [
        (
            storage.replace(&path, &[0; 17]).await.err().unwrap(),
            StorageFailure::InvalidPath,
        ),
        (
            storage.replace(&positions, &[0; 17]).await.err().unwrap(),
            StorageFailure::SingleRequestTooLarge {
                size: 17,
                limit: 16,
            },
        ),
        (
            storage.begin_upload(&positions, 4).await.err().unwrap(),
            StorageFailure::InvalidPath,
        ),
        (
            storage.begin_upload(&path, 0).await.err().unwrap(),
            StorageFailure::InvalidPart,
        ),
        (
            storage.upload_part(&mut session, &[]).await.err().unwrap(),
            StorageFailure::InvalidPart,
        ),
    ] {
        assert_eq!(error.failure(), expected);
    }
    let mut foreign = session.clone();
    foreign.location = StorageConfig::Dropbox {
        namespace_id: "another".into(),
    };
    for error in [
        storage.resume_upload(&mut foreign).await.err().unwrap(),
        storage
            .upload_part(&mut foreign, b"data")
            .await
            .err()
            .unwrap(),
        storage.finish_upload(&mut foreign).await.err().unwrap(),
        storage.abort_upload(&foreign).await.err().unwrap(),
        storage.restart_upload(&foreign).await.err().unwrap(),
    ] {
        assert_eq!(error.failure(), StorageFailure::SessionMismatch);
    }
    assert_eq!(storage.request_count(), before);
    storage.replace(&positions, &[0; 16]).await.unwrap();
    assert_eq!(storage.request_count(), before + 1);
    storage.upload_part(&mut session, b"data").await.unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    assert!(session.is_complete());
    let before = storage.request_count();
    storage.resume_upload(&mut session).await.unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    storage.abort_upload(&session).await.unwrap();
    assert_eq!(
        storage
            .upload_part(&mut session, b"data")
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::InvalidPart
    );
    assert_eq!(
        storage
            .restart_upload(&session)
            .await
            .err()
            .unwrap()
            .failure(),
        StorageFailure::InvalidPart
    );
    // Completion cannot bypass the connection's location check.
    session.location = foreign.location;
    assert_eq!(
        storage
            .finish_upload(&mut session)
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::SessionMismatch
    );
    assert_eq!(storage.request_count(), before);
}
