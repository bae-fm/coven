use super::*;
use crate::test_utils::MemoryStorage;
use crate::test_utils::STORE_ID;
use coven_foundation::id_source::DeviceId;

#[tokio::test]
async fn replacement_tokens_cannot_change_dropbox_access_or_provider() {
    for location in [
        DropboxLocation::AppFolder,
        DropboxLocation::SharedFolder {
            namespace_id: "namespace".into(),
        },
    ] {
        let required = OAuthAccess::Dropbox(location.access());
        let storage = MemoryStorage::builder()
            .location(StorageConfig::Dropbox { location })
            .build()
            .unwrap();
        let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
        storage.create(&path, b"retained").await.unwrap();
        for access in [
            OAuthAccess::Dropbox(DropboxAccess::AppFolder),
            OAuthAccess::Dropbox(DropboxAccess::FullDropbox),
            OAuthAccess::GoogleDrive(GoogleDriveAccess::Full),
        ] {
            let result = storage
                .set_oauth_tokens(OAuthTokens {
                    client_id: "client".into(),
                    access,
                    access_token: coven_crypto::SecretText::new("token".into()),
                    refresh_token: None,
                    expires_at: None,
                })
                .await;
            if access == required {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().failure(),
                    StorageFailure::AccessNotGranted { required }
                );
            }
            assert_eq!(storage.read(&path).await.unwrap(), b"retained");
        }
    }
}

#[tokio::test]
async fn writer_discovery_never_reads_history() {
    let storage = MemoryStorage::builder().build().unwrap();
    let path = ObjectPath::store_log(STORE_ID, DeviceId(7), std::num::NonZeroU64::MIN);
    storage.create(&path, b"entry").await.unwrap();
    let before = storage.request_count();
    assert_eq!(storage.list_writers(STORE_ID).await.unwrap(), [DeviceId(7)]);
    assert_eq!(storage.request_count(), before + 1);
    assert!(matches!(
        storage.request_trace().last(),
        Some(crate::test_utils::MemoryRequest::Other("list_writers"))
    ));
}

#[tokio::test]
async fn store_log_entries_never_use_recorded_sessions() {
    let storage = MemoryStorage::builder()
        .transfer_limits(16, 4)
        .build()
        .unwrap();
    let path = ObjectPath::store_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    assert!(matches!(
        storage.begin_upload(&path, 4).await,
        Err(error) if error.failure() == StorageFailure::InvalidPath
    ));
    assert_eq!(storage.request_count(), 0);
    assert!(matches!(
        storage.create(&path, &[1; 17]).await,
        Err(error) if error.failure() == StorageFailure::SingleRequestTooLarge { size: 17, limit: 16 }
    ));
    assert_eq!(storage.request_count(), 0);
    storage.create(&path, &[1; 16]).await.unwrap();
    assert_eq!(storage.request_count(), 1);
    let other = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let session = storage.begin_upload(&other, 4).await.unwrap();
    let mut recorded: serde_json::Value =
        serde_json::from_slice(session.encode().unwrap().as_bytes()).unwrap();
    recorded["path"] = serde_json::Value::String(path.as_str().to_owned());
    assert!(matches!(
        UploadSession::decode(&serde_json::to_vec(&recorded).unwrap()),
        Err(error) if error.failure() == StorageFailure::InvalidPath
    ));
    storage.abort_upload(&session).await.unwrap();
}

#[tokio::test]
async fn clock_replacement_guards_preserve_the_previous_object() {
    let storage = MemoryStorage::builder()
        .transfer_limits(16, 4)
        .build()
        .unwrap();
    let path = ObjectPath::clock(STORE_ID, DeviceId(31));
    storage.replace(&path, b"clock").await.unwrap();
    let before = storage.request_count();
    assert_eq!(
        storage
            .replace(&path, &[0; 17])
            .await
            .unwrap_err()
            .failure(),
        StorageFailure::SingleRequestTooLarge {
            size: 17,
            limit: 16
        }
    );
    assert!(matches!(
        storage.begin_upload(&path, 4).await,
        Err(error) if error.failure() == StorageFailure::InvalidPath
    ));
    assert_eq!(storage.request_count(), before);
    assert_eq!(storage.read(&path).await.unwrap(), b"clock");
    let immutable = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let session = storage.begin_upload(&immutable, 4).await.unwrap();
    let mut recorded: serde_json::Value =
        serde_json::from_slice(session.encode().unwrap().as_bytes()).unwrap();
    recorded["path"] = serde_json::Value::String(path.as_str().to_owned());
    assert!(matches!(
        UploadSession::decode(&serde_json::to_vec(&recorded).unwrap()),
        Err(error) if error.failure() == StorageFailure::InvalidPath
    ));
    storage.abort_upload(&session).await.unwrap();
}

#[tokio::test]
async fn wrong_location_and_invalid_part_keep_the_provider_refusal_order() {
    for (config, expected) in [
        (
            StorageConfig::S3 {
                bucket: "bucket".into(),
                region: "region".into(),
                endpoint: None,
                profile: None,
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
                location: crate::DropboxLocation::SharedFolder {
                    namespace_id: "namespace".into(),
                },
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
        let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        session.location = StorageConfig::Dropbox {
            location: crate::DropboxLocation::SharedFolder {
                namespace_id: "another".into(),
            },
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
        .transfer_limits(16, 4)
        .build()
        .unwrap();
    let path = ObjectPath::device_log(STORE_ID, DeviceId(31), std::num::NonZeroU64::MIN);
    let positions = ObjectPath::positions(STORE_ID, DeviceId(31));
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
        location: crate::DropboxLocation::SharedFolder {
            namespace_id: "another".into(),
        },
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
