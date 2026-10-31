use super::*;
use crate::test_utils::MemoryStorage;
use crate::test_utils::STORE_ID;

#[tokio::test]
async fn replacement_tokens_cannot_change_dropbox_access_or_provider() {
    for location in [
        DropboxLocation::AppFolder,
        DropboxLocation::SharedFolder { namespace_id: "namespace".into() },
    ] {
        let required = OAuthAccess::Dropbox(location.access());
        let storage = MemoryStorage::builder()
            .location(StorageConfig::Dropbox { account_label: None, location })
            .build()
            .unwrap();
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        storage.create(&path, b"retained").await.unwrap();
        for access in [
            OAuthAccess::Dropbox(DropboxAccess::AppFolder),
            OAuthAccess::Dropbox(DropboxAccess::FullDropbox),
            OAuthAccess::OneDrive,
        ] {
            let result = storage
                .set_oauth_tokens(OAuthTokens {
                    client_id: "client".into(),
                    access,
                    access_token: coven_crypto::SecretText::new("token".into()),
                    refresh_token: None,
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
async fn floor_replacement_guards_preserve_the_previous_object() {
    let storage = MemoryStorage::builder().transfer_limits(16, 4).build().unwrap();
    let path = ObjectPath::floor(STORE_ID);
    storage.create(&path, b"floor").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let before = storage.request_count();
    assert_eq!(
        storage.replace(&previous, &[0; 17]).await.unwrap_err().failure(),
        StorageFailure::SingleRequestTooLarge { size: 17, limit: 16 }
    );
    assert_eq!(storage.request_count(), before);
    assert_eq!(storage.read(&path).await.unwrap(), b"floor");
    let immutable = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&immutable, b"slot").await.unwrap();
    let immutable = storage.status(&immutable).await.unwrap().unwrap();
    assert!(matches!(storage.begin_replace(&immutable, 4).await,
        Err(error) if error.failure() == StorageFailure::InvalidPath));
    let session = storage.begin_replace(&previous, 4).await.unwrap();
    let mut recorded: serde_json::Value =
        serde_json::from_slice(session.encode().unwrap().as_bytes()).unwrap();
    recorded["target"]["Replace"]["path"] = serde_json::json!(immutable.path);
    assert!(matches!(UploadSession::decode(&serde_json::to_vec(&recorded).unwrap()),
        Err(error) if error.failure() == StorageFailure::InvalidPath));
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
                prefix: crate::S3Prefix::parse("store").unwrap(),
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
                account_label: None,
                location: crate::DropboxLocation::SharedFolder { namespace_id: "namespace".into() },
            },
            StorageFailure::SessionMismatch,
        ),
        (
            StorageConfig::OneDrive {
                account_label: None,
                drive_id: "drive".into(),
                folder_id: "folder".into(),
            },
            StorageFailure::SessionMismatch,
        ),
    ] {
        let storage = MemoryStorage::builder().location(config).build().unwrap();
        let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
        let mut session = storage.begin_upload(&path, 4).await.unwrap();
        session.location = StorageConfig::Dropbox {
            account_label: None,
            location: crate::DropboxLocation::SharedFolder { namespace_id: "another".into() },
        };
        let before = storage.request_count();
        assert_eq!(storage.upload_part(&mut session, &[]).await.unwrap_err().failure(), expected);
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
    let path = ObjectPath::slot(STORE_ID, std::num::NonZeroU64::MIN);
    storage.create(&path, b"old slot").await.unwrap();
    let previous = storage.status(&path).await.unwrap().unwrap();
    let floor = ObjectPath::floor(STORE_ID);
    storage.create(&floor, b"old floor").await.unwrap();
    let floor_state = storage.status(&floor).await.unwrap().unwrap();
    storage.delete(&path).await.unwrap();
    let mut session = storage.begin_upload(&path, 4).await.unwrap();
    let before = storage.request_count();
    for (error, expected) in [
        (storage.replace(&previous, &[0; 17]).await.err().unwrap(), StorageFailure::InvalidPath),
        (
            storage.replace(&floor_state, &[0; 17]).await.err().unwrap(),
            StorageFailure::SingleRequestTooLarge { size: 17, limit: 16 },
        ),
        (storage.begin_replace(&previous, 4).await.err().unwrap(), StorageFailure::InvalidPath),
        (storage.begin_upload(&path, 0).await.err().unwrap(), StorageFailure::InvalidPart),
        (storage.upload_part(&mut session, &[]).await.err().unwrap(), StorageFailure::InvalidPart),
    ] {
        assert_eq!(error.failure(), expected);
    }
    let mut foreign = session.clone();
    foreign.location = StorageConfig::Dropbox {
        account_label: None,
        location: crate::DropboxLocation::SharedFolder { namespace_id: "another".into() },
    };
    for error in [
        storage.resume_upload(&mut foreign).await.err().unwrap(),
        storage.upload_part(&mut foreign, b"data").await.err().unwrap(),
        storage.finish_upload(&mut foreign).await.err().unwrap(),
        storage.abort_upload(&foreign).await.err().unwrap(),
        storage.restart_upload(&foreign).await.err().unwrap(),
    ] {
        assert_eq!(error.failure(), StorageFailure::SessionMismatch);
    }
    assert_eq!(storage.request_count(), before);
    storage.replace(&floor_state, &[0; 16]).await.unwrap();
    assert_eq!(storage.request_count(), before + 1);
    storage.upload_part(&mut session, b"data").await.unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    assert!(session.is_complete());
    let before = storage.request_count();
    storage.resume_upload(&mut session).await.unwrap();
    storage.finish_upload(&mut session).await.unwrap();
    storage.abort_upload(&session).await.unwrap();
    assert_eq!(
        storage.upload_part(&mut session, b"data").await.unwrap_err().failure(),
        StorageFailure::InvalidPart
    );
    assert_eq!(
        storage.restart_upload(&session).await.err().unwrap().failure(),
        StorageFailure::InvalidPart
    );
    // Completion cannot bypass the connection's location check.
    session.location = foreign.location;
    assert_eq!(
        storage.finish_upload(&mut session).await.unwrap_err().failure(),
        StorageFailure::SessionMismatch
    );
    assert_eq!(storage.request_count(), before);
}
