//! S3 setup's detection and safety check at the application boundary (§4, E5).
use super::*;
use coven_storage::{test_utils::DETECTED_S3_SETTINGS, S3Detection, S3Profile};

fn uncatalogued(profile: Option<S3Profile>) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".parse().unwrap()),
        profile,
        prefix: "store".into(),
    }
}

/// Memory storage whose service observed no behavior holding, as a server
/// that accepts writes to an occupied name does.
struct Overwrites(Arc<MemoryStorage>);
#[async_trait::async_trait]
impl StorageConnector for Overwrites {
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        device: DeviceId,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.0.connect(config, credentials, device).await
    }
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
    ) -> Result<S3Detection, StorageError> {
        Ok(S3Detection {
            settings: DETECTED_S3_SETTINGS,
            observations: Vec::new(),
        })
    }
}

struct Opened {
    handle: CovenHandle,
    directory: StoreDir,
    _root: tempfile::TempDir,
}

async fn open(storage: Arc<dyn StorageConnector>) -> Opened {
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let directory = app
        .create_store(
            &StoreLayout::new(root.path().into()),
            "Loop",
            Arc::new(UuidIds),
        )
        .await
        .unwrap();
    let handle = builder(&app, StoreLayout::new(root.path().into()), clock, storage)
        .open(directory.id())
        .await
        .unwrap();
    handle.initialize_identity().unwrap();
    Opened {
        handle,
        directory,
        _root: root,
    }
}

async fn setup(
    handle: &CovenHandle,
    location: StorageConfig,
) -> Result<ConnectedStorage, StorageSetupError> {
    handle
        .setup_s3_storage(
            location,
            "Test device",
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
}

#[tokio::test]
async fn an_uncatalogued_endpoint_is_set_up_and_reconnected_with_detected_settings() {
    let detected = uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS)));
    let storage = Arc::new(
        MemoryStorage::builder()
            .location(detected.clone())
            .build()
            .unwrap(),
    );
    let opened = open(storage.clone()).await;
    let detection = opened
        .handle
        .detect_s3_settings(
            uncatalogued(None),
            "member-key".into(),
            SecretText::new("secret".into()),
        )
        .await
        .unwrap();
    assert_eq!(detection.settings, DETECTED_S3_SETTINGS);
    let connected = setup(&opened.handle, uncatalogued(None)).await.unwrap();
    assert_eq!(connected.storage, detected);
    assert_eq!(
        StorageSettings::new(opened.directory.clone())
            .read()
            .unwrap(),
        Some(detected.clone())
    );
    // Reconnecting with the same location and no profile reuses the
    // recorded settings rather than refusing a location change.
    opened.handle.disconnect_storage().await.unwrap();
    let reconnected = setup(&opened.handle, uncatalogued(None)).await.unwrap();
    assert_eq!(reconnected.storage, detected);
    opened.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_server_that_overwrites_occupied_names_is_refused_before_anything_is_committed() {
    let storage = Arc::new(
        MemoryStorage::builder()
            .location(uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS))))
            .build()
            .unwrap(),
    );
    let opened = open(Arc::new(Overwrites(storage.clone()))).await;
    for location in [
        uncatalogued(None),
        uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS))),
    ] {
        let error = setup(&opened.handle, location).await.unwrap_err();
        assert!(
            matches!(
                &error,
                StorageSetupError::ProviderCheck {
                    check: StorageCheck::CreateOnce,
                    source,
                } if source.failure() == StorageFailure::Protocol
            ),
            "{error:?}"
        );
    }
    assert_eq!(storage.request_count(), 0);
    assert_eq!(
        StorageSettings::new(opened.directory.clone())
            .read()
            .unwrap(),
        None
    );
    opened.handle.close().await.unwrap();
}
