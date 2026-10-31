//! S3 setup's detection and safety check at the application boundary (§20.7, §20.8, E5).
use super::*;
use coven_storage::{test_utils::DETECTED_S3_SETTINGS, OwnBucketReason, S3Detection, S3Profile};

fn uncatalogued(profile: Option<S3Profile>) -> StorageConfig {
    uncatalogued_under(profile, "")
}

fn uncatalogued_under(profile: Option<S3Profile>, prefix: &str) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".parse().unwrap()),
        profile,
        prefix: crate::S3Prefix::parse(prefix).unwrap(),
    }
}

fn r2(prefix: &str) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "auto".into(),
        endpoint: Some("https://account.r2.cloudflarestorage.com".parse().unwrap()),
        profile: None,
        prefix: crate::S3Prefix::parse(prefix).unwrap(),
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
        auth: coven_storage::ProviderAuth,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.0.connect(config, auth, publications, cooldown, transfers, setup).await
    }
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
        _waits: &coven_storage::SetupWaits,
    ) -> Result<S3Detection, StorageError> {
        Ok(S3Detection {
            versioning: Ok(coven_storage::S3BucketVersioning::Disabled),
            settings: DETECTED_S3_SETTINGS,
            results: Vec::new(),
        })
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.0.check_new_s3_store(config, credentials, store, waits).await
    }
}

struct Opened {
    handle: CovenHandle,
    directory: StoreDir,
    _root: tempfile::TempDir,
}

struct BucketState {
    storage: Arc<MemoryStorage>,
    versioning: std::sync::Mutex<Result<S3BucketVersioning, Arc<StorageError>>>,
}
#[async_trait::async_trait]
impl StorageConnector for BucketState {
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.storage.connect(config, auth, publications, cooldown, transfers, setup).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<S3Detection, StorageError> {
        let mut detection = self.storage.detect_s3(config, credentials, store, waits).await?;
        detection.versioning = self.versioning.lock().unwrap().clone();
        Ok(detection)
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_s3_store(config, credentials, store, waits).await
    }
}

#[tokio::test]
async fn versioning_refusal_commits_nothing_and_preserves_an_existing_connection() {
    let location = uncatalogued(Some(S3Profile::Named(S3ProfileName::CephRgw)));
    let storage = Arc::new(MemoryStorage::builder().location(location.clone()).build().unwrap());
    let connector = Arc::new(BucketState {
        storage: storage.clone(),
        versioning: std::sync::Mutex::new(Ok(S3BucketVersioning::Enabled)),
    });
    let opened = open(connector.clone()).await;
    for state in [S3BucketVersioning::Enabled, S3BucketVersioning::Suspended] {
        *connector.versioning.lock().unwrap() = Ok(state);
        assert!(
            matches!(setup(&opened.handle, location.clone()).await.unwrap_err(), StorageSetupError::S3VersioningUnsupported { state: actual } if actual == state)
        );
    }
    *connector.versioning.lock().unwrap() = Err(Arc::new(StorageFailure::PermissionDenied.into()));
    assert!(matches!(
        setup(&opened.handle, location.clone()).await.unwrap_err(),
        StorageSetupError::S3VersioningUnknown { .. }
    ));
    assert_eq!(storage.request_count(), 0);
    assert_eq!(StorageSettings::new(opened.directory.clone()).read().unwrap(), None);
    *connector.versioning.lock().unwrap() = Ok(S3BucketVersioning::Disabled);
    setup(&opened.handle, location.clone()).await.unwrap();
    let before = storage.request_count();
    *connector.versioning.lock().unwrap() = Ok(S3BucketVersioning::Enabled);
    assert!(matches!(
        setup(&opened.handle, location.clone()).await.unwrap_err(),
        StorageSetupError::S3VersioningUnsupported { .. }
    ));
    assert_eq!(storage.request_count(), before);
    assert_eq!(StorageSettings::new(opened.directory.clone()).read().unwrap(), Some(location));
    opened.handle.close().await.unwrap();
}

async fn open(storage: Arc<dyn StorageConnector>) -> Opened {
    let root = tempfile::tempdir().unwrap();
    let app = TestCoven::new();
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let directory = app
        .create_closed_store(&StoreLayout::new(root.path().into()), "Loop", Arc::new(UuidIds))
        .await
        .unwrap();
    let handle = builder(&app, StoreLayout::new(root.path().into()), clock, storage)
        .open(directory.id(), |_| ())
        .await
        .unwrap();

    Opened { handle, directory, _root: root }
}

async fn setup(
    handle: &CovenHandle,
    location: StorageConfig,
) -> Result<StorageSetup, StorageSetupError> {
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
    let storage = Arc::new(MemoryStorage::builder().location(detected.clone()).build().unwrap());
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
    assert_eq!(connected.connection.storage, detected);
    assert_eq!(
        StorageSettings::new(opened.directory.clone()).read().unwrap(),
        Some(detected.clone())
    );
    // Reconnecting with the same location and no profile reuses the
    // recorded settings rather than refusing a location change.
    opened.handle.disconnect_storage().await.unwrap();
    let reconnected = setup(&opened.handle, uncatalogued(None)).await.unwrap();
    assert_eq!(reconnected.connection.storage, detected);
    assert_eq!(reconnected.restore_code.as_str(), connected.restore_code.as_str());
    assert_eq!(reconnected.restore_code.as_str(), opened.handle.restore_code().await.unwrap());
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
    for location in
        [uncatalogued(None), uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS)))]
    {
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
    assert_eq!(StorageSettings::new(opened.directory.clone()).read().unwrap(), None);
    opened.handle.close().await.unwrap();
}

fn own_bucket_required(error: &StorageSetupError, reason: OwnBucketReason) -> bool {
    matches!(
        error,
        StorageSetupError::Storage(source)
            if source.failure() == StorageFailure::OwnBucketRequired { reason }
    )
}

struct ConcurrentSetup {
    storage: Arc<MemoryStorage>,
    listed: tokio::sync::Barrier,
}

#[async_trait::async_trait]
impl StorageConnector for ConcurrentSetup {
    async fn connect(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
        setup: Option<Arc<coven_storage::SetupWaits>>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.storage.connect(config, auth, publications, cooldown, transfers, setup).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<S3Detection, StorageError> {
        self.storage.detect_s3(config, credentials, store, waits).await
    }
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_s3_store(config, credentials, store, waits).await?;
        self.listed.wait().await;
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_setups_after_empty_root_listings_admit_exactly_one_store() {
    let storage = Arc::new(MemoryStorage::builder().location(r2("")).build().unwrap());
    storage
        .set_faults(coven_storage::test_utils::Faults {
            hide_listings: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let connector = Arc::new(ConcurrentSetup { storage, listed: tokio::sync::Barrier::new(2) });
    let first = open(connector.clone()).await;
    let second = open(connector).await;
    let results = tokio::join!(setup(&first.handle, r2("")), setup(&second.handle, r2("")));
    let results = [results.0, results.1];
    first.handle.close().await.unwrap();
    second.handle.close().await.unwrap();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let error = results.into_iter().find_map(Result::err).unwrap();
    assert!(own_bucket_required(&error, OwnBucketReason::Occupied), "{error:?}");
}

#[tokio::test]
async fn an_r2_store_is_created_only_in_a_bucket_of_its_own() {
    let storage = Arc::new(MemoryStorage::builder().location(r2("")).build().unwrap());
    let first = open(storage.clone()).await;
    setup(&first.handle, r2("")).await.unwrap();
    // Every member's key reaches the whole bucket, so a second store there
    // is refused before setup writes anything.
    let second = open(storage.clone()).await;
    let before = storage.request_count();
    let error = setup(&second.handle, r2("")).await.unwrap_err();
    assert!(own_bucket_required(&error, OwnBucketReason::Occupied), "{error:?}");
    assert_eq!(storage.request_count(), before);
    assert_eq!(StorageSettings::new(second.directory.clone()).read().unwrap(), None);
    // Reconnecting reads the bucket marker, without listing the root.
    first.handle.disconnect_storage().await.unwrap();
    setup(&first.handle, r2("")).await.unwrap();
    first.handle.close().await.unwrap();
    second.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_base_prefix_is_refused_where_member_keys_reach_the_whole_bucket() {
    let storage = Arc::new(MemoryStorage::builder().location(r2("")).build().unwrap());
    let opened = open(storage.clone()).await;
    for location in [
        r2("stores"),
        uncatalogued_under(Some(S3Profile::Custom(DETECTED_S3_SETTINGS)), "stores"),
        uncatalogued_under(None, "stores"),
    ] {
        let error = setup(&opened.handle, location.clone()).await.unwrap_err();
        assert!(own_bucket_required(&error, OwnBucketReason::Prefix), "{error:?}");
        let error = opened
            .handle
            .detect_s3_settings(location, "member-key".into(), SecretText::new("secret".into()))
            .await
            .unwrap_err();
        assert!(own_bucket_required(&error, OwnBucketReason::Prefix), "{error:?}");
    }
    assert_eq!(storage.request_count(), 0);
    opened.handle.close().await.unwrap();
}

#[tokio::test]
async fn a_failed_first_setup_keeps_its_bucket_claim_and_can_retry() {
    let storage = Arc::new(MemoryStorage::builder().location(r2("")).build().unwrap());
    let opened = open(storage.clone()).await;
    storage
        .set_faults(coven_storage::test_utils::Faults {
            read_failures: std::collections::BTreeMap::from([(
                ObjectPath::floor(opened.directory.id()),
                StorageFailure::PermissionDenied,
            )]),
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    assert!(setup(&opened.handle, r2("")).await.is_err());
    assert_eq!(StorageSettings::new(opened.directory.clone()).read().unwrap(), None);
    storage.check_bucket(opened.directory.id()).await.unwrap();
    storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    setup(&opened.handle, r2("")).await.unwrap();
    opened.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_refuses_a_bucket_marker_naming_another_store() {
    let storage = Arc::new(MemoryStorage::builder().location(r2("")).build().unwrap());
    let opened = open(storage.clone()).await;
    setup(&opened.handle, r2("")).await.unwrap();
    opened.handle.disconnect_storage().await.unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    storage.plant_bucket_claim(Some(other.to_string().into_bytes())).await;
    let error = setup(&opened.handle, r2("")).await.unwrap_err();
    assert!(own_bucket_required(&error, OwnBucketReason::Occupied), "{error:?}");
    assert_eq!(StorageSettings::new(opened.directory.clone()).read().unwrap(), Some(r2("")));
    storage.check_bucket(other).await.unwrap();
    opened.handle.close().await.unwrap();
}

/// Run setup while advancing the store's clock a second at a time, so its
/// waits for rate-limited requests pass.
async fn setup_through_waits(f: &Fixture) -> Result<StorageSetup, StorageSetupError> {
    let setup = f.setup();
    tokio::pin!(setup);
    loop {
        tokio::select! {
            result = &mut setup => return result,
            () = tokio::time::sleep(Duration::from_millis(5)) => f.advance(Duration::from_secs(1)),
        }
    }
}

/// A provider limiting the request rate during setup is waited out (§20.8).
#[tokio::test]
async fn setup_sends_rate_limited_requests_again() {
    let f = Fixture::new().await;
    f.storage
        .set_faults(Faults {
            fail_next: 3,
            failure: StorageFailure::RateLimited,
            response: Some(429),
            ..Faults::none()
        })
        .await;
    let started = f.clock.now();
    setup_through_waits(&f).await.unwrap();
    assert!(f.clock.now() >= started + Duration::from_secs(1 + 2 + 4));
    status(&f.handle, |s| matches!(s, SyncStatus::Synced { .. })).await;
    f.handle.close().await.unwrap();
}

/// Once its minute of waiting is spent, setup fails with the refusal and
/// commits nothing.
#[tokio::test]
async fn setup_fails_once_its_minute_of_rate_limit_waits_is_spent() {
    let f = Fixture::new().await;
    f.storage
        .set_faults(Faults {
            fail_next: usize::MAX,
            failure: StorageFailure::RateLimited,
            response: Some(429),
            ..Faults::none()
        })
        .await;
    let error = setup_through_waits(&f).await.unwrap_err();
    // The first request is the provider check's create.
    assert!(
        matches!(&error, StorageSetupError::ProviderCheck { check: StorageCheck::Create, source }
            if source.failure() == StorageFailure::RateLimited),
        "{error:?}"
    );
    assert!(f.handle.storage_config().await.unwrap().is_none());
    f.handle.close().await.unwrap();
}
