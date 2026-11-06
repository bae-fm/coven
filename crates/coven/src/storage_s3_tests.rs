//! S3 setup's detection and safety check at the application boundary (§20.7, §20.8, E5).
use super::*;
use coven_storage::{test_utils::DETECTED_S3_SETTINGS, S3Detection, S3Profile};

fn uncatalogued(profile: Option<S3Profile>) -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".parse().unwrap()),
        profile,
    }
}

fn r2() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "auto".into(),
        endpoint: Some("https://account.r2.cloudflarestorage.com".parse().unwrap()),
        profile: None,
    }
}

fn aws() -> StorageConfig {
    StorageConfig::S3 {
        bucket: "test".into(),
        region: "us-east-1".into(),
        endpoint: None,
        profile: None,
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
        device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.0.connect(config, auth, device, publications, cooldown, transfers).await
    }
    async fn detect_s3(
        &self,
        _config: StorageConfig,
        _credentials: S3Credentials,
        _store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        _waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        coven_storage::S3DetectionRun::unwritten(
            runs,
            Ok(S3Detection {
                versioning: Ok(coven_storage::S3BucketVersioning::Disabled),
                settings: DETECTED_S3_SETTINGS,
                results: Vec::new(),
            }),
        )
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.0.check_new_store(config, auth, store, waits).await
    }
}

struct Opened {
    app: Coven,
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
        device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.storage.connect(config, auth, device, publications, cooldown, transfers).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        let mut run = self.storage.detect_s3(config, credentials, store, runs, waits).await;
        if let Ok(detection) = &mut run.detection {
            detection.versioning = self.versioning.lock().unwrap().clone();
        }
        run
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_store(config, auth, store, waits).await
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
            matches!(setup(&opened.handle, location.clone()).await.unwrap_err(), StorageSetupError::Check(StorageCheckError::S3VersioningUnsupported { state: actual }) if actual == state)
        );
    }
    *connector.versioning.lock().unwrap() = Err(Arc::new(StorageFailure::PermissionDenied.into()));
    assert!(matches!(
        setup(&opened.handle, location.clone()).await.unwrap_err(),
        StorageSetupError::Check(StorageCheckError::S3VersioningUnknown { .. })
    ));
    assert_eq!(storage.request_count(), 0);
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), None);
    *connector.versioning.lock().unwrap() = Ok(S3BucketVersioning::Disabled);
    setup(&opened.handle, location.clone()).await.unwrap();
    // The connection's first pass runs beside the test: count from where
    // its workers have parked, and again once they have taken whatever the
    // refused setup left them.
    opened.handle.test_parked().await;
    let before = storage.request_count();
    *connector.versioning.lock().unwrap() = Ok(S3BucketVersioning::Enabled);
    assert!(matches!(
        setup(&opened.handle, location.clone()).await.unwrap_err(),
        StorageSetupError::Check(StorageCheckError::S3VersioningUnsupported { .. })
    ));
    opened.handle.test_parked().await;
    assert_eq!(storage.request_count(), before);
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), Some(location));
    opened.handle.close().await.unwrap();
}

async fn open(storage: Arc<dyn StorageConnector>) -> Opened {
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let directory = app.create_closed_store("Loop", Arc::new(UuidIds)).await.unwrap();
    let handle = builder(&app, clock, storage).open(directory.id(), |_| ()).await.unwrap();

    Opened { app, handle, directory, _root: root }
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
            |_| {},
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
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), Some(detected.clone()));
    // Reconnecting with the same location and no profile reuses the
    // recorded settings rather than refusing a location change.
    let reconnected = setup(&opened.handle, uncatalogued(None)).await.unwrap();
    assert_eq!(reconnected.connection.storage, detected);
    assert_eq!(reconnected.restore_code.as_str(), connected.restore_code.as_str());
    assert_eq!(reconnected.restore_code.as_str(), opened.handle.restore_code().await.unwrap());
    opened.handle.close().await.unwrap();
}

/// Detecting settings commits only its run's record: no location is
/// recorded, custody keeps no storage credentials, the store still has no
/// storage, and the bucket holds no object of the store once it returns (E5).
#[tokio::test]
async fn detecting_settings_commits_nothing() {
    let location = uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS)));
    let storage = Arc::new(MemoryStorage::builder().location(location).build().unwrap());
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
    assert!(coven_storage::recorded_config(&opened.directory).unwrap().is_none());
    assert!(opened.handle.storage_config().await.unwrap().is_none());
    let custody = opened.app.store_keychain(opened.directory.id());
    assert!(custody.storage_credentials().unwrap().is_none());
    assert_eq!(opened.handle.sync_status().await.unwrap().state, SyncState::NotSynced);
    let left = storage.list(&ObjectPrefix::all(opened.directory.id())).await.unwrap();
    assert!(left.is_empty(), "{left:?}");
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
                StorageSetupError::Check(StorageCheckError::ProviderCheck {
                    check: StorageCheck::CreateOnce,
                    source,
                }) if source.failure() == StorageFailure::Protocol
            ),
            "{error:?}"
        );
    }
    assert_eq!(storage.request_count(), 0);
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), None);
    opened.handle.close().await.unwrap();
}

fn own_location_required(error: &StorageSetupError) -> bool {
    matches!(
        error,
        StorageSetupError::Storage(source)
            if source.failure() == StorageFailure::OwnLocationRequired
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
        device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn coven_storage::PublicationGate>,
        cooldown: Arc<dyn coven_storage::Cooldown>,
        transfers: Arc<coven_storage::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        self.storage.connect(config, auth, device, publications, cooldown, transfers).await
    }
    async fn detect_s3(
        &self,
        config: StorageConfig,
        credentials: S3Credentials,
        store: StoreId,
        runs: &coven_storage::S3DetectionRuns,
        waits: &coven_storage::SetupWaits,
    ) -> coven_storage::S3DetectionRun {
        self.storage.detect_s3(config, credentials, store, runs, waits).await
    }
    async fn check_new_store(
        &self,
        config: StorageConfig,
        auth: coven_storage::ProviderAuth,
        store: StoreId,
        waits: &coven_storage::SetupWaits,
    ) -> Result<(), StorageError> {
        self.storage.check_new_store(config, auth, store, waits).await?;
        self.listed.wait().await;
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_setups_after_empty_root_listings_admit_exactly_one_store() {
    let storage = Arc::new(MemoryStorage::builder().location(r2()).build().unwrap());
    storage
        .set_faults(coven_storage::test_utils::Faults {
            hide_listings: true,
            ..coven_storage::test_utils::Faults::none()
        })
        .await;
    let connector = Arc::new(ConcurrentSetup { storage, listed: tokio::sync::Barrier::new(2) });
    let first = open(connector.clone()).await;
    let second = open(connector).await;
    let results = tokio::join!(setup(&first.handle, r2()), setup(&second.handle, r2()));
    let results = [results.0, results.1];
    first.handle.close().await.unwrap();
    second.handle.close().await.unwrap();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let error = results.into_iter().find_map(Result::err).unwrap();
    assert!(own_location_required(&error), "{error:?}");
}

#[tokio::test]
async fn a_store_is_created_only_in_a_bucket_of_its_own() {
    for location in [r2(), aws()] {
        let storage =
            Arc::new(MemoryStorage::builder().location(location.clone()).build().unwrap());
        let first = open(storage.clone()).await;
        setup(&first.handle, location.clone()).await.unwrap();
        // Every member's key reaches the whole bucket, so a second store
        // there is refused before setup writes anything.
        let second = open(storage.clone()).await;
        let before = storage.request_count();
        let error = setup(&second.handle, location.clone()).await.unwrap_err();
        assert!(own_location_required(&error), "{location:?}: {error:?}");
        assert_eq!(storage.request_count(), before);
        assert_eq!(coven_storage::recorded_config(&second.directory).unwrap(), None);
        // Reconnecting reads the location marker, without listing the root.
        setup(&first.handle, location).await.unwrap();
        first.handle.close().await.unwrap();
        second.handle.close().await.unwrap();
    }
}

/// Ana's first R2 setup claims its bucket, then fails reading the floor:
/// it is past its point of no return, so it keeps its location claim and
/// commits nothing yet. Once storage answers, the loop setup started
/// finishes it, and a later setup reconnects.
#[tokio::test]
async fn a_setup_failing_past_its_location_claim_finishes_through_the_loop() {
    let storage = Arc::new(MemoryStorage::builder().location(r2()).build().unwrap());
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
    assert!(setup(&opened.handle, r2()).await.is_err());
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), None);
    storage.check_location_claim(opened.directory.id()).await.unwrap();
    storage.set_faults(coven_storage::test_utils::Faults::none()).await;
    opened.handle.retry_now().await.unwrap().await;
    let setups = opened
        .handle
        .read(|sql| {
            Ok::<_, CovenError>(sql.query(
                "SELECT operation FROM coven_operations WHERE kind='StorageSetup'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    let outcome = opened.handle.operation_outcome(OperationId(setups[0])).await.unwrap();
    assert_eq!(outcome, OperationOutcome::Landed(coven_merge::Slot(1)));
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), Some(r2()));
    setup(&opened.handle, r2()).await.unwrap();
    opened.handle.close().await.unwrap();
}

#[tokio::test]
async fn reconnect_refuses_a_bucket_marker_naming_another_store() {
    let storage = Arc::new(MemoryStorage::builder().location(r2()).build().unwrap());
    let opened = open(storage.clone()).await;
    setup(&opened.handle, r2()).await.unwrap();
    let other = StoreId(uuid::Uuid::from_bytes([0x20; 16]));
    storage.plant_location_marker(Some(other.to_string().into_bytes())).await;
    let error = setup(&opened.handle, r2()).await.unwrap_err();
    assert!(own_location_required(&error), "{error:?}");
    assert_eq!(coven_storage::recorded_config(&opened.directory).unwrap(), Some(r2()));
    storage.check_location_claim(other).await.unwrap();
    opened.handle.close().await.unwrap();
}

/// Run setup, setting the store's clock to the end of each wait it arms,
/// so its waits for rate-limited requests pass.
async fn setup_through_waits(f: &Fixture) -> Result<StorageSetup, StorageSetupError> {
    let setup = f.setup();
    tokio::pin!(setup);
    use coven_foundation::clock::Clock as _;
    let mut seen = f.clock.sleeps_begun();
    loop {
        tokio::select! {
            result = &mut setup => return result,
            (begun, deadline) = f.clock.sleep_begun_after(seen) => {
                seen = begun;
                f.advance(deadline.saturating_sub(f.clock.elapsed()));
            }
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
    synced(&f.handle).await;
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
        matches!(&error, StorageSetupError::Check(StorageCheckError::ProviderCheck { check: StorageCheck::Create, source })
            if source.failure() == StorageFailure::RateLimited),
        "{error:?}"
    );
    assert!(f.handle.storage_config().await.unwrap().is_none());
    f.handle.close().await.unwrap();
}

/// Setup reads and writes only under its own store's id, besides the bucket
/// marker it claims (§20.3).
#[tokio::test]
async fn setup_reads_and_writes_only_under_its_stores_folder() {
    let f = Fixture::new().await;
    f.setup().await.unwrap();
    synced(&f.handle).await;
    let store = format!("{}/", f.directory.id());
    let trace = f.storage.request_trace();
    assert!(!trace.is_empty());
    for request in trace {
        let name = match &request {
            MemoryRequest::Read(path, _)
            | MemoryRequest::Create(path)
            | MemoryRequest::Replace(path)
            | MemoryRequest::Status(path)
            | MemoryRequest::Delete(path) => path.as_str().to_owned(),
            MemoryRequest::List(prefix) => prefix.as_str().to_owned(),
            MemoryRequest::Other(_) => continue,
        };
        assert!(name.starts_with(&store), "{request:?} is outside the store's folder {store}");
    }
    f.handle.close().await.unwrap();
}

/// Ana's app detects her bucket's settings and is killed mid-detection. Her
/// detection's run was recorded before its first probe, so after she
/// reopens the store the next detection there is handed that run to clean
/// up after; once it confirms the run's leftovers gone, the record ends
/// and no later detection is handed it again (§20.7).
#[tokio::test]
async fn a_cut_detection_run_is_handed_to_the_next_detection_after_reopening() {
    let connector = crate::tests::Connector::new(Arc::new(
        MemoryStorage::builder()
            .location(uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS))))
            .build()
            .unwrap(),
    ));
    let root = tempfile::tempdir().unwrap();
    let app = Coven::in_memory(root.path().into());
    let clock = Arc::new(FixedClock::new(UNIX_EPOCH + Duration::from_secs(1000)));
    let directory = app.create_closed_store("Loop", Arc::new(UuidIds)).await.unwrap();
    let handle =
        builder(&app, clock.clone(), connector.clone()).open(directory.id(), |_| ()).await.unwrap();
    let cut = connector.detect(&handle, uncatalogued(None), true).await;
    assert!(cut.earlier.is_empty());
    handle.close().await.unwrap();

    let handle =
        builder(&app, clock, connector.clone()).open(directory.id(), |_| ()).await.unwrap();
    let next = connector.detect(&handle, uncatalogued(None), false).await;
    assert_eq!(next.earlier, [cut.run]);
    let last = connector.detect(&handle, uncatalogued(None), false).await;
    assert!(last.earlier.is_empty(), "{last:?}");
    handle.close().await.unwrap();
}

/// A detection run cut short where the store's own bucket is ends once the
/// store connects there, which removes what it left; one cut short at
/// another bucket stays recorded for a detection there (§20.7).
#[tokio::test]
async fn connecting_ends_the_detection_runs_recorded_at_its_bucket() {
    let connector = crate::tests::Connector::new(Arc::new(
        MemoryStorage::builder()
            .location(uncatalogued(Some(S3Profile::Custom(DETECTED_S3_SETTINGS))))
            .build()
            .unwrap(),
    ));
    let opened = open(connector.clone()).await;
    setup(&opened.handle, uncatalogued(None)).await.unwrap();
    let elsewhere = StorageConfig::S3 {
        bucket: "elsewhere".into(),
        region: "us-east-1".into(),
        endpoint: Some("http://localhost:9000".parse().unwrap()),
        profile: None,
    };
    connector.detect(&opened.handle, uncatalogued(None), true).await;
    let away = connector.detect(&opened.handle, elsewhere.clone(), true).await;
    opened.handle.unlock_store_key().await.unwrap();
    let here = connector.detect(&opened.handle, uncatalogued(None), false).await;
    assert!(here.earlier.is_empty(), "{here:?}");
    let there = connector.detect(&opened.handle, elsewhere, false).await;
    assert_eq!(there.earlier, [away.run]);
    opened.handle.close().await.unwrap();
}
