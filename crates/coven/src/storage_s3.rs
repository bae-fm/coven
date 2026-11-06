//! S3 setup: a new store's own-bucket check where member keys reach a whole
//! bucket, then detection and the safety check of the effective settings
//! against the service, before the provider check and any commitment (§20.8).

use super::*;
use coven_storage::{S3Detection, S3DetectionRuns};
use coven_sync::DetectionRecord;

impl StorageConnections {
    pub(crate) async fn setup_s3(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
        progress: SetupProgress,
    ) -> Result<StorageSetup, StorageSetupError> {
        require_s3(&storage)?;
        let credentials = S3Credentials { access_key_id, secret_access_key };
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let mut progress = ProgressReport::new(progress, owner.clock.clone());
            let waits = SetupWaits::default();
            let recorded = owner.codes.location().await.map_err(setup_error)?;
            let config = recorded_s3(storage, recorded.as_ref());
            owner.check_location(&config).await?;
            let journal = owner
                .begin_setup(&device_name, &StorageCredentials::S3(credentials.clone()))
                .await?;
            owner
                .run_setup(journal, async |journal| {
                    if journal.creating() {
                        // Creating the store: where member keys reach the
                        // whole bucket, the bucket must hold nothing but this
                        // store (§20.7).
                        let checking = owner.connector.check_new_s3_store(
                            config.clone(),
                            credentials.clone(),
                            owner.store(),
                            &waits,
                        );
                        setup_step::single(
                            &mut progress,
                            StorageSetupStep::CheckingBucket,
                            checking,
                        )
                        .await?;
                    }
                    // §20.8's safety check: the effective settings must agree
                    // with what the service does, before the provider check
                    // and any commitment.
                    let detecting = async {
                        let detection =
                            owner.detect_recorded(&config, credentials.clone(), &waits).await?;
                        Ok::<_, StorageSetupError>(detection.resolve(config)?)
                    };
                    let config = setup_step::single(
                        &mut progress,
                        StorageSetupStep::DetectingSettings,
                        detecting,
                    )
                    .await?;
                    owner.reached(setup::SetupStop::Detected).await?;
                    owner.check_cancel()?;
                    owner
                        .setup_connection(
                            journal,
                            config,
                            ProviderAuth::S3(credentials),
                            None,
                            waits,
                            &mut progress,
                        )
                        .await
                })
                .await
        })
        .await
    }

    pub(crate) async fn detect_s3(
        self: &Arc<Self>,
        storage: StorageConfig,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<S3Detection, StorageSetupError> {
        require_s3(&storage)?;
        let credentials = S3Credentials { access_key_id, secret_access_key };
        // The task owns detection until its cleanup finishes, even if the
        // awaiting app future is dropped. Detecting is its own call: its
        // rate-limited probes draw on their own minute (§20.8).
        self.call(move |owner| async move {
            owner.detect_recorded(&storage, credentials, &SetupWaits::default()).await
        })
        .await
    }

    /// Detect at `config` as one recorded run (§20.7): record the run
    /// before its first probe, hand detection the earlier runs recorded
    /// where its probes go, and end the record of each run whose probes and
    /// uploads it confirmed gone. A run it couldn't confirm keeps its
    /// record for the next detection or connection there, a crash's
    /// included.
    async fn detect_recorded(
        &self,
        config: &StorageConfig,
        credentials: S3Credentials,
        waits: &SetupWaits,
    ) -> Result<S3Detection, StorageSetupError> {
        let earlier = DetectionRecord::at(&self.database, config).await.map_err(setup_error)?;
        let record = DetectionRecord::start(&self.database, config, self.ids.new_id())
            .await
            .map_err(setup_error)?;
        let runs = S3DetectionRuns {
            run: record.run(),
            earlier: earlier.iter().map(DetectionRecord::run).collect(),
        };
        let detected =
            self.connector.detect_s3(config.clone(), credentials, self.store(), &runs, waits).await;
        let ended = async {
            for record in earlier.iter().chain([&record]) {
                if detected.cleaned.contains(&record.run()) {
                    record.end(&self.database).await?;
                }
            }
            Ok::<_, SyncError>(())
        }
        .await;
        match (detected.detection, ended) {
            (detection, Ok(())) => Ok(detection?),
            (Ok(_), Err(error)) => Err(setup_error(error)),
            (Err(detection), Err(error)) => Err(setup::with_cleanup(detection.into(), error)),
        }
    }
}

/// End each detection run recorded in `database` where `storage`, just
/// connected to `location`, finds `store`'s probes, once the connection has
/// removed what it left (§20.7). A run whose leftovers can't be removed yet
/// keeps its record for the next detection or connection there: they are
/// space nothing refers to, so the connection doesn't wait on them.
pub(super) async fn end_detection_runs(
    database: &coven_database::Database,
    store: StoreId,
    location: &StorageConfig,
    storage: &StorageConnection,
) -> Result<(), SyncError> {
    for record in DetectionRecord::at(database, location).await? {
        match storage.end_detection_run(store, record.run()).await {
            Ok(()) => record.end(database).await?,
            Err(error) => {
                tracing::warn!("a detection run's probes can't be removed yet: {error}");
            }
        }
    }
    Ok(())
}

fn require_s3(storage: &StorageConfig) -> Result<(), StorageSetupError> {
    if storage.provider() != CloudProvider::S3 {
        return Err(StorageFailure::InvalidConfiguration.with_source("expected S3 storage").into());
    }
    Ok(())
}

/// Reconnecting with the recorded S3 location and no profile reuses the
/// recorded settings, which every other device already uses (§20.7).
fn recorded_s3(supplied: StorageConfig, recorded: Option<&StorageConfig>) -> StorageConfig {
    match recorded {
        Some(recorded)
            if matches!(supplied, StorageConfig::S3 { profile: None, .. })
                && recorded.same_s3_objects(&supplied) =>
        {
            recorded.clone()
        }
        _ => supplied,
    }
}
