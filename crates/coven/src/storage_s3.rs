//! S3 setup: a new store's own-bucket check where member keys reach a whole
//! bucket, then detection and the safety check of the effective settings
//! against the service, before the provider check and any commitment (§20.8).

use super::*;
use coven_storage::S3Detection;

impl StorageConnections {
    pub(crate) async fn setup_s3(
        self: &Arc<Self>,
        storage: StorageConfig,
        device_name: &str,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<StorageSetup, StorageSetupError> {
        if storage.provider() != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("expected S3 storage")
                .into());
        }
        let credentials = S3Credentials {
            access_key_id,
            secret_access_key,
        };
        let device_name = device_name.to_owned();
        self.call(move |owner| async move {
            let waits = Arc::new(SetupWaits::default());
            let outcome = async {
                let recorded = owner.codes.location().await.map_err(setup_error)?;
                let config = recorded_s3(storage, recorded.as_ref());
                owner.check_location(&config).await?;
                if recorded.is_none() {
                    // Creating the store: where member keys reach the whole
                    // bucket, the bucket must hold nothing but this store (§20.7).
                    owner
                        .connector
                        .check_new_s3_store(
                            config.clone(),
                            credentials.clone(),
                            owner.store(),
                            &waits,
                        )
                        .await?;
                }
                // §20.8's safety check: the effective settings must agree with what
                // the service does, before the provider check and any commitment.
                let detection = owner
                    .connector
                    .detect_s3(config.clone(), credentials.clone(), owner.store(), &waits)
                    .await?;
                let config = detection.resolve(config)?;
                owner
                    .setup_connection(
                        config,
                        &device_name,
                        ProviderAuth::S3(credentials),
                        None,
                        &waits,
                    )
                    .await
            }
            .await;
            waits.end();
            outcome
        })
        .await
    }

    pub(crate) async fn detect_s3(
        self: &Arc<Self>,
        storage: StorageConfig,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<S3Detection, StorageSetupError> {
        if storage.provider() != CloudProvider::S3 {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("expected S3 storage")
                .into());
        }
        let credentials = S3Credentials {
            access_key_id,
            secret_access_key,
        };
        // The task owns detection until its cleanup finishes, even if the
        // awaiting app future is dropped. Detecting is its own call: its
        // rate-limited probes draw on their own minute (§20.8).
        self.call(move |owner| async move {
            Ok(owner
                .connector
                .detect_s3(storage, credentials, owner.store(), &SetupWaits::default())
                .await?)
        })
        .await
    }
}

/// Reconnecting with the recorded S3 location and no profile reuses the
/// recorded settings, which every other device already uses (§20.7).
fn recorded_s3(supplied: StorageConfig, recorded: Option<&StorageConfig>) -> StorageConfig {
    match (supplied, recorded) {
        (
            StorageConfig::S3 {
                bucket,
                region,
                endpoint,
                profile: None,
                prefix,
            },
            Some(recorded @ StorageConfig::S3 { .. }),
        ) if matches!(recorded, StorageConfig::S3 {
                bucket: b, region: r, endpoint: e, prefix: p, ..
            } if *b == bucket && *r == region && *e == endpoint && *p == prefix) =>
        {
            recorded.clone()
        }
        (supplied, _) => supplied,
    }
}
