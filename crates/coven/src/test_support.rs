use std::sync::Arc;

use coven_foundation::config::{
    CloudHomeConfig, CloudProvider, ExactUploadVerification, HomeStorage,
};
use coven_keys::keys::CloudHomeCredentials;
use coven_storage::cloud::{CloudHome as _, CloudHomeFactory, ExactCloudHome};

/// A reserved prefix in the configured live S3-compatible test bucket.
///
/// Every live test names its own stable prefix. Resetting it before and after
/// the run means a failed process leaves evidence for inspection while the next
/// run starts from the same known-empty location.
pub(crate) struct RealS3TestHome {
    home: Arc<coven_storage::S3CloudHome>,
    config: CloudHomeConfig,
    credentials: CloudHomeCredentials,
}

impl RealS3TestHome {
    pub(crate) async fn open(factory: &CloudHomeFactory, case: &str, storage: HomeStorage) -> Self {
        let bucket = required_env("COVEN_TEST_S3_BUCKET");
        let region = required_env("COVEN_TEST_S3_REGION");
        let endpoint = required_env("COVEN_TEST_S3_URL");
        let access_key = required_env("COVEN_TEST_S3_KEY");
        let secret_key = required_env("COVEN_TEST_S3_SECRET");
        let prefix = [
            std::env::var("COVEN_TEST_S3_PREFIX")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.trim_matches('/').to_string()),
            Some(format!("coven-live-tests/{case}")),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("/");
        let home = Arc::new(
            factory
                .open_s3(
                    bucket.clone(),
                    region.clone(),
                    Some(endpoint.clone()),
                    access_key.clone(),
                    secret_key.clone(),
                    Some(prefix.clone()),
                    ExactUploadVerification::MetadataHash,
                    Arc::new(crate::SystemClock),
                )
                .await
                .expect("open configured live S3 test home"),
        );
        Self {
            home,
            config: CloudHomeConfig {
                provider: Some(CloudProvider::S3),
                storage,
                s3_bucket: Some(bucket),
                s3_region: Some(region),
                s3_endpoint: Some(endpoint),
                s3_key_prefix: Some(prefix),
                exact_upload_verification: ExactUploadVerification::MetadataHash,
                ..Default::default()
            },
            credentials: CloudHomeCredentials::S3 {
                access_key,
                secret_key,
            },
        }
    }

    pub(crate) fn config(&self) -> CloudHomeConfig {
        self.config.clone()
    }

    pub(crate) fn credentials(&self) -> CloudHomeCredentials {
        self.credentials.clone()
    }

    pub(crate) fn home(&self) -> Arc<dyn ExactCloudHome> {
        self.home.clone()
    }

    pub(crate) async fn reset(&self) {
        for key in self.home.list("").await.expect("list live test objects") {
            self.home
                .delete(&key)
                .await
                .expect("delete live test object");
        }
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for this ignored live test"))
}
