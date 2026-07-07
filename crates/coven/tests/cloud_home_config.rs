//! A missing cloud-home configuration surfaces as a non-retryable
//! `CloudHomeError::Configuration` at the public `create_cloud_home` surface, so
//! a host can tell "fix your settings" apart from a transient network failure it
//! should keep retrying.

use std::sync::Arc;

use coven::clock::SystemClock;
use coven::config::{CloudProvider, Config};
use coven::keys::KeyService;
use coven::library_dir::LibraryDir;
use coven::storage::cloud::{create_cloud_home, CloudHomeError};

#[tokio::test]
async fn s3_without_a_bucket_is_a_non_retryable_configuration_error() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let mut config = Config::with_defaults(
        "lib-cfg".to_string(),
        "device-cfg".to_string(),
        LibraryDir::new(tmp.path()),
        "Cfg".to_string(),
    );
    // A provider is selected but its required bucket is unset: the user has to
    // fix the configuration, and retrying cannot make the bucket appear.
    config.cloud_home.provider = Some(CloudProvider::S3);
    config.cloud_home.s3_bucket = None;

    let key_service = KeyService::new("lib-cfg".to_string());
    let error = match create_cloud_home(&config, &key_service, Arc::new(SystemClock)).await {
        Ok(_) => panic!("a provider with no bucket must not build a cloud home"),
        Err(error) => error,
    };

    assert!(
        matches!(error, CloudHomeError::Configuration(_)),
        "got {error:?}"
    );
    assert!(
        !error.is_retryable(),
        "a missing bucket is fatal until reconfigured, not a transient retry",
    );
}
