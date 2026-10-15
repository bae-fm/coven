#![cfg(test)]

use super::*;
use coven_foundation::{
    clock::SystemClock,
    id_source::{StoreId, UuidIds},
};
use std::sync::Arc;

pub(super) struct LiveS3 {
    pub(super) config: StorageConfig,
    credentials: S3Credentials,
}
impl LiveS3 {
    pub(super) fn load() -> Self {
        let mut settings = crate::providers::tests::live_settings("COVEN_LIVE_S3_ENV");
        let endpoint = settings
            .remove("COVEN_LIVE_S3_ENDPOINT")
            .map(|value| value.as_str().parse().expect("valid live endpoint"));
        let profile = match std::env::var("COVEN_LIVE_S3_PROFILE") {
            Ok(name) => Some(S3Profile::Named(
                serde_json::from_value(serde_json::Value::String(name))
                    .expect("known S3 profile name"),
            )),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => panic!("invalid live profile: {error}"),
        };
        let mut take = |key| {
            settings
                .remove(key)
                .expect("required live S3 setting missing")
        };
        // A service whose member keys reach a whole bucket keeps each store
        // at the bucket's root (§4); the suite's stores share the test bucket.
        let prefix = match crate::providers::s3_key_scope(endpoint.as_ref(), profile.as_ref())
            .expect("valid live endpoint")
        {
            crate::providers::S3KeyScope::Bucket => String::new(),
            crate::providers::S3KeyScope::Prefix => "coven-live".into(),
        };
        let config = StorageConfig::S3 {
            bucket: take("COVEN_LIVE_S3_BUCKET").as_str().to_owned(),
            region: take("COVEN_LIVE_S3_REGION").as_str().to_owned(),
            endpoint,
            profile,
            prefix,
        };
        let credentials = S3Credentials {
            access_key_id: take("COVEN_LIVE_S3_ACCESS_KEY_ID").as_str().to_owned(),
            secret_access_key: take("COVEN_LIVE_S3_SECRET_ACCESS_KEY"),
        };
        config.validate().expect("valid live configuration");
        Self {
            config,
            credentials,
        }
    }
    /// Production detection from the env location, under a fresh store.
    pub(super) async fn detect(&self) -> Result<crate::S3Detection, StorageError> {
        let store = StoreId(coven_foundation::id_source::IdSource::new_id(&UuidIds));
        let result = super::detect(
            self.config.clone(),
            self.credentials.clone(),
            store,
            Arc::new(SystemClock),
            Arc::new(UuidIds),
        )
        .await;
        // Production detection removes current probe objects. Certification
        // additionally removes retained versions and delete markers.
        let settings = match &result {
            Ok(detection) => detection.settings,
            Err(_) => {
                let StorageConfig::S3 {
                    endpoint, profile, ..
                } = &self.config
                else {
                    unreachable!()
                };
                profile::settings(endpoint.as_ref(), profile.as_ref())?.unwrap_or(profile::PLAIN)
            }
        };
        let storage = self.connect(Some(settings));
        // A refused versioning read, which detection records as unknown, may
        // hide retained versions; the version listing itself establishes them.
        let cleanup = match storage.provider.bucket_versioning().await {
            Ok(S3BucketVersioning::Enabled | S3BucketVersioning::Suspended) => Ok(true),
            Err(error) if versioning::unknown(&error) => Ok(true),
            Ok(_) => Ok(false),
            Err(error) => Err(error),
        };
        let cleanup = match cleanup {
            Ok(true) => {
                live_cleanup::cleanup_versions(
                    &storage.provider,
                    &ObjectPrefix::all(store).under(&storage.provider.prefix),
                )
                .await
            }
            Ok(false) => Ok(()),
            Err(error) => Err(error),
        };
        crate::transfer::with_cleanup(result, cleanup)
    }
    /// The env key as the service's admin access, for minting member keys,
    /// with `COVEN_LIVE_S3_IAM_ENDPOINT` where the service runs IAM apart
    /// from its S3 endpoint (Versity Gateway).
    pub(super) fn admin(&self) -> crate::AdminCredential {
        let iam_endpoint = match std::env::var("COVEN_LIVE_S3_IAM_ENDPOINT") {
            Ok(url) => Some(url.parse().expect("valid live IAM endpoint")),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => panic!("invalid live IAM endpoint: {error}"),
        };
        crate::AdminCredential::AccessKey {
            access_key_id: self.credentials.access_key_id.clone(),
            secret_access_key: self.credentials.secret_access_key.clone(),
            session_token: None,
            iam_endpoint,
        }
    }
    pub(super) fn connect(&self, settings: Option<S3Settings>) -> StorageConnection<S3Storage> {
        let config = self.config.clone();
        let credentials = self.credentials.clone();
        let mut provider = match settings {
            Some(settings) => S3Storage::with_settings(
                config,
                credentials,
                Arc::new(SystemClock),
                Arc::new(UuidIds),
                std::sync::Arc::new(crate::NoCooldown),
                settings,
            ),
            None => S3Storage::new(
                config,
                credentials,
                Arc::new(SystemClock),
                Arc::new(UuidIds),
                std::sync::Arc::new(crate::NoCooldown),
            ),
        }
        .expect("live S3 connection");
        provider.single_request_limit = 1024 * 1024;
        StorageConnection::from_provider(Arc::new(provider))
    }
}
