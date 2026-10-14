//! The in-memory provider as an application's storage connector: settings
//! saved through the builder's connector hook, S3 detection that conforms to
//! any settings, and the S3 new-store bucket check.
use super::*;

#[async_trait]
impl crate::providers::StorageConnector for MemoryStorage {
    async fn prepare_cloudkit(
        &self,
        container: &str,
        _store: coven_foundation::id_source::StoreId,
    ) -> Result<StorageConfig, StorageError> {
        let config = self.config();
        if !matches!(&config, StorageConfig::CloudKit { container: expected, .. } if expected == container)
        {
            return Err(StorageFailure::InvalidConfiguration
                .with_source("memory connector container mismatch"));
        }
        Ok(config)
    }
    async fn connect(
        &self,
        config: StorageConfig,
        credentials: StorageCredentials,
        _device: coven_foundation::id_source::DeviceId,
        publications: Arc<dyn crate::PublicationGate>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        Ok(self
            .connect_device(config, credentials)
            .await?
            .with_publication_gate(publications)
            .shared_connection())
    }
    /// Memory behaves as any settings demand: the detection conforms to the
    /// location's own settings, or to plain S3 settings for detection.
    async fn detect_s3(
        &self,
        config: StorageConfig,
        _credentials: crate::S3Credentials,
        _store: coven_foundation::id_source::StoreId,
    ) -> Result<S3Detection, StorageError> {
        config.validate()?;
        let StorageConfig::S3 {
            endpoint, profile, ..
        } = &config
        else {
            return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
        };
        Ok(conforming_detection(
            crate::providers::s3_settings(endpoint.as_ref(), profile.as_ref())?
                .unwrap_or(DETECTED_S3_SETTINGS),
        ))
    }
    /// Every admin credential reaches the memory account's key administration.
    fn s3_key_admin(
        &self,
        config: StorageConfig,
        _admin: crate::AdminCredential,
    ) -> Result<Arc<dyn crate::AccessKeyAdmin>, StorageError> {
        config.validate()?;
        Ok(Arc::new(self.key_admin()))
    }
    /// Memory's objects are one bucket: where the location's member keys
    /// reach the whole bucket, another store's object occupies it.
    async fn check_new_s3_store(
        &self,
        config: StorageConfig,
        _credentials: crate::S3Credentials,
        store: coven_foundation::id_source::StoreId,
    ) -> Result<(), StorageError> {
        config.validate()?;
        let StorageConfig::S3 {
            endpoint, profile, ..
        } = &config
        else {
            return Err(StorageFailure::InvalidConfiguration.with_source("expected S3"));
        };
        let scope = crate::providers::s3_key_scope(endpoint.as_ref(), profile.as_ref())?;
        if scope == crate::providers::S3KeyScope::Bucket
            && self
                .provider
                .state
                .lock()
                .await
                .objects
                .keys()
                .any(|path| path.store() != store)
        {
            return Err(StorageFailure::OwnBucketRequired {
                reason: crate::OwnBucketReason::Occupied,
            }
            .into());
        }
        Ok(())
    }
}

/// What memory's detection reports where the location needs detection.
pub const DETECTED_S3_SETTINGS: S3Settings = S3Settings {
    signing: S3Signing::AwsSigV4,
    single_put: S3SinglePut::IfNoneMatch,
    multipart: S3Multipart::ConditionalCompletion,
    multipart_timestamp: S3MultipartTimestamp::Completion,
    checksums: S3Checksums::Absent,
    listing: S3Listing::V2,
    revision: S3Revision::ETag,
    response_headers: S3ResponseHeaders::S3,
    request_checksums: false,
};

/// A detection whose every observation agrees with these settings.
pub fn conforming_detection(settings: S3Settings) -> S3Detection {
    let completion = settings.multipart_timestamp == S3MultipartTimestamp::Completion;
    let observations = [
        (S3Probe::Signing(settings.signing), true),
        (S3Probe::SinglePut(settings.single_put), true),
        (S3Probe::PartialUploadHidden, true),
        (
            S3Probe::ConditionalCompletion,
            settings.multipart == S3Multipart::ConditionalCompletion,
        ),
        (S3Probe::CompletionTimestamp, completion),
        (S3Probe::CopyTimestamp, true),
        (S3Probe::Listing(S3Listing::V1), true),
        (S3Probe::Listing(settings.listing), true),
        (S3Probe::ChecksumMode, settings.request_checksums),
        (
            S3Probe::ChecksumReturned,
            settings.checksums != S3Checksums::Absent,
        ),
        (
            S3Probe::ChecksumScopeStated,
            settings.checksums != S3Checksums::UnstatedIgnored,
        ),
        (
            S3Probe::GoogleGeneration,
            settings.revision == S3Revision::Generation,
        ),
        (
            S3Probe::GoogleHeaders,
            settings.response_headers == S3ResponseHeaders::Google,
        ),
        (S3Probe::BucketList, true),
    ]
    .into_iter()
    .chain(match settings.multipart {
        S3Multipart::Copy(condition) => Some((S3Probe::CopyPublication(condition), true)),
        S3Multipart::ConditionalCompletion => None,
    })
    .map(|(probe, held)| S3Observation {
        probe,
        held,
        detail: "memory".into(),
    })
    .collect();
    S3Detection {
        versioning: Ok(S3BucketVersioning::Disabled),
        settings,
        observations,
    }
}
