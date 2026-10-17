//! The in-memory provider as an application's storage connector: settings
//! saved through the builder's connector hook, S3 detection that conforms to
//! any settings, and the S3 new-store bucket check.
use super::*;

impl MemoryStorage {
    /// Bind a simulated installation to the real synchronization gate, preserving
    /// this provider's credentials, durable objects and request observations.
    pub fn with_publication_gate(&self, publications: Arc<dyn PublicationGate>) -> Self {
        Self::with_gate(
            self.provider.clone(),
            publications,
            self.cooldown.clone(),
            self.transfers.clone(),
        )
    }

    /// Bind this connection to the store's shared provider cooldown.
    pub fn with_cooldown(&self, cooldown: Arc<dyn crate::Cooldown>) -> Self {
        Self::with_gate(
            self.provider.clone(),
            self.publications.clone(),
            cooldown,
            self.transfers.clone(),
        )
    }

    /// Use the handle's transfer budget on this simulated connection.
    pub fn with_transfers(&self, transfers: Arc<crate::TransferBudget>) -> Self {
        Self::with_gate(
            self.provider.clone(),
            self.publications.clone(),
            self.cooldown.clone(),
            transfers,
        )
    }

    /// Erase the provider type while retaining this exact connection's credentials.
    pub fn shared_connection(&self) -> Arc<StorageConnection> {
        Arc::new(StorageConnection::with_gate(
            self.provider.clone() as Arc<dyn ProviderOps>,
            self.publications.clone(),
            self.cooldown.clone(),
            self.transfers.clone(),
        ))
    }
}

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
        auth: crate::ProviderAuth,
        publications: Arc<dyn crate::PublicationGate>,
        cooldown: Arc<dyn crate::Cooldown>,
        transfers: Arc<crate::TransferBudget>,
    ) -> Result<Arc<StorageConnection>, StorageError> {
        Ok(self
            .connect_device(config, auth)
            .await?
            .with_publication_gate(publications)
            .with_cooldown(cooldown)
            .with_transfers(transfers)
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
    copy_publication: S3CopyPublication::Never,
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
    let results = [
        (S3Probe::Signing(settings.signing), true),
        (S3Probe::SinglePut(settings.single_put), true),
        (S3Probe::PartialUploadHidden, true),
        (
            S3Probe::ConditionalCompletion,
            settings.copy_publication == S3CopyPublication::Never,
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
    .chain(
        settings
            .copy_publication
            .multipart()
            .map(|condition| (S3Probe::CopyPublication(condition), true)),
    )
    .map(|(probe, held)| S3ProbeResult {
        probe,
        held,
        detail: "memory".into(),
    })
    .collect();
    S3Detection {
        versioning: Ok(S3BucketVersioning::Disabled),
        settings,
        results,
    }
}
