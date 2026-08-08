//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

use coven_foundation::config::{CloudProvider, Config};

/// Why building the sync storage from config failed. Each arm preserves the
/// typed error the layer below produced — notably
/// [`CloudHomeError`](super::CloudHomeError), so the
/// [`is_retryable`](super::CloudHomeError::is_retryable) verdict survives up to
/// the caller rather than being flattened into a string here.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] super::CloudHomeError),
    #[error("key error: {0}")]
    Key(#[from] coven_keys::keys::KeyError),
    #[error("no encryption key found for an encrypted cloud home")]
    NoEncryptionKey,
    #[error("{provider:?} cannot provide exact protocol and blob slots with this configuration")]
    ExactSlotsUnavailable { provider: CloudProvider },
}

pub fn require_exact_slot_capabilities_config(config: &Config) -> Result<(), StorageSetupError> {
    let provider = config.cloud_home.provider.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration(
            "sync requires a cloud provider with exact-slot storage".to_string(),
        )
    })?;
    if exact_slot_capabilities_supported(&provider, config.cloud_home.exact_upload_verification) {
        Ok(())
    } else {
        Err(StorageSetupError::ExactSlotsUnavailable { provider })
    }
}

pub fn require_exact_slot_capabilities_join_info(
    join_info: &crate::cloud::CloudHomeJoinInfo,
    verification: coven_foundation::config::ExactUploadVerification,
) -> Result<(), CloudProvider> {
    let provider = join_info.cloud_provider();
    if exact_slot_capabilities_supported(&provider, verification) {
        Ok(())
    } else {
        Err(provider)
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub fn require_exact_slot_capabilities_home(
    home: std::sync::Arc<dyn super::CloudHome>,
    provider: Option<CloudProvider>,
) -> Result<(), StorageSetupError> {
    if home.exact_slot_storage().is_some() {
        Ok(())
    } else if let Some(provider) = provider {
        Err(StorageSetupError::ExactSlotsUnavailable { provider })
    } else {
        Err(super::CloudHomeError::Configuration(
            "sync requires a cloud provider with exact-slot storage".to_string(),
        )
        .into())
    }
}

fn exact_slot_capabilities_supported(
    provider: &CloudProvider,
    verification: coven_foundation::config::ExactUploadVerification,
) -> bool {
    !matches!(
        verification,
        coven_foundation::config::ExactUploadVerification::UploadChecksum
    ) || matches!(provider, CloudProvider::S3)
}

/// Cloud provider setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::CloudHomeJoinInfo;
    use coven_foundation::store_dir::StoreDir;

    #[test]
    fn exact_slot_admission_rejects_upload_checksums_where_the_provider_cannot_enforce_them() {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "Provider matrix".to_string(),
        );

        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        config.cloud_home.exact_upload_verification =
            coven_foundation::config::ExactUploadVerification::UploadChecksum;
        assert!(require_exact_slot_capabilities_config(&config).is_ok());

        for provider in [
            CloudProvider::GoogleDrive,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
            CloudProvider::CloudKit,
        ] {
            config.cloud_home.provider = Some(provider.clone());
            assert!(matches!(
                require_exact_slot_capabilities_config(&config),
                Err(StorageSetupError::ExactSlotsUnavailable { provider: rejected })
                    if rejected == provider
            ));
            config.cloud_home.exact_upload_verification =
                coven_foundation::config::ExactUploadVerification::MetadataHash;
            assert!(require_exact_slot_capabilities_config(&config).is_ok());
            config.cloud_home.exact_upload_verification =
                coven_foundation::config::ExactUploadVerification::UploadChecksum;
        }
    }

    #[test]
    fn join_admission_uses_the_local_verification_policy() {
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };

        assert!(require_exact_slot_capabilities_join_info(
            &join_info,
            coven_foundation::config::ExactUploadVerification::UploadChecksum,
        )
        .is_ok());
    }
}
