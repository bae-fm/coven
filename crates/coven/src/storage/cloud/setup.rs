//! Cloud provider setup and management.
//!
//! Contains the OAuth sign-in flows for Google Drive, Dropbox, and OneDrive,
//! as well as managed service signup/login, disconnect, and account display logic.

use coven_foundation::config::{CloudProvider, Config};

/// Why building the sync storage from config failed. Each arm preserves the
/// typed error the layer below produced — notably [`CloudHomeError`], so the
/// [`CloudHomeError::is_retryable`] verdict survives up to the caller rather than
/// being flattened into a string here.
#[derive(Debug, thiserror::Error)]
pub enum StorageSetupError {
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] super::CloudHomeError),
    #[error("key error: {0}")]
    Key(#[from] crate::keys::KeyError),
    #[error("no encryption key found for an encrypted cloud home")]
    NoEncryptionKey,
    #[error("{provider:?} cannot provide exact protocol and blob slots with this configuration")]
    ExactSlotsUnavailable { provider: CloudProvider },
}

pub(crate) fn require_exact_slot_capabilities_config(
    config: &Config,
) -> Result<(), StorageSetupError> {
    let provider = config.cloud_home.provider.clone().ok_or_else(|| {
        super::CloudHomeError::Configuration(
            "sync requires a cloud provider with exact-slot storage".to_string(),
        )
    })?;
    if exact_slot_capabilities_supported(
        &provider,
        config.cloud_home.s3_endpoint.is_some(),
        config.cloud_home.s3_exact_slots,
    ) {
        Ok(())
    } else {
        Err(StorageSetupError::ExactSlotsUnavailable { provider })
    }
}

pub(crate) fn require_exact_slot_capabilities_join_info(
    join_info: &crate::storage::cloud::CloudHomeJoinInfo,
    custom_s3_exact_slots: Option<coven_foundation::config::CustomS3ExactSlots>,
) -> Result<(), CloudProvider> {
    let provider = join_info.cloud_provider();
    let custom_endpoint = matches!(
        join_info,
        crate::storage::cloud::CloudHomeJoinInfo::S3 {
            endpoint: Some(_),
            ..
        }
    );
    if exact_slot_capabilities_supported(&provider, custom_endpoint, custom_s3_exact_slots) {
        Ok(())
    } else {
        Err(provider)
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn require_exact_slot_capabilities_home(
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
    custom_s3_endpoint: bool,
    custom_s3_exact_slots: Option<coven_foundation::config::CustomS3ExactSlots>,
) -> bool {
    match provider {
        CloudProvider::S3 if !custom_s3_endpoint => true,
        CloudProvider::S3 => {
            custom_s3_exact_slots
                == Some(coven_foundation::config::CustomS3ExactSlots::StandardConditionalRequests)
        }
        CloudProvider::GoogleDrive
        | CloudProvider::Dropbox
        | CloudProvider::OneDrive
        | CloudProvider::CloudKit => true,
    }
}

/// Cloud provider setup error.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SetupError(pub String);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::CloudHomeJoinInfo;
    use coven_foundation::store_dir::StoreDir;

    #[test]
    fn exact_slot_admission_is_universal_and_uses_local_s3_assertions() {
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            StoreDir::new("unused-store-dir"),
            "Provider matrix".to_string(),
        );

        config.cloud_home.provider = Some(CloudProvider::S3);
        config.cloud_home.s3_endpoint = None;
        assert!(require_exact_slot_capabilities_config(&config).is_ok());

        config.cloud_home.s3_endpoint = Some("https://objects.example".to_string());
        assert!(matches!(
            require_exact_slot_capabilities_config(&config),
            Err(StorageSetupError::ExactSlotsUnavailable {
                provider: CloudProvider::S3
            })
        ));
        config.cloud_home.s3_exact_slots =
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests);
        assert!(require_exact_slot_capabilities_config(&config).is_ok());

        for provider in [
            CloudProvider::GoogleDrive,
            CloudProvider::Dropbox,
            CloudProvider::OneDrive,
            CloudProvider::CloudKit,
        ] {
            config.cloud_home.provider = Some(provider.clone());
            assert!(require_exact_slot_capabilities_config(&config).is_ok());
        }
    }

    #[test]
    fn custom_s3_join_requires_the_local_exact_slot_assertion() {
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };

        assert_eq!(
            require_exact_slot_capabilities_join_info(&join_info, None),
            Err(CloudProvider::S3),
        );
        assert!(require_exact_slot_capabilities_join_info(
            &join_info,
            Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        )
        .is_ok());
    }
}
