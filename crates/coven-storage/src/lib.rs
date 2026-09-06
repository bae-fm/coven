//! The cloud home and what Coven keeps in it.
//!
//! A `CloudHome` is raw bytes in and out of one provider — S3 and its
//! compatibles, CloudKit, and the OAuth providers (Google Drive, Dropbox,
//! OneDrive). Above it, `CloudSyncConnection` applies the key layout and the
//! at-rest protection, and exposes the exact-slot protocol-object and blob
//! operations replication runs against. Beside them sit the join and restore
//! codes that carry a home's coordinates between devices, and the OAuth
//! authorization flow that obtains a provider session in the first place.

pub mod cloud;
mod cloud_object_storage;
mod local_file;
pub mod oauth;
mod objects;
pub mod provider_probe;
mod remote;

pub use cloud_object_storage::*;
pub use objects::*;
pub use remote::*;

pub use cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
    CloudKitShareAcceptance, CloudKitSharePermission,
};
pub use cloud::s3::S3CloudHome;
#[cfg(any(test, feature = "test-utils"))]
pub use cloud::test_utils::InMemoryCloudHome;
pub use cloud::{
    no_progress, write_cloud_object_stream, BlobBody, BoxPartSink, CloudAccessOutcome,
    CloudAccessState, CloudFileReadError, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    CloudObjectStream, CloudObjectVersion, CloudVersionedObject, ConditionalWriteOutcome,
    DownloadProgress, ExactCloudHome, ExactCreateOutcome, ExactSlotStorage, ExactUpload,
    ExactUploadSource, PartSink, UploadControl, UploadProgress,
};

#[cfg(feature = "oauth-providers")]
pub async fn fetch_account_email(
    provider: coven_foundation::config::CloudProvider,
    tokens: &crate::oauth::OAuthTokens,
) -> Result<String, crate::oauth::OAuthError> {
    use coven_foundation::config::CloudProvider;

    let result = match provider {
        CloudProvider::GoogleDrive => cloud::account_email::fetch_google(tokens).await,
        CloudProvider::Dropbox => cloud::account_email::fetch_dropbox(tokens).await,
        CloudProvider::OneDrive => cloud::account_email::fetch_onedrive(tokens).await,
        other => {
            return Err(crate::oauth::OAuthError::UnsupportedProvider(other));
        }
    };
    result.map_err(crate::oauth::OAuthError::AccountFetch)
}
