mod objects;
mod protocol;
mod remote;

pub(crate) mod cloud;

pub(crate) use objects::*;
pub(crate) use protocol::*;
pub(crate) use remote::*;

pub use protocol::{
    AwsPrincipal, CloudKitEnvironment, GoogleDriveCorpus, ProviderDeviceBinding,
    ProviderPrincipalId, ResolvedProviderBinding, S3EndpointBinding, StoreProviderBinding,
};
#[cfg(any(test, feature = "test-utils"))]
pub use remote::CloudCipher;

pub use cloud::cloudkit::{
    CloudKitAcceptedShareRecord, CloudKitAtomicCreateBatch, CloudKitOps, CloudKitProviderIdentity,
    CloudKitRecordCreate, CloudKitRecordVersion, CloudKitScope, CloudKitShare,
    CloudKitShareAcceptance, CloudKitSharePermission,
};
pub use cloud::s3::S3CloudHome;
pub use cloud::setup::generate_restore_code;
#[cfg(any(test, feature = "test-utils"))]
pub use cloud::test_utils::InMemoryCloudHome;

#[cfg(feature = "oauth-providers")]
pub async fn fetch_account_email(
    provider: crate::config::CloudProvider,
    tokens: &crate::oauth::OAuthTokens,
) -> Result<String, crate::oauth::OAuthError> {
    use crate::config::CloudProvider;

    let result = match provider {
        CloudProvider::GoogleDrive => cloud::account_email::fetch_google(tokens).await,
        CloudProvider::Dropbox => cloud::account_email::fetch_dropbox(tokens).await,
        CloudProvider::OneDrive => cloud::account_email::fetch_onedrive(tokens).await,
        other => {
            return Err(crate::oauth::OAuthError::AccountFetch(format!(
                "{other:?} does not use OAuth; account email is only fetched for OAuth providers"
            )));
        }
    };
    result.map_err(|error| crate::oauth::OAuthError::AccountFetch(error.to_string()))
}
pub use cloud::{
    create_cloud_home, write_cloud_object_stream, BlobBody, BoxPartSink, CloudAccessOutcome,
    CloudAccessState, CloudFileReadError, CloudHome, CloudHomeError, CloudHomeJoinInfo,
    CloudObjectStream, CloudObjectVersion, CloudVersionedObject, ExactSlotStorage, ObjectSlot,
    PartSink, PhysicalObjectLocator, UploadProgress,
};
