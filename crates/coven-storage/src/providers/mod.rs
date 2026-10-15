//! Built-in provider network implementations.
mod access;
mod admin_access;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use admin_access::DesktopDeviceSignIn;
pub use admin_access::{obtain_admin_access, AdminAccessSource, DeviceSignInPresenter};
mod http;
mod http_body;
mod oauth;
mod object_metadata;
mod pagination;
pub use http::{OAuthSession, ProviderResponse};
pub use oauth::{
    DropboxClients, GoogleDriveClient, OAuthClients, OAuthError, OAuthFlow, OAuthPresenter,
    OAuthRedirectError,
};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
mod desktop_oauth;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use desktop_oauth::DesktopOAuthPresenter;
mod s3;
pub(crate) mod s3_admin_query;
/// The prefix policy a member key needs, as guided console steps show it (§4).
pub fn s3_member_policy(
    config: &crate::StorageConfig,
    store: coven_foundation::id_source::StoreId,
) -> Result<String, crate::StorageError> {
    s3::member_policy(config, store)
}
/// Whether a provider failure's native cause is a response the provider sent,
/// even an error one (E5).
pub(crate) fn response_arrived(source: &(dyn std::error::Error + 'static)) -> bool {
    source.is::<ProviderResponse>()
        || source.is::<OAuthRedirectError>()
        || s3::response_status(source).is_some()
}
pub(crate) use s3::{
    admit_s3_setup, check_new_store as check_new_s3_store, check_s3_versioning,
    detect as detect_s3, s3_key_scope, s3_settings, S3KeyScope, S3Storage,
};
mod google_drive;
mod google_drive_access;
pub(crate) use google_drive::GoogleDriveStorage;
mod dropbox;
mod dropbox_access;
pub(crate) use dropbox::DropboxStorage;
mod onedrive;
mod onedrive_access;
pub(crate) use onedrive::OneDriveStorage;
#[cfg(target_vendor = "apple")]
mod cloudkit;
#[cfg(target_vendor = "apple")]
pub(crate) use cloudkit::CloudKitStorage;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

mod connector;
pub use connector::{ProviderConnector, StorageConnector};
