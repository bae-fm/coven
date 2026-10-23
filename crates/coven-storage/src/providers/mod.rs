//! Built-in provider network implementations.
mod access;
mod error_text;
mod http;
mod http_body;
mod http_client;
mod oauth;
mod oauth_folders;
pub use oauth_folders::StorageFolder;
mod object_metadata;
mod onedrive_item;
mod pagination;
pub use http::{OAuthSession, ProviderResponse};
pub use oauth::{
    DropboxClients, OAuthClients, OAuthError, OAuthFlow, OAuthPresenter, OAuthRedirectError,
    OAuthTokenRefresh,
};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
mod desktop_oauth;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use desktop_oauth::DesktopOAuthPresenter;
mod s3;
/// The prefix policy a member key needs, as guided console steps show it (§20.10).
pub fn s3_member_policy(
    config: &crate::StorageConfig,
    store: coven_foundation::id_source::StoreId,
) -> Result<String, crate::StorageError> {
    s3::member_policy(config, store)
}
/// Whether a provider failure's native cause is a response the provider sent,
/// even an error one (E5).
pub(crate) fn response_arrived(source: &(dyn std::error::Error + 'static)) -> bool {
    #[cfg(target_vendor = "apple")]
    if let Some(native) = source.downcast_ref::<cloudkit::NativeError>() {
        return native.answered();
    }
    source.is::<ProviderResponse>()
        || source.is::<OAuthRedirectError>()
        || s3::response_status(source).is_some()
}
/// The wait a provider failure's native response asked every consumer to
/// observe: HTTP `Retry-After`, Dropbox's `retry_after`, or CloudKit's
/// `CKErrorRetryAfterKey` (§6.7).
pub(crate) fn retry_after(
    provider: crate::CloudProvider,
    source: &(dyn std::error::Error + 'static),
) -> Option<crate::RetryAfter> {
    #[cfg(target_vendor = "apple")]
    if let Some(native) = source.downcast_ref::<cloudkit::NativeError>() {
        return native.retry_after();
    }
    if let Some(response) = source.downcast_ref::<ProviderResponse>() {
        return response.retry_after(provider);
    }
    s3::response_retry_after(source)
}
#[cfg(feature = "dev-tools")]
pub(crate) use s3::empty_prefix as empty_s3_prefix;
#[cfg(any(test, feature = "dev-tools"))]
pub use s3::EmptiedS3Prefix;
#[cfg(feature = "dev-tools")]
pub use s3::EmptyS3PrefixError;
pub(crate) use s3::{
    check_new_store as check_new_s3_store, check_s3_versioning, detect as detect_s3, s3_key_scope,
    s3_settings, S3KeyScope, S3Storage,
};
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
