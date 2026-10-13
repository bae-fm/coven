//! Built-in provider network implementations.
mod access;
mod http;
mod oauth;
mod object_metadata;
mod pagination;
pub use http::{OAuthSession, ProviderResponse};
pub use oauth::{OAuthClients, OAuthError, OAuthFlow, OAuthPresenter, OAuthRedirectError};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
mod desktop_oauth;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use desktop_oauth::DesktopOAuthPresenter;
mod s3;
pub(crate) use s3::S3Storage;
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
