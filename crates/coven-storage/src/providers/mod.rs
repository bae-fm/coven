//! Provider network implementations and the app's CloudKit bridge.
mod access;
mod http;
mod oauth;
pub use http::{OAuthSession, ProviderResponse};
pub use oauth::{OAuthClients, OAuthError, OAuthFlow, OAuthPresenter};
#[cfg(not(any(target_os = "ios", target_os = "android")))]
mod desktop_oauth;
#[cfg(not(any(target_os = "ios", target_os = "android")))]
pub use desktop_oauth::DesktopOAuthPresenter;
mod s3;
pub use s3::S3Storage;
mod google_drive;
mod google_drive_access;
pub use google_drive::GoogleDriveStorage;
mod dropbox;
mod dropbox_access;
pub use dropbox::DropboxStorage;
mod onedrive;
mod onedrive_access;
pub use onedrive::OneDriveStorage;
mod cloudkit;
pub use cloudkit::{CloudKitOps, CloudKitStorage, CloudKitUpload, CloudKitUploadStatus};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

mod connector;
pub use connector::{ProviderConnector, StorageConnector};
