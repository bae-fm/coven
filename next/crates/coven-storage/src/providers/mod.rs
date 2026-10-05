//! Provider network implementations and the app's CloudKit bridge.
mod http;
mod oauth;
pub use http::{OAuthSession, ProviderResponse};
pub use oauth::{AuthorizeRequest, OAuthClients, OAuthError};
mod s3;
pub use s3::S3Storage;
mod google_drive;
pub use google_drive::GoogleDriveStorage;
mod dropbox;
pub use dropbox::DropboxStorage;
mod onedrive;
pub use onedrive::OneDriveStorage;
mod cloudkit;
pub use cloudkit::{CloudKitOps, CloudKitStorage, CloudKitUpload, CloudKitUploadStatus};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
