//! CloudHome: low-level cloud storage abstraction.
//!
//! Each backend (S3, R2, B2, etc.) implements `CloudHome` -- 8 methods for
//! raw bytes in/out. No encryption, no path layout knowledge, no sync
//! semantics. Higher-level concerns live in `CloudSyncStorage` which wraps any
//! `dyn CloudHome` and applies the path layout and at-rest protection.

pub mod cloudkit;
pub mod dropbox;
pub mod google_drive;
pub mod oauth_session;
pub mod onedrive;
pub mod s3;
pub mod setup;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Errors from raw cloud storage operations.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Information needed to join a cloud home from another device.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CloudHomeJoinInfo {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
        #[serde(default)]
        key_prefix: Option<String>,
    },
    GoogleDrive {
        folder_id: String,
    },
    Dropbox {
        shared_folder_id: String,
    },
    OneDrive {
        drive_id: String,
        folder_id: String,
    },
    CloudKit {
        share_url: String,
    },
}

impl CloudHomeJoinInfo {
    pub fn cloud_provider(&self) -> crate::config::CloudProvider {
        use crate::config::CloudProvider;
        match self {
            CloudHomeJoinInfo::S3 { .. } => CloudProvider::S3,
            CloudHomeJoinInfo::GoogleDrive { .. } => CloudProvider::GoogleDrive,
            CloudHomeJoinInfo::Dropbox { .. } => CloudProvider::Dropbox,
            CloudHomeJoinInfo::OneDrive { .. } => CloudProvider::OneDrive,
            CloudHomeJoinInfo::CloudKit { .. } => CloudProvider::CloudKit,
        }
    }
}

/// Reports how many bytes of a `write` have reached the backend so far.
/// Called with the cumulative byte count as the body uploads; backends that
/// can't observe sub-call progress call it once at the end with the full size.
/// The count is of the bytes handed to `write` (the encrypted payload).
pub type UploadProgress<'a> = dyn Fn(u64) + Send + Sync + 'a;

/// Chunk size the in-memory test backend uses to drive its `UploadProgress`
/// callback in several ticks. Real providers whose resumable API mandates a
/// specific alignment (OneDrive 320 KiB multiples, Google Drive 256 KiB
/// multiples, S3 5 MiB minimum parts) define their own constant.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) const PROGRESS_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// A progress sink that discards its reports. For `write` calls whose payload
/// is a small control file (auth keys, head pointers, the snapshot) where no
/// per-file progress bar is driven — only the blob outbox surfaces progress.
pub fn no_progress() -> impl Fn(u64) + Send + Sync {
    |_| {}
}

/// Low-level cloud storage. Implementations handle a single library.
///
/// All methods deal in raw bytes. No encryption or path layout logic.
#[async_trait]
pub trait CloudHome: Send + Sync {
    /// Verify the backend is reachable with the configured credentials.
    /// Setup flows call this *before* persisting credentials, so a typo or
    /// missing bucket fails fast at setup time instead of via a delayed
    /// reconnect banner. Default implementation issues a no-op list against
    /// a sentinel prefix — backends override with cheaper provider-specific
    /// auth checks (e.g. S3 HeadBucket) where available.
    async fn probe(&self) -> Result<(), CloudHomeError> {
        self.list("__coven_probe__").await.map(drop)
    }

    /// Write bytes to a key, creating or overwriting. `progress` is called with
    /// the cumulative count of bytes sent so the caller can drive a per-file
    /// progress bar; backends that can't observe sub-call progress call it once
    /// at the end with `data.len()`.
    async fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        progress: &UploadProgress<'_>,
    ) -> Result<(), CloudHomeError>;

    /// Read the full contents of a key.
    async fn read(&self, key: &str) -> Result<Vec<u8>, CloudHomeError>;

    /// Read a byte range from a key. `start` is inclusive, `end` is exclusive.
    async fn read_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, CloudHomeError>;

    /// List all keys under a prefix.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, CloudHomeError>;

    /// Delete a key. Not an error if the key does not exist.
    async fn delete(&self, key: &str) -> Result<(), CloudHomeError>;

    /// Check whether a key exists.
    async fn exists(&self, key: &str) -> Result<bool, CloudHomeError>;

    /// Grant access to a member and return connection info for the cloud home.
    /// For S3 this ignores `member_id` and returns bucket/region/endpoint
    /// (access is managed externally via IAM/pre-shared credentials).
    /// For consumer clouds this shares the folder with the member's account.
    async fn grant_access(&self, member_id: &str) -> Result<CloudHomeJoinInfo, CloudHomeError>;

    /// Revoke a previously granted access. No-op for backends where access
    /// is controlled externally (e.g. S3 with pre-shared credentials).
    async fn revoke_access(&self, member_id: &str) -> Result<(), CloudHomeError>;
}

/// Extract the OAuth token JSON from cloud home credentials, or return a storage error.
fn require_oauth_token(
    key_service: &crate::keys::KeyService,
    provider_name: &str,
) -> Result<String, CloudHomeError> {
    match key_service
        .get_cloud_home_credentials()
        .map_err(|e| CloudHomeError::Storage(format!("{provider_name} credentials error: {e}")))?
    {
        Some(crate::keys::CloudHomeCredentials::OAuth { token_json }) => Ok(token_json),
        _ => Err(CloudHomeError::Storage(format!(
            "{provider_name} OAuth token not in keyring"
        ))),
    }
}

fn parse_oauth_tokens(
    key_service: &crate::keys::KeyService,
    provider_name: &str,
) -> Result<crate::oauth::OAuthTokens, CloudHomeError> {
    let token_json = require_oauth_token(key_service, provider_name)?;
    serde_json::from_str(&token_json)
        .map_err(|e| CloudHomeError::Storage(format!("invalid OAuth token JSON: {e}")))
}

/// Construct a CloudHome from the desktop app's Config + KeyService.
/// Reads provider settings from config and credentials from the OS keyring.
pub async fn create_cloud_home(
    config: &crate::config::Config,
    key_service: &crate::keys::KeyService,
    clock: crate::clock::ClockRef,
) -> Result<Box<dyn CloudHome>, CloudHomeError> {
    use crate::config::CloudProvider;

    match config.cloud_home.provider {
        Some(CloudProvider::S3) | None => {
            let bucket =
                config.cloud_home.s3_bucket.clone().ok_or_else(|| {
                    CloudHomeError::Storage("S3 bucket not configured".to_string())
                })?;
            let region =
                config.cloud_home.s3_region.clone().ok_or_else(|| {
                    CloudHomeError::Storage("S3 region not configured".to_string())
                })?;
            let endpoint = config.cloud_home.s3_endpoint.clone();

            let (access_key, secret_key) = match key_service
                .get_cloud_home_credentials()
                .map_err(|e| CloudHomeError::Storage(format!("S3 credentials error: {e}")))?
            {
                Some(crate::keys::CloudHomeCredentials::S3 {
                    access_key,
                    secret_key,
                }) => (access_key, secret_key),
                _ => {
                    return Err(CloudHomeError::Storage(
                        "S3 credentials not in keyring".to_string(),
                    ))
                }
            };

            let s3 = s3::S3CloudHome::new(
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                config.cloud_home.s3_key_prefix.clone(),
            )
            .await?;
            Ok(Box::new(s3))
        }
        Some(CloudProvider::GoogleDrive) => {
            let folder_id = config
                .cloud_home
                .google_drive_folder_id
                .clone()
                .ok_or_else(|| {
                    CloudHomeError::Storage("Google Drive folder ID not configured".to_string())
                })?;
            let tokens = parse_oauth_tokens(key_service, "Google Drive")?;
            Ok(Box::new(google_drive::GoogleDriveCloudHome::new(
                folder_id,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        Some(CloudProvider::Dropbox) => {
            let folder_path = config
                .cloud_home
                .dropbox_folder_path
                .clone()
                .ok_or_else(|| {
                    CloudHomeError::Storage("Dropbox folder path not configured".to_string())
                })?;
            let tokens = parse_oauth_tokens(key_service, "Dropbox")?;
            Ok(Box::new(dropbox::DropboxCloudHome::new(
                folder_path,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        Some(CloudProvider::OneDrive) => {
            let drive_id = config.cloud_home.onedrive_drive_id.clone().ok_or_else(|| {
                CloudHomeError::Storage("OneDrive drive ID not configured".to_string())
            })?;
            let folder_id = config
                .cloud_home
                .onedrive_folder_id
                .clone()
                .ok_or_else(|| {
                    CloudHomeError::Storage("OneDrive folder ID not configured".to_string())
                })?;
            let tokens = parse_oauth_tokens(key_service, "OneDrive")?;
            Ok(Box::new(onedrive::OneDriveCloudHome::new(
                drive_id,
                folder_id,
                tokens,
                key_service.clone(),
                clock,
            )))
        }
        Some(CloudProvider::CloudKit) => Err(CloudHomeError::Storage(
            "CloudKit requires the native Swift driver; construct via the host's Swift layer"
                .to_string(),
        )),
    }
}
