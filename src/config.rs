//! Sync + storage configuration.
//!
//! `Config` is a plain runtime struct the host populates from its own config.
//! coven never persists it — the host owns its config file and maps the
//! sync-relevant fields into `Config` when constructing the sync manager.

use serde::{Deserialize, Serialize};

use crate::library_dir::LibraryDir;

/// Cloud home provider selection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CloudProvider {
    S3,
    GoogleDrive,
    Dropbox,
    OneDrive,
    CloudKit,
}

impl CloudProvider {
    /// Whether connecting, restoring, or joining on this provider requires
    /// running an OAuth flow first — true for the account-based consumer clouds
    /// (Google Drive, Dropbox, OneDrive), false for S3 and CloudKit.
    pub fn needs_oauth(&self) -> bool {
        matches!(self, Self::GoogleDrive | Self::Dropbox | Self::OneDrive)
    }
}

/// How a cloud home stores its objects: opaque (encrypted, unreadable to anyone
/// who can read the bucket) or browsable (stored in the clear at readable paths).
/// This is *not* about who can reach the bucket — the storage provider's own
/// access control applies either way; it is about whether what they store is
/// legible. The host picks it once, when it creates the home; it cannot change
/// later (it determines how every object is written). One choice drives two
/// mechanisms together:
///
/// - `Opaque` (the default): every object is encrypted at rest under the library
///   key (the `.enc` suffix) and blobs use coven's content-addressed path
///   `{namespace}/{ab}/{cd}/{id}`. Anyone with bucket access sees only ciphertext
///   under opaque keys. Sharing a library (inviting members) requires an opaque
///   home, because it wraps and rotates the library key.
/// - `Browsable`: every object is stored in the clear (no `.enc` suffix) and
///   blobs use the consumer-supplied readable path `{namespace}/{cloud_path}`, so
///   anyone with bucket access can read the actual files by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HomeStorage {
    Opaque,
    Browsable,
}

impl HomeStorage {
    /// An opaque home is encrypted at rest and obfuscates its blob paths; a
    /// browsable home does neither.
    pub fn is_opaque(self) -> bool {
        matches!(self, HomeStorage::Opaque)
    }

    /// Whether this home stores its objects in the clear at readable paths (the
    /// inverse of [`Self::is_opaque`]).
    pub fn is_browsable(self) -> bool {
        matches!(self, HomeStorage::Browsable)
    }
}

/// The cloud home: which provider backs sync and its per-provider settings.
/// One cohesive unit — connecting picks a provider and fills its fields;
/// disconnecting resets the whole thing to default.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloudHomeConfig {
    /// Selected provider. None = not configured.
    #[serde(default)]
    pub provider: Option<CloudProvider>,
    #[serde(default)]
    pub s3_bucket: Option<String>,
    #[serde(default)]
    pub s3_region: Option<String>,
    #[serde(default)]
    pub s3_endpoint: Option<String>,
    #[serde(default)]
    pub s3_key_prefix: Option<String>,
    #[serde(default)]
    pub google_drive_folder_id: Option<String>,
    #[serde(default)]
    pub dropbox_folder_path: Option<String>,
    #[serde(default)]
    pub onedrive_drive_id: Option<String>,
    #[serde(default)]
    pub onedrive_folder_id: Option<String>,
    /// Whether this library's CloudKit zone is shared (joiner) vs owned (creator).
    #[serde(default)]
    pub cloudkit_is_shared: bool,
    /// How this home stores its objects: opaque ([`HomeStorage::Opaque`]) or
    /// browsable ([`HomeStorage::Browsable`]). Drives both the at-rest cipher and
    /// the blob-path scheme — see [`HomeStorage`].
    ///
    /// Defaults to `Opaque`: the default home is encrypted with obfuscated blob
    /// paths, and an on-disk config written before this field existed describes
    /// such a home, so an absent value reads back as `Opaque`.
    #[serde(default = "default_storage")]
    pub storage: HomeStorage,
}

/// The default for [`CloudHomeConfig::storage`]: every home is opaque (encrypted)
/// unless the host explicitly creates a browsable one.
fn default_storage() -> HomeStorage {
    HomeStorage::Opaque
}

impl Default for CloudHomeConfig {
    fn default() -> Self {
        Self {
            provider: None,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_key_prefix: None,
            google_drive_folder_id: None,
            dropbox_folder_path: None,
            onedrive_drive_id: None,
            onedrive_folder_id: None,
            cloudkit_is_shared: false,
            storage: default_storage(),
        }
    }
}

/// Configuration errors.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Sync + storage configuration for one library.
#[derive(Clone, Debug)]
pub struct Config {
    pub library_id: String,
    /// Unique device identifier for sync changeset namespacing.
    pub device_id: String,
    pub library_dir: LibraryDir,
    pub library_name: String,
    /// Whether an encryption key is stored in the keyring (hint flag).
    pub encryption_key_stored: bool,
    /// SHA-256 fingerprint of the encryption key (detects wrong key without decryption).
    pub encryption_key_fingerprint: Option<String>,
    /// Cloud home provider + its settings.
    pub cloud_home: CloudHomeConfig,
}

impl Config {
    /// Whether sync is configured: a provider is selected and the matching
    /// settings + credentials are present.
    pub fn sync_enabled(&self, key_service: &crate::keys::KeyService) -> bool {
        use crate::keys::CloudHomeCredentials;

        let creds = key_service
            .get_cloud_home_credentials()
            .unwrap_or_else(|e| {
                tracing::warn!("reading cloud home credentials for sync_enabled: {e}");
                None
            });
        let has_s3 = matches!(creds, Some(CloudHomeCredentials::S3 { .. }));
        let has_oauth = matches!(creds, Some(CloudHomeCredentials::OAuth { .. }));

        let ch = &self.cloud_home;
        match ch.provider {
            Some(CloudProvider::S3) => ch.s3_bucket.is_some() && ch.s3_region.is_some() && has_s3,
            Some(CloudProvider::GoogleDrive) => ch.google_drive_folder_id.is_some() && has_oauth,
            Some(CloudProvider::Dropbox) => ch.dropbox_folder_path.is_some() && has_oauth,
            Some(CloudProvider::OneDrive) => {
                ch.onedrive_drive_id.is_some() && ch.onedrive_folder_id.is_some() && has_oauth
            }
            Some(CloudProvider::CloudKit) => true,
            None => false,
        }
    }

    /// Construct a config with defaults for a new or joined library.
    pub fn with_defaults(
        library_id: String,
        device_id: String,
        library_dir: LibraryDir,
        library_name: String,
    ) -> Self {
        Self {
            library_id,
            device_id,
            library_dir,
            library_name,
            encryption_key_stored: false,
            encryption_key_fingerprint: None,
            cloud_home: CloudHomeConfig::default(),
        }
    }

    /// Persist the sync config to `library_dir/config.yaml`.
    pub fn save(&self) -> Result<(), ConfigError> {
        self.save_to_config_yaml()
    }

    /// Persist the sync config to `library_dir/config.yaml`.
    pub fn save_to_config_yaml(&self) -> Result<(), ConfigError> {
        std::fs::create_dir_all(&*self.library_dir)?;
        let yaml: ConfigYaml = self.into();
        let text =
            serde_yaml::to_string(&yaml).map_err(|e| ConfigError::Serialization(e.to_string()))?;
        std::fs::write(self.library_dir.config_path(), text)?;
        Ok(())
    }
}

/// On-disk form of [`Config`] (the runtime `library_dir` is supplied separately).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigYaml {
    pub library_id: String,
    pub library_name: String,
    #[serde(default)]
    pub device_id: Option<String>,
    #[serde(default)]
    pub encryption_key_stored: bool,
    #[serde(default)]
    pub encryption_key_fingerprint: Option<String>,
    #[serde(default, flatten)]
    pub cloud_home: CloudHomeConfig,
}

impl ConfigYaml {
    /// Build a runtime [`Config`] from the on-disk form, supplying the resolved
    /// device id and library directory.
    pub fn into_config(self, device_id: String, library_dir: LibraryDir) -> Config {
        Config {
            library_id: self.library_id,
            device_id,
            library_dir,
            library_name: self.library_name,
            encryption_key_stored: self.encryption_key_stored,
            encryption_key_fingerprint: self.encryption_key_fingerprint,
            cloud_home: self.cloud_home,
        }
    }
}

impl From<&Config> for ConfigYaml {
    fn from(config: &Config) -> Self {
        Self {
            library_id: config.library_id.clone(),
            library_name: config.library_name.clone(),
            device_id: Some(config.device_id.clone()),
            encryption_key_stored: config.encryption_key_stored,
            encryption_key_fingerprint: config.encryption_key_fingerprint.clone(),
            cloud_home: config.cloud_home.clone(),
        }
    }
}
