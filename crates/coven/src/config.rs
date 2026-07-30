//! Sync + storage configuration.
//!
//! `Config` is the runtime struct the sync manager reads. coven persists the
//! sync-relevant fields to `config.yaml` in the store directory
//! ([`Config::save_to_config_yaml`]) and reads them back
//! ([`Config::load_from_config_yaml`]); the store directory itself is not
//! part of the file — the caller supplies it at both save and load.

use serde::{Deserialize, Serialize};

use crate::store_dir::StoreDir;

/// Cloud home provider selection.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum CloudProvider {
    S3,
    GoogleDrive,
    Dropbox,
    OneDrive,
    CloudKit,
}

/// Local operator assertion required before a custom S3 endpoint may provide
/// exact protocol and blob slots. This is local configuration and is never
/// accepted from invite or restore data.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustomS3ExactSlots {
    StandardConditionalRequests,
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
/// - `Opaque` (the default): every object is encrypted at rest under the store
///   key (the `.enc` suffix) and blobs use coven's content-addressed path under
///   the uploading device, `{namespace}/{uploader}/{ab}/{cd}/{id}`. Anyone with
///   bucket access sees only ciphertext
///   under opaque keys. Sharing a store (inviting members) requires an opaque
///   home, because it wraps and rotates the store key.
/// - `Browsable`: every object is stored in the clear (no `.enc` suffix) and
///   blobs use the consumer-supplied readable path `{namespace}/{cloud_path}`, so
///   anyone with bucket access can read the actual files by name. Browsable
///   storage cannot be combined with per-row audiences declared through
///   [`crate::SyncedTable::scoped_by`].
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    pub s3_exact_slots: Option<CustomS3ExactSlots>,
    #[serde(default)]
    pub google_drive_folder_id: Option<String>,
    #[serde(default)]
    pub dropbox_folder_path: Option<String>,
    #[serde(default)]
    pub onedrive_drive_id: Option<String>,
    #[serde(default)]
    pub onedrive_folder_id: Option<String>,
    #[serde(default)]
    pub cloudkit_owner_name: Option<String>,
    #[serde(default)]
    pub cloudkit_zone_name: Option<String>,
    /// How this home stores its objects: opaque ([`HomeStorage::Opaque`]) or
    /// browsable ([`HomeStorage::Browsable`]). Drives both the at-rest cipher and
    /// the blob-path scheme — see [`HomeStorage`].
    pub storage: HomeStorage,
}

impl Default for CloudHomeConfig {
    fn default() -> Self {
        Self {
            provider: None,
            s3_bucket: None,
            s3_region: None,
            s3_endpoint: None,
            s3_key_prefix: None,
            s3_exact_slots: None,
            google_drive_folder_id: None,
            dropbox_folder_path: None,
            onedrive_drive_id: None,
            onedrive_folder_id: None,
            cloudkit_owner_name: None,
            cloudkit_zone_name: None,
            storage: HomeStorage::Opaque,
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
    #[error("store already exists locally: {0}")]
    StoreExists(String),
}

/// Sync + storage configuration for one store.
#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub store_id: String,
    /// Unique device identifier for sync changeset namespacing.
    pub device_id: String,
    pub store_dir: StoreDir,
    pub store_name: String,
    /// Whether an encryption key is stored in the keyring (hint flag).
    pub encryption_key_stored: bool,
    /// SHA-256 fingerprint of the encryption key (detects wrong key without decryption).
    pub encryption_key_fingerprint: Option<String>,
    /// Cloud home provider + its settings.
    pub cloud_home: CloudHomeConfig,
}

impl Config {
    /// Construct a config with defaults for a new or joined store.
    pub fn with_defaults(
        store_id: String,
        device_id: String,
        store_dir: StoreDir,
        store_name: String,
    ) -> Self {
        Self {
            store_id,
            device_id,
            store_dir,
            store_name,
            encryption_key_stored: false,
            encryption_key_fingerprint: None,
            cloud_home: CloudHomeConfig::default(),
        }
    }

    /// Persist the sync config to `store_dir/config.yaml`.
    pub fn save_to_config_yaml(&self) -> Result<(), ConfigError> {
        let yaml: ConfigYaml = self.into();
        let text =
            serde_yaml::to_string(&yaml).map_err(|e| ConfigError::Serialization(e.to_string()))?;
        crate::local_blob::write_atomic_durable_blocking(
            &self.store_dir.config_path(),
            text.as_bytes(),
        )
        .map_err(ConfigError::Config)
    }

    /// Read `store_dir/config.yaml` back into a runtime `Config`; `store_dir`
    /// is supplied by the caller, since it is not itself persisted (see the
    /// module doc). A missing or unparseable file is a loud [`ConfigError`]
    /// naming the path.
    pub fn load_from_config_yaml(store_dir: StoreDir) -> Result<Config, ConfigError> {
        let path = store_dir.config_path();
        let text = std::fs::read_to_string(&path)
            .map_err(|e| ConfigError::Config(format!("failed to read {}: {e}", path.display())))?;
        let yaml: ConfigYaml = serde_yaml::from_str(&text)
            .map_err(|e| ConfigError::Config(format!("failed to parse {}: {e}", path.display())))?;
        Ok(yaml.into_config(store_dir))
    }
}

/// On-disk form of [`Config`] (the runtime `store_dir` is supplied separately).
///
/// This is the `config.yaml` wire format, not published API: hosts read and
/// write it through [`Config::save_to_config_yaml`] and
/// [`Config::load_from_config_yaml`], which are the only things that name it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ConfigYaml {
    pub(crate) store_id: String,
    pub(crate) store_name: String,
    pub(crate) device_id: String,
    #[serde(default)]
    pub(crate) encryption_key_stored: bool,
    #[serde(default)]
    pub(crate) encryption_key_fingerprint: Option<String>,
    #[serde(default, flatten)]
    pub(crate) cloud_home: CloudHomeConfig,
}

impl From<&Config> for ConfigYaml {
    fn from(config: &Config) -> Self {
        Self {
            store_id: config.store_id.clone(),
            store_name: config.store_name.clone(),
            device_id: config.device_id.clone(),
            encryption_key_stored: config.encryption_key_stored,
            encryption_key_fingerprint: config.encryption_key_fingerprint.clone(),
            cloud_home: config.cloud_home.clone(),
        }
    }
}

impl ConfigYaml {
    /// Pair to [`From<&Config> for ConfigYaml`]: rebuild a runtime `Config`
    /// from its on-disk form plus the `store_dir` the file doesn't carry.
    fn into_config(self, store_dir: StoreDir) -> Config {
        Config {
            store_id: self.store_id,
            device_id: self.device_id,
            store_dir,
            store_name: self.store_name,
            encryption_key_stored: self.encryption_key_stored,
            encryption_key_fingerprint: self.encryption_key_fingerprint,
            cloud_home: self.cloud_home,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_requirement_follows_the_provider() {
        assert!(!CloudProvider::S3.needs_oauth());
        assert!(!CloudProvider::CloudKit.needs_oauth());
        assert!(CloudProvider::GoogleDrive.needs_oauth());
        assert!(CloudProvider::Dropbox.needs_oauth());
        assert!(CloudProvider::OneDrive.needs_oauth());
    }

    /// Saving a `Config` and loading it back must reproduce every field,
    /// including `store_dir` — the caller supplies the same `store_dir` to
    /// both `save_to_config_yaml` (via `self`) and `load_from_config_yaml`,
    /// so it must come back unchanged along with everything the file does
    /// carry.
    #[test]
    fn round_trips_through_save_and_load() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(dir.path());
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            store_dir.clone(),
            "My Store".to_string(),
        );
        config.encryption_key_stored = true;
        config.encryption_key_fingerprint = Some("abc123".to_string());
        config.cloud_home = CloudHomeConfig {
            provider: Some(CloudProvider::S3),
            s3_bucket: Some("bucket".to_string()),
            s3_region: Some("us-east-1".to_string()),
            s3_exact_slots: Some(CustomS3ExactSlots::StandardConditionalRequests),
            storage: HomeStorage::Opaque,
            ..CloudHomeConfig::default()
        };

        config.save_to_config_yaml().expect("save");
        let config_yaml = std::fs::read_to_string(config.store_dir.config_path())
            .expect("read saved local config");
        assert!(config_yaml.contains("s3_exact_slots"));
        let loaded = Config::load_from_config_yaml(store_dir).expect("load");

        assert_eq!(loaded, config);

        let join_info = crate::storage::cloud::CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("https://objects.example".to_string()),
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };
        let shared_wire = serde_json::to_string(&join_info).expect("serialize shared join info");
        assert!(!shared_wire.contains("s3_exact_slots"));
        assert!(!shared_wire.contains("strong_reads"));
    }

    /// A CloudKit share join persists `cloudkit_owner_name` and
    /// `cloudkit_zone_name` — the only two fields the share arm writes — and
    /// both come back unchanged.
    #[test]
    fn round_trips_cloudkit_share_owner_and_zone() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(dir.path());
        let mut config = Config::with_defaults(
            "store-1".to_string(),
            "device-1".to_string(),
            store_dir.clone(),
            "Shared CloudKit Store".to_string(),
        );
        config.cloud_home = CloudHomeConfig {
            provider: Some(CloudProvider::CloudKit),
            cloudkit_owner_name: Some("owner-name".to_string()),
            cloudkit_zone_name: Some("zone-name".to_string()),
            storage: HomeStorage::Opaque,
            ..CloudHomeConfig::default()
        };

        config.save_to_config_yaml().expect("save");
        let loaded = Config::load_from_config_yaml(store_dir).expect("load");
        assert_eq!(loaded, config);
    }

    /// A file that omits every field with a designed default
    /// (`encryption_key_stored`, `encryption_key_fingerprint`, the flattened
    /// `cloud_home`) still loads — those absences are real inputs, not bugs.
    /// `storage` has no default (the host must pick opaque vs. browsable when
    /// it creates the home), so it is the one `cloud_home` field still spelled
    /// out.
    #[test]
    fn load_with_absent_optional_fields_uses_defaults() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(dir.path());
        std::fs::write(
            store_dir.config_path(),
            "store_id: store-1\nstore_name: My Store\ndevice_id: device-1\nstorage: opaque\n",
        )
        .expect("write config.yaml");

        let loaded = Config::load_from_config_yaml(store_dir.clone()).expect("load");

        assert_eq!(loaded.store_id, "store-1");
        assert_eq!(loaded.store_name, "My Store");
        assert_eq!(loaded.device_id, "device-1");
        assert!(!loaded.encryption_key_stored);
        assert_eq!(loaded.encryption_key_fingerprint, None);
        assert_eq!(loaded.cloud_home, CloudHomeConfig::default());
        assert_eq!(loaded.store_dir, store_dir);
    }

    /// `device_id` is a required field on the wire, unlike the designed-default
    /// ones above: the save side always writes it, so a file without one is bad
    /// data, not an absence to tolerate — it must fail loudly, not default.
    #[test]
    fn load_with_missing_device_id_errors() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(dir.path());
        std::fs::write(
            store_dir.config_path(),
            "store_id: store-1\nstore_name: My Store\n",
        )
        .expect("write config.yaml");

        let err = Config::load_from_config_yaml(store_dir).expect_err("missing device_id");
        assert!(matches!(err, ConfigError::Config(_)));
    }

    /// No `config.yaml` at all names the path in the error rather than
    /// failing opaquely.
    #[test]
    fn load_with_no_file_errors_naming_the_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(dir.path());

        let err = Config::load_from_config_yaml(store_dir.clone()).expect_err("no file");
        let message = err.to_string();
        assert!(
            message.contains(&store_dir.config_path().display().to_string()),
            "error should name the missing path, got: {message}",
        );
    }
}
