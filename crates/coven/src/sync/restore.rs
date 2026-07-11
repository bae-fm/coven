//! Restore an existing store from cloud storage.
//!
//! Unlike join (which unwraps the encryption key from an invite), restore takes
//! the encryption key directly from the user — present for an opaque home,
//! absent for a browsable one.

use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::config::{Config, ConfigError, HomeStorage};
use crate::encryption::{EncryptionError, EncryptionService};
use crate::keys::{KeyError, KeyService, UserKeypair};
use crate::migration::Migration;
use crate::oauth::OAuthTokens;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::store_dir::StoreDir;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::join::{bootstrap_and_save_store, BootstrapSaveError, JoinError};
use crate::sync::pull::PullError;
use crate::sync::session::SyncedTable;
use crate::sync::snapshot::SnapshotError;

/// Cloud provider source for restore. Carries all connection details including
/// OAuth tokens (unlike RestoreProvider which omits them for serialization).
pub enum RestoreSource {
    S3 {
        bucket: String,
        region: String,
        endpoint: Option<String>,
        access_key: String,
        secret_key: String,
    },
    CloudKit {
        ops: Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>,
    },
    GoogleDrive {
        folder_id: String,
        tokens: OAuthTokens,
    },
    Dropbox {
        folder_path: String,
        tokens: OAuthTokens,
    },
    OneDrive {
        drive_id: String,
        folder_id: String,
        tokens: OAuthTokens,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("encryption: {0}")]
    Encryption(#[from] EncryptionError),
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("pull: {0}")]
    Pull(#[from] PullError),
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("keyring: {0}")]
    Key(#[from] KeyError),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("join: {0}")]
    Join(#[from] JoinError),
    #[error("cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("cleanup: {0}")]
    Cleanup(String),
    #[error("invalid restore code: {0}")]
    InvalidCode(String),
    #[error("store already exists locally: {0}")]
    StoreExists(String),
    #[error("invalid signing key: {0}")]
    InvalidSigningKey(String),
    #[error("provider: {0}")]
    Provider(String),
}

impl From<BootstrapSaveError> for RestoreError {
    fn from(error: BootstrapSaveError) -> Self {
        match error {
            BootstrapSaveError::Snapshot(error) => RestoreError::Snapshot(error),
            BootstrapSaveError::Join(error) => RestoreError::Join(error),
            BootstrapSaveError::Config(error) => RestoreError::Config(error),
            BootstrapSaveError::Key(error) => RestoreError::Key(error),
        }
    }
}

/// Build a `(JoinInfo, Box<dyn CloudHome>)` from a `RestoreSource`.
async fn build_cloud_home(
    source: RestoreSource,
    store_id: &str,
    clock: crate::clock::ClockRef,
) -> Result<(CloudHomeJoinInfo, Box<dyn CloudHome>), RestoreError> {
    use crate::storage::cloud::*;

    // Consumed only by the oauth provider arms below.
    #[cfg(not(feature = "oauth-providers"))]
    let _ = (store_id, &clock);

    match source {
        RestoreSource::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
        } => {
            let s3_home = s3::S3CloudHome::new(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                access_key.clone(),
                secret_key.clone(),
                None,
            )
            .await?;

            let info = CloudHomeJoinInfo::S3 {
                bucket,
                region,
                endpoint,
                key_prefix: None,
                access_key,
                secret_key,
            };
            Ok((info, Box::new(s3_home)))
        }

        RestoreSource::CloudKit { ops } => {
            let home = cloudkit::CloudKitCloudHome::new_private(ops);
            let info = CloudHomeJoinInfo::CloudKit;
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        #[cfg(feature = "oauth-providers")]
        RestoreSource::GoogleDrive { folder_id, tokens } => {
            let ks = KeyService::new(store_id.to_string());
            let home =
                google_drive::GoogleDriveCloudHome::new(folder_id.clone(), tokens, ks, clock)?;
            let info = CloudHomeJoinInfo::GoogleDrive { folder_id };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        #[cfg(feature = "oauth-providers")]
        RestoreSource::Dropbox {
            folder_path,
            tokens,
        } => {
            let ks = KeyService::new(store_id.to_string());
            let home = dropbox::DropboxCloudHome::new(folder_path.clone(), tokens, ks, clock)?;
            let info = CloudHomeJoinInfo::Dropbox {
                shared_folder_id: folder_path,
            };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        #[cfg(feature = "oauth-providers")]
        RestoreSource::OneDrive {
            drive_id,
            folder_id,
            tokens,
        } => {
            let ks = KeyService::new(store_id.to_string());
            let home = onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                tokens,
                ks,
                clock,
            )?;
            let info = CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        #[cfg(not(feature = "oauth-providers"))]
        RestoreSource::GoogleDrive { .. }
        | RestoreSource::Dropbox { .. }
        | RestoreSource::OneDrive { .. } => Err(RestoreError::Provider(
            "OAuth cloud providers are not supported in this build".to_string(),
        )),
    }
}

/// Restore a store from cloud storage.
///
/// Validates inputs, constructs the cloud home from the source, runs the sync
/// protocol, and sets the store as active. `keypair` is the restored device's
/// signing identity (recovered from the restore code); the storage signs the
/// control objects it writes with it, and it is the same key the caller imports
/// once restore succeeds.
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_cloud(
    store_id: &str,
    encryption_key_hex: Option<&str>,
    store_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    source: RestoreSource,
    keypair: &UserKeypair,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
) -> Result<Config, RestoreError> {
    // Guard the destructive `stores/<id>` create/delete against any direct
    // caller, independent of the decode-time check on untrusted input.
    crate::store_dir::validate_path_token(store_id)
        .map_err(|e| RestoreError::InvalidCode(format!("invalid store id: {e}")))?;

    // Refuse a store already present locally before any provider side effect. The
    // decode guaranteed the id is a safe single component, so the directory is a
    // direct child of `stores/` and cannot escape it. Re-running a restore for a
    // store you already have adds nothing — the existing store is the data — and
    // letting it proceed would, on any bootstrap failure below, delete that store's
    // database and blobs during cleanup. Refusing here makes the failure-cleanup only
    // ever remove a directory this invocation created.
    let store_dir = StoreDir::for_store(app_dir, store_id);
    if store_dir.exists() {
        return Err(RestoreError::StoreExists(store_id.to_string()));
    }

    // The key's presence is the home's storage mode: a key present ⇒ an opaque
    // home (encrypted, obfuscated blob paths); a key absent ⇒ a browsable home
    // (plaintext, readable blob paths). The cipher and the blob-path scheme both
    // follow from it, so this device computes the same blob keys the source wrote.
    let storage = if encryption_key_hex.is_some() {
        HomeStorage::Opaque
    } else {
        HomeStorage::Browsable
    };
    let cipher = match encryption_key_hex {
        Some(key_hex) => {
            on_status("Verifying encryption key...");
            CloudCipher::Encrypted(EncryptionService::new(key_hex)?)
        }
        None => CloudCipher::Plaintext,
    };

    let blob_paths = BlobPathScheme::for_storage(storage);

    let (join_info, cloud_home) = build_cloud_home(source, store_id, clock).await?;

    let storage = CloudSyncStorage::new(
        std::sync::Arc::from(cloud_home),
        cipher.clone(),
        blob_paths,
        store_id.to_string(),
        keypair.clone(),
    );

    // Create the store directory under `stores/` (its non-existence was checked
    // up front, so this create and the failure-cleanup below own it entirely).
    let device_id = ids.new_id();
    std::fs::create_dir_all(&*store_dir)?;

    let key_service = KeyService::new(store_id.to_string());

    let result = bootstrap_and_save_store(
        &storage,
        &cipher,
        encryption_key_hex,
        &store_dir,
        store_id,
        &device_id,
        crate::sync::join::BootstrapContext::Restore,
        synced_tables,
        migrations,
        &join_info,
        store_name,
        &key_service,
        &on_status,
    )
    .await;

    match result {
        Ok(config) => {
            // The host records this as the active store after this returns.
            info!(
                "Cloud restore complete: store at {}",
                config.store_dir.display()
            );
            Ok(config)
        }
        Err(err) => {
            let restore_error = RestoreError::from(err);
            if let Err(cleanup_error) = std::fs::remove_dir_all(&*store_dir) {
                return Err(RestoreError::Cleanup(format!(
                    "failed to remove store directory after restore failed: {cleanup_error}; original error: {restore_error}"
                )));
            }
            Err(restore_error)
        }
    }
}

/// Restore a store from a restore code string.
///
/// Decodes the restore code, converts provider → RestoreSource (adding OAuth
/// tokens for providers that need them), imports the signing key, and
/// delegates to `restore_from_cloud`.
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_code(
    code: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
) -> Result<Config, RestoreError> {
    use crate::sync::restore_code::{self, RestoreProvider};

    let parsed = restore_code::decode_restore_code(code)
        .map_err(|e| RestoreError::InvalidCode(e.to_string()))?;

    // `decode_restore_code` already validated the field; convert it to bytes so
    // restore can rebuild and later import the signing identity.
    let signing_key_bytes = hex::decode(&parsed.sk)
        .map_err(|e| RestoreError::InvalidSigningKey(format!("invalid encoding: {e}")))?;
    // The restored device's identity. Rebuilt here (not imported yet) so the
    // storage can sign its control objects during restore, while the keyring
    // import still happens only after restore succeeds.
    let signing_key: [u8; crate::keys::SIGN_SECRETKEYBYTES] =
        signing_key_bytes.clone().try_into().map_err(|_| {
            RestoreError::InvalidSigningKey(format!(
                "Signing key must be {} bytes",
                crate::keys::SIGN_SECRETKEYBYTES
            ))
        })?;
    let keypair = UserKeypair::from_signing_key_bytes(&signing_key).map_err(RestoreError::Key)?;

    let require_oauth = |provider_name: &str| -> Result<crate::oauth::OAuthTokens, RestoreError> {
        oauth_tokens.clone().ok_or_else(|| {
            RestoreError::Provider(format!("{provider_name} restore requires OAuth token"))
        })
    };

    let source = match parsed.provider {
        RestoreProvider::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            ..
        } => RestoreSource::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
        },
        RestoreProvider::CloudKit => {
            let ops = cloudkit_ops.ok_or_else(|| {
                RestoreError::Provider("CloudKit driver not provided".to_string())
            })?;
            RestoreSource::CloudKit { ops }
        }
        RestoreProvider::GoogleDrive { folder_id } => RestoreSource::GoogleDrive {
            folder_id,
            tokens: require_oauth("Google Drive")?,
        },
        RestoreProvider::Dropbox { folder_path } => RestoreSource::Dropbox {
            folder_path,
            tokens: require_oauth("Dropbox")?,
        },
        RestoreProvider::OneDrive {
            drive_id,
            folder_id,
        } => RestoreSource::OneDrive {
            drive_id,
            folder_id,
            tokens: require_oauth("OneDrive")?,
        },
    };

    let config = restore_from_cloud(
        &parsed.sid,
        parsed.ek.as_deref(),
        &parsed.name,
        synced_tables,
        migrations,
        source,
        &keypair,
        app_dir,
        clock,
        ids,
        on_status,
    )
    .await?;

    // Import signing key after restore succeeds so we don't overwrite an existing
    // keypair if the restore fails.
    let global_ks = KeyService::new("global".to_string());
    global_ks
        .import_user_keypair(&signing_key_bytes)
        .map_err(RestoreError::Key)?;

    Ok(config)
}
