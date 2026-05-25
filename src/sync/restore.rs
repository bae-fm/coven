//! Restore an existing library from cloud storage.
//!
//! Unlike join (which unwraps the encryption key from an invite), restore takes the
//! encryption key directly from the user. Unlike join, restore sets
//! `cloudkit_is_shared = false` because the restorer is the owner.

use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::blob::BlobPlan;
use crate::config::{Config, ConfigError};
use crate::encryption::{EncryptionError, EncryptionService};
use crate::keys::{KeyError, KeyService};
use crate::library_dir::LibraryDir;
use crate::oauth::OAuthTokens;
use crate::storage::cloud::{CloudHome, CloudHomeJoinInfo};
use crate::sync::encrypted_storage::EncryptedSyncStorage;
use crate::sync::join::{build_config, derive_credentials, open_db_and_pull, JoinError};
use crate::sync::pull::PullError;
use crate::sync::snapshot::{bootstrap_from_snapshot, SnapshotError};
use crate::sync::storage::SyncStorage;

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
    HttpProxy {
        url: String,
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
    #[error("database: {0}")]
    Database(String),
}

impl From<JoinError> for RestoreError {
    fn from(e: JoinError) -> Self {
        match e {
            JoinError::Encryption(e) => RestoreError::Encryption(e),
            JoinError::Snapshot(e) => RestoreError::Snapshot(e),
            JoinError::Pull(e) => RestoreError::Pull(e),
            JoinError::Config(e) => RestoreError::Config(e),
            JoinError::Key(e) => RestoreError::Key(e),
            JoinError::Io(e) => RestoreError::Io(e),
            JoinError::Database(s) => RestoreError::Database(s),
            // CloudHome and Invite errors don't occur in restore path,
            // but we need to handle them for exhaustiveness.
            other => RestoreError::Database(other.to_string()),
        }
    }
}

/// Build a `(JoinInfo, Box<dyn CloudHome>)` from a `RestoreSource`.
async fn build_cloud_home(
    source: RestoreSource,
    library_id: &str,
    dev_mode: bool,
    global_ks: &KeyService,
    clock: crate::clock::ClockRef,
) -> Result<(CloudHomeJoinInfo, Box<dyn CloudHome>), RestoreError> {
    use crate::storage::cloud::*;

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
            .await
            .map_err(|e| RestoreError::Database(format!("Failed to connect to S3: {e}")))?;

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
            let home = cloudkit::CloudKitCloudHome::new(ops);
            let info = CloudHomeJoinInfo::CloudKit {
                share_url: String::new(),
            };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        RestoreSource::GoogleDrive { folder_id, tokens } => {
            let ks = KeyService::new(dev_mode, library_id.to_string());
            let home =
                google_drive::GoogleDriveCloudHome::new(folder_id.clone(), tokens, ks, clock);
            let info = CloudHomeJoinInfo::GoogleDrive { folder_id };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        RestoreSource::Dropbox {
            folder_path,
            tokens,
        } => {
            let ks = KeyService::new(dev_mode, library_id.to_string());
            let home = dropbox::DropboxCloudHome::new(folder_path.clone(), tokens, ks, clock);
            let info = CloudHomeJoinInfo::Dropbox {
                shared_folder_id: folder_path,
            };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        RestoreSource::OneDrive {
            drive_id,
            folder_id,
            tokens,
        } => {
            let ks = KeyService::new(dev_mode, library_id.to_string());
            let home = onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                tokens,
                ks,
                clock,
            );
            let info = CloudHomeJoinInfo::OneDrive {
                drive_id,
                folder_id,
            };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        RestoreSource::HttpProxy { url } => {
            let keypair = global_ks
                .get_or_create_user_keypair()
                .map_err(RestoreError::Key)?;
            let home = http::HttpCloudHome::new(url.clone(), keypair, clock);
            let info = CloudHomeJoinInfo::HttpProxy { url };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }
    }
}

/// Restore a library from cloud storage.
///
/// Validates inputs, constructs the cloud home from the source, runs the sync
/// protocol, and sets the library as active.
pub async fn restore_from_cloud(
    library_id: &str,
    encryption_key_hex: &str,
    library_name: &str,
    source: RestoreSource,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    make_blob_plan: impl Fn(&LibraryDir) -> Box<dyn BlobPlan>,
    on_status: impl Fn(&str),
) -> Result<Config, RestoreError> {
    if library_id.is_empty() || encryption_key_hex.is_empty() {
        return Err(RestoreError::Database(
            "Library ID and encryption key are required".to_string(),
        ));
    }
    if encryption_key_hex.len() != 64 {
        return Err(RestoreError::Database(
            "Encryption key must be 64 hex characters (32 bytes)".to_string(),
        ));
    }
    if hex::decode(encryption_key_hex).is_err() {
        return Err(RestoreError::Database(
            "Invalid hex encoding in encryption key".to_string(),
        ));
    }

    let dev_mode = Config::is_dev_mode();
    let global_ks = KeyService::new(dev_mode, "global".to_string());

    let (join_info, cloud_home) =
        build_cloud_home(source, library_id, dev_mode, &global_ks, clock).await?;

    // Create encryption service from the user-provided key.
    on_status("Verifying encryption key...");
    let encryption = EncryptionService::new(encryption_key_hex)?;
    let storage = EncryptedSyncStorage::new(cloud_home, encryption.clone());

    // Create library directory.
    let device_id = ids.new_id();
    let library_dir = LibraryDir::new(app_dir.join("libraries").join(library_id));
    std::fs::create_dir_all(&*library_dir)?;
    // The host's blob plan is bound to the library dir we just created.
    let blob_plan = make_blob_plan(&library_dir);

    let key_service = KeyService::new(dev_mode, library_id.to_string());

    let result = bootstrap_and_save(
        &storage,
        &encryption,
        encryption_key_hex,
        &library_dir,
        library_id,
        &device_id,
        &join_info,
        library_name,
        &key_service,
        blob_plan.as_ref(),
        &on_status,
    )
    .await;

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&*library_dir);
        return result;
    }

    let config = result?;
    // The host records this as the active library after this returns.

    info!(
        "Cloud restore complete: library at {}",
        config.library_dir.display()
    );

    Ok(config)
}

/// Restore a library from a restore code string.
///
/// Decodes the restore code, converts provider → RestoreSource (adding OAuth
/// tokens for providers that need them), imports the signing key, and
/// delegates to `restore_from_cloud`.
pub async fn restore_from_code(
    code: &str,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    make_blob_plan: impl Fn(&LibraryDir) -> Box<dyn BlobPlan>,
    on_status: impl Fn(&str),
) -> Result<Config, RestoreError> {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    use crate::sync::restore_code::{self, RestoreProvider};

    let parsed = restore_code::decode_restore_code(code)
        .map_err(|e| RestoreError::Database(format!("Invalid restore code: {e}")))?;

    // Decode signing key upfront so we fail fast on bad encoding.
    let signing_key_bytes = URL_SAFE_NO_PAD
        .decode(&parsed.sk)
        .map_err(|e| RestoreError::Database(format!("Invalid signing key encoding: {e}")))?;

    let require_oauth = |provider_name: &str| -> Result<crate::oauth::OAuthTokens, RestoreError> {
        oauth_tokens.clone().ok_or_else(|| {
            RestoreError::Database(format!("{provider_name} restore requires OAuth token"))
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
                RestoreError::Database("CloudKit driver not provided".to_string())
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
        RestoreProvider::HttpProxy { url } => RestoreSource::HttpProxy { url },
    };

    let config = restore_from_cloud(
        &parsed.lid,
        &parsed.ek,
        &parsed.name,
        source,
        app_dir,
        clock,
        ids,
        make_blob_plan,
        on_status,
    )
    .await?;

    // Import signing key after restore succeeds so we don't overwrite an existing
    // keypair if the restore fails.
    let dev_mode = Config::is_dev_mode();
    let global_ks = KeyService::new(dev_mode, "global".to_string());
    global_ks
        .import_user_keypair(&signing_key_bytes)
        .map_err(RestoreError::Key)?;

    Ok(config)
}

/// Inner bootstrap + save logic, separated so the caller can clean up on failure.
async fn bootstrap_and_save(
    storage: &EncryptedSyncStorage,
    encryption: &EncryptionService,
    encryption_key_hex: &str,
    library_dir: &LibraryDir,
    library_id: &str,
    device_id: &str,
    join_info: &CloudHomeJoinInfo,
    library_name: &str,
    key_service: &KeyService,
    blob_plan: &dyn BlobPlan,
    on_status: &impl Fn(&str),
) -> Result<Config, RestoreError> {
    // Step 3: Bootstrap from snapshot.
    on_status("Downloading library snapshot...");
    let db_path = library_dir.db_path();
    let bucket_dyn: &dyn SyncStorage = storage;
    let bootstrap_result = bootstrap_from_snapshot(bucket_dyn, encryption, &db_path).await?;

    info!(
        "Bootstrapped from snapshot ({} device cursors)",
        bootstrap_result.cursors.len()
    );

    // Step 4: Pull changesets since the snapshot.
    on_status("Applying recent changes...");
    let cursors = bootstrap_result.cursors;

    let changesets_applied = open_db_and_pull(
        &db_path,
        bucket_dyn,
        device_id,
        &cursors,
        library_dir,
        blob_plan,
    )
    .await?;

    if changesets_applied > 0 {
        info!("Applied {changesets_applied} changesets since snapshot");
    }

    // Step 5: Save encryption key to keyring.
    on_status("Saving configuration...");
    key_service.set_encryption_key(encryption_key_hex)?;

    // Step 6: Save cloud credentials to keyring.
    let credentials = derive_credentials(join_info);
    key_service.set_cloud_home_credentials(&credentials)?;

    // Step 7: Create and save config.
    let mut config = build_config(
        library_id,
        device_id,
        library_dir,
        library_name,
        join_info,
        encryption,
    );

    // Restore is done by the owner — CloudKit uses the private database.
    // build_config sets cloudkit_is_shared = true (for joiners); override for restore.
    if matches!(join_info, CloudHomeJoinInfo::CloudKit { .. }) {
        config.cloud_home_cloudkit_is_shared = false;
    }

    config.save_to_config_yaml()?;

    info!(
        "Restored library {} at {}",
        library_id,
        library_dir.display()
    );
    Ok(config)
}
