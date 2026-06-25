//! Restore an existing library from cloud storage.
//!
//! Unlike join (which unwraps the encryption key from an invite), restore takes
//! the encryption key directly from the user — present for an opaque home,
//! absent for a browsable one. Unlike join, restore sets
//! `cloudkit_is_shared = false` because the restorer is the owner.

use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::config::{Config, ConfigError, HomeStorage};
use crate::encryption::{EncryptionError, EncryptionService};
use crate::keys::{KeyError, KeyService, UserKeypair};
use crate::library_dir::LibraryDir;
use crate::oauth::OAuthTokens;
use crate::storage::cloud::{CloudHome, CloudHomeJoinInfo};
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::join::{build_config, derive_credentials, open_db_and_pull, JoinError};
use crate::sync::pull::PullError;
use crate::sync::session::SyncedTable;
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
    clock: crate::clock::ClockRef,
) -> Result<(CloudHomeJoinInfo, Box<dyn CloudHome>), RestoreError> {
    use crate::storage::cloud::*;

    // Consumed only by the oauth provider arms below.
    #[cfg(not(feature = "oauth-providers"))]
    let _ = (library_id, &clock);

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

        #[cfg(feature = "oauth-providers")]
        RestoreSource::GoogleDrive { folder_id, tokens } => {
            let ks = KeyService::new(library_id.to_string());
            let home =
                google_drive::GoogleDriveCloudHome::new(folder_id.clone(), tokens, ks, clock);
            let info = CloudHomeJoinInfo::GoogleDrive { folder_id };
            Ok((info, Box::new(home) as Box<dyn CloudHome>))
        }

        #[cfg(feature = "oauth-providers")]
        RestoreSource::Dropbox {
            folder_path,
            tokens,
        } => {
            let ks = KeyService::new(library_id.to_string());
            let home = dropbox::DropboxCloudHome::new(folder_path.clone(), tokens, ks, clock);
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
            let ks = KeyService::new(library_id.to_string());
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

        #[cfg(not(feature = "oauth-providers"))]
        RestoreSource::GoogleDrive { .. }
        | RestoreSource::Dropbox { .. }
        | RestoreSource::OneDrive { .. } => Err(RestoreError::Database(
            "OAuth cloud providers are not supported in this build".to_string(),
        )),
    }
}

/// Restore a library from cloud storage.
///
/// Validates inputs, constructs the cloud home from the source, runs the sync
/// protocol, and sets the library as active. `keypair` is the restored device's
/// signing identity (recovered from the restore code); the storage signs the
/// control objects it writes with it, and it is the same key the caller imports
/// once restore succeeds.
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_cloud(
    library_id: &str,
    encryption_key_hex: Option<&str>,
    library_name: &str,
    synced_tables: &[SyncedTable],
    source: RestoreSource,
    keypair: &UserKeypair,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
) -> Result<Config, RestoreError> {
    // A function that reaches `remove_dir_all` validates its own id: this guards
    // the destructive `libraries/<id>` create/delete against any direct caller,
    // independent of the decode-time check that validates untrusted input up front.
    crate::library_dir::validate_path_token(library_id)
        .map_err(|e| RestoreError::Database(format!("invalid library id: {e}")))?;

    // `library_id` is a safe single path component: `decode_restore_code` rejects
    // any `lid` that isn't, so the directory it names below stays under
    // `libraries/`. (A non-empty id is part of that guarantee — the decoder rejects
    // the empty string too.)

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
            if key_hex.len() != 64 {
                return Err(RestoreError::Database(
                    "Encryption key must be 64 hex characters (32 bytes)".to_string(),
                ));
            }
            if hex::decode(key_hex).is_err() {
                return Err(RestoreError::Database(
                    "Invalid hex encoding in encryption key".to_string(),
                ));
            }
            on_status("Verifying encryption key...");
            CloudCipher::Encrypted(EncryptionService::new(key_hex)?)
        }
        None => CloudCipher::Plaintext,
    };

    let blob_paths = BlobPathScheme::for_storage(storage);

    let (join_info, cloud_home) = build_cloud_home(source, library_id, clock).await?;

    let storage = CloudSyncStorage::new(
        std::sync::Arc::from(cloud_home),
        cipher.clone(),
        blob_paths,
        keypair.clone(),
    );

    // Create the library directory under `libraries/`, named by the restore code's
    // `lid`. The decode guaranteed the id is a safe single component, so the
    // directory is a direct child of `libraries/` and cannot escape it.
    let device_id = ids.new_id();
    let library_dir = LibraryDir::new(app_dir.join("libraries").join(library_id));
    std::fs::create_dir_all(&*library_dir)?;

    let key_service = KeyService::new(library_id.to_string());

    let result = bootstrap_and_save(
        &storage,
        &cipher,
        encryption_key_hex,
        &library_dir,
        library_id,
        &device_id,
        synced_tables,
        &join_info,
        library_name,
        &key_service,
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
#[allow(clippy::too_many_arguments)]
pub async fn restore_from_code(
    code: &str,
    synced_tables: &[SyncedTable],
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    app_dir: &Path,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
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
    // The restored device's identity. Rebuilt here (not imported yet) so the
    // storage can sign its control objects during restore, while the keyring
    // import still happens only after restore succeeds.
    let signing_key: [u8; crate::keys::SIGN_SECRETKEYBYTES] =
        signing_key_bytes.clone().try_into().map_err(|_| {
            RestoreError::Database(format!(
                "Signing key must be {} bytes",
                crate::keys::SIGN_SECRETKEYBYTES
            ))
        })?;
    let keypair = UserKeypair::from_signing_key_bytes(&signing_key).map_err(RestoreError::Key)?;

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
    };

    let config = restore_from_cloud(
        &parsed.lid,
        parsed.ek.as_deref(),
        &parsed.name,
        synced_tables,
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

/// Inner bootstrap + save logic, separated so the caller can clean up on failure.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_and_save(
    storage: &CloudSyncStorage,
    cipher: &CloudCipher,
    encryption_key_hex: Option<&str>,
    library_dir: &LibraryDir,
    library_id: &str,
    device_id: &str,
    synced_tables: &[SyncedTable],
    join_info: &CloudHomeJoinInfo,
    library_name: &str,
    key_service: &KeyService,
    on_status: &impl Fn(&str),
) -> Result<Config, RestoreError> {
    // Bootstrap from the snapshot. Restore pins no owner up front (it recovers a
    // library this device may not have founded — the owner is adopted from the
    // chain's founder after the pull, trust-on-first-use, since the restore code
    // already carries the bucket's own credentials). So the snapshot is
    // authenticated against the membership chain anchored to its own founder: the
    // author must still be a current write-capable member, and an unsigned or
    // tampered snapshot is refused.
    on_status("Downloading library snapshot...");
    let db_path = library_dir.db_path();
    let bucket_dyn: &dyn SyncStorage = storage;
    let bootstrap_result =
        bootstrap_from_snapshot(bucket_dyn, library_id, cipher, None, &db_path).await?;

    info!(
        "Bootstrapped from snapshot ({} device cursors)",
        bootstrap_result.cursors.len()
    );

    // Step 4: Pull changesets since the snapshot.
    on_status("Applying recent changes...");
    let cursors = bootstrap_result.cursors;

    // Restore leaves the owner unpinned: this is the user's own library and bucket,
    // so the owner is adopted from the chain founder on first sync connect (issue
    // #102), rather than asserted from the restore code.
    let changesets_applied = open_db_and_pull(
        &db_path,
        synced_tables,
        device_id,
        None,
        bucket_dyn,
        &cursors,
        library_dir,
    )
    .await?;

    if changesets_applied > 0 {
        info!("Applied {changesets_applied} changesets since snapshot");
    }

    // Step 5: Save the encryption key to the keyring — only for a private home.
    // A public home has no key to store.
    on_status("Saving configuration...");
    if let Some(key_hex) = encryption_key_hex {
        key_service.set_encryption_key(key_hex)?;
    }

    // Step 6: Save cloud credentials to keyring.
    let credentials = derive_credentials(join_info);
    key_service.set_cloud_home_credentials(&credentials)?;

    // Step 7: Create and save config. The cipher records the home's storage mode:
    // opaque (key stored + fingerprint) or browsable (no key).
    let mut config = build_config(
        library_id,
        device_id,
        library_dir,
        library_name,
        join_info,
        cipher,
    );

    // Restore is done by the owner — CloudKit uses the private database.
    // build_config sets cloudkit_is_shared = true (for joiners); override for restore.
    if matches!(join_info, CloudHomeJoinInfo::CloudKit { .. }) {
        config.cloud_home.cloudkit_is_shared = false;
    }

    config.save_to_config_yaml()?;

    info!(
        "Restored library {} at {}",
        library_id,
        library_dir.display()
    );
    Ok(config)
}
