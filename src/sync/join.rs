//! Join an existing shared library using an invite code.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::collections::HashMap;
use std::ffi::CString;
use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::config::{CloudProvider, Config, ConfigError};
use crate::encryption::{EncryptionError, EncryptionService};
use crate::join_code::InviteCode;
use crate::keys::{CloudHomeCredentials, KeyError, KeyService};
use crate::blob::BlobPlan;
use crate::library_dir::LibraryDir;
use crate::oauth::OAuthError;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::encrypted_storage::EncryptedSyncStorage;
use crate::sync::invite::{unwrap_library_key, InviteError};
use crate::sync::pull::{pull_changes, PullError};
use crate::sync::snapshot::{bootstrap_from_snapshot, SnapshotError};
use crate::sync::storage::SyncStorage;

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("encryption: {0}")]
    Encryption(#[from] EncryptionError),
    #[error("invite: {0}")]
    Invite(#[from] InviteError),
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
    #[error("oauth: {0}")]
    OAuth(#[from] OAuthError),
}

/// Build a CloudHome from a JoinInfo for the join flow.
///
/// For OAuth providers, runs the OAuth authorization flow inline and saves
/// credentials to the library-scoped keyring.
async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    keypair: &crate::keys::UserKeypair,
    lib_ks: &KeyService,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: crate::clock::ClockRef,
) -> Result<Box<dyn CloudHome>, JoinError> {
    use crate::storage::cloud::*;

    match join_info {
        CloudHomeJoinInfo::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        } => {
            let s3 = s3::S3CloudHome::new(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                access_key.clone(),
                secret_key.clone(),
                key_prefix.clone(),
            )
            .await?;
            Ok(Box::new(s3))
        }

        CloudHomeJoinInfo::HttpProxy { url } => Ok(Box::new(http::HttpCloudHome::new(
            url.clone(),
            keypair.clone(),
            clock,
        ))),

        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            info!("Authorizing with Google Drive...");
            let tokens = crate::oauth::authorize_provider(
                CloudProvider::GoogleDrive,
                oauth_cancel.clone(),
                clock.as_ref(),
            )
            .await?;
            let token_json = serde_json::to_string(&tokens)
                .map_err(|e| JoinError::Database(format!("Failed to serialize tokens: {e}")))?;
            lib_ks.set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })?;
            Ok(Box::new(google_drive::GoogleDriveCloudHome::new(
                folder_id.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )))
        }

        CloudHomeJoinInfo::Dropbox { shared_folder_id } => {
            info!("Authorizing with Dropbox...");
            let tokens = crate::oauth::authorize_provider(
                CloudProvider::Dropbox,
                oauth_cancel.clone(),
                clock.as_ref(),
            )
            .await?;
            let token_json = serde_json::to_string(&tokens)
                .map_err(|e| JoinError::Database(format!("Failed to serialize tokens: {e}")))?;
            lib_ks.set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })?;
            Ok(Box::new(dropbox::DropboxCloudHome::new(
                shared_folder_id.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )))
        }

        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            info!("Authorizing with OneDrive...");
            let tokens = crate::oauth::authorize_provider(
                CloudProvider::OneDrive,
                oauth_cancel.clone(),
                clock.as_ref(),
            )
            .await?;
            let token_json = serde_json::to_string(&tokens)
                .map_err(|e| JoinError::Database(format!("Failed to serialize tokens: {e}")))?;
            lib_ks.set_cloud_home_credentials(&CloudHomeCredentials::OAuth { token_json })?;
            Ok(Box::new(onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )))
        }

        CloudHomeJoinInfo::CloudKit { .. } => {
            let ops = cloudkit_ops
                .ok_or_else(|| JoinError::Database("CloudKit driver not provided".to_string()))?;
            Ok(Box::new(cloudkit::CloudKitCloudHome::new(ops)))
        }
    }
}

/// Join a shared library using an invite code string.
///
/// Handles everything: decode invite, get keypair, build cloud home (including
/// OAuth flows), run the join protocol, and set as active library.
pub async fn join_from_invite_code(
    invite_code_str: &str,
    app_dir: &Path,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    oauth_cancel: tokio::sync::watch::Receiver<bool>,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    blob_plan: &dyn BlobPlan,
    on_status: impl Fn(&str),
) -> Result<Config, JoinError> {
    let code = crate::join_code::decode(invite_code_str)
        .map_err(|e| JoinError::Database(format!("Invalid invite code: {e}")))?;

    let dev_mode = Config::is_dev_mode();
    let global_ks = KeyService::new(dev_mode, "global".to_string());
    let keypair = global_ks.get_or_create_user_keypair()?;
    let lib_ks = KeyService::new(dev_mode, code.library_id.clone());

    let cloud_home = build_cloud_home_for_join(
        &code.join_info,
        &keypair,
        &lib_ks,
        cloudkit_ops,
        oauth_cancel,
        clock,
    )
    .await?;

    let config = join_library(
        app_dir,
        code,
        &global_ks,
        cloud_home,
        ids.as_ref(),
        blob_plan,
        &on_status,
    )
    .await?;

    // The host records this as the active library after this returns.
    Ok(config)
}

/// Join an existing shared library using a decoded invite code.
///
/// Lower-level function — caller provides pre-built `CloudHome`.
/// Prefer `join_from_invite_code` for the full flow.
///
/// `on_status` is called with progress messages for UI feedback.
pub async fn join_library(
    bae_dir: &Path,
    code: InviteCode,
    key_service: &KeyService,
    cloud_home: Box<dyn CloudHome>,
    ids: &dyn crate::id_provider::IdProvider,
    blob_plan: &dyn BlobPlan,
    on_status: impl Fn(&str),
) -> Result<Config, JoinError> {
    // Step 1: Load user keypair (must already exist — the inviter wrapped the
    // library key for this public key).
    on_status("Loading keypair...");
    let user_keypair = key_service.get_user_keypair()?;

    // Step 2: Accept invitation to get the library encryption key.
    // Uses CloudHome directly — wrapped keys are sealed-box encrypted,
    // no library-key encryption needed.
    on_status("Accepting invitation...");
    let encryption_key = unwrap_library_key(cloud_home.as_ref(), &user_keypair).await?;
    let encryption_key_hex = hex::encode(encryption_key);

    // Step 3: Create the sync storage with the real encryption key.
    on_status("Downloading library snapshot...");
    let encryption = EncryptionService::new(&encryption_key_hex)?;
    let storage = EncryptedSyncStorage::new(cloud_home, encryption.clone());

    // Step 4: Create library directory using the invite code's library_id.
    let library_id = code.library_id;
    let device_id = ids.new_id();
    let library_dir = LibraryDir::new(bae_dir.join("libraries").join(&library_id));
    std::fs::create_dir_all(&*library_dir)?;

    // All steps after directory creation are wrapped so we can clean up on failure.
    let new_key_service = KeyService::new(key_service.is_dev_mode(), library_id.clone());

    let result = bootstrap_and_save(
        &storage,
        &encryption,
        &encryption_key_hex,
        &library_dir,
        &library_id,
        &device_id,
        &code.join_info,
        &code.library_name,
        &new_key_service,
        blob_plan,
        &on_status,
    )
    .await;

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&*library_dir);
    }

    result
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
) -> Result<Config, JoinError> {
    // Step 5: Bootstrap from snapshot.
    let db_path = library_dir.db_path();
    let bucket_dyn: &dyn SyncStorage = storage;
    let bootstrap_result = bootstrap_from_snapshot(bucket_dyn, encryption, &db_path).await?;

    info!(
        "Bootstrapped from snapshot ({} device cursors)",
        bootstrap_result.cursors.len()
    );

    // Step 6: Pull changesets since the snapshot.
    on_status("Applying recent changes...");
    let cursors = bootstrap_result.cursors;

    let changesets_applied =
        open_db_and_pull(&db_path, bucket_dyn, device_id, &cursors, library_dir, blob_plan).await?;

    if changesets_applied > 0 {
        info!("Applied {changesets_applied} changesets since snapshot");
    }

    // Step 7: Save encryption key to keyring.
    on_status("Saving configuration...");
    key_service.set_encryption_key(encryption_key_hex)?;

    // Step 8: Save cloud credentials to keyring.
    let credentials = derive_credentials(join_info);
    key_service.set_cloud_home_credentials(&credentials)?;

    // Step 9: Create and save config.
    let config = build_config(
        library_id,
        device_id,
        library_dir,
        library_name,
        join_info,
        encryption,
    );

    config.save_to_config_yaml()?;

    info!("Joined library {} at {}", library_id, library_dir.display());
    Ok(config)
}

/// Open the database, pull changes, close the database.
pub(crate) async fn open_db_and_pull(
    db_path: &Path,
    storage: &dyn SyncStorage,
    device_id: &str,
    cursors: &HashMap<String, u64>,
    library_dir: &LibraryDir,
    blob_plan: &dyn BlobPlan,
) -> Result<u64, JoinError> {
    unsafe {
        let c_path = CString::new(db_path.to_str().unwrap()).unwrap();
        let mut db: *mut libsqlite3_sys::sqlite3 = std::ptr::null_mut();
        let rc = libsqlite3_sys::sqlite3_open(c_path.as_ptr(), &mut db);
        if rc != libsqlite3_sys::SQLITE_OK {
            return Err(JoinError::Database(
                "Failed to open database for changeset application".to_string(),
            ));
        }

        let result =
            match pull_changes(db, storage, device_id, cursors, None, library_dir, blob_plan).await
            {
            Ok((_updated_cursors, pull_result)) => Ok(pull_result.changesets_applied),
            Err(e) => {
                libsqlite3_sys::sqlite3_close(db);
                Err(JoinError::Pull(e))
            }
        };

        if result.is_ok() {
            libsqlite3_sys::sqlite3_close(db);
        }

        result
    }
}

/// Derive the CloudHomeCredentials to persist from the JoinInfo.
///
/// S3 credentials come from the invite code. HttpProxy uses keypair auth (no stored creds).
/// OAuth providers store the joiner's token, but that's already saved to the keyring
/// by the caller before constructing the CloudHome.
pub(crate) fn derive_credentials(join_info: &CloudHomeJoinInfo) -> CloudHomeCredentials {
    match join_info {
        CloudHomeJoinInfo::S3 {
            access_key,
            secret_key,
            ..
        } => CloudHomeCredentials::S3 {
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
        },
        CloudHomeJoinInfo::HttpProxy { .. } => CloudHomeCredentials::None,
        // OAuth providers: the joiner's token was already saved to the keyring
        // before constructing the CloudHome. No additional save needed here,
        // but set_cloud_home_credentials expects a value, so pass what we have.
        CloudHomeJoinInfo::GoogleDrive { .. }
        | CloudHomeJoinInfo::Dropbox { .. }
        | CloudHomeJoinInfo::OneDrive { .. }
        | CloudHomeJoinInfo::CloudKit { .. } => CloudHomeCredentials::None,
    }
}

/// Build the Config struct from join parameters.
pub(crate) fn build_config(
    library_id: &str,
    device_id: &str,
    library_dir: &LibraryDir,
    library_name: &str,
    join_info: &CloudHomeJoinInfo,
    encryption: &EncryptionService,
) -> Config {
    let mut config = Config::with_defaults(
        library_id.to_string(),
        device_id.to_string(),
        library_dir.clone(),
        library_name.to_string(),
    );

    config.encryption_key_stored = true;
    config.encryption_key_fingerprint = Some(encryption.fingerprint());

    config.cloud_provider = Some(match join_info {
        CloudHomeJoinInfo::S3 { .. } => CloudProvider::S3,
        CloudHomeJoinInfo::HttpProxy { .. } => CloudProvider::HttpProxy,
        CloudHomeJoinInfo::GoogleDrive { .. } => CloudProvider::GoogleDrive,
        CloudHomeJoinInfo::Dropbox { .. } => CloudProvider::Dropbox,
        CloudHomeJoinInfo::OneDrive { .. } => CloudProvider::OneDrive,
        CloudHomeJoinInfo::CloudKit { .. } => CloudProvider::CloudKit,
    });

    match join_info {
        CloudHomeJoinInfo::S3 {
            bucket,
            region,
            endpoint,
            key_prefix,
            ..
        } => {
            config.cloud_home_s3_bucket = Some(bucket.clone());
            config.cloud_home_s3_region = Some(region.clone());
            config.cloud_home_s3_endpoint = endpoint.clone();
            config.cloud_home_s3_key_prefix = key_prefix.clone();
        }
        CloudHomeJoinInfo::HttpProxy { url } => {
            config.cloud_home_http_url = Some(url.clone());
        }
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            config.cloud_home_google_drive_folder_id = Some(folder_id.clone());
        }
        CloudHomeJoinInfo::Dropbox { shared_folder_id } => {
            config.cloud_home_dropbox_folder_path = Some(shared_folder_id.clone());
        }
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            config.cloud_home_onedrive_drive_id = Some(drive_id.clone());
            config.cloud_home_onedrive_folder_id = Some(folder_id.clone());
        }
        CloudHomeJoinInfo::CloudKit { .. } => {
            config.cloud_home_cloudkit_is_shared = true;
        }
    }

    config
}
