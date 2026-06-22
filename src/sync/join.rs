//! Join an existing shared library using an invite code.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tracing::info;

use crate::blob::BlobSource;
use crate::config::{CloudProvider, Config, ConfigError, HomeStorage};
use crate::database::Database;
use crate::encryption::{EncryptionError, EncryptionService};
use crate::join_code::InviteCode;
use crate::keys::{CloudHomeCredentials, KeyError, KeyService};
use crate::library_dir::LibraryDir;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::invite::{unwrap_library_key, InviteError};
use crate::sync::pull::{pull_changes, PullError};
use crate::sync::session::SyncedTable;
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
}

/// The invite names an OAuth provider, so joining needs a token the caller
/// fetched via the host OAuth flow first — the same precondition restore has.
#[cfg(feature = "oauth-providers")]
fn require_join_oauth(
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    provider_name: &str,
) -> Result<crate::oauth::OAuthTokens, JoinError> {
    oauth_tokens
        .ok_or_else(|| JoinError::Database(format!("{provider_name} join requires an OAuth token")))
}

/// Build a CloudHome from a JoinInfo for the join flow.
///
/// For OAuth providers, the caller supplies the tokens — fetched via the host's
/// OAuth flow before calling join, the same way restore does — and they are
/// saved to the library-scoped keyring.
async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    lib_ks: &KeyService,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
) -> Result<Box<dyn CloudHome>, JoinError> {
    use crate::storage::cloud::*;

    // Consumed only by the oauth provider arms below.
    #[cfg(not(feature = "oauth-providers"))]
    let _ = (&lib_ks, &oauth_tokens, &clock);

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

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            let tokens = require_join_oauth(oauth_tokens, "Google Drive")?;
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

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { shared_folder_id } => {
            let tokens = require_join_oauth(oauth_tokens, "Dropbox")?;
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

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            let tokens = require_join_oauth(oauth_tokens, "OneDrive")?;
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

        #[cfg(not(feature = "oauth-providers"))]
        CloudHomeJoinInfo::GoogleDrive { .. }
        | CloudHomeJoinInfo::Dropbox { .. }
        | CloudHomeJoinInfo::OneDrive { .. } => Err(JoinError::Database(
            "OAuth cloud providers are not supported in this build".to_string(),
        )),
        CloudHomeJoinInfo::CloudKit { .. } => {
            let ops = cloudkit_ops
                .ok_or_else(|| JoinError::Database("CloudKit driver not provided".to_string()))?;
            Ok(Box::new(cloudkit::CloudKitCloudHome::new(ops)))
        }
    }
}

/// Join a shared library using an invite code string.
///
/// Handles everything: decode invite, get keypair, build cloud home (using
/// caller-provided OAuth tokens for the providers that need them), run the join
/// protocol, and set as active library.
#[allow(clippy::too_many_arguments)]
pub async fn join_from_invite_code(
    invite_code_str: &str,
    app_dir: &Path,
    synced_tables: &[SyncedTable],
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    make_blob_source: impl Fn(&LibraryDir) -> Box<dyn BlobSource>,
    on_status: impl Fn(&str),
) -> Result<Config, JoinError> {
    let code = crate::join_code::decode(invite_code_str)
        .map_err(|e| JoinError::Database(format!("Invalid invite code: {e}")))?;

    let global_ks = KeyService::new("global".to_string());
    let lib_ks = KeyService::new(code.library_id.clone());

    let cloud_home =
        build_cloud_home_for_join(&code.join_info, &lib_ks, oauth_tokens, cloudkit_ops, clock)
            .await?;

    let config = join_library(
        app_dir,
        code,
        synced_tables,
        &global_ks,
        cloud_home,
        ids.as_ref(),
        make_blob_source,
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
#[allow(clippy::too_many_arguments)]
pub async fn join_library(
    data_dir: &Path,
    code: InviteCode,
    synced_tables: &[SyncedTable],
    key_service: &KeyService,
    cloud_home: Box<dyn CloudHome>,
    ids: &dyn crate::id_provider::IdProvider,
    make_blob_source: impl Fn(&LibraryDir) -> Box<dyn BlobSource>,
    on_status: impl Fn(&str),
) -> Result<Config, JoinError> {
    // Step 1: Load user keypair (must already exist — the inviter wrapped the
    // library key for this public key).
    on_status("Loading keypair...");
    let user_keypair = key_service.get_user_keypair()?;

    // Accept the invitation to get the library encryption key. Uses CloudHome
    // directly — wrapped keys are sealed-box encrypted, no library-key encryption
    // needed. The key is authenticated against the owner the invite pins (the
    // chain founder) before adoption, so a bucket writer can't substitute it.
    on_status("Accepting invitation...");
    let encryption_key = unwrap_library_key(
        cloud_home.as_ref(),
        &user_keypair,
        &code.library_id,
        &code.owner_pubkey,
    )
    .await?;
    let encryption_key_hex = hex::encode(encryption_key);

    // Step 3: Create the sync storage with the real encryption key. Joining a
    // shared library only makes sense over an opaque home — the invite wraps the
    // library key — so the home is always opaque here: the cipher is `Encrypted`
    // and the blob-path scheme is `Hashed`, matching what the owner writes.
    on_status("Downloading library snapshot...");
    let encryption = EncryptionService::new(&encryption_key_hex)?;
    let cipher = CloudCipher::Encrypted(encryption);
    let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
    // The device's signing identity signs the head/min_schema control objects it
    // writes; it's the same keypair the invite wrapped the library key for.
    let storage =
        CloudSyncStorage::new(cloud_home, cipher.clone(), blob_paths, user_keypair.clone());

    // Step 4: Create library directory using the invite code's library_id.
    let library_id = code.library_id;
    let device_id = ids.new_id();
    let library_dir = LibraryDir::new(data_dir.join("libraries").join(&library_id));
    std::fs::create_dir_all(&*library_dir)?;
    // The host's blob source is bound to the library dir we just created.
    let blob_source = make_blob_source(&library_dir);

    // All steps after directory creation are wrapped so we can clean up on failure.
    let new_key_service = KeyService::new(library_id.clone());

    let result = bootstrap_and_save(
        &storage,
        &cipher,
        &encryption_key_hex,
        &library_dir,
        &library_id,
        &device_id,
        &code.owner_pubkey,
        synced_tables,
        &code.join_info,
        &code.library_name,
        &new_key_service,
        blob_source.as_ref(),
        &on_status,
    )
    .await;

    if result.is_err() {
        let _ = std::fs::remove_dir_all(&*library_dir);
    }

    result
}

/// Inner bootstrap + save logic, separated so the caller can clean up on failure.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_and_save(
    storage: &CloudSyncStorage,
    cipher: &CloudCipher,
    encryption_key_hex: &str,
    library_dir: &LibraryDir,
    library_id: &str,
    device_id: &str,
    owner_pubkey: &str,
    synced_tables: &[SyncedTable],
    join_info: &CloudHomeJoinInfo,
    library_name: &str,
    key_service: &KeyService,
    blob_source: &dyn BlobSource,
    on_status: &impl Fn(&str),
) -> Result<Config, JoinError> {
    // Step 5: Bootstrap from snapshot.
    let db_path = library_dir.db_path();
    let bucket_dyn: &dyn SyncStorage = storage;
    let bootstrap_result = bootstrap_from_snapshot(bucket_dyn, cipher, &db_path).await?;

    info!(
        "Bootstrapped from snapshot ({} device cursors)",
        bootstrap_result.cursors.len()
    );

    // Step 6: Pull changesets since the snapshot.
    on_status("Applying recent changes...");
    let cursors = bootstrap_result.cursors;

    let changesets_applied = open_db_and_pull(
        &db_path,
        synced_tables,
        device_id,
        Some(owner_pubkey),
        bucket_dyn,
        &cursors,
        library_dir,
        blob_source,
    )
    .await?;

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
        cipher,
    );

    config.save_to_config_yaml()?;

    info!("Joined library {} at {}", library_id, library_dir.display());
    Ok(config)
}

/// Open a [`Database`] over the bootstrapped db file and pull changesets since
/// the snapshot.
///
/// The snapshot the bootstrap wrote already carries the full schema (the host's
/// tables and coven's bookkeeping), so `Database::open`'s bookkeeping migration
/// is idempotent and the host migrate is a no-op here. The fresh capture session
/// is suspended before pulling — a just-bootstrapped library has no local
/// changes to capture, and pull must apply with no session active.
pub(crate) async fn open_db_and_pull(
    db_path: &Path,
    synced_tables: &[SyncedTable],
    device_id: &str,
    owner_pubkey: Option<&str>,
    storage: &dyn SyncStorage,
    cursors: &HashMap<String, u64>,
    library_dir: &LibraryDir,
    blob_source: &dyn BlobSource,
) -> Result<u64, JoinError> {
    let (db, _stamper) = Database::open(
        db_path,
        synced_tables.to_vec(),
        device_id.to_string(),
        |_conn| Ok(()),
    )
    .map_err(|e| {
        JoinError::Database(format!("Failed to open database for changeset apply: {e}"))
    })?;

    // Pin the library owner from the invite BEFORE the pull below loads and anchors
    // the membership chain (issue #102). The pull then refuses a chain whose founder
    // isn't this owner, so a tampered chain can't be adopted during join. `None`
    // (restore, or a chain-less test) leaves the owner unpinned — restore pins it
    // from the chain founder on first sync connect.
    if let Some(owner) = owner_pubkey {
        db.set_sync_state(crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY, owner)
            .await
            .map_err(|e| JoinError::Database(format!("Failed to pin library owner: {e}")))?;
    }

    // Suspend the capture session so the apply during pull is not re-recorded.
    db.take_changeset_and_suspend()
        .await
        .map_err(|e| JoinError::Database(format!("Failed to suspend capture session: {e}")))?;

    // Download the blob files the snapshot's rows reference. The snapshot carried
    // the catalog rows but no per-row image/torrent files, and the pull below
    // starts past the snapshot's cursors so it never re-walks the INSERTs that
    // first carried them — without this a bootstrapped device renders placeholders
    // for synced cover art. A blob whose object is not yet in the cloud (or whose
    // download hits a transient error) here is not lost: the not-yet-complete
    // state is recorded in `sync_state`, and each later sync cycle re-runs the
    // reconciliation until every referenced blob is local.
    let backfill_ok =
        crate::sync::snapshot::reconcile_snapshot_blobs(&db, db_path, storage, blob_source)
            .await
            .map_err(|e| JoinError::Database(format!("Failed to reconcile snapshot blobs: {e}")))?;
    db.set_sync_state(
        crate::sync::snapshot::SNAPSHOT_BLOB_BACKFILL_PENDING,
        if backfill_ok { "" } else { "1" },
    )
    .await
    .map_err(|e| {
        JoinError::Database(format!(
            "Failed to persist snapshot blob backfill flag: {e}"
        ))
    })?;

    // Pull over the set `Database::open` owns (the host's tables plus coven's
    // injected `item_keys`), not the raw host list — one source of truth.
    let (updated_cursors, pull_result) = pull_changes(
        &db,
        db.synced_tables(),
        storage,
        device_id,
        cursors,
        library_dir,
        blob_source,
    )
    .await
    .map_err(JoinError::Pull)?;

    // Restore passes no owner (it recovers an existing library this device may not
    // have founded): adopt the owner from the chain's founder now, before the first
    // sync connect anchors the chain. Without this the connect would find a chain
    // founded by another key with no owner pinned and refuse it as foreign. This is
    // trust-on-first-use, acceptable for restore because the restore code carries
    // the bucket's own credentials — whoever holds it already controls the bucket.
    // Join already pinned its owner from the invite, so it skips this.
    if owner_pubkey.is_none() {
        let entries = storage.list_membership_entries().await.map_err(|e| {
            JoinError::Database(format!(
                "restore: failed to list membership to pin owner: {e}"
            ))
        })?;
        // An empty listing is a chain-less (plaintext/open) library — nothing to
        // pin. A non-empty one must load and pin, or fail the restore loudly so it
        // can be retried as a unit; leaving the owner unpinned and deferring to the
        // first sync connect would be a silent self-heal.
        if !entries.is_empty() {
            let chain = crate::sync::membership_ops::download_chain(storage, &entries)
                .await
                .map_err(|e| {
                    JoinError::Database(format!("restore: failed to load chain to pin owner: {e}"))
                })?;
            // A validated chain always has a founder (validation rejects an empty
            // chain), so this is defensive — but fail loud rather than skip the pin.
            let founder = chain.founder_pubkey().ok_or_else(|| {
                JoinError::Database("restore: loaded chain has no founder to pin".to_string())
            })?;
            db.set_sync_state(crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY, founder)
                .await
                .map_err(|e| JoinError::Database(format!("Failed to pin library owner: {e}")))?;
        }
    }

    // Persist the post-bootstrap sync bookkeeping so the first real sync cycle
    // treats this device as a joiner, not a brand-new library. `bootstrap_from_snapshot`
    // restores the snapshot's data but writes no `sync_state`, and `pull_changes`
    // returns the advanced cursors for the caller to persist. Without both, the
    // first cycle sees an empty `sync_state` (`snapshot_seq = None`, no cursors),
    // trips `is_initial_sync`, and republishes this device's snapshot over the
    // shared one — overwriting the owner's catalog with whatever this device
    // happens to hold.
    //
    // Cursors begin at the snapshot's per-device positions and advance by whatever
    // the pull applied; persist the merged map so the next cycle does not re-pull
    // from zero.
    let mut persisted_cursors = cursors.clone();
    persisted_cursors.extend(updated_cursors);
    for (cursor_device, seq) in &persisted_cursors {
        db.set_sync_cursor(cursor_device, *seq).await.map_err(|e| {
            JoinError::Database(format!(
                "Failed to persist sync cursor for {cursor_device}: {e}"
            ))
        })?;
    }

    // This device bootstrapped from a snapshot; its snapshot basis is its current
    // `local_seq` (0 — it has pushed no changesets of its own). Recording it is
    // what keeps the first cycle's initial-sync path from firing.
    db.set_sync_state("snapshot_seq", "0")
        .await
        .map_err(|e| JoinError::Database(format!("Failed to persist snapshot_seq: {e}")))?;

    Ok(pull_result.changesets_applied)
}

/// Derive the CloudHomeCredentials to persist from the JoinInfo.
///
/// S3 credentials come from the invite code. OAuth providers store the joiner's
/// token, but that's already saved to the keyring by the caller before
/// constructing the CloudHome.
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
        // OAuth providers: the joiner's token was already saved to the keyring
        // before constructing the CloudHome. No additional save needed here,
        // but set_cloud_home_credentials expects a value, so pass what we have.
        CloudHomeJoinInfo::GoogleDrive { .. }
        | CloudHomeJoinInfo::Dropbox { .. }
        | CloudHomeJoinInfo::OneDrive { .. }
        | CloudHomeJoinInfo::CloudKit { .. } => CloudHomeCredentials::None,
    }
}

/// Build the Config struct from join/restore parameters. The cipher records the
/// home's storage mode: `Encrypted` ⇒ opaque (a library key is stored, with its
/// fingerprint), `Plaintext` ⇒ browsable (no key, no fingerprint).
pub(crate) fn build_config(
    library_id: &str,
    device_id: &str,
    library_dir: &LibraryDir,
    library_name: &str,
    join_info: &CloudHomeJoinInfo,
    cipher: &CloudCipher,
) -> Config {
    let mut config = Config::with_defaults(
        library_id.to_string(),
        device_id.to_string(),
        library_dir.clone(),
        library_name.to_string(),
    );

    match cipher {
        CloudCipher::Encrypted(enc) => {
            config.cloud_home.storage = HomeStorage::Opaque;
            config.encryption_key_stored = true;
            config.encryption_key_fingerprint = Some(enc.fingerprint());
        }
        CloudCipher::Plaintext => {
            config.cloud_home.storage = HomeStorage::Browsable;
            config.encryption_key_stored = false;
            config.encryption_key_fingerprint = None;
        }
    }

    config.cloud_home.provider = Some(match join_info {
        CloudHomeJoinInfo::S3 { .. } => CloudProvider::S3,
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
            config.cloud_home.s3_bucket = Some(bucket.clone());
            config.cloud_home.s3_region = Some(region.clone());
            config.cloud_home.s3_endpoint = endpoint.clone();
            config.cloud_home.s3_key_prefix = key_prefix.clone();
        }
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            config.cloud_home.google_drive_folder_id = Some(folder_id.clone());
        }
        CloudHomeJoinInfo::Dropbox { shared_folder_id } => {
            config.cloud_home.dropbox_folder_path = Some(shared_folder_id.clone());
        }
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            config.cloud_home.onedrive_drive_id = Some(drive_id.clone());
            config.cloud_home.onedrive_folder_id = Some(folder_id.clone());
        }
        CloudHomeJoinInfo::CloudKit { .. } => {
            config.cloud_home.cloudkit_is_shared = true;
        }
    }

    config
}
