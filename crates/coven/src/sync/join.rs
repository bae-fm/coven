//! Join an existing shared store using an invite code.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tokio::sync::watch;
use tracing::info;

use crate::config::{CloudProvider, Config, ConfigError, HomeStorage};
use crate::database::Database;
use crate::encryption::{EncryptionError, MasterKeyring};
use crate::join_code::InviteCode;
use crate::keys::{CloudHomeCredentials, DeviceKeys, KeyError, MasterKeyCustody, StoreKeys};
use crate::migration::{supported_version, Migration};
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::store_dir::{StoreDir, StoreLayout};
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::invite::{unwrap_store_keyring, InviteError};
use crate::sync::membership::MembershipCoord;
use crate::sync::pull::{pull_changes, PullError};
use crate::sync::session::SyncedTable;
use crate::sync::snapshot::{bootstrap_from_snapshot, SnapshotError};
use crate::sync::storage::SyncStorage;

/// Why joining or restoring a store failed. Both are the same operation —
/// bootstrap a store from the cloud — differing only in their entry data (an
/// invite that wraps the store key vs a restore code that carries the bucket
/// credentials), so they share one error shape rather than two that duplicate
/// most of their variants and then have to map between each other.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
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
    #[error("invalid code: {0}")]
    InvalidCode(String),
    #[error("store already exists locally: {0}")]
    StoreExists(String),
    #[error("provider: {0}")]
    Provider(String),
    #[error("membership: {0}")]
    Membership(String),
    #[error("database: {0}")]
    Database(String),
    #[error("invalid signing key: {0}")]
    InvalidSigningKey(String),
    /// The caller's cancel signal fired at a phase boundary, so the join or
    /// restore stopped before saving the store. This returns through the same
    /// failure-cleanup path a real error takes — removing the partly-created
    /// store directory and any per-store keyring entries written so far — so a
    /// cancelled bootstrap leaves no residue in either place.
    #[error("the operation was cancelled")]
    Cancelled,
    /// Bootstrap failed AND cleaning up what it had durably written also failed.
    /// Both are carried: `cause` is the original bootstrap failure that
    /// triggered the cleanup, `cleanup` is why the cleanup itself failed — the
    /// cause is preserved as a value, not flattened into a string.
    #[error("could not clean up the partial store after bootstrap failed: {cleanup} (bootstrap error: {cause})")]
    Cleanup {
        cleanup: String,
        cause: Box<BootstrapError>,
    },
}

/// Undo everything a bootstrap attempt may have durably written, then return
/// the error to propagate. The exists-guard at the top of every join/restore
/// entry point establishes that this invocation owns everything under the
/// store id — no live store existed when it started, so the store-scoped
/// directory, custody, and keyring accounts are this invocation's alone to
/// remove, which makes total removal unconditionally safe here. Three steps,
/// each best-effort so one failing doesn't skip the others: the store
/// directory (tolerating it never having existed — a failure before
/// `create_dir_all` leaves nothing to remove; also covers a Passphrase
/// custody's wrapped-file, which lives inside it), the master key via custody
/// (idempotent regardless of policy), and the cloud-home credentials (OAuth
/// tokens are stored *as* credentials — see
/// `StoreKeys::set_cloud_home_oauth_tokens` — so this one delete covers both).
/// On a clean run the original `cause` is returned unchanged; if any step
/// fails, every failure is carried in a [`BootstrapError::Cleanup`] so none is
/// lost. Shared by join and restore.
pub(crate) fn cleanup_after_bootstrap_failure(
    store_dir: &StoreDir,
    store_keys: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    cause: BootstrapError,
) -> BootstrapError {
    let mut failures: Vec<String> = Vec::new();

    match std::fs::remove_dir_all(&**store_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => failures.push(format!("store directory: {e}")),
    }

    if let Err(e) = custody.forget() {
        failures.push(format!("master key: {e}"));
    }

    if let Err(e) = store_keys.delete_cloud_home_credentials() {
        failures.push(format!("cloud home credentials: {e}"));
    }

    if failures.is_empty() {
        cause
    } else {
        BootstrapError::Cleanup {
            cleanup: failures.join("; "),
            cause: Box::new(cause),
        }
    }
}

/// Fail with [`BootstrapError::Cancelled`] if the caller's cancel signal has
/// fired. Called only at phase boundaries — never mid-download or mid-write —
/// so a cancellation returns through the same failure-cleanup path a real error
/// takes, mirroring `make_local`'s cooperative cancellation.
fn error_if_cancelled(cancel: &watch::Receiver<bool>) -> Result<(), BootstrapError> {
    if *cancel.borrow() {
        Err(BootstrapError::Cancelled)
    } else {
        Ok(())
    }
}

pub(crate) enum BootstrapContext<'a> {
    Join { owner_pubkey: &'a str },
    Restore,
}

impl BootstrapContext<'_> {
    fn owner_pubkey(&self) -> Option<&str> {
        match self {
            BootstrapContext::Join { owner_pubkey } => Some(*owner_pubkey),
            BootstrapContext::Restore => None,
        }
    }
}

/// The invite names an OAuth provider, so joining needs a token the caller
/// fetched via the host OAuth flow first — the same precondition restore has.
#[cfg(feature = "oauth-providers")]
fn require_join_oauth(
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    provider_name: &str,
) -> Result<crate::oauth::OAuthTokens, BootstrapError> {
    oauth_tokens.ok_or_else(|| {
        BootstrapError::Provider(format!("{provider_name} join requires an OAuth token"))
    })
}

/// Persist the caller-supplied OAuth tokens for the store `ks` is scoped to, so
/// the next launch's home construction (`parse_oauth_tokens` in
/// `storage::cloud`) can read them back from the keyring instead of erroring on
/// their absence. Both join and restore build an OAuth provider home from
/// tokens the caller already holds; both must save them here before returning
/// that home.
#[cfg(feature = "oauth-providers")]
pub(crate) fn persist_oauth_tokens(
    ks: &StoreKeys,
    tokens: &crate::oauth::OAuthTokens,
) -> Result<(), KeyError> {
    ks.set_cloud_home_oauth_tokens(tokens)
}

/// Build a CloudHome from a JoinInfo for the join flow.
///
/// For OAuth providers, the caller supplies the tokens — fetched via the host's
/// OAuth flow before calling join, the same way restore does — and they are
/// saved to the store-scoped keyring.
async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    lib_ks: &StoreKeys,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
) -> Result<Box<dyn CloudHome>, BootstrapError> {
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
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(Box::new(google_drive::GoogleDriveCloudHome::new(
                folder_id.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )?))
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            let tokens = require_join_oauth(oauth_tokens, "Dropbox")?;
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(Box::new(dropbox::DropboxCloudHome::new(
                folder_path.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )?))
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            let tokens = require_join_oauth(oauth_tokens, "OneDrive")?;
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(Box::new(onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                tokens,
                lib_ks.clone(),
                clock,
            )?))
        }

        #[cfg(not(feature = "oauth-providers"))]
        CloudHomeJoinInfo::GoogleDrive { .. }
        | CloudHomeJoinInfo::Dropbox { .. }
        | CloudHomeJoinInfo::OneDrive { .. } => Err(BootstrapError::Provider(
            "OAuth cloud providers are not supported in this build".to_string(),
        )),
        CloudHomeJoinInfo::CloudKit => {
            let ops = cloudkit_ops.ok_or_else(|| {
                BootstrapError::Provider("CloudKit driver not provided".to_string())
            })?;
            Ok(Box::new(cloudkit::CloudKitCloudHome::new_private(ops)))
        }
        CloudHomeJoinInfo::CloudKitShare {
            share_url,
            owner_name,
            zone_name,
        } => {
            let ops = cloudkit_ops.ok_or_else(|| {
                BootstrapError::Provider("CloudKit driver not provided".to_string())
            })?;
            let accepted = cloudkit::accept_share(ops.clone(), share_url.clone()).await?;
            if accepted.owner_name != *owner_name || accepted.zone_name != *zone_name {
                return Err(BootstrapError::Provider(format!(
                    "CloudKit accepted share zone mismatch: invite owner/zone {owner_name}/{zone_name}, accepted {}/{}",
                    accepted.owner_name, accepted.zone_name
                )));
            }
            Ok(Box::new(cloudkit::CloudKitCloudHome::new_shared(
                ops,
                owner_name.clone(),
                zone_name.clone(),
            )))
        }
    }
}

/// Join a shared store using an invite code string.
///
/// Handles everything: decode invite, get keypair, build cloud home (using
/// caller-provided OAuth tokens for the providers that need them), run the join
/// protocol, and set as active store.
#[allow(clippy::too_many_arguments)]
pub async fn join_from_invite_code(
    invite_code_str: &str,
    layout: &StoreLayout,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    key_custody: crate::custody::KeyCustody,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    crate::install_platform();

    let code = crate::join_code::decode(invite_code_str)
        .map_err(|e| BootstrapError::InvalidCode(e.to_string()))?;

    // Refuse a store already present locally before any keyring or provider side
    // effect. Re-joining a store you already have adds nothing — the existing
    // store is the data. `join_store` below is the authoritative guard against
    // the destructive failure-cleanup; this check exists so a refused join also
    // leaves no residue from `build_cloud_home_for_join`, which runs first and
    // saves OAuth tokens to the keyring and can accept a CloudKit share.
    let store_dir = layout.store_dir(&code.store_id);
    if store_dir.exists() {
        return Err(BootstrapError::StoreExists(code.store_id));
    }

    // Hoisted here, before any durable write below, so a failure at any step —
    // including `build_cloud_home_for_join`'s OAuth persist, which runs before
    // `join_store` even creates the store directory — funnels through the same
    // rollback instead of escaping via `?`.
    let store_keys = StoreKeys::new(code.store_id.clone());
    let custody = key_custody.resolve(&code.store_id, &store_dir);

    let result = async {
        let cloud_home = build_cloud_home_for_join(
            &code.join_info,
            &store_keys,
            oauth_tokens,
            cloudkit_ops,
            clock,
        )
        .await?;

        join_store(
            layout,
            code,
            synced_tables,
            migrations,
            custody.clone(),
            cloud_home,
            ids.as_ref(),
            &on_status,
            cancel,
        )
        .await
    }
    .await;

    match result {
        // The host records this as the active store after this returns.
        Ok(config) => Ok(config),
        Err(err) => Err(cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            err,
        )),
    }
}

/// Join an existing shared store using a decoded invite code.
///
/// Lower-level function — caller provides pre-built `CloudHome`.
/// Prefer `join_from_invite_code` for the full flow.
///
/// `on_status` is called with progress messages for UI feedback.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn join_store(
    layout: &StoreLayout,
    code: InviteCode,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    custody: Arc<dyn MasterKeyCustody>,
    cloud_home: Box<dyn CloudHome>,
    ids: &dyn crate::id_provider::IdProvider,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    crate::install_platform();

    // Guard the destructive `stores/<id>` create/delete against any direct
    // caller, independent of the decode-time check on untrusted input.
    crate::store_dir::validate_path_token(&code.store_id)
        .map_err(|e| BootstrapError::InvalidCode(format!("invalid store id: {e}")))?;

    // Refuse a store already present locally before any keyring or filesystem
    // side effect, independent of whatever `join_from_invite_code` (or any
    // other caller) already checked. Re-joining a store you already have adds
    // nothing — the existing store is the data — and letting the join proceed
    // would, on any bootstrap failure below, delete that store's database and
    // blobs during cleanup. This is the authoritative guard: it makes the
    // failure-cleanup below only ever remove a directory this invocation
    // created.
    let store_dir = layout.store_dir(&code.store_id);
    if store_dir.exists() {
        return Err(BootstrapError::StoreExists(code.store_id));
    }

    // Hoisted here, before any durable write below, so every step through the
    // end of this function — keypair load, invitation accept, directory
    // creation, bootstrap — funnels a failure through the same rollback
    // instead of a bare `?` escaping it.
    let store_keys = StoreKeys::new(code.store_id.clone());

    let result = async {
        // Load the device signing keypair (must already exist — the inviter
        // wrapped the store key for this public key, so join never mints one
        // of its own). It is device-global, not store-scoped.
        on_status("Loading keypair...");
        let user_keypair = DeviceKeys::require_user_keypair()?;

        // Accept the invitation to get the store encryption key. The joiner
        // authenticates it against the current Owner set derived from the
        // membership chain anchored to the owner the invite pins (the chain
        // founder), so any current Owner's invite is joinable yet a bucket
        // writer still can't substitute a key. The chain is sealed under the
        // store key, so `Arc` the home once and reuse it for the sync storage
        // below.
        on_status("Accepting invitation...");
        let cloud_home: std::sync::Arc<dyn CloudHome> = std::sync::Arc::from(cloud_home);
        let encryption = unwrap_store_keyring(
            cloud_home.clone(),
            &user_keypair,
            &code.store_id,
            &code.owner_pubkey,
        )
        .await?;
        let master_key = MasterKeyring::from(encryption.clone());

        // Create the sync storage with the real encryption key. Joining a
        // shared store only makes sense over an opaque home — the invite wraps
        // the store key — so the home is always opaque here: the cipher is
        // `Encrypted` and the blob-path scheme is `Hashed`, matching what the
        // owner writes.
        let cipher = CloudCipher::Encrypted(encryption);
        let blob_paths = BlobPathScheme::for_storage(HomeStorage::Opaque);
        // The device's signing identity signs the head/min_schema control
        // objects it writes; it's the same keypair the invite wrapped the
        // store key for.
        let storage = CloudSyncStorage::new(
            cloud_home,
            cipher.clone(),
            blob_paths,
            code.store_id.clone(),
            user_keypair.clone(),
        );

        // Create the store directory under `stores/`, named by the invite's id
        // (its non-existence was checked up front, so this create and the
        // failure-cleanup below own it entirely).
        let device_id = ids.new_id();
        std::fs::create_dir_all(&*store_dir)?;

        bootstrap_and_save_store(
            &storage,
            &cipher,
            Some(&master_key),
            &store_dir,
            &code.store_id,
            &device_id,
            BootstrapContext::Join {
                owner_pubkey: &code.owner_pubkey,
            },
            &code.membership_floor,
            synced_tables,
            migrations,
            &code.join_info,
            &code.store_name,
            &store_keys,
            custody.as_ref(),
            &on_status,
            cancel,
        )
        .await
    }
    .await;

    match result {
        Ok(config) => {
            info!("Joined store {} at {}", code.store_id, store_dir.display());
            Ok(config)
        }
        Err(err) => Err(cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            err,
        )),
    }
}

/// Inner bootstrap + save logic for join and restore, separated so callers can
/// clean up the store directory on failure.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap_and_save_store(
    storage: &CloudSyncStorage,
    cipher: &CloudCipher,
    master_key: Option<&MasterKeyring>,
    store_dir: &StoreDir,
    store_id: &str,
    device_id: &str,
    context: BootstrapContext<'_>,
    membership_floor: &[MembershipCoord],
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    join_info: &CloudHomeJoinInfo,
    store_name: &str,
    key_service: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    on_status: &impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    // Join pins the store owner from the invite. Restore adopts the owner
    // from the chain founder during `open_db_and_pull`, because the restore code
    // carries the bucket credentials rather than an inviter assertion.
    //
    // Cancellation is checked between phases (before the snapshot download here,
    // before the pull inside `open_db_and_pull`, and per-blob during blob
    // reconciliation), never inside a download. A cancel returns through the same
    // failure-cleanup the caller runs, so the store directory is removed.
    error_if_cancelled(cancel)?;
    on_status("Downloading store snapshot...");
    let db_path = store_dir.db_path();
    let bucket_dyn: &dyn SyncStorage = storage;
    let binary_schema_version = supported_version(migrations);
    let owner_pubkey = context.owner_pubkey();
    let bootstrap_result = bootstrap_from_snapshot(
        bucket_dyn,
        store_id,
        owner_pubkey,
        binary_schema_version,
        &db_path,
    )
    .await?;

    info!(
        "Bootstrapped from snapshot ({} device cursors)",
        bootstrap_result.cursors.len()
    );

    // Step 6: Pull changesets since the snapshot.
    error_if_cancelled(cancel)?;
    on_status("Applying recent changes...");
    let cursors = bootstrap_result.cursors;

    let changesets_applied = open_db_and_pull(
        &db_path,
        synced_tables,
        migrations,
        device_id,
        owner_pubkey,
        membership_floor,
        bucket_dyn,
        &cursors,
        store_dir,
        cancel,
    )
    .await?;

    if changesets_applied > 0 {
        info!("Applied {changesets_applied} changesets since snapshot");
    }

    // Step 7: Persist the master key via custody.
    on_status("Saving configuration...");
    if let Some(keyring) = master_key {
        custody.persist(keyring)?;
    }

    // Step 8: Save cloud credentials to keyring.
    if let Some(credentials) = derive_credentials(join_info) {
        key_service.set_cloud_home_credentials(&credentials)?;
    }

    // Step 9: Create and save config.
    let config = build_config(
        store_id, device_id, store_dir, store_name, join_info, cipher,
    );

    config.save_to_config_yaml()?;

    Ok(config)
}

/// Open a [`Database`] over the bootstrapped db file and pull changesets since
/// the snapshot. The snapshot already carries the full schema and the writer's
/// `user_version`, so the migration ladder only carries the image forward when
/// this binary is newer (a no-op when they match).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_db_and_pull(
    db_path: &Path,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    device_id: &str,
    owner_pubkey: Option<&str>,
    membership_floor: &[MembershipCoord],
    storage: &dyn SyncStorage,
    cursors: &HashMap<String, u64>,
    store_dir: &StoreDir,
    cancel: &watch::Receiver<bool>,
) -> Result<u64, BootstrapError> {
    let (db, _stamper) = Database::open(
        db_path,
        synced_tables.to_vec(),
        // This bootstrap database only applies changesets during join; it never runs
        // the tombstone GC, so the grace is immaterial and takes the default.
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        device_id.to_string(),
        migrations,
    )
    .map_err(|e| {
        BootstrapError::Database(format!("Failed to open database for changeset apply: {e}"))
    })?;

    // Seed this device's per-author membership-head watermark from the invite
    // or restore code's floor BEFORE the pull below loads and anchors the
    // membership chain for the first time. A fresh joiner or restorer has no
    // watermark yet, so without this, the first load would accept any signed
    // head — including an older one a storage provider chooses to serve, e.g.
    // from before a removal the floor already reflects. Seeding it here makes
    // that first load monotonic from the start, exactly like every later cycle.
    crate::sync::membership_ops::seed_head_watermark(&db, membership_floor)
        .await
        .map_err(|e| {
            BootstrapError::Database(format!("Failed to seed membership head watermark: {e}"))
        })?;

    // Pin the store owner from the invite BEFORE the pull below loads and anchors
    // the membership chain (issue #102). The pull then refuses a chain whose founder
    // isn't this owner, so a tampered chain can't be adopted during join. `None`
    // means restore or a chain-less test; restore pins the chain founder below
    // after loading membership entries from the bootstrapped storage.
    if let Some(owner) = owner_pubkey {
        db.set_sync_state(crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY, owner)
            .await
            .map_err(|e| BootstrapError::Database(format!("Failed to pin store owner: {e}")))?;
    }

    // Download the blob files the snapshot's rows reference: the snapshot carried
    // the catalog rows but no blob files, and the pull starts past its cursors so
    // it never re-walks the INSERTs that first carried them. Missing eager blobs
    // abort the bootstrap before the store is saved. Cancellation is checked
    // between blobs inside the reconcile, surfacing as `Cancelled` here.
    match crate::sync::snapshot::reconcile_snapshot_blobs(
        &db,
        db_path,
        storage,
        store_dir,
        db.synced_tables(),
        cancel,
    )
    .await
    .map_err(|e| BootstrapError::Database(format!("Failed to reconcile snapshot blobs: {e}")))?
    {
        crate::sync::snapshot::SnapshotBlobReconcile::Complete => {}
        crate::sync::snapshot::SnapshotBlobReconcile::Incomplete => {
            return Err(BootstrapError::Database(
                "Snapshot blob reconciliation did not land every required eager blob".to_string(),
            ));
        }
        crate::sync::snapshot::SnapshotBlobReconcile::Cancelled => {
            return Err(BootstrapError::Cancelled);
        }
    }

    // The pull downloads every changeset published since the snapshot — the last
    // heavy phase. Check cancellation before entering it; the pull itself is a
    // single phase here (per-changeset cancel is the sync loop's own concern).
    error_if_cancelled(cancel)?;

    // Pull over the synced set coven owns, not the raw host list — one source of
    // truth. Load and anchor the
    // membership chain first (join is a standalone, non-cycle pull), against the
    // owner just pinned above; restore hasn't pinned yet, so it loads the chain
    // best-effort and pins from the founder below.
    let membership = crate::sync::pull::load_cycle_membership(storage, &db)
        .await
        .map_err(BootstrapError::Pull)?;
    let (updated_cursors, pull_result) = pull_changes(
        &db,
        db.synced_tables(),
        storage,
        device_id,
        cursors,
        store_dir,
        membership.chain,
        membership.pinned_owner,
    )
    .await
    .map_err(BootstrapError::Pull)?;

    // Restore passes no owner (it recovers an existing store this device may not
    // have founded): adopt the owner from the chain's founder now, before the first
    // sync connect anchors the chain. Without this the connect would find a chain
    // founded by another key with no owner pinned and refuse it as foreign. This is
    // trust-on-first-use, acceptable for restore because the restore code carries
    // the bucket's own credentials — whoever holds it already controls the bucket.
    // Join already pinned its owner from the invite, so it skips this.
    if owner_pubkey.is_none() {
        let entries = storage.list_membership_entries().await.map_err(|e| {
            BootstrapError::Membership(format!(
                "restore: failed to list membership to pin owner: {e}"
            ))
        })?;
        // An empty listing is a chain-less (plaintext/open) store — nothing to
        // pin. A non-empty one must load and pin, or fail the restore loudly so it
        // can be retried as a unit; leaving the owner unpinned and deferring to the
        // first sync connect would be a silent self-heal.
        if !entries.is_empty() {
            let chain = crate::sync::membership_ops::download_chain(storage, &entries)
                .await
                .map_err(|e| {
                    BootstrapError::Membership(format!(
                        "restore: failed to load chain to pin owner: {e}"
                    ))
                })?;
            // A validated chain always has a founder (validation rejects an empty
            // chain), so this is defensive — but fail loud rather than skip the pin.
            let founder = chain.founder_pubkey().ok_or_else(|| {
                BootstrapError::Membership(
                    "restore: loaded chain has no founder to pin".to_string(),
                )
            })?;
            db.set_sync_state(crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY, founder)
                .await
                .map_err(|e| BootstrapError::Database(format!("Failed to pin store owner: {e}")))?;
        }
    }

    // Persist the post-bootstrap sync bookkeeping so the first real sync cycle
    // treats this device as a joiner, not a brand-new store. `bootstrap_from_snapshot`
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
            BootstrapError::Database(format!(
                "Failed to persist sync cursor for {cursor_device}: {e}"
            ))
        })?;
    }

    // This device bootstrapped from a snapshot; its snapshot basis is its current
    // `local_seq` (0 — it has pushed no changesets of its own). Recording it is
    // what keeps the first cycle's initial-sync path from firing.
    db.set_sync_state("snapshot_seq", "0")
        .await
        .map_err(|e| BootstrapError::Database(format!("Failed to persist snapshot_seq: {e}")))?;

    Ok(pull_result.changesets_applied)
}

/// The credentials to persist for this join, or `None` when the provider needs
/// no stored secret (OAuth tokens are already saved; CloudKit uses the container).
pub(crate) fn derive_credentials(join_info: &CloudHomeJoinInfo) -> Option<CloudHomeCredentials> {
    match join_info {
        CloudHomeJoinInfo::S3 {
            access_key,
            secret_key,
            ..
        } => Some(CloudHomeCredentials::S3 {
            access_key: access_key.clone(),
            secret_key: secret_key.clone(),
        }),
        _ => None,
    }
}

/// Build the Config struct from join/restore parameters. The cipher records the
/// home's storage mode: `Encrypted` ⇒ opaque (a store key is stored, with its
/// fingerprint), `Plaintext` ⇒ browsable (no key, no fingerprint).
pub(crate) fn build_config(
    store_id: &str,
    device_id: &str,
    store_dir: &StoreDir,
    store_name: &str,
    join_info: &CloudHomeJoinInfo,
    cipher: &CloudCipher,
) -> Config {
    let mut config = Config::with_defaults(
        store_id.to_string(),
        device_id.to_string(),
        store_dir.clone(),
        store_name.to_string(),
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
        CloudHomeJoinInfo::CloudKit | CloudHomeJoinInfo::CloudKitShare { .. } => {
            CloudProvider::CloudKit
        }
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
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            config.cloud_home.dropbox_folder_path = Some(folder_path.clone());
        }
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            config.cloud_home.onedrive_drive_id = Some(drive_id.clone());
            config.cloud_home.onedrive_folder_id = Some(folder_id.clone());
        }
        CloudHomeJoinInfo::CloudKit => {}
        CloudHomeJoinInfo::CloudKitShare {
            owner_name,
            zone_name,
            ..
        } => {
            config.cloud_home.cloudkit_owner_name = Some(owner_name.clone());
            config.cloud_home.cloudkit_zone_name = Some(zone_name.clone());
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;

    /// When the post-failure directory cleanup itself fails, both failures are
    /// carried: `cleanup` records why the removal failed and `cause` preserves
    /// the ORIGINAL bootstrap error as a value — not flattened into a string.
    /// Join and restore both route their failure path through this one helper,
    /// so exercising it covers both flows' cleanup behavior. The dir removal is
    /// the failure here: a *file* sits where the store dir should be, so
    /// `remove_dir_all` fails with something other than not-found (which is
    /// tolerated, not a failure — see the dedicated test below).
    #[test]
    fn cleanup_failure_carries_the_original_bootstrap_cause() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let blocked = StoreDir::new(tmp.path().join("blocked-by-a-file"));
        std::fs::write(&*blocked, b"not a directory").expect("seed a file at the store dir path");
        let store_keys = StoreKeys::new("cleanup-failure-cause-test".to_string());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("cleanup-failure-cause-test", &blocked);

        let wrapped = cleanup_after_bootstrap_failure(
            &blocked,
            &store_keys,
            custody.as_ref(),
            BootstrapError::Database("bootstrap boom".to_string()),
        );

        match wrapped {
            BootstrapError::Cleanup { cleanup, cause } => {
                assert!(!cleanup.is_empty(), "the removal failure is recorded");
                assert!(
                    matches!(*cause, BootstrapError::Database(ref m) if m == "bootstrap boom"),
                    "the original bootstrap cause is preserved as a value, got {cause:?}",
                );
            }
            other => panic!("a failed cleanup must yield Cleanup, got {other:?}"),
        }
    }

    /// When the cleanup succeeds, the original bootstrap error propagates
    /// unchanged — no `Cleanup` wrapper — and the partial store dir is gone.
    #[test]
    fn successful_cleanup_returns_the_cause_unchanged() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path().join("to-remove"));
        std::fs::create_dir_all(&*store_dir).expect("create store dir");
        let store_keys = StoreKeys::new("successful-cleanup-test".to_string());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("successful-cleanup-test", &store_dir);

        let returned = cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            BootstrapError::Database("bootstrap boom".to_string()),
        );

        assert!(
            matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
            "a clean removal returns the cause unchanged, got {returned:?}",
        );
        assert!(!store_dir.exists(), "the partial store dir was removed");
    }

    /// A bootstrap failure before `create_dir_all` ever ran (e.g. the OAuth
    /// persist or cloud-home construction failed first) leaves no store dir to
    /// remove. `remove_dir_all` on a path that never existed returns
    /// `NotFound`, and that must be tolerated — not folded into `Cleanup` — so a
    /// pre-directory failure still reports as the plain original cause.
    #[test]
    fn cleanup_tolerates_a_store_dir_that_was_never_created() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let never_created = StoreDir::new(tmp.path().join("never-created"));
        let store_keys = StoreKeys::new("never-created-dir-test".to_string());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("never-created-dir-test", &never_created);

        let returned = cleanup_after_bootstrap_failure(
            &never_created,
            &store_keys,
            custody.as_ref(),
            BootstrapError::Database("bootstrap boom".to_string()),
        );

        assert!(
            matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
            "a missing store dir must not itself count as a cleanup failure, got {returned:?}",
        );
    }

    /// The extended rollback also removes the store-scoped keyring accounts —
    /// the encryption master key and the cloud-home credentials (which is also
    /// where an OAuth token lands, via `set_cloud_home_oauth_tokens`) — not just
    /// the directory. Seed both accounts the way a partial bootstrap would have
    /// written them, then assert cleanup leaves neither behind.
    #[test]
    fn cleanup_also_removes_both_keyring_accounts() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path().join("keyring-cleanup-test"));
        std::fs::create_dir_all(&*store_dir).expect("create store dir");
        let store_keys = StoreKeys::new("keyring-cleanup-test".to_string());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("keyring-cleanup-test", &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key via custody");
        store_keys
            .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
            })
            .expect("seed cloud home credentials");

        let returned = cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            BootstrapError::Database("bootstrap boom".to_string()),
        );

        assert!(
            matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
            "a clean removal returns the cause unchanged, got {returned:?}",
        );
        assert!(!store_dir.exists(), "the partial store dir was removed");
        assert_eq!(
            store_keys.get_encryption_key().expect("read keyring"),
            None,
            "the encryption key must be removed from the keyring",
        );
        assert!(
            store_keys
                .get_cloud_home_credentials()
                .expect("read keyring")
                .is_none(),
            "the cloud home credentials must be removed from the keyring",
        );
    }

    /// Only S3 maps to a stored value; every other provider returns `None` so
    /// the join never overwrites an already-saved OAuth token (or a CloudKit
    /// container) with credentials.
    #[test]
    fn derive_credentials_only_stores_for_s3() {
        let s3 = CloudHomeJoinInfo::S3 {
            bucket: "b".to_string(),
            region: "r".to_string(),
            endpoint: None,
            key_prefix: None,
            access_key: "ak".to_string(),
            secret_key: "sk".to_string(),
        };
        match derive_credentials(&s3) {
            Some(CloudHomeCredentials::S3 {
                access_key,
                secret_key,
            }) => {
                assert_eq!(access_key, "ak");
                assert_eq!(secret_key, "sk");
            }
            other => panic!("expected Some(S3), got {other:?}"),
        }

        for oauth in [
            CloudHomeJoinInfo::GoogleDrive {
                folder_id: "f".to_string(),
            },
            CloudHomeJoinInfo::Dropbox {
                folder_path: "f".to_string(),
            },
            CloudHomeJoinInfo::OneDrive {
                drive_id: "d".to_string(),
                folder_id: "f".to_string(),
            },
            CloudHomeJoinInfo::CloudKit,
        ] {
            assert!(
                derive_credentials(&oauth).is_none(),
                "non-S3 provider must not map to stored credentials"
            );
        }
    }
}
