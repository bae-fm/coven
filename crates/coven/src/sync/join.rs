//! Join an existing shared store using an invite code.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::path::Path;
use std::sync::Arc;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::{CloudProvider, Config, ConfigError, HomeStorage};
use crate::database::Database;
use crate::encryption::{EncryptionError, MasterKeyring};
use crate::identity_custody::IdentityCustody;
use crate::join_code::{InviteCode, MembershipFloor};
use crate::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys, UserKeypair,
};
use crate::migration::{supported_version, Migration};
use crate::storage::cloud::setup::CoordinationClients;
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::store_dir::{StoreDir, StoreLayout};
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::sync::invite::{unwrap_serial_store_keyring, unwrap_store_keyring, InviteError};
use crate::sync::pull::PullError;
use crate::sync::session::SyncedTable;
use crate::sync::snapshot::{bootstrap_from_snapshot, BootstrapResult, SnapshotError};
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
    #[error("Store pull: {0}")]
    StorePull(#[from] crate::sync::store_pull::StorePullError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] crate::sync::store_registration::StoreRegistrationError),
    #[error("storage: {0}")]
    Storage(#[from] crate::sync::storage::StorageError),
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
    /// A hard crash left a store directory with no saved config, and clearing
    /// that torn-bootstrap residue before retrying failed — so the retry can't
    /// proceed over the leftover directory or keyring entries.
    #[error("could not clear a torn bootstrap for {store_id}: {failures}")]
    TornBootstrapCleanup { store_id: String, failures: String },
    #[error("provider: {0}")]
    Provider(String),
    #[error("membership: {0}")]
    Membership(String),
    #[error("Store write policy is {actual:?}, but the application requires {expected:?}")]
    WritePolicyMismatch {
        expected: crate::WritePolicy,
        actual: crate::WritePolicy,
    },
    #[error("{provider:?} cannot coordinate a Serial Store with this configuration")]
    SerialCoordinationUnavailable { provider: crate::CloudProvider },
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
    #[error("bootstrap failed and its Store device retirement could not be published: {rollback} (bootstrap error: {cause})")]
    RegistrationRollback {
        rollback: String,
        cause: Box<BootstrapError>,
    },
}

impl BootstrapError {
    fn preserves_registration_rollback(&self) -> bool {
        matches!(self, Self::RegistrationRollback { .. })
    }
}

/// Undo everything a bootstrap attempt may have durably written (see
/// [`remove_bootstrap_residue`]), then return the error to propagate. The
/// completion-marker guard at the top of every join/restore entry point
/// establishes that this invocation owns everything under the store id — no
/// completed store existed when it started — which makes total removal
/// unconditionally safe here. On a clean run the original `cause` is returned
/// unchanged; if any removal step fails, every failure is carried in a
/// [`BootstrapError::Cleanup`] so none is lost. Shared by join and restore.
pub(crate) fn cleanup_after_bootstrap_failure(
    store_dir: &StoreDir,
    store_keys: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    cause: BootstrapError,
) -> BootstrapError {
    if cause.preserves_registration_rollback() {
        return cause;
    }
    let failures = remove_bootstrap_residue(store_dir, store_keys, custody, identity_custody);
    if failures.is_empty() {
        cause
    } else {
        BootstrapError::Cleanup {
            cleanup: failures.join("; "),
            cause: Box::new(cause),
        }
    }
}

/// Remove everything a bootstrap attempt may have durably written under this
/// store id, returning a message for each step that failed (empty ⇒ a clean
/// removal). Four steps, each best-effort so one failing doesn't skip the
/// others: the store directory (tolerating it never having existed, and also
/// covering a Passphrase custody's wrapped file, which lives inside it), the
/// master key via custody (idempotent regardless of policy), this store's
/// identity via its own custody (idempotent the same way — a no-op if the
/// identity was never established), and the cloud-home credentials (OAuth tokens
/// are stored *as* credentials — see `StoreKeys::set_cloud_home_oauth_tokens` —
/// so this one delete covers both). Shared by the post-failure cleanup and the
/// torn-bootstrap guard: both own everything under the store id, because the
/// completion-marker guard establishes no completed store exists at that id.
fn remove_bootstrap_residue(
    store_dir: &StoreDir,
    store_keys: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
) -> Vec<String> {
    let mut failures: Vec<String> = Vec::new();

    match std::fs::remove_dir_all(&**store_dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => failures.push(format!("store directory: {e}")),
    }

    if let Err(e) = custody.forget() {
        failures.push(format!("master key: {e}"));
    }

    if let Err(e) = identity_custody.forget() {
        failures.push(format!("identity: {e}"));
    }

    if let Err(e) = store_keys.delete_cloud_home_credentials() {
        failures.push(format!("cloud home credentials: {e}"));
    }

    failures
}

/// Dispatch a join or restore on the completion marker rather than bare
/// directory existence.
///
/// A store is *complete* once its `config.yaml` — the last durable write of a
/// successful bootstrap — exists. If it does, the directory holds real data:
/// refuse with [`BootstrapError::StoreExists`], leaving it untouched. A store
/// directory *without* that config is the residue of a bootstrap a hard crash
/// (power loss, kill) interrupted before the config save. The host records a
/// store only on `Ok`, so nothing references that residue and it is this
/// retry's to clear: remove the directory and the same store-scoped keyring
/// entries a failed bootstrap's cleanup removes, then let the caller proceed
/// with a fresh attempt. A residue-removal failure blocks the retry (the
/// leftovers would collide), so it surfaces as
/// [`BootstrapError::TornBootstrapCleanup`] rather than a silent proceed.
///
/// Cloud protocol state is append-only. A retry reuses the pending join identity
/// or the identity carried by the restore code, so it verifies and continues the
/// same signed registration chain instead of deleting published objects.
pub(crate) fn refuse_completed_or_clear_torn_store(
    store_dir: &StoreDir,
    store_keys: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    store_id: &str,
) -> Result<(), BootstrapError> {
    if store_dir.config_path().exists() {
        return Err(BootstrapError::StoreExists(store_id.to_string()));
    }

    if store_dir.exists() {
        warn!(
            store_dir = %store_dir.display(),
            "clearing a torn bootstrap: a store directory with no saved config, left by a join or restore a crash interrupted before completion"
        );
        // The directory and keyring entries are this retry's to clear. Immutable
        // Store objects remain in cloud storage and are verified on reuse.
        let failures = remove_bootstrap_residue(store_dir, store_keys, custody, identity_custody);
        if !failures.is_empty() {
            return Err(BootstrapError::TornBootstrapCleanup {
                store_id: store_id.to_string(),
                failures: failures.join("; "),
            });
        }
    }

    Ok(())
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

/// Both variants carry the store's signing identity, and
/// `bootstrap_and_save_store` establishes it in the store's own custody as the
/// last step before the config marker — so a saved config always implies a
/// resolvable identity. What differs is where the keypair comes from and what
/// happens to its source afterward: restore's comes from the restore code the
/// user still holds (re-suppliable, nothing to clean up), join's comes from the
/// pending slot its request minted — still consumable by a retry after a torn
/// bootstrap's wipe, and discarded only once the whole join succeeds (see
/// [`join_store`]).
pub(crate) enum BootstrapContext<'a> {
    /// Join pins the store owner from the invite.
    Join {
        owner_pubkey: &'a str,
        keypair: &'a UserKeypair,
    },
    Restore {
        founder_pubkey: &'a str,
        keypair: &'a UserKeypair,
    },
}

impl BootstrapContext<'_> {
    fn owner_pubkey(&self) -> &str {
        match self {
            BootstrapContext::Join { owner_pubkey, .. }
            | BootstrapContext::Restore {
                founder_pubkey: owner_pubkey,
                ..
            } => owner_pubkey,
        }
    }

    fn keypair(&self) -> &UserKeypair {
        match self {
            BootstrapContext::Join { keypair, .. } | BootstrapContext::Restore { keypair, .. } => {
                keypair
            }
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
struct JoinCloudHome {
    home: Arc<dyn CloudHome>,
    coordination: Option<CoordinationClients>,
}

async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    lib_ks: &StoreKeys,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
) -> Result<JoinCloudHome, BootstrapError> {
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
            let (s3, peer) = s3::S3CloudHome::new_pair(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                access_key.clone(),
                secret_key.clone(),
                key_prefix.clone(),
            )
            .await?;
            let s3 = Arc::new(s3);
            Ok(JoinCloudHome {
                home: s3.clone(),
                coordination: Some((s3, Arc::new(peer))),
            })
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            let tokens = require_join_oauth(oauth_tokens, "Google Drive")?;
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(JoinCloudHome {
                home: Arc::new(google_drive::GoogleDriveCloudHome::new(
                    folder_id.clone(),
                    tokens,
                    lib_ks.clone(),
                    clock,
                )?),
                coordination: None,
            })
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            let tokens = require_join_oauth(oauth_tokens, "Dropbox")?;
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(JoinCloudHome {
                home: Arc::new(dropbox::DropboxCloudHome::new(
                    folder_path.clone(),
                    tokens,
                    lib_ks.clone(),
                    clock,
                )?),
                coordination: None,
            })
        }

        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            let tokens = require_join_oauth(oauth_tokens, "OneDrive")?;
            persist_oauth_tokens(lib_ks, &tokens)?;
            Ok(JoinCloudHome {
                home: Arc::new(onedrive::OneDriveCloudHome::new(
                    drive_id.clone(),
                    folder_id.clone(),
                    tokens,
                    lib_ks.clone(),
                    clock,
                )?),
                coordination: None,
            })
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
            let home = Arc::new(cloudkit::CloudKitCloudHome::new_private(ops.clone()));
            Ok(JoinCloudHome {
                home: home.clone(),
                coordination: Some((
                    home,
                    Arc::new(cloudkit::CloudKitCloudHome::new_private(ops)),
                )),
            })
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
            let home = Arc::new(cloudkit::CloudKitCloudHome::new_shared(
                ops.clone(),
                owner_name.clone(),
                zone_name.clone(),
            ));
            Ok(JoinCloudHome {
                home: home.clone(),
                coordination: Some((
                    home,
                    Arc::new(cloudkit::CloudKitCloudHome::new_shared(
                        ops,
                        owner_name.clone(),
                        zone_name.clone(),
                    )),
                )),
            })
        }
    }
}

/// Join a shared store using an invite code string.
///
/// `join_request_code` is the string this device's own [`crate::generate_join_request`]
/// returned when it asked to join — decoded here to recover the public key that
/// names the pending identity minted for that request, which a completed join
/// promotes into this store's own identity custody (see
/// [`crate::keys::mint_pending_identity`]/[`crate::keys::promote_pending_identity`]).
///
/// Handles everything: decode invite, promote the pending identity, build cloud
/// home (using caller-provided OAuth tokens for the providers that need them),
/// run the join protocol, and set as active store.
#[allow(clippy::too_many_arguments)]
pub async fn join_from_invite_code(
    invite_code_str: &str,
    join_request_code: &str,
    layout: &StoreLayout,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    expected_write_policy: crate::WritePolicy,
    custom_s3_serial: Option<crate::CustomS3Serial>,
    key_custody: crate::custody::KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    ids: crate::id_provider::IdRef,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    let code = crate::join_code::decode(invite_code_str)
        .map_err(|e| BootstrapError::InvalidCode(e.to_string()))?;
    let actual_write_policy = code.membership_floor.write_policy();
    if actual_write_policy != expected_write_policy {
        return Err(BootstrapError::WritePolicyMismatch {
            expected: expected_write_policy,
            actual: actual_write_policy,
        });
    }
    crate::storage::cloud::setup::require_serial_coordination_join_info(
        &code.join_info,
        custom_s3_serial,
        actual_write_policy,
    )
    .map_err(|provider| BootstrapError::SerialCoordinationUnavailable { provider })?;
    let joiner_public_key = crate::join_code::decode_join_request(join_request_code)
        .map_err(|e| BootstrapError::InvalidCode(e.to_string()))?
        .public_key;

    let store_dir = layout.store_dir(&code.store_id);

    // Hoisted here, before any durable write below, so a failure at any step —
    // including `build_cloud_home_for_join`'s OAuth persist, which runs before
    // `join_store` even creates the store directory — funnels through the same
    // rollback instead of escaping via `?`. Resolving custody has no side
    // effect, so it is safe ahead of the completion-marker guard, which itself
    // may remove torn-bootstrap keyring residue through these handles.
    let store_keys = StoreKeys::new(code.store_id.clone());
    let custody = key_custody.resolve(&code.store_id, &store_dir);
    let identity_custody = identity_custody.resolve(&code.store_id, &store_dir);

    // Refuse a *completed* store (config present) and clear a torn one before
    // any keyring or provider side effect. Re-joining a store you already have
    // adds nothing — the existing store is the data. `join_store` below is the
    // authoritative guard against the destructive failure-cleanup; this check
    // exists so a refused join also leaves no residue from
    // `build_cloud_home_for_join`, which runs first and saves OAuth tokens to
    // the keyring and can accept a CloudKit share.
    refuse_completed_or_clear_torn_store(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &code.store_id,
    )?;

    let result = async {
        let cloud = build_cloud_home_for_join(
            &code.join_info,
            &store_keys,
            oauth_tokens,
            cloudkit_ops,
            clock.clone(),
        )
        .await?;

        join_store_with_coordination(
            layout,
            code,
            &joiner_public_key,
            synced_tables,
            migrations,
            custody.clone(),
            identity_custody.clone(),
            cloud.home,
            cloud.coordination,
            custom_s3_serial,
            clock,
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
            identity_custody.as_ref(),
            err,
        )),
    }
}

/// Join an existing shared store using a decoded invite code.
///
/// Lower-level function — caller provides pre-built `CloudHome`.
/// Prefer `join_from_invite_code` for the full flow. `joiner_public_key_hex`
/// is the pending identity's public key (see [`join_from_invite_code`]'s
/// doc) — the store's own identity once a completed join promotes it.
///
/// `on_status` is called with progress messages for UI feedback.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn join_store(
    layout: &StoreLayout,
    code: InviteCode,
    joiner_public_key_hex: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    cloud_home: Box<dyn CloudHome>,
    ids: &dyn crate::id_provider::IdProvider,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    join_store_with_coordination(
        layout,
        code,
        joiner_public_key_hex,
        synced_tables,
        migrations,
        custody,
        identity_custody,
        Arc::from(cloud_home),
        None,
        None,
        Arc::new(crate::clock::SystemClock),
        ids,
        on_status,
        cancel,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join_store_with_coordination(
    layout: &StoreLayout,
    code: InviteCode,
    joiner_public_key_hex: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    cloud_home: Arc<dyn CloudHome>,
    coordination: Option<CoordinationClients>,
    custom_s3_serial: Option<crate::CustomS3Serial>,
    clock: crate::clock::ClockRef,
    ids: &dyn crate::id_provider::IdProvider,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError> {
    // Guard the destructive `stores/<id>` create/delete against any direct
    // caller, independent of the decode-time check on untrusted input.
    crate::store_dir::validate_path_token(&code.store_id)
        .map_err(|e| BootstrapError::InvalidCode(format!("invalid store id: {e}")))?;

    let store_dir = layout.store_dir(&code.store_id);

    // Hoisted here, before any durable write below, so every step through the
    // end of this function — keypair load, invitation accept, directory
    // creation, bootstrap — funnels a failure through the same rollback
    // instead of a bare `?` escaping it.
    let store_keys = StoreKeys::new(code.store_id.clone());

    // Refuse a *completed* store (config present) and clear a torn one,
    // independent of whatever `join_from_invite_code` (or any other caller)
    // already checked. Re-joining a store you already have adds nothing — the
    // existing store is the data — and letting the join proceed would, on any
    // bootstrap failure below, delete that store's database and blobs during
    // cleanup. This is the authoritative guard: it makes the failure-cleanup
    // below only ever remove a directory this invocation created.
    refuse_completed_or_clear_torn_store(
        &store_dir,
        &store_keys,
        custody.as_ref(),
        identity_custody.as_ref(),
        &code.store_id,
    )?;

    let result = async {
        // Load the pending identity this join's request minted (the inviter
        // wrapped the store key for this public key, so join never mints one
        // of its own). Read without consuming: the pending slot must survive a
        // torn bootstrap's wipe so a retry can re-establish the identity, and
        // is discarded only once the whole join succeeds, below.
        on_status("Loading keypair...");
        let user_keypair = crate::keys::peek_pending_identity(joiner_public_key_hex)?;

        // Accept the invitation to get the store encryption key. The joiner
        // authenticates it against the current Owner set derived from the
        // membership chain anchored to the owner the invite pins (the chain
        // founder), so any current Owner's invite is joinable yet a bucket
        // writer still can't substitute a key. The chain is sealed under the
        // store key, so `Arc` the home once and reuse it for the sync storage
        // below.
        on_status("Accepting invitation...");
        let encryption = match &code.membership_floor {
            MembershipFloor::MergeConcurrent(floor) => {
                unwrap_store_keyring(
                    cloud_home.clone(),
                    &user_keypair,
                    code.store_root_hash,
                    &code.store_id,
                    &code.owner_pubkey,
                    floor,
                )
                .await?
            }
            MembershipFloor::Serial(Some(position)) => {
                if coordination.is_none() {
                    return Err(BootstrapError::Membership(
                        "Serial invite provider has no coordination capability".to_string(),
                    ));
                }
                unwrap_serial_store_keyring(
                    cloud_home.clone(),
                    &user_keypair,
                    &code.store_id,
                    &code.key_author_pubkey,
                    position,
                )
                .await?
            }
            MembershipFloor::Serial(None) => {
                return Err(BootstrapError::Membership(
                    "Serial invite has the founder-only restore floor".to_string(),
                ));
            }
        };
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
        )
        .with_copy_ids(std::sync::Arc::new(
            crate::storage::cloud::RandomCopyIdGenerator,
        ));
        let storage = match coordination {
            Some((primary, peer)) => storage.with_serial_coordination_clients(primary, peer),
            None => storage,
        };

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
            code.store_root_hash,
            BootstrapContext::Join {
                owner_pubkey: &code.owner_pubkey,
                keypair: &user_keypair,
            },
            &code.membership_floor,
            synced_tables,
            migrations,
            &code.join_info,
            &code.store_name,
            custom_s3_serial,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            clock.as_ref(),
            &on_status,
            cancel,
        )
        .await
    }
    .await;

    match result {
        Ok(config) => {
            // The store's identity was established in its own custody before
            // the config marker (see `bootstrap_and_save_store`), so the join
            // is complete — the pending slot is now a consumed source. Discard
            // it best-effort: a failed delete leaves a harmless leftover
            // pending entry (the established identity is what every reader
            // resolves), never a store whose identity is missing.
            if let Err(e) = crate::keys::discard_pending_identity(joiner_public_key_hex) {
                warn!(
                    "failed to discard the consumed pending identity {joiner_public_key_hex}: {e}"
                );
            }
            info!("Joined store {} at {}", code.store_id, store_dir.display());
            Ok(config)
        }
        Err(err) => Err(cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
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
    store_root_hash: crate::sync::store_commit::ObjectHash,
    context: BootstrapContext<'_>,
    membership_floor: &MembershipFloor,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    join_info: &CloudHomeJoinInfo,
    store_name: &str,
    custom_s3_serial: Option<crate::CustomS3Serial>,
    key_service: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
    clock: &dyn crate::clock::Clock,
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
        store_root_hash,
        owner_pubkey,
        membership_floor,
        binary_schema_version,
        &db_path,
    )
    .await?;

    info!(
        "Bootstrapped from snapshot ({} device coverage entries)",
        bootstrap_result.coverage_count()
    );

    let published_at = clock.now().to_rfc3339();
    let coordination =
        if bootstrap_result.write_policy() == crate::WritePolicy::Serial {
            Some(storage.serial_coordination().map_err(|error| {
                BootstrapError::Membership(format!("Serial coordination: {error}"))
            })?)
        } else {
            None
        };

    let committed = async {
        // Pull Store commits beyond the snapshot coverage.
        error_if_cancelled(cancel)?;
        on_status("Applying recent changes...");
        let changesets_applied = open_db_and_pull(
            store_id,
            &db_path,
            synced_tables,
            migrations,
            device_id,
            owner_pubkey,
            Some(context.keypair()),
            membership_floor,
            storage,
            coordination,
            Some(&published_at),
            bootstrap_result,
            store_dir,
            cancel,
        )
        .await?;

        if changesets_applied > 0 {
            info!("Applied {changesets_applied} changesets since snapshot");
        }

        // Persist the master key via custody.
        on_status("Saving configuration...");
        if let Some(keyring) = master_key {
            custody.persist(keyring)?;
        }

        // Save cloud credentials to keyring.
        if let Some(credentials) = derive_credentials(join_info) {
            key_service.set_cloud_home_credentials(&credentials)?;
        }

        // Establish the store's signing identity BEFORE the config save, so
        // the saved config — the completion marker — always implies a
        // resolvable identity. A crash before the save is a torn bootstrap the
        // retry clears and re-establishes: from the restore code the user
        // still holds, or from join's pending slot, which the torn-bootstrap
        // wipe leaves in place (it is keyed by the request, not the store) and
        // which is discarded only once the whole join succeeds (see
        // `join_store`). Inside the unit, so a later failure deletes the
        // bootstrap reader the same as any other.
        crate::keys::import_identity(identity_custody, &context.keypair().to_keypair_bytes())?;

        // Create and save config — the last durable write and the
        // store's completion marker.
        let config = build_config(
            store_id,
            device_id,
            store_dir,
            store_name,
            join_info,
            cipher,
            custom_s3_serial,
        );
        config.save_to_config_yaml()?;
        Ok(config)
    }
    .await;

    match committed {
        Ok(config) => Ok(config),
        Err(cause) => {
            match finish_failed_bootstrap_registration_rollback(
                &db_path,
                synced_tables,
                migrations,
                membership_floor.write_policy(),
                device_id,
                storage,
                coordination,
                context.keypair(),
                &published_at,
            )
            .await
            {
                Ok(()) => Err(cause),
                Err(rollback) => Err(BootstrapError::RegistrationRollback {
                    rollback,
                    cause: Box::new(cause),
                }),
            }
        }
    }
}

pub(crate) async fn finish_failed_bootstrap_registration_rollback(
    db_path: &Path,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    write_policy: crate::WritePolicy,
    device_id: &str,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn crate::sync::storage::CoordinationStorage>,
    signer: &UserKeypair,
    published_at: &str,
) -> Result<(), String> {
    if !db_path.exists() {
        return Ok(());
    }
    let (db, _stamper) = Database::open(
        db_path,
        synced_tables.to_vec(),
        crate::blob::delete::BLOB_TOMBSTONE_GRACE,
        crate::blob::TransferLimits::serial(),
        write_policy,
        device_id.to_string(),
        migrations,
    )
    .map_err(|error| format!("open durable registration rollback outbox: {error}"))?;
    let membership = match write_policy {
        crate::WritePolicy::MergeConcurrent => Some(
            crate::sync::pull::load_cycle_membership(storage, &db)
                .await
                .map_err(|error| format!("load registration rollback membership: {error}"))?,
        ),
        crate::WritePolicy::Serial => None,
    };
    crate::sync::store_registration::retire_registration_with_coordination(
        &db,
        storage,
        coordination,
        signer,
        membership.as_ref().and_then(|loaded| loaded.chain.as_ref()),
        published_at,
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

/// Open a [`crate::database::Database`] over the bootstrapped db file and pull changesets since
/// the snapshot. The snapshot already carries the full schema and the writer's
/// `user_version`, so the migration ladder only carries the image forward when
/// this binary is newer (a no-op when they match).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn open_db_and_pull(
    store_id: &str,
    db_path: &Path,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    device_id: &str,
    owner_pubkey: &str,
    registration_signer: Option<&UserKeypair>,
    membership_floor: &MembershipFloor,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn crate::sync::storage::CoordinationStorage>,
    registration_published_at: Option<&str>,
    bootstrap: BootstrapResult,
    store_dir: &StoreDir,
    cancel: &watch::Receiver<bool>,
) -> Result<u64, BootstrapError> {
    let db = bootstrap
        .open_database(
            store_id,
            db_path,
            synced_tables.to_vec(),
            // This bootstrap database only applies changesets during join; it never runs
            // the tombstone GC, so the grace is immaterial and takes the default.
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            device_id.to_string(),
            migrations,
        )
        .await?;

    // Seed this device's per-author membership-head watermark from the invite
    // or restore code's floor BEFORE the pull below loads and anchors the
    // membership chain for the first time. A fresh joiner or restorer has no
    // watermark yet, so without this, the first load would accept any signed
    // head — including an older one a storage provider chooses to serve, e.g.
    // from before a removal the floor already reflects. Seeding it here makes
    // that first load monotonic from the start, exactly like every later cycle.
    if let MembershipFloor::MergeConcurrent(floor) = membership_floor {
        crate::sync::membership_ops::seed_head_watermark(&db, floor)
            .await
            .map_err(|e| {
                BootstrapError::Database(format!("Failed to seed membership head watermark: {e}"))
            })?;
    }

    db.set_protocol_state(
        crate::sync::membership_ops::OWNER_PUBKEY_STATE_KEY,
        owner_pubkey,
    )
    .await
    .map_err(|e| BootstrapError::Database(format!("Failed to pin store owner: {e}")))?;

    // Download the blob files the snapshot's rows reference: the snapshot carried
    // the catalog rows but no blob files, and the pull starts past its signed
    // coverage, so it never re-walks the INSERTs that first carried them. Missing
    // eager blobs abort the bootstrap before the store is saved. Cancellation is
    // checked between blobs inside the reconcile, surfacing as `Cancelled` here.
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
    // truth. Load and anchor the membership chain first (join is a standalone,
    // non-cycle pull), against the owner pinned above. Restore has not pinned an
    // owner yet, so it anchors the chain at its signed founder below.
    let membership = match membership_floor {
        MembershipFloor::MergeConcurrent(_) => Some(
            crate::sync::pull::load_cycle_membership(storage, &db)
                .await
                .map_err(BootstrapError::Pull)?,
        ),
        MembershipFloor::Serial(_) => None,
    };
    let store_root_hash = db
        .get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await
        .map_err(|error| BootstrapError::Database(error.to_string()))?
        .ok_or_else(|| BootstrapError::Database("store protocol root hash is absent".to_string()))?
        .parse()
        .map_err(|error| BootstrapError::Database(format!("store protocol root hash: {error}")))?;
    if db.write_policy() == crate::WritePolicy::Serial && coordination.is_none() {
        return Err(BootstrapError::Membership(
            "Serial coordination capability is absent".to_string(),
        ));
    }
    let pull_result = crate::sync::store_pull::pull_store_commits_with_coordination(
        &db,
        db.synced_tables(),
        storage,
        coordination,
        store_root_hash,
        device_id,
        store_dir,
        membership.as_ref().and_then(|loaded| loaded.chain.as_ref()),
    )
    .await?;

    if let Some(signer) = registration_signer {
        let published_at = registration_published_at.ok_or_else(|| {
            BootstrapError::Database(
                "registration signer has no registration publication timestamp".to_string(),
            )
        })?;
        let signer_pubkey = crate::keys::public_key_hex(signer);
        let authorized = match membership_floor {
            MembershipFloor::MergeConcurrent(_) => membership
                .as_ref()
                .and_then(|loaded| loaded.chain.as_ref())
                .is_some_and(|chain| {
                    chain
                        .current_members()
                        .iter()
                        .any(|(pubkey, _)| pubkey == &signer_pubkey)
                }),
            MembershipFloor::Serial(Some(floor)) => {
                let exact = crate::sync::store_objects::load_serial_commit_at_position(
                    storage,
                    store_root_hash,
                    floor,
                )
                .await
                .map_err(|error| BootstrapError::Membership(error.to_string()))?
                .is_some();
                exact
                    && db
                        .serial_membership_state()
                        .await
                        .map_err(|error| BootstrapError::Database(error.to_string()))?
                        .is_some_and(|state| {
                            state
                                .current_members()
                                .iter()
                                .any(|(pubkey, _)| pubkey == &signer_pubkey)
                        })
            }
            MembershipFloor::Serial(None) => false,
        };
        if !authorized {
            return Err(BootstrapError::Membership(format!(
                "Store device registration author {signer_pubkey} is not an active member at the invite floor"
            )));
        }
        crate::sync::store_registration::ensure_active_registration_with_coordination(
            &db,
            storage,
            coordination,
            signer,
            membership.as_ref().and_then(|loaded| loaded.chain.as_ref()),
            published_at,
        )
        .await?;
    }

    // Bootstrap installed the snapshot's position vector before pulling anything
    // above it, and each higher position committed with its rows. Record the remaining
    // bootstrap marker so the first real cycle treats this device as a joiner,
    // not a brand-new store that should publish an initial snapshot.
    db.set_protocol_state("snapshot_seq", "0")
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
    custom_s3_serial: Option<crate::CustomS3Serial>,
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
            config.cloud_home.s3_serial = custom_s3_serial;
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

    /// A store whose `config.yaml` marker is present is a completed store: the
    /// guard refuses it with `StoreExists` and touches nothing — neither the
    /// directory nor the keyring entries a live store depends on.
    #[test]
    fn guard_refuses_a_completed_store_and_leaves_it_untouched() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path().join("completed"));
        std::fs::create_dir_all(&*store_dir).expect("create store dir");
        std::fs::write(store_dir.config_path(), b"store_id: completed\n")
            .expect("seed completion marker");
        let store_keys = StoreKeys::new("guard-completed-test".to_string());
        let custody =
            crate::custody::KeyCustody::Keyring.resolve("guard-completed-test", &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key");
        let identity_custody = IdentityCustody::Keyring.resolve("guard-completed-test", &store_dir);

        let result = refuse_completed_or_clear_torn_store(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            "guard-completed-test",
        );

        assert!(
            matches!(result, Err(BootstrapError::StoreExists(ref id)) if id == "guard-completed-test"),
            "a completed store must be refused with StoreExists, got {result:?}",
        );
        assert!(store_dir.config_path().exists(), "the marker is untouched");
        assert!(
            custody.unlock().expect("read master key").is_some(),
            "a refused completed store keeps its keyring entries",
        );
    }

    /// A store directory with no `config.yaml` marker is a torn bootstrap a
    /// crash interrupted before completion: the guard clears it — the directory
    /// and the store-scoped keyring entries — and returns `Ok` so the caller
    /// retries from a clean slate.
    #[test]
    fn guard_clears_a_torn_bootstrap_and_lets_the_retry_proceed() {
        crate::keys::test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path().join("torn"));
        std::fs::create_dir_all(&*store_dir).expect("create store dir");
        // Partial bootstrap residue: a torn database image, no config marker.
        std::fs::write(store_dir.db_path(), b"half-written-db").expect("seed torn db");
        let store_keys = StoreKeys::new("guard-torn-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve("guard-torn-test", &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key");
        let identity_custody = IdentityCustody::Keyring.resolve("guard-torn-test", &store_dir);
        identity_custody
            .persist(&UserKeypair::generate())
            .expect("seed the identity");
        store_keys
            .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
            })
            .expect("seed cloud home credentials");

        let result = refuse_completed_or_clear_torn_store(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            "guard-torn-test",
        );

        assert!(
            result.is_ok(),
            "a torn bootstrap clears and proceeds, got {result:?}"
        );
        assert!(!store_dir.exists(), "the torn directory was removed");
        assert!(
            custody.unlock().expect("read master key").is_none(),
            "the torn store's master key was cleared",
        );
        assert!(
            identity_custody.unlock().expect("read identity").is_none(),
            "the torn store's identity was cleared",
        );
        assert!(
            store_keys
                .get_cloud_home_credentials()
                .expect("read keyring")
                .is_none(),
            "the torn store's cloud home credentials were cleared",
        );
    }

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
        let identity_custody =
            IdentityCustody::Keyring.resolve("cleanup-failure-cause-test", &blocked);

        let wrapped = cleanup_after_bootstrap_failure(
            &blocked,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
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
        let identity_custody =
            IdentityCustody::Keyring.resolve("successful-cleanup-test", &store_dir);

        let returned = cleanup_after_bootstrap_failure(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
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
        let identity_custody =
            IdentityCustody::Keyring.resolve("never-created-dir-test", &never_created);

        let returned = cleanup_after_bootstrap_failure(
            &never_created,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
            BootstrapError::Database("bootstrap boom".to_string()),
        );

        assert!(
            matches!(returned, BootstrapError::Database(ref m) if m == "bootstrap boom"),
            "a missing store dir must not itself count as a cleanup failure, got {returned:?}",
        );
    }

    /// The extended rollback also removes the store-scoped keyring accounts —
    /// the encryption master key, this store's identity, and the cloud-home
    /// credentials (which is also where an OAuth token lands, via
    /// `set_cloud_home_oauth_tokens`) — not just the directory. Seed all three
    /// the way a partial bootstrap would have written them, then assert
    /// cleanup leaves none behind.
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
        let identity_custody = IdentityCustody::Keyring.resolve("keyring-cleanup-test", &store_dir);
        identity_custody
            .persist(&crate::keys::UserKeypair::generate())
            .expect("seed this store's identity via custody");
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
            identity_custody.as_ref(),
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
            identity_custody
                .unlock()
                .expect("read identity custody")
                .is_none(),
            "this store's identity must be removed from custody",
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
