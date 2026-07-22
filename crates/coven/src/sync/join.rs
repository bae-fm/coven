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
    Invite(#[source] Box<InviteError>),
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
    #[error("pull: {0}")]
    Pull(#[from] PullError),
    #[error("Store pull: {0}")]
    StorePull(#[from] crate::sync::store_pull::StorePullError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] crate::sync::store_registration::StoreRegistrationError),
    #[error("Store device join: {0}")]
    DeviceJoin(#[from] crate::DeviceJoinError),
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
    #[error("could not remove cancelled join state for {store_id}: {failures}")]
    CancelledJoinCleanup { store_id: String, failures: String },
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
    #[error("{provider:?} cannot provide exact protocol and blob slots with this configuration")]
    ExactSlotsUnavailable { provider: crate::CloudProvider },
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

impl From<InviteError> for BootstrapError {
    fn from(error: InviteError) -> Self {
        Self::Invite(Box::new(error))
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

/// The identity and verified authority carried by a restore code. Bootstrap
/// installs the identity in store custody before writing the config marker.
pub(crate) struct RestoreBootstrapContext<'a> {
    pub founder_pubkey: &'a str,
    pub keypair: &'a UserKeypair,
    pub authority: &'a crate::sync::restore_code::RestoreAuthority,
    pub continuation: Option<(
        &'a crate::sync::restore_code::ActivatedContinuation,
        &'a UserKeypair,
    )>,
}

impl RestoreBootstrapContext<'_> {
    fn owner_pubkey(&self) -> &str {
        self.founder_pubkey
    }

    fn keypair(&self) -> &UserKeypair {
        self.keypair
    }

    fn activated_continuation(
        &self,
    ) -> Option<(
        &crate::sync::restore_code::ActivatedContinuation,
        &UserKeypair,
        &UserKeypair,
    )> {
        self.continuation
            .map(|(continuation, device)| (continuation, self.keypair, device))
    }

    fn owner_recovery(
        &self,
    ) -> Option<(
        &crate::sync::restore_code::OwnerRecoveryAuthority,
        &UserKeypair,
    )> {
        match self.authority {
            crate::sync::restore_code::RestoreAuthority::OwnerRecovery(authority) => {
                Some((authority, self.keypair))
            }
            crate::sync::restore_code::RestoreAuthority::ActivatedContinuation(_) => None,
        }
    }
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

#[cfg(feature = "oauth-providers")]
fn require_join_oauth(
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    provider_name: &str,
) -> Result<crate::oauth::OAuthTokens, BootstrapError> {
    oauth_tokens.ok_or_else(|| {
        BootstrapError::Provider(format!("{provider_name} join requires an OAuth token"))
    })
}

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
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
) -> Result<JoinCloudHome, BootstrapError> {
    use crate::storage::cloud::*;

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
                custom_s3_exact_slots,
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

/// A joining device's local half of the four-transfer admission exchange.
/// The journal lives outside the incomplete store directory, so every method
/// can be retried after process termination without losing its exact predecessor.
pub struct DeviceJoinClient {
    code: InviteCode,
    member_pubkey: String,
    layout: StoreLayout,
    synced_tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
    custom_s3_serial: Option<crate::CustomS3Serial>,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    #[cfg(test)]
    test_home: Option<Arc<dyn CloudHome>>,
}

struct DeviceJoinStorage {
    storage: CloudSyncStorage,
    exact: Arc<dyn crate::storage::cloud::ExactSlotStorage>,
    keyring: MasterKeyring,
}

impl DeviceJoinClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        invite_code: &str,
        join_request_code: &str,
        layout: StoreLayout,
        synced_tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
        expected_write_policy: crate::WritePolicy,
        custom_s3_serial: Option<crate::CustomS3Serial>,
        custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
        key_custody: crate::custody::KeyCustody,
        identity_custody: IdentityCustody,
        oauth_tokens: Option<crate::oauth::OAuthTokens>,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        clock: crate::clock::ClockRef,
    ) -> Result<Self, BootstrapError> {
        let code = crate::join_code::decode(invite_code)
            .map_err(|error| BootstrapError::InvalidCode(error.to_string()))?;
        let member_pubkey = crate::join_code::decode_join_request(join_request_code)
            .map_err(|error| BootstrapError::InvalidCode(error.to_string()))?
            .public_key;
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
        crate::storage::cloud::setup::require_exact_slot_capabilities_join_info(
            &code.join_info,
            custom_s3_exact_slots,
        )
        .map_err(|provider| BootstrapError::ExactSlotsUnavailable { provider })?;
        crate::store_dir::validate_path_token(&code.store_id)
            .map_err(|error| BootstrapError::InvalidCode(format!("invalid store id: {error}")))?;
        let store_dir = layout.store_dir(&code.store_id);
        let custody = key_custody.resolve(&code.store_id, &store_dir);
        let identity_custody = identity_custody.resolve(&code.store_id, &store_dir);
        Ok(Self {
            code,
            member_pubkey,
            layout,
            synced_tables,
            migrations,
            custom_s3_serial,
            custom_s3_exact_slots,
            custody,
            identity_custody,
            oauth_tokens,
            cloudkit_ops,
            clock,
            #[cfg(test)]
            test_home: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_test_bootstrap_home(mut self, home: Arc<dyn CloudHome>) -> Self {
        self.test_home = Some(home);
        self
    }

    pub async fn prepare_provider_access_request(
        &self,
        offer: crate::DeviceJoinOffer,
    ) -> Result<crate::DeviceProviderAccessRequest, BootstrapError> {
        self.require_offer(&offer)?;
        let signer = crate::keys::peek_pending_identity(&offer.member_pubkey)?;
        let cloud = self.build_cloud_home().await?;
        let exact = cloud.home.clone().exact_slot_storage().ok_or_else(|| {
            BootstrapError::Provider("provider has no exact-slot adapter".to_string())
        })?;
        let binding = exact
            .provider_binding()
            .await
            .map_err(BootstrapError::CloudHome)?;
        let pending = self.open_pending_journal()?;
        Ok(
            crate::sync::device_join::prepare_device_provider_access_request(
                &pending, binding, &signer, offer,
            )
            .await?,
        )
    }

    pub async fn accept_device_join_abandonment(
        &self,
        abandonment: crate::DeviceJoinAbandonment,
    ) -> Result<crate::DeviceJoinAbandonment, BootstrapError> {
        let signer = crate::keys::peek_pending_identity(&self.member_pubkey)?;
        let join = self.build_storage(&signer).await?;
        let pending = self.open_pending_journal()?;
        Ok(crate::sync::device_join::observe_device_join_abandonment(
            &pending,
            &join.storage,
            &self.code.store_root,
            abandonment,
        )
        .await?)
    }

    pub fn device_join_status(
        &self,
        attempt_id: crate::DeviceJoinAttemptId,
    ) -> Result<Option<crate::DeviceJoinStatus>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(crate::sync::device_join::load_pending_device_join_status(
            &pending, attempt_id,
        )?)
    }

    pub fn resume_device_joins(&self) -> Result<Vec<crate::DeviceJoinAction>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(crate::sync::device_join::load_pending_device_join_actions(
            &pending,
        )?)
    }

    pub async fn close_pending_device_join(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::JoinerJoinTerminal, BootstrapError> {
        let signer = crate::keys::peek_pending_identity(&self.member_pubkey)?;
        let join = self.build_storage(&signer).await?;
        let pending = self.open_pending_journal()?;
        Ok(crate::sync::device_join::close_joining_device(
            &pending,
            &join.storage,
            join.exact.as_ref(),
            &self.code.store_root,
            &signer,
            cancellation,
        )
        .await?)
    }

    pub async fn complete_cancelled_device_join(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), BootstrapError> {
        let pending = self.open_pending_journal()?;
        match crate::sync::device_join::load_pending_device_join_status(
            &pending,
            activation.receipt.attempt_id,
        )? {
            Some(crate::DeviceJoinStatus::CleanupActivated {
                activation: durable,
            }) if durable == activation => {}
            Some(crate::DeviceJoinStatus::CleanupActivated { .. }) => {
                return Err(crate::DeviceJoinError::JournalConflict.into());
            }
            _ => {
                let signer = crate::keys::peek_pending_identity(&self.member_pubkey)?;
                let join = self.build_storage(&signer).await?;
                crate::sync::device_join::accept_joiner_device_join_cleanup(
                    &pending,
                    &join.storage,
                    &self.code.store_root,
                    activation.clone(),
                )
                .await?;
            }
        }

        let store_dir = self.layout.store_dir(&self.code.store_id);
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.code.store_id.clone()));
        }
        let failures = remove_bootstrap_residue(
            &store_dir,
            &StoreKeys::new(self.code.store_id.clone()),
            self.custody.as_ref(),
            self.identity_custody.as_ref(),
        );
        if !failures.is_empty() {
            return Err(BootstrapError::CancelledJoinCleanup {
                store_id: self.code.store_id.clone(),
                failures: failures.join("; "),
            });
        }
        crate::keys::discard_pending_identity(&self.member_pubkey)?;
        crate::sync::device_join::complete_joiner_device_join_cleanup(&pending, activation)?;
        Ok(())
    }

    pub async fn prepare_registration_request(
        &self,
        approval: crate::DeviceProviderAdmissionApproval,
    ) -> Result<crate::DeviceRegistrationRequest, BootstrapError> {
        let offer = &approval.request.offer;
        self.require_offer(offer)?;
        let signer = crate::keys::peek_pending_identity(&offer.member_pubkey)?;
        let join = self.build_storage(&signer).await?;
        let pending = self.open_pending_journal()?;
        let coordination = match &self.code.membership_floor {
            MembershipFloor::MergeConcurrent(_) => None,
            MembershipFloor::Serial(_) => {
                Some(join.storage.serial_coordination().map_err(|error| {
                    BootstrapError::Membership(format!("Serial coordination: {error}"))
                })?)
            }
        };
        Ok(
            crate::sync::device_join::prepare_device_registration_request(
                &pending,
                &join.storage,
                coordination,
                Some(join.exact.as_ref()),
                &signer,
                approval,
            )
            .await?,
        )
    }

    pub async fn bootstrap_pending_device(
        &self,
        bootstrap: crate::ProviderReadyDeviceBootstrap,
        on_status: impl Fn(&str),
        cancel: &watch::Receiver<bool>,
    ) -> Result<crate::sync::device_join::DeviceJoinReadiness, BootstrapError> {
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        self.require_offer(offer)?;
        let attempt = &bootstrap.bootstrap.publication_authorization.attempt;
        let pending = self.open_pending_journal()?;
        if let Some(record) = pending.load(attempt.attempt_id, crate::DeviceJoinRole::Joiner)? {
            if let crate::sync::device_join::DeviceJoinRoleProgress::Joiner(
                crate::sync::device_join::JoinerJoinProgress::Ready(readiness),
            ) = &*record.progress
            {
                if readiness.proof.attempt != *attempt {
                    return Err(crate::DeviceJoinError::JournalConflict.into());
                }
                let store_dir = self.layout.store_dir(&self.code.store_id);
                if store_dir.db_path().exists() {
                    return Ok(readiness.clone());
                }
            }
        }
        error_if_cancelled(cancel)?;
        let signer = crate::keys::peek_pending_identity(&offer.member_pubkey)?;
        let join = self.build_storage(&signer).await?;
        let store_dir = self.layout.store_dir(&self.code.store_id);
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.code.store_id.clone()));
        }
        std::fs::create_dir_all(&*store_dir)?;
        on_status("Downloading store snapshot...");
        let db_path = store_dir.db_path();
        let coordination = match &self.code.membership_floor {
            MembershipFloor::MergeConcurrent(_) => None,
            MembershipFloor::Serial(_) => {
                Some(join.storage.serial_coordination().map_err(|error| {
                    BootstrapError::Membership(format!("Serial coordination: {error}"))
                })?)
            }
        };
        let snapshot = bootstrap_from_snapshot(
            &join.storage,
            coordination,
            &self.code.store_id,
            offer.store_root.clone(),
            &self.code.membership_floor,
            supported_version(&self.migrations),
            &db_path,
        )
        .await?;
        let device_id = bootstrap
            .bootstrap
            .request
            .expected_registration
            .device_id
            .to_string();
        let db = snapshot
            .open_database(
                &self.code.store_id,
                &db_path,
                self.synced_tables.clone(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::serial(),
                device_id,
                &self.migrations,
            )
            .await?;
        let published_at = self.clock.now().to_rfc3339();
        on_status("Installing device registration...");
        let readiness = crate::sync::device_join::bootstrap_pending_device(
            &db,
            &pending,
            &join.storage,
            Some(join.exact.as_ref()),
            &signer,
            bootstrap,
            &published_at,
        )
        .await?;
        match crate::sync::snapshot::reconcile_snapshot_blobs(
            &db,
            &db_path,
            &join.storage,
            &store_dir,
            db.synced_tables(),
            cancel,
        )
        .await
        .map_err(|error| BootstrapError::Database(error.to_string()))?
        {
            crate::sync::snapshot::SnapshotBlobReconcile::Complete => Ok(readiness),
            crate::sync::snapshot::SnapshotBlobReconcile::Incomplete => {
                Err(BootstrapError::Database(
                    "snapshot blob reconciliation did not land every required eager blob"
                        .to_string(),
                ))
            }
            crate::sync::snapshot::SnapshotBlobReconcile::Cancelled => {
                Err(BootstrapError::Cancelled)
            }
        }
    }

    pub async fn complete_device_join(
        &self,
        activation: crate::DeviceJoinActivation,
        on_status: impl Fn(&str),
    ) -> Result<Config, BootstrapError> {
        let attempt_id = activation.outcome.attempt().attempt_id;
        let pending = self.open_pending_journal()?;
        let pending_record = pending.load(attempt_id, crate::DeviceJoinRole::Joiner)?;
        let pending_readiness = match pending_record.as_ref().map(|record| &*record.progress) {
            Some(crate::sync::device_join::DeviceJoinRoleProgress::Joiner(
                crate::sync::device_join::JoinerJoinProgress::Ready(_),
            )) => Some(crate::sync::device_join::observe_device_join_activation(
                &pending,
                &activation,
            )?),
            Some(crate::sync::device_join::DeviceJoinRoleProgress::Joiner(
                crate::sync::device_join::JoinerJoinProgress::ActivationObserved { .. },
            )) => Some(crate::sync::device_join::observe_device_join_activation(
                &pending,
                &activation,
            )?),
            Some(_) => return Err(crate::DeviceJoinError::JournalConflict.into()),
            None => None,
        };
        let store_dir = self.layout.store_dir(&self.code.store_id);
        let completed_config = if store_dir.config_path().exists() {
            Some(Config::load_from_config_yaml(store_dir.clone())?)
        } else {
            None
        };
        if completed_config
            .as_ref()
            .is_some_and(|config| config.store_id != self.code.store_id)
        {
            return Err(crate::DeviceJoinError::JournalConflict.into());
        }
        let device_id = match (pending_readiness.as_ref(), completed_config.as_ref()) {
            (Some(readiness), _) => readiness.proof.registration.device_id.to_string(),
            (None, Some(config)) => config.device_id.clone(),
            (None, None) => return Err(crate::DeviceJoinError::JournalConflict.into()),
        };
        let signer = match completed_config {
            Some(_) => crate::keys::require_identity(self.identity_custody.as_ref())?,
            None => crate::keys::peek_pending_identity(&self.member_pubkey)?,
        };
        let join = self.build_storage(&signer).await?;
        let db_path = store_dir.db_path();
        let (db, _stamper) = Database::open(
            &db_path,
            self.synced_tables.clone(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::serial(),
            self.code.membership_floor.write_policy(),
            device_id.clone(),
            &self.migrations,
        )
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
        let joined = crate::sync::device_join::materialize_device_join_activation(
            &db,
            &join.storage,
            activation.clone(),
        )
        .await?;
        if pending_readiness
            .as_ref()
            .is_some_and(|readiness| joined.registration != readiness.proof.registration)
            || joined.registration.device_id.to_string() != device_id
        {
            return Err(crate::DeviceJoinError::JournalConflict.into());
        }
        on_status("Saving configuration...");
        self.custody.persist(&join.keyring)?;
        crate::keys::import_identity(self.identity_custody.as_ref(), &signer.to_keypair_bytes())?;
        let store_keys = StoreKeys::new(self.code.store_id.clone());
        if let Some(credentials) = derive_credentials(&self.code.join_info) {
            store_keys.set_cloud_home_credentials(&credentials)?;
        }
        #[cfg(feature = "oauth-providers")]
        if let Some(tokens) = &self.oauth_tokens {
            persist_oauth_tokens(&store_keys, tokens)?;
        }
        let cipher = CloudCipher::Encrypted(join.keyring.clone().into());
        let mut config = build_config(
            &self.code.store_id,
            &device_id,
            &store_dir,
            &self.code.store_name,
            &self.code.join_info,
            &cipher,
            self.custom_s3_serial,
        );
        if matches!(
            self.code.join_info,
            CloudHomeJoinInfo::S3 {
                endpoint: Some(_),
                ..
            }
        ) {
            config.cloud_home.s3_exact_slots = self.custom_s3_exact_slots;
        }
        config.save_to_config_yaml()?;
        crate::sync::device_join::complete_device_join(&db, &pending, &join.storage, activation)
            .await?;
        crate::keys::discard_pending_identity(&self.member_pubkey)?;
        info!(store_id = %self.code.store_id, "joined Store device");
        Ok(config)
    }

    fn require_offer(&self, offer: &crate::DeviceJoinOffer) -> Result<(), BootstrapError> {
        if offer.store_root != self.code.store_root || offer.member_pubkey != self.member_pubkey {
            return Err(crate::DeviceJoinError::OfferMismatch.into());
        }
        Ok(())
    }

    fn open_pending_journal(&self) -> Result<crate::DeviceJoinJournalDatabase, BootstrapError> {
        let directory = self.layout.stores_root().join(".pending-device-joins");
        std::fs::create_dir_all(&directory)?;
        Ok(crate::DeviceJoinJournalDatabase::open(
            directory.join(format!("{}.sqlite", self.code.store_id)),
        )?)
    }

    async fn build_cloud_home(&self) -> Result<JoinCloudHome, BootstrapError> {
        #[cfg(test)]
        if let Some(home) = &self.test_home {
            return Ok(JoinCloudHome {
                home: home.clone(),
                coordination: None,
            });
        }
        build_cloud_home_for_join(
            &self.code.join_info,
            &StoreKeys::new(self.code.store_id.clone()),
            self.oauth_tokens.clone(),
            self.cloudkit_ops.clone(),
            self.clock.clone(),
            self.custom_s3_exact_slots,
        )
        .await
    }

    async fn build_storage(
        &self,
        signer: &UserKeypair,
    ) -> Result<DeviceJoinStorage, BootstrapError> {
        let cloud = self.build_cloud_home().await?;
        let bootstrap_storage = CloudSyncStorage::new(
            cloud.home.clone(),
            CloudCipher::Plaintext,
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.code.store_id.clone(),
            signer.clone(),
        )?;
        let bootstrap_storage = match cloud.coordination.as_ref() {
            Some((primary, peer)) => {
                bootstrap_storage.with_serial_coordination_clients(primary.clone(), peer.clone())
            }
            None => bootstrap_storage,
        };
        let encryption = match &self.code.membership_floor {
            MembershipFloor::MergeConcurrent(floor) => {
                unwrap_store_keyring(
                    &bootstrap_storage,
                    signer,
                    &self.code.store_root,
                    &self.code.owner_pubkey,
                    &self.code.wrapped_key,
                    floor,
                )
                .await?
            }
            MembershipFloor::Serial(Some(reference)) => {
                if cloud.coordination.is_none() {
                    return Err(BootstrapError::Membership(
                        "Serial invite provider has no coordination capability".to_string(),
                    ));
                }
                unwrap_serial_store_keyring(
                    &bootstrap_storage,
                    bootstrap_storage.serial_coordination().map_err(|error| {
                        BootstrapError::Membership(format!("Serial coordination: {error}"))
                    })?,
                    signer,
                    &self.code.store_root,
                    &self.code.wrapped_key,
                    reference,
                )
                .await?
            }
            MembershipFloor::Serial(None) => {
                return Err(BootstrapError::Membership(
                    "Serial invite has the founder-only restore floor".to_string(),
                ));
            }
        };
        let exact = cloud.home.clone().exact_slot_storage().ok_or_else(|| {
            BootstrapError::Provider("provider has no exact-slot adapter".to_string())
        })?;
        let keyring = MasterKeyring::from(encryption.clone());
        let storage = CloudSyncStorage::new(
            cloud.home,
            CloudCipher::Encrypted(encryption),
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.code.store_id.clone(),
            signer.clone(),
        )?;
        let storage = match cloud.coordination {
            Some((primary, peer)) => storage.with_serial_coordination_clients(primary, peer),
            None => storage,
        };
        Ok(DeviceJoinStorage {
            storage,
            exact,
            keyring,
        })
    }
}

/// Inner restore bootstrap logic, separated so callers can clean up the store
/// directory on failure.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn bootstrap_and_save_store(
    storage: &CloudSyncStorage,
    cipher: &CloudCipher,
    master_key: Option<&MasterKeyring>,
    store_dir: &StoreDir,
    store_id: &str,
    device_id: &str,
    store_root: crate::sync::store_commit::StoreRootRef,
    context: RestoreBootstrapContext<'_>,
    membership_floor: &MembershipFloor,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    join_info: &CloudHomeJoinInfo,
    store_name: &str,
    custom_s3_serial: Option<crate::CustomS3Serial>,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    key_service: &StoreKeys,
    custody: &dyn MasterKeyCustody,
    identity_custody: &dyn DeviceIdentityCustody,
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
    let coordination = match membership_floor {
        MembershipFloor::MergeConcurrent(_) => None,
        MembershipFloor::Serial(_) => Some(storage.serial_coordination().map_err(|error| {
            BootstrapError::Membership(format!("Serial coordination: {error}"))
        })?),
    };
    let bootstrap_result = bootstrap_from_snapshot(
        bucket_dyn,
        coordination,
        store_id,
        store_root.clone(),
        membership_floor,
        binary_schema_version,
        &db_path,
    )
    .await?;

    info!(
        "Bootstrapped from snapshot ({} device coverage entries)",
        bootstrap_result.coverage_count()
    );

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
            context.owner_pubkey(),
            store_root.clone(),
            context.activated_continuation(),
            context.owner_recovery(),
            membership_floor,
            storage,
            coordination,
            bootstrap_result,
            store_dir,
            cancel,
        )
        .await?;

        if changesets_applied.changesets_applied > 0 {
            info!(
                "Applied {} changesets since snapshot",
                changesets_applied.changesets_applied
            );
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
        // retry clears and re-establishes from the restore code the user still
        // holds. Inside the unit, so a later failure deletes the bootstrap
        // reader the same as any other.
        crate::keys::import_identity(identity_custody, &context.keypair().to_keypair_bytes())?;

        // Create and save config — the last durable write and the
        // store's completion marker.
        let mut config = build_config(
            store_id,
            device_id,
            store_dir,
            store_name,
            join_info,
            cipher,
            custom_s3_serial,
        );
        if matches!(
            join_info,
            CloudHomeJoinInfo::S3 {
                endpoint: Some(_),
                ..
            }
        ) {
            config.cloud_home.s3_exact_slots = custom_s3_exact_slots;
        }
        config.save_to_config_yaml()?;
        Ok(config)
    }
    .await;

    committed
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
    store_root: crate::sync::store_commit::StoreRootRef,
    activated_continuation: Option<(
        &crate::sync::restore_code::ActivatedContinuation,
        &UserKeypair,
        &UserKeypair,
    )>,
    owner_recovery: Option<(
        &crate::sync::restore_code::OwnerRecoveryAuthority,
        &UserKeypair,
    )>,
    membership_floor: &MembershipFloor,
    storage: &dyn SyncStorage,
    coordination: Option<&dyn crate::sync::storage::CoordinationStorage>,
    bootstrap: BootstrapResult,
    store_dir: &StoreDir,
    cancel: &watch::Receiver<bool>,
) -> Result<OpenDbPullOutcome, BootstrapError> {
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

    // Seed this device's per-author-stream membership-head watermark from the invite
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
    if db.write_policy() == crate::WritePolicy::Serial && coordination.is_none() {
        return Err(BootstrapError::Membership(
            "Serial coordination capability is absent".to_string(),
        ));
    }
    let pull_result = Box::pin(crate::sync::store_engine::pull_store_commits(
        &db,
        db.synced_tables(),
        storage,
        coordination,
        store_root.store_root_hash,
        store_dir,
        membership.as_ref().and_then(|loaded| loaded.chain.as_ref()),
        None,
    ))
    .await?;

    if let Some((continuation, identity_signer, device_signer)) = activated_continuation {
        let registration = crate::sync::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &store_root,
            continuation.registration.device_id,
        )
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
        let latest_ack = crate::sync::store_objects::load_store_ack_ref(
            storage,
            &store_root,
            &continuation.latest_ack,
            &registration,
        )
        .await
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
        let mut ack_chain = vec![(continuation.latest_ack.clone(), latest_ack.value)];
        loop {
            let (successor_ref, successor) = ack_chain.last().expect("ack chain is nonempty");
            let Some((predecessor, loaded)) =
                crate::sync::store_objects::load_store_ack_predecessor(
                    storage,
                    &store_root,
                    successor_ref,
                    successor,
                    &registration,
                )
                .await
                .map_err(|error| BootstrapError::Database(error.to_string()))?
            else {
                break;
            };
            ack_chain.push((predecessor, loaded.value));
        }
        let latest_snapshot = match &continuation.latest_snapshot {
            Some(reference) => Some(
                crate::sync::store_snapshot::load_store_snapshot_ref(
                    storage,
                    &store_root,
                    &continuation.registration,
                    &registration,
                    reference,
                )
                .await
                .map_err(|error| BootstrapError::Database(error.to_string()))?,
            ),
            None => None,
        };
        db.install_activated_device_continuation(
            continuation.clone(),
            identity_signer,
            device_signer,
            ack_chain,
            latest_snapshot,
        )
        .await
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
    }

    if let Some((authority, identity_signer)) = owner_recovery {
        match membership_floor {
            MembershipFloor::MergeConcurrent(_) => {
                let membership = membership
                    .as_ref()
                    .and_then(|loaded| loaded.chain.as_ref())
                    .ok_or_else(|| {
                        BootstrapError::Membership(
                            "Owner recovery requires resolved MergeConcurrent membership"
                                .to_string(),
                        )
                    })?;
                Box::pin(crate::sync::store_registration::recover_owner_device_merge(
                    &db,
                    storage,
                    identity_signer,
                    authority,
                    membership,
                ))
                .await?;
            }
            MembershipFloor::Serial(_) => {
                let coordination = coordination.ok_or_else(|| {
                    BootstrapError::Membership(
                        "Serial Owner recovery requires coordination".to_string(),
                    )
                })?;
                Box::pin(
                    crate::sync::store_registration::recover_owner_device_serial(
                        &db,
                        storage,
                        coordination,
                        identity_signer,
                        authority,
                    ),
                )
                .await?;
            }
        }
    }

    // Bootstrap installed the snapshot's position vector before pulling anything
    // above it, and each higher position committed with its rows. Record the remaining
    // bootstrap marker so the first real cycle treats this device as a joiner,
    // not a brand-new store that should publish an initial snapshot.
    db.set_protocol_state("snapshot_seq", "0")
        .await
        .map_err(|e| BootstrapError::Database(format!("Failed to persist snapshot_seq: {e}")))?;

    Ok(OpenDbPullOutcome {
        changesets_applied: pull_result.changesets_applied,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OpenDbPullOutcome {
    pub changesets_applied: u64,
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
