//! Join an existing shared store using an invite code.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::path::Path;
use std::sync::Arc;

use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::{CloudProvider, Config, ConfigError, HomeStorage};
use crate::database::supported_version;
use crate::database::Database;
use crate::encryption::{EncryptionError, EncryptionService, MasterKeyring};
use crate::identity_custody::IdentityCustody;
use crate::joining::{InviteCode, MembershipFloor};
use crate::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys, UserKeypair,
};
use crate::storage::cloud::{CloudHome, CloudHomeError, CloudHomeJoinInfo};
use crate::storage::SyncStorage;
use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
use crate::store_dir::{StoreDir, StoreLayout};
use crate::sync::session::SyncedTable;
use crate::sync::store::{
    bootstrap_from_snapshot, BootstrapResult, InviteError, PullError, SnapshotBlobReconcile,
    SnapshotError,
};
use crate::Migration;

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
    StorePull(#[from] crate::sync::store::StorePullError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] crate::sync::store::StoreRegistrationError),
    #[error("Store device join: {0}")]
    DeviceJoin(#[from] crate::DeviceJoinError),
    #[error("Store device join transport: {0}")]
    DeviceJoinTransport(#[from] crate::sync::store::DeviceJoinTransportError),
    #[error("storage: {0}")]
    Storage(#[from] crate::storage::StorageError),
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

/// The complete local cleanup capability for one bootstrap attempt.
pub(crate) struct BootstrapCleanup<'a> {
    store_dir: &'a StoreDir,
    store_keys: &'a StoreKeys,
    custody: &'a dyn MasterKeyCustody,
    identity_custody: &'a dyn DeviceIdentityCustody,
}

impl<'a> BootstrapCleanup<'a> {
    pub(crate) fn new(
        store_dir: &'a StoreDir,
        store_keys: &'a StoreKeys,
        custody: &'a dyn MasterKeyCustody,
        identity_custody: &'a dyn DeviceIdentityCustody,
    ) -> Self {
        Self {
            store_dir,
            store_keys,
            custody,
            identity_custody,
        }
    }

    /// Refuse a completed store and remove all local state from a torn attempt.
    pub(crate) fn refuse_completed_or_clear(&self, store_id: &str) -> Result<(), BootstrapError> {
        if self.store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(store_id.to_string()));
        }

        if self.store_dir.exists() {
            warn!(
                store_dir = %self.store_dir.display(),
                "clearing a torn bootstrap: a store directory with no saved config, left by a join or restore a crash interrupted before completion"
            );
            let failures = self.remove();
            if !failures.is_empty() {
                return Err(BootstrapError::TornBootstrapCleanup {
                    store_id: store_id.to_string(),
                    failures: failures.join("; "),
                });
            }
        }

        Ok(())
    }

    /// Remove partial local state and preserve the initiating bootstrap error.
    pub(crate) fn after_failure(&self, cause: BootstrapError) -> BootstrapError {
        let failures = self.remove();
        if failures.is_empty() {
            cause
        } else {
            BootstrapError::Cleanup {
                cleanup: failures.join("; "),
                cause: Box::new(cause),
            }
        }
    }

    /// Remove every local artifact the bound bootstrap attempt may have written.
    pub(crate) fn remove(&self) -> Vec<String> {
        let mut failures = Vec::new();

        match std::fs::remove_dir_all(&**self.store_dir) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => failures.push(format!("store directory: {error}")),
        }
        if let Err(error) = self.custody.forget() {
            failures.push(format!("master key: {error}"));
        }
        if let Err(error) = self.identity_custody.forget() {
            failures.push(format!("identity: {error}"));
        }
        if let Err(error) = self.store_keys.delete_cloud_home_credentials() {
            failures.push(format!("cloud home credentials: {error}"));
        }

        failures
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

/// The identity and verified authority carried by a restore code. Bootstrap
/// installs the identity in store custody before writing the config marker.
pub(crate) struct RestoreBootstrapContext<'a> {
    pub keypair: &'a UserKeypair,
    pub authority: &'a crate::restoration::RestoreAuthority,
    pub continuation: Option<(
        &'a crate::restoration::ActivatedContinuation,
        &'a UserKeypair,
    )>,
}

impl RestoreBootstrapContext<'_> {
    fn keypair(&self) -> &UserKeypair {
        self.keypair
    }

    fn activated_continuation(&self) -> Option<&crate::restoration::ActivatedContinuation> {
        self.continuation.map(|(continuation, _)| continuation)
    }

    fn owner_recovery(&self) -> Option<&crate::restoration::OwnerRecoveryAuthority> {
        match self.authority {
            crate::restoration::RestoreAuthority::OwnerRecovery(authority) => Some(authority),
            crate::restoration::RestoreAuthority::ActivatedContinuation(_) => None,
        }
    }
}

async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    lib_ks: &StoreKeys,
    oauth_clients: &crate::oauth::OAuthClients,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
) -> Result<Arc<dyn CloudHome>, BootstrapError> {
    use crate::storage::cloud::*;

    #[cfg(not(feature = "oauth-providers"))]
    let _ = (&lib_ks, oauth_clients, &oauth_tokens, &clock);

    match join_info {
        CloudHomeJoinInfo::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        } => {
            let s3 = s3::open_cloud_home(
                bucket.clone(),
                region.clone(),
                endpoint.clone(),
                access_key.clone(),
                secret_key.clone(),
                key_prefix.clone(),
                custom_s3_exact_slots,
            )
            .await?;
            Ok(Arc::new(s3))
        }
        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            let tokens = oauth_tokens.ok_or_else(|| {
                BootstrapError::Provider("Google Drive join requires an OAuth token".to_string())
            })?;
            let oauth_config = oauth_clients
                .config_for(CloudProvider::GoogleDrive)
                .map_err(|error| BootstrapError::Provider(error.to_string()))?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                lib_ks.clone(),
                clock,
                oauth_config,
                "Google Drive",
            );
            Ok(Arc::new(google_drive::GoogleDriveCloudHome::new(
                folder_id.clone(),
                session,
            )))
        }
        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            let tokens = oauth_tokens.ok_or_else(|| {
                BootstrapError::Provider("Dropbox join requires an OAuth token".to_string())
            })?;
            let oauth_config = oauth_clients
                .config_for(CloudProvider::Dropbox)
                .map_err(|error| BootstrapError::Provider(error.to_string()))?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                lib_ks.clone(),
                clock,
                oauth_config,
                "Dropbox",
            );
            Ok(Arc::new(dropbox::DropboxCloudHome::new(
                folder_path.clone(),
                session,
            )))
        }
        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::OneDrive {
            drive_id,
            folder_id,
        } => {
            let tokens = oauth_tokens.ok_or_else(|| {
                BootstrapError::Provider("OneDrive join requires an OAuth token".to_string())
            })?;
            let oauth_config = oauth_clients
                .config_for(CloudProvider::OneDrive)
                .map_err(|error| BootstrapError::Provider(error.to_string()))?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                lib_ks.clone(),
                clock,
                oauth_config,
                "OneDrive",
            );
            Ok(Arc::new(onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                session,
            )))
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
            Ok(Arc::new(cloudkit::CloudKitCloudHome::new_private(ops)))
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
            Ok(home)
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
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    store_keys: StoreKeys,
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    oauth_clients: crate::oauth::OAuthClients,
    oauth_tokens: Option<crate::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    clock: crate::clock::ClockRef,
    #[cfg(any(test, feature = "test-utils"))]
    test_home: Option<Arc<dyn CloudHome>>,
}

struct DeviceJoinStorage {
    storage: CloudSyncStorage,
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
        custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
        key_custody: crate::custody::KeyCustody,
        identity_custody: IdentityCustody,
        oauth_clients: crate::oauth::OAuthClients,
        oauth_tokens: Option<crate::oauth::OAuthTokens>,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        clock: crate::clock::ClockRef,
    ) -> Result<Self, BootstrapError> {
        let code = crate::joining::decode(invite_code)
            .map_err(|error| BootstrapError::InvalidCode(error.to_string()))?;
        let member_pubkey = crate::joining::decode_join_request(join_request_code)
            .map_err(|error| BootstrapError::InvalidCode(error.to_string()))?
            .public_key;
        crate::storage::cloud::setup::require_exact_slot_capabilities_join_info(
            &code.join_info,
            custom_s3_exact_slots,
        )
        .map_err(|provider| BootstrapError::ExactSlotsUnavailable { provider })?;
        crate::store_dir::validate_path_token(&code.store_id)
            .map_err(|error| BootstrapError::InvalidCode(format!("invalid store id: {error}")))?;
        let store_dir = layout.store_dir(&code.store_id);
        let store_keys = StoreKeys::bind(code.store_id.clone());
        let custody = key_custody.resolve(&store_keys, &store_dir);
        let identity_custody = identity_custody.resolve(&store_keys, &store_dir);
        Ok(Self {
            code,
            member_pubkey,
            layout,
            synced_tables,
            migrations,
            custom_s3_exact_slots,
            store_keys,
            custody,
            identity_custody,
            oauth_clients,
            oauth_tokens,
            cloudkit_ops,
            clock,
            #[cfg(any(test, feature = "test-utils"))]
            test_home: None,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
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
        let storage = self.transport_storage().await?;
        let pending = self.open_pending_journal()?;
        let authority = crate::sync::store::open_pending_device_join_authority(
            &pending, &storage, &signer, offer,
        )
        .await?;
        Ok(authority.prepare_provider_access_request().await?)
    }

    pub async fn accept_device_join_abandonment(
        &self,
        abandonment: crate::DeviceJoinAbandonment,
    ) -> Result<crate::DeviceJoinAbandonment, BootstrapError> {
        let storage = self.transport_storage().await?;
        let pending = self.open_pending_journal()?;
        let mut observation = crate::sync::store::observe_pending_device_join(
            &pending,
            &storage,
            &self.code.store_root,
            abandonment.abandonment.attempt_id,
        )
        .await?;
        Ok(observation.observe_abandonment(abandonment).await?)
    }

    pub fn device_join_status(
        &self,
        attempt_id: crate::DeviceJoinAttemptId,
    ) -> Result<Option<crate::DeviceJoinStatus>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(pending.status(attempt_id)?)
    }

    pub fn resume_device_joins(&self) -> Result<Vec<crate::DeviceJoinAction>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(pending.actions()?)
    }

    pub async fn close_pending_device_join(
        &self,
        cancellation: crate::DeviceJoinCancellation,
    ) -> Result<crate::JoinerJoinTerminal, BootstrapError> {
        let signer = crate::keys::peek_pending_identity(&self.member_pubkey)?;
        let storage = self.transport_storage().await?;
        let pending = self.open_pending_journal()?;
        let observation = crate::sync::store::observe_pending_device_join(
            &pending,
            &storage,
            &self.code.store_root,
            cancellation.outcome.attempt().attempt_id,
        )
        .await?;
        let mut closure = observation.authorize_closure(&signer);
        Ok(closure.close(cancellation).await?)
    }

    pub async fn complete_cancelled_device_join(
        &self,
        activation: crate::DeviceJoinCleanupActivation,
    ) -> Result<(), BootstrapError> {
        let pending = self.open_pending_journal()?;
        match pending.status(activation.receipt.attempt_id)? {
            Some(crate::DeviceJoinStatus::CleanupActivated {
                activation: durable,
            }) if durable == activation => {}
            Some(crate::DeviceJoinStatus::CleanupActivated { .. }) => {
                return Err(crate::DeviceJoinError::JournalConflict.into());
            }
            _ => {
                let storage = self.transport_storage().await?;
                let mut observation = crate::sync::store::observe_pending_device_join(
                    &pending,
                    &storage,
                    &self.code.store_root,
                    activation.receipt.attempt_id,
                )
                .await?;
                observation.accept_cleanup(activation.clone()).await?;
            }
        }

        let store_dir = self.layout.store_dir(&self.code.store_id);
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.code.store_id.clone()));
        }
        let cleanup = BootstrapCleanup::new(
            &store_dir,
            &self.store_keys,
            self.custody.as_ref(),
            self.identity_custody.as_ref(),
        );
        let failures = cleanup.remove();
        if !failures.is_empty() {
            return Err(BootstrapError::CancelledJoinCleanup {
                store_id: self.code.store_id.clone(),
                failures: failures.join("; "),
            });
        }
        crate::keys::discard_pending_identity(&self.member_pubkey)?;
        pending.complete_joiner_cleanup(activation)?;
        Ok(())
    }

    pub async fn prepare_registration_request(
        &self,
        approval: crate::DeviceProviderAdmissionApproval,
    ) -> Result<crate::DeviceRegistrationRequest, BootstrapError> {
        let offer = &approval.request.offer;
        self.require_offer(offer)?;
        let signer = crate::keys::peek_pending_identity(&offer.member_pubkey)?;
        let storage = self.transport_storage().await?;
        let pending = self.open_pending_journal()?;
        let mut authority = crate::sync::store::open_pending_device_join_authority(
            &pending,
            &storage,
            &signer,
            offer.as_ref().clone(),
        )
        .await?;
        Ok(authority.prepare_registration_request(approval).await?)
    }

    pub async fn bootstrap_pending_device(
        &self,
        bootstrap: crate::ProviderReadyDeviceBootstrap,
        on_status: impl Fn(&str),
        cancel: &watch::Receiver<bool>,
    ) -> Result<crate::sync::store::DeviceJoinReadiness, BootstrapError> {
        let offer = &bootstrap.bootstrap.request.approval.request.offer;
        self.require_offer(offer)?;
        let attempt = &bootstrap.bootstrap.publication_authorization.attempt;
        let pending = self.open_pending_journal()?;
        error_if_cancelled(cancel)?;
        let signer = crate::keys::peek_pending_identity(&offer.member_pubkey)?;
        let join = self.build_storage(&signer).await?;
        let store_dir = self.layout.store_dir(&self.code.store_id);
        if let Some(readiness) = pending.completed_joiner_readiness(attempt)? {
            if store_dir.db_path().exists() {
                return Ok(readiness);
            }
        }
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.code.store_id.clone()));
        }
        std::fs::create_dir_all(&*store_dir)?;
        on_status("Downloading store snapshot...");
        let db_path = store_dir.db_path();
        let snapshot = bootstrap_from_snapshot(
            &join.storage,
            &self.code.store_id,
            offer.store_root.clone(),
            &self.code.membership_floor,
            supported_version(&self.migrations),
            &db_path,
            &signer,
        )
        .await?;
        let routing_encryption = EncryptionService::from(join.keyring.clone());
        let device_id = bootstrap
            .bootstrap
            .request
            .expected_registration
            .device_id
            .to_string();
        let opened = snapshot
            .open_database(
                &self.code.store_id,
                &db_path,
                self.synced_tables.clone(),
                crate::blob::delete::BLOB_TOMBSTONE_GRACE,
                crate::blob::TransferLimits::one_at_a_time(),
                device_id,
                self.clock.clone(),
                &self.migrations,
                Some(&routing_encryption),
            )
            .await?;
        match opened
            .reconcile_snapshot_blobs(&store_dir, cancel)
            .await
            .map_err(|error| BootstrapError::Database(error.to_string()))?
        {
            SnapshotBlobReconcile::Complete => {}
            SnapshotBlobReconcile::Incomplete => {
                return Err(BootstrapError::Database(
                    "snapshot blob reconciliation did not land every required eager blob"
                        .to_string(),
                ));
            }
            SnapshotBlobReconcile::Cancelled => return Err(BootstrapError::Cancelled),
        }
        let published_at = self.clock.now().to_rfc3339();
        on_status("Installing device registration...");
        let mut joining = opened
            .begin_device_join(&pending, offer.as_ref().clone())
            .await?;
        Ok(joining.bootstrap(bootstrap, &published_at).await?)
    }

    pub async fn complete_device_join(
        &self,
        activation: crate::DeviceJoinActivation,
        on_status: impl Fn(&str),
    ) -> Result<Config, BootstrapError> {
        let attempt_id = activation.outcome.attempt().attempt_id;
        let pending = self.open_pending_journal()?;
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
        let signer = match completed_config.as_ref() {
            Some(_) => crate::keys::require_identity(self.identity_custody.as_ref())?,
            None => crate::keys::peek_pending_identity(&self.member_pubkey)?,
        };
        let join = self.build_storage(&signer).await?;
        let pending_readiness = pending.observe_joiner_activation_if_pending(&activation)?;
        let device_id = match (pending_readiness.as_ref(), completed_config.as_ref()) {
            (Some(readiness), _) => readiness.proof.registration.device_id.to_string(),
            (None, Some(config)) => config.device_id.clone(),
            (None, None) => return Err(crate::DeviceJoinError::JournalConflict.into()),
        };
        let db_path = store_dir.db_path();
        let (db, _stamper) = Database::open(
            &db_path,
            self.synced_tables.clone(),
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            device_id.clone(),
            self.clock.clone(),
            &self.migrations,
        )
        .map_err(|error| BootstrapError::Database(error.to_string()))?;
        let database = crate::database::StoreDatabase::from_database(db.clone());
        // Converge over everything the owner published after this device's
        // bootstrap before materializing the commit that activates the join.
        //
        // The attempt pins `bootstrap_cut` and its activation together, so the
        // bootstrap covers exactly that pair. The outcome activation is composed
        // later, against whatever the owner's frontier has become by then — so
        // every Store commit the owner published in between sits between the
        // bootstrapped history and the commit to materialize. The activation
        // resolves its predecessor device state out of the local history, which
        // has to hold those commits and the row data they carry.
        on_status("Catching up on store history...");
        let routing_encryption = EncryptionService::from(join.keyring.clone());
        let mut joining = crate::sync::store::resume_joining_store(
            &pending,
            database,
            &join.storage,
            &signer,
            &self.code.store_root,
            attempt_id,
        )
        .await?;
        joining
            .pull_store_history(&store_dir, Some(&routing_encryption))
            .await?;
        let joined = joining.materialize(activation.clone()).await?;
        if pending_readiness
            .as_ref()
            .is_some_and(|readiness| joined.registration != readiness.proof.registration)
            || joined.registration.device_id.to_string() != device_id
        {
            return Err(crate::DeviceJoinError::JournalConflict.into());
        }
        on_status("Saving configuration...");
        self.custody.persist(&join.keyring)?;
        self.identity_custody.establish(&signer)?;
        if let Some(credentials) = derive_credentials(&self.code.join_info) {
            self.store_keys.set_cloud_home_credentials(&credentials)?;
        }
        #[cfg(feature = "oauth-providers")]
        if let Some(tokens) = &self.oauth_tokens {
            self.store_keys.set_cloud_home_oauth_tokens(tokens)?;
        }
        let cipher = CloudCipher::Encrypted(join.keyring.clone().into());
        let mut config = build_config(
            &self.code.store_id,
            &device_id,
            &store_dir,
            &self.code.store_name,
            &self.code.join_info,
            &cipher,
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
        joining.complete(activation).await?;
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

    async fn build_cloud_home(&self) -> Result<Arc<dyn CloudHome>, BootstrapError> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(home) = &self.test_home {
            return Ok(home.clone());
        }
        build_cloud_home_for_join(
            &self.code.join_info,
            &self.store_keys,
            &self.oauth_clients,
            self.oauth_tokens.clone(),
            self.cloudkit_ops.clone(),
            self.clock.clone(),
            self.custom_s3_exact_slots,
        )
        .await
    }

    /// Plaintext storage over the joining device's cloud home.
    ///
    /// Both the pre-key bootstrap reads and the device-join transport go
    /// through this: the transport's objects carry their own per-attempt seal,
    /// so they need no store key — which is what lets a joiner publish its
    /// access request before it has unwrapped the store keyring at all.
    pub(crate) async fn transport_storage(&self) -> Result<CloudSyncStorage, BootstrapError> {
        let signer = crate::keys::peek_pending_identity(&self.member_pubkey)?;
        let cloud = self.build_cloud_home().await?;
        self.plaintext_storage(cloud, &signer)
    }

    fn plaintext_storage(
        &self,
        home: Arc<dyn CloudHome>,
        signer: &UserKeypair,
    ) -> Result<CloudSyncStorage, BootstrapError> {
        Ok(CloudSyncStorage::new(
            home,
            CloudCipher::Plaintext,
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.code.store_id.clone(),
            signer.clone(),
        )?)
    }

    async fn build_storage(
        &self,
        signer: &UserKeypair,
    ) -> Result<DeviceJoinStorage, BootstrapError> {
        let cloud = self.build_cloud_home().await?;
        let bootstrap_storage = self.plaintext_storage(cloud.clone(), signer)?;
        let encryption = self.code.open_keyring(&bootstrap_storage, signer).await?;
        cloud.clone().exact_slot_storage().ok_or_else(|| {
            BootstrapError::Provider("provider has no exact-slot adapter".to_string())
        })?;
        let keyring = MasterKeyring::from(encryption.clone());
        let storage = CloudSyncStorage::new(
            cloud,
            CloudCipher::Encrypted(encryption),
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.code.store_id.clone(),
            signer.clone(),
        )?;
        Ok(DeviceJoinStorage { storage, keyring })
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
    clock: crate::clock::ClockRef,
    store_root: crate::protocol::store_commit::StoreRootRef,
    context: RestoreBootstrapContext<'_>,
    membership_floor: &MembershipFloor,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    join_info: &CloudHomeJoinInfo,
    store_name: &str,
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
    let bootstrap_result = bootstrap_from_snapshot(
        bucket_dyn,
        store_id,
        store_root.clone(),
        membership_floor,
        binary_schema_version,
        &db_path,
        context.keypair(),
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
        let routing_encryption = master_key.map(|keyring| EncryptionService::from(keyring.clone()));
        let changesets_applied = open_db_and_pull(
            store_id,
            &db_path,
            synced_tables,
            migrations,
            device_id,
            clock,
            context.activated_continuation(),
            context.owner_recovery(),
            routing_encryption.as_ref(),
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
        identity_custody.establish(context.keypair())?;

        // Create and save config — the last durable write and the
        // store's completion marker.
        let mut config = build_config(
            store_id, device_id, store_dir, store_name, join_info, cipher,
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
    clock: crate::clock::ClockRef,
    activated_continuation: Option<&crate::restoration::ActivatedContinuation>,
    owner_recovery: Option<&crate::restoration::OwnerRecoveryAuthority>,
    routing_encryption: Option<&EncryptionService>,
    bootstrap: BootstrapResult<'_>,
    store_dir: &StoreDir,
    cancel: &watch::Receiver<bool>,
) -> Result<OpenDbPullOutcome, BootstrapError> {
    let mut store = bootstrap
        .open_database(
            store_id,
            db_path,
            synced_tables.to_vec(),
            // This bootstrap database only applies changesets during join; it never runs
            // the tombstone GC, so the grace is immaterial and takes the default.
            crate::blob::delete::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            device_id.to_string(),
            clock,
            migrations,
            routing_encryption,
        )
        .await?;

    // Download the blob files the snapshot's rows reference: the snapshot carried
    // the catalog rows but no blob files, and the pull starts past its signed
    // coverage, so it never re-walks the INSERTs that first carried them. Missing
    // eager blobs abort the bootstrap before the store is saved. Cancellation is
    // checked between blobs inside the reconcile, surfacing as `Cancelled` here.
    match store
        .reconcile_snapshot_blobs(store_dir, cancel)
        .await
        .map_err(|e| BootstrapError::Database(format!("Failed to reconcile snapshot blobs: {e}")))?
    {
        SnapshotBlobReconcile::Complete => {}
        SnapshotBlobReconcile::Incomplete => {
            return Err(BootstrapError::Database(
                "Snapshot blob reconciliation did not land every required eager blob".to_string(),
            ));
        }
        SnapshotBlobReconcile::Cancelled => {
            return Err(BootstrapError::Cancelled);
        }
    }

    // The pull downloads every changeset published since the snapshot — the last
    // heavy phase. Check cancellation before entering it; the pull itself is a
    // single phase here (per-changeset cancel is the sync loop's own concern).
    error_if_cancelled(cancel)?;

    let pull_result = store.pull(store_dir, routing_encryption).await?;

    if let Some(continuation) = activated_continuation {
        store
            .install_activated_device_continuation(continuation.clone())
            .await?;
    }

    if let Some(authority) = owner_recovery {
        store.recover_owner_device(authority).await?;
    }

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
        let store_keys = StoreKeys::bind("guard-completed-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key");
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);

        let cleanup = BootstrapCleanup::new(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let result = cleanup.refuse_completed_or_clear("guard-completed-test");

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
        let store_keys = StoreKeys::bind("guard-torn-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key");
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
        identity_custody
            .persist(&UserKeypair::generate())
            .expect("seed the identity");
        store_keys
            .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
            })
            .expect("seed cloud home credentials");

        let cleanup = BootstrapCleanup::new(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let result = cleanup.refuse_completed_or_clear("guard-torn-test");

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
        let store_keys = StoreKeys::bind("cleanup-failure-cause-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &blocked);
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &blocked);

        let cleanup = BootstrapCleanup::new(
            &blocked,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let wrapped = cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

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
        let store_keys = StoreKeys::bind("successful-cleanup-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);

        let cleanup = BootstrapCleanup::new(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let returned =
            cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

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
        let store_keys = StoreKeys::bind("never-created-dir-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &never_created);
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &never_created);

        let cleanup = BootstrapCleanup::new(
            &never_created,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let returned =
            cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

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
        let store_keys = StoreKeys::bind("keyring-cleanup-test".to_string());
        let custody = crate::custody::KeyCustody::Keyring.resolve(&store_keys, &store_dir);
        custody
            .persist(&MasterKeyring::generate())
            .expect("seed the master key via custody");
        let identity_custody = IdentityCustody::Keyring.resolve(&store_keys, &store_dir);
        identity_custody
            .persist(&crate::keys::UserKeypair::generate())
            .expect("seed this store's identity via custody");
        store_keys
            .set_cloud_home_credentials(&CloudHomeCredentials::S3 {
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
            })
            .expect("seed cloud home credentials");

        let cleanup = BootstrapCleanup::new(
            &store_dir,
            &store_keys,
            custody.as_ref(),
            identity_custody.as_ref(),
        );
        let returned =
            cleanup.after_failure(BootstrapError::Database("bootstrap boom".to_string()));

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
