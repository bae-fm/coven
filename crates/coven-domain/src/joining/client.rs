//! Join an existing shared store using a recipient-sealed device invitation.
//!
//! Shared across all platforms (macOS, iOS, CLI).

use std::sync::Arc;

use tokio::sync::watch;
use tracing::{info, warn};

use coven_database::supported_version;
use coven_database::Migration;
use coven_database::{CovenMigrationPolicy, Database};
#[cfg(feature = "oauth-providers")]
use coven_foundation::config::CloudProvider;
use coven_foundation::config::{Config, ConfigError, HomeStorage};
use coven_foundation::stage_timing::StageTimings;
use coven_foundation::store_dir::{StoreDir, StoreLayout};
use coven_keys::encryption::{EncryptionError, EncryptionService, MasterKeyring};
use coven_keys::identity_custody::IdentityCustody;
use coven_keys::keys::{
    CloudHomeCredentials, DeviceIdentityCustody, KeyError, MasterKeyCustody, StoreKeys, UserKeypair,
};
use coven_protocol::synced_schema::SyncedTable;
use coven_replication::sync::store::{
    MembershipMutationError, PreparedDeviceJoinSnapshot, PreparedSnapshotBootstrap, PullError,
    SnapshotError,
};
use coven_replication::sync::MemberAdmission;
use coven_storage::cloud::{CloudHomeError, CloudHomeJoinInfo, ExactCloudHome};
use coven_storage::{BlobPathScheme, CloudCipher, CloudSyncConnection};

/// Why joining or restoring a store failed. Both are the same operation —
/// bootstrap a store from the cloud — differing only in their entry data (an
/// admission that wraps the store key vs a restore code that carries the bucket
/// credentials), so they share one error shape rather than two that duplicate
/// most of their variants and then have to map between each other.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("encryption: {0}")]
    Encryption(#[from] EncryptionError),
    #[error("membership mutation: {0}")]
    MembershipMutation(#[source] Box<MembershipMutationError>),
    #[error("snapshot: {0}")]
    Snapshot(SnapshotError),
    #[error("pull: {0}")]
    Pull(#[from] PullError),
    #[error("Store pull: {0}")]
    StorePull(#[from] coven_replication::sync::store::StorePullError),
    #[error("Store device registration: {0}")]
    StoreRegistration(#[from] coven_replication::sync::store::StoreRegistrationError),
    #[error("Store device join: {0}")]
    DeviceJoin(#[from] coven_replication::sync::DeviceJoinError),
    #[error("Store device join transport: {0}")]
    DeviceJoinTransport(#[from] coven_replication::sync::store::DeviceJoinTransportError),
    #[error("storage: {0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("keyring: {0}")]
    Key(#[from] KeyError),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("device invitation: {0}")]
    DeviceInvite(#[from] crate::joining::DeviceInviteError),
    #[error("device pairing: {0}")]
    Pairing(#[from] crate::joining::DevicePairingTransportError),
    #[error("device pairing state: {0}")]
    PairingState(#[from] crate::joining::DevicePairingError),
    #[error("device join invite version {0} is not supported")]
    UnsupportedDeviceInviteVersion(u32),
    #[error("invalid store id: {0}")]
    InvalidStoreId(#[from] coven_foundation::store_dir::PathTokenError),
    #[error("invalid restore code: {0}")]
    RestoreCode(#[from] crate::restoration::RestoreCodeError),
    #[error("store already exists locally: {0}")]
    StoreExists(String),
    /// A hard crash left a store directory with no saved config, and clearing
    /// that torn-bootstrap residue before retrying failed — so the retry can't
    /// proceed over the leftover directory or keyring entries.
    #[error("could not clear a torn bootstrap for {store_id}: {failures}")]
    TornBootstrapCleanup {
        store_id: String,
        failures: BootstrapCleanupFailures,
    },
    #[error("could not remove cancelled join state for {store_id}: {failures}")]
    CancelledJoinCleanup {
        store_id: String,
        failures: BootstrapCleanupFailures,
    },
    #[error("provider: {0}")]
    Provider(String),
    #[cfg(feature = "oauth-providers")]
    #[error("OAuth client configuration: {0}")]
    OAuthClient(#[from] coven_storage::oauth::OAuthClientCredsError),
    #[error("{provider:?} cannot provide exact protocol and blob slots with this configuration")]
    ExactSlotsUnavailable {
        provider: coven_foundation::config::CloudProvider,
    },
    #[error("database open: {0}")]
    DatabaseOpen(#[from] coven_database::OpenError),
    #[error("invalid signing key: {0}")]
    InvalidSigningKey(#[from] SigningKeyError),
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
        cleanup: BootstrapCleanupFailures,
        cause: Box<BootstrapError>,
    },
}

impl From<SnapshotError> for BootstrapError {
    fn from(error: SnapshotError) -> Self {
        match error {
            SnapshotError::Cancelled => Self::Cancelled,
            error => Self::Snapshot(error),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SigningKeyError {
    #[error("{0}")]
    Material(#[from] coven_foundation::code_envelope::FixedHexError),
    #[error("activated continuation has no device signing key")]
    MissingContinuationSigner,
    #[error("Owner recovery cannot carry an activated device signer")]
    UnexpectedOwnerRecoverySigner,
}

#[derive(Debug, thiserror::Error)]
pub enum BootstrapCleanupFailure {
    #[error("store directory: {0}")]
    StoreDirectory(#[source] std::io::Error),
    #[error("master key: {0}")]
    MasterKey(#[source] KeyError),
    #[error("identity: {0}")]
    Identity(#[source] KeyError),
    #[error("cloud home credentials: {0}")]
    CloudHomeCredentials(#[source] KeyError),
}

#[derive(Debug)]
pub struct BootstrapCleanupFailures(Vec<BootstrapCleanupFailure>);

impl BootstrapCleanupFailures {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Display for BootstrapCleanupFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, failure) in self.0.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(formatter, "{failure}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BootstrapCleanupFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|failure| failure as _)
    }
}

impl From<MembershipMutationError> for BootstrapError {
    fn from(error: MembershipMutationError) -> Self {
        Self::MembershipMutation(Box::new(error))
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
                "clearing a torn bootstrap: a store directory with no saved config, left by a restore that a crash interrupted before completion"
            );
            let failures = self.remove();
            if !failures.is_empty() {
                return Err(BootstrapError::TornBootstrapCleanup {
                    store_id: store_id.to_string(),
                    failures,
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
                cleanup: failures,
                cause: Box::new(cause),
            }
        }
    }

    /// Remove every local artifact the bound bootstrap attempt may have written.
    pub(crate) fn remove(&self) -> BootstrapCleanupFailures {
        let mut failures = Vec::new();

        if let Err(error) = self.store_dir.remove_tree() {
            failures.push(BootstrapCleanupFailure::StoreDirectory(error));
        }
        if let Err(error) = self.custody.forget() {
            failures.push(BootstrapCleanupFailure::MasterKey(error));
        }
        if let Err(error) = self.identity_custody.forget() {
            failures.push(BootstrapCleanupFailure::Identity(error));
        }
        if let Err(error) = self.store_keys.delete_cloud_home_credentials() {
            failures.push(BootstrapCleanupFailure::CloudHomeCredentials(error));
        }

        BootstrapCleanupFailures(failures)
    }
}

async fn build_cloud_home_for_join(
    join_info: &CloudHomeJoinInfo,
    lib_ks: &StoreKeys,
    cloud_homes: &coven_storage::cloud::CloudHomeFactory,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
) -> Result<Arc<dyn ExactCloudHome>, BootstrapError> {
    use coven_storage::cloud::*;

    #[cfg(not(feature = "oauth-providers"))]
    let _ = (&lib_ks, cloud_homes, &oauth_tokens, &clock);
    #[cfg(feature = "oauth-providers")]
    let credential_custody =
        coven_keys::keys::CloudHomeCredentialsOwner::new(lib_ks.clone()).current();

    match join_info {
        CloudHomeJoinInfo::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            key_prefix,
        } => {
            let s3 = cloud_homes
                .open_s3(
                    bucket.clone(),
                    region.clone(),
                    endpoint.clone(),
                    access_key.clone(),
                    secret_key.clone(),
                    key_prefix.clone(),
                    exact_upload_verification,
                    clock.clone(),
                )
                .await?;
            Ok(Arc::new(s3))
        }
        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::GoogleDrive { folder_id } => {
            let tokens = oauth_tokens.ok_or_else(|| {
                BootstrapError::Provider("Google Drive join requires an OAuth token".to_string())
            })?;
            let oauth_config = cloud_homes.oauth_config_for(CloudProvider::GoogleDrive)?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                credential_custody.clone(),
                clock,
                oauth_config,
                "Google Drive",
            );
            Ok(Arc::new(google_drive::GoogleDriveCloudHome::new(
                folder_id.clone(),
                session,
                exact_upload_verification,
            )))
        }
        #[cfg(feature = "oauth-providers")]
        CloudHomeJoinInfo::Dropbox { folder_path } => {
            let tokens = oauth_tokens.ok_or_else(|| {
                BootstrapError::Provider("Dropbox join requires an OAuth token".to_string())
            })?;
            let oauth_config = cloud_homes.oauth_config_for(CloudProvider::Dropbox)?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                credential_custody.clone(),
                clock,
                oauth_config,
                "Dropbox",
            );
            Ok(Arc::new(dropbox::DropboxCloudHome::new(
                folder_path.clone(),
                session,
                exact_upload_verification,
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
            let oauth_config = cloud_homes.oauth_config_for(CloudProvider::OneDrive)?;
            let session = oauth_session::OAuthSession::new(
                tokens,
                credential_custody,
                clock,
                oauth_config,
                "OneDrive",
            );
            Ok(Arc::new(onedrive::OneDriveCloudHome::new(
                drive_id.clone(),
                folder_id.clone(),
                session,
                exact_upload_verification,
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
            Ok(Arc::new(cloudkit::CloudKitCloudHome::new_private(
                ops,
                exact_upload_verification,
            )))
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
                exact_upload_verification,
            ));
            Ok(home)
        }
    }
}

pub(crate) enum EnrollmentProviderAccess {
    Supplied(Option<coven_storage::oauth::OAuthTokens>),
    Stored,
    #[cfg(any(test, feature = "test-utils"))]
    InjectedHome,
}

#[cfg(feature = "oauth-providers")]
pub(crate) fn enrollment_oauth_tokens(
    join_info: &CloudHomeJoinInfo,
    store_keys: &StoreKeys,
    access: EnrollmentProviderAccess,
) -> Result<Option<coven_storage::oauth::OAuthTokens>, BootstrapError> {
    let provider = match join_info {
        CloudHomeJoinInfo::GoogleDrive { .. } => "Google Drive",
        CloudHomeJoinInfo::Dropbox { .. } => "Dropbox",
        CloudHomeJoinInfo::OneDrive { .. } => "OneDrive",
        CloudHomeJoinInfo::S3 { .. }
        | CloudHomeJoinInfo::CloudKit
        | CloudHomeJoinInfo::CloudKitShare { .. } => return Ok(None),
    };
    match access {
        EnrollmentProviderAccess::Supplied(Some(tokens)) => {
            store_keys.set_cloud_home_oauth_tokens(&tokens)?;
            Ok(Some(tokens))
        }
        EnrollmentProviderAccess::Supplied(None) | EnrollmentProviderAccess::Stored => store_keys
            .get_cloud_home_oauth_tokens()?
            .map(Some)
            .ok_or_else(|| {
                BootstrapError::Provider(format!(
                    "{provider} device enrollment requires OAuth authorization"
                ))
            }),
        #[cfg(any(test, feature = "test-utils"))]
        EnrollmentProviderAccess::InjectedHome => Ok(None),
    }
}

#[cfg(not(feature = "oauth-providers"))]
pub(crate) fn enrollment_oauth_tokens(
    join_info: &CloudHomeJoinInfo,
    _store_keys: &StoreKeys,
    access: EnrollmentProviderAccess,
) -> Result<Option<coven_storage::oauth::OAuthTokens>, BootstrapError> {
    match access {
        EnrollmentProviderAccess::Supplied(tokens) => drop(tokens),
        EnrollmentProviderAccess::Stored => {}
        #[cfg(any(test, feature = "test-utils"))]
        EnrollmentProviderAccess::InjectedHome => {}
    }
    match join_info {
        CloudHomeJoinInfo::GoogleDrive { .. }
        | CloudHomeJoinInfo::Dropbox { .. }
        | CloudHomeJoinInfo::OneDrive { .. } => Err(BootstrapError::Provider(
            "OAuth cloud providers are not supported in this build".to_string(),
        )),
        CloudHomeJoinInfo::S3 { .. }
        | CloudHomeJoinInfo::CloudKit
        | CloudHomeJoinInfo::CloudKitShare { .. } => Ok(None),
    }
}

/// A joining device's local half of the four-transfer admission exchange.
/// The journal lives outside the incomplete store directory, so every method
/// can be retried after process termination without losing its exact predecessor.
pub(crate) struct DeviceJoinClient {
    admission: MemberAdmission,
    member_pubkey: String,
    layout: StoreLayout,
    synced_tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
    coven_migration_policy: CovenMigrationPolicy,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    transfer_limits: coven_protocol::blob::TransferLimits,
    store_keys: StoreKeys,
    custody: Arc<dyn MasterKeyCustody>,
    identity_custody: Arc<dyn DeviceIdentityCustody>,
    cloud_homes: coven_storage::cloud::CloudHomeFactory,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
    #[cfg(any(test, feature = "test-utils"))]
    test_home: Option<Arc<dyn ExactCloudHome>>,
}

struct DeviceJoinStorage {
    storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
    keyring: MasterKeyring,
    /// The owner-anchored membership chain the keyring open already walked and
    /// verified. Installing this device's owner anchor needs the same chain, so
    /// it is kept rather than walked from the cloud a second time.
    membership: coven_protocol::membership::MembershipChain,
}

impl DeviceJoinClient {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        admission: MemberAdmission,
        member_pubkey: String,
        layout: StoreLayout,
        synced_tables: Vec<SyncedTable>,
        migrations: Vec<Migration>,
        coven_migration_policy: CovenMigrationPolicy,
        exact_upload_verification: coven_foundation::config::ExactUploadVerification,
        transfer_limits: coven_protocol::blob::TransferLimits,
        key_custody: coven_keys::custody::KeyCustody,
        identity_custody: IdentityCustody,
        oauth_clients: coven_storage::oauth::OAuthClients,
        oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
        cloudkit_ops: Option<Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
        clock: coven_foundation::clock::ClockRef,
    ) -> Result<Self, BootstrapError> {
        if admission.wrapped_key.recipient_pubkey != member_pubkey {
            return Err(crate::joining::DeviceInviteError::RecipientMismatch.into());
        }
        coven_storage::cloud::setup::require_exact_slot_capabilities_join_info(
            &admission.join_info,
            exact_upload_verification,
        )
        .map_err(|provider| BootstrapError::ExactSlotsUnavailable { provider })?;
        coven_foundation::store_dir::validate_path_token(&admission.store_id)?;
        let store_dir = layout.store_dir(&admission.store_id);
        let store_keys = StoreKeys::bind(admission.store_id.clone());
        let custody = key_custody.resolve(&store_keys, &store_dir);
        let identity_custody = identity_custody.resolve(&store_keys, &store_dir);
        Ok(Self {
            admission,
            member_pubkey,
            layout,
            synced_tables,
            migrations,
            coven_migration_policy,
            exact_upload_verification,
            transfer_limits,
            store_keys,
            custody,
            identity_custody,
            cloud_homes: coven_storage::cloud::CloudHomeFactory::new(oauth_clients),
            oauth_tokens,
            cloudkit_ops,
            clock,
            #[cfg(any(test, feature = "test-utils"))]
            test_home: None,
        })
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn with_test_bootstrap_home(mut self, home: Arc<dyn ExactCloudHome>) -> Self {
        self.test_home = Some(home);
        self
    }

    pub(crate) async fn prepare_provider_access_request(
        &self,
        offer: coven_replication::sync::DeviceJoinOffer,
    ) -> Result<coven_replication::sync::DeviceProviderAccessRequest, BootstrapError> {
        self.require_offer(&offer)?;
        let signer = coven_keys::keys::peek_pending_identity(&offer.member_pubkey)?;
        let storage: Arc<dyn coven_storage::CloudSyncObjectStorage> =
            Arc::new(self.transport_storage().await?);
        let pending = self.open_pending_journal()?;
        let observation = coven_replication::sync::store::PendingDeviceJoinObservation::open(
            &pending,
            &storage,
            &offer.store_root,
            offer.attempt_id,
        )
        .await?;
        let authority = coven_replication::sync::store::PendingDeviceJoinAuthority::open(
            observation,
            &signer,
            offer,
        )
        .await?;
        Ok(authority.prepare_provider_access_request().await?)
    }

    pub(crate) async fn accept_device_join_abandonment(
        &self,
        abandonment: coven_replication::sync::DeviceJoinAbandonment,
    ) -> Result<coven_replication::sync::DeviceJoinAbandonment, BootstrapError> {
        let storage: Arc<dyn coven_storage::CloudSyncObjectStorage> =
            Arc::new(self.transport_storage().await?);
        let pending = self.open_pending_journal()?;
        let mut observation = coven_replication::sync::store::PendingDeviceJoinObservation::open(
            &pending,
            &storage,
            &self.admission.store_root,
            abandonment.abandonment.attempt_id,
        )
        .await?;
        Ok(observation.observe_abandonment(abandonment).await?)
    }

    /// The library this join produced, when it already exists.
    ///
    /// This is the joining device's finished marker. It used to be a journal
    /// row that outlived the join it described; the config file is written by
    /// the same completion, says the same thing, and is the file the library is
    /// opened from — so the row was a second copy of it that nothing else read.
    pub(crate) fn completed_library(&self) -> Result<Option<Config>, BootstrapError> {
        let store_dir = self.layout.store_dir(&self.admission.store_id);
        if !store_dir.config_path().exists() {
            return Ok(None);
        }
        let config = Config::load_from_config_yaml(&store_dir)?;
        if config.store_id != self.admission.store_id {
            return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into());
        }
        Ok(Some(config))
    }

    pub(crate) fn device_join_status(
        &self,
        attempt_id: coven_protocol::DeviceJoinAttemptId,
    ) -> Result<Option<coven_replication::sync::DeviceJoinStatus>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(pending.status(attempt_id)?)
    }

    #[cfg(test)]
    pub(crate) fn pending_journal_records_for_test(
        &self,
    ) -> Result<
        Vec<coven_protocol::store_commit::device_join_journal::DeviceJoinJournalRecord>,
        BootstrapError,
    > {
        Ok(self.open_pending_journal()?.records()?)
    }

    #[cfg(test)]
    pub(crate) fn resume_device_joins(
        &self,
    ) -> Result<Vec<coven_replication::sync::DeviceJoinAction>, BootstrapError> {
        let pending = self.open_pending_journal()?;
        Ok(pending.actions()?)
    }

    pub(crate) async fn prepare_registration_request(
        &self,
        approval: coven_replication::sync::DeviceProviderAdmissionApproval,
    ) -> Result<coven_replication::sync::DeviceRegistrationRequest, BootstrapError> {
        let offer = &approval.request.offer;
        self.require_offer(offer)?;
        let signer = coven_keys::keys::peek_pending_identity(&offer.member_pubkey)?;
        let storage: Arc<dyn coven_storage::CloudSyncObjectStorage> =
            Arc::new(self.transport_storage().await?);
        let pending = self.open_pending_journal()?;
        let observation = coven_replication::sync::store::PendingDeviceJoinObservation::open(
            &pending,
            &storage,
            &offer.store_root,
            offer.attempt_id,
        )
        .await?;
        let mut authority = coven_replication::sync::store::PendingDeviceJoinAuthority::open(
            observation,
            &signer,
            offer.as_ref().clone(),
        )
        .await?;
        Ok(authority.prepare_registration_request(approval).await?)
    }

    pub(crate) fn record_same_principal_registration_request(
        &self,
        approval: coven_replication::sync::DeviceProviderAdmissionApproval,
    ) -> Result<coven_replication::sync::DeviceRegistrationRequest, BootstrapError> {
        let offer = approval.request.offer.as_ref().clone();
        self.require_offer(&offer)?;
        let pending = self.open_pending_journal()?;
        Ok(coven_replication::sync::store::PendingDeviceJoinAuthority::record_same_principal_registration_request(
            &pending,
            &offer,
            approval,
        )?)
    }

    /// Report the breakdown whichever way the bootstrap ends. A join that took
    /// four minutes is exactly the one whose stage timings are wanted, and a
    /// join that failed partway still reached the stages it reached.
    pub(crate) async fn bootstrap_pending_device(
        &self,
        bootstrap: coven_replication::sync::ProviderReadyDeviceBootstrap,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinReadiness,
        BootstrapError,
    > {
        // Opened before the run so the run counts through it. Opening one is
        // local, so nothing measurable happens ahead of the first stage.
        let cloud = self.build_cloud_home().await?;
        let mut timings =
            StageTimings::counting("Device join bootstrap", cloud.provider_requests());
        let outcome = Box::pin(self.bootstrap_pending_device_staged(
            cloud,
            bootstrap,
            on_progress,
            cancel,
            &mut timings,
        ))
        .await;
        timings.report();
        outcome
    }

    async fn bootstrap_pending_device_staged(
        &self,
        cloud: Arc<dyn ExactCloudHome>,
        bootstrap: coven_replication::sync::ProviderReadyDeviceBootstrap,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
        timings: &mut StageTimings,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceJoinReadiness,
        BootstrapError,
    > {
        let offer = &bootstrap.bootstrap.request.approval().request.offer;
        self.require_offer(offer)?;
        let attempt_id = bootstrap.bootstrap.publication_authorization.attempt_id;
        let pending = self.open_pending_journal()?;
        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        let signer = coven_keys::keys::peek_pending_identity(&offer.member_pubkey)?;
        let join = timings
            .stage("open Store storage", self.build_storage(cloud, &signer))
            .await?;
        let store_dir = self.layout.store_dir(&self.admission.store_id);
        if let Some(readiness) = pending.completed_joiner_readiness(attempt_id)? {
            if store_dir.db_path().exists() {
                return Ok(readiness);
            }
        }
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.admission.store_id.clone()));
        }
        store_dir.ensure_created()?;
        let db_path = store_dir.db_path();
        let history_verifier = timings
            .stage(
                "read Store root",
                coven_replication::sync::store::HistoryConstructionAuthority::for_snapshot()
                    .open_pinned(join.storage.as_ref(), &offer.store_root),
            )
            .await
            .map_err(SnapshotError::from)?;
        // Selecting the snapshot resolves membership at the admission floor on
        // this verifier, which is a second one and holds nothing the first
        // walked. The rollup is what keeps that from being a second walk.
        timings
            .stage(
                "read the membership rollup",
                history_verifier.adopt_published_membership_rollup(),
            )
            .await;
        let snapshot = timings
            .stage(
                "download snapshot",
                PreparedSnapshotBootstrap::prepare(
                    &join.storage,
                    history_verifier,
                    &self.admission.membership_floor,
                    supported_version(&self.migrations),
                    &db_path,
                    &signer,
                    std::sync::Arc::clone(on_progress),
                    cancel,
                ),
            )
            .await?;
        on_progress(coven_replication::sync::JoiningDeviceJoinProgress::InstallingSnapshot);
        let routing_encryption = EncryptionService::from(join.keyring.clone());
        let device_id = bootstrap
            .bootstrap
            .request
            .expected_registration()
            .device_id
            .to_string();
        let opened = timings
            .stage(
                "install snapshot",
                snapshot.install(
                    &store_dir,
                    self.synced_tables.clone(),
                    coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                    self.transfer_limits,
                    device_id,
                    self.clock.clone(),
                    &self.migrations,
                    self.coven_migration_policy,
                    Some(&routing_encryption),
                ),
            )
            .await?;
        let published_at = self.clock.now().to_rfc3339();
        let mut joining = timings
            .stage(
                "load membership",
                opened.begin_device_join(&pending, offer.as_ref().clone()),
            )
            .await?;
        Ok(timings
            .stage(
                "install history",
                joining.bootstrap(bootstrap, &published_at, Some(&routing_encryption)),
            )
            .await?)
    }

    pub(crate) async fn complete_device_join(
        &self,
        activation: coven_replication::sync::DeviceJoinActivation,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
    ) -> Result<Config, BootstrapError> {
        // Opened before the run so the run counts through it. Opening one is
        // local, so nothing measurable happens ahead of the first stage.
        let cloud = self.build_cloud_home().await?;
        let mut timings =
            StageTimings::counting("Device join completion", cloud.provider_requests());
        let outcome = Box::pin(self.complete_device_join_staged(
            cloud,
            activation,
            on_progress,
            &mut timings,
        ))
        .await;
        timings.report();
        outcome
    }

    async fn complete_device_join_staged(
        &self,
        cloud: Arc<dyn ExactCloudHome>,
        activation: coven_replication::sync::DeviceJoinActivation,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        timings: &mut StageTimings,
    ) -> Result<Config, BootstrapError> {
        let attempt_id = activation.attempt_id;
        let pending = self.open_pending_journal()?;
        let store_dir = self.layout.store_dir(&self.admission.store_id);
        let completed_config = if store_dir.config_path().exists() {
            Some(Config::load_from_config_yaml(&store_dir)?)
        } else {
            None
        };
        if completed_config
            .as_ref()
            .is_some_and(|config| config.store_id != self.admission.store_id)
        {
            return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into());
        }
        let signer = match completed_config.as_ref() {
            Some(_) => coven_keys::keys::require_identity(self.identity_custody.as_ref())?,
            None => coven_keys::keys::peek_pending_identity(&self.member_pubkey)?,
        };
        let join = timings
            .stage("open Store storage", self.build_storage(cloud, &signer))
            .await?;
        let pending_readiness = pending.observe_joiner_activation_if_pending(&activation)?;
        let device_id = match (pending_readiness.as_ref(), completed_config.as_ref()) {
            (Some(readiness), _) => readiness.proof.registration.device_id.to_string(),
            (None, Some(config)) => config.device_id.clone(),
            (None, None) => {
                return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into())
            }
        };
        let db_path = store_dir.db_path();
        let db = Database::open(
            &db_path,
            self.synced_tables.clone(),
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            self.transfer_limits,
            device_id.clone(),
            self.clock.clone(),
            self.coven_migration_policy,
            &self.migrations,
        )?;
        let database = coven_database::StoreDatabase::from_database(db.clone());
        let routing_encryption = EncryptionService::from(join.keyring.clone());
        let observation = timings
            .stage(
                "read Store root",
                coven_replication::sync::store::PendingDeviceJoinObservation::open(
                    &pending,
                    &join.storage,
                    &self.admission.store_root,
                    attempt_id,
                ),
            )
            .await?;
        let mut joining = timings
            .stage(
                "load membership",
                observation.into_joining_store(
                    database,
                    &store_dir,
                    signer.clone(),
                    Some(join.membership.clone()),
                ),
            )
            .await?;
        on_progress(coven_replication::sync::JoiningDeviceJoinProgress::CatchingUp);
        timings
            .stage(
                "pull history",
                joining.pull_store_history(Some(&routing_encryption)),
            )
            .await?;
        let joined = timings
            .stage(
                "materialize activation",
                joining.materialize(activation.clone()),
            )
            .await?;
        if pending_readiness
            .as_ref()
            .is_some_and(|readiness| joined.registration != readiness.proof.registration)
            || joined.registration.device_id.to_string() != device_id
        {
            return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into());
        }
        on_progress(coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary);
        self.custody.persist(&join.keyring)?;
        self.identity_custody.establish(&signer)?;
        if let Some(credentials) = derive_credentials(&self.admission.join_info) {
            self.store_keys.set_cloud_home_credentials(&credentials)?;
        }
        let cipher = CloudCipher::Encrypted(join.keyring.clone().into());
        let mut config = super::build_config(
            &self.admission.store_id,
            &device_id,
            &self.admission.store_name,
            &self.admission.join_info,
            &cipher,
        );
        config.cloud_home.exact_upload_verification = self.exact_upload_verification;
        config.save_to_config_yaml(&store_dir)?;
        timings
            .stage("close join journal", joining.complete(activation))
            .await?;
        coven_keys::keys::discard_pending_identity(&self.member_pubkey)?;
        info!(store_id = %self.admission.store_id, "joined Store device");
        Ok(config)
    }

    pub(crate) async fn install_same_principal_device_join(
        &self,
        join: coven_replication::sync::SamePrincipalDeviceJoin,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
    ) -> Result<Config, BootstrapError> {
        // Opened before the run so the run counts through it. Opening one is
        // local, so nothing measurable happens ahead of the first stage.
        let cloud = self.build_cloud_home().await?;
        let mut timings =
            StageTimings::counting("Same-provider device join", cloud.provider_requests());
        let outcome = Box::pin(self.install_same_principal_device_join_staged(
            cloud,
            join,
            on_progress,
            cancel,
            &mut timings,
        ))
        .await;
        timings.report();
        outcome
    }

    async fn install_same_principal_device_join_staged(
        &self,
        cloud: Arc<dyn ExactCloudHome>,
        join: coven_replication::sync::SamePrincipalDeviceJoin,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
        timings: &mut StageTimings,
    ) -> Result<Config, BootstrapError> {
        join.verify_shape()
            .map_err(coven_replication::sync::DeviceJoinError::from)?;
        let offer = &join.bootstrap.bootstrap.request.approval().request.offer;
        self.require_offer(offer)?;
        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        let pending = self.open_pending_journal()?;
        let store_dir = self.layout.store_dir(&self.admission.store_id);
        if store_dir.config_path().exists() {
            return Err(BootstrapError::StoreExists(self.admission.store_id.clone()));
        }
        let signer = coven_keys::keys::peek_pending_identity(&offer.member_pubkey)?;
        let storage = timings
            .stage("open Store storage", self.build_storage(cloud, &signer))
            .await?;
        store_dir.ensure_created()?;
        let prepared = timings
            .stage(
                "download snapshot",
                PreparedDeviceJoinSnapshot::prepare(
                    &storage.storage,
                    (*join.installation).clone(),
                    supported_version(&self.migrations),
                    &store_dir.db_path(),
                    on_progress,
                    cancel,
                ),
            )
            .await?;
        if *cancel.borrow() {
            return Err(BootstrapError::Cancelled);
        }
        on_progress(coven_replication::sync::JoiningDeviceJoinProgress::InstallingSnapshot);
        let routing_encryption = EncryptionService::from(storage.keyring.clone());
        let device_id = join
            .bootstrap
            .bootstrap
            .request
            .expected_registration()
            .device_id
            .to_string();
        let installed = timings
            .stage("install snapshot", async {
                prepared.install(
                    self.synced_tables.clone(),
                    coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                    self.transfer_limits,
                    device_id.clone(),
                    self.clock.clone(),
                    &self.migrations,
                    self.coven_migration_policy,
                    &routing_encryption,
                )
            })
            .await?;
        let completion = timings.stage("install history", coven_replication::sync::store::PendingDeviceJoinAuthority::prepare_same_principal_completion(
            &pending,
            &storage.storage,
            &store_dir,
            &signer,
            join,
            installed,
            &self.clock.now().to_rfc3339(),
            Some(&routing_encryption),
            Some(storage.membership.clone()),
        ))
        .await?;
        if completion.joined().registration.device_id.to_string() != device_id {
            return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into());
        }
        on_progress(coven_replication::sync::JoiningDeviceJoinProgress::SavingLibrary);
        self.custody.persist(&storage.keyring)?;
        self.identity_custody.establish(&signer)?;
        if let Some(credentials) = derive_credentials(&self.admission.join_info) {
            self.store_keys.set_cloud_home_credentials(&credentials)?;
        }
        let cipher = CloudCipher::Encrypted(storage.keyring.clone().into());
        let mut config = super::build_config(
            &self.admission.store_id,
            &device_id,
            &self.admission.store_name,
            &self.admission.join_info,
            &cipher,
        );
        config.cloud_home.exact_upload_verification = self.exact_upload_verification;
        config.save_to_config_yaml(&store_dir)?;
        timings
            .stage("close join journal", completion.complete())
            .await?;
        coven_keys::keys::discard_pending_identity(&self.member_pubkey)?;
        info!(store_id = %self.admission.store_id, "joined Store device");
        Ok(config)
    }

    fn require_offer(
        &self,
        offer: &coven_replication::sync::DeviceJoinOffer,
    ) -> Result<(), BootstrapError> {
        if offer.store_root != self.admission.store_root
            || offer.member_pubkey != self.member_pubkey
        {
            return Err(coven_replication::sync::DeviceJoinError::OfferMismatch.into());
        }
        Ok(())
    }

    fn open_pending_journal(
        &self,
    ) -> Result<coven_replication::sync::DeviceJoinJournalDatabase, BootstrapError> {
        let directory = self.layout.stores_root().join(".pending-device-joins");
        Ok(coven_replication::sync::DeviceJoinJournalDatabase::open(
            directory.join(format!("{}.sqlite", self.admission.store_id)),
        )?)
    }

    /// Open the home this join reads through, counting every operation asked
    /// of it.
    ///
    /// A join opens two storages over this one home — a plaintext one to pin
    /// the Store root and walk the membership chain, then an encrypted one for
    /// everything after — so counting here rather than per storage is what puts
    /// the whole join's operations in one running total. The home a test
    /// supplies is left alone and counts nothing.
    async fn build_cloud_home(&self) -> Result<Arc<dyn ExactCloudHome>, BootstrapError> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(home) = &self.test_home {
            return Ok(home.clone());
        }
        let home = build_cloud_home_for_join(
            &self.admission.join_info,
            &self.store_keys,
            &self.cloud_homes,
            self.oauth_tokens.clone(),
            self.cloudkit_ops.clone(),
            self.clock.clone(),
            self.exact_upload_verification,
        )
        .await?;
        Ok(Arc::new(coven_storage::cloud::CountingCloudHome::new(home)))
    }

    /// Plaintext storage over the joining device's cloud home.
    ///
    /// Both the pre-key bootstrap reads and the device-join transport go
    /// through this: the transport's objects carry their own per-attempt seal,
    /// so they need no store key — which is what lets a joiner publish its
    /// access request before it has unwrapped the store keyring at all.
    pub(super) async fn transport_storage(&self) -> Result<CloudSyncConnection, BootstrapError> {
        let signer = coven_keys::keys::peek_pending_identity(&self.member_pubkey)?;
        let cloud = self.build_cloud_home().await?;
        self.plaintext_storage(cloud, &signer)
    }

    fn plaintext_storage(
        &self,
        home: Arc<dyn ExactCloudHome>,
        signer: &UserKeypair,
    ) -> Result<CloudSyncConnection, BootstrapError> {
        Ok(CloudSyncConnection::new(
            home,
            CloudCipher::Plaintext,
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.admission.store_id.clone(),
            signer.clone(),
        ))
    }

    /// Unwrap the store keyring over the home this join already opened.
    ///
    /// Timed as one step by its callers, where it has been the second-largest
    /// on a live join. Constructing the home is local — no bucket check, no
    /// auth probe — and its caller does it, so all of this time is the two
    /// reads that pin the Store root and its founder, the membership rollup,
    /// the membership chain walk, and the wrapped-key reads.
    ///
    /// The walk used to grow with the Store's whole membership history: a
    /// listing and a read per head, then a read per entry, back to the founding
    /// entry. It now takes everything up to the newest published snapshot's
    /// membership frontier from that snapshot's rollup, in one read, and walks
    /// the provider only for what was published after it — so what is left is
    /// the probe that finds each stream's end, and the tail itself.
    async fn build_storage(
        &self,
        cloud: Arc<dyn ExactCloudHome>,
        signer: &UserKeypair,
    ) -> Result<DeviceJoinStorage, BootstrapError> {
        let mut timings =
            StageTimings::counting("Device join Store storage", cloud.provider_requests());
        let outcome = Box::pin(self.build_storage_staged(cloud, signer, &mut timings)).await;
        timings.report();
        outcome
    }

    async fn build_storage_staged(
        &self,
        cloud: Arc<dyn ExactCloudHome>,
        signer: &UserKeypair,
        timings: &mut StageTimings,
    ) -> Result<DeviceJoinStorage, BootstrapError> {
        let bootstrap_storage = self.plaintext_storage(cloud.clone(), signer)?;
        let recipient = hex::encode(signer.public_key());
        if self.admission.wrapped_key.recipient_pubkey != recipient {
            return Err(
                coven_replication::sync::store::MembershipMutationError::Crypto(
                    "admission wrapped-key ref names another recipient".to_string(),
                )
                .into(),
            );
        }
        self.admission
            .membership_floor
            .validate()
            .map_err(coven_replication::sync::store::MembershipMutationError::MembershipFloor)?;
        let mut history = timings
            .stage(
                "pin the Store root",
                coven_replication::sync::store::HistoryConstructionAuthority::admission()
                    .open_pinned(&bootstrap_storage, &self.admission.store_root),
            )
            .await
            .map_err(coven_replication::sync::store::MembershipMutationError::from)?;
        // What the walk below would otherwise fetch two round trips at a time,
        // in one read. Advisory: a Store with no published rollup, or one that
        // does not authenticate, leaves the walk exactly as it was.
        timings
            .stage(
                "read the membership rollup",
                history.adopt_published_membership_rollup(),
            )
            .await;
        let chain = timings
            .stage(
                "walk the membership chain",
                history.load_exact_anchored_membership(
                    &self.admission.membership_floor.0,
                    Some(&self.admission.owner_pubkey),
                ),
            )
            .await
            .map_err(coven_replication::sync::store::MembershipMutationError::from)?;
        let encryption = timings
            .stage(
                "open the keyring",
                coven_replication::sync::store::StoreKeyrings::new(
                    &bootstrap_storage,
                    self.admission.store_root.clone(),
                )
                .open_containing(signer, &chain, &self.admission.wrapped_key),
            )
            .await?;
        let keyring = MasterKeyring::from(encryption.clone());
        let storage = CloudSyncConnection::new(
            cloud,
            CloudCipher::Encrypted(encryption),
            BlobPathScheme::for_storage(HomeStorage::Opaque),
            self.admission.store_id.clone(),
            signer.clone(),
        );
        Ok(DeviceJoinStorage {
            storage: Arc::new(storage),
            keyring,
            membership: chain,
        })
    }
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

#[cfg(test)]
#[path = "client_tests.rs"]
mod tests;
