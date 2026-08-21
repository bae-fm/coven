/// Shared test helpers for sync module tests.
///
/// These drive a real [`Database`] over an in-memory connection carrying the
/// synthetic test schema, so tests exercise the engine through the same path
/// production does.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use coven_database::Database;
use coven_foundation::store_dir::StoreDir;
use coven_keys::encryption::MasterKeyring;
use coven_keys::keys::{KeyError, MasterKeyCustody, UserKeypair};
#[cfg(test)]
use coven_protocol::store_commit::ObjectHash;
use coven_storage::CloudSyncObjectStorage;

/// The synthetic store's schema and `Database` constructors, which the database
/// layer owns and its own tests open directly.
pub use coven_database::synthetic_store::*;
pub use coven_foundation::store_dir::temp_store_dir;
pub use coven_storage::cloud::test_utils::{test_cloud_home, test_cloud_home_with_binding};

pub fn staged_snapshot_image(bytes: &[u8]) -> coven_database::SnapshotDatabaseImage {
    let file = tempfile::NamedTempFile::new().expect("create staged snapshot fixture path");
    let path = file.path().to_path_buf();
    file.close().expect("release staged snapshot fixture path");
    coven_database::SnapshotDatabaseImage::create(path, bytes)
        .expect("write staged snapshot fixture")
}

#[cfg(test)]
pub fn test_cache_locator_hash(label: &str) -> ObjectHash {
    ObjectHash::digest(label.as_bytes())
}

/// In-memory [`MasterKeyCustody`] for tests, with a switch to force `persist`
/// to fail. The switch models a device whose keyring is momentarily
/// unwritable, so a test can drive a key adoption into its failure path and then
/// clear the switch to prove the retry converges. Stores the serialized form
/// (like the real `Keyring` preset), so `stored_key` reflects exactly what a
/// caller wrote.
#[derive(Clone, Default)]
pub struct TestCustody {
    value: Arc<Mutex<Option<String>>>,
    fail: Arc<std::sync::atomic::AtomicBool>,
}

impl TestCustody {
    pub fn set_initial_key(&self, key: [u8; 32]) {
        *self.value.lock().unwrap() = Some(
            MasterKeyring::from(coven_keys::encryption::EncryptionService::from_key(key))
                .to_serialized(),
        );
    }

    pub fn stored_key(&self) -> Option<String> {
        self.value.lock().unwrap().clone()
    }

    /// Make the next and every subsequent `persist` fail until cleared.
    pub fn fail_writes(&self) {
        self.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Let `persist` succeed again.
    pub fn allow_writes(&self) {
        self.fail.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MasterKeyCustody for TestCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        self.value
            .lock()
            .unwrap()
            .as_deref()
            .map(MasterKeyring::from_serialized)
            .transpose()
            .map_err(KeyError::Encryption)
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KeyError::Custody {
                operation: "persist",
                source: Box::new(std::io::Error::other("forced keyring write failure")),
            });
        }
        *self.value.lock().unwrap() = Some(keyring.to_serialized());
        Ok(())
    }

    fn forget(&self) -> Result<(), KeyError> {
        *self.value.lock().unwrap() = None;
        Ok(())
    }
}

/// Copy the file-backed payloads one store directory holds into another.
///
/// A store is a directory, not a file: rows name payload files beside the
/// database, so a test that copies the database with `VACUUM INTO` and opens the
/// copy has to bring those files along, exactly as a device carries its whole
/// store directory rather than one file out of it.
pub fn copy_payload_files(
    from: &coven_foundation::store_dir::StoreDir,
    to: &coven_foundation::store_dir::StoreDir,
) {
    let source = from.payload_spool_dir();
    let destination = to.payload_spool_dir();
    match std::fs::metadata(&source) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => panic!("payload file path is not a directory: {}", source.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!(
            "inspect payload file directory {}: {error}",
            source.display()
        ),
    }
    std::fs::create_dir_all(&destination).expect("create the copied payload spool directory");
    for entry in std::fs::read_dir(&source).expect("read the payload spool being copied") {
        let entry = entry.expect("payload spool entry");
        std::fs::copy(entry.path(), destination.join(entry.file_name()))
            .expect("copy one payload into the copied store directory");
    }
}

/// Hex-encoded ed25519 public key, as membership entries and the wrapped-key
/// store identify a member.
pub fn pubkey_hex(kp: &UserKeypair) -> String {
    coven_keys::keys::public_key_hex(kp)
}

/// Ed25519 identity derived from exact test-owned seed bytes.
pub fn user_keypair_from_seed(seed: [u8; 32]) -> UserKeypair {
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed);
    UserKeypair::from_signing_key_bytes(&signing_key.to_keypair_bytes())
        .expect("seed-derived signing key is valid")
}

/// Grants a Dropbox shared-folder membership to whichever peer account asks —
/// the provider-side step a cross-principal admission needs before the joining
/// device can write to the store's namespace.
pub struct TestDropboxAccessAdministrator {
    pub namespace_id: String,
}

#[async_trait::async_trait]
impl crate::sync::store::DeviceProviderAccessAdministrator for TestDropboxAccessAdministrator {
    async fn grant_member_access(
        &self,
        _member_pubkey: &str,
        _provider_account_email: Option<&str>,
        peer: &coven_protocol::objects::ProviderDeviceBinding,
    ) -> Result<coven_protocol::provider::ProviderAccessLocator, crate::sync::store::DeviceJoinError>
    {
        let coven_protocol::objects::ProviderPrincipalId::Dropbox { account_id } = &peer.principal
        else {
            return Err(crate::sync::store::DeviceJoinError::Provider(
                "test Dropbox access administrator received a non-Dropbox peer".to_string(),
            ));
        };
        Ok(
            coven_protocol::provider::ProviderAccessLocator::DropboxSharedFolderMember {
                namespace_id: self.namespace_id.clone(),
                account_id: account_id.clone(),
            },
        )
    }
}

pub struct CrossPrincipalTestDevice {
    storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage>,
    access_administrator: TestDropboxAccessAdministrator,
}

impl CrossPrincipalTestDevice {
    pub async fn pending_device_join_observation(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        root: &coven_protocol::store_commit::StoreRootRef,
        attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
    ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, TestError> {
        crate::sync::store::PendingDeviceJoinObservation::open(
            pending,
            &self.storage,
            root,
            attempt_id,
        )
        .await
        .map_err(TestError::from)
    }

    /// Download and install the newest Store snapshot this joining device may
    /// install, into `store_dir`, through its own provider access.
    ///
    /// The step production runs before it asks for the history published after
    /// that snapshot. Skipping it leaves the joining device asking for the
    /// closure back to genesis, which means a package per commit — and a store
    /// that reclaims has deleted the ones its snapshot restates.
    #[cfg(test)]
    pub async fn install_store_snapshot<'a>(
        &'a self,
        store_dir: &'a coven_foundation::store_dir::StoreDir,
        root: &coven_protocol::store_commit::StoreRootRef,
        membership: &coven_protocol::membership::MembershipChain,
        identity: &UserKeypair,
        device_id: String,
        binary_schema_version: u32,
        synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        migrations: &[coven_database::Migration],
    ) -> Result<crate::sync::store::RestoringStore<'a>, TestError> {
        let history_verifier = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
            .open_pinned(self.storage.as_ref(), root)
            .await
            .map_err(crate::sync::store::SnapshotError::from)?;
        let cancel = tokio::sync::watch::channel(false).1;
        store_dir.ensure_created()?;
        Ok(crate::sync::store::PreparedSnapshotBootstrap::prepare(
            &self.storage,
            history_verifier,
            &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
            binary_schema_version,
            &store_dir.db_path(),
            identity,
            std::sync::Arc::new(|_| {}),
            &cancel,
        )
        .await?
        .install(
            store_dir,
            synced_tables,
            coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
            coven_protocol::blob::TransferLimits::one_at_a_time(),
            device_id,
            std::sync::Arc::new(coven_foundation::clock::SystemClock),
            migrations,
            None,
        )
        .await?)
    }

    pub async fn open_pending_device_join(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        identity: &UserKeypair,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, TestError> {
        let observation = self
            .pending_device_join_observation(pending, &offer.store_root, offer.attempt_id)
            .await?;
        crate::sync::store::PendingDeviceJoinAuthority::open(observation, identity, offer)
            .await
            .map_err(TestError::from)
    }

    pub async fn authorize_device_provider_access(
        &self,
        owner: &TestDevice,
        request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
        crate::sync::DeviceJoinError,
    > {
        owner
            .authorize_device_provider_access(request, Some(&self.access_administrator))
            .await
    }
}

pub struct TestStore {
    home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    /// Every provider operation this Store's devices have asked for, counted at
    /// the same boundary a shipped home counts them.
    provider_requests: Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>>,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    root: coven_protocol::store_commit::StoreRootRef,
    signer: UserKeypair,
    founder: TestDevice,
    producers: Arc<tokio::sync::Mutex<TestStoreProducers>>,
}

pub type TestStoreParts = (Arc<TestStore>, Arc<coven_storage::CloudSyncConnection>);

#[derive(Debug)]
pub struct TestError(Box<TestErrorCause>);

impl TestError {
    pub(crate) fn invariant(message: impl Into<String>) -> Self {
        Self(Box::new(TestErrorCause::Invariant(message.into())))
    }

    #[cfg(test)]
    pub(crate) fn initialization_source(
        &self,
    ) -> Option<&crate::sync::store::StoreInitializationError> {
        match self.0.as_ref() {
            TestErrorCause::Initialization(error) => Some(error),
            _ => None,
        }
    }
}

impl std::fmt::Display for TestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for TestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[derive(Debug, thiserror::Error)]
enum TestErrorCause {
    #[error("test fixture invariant failed: {0}")]
    Invariant(String),
    #[error(transparent)]
    Database(#[from] coven_database::DbError),
    #[error(transparent)]
    HostWrite(#[from] coven_database::HostWriteError<coven_database::DbError>),
    #[error(transparent)]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error(transparent)]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error(transparent)]
    Initialization(#[from] crate::sync::store::StoreInitializationError),
    #[error(transparent)]
    Store(#[from] crate::sync::store::StoreError),
    #[error(transparent)]
    Registration(#[from] crate::sync::store::StoreRegistrationError),
    #[error(transparent)]
    WriterAuthorization(#[from] crate::sync::store::StoreWriterAuthorizationError),
    #[error(transparent)]
    Pull(#[from] crate::sync::store::StorePullError),
    #[error(transparent)]
    Cycle(#[from] crate::sync::cycle::SyncCycleFailure),
    #[error(transparent)]
    SyncInitialization(#[from] crate::sync::cycle::InitSyncError),
    #[error(transparent)]
    Membership(#[from] crate::sync::store::MembershipOpsError),
    #[error(transparent)]
    MembershipMutation(#[from] crate::sync::store::MembershipMutationError),
    #[error(transparent)]
    DeviceJoin(#[from] crate::sync::store::DeviceJoinError),
    #[error(transparent)]
    OwnerPromotion(#[from] crate::sync::store::OwnerPromotionError),
    #[error(transparent)]
    Snapshot(#[from] crate::sync::store::SnapshotError),
    #[error(transparent)]
    Acknowledgement(#[from] crate::sync::store::StoreAckError),
    #[error(transparent)]
    PublishedBlobDrop(#[from] crate::sync::store::blob::PublishedBlobDropError),
    #[error(transparent)]
    Encryption(#[from] coven_keys::encryption::EncryptionError),
    #[error(transparent)]
    Key(#[from] coven_keys::keys::KeyError),
    #[error(transparent)]
    File(#[from] coven_foundation::atomic_file::FileError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TestPull(#[from] TestPullError),
    #[error(transparent)]
    DatabaseOpen(#[from] coven_database::OpenError),
}

macro_rules! test_error_from {
    ($source:ty, $variant:ident) => {
        impl From<$source> for TestError {
            fn from(source: $source) -> Self {
                Self(Box::new(TestErrorCause::$variant(source)))
            }
        }
    };
}

test_error_from!(coven_database::DbError, Database);
test_error_from!(
    coven_database::HostWriteError<coven_database::DbError>,
    HostWrite
);
test_error_from!(coven_protocol::objects::StorageError, Storage);
test_error_from!(coven_protocol::store_commit::StoreProtocolError, Protocol);
test_error_from!(crate::sync::store::StoreInitializationError, Initialization);
test_error_from!(crate::sync::store::StoreError, Store);
test_error_from!(crate::sync::store::StoreRegistrationError, Registration);
test_error_from!(
    crate::sync::store::StoreWriterAuthorizationError,
    WriterAuthorization
);
test_error_from!(crate::sync::store::StorePullError, Pull);
test_error_from!(crate::sync::cycle::SyncCycleFailure, Cycle);
test_error_from!(crate::sync::cycle::InitSyncError, SyncInitialization);
test_error_from!(crate::sync::store::MembershipOpsError, Membership);
test_error_from!(
    crate::sync::store::MembershipMutationError,
    MembershipMutation
);
test_error_from!(crate::sync::store::DeviceJoinError, DeviceJoin);
test_error_from!(crate::sync::store::OwnerPromotionError, OwnerPromotion);
test_error_from!(crate::sync::store::SnapshotError, Snapshot);
test_error_from!(crate::sync::store::StoreAckError, Acknowledgement);
test_error_from!(
    crate::sync::store::blob::PublishedBlobDropError,
    PublishedBlobDrop
);
test_error_from!(coven_keys::encryption::EncryptionError, Encryption);
test_error_from!(coven_keys::keys::KeyError, Key);
test_error_from!(coven_foundation::atomic_file::FileError, File);
test_error_from!(serde_json::Error, Json);
test_error_from!(std::io::Error, Io);
test_error_from!(TestPullError, TestPull);
test_error_from!(coven_database::OpenError, DatabaseOpen);

/// Why a test pull did not produce a result. Keeps the three steps a test pull
/// runs — opening the store, authorizing the writer, running the cycle — apart,
/// so a test asserting on one of them cannot pass on another.
#[derive(Debug, thiserror::Error)]
pub enum TestPullError {
    #[error("open Store: {0}")]
    Open(#[source] crate::sync::store::StoreInitializationError),
    #[error("authorize Store writer: {0}")]
    Authorize(#[from] crate::sync::store::StoreWriterAuthorizationError),
    #[error("pull: {0}")]
    Pull(#[from] crate::sync::cycle::SyncCycleFailure),
}

mod test_device {
    use super::*;

    pub struct TestDeviceSigningAuthority {
        registration: coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
        device_signer: UserKeypair,
    }

    impl TestDeviceSigningAuthority {
        pub fn registration_ref(
            &self,
        ) -> &coven_protocol::store_commit::StoreDeviceRegistrationRef {
            self.registration.reference()
        }

        pub fn registration(&self) -> &coven_protocol::store_commit::StoreDeviceRegistration {
            self.registration.value()
        }

        pub fn referenced_registration(
            &self,
        ) -> &coven_protocol::store_commit::ReferencedStoreDeviceRegistration {
            &self.registration
        }

        #[allow(clippy::too_many_arguments)]
        pub fn sign_device_join_attempt_for_test(
            &self,
            store_root: coven_protocol::store_commit::StoreRootRef,
            attempt_id: coven_protocol::store_commit::DeviceJoinAttemptId,
            attempt_slot: coven_protocol::objects::ObjectSlot,
            expected_registration: coven_protocol::store_commit::StoreDeviceRegistration,
            registration_slot: coven_protocol::objects::ObjectSlot,
            outcome_slot: coven_protocol::objects::ObjectSlot,
            bootstrap_cut: coven_protocol::store_commit::StoreHistoryCut,
            membership: coven_protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
            provider_approval: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
            provider_response: coven_protocol::store_commit::device_join_exchange::DeviceProviderResponseReservation,
            owner_grant: coven_protocol::membership::MembershipGrantId,
        ) -> Result<
            coven_protocol::store_commit::DeviceJoinAttempt,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::store_commit::DeviceJoinAttempt::signed(
                store_root,
                attempt_id,
                attempt_slot,
                expected_registration,
                registration_slot,
                outcome_slot,
                bootstrap_cut,
                membership,
                provider_admin_grant,
                provider_approval,
                provider_response,
                self.registration.reference().clone(),
                owner_grant,
                self.registration.value(),
                &self.device_signer,
            )
        }

        pub fn sign_provider_admission_approval_without_shape_validation_for_test(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
            admission: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission,
        ) -> coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval
        {
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval::signed_without_shape_validation_for_test(
                request,
                admission,
                &self.device_signer,
            )
        }

        pub fn sign_device_head_for_test(
            &self,
            store_root_hash: coven_protocol::store_commit::ObjectHash,
            commit: coven_protocol::store_commit::StoreBatchCommitRef,
            successor: coven_protocol::store_commit::SuccessorLink,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceHead,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::store_commit::StoreDeviceHead::signed(
                store_root_hash,
                self.registration.reference().clone(),
                commit,
                successor,
                &self.device_signer,
            )
        }

        pub fn sign_reclaim_receipt_for_test(
            &self,
            store_root_hash: coven_protocol::store_commit::ObjectHash,
            authorization: coven_protocol::reclaim::ReclaimAuthorizationRef,
            provider_admin_state: coven_protocol::circle_control::StoreMembershipStateRef,
            provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId,
        ) -> Result<
            coven_protocol::reclaim::ReclaimReceipt,
            coven_protocol::store_commit::StoreProtocolError,
        > {
            coven_protocol::reclaim::ReclaimReceipt::signed(
                store_root_hash,
                authorization,
                provider_admin_state,
                provider_admin_grant,
                self.registration.reference().clone(),
                self.registration.value(),
                &self.device_signer,
            )
        }
    }

    #[derive(Clone)]
    pub struct TestDevice {
        db: coven_database::StoreDatabase,
        store: std::sync::Arc<crate::sync::store::Store>,
        store_dir: StoreDir,
        device_id: String,
        storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
        identity: UserKeypair,
        /// One per device, for the device's life — the shape a sync loop has,
        /// so repeated cycles here cost what repeated cycles cost there.
        settled: std::sync::Arc<crate::sync::store::SettledCycle>,
    }

    impl TestDevice {
        /// Replay this device's retained history and count `table`'s rows.
        ///
        /// The device performs the replay with the database it owns rather than
        /// handing that database out, so a caller checking a replay never gets
        /// a handle it could write through.
        /// Answer a read-only query against this device's Store, for a test
        /// that never holds the database — a device installed by a join or a
        /// restore is opened by that install, not by its caller.
        pub async fn query_test_text(&self, sql: &str) -> String {
            self.db
                .test_query_optional_text(sql.to_string())
                .await
                .expect("test text query failed")
                .expect("test text query matched no row")
        }

        pub async fn test_row_exists(&self, sql: &str) -> bool {
            self.db
                .test_query_optional_text(format!("SELECT 'found' FROM ({sql}) LIMIT 1"))
                .await
                .expect("test row-existence query failed")
                .is_some()
        }

        pub async fn latest_local_store_device_registration(
            &self,
        ) -> Result<Option<coven_database::DurableDeviceRegistration>, coven_database::DbError>
        {
            self.db.latest_local_store_device_registration().await
        }

        pub async fn replay_row_count_for_test(
            &self,
            table: &str,
        ) -> Result<i64, coven_database::DbError> {
            self.db
                .replay_row_count_for_test(
                    self.store.root_ref_for_test().clone(),
                    table.to_string(),
                )
                .await
        }

        pub fn device_id(&self) -> String {
            self.device_id.clone()
        }

        pub async fn create(
            db: &Database,
            store_dir: StoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreInitializationError> {
            Self::create_with_database(
                coven_database::StoreDatabase::new(db),
                store_dir,
                storage,
                founder_timestamp,
                identity,
            )
            .await
        }

        pub async fn create_with_database(
            database: coven_database::StoreDatabase,
            store_dir: StoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            founder_timestamp: &str,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreInitializationError> {
            database.assert_owns_payload_directory_for_test(&store_dir);
            let initialized = crate::sync::store::Store::create(
                database.clone(),
                storage.clone(),
                store_dir.clone(),
                founder_timestamp,
                &identity,
            )
            .await?;
            let (store, device_id) = initialized.into_parts();
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(store),
                store_dir,
                device_id,
                storage,
                identity,
                settled: std::sync::Arc::default(),
            })
        }

        pub async fn open_with_database(
            database: coven_database::StoreDatabase,
            store_dir: StoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            root: &coven_protocol::store_commit::StoreRootRef,
            identity: &UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreInitializationError> {
            database.assert_owns_payload_directory_for_test(&store_dir);
            let initialized = crate::sync::store::Store::open(
                database.clone(),
                storage.clone(),
                store_dir.clone(),
                root,
                identity,
            )
            .await?;
            let (store, device_id) = initialized.into_parts();
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(store),
                store_dir,
                device_id,
                storage,
                identity: identity.clone(),
                settled: std::sync::Arc::default(),
            })
        }

        pub async fn activate_joined(
            observer: Self,
            joining_database: coven_database::StoreDatabase,
            joining_store_dir: StoreDir,
            joining_identity: &UserKeypair,
            published_at: &str,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
        ) -> Result<Self, TestError> {
            let activated_database = joining_database.clone();
            observer.ensure_device_join_snapshot_for_test().await?;
            let pending_dir = tempfile::tempdir()?;
            let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
                pending_dir.path().join("pending-device-join.sqlite"),
            )?;
            let offer = observer
                .begin_device_join(&pubkey_hex(joining_identity))
                .await?;
            let mut pending_join = observer
                .open_pending_device_join_for_test(&pending, joining_identity, offer)
                .await?;
            let access_request = pending_join.prepare_provider_access_request().await?;
            let approval = observer
                .authorize_device_provider_access(access_request, None)
                .await?;
            let registration_request = pending_join.prepare_registration_request(approval).await?;
            let join = observer
                .activate_same_principal_join_for_test(registration_request)
                .await?;
            let mut joining = pending_join
                .begin_joining_store(joining_database, &joining_store_dir)
                .await?;
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let bootstrap_pull = joining
                .pull_store_history(Some(&routing_encryption))
                .await?;
            if !bootstrap_pull.held_positions.is_empty() {
                return Err(TestError::invariant(format!(
                    "device join bootstrap pull held signed positions: {:?}",
                    bootstrap_pull.held_positions
                )));
            }
            joining
                .bootstrap(
                    join.bootstrap.clone(),
                    published_at,
                    Some(&routing_encryption),
                )
                .await?;
            joining.complete(join.activation).await?;
            Self::load_with_database(
                activated_database,
                storage,
                joining_identity.clone(),
                joining_store_dir,
            )
            .await
            .map_err(TestError::from)
        }

        /// Join a device the way production does: install the owner's newest
        /// snapshot, then carry only the history published after it.
        ///
        /// [`activate_joined`](Self::activate_joined) pulls the whole history
        /// into an empty database instead, which leaves the joining device on a
        /// genesis replay baseline holding — and pinning for replay — every
        /// commit back to the beginning of the store. A store that reclaims has
        /// deleted the packages its snapshot restates, so a device joined that
        /// way cannot pull past the first reclaim it meets.
        #[cfg(test)]
        #[allow(clippy::too_many_arguments)]
        pub async fn activate_joined_from_snapshot(
            observer: Self,
            joining_store_dir: StoreDir,
            joining_identity: &UserKeypair,
            published_at: &str,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
            migrations: Vec<coven_database::Migration>,
            binary_schema_version: u32,
        ) -> Result<Self, TestError> {
            observer.ensure_device_join_snapshot_for_test().await?;
            let pending_dir = tempfile::tempdir()?;
            let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
                pending_dir.path().join("pending-device-join.sqlite"),
            )?;
            let offer = observer
                .begin_device_join(&pubkey_hex(joining_identity))
                .await?;
            let mut pending_join = observer
                .open_pending_device_join_for_test(&pending, joining_identity, offer.clone())
                .await?;
            let access_request = pending_join.prepare_provider_access_request().await?;
            let approval = observer
                .authorize_device_provider_access(access_request, None)
                .await?;
            let registration_request = pending_join.prepare_registration_request(approval).await?;
            let join = observer
                .activate_same_principal_join_for_test(registration_request)
                .await?;
            drop(pending_join);
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let history_verifier = crate::sync::store::HistoryConstructionAuthority::for_snapshot()
                .open_pinned(storage.as_ref(), &offer.store_root)
                .await
                .map_err(crate::sync::store::SnapshotError::from)?;
            let cancel = tokio::sync::watch::channel(false).1;
            joining_store_dir.ensure_created()?;
            let open_synced_tables = synced_tables.clone();
            let joined_device_id = offer.attempt_id.to_string();
            let membership = observer.membership().await?;
            let storage_object: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
                storage.clone();
            let restoring = crate::sync::store::PreparedSnapshotBootstrap::prepare(
                &storage_object,
                history_verifier,
                &coven_protocol::membership::MembershipFloor(membership.head_refs().to_vec()),
                binary_schema_version,
                &joining_store_dir.db_path(),
                joining_identity,
                std::sync::Arc::new(|_| {}),
                &cancel,
            )
            .await?
            .install(
                &joining_store_dir,
                synced_tables,
                coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
                coven_protocol::blob::TransferLimits::one_at_a_time(),
                offer.attempt_id.to_string(),
                std::sync::Arc::new(coven_foundation::clock::SystemClock),
                &migrations,
                Some(&routing_encryption),
            )
            .await?;
            let mut joining = restoring.begin_device_join(&pending, offer).await?;
            joining
                .bootstrap(
                    join.bootstrap.clone(),
                    published_at,
                    Some(&routing_encryption),
                )
                .await?;
            joining.complete(join.activation).await?;
            // The install owns the database it created, so the join has to be
            // finished and dropped before a device opens it — the same order
            // production has, where the join and the running device are
            // separate processes over one file.
            drop(joining);
            open_joined_test_device(
                joining_store_dir,
                joining_identity,
                storage,
                joined_device_id,
                open_synced_tables,
                &migrations,
            )
            .await
        }

        pub async fn latest_local_store_snapshot_for_test(
            &self,
        ) -> Result<Option<coven_database::PublishedStoreSnapshot>, TestError> {
            Ok(self.db.latest_local_store_snapshot().await?)
        }

        /// Publish one more generation over the current frontier and acknowledge
        /// it, the way the cadence would.
        pub async fn publish_snapshot_generation_for_test(
            &self,
        ) -> Result<coven_database::PublishedStoreSnapshot, TestError> {
            let image_dir = tempfile::tempdir()?;
            let root = self.store.root_ref_for_test().clone();
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let image = self
                .db
                .capture_snapshot_image_for_test(
                    root,
                    image_dir.path().to_path_buf(),
                    Some(routing_encryption),
                )
                .await?;
            let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db.materialized_frontier().await?,
            )?;
            self.publish_snapshot(image, coverage.clone()).await?;
            self.publish_acknowledgement(coverage).await?;
            self.db.latest_local_store_snapshot().await?.ok_or_else(|| {
                TestError::invariant("the published generation is absent".to_string())
            })
        }

        pub async fn ensure_device_join_snapshot_for_test(&self) -> Result<(), TestError> {
            if let Some(snapshot) = self.db.latest_local_store_snapshot().await? {
                let acknowledged = if let Some(published) = self.db.latest_local_store_ack().await?
                {
                    let authority = self.device_authority_for_test().await?;
                    let acknowledgement = self
                        .load_store_ack_for_test(
                            &published.reference,
                            authority.registration.value(),
                        )
                        .await?;
                    acknowledgement.snapshot.as_ref().is_some_and(|locator| {
                        locator.author_registration == snapshot.meta.author_registration
                            && locator.snapshot == snapshot.reference
                            && acknowledgement
                                .store_cut
                                .frontier()
                                .covers(&snapshot.meta.coverage)
                    })
                } else {
                    false
                };
                if !acknowledged {
                    self.publish_acknowledgement(snapshot.meta.coverage.clone())
                        .await?;
                }
                return Ok(());
            }
            let image_dir = tempfile::tempdir()?;
            let root = self.store.root_ref_for_test().clone();
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let image = self
                .db
                .capture_snapshot_image_for_test(
                    root,
                    image_dir.path().to_path_buf(),
                    Some(routing_encryption),
                )
                .await?;
            let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db.materialized_frontier().await?,
            )?;
            self.publish_snapshot(image, coverage.clone()).await?;
            self.publish_acknowledgement(coverage).await?;
            Ok(())
        }

        pub async fn load(
            db: &Database,
            store_dir: StoreDir,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            identity: UserKeypair,
        ) -> Result<Self, crate::sync::store::StoreError> {
            Self::load_with_database(
                coven_database::StoreDatabase::new(db),
                storage,
                identity,
                store_dir,
            )
            .await
        }

        pub async fn load_with_database(
            database: coven_database::StoreDatabase,
            storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
            identity: UserKeypair,
            store_dir: StoreDir,
        ) -> Result<Self, crate::sync::store::StoreError> {
            database.assert_owns_payload_directory_for_test(&store_dir);
            let store = crate::sync::store::Store::load(
                database.clone(),
                storage.clone(),
                store_dir.clone(),
                identity.clone(),
            )
            .await?;
            let device_id = database
                .get_protocol_state(coven_database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
                .ok_or(crate::sync::store::StoreError::MissingState {
                    key: coven_database::LOCAL_DEVICE_ID_STATE_KEY,
                })?;
            Ok(Self {
                db: database,
                store: std::sync::Arc::new(store),
                store_dir,
                device_id,
                storage,
                identity,
                settled: std::sync::Arc::default(),
            })
        }

        pub fn adopt_key_rotation(
            &self,
            encryption: &coven_keys::encryption::EncryptionService,
            custody: &dyn coven_keys::keys::MasterKeyCustody,
        ) -> Result<String, coven_keys::keys::KeyError> {
            self.storage
                .adopt_key_rotation_for_test(encryption, custody)
        }

        pub fn store_root(&self) -> &coven_protocol::store_commit::StoreRootRef {
            self.store.store_root()
        }

        pub async fn authorize_writer(
            &self,
        ) -> Result<crate::sync::store::AuthorizedWriterOperation<'_>, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreError::from)
        }

        pub async fn execute_unscoped_host_sql_for_test(
            &self,
            sql: String,
        ) -> Result<(), coven_database::HostWriteError<coven_database::DbError>> {
            self.store.execute_unscoped_host_sql_for_test(sql).await
        }

        pub async fn membership_for_test(
            &self,
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store.membership_for_test().await
        }

        pub async fn latest_local_store_position(
            &self,
        ) -> Result<
            Option<coven_protocol::store_commit::StoreBatchCommitRef>,
            crate::sync::store::StoreError,
        > {
            self.store.latest_local_store_position().await
        }

        pub async fn load_commit_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
            crate::sync::store::StoreError,
        > {
            self.store.load_commit_for_test(reference).await
        }

        pub async fn load_membership_head_for_test(
            &self,
            reference: &coven_protocol::membership::MembershipHeadRef,
        ) -> Result<coven_protocol::membership::AuthorHead, crate::sync::store::StoreError>
        {
            self.store.load_membership_head_for_test(reference).await
        }

        pub async fn load_exact_materialized_commit(
            &self,
            stream_id: &str,
            sequence: u64,
        ) -> Result<
            Option<(
                coven_protocol::store_commit::StoreBatchCommitRef,
                coven_protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            crate::sync::store::StoreError,
        > {
            self.store
                .load_exact_materialized_commit(stream_id, sequence)
                .await
        }

        pub fn device_join_transport(
            &self,
        ) -> crate::sync::store::device_join::transport::StoreDeviceJoinTransport<'_> {
            self.store.device_join_transport()
        }

        pub fn circles(&self) -> crate::sync::store::StoreCircleCommands<'_> {
            self.store.circles()
        }

        pub async fn circle_epoch_access(
            &self,
            circle_id: coven_protocol::circle::CircleId,
            expected_control: coven_protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<coven_protocol::circle_activation::CircleEpochAccess>,
            coven_database::DbError,
        > {
            self.store
                .circle_epoch_access(circle_id, expected_control)
                .await
        }

        pub async fn discard_blocked_write(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<Vec<coven_protocol::write::WriteId>, crate::sync::store::StoreError> {
            self.store.discard_blocked_write(write_id).await
        }

        pub async fn restore_membership(
            &self,
        ) -> Result<
            crate::sync::store::StoreRestoreMembership,
            crate::sync::store::MembershipOpsError,
        > {
            self.store.restore_membership().await
        }

        pub async fn owner_recovery_for_test(
            &self,
        ) -> Result<crate::sync::store::RestoringStore<'_>, TestError> {
            self.store.owner_recovery_for_test().await
        }

        pub async fn begin_device_join(
            &self,
            member_pubkey: &str,
        ) -> Result<crate::sync::DeviceJoinOffer, crate::sync::DeviceJoinError> {
            self.store.begin_device_join(member_pubkey).await
        }

        pub async fn begin_owner_promotion_for_device(
            &self,
            device_id: coven_protocol::StoreDeviceId,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion_for_device(device_id).await
        }

        pub async fn begin_owner_promotion(
            &self,
            member_registration: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionRequest,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.begin_owner_promotion(member_registration).await
        }

        pub async fn accept_owner_promotion(
            &self,
            request: coven_protocol::store_commit::OwnerPromotionRequest,
        ) -> Result<
            coven_protocol::store_commit::OwnerPromotionAcceptance,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store.accept_owner_promotion(request).await
        }

        pub async fn finalize_owner_promotion(
            &self,
            encryption: &coven_keys::encryption::EncryptionService,
            acceptance: coven_protocol::store_commit::OwnerPromotionAcceptance,
        ) -> Result<
            coven_protocol::circle_control::StoreMembershipStateRef,
            crate::sync::store::OwnerPromotionError,
        > {
            self.store
                .finalize_owner_promotion(encryption, acceptance)
                .await
        }

        pub async fn blob_key_fingerprint_for_test(
            &self,
            authority: &coven_protocol::blob::RowBlobAuthority,
            stored: &coven_protocol::blob::locator::StoredBlobRef,
        ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, TestError> {
            self.store
                .blob_key_fingerprint_for_test(authority, stored)
                .await
                .map_err(TestError::from)
        }

        pub async fn announcement_stream_id_for_test(
            &self,
        ) -> Result<coven_protocol::membership::AuthorStreamId, crate::sync::store::StoreError>
        {
            self.store.announcement_stream_id_for_test().await
        }

        pub async fn sign_device_head_for_test(
            &self,
            commit: coven_protocol::store_commit::StoreBatchCommitRef,
            successor: coven_protocol::store_commit::SuccessorLink,
        ) -> Result<coven_protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .sign_device_head_for_test(commit, successor)
                .await
        }

        pub async fn owner_promotion_target_for_test(
            &self,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceRegistrationRef,
            crate::sync::store::StoreError,
        > {
            self.store.owner_promotion_target_for_test().await
        }

        pub async fn observe_excluded_candidate_head_for_test(
            &self,
            candidate: &coven_protocol::store_commit::StoreDeviceHead,
            candidate_commit: &coven_protocol::store_commit::StoreBatchCommit,
            candidate_object: &coven_protocol::objects::ExactObjectRef,
        ) -> Result<
            crate::sync::store::ExcludedCandidateHeadObservation,
            crate::sync::store::StoreError,
        > {
            self.store
                .observe_excluded_candidate_head_for_test(
                    candidate,
                    candidate_commit,
                    candidate_object,
                )
                .await
        }

        pub async fn cleanup_merge_candidate_for_test(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store.cleanup_merge_candidate_for_test(write_id).await
        }

        pub async fn resign_snapshot_meta_for_test(
            &self,
            meta: coven_protocol::store_commit::SnapshotMeta,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store.resign_snapshot_meta_for_test(meta).await
        }

        pub async fn parse_local_snapshot_meta_for_test(
            &self,
            bytes: &[u8],
            reference: &coven_protocol::store_commit::StoreSnapshotRef,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::StoreError>
        {
            self.store
                .parse_local_snapshot_meta_for_test(bytes, reference)
                .await
        }

        pub async fn prepare_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store.prepare_operation_plan_for_test().await
        }

        pub async fn authorize_retained_outbound_for_test(
            &self,
            order: &coven_protocol::store_commit::StoreCommitOrder,
            candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<crate::sync::store::MergeOutboundAuthorization, crate::sync::store::StoreError>
        {
            self.store
                .authorize_retained_outbound_for_test(order, candidate_membership_heads)
                .await
        }

        pub async fn complete_revoke_rotation_adoption_for_test(
            &self,
            pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
            adopted_generation: u64,
        ) -> Result<(), crate::sync::store::MembershipMutationError> {
            self.store
                .complete_revoke_rotation_adoption_for_test(pending_rotation, adopted_generation)
                .await
        }

        pub async fn retained_merge_replay_inputs_for_test(
            &self,
        ) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, coven_database::DbError>
        {
            self.store.retained_merge_replay_inputs_for_test().await
        }

        pub async fn resolved_store_device_state_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceStateRef,
        ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, coven_database::DbError>
        {
            self.store
                .resolved_store_device_state_for_test(reference)
                .await
        }

        pub async fn retained_merge_materialization_for_test(
            &self,
            reference: coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<coven_database::OwnedVerifiedMergeMaterialization, coven_database::DbError>
        {
            self.store
                .retained_merge_materialization_for_test(reference)
                .await
        }

        pub async fn prepare_conflict_resolution_plan_for_test(
            &self,
            candidate_membership_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .prepare_conflict_resolution_plan_for_test(candidate_membership_heads)
                .await
        }

        pub async fn load_membership_at_exact_heads_for_test(
            &self,
            heads: &[coven_protocol::membership::MembershipHeadRef],
            resolutions: &[coven_protocol::membership::StoreMembershipConflictResolutionRef],
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .load_membership_at_exact_heads_for_test(heads, resolutions)
                .await
        }

        pub async fn project_membership_for_test(
            &self,
            candidate_heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<coven_protocol::membership::MembershipChain, crate::sync::store::StoreError>
        {
            self.store
                .project_membership_for_test(candidate_heads)
                .await
        }

        pub async fn assert_deep_membership_projection_for_test(
            &self,
            heads: &[coven_protocol::membership::MembershipHeadRef],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .assert_deep_membership_projection_for_test(heads)
                .await
        }

        pub async fn verify_device_join_attempt_for_test(
            &self,
            reference: &coven_protocol::store_commit::DeviceJoinAttemptRef,
            owner: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_device_join_attempt_for_test(reference, owner)
                .await
        }

        pub async fn exact_next_announcement_slot_for_test(
            &self,
            registration_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
            previous: Option<&coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            (
                coven_protocol::objects::ObjectSlot,
                Option<coven_protocol::store_commit::StoreDeviceHeadRef>,
            ),
            crate::sync::store::StoreError,
        > {
            self.store
                .exact_next_announcement_slot_for_test(registration_ref, registration, previous)
                .await
        }

        pub async fn load_registration_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceRegistration,
            crate::sync::store::StoreError,
        > {
            self.store.load_registration_for_test(reference).await
        }

        pub async fn verify_installable_snapshots_for_test(
            &self,
            snapshots: &[coven_database::PublishedStoreSnapshot],
        ) -> Result<(), crate::sync::store::StoreError> {
            self.store
                .verify_installable_snapshots_for_test(snapshots)
                .await
        }

        pub async fn open_circle_package_for_test(
            &self,
            access: &coven_protocol::circle_activation::CircleEpochAccess,
            commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            reference: &coven_protocol::store_commit::CirclePackageRef,
        ) -> Result<Vec<u8>, crate::sync::store::StoreError> {
            self.store
                .open_circle_package_for_test(access, commit, reference)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn pull_readiness_for_test(
            &self,
            coverage: &coven_protocol::store_commit::CommitFrontier,
            frontier: &std::collections::BTreeMap<
                String,
                coven_protocol::store_commit::StoreBatchCommitRef,
            >,
            device_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
            exclusion_freezes: &[coven_protocol::store_commit::StoreDeviceProposalAck],
            commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
            commit: &coven_protocol::store_commit::StoreBatchCommit,
        ) -> Result<crate::sync::store::Readiness, crate::sync::store::StorePullError> {
            self.store
                .pull_readiness_for_test(
                    coverage,
                    frontier,
                    device_state,
                    exclusion_freezes,
                    commit_ref,
                    commit,
                )
                .await
        }

        pub async fn verified_merge_membership_prefix_for_test(
            &self,
            references: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
            predecessors: impl IntoIterator<Item = coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<
            crate::sync::store::VerifiedMergeMembershipPrefix,
            crate::sync::store::StorePullError,
        > {
            self.store
                .verified_merge_membership_prefix_for_test(references, predecessors)
                .await
        }

        pub async fn retained_merge_history_frontier_for_test(
            &self,
            references: Vec<coven_protocol::store_commit::StoreBatchCommitRef>,
        ) -> Result<Vec<coven_database::RetainedMergeHistoryCheckpoint>, coven_database::DbError>
        {
            self.store
                .retained_merge_history_frontier_for_test(references)
                .await
        }

        pub async fn verified_circle_activation_for_test(
            &self,
            circle_id: coven_protocol::circle::CircleId,
            control: coven_protocol::circle::CircleControlCoord,
        ) -> Result<
            Option<coven_protocol::circle_activation::VerifiedCircleReference>,
            coven_database::DbError,
        > {
            self.store
                .verified_circle_activation_for_test(circle_id, control)
                .await
        }

        pub async fn finalized_circle_close_outcome_for_test(
            &self,
            circle_id: coven_protocol::circle::CircleId,
        ) -> Result<
            coven_protocol::circle::CircleEpochCloseOutcome,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .finalized_circle_close_outcome_for_test(circle_id)
                .await
        }

        pub async fn circle_package_is_retained_for_replay_for_test(
            &self,
            target: coven_protocol::store_commit::CirclePackageRef,
            activation: coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<bool, coven_database::DbError> {
            self.store
                .circle_package_is_retained_for_replay_for_test(target, activation)
                .await
        }

        pub async fn load_circle_acknowledgement_for_test(
            &self,
            reference: &coven_protocol::store_commit::CircleAckRef,
        ) -> Result<coven_protocol::store_commit::CircleAck, crate::sync::store::StoreAckError>
        {
            self.store
                .load_circle_acknowledgement_for_test(reference)
                .await
        }

        pub async fn load_applicable_circle_packages_for_test(
            &self,
            verified: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            activations: &[coven_protocol::circle_activation::VerifiedCircleReference],
            author: &coven_protocol::store_commit::StoreDeviceRegistration,
            local_store_membership: coven_protocol::membership::LocalStoreMembership,
        ) -> Result<
            Vec<crate::sync::store::LoadedCirclePackage>,
            crate::sync::store::CirclePackageReadError,
        > {
            self.store
                .load_applicable_circle_packages_for_test(
                    verified,
                    activations,
                    author,
                    local_store_membership,
                )
                .await
        }

        pub fn protocol_root_for_test(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        pub async fn prepare_acknowledgement_activation_for_test(
            &self,
            acknowledgement: coven_protocol::store_commit::StoreAckRef,
            candidate: coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        ) -> Result<(), coven_database::DbError> {
            self.store
                .prepare_acknowledgement_activation_for_test(acknowledgement, candidate)
                .await
        }

        pub async fn prepare_merge_history_successor_for_test(
            &self,
            verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
            recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
            evidence: crate::sync::store::MergeHistorySuccessorEvidence,
        ) -> Result<crate::sync::store::PreparedMergeHistorySuccessor, crate::sync::store::StoreError>
        {
            self.store
                .prepare_merge_history_successor_for_test(
                    verified_commit,
                    recovery_author,
                    evidence,
                )
                .await
        }

        pub async fn prepare_device_join_bootstrap_for_test(
            &self,
            bootstrap_cut: &coven_protocol::store_commit::StoreHistoryCut,
            attempt_activation: &coven_protocol::store_commit::StoreBatchCommitRef,
            membership_state: &coven_protocol::circle_control::StoreMembershipStateRef,
            installed: &coven_protocol::store_commit::CommitFrontier,
        ) -> Result<coven_database::DeviceJoinBootstrapPlan, crate::sync::store::StoreError>
        {
            self.store
                .prepare_device_join_bootstrap_for_test(
                    bootstrap_cut,
                    attempt_activation,
                    membership_state,
                    installed,
                )
                .await
        }

        pub async fn load_store_package_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<
            Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>,
            crate::sync::store::StoreError,
        > {
            self.store.load_store_package_for_test(reference).await
        }

        pub async fn load_store_ack_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreAckRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<coven_protocol::store_commit::StoreAck, crate::sync::store::StoreError>
        {
            self.store
                .load_store_ack_for_test(reference, registration)
                .await
        }

        pub async fn load_head_for_test(
            &self,
            reference: &coven_protocol::store_commit::StoreDeviceHeadRef,
            registration: &coven_protocol::store_commit::StoreDeviceRegistration,
            commit: &coven_protocol::store_commit::StoreBatchCommitRef,
        ) -> Result<coven_protocol::store_commit::StoreDeviceHead, crate::sync::store::StoreError>
        {
            self.store
                .load_head_for_test(reference, registration, commit)
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn remove_member(
            &self,
            public_key_hex: &str,
            encryption: &coven_keys::encryption::EncryptionService,
            master_keys: &dyn coven_keys::keys::MasterKeyCustody,
            cipher: &dyn coven_storage::CloudSyncCipherStateAccess,
            pending_rotation: &dyn coven_storage::CloudSyncRotationStateAccess,
        ) -> Result<String, crate::sync::store::MembershipOpsError> {
            self.store
                .remove_member(
                    public_key_hex,
                    encryption,
                    master_keys,
                    cipher,
                    pending_rotation,
                )
                .await
        }

        pub async fn authorize_device_provider_access(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceProviderAccessRequest,
            access_administrator: Option<
                &dyn crate::sync::store::DeviceProviderAccessAdministrator,
            >,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionApproval,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .authorize_device_provider_access(request, access_administrator)
                .await
        }

        pub async fn publish_device_provider_challenge(
            &self,
            bootstrap: coven_protocol::store_commit::device_join_exchange::ProvisionalDeviceBootstrap,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProviderReadyDeviceBootstrap,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .publish_device_provider_challenge(bootstrap)
                .await
        }

        pub async fn complete_device_provider_admission(
            &self,
            readiness: coven_protocol::store_commit::device_join_exchange::DeviceJoinReadiness,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .complete_device_provider_admission(readiness)
                .await
        }

        pub async fn close_device_provider_admission(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .close_device_provider_admission(cancellation)
                .await
        }

        pub async fn abandon_device_join(
            &self,
            offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinAbandonment,
            crate::sync::DeviceJoinError,
        > {
            self.store.abandon_device_join(offer).await
        }

        pub async fn accept_device_registration_request(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceRegistrationRequest,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::ProvisionalDeviceBootstrap,
            crate::sync::DeviceJoinError,
        > {
            self.store.accept_device_registration_request(request).await
        }

        pub async fn activate_same_principal_join_for_test(
            &self,
            request: coven_protocol::store_commit::device_join_exchange::DeviceRegistrationRequest,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::SamePrincipalDeviceJoin,
            crate::sync::DeviceJoinError,
        > {
            let mut writer = self
                .store
                .authorize_writer()
                .await
                .map_err(crate::sync::DeviceJoinError::from)?;
            writer
                .join_operation()
                .activate_same_principal_join(request)
                .await
        }

        pub async fn cancel_device_join(
            &self,
            attempt: coven_protocol::store_commit::DeviceJoinAttemptRef,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            crate::sync::DeviceJoinError,
        > {
            self.store.cancel_device_join(attempt).await
        }

        pub async fn finalize_device_join(
            &self,
            completion: coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store.finalize_device_join(completion).await
        }

        pub async fn complete_owner_device_join_cleanup(
            &self,
            activation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .complete_owner_device_join_cleanup(activation)
                .await
        }

        pub async fn revoke_joining_device_writes(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            revocation_executor: &dyn crate::sync::store::DeviceJoinWriteRevocationExecutor,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .revoke_joining_device_writes(cancellation, revocation_executor)
                .await
        }

        pub async fn prepare_device_join_cleanup(
            &self,
            cancellation: coven_protocol::store_commit::device_join_exchange::DeviceJoinCancellation,
            administrator_terminal: coven_protocol::store_commit::device_join_exchange::ProviderAdminJoinTerminal,
            joiner_terminal: coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceipt,
            crate::sync::DeviceJoinError,
        > {
            self.store
                .prepare_device_join_cleanup(cancellation, administrator_terminal, joiner_terminal)
                .await
        }

        pub async fn activate_device_join_cleanup(
            &self,
            receipt: coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupReceipt,
        ) -> Result<
            coven_protocol::store_commit::device_join_exchange::DeviceJoinCleanupActivation,
            crate::sync::DeviceJoinError,
        > {
            self.store.activate_device_join_cleanup(receipt).await
        }

        pub async fn device_exclusion_operations_for_test(
            &self,
        ) -> Result<
            Vec<crate::sync::store::StoreDeviceExclusionOperationInfo>,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.device_exclusion_operations_for_test().await
        }

        pub async fn stage_uploaded_device_exclusion_proposal_for_test(
            &self,
        ) -> Result<
            coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store
                .stage_uploaded_device_exclusion_proposal_for_test()
                .await
        }

        pub async fn propose_device_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.propose_device_exclusion(target).await
        }

        pub async fn cancel_device_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.cancel_device_exclusion(proposal).await
        }

        pub async fn finalize_device_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> Result<
            crate::sync::store::StoreDeviceExclusionResult,
            crate::sync::store::StoreDeviceExclusionError,
        > {
            self.store.finalize_device_exclusion(proposal).await
        }

        pub async fn pending_device_join_observation_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, TestError> {
            self.store
                .pending_device_join_observation_for_test(pending, offer)
                .await
                .map_err(TestError::from)
        }

        pub async fn open_pending_device_join_for_test(
            &self,
            pending: &crate::sync::store::DeviceJoinJournalDatabase,
            identity: &UserKeypair,
            offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
        ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, TestError> {
            self.store
                .open_pending_device_join_for_test(pending, identity, offer)
                .await
                .map_err(TestError::from)
        }

        pub async fn prepare_snapshot_bootstrap_for_test(
            &self,
            membership_floor: &coven_protocol::membership::MembershipFloor,
            binary_schema_version: u32,
            target_path: &std::path::Path,
            restorer_identity: &UserKeypair,
        ) -> Result<
            crate::sync::store::PreparedSnapshotBootstrap<'_>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .prepare_snapshot_bootstrap_for_test(
                    membership_floor,
                    binary_schema_version,
                    target_path,
                    restorer_identity,
                )
                .await
        }

        #[allow(clippy::too_many_arguments)]
        pub async fn admit_member(
            &self,
            member_pubkey: &str,
            member_email: Option<&str>,
            role: coven_protocol::membership::MemberRole,
            encryption: &coven_keys::encryption::EncryptionService,
            store_id: &str,
            store_name: &str,
        ) -> Result<crate::sync::store::MemberAdmission, crate::sync::store::MembershipOpsError>
        {
            self.store
                .admit_member(
                    member_pubkey,
                    member_email,
                    role,
                    encryption,
                    store_id,
                    store_name,
                )
                .await
        }

        pub async fn drain_uploads(
            &self,
            clock: &dyn coven_foundation::clock::Clock,
            routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::blob::DrainOutcome, TestError> {
            self.store
                .authorize_writer()
                .await
                .map_err(TestError::from)?
                .drain_uploads(clock, routing_encryption, observer)
                .await
                .map_err(TestError::from)
        }

        pub async fn publish_pending_store_database(&self) -> Result<bool, TestError> {
            let mut writer = self.store.authorize_writer().await?;
            let prepared = writer.prepare_pending_store_write().await?;
            let published = writer.drain_store_writes().await?;
            if published > 0 {
                crate::sync::test_owner_graph::TestOwnerGraph::new(
                    self.db.clone(),
                    self.store_dir.clone(),
                )
                .drain_published_blob_drop_intents(u64::MAX)
                .await?;
                coven_database::LocalBlobCleanup::new(&self.db)
                    .drain()
                    .await?;
            }
            Ok(prepared || published > 0)
        }

        pub async fn publish_fixture_position(&self, note_id: &str) -> u64 {
            self.db
                .insert_fixture_position_for_test(note_id)
                .await
                .expect("insert fixture Store position");
            assert!(self
                .publish_pending_store_database()
                .await
                .expect("publish fixture Store position"));
            self.latest_local_store_position()
                .await
                .expect("read fixture Store position")
                .expect("fixture Store write has an exact position")
                .coord
                .sequence()
        }

        pub async fn publish_exact_remote_blob_binding(
            &self,
            root_id: &str,
            row_id: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let local = self
                .db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Local row blob reference");
            let source = self
                .store_dir
                .local_blob_path(&local.blob().namespace, &local.blob().id)
                .expect("resolve host blob source");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&source, bytes)
                .await
                .expect("write host blob source");
            crate::sync::test_owner_graph::TestOwnerGraph::new(
                self.db.clone(),
                self.store_dir.clone(),
            )
            .make_remote("notes", root_id, false)
            .await
            .expect("start exact make_remote");
            let clock = coven_foundation::clock::FixedClock(
                chrono::DateTime::parse_from_rfc3339("2024-06-01T01:00:00Z")
                    .expect("valid exact blob publication time")
                    .with_timezone(&chrono::Utc),
            );
            let outcome = self
                .drain_uploads(&clock, None, None)
                .await
                .expect("drain exact blob upload");
            assert_eq!(outcome.uploaded(), 1);
            assert!(self
                .publish_pending_store_database()
                .await
                .expect("publish exact remote blob binding"));
            self.db
                .row_blob_ref("note_photos", row_id)
                .await
                .expect("load exact Remote row blob reference")
                .stored()
                .cloned()
                .expect("Remote row owns an exact stored blob reference")
        }

        pub async fn activated_store_device_registration_for_test(
            &self,
            reference: coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> Result<
            coven_protocol::store_commit::ReferencedStoreDeviceRegistration,
            coven_database::DbError,
        > {
            self.db.activated_store_device_registration(reference).await
        }

        pub fn schema_version(&self) -> u32 {
            self.db.schema_version()
        }

        pub async fn device_authority_for_test(
            &self,
        ) -> Result<TestDeviceSigningAuthority, TestError> {
            let registration = self
                .db
                .activated_store_device_registration_records()
                .await?
                .into_iter()
                .find(|registration| registration.value().device_id.to_string() == self.device_id)
                .ok_or_else(|| {
                    TestError::invariant("test device registration is not active".to_string())
                })?;
            let device_signer = registration.value().device_signer(&self.identity)?;
            Ok(TestDeviceSigningAuthority {
                registration,
                device_signer,
            })
        }

        pub async fn publish_changeset_for_test(
            &self,
            sequence: u64,
            changeset: Vec<u8>,
            schema_version: u32,
        ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, TestError> {
            if schema_version != self.db.schema_version() {
                return Err(TestError::invariant(format!(
                    "test changeset schema version {schema_version} differs from producer schema {}",
                    self.db.schema_version()
                )));
            }
            let before = self.latest_local_store_position().await?;
            let expected = before
                .as_ref()
                .map_or(1, |reference| reference.coord.sequence().saturating_add(1));
            if sequence != expected {
                return Err(TestError::invariant(format!(
                    "test producer expected sequence {expected}, got {sequence}"
                )));
            }
            self.db.enqueue_store_changeset_for_test(changeset).await?;
            let mut writer = self.authorize_writer().await?;
            let published = writer.publish_pending_store_writes().await?;
            if published == 0 {
                return Err(TestError::invariant(
                    "test changeset did not prepare a Store commit".to_string(),
                ));
            }
            writer.latest_local_store_position().await?.ok_or_else(|| {
                TestError::invariant("published test changeset has no Store position".to_string())
            })
        }

        pub async fn publish_changeset_after_for_test(
            &self,
            changeset: Vec<u8>,
            previous_sequence: u64,
        ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, TestError> {
            let before = self.latest_local_store_position().await?;
            let actual_previous_sequence = before
                .as_ref()
                .map_or(0, |position| position.coord.sequence());
            if actual_previous_sequence != previous_sequence {
                return Err(TestError::invariant(format!(
                    "Store position is {actual_previous_sequence}, expected {previous_sequence}"
                )));
            }
            self.db.enqueue_store_changeset_for_test(changeset).await?;
            let mut writer = self.store.authorize_writer().await?;
            if !writer.prepare_pending_store_write().await? {
                return Err(TestError::invariant(
                    "test changeset did not prepare a Store commit".to_string(),
                ));
            }
            writer.drain_store_writes().await?;
            writer.latest_local_store_position().await?.ok_or_else(|| {
                TestError::invariant("published test changeset has no Store position".to_string())
            })
        }

        pub async fn create_exact_opaque_blob(
            &self,
            namespace: &str,
            id: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load exact blob write authority");
            let authority = coven_protocol::objects::BlobWriteAuthority::new(&registration);
            let protection = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            let locator = coven_protocol::blob::locator::BlobLocator::opaque(
                namespace,
                id,
                authority.reference.clone(),
                coven_protocol::blob::locator::RemoteAudience::Store,
                coven_protocol::blob::BlobScope::Master,
                protection.seal_key_fingerprint(),
                bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build exact blob locator");
            let temp = tempfile::tempdir().expect("create exact blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write exact blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate exact blob slot");
            let spool_stage = self
                .store_dir
                .stage_atomic_file(&spool)
                .await
                .expect("create exact blob spool stage");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    coven_protocol::objects::BlobSpoolProtection::Opaque(protection),
                    &plaintext,
                    spool_stage,
                    coven_storage::cloud::no_preparation_progress(),
                )
                .await
                .expect("seal exact blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare exact blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &coven_storage::cloud::no_progress(),
                )
                .await
                .expect("create exact blob object");
            stored
        }

        pub async fn create_exact_browsable_blob(
            &self,
            namespace: &str,
            id: &str,
            cloud_path: &str,
            bytes: &[u8],
        ) -> coven_protocol::blob::locator::StoredBlobRef {
            let registration = self
                .db
                .local_blob_write_authority()
                .await
                .expect("load browsable blob write authority");
            let authority = coven_protocol::objects::BlobWriteAuthority::new(&registration);
            let locator = coven_protocol::blob::locator::BlobLocator::browsable(
                namespace,
                id,
                authority.reference.clone(),
                cloud_path,
                bytes.len() as u64,
                coven_protocol::store_commit::ObjectHash::digest(bytes),
            )
            .expect("build browsable blob locator");
            let temp = tempfile::tempdir().expect("create browsable blob spool directory");
            let plaintext = temp.path().join("plaintext");
            let spool = temp.path().join("stored");
            coven_foundation::local_file::AtomicStagedFile::write_for_test(&plaintext, bytes)
                .await
                .expect("write browsable blob plaintext");
            let slot = self
                .storage
                .allocate_blob_slot(&locator, &authority)
                .await
                .expect("allocate browsable blob slot");
            let spool_stage = self
                .store_dir
                .stage_atomic_file(&spool)
                .await
                .expect("create browsable blob spool stage");
            self.storage
                .seal_blob_to_spool(
                    &locator,
                    &authority,
                    coven_protocol::objects::BlobSpoolProtection::Browsable,
                    &plaintext,
                    spool_stage,
                    coven_storage::cloud::no_preparation_progress(),
                )
                .await
                .expect("stage browsable blob");
            let stored = self
                .storage
                .prepare_blob_object(&locator, &authority, slot, &spool)
                .await
                .expect("prepare browsable blob object");
            self.storage
                .create_blob_object_from_file(
                    &stored,
                    &authority,
                    &spool,
                    &coven_storage::cloud::no_progress(),
                )
                .await
                .expect("create browsable blob object");
            stored
        }

        pub async fn run_cycle(
            &self,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with(&coven_foundation::clock::SystemClock, None, observer)
                .await
        }

        pub async fn run_cycle_with(
            &self,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>>,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        {
            self.run_cycle_with_storage(
                self.store.clone(),
                self.storage.clone(),
                clock,
                master_keys,
                observer,
            )
            .await
        }

        pub async fn run_cycle_with_interceptor<I>(
            &self,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>>,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
            interceptor: I,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            I: super::StorageInterceptor + 'static,
        {
            let storage = std::sync::Arc::new(super::InterceptedStorage::new(
                self.storage.clone(),
                interceptor,
            ));
            let store_storage: std::sync::Arc<dyn coven_storage::CloudSyncObjectStorage> =
                storage.clone();
            let store = std::sync::Arc::new(self.store.with_test_storage(store_storage));
            self.run_cycle_with_storage(store, storage, clock, master_keys, observer)
                .await
        }

        async fn run_cycle_with_storage<S>(
            &self,
            store: std::sync::Arc<crate::sync::store::Store>,
            storage: std::sync::Arc<S>,
            clock: &dyn coven_foundation::clock::Clock,
            master_keys: Option<std::sync::Arc<dyn coven_keys::keys::MasterKeyCustody>>,
            observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
        ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure>
        where
            S: crate::sync::cycle::CloudSyncCycleConnection + 'static,
        {
            let components = crate::sync::cycle::SyncComponents::from_retained_test_device(
                store,
                self.db.clone(),
                self.store_dir.clone(),
                storage,
                self.storage.store_id().to_string(),
                self.device_id.clone(),
                master_keys.unwrap_or_else(|| std::sync::Arc::new(super::TestCustody::default())),
                self.settled.clone(),
            );
            components.run_cycle(clock, observer).await
        }

        pub fn current_keyring_for_test(&self) -> Option<coven_storage::CloudKeyringFacts> {
            self.storage.keyring_facts_for_test()
        }

        pub fn mark_rotation_committed_for_test(
            &self,
            generation: u64,
        ) -> Result<(), coven_storage::RotationStateError> {
            self.storage.mark_rotation_committed_for_test(generation)
        }

        pub fn pending_rotation_generation_for_test(&self) -> Option<u64> {
            self.storage.pending_rotation_generation_for_test()
        }

        pub fn clear_rotation_gate_for_test(&self) {
            self.storage.clear_rotation_gate_for_test();
        }

        pub async fn create_circle(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<coven_protocol::CircleId, crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .create_circle(metadata_stamp, name)
                .await
        }

        /// The writer authority every Circle helper below has to take before it
        /// can name its operation, with the one error shape they all report.
        async fn circle_writer(
            &self,
        ) -> Result<
            crate::sync::store::AuthorizedWriterOperation<'_>,
            crate::sync::store::CircleOperationError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::CircleOperationError::from)
        }

        #[cfg(test)]
        pub(crate) async fn prepare_circle_operation(
            &self,
            metadata_stamp: &str,
            name: &str,
        ) -> Result<
            crate::sync::store::circles::PreparedCircleJournal,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_create_for_test(metadata_stamp, name)
                .await
        }

        pub async fn publish_circle_epoch_close_response(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.circle_writer()
                .await?
                .circles()
                .publish_circle_epoch_close_responses()
                .await
        }

        pub async fn publish_circle_operation(
            &self,
            operation_id: &coven_protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.circle_writer()
                .await?
                .circles()
                .publish_prepared_operation_for_test(operation_id, Some(&routing_key))
                .await
        }

        pub async fn resume_circle_operations(
            &self,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                self.store.store_root().store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.circle_writer()
                .await?
                .circles()
                .resume_circle_operations(Some(&routing_key))
                .await
        }

        pub async fn retry_circle_operation(
            &self,
            operation_id: &coven_protocol::circle::CircleOperationId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .retry_circle_operation(
                    operation_id,
                    Some(&coven_keys::encryption::EncryptionService::from_key(
                        [42; 32],
                    )),
                )
                .await
        }

        pub async fn prepare_pending_store_write(
            &self,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreError::from)?
                .prepare_pending_store_write()
                .await
        }

        #[cfg(test)]
        pub async fn prepare_blocked_transfer_candidate(
            &self,
            label: &str,
        ) -> coven_protocol::write::WriteId {
            let statement = format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('{label}', 'pending', NULL, 1, \
                     '0000000002000-0000-{label}', '2026-07-18')"
            );
            self.db
                .run_host_store_write_for_test(None, None, move |transaction| {
                    transaction
                        .execute_batch(&statement)
                        .map_err(coven_database::DbError::from)
                })
                .await
                .expect("capture transfer candidate host write");
            assert!(self
                .prepare_pending_store_write()
                .await
                .expect("prepare transfer candidate"));
            let candidate = self
                .db
                .oldest_prepared_store_write()
                .await
                .expect("load transfer candidate")
                .expect("transfer candidate exists");
            let write_id = candidate.commit.value.write_id.clone();
            self.db
                .set_write_status(
                    &write_id,
                    coven_protocol::write::WriteStatus::Blocked(
                        coven_protocol::write::WriteBlock::InvalidProtocolState {
                            reason: "exercise restored author-exclusion evidence".to_string(),
                        },
                    ),
                )
                .await
                .expect("block transfer candidate");
            write_id
        }

        #[cfg(test)]
        pub async fn prepare_store_operation_plan_for_test(
            &self,
        ) -> Result<crate::sync::store::StoreOperationCommitPlan, crate::sync::store::StoreError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreError::from)?
                .prepare_plan()
                .await
        }

        pub async fn drain_store_writes(&self) -> Result<u64, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreError::from)?
                .drain_store_writes()
                .await
        }

        pub async fn reclaim_packages(
            &self,
        ) -> Result<crate::sync::store::StoreReclaimResult, crate::sync::store::StoreReclaimError>
        {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreReclaimError::from)?
                .reclaim_packages(&crate::sync::store::SettledCycle::default())
                .await
        }

        pub async fn abandon_merge_candidate(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<crate::sync::store::MergeCandidateAbandonment, crate::sync::store::StoreError>
        {
            self.store.abandon_merge_candidate(write_id).await
        }

        pub async fn prepare_merge_candidate_abandonment(
            &self,
            write_id: coven_protocol::write::WriteId,
        ) -> Result<bool, crate::sync::store::StoreError> {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::StoreError::from)?
                .prepare_merge_candidate_abandonment(write_id)
                .await
        }

        pub async fn prepare_peer_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionProposalRef {
            let proposal = match self
                .propose_device_exclusion(target)
                .await
                .expect("propose peer exclusion")
            {
                crate::sync::store::StoreDeviceExclusionResult::ProposalActivated {
                    proposal,
                    ..
                } => proposal,
                result => panic!("unexpected exclusion proposal result: {result:?}"),
            };
            let freezes = self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read owner exclusion freeze");
            assert_eq!(freezes.len(), 1);
            assert_eq!(freezes[0].proposal, proposal);
            assert_eq!(&freezes[0].proposal.target, target);
            let frontier = coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db
                    .materialized_frontier()
                    .await
                    .expect("read owner exclusion frontier"),
            )
            .expect("shape owner exclusion frontier");
            let acknowledgement = self
                .stage_acknowledgement(frontier, "2026-07-18T00:01:00Z".to_string())
                .await
                .expect("stage owner exclusion acknowledgement")
                .expect("the exclusion freeze is new, so it is acknowledged");
            let coven_protocol::store_commit::StoreAckExclusionState { proposal_freezes } =
                acknowledgement.exclusions.clone();
            assert_eq!(proposal_freezes, freezes);
            assert_eq!(
                self.drain_acknowledgements()
                    .await
                    .expect("publish owner exclusion acknowledgement"),
                1
            );
            proposal
        }

        pub async fn activate_peer_exclusion(
            &self,
            proposal: &coven_protocol::store_commit::StoreDeviceExclusionProposalRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionRef {
            let result = self
                .finalize_device_exclusion(proposal)
                .await
                .expect("finalize peer exclusion");
            let crate::sync::store::StoreDeviceExclusionResult::OutcomeActivated {
                outcome:
                    coven_protocol::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(exclusion),
                ..
            } = result
            else {
                panic!("unexpected exclusion result: {result:?}")
            };
            assert!(self
                .db
                .store_device_exclusion_freezes()
                .await
                .expect("read released owner exclusion freeze")
                .is_empty());
            exclusion
        }

        pub async fn finalize_peer_exclusion(
            &self,
            target: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        ) -> coven_protocol::store_commit::StoreDeviceExclusionRef {
            let proposal = self.prepare_peer_exclusion(target).await;
            self.activate_peer_exclusion(&proposal).await
        }

        pub async fn prepare_circle_object(
            &self,
            context: &coven_protocol::objects::ProtocolObjectContext,
            semantic_prefix: &str,
            extension: &str,
            bytes: Vec<u8>,
        ) -> Result<
            coven_protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_object_for_test(context, semantic_prefix, extension, bytes)
                .await
        }

        pub async fn prepare_circle_object_at(
            &self,
            context: &coven_protocol::objects::ProtocolObjectContext,
            slot: coven_protocol::objects::ObjectSlot,
            semantic_prefix: &str,
            bytes: Vec<u8>,
        ) -> Result<
            coven_protocol::objects::PreparedExactObject,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_object_at_for_test(context, slot, semantic_prefix, bytes)
        }

        pub async fn prepare_circle_activation_objects(
            &self,
            draft: coven_protocol::circle::CircleTransitionDraft,
            history: &crate::sync::store::CircleTransitionHistory,
            candidate_family: coven_protocol::store_commit::CandidateFamilyId,
        ) -> Result<
            (
                coven_protocol::circle::PreparedCircleTransition,
                coven_protocol::store_commit::CircleActivationObjects,
                std::collections::BTreeMap<String, coven_protocol::objects::PreparedExactObject>,
                Option<coven_protocol::objects::ExactObjectRef>,
                Vec<coven_protocol::store_commit::StreamActivation>,
            ),
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .prepare_circle_activation_objects_for_test(draft, history, candidate_family)
                .await
        }

        pub async fn sign_circle_commit(
            &self,
            old_commit: &coven_protocol::store_commit::StoreBatchCommit,
            coord: coven_protocol::store_commit::StoreCommitCoord,
            reference: coven_protocol::store_commit::CircleControlRef,
            stream_activations: Vec<coven_protocol::store_commit::StreamActivation>,
        ) -> Result<
            coven_protocol::store_commit::StoreBatchCommit,
            crate::sync::store::CircleOperationError,
        > {
            self.circle_writer()
                .await?
                .circles()
                .sign_circle_commit_for_test(old_commit, coord, reference, stream_activations)
        }

        pub async fn rename_circle(
            &self,
            metadata_stamp: &str,
            circle_id: coven_protocol::CircleId,
            name: &str,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store
                .circles()
                .rename_circle(metadata_stamp, circle_id, name)
                .await
        }

        pub async fn delete_circle(
            &self,
            circle_id: coven_protocol::CircleId,
        ) -> Result<(), crate::sync::store::CircleOperationError> {
            self.store.circles().delete_circle(circle_id).await
        }

        pub async fn load_circle_activations(
            &self,
            commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
            commit: &coven_protocol::store_commit::StoreBatchCommit,
            author: &coven_protocol::store_commit::StoreDeviceRegistration,
        ) -> Result<
            coven_protocol::circle_activation::VerifiedCircleActivations,
            crate::sync::store::CircleOperationError,
        > {
            let routing_key = coven_protocol::circle::derive_row_routing_key(
                &coven_keys::encryption::EncryptionService::from_key([42; 32]),
                commit.store_root_hash,
            )
            .expect("derive Circle test routing key");
            self.store
                .load_circle_activations_for_test(commit_ref, commit, author, Some(&routing_key))
                .await
        }

        pub async fn circle_blob_opening_error(
            &self,
            authority: &coven_protocol::blob::RowBlobAuthority,
            stored: &coven_protocol::blob::locator::StoredBlobRef,
        ) -> crate::sync::store::StoreError {
            match self
                .store
                .blob_key_fingerprint_for_test(authority, stored)
                .await
            {
                Ok(_) => panic!("invalid Circle blob authority must fail"),
                Err(error) => error,
            }
        }

        pub async fn load_circle_snapshot_refs(
            &self,
            circle_id: coven_protocol::CircleId,
            access: &coven_protocol::circle_activation::CircleEpochAccess,
        ) -> Result<
            Vec<(
                coven_protocol::store_commit::CircleSnapshotRef,
                coven_protocol::store_commit::CircleSnapshotMeta,
            )>,
            TestError,
        > {
            self.store
                .authorize_writer()
                .await?
                .circles()
                .snapshots()
                .load_circle_snapshot_refs_for_test(circle_id, access)
                .await
                .map_err(TestError::from)
        }

        pub async fn membership(
            &self,
        ) -> Result<coven_protocol::membership::MembershipChain, TestError> {
            self.store
                .membership_for_test()
                .await
                .map_err(TestError::from)
        }

        pub fn protocol_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
            self.store.protocol_root_for_test()
        }

        #[cfg(test)]
        pub async fn prepare_wrapped_key(
            &self,
            recipient: &str,
            value: coven_protocol::wrapped_store_key::WrappedStoreKey,
        ) -> Result<coven_protocol::wrapped_store_key::PreparedWrappedStoreKey, TestError> {
            self.store
                .prepare_wrapped_key_for_test(recipient, value)
                .await
        }

        #[cfg(test)]
        pub async fn membership_keyring_facts(&self) -> Result<([u8; 32], usize), TestError> {
            self.store.membership_keyring_facts_for_test().await
        }

        pub async fn publish_snapshot(
            &self,
            db_image: Vec<u8>,
            coverage: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, TestError> {
            self.publish_snapshot_at(db_image, coverage, "2026-07-16T00:00:00Z")
                .await
                .map_err(TestError::from)
        }

        pub async fn publish_snapshot_at(
            &self,
            db_image: Vec<u8>,
            coverage: coven_protocol::store_commit::CommitFrontier,
            created_at: &str,
        ) -> Result<coven_protocol::store_commit::SnapshotMeta, crate::sync::store::SnapshotError>
        {
            self.store
                .publish_snapshot_for_test(
                    coven_database::CreatedSnapshot::new(
                        staged_snapshot_image(&db_image),
                        Vec::new(),
                    ),
                    coverage,
                    created_at.to_string(),
                )
                .await
        }

        pub async fn resume_snapshot_publication(
            &self,
        ) -> Result<
            Option<coven_protocol::store_commit::SnapshotMeta>,
            crate::sync::store::SnapshotError,
        > {
            self.store
                .authorize_writer()
                .await
                .map_err(crate::sync::store::SnapshotError::from)?
                .resume_snapshot_publication()
                .await
        }

        /// Acknowledge `frontier`, and report what advancing this device's
        /// replay baseline over the snapshot it named retired.
        pub async fn publish_acknowledgement(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<Option<coven_database::AdvancedReplayBaseline>, TestError> {
            let staged = self
                .store
                .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:01Z".to_string())
                .await?;
            let published = self.store.drain_acknowledgements_for_test().await?;
            if published != 1 {
                return Err(TestError::invariant(format!(
                "snapshot acknowledgement fixture published {published} acknowledgements instead of one"
            )));
            }
            Ok(staged.baseline_advance)
        }

        pub async fn stage_acknowledgement(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<Option<coven_protocol::store_commit::StoreAck>, TestError> {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
                .map(|staged| staged.acknowledgement)
                .map_err(TestError::from)
        }

        /// Acknowledge `frontier` the way a device on a build without the
        /// advance did: publish the statement, leave the baseline where it is.
        pub async fn publish_acknowledgement_without_advancing(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<(), TestError> {
            self.store
                .stage_acknowledgement_without_advancing_for_test(
                    frontier,
                    "2026-07-16T00:00:03Z".to_string(),
                )
                .await?
                .ok_or_else(|| {
                    TestError::invariant(
                        "the fixture acknowledgement asserted nothing new".to_string(),
                    )
                })?;
            let published = self.store.drain_acknowledgements_for_test().await?;
            if published != 1 {
                return Err(TestError::invariant(format!(
                    "fixture published {published} acknowledgements instead of one"
                )));
            }
            Ok(())
        }

        /// Stand on the snapshot this device has acknowledged, the way the
        /// cycle does, and report what it did or why it did nothing.
        pub async fn stand_on_acknowledged_snapshot(
            &self,
        ) -> Result<crate::sync::store::ReplayBaselineAdvance, TestError> {
            self.store
                .stand_on_acknowledged_snapshot_for_test()
                .await
                .map_err(TestError::from)
        }

        /// Acknowledge `frontier` and report the whole pass: what it staged,
        /// if anything, and what standing on the acknowledged snapshots
        /// retired.
        pub async fn stage_acknowledgement_reporting_advance(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<crate::sync::store::StagedStoreAcknowledgement, TestError> {
            self.store
                .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:05Z".to_string())
                .await
                .map_err(TestError::from)
        }

        /// Stage an acknowledgement and report only what the baseline advance
        /// it licensed retired.
        pub async fn advance_baseline_by_acknowledging(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
        ) -> Result<Option<coven_database::AdvancedReplayBaseline>, TestError> {
            self.store
                .stage_acknowledgement_for_test(frontier, "2026-07-16T00:00:02Z".to_string())
                .await
                .map(|staged| staged.baseline_advance)
                .map_err(TestError::from)
        }

        pub async fn materialized_frontier(
            &self,
        ) -> Result<
            std::collections::BTreeMap<String, coven_protocol::store_commit::StoreBatchCommitRef>,
            TestError,
        > {
            self.db
                .materialized_frontier()
                .await
                .map_err(TestError::from)
        }

        pub async fn drain_acknowledgements(&self) -> Result<u64, TestError> {
            self.store
                .drain_acknowledgements_for_test()
                .await
                .map_err(TestError::from)
        }

        #[cfg(test)]
        pub async fn stage_acknowledgement_exact(
            &self,
            frontier: coven_protocol::store_commit::CommitFrontier,
            sync_time: String,
        ) -> Result<Option<coven_protocol::store_commit::StoreAck>, crate::sync::store::StoreAckError>
        {
            self.store
                .stage_acknowledgement_for_test(frontier, sync_time)
                .await
                .map(|staged| staged.acknowledgement)
        }

        #[cfg(test)]
        pub async fn acknowledgement_frontier(
            &self,
        ) -> Result<coven_protocol::store_commit::CommitFrontier, crate::sync::store::StoreAckError>
        {
            coven_protocol::store_commit::CommitFrontier::from_refs(
                self.db.materialized_frontier().await?,
            )
            .map_err(crate::sync::store::StoreAckError::Protocol)
        }

        /// Stage this device's acknowledgement of what it has materialized, and
        /// fail if there was nothing new to say — the tests that use this are
        /// testing what an acknowledgement does, so one has to be staged.
        /// [`Self::stage_current_acknowledgement_if_new`] is for the tests about
        /// whether one is staged at all.
        #[cfg(test)]
        pub async fn stage_current_acknowledgement(
            &self,
            sync_time: &str,
        ) -> Result<coven_protocol::store_commit::StoreAck, crate::sync::store::StoreAckError>
        {
            Ok(self
                .stage_current_acknowledgement_if_new(sync_time)
                .await?
                .expect("the standing acknowledgement no longer holds"))
        }

        #[cfg(test)]
        pub async fn stage_current_acknowledgement_if_new(
            &self,
            sync_time: &str,
        ) -> Result<Option<coven_protocol::store_commit::StoreAck>, crate::sync::store::StoreAckError>
        {
            let frontier = self.acknowledgement_frontier().await?;
            self.stage_acknowledgement_exact(frontier, sync_time.to_string())
                .await
        }

        #[cfg(any(test, feature = "test-utils"))]
        pub fn typed_device_id(&self) -> coven_protocol::store_commit::StoreDeviceId {
            self.device_id
                .parse()
                .expect("TestDevice retains a valid Store device id")
        }

        #[cfg(test)]
        pub async fn prepare_acknowledgement_candidate_for_test(
            &self,
            outbound: &coven_database::OutboundStoreAck,
        ) -> coven_protocol::prepared_commit::PreparedStoreOperationCommit {
            let mut writer = self
                .authorize_writer()
                .await
                .expect("authorize acknowledgement writer");
            let plan = writer
                .prepare_plan()
                .await
                .expect("prepare acknowledgement activation");
            plan.validate_acknowledgement(&outbound.ack.value)
                .expect("acknowledgement matches activation predecessor");
            let candidate = writer
                .prepare_candidate(
                    plan,
                    crate::sync::store::StoreOperationBatch::Acknowledgement {
                        reference: outbound.reference.clone(),
                        value: outbound.ack.value.clone(),
                        circle_acknowledgements: Vec::new(),
                    },
                )
                .await
                .expect("prepare acknowledgement candidate");
            self.prepare_acknowledgement_activation_for_test(
                outbound.reference.clone(),
                candidate.clone(),
            )
            .await
            .expect("persist acknowledgement candidate");
            candidate
        }

        #[cfg(test)]
        pub async fn drain_acknowledgements_exact(
            &self,
        ) -> Result<u64, crate::sync::store::StoreAckError> {
            self.store.drain_acknowledgements_for_test().await
        }

        #[cfg(test)]
        pub async fn stage_circle_acknowledgements(
            &self,
            frontier: &coven_protocol::store_commit::CommitFrontier,
            sync_time: &str,
        ) -> Result<(), crate::sync::store::StoreAckError> {
            self.store
                .stage_circle_acknowledgements_for_test(frontier, sync_time)
                .await
        }

        pub async fn load_commit_ancestry_until(
            &self,
            start: coven_protocol::store_commit::StoreBatchCommitRef,
            coverage: &coven_protocol::store_commit::CommitFrontier,
        ) -> Result<
            Vec<(
                coven_protocol::store_commit::StoreBatchCommitRef,
                coven_protocol::store_commit::VerifiedStoreBatchCommit,
            )>,
            TestError,
        > {
            self.store
                .load_commit_ancestry_until_for_test(start, coverage)
                .await
                .map_err(TestError::from)
        }

        pub async fn export_activated_device_continuation(
            &self,
        ) -> Result<coven_protocol::recovery::ActivatedContinuation, TestError> {
            self.store
                .export_activated_device_continuation_for_test()
                .await
                .map_err(TestError::from)
        }

        pub async fn latest_store_position(
            &self,
        ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, TestError> {
            self.store
                .latest_local_store_position()
                .await
                .map_err(TestError::from)
        }

        pub async fn pull_store(
            &self,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            TestPullError,
        > {
            let routing_encryption = coven_keys::encryption::EncryptionService::from_key([42; 32]);
            self.pull_store_with_encryption(&routing_encryption).await
        }

        /// Pull, then run the eager cache fill the sync loop runs behind its
        /// cycles. A pull records what its rows bind and downloads none of it,
        /// so this is what makes an eager blob's bytes local.
        pub async fn pull_store_and_fill_eager(
            &self,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            TestPullError,
        > {
            let pulled = self.pull_store().await?;
            crate::sync::test_owner_graph::TestOwnerGraph::new(
                self.db.clone(),
                self.store_dir.clone(),
            )
            .fill_eager_cache(self.storage.clone())
            .await
            .expect("fill the eager cache behind the pull");
            Ok(pulled)
        }

        pub async fn pull_store_with_encryption(
            &self,
            routing_encryption: &coven_keys::encryption::EncryptionService,
        ) -> Result<
            (
                std::collections::BTreeMap<String, u64>,
                crate::sync::store::StorePullResult,
            ),
            TestPullError,
        > {
            let mut authorization = self.store.authorize_writer().await?;
            let result = authorization.pull(Some(routing_encryption)).await?;
            let sequences = result
                .frontier
                .iter()
                .map(|(stream, reference)| (stream.clone(), reference.coord.sequence()))
                .collect();
            Ok((sequences, result))
        }
    }
}

pub use test_device::{TestDevice, TestDeviceSigningAuthority};

/// The Store a completed join installed, for a test that only asks it
/// questions.
///
/// A joining device never opens a database of its own — the snapshot install is
/// what creates it — so a fixture cannot hand one back without handing out a
/// raw database. It hands back this instead: the questions the join is asserted
/// on, and nothing to write through.
#[cfg(test)]
pub struct JoinedTestStore {
    database: coven_database::StoreDatabase,
}

#[cfg(test)]
impl JoinedTestStore {
    pub async fn latest_local_store_device_registration(
        &self,
    ) -> Result<Option<coven_database::DurableDeviceRegistration>, coven_database::DbError> {
        self.database.latest_local_store_device_registration().await
    }

    pub async fn query_test_text(&self, sql: &str) -> String {
        self.database
            .test_query_optional_text(sql.to_string())
            .await
            .expect("test text query failed")
            .expect("test text query matched no row")
    }
}

/// Open the Store a join installed, once the join has closed its own handle.
#[cfg(test)]
fn open_joined_test_store(
    store_dir: &StoreDir,
    device_id: String,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: &[coven_database::Migration],
) -> Result<Database, TestError> {
    Ok(Database::open(
        &store_dir.db_path(),
        synced_tables,
        coven_protocol::blob::BLOB_TOMBSTONE_GRACE,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        device_id,
        std::sync::Arc::new(coven_foundation::clock::SystemClock),
        migrations,
    )?)
}

/// Open the device a join installed, over the database the install created.
///
/// A joining device never opens a database of its own: the snapshot install is
/// what creates the file, so the join has to finish and close before anything
/// else opens it. This is that second open, and it is the only handle a test
/// gets — the fixtures hand back a device, not a database.
#[cfg(test)]
async fn open_joined_test_device(
    store_dir: StoreDir,
    identity: &UserKeypair,
    storage: std::sync::Arc<coven_storage::CloudSyncConnection>,
    device_id: String,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: &[coven_database::Migration],
) -> Result<TestDevice, TestError> {
    let database = open_joined_test_store(&store_dir, device_id, synced_tables, migrations)?;
    TestDevice::load(&database, store_dir, storage, identity.clone())
        .await
        .map_err(TestError::from)
}

struct TestStoreProducers {
    unassigned: Option<TestDevice>,
    by_name: HashMap<String, TestDevice>,
}

impl TestStore {
    pub fn root(&self) -> coven_protocol::store_commit::StoreRootRef {
        self.root.clone()
    }

    pub async fn execute_unscoped_host_sql_for_test(
        &self,
        sql: impl Into<String>,
    ) -> Result<(), coven_database::HostWriteError<coven_database::DbError>> {
        self.founder
            .execute_unscoped_host_sql_for_test(sql.into())
            .await
    }

    pub async fn bind_founder_device(
        &self,
        database: &Database,
        store_dir: StoreDir,
    ) -> Result<TestDevice, crate::sync::store::StoreError> {
        self.bind_device_in(database, store_dir, &self.signer).await
    }

    pub async fn open_store_with_identity(
        &self,
        database: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, crate::sync::store::StoreInitializationError> {
        self.open_store_with_storage(
            coven_database::StoreDatabase::new(database),
            self.storage.clone(),
            store_dir,
            identity,
        )
        .await
    }

    pub async fn open_store_with_storage(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store::Store, crate::sync::store::StoreInitializationError> {
        crate::sync::store::Store::open(database, storage, store_dir, &self.root, identity)
            .await
            .map(|initialized| initialized.into_parts().0)
    }

    pub async fn open_founder_store_with_storage(
        &self,
        database: coven_database::StoreDatabase,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: StoreDir,
    ) -> Result<crate::sync::store::Store, crate::sync::store::StoreInitializationError> {
        self.open_store_with_storage(database, storage, store_dir, &self.signer)
            .await
    }

    pub fn tombstone_deletions(&self) -> Vec<String> {
        self.home.deletes_seen()
    }

    pub fn tombstone_provider_key(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> String {
        coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(self.storage.as_ref()),
        )
    }

    pub fn stored_tombstone_bytes(&self, key: &str) -> Option<Vec<u8>> {
        let stored = self.home.get(key)?;
        let aad_context = coven_storage::cloud_aad_context(self.storage.store_id(), key);
        coven_storage::CloudSyncCipherStateAccess::open(self.storage.as_ref(), stored, &aad_context)
            .ok()
    }

    pub async fn plant_tombstone_bytes(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let aad_context = coven_storage::cloud_aad_context(self.storage.store_id(), key);
        let stored = coven_storage::CloudSyncCipherStateAccess::seal(
            self.storage.as_ref(),
            bytes,
            &aad_context,
        );
        self.storage
            .write_provider_bytes_for_test(key, stored)
            .await
    }

    /// Plants a typed tombstone through the Store's exact cloud layout while
    /// bypassing the signing drain, so deletion tests can exercise rejected
    /// signatures and Store identities.
    pub async fn plant_tombstone(&self, tombstone: &crate::blob::delete::BlobTombstoneJson) {
        let key = self.tombstone_provider_key(&tombstone.stored);
        let bytes = serde_json::to_vec(tombstone).expect("serialize tombstone");
        self.plant_tombstone_bytes(&key, bytes)
            .await
            .expect("plant tombstone");
    }

    pub fn fail_exact_delete_on_call(&self, call: usize) {
        self.home.fail_exact_delete_on_call(call);
    }

    pub fn fail_nth_exact_delete_of(
        &self,
        slots: &[&coven_protocol::objects::ObjectSlot],
        call: usize,
    ) {
        self.home.fail_nth_exact_delete_of(slots, call);
    }

    pub fn sort_provider_listings(&self) {
        self.home.sort_listings();
    }

    pub fn provider_object_is_absent(&self, logical_key: &str) -> bool {
        self.home.get(logical_key).is_none()
    }

    pub fn arm_provider_write_failures(&self) {
        self.home.arm_write_failures();
    }

    pub fn fail_exact_create_before_call(&self, call: usize) {
        self.home.fail_exact_create_before_call(call);
    }

    pub fn exact_creates(&self) -> Vec<coven_protocol::objects::ObjectSlot> {
        self.home.exact_creates()
    }

    pub fn clear_exact_creates(&self) {
        self.home.clear_exact_creates();
    }

    pub fn fail_exact_create_after_call(&self, call: usize) {
        self.home.fail_exact_create_after_call(call);
    }

    pub fn pause_after_exact_create_call(
        &self,
        call: usize,
    ) -> (
        std::sync::Arc<tokio::sync::Notify>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.home.pause_after_exact_create_call(call)
    }

    pub async fn pull_with_storage_for_test(
        &self,
        database: &Database,
        storage: Arc<dyn coven_storage::CloudSyncObjectStorage>,
        store_dir: &StoreDir,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, crate::sync::cycle::SyncCycleFailure> {
        let store = crate::sync::store::Store::load(
            coven_database::StoreDatabase::new(database),
            storage,
            store_dir.clone(),
            self.signer.clone(),
        )
        .await
        .map_err(|error| crate::sync::cycle::SyncCycleFailure::operation("load Store", error))?;
        store
            .authorize_writer()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("authorize Store writer", error)
            })?
            .pull(routing_encryption)
            .await
    }

    pub async fn founder_recovery_authority(
        &self,
    ) -> coven_protocol::recovery::OwnerRecoveryAuthority {
        let protocol_root = self.founder.protocol_root_for_test();
        let owner_grant = protocol_root.descriptor.founder_grant.clone();
        let activation = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
            &self.root,
            &coven_keys::keys::public_key_hex(&self.signer),
            &owner_grant,
            &protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        coven_protocol::recovery::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(self.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: coven_protocol::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: coven_protocol::store_commit::OwnerRecoveryPosition::BeforeFirst {
                    activation,
                },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    pub async fn run_founder_cycle(
        &self,
        observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
    ) -> Result<crate::sync::cycle::SyncCycleResult, crate::sync::cycle::SyncCycleFailure> {
        self.founder.run_cycle(observer).await
    }

    pub async fn publish_fixture_position(&self, note_id: &str) -> u64 {
        self.founder.publish_fixture_position(note_id).await
    }

    pub async fn create_exact_opaque_blob(
        &self,
        namespace: &str,
        id: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_opaque_blob(namespace, id, bytes)
            .await
    }

    pub async fn create_exact_browsable_blob(
        &self,
        namespace: &str,
        id: &str,
        cloud_path: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .create_exact_browsable_blob(namespace, id, cloud_path, bytes)
            .await
    }

    pub async fn publish_exact_remote_blob_binding(
        &self,
        root_id: &str,
        row_id: &str,
        bytes: &[u8],
    ) -> coven_protocol::blob::locator::StoredBlobRef {
        self.founder
            .publish_exact_remote_blob_binding(root_id, row_id, bytes)
            .await
    }

    pub async fn pull_into_result(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> Result<
        (
            std::collections::BTreeMap<String, u64>,
            crate::sync::store::StorePullResult,
        ),
        TestPullError,
    > {
        let device = Box::pin(self.open_into(db, store_dir.clone()))
            .await
            .map_err(TestPullError::Open)?;
        device.pull_store().await
    }

    pub async fn pull_into(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        self.pull_into_result(db, store_dir)
            .await
            .expect("pull exact test Store")
    }

    /// Pull, then run the eager cache fill the sync loop runs behind its cycles.
    ///
    /// A pull records what its rows bind and downloads none of it, so a test
    /// that wants an eager blob's bytes on disk has to do what the loop does.
    pub async fn pull_and_fill_into(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> (
        std::collections::BTreeMap<String, u64>,
        crate::sync::store::StorePullResult,
    ) {
        let pulled = self.pull_into(db, store_dir).await;
        crate::sync::test_owner_graph::TestOwnerGraph::new(
            coven_database::StoreDatabase::new(db),
            store_dir.clone(),
        )
        .fill_eager_cache(self.storage.clone())
        .await
        .expect("fill the eager cache behind the pull");
        pulled
    }

    pub async fn promote_active_member_fixture(
        &self,
        owner_db: &Database,
        owner_db_store_dir: StoreDir,
        member_db: &Database,
        member_db_store_dir: StoreDir,
        owner: &UserKeypair,
        member: &UserKeypair,
        encryption: &coven_keys::encryption::EncryptionService,
    ) -> Result<coven_protocol::circle_control::StoreMembershipStateRef, TestError> {
        let owner_device = self
            .bind_device_in(owner_db, owner_db_store_dir.clone(), owner)
            .await?;
        let member_device = self
            .bind_device_in(member_db, member_db_store_dir.clone(), member)
            .await?;
        let request = owner_device
            .begin_owner_promotion_for_device(member_device.typed_device_id())
            .await?;
        let acceptance = member_device.accept_owner_promotion(request).await?;
        let finalized = owner_device
            .finalize_owner_promotion(encryption, acceptance)
            .await?;
        let (_, pull) = member_device.pull_store_with_encryption(encryption).await?;
        if !pull.held_positions.is_empty() {
            return Err(TestError::invariant(format!(
                "Owner promotion pull held signed positions: {:?}",
                pull.held_positions
            )));
        }
        Ok(finalized)
    }

    pub async fn create(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, TestError> {
        Box::pin(Self::create_with_protection(
            db,
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    pub async fn create_with_connection(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<TestStoreParts, TestError> {
        Box::pin(Self::create_with_protection_database(
            coven_database::StoreDatabase::new(db),
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
        ))
        .await
    }

    pub async fn create_encrypted(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> Result<Arc<Self>, TestError> {
        Self::create_with_protection(
            db,
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(encryption),
            coven_storage::BlobPathScheme::Hashed,
        )
        .await
    }

    pub async fn create_encrypted_with_connection(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> Result<TestStoreParts, TestError> {
        Self::create_with_protection_database(
            coven_database::StoreDatabase::new(db),
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(encryption),
            coven_storage::BlobPathScheme::Hashed,
        )
        .await
    }

    pub async fn create_with_database(
        database: coven_database::StoreDatabase,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, TestError> {
        Box::pin(Self::create_with_protection_database(
            database,
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Encrypted(
                coven_keys::encryption::EncryptionService::from_key([42; 32]),
            ),
            coven_storage::BlobPathScheme::Hashed,
        ))
        .await
        .map(|(store, _)| store)
    }

    /// A store whose home keeps blobs **browsable**: stored in the clear under
    /// readable paths. The counterpart of [`Self::create`], whose home is opaque
    /// (sealed under the store key, hashed paths). The pair is fixed per home,
    /// so a test that needs the browsable verification story needs this store.
    pub async fn create_browsable(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<Arc<Self>, TestError> {
        Box::pin(Self::create_with_protection(
            db,
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Plaintext,
            coven_storage::BlobPathScheme::Plain,
        ))
        .await
    }

    pub async fn create_browsable_with_connection(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
    ) -> Result<TestStoreParts, TestError> {
        Box::pin(Self::create_with_protection_database(
            coven_database::StoreDatabase::new(db),
            store_dir,
            store_id,
            signer,
            home,
            coven_storage::CloudCipher::Plaintext,
            coven_storage::BlobPathScheme::Plain,
        ))
        .await
    }

    async fn create_with_protection(
        db: &Database,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: coven_storage::CloudCipher,
        blob_paths: coven_storage::BlobPathScheme,
    ) -> Result<Arc<Self>, TestError> {
        Self::create_with_protection_database(
            coven_database::StoreDatabase::new(db),
            store_dir,
            store_id,
            signer,
            home,
            cipher,
            blob_paths,
        )
        .await
        .map(|(store, _)| store)
    }

    async fn create_with_protection_database(
        database: coven_database::StoreDatabase,
        store_dir: StoreDir,
        store_id: &str,
        signer: UserKeypair,
        home: std::sync::Arc<coven_storage::cloud::test_utils::InMemoryCloudHome>,
        cipher: coven_storage::CloudCipher,
        blob_paths: coven_storage::BlobPathScheme,
    ) -> Result<TestStoreParts, TestError> {
        // Counted the way a shipped home is counted, at the one boundary every
        // provider call crosses, so a test can assert a settled cycle's budget
        // in the same unit the cycle log reports it in.
        let counted: std::sync::Arc<dyn coven_storage::ExactCloudHome> =
            std::sync::Arc::new(coven_storage::cloud::CountingCloudHome::new(home.clone()));
        let provider_requests = coven_storage::cloud::CloudHome::provider_requests(&*counted);
        let storage = std::sync::Arc::new(coven_storage::CloudSyncConnection::new(
            counted,
            cipher,
            blob_paths,
            store_id,
            signer.clone(),
        ));
        let founder = TestDevice::create_with_database(
            database,
            store_dir,
            storage.clone(),
            store_id,
            signer.clone(),
        )
        .await?;
        let root = founder.store_root().clone();
        let store = Arc::new(Self {
            home,
            provider_requests,
            storage: storage.clone(),
            root,
            signer,
            founder: founder.clone(),
            producers: Arc::new(tokio::sync::Mutex::new(TestStoreProducers {
                unassigned: Some(founder),
                by_name: HashMap::new(),
            })),
        });
        Ok((store, storage))
    }

    /// Provider operations asked for so far. The unit the cycle log reports in,
    /// so a budget written here is the budget read there.
    pub fn provider_requests_issued(&self) -> u64 {
        self.provider_requests
            .as_ref()
            .expect("test Store home is counted")
            .issued()
    }

    pub fn protocol_founder_pubkey(&self) -> String {
        coven_keys::keys::public_key_hex(&self.signer)
    }

    pub async fn create_exact_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: &[u8],
    ) -> Result<coven_protocol::objects::ExactObjectRef, TestError> {
        let slot = self
            .storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await?;
        let prepared =
            self.storage
                .prepare_protocol_object(context, slot, semantic_prefix, bytes.to_vec())?;
        self.storage.create_protocol_object(&prepared).await?;
        Ok(prepared.reference().clone())
    }

    /// Publish one object from the bytes and reference that identify it, for
    /// tests holding a candidate that carries references rather than uploads.
    pub async fn publish_exact_protocol_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
        bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let prepared = coven_protocol::objects::PreparedExactObject::new(object.clone(), bytes)?;
        self.storage.create_protocol_object(&prepared).await
    }

    pub async fn publish_prepared_protocol_object(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.create_protocol_object(prepared).await
    }

    pub async fn read_exact_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        object: &coven_protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.storage
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    pub async fn contains_blob_object(&self, reference: &coven_protocol::blob::RowBlobRef) -> bool {
        match reference.stored() {
            Some(stored) => self
                .contains_stored_blob_object(stored)
                .await
                .unwrap_or_else(|error| panic!("verify exact blob object: {error}")),
            None => false,
        }
    }

    pub async fn contains_stored_blob_object(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, coven_protocol::objects::StorageError> {
        match self.storage.verify_blob_object(stored).await {
            Ok(()) => Ok(true),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn contains_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, coven_storage::cloud::CloudHomeError> {
        let key = coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(self.storage.as_ref()),
        );
        coven_storage::cloud::CloudHome::exists(self.home.as_ref(), &key).await
    }

    pub async fn contains_membership_rollup(
        &self,
        rollup: &coven_protocol::store_commit::MembershipRollupRef,
    ) -> Result<bool, TestError> {
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreMembershipRollup,
        );
        let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &rollup.object,
            coven_protocol::objects::ProtectedObjectDomain::StoreMembershipRollup.extension(),
        )?;
        match self
            .storage
            .read_protocol_object(&context, &rollup.object, &prefix)
            .await
        {
            Ok(_) => Ok(true),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(TestError::from(error)),
        }
    }

    pub async fn contains_circle_snapshot_image(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        meta: &coven_protocol::store_commit::CircleSnapshotMeta,
    ) -> Result<bool, TestError> {
        let access = self
            .founder
            .circle_epoch_access(circle_id, meta.control.clone())
            .await?
            .ok_or_else(|| {
                TestError::invariant(
                    "the Circle snapshot control has no retained access".to_string(),
                )
            })?;
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        let prefix = coven_protocol::store_commit::semantic_prefix_from_exact_object(
            &meta.bootstrap.image.object,
            coven_protocol::objects::ProtectedObjectDomain::CircleSnapshotImage.extension(),
        )?;
        match self
            .storage
            .read_protocol_object(&context, &meta.bootstrap.image.object, &prefix)
            .await
        {
            Ok(_) => Ok(true),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(TestError::from(error)),
        }
    }

    pub async fn circle_package_in(
        &self,
        commit_ref: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> coven_protocol::store_commit::CirclePackageRef {
        let commit = self
            .founder
            .load_commit_for_test(commit_ref)
            .await
            .expect("load the exact Circle package commit");
        let [package] = commit.value().circle_packages() else {
            panic!("the commit must carry exactly one Circle package");
        };
        package.clone()
    }

    pub async fn circle_package_object_present(
        &self,
        package: &coven_protocol::store_commit::CirclePackageRef,
        activation: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> bool {
        let access = self
            .founder
            .circle_epoch_access(package.circle_id, package.control.clone())
            .await
            .expect("resolve Circle package access")
            .expect("the package's control stays retained after its epoch closed");
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CirclePackage,
        );
        let prefix = coven_protocol::store_commit::circle_package_semantic_prefix(
            package.circle_id,
            package.package.candidate_family,
            &activation.coord.stream_id.to_string(),
            activation.coord.sequence(),
            package.package.content_hash,
        );
        match self
            .storage
            .read_protocol_object(&context, &package.package.object, &prefix)
            .await
        {
            Ok(_) => true,
            Err(coven_protocol::objects::StorageError::NotFound(_)) => false,
            Err(error) => panic!("read the exact Circle package object: {error}"),
        }
    }

    pub async fn publish_competing_store_head(
        &self,
        journal: &coven_protocol::circle_journal::CircleOperationJournal,
    ) -> (
        coven_protocol::objects::ExactObjectRef,
        coven_protocol::objects::ExactObjectRef,
    ) {
        let candidate = journal.commit().expect("parse candidate Store commit");
        let coord = journal.operation().commit_ref.coord.clone();
        let head = &journal.operation().policy.head;
        let registration = self
            .founder
            .activated_store_device_registration_for_test(candidate.author_registration.clone())
            .await
            .expect("load candidate author registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive candidate device signer");
        let schema_version = self.founder.schema_version();
        let package = coven_protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate.candidate_family(),
            candidate.write_id.clone(),
            coord.clone(),
            schema_version,
            b"competing valid package".to_vec(),
            Vec::new(),
        )
        .expect("construct competing package");
        let package_bytes = package.to_bytes();
        let package_prefix = coven_protocol::store_commit::package_semantic_prefix(
            candidate.candidate_family(),
            &coord.stream_id.to_string(),
            candidate.seq(),
            coven_protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("reserve competing package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare competing package");
        self.storage
            .create_protocol_object(&package_prepared)
            .await
            .expect("publish competing package");
        let membership = self
            .founder
            .membership()
            .await
            .expect("load competing commit membership");
        let predecessor = membership
            .write_grant_authority(&registration.value().author_pubkey)
            .expect("competing author has an active write grant");
        let winner = coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            self.root.store_root_hash,
            candidate.write_id.clone(),
            coord.clone(),
            candidate.author_registration.clone(),
            registration.value(),
            candidate.order.clone(),
            candidate.membership_state.clone(),
            candidate.device_state.clone(),
            coven_protocol::store_commit::StoreOperationMembershipAuthority { predecessor },
            coven_protocol::store_commit::StoreCommitOperationsInput {
                store_package: Some(coven_protocol::store_commit::StorePackageInput {
                    candidate_family: candidate.candidate_family(),
                    schema_version,
                    bytes: &package_bytes,
                    object: package_prepared.reference().clone(),
                }),
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
            &device_signer,
        )
        .expect("sign competing commit");
        let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            winner.candidate_family(),
            &coord.stream_id.to_string(),
            winner.seq(),
            winner.commit_hash(),
        );
        let commit_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("reserve competing commit slot");
        let commit_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                winner.to_bytes(),
            )
            .expect("prepare competing commit");
        self.storage
            .create_protocol_object(&commit_prepared)
            .await
            .expect("publish competing commit");
        let winner_ref = coven_protocol::store_commit::StoreBatchCommitRef::from_commit(
            &winner,
            coord,
            commit_prepared.reference().clone(),
        )
        .expect("reference competing commit");
        assert_ne!(winner_ref, journal.operation().commit_ref);
        let winner_head = coven_protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.author_registration.clone(),
            winner_ref,
            head.successor.clone(),
            &device_signer,
        )
        .expect("sign competing head");
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_slot = journal
            .operation()
            .prepared_objects
            .get("store-head")
            .expect("candidate carries a prepared Store head")
            .slot()
            .clone();
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.seq(),
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                head_slot,
                &head_prefix,
                winner_head.to_bytes(),
            )
            .expect("prepare competing head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish competing head");
        (
            commit_prepared.reference().clone(),
            head_prepared.reference().clone(),
        )
    }

    pub async fn publish_third_candidate_winner(
        &self,
        peer_db: &Database,
        candidate: &coven_database::BlockedMergeCandidate,
    ) {
        let registration = coven_database::StoreDatabase::new(peer_db)
            .activated_store_device_registration(
                candidate.commit.value().author_registration.clone(),
            )
            .await
            .expect("load third-winner device registration");
        let device_signer = registration
            .value()
            .device_signer(&self.signer)
            .expect("derive third-winner device signer");
        let coord = candidate.head.commit.coord.clone();
        let candidate_family = candidate.commit.value().candidate_family();
        let package = coven_protocol::audience_package::AudiencePackage::store(
            self.root.store_root_hash,
            candidate_family,
            candidate.commit.value().write_id.clone(),
            coord.clone(),
            peer_db.schema_version(),
            b"third winner package".to_vec(),
            Vec::new(),
        )
        .expect("construct third winner package");
        let coven_protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        } = coord.clone();
        let package_bytes = package.to_bytes();
        let package_context = coven_protocol::objects::ProtocolObjectContext::store_encrypted(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StorePackage,
        );
        let package_prefix = coven_protocol::store_commit::package_semantic_prefix(
            candidate_family,
            &stream_id.to_string(),
            sequence,
            coven_protocol::store_commit::ObjectHash::digest(&package_bytes),
        );
        let package_slot = self
            .storage
            .allocate_protocol_slot(&package_context, &package_prefix, ".pkg")
            .await
            .expect("allocate third winner package slot");
        let package_prepared = self
            .storage
            .prepare_protocol_object(
                &package_context,
                package_slot,
                &package_prefix,
                package_bytes.clone(),
            )
            .expect("prepare third winner package");
        let third = coven_protocol::store_commit::StoreBatchCommit::signed_operations(
            self.root.store_root_hash,
            candidate.commit.value().write_id.clone(),
            coord.clone(),
            candidate.commit.value().author_registration.clone(),
            registration.value(),
            candidate.commit.value().order.clone(),
            candidate.commit.value().membership_state.clone(),
            candidate.commit.value().device_state.clone(),
            candidate
                .commit
                .value()
                .operations_membership_authority()
                .expect("load third winner membership authority"),
            coven_protocol::store_commit::StoreCommitOperationsInput {
                store_package: Some(coven_protocol::store_commit::StorePackageInput {
                    candidate_family,
                    schema_version: peer_db.schema_version(),
                    bytes: &package_bytes,
                    object: package_prepared.reference().clone(),
                }),
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
            &device_signer,
        )
        .expect("sign third ordinary winner");
        let commit_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreCommit,
        );
        let commit_prefix = coven_protocol::store_commit::commit_semantic_prefix(
            third.candidate_family(),
            &stream_id.to_string(),
            sequence,
            third.commit_hash(),
        );
        let commit_slot = self
            .storage
            .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
            .await
            .expect("allocate third winner commit slot");
        let third_prepared = self
            .storage
            .prepare_protocol_object(
                &commit_context,
                commit_slot,
                &commit_prefix,
                third.to_bytes(),
            )
            .expect("prepare third winner commit");
        self.storage
            .create_protocol_object(&third_prepared)
            .await
            .expect("publish third winner commit");
        let third_ref = coven_protocol::store_commit::StoreBatchCommitRef::from_commit(
            &third,
            coord,
            third_prepared.reference().clone(),
        )
        .expect("reference third winner commit");
        let third_head = coven_protocol::store_commit::StoreDeviceHead::signed(
            self.root.store_root_hash,
            candidate.commit.value().author_registration.clone(),
            third_ref,
            candidate.head.successor.clone(),
            &device_signer,
        )
        .expect("sign third winner head");
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let head_prefix = coven_protocol::store_commit::head_slot_prefix(
            &candidate
                .commit
                .value()
                .author_registration
                .device_id
                .to_string(),
            sequence,
        );
        let head_prepared = self
            .storage
            .prepare_protocol_object(
                &head_context,
                candidate.head_object.slot().clone(),
                &head_prefix,
                third_head.to_bytes(),
            )
            .expect("prepare third winner head");
        self.storage
            .create_protocol_object(&head_prepared)
            .await
            .expect("publish third winner head");
    }

    pub async fn overwrite_membership_head(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
        head: &coven_protocol::membership::AuthorHead,
    ) {
        self.storage
            .delete_protocol_object(&reference.object)
            .await
            .expect("delete exact head before replacement");
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
        );
        let prefix = coven_protocol::store_commit::membership_head_slot_prefix(
            &reference.coord.author_pubkey,
            &reference.coord.author_owner_grant,
            reference.coord.stream_id,
            reference.coord.seq,
        );
        let prepared = self
            .storage
            .prepare_protocol_object(
                &context,
                reference.object.slot().clone(),
                &prefix,
                serde_json::to_vec(head).expect("serialize replacement head"),
            )
            .expect("prepare replacement head");
        self.storage
            .create_protocol_object(&prepared)
            .await
            .expect("write replacement head");
    }

    pub async fn delete_membership_head_for_test(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.storage.delete_protocol_object(&reference.object).await
    }

    pub async fn pending_device_join_observation(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: &coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinObservation<'_>, TestError> {
        self.founder
            .pending_device_join_observation_for_test(pending, offer)
            .await
    }

    pub async fn open_pending_device_join(
        &self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        identity: &UserKeypair,
        offer: coven_protocol::store_commit::device_join_exchange::DeviceJoinOffer,
    ) -> Result<crate::sync::store::PendingDeviceJoinAuthority<'_>, TestError> {
        self.founder
            .open_pending_device_join_for_test(pending, identity, offer)
            .await
    }

    pub async fn prepare_snapshot_bootstrap<'a>(
        &'a self,
        membership_floor: &coven_protocol::membership::MembershipFloor,
        binary_schema_version: u32,
        target_path: &std::path::Path,
        restorer_identity: &UserKeypair,
    ) -> Result<crate::sync::store::PreparedSnapshotBootstrap<'a>, crate::sync::store::SnapshotError>
    {
        self.founder
            .prepare_snapshot_bootstrap_for_test(
                membership_floor,
                binary_schema_version,
                target_path,
                restorer_identity,
            )
            .await
    }

    pub async fn bind_device_in(
        &self,
        db: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<TestDevice, crate::sync::store::StoreError> {
        TestDevice::load_with_database(
            coven_database::StoreDatabase::new(db),
            std::sync::Arc::new(self.storage.connection_for_test_identity(identity.clone())),
            identity.clone(),
            store_dir,
        )
        .await
    }

    pub async fn bind_device(
        &self,
        db: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<TestDevice, crate::sync::store::StoreError> {
        self.bind_device_in(db, store_dir, identity).await
    }

    pub async fn drain_uploads(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: &coven_foundation::store_dir::StoreDir,
        clock: &dyn coven_foundation::clock::Clock,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        observer: Option<&dyn coven_protocol::blob::BlobTransitionObserver>,
    ) -> Result<crate::blob::DrainOutcome, TestError> {
        let store = self
            .bind_store_device(database, store_dir.clone(), &self.signer)
            .await?;
        store
            .drain_uploads(clock, routing_encryption, observer)
            .await
    }

    pub async fn activate_joined_device(
        &self,
        observer_db: &Database,
        observer_store_dir: StoreDir,
        joining_db: &Database,
        joining_store_dir: StoreDir,
        joining_identity: &UserKeypair,
        published_at: &str,
    ) -> Result<TestDevice, TestError> {
        let observer = self
            .bind_device_in(observer_db, observer_store_dir, &self.signer)
            .await?;
        TestDevice::activate_joined(
            observer,
            coven_database::StoreDatabase::new(joining_db),
            joining_store_dir,
            joining_identity,
            published_at,
            std::sync::Arc::new(
                self.storage
                    .connection_for_test_identity(joining_identity.clone()),
            ),
        )
        .await
    }

    /// Join a device through the production shape: snapshot install first,
    /// then only the history published after it. Hands back the database the
    /// install created, because the joining device's database is the image.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub async fn activate_joined_device_from_snapshot(
        &self,
        observer_db: &Database,
        observer_store_dir: StoreDir,
        joining_store_dir: StoreDir,
        joining_identity: &UserKeypair,
        published_at: &str,
        synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        migrations: Vec<coven_database::Migration>,
        binary_schema_version: u32,
    ) -> Result<TestDevice, TestError> {
        let observer = self
            .bind_device_in(observer_db, observer_store_dir, &self.signer)
            .await?;
        TestDevice::activate_joined_from_snapshot(
            observer,
            joining_store_dir,
            joining_identity,
            published_at,
            std::sync::Arc::new(
                self.storage
                    .connection_for_test_identity(joining_identity.clone()),
            ),
            synced_tables,
            migrations,
            binary_schema_version,
        )
        .await
    }

    pub async fn bind_store_device(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: StoreDir,
        identity: &UserKeypair,
    ) -> Result<TestDevice, TestError> {
        if identity.public_key() != self.signer.public_key() {
            return Err(TestError::invariant(
                "custom Store database binding requires the founder identity".to_string(),
            ));
        }
        TestDevice::load_with_database(
            database.clone(),
            std::sync::Arc::new(self.storage.connection_for_test_identity(identity.clone())),
            identity.clone(),
            store_dir,
        )
        .await
        .map_err(TestError::from)
    }

    pub async fn admit_member(
        &self,
        db: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
        member_pubkey: &str,
        member_email: Option<&str>,
        role: coven_protocol::membership::MemberRole,
        encryption: &coven_keys::encryption::EncryptionService,
        store_name: &str,
    ) -> Result<crate::sync::store::MemberAdmission, crate::sync::store::MembershipOpsError> {
        let device = self
            .bind_device_in(db, store_dir, identity)
            .await
            .map_err(crate::sync::store::MembershipOpsError::Store)?;
        device
            .admit_member(
                member_pubkey,
                member_email,
                role,
                encryption,
                self.storage.store_id(),
                store_name,
            )
            .await
    }

    pub async fn admit_and_activate_peer(
        &self,
        observer_db: &Database,
        observer_db_store_dir: StoreDir,
        peer_db: &Database,
        peer_db_store_dir: StoreDir,
        peer: &UserKeypair,
    ) -> Result<TestDevice, TestError> {
        self.admit_member(
            observer_db,
            observer_db_store_dir.clone(),
            &self.signer,
            &pubkey_hex(peer),
            None,
            coven_protocol::membership::MemberRole::Member,
            &coven_keys::encryption::EncryptionService::from_key([42; 32]),
            "Test Store",
        )
        .await?;
        self.activate_joined_device(
            observer_db,
            observer_db_store_dir.clone(),
            peer_db,
            peer_db_store_dir.clone(),
            peer,
            "2026-07-16T00:00:00Z",
        )
        .await
    }

    pub async fn remove_member(
        &self,
        db: &Database,
        store_dir: StoreDir,
        identity: &UserKeypair,
        member_pubkey: &str,
        encryption: &coven_keys::encryption::EncryptionService,
        master_keys: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<String, crate::sync::store::MembershipOpsError> {
        let device = self
            .bind_device_in(db, store_dir, identity)
            .await
            .map_err(crate::sync::store::MembershipOpsError::Store)?;
        device
            .remove_member(
                member_pubkey,
                encryption,
                master_keys,
                self.storage.as_ref(),
                self.storage.as_ref(),
            )
            .await
    }

    pub async fn device_id(&self, name: &str) -> Result<String, TestError> {
        self.ensure_producer_registered(name).await?;
        let producers = self.producers.lock().await;
        Ok(producers
            .by_name
            .get(name)
            .expect("registered test producer exists")
            .device_id())
    }

    pub async fn latest_store_position(
        &self,
    ) -> Result<Option<coven_protocol::store_commit::StoreBatchCommitRef>, TestError> {
        self.founder.latest_store_position().await
    }

    pub async fn load_commit_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<coven_protocol::store_commit::VerifiedStoreBatchCommit, TestError> {
        self.founder
            .load_commit_for_test(reference)
            .await
            .map_err(TestError::from)
    }

    pub async fn load_membership_head_for_test(
        &self,
        reference: &coven_protocol::membership::MembershipHeadRef,
    ) -> Result<coven_protocol::membership::AuthorHead, TestError> {
        self.founder
            .load_membership_head_for_test(reference)
            .await
            .map_err(TestError::from)
    }

    pub async fn load_registration_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<coven_protocol::store_commit::StoreDeviceRegistration, TestError> {
        self.founder
            .load_registration_for_test(reference)
            .await
            .map_err(TestError::from)
    }

    pub async fn load_store_package_for_test(
        &self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<Option<coven_protocol::objects::VerifiedObject<Vec<u8>>>, TestError> {
        self.founder
            .load_store_package_for_test(reference)
            .await
            .map_err(TestError::from)
    }

    pub async fn prepare_founder_store_partition_blob_for_test(
        &self,
        fact: &coven_database::StoreWriteBlobFact,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<(), crate::sync::store::StoreError> {
        self.founder
            .authorize_writer()
            .await?
            .prepare_store_partition_blob(fact, authority)
            .await
            .map(|_| ())
    }

    pub async fn next_commit_sequence(&self, name: &str) -> Result<u64, TestError> {
        self.ensure_producer_registered(name).await?;
        let producer = {
            let producers = self.producers.lock().await;
            producers
                .by_name
                .get(name)
                .expect("registered test producer exists")
                .clone()
        };
        producer
            .latest_local_store_position()
            .await?
            .map_or(Ok(1), |reference| {
                reference.coord.sequence().checked_add(1).ok_or_else(|| {
                    TestError::invariant("test producer sequence exhausted u64".to_string())
                })
            })
    }

    pub async fn founder_device_authority(&self) -> Result<TestDeviceSigningAuthority, TestError> {
        self.founder.device_authority_for_test().await
    }

    async fn ensure_producer_registered(&self, name: &str) -> Result<(), TestError> {
        {
            let producers = self.producers.lock().await;
            if producers.by_name.contains_key(name) {
                return Ok(());
            }
        }

        let unassigned = {
            let mut producers = self.producers.lock().await;
            producers.unassigned.take()
        };
        let producer = match unassigned {
            Some(producer) => producer,
            None => {
                let db_store_dir = crate::sync::test_helpers::test_store_dir();
                let db = crate::sync::test_helpers::open_test_db(db_store_dir.clone());
                let observer = {
                    let producers = self.producers.lock().await;
                    producers
                        .by_name
                        .values()
                        .next()
                        .ok_or_else(|| {
                            TestError::invariant(
                                "test Store has no active device observer".to_string(),
                            )
                        })?
                        .clone()
                };
                TestDevice::activate_joined(
                    observer,
                    coven_database::StoreDatabase::new(&db),
                    db_store_dir,
                    &self.signer,
                    "2026-07-16T00:00:00Z",
                    std::sync::Arc::new(
                        self.storage
                            .connection_for_test_identity(self.signer.clone()),
                    ),
                )
                .await?
            }
        };
        let mut producers = self.producers.lock().await;
        if producers
            .by_name
            .insert(name.to_string(), producer)
            .is_some()
        {
            return Err(TestError::invariant(format!(
                "test producer {name:?} was registered twice"
            )));
        }
        Ok(())
    }

    pub async fn open_into(
        &self,
        db: &Database,
        store_dir: StoreDir,
    ) -> Result<TestDevice, crate::sync::store::StoreInitializationError> {
        TestDevice::open_with_database(
            coven_database::StoreDatabase::new(db),
            store_dir,
            std::sync::Arc::new(
                self.storage
                    .connection_for_test_identity(self.signer.clone()),
            ),
            &self.root,
            &self.signer,
        )
        .await
    }

    pub async fn open_into_store_database(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: StoreDir,
    ) -> Result<TestDevice, crate::sync::store::StoreInitializationError> {
        TestDevice::open_with_database(
            database.clone(),
            store_dir,
            std::sync::Arc::new(
                self.storage
                    .connection_for_test_identity(self.signer.clone()),
            ),
            &self.root,
            &self.signer,
        )
        .await
    }

    pub async fn publish_pending(
        &self,
        db: &Database,
        store_dir: &StoreDir,
    ) -> Result<bool, TestError> {
        self.publish_pending_store_database(&coven_database::StoreDatabase::new(db), store_dir)
            .await
    }

    pub async fn publish_pending_store_database(
        &self,
        database: &coven_database::StoreDatabase,
        store_dir: &StoreDir,
    ) -> Result<bool, TestError> {
        let device = self
            .bind_store_device(database, store_dir.clone(), &self.signer)
            .await?;
        device.publish_pending_store_database().await
    }

    #[cfg(test)]
    pub async fn cross_principal_device_for_test(
        &self,
        identity: &UserKeypair,
        peer_account_id: &str,
    ) -> Result<CrossPrincipalTestDevice, TestError> {
        let provider_binding =
            coven_storage::CloudSyncObjectStorage::provider_binding(&*self.storage).await?;
        let coven_protocol::objects::StoreProviderBinding::Dropbox { namespace_id } =
            &provider_binding.store
        else {
            return Err(TestError::invariant(
                "cross-principal test Store is not Dropbox".to_string(),
            ));
        };
        let peer_binding = coven_protocol::objects::ResolvedProviderBinding {
            store: provider_binding.store.clone(),
            device: coven_protocol::objects::ProviderDeviceBinding {
                principal: coven_protocol::objects::ProviderPrincipalId::Dropbox {
                    account_id: peer_account_id.to_string(),
                },
            },
        };
        let peer_home: std::sync::Arc<dyn coven_storage::ExactCloudHome> = std::sync::Arc::new(
            self.home
                .as_ref()
                .clone()
                .with_provider_binding(peer_binding),
        );
        Ok(CrossPrincipalTestDevice {
            storage: std::sync::Arc::new(
                self.storage
                    .connection_for_test_identity_and_home(identity.clone(), peer_home),
            ),
            access_administrator: TestDropboxAccessAdministrator {
                namespace_id: namespace_id.clone(),
            },
        })
    }

    /// Run a cross-principal device join end to end and hand back the database
    /// the joining device ends up with.
    ///
    /// The joining device installs the owner's newest snapshot first and then
    /// carries only the history published after it, which is the shape
    /// production has: a joiner that carried the closure back to genesis would
    /// need every package ever written, including the ones reclamation deletes
    /// once every device has acknowledged the snapshot restating them.
    #[cfg(test)]
    pub fn install_cross_principal_device<'a>(
        &'a self,
        joining_store_dir: StoreDir,
        synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        migrations: Vec<coven_database::Migration>,
        binary_schema_version: u32,
        identity: &'a UserKeypair,
        peer_account_id: &'a str,
        published_at: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<JoinedTestStore, TestError>> + 'a>>
    {
        Box::pin(async move {
            let open_synced_tables = synced_tables.clone();
            let observer = self.founder.clone();
            // The joining device installs a snapshot, so the Store has to have
            // published one — the same precondition production has.
            observer.ensure_device_join_snapshot_for_test().await?;
            let peer = self
                .cross_principal_device_for_test(identity, peer_account_id)
                .await?;
            let pending_dir = tempfile::tempdir()?;
            let pending = crate::sync::store::DeviceJoinJournalDatabase::open_for_test(
                pending_dir.path().join("pending-device-join.sqlite"),
            )?;
            let offer = observer.begin_device_join(&pubkey_hex(identity)).await?;
            let mut pending_join = peer
                .open_pending_device_join(&pending, identity, offer.clone())
                .await?;
            let access_request = pending_join.prepare_provider_access_request().await?;
            let approval = peer
                .authorize_device_provider_access(&observer, access_request)
                .await?;
            if !matches!(
                approval.admission,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::CrossPrincipal { .. }
            ) {
                return Err(
                    TestError::invariant(
                        "distinct provider principals produced same-principal admission",
                    ),
                );
            }
            let registration_request = pending_join.prepare_registration_request(approval).await?;
            let provisional = observer
                .accept_device_registration_request(registration_request)
                .await?;
            let provider_ready = observer
                .publish_device_provider_challenge(provisional)
                .await?;
            drop(pending_join);
            let joined_device_id = offer.attempt_id.to_string();
            let restoring = peer
                .install_store_snapshot(
                    &joining_store_dir,
                    &offer.store_root,
                    &observer.membership().await?,
                    identity,
                    offer.attempt_id.to_string(),
                    binary_schema_version,
                    synced_tables,
                    &migrations,
                )
                .await?;
            let mut joining = restoring.begin_device_join(&pending, offer).await?;
            let readiness = joining
                .bootstrap(provider_ready, published_at, None)
                .await?;
            if !matches!(
                readiness.provider,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderReadiness::CrossPrincipal(_)
            ) {
                return Err(
                    TestError::invariant(
                        "distinct provider principals produced same-principal readiness",
                    ),
                );
            }
            let completion = observer
                .complete_device_provider_admission(readiness)
                .await?;
            if !matches!(
                completion,
                coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmissionCompletion::CrossPrincipal { .. }
            ) {
                return Err(
                    TestError::invariant(
                        "distinct provider principals produced same-principal completion",
                    ),
                );
            }
            let activation = observer.finalize_device_join(completion).await?;
            joining.complete(activation).await?;
            drop(joining);
            let database = open_joined_test_store(
                &joining_store_dir,
                joined_device_id,
                open_synced_tables,
                &migrations,
            )?;
            Ok(JoinedTestStore {
                database: coven_database::StoreDatabase::new(&database),
            })
        })
    }

    #[cfg(test)]
    pub async fn push_circle_snapshots(
        &self,
        db: &Database,
        store_dir: StoreDir,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: &coven_keys::encryption::EncryptionService,
    ) -> Result<coven_protocol::store_commit::CircleSnapshotMeta, crate::sync::store::SnapshotError>
    {
        self.bind_device(db, store_dir, &self.signer)
            .await
            .map_err(|error| crate::sync::store::SnapshotError::PublicationStore(Box::new(error)))?
            .authorize_writer()
            .await
            .map_err(crate::sync::store::SnapshotError::from)?
            .circles()
            .snapshots()
            .author_one_circle_snapshot_for_test(
                temp_dir,
                schema_version,
                created_at,
                store_routing,
            )
            .await
    }

    #[cfg(test)]
    pub async fn load_circle_snapshot_metas(
        &self,
        db: &Database,
        store_dir: StoreDir,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<
        Vec<coven_protocol::store_commit::CircleSnapshotMeta>,
        crate::sync::store::SnapshotError,
    > {
        self.bind_device(db, store_dir, &self.signer)
            .await
            .map_err(|error| crate::sync::store::SnapshotError::PublicationStore(Box::new(error)))?
            .authorize_writer()
            .await
            .map_err(crate::sync::store::SnapshotError::from)?
            .circles()
            .snapshots()
            .load_circle_snapshot_metas_for_test(circle_id, access)
            .await
    }

    #[cfg(test)]
    pub async fn verify_standalone_circle_snapshot_image(
        &self,
        db: &Database,
        store_dir: StoreDir,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        store_routing: &coven_keys::encryption::EncryptionService,
    ) -> Result<(), crate::sync::store::SnapshotError> {
        self.bind_device(db, store_dir, &self.signer)
            .await
            .map_err(|error| crate::sync::store::SnapshotError::PublicationStore(Box::new(error)))?
            .authorize_writer()
            .await
            .map_err(crate::sync::store::SnapshotError::from)?
            .circles()
            .snapshots()
            .verify_standalone_circle_snapshot_image_for_test(circle_id, access, store_routing)
            .await
    }

    #[cfg(test)]
    pub async fn circle_snapshot_is_stable(
        &self,
        db: &Database,
        store_dir: StoreDir,
        circle_id: coven_protocol::circle::CircleId,
        snapshot_cut: &coven_protocol::store_commit::CommitFrontier,
    ) -> Result<bool, crate::sync::store::SnapshotError> {
        self.bind_device(db, store_dir, &self.signer)
            .await
            .map_err(|error| crate::sync::store::SnapshotError::PublicationStore(Box::new(error)))?
            .authorize_writer()
            .await
            .map_err(crate::sync::store::SnapshotError::from)?
            .circles()
            .snapshots()
            .circle_snapshot_is_stable(circle_id, snapshot_cut)
            .await
    }

    #[cfg(test)]
    pub async fn load_circle_acknowledgement(
        &self,
        db: &Database,
        store_dir: StoreDir,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<coven_protocol::store_commit::CircleAck, crate::sync::store::StoreAckError> {
        self.bind_device(db, store_dir, &self.signer)
            .await
            .map_err(crate::sync::store::StoreAckError::Outbound)?
            .load_circle_acknowledgement_for_test(reference)
            .await
    }

    #[cfg(test)]
    pub async fn read_circle_snapshot_image(
        &self,
        selected: &coven_protocol::store_commit::CircleSnapshotMeta,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        let context = access.protocol_context(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotImage,
        );
        self.storage
            .read_protocol_object(
                &context,
                &selected.bootstrap.image.object,
                &coven_protocol::store_commit::circle_snapshot_image_semantic_prefix(
                    selected.circle_id,
                    &selected.author_registration.device_id.to_string(),
                    selected.bootstrap.image.image_hash,
                ),
            )
            .await
    }

    #[cfg(test)]
    pub async fn circle_snapshot_meta_is_unreadable(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        encryption: coven_keys::encryption::EncryptionService,
    ) -> bool {
        let context = coven_protocol::objects::ProtocolObjectContext::circle(
            self.root.store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::CircleSnapshotMeta,
            encryption,
        );
        let prefix = coven_protocol::store_commit::circle_snapshot_slot_prefix(
            circle_id,
            &self.founder.device_id(),
            0,
        );
        let slot = coven_protocol::objects::ObjectSlot::logical(format!("{prefix}.json"))
            .expect("valid generation-zero Circle snapshot slot");
        self.storage
            .read_protocol_slot(&context, &slot, &prefix)
            .await
            .is_err()
    }

    #[cfg(test)]
    pub fn store_root_hash(&self) -> coven_protocol::store_commit::ObjectHash {
        self.root.store_root_hash
    }

    #[cfg(test)]
    pub async fn publish_changeset(
        &self,
        name: &str,
        sequence: u64,
        changeset: &[u8],
        schema_version: u32,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, TestError> {
        self.ensure_producer_registered(name).await?;
        let device = {
            let producers = self.producers.lock().await;
            producers
                .by_name
                .get(name)
                .expect("registered test producer exists")
                .clone()
        };
        device
            .publish_changeset_for_test(sequence, changeset.to_vec(), schema_version)
            .await
    }

    #[cfg(test)]
    pub async fn publish_founder_changeset(
        &self,
        changeset: Vec<u8>,
        previous_sequence: u64,
    ) -> Result<coven_protocol::store_commit::StoreBatchCommitRef, TestError> {
        self.founder
            .publish_changeset_after_for_test(changeset, previous_sequence)
            .await
    }
}

/// A plaintext cloud cipher — the default for tests that are not exercising
/// sealing.
#[cfg(test)]
pub fn plaintext_cipher() -> std::sync::RwLock<coven_storage::CloudCipher> {
    std::sync::RwLock::new(coven_storage::CloudCipher::Plaintext)
}

/// Which protocol read an interceptor hook is running ahead of.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolRead {
    Object,
    Slot,
    PreparedSlot,
    /// Naming the slots under a prefix, which fetches no object's bytes. Apart
    /// from `Slot` because a reader that lists a prefix and then reads what it
    /// found makes one of these and N of those, and a test counting reads or
    /// failing the Nth one means the reads.
    Listing,
}

#[cfg(any(test, feature = "test-utils"))]
pub enum ProviderObjectExistsInterception {
    Proceed,
    DeleteAndReportAbsent,
}

/// Test-side observation of a [`CloudSyncObjectStorage`] call.
///
/// Every hook runs before the wrapped storage does the work, and returning `Err`
/// fails the call without reaching it. All hooks default to doing nothing, so an
/// interceptor states only the operations its test is about — which is the point:
/// a test that intercepts two reads should not also have to restate the sixteen
/// operations it does not care about.
#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
pub trait StorageInterceptor: Send + Sync {
    async fn before_protocol_create(
        &self,
        _prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_protocol_read(
        &self,
        _read: ProtocolRead,
        _semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_allocate(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_prepare(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_create(
        &self,
        _blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_blob_stage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_read(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_write(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }

    async fn before_provider_object_exists(
        &self,
        _key: &str,
    ) -> Result<ProviderObjectExistsInterception, coven_protocol::objects::StorageError> {
        Ok(ProviderObjectExistsInterception::Proceed)
    }

    async fn before_provider_object_delete(
        &self,
        _key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        Ok(())
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T> StorageInterceptor for std::sync::Arc<T>
where
    T: StorageInterceptor + ?Sized,
{
    async fn before_protocol_create(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_protocol_create(prepared).await
    }

    async fn before_protocol_read(
        &self,
        read: ProtocolRead,
        semantic_prefix: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_protocol_read(read, semantic_prefix).await
    }

    async fn before_blob_allocate(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_allocate().await
    }

    async fn before_blob_prepare(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_prepare().await
    }

    async fn before_blob_create(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_create(blob).await
    }

    async fn before_blob_stage(&self) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_blob_stage().await
    }

    async fn before_provider_object_read(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_read(key).await
    }

    async fn before_provider_object_write(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_write(key).await
    }

    async fn before_provider_object_exists(
        &self,
        key: &str,
    ) -> Result<ProviderObjectExistsInterception, coven_protocol::objects::StorageError> {
        (**self).before_provider_object_exists(key).await
    }

    async fn before_provider_object_delete(
        &self,
        key: &str,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        (**self).before_provider_object_delete(key).await
    }
}

/// A [`CloudSyncObjectStorage`] that forwards every call to `inner`, giving `interceptor`
/// its chance first.
#[cfg(any(test, feature = "test-utils"))]
pub struct InterceptedStorage<S, I: StorageInterceptor>
where
    S: std::ops::Deref,
{
    inner: S,
    interceptor: I,
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> coven_storage::CloudSyncCipherStateAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncCipherStateAccess,
    I: StorageInterceptor,
{
    fn is_plaintext(&self) -> bool {
        self.inner.is_plaintext()
    }

    fn suffix(&self) -> &'static str {
        self.inner.suffix()
    }

    fn current_generation(&self) -> Option<u64> {
        self.inner.current_generation()
    }

    fn current_fingerprint(&self) -> Option<String> {
        self.inner.current_fingerprint()
    }

    fn open(
        &self,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, coven_keys::encryption::EncryptionError> {
        self.inner.open(stored, aad_context)
    }

    fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        self.inner.seal(plaintext, aad_context)
    }

    fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<
        (coven_keys::encryption::KeyFingerprint, Vec<u8>),
        coven_keys::encryption::EncryptionError,
    > {
        self.inner.open_sealed_blob_for_test(stored, aad_context)
    }

    fn merged_keyring(
        &self,
        new_encryption: &coven_keys::encryption::EncryptionService,
    ) -> Result<coven_storage::CloudKeyringMerge, coven_keys::encryption::EncryptionError> {
        self.inner.merged_keyring(new_encryption)
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &coven_keys::encryption::EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError> {
        self.inner.merge_key_rotation(new_encryption, custody)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> coven_storage::CloudSyncRotationStateAccess for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncRotationStateAccess,
    I: StorageInterceptor,
{
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), coven_storage::RotationStateError> {
        self.inner.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), coven_storage::RotationStateError> {
        self.inner.mark_committed_mutation(generation, mutation)
    }

    fn remove_candidate(
        &self,
        generation: u64,
        mutation: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), coven_storage::RotationStateError> {
        self.inner.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: coven_protocol::store_commit::ObjectHash,
        replacement: coven_protocol::store_commit::ObjectHash,
    ) -> Result<(), coven_storage::RotationStateError> {
        self.inner
            .replace_candidate_mutation(generation, previous, replacement)
    }

    fn gate(&self) -> Option<coven_protocol::objects::RotationGate> {
        self.inner.gate()
    }

    fn install_durable_gate(&self, gate: Option<coven_protocol::objects::RotationGate>) {
        self.inner.install_durable_gate(gate);
    }

    fn check(
        &self,
        live_generation: Option<u64>,
    ) -> Result<(), coven_protocol::objects::RotationPending> {
        self.inner.check(live_generation)
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I> crate::sync::cycle::CloudSyncCycleConnection for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: crate::sync::cycle::CloudSyncCycleConnection,
    I: StorageInterceptor,
{
}

#[cfg(any(test, feature = "test-utils"))]
impl<S, I: StorageInterceptor> InterceptedStorage<S, I>
where
    S: std::ops::Deref,
{
    pub fn new(inner: S, interceptor: I) -> Self {
        Self { inner, interceptor }
    }

    pub fn interceptor(&self) -> &I {
        &self.interceptor
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl<S, I> coven_storage::CloudSyncObjectStorage for InterceptedStorage<S, I>
where
    S: std::ops::Deref + Send + Sync,
    S::Target: coven_storage::CloudSyncObjectStorage + coven_storage::CloudSyncCipherStateAccess,
    I: StorageInterceptor,
{
    fn blob_path_scheme(&self) -> coven_storage::BlobPathScheme {
        self.inner.blob_path_scheme()
    }

    async fn probe_provider(&self) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.probe_provider().await
    }

    fn provider_requests(
        &self,
    ) -> Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>> {
        self.inner.provider_requests()
    }

    async fn set_member_access(
        &self,
        state: coven_storage::cloud::CloudAccessState,
    ) -> Result<coven_storage::cloud::CloudAccessOutcome, coven_protocol::objects::StorageError>
    {
        self.inner.set_member_access(state).await
    }

    async fn read_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<Option<Vec<u8>>, coven_protocol::objects::StorageError> {
        let key = coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(&*self.inner),
        );
        self.interceptor.before_provider_object_read(&key).await?;
        self.inner.read_blob_tombstone(stored).await
    }

    async fn write_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        plaintext: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let key = coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(&*self.inner),
        );
        self.interceptor.before_provider_object_write(&key).await?;
        self.inner.write_blob_tombstone(stored, plaintext).await
    }

    async fn list_blob_tombstones(
        &self,
    ) -> Result<Vec<coven_storage::ListedBlobTombstone>, coven_protocol::objects::StorageError>
    {
        self.inner.list_blob_tombstones().await
    }

    async fn blob_tombstone_exists(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, coven_protocol::objects::StorageError> {
        let key = coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(&*self.inner),
        );
        match self.interceptor.before_provider_object_exists(&key).await? {
            ProviderObjectExistsInterception::Proceed => {
                self.inner.blob_tombstone_exists(stored).await
            }
            ProviderObjectExistsInterception::DeleteAndReportAbsent => {
                self.inner.delete_blob_tombstone(stored).await?;
                Ok(false)
            }
        }
    }

    async fn delete_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        let key = coven_storage::blob_tombstone_key(
            stored,
            coven_storage::CloudSyncCipherStateAccess::suffix(&*self.inner),
        );
        self.interceptor.before_provider_object_delete(&key).await?;
        self.inner.delete_blob_tombstone(stored).await
    }

    async fn read_provider_bytes_for_test(
        &self,
        key: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.interceptor.before_provider_object_read(key).await?;
        self.inner.read_provider_bytes_for_test(key).await
    }

    async fn write_provider_bytes_for_test(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_provider_object_write(key).await?;
        self.inner.write_provider_bytes_for_test(key, bytes).await
    }

    async fn list_provider_keys_for_test(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, coven_protocol::objects::StorageError> {
        self.inner.list_provider_keys_for_test(prefix).await
    }

    async fn provider_key_exists_for_test(
        &self,
        key: &str,
    ) -> Result<bool, coven_protocol::objects::StorageError> {
        match self.interceptor.before_provider_object_exists(key).await? {
            ProviderObjectExistsInterception::Proceed => {
                self.inner.provider_key_exists_for_test(key).await
            }
            ProviderObjectExistsInterception::DeleteAndReportAbsent => {
                Err(coven_protocol::objects::StorageError::InvalidContent(
                    "raw test-key interception cannot delete through the production API"
                        .to_string(),
                ))
            }
        }
    }

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: coven_protocol::provider::ProviderProbeId,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::provider::ProviderProbeError>
    {
        self.inner
            .reserve_cross_principal_response_slot(probe_id)
            .await
    }

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        store: &coven_protocol::StoreProviderBinding,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .prepare_cross_principal_challenge(
                publication_journal,
                probe_id,
                store,
                context,
                administrator_signer,
            )
            .await
    }

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        store: &coven_protocol::StoreProviderBinding,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .settle_cross_principal_challenge(
                publication_journal,
                authorization,
                challenge,
                context,
                store,
            )
            .await
    }

    async fn create_cross_principal_response(
        &self,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &coven_keys::keys::UserKeypair,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeResponse,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .create_cross_principal_response(
                challenge,
                context,
                store,
                administrator_signing_pubkey,
                peer_signer,
            )
            .await
    }

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        response: &coven_protocol::provider::CrossPrincipalProbeResponse,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .complete_cross_principal_probe(
                journal,
                challenge,
                response,
                context,
                store,
                administrator_signer,
                peer_signing_pubkey,
            )
            .await
    }

    async fn probe_exact_slots(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        binding: &coven_protocol::objects::ResolvedProviderBinding,
    ) -> Result<
        coven_protocol::provider::ExactSlotProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    > {
        self.inner
            .probe_exact_slots(journal, probe_id, binding)
            .await
    }

    async fn observe_exact_slot(
        &self,
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<
        Option<coven_protocol::objects::ExactObjectRef>,
        coven_protocol::objects::StorageError,
    > {
        self.inner.observe_exact_slot(slot).await
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_exact_slot_and_verify_absent(slot).await
    }

    fn store_blob_key_fingerprint(
        &self,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, coven_protocol::objects::StorageError>
    {
        self.inner.store_blob_key_fingerprint()
    }

    fn create_store_key_confirmation(
        &self,
        creation_id: coven_protocol::store_commit::StoreCreationId,
    ) -> Result<
        coven_protocol::store_commit::StoreKeyConfirmation,
        coven_protocol::objects::StorageError,
    > {
        self.inner.create_store_key_confirmation(creation_id)
    }

    fn verify_store_key_confirmation(
        &self,
        creation_id: coven_protocol::store_commit::StoreCreationId,
        confirmation: &coven_protocol::store_commit::StoreKeyConfirmation,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner
            .verify_store_key_confirmation(creation_id, confirmation)
    }

    async fn seal_store_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        plaintext_file: &std::path::Path,
        spool: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::PreparationProgress,
    ) -> Result<coven_protocol::objects::BlobSpoolWrite, coven_protocol::objects::StorageError>
    {
        self.inner
            .seal_store_blob_to_spool(locator, authority, plaintext_file, spool, progress)
            .await
    }

    async fn stage_verified_store_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, coven_protocol::objects::StorageError>
    {
        self.interceptor.before_blob_stage().await?;
        self.inner
            .stage_verified_store_blob_plaintext(blob, stage, progress)
            .await
    }

    async fn open_store_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<coven_storage::BlobRangeReader, coven_protocol::objects::StorageError> {
        self.inner.open_store_blob_range_reader(blob).await
    }

    async fn provider_binding(
        &self,
    ) -> Result<
        coven_protocol::objects::ResolvedProviderBinding,
        coven_protocol::objects::StorageError,
    > {
        self.inner.provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::objects::StorageError> {
        self.inner
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, coven_protocol::objects::StorageError>
    {
        self.inner
            .prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn open_prepared_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        prepared: &coven_protocol::objects::PreparedExactObject,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.inner
            .open_prepared_protocol_object(context, prepared, semantic_prefix)
            .await
    }

    async fn create_protocol_object(
        &self,
        prepared: &coven_protocol::objects::PreparedExactObject,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_protocol_create(prepared).await?;
        self.inner.create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        object: &coven_protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Object, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_object_with_progress(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        object: &coven_protocol::objects::ExactObjectRef,
        semantic_prefix: &str,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<Vec<u8>, coven_protocol::objects::StorageError> {
        self.interceptor
            .before_protocol_read(ProtocolRead::Object, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_object_with_progress(context, object, semantic_prefix, progress)
            .await
    }

    async fn list_protocol_slots(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        listing_prefix: &str,
    ) -> Result<Vec<coven_protocol::objects::ObjectSlot>, coven_protocol::objects::StorageError>
    {
        self.interceptor
            .before_protocol_read(ProtocolRead::Listing, listing_prefix)
            .await?;
        self.inner
            .list_protocol_slots(context, listing_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, coven_protocol::objects::ExactObjectRef),
        coven_protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::Slot, semantic_prefix)
            .await?;
        self.inner
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: &coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        (Vec<u8>, coven_protocol::objects::PreparedExactObject),
        coven_protocol::objects::StorageError,
    > {
        self.interceptor
            .before_protocol_read(ProtocolRead::PreparedSlot, semantic_prefix)
            .await?;
        self.inner
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<coven_protocol::objects::ObjectSlot, coven_protocol::objects::StorageError> {
        self.interceptor.before_blob_allocate().await?;
        self.inner.allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        protection: coven_protocol::objects::BlobSpoolProtection,
        plaintext_file: &std::path::Path,
        spool: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::PreparationProgress,
    ) -> Result<coven_protocol::objects::BlobSpoolWrite, coven_protocol::objects::StorageError>
    {
        self.inner
            .seal_blob_to_spool(
                locator,
                authority,
                protection,
                plaintext_file,
                spool,
                progress,
            )
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        slot: coven_protocol::objects::ObjectSlot,
        stored_file: &std::path::Path,
    ) -> Result<coven_protocol::blob::locator::StoredBlobRef, coven_protocol::objects::StorageError>
    {
        self.interceptor.before_blob_prepare().await?;
        self.inner
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
        stored_file: &std::path::Path,
        progress: &coven_storage::cloud::UploadProgress,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.interceptor.before_blob_create(blob).await?;
        self.inner
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.verify_blob_object(blob).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, coven_protocol::objects::StorageError>
    {
        self.interceptor.before_blob_stage().await?;
        self.inner
            .stage_verified_blob_plaintext(blob, protection, stage, progress)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: coven_protocol::objects::BlobSpoolProtection,
    ) -> Result<coven_storage::BlobRangeReader, coven_protocol::objects::StorageError> {
        self.inner.open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), coven_protocol::objects::StorageError> {
        self.inner.delete_blob_object(blob).await
    }
}
