//! High-level sync manager: lifecycle, membership, status.
//!
//! Owns the sync lifecycle — cloud home + sync loop — and starts/stops it when
//! a provider is connected/disconnected, no app restart required. The host
//! supplies the config snapshot, keys, encryption, database, clock, and blob
//! handling; coven drives the rest.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{error, info};

use crate::blob::transition::{self, MakeLocalError, MakeRemoteError};
use crate::blob::BlobTransitionObserver;
use crate::clock::ClockRef;
use crate::config::Config;
use crate::coven::StoreOpenGuard;
use crate::database::{Database, DbError};
use crate::encryption::EncryptionService;
use crate::keys::{DeviceKeys, KeyError, StoreKeys};
use crate::storage::cloud::setup::{SetupError, StorageSetupError};
use crate::storage::cloud::{CloudHome, CloudHomeError};
use crate::store_dir::StoreDir;
#[cfg(any(test, feature = "test-utils"))]
use crate::sync::cloud_storage::CloudSyncStorage;
use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher};
use crate::sync::cycle::{InitSyncError, SyncComponents};
use crate::sync::hlc::Hlc;
/// `MemberInfo` lives next to `MemberRole` in the membership module; coven's
/// public path reaches it through here (re-exported from `lib.rs`).
pub use crate::sync::membership::MemberInfo;
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::sync_loop::{SyncLoopError, SyncLoopHandle, SyncLoopStatus};

/// Supplies the host's current config on demand. coven reads it fresh each call
/// — never snapshotting or writing it — so a host with reactive config sees
/// changes without rebuilding the manager.
pub type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("sync is not configured")]
    NotConfigured,
    #[error("sync loop is not running")]
    LoopNotRunning,
    #[error("sharing requires an encrypted cloud home")]
    NotEncryptedHome,
    #[error(
        "opaque cloud home configured without an encryption service (a locked store, or a browsable/opaque storage mismatch)"
    )]
    OpaqueHomeWithoutEncryption,
    #[error("failed to build cloud home: {0}")]
    CloudHome(#[from] CloudHomeError),
    #[error("failed to create sync storage: {0}")]
    StorageSetup(StorageSetupError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("sync initialization error: {0}")]
    Init(#[from] InitSyncError),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("membership error: {0}")]
    Membership(crate::sync::membership_ops::MembershipOpsError),
    #[error("blob upload drain failed: {0}")]
    BlobUpload(DbError),
    #[error("sync loop error: {0}")]
    Loop(SyncLoopError),
}

/// Refuse a membership operation on a plaintext home. Inviting wraps the store
/// key to a member and removing rotates it — both meaningless without a key — so
/// the caller must bail before mutating the membership chain or re-wrapping keys.
fn require_encrypted_home(cipher: &RwLock<CloudCipher>) -> Result<EncryptionService, SyncError> {
    match &*cipher.read().unwrap() {
        CloudCipher::Encrypted(encryption) => Ok(encryption.clone()),
        CloudCipher::Plaintext => Err(SyncError::NotEncryptedHome),
    }
}

/// High-level sync manager.
///
/// Holds an `EncryptionService` for an opaque home and `None` for a browsable
/// (plaintext) one — a browsable home has no store key. The at-rest cipher is
/// chosen per cycle from the home's [`HomeStorage`](crate::config::HomeStorage),
/// so the service is consulted only on an opaque home.
pub struct SyncManager {
    config_provider: ConfigProvider,
    key_service: StoreKeys,
    encryption_service: Option<EncryptionService>,
    db: Database,
    clock: ClockRef,
    cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
    observer: Option<Arc<dyn BlobTransitionObserver>>,

    /// The store-directory lock, cloned into every sync loop this manager
    /// starts so the loop's thread keeps it alive for the whole of its final
    /// cycle. The lock releases when the last writer — a running loop, else the
    /// handle — is gone, never while a detached loop thread is still writing.
    open_guard: Arc<StoreOpenGuard>,

    /// coven's `_updated_at` register, the same `Arc<Hlc>` the owned [`Database`]
    /// holds. The sync loop advances it past pulled rows and stamps envelopes off
    /// it, so it shares the clock the host stamps rows from.
    hlc: Arc<Hlc>,

    /// The status broadcast the [`CovenHandle`](crate::CovenHandle) owns, cloned
    /// into every sync loop this manager starts so a subscription outlives the
    /// loop restarts a reconnect performs.
    status_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,

    // Mutable sync state — updated when providers are connected/disconnected
    sync_loop_handle: RwLock<Option<Arc<SyncLoopHandle>>>,
    cloud_home: RwLock<Option<Arc<dyn CloudHome>>>,
}

impl SyncManager {
    /// Build the manager off the owned [`Database`]. The database already seeded
    /// its register clock past every value on disk at `open`; the manager shares
    /// that `Arc<Hlc>` so the sync loop's advance-on-pull and envelope stamps use
    /// the same instance the host stamps rows from.
    ///
    /// Construction is infallible and synchronous: seeding already happened in
    /// the open path. The manager is built lazily, only once a provider is
    /// connected.
    pub(crate) fn new(
        config_provider: ConfigProvider,
        key_service: StoreKeys,
        encryption_service: Option<EncryptionService>,
        db: Database,
        clock: ClockRef,
        cloudkit_ops: Option<Arc<dyn crate::storage::cloud::cloudkit::CloudKitOps>>,
        observer: Option<Arc<dyn BlobTransitionObserver>>,
        open_guard: Arc<StoreOpenGuard>,
        status_tx: tokio::sync::broadcast::Sender<SyncLoopStatus>,
    ) -> Self {
        let hlc = db.hlc();
        Self {
            config_provider,
            key_service,
            encryption_service,
            db,
            clock,
            cloudkit_ops,
            observer,
            open_guard,
            hlc,
            status_tx,
            sync_loop_handle: RwLock::new(None),
            cloud_home: RwLock::new(None),
        }
    }

    pub fn cloud_home(&self) -> Option<Arc<dyn CloudHome>> {
        self.cloud_home.read().unwrap().clone()
    }

    /// The encryption service this manager holds: `Some` for an opaque home
    /// (sealed under the store key), `None` for a browsable one (no store
    /// key, stored in the clear).
    pub fn encryption_service(&self) -> Option<EncryptionService> {
        self.encryption_service.clone()
    }

    pub fn sync_loop_handle(&self) -> Option<Arc<SyncLoopHandle>> {
        self.sync_loop_handle.read().unwrap().clone()
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// Initialize cloud home and sync loop from current config.
    /// Called at startup (if already configured) and after connecting a provider.
    ///
    /// Two outcomes are success: a configured provider whose home builds and whose
    /// loop starts, and a store with no configured provider that legitimately
    /// starts no loop — the latter is a logged `Ok(())` no-op. A cloud-home build
    /// that *fails* (missing credentials, a bad provider config) is an `Err`, not
    /// "no provider connected": the caller must not install a manager that reports
    /// success with nothing started.
    pub async fn start_sync(&self) -> Result<(), SyncError> {
        let config = (self.config_provider)();

        if config.cloud_home.provider.is_none() {
            self.stop_sync()?;
            // Not a failure: a store with no configured provider starts no
            // cloud home or sync loop.
            info!("start_sync: sync not configured; no loop started");
            return Ok(());
        }

        self.stop_current_connection()?;

        // The home's at-rest cipher: an opaque home seals under the manager's
        // store key, a browsable home stores in the clear. An opaque home with
        // no store key is a config contradiction (a locked store, or a
        // browsable/opaque storage mismatch) — decided from storage mode and the
        // held encryption service alone, so check it before building the home and
        // fail with a typed error rather than deep inside it. Built once here so
        // the sync loop and storage share one instance — a member removal rotates
        // the key in place through it.
        let cipher =
            CloudCipher::for_storage(config.cloud_home.storage, self.encryption_service.clone())
                .ok_or(SyncError::OpaqueHomeWithoutEncryption)?;

        // Build the cloud home. A failure here is a real fault — surface it so the
        // caller never installs a manager that started nothing.
        let cloud_home = crate::storage::cloud::create_cloud_home_with_cloudkit(
            &config,
            &self.key_service,
            self.clock.clone(),
            self.cloudkit_ops.clone(),
        )
        .await
        .map_err(SyncError::from)?;
        let cloud_home: Arc<dyn CloudHome> = Arc::from(cloud_home);

        // Initialize sync loop. The synced-table set is owned by the Database, so
        // init_sync reads it from there rather than from a separately-held copy.
        // Sync is enabled here, so `None` means a real startup failure (no synced
        // tables, storage/keypair/auth/membership bootstrap) that init_sync already
        // logged — surface it so the caller never installs a manager whose loop
        // never started.
        let user_keypair = DeviceKeys::get_or_create_user_keypair()?;
        let storage = crate::storage::cloud::setup::create_sync_storage_with_home(
            &config,
            &self.key_service,
            cloud_home.clone(),
            Some(cipher.clone()),
        )
        .map_err(SyncError::StorageSetup)?;

        let components = crate::sync::cycle::init_sync_over_storage(
            &config,
            &self.db,
            &cipher,
            self.hlc.clone(),
            user_keypair,
            storage,
        )
        .await
        .map_err(SyncError::from)?;

        let _handle = self.install_sync_loop(components, config.store_dir.clone())?;
        *self.cloud_home.write().unwrap() = Some(cloud_home);

        Ok(())
    }

    /// Build the sync-loop handle off `components`, start it, and install it. The
    /// shared install tail of [`start_sync`](Self::start_sync) and the test-only
    /// [`start_sync_with_home`](Self::start_sync_with_home): both reach it only
    /// after the bootstrap has produced [`SyncComponents`], so the loop handle is
    /// installed whole, never on a half-built bootstrap.
    fn install_sync_loop(
        &self,
        components: SyncComponents,
        store_dir: StoreDir,
    ) -> Result<Arc<SyncLoopHandle>, SyncError> {
        let handle = Arc::new(SyncLoopHandle::new(
            components,
            self.db.clone(),
            self.key_service.clone(),
            self.clock.clone(),
            store_dir,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
        ));
        handle.start().map_err(SyncError::Loop)?;

        info!("Sync loop started");
        *self.sync_loop_handle.write().unwrap() = Some(handle.clone());
        Ok(handle)
    }

    /// Test-only: stand the sync loop over an injected `home`/`cipher` instead of
    /// building the cloud home from config via `create_cloud_home`.
    ///
    /// The counterpart of [`start_sync`](Self::start_sync) for a host's
    /// integration tests, which drive coven over a mock [`CloudHome`] no provider
    /// match would ever produce. It skips the config-provider gate — the injected
    /// home IS the enablement, there are no real credentials to check — installs
    /// the home, builds a [`CloudSyncStorage`] over it under the supplied `cipher`
    /// (and the config's blob-path scheme), runs the same bootstrap
    /// [`init_sync`](crate::sync::cycle::init_sync) does via
    /// [`init_sync_over_storage`](crate::sync::cycle::init_sync_over_storage), and
    /// starts the loop. A bootstrap failure is an `Err`, the same fail-loud
    /// discipline `start_sync` keeps — and commit-whole: the home and loop handle
    /// are installed only after the keypair load and bootstrap both succeed, so a
    /// failure leaves nothing installed.
    ///
    /// After this returns, the connected loop's storage is reachable via
    /// [`sync_loop_handle`](Self::sync_loop_handle)`().storage()`, so the handle's
    /// read path serves blobs over the same injected home with no separate hook.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn start_sync_with_home(
        &self,
        home: std::sync::Arc<dyn CloudHome>,
        cipher: CloudCipher,
    ) -> Result<(), SyncError> {
        let config = (self.config_provider)();
        self.stop_current_connection()?;

        let keypair = DeviceKeys::get_or_create_user_keypair()?;
        let storage = CloudSyncStorage::new(
            home.clone(),
            cipher.clone(),
            BlobPathScheme::for_storage(config.cloud_home.storage),
            config.store_id.clone(),
            keypair.clone(),
        );

        let components = crate::sync::cycle::init_sync_over_storage(
            &config,
            &self.db,
            &cipher,
            self.hlc.clone(),
            keypair,
            storage,
        )
        .await
        .map_err(SyncError::from)?;

        let _handle = self.install_sync_loop(components, config.store_dir.clone())?;
        *self.cloud_home.write().unwrap() = Some(home);

        Ok(())
    }

    /// Tear down the sync loop and cloud home.
    pub fn stop_sync(&self) -> Result<(), SyncError> {
        let stop_result = self.stop_current_loop();
        *self.sync_loop_handle.write().unwrap() = None;
        *self.cloud_home.write().unwrap() = None;

        match stop_result {
            Ok(()) => {
                info!("Sync loop stopped");
                Ok(())
            }
            Err(error) => {
                error!("Sync loop stop failed: {error}");
                Err(error)
            }
        }
    }

    fn stop_current_loop(&self) -> Result<(), SyncError> {
        let handle = self.sync_loop_handle.write().unwrap().take();
        if let Some(handle) = handle {
            handle.stop().map_err(SyncError::Loop)?;
        }
        Ok(())
    }

    fn stop_current_connection(&self) -> Result<(), SyncError> {
        let stop_result = self.stop_current_loop();
        *self.cloud_home.write().unwrap() = None;
        stop_result
    }

    // =========================================================================
    // Status / config queries
    // =========================================================================

    pub fn is_sync_ready(&self) -> bool {
        self.sync_loop_handle
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|h| h.is_running())
    }

    pub fn trigger_sync(&self) {
        if let Some(ref sync_loop) = *self.sync_loop_handle.read().unwrap() {
            sync_loop.trigger();
        }
    }

    // =========================================================================
    // Blob locality transitions (make_remote / make_local / cancel_make_remote)
    // =========================================================================

    /// The blob-path scheme the configured home keys objects under (`Hashed` for an
    /// opaque home, `Plain` for a browsable one) — how coven derives each blob's
    /// cloud object key at transition time.
    fn blob_path_scheme(&self) -> BlobPathScheme {
        BlobPathScheme::for_storage((self.config_provider)().cloud_home.storage)
    }

    /// Make `(root_table, root_id)` Remote (Local → Remote): enqueue an upload per
    /// user-provided blob from its external file and record the make_remote intent,
    /// then return. The drain uploads each and flips the gate true on the last (see
    /// [`crate::blob::transition::make_remote`]); the gate flip re-emits the subtree,
    /// the cycle's inline push uploads the root's host-provided blobs, and
    /// `on_root_made_remote` fires. `pin` keeps the uploaded blobs in coven's cache
    /// as pinned (offline) copies.
    pub async fn make_remote(
        &self,
        root_table: &str,
        root_id: &str,
        pin: bool,
    ) -> Result<(), MakeRemoteError> {
        if !self.is_sync_ready() {
            return Err(MakeRemoteError::SyncNotReady);
        }
        transition::make_remote(
            &self.db,
            self.blob_path_scheme(),
            &self.self_uploader()?,
            &self.hlc,
            root_table,
            root_id,
            pin,
        )
        .await?;
        self.trigger_sync();
        Ok(())
    }

    /// This device's hex public key — the `{uploader}` segment its own blob
    /// uploads key under. Ready only once the keypair is loaded, which
    /// `is_sync_ready` has already established at the call sites below.
    fn self_uploader(&self) -> Result<String, MakeRemoteError> {
        match DeviceKeys::get_user_public_key() {
            Ok(Some(pk)) => Ok(hex::encode(pk)),
            // No keypair loaded yet: sync genuinely isn't ready.
            Ok(None) => Err(MakeRemoteError::SyncNotReady),
            // A key-store failure (e.g. the keychain is unavailable) is not "not
            // configured" — surface the real cause rather than silently reporting
            // the transition as not-ready.
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "reading this device's public key failed; cannot start the transition",
                );
                Err(MakeRemoteError::SyncNotReady)
            }
        }
    }

    /// Cancel an in-flight make_remote of `(root_table, root_id)`: clear its intent
    /// and pending uploads and tombstone any blob that already landed. The gate never
    /// flips, so the root stays Local.
    pub async fn cancel_make_remote(
        &self,
        root_table: &str,
        root_id: &str,
    ) -> Result<(), MakeRemoteError> {
        if !self.is_sync_ready() {
            return Err(MakeRemoteError::SyncNotReady);
        }
        let store_dir = (self.config_provider)().store_dir;
        transition::cancel_make_remote(
            &self.db,
            &store_dir,
            self.blob_path_scheme(),
            &self.self_uploader()?,
            &self.hlc,
            root_table,
            root_id,
        )
        .await?;
        self.trigger_sync();
        Ok(())
    }

    /// Make `(root_table, root_id)` Local (Remote → Local): bring each blob back to a
    /// local file durability-first — a user-provided blob to the path named in `dest`
    /// (blob id → destination path), a host-provided blob to coven's local store (no
    /// dest) — then flip the gate false, register the user-provided external refs,
    /// and enqueue the cloud deletes in one atomic commit. Awaitable; `cancel` aborts
    /// before the commit (the root stays Remote). `dest` carries user-provided ids
    /// only. Per-blob materialize progress and the completion event reach the
    /// observer this manager was built with.
    pub async fn make_local(
        &self,
        root_table: &str,
        root_id: &str,
        dest: &HashMap<String, PathBuf>,
        cancel: &watch::Receiver<bool>,
    ) -> Result<(), MakeLocalError> {
        if !self.is_sync_ready() {
            return Err(MakeLocalError::SyncNotReady);
        }
        let sync_loop = self
            .sync_loop_handle()
            .ok_or(MakeLocalError::SyncNotReady)?;
        let store_dir = (self.config_provider)().store_dir;
        let storage: &dyn SyncStorage = &**sync_loop.storage();
        transition::make_local(
            &self.db,
            storage,
            &store_dir,
            self.blob_path_scheme(),
            &self.hlc,
            self.observer.as_deref(),
            root_table,
            root_id,
            dest,
            cancel,
        )
        .await?;
        self.trigger_sync();
        Ok(())
    }

    // =========================================================================
    // Keys / codes
    // =========================================================================

    // =========================================================================
    // Membership
    // =========================================================================

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError> {
        let config = (self.config_provider)();
        if config.cloud_home.provider.is_none() {
            info!("get_members: sync not configured; returning no members");
            return Ok(Vec::new());
        }

        let loop_storage = self
            .sync_loop_handle()
            .map(|handle| handle.storage().clone());
        let owned_storage;
        let storage: &dyn SyncStorage = if let Some(storage) = loop_storage.as_deref() {
            storage
        } else {
            owned_storage = match self.cloud_home() {
                Some(home) => crate::storage::cloud::setup::create_sync_storage_with_home(
                    &config,
                    &self.key_service,
                    home,
                    None,
                ),
                None => {
                    crate::storage::cloud::setup::create_sync_storage_with_cloudkit(
                        &config,
                        &self.key_service,
                        None,
                        self.clock.clone(),
                        self.cloudkit_ops.clone(),
                    )
                    .await
                }
            }
            .map_err(SyncError::StorageSetup)?;
            &owned_storage
        };

        let user_pubkey = DeviceKeys::get_user_public_key()?;
        crate::sync::membership_ops::get_members(
            storage,
            user_pubkey.as_ref().map(|k| k.as_slice()),
        )
        .await
        .map_err(SyncError::Membership)
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: MemberRole,
    ) -> Result<String, SyncError> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or(SyncError::LoopNotRunning)?;

        // Inviting a member wraps the store key to them, which only an encrypted
        // home has. Refuse before touching the membership chain.
        let encryption = require_encrypted_home(sync_loop.cipher())?;

        let (store_id, store_name) = {
            let config = (self.config_provider)();
            (config.store_id.clone(), config.store_name.clone())
        };

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let invite_code = crate::sync::membership_ops::invite_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
            invitee_email,
            role,
            &encryption,
            &store_id,
            &store_name,
        )
        .await
        .map_err(SyncError::Membership)?;

        Ok(crate::join_code::encode(&invite_code))
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, SyncError> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or(SyncError::LoopNotRunning)?;

        // Removing a member rotates the store key, which only an encrypted home
        // has. Refuse up front so a plaintext home never mutates the membership
        // chain or re-wraps keys before the rotation fails.
        let current_encryption = require_encrypted_home(sync_loop.cipher())?;

        let store_id = (self.config_provider)().store_id.clone();

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        // Removing a member commits the cloud key rotation and then adopts the
        // rotated key into this device's keyring and live cipher. The host records
        // the returned fingerprint and that a key is stored in its own config; an
        // adoption failure surfaces as its own membership variant naming the
        // half-applied state and its remedies.
        let fingerprint = crate::sync::membership_ops::remove_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
            &store_id,
            &current_encryption,
            &self.key_service,
            sync_loop.cipher(),
        )
        .await
        .map_err(SyncError::Membership)?;

        Ok(fingerprint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::clock::SystemClock;
    use crate::config::CloudProvider;
    use crate::coven::StoreOpenGuard;
    use crate::keys::{test_keyring, StoreKeys};
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::CloudHomeJoinInfo;
    use crate::store_dir::StoreDir;
    use std::sync::Arc;

    #[tokio::test]
    async fn get_members_surfaces_malformed_cloud_credentials() {
        test_keyring::install();
        let tmp = tempfile::tempdir().expect("temp dir");
        let store_dir = StoreDir::new(tmp.path());
        let store_id = "sync-enabled-malformed-credentials";
        let key_service = StoreKeys::new(store_id.to_string());
        key_service
            .cloud_home_credentials_entry_for_test()
            .expect("create credentials entry")
            .set_password("{")
            .expect("write malformed credentials");
        let join_info = CloudHomeJoinInfo::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: None,
            access_key: "access".to_string(),
            secret_key: "secret".to_string(),
            key_prefix: None,
        };
        let config = crate::sync::join::build_config(
            store_id,
            "device",
            &store_dir,
            "store",
            &join_info,
            &CloudCipher::Plaintext,
        );
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            key_service,
            None,
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            StoreOpenGuard::acquire_for_test(&store_dir),
            tokio::sync::broadcast::channel(16).0,
        );

        let error = manager
            .get_members()
            .await
            .expect_err("malformed stored credentials must fail");
        // The typed CloudHomeError survives up through StorageSetup to the public
        // SyncError surface — not flattened into a string — so its retryability
        // verdict is still readable: malformed credentials are a configuration
        // fault the user must fix, not a transient retry.
        let SyncError::StorageSetup(StorageSetupError::CloudHome(cloud_home_error)) = &error else {
            panic!("expected StorageSetup(CloudHome(_)), got {error:?}");
        };
        assert!(matches!(cloud_home_error, CloudHomeError::Configuration(_)));
        assert!(!cloud_home_error.is_retryable());
        assert!(error
            .to_string()
            .contains("malformed cloud home credentials JSON"));
    }

    #[tokio::test]
    async fn start_sync_rejects_an_opaque_home_without_an_encryption_service() {
        test_keyring::install();
        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let mut config = Config::with_defaults(
            "lib-opaque-no-encryption".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        );
        // Opaque storage (the default) with a configured provider but no
        // encryption service is a locked-store contradiction — the manager
        // holds `None` for the encryption service.
        config.cloud_home.provider = Some(CloudProvider::S3);
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new("lib-opaque-no-encryption".to_string()),
            None,
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::broadcast::channel(16).0,
        );

        let error = manager
            .start_sync()
            .await
            .expect_err("opaque home without an encryption service must fail");
        assert!(
            matches!(error, SyncError::OpaqueHomeWithoutEncryption),
            "expected OpaqueHomeWithoutEncryption, got {error:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn start_sync_with_home_stops_the_previous_loop_before_replacement() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let config = Config::with_defaults(
            "lib-manager-restart".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        );
        let manager = SyncManager::new(
            Arc::new(move || config.clone()),
            StoreKeys::new("lib-manager-restart".to_string()),
            None,
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::broadcast::channel(16).0,
        );

        manager
            .start_sync_with_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("first test home starts");
        let first_loop = manager
            .sync_loop_handle()
            .expect("first loop handle installed");
        assert!(first_loop.is_running(), "first loop starts running");

        manager
            .start_sync_with_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("replacement test home starts");
        let replacement_loop = manager
            .sync_loop_handle()
            .expect("replacement loop handle installed");

        assert!(
            !first_loop.is_running(),
            "starting sync again stops the previous loop before replacement",
        );
        assert!(
            replacement_loop.is_running(),
            "replacement loop remains running",
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn failed_restart_leaves_no_stale_cloud_home() {
        test_keyring::install();
        let _guard = test_keyring::SIGNING_KEY_GUARD.lock().unwrap();

        let (_tmp, store_dir) = crate::sync::test_helpers::temp_store_dir();
        let open_guard = StoreOpenGuard::acquire_for_test(&store_dir);
        let config = Arc::new(RwLock::new(Config::with_defaults(
            "lib-manager-failed-restart".to_string(),
            "test-device".to_string(),
            store_dir,
            "Test Store".to_string(),
        )));
        let manager = SyncManager::new(
            {
                let config = config.clone();
                Arc::new(move || config.read().unwrap().clone())
            },
            StoreKeys::new("lib-manager-failed-restart".to_string()),
            // An encryption service so the opaque default storage passes the
            // cipher precondition and the restart fails at the home build itself.
            Some(EncryptionService::from_key([7u8; 32])),
            crate::sync::test_helpers::open_test_db(),
            Arc::new(SystemClock),
            None,
            None,
            open_guard,
            tokio::sync::broadcast::channel(16).0,
        );

        manager
            .start_sync_with_home(Arc::new(InMemoryCloudHome::new()), CloudCipher::Plaintext)
            .await
            .expect("injected home starts");
        assert!(manager.cloud_home().is_some(), "injected home is installed");

        config.write().unwrap().cloud_home.provider = Some(CloudProvider::S3);
        let error = manager
            .start_sync()
            .await
            .expect_err("invalid configured provider fails restart");
        assert!(
            error.to_string().contains("failed to build cloud home"),
            "restart failure surfaces the provider setup error: {error}",
        );
        assert!(
            manager.sync_loop_handle().is_none(),
            "failed restart leaves no loop installed",
        );
        assert!(
            manager.cloud_home().is_none(),
            "failed restart must not leave the previous cloud home installed",
        );
    }
}
