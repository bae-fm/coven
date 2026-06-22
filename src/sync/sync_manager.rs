//! High-level sync manager: lifecycle, membership, status.
//!
//! Owns the sync lifecycle — cloud home + sync loop — and starts/stops it when
//! a provider is connected/disconnected, no app restart required. The host
//! supplies the config snapshot, keys, encryption, database, clock, and blob
//! handling; coven drives the rest.

use std::sync::{Arc, RwLock};

use tracing::info;

use crate::blob::{BlobSource, BlobUploadObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::keys::KeyService;
use crate::storage::cloud::CloudHome;
use crate::sync::cloud_storage::CloudCipher;
use crate::sync::hlc::Hlc;
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::sync_loop::SyncLoopHandle;

/// Supplies the host's current config on demand. coven reads it fresh each call
/// — never snapshotting or writing it — so a host with reactive config sees
/// changes without rebuilding the manager.
pub type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

/// Refuse a membership operation on a plaintext home. Inviting wraps the library
/// key to a member and removing rotates it — both meaningless without a key — so
/// the caller must bail before mutating the membership chain or re-wrapping keys.
fn require_encrypted_home(cipher: &RwLock<CloudCipher>) -> Result<(), String> {
    if cipher.read().unwrap().is_plaintext() {
        return Err("sharing requires an encrypted cloud home".to_string());
    }
    Ok(())
}

/// High-level sync manager.
///
/// Holds an `EncryptionService` for an opaque home and `None` for a browsable
/// (plaintext) one — a browsable home has no library key. The at-rest cipher is
/// chosen per cycle from the home's [`HomeStorage`](crate::config::HomeStorage),
/// so the service is consulted only on an opaque home.
pub struct SyncManager {
    config_provider: ConfigProvider,
    key_service: KeyService,
    encryption_service: Option<EncryptionService>,
    db: Database,
    clock: ClockRef,
    blob_source: Arc<dyn BlobSource>,
    observer: Option<Arc<dyn BlobUploadObserver>>,

    /// coven's `_updated_at` register, the same `Arc<Hlc>` the owned [`Database`]
    /// holds. The sync loop advances it past pulled rows and stamps envelopes off
    /// it, so it shares the clock the host stamps rows from.
    hlc: Arc<Hlc>,

    // Mutable sync state — updated when providers are connected/disconnected
    sync_loop_handle: RwLock<Option<Arc<SyncLoopHandle>>>,
    cloud_home: RwLock<Option<Arc<dyn CloudHome>>>,
}

/// A member as returned by get_members.
#[derive(Debug, Clone)]
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

/// Sync status snapshot.
#[derive(Debug, Clone)]
pub struct SyncStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
}

impl SyncManager {
    /// Build the manager off the owned [`Database`]. The database already seeded
    /// its register clock past every value on disk at `open`; the manager shares
    /// that `Arc<Hlc>` so the sync loop's advance-on-pull and envelope stamps use
    /// the same instance the host stamps rows from.
    ///
    /// Construction is infallible and synchronous: seeding happened in
    /// `Database::open`. The manager is built lazily, only once a provider is
    /// connected.
    pub fn new(
        config_provider: ConfigProvider,
        key_service: KeyService,
        encryption_service: Option<EncryptionService>,
        db: Database,
        clock: ClockRef,
        blob_source: Arc<dyn BlobSource>,
        observer: Option<Arc<dyn BlobUploadObserver>>,
    ) -> Self {
        let hlc = db.hlc();
        Self {
            config_provider,
            key_service,
            encryption_service,
            db,
            clock,
            blob_source,
            observer,
            hlc,
            sync_loop_handle: RwLock::new(None),
            cloud_home: RwLock::new(None),
        }
    }

    pub fn encryption_service(&self) -> Option<&EncryptionService> {
        self.encryption_service.as_ref()
    }

    pub fn cloud_home(&self) -> Option<Arc<dyn CloudHome>> {
        self.cloud_home.read().unwrap().clone()
    }

    /// The at-rest cipher this home applies to its blob objects, derived from the
    /// configured storage mode (see [`CloudCipher::for_storage`]): `Encrypted`
    /// under the library key for an opaque home, `Plaintext` for a browsable one.
    /// `None` only for an opaque home with no encryption service (a locked
    /// library). A host streaming a managed blob builds a
    /// [`BlobRangeReader`](crate::sync::cloud_storage::BlobRangeReader) with this
    /// so a read applies the same protection the upload sealed under — the same
    /// cipher this manager builds the sync loop with in `start_sync`.
    pub fn blob_cipher(&self) -> Option<CloudCipher> {
        let config = (self.config_provider)();
        CloudCipher::for_storage(config.cloud_home.storage, self.encryption_service.clone())
    }

    pub fn sync_loop_handle(&self) -> Option<Arc<SyncLoopHandle>> {
        self.sync_loop_handle.read().unwrap().clone()
    }

    // =========================================================================
    // Sync lifecycle
    // =========================================================================

    /// Initialize cloud home and sync loop from current config.
    /// Called at startup (if already configured) and after connecting a provider.
    pub async fn start_sync(&self) {
        let config = (self.config_provider)();

        // Create cloud home
        let cloud_home: Option<Arc<dyn CloudHome>> = match crate::storage::cloud::create_cloud_home(
            &config,
            &self.key_service,
            self.clock.clone(),
        )
        .await
        {
            Ok(ch) => Some(Arc::from(ch)),
            Err(e) => {
                info!("Cloud home not available: {e}");
                return;
            }
        };

        *self.cloud_home.write().unwrap() = cloud_home;

        if !config.sync_enabled(&self.key_service) {
            return;
        }

        // The home's at-rest cipher: an opaque home seals under the manager's
        // library key, a browsable home stores in the clear. Built here so the
        // sync loop and storage share one instance — a member removal rotates the
        // key in place through it.
        let cipher =
            CloudCipher::for_storage(config.cloud_home.storage, self.encryption_service.clone())
                .expect("an opaque cloud home must be built with an encryption service");

        // Initialize sync loop. The synced-table set is owned by the Database, so
        // init_sync reads it from there rather than from a separately-held copy.
        let sync_loop = crate::sync::cycle::init_sync(
            &config,
            &self.key_service,
            &self.db,
            self.clock.clone(),
            &cipher,
            self.hlc.clone(),
        )
        .await;

        if let Some(components) = sync_loop {
            let library_dir = config.library_dir.clone();
            let handle = Arc::new(SyncLoopHandle::new(
                components,
                self.db.clone(),
                self.clock.clone(),
                library_dir,
                self.blob_source.clone(),
                self.observer.clone(),
            ));
            handle.start();

            info!("Sync loop started");
            *self.sync_loop_handle.write().unwrap() = Some(handle);
        }
    }

    /// Tear down the sync loop and cloud home.
    pub fn stop_sync(&self) {
        *self.sync_loop_handle.write().unwrap() = None;
        *self.cloud_home.write().unwrap() = None;

        info!("Sync loop stopped");
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
    // Keys / codes
    // =========================================================================

    pub fn get_user_pubkey(&self) -> Result<Option<String>, String> {
        self.key_service
            .get_user_public_key()
            .map(|opt| opt.map(hex::encode))
            .map_err(|e| format!("Failed to read user public key: {e}"))
    }

    pub fn generate_restore_code(&self) -> Result<String, String> {
        let config = (self.config_provider)();
        crate::storage::cloud::setup::generate_restore_code(&config, &self.key_service)
            .map_err(|e| e.to_string())
    }

    // =========================================================================
    // Membership
    // =========================================================================

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, String> {
        let config = (self.config_provider)();
        if !config.sync_enabled(&self.key_service) {
            return Ok(Vec::new());
        }

        let storage = crate::storage::cloud::setup::create_sync_storage(
            &config,
            &self.key_service,
            None,
            self.clock.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create storage client: {e}"))?;

        let user_pubkey = self
            .key_service
            .get_user_public_key()
            .map_err(|e| format!("Failed to read user public key: {e}"))?;
        let members = crate::sync::membership_ops::get_members(
            &storage,
            user_pubkey.as_ref().map(|k| k.as_slice()),
        )
        .await
        .map_err(|e| e.0)?;

        Ok(members
            .into_iter()
            .map(|m| MemberInfo {
                pubkey: m.pubkey,
                role: m.role,
                is_self: m.is_self,
            })
            .collect())
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        role: MemberRole,
    ) -> Result<String, String> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or("Sync is not configured")?;

        // Inviting a member wraps the library key to them, which only an encrypted
        // home has. Refuse before touching the membership chain.
        require_encrypted_home(sync_loop.cipher())?;

        let encryption_key_hex = self
            .key_service
            .get_encryption_key()
            .map_err(|e| format!("Failed to read encryption key: {e}"))?
            .ok_or("Encryption key not configured")?;

        let key_bytes: [u8; 32] = hex::decode(&encryption_key_hex)
            .map_err(|e| format!("Invalid encryption key hex: {e}"))?
            .try_into()
            .map_err(|_| "Encryption key wrong length".to_string())?;

        let (library_id, library_name) = {
            let config = (self.config_provider)();
            (config.library_id.clone(), config.library_name.clone())
        };

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let invite_code = crate::sync::membership_ops::invite_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
            role,
            &key_bytes,
            &library_id,
            &library_name,
        )
        .await
        .map_err(|e| e.0)?;

        Ok(crate::join_code::encode(&invite_code))
    }

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<String, String> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or("Sync is not configured")?;

        // Removing a member rotates the library key, which only an encrypted home
        // has. Refuse up front so a plaintext home never mutates the membership
        // chain or re-wraps keys before the rotation fails.
        require_encrypted_home(sync_loop.cipher())?;

        let library_id = (self.config_provider)().library_id.clone();

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let new_key = crate::sync::membership_ops::remove_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
            &library_id,
        )
        .await
        .map_err(|e| e.0)?;

        // Rotate the in-use key; the host records the returned fingerprint and
        // that a key is stored in its own config.
        let fingerprint = crate::sync::membership_ops::apply_key_rotation(
            new_key,
            &self.key_service,
            sync_loop.cipher(),
        )
        .map_err(|e| e.0)?;

        Ok(fingerprint)
    }
}
