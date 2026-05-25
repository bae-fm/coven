//! High-level sync manager: lifecycle, membership, status.
//!
//! Owns the sync lifecycle — cloud home + sync loop — and starts/stops it when
//! a provider is connected/disconnected, no app restart required. The host
//! supplies the config snapshot, keys, encryption, database, clock, and blob
//! handling; coven drives the rest.

use std::sync::{Arc, RwLock};

use tracing::info;

use crate::blob::{BlobPlan, BlobUploadObserver};
use crate::clock::ClockRef;
use crate::config::Config;
use crate::db::SyncDb;
use crate::encryption::EncryptionService;
use crate::keys::KeyService;
use crate::storage::cloud::CloudHome;
use crate::sync::membership::MemberRole;
use crate::sync::storage::SyncStorage;
use crate::sync::sync_loop::SyncLoopHandle;

/// High-level sync manager.
///
/// Always has a valid EncryptionService — if no encryption key exists,
/// don't create a SyncManager at all.
pub struct SyncManager {
    config: RwLock<Config>,
    key_service: KeyService,
    encryption_service: EncryptionService,
    db: Arc<dyn SyncDb>,
    clock: ClockRef,
    blob_plan: Arc<dyn BlobPlan>,
    observer: Option<Arc<dyn BlobUploadObserver>>,

    // Mutable sync state — updated when providers are connected/disconnected
    sync_loop_handle: RwLock<Option<Arc<SyncLoopHandle>>>,
    cloud_home: RwLock<Option<Arc<dyn CloudHome>>>,
}

/// A member as returned by get_members.
pub struct MemberInfo {
    pub pubkey: String,
    pub role: MemberRole,
    pub is_self: bool,
}

/// Sync status snapshot.
pub struct SyncStatus {
    pub configured: bool,
    pub syncing: bool,
    pub last_sync_time: Option<String>,
    pub error: Option<String>,
    pub device_count: u32,
}

impl SyncManager {
    pub fn new(
        config: Config,
        key_service: KeyService,
        encryption_service: EncryptionService,
        db: Arc<dyn SyncDb>,
        clock: ClockRef,
        blob_plan: Arc<dyn BlobPlan>,
        observer: Option<Arc<dyn BlobUploadObserver>>,
    ) -> Self {
        Self {
            config: RwLock::new(config),
            key_service,
            encryption_service,
            db,
            clock,
            blob_plan,
            observer,
            sync_loop_handle: RwLock::new(None),
            cloud_home: RwLock::new(None),
        }
    }

    pub fn encryption_service(&self) -> &EncryptionService {
        &self.encryption_service
    }

    pub fn cloud_home(&self) -> Option<Arc<dyn CloudHome>> {
        self.cloud_home.read().unwrap().clone()
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
        let config = self.config.read().unwrap().clone();

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

        // Initialize sync loop
        let sync_loop = crate::sync::cycle::init_sync(
            &config,
            &self.key_service,
            self.db.as_ref(),
            self.clock.clone(),
            &self.encryption_service,
        )
        .await;

        if let Some(components) = sync_loop {
            let library_dir = config.library_dir.clone();
            let handle = Arc::new(SyncLoopHandle::new(
                components,
                self.db.clone(),
                self.clock.clone(),
                library_dir,
                self.blob_plan.clone(),
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

    pub fn get_user_pubkey(&self) -> Option<String> {
        self.key_service.get_user_public_key().map(hex::encode)
    }

    pub fn generate_restore_code(&self) -> Result<String, String> {
        let config = self.config.read().unwrap().clone();
        crate::storage::cloud::setup::generate_restore_code(&config, &self.key_service)
            .map_err(|e| e.to_string())
    }

    // =========================================================================
    // Membership
    // =========================================================================

    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, String> {
        let config = self.config.read().unwrap().clone();
        if !config.sync_enabled(&self.key_service) {
            return Ok(Vec::new());
        }

        let storage = crate::storage::cloud::setup::create_sync_storage(
            &config,
            &self.key_service,
            &Some(self.encryption_service.clone()),
            self.clock.clone(),
        )
        .await
        .map_err(|e| format!("Failed to create storage client: {e}"))?;

        let user_pubkey = self.key_service.get_user_public_key();
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

        let encryption_key_hex = self
            .key_service
            .get_encryption_key()
            .ok_or("Encryption key not configured")?;

        let key_bytes: [u8; 32] = hex::decode(&encryption_key_hex)
            .map_err(|e| format!("Invalid encryption key hex: {e}"))?
            .try_into()
            .map_err(|_| "Encryption key wrong length".to_string())?;

        let (library_id, library_name) = {
            let config = self.config.read().unwrap();
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

    pub async fn remove_member(&self, public_key_hex: &str) -> Result<(), String> {
        let sync_loop = self
            .sync_loop_handle
            .read()
            .unwrap()
            .clone()
            .ok_or("Sync is not configured")?;

        let storage: &dyn SyncStorage = &**sync_loop.storage();
        let cloud_home = sync_loop.storage().cloud_home();

        let new_key = crate::sync::membership_ops::remove_member(
            storage,
            cloud_home,
            sync_loop.user_keypair(),
            sync_loop.hlc(),
            public_key_hex,
        )
        .await
        .map_err(|e| e.0)?;

        // Record that an encryption key is stored and persist the rotated key.
        let config = {
            let mut config = self.config.write().unwrap();
            config.encryption_key_stored = true;
            let _ = config.save();
            config.clone()
        };

        crate::sync::membership_ops::apply_key_rotation(
            new_key,
            &self.key_service,
            sync_loop.encryption(),
            &config,
        )
        .map_err(|e| e.0)?;

        Ok(())
    }
}
