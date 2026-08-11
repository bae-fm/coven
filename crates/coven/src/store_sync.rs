use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_security::StoreSecurity;
use coven_database::StoreDatabase;
use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_keys::encryption::EncryptionService;
use coven_protocol::blob::{BlobRef, BlobTransitionObserver};
use coven_protocol::objects::StorageError;
use coven_replication::blob::transition::{MakeLocalError, MakeRemoteError};
use coven_replication::sync::sync_loop::{SyncLoopHandle, SyncLoopStatus};
use coven_replication::sync::Store;
use coven_storage::cloud::setup::StorageSetupError;
#[cfg(test)]
use coven_storage::BlobChunking;
use coven_storage::{BlobPathScheme, CloudSyncConnection, CloudSyncObjectStorage};

pub(crate) type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

pub(crate) use coven_replication::sync::SyncError;

/// Who runs sync cycles for a connected cloud.
///
/// Production always connects under `Loop`: a background thread runs cycles on a
/// timer and on trigger, and an operation that queues work for a cycle to carry
/// out refuses once that thread is gone. `Caller` is the test-only alternative —
/// no thread exists and the host drives sync itself, so its own `drain_uploads`
/// is the only drain that runs and cannot lose a race with a cycle's.
enum SyncDriver {
    Loop,
    #[cfg(any(test, feature = "test-utils"))]
    Caller,
}

/// The store's sync connection.
///
/// Three questions that used to travel as one: whether a connection is installed
/// at all, whether it has a cloud to talk to, and — within `WithCloud` — who runs
/// its cycles. Each capability asks for exactly the state it needs. Reading the
/// connected cloud's path scheme needs only a cloud. Queueing a blob transition
/// needs a driver that will carry it out. A Circle write command needs the loop
/// *thread* itself, because that thread services the command channel.
enum SyncConnection {
    /// No connection installed: none was ever made, or the last was discarded.
    Disconnected,
    /// Connected with no cloud attached — either no provider is configured, or
    /// `stop_sync` released the one that was. `start_sync` rebuilds from config.
    WithoutCloud,
    /// Cloud-backed Store authority retained for commands while no sync loop is
    /// installed. Starting sync replaces this with `WithCloud`.
    CommandOnly { store: Arc<Store> },
    /// Connected over a cloud provider.
    WithCloud {
        sync: Arc<SyncLoopHandle>,
        #[cfg(test)]
        storage: Arc<dyn CloudSyncObjectStorage>,
        driver: SyncDriver,
    },
}

/// Who carries out a Store command that needs no running sync loop — the two
/// states [`StoreSync::ensure_command_authority`] can leave the connection in, and
/// therefore the only two it can answer with. Resolving to this instead of to
/// "the state is now good enough" is what leaves no third case to guard.
enum CommandAuthority {
    /// A cloud connection is installed; its loop handle serves the command.
    Connected(Arc<SyncLoopHandle>),
    /// No cloud connection; the Store retained for commands serves it.
    CommandOnly(Arc<Store>),
}

macro_rules! connected_sync {
    ($owner:expr) => {{
        let connection = $owner.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => Some(std::sync::Arc::clone(sync)),
            _ => None,
        }
    }};
}

macro_rules! active_sync {
    ($owner:expr) => {{
        let connection = $owner.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, driver, .. } => {
                let driven = match driver {
                    SyncDriver::Loop => sync.is_running(),
                    #[cfg(any(test, feature = "test-utils"))]
                    SyncDriver::Caller => true,
                };
                driven.then(|| std::sync::Arc::clone(sync))
            }
            _ => None,
        }
    }};
}

macro_rules! active_circle_sync {
    ($owner:expr) => {{
        let active = active_sync!($owner).ok_or(crate::CircleError::NotConfigured)?;
        if active.is_running() {
            Ok(active)
        } else {
            Err(crate::CircleError::LoopNotRunning)
        }
    }};
}

macro_rules! installed_command_authority {
    ($owner:expr) => {{
        let connection = $owner.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => {
                CommandAuthority::Connected(std::sync::Arc::clone(sync))
            }
            SyncConnection::CommandOnly { store } => {
                CommandAuthority::CommandOnly(std::sync::Arc::clone(store))
            }
            _ => unreachable!("command authority was installed under the lifecycle lock"),
        }
    }};
}

mod blobs;
mod commands;
mod connection;
mod test_access;

#[derive(Clone)]
pub(crate) struct StoreSync {
    config_provider: ConfigProvider,
    security: StoreSecurity,
    master_keys: Arc<dyn coven_keys::keys::MasterKeyCustody>,
    database: StoreDatabase,
    #[cfg(test)]
    store_dir: coven_foundation::store_dir::StoreDir,
    clock: ClockRef,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    open_guard: Arc<StoreOpenGuard>,
    cloud_storage: StoreCloudStorage,
    blob_access: crate::store_blobs::StoreBlobAccess,
    local_blob_transitions: coven_replication::blob::transition::LocalBlobTransitions,
    state: Arc<RwLock<SyncConnection>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    #[cfg(test)]
    stopped_loops: Arc<std::sync::atomic::AtomicU64>,
}

impl StoreSync {
    async fn install_storage_connection(
        &self,
        config: Config,
        storage: Arc<CloudSyncConnection>,
        driver: SyncDriver,
    ) -> Result<(), SyncError> {
        let routing_encryption = if storage.is_plaintext() {
            None
        } else {
            let keyring = self
                .master_keys
                .unlock()?
                .ok_or(coven_keys::keys::RoutingEncryptionError::NotEstablished)?;
            Some(EncryptionService::from(keyring))
        };
        let initialization = match self.database.local_store_root_ref().await? {
            Some(expected_store_root) => {
                coven_replication::sync::cycle::StoreInitialization::OpenStore {
                    expected_store_root,
                }
            }
            None => coven_replication::sync::cycle::StoreInitialization::CreateStore,
        };
        let components = self
            .security
            .initialize_sync_components(
                self.database.clone(),
                Arc::clone(&storage),
                initialization,
                routing_encryption.clone(),
            )
            .await?;
        let blob_transitions = coven_replication::blob::transition::ConnectedBlobTransitions::new(
            self.local_blob_transitions.clone(),
            Arc::new(self.blob_access.clone()),
            routing_encryption,
            self.observer.clone(),
        );
        let sync = Arc::new(SyncLoopHandle::new(
            components,
            blob_transitions,
            self.master_keys.clone(),
            self.clock.clone(),
            config,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
        ));
        if matches!(&driver, SyncDriver::Loop) {
            if let Err(error) = sync.start() {
                self.blob_access.clear_connection();
                return Err(SyncError::Loop(error));
            }
            info!("Sync loop started");
        }
        let storage: Arc<dyn CloudSyncObjectStorage> = storage;
        self.install_cloud(sync, storage, driver);
        Ok(())
    }

    /// Who carries out a command that needs no running sync loop, installing
    /// the command-only Store authority when the connection holds none.
    /// Callers hold the lifecycle lock across this and the command it resolves,
    /// so the authority they receive is the one still installed when they use
    /// it — and because they receive the authority rather than a promise about
    /// `self.state`, there is no "installed it, then failed to find it" case
    /// for them to re-check.
    async fn ensure_command_authority(&self) -> Result<(), SyncError> {
        if connected_sync!(self).is_some() {
            return Ok(());
        }
        if matches!(
            &*self.state.read().expect("read Store sync connection"),
            SyncConnection::CommandOnly { .. }
        ) {
            return Ok(());
        }
        let config = self.command_config();
        let storage = self
            .cloud_storage
            .open(&config, None, None)
            .await
            .map_err(Self::map_storage_setup_error)?;
        let store = Arc::new(
            self.security
                .load_store(self.database.clone(), Arc::new(storage))
                .await?,
        );
        *self.state.write().expect("write Store sync connection") =
            SyncConnection::CommandOnly { store };
        Ok(())
    }
}

#[cfg(test)]
mod tests;
