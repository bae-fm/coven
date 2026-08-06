use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, error, info};

use crate::blob::transition::{MakeLocalError, MakeRemoteError};
use crate::database::StoreDatabase;
use crate::protocol::blob::{BlobRef, BlobTransitionObserver};
use crate::protocol::objects::StorageError;
use crate::storage::cloud::setup::StorageSetupError;
#[cfg(any(test, feature = "test-utils"))]
use crate::storage::cloud::CloudHome;
#[cfg(test)]
use crate::storage::BlobChunking;
use crate::storage::{BlobPathScheme, CloudSyncStorage, SyncStorage};
use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_security::StoreSecurity;
use crate::sync::cycle::SyncComponents;
use crate::sync::store::blob::LocalStoreBlobAccess;
use crate::sync::sync_loop::{SyncLoopHandle, SyncLoopStatus};
use crate::sync::Store;
use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_keys::encryption::EncryptionService;

pub(crate) type ConfigProvider = Arc<dyn Fn() -> Config + Send + Sync>;

pub(crate) use crate::sync::SyncError;

mod blobs;
mod commands;
mod connection;
mod test_access;

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
        storage: Arc<dyn SyncStorage>,
        driver: SyncDriver,
    },
}

/// Who carries out a Store command that needs no running sync loop — the two
/// states [`StoreSync::command_authority`] can leave the connection in, and
/// therefore the only two it can answer with. Resolving to this instead of to
/// "the state is now good enough" is what leaves no third case to guard.
enum CommandAuthority {
    /// A cloud connection is installed; its loop handle serves the command.
    Connected(Arc<SyncLoopHandle>),
    /// No cloud connection; the Store retained for commands serves it.
    CommandOnly(Arc<Store>),
}

#[derive(Clone)]
pub(crate) struct StoreSync {
    config_provider: ConfigProvider,
    security: StoreSecurity,
    master_keys: Arc<dyn coven_keys::keys::MasterKeyCustody>,
    database: StoreDatabase,
    store_dir: coven_foundation::store_dir::StoreDir,
    clock: ClockRef,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    open_guard: Arc<StoreOpenGuard>,
    cloud_storage: StoreCloudStorage,
    local_blob_access: LocalStoreBlobAccess,
    blob_access: crate::store_blobs::StoreBlobAccess,
    local_blob_transitions: crate::blob::transition::LocalBlobTransitions,
    state: Arc<RwLock<SyncConnection>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    #[cfg(test)]
    stopped_loops: Arc<std::sync::atomic::AtomicU64>,
}

impl StoreSync {
    /// The loop handle of an installed cloud connection, whoever drives its
    /// cycles. Enough for anything the connection already knows — its config,
    /// path scheme, uploader — and for waking a cycle that may or may not run.
    fn connected(&self) -> Option<Arc<SyncLoopHandle>> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, .. } => Some(Arc::clone(sync)),
            _ => None,
        }
    }

    /// The loop handle of a connection whose cycles someone is actually
    /// driving. Required by anything that queues work for a cycle to carry out,
    /// which a connection with no driver would never complete.
    fn active(&self) -> Option<Arc<SyncLoopHandle>> {
        let connection = self.state.read().expect("read Store sync connection");
        match &*connection {
            SyncConnection::WithCloud { sync, driver, .. } => {
                let driven = match driver {
                    SyncDriver::Loop => sync.is_running(),
                    #[cfg(any(test, feature = "test-utils"))]
                    SyncDriver::Caller => true,
                };
                driven.then(|| Arc::clone(sync))
            }
            _ => None,
        }
    }

    async fn initialize_components(
        &self,
        storage: Arc<CloudSyncStorage>,
        routing_encryption: Option<EncryptionService>,
    ) -> Result<SyncComponents, SyncError> {
        let initialization = match self.database.local_store_root_ref().await? {
            Some(expected_store_root) => crate::sync::cycle::StoreInitialization::OpenStore {
                expected_store_root,
            },
            None => crate::sync::cycle::StoreInitialization::CreateStore,
        };
        self.security
            .established_identity()?
            .initialize_sync_components(
                self.database.clone(),
                self.store_dir.clone(),
                self.local_blob_access.clone(),
                storage,
                initialization,
                routing_encryption,
            )
            .await
            .map_err(SyncError::from)
    }

    /// Assemble the connected sync over `storage`. The loop thread is a separate
    /// decision the caller makes: production starts it, a caller-driven test
    /// connection leaves it unstarted.
    fn build_sync(
        &self,
        components: SyncComponents,
        config: Config,
        routing_encryption: Option<EncryptionService>,
    ) -> Arc<SyncLoopHandle> {
        let blob_transitions = crate::blob::transition::ConnectedBlobTransitions::new(
            self.local_blob_transitions.clone(),
            Arc::new(self.blob_access.clone()),
            routing_encryption,
            self.observer.clone(),
        );
        Arc::new(SyncLoopHandle::new(
            components,
            blob_transitions,
            self.master_keys.clone(),
            self.clock.clone(),
            config,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
        ))
    }

    /// Who carries out a command that needs no running sync loop, installing
    /// the command-only Store authority when the connection holds none.
    /// Callers hold the lifecycle lock across this and the command it resolves,
    /// so the authority they receive is the one still installed when they use
    /// it — and because they receive the authority rather than a promise about
    /// `self.state`, there is no "installed it, then failed to find it" case
    /// for them to re-check.
    async fn command_authority(&self) -> Result<CommandAuthority, SyncError> {
        if let Some(sync) = self.connected() {
            return Ok(CommandAuthority::Connected(sync));
        }
        if let SyncConnection::CommandOnly { store } =
            &*self.state.read().expect("read Store sync connection")
        {
            return Ok(CommandAuthority::CommandOnly(Arc::clone(store)));
        }
        let config = self.command_config();
        let storage = self
            .cloud_storage
            .open(&config, None, None)
            .await
            .map_err(SyncError::StorageSetup)?;
        let store = Arc::new(
            self.security
                .established_identity()?
                .load_store(
                    self.database.clone(),
                    Arc::new(storage),
                    self.store_dir.clone(),
                )
                .await
                .map_err(SyncError::from)?,
        );
        *self.state.write().expect("write Store sync connection") = SyncConnection::CommandOnly {
            store: Arc::clone(&store),
        };
        Ok(CommandAuthority::CommandOnly(store))
    }

    /// A Circle write command needs the loop *thread* itself, because that
    /// thread services the command channel a caller-driven connection has none
    /// of.
    fn active_circle_operation(&self) -> Result<Arc<SyncLoopHandle>, crate::CircleError> {
        let active = self.active().ok_or(crate::CircleError::NotConfigured)?;
        if !active.is_running() {
            return Err(crate::CircleError::LoopNotRunning);
        }
        Ok(active)
    }
}

#[cfg(test)]
mod tests;
