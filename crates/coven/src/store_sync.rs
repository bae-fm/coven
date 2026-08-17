use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use tokio::sync::watch;
use tracing::{debug, info};

use crate::store_cloud_storage::StoreCloudStorage;
use crate::store_security::{StoreSecurity, SyncKeyCustody};
use coven_database::StoreDatabase;
use coven_foundation::clock::ClockRef;
use coven_foundation::config::Config;
use coven_foundation::store_dir::StoreOpenGuard;
use coven_protocol::blob::{BlobRef, BlobTransitionObserver};
use coven_protocol::objects::StorageError;
use coven_replication::blob::transition::{MakeLocalError, MakeRemoteError};
use coven_replication::sync::sync_loop::{
    PreparedSyncLoopRuntime, SyncLoopHandle, SyncLoopRuntimeFactory, SyncLoopStatus,
};
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

struct PreparedSyncConnection {
    sync: Option<Arc<SyncLoopHandle>>,
    storage: Option<Arc<dyn CloudSyncObjectStorage>>,
    driver: Option<SyncDriver>,
}

struct PreparedStorageInitialization {
    components: coven_replication::sync::cycle::PreparedSyncComponents,
    storage: Arc<dyn CloudSyncObjectStorage>,
    driver: SyncDriver,
    config: Config,
    runtime: Option<PreparedSyncLoopRuntime>,
}

impl StoreSync {
    async fn initialize_storage(
        &self,
        initialization: PreparedStorageInitialization,
    ) -> Result<PreparedSyncConnection, SyncError> {
        let components = initialization
            .components
            .initialize(self.observer.clone())
            .await?;
        let sync = Arc::new(SyncLoopHandle::new(
            components,
            self.clock.clone(),
            initialization.config,
            self.observer.clone(),
            self.open_guard.clone(),
            self.status_tx.clone(),
            self.eager_cache_status_tx.clone(),
            initialization.runtime,
        ));
        if matches!(&initialization.driver, SyncDriver::Loop) {
            info!("Sync loop prepared");
        }
        Ok(PreparedSyncConnection {
            sync: Some(sync),
            storage: Some(initialization.storage),
            driver: Some(initialization.driver),
        })
    }
}

impl PreparedSyncConnection {
    fn install(mut self, owner: &StoreSync) {
        if matches!(&self.driver, Some(SyncDriver::Loop)) {
            self.sync
                .as_ref()
                .expect("prepared sync exists until install")
                .activate();
            info!("Sync loop activated");
        }
        let sync = self
            .sync
            .take()
            .expect("prepared sync exists until install");
        let storage = self
            .storage
            .take()
            .expect("prepared storage exists until install");
        let driver = self
            .driver
            .take()
            .expect("prepared driver exists until install");
        owner.install_cloud(sync, storage, driver);
    }
}

impl Drop for PreparedSyncConnection {
    fn drop(&mut self) {
        if matches!(&self.driver, Some(SyncDriver::Loop))
            && self.sync.as_ref().is_some_and(|sync| sync.is_running())
        {
            self.sync.as_ref().expect("checked sync").stop();
        }
    }
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
mod setup;
mod test_access;

#[derive(Clone)]
pub(crate) struct StoreSync {
    config_provider: ConfigProvider,
    security: StoreSecurity,
    database: StoreDatabase,
    #[cfg(test)]
    store_dir: coven_foundation::store_dir::StoreDir,
    clock: ClockRef,
    observer: Option<Arc<dyn BlobTransitionObserver>>,
    open_guard: Arc<StoreOpenGuard>,
    cloud_storage: StoreCloudStorage,
    blob_access: crate::store_blobs::StoreBlobAccess,
    state: Arc<RwLock<SyncConnection>>,
    lifecycle: Arc<tokio::sync::Mutex<()>>,
    status_tx: tokio::sync::watch::Sender<SyncLoopStatus>,
    eager_cache_status_tx:
        tokio::sync::watch::Sender<coven_replication::sync::store::EagerCacheFillStatus>,
    runtime_factory: Arc<dyn SyncLoopRuntimeFactory>,
    #[cfg(test)]
    stopped_loops: Arc<std::sync::atomic::AtomicU64>,
}

impl StoreSync {
    async fn execute_cloud_operation<T, E, F, Future>(
        &self,
        operation: F,
        runtime_error: fn(coven_storage::cloud::CloudRuntimeError) -> E,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(StoreSync) -> Future + Send + 'static,
        Future: std::future::Future<Output = Result<T, E>> + Send + 'static,
    {
        let owner = self.clone();
        self.cloud_storage
            .execute(move || operation(owner))
            .await
            .map_err(runtime_error)?
    }

    fn cloud_runtime_sync_error(error: coven_storage::cloud::CloudRuntimeError) -> SyncError {
        coven_storage::cloud::CloudHomeError::transport("run cloud lifecycle operation", error)
            .into()
    }

    async fn prepare_storage_initialization(
        &self,
        config: Config,
        storage: Arc<CloudSyncConnection>,
        driver: SyncDriver,
        key_custody: SyncKeyCustody,
    ) -> Result<PreparedStorageInitialization, SyncError> {
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
            .prepare_sync_components(
                self.database.clone(),
                Arc::clone(&storage),
                initialization,
                key_custody,
            )
            .await?;
        let runtime = match driver {
            SyncDriver::Loop => Some(self.runtime_factory.prepare().map_err(SyncError::Loop)?),
            #[cfg(any(test, feature = "test-utils"))]
            SyncDriver::Caller => None,
        };
        let storage: Arc<dyn CloudSyncObjectStorage> = storage;
        Ok(PreparedStorageInitialization {
            components,
            config,
            storage,
            driver,
            runtime,
        })
    }

    async fn prepare_storage_connection(
        &self,
        config: Config,
        storage: Arc<CloudSyncConnection>,
        driver: SyncDriver,
    ) -> Result<PreparedSyncConnection, SyncError> {
        let initialization = self
            .prepare_storage_initialization(config, storage, driver, SyncKeyCustody::Current)
            .await?;
        self.initialize_storage(initialization).await
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
