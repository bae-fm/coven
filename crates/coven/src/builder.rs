//! Compose database, custody, operation and file owners at the store boundary.

use crate::*;
use coven_crypto::custody::{
    InMemoryCustody, Keychain, KeyringCustody, PassphraseCustody, StoreCustody, StoreKeychain,
    StoreKeys,
};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreFile;
use coven_storage::Storage;
use std::sync::Arc;

/// The choices collected before opening a store (E1).
pub struct CovenBuilder {
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    policy: Option<CovenMigrationPolicy>,
    layout: StoreLayout,
    ids: IdSourceRef,
    clock: ClockRef,
    storage: Option<Arc<dyn coven_storage::Storage>>,
    connector: Option<Arc<dyn StorageConnector>>,
    limits: TransferLimits,
    oauth: Option<OAuthClients>,
    cloudkit: Option<Arc<dyn CloudKitOps>>,
    keys: KeyCustody,
    identity: IdentityCustody,
    #[cfg(any(test, feature = "test-utils"))]
    keychain: Option<Arc<Keychain>>,
}

impl CovenBuilder {
    pub(crate) fn new(layout: StoreLayout) -> Self {
        let ids: IdSourceRef = Arc::new(UuidIds);
        let clock: ClockRef = Arc::new(SystemClock);
        Self {
            tables: None,
            migrations: None,
            policy: None,
            layout,
            ids,
            clock,
            storage: None,
            connector: None,
            oauth: None,
            limits: TransferLimits::default(),
            cloudkit: None,
            keys: KeyCustody::Keyring,
            identity: IdentityCustody::Keyring,
            #[cfg(any(test, feature = "test-utils"))]
            keychain: None,
        }
    }

    /// The tables that sync (E2). Required.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.tables = Some(tables);
        self
    }
    /// The app's schema migrations, numbered from 1 with no gaps (E13).
    /// Required.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.migrations = Some(migrations);
        self
    }
    /// Whether opening may migrate coven's own tables to this version of
    /// coven (§17.2). Required by `open`.
    pub fn coven_migration_policy(mut self, policy: CovenMigrationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.clock = clock;
        self
    }
    /// The source of new ids (§20.2). Defaults to `UuidIds`, random UUIDs.
    pub fn id_source(mut self, ids: IdSourceRef) -> Self {
        self.ids = ids;
        self
    }
    /// Supply the connected storage capability used by operations and files.
    /// Provider setup and credential custody remain with their existing owners.
    pub fn storage(mut self, storage: Arc<dyn coven_storage::Storage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Supply provider construction for setup and reconnect; tests can use memory storage.
    pub fn storage_connector(mut self, connector: Arc<dyn StorageConnector>) -> Self {
        self.connector = Some(connector);
        self
    }
    /// Configure this app's own sign-in clients. Coven supplies no client ids.
    pub fn oauth_clients(mut self, clients: OAuthClients) -> Self {
        self.oauth = Some(clients);
        self
    }
    /// Supply the app's native CloudKit bridge.
    pub fn apply_cloudkit_ops(mut self, ops: Option<Arc<dyn CloudKitOps>>) -> Self {
        self.cloudkit = ops;
        self
    }

    /// Limit file uploads running together; defaults to one.
    pub fn max_concurrent_uploads(mut self, n: std::num::NonZeroUsize) -> Self {
        self.limits.uploads = n;
        self
    }
    /// Limit downloads in each pin call; defaults to one.
    pub fn max_concurrent_downloads(mut self, n: std::num::NonZeroUsize) -> Self {
        self.limits.downloads = n;
        self
    }

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `StoreKeyCustody`.
    pub fn key_custody(mut self, custody: KeyCustody) -> Self {
        self.keys = custody;
        self
    }
    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(mut self, custody: IdentityCustody) -> Self {
        self.identity = custody;
        self
    }

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening runs migrations and resumes unfinished operations and committed
    /// file work. An empty journal needs no keys; resumed steps read keys when
    /// needed. No sync loop starts, and local database calls need no unlocked key.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle> {
        crate::coven::blocking(move || self.open_graph(store, false))
            .await?
            .open()
            .await
    }

    /// Recover a damaged database from storage, preserving readable waiting work.
    /// Storage and unlocked keys are required before any database file moves.
    /// The damaged SQLite files remain in a named archive. An interrupted reload
    /// must be retried explicitly; ordinary opens refuse its unpublished state.
    pub async fn open_reloading(self, store: StoreId) -> Result<CovenHandle, RecoveryError> {
        crate::coven::blocking(move || self.open_graph(store, true))
            .await?
            .open_reloading()
            .await
    }

    /// Opens the store for reading only, alongside a handle that has it open.
    /// Its shared lock protects both read connections and local cache metadata.
    /// It runs no migration and refuses a database whose schema is newer or
    /// whose coven tables need migrating.
    pub async fn open_read_only(self, store: StoreId) -> CovenResult<CovenReadHandle> {
        let directory = self.layout.store_dir(&store);
        let ids = self.ids.clone();
        let clock = self.clock.clone();
        let storage = self.storage.clone();
        let limits = self.limits;
        let (database, keys) = crate::coven::blocking(move || self.read_graph(store)).await?;
        let database = database.open_read_only().await?;
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::read_only(database.clone()),
            directory,
            storage,
            clock,
            ids,
            limits,
        );
        Ok(CovenReadHandle::new(database, keys, files))
    }

    fn database(&self, directory: StoreDir) -> CovenResult<DatabaseBuilder> {
        let tables = self
            .tables
            .clone()
            .ok_or(CovenError::MissingConfiguration {
                field: "synced_tables",
            })?;
        let migrations = self
            .migrations
            .clone()
            .ok_or(CovenError::MissingConfiguration {
                field: "migrations",
            })?;
        let mut database = DatabaseBuilder::new(directory)
            .migration_operation(coven_sync::StoreLogSync::migration_operation)
            .synced_tables(tables)
            .migrations(migrations)
            .clock(self.clock.clone())
            .id_source(self.ids.clone());
        if let Some(policy) = self.policy {
            database = database.coven_migration_policy(policy);
        }
        Ok(database)
    }

    fn keychain(&self) -> Result<Arc<Keychain>, KeyError> {
        #[cfg(any(test, feature = "test-utils"))]
        if let Some(keychain) = &self.keychain {
            return Ok(keychain.clone());
        }
        Keychain::registered()
    }

    fn open_graph(self, store: StoreId, recovering: bool) -> CovenResult<OpeningStore> {
        let directory = self.layout.store_dir(&store);
        let database = self.database(directory.clone())?;
        let lock = directory.lock_exclusive()?;
        let settings = lock.settings()?;
        let keychain = Arc::new(StoreKeychain::new(self.keychain()?, settings.id));
        let new_recovery = recovering
            && match directory.check_database_recovery() {
                Ok(()) => true,
                Err(StoreLockError::RecoveryPending(_)) => false,
                Err(error) => return Err(error.into()),
            };
        if new_recovery || keychain.device_id()? != Some(settings.device_id) {
            let device = self.ids.new_device_id();
            lock.set_device_id(device)?;
            keychain.set_device_id(device)?;
        }
        Ok(OpeningStore {
            database,
            lock,
            owners: self.owners(directory, keychain)?,
        })
    }

    fn owners(
        self,
        directory: StoreDir,
        keychain: Arc<StoreKeychain>,
    ) -> CovenResult<OpeningOwners> {
        let settings = directory.settings()?;
        let has_storage_credentials = keychain.storage_credentials()?.is_some();
        let keys = Self::make_keys(self.keys, &directory, settings.id, keychain.clone());
        let identity =
            Self::make_identity(self.identity, &directory, settings.id, keychain.clone());
        Ok(OpeningOwners {
            directory,
            has_storage_credentials,
            custody: StoreCustody::new(
                StoreKeys::new(keys.clone()),
                identity.clone(),
                keychain.clone(),
            ),
            keychain,
            connector: match self.connector {
                Some(connector) => connector,
                None => Arc::new(coven_storage::providers::ProviderConnector::new(
                    self.clock.clone(),
                    self.ids.clone(),
                    self.cloudkit,
                )),
            },
            oauth: self.oauth,
            limits: self.limits,
            device: settings.device_id,
            initial_name: settings.name,
            keys,
            identity,
            clock: self.clock,
            ids: self.ids,
            storage: self
                .storage
                .map(|storage| Arc::new(coven_storage::StorageConnection::new(storage))),
        })
    }

    fn make_identity(
        custody: IdentityCustody,
        directory: &StoreDir,
        id: StoreId,
        keychain: Arc<StoreKeychain>,
    ) -> Arc<dyn MemberKeyCustody> {
        match custody {
            IdentityCustody::Keyring => Arc::new(KeyringCustody::new(keychain.clone())),
            IdentityCustody::Passphrase(secret) => Arc::new(PassphraseCustody::new(
                secret,
                directory.owned_file(StoreFile::MemberKeys),
                id,
            )),
            IdentityCustody::InMemory => Arc::new(InMemoryCustody::empty()),
            IdentityCustody::Custom(keys) => keys,
        }
    }

    fn read_graph(self, store: StoreId) -> CovenResult<(DatabaseBuilder, StoreKeys)> {
        let directory = self.layout.store_dir(&store);
        let database = self.database(directory.clone())?;
        let keychain = Arc::new(StoreKeychain::new(self.keychain()?, store));
        let keys = Self::make_keys(self.keys, &directory, store, keychain);
        Ok((database, StoreKeys::new(keys)))
    }

    fn make_keys(
        custody: KeyCustody,
        directory: &StoreDir,
        id: StoreId,
        keychain: Arc<StoreKeychain>,
    ) -> Arc<dyn StoreKeyCustody> {
        match custody {
            KeyCustody::Keyring => Arc::new(KeyringCustody::new(keychain)),
            KeyCustody::Passphrase(secret) => Arc::new(PassphraseCustody::new(
                secret,
                directory.owned_file(StoreFile::StoreKeys),
                id,
            )),
            KeyCustody::InMemory => Arc::new(InMemoryCustody::empty()),
            KeyCustody::Custom(keys) => keys,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn with_keychain(mut self, keychain: Arc<Keychain>) -> Self {
        self.keychain = Some(keychain);
        self
    }
}

/// Retains the graph while its asynchronous database opening is in progress.
struct OpeningStore {
    database: DatabaseBuilder,
    lock: coven_foundation::files::StoreLock,
    owners: OpeningOwners,
}

struct OpeningOwners {
    has_storage_credentials: bool,
    initial_name: String,
    limits: TransferLimits,
    keychain: Arc<StoreKeychain>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthClients>,
    device: DeviceId,
    directory: StoreDir,
    custody: StoreCustody,
    keys: Arc<dyn StoreKeyCustody>,
    identity: Arc<dyn MemberKeyCustody>,
    clock: ClockRef,
    ids: IdSourceRef,
    storage: Option<Arc<coven_storage::StorageConnection>>,
}

impl OpeningStore {
    async fn open(self) -> CovenResult<CovenHandle> {
        let database = self.database.open_locked(self.lock).await?;
        let sync = self.owners.sync(database.clone());
        Ok(self.owners.handle(database, sync))
    }

    async fn open_reloading(self) -> Result<CovenHandle, RecoveryError> {
        let storage = self.owners.storage.as_ref().ok_or(SyncError::NoStorage)?;
        self.owners
            .keys
            .unlock()
            .map_err(CovenError::from)?
            .ok_or(RecoveryError::NoStoreKeys)?;
        self.owners
            .identity
            .unlock()
            .map_err(CovenError::from)?
            .ok_or(SyncError::MissingMemberKeys)?;
        storage
            .list(&ObjectPrefix::all())
            .await
            .map_err(SyncError::from)?;
        let archive = coven_foundation::files::FileName::new(self.owners.ids.new_id().to_string())
            .expect("UUID filename");
        let database = self
            .database
            .open_reloading_locked(self.lock, archive)
            .await?;
        let mut sync = self.owners.sync(database.clone());
        // Store-log replay and snapshot loading remain owned by sync. The new
        // device identity prevents reuse of numbers the damaged file cannot supply.
        sync.sync_store_log().await.map_err(SyncError::from)?;
        sync.reload_from_snapshots().await?;
        let local = database.local_store_log().await.map_err(CovenError::from)?;
        if !local.log.replay.state.devices.contains_key(&local.device) {
            sync.make_and_upload_entry(coven_format::store_log::StoreChange::AddDevice {
                device: local.device,
                name: "Recovered device".into(),
            })
            .await?;
        }
        database.finish_recovery().await.map_err(CovenError::from)?;
        Ok(self.owners.handle(database, sync))
    }
}

impl OpeningOwners {
    fn sync(&self, database: coven_database::Database) -> coven_sync::StoreLogSync {
        match &self.storage {
            Some(storage) => coven_sync::StoreLogSync::new(
                storage.clone(),
                database,
                self.keys.clone(),
                self.identity.clone(),
                self.clock.clone(),
                self.ids.clone(),
                self.directory.clone(),
            ),
            None => coven_sync::StoreLogSync::disconnected(
                database,
                self.keys.clone(),
                self.identity.clone(),
                self.clock.clone(),
                self.ids.clone(),
                self.directory.clone(),
            ),
        }
    }

    fn handle(
        self,
        database: coven_database::Database,
        sync: coven_sync::StoreLogSync,
    ) -> CovenHandle {
        let writes = match &self.storage {
            Some(storage) => coven_sync::DeviceLogSync::new(
                storage.clone(),
                database.clone(),
                self.keys.clone(),
                self.identity.clone(),
            ),
            None => coven_sync::DeviceLogSync::disconnected(
                database.clone(),
                self.keys.clone(),
                self.identity.clone(),
            ),
        };
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            self.directory.clone(),
            self.storage
                .clone()
                .map(|storage| storage as Arc<dyn coven_storage::Storage>),
            self.clock.clone(),
            self.ids.clone(),
            self.limits,
        );
        let operations =
            coven_sync::Operations::new(sync, files.clone(), writes, self.clock.clone());
        let codes = coven_sync::RestoreCodes::new(
            database.clone(),
            self.identity,
            self.keychain,
            coven_storage::StorageSettings::new(self.directory.clone()),
            operations.clone(),
            self.oauth.clone(),
        );
        let sync = coven_sync::SyncLoop::new(
            operations.clone(),
            codes.clone(),
            database.sync_changes(),
            self.clock.clone(),
            self.storage,
            self.connector.clone(),
            self.device,
            self.has_storage_credentials,
        );
        let storage = Arc::new(crate::storage::StorageConnections::new(
            codes.clone(),
            self.initial_name,
            self.keys,
            self.connector,
            self.oauth,
            sync.clone(),
            self.device,
            self.ids,
            self.clock,
        ));
        CovenHandle::new(
            database,
            self.custody,
            operations,
            files,
            sync,
            storage,
            codes,
        )
    }
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;

#[path = "bootstrap.rs"]
mod bootstrap;
pub use bootstrap::{join_with_invite, restore_from_code, restore_from_keychain, BootstrapError};
