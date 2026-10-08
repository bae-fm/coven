//! Compose database, custody, operation and file owners at the store boundary.

use crate::authentication::Authentication;
use crate::*;
use coven_crypto::custody::{
    InMemoryCustody, Keychain, KeyringCustody, PassphraseCustody, StoreCustody, StoreKeychain,
};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreFile;
use coven_storage::{
    providers::{OAuthFlow, StorageConnector},
    Storage,
};
use std::sync::Arc;

/// The choices collected before opening a store (E1).
pub struct CovenBuilder {
    tables: Option<Vec<SyncedTable>>,
    migrations: Option<Vec<Migration>>,
    layout: StoreLayout,
    ids: IdSourceRef,
    clock: ClockRef,
    connector: Option<Arc<dyn StorageConnector>>,
    limits: TransferLimits,
    oauth: Option<OAuthClients>,
    presenter: Option<Arc<dyn OAuthPresenter>>,
    authentication: Option<Authentication>,
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
            layout,
            ids,
            clock,
            connector: None,
            oauth: None,
            #[cfg(not(any(target_os = "ios", target_os = "android")))]
            presenter: Some(Arc::new(DesktopOAuthPresenter)),
            #[cfg(any(target_os = "ios", target_os = "android"))]
            presenter: None,
            authentication: None,
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
    /// Replace provider construction in tests while exercising setup, credential
    /// custody, reconnect, restore and recovery through the production graph.
    /// `coven_storage::test_utils::MemoryStorage` implements this connector.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn storage_connector(mut self, connector: Arc<dyn StorageConnector>) -> Self {
        self.connector = Some(connector);
        self
    }
    fn connector(&mut self) -> Arc<dyn StorageConnector> {
        self.connector
            .get_or_insert_with(|| {
                Arc::new(coven_storage::providers::ProviderConnector::new(
                    self.clock.clone(),
                    self.ids.clone(),
                    self.cloudkit.clone(),
                ))
            })
            .clone()
    }

    /// Configure this app's own sign-in clients. Coven supplies no client ids.
    pub fn oauth_clients(mut self, clients: OAuthClients) -> Self {
        self.oauth = Some(clients);
        self
    }
    /// Present sign-in through the app's platform sheet. Desktop defaults to
    /// `DesktopOAuthPresenter`; iOS and Android require the app's presenter.
    pub fn oauth_presenter(mut self, presenter: Arc<dyn OAuthPresenter>) -> Self {
        self.presenter = Some(presenter);
        self
    }

    /// Sign in and keep tokens in session custody for restore, join or opening.
    /// Replaces the prior sign-in only on success. Dropping the future cancels it.
    pub async fn authenticate(&mut self, provider: CloudProvider) -> Result<(), OAuthError> {
        let flow = self.oauth_flow().ok_or(OAuthError::Unavailable(provider))?;
        let tokens = flow.authenticate(provider).await?;
        self.authentication = Some(Authentication { provider, tokens });
        Ok(())
    }

    fn oauth_flow(&self) -> Option<OAuthFlow> {
        Some(OAuthFlow::new(self.oauth.clone()?, self.presenter.clone()?))
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
    /// Opening always migrates coven's tables before the app's schema, then
    /// resumes unfinished operations and committed file work. An empty journal
    /// needs no keys; resumed steps read keys when needed. No sync loop starts,
    /// and local database calls need no unlocked key.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle> {
        crate::coven::blocking(move || self.open_graph(store, false))
            .await?
            .open()
            .await
    }

    /// Recover a damaged database from storage, preserving readable waiting work.
    /// Connects from saved settings and custody credentials, refreshing expired
    /// sign-in tokens. Storage and unlocked keys are checked before any database
    /// file moves. The fresh device id is registered with the app's device name.
    /// The damaged SQLite files remain in a named archive. An interrupted reload
    /// must be retried explicitly; ordinary opens refuse its unpublished state.
    pub async fn open_reloading(
        self,
        store: StoreId,
        device_name: &str,
    ) -> Result<CovenHandle, RecoveryError> {
        crate::coven::blocking(move || self.open_graph(store, true))
            .await?
            .open_reloading(device_name)
            .await
    }

    /// Opens the store for reading only, alongside a handle that has it open.
    /// Its shared lock protects both read connections and local cache metadata.
    /// It runs no migration and refuses a database whose schema is newer or
    /// whose coven tables need migrating.
    /// File reads connect using saved settings and custody credentials when present.
    pub async fn open_read_only(
        mut self,
        store: StoreId,
    ) -> Result<CovenReadHandle, ReadOnlyOpenError> {
        let directory = self.layout.store_dir(&store);
        let ids = self.ids.clone();
        let clock = self.clock.clone();
        let connector = self.connector();
        let limits = self.limits;
        let database = self.database(directory.clone())?;
        let database = database.open_read_only().await?;
        let settings = coven_storage::StorageSettings::new(directory.clone());
        let data = crate::coven::blocking(move || {
            let keychain = StoreKeychain::new(self.keychain()?, store);
            coven_sync::read_connection(&settings, &keychain)
        })
        .await?;
        let storage = match data {
            Some(data) => Some(
                connector
                    .connect(
                        data.location,
                        data.credentials,
                        directory.settings().map_err(CovenError::from)?.device_id,
                    )
                    .await
                    .map_err(SyncError::from)?,
            ),
            None => None,
        };
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::read_only(database.clone()),
            directory,
            storage,
            clock,
            ids,
            limits,
        );
        Ok(CovenReadHandle::new(database, files))
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
        Ok(DatabaseBuilder::new(directory)
            .migration_operation(coven_sync::StoreLogSync::migration_operation)
            .synced_tables(tables)
            .migrations(migrations)
            .clock(self.clock.clone())
            .id_source(self.ids.clone()))
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
        mut self,
        directory: StoreDir,
        keychain: Arc<StoreKeychain>,
    ) -> CovenResult<OpeningOwners> {
        let oauth = self.oauth_flow();
        let connector = self.connector();
        let settings = directory.settings()?;
        let has_storage_credentials = keychain.storage_credentials()?.is_some();
        let keys = Self::make_keys(self.keys, &directory, settings.id, keychain.clone());
        let identity =
            Self::make_identity(self.identity, &directory, settings.id, keychain.clone());
        Ok(OpeningOwners {
            directory,
            has_storage_credentials,
            custody: StoreCustody::new(identity.clone(), keychain.clone()),
            keychain,
            connector,
            oauth,
            authentication: self.authentication,
            limits: self.limits,
            device: settings.device_id,
            initial_name: settings.name,
            keys,
            identity,
            clock: self.clock,
            ids: self.ids,
            storage: None,
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
    oauth: Option<OAuthFlow>,
    authentication: Option<Authentication>,
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

    async fn open_reloading(mut self, device_name: &str) -> Result<CovenHandle, RecoveryError> {
        let mut data = coven_sync::read_connection(
            &coven_storage::StorageSettings::new(self.owners.directory.clone()),
            &self.owners.keychain,
        )?
        .ok_or(SyncError::NoStorage)?;
        self.owners
            .keys
            .unlock()
            .map_err(CovenError::from)?
            .ok_or(RecoveryError::NoStoreKeys)?;
        self.owners
            .identity
            .unlock()
            .map_err(CovenError::from)?
            .ok_or(SyncError::from(
                coven_storage::StorageFailure::MemberKeysMissing,
            ))?;
        if let Some(credentials) = coven_sync::refreshed_credentials(
            &data,
            self.owners.oauth.as_ref(),
            self.owners.clock.now(),
        )
        .await?
        {
            coven_sync::commit_credentials(&self.owners.keychain, &credentials, None)?;
            data.credentials = credentials;
        }
        let storage = self
            .owners
            .connector
            .connect(data.location, data.credentials, self.owners.device)
            .await
            .map_err(SyncError::from)?;
        let storage = Arc::new(coven_storage::StorageConnection::new(storage));
        storage
            .list(&ObjectPrefix::all())
            .await
            .map_err(SyncError::from)?;
        self.owners.storage = Some(storage);
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
                name: device_name.into(),
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
            self.authentication,
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
