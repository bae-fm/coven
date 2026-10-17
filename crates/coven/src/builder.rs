//! Compose database, custody, operation and file owners at the store boundary.

use crate::*;
use coven_crypto::custody::{
    InMemoryCustody, KeySession, Keychain, KeyringCustody, PassphraseCustody, StoreCustody,
    StoreKeychain,
};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreFile;
use coven_storage::{
    providers::{OAuthFlow, StorageConnector},
    Storage,
};
use std::sync::Arc;

/// The choices for creating or opening a store (E1).
#[cfg_attr(
    not(feature = "test-utils"),
    doc = "\nProduction builders cannot replace the random id source.\n```compile_fail\nuse coven::{Coven, StoreLayout, UuidIds};\nuse std::sync::Arc;\nlet builder = Coven::builder(StoreLayout::new(\"stores\".into()));\nlet _ = builder.id_source(Arc::new(UuidIds));\n```"
)]
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
    /// The latest sign-in, held for this session until bootstrap or setup publishes it.
    authentication: Option<Arc<coven_sync::SessionSignIn>>,
    keys: CustodyChoice<KeyCustody, StoreKeyring>,
    identity: CustodyChoice<IdentityCustody, MemberIdentity>,
    #[cfg(any(test, feature = "test-utils"))]
    keychain: Option<Arc<Keychain>>,
}

/// Reset transfers its unlocked sessions into the replacement installation.
enum CustodyChoice<C, T> {
    Configured(C),
    Session(Arc<KeySession<T>>),
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
            keys: CustodyChoice::Configured(KeyCustody::Keyring),
            identity: CustodyChoice::Configured(IdentityCustody::Keyring),
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
    /// Replace OS-random ids in tests (§20.2).
    #[cfg(any(test, feature = "test-utils"))]
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
    pub async fn authenticate(&mut self, access: OAuthAccess) -> Result<(), OAuthError> {
        let flow = self
            .oauth_flow()
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        self.authentication = Some(coven_sync::SessionSignIn::new(tokens, self.oauth.clone()));
        Ok(())
    }

    fn oauth_flow(&self) -> Option<OAuthFlow> {
        Some(OAuthFlow::new(self.oauth.clone()?, self.presenter.clone()?))
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
        self.keys = CustodyChoice::Configured(custody);
        self
    }
    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(mut self, custody: IdentityCustody) -> Self {
        self.identity = CustodyChoice::Configured(custody);
        self
    }

    /// Create and open a store, initializing its member in the chosen custody
    /// before publication. Cancellation does not interrupt creation or rollback.
    pub async fn create_store(self, name: &str) -> Result<CovenHandle, StoreCreationError> {
        let name = name.to_owned();
        crate::coven::completion(tokio::spawn(creation::create(self, name))).await
    }

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening always migrates coven's tables before the app's schema, then
    /// unlocks both custody sessions before resuming unfinished operations and
    /// committed file work. Empty custody is allowed; an unlock failure prevents
    /// opening. No sync loop starts, and local database calls need no key material.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle> {
        self.open_graph(store).await?.open().await
    }

    /// Replace this installation with a new device loaded from storage (§10,
    /// §19.2), whenever the app chooses: after sync found another copy using
    /// this device's id, a damaged database, or a device entry `open` found
    /// missing. Uses saved settings, credentials and member keys; nothing
    /// unsent is salvaged.
    /// An interrupted call resumes with the same fresh id. The returned
    /// handle's `device_reset()` names the old and new ids.
    pub async fn reset_device(
        self,
        store: StoreId,
        device_name: &str,
    ) -> Result<CovenHandle, BootstrapError> {
        bootstrap::reset_device(self, store, device_name).await
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
        // A read-only handle cannot persist a cooldown; it holds its own requests.
        let waits = Arc::new(coven_sync::RetryWaits::in_memory(clock.clone()));
        let connector = self.connector();
        let limits = self.limits;
        let database = self.database(directory.clone())?;
        let database = database.open_read_only().await?;
        if let Some(identity) = database.store_state().await?.store {
            identity
                .check_settings(&directory.settings().map_err(CovenError::from)?)
                .map_err(CovenError::from)?;
        }
        let settings = coven_storage::StorageSettings::new(directory.clone());
        let oauth = self.oauth.clone();
        let (credentials, data) = crate::coven::blocking(move || {
            let keychain = Arc::new(StoreKeychain::new(self.keychain()?, store));
            let credentials = Arc::new(coven_sync::DeviceCredentials::new(keychain, oauth));
            let data = credentials.connection(&settings)?;
            Ok::<_, SyncError>((credentials, data))
        })
        .await?;
        let storage: Option<Arc<dyn Storage>> = match data {
            Some(data) => Some(
                connector
                    .connect(
                        data.location,
                        coven_storage::ProviderAuth::new(data.credentials, credentials),
                        Arc::new(crate::storage::NoPublications),
                        waits.clone(),
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
            Arc::new(coven_sync::SyncState::new()),
            waits,
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
            .migration_operation(coven_sync::SequenceSync::migration_operation)
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

    async fn open_graph(self, store: StoreId) -> CovenResult<OpeningStore> {
        let directory = self.layout.store_dir(&store);
        let database = self.database(directory.clone())?;
        let lock = directory.lock_for_open().await?;
        let closing = lock.closing_guard();
        let result = crate::coven::blocking(move || {
            let mut builder = self;
            let settings = lock.settings()?;
            let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, settings.id));
            // An interrupted reset_device resumes only through reset_device (§10).
            if directory.unfinished_device_reset()?.is_some() {
                return Err(StoreLockError::BootstrapPending(settings.id).into());
            }
            // A copy restored without its non-backed-up custody id, or a
            // keychain that no longer names it, changes nothing (§10).
            if keychain.device_id()? != Some(settings.device_id) {
                let identity = Self::make_identity(
                    std::mem::replace(
                        &mut builder.identity,
                        CustodyChoice::Configured(IdentityCustody::InMemory),
                    ),
                    &directory,
                    settings.id,
                    keychain,
                );
                let store = settings.id;
                // Without member keys a connected store can only be restored
                // with its code; reset_device reads them from custody.
                let connected = coven_storage::StorageSettings::new(directory.clone())
                    .read()
                    .map_err(|error| SettingsError::Corrupt(Box::new(error)))?
                    .is_some();
                return Err(match identity?.read()? {
                    None if connected => CovenError::MemberKeysMissing { store },
                    _ => CovenError::DeviceKeyMissing { store },
                });
            }
            Ok(OpeningStore {
                database,
                lock,
                builder,
                directory,
                keychain,
            })
        })
        .await;
        closing.disarm();
        result
    }

    fn owners(
        mut self,
        directory: StoreDir,
        keychain: Arc<StoreKeychain>,
    ) -> CovenResult<OpeningOwners> {
        let oauth = self.oauth_flow();
        let credentials = Arc::new(coven_sync::DeviceCredentials::new(
            keychain.clone(),
            self.oauth.clone(),
        ));
        let connector = self.connector();
        let settings = directory.settings()?;
        let keys = Self::make_keys(self.keys, &directory, settings.id, keychain.clone())?;
        let identity =
            Self::make_identity(self.identity, &directory, settings.id, keychain.clone())?;
        let state = Arc::new(coven_sync::SyncState::new());
        Ok(OpeningOwners {
            directory,
            custody: StoreCustody::new(keys.clone(), identity.clone(), keychain.clone()),
            keychain,
            credentials,
            connector,
            oauth,
            clients: self.oauth,
            authentication: self.authentication,
            limits: self.limits,
            device: settings.device_id,
            initial_name: settings.name,
            keys,
            identity,
            publications: Arc::new(crate::storage::Publications::new(state.clone())),
            state,
            clock: self.clock,
            ids: self.ids,
            storage: None,
        })
    }

    fn make_identity(
        custody: CustodyChoice<IdentityCustody, MemberIdentity>,
        directory: &StoreDir,
        id: StoreId,
        keychain: Arc<StoreKeychain>,
    ) -> Result<Arc<KeySession<MemberIdentity>>, KeyError> {
        let custody = match custody {
            CustodyChoice::Session(session) => return Ok(session),
            CustodyChoice::Configured(custody) => custody,
        };
        let custody: Arc<dyn MemberKeyCustody> = match custody {
            IdentityCustody::Keyring => Arc::new(KeyringCustody::new(keychain.clone())),
            IdentityCustody::Passphrase(secret) => Arc::new(PassphraseCustody::new(
                secret,
                directory.owned_file(StoreFile::MemberKeys),
                id,
            )),
            IdentityCustody::InMemory => Arc::new(InMemoryCustody::empty()),
            IdentityCustody::Custom(keys) => keys,
        };
        Ok(Arc::new(KeySession::member(custody)?))
    }

    fn make_keys(
        custody: CustodyChoice<KeyCustody, StoreKeyring>,
        directory: &StoreDir,
        id: StoreId,
        keychain: Arc<StoreKeychain>,
    ) -> Result<Arc<KeySession<StoreKeyring>>, KeyError> {
        let custody = match custody {
            CustodyChoice::Session(session) => return Ok(session),
            CustodyChoice::Configured(custody) => custody,
        };
        let custody: Arc<dyn StoreKeyCustody> = match custody {
            KeyCustody::Keyring => Arc::new(KeyringCustody::new(keychain)),
            KeyCustody::Passphrase(secret) => Arc::new(PassphraseCustody::new(
                secret,
                directory.owned_file(StoreFile::StoreKeys),
                id,
            )),
            KeyCustody::InMemory => Arc::new(InMemoryCustody::empty()),
            KeyCustody::Custom(keys) => keys,
        };
        Ok(Arc::new(KeySession::store(custody)?))
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
    builder: CovenBuilder,
    directory: StoreDir,
    keychain: Arc<StoreKeychain>,
}

struct OpeningOwners {
    initial_name: String,
    limits: TransferLimits,
    keychain: Arc<StoreKeychain>,
    credentials: Arc<coven_sync::DeviceCredentials>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    clients: Option<OAuthClients>,
    authentication: Option<Arc<coven_sync::SessionSignIn>>,
    device: DeviceId,
    directory: StoreDir,
    custody: StoreCustody,
    keys: Arc<KeySession<StoreKeyring>>,
    identity: Arc<KeySession<MemberIdentity>>,
    /// This installation's sync state, shared by the handle's transfers.
    state: Arc<coven_sync::SyncState>,
    /// The gate of every connection this handle makes, around `state`.
    publications: Arc<crate::storage::Publications>,
    clock: ClockRef,
    ids: IdSourceRef,
    /// A loaded installation's connection, made through `publications`.
    storage: Option<Arc<coven_storage::StorageConnection>>,
}

impl OpeningStore {
    async fn open(self) -> CovenResult<CovenHandle> {
        let database = self.database.open_locked(self.lock).await?;
        if let Some(identity) = database.store_state().await?.store {
            identity.check_settings(&self.directory.settings()?)?;
        }
        let waits = Arc::new(
            coven_sync::RetryWaits::open(database.clone(), self.builder.clock.clone()).await?,
        );
        let owners =
            crate::coven::blocking(move || self.builder.owners(self.directory, self.keychain))
                .await?;
        owners
            .handle(database, waits)
            .await
            .map_err(|error| CovenError::Database(DbError::SyncStream(Box::new(error))))
    }
}

impl OpeningOwners {
    async fn handle(
        self,
        database: coven_database::Database,
        waits: Arc<coven_sync::RetryWaits>,
    ) -> Result<CovenHandle, SyncError> {
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            self.directory.clone(),
            self.storage
                .clone()
                .map(|storage| storage as Arc<dyn coven_storage::Storage>),
            self.state,
            waits.clone(),
            self.ids.clone(),
            self.limits,
        );
        let storage = crate::storage::StorageConnections::new(
            database.clone(),
            files.clone(),
            self.identity,
            self.credentials,
            self.publications,
            self.directory.clone(),
            waits,
            self.initial_name,
            self.keys,
            self.connector,
            self.oauth,
            self.clients,
            self.authentication,
            self.storage,
            self.device,
            self.ids,
            self.clock,
        )
        .await;
        let storage = match storage {
            Ok(storage) => Arc::new(storage),
            Err(error) => {
                files.close().await;
                return Err(error);
            }
        };
        Ok(CovenHandle::new(
            database,
            self.custody,
            files,
            storage,
            self.directory,
            self.keychain,
            self.device,
        ))
    }
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;

#[path = "builder_creation.rs"]
mod creation;

#[path = "bootstrap.rs"]
mod bootstrap;
pub use bootstrap::{
    join_with_invite, restore_from_code, restore_from_keychain, BootstrapError, BootstrapStatus,
};
