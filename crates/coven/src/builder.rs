//! Compose database, custody, operation and file owners at the store boundary.

use crate::*;
use coven_crypto::custody::{
    InMemoryCustody, KeySession, Keychain, KeyringCustody, PassphraseCustody, PersonEntries,
    StoreCustody, StoreKeychain,
};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreDirAccess;
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
    namespaces: Vec<coven_database::Namespace>,
    migrations: Option<Vec<Migration>>,
    layout: StoreLayout,
    ids: IdSourceRef,
    clock: ClockRef,
    connector: Option<Arc<dyn StorageConnector>>,
    transfers: Arc<coven_storage::TransferBudget>,
    oauth: Option<OAuthClients>,
    presenter: Option<Arc<dyn OAuthPresenter>>,
    /// The latest sign-in, held for this session until bootstrap or setup
    /// publishes it, or a join keeps it on the device beside its saved code.
    authentication: Option<Arc<coven_sync::SessionSignIn>>,
    keys: CustodyChoice<KeyCustody, StoreKeyring>,
    identity: CustodyChoice<IdentityCustody, MemberIdentity>,
    sqlite: SqliteSettings,
    sql_functions: coven_database::SqlFunctions,
    #[cfg(any(test, feature = "test-utils"))]
    keychain: Option<Arc<Keychain>>,
    /// Stop a replacement right after its swap, as if the process ended there.
    #[cfg(test)]
    interrupt_after_swap: bool,
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
            namespaces: Vec::new(),
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
            transfers: Arc::new(coven_storage::TransferBudget::new(TransferLimits::default())),
            keys: CustodyChoice::Configured(KeyCustody::Keyring),
            identity: CustodyChoice::Configured(IdentityCustody::Keyring),
            sqlite: SqliteSettings::default(),
            sql_functions: coven_database::SqlFunctions::default(),
            #[cfg(any(test, feature = "test-utils"))]
            keychain: None,
            #[cfg(test)]
            interrupt_after_swap: false,
        }
    }

    /// The tables that sync (E2). Required.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.tables = Some(tables);
        self
    }
    /// The file namespaces, each with its kind and cache budget (E1). A file
    /// is staged into one of them.
    pub fn namespaces(mut self, namespaces: Vec<coven_database::Namespace>) -> Self {
        self.namespaces = namespaces;
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
    /// Replace OS-random ids in tests (Appendix F.2).
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
    pub fn max_concurrent_uploads(self, n: std::num::NonZeroUsize) -> Self {
        let mut limits = self.transfers.limits();
        limits.uploads = n;
        self.transfers.set_limits(limits);
        self
    }
    /// Limit file downloads running together; defaults to one. The
    /// sequence's own reads don't count against it.
    pub fn max_concurrent_downloads(self, n: std::num::NonZeroUsize) -> Self {
        let mut limits = self.transfers.limits();
        limits.downloads = n;
        self.transfers.set_limits(limits);
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

    /// SQLite settings for every connection a handle opens to the store's
    /// database (§4). Unset ones keep SQLite's defaults, except that
    /// `checkpoint_fullfsync` is on for macOS and iOS; the page size and
    /// auto-vacuum mode apply only when the database is created.
    pub fn sqlite_settings(mut self, settings: SqliteSettings) -> Self {
        self.sqlite = settings;
        self
    }

    /// A collation the app's SQL names with `COLLATE name` (§4), ordering
    /// text by `compare` at `version`, on every connection a handle opens to
    /// the store's database. The order is part of the store's schema:
    /// `compare` must be deterministic, the same on every device and in every
    /// app version that registers this `version`, and a total order. Every
    /// change to the order takes a new version, and a writable open then
    /// rebuilds the indexes in it. A name that is SQLite's own (`BINARY`,
    /// `NOCASE`, `RTRIM`) or given twice fails the open, and a panic in
    /// `compare` ends the process.
    pub fn collation<F>(mut self, name: impl Into<String>, version: u32, compare: F) -> Self
    where
        F: Fn(&str, &str) -> std::cmp::Ordering + Send + Sync + 'static,
    {
        self.sql_functions =
            std::mem::take(&mut self.sql_functions).collation(name, version, compare);
        self
    }

    /// A scalar function the app's SQL calls as `name(...)` with exactly
    /// `arguments` arguments (§4), on every connection a handle opens to the
    /// store's database. `compute` reads the arguments and returns the
    /// result, or an error that fails the statement calling it.
    /// A deterministic function can appear in indexes, CHECKs and generated
    /// columns, and is then part of the store's schema: at its version it
    /// must give the same result for the same arguments on every device and
    /// in every app version, and takes at least one argument. Every change
    /// to its results takes a new version, and a writable open then rebuilds
    /// the indexes that call it. A name SQLite or coven
    /// already has, one with coven's prefix `coven_` or `_coven_`, or one
    /// given twice fails the open, and a panic in `compute` ends the process.
    pub fn function<F, T>(
        mut self,
        name: impl Into<String>,
        arguments: u8,
        determinism: Determinism,
        compute: F,
    ) -> Self
    where
        F: Fn(&FunctionArguments<'_>) -> SqliteResult<T> + Send + Sync + 'static,
        T: Into<types::Value>,
    {
        self.sql_functions =
            std::mem::take(&mut self.sql_functions).scalar(name, arguments, determinism, compute);
        self
    }

    /// Create and open a store, initializing its member in the chosen custody
    /// before publication. Cancellation does not interrupt creation or rollback.
    /// Errors carry no handle; a published store is closed before returning.
    pub async fn create_store(self, name: &str) -> Result<CovenHandle, StoreCreationError> {
        let name = name.to_owned();
        crate::coven::completion(tokio::spawn(creation::create(
            self,
            name,
            coven_foundation::files::BootstrapStore::publish,
        )))
        .await
    }

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening always migrates coven's tables before the app's schema, then
    /// unlocks both custody sessions before resuming unfinished operations and
    /// committed file work. Empty custody is allowed; an unlock failure prevents
    /// opening. No sync loop starts, and local database calls need no key material.
    pub async fn open(self, store: StoreId) -> CovenResult<CovenHandle> {
        self.open_graph(store).await?.open().await
    }

    /// Replace this installation with a new device loaded from storage
    /// (§19.3), whenever the app chooses: after sync found another copy using
    /// this device's id, a damaged database, or a device entry `open` found
    /// missing. Uses saved settings, credentials and member keys; nothing
    /// unsent is salvaged. The new device loads beside the old copy, which
    /// moves aside only once loading is complete; saved member keys that
    /// history has replaced fail with `MemberKeysReplaced` and change nothing.
    /// An interrupted call resumes with the same fresh id. A swap that
    /// `restore_device` began is refused with
    /// `StoreLockError::ReplacementUnfinished`, changing nothing. The returned
    /// handle's `device_reset()` names the old and new ids.
    pub async fn reset_device(
        self,
        store: StoreId,
        device_name: &str,
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::reset_device(self, store, device_name)).await
    }

    /// Restore this device's installation of `store` from the person's
    /// current restore code, for a device whose member keys were replaced
    /// (§13.1). The code must name this store, and its pair must be current,
    /// checked against history before the store or keychain changes. The new
    /// device loads beside the old copy, which stays openable until the load
    /// completes; then it swaps in, and the old copy is removed. An
    /// interrupted call is finished by retrying it; a swap `reset_device`
    /// began is refused with `StoreLockError::ReplacementUnfinished`, changing
    /// nothing. A device whose custody and applied history already hold the
    /// code's pair just opens. S3 requires the separately held key, as
    /// `restore_from_code` does.
    pub async fn restore_device(
        self,
        store: StoreId,
        code: &str,
        s3_credentials: Option<S3Credentials>,
        device_name: &str,
        on_status: impl Fn(BootstrapStatus),
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::restore_device(
            self,
            store,
            code,
            s3_credentials,
            device_name,
            on_status,
            cancel,
        ))
        .await
    }

    /// The joins under way on this device, ordered by store id (§13.2). A
    /// saved code coven can't read fails the call; it is never skipped.
    pub async fn unfinished_joins(&self) -> Result<Vec<InviteInfo>, BootstrapError> {
        bootstrap::unfinished_joins(self).await
    }

    /// Continues an unfinished join from its saved code and device name, as
    /// `join_with_invite` does. On Dropbox and OneDrive it connects through
    /// the sign-in the join keeps on the device, or through a sign-in this
    /// builder holds, which replaces the kept one once it is shown to grant
    /// the location's access. With neither, or with a kept sign-in the
    /// provider refuses even after its one refresh, it fails with
    /// `OAuthError::Reauthorize` and the join stays unfinished (§13.2).
    /// Returns `None` when the request was declined or the invite expired,
    /// after deleting the kept sign-in and the saved code. A store with no
    /// saved code fails with `BootstrapError::NoUnfinishedJoin`.
    pub async fn resume_join(
        self,
        store: StoreId,
        on_status: impl Fn(BootstrapStatus),
        control: &JoinControl,
    ) -> Result<Option<CovenHandle>, BootstrapError> {
        Box::pin(bootstrap::resume_join(self, store, on_status, control)).await
    }

    /// Opens the store for reading only, alongside a handle that has it open.
    /// Its shared lock protects both read connections and local cache metadata.
    /// It runs no migration and refuses a database whose schema is newer,
    /// whose coven tables need migrating, or whose indexes were built with
    /// other versions of the app's collations and functions.
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
        let transfers = self.transfers.clone();
        let database = self.database(directory.clone())?;
        // Before the swap's marker exists, only the record shows that a
        // replacement is moving the files a reader would open (§19.3).
        if let Some(record) = directory
            .unfinished_replacement()
            .map_err(CovenError::from)?
        {
            return Err(CovenError::from(StoreLockError::ReplacementUnfinished {
                store,
                by: record.by,
            })
            .into());
        }
        let database = database.open_read_only().await?;
        if let Some(identity) = database.store_state().await?.store {
            identity
                .check_settings(&directory.settings().map_err(CovenError::from)?)
                .map_err(CovenError::from)?;
        }
        let settings = coven_storage::StorageSettings::new(directory.clone());
        let oauth = self.oauth.clone();
        let (credentials, data) = crate::coven::blocking(move || {
            let keychain = self.keychain()?;
            let credentials = Arc::new(coven_sync::DeviceCredentials::new(
                Arc::new(StoreKeychain::new(keychain.clone(), store)),
                Arc::new(PersonEntries::new(keychain, store)),
                oauth,
            ));
            let data = credentials.connection(&settings)?;
            Ok::<_, SyncError>((credentials, data))
        })
        .await?;
        let storage: Option<Arc<dyn Storage>> = match data {
            Some(data) => {
                let storage = connector
                    .connect(
                        data.location,
                        coven_storage::ProviderAuth::new(data.credentials, credentials),
                        Arc::new(crate::storage::NoPublications),
                        waits.clone(),
                        transfers.clone(),
                        None,
                    )
                    .await
                    .map_err(SyncError::from)?;
                storage.check_bucket(store).await.map_err(SyncError::from)?;
                Some(storage)
            }
            None => None,
        };
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::read_only(database.clone()),
            directory,
            storage,
            Arc::new(coven_sync::SyncState::new()),
            waits,
            ids,
            transfers,
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
            .namespaces(self.namespaces.clone())
            .migrations(migrations)
            .clock(self.clock.clone())
            .id_source(self.ids.clone())
            .sqlite_settings(self.sqlite.clone())
            .sql_functions(self.sql_functions.clone()))
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
            // An interrupted replacement resumes only through the call that
            // began it (§19.3).
            if let Some(record) = directory.unfinished_replacement()? {
                return Err(StoreLockError::ReplacementUnfinished {
                    store: settings.id,
                    by: record.by,
                }
                .into());
            }
            // A copy restored without its non-backed-up custody id, or a
            // keychain that no longer names it, changes nothing (§19.3).
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
        let settings = directory.settings()?;
        let person = Arc::new(PersonEntries::new(self.keychain()?, settings.id));
        let credentials = Arc::new(coven_sync::DeviceCredentials::new(
            keychain.clone(),
            person.clone(),
            self.oauth.clone(),
        ));
        let connector = self.connector();
        let keys = Self::make_keys(self.keys, &directory, settings.id, keychain.clone())?;
        let identity =
            Self::make_identity(self.identity, &directory, settings.id, keychain.clone())?;
        let state = Arc::new(coven_sync::SyncState::new());
        Ok(OpeningOwners {
            directory,
            custody: StoreCustody::new(keys.clone(), identity.clone(), keychain.clone()),
            keychain,
            person,
            credentials,
            connector,
            oauth,
            clients: self.oauth,
            authentication: self.authentication,
            transfers: self.transfers,
            device: settings.device_id,
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

    /// Make the next replacement stop right after its swap, undoing nothing.
    #[cfg(test)]
    pub(crate) fn interrupt_after_swap(mut self) -> Self {
        self.interrupt_after_swap = true;
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
    transfers: Arc<coven_storage::TransferBudget>,
    keychain: Arc<StoreKeychain>,
    /// The person's entries, deleted when they leave the store (§14.1).
    person: Arc<PersonEntries>,
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
        let state = database.store_state().await?;
        if let Some(identity) = &state.store {
            identity.check_settings(&self.directory.settings()?)?;
        }
        let waits = Arc::new(
            coven_sync::RetryWaits::open(database.clone(), self.builder.clock.clone()).await?,
        );
        let owners = crate::coven::blocking(move || {
            let owners = self.builder.owners(self.directory, self.keychain)?;
            // The saved restore code's name catches up with this store's
            // when the store's is newer, and an unreadable entry is replaced
            // with this device's current code (§13.1). A store never set up
            // has no saved code.
            if let Some(identity) = &state.store {
                coven_sync::follow_name(
                    &owners.person,
                    &coven_sync::SavedName::of(identity),
                    || {
                        let location =
                            coven_storage::StorageSettings::new(owners.directory.clone())
                                .read()
                                .map_err(|error| sync_error(error.into()))?;
                        coven_sync::replacement_code(
                            &state,
                            owners.identity.read()?.as_ref(),
                            location.as_ref(),
                        )
                        .map_err(sync_error)
                    },
                )?;
            }
            Ok::<_, CovenError>(owners)
        })
        .await?;
        owners
            .handle(database, waits)
            .map_err(|error| CovenError::Database(DbError::SyncStream(Box::new(error))))
    }
}

/// A sync failure while opening, as the opening call reports it.
fn sync_error(error: SyncError) -> CovenError {
    CovenError::Database(DbError::SyncStream(Box::new(error)))
}

impl OpeningOwners {
    fn handle(
        self,
        database: coven_database::Database,
        waits: Arc<coven_sync::RetryWaits>,
    ) -> Result<CovenHandle, SyncError> {
        let configured = self
            .credentials
            .connection(&coven_storage::StorageSettings::new(self.directory.clone()))?
            .is_some();
        Ok(self.into_handle(database, waits, configured))
    }

    /// Construction after publication performs no fallible work and cannot suspend.
    fn into_handle(
        self,
        database: coven_database::Database,
        waits: Arc<coven_sync::RetryWaits>,
        configured: bool,
    ) -> CovenHandle {
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            self.directory.clone(),
            self.storage
                .clone()
                .map(|storage| storage as Arc<dyn coven_storage::Storage>),
            self.state,
            waits.clone(),
            self.ids.clone(),
            self.transfers.clone(),
        );
        let storage = crate::storage::StorageConnections::new(
            database.clone(),
            files.clone(),
            self.identity,
            self.person,
            self.credentials,
            self.publications,
            self.directory.clone(),
            waits,
            self.transfers,
            self.keys,
            self.connector,
            self.oauth,
            self.clients,
            self.authentication,
            self.storage,
            configured,
            self.device,
            self.ids,
            self.clock,
        );
        CovenHandle::new(
            database,
            self.custody,
            files,
            Arc::new(storage),
            self.directory,
            self.keychain,
            self.device,
        )
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
    join_with_invite, restore_from_code, restore_from_saved_code, BootstrapError, BootstrapStatus,
    JoinControl,
};
