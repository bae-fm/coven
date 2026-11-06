//! Compose database, custody, operation and file owners at the store boundary.

use crate::*;
use coven_crypto::custody::{
    Custody, CustodyChoice, InMemoryCustody, KeyMaterial, KeySession, Keychain, KeyringCustody,
    PassphraseCustody, PersonEntries, StoreCustody, StoreKeychain,
};
use coven_database::{DatabaseBuilder, OpenStep};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::StoreFile;
use coven_foundation::progress::Progress;
#[cfg(test)]
use coven_foundation::progress::RecordingClock;
use coven_storage::{
    providers::{OAuthExchange, OAuthFlow, StorageConnector},
    Storage,
};
use std::sync::Arc;

/// The choices for creating or opening a store (E1).
#[cfg_attr(
    not(feature = "test-utils"),
    doc = "\nProduction builders cannot replace the random id source.\n```compile_fail\nuse coven::{Coven, UuidIds};\nuse std::sync::Arc;\nlet coven = Coven::new(\"stores\".into(), \"com.example.notes\", Arc::new(coven::task::LoggedBugs)).unwrap();\nlet _ = coven.builder().id_source(Arc::new(UuidIds));\n```"
)]
pub struct CovenBuilder {
    tables: Option<Vec<SyncedTable>>,
    namespaces: Vec<coven_database::Namespace>,
    migrations: Option<Vec<Migration>>,
    directories: coven_foundation::files::StoreDirectories,
    keychain: Arc<Keychain>,
    ids: IdSourceRef,
    clock: ClockRef,
    connector: Option<Arc<dyn StorageConnector>>,
    transfers: Arc<coven_storage::TransferBudget>,
    /// Where the store's watch hears about the device's network (§6.7).
    network: coven_foundation::network_watch::NetworkSource,
    oauth: Option<OAuthClients>,
    presenter: Option<Arc<dyn OAuthPresenter>>,
    /// The latest sign-in, held for this session until bootstrap or setup
    /// publishes it, or a join keeps it on the device beside its saved code.
    authentication: Option<Arc<coven_sync::SessionSignIn>>,
    keys: SessionSource<StoreKeyring>,
    identity: SessionSource<MemberIdentity>,
    sqlite: SqliteSettings,
    sql_functions: coven_database::SqlFunctions,
    /// Stop a replacement right after its swap, as if the process ended there.
    #[cfg(test)]
    interrupt_after_swap: bool,
    /// Fail the insert of the device-reset notice a reset commits, as a full
    /// disk would.
    #[cfg(test)]
    fail_device_reset_notice: bool,
    /// Stop a replacement right after its fresh device registered, before
    /// its swap, as if the process ended there.
    #[cfg(test)]
    interrupt_after_registering: bool,
    /// Stop the next commit of a new installation's custody right after it
    /// writes the device id, as if the process ended there.
    #[cfg(test)]
    interrupt_commit: bool,
    /// Records every value a bootstrap through this builder reports as it
    /// loads; also the builder's clock.
    #[cfg(test)]
    loading_recorder: Option<Arc<RecordingClock<coven_database::LoadStep>>>,
}

/// What a store's owners continue from as its handle is made.
enum Continuing {
    /// The store's own files: a new sequence state, no connection yet, and
    /// device credentials over this installation's keychain.
    Fresh,
    /// A bootstrap that loaded the store from storage: the sequence state
    /// and connection it loaded through, and, for a join, the device
    /// credentials its sign-in is kept in (§13.2).
    Loaded {
        state: Arc<coven_sync::SequenceState>,
        storage: Arc<coven_storage::StorageConnection>,
        credentials: Option<Arc<coven_sync::DeviceCredentials>>,
    },
}

/// What a builder makes a key kind's session from: the configured choice, or
/// the unlocked session a reset or restore transfers into the replacement.
enum SessionSource<T: KeyMaterial> {
    Configured(CustodyChoice<T>),
    Session(Arc<KeySession<T>>),
}

impl CovenBuilder {
    pub(crate) fn new(
        directories: coven_foundation::files::StoreDirectories,
        keychain: Arc<Keychain>,
        network: coven_foundation::network_watch::NetworkSource,
    ) -> Self {
        let ids: IdSourceRef = Arc::new(UuidIds);
        let clock: ClockRef = Arc::new(SystemClock);
        Self {
            tables: None,
            namespaces: Vec::new(),
            migrations: None,
            directories,
            keychain,
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
            network,
            keys: SessionSource::Configured(CustodyChoice::Keyring),
            identity: SessionSource::Configured(CustodyChoice::Keyring),
            sqlite: SqliteSettings::default(),
            sql_functions: coven_database::SqlFunctions::default(),
            #[cfg(test)]
            interrupt_after_swap: false,
            #[cfg(test)]
            fail_device_reset_notice: false,
            #[cfg(test)]
            interrupt_after_registering: false,
            #[cfg(test)]
            interrupt_commit: false,
            #[cfg(test)]
            loading_recorder: None,
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
        let exchange = self.oauth_exchange();
        let flow = Self::oauth_flow(exchange.clone(), self.presenter.clone())
            .ok_or(OAuthError::Unavailable(access.provider()))?;
        let tokens = flow.authenticate(access).await?;
        self.authentication = Some(coven_sync::SessionSignIn::new(tokens, exchange));
        Ok(())
    }

    /// The app's sign-in clients, sending their token requests with this
    /// builder's clock and a client of their own.
    fn oauth_exchange(&self) -> Option<OAuthExchange> {
        Some(OAuthExchange::new(self.oauth.clone()?, self.clock.clone()))
    }

    /// Whether the app configured sign-in: its clients and its presenter.
    fn signs_in(&self) -> bool {
        self.oauth.is_some() && self.presenter.is_some()
    }

    fn oauth_flow(
        exchange: Option<OAuthExchange>,
        presenter: Option<Arc<dyn OAuthPresenter>>,
    ) -> Option<OAuthFlow> {
        Some(OAuthFlow::new(exchange?, presenter?))
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
    /// app's own `Custody<StoreKeyring>`.
    pub fn key_custody(mut self, custody: CustodyChoice<StoreKeyring>) -> Self {
        self.keys = SessionSource::Configured(custody);
        self
    }
    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `Custody<MemberIdentity>` as the last.
    pub fn identity_custody(mut self, custody: CustodyChoice<MemberIdentity>) -> Self {
        self.identity = SessionSource::Configured(custody);
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

    /// Apple's protection class for the store's files: when they can be read
    /// and written against whether the device is locked (E1). Creating,
    /// restoring, joining or replacing a store gives each new installation's
    /// directory the class before anything is made there. A writable open,
    /// a resumed restore or join, and a replacement give it to every file
    /// already there, unless they were last given this class. On a volume
    /// that keeps no classes each of them fails before any file changes.
    /// Unset, coven leaves every class as it is, and new files take their
    /// folder's.
    #[cfg(target_vendor = "apple")]
    pub fn file_protection(mut self, protection: FileProtection) -> Self {
        self.directories = self.directories.file_protection(protection);
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
        let bugs = self.directories.bugs().clone();
        crate::task::spawn_completing(
            "create the store",
            bugs,
            creation::create(self, name, coven_foundation::files::BootstrapStore::publish),
        )
        .await?
    }

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening always migrates coven's tables before the app's schema, then
    /// unlocks both custody sessions before resuming unfinished operations and
    /// committed file work. Empty custody is allowed; an unlock failure prevents
    /// opening. No sync loop starts, and local database calls need no key material.
    /// `on_progress` is called on this task with each step the open runs and
    /// how much of it is done, at most a few times a second (§4).
    pub async fn open(
        self,
        store: StoreId,
        on_progress: impl Fn(Progress<OpenStep>),
    ) -> CovenResult<CovenHandle> {
        // Its work rides on the heap, as every bootstrap's does, so an app's
        // future holding this call stays small.
        Box::pin(async move {
            let opening = self.open_graph(store, Opening::ForUse).await?;
            let progress = opening.database.subscribe_progress();
            progress.forward(opening.open(), on_progress).await
        })
        .await
    }

    /// Finishes this device's recorded deletion of `store`, which
    /// `CovenHandle::delete_store` began and couldn't finish: it opens the
    /// store as `open` does, then goes on as the deletion would, with the
    /// choice for unsent work it recorded. Before the device's removal is
    /// journaled, a refusal or failure removes the record and leaves the
    /// store as it was; from the journal on, a failure is
    /// `StoreDeletionError::Unfinished` and the deletion stays recorded
    /// (§11.6, E1). A store with no deletion recorded fails with
    /// `StoreDeletionError::NoUnfinishedDeletion`.
    pub async fn resume_deletion(self, store: StoreId) -> Result<(), StoreDeletionError> {
        Box::pin(async move {
            let directory = self.directories.store_dir(&store);
            let bugs = directory.bugs().clone();
            let unsent =
                crate::task::blocking_completing("read the deletion record", bugs, move || {
                    directory.unfinished_deletion()
                })
                .await??
                .ok_or(StoreDeletionError::NoUnfinishedDeletion(store))?;
            let opening = self
                .open_graph(store, Opening::ToDelete)
                .await
                .map_err(StoreDeletionError::Open)?;
            let handle = opening.open().await.map_err(StoreDeletionError::Open)?;
            handle.leave(unsent).await
        })
        .await
    }

    /// Replace this installation with a new device loaded from storage
    /// (§19.3), whenever the app chooses: after sync found another copy using
    /// this device's id, a damaged database, or a device entry `open` found
    /// missing. Uses saved settings, credentials and member keys; nothing
    /// unsent is salvaged. The new device loads beside the old copy, which
    /// moves aside only once loading is complete; saved member keys that
    /// history has replaced fail with `MemberKeysReplaced` and change nothing.
    /// Just before the fresh device registers, the reset records itself and
    /// only goes forward: the store refuses opens with
    /// `StoreLockError::ReplacementUnfinished` until this call, or
    /// `resume_replacement`, finishes it. An interrupted call resumes with the
    /// same fresh id. A replacement `restore_device` began is refused with
    /// `StoreLockError::ReplacementUnfinished`, changing nothing. The returned
    /// handle's `device_reset()` names the old and new ids. `on_status` hears
    /// how far the new device has come, as the other bootstrap calls report
    /// it.
    pub async fn reset_device(
        self,
        store: StoreId,
        device_name: &str,
        on_status: impl Fn(BootstrapStatus),
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::reset_device(self, store, device_name, on_status)).await
    }

    /// Restore this device's installation of `store` from the person's
    /// current restore code, for a device whose member keys were replaced
    /// (§13.1). The code must name this store, and its pair must be current,
    /// checked against history before the store or keychain changes. The new
    /// device loads beside the old copy, which stays openable until the
    /// restore records itself, just before its fresh device registers; from
    /// then on the store refuses opens and the restore only goes forward,
    /// and once registered it swaps in and the old copy is removed. Cancelling
    /// past the record waits for the registration, then fails with
    /// `RestoreRegistered`. An interrupted call is finished by retrying it, or
    /// by `resume_replacement`; a replacement `reset_device` began is refused
    /// with `StoreLockError::ReplacementUnfinished`, changing nothing. A
    /// device whose custody and applied history already hold the code's pair
    /// just opens. S3 requires the separately held key, as
    /// `restore_from_code` does. `prepare` is the app's step on the new
    /// installation: one bulk write (`CovenHandle::write_bulk`) of its
    /// device-local tables, run after loading and before the record, so what
    /// it writes, such as preferences read from the old copy before the call,
    /// takes the old copy's place with the new installation or not at all. A
    /// call that finds the replacement recorded, or the pair already
    /// installed, doesn't run it.
    pub async fn restore_device(
        self,
        store: StoreId,
        code: &str,
        s3_credentials: Option<S3Credentials>,
        device_name: &str,
        prepare: impl FnOnce(BulkSqlContext<'_>) -> CovenResult<()> + Send + 'static,
        on_status: impl Fn(BootstrapStatus),
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::restore_device(
            self,
            store,
            code,
            s3_credentials,
            device_name,
            Box::new(prepare),
            on_status,
            cancel,
        ))
        .await
    }

    /// Finishes this device's replacement of `store` past its record, as the
    /// call that began it would: `reset_device` or `restore_device`, from
    /// what the replacement kept on the device, with neither the code nor a
    /// new sign-in (§19.3). On Dropbox and OneDrive it connects through the
    /// kept sign-in, or a sign-in this builder holds once it grants the
    /// location's access. A store with no replacement under way fails with
    /// `BootstrapError::NoUnfinishedReplacement`.
    pub async fn resume_replacement(
        self,
        store: StoreId,
        on_status: impl Fn(BootstrapStatus),
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::resume_replacement(self, store, on_status, cancel)).await
    }

    /// The joins under way on this device, ordered by store id (§13.2). A
    /// saved code coven can't read fails the call; it is never skipped. An
    /// unfinished installation that keeps neither a saved code nor a
    /// restore's record, and that no call holds, is deleted: a crash left it
    /// before its call wrote to storage (§13.4).
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

    /// The restores under way on this device, ordered by store id: each
    /// restore that recorded itself before registering the device and has
    /// not ended, as `decode_code_info` reads its code (§13.1). A record coven
    /// can't read fails the call; it is never skipped. An unfinished
    /// installation that keeps neither a record nor a join's saved code, and
    /// that no call holds, is deleted, as `unfinished_joins` does (§13.4).
    pub async fn unfinished_restores(&self) -> Result<Vec<CodeMetadata>, BootstrapError> {
        bootstrap::unfinished_restores(self).await
    }

    /// Continues an unfinished restore from its record, as `restore_from_code`
    /// does: the code and device name it recorded, and the S3 key or sign-in
    /// it kept on the device. On Dropbox and OneDrive a sign-in this builder
    /// holds replaces the kept one once it is shown to grant the location's
    /// access; with neither, or with a kept sign-in the provider refuses even
    /// after its one refresh, it fails with `OAuthError::Reauthorize` and the
    /// restore stays unfinished. Cancelling ends it as `restore_from_code`'s
    /// cancel does. A store with no record fails with
    /// `BootstrapError::NoUnfinishedRestore`.
    pub async fn resume_restore(
        self,
        store: StoreId,
        on_status: impl Fn(BootstrapStatus),
        cancel: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<CovenHandle, BootstrapError> {
        Box::pin(bootstrap::resume_restore(self, store, on_status, cancel)).await
    }

    fn database(&self, directory: StoreDir) -> CovenResult<DatabaseBuilder> {
        let tables = self
            .tables
            .clone()
            .ok_or(CovenError::MissingConfiguration { field: "synced_tables" })?;
        let migrations = self
            .migrations
            .clone()
            .ok_or(CovenError::MissingConfiguration { field: "migrations" })?;
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
        Ok(self.keychain.clone())
    }

    async fn open_graph(self, store: StoreId, opening: Opening) -> CovenResult<OpeningStore> {
        let directory = self.directories.store_dir(&store);
        let database = self.database(directory.clone())?;
        let lock = directory.lock_for_open().await?;
        let closing = lock.closing_guard();
        let bugs = directory.bugs().clone();
        let result =
            crate::task::blocking_completing("open the store's custody", bugs, move || {
                let mut builder = self;
                let settings = lock.settings()?;
                let keychain = Arc::new(StoreKeychain::new(builder.keychain()?, settings.id));
                // An interrupted replacement resumes only through the call that
                // began it (§19.3).
                refuse_unfinished_replacement(&directory, settings.id)?;
                // A store this device is leaving opens only to finish that.
                if opening == Opening::ForUse {
                    refuse_unfinished_deletion(&directory, settings.id)?;
                }
                // A copy restored without its non-backed-up custody id, or a
                // keychain that no longer names it, changes nothing (§19.3).
                if keychain.device_id()? != Some(settings.device_id) {
                    let identity = Self::make_session(
                        std::mem::replace(
                            &mut builder.identity,
                            SessionSource::Configured(CustodyChoice::InMemory),
                        ),
                        &directory,
                        settings.id,
                        keychain,
                    );
                    let store = settings.id;
                    // Without member keys a connected store can only be restored
                    // with its code; reset_device reads them from custody.
                    let connected = coven_storage::recorded_config(&directory)
                        .map_err(|error| SettingsError::Corrupt(Box::new(error)))?
                        .is_some();
                    return Err(match identity?.read()? {
                        None if connected => CovenError::MemberKeysMissing { store },
                        _ => CovenError::DeviceKeyMissing { store },
                    });
                }
                // Before the database opens, so its WAL and shared-memory files
                // are made with the class (E1).
                coven_foundation::files::protect_files(&directory, &lock).map_err(|error| {
                    match error {
                        coven_foundation::files::FileError::DeviceLocked { path } => {
                            CovenError::DeviceLocked { path }
                        }
                        error => DbError::from(error).into(),
                    }
                })?;
                Ok(OpeningStore { database, lock, builder, directory, keychain })
            })
            .await
            .unwrap_or_else(|shutdown| Err(shutdown.into()));
        closing.disarm();
        result
    }

    fn owners(
        mut self,
        directory: StoreDir,
        keychain: Arc<StoreKeychain>,
        continuing: Continuing,
    ) -> CovenResult<OpeningOwners> {
        let clients = self.oauth_exchange();
        let oauth = Self::oauth_flow(clients.clone(), self.presenter.clone());
        let settings = directory.settings()?;
        let person = Arc::new(PersonEntries::new(self.keychain()?, settings.id));
        let (state, storage, credentials) = match continuing {
            Continuing::Fresh => (Arc::new(coven_sync::SequenceState::new()), None, None),
            Continuing::Loaded { state, storage, credentials } => {
                (state, Some(storage), credentials)
            }
        };
        // A restore keeps no sign-in in device credentials: the absence is
        // designed, not a default over a failure.
        let credentials = credentials.unwrap_or_else(|| {
            Arc::new(coven_sync::DeviceCredentials::new(
                keychain.clone(),
                person.clone(),
                directory.clone(),
                clients.clone(),
            ))
        });
        let connector = self.connector();
        let keys = Self::make_session(self.keys, &directory, settings.id, keychain.clone())?;
        let identity =
            Self::make_session(self.identity, &directory, settings.id, keychain.clone())?;
        let network = coven_foundation::network_watch::NetworkWatch::start(
            &self.network,
            self.directories.bugs(),
        )
        .map_err(CovenError::NetworkWatch)?;
        Ok(OpeningOwners {
            directory,
            custody: StoreCustody::new(keys.clone(), identity.clone(), keychain.clone()),
            keychain,
            person,
            credentials,
            connector,
            oauth,
            clients,
            authentication: self.authentication,
            transfers: self.transfers,
            network,
            device: settings.device_id,
            keys,
            identity,
            state,
            clock: self.clock,
            ids: self.ids,
            storage,
            #[cfg(test)]
            interrupt_commit: self.interrupt_commit,
        })
    }

    fn make_session<T: KeyMaterial>(
        chosen: SessionSource<T>,
        directory: &StoreDir,
        id: StoreId,
        keychain: Arc<StoreKeychain>,
    ) -> Result<Arc<KeySession<T>>, KeyError> {
        let custody = match chosen {
            SessionSource::Session(session) => return Ok(session),
            SessionSource::Configured(custody) => custody,
        };
        let custody: Arc<dyn Custody<T>> = match custody {
            CustodyChoice::Keyring => Arc::new(KeyringCustody::new(keychain)),
            CustodyChoice::Passphrase(secret) => {
                Arc::new(PassphraseCustody::new(secret, directory.owned_file(T::FILE), id))
            }
            CustodyChoice::InMemory => Arc::new(InMemoryCustody::empty()),
            CustodyChoice::Custom(custody) => custody,
        };
        Ok(Arc::new(KeySession::open(custody)?))
    }

    /// Make the next replacement stop right after its swap, undoing nothing.
    #[cfg(test)]
    pub(crate) fn interrupt_after_swap(mut self) -> Self {
        self.interrupt_after_swap = true;
        self
    }

    /// Make the insert of the next reset's device-reset notice fail.
    #[cfg(test)]
    pub(crate) fn fail_device_reset_notice(mut self) -> Self {
        self.fail_device_reset_notice = true;
        self
    }

    /// Make the next replacement stop right after its fresh device
    /// registered, before its swap, undoing nothing.
    #[cfg(test)]
    pub(crate) fn interrupt_after_registering(mut self) -> Self {
        self.interrupt_after_registering = true;
        self
    }

    /// Make the next commit of a new installation's custody stop right after
    /// it writes the device id, undoing nothing.
    #[cfg(test)]
    pub(crate) fn interrupt_commit(mut self) -> Self {
        self.interrupt_commit = true;
        self
    }

    /// Keep time by `recorder`, which records every value a bootstrap
    /// through this builder reports as it loads, values a host reading the
    /// loading watch could miss.
    #[cfg(test)]
    pub(crate) fn record_loading(
        mut self,
        recorder: Arc<RecordingClock<coven_database::LoadStep>>,
    ) -> Self {
        self.clock = recorder.clone();
        self.loading_recorder = Some(recorder);
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
    /// The store's watch of the device's network, from opening to closing.
    network: coven_foundation::network_watch::NetworkWatch,
    keychain: Arc<StoreKeychain>,
    /// The person's entries, deleted when they leave the store (§14.1).
    person: Arc<PersonEntries>,
    credentials: Arc<coven_sync::DeviceCredentials>,
    connector: Arc<dyn StorageConnector>,
    oauth: Option<OAuthFlow>,
    clients: Option<OAuthExchange>,
    authentication: Option<Arc<coven_sync::SessionSignIn>>,
    device: DeviceId,
    directory: StoreDir,
    custody: StoreCustody,
    keys: Arc<KeySession<StoreKeyring>>,
    identity: Arc<KeySession<MemberIdentity>>,
    /// This installation's sequence state, shared by the handle's transfers.
    state: Arc<coven_sync::SequenceState>,
    clock: ClockRef,
    ids: IdSourceRef,
    /// A loaded installation's connection, made through `state`.
    storage: Option<Arc<coven_storage::StorageConnection>>,
    /// Stop the commit right after it writes the device id.
    #[cfg(test)]
    interrupt_commit: bool,
}

impl OpeningStore {
    async fn open(self) -> CovenResult<CovenHandle> {
        let database = self.database.open_locked(self.lock).await?;
        let state = database.store_state().await?;
        if let Some(identity) = &state.store {
            check_store_settings(identity, &self.directory)?;
        }
        let waits = Arc::new(
            coven_sync::RetryWaits::open(database.clone(), self.builder.clock.clone()).await?,
        );
        let bugs = database.bugs().clone();
        let owners = crate::task::blocking_completing("open the store's owners", bugs, move || {
            let owners = self.builder.owners(self.directory, self.keychain, Continuing::Fresh)?;
            // The saved restore code's name catches up with this store's
            // when the store's is newer, and an unreadable entry is replaced
            // with this device's current code (§13.1). A store never set up
            // has no saved code.
            if let Some(identity) = &state.store {
                coven_sync::follow_name(
                    &owners.person,
                    &coven_sync::SavedName::of(identity),
                    || {
                        let location = coven_storage::recorded_config(&owners.directory)
                            .map_err(|error| CovenError::from(SyncError::from(error)))?;
                        coven_sync::replacement_code(
                            &state,
                            owners.identity.read()?.as_ref(),
                            location.as_ref(),
                        )
                        .map_err(CovenError::from)
                    },
                )?;
                // Where each write chooses the cloud backup, as Block Store's
                // does, the entries are stored again, so a screen lock added
                // since they were written lets them reach it (§13.1).
                owners.person.store_again()?;
            }
            Ok::<_, CovenError>(owners)
        })
        .await??;
        let location = owners.credentials.location().map_err(CovenError::from)?;
        let configured = location.is_connected();
        // A setup a crash cut is undone, or left for the first connection
        // to finish, before the handle exists (§11.6).
        let finishing =
            Box::pin(crate::storage::settle_opened(&database, &owners.keychain, &owners.keys))
                .await
                .map_err(CovenError::from)?;
        // Boxed, so `open`'s future stays as small as an app's stack needs
        // (`application_api_tests.rs`).
        Box::pin(crate::storage::record_opened(&database, &owners.clock, location, None))
            .await
            .map_err(CovenError::from)?;
        Ok(owners.into_handle(database, waits, configured, finishing))
    }
}

/// Why the store opens.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Opening {
    /// For the app's use.
    ForUse,
    /// To finish this device's recorded deletion of it.
    ToDelete,
}

/// A store this device is deleting and leaving opens only to finish that
/// (E1, §11.6).
fn refuse_unfinished_deletion(directory: &impl StoreDirAccess, store: StoreId) -> CovenResult<()> {
    match directory.unfinished_deletion().map_err(DbError::from)? {
        Some(_) => Err(StoreLockError::DeletionUnfinished(store).into()),
        None => Ok(()),
    }
}

/// An interrupted replacement resumes only through the call that began it
/// (§19.3).
fn refuse_unfinished_replacement(
    directory: &impl StoreDirAccess,
    store: StoreId,
) -> CovenResult<()> {
    match directory.unfinished_replacement()? {
        Some(record) => Err(StoreLockError::ReplacementUnfinished { store, by: record.by }.into()),
        None => Ok(()),
    }
}

/// The store's identity names the store its directory's settings were
/// written for.
fn check_store_settings(
    identity: &coven_database::StoreIdentity,
    directory: &impl StoreDirAccess,
) -> CovenResult<()> {
    Ok(identity.check_settings(&directory.settings()?)?)
}

/// The device-custody credentials of `store`'s installation.
pub(crate) fn device_credentials(
    keychain: Arc<Keychain>,
    store: StoreId,
    directory: StoreDir,
    oauth: Option<OAuthExchange>,
) -> Arc<coven_sync::DeviceCredentials> {
    Arc::new(coven_sync::DeviceCredentials::new(
        Arc::new(StoreKeychain::new(keychain.clone(), store)),
        Arc::new(PersonEntries::new(keychain, store)),
        directory,
        oauth,
    ))
}

impl OpeningOwners {
    /// Construction after publication performs no fallible work and cannot suspend.
    fn into_handle(
        self,
        database: coven_database::Database,
        waits: Arc<coven_sync::RetryWaits>,
        configured: bool,
        setup_finishing: bool,
    ) -> CovenHandle {
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            self.directory.clone(),
            self.storage.clone().map(|storage| storage as Arc<dyn coven_storage::Storage>),
            self.state.clone(),
            waits.clone(),
            self.ids.clone(),
            self.transfers.clone(),
            self.clock.clone(),
            self.network.view(),
        );
        let network =
            coven_sync::NetworkFollow::start(self.network, waits.clone(), database.clone());
        let steps = Arc::new(coven_sync::OperationSteps::new(
            self.device,
            self.identity.clone(),
            self.ids.clone(),
            self.clock.clone(),
            self.directory.clone(),
            waits.clone(),
            self.state.clone(),
        ));
        let codes = coven_sync::RestoreCodes::new(
            self.directory.clone(),
            database.clone(),
            self.identity.clone(),
            self.credentials.clone(),
        );
        let storage = crate::storage::StorageConnections::new(
            database.clone(),
            files.clone(),
            self.identity,
            self.person,
            self.credentials,
            codes.clone(),
            self.state.clone(),
            self.directory.clone(),
            waits.clone(),
            self.transfers,
            self.keys,
            self.connector,
            self.oauth,
            self.clients,
            self.authentication,
            self.storage,
            configured,
            self.keychain.clone(),
            setup_finishing,
            self.device,
            self.ids,
            self.clock,
        );
        CovenHandle::new(
            database,
            self.custody,
            files,
            steps,
            waits,
            self.state,
            codes,
            Arc::new(network),
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

#[path = "builder_read_only.rs"]
mod read_only;

#[path = "bootstrap.rs"]
mod bootstrap;
pub use bootstrap::{
    join_with_invite, restore_from_code, restore_from_saved_code, BootstrapError, BootstrapStatus,
    JoinControl,
};
