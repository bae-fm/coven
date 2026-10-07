//! Compose database, custody, operation and file owners at the store boundary.

use crate::*;
use coven_crypto::custody::{
    InMemoryCustody, Keychain, KeyringCustody, PassphraseCustody, StoreCustody, StoreKeychain,
    StoreKeys,
};
use coven_database::DatabaseBuilder;
use coven_foundation::files::StoreFile;
use std::sync::Arc;

/// The choices collected before opening a store (§20.1).
pub struct CovenBuilder {
    database: DatabaseBuilder,
    directory: StoreDir,
    ids: IdSourceRef,
    clock: ClockRef,
    storage: Option<Arc<dyn coven_storage::Storage>>,
    keys: KeyCustody,
    identity: IdentityCustody,
    #[cfg(any(test, feature = "test-utils"))]
    keychain: Option<Arc<Keychain>>,
}

impl CovenBuilder {
    pub(crate) fn new(directory: StoreDir) -> Self {
        let ids: IdSourceRef = Arc::new(UuidIds);
        let clock: ClockRef = Arc::new(SystemClock);
        Self {
            database: DatabaseBuilder::new(directory.clone())
                .id_source(ids.clone())
                .clock(clock.clone()),
            directory,
            ids,
            clock,
            storage: None,
            keys: KeyCustody::Keyring,
            identity: IdentityCustody::Keyring,
            #[cfg(any(test, feature = "test-utils"))]
            keychain: None,
        }
    }

    /// The tables that sync (§20.2). Required.
    pub fn synced_tables(mut self, tables: Vec<SyncedTable>) -> Self {
        self.database = self.database.synced_tables(tables);
        self
    }
    /// The app's schema migrations, numbered from 1 with no gaps (§20.13).
    /// Required.
    pub fn migrations(mut self, migrations: Vec<Migration>) -> Self {
        self.database = self.database.migrations(migrations);
        self
    }
    /// Whether opening may migrate coven's own tables to this version of
    /// coven (§17.2). Required by `open`.
    pub fn coven_migration_policy(mut self, policy: CovenMigrationPolicy) -> Self {
        self.database = self.database.coven_migration_policy(policy);
        self
    }
    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(mut self, clock: ClockRef) -> Self {
        self.database = self.database.clock(clock.clone());
        self.clock = clock;
        self
    }
    /// The source of new ids (§21.2). Defaults to `UuidIds`, random UUIDs.
    pub fn id_source(mut self, ids: IdSourceRef) -> Self {
        self.database = self.database.id_source(ids.clone());
        self.ids = ids;
        self
    }
    /// Supply the connected storage capability used by operations and files.
    /// Provider setup and credential custody remain with their existing owners.
    pub fn storage(mut self, storage: Arc<dyn coven_storage::Storage>) -> Self {
        self.storage = Some(storage);
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
    pub async fn open(self) -> CovenResult<CovenHandle> {
        crate::coven::blocking(move || self.open_graph())
            .await?
            .open()
            .await
    }

    /// Opens the store for reading only, alongside a handle that has it open.
    /// Its shared lock protects both read connections and local cache metadata.
    /// It runs no migration and refuses a database whose schema is newer or
    /// whose coven tables need migrating.
    pub async fn open_read_only(self) -> CovenResult<CovenReadHandle> {
        let directory = self.directory.clone();
        let ids = self.ids.clone();
        let clock = self.clock.clone();
        let storage = self.storage.clone();
        let (database, keys) = crate::coven::blocking(move || self.read_graph()).await?;
        let database = database.open_read_only().await?;
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::read_only(database.clone()),
            directory,
            storage,
            clock,
            ids,
        );
        Ok(CovenReadHandle::new(database, keys, files))
    }

    fn open_graph(self) -> CovenResult<OpeningStore> {
        let lock = self.directory.lock_exclusive()?;
        let settings = lock.settings()?;
        #[cfg(any(test, feature = "test-utils"))]
        let keychain = match self.keychain {
            Some(keychain) => keychain,
            None => Keychain::registered()?,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let keychain = Keychain::registered()?;
        let keychain = Arc::new(StoreKeychain::new(keychain, settings.id));
        if keychain.device_id()? != Some(settings.device_id) {
            let device = self.ids.new_device_id();
            lock.set_device_id(device)?;
            keychain.set_device_id(device)?;
        }
        let keys = Self::make_keys(self.keys, &self.directory, settings.id, keychain.clone());
        let identity: Arc<dyn MemberKeyCustody> = match self.identity {
            IdentityCustody::Keyring => Arc::new(KeyringCustody::new(keychain.clone())),
            IdentityCustody::Passphrase(secret) => Arc::new(PassphraseCustody::new(
                secret,
                self.directory.owned_file(StoreFile::MemberKeys),
                settings.id,
            )),
            IdentityCustody::InMemory(keys) => Arc::new(InMemoryCustody::new(keys)),
            IdentityCustody::Custom(keys) => keys,
        };
        Ok(OpeningStore {
            database: self.database,
            directory: self.directory,
            custody: StoreCustody::new(StoreKeys::new(keys.clone()), identity.clone(), keychain),
            keys,
            identity,
            lock,
            clock: self.clock,
            ids: self.ids,
            storage: self.storage,
        })
    }

    fn read_graph(self) -> CovenResult<(DatabaseBuilder, StoreKeys)> {
        #[cfg(any(test, feature = "test-utils"))]
        let keychain = match self.keychain {
            Some(keychain) => keychain,
            None => Keychain::registered()?,
        };
        #[cfg(not(any(test, feature = "test-utils")))]
        let keychain = Keychain::registered()?;
        let keychain = Arc::new(StoreKeychain::new(keychain, self.directory.id()));
        let keys = Self::make_keys(self.keys, &self.directory, self.directory.id(), keychain);
        Ok((self.database, StoreKeys::new(keys)))
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
            KeyCustody::InMemory(keys) => Arc::new(InMemoryCustody::new(keys)),
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
    directory: StoreDir,
    custody: StoreCustody,
    keys: Arc<dyn StoreKeyCustody>,
    identity: Arc<dyn MemberKeyCustody>,
    lock: coven_foundation::files::StoreLock,
    clock: ClockRef,
    ids: IdSourceRef,
    storage: Option<Arc<dyn coven_storage::Storage>>,
}

impl OpeningStore {
    async fn open(self) -> CovenResult<CovenHandle> {
        let database = self.database.open_locked(self.lock).await?;
        let files = coven_sync::Files::new(
            coven_database::FileDatabase::new(database.clone()),
            self.directory,
            self.storage.clone(),
            self.clock.clone(),
            self.ids.clone(),
        );
        let sync = match self.storage {
            Some(storage) => coven_sync::StoreLogSync::new(
                storage,
                database.clone(),
                self.keys,
                self.identity,
                self.clock,
                self.ids.clone(),
            ),
            None => coven_sync::StoreLogSync::disconnected(
                database.clone(),
                self.keys,
                self.identity,
                self.clock,
                self.ids.clone(),
            ),
        };
        let operations = coven_sync::Operations::new(sync);
        Ok(CovenHandle::new(database, self.custody, operations, files))
    }
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;
