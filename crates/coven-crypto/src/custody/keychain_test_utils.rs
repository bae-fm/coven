use super::*;
use crate::custody::block_store::BlockStore;
use crate::custody::block_store_test_utils::MemoryBlockStore;
use crate::custody::person_entries::restore_code_store;
use crate::custody::KeychainError;
use std::collections::BTreeMap;

/// The synced entries iCloud Keychain carries to every device of one person:
/// each in-memory keychain of the person holds a share, which only a keychain
/// makes, its first device's for itself and each other device's from one it
/// already has.
#[derive(Clone)]
pub(super) struct SyncedEntries(Arc<Mutex<SyncedGroups>>);

impl SyncedEntries {
    fn groups(&self) -> std::sync::MutexGuard<'_, SyncedGroups> {
        self.0.lock().expect("in-memory synced keychain entries lock is poisoned")
    }
}

/// The keychain groups an app can read. Apple keeps one copy of an entry per
/// group: a write naming no group adds to the default group, or, when that
/// group already holds the entry, updates every group's copy; a read naming
/// none gives the first copy, and a deletion naming none deletes them all.
#[derive(Default)]
pub(super) struct SyncedGroups {
    default: BTreeMap<String, SecretBytes>,
    another: BTreeMap<String, SecretBytes>,
}

impl SyncedGroups {
    fn read(&self, account: &str) -> Option<SecretBytes> {
        self.copies(account).into_iter().next()
    }

    fn write(&mut self, account: &str, bytes: &[u8]) {
        if self.default.contains_key(account) {
            if let Some(copy) = self.another.get_mut(account) {
                *copy = SecretBytes::new(bytes.to_vec());
            }
        }
        self.default.insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
    }

    fn delete(&mut self, account: &str) {
        self.default.remove(account);
        self.another.remove(account);
    }

    fn copies(&self, account: &str) -> Vec<SecretBytes> {
        [&self.default, &self.another]
            .into_iter()
            .filter_map(|group| group.get(account))
            .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec()))
            .collect()
    }

    fn replace(&mut self, account: &str, bytes: &[u8]) {
        let bytes = SecretBytes::new(bytes.to_vec());
        if self.default.contains_key(account) || !self.another.contains_key(account) {
            self.default.insert(account.to_owned(), bytes);
            self.another.remove(account);
        } else {
            self.another.insert(account.to_owned(), bytes);
        }
    }

    fn all(&self) -> Vec<(String, SecretBytes)> {
        [&self.default, &self.another]
            .into_iter()
            .flatten()
            .map(|(account, bytes)| (account.clone(), SecretBytes::new(bytes.as_bytes().to_vec())))
            .collect()
    }
}

/// Where an in-memory keychain keeps the person's entries.
pub(super) enum MemoryPerson {
    /// iCloud Keychain: shared with every keychain made by `another_device`.
    ICloud(SyncedEntries),
    /// Block Store: this phone's own, given to phones set up from it.
    BlockStore(BlockStore<MemoryBlockStore>),
}

pub(super) struct MemoryEntries {
    /// This device's own entries.
    device: BTreeMap<String, SecretBytes>,
    /// The person's entries.
    person: MemoryPerson,
    fail_after: std::collections::VecDeque<usize>,
    /// Refuse this device's calls on the person's entries while set.
    refuse_person: bool,
    /// Panic in the next read, write, delete or list, as a bug in the
    /// keychain's code would.
    panic_next: bool,
}

/// Panic if a test asked the next read, write, delete or list to, before it
/// changes anything and with the entries' lock released, so the entries
/// stay usable after it.
fn panic_if_asked(backend: &Mutex<MemoryEntries>) {
    let asked = std::mem::take(
        &mut backend.lock().expect("in-memory keychain entries lock is poisoned").panic_next,
    );
    if asked {
        panic!("the in-memory keychain's operation panicked");
    }
}

impl MemoryEntries {
    fn check(&mut self, scope: EntryScope) -> Result<(), KeyError> {
        let person = scope == EntryScope::Person && self.refuse_person;
        if person || self.fail_after.front() == Some(&0) {
            if !person {
                self.fail_after.pop_front();
            }
            return Err(KeychainError::from(keyring_core::Error::NoStorageAccess(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "in-memory keychain refused the operation",
                ),
            )))
            .into());
        }
        if let Some(remaining) = self.fail_after.front_mut() {
            *remaining -= 1;
        }
        Ok(())
    }

    /// Run `device` on this device's entries, or `person` on the person's.
    fn change<T>(
        &mut self,
        scope: EntryScope,
        device: impl FnOnce(&mut BTreeMap<String, SecretBytes>) -> T,
        person: impl FnOnce(&mut MemoryPerson) -> Result<T, KeyError>,
    ) -> Result<T, KeyError> {
        self.check(scope)?;
        match scope {
            EntryScope::DeviceOnly => Ok(device(&mut self.device)),
            EntryScope::Person => person(&mut self.person),
        }
    }

    fn person<T>(
        &mut self,
        change: impl FnOnce(&mut MemoryPerson) -> Result<T, KeyError>,
    ) -> Result<T, KeyError> {
        self.check(EntryScope::Person)?;
        change(&mut self.person)
    }
}

impl KeychainBackend for Mutex<MemoryEntries> {
    fn supports_person_entries(&self) -> bool {
        true
    }

    fn person_backup_follows_writes(&self) -> bool {
        matches!(
            self.lock().expect("in-memory keychain entries lock is poisoned").person,
            MemoryPerson::BlockStore(_)
        )
    }

    fn person_value_limit(&self, service: &str, account: &str) -> Option<usize> {
        match &self.lock().expect("in-memory keychain entries lock is poisoned").person {
            MemoryPerson::ICloud(_) => None,
            MemoryPerson::BlockStore(_) => {
                Some(BlockStore::<MemoryBlockStore>::value_limit(service, account))
            }
        }
    }

    fn read(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").change(
            scope,
            |entries| entries.get(account).map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec())),
            |person| match person {
                MemoryPerson::ICloud(entries) => Ok(entries.groups().read(account)),
                MemoryPerson::BlockStore(store) => store.read(service, account),
            },
        )
    }

    fn write(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").change(
            scope,
            |entries| {
                entries.insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
            },
            |person| match person {
                MemoryPerson::ICloud(entries) => {
                    entries.groups().write(account, bytes);
                    Ok(())
                }
                MemoryPerson::BlockStore(store) => store.write(service, account, bytes),
            },
        )
    }

    fn delete(&self, scope: EntryScope, service: &str, account: &str) -> Result<(), KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").change(
            scope,
            |entries| {
                entries.remove(account);
            },
            |person| match person {
                MemoryPerson::ICloud(entries) => {
                    entries.groups().delete(account);
                    Ok(())
                }
                MemoryPerson::BlockStore(store) => store.delete(service, account),
            },
        )
    }

    fn person_restore_codes(&self, service: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").person(|person| {
            match person {
                MemoryPerson::ICloud(entries) => {
                    let mut codes = Vec::new();
                    for (account, bytes) in entries.groups().all() {
                        if let Some(store) = restore_code_store(&account)? {
                            codes.push((store, bytes));
                        }
                    }
                    Ok(codes)
                }
                MemoryPerson::BlockStore(store) => store.restore_codes(service),
            }
        })
    }

    fn person_copies(&self, service: &str, account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").person(|person| {
            match person {
                MemoryPerson::ICloud(entries) => Ok(entries.groups().copies(account)),
                MemoryPerson::BlockStore(store) => {
                    Ok(store.read(service, account)?.into_iter().collect())
                }
            }
        })
    }

    fn replace_person_entry(
        &self,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        panic_if_asked(self);
        self.lock().expect("in-memory keychain entries lock is poisoned").person(|person| {
            match person {
                MemoryPerson::ICloud(entries) => {
                    entries.groups().replace(account, bytes);
                    Ok(())
                }
                MemoryPerson::BlockStore(store) => store.write(service, account, bytes),
            }
        })
    }
}

impl Keychain {
    /// An isolated in-memory keychain with separate device-only entries and
    /// person entries. The person's entries behave as iCloud Keychain keeps
    /// them, on every test platform, without touching the OS.
    pub fn in_memory(name: impl Into<String>) -> Result<Arc<Self>, KeyError> {
        let entries = SyncedEntries(Arc::new(Mutex::new(SyncedGroups::default())));
        Self::memory(name.into(), MemoryPerson::ICloud(entries))
    }

    /// An isolated in-memory keychain whose person entries behave as Block
    /// Store keeps them on an Android phone: this phone's own, with Block
    /// Store's limits, given only to a phone set up from it.
    pub fn in_memory_block_store(name: impl Into<String>) -> Result<Arc<Self>, KeyError> {
        Self::memory(
            name.into(),
            MemoryPerson::BlockStore(BlockStore::new(MemoryBlockStore::new())),
        )
    }

    /// Another device of the same person: its own empty device-only entries.
    /// With iCloud Keychain it has the same person entries, as iCloud
    /// Keychain carries them between the person's devices; with Block Store
    /// its own, empty. Failures injected into either keychain refuse only its
    /// own calls. Panics if this is a native keychain.
    pub fn another_device(&self) -> Arc<Self> {
        let person = match self.synced_entries() {
            Some(entries) => MemoryPerson::ICloud(entries),
            None => MemoryPerson::BlockStore(BlockStore::new(MemoryBlockStore::new())),
        };
        Self::memory(self.name.clone(), person).expect("registered service name is valid")
    }

    /// A share of the person's iCloud Keychain entries, for another of the
    /// person's devices; none with Block Store, whose entries are each
    /// phone's own.
    fn synced_entries(&self) -> Option<SyncedEntries> {
        self.with_memory(|memory| match &memory.person {
            MemoryPerson::ICloud(entries) => Some(entries.clone()),
            MemoryPerson::BlockStore(_) => None,
        })
    }

    /// A new phone set up from this one by direct transfer: no device-only
    /// entries, and a copy of every Block Store entry. Requires an in-memory
    /// Block Store keychain.
    pub fn set_up_new_device(&self) -> Arc<Self> {
        let store = self.memory_block_store(|store| store.set_up_new_phone());
        Self::memory(self.name.clone(), MemoryPerson::BlockStore(BlockStore::new(store)))
            .expect("registered service name is valid")
    }

    /// A new phone set up from this phone's last cloud backup: no device-only
    /// entries, and the Block Store entries that backup holds. Requires an
    /// in-memory Block Store keychain.
    pub fn restore_new_device_from_cloud(&self) -> Arc<Self> {
        let store = self.memory_block_store(|store| store.restore_new_phone_from_cloud());
        Self::memory(self.name.clone(), MemoryPerson::BlockStore(BlockStore::new(store)))
            .expect("registered service name is valid")
    }

    /// Google's periodic cloud backup of this phone's Block Store: the entries
    /// stored to be backed up. Requires an in-memory Block Store keychain.
    pub fn back_up_to_cloud(&self) {
        self.memory_block_store(MemoryBlockStore::back_up_to_cloud);
    }

    /// Whether this phone's cloud backup would be end-to-end encrypted: it has
    /// a screen lock. Requires an in-memory Block Store keychain.
    pub fn set_end_to_end_encryption(&self, available: bool) {
        self.memory_block_store(|store| store.set_end_to_end_encryption(available));
    }

    fn memory_block_store<T>(&self, f: impl FnOnce(&MemoryBlockStore) -> T) -> T {
        self.with_memory(|memory| match &memory.person {
            MemoryPerson::BlockStore(store) => f(store.api()),
            MemoryPerson::ICloud(_) => panic!("this in-memory keychain keeps no Block Store"),
        })
    }

    fn memory(name: String, person: MemoryPerson) -> Result<Arc<Self>, KeyError> {
        validate_service(&name)?;
        Ok(Arc::new(Self {
            name,
            backend: Box::new(Mutex::new(MemoryEntries {
                device: BTreeMap::new(),
                person,
                fail_after: std::collections::VecDeque::new(),
                refuse_person: false,
                panic_next: false,
            })),
            host_secrets: Mutex::new(()),
        }))
    }

    /// Run `change` on the in-memory keychain's entries. Panics if this is
    /// a native keychain.
    fn with_memory<T>(&self, change: impl FnOnce(&mut MemoryEntries) -> T) -> T {
        let backend: &dyn std::any::Any = self.backend.as_ref();
        let memory: &Mutex<MemoryEntries> =
            backend.downcast_ref().expect("failure injection requires an in-memory keychain");
        change(&mut memory.lock().expect("in-memory keychain entries lock is poisoned"))
    }

    /// Run `change` on this device's own entries, as a test inspects or
    /// moves them.
    #[cfg(test)]
    pub(super) fn with_device_entries<T>(
        &self,
        change: impl FnOnce(&mut BTreeMap<String, SecretBytes>) -> T,
    ) -> T {
        self.with_memory(|memory| change(&mut memory.device))
    }

    /// Make the fake refuse the next read, write, delete or list before changing state.
    /// Panics if this is a native keychain.
    pub fn fail_next_operation(&self) {
        self.fail_operations_after(&[0]);
    }

    /// Make the fake panic in the next read, write, delete or list before
    /// changing state, as a bug in the keychain's code would. Panics if
    /// this is a native keychain.
    pub fn panic_next_operation(&self) {
        self.with_memory(|memory| memory.panic_next = true);
    }

    /// Refuse successive operations after each given number of successful calls.
    /// For example `[2, 0]` refuses the third call and its immediate rollback.
    /// Replaces any existing failure schedule; requires an in-memory keychain.
    pub fn fail_operations_after(&self, successes: &[usize]) {
        self.with_memory(|memory| memory.fail_after = successes.iter().copied().collect());
    }

    /// Save a person entry's copy in a second keychain group, as an app that
    /// moved to a new group can find its old copy beside the new one.
    /// Requires an in-memory iCloud keychain.
    pub(crate) fn write_in_another_group(&self, account: &str, bytes: &[u8]) {
        self.with_memory(|memory| match &memory.person {
            MemoryPerson::ICloud(entries) => {
                entries
                    .groups()
                    .another
                    .insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
            }
            MemoryPerson::BlockStore(_) => panic!("Block Store has no keychain groups"),
        });
    }

    /// While `refuse` holds, refuse every call this device makes on a person
    /// entry before changing state, letting its device-only calls through.
    /// Requires an in-memory keychain.
    pub fn refuse_person_entry_operations(&self, refuse: bool) {
        self.with_memory(|memory| memory.refuse_person = refuse);
    }
}
