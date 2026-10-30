use super::*;
use crate::custody::person_entries::restore_code_store;
use crate::custody::KeychainError;
use std::collections::BTreeMap;

/// The synced entries iCloud Keychain carries to every device of one person.
type SyncedEntries = Arc<Mutex<SyncedGroups>>;

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
        self.default
            .insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
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

pub(super) struct MemoryEntries {
    /// This device's own entries.
    pub(super) device: BTreeMap<String, SecretBytes>,
    /// Shared with every in-memory keychain made by `another_device`.
    pub(super) synced: SyncedEntries,
    pub(super) fail_after: std::collections::VecDeque<usize>,
    /// Refuse this device's calls on synced entries while set.
    pub(super) refuse_person: bool,
}

impl MemoryEntries {
    fn check(&mut self, scope: EntryScope) -> Result<(), KeyError> {
        let synced = scope == EntryScope::Person && self.refuse_person;
        if synced || self.fail_after.front() == Some(&0) {
            if !synced {
                self.fail_after.pop_front();
            }
            return Err(
                KeychainError::from(keyring_core::Error::NoStorageAccess(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "in-memory keychain refused the operation",
                    ),
                )))
                .into(),
            );
        }
        if let Some(remaining) = self.fail_after.front_mut() {
            *remaining -= 1;
        }
        Ok(())
    }

    fn change<T>(
        &mut self,
        scope: EntryScope,
        device: impl FnOnce(&mut BTreeMap<String, SecretBytes>) -> T,
        synced_groups: impl FnOnce(&mut SyncedGroups) -> T,
    ) -> Result<T, KeyError> {
        self.check(scope)?;
        Ok(match scope {
            EntryScope::DeviceOnly => device(&mut self.device),
            EntryScope::Person => synced_groups(&mut synced(&self.synced)),
        })
    }

    fn synced<T>(&mut self, change: impl FnOnce(&mut SyncedGroups) -> T) -> Result<T, KeyError> {
        self.check(EntryScope::Person)?;
        Ok(change(&mut synced(&self.synced)))
    }
}

fn synced(entries: &SyncedEntries) -> std::sync::MutexGuard<'_, SyncedGroups> {
    entries
        .lock()
        .expect("in-memory synced keychain entries lock is poisoned")
}

impl KeychainBackend for Mutex<MemoryEntries> {
    fn supports_person_entries(&self) -> bool {
        true
    }

    fn read(
        &self,
        scope: EntryScope,
        _: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .change(
                scope,
                |entries| {
                    entries
                        .get(account)
                        .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec()))
                },
                |groups| groups.read(account),
            )
    }

    fn write(
        &self,
        scope: EntryScope,
        _: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .change(
                scope,
                |entries| {
                    entries.insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
                },
                |groups| groups.write(account, bytes),
            )
    }

    fn delete(&self, scope: EntryScope, _: &str, account: &str) -> Result<(), KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .change(
                scope,
                |entries| {
                    entries.remove(account);
                },
                |groups| groups.delete(account),
            )
    }

    fn person_restore_codes(&self, _: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let entries = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .synced(|groups| groups.all())?;
        let mut codes = Vec::new();
        for (account, bytes) in entries {
            if let Some(store) = restore_code_store(&account)? {
                codes.push((store, bytes));
            }
        }
        Ok(codes)
    }

    fn person_copies(&self, _: &str, account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .synced(|groups| groups.copies(account))
    }

    fn replace_person_entry(&self, _: &str, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .synced(|groups| groups.replace(account, bytes))
    }
}

impl Keychain {
    /// An isolated in-memory keychain with separate device-only and synced entries.
    /// It models Apple sync on every test platform without touching the OS.
    pub fn in_memory(name: impl Into<String>) -> Result<Arc<Self>, KeyError> {
        Self::memory(name.into(), SyncedEntries::default())
    }

    /// Another device of the same person: its own empty device-only entries,
    /// and the same synced entries, as iCloud Keychain carries them between
    /// the person's devices. Failures injected into either keychain refuse only
    /// its own calls. Panics if this is a native keychain.
    pub fn another_device(&self) -> Arc<Self> {
        let synced = self
            .memory_backend()
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .synced
            .clone();
        Self::memory(self.name.clone(), synced).expect("registered service name is valid")
    }

    fn memory(name: String, synced: SyncedEntries) -> Result<Arc<Self>, KeyError> {
        validate_service(&name)?;
        Ok(Arc::new(Self {
            name,
            backend: Box::new(Mutex::new(MemoryEntries {
                device: BTreeMap::new(),
                synced,
                fail_after: std::collections::VecDeque::new(),
                refuse_person: false,
            })),
            host_secrets: Mutex::new(()),
        }))
    }

    pub(super) fn memory_backend(&self) -> &Mutex<MemoryEntries> {
        let backend: &dyn std::any::Any = self.backend.as_ref();
        backend
            .downcast_ref()
            .expect("failure injection requires an in-memory keychain")
    }

    /// Make the fake refuse the next read, write, delete or list before changing state.
    /// Panics if this is a native keychain.
    pub fn fail_next_operation(&self) {
        self.fail_operations_after(&[0]);
    }

    /// Refuse successive operations after each given number of successful calls.
    /// For example `[2, 0]` refuses the third call and its immediate rollback.
    /// Replaces any existing failure schedule; requires an in-memory keychain.
    pub fn fail_operations_after(&self, successes: &[usize]) {
        self.memory_backend()
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .fail_after = successes.iter().copied().collect();
    }

    /// Save a synced entry's copy in a second keychain group, as an app that
    /// moved to a new group can find its old copy beside the new one.
    /// Requires an in-memory keychain.
    pub(crate) fn write_in_another_group(&self, account: &str, bytes: &[u8]) {
        synced(
            &self
                .memory_backend()
                .lock()
                .expect("in-memory keychain entries lock is poisoned")
                .synced,
        )
        .another
        .insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
    }

    /// While `refuse` holds, refuse every call this device makes on a synced
    /// entry before changing state, letting its device-only calls through.
    /// Requires an in-memory keychain.
    pub fn refuse_person_entry_operations(&self, refuse: bool) {
        self.memory_backend()
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .refuse_person = refuse;
    }
}
