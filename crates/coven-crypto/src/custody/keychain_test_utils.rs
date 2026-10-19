use super::*;
use crate::custody::synced_keychain::restore_code_store;
use crate::custody::KeychainError;
use std::collections::BTreeMap;

/// The synced entries iCloud Keychain carries to every device of one person.
type SyncedEntries = Arc<Mutex<BTreeMap<String, SecretBytes>>>;

pub(super) struct MemoryEntries {
    /// This device's own entries.
    pub(super) device: BTreeMap<String, SecretBytes>,
    /// Shared with every in-memory keychain made by `another_device`.
    pub(super) synced: SyncedEntries,
    pub(super) fail_after: std::collections::VecDeque<usize>,
    /// Refuse this device's calls on synced entries while set.
    pub(super) refuse_synced: bool,
}

impl MemoryEntries {
    fn check(&mut self, scope: EntryScope) -> Result<(), KeyError> {
        let synced = scope == EntryScope::Synced && self.refuse_synced;
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
        change: impl FnOnce(&mut BTreeMap<String, SecretBytes>) -> T,
    ) -> Result<T, KeyError> {
        self.check(scope)?;
        Ok(match scope {
            EntryScope::DeviceOnly => change(&mut self.device),
            EntryScope::Synced => change(&mut synced(&self.synced)),
        })
    }
}

fn synced(entries: &SyncedEntries) -> std::sync::MutexGuard<'_, BTreeMap<String, SecretBytes>> {
    entries
        .lock()
        .expect("in-memory synced keychain entries lock is poisoned")
}

impl KeychainBackend for Mutex<MemoryEntries> {
    fn supports_synced_entries(&self) -> bool {
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
            .change(scope, |entries| {
                entries
                    .get(account)
                    .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec()))
            })
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
            .change(scope, |entries| {
                entries.insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
            })
    }

    fn delete(&self, scope: EntryScope, _: &str, account: &str) -> Result<(), KeyError> {
        self.lock()
            .expect("in-memory keychain entries lock is poisoned")
            .change(scope, |entries| {
                entries.remove(account);
            })
    }

    fn synced_restore_codes(&self, _: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let entries = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .change(EntryScope::Synced, |entries| {
                entries
                    .iter()
                    .map(|(account, bytes)| {
                        (account.clone(), SecretBytes::new(bytes.as_bytes().to_vec()))
                    })
                    .collect::<Vec<_>>()
            })?;
        let mut codes = Vec::new();
        for (account, bytes) in entries {
            if let Some(store) = restore_code_store(&account)? {
                codes.push((store, bytes));
            }
        }
        Ok(codes)
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
                refuse_synced: false,
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

    /// While `refuse` holds, refuse every call this device makes on a synced
    /// entry before changing state, letting its device-only calls through.
    /// Requires an in-memory keychain.
    pub fn refuse_synced_operations(&self, refuse: bool) {
        self.memory_backend()
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .refuse_synced = refuse;
    }
}
