use super::*;
use crate::custody::KeychainError;

pub(super) struct MemoryEntries {
    pub(super) entries: std::collections::BTreeMap<(EntryScope, String), SecretBytes>,
    pub(super) fail_after: Option<usize>,
}

impl MemoryEntries {
    fn check(&mut self) -> Result<(), KeyError> {
        if self.fail_after == Some(0) {
            self.fail_after = None;
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
        if let Some(remaining) = &mut self.fail_after {
            *remaining -= 1;
        }
        Ok(())
    }
}

impl KeychainBackend for Mutex<MemoryEntries> {
    fn supports_synced_restore_codes(&self) -> bool {
        true
    }

    fn read(
        &self,
        scope: EntryScope,
        _: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        let mut memory = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned");
        memory.check()?;
        Ok(memory
            .entries
            .get(&(scope, account.to_owned()))
            .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec())))
    }

    fn write(
        &self,
        scope: EntryScope,
        _: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        let mut memory = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned");
        memory.check()?;
        memory.entries.insert(
            (scope, account.to_owned()),
            SecretBytes::new(bytes.to_vec()),
        );
        Ok(())
    }

    fn delete(&self, scope: EntryScope, _: &str, account: &str) -> Result<(), KeyError> {
        let mut memory = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned");
        memory.check()?;
        memory.entries.remove(&(scope, account.to_owned()));
        Ok(())
    }

    fn synced_restore_codes(&self, _: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let mut memory = self
            .lock()
            .expect("in-memory keychain entries lock is poisoned");
        memory.check()?;
        let mut codes = Vec::new();
        for ((scope, account), bytes) in &memory.entries {
            if *scope == EntryScope::Synced {
                if let Some(store) = restore_code_store(account)? {
                    codes.push((store, SecretBytes::new(bytes.as_bytes().to_vec())));
                }
            }
        }
        Ok(codes)
    }
}

impl Keychain {
    /// An isolated in-memory keychain with separate device-only and synced entries.
    /// It models Apple sync on every test platform without touching the OS.
    pub fn in_memory(name: impl Into<String>) -> Result<Arc<Self>, KeyError> {
        let name = name.into();
        validate_service(&name)?;
        Ok(Arc::new(Self {
            name,
            backend: Box::new(Mutex::new(MemoryEntries {
                entries: std::collections::BTreeMap::new(),
                fail_after: None,
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
        self.memory_backend()
            .lock()
            .expect("in-memory keychain entries lock is poisoned")
            .fail_after = Some(0);
    }
}
