//! One registered service, with independent entries for each store (§20.1).

use super::platform::NativeKeychain;
#[cfg(any(test, feature = "test-utils"))]
use super::KeychainError;
use super::{KeyError, SecretNameError, MEMBER_KEYS_ENTRY, STORE_KEYS_ENTRY};
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;
use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

// The API registers process-wide naming metadata (§20.1), not a live capability.
// Each composition root acquires its keychain explicitly and injects it.
static SERVICE_NAME: Mutex<Option<String>> = Mutex::new(None);

/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens (§20.1). Repeating the same
/// name is idempotent; another name is refused. This registers only the name;
/// the composition root constructs the native capability with `Keychain::registered`.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError> {
    register(&SERVICE_NAME, name.into())
}

fn register(service: &Mutex<Option<String>>, name: String) -> Result<(), KeyError> {
    validate_service(&name)?;
    let mut registered = service
        .lock()
        .expect("keyring service name lock is poisoned");
    if let Some(existing) = registered.as_ref() {
        return if *existing == name {
            Ok(())
        } else {
            Err(KeyError::ServiceAlreadyRegistered)
        };
    }
    *registered = Some(name);
    Ok(())
}

fn validate_service(name: &str) -> Result<(), KeyError> {
    if name.is_empty() || name.contains('\0') {
        return Err(KeyError::InvalidServiceName);
    }
    Ok(())
}

const RESTORE_CODE_ENTRY: &str = "restore-code";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum EntryScope {
    DeviceOnly,
    Synced,
}

enum Backend {
    Native(NativeKeychain),
    #[cfg(any(test, feature = "test-utils"))]
    Memory(Mutex<MemoryEntries>),
}

#[cfg(any(test, feature = "test-utils"))]
struct MemoryEntries {
    entries: std::collections::BTreeMap<(EntryScope, String), SecretBytes>,
    fail_next: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryEntries {
    fn check(&mut self) -> Result<(), KeyError> {
        if std::mem::replace(&mut self.fail_next, false) {
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
        Ok(())
    }
}

/// The OS keychain capability, constructed only at a composition root.
/// It never exposes native entries or its underlying credential store.
pub struct Keychain {
    name: String,
    backend: Backend,
}

impl Keychain {
    /// Construct the native capability under the service registered at startup.
    /// Call only at a composition root, then inject it; no key is read here.
    pub fn registered() -> Result<Arc<Self>, KeyError> {
        let name = SERVICE_NAME
            .lock()
            .expect("keyring service name lock is poisoned")
            .as_ref()
            .cloned()
            .ok_or(KeyError::ServiceNotRegistered)?;
        Ok(Arc::new(Self {
            name,
            backend: Backend::Native(NativeKeychain::new()?),
        }))
    }

    /// List this service's synced restore codes without knowing any store ids (§12.1).
    /// Apple searches the synced store by service; account prefixes are filtered
    /// locally because its keyring API supports exact matches only. Results are
    /// limited to the app's accessible keychain groups. Any read failure fails
    /// the entire call; a missing or unreadable code is never silently omitted.
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn synced_restore_codes(&self) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let codes = match &self.backend {
            Backend::Native(native) => native.synced_restore_codes(&self.name)?,
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory
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
                codes
            }
        };
        let mut stores = std::collections::BTreeMap::new();
        for (store, code) in codes {
            if stores.insert(store, code).is_some() {
                return Err(KeyError::AmbiguousRestoreCode(store));
            }
        }
        Ok(stores.into_iter().collect())
    }

    fn read(&self, scope: EntryScope, account: &str) -> Result<Option<SecretBytes>, KeyError> {
        match &self.backend {
            Backend::Native(native) => native.read(scope, &self.name, account),
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory
                    .lock()
                    .expect("in-memory keychain entries lock is poisoned");
                memory.check()?;
                Ok(memory
                    .entries
                    .get(&(scope, account.to_owned()))
                    .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec())))
            }
        }
    }

    fn write(&self, scope: EntryScope, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        match &self.backend {
            Backend::Native(native) => native.write(scope, &self.name, account, bytes),
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory
                    .lock()
                    .expect("in-memory keychain entries lock is poisoned");
                memory.check()?;
                memory.entries.insert(
                    (scope, account.to_owned()),
                    SecretBytes::new(bytes.to_vec()),
                );
                Ok(())
            }
        }
    }

    fn delete(&self, scope: EntryScope, account: &str) -> Result<(), KeyError> {
        match &self.backend {
            Backend::Native(native) => native.delete(scope, &self.name, account),
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory
                    .lock()
                    .expect("in-memory keychain entries lock is poisoned");
                memory.check()?;
                memory.entries.remove(&(scope, account.to_owned()));
                Ok(())
            }
        }
    }

    /// An isolated in-memory keychain with separate device-only and synced entries.
    /// It models Apple sync on every test platform without touching the OS.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn in_memory(name: impl Into<String>) -> Result<Arc<Self>, KeyError> {
        let name = name.into();
        validate_service(&name)?;
        Ok(Arc::new(Self {
            name,
            backend: Backend::Memory(Mutex::new(MemoryEntries {
                entries: std::collections::BTreeMap::new(),
                fail_next: false,
            })),
        }))
    }

    /// Make the fake refuse the next read, write, delete or list before changing state.
    /// Panics if this is a native keychain.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_next_operation(&self) {
        match &self.backend {
            Backend::Memory(memory) => {
                memory
                    .lock()
                    .expect("in-memory keychain entries lock is poisoned")
                    .fail_next = true;
            }
            Backend::Native(_) => panic!("failure injection requires an in-memory keychain"),
        }
    }
}

impl std::fmt::Debug for Keychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Keychain([REDACTED])")
    }
}

/// One store's device-only keys and host secrets, and its synced restore code.
pub struct StoreKeychain {
    keychain: Arc<Keychain>,
    store: StoreId,
}

impl StoreKeychain {
    /// Bind the injected keychain to one store without reading a key (§20.1).
    pub fn new(keychain: Arc<Keychain>, store: StoreId) -> Self {
        Self { keychain, store }
    }

    fn account(&self, name: &str) -> String {
        format!("{name}:{}", self.store)
    }
    fn read(&self, name: &str) -> Result<Option<SecretBytes>, KeyError> {
        self.keychain
            .read(EntryScope::DeviceOnly, &self.account(name))
    }
    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.keychain
            .write(EntryScope::DeviceOnly, &self.account(name), bytes)
    }
    fn remove(&self, name: &str) -> Result<(), KeyError> {
        self.keychain
            .delete(EntryScope::DeviceOnly, &self.account(name))
    }

    /// Keep exactly the restore-code bytes in iCloud Keychain (§12.1).
    /// The caller supplies the encoded member keys, store identity and storage
    /// credentials; the crypto crate does not define the restore-code format.
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn set_synced_restore_code(&self, code: &SecretBytes) -> Result<(), KeyError> {
        self.keychain.write(
            EntryScope::Synced,
            &self.account(RESTORE_CODE_ENTRY),
            code.as_bytes(),
        )
    }

    /// Read the synced restore code, or `None` if absent (§12.1).
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn synced_restore_code(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.keychain
            .read(EntryScope::Synced, &self.account(RESTORE_CODE_ENTRY))
    }

    /// Delete the synced restore code; succeeds if absent (§12.1).
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn delete_synced_restore_code(&self) -> Result<(), KeyError> {
        self.keychain
            .delete(EntryScope::Synced, &self.account(RESTORE_CODE_ENTRY))
    }

    /// Keeps an app secret in the same keychain and access policy as coven's keys.
    /// Names cannot be empty, contain `:` or NUL, or name coven's own entries.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        validate_host_name(name)?;
        self.write(name, value.as_bytes())
    }

    /// The app secret, or `None` if it was never set (§20.11).
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        validate_host_name(name)?;
        self.read(name)?
            .map(|bytes| {
                std::str::from_utf8(bytes.as_bytes())
                    .map(str::to_owned)
                    .map_err(|_| KeyError::HostSecretEncoding)
            })
            .transpose()
    }

    /// Deletes the app secret; succeeds if it was never set (§20.11).
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        validate_host_name(name)?;
        self.remove(name)
    }
}

impl std::fmt::Debug for StoreKeychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreKeychain([REDACTED])")
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
pub(crate) fn restore_code_store(account: &str) -> Result<Option<StoreId>, KeyError> {
    match account.split_once(':') {
        Some((RESTORE_CODE_ENTRY, id)) => uuid::Uuid::parse_str(id)
            .map(|id| Some(StoreId(id)))
            .map_err(|_| KeyError::RestoreCodeStoreId),
        _ => {
            tracing::debug!(account, "skipping synced entry that is not a restore code");
            Ok(None)
        }
    }
}

fn validate_host_name(name: &str) -> Result<(), SecretNameError> {
    if name.is_empty() {
        return Err(SecretNameError::Empty);
    }
    if name.contains(':') {
        return Err(SecretNameError::Separator);
    }
    if name.contains('\0') {
        return Err(SecretNameError::Nul);
    }
    if [STORE_KEYS_ENTRY, MEMBER_KEYS_ENTRY, RESTORE_CODE_ENTRY].contains(&name) {
        return Err(SecretNameError::Reserved);
    }
    Ok(())
}

/// OS keychain custody for `StoreKeyring` or `MemberKeys`, built at a composition root.
pub struct KeyringCustody<T> {
    keychain: Arc<StoreKeychain>,
    material: PhantomData<fn() -> T>,
}

impl<T> KeyringCustody<T> {
    /// Retain the injected store keychain; construction reads no key (§20.1).
    pub fn new(keychain: Arc<StoreKeychain>) -> Self {
        Self {
            keychain,
            material: PhantomData,
        }
    }
    pub(crate) fn read(&self, name: &str) -> Result<Option<SecretBytes>, KeyError> {
        self.keychain.read(name)
    }
    pub(crate) fn write(&self, name: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.keychain.write(name, bytes)
    }
    pub(crate) fn remove(&self, name: &str) -> Result<(), KeyError> {
        self.keychain.remove(name)
    }
}

impl<T> std::fmt::Debug for KeyringCustody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeyringCustody([REDACTED])")
    }
}

#[cfg(test)]
#[path = "keychain_tests.rs"]
mod tests;
