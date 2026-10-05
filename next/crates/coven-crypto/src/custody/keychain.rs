//! One registered service, with independent entries for each store (§20.1).

use super::{KeyError, KeychainError, SecretNameError, MEMBER_KEYS_ENTRY, STORE_KEYS_ENTRY};
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
    let mut registered = service.lock().map_err(|_| KeyError::Poisoned)?;
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

enum Backend {
    Native(Arc<keyring_core::CredentialStore>),
    #[cfg(any(test, feature = "test-utils"))]
    Memory(Mutex<MemoryEntries>),
}

#[cfg(any(test, feature = "test-utils"))]
struct MemoryEntries {
    entries: std::collections::BTreeMap<String, SecretBytes>,
    fail_next: bool,
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryEntries {
    fn check(&mut self) -> Result<(), KeyError> {
        if std::mem::replace(&mut self.fail_next, false) {
            return Err(KeyError::TestKeychainFailure);
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
            .map_err(|_| KeyError::Poisoned)?
            .as_ref()
            .cloned()
            .ok_or(KeyError::ServiceNotRegistered)?;
        Ok(Arc::new(Self {
            name,
            backend: Backend::Native(super::platform::store()?),
        }))
    }

    fn read(&self, account: &str) -> Result<Option<SecretBytes>, KeyError> {
        match &self.backend {
            Backend::Native(store) => {
                let entry = store
                    .build(&self.name, account, None)
                    .map_err(KeychainError::from)?;
                match entry.get_secret() {
                    Ok(bytes) => Ok(Some(SecretBytes::new(bytes))),
                    Err(keyring_core::Error::NoEntry) => {
                        tracing::debug!(account, "keychain entry is absent");
                        Ok(None)
                    }
                    Err(error) => Err(KeychainError::from(error).into()),
                }
            }
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory.lock().map_err(|_| KeyError::Poisoned)?;
                memory.check()?;
                Ok(memory
                    .entries
                    .get(account)
                    .map(|bytes| SecretBytes::new(bytes.as_bytes().to_vec())))
            }
        }
    }

    fn write(&self, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        match &self.backend {
            Backend::Native(store) => store
                .build(&self.name, account, None)
                .and_then(|entry| entry.set_secret(bytes))
                .map_err(|e| KeychainError::from(e).into()),
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory.lock().map_err(|_| KeyError::Poisoned)?;
                memory.check()?;
                memory
                    .entries
                    .insert(account.to_owned(), SecretBytes::new(bytes.to_vec()));
                Ok(())
            }
        }
    }

    fn delete(&self, account: &str) -> Result<(), KeyError> {
        match &self.backend {
            Backend::Native(store) => {
                let entry = store
                    .build(&self.name, account, None)
                    .map_err(KeychainError::from)?;
                match entry.delete_credential() {
                    Ok(()) => Ok(()),
                    Err(keyring_core::Error::NoEntry) => {
                        tracing::debug!(account, "keychain entry is already absent");
                        Ok(())
                    }
                    Err(error) => Err(KeychainError::from(error).into()),
                }
            }
            #[cfg(any(test, feature = "test-utils"))]
            Backend::Memory(memory) => {
                let mut memory = memory.lock().map_err(|_| KeyError::Poisoned)?;
                memory.check()?;
                memory.entries.remove(account);
                Ok(())
            }
        }
    }

    /// An isolated in-memory keychain; it never registers or touches the OS.
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

    /// Make the fake refuse the next read, write or delete before changing state.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn fail_next_operation(&self) -> Result<(), KeyError> {
        match &self.backend {
            Backend::Memory(memory) => {
                memory.lock().map_err(|_| KeyError::Poisoned)?.fail_next = true;
                Ok(())
            }
            Backend::Native(_) => Err(KeyError::TestKeychainFailure),
        }
    }
}

impl std::fmt::Debug for Keychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Keychain([REDACTED])")
    }
}

/// One store's keys and host secrets under the same service and access policy.
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
        self.keychain.read(&self.account(name))
    }
    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.keychain.write(&self.account(name), bytes)
    }
    fn remove(&self, name: &str) -> Result<(), KeyError> {
        self.keychain.delete(&self.account(name))
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
    if [STORE_KEYS_ENTRY, MEMBER_KEYS_ENTRY].contains(&name) {
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
