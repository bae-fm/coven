//! One registered service, with independent entries for each store (E1).

use super::platform::NativeKeychain;
use super::{KeyError, MEMBER_KEYS_ENTRY, STORE_KEYS_ENTRY};
use crate::{wire, MaterialError, SecretBytes};
use coven_foundation::id_source::{DeviceId, StoreId};
use std::{
    collections::BTreeSet,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

// The API registers process-wide naming metadata (E1), not a live capability.
// Each composition root acquires its keychain explicitly and injects it.
static SERVICE_NAME: Mutex<Option<String>> = Mutex::new(None);

/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens (E1). Repeating the same
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
const DEVICE_ID_ENTRY: &str = "device-id";
const CREDENTIALS_ENTRY: &str = "storage-credentials";
const HOST_SECRET_NAMES_ENTRY: &str = "host-secret-names";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum EntryScope {
    DeviceOnly,
    Synced,
}

pub(crate) trait KeychainBackend: std::any::Any + Send + Sync {
    fn read(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError>;
    fn write(
        &self,
        scope: EntryScope,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError>;
    fn delete(&self, scope: EntryScope, service: &str, account: &str) -> Result<(), KeyError>;
    fn synced_restore_codes(&self, service: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError>;
    fn supports_synced_restore_codes(&self) -> bool;
}

/// The OS keychain capability, constructed only at a composition root.
/// It never exposes native entries or its underlying credential store.
pub struct Keychain {
    name: String,
    backend: Box<dyn KeychainBackend>,
    host_secrets: Mutex<()>,
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
            backend: Box::new(NativeKeychain::new()?),
            host_secrets: Mutex::new(()),
        }))
    }

    /// List this service's synced restore codes without knowing any store ids (§12.1).
    /// Apple searches the synced store by service; account prefixes are filtered
    /// locally because its keyring API supports exact matches only. Results are
    /// limited to the app's accessible keychain groups. Any read failure fails
    /// the entire call; a missing or unreadable code is never silently omitted.
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn synced_restore_codes(&self) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let codes = self.backend.synced_restore_codes(&self.name)?;
        let mut stores = std::collections::BTreeMap::new();
        for (store, code) in codes {
            if stores.insert(store, code).is_some() {
                return Err(KeyError::AmbiguousRestoreCode(store));
            }
        }
        Ok(stores.into_iter().collect())
    }

    fn read(&self, scope: EntryScope, account: &str) -> Result<Option<SecretBytes>, KeyError> {
        self.backend.read(scope, &self.name, account)
    }

    fn write(&self, scope: EntryScope, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.backend.write(scope, &self.name, account, bytes)
    }

    fn delete(&self, scope: EntryScope, account: &str) -> Result<(), KeyError> {
        self.backend.delete(scope, &self.name, account)
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
    /// Bind the injected keychain to one store without reading a key (E1).
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

    /// The installation id kept outside device backups (§10). Reading it does
    /// not unlock any member or store key.
    pub fn device_id(&self) -> Result<Option<DeviceId>, KeyError> {
        self.read(DEVICE_ID_ENTRY)?
            .map(|bytes| {
                let bytes: [u8; 8] = bytes
                    .as_bytes()
                    .try_into()
                    .map_err(|_| KeyError::DeviceIdEncoding)?;
                Ok(DeviceId(u64::from_be_bytes(bytes)))
            })
            .transpose()
    }

    /// Keep the installation id in this device's non-restored keychain scope.
    pub fn set_device_id(&self, device: DeviceId) -> Result<(), KeyError> {
        self.write(DEVICE_ID_ENTRY, &device.0.to_be_bytes())
    }

    /// Remove an unpublished installation's entry after creation failed.
    pub fn delete_device_id(&self) -> Result<(), KeyError> {
        self.remove(DEVICE_ID_ENTRY)
    }

    /// Provider-encoded credentials. Public storage settings never contain them.
    pub fn storage_credentials(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.read(CREDENTIALS_ENTRY)
    }

    /// Commit provider-encoded credentials in this device's keychain scope.
    pub fn set_storage_credentials(&self, bytes: &SecretBytes) -> Result<(), KeyError> {
        self.write(CREDENTIALS_ENTRY, bytes.as_bytes())
    }

    /// Remove this device's provider credentials; an absent entry succeeds.
    pub fn delete_storage_credentials(&self) -> Result<(), KeyError> {
        self.remove(CREDENTIALS_ENTRY)
    }

    /// Whether this backend supports iCloud restore-code publication. The memory
    /// backend models Apple keychains on every test platform.
    pub fn supports_synced_restore_codes(&self) -> bool {
        self.keychain.backend.supports_synced_restore_codes()
    }

    /// Remove every coven entry, including all recorded host secrets, without
    /// opening the database or decoding the values. Delete listed secrets before
    /// the list, then coven's other entries. The caller holds the store's deletion
    /// lock; repeating after a failure is safe, including absent listed entries.
    pub fn delete_store_entries(&self) -> Result<(), KeyError> {
        let _guard = self
            .keychain
            .host_secrets
            .lock()
            .expect("host secrets lock poisoned");
        if let Some(bytes) = self.read(HOST_SECRET_NAMES_ENTRY)? {
            for name in decode_host_secret_names(bytes.as_bytes())? {
                self.remove(&host_secret_entry(name))?;
            }
        }
        for name in [
            HOST_SECRET_NAMES_ENTRY,
            STORE_KEYS_ENTRY,
            MEMBER_KEYS_ENTRY,
            DEVICE_ID_ENTRY,
            CREDENTIALS_ENTRY,
        ] {
            self.remove(name)?;
        }
        if self.supports_synced_restore_codes() {
            self.delete_synced_restore_code()?;
        }
        Ok(())
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

    /// Records the name in a device-only keychain list before writing the value
    /// to its own entry. Arbitrary names are encoded into native account names;
    /// each value retains the platform's per-entry size limit. A failed write
    /// may leave an extra name, so deletion can always find every saved secret.
    /// The caller holds the store's writer lock; this capability serializes updates.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.change_host_secret(name, Some(value))
    }

    /// The app secret, or `None` if it was never set (E11).
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.read(&host_secret_entry(name))?
            .map(|bytes| {
                std::str::from_utf8(bytes.as_bytes())
                    .map(str::to_owned)
                    .map_err(|_| MaterialError::Encoding.into())
            })
            .transpose()
    }

    /// Removes the secret before its recorded name (E11); absent entries succeed.
    /// Failure can leave an extra name; retrying is safe. The caller holds the
    /// store's writer lock.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.change_host_secret(name, None)
    }

    fn change_host_secret(&self, name: &str, value: Option<&str>) -> Result<(), KeyError> {
        let _guard = self
            .keychain
            .host_secrets
            .lock()
            .expect("host secrets lock poisoned");
        let bytes = self.read(HOST_SECRET_NAMES_ENTRY)?;
        let mut names = match &bytes {
            Some(bytes) => decode_host_secret_names(bytes.as_bytes())?,
            None => BTreeSet::new(),
        };
        match value {
            Some(value) => {
                names.insert(name);
                self.write_host_secret_names(&names)?;
                self.write(&host_secret_entry(name), value.as_bytes())
            }
            None => {
                self.remove(&host_secret_entry(name))?;
                names.remove(name);
                self.write_host_secret_names(&names)
            }
        }
    }

    fn write_host_secret_names(&self, names: &BTreeSet<&str>) -> Result<(), KeyError> {
        if names.is_empty() {
            self.remove(HOST_SECRET_NAMES_ENTRY)
        } else {
            self.write(
                HOST_SECRET_NAMES_ENTRY,
                encode_host_secret_names(names)?.as_bytes(),
            )
        }
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

// Hex keeps empty, NUL-containing and Unicode names distinct and separates every
// app name from coven's own entries, even on case-insensitive native keychains.
fn host_secret_entry(name: &str) -> String {
    format!("host-secret-{}", hex::encode(name.as_bytes()))
}

// Local custody framing: a versioned prefix and length-prefixed UTF-8 names.
// Values live in independent native entries and have no coven framing overhead.
const HOST_SECRET_NAMES_PREFIX: &[u8] = b"CVHN\x01";

fn decode_host_secret_names(mut bytes: &[u8]) -> Result<BTreeSet<&str>, MaterialError> {
    wire::prefix(&mut bytes, HOST_SECRET_NAMES_PREFIX)?;
    let mut names = BTreeSet::new();
    while !bytes.is_empty() {
        let len =
            usize::try_from(wire::number(&mut bytes)?).map_err(|_| MaterialError::Encoding)?;
        let name = std::str::from_utf8(wire::take(&mut bytes, len)?)
            .map_err(|_| MaterialError::Encoding)?;
        if !names.insert(name) {
            return Err(MaterialError::Encoding);
        }
    }
    Ok(names)
}

fn encode_host_secret_names(names: &BTreeSet<&str>) -> Result<SecretBytes, MaterialError> {
    let len = names
        .iter()
        .try_fold(HOST_SECRET_NAMES_PREFIX.len(), |len, name| {
            len.checked_add(8)?.checked_add(name.len())
        })
        .ok_or(MaterialError::Encoding)?;
    let mut bytes = SecretBytes::new(Vec::with_capacity(len));
    bytes.0.extend_from_slice(HOST_SECRET_NAMES_PREFIX);
    for name in names {
        bytes
            .0
            .extend_from_slice(&(name.len() as u64).to_le_bytes());
        bytes.0.extend_from_slice(name.as_bytes());
    }
    Ok(bytes)
}

/// OS keychain custody for `StoreKeyring` or `MemberKeys`, built at a composition root.
pub struct KeyringCustody<T> {
    keychain: Arc<StoreKeychain>,
    material: PhantomData<fn() -> T>,
}

impl<T> KeyringCustody<T> {
    /// Retain the injected store keychain; construction reads no key (E1).
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

#[cfg(any(test, feature = "test-utils"))]
#[path = "keychain_test_utils.rs"]
mod test_utils;
