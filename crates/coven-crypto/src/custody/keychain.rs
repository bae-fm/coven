//! One registered service, with independent entries for each store (E1).

use super::platform::NativeKeychain;
use super::{KeyError, PersonRestoreCode, MEMBER_KEYS_ENTRY, STORE_KEYS_ENTRY};
use crate::{wire, MaterialError, SecretBytes, SecretText};
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

const DEVICE_ID_ENTRY: &str = "device-id";
const CREDENTIALS_ENTRY: &str = "storage-credentials";
const HOST_SECRET_NAMES_ENTRY: &str = "host-secret-names";
const INVITE_CODE_ENTRY: &str = "invite-code";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum EntryScope {
    /// This device's own entries, which never leave it.
    DeviceOnly,
    /// The person's entries, which reach their next devices: iCloud Keychain
    /// carries them to every device on the person's Apple account, and Block
    /// Store gives an Android phone's to a phone set up from it.
    Person,
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
    fn person_restore_codes(&self, service: &str) -> Result<Vec<(StoreId, SecretBytes)>, KeyError>;
    /// Every keychain group's copy of one person entry.
    fn person_copies(&self, service: &str, account: &str) -> Result<Vec<SecretBytes>, KeyError>;
    /// Leave exactly one copy of a person entry, holding `bytes`: write the
    /// first group's copy, or a new entry when there is none, then delete the
    /// other groups' copies. A failure part way leaves more than one copy.
    fn replace_person_entry(
        &self,
        service: &str,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError>;
    fn supports_person_entries(&self) -> bool;
    /// The most bytes a person entry's value may take, or `None` when the
    /// backend sets no limit a restore code could reach.
    fn person_value_limit(&self, service: &str, account: &str) -> Option<usize>;
    /// Whether each write of a person entry chooses again whether it goes to
    /// a cloud backup, as Block Store's does.
    fn person_backup_follows_writes(&self) -> bool;
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

    /// List this service's saved restore codes without knowing any store ids,
    /// one per store, ordered by store id (§13.1). Apple searches iCloud
    /// Keychain by service; account prefixes are filtered locally because its
    /// keyring API supports exact matches only. Results are limited to the
    /// app's accessible keychain groups; a store more than one of them holds
    /// an entry for is `PersonRestoreCode::Ambiguous`. Block Store gives every
    /// entry the app keeps, and only this service's restore codes are kept. Any read failure fails
    /// the entire call; a missing or unreadable code is never silently omitted.
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn person_restore_codes(&self) -> Result<Vec<(StoreId, PersonRestoreCode)>, KeyError> {
        Ok(by_store(self.backend.person_restore_codes(&self.name)?))
    }

    /// Whether this backend keeps the person's entries: iCloud Keychain on
    /// Apple, Block Store on an Android phone with Google Play services, and
    /// the memory backend on every test platform.
    pub(super) fn supports_person_entries(&self) -> bool {
        self.backend.supports_person_entries()
    }

    /// Whether each write of a person entry chooses again whether it goes to
    /// a cloud backup (§13.1).
    pub(super) fn person_backup_follows_writes(&self) -> bool {
        self.backend.person_backup_follows_writes()
    }

    /// The most bytes the person entry `account` may hold (§13.1).
    pub(super) fn person_value_limit(&self, account: &str) -> Option<usize> {
        self.backend.person_value_limit(&self.name, account)
    }

    pub(super) fn read(
        &self,
        scope: EntryScope,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        self.backend.read(scope, &self.name, account)
    }

    pub(super) fn write(
        &self,
        scope: EntryScope,
        account: &str,
        bytes: &[u8],
    ) -> Result<(), KeyError> {
        self.backend.write(scope, &self.name, account, bytes)
    }

    pub(super) fn delete(&self, scope: EntryScope, account: &str) -> Result<(), KeyError> {
        self.backend.delete(scope, &self.name, account)
    }

    pub(super) fn person_copies(&self, account: &str) -> Result<Vec<SecretBytes>, KeyError> {
        self.backend.person_copies(&self.name, account)
    }

    pub(super) fn replace_person_entry(&self, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.backend
            .replace_person_entry(&self.name, account, bytes)
    }
}

impl std::fmt::Debug for Keychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Keychain([REDACTED])")
    }
}

/// One store's entries kept on this device only: its device id, store and
/// member keys, provider credentials, the invite code of a join under way and
/// the app's host secrets (§12.4, §13.2). They
/// never leave the device, so deleting them reaches no other device of the
/// person. Every read, write and deletion here uses the device-only scope; the
/// person's entries belong to [`PersonEntries`](super::PersonEntries).
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

    /// The installation id, in a device-only entry a backup doesn't carry to
    /// another device (§19.3). Reading it does not unlock any member or store
    /// key.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored device id fails only by not being 8 bytes"
    )]
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

    /// The invite code of a join under way for this store on this device:
    /// its saved request is encrypted with the code's secret, so resuming
    /// needs the code (§13.2). It is never synced.
    #[expect(
        clippy::map_err_ignore,
        reason = "a saved invite code fails only by not being UTF-8, which Encoding names"
    )]
    pub fn invite_code(&self) -> Result<Option<SecretText>, KeyError> {
        self.read(INVITE_CODE_ENTRY)?
            .map(|bytes| {
                std::str::from_utf8(bytes.as_bytes())
                    .map(|code| SecretText::new(code.to_owned()))
                    .map_err(|_| MaterialError::Encoding.into())
            })
            .transpose()
    }

    /// Keep a join's invite code until the join ends.
    pub fn set_invite_code(&self, code: &SecretText) -> Result<(), KeyError> {
        self.write(INVITE_CODE_ENTRY, code.as_str().as_bytes())
    }

    /// Remove a join's invite code once the join has ended; an absent entry
    /// succeeds.
    pub fn delete_invite_code(&self) -> Result<(), KeyError> {
        self.remove(INVITE_CODE_ENTRY)
    }

    /// Remove this device's entries for the store, including all recorded host
    /// secrets, without opening the database or decoding the values. Delete
    /// listed secrets before the list, then coven's other entries. The person's
    /// restore code and S3 key stay: they serve the person's next devices
    /// (§13.1). The caller holds the store's deletion lock;
    /// repeating after a failure is safe, including absent listed entries.
    pub fn delete_device_entries(&self) -> Result<(), KeyError> {
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
            INVITE_CODE_ENTRY,
        ] {
            self.remove(name)?;
        }
        Ok(())
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
    #[expect(
        clippy::map_err_ignore,
        reason = "a host secret fails only by not being UTF-8, which Encoding names"
    )]
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

// Hex keeps empty, NUL-containing and Unicode names distinct and separates every
// app name from coven's own entries, even on case-insensitive native keychains.
fn host_secret_entry(name: &str) -> String {
    format!("host-secret-{}", hex::encode(name.as_bytes()))
}

// Local custody framing: a versioned prefix and length-prefixed UTF-8 names.
// Values live in independent native entries and have no coven framing overhead.
const HOST_SECRET_NAMES_PREFIX: &[u8] = b"CVHN\x01";

#[expect(
    clippy::map_err_ignore,
    reason = "a name list fails only by an invalid length or name: Encoding"
)]
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

/// OS keychain custody for `StoreKeyring` or `MemberIdentity`, built at a composition root.
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

/// Group the restore codes a search found by store, ordered by store id. A
/// store found more than once is ambiguous: no copy is the store's code.
fn by_store(codes: Vec<(StoreId, SecretBytes)>) -> Vec<(StoreId, PersonRestoreCode)> {
    let mut stores = std::collections::BTreeMap::new();
    for (store, code) in codes {
        stores
            .entry(store)
            .and_modify(|found| {
                tracing::warn!(%store, "more than one keychain group holds a restore code");
                *found = PersonRestoreCode::Ambiguous;
            })
            .or_insert(PersonRestoreCode::Saved(code));
    }
    stores.into_iter().collect()
}

#[cfg(test)]
#[path = "keychain_tests.rs"]
mod tests;

#[cfg(any(test, feature = "test-utils"))]
#[path = "keychain_test_utils.rs"]
mod test_utils;
