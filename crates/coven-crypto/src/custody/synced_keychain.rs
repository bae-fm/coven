//! The person's entries for one store that iCloud Keychain carries to all
//! their devices (§13.1).

use super::keychain::{EntryScope, Keychain};
use super::KeyError;
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;
use std::sync::Arc;

pub(super) const RESTORE_CODE_ENTRY: &str = "restore-code";
pub(super) const S3_CREDENTIALS_ENTRY: &str = "s3-credentials";

/// One store's restore code and the S3 key saved beside it, in iCloud
/// Keychain. They belong to the person, not to this device: every device on
/// the person's Apple account sees them. So removing the store from one device
/// keeps them, and only the person leaving the store deletes them (§13.1,
/// §14.1). Every read, write and deletion here uses the synced scope; this
/// device's own entries belong to [`StoreKeychain`](super::StoreKeychain).
pub struct SyncedKeychain {
    keychain: Arc<Keychain>,
    store: StoreId,
}

impl SyncedKeychain {
    /// Bind the injected keychain to one store without reading an entry (E1).
    pub fn new(keychain: Arc<Keychain>, store: StoreId) -> Self {
        Self { keychain, store }
    }

    fn account(&self, name: &str) -> String {
        format!("{name}:{}", self.store)
    }
    fn read(&self, name: &str) -> Result<Option<SecretBytes>, KeyError> {
        self.keychain.read(EntryScope::Synced, &self.account(name))
    }
    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), KeyError> {
        self.keychain
            .write(EntryScope::Synced, &self.account(name), bytes)
    }
    fn remove(&self, name: &str) -> Result<(), KeyError> {
        self.keychain
            .delete(EntryScope::Synced, &self.account(name))
    }

    /// Whether this keychain keeps synced entries: Apple keychains, and the
    /// memory backend on every test platform.
    pub fn supported(&self) -> bool {
        self.keychain.supports_synced_entries()
    }

    /// Keep exactly the restore-code bytes in iCloud Keychain (§13.1).
    /// The caller supplies the encoded member keys, store identity and location;
    /// the crypto crate does not define the restore-code format.
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn set_restore_code(&self, code: &SecretBytes) -> Result<(), KeyError> {
        self.write(RESTORE_CODE_ENTRY, code.as_bytes())
    }

    /// Read the synced restore code, or `None` if absent (§13.1).
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn restore_code(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.read(RESTORE_CODE_ENTRY)
    }

    /// Delete the synced restore code; succeeds if absent (§13.1).
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn delete_restore_code(&self) -> Result<(), KeyError> {
        self.remove(RESTORE_CODE_ENTRY)
    }

    /// Keep a separately encoded S3 key beside the code in iCloud Keychain.
    /// Storage owns the encoding; this entry never contains the restore code.
    /// Native non-Apple keychains return `KeyError::Unsupported`.
    pub fn set_s3_credentials(&self, credentials: &SecretBytes) -> Result<(), KeyError> {
        self.write(S3_CREDENTIALS_ENTRY, credentials.as_bytes())
    }

    /// Read the separately held S3 key, or `None` when the person must enter it.
    pub fn s3_credentials(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.read(S3_CREDENTIALS_ENTRY)
    }

    /// Remove the separately synced S3 key; absent entries succeed.
    pub fn delete_s3_credentials(&self) -> Result<(), KeyError> {
        self.remove(S3_CREDENTIALS_ENTRY)
    }

    /// The person left the store: delete its S3 key, then its restore code,
    /// from every one of their devices (§14.1). The code goes last, because it
    /// is what `saved_restore_codes` lists: a failure part way leaves the
    /// store listed, never an unlisted key. Absent entries succeed, so
    /// repeating after a failure is safe. A keychain without synced entries
    /// holds neither, and nothing is deleted.
    pub fn delete_entries(&self) -> Result<(), KeyError> {
        if !self.supported() {
            return Ok(());
        }
        self.delete_s3_credentials()?;
        self.delete_restore_code()
    }
}

impl std::fmt::Debug for SyncedKeychain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SyncedKeychain([REDACTED])")
    }
}

/// The store named by a synced restore-code account; `None` for other entries.
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

#[cfg(test)]
#[path = "synced_keychain_tests.rs"]
mod tests;
