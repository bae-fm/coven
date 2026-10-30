//! Discover and forget saved recovery choices without exposing their private
//! keys (§13.1).

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
use crate::CloudProvider;
use crate::{BootstrapError, StorageConfig, StoreId};
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
use coven_crypto::custody::{Keychain, PersonEntries, PersonRestoreCode};
use coven_sync::UnreadableSavedCode;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
use std::sync::Arc;

/// A store whose restore code is saved in iCloud Keychain or Block Store,
/// without secrets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedRestoreCode {
    /// The store's name as one of the person's devices last saw it: a label
    /// only, never used to find the store. A rename made while none of their
    /// devices had the store open leaves it unchanged (§13.1).
    pub name: String,
    /// The store named by the saved code.
    pub store_id: StoreId,
    /// The code's provider and account or location, with no credentials.
    /// `provider()` identifies the provider; `oauth_access()` names its sign-in requirement.
    pub location: StorageConfig,
    /// Whether a separate S3 key was present when this entry was listed.
    pub has_s3_credentials: bool,
}

/// One store's saved entry, as listing found it (§13.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SavedRestoreEntry {
    /// An entry the app can pass to `restore_from_saved_code`.
    Readable(SavedRestoreCode),
    /// An entry coven can't restore from. The app offers to forget it.
    Unreadable {
        /// The store its keychain account names.
        store_id: StoreId,
        /// Why it can't be restored from.
        reason: UnreadableSavedCode,
    },
}

impl SavedRestoreEntry {
    /// The store this entry is saved under.
    pub fn store_id(&self) -> StoreId {
        match self {
            Self::Readable(saved) => saved.store_id,
            Self::Unreadable { store_id, .. } => *store_id,
        }
    }
}

/// List the registered service's saved entries (§13.1), in iCloud Keychain
/// on Apple and in this phone's Block Store on Android: one per
/// store, ordered by store id, or an empty list. An entry coven can't restore
/// from is listed as `Unreadable`, never omitted, so the person can forget it.
/// A keychain read failure (the search, an entry's bytes or an S3 key's
/// presence) fails the whole call, as does a restore-code account with no
/// store id, `KeyError::RestoreCodeStoreId`.
/// Platforms with neither iCloud Keychain nor Block Store return `BootstrapError::SecureStorage(KeyError::Unsupported)`.
/// Pass a readable entry to `restore_from_saved_code`, which reads its current code and key.
pub async fn saved_restore_codes() -> Result<Vec<SavedRestoreEntry>, BootstrapError> {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "android"))]
    {
        crate::coven::blocking(|| list(Keychain::registered()?)).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
    {
        Err(crate::KeyError::Unsupported.into())
    }
}

/// Delete the store's saved S3 key, then its restore code: from iCloud
/// Keychain, and so from every one of the person's Apple devices, or from this
/// phone's Block Store, leaving other phones' own (§13.1). Readable and
/// unreadable entries alike; every keychain group's copy goes.
/// The store, its storage and every device's copy stay; no store need be open
/// and no provider connected. Absent entries succeed; after a failure part way
/// the store is still listed, and calling again finishes. A later join,
/// restore, reset or member-key replacement saves the entry again.
/// Platforms with neither iCloud Keychain nor Block Store return `BootstrapError::SecureStorage(KeyError::Unsupported)`.
pub async fn forget_saved_restore_code(store: StoreId) -> Result<(), BootstrapError> {
    #[cfg(any(target_os = "macos", target_os = "ios", target_os = "android"))]
    {
        crate::coven::blocking(move || forget(Keychain::registered()?, store)).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios", target_os = "android")))]
    {
        let _ = store;
        Err(crate::KeyError::Unsupported.into())
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
pub(crate) fn list(keychain: Arc<Keychain>) -> Result<Vec<SavedRestoreEntry>, BootstrapError> {
    keychain
        .person_restore_codes()?
        .into_iter()
        .map(|(store, code)| entry(&PersonEntries::new(keychain.clone(), store), code))
        .collect()
}

/// Read one store's listed entry. Only a keychain read fails it; an entry
/// coven can't restore from is `Unreadable`.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
pub(crate) fn entry(
    person: &PersonEntries,
    code: PersonRestoreCode,
) -> Result<SavedRestoreEntry, BootstrapError> {
    let store_id = person.store();
    let decoded = match code {
        PersonRestoreCode::Saved(bytes) => coven_sync::read_saved_code(store_id, bytes.as_bytes()),
        PersonRestoreCode::Ambiguous => Err(UnreadableSavedCode::Ambiguous),
    };
    let (saved, location) = match decoded {
        Ok(decoded) => decoded,
        Err(reason) => return Ok(SavedRestoreEntry::Unreadable { store_id, reason }),
    };
    let has_s3_credentials =
        location.provider() == CloudProvider::S3 && person.s3_credentials()?.is_some();
    Ok(SavedRestoreEntry::Readable(SavedRestoreCode {
        name: saved.name,
        store_id,
        location,
        has_s3_credentials,
    }))
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
pub(crate) fn forget(keychain: Arc<Keychain>, store: StoreId) -> Result<(), BootstrapError> {
    Ok(PersonEntries::new(keychain, store).forget()?)
}

#[cfg(test)]
#[path = "saved_restore_tests.rs"]
mod tests;
