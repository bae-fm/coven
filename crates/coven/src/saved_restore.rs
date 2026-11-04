//! Discover and forget saved recovery choices without exposing their private
//! keys (§13.1).

use crate::CloudProvider;
use crate::{BootstrapError, StorageConfig, StoreId};
use coven_crypto::custody::{Keychain, PersonEntries, PersonRestoreCode};
use coven_sync::UnreadableSavedCode;
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

pub(crate) fn list(keychain: Arc<Keychain>) -> Result<Vec<SavedRestoreEntry>, BootstrapError> {
    keychain
        .person_restore_codes()?
        .into_iter()
        .map(|(store, code)| entry(&PersonEntries::new(keychain.clone(), store), code))
        .collect()
}

/// Read one store's listed entry. Only a keychain read fails it; an entry
/// coven can't restore from is `Unreadable`.
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

pub(crate) fn forget(keychain: Arc<Keychain>, store: StoreId) -> Result<(), BootstrapError> {
    Ok(PersonEntries::new(keychain, store).forget()?)
}

#[cfg(test)]
#[path = "saved_restore_tests.rs"]
mod tests;
