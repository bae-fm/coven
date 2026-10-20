//! The person's restore code as iCloud Keychain saves it, with the store's name
//! as their devices last saw it (§13.1).

use crate::SyncError;
use coven_crypto::{
    custody::{KeyError, RestoreCodeChange, SyncedKeychain},
    MaterialError, SecretBytes,
};
use coven_format::codes::{KeychainRestoreCode, RestoreCode};
use coven_foundation::id_source::StoreId;
use coven_merge::Slot;
use coven_storage::StorageConfig;

/// The store's name as this device has it, and the slot that set it: 1 for
/// creation, otherwise the rename's. Of two names, the later slot's is newer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedName {
    /// The store's name.
    pub name: String,
    /// The slot that set it.
    pub named_at: Slot,
}

impl SavedName {
    /// The name in `store`'s identity, as its applied state holds it.
    pub fn of(store: &coven_database::StoreIdentity) -> Self {
        Self {
            name: store.name.clone(),
            named_at: store.named_at,
        }
    }
}

/// Save `code`, with this device's name or the entry's when that is newer
/// (§13.1). The code is this device's to save; an entry it can't read, or
/// one for another store, is replaced.
pub(crate) fn save(
    synced: &SyncedKeychain,
    code: &RestoreCode,
    name: &SavedName,
) -> Result<(), SyncError> {
    synced.change_restore_code(|saved| {
        let newer = saved
            .and_then(|bytes| KeychainRestoreCode::from_bytes(bytes.as_bytes()).ok())
            .filter(|saved| saved.code.store == code.store && saved.named_at > name.named_at.0);
        let (label, named_at) = match &newer {
            Some(saved) => (saved.name.as_str(), saved.named_at),
            None => (name.name.as_str(), name.named_at.0),
        };
        Ok::<_, SyncError>(RestoreCodeChange::Write(encode(label, named_at, code)?))
    })
}

/// Rewrite the entry's name when `name` is newer, keeping the code the entry
/// holds: only setup, joining, restoring, resetting and member-key replacement
/// save a code (§13.1). An absent entry stays absent, so a forgotten entry
/// stays forgotten. An entry that can't be read fails with
/// `MaterialError::Encoding`, and one naming another store with
/// `RestoreCodeStoreId`: there is no code to keep.
pub fn follow_name(synced: &SyncedKeychain, name: &SavedName) -> Result<(), KeyError> {
    if !synced.supported() {
        return Ok(());
    }
    synced.change_restore_code(|saved| {
        let Some(bytes) = saved else {
            return Ok(RestoreCodeChange::Keep);
        };
        let saved = KeychainRestoreCode::from_bytes(bytes.as_bytes())
            .map_err(|_| MaterialError::Encoding)?;
        if saved.code.store != synced.store() {
            return Err(KeyError::RestoreCodeStoreId);
        }
        if saved.named_at >= name.named_at.0 {
            return Ok(RestoreCodeChange::Keep);
        }
        tracing::debug!(
            from = saved.named_at,
            to = name.named_at.0,
            "rewriting the saved restore code's store name"
        );
        let bytes = KeychainRestoreCode::encode(&name.name, name.named_at.0, &saved.code)
            .map_err(|_| MaterialError::Encoding)?;
        Ok(RestoreCodeChange::Write(SecretBytes::new(bytes.to_vec())))
    })
}

/// Why a saved entry can't be restored from. Listing shows it, with its
/// store id, so the person can forget it (§13.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnreadableSavedCode {
    /// Not a saved restore code coven reads: saved in another format, such as
    /// a bare restore code from before entries carried the store's name, or
    /// damaged. Includes a code whose storage location doesn't decode.
    Format,
    /// The code of another store than the one its account names.
    OtherStore,
    /// More than one of the app's keychain groups holds an entry for the
    /// store, so reading it by store id could give either.
    Ambiguous,
}

/// Read a saved entry for `store`, and its code's location.
pub fn read_saved_code(
    store: StoreId,
    bytes: &[u8],
) -> Result<(KeychainRestoreCode, StorageConfig), UnreadableSavedCode> {
    let saved = KeychainRestoreCode::from_bytes(bytes).map_err(|error| {
        tracing::warn!(%store, %error, "saved restore code does not decode");
        UnreadableSavedCode::Format
    })?;
    if saved.code.store != store {
        tracing::warn!(%store, other = %saved.code.store, "saved restore code names another store");
        return Err(UnreadableSavedCode::OtherStore);
    }
    let location = StorageConfig::decode(saved.code.storage.as_bytes()).map_err(|error| {
        tracing::warn!(%store, %error, "saved restore code's location does not decode");
        UnreadableSavedCode::Format
    })?;
    Ok((saved, location))
}

fn encode(name: &str, named_at: u64, code: &RestoreCode) -> Result<SecretBytes, SyncError> {
    Ok(SecretBytes::new(
        KeychainRestoreCode::encode(name, named_at, code)?.to_vec(),
    ))
}

#[cfg(test)]
#[path = "saved_code_tests.rs"]
mod tests;
