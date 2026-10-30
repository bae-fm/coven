//! The person's restore code as iCloud Keychain or Block Store saves it, with
//! the store's name as their devices last saw it (§13.1).

use crate::SyncError;
use coven_crypto::{
    custody::{KeyError, PersonEntries, PersonRestoreCode, RestoreCodeChange},
    MaterialError, MemberIdentity, SecretBytes,
};
use coven_database::StoreState;
use coven_format::codes::{KeychainRestoreCode, RestoreCode};
use coven_foundation::id_source::StoreId;
use coven_merge::Slot;
use coven_storage::{RestoreStorage, StorageConfig};

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
    person: &PersonEntries,
    code: &RestoreCode,
    name: &SavedName,
) -> Result<(), SyncError> {
    person.change_restore_code(|saved| {
        let newer = match saved {
            Some(PersonRestoreCode::Saved(bytes)) => {
                KeychainRestoreCode::from_bytes(bytes.as_bytes()).ok()
            }
            None | Some(PersonRestoreCode::Ambiguous) => None,
        }
        .filter(|saved| saved.code.store == code.store && saved.named_at > name.named_at.0);
        let (label, named_at) = match &newer {
            Some(saved) => (saved.name.as_str(), saved.named_at),
            None => (name.name.as_str(), name.named_at.0),
        };
        Ok::<_, SyncError>(RestoreCodeChange::Write(encode(
            person, label, named_at, code,
        )?))
    })
}

/// Rewrite the entry's name when `name` is newer, keeping the code the entry
/// holds: only setup, joining, restoring, resetting and member-key replacement
/// save a code over a readable one (§13.1). An absent entry stays absent, so a
/// forgotten entry stays forgotten.
///
/// This device has the store open, so it holds the store's current code and
/// name. An entry it can't read (another format, another store's code, or a
/// copy in more than one keychain group) is replaced with `current`'s code
/// under `name`, as one copy. When `current` gives no code, because this
/// device's keys were replaced, the store has no location, or the member was
/// removed, the entry stays as it is. A keychain failure fails the call.
pub fn follow_name<E: From<KeyError>>(
    person: &PersonEntries,
    name: &SavedName,
    current: impl FnOnce() -> Result<Option<RestoreCode>, E>,
) -> Result<(), E> {
    if !person.supported() {
        return Ok(());
    }
    person.change_restore_code(|saved| {
        let read = match saved {
            None => return Ok(RestoreCodeChange::Keep),
            Some(PersonRestoreCode::Saved(bytes)) => {
                read_saved_code(person.store(), bytes.as_bytes())
            }
            Some(PersonRestoreCode::Ambiguous) => Err(UnreadableSavedCode::Ambiguous),
        };
        let saved = match read {
            Ok((saved, _)) => saved,
            Err(reason) => {
                let Some(code) = current()? else {
                    tracing::warn!(
                        ?reason,
                        "keeping an unreadable saved restore code: this device holds no current code"
                    );
                    return Ok(RestoreCodeChange::Keep);
                };
                tracing::warn!(?reason, "replacing an unreadable saved restore code");
                let bytes = encode(person, &name.name, name.named_at.0, &code)?;
                return Ok(RestoreCodeChange::Write(bytes));
            }
        };
        if saved.named_at >= name.named_at.0 {
            return Ok(RestoreCodeChange::Keep);
        }
        tracing::debug!(
            from = saved.named_at,
            to = name.named_at.0,
            "rewriting the saved restore code's store name"
        );
        let bytes = encode(person, &name.name, name.named_at.0, &saved.code)?;
        Ok(RestoreCodeChange::Write(bytes))
    })
}

/// The code a device with the store open saves in place of an unreadable
/// entry (§13.1), or `None` when it holds no current code: the store has no
/// location or identity, custody holds no member pair, the member was
/// removed, or another device replaced the keys this device holds.
pub fn replacement_code(
    state: &StoreState,
    identity: Option<&MemberIdentity>,
    location: Option<&StorageConfig>,
) -> Result<Option<RestoreCode>, SyncError> {
    let (Some(identity), Some(location)) = (identity, location) else {
        return Ok(None);
    };
    let storage = RestoreStorage::location_only(location.clone());
    match crate::restore_codes::member_code(state, identity, &storage) {
        Ok(code) => Ok(Some(code)),
        Err(SyncError::NoStorage | SyncError::Removed | SyncError::MemberKeysReplaced { .. }) => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Why a saved entry can't be restored from. Listing shows it, with its
/// store id, so the person can forget it (§13.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnreadableSavedCode {
    /// Not a saved restore code coven reads: saved in another format, such as
    /// a bare restore code from before entries carried the store's name, or
    /// damaged. Includes a code whose storage part doesn't decode or
    /// carries an S3 key.
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
    let storage = RestoreStorage::decode(saved.code.storage.as_bytes()).map_err(|error| {
        tracing::warn!(%store, %error, "saved restore code's location does not decode");
        UnreadableSavedCode::Format
    })?;
    // The S3 key is its own entry beside the code, which carries none.
    if storage.s3_key.is_some() {
        tracing::warn!(%store, "saved restore code carries an S3 key");
        return Err(UnreadableSavedCode::Format);
    }
    Ok((saved, storage.location))
}

/// The saved entry's bytes for `code` under `name`. Where the keychain limits
/// an entry, as Block Store does, a name too long to fit is saved shortened,
/// cut at a character boundary: the name only labels the entry, and restore
/// finds the store by its id (§13.1). A code that doesn't fit even with a
/// one-character name fails with `SavedEntryTooLarge`.
#[expect(
    clippy::map_err_ignore,
    reason = "the encoder fails only on a name past its bound or on memory: Encoding"
)]
fn encode(
    person: &PersonEntries,
    name: &str,
    named_at: u64,
    code: &RestoreCode,
) -> Result<SecretBytes, KeyError> {
    let encode = |name: &str| {
        KeychainRestoreCode::encode(name, named_at, code)
            .map_err(|_| KeyError::from(MaterialError::Encoding))
    };
    let whole = encode(name)?;
    let Some(limit) = person.restore_code_limit() else {
        return Ok(SecretBytes::new(whole.to_vec()));
    };
    if whole.len() <= limit {
        return Ok(SecretBytes::new(whole.to_vec()));
    }
    let room = limit
        .checked_sub(whole.len() - name.len())
        .ok_or(KeyError::SavedEntryTooLarge)?;
    let shortened = &name[..name.floor_char_boundary(room)];
    if shortened.is_empty() {
        return Err(KeyError::SavedEntryTooLarge);
    }
    tracing::info!(
        from = name.len(),
        to = shortened.len(),
        "saving the store's name shortened to fit the entry"
    );
    Ok(SecretBytes::new(encode(shortened)?.to_vec()))
}

#[cfg(test)]
#[path = "saved_code_tests.rs"]
mod tests;
