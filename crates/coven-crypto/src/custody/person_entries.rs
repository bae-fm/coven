//! The person's entries for one store, which reach their next devices:
//! iCloud Keychain carries them to all their Apple devices, and Block Store
//! gives an Android phone's to a phone set up from it (§13.1).

use super::keychain::{EntryScope, Keychain};
use super::KeyError;
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;
use std::sync::{Arc, Mutex};

pub(super) const RESTORE_CODE_ENTRY: &str = "restore-code";
pub(super) const S3_CREDENTIALS_ENTRY: &str = "s3-credentials";

/// Held across every change of a person entry in this process, so a change
/// that reads the restore code and writes it back sees no other change in
/// between. The native keychain is one per process, and so is every handle,
/// discovery call or deletion that reaches the same entries.
static CHANGES: Mutex<()> = Mutex::new(());

fn changes() -> std::sync::MutexGuard<'static, ()> {
    CHANGES
        .lock()
        .expect("person entries changes lock poisoned")
}

/// What [`PersonEntries::change_restore_code`] does with the entry it read.
pub enum RestoreCodeChange {
    /// Leave the entry as it is, or absent.
    Keep,
    /// Replace the entry with these bytes.
    Write(SecretBytes),
    /// Delete the entry.
    Delete,
}

/// One store's restore code and the S3 key saved beside it, in iCloud
/// Keychain or Block Store. They belong to the person, not to this device:
/// every device on the person's Apple account sees them, and a phone set up
/// from this one receives them. So removing the store from one device keeps
/// them, and only the person leaving the store deletes them (§13.1, §14.1).
/// Every read, write and deletion here uses the person scope; this
/// device's own entries belong to [`StoreKeychain`](super::StoreKeychain).
pub struct PersonEntries {
    keychain: Arc<Keychain>,
    store: StoreId,
    /// Set once the person leaves; every later write is refused, so nothing
    /// sharing this keychain puts an entry back (§14.1).
    left: Mutex<bool>,
}

impl PersonEntries {
    /// Bind the injected keychain to one store without reading an entry (E1).
    pub fn new(keychain: Arc<Keychain>, store: StoreId) -> Self {
        Self {
            keychain,
            store,
            left: Mutex::new(false),
        }
    }

    /// The store whose entries these are.
    pub fn store(&self) -> StoreId {
        self.store
    }

    fn account(&self, name: &str) -> String {
        format!("{name}:{}", self.store)
    }
    fn read(&self, name: &str) -> Result<Option<SecretBytes>, KeyError> {
        self.keychain.read(EntryScope::Person, &self.account(name))
    }
    fn write(&self, name: &str, bytes: &[u8]) -> Result<(), KeyError> {
        // Held through the write, so leaving can't run in between.
        let left = self.left.lock().expect("person entries lock poisoned");
        if *left {
            return Err(KeyError::Left);
        }
        let _changes = changes();
        self.keychain
            .write(EntryScope::Person, &self.account(name), bytes)
    }
    fn remove(&self, name: &str) -> Result<(), KeyError> {
        let _changes = changes();
        self.keychain
            .delete(EntryScope::Person, &self.account(name))
    }

    /// Whether this keychain keeps person entries: Apple keychains, Block
    /// Store on Android, and the memory backend on every test platform.
    pub fn supported(&self) -> bool {
        self.keychain.supports_person_entries()
    }

    /// The most bytes the saved restore code may take, or `None` when the
    /// keychain sets no limit it could reach. Block Store holds 1,024 bytes
    /// for an entry, its name included (§13.1).
    pub fn restore_code_limit(&self) -> Option<usize> {
        self.keychain
            .person_value_limit(&self.account(RESTORE_CODE_ENTRY))
    }

    /// Keep exactly these bytes as the saved restore code (§13.1), whatever
    /// the entry held. The caller encodes the code with the store's name; the
    /// crypto crate does not define that format.
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn set_restore_code(&self, code: &SecretBytes) -> Result<(), KeyError> {
        self.write(RESTORE_CODE_ENTRY, code.as_bytes())
    }

    /// Read the saved restore code's bytes, or `None` if absent (§13.1).
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn restore_code(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.read(RESTORE_CODE_ENTRY)
    }

    /// Read the saved restore code and keep, replace or delete it as `change`
    /// decides from what it read, with no other change of a person entry in
    /// this process in between. A name rewrite keeps the code it read this
    /// way, and a code saved meanwhile is never overwritten by an older one
    /// (§13.1). `change` sees every keychain group's copy: more than one is
    /// `PersonRestoreCode::Ambiguous`. A write leaves exactly one copy: it
    /// writes the first group's and then deletes the others, so a failure
    /// part way leaves the entry ambiguous, never absent, and repeating the
    /// change finishes. A write after leaving returns `KeyError::Left`.
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn change_restore_code<E: From<KeyError>>(
        &self,
        change: impl FnOnce(Option<PersonRestoreCode>) -> Result<RestoreCodeChange, E>,
    ) -> Result<(), E> {
        let left = self.left.lock().expect("person entries lock poisoned");
        let _changes = changes();
        let account = self.account(RESTORE_CODE_ENTRY);
        let mut copies = self.keychain.person_copies(&account)?;
        let saved = match copies.len() {
            0 => None,
            1 => copies.pop().map(PersonRestoreCode::Saved),
            _ => Some(PersonRestoreCode::Ambiguous),
        };
        match change(saved)? {
            RestoreCodeChange::Keep => Ok(()),
            RestoreCodeChange::Write(_) if *left => Err(KeyError::Left.into()),
            RestoreCodeChange::Write(bytes) => Ok(self
                .keychain
                .replace_person_entry(&account, bytes.as_bytes())?),
            RestoreCodeChange::Delete => Ok(self.keychain.delete(EntryScope::Person, &account)?),
        }
    }

    /// Save a copy of the restore code in a second keychain group beside this
    /// store's entry, so listing finds the store ambiguous. Requires an
    /// in-memory keychain.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_restore_code_in_another_group(&self, code: &SecretBytes) {
        self.keychain
            .write_in_another_group(&self.account(RESTORE_CODE_ENTRY), code.as_bytes());
    }

    /// Delete the saved restore code; succeeds if absent (§13.1).
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn delete_restore_code(&self) -> Result<(), KeyError> {
        self.remove(RESTORE_CODE_ENTRY)
    }

    /// Keep a separately encoded S3 key beside the code.
    /// Storage owns the encoding; this entry never contains the restore code.
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn set_s3_credentials(&self, credentials: &SecretBytes) -> Result<(), KeyError> {
        self.write(S3_CREDENTIALS_ENTRY, credentials.as_bytes())
    }

    /// Read the separately held S3 key, or `None` when the person must enter it.
    pub fn s3_credentials(&self) -> Result<Option<SecretBytes>, KeyError> {
        self.read(S3_CREDENTIALS_ENTRY)
    }

    /// Remove the S3 key saved beside the code; absent entries succeed.
    pub fn delete_s3_credentials(&self) -> Result<(), KeyError> {
        self.remove(S3_CREDENTIALS_ENTRY)
    }

    /// Delete the store's S3 key, then its restore code, from every one of
    /// the person's devices, without leaving: the store, its storage and every
    /// device's copy stay, and a later saved code is kept (§13.1). The code
    /// goes last, because it is what `saved_restore_codes` lists: a failure
    /// part way leaves the store listed, never an unlisted key. Absent entries
    /// succeed, so repeating after a failure is safe.
    /// Keychains with neither iCloud Keychain nor Block Store return `KeyError::Unsupported`.
    pub fn forget(&self) -> Result<(), KeyError> {
        self.delete_s3_credentials()?;
        self.delete_restore_code()
    }

    /// The person left the store: forget its entries (§14.1). A keychain
    /// without person entries holds neither, and nothing is deleted. From the
    /// first call on, writes return `KeyError::Left`, even if this deletion
    /// fails part way.
    pub fn delete_entries(&self) -> Result<(), KeyError> {
        let mut left = self.left.lock().expect("person entries lock poisoned");
        *left = true;
        if !self.supported() {
            return Ok(());
        }
        self.forget()
    }
}

impl std::fmt::Debug for PersonEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PersonEntries([REDACTED])")
    }
}

/// One store's saved restore-code entry, as listing found it (§13.1).
pub enum PersonRestoreCode {
    /// The entry's bytes.
    Saved(SecretBytes),
    /// More than one of the app's keychain groups holds an entry for the
    /// store. Reading by store id could give either; deleting by store id
    /// removes them all.
    Ambiguous,
}

impl std::fmt::Debug for PersonRestoreCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Saved(_) => f.write_str("Saved([REDACTED])"),
            Self::Ambiguous => f.write_str("Ambiguous"),
        }
    }
}

/// The store named by a saved restore-code account; `None` for other entries.
/// The id must be spelled as [`PersonEntries`] writes it: an account it never
/// writes is one no store id reads or deletes.
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "android",
    test,
    feature = "test-utils"
))]
pub(crate) fn restore_code_store(account: &str) -> Result<Option<StoreId>, KeyError> {
    match account.split_once(':') {
        Some((RESTORE_CODE_ENTRY, id)) => uuid::Uuid::parse_str(id)
            .ok()
            .map(StoreId)
            .filter(|store| store.to_string() == id)
            .map(Some)
            .ok_or(KeyError::RestoreCodeStoreId),
        _ => {
            tracing::debug!(account, "skipping person entry that is not a restore code");
            Ok(None)
        }
    }
}

#[cfg(test)]
#[path = "person_entries_tests.rs"]
mod tests;
