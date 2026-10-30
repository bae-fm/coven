//! Device credentials and the person's saved recovery values commit together.

use crate::{saved_code, SavedName, SyncError};
use coven_crypto::{
    custody::{PersonEntries, StoreKeychain},
    SecretBytes,
};
use coven_format::codes::RestoreCode;
use coven_storage::StorageCredentials;
use std::sync::Arc;

/// The keychain entries a credential change commits: this device's provider
/// credentials, and the person's restore code and S3 key that iCloud Keychain
/// carries to all their devices (§13.1).
pub struct CredentialKeychains {
    /// This device's own entries for the store.
    device: Arc<StoreKeychain>,
    /// The person's entries for the store, shared by all their devices.
    person: Arc<PersonEntries>,
}

impl CredentialKeychains {
    /// Pair one store's device-only entries and the person's entries.
    pub fn new(device: Arc<StoreKeychain>, person: Arc<PersonEntries>) -> Self {
        Self { device, person }
    }

    /// This device's committed provider credentials, if any.
    pub fn storage_credentials(&self) -> Result<Option<SecretBytes>, SyncError> {
        Ok(self.device.storage_credentials()?)
    }

    /// Remove this device's provider credentials; an absent entry succeeds.
    pub fn delete_storage_credentials(&self) -> Result<(), SyncError> {
        Ok(self.device.delete_storage_credentials()?)
    }

    /// Save the saved restore code with the store's name, or the entry's
    /// when that is newer, where the keychain keeps person entries (§13.1).
    pub fn refresh_restore_code(
        &self,
        code: &RestoreCode,
        name: &SavedName,
    ) -> Result<(), SyncError> {
        if self.person.supported() {
            saved_code::save(&self.person, code, name)?;
        }
        Ok(())
    }

    /// The saved location with this device's credentials; None when either is absent.
    pub fn connection(
        &self,
        settings: &coven_storage::StorageSettings,
    ) -> Result<Option<coven_storage::ConnectionCredentials>, SyncError> {
        crate::read_connection(settings, &self.device)
    }
}

#[derive(Clone, Copy)]
enum CredentialEntry {
    DeviceCredentials,
    RestoreCode,
    S3Credentials,
}

impl CredentialEntry {
    fn read(&self, keychains: &CredentialKeychains) -> Result<Option<SecretBytes>, SyncError> {
        Ok(match self {
            Self::DeviceCredentials => keychains.device.storage_credentials()?,
            Self::RestoreCode => keychains.person.restore_code()?,
            Self::S3Credentials => keychains.person.s3_credentials()?,
        })
    }

    fn write(&self, keychains: &CredentialKeychains, bytes: &SecretBytes) -> Result<(), SyncError> {
        match self {
            Self::DeviceCredentials => keychains.device.set_storage_credentials(bytes),
            Self::RestoreCode => keychains.person.set_restore_code(bytes),
            Self::S3Credentials => keychains.person.set_s3_credentials(bytes),
        }
        .map_err(SyncError::from)
    }

    fn delete(&self, keychains: &CredentialKeychains) -> Result<(), SyncError> {
        match self {
            Self::DeviceCredentials => keychains.device.delete_storage_credentials(),
            Self::RestoreCode => keychains.person.delete_restore_code(),
            Self::S3Credentials => keychains.person.delete_s3_credentials(),
        }
        .map_err(SyncError::from)
    }

    fn replace(
        &self,
        keychain: &CredentialKeychains,
        value: Option<&SecretBytes>,
    ) -> Result<(), SyncError> {
        match value {
            Some(bytes) => self.write(keychain, bytes),
            None => self.delete(keychain),
        }
    }
}

/// What a credential commit puts in one entry.
enum Update<'a> {
    /// Exactly these bytes, or nothing.
    Value(Option<SecretBytes>),
    /// The restore code, with the newer of this name and the entry's (§13.1).
    Code(&'a RestoreCode, &'a SavedName),
}

impl Update<'_> {
    fn apply(
        &self,
        entry: CredentialEntry,
        keychain: &CredentialKeychains,
    ) -> Result<(), SyncError> {
        match self {
            Self::Value(value) => entry.replace(keychain, value.as_ref()),
            Self::Code(code, name) => saved_code::save(&keychain.person, code, name),
        }
    }
}

/// Previous values of exactly the custody entries a credential commit changes.
/// Setup and bootstrap retain this until their whole operation has committed.
pub struct CredentialRollback {
    entries: Vec<(CredentialEntry, Option<SecretBytes>)>,
}

impl CredentialRollback {
    /// Restore every captured value, retaining all failures for the initiator.
    pub fn restore(self, keychain: &CredentialKeychains) -> Result<(), SyncError> {
        let mut failure = None;
        for (entry, bytes) in self.entries.into_iter().rev() {
            let result = entry.replace(keychain, bytes.as_ref());
            if let Err(error) = result {
                failure = Some(match failure {
                    Some(operation) => cleanup_error(operation, error),
                    None => error,
                });
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Commit device credentials and, on Apple, the separate S3 key. Setup and
/// bootstrap also supply the restore code with the store's name, and remove
/// any S3 entry when setting up an account provider. Replacing credentials
/// supplies no code; OAuth refresh touches neither person entry. A failed
/// write restores every prior value and returns any rollback failures with
/// the original cause.
pub fn commit_credentials(
    keychain: &CredentialKeychains,
    credentials: &StorageCredentials,
    code: Option<(&RestoreCode, &SavedName)>,
) -> Result<CredentialRollback, SyncError> {
    let mut updates = vec![(
        CredentialEntry::DeviceCredentials,
        Update::Value(Some(credentials.encode()?)),
    )];
    if keychain.person.supported() {
        if let StorageCredentials::S3(keys) = credentials {
            updates.push((
                CredentialEntry::S3Credentials,
                Update::Value(Some(keys.encode()?)),
            ));
        } else if code.is_some() {
            updates.push((CredentialEntry::S3Credentials, Update::Value(None)));
        }
        if let Some((code, name)) = code {
            updates.push((CredentialEntry::RestoreCode, Update::Code(code, name)));
        }
    }
    apply(keychain, updates)
}

/// Commit an account provider's person entries beside device credentials
/// already in place: the restore code with the store's name, and no S3 key.
/// A join publishes this way, its sign-in kept on the device since it saved
/// its invite code (§13.2). A failed write restores every prior value.
pub(crate) fn commit_account_code(
    keychain: &CredentialKeychains,
    code: &RestoreCode,
    name: &SavedName,
) -> Result<CredentialRollback, SyncError> {
    let updates = if keychain.person.supported() {
        vec![
            (CredentialEntry::S3Credentials, Update::Value(None)),
            (CredentialEntry::RestoreCode, Update::Code(code, name)),
        ]
    } else {
        Vec::new()
    };
    apply(keychain, updates)
}

/// Write each update in order, after reading the value each replaces. A
/// failed write restores every prior value and returns any rollback failure
/// with the original cause.
fn apply(
    keychain: &CredentialKeychains,
    updates: Vec<(CredentialEntry, Update<'_>)>,
) -> Result<CredentialRollback, SyncError> {
    let previous = CredentialRollback {
        entries: updates
            .iter()
            .map(|(entry, _)| Ok((*entry, entry.read(keychain)?)))
            .collect::<Result<_, SyncError>>()?,
    };
    for (entry, update) in updates {
        if let Err(operation) = update.apply(entry, keychain) {
            return Err(match previous.restore(keychain) {
                Ok(()) => operation,
                Err(cleanup) => cleanup_error(operation, cleanup),
            });
        }
    }
    Ok(previous)
}

pub(crate) fn cleanup_error(operation: SyncError, cleanup: SyncError) -> SyncError {
    SyncError::Cleanup {
        operation: Box::new(operation),
        cleanup: Box::new(cleanup),
    }
}

#[cfg(test)]
#[path = "credential_custody_tests.rs"]
mod tests;
