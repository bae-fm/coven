//! Device credentials and separately synced recovery values commit together.

use crate::SyncError;
use coven_crypto::{custody::StoreKeychain, SecretBytes};
use coven_format::codes::RestoreCode;
use coven_storage::StorageCredentials;

#[derive(Clone, Copy)]
enum CredentialEntry {
    DeviceCredentials,
    RestoreCode,
    S3Credentials,
}

impl CredentialEntry {
    fn read(&self, keychain: &StoreKeychain) -> Result<Option<SecretBytes>, SyncError> {
        Ok(match self {
            Self::DeviceCredentials => keychain.storage_credentials()?,
            Self::RestoreCode => keychain.synced_restore_code()?,
            Self::S3Credentials => keychain.synced_s3_credentials()?,
        })
    }

    fn write(&self, keychain: &StoreKeychain, bytes: &SecretBytes) -> Result<(), SyncError> {
        match self {
            Self::DeviceCredentials => keychain.set_storage_credentials(bytes),
            Self::RestoreCode => keychain.set_synced_restore_code(bytes),
            Self::S3Credentials => keychain.set_synced_s3_credentials(bytes),
        }
        .map_err(SyncError::from)
    }

    fn delete(&self, keychain: &StoreKeychain) -> Result<(), SyncError> {
        match self {
            Self::DeviceCredentials => keychain.delete_storage_credentials(),
            Self::RestoreCode => keychain.delete_synced_restore_code(),
            Self::S3Credentials => keychain.delete_synced_s3_credentials(),
        }
        .map_err(SyncError::from)
    }

    fn replace(
        &self,
        keychain: &StoreKeychain,
        value: Option<&SecretBytes>,
    ) -> Result<(), SyncError> {
        match value {
            Some(bytes) => self.write(keychain, bytes),
            None => self.delete(keychain),
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
    pub fn restore(self, keychain: &StoreKeychain) -> Result<(), SyncError> {
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
/// bootstrap also supply the restore code and remove any S3 entry when setting
/// up an account provider. Replacing credentials supplies no
/// code; OAuth refresh touches neither synced entry. A failed write restores
/// every prior value and returns any rollback failures with the original cause.
pub fn commit_credentials(
    keychain: &StoreKeychain,
    credentials: &StorageCredentials,
    code: Option<&RestoreCode>,
) -> Result<CredentialRollback, SyncError> {
    let mut updates = vec![(
        CredentialEntry::DeviceCredentials,
        Some(credentials.encode()?),
    )];
    if keychain.supports_synced_entries() {
        if let StorageCredentials::S3(keys) = credentials {
            updates.push((CredentialEntry::S3Credentials, Some(keys.encode()?)));
        } else if code.is_some() {
            updates.push((CredentialEntry::S3Credentials, None));
        }
        if let Some(code) = code {
            updates.push((
                CredentialEntry::RestoreCode,
                Some(SecretBytes::new(code.to_bytes()?.to_vec())),
            ));
        }
    }
    let previous = CredentialRollback {
        entries: updates
            .iter()
            .map(|(entry, _)| Ok((*entry, entry.read(keychain)?)))
            .collect::<Result<_, SyncError>>()?,
    };
    for (entry, bytes) in updates {
        if let Err(operation) = entry.replace(keychain, bytes.as_ref()) {
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
