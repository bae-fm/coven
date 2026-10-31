//! The person's entries on Android, kept in Google's Block Store (§13.1).
//!
//! Block Store keeps a few small entries for the app on this phone, and gives
//! them to the app on a new phone set up from it: copied straight across
//! during setup, or from this phone's cloud backup. It doesn't sync between
//! running phones, so each phone keeps its own entries.

use super::person_entries::restore_code_store;
use super::{KeyError, KeychainError};
use crate::SecretBytes;
use coven_foundation::id_source::StoreId;

/// Block Store's limit on one entry: its key and bytes together
/// (`BlockstoreClient.MAX_SIZE`).
pub(crate) const ENTRY_LIMIT: usize = 1024;

/// `BlockstoreStatusCodes.MAX_SIZE_EXCEEDED`: an entry past [`ENTRY_LIMIT`].
pub(crate) const MAX_SIZE_EXCEEDED: i32 = 40000;
/// `BlockstoreStatusCodes.TOO_MANY_ENTRIES`: a new key past the app's 16.
pub(crate) const TOO_MANY_ENTRIES: i32 = 40001;
/// `BlockstoreStatusCodes.FEATURE_NOT_SUPPORTED`.
pub(crate) const FEATURE_NOT_SUPPORTED: i32 = 40002;

/// A Block Store call that failed: the Java exception's class and message,
/// and the status code its `ApiException` carries, when it is one.
#[derive(Debug)]
pub(crate) struct BlockStoreFailure {
    /// The exception's class name.
    pub(crate) exception: String,
    /// The `ApiException` status code, when the call failed with one.
    pub(crate) status: Option<i32>,
    /// The exception's message, when it has one.
    pub(crate) message: Option<String>,
}

impl std::fmt::Display for BlockStoreFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Block Store call failed with {}", self.exception)?;
        if let Some(status) = self.status {
            write!(f, ", status {status}")?;
        }
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        Ok(())
    }
}

impl std::error::Error for BlockStoreFailure {}

/// Block Store's calls, as coven makes them. On Android they go through JNI to
/// Google Play services; tests give a fake.
pub(crate) trait BlockStoreApi: Send + Sync + 'static {
    /// Store `bytes` under `key`, replacing what the key held.
    fn store(
        &self,
        key: &str,
        bytes: &[u8],
        backup_to_cloud: bool,
    ) -> Result<(), BlockStoreFailure>;
    /// The bytes under `key`, or `None`.
    fn retrieve(&self, key: &str) -> Result<Option<SecretBytes>, BlockStoreFailure>;
    /// Every entry the app keeps, its own included.
    fn retrieve_all(&self) -> Result<Vec<(String, SecretBytes)>, BlockStoreFailure>;
    /// Delete the entry under `key`; an absent key succeeds.
    fn delete(&self, key: &str) -> Result<(), BlockStoreFailure>;
    /// Whether a copy in Google's cloud backup would be end-to-end encrypted:
    /// Android 9 or later, with a screen lock.
    fn end_to_end_encryption_available(&self) -> Result<bool, BlockStoreFailure>;
}

/// The person's entries in Block Store. Each is named
/// `<service>/<entry>:<store>`: Block Store has no service of its own, so the
/// name carries the one registered at startup, and the app's own Block Store
/// entries stay apart from coven's.
pub(crate) struct BlockStore<A> {
    api: A,
}

impl<A: BlockStoreApi> BlockStore<A> {
    pub(crate) fn new(api: A) -> Self {
        Self { api }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn api(&self) -> &A {
        &self.api
    }

    pub(crate) fn read(
        &self,
        service: &str,
        account: &str,
    ) -> Result<Option<SecretBytes>, KeyError> {
        self.api.retrieve(&key(service, account)).map_err(refused)
    }

    /// Store the entry, letting Block Store copy it to Google's cloud backup
    /// only when that copy is end-to-end encrypted: it holds the person's
    /// private keys. Each write asks again, so the choice follows the phone's
    /// screen lock.
    pub(crate) fn write(&self, service: &str, account: &str, bytes: &[u8]) -> Result<(), KeyError> {
        let backup_to_cloud = self.api.end_to_end_encryption_available().map_err(refused)?;
        tracing::debug!(account, backup_to_cloud, "storing a person entry in Block Store");
        self.api.store(&key(service, account), bytes, backup_to_cloud).map_err(refused)
    }

    pub(crate) fn delete(&self, service: &str, account: &str) -> Result<(), KeyError> {
        self.api.delete(&key(service, account)).map_err(refused)
    }

    /// Every saved restore code under `service`, with its store. Entries
    /// under another service, or the app's own, are left out.
    pub(crate) fn restore_codes(
        &self,
        service: &str,
    ) -> Result<Vec<(StoreId, SecretBytes)>, KeyError> {
        let prefix = format!("{service}/");
        let mut codes = Vec::new();
        for (name, bytes) in self.api.retrieve_all().map_err(refused)? {
            let Some(account) = name.strip_prefix(&prefix) else {
                tracing::debug!(name, "skipping a Block Store entry outside this service");
                continue;
            };
            if let Some(store) = restore_code_store(account)? {
                codes.push((store, bytes));
            }
        }
        Ok(codes)
    }

    /// The bytes an entry's value may take beside its name.
    pub(crate) fn value_limit(service: &str, account: &str) -> usize {
        ENTRY_LIMIT.saturating_sub(key(service, account).len())
    }
}

fn key(service: &str, account: &str) -> String {
    format!("{service}/{account}")
}

/// Block Store's own refusals get their own errors; any other failure keeps
/// the Java exception as the native cause (§12.4).
fn refused(failure: BlockStoreFailure) -> KeyError {
    match failure.status {
        Some(MAX_SIZE_EXCEEDED) => KeyError::SavedEntryTooLarge,
        Some(TOO_MANY_ENTRIES) => KeyError::SavedEntriesFull,
        Some(FEATURE_NOT_SUPPORTED) => KeyError::Unsupported,
        _ => KeychainError::from(keyring_core::Error::PlatformFailure(Box::new(failure))).into(),
    }
}

#[cfg(test)]
#[path = "block_store_tests.rs"]
mod tests;
