//! Discover and forget saved recovery choices without exposing their private
//! keys (§13.1).

#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
use crate::CloudProvider;
use crate::{BootstrapError, CodeError, StorageConfig, StoreId};
#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
use coven_crypto::custody::{Keychain, SyncedKeychain};
use coven_format::codes::KeychainRestoreCode;
#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
use std::sync::Arc;

/// A store whose restore code is saved in iCloud Keychain, without secrets.
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

/// List the registered service's iCloud Keychain restore codes (§13.1).
/// Returns one entry per store, ordered by store id, or an empty list.
/// Read failures, malformed codes and ambiguous entries fail the whole call.
/// Native non-Apple platforms return `BootstrapError::SecureStorage(KeyError::Unsupported)`.
/// Pass the chosen entry to `restore_from_keychain`, which reads its current code and key.
pub async fn saved_restore_codes() -> Result<Vec<SavedRestoreCode>, BootstrapError> {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        crate::coven::blocking(|| list(Keychain::registered()?)).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        Err(crate::KeyError::Unsupported.into())
    }
}

/// Delete the store's saved S3 key, then its restore code, from iCloud
/// Keychain, and so from every one of the person's Apple devices (§13.1).
/// The store, its storage and every device's copy stay; no store need be open
/// and no provider connected. Absent entries succeed; after a failure part way
/// the store is still listed, and calling again finishes. A later join,
/// restore, reset or member-key replacement saves the entry again.
/// Native non-Apple platforms return `BootstrapError::SecureStorage(KeyError::Unsupported)`.
pub async fn forget_saved_restore_code(store: StoreId) -> Result<(), BootstrapError> {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        crate::coven::blocking(move || forget(Keychain::registered()?, store)).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        let _ = store;
        Err(crate::KeyError::Unsupported.into())
    }
}

#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
pub(crate) fn list(keychain: Arc<Keychain>) -> Result<Vec<SavedRestoreCode>, BootstrapError> {
    keychain
        .synced_restore_codes()?
        .into_iter()
        .map(|(store_id, bytes)| {
            let (saved, location) = decode(store_id, bytes.as_bytes())?;
            let has_s3_credentials = location.provider() == CloudProvider::S3
                && SyncedKeychain::new(keychain.clone(), store_id)
                    .s3_credentials()?
                    .is_some();
            Ok(SavedRestoreCode {
                name: saved.name,
                store_id,
                location,
                has_s3_credentials,
            })
        })
        .collect()
}

#[cfg(any(target_os = "macos", target_os = "ios", test, feature = "test-utils"))]
pub(crate) fn forget(keychain: Arc<Keychain>, store: StoreId) -> Result<(), BootstrapError> {
    Ok(SyncedKeychain::new(keychain, store).forget()?)
}

/// Read a saved entry for `store`, and its code's location.
pub(crate) fn decode(
    store: StoreId,
    bytes: &[u8],
) -> Result<(KeychainRestoreCode, StorageConfig), CodeError> {
    let saved = coven_sync::read_saved_code(store, bytes).map_err(|_| CodeError::Invalid)?;
    let location =
        StorageConfig::decode(saved.code.storage.as_bytes()).map_err(|_| CodeError::Invalid)?;
    Ok((saved, location))
}

#[cfg(test)]
#[path = "saved_restore_tests.rs"]
mod tests;
