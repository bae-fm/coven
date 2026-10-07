//! Store-scoped key and identity operations, without exposing retained custody.

use super::{KeyError, MemberKeyCustody, StoreKeyCustody, StoreKeychain};
use crate::{CryptoError, MemberId, MemberKeys, SealError};
use coven_foundation::id_source::KeyId;
use std::sync::Arc;

/// Initializing this device's member identity failed (E11).
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// Custody already holds member keys.
    #[error("member identity already initialized")]
    AlreadyInitialized,
    /// Making the two key pairs failed.
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    /// Reading or persisting identity custody failed.
    #[error(transparent)]
    Custody(#[from] KeyError),
}

/// The store's key operations, retaining its injected custody and keychain.
/// Construction reads no keys; each operation unlocks only the custody it needs.
pub struct StoreCustody {
    keys: StoreKeys,
    identity: Arc<dyn MemberKeyCustody>,
    keychain: Arc<StoreKeychain>,
}

impl StoreCustody {
    /// Compose already chosen custody at the application opening root.
    pub fn new(
        keys: StoreKeys,
        identity: Arc<dyn MemberKeyCustody>,
        keychain: Arc<StoreKeychain>,
    ) -> Self {
        Self {
            keys,
            identity,
            keychain,
        }
    }

    /// Makes this member's two key pairs and puts them in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&mut self) -> Result<MemberId, IdentityError> {
        if self.identity.unlock()?.is_some() {
            return Err(IdentityError::AlreadyInitialized);
        }
        let keys = MemberKeys::generate()?;
        self.identity.persist(&keys)?;
        Ok(keys.member_id())
    }

    /// Remove kept store and circle keys. The identity and app secrets remain.
    pub fn forget_store_keys(&mut self) -> Result<(), KeyError> {
        self.keys.forget_store_keys()
    }

    /// Keeps an app secret under the keychain's device-only access policy.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.keychain.set_host_secret(name, value)
    }

    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.keychain.host_secret(name)
    }

    /// Deletes the secret; succeeds if it was never set.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.keychain.delete_host_secret(name)
    }

    /// Seal app data with the key selected by the caller's committed store state.
    pub fn seal_app_data(
        &self,
        key: KeyId,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, SealError> {
        self.keys.seal_app_data(key, plaintext, aad)
    }

    /// Decrypt app data using the key id in its authenticated header.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        self.keys.open_app_data(sealed, aad)
    }
}

/// Store and circle key custody with lazy app-data cryptography. No key is read
/// by construction, and decrypted key material is dropped after each call.
pub struct StoreKeys {
    custody: Arc<dyn StoreKeyCustody>,
}

impl StoreKeys {
    /// Retain the custody chosen at the application opening root.
    pub fn new(custody: Arc<dyn StoreKeyCustody>) -> Self {
        Self { custody }
    }

    /// Forget kept store keys, retaining the original custody failure if any.
    pub fn forget_store_keys(&mut self) -> Result<(), KeyError> {
        self.custody.forget()
    }

    /// Unlock custody and seal with exactly the selected key; a missing key fails.
    pub fn seal_app_data(
        &self,
        key: KeyId,
        plaintext: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, SealError> {
        let keys = self.custody.unlock()?.ok_or(SealError::NoStoreKeys)?;
        keys.seal_app_data(key, plaintext, aad)
    }

    /// Open app data with the retained key identified by its authenticated header.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError> {
        let keys = self.custody.unlock()?.ok_or(SealError::NoStoreKeys)?;
        keys.open_app_data(sealed, aad)
    }
}
