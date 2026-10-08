//! Member identity and host secrets, without exposing retained custody.

use super::{KeyError, MemberKeyCustody, StoreKeychain};
use crate::{CryptoError, MemberId, MemberKeys};
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

/// The store's identity and host secrets, retaining their custody and keychain.
/// Construction reads no keys; each operation unlocks only the custody it needs.
pub struct StoreCustody {
    identity: Arc<dyn MemberKeyCustody>,
    keychain: Arc<StoreKeychain>,
}

impl StoreCustody {
    /// Compose already chosen custody at the application opening root.
    pub fn new(identity: Arc<dyn MemberKeyCustody>, keychain: Arc<StoreKeychain>) -> Self {
        Self { identity, keychain }
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

    /// Records the name before saving the value in its own device-only entry,
    /// so deleting the store can remove every saved secret. Names are arbitrary
    /// strings; each value retains the platform's per-entry size limit.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError> {
        self.keychain.set_host_secret(name, value)
    }

    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError> {
        self.keychain.host_secret(name)
    }

    /// Deletes the secret before its recorded name; succeeds if absent.
    /// Failure may leave an extra name; retrying is safe.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError> {
        self.keychain.delete_host_secret(name)
    }
}
