//! Member identity and host secrets, without exposing retained custody.

use super::{KeyError, KeySession, StoreKeychain};
use crate::{CryptoError, MemberIdentity, StoreKeyring};
use coven_foundation::id_source::MemberId;
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
    Custody(KeyError),
    /// Initializing the identity panicked: a bug, which the caller reports
    /// (E15).
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
}

impl From<KeyError> for IdentityError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::Custody(error),
        }
    }
}

impl coven_foundation::task::CarriesBug for IdentityError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            Self::Custody(error) => error.bug(),
            Self::AlreadyInitialized | Self::Crypto(_) => None,
        }
    }
}

/// The store's identity and host secrets, retaining their custody and keychain.
/// Shares the unlocked sessions used by the handle and its workers.
pub struct StoreCustody {
    identity: Arc<KeySession<MemberIdentity>>,
    keys: Arc<KeySession<StoreKeyring>>,
    keychain: Arc<StoreKeychain>,
}

impl StoreCustody {
    /// Compose already chosen custody at the application opening root.
    pub fn new(
        keys: Arc<KeySession<StoreKeyring>>,
        identity: Arc<KeySession<MemberIdentity>>,
        keychain: Arc<StoreKeychain>,
    ) -> Self {
        Self { keys, identity, keychain }
    }

    /// Release both sessions after the handle has stopped their users.
    pub fn close(&self) {
        self.keys.close();
        self.identity.close();
    }

    /// Makes this member's two key pairs for the independently generated
    /// `member`, for the person creating a store, who then puts them in
    /// identity custody. Fails if custody already holds keys. It writes
    /// nothing, so a creation that fails here has nothing of its own to
    /// take back. Joining and restoring put their identity there
    /// themselves.
    pub fn new_identity(&self, member: MemberId) -> Result<MemberIdentity, IdentityError> {
        if self.identity.read()?.is_some() {
            return Err(IdentityError::AlreadyInitialized);
        }
        Ok(MemberIdentity::generate(member)?)
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

#[cfg(test)]
#[path = "store_custody_tests.rs"]
mod tests;
