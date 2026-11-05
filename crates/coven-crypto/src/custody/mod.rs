//! Where this device keeps its opened keys and its member's two key pairs (E1).

use crate::{MaterialError, MemberIdentity, SecretBytes, StoreKeyring};
use coven_foundation::files::StoreFile;
use std::sync::Arc;

#[cfg(any(target_os = "android", test, feature = "test-utils"))]
mod block_store;
#[cfg(target_os = "android")]
mod block_store_jni;
#[cfg(any(test, feature = "test-utils"))]
mod block_store_test_utils;
mod error;
mod key_session;
mod keychain;
mod memory;
mod passphrase;
mod person_entries;
mod platform;
mod store_custody;

pub use error::{KeyError, KeychainError};
pub use key_session::KeySession;
pub use keychain::{Keychain, KeyringCustody, StoreKeychain};
pub use memory::InMemoryCustody;
pub use passphrase::{Passphrase, PassphraseCustody};
pub use person_entries::{PersonEntries, PersonRestoreCode, RestoreCodeChange};
pub use store_custody::{IdentityError, StoreCustody};

mod sealed {
    pub trait Sealed {}
}

/// What custody keeps: the store keys or the member's identity. Sealed: coven
/// alone says how each is named and encoded.
pub trait KeyMaterial: sealed::Sealed + Clone + Send + Sync + 'static {
    /// The keychain entry and passphrase file entry it is kept under.
    const ENTRY: &'static str;
    /// The store file a passphrase custody seals it in.
    const FILE: StoreFile;
    /// Encode it for custody.
    fn to_secret_bytes(&self) -> SecretBytes;
    /// Decode what `to_secret_bytes` encoded.
    fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl sealed::Sealed for StoreKeyring {}
impl sealed::Sealed for MemberIdentity {}

impl KeyMaterial for StoreKeyring {
    const ENTRY: &'static str = "store-keys";
    const FILE: StoreFile = StoreFile::StoreKeys;
    fn to_secret_bytes(&self) -> SecretBytes {
        Self::to_secret_bytes(self)
    }
    fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError> {
        Self::from_secret_bytes(bytes)
    }
}

impl KeyMaterial for MemberIdentity {
    const ENTRY: &'static str = "member-keys";
    const FILE: StoreFile = StoreFile::MemberKeys;
    fn to_secret_bytes(&self) -> SecretBytes {
        Self::to_secret_bytes(self)
    }
    fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError> {
        Self::from_secret_bytes(bytes)
    }
}

/// The app's own store for key material this device holds (E1).
pub trait Custody<T: KeyMaterial>: Send + Sync {
    /// Called once at open; `None` when this device has never held `T`.
    /// Retain the unlocked persistence capability until close.
    fn unlock(&self) -> Result<Option<T>, KeyError>;
    /// Keeps `material`, replacing what was kept.
    fn persist(&self, material: &T) -> Result<(), KeyError>;
    /// Removes what was kept; succeeds if nothing was kept.
    fn forget(&self) -> Result<(), KeyError>;
    /// Erase held memory after users finish; performs no persistence or IO.
    fn close(&self);
}

/// Where this device keeps `T` (E1).
pub enum CustodyChoice<T: KeyMaterial> {
    /// The OS keychain under the service registered at startup.
    Keyring,
    /// A file sealed with a key derived from this passphrase.
    Passphrase(Passphrase),
    /// Starts empty; holds only this handle's session: what opening, creation,
    /// restore or join puts there.
    InMemory,
    /// The app's own store for `T`.
    Custom(Arc<dyn Custody<T>>),
}

impl<T: KeyMaterial> std::fmt::Debug for CustodyChoice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keyring => "CustodyChoice::Keyring",
            Self::Passphrase(_) => "CustodyChoice::Passphrase([REDACTED])",
            Self::InMemory => "CustodyChoice::InMemory",
            Self::Custom(_) => "CustodyChoice::Custom(..)",
        })
    }
}

impl<T: KeyMaterial> Custody<T> for InMemoryCustody<T> {
    fn unlock(&self) -> Result<Option<T>, KeyError> {
        Ok(self.read())
    }
    fn persist(&self, material: &T) -> Result<(), KeyError> {
        self.write(material);
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove();
        Ok(())
    }
    fn close(&self) {
        self.remove();
    }
}

impl<T: KeyMaterial> Custody<T> for PassphraseCustody<T> {
    fn unlock(&self) -> Result<Option<T>, KeyError> {
        self.read(T::ENTRY)?
            .map(|bytes| T::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, material: &T) -> Result<(), KeyError> {
        self.write(T::ENTRY, material.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove()
    }
    fn close(&self) {
        self.release();
    }
}

impl<T: KeyMaterial> Custody<T> for KeyringCustody<T> {
    fn unlock(&self) -> Result<Option<T>, KeyError> {
        self.read(T::ENTRY)?
            .map(|bytes| T::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, material: &T) -> Result<(), KeyError> {
        self.write(T::ENTRY, material.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove(T::ENTRY)
    }
    fn close(&self) {}
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
