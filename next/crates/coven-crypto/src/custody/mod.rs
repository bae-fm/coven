//! Where this device keeps its opened keys and its member's two key pairs (§20.1).

use crate::{MemberKeys, StoreKeyring};
use std::sync::Arc;

mod error;
mod keychain;
mod memory;
mod passphrase;
mod platform;

pub use error::{KeyError, KeychainError, SecretNameError};
pub use keychain::{set_keyring_service, Keychain, KeyringCustody, StoreKeychain};
pub use memory::InMemoryCustody;
pub use passphrase::{Passphrase, PassphraseCustody};

const STORE_KEYS_ENTRY: &str = "store-keys";
const MEMBER_KEYS_ENTRY: &str = "member-keys";

/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// The keys, or `None` when this device has never held any.
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError>;
    /// Keeps `keyring`, replacing what was kept.
    fn persist(&self, keyring: &StoreKeyring) -> Result<(), KeyError>;
    /// Removes the keys; succeeds if none were kept.
    fn forget(&self) -> Result<(), KeyError>;
}

/// The app's own store for this member's keys.
pub trait MemberKeyCustody: Send + Sync {
    /// The member's keys, or `None` when none were kept.
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError>;
    /// Keeps the member's keys, replacing what was kept.
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError>;
    /// Removes the member's keys; succeeds if none were kept.
    fn forget(&self) -> Result<(), KeyError>;
}

/// Where this device keeps every store and circle key it has opened (§20.1).
pub enum KeyCustody {
    /// The OS keychain under the service registered at startup.
    Keyring,
    /// A file sealed with a key derived from this passphrase.
    Passphrase(Passphrase),
    /// Keys supplied for this session only.
    InMemory(StoreKeyring),
    /// The app's own store for opened keys.
    Custom(Arc<dyn StoreKeyCustody>),
}

/// Where this device keeps its member's signing and sealing key pairs (§20.1).
pub enum IdentityCustody {
    /// The OS keychain under the service registered at startup.
    Keyring,
    /// A file sealed with a key derived from this passphrase.
    Passphrase(Passphrase),
    /// Member keys supplied for this session only.
    InMemory(MemberKeys),
    /// The app's own store for member keys.
    Custom(Arc<dyn MemberKeyCustody>),
}

impl std::fmt::Debug for KeyCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keyring => "KeyCustody::Keyring",
            Self::Passphrase(_) => "KeyCustody::Passphrase([REDACTED])",
            Self::InMemory(_) => "KeyCustody::InMemory([REDACTED])",
            Self::Custom(_) => "KeyCustody::Custom(..)",
        })
    }
}

impl std::fmt::Debug for IdentityCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keyring => "IdentityCustody::Keyring",
            Self::Passphrase(_) => "IdentityCustody::Passphrase([REDACTED])",
            Self::InMemory(_) => "IdentityCustody::InMemory([REDACTED])",
            Self::Custom(_) => "IdentityCustody::Custom(..)",
        })
    }
}

impl StoreKeyCustody for InMemoryCustody<StoreKeyring> {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        Ok(self.read())
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        self.write(keys);
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove();
        Ok(())
    }
}

impl StoreKeyCustody for PassphraseCustody<StoreKeyring> {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.read(STORE_KEYS_ENTRY)?
            .map(|bytes| StoreKeyring::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        self.write(STORE_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove()
    }
}

impl StoreKeyCustody for KeyringCustody<StoreKeyring> {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.read(STORE_KEYS_ENTRY)?
            .map(|bytes| StoreKeyring::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        self.write(STORE_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove(STORE_KEYS_ENTRY)
    }
}

impl MemberKeyCustody for InMemoryCustody<MemberKeys> {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        Ok(self.read())
    }
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError> {
        self.write(keys);
        Ok(())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove();
        Ok(())
    }
}

impl MemberKeyCustody for PassphraseCustody<MemberKeys> {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        self.read(MEMBER_KEYS_ENTRY)?
            .map(|bytes| MemberKeys::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError> {
        self.write(MEMBER_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove()
    }
}

impl MemberKeyCustody for KeyringCustody<MemberKeys> {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        self.read(MEMBER_KEYS_ENTRY)?
            .map(|bytes| MemberKeys::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from))
            .transpose()
    }
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError> {
        self.write(MEMBER_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove(MEMBER_KEYS_ENTRY)
    }
}
#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
