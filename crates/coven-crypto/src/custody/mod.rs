//! Where this device keeps its opened keys and its member's two key pairs (E1).

use crate::{MemberIdentity, StoreKeyring};
use std::sync::Arc;

mod error;
mod key_session;
mod keychain;
mod memory;
mod passphrase;
mod platform;
mod store_custody;
mod synced_keychain;

pub use error::{KeyError, KeychainError};
pub use key_session::KeySession;
pub use keychain::{set_keyring_service, Keychain, KeyringCustody, StoreKeychain};
pub use memory::InMemoryCustody;
pub use passphrase::{Passphrase, PassphraseCustody};
pub use store_custody::{IdentityError, StoreCustody};
pub use synced_keychain::{RestoreCodeChange, SyncedKeychain};

const STORE_KEYS_ENTRY: &str = "store-keys";
const MEMBER_KEYS_ENTRY: &str = "member-keys";

/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// Called once at open; `None` when this device has never held keys.
    /// Retain the unlocked persistence capability until close.
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError>;
    /// Keeps `keyring`, replacing what was kept.
    fn persist(&self, keyring: &StoreKeyring) -> Result<(), KeyError>;
    /// Removes the keys; succeeds if none were kept.
    fn forget(&self) -> Result<(), KeyError>;
    /// Erase held memory after users finish; performs no persistence or IO.
    fn close(&self);
}

/// The app's own store for the stable member id and its held private pair.
pub trait MemberKeyCustody: Send + Sync {
    /// Called once at open; persistence reuses the held session capability.
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError>;
    /// Keeps the member's identity, replacing what was kept.
    fn persist(&self, identity: &MemberIdentity) -> Result<(), KeyError>;
    /// Removes the member's identity; succeeds if none was kept.
    fn forget(&self) -> Result<(), KeyError>;
    /// Erase held memory after users finish; performs no persistence or IO.
    fn close(&self);
}

/// Where this device keeps every store and circle key it has opened (E1).
pub enum KeyCustody {
    /// The OS keychain under the service registered at startup.
    Keyring,
    /// A file sealed with a key derived from this passphrase.
    Passphrase(Passphrase),
    /// Keys retained for this session only; starts empty.
    InMemory,
    /// The app's own store for opened keys.
    Custom(Arc<dyn StoreKeyCustody>),
}

/// Where this device keeps its stable member id and private key pairs (E1).
pub enum IdentityCustody {
    /// The OS keychain under the service registered at startup.
    Keyring,
    /// A file sealed with a key derived from this passphrase.
    Passphrase(Passphrase),
    /// Member keys retained for this session only; starts empty.
    InMemory,
    /// The app's own store for member keys.
    Custom(Arc<dyn MemberKeyCustody>),
}

impl std::fmt::Debug for KeyCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keyring => "KeyCustody::Keyring",
            Self::Passphrase(_) => "KeyCustody::Passphrase([REDACTED])",
            Self::InMemory => "KeyCustody::InMemory",
            Self::Custom(_) => "KeyCustody::Custom(..)",
        })
    }
}

impl std::fmt::Debug for IdentityCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Keyring => "IdentityCustody::Keyring",
            Self::Passphrase(_) => "IdentityCustody::Passphrase([REDACTED])",
            Self::InMemory => "IdentityCustody::InMemory",
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
    fn close(&self) {
        self.remove();
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
    fn close(&self) {
        self.release();
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
    fn close(&self) {}
}

impl MemberKeyCustody for InMemoryCustody<MemberIdentity> {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        Ok(self.read())
    }
    fn persist(&self, keys: &MemberIdentity) -> Result<(), KeyError> {
        self.write(keys);
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

impl MemberKeyCustody for PassphraseCustody<MemberIdentity> {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        self.read(MEMBER_KEYS_ENTRY)?
            .map(|bytes| {
                MemberIdentity::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from)
            })
            .transpose()
    }
    fn persist(&self, keys: &MemberIdentity) -> Result<(), KeyError> {
        self.write(MEMBER_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove()
    }
    fn close(&self) {
        self.release();
    }
}

impl MemberKeyCustody for KeyringCustody<MemberIdentity> {
    fn unlock(&self) -> Result<Option<MemberIdentity>, KeyError> {
        self.read(MEMBER_KEYS_ENTRY)?
            .map(|bytes| {
                MemberIdentity::from_secret_bytes(bytes.as_bytes()).map_err(KeyError::from)
            })
            .transpose()
    }
    fn persist(&self, keys: &MemberIdentity) -> Result<(), KeyError> {
        self.write(MEMBER_KEYS_ENTRY, keys.to_secret_bytes().as_bytes())
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.remove(MEMBER_KEYS_ENTRY)
    }
    fn close(&self) {}
}
#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
