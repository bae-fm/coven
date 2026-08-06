//! The custody presets, over whichever secret a policy protects.
//!
//! A store holds two secrets under custody — its master keyring and its device
//! signing identity — and protects them the same three ways: from the OS
//! keyring, from a value supplied for the session, or from a passphrase-wrapped
//! file in the store directory. Only the keyring preset differs between them
//! (see [`KeyCustody::resolve`](crate::custody::KeyCustody::resolve) and
//! [`IdentityCustody::resolve`](crate::identity_custody::IdentityCustody::resolve)); the other two
//! differ only in which file the secret lives in and how it converts to bytes,
//! which is what [`CustodySecret`] names.
//!
//! Both stay behind their own public trait —
//! [`MasterKeyCustody`](crate::keys::MasterKeyCustody) and
//! [`DeviceIdentityCustody`](crate::keys::DeviceIdentityCustody) — because a host
//! implements one or the other for a specific secret, not a generic one.

use std::marker::PhantomData;
use std::sync::RwLock;

use crate::envelope::{Passphrase, PassphraseVault};
use crate::keys::KeyError;
use coven_foundation::store_dir::StoreDir;

/// A secret a store keeps under custody: where a passphrase-wrapped one is
/// written, and how it converts to and from the plaintext bytes the envelope
/// seals.
///
/// `from_bytes` is where a secret says what a well-formed one looks like — a
/// file that decrypts under the right passphrase but does not parse is a
/// [`KeyError::Crypto`], never a silently absent secret.
pub(crate) trait CustodySecret: Clone + Send + Sync + Sized + 'static {
    /// The file under the store directory the passphrase preset wraps it in.
    const FILE: &'static str;

    /// The secret's plaintext bytes, as `from_bytes` will read them back.
    fn to_bytes(&self) -> Vec<u8>;

    /// Recover the secret from the plaintext an unlock produced.
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, KeyError>;
}

/// Supplied for this session and never persisted by coven.
pub(crate) struct InMemoryCustody<T> {
    secret: RwLock<Option<T>>,
}

impl<T: CustodySecret> InMemoryCustody<T> {
    pub(crate) fn new(seed: T) -> Self {
        Self {
            secret: RwLock::new(Some(seed)),
        }
    }

    pub(crate) fn unlock(&self) -> Result<Option<T>, KeyError> {
        Ok(self.secret.read().unwrap().clone())
    }

    pub(crate) fn persist(&self, secret: &T) -> Result<(), KeyError> {
        *self.secret.write().unwrap() = Some(secret.clone());
        Ok(())
    }

    pub(crate) fn forget(&self) -> Result<(), KeyError> {
        *self.secret.write().unwrap() = None;
        Ok(())
    }
}

/// Argon2id over a [`Passphrase`] wraps the secret, via the shared
/// [`PassphraseVault`] — the wrapped blob is a JSON envelope in
/// [`T::FILE`](CustodySecret::FILE) under the store directory, not a keyring
/// entry. One envelope format for every secret, so the two cannot drift into
/// wire formats that only happen to agree.
pub(crate) struct PassphraseCustody<T> {
    vault: PassphraseVault,
    secret: PhantomData<fn() -> T>,
}

impl<T: CustodySecret> PassphraseCustody<T> {
    pub(crate) fn new(passphrase: Passphrase, store_dir: &StoreDir) -> Self {
        Self {
            vault: PassphraseVault::new(passphrase, store_dir.join(T::FILE)),
            secret: PhantomData,
        }
    }

    pub(crate) fn unlock(&self) -> Result<Option<T>, KeyError> {
        let Some(plaintext) = self.vault.unlock()? else {
            return Ok(None);
        };
        T::from_bytes(plaintext).map(Some)
    }

    pub(crate) fn persist(&self, secret: &T) -> Result<(), KeyError> {
        self.vault.persist(&secret.to_bytes())
    }

    pub(crate) fn forget(&self) -> Result<(), KeyError> {
        self.vault.forget()
    }
}
