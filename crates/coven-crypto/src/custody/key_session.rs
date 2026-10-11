//! One unlocked custody session shared by the handle's workers.

use super::{KeyError, MemberKeyCustody, StoreKeyCustody};
use crate::{MemberKeys, StoreKeyring};
use std::sync::{Arc, Mutex};

/// Held keys and the capability that saves them, released together on close.
/// Consumers read memory; only opening calls the configured custody's unlock.
/// Share this owner across every worker using the same custody.
pub struct KeySession<T> {
    held: Mutex<Option<Held<T>>>,
}

struct Held<T> {
    keys: Option<T>,
    custody: Box<dyn CustodyPersistence<T>>,
}

struct Custody<C: ?Sized>(Arc<C>);

trait CustodyPersistence<T>: Send + Sync {
    fn unlock(&self) -> Result<Option<T>, KeyError>;
    fn persist(&self, keys: &T) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
    fn close(&self);
}

impl<T: Clone> KeySession<T> {
    fn open(custody: Box<dyn CustodyPersistence<T>>) -> Result<Self, KeyError> {
        // The guard also closes a backend that fails partway through unlocking.
        let mut held = Held {
            keys: None,
            custody,
        };
        held.keys = held.custody.unlock()?;
        Ok(Self {
            held: Mutex::new(Some(held)),
        })
    }

    /// Read an independent copy of the held material without custody IO.
    pub fn read(&self) -> Result<Option<T>, KeyError> {
        let held = self.held.lock().expect("custody session lock poisoned");
        Ok(held.as_ref().ok_or(KeyError::StoreClosed)?.keys.clone())
    }

    /// Save changed keys before installing them in the shared session.
    /// A failed save leaves the held material unchanged and reaches the caller.
    pub fn persist(&self, keys: &T) -> Result<(), KeyError> {
        let mut held = self.held.lock().expect("custody session lock poisoned");
        let held = held.as_mut().ok_or(KeyError::StoreClosed)?;
        held.custody.persist(keys)?;
        held.keys = Some(keys.clone());
        Ok(())
    }

    /// Remove persisted keys before erasing the session's copy. A failed removal
    /// retains the held keys. The persistence capability lasts until close, so
    /// acquiring keys again does not require another passphrase derivation.
    pub fn forget(&self) -> Result<(), KeyError> {
        let mut held = self.held.lock().expect("custody session lock poisoned");
        let held = held.as_mut().ok_or(KeyError::StoreClosed)?;
        held.custody.forget()?;
        held.keys = None;
        Ok(())
    }

    /// Erase the held material and close custody after its users finish.
    /// Also happens when the last session owner is dropped. Performs no IO.
    pub fn close(&self) {
        self.held
            .lock()
            .expect("custody session lock poisoned")
            .take();
    }
}

impl KeySession<StoreKeyring> {
    /// Unlock store and circle custody once at the opening composition root.
    pub fn store(custody: Arc<dyn StoreKeyCustody>) -> Result<Self, KeyError> {
        Self::open(Box::new(Custody(custody)))
    }
}

impl KeySession<MemberKeys> {
    /// Unlock member custody once at the opening composition root.
    pub fn member(custody: Arc<dyn MemberKeyCustody>) -> Result<Self, KeyError> {
        Self::open(Box::new(Custody(custody)))
    }
}

impl<T> Drop for Held<T> {
    fn drop(&mut self) {
        self.keys.take();
        self.custody.close();
    }
}

impl<T> std::fmt::Debug for KeySession<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeySession([REDACTED])")
    }
}

impl CustodyPersistence<StoreKeyring> for Custody<dyn StoreKeyCustody> {
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError> {
        self.0.as_ref().unlock()
    }
    fn persist(&self, keys: &StoreKeyring) -> Result<(), KeyError> {
        self.0.as_ref().persist(keys)
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.0.as_ref().forget()
    }
    fn close(&self) {
        self.0.as_ref().close();
    }
}

impl CustodyPersistence<MemberKeys> for Custody<dyn MemberKeyCustody> {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError> {
        self.0.as_ref().unlock()
    }
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError> {
        self.0.as_ref().persist(keys)
    }
    fn forget(&self) -> Result<(), KeyError> {
        self.0.as_ref().forget()
    }
    fn close(&self) {
        self.0.as_ref().close();
    }
}

#[cfg(test)]
#[path = "key_session_tests.rs"]
mod tests;
