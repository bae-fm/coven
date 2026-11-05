//! One unlocked custody session shared by the handle's workers.

use super::{Custody, KeyError, KeyMaterial};
use coven_foundation::locks::lock_unpoisoned;
use std::sync::{Arc, Mutex};

/// Held keys and the capability that saves them, released together on close.
/// Consumers read memory; only opening calls the configured custody's unlock.
/// Share this owner across every worker using the same custody.
pub struct KeySession<T: KeyMaterial> {
    held: Mutex<Option<Held<T>>>,
}

struct Held<T: KeyMaterial> {
    keys: Option<T>,
    custody: Arc<dyn Custody<T>>,
}

impl<T: KeyMaterial> KeySession<T> {
    /// Unlock custody once at the opening composition root.
    pub fn open(custody: Arc<dyn Custody<T>>) -> Result<Self, KeyError> {
        // The guard also closes a backend that fails partway through unlocking.
        let mut held = Held { keys: None, custody };
        held.keys = held.custody.unlock()?;
        Ok(Self { held: Mutex::new(Some(held)) })
    }

    /// Read an independent copy of the held material without custody IO.
    pub fn read(&self) -> Result<Option<T>, KeyError> {
        let held = lock_unpoisoned(&self.held);
        Ok(held.as_ref().ok_or(KeyError::StoreClosed)?.keys.clone())
    }

    /// Save changed keys before installing them in the shared session.
    /// A failed save leaves the held material unchanged and reaches the caller.
    pub fn persist(&self, keys: &T) -> Result<(), KeyError> {
        let mut held = lock_unpoisoned(&self.held);
        let held = held.as_mut().ok_or(KeyError::StoreClosed)?;
        held.custody.persist(keys)?;
        held.keys = Some(keys.clone());
        Ok(())
    }

    /// Remove persisted keys before erasing the session's copy. A failed removal
    /// retains the held keys. The persistence capability lasts until close, so
    /// acquiring keys again does not require another passphrase derivation.
    pub fn forget(&self) -> Result<(), KeyError> {
        let mut held = lock_unpoisoned(&self.held);
        let held = held.as_mut().ok_or(KeyError::StoreClosed)?;
        held.custody.forget()?;
        held.keys = None;
        Ok(())
    }

    /// Erase the held material and close custody after its users finish.
    /// Also happens when the last session owner is dropped. Performs no IO.
    pub fn close(&self) {
        lock_unpoisoned(&self.held).take();
    }
}

impl<T: KeyMaterial> Drop for Held<T> {
    fn drop(&mut self) {
        self.keys.take();
        self.custody.close();
    }
}

impl<T: KeyMaterial> std::fmt::Debug for KeySession<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeySession([REDACTED])")
    }
}

#[cfg(test)]
#[path = "key_session_tests.rs"]
mod tests;
