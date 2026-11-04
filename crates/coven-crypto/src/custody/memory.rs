//! Custody for one session only (E1).

use coven_foundation::locks::lock_unpoisoned;
use std::sync::Mutex;

/// In-memory custody for `StoreKeyring` or `MemberIdentity`, erased with the session.
pub struct InMemoryCustody<T> {
    secret: Mutex<Option<T>>,
}

impl<T: Clone> InMemoryCustody<T> {
    /// Start without keys; bootstrap fills custody only after opening sealed keys.
    pub fn empty() -> Self {
        Self { secret: Mutex::new(None) }
    }

    /// Retain the supplied keys for this session (E1).
    pub fn new(secret: T) -> Self {
        Self { secret: Mutex::new(Some(secret)) }
    }

    pub(crate) fn read(&self) -> Option<T> {
        // The custody traits return owned keys while custody keeps its copy.
        lock_unpoisoned(&self.secret).clone()
    }

    pub(crate) fn write(&self, secret: &T) {
        *lock_unpoisoned(&self.secret) = Some(secret.clone());
    }

    pub(crate) fn remove(&self) {
        *lock_unpoisoned(&self.secret) = None;
    }
}

impl<T> std::fmt::Debug for InMemoryCustody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InMemoryCustody([REDACTED])")
    }
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;
