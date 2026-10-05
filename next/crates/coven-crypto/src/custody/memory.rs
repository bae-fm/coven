//! Custody for one session only (§20.1).

use super::KeyError;
use std::sync::Mutex;

/// In-memory custody for `StoreKeyring` or `MemberKeys`, erased with the session.
pub struct InMemoryCustody<T> {
    secret: Mutex<Option<T>>,
}

impl<T: Clone> InMemoryCustody<T> {
    /// Retain the supplied keys for this session (§20.1).
    pub fn new(secret: T) -> Self {
        Self {
            secret: Mutex::new(Some(secret)),
        }
    }

    pub(crate) fn read(&self) -> Result<Option<T>, KeyError> {
        // The custody traits return owned keys while custody keeps its copy.
        Ok(self.secret.lock().map_err(|_| KeyError::Poisoned)?.clone())
    }

    pub(crate) fn write(&self, secret: &T) -> Result<(), KeyError> {
        *self.secret.lock().map_err(|_| KeyError::Poisoned)? = Some(secret.clone());
        Ok(())
    }

    pub(crate) fn remove(&self) -> Result<(), KeyError> {
        *self.secret.lock().map_err(|_| KeyError::Poisoned)? = None;
        Ok(())
    }
}

impl<T> std::fmt::Debug for InMemoryCustody<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InMemoryCustody([REDACTED])")
    }
}
