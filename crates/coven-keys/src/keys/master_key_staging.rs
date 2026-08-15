use std::sync::{Arc, Mutex};

use crate::encryption::MasterKeyring;

use super::{KeyError, MasterKeyCustody, MasterKeyError};

enum StagedMasterKeyState {
    Proposed(MasterKeyring),
    Committed,
    RolledBack,
}

/// A generated master key retained in memory until cloud setup commits.
pub struct StagedMasterKeyCustody {
    destination: Arc<dyn MasterKeyCustody>,
    state: Mutex<StagedMasterKeyState>,
}

impl StagedMasterKeyCustody {
    pub fn new(
        destination: Arc<dyn MasterKeyCustody>,
        proposed: MasterKeyring,
    ) -> Result<Arc<Self>, MasterKeyError> {
        if destination.unlock()?.is_some() {
            return Err(MasterKeyError::AlreadyEstablished);
        }
        Ok(Arc::new(Self {
            destination,
            state: Mutex::new(StagedMasterKeyState::Proposed(proposed)),
        }))
    }

    pub fn commit(&self) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged master key");
        match &*state {
            StagedMasterKeyState::Proposed(keyring) => {
                self.destination.persist(keyring)?;
                *state = StagedMasterKeyState::Committed;
                Ok(())
            }
            StagedMasterKeyState::Committed => Ok(()),
            StagedMasterKeyState::RolledBack => Err(KeyError::MasterKeySetupRolledBack {
                operation: "commit",
            }),
        }
    }

    pub fn rollback(&self) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged master key");
        match &*state {
            StagedMasterKeyState::Proposed(_) => {
                *state = StagedMasterKeyState::RolledBack;
                Ok(())
            }
            StagedMasterKeyState::Committed => {
                self.destination.forget()?;
                *state = StagedMasterKeyState::RolledBack;
                Ok(())
            }
            StagedMasterKeyState::RolledBack => Ok(()),
        }
    }
}

impl MasterKeyCustody for StagedMasterKeyCustody {
    fn unlock(&self) -> Result<Option<MasterKeyring>, KeyError> {
        let state = self.state.lock().expect("lock staged master key");
        match &*state {
            StagedMasterKeyState::Proposed(keyring) => Ok(Some(keyring.clone())),
            StagedMasterKeyState::Committed => self.destination.unlock(),
            StagedMasterKeyState::RolledBack => Err(KeyError::MasterKeySetupRolledBack {
                operation: "unlock",
            }),
        }
    }

    fn persist(&self, keyring: &MasterKeyring) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged master key");
        match &mut *state {
            StagedMasterKeyState::Proposed(proposed) => {
                *proposed = keyring.clone();
                Ok(())
            }
            StagedMasterKeyState::Committed => self.destination.persist(keyring),
            StagedMasterKeyState::RolledBack => Err(KeyError::MasterKeySetupRolledBack {
                operation: "persist",
            }),
        }
    }

    fn forget(&self) -> Result<(), KeyError> {
        self.rollback()
    }
}

#[cfg(test)]
#[path = "master_key_staging_tests.rs"]
mod tests;
