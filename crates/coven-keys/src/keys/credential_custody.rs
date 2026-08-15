use std::sync::{Arc, Mutex};

use super::{CloudHomeCredentials, KeyError, StoreKeys};

/// Retained access to one store's cloud-provider credentials.
///
/// Provider implementations read and refresh credentials through this
/// capability rather than reaching into the platform keyring. Cloud setup uses
/// [`StagedCloudHomeCredentials`] so provider construction and token refreshes
/// remain in memory until the complete cloud connection is ready to install.
pub trait CloudHomeCredentialCustody: Send + Sync {
    fn unlock(&self) -> Result<Option<CloudHomeCredentials>, KeyError>;

    fn persist(&self, credentials: &CloudHomeCredentials) -> Result<(), KeyError>;
}

/// The durable owner that issues one credential capability per provider
/// connection and rejects writes from providers replaced by a later setup.
#[derive(Clone)]
pub struct CloudHomeCredentialsOwner {
    destination: StoreKeys,
    epoch: Arc<Mutex<u64>>,
}

impl CloudHomeCredentialsOwner {
    pub fn new(destination: StoreKeys) -> Self {
        Self {
            destination,
            epoch: Arc::new(Mutex::new(0)),
        }
    }

    pub fn current(&self) -> Arc<dyn CloudHomeCredentialCustody> {
        let epoch = *self.epoch.lock().expect("lock cloud credential epoch");
        Arc::new(CloudHomeCredentialLease {
            owner: self.clone(),
            epoch,
        })
    }

    pub fn stage(&self, proposed: Option<CloudHomeCredentials>) -> Arc<StagedCloudHomeCredentials> {
        let base_epoch = *self.epoch.lock().expect("lock cloud credential epoch");
        Arc::new(StagedCloudHomeCredentials {
            owner: self.clone(),
            base_epoch,
            state: Mutex::new(StagedCredentialState::Proposed(proposed)),
        })
    }

    fn read_at(&self, expected_epoch: u64) -> Result<Option<CloudHomeCredentials>, KeyError> {
        let epoch = self.epoch.lock().expect("lock cloud credential epoch");
        if *epoch != expected_epoch {
            return Err(KeyError::CloudCredentialsSuperseded);
        }
        self.destination.get_cloud_home_credentials()
    }

    fn write_at(
        &self,
        expected_epoch: u64,
        credentials: &CloudHomeCredentials,
    ) -> Result<(), KeyError> {
        let epoch = self.epoch.lock().expect("lock cloud credential epoch");
        if *epoch != expected_epoch {
            return Err(KeyError::CloudCredentialsSuperseded);
        }
        self.destination.set_cloud_home_credentials(credentials)
    }
}

struct CloudHomeCredentialLease {
    owner: CloudHomeCredentialsOwner,
    epoch: u64,
}

impl CloudHomeCredentialCustody for CloudHomeCredentialLease {
    fn unlock(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        self.owner.read_at(self.epoch)
    }

    fn persist(&self, credentials: &CloudHomeCredentials) -> Result<(), KeyError> {
        self.owner.write_at(self.epoch, credentials)
    }
}

enum StagedCredentialState {
    Proposed(Option<CloudHomeCredentials>),
    Committed {
        previous: Option<CloudHomeCredentials>,
    },
    RolledBack,
}

/// One proposed cloud credential value and the durable value it would replace.
///
/// Clones of this object are the credential capability retained by a proposed
/// provider. Before [`commit`](Self::commit), provider refreshes update only the
/// proposed value. Commit writes the latest proposal to the keyring and makes
/// later refreshes durable. Rollback restores the exact value commit replaced.
pub struct StagedCloudHomeCredentials {
    owner: CloudHomeCredentialsOwner,
    base_epoch: u64,
    state: Mutex<StagedCredentialState>,
}

impl StagedCloudHomeCredentials {
    fn committed_epoch(&self) -> u64 {
        self.base_epoch
            .checked_add(1)
            .expect("cloud credential epoch overflow")
    }

    /// Persist the latest proposed value. Repeating commit after it succeeded is
    /// idempotent.
    pub fn commit(&self) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged cloud credentials");
        match &*state {
            StagedCredentialState::Proposed(credentials) => {
                let credentials = credentials.clone();
                let mut epoch = self
                    .owner
                    .epoch
                    .lock()
                    .expect("lock cloud credential epoch");
                if *epoch != self.base_epoch {
                    return Err(KeyError::CloudCredentialsSuperseded);
                }
                let previous = self.owner.destination.get_cloud_home_credentials()?;
                match &credentials {
                    Some(credentials) => self
                        .owner
                        .destination
                        .set_cloud_home_credentials(credentials)?,
                    None => self.owner.destination.delete_cloud_home_credentials()?,
                }
                *epoch = self.committed_epoch();
                *state = StagedCredentialState::Committed { previous };
                Ok(())
            }
            StagedCredentialState::Committed { .. } => Ok(()),
            StagedCredentialState::RolledBack => Err(KeyError::CloudCredentialsRolledBack {
                operation: "commit",
            }),
        }
    }

    /// Restore the durable value this proposal replaced. Repeating rollback is
    /// idempotent so every failing setup path can invoke it unconditionally.
    pub fn rollback(&self) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged cloud credentials");
        match &*state {
            StagedCredentialState::Proposed(_) => {
                let epoch = self
                    .owner
                    .epoch
                    .lock()
                    .expect("lock cloud credential epoch");
                if *epoch != self.base_epoch {
                    return Err(KeyError::CloudCredentialsSuperseded);
                }
                *state = StagedCredentialState::RolledBack;
                Ok(())
            }
            StagedCredentialState::Committed { previous } => {
                let mut epoch = self
                    .owner
                    .epoch
                    .lock()
                    .expect("lock cloud credential epoch");
                if *epoch != self.committed_epoch() {
                    return Err(KeyError::CloudCredentialsSuperseded);
                }
                match previous {
                    Some(previous) => self
                        .owner
                        .destination
                        .set_cloud_home_credentials(previous)?,
                    None => self.owner.destination.delete_cloud_home_credentials()?,
                }
                *epoch = self.base_epoch;
                *state = StagedCredentialState::RolledBack;
                Ok(())
            }
            StagedCredentialState::RolledBack => Ok(()),
        }
    }
}

impl CloudHomeCredentialCustody for StagedCloudHomeCredentials {
    fn unlock(&self) -> Result<Option<CloudHomeCredentials>, KeyError> {
        let state = self.state.lock().expect("lock staged cloud credentials");
        match &*state {
            StagedCredentialState::Proposed(credentials) => Ok(credentials.clone()),
            StagedCredentialState::Committed { .. } => self.owner.read_at(self.committed_epoch()),
            StagedCredentialState::RolledBack => Err(KeyError::CloudCredentialsRolledBack {
                operation: "unlock",
            }),
        }
    }

    fn persist(&self, credentials: &CloudHomeCredentials) -> Result<(), KeyError> {
        let mut state = self.state.lock().expect("lock staged cloud credentials");
        match &mut *state {
            StagedCredentialState::Proposed(proposed) => {
                *proposed = Some(credentials.clone());
                Ok(())
            }
            StagedCredentialState::Committed { .. } => {
                self.owner.write_at(self.committed_epoch(), credentials)
            }
            StagedCredentialState::RolledBack => Err(KeyError::CloudCredentialsRolledBack {
                operation: "persist refreshed",
            }),
        }
    }
}

#[cfg(test)]
#[path = "credential_custody_tests.rs"]
mod tests;
