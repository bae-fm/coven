use crate::{CloudHomeConfig, CloudHomeKeyState, SyncError};

/// A cloud-home setup that Coven has connected and committed.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectedCloudHome {
    pub cloud_home: CloudHomeConfig,
    pub key_state: CloudHomeKeyState,
}

/// Which durable key material could not be restored after setup failed.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeRollbackError {
    #[error("restore cloud-home credentials: {0}")]
    Credentials(#[source] coven_keys::keys::KeyError),
    #[error("restore cloud-home master key: {0}")]
    MasterKey(#[source] coven_keys::keys::KeyError),
    #[error("restore cloud-home credentials: {credentials}; restore master key: {master_key}")]
    Both {
        #[source]
        credentials: coven_keys::keys::KeyError,
        master_key: coven_keys::keys::KeyError,
    },
}

/// Why a proposed cloud home was not installed.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeSetupError {
    #[error("prepare cloud-home master key: {0}")]
    MasterKey(#[source] Box<coven_keys::keys::MasterKeyError>),
    #[error("prepare cloud-home connection: {0}")]
    Connection(#[source] Box<SyncError>),
    #[error("commit cloud-home {subject}: {source}")]
    Commit {
        subject: &'static str,
        #[source]
        source: Box<coven_keys::keys::KeyError>,
    },
    #[error("{failure}; rollback also failed: {rollback}")]
    Rollback {
        failure: Box<CloudHomeSetupError>,
        #[source]
        rollback: Box<CloudHomeRollbackError>,
    },
}

impl CloudHomeSetupError {
    pub(crate) fn with_rollback(self, rollback: Result<(), CloudHomeRollbackError>) -> Self {
        match rollback {
            Ok(()) => self,
            Err(rollback) => Self::Rollback {
                failure: Box::new(self),
                rollback: Box::new(rollback),
            },
        }
    }
}
