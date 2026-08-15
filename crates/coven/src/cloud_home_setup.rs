use crate::{CloudHomeConfig, CloudHomeKeyState, SyncError};

/// A cloud-home setup that Coven has connected and committed.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectedCloudHome {
    pub cloud_home: CloudHomeConfig,
    pub key_state: CloudHomeKeyState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloudHomeSetupFailure {
    Authentication,
    PermissionDenied,
    ContainerNotFound,
    RegionMismatch,
    QuotaExceeded,
    InvalidConfiguration,
    LocationOccupied,
    Network,
    SecureStorage,
    Internal,
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
    pub fn failure(&self) -> CloudHomeSetupFailure {
        match self {
            Self::MasterKey(_) | Self::Commit { .. } => CloudHomeSetupFailure::SecureStorage,
            Self::Connection(error) => classify_setup_error(error.as_ref()),
            Self::Rollback { failure, .. } => failure.failure(),
        }
    }

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

fn classify_backend_failure(
    failure: coven_protocol::objects::StorageBackendFailure,
) -> CloudHomeSetupFailure {
    use coven_protocol::objects::StorageBackendFailure;
    match failure {
        StorageBackendFailure::Authentication => CloudHomeSetupFailure::Authentication,
        StorageBackendFailure::PermissionDenied => CloudHomeSetupFailure::PermissionDenied,
        StorageBackendFailure::ContainerNotFound => CloudHomeSetupFailure::ContainerNotFound,
        StorageBackendFailure::RegionMismatch => CloudHomeSetupFailure::RegionMismatch,
        StorageBackendFailure::QuotaExceeded => CloudHomeSetupFailure::QuotaExceeded,
        StorageBackendFailure::Configuration => CloudHomeSetupFailure::InvalidConfiguration,
        StorageBackendFailure::Transport => CloudHomeSetupFailure::Network,
        StorageBackendFailure::Internal => CloudHomeSetupFailure::Internal,
    }
}

fn classify_setup_error(error: &(dyn std::error::Error + 'static)) -> CloudHomeSetupFailure {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<coven_keys::keys::KeyError>()
            .is_some()
        {
            return CloudHomeSetupFailure::SecureStorage;
        }
        if let Some(error) = source.downcast_ref::<coven_storage::cloud::CloudHomeError>() {
            match error {
                coven_storage::cloud::CloudHomeError::AlreadyExists(_)
                | coven_storage::cloud::CloudHomeError::SlotCollision(_) => {
                    return CloudHomeSetupFailure::LocationOccupied;
                }
                coven_storage::cloud::CloudHomeError::Configuration(_) => {
                    return CloudHomeSetupFailure::InvalidConfiguration;
                }
                coven_storage::cloud::CloudHomeError::Transport(_)
                | coven_storage::cloud::CloudHomeError::Io(_) => {
                    return CloudHomeSetupFailure::Network;
                }
                _ => {}
            }
            if let Some(failure) = error.backend_failure() {
                return classify_backend_failure(failure);
            }
        }
        if let Some(error) = source.downcast_ref::<coven_protocol::objects::StorageError>() {
            match error {
                coven_protocol::objects::StorageError::AlreadyExists(_)
                | coven_protocol::objects::StorageError::SlotCollision(_) => {
                    return CloudHomeSetupFailure::LocationOccupied;
                }
                coven_protocol::objects::StorageError::Configuration(_) => {
                    return CloudHomeSetupFailure::InvalidConfiguration;
                }
                coven_protocol::objects::StorageError::Key(_) => {
                    return CloudHomeSetupFailure::SecureStorage;
                }
                _ => {}
            }
            if let Some(failure) = error.backend_failure() {
                return classify_backend_failure(failure);
            }
        }
        current = source.source();
    }
    CloudHomeSetupFailure::Internal
}

/// Why a returning opaque cloud home could not be unlocked and connected.
#[derive(Debug, thiserror::Error)]
pub enum CloudHomeUnlockError {
    #[error("this cloud home does not require a master key")]
    KeyNotRequired,
    #[error("prepare imported cloud-home master key: {0}")]
    MasterKey(#[source] Box<coven_keys::keys::MasterKeyError>),
    #[error("prepare cloud-home connection: {0}")]
    Connection(#[source] Box<SyncError>),
    #[error("commit cloud-home master key: {0}")]
    Commit(#[source] Box<coven_keys::keys::KeyError>),
    #[error("{failure}; rollback also failed: {rollback}")]
    Rollback {
        failure: Box<CloudHomeUnlockError>,
        #[source]
        rollback: Box<coven_keys::keys::KeyError>,
    },
}

impl CloudHomeUnlockError {
    pub(crate) fn with_rollback(self, rollback: Result<(), coven_keys::keys::KeyError>) -> Self {
        match rollback {
            Ok(()) => self,
            Err(rollback) => Self::Rollback {
                failure: Box::new(self),
                rollback: Box::new(rollback),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use coven_protocol::objects::StorageBackendFailure;

    fn backend_setup_error(kind: StorageBackendFailure) -> CloudHomeSetupError {
        CloudHomeSetupError::Connection(Box::new(SyncError::CloudHome(
            coven_storage::cloud::CloudHomeError::backend(
                kind,
                "test cloud setup",
                std::io::Error::other("test backend failure"),
            ),
        )))
    }

    #[test]
    fn cloud_home_setup_preserves_every_actionable_provider_failure() {
        let cases = [
            (
                StorageBackendFailure::Authentication,
                CloudHomeSetupFailure::Authentication,
            ),
            (
                StorageBackendFailure::PermissionDenied,
                CloudHomeSetupFailure::PermissionDenied,
            ),
            (
                StorageBackendFailure::ContainerNotFound,
                CloudHomeSetupFailure::ContainerNotFound,
            ),
            (
                StorageBackendFailure::RegionMismatch,
                CloudHomeSetupFailure::RegionMismatch,
            ),
            (
                StorageBackendFailure::QuotaExceeded,
                CloudHomeSetupFailure::QuotaExceeded,
            ),
            (
                StorageBackendFailure::Configuration,
                CloudHomeSetupFailure::InvalidConfiguration,
            ),
            (
                StorageBackendFailure::Transport,
                CloudHomeSetupFailure::Network,
            ),
            (
                StorageBackendFailure::Internal,
                CloudHomeSetupFailure::Internal,
            ),
        ];

        for (backend, expected) in cases {
            assert_eq!(backend_setup_error(backend).failure(), expected);
        }
    }

    #[test]
    fn occupied_cloud_location_is_distinct_from_credentials_and_network() {
        let error = CloudHomeSetupError::Connection(Box::new(SyncError::CloudHome(
            coven_storage::cloud::CloudHomeError::SlotCollision(
                "store/protocol-root.json".to_string(),
            ),
        )));

        assert_eq!(error.failure(), CloudHomeSetupFailure::LocationOccupied);
    }
}
