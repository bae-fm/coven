//! Blob domain workflows: locality transitions, tombstone lifecycle, retry
//! policy, and local cleanup. The blob value model — references, locators,
//! scopes, transfer limits, and the transition observer port — lives in
//! [`coven_protocol::blob`]; upload execution outcomes live here with the
//! database and filesystem errors they preserve.

pub(crate) mod delete;
pub(crate) mod progress;
pub(crate) mod retry;
pub mod transition;

pub use delete::BlobTombstoneJson;
pub use transition::{MakeLocalError, MakeRemoteError};

#[derive(Debug)]
pub enum DrainOutcome {
    Drained {
        uploaded: usize,
        yielded_for_publish: bool,
        failures: UploadFailures,
    },
    QueueEmpty,
    AllInBackoff,
    Paused,
}

#[cfg(any(test, feature = "test-utils"))]
impl DrainOutcome {
    #[track_caller]
    fn drained(&self) -> (usize, bool, &UploadFailures) {
        match self {
            Self::Drained {
                uploaded,
                yielded_for_publish,
                failures,
            } => (*uploaded, *yielded_for_publish, failures),
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }

    #[track_caller]
    pub fn uploaded(&self) -> usize {
        self.drained().0
    }

    #[track_caller]
    pub fn yielded_for_publish(&self) -> bool {
        self.drained().1
    }

    #[track_caller]
    pub fn failures(&self) -> &UploadFailures {
        self.drained().2
    }

    #[track_caller]
    pub fn into_failures(self) -> UploadFailures {
        match self {
            Self::Drained { failures, .. } => failures,
            other => panic!("expected a drain that attempted queued entries, got {other:?}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UploadFailureCause {
    #[error("local upload state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("local upload locator: {0}")]
    Locator(#[from] coven_protocol::blob::locator::BlobLocatorError),
    #[error("local upload file: {0}")]
    File(#[from] coven_foundation::atomic_file::FileError),
    #[error("local upload pin: {0}")]
    Pin(#[from] coven_foundation::store_dir::StoreBlobFileError),
    #[error("cancelled cache copy: {0}")]
    CachedRemoval(#[from] coven_foundation::store_dir::CachedLocatorRemovalError),
    #[error("blob storage: {0}")]
    Storage(#[from] coven_protocol::objects::StorageError),
    #[error("local upload state: {0}")]
    InvalidState(String),
}

#[derive(Debug)]
pub struct UploadFailure {
    pub entry_id: i64,
    pub object_key: String,
    pub cause: UploadFailureCause,
}

#[derive(Debug)]
pub struct UploadFailures(Vec<UploadFailure>);

impl UploadFailures {
    pub fn new(failures: Vec<UploadFailure>) -> Self {
        Self(failures)
    }

    pub fn failures(&self) -> &[UploadFailure] {
        &self.0
    }

    pub fn has_transport_failure(&self) -> bool {
        self.0.iter().any(|failure| {
            matches!(&failure.cause, UploadFailureCause::Storage(error) if error.is_transport())
        })
    }
}

impl std::fmt::Display for UploadFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} blob upload(s) failed", self.0.len())?;
        for failure in &self.0 {
            write!(
                formatter,
                "; entry {} {}: {}",
                failure.entry_id, failure.object_key, failure.cause
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for UploadFailures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.iter().find_map(|failure| match &failure.cause {
            UploadFailureCause::Storage(error) if error.is_transport() => {
                Some(error as &(dyn std::error::Error + 'static))
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod upload_tests;

#[cfg(test)]
mod transition_tests;

#[cfg(test)]
mod local_store_tests;

#[cfg(test)]
mod delete_tests;
