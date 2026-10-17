//! New storage credentials on this owner's live connection (§4, E9).

use super::SequenceSync;
use crate::{restore_codes::install_session, StorageCommit, SyncError};
use coven_storage::{ConnectionCredentials, StorageFailure};

impl SequenceSync {
    /// Commit `next` to custody, then install it on the live connection. A
    /// failed installation rolls custody back and reinstalls `previous`,
    /// reporting every failure.
    pub(crate) async fn install_credentials(
        &self,
        previous: Option<ConnectionCredentials>,
        next: ConnectionCredentials,
        commit: StorageCommit,
    ) -> Result<(), SyncError> {
        if self.storage.config() != next.location {
            return Err(StorageFailure::InvitationMismatch.into());
        }
        let previous = previous.ok_or_else(|| {
            StorageFailure::InvalidConfiguration
                .with_source("connected storage has no held credentials")
        })?;
        let rollback = commit()?;
        if let Err(error) = install_session(self.storage.as_ref(), next.credentials).await {
            let mut error = SyncError::Storage(error);
            for result in [
                rollback(),
                install_session(self.storage.as_ref(), previous.credentials)
                    .await
                    .map_err(SyncError::from),
            ] {
                if let Err(cleanup) = result {
                    error = SyncError::Cleanup {
                        operation: Box::new(error),
                        cleanup: Box::new(cleanup),
                    };
                }
            }
            return Err(error);
        }
        // New credentials are evidence for work waiting on storage.
        self.waits.connection_changed();
        Ok(())
    }
}
