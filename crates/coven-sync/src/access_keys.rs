//! Confirmed provider deletion of an S3 access key.

use coven_storage::StorageError;

/// Whether a provider read found a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyPresence {
    /// The provider still lists the key.
    Present,
    /// The provider's read no longer lists the key.
    Absent,
}

/// A provider read made after deleting a key, naming the key it read.
#[derive(Debug)]
pub(crate) struct KeyObservation {
    /// The key id the read concerned.
    pub(crate) access_key_id: String,
    /// What the read found, or why it failed.
    pub(crate) reply: Result<KeyPresence, StorageError>,
}

/// Why a deletion with admin access did not clear its notice (§13).
#[derive(Debug, thiserror::Error)]
pub enum DeleteAccessKeyError {
    /// The provider refused or failed the deletion.
    #[error("deleting the S3 key failed")]
    DeleteFailed(#[source] StorageError),
    /// No read followed the deletion.
    #[error("no provider read confirmed the S3 key's deletion")]
    ObservationMissing,
    /// The read concerned another key.
    #[error("the provider read concerned another S3 key")]
    WrongKey,
    /// The read still found the key.
    #[error("the provider still has the S3 key")]
    StillPresent,
    /// The read failed.
    #[error("reading the S3 key's state failed")]
    ObservationFailed(#[source] StorageError),
}

/// Accept a deletion only when it succeeded and a later read of this exact key
/// found it absent. Admin access is required by the deleting call itself.
pub(crate) fn verify_deletion(
    access_key_id: &str,
    deleted: Result<(), StorageError>,
    observation: Option<KeyObservation>,
) -> Result<(), DeleteAccessKeyError> {
    deleted.map_err(DeleteAccessKeyError::DeleteFailed)?;
    let read = observation.ok_or(DeleteAccessKeyError::ObservationMissing)?;
    if read.access_key_id != access_key_id {
        return Err(DeleteAccessKeyError::WrongKey);
    }
    match read.reply {
        Err(error) => Err(DeleteAccessKeyError::ObservationFailed(error)),
        Ok(KeyPresence::Present) => Err(DeleteAccessKeyError::StillPresent),
        Ok(KeyPresence::Absent) => Ok(()),
    }
}

#[cfg(test)]
#[path = "access_keys_tests.rs"]
mod tests;

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Deletion {
    #[serde(with = "crate::pending_encoding::AccessKey")]
    pub(crate) key: coven_database::AccessKeyToDelete,
}

impl crate::SequenceSync {
    /// Delete a journaled S3 credential and verify its absence before clearing it.
    pub async fn delete_access_key(
        &self,
        id: coven_database::OperationId,
        admin: &dyn coven_storage::AccessKeyAdmin,
    ) -> Result<(), crate::SyncError> {
        let record = self
            .database
            .operation(id)
            .await?
            .ok_or(crate::SyncError::NotPending(id))?;
        if record.kind != "delete-access-key" {
            return Err(crate::SyncError::NotPending(id));
        }
        let Deletion { key } = serde_json::from_slice(&record.data)?;
        let deleted = admin.revoke(&key.access_key_id, &key.principal).await;
        let observation = match &deleted {
            Ok(()) => Some(KeyObservation {
                access_key_id: key.access_key_id.clone(),
                reply: admin
                    .key_exists(&key.access_key_id, &key.principal)
                    .await
                    .map(|present| {
                        if present {
                            KeyPresence::Present
                        } else {
                            KeyPresence::Absent
                        }
                    }),
            }),
            Err(_) => None,
        };
        verify_deletion(&key.access_key_id, deleted, observation)?;
        self.database.clear_access_key(key.access_key_id).await?;
        Ok(())
    }

    /// Record the application's explicit confirmation of console-side deletion.
    pub async fn confirm_access_key_deleted(
        &self,
        access_key_id: String,
    ) -> Result<(), crate::SyncError> {
        Ok(self.database.clear_access_key(access_key_id).await?)
    }
}
