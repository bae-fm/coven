//! Confirmed provider deletion of an S3 access key.

use coven_storage::StorageError;

/// Why a deletion with admin access did not clear its notice (§13).
#[derive(Debug, thiserror::Error)]
pub enum DeleteAccessKeyError {
    /// The provider refused or failed the deletion.
    #[error("deleting the S3 key failed")]
    DeleteFailed(#[source] StorageError),
    /// The read still found the key.
    #[error("the provider still has the S3 key")]
    StillPresent,
    /// The read failed.
    #[error("reading the S3 key's state failed")]
    ReadFailed(#[source] StorageError),
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
        admin
            .revoke(&key.access_key_id, &key.principal)
            .await
            .map_err(DeleteAccessKeyError::DeleteFailed)?;
        if admin
            .key_exists(&key.access_key_id, &key.principal)
            .await
            .map_err(DeleteAccessKeyError::ReadFailed)?
        {
            return Err(DeleteAccessKeyError::StillPresent.into());
        }
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

/// The pending `DeleteAccessKey` work in a discarded operation journal (§10).
/// A reset carries these into its replacement database; a key the old copy
/// made but never recorded is reclaimed after its device's retirement (§13).
pub(crate) fn owed_access_key_deletions(
    records: &[coven_database::OperationRecord],
) -> Result<Vec<coven_database::AccessKeyDeletion>, crate::SyncError> {
    let mut deletions = Vec::new();
    for record in records
        .iter()
        .filter(|record| record.kind == "delete-access-key")
    {
        let Deletion { key } = serde_json::from_slice(&record.data)?;
        deletions.push(coven_database::AccessKeyDeletion {
            key,
            operation: coven_database::NewOperation {
                kind: record.kind.clone(),
                data: record.data.clone(),
                started_by: "coven".into(),
                pauses_writes: false,
            },
        });
    }
    Ok(deletions)
}
