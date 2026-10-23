//! The S3 keys owed deletion in the provider's console (§14.3).

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct Deletion {
    #[serde(with = "crate::pending_encoding::AccessKey")]
    pub(crate) key: coven_database::AccessKeyToDelete,
}

impl crate::SequenceSync {
    /// Record the application's explicit confirmation of console-side deletion.
    pub async fn confirm_access_key_deleted(
        &self,
        access_key_id: String,
    ) -> Result<(), crate::SyncError> {
        Ok(self.database.clear_access_key(access_key_id).await?)
    }
}

/// The pending `DeleteAccessKey` work in a discarded operation journal (§19.3).
/// A reset carries these into its replacement database; a key the old copy
/// made but never recorded is reclaimed after its device's retirement (§14.3).
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
