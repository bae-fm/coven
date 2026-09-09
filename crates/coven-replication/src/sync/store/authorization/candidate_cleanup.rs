use super::*;

/// Delete every object an operation-specific journal has proved unaccepted.
pub(crate) async fn delete_candidate_cleanup_targets<E>(
    storage: &dyn CloudSyncObjectStorage,
    targets: impl IntoIterator<Item = coven_database::CandidateCleanupObject>,
) -> Result<(), E>
where
    E: From<coven_protocol::objects::StoreObjectError>,
{
    for target in targets {
        storage
            .delete_protocol_object(&target.object)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
    }
    Ok(())
}

/// Finish exact candidate retirement while retaining any spool another durable
/// preparation owns. Both upload paths are excluded until the database job has
/// transferred or removed every retired file claim.
pub(crate) async fn retire_store_write_candidates(
    database: &coven_database::StoreDatabase,
    storage: &dyn CloudSyncObjectStorage,
    active: coven_database::ActiveStorePublication,
) -> Result<(), StoreError> {
    if active.retired_candidates().is_empty() {
        return Ok(());
    }
    let _upload = database.blob_upload_drain_permit().await;
    let _snapshot = database.snapshot_publication_permit().await;
    let targets = database.retired_store_write_cleanup(active.clone()).await?;
    delete_candidate_cleanup_targets::<StoreError>(storage, targets).await?;
    for retired in active.retired_candidates() {
        for publication in &retired.publications {
            storage
                .delete_protocol_object(&publication.object)
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
        }
    }
    database
        .complete_retired_store_write_cleanup(active)
        .await?;
    Ok(())
}
