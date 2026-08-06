use super::*;

/// Delete every object a nonactivated candidate left behind and record each one
/// absent. An object is marked absent only after its own deletion succeeds, so a
/// drain that fails partway leaves the remaining objects named as cleanup
/// targets for the operation that retries it.
pub(crate) async fn delete_candidate_cleanup_targets<E>(
    storage: &dyn SyncStorage,
    database: &StoreDatabase,
    targets: impl IntoIterator<Item = coven_database::CandidateCleanupObject>,
) -> Result<(), E>
where
    E: From<coven_protocol::objects::StoreObjectError> + From<coven_database::DbError>,
{
    for target in targets {
        storage
            .delete_protocol_object(&target.object)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        database
            .mark_candidate_cleanup_absent(target.object)
            .await?;
    }
    Ok(())
}
