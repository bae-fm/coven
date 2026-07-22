use super::*;

pub(crate) enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

pub(crate) async fn required_pull_root(
    db: &Database,
    requested_hash: ObjectHash,
) -> Result<StoreRootRef, StorePullError> {
    let root = db
        .local_store_root_ref()
        .await
        .map_err(|error| StorePullError::Database(format!("load exact Store root: {error}")))?
        .ok_or_else(|| {
            StorePullError::Database("Store root exact reference is absent".to_string())
        })?;
    if root.store_root_hash != requested_hash {
        return Err(StorePullError::Database(
            "requested Store root differs from the durable exact root reference".to_string(),
        ));
    }
    Ok(root)
}
