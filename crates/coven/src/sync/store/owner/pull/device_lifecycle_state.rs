use super::*;

pub(crate) fn history_cut_references(cut: &StoreHistoryCut) -> Vec<StoreBatchCommitRef> {
    cut.0.values().cloned().collect()
}

pub(crate) fn commit_predecessor_references(commit: &StoreBatchCommit) -> Vec<StoreBatchCommitRef> {
    commit
        .order
        .predecessor
        .iter()
        .chain(commit.order.dependencies.values())
        .cloned()
        .collect()
}

pub(crate) fn verified_merge_predecessor_state(
    genesis: &ResolvedStoreDeviceState,
    states: &BTreeMap<StoreBatchCommitRef, ResolvedStoreDeviceState>,
    commit: &StoreBatchCommit,
) -> Result<ResolvedStoreDeviceState, StorePullError> {
    let predecessor = &commit.order.predecessor;
    let dependencies = &commit.order.dependencies;
    let mut predecessor_refs = dependencies.values().collect::<Vec<_>>();
    predecessor_refs.extend(predecessor.iter());
    let predecessor_state = if predecessor_refs.is_empty() {
        genesis.clone()
    } else {
        ResolvedStoreDeviceState::merge(
            predecessor_refs
                .into_iter()
                .map(|dependency| {
                    states.get(dependency).cloned().ok_or_else(|| {
                        StorePullError::InvalidState(
                            "Merge history has an unresolved predecessor state".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(StorePullError::Protocol)?
    };
    let mut frontier = dependencies.clone();
    if let Some(predecessor) = predecessor {
        let stream_id = predecessor.coord.stream_id;
        if frontier
            .insert(stream_id, predecessor.clone())
            .is_some_and(|existing| existing != *predecessor)
        {
            return Err(StorePullError::InvalidState(
                "Merge predecessor conflicts with its dependency cut".to_string(),
            ));
        }
    }
    let expected_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(frontier), &predecessor_state)
            .map_err(StorePullError::Protocol)?;
    if commit.device_state != expected_state {
        return Err(StorePullError::InvalidState(
            "Merge commit names another predecessor device state".to_string(),
        ));
    }
    Ok(predecessor_state)
}
