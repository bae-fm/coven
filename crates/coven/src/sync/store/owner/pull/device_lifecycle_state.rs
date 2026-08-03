use super::*;

pub(crate) struct DeviceJoinBootstrapCommit {
    pub reference: StoreBatchCommitRef,
    pub commit: VerifiedStoreBatchCommit,
    pub registrations: Vec<ActivatedStoreDeviceRegistration>,
    pub device_operations: VerifiedStoreDeviceOperations,
    pub activation: DeviceJoinBootstrapActivation,
}

pub(crate) struct DeviceJoinBootstrapActivation {
    pub(crate) head: StoreDeviceHead,
    pub(crate) object: ExactObjectRef,
    pub(crate) history_summary: RetainedVerifiedMergeHistorySummary,
}

pub(crate) struct DeviceJoinBootstrapPlan {
    pub founder_reference: StoreDeviceRegistrationRef,
    pub founder: StoreDeviceRegistration,
    pub founder_bytes: Vec<u8>,
    pub genesis: ResolvedStoreDeviceState,
    pub membership: crate::database::InitialStoreMembershipAuthority,
    pub commits: Vec<DeviceJoinBootstrapCommit>,
}

impl DeviceJoinBootstrapPlan {
    pub(crate) fn verified_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<&VerifiedStoreBatchCommit> {
        self.commits
            .iter()
            .find(|commit| &commit.reference == reference)
            .map(|commit| &commit.commit)
    }
}

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

pub(crate) fn predecessor_with_recovery_author(
    mut predecessor: ResolvedStoreDeviceState,
    commit: &StoreBatchCommit,
    registrations: &[ActivatedStoreDeviceRegistration],
) -> Result<(ResolvedStoreDeviceState, Option<StoreDeviceRegistrationRef>), StoreProtocolError> {
    if commit.device_registrations().len() != registrations.len() {
        return Err(StoreProtocolError::Malformed(
            "verified registrations do not cover every activation".to_string(),
        ));
    }
    for (activated, registration) in commit.device_registrations().iter().zip(registrations) {
        registration.verify_reference(activated)?;
        if activated.registration == commit.author_registration {
            if let Some(cursor) = registration.recovery_cursor()? {
                predecessor = predecessor
                    .activate_registration(activated.registration.clone(), Some(cursor))?;
                return Ok((predecessor, Some(activated.registration.clone())));
            }
        }
    }
    Ok((predecessor, None))
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
                        StorePullError::Database(
                            "Merge history has an unresolved predecessor state".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?
    };
    let mut frontier = dependencies.clone();
    if let Some(predecessor) = predecessor {
        let stream_id = predecessor.coord.stream_id;
        if frontier
            .insert(stream_id, predecessor.clone())
            .is_some_and(|existing| existing != *predecessor)
        {
            return Err(StorePullError::Database(
                "Merge predecessor conflicts with its dependency cut".to_string(),
            ));
        }
    }
    let expected_state =
        StoreDeviceStateRef::from_resolved(CommitFrontier(frontier), &predecessor_state)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
    if commit.device_state != expected_state {
        return Err(StorePullError::Database(
            "Merge commit names another predecessor device state".to_string(),
        ));
    }
    Ok(predecessor_state)
}
