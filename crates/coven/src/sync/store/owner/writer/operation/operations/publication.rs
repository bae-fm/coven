use super::*;

pub(crate) struct PreparedStoreOperationActivation {
    pub(crate) candidate: Box<PreparedStoreOperationCommit>,
    pub(crate) retained_operation_objects: Vec<ExactObjectRef>,
}

pub(crate) enum StoreMembershipJournalCompletion {
    Mutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
    RotationMutation {
        intent_hash: ObjectHash,
        progress_bytes: Vec<u8>,
        generation: u64,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
    OwnerPromotion {
        transition: super::owner_promotion::OwnerPromotionJournalTransition,
        remote_objects: Vec<super::remote_object::RemoteObjectRecord>,
    },
}

impl StoreMembershipJournalCompletion {
    pub(crate) fn object_refs(&self) -> Vec<ExactObjectRef> {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .map(|remote| remote.object().clone())
            .collect()
    }

    pub(crate) fn remote_object(
        &self,
        object: &ExactObjectRef,
    ) -> Result<super::remote_object::RemoteObjectRecord, StoreError> {
        let remote_objects = match self {
            Self::Mutation { remote_objects, .. }
            | Self::RotationMutation { remote_objects, .. }
            | Self::OwnerPromotion { remote_objects, .. } => remote_objects,
        };
        remote_objects
            .iter()
            .find(|remote| remote.object() == object)
            .cloned()
            .ok_or_else(|| {
                StoreError::InvalidOutbound(
                    "membership completion omits an exact activated object".to_string(),
                )
            })
    }
}

pub(crate) fn retained_store_operation_objects(
    commit: &StoreBatchCommit,
) -> Result<Vec<ExactObjectRef>, StoreError> {
    let objects = commit
        .acknowledgement()
        .map(|reference| reference.object.clone())
        .into_iter()
        .chain(
            commit
                .circle_acknowledgements()
                .iter()
                .map(|reference| reference.object.clone()),
        )
        .chain(
            commit
                .device_exclusion_proposals()
                .iter()
                .map(|reference| reference.object.clone()),
        )
        .chain(
            commit
                .device_exclusion_outcomes()
                .iter()
                .map(|reference| reference.object().clone()),
        )
        .chain(
            commit
                .reclaim_authorization()
                .into_iter()
                .flat_map(|reference| {
                    [reference.evidence.object.clone(), reference.object.clone()]
                }),
        )
        .chain(
            commit
                .reclaim_receipt()
                .map(|reference| reference.object.clone()),
        )
        .collect::<Vec<_>>();
    if objects
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        != objects.len()
    {
        return Err(StoreError::InvalidOutbound(
            "Store operation publication repeats a retained authority object".to_string(),
        ));
    }
    Ok(objects)
}
