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

    pub(crate) fn complete_on(
        self,
        tx: &rusqlite::Transaction<'_>,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), crate::database::DbError> {
        match self {
            Self::Mutation {
                intent_hash,
                progress_bytes,
                remote_objects,
            } => StoreDatabase::record_activated_membership_candidate_mutation_on(
                tx,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::WithoutRotation,
            ),
            Self::RotationMutation {
                intent_hash,
                progress_bytes,
                generation,
                remote_objects,
            } => StoreDatabase::record_activated_membership_candidate_mutation_on(
                tx,
                intent_hash,
                candidate,
                &remote_objects
                    .iter()
                    .map(|remote| remote.object().clone())
                    .collect::<Vec<_>>(),
                progress_bytes,
                crate::database::MembershipMutationActivation::Rotation { generation },
            ),
            Self::OwnerPromotion {
                transition,
                remote_objects,
            } => {
                let mut unique = std::collections::BTreeSet::new();
                let object_ids = remote_objects
                    .iter()
                    .map(|remote| remote.object_id())
                    .map(|object_id| {
                        if unique.insert(object_id) {
                            Ok(object_id)
                        } else {
                            Err(crate::database::DbError::Message(
                                "activated Owner-promotion graph repeats an exact object"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if object_ids.is_empty() {
                    return Err(crate::database::DbError::Message(
                        "activated Owner-promotion graph is empty".to_string(),
                    ));
                }
                MergeMaterializationTransaction::new(tx)
                    .activate_store_operation_remote_objects(candidate, &object_ids)?;
                let (journal_key, target_key, previous_value, next_value, remote_objects) =
                    transition.into_values();
                StoreDatabase::advance_owner_promotion_journal_on(
                    tx,
                    journal_key,
                    target_key,
                    previous_value,
                    next_value,
                    remote_objects,
                )
            }
        }
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
