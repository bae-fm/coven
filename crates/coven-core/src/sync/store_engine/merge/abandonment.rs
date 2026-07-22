use super::*;
use crate::database::{MergeCandidateAbandonmentPreparation, PreparedProtocolObject};
use crate::sync::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateCleanupManifest, ObjectHash,
    StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceHead, StoreDeviceRegistration,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::store_outbound::{load_local_store_authority, StoreOutboundError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

pub(crate) async fn prepare_merge_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<bool, StoreOutboundError> {
    let Some(candidate) = db.blocked_merge_candidate(write_id.clone()).await? else {
        return Ok(false);
    };
    let candidate_summary = db.blocked_merge_history_summary(write_id.clone()).await?;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, identity_signer).await?;
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "blocked Merge candidate belongs to another local registration".to_string(),
        ));
    }
    let coord = candidate.head.value.commit.coord.clone();
    let commit = StoreBatchCommit::signed_with_candidate_abandonment(
        root.store_root_hash,
        write_id.clone(),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        candidate.commit.value.order.clone(),
        candidate.commit.value.membership_state.clone(),
        candidate.commit.value.device_state.clone(),
        vec![CandidateCleanupManifest {
            candidate: StoreBatchCommitDeletionTarget {
                coord: coord.clone(),
                object: candidate.commit.object.clone(),
                canonical_signed_bytes: candidate.commit.bytes.clone(),
            },
        }],
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let StoreCommitCoord::MergeConcurrent {
        stream_id,
        sequence,
    } = coord.clone()
    else {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate reached Merge abandonment".to_string(),
        ));
    };
    let commit_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id.to_string(),
        sequence,
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let history_summary = crate::sync::store_pull::prepare_merge_abandonment_history_summary(
        &candidate_summary,
        &candidate.head.value.commit,
        &candidate.commit.value,
        &commit_ref,
        &commit,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref,
        commit_ref,
        history_summary.digest(),
        candidate.head.value.successor,
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = head_slot_prefix(device_id, sequence);
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            candidate.head.object.slot().clone(),
            &head_prefix,
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    db.prepare_merge_candidate_abandonment(MergeCandidateAbandonmentPreparation {
        write_id,
        commit: PreparedProtocolObject {
            value: commit,
            prepared: commit_prepared,
        },
        head: PreparedProtocolObject {
            value: head,
            prepared: head_prepared,
        },
        history_summary,
    })
    .await?;
    Ok(true)
}

pub(crate) async fn abandon_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreOutboundError> {
    match db.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                    .await?;
                db.finish_retracted_merge_candidate_cleanup(write_id.clone())
                    .await?;
                return Ok(MergeCandidateAbandonment::Abandoned);
            }
            if matches!(
                db.write_status(&write_id).await?,
                crate::WriteStatus::Resolved(_)
            ) {
                return Ok(MergeCandidateAbandonment::NotRequired);
            }
            if let Some(candidate) = db.blocked_merge_candidate(write_id.clone()).await? {
                if let Some(nonactivation) =
                    Box::pin(excluded_candidate_nonactivation(db, storage, &candidate)).await?
                {
                    db.begin_blocked_merge_candidate_nonactivation(write_id.clone(), nonactivation)
                        .await?;
                    crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                        .await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
            }
            if !prepare_merge_candidate_abandonment(
                db,
                storage,
                device_id,
                identity_signer,
                write_id.clone(),
            )
            .await?
            {
                return Ok(MergeCandidateAbandonment::NotRequired);
            }
        }
        crate::database::MergeAbandonmentState::Prepared => {
            let candidates = db
                .prepared_merge_abandonment_candidates(write_id.clone())
                .await?
                .ok_or_else(|| {
                    StoreOutboundError::InvalidOutbound(
                        "prepared Merge abandonment has no exact candidates".to_string(),
                    )
                })?;
            let candidate = Box::pin(excluded_candidate_nonactivation(
                db,
                storage,
                &candidates.candidate,
            ))
            .await?;
            let authority = Box::pin(excluded_candidate_nonactivation(
                db,
                storage,
                &candidates.authority,
            ))
            .await?;
            match (candidate, authority) {
                (Some(candidate), Some(authority)) => {
                    db.begin_prepared_merge_abandonment_nonactivation(
                        write_id.clone(),
                        candidate,
                        authority,
                    )
                    .await?;
                    crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                        .await?;
                    db.finish_author_excluded_merge_abandonment(write_id)
                        .await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
                (None, None) => {}
                _ => {
                    return Err(StoreOutboundError::InvalidOutbound(
                        "prepared Merge abandonment candidates disagree on author exclusion"
                            .to_string(),
                    ));
                }
            }
        }
        crate::database::MergeAbandonmentState::Accepted
        | crate::database::MergeAbandonmentState::CandidateWon
        | crate::database::MergeAbandonmentState::OtherWon => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                    .await?;
            }
            return finish_merge_abandonment(db, storage, write_id).await;
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                    .await?;
            }
            db.finish_author_excluded_merge_abandonment(write_id)
                .await?;
            return Ok(MergeCandidateAbandonment::Abandoned);
        }
    }
    crate::sync::store_engine::merge::publication::drain_store_writes(db, storage).await?;
    if !db.merge_candidate_cleanup_pending(&write_id).await? {
        return Err(StoreOutboundError::InvalidOutbound(
            "accepted Merge abandonment has no exact cleanup transition".to_string(),
        ));
    }
    crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
    finish_merge_abandonment(db, storage, write_id).await
}

async fn finish_merge_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreOutboundError> {
    match db.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None
        | crate::database::MergeAbandonmentState::Accepted => {
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::OtherWon => {
            db.finish_lost_merge_abandonment(write_id).await?;
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::CandidateWon => {
            db.resume_winning_merge_candidate(write_id).await?;
            crate::sync::store_engine::merge::publication::drain_store_writes(db, storage).await?;
            Ok(MergeCandidateAbandonment::CandidateActivated)
        }
        crate::database::MergeAbandonmentState::Prepared => {
            Err(StoreOutboundError::InvalidOutbound(
                "Merge abandonment has no accepted head outcome".to_string(),
            ))
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                crate::sync::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
                    .await?;
            }
            db.finish_author_excluded_merge_abandonment(write_id)
                .await?;
            Ok(MergeCandidateAbandonment::Abandoned)
        }
    }
}

async fn excluded_candidate_nonactivation(
    db: &Database,
    storage: &dyn SyncStorage,
    candidate: &crate::database::BlockedMergeCandidate,
) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreOutboundError>
{
    let candidate_ref = candidate.head.value.commit.clone();
    let Some(locator) = db
        .author_exclusion_activation_for_candidate(
            candidate_ref.clone(),
            candidate.commit.value.author_registration.clone(),
        )
        .await?
    else {
        return Ok(None);
    };
    let root = db.local_store_root_ref().await?.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("blocked Merge candidate has no Store root".to_string())
    })?;
    let candidate_target = StoreBatchCommitDeletionTarget {
        coord: candidate_ref.coord.clone(),
        object: candidate.commit.object.clone(),
        canonical_signed_bytes: candidate.commit.bytes.clone(),
    };
    let nonactivation = match observe_excluded_candidate_head(
        db,
        storage,
        root.store_root_hash,
        &candidate.head.value,
        &candidate.commit.value,
        &candidate.head.object,
    )
    .await?
    {
        ExcludedCandidateHeadObservation::AuthorExclusion => {
            let activation = Box::pin(crate::sync::store_pull::verify_author_exclusion_activation(
                db,
                storage,
                &root,
                &locator,
                &candidate_ref,
                &candidate.commit.value,
                &candidate.head.value,
                &candidate.head.object,
            ))
            .await?;
            crate::sync::remote_object::VerifiedCandidateNonactivation::author_exclusion(
                &activation,
                candidate_target,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
        }
        ExcludedCandidateHeadObservation::MergeWinner(observation) => {
            let registration = db
                .activated_store_device_registration(
                    candidate.commit.value.author_registration.clone(),
                )
                .await?;
            observation
                .verified_nonactivation(candidate_target, &registration)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?
        }
    };
    Ok(Some(nonactivation))
}

/// Publish the exact prepared object graph in sequence order. Every remote object
/// is verified at its reserved slot before the exact head activates the commit.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedMergeWinner {
    store_root_hash: ObjectHash,
    expected_slot: crate::storage::cloud::ObjectSlot,
    expected: StoreDeviceHead,
    expected_commit: Box<StoreBatchCommit>,
    winner: StoreDeviceHead,
    winner_prepared: PreparedExactObject,
    winner_commit: Box<StoreBatchCommit>,
}

impl VerifiedMergeWinner {
    pub(crate) fn verified_nonactivation(
        &self,
        candidate: StoreBatchCommitDeletionTarget,
        author: &StoreDeviceRegistration,
    ) -> Result<
        crate::sync::remote_object::VerifiedCandidateNonactivation,
        crate::sync::remote_object::RemoteObjectRecordError,
    > {
        let commit = candidate
            .verify_nonactivation_candidate(self.store_root_hash, author)
            .map_err(|error| {
                crate::sync::remote_object::RemoteObjectRecordError::InvalidProof(error.to_string())
            })?;
        let reference = StoreBatchCommitRef::from_commit(
            &commit,
            candidate.coord.clone(),
            candidate.object.clone(),
        )
        .map_err(|error| {
            crate::sync::remote_object::RemoteObjectRecordError::InvalidProof(error.to_string())
        })?;
        if self.expected.store_root_hash != self.store_root_hash
            || commit.store_root_hash != self.store_root_hash
            || self.expected.author_registration != commit.author_registration
            || self.expected.commit.coord != reference.coord
            || self.expected_commit.author_registration != commit.author_registration
            || self.expected_commit.order.predecessor() != commit.order.predecessor()
            || self.winner.store_root_hash != self.store_root_hash
            || self.winner.author_registration != self.expected.author_registration
            || self.winner.commit.coord != self.expected.commit.coord
            || self.winner.successor.activation != self.expected.successor.activation
            || self.winner.successor.predecessor != self.expected.successor.predecessor
            || self.winner_prepared.reference().slot() != &self.expected_slot
            || self.winner.commit == reference
            || self.winner.commit.commit_hash != self.winner_commit.commit_hash()
        {
            return Err(
                crate::sync::remote_object::RemoteObjectRecordError::InvalidProof(
                    "Merge winner observation is not bound to the losing candidate's exact activation point"
                        .to_string(),
                ),
            );
        }
        self.winner
            .commit
            .verify_commit(&self.winner_commit)
            .map_err(|error| {
                crate::sync::remote_object::RemoteObjectRecordError::InvalidProof(error.to_string())
            })?;
        crate::sync::remote_object::VerifiedCandidateNonactivation::from_verified_merge_winner(
            candidate,
            crate::sync::store_commit::StoreDeviceHeadRef {
                head_hash: self.winner.head_hash(),
                object: self.winner_prepared.reference().clone(),
            },
            self.winner.commit.clone(),
        )
    }

    pub(crate) fn winner(&self) -> &StoreDeviceHead {
        &self.winner
    }

    pub(crate) fn winner_prepared(&self) -> &PreparedExactObject {
        &self.winner_prepared
    }

    pub(crate) fn winner_commit(&self) -> &StoreBatchCommit {
        &self.winner_commit
    }

    pub(crate) fn into_head(self) -> (StoreDeviceHead, PreparedExactObject) {
        (self.winner, self.winner_prepared)
    }

    #[cfg(test)]
    pub(crate) fn winner_mut_for_test(&mut self) -> &mut StoreDeviceHead {
        &mut self.winner
    }

    #[cfg(test)]
    pub(crate) fn set_expected_slot_for_test(
        &mut self,
        expected_slot: crate::storage::cloud::ObjectSlot,
    ) {
        self.expected_slot = expected_slot;
    }
}

pub(crate) enum ExcludedCandidateHeadObservation {
    AuthorExclusion,
    MergeWinner(VerifiedMergeWinner),
}

pub(crate) async fn observe_excluded_candidate_head(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    candidate: &StoreDeviceHead,
    candidate_commit: &StoreBatchCommit,
    candidate_object: &ExactObjectRef,
) -> Result<ExcludedCandidateHeadObservation, StoreOutboundError> {
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreHead);
    let prefix = head_slot_prefix(
        &candidate.author_registration.device_id.to_string(),
        candidate.commit.coord.sequence(),
    );
    match storage
        .read_protocol_slot(&context, candidate_object.slot(), &prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(ExcludedCandidateHeadObservation::AuthorExclusion),
        Ok((bytes, object)) if bytes == candidate.to_bytes() && object == *candidate_object => {
            Ok(ExcludedCandidateHeadObservation::AuthorExclusion)
        }
        Ok(_) => read_occupied_merge_head(
            db,
            storage,
            store_root_hash,
            candidate,
            candidate_commit,
            candidate_object.slot(),
            &prefix,
        )
        .await
        .map(ExcludedCandidateHeadObservation::MergeWinner),
        Err(error) => Err(StoreObjectError::Storage(error).into()),
    }
}

pub(crate) fn verify_merge_candidate_nonactivations(
    observation: &VerifiedMergeWinner,
    targets: impl IntoIterator<Item = StoreBatchCommitDeletionTarget>,
    author: &StoreDeviceRegistration,
) -> Result<Vec<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreOutboundError> {
    let mut nonactivations = Vec::new();
    for target in targets {
        if target.coord == observation.winner().commit.coord
            && target.object == observation.winner().commit.object
            && target.canonical_signed_bytes == observation.winner_commit().to_bytes()
        {
            continue;
        }
        nonactivations.push(
            observation
                .verified_nonactivation(target, author)
                .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        );
    }
    Ok(nonactivations)
}

pub(crate) async fn read_occupied_merge_head(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    expected: &StoreDeviceHead,
    expected_commit: &StoreBatchCommit,
    slot: &crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
) -> Result<VerifiedMergeWinner, StoreOutboundError> {
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreHead);
    let (winner_bytes, winner_prepared) = storage
        .read_prepared_protocol_slot(&context, slot, semantic_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified: StoreDeviceHead = serde_json::from_slice(&winner_bytes).map_err(|error| {
        StoreOutboundError::InvalidOutbound(format!("parse competing Merge head: {error}"))
    })?;
    if unverified.author_registration != expected.author_registration
        || unverified.commit.coord != expected.commit.coord
        || unverified.successor.activation != expected.successor.activation
        || unverified.successor.predecessor != expected.successor.predecessor
    {
        return Err(StoreOutboundError::InvalidOutbound(
            "competing Merge head does not occupy the prepared successor point".to_string(),
        ));
    }
    let registration = db
        .activated_store_device_registration(expected.author_registration.clone())
        .await?;
    expected
        .commit
        .verify_commit(expected_commit)
        .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    StoreBatchCommit::parse_at(
        &expected_commit.to_bytes(),
        store_root_hash,
        &expected.commit.coord,
        &registration,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    StoreDeviceHead::parse_at(
        &expected.to_bytes(),
        store_root_hash,
        &registration,
        &expected.commit,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let winner_commit = crate::sync::store_objects::load_commit_ref(
        storage,
        store_root_hash,
        &unverified.commit,
        &registration,
    )
    .await?
    .value;
    let winner = StoreDeviceHead::parse_at(
        &winner_bytes,
        store_root_hash,
        &registration,
        &unverified.commit,
    )
    .map_err(|error| {
        StoreOutboundError::InvalidOutbound(format!("verify occupied Merge head: {error}"))
    })?;
    Ok(VerifiedMergeWinner {
        store_root_hash,
        expected_slot: slot.clone(),
        expected: expected.clone(),
        expected_commit: Box::new(expected_commit.clone()),
        winner,
        winner_prepared,
        winner_commit: Box::new(winner_commit),
    })
}
