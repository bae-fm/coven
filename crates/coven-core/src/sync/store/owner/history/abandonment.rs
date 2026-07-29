use crate::database::PreparedProtocolObject;
use crate::sync::storage::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store::database::publication_state::MergeCandidateAbandonmentPreparation;
use crate::sync::store::StoreError;
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateCleanupManifest, ObjectHash,
    StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreDeviceHead, StoreDeviceRegistration,
    VerifiedStoreBatchCommit,
};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

pub(crate) async fn prepare_merge_candidate_abandonment(
    operation: &mut super::super::AuthorizedWriterOperation<'_>,
    write_id: crate::WriteId,
) -> Result<bool, StoreError> {
    let database = operation.database().clone();
    let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await? else {
        return Ok(false);
    };
    let candidate_summary = database
        .blocked_merge_history_summary(write_id.clone())
        .await?;
    let (registration_ref, registration, device_signer) = operation.registration();
    let registration_ref = registration_ref.clone();
    let registration = registration.clone();
    let device_signer = device_signer.clone();
    let device_id = registration_ref.device_id.to_string();
    let root = operation.history_verifier_mut().root().clone();
    let storage = operation.history_verifier_mut().storage();
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreError::InvalidOutbound(
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
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let stream_id = coord.stream_id;
    let sequence = coord.sequence;
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
    let commit = VerifiedStoreBatchCommit::parse_prepared(
        &commit.to_bytes(),
        root.store_root_hash,
        coord,
        commit_prepared.reference().clone(),
        &registration,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let commit_ref = commit.reference().clone();
    let history_summary = super::super::pull::prepare_merge_abandonment_history_summary(
        &candidate_summary,
        &candidate.commit.value,
        &commit,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let head = StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref,
        commit_ref.clone(),
        history_summary.digest(),
        candidate.head.value.successor,
        &device_signer,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let head_context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let head_prefix = head_slot_prefix(&device_id, sequence);
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            candidate.head.object.slot().clone(),
            &head_prefix,
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    database
        .prepare_merge_candidate_abandonment(MergeCandidateAbandonmentPreparation {
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
    operation: &mut super::super::AuthorizedWriterOperation<'_>,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    let root = operation.history_verifier_mut().root().clone();
    let database = operation.database().clone();
    let db = database.sqlite();
    match database.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                operation.cleanup_merge_candidate(write_id.clone()).await?;
                database
                    .finish_retracted_merge_candidate_cleanup(write_id.clone())
                    .await?;
                return Ok(MergeCandidateAbandonment::Abandoned);
            }
            if matches!(
                db.write_status(&write_id).await?,
                crate::WriteStatus::Resolved(_)
            ) {
                return Ok(MergeCandidateAbandonment::NotRequired);
            }
            if let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await? {
                let verified = operation
                    .history_verifier_mut()
                    .authenticate_blocked_candidate(&candidate)
                    .await?;
                if let Some(nonactivation) = operation
                    .history()
                    .excluded_candidate_nonactivation(
                        &verified,
                        &candidate.head.value,
                        &candidate.head.object,
                    )
                    .await?
                {
                    database
                        .begin_blocked_merge_candidate_nonactivation(
                            root.clone(),
                            write_id.clone(),
                            nonactivation,
                        )
                        .await?;
                    operation.cleanup_merge_candidate(write_id.clone()).await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
            }
            if !prepare_merge_candidate_abandonment(operation, write_id.clone()).await? {
                return Ok(MergeCandidateAbandonment::NotRequired);
            }
        }
        crate::database::MergeAbandonmentState::Prepared => {
            let candidates = database
                .prepared_merge_abandonment_candidates(write_id.clone())
                .await?
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "prepared Merge abandonment has no exact candidates".to_string(),
                    )
                })?;
            let verified_candidate = operation
                .history_verifier_mut()
                .authenticate_blocked_candidate(&candidates.candidate)
                .await?;
            let candidate = operation
                .history()
                .excluded_candidate_nonactivation(
                    &verified_candidate,
                    &candidates.candidate.head.value,
                    &candidates.candidate.head.object,
                )
                .await?;
            let verified_authority = operation
                .history_verifier_mut()
                .authenticate_blocked_candidate(&candidates.authority)
                .await?;
            let authority = operation
                .history()
                .excluded_candidate_nonactivation(
                    &verified_authority,
                    &candidates.authority.head.value,
                    &candidates.authority.head.object,
                )
                .await?;
            match (candidate, authority) {
                (Some(candidate), Some(authority)) => {
                    database
                        .begin_prepared_merge_abandonment_nonactivation(
                            root.clone(),
                            write_id.clone(),
                            candidate,
                            authority,
                        )
                        .await?;
                    operation.cleanup_merge_candidate(write_id.clone()).await?;
                    database
                        .finish_author_excluded_merge_abandonment(write_id)
                        .await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
                (None, None) => {}
                _ => {
                    return Err(StoreError::InvalidOutbound(
                        "prepared Merge abandonment candidates disagree on author exclusion"
                            .to_string(),
                    ));
                }
            }
        }
        crate::database::MergeAbandonmentState::Accepted
        | crate::database::MergeAbandonmentState::CandidateWon
        | crate::database::MergeAbandonmentState::OtherWon => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                operation.cleanup_merge_candidate(write_id.clone()).await?;
            }
            return finish_merge_abandonment(operation, write_id).await;
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                operation.cleanup_merge_candidate(write_id.clone()).await?;
            }
            database
                .finish_author_excluded_merge_abandonment(write_id)
                .await?;
            return Ok(MergeCandidateAbandonment::Abandoned);
        }
    }
    operation.drain_prepared_store_writes().await?;
    if !database.merge_candidate_cleanup_pending(&write_id).await? {
        return Err(StoreError::InvalidOutbound(
            "accepted Merge abandonment has no exact cleanup transition".to_string(),
        ));
    }
    operation.cleanup_merge_candidate(write_id.clone()).await?;
    finish_merge_abandonment(operation, write_id).await
}

async fn finish_merge_abandonment(
    operation: &mut super::super::AuthorizedWriterOperation<'_>,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    let database = operation.database().clone();
    match database.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None
        | crate::database::MergeAbandonmentState::Accepted => {
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::OtherWon => {
            database.finish_lost_merge_abandonment(write_id).await?;
            Ok(MergeCandidateAbandonment::Abandoned)
        }
        crate::database::MergeAbandonmentState::CandidateWon => {
            database.resume_winning_merge_candidate(write_id).await?;
            operation.drain_prepared_store_writes().await?;
            Ok(MergeCandidateAbandonment::CandidateActivated)
        }
        crate::database::MergeAbandonmentState::Prepared => Err(StoreError::InvalidOutbound(
            "Merge abandonment has no accepted head outcome".to_string(),
        )),
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                operation.cleanup_merge_candidate(write_id.clone()).await?;
            }
            database
                .finish_author_excluded_merge_abandonment(write_id)
                .await?;
            Ok(MergeCandidateAbandonment::Abandoned)
        }
    }
}

/// The nonactivation proof for discarding a candidate whose slot is already
/// resolved. Unlike Merge abandonment, discard never publishes an abandonment
/// commit to race for the slot — it is invoked after the slot is lost, so it
/// observes the outcome directly. A different verified winner occupying the
/// successor slot is a standalone proof (the candidate is bound to that
/// create-once slot and can never take it), independent of the author's status.
/// Author exclusion covers a slot the author was excluded from before anyone
/// claimed it. An accepted Store commit whose membership state tombstones the
/// candidate's exact grant and whose predecessor cut excludes the candidate is
/// the membership-revocation proof.
/// Publish the exact prepared object graph in sequence order. Every remote object
/// is verified at its reserved slot before the exact head activates the commit.
#[derive(Debug, Clone)]
pub(crate) struct VerifiedMergeWinner {
    store_root_hash: ObjectHash,
    expected_slot: crate::storage::cloud::ObjectSlot,
    expected: StoreDeviceHead,
    expected_commit: Box<VerifiedStoreBatchCommit>,
    winner: StoreDeviceHead,
    winner_prepared: PreparedExactObject,
    winner_commit: Box<VerifiedStoreBatchCommit>,
}

impl VerifiedMergeWinner {
    pub(super) fn from_verified_parts(
        store_root_hash: ObjectHash,
        expected_slot: crate::storage::cloud::ObjectSlot,
        expected: StoreDeviceHead,
        expected_commit: VerifiedStoreBatchCommit,
        winner: StoreDeviceHead,
        winner_prepared: PreparedExactObject,
        winner_commit: VerifiedStoreBatchCommit,
    ) -> Self {
        Self {
            store_root_hash,
            expected_slot,
            expected,
            expected_commit: Box::new(expected_commit),
            winner,
            winner_prepared,
            winner_commit: Box::new(winner_commit),
        }
    }

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
        let reference = commit.reference().clone();
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
            || self.winner.commit != *self.winner_commit.reference()
        {
            return Err(
                crate::sync::remote_object::RemoteObjectRecordError::InvalidProof(
                    "Merge winner observation is not bound to the losing candidate's exact activation point"
                        .to_string(),
                ),
            );
        }
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

    pub(crate) fn winner_commit(&self) -> &VerifiedStoreBatchCommit {
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

pub(crate) fn verify_merge_candidate_nonactivations(
    observation: &VerifiedMergeWinner,
    targets: impl IntoIterator<Item = StoreBatchCommitDeletionTarget>,
    author: &StoreDeviceRegistration,
) -> Result<Vec<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError> {
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
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        );
    }
    Ok(nonactivations)
}
