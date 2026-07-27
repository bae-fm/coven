use super::database::StoreDatabase;
use super::StoreError;
use super::*;
use crate::database::PreparedProtocolObject;
use crate::sync::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
};
use crate::sync::store::database::publication_state::MergeCandidateAbandonmentPreparation;
use crate::sync::store_commit::{
    commit_semantic_prefix, head_slot_prefix, CandidateCleanupManifest, ObjectHash,
    StoreBatchCommit, StoreBatchCommitDeletionTarget, StoreDeviceHead, StoreDeviceRegistration,
    VerifiedStoreBatchCommit,
};
use crate::sync::store_objects::StoreObjectError;

use super::operations::load_local_store_authority;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeCandidateAbandonment {
    NotRequired,
    Abandoned,
    CandidateActivated,
}

pub(crate) async fn prepare_merge_candidate_abandonment(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<bool, StoreError> {
    let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await? else {
        return Ok(false);
    };
    let candidate_summary = database
        .blocked_merge_history_summary(write_id.clone())
        .await?;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(database, device_id, identity_signer).await?;
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
    let history_summary = super::pull::prepare_merge_abandonment_history_summary(
        &candidate_summary,
        &candidate.head.value.commit,
        &candidate.commit.value,
        &commit_ref,
        commit.value(),
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
    let head_prefix = head_slot_prefix(device_id, sequence);
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
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
    let db = database.sqlite();
    let root = database.local_store_root_ref().await?.ok_or_else(|| {
        StoreError::InvalidOutbound("Merge abandonment has no Store root".to_string())
    })?;
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root).await?;
    match database.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                super::pull::cleanup_merge_candidate(database, storage, write_id.clone()).await?;
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
                let verified =
                    authenticate_blocked_candidate(&mut history_verifier, &candidate).await?;
                if let Some(nonactivation) = Box::pin(excluded_candidate_nonactivation(
                    database,
                    storage,
                    &root,
                    &mut history_verifier,
                    &verified,
                    &candidate.head.value,
                    &candidate.head.object,
                ))
                .await?
                {
                    database
                        .begin_blocked_merge_candidate_nonactivation(
                            write_id.clone(),
                            nonactivation,
                        )
                        .await?;
                    super::pull::cleanup_merge_candidate(database, storage, write_id.clone())
                        .await?;
                    return Ok(MergeCandidateAbandonment::Abandoned);
                }
            }
            if !prepare_merge_candidate_abandonment(
                database,
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
            let candidates = database
                .prepared_merge_abandonment_candidates(write_id.clone())
                .await?
                .ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "prepared Merge abandonment has no exact candidates".to_string(),
                    )
                })?;
            let verified_candidate =
                authenticate_blocked_candidate(&mut history_verifier, &candidates.candidate)
                    .await?;
            let candidate = Box::pin(excluded_candidate_nonactivation(
                database,
                storage,
                &root,
                &mut history_verifier,
                &verified_candidate,
                &candidates.candidate.head.value,
                &candidates.candidate.head.object,
            ))
            .await?;
            let verified_authority =
                authenticate_blocked_candidate(&mut history_verifier, &candidates.authority)
                    .await?;
            let authority = Box::pin(excluded_candidate_nonactivation(
                database,
                storage,
                &root,
                &mut history_verifier,
                &verified_authority,
                &candidates.authority.head.value,
                &candidates.authority.head.object,
            ))
            .await?;
            match (candidate, authority) {
                (Some(candidate), Some(authority)) => {
                    database
                        .begin_prepared_merge_abandonment_nonactivation(
                            write_id.clone(),
                            candidate,
                            authority,
                        )
                        .await?;
                    super::pull::cleanup_merge_candidate(database, storage, write_id.clone())
                        .await?;
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
                super::pull::cleanup_merge_candidate(database, storage, write_id.clone()).await?;
            }
            return finish_merge_abandonment(database, storage, write_id).await;
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                super::pull::cleanup_merge_candidate(database, storage, write_id.clone()).await?;
            }
            database
                .finish_author_excluded_merge_abandonment(write_id)
                .await?;
            return Ok(MergeCandidateAbandonment::Abandoned);
        }
    }
    crate::sync::store::publication::drain_store_writes(database, storage).await?;
    if !database.merge_candidate_cleanup_pending(&write_id).await? {
        return Err(StoreError::InvalidOutbound(
            "accepted Merge abandonment has no exact cleanup transition".to_string(),
        ));
    }
    super::pull::cleanup_merge_candidate(database, storage, write_id.clone()).await?;
    finish_merge_abandonment(database, storage, write_id).await
}

async fn finish_merge_abandonment(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreError> {
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
            crate::sync::store::publication::drain_store_writes(database, storage).await?;
            Ok(MergeCandidateAbandonment::CandidateActivated)
        }
        crate::database::MergeAbandonmentState::Prepared => Err(StoreError::InvalidOutbound(
            "Merge abandonment has no accepted head outcome".to_string(),
        )),
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if database.merge_candidate_cleanup_pending(&write_id).await? {
                super::pull::cleanup_merge_candidate(database, storage, write_id.clone()).await?;
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
async fn authenticate_blocked_candidate(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    candidate: &crate::database::BlockedMergeCandidate,
) -> Result<crate::sync::store_commit::VerifiedStoreBatchCommit, StoreError> {
    let reference = &candidate.head.value.commit;
    let verified = history_verifier
        .commit_verifier()
        .authenticate_bytes(reference, &candidate.commit.bytes)
        .await?;
    if verified.value() != candidate.commit.value.value()
        || verified.reference().object != candidate.commit.object
        || verified.value().to_bytes() != candidate.commit.bytes
    {
        return Err(StoreError::InvalidOutbound(
            "blocked Merge candidate differs from its authenticated commit".to_string(),
        ));
    }
    Ok(verified)
}

pub(crate) async fn discard_candidate_nonactivation(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    candidate: &crate::database::BlockedMergeCandidate,
    revoked_grant: Option<&crate::sync::membership::MembershipGrantId>,
) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError> {
    let root = database.local_store_root_ref().await?.ok_or_else(|| {
        StoreError::InvalidOutbound("discard candidate has no Store root".to_string())
    })?;
    let mut history_verifier =
        crate::sync::store::pull::MergeHistoryVerifier::new(storage, &root).await?;
    let verified_candidate =
        authenticate_blocked_candidate(&mut history_verifier, candidate).await?;
    if let ExcludedCandidateHeadObservation::MergeWinner(observation) =
        observe_excluded_candidate_head(
            database,
            storage,
            history_verifier.commit_verifier(),
            &candidate.head.value,
            &verified_candidate,
            &candidate.head.object,
        )
        .await?
    {
        let target = StoreBatchCommitDeletionTarget {
            coord: verified_candidate.reference().coord.clone(),
            object: verified_candidate.reference().object.clone(),
            canonical_signed_bytes: verified_candidate.value().to_bytes(),
        };
        return Ok(Some(
            observation
                .verified_nonactivation(target, verified_candidate.author())
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        ));
    }
    if let Some(nonactivation) = excluded_candidate_nonactivation(
        database,
        storage,
        &root,
        &mut history_verifier,
        &verified_candidate,
        &candidate.head.value,
        &candidate.head.object,
    )
    .await?
    {
        return Ok(Some(nonactivation));
    }
    let Some(revoked_grant) = revoked_grant else {
        return Ok(None);
    };
    membership_revocation_candidate_nonactivation(
        database,
        storage,
        &root,
        &mut history_verifier,
        revoked_grant,
        &verified_candidate,
        &candidate.head.value,
        &candidate.head.object,
    )
    .await
}

async fn membership_revocation_candidate_nonactivation(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    revoked_grant: &crate::sync::membership::MembershipGrantId,
    candidate: &crate::sync::store_commit::VerifiedStoreBatchCommit,
    candidate_head: &crate::sync::store_commit::StoreDeviceHead,
    candidate_head_object: &crate::sync::storage::ExactObjectRef,
) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError> {
    let expected_stream = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &candidate.value().author_registration,
        crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let candidate_sequence = candidate.reference().coord.sequence();
    for witness in database.retained_merge_replay_inputs().await? {
        let predecessor_cut = witness
            .commit()
            .order
            .predecessor_cut()
            .map_err(|error| StoreError::Database(error.to_string()))?;
        if predecessor_cut
            .commits()
            .get(&expected_stream)
            .is_some_and(|covered| candidate_sequence <= covered.coord.sequence())
        {
            continue;
        }
        let membership = crate::sync::store::pull::load_merge_predecessor_membership(
            storage,
            root,
            &witness.commit().membership_state,
        )
        .await
        .map_err(|error| match error {
            crate::sync::store::pull::RegistrationLoadError::Object(error) => {
                StoreError::Object(error)
            }
            crate::sync::store::pull::RegistrationLoadError::Invalid(error) => {
                StoreError::Database(error)
            }
        })?;
        let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            continue;
        };
        if !matches!(
            resolved.grants.get(revoked_grant),
            Some(crate::sync::causal_grants::GrantState::Tombstoned { .. })
        ) {
            continue;
        }
        let activation_head = crate::sync::store_commit::StoreDeviceHeadRef {
            head_hash: witness.activation_head().head_hash(),
            object: witness.activation_head_object().clone(),
        };
        return Box::pin(
            crate::sync::store::pull::verify_membership_grant_revocation_nonactivation(
                storage,
                root,
                history_verifier,
                revoked_grant,
                &witness.commit().membership_state,
                witness.commit_ref(),
                &activation_head,
                candidate,
                candidate_head,
                candidate_head_object,
            ),
        )
        .await
        .map(Some)
        .map_err(StoreError::from);
    }
    Ok(None)
}

pub(crate) async fn excluded_candidate_nonactivation(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    candidate: &crate::sync::store_commit::VerifiedStoreBatchCommit,
    candidate_head: &crate::sync::store_commit::StoreDeviceHead,
    candidate_head_object: &crate::sync::storage::ExactObjectRef,
) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError> {
    let candidate_ref = candidate.reference().clone();
    let Some(locator) = database
        .author_exclusion_activation_for_candidate(
            candidate_ref.clone(),
            candidate.value().author_registration.clone(),
        )
        .await?
    else {
        return Ok(None);
    };
    let candidate_target = StoreBatchCommitDeletionTarget {
        coord: candidate_ref.coord.clone(),
        object: candidate_ref.object.clone(),
        canonical_signed_bytes: candidate.value().to_bytes(),
    };
    let nonactivation = match observe_excluded_candidate_head(
        database,
        storage,
        history_verifier.commit_verifier(),
        candidate_head,
        candidate,
        candidate_head_object,
    )
    .await?
    {
        ExcludedCandidateHeadObservation::AuthorExclusion => {
            Box::pin(super::pull::verify_author_exclusion_nonactivation(
                database,
                storage,
                root,
                history_verifier.commit_verifier(),
                &locator,
                candidate,
                candidate_head,
                candidate_head_object,
            ))
            .await?
        }
        ExcludedCandidateHeadObservation::MergeWinner(observation) => observation
            .verified_nonactivation(candidate_target, candidate.author())
            .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
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
    expected_commit: Box<VerifiedStoreBatchCommit>,
    winner: StoreDeviceHead,
    winner_prepared: PreparedExactObject,
    winner_commit: Box<VerifiedStoreBatchCommit>,
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

pub(crate) async fn observe_excluded_candidate_head(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    commit_verifier: &mut crate::sync::store::pull::StoreCommitVerifier<'_>,
    candidate: &StoreDeviceHead,
    candidate_commit: &VerifiedStoreBatchCommit,
    candidate_object: &ExactObjectRef,
) -> Result<ExcludedCandidateHeadObservation, StoreError> {
    let store_root_hash = commit_verifier.root().store_root_hash;
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
            database,
            storage,
            commit_verifier,
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

pub(crate) async fn read_occupied_merge_head(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    commit_verifier: &mut crate::sync::store::pull::StoreCommitVerifier<'_>,
    expected: &StoreDeviceHead,
    expected_commit: &VerifiedStoreBatchCommit,
    slot: &crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
) -> Result<VerifiedMergeWinner, StoreError> {
    let store_root_hash = commit_verifier.root().store_root_hash;
    let context =
        ProtocolObjectContext::signed_plaintext(store_root_hash, ProtocolObjectDomain::StoreHead);
    let (winner_bytes, winner_prepared) = storage
        .read_prepared_protocol_slot(&context, slot, semantic_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified: StoreDeviceHead = serde_json::from_slice(&winner_bytes).map_err(|error| {
        StoreError::InvalidOutbound(format!("parse competing Merge head: {error}"))
    })?;
    if unverified.author_registration != expected.author_registration
        || unverified.commit.coord != expected.commit.coord
        || unverified.successor.activation != expected.successor.activation
        || unverified.successor.predecessor != expected.successor.predecessor
    {
        return Err(StoreError::InvalidOutbound(
            "competing Merge head does not occupy the prepared successor point".to_string(),
        ));
    }
    let registration = database
        .activated_store_device_registration(expected.author_registration.clone())
        .await?;
    if expected_commit.store_root_hash() != store_root_hash
        || expected_commit.reference() != &expected.commit
        || expected_commit.author() != &registration
    {
        return Err(StoreError::InvalidOutbound(
            "expected Merge head differs from its authenticated commit".to_string(),
        ));
    }
    StoreDeviceHead::parse_at(
        &expected.to_bytes(),
        store_root_hash,
        &registration,
        &expected.commit,
    )
    .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
    let winner_commit = commit_verifier.load_ref(&unverified.commit).await?;
    if winner_commit.author() != &registration {
        return Err(StoreError::InvalidOutbound(
            "occupied Merge head commit has a different authenticated author".to_string(),
        ));
    }
    let winner = StoreDeviceHead::parse_at(
        &winner_bytes,
        store_root_hash,
        &registration,
        &unverified.commit,
    )
    .map_err(|error| StoreError::InvalidOutbound(format!("verify occupied Merge head: {error}")))?;
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
