use super::*;

pub async fn prepare_merge_candidate_abandonment(
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
    let history_summary = super::store_pull::prepare_merge_abandonment_history_summary(
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

pub async fn prepare_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    branch_id: crate::PendingBranchId,
) -> Result<bool, StoreOutboundError> {
    if let Some(prepared) = db.prepared_serial_candidate_abandonment().await? {
        if prepared.branch_id != branch_id {
            return Err(StoreOutboundError::InvalidOutbound(
                "another Serial branch already owns candidate abandonment".to_string(),
            ));
        }
        return Ok(false);
    }
    let branch = db.prepared_serial_store_branch().await?.ok_or_else(|| {
        StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment requires an exact prepared branch".to_string(),
        )
    })?;
    if branch.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate abandonment names another prepared branch".to_string(),
        ));
    }
    let candidate = branch.writes.first().ok_or_else(|| {
        StoreOutboundError::InvalidOutbound("prepared Serial branch has no candidates".to_string())
    })?;
    let (root, registration_ref, registration, device_signer) =
        load_local_store_authority(db, device_id, identity_signer).await?;
    if candidate.commit.value.author_registration != registration_ref {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial candidate belongs to another local registration".to_string(),
        ));
    }
    let coord = StoreCommitCoord::Serial {
        sequence: candidate.commit.value.seq(),
    };
    let candidate_ref = StoreBatchCommitRef::from_commit(
        &candidate.commit.value,
        coord.clone(),
        candidate.commit.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let commit = StoreBatchCommit::signed_with_candidate_abandonment(
        root.store_root_hash,
        candidate.commit.value.write_id.clone(),
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
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        commit.candidate_family(),
        SERIAL_STREAM_ID,
        commit.seq(),
        commit.commit_hash(),
    );
    let slot = storage
        .allocate_protocol_slot(&context, &prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(&context, slot, &prefix, commit.to_bytes())
        .map_err(StoreObjectError::from)?;
    let authority_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    let head = StoreSerialHead::signed(
        root.store_root_hash,
        StoreSerialHeadState::Commit {
            author_registration: registration_ref,
            commit: authority_ref,
        },
        &device_signer,
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.prepare_serial_candidate_abandonment(SerialCandidateAbandonmentPreparation {
        branch_id,
        candidate: candidate_ref,
        commit: PreparedProtocolObject {
            value: commit,
            prepared: commit_prepared,
        },
        head,
        original_head_bytes: branch.head.bytes,
    })
    .await?;
    Ok(true)
}

pub async fn publish_serial_candidate_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    branch_id: crate::PendingBranchId,
) -> Result<SerialCandidateAbandonmentWinner, StoreOutboundError> {
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment is not prepared".to_string(),
            )
        })?;
    if prepared.branch_id != branch_id {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment belongs to another branch".to_string(),
        ));
    }
    let classify = |observed: SerialHeadObservation| {
        let accepted = observed.versioned().ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial abandonment observed no versioned coordination head".to_string(),
            )
        })?;
        if observed.bytes() == Some(prepared.head.bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::Authority { accepted });
        }
        if observed.bytes() == Some(prepared.original_head_bytes.as_slice()) {
            return Ok(SerialCandidateAbandonmentWinner::OriginalBranch { accepted });
        }
        Ok(SerialCandidateAbandonmentWinner::Other {
            current: observed.predecessor()?,
        })
    };
    let observed = observe_serial_head(db, coordination).await?;
    if observed.bytes() == Some(prepared.head.bytes.as_slice())
        || observed.bytes() == Some(prepared.original_head_bytes.as_slice())
        || observed.predecessor()? != db.exact_serial_predecessor(prepared.base.clone()).await?
    {
        return classify(observed);
    }
    if observed.bytes() != Some(prepared.base_head.bytes.as_slice())
        || observed.version() != Some(&prepared.base_head.version)
    {
        return Err(StoreOutboundError::InvalidState {
            key: SERIAL_COORDINATION_HEAD,
            reason: "bytes or provider version changed at the abandonment base".to_string(),
        });
    }
    storage
        .create_protocol_object(&prepared.authority.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let context = ProtocolObjectContext::signed_plaintext(
        prepared.authority.value.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        prepared.authority.value.candidate_family(),
        SERIAL_STREAM_ID,
        prepared.authority.value.seq(),
        prepared.authority.value.commit_hash(),
    );
    let opened = storage
        .read_protocol_object(&context, &prepared.authority.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != prepared.authority.bytes {
        return Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    let authority_ref = StoreBatchCommitRef::from_commit(
        &prepared.authority.value,
        StoreCommitCoord::Serial {
            sequence: prepared.authority.value.seq(),
        },
        prepared.authority.object.clone(),
    )
    .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
    db.mark_candidate_commit_uploaded(authority_ref).await?;
    let activation = coordination
        .replace_head(
            serial_head_key(),
            &prepared.base_head.version,
            &prepared.head.bytes,
        )
        .await;
    match activation {
        Ok(accepted) if accepted.bytes == prepared.head.bytes => {
            Ok(SerialCandidateAbandonmentWinner::Authority { accepted })
        }
        Ok(_) => Err(StoreOutboundError::InvalidOutbound(
            "Serial abandonment head readback differs from its signed bytes".to_string(),
        )),
        Err(ReplaceHeadError::Coordination(error)) => {
            let after = observe_serial_head(db, coordination).await?;
            if after.bytes() != Some(prepared.base_head.bytes.as_slice())
                || after.version() != Some(&prepared.base_head.version)
            {
                classify(after)
            } else {
                Err(StoreOutboundError::Coordination(error))
            }
        }
        Err(ReplaceHeadError::VersionMismatch) => {
            classify(observe_serial_head(db, coordination).await?)
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn abandon_serial_branch(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    branch_id: crate::PendingBranchId,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    prepare_serial_candidate_abandonment(
        db,
        storage,
        device_id,
        identity_signer,
        branch_id.clone(),
    )
    .await?;
    let prepared = db
        .prepared_serial_candidate_abandonment()
        .await?
        .ok_or_else(|| {
            StoreOutboundError::InvalidOutbound(
                "Serial candidate abandonment disappeared after preparation".to_string(),
            )
        })?;
    let winner =
        publish_serial_candidate_abandonment(db, storage, coordination, branch_id.clone()).await?;
    let root = required_store_root(db).await?;
    match winner {
        SerialCandidateAbandonmentWinner::OriginalBranch { accepted } => {
            let plan = super::store_pull::prepare_serial_resolution(
                db,
                storage,
                coordination,
                root.store_root_hash,
                store_dir,
                prepared.base,
                identity_signer,
            )
            .await?;
            super::store_pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
            db.remove_losing_serial_abandonment_authority().await?;
            db.complete_prepared_serial_branch(accepted).await?;
            Ok(SerialBranchAbandonment::OriginalBranchActivated)
        }
        SerialCandidateAbandonmentWinner::Authority { .. } => {
            let authority_ref = StoreBatchCommitRef::from_commit(
                &prepared.authority.value,
                StoreCommitCoord::Serial {
                    sequence: prepared.authority.value.seq(),
                },
                prepared.authority.object,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?;
            db.mark_serial_branch_conflict(
                branch_id.clone(),
                prepared.base.clone(),
                StoreSerialPredecessor::Commit(authority_ref),
            )
            .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
        SerialCandidateAbandonmentWinner::Other { current } => {
            db.mark_serial_branch_conflict(branch_id.clone(), prepared.base.clone(), current)
                .await?;
            finish_serial_branch_abandonment(
                db,
                storage,
                coordination,
                identity_signer,
                store_dir,
                root.store_root_hash,
                branch_id,
                prepared.base,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish_serial_branch_abandonment(
    db: &Database,
    storage: &dyn SyncStorage,
    coordination: &dyn CoordinationStorage,
    identity_signer: &UserKeypair,
    store_dir: &StoreDir,
    store_root_hash: ObjectHash,
    branch_id: crate::PendingBranchId,
    branch_base: Option<StoreBatchCommitRef>,
) -> Result<SerialBranchAbandonment, StoreOutboundError> {
    let plan = super::store_pull::prepare_serial_resolution(
        db,
        storage,
        coordination,
        store_root_hash,
        store_dir,
        branch_base,
        identity_signer,
    )
    .await?;
    super::store_pull::cleanup_serial_candidates(db, storage, branch_id.clone(), &plan).await?;
    super::store_pull::cleanup_serial_abandonment_authority(db, storage, &plan).await?;
    db.discard_serial_branch_after_abandonment(branch_id, plan)
        .await?;
    Ok(SerialBranchAbandonment::Discarded)
}

pub async fn abandon_merge_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    device_id: &str,
    identity_signer: &UserKeypair,
    write_id: crate::WriteId,
) -> Result<MergeCandidateAbandonment, StoreOutboundError> {
    match db.merge_abandonment_state(&write_id).await? {
        crate::database::MergeAbandonmentState::None => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
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
                    super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
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
                    super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone())
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
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
            }
            return finish_merge_abandonment(db, storage, write_id).await;
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
            }
            db.finish_author_excluded_merge_abandonment(write_id)
                .await?;
            return Ok(MergeCandidateAbandonment::Abandoned);
        }
    }
    drain_merge_store_writes(db, storage).await?;
    if !db.merge_candidate_cleanup_pending(&write_id).await? {
        return Err(StoreOutboundError::InvalidOutbound(
            "accepted Merge abandonment has no exact cleanup transition".to_string(),
        ));
    }
    super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
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
            drain_merge_store_writes(db, storage).await?;
            Ok(MergeCandidateAbandonment::CandidateActivated)
        }
        crate::database::MergeAbandonmentState::Prepared => {
            Err(StoreOutboundError::InvalidOutbound(
                "Merge abandonment has no accepted head outcome".to_string(),
            ))
        }
        crate::database::MergeAbandonmentState::AuthorExcluded => {
            if db.merge_candidate_cleanup_pending(&write_id).await? {
                super::store_pull::cleanup_merge_candidate(db, storage, write_id.clone()).await?;
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
) -> Result<Option<super::remote_object::VerifiedCandidateNonactivation>, StoreOutboundError> {
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
            let activation = Box::pin(super::store_pull::verify_author_exclusion_activation(
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
            super::remote_object::VerifiedCandidateNonactivation::author_exclusion(
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
            super::remote_object::VerifiedCandidateNonactivation::merge(
                &observation,
                candidate_target,
                &registration,
            )
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
    pub(crate) fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn expected(&self) -> &StoreDeviceHead {
        &self.expected
    }

    pub(crate) fn expected_commit(&self) -> &StoreBatchCommit {
        &self.expected_commit
    }

    pub(crate) fn expected_slot(&self) -> &crate::storage::cloud::ObjectSlot {
        &self.expected_slot
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

    pub(super) fn into_head(self) -> (StoreDeviceHead, PreparedExactObject) {
        (self.winner, self.winner_prepared)
    }

    #[cfg(test)]
    pub(super) fn winner_mut_for_test(&mut self) -> &mut StoreDeviceHead {
        &mut self.winner
    }

    #[cfg(test)]
    pub(super) fn set_expected_slot_for_test(
        &mut self,
        expected_slot: crate::storage::cloud::ObjectSlot,
    ) {
        self.expected_slot = expected_slot;
    }
}

pub(super) enum ExcludedCandidateHeadObservation {
    AuthorExclusion,
    MergeWinner(VerifiedMergeWinner),
}

pub(super) async fn observe_excluded_candidate_head(
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

pub(super) fn verify_merge_candidate_nonactivations(
    observation: &VerifiedMergeWinner,
    targets: impl IntoIterator<Item = StoreBatchCommitDeletionTarget>,
    author: &StoreDeviceRegistration,
) -> Result<Vec<super::remote_object::VerifiedCandidateNonactivation>, StoreOutboundError> {
    let mut nonactivations = Vec::new();
    for target in targets {
        if target.coord == observation.winner().commit.coord
            && target.object == observation.winner().commit.object
            && target.canonical_signed_bytes == observation.winner_commit().to_bytes()
        {
            continue;
        }
        nonactivations.push(
            super::remote_object::VerifiedCandidateNonactivation::merge(
                observation,
                target,
                author,
            )
            .map_err(|error| StoreOutboundError::InvalidOutbound(error.to_string()))?,
        );
    }
    Ok(nonactivations)
}

pub(super) async fn read_occupied_merge_head(
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
    let winner_commit = super::store_objects::load_commit_ref(
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
