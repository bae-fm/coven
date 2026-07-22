use super::*;

#[allow(clippy::too_many_arguments)]
async fn verify_terminal_candidate_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
    candidate_author: &StoreDeviceRegistration,
) -> Result<super::remote_object::VerifiedCandidateHead, StorePullError> {
    if candidate_head.commit != *candidate
        || candidate_head.author_registration != candidate_commit.author_registration
    {
        return Err(StorePullError::Database(
            "terminal candidate head names another commit or author".to_string(),
        ));
    }
    StoreDeviceHead::parse_at(
        &candidate_head.to_bytes(),
        root.store_root_hash,
        candidate_author,
        candidate,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate
        .verify_commit(candidate_commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate_commit
        .verify_at(root.store_root_hash, &candidate.coord, candidate_author)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    candidate_head_object.verify(&candidate_head.to_bytes())?;
    let (candidate_slot, predecessor_head) =
        crate::sync::store::operations::exact_next_announcement_slot(
            storage,
            root,
            &candidate_commit.author_registration,
            candidate_author,
            candidate_commit.order.predecessor(),
        )
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    let activation = candidate_author
        .store_announcement_activation(&candidate_commit.author_registration)
        .map_err(|error| StorePullError::Database(error.to_string()))?
        .activation_id();
    if candidate_slot != *candidate_head_object.slot()
        || candidate_head.successor.activation != activation
        || candidate_head.successor.predecessor
            != predecessor_head.map(|reference| reference.object)
    {
        return Err(StorePullError::Database(
            "terminal candidate head does not occupy its exact successor slot".to_string(),
        ));
    }
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreHead,
    );
    let candidate_prefix = head_slot_prefix(
        &candidate_head.author_registration.device_id.to_string(),
        candidate.coord.sequence(),
    );
    match storage
        .read_protocol_slot(&context, &candidate_slot, &candidate_prefix)
        .await
    {
        Err(StorageError::NotFound(_)) => Ok(
            super::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                object: candidate_head_object.clone(),
            },
        ),
        Ok((bytes, object))
            if bytes == candidate_head.to_bytes() && object == *candidate_head_object =>
        {
            Ok(
                super::remote_object::VerifiedCandidateHead::ExactLateCandidate {
                    object: candidate_head_object.clone(),
                },
            )
        }
        Ok((bytes, object)) => {
            object.verify(&bytes)?;
            let unverified: StoreDeviceHead = serde_json::from_slice(&bytes).map_err(|error| {
                StorePullError::Database(format!(
                    "parse competing terminal candidate head: {error}"
                ))
            })?;
            if object.slot() != candidate_head_object.slot()
                || unverified.author_registration != candidate_head.author_registration
                || unverified.commit.coord != candidate_head.commit.coord
                || unverified.successor != candidate_head.successor
            {
                return Err(StorePullError::Database(
                    "competing terminal candidate head differs from the exact successor point"
                        .to_string(),
                ));
            }
            load_commit_ref(
                storage,
                root.store_root_hash,
                &unverified.commit,
                candidate_author,
            )
            .await?;
            let winner = StoreDeviceHead::parse_at(
                &bytes,
                root.store_root_hash,
                candidate_author,
                &unverified.commit,
            )
            .map_err(|error| StorePullError::Database(error.to_string()))?;
            if winner != unverified {
                return Err(StorePullError::Database(
                    "competing terminal candidate head is not authenticated".to_string(),
                ));
            }
            Ok(
                super::remote_object::VerifiedCandidateHead::ExactCandidateAbsent {
                    object: candidate_head_object.clone(),
                },
            )
        }
        Err(error) => Err(StorePullError::Storage(error)),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn verify_author_exclusion_nonactivation_with_verified_operation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    locator: &crate::database::AuthorExclusionActivationLocator,
    activation_head: &StoreDeviceHead,
    activation_head_object: &ExactObjectRef,
    activation_commit_ref: &StoreBatchCommitRef,
    activation_commit: &StoreBatchCommit,
    activation_predecessor_state: &ResolvedStoreDeviceState,
    operations: &VerifiedStoreDeviceOperations,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
) -> Result<super::remote_object::VerifiedCandidateNonactivation, StorePullError> {
    let verified_activation_head = super::store_commit::StoreDeviceHeadRef {
        head_hash: activation_head.head_hash(),
        object: activation_head_object.clone(),
    };
    activation_commit_ref
        .verify_commit(activation_commit)
        .map_err(|error| StorePullError::Database(error.to_string()))?;
    if activation_head.commit != *activation_commit_ref
        || locator.activation_head() != &verified_activation_head
        || !activation_commit.device_exclusion_outcomes().contains(
            &super::store_commit::StoreDeviceExclusionOutcomeRef::Excluded(
                locator.exclusion().clone(),
            ),
        )
        || !device_state_has_active_registration(
            activation_predecessor_state,
            &locator.exclusion().proposal.target,
        )
    {
        return Err(StorePullError::Database(
            "author exclusion activation differs from its verified commit and predecessor"
                .to_string(),
        ));
    }
    let exact_cut = operations
        .exclusions()
        .find_map(|(exclusion, cut)| (exclusion == locator.exclusion()).then_some(cut));
    if exact_cut != Some(&StoreHistoryCut(locator.accepted_cut().clone())) {
        return Err(StorePullError::Database(
            "author exclusion locator differs from the verified outcome cutoff".to_string(),
        ));
    }
    let target_registration = Box::pin(load_registration_ref(
        storage,
        root,
        &locator.exclusion().proposal.target,
    ))
    .await?;
    if candidate_head.commit != *candidate
        || candidate_head.author_registration != locator.exclusion().proposal.target
        || candidate_commit.author_registration != candidate_head.author_registration
    {
        return Err(StorePullError::Database(
            "candidate head differs from the excluded author and exact candidate".to_string(),
        ));
    }
    let verified_candidate_head = Box::pin(verify_terminal_candidate_head(
        storage,
        root,
        candidate,
        candidate_commit,
        candidate_head,
        candidate_head_object,
        &target_registration.value,
    ))
    .await?;
    let durable = super::remote_object::CandidateNonactivation::from_durable_parts(
        candidate,
        candidate_commit,
        super::remote_object::CandidateNonactivationProof::AuthorExclusion {
            exclusion: locator.exclusion().clone(),
            accepted_cut: locator.accepted_cut().clone(),
            activation_head: verified_activation_head,
        },
    )
    .map_err(|error| StorePullError::Database(error.to_string()))?;
    super::remote_object::VerifiedCandidateNonactivation::from_verified_author_exclusion(
        durable,
        candidate.clone(),
        verified_candidate_head,
    )
    .map_err(|error| StorePullError::Database(error.to_string()))
}

pub(crate) async fn verify_author_exclusion_nonactivation(
    db: &Database,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    locator: &crate::database::AuthorExclusionActivationLocator,
    candidate: &StoreBatchCommitRef,
    candidate_commit: &StoreBatchCommit,
    candidate_head: &StoreDeviceHead,
    candidate_head_object: &ExactObjectRef,
) -> Result<super::remote_object::VerifiedCandidateNonactivation, StorePullError> {
    let retained =
        Box::pin(db.retained_merge_materialization(locator.activation_commit().clone())).await?;
    let (_, predecessor_state) =
        Box::pin(db.store_device_state_for_order(&retained.commit().order)).await?;
    Box::pin(
        verify_author_exclusion_nonactivation_with_verified_operation(
            storage,
            root,
            locator,
            retained.activation_head(),
            retained.activation_head_object(),
            retained.commit_ref(),
            retained.commit(),
            &predecessor_state,
            retained.device_operations(),
            candidate,
            candidate_commit,
            candidate_head,
            candidate_head_object,
        ),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn verify_membership_grant_revocation_nonactivation<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    grant_id: &'a super::membership::MembershipGrantId,
    membership: &'a crate::sync::circle_control::StoreMembershipStateRef,
    activation_commit: &'a StoreBatchCommitRef,
    activation_head: &'a super::store_commit::StoreDeviceHeadRef,
    candidate: &'a StoreBatchCommitRef,
    candidate_commit: &'a StoreBatchCommit,
    candidate_head: &'a StoreDeviceHead,
    candidate_head_object: &'a ExactObjectRef,
) -> StorePullFuture<'a, super::remote_object::VerifiedCandidateNonactivation> {
    Box::pin(async move {
        let head_prefix = super::store_commit::semantic_prefix_from_exact_object(
            &activation_head.object,
            ".json",
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_bytes = storage
            .read_protocol_object(&context, &activation_head.object, &head_prefix)
            .await?;
        activation_head.object.verify(&head_bytes)?;
        let witness_head: StoreDeviceHead =
            serde_json::from_slice(&head_bytes).map_err(|error| {
                StorePullError::Database(format!("membership revocation witness head: {error}"))
            })?;
        if witness_head.head_hash() != activation_head.head_hash
            || &witness_head.commit != activation_commit
        {
            return Err(StorePullError::Database(
                "membership revocation witness head differs from its exact activation".to_string(),
            ));
        }
        let witness_author =
            load_registration_ref(storage, root, &witness_head.author_registration).await?;
        let opened = super::store_objects::load_head_ref(
            storage,
            root.store_root_hash,
            activation_head,
            &witness_author.value,
            &witness_head.commit,
        )
        .await?;
        let (_, exact_head) = crate::sync::store::operations::exact_next_announcement_slot(
            storage,
            root,
            &witness_head.author_registration,
            &witness_author.value,
            Some(&witness_head.commit),
        )
        .await
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        if exact_head.as_ref() != Some(activation_head) || opened.value != witness_head {
            return Err(StorePullError::Database(
                "membership revocation witness is not an accepted exact head".to_string(),
            ));
        }
        let witness_commit = load_commit_ref(
            storage,
            root.store_root_hash,
            &witness_head.commit,
            &witness_author.value,
        )
        .await?;
        let (_, _, replayed_witness_commit, _) = Box::pin(replay_merge_device_history(
            storage,
            root,
            &witness_head.commit,
        ))
        .await?;
        if replayed_witness_commit != witness_commit.value {
            return Err(StorePullError::Database(
                "membership revocation witness commit differs from its verified history"
                    .to_string(),
            ));
        }
        if witness_commit.value.membership_state != *membership {
            return Err(StorePullError::Database(
                "membership revocation witness commit names another membership state".to_string(),
            ));
        }
        let current_membership = load_merge_predecessor_membership(
            storage,
            root,
            &witness_commit.value.membership_state,
        )
        .await
        .map_err(|error| match error {
            RegistrationLoadError::Object(error) => StorePullError::Object(error),
            RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
        })?;
        let MembershipStatus::Resolved(current) = current_membership.status() else {
            return Err(StorePullError::Database(
                "membership revocation witness state is conflicted".to_string(),
            ));
        };
        let Some(super::causal_grants::GrantState::Tombstoned {
            record: current_record,
            ..
        }) = current.grants.get(grant_id)
        else {
            return Err(StorePullError::Database(
                "membership revocation witness grant is not tombstoned".to_string(),
            ));
        };
        let candidate_author =
            load_registration_ref(storage, root, &candidate_commit.author_registration).await?;
        candidate
            .verify_commit(candidate_commit)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let predecessor_membership =
            load_merge_predecessor_membership(storage, root, &candidate_commit.membership_state)
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                })?;
        let MembershipStatus::Resolved(predecessor) = predecessor_membership.status() else {
            return Err(StorePullError::Database(
                "membership revocation candidate predecessor is conflicted".to_string(),
            ));
        };
        let Some(predecessor_record) = predecessor.active_grant(grant_id) else {
            return Err(StorePullError::Database(
                "membership revocation grant was not active at the candidate predecessor"
                    .to_string(),
            ));
        };
        if predecessor_record != current_record
            || predecessor_record.member_pubkey != candidate_author.value.author_pubkey
            || candidate_commit.membership_authority.as_ref()
                != Some(&predecessor_record.creation_authority)
        {
            return Err(StorePullError::Database(
                "membership revocation grant differs from the candidate's signed authority"
                    .to_string(),
            ));
        }
        let cap = witness_commit
            .value
            .order
            .predecessor_cut()
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let expected_stream = super::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &candidate_commit.author_registration,
            super::store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = candidate.coord;
        if stream_id != expected_stream
            || cap
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| sequence <= covered.coord.sequence())
        {
            return Err(StorePullError::Database(
                "membership revocation candidate is not beyond the accepted witness cut"
                    .to_string(),
            ));
        }
        let verified_candidate_head = verify_terminal_candidate_head(
            storage,
            root,
            candidate,
            candidate_commit,
            candidate_head,
            candidate_head_object,
            &candidate_author.value,
        )
        .await?;
        let durable = super::remote_object::CandidateNonactivation::from_durable_parts(
            candidate,
            candidate_commit,
            super::remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
                grant_id: grant_id.clone(),
                membership: membership.clone(),
                activation_commit: witness_head.commit,
                activation_head: activation_head.clone(),
            },
        )
        .map_err(|error| StorePullError::Database(error.to_string()))?;
        super::remote_object::VerifiedCandidateNonactivation::from_verified_membership_grant_revocation(
            durable,
            candidate.clone(),
            verified_candidate_head,
        )
        .map_err(|error| StorePullError::Database(error.to_string()))
    })
}
