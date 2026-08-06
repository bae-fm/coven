use super::nonactivation::*;
use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn retained_device_state_for_order(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(pull::StorePullError::Protocol)?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        self.retained_merge_device_state(&frontier, &checkpoints)
            .await
    }

    pub(crate) async fn authorize_retained_conflict_resolution(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
        resolver_pubkey: &str,
    ) -> Result<MergeConflictResolutionAuthorization, pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(pull::StorePullError::Protocol)?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(pull::StorePullError::MembershipChain)?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::InvalidState)?;
        let (device_state_ref, device_state) = self
            .retained_merge_device_state(&frontier, &checkpoints)
            .await?;
        if !crate::sync::store::owner::verified_history::registration::device_state_has_active_registration(
            &device_state,
            author_registration,
        ) {
            return Err(pull::StorePullError::InvalidState(
                "Merge conflict-resolution author is inactive at its predecessor cut".to_string(),
            ));
        }
        self.history_verifier
            .verify_canonical_owner_registration(
                &device_state,
                resolver_pubkey,
                author_registration,
            )
            .await?;
        Ok(MergeConflictResolutionAuthorization {
            membership,
            device_state_ref,
            device_state,
        })
    }

    pub(crate) async fn authorize_retained_outbound(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
    ) -> Result<MergeOutboundAuthorization, pull::StorePullError> {
        let frontier = order
            .predecessor_cut()
            .map_err(pull::StorePullError::Protocol)?
            .0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let prefix = VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(pull::StorePullError::MembershipChain)?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::InvalidState)?;
        let (device_state_ref, device_state) = self
            .retained_merge_device_state(&frontier, &checkpoints)
            .await?;
        if !crate::sync::store::owner::verified_history::registration::device_state_has_active_registration(
            &device_state,
            author_registration,
        ) {
            return Err(pull::StorePullError::InvalidState(
                "Merge outbound author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        let MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(pull::StorePullError::InvalidState(
                "Merge outbound predecessor membership is conflicted".to_string(),
            ));
        };
        let membership_state = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            device_state.recovery.clone(),
            resolved.state_hash,
        )
        .map_err(pull::StorePullError::Protocol)?;
        Ok(MergeOutboundAuthorization {
            membership,
            membership_state,
            device_state_ref,
            device_state,
        })
    }

    pub(crate) async fn retained_merge_device_state(
        &self,
        frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
        checkpoints: &[OpenedRetainedMergeHistorySummary],
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
        let state = if checkpoints.is_empty() {
            let founder = self.history_verifier.load_founder_registration().await?;
            let founder_ref = StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object.clone(),
            );
            ResolvedStoreDeviceState::founder(
                self.history_verifier.verified_root().reference(),
                founder_ref,
                &self
                    .history_verifier
                    .verified_root()
                    .protocol()
                    .descriptor
                    .founder_pubkey,
                self.history_verifier
                    .verified_root()
                    .protocol()
                    .descriptor
                    .founder_grant
                    .clone(),
                &self
                    .history_verifier
                    .verified_root()
                    .protocol()
                    .descriptor
                    .founder_recovery,
            )
            .map_err(pull::StorePullError::Protocol)?
        } else {
            ResolvedStoreDeviceState::merge(
                checkpoints
                    .iter()
                    .map(|checkpoint| checkpoint.post_state.clone()),
            )
            .map_err(pull::StorePullError::Protocol)?
        };
        let reference =
            StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.clone()), &state)
                .map_err(pull::StorePullError::Protocol)?;
        Ok((reference, state))
    }

    pub(crate) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &crate::protocol::membership::MembershipChain,
        recovery_author: Option<&crate::protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: crate::protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::owner::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::owner::verified_history::PreparedMergeHistorySuccessor,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let root = self.history_verifier.verified_root().reference();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(
                crate::sync::store::owner::pull::StorePullError::InvalidState(
                    "authenticated Merge successor belongs to another Store root".to_string(),
                ),
            );
        }
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let author = verified_commit.author();
        state_after.validate_canonical().map_err(|error| {
            crate::sync::store::owner::pull::StorePullError::context(
                "validate Merge successor post-state",
                error,
            )
        })?;
        let predecessor_refs =
            crate::sync::store::owner::pull::commit_predecessor_references(commit);
        let predecessors = self
            .retained_history_checkpoints(predecessor_refs.clone())
            .await?;
        let (expected_predecessor_ref, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await
            .map_err(crate::sync::store::owner::pull::StorePullError::Database)?;
        if commit.device_state != expected_predecessor_ref {
            return Err(
                crate::sync::store::owner::pull::StorePullError::InvalidState(
                    "Merge successor names another predecessor device state".to_string(),
                ),
            );
        }
        if let Some(recovery_author) = recovery_author {
            let retained_recovery_registration =
                evidence.registrations.iter().any(|registration| {
                    registration.reference() == recovery_author
                        && matches!(
                            &registration.value().origin,
                            crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                                ..
                            }
                        )
                });
            let recovery_activation =
                commit.device_registrations().iter().any(|activation| {
                    activation.registration == *recovery_author
                        && matches!(
                        &activation.authority,
                        crate::protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                            ..
                        }
                    )
                });
            if recovery_author != &commit.author_registration
                || !retained_recovery_registration
                || !recovery_activation
            {
                return Err(
                    crate::sync::store::owner::pull::StorePullError::InvalidState(
                        "Merge successor recovery author lacks its exact retained activation"
                            .to_string(),
                    ),
                );
            }
        }
        if !crate::sync::store::owner::verified_history::registration::device_state_has_active_registration(
            &predecessor_state,
            &commit.author_registration,
        ) && recovery_author != Some(&commit.author_registration)
        {
            return Err(crate::sync::store::owner::pull::StorePullError::InvalidState(
                "Merge successor author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        crate::sync::store::owner::verified_history::verify_merge_membership_state_ref(
            &commit.membership_state,
            membership,
            &predecessor_state,
        )?;

        compose_merge_history_successor(
            root,
            commit,
            commit_ref,
            membership,
            author,
            state_after,
            predecessors,
            evidence,
        )
    }

    pub(crate) async fn prepare_merge_snapshot_history_summary(
        &self,
        coverage: &crate::protocol::store_commit::CommitFrontier,
        membership: &crate::protocol::membership::MembershipChain,
        state: &crate::protocol::store_commit::ResolvedStoreDeviceState,
        author_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let frontier = &coverage.0;
        let root = self.history_verifier.verified_root().reference();
        let predecessors = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        compose_merge_snapshot_history_summary(
            root,
            coverage,
            membership,
            state,
            author_ref,
            author,
            predecessors,
        )
    }

    pub(crate) async fn observe_occupied_merge_head(
        &mut self,
        expected: &crate::protocol::store_commit::StoreDeviceHead,
        expected_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        slot: &crate::protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<abandonment::VerifiedMergeWinner, StoreError> {
        let store_root_hash = self
            .history_verifier
            .verified_root()
            .reference()
            .store_root_hash;
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let (winner_bytes, winner_prepared) = self
            .storage
            .read_prepared_protocol_slot(&context, slot, semantic_prefix)
            .await
            .map_err(crate::protocol::objects::StoreObjectError::from)?;
        let unverified: crate::protocol::store_commit::StoreDeviceHead =
            serde_json::from_slice(&winner_bytes).map_err(|error| {
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
        let registration = self
            .database
            .activated_store_device_registration(expected.author_registration.clone())
            .await?;
        if expected_commit.store_root_hash() != store_root_hash
            || expected_commit.reference() != &expected.commit
            || expected_commit.author() != registration.value()
        {
            return Err(StoreError::InvalidOutbound(
                "expected Merge head differs from its authenticated commit".to_string(),
            ));
        }
        crate::protocol::store_commit::StoreDeviceHead::parse_at(
            &expected.to_bytes(),
            store_root_hash,
            registration.value(),
            &expected.commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let winner_commit = self.history_verifier.load_ref(&unverified.commit).await?;
        if winner_commit.author() != registration.value() {
            return Err(StoreError::InvalidOutbound(
                "occupied Merge head commit has a different authenticated author".to_string(),
            ));
        }
        let winner = crate::protocol::store_commit::StoreDeviceHead::parse_at(
            &winner_bytes,
            store_root_hash,
            registration.value(),
            &unverified.commit,
        )
        .map_err(|error| {
            StoreError::InvalidOutbound(format!("verify occupied Merge head: {error}"))
        })?;
        Ok(abandonment::VerifiedMergeWinner::from_verified_parts(
            store_root_hash,
            slot.clone(),
            expected.clone(),
            expected_commit.clone(),
            winner,
            winner_prepared,
            winner_commit,
        ))
    }

    pub(crate) async fn retained_history_checkpoints(
        &self,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<OpenedRetainedMergeHistorySummary>, pull::StorePullError> {
        let root = self.history_verifier.verified_root().reference();
        let checkpoints = self
            .database
            .retained_merge_history_frontier(root.clone(), references)
            .await
            .map_err(pull::StorePullError::Database)?;
        if checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
        {
            return Err(pull::StorePullError::InvalidState(
                "Merge operation is missing retained predecessor authority".to_string(),
            ));
        }
        Ok(checkpoints)
    }

    pub(crate) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &crate::protocol::store_commit::StoreDeviceHead,
        candidate_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &crate::protocol::objects::ExactObjectRef,
    ) -> Result<abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let store_root_hash = self
            .history_verifier
            .verified_root()
            .reference()
            .store_root_hash;
        let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::protocol::objects::ProtocolObjectDomain::StoreHead,
        );
        let prefix = crate::protocol::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.commit.coord.sequence(),
        );
        match self
            .storage
            .read_protocol_slot(&context, candidate_object.slot(), &prefix)
            .await
        {
            Err(crate::protocol::objects::StorageError::NotFound(_)) => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok((bytes, object)) if bytes == candidate.to_bytes() && object == *candidate_object => {
                Ok(abandonment::ExcludedCandidateHeadObservation::AuthorExclusion)
            }
            Ok(_) => self
                .observe_occupied_merge_head(
                    candidate,
                    candidate_commit,
                    candidate_object.slot(),
                    &prefix,
                )
                .await
                .map(abandonment::ExcludedCandidateHeadObservation::MergeWinner),
            Err(error) => Err(crate::protocol::objects::StoreObjectError::Storage(error).into()),
        }
    }
}
