use super::*;

pub(crate) struct MergeConflictResolutionAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) device_state_ref: StoreDeviceStateRef,
    pub(crate) device_state: ResolvedStoreDeviceState,
}

pub(crate) enum TerminalNonactivationCandidate {
    StoreWrite {
        write_id: coven_protocol::write::WriteId,
        verification: coven_database::TerminalCandidateCleanupVerification,
    },
    CircleOperation {
        operation_id: coven_protocol::circle::CircleOperationId,
        verification: coven_database::TerminalCandidateCleanupVerification,
    },
    MergeRetraction {
        reference: coven_protocol::store_commit::StoreBatchCommitRef,
        verification: coven_database::TerminalCandidateCleanupVerification,
    },
}

pub(crate) fn validate_retained_membership_floors(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
    membership: &MembershipChain,
) -> Result<(), pull::StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        !checkpoint
            .summary
            .membership_floor
            .is_included_in(membership)
    }) {
        return Err(pull::StorePullError::InvalidState(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn discard_candidate_nonactivation(
        &mut self,
        candidate: &coven_database::BlockedMergeCandidate,
        revoked_grant: Option<&coven_protocol::membership::MembershipGrantId>,
    ) -> Result<Option<coven_protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let verified_candidate = self
            .history_verifier
            .authenticate_blocked_candidate(candidate)
            .await?;
        if let abandonment::ExcludedCandidateHeadObservation::MergeWinner(observation) = self
            .observe_excluded_candidate_head(
                &candidate.head.value,
                &verified_candidate,
                &candidate.head.object,
            )
            .await?
        {
            let target = coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
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
        if let Some(nonactivation) = self
            .excluded_candidate_nonactivation(
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
        self.membership_revocation_candidate_nonactivation(
            revoked_grant,
            &verified_candidate,
            &candidate.head.value,
            &candidate.head.object,
        )
        .await
    }

    pub(crate) async fn membership_revocation_candidate_nonactivation(
        &mut self,
        revoked_grant: &coven_protocol::membership::MembershipGrantId,
        candidate: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<Option<coven_protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let root = self.history_verifier.verified_root().reference().clone();
        let expected_stream =
            coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &candidate.value().author_registration,
                coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        let candidate_sequence = candidate.reference().coord.sequence();
        for witness in self
            .database
            .retained_merge_replay_inputs(root.clone())
            .await?
        {
            let predecessor_cut = witness
                .commit()
                .order
                .predecessor_cut()
                .map_err(StoreError::Protocol)?;
            if predecessor_cut
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| candidate_sequence <= covered.coord.sequence())
            {
                continue;
            }
            let membership = self
                .history_verifier
                .load_predecessor_membership(&witness.commit().membership_state)
                .await
                .map_err(|error| match error {
                    crate::sync::store::owner::verified_history::registration::RegistrationLoadError::Object(error) => {
                        StoreError::Object(error)
                    }
                    crate::sync::store::owner::verified_history::registration::RegistrationLoadError::Invalid(
                        error,
                    ) => StoreError::InvalidOutbound(error),
                })?;
            let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
                membership.status()
            else {
                continue;
            };
            if !matches!(
                resolved.grants.get(revoked_grant),
                Some(coven_protocol::causal_grants::GrantState::Tombstoned { .. })
            ) {
                continue;
            }
            let activation_head = coven_protocol::store_commit::StoreDeviceHeadRef {
                head_hash: witness.activation_head().head_hash(),
                object: witness.activation_head_object().clone(),
            };
            return self
                .history_verifier
                .verify_membership_grant_revocation_nonactivation(
                    revoked_grant,
                    &witness.commit().membership_state,
                    witness.commit_ref(),
                    &activation_head,
                    candidate,
                    candidate_head,
                    candidate_head_object,
                )
                .await
                .map(Some)
                .map_err(StoreError::from);
        }
        Ok(None)
    }

    pub(crate) async fn excluded_candidate_nonactivation(
        &mut self,
        candidate: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<Option<coven_protocol::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let candidate_ref = candidate.reference().clone();
        let root = self.history_verifier.verified_root().reference().clone();
        let Some(locator) = self
            .database
            .author_exclusion_activation_for_candidate(
                root,
                candidate_ref.clone(),
                candidate.value().author_registration.clone(),
            )
            .await?
        else {
            return Ok(None);
        };
        let candidate_target = coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: candidate_ref.coord.clone(),
            object: candidate_ref.object.clone(),
            canonical_signed_bytes: candidate.value().to_bytes(),
        };
        let nonactivation = match self
            .observe_excluded_candidate_head(candidate_head, candidate, candidate_head_object)
            .await?
        {
            abandonment::ExcludedCandidateHeadObservation::AuthorExclusion => {
                self.verify_author_exclusion_nonactivation(
                    &locator,
                    candidate,
                    candidate_head,
                    candidate_head_object,
                )
                .await?
            }
            abandonment::ExcludedCandidateHeadObservation::MergeWinner(observation) => observation
                .verified_nonactivation(candidate_target, candidate.author())
                .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?,
        };
        Ok(Some(nonactivation))
    }

    pub(crate) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &coven_database::AuthorExclusionActivationLocator,
        candidate: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_head_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<
        coven_protocol::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let retained = self
            .database
            .retained_merge_materialization(
                self.history_verifier.verified_root().reference().clone(),
                locator.activation_commit().clone(),
            )
            .await?;
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&retained.commit().order)
            .await?;
        let activation_commit = self
            .history_verifier
            .load_ref(retained.commit_ref())
            .await?;
        if activation_commit.value() != retained.commit() {
            return Err(
                crate::sync::store::owner::pull::StorePullError::InvalidState(
                    "retained exclusion activation differs from its authenticated commit"
                        .to_string(),
                ),
            );
        }
        self.history_verifier
            .verify_author_exclusion_nonactivation(
                locator,
                retained.activation_head(),
                retained.activation_head_object(),
                &activation_commit,
                &predecessor_state,
                retained.device_operations(),
                candidate,
                candidate_head,
                candidate_head_object,
            )
            .await
    }

    pub(crate) async fn apply_terminal_nonactivation(
        &mut self,
        candidate: TerminalNonactivationCandidate,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let verification = match &candidate {
            TerminalNonactivationCandidate::StoreWrite { verification, .. }
            | TerminalNonactivationCandidate::CircleOperation { verification, .. }
            | TerminalNonactivationCandidate::MergeRetraction { verification, .. } => verification,
        };
        let nonactivation = self.verify_terminal_nonactivation(verification).await?;
        let root = self.history_verifier.verified_root().reference().clone();
        match candidate {
            TerminalNonactivationCandidate::StoreWrite { write_id, .. } => {
                self.database
                    .reconcile_merge_candidate_terminal_head(root, write_id, nonactivation)
                    .await?;
            }
            TerminalNonactivationCandidate::CircleOperation { operation_id, .. } => {
                self.database
                    .reconcile_circle_operation_terminal_head(root, &operation_id, nonactivation)
                    .await?;
            }
            TerminalNonactivationCandidate::MergeRetraction { reference, .. } => {
                self.database
                    .confirm_merge_retraction_cleanup_nonactivation(root, reference, nonactivation)
                    .await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn verify_terminal_nonactivation(
        &mut self,
        verification: &coven_database::TerminalCandidateCleanupVerification,
    ) -> Result<
        coven_protocol::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let reference = &verification.candidate.head.value.commit;
        let candidate = self
            .history_verifier
            .authenticate_bytes(reference, &verification.candidate.commit.bytes)
            .await?;
        if candidate.value() != verification.candidate.commit.value.value() {
            return Err(
                crate::sync::store::owner::pull::StorePullError::InvalidState(
                    "terminal cleanup candidate differs from its authenticated commit".to_string(),
                ),
            );
        }
        let target = coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
            coord: reference.coord.clone(),
            object: verification.candidate.commit.object.clone(),
            canonical_signed_bytes: verification.candidate.commit.bytes.clone(),
        };
        match &verification.authority {
            coven_database::TerminalCandidateAuthority::AuthorExclusion(locator) => {
                self.verify_author_exclusion_nonactivation(
                    locator,
                    &candidate,
                    &verification.candidate.head.value,
                    &verification.candidate.head.object,
                )
                .await
            }
            coven_database::TerminalCandidateAuthority::MembershipGrantRevocation {
                grant_id,
                membership,
                activation_commit,
                activation_head,
            } => {
                self.history_verifier
                    .verify_membership_grant_revocation_nonactivation(
                        grant_id,
                        membership,
                        activation_commit,
                        activation_head,
                        &candidate,
                        &verification.candidate.head.value,
                        &verification.candidate.head.object,
                    )
                    .await
            }
            coven_database::TerminalCandidateAuthority::DependencyRetraction(authority) => {
                coven_protocol::remote_object::VerifiedCandidateNonactivation::from_verified_dependency_retraction_authority(
                    authority.clone(),
                    target,
                    candidate.author(),
                    verification.candidate.head.object.clone(),
                )
                .map_err(crate::sync::store::owner::pull::StorePullError::RemoteObject)
            }
        }
    }
}
