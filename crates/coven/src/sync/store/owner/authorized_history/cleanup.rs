use super::nonactivation::*;
use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn cleanup_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.verified_root().reference().clone();
        for verification in self
            .database
            .merge_candidate_terminal_verifications(root, write_id.clone())
            .await?
        {
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::StoreWrite {
                write_id: write_id.clone(),
                verification,
            })
            .await?;
        }
        let targets = self
            .database
            .merge_candidate_cleanup_targets(write_id)
            .await?;
        delete_candidate_cleanup_targets(self.storage.as_ref(), &self.database, targets).await
    }

    pub(crate) async fn cleanup_circle_operation_candidate(
        &mut self,
        operation_id: &crate::protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.verified_root().reference().clone();
        for verification in self
            .database
            .circle_operation_discard_terminal_verifications(root, operation_id)
            .await?
        {
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::CircleOperation {
                operation_id: operation_id.clone(),
                verification,
            })
            .await?;
        }
        let targets = self
            .database
            .circle_operation_discard_cleanup_targets(operation_id)
            .await?;
        delete_candidate_cleanup_targets(self.storage.as_ref(), &self.database, targets).await
    }

    pub(crate) async fn resume_merge_retraction_cleanups(
        &mut self,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        for candidate in self.database.pending_merge_retraction_cleanups().await? {
            let root = self.history_verifier.verified_root().reference().clone();
            let verification = self
                .database
                .merge_retraction_cleanup_verification(root, candidate.clone())
                .await?;
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::MergeRetraction {
                reference: candidate.clone(),
                verification,
            })
            .await?;
            let targets = self
                .database
                .merge_retraction_cleanup_targets(candidate.clone())
                .await?;
            delete_candidate_cleanup_targets::<crate::sync::store::owner::pull::StorePullError>(
                self.storage.as_ref(),
                &self.database,
                targets,
            )
            .await?;
            self.database
                .finish_merge_retraction_cleanup(candidate)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn materialize_device_join_activation(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.materialize_device_join_activation_inner(reference, expected_outcome, membership_state)
            .await
    }

    pub(crate) async fn abandon_excluded_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<Option<abandonment::MergeCandidateAbandonment>, StoreError> {
        let root = self.history_verifier.verified_root().reference().clone();
        let database = self.database.clone();
        let db = &database;
        match database.merge_abandonment_state(&write_id).await? {
            crate::database::MergeAbandonmentState::None => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                    database
                        .finish_retracted_merge_candidate_cleanup(write_id)
                        .await?;
                    return Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned));
                }
                if matches!(
                    db.write_status(&write_id).await?,
                    crate::WriteStatus::Resolved(_)
                ) {
                    return Ok(Some(abandonment::MergeCandidateAbandonment::NotRequired));
                }
                let Some(candidate) = database.blocked_merge_candidate(write_id.clone()).await?
                else {
                    return Ok(None);
                };
                let verified = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidate)
                    .await?;
                let Some(nonactivation) = self
                    .excluded_candidate_nonactivation(
                        &verified,
                        &candidate.head.value,
                        &candidate.head.object,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                database
                    .begin_blocked_merge_candidate_nonactivation(
                        root.clone(),
                        write_id.clone(),
                        nonactivation,
                    )
                    .await?;
                self.cleanup_merge_candidate(write_id).await?;
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
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
                let verified_candidate = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidates.candidate)
                    .await?;
                let candidate = self
                    .excluded_candidate_nonactivation(
                        &verified_candidate,
                        &candidates.candidate.head.value,
                        &candidates.candidate.head.object,
                    )
                    .await?;
                let verified_authority = self
                    .history_verifier
                    .authenticate_blocked_candidate(&candidates.authority)
                    .await?;
                let authority = self
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
                        self.cleanup_merge_candidate(write_id.clone()).await?;
                        database
                            .finish_author_excluded_merge_abandonment(write_id)
                            .await?;
                        Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
                    }
                    (None, None) => Ok(None),
                    _ => Err(StoreError::InvalidOutbound(
                        "prepared Merge abandonment candidates disagree on author exclusion"
                            .to_string(),
                    )),
                }
            }
            crate::database::MergeAbandonmentState::Accepted
            | crate::database::MergeAbandonmentState::OtherWon => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                if matches!(
                    database.merge_abandonment_state(&write_id).await?,
                    crate::database::MergeAbandonmentState::OtherWon
                ) {
                    database.finish_lost_merge_abandonment(write_id).await?;
                }
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
            }
            crate::database::MergeAbandonmentState::AuthorExcluded => {
                if database.merge_candidate_cleanup_pending(&write_id).await? {
                    self.cleanup_merge_candidate(write_id.clone()).await?;
                }
                database
                    .finish_author_excluded_merge_abandonment(write_id)
                    .await?;
                Ok(Some(abandonment::MergeCandidateAbandonment::Abandoned))
            }
            crate::database::MergeAbandonmentState::CandidateWon => Ok(None),
        }
    }

    pub(crate) async fn materialize_device_join_activation_inner(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::protocol::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), pull::StorePullError> {
        let root = self.history_verifier.verified_root().reference().clone();
        let crate::protocol::store_commit::StoreCommitCoord {
            stream_id,
            sequence,
        } = reference.coord;
        let stream_id = stream_id.to_string();
        if let Some(materialized) = self
            .database
            .exact_materialized_ref(&stream_id, sequence)
            .await?
        {
            if materialized == *reference {
                return Ok(());
            }
            return Err(pull::StorePullError::Database(format!(
                "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
            )));
        }
        let verified_commit = self.history_verifier.load_ref(reference).await?;
        let commit = verified_commit.value().clone();
        let author = verified_commit.author().clone();
        pull::verify_device_join_activation_commit(&commit, expected_outcome)?;
        if &commit.membership_state != membership_state {
            return Err(pull::StorePullError::Database(
                "device join activation differs from its expected Merge membership state"
                    .to_string(),
            ));
        }
        let predecessor_cut = commit
            .order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let frontier = predecessor_cut.0;
        let membership = self
            .history_verifier
            .verify_merge_history_authority(&frontier, &commit.membership_state)
            .await?
            .membership;
        let accepted_frontier = pull::commit_predecessor_references(&commit);
        let registrations = self
            .history_verifier
            .load_merge_commit_registrations(&commit, &author, &membership, &accepted_frontier)
            .await?;
        if !pull::membership_authorizes(Some(&membership), &commit, &author) {
            return Err(pull::StorePullError::Database(
                "device join activation author is not authorized by its exact predecessor membership"
                    .to_string(),
            ));
        }
        let head = self
            .history_verifier
            .load_activation_head(&verified_commit)
            .await?;
        let head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: head.value.head_hash(),
            object: head.object.clone(),
        };
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await?;
        let (authorized_predecessor, recovery_author) = predecessor_state
            .preactivate_recovery_author(&commit, &registrations)
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let device_operations =
            crate::protocol::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                &commit,
            )
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let state_after = device_operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .and_then(|state| {
                state.apply_verified_lifecycle(
                    &commit,
                    &registrations,
                    recovery_author.as_ref(),
                    None,
                )
            })
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let history = self
            .prepare_merge_history_successor(
                &verified_commit,
                &membership,
                recovery_author.as_ref(),
                state_after.clone(),
                MergeHistorySuccessorEvidence {
                    registrations: registrations
                        .iter()
                        .map(|registration| registration.registration().clone())
                        .collect(),
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await?;
        history
            .summary
            .open(&commit, reference, &head.value, &head_ref, &state_after)
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        self.database
            .materialize_device_join_activation(
                root,
                verified_commit,
                registrations,
                device_operations,
                head.value,
                head.object,
                history.summary,
            )
            .await?;
        Ok(())
    }
}
