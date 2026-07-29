use super::*;
use std::collections::BTreeMap;

pub(super) mod abandonment;

use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::membership::{
    AuthorStreamId, MembershipChain, MembershipHeadRef, MembershipStatus,
};
use crate::sync::store_commit::{
    CommitFrontier, OpenedRetainedMergeHistorySummary, ResolvedStoreDeviceState,
    StoreBatchCommitRef, StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreRootRef,
};

fn invitation_history_error(
    error: pull::StorePullError,
) -> crate::sync::store::membership::AnchoredChainError {
    match error {
        pull::StorePullError::Object(error) => {
            crate::sync::store::membership::AnchoredChainError::from_store_object(error)
        }
        pull::StorePullError::Storage(source) if source.is_transport() => {
            crate::sync::store::membership::AnchoredChainError::StorageUnavailable {
                operation: "authenticating membership Store history".to_string(),
                source,
            }
        }
        pull::StorePullError::Storage(error) => {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
        }
        error => crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string()),
    }
}

pub(super) struct InvitationHistory<'storage> {
    verifier: pull::MergeHistoryVerifier<'storage>,
    identity: &'storage crate::keys::UserKeypair,
}

impl<'storage> InvitationHistory<'storage> {
    pub(super) async fn open(
        storage: &'storage dyn crate::sync::storage::SyncStorage,
        identity: &'storage crate::keys::UserKeypair,
        root: &StoreRootRef,
    ) -> Result<Self, crate::sync::store::membership::InviteError> {
        let verifier = pull::MergeHistoryVerifier::new(storage, root)
            .await
            .map_err(invitation_history_error)
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(format!(
                    "membership chain: {error}"
                ))
            })?;
        Ok(Self { verifier, identity })
    }

    pub(super) async fn load_membership(
        &mut self,
        floor: &[MembershipHeadRef],
        founder: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::InviteError> {
        self.verifier
            .load_exact_anchored_membership(floor, Some(founder))
            .await
            .map_err(|error| {
                crate::sync::store::membership::InviteError::Crypto(format!(
                    "membership chain: {error}"
                ))
            })
    }

    pub(super) fn keyring<'operation>(
        &'operation self,
        membership: &'operation MembershipChain,
    ) -> super::keyring::AuthorizedMembershipKeyring<'operation, 'storage> {
        super::keyring::AuthorizedMembershipKeyring::bind(&self.verifier, self.identity, membership)
    }
}

pub(super) struct MergeConflictResolutionAuthorization {
    pub(super) membership: MembershipChain,
    pub(super) device_state_ref: StoreDeviceStateRef,
    pub(super) device_state: ResolvedStoreDeviceState,
}

enum TerminalNonactivationCandidate {
    StoreWrite {
        write_id: crate::WriteId,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
    CircleOperation {
        operation_id: crate::sync::circle::CircleOperationId,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
    MergeRetraction {
        reference: crate::sync::store_commit::StoreBatchCommitRef,
        verification: crate::database::TerminalCandidateCleanupVerification,
    },
}

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(super) async fn load_current_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::MembershipOpsError> {
        let _membership_load = self.database.lock_membership_load().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        self.database
            .persist_membership_head_cursors(chain.head_refs().to_vec())
            .await
            .map_err(|error| {
                crate::sync::store::membership::MembershipOpsError::Database(error.to_string())
            })?;
        Ok(chain)
    }

    pub(super) async fn load_and_install_owner_membership(
        &mut self,
        owner_pubkey: &str,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        let _membership_load = self.database.lock_membership_load().await;
        let cursors = self
            .database
            .membership_head_cursors()
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        let chain = Box::pin(
            self.history_verifier
                .load_exact_anchored_membership(&cursors.head_refs, Some(owner_pubkey)),
        )
        .await?;
        let root = self.history_verifier.root().clone();
        let root_bytes = self.history_verifier.verified_root_object().bytes.clone();
        let protocol_root = self.history_verifier.verified_root().clone();
        let founder = chain.founder_coord().ok_or_else(|| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(
                "owner-anchored membership chain is empty".to_string(),
            )
        })?;
        let founder_head_ref = chain
            .head_ref_for_stream(
                &founder.author_pubkey,
                &founder.author_owner_grant,
                founder.stream_id,
            )
            .cloned()
            .ok_or_else(|| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner-anchored membership chain has no exact founder head".to_string(),
                )
            })?;
        let founder_head = self
            .history_verifier
            .load_exact_membership_head(&founder_head_ref)
            .await?;
        let founder_registration_ref = founder_head.body.author_registration.clone();
        let founder_registration = self
            .history_verifier
            .commit_verifier_ref()
            .load_registration(&founder_registration_ref)
            .await
            .map_err(crate::sync::store::membership::AnchoredChainError::from_store_object)?;
        let founder_registration_bytes = founder_registration.bytes;
        let founder_registration = founder_registration.value;
        if founder_registration.author_pubkey != owner_pubkey
            || !matches!(
                founder_registration.origin,
                crate::sync::store_commit::StoreDeviceRegistrationOrigin::Founder { .. }
            )
        {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "founder head registration is not activated by the Store root".to_string(),
                ),
            );
        }
        if protocol_root.descriptor.founder_pubkey != owner_pubkey {
            return Err(
                crate::sync::store::membership::AnchoredChainError::LoadFailed(
                    "owner anchor differs from the Store root founder".to_string(),
                ),
            );
        }
        let founder_genesis = crate::sync::store_commit::ResolvedStoreDeviceState::founder(
            &root,
            founder_registration_ref.clone(),
            &protocol_root.descriptor.founder_pubkey,
            protocol_root.descriptor.founder_grant.clone(),
            &protocol_root.descriptor.founder_recovery,
        )
        .map_err(|error| {
            crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
        })?;
        self.database
            .install_store_owner_anchor(
                root,
                root_bytes,
                founder_registration_ref,
                founder_registration,
                founder_registration_bytes,
                founder_genesis,
                owner_pubkey.to_string(),
                crate::database::InitialStoreMembershipAuthority {
                    head_refs: chain.head_refs().to_vec(),
                },
            )
            .await
            .map_err(|error| {
                crate::sync::store::membership::AnchoredChainError::LoadFailed(error.to_string())
            })?;
        Ok(chain)
    }

    pub(super) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[MembershipHeadRef],
        prefix: &pull::VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .project_membership_to_verified_prefix(candidate_heads, prefix)
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_exact_membership_head_for_test(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<
        crate::sync::membership::AuthorHead,
        crate::sync::store::membership::AnchoredChainError,
    > {
        self.history_verifier
            .load_exact_membership_head(reference)
            .await
    }

    #[cfg(test)]
    pub(super) async fn load_membership_at_exact_heads_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
        resolutions: &[crate::sync::membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.history_verifier
            .load_membership_at_exact_heads(heads, resolutions)
            .await
    }

    #[cfg(test)]
    pub(super) async fn assert_deep_membership_projection_for_test(
        &mut self,
        heads: &[MembershipHeadRef],
    ) {
        self.history_verifier
            .assert_deep_membership_projection(heads)
            .await;
    }

    pub(super) async fn prepare_pull_retained_history(
        &mut self,
    ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, pull::StorePullError> {
        let retained_refs = self.database.retained_merge_materialization_refs().await?;
        self.history_verifier.verify_refs(retained_refs).await?;
        let retained_commit_proofs = self
            .history_verifier
            .history()
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.verified.clone()))
            .collect();
        let retained = self
            .database
            .retained_merge_replay_inputs_with_verified_commits(
                self.history_verifier.root().clone(),
                retained_commit_proofs,
            )
            .await?;
        self.resume_merge_retraction_cleanups().await?;
        Ok(retained)
    }

    pub(super) async fn cleanup_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.root().clone();
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
        for target in targets {
            self.history_verifier
                .storage()
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::sync::store_objects::StoreObjectError::from)?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn cleanup_circle_operation_candidate(
        &mut self,
        operation_id: &crate::sync::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let root = self.history_verifier.root().clone();
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
        for target in targets {
            self.history_verifier
                .storage()
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::sync::store_objects::StoreObjectError::from)?;
            self.database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn resume_merge_retraction_cleanups(
        &mut self,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        for candidate in self.database.pending_merge_retraction_cleanups().await? {
            let root = self.history_verifier.root().clone();
            let verification = self
                .database
                .merge_retraction_cleanup_verification(root, candidate.clone())
                .await?;
            self.apply_terminal_nonactivation(TerminalNonactivationCandidate::MergeRetraction {
                reference: candidate.clone(),
                verification,
            })
            .await?;
            for target in self
                .database
                .merge_retraction_cleanup_targets(candidate.clone())
                .await?
            {
                self.history_verifier
                    .storage()
                    .delete_protocol_object(&target.object)
                    .await
                    .map_err(crate::sync::store_objects::StoreObjectError::from)?;
                self.database
                    .mark_candidate_cleanup_absent(target.object)
                    .await?;
            }
            self.database
                .finish_merge_retraction_cleanup(candidate)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn materialize_device_join_activation(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::sync::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.materialize_device_join_activation_inner(reference, expected_outcome, membership_state)
            .await
    }

    pub(super) async fn abandon_excluded_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<Option<abandonment::MergeCandidateAbandonment>, StoreError> {
        let root = self.history_verifier.root().clone();
        let database = self.database.clone();
        let db = database.sqlite();
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
}

impl AuthorizedWriterOperation<'_> {
    pub(super) async fn cleanup_merge_candidate(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.store.history.cleanup_merge_candidate(write_id).await
    }

    pub(super) async fn cleanup_circle_operation_candidate(
        &mut self,
        operation_id: &crate::sync::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.store
            .history
            .cleanup_circle_operation_candidate(operation_id)
            .await
    }
}

impl AuthorizedStore<'_> {
    pub(super) async fn cleanup_circle_operation_candidate(
        &mut self,
        operation_id: &crate::sync::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.history
            .cleanup_circle_operation_candidate(operation_id)
            .await
    }
}

impl AuthorizedStoreHistory<'_> {
    async fn materialize_device_join_activation_inner(
        &mut self,
        reference: &StoreBatchCommitRef,
        expected_outcome: &crate::sync::store_commit::DeviceJoinOutcomeRef,
        membership_state: &StoreMembershipStateRef,
    ) -> Result<(), pull::StorePullError> {
        let root = self.history_verifier.root().clone();
        let db = self.database.sqlite();
        let crate::sync::store_commit::StoreCommitCoord {
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
        let head_ref = crate::sync::store_commit::StoreDeviceHeadRef {
            head_hash: head.value.head_hash(),
            object: head.object.clone(),
        };
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await?;
        let (authorized_predecessor, recovery_author) =
            pull::predecessor_with_recovery_author(predecessor_state, &commit, &registrations)
                .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let device_operations =
            crate::sync::store_commit::VerifiedStoreDeviceOperations::without_exclusions(&commit)
                .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        let state_after = device_operations
            .apply_to(authorized_predecessor, &commit.device_state)
            .and_then(|state| {
                pull::apply_verified_device_lifecycle(
                    state,
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
                pull::MergeHistorySuccessorEvidence {
                    registrations: commit
                        .device_registrations()
                        .iter()
                        .zip(&registrations)
                        .map(|(activation, (value, _))| {
                            crate::sync::store_commit::RetainedVerifiedRegistration {
                                reference: activation.registration.clone(),
                                value: value.clone(),
                            }
                        })
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
        let commit_ref = reference.clone();
        let expected_ref = reference.clone();
        db.call(move |connection| {
            let tx = connection
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            if let Some(materialized) =
                crate::sync::store::database::StoreDatabase::materialized_commit_ref_on(
                    &tx,
                    &stream_id,
                    sequence,
                )?
            {
                if materialized != expected_ref {
                    return Err(crate::database::DbError::Message(format!(
                        "device join activation coordinate {stream_id}/{sequence} is already occupied by another commit"
                    )));
                }
                tx.commit().map_err(crate::database::DbError::from)?;
                return Ok(());
            }
            crate::sync::store::database::StoreDatabase::record_activated_store_device_registrations_on(
                &tx,
                &commit,
                &registrations,
            )?;
            let circle_activations =
                crate::sync::store::circle_controls::VerifiedCircleActivations::none(
                    &commit,
                    &commit_ref,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
            let materialization = crate::database::VerifiedMergeMaterialization::verify(
                &root,
                &verified_commit,
                &registrations,
                &device_operations,
                &circle_activations,
                &head.value,
                &head.object,
                &history.summary,
                None,
                &[],
                None,
            )?;
            crate::sync::store::database::StoreDatabaseTransaction::new(&tx)
                .record_verified_merge_materialization(materialization)?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await?;
        Ok(())
    }

    pub(super) async fn retained_device_state_for_order(
        &self,
        order: &crate::sync::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
        let verifier = self.history_verifier.commit_verifier_ref();
        let root = verifier.root();
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .database
            .retained_merge_history_frontier(root.clone(), frontier.values().cloned().collect())
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        require_complete_retained_frontier(root, &frontier, &checkpoints)?;
        retained_merge_device_state(verifier, &frontier, &checkpoints).await
    }

    pub(super) async fn authorize_retained_conflict_resolution(
        &self,
        order: &crate::sync::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
        resolver_pubkey: &str,
    ) -> Result<MergeConflictResolutionAuthorization, pull::StorePullError> {
        let verifier = self.history_verifier.commit_verifier_ref();
        let root = verifier.root();
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .database
            .retained_merge_history_frontier(root.clone(), frontier.values().cloned().collect())
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        require_complete_retained_frontier(root, &frontier, &checkpoints)?;
        let prefix = pull::VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::Database)?;
        let (device_state_ref, device_state) =
            retained_merge_device_state(verifier, &frontier, &checkpoints).await?;
        if !pull::device_state_has_active_registration(&device_state, author_registration) {
            return Err(pull::StorePullError::Database(
                "Merge conflict-resolution author is inactive at its predecessor cut".to_string(),
            ));
        }
        pull::verify_canonical_owner_registration(
            verifier,
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

    pub(super) async fn authorize_retained_outbound(
        &self,
        order: &crate::sync::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
    ) -> Result<pull::MergeOutboundAuthorization, pull::StorePullError> {
        let verifier = self.history_verifier.commit_verifier_ref();
        let root = verifier.root();
        let frontier = order
            .predecessor_cut()
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?
            .0;
        let checkpoints = self
            .database
            .retained_merge_history_frontier(root.clone(), frontier.values().cloned().collect())
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        require_complete_retained_frontier(root, &frontier, &checkpoints)?;
        let prefix = pull::VerifiedMergeMembershipPrefix::from_retained(&checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix
            .validate_complete_membership(&membership)
            .map_err(pull::StorePullError::Database)?;
        let (device_state_ref, device_state) =
            retained_merge_device_state(verifier, &frontier, &checkpoints).await?;
        if !pull::device_state_has_active_registration(&device_state, author_registration) {
            return Err(pull::StorePullError::Database(
                "Merge outbound author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        let MembershipStatus::Resolved(resolved) = membership.status() else {
            return Err(pull::StorePullError::Database(
                "Merge outbound predecessor membership is conflicted".to_string(),
            ));
        };
        let membership_state = StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            device_state.recovery.clone(),
            resolved.state_hash,
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
        Ok(pull::MergeOutboundAuthorization {
            membership,
            membership_state,
            device_state_ref,
            device_state,
        })
    }

    pub(super) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        membership: &crate::sync::membership::MembershipChain,
        recovery_author: Option<&crate::sync::store_commit::StoreDeviceRegistrationRef>,
        state_after: crate::sync::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::owner::pull::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::owner::pull::PreparedMergeHistorySuccessor,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let root = self.history_verifier.root();
        if verified_commit.store_root_hash() != root.store_root_hash {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "authenticated Merge successor belongs to another Store root".to_string(),
            ));
        }
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let author = verified_commit.author();
        state_after.validate_canonical().map_err(|error| {
            crate::sync::store::owner::pull::StorePullError::Database(format!(
                "validate Merge successor post-state: {error}"
            ))
        })?;
        let predecessor_refs =
            crate::sync::store::owner::pull::commit_predecessor_references(commit);
        let predecessors = self
            .database
            .retained_merge_history_frontier(root.clone(), predecessor_refs.clone())
            .await
            .map_err(|error| {
                crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
            })?;
        if predecessors.len() != predecessor_refs.len() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge successor is missing a retained direct predecessor".to_string(),
            ));
        }
        let (expected_predecessor_ref, predecessor_state) = self
            .database
            .store_device_state_for_order(&commit.order)
            .await
            .map_err(|error| {
                crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
            })?;
        if commit.device_state != expected_predecessor_ref {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge successor names another predecessor device state".to_string(),
            ));
        }
        if let Some(recovery_author) = recovery_author {
            let retained_recovery_registration =
                evidence.registrations.iter().any(|registration| {
                    registration.reference == *recovery_author
                        && matches!(
                            &registration.value.origin,
                            crate::sync::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                                ..
                            }
                        )
                });
            let recovery_activation =
                commit.device_registrations().iter().any(|activation| {
                    activation.registration == *recovery_author
                        && matches!(
                        &activation.authority,
                        crate::sync::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                            ..
                        }
                    )
                });
            if recovery_author != &commit.author_registration
                || !retained_recovery_registration
                || !recovery_activation
            {
                return Err(crate::sync::store::owner::pull::StorePullError::Database(
                    "Merge successor recovery author lacks its exact retained activation"
                        .to_string(),
                ));
            }
        }
        if !crate::sync::store::owner::pull::device_state_has_active_registration(
            &predecessor_state,
            &commit.author_registration,
        ) && recovery_author != Some(&commit.author_registration)
        {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge successor author is inactive at its exact predecessor cut".to_string(),
            ));
        }
        crate::sync::store::owner::pull::verify_merge_membership_state_ref(
            &commit.membership_state,
            membership,
            &predecessor_state,
        )?;

        crate::sync::store::owner::pull::compose_merge_history_successor(
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

    pub(super) async fn prepare_merge_snapshot_history_summary(
        &self,
        coverage: &crate::sync::store_commit::CommitFrontier,
        membership: &crate::sync::membership::MembershipChain,
        state: &crate::sync::store_commit::ResolvedStoreDeviceState,
        author_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
        author: &crate::sync::store_commit::StoreDeviceRegistration,
    ) -> Result<
        crate::sync::store_commit::RetainedVerifiedMergeHistorySummary,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let frontier = &coverage.0;
        let root = self.history_verifier.root();
        let predecessors = self
            .database
            .retained_merge_history_frontier(root.clone(), frontier.values().cloned().collect())
            .await
            .map_err(|error| {
                crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
            })?;
        if predecessors.len() != frontier.len() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "Merge snapshot is missing a retained checkpoint at its coverage frontier"
                    .to_string(),
            ));
        }
        crate::sync::store::owner::pull::compose_merge_snapshot_history_summary(
            root,
            coverage,
            membership,
            state,
            author_ref,
            author,
            predecessors,
        )
    }

    pub(super) async fn observe_occupied_merge_head(
        &mut self,
        expected: &crate::sync::store_commit::StoreDeviceHead,
        expected_commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        slot: &crate::storage::cloud::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<abandonment::VerifiedMergeWinner, StoreError> {
        let store_root_hash = self.history_verifier.root().store_root_hash;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::sync::storage::ProtocolObjectDomain::StoreHead,
        );
        let (winner_bytes, winner_prepared) = self
            .history_verifier
            .storage()
            .read_prepared_protocol_slot(&context, slot, semantic_prefix)
            .await
            .map_err(crate::sync::store_objects::StoreObjectError::from)?;
        let unverified: crate::sync::store_commit::StoreDeviceHead =
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
            || expected_commit.author() != &registration
        {
            return Err(StoreError::InvalidOutbound(
                "expected Merge head differs from its authenticated commit".to_string(),
            ));
        }
        crate::sync::store_commit::StoreDeviceHead::parse_at(
            &expected.to_bytes(),
            store_root_hash,
            &registration,
            &expected.commit,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let winner_commit = self
            .history_verifier
            .commit_verifier()
            .load_ref(&unverified.commit)
            .await?;
        if winner_commit.author() != &registration {
            return Err(StoreError::InvalidOutbound(
                "occupied Merge head commit has a different authenticated author".to_string(),
            ));
        }
        let winner = crate::sync::store_commit::StoreDeviceHead::parse_at(
            &winner_bytes,
            store_root_hash,
            &registration,
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

    pub(super) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &crate::sync::store_commit::StoreDeviceHead,
        candidate_commit: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<abandonment::ExcludedCandidateHeadObservation, StoreError> {
        let store_root_hash = self.history_verifier.root().store_root_hash;
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            crate::sync::storage::ProtocolObjectDomain::StoreHead,
        );
        let prefix = crate::sync::store_commit::head_slot_prefix(
            &candidate.author_registration.device_id.to_string(),
            candidate.commit.coord.sequence(),
        );
        match self
            .history_verifier
            .storage()
            .read_protocol_slot(&context, candidate_object.slot(), &prefix)
            .await
        {
            Err(crate::sync::storage::StorageError::NotFound(_)) => {
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
            Err(error) => Err(crate::sync::store_objects::StoreObjectError::Storage(error).into()),
        }
    }

    pub(super) async fn discard_candidate_nonactivation(
        &mut self,
        candidate: &crate::database::BlockedMergeCandidate,
        revoked_grant: Option<&crate::sync::membership::MembershipGrantId>,
    ) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError>
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
            let target = crate::sync::store_commit::StoreBatchCommitDeletionTarget {
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

    async fn membership_revocation_candidate_nonactivation(
        &mut self,
        revoked_grant: &crate::sync::membership::MembershipGrantId,
        candidate: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::sync::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let root = self.history_verifier.root().clone();
        let expected_stream =
            crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &candidate.value().author_registration,
                crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
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
                .map_err(|error| StoreError::Database(error.to_string()))?;
            if predecessor_cut
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| candidate_sequence <= covered.coord.sequence())
            {
                continue;
            }
            let membership =
                crate::sync::store::owner::pull::load_merge_predecessor_membership_with_history(
                    &mut self.history_verifier,
                    &witness.commit().membership_state,
                )
                .await
                .map_err(|error| match error {
                    crate::sync::store::owner::pull::RegistrationLoadError::Object(error) => {
                        StoreError::Object(error)
                    }
                    crate::sync::store::owner::pull::RegistrationLoadError::Invalid(error) => {
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

    pub(super) async fn excluded_candidate_nonactivation(
        &mut self,
        candidate: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::sync::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<Option<crate::sync::remote_object::VerifiedCandidateNonactivation>, StoreError>
    {
        let candidate_ref = candidate.reference().clone();
        let root = self.history_verifier.root().clone();
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
        let candidate_target = crate::sync::store_commit::StoreBatchCommitDeletionTarget {
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

    pub(super) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &crate::database::AuthorExclusionActivationLocator,
        candidate: &crate::sync::store_commit::VerifiedStoreBatchCommit,
        candidate_head: &crate::sync::store_commit::StoreDeviceHead,
        candidate_head_object: &crate::sync::storage::ExactObjectRef,
    ) -> Result<
        crate::sync::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let retained = self
            .database
            .retained_merge_materialization(
                self.history_verifier.root().clone(),
                locator.activation_commit().clone(),
            )
            .await?;
        let (_, predecessor_state) = self
            .database
            .store_device_state_for_order(&retained.commit().order)
            .await?;
        let activation_commit = self
            .history_verifier
            .commit_verifier()
            .load_ref(retained.commit_ref())
            .await?;
        if activation_commit.value() != retained.commit() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "retained exclusion activation differs from its authenticated commit".to_string(),
            ));
        }
        self.history_verifier
            .commit_verifier()
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
}

fn require_complete_retained_frontier(
    root: &StoreRootRef,
    frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<(), pull::StorePullError> {
    if checkpoints.len() != frontier.len()
        || checkpoints
            .iter()
            .any(|checkpoint| checkpoint.summary.store_root_hash != root.store_root_hash)
    {
        return Err(pull::StorePullError::Database(
            "Merge operation is missing retained predecessor authority".to_string(),
        ));
    }
    Ok(())
}

fn validate_retained_membership_floors(
    checkpoints: &[OpenedRetainedMergeHistorySummary],
    membership: &MembershipChain,
) -> Result<(), pull::StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        !retained_membership_floor_is_included(&checkpoint.summary.membership_floor, membership)
    }) {
        return Err(pull::StorePullError::Database(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn retained_membership_floor_is_included(
    floor: &crate::sync::store_commit::MembershipCausalFloor,
    membership: &MembershipChain,
) -> bool {
    floor
        .effective_coordinates
        .iter()
        .all(|coordinate| membership.effectively_contains_coord(coordinate))
        && floor.resolutions.iter().all(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_ok()
        })
}

async fn retained_merge_device_state(
    verifier: &pull::StoreCommitVerifier<'_>,
    frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
    checkpoints: &[OpenedRetainedMergeHistorySummary],
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
    let root = verifier.root();
    let root_value = verifier.verified_root();
    let state = if checkpoints.is_empty() {
        let founder = verifier.load_founder_registration().await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
        ResolvedStoreDeviceState::founder(
            root,
            founder_ref,
            &root_value.descriptor.founder_pubkey,
            root_value.descriptor.founder_grant.clone(),
            &root_value.descriptor.founder_recovery,
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?
    } else {
        ResolvedStoreDeviceState::merge(
            checkpoints
                .iter()
                .map(|checkpoint| checkpoint.post_state.clone()),
        )
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?
    };
    let reference = StoreDeviceStateRef::from_resolved(CommitFrontier(frontier.clone()), &state)
        .map_err(|error| pull::StorePullError::Database(error.to_string()))?;
    Ok((reference, state))
}

impl AuthorizedStoreHistory<'_> {
    async fn apply_terminal_nonactivation(
        &mut self,
        candidate: TerminalNonactivationCandidate,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        let verification = match &candidate {
            TerminalNonactivationCandidate::StoreWrite { verification, .. }
            | TerminalNonactivationCandidate::CircleOperation { verification, .. }
            | TerminalNonactivationCandidate::MergeRetraction { verification, .. } => verification,
        };
        let nonactivation = self.verify_terminal_nonactivation(verification).await?;
        let root = self.history_verifier.root().clone();
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

    async fn verify_terminal_nonactivation(
        &mut self,
        verification: &crate::database::TerminalCandidateCleanupVerification,
    ) -> Result<
        crate::sync::remote_object::VerifiedCandidateNonactivation,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let reference = &verification.candidate.head.value.commit;
        let candidate = self
            .history_verifier
            .commit_verifier()
            .authenticate_bytes(reference, &verification.candidate.commit.bytes)
            .await?;
        if candidate.value() != verification.candidate.commit.value.value() {
            return Err(crate::sync::store::owner::pull::StorePullError::Database(
                "terminal cleanup candidate differs from its authenticated commit".to_string(),
            ));
        }
        let target = crate::sync::store_commit::StoreBatchCommitDeletionTarget {
            coord: reference.coord.clone(),
            object: verification.candidate.commit.object.clone(),
            canonical_signed_bytes: verification.candidate.commit.bytes.clone(),
        };
        match &verification.authority {
            crate::database::TerminalCandidateAuthority::AuthorExclusion(locator) => {
                self.verify_author_exclusion_nonactivation(
                    locator,
                    &candidate,
                    &verification.candidate.head.value,
                    &verification.candidate.head.object,
                )
                .await
            }
            crate::database::TerminalCandidateAuthority::MembershipGrantRevocation {
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
            crate::database::TerminalCandidateAuthority::DependencyRetraction(authority) => {
                crate::sync::remote_object::VerifiedCandidateNonactivation::from_verified_dependency_retraction_authority(
                    authority.clone(),
                    target,
                    candidate.author(),
                    verification.candidate.head.object.clone(),
                )
                .map_err(|error| {
                    crate::sync::store::owner::pull::StorePullError::Database(error.to_string())
                })
            }
        }
    }
}
