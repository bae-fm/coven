use super::*;
use crate::sync::store::commit_verification::merge_history::validate_composed_snapshot_history_summary;
use crate::sync::store::merge_conflict;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn authorize_retained_conflict_resolution(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
        resolver_pubkey: &str,
    ) -> Result<merge_conflict::MergeConflictResolutionAuthorization, pull::StorePullError> {
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
        merge_conflict::validate_retained_membership_floors(&checkpoints, &membership)?;
        prefix.validate_complete_membership(&membership)?;
        let (device_state_ref, device_state) = self.retained_merge_device_state(&frontier).await?;
        if !crate::sync::store::commit_verification::merge_history::registration::device_state_has_active_registration(
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
        Ok(merge_conflict::MergeConflictResolutionAuthorization {
            membership,
            device_state_ref,
            device_state,
        })
    }

    pub(crate) async fn authorize_retained_outbound(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
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
        self.authorize_outbound_at_checkpoints(
            &frontier,
            &checkpoints,
            candidate_membership_heads,
            author_registration,
        )
        .await
    }

    pub(crate) async fn authorize_retained_preparation(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
        discovered_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
    ) -> Result<MergeOutboundAuthorization, pull::StorePullError> {
        let frontier = order.predecessor_cut()?.0;
        let checkpoints = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let mut heads = discovered_heads.to_vec();
        for checkpoint in &checkpoints {
            match checkpoint {
                coven_database::RetainedMergeHistoryCheckpoint::Snapshot(snapshot) => {
                    for proof in snapshot.summary.membership_proofs.values() {
                        heads.extend(proof.commit_value.membership_state.heads.iter().cloned());
                        heads.push(proof.head.clone());
                    }
                }
                coven_database::RetainedMergeHistoryCheckpoint::Commit(input) => {
                    heads.extend(input.commit().membership_state.heads.iter().cloned());
                    if let Some(proof) = &input.history_evidence().membership_proof {
                        heads.push(proof.head.clone());
                    }
                }
            }
        }
        let heads = coven_protocol::membership::MembershipFloor::from_heads(heads)
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        self.authorize_outbound_at_checkpoints(
            &frontier,
            &checkpoints,
            &heads.0,
            author_registration,
        )
        .await
    }

    async fn authorize_outbound_at_checkpoints(
        &self,
        frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
        checkpoints: &[coven_database::RetainedMergeHistoryCheckpoint],
        candidate_membership_heads: &[MembershipHeadRef],
        author_registration: &StoreDeviceRegistrationRef,
    ) -> Result<MergeOutboundAuthorization, pull::StorePullError> {
        let prefix = VerifiedMergeMembershipPrefix::from_retained(checkpoints)?;
        let membership = self
            .project_membership_to_verified_prefix(candidate_membership_heads, &prefix)
            .await
            .map_err(pull::StorePullError::MembershipChain)?;
        merge_conflict::validate_retained_membership_floors(checkpoints, &membership)?;
        prefix.validate_complete_membership(&membership)?;
        let (device_state_ref, device_state) = self.retained_merge_device_state(frontier).await?;
        if !crate::sync::store::commit_verification::merge_history::registration::device_state_has_active_registration(
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
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
        retained_merge_device_state(&self.database, frontier).await
    }

    pub(crate) async fn prepare_merge_history_successor(
        &self,
        verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &coven_protocol::membership::MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        predecessor_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        state_after: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
        crate::sync::store::pull::StorePullError,
    > {
        prepare_merge_history_successor(
            &self.history_verifier,
            verified_commit,
            membership,
            recovery_author,
            predecessor_state,
            state_after,
            evidence,
        )
        .await
    }

    pub(crate) async fn prepare_merge_snapshot_history_summary(
        &self,
        coverage: &coven_protocol::store_commit::CommitFrontier,
        membership: &coven_protocol::membership::MembershipChain,
        state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
        author_ref: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
        author: &coven_protocol::store_commit::StoreDeviceRegistration,
        publication: &coven_protocol::store_commit::StoreCurrentPublicationRecord,
    ) -> Result<
        coven_protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        crate::sync::store::pull::StorePullError,
    > {
        let frontier = &coverage.0;
        let root = self.history_verifier.verified_root().reference();
        let predecessors = self
            .retained_history_checkpoints(frontier.values().cloned().collect())
            .await?;
        let receipts = predecessors
            .iter()
            .filter_map(|checkpoint| match checkpoint {
                coven_database::RetainedMergeHistoryCheckpoint::Commit(input) => {
                    input.commit().reclaim_receipt().cloned()
                }
                coven_database::RetainedMergeHistoryCheckpoint::Snapshot(_) => None,
            })
            .collect::<Vec<_>>();
        let mut summary = compose_merge_snapshot_history_summary(
            root,
            coverage,
            membership,
            state,
            author_ref,
            author,
            &predecessors,
        )?;
        let mut requests = Vec::new();
        for checkpoint in &predecessors {
            let coven_database::RetainedMergeHistoryCheckpoint::Commit(input) = checkpoint else {
                continue;
            };
            if input.commit().owner_promotion_request().is_none() {
                continue;
            }
            let accepted = input.acceptance().exact_publication().ok_or_else(|| {
                pull::StorePullError::InvalidState(
                    "request retirement lacks its exact accepted publication".into(),
                )
            })?;
            let predecessor_cut = input.commit().order.predecessor_cut()?;
            let (_, predecessor_state) =
                self.retained_merge_device_state(&predecessor_cut.0).await?;
            requests.push((input.commit(), accepted, predecessor_state));
        }
        self.history_verifier
            .retain_pending_owner_promotions(&mut summary, requests, membership, state)
            .await?;
        let mut join_commits = BTreeMap::new();
        let mut join_publications = BTreeMap::new();
        for checkpoint in &predecessors {
            let coven_database::RetainedMergeHistoryCheckpoint::Commit(input) = checkpoint else {
                continue;
            };
            join_commits.insert(
                input.commit_ref().clone(),
                coven_database::DeviceJoinBootstrapCommit {
                    reference: input.commit_ref().clone(),
                    commit: input.verified_commit().clone(),
                    registrations: input.registrations().to_vec(),
                    device_operations: input.device_operations().clone(),
                    history_evidence: input.history_evidence().clone(),
                },
            );
            if let Some(exact) = input.acceptance().exact_publication() {
                join_publications.insert(input.commit_ref().clone(), exact.reference().clone());
            }
        }
        self.history_verifier
            .retain_pending_device_joins(
                &mut summary,
                state,
                join_commits,
                join_publications,
                publication,
            )
            .await?;
        if let Some(previous) = publication.latest_snapshot() {
            let metadata = self
                .history_verifier
                .load_snapshot_metadata(&previous.snapshot)
                .await?;
            summary
                .reclaim
                .include_previous_snapshot(previous, &metadata)
                .map_err(pull::StorePullError::Protocol)?;
            summary
                .reclaim
                .retire_receipts(&receipts)
                .map_err(pull::StorePullError::Protocol)?;
        }
        self.history_verifier
            .prune_absent_snapshot_artifacts(&mut summary.reclaim)
            .await?;
        // Newly covered entries become deletion obligations without probing
        // them. Only inherited obligations can be omitted after verified absence.
        self.history_verifier
            .retain_snapshot_publication_objects(&mut summary)?;
        // The fold above sees only the acknowledgements made inside this cut. A
        // summary has to state each device's chain from sequence one, because a
        // device restoring from it has no rows to walk — so walk each chain once,
        // here, where the verifier can.
        for chain in summary.acknowledgements.values_mut() {
            let (reference, value) = chain
                .latest()
                .ok_or_else(|| {
                    pull::StorePullError::InvalidState(
                        "composed acknowledgement chain is empty".to_string(),
                    )
                })?
                .clone();
            let registration = self
                .history_verifier
                .load_registration(&reference.registration)
                .await
                .map_err(pull::StorePullError::Object)?;
            chain.chain = self
                .history_verifier
                .load_acknowledgement_proof_chain(reference, value, &registration.value)
                .await
                .map_err(pull::StorePullError::from)?;
        }
        validate_composed_snapshot_history_summary(&summary, coverage)?;
        Ok(summary)
    }

    pub(crate) async fn retained_history_checkpoints(
        &self,
        references: Vec<StoreBatchCommitRef>,
    ) -> Result<Vec<coven_database::RetainedMergeHistoryCheckpoint>, pull::StorePullError> {
        retained_history_checkpoints(&self.database, &self.history_verifier, references).await
    }
}

/// Seed a history verifier from the history this device already holds.
///
/// The baseline is admitted before the retained rows because it is the floor
/// every later history walk stops at.
///
/// Getting the order wrong is not a slow path, it is a walk to genesis: a
/// verifier that does not know where its baseline is asks the provider for
/// every commit under it, once per commit standing above it, on every cycle.
pub(crate) async fn seed_verifier_from_retained_history(
    database: &StoreDatabase,
    history: &mut MergeHistoryVerifier<'_>,
) -> Result<Vec<coven_database::OwnedVerifiedMergeMaterialization>, pull::StorePullError> {
    let root = history.verified_root().reference().clone();
    let baseline = database
        .installed_replay_baseline()
        .await
        .map_err(|error| {
            pull::StorePullError::Database(coven_database::DbError::context(
                "load installed replay baseline",
                error,
            ))
        })?;
    history.admit_installed_baseline(baseline)?;
    let retained = database
        .retained_merge_replay_inputs(root)
        .await
        .map_err(|error| {
            pull::StorePullError::Database(coven_database::DbError::context(
                "load retained Merge replay inputs",
                error,
            ))
        })?;
    history.admit_retained_history(&retained)?;
    history
        .verify_refs(
            retained
                .iter()
                .map(|materialization| materialization.commit_ref().clone())
                .collect::<Vec<_>>(),
        )
        .await?;
    Ok(retained)
}

/// The device state a commit's predecessor cut resolves to, read from the
/// retained checkpoints its frontier names.
pub(crate) async fn retained_history_checkpoints(
    database: &StoreDatabase,
    history: &MergeHistoryVerifier<'_>,
    references: Vec<StoreBatchCommitRef>,
) -> Result<Vec<coven_database::RetainedMergeHistoryCheckpoint>, pull::StorePullError> {
    let root = history.verified_root().reference();
    let checkpoints = database
        .retained_merge_history_frontier(root.clone(), references)
        .await
        .map_err(pull::StorePullError::Database)?;
    if checkpoints.iter().any(|checkpoint| match checkpoint {
        coven_database::RetainedMergeHistoryCheckpoint::Snapshot(checkpoint) => {
            checkpoint.summary.store_root_hash != root.store_root_hash
        }
        coven_database::RetainedMergeHistoryCheckpoint::Commit(materialization) => {
            materialization.root() != root
        }
    }) {
        return Err(pull::StorePullError::InvalidState(
            "Merge operation is missing retained predecessor authority".to_string(),
        ));
    }
    Ok(checkpoints)
}

pub(crate) async fn retained_merge_device_state(
    database: &StoreDatabase,
    frontier: &BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), pull::StorePullError> {
    database
        .store_device_state_for_history_cut(&coven_protocol::store_commit::StoreHistoryCut(
            frontier.clone(),
        ))
        .await
        .map_err(pull::StorePullError::Database)
}

pub(crate) async fn prepare_merge_history_successor(
    history: &MergeHistoryVerifier<'_>,
    verified_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    membership: &coven_protocol::membership::MembershipChain,
    recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
    predecessor_state: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    state_after: &coven_protocol::store_commit::ResolvedStoreDeviceState,
    evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
) -> Result<
    crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
    crate::sync::store::pull::StorePullError,
> {
    let root = history.verified_root().reference();
    if verified_commit.store_root_hash() != root.store_root_hash {
        return Err(crate::sync::store::pull::StorePullError::InvalidState(
            "authenticated Merge successor belongs to another Store root".to_string(),
        ));
    }
    let commit = verified_commit.value();
    state_after.validate_canonical().map_err(|error| {
        crate::sync::store::pull::StorePullError::context(
            "validate Merge successor post-state",
            error,
        )
    })?;
    let predecessor_cut = commit
        .order
        .predecessor_cut()
        .map_err(crate::sync::store::pull::StorePullError::Protocol)?;
    let expected_predecessor_ref =
        coven_protocol::store_commit::StoreDeviceStateRef::from_resolved(
            coven_protocol::store_commit::CommitFrontier(predecessor_cut.0),
            predecessor_state,
        )
        .map_err(crate::sync::store::pull::StorePullError::Protocol)?;
    if commit.device_state != expected_predecessor_ref {
        return Err(crate::sync::store::pull::StorePullError::InvalidState(
            "Merge successor names another predecessor device state".to_string(),
        ));
    }
    if let Some(recovery_author) = recovery_author {
        let retained_recovery_registration = evidence.registrations.iter().any(|registration| {
            registration.reference() == recovery_author
                && matches!(
                    &registration.value().origin,
                    coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery { .. }
                )
        });
        let recovery_activation =
                commit.device_registrations().iter().any(|activation| {
                    activation.registration == *recovery_author
                        && matches!(
                        &activation.authority,
                        coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                            ..
                        }
                    )
                });
        if recovery_author != &commit.author_registration
            || !retained_recovery_registration
            || !recovery_activation
        {
            return Err(crate::sync::store::pull::StorePullError::InvalidState(
                "Merge successor recovery author lacks its exact retained activation".to_string(),
            ));
        }
    }
    if !crate::sync::store::commit_verification::merge_history::registration::device_state_has_active_registration(
            predecessor_state,
            &commit.author_registration,
        ) && recovery_author != Some(&commit.author_registration)
        {
            return Err(crate::sync::store::pull::StorePullError::InvalidState(
                "Merge successor author is inactive at its exact predecessor cut".to_string(),
            ));
        }
    crate::sync::store::commit_verification::merge_history::verify_merge_membership_state_ref(
        &commit.membership_state,
        membership,
        predecessor_state,
    )?;

    let retained_evidence = coven_protocol::store_commit::RetainedMergeCommitEvidence {
        acknowledgement: evidence.acknowledgement.map(Box::new),
        membership_proof: evidence.membership_proof.map(Box::new),
    };
    Ok(PreparedMergeHistorySuccessor {
        history_evidence: retained_evidence,
    })
}
