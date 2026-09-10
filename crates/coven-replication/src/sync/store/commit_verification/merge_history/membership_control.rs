use super::membership;
use super::*;

pub(crate) fn verify_merge_membership_state_ref(
    state: &StoreMembershipStateRef,
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    let expected = merge_membership_state_ref(membership, device_state)?;
    if &expected != state {
        return Err(StorePullError::InvalidState(
            "Store history membership reference differs from its exact resolved state".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn merge_membership_state_ref(
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<StoreMembershipStateRef, StorePullError> {
    StoreMembershipStateRef::from_membership(membership, device_state.recovery.clone())
        .map_err(StorePullError::Protocol)
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeMembershipHeadActivation {
    pub(super) commit: StoreBatchCommitRef,
    pub(super) transition: protocol_membership::MergeMembershipHeadTransition,
}

impl VerifiedMergeMembershipHeadActivation {
    pub(crate) fn verifies(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        &self.commit == commit && self.transition.matches_head(head, reference)
    }
}

#[derive(Clone)]
pub(crate) struct VerifiedMergeMembershipControl {
    pub(crate) activations: VerifiedCircleActivations,
    pub(crate) head_activation: VerifiedMergeMembershipHeadActivation,
}

#[derive(Clone, Default)]
pub struct VerifiedMergeMembershipPrefix {
    commits: BTreeSet<StoreBatchCommitRef>,
    predecessor_memberships: Vec<MembershipChain>,
    head_activations: BTreeMap<StoreBatchCommitRef, VerifiedMergeMembershipHeadActivation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VerifiedMergePrefixHeadStatus {
    Included,
    OutsidePrefix,
}

impl VerifiedMergeMembershipPrefix {
    pub(super) fn extends(&self, verified: &Self) -> bool {
        verified.commits.is_subset(&self.commits)
    }

    pub(crate) fn from_retained(
        checkpoints: &[coven_database::RetainedMergeHistoryCheckpoint],
    ) -> Result<Self, StorePullError> {
        let mut prefix = Self::default();
        for checkpoint in checkpoints {
            match checkpoint {
                coven_database::RetainedMergeHistoryCheckpoint::Snapshot(checkpoint) => {
                    prefix.insert_snapshot_summary(
                        &checkpoint.summary,
                        checkpoint.summary.post_state.frontier(),
                    )?;
                }
                coven_database::RetainedMergeHistoryCheckpoint::Commit(materialization) => {
                    prefix.commits.insert(materialization.commit_ref().clone());
                    if let Some(proof) = &materialization.history_evidence().membership_proof {
                        prefix.insert_retained_proof(proof)?;
                    }
                }
            }
        }
        Ok(prefix)
    }

    pub(super) fn insert_snapshot_summary(
        &mut self,
        checkpoint: &RetainedVerifiedMergeHistorySummary,
        frontier: &CommitFrontier,
    ) -> Result<(), StorePullError> {
        self.commits.extend(
            checkpoint
                .causal_cut
                .values()
                .filter(|reference| frontier.covers_commit(reference))
                .cloned(),
        );
        for proof in checkpoint.membership_proofs.values() {
            if frontier.covers_commit(&proof.commit) {
                self.insert_retained_proof(proof)?;
            }
        }
        Ok(())
    }

    fn insert_retained_proof(
        &mut self,
        proof: &store_commit::RetainedMergeMembershipProof,
    ) -> Result<(), StorePullError> {
        let Some(store_commit::StoreControl { transition }) = proof.commit_value.control() else {
            return Err(StorePullError::InvalidState(
                "retained Merge membership proof has no membership control".to_string(),
            ));
        };
        if transition.body.author_registration != proof.commit_value.author_registration {
            return Err(StorePullError::InvalidState(
                "retained membership transition has another Store commit author".into(),
            ));
        }
        let activation = VerifiedMergeMembershipHeadActivation {
            commit: proof.commit.clone(),
            transition: transition.clone(),
        };
        match self.head_activations.entry(proof.commit.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(activation);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &activation => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                return Err(StorePullError::InvalidState(
                    "retained checkpoints disagree on a membership activation".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn head_activation(
        &self,
        commit: &StoreBatchCommitRef,
    ) -> Option<&VerifiedMergeMembershipHeadActivation> {
        self.head_activations.get(commit)
    }

    pub(crate) fn classify_head(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedMergePrefixHeadStatus, StorePullError> {
        if !self.commits.contains(commit) {
            return Ok(VerifiedMergePrefixHeadStatus::OutsidePrefix);
        }
        let proof = self.head_activations.get(commit).ok_or_else(|| {
            StorePullError::InvalidState(
                "in-prefix membership activation is absent from its verified Store control"
                    .to_string(),
            )
        })?;
        if !proof.verifies(reference, head, commit) {
            return Err(StorePullError::InvalidState(
                "membership head differs from its in-prefix verified Store control".to_string(),
            ));
        }
        Ok(VerifiedMergePrefixHeadStatus::Included)
    }

    pub(crate) fn validate_complete_membership(
        &self,
        membership: &MembershipChain,
    ) -> Result<(), StorePullError> {
        if self
            .predecessor_memberships
            .iter()
            .any(|predecessor| !membership.causally_includes(predecessor))
        {
            return Err(StorePullError::InvalidState(
                "membership state regresses below an exact Store predecessor membership"
                    .to_string(),
            ));
        }
        if self
            .head_activations
            .values()
            .any(|proof| !membership.contains_coord(&proof.transition.body.entry.coord))
        {
            return Err(StorePullError::InvalidState(
                "membership state omits an accepted Store membership control".to_string(),
            ));
        }
        Ok(())
    }
}

/// The membership authority a commit's predecessors establish, down to the
/// installed baseline.
///
/// The snapshot's retained controls contribute only within the requested
/// predecessor cut. An earlier retained commit must not inherit controls
/// accepted between its predecessors and the snapshot.
pub(crate) fn verified_merge_membership_prefix(
    history: &VerifiedMergeHistory,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
    let closure = verified_merge_commit_closure(history, tips)?;
    let mut prefix = VerifiedMergeMembershipPrefix {
        commits: closure.clone(),
        ..VerifiedMergeMembershipPrefix::default()
    };
    if closure
        .iter()
        .any(|reference| history.superseded(reference))
    {
        let snapshot = history.baseline.snapshot().ok_or_else(|| {
            StorePullError::InvalidState(
                "snapshot-covered membership prefix has no retained history summary".to_string(),
            )
        })?;
        let summary = &snapshot.meta.history_summary;
        let mut frontier = CommitFrontier(BTreeMap::new());
        for reference in &closure {
            if history.superseded(reference)
                && summary.causal_cut.get(&reference.coord) != Some(reference)
            {
                return Err(StorePullError::InvalidState(
                    "snapshot-covered membership predecessor has no exact accepted reference"
                        .into(),
                ));
            }
            frontier = frontier.join(CommitFrontier(BTreeMap::from([(
                reference.coord.stream_id,
                reference.clone(),
            )])))?;
        }
        prefix.insert_snapshot_summary(summary, &frontier)?;
    }
    for reference in closure {
        let Some(verified) = history.commits.get(&reference) else {
            continue;
        };
        prefix
            .predecessor_memberships
            .push(verified.predecessor_membership.clone());
        if let Some(control) = &verified.membership_control {
            prefix
                .head_activations
                .insert(reference, control.head_activation.clone());
        }
    }
    Ok(prefix)
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn verify_membership_control_with_retained_history(
        &mut self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
        predecessor_membership: &MembershipChain,
        predecessor_state: &ResolvedStoreDeviceState,
    ) -> Result<VerifiedCircleActivations, StorePullError> {
        let Some(store_commit::StoreControl { transition }) = commit.control() else {
            return Err(StorePullError::InvalidState(
                "Merge membership verifier received another Store control".to_string(),
            ));
        };
        let root = self.root.reference().clone();
        let state = &commit.membership_state;
        let commit_author = self
            .commit_verifier
            .load_registration(&commit.author_registration)
            .await?;
        if transition.body.author_registration != commit.author_registration
            || transition.body.entry.coord.author_pubkey != commit_author.value.author_pubkey
            || transition.body.successor.predecessor
                != transition
                    .body
                    .predecessor_head()
                    .map(|reference| reference.object.clone())
        {
            return Err(StorePullError::InvalidState(
                "Merge membership transition differs from its Store authority".to_string(),
            ));
        }
        match transition.body.predecessor_head() {
            Some(predecessor) if state.heads.binary_search(predecessor).is_err() => {
                return Err(StorePullError::InvalidState(
                    "Merge membership transition predecessor is absent from its signed state"
                        .to_string(),
                ));
            }
            None if state.heads.iter().any(|head| {
                head.coord.stream_key() == transition.body.entry.coord.stream_key()
            }) =>
            {
                return Err(StorePullError::InvalidState(
                    "first Merge membership transition has an existing signed predecessor"
                        .to_string(),
                ));
            }
            _ => {}
        }
        let opened_entry = self
            .commit_verifier
            .membership_objects()
            .load_entry(&transition.body.entry)
            .await?;
        if opened_entry.value.coord() != transition.body.entry.coord
            || opened_entry.value.dependencies != predecessor_membership.effective_frontier()
        {
            return Err(StorePullError::InvalidState(
                "Merge membership transition differs from its exact entry".to_string(),
            ));
        }
        let device_control_matches = match &opened_entry.value.change {
            protocol_membership::StoreAuthorityChange::DeviceRegistrationActivation {
                registration,
            } => Some(
                commit.device_registrations() == std::slice::from_ref(registration)
                    && commit.device_exclusion_proposals().is_empty()
                    && commit.device_exclusion_outcomes().is_empty(),
            ),
            protocol_membership::StoreAuthorityChange::DeviceExclusionProposal { proposal } => {
                Some(
                    commit.device_exclusion_proposals() == std::slice::from_ref(proposal)
                        && commit.device_exclusion_outcomes().is_empty()
                        && commit.device_registrations().is_empty(),
                )
            }
            protocol_membership::StoreAuthorityChange::DeviceExclusionOutcome { outcome } => Some(
                commit.device_exclusion_outcomes() == std::slice::from_ref(outcome)
                    && commit.device_exclusion_proposals().is_empty()
                    && commit.device_registrations().is_empty(),
            ),
            _ => None,
        };
        if let Some(matches) = device_control_matches {
            if !matches || !commit.stream_activations().is_empty() {
                return Err(StorePullError::InvalidState(
                    "Store device control differs from its exact authority entry".into(),
                ));
            }
            let mut successor_membership = predecessor_membership.clone();
            successor_membership.add_entry(opened_entry.value)?;
            return VerifiedCircleActivations::membership_control(commit, commit_ref)
                .map_err(StorePullError::from);
        }
        if let protocol_membership::StoreAuthorityChange::RemoveMember {
            user_pubkey,
            removes,
            retirement_device_state,
            ..
        } = &opened_entry.value.change
        {
            let removes_exact_member =
                removes == &predecessor_membership.active_grant_ids(user_pubkey);
            let retires_owner = removes.iter().any(|grant| {
                predecessor_membership
                    .active_grant(grant)
                    .is_some_and(|record| {
                        matches!(
                            record.role,
                            protocol_membership::StoreMembershipRoleGrant::Owner { .. }
                        )
                    })
            });
            if !removes_exact_member
                || retires_owner != retirement_device_state.is_some()
                || retirement_device_state
                    .as_ref()
                    .is_some_and(|state| state != &commit.device_state)
                || !commit.stream_activations().is_empty()
            {
                return Err(StorePullError::InvalidState(
                    "Merge removal control differs from its exact membership entry".to_string(),
                ));
            }
            let mut successor_membership = predecessor_membership.clone();
            successor_membership.add_entry(opened_entry.value)?;
            return VerifiedCircleActivations::membership_control(commit, commit_ref)
                .map_err(StorePullError::from);
        }
        if let protocol_membership::StoreAuthorityChange::SetMember {
            user_pubkey,
            role:
                protocol_membership::StoreMembershipRoleGrant::Member
                | protocol_membership::StoreMembershipRoleGrant::Follower,
            replaces,
            retirement_device_state,
            ..
        } = &opened_entry.value.change
        {
            if replaces != &predecessor_membership.active_grant_ids(user_pubkey)
                || retirement_device_state
                    .as_ref()
                    .is_some_and(|state| state != &commit.device_state)
                || !commit.stream_activations().is_empty()
            {
                return Err(StorePullError::InvalidState(
                    "Merge member assignment differs from its exact membership entry".into(),
                ));
            }
            let mut successor_membership = predecessor_membership.clone();
            successor_membership.add_entry(opened_entry.value)?;
            return VerifiedCircleActivations::membership_control(commit, commit_ref)
                .map_err(StorePullError::from);
        }
        let protocol_membership::StoreAuthorityChange::SetMember {
            user_pubkey,
            role:
                protocol_membership::StoreMembershipRoleGrant::Owner {
                    recovery: protocol_membership::OwnerRecoveryAnchorRef::Promotion { acceptance },
                },
            grant_id,
            membership: Some(membership_anchor),
            replaces,
            retirement_device_state,
            ..
        } = &opened_entry.value.change
        else {
            return Err(StorePullError::InvalidState(
                "Merge membership control does not activate one Owner promotion".to_string(),
            ));
        };
        if retirement_device_state.is_some()
            || user_pubkey != &acceptance.request.member_pubkey
            || grant_id != &acceptance.request.intended_owner_grant
            || replaces != &BTreeSet::from([acceptance.request.member_grant.clone()])
            || acceptance.request.promoter_registration != commit.author_registration
        {
            return Err(StorePullError::InvalidState(
                "Merge Owner-promotion control differs from its exact membership entry".to_string(),
            ));
        }
        let request_membership = self
            .verify_owner_promotion_acceptance_in_loaded_history(acceptance)
            .await?;
        let request_activation = acceptance.activation.commit();
        let predecessor_cut = commit.order.predecessor_cut()?;
        let predecessor_frontier = predecessor_cut.commits();
        let request_stream = request_activation.coord.stream_id;
        let activation_is_covered = predecessor_frontier
            .get(&request_stream)
            .is_some_and(|head| head.coord.sequence() >= request_activation.coord.sequence());
        let promoter_is_active = device_state_has_active_registration(
            predecessor_state,
            &acceptance.request.promoter_registration,
        );
        let candidate_is_active = device_state_has_active_registration(
            predecessor_state,
            &acceptance.request.member_registration,
        );
        let promoter_grant_is_active = predecessor_membership
            .active_owner_grant(&commit_author.value.author_pubkey)
            .as_ref()
            == Some(&acceptance.request.promoter_owner_grant);
        let candidate_grant_is_active = predecessor_membership
            .active_grant(&acceptance.request.member_grant)
            .is_some_and(|record| {
                record.member_pubkey == acceptance.request.member_pubkey
                    && record.role == protocol_membership::StoreMembershipRoleGrant::Member
            });
        if !predecessor_membership.causally_includes(&request_membership)
            || !activation_is_covered
            || !promoter_is_active
            || !candidate_is_active
            || !promoter_grant_is_active
            || !candidate_grant_is_active
        {
            return Err(StorePullError::InvalidState(
                "Merge Owner-promotion transition does not include its accepted authority"
                    .to_string(),
            ));
        }
        let store_commit::OwnerPromotionAnchors {
            membership,
            recovery,
        } = &acceptance.anchors;
        if membership != membership_anchor {
            return Err(StorePullError::InvalidState(
                "Merge Owner-promotion entry carries another membership anchor".to_string(),
            ));
        }
        let mut expected = vec![
            store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                membership.clone(),
            ),
            store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.request.member_registration.clone(),
                acceptance.request.intended_owner_grant.clone(),
                recovery.clone(),
            ),
        ];
        expected.sort();
        if commit.stream_activations() != expected {
            return Err(StorePullError::InvalidState(
                "Merge Owner-promotion control carries different stream activations".to_string(),
            ));
        }
        VerifiedCircleActivations::membership_control(commit, commit_ref)
            .map_err(StorePullError::from)
    }

    pub(crate) async fn verified_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        if commit.control().is_none() {
            return Ok(None);
        }
        let verified = self.history.commits.get(commit_ref).ok_or_else(|| {
            StorePullError::InvalidState(
                "membership objects require an operation-verified Store commit".into(),
            )
        })?;
        if verified.verified.value() != commit {
            return Err(StorePullError::InvalidState(
                "membership objects differ from their operation-verified Store commit".into(),
            ));
        }
        let proof = verified
            .history_evidence
            .membership_proof
            .as_deref()
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "operation-verified Store control has no retained membership proof".into(),
                )
            })?;
        VerifiedMergeMembershipClosure::from_verified_proof(proof.clone()).map(Some)
    }

    pub(crate) async fn verify_accepted_provider_access_activation(
        &mut self,
        access: &coven_protocol::provider::ActivatedStoreMemberProviderAccessGrant,
        provider_admin: &coven_protocol::provider::ProviderAdminGrantRecord,
        administrator: &StoreDeviceRegistration,
    ) -> Result<(), StorePullError> {
        let grant = self
            .load_provider_access_grant(&access.grant_ref, administrator)
            .await?;
        if grant.value != access.grant {
            return Err(StorePullError::InvalidState(
                "device provider approval embeds a different access grant than its exact reference"
                    .to_string(),
            ));
        }
        let activation = self.load_ref(&access.activation).await?;
        if activation.value().provider_access_grants() != std::slice::from_ref(&access.grant_ref)
            || activation.value().author_registration != access.grant.administrator
            || activation.author() != administrator
        {
            return Err(StorePullError::InvalidState(
                "device provider approval activation is not the administrator's exact sole access grant"
                    .to_string(),
            ));
        }
        if !self.current_history_contains(&access.activation).await? {
            return Err(StorePullError::InvalidState(
                "device provider approval activation is absent from current accepted Store history"
                    .to_string(),
            ));
        }
        let membership = self
            .load_predecessor_membership(&activation.value().membership_state)
            .await
            .map_err(StorePullError::from)?;
        if !predecessor_verifies_provider_administrator(
            &membership,
            &access.grant.administrator_grant,
            &activation.value().author_registration,
            provider_admin,
        ) {
            return Err(StorePullError::InvalidState(
                "device provider approval activation lacks exact predecessor provider-administrator authority"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn current_history_contains(
        &mut self,
        expected: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        let publication = self.load_current_accepted_publication().await?;
        Ok(publication.commits.contains_key(expected)
            || publication
                .accepted_snapshots
                .last()
                .is_some_and(|selected| {
                    selected
                        .snapshot
                        .meta
                        .history_summary
                        .causal_cut
                        .values()
                        .any(|accepted| accepted == expected)
                }))
    }

    /// Verify admission before opening the Store keyring. Portable membership
    /// acceptance results establish authority without reading encrypted history.
    pub async fn load_accepted_anchored_membership(
        &self,
        heads: &[protocol_membership::MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::AcceptedMembershipActivation::new(&self.root, &self.commit_verifier)
            .load_exact_anchored_chain(heads, owner)
            .await
    }

    pub async fn load_exact_anchored_membership(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        self.load_exact_anchored_membership_traversal(heads, owner)
            .await
            .map(|(membership, _)| membership)
    }

    /// The anchored walk, and every membership object it read on the way.
    ///
    /// A snapshot publisher needs the second half: what it publishes as the
    /// membership rollup is exactly the set of objects a reader of this same
    /// frontier would otherwise fetch one at a time.
    pub(crate) async fn load_exact_anchored_membership_traversal(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<
        (MembershipChain, Vec<membership::TraversedMembershipStream>),
        crate::sync::store::membership::AnchoredChainError,
    > {
        let (membership, traversed) = membership::HistoryMembershipActivation::new(self)
            .load_exact_anchored_chain(heads, owner)
            .await?;
        if self.history.commits.is_empty() {
            let authority = VerifiedMergeMembershipPrefix::default();
            authority
                .validate_complete_membership(&membership)
                .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
            self.remember_verified_membership(authority, membership.clone());
        }
        Ok((membership, traversed))
    }

    pub(crate) async fn load_membership_at_exact_heads(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::HistoryMembershipActivation::new(self)
            .load_at_exact_heads(heads)
            .await
    }

    pub(crate) async fn load_membership_at_verified_prefix(
        &self,
        heads: &[protocol_membership::MembershipHeadRef],
        verified_activations: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        VerifiedPrefixMembershipActivation::new(
            &self.root,
            &self.commit_verifier,
            verified_activations,
        )
        .load_at_exact_heads(heads)
        .await
    }

    pub(crate) async fn load_predecessor_membership(
        &mut self,
        state: &StoreMembershipStateRef,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_exact_heads(&state.heads)
            .await
            .map_err(RegistrationLoadError::from)
    }

    pub(crate) async fn load_predecessor_membership_at_verified_prefix(
        &self,
        state: &StoreMembershipStateRef,
        verified_activations: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_verified_prefix(&state.heads, verified_activations)
            .await
            .map_err(RegistrationLoadError::from)
    }

    pub(crate) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[protocol_membership::MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        VerifiedPrefixMembershipActivation::new(&self.root, &self.commit_verifier, prefix)
            .project(candidate_heads)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn load_exact_membership_head(
        &mut self,
        reference: &protocol_membership::MembershipHeadRef,
    ) -> Result<protocol_membership::AuthorHead, crate::sync::store::membership::AnchoredChainError>
    {
        self.commit_verifier
            .membership_objects()
            .load_head(reference)
            .await
            .map(|loaded| loaded.value)
            .map_err(membership::map_membership_object_error)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn assert_deep_membership_projection(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
    ) {
        membership::HistoryMembershipActivation::new(self)
            .assert_deep_valid_predecessor_path_is_iterative(heads)
            .await;
    }
}
