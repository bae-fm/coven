use super::membership;
use super::*;

pub(crate) fn verify_merge_membership_state_ref(
    state: &StoreMembershipStateRef,
    membership: &MembershipChain,
    device_state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    let MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StorePullError::InvalidState(
            "Store history membership state is conflicted".to_string(),
        ));
    };
    let expected = StoreMembershipStateRef::from_parts(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        device_state.recovery.clone(),
        resolved.state_hash,
    )
    .map_err(StorePullError::Protocol)?;
    if &expected != state {
        return Err(StorePullError::InvalidState(
            "Store history membership reference differs from its exact resolved state".to_string(),
        ));
    }
    Ok(())
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

pub(crate) struct VerifiedMergeMembershipControl {
    pub(crate) activations: VerifiedCircleActivations,
    pub(crate) head_activation: VerifiedMergeMembershipHeadActivation,
    pub(crate) conflict_resolution: Option<VerifiedMergeConflictResolutionActivation>,
}

impl VerifiedMergeMembershipControl {
    pub(crate) fn verifies_head_activation(
        &self,
        reference: &protocol_membership::MembershipHeadRef,
        head: &protocol_membership::AuthorHead,
        commit: &StoreBatchCommitRef,
    ) -> bool {
        self.head_activation.verifies(reference, head, commit)
    }
}

#[derive(Clone, Default)]
pub struct VerifiedMergeMembershipPrefix {
    commits: BTreeSet<StoreBatchCommitRef>,
    predecessor_memberships: Vec<MembershipChain>,
    head_activations: BTreeMap<StoreBatchCommitRef, VerifiedMergeMembershipHeadActivation>,
    conflict_resolutions: BTreeMap<
        protocol_membership::StoreMembershipConflictResolutionRef,
        VerifiedMergeConflictResolutionActivation,
    >,
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
                    prefix
                        .commits
                        .extend(checkpoint.summary.causal_cut.values().cloned());
                    for proof in checkpoint.summary.membership_proofs.values() {
                        prefix.insert_retained_proof(proof)?;
                    }
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

    fn insert_retained_proof(
        &mut self,
        proof: &store_commit::RetainedMergeMembershipProof,
    ) -> Result<(), StorePullError> {
        let Some(store_commit::StoreControl { transition }) = proof.commit_value.control() else {
            return Err(StorePullError::InvalidState(
                "retained Merge membership proof has no membership control".to_string(),
            ));
        };
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
        if let Some(reference) = &proof.resolution {
            let activation = VerifiedMergeConflictResolutionActivation {
                reference: reference.clone(),
            };
            match self.conflict_resolutions.entry(reference.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(activation);
                }
                std::collections::btree_map::Entry::Occupied(entry)
                    if entry.get() == &activation => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(StorePullError::InvalidState(
                        "retained checkpoints disagree on a conflict resolution".to_string(),
                    ));
                }
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

    pub(crate) fn verifies_conflict_resolution(
        &self,
        reference: &protocol_membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        self.conflict_resolutions
            .get(reference)
            .is_some_and(|proof| proof.verifies(reference))
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
        if self.conflict_resolutions.keys().any(|reference| {
            membership
                .resolution_refs()
                .binary_search(reference)
                .is_err()
        }) {
            return Err(StorePullError::InvalidState(
                "membership state omits an accepted Store conflict resolution".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn verified_merge_membership_prefix(
    commits: &BTreeMap<StoreBatchCommitRef, VerifiedMergeHistoryCommit>,
    tips: impl IntoIterator<Item = StoreBatchCommitRef>,
) -> Result<VerifiedMergeMembershipPrefix, StorePullError> {
    let closure = verified_merge_commit_closure(commits, tips)?;
    let mut prefix = VerifiedMergeMembershipPrefix {
        commits: closure.clone(),
        ..VerifiedMergeMembershipPrefix::default()
    };
    for reference in closure {
        let verified = &commits[&reference];
        prefix
            .predecessor_memberships
            .push(verified.predecessor_membership.clone());
        if let Some(control) = &verified.membership_control {
            prefix
                .head_activations
                .insert(reference, control.head_activation.clone());
            if let Some(resolution) = &control.conflict_resolution {
                prefix
                    .conflict_resolutions
                    .insert(resolution.reference.clone(), resolution.clone());
            }
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
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<
        (
            VerifiedCircleActivations,
            Option<VerifiedMergeConflictResolutionActivation>,
        ),
        StorePullError,
    > {
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
            || transition.body.resolutions != state.resolutions
            || transition.body.successor.predecessor
                != transition
                    .body
                    .predecessor
                    .as_ref()
                    .map(|reference| reference.object.clone())
        {
            return Err(StorePullError::InvalidState(
                "Merge membership transition differs from its Store authority".to_string(),
            ));
        }
        match &transition.body.predecessor {
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
            || opened_entry.value.resolution_dependencies != transition.body.resolutions
        {
            return Err(StorePullError::InvalidState(
                "Merge membership transition differs from its exact entry".to_string(),
            ));
        }
        if let protocol_membership::MembershipChange::RemoveMember {
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
                || !retires_owner
                || retirement_device_state.as_ref() != Some(&commit.device_state)
                || !commit.stream_activations().is_empty()
            {
                return Err(StorePullError::InvalidState(
                    "Merge Owner-removal control differs from its exact membership entry"
                        .to_string(),
                ));
            }
            let mut successor_membership = predecessor_membership.clone();
            successor_membership.add_entry(opened_entry.value)?;
            return VerifiedCircleActivations::membership_control(commit, commit_ref)
                .map(|activations| (activations, None))
                .map_err(StorePullError::from);
        }
        if let protocol_membership::MembershipChange::ResolutionActivation { resolution } =
            &opened_entry.value.change
        {
            let resolution = resolution.clone();
            let resolution_proof = pending_resolution
                .filter(|proof| proof.verifies(&resolution))
                .ok_or_else(|| {
                    StorePullError::InvalidState(
                        "Merge conflict resolution lacks its verified Store activation".to_string(),
                    )
                })?
                .clone();
            let opened_resolution = self
                .commit_verifier
                .membership_objects()
                .load_resolution(&resolution)
                .await?;
            let acceptance = &opened_resolution.value.replacement_acceptance;
            let mut expected = vec![
                store_commit::StreamActivation::grant_authorized(
                    root.store_root_hash,
                    acceptance.owner_registration.clone(),
                    opened_resolution.value.replacement_grant.clone(),
                    acceptance.membership.clone(),
                ),
                store_commit::StreamActivation::grant_authorized(
                    root.store_root_hash,
                    acceptance.owner_registration.clone(),
                    opened_resolution.value.replacement_grant.clone(),
                    acceptance.recovery.clone(),
                ),
            ];
            expected.sort();
            if transition.body.predecessor.is_some()
                || transition
                    .body
                    .resolutions
                    .binary_search(&resolution)
                    .is_err()
                || commit.stream_activations() != expected
            {
                return Err(StorePullError::InvalidState(
                    "Merge conflict-resolution control differs from its exact membership entry"
                        .to_string(),
                ));
            }
            let mut successor_membership = predecessor_membership.clone();
            successor_membership.add_entry(opened_entry.value)?;
            return VerifiedCircleActivations::membership_control(commit, commit_ref)
                .map(|activations| (activations, Some(resolution_proof)))
                .map_err(StorePullError::from);
        }
        let protocol_membership::MembershipChange::SetMember {
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
        self.verify_owner_promotion_acceptance_in_loaded_history(acceptance)
            .await?;
        let request_activation = acceptance.activation.commit();
        let request_commit = self
            .history
            .commits
            .get(request_activation)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "Merge Owner-promotion request activation is absent from its verified history"
                        .to_string(),
                )
            })?;
        let verified_membership_activations = verified_merge_membership_prefix(
            &self.history.commits,
            commit_predecessor_references(request_commit.verified.value()),
        )?;
        let request_membership = self
            .load_membership_at_verified_prefix(
                &acceptance.request.predecessor_membership.heads,
                &acceptance.request.predecessor_membership.resolutions,
                &verified_membership_activations,
                None,
            )
            .await?;
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
            .map(|activations| (activations, None))
            .map_err(StorePullError::from)
    }

    pub(crate) async fn verified_membership_objects(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeMembershipClosure>, StorePullError> {
        self.commit_verifier
            .verified_merge_membership_objects(commit_ref, commit)
            .await
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
        if !self
            .current_history_contains(&membership, &access.activation)
            .await?
        {
            return Err(StorePullError::InvalidState(
                "device provider approval activation is absent from current accepted Store history"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn current_history_contains(
        &mut self,
        membership: &MembershipChain,
        expected: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        self.verify_refs([expected.clone()]).await?;
        let mut state = self
            .history
            .commits
            .get(expected)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "provider-access activation is absent from its verified Merge graph"
                        .to_string(),
                )
            })?
            .state_after
            .clone();
        let mut registrations = BTreeMap::new();
        let founder = self.commit_verifier.load_founder_registration().await?;
        let founder_ref =
            StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object);
        registrations.insert(
            founder_ref.device_id,
            ReferencedStoreDeviceRegistration::verified(founder_ref, founder.value)
                .map_err(StorePullError::Protocol)?,
        );
        for recovered in self.discover_owner_recoveries(membership).await? {
            registrations.insert(recovered.reference().device_id, recovered);
        }
        self.load_state_registrations(&state, &mut registrations)
            .await?;

        let mut accepted = BTreeMap::new();
        let mut observed_states = BTreeSet::new();
        loop {
            let mut next = BTreeMap::new();
            for registration in registrations.values() {
                let registration_ref = registration.reference();
                let inactive_cut = match state.devices.get(&registration_ref.device_id) {
                    Some(record) if record.registration != *registration_ref => {
                        return Err(StorePullError::InvalidState(
                            "current Merge device state names another registration revision"
                                .to_string(),
                        ));
                    }
                    Some(record) => match &record.status {
                        StoreDeviceStatus::Active => None,
                        StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                    },
                    None => None,
                };
                let discovered = self
                    .discover_merge_stream(registration_ref, registration.value(), inactive_cut)
                    .await?;
                if matches!(discovered.block, Some(MergeStreamBlock::Authenticated(_))) {
                    return Err(StorePullError::InvalidState(
                        "an authenticated Merge stream position cannot be verified".to_string(),
                    ));
                }
                if let Some((_, _, reference, _)) = discovered.commits.last() {
                    let stream_id = reference.coord.stream_id;
                    next.insert(stream_id, reference.clone());
                }
            }
            self.verify_refs(next.values().cloned()).await?;
            let next_state = if next.is_empty() {
                self.history.genesis.clone()
            } else {
                ResolvedStoreDeviceState::merge(
                    next.values()
                        .map(|reference| {
                            self.history
                                .commits
                                .get(reference)
                                .map(|commit| commit.state_after.clone())
                                .ok_or_else(|| {
                                    StorePullError::InvalidState(
                                        "current Merge frontier is absent from its verified graph"
                                            .to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .map_err(StorePullError::Protocol)?
            };
            let registration_count = registrations.len();
            self.load_state_registrations(&next_state, &mut registrations)
                .await?;
            let stable = next == accepted
                && next_state == state
                && registrations.len() == registration_count;
            if stable {
                let accepted_closure =
                    verified_merge_commit_closure(&self.history.commits, next.values().cloned())?;
                return Ok(accepted_closure.contains(expected));
            }
            let state_fingerprint = ObjectHash::digest(
                &serde_json::to_vec(&(&next, &next_state))
                    .map_err(StorePullError::Serialization)?,
            );
            if !observed_states.insert(state_fingerprint) {
                return Err(StorePullError::InvalidState(
                    "current Merge authority discovery does not reach one stable frontier"
                        .to_string(),
                ));
            }
            accepted = next;
            state = next_state;
        }
    }

    pub async fn load_exact_anchored_membership(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        owner: Option<&str>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        let membership = membership::HistoryMembershipActivation::new(self)
            .load_exact_anchored_chain(heads, owner)
            .await?;
        if self.history.commits.is_empty() {
            let authority = VerifiedMergeMembershipPrefix::default();
            authority
                .validate_complete_membership(&membership)
                .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
            self.remember_verified_membership(authority, membership.clone());
        }
        Ok(membership)
    }

    pub(crate) async fn load_membership_at_exact_heads(
        &mut self,
        heads: &[protocol_membership::MembershipHeadRef],
        resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        membership::HistoryMembershipActivation::new(self)
            .load_at_exact_heads(heads, resolutions)
            .await
    }

    pub(crate) async fn load_membership_at_verified_prefix(
        &self,
        heads: &[protocol_membership::MembershipHeadRef],
        resolutions: &[protocol_membership::StoreMembershipConflictResolutionRef],
        verified_activations: &VerifiedMergeMembershipPrefix,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        VerifiedPrefixMembershipActivation::new(
            &self.root,
            &self.commit_verifier,
            verified_activations,
        )
        .load_at_exact_heads(heads, resolutions, pending_resolution)
        .await
    }

    pub(crate) async fn load_predecessor_membership(
        &mut self,
        state: &StoreMembershipStateRef,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_exact_heads(&state.heads, &state.resolutions)
            .await
            .map_err(RegistrationLoadError::from)
    }

    pub(crate) async fn load_predecessor_membership_at_verified_prefix(
        &self,
        state: &StoreMembershipStateRef,
        verified_activations: &VerifiedMergeMembershipPrefix,
        pending_resolution: Option<&VerifiedMergeConflictResolutionActivation>,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.load_membership_at_verified_prefix(
            &state.heads,
            &state.resolutions,
            verified_activations,
            pending_resolution,
        )
        .await
        .map_err(RegistrationLoadError::from)
    }

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

    pub(crate) async fn project_membership_to_verified_prefix(
        &self,
        candidate_heads: &[protocol_membership::MembershipHeadRef],
        prefix: &VerifiedMergeMembershipPrefix,
    ) -> Result<MembershipChain, crate::sync::store::membership::AnchoredChainError> {
        VerifiedPrefixMembershipActivation::new(&self.root, &self.commit_verifier, prefix)
            .project(candidate_heads)
            .await
    }

    pub(crate) async fn verify_membership_grant_revocation_nonactivation(
        &mut self,
        grant_id: &protocol_membership::MembershipGrantId,
        membership: &StoreMembershipStateRef,
        activation_commit: &StoreBatchCommitRef,
        activation_head: &store_commit::StoreDeviceHeadRef,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<remote_object::VerifiedCandidateNonactivation, StorePullError> {
        let root = self.root.reference().clone();
        let head_prefix =
            store_commit::semantic_prefix_from_exact_object(&activation_head.object, ".json")
                .map_err(StorePullError::Protocol)?;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let head_bytes = self
            .commit_verifier
            .read_protocol_object(&context, &activation_head.object, &head_prefix)
            .await?;
        activation_head.object.verify(&head_bytes)?;
        let witness_head: StoreDeviceHead =
            serde_json::from_slice(&head_bytes).map_err(|error| {
                StorePullError::context("membership revocation witness head", error)
            })?;
        if witness_head.head_hash() != activation_head.head_hash
            || &witness_head.commit != activation_commit
        {
            return Err(StorePullError::InvalidState(
                "membership revocation witness head differs from its exact activation".to_string(),
            ));
        }
        let witness_author = self
            .commit_verifier
            .load_registration(&witness_head.author_registration)
            .await?;
        let opened = self
            .commit_verifier
            .load_head(activation_head, &witness_author.value, &witness_head.commit)
            .await?;
        self.verify_refs([witness_head.commit.clone()]).await?;
        let witness_commit = self
            .history
            .commits
            .get(&witness_head.commit)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "membership revocation witness is absent from its verified history".to_string(),
                )
            })?
            .verified
            .clone();
        if witness_commit.author() != &witness_author.value {
            return Err(StorePullError::InvalidState(
                "membership revocation witness commit belongs to another author".to_string(),
            ));
        }
        let (_, exact_head) = self
            .commit_verifier
            .exact_next_announcement_slot(
                &witness_head.author_registration,
                &witness_author.value,
                Some(&witness_commit),
            )
            .await
            .map_err(|error| StorePullError::Store(Box::new(error)))?;
        if exact_head.as_ref() != Some(activation_head) || opened.value != witness_head {
            return Err(StorePullError::InvalidState(
                "membership revocation witness is not an accepted exact head".to_string(),
            ));
        }
        if witness_commit.value().membership_state != *membership {
            return Err(StorePullError::InvalidState(
                "membership revocation witness commit names another membership state".to_string(),
            ));
        }
        let current_membership = self
            .load_predecessor_membership(&witness_commit.value().membership_state)
            .await
            .map_err(StorePullError::from)?;
        let MembershipStatus::Resolved(current) = current_membership.status() else {
            return Err(StorePullError::InvalidState(
                "membership revocation witness state is conflicted".to_string(),
            ));
        };
        let Some(causal_grants::GrantState::Tombstoned {
            record: current_record,
            ..
        }) = current.grants.get(grant_id)
        else {
            return Err(StorePullError::InvalidState(
                "membership revocation witness grant is not tombstoned".to_string(),
            ));
        };
        let candidate_ref = candidate.reference();
        let candidate_commit = candidate.value();
        let candidate_author = candidate.author();
        let predecessor_membership = self
            .load_predecessor_membership(&candidate_commit.membership_state)
            .await
            .map_err(StorePullError::from)?;
        let MembershipStatus::Resolved(predecessor) = predecessor_membership.status() else {
            return Err(StorePullError::InvalidState(
                "membership revocation candidate predecessor is conflicted".to_string(),
            ));
        };
        let Some(predecessor_record) = predecessor.active_grant(grant_id) else {
            return Err(StorePullError::InvalidState(
                "membership revocation grant was not active at the candidate predecessor"
                    .to_string(),
            ));
        };
        if predecessor_record != current_record
            || predecessor_record.member_pubkey != candidate_author.author_pubkey
            || candidate_commit.membership_authority.as_ref()
                != Some(&predecessor_record.creation_authority)
        {
            return Err(StorePullError::InvalidState(
                "membership revocation grant differs from the candidate's signed authority"
                    .to_string(),
            ));
        }
        let cap = witness_commit
            .value()
            .order
            .predecessor_cut()
            .map_err(StorePullError::Protocol)?;
        let expected_stream = store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &candidate_commit.author_registration,
            store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = candidate_ref.coord;
        if stream_id != expected_stream
            || cap
                .commits()
                .get(&expected_stream)
                .is_some_and(|covered| sequence <= covered.coord.sequence())
        {
            return Err(StorePullError::InvalidState(
                "membership revocation candidate is not beyond the accepted witness cut"
                    .to_string(),
            ));
        }
        let verified_candidate_head = self
            .commit_verifier
            .verify_terminal_candidate_head(candidate, candidate_head, candidate_head_object)
            .await?;
        let durable = remote_object::CandidateNonactivation::from_durable_parts(
            candidate_ref,
            candidate_commit,
            remote_object::CandidateNonactivationProof::MergeMembershipGrantRevocation {
                grant_id: grant_id.clone(),
                membership: membership.clone(),
                activation_commit: witness_head.commit.clone(),
                activation_head: activation_head.clone(),
            },
        )
        .map_err(StorePullError::RemoteObject)?;
        remote_object::VerifiedCandidateNonactivation::from_verified_membership_grant_revocation(
            durable,
            candidate_ref.clone(),
            verified_candidate_head,
        )
        .map_err(StorePullError::RemoteObject)
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
