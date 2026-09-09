use super::*;

impl MergeHistoryVerifier<'_> {
    /// Derive membership from the whole verified accepted boundary, including
    /// controls whose row materialization is still held. Membership discovery
    /// is required for plaintext admission, not for this accepted history.
    pub(crate) async fn membership_at_accepted_publication(
        &mut self,
        publication: &VerifiedStorePublication,
        known_heads: &[protocol_membership::MembershipHeadRef],
    ) -> Result<MembershipChain, StorePullError> {
        let mut frontier = self.history.baseline.coverage().clone();
        for reference in publication.commits.keys() {
            frontier = frontier.join(CommitFrontier(BTreeMap::from([(
                reference.coord.stream_id,
                reference.clone(),
            )])))?;
        }
        self.verify_refs(frontier.commits().values().cloned())
            .await?;
        let prefix = self.verified_membership_prefix(frontier.commits().values().cloned())?;
        let mut heads = known_heads.to_vec();
        let mut resolutions = BTreeSet::new();
        self.extend_membership_cut_from_history(frontier.commits(), &mut heads, &mut resolutions)?;
        let heads = protocol_membership::MembershipFloor::from_heads(heads)
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        let membership = self
            .project_membership_to_verified_prefix(&heads.0, &prefix)
            .await?;
        if membership.resolution_refs() != resolutions.into_iter().collect::<Vec<_>>() {
            return Err(StorePullError::InvalidState(
                "current membership differs from its accepted resolution cut".into(),
            ));
        }
        prefix.validate_complete_membership(&membership)?;
        Ok(membership)
    }

    pub(super) async fn verify_commit_publication_authority(
        &mut self,
        reference: &StoreBatchCommitRef,
        position: store_commit::StorePublicationPosition,
    ) -> Result<protocol_membership::MembershipFloor, StorePullError> {
        let frontier = self.accepted_frontier_before(position)?;
        self.verify_refs(frontier.values().cloned()).await?;
        let (state, prefix) = self.verified_merge_history_authority_parts(&frontier)?;
        let candidate = self.history.commits.get(reference).ok_or_else(|| {
            StorePullError::InvalidState(
                "publication authority requires its verified Store commit".to_string(),
            )
        })?;
        let commit = candidate.verified.clone();
        let registrations = candidate.registrations.clone();
        let operations = candidate.operations.clone();
        let membership_entry = candidate
            .history_evidence
            .membership_proof
            .as_ref()
            .map(|proof| proof.entry_value.clone());
        let pending_resolution = candidate
            .membership_control
            .as_ref()
            .and_then(|control| control.conflict_resolution.clone());
        let mut head_refs = commit.value().membership_state.heads.clone();
        let mut resolutions = commit
            .value()
            .membership_state
            .resolutions
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        self.extend_membership_cut_from_history(&frontier, &mut head_refs, &mut resolutions)?;
        let heads = protocol_membership::MembershipFloor::from_heads(head_refs)
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        if let Some(pending) = &pending_resolution {
            if prefix.verifies_conflict_resolution(pending.reference()) {
                return Err(StorePullError::InvalidState(
                    "Store publication repeats an accepted membership resolution".to_string(),
                ));
            }
            resolutions.remove(pending.reference());
        }
        let mut membership = self
            .load_membership_at_verified_prefix(
                &heads.0,
                &resolutions.into_iter().collect::<Vec<_>>(),
                &prefix,
                None,
            )
            .await?;
        prefix.validate_complete_membership(&membership)?;
        if let Some(pending) = &pending_resolution {
            let resolution = self
                .membership_objects()
                .load_resolution(pending.reference())
                .await?;
            membership.apply_resolutions(
                self.root.reference().store_root_hash,
                &[(pending.reference().clone(), resolution.value)],
            )?;
        }
        if !membership_authorizes(Some(&membership), commit.value(), commit.author()) {
            return Err(StorePullError::InvalidState(
                "Store publication author lacks current membership authority".to_string(),
            ));
        }
        if commit.value().control().is_some() {
            let entry = membership_entry.as_ref().ok_or_else(|| {
                StorePullError::InvalidState(
                    "membership publication omits its verified exact entry".into(),
                )
            })?;
            membership
                .validate_publication_predecessor(entry)
                .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        }
        for activated in commit.value().device_registrations() {
            if let StoreDeviceRegistrationActivationRef::Recovery { node, .. } =
                &activated.authority
            {
                let node = self.load_owner_recovery_node(node).await?.value;
                self.verify_owner_recovery_node_authority_at_activation(&node, &membership)
                    .await?;
            }
        }
        let (state, _) = state.preactivate_recovery_author(commit.value(), &registrations)?;
        if !device_state_has_active_registration(&state, &commit.value().author_registration) {
            return Err(StorePullError::InvalidState(
                "Store publication author is inactive at its acceptance boundary".to_string(),
            ));
        }
        // Immutable candidates may be accepted after intervening device controls.
        // Validate their transitions against the actual accepted predecessor.
        operations.apply_to(state.clone())?;
        if pending_resolution.is_some() {
            self.verify_canonical_owner_registration(
                &state,
                &commit.author().author_pubkey,
                &commit.value().author_registration,
            )
            .await?;
        }
        Ok(protocol_membership::MembershipFloor(
            membership.head_refs().to_vec(),
        ))
    }

    fn extend_membership_cut_from_history(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        head_refs: &mut Vec<protocol_membership::MembershipHeadRef>,
        resolutions: &mut BTreeSet<protocol_membership::StoreMembershipConflictResolutionRef>,
    ) -> Result<(), StorePullError> {
        if let Some(snapshot) = self.history.baseline.snapshot() {
            head_refs.extend(snapshot.meta.state.membership.heads.iter().cloned());
            resolutions.extend(snapshot.meta.state.membership.resolutions.iter().cloned());
        }
        let closure = verified_merge_commit_closure(&self.history, frontier.values().cloned())?;
        for prior in closure
            .iter()
            .filter_map(|prior| self.history.commits.get(prior))
        {
            head_refs.extend(
                prior
                    .verified
                    .value()
                    .membership_state
                    .heads
                    .iter()
                    .cloned(),
            );
            resolutions.extend(
                prior
                    .verified
                    .value()
                    .membership_state
                    .resolutions
                    .iter()
                    .cloned(),
            );
            if let Some(proof) = &prior.history_evidence.membership_proof {
                head_refs.push(proof.head.clone());
                resolutions.extend(proof.head_value.body.resolutions.iter().cloned());
            }
        }
        Ok(())
    }
}
