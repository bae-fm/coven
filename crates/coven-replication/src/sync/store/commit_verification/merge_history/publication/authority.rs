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
        self.extend_membership_cut_from_history(frontier.commits(), &mut heads)?;
        let heads = protocol_membership::MembershipFloor::from_heads(heads)
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        let membership = self
            .project_membership_to_verified_prefix(&heads.0, &prefix)
            .await?;
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
        let mut head_refs = commit.value().membership_state.heads.clone();
        self.extend_membership_cut_from_history(&frontier, &mut head_refs)?;
        let heads = protocol_membership::MembershipFloor::from_heads(head_refs)
            .map_err(crate::sync::store::membership::AnchoredChainError::from)?;
        let membership = self
            .load_membership_at_verified_prefix(&heads.0, &prefix)
            .await?;
        prefix.validate_complete_membership(&membership)?;
        if !membership_authorizes(&membership, commit.value(), commit.author()) {
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
        Ok(protocol_membership::MembershipFloor(
            membership.head_refs().to_vec(),
        ))
    }

    fn extend_membership_cut_from_history(
        &self,
        frontier: &BTreeMap<protocol_membership::AuthorStreamId, StoreBatchCommitRef>,
        head_refs: &mut Vec<protocol_membership::MembershipHeadRef>,
    ) -> Result<(), StorePullError> {
        if let Some(snapshot) = self.history.baseline.snapshot() {
            head_refs.extend(snapshot.meta.state.membership.heads.iter().cloned());
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
            if let Some(proof) = &prior.history_evidence.membership_proof {
                head_refs.push(proof.head.clone());
            }
        }
        Ok(())
    }
}
