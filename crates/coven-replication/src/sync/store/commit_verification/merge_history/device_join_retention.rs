use super::*;

impl MergeHistoryVerifier<'_> {
    pub(crate) fn retained_device_join_bootstrap(
        &self,
        activation: &StoreBatchCommitRef,
    ) -> Option<store_commit::device_join_exchange::DeviceJoinBootstrapClosure> {
        self.history
            .baseline
            .history_summary()
            .and_then(|baseline| baseline.summary.pending_device_joins.get(activation))
            .cloned()
    }

    pub(crate) async fn retain_pending_device_joins(
        &self,
        summary: &mut RetainedVerifiedMergeHistorySummary,
        state: &ResolvedStoreDeviceState,
        commits: BTreeMap<StoreBatchCommitRef, DeviceJoinBootstrapCommit>,
        publications: BTreeMap<StoreBatchCommitRef, store_commit::StorePublicationRef>,
        snapshot_predecessor: &store_commit::StoreCurrentPublicationRecord,
    ) -> Result<(), StorePullError> {
        let mut candidates = BTreeMap::new();
        for (reference, closure) in &summary.pending_device_joins {
            candidates.insert(reference.clone(), closure.verified_commit(reference)?);
        }
        for (reference, input) in &commits {
            if input
                .commit
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)))
            {
                candidates.insert(reference.clone(), input.commit.clone());
            }
        }
        let mut retained = BTreeMap::new();
        for (activation, opening) in candidates {
            let accepted = summary
                .membership_proofs
                .values()
                .map(|proof| &proof.commit_value)
                .chain(commits.values().map(|input| input.commit.value()));
            if !opening.has_pending_device_join_bootstrap(
                state,
                summary.post_state.frontier(),
                accepted,
            )? {
                continue;
            }
            if let Some(closure) = summary.pending_device_joins.get(&activation) {
                retained.insert(activation, closure.clone());
                continue;
            }
            let current = if opening
                .pending_bootstrap_registration(state, summary.post_state.frontier())?
                .is_some()
            {
                let proof = summary.membership_proofs.get(&activation).ok_or_else(|| {
                    StorePullError::InvalidState(
                        "registered pending Join has no accepted membership proof".into(),
                    )
                })?;
                let result = self
                    .commit_verifier
                    .membership_objects()
                    .load_head_acceptance(&proof.head, &proof.head_value)
                    .await?;
                if publications.get(&activation) != Some(result.value.publication()?) {
                    return Err(StorePullError::InvalidState(
                        "pending device join lacks its exact accepted publication".into(),
                    ));
                }
                result.value.accepted_current.clone()
            } else {
                snapshot_predecessor.clone()
            };
            let snapshot = current.latest_snapshot().ok_or_else(|| {
                StorePullError::InvalidState("pending device join has no accepted snapshot".into())
            })?;
            if opening.publication_base()
                != &store_commit::StorePublicationBase::Snapshot(snapshot.clone())
            {
                return Err(StorePullError::InvalidState(
                    "pending Attempt was not retained at its first covering snapshot".into(),
                ));
            }
            let metadata = self.load_snapshot_metadata(&snapshot.snapshot).await?;
            let publication = self
                .load_accepted_publication_interval(
                    metadata.publication_predecessor.clone(),
                    current,
                )
                .await?;
            let founder = self.load_founder_registration().await?;
            let plan = DeviceJoinBootstrapPlan::from_verified_commits(
                ReferencedStoreDeviceRegistration::verified(self.founder.clone(), founder.value)?,
                self.history.genesis.clone(),
                coven_database::InitialStoreMembershipAuthority {
                    head_refs: opening.membership_state.heads.clone(),
                },
                &metadata.coverage,
                coven_database::AcceptedStorePublicationInterval::from_verified(publication, None),
                commits.clone(),
            )?;
            let closure = plan.into_closure(self.root.reference())?;
            if publications.get(&activation)
                != Some(closure.accepted_commit(&activation)?.reference())
            {
                return Err(StorePullError::InvalidState(
                    "pending Attempt differs from its accepted publication".into(),
                ));
            }
            retained.insert(activation, closure);
        }
        summary.pending_device_joins = retained;
        self.verify_retained_device_joins(summary, state).await
    }

    pub(crate) async fn verify_retained_device_joins(
        &self,
        summary: &RetainedVerifiedMergeHistorySummary,
        state: &ResolvedStoreDeviceState,
    ) -> Result<(), StorePullError> {
        for (activation, proof) in &summary.membership_proofs {
            if proof
                .commit_value
                .pending_bootstrap_registration(state, summary.post_state.frontier())?
                .is_some()
                && !summary.pending_device_joins.contains_key(activation)
            {
                return Err(StorePullError::InvalidState(
                    "snapshot omits an unconsumed device join".into(),
                ));
            }
        }
        for (activation, closure) in &summary.pending_device_joins {
            let opening = closure.verified_commit(activation)?;
            let carried = closure
                .commits
                .iter()
                .map(|commit| closure.verified_commit(&commit.reference))
                .collect::<Result<Vec<_>, _>>()?;
            if !opening.has_pending_device_join_bootstrap(
                state,
                summary.post_state.frontier(),
                summary
                    .membership_proofs
                    .values()
                    .map(|proof| &proof.commit_value)
                    .chain(carried.iter().map(|commit| commit.value())),
            )? || closure.membership.0 != opening.membership_state.heads
                || closure.founder.reference() != &self.founder
                || closure.genesis != self.history.genesis
            {
                return Err(StorePullError::InvalidState(
                    "retained device join has no matching live bootstrap consumer".into(),
                ));
            }
            closure.accepted_commit(activation)?;
            if opening
                .pending_bootstrap_registration(state, summary.post_state.frontier())?
                .is_some()
            {
                let proof = summary.membership_proofs.get(activation).ok_or_else(|| {
                    StorePullError::InvalidState(
                        "registered pending Join has no accepted membership proof".into(),
                    )
                })?;
                let result = self
                    .commit_verifier
                    .membership_objects()
                    .load_head_acceptance(&proof.head, &proof.head_value)
                    .await?;
                if closure.publication.current != result.value.accepted_current
                    || closure.accepted_commit(activation)?.reference()
                        != result.value.publication()?
                {
                    return Err(StorePullError::InvalidState(
                        "retained device join differs from its accepted registration".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
