use super::*;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMergeConflictResolutionActivation {
    pub(super) reference: protocol_membership::StoreMembershipConflictResolutionRef,
}

impl VerifiedMergeConflictResolutionActivation {
    pub(crate) fn reference(&self) -> &protocol_membership::StoreMembershipConflictResolutionRef {
        &self.reference
    }

    pub(crate) fn verifies(
        &self,
        reference: &protocol_membership::StoreMembershipConflictResolutionRef,
    ) -> bool {
        &self.reference == reference
    }
}

pub(crate) struct VerifiedOwnerPromotionRequestActivation {
    activation: store_commit::OwnerPromotionRequestActivation,
}

impl VerifiedOwnerPromotionRequestActivation {
    pub(crate) fn activation(&self) -> &store_commit::OwnerPromotionRequestActivation {
        &self.activation
    }
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn load_owner_promotion_request_publication(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<store_commit::RetainedOwnerPromotionRequestPublication, StorePullError> {
        self.commit_verifier
            .load_owner_promotion_request_publication(commit)
            .await
    }

    pub(crate) async fn retain_pending_owner_promotions<'input>(
        &self,
        summary: &mut RetainedVerifiedMergeHistorySummary,
        requests: impl IntoIterator<
            Item = (
                &'input StoreBatchCommit,
                &'input coven_database::AcceptedStoreCommitPublication,
                ResolvedStoreDeviceState,
            ),
        >,
        membership: &MembershipChain,
        state: &ResolvedStoreDeviceState,
    ) -> Result<(), StorePullError> {
        for (commit, accepted, predecessor_state) in requests {
            let request = commit.owner_promotion_request().ok_or_else(|| {
                StorePullError::InvalidState("request retirement received another operation".into())
            })?;
            let publication = self
                .load_owner_promotion_request_publication(commit)
                .await?;
            publication.value.validate_for_request_commit(commit)?;
            if &publication.value.publication != accepted.reference()
                || !matches!(
                    &accepted.entry().payload,
                    store_commit::StorePublicationPayload::Commit(reference)
                        if reference == &publication.value.commit
                )
            {
                return Err(StorePullError::InvalidState(
                    "request result differs from its exact accepted publication".into(),
                ));
            }
            let retained = store_commit::RetainedOwnerPromotionRequest {
                commit: commit.clone(),
                publication,
                predecessor_state,
            };
            retained.validate_shape()?;
            match summary.pending_owner_promotions.entry(request.promotion_id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(retained);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &retained => {
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(StorePullError::InvalidState(
                        "promotion request has different exact accepted results".into(),
                    ))
                }
            }
        }
        let mut pending = BTreeMap::new();
        for (id, proof) in std::mem::take(&mut summary.pending_owner_promotions) {
            if promotion_request_remains_actionable(proof.request()?, membership, state) {
                pending.insert(id, proof);
            }
        }
        summary.pending_owner_promotions = pending;
        Ok(())
    }

    pub(super) async fn verify_retained_owner_promotions(
        &self,
        summary: &RetainedVerifiedMergeHistorySummary,
        prefix: &VerifiedMergeMembershipPrefix,
        predecessor: &store_commit::StoreCurrentPublicationRecordBody,
    ) -> Result<(), StorePullError> {
        for proof in summary.pending_owner_promotions.values() {
            proof.validate_shape()?;
            let publication = &proof.publication.value.publication;
            if predecessor.accepted().is_none_or(|last| {
                publication.position > last.position
                    || (publication.position == last.position && publication != last)
            }) {
                return Err(StorePullError::InvalidState(
                    "retained request publication is outside its snapshot's accepted boundary"
                        .into(),
                ));
            }
            let request = proof.request()?;
            let membership = self
                .load_membership_at_verified_prefix(
                    &request.predecessor_membership.heads,
                    &request.predecessor_membership.resolutions,
                    prefix,
                    None,
                )
                .await?;
            let promoter = self
                .commit_verifier
                .load_registration(&request.promoter_registration)
                .await?;
            let member = self
                .commit_verifier
                .load_registration(&request.member_registration)
                .await?;
            proof
                .publication
                .value
                .verify_for(&proof.commit, &promoter.value)?;
            verify_promotion_request_predecessor(
                request,
                &promoter.value,
                &member.value,
                &membership,
                &proof.predecessor_state,
            )?;
        }
        Ok(())
    }

    pub(crate) async fn verify_resolution_activation_acceptance(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<Option<VerifiedMergeConflictResolutionActivation>, StorePullError> {
        let root = self.root.reference();
        let Some(store_commit::StoreControl { transition }) = commit.control() else {
            return Ok(None);
        };
        let entry = self
            .commit_verifier
            .membership_objects()
            .load_entry(&transition.body.entry)
            .await?;
        let protocol_membership::StoreAuthorityChange::ResolutionActivation { resolution } =
            &entry.value.change
        else {
            return Ok(None);
        };
        if entry.value.coord() != transition.body.entry.coord {
            return Err(StorePullError::InvalidState(
                "Merge resolution activation differs from its exact transition".to_string(),
            ));
        }
        let value = self
            .commit_verifier
            .membership_objects()
            .load_resolution(resolution)
            .await?;
        let registration = self
            .commit_verifier
            .load_registration(&commit.author_registration)
            .await?;
        let acceptance = &value.value.replacement_acceptance;
        let mut expected_activations = vec![
            store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                value.value.replacement_grant.clone(),
                acceptance.membership.clone(),
            ),
            store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                acceptance.owner_registration.clone(),
                value.value.replacement_grant.clone(),
                acceptance.recovery.clone(),
            ),
        ];
        expected_activations.sort();
        if acceptance.owner_registration != commit.author_registration
            || registration.value.author_pubkey != value.value.resolver_pubkey
            || entry.value.author_pubkey != value.value.resolver_pubkey
            || transition.body.author_registration != commit.author_registration
            || commit.stream_activations() != expected_activations
        {
            return Err(StorePullError::InvalidState(
                "Merge resolution activation differs from its accepted Owner authority".to_string(),
            ));
        }
        self.verify_owner_conflict_acceptance_at_tips(
            acceptance,
            &value.value.resolver_pubkey,
            commit_predecessor_references(commit),
        )
        .await?;
        Ok(Some(VerifiedMergeConflictResolutionActivation {
            reference: resolution.clone(),
        }))
    }

    async fn verify_owner_conflict_acceptance_at_tips(
        &self,
        acceptance: &store_commit::OwnerConflictResolutionAcceptance,
        resolver_pubkey: &str,
        allowed_tips: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        let registration = self
            .commit_verifier
            .load_registration(&acceptance.owner_registration)
            .await?;
        acceptance
            .verify(&registration.value)
            .map_err(StorePullError::Protocol)?;
        let state = merge_device_state_from_verified_history(
            &acceptance.device_state,
            &self.history,
            allowed_tips,
        )?;
        if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
            return Err(StorePullError::InvalidState(
                "conflict-resolution Owner registration is not active at its exact device state"
                    .to_string(),
            ));
        }
        self.commit_verifier
            .verify_canonical_owner_registration(
                &state,
                resolver_pubkey,
                &acceptance.owner_registration,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn verify_owner_recovery_activation(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<
        Option<(
            protocol_membership::MembershipGrantId,
            store_commit::OwnerRecoveryActivationId,
        )>,
        StorePullError,
    > {
        self.commit_verifier
            .verify_owner_recovery_activation(commit)
            .await
    }

    pub(crate) async fn verify_canonical_owner_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        owner_pubkey: &str,
        selected: &StoreDeviceRegistrationRef,
    ) -> Result<(), StorePullError> {
        self.commit_verifier
            .verify_canonical_owner_registration(state, owner_pubkey, selected)
            .await
    }

    pub(crate) async fn verify_owner_recovery_node_authority_at_activation(
        &mut self,
        node: &OwnerRecoveryNode,
        activation_membership: &MembershipChain,
    ) -> Result<(), StorePullError> {
        let historical = self.load_predecessor_membership(&node.membership).await?;
        Self::verify_owner_recovery_node_authority(node, &historical, activation_membership)
            .map_err(StorePullError::from)
    }

    pub(super) fn verify_owner_recovery_node_authority(
        node: &OwnerRecoveryNode,
        historical_membership: &MembershipChain,
        activation_membership: &MembershipChain,
    ) -> Result<(), RegistrationLoadError> {
        if !predecessor_verifies_owner(
            historical_membership,
            &node.membership,
            &node.owner_pubkey,
            &node.owner_grant,
        ) || activation_membership
            .active_owner_grant(&node.owner_pubkey)
            .as_ref()
            != Some(&node.owner_grant)
        {
            return Err(RegistrationLoadError::Invalid(
                "Owner recovery node lacks its exact historical and current Owner authority"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn find_owner_promotion_request_activation(
        &mut self,
        request: &store_commit::OwnerPromotionRequest,
    ) -> Result<VerifiedOwnerPromotionRequestActivation, StorePullError> {
        let root = self.root.reference().clone();
        let promoter = self
            .commit_verifier
            .load_registration(&request.promoter_registration)
            .await?;
        request
            .verify(&root, &promoter.value)
            .map_err(StorePullError::Protocol)?;
        if let Some(proof) = self
            .history
            .baseline
            .history_summary()
            .and_then(|baseline| {
                baseline
                    .summary
                    .pending_owner_promotions
                    .get(&request.promotion_id)
            })
        {
            if proof.request()? != request {
                return Err(StorePullError::InvalidState(
                    "promotion id names another retained request".into(),
                ));
            }
            return Ok(VerifiedOwnerPromotionRequestActivation {
                activation: (*proof.publication.value).clone(),
            });
        }
        let mut matches = self
            .history
            .commits
            .iter()
            .filter_map(|(reference, commit)| {
                (commit.verified.value().owner_promotion_request() == Some(request))
                    .then_some(reference)
            });
        let Some(commit) = matches.next().cloned() else {
            return Err(StorePullError::InvalidState(
                "Owner-promotion request has no accepted Merge activation".to_string(),
            ));
        };
        if matches.next().is_some() {
            return Err(StorePullError::InvalidState(
                "Owner-promotion request has more than one Merge activation".to_string(),
            ));
        }
        let publication = self
            .accepted_publication(&commit)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "Owner-promotion request commit has no accepted Store publication".to_string(),
                )
            })?
            .reference()
            .clone();
        Ok(VerifiedOwnerPromotionRequestActivation {
            activation: store_commit::OwnerPromotionRequestActivation {
                commit,
                publication,
            },
        })
    }

    pub(crate) async fn verify_owner_promotion_acceptance_with_history(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), StorePullError> {
        let store_commit::OwnerPromotionRequestActivation {
            commit: activation_commit,
            ..
        } = &acceptance.activation;
        self.verify_refs([activation_commit.clone()]).await?;
        self.verify_owner_promotion_acceptance_in_loaded_history(acceptance)
            .await
            .map(drop)
    }

    pub(crate) async fn verify_owner_promotion_acceptance_from_request_activation(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
        verified: VerifiedOwnerPromotionRequestActivation,
    ) -> Result<(), StorePullError> {
        if acceptance.activation != verified.activation {
            return Err(StorePullError::InvalidState(
                "Owner-promotion acceptance names another request activation".to_string(),
            ));
        }
        self.verify_owner_promotion_acceptance_in_loaded_history(acceptance)
            .await
            .map(drop)
    }

    pub(super) async fn verify_owner_promotion_acceptance_in_loaded_history(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
    ) -> Result<MembershipChain, StorePullError> {
        let request = &acceptance.request;
        let promoter = self
            .commit_verifier
            .load_registration(&request.promoter_registration)
            .await?;
        let candidate = self
            .commit_verifier
            .load_registration(&request.member_registration)
            .await?;
        request.verify(self.root.reference(), &promoter.value)?;
        acceptance.verify(&candidate.value)?;
        let (membership, predecessor_state) =
            if self.history.baseline.covers(&acceptance.activation.commit) {
                let baseline = self.history.baseline.history_summary().ok_or_else(|| {
                    StorePullError::InvalidState(
                        "covered request has no installed snapshot authority".into(),
                    )
                })?;
                let proof = baseline
                    .summary
                    .pending_owner_promotions
                    .get(&request.promotion_id)
                    .ok_or_else(|| {
                        StorePullError::InvalidState(
                            "snapshot has no continuing authority for this promotion request"
                                .into(),
                        )
                    })?;
                if proof.request()? != request.as_ref()
                    || *proof.publication.value != acceptance.activation
                {
                    return Err(StorePullError::InvalidState(
                        "promotion acceptance differs from its exact retained request result"
                            .into(),
                    ));
                }
                let prefix = VerifiedMergeMembershipPrefix::from_retained(&[
                    coven_database::RetainedMergeHistoryCheckpoint::Snapshot(baseline.clone()),
                ])?;
                let membership = self
                    .load_membership_at_verified_prefix(
                        &request.predecessor_membership.heads,
                        &request.predecessor_membership.resolutions,
                        &prefix,
                        None,
                    )
                    .await?;
                (membership, proof.predecessor_state.clone())
            } else {
                let verified = self
                    .history
                    .commits
                    .get(&acceptance.activation.commit)
                    .ok_or_else(|| {
                        StorePullError::InvalidState(
                        "Owner-promotion request activation is absent from its verified history"
                            .into(),
                    )
                    })?;
                if self
                    .accepted_publication(&acceptance.activation.commit)
                    .map(coven_database::AcceptedStoreCommitPublication::reference)
                    != Some(&acceptance.activation.publication)
                    || verified.verified.value().owner_promotion_request() != Some(request)
                {
                    return Err(StorePullError::InvalidState(
                        "Owner-promotion request is not activated by its exact Store publication"
                            .into(),
                    ));
                }
                acceptance
                    .activation
                    .validate_for_request_commit(verified.verified.value())?;
                (
                    verified.predecessor_membership.clone(),
                    verified.predecessor_state.clone(),
                )
            };
        verify_promotion_request_predecessor(
            request,
            &promoter.value,
            &candidate.value,
            &membership,
            &predecessor_state,
        )?;
        Ok(membership)
    }

    pub(crate) async fn verify_owner_conflict_acceptance(
        &mut self,
        acceptance: &store_commit::OwnerConflictResolutionAcceptance,
        resolver_pubkey: &str,
    ) -> Result<(), StorePullError> {
        let frontier = acceptance.device_state.frontier();
        let tips = frontier.commits().values().cloned().collect::<Vec<_>>();
        self.verify_refs(tips.clone()).await?;
        self.verify_owner_conflict_acceptance_at_tips(acceptance, resolver_pubkey, tips)
            .await
    }
}

fn promotion_request_remains_actionable(
    request: &store_commit::OwnerPromotionRequest,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
) -> bool {
    let Some(promoter) = membership.active_grant(&request.promoter_owner_grant) else {
        return false;
    };
    membership
        .active_owner_grant(&promoter.member_pubkey)
        .as_ref()
        == Some(&request.promoter_owner_grant)
        && membership.active_grant_ids(&request.member_pubkey)
            == BTreeSet::from([request.member_grant.clone()])
        && membership
            .active_grant(&request.member_grant)
            .is_some_and(|member| {
                member.member_pubkey == request.member_pubkey
                    && member.role == protocol_membership::StoreMembershipRoleGrant::Member
            })
        && device_state_has_active_registration(state, &request.promoter_registration)
        && device_state_has_active_registration(state, &request.member_registration)
        && !membership.head_refs().iter().any(|head| {
            head.coord.author_pubkey == promoter.member_pubkey
                && head.coord.author_owner_grant == request.promoter_owner_grant
                && head.coord.stream_id == request.finalization.author_stream
                && head.coord.seq >= request.finalization.seq
        })
}

fn verify_promotion_request_predecessor(
    request: &store_commit::OwnerPromotionRequest,
    promoter: &StoreDeviceRegistration,
    member: &StoreDeviceRegistration,
    membership: &MembershipChain,
    state: &ResolvedStoreDeviceState,
) -> Result<(), StorePullError> {
    verify_merge_membership_state_ref(&request.predecessor_membership, membership, state)?;
    if !device_state_has_active_registration(state, &request.promoter_registration)
        || !device_state_has_active_registration(state, &request.member_registration)
    {
        return Err(StorePullError::InvalidState(
            "Owner-promotion request registrations are not active at its exact predecessor".into(),
        ));
    }
    if membership
        .active_owner_grant(&promoter.author_pubkey)
        .as_ref()
        != Some(&request.promoter_owner_grant)
        || membership.active_grant_ids(&request.member_pubkey)
            != BTreeSet::from([request.member_grant.clone()])
        || membership
            .active_grant(&request.member_grant)
            .is_none_or(|record| {
                record.member_pubkey != request.member_pubkey
                    || record.role != protocol_membership::StoreMembershipRoleGrant::Member
            })
        || member.author_pubkey != request.member_pubkey
    {
        return Err(StorePullError::InvalidState(
            "Owner-promotion request does not name the exact active Owner and Member grants".into(),
        ));
    }
    Ok(())
}
