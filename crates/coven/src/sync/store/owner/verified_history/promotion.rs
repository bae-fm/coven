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
        let protocol_membership::MembershipChange::ResolutionActivation { resolution } =
            &entry.value.change
        else {
            return Ok(None);
        };
        if entry.value.coord() != transition.body.entry.coord {
            return Err(StorePullError::Database(
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
            return Err(StorePullError::Database(
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
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let state = merge_device_state_from_verified_history(
            &acceptance.device_state,
            &self.history.genesis,
            &self.history.commits,
            allowed_tips,
        )?;
        if !device_state_has_active_registration(&state, &acceptance.owner_registration) {
            return Err(StorePullError::Database(
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

    pub(crate) async fn discover_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<Vec<ReferencedStoreDeviceRegistration>, StorePullError> {
        self.commit_verifier
            .discover_owner_recoveries(membership)
            .await
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
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        let discovered = self
            .discover_merge_stream(&request.promoter_registration, &promoter.value, None)
            .await?;
        let mut matches =
            discovered
                .commits
                .into_iter()
                .filter_map(|(head_ref, _, commit_ref, commit)| {
                    (commit.owner_promotion_request() == Some(request))
                        .then_some((commit_ref, head_ref))
                });
        let Some((commit, head)) = matches.next() else {
            return Err(StorePullError::Database(
                "Owner-promotion request has no accepted Merge activation".to_string(),
            ));
        };
        if matches.next().is_some() {
            return Err(StorePullError::Database(
                "Owner-promotion request has more than one Merge activation".to_string(),
            ));
        }
        self.verify_refs([commit.clone()]).await?;
        Ok(VerifiedOwnerPromotionRequestActivation {
            activation: store_commit::OwnerPromotionRequestActivation { commit, head },
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
    }

    pub(crate) async fn verify_owner_promotion_acceptance_from_request_activation(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
        verified: VerifiedOwnerPromotionRequestActivation,
    ) -> Result<(), StorePullError> {
        if acceptance.activation != verified.activation {
            return Err(StorePullError::Database(
                "Owner-promotion acceptance names another request activation".to_string(),
            ));
        }
        self.verify_owner_promotion_acceptance_in_loaded_history(acceptance)
            .await
    }

    pub(super) async fn verify_owner_promotion_acceptance_in_loaded_history(
        &mut self,
        acceptance: &store_commit::OwnerPromotionAcceptance,
    ) -> Result<(), StorePullError> {
        let root = self.root.reference().clone();
        let request = &acceptance.request;
        let store_commit::OwnerPromotionRequestActivation {
            commit: activation_commit,
            head: activation_head,
        } = &acceptance.activation;
        let promoter = self
            .commit_verifier
            .load_registration(&request.promoter_registration)
            .await?;
        let candidate = self
            .commit_verifier
            .load_registration(&request.member_registration)
            .await?;
        request
            .verify(&root, &promoter.value)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        acceptance
            .verify(&candidate.value)
            .map_err(|error| StorePullError::Database(error.to_string()))?;

        let opened = self
            .commit_verifier
            .load_head(activation_head, &promoter.value, activation_commit)
            .await?;
        let verified = self.history.commits.get(activation_commit).ok_or_else(|| {
            StorePullError::Database(
                "Owner-promotion request activation is absent from its verified history"
                    .to_string(),
            )
        })?;
        let (_, exact_head) = self
            .commit_verifier
            .exact_next_announcement_slot(
                &request.promoter_registration,
                &promoter.value,
                Some(&verified.verified),
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        if opened.value.head_hash() != activation_head.head_hash
            || opened.value.commit != *activation_commit
            || exact_head.as_ref() != Some(activation_head)
        {
            return Err(StorePullError::Database(
                "Owner-promotion request is not activated by its exact Merge head".to_string(),
            ));
        }
        let verified_commit = verified.verified.value();
        if verified_commit.owner_promotion_request() != Some(request)
            || verified_commit.membership_state != request.predecessor_membership
            || verified_commit.device_state != request.predecessor_devices
            || verified_commit.author_registration != request.promoter_registration
        {
            return Err(StorePullError::Database(
                "Owner-promotion request commit differs from its signed predecessor authority"
                    .to_string(),
            ));
        }
        let verified_membership_activations = verified_merge_membership_prefix(
            &self.history.commits,
            commit_predecessor_references(verified_commit),
        )?;
        let membership = self
            .load_membership_at_verified_prefix(
                &request.predecessor_membership.heads,
                &request.predecessor_membership.resolutions,
                &verified_membership_activations,
                None,
            )
            .await
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        verify_merge_membership_state_ref(
            &request.predecessor_membership,
            &membership,
            &verified.predecessor_state,
        )?;
        if !device_state_has_active_registration(
            &verified.predecessor_state,
            &request.promoter_registration,
        ) || !device_state_has_active_registration(
            &verified.predecessor_state,
            &request.member_registration,
        ) {
            return Err(StorePullError::Database(
                "Owner-promotion request registrations are not active at its exact predecessor"
                    .to_string(),
            ));
        }
        if membership
            .active_owner_grant(&promoter.value.author_pubkey)
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
            || candidate.value.author_pubkey != request.member_pubkey
        {
            return Err(StorePullError::Database(
                "Owner-promotion request does not name the exact active Owner and Member grants"
                    .to_string(),
            ));
        }
        Ok(())
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
