use super::*;

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn load_device_join_cleanup_activation(
        &mut self,
        activation: &device_join::DeviceJoinCleanupActivation,
    ) -> Result<LoadedDeviceJoinCleanupActivation, StorePullError> {
        let verified_commit = self.load_ref(&activation.activation).await?;
        if verified_commit.value().device_join_cleanup_receipts()
            != std::slice::from_ref(&activation.receipt)
        {
            return Err(StorePullError::InvalidState(
                "device join cleanup activation does not contain its exact sole receipt"
                    .to_string(),
            ));
        }
        let receipts = self
            .load_commit_join_cleanup_receipts(verified_commit.value(), verified_commit.author())
            .await
            .map_err(StorePullError::from)?;
        Ok(LoadedDeviceJoinCleanupActivation {
            verified_commit,
            receipts,
        })
    }

    pub(crate) async fn verify_device_join_cleanup_activation(
        &mut self,
        activation: LoadedDeviceJoinCleanupActivation,
    ) -> Result<
        coven_protocol::store_commit::device_join_exchange::JoinerJoinTerminal,
        StorePullError,
    > {
        let membership = self
            .load_predecessor_membership(&activation.verified_commit.value().membership_state)
            .await
            .map_err(StorePullError::from)?;
        if !membership.is_owner_now(&activation.verified_commit.author().author_pubkey) {
            return Err(StorePullError::InvalidState(
                "device join cleanup activation author is not an active Merge Owner".to_string(),
            ));
        }
        let [loaded] = <[_; 1]>::try_from(activation.receipts).map_err(|_| {
            StorePullError::InvalidState(
                "device join cleanup activation does not resolve to one verified receipt"
                    .to_string(),
            )
        })?;
        let attempt = self
            .verify_device_join_attempt_evidence(loaded.attempt)
            .await?;
        let expected = &attempt.value.provider_approval.request.offer.provider_admin;
        if !predecessor_verifies_provider_administrator(
            &membership,
            &loaded.receipt.provider_admin_grant,
            &loaded.receipt.executor,
            expected,
        ) {
            return Err(StorePullError::InvalidState(
                "device join cleanup executor is not the effective Merge provider administrator"
                    .to_string(),
            ));
        }
        Ok(loaded.receipt.joiner_terminal.clone())
    }

    pub(super) async fn validate_commit_join_abandonments(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
    ) -> Result<(), RegistrationLoadError> {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join abandonment activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join abandonment activation author is not an active Owner".to_string(),
            ));
        }
        for reference in commit
            .device_join_attempt_decisions()
            .iter()
            .filter_map(|decision| match decision {
                DeviceJoinAttemptDecisionRef::Attempt(_) => None,
                DeviceJoinAttemptDecisionRef::Abandoned(reference) => Some(reference),
            })
        {
            let context = ProtocolObjectContext::signed_plaintext(
                self.root.reference().store_root_hash,
                ProtocolObjectDomain::DeviceJoinAbandonment,
            );
            let semantic_prefix =
                store_commit::device_join_abandonment_semantic_prefix(reference.attempt_id);
            let bytes = self
                .commit_verifier
                .read_protocol_object(&context, &reference.object, &semantic_prefix)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let abandonment: device_join::DeviceJoinAbandonmentObject =
                serde_json::from_slice(&bytes).map_err(RegistrationLoadError::from)?;
            if abandonment.store_root_hash != self.root.reference().store_root_hash
                || abandonment.owner_registration != commit.author_registration
                || abandonment.attempt_slot != *reference.object.slot()
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join abandonment differs from its activating commit".to_string(),
                ));
            }
            reference
                .verify(&abandonment, activating_author)
                .map_err(RegistrationLoadError::from)?;
        }
        Ok(())
    }

    async fn load_commit_join_cleanup_receipts(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
    ) -> Result<Vec<LoadedCommitJoinCleanupReceipt>, RegistrationLoadError> {
        let mut receipts = Vec::with_capacity(commit.device_join_cleanup_receipts().len());
        for reference in commit.device_join_cleanup_receipts() {
            let context = ProtocolObjectContext::signed_plaintext(
                self.root.reference().store_root_hash,
                ProtocolObjectDomain::DeviceJoinCleanupReceipt,
            );
            let semantic_prefix =
                store_commit::device_join_cleanup_receipt_semantic_prefix(reference.attempt_id);
            let bytes = self
                .commit_verifier
                .read_protocol_object(&context, &reference.object, &semantic_prefix)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let receipt: device_join::DeviceJoinCleanupReceiptObject =
                serde_json::from_slice(&bytes).map_err(RegistrationLoadError::from)?;
            if receipt.executor != commit.author_registration
                || receipt.membership != commit.membership_state
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt differs from its activating predecessor"
                        .to_string(),
                ));
            }
            let attempt_ref = receipt.cancellation.attempt();
            let (attempt, owner) = self
                .load_device_join_attempt_and_owner(attempt_ref)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let attempt = self
                .validate_device_join_attempt_evidence(attempt, &owner.value)
                .await
                .map_err(registration_attempt_error)?;
            let expected_administrator = &attempt
                .attempt
                .value
                .provider_approval
                .request
                .offer
                .provider_admin;
            if activating_author.provider != expected_administrator.provider
                || attempt
                    .attempt
                    .value
                    .provider_approval
                    .request
                    .offer
                    .provider
                    != self.root.protocol().descriptor.provider
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup executor differs from its exact provider authority"
                        .to_string(),
                ));
            }
            reference
                .verify(&receipt, activating_author)
                .and_then(|_| receipt.verify(&attempt.attempt.value, activating_author))
                .map_err(RegistrationLoadError::from)?;
            match &receipt.administrator_terminal {
                device_join::ProviderAdminJoinTerminal::Completed(_) => {}
                device_join::ProviderAdminJoinTerminal::Cancelled(closure) => {
                    let administrator = self
                        .load_registration(&closure.administrator_registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    closure
                        .verify(&administrator)
                        .map_err(RegistrationLoadError::from)?;
                }
                device_join::ProviderAdminJoinTerminal::WriteRevoked(revocation) => {
                    let executor = self
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(RegistrationLoadError::from)?;
                }
            }
            match &receipt.joiner_terminal {
                device_join::JoinerJoinTerminal::Ready(_) => {}
                device_join::JoinerJoinTerminal::Cancelled(closure) => {
                    closure.verify().map_err(RegistrationLoadError::from)?
                }
                device_join::JoinerJoinTerminal::WriteRevoked(revocation) => {
                    let executor = self
                        .load_registration(&revocation.executor)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    revocation
                        .verify(&executor)
                        .map_err(RegistrationLoadError::from)?;
                }
            }
            receipts.push(LoadedCommitJoinCleanupReceipt { receipt, attempt });
        }
        Ok(receipts)
    }

    pub(crate) async fn validate_device_join_attempt_evidence(
        &self,
        attempt: VerifiedObject<DeviceJoinAttempt>,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        self.commit_verifier
            .validate_device_join_attempt_evidence(attempt, owner)
            .await
    }

    pub(crate) async fn verify_author_exclusion_nonactivation(
        &mut self,
        locator: &coven_database::AuthorExclusionActivationLocator,
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
        activation_commit: &VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        operations: &VerifiedStoreDeviceOperations,
        candidate: &VerifiedStoreBatchCommit,
        candidate_head: &StoreDeviceHead,
        candidate_head_object: &ExactObjectRef,
    ) -> Result<remote_object::VerifiedCandidateNonactivation, StorePullError> {
        self.commit_verifier
            .verify_author_exclusion_nonactivation(
                locator,
                activation_head,
                activation_head_object,
                activation_commit,
                activation_predecessor_state,
                operations,
                candidate,
                candidate_head,
                candidate_head_object,
            )
            .await
    }

    pub(crate) async fn load_verified_device_join_attempt(
        &mut self,
        reference: &store_commit::DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<store_commit::DeviceJoinAttempt>, StorePullError> {
        let evidence = self
            .commit_verifier
            .load_device_join_attempt_evidence(reference, owner)
            .await?;
        self.verify_device_join_attempt_evidence(evidence).await
    }

    pub(crate) async fn authenticate_blocked_candidate(
        &mut self,
        candidate: &coven_database::BlockedMergeCandidate,
    ) -> Result<VerifiedStoreBatchCommit, StoreError> {
        let reference = &candidate.head.commit;
        let verified = self
            .commit_verifier
            .authenticate_bytes(reference, &candidate.commit_bytes)
            .await?;
        if verified.value() != candidate.commit.value()
            || verified.reference().object != candidate.commit_object
            || verified.value().to_bytes() != candidate.commit_bytes
        {
            return Err(StoreError::InvalidOutbound(
                "blocked Merge candidate differs from its authenticated commit".to_string(),
            ));
        }
        Ok(verified)
    }

    pub(crate) async fn load_verified_device_join_attempt_and_owner(
        &mut self,
        reference: &store_commit::DeviceJoinAttemptRef,
    ) -> Result<
        (
            VerifiedObject<store_commit::DeviceJoinAttempt>,
            VerifiedObject<StoreDeviceRegistration>,
        ),
        StorePullError,
    > {
        let (attempt, owner) = self
            .commit_verifier
            .load_device_join_attempt_and_owner(reference)
            .await?;
        let evidence = self
            .commit_verifier
            .validate_device_join_attempt_evidence(attempt, &owner.value)
            .await?;
        let attempt = self.verify_device_join_attempt_evidence(evidence).await?;
        Ok((attempt, owner))
    }

    pub(crate) async fn verify_attempt_and_prepare_device_join_bootstrap(
        &mut self,
        attempt: &store_commit::DeviceJoinAttemptRef,
        attempt_owner: &StoreDeviceRegistration,
        attempt_activation: &StoreBatchCommitRef,
    ) -> Result<
        (
            VerifiedObject<store_commit::DeviceJoinAttempt>,
            DeviceJoinBootstrapPlan,
        ),
        StorePullError,
    > {
        let evidence = self
            .commit_verifier
            .load_device_join_attempt_evidence(attempt, attempt_owner)
            .await?;
        let verified_attempt = self.verify_device_join_attempt_evidence(evidence).await?;
        // This device builds the plan for itself and materializes it into a
        // database that holds no Store snapshot, so nothing is already there to
        // carry from — the closure runs back to genesis.
        let plan = self
            .prepare_device_join_bootstrap(
                &verified_attempt.value.bootstrap_cut,
                attempt_activation,
                &verified_attempt.value.membership,
                &CommitFrontier(BTreeMap::new()),
            )
            .await?;
        Ok((verified_attempt, plan))
    }

    /// The commits a joining device has to materialize to stand at
    /// `bootstrap_cut`, in an order that never puts a commit before one it
    /// depends on.
    ///
    /// `installed` is the history that device already holds when it applies the
    /// plan — the coverage of the Store snapshot it installs first. The walk
    /// stops there instead of at genesis, so a join onto a long-lived Store
    /// carries only what was published after that snapshot. A device that
    /// starts from nothing passes an empty frontier and gets the whole closure.
    ///
    /// Trimming does not weaken what the receiver checks. Every commit that is
    /// still carried is parsed and signature-checked on arrival exactly as
    /// before, and the commits left out are the ones the owner already signed
    /// for in the snapshot's metadata, which the receiver verifies before it
    /// installs the image.
    pub(crate) async fn prepare_device_join_bootstrap(
        &mut self,
        bootstrap_cut: &StoreHistoryCut,
        attempt_activation: &StoreBatchCommitRef,
        membership_state: &StoreMembershipStateRef,
        installed: &CommitFrontier,
    ) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
        let membership = self
            .load_predecessor_membership(membership_state)
            .await
            .map_err(StorePullError::from)?;
        let mut pending = history_cut_references(bootstrap_cut);
        pending.push(attempt_activation.clone());
        self.verify_refs(pending.clone()).await?;
        self.prepare_device_join_bootstrap_from_verified_parts(
            bootstrap_cut,
            attempt_activation,
            membership_state,
            membership,
            pending,
            installed,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_device_join_bootstrap_from_verified_parts(
        &self,
        bootstrap_cut: &StoreHistoryCut,
        attempt_activation: &StoreBatchCommitRef,
        membership_state: &StoreMembershipStateRef,
        membership: MembershipChain,
        mut pending: Vec<StoreBatchCommitRef>,
        installed: &CommitFrontier,
    ) -> Result<DeviceJoinBootstrapPlan, StorePullError> {
        // The bootstrap carries the founder registration itself, so this is one
        // of the few places that wants the object rather than its reference.
        let founder = self.load_founder_registration().await?;
        let founder_reference = self.founder.clone();
        let genesis = self.history.genesis.clone();
        let activation = self
            .history
            .commits
            .get(attempt_activation)
            .ok_or_else(|| {
                StorePullError::InvalidState(
                    "device join attempt activation is absent from its graph".into(),
                )
            })?;
        if activation
            .verified
            .value()
            .order
            .predecessor_cut()
            .map_err(StorePullError::Protocol)?
            != *bootstrap_cut
        {
            return Err(StorePullError::InvalidState(
                "device join attempt activation predecessor differs from its signed bootstrap cut"
                    .to_string(),
            ));
        }
        if &activation.verified.value().membership_state != membership_state {
            return Err(StorePullError::InvalidState(
                "device join attempt activation differs from its exact verified membership state"
                    .to_string(),
            ));
        }

        let mut required = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if installed.covers_commit(&reference) || !required.insert(reference.clone()) {
                continue;
            }
            let verified = self.history.commits.get(&reference).ok_or_else(|| {
                StorePullError::InvalidState(format!(
                    "verified device join bootstrap is missing required commit {}/{}",
                    reference.coord.stream_id, reference.coord.sequence,
                ))
            })?;
            pending.extend(commit_predecessor_references(verified.verified.value()));
        }

        let mut emitted = BTreeSet::new();
        let mut ordered = Vec::with_capacity(required.len());
        while emitted.len() != required.len() {
            let next = required.iter().find_map(|reference| {
                let verified = &self.history.commits[reference];
                (!emitted.contains(reference)
                    && commit_predecessor_references(verified.verified.value())
                        .iter()
                        .all(|dependency| {
                            installed.covers_commit(dependency) || emitted.contains(dependency)
                        }))
                .then(|| reference.clone())
            });
            let Some(reference) = next else {
                return Err(StorePullError::InvalidState(
                    "verified device join bootstrap history has an unresolved predecessor"
                        .to_string(),
                ));
            };
            let verified = &self.history.commits[&reference];
            ordered.push(DeviceJoinBootstrapCommit {
                reference: reference.clone(),
                commit: verified.verified.clone(),
                registrations: verified.registrations.clone(),
                device_operations: verified.operations.clone(),
                activation: DeviceJoinBootstrapActivation {
                    head: verified.activation_head.clone(),
                    object: verified.activation_head_object.clone(),
                    history_evidence: verified.history_evidence.clone(),
                },
            });
            emitted.insert(reference);
        }

        Ok(DeviceJoinBootstrapPlan {
            founder_reference,
            founder: founder.value,
            founder_bytes: founder.bytes,
            genesis,
            membership: coven_database::InitialStoreMembershipAuthority {
                head_refs: membership.head_refs().to_vec(),
            },
            commits: ordered,
        })
    }

    pub(crate) async fn load_commit_join_evidence(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
    ) -> Result<LoadedCommitJoinEvidence, RegistrationLoadError> {
        let loaded_cleanup = self
            .load_commit_join_cleanup_receipts(commit, activating_author)
            .await?;
        let mut attempts = BTreeMap::new();
        let mut cleanup_receipts = Vec::with_capacity(loaded_cleanup.len());
        for loaded in loaded_cleanup {
            let attempt = loaded.receipt.cancellation.attempt().clone();
            attempts.entry(attempt.clone()).or_insert(loaded.attempt);
            cleanup_receipts.push(CommitJoinCleanupReceiptEvidence {
                receipt: loaded.receipt,
                attempt,
            });
        }
        let references = commit
            .device_join_attempt_decisions()
            .iter()
            .filter_map(|decision| match decision {
                DeviceJoinAttemptDecisionRef::Attempt(reference) => Some(reference),
                DeviceJoinAttemptDecisionRef::Abandoned(_) => None,
            })
            .chain(
                commit
                    .device_join_outcomes()
                    .iter()
                    .map(|outcome| outcome.attempt()),
            )
            .cloned()
            .collect::<BTreeSet<_>>();
        for reference in references {
            if attempts.contains_key(&reference) {
                continue;
            }
            let (attempt, owner) = self
                .load_device_join_attempt_and_owner(&reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let evidence = self
                .validate_device_join_attempt_evidence(attempt, &owner.value)
                .await
                .map_err(registration_attempt_error)?;
            attempts.insert(reference, evidence);
        }
        Ok(LoadedCommitJoinEvidence {
            attempts,
            cleanup_receipts,
        })
    }

    pub(crate) async fn validate_commit_join_outcomes(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
        accepted: VerifiedMergePredecessorHistory<'_>,
    ) -> Result<BTreeMap<DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>, RegistrationLoadError>
    {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join outcome activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join outcome activation author is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        let mut verified = BTreeMap::new();
        for outcome_ref in commit.device_join_outcomes() {
            let attempt = join_evidence
                .attempts
                .get(outcome_ref.attempt())
                .ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "device join outcome has no verified exact attempt".to_string(),
                    )
                })?;
            let attempt_activated_here =
                commit
                    .device_join_attempt_decisions()
                    .iter()
                    .any(|decision| {
                        matches!(
                            decision,
                            DeviceJoinAttemptDecisionRef::Attempt(reference)
                                if reference == outcome_ref.attempt()
                        )
                    });
            let accepted_before = accepted
                .contains_join_attempt(outcome_ref.attempt())
                .map_err(registration_attempt_error)?;
            let same_principal_attempt_activated_here = attempt_activated_here
                && matches!(
                    attempt.provider_approval.admission,
                    coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission::SamePrincipal
                );
            if !(accepted_before || same_principal_attempt_activated_here) {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome names an attempt absent from its predecessor history"
                        .to_string(),
                ));
            }
            if attempt.owner_registration != commit.author_registration
                || outcome_ref.slot() != &attempt.outcome_slot
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome differs from its exact Owner attempt".to_string(),
                ));
            }
            let outcome = self
                .load_device_join_outcome(outcome_ref, activating_author)
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            if outcome.owner_registration != attempt.owner_registration
                || outcome.owner_grant != attempt.owner_grant
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome signer differs from its attempt".to_string(),
                ));
            }
            let activation = commit.device_registrations().iter().find(|activation| {
                matches!(
                    &activation.authority,
                    StoreDeviceRegistrationActivationRef::Join { outcome, .. }
                        if outcome == outcome_ref
                )
            });
            if matches!(
                &outcome.disposition,
                DeviceJoinDisposition::Activated { .. }
            ) != activation.is_some()
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome and registration activation are not one closed operation"
                        .to_string(),
                ));
            }
            if verified
                .insert(
                    outcome_ref.clone(),
                    VerifiedCommitJoinOutcome {
                        attempt: attempt.clone(),
                        owner: activating_author.clone(),
                        outcome,
                    },
                )
                .is_some()
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join outcome is duplicated in one commit".to_string(),
                ));
            }
        }
        Ok(verified)
    }
}
