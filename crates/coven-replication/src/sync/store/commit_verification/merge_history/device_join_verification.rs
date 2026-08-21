use super::*;

impl<'a> MergeHistoryVerifier<'a> {
    /// The attempts this commit activates a registration under.
    ///
    /// A registration is activated by a commit whose author is an active Owner
    /// at its predecessor, naming the attempt it was opened under. That attempt
    /// has to have been opened by a commit already in this device's history —
    /// or by this same commit, which is what a same-provider join does in one
    /// step. Nothing else is read: an outcome file restating the owner, the
    /// grant, and the registration it activates said nothing this commit does
    /// not say itself.
    pub(super) async fn validate_commit_join_activations(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        accepted: VerifiedMergePredecessorHistory<'_>,
    ) -> Result<BTreeSet<store_commit::DeviceJoinAttemptId>, RegistrationLoadError> {
        let mut activated = BTreeSet::new();
        for registration in commit.device_registrations() {
            let StoreDeviceRegistrationActivationRef::Join { attempt_id } = &registration.authority
            else {
                continue;
            };
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device join activation has no exact predecessor authority".to_string(),
                )
            })?;
            if !predecessor.is_owner_now(&activating_author.author_pubkey) {
                return Err(RegistrationLoadError::Invalid(
                    "device join activation author is not an active Owner at its predecessor"
                        .to_string(),
                ));
            }
            let opened_here = commit
                .device_join_attempt_decisions()
                .iter()
                .any(|decision| {
                    matches!(
                        decision,
                        DeviceJoinAttemptDecisionRef::Attempt(opened) if opened == attempt_id
                    )
                });
            let opened_before = accepted
                .contains_join_attempt(*attempt_id)
                .map_err(registration_attempt_error)?;
            if !(opened_here || opened_before) {
                return Err(RegistrationLoadError::Invalid(
                    "device join activation names an attempt absent from its predecessor history"
                        .to_string(),
                ));
            }
            activated.insert(*attempt_id);
        }
        Ok(activated)
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

    /// Verify the attempt this device is joining under and build the history
    /// it has to carry.
    ///
    /// `installed` is the coverage of the Store snapshot the joining device has
    /// already installed into its database — empty only when it truly starts
    /// from nothing. Passing the real coverage is what keeps the plan buildable
    /// at all on a store that reclaims: every commit the plan carries is read
    /// from its package, and a package behind an acknowledged snapshot is
    /// exactly what reclaim deletes.
    /// Verify the commit that opened this attempt and build the history the
    /// joining device has to carry.
    ///
    /// The commit is the attempt: its predecessor cut is the history the
    /// admitting device declared this device would install from, and its
    /// membership state is the authority that cut is read under. Both used to
    /// be restated in a separate signed file by the same device that signed the
    /// commit, which established nothing the commit did not.
    ///
    /// `installed` is the coverage of the Store snapshot the joining device has
    /// already installed into its database — empty only when it truly starts
    /// from nothing. Passing the real coverage is what keeps the plan buildable
    /// at all on a store that reclaims: every commit the plan carries is read
    /// from its package, and a package behind an acknowledged snapshot is
    /// exactly what reclaim deletes.
    pub(crate) async fn verify_attempt_and_prepare_device_join_bootstrap(
        &mut self,
        attempt_id: store_commit::DeviceJoinAttemptId,
        attempt_activation: &StoreBatchCommitRef,
        installed: &CommitFrontier,
    ) -> Result<(StoreHistoryCut, DeviceJoinBootstrapPlan), StorePullError> {
        self.verify_refs([attempt_activation.clone()]).await?;
        let activation = self.load_ref(attempt_activation).await?;
        let opens_this_attempt = activation
            .value()
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| {
                matches!(
                    decision,
                    store_commit::DeviceJoinAttemptDecisionRef::Attempt(opened)
                        if *opened == attempt_id
                )
            });
        if !opens_this_attempt {
            return Err(StorePullError::InvalidState(
                "device join attempt activation does not open this attempt".to_string(),
            ));
        }
        let bootstrap_cut = activation
            .value()
            .order
            .predecessor_cut()
            .map_err(StorePullError::Protocol)?;
        let membership_state = activation.value().membership_state.clone();
        let plan = self
            .prepare_device_join_bootstrap(
                &bootstrap_cut,
                attempt_activation,
                &membership_state,
                installed,
            )
            .await?;
        Ok((bootstrap_cut, plan))
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
}
