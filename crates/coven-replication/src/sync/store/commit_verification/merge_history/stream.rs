use super::*;

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn history_cut_covers(
        &mut self,
        cut: &StoreHistoryCut,
        target: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        let Some(covering) = cut.0.get(&target.coord.stream_id) else {
            return Ok(false);
        };
        self.commit_position_covers(covering, target)
            .await
            .map_err(|error| match error {
                CommitCoverageError::Object(error) => StorePullError::Object(error),
                CommitCoverageError::MissingAncestry { commit_hash } => {
                    StorePullError::InvalidState(format!(
                        "exact Store ancestry is missing commit {commit_hash}"
                    ))
                }
            })
    }

    pub(crate) async fn registration_activation(
        &self,
        activated: &ActivatedStoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        activating_author: &StoreDeviceRegistration,
        predecessor: &MembershipChain,
        activated_join_attempts: &BTreeSet<coven_protocol::store_commit::DeviceJoinAttemptId>,
        recovery_memberships: &BTreeMap<OwnerRecoveryNodeRef, MembershipChain>,
    ) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "registration activation commit author is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        match (&registration.origin, &activated.authority) {
            (
                StoreDeviceRegistrationOrigin::Join {
                    attempt_id: origin_attempt,
                },
                StoreDeviceRegistrationActivationRef::Join { attempt_id },
            ) if origin_attempt == attempt_id => {
                // The registration says which attempt it was made under, signed
                // by the joining device itself, and the activating commit says
                // which attempt it activates. Whether that attempt was really
                // opened is settled against this device's own history.
                if !activated_join_attempts.contains(attempt_id) {
                    return Err(RegistrationLoadError::Invalid(
                        "registration activation names an unverified join attempt".to_string(),
                    ));
                }
                Ok(StoreDeviceRegistrationActivation::Join {
                    attempt_id: *attempt_id,
                })
            }
            (
                StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id: origin_recovery,
                    recovery_slot,
                    ..
                },
                StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
            ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
                let node_value = self
                    .load_owner_recovery_node(node)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                let mut reached_ref = node.clone();
                let mut reached = node_value.clone();
                while let Some(predecessor_ref) = reached.predecessor.clone() {
                    let predecessor_node = self
                        .load_owner_recovery_node(&predecessor_ref)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    if predecessor_node.next_slot != *reached_ref.object.slot() {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery node does not occupy its exact predecessor successor slot"
                                .to_string(),
                        ));
                    }
                    if predecessor_node.recovery_id != node_value.recovery_id {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery predecessor belongs to another recovery operation"
                                .to_string(),
                        ));
                    }
                    reached_ref = predecessor_ref;
                    reached = predecessor_node;
                }
                if node_value.recovery_id != *recovery_id
                    || node_value.readiness.registration != activated.registration
                    || node_value.next_slot == *node.object.slot()
                    || registration.author_pubkey != node_value.owner_pubkey
                {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery node differs from its exact registration".to_string(),
                    ));
                }
                let historical_membership = recovery_memberships.get(node).ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "recovery activation lacks its exact historical membership".to_string(),
                    )
                })?;
                Self::verify_owner_recovery_node_authority(
                    &node_value,
                    historical_membership,
                    predecessor,
                )?;
                let initial_ack = self
                    .load_store_ack(&node_value.readiness.initial_ack, registration)
                    .await
                    .map_err(RegistrationLoadError::Object)?;
                if initial_ack.sequence != 1
                    || initial_ack.successor.predecessor.is_some()
                    || initial_ack.registration != activated.registration
                    || initial_ack.store_cut != node_value.readiness.bootstrap_cut
                {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery readiness differs from its initial acknowledgement".to_string(),
                    ));
                }
                Ok(StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: *recovery_id,
                    node: node.clone(),
                })
            }
            _ => Err(RegistrationLoadError::Invalid(format!(
                "Store registration {} origin differs from its activation authority",
                registration.device_id
            ))),
        }
    }

    pub(crate) async fn predecessor_commit_matching(
        &mut self,
        order: &store_commit::StoreCommitOrder,
        mut matches: PredecessorCommitPredicate<'_>,
    ) -> Result<Option<VerifiedStoreBatchCommit>, RegistrationLoadError> {
        let mut pending = order
            .predecessor
            .iter()
            .chain(order.dependencies.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = self
                .load_ref(&reference)
                .await
                .map_err(registration_attempt_error)?;
            if matches(&commit) {
                return Ok(Some(commit));
            }
            pending.extend(commit.value().order.predecessor.iter().cloned());
            pending.extend(commit.value().order.dependencies.values().cloned());
        }
        Ok(None)
    }

    /// Keep the history traversal on the heap across its callers. Constructing
    /// the box before polling also releases construction storage before nested
    /// authority verification begins.
    #[inline(never)]
    pub(crate) fn verify_refs<'verification, I>(
        &'verification mut self,
        tips: I,
    ) -> std::pin::Pin<
        Box<
            impl std::future::Future<Output = Result<(), StorePullError>>
                + 'verification
                + use<'verification, 'a, I>,
        >,
    >
    where
        I: IntoIterator<Item = StoreBatchCommitRef> + 'verification,
    {
        Box::pin(async move {
            let mut pending = tips.into_iter().collect::<Vec<_>>();
            let mut loaded = BTreeMap::<StoreBatchCommitRef, VerifiedStoreBatchCommit>::new();
            while let Some(reference) = pending.pop() {
                // The baseline is where this walk ends. A covered commit is retired
                // — its rows, its package and its announcement head are gone, and
                // the signed image restates what it did — so loading it would ask
                // the provider for history this device deliberately dropped, once
                // per commit standing above it.
                if self.history.commits.contains_key(&reference)
                    || loaded.contains_key(&reference)
                    || self.history.superseded(&reference)
                {
                    continue;
                }
                let verified = self.load_ref(&reference).await?;
                pending.extend(commit_predecessor_references(verified.value()));
                loaded.insert(reference, verified);
            }

            while !loaded.is_empty() {
                let next = loaded.iter().find_map(|(reference, verified)| {
                    commit_predecessor_references(verified.value())
                        .iter()
                        .all(|dependency| {
                            self.history.commits.contains_key(dependency)
                                || self.history.superseded(dependency)
                        })
                        .then(|| reference.clone())
                });
                let Some(reference) = next else {
                    return Err(StorePullError::InvalidState(
                        "Merge history is cyclic or has an unresolved predecessor".to_string(),
                    ));
                };
                let verified = loaded.remove(&reference).ok_or_else(|| {
                    StorePullError::InvalidState(
                        "selected exclusion-history commit disappeared before verification"
                            .to_string(),
                    )
                })?;
                if !self.accepted_publications.contains_key(&reference)
                    && !self.history.retained.contains_key(&reference)
                {
                    return Err(StorePullError::InvalidState(format!(
                        "Store commit {reference:?} is absent from accepted publication history"
                    )));
                }
                let commit = verified.value().clone();
                let author = verified.author().clone();
                let predecessor_state = self.verified_predecessor_state(&commit)?;
                let verified_membership_prefix = verified_merge_membership_prefix(
                    &self.history,
                    commit_predecessor_references(&commit),
                )?;
                let cached_membership = self.cached_verified_membership(
                    &commit.membership_state,
                    &verified_membership_prefix,
                );
                let membership = match cached_membership {
                    Some(membership) => membership,
                    None => self
                        .load_membership_at_verified_prefix(
                            &commit.membership_state.heads,
                            &verified_membership_prefix,
                        )
                        .await
                        .map_err(StorePullError::MembershipChain)?,
                };
                verified_membership_prefix.validate_complete_membership(&membership)?;
                verify_merge_membership_state_ref(
                    &commit.membership_state,
                    &membership,
                    &predecessor_state,
                )?;
                if !membership_authorizes(&membership, &commit, &author) {
                    return Err(StorePullError::InvalidState(
                        "Merge history commit lacks exact membership authority".to_string(),
                    ));
                }
                let accepted_frontier = commit_predecessor_references(&commit);
                let registrations = match self.history.retained_registrations(&reference) {
                    Some(registrations) => registrations.to_vec(),
                    None => {
                        Box::pin(self.load_merge_commit_registrations(
                            &commit,
                            &author,
                            &membership,
                            &accepted_frontier,
                        ))
                        .await?
                    }
                };
                let (authorized_predecessor, recovery_author) = predecessor_state
                    .clone()
                    .preactivate_recovery_author(&commit, &registrations)
                    .map_err(StorePullError::Protocol)?;
                if !device_state_has_active_registration(
                    &authorized_predecessor,
                    &commit.author_registration,
                ) {
                    return Err(StorePullError::InvalidState(
                        "author exclusion history commit author is inactive at its predecessor"
                            .to_string(),
                    ));
                }
                let control_entry = match commit.control() {
                    Some(store_commit::StoreControl { transition }) => Some(
                        self.commit_verifier
                            .membership_objects()
                            .load_entry(&transition.body.entry)
                            .await
                            .map_err(StorePullError::Object)?
                            .value,
                    ),
                    None => None,
                };
                let operations = Box::pin(self.commit_verifier.load_commit_device_operations(
                    &commit,
                    control_entry.as_ref(),
                    &authorized_predecessor,
                    &membership,
                ))
                .await
                .map_err(StorePullError::from)?;
                let acknowledgement = self
                    .validate_commit_acknowledgement(&commit, &author)
                    .await
                    .map_err(StorePullError::from)?;
                let membership_control =
                    if let Some(store_commit::StoreControl { transition }) = commit.control() {
                        let activations =
                            Box::pin(self.verify_membership_control_with_retained_history(
                                &reference,
                                &commit,
                                &membership,
                                &predecessor_state,
                            ))
                            .await?;
                        Some(VerifiedMergeMembershipControl {
                            activations,
                            head_activation: VerifiedMergeMembershipHeadActivation {
                                commit: reference.clone(),
                                transition: transition.clone(),
                            },
                        })
                    } else {
                        None
                    };
                let owner_recovery = self
                    .commit_verifier
                    .verify_owner_recovery_activation(&commit)
                    .await?;
                let state = operations
                    .apply_to(authorized_predecessor)
                    .map_err(StorePullError::Protocol)?;
                let state = state
                    .apply_verified_lifecycle(
                        &commit,
                        &registrations,
                        recovery_author.as_ref(),
                        owner_recovery,
                    )
                    .map_err(StorePullError::Protocol)?;
                let membership_closure = Box::pin(
                    self.commit_verifier
                        .verified_merge_membership_objects(&reference, &commit),
                )
                .await?;
                let retained_acknowledgement = match acknowledgement {
                    Some((acknowledgement_ref, acknowledgement_value)) => {
                        Some(self.retain_acknowledgement(
                            &verified,
                            acknowledgement_ref,
                            acknowledgement_value,
                        )?)
                    }
                    None => None,
                };
                let history_evidence = store_commit::RetainedMergeCommitEvidence {
                    acknowledgement: retained_acknowledgement.map(Box::new),
                    membership_proof: membership_closure.map(|closure| Box::new(closure.proof)),
                };
                history_evidence
                    .validate_for(&reference, &commit)
                    .map_err(StorePullError::Protocol)?;
                let membership_to_remember = membership.clone();
                self.history.commits.insert(
                    reference,
                    VerifiedMergeHistoryCommit {
                        verified,
                        predecessor_membership: membership,
                        predecessor_state,
                        state_after: state,
                        registrations,
                        operations,
                        membership_control,
                        history_evidence,
                    },
                );
                self.remember_verified_membership(
                    verified_membership_prefix,
                    membership_to_remember,
                );
            }
            Ok(())
        })
    }
}
