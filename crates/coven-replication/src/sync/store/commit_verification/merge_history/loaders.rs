use super::*;

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn load_acknowledgement_proof_chain(
        &self,
        latest_ref: StoreAckRef,
        latest: StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<BTreeMap<u64, (StoreAckRef, StoreAck)>, RegistrationLoadError> {
        let mut chain = BTreeMap::new();
        let mut current_ref = latest_ref;
        let mut current = latest;
        loop {
            if chain
                .insert(current_ref.sequence, (current_ref.clone(), current.clone()))
                .is_some()
            {
                return Err(RegistrationLoadError::Invalid(
                    "Store acknowledgement proof chain repeats a sequence".to_string(),
                ));
            }
            if current.successor.predecessor.is_none() {
                break;
            }
            // The verifier answers from what it already holds when it can, so a
            // walk over a prefix another walk covered stops at the first ack it
            // has rather than reading the rest again.
            let Some((predecessor_ref, predecessor)) = self
                .load_store_ack_predecessor(&current_ref, &current, registration)
                .await
                .map_err(RegistrationLoadError::Object)?
            else {
                return Err(RegistrationLoadError::Invalid(
                    "Store acknowledgement named a predecessor that was not loaded".to_string(),
                ));
            };
            current_ref = predecessor_ref;
            current = predecessor;
        }
        if chain.first_key_value().map(|(sequence, _)| *sequence) != Some(1)
            || chain.last_key_value().map(|(sequence, _)| *sequence) != Some(chain.len() as u64)
        {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement proof chain is not contiguous from sequence one".to_string(),
            ));
        }
        for (reference, value) in chain.values() {
            self.commit_verifier
                .remember_acknowledgement(reference, value)
                .map_err(RegistrationLoadError::from)?;
        }
        Ok(chain)
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, StorePullError> {
        if let Some(verified) = self.history.commits.get(reference) {
            return Ok(verified.verified.clone());
        }
        Ok(self.commit_verifier.load_ref(reference).await?)
    }

    pub(crate) async fn load_covered_commits(
        &mut self,
        coverage: &CommitFrontier,
    ) -> Result<Vec<(StoreBatchCommitRef, VerifiedStoreBatchCommit)>, StorePullError> {
        let mut commits = BTreeMap::new();
        for tip in coverage.0.values() {
            let mut cursor = Some(tip.clone());
            while let Some(reference) = cursor {
                if commits.contains_key(&reference) {
                    break;
                }
                let commit = self.load_ref(&reference).await?;
                cursor = commit.value().order.predecessor().cloned();
                commits.insert(reference, commit);
            }
        }
        Ok(commits.into_iter().collect())
    }

    pub(crate) async fn commit_position_covers(
        &mut self,
        covering: &StoreBatchCommitRef,
        covered: &StoreBatchCommitRef,
    ) -> Result<bool, CommitCoverageError> {
        if covering.coord.stream_id != covered.coord.stream_id
            || covering.coord.sequence() < covered.coord.sequence()
        {
            return Ok(false);
        }
        let mut cursor = covering.clone();
        while cursor.coord.sequence() > covered.coord.sequence() {
            let commit = self.commit_verifier.load_ref(&cursor).await?;
            cursor = commit.value().order.predecessor().cloned().ok_or(
                CommitCoverageError::MissingAncestry {
                    commit_hash: cursor.commit_hash,
                },
            )?;
        }
        Ok(cursor == *covered)
    }

    pub(crate) async fn authenticate_bytes(
        &mut self,
        reference: &StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<VerifiedStoreBatchCommit, StoreObjectError> {
        self.commit_verifier
            .authenticate_bytes(reference, bytes)
            .await
    }

    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        self.commit_verifier.load_registration(reference).await
    }

    pub(crate) async fn load_founder_registration(
        &self,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        self.commit_verifier.load_registration(&self.founder).await
    }

    pub(crate) async fn load_store_snapshot_image(
        &self,
        snapshot: &coven_database::PublishedStoreSnapshot,
    ) -> Result<Vec<u8>, StoreObjectError> {
        self.commit_verifier
            .load_store_snapshot_image(&snapshot.reference, &snapshot.meta)
            .await
    }

    pub(crate) async fn load_device_exclusion_proposal(
        &self,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> Result<VerifiedDeviceExclusionProposal, StoreObjectError> {
        self.commit_verifier
            .load_device_exclusion_proposal(reference)
            .await
    }

    pub(crate) async fn load_device_exclusion_outcome(
        &self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &VerifiedDeviceExclusionProposal,
    ) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
        self.commit_verifier
            .load_device_exclusion_outcome(reference, proposal)
            .await
    }

    pub(crate) async fn load_store_ack(
        &self,
        reference: &StoreAckRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<StoreAck, StoreObjectError> {
        self.commit_verifier
            .load_store_ack(reference, registration)
            .await
    }

    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, StoreAck)>, StoreObjectError> {
        self.commit_verifier
            .load_store_ack_predecessor(successor_ref, successor, registration)
            .await
    }

    pub(crate) async fn load_store_snapshot(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &StoreSnapshotRef,
    ) -> Result<(StoreSnapshotRef, SnapshotMeta), StoreObjectError> {
        if let Some(installed) = self
            .history
            .baseline
            .snapshot()
            .filter(|installed| &installed.reference == reference)
        {
            installed
                .meta
                .verify_at(
                    self.root.reference().store_root_hash,
                    reference,
                    registration,
                )
                .and_then(|()| {
                    if installed.meta.author_registration != *registration_ref {
                        return Err(StoreProtocolError::Malformed(
                            "installed Store snapshot names another exact author registration"
                                .to_string(),
                        ));
                    }
                    Ok(())
                })
                .map_err(|source| StoreObjectError::InvalidObject {
                    semantic_prefix: reference.object.slot().logical_key().to_string(),
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                })?;
            return Ok((reference.clone(), installed.meta.clone()));
        }
        self.commit_verifier
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(crate) async fn load_reclaim_authorization(
        &self,
        reference: &coven_protocol::reclaim::ReclaimAuthorizationRef,
    ) -> Result<
        crate::sync::store::commit_verification::commit::VerifiedReclaimAuthorization,
        StoreObjectError,
    > {
        self.commit_verifier
            .load_reclaim_authorization(reference)
            .await
    }

    pub(crate) async fn load_reclaim_receipt(
        &self,
        reference: &coven_protocol::reclaim::ReclaimReceiptRef,
    ) -> Result<
        crate::sync::store::commit_verification::commit::VerifiedReclaimReceipt,
        StoreObjectError,
    > {
        self.commit_verifier.load_reclaim_receipt(reference).await
    }

    pub(crate) async fn load_owner_recovery_node(
        &self,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<VerifiedObject<OwnerRecoveryNode>, StoreObjectError> {
        self.commit_verifier
            .load_owner_recovery_node(reference)
            .await
    }

    pub(crate) async fn load_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<Option<VerifiedObject<Vec<u8>>>, StoreObjectError> {
        self.commit_verifier.load_store_package(reference).await
    }

    pub(crate) async fn prefetch_store_packages<'commits>(
        &self,
        commits: impl IntoIterator<
            Item = (
                &'commits StoreBatchCommitRef,
                &'commits coven_protocol::store_commit::StoreBatchCommit,
            ),
        >,
    ) {
        self.commit_verifier.prefetch_store_packages(commits).await
    }

    pub(crate) async fn load_provider_access_grant(
        &self,
        reference: &coven_protocol::provider::StoreMemberProviderAccessGrantRef,
        administrator: &StoreDeviceRegistration,
    ) -> Result<
        VerifiedObject<coven_protocol::provider::StoreMemberProviderAccessGrant>,
        StoreObjectError,
    > {
        self.commit_verifier
            .load_provider_access_grant(reference, administrator)
            .await
    }

    pub(crate) async fn load_merge_commit_registrations(
        &mut self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: &MembershipChain,
        accepted_frontier: &[StoreBatchCommitRef],
    ) -> Result<Vec<ActivatedStoreDeviceRegistration>, RegistrationLoadError> {
        let recovery_nodes = commit
            .device_registrations()
            .iter()
            .filter_map(|activated| match &activated.authority {
                StoreDeviceRegistrationActivationRef::Recovery { node, .. } => Some(node.clone()),
                StoreDeviceRegistrationActivationRef::Join { .. } => None,
            })
            .collect::<Vec<_>>();
        let mut recovery_memberships = BTreeMap::new();
        for node_ref in recovery_nodes {
            let node = self.load_owner_recovery_node(&node_ref).await?.value;
            let membership = self.load_predecessor_membership(&node.membership).await?;
            recovery_memberships.insert(node_ref, membership);
        }
        let accepted = VerifiedMergePredecessorHistory::new(&self.history, accepted_frontier);
        if let Some(reference) = commit.reclaim_authorization() {
            let opened = self
                .commit_verifier
                .load_reclaim_authorization_record(reference, &activating_author.author_pubkey)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let authorization = &opened.value;
            let owner_authorized = authorization.authority.membership == commit.membership_state
                && predecessor_verifies_owner(
                    predecessor,
                    &authorization.authority.membership,
                    &activating_author.author_pubkey,
                    &authorization.authority.owner_grant,
                );
            if !owner_authorized {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim authorization signer is not an active Owner at its exact predecessor"
                        .to_string(),
                ));
            }
            // Each kind of activating authority is re-read differently, so the binding
            // between the evidence and the object it authorizes deleting dispatches on
            // which authority published the target.
            let target = &authorization.target;
            match target.activation() {
                coven_protocol::reclaim::ReclaimActivation::Commit(activating_commit) => {
                    accepted.validate_commit_activated_reclaim_target(target, activating_commit)
                }
                coven_protocol::reclaim::ReclaimActivation::CircleSnapshotMetadata(activation) => {
                    validate_circle_snapshot_activated_reclaim_target(target, &activation)
                }
                coven_protocol::reclaim::ReclaimActivation::PackageBlobBinding(activation) => {
                    accepted.validate_package_bound_reclaim_target(target, activation)
                }
                coven_protocol::reclaim::ReclaimActivation::StoreBlobInventory(blob) => {
                    let store_commit::StorePublicationBase::Snapshot(base) =
                        commit.publication_base()
                    else {
                        return Err(RegistrationLoadError::Invalid(
                            "Store blob reclaim has no accepted snapshot inventory".into(),
                        ));
                    };
                    // Admission binds this base to the snapshot at the commit's
                    // exact publication. The retained replay floor can be older.
                    let snapshot = coven_database::PublishedStoreSnapshot {
                        reference: base.snapshot.clone(),
                        meta: self
                            .load_snapshot_metadata(&base.snapshot)
                            .await
                            .map_err(registration_attempt_error)?,
                    };
                    if self
                        .store_snapshot_blob_is_reclaimable(&snapshot, blob)
                        .await
                        .map_err(registration_attempt_error)?
                    {
                        Ok(())
                    } else {
                        Err(RegistrationLoadError::Invalid(
                            "Store blob reclaim is absent from the accepted orphan inventory"
                                .into(),
                        ))
                    }
                }
            }?;
        }
        if let Some(reference) = commit.reclaim_receipt() {
            let opened = self
                .load_reclaim_receipt(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let receipt = &opened.receipt.value;
            if receipt.executor != commit.author_registration
                || opened.executor != *activating_author
                || receipt.provider_admin_state != commit.membership_state
                || !predecessor_verifies_provider_administrator_grant(
                    predecessor,
                    &receipt.provider_admin_grant,
                    &receipt.executor,
                )
            {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim receipt signer is not the effective provider administrator at its exact predecessor"
                        .to_string(),
                ));
            }
            if !accepted
                .contains_reclaim_authorization(&receipt.authorization)
                .map_err(registration_attempt_error)?
            {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim receipt authorization is absent from predecessor history".to_string(),
                ));
            }
        }
        let has_join_attempt = commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(_)));
        if has_join_attempt && !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join attempt activation author is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        let activated_join_attempts = Self::validate_commit_join_activations(
            commit,
            activating_author,
            predecessor,
            accepted,
        )?;
        let has_join_abandonment = commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)));
        if has_join_abandonment {
            self.validate_commit_join_abandonments(commit, activating_author, predecessor)
                .await?;
        }
        let mut registrations = Vec::with_capacity(commit.device_registrations().len());
        for activated in commit.device_registrations() {
            let registration = Box::pin(self.load_registration(&activated.registration))
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let authority = Box::pin(self.registration_activation(
                activated,
                &registration,
                activating_author,
                predecessor,
                &activated_join_attempts,
                &recovery_memberships,
            ))
            .await?;
            let registration = ReferencedStoreDeviceRegistration::verified(
                activated.registration.clone(),
                registration,
            )
            .map_err(RegistrationLoadError::from)?;
            let registration = ActivatedStoreDeviceRegistration::verified(registration, authority)
                .map_err(RegistrationLoadError::from)?;
            registration
                .verify_reference(activated)
                .map_err(RegistrationLoadError::from)?;
            registrations.push(registration);
        }
        Ok(registrations)
    }
}
