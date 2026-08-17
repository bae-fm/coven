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
            let Some(predecessor_object) = current.successor.predecessor.as_ref() else {
                break;
            };
            let cached = self
                .verified_acknowledgements
                .lock()
                .expect("verified acknowledgement cache poisoned")
                .get(predecessor_object)
                .cloned();
            match cached {
                Some((predecessor_ref, predecessor)) => {
                    let expected_sequence =
                        current_ref.sequence.checked_sub(1).ok_or_else(|| {
                            RegistrationLoadError::Invalid(
                                "Store acknowledgement predecessor underflows sequence one"
                                    .to_string(),
                            )
                        })?;
                    if predecessor_ref.object != *predecessor_object
                        || predecessor_ref.registration != current_ref.registration
                        || predecessor_ref.sequence != expected_sequence
                        || predecessor.registration != predecessor_ref.registration
                        || predecessor.sequence != predecessor_ref.sequence
                    {
                        return Err(RegistrationLoadError::Invalid(
                            "cached Store acknowledgement differs from its successor".to_string(),
                        ));
                    }
                    current_ref = predecessor_ref;
                    current = predecessor;
                }
                None => {
                    let Some((predecessor_ref, predecessor)) = self
                        .load_store_ack_predecessor(&current_ref, &current, registration)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                    else {
                        return Err(RegistrationLoadError::Invalid(
                            "Store acknowledgement named a predecessor that was not loaded"
                                .to_string(),
                        ));
                    };
                    current_ref = predecessor_ref;
                    current = predecessor.value;
                }
            }
        }
        if chain.first_key_value().map(|(sequence, _)| *sequence) != Some(1)
            || chain.last_key_value().map(|(sequence, _)| *sequence) != Some(chain.len() as u64)
        {
            return Err(RegistrationLoadError::Invalid(
                "Store acknowledgement proof chain is not contiguous from sequence one".to_string(),
            ));
        }
        self.verified_acknowledgements
            .lock()
            .expect("verified acknowledgement cache poisoned")
            .extend(chain.values().map(|(reference, value)| {
                (reference.object.clone(), (reference.clone(), value.clone()))
            }));
        Ok(chain)
    }

    pub(crate) async fn load_head(
        &self,
        reference: &StoreDeviceHeadRef,
        registration: &StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StoreObjectError> {
        self.commit_verifier
            .load_head(reference, registration, commit)
            .await
    }

    pub(super) async fn load_state_registrations(
        &self,
        state: &ResolvedStoreDeviceState,
        registrations: &mut BTreeMap<
            store_commit::StoreDeviceId,
            ReferencedStoreDeviceRegistration,
        >,
    ) -> Result<(), StorePullError> {
        for (device_id, record) in &state.devices {
            if registrations
                .get(device_id)
                .is_some_and(|registration| registration.reference() == &record.registration)
            {
                continue;
            }
            let registration = self
                .commit_verifier
                .load_registration(&record.registration)
                .await?;
            if registration.value.device_id != *device_id {
                return Err(StorePullError::InvalidState(
                    "current Merge device state registration has another device id".to_string(),
                ));
            }
            registrations.insert(
                *device_id,
                ReferencedStoreDeviceRegistration::verified(
                    record.registration.clone(),
                    registration.value,
                )
                .map_err(StorePullError::Protocol)?,
            );
        }
        Ok(())
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
        self.commit_verifier.load_founder_registration().await
    }

    pub(crate) async fn load_snapshot_image(
        &self,
        snapshot: &coven_database::PublishedStoreSnapshot,
        progress: coven_storage::cloud::DownloadProgress,
    ) -> Result<Vec<u8>, StoreObjectError> {
        let context = ProtocolObjectContext::store_encrypted(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let semantic_prefix = coven_protocol::store_commit::snapshot_image_semantic_prefix(
            &snapshot.meta.author_registration.device_id.to_string(),
            snapshot.meta.image.image_hash,
        );
        self.commit_verifier
            .read_protocol_object_with_progress(
                &context,
                &snapshot.meta.image.object,
                &semantic_prefix,
                progress,
            )
            .await
    }

    pub(crate) async fn load_device_join_attempt_and_owner(
        &self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<
        (
            VerifiedObject<DeviceJoinAttempt>,
            VerifiedObject<StoreDeviceRegistration>,
        ),
        StoreObjectError,
    > {
        self.commit_verifier
            .load_device_join_attempt_and_owner(reference)
            .await
    }

    pub(crate) async fn load_device_join_outcome(
        &self,
        reference: &DeviceJoinOutcomeRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
        self.commit_verifier
            .load_device_join_outcome(reference, owner)
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
    ) -> Result<VerifiedObject<StoreAck>, StoreObjectError> {
        self.commit_verifier
            .load_store_ack(reference, registration)
            .await
    }

    pub(crate) async fn load_store_ack_predecessor(
        &self,
        successor_ref: &StoreAckRef,
        successor: &StoreAck,
        registration: &StoreDeviceRegistration,
    ) -> Result<Option<(StoreAckRef, VerifiedObject<StoreAck>)>, StoreObjectError> {
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
        self.commit_verifier
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(crate) async fn load_store_snapshot_stream(
        &self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        Vec<coven_database::PublishedStoreSnapshot>,
        crate::sync::store::snapshots::SnapshotError,
    > {
        self.commit_verifier
            .load_store_snapshot_stream(registration_ref, registration)
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

    pub(crate) async fn exact_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            coven_protocol::objects::ObjectSlot,
            Option<StoreDeviceHeadRef>,
        ),
        StoreError,
    > {
        self.commit_verifier
            .exact_next_announcement_slot(registration_ref, registration, previous)
            .await
    }

    pub(crate) async fn load_merge_commit_registrations(
        &self,
        commit: &StoreBatchCommit,
        author: &StoreDeviceRegistration,
        membership: &MembershipChain,
        accepted_frontier: &[StoreBatchCommitRef],
    ) -> Result<Vec<ActivatedStoreDeviceRegistration>, StorePullError> {
        let accepted =
            VerifiedMergePredecessorHistory::new(&self.history.commits, accepted_frontier);
        let loaded = self.load_commit_join_evidence(commit, author).await;
        let loaded = loaded.map_err(StorePullError::from)?;
        let join_evidence = accepted.verify_commit_join_evidence(commit, loaded)?;
        let registrations = self
            .load_commit_registrations(commit, author, Some(membership), &join_evidence, accepted)
            .await;
        registrations.map_err(StorePullError::from)
    }

    async fn load_commit_registrations(
        &self,
        commit: &StoreBatchCommit,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
        accepted: VerifiedMergePredecessorHistory<'_>,
    ) -> Result<Vec<ActivatedStoreDeviceRegistration>, RegistrationLoadError> {
        if join_evidence.commit != *commit {
            return Err(RegistrationLoadError::Invalid(
                "verified device-join evidence belongs to another Store commit".to_string(),
            ));
        }
        if commit.acknowledgement().is_some() {
            self.validate_commit_acknowledgement(commit, activating_author)
                .await?;
        }
        if let Some(reference) = commit.reclaim_authorization() {
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim authorization activation has no exact predecessor owner authority"
                        .to_string(),
                )
            })?;
            let opened = self
                .load_reclaim_authorization(reference)
                .await
                .map_err(RegistrationLoadError::Object)?;
            let evidence = &opened.evidence.value;
            let authorization = &opened.authorization.value;
            let owner_authorized = authorization.authority.membership == commit.membership_state
                && predecessor_verifies_owner(
                    predecessor,
                    &authorization.authority.membership,
                    &evidence.author_pubkey,
                    &authorization.authority.owner_grant,
                );
            if evidence.author_pubkey != activating_author.author_pubkey || !owner_authorized {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim authorization signer is not an active Owner at its exact predecessor"
                        .to_string(),
                ));
            }
            // Each kind of activating authority is re-read differently, so the binding
            // between the evidence and the object it authorizes deleting dispatches on
            // which authority published the target.
            let target = evidence.claim.target();
            match target.activation() {
                coven_protocol::reclaim::ReclaimActivation::Commit(activating_commit) => {
                    accepted.validate_commit_activated_reclaim_target(&target, activating_commit)
                }
                coven_protocol::reclaim::ReclaimActivation::CircleSnapshotMetadata(activation) => {
                    validate_circle_snapshot_activated_reclaim_target(&target, &activation)
                }
                coven_protocol::reclaim::ReclaimActivation::PackageBlobBinding(activation) => {
                    accepted.validate_package_bound_reclaim_target(&target, &activation)
                }
            }?;
        }
        if let Some(reference) = commit.reclaim_receipt() {
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "reclaim receipt activation has no exact predecessor provider authority"
                        .to_string(),
                )
            })?;
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
            if accepted
                .find(|_, candidate| {
                    candidate.reclaim_authorization() == Some(&receipt.authorization)
                })
                .map_err(registration_attempt_error)?
                .is_none()
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
        if has_join_attempt {
            validate_commit_join_attempts(commit, activating_author, predecessor, join_evidence)?;
        }
        let verified_join_outcomes = if commit.device_join_outcomes().is_empty() {
            BTreeMap::new()
        } else {
            Box::pin(self.validate_commit_join_outcomes(
                commit,
                activating_author,
                predecessor,
                join_evidence,
                accepted,
            ))
            .await?
        };
        let has_join_abandonment = commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| matches!(decision, DeviceJoinAttemptDecisionRef::Abandoned(_)));
        if has_join_abandonment {
            self.validate_commit_join_abandonments(commit, activating_author, predecessor)
                .await?;
        }
        if !commit.device_join_cleanup_receipts().is_empty() {
            accepted.validate_commit_join_cleanup_receipts(
                activating_author,
                predecessor,
                join_evidence,
            )?;
        }
        let mut registrations = Vec::with_capacity(commit.device_registrations().len());
        for activated in commit.device_registrations() {
            let registration = Box::pin(self.load_registration(&activated.registration))
                .await
                .map_err(RegistrationLoadError::Object)?
                .value;
            let predecessor = predecessor.ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "registration activation has no exact predecessor membership authority"
                        .to_string(),
                )
            })?;
            let authority = Box::pin(self.registration_activation(
                activated,
                &registration,
                activating_author,
                predecessor,
                &verified_join_outcomes,
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

    pub(crate) async fn load_activation_head(
        &mut self,
        verified_commit: &VerifiedStoreBatchCommit,
    ) -> Result<VerifiedObject<StoreDeviceHead>, StorePullError> {
        let author = verified_commit.author().clone();
        let commit = verified_commit.value();
        let (_, head_ref) = self
            .commit_verifier
            .exact_next_announcement_slot(
                &commit.author_registration,
                &author,
                Some(verified_commit),
            )
            .await
            .map_err(|error| StorePullError::Store(Box::new(error)))?;
        let head_ref = head_ref.ok_or_else(|| {
            StorePullError::InvalidState(
                "device join activation has no exact accepted activation head".to_string(),
            )
        })?;
        Ok(self
            .commit_verifier
            .load_head(&head_ref, &author, verified_commit.reference())
            .await?)
    }
}
