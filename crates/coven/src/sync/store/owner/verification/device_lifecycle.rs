use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn load_owner_signed_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinAttempt>, StoreObjectError> {
        let (bytes, semantic_prefix) = self.read_device_join_attempt(reference).await?;
        self.verify_device_join_attempt(reference, owner, bytes, semantic_prefix)
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
        let (bytes, semantic_prefix) = self.read_device_join_attempt(reference).await?;
        let unverified: DeviceJoinAttempt =
            decode_protocol_object(&bytes).map_err(|source| StoreObjectError::InvalidObject {
                semantic_prefix: semantic_prefix.clone(),
                key: reference.object.slot().logical_key().to_string(),
                source: Box::new(source),
            })?;
        let owner = self
            .load_registration(&unverified.owner_registration)
            .await?;
        let attempt = self
            .verify_device_join_attempt(reference, &owner.value, bytes, semantic_prefix)
            .await?;
        Ok((attempt, owner))
    }

    pub(crate) async fn read_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
    ) -> Result<(Vec<u8>, String), StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::DeviceJoinAttempt,
        );
        let semantic_prefix = device_join_attempt_semantic_prefix(reference.attempt_id);
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await?;
        Ok((bytes, semantic_prefix))
    }

    pub(crate) async fn verify_device_join_attempt(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
        bytes: Vec<u8>,
        semantic_prefix: String,
    ) -> Result<VerifiedObject<DeviceJoinAttempt>, StoreObjectError> {
        let expected = reference.clone();
        let owner = owner.clone();
        let verify_bytes = bytes.clone();
        let value = run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || DeviceJoinAttempt::parse_at(&verify_bytes, &expected, &owner)),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.attempt_hash,
            object: reference.object.clone(),
        })
    }

    pub(crate) async fn load_device_join_outcome(
        &self,
        reference: &DeviceJoinOutcomeRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<VerifiedObject<DeviceJoinOutcome>, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::DeviceJoinOutcome,
        );
        let semantic_prefix = device_join_outcome_semantic_prefix(reference.attempt().attempt_id);
        let expected_hash = match reference {
            DeviceJoinOutcomeRef::Activated { outcome_hash, .. }
            | DeviceJoinOutcomeRef::Cancelled { outcome_hash, .. } => *outcome_hash,
        };
        let expected = reference.clone();
        let expected_owner = owner.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
        self.load_exact_object(
            &context,
            reference.object(),
            &semantic_prefix,
            expected_hash,
            move |bytes| {
                let outcome: DeviceJoinOutcome = decode_protocol_object(bytes)?;
                verify_store_root(expected_store_root_hash, outcome.store_root_hash)?;
                outcome
                    .owner_registration
                    .verify_registration(&expected_owner)?;
                outcome.verify_by(&expected_owner.device_signing_pubkey)?;
                expected.verify_outcome(&outcome)?;
                Ok(outcome)
            },
        )
        .await
    }

    pub(crate) async fn load_device_exclusion_proposal(
        &self,
        reference: &StoreDeviceExclusionProposalRef,
    ) -> Result<VerifiedDeviceExclusionProposal, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionProposal,
        );
        let semantic_prefix = device_exclusion_proposal_semantic_prefix(
            reference.target.device_id,
            reference.proposal_id,
            reference.proposal_hash,
        );
        let expected = reference.clone();
        let expected_store_root_hash = self.root.reference().store_root_hash;
        let opened = self
            .load_exact_object(
                &context,
                &reference.object,
                &semantic_prefix,
                reference.proposal_hash,
                move |bytes| {
                    let proposal: StoreDeviceExclusionProposal = decode_protocol_object(bytes)?;
                    expected.verify_proposal(&proposal)?;
                    verify_store_root(expected_store_root_hash, proposal.store_root_hash)?;
                    Ok(proposal)
                },
            )
            .await?;
        let target = self.load_registration(&opened.value.target).await?.value;
        let owner = self
            .load_registration(&opened.value.owner_registration)
            .await?
            .value;
        let verified =
            StoreDeviceExclusionProposal::parse_at(&opened.bytes, reference, &target, &owner)
                .map_err(|source| StoreObjectError::InvalidObject {
                    semantic_prefix,
                    key: reference.object.slot().logical_key().to_string(),
                    source: Box::new(source),
                })?;
        Ok(VerifiedDeviceExclusionProposal {
            reference: reference.clone(),
            object: VerifiedObject {
                value: verified,
                ..opened
            },
            target,
            owner,
        })
    }

    pub(crate) async fn load_device_exclusion_outcome(
        &self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &VerifiedDeviceExclusionProposal,
    ) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let semantic_prefix = device_exclusion_outcome_semantic_prefix(
            proposal.object.value.target.device_id,
            proposal.object.value.proposal_id,
        );
        let expected = reference.clone();
        let opened = self
            .load_exact_object(
                &context,
                reference.object(),
                &semantic_prefix,
                reference.outcome_hash(),
                move |bytes| {
                    let outcome: StoreDeviceExclusionOutcome = decode_protocol_object(bytes)?;
                    if outcome.outcome_hash() != expected.outcome_hash()
                        || outcome.proposal() != expected.proposal()
                    {
                        return Err(StoreProtocolError::DeviceStateMismatch);
                    }
                    Ok(outcome)
                },
            )
            .await?;
        let owner_ref = match &opened.value {
            StoreDeviceExclusionOutcome::Excluded(exclusion) => &exclusion.owner_registration,
            StoreDeviceExclusionOutcome::Cancelled(cancellation) => {
                &cancellation.owner_registration
            }
        };
        let owner = self.load_registration(owner_ref).await?.value;
        let verified = StoreDeviceExclusionOutcome::parse_at(
            &opened.bytes,
            reference,
            &proposal.object.value,
            &proposal.target,
            &owner,
        )
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix,
            key: reference.object().slot().logical_key().to_string(),
            source: Box::new(source),
        })?;
        Ok(VerifiedDeviceExclusionOutcome {
            object: VerifiedObject {
                value: verified,
                ..opened
            },
            owner,
        })
    }

    pub(crate) async fn load_device_join_attempt_evidence(
        &self,
        reference: &DeviceJoinAttemptRef,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        let attempt = self
            .load_owner_signed_device_join_attempt(reference, owner)
            .await?;
        self.validate_device_join_attempt_evidence(attempt, owner)
            .await
    }

    pub(crate) async fn validate_device_join_attempt_evidence(
        &self,
        attempt: VerifiedObject<DeviceJoinAttempt>,
        owner: &StoreDeviceRegistration,
    ) -> Result<LoadedDeviceJoinAttemptEvidence, StorePullError> {
        if attempt.value.store_root != self.root.reference().clone() {
            return Err(StorePullError::Database(
                "device join attempt names another Store root".to_string(),
            ));
        }
        let offer = &attempt.value.provider_approval.request.offer;
        let administrator = self
            .load_registration(&offer.provider_admin.administrator)
            .await?
            .value;
        attempt
            .value
            .provider_approval
            .verify(self.root.object(), owner, &administrator)
            .map_err(|error| StorePullError::Database(error.to_string()))?;
        Ok(LoadedDeviceJoinAttemptEvidence { attempt })
    }
}
