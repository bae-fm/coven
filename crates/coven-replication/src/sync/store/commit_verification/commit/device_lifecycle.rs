use super::*;

impl<'a> StoreCommitVerifier<'a> {
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
}
