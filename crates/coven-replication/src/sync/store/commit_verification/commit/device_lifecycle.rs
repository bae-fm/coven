use super::*;

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn load_device_exclusion_outcome(
        &self,
        reference: &StoreDeviceExclusionOutcomeRef,
        proposal: &StoreDeviceExclusionProposal,
        target: &StoreDeviceRegistration,
    ) -> Result<VerifiedDeviceExclusionOutcome, StoreObjectError> {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceExclusionOutcome,
        );
        let semantic_prefix = device_exclusion_outcome_semantic_prefix(
            proposal.target.device_id,
            proposal.proposal_id,
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
            proposal,
            target,
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
