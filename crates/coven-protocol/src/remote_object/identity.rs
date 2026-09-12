use super::*;

pub(super) fn validate_candidate_exclusive_identity(
    identity: &CandidateExclusiveTarget,
    canonical_semantic_bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    identity.validate_semantic(canonical_semantic_bytes)?;
    if identity.family != identity.domain.family() || identity.object != *identity.domain.object() {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    match &identity.domain {
        CandidateExclusiveObjectDomain::MergeMembershipEntry { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MergeMembershipEntry {
                        reference: reference.clone(),
                    },
                    semantic_hash: identity.semantic_hash,
                    object: identity.object.clone(),
                },
                canonical_semantic_bytes,
            )
        }
        CandidateExclusiveObjectDomain::MergeMembershipHead { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MergeMembershipHead {
                        reference: reference.clone(),
                    },
                    semantic_hash: identity.semantic_hash,
                    object: identity.object.clone(),
                },
                canonical_semantic_bytes,
            )
        }
        CandidateExclusiveObjectDomain::StorePackage { reference } => {
            validate_package_reference(reference, None, canonical_semantic_bytes, &identity.object)
        }
        CandidateExclusiveObjectDomain::CirclePackage { reference } => validate_package_reference(
            &reference.package,
            Some(reference),
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleEpochCloseIntent {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_intent_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleEpochCloseOutcome {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_outcome_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleEpochCloseCancellation {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_cancellation_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleBootstrapImage {
            circle_id,
            reference,
            ..
        } => {
            let expected_prefix = crate::store_commit::circle_bootstrap_image_semantic_prefix(
                *circle_id,
                identity.family,
                &reference.owner_pubkey,
                reference.epoch_id,
                &reference.recipient_slot,
                reference.image.image_hash,
            );
            if !canonical_semantic_bytes.is_empty()
                || reference.image.object != identity.object
                || reference.image.object.slot().logical_key() != format!("{expected_prefix}.db")
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
            Ok(())
        }
    }
}

pub(super) fn validate_package_reference(
    reference: &crate::store_commit::StorePackageRef,
    circle: Option<&crate::store_commit::CirclePackageRef>,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let package = crate::audience_package::AudiencePackage::parse(canonical_semantic_bytes)?;
    let size = u64::try_from(canonical_semantic_bytes.len())
        .map_err(|_| RemoteObjectRecordError::DomainMismatch)?;
    if reference.object != *object
        || reference.content_hash != ObjectHash::digest(canonical_semantic_bytes)
        || reference.schema_version != package.schema_version()
        || reference.changeset_size != size
        || reference.candidate_family != package.candidate_family()
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    match (circle, package.audience()) {
        (None, crate::audience_package::PackageAudience::Store) => Ok(()),
        (
            Some(reference),
            crate::audience_package::PackageAudience::Circle {
                circle_id,
                control,
                key_fingerprint,
            },
        ) if reference.circle_id == *circle_id
            && reference.control == *control
            && reference.key_fingerprint == *key_fingerprint =>
        {
            Ok(())
        }
        _ => Err(RemoteObjectRecordError::DomainMismatch),
    }
}

pub(super) fn validate_circle_epoch_close_intent_identity(
    circle_id: CircleId,
    reference: &crate::circle_control::CircleEpochCloseIntentRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let intent: crate::circle_control::CircleEpochCloseIntent =
        serde_json::from_slice(canonical_semantic_bytes)?;
    let parsed_bytes = serde_json::to_vec(&intent)?;
    let expected = format!(
        "{}.json",
        crate::circle_control::circle_epoch_close_intent_semantic_prefix(
            circle_id,
            reference.close_id,
            reference.intent_hash,
        )
    );
    if parsed_bytes != canonical_semantic_bytes
        || !intent.verify()
        || intent.circle_id != circle_id
        || intent.close_id != reference.close_id
        || intent.intent_hash() != reference.intent_hash
        || reference.object != *object
        || reference.object.slot().logical_key() != expected
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

pub(super) fn validate_circle_epoch_close_outcome_identity(
    circle_id: CircleId,
    reference: &crate::circle_control::CircleEpochCloseOutcomeRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let crate::circle_control::CircleEpochCloseSlotValue::Outcome(outcome) =
        crate::circle_control::CircleEpochCloseSlotValue::parse(canonical_semantic_bytes)?
    else {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    };
    let expected = format!(
        "{}.json",
        crate::circle_control::circle_epoch_close_outcome_semantic_prefix(
            circle_id,
            reference.close_id,
        )
    );
    if !outcome.verify_signature()
        || outcome.circle_id != circle_id
        || outcome.close_id != reference.close_id
        || outcome.outcome_hash() != reference.outcome_hash
        || reference.object != *object
        || reference.object.slot().logical_key() != expected
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

pub(super) fn validate_circle_epoch_close_cancellation_identity(
    circle_id: CircleId,
    reference: &crate::circle_control::CircleEpochCloseCancellationRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let crate::circle_control::CircleEpochCloseSlotValue::Cancellation(cancellation) =
        crate::circle_control::CircleEpochCloseSlotValue::parse(canonical_semantic_bytes)?
    else {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    };
    let expected = format!(
        "{}.json",
        crate::circle_control::circle_epoch_close_outcome_semantic_prefix(
            circle_id,
            reference.close_id,
        )
    );
    if !cancellation.verify_signature()
        || cancellation.circle_id != circle_id
        || cancellation.close_id != reference.close_id
        || cancellation.cancellation_hash() != reference.cancellation_hash
        || reference.object != *object
        || reference.object.slot().logical_key() != expected
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

pub(super) fn validate_retained_authority_identity(
    identity: &RetainedAuthorityObjectRef,
    canonical_semantic_bytes: &[u8],
) -> Result<(), RemoteObjectRecordError> {
    identity.validate_semantic(canonical_semantic_bytes)?;
    match &identity.domain {
        RetainedAuthorityObjectDomain::Commit { reference } => {
            let commit: crate::store_commit::StoreBatchCommit =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference.verify_commit(&commit)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::Acknowledgement { reference } => {
            let acknowledgement: crate::store_commit::StoreAck =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if acknowledgement.registration != reference.registration
                || acknowledgement.sequence != reference.sequence
                || acknowledgement.ack_hash() != reference.ack_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::CircleAcknowledgement { reference } => {
            let acknowledgement: crate::store_commit::CircleAck =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if acknowledgement.registration != reference.registration
                || acknowledgement.circle_id != reference.circle_id
                || acknowledgement.sequence != reference.sequence
                || acknowledgement.ack_hash() != reference.ack_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ProviderAccessGrant { reference } => {
            let grant: crate::provider::StoreMemberProviderAccessGrant =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference
                .verify(&grant)
                .map_err(|_| RemoteObjectRecordError::StoredReferenceMismatch)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceJoinAbandonment { reference } => {
            let abandonment: crate::store_commit::device_join_exchange::DeviceJoinAbandonmentObject =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if reference.attempt_id != abandonment.attempt_id
                || reference.abandonment_hash != abandonment.abandonment_hash()
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceRegistration { reference } => {
            let registration: crate::store_commit::StoreDeviceRegistration =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference.verify_registration(&registration)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipEntry { reference } => {
            let entry: crate::membership::MembershipEntry =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if entry.coord() != reference.coord || reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipHead { reference } => {
            let head: crate::membership::AuthorHead =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if head.entry_coord() != reference.coord
                || head.head_hash() != reference.head_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::OwnerPromotionRequestPublication {
            promotion_id,
            activation,
        } => {
            let value: crate::store_commit::OwnerPromotionRequestPublication =
                serde_json::from_slice(canonical_semantic_bytes)?;
            value.require_version()?;
            value.publication.validate_slot()?;
            let expected = format!(
                "{}.json",
                crate::store_commit::owner_promotion_request_publication_semantic_prefix(
                    *promotion_id
                ),
            );
            if value.body() != activation
                || value.to_bytes() != canonical_semantic_bytes
                || identity.object.slot().logical_key() != expected
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
            identity.object.verify(canonical_semantic_bytes)?;
        }
        RetainedAuthorityObjectDomain::MembershipHeadAcceptance { head, publication } => {
            let value: crate::membership::MembershipHeadAcceptance =
                serde_json::from_slice(canonical_semantic_bytes)?;
            let expected = format!(
                "{}.json",
                crate::membership::membership_head_acceptance_semantic_prefix(&head.coord),
            );
            if value.head != *head
                || value.publication()? != publication
                || value.store_root_hash != publication.store_root_hash
                || identity.object.slot().logical_key() != expected
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
            identity.object.verify(canonical_semantic_bytes)?;
        }
        RetainedAuthorityObjectDomain::DeviceExclusionProposal { reference } => {
            let proposal: crate::store_commit::StoreDeviceExclusionProposal =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference.verify_proposal(&proposal)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceExclusionOutcome { reference } => {
            let outcome: crate::store_commit::StoreDeviceExclusionOutcome =
                serde_json::from_slice(canonical_semantic_bytes)?;
            if outcome.proposal() != reference.proposal()
                || outcome.outcome_hash() != reference.outcome_hash()
                || reference.object() != &identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimEvidence { reference } => {
            let value: crate::reclaim::ReclaimEvidence =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference.verify(&value)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimAuthorization { reference } => {
            let value: crate::reclaim::ReclaimAuthorization =
                serde_json::from_slice(canonical_semantic_bytes)?;
            reference.verify_identity(&value)?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::CircleEpochCloseIntent {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_intent_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
        RetainedAuthorityObjectDomain::CircleEpochCloseOutcome {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_outcome_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
        RetainedAuthorityObjectDomain::CircleEpochCloseCancellation {
            circle_id,
            reference,
            ..
        } => validate_circle_epoch_close_cancellation_identity(
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
    }
    Ok(())
}
