use super::*;

pub(super) fn validate_access_pairs(
    objects: &[CandidateExclusiveObjectDomain],
    materials: &std::collections::BTreeMap<ExactObjectRef, CandidateObjectMaterial>,
) -> Result<(), RemoteObjectRecordError> {
    let mut index = 0;
    while index < objects.len() {
        let CandidateExclusiveObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference: leaf_ref,
        } = &objects[index]
        else {
            index += 1;
            continue;
        };
        let Some(CandidateExclusiveObjectDomain::CircleAccessEnvelope {
            family: envelope_family,
            circle_id: envelope_circle,
            reference: envelope_ref,
        }) = objects.get(index + 1)
        else {
            return Err(RemoteObjectRecordError::DomainMismatch);
        };
        if family != envelope_family
            || circle_id != envelope_circle
            || leaf_ref.owner_pubkey != envelope_ref.owner_pubkey
            || leaf_ref.recipient_slot != envelope_ref.recipient_slot
            || leaf_ref.leaf_id != envelope_ref.leaf_id
            || leaf_ref.leaf_hash != envelope_ref.leaf_hash
        {
            return Err(RemoteObjectRecordError::DomainMismatch);
        }
        let leaf_material = materials
            .get(&leaf_ref.object)
            .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
        let envelope_material = materials
            .get(&envelope_ref.object)
            .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
        let leaf: crate::circle_control::CircleAccessLeaf =
            serde_json::from_slice(&leaf_material.canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let envelope: crate::circle_control::AccessEnvelope =
            serde_json::from_slice(&envelope_material.canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        let leaf_bytes = serde_json::to_vec(&leaf)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
        if envelope.value_hash != ObjectHash::digest(&leaf_bytes)
            || ObjectHash::digest(&leaf_material.stored_bytes) != leaf_ref.leaf_hash
        {
            return Err(RemoteObjectRecordError::StoredReferenceMismatch);
        }
        let bootstrap = match &leaf.disposition {
            crate::circle_control::CircleAccessDisposition::Active { bootstrap, .. } => {
                bootstrap.as_ref()
            }
            crate::circle_control::CircleAccessDisposition::Inactive => None,
        };
        match bootstrap {
            Some(bootstrap) => {
                let Some(CandidateExclusiveObjectDomain::CircleBootstrapImage {
                    family: bootstrap_family,
                    circle_id: bootstrap_circle,
                    owner_pubkey,
                    epoch_id,
                    recipient_slot,
                    reference,
                }) = objects.get(index + 2)
                else {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                };
                let material = materials
                    .get(&reference.object)
                    .ok_or(RemoteObjectRecordError::CandidateObjectMissing)?;
                if bootstrap_family != family
                    || bootstrap_circle != circle_id
                    || owner_pubkey != &leaf.owner_pubkey
                    || *epoch_id != leaf.epoch_id
                    || recipient_slot != &leaf.recipient_slot
                    || reference != &bootstrap.image
                    || !bootstrap.verify_for_access(&leaf)
                    || !material.canonical_semantic_bytes.is_empty()
                    || reference.object.verify(&material.stored_bytes).is_err()
                {
                    return Err(RemoteObjectRecordError::StoredReferenceMismatch);
                }
                index += 3;
            }
            None => {
                if matches!(
                    objects.get(index + 2),
                    Some(CandidateExclusiveObjectDomain::CircleBootstrapImage { .. })
                ) {
                    return Err(RemoteObjectRecordError::DomainMismatch);
                }
                index += 2;
            }
        }
    }
    Ok(())
}

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
        CandidateExclusiveObjectDomain::MergeMembershipWrappedStoreKey { reference, .. } => {
            validate_retained_authority_identity(
                &RetainedAuthorityObjectRef {
                    domain: RetainedAuthorityObjectDomain::MergeMembershipWrappedStoreKey {
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
        CandidateExclusiveObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference,
        } => validate_circle_access_leaf_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        ),
        CandidateExclusiveObjectDomain::CircleAccessEnvelope {
            family,
            circle_id,
            reference,
        } => validate_circle_access_envelope_identity(
            *family,
            *circle_id,
            reference,
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
            owner_pubkey,
            epoch_id,
            recipient_slot,
            reference,
            ..
        } => {
            let expected_prefix = crate::store_commit::circle_bootstrap_image_semantic_prefix(
                *circle_id,
                identity.family,
                owner_pubkey,
                *epoch_id,
                recipient_slot,
                reference.image_hash,
            );
            if !canonical_semantic_bytes.is_empty()
                || reference.object != identity.object
                || reference.object.slot().logical_key() != format!("{expected_prefix}.db")
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
    let package = crate::audience_package::AudiencePackage::parse(canonical_semantic_bytes)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
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

pub(super) fn validate_circle_access_leaf_identity(
    family: CandidateFamilyId,
    circle_id: CircleId,
    reference: &crate::store_commit::CircleAccessLeafObjectRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let leaf: crate::circle_control::CircleAccessLeaf =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let parsed_bytes = serde_json::to_vec(&leaf)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    if parsed_bytes != canonical_semantic_bytes
        || !leaf.verify_signature()
        || leaf.candidate_family != family
        || leaf.circle_id != circle_id
        || leaf.owner_pubkey != reference.owner_pubkey
        || leaf.epoch_id != reference.epoch_id
        || leaf.recipient_slot != reference.recipient_slot
        || leaf.leaf_id != reference.leaf_id
        || reference.leaf_hash != reference.object.stored_hash()
        || reference.object != *object
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

pub(super) fn validate_circle_access_envelope_identity(
    family: CandidateFamilyId,
    circle_id: CircleId,
    reference: &crate::store_commit::CircleAccessEnvelopeObjectRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let envelope: crate::circle_control::AccessEnvelope =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let parsed_bytes = serde_json::to_vec(&envelope)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    if parsed_bytes != canonical_semantic_bytes
        || envelope.verify_by(&envelope.owner_pubkey).is_err()
        || envelope.candidate_family != family
        || envelope.circle_id != circle_id
        || envelope.owner_pubkey != reference.owner_pubkey
        || envelope.recipient_slot != reference.recipient_slot
        || envelope.control_hash != reference.control_hash
        || envelope.leaf_id != reference.leaf_id
        || envelope.leaf_hash != reference.leaf_hash
        || reference.object != *object
    {
        return Err(RemoteObjectRecordError::StoredReferenceMismatch);
    }
    Ok(())
}

pub(super) fn validate_circle_epoch_close_intent_identity(
    circle_id: CircleId,
    reference: &crate::circle_control::CircleEpochCloseIntentRef,
    canonical_semantic_bytes: &[u8],
    object: &ExactObjectRef,
) -> Result<(), RemoteObjectRecordError> {
    let intent: crate::circle_control::CircleEpochCloseIntent =
        serde_json::from_slice(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
    let parsed_bytes = serde_json::to_vec(&intent)
        .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
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
        crate::circle_control::CircleEpochCloseSlotValue::parse(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?
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
        crate::circle_control::CircleEpochCloseSlotValue::parse(canonical_semantic_bytes)
            .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?
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
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_commit(&commit)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceHead { reference } => {
            let head: crate::store_commit::StoreDeviceHead =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if head.head_hash() != reference.head_hash || reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::Acknowledgement { reference } => {
            let acknowledgement: crate::store_commit::StoreAck =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
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
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if acknowledgement.registration != reference.registration
                || acknowledgement.circle_id != reference.circle_id
                || acknowledgement.sequence != reference.sequence
                || acknowledgement.ack_hash() != reference.ack_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipWrappedStoreKey { reference } => {
            let wrapped: crate::wrapped_store_key::WrappedStoreKey =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .validate_value(&wrapped, canonical_semantic_bytes)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::StoreMembershipResolution { reference } => {
            let resolution: crate::membership::StoreMembershipConflictResolution =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            let expected_key = format!(
                "{}.json",
                crate::store_commit::membership_resolution_semantic_prefix(
                    reference.conflict_hash,
                    &reference.resolver_pubkey,
                    reference.resolution_hash,
                )
            );
            if resolution.conflict_hash != reference.conflict_hash
                || resolution.resolver_pubkey != reference.resolver_pubkey
                || resolution.resolution_hash() != reference.resolution_hash
                || reference.object != identity.object
                || reference.object.slot().logical_key() != expected_key
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipEntry { reference } => {
            let entry: crate::membership::MembershipEntry =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if entry.coord() != reference.coord || reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::MergeMembershipHead { reference } => {
            let head: crate::membership::AuthorHead =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if head.entry_coord() != reference.coord
                || head.head_hash() != reference.head_hash
                || reference.object != identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceExclusionProposal { reference } => {
            let proposal: crate::store_commit::StoreDeviceExclusionProposal =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_proposal(&proposal)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::DeviceExclusionOutcome { reference } => {
            let outcome: crate::store_commit::StoreDeviceExclusionOutcome =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if outcome.proposal() != reference.proposal()
                || outcome.outcome_hash() != reference.outcome_hash()
                || reference.object() != &identity.object
            {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimEvidence { reference } => {
            let value: crate::reclaim::ReclaimEvidence =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimAuthorization { reference } => {
            let value: crate::reclaim::ReclaimAuthorization =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_identity(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::ReclaimReceipt { reference } => {
            let value: crate::reclaim::ReclaimReceipt =
                serde_json::from_slice(canonical_semantic_bytes)
                    .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            reference
                .verify_identity(&value)
                .map_err(|error| RemoteObjectRecordError::InvalidDomain(error.to_string()))?;
            if reference.object != identity.object {
                return Err(RemoteObjectRecordError::StoredReferenceMismatch);
            }
        }
        RetainedAuthorityObjectDomain::CircleAccessLeaf {
            family,
            circle_id,
            reference,
        } => validate_circle_access_leaf_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
        RetainedAuthorityObjectDomain::CircleAccessEnvelope {
            family,
            circle_id,
            reference,
        } => validate_circle_access_envelope_identity(
            *family,
            *circle_id,
            reference,
            canonical_semantic_bytes,
            &identity.object,
        )?,
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
