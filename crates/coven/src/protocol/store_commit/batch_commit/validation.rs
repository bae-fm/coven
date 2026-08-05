use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn validate_commit_envelope(
    store_root_hash: ObjectHash,
    coord: &StoreCommitCoord,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    order: &StoreCommitOrder,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
    membership_authority: Option<&MembershipGrantCreationAuthority>,
    signer: &UserKeypair,
) -> Result<(), StoreProtocolError> {
    author_registration.verify_registration(author)?;
    if keys::public_key_hex(signer) != author.device_signing_pubkey {
        return Err(StoreProtocolError::InvalidSignature);
    }
    if order.seq() == 0 {
        return Err(StoreProtocolError::InvalidSequence(0));
    }
    validate_commit_order(order)?;
    validate_commit_predecessor_states(order, membership_state, device_state)?;
    if coord.sequence() != order.seq() {
        return Err(StoreProtocolError::Malformed(
            "Store commit coordinate disagrees with its order".to_string(),
        ));
    }
    if let Some(authority) = membership_authority {
        validate_membership_authority(authority)?;
    }
    crate::protocol::objects::verify_store_root(
        store_root_hash,
        author.store_root.store_root_hash,
    )?;
    Ok(())
}

pub(super) fn validate_commit_body(
    store_root_hash: ObjectHash,
    body: &StoreCommitBody,
    author: &StoreDeviceRegistrationRef,
) -> Result<(), StoreProtocolError> {
    match body {
        StoreCommitBody::Operations(operations) => {
            if operations.is_empty() {
                return Err(StoreProtocolError::EmptyBatch);
            }
            validate_circle_control_refs(&operations.circle_controls)?;
            validate_commit_acknowledgement(&operations.acknowledgement, author)?;
            validate_commit_circle_acknowledgements(&operations.circle_acknowledgements, author)?;
            validate_device_join_attempt_decision_refs(&operations.device_join_attempt_decisions)?;
            validate_device_join_outcome_refs(&operations.device_join_outcomes)?;
            validate_device_join_cleanup_receipt_refs(&operations.device_join_cleanup_receipts)?;
            validate_provider_access_refs(&operations.provider_access_grants)?;
            validate_device_registration_refs(&operations.device_registrations)?;
            validate_device_exclusion_refs(
                &operations.device_exclusion_proposals,
                &operations.device_exclusion_outcomes,
            )?;
            validate_stream_activations(
                store_root_hash,
                author,
                operations.control.as_ref(),
                &operations.stream_activations,
            )?;
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::OwnerPromotionRequest { request } => {
            if request.store_root_hash != store_root_hash
                || request.promoter_registration != *author
            {
                return Err(StoreProtocolError::OwnerPromotionMismatch);
            }
        }
        StoreCommitBody::AbandonCandidates { manifests } => {
            if manifests.is_empty() {
                return Err(StoreProtocolError::Malformed(
                    "candidate abandonment has no candidates".to_string(),
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_owner_promotion_request_for_commit(
    request: &OwnerPromotionRequest,
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    author: &StoreDeviceRegistration,
    membership_state: &StoreMembershipStateRef,
    device_state: &StoreDeviceStateRef,
) -> Result<(), StoreProtocolError> {
    request.verify(&author.store_root, author)?;
    if request.store_root_hash != store_root_hash
        || request.promoter_registration != *author_registration
        || request.predecessor_membership != *membership_state
        || request.predecessor_devices != *device_state
    {
        return Err(StoreProtocolError::OwnerPromotionMismatch);
    }
    Ok(())
}

pub(crate) fn validate_stream_activations(
    store_root_hash: ObjectHash,
    author: &StoreDeviceRegistrationRef,
    control: Option<&StoreControl>,
    activations: &[StreamActivation],
) -> Result<(), StoreProtocolError> {
    if activations.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "stream activations are not strictly sorted and unique".to_string(),
        ));
    }
    let mut activation_ids = BTreeSet::new();
    let mut stream_ids = BTreeSet::new();
    let mut first_slots = BTreeSet::new();
    for activation in activations {
        crate::protocol::objects::verify_store_root(store_root_hash, activation.store_root_hash())?;
        let owner_promotion = control.is_some();
        if activation.author_registration() != author && !owner_promotion {
            return Err(StoreProtocolError::Malformed(
                "stream activation registration differs from its commit author".to_string(),
            ));
        }
        let allowed_anchor = matches!(
            (control, activation),
            (
                Some(StoreControl { .. }),
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::StoreMembership { .. }
                        | GrantStreamAnchor::OwnerRecovery { .. },
                    ..
                }
            ) | (
                _,
                StreamActivation::GrantAuthorized {
                    anchor: GrantStreamAnchor::CircleControl { .. }
                        | GrantStreamAnchor::CircleRoster { .. }
                        | GrantStreamAnchor::CircleMetadata { .. },
                    ..
                }
            )
        );
        if !allowed_anchor {
            return Err(StoreProtocolError::Malformed(
                "Store commit contains a root- or registration-authorized stream anchor"
                    .to_string(),
            ));
        }
        if !activation_ids.insert(activation.activation_id())
            || !stream_ids.insert(activation.author_stream_id())
            || !first_slots.insert(activation.first_slot().clone())
        {
            return Err(StoreProtocolError::Malformed(
                "stream activations repeat an activation, author stream, or first slot".to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_candidate_abandonment(
    manifests: &[CandidateCleanupManifest],
    store_root_hash: ObjectHash,
    author_registration: &StoreDeviceRegistrationRef,
    coord: &StoreCommitCoord,
    order: &StoreCommitOrder,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    if manifests.is_empty() {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment has no candidates".to_string(),
        ));
    }
    if manifests.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(StoreProtocolError::Malformed(
            "candidate abandonment manifests are not strictly sorted and unique".to_string(),
        ));
    }
    for manifest in manifests {
        if &manifest.candidate.coord != coord {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate occupies a different competition point".to_string(),
            ));
        }
        let candidate = manifest
            .candidate
            .verify_candidate(store_root_hash, author)?;
        if &candidate.author_registration != author_registration {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different author registration".to_string(),
            ));
        }
        let shares_predecessor = candidate.order.predecessor == order.predecessor;
        if !shares_predecessor {
            return Err(StoreProtocolError::Malformed(
                "abandoned candidate has a different predecessor".to_string(),
            ));
        }
    }
    Ok(())
}

pub(crate) fn candidate_manifest(
    family: CandidateFamilyId,
    body: &StoreCommitBody,
) -> Result<CandidateObjectManifest, StoreProtocolError> {
    let mut objects = Vec::new();
    match body {
        StoreCommitBody::Operations(operations) => {
            objects.extend(
                operations
                    .store_package
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::StorePackage),
            );
            objects.extend(
                operations
                    .circle_packages
                    .iter()
                    .cloned()
                    .map(CandidateExclusiveObjectRef::CirclePackage),
            );
            for control in &operations.circle_controls {
                let circle_id = control.circle_id();
                if let Some(reference) = &control.objects().close_intent {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseIntent {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if let Some(reference) = &control.objects().close_outcome {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseOutcome {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if let Some(reference) = &control.objects().close_cancellation {
                    objects.push(CandidateExclusiveObjectRef::CircleEpochCloseCancellation {
                        circle_id,
                        reference: reference.clone(),
                    });
                }
                if control
                    .objects()
                    .access
                    .iter()
                    .any(|access| access.envelope.control_hash != control.control().control_hash())
                {
                    return Err(StoreProtocolError::Malformed(
                        "Circle access envelope differs from its activating control".to_string(),
                    ));
                }
                objects.extend(
                    control.objects().access.iter().cloned().map(|access| {
                        CandidateExclusiveObjectRef::CircleAccess { circle_id, access }
                    }),
                );
            }
        }
        StoreCommitBody::ReclaimAuthorization { .. } => {}
        StoreCommitBody::ReclaimReceipt { .. } => {}
        StoreCommitBody::OwnerPromotionRequest { .. }
        | StoreCommitBody::AbandonCandidates { .. } => {}
    }
    objects.sort_by_cached_key(|object| {
        serde_json::to_vec(object).expect("candidate object serialization cannot fail")
    });
    let mut exact_refs = BTreeSet::new();
    let mut access_keys = BTreeSet::new();
    for object in &objects {
        validate_candidate_object_path(family, object)?;
        match object {
            CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
                let key = (
                    *circle_id,
                    access.leaf.owner_pubkey.clone(),
                    access.leaf.recipient_slot.clone(),
                    access.envelope.control_hash,
                );
                if !access_keys.insert(key) {
                    return Err(StoreProtocolError::Malformed(
                        "candidate object manifest repeats a Circle access semantic key"
                            .to_string(),
                    ));
                }
                insert_candidate_exact_ref(&mut exact_refs, &access.leaf.object)?;
                insert_candidate_exact_ref(&mut exact_refs, &access.envelope.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseIntent { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseOutcome { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CircleEpochCloseCancellation { reference, .. } => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::StorePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.object)?;
            }
            CandidateExclusiveObjectRef::CirclePackage(reference) => {
                insert_candidate_exact_ref(&mut exact_refs, &reference.package.object)?;
            }
        }
    }
    Ok(CandidateObjectManifest { family, objects })
}

pub(super) fn insert_candidate_exact_ref<'a>(
    exact_refs: &mut BTreeSet<&'a ExactObjectRef>,
    object: &'a ExactObjectRef,
) -> Result<(), StoreProtocolError> {
    if !exact_refs.insert(object) {
        return Err(StoreProtocolError::Malformed(
            "candidate object manifest repeats an exact object reference".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_candidate_object_path(
    family: CandidateFamilyId,
    candidate: &CandidateExclusiveObjectRef,
) -> Result<(), StoreProtocolError> {
    match candidate {
        CandidateExclusiveObjectRef::StorePackage(reference) => {
            if reference.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its manifest".to_string(),
                ));
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CirclePackage(reference) => {
            if reference.package.candidate_family != family {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its manifest".to_string(),
                ));
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleAccess { circle_id, access } => {
            validate_circle_access_ref(*circle_id, family, access)?;
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseIntent {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_intent_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                    reference.intent_hash,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseOutcome {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
        CandidateExclusiveObjectRef::CircleEpochCloseCancellation {
            circle_id,
            reference,
        } => {
            let expected = format!(
                "{}.json",
                crate::protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                    *circle_id,
                    reference.close_id,
                )
            );
            if reference.object.slot().logical_key() != expected {
                return Err(StoreProtocolError::RelocatedCandidateObject {
                    expected,
                    actual: reference.object.slot().logical_key().to_string(),
                });
            }
            Ok(())
        }
    }
}

pub(super) fn validate_circle_access_ref(
    circle_id: CircleId,
    family: CandidateFamilyId,
    access: &CircleAccessObjectRef,
) -> Result<(), StoreProtocolError> {
    if access.leaf.owner_pubkey != access.envelope.owner_pubkey
        || access.leaf.recipient_slot != access.envelope.recipient_slot
        || access.leaf.leaf_id != access.envelope.leaf_id
        || access.leaf.leaf_hash != access.envelope.leaf_hash
        || access.leaf.leaf_hash != access.leaf.object.stored_hash()
    {
        return Err(StoreProtocolError::Malformed(
            "paired Circle access leaf and envelope references differ".to_string(),
        ));
    }
    let leaf_expected = circle_access_leaf_semantic_prefix(
        circle_id,
        family,
        &access.leaf.owner_pubkey,
        access.leaf.epoch_id,
        &access.leaf.recipient_slot,
        access.leaf.leaf_id,
    );
    if access.leaf.object.slot().logical_key() != leaf_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: leaf_expected,
            actual: access.leaf.object.slot().logical_key().to_string(),
        });
    }
    let envelope_expected = format!(
        "{}.json",
        circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &access.envelope.owner_pubkey,
            &access.envelope.recipient_slot,
            access.envelope.control_hash,
        )
    );
    if access.envelope.object.slot().logical_key() != envelope_expected {
        return Err(StoreProtocolError::RelocatedCandidateObject {
            expected: envelope_expected,
            actual: access.envelope.object.slot().logical_key().to_string(),
        });
    }
    Ok(())
}

pub(super) fn package_ref(
    semantic_prefix: &str,
    input: &StorePackageInput<'_>,
) -> Result<StorePackageRef, StoreProtocolError> {
    let package_bytes = input.bytes;
    let changeset_size =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    let content_hash = ObjectHash::digest(package_bytes);
    let expected_key = format!("{semantic_prefix}.pkg");
    if input.object.slot().logical_key() != expected_key {
        return Err(StoreProtocolError::RelocatedPackage {
            expected: expected_key,
            actual: input.object.slot().logical_key().to_string(),
        });
    }
    Ok(StorePackageRef {
        candidate_family: input.candidate_family,
        content_hash,
        schema_version: input.schema_version,
        changeset_size,
        object: input.object.clone(),
    })
}

pub(super) fn verify_package_ref(
    package: &StorePackageRef,
    package_bytes: &[u8],
) -> Result<(), StoreProtocolError> {
    let length =
        u64::try_from(package_bytes.len()).map_err(|_| StoreProtocolError::PackageTooLarge)?;
    if length != package.changeset_size {
        return Err(StoreProtocolError::PackageLengthMismatch {
            expected: package.changeset_size,
            actual: length,
        });
    }
    let actual = ObjectHash::digest(package_bytes);
    if actual != package.content_hash {
        return Err(StoreProtocolError::PackageHashMismatch {
            expected: package.content_hash,
            actual,
        });
    }
    Ok(())
}

pub(super) fn validate_control(
    author_registration: &StoreDeviceRegistrationRef,
    author_pubkey: &str,
    _membership_state: &StoreMembershipStateRef,
    control: Option<&StoreControl>,
) -> Result<(), StoreProtocolError> {
    let Some(control) = control else {
        return Ok(());
    };
    let transition = &control.transition;
    if transition.body.author_registration != *author_registration
        || transition.body.entry.coord.author_pubkey != author_pubkey
        || transition.body.entry.coord.seq == 0
    {
        return Err(StoreProtocolError::InvalidMergeMembershipControl);
    }
    Ok(())
}

pub(super) fn validate_parsed_control(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    validate_control(
        &commit.author_registration,
        &author.author_pubkey,
        &commit.membership_state,
        commit.control(),
    )
}

pub(super) fn validate_circle_control_coord(
    coord: &CircleControlCoord,
) -> Result<(), StoreProtocolError> {
    coord
        .validate()
        .map_err(|_| StoreProtocolError::InvalidCircleControlCoord)?;
    Ok(())
}

pub(super) fn validate_circle_control_refs(
    controls: &[CircleControlRef],
) -> Result<(), StoreProtocolError> {
    let mut seen = BTreeSet::new();
    for control_ref in controls {
        if !seen.insert(control_ref.circle_id()) {
            return Err(StoreProtocolError::DuplicateCircleControl(
                control_ref.circle_id(),
            ));
        }
        validate_circle_control_coord(control_ref.control())?;
    }
    Ok(())
}

impl StoreBatchCommit {
    pub fn commit_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn verify_at(
        &self,
        expected_store_root_hash: ObjectHash,
        expected_coord: &StoreCommitCoord,
        author: &StoreDeviceRegistration,
    ) -> Result<(), StoreProtocolError> {
        self.require_version()?;
        crate::protocol::objects::verify_store_root(
            expected_store_root_hash,
            self.store_root_hash,
        )?;
        let stream_id = commit_stream_id(expected_coord);
        if self.order.seq() != expected_coord.sequence() {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: commit_slot_prefix(&stream_id, expected_coord.sequence()),
                actual: commit_slot_prefix(&stream_id, self.order.seq()),
            });
        }
        self.author_registration.verify_registration(author)?;
        let family = self.candidate_family();
        if let Some(package) = self.store_package() {
            if package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Store package candidate family differs from its commit".to_string(),
                ));
            }
            let expected =
                package_semantic_prefix(family, &stream_id, self.order.seq(), package.content_hash);
            if package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedPackage {
                    expected,
                    actual: package.object.slot().logical_key().to_string(),
                });
            }
        }
        let mut seen_circles = BTreeSet::new();
        for circle_package in self.circle_packages() {
            if circle_package.package.candidate_family != self.candidate_family() {
                return Err(StoreProtocolError::Malformed(
                    "Circle package candidate family differs from its commit".to_string(),
                ));
            }
            if !seen_circles.insert(circle_package.circle_id) {
                return Err(StoreProtocolError::DuplicateCirclePackage(
                    circle_package.circle_id,
                ));
            }
            validate_circle_control_coord(&circle_package.control)?;
            let expected = circle_package_semantic_prefix(
                circle_package.circle_id,
                family,
                &stream_id,
                self.seq(),
                circle_package.package.content_hash,
            );
            if circle_package.package.object.slot().logical_key() != format!("{expected}.pkg") {
                return Err(StoreProtocolError::RelocatedCirclePackage {
                    circle_id: circle_package.circle_id,
                    expected,
                    actual: circle_package
                        .package
                        .object
                        .slot()
                        .logical_key()
                        .to_string(),
                });
            }
        }
        validate_commit_body(self.store_root_hash, &self.body, &self.author_registration)?;
        if matches!(self.body, StoreCommitBody::Operations(_)) {
            validate_operation_membership_authority(
                self.membership_authority.as_ref().ok_or_else(|| {
                    StoreProtocolError::Malformed(
                        "operations commit omits membership authority".to_string(),
                    )
                })?,
            )?;
        }
        if let StoreCommitBody::AbandonCandidates { manifests } = &self.body {
            validate_candidate_abandonment(
                manifests,
                self.store_root_hash,
                &self.author_registration,
                expected_coord,
                &self.order,
                author,
            )?;
        }
        if let StoreCommitBody::OwnerPromotionRequest { request } = &self.body {
            validate_owner_promotion_request_for_commit(
                request,
                self.store_root_hash,
                &self.author_registration,
                author,
                &self.membership_state,
                &self.device_state,
            )?;
        }
        self.verified_candidate_objects()?;
        validate_commit_order(&self.order)?;
        validate_commit_predecessor_states(
            &self.order,
            &self.membership_state,
            &self.device_state,
        )?;
        if let Some(authority) = self.membership_authority.as_ref() {
            validate_membership_authority(authority)?;
        }
        validate_parsed_control(self, author)?;
        self.verify_by(&author.device_signing_pubkey)?;
        Ok(())
    }

    pub fn verify_store_package(&self, package_bytes: &[u8]) -> Result<(), StoreProtocolError> {
        let package = self
            .store_package()
            .ok_or(StoreProtocolError::MissingStorePackage)?;
        verify_package_ref(package, package_bytes)
    }

    pub fn verify_circle_package(
        &self,
        circle_id: CircleId,
        package_bytes: &[u8],
    ) -> Result<(), StoreProtocolError> {
        let package = self
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .ok_or(StoreProtocolError::MissingCirclePackage(circle_id))?;
        verify_package_ref(&package.package, package_bytes)
    }

    #[cfg(test)]
    pub(crate) fn operations_membership_authority(
        &self,
    ) -> Result<StoreOperationMembershipAuthority, StoreProtocolError> {
        if self.operations().is_none() {
            return Err(StoreProtocolError::Malformed(
                "Store commit does not carry operations".to_string(),
            ));
        }
        let predecessor = self.membership_authority.clone().ok_or_else(|| {
            StoreProtocolError::Malformed(
                "operations commit omits its predecessor membership grant authority".to_string(),
            )
        })?;
        validate_operation_membership_authority(&predecessor)?;
        Ok(StoreOperationMembershipAuthority { predecessor })
    }
}
