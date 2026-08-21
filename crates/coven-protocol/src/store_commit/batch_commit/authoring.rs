use super::validation::*;
use super::*;

impl StoreBatchCommit {
    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_authorization(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        authorization: crate::reclaim::ReclaimAuthorizationRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimAuthorization {
                authorization: Box::new(authorization),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_reclaim_receipt(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        receipt: crate::reclaim::ReclaimReceiptRef,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::ReclaimReceipt {
                receipt: Box::new(receipt),
            },
            signer,
        )
    }

    pub fn merge_dependencies(&self) -> &BTreeMap<AuthorStreamId, StoreBatchCommitRef> {
        &self.order.dependencies
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_owner_promotion_request(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        request: OwnerPromotionRequest,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            Some(&membership_authority),
            signer,
        )?;
        validate_owner_promotion_request_for_commit(
            &request,
            store_root_hash,
            &author_registration,
            author,
            &membership_state,
            &device_state,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            Some(membership_authority),
            StoreCommitBody::OwnerPromotionRequest {
                request: Box::new(request),
            },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_with_candidate_abandonment(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        mut manifests: Vec<CandidateCleanupManifest>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            None,
            signer,
        )?;
        manifests.sort();
        validate_candidate_abandonment(
            &manifests,
            store_root_hash,
            &author_registration,
            &coord,
            &order,
            author,
        )?;
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            None,
            StoreCommitBody::AbandonCandidates { manifests },
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_operations(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        coord: StoreCommitCoord,
        author_registration: StoreDeviceRegistrationRef,
        author: &StoreDeviceRegistration,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: StoreOperationMembershipAuthority,
        input: StoreCommitOperationsInput<'_>,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let membership_authority = membership_authority.into_commit_authority();
        validate_commit_envelope(
            store_root_hash,
            &coord,
            &author_registration,
            author,
            &order,
            &membership_state,
            &device_state,
            Some(&membership_authority),
            signer,
        )?;
        let StoreCommitOperationsInput {
            acknowledgement,
            circle_acknowledgements,
            control,
            device_join_attempt_decisions,
            provider_access_grants,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        } = input;
        validate_control(
            &author_registration,
            &author.author_pubkey,
            &membership_state,
            control.as_ref(),
        )?;
        validate_commit_acknowledgement(&acknowledgement, &author_registration)?;
        validate_commit_circle_acknowledgements(&circle_acknowledgements, &author_registration)?;
        let stream_id = commit_stream_id(&coord);
        let seq = order.seq();
        let candidate_family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        let store_package = store_package
            .map(|input| {
                if input.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Store package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = package_semantic_prefix(
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.bytes),
                );
                package_ref(&semantic_prefix, &input)
            })
            .transpose()?;
        validate_device_join_attempt_decision_refs(&device_join_attempt_decisions)?;
        validate_provider_access_refs(&provider_access_grants)?;
        validate_device_registration_refs(&device_registrations)?;
        validate_device_exclusion_refs(&device_exclusion_proposals, &device_exclusion_outcomes)?;
        validate_stream_activations(
            store_root_hash,
            &author_registration,
            control.as_ref(),
            &stream_activations,
        )?;
        let mut seen_circles = BTreeSet::new();
        let circle_packages = circle_packages
            .iter()
            .map(|input| {
                if !seen_circles.insert(input.circle_id) {
                    return Err(StoreProtocolError::DuplicateCirclePackage(input.circle_id));
                }
                validate_circle_control_coord(&input.control)?;
                if input.package.candidate_family != candidate_family {
                    return Err(StoreProtocolError::Malformed(
                        "Circle package candidate family differs from its commit".to_string(),
                    ));
                }
                let semantic_prefix = circle_package_semantic_prefix(
                    input.circle_id,
                    candidate_family,
                    &stream_id,
                    seq,
                    ObjectHash::digest(input.package.bytes),
                );
                let package = package_ref(&semantic_prefix, &input.package)?;
                Ok(CirclePackageRef {
                    circle_id: input.circle_id,
                    control: input.control.clone(),
                    package,
                    key_fingerprint: input.key_fingerprint,
                })
            })
            .collect::<Result<Vec<_>, StoreProtocolError>>()?;
        validate_circle_control_refs(&circle_controls)?;
        let operations = StoreCommitOperations {
            acknowledgement,
            circle_acknowledgements,
            control,
            device_join_attempt_decisions,
            provider_access_grants,
            device_registrations,
            device_exclusion_proposals,
            device_exclusion_outcomes,
            stream_activations,
            circle_controls,
            store_package,
            circle_packages,
        };
        if operations.is_empty() {
            return Err(StoreProtocolError::EmptyBatch);
        }
        Self::finish_signed_body(
            store_root_hash,
            write_id,
            author_registration,
            order,
            membership_state,
            device_state,
            Some(membership_authority),
            StoreCommitBody::Operations(operations),
            signer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_signed_body(
        store_root_hash: ObjectHash,
        write_id: WriteId,
        author_registration: StoreDeviceRegistrationRef,
        order: StoreCommitOrder,
        membership_state: StoreMembershipStateRef,
        device_state: StoreDeviceStateRef,
        membership_authority: Option<MembershipGrantCreationAuthority>,
        body: StoreCommitBody,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let family =
            CandidateFamilyId::derive(store_root_hash, &author_registration, &write_id, &order);
        validate_commit_body(store_root_hash, &body, &author_registration)?;
        let candidate_objects = candidate_manifest(family, &body)?;
        Ok(Signed::sign(
            StoreBatchCommitBody {
                store_root_hash,
                write_id,
                author_registration,
                order,
                membership_state,
                device_state,
                membership_authority,
                candidate_objects,
                body,
            },
            signer,
        ))
    }
}
