use super::*;

fn registration_slot_semantic_prefix(object: &ExactObjectRef) -> Result<String, StoreObjectError> {
    object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .map(str::to_string)
        .ok_or_else(|| StoreObjectError::InvalidObject {
            semantic_prefix: object.slot().logical_key().to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )),
        })
}

fn verify_opened_registration(
    bytes: &[u8],
    root: &StoreRootRef,
    reference: &StoreDeviceRegistrationRef,
    pinned_root: &StoreProtocolRoot,
) -> Result<StoreDeviceRegistration, StoreProtocolError> {
    let registration = StoreDeviceRegistration::parse_at(bytes, root, reference.device_id)?;
    reference.verify_registration(&registration)?;
    let expected_prefix = match &registration.origin {
        StoreDeviceRegistrationOrigin::Founder { creation_id }
            if *creation_id == pinned_root.descriptor.creation_id
                && registration.provider
                    == pinned_root.descriptor.founder_provider_admin.provider
                && reference.object.slot() == &pinned_root.descriptor.founder_registration =>
        {
            founder_registration_semantic_prefix(*creation_id)
        }
        StoreDeviceRegistrationOrigin::Founder { .. } => {
            return Err(StoreProtocolError::InvalidFounder);
        }
        _ if reference.object.slot() != &pinned_root.descriptor.founder_registration => {
            registration_semantic_prefix(&reference.device_id.to_string())
        }
        _ => return Err(StoreProtocolError::InvalidFounder),
    };
    let actual_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreProtocolError::Malformed(
                "device registration slot has no .json suffix".to_string(),
            )
        })?;
    if actual_prefix != expected_prefix {
        return Err(StoreProtocolError::Malformed(
            "device registration exact slot does not match its signed origin".to_string(),
        ));
    }
    Ok(registration)
}

impl<'a> StoreCommitVerifier<'a> {
    pub(crate) async fn load_registration(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        if let Some(registration) = self
            .registrations
            .lock()
            .expect("verified registration cache mutex is not poisoned")
            .get(reference)
        {
            return Ok(registration.clone());
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix = registration_slot_semantic_prefix(&reference.object)?;
        let bytes = self
            .storage
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await?;
        let verify_bytes = bytes.clone();
        let expected_root = self.root.reference().clone();
        let expected_reference = reference.clone();
        let pinned_root = self.root.protocol().clone();
        let value = run_blocking_object_verification(
            &semantic_prefix,
            &reference.object,
            Box::new(move || {
                verify_opened_registration(
                    &verify_bytes,
                    &expected_root,
                    &expected_reference,
                    &pinned_root,
                )
            }),
        )
        .await?;
        let verified = VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.registration_hash,
            object: reference.object.clone(),
        };
        let mut registrations = self
            .registrations
            .lock()
            .expect("verified registration cache mutex is not poisoned");
        Ok(registrations
            .entry(reference.clone())
            .or_insert(verified)
            .clone())
    }

    pub(crate) async fn verify_owner_recovery_activation(
        &self,
        commit: &StoreBatchCommit,
    ) -> Result<
        Option<(
            coven_protocol::membership::MembershipGrantId,
            OwnerRecoveryActivationId,
        )>,
        StorePullError,
    > {
        let mut recoveries = commit.stream_activations().iter().filter_map(|activation| {
            let StreamActivation::GrantAuthorized {
                author_registration,
                grant_id,
                anchor: anchor @ GrantStreamAnchor::OwnerRecovery { .. },
                ..
            } = activation
            else {
                return None;
            };
            Some((author_registration, grant_id, anchor))
        });
        let Some((registration_ref, grant_id, anchor)) = recoveries.next() else {
            return Ok(None);
        };
        if recoveries.next().is_some() {
            return Err(StorePullError::InvalidState(
                "Store commit activates more than one Owner recovery stream".to_string(),
            ));
        }
        let registration = self.load_registration(registration_ref).await?;
        OwnerRecoveryActivationId::derive(
            self.root.reference(),
            &registration.value.author_pubkey,
            grant_id,
            anchor,
        )
        .map(|activation| Some((grant_id.clone(), activation)))
        .map_err(StorePullError::Protocol)
    }

    pub(crate) async fn discover_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<Vec<ReferencedStoreDeviceRegistration>, StorePullError> {
        let protocol = &self.root.protocol();
        if membership
            .active_owner_grant(&protocol.descriptor.founder_pubkey)
            .as_ref()
            != Some(&protocol.descriptor.founder_grant)
        {
            return Ok(Vec::new());
        }
        let GrantStreamAnchor::OwnerRecovery { first_slot } = &protocol.descriptor.founder_recovery
        else {
            return Err(StorePullError::InvalidState(
                "Store founder recovery authority has no recovery stream".into(),
            ));
        };
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let mut slot = first_slot.clone();
        let mut predecessor: Option<OwnerRecoveryNodeRef> = None;
        let mut sequence = 1_u64;
        let mut recovered = Vec::new();
        loop {
            let prefix = owner_recovery_semantic_prefix(
                &protocol.descriptor.founder_pubkey,
                protocol.descriptor.founder_grant.clone(),
                sequence,
            );
            let (bytes, object) = match self
                .storage
                .read_protocol_slot(&context, &slot, &prefix)
                .await
            {
                Ok(opened) => opened,
                Err(StorageError::NotFound(_)) => break,
                Err(error) => return Err(StoreObjectError::Storage(error).into()),
            };
            let unverified: OwnerRecoveryNode = serde_json::from_slice(&bytes)
                .map_err(|error| StorePullError::context("Owner recovery node", error))?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: unverified.owner_pubkey.clone(),
                owner_grant: unverified.owner_grant.clone(),
                sequence: unverified.sequence,
                node_hash: unverified.node_hash(),
                object,
            };
            let node = OwnerRecoveryNode::parse_at(&bytes, self.root.reference(), &reference)
                .map_err(StorePullError::Protocol)?;
            if reference.owner_pubkey != protocol.descriptor.founder_pubkey
                || reference.owner_grant != protocol.descriptor.founder_grant
                || reference.sequence != sequence
                || node.predecessor != predecessor
                || !predecessor_verifies_owner(
                    membership,
                    &node.membership,
                    &node.owner_pubkey,
                    &node.owner_grant,
                )
            {
                return Err(StorePullError::InvalidState(
                    "Owner recovery stream differs from its root-anchored authority".into(),
                ));
            }
            let registration = self
                .load_registration(&node.readiness.registration)
                .await?
                .value;
            let initial_ack = self
                .load_store_ack(&node.readiness.initial_ack, &registration)
                .await?;
            let origin_matches = matches!(
                &registration.origin,
                StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id,
                    recovery_slot,
                    owner_grant,
                } if *recovery_id == node.recovery_id
                    && recovery_slot == reference.slot()
                    && owner_grant == &node.owner_grant
            );
            if !origin_matches
                || registration.author_pubkey != node.owner_pubkey
                || initial_ack.sequence != 1
                || initial_ack.successor.predecessor.is_some()
                || initial_ack.store_cut != node.readiness.bootstrap_cut
                || initial_ack.registration != node.readiness.registration
            {
                return Err(StorePullError::InvalidState(
                    "Owner recovery readiness differs from its registration graph".into(),
                ));
            }
            recovered.push(
                ReferencedStoreDeviceRegistration::verified(
                    node.readiness.registration.clone(),
                    registration,
                )
                .map_err(StorePullError::Protocol)?,
            );
            slot = node.next_slot.clone();
            predecessor = Some(reference);
            sequence = sequence.checked_add(1).ok_or_else(|| {
                StorePullError::InvalidState("Owner recovery sequence overflow".into())
            })?;
        }
        Ok(recovered)
    }

    pub(crate) async fn load_active_registrations(
        &self,
        state: &ResolvedStoreDeviceState,
    ) -> Result<BTreeMap<StoreDeviceId, ReferencedStoreDeviceRegistration>, StorePullError> {
        let mut active = BTreeMap::new();
        for (device_id, record) in &state.devices {
            if !matches!(record.status, StoreDeviceStatus::Active) {
                continue;
            }
            let registration = self.load_registration(&record.registration).await?;
            if registration.value.device_id != *device_id {
                return Err(StorePullError::InvalidState(
                    "resolved Store device state names another exact registration".to_string(),
                ));
            }
            active.insert(
                *device_id,
                ReferencedStoreDeviceRegistration::verified(
                    record.registration.clone(),
                    registration.value,
                )
                .map_err(StorePullError::Protocol)?,
            );
        }
        Ok(active)
    }

    pub(crate) async fn verify_canonical_owner_registration(
        &self,
        state: &ResolvedStoreDeviceState,
        owner_pubkey: &str,
        selected: &StoreDeviceRegistrationRef,
    ) -> Result<(), StorePullError> {
        let active = self.load_active_registrations(state).await?;
        let canonical = active
            .values()
            .filter(|registration| registration.value().author_pubkey == owner_pubkey)
            .map(ReferencedStoreDeviceRegistration::reference)
            .min();
        if canonical != Some(selected) {
            return Err(StorePullError::InvalidState(
                "conflict-resolution acceptance does not use the canonical active Owner registration"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn load_founder_registration(
        &self,
    ) -> Result<VerifiedObject<StoreDeviceRegistration>, StoreObjectError> {
        if let Some(reference) = self.founder_registration.get() {
            return self.load_registration(reference).await;
        }
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix =
            founder_registration_semantic_prefix(self.root.protocol().descriptor.creation_id);
        let (bytes, object) = self
            .storage
            .read_protocol_slot(
                &context,
                &self.root.protocol().descriptor.founder_registration,
                &semantic_prefix,
            )
            .await?;
        let verify_bytes = bytes.clone();
        let verify_object = object.clone();
        let verify_root = self.root.reference().clone();
        let verify_root_value = self.root.protocol().clone();
        let (value, reference) = run_blocking_object_verification(
            &semantic_prefix,
            &object,
            Box::new(move || {
                let unverified: StoreDeviceRegistration =
                    serde_json::from_slice(&verify_bytes).map_err(StoreProtocolError::from)?;
                let reference =
                    StoreDeviceRegistrationRef::from_registration(&unverified, verify_object);
                let value = verify_opened_registration(
                    &verify_bytes,
                    &verify_root,
                    &reference,
                    &verify_root_value,
                )?;
                Ok((value, reference))
            }),
        )
        .await?;
        let verified = VerifiedObject {
            value,
            bytes,
            semantic_hash: reference.registration_hash,
            object,
        };
        self.registrations
            .lock()
            .expect("verified registration cache mutex is not poisoned")
            .entry(reference.clone())
            .or_insert_with(|| verified.clone());
        let _ = self.founder_registration.set(reference);
        Ok(verified)
    }

    pub(crate) async fn load_exact_object<T>(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
        semantic_hash: ObjectHash,
        verify: impl FnOnce(&[u8]) -> Result<T, StoreProtocolError> + Send + 'static,
    ) -> Result<VerifiedObject<T>, StoreObjectError>
    where
        T: Send + 'static,
    {
        let bytes = self
            .storage
            .read_protocol_object(context, object, semantic_prefix)
            .await?;
        let verify_bytes = bytes.clone();
        let value = run_blocking_object_verification(
            semantic_prefix,
            object,
            Box::new(move || verify(&verify_bytes)),
        )
        .await?;
        Ok(VerifiedObject {
            value,
            bytes,
            semantic_hash,
            object: object.clone(),
        })
    }

    pub(crate) async fn load_provider_access_grant(
        &self,
        reference: &coven_protocol::provider::StoreMemberProviderAccessGrantRef,
        administrator: &StoreDeviceRegistration,
    ) -> Result<
        VerifiedObject<coven_protocol::provider::StoreMemberProviderAccessGrant>,
        StoreObjectError,
    > {
        let context = ProtocolObjectContext::signed_plaintext(
            self.root.reference().store_root_hash,
            ProtocolObjectDomain::ProviderAccessGrant,
        );
        let semantic_prefix = provider_access_grant_semantic_prefix(&reference.grant_id);
        let expected = reference.clone();
        let administrator = administrator.clone();
        let store = self.root.protocol().descriptor.provider.clone();
        self.load_exact_object(
            &context,
            &reference.object,
            &semantic_prefix,
            reference.grant_hash,
            move |bytes| {
                let grant: coven_protocol::provider::StoreMemberProviderAccessGrant =
                    decode_protocol_object(bytes)?;
                expected
                    .verify(&grant)
                    .and_then(|()| grant.verify(&store, &administrator))
                    .map_err(|_| StoreProtocolError::ProviderAccessMismatch)?;
                Ok(grant)
            },
        )
        .await
    }
}
