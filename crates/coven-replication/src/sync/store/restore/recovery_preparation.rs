use super::*;

pub(super) enum RecoveryProtocolObject<T, R> {
    Existing {
        exact: coven_protocol::objects::ExactProtocolObject<T>,
        reference: R,
    },
    Prepared {
        exact: coven_protocol::objects::ExactProtocolObject<T>,
        reference: R,
    },
}

impl<T, R> RecoveryProtocolObject<T, R> {
    pub(super) fn from_remote_state(
        exact: coven_protocol::objects::ExactProtocolObject<T>,
        reference: R,
        exists: bool,
    ) -> Self {
        if exists {
            Self::Existing { exact, reference }
        } else {
            Self::Prepared { exact, reference }
        }
    }

    pub(super) fn exact(&self) -> &coven_protocol::objects::ExactProtocolObject<T> {
        match self {
            Self::Existing { exact, .. } | Self::Prepared { exact, .. } => exact,
        }
    }

    pub(super) fn reference(&self) -> &R {
        match self {
            Self::Existing { reference, .. } | Self::Prepared { reference, .. } => reference,
        }
    }

    pub(super) fn prepared_for_creation(&self) -> Option<&PreparedExactObject> {
        match self {
            Self::Existing { .. } => None,
            Self::Prepared { exact, .. } => Some(&exact.prepared),
        }
    }

    pub(super) fn into_exact(self) -> coven_protocol::objects::ExactProtocolObject<T> {
        match self {
            Self::Existing { exact, .. } | Self::Prepared { exact, .. } => exact,
        }
    }
}

pub(super) struct PreparedRecoveryReadiness {
    pub(super) registration:
        RecoveryProtocolObject<StoreDeviceRegistration, StoreDeviceRegistrationRef>,
    pub(super) initial_ack: RecoveryProtocolObject<StoreAck, StoreAckRef>,
}

impl<'storage> RestoringStore<'storage> {
    pub(super) async fn prepare_or_load_recovery_registration(
        &self,
        expected: StoreDeviceRegistration,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<
        RecoveryProtocolObject<StoreDeviceRegistration, StoreDeviceRegistrationRef>,
        StoreRegistrationError,
    > {
        let root = &self.root;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        match self
            .storage
            .read_prepared_protocol_slot(&context, &slot, semantic_prefix)
            .await
        {
            Ok((bytes, prepared)) => {
                let registration =
                    StoreDeviceRegistration::parse_at(&bytes, root, expected.device_id)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                if registration != expected {
                    return Err(StoreRegistrationError::Invalid(
                        "existing Owner recovery registration differs from its exact authority"
                            .into(),
                    ));
                }
                let reference = StoreDeviceRegistrationRef::from_registration(
                    &registration,
                    prepared.reference().clone(),
                );
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Existing {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: registration,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(coven_protocol::objects::StorageError::NotFound(_)) => {
                let bytes = expected.to_bytes();
                let prepared = self
                    .storage
                    .prepare_protocol_object(&context, slot, semantic_prefix, bytes.clone())
                    .map_err(StoreObjectError::from)?;
                let reference = StoreDeviceRegistrationRef::from_registration(
                    &expected,
                    prepared.reference().clone(),
                );
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Prepared {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: expected,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(error) => Err(StoreObjectError::from(error).into()),
        }
    }

    pub(super) async fn prepared_protocol_object_exists(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
        expected_bytes: &[u8],
    ) -> Result<bool, StoreRegistrationError> {
        match self
            .storage
            .read_prepared_protocol_slot(context, prepared.reference().slot(), semantic_prefix)
            .await
        {
            Ok((bytes, opened))
                if bytes == expected_bytes && opened.reference() == prepared.reference() =>
            {
                Ok(true)
            }
            Ok(_) => Err(StoreRegistrationError::Invalid(format!(
                "exact object {semantic_prefix:?} differs from its staged Owner recovery bytes"
            ))),
            Err(coven_protocol::objects::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(StoreObjectError::from(error).into()),
        }
    }

    pub(super) async fn prepare_or_load_initial_recovery_ack(
        &self,
        registration: &StoreDeviceRegistration,
        registration_ref: &StoreDeviceRegistrationRef,
        first_slot: coven_protocol::objects::ObjectSlot,
        store_cut: StoreHistoryCut,
        device_state: StoreDeviceStateRef,
        published_at: &str,
        device_signer: &UserKeypair,
    ) -> Result<RecoveryProtocolObject<StoreAck, StoreAckRef>, StoreRegistrationError> {
        let root = &self.root;
        let storage = self.storage;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let prefix = ack_slot_prefix(&registration.device_id.to_string(), 1);
        match storage
            .read_prepared_protocol_slot(&context, &first_slot, &prefix)
            .await
        {
            Ok((bytes, prepared)) => {
                let object = prepared.reference();
                let decoded: StoreAck = serde_json::from_slice(&bytes)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                let reference = StoreAckRef {
                    registration: registration_ref.clone(),
                    sequence: decoded.sequence,
                    ack_hash: decoded.ack_hash(),
                    object: object.clone(),
                };
                let ack = StoreAck::parse_at(&bytes, root, &reference, registration)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                let expected_activation = registration
                    .store_acknowledgement_activation(registration_ref)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                    .activation_id();
                if ack.sequence != 1
                    || ack.successor.predecessor.is_some()
                    || ack.registration != *registration_ref
                    || ack.store_cut != store_cut
                    || ack.device_state != device_state
                    || ack.last_sync != published_at
                    || ack.successor.activation != expected_activation
                    || ack.successor.next_slot == first_slot
                {
                    return Err(StoreRegistrationError::Invalid(
                        "existing Owner recovery acknowledgement differs from its exact authority"
                            .into(),
                    ));
                }
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Existing {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: ack,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(coven_protocol::objects::StorageError::NotFound(_)) => {
                let next_slot = storage
                    .allocate_protocol_slot(
                        &context,
                        &ack_slot_prefix(&registration.device_id.to_string(), 2),
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?;
                let ack = StoreAck::signed(
                    root.store_root_hash,
                    registration_ref.clone(),
                    1,
                    store_cut,
                    device_state,
                    None,
                    StoreAckExclusionState {
                        proposal_freezes: Vec::new(),
                    },
                    published_at.to_string(),
                    SuccessorLink {
                        activation: registration
                            .store_acknowledgement_activation(registration_ref)
                            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                            .activation_id(),
                        predecessor: None,
                        next_slot,
                    },
                    device_signer,
                )
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                let bytes = ack.to_bytes();
                let prepared = storage
                    .prepare_protocol_object(&context, first_slot, &prefix, bytes.clone())
                    .map_err(StoreObjectError::from)?;
                let reference = StoreAckRef {
                    registration: registration_ref.clone(),
                    sequence: 1,
                    ack_hash: ack.ack_hash(),
                    object: prepared.reference().clone(),
                };
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Prepared {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: ack,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(error) => Err(StoreObjectError::from(error).into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn prepare_or_load_owner_recovery_node(
        &self,
        recovery_slot: coven_protocol::objects::ObjectSlot,
        owner_pubkey: &str,
        owner_grant: &coven_protocol::membership::MembershipGrantId,
        sequence: u64,
        recovery_id: DeviceRecoveryId,
        membership: &coven_protocol::circle_control::StoreMembershipStateRef,
        predecessor: &Option<OwnerRecoveryNodeRef>,
        readiness: &DeviceRecoveryReadiness,
        identity_signer: &UserKeypair,
    ) -> Result<
        RecoveryProtocolObject<OwnerRecoveryNode, OwnerRecoveryNodeRef>,
        StoreRegistrationError,
    > {
        let root = &self.root;
        let storage = self.storage;
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::OwnerRecoveryNode,
        );
        let prefix = owner_recovery_semantic_prefix(owner_pubkey, owner_grant.clone(), sequence);
        match storage
            .read_prepared_protocol_slot(&context, &recovery_slot, &prefix)
            .await
        {
            Ok((bytes, prepared)) => {
                let object = prepared.reference();
                let decoded: OwnerRecoveryNode = serde_json::from_slice(&bytes)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                let reference = OwnerRecoveryNodeRef {
                    owner_pubkey: decoded.owner_pubkey.clone(),
                    owner_grant: decoded.owner_grant.clone(),
                    sequence: decoded.sequence,
                    node_hash: decoded.node_hash(),
                    object: object.clone(),
                };
                let node = OwnerRecoveryNode::parse_at(&bytes, root, &reference)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                if node.recovery_id != recovery_id
                    || node.owner_pubkey != owner_pubkey
                    || node.owner_grant != *owner_grant
                    || node.sequence != sequence
                    || node.membership != *membership
                    || node.predecessor != *predecessor
                    || node.readiness != *readiness
                    || node.next_slot == recovery_slot
                {
                    return Err(StoreRegistrationError::Invalid(
                        "existing Owner recovery node differs from its exact authority".into(),
                    ));
                }
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Existing {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: node,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(coven_protocol::objects::StorageError::NotFound(_)) => {
                let next_sequence = sequence.checked_add(1).ok_or_else(|| {
                    StoreRegistrationError::Invalid("Owner recovery sequence overflow".into())
                })?;
                let next_slot = storage
                    .allocate_protocol_slot(
                        &context,
                        &owner_recovery_semantic_prefix(
                            owner_pubkey,
                            owner_grant.clone(),
                            next_sequence,
                        ),
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?;
                let node = OwnerRecoveryNode::signed(
                    root.store_root_hash,
                    recovery_id,
                    owner_grant.clone(),
                    sequence,
                    membership.clone(),
                    predecessor.clone(),
                    readiness.clone(),
                    next_slot,
                    identity_signer,
                )
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                let bytes = node.to_bytes();
                let prepared = storage
                    .prepare_protocol_object(&context, recovery_slot, &prefix, bytes.clone())
                    .map_err(StoreObjectError::from)?;
                let reference = OwnerRecoveryNodeRef {
                    owner_pubkey: owner_pubkey.to_string(),
                    owner_grant: owner_grant.clone(),
                    sequence,
                    node_hash: node.node_hash(),
                    object: prepared.reference().clone(),
                };
                let object = prepared.reference().clone();
                Ok(RecoveryProtocolObject::Prepared {
                    exact: coven_protocol::objects::ExactProtocolObject {
                        value: node,
                        bytes,
                        object,
                        prepared,
                    },
                    reference,
                })
            }
            Err(error) => Err(StoreObjectError::from(error).into()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn install_activated_owner_recovery(
        &self,
        origin: &StoreDeviceRegistrationOrigin,
        device_id: coven_protocol::store_commit::StoreDeviceId,
        recovery_id: DeviceRecoveryId,
        recovery_slot: &coven_protocol::objects::ObjectSlot,
        owner_pubkey: &str,
        owner_grant: &coven_protocol::membership::MembershipGrantId,
        sequence: u64,
        predecessor: &Option<OwnerRecoveryNodeRef>,
    ) -> Result<Option<StoreDeviceRegistrationRef>, StoreRegistrationError> {
        let database = &self.database;
        let history = self.history.restore_history();
        let storage = self.storage;
        let root = &self.root;
        let Some(registration) = database
            .activated_store_device_registration_for_device(device_id)
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?
        else {
            return Ok(None);
        };
        let registration_ref = registration.reference();
        let StoreDeviceRegistrationActivation::Recovery {
            recovery_id: activated_recovery_id,
            node: node_ref,
        } = registration.activation().clone()
        else {
            return Err(StoreRegistrationError::Invalid(
                "derived Owner recovery device has a non-recovery activation".into(),
            ));
        };
        if registration.value().origin != *origin
            || registration.value().author_pubkey != owner_pubkey
            || activated_recovery_id != recovery_id
            || node_ref.owner_pubkey != owner_pubkey
            || node_ref.owner_grant != *owner_grant
            || node_ref.sequence != sequence
            || node_ref.object.slot() != recovery_slot
        {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery registration differs from the requested authority".into(),
            ));
        }
        let provider = storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device;
        if registration.value().provider != provider {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery registration belongs to another provider principal"
                    .into(),
            ));
        }
        let node = history.load_owner_recovery_node(&node_ref).await?;
        if node.value.recovery_id != recovery_id
            || node.value.predecessor != *predecessor
            || &node.value.readiness.registration != registration_ref
        {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery node differs from the requested authority".into(),
            ));
        }
        let initial_ack_ref = node.value.readiness.initial_ack.clone();
        let initial_ack = history
            .load_store_ack(&initial_ack_ref, registration.value())
            .await?;
        if initial_ack.value.store_cut != node.value.readiness.bootstrap_cut {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery acknowledgement differs from its recovery node".into(),
            ));
        }

        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let (registration_bytes, registration_prepared) = storage
            .read_prepared_protocol_slot(
                &registration_context,
                registration_ref.object.slot(),
                &registration_semantic_prefix(&registration_ref.device_id.to_string()),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if registration_bytes != registration.value().to_bytes()
            || registration_prepared.reference() != &registration_ref.object
        {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery registration differs from its prepared exact object"
                    .into(),
            ));
        }
        let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let (initial_ack_bytes, initial_ack_prepared) = storage
            .read_prepared_protocol_slot(
                &ack_context,
                initial_ack_ref.object.slot(),
                &ack_slot_prefix(
                    &registration_ref.device_id.to_string(),
                    initial_ack_ref.sequence,
                ),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if initial_ack_bytes != initial_ack.bytes
            || initial_ack_prepared.reference() != &initial_ack_ref.object
        {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery acknowledgement differs from its prepared exact object"
                    .into(),
            ));
        }
        let already_activated = database
            .stage_owner_recovery_registration(
                coven_protocol::objects::ExactProtocolObject {
                    value: registration.value().clone(),
                    bytes: registration_bytes,
                    object: registration_prepared.reference().clone(),
                    prepared: registration_prepared,
                },
                initial_ack_ref,
                coven_protocol::objects::ExactProtocolObject {
                    value: initial_ack.value,
                    bytes: initial_ack_bytes,
                    object: initial_ack_prepared.reference().clone(),
                    prepared: initial_ack_prepared,
                },
                registration.activation().clone(),
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
        if !already_activated {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery disappeared while installing its local journal".into(),
            ));
        }
        Ok(Some(registration.reference().clone()))
    }
}
