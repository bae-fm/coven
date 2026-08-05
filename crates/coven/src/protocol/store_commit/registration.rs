use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeviceRecoveryReadiness {
    pub registration: StoreDeviceRegistrationRef,
    pub initial_ack: StoreAckRef,
    pub bootstrap_cut: StoreHistoryCut,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerRecoveryNodeBody {
    pub store_root_hash: ObjectHash,
    pub recovery_id: DeviceRecoveryId,
    pub owner_pubkey: String,
    pub owner_grant: MembershipGrantId,
    pub sequence: u64,
    pub membership: StoreMembershipStateRef,
    pub predecessor: Option<OwnerRecoveryNodeRef>,
    pub readiness: DeviceRecoveryReadiness,
    pub next_slot: ObjectSlot,
}

impl SignedBody for OwnerRecoveryNodeBody {
    const DOMAIN: &'static [u8] = OWNER_RECOVERY_NODE_DOMAIN;
}

pub(crate) type OwnerRecoveryNode = Signed<OwnerRecoveryNodeBody>;

impl OwnerRecoveryNode {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed(
        store_root_hash: ObjectHash,
        recovery_id: DeviceRecoveryId,
        owner_grant: MembershipGrantId,
        sequence: u64,
        membership: StoreMembershipStateRef,
        predecessor: Option<OwnerRecoveryNodeRef>,
        readiness: DeviceRecoveryReadiness,
        next_slot: ObjectSlot,
        owner_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let body = OwnerRecoveryNodeBody {
            store_root_hash,
            recovery_id,
            owner_pubkey: keys::public_key_hex(owner_signer),
            owner_grant,
            sequence,
            membership,
            predecessor,
            readiness,
            next_slot,
        };
        body.validate_shape()?;
        Ok(Signed::sign(body, owner_signer))
    }

    pub(crate) fn parse_at(
        bytes: &[u8],
        store_root: &StoreRootRef,
        reference: &OwnerRecoveryNodeRef,
    ) -> Result<Self, StoreProtocolError> {
        let node: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        node.body().validate_shape()?;
        if node.store_root_hash != store_root.store_root_hash
            || node.owner_pubkey != reference.owner_pubkey
            || node.owner_grant != reference.owner_grant
            || node.sequence != reference.sequence
            || node.node_hash() != reference.node_hash
        {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        let owner_pubkey = node.owner_pubkey.clone();
        node.verify_by(&owner_pubkey)?;
        Ok(node)
    }

    pub(crate) fn node_hash(&self) -> ObjectHash {
        self.hash()
    }
}

impl OwnerRecoveryNodeBody {
    fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        let predecessor_matches = match &self.predecessor {
            None => self.sequence == 1,
            Some(predecessor) => {
                predecessor.owner_pubkey == self.owner_pubkey
                    && predecessor.owner_grant == self.owner_grant
                    && predecessor.sequence.checked_add(1) == Some(self.sequence)
            }
        };
        if !predecessor_matches || self.readiness.initial_ack.sequence != 1 {
            return Err(StoreProtocolError::OwnerRecoveryMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationOrigin {
    Founder {
        creation_id: StoreCreationId,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        attempt_slot: ObjectSlot,
        outcome_slot: ObjectSlot,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        recovery_slot: ObjectSlot,
        owner_grant: MembershipGrantId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivation {
    Founder {
        root: StoreRootRef,
    },
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatedStoreDeviceRegistrationRef {
    pub registration: StoreDeviceRegistrationRef,
    pub authority: StoreDeviceRegistrationActivationRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StoreDeviceRegistrationActivationRef {
    Join {
        attempt_id: DeviceJoinAttemptId,
        outcome: DeviceJoinOutcomeRef,
    },
    Recovery {
        recovery_id: DeviceRecoveryId,
        node: OwnerRecoveryNodeRef,
    },
}

impl StoreDeviceRegistrationOrigin {
    pub(super) fn external_id(&self) -> ObjectHash {
        match self {
            Self::Founder { creation_id } => creation_id.object_hash(),
            Self::Join { attempt_id, .. } => attempt_id.object_hash(),
            Self::Recovery { recovery_id, .. } => recovery_id.object_hash(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum DeviceStreamAnchor {
    StoreAnnouncements {
        first_slot: ObjectSlot,
    },
    StoreAcknowledgements {
        first_slot: ObjectSlot,
    },
    StoreSnapshots {
        first_slot: ObjectSlot,
    },
    /// Per-(device, Circle) acknowledgement stream. Unlike the three permanent
    /// anchors above, this is never a registration field: it is derived on
    /// demand to bind one device's Circle-acknowledgement stream to its Circle.
    CircleAcknowledgements {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    /// Per-(device, Circle) snapshot stream. Like the Circle-acknowledgement
    /// anchor, never a registration field: derived on demand to bind one
    /// device's Circle-snapshot stream to its Circle.
    CircleSnapshots {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
}

impl DeviceStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreAnnouncements { first_slot }
            | Self::StoreAcknowledgements { first_slot }
            | Self::StoreSnapshots { first_slot }
            | Self::CircleAcknowledgements { first_slot, .. }
            | Self::CircleSnapshots { first_slot, .. } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum GrantStreamAnchor {
    StoreMembership {
        first_slot: ObjectSlot,
    },
    OwnerRecovery {
        first_slot: ObjectSlot,
    },
    CircleControl {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleRoster {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
    CircleMetadata {
        circle_id: CircleId,
        first_slot: ObjectSlot,
    },
}

impl GrantStreamAnchor {
    pub fn first_slot(&self) -> &ObjectSlot {
        match self {
            Self::StoreMembership { first_slot }
            | Self::OwnerRecovery { first_slot }
            | Self::CircleControl { first_slot, .. }
            | Self::CircleRoster { first_slot, .. }
            | Self::CircleMetadata { first_slot, .. } => first_slot,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceRegistrationBody {
    pub store_root: StoreRootRef,
    pub device_id: StoreDeviceId,
    pub author_pubkey: String,
    pub device_signing_pubkey: String,
    pub origin: StoreDeviceRegistrationOrigin,
    pub provider: ProviderDeviceBinding,
    pub store_commits: DeviceStreamAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
}

impl SignedBody for StoreDeviceRegistrationBody {
    const DOMAIN: &'static [u8] = REGISTRATION_DOMAIN;
}

pub(crate) type StoreDeviceRegistration = Signed<StoreDeviceRegistrationBody>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReferencedStoreDeviceRegistration {
    reference: StoreDeviceRegistrationRef,
    value: StoreDeviceRegistration,
}

impl ReferencedStoreDeviceRegistration {
    pub(crate) fn verified(
        reference: StoreDeviceRegistrationRef,
        value: StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        reference.verify_registration(&value)?;
        Ok(Self { reference, value })
    }

    pub(crate) fn reference(&self) -> &StoreDeviceRegistrationRef {
        &self.reference
    }

    pub(crate) fn value(&self) -> &StoreDeviceRegistration {
        &self.value
    }
}

impl<'de> Deserialize<'de> for ReferencedStoreDeviceRegistration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct EncodedRegistration {
            reference: StoreDeviceRegistrationRef,
            value: StoreDeviceRegistration,
        }

        let encoded = EncodedRegistration::deserialize(deserializer)?;
        Self::verified(encoded.reference, encoded.value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ActivatedStoreDeviceRegistration {
    registration: ReferencedStoreDeviceRegistration,
    activation: StoreDeviceRegistrationActivation,
}

impl ActivatedStoreDeviceRegistration {
    pub(crate) fn verified(
        registration: ReferencedStoreDeviceRegistration,
        activation: StoreDeviceRegistrationActivation,
    ) -> Result<Self, StoreProtocolError> {
        let value = registration.value();
        let matches = match (&value.origin, &activation) {
            (
                StoreDeviceRegistrationOrigin::Founder { .. },
                StoreDeviceRegistrationActivation::Founder { root },
            ) => &value.store_root == root,
            (
                StoreDeviceRegistrationOrigin::Join {
                    attempt_id: origin_attempt,
                    outcome_slot,
                    ..
                },
                StoreDeviceRegistrationActivation::Join {
                    attempt_id,
                    outcome,
                },
            ) => origin_attempt == attempt_id && outcome_slot == outcome.slot(),
            (
                StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id: origin_recovery,
                    recovery_slot,
                    ..
                },
                StoreDeviceRegistrationActivation::Recovery { recovery_id, node },
            ) => origin_recovery == recovery_id && recovery_slot == node.slot(),
            _ => false,
        };
        if !matches {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(Self {
            registration,
            activation,
        })
    }

    pub(crate) fn verify_reference(
        &self,
        reference: &ActivatedStoreDeviceRegistrationRef,
    ) -> Result<(), StoreProtocolError> {
        if self.registration.reference() != &reference.registration {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        let matches = match (&reference.authority, &self.activation) {
            (
                StoreDeviceRegistrationActivationRef::Join {
                    attempt_id,
                    outcome,
                },
                StoreDeviceRegistrationActivation::Join {
                    attempt_id: activated_attempt,
                    outcome: activated_outcome,
                },
            ) => attempt_id == activated_attempt && outcome == activated_outcome,
            (
                StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
                StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: activated_recovery,
                    node: activated_node,
                },
            ) => recovery_id == activated_recovery && node == activated_node,
            _ => false,
        };
        if !matches {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        Ok(())
    }

    pub(crate) fn activated_reference(
        &self,
    ) -> Result<ActivatedStoreDeviceRegistrationRef, StoreProtocolError> {
        let authority = match &self.activation {
            StoreDeviceRegistrationActivation::Founder { .. } => {
                return Err(StoreProtocolError::DeviceStateMismatch)
            }
            StoreDeviceRegistrationActivation::Join {
                attempt_id,
                outcome,
            } => StoreDeviceRegistrationActivationRef::Join {
                attempt_id: *attempt_id,
                outcome: outcome.clone(),
            },
            StoreDeviceRegistrationActivation::Recovery { recovery_id, node } => {
                StoreDeviceRegistrationActivationRef::Recovery {
                    recovery_id: *recovery_id,
                    node: node.clone(),
                }
            }
        };
        Ok(ActivatedStoreDeviceRegistrationRef {
            registration: self.registration.reference().clone(),
            authority,
        })
    }

    pub(crate) fn registration(&self) -> &ReferencedStoreDeviceRegistration {
        &self.registration
    }

    pub(crate) fn reference(&self) -> &StoreDeviceRegistrationRef {
        self.registration.reference()
    }

    pub(crate) fn value(&self) -> &StoreDeviceRegistration {
        self.registration.value()
    }

    pub(crate) fn activation(&self) -> &StoreDeviceRegistrationActivation {
        &self.activation
    }

    pub(crate) fn recovery_cursor(
        &self,
    ) -> Result<Option<OwnerRecoveryCursor>, StoreProtocolError> {
        match (&self.registration.value().origin, &self.activation) {
            (
                StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id,
                    recovery_slot,
                    owner_grant,
                },
                StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: activated_recovery_id,
                    node,
                },
            ) if recovery_id == activated_recovery_id
                && recovery_slot == node.object.slot()
                && owner_grant == &node.owner_grant =>
            {
                Ok(Some(OwnerRecoveryCursor {
                    owner_grant: owner_grant.clone(),
                    position: OwnerRecoveryPosition::At { node: node.clone() },
                }))
            }
            (
                StoreDeviceRegistrationOrigin::Join {
                    attempt_id,
                    outcome_slot,
                    ..
                },
                StoreDeviceRegistrationActivation::Join {
                    attempt_id: activated_attempt_id,
                    outcome,
                },
            ) if attempt_id == activated_attempt_id && outcome_slot == outcome.slot() => Ok(None),
            (
                StoreDeviceRegistrationOrigin::Founder { .. },
                StoreDeviceRegistrationActivation::Founder { .. },
            ) => Ok(None),
            _ => Err(StoreProtocolError::Malformed(
                "registration origin differs from its exact activation authority".to_string(),
            )),
        }
    }
}

impl<'de> Deserialize<'de> for ActivatedStoreDeviceRegistration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct EncodedActivation {
            registration: ReferencedStoreDeviceRegistration,
            activation: StoreDeviceRegistrationActivation,
        }

        let encoded = EncodedActivation::deserialize(deserializer)?;
        Self::verified(encoded.registration, encoded.activation).map_err(serde::de::Error::custom)
    }
}

impl StoreDeviceRegistration {
    fn device_stream_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
        anchor: &DeviceStreamAnchor,
    ) -> Result<StreamActivation, StoreProtocolError> {
        reference.verify_registration(self)?;
        Ok(StreamActivation::device_authorized(
            self.store_root.store_root_hash,
            reference.clone(),
            anchor.clone(),
        ))
    }

    pub fn store_announcement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.store_commits)
    }

    pub fn store_acknowledgement_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.acknowledgements)
    }

    pub fn store_snapshot_activation(
        &self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<StreamActivation, StoreProtocolError> {
        self.device_stream_activation(reference, &self.snapshots)
    }

    pub fn signed(
        store_root: StoreRootRef,
        origin: StoreDeviceRegistrationOrigin,
        provider: ProviderDeviceBinding,
        store_commits: DeviceStreamAnchor,
        acknowledgements: DeviceStreamAnchor,
        snapshots: DeviceStreamAnchor,
        identity_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_registration_anchors(&store_commits, &acknowledgements, &snapshots)?;
        let author_pubkey = keys::public_key_hex(identity_signer);
        let device_signer = derive_device_signer(identity_signer, &store_root, &origin);
        let device_signing_pubkey = keys::public_key_hex(&device_signer);
        let device_id = StoreDeviceId::derive(&store_root, &origin);
        Ok(Signed::sign(
            StoreDeviceRegistrationBody {
                store_root,
                device_id,
                author_pubkey,
                device_signing_pubkey,
                origin,
                provider,
                store_commits,
                acknowledgements,
                snapshots,
            },
            identity_signer,
        ))
    }

    pub(crate) fn device_signer(
        &self,
        identity_signer: &UserKeypair,
    ) -> Result<UserKeypair, StoreProtocolError> {
        if keys::public_key_hex(identity_signer) != self.author_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        let signer = derive_device_signer(identity_signer, &self.store_root, &self.origin);
        if keys::public_key_hex(&signer) != self.device_signing_pubkey {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(signer)
    }

    pub fn registration_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected_device: StoreDeviceId,
    ) -> Result<Self, StoreProtocolError> {
        let registration: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        registration.require_version()?;
        if &registration.store_root != expected_store_root {
            return Err(StoreProtocolError::StoreRootMismatch {
                expected: expected_store_root.store_root_hash,
                actual: registration.store_root.store_root_hash,
            });
        }
        if registration.device_id != expected_device {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: registration_slot_prefix(&expected_device.to_string()),
                actual: registration_slot_prefix(&registration.device_id.to_string()),
            });
        }
        if registration.device_id
            != StoreDeviceId::derive(&registration.store_root, &registration.origin)
        {
            return Err(StoreProtocolError::Malformed(
                "Store device id differs from its root and origin".to_string(),
            ));
        }
        validate_registration_anchors(
            &registration.store_commits,
            &registration.acknowledgements,
            &registration.snapshots,
        )?;
        let author_pubkey = registration.author_pubkey.clone();
        registration.verify_by(&author_pubkey)?;
        Ok(registration)
    }
}

fn derive_device_signer(
    identity_signer: &UserKeypair,
    store_root: &StoreRootRef,
    origin: &StoreDeviceRegistrationOrigin,
) -> UserKeypair {
    const DOMAIN: &[u8] = b"coven.store-device-signing-key.v1\0";
    let context = serde_json::to_vec(&(store_root, origin))
        .expect("Store device signing context serialization cannot fail");
    identity_signer.derive_signing_key(DOMAIN, &context)
}

fn validate_registration_anchors(
    commits: &DeviceStreamAnchor,
    acknowledgements: &DeviceStreamAnchor,
    snapshots: &DeviceStreamAnchor,
) -> Result<(), StoreProtocolError> {
    if !matches!(
        acknowledgements,
        DeviceStreamAnchor::StoreAcknowledgements { .. }
    ) || !matches!(snapshots, DeviceStreamAnchor::StoreSnapshots { .. })
        || !matches!(commits, DeviceStreamAnchor::StoreAnnouncements { .. })
    {
        return Err(StoreProtocolError::Malformed(
            "Store device registration contains mismatched permanent stream anchors".to_string(),
        ));
    }
    Ok(())
}
