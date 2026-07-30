use super::validation::{require_version, validate_commit_frontier, validate_successor_sequence};
use super::*;

/// One device's signed acknowledgement of the exact private Circle history it
/// currently holds, encrypted to the Circle epoch key it names. Store members
/// outside the Circle observe only the object's shape and timing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleAck {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub registration: StoreDeviceRegistrationRef,
    pub sequence: u64,
    /// The device's accepted Store frontier at staging time — Circle packages
    /// are activated by Store commits, so Circle coverage IS a Store frontier.
    pub store_cut: CommitFrontier,
    /// The exact activated control and epoch the device's live projection
    /// derives from.
    pub control: CircleControlCoord,
    pub epoch_id: CircleEpochId,
    pub key_fingerprint: KeyFingerprint,
    /// The exact coverage the device's projection was seeded from: the retained
    /// bootstrap coverage row (control, activating commit, exact cut, image
    /// hash). `None` exactly for a founder/source device whose projection never
    /// came from an image.
    pub seeded_from: Option<CircleBootstrapCoverageRef>,
    pub last_sync: String,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAckRef {
    pub registration: StoreDeviceRegistrationRef,
    pub circle_id: CircleId,
    pub sequence: u64,
    pub ack_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Serialize)]
struct CircleAckSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    registration: &'a StoreDeviceRegistrationRef,
    sequence: u64,
    store_cut: &'a CommitFrontier,
    control: &'a CircleControlCoord,
    epoch_id: CircleEpochId,
    key_fingerprint: KeyFingerprint,
    seeded_from: Option<&'a CircleBootstrapCoverageRef>,
    last_sync: &'a str,
    successor: &'a SuccessorLink,
}

impl CircleAck {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn signed(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        registration: StoreDeviceRegistrationRef,
        sequence: u64,
        store_cut: CommitFrontier,
        control: CircleControlCoord,
        epoch_id: CircleEpochId,
        key_fingerprint: KeyFingerprint,
        seeded_from: Option<CircleBootstrapCoverageRef>,
        last_sync: String,
        successor: SuccessorLink,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        validate_successor_sequence(sequence, &successor)?;
        validate_circle_ack_state(&store_cut, &control, &seeded_from, circle_id)?;
        let mut ack = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            registration,
            sequence,
            store_cut,
            control,
            epoch_id,
            key_fingerprint,
            seeded_from,
            last_sync,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(device_signer, &ack.canonical_signed_bytes());
        ack.signature = signature;
        Ok(ack)
    }

    fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            CIRCLE_ACK_DOMAIN,
            &CircleAckSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                circle_id: self.circle_id,
                registration: &self.registration,
                sequence: self.sequence,
                store_cut: &self.store_cut,
                control: &self.control,
                epoch_id: self.epoch_id,
                key_fingerprint: self.key_fingerprint,
                seeded_from: self.seeded_from.as_ref(),
                last_sync: &self.last_sync,
                successor: &self.successor,
            },
        )
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("CircleAck serialization cannot fail")
    }

    pub(crate) fn ack_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    /// Verify one exact Circle acknowledgement against its expected reference and
    /// author registration. The successor's stream activation is not recomputed
    /// here: a Circle-acknowledgement stream's first slot is not carried by the
    /// author's registration (unlike a Store-acknowledgement stream), so only
    /// the author that holds it can reproduce the activation. A reader trusts
    /// the Store commit that named this acknowledgement as the sole activation
    /// authority, and checks the predecessor/sequence chain for ordering.
    pub(crate) fn parse_at(
        bytes: &[u8],
        expected_store_root: &StoreRootRef,
        expected: &CircleAckRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let ack: Self = crate::storage::decode_protocol_object(bytes)?;
        require_version(ack.version)?;
        crate::storage::verify_store_root(
            expected_store_root.store_root_hash,
            ack.store_root_hash,
        )?;
        ack.registration.verify_registration(author)?;
        if ack.registration != expected.registration {
            return Err(StoreProtocolError::DeviceRegistrationRefMismatch {
                device_id: expected.registration.device_id.to_string(),
                expected: expected.registration.registration_hash,
                actual: ack.registration.registration_hash,
            });
        }
        if ack.circle_id != expected.circle_id {
            return Err(StoreProtocolError::Malformed(
                "Circle acknowledgement names another Circle".to_string(),
            ));
        }
        if ack.sequence != expected.sequence {
            return Err(StoreProtocolError::RelocatedSlot {
                expected: circle_ack_slot_prefix(
                    expected.circle_id,
                    &author.device_id.to_string(),
                    expected.sequence,
                ),
                actual: circle_ack_slot_prefix(
                    ack.circle_id,
                    &author.device_id.to_string(),
                    ack.sequence,
                ),
            });
        }
        validate_successor_sequence(ack.sequence, &ack.successor)?;
        validate_circle_ack_state(
            &ack.store_cut,
            &ack.control,
            &ack.seeded_from,
            ack.circle_id,
        )?;
        if !keys::verify_signature_hex(
            &author.device_signing_pubkey,
            &ack.signature,
            &ack.canonical_signed_bytes(),
        ) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        if ack.ack_hash() != expected.ack_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.ack_hash,
                actual: ack.ack_hash(),
            });
        }
        Ok(ack)
    }
}

fn validate_circle_ack_state(
    store_cut: &CommitFrontier,
    control: &CircleControlCoord,
    seeded_from: &Option<CircleBootstrapCoverageRef>,
    circle_id: CircleId,
) -> Result<(), StoreProtocolError> {
    validate_commit_frontier(store_cut)?;
    control
        .validate()
        .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
    if let Some(seeded_from) = seeded_from {
        if seeded_from.circle_id != circle_id {
            return Err(StoreProtocolError::Malformed(
                "Circle acknowledgement seed coverage names another Circle".to_string(),
            ));
        }
    }
    Ok(())
}
