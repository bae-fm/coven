use super::validation::{validate_commit_frontier, validate_successor_sequence};
use super::*;

/// One device's signed acknowledgement of the exact private Circle history it
/// currently holds, encrypted to the Circle epoch key it names. Store members
/// outside the Circle observe only the object's shape and timing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleAckBody {
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
}

impl SignedBody for CircleAckBody {
    const DOMAIN: &'static [u8] = CIRCLE_ACK_DOMAIN;
}

pub(crate) type CircleAck = Signed<CircleAckBody>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleAckRef {
    pub registration: StoreDeviceRegistrationRef,
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub sequence: u64,
    pub ack_hash: ObjectHash,
    pub object: ExactObjectRef,
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
        Ok(Signed::sign(
            CircleAckBody {
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
            },
            device_signer,
        ))
    }

    pub(crate) fn ack_hash(&self) -> ObjectHash {
        self.hash()
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
        let ack: Self = crate::protocol::objects::decode_protocol_object(bytes)?;
        ack.require_version()?;
        crate::protocol::objects::verify_store_root(
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
        if ack.control != expected.control {
            return Err(StoreProtocolError::Malformed(
                "Circle acknowledgement names another control".to_string(),
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
        ack.verify_by(&author.device_signing_pubkey)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circle_ack_reference_names_its_encryption_control() {
        let control = CircleControlCoord {
            device_id: "circle-ack-author".to_string(),
            stream_id: crate::protocol::causal_grants::AuthorStreamId::from_digest(
                ObjectHash::digest(b"circle-ack-stream"),
            ),
            author_pubkey: "circle-ack-author-pubkey".to_string(),
            author_owner_grant: crate::protocol::causal_grants::MembershipGrantId::from_test_label(
                "circle-ack-owner",
            ),
            seq: 1,
            control_hash: ObjectHash::digest(b"circle-ack-control"),
        };
        let object = ExactObjectRef::new(
            crate::protocol::objects::ObjectSlot::logical(
                "circles/ack-test/acknowledgements/device/1.json".to_string(),
            )
            .expect("valid acknowledgement slot"),
            1,
            ObjectHash::digest(b"ack"),
        );
        let reference = CircleAckRef {
            registration: StoreDeviceRegistrationRef {
                device_id: ObjectHash::digest(b"circle-ack-device")
                    .to_string()
                    .parse()
                    .expect("digest is a valid device id"),
                registration_hash: ObjectHash::digest(b"circle-ack-registration"),
                object: object.clone(),
            },
            circle_id: CircleId::from_bytes([1; 16]),
            control: control.clone(),
            sequence: 1,
            ack_hash: ObjectHash::digest(b"circle-ack"),
            object,
        };

        let encoded = serde_json::to_value(reference).expect("serialize acknowledgement ref");
        assert_eq!(
            encoded.get("control"),
            Some(&serde_json::to_value(control).expect("serialize control"))
        );
    }
}
