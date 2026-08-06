use super::*;

/// The wire body of one device's Store head. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHeadBody {
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub commit: StoreBatchCommitRef,
    pub history_summary: ObjectHash,
    pub successor: SuccessorLink,
}

impl SignedBody for StoreDeviceHeadBody {
    const DOMAIN: &'static [u8] = HEAD_DOMAIN;
}

pub type StoreDeviceHead = Signed<StoreDeviceHeadBody>;

impl StoreDeviceHead {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        commit: StoreBatchCommitRef,
        history_summary: ObjectHash,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        if commit.coord.sequence() == 0 {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        Ok(Signed::sign(
            StoreDeviceHeadBody {
                store_root_hash,
                author_registration,
                commit,
                history_summary,
                successor,
            },
            signer,
        ))
    }

    pub fn head_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn slot_sequence(&self) -> u64 {
        self.commit.coord.sequence()
    }

    pub fn signature_is_valid_for(&self, expected_registration: &StoreDeviceRegistration) -> bool {
        self.verify_by(&expected_registration.device_signing_pubkey)
            .is_ok()
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected_registration: &StoreDeviceRegistration,
        expected_ref: &StoreBatchCommitRef,
    ) -> Result<Self, StoreProtocolError> {
        let head: Self = crate::objects::decode_protocol_object(bytes)?;
        head.require_version()?;
        crate::objects::verify_store_root(expected_store_root_hash, head.store_root_hash)?;
        head.author_registration
            .verify_registration(expected_registration)?;
        if &head.commit != expected_ref {
            return Err(StoreProtocolError::Malformed(
                "Store head activates a different exact commit".to_string(),
            ));
        }
        if head.commit.coord.sequence() == 0 {
            return Err(StoreProtocolError::InvalidSequence(0));
        }
        if !head.signature_is_valid_for(expected_registration) {
            return Err(StoreProtocolError::InvalidSignature);
        }
        Ok(head)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHeadRef {
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}
