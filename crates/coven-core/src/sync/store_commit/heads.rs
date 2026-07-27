use super::validation::require_version;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDeviceHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub commit: StoreBatchCommitRef,
    pub history_summary: ObjectHash,
    pub successor: SuccessorLink,
    pub signature: String,
}

#[derive(Serialize)]
struct HeadSignedFields<'a> {
    version: u32,
    store_root_hash: ObjectHash,
    author_registration: &'a StoreDeviceRegistrationRef,
    commit: &'a StoreBatchCommitRef,
    history_summary: ObjectHash,
    successor: &'a SuccessorLink,
}

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
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            author_registration,
            commit,
            history_summary,
            successor,
            signature: String::new(),
        };
        let (_, signature) = keys::sign_hex(signer, &head.canonical_signed_bytes());
        head.signature = signature;
        Ok(head)
    }

    pub(crate) fn canonical_signed_bytes(&self) -> Vec<u8> {
        domain_json(
            HEAD_DOMAIN,
            &HeadSignedFields {
                version: self.version,
                store_root_hash: self.store_root_hash,
                author_registration: &self.author_registration,
                commit: &self.commit,
                history_summary: self.history_summary,
                successor: &self.successor,
            },
        )
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StoreDeviceHead serialization cannot fail")
    }

    pub fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(&self.canonical_signed_bytes())
    }

    pub fn slot_sequence(&self) -> u64 {
        self.commit.coord.sequence()
    }

    pub(crate) fn signature_is_valid_for(
        &self,
        expected_registration: &StoreDeviceRegistration,
    ) -> bool {
        keys::verify_signature_hex(
            &expected_registration.device_signing_pubkey,
            &self.signature,
            &self.canonical_signed_bytes(),
        )
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected_registration: &StoreDeviceRegistration,
        expected_ref: &StoreBatchCommitRef,
    ) -> Result<Self, StoreProtocolError> {
        let head: Self = serde_json::from_slice(bytes)
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
        require_version(head.version)?;
        crate::sync::store_objects::verify_store_root(
            expected_store_root_hash,
            head.store_root_hash,
        )?;
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
