use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataCoord {
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub metadata_hash: ObjectHash,
}

impl CircleMetadataCoord {
    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleMetadata {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub epoch_id: CircleEpochId,
    pub name: String,
    pub seq: u64,
    pub previous_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleMetadataCoord>,
    pub metadata_stamp: String,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub author_roster: CircleRosterStateRef,
    pub key_fingerprint: KeyFingerprint,
    pub signature: String,
}

impl CircleMetadata {
    pub(super) fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        name: &str,
        metadata_stamp: &str,
        device_id: &str,
        stream_id: AuthorStreamId,
        owner_grant: MembershipGrantId,
        author_roster: CircleRosterStateRef,
        key_fingerprint: KeyFingerprint,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        if name.trim().is_empty() {
            return Err(CircleTransitionError::EmptyName);
        }
        let author_pubkey = keys::public_key_hex(signer);
        let mut metadata = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            name: name.to_string(),
            seq: 1,
            previous_hash: None,
            dependencies: Vec::new(),
            metadata_stamp: metadata_stamp.to_string(),
            author_pubkey,
            device_id: device_id.to_string(),
            stream_id,
            author_owner_grant: owner_grant,
            author_roster,
            key_fingerprint,
            signature: String::new(),
        };
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;
        Ok(metadata)
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            epoch_id: CircleEpochId,
            name: &'a str,
            seq: u64,
            previous_hash: Option<ObjectHash>,
            dependencies: &'a [CircleMetadataCoord],
            metadata_stamp: &'a str,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            author_roster: &'a CircleRosterStateRef,
            key_fingerprint: KeyFingerprint,
        }
        serde_json::to_vec(&Signed {
            domain: METADATA_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            epoch_id: self.epoch_id,
            name: &self.name,
            seq: self.seq,
            previous_hash: self.previous_hash,
            dependencies: &self.dependencies,
            metadata_stamp: &self.metadata_stamp,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            author_roster: &self.author_roster,
            key_fingerprint: self.key_fingerprint,
        })
        .expect("circle metadata serialization cannot fail")
    }

    pub(crate) fn metadata_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata serialization cannot fail"),
        )
    }

    pub(crate) fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.metadata_hash(),
        }
    }

    pub(crate) fn verify(&self) -> bool {
        let position_is_valid = (self.seq == 1
            && self.previous_hash.is_none()
            && self
                .dependencies
                .iter()
                .all(|dependency| dependency.stream_key() != self.coord().stream_key()))
            || (self.seq > 1 && self.previous_hash.is_some());
        self.version == STORE_PROTOCOL_VERSION
            && !self.name.trim().is_empty()
            && position_is_valid
            && self
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            && keys::verify_signature_hex(
                &self.author_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CircleMetadataHead {
    pub version: u32,
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub author_pubkey: String,
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub tip_hash: ObjectHash,
    pub tip: ExactObjectRef,
    pub successor: SuccessorLink,
    pub signature: String,
}

impl CircleMetadataHead {
    pub(crate) fn signed(
        metadata: &CircleMetadata,
        tip: ExactObjectRef,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Self {
        let mut head = Self {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash: metadata.store_root_hash,
            circle_id: metadata.circle_id,
            author_pubkey: metadata.author_pubkey.clone(),
            device_id: metadata.device_id.clone(),
            stream_id: metadata.stream_id,
            author_owner_grant: metadata.author_owner_grant.clone(),
            seq: metadata.seq,
            tip_hash: metadata.metadata_hash(),
            tip,
            successor,
            signature: String::new(),
        };
        head.signature = keys::sign_hex(signer, &head.canonical_bytes()).1;
        head
    }

    pub(crate) fn canonical_bytes(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct Signed<'a> {
            domain: &'static str,
            version: u32,
            store_root_hash: ObjectHash,
            circle_id: CircleId,
            author_pubkey: &'a str,
            device_id: &'a str,
            stream_id: AuthorStreamId,
            author_owner_grant: &'a MembershipGrantId,
            seq: u64,
            tip_hash: ObjectHash,
            tip: &'a ExactObjectRef,
            successor: &'a SuccessorLink,
        }
        serde_json::to_vec(&Signed {
            domain: METADATA_HEAD_DOMAIN,
            version: self.version,
            store_root_hash: self.store_root_hash,
            circle_id: self.circle_id,
            author_pubkey: &self.author_pubkey,
            device_id: &self.device_id,
            stream_id: self.stream_id,
            author_owner_grant: &self.author_owner_grant,
            seq: self.seq,
            tip_hash: self.tip_hash,
            tip: &self.tip,
            successor: &self.successor,
        })
        .expect("circle metadata head serialization cannot fail")
    }

    pub(crate) fn head_hash(&self) -> ObjectHash {
        ObjectHash::digest(
            &serde_json::to_vec(self).expect("circle metadata head serialization cannot fail"),
        )
    }

    pub(crate) fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            stream_id: self.stream_id,
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.tip_hash,
        }
    }

    pub(crate) fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        self.version == STORE_PROTOCOL_VERSION
            && self.seq > 0
            && !self.device_id.is_empty()
            && self.device_id == registration.device_id.to_string()
            && keys::verify_signature_hex(
                &registration.device_signing_pubkey,
                &self.signature,
                &self.canonical_bytes(),
            )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataHeadRef {
    pub coord: CircleMetadataCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

impl CircleMetadataHeadRef {
    pub(crate) fn from_stored_head(head: &CircleMetadataHead, object: ExactObjectRef) -> Self {
        Self {
            coord: head.coord(),
            head_hash: head.head_hash(),
            object,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MergeCircleMetadataStateRef {
    pub heads: Vec<CircleMetadataHeadRef>,
    pub selected: CircleMetadataCoord,
    pub state_hash: ObjectHash,
}

pub(crate) type CircleMetadataStateRef = MergeCircleMetadataStateRef;
