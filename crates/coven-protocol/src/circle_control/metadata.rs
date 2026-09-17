use super::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataCoord {
    pub author_pubkey: String,
    pub device_id: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub metadata_hash: ObjectHash,
}

impl CircleMetadataCoord {
    pub fn stream_key(&self) -> CircleAuthorStreamKey {
        CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
        }
    }
}

/// The wire body of one Circle metadata entry. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleMetadataBody {
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
    pub author_owner_grant: MembershipGrantId,
    pub author_roster: CircleRosterStateRef,
    pub key_fingerprint: KeyFingerprint,
}

impl SignedBody for CircleMetadataBody {
    const DOMAIN: &'static [u8] = METADATA_DOMAIN;
}

pub type CircleMetadata = Signed<CircleMetadataBody>;

impl CircleMetadata {
    pub(super) fn founder(
        store_root_hash: ObjectHash,
        circle_id: CircleId,
        epoch_id: CircleEpochId,
        name: &str,
        metadata_stamp: &str,
        device_id: &str,
        owner_grant: MembershipGrantId,
        author_roster: CircleRosterStateRef,
        key_fingerprint: KeyFingerprint,
        signer: &dyn coven_keys::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        if name.trim().is_empty() {
            return Err(CircleTransitionError::EmptyName);
        }
        Ok(Signed::sign(
            CircleMetadataBody {
                store_root_hash,
                circle_id,
                epoch_id,
                name: name.to_string(),
                seq: 1,
                previous_hash: None,
                dependencies: Vec::new(),
                metadata_stamp: metadata_stamp.to_string(),
                author_pubkey: keys::public_key_hex(signer),
                device_id: device_id.to_string(),
                author_owner_grant: owner_grant,
                author_roster,
                key_fingerprint,
            },
            signer,
        ))
    }

    pub fn metadata_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn coord(&self) -> CircleMetadataCoord {
        CircleMetadataCoord {
            author_pubkey: self.author_pubkey.clone(),
            device_id: self.device_id.clone(),
            author_owner_grant: self.author_owner_grant.clone(),
            seq: self.seq,
            metadata_hash: self.metadata_hash(),
        }
    }

    pub fn verify(&self) -> bool {
        let position_is_valid = crate::causal_grants::author_stream_position_is_valid(
            self.seq,
            self.previous_hash,
            &self.coord().stream_key(),
            self.dependencies
                .iter()
                .map(CircleMetadataCoord::stream_key),
        );
        !self.name.trim().is_empty()
            && crate::hlc::Timestamp::parse(&self.metadata_stamp)
                .is_some_and(|stamp| stamp.to_string() == self.metadata_stamp)
            && position_is_valid
            && self
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            && self.verify_by(&self.author_pubkey).is_ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleMetadataStateRef {
    /// The raw author-stream frontier of the metadata history this control
    /// names: one coordinate per author stream, canonical by stream key.
    pub frontier: Vec<CircleMetadataCoord>,
    pub selected: CircleMetadataCoord,
    pub state_hash: ObjectHash,
}

pub type CircleMetadataStateRef = MergeCircleMetadataStateRef;
