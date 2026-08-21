use super::*;

/// The exact coordinate of one published membership rollup.
///
/// `rollup_hash` is the digest of the rollup's canonical bytes — the same
/// identity a snapshot image reference carries, and for the same reason: a
/// reader that fetches the object this names can tell whether it got the bytes
/// the snapshot meant before it looks at anything inside.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRollupRef {
    pub rollup_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// One membership head and the entry it selects, carried by value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRollupHead {
    pub head: MembershipHeadRef,
    pub head_value: AuthorHead,
    pub entry: MembershipEntryRef,
    pub entry_value: MembershipEntry,
}

/// One conflict resolution the carried heads depend on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRollupResolution {
    pub resolution: StoreMembershipConflictResolutionRef,
    pub resolution_value: StoreMembershipConflictResolution,
}

/// One author stream's heads, in sequence order from the stream's anchor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRollupStream {
    pub author_pubkey: String,
    pub author_owner_grant: MembershipGrantId,
    pub stream_id: AuthorStreamId,
    pub heads: Vec<MembershipRollupHead>,
}

/// Every membership object a reader needs to reach one membership frontier,
/// carried in one object.
///
/// The membership chain is hash-linked per author stream, so a reader has to
/// verify it in order — but it does not have to *fetch* it in order, and it
/// does not have to fetch it one object at a time. A device joining a Store
/// with a few dozen membership changes spent two provider round trips per
/// change discovering and reading objects that had not moved in months, which
/// on a live store was about eighty percent of the whole join.
///
/// This carries all of them. Nothing in it is believed: a reader takes the
/// bytes, keys them by the slot and the content address they claim, and then
/// runs the identical anchored-chain walk it would have run over its own
/// reads — same signature checks, same predecessor linkage, same Store-commit
/// activation for authority changes, same conflict-resolution layering. A
/// rollup that is stale costs the reader the tail it does not cover; a rollup
/// that is wrong is refused here and the reader walks the provider exactly as
/// it did before.
///
/// It is published beside a snapshot and named by the signed snapshot metadata,
/// which is what makes it discoverable before a joining device has opened the
/// Store keyring — the membership chain is what *opens* that keyring, so
/// nothing a joiner needs to read it can live behind it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipRollupBody {
    pub store_root_hash: ObjectHash,
    pub author_registration: StoreDeviceRegistrationRef,
    pub streams: Vec<MembershipRollupStream>,
    pub resolutions: Vec<MembershipRollupResolution>,
}

impl SignedBody for MembershipRollupBody {
    const DOMAIN: &'static [u8] = MEMBERSHIP_ROLLUP_DOMAIN;
}

pub type MembershipRollup = Signed<MembershipRollupBody>;

impl MembershipRollup {
    pub fn signed(
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        streams: Vec<MembershipRollupStream>,
        resolutions: Vec<MembershipRollupResolution>,
        device_signer: &UserKeypair,
    ) -> Result<Self, StoreProtocolError> {
        let rollup = Signed::sign(
            MembershipRollupBody {
                store_root_hash,
                author_registration,
                streams,
                resolutions,
            },
            device_signer,
        );
        rollup.validate_shape()?;
        Ok(rollup)
    }

    /// Everything about a rollup that can be checked without the chain: each
    /// carried object hashes to the reference that names it, carries its own
    /// author's signature, and sits at the coordinate its stream claims.
    ///
    /// This is deliberately not the whole of membership verification — grant
    /// authority, predecessor linkage across a conflict layer, and Store-commit
    /// activation are decided by the walk that consumes these bytes, over the
    /// same code path that decides them for bytes read off the provider. What
    /// this establishes is that the rollup is a faithful carrier: every object
    /// in it is the object its reference names.
    pub fn validate_shape(&self) -> Result<(), StoreProtocolError> {
        if self
            .streams
            .windows(2)
            .any(|pair| stream_key(&pair[0]) >= stream_key(&pair[1]))
        {
            return Err(StoreProtocolError::Malformed(
                "membership rollup streams are not canonical".to_string(),
            ));
        }
        for stream in &self.streams {
            if stream.heads.is_empty() {
                return Err(StoreProtocolError::Malformed(
                    "membership rollup carries an empty author stream".to_string(),
                ));
            }
            for (index, carried) in stream.heads.iter().enumerate() {
                let sequence = u64::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1))
                    .ok_or_else(|| {
                        StoreProtocolError::Malformed(
                            "membership rollup sequence overflow".to_string(),
                        )
                    })?;
                carried.validate_at(stream, sequence)?;
            }
        }
        for carried in &self.resolutions {
            let value = &carried.resolution_value;
            if value.store_root_hash != self.store_root_hash
                || !value.verify_signature()
                || value.resolution_ref(carried.resolution.object.clone()) != carried.resolution
            {
                return Err(StoreProtocolError::Malformed(
                    "membership rollup carries an unauthentic conflict resolution".to_string(),
                ));
            }
            carried
                .resolution
                .object
                .verify(&serde_json::to_vec(value)?)?;
        }
        Ok(())
    }

    pub fn parse_at(
        bytes: &[u8],
        expected_store_root_hash: ObjectHash,
        expected: &MembershipRollupRef,
        author: &StoreDeviceRegistration,
    ) -> Result<Self, StoreProtocolError> {
        let rollup: Self = crate::objects::decode_protocol_object(bytes)?;
        rollup.require_version()?;
        crate::objects::verify_store_root(expected_store_root_hash, rollup.store_root_hash)?;
        rollup.author_registration.verify_registration(author)?;
        rollup.validate_shape()?;
        rollup.verify_by(&author.device_signing_pubkey)?;
        let actual = ObjectHash::digest(bytes);
        if actual != expected.rollup_hash {
            return Err(StoreProtocolError::ObjectHashMismatch {
                expected: expected.rollup_hash,
                actual,
            });
        }
        Ok(rollup)
    }
}

impl MembershipRollupHead {
    fn validate_at(
        &self,
        stream: &MembershipRollupStream,
        sequence: u64,
    ) -> Result<(), StoreProtocolError> {
        let coord = self.head_value.entry_coord();
        if coord != self.head.coord
            || coord.author_pubkey != stream.author_pubkey
            || coord.author_owner_grant != stream.author_owner_grant
            || coord.stream_id != stream.stream_id
            || coord.seq != sequence
            || self.head.head_hash != self.head_value.head_hash()
            || self.head_value.body.entry != self.entry
            || self.entry.coord != self.entry_value.coord()
            || !verify_membership_entry(&self.entry_value)
        {
            return Err(StoreProtocolError::Malformed(format!(
                "membership rollup head {}/{}/{sequence} does not match its own reference",
                stream.author_pubkey, stream.stream_id
            )));
        }
        self.head
            .object
            .verify(&serde_json::to_vec(&self.head_value)?)?;
        self.entry
            .object
            .verify(&serde_json::to_vec(&self.entry_value)?)?;
        Ok(())
    }
}

fn stream_key(stream: &MembershipRollupStream) -> (&str, &MembershipGrantId, AuthorStreamId) {
    (
        &stream.author_pubkey,
        &stream.author_owner_grant,
        stream.stream_id,
    )
}
