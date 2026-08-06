use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleControlOrder {
    pub device_id: String,
    pub stream_id: AuthorStreamId,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_control_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleControlCoord>,
}

/// A terminal deletion. It freezes the epoch spine it terminated — the same
/// `MergeActiveCircleEpoch` an `EpochClose` freezes — so historical package
/// verification and exact reclamation keep the epoch, key fingerprint, and
/// roster-head spine with no live access material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletedCircle {
    pub frozen_epoch: MergeActiveCircleEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum CircleControlState {
    ActiveEpoch(MergeActiveCircleEpoch),
    EpochClose(CircleEpochClose),
    Deleted(DeletedCircle),
}

impl CircleControlState {
    pub fn access_epoch(&self) -> &MergeActiveCircleEpoch {
        match self {
            Self::ActiveEpoch(active) => active,
            Self::EpochClose(close) => &close.frozen_epoch,
            Self::Deleted(deleted) => &deleted.frozen_epoch,
        }
    }

    pub fn access_epoch_mut(&mut self) -> &mut MergeActiveCircleEpoch {
        match self {
            Self::ActiveEpoch(active) => active,
            Self::EpochClose(close) => &mut close.frozen_epoch,
            Self::Deleted(deleted) => &mut deleted.frozen_epoch,
        }
    }

    pub fn active_epoch(&self) -> Option<&MergeActiveCircleEpoch> {
        match self {
            Self::ActiveEpoch(active) => Some(active),
            Self::EpochClose(_) | Self::Deleted(_) => None,
        }
    }

    pub fn active_epoch_mut(&mut self) -> Option<&mut MergeActiveCircleEpoch> {
        match self {
            Self::ActiveEpoch(active) => Some(active),
            Self::EpochClose(_) | Self::Deleted(_) => None,
        }
    }

    pub fn is_deleted(&self) -> bool {
        matches!(self, Self::Deleted(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleControlHeadRef {
    pub coord: CircleControlCoord,
    pub head_hash: ObjectHash,
    pub object: ExactObjectRef,
}

/// One losing branch of a resolved control conflict, carried so the resolution
/// can cover every branch's frontier rather than only the chosen branch's: the
/// branch's control head, its metadata and roster head frontiers, and the
/// metadata entry that branch selected. The resolution unions these into its own
/// frontier so no author-stream head is re-allocated once the conflict collapses,
/// and re-derives its name as the deterministic metadata selection across the
/// union.
#[derive(Debug, Clone)]
pub struct ResolvedConflictBranch {
    pub control_head: MergeCircleControlHeadRef,
    pub metadata_heads: Vec<CircleMetadataHeadRef>,
    pub roster_heads: Vec<CircleRosterHeadRef>,
    pub selected_metadata: CircleMetadata,
}

/// Insert `head` into a frontier keyed by author stream, keeping the deeper
/// (higher-sequence) head when the stream already carries one. Merging every
/// conflicting branch's heads this way yields the union frontier: each stream is
/// covered at its deepest position across all branches, so a device that authored
/// on that stream continues from its own head instead of re-allocating it.
pub fn merge_frontier_head<H>(
    frontier: &mut Vec<H>,
    head: H,
    stream_key: impl Fn(&H) -> CircleAuthorStreamKey,
    seq: impl Fn(&H) -> u64,
) {
    let key = stream_key(&head);
    match frontier
        .iter_mut()
        .find(|existing| stream_key(existing) == key)
    {
        Some(existing) if seq(&head) > seq(existing) => *existing = head,
        Some(_) => {}
        None => frontier.push(head),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeCircleOwnerAuthorityRef {
    Roster {
        roster: MergeCircleRosterStateRef,
        grant_id: MembershipGrantId,
        created_at: crate::circle_roster::CircleRosterCoord,
    },
    ConflictResolution {
        conflict_hash: ObjectHash,
        resolution_hash: ObjectHash,
    },
}

impl MergeCircleOwnerAuthorityRef {
    pub(crate) fn grant_id(&self, author_pubkey: &str) -> MembershipGrantId {
        match self {
            Self::Roster { grant_id, .. } => grant_id.clone(),
            Self::ConflictResolution { conflict_hash, .. } => {
                crate::circle_roster::derive_circle_resolution_grant(conflict_hash, author_pubkey)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlValue {
    pub order: MergeCircleControlOrder,
    pub state: CircleControlState,
    pub author_authority: MergeCircleOwnerAuthorityRef,
    pub membership_authority: MembershipGrantCreationAuthority,
}

/// The wire body of one Circle control. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub value: CircleControlValue,
    pub author_pubkey: String,
}

impl SignedBody for CircleControlBody {
    const DOMAIN: &'static [u8] = CONTROL_DOMAIN;
}

pub type CircleControl = Signed<CircleControlBody>;

impl CircleControlBody {
    pub fn state(&self) -> &CircleControlState {
        &self.value.state
    }

    pub fn active_epoch(&self) -> Option<&MergeActiveCircleEpoch> {
        self.value.state.active_epoch()
    }

    pub fn access_epoch(&self) -> &MergeActiveCircleEpoch {
        self.value.state.access_epoch()
    }

    pub fn active_common(&self) -> &ActiveCircleEpochCore {
        &self.access_epoch().common
    }

    pub fn epoch_id(&self) -> CircleEpochId {
        self.active_common().epoch_id
    }

    pub fn key_fingerprint(&self) -> KeyFingerprint {
        self.active_common().key_fingerprint
    }

    pub fn owners(&self) -> &[String] {
        &self.active_common().owners
    }

    pub(crate) fn access_root(&self) -> ObjectHash {
        self.active_common().access_root
    }

    pub fn roster_state_ref(&self) -> CircleRosterStateRef {
        self.access_epoch().roster.clone()
    }

    pub fn metadata_state_ref(&self) -> CircleMetadataStateRef {
        self.access_epoch().metadata.clone()
    }

    pub fn store_membership_state_ref(&self) -> StoreMembershipStateRef {
        self.access_epoch().store_membership.clone()
    }

    pub fn previous_control_hash(&self) -> Option<ObjectHash> {
        self.value.order.previous_control_hash
    }

    pub fn is_founder(&self) -> bool {
        self.value.order.seq == 1
            && self.value.order.previous_control_hash.is_none()
            && self.value.order.dependencies.is_empty()
    }

    pub(crate) fn ordinal(&self) -> u64 {
        self.value.order.seq
    }

    pub fn author_grant_id(&self) -> MembershipGrantId {
        self.value.author_authority.grant_id(&self.author_pubkey)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn membership_authority(&self) -> &MembershipGrantCreationAuthority {
        &self.value.membership_authority
    }
}

impl CircleControl {
    pub fn control_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn causally_covers(&self, prior: &Self) -> bool {
        if self.store_root_hash != prior.store_root_hash || self.circle_id != prior.circle_id {
            return false;
        }
        self.value.order.previous_control_hash == Some(prior.control_hash())
            || self
                .value
                .order
                .dependencies
                .binary_search(&prior.coord())
                .is_ok()
    }

    pub fn verify(&self) -> bool {
        let order = &self.value.order;
        let access_epoch = self.access_epoch();
        let author_authority = &self.value.author_authority;
        let grant_id = author_authority.grant_id(&self.author_pubkey);
        let stream_key = CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: order.device_id.clone(),
            stream_id: order.stream_id,
            author_owner_grant: order.author_owner_grant.clone(),
        };
        let covered_are_canonical = access_epoch
            .covered_control_heads
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key());
        let own_predecessor = access_epoch
            .covered_control_heads
            .iter()
            .find(|head| head.coord.stream_key() == stream_key);
        let expected_dependencies = access_epoch
            .covered_control_heads
            .iter()
            .filter(|head| head.coord.stream_key() != stream_key)
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>();
        let order_is_valid = !order.device_id.is_empty()
            && order.seq > 0
            && order.author_owner_grant == grant_id
            && covered_are_canonical
            && order.dependencies == expected_dependencies;
        let authority_is_founder_roster = matches!(
            author_authority,
            MergeCircleOwnerAuthorityRef::Roster { roster, .. }
                if roster == &access_epoch.roster
        );
        let founder = order.seq == 1 && access_epoch.covered_control_heads.is_empty();
        let continuity_is_valid = match (order.seq, own_predecessor) {
            (1, None) => order.previous_control_hash.is_none(),
            (seq, Some(predecessor)) if seq > 1 => {
                predecessor.coord.seq.checked_add(1) == Some(seq)
                    && order.previous_control_hash == Some(predecessor.coord.control_hash)
            }
            _ => false,
        };
        let founder_identity_is_valid = !founder
            || (authority_is_founder_roster
                && self.circle_id
                    == CircleId::founder(self.store_root_hash, &self.author_pubkey, &grant_id));
        let common = &access_epoch.common;
        let owners_are_canonical =
            !common.owners.is_empty() && common.owners.windows(2).all(|pair| pair[0] < pair[1]);
        let origin_is_valid = match &common.origin {
            CircleEpochOrigin::Founder => true,
            CircleEpochOrigin::Closed { cutoff, .. } => {
                crate::store_commit::validate_commit_frontier(cutoff).is_ok()
            }
        };
        let state_is_valid = match &self.value.state {
            CircleControlState::ActiveEpoch(_) => true,
            CircleControlState::EpochClose(close) => !founder && close.verify_shape(self.circle_id),
            // A deletion is always a successor of a live control; the frozen
            // epoch it carries is validated by the shared access-epoch checks
            // above.
            CircleControlState::Deleted(_) => !founder,
        };
        owners_are_canonical
            && origin_is_valid
            && state_is_valid
            && order_is_valid
            && continuity_is_valid
            && founder_identity_is_valid
            && self.verify_by(&self.author_pubkey).is_ok()
    }

    pub fn coord(&self) -> CircleControlCoord {
        let order = &self.value.order;
        CircleControlCoord {
            device_id: order.device_id.clone(),
            stream_id: order.stream_id,
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: order.author_owner_grant.clone(),
            seq: order.seq,
            control_hash: self.control_hash(),
        }
    }
}

/// The wire body of one Circle control head. Every field here is signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlHeadBody {
    pub store_root_hash: ObjectHash,
    pub circle_id: CircleId,
    pub control: CircleControlCoord,
    pub entry: ExactObjectRef,
    pub successor: SuccessorLink,
}

impl SignedBody for CircleControlHeadBody {
    const DOMAIN: &'static [u8] = CONTROL_HEAD_DOMAIN;
}

pub type CircleControlHead = Signed<CircleControlHeadBody>;

impl CircleControlHead {
    pub fn signed(
        control: &CircleControl,
        entry: ExactObjectRef,
        successor: SuccessorLink,
        signer: &UserKeypair,
    ) -> Self {
        Signed::sign(
            CircleControlHeadBody {
                store_root_hash: control.store_root_hash,
                circle_id: control.circle_id,
                control: control.coord(),
                entry,
                successor,
            },
            signer,
        )
    }

    pub fn head_hash(&self) -> ObjectHash {
        self.hash()
    }

    pub fn verify(&self, registration: &StoreDeviceRegistration) -> bool {
        self.control.validate().is_ok()
            && self.control.device_id == registration.device_id.to_string()
            && self.verify_by(&registration.device_signing_pubkey).is_ok()
    }
}

/// The wire body of one recipient's access envelope. Every field here is
/// signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessEnvelopeBody {
    pub store_root_hash: ObjectHash,
    pub candidate_family: crate::store_commit::CandidateFamilyId,
    pub circle_id: CircleId,
    pub owner_pubkey: String,
    pub recipient_slot: String,
    pub control_hash: ObjectHash,
    pub leaf_id: AccessLeafId,
    pub leaf_hash: ObjectHash,
    pub value_hash: ObjectHash,
    pub proof: Vec<MerkleStep>,
}

impl SignedBody for AccessEnvelopeBody {
    const DOMAIN: &'static [u8] = ENVELOPE_DOMAIN;
}

pub type AccessEnvelope = Signed<AccessEnvelopeBody>;

impl AccessEnvelope {
    pub fn verify(
        &self,
        control: &PreparedCircleControl,
        candidate_family: crate::store_commit::CandidateFamilyId,
    ) -> bool {
        self.store_root_hash == control.value.store_root_hash
            && self.candidate_family == candidate_family
            && self.circle_id == control.value.circle_id
            && self.owner_pubkey == control.value.author_pubkey
            && self.control_hash == control.coord.control_hash()
            && self.verify_by(&self.owner_pubkey).is_ok()
            && verify_merkle_proof(self.leaf_hash, &self.proof, control.value.access_root())
    }
}
