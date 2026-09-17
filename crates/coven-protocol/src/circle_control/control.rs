use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleControlOrder {
    pub device_id: String,
    pub author_owner_grant: MembershipGrantId,
    pub seq: u64,
    pub previous_control_hash: Option<ObjectHash>,
    pub dependencies: Vec<CircleControlCoord>,
}

/// A terminal deletion. It freezes the epoch spine it terminated — the same
/// `MergeActiveCircleEpoch` an `EpochClose` freezes — so historical package
/// verification and exact reclamation keep the epoch, key fingerprint, and
/// roster spine with no live access material.
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

/// One observed predecessor control of this Circle: its exact coordinate and the
/// exact accepted Store commit that activated it. The activating commit is what
/// a verifier resolves to prove the predecessor really entered accepted history.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlActivationRef {
    pub coord: CircleControlCoord,
    pub activating_commit: StoreBatchCommitRef,
}

/// One losing branch of a resolved control conflict, carried so the resolution
/// can cover every branch's frontier rather than only the chosen branch's: the
/// branch's control activation, its metadata and roster frontiers, and the
/// metadata entry that branch selected. The resolution unions these into its own
/// frontier so every branch's author-stream position stays covered once the
/// conflict collapses, and re-derives its name as the deterministic metadata
/// selection across the union.
#[derive(Debug, Clone)]
pub struct ResolvedConflictBranch {
    pub control: CircleControlActivationRef,
    pub metadata_frontier: Vec<CircleMetadataCoord>,
    pub roster_frontier: Vec<CircleRosterCoord>,
    pub selected_metadata: CircleMetadata,
}

/// Insert `coord` into a frontier keyed by author stream, keeping the deeper
/// (higher-sequence) position when the stream already carries one. Merging every
/// conflicting branch's frontier this way yields the union frontier: each stream
/// is covered at its deepest position across all branches, so a device that
/// authored on that stream continues from its own position.
pub fn merge_frontier_coord<C>(
    frontier: &mut Vec<C>,
    coord: C,
    stream_key: impl Fn(&C) -> CircleAuthorStreamKey,
    seq: impl Fn(&C) -> u64,
) {
    let key = stream_key(&coord);
    match frontier
        .iter_mut()
        .find(|existing| stream_key(existing) == key)
    {
        Some(existing) if seq(&coord) > seq(existing) => *existing = coord,
        Some(_) => {}
        None => frontier.push(coord),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeCircleOwnerAuthorityRef {
    pub roster: MergeCircleRosterStateRef,
    pub grant_id: MembershipGrantId,
    pub created_at: crate::circle_roster::CircleRosterCoord,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircleControlValue {
    pub order: MergeCircleControlOrder,
    pub state: CircleControlState,
    /// Every Store member's sealed access entry at this control. The control's
    /// own signature is what binds the set.
    pub access: CircleAccessMap,
    pub author_authority: MergeCircleOwnerAuthorityRef,
    pub membership_authority: MembershipCoord,
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

    pub fn covered_controls(&self) -> &[CircleControlActivationRef] {
        &self.access_epoch().covered_controls
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
        self.value.author_authority.grant_id.clone()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn membership_authority(&self) -> &MembershipCoord {
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
        let grant_id = author_authority.grant_id.clone();
        let stream_key = CircleAuthorStreamKey {
            author_pubkey: self.author_pubkey.clone(),
            device_id: order.device_id.clone(),
            author_owner_grant: order.author_owner_grant.clone(),
        };
        let covered_are_canonical = access_epoch
            .covered_controls
            .windows(2)
            .all(|pair| pair[0].coord.stream_key() < pair[1].coord.stream_key());
        let own_predecessor = access_epoch
            .covered_controls
            .iter()
            .find(|covered| covered.coord.stream_key() == stream_key);
        let expected_dependencies = access_epoch
            .covered_controls
            .iter()
            .filter(|covered| covered.coord.stream_key() != stream_key)
            .map(|covered| covered.coord.clone())
            .collect::<Vec<_>>();
        let order_is_valid = !order.device_id.is_empty()
            && order.seq > 0
            && order.author_owner_grant == grant_id
            && covered_are_canonical
            && order.dependencies == expected_dependencies;
        let authority_is_founder_roster = author_authority.roster == access_epoch.roster;
        let founder = order.seq == 1 && access_epoch.covered_controls.is_empty();
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
        // A deletion issues no access material; every live control seals one
        // entry per Store member.
        let access_matches_state = match &self.value.state {
            CircleControlState::Deleted(_) => self.value.access.is_empty(),
            CircleControlState::ActiveEpoch(_) | CircleControlState::EpochClose(_) => {
                !self.value.access.is_empty() && self.value.access.verify_shape()
            }
        };
        owners_are_canonical
            && origin_is_valid
            && state_is_valid
            && access_matches_state
            && order_is_valid
            && continuity_is_valid
            && founder_identity_is_valid
            && self.verify_by(&self.author_pubkey).is_ok()
    }

    pub fn coord(&self) -> CircleControlCoord {
        let order = &self.value.order;
        CircleControlCoord {
            device_id: order.device_id.clone(),
            author_pubkey: self.author_pubkey.clone(),
            author_owner_grant: order.author_owner_grant.clone(),
            seq: order.seq,
            control_hash: self.control_hash(),
        }
    }
}
