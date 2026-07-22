use super::*;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StoreProtocolError {
    #[error("object hash must be exactly 64 lowercase hexadecimal characters: {0:?}")]
    InvalidObjectHash(String),
    #[error("unsupported Store protocol version {0}")]
    UnsupportedVersion(u32),
    #[error("malformed Store protocol object: {0}")]
    Malformed(String),
    #[error("Store protocol signature is invalid")]
    InvalidSignature,
    #[error("Owner promotion evidence does not match its exact Store authority")]
    OwnerPromotionMismatch,
    #[error("Store protocol object is in slot {actual:?}, expected {expected:?}")]
    RelocatedSlot { expected: String, actual: String },
    #[error("Store package names key {actual:?}, expected {expected:?}")]
    RelocatedPackage { expected: String, actual: String },
    #[error("candidate object names key {actual:?}, expected {expected:?}")]
    RelocatedCandidateObject { expected: String, actual: String },
    #[error("Store protocol root hash is {actual}, expected {expected}")]
    StoreRootMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store protocol root id is {actual}, expected {expected}")]
    StoreRootIdMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store id is {actual:?}, expected {expected:?}")]
    StoreMismatch { expected: String, actual: String },
    #[error("founder is {actual:?}, expected {expected:?}")]
    FounderMismatch { expected: String, actual: String },
    #[error("store protocol root has an invalid founder membership entry")]
    InvalidFounder,
    #[error("Store sync-routing hash is {actual}, expected {expected}")]
    SyncRoutingMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store Merge membership control is invalid or signed by a different device")]
    InvalidMergeMembershipControl,
    #[error("Store batch has no Store package, circle package, or control")]
    EmptyBatch,
    #[error("Store batch has no Store package")]
    MissingStorePackage,
    #[error("Store batch repeats Store device registration {device_id:?} revision {revision}")]
    DuplicateDeviceRegistration { device_id: String, revision: u64 },
    #[error(
        "Store device registration {device_id:?} revision {revision} has hash {actual}, expected {expected}"
    )]
    DeviceRegistrationRefMismatch {
        device_id: String,
        revision: u64,
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("device join attempt fields do not name one exact registration lifecycle")]
    JoinAttemptMismatch,
    #[error("device readiness proof differs from its exact attempt, registration, or initial acknowledgement")]
    DeviceReadinessMismatch,
    #[error("device join outcome differs from its exact attempt or closed outcome variant")]
    JoinOutcomeMismatch,
    #[error("provider access activation contains duplicate or contradictory exact authority")]
    ProviderAccessMismatch,
    #[error("Owner recovery node differs from its exact registration lifecycle")]
    OwnerRecoveryMismatch,
    #[error("Store device state differs from its signed predecessor state")]
    DeviceStateMismatch,
    #[error("Store batch has no package for circle {0}")]
    MissingCirclePackage(CircleId),
    #[error("Store batch has more than one package for circle {0}")]
    DuplicateCirclePackage(CircleId),
    #[error("Store batch has more than one control for circle {0}")]
    DuplicateCircleControl(CircleId),
    #[error("circle control coordinate is invalid")]
    InvalidCircleControlCoord,
    #[error("circle {circle_id} package is at {actual:?}, expected {expected:?}")]
    RelocatedCirclePackage {
        circle_id: CircleId,
        expected: String,
        actual: String,
    },
    #[error("Store key generation must be positive, got {0}")]
    InvalidKeyGeneration(u64),
    #[error("store protocol root store id is empty")]
    EmptyStoreId,
    #[error("Store commit sequence must start at 1, got {0}")]
    InvalidSequence(u64),
    #[error("Store commit sequence 1 must not name a predecessor")]
    UnexpectedPredecessor,
    #[error("Store commit after sequence 1 must name its predecessor hash")]
    MissingPredecessor,
    #[error("Store control revision must start at 1, got {0}")]
    InvalidRevision(u64),
    #[error("Store control revision 1 must not name a predecessor")]
    UnexpectedControlPredecessor,
    #[error("Store control revision after 1 must name its predecessor hash")]
    MissingControlPredecessor,
    #[error("Store acknowledgement sequence must start at 1, got {0}")]
    InvalidAckSequence(u64),
    #[error("Store acknowledgement sequence 1 must not name a predecessor object")]
    UnexpectedAckPredecessor,
    #[error("Store acknowledgement after sequence 1 must name its predecessor object")]
    MissingAckPredecessor,
    #[error("Store commit for {0:?} must not name its own device as a dependency")]
    OwnDependency(String),
    #[error(
        "invalid membership coordinate {author}/{grant}/{stream_id}/{seq} with entry hash {entry_hash}"
    )]
    InvalidMembershipCoordinate {
        author: String,
        grant: String,
        stream_id: String,
        seq: u64,
        entry_hash: String,
    },
    #[error("invalid Store membership resolution authority for resolver {0:?}")]
    InvalidMembershipResolutionAuthority(String),
    #[error("membership object coordinate {expected:?} differs from signed entry {declared:?}")]
    MembershipCoordinateMismatch {
        expected: Box<MembershipCoord>,
        declared: Box<MembershipCoord>,
    },
    #[error("Store package length exceeds the platform address space")]
    PackageTooLarge,
    #[error("Store package length is {actual}, expected {expected}")]
    PackageLengthMismatch { expected: u64, actual: u64 },
    #[error("Store package hash is {actual}, expected {expected}")]
    PackageHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
    #[error("Store object hash is {actual}, expected {expected}")]
    ObjectHashMismatch {
        expected: ObjectHash,
        actual: ObjectHash,
    },
}

pub fn protocol_prefix() -> &'static str {
    STORE_PROTOCOL_PREFIX
}

pub fn store_protocol_root_logical_key() -> &'static str {
    STORE_PROTOCOL_ROOT_SEMANTIC_PATH
}

pub fn device_join_attempt_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_ATTEMPT_PREFIX}{attempt_id}")
}

pub fn device_self_retirement_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &StoreDeviceId,
    retirement_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/device-self-retirements/{device_id}/{retirement_hash}",
        family.as_hash()
    )
}

pub fn circle_access_leaf_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    owner_pubkey: &str,
    epoch_id: CircleEpochId,
    recipient_slot: &str,
    leaf_id: AccessLeafId,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/access-leaves/{owner_pubkey}/{epoch_id}/{recipient_slot}/{leaf_id}",
        family.as_hash(),
    )
}

pub fn circle_access_envelope_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    owner_pubkey: &str,
    recipient_slot: &str,
    control_hash: ObjectHash,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/access-envelopes/{owner_pubkey}/{recipient_slot}/{control_hash}",
        family.as_hash(),
    )
}

pub fn device_join_outcome_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_OUTCOME_PREFIX}{attempt_id}")
}

pub fn device_join_abandonment_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    device_join_attempt_semantic_prefix(attempt_id)
}

pub fn device_join_cleanup_receipt_semantic_prefix(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{STORE_DEVICE_JOIN_CLEANUP_RECEIPT_PREFIX}{attempt_id}")
}

pub fn device_exclusion_proposal_semantic_prefix(
    target: StoreDeviceId,
    proposal_id: StoreDeviceExclusionProposalId,
    proposal_hash: ObjectHash,
) -> String {
    format!("{STORE_DEVICE_EXCLUSION_PROPOSAL_PREFIX}{target}/{proposal_id}/{proposal_hash}")
}

pub fn device_exclusion_outcome_semantic_prefix(
    target: StoreDeviceId,
    proposal_id: StoreDeviceExclusionProposalId,
) -> String {
    format!("{STORE_DEVICE_EXCLUSION_OUTCOME_PREFIX}{target}/{proposal_id}")
}

pub fn provider_access_grant_semantic_prefix(
    grant_id: &crate::sync::provider::ProviderAccessGrantId,
) -> String {
    format!("{STORE_PROVIDER_ACCESS_GRANT_PREFIX}{}", grant_id.0)
}

pub fn provider_access_withdrawal_semantic_prefix(
    grant_id: &crate::sync::provider::ProviderAccessGrantId,
) -> String {
    format!("{STORE_PROVIDER_ACCESS_WITHDRAWAL_PREFIX}{}", grant_id.0)
}

pub fn owner_recovery_semantic_prefix(
    owner_pubkey: &str,
    owner_grant: MembershipGrantId,
    sequence: u64,
) -> String {
    format!("{STORE_OWNER_RECOVERY_PREFIX}{owner_pubkey}/{owner_grant}/{sequence}")
}

pub fn package_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/packages/{device_id}/{seq}/{package_hash}",
        family.as_hash()
    )
}

pub fn circle_package_semantic_prefix(
    circle_id: CircleId,
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    package_hash: ObjectHash,
) -> String {
    format!(
        "circles/{circle_id}/candidates/{}/packages/{device_id}/{seq}/{package_hash}",
        family.as_hash()
    )
}

pub fn commit_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("{STORE_CANDIDATE_PREFIX}*/commits/{device_id}/{seq}")
}

pub fn commit_semantic_prefix(
    family: CandidateFamilyId,
    device_id: &str,
    seq: u64,
    commit_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_CANDIDATE_PREFIX}{}/commits/{device_id}/{seq}/{commit_hash}",
        family.as_hash()
    )
}

pub fn semantic_prefix_from_exact_object(
    object: &ExactObjectRef,
    extension: &str,
) -> Result<String, StoreProtocolError> {
    object
        .slot()
        .logical_key()
        .strip_suffix(extension)
        .map(str::to_string)
        .ok_or_else(|| StoreProtocolError::RelocatedSlot {
            expected: format!("candidate object ending in {extension}"),
            actual: object.slot().logical_key().to_string(),
        })
}

pub fn head_slot_prefix(device_id: &str, seq: u64) -> String {
    format!("{STORE_HEAD_PREFIX}{device_id}/{seq}")
}

pub fn head_semantic_prefix(device_id: &str, seq: u64, head_hash: ObjectHash) -> String {
    format!("{}/{head_hash}", head_slot_prefix(device_id, seq))
}

pub fn registration_slot_prefix(device_id: &str) -> String {
    format!("{STORE_DEVICE_REGISTRATION_PREFIX}{device_id}")
}

pub fn registration_semantic_prefix(device_id: &str) -> String {
    registration_slot_prefix(device_id)
}

pub fn founder_registration_semantic_prefix(creation_id: StoreCreationId) -> String {
    format!("store-v1/devices/founder/{creation_id}/registration")
}

pub fn founder_membership_head_semantic_prefix(creation_id: StoreCreationId) -> String {
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}founder/{creation_id}/1")
}

pub fn ack_slot_prefix(device_id: &str, revision: u64) -> String {
    format!("{STORE_ACK_PREFIX}{device_id}/{revision}")
}

pub fn ack_semantic_prefix(device_id: &str, revision: u64, ack_hash: ObjectHash) -> String {
    format!("{}/{ack_hash}", ack_slot_prefix(device_id, revision))
}

pub fn snapshot_slot_prefix(device_id: &str, generation: u64) -> String {
    format!("{STORE_SNAPSHOT_META_PREFIX}{device_id}/{generation}")
}

pub fn membership_entry_semantic_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    entry_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_MEMBERSHIP_ENTRY_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}/{entry_hash}"
    )
}

pub fn membership_head_semantic_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
    head_hash: ObjectHash,
) -> String {
    format!(
        "{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}/{head_hash}"
    )
}

pub fn membership_head_slot_prefix(
    author: &str,
    author_owner_grant: &MembershipGrantId,
    stream_id: AuthorStreamId,
    seq: u64,
) -> String {
    format!("{STORE_MEMBERSHIP_HEAD_PREFIX}{author}/{author_owner_grant}/{stream_id}/{seq}")
}

pub fn membership_resolution_semantic_prefix(
    conflict_hash: ObjectHash,
    resolver: &str,
    resolution_hash: ObjectHash,
) -> String {
    format!("store-v1/membership/resolutions/{conflict_hash}/{resolver}/{resolution_hash}")
}

pub fn snapshot_image_semantic_prefix(author: &str, image_hash: ObjectHash) -> String {
    format!("{STORE_SNAPSHOT_IMAGE_PREFIX}{author}/{image_hash}")
}

pub fn snapshot_semantic_prefix(author: &str, snapshot_hash: ObjectHash) -> String {
    format!("{STORE_SNAPSHOT_META_PREFIX}{author}/{snapshot_hash}")
}

pub(crate) fn domain_json(domain: &[u8], value: &impl Serialize) -> Vec<u8> {
    let json = serde_json::to_vec(value).expect("canonical Store fields serialize");
    let mut bytes = Vec::with_capacity(domain.len() + json.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&json);
    bytes
}

pub(super) fn require_version(version: u32) -> Result<(), StoreProtocolError> {
    if version == STORE_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(StoreProtocolError::UnsupportedVersion(version))
    }
}

pub(super) fn validate_commit_order(order: &StoreCommitOrder) -> Result<(), StoreProtocolError> {
    let seq = order.seq();
    if seq == 0 {
        return Err(StoreProtocolError::InvalidSequence(0));
    }
    {
        let predecessor = &order.predecessor;
        let dependencies = &order.dependencies;
        match (seq, predecessor) {
            (1, None) => {}
            (1, Some(_)) => return Err(StoreProtocolError::UnexpectedPredecessor),
            (_, None) => return Err(StoreProtocolError::MissingPredecessor),
            (_, Some(reference)) => {
                if reference.coord.sequence.checked_add(1) != Some(seq) {
                    return Err(StoreProtocolError::Malformed(
                        "predecessor is not the preceding author-stream commit".to_string(),
                    ));
                }
            }
        }
        for (stream_id, reference) in dependencies {
            if reference.coord.stream_id != *stream_id || reference.coord.sequence == 0 {
                return Err(StoreProtocolError::Malformed(format!(
                    "dependency {stream_id} has a different exact coordinate"
                )));
            }
        }
    }
    Ok(())
}

pub(super) fn validate_commit_predecessor_states(
    order: &StoreCommitOrder,
    membership: &StoreMembershipStateRef,
    devices: &StoreDeviceStateRef,
) -> Result<(), StoreProtocolError> {
    membership.validate_shape()?;
    if membership.recovery() != devices.recovery() {
        return Err(StoreProtocolError::OwnerRecoveryMismatch);
    }
    validate_recovery_cursors(membership.recovery())?;
    validate_recovery_cursors(devices.recovery())?;
    {
        let mut expected = order.dependencies.clone();
        if let Some(predecessor) = &order.predecessor {
            if expected
                .insert(predecessor.coord.stream_id, predecessor.clone())
                .is_some_and(|dependency| dependency != *predecessor)
            {
                return Err(StoreProtocolError::Malformed(
                    "Merge predecessor disagrees with the same-stream dependency".to_string(),
                ));
            }
        }
        if devices.frontier() != &CommitFrontier(expected) {
            return Err(StoreProtocolError::Malformed(
                "Store device state names a different Merge predecessor cut".to_string(),
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_commit_frontier(
    frontier: &CommitFrontier,
) -> Result<(), StoreProtocolError> {
    {
        for (stream_id, reference) in &frontier.0 {
            if reference.coord.stream_id != *stream_id || reference.coord.sequence == 0 {
                return Err(StoreProtocolError::Malformed(format!(
                    "frontier entry {stream_id} has a different exact coordinate"
                )));
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_store_history_cut(
    frontier: &StoreHistoryCut,
) -> Result<(), StoreProtocolError> {
    validate_commit_frontier(&CommitFrontier(frontier.0.clone()))
}

pub(super) fn validate_store_device_state_ref(
    state: &StoreDeviceStateRef,
) -> Result<(), StoreProtocolError> {
    validate_recovery_cursors(state.recovery())?;
    validate_commit_frontier(state.frontier())
}

pub(super) fn validate_successor_sequence(
    sequence: u64,
    successor: &SuccessorLink,
) -> Result<(), StoreProtocolError> {
    match (sequence, successor.predecessor.is_some()) {
        (0, _) => Err(StoreProtocolError::InvalidAckSequence(0)),
        (1, false) => Ok(()),
        (1, true) => Err(StoreProtocolError::UnexpectedAckPredecessor),
        (_, true) => Ok(()),
        (_, false) => Err(StoreProtocolError::MissingAckPredecessor),
    }
}

pub(super) fn validate_ack_state(
    store_root_hash: ObjectHash,
    registration: &StoreDeviceRegistrationRef,
    store_cut: &StoreHistoryCut,
    device_state: &StoreDeviceStateRef,
    exclusions: &StoreAckExclusionState,
) -> Result<(), StoreProtocolError> {
    validate_store_history_cut(store_cut)?;
    let _ = (store_root_hash, registration);
    let state_matches = device_state.frontier() == &store_cut.frontier();
    if !state_matches {
        return Err(StoreProtocolError::DeviceStateMismatch);
    }
    {
        let proposal_freezes = &exclusions.proposal_freezes;
        if proposal_freezes
            .windows(2)
            .any(|pair| pair[0].proposal.proposal_id >= pair[1].proposal.proposal_id)
        {
            return Err(StoreProtocolError::DeviceStateMismatch);
        }
        for freeze in proposal_freezes {
            validate_store_history_cut(&freeze.target_cut)?;
            freeze.proposal.validate_path()?;
            if !store_cut.frontier().covers(&freeze.target_cut.frontier()) {
                return Err(StoreProtocolError::DeviceStateMismatch);
            }
        }
        Ok(())
    }
}

fn validate_membership_coord(coord: &MembershipCoord) -> Result<(), StoreProtocolError> {
    if coord.seq == 0 || coord.author_pubkey.is_empty() {
        return Err(StoreProtocolError::InvalidMembershipCoordinate {
            author: coord.author_pubkey.clone(),
            grant: coord.author_owner_grant.to_string(),
            stream_id: coord.stream_id.to_string(),
            seq: coord.seq,
            entry_hash: coord.entry_hash.to_string(),
        });
    }
    Ok(())
}

pub(super) fn validate_membership_authority(
    authority: &MembershipGrantCreationAuthority,
) -> Result<(), StoreProtocolError> {
    match authority {
        MembershipGrantCreationAuthority::Entry(coord) => validate_membership_coord(coord),
        MembershipGrantCreationAuthority::ConflictResolution(reference) => {
            let resolver = hex::decode(&reference.resolver_pubkey).map_err(|_| {
                StoreProtocolError::InvalidMembershipResolutionAuthority(
                    reference.resolver_pubkey.clone(),
                )
            })?;
            if resolver.len() != crate::keys::SIGN_PUBLICKEYBYTES {
                return Err(StoreProtocolError::InvalidMembershipResolutionAuthority(
                    reference.resolver_pubkey.clone(),
                ));
            }
            Ok(())
        }
    }
}

pub(super) fn validate_operation_membership_authority(
    authority: &MembershipGrantCreationAuthority,
) -> Result<(), StoreProtocolError> {
    validate_membership_authority(authority)
}
