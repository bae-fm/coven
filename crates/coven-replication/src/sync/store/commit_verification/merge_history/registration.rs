use super::join_validation::*;
use super::*;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RegistrationLoadError {
    #[error("registration object: {0}")]
    Object(#[from] StoreObjectError),
    #[error("registration protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("registration database: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("registration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("registration device join exchange: {0}")]
    DeviceJoinExchange(
        #[from] coven_protocol::store_commit::device_join_exchange::DeviceJoinExchangeError,
    ),
    #[error("registration membership chain: {0}")]
    AnchoredChain(#[from] crate::sync::store::AnchoredChainError),
    #[error("registration Store pull: {0}")]
    Pull(#[source] Box<StorePullError>),
    #[error("registration state is invalid: {0}")]
    Invalid(String),
}

impl From<StorePullError> for RegistrationLoadError {
    fn from(error: StorePullError) -> Self {
        Self::Pull(Box::new(error))
    }
}

impl From<RegistrationLoadError> for StorePullError {
    fn from(error: RegistrationLoadError) -> Self {
        match error {
            RegistrationLoadError::Object(error) => Self::Object(error),
            RegistrationLoadError::Protocol(error) => Self::Protocol(error),
            RegistrationLoadError::Database(error) => Self::Database(error),
            RegistrationLoadError::Json(error) => Self::Serialization(error),
            RegistrationLoadError::DeviceJoinExchange(error) => Self::DeviceJoinExchange(error),
            RegistrationLoadError::AnchoredChain(error) => Self::MembershipChain(error),
            RegistrationLoadError::Pull(error) => *error,
            RegistrationLoadError::Invalid(error) => Self::InvalidState(error),
        }
    }
}

pub(crate) struct VerifiedCommitJoinOutcome {
    pub(crate) attempt: DeviceJoinAttempt,
    pub(crate) owner: StoreDeviceRegistration,
    pub(crate) outcome: super::store_commit::DeviceJoinOutcome,
}

pub(crate) fn registration_attempt_error(error: StorePullError) -> RegistrationLoadError {
    match error {
        StorePullError::Object(error) => RegistrationLoadError::Object(error),
        StorePullError::Storage(error) => {
            RegistrationLoadError::Object(StoreObjectError::Storage(error))
        }
        error => RegistrationLoadError::from(error),
    }
}

pub(crate) struct LoadedDeviceJoinCleanupActivation {
    pub(crate) verified_commit: VerifiedStoreBatchCommit,
    pub(crate) receipts: Vec<LoadedCommitJoinCleanupReceipt>,
}

/// Bind a reclaim target to the Circle snapshot generation that published it.
/// The generation's metadata rides no Store commit and is sealed under the Circle
/// epoch key, so a Store member outside the Circle cannot read it. What every
/// member can check is the addressing the stream derives from public identity:
/// both the metadata slot and the image key are computed from the Circle, the
/// author's device, and the generation, so evidence naming another Circle, another
/// device's stream, or another generation cannot describe this object. A member
/// inside the Circle re-walks the stream itself before authorizing any delete.
pub(super) fn validate_circle_snapshot_activated_reclaim_target(
    target: &coven_protocol::reclaim::ReclaimTarget,
    activation: &coven_protocol::reclaim::CircleSnapshotStreamActivation<'_>,
) -> Result<(), RegistrationLoadError> {
    let coven_protocol::reclaim::ReclaimTarget::CircleSnapshotImage(snapshot_image) = target else {
        return Err(RegistrationLoadError::Invalid(
            "reclaim target is not published by a Circle snapshot stream".to_string(),
        ));
    };
    let device_id = activation.author_registration.device_id.to_string();
    let expected_metadata = format!(
        "{}.json",
        super::store_commit::circle_snapshot_slot_prefix(
            activation.circle_id,
            &device_id,
            activation.snapshot.generation,
        )
    );
    let expected_image = format!(
        "{}.db",
        super::store_commit::circle_snapshot_image_semantic_prefix(
            activation.circle_id,
            &device_id,
            snapshot_image.image.image_hash,
        )
    );
    if activation.snapshot.object.slot().logical_key() != expected_metadata
        || snapshot_image.image.object.slot().logical_key() != expected_image
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact Circle snapshot generation".to_string(),
        ));
    }
    Ok(())
}

/// The Store snapshot generation that published a reclaimed membership rollup,
/// checked against the slots both objects must occupy.
///
/// A Store snapshot stream is anchored on its author's device registration
/// alone, so the two logical keys are the whole binding: the generation's
/// metadata slot, and the rollup's content-addressed slot under this author.
pub(super) fn validate_store_snapshot_activated_reclaim_target(
    target: &coven_protocol::reclaim::ReclaimTarget,
    activation: &coven_protocol::reclaim::StoreSnapshotStreamActivation<'_>,
) -> Result<(), RegistrationLoadError> {
    let coven_protocol::reclaim::ReclaimTarget::StoreMembershipRollup(rollup) = target else {
        return Err(RegistrationLoadError::Invalid(
            "reclaim target is not published by a Store snapshot stream".to_string(),
        ));
    };
    let device_id = activation.author_registration.device_id.to_string();
    let expected_metadata = format!(
        "{}.json",
        super::store_commit::snapshot_slot_prefix(&device_id, activation.snapshot.generation)
    );
    let expected_rollup = format!(
        "{}.json",
        super::store_commit::membership_rollup_semantic_prefix(
            &device_id,
            rollup.rollup.rollup_hash,
        )
    );
    if activation.snapshot.object.slot().logical_key() != expected_metadata
        || rollup.rollup.object.slot().logical_key() != expected_rollup
    {
        return Err(RegistrationLoadError::Invalid(
            "reclaim evidence target differs from its exact Store snapshot generation".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn device_state_has_active_registration(
    state: &ResolvedStoreDeviceState,
    registration: &StoreDeviceRegistrationRef,
) -> bool {
    state
        .devices
        .get(&registration.device_id)
        .is_some_and(|record| {
            record.registration == *registration
                && matches!(record.status, StoreDeviceStatus::Active)
        })
}

pub(crate) fn device_state_has_pending_proposal(
    state: &ResolvedStoreDeviceState,
    proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
) -> bool {
    state
        .devices
        .get(&proposal.target.device_id)
        .and_then(|record| record.proposals.get(&proposal.proposal_id))
        .is_some_and(|state| {
            matches!(state, StoreDeviceProposalState::Pending { proposal: pending } if pending == proposal)
        })
}
