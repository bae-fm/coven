use super::join_validation::*;
use super::*;

pub(crate) enum RegistrationLoadError {
    Object(StoreObjectError),
    Invalid(String),
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
        error => RegistrationLoadError::Invalid(error.to_string()),
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
