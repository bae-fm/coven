use crate::sync::store::pull;
use coven_protocol::membership::MembershipChain;

pub(crate) struct MergeConflictResolutionAuthorization {
    pub(crate) membership: MembershipChain,
    pub(crate) device_state_ref: coven_protocol::store_commit::StoreDeviceStateRef,
    pub(crate) device_state: coven_protocol::store_commit::ResolvedStoreDeviceState,
}

pub(crate) fn validate_retained_membership_floors(
    checkpoints: &[coven_database::RetainedMergeHistoryCheckpoint],
    membership: &MembershipChain,
) -> Result<(), pull::StorePullError> {
    if checkpoints.iter().any(|checkpoint| {
        matches!(
            checkpoint,
            coven_database::RetainedMergeHistoryCheckpoint::Snapshot(checkpoint)
                if !checkpoint.summary.membership_floor.is_included_in(membership)
        )
    }) {
        return Err(pull::StorePullError::InvalidState(
            "Merge membership omits retained effective predecessor authority".to_string(),
        ));
    }
    Ok(())
}
