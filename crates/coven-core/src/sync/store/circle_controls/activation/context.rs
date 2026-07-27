use crate::sync::circle::{PreparedCircleControl, StoreMembershipStateRef};
use crate::sync::storage::{ExactObjectRef, ProtocolObjectContext, SyncStorage};
use crate::sync::store::circle_controls::CircleOperationError;
use crate::sync::store_commit::{
    CircleControlRef, StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreRootRef,
};

pub(super) async fn verify_control_membership(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    control: &PreparedCircleControl,
    founder_pubkey: &str,
) -> Result<Vec<(String, crate::sync::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.access_epoch().store_membership;
    let chain = Box::pin(
        crate::sync::store::membership::load_anchored_chain_at_exact_heads(
            storage,
            root,
            founder_pubkey,
            &state.heads,
            &state.resolutions,
        ),
    )
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if !chain.authorizes_write_authority(
        &control.value.value.membership_authority,
        &control.value.author_pubkey,
    ) {
        return Err(CircleOperationError::InvalidState(
            "Store membership does not authorize circle control author".to_string(),
        ));
    }
    let membership_state_hash = match chain.status() {
        crate::sync::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
        crate::sync::membership::MembershipStatus::Conflict(_) => {
            return Err(CircleOperationError::InvalidState(
                "Store membership state has an unresolved conflict".to_string(),
            ));
        }
    };
    let expected_state = StoreMembershipStateRef::from_parts(
        state.heads.clone(),
        state.resolutions.clone(),
        state.recovery.clone(),
        membership_state_hash,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if expected_state != control.value.store_membership_state_ref() {
        return Err(CircleOperationError::InvalidState(
            "circle control Store membership state reference is invalid".to_string(),
        ));
    }
    Ok(chain.current_members())
}

pub(crate) fn verify_control_context(
    reference: &CircleControlRef,
    control: &PreparedCircleControl,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
) -> Result<(), CircleOperationError> {
    commit_ref
        .verify_commit(commit)
        .and_then(|()| commit.verify_at(commit.store_root_hash, &commit_ref.coord, author))
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let device_matches = control.value.value.order.device_id == author.device_id.to_string();
    if !control.verify()
        || reference.circle_id() != control.value.circle_id
        || reference.control() != &control.coord
        || control.value.store_root_hash != commit.store_root_hash
        || control.value.author_pubkey != author.author_pubkey
        || !device_matches
    {
        return Err(CircleOperationError::InvalidState(
            "circle control context differs from its Store reference and commit".to_string(),
        ));
    }
    Ok(())
}

pub(crate) async fn read_exact_circle_object(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    storage
        .read_protocol_object(context, object, semantic_prefix)
        .await
        .map_err(crate::sync::store_objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}
