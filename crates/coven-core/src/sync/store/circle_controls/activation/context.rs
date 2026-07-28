use crate::sync::circle::{PreparedCircleControl, StoreMembershipStateRef};
use crate::sync::storage::{ExactObjectRef, ProtocolObjectContext, SyncStorage};
use crate::sync::store::circle_controls::CircleOperationError;
use crate::sync::store_commit::{CircleControlRef, StoreRootRef, VerifiedStoreBatchCommit};

pub(super) async fn verify_control_membership(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    control: &PreparedCircleControl,
) -> Result<Vec<(String, crate::sync::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.access_epoch().store_membership;
    let chain = Box::pin(
        crate::sync::store::membership::load_anchored_chain_at_exact_heads_with_history(
            history_verifier,
            &state.heads,
            &state.resolutions,
        ),
    )
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    verify_loaded_control_membership(control, chain)
}

pub(super) async fn verify_control_membership_with_verified_activations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    verified_root: &crate::sync::store_commit::StoreProtocolRoot,
    control: &PreparedCircleControl,
    verified_activations: &crate::sync::store::pull::VerifiedMergeMembershipPrefix,
) -> Result<Vec<(String, crate::sync::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.access_epoch().store_membership;
    let chain = Box::pin(
        crate::sync::store::membership::load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
            storage,
            root,
            verified_root,
            &verified_root.descriptor.founder_pubkey,
            &state.heads,
            &state.resolutions,
            verified_activations,
            None,
        ),
    )
    .await
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    verify_loaded_control_membership(control, chain)
}

fn verify_loaded_control_membership(
    control: &PreparedCircleControl,
    chain: crate::sync::membership::MembershipChain,
) -> Result<Vec<(String, crate::sync::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.access_epoch().store_membership;
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

pub(crate) fn verify_control_context_for_verified_commit(
    reference: &CircleControlRef,
    control: &PreparedCircleControl,
    verified: &VerifiedStoreBatchCommit,
) -> Result<(), CircleOperationError> {
    verified
        .reference()
        .verify_commit(verified.value())
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let commit = verified.value();
    let author = verified.author();
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
