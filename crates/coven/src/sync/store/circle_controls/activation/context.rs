use crate::protocol::circle::PreparedCircleControl;
use crate::protocol::objects::{ExactObjectRef, ProtocolObjectContext};
use crate::protocol::store_commit::{CircleControlRef, VerifiedStoreBatchCommit};
use crate::storage::SyncStorage;
use crate::sync::store::circle_controls::CircleOperationError;

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
        .map_err(crate::protocol::objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}
