use crate::sync::store::circle_controls::CircleOperationError;
use coven_protocol::objects::{ExactObjectRef, ProtocolObjectContext};
use coven_storage::SyncStorage;

pub(crate) async fn read_exact_circle_object(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
) -> Result<Vec<u8>, CircleOperationError> {
    storage
        .read_protocol_object(context, object, semantic_prefix)
        .await
        .map_err(coven_protocol::objects::StoreObjectError::from)
        .map_err(CircleOperationError::from)
}
