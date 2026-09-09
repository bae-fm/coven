use super::*;

impl StoreCommitVerifier<'_> {
    pub(crate) async fn exact_protocol_object_is_absent(
        &self,
        object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<bool, StoreObjectError> {
        match self.storage.observe_exact_slot(object.slot()).await? {
            None => Ok(true),
            Some(observed) if observed == *object => Ok(false),
            Some(_) => Err(
                coven_protocol::objects::StorageError::SlotCollision(format!(
                    "retired artifact slot {} contains different bytes",
                    object.slot().logical_key()
                ))
                .into(),
            ),
        }
    }
}
