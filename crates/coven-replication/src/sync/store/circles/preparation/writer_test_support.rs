use super::*;
use crate::sync::store::circles::authorized_writer::AuthorizedCircleWriter;

impl AuthorizedCircleWriter<'_, '_> {
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_circle_object_for_test(
        &mut self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object(context, semantic_prefix, extension, bytes)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn prepare_circle_object_at_for_test(
        &mut self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        bytes: Vec<u8>,
    ) -> Result<coven_protocol::objects::PreparedExactObject, CircleOperationError> {
        self.preparer()
            .prepare_circle_object_at(context, slot, semantic_prefix, bytes)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn prepare_circle_activation_objects_for_test(
        &mut self,
        draft: coven_protocol::circle::CircleTransitionDraft,
        history: &CircleTransitionHistory,
    ) -> Result<
        (
            coven_protocol::circle::PreparedCircleTransition,
            coven_protocol::store_commit::CircleActivationObjects,
            std::collections::BTreeMap<String, coven_protocol::objects::PreparedExactObject>,
            Option<coven_protocol::objects::ExactObjectRef>,
            Vec<coven_protocol::store_commit::StreamActivation>,
        ),
        CircleOperationError,
    > {
        self.preparer()
            .prepare_circle_activation_objects(draft, history, &[])
            .await
    }
}
