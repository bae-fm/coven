use super::CircleOperationError;
use crate::keys::UserKeypair;
use crate::sync::circle::{
    circle_epoch_close_response_semantic_prefix, CircleControlState, CircleEpochCloseResponse,
};
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use crate::sync::store::{operations, AuthorizedStore};
use crate::sync::store_commit::CommitFrontier;
use crate::sync::store_objects::StoreObjectError;

impl AuthorizedStore<'_> {
    pub(crate) async fn publish_circle_epoch_close_responses(
        &self,
        identity: &UserKeypair,
    ) -> Result<(), CircleOperationError> {
        let controls = self.database().closing_circle_controls().await?;
        if controls.is_empty() {
            return Ok(());
        }
        let device_id = self
            .db()
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await?
            .ok_or(CircleOperationError::MissingState(
                crate::database::LOCAL_DEVICE_ID_STATE_KEY,
            ))?;
        let (root, registration_ref, registration, device_signer) =
            operations::load_local_store_authority(self.database(), &device_id, identity)
                .await
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        let frontier = CommitFrontier::from_refs(self.database().materialized_frontier().await?)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        for control in controls {
            let CircleControlState::EpochClose(close) = control.value.state() else {
                return Err(CircleOperationError::InvalidState(
                    "closing Circle state contains an active control".to_string(),
                ));
            };
            let Some(participant) = close
                .participants
                .iter()
                .find(|participant| participant.registration == registration_ref)
            else {
                tracing::debug!(
                    circle_id = %control.value.circle_id,
                    close_id = %close.close_id,
                    device_id = %registration_ref.device_id,
                    "local device is not a participant in the Circle epoch close"
                );
                continue;
            };
            let response = CircleEpochCloseResponse::signed(
                &control,
                registration_ref.clone(),
                frontier.clone(),
                &registration,
                &device_signer,
            )?;
            let prefix = circle_epoch_close_response_semantic_prefix(
                control.value.circle_id,
                close.close_id,
                registration_ref.device_id,
            );
            let context = ProtocolObjectContext::store_encrypted(
                root.store_root_hash,
                ProtocolObjectDomain::CircleEpochCloseResponse,
            );
            let prepared = self
                .storage()
                .prepare_protocol_object(
                    &context,
                    participant.response_slot.clone(),
                    &prefix,
                    response.to_bytes(),
                )
                .map_err(StoreObjectError::from)?;
            match self.storage().create_protocol_object(&prepared).await {
                Ok(()) | Err(StorageError::SlotCollision(_)) => {}
                Err(error) => return Err(StoreObjectError::from(error).into()),
            }
            let (winner_bytes, _) = self
                .storage()
                .read_prepared_protocol_slot(&context, &participant.response_slot, &prefix)
                .await
                .map_err(StoreObjectError::from)?;
            CircleEpochCloseResponse::parse_for(&winner_bytes, &control, &registration)?;
        }
        Ok(())
    }
}
