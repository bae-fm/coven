use super::*;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store_commit::{circle_ack_slot_prefix, CircleAck, CommitFrontier};
use crate::sync::store_objects::StoreObjectError;

impl AuthorizedStoreHistory<'_> {
    pub(super) async fn load_circle_acknowledgement(
        &self,
        reference: &crate::sync::store_commit::CircleAckRef,
        control: &crate::sync::circle::CircleControlCoord,
    ) -> Result<CircleAck, writer::StoreAckError> {
        let access = self
            .database()
            .circle_package_access(self.root().clone(), reference.circle_id, control.clone())
            .await?
            .ok_or_else(|| {
                writer::StoreAckError::InvalidOutbound(format!(
                    "Circle {} acknowledgement key is not resolvable from retained controls",
                    reference.circle_id
                ))
            })?;
        let author = self
            .database()
            .activated_store_device_registration(reference.registration.clone())
            .await?;
        let context = ProtocolObjectContext::circle(
            self.root().store_root_hash,
            ProtocolObjectDomain::CircleAcknowledgement,
            access.into_encryption(),
        );
        let semantic_prefix = circle_ack_slot_prefix(
            reference.circle_id,
            &author.device_id.to_string(),
            reference.sequence,
        );
        let bytes = self
            .storage()
            .read_protocol_object(&context, &reference.object, &semantic_prefix)
            .await
            .map_err(StoreObjectError::from)?;
        CircleAck::parse_at(&bytes, self.root(), reference, &author)
            .map_err(|error| writer::StoreAckError::InvalidOutbound(error.to_string()))
    }

    pub(super) async fn load_circle_acknowledgement_under_retained_controls(
        &self,
        reference: &crate::sync::store_commit::CircleAckRef,
        preferred: &crate::sync::circle::CircleControlCoord,
        retained: &[crate::sync::circle::CircleControlCoord],
    ) -> Result<CircleAck, writer::StoreAckError> {
        let mut last_error = None;
        for control in std::iter::once(preferred)
            .chain(retained.iter().filter(|control| *control != preferred))
        {
            match self.load_circle_acknowledgement(reference, control).await {
                Ok(acknowledgement) => return Ok(acknowledgement),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            writer::StoreAckError::InvalidOutbound(format!(
                "Circle {} acknowledgement has no retained control to resolve its epoch key",
                reference.circle_id
            ))
        }))
    }

    pub(super) async fn stable_circle_acknowledgements_dominating(
        &self,
        circle_id: crate::sync::circle::CircleId,
        current_control: &crate::sync::circle::CircleControlCoord,
        snapshot_cut: &CommitFrontier,
    ) -> Result<Option<Vec<crate::sync::store_commit::CircleAckRef>>, writer::StoreAckError> {
        let devices = self
            .database()
            .active_circle_access_devices(circle_id)
            .await?;
        if devices.is_empty() {
            return Ok(None);
        }
        let mut acknowledgements = Vec::new();
        for device_id in devices {
            let Some(reference) = self
                .database()
                .activated_circle_ack(circle_id, device_id)
                .await?
            else {
                return Ok(None);
            };
            let acknowledgement = self
                .load_circle_acknowledgement(&reference, current_control)
                .await?;
            if !acknowledgement.store_cut.covers(snapshot_cut) {
                return Ok(None);
            }
            acknowledgements.push(reference);
        }
        acknowledgements.sort();
        Ok(Some(acknowledgements))
    }
}
