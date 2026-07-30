use super::{CircleOperationError, VerifiedCircleHistory};
use crate::database::StoreDatabase;
use crate::protocol::circle::CircleOperationId;

pub(crate) struct CircleOperationDiscarder<'operation, 'storage> {
    database: StoreDatabase,
    history: VerifiedCircleHistory<'operation, 'storage>,
}

impl<'operation, 'storage> CircleOperationDiscarder<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        history: VerifiedCircleHistory<'operation, 'storage>,
    ) -> Self {
        Self { database, history }
    }

    pub(crate) async fn discard(
        &mut self,
        operation_id: &CircleOperationId,
    ) -> Result<(), CircleOperationError> {
        let journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::Journal(format!("circle operation {operation_id} is absent"))
            })?;
        if !journal.is_discarding() {
            let discard_candidate = self
                .database
                .circle_operation_discard_candidate(operation_id)
                .await?;
            let Some(nonactivation) = self
                .history
                .discard_candidate_nonactivation(
                    &discard_candidate.candidate,
                    discard_candidate.revoked_grant.as_ref(),
                )
                .await?
            else {
                return Err(CircleOperationError::DiscardRequiresNonactivation {
                    operation_id: operation_id.clone(),
                });
            };
            self.database
                .begin_circle_operation_discard(
                    self.history.root().clone(),
                    operation_id,
                    nonactivation,
                )
                .await?;
        }
        self.history
            .cleanup_operation_candidate(operation_id)
            .await
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "Circle operation {operation_id} discard cleanup: {error}"
                ))
            })?;
        self.database
            .finish_circle_operation_discard(operation_id)
            .await?;
        Ok(())
    }
}
