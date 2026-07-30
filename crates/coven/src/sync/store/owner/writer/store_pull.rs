use super::*;

impl AuthorizedWriterOperation<'_> {
    pub(crate) async fn pull(
        &mut self,
        store_dir: &StoreDir,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<crate::sync::store::StorePullResult, SyncCycleFailure> {
        let identity = self.writer.identity;
        let membership = self.membership.clone();
        let execution = self
            .history
            .pull(store_dir, &membership, Some(identity), routing_encryption)
            .await
            .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        Ok(false)
    }
}
