use super::*;

impl AuthorizedMergeStoreEngine<'_> {
    pub(in crate::sync::store_engine) async fn stage_and_publish_ack(
        &self,
        identity: &UserKeypair,
        sync_time: &str,
    ) -> Result<(), SyncCycleFailure> {
        Box::pin(super::super::stage_and_publish_ack(
            self, identity, sync_time,
        ))
        .await
    }
}

impl AcknowledgementEngine for AuthorizedMergeStoreEngine<'_> {
    fn acknowledgement_database(&self) -> &Database {
        self.db()
    }

    fn acknowledgement_policy(&self) -> crate::WritePolicy {
        crate::WritePolicy::MergeConcurrent
    }

    async fn drain_acknowledgements(
        &self,
        identity: &UserKeypair,
    ) -> Result<u64, crate::sync::store_ack::StoreAckError> {
        crate::sync::store_ack::drain_outbound_store_acks(
            self.db(),
            self.storage(),
            None,
            identity,
            Some(&self.membership),
        )
        .await
    }

    async fn stage_acknowledgement(
        &self,
        frontier: CommitFrontier,
        sync_time: String,
        identity: &UserKeypair,
    ) -> Result<crate::sync::store_commit::StoreAck, crate::sync::store_ack::StoreAckError> {
        crate::sync::store_ack::stage_store_ack(
            self.db(),
            self.storage(),
            None,
            frontier,
            sync_time,
            identity,
        )
        .await
    }
}
