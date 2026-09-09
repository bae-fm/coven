use coven_database::PreparedStoreWriteCommit;
use coven_protocol::store_commit::{StoreBatchCommitRef, StoreProtocolRoot};

use super::{TestError, TestStore};

impl TestStore {
    pub async fn prepare_uploaded_changeset_for_test(
        &self,
        name: &str,
        sequence: u64,
        changeset: &[u8],
    ) -> Result<(StoreProtocolRoot, PreparedStoreWriteCommit), TestError> {
        self.ensure_producer_registered(name).await?;
        let producer = self
            .producers
            .lock()
            .await
            .by_name
            .get(name)
            .expect("registered test producer exists")
            .clone();
        let pending = producer
            .prepare_uploaded_changeset_for_test(sequence, changeset.to_vec())
            .await?;
        Ok((producer.protocol_root_for_test().clone(), pending))
    }

    pub async fn complete_uploaded_changeset_for_test(
        &self,
        name: &str,
    ) -> Result<StoreBatchCommitRef, TestError> {
        let producer = self
            .producers
            .lock()
            .await
            .by_name
            .get(name)
            .ok_or_else(|| TestError::invariant("prepared test producer is absent"))?
            .clone();
        if !producer.publish_pending_store_database().await? {
            return Err(TestError::invariant(
                "prepared test producer has no pending write",
            ));
        }
        producer
            .latest_local_store_position()
            .await?
            .ok_or_else(|| TestError::invariant("completed test producer has no published commit"))
    }
}
