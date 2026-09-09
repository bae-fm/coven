use super::*;

impl StoreDatabase {
    pub async fn latest_local_write_facts_for_test(&self) -> Result<(String, i64, i64), DbError> {
        self.call_store(|session| session.latest_local_write_facts_for_test())
            .await
    }

    pub async fn install_retracted_device_state_failure_trigger_for_test(
        &self,
    ) -> Result<(), DbError> {
        self.call_store(|session| session.install_retracted_device_state_failure_trigger_for_test())
            .await
    }

    pub async fn prepared_write_count_for_test(&self, write_id: WriteId) -> Result<i64, DbError> {
        self.call_store(move |session| session.prepared_write_count_for_test(&write_id))
            .await
    }

    pub async fn begin_remote_candidate_nonactivation_for_test(
        &self,
        object_id: coven_protocol::store_commit::ObjectHash,
        nonactivation: coven_protocol::remote_object::CandidateNonactivation,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.begin_remote_candidate_nonactivation_for_test(object_id, nonactivation)
        })
        .await
    }

    pub async fn install_indexed_shared_blobs_for_test(
        &self,
        write_id: WriteId,
        records: Vec<coven_protocol::remote_object::RemoteObjectRecord>,
    ) -> Result<(), DbError> {
        self.call_store(move |session| {
            session.install_indexed_shared_blobs_for_test(&write_id, records)
        })
        .await
    }
}
