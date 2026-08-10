use super::*;

impl StoreDatabase {
    pub async fn sole_author_exclusion_activation_evidence_for_test(
        &self,
    ) -> Result<(String, String, String, String), DbError> {
        self.call_store(|session| session.sole_author_exclusion_activation_evidence_for_test())
            .await
    }

    pub async fn author_exclusion_activation_evidence_for_test(
        &self,
        exclusion: &StoreDeviceExclusionRef,
    ) -> Result<(String, String), DbError> {
        let exclusion = serde_json::to_string(exclusion)
            .map_err(|error| DbError::context("serialize exclusion ref", error))?;
        self.call_store(move |session| {
            session.author_exclusion_activation_evidence_for_test(&exclusion)
        })
        .await
    }

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

    pub async fn tamper_author_exclusion_locator_for_test(
        &self,
        exclusion: &StoreDeviceExclusionRef,
        candidate: &StoreBatchCommitRef,
        tamper: AuthorExclusionLocatorTamper,
    ) -> Result<(), DbError> {
        let exclusion = exclusion.clone();
        let candidate = candidate.clone();
        self.call_store(move |session| {
            session.tamper_author_exclusion_locator_for_test(exclusion, &candidate, tamper)
        })
        .await
    }
}
