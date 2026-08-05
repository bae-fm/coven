use super::*;

impl<'storage> RestoringStore<'storage> {
    #[cfg(test)]
    pub(crate) async fn install_device_join_bootstrap_for_test(
        &self,
        plan: crate::database::DeviceJoinBootstrapPlan,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .install_device_join_bootstrap(self.root.clone(), plan)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn installed_store_root_for_test(
        &self,
    ) -> Result<Option<StoreRootRef>, crate::database::DbError> {
        self.database.local_store_root_ref().await
    }

    #[cfg(test)]
    pub(crate) async fn generation_zero_replay_baseline_for_test(
        &self,
    ) -> Result<crate::database::RetainedReplayBaseline, crate::database::DbError> {
        self.database
            .generation_zero_replay_baseline_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn replace_generation_zero_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .replace_generation_zero_replay_authority_for_test(authority_bytes)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn read_local_blob_for_test(
        &self,
        store_dir: &crate::store_dir::StoreDir,
        table: &str,
        row_id: &str,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        let reference = self.database.row_blob_ref(table, row_id).await?;
        crate::sync::test_owner_graph::TestOwnerGraph::new(self.database.clone(), store_dir.clone())
            .local_access()
            .read(&reference)
            .await
    }

    #[cfg(test)]
    pub(crate) fn schema_version_for_test(&self) -> u32 {
        self.database.schema_version()
    }

    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_counts_for_test(
        &self,
    ) -> Result<(i64, i64, i64), crate::database::DbError> {
        self.database.scoped_snapshot_counts_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), crate::database::DbError> {
        self.database
            .migrated_scoped_snapshot_facts_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn materialized_frontier_for_test(
        &self,
    ) -> Result<
        BTreeMap<String, crate::protocol::store_commit::StoreBatchCommitRef>,
        crate::database::DbError,
    > {
        self.database.materialized_frontier().await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_coverage_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleBootstrapCoverageRef>, crate::database::DbError>
    {
        self.database.circle_bootstrap_coverage_ref(circle_id).await
    }

    #[cfg(test)]
    pub(crate) async fn circle_control_activation_count_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<i64, crate::database::DbError> {
        self.database
            .circle_control_activation_count_for_test(circle_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_replay_inputs_for_test(
        &self,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::protocol::circle_activation::VerifiedCircleImage,
        )>,
        crate::database::DbError,
    > {
        self.database.circle_bootstrap_replay_inputs().await
    }

    #[cfg(test)]
    pub(crate) async fn transfer_prepared_write_from_for_test(
        &self,
        source: &StoreDatabase,
        write_id: &crate::WriteId,
    ) -> Result<(), crate::database::DbError> {
        source
            .transfer_prepared_write_to_for_test(&self.database, write_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn blocked_merge_candidate_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Option<crate::database::BlockedMergeCandidate>, crate::database::DbError> {
        self.database.blocked_merge_candidate(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn tamper_author_exclusion_locator_for_test(
        &self,
        exclusion: &crate::protocol::store_commit::StoreDeviceExclusionRef,
        candidate: &crate::protocol::store_commit::StoreBatchCommitRef,
        tamper: crate::database::AuthorExclusionLocatorTamper,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .tamper_author_exclusion_locator_for_test(exclusion, candidate, tamper)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn author_exclusion_activation_for_candidate_for_test(
        &self,
        candidate: crate::protocol::store_commit::StoreBatchCommitRef,
        author: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<Option<crate::database::AuthorExclusionActivationLocator>, crate::database::DbError>
    {
        self.database
            .author_exclusion_activation_for_candidate(self.root.clone(), candidate, author)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn author_exclusion_activation_evidence_for_test(
        &self,
        exclusion: &crate::protocol::store_commit::StoreDeviceExclusionRef,
    ) -> Result<(String, String), crate::database::DbError> {
        self.database
            .author_exclusion_activation_evidence_for_test(exclusion)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn merge_candidate_cleanup_pending_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<bool, crate::database::DbError> {
        self.database
            .merge_candidate_cleanup_pending(write_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn abandon_merge_candidate_for_test(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<
        crate::sync::store::owner::history::abandonment::MergeCandidateAbandonment,
        StoreError,
    > {
        self.history
            .abandon_excluded_merge_candidate(write_id)
            .await
            .and_then(|result| {
                result.ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "restored candidate has no verified exclusion authority".to_string(),
                    )
                })
            })
    }
}
