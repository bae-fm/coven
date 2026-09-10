use super::*;

impl<'storage> RestoringStore<'storage> {
    #[cfg(test)]
    pub(crate) async fn circle_publication_context_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: coven_protocol::circle::CircleControlCoord,
    ) -> Result<coven_protocol::circle_activation::CircleEpochAccess, coven_database::DbError> {
        self.database
            .circle_publication_context(circle_id, control)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn installed_store_root_for_test(
        &self,
    ) -> Result<Option<StoreRootRef>, coven_database::DbError> {
        self.database.local_store_root_ref().await
    }

    #[cfg(test)]
    pub(crate) async fn replay_baseline_for_test(
        &self,
    ) -> Result<coven_database::RetainedReplayBaseline, coven_database::DbError> {
        self.database.replay_baseline_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn replace_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), coven_database::DbError> {
        self.database
            .replace_replay_authority_for_test(authority_bytes)
            .await
    }

    #[cfg(test)]
    pub(crate) fn schema_version_for_test(&self) -> u32 {
        self.database.schema_version()
    }

    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_counts_for_test(
        &self,
    ) -> Result<(i64, i64, i64), coven_database::DbError> {
        self.database.scoped_snapshot_counts_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), coven_database::DbError> {
        self.database
            .migrated_scoped_snapshot_facts_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn materialized_frontier_for_test(
        &self,
    ) -> Result<
        BTreeMap<String, coven_protocol::store_commit::StoreBatchCommitRef>,
        coven_database::DbError,
    > {
        self.database.materialized_frontier().await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_coverage_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<Option<coven_protocol::circle::CircleBootstrapCoverageRef>, coven_database::DbError>
    {
        self.database.circle_bootstrap_coverage_ref(circle_id).await
    }

    #[cfg(test)]
    pub(crate) async fn circle_control_activation_count_for_test(
        &self,
        circle_id: coven_protocol::circle::CircleId,
    ) -> Result<i64, coven_database::DbError> {
        self.database
            .circle_control_activation_count_for_test(circle_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_replay_inputs_for_test(
        &self,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::StoreBatchCommitRef,
            coven_protocol::circle_activation::VerifiedCircleImage,
        )>,
        coven_database::DbError,
    > {
        self.database.circle_bootstrap_replay_inputs().await
    }
}
