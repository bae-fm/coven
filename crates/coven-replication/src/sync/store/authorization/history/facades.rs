use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        self.blob_cache.drain_local_cleanup().await
    }

    pub(crate) async fn pull(
        &mut self,
        membership: &coven_protocol::membership::MembershipChain,
        identity: Option<&UserKeypair>,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<pull::StorePullExecution, pull::StorePullError> {
        pull::AuthorizedPull::load(
            self.pull_history(),
            membership,
            identity,
            routing_encryption,
        )
        .await?
        .execute()
        .await
    }

    /// Read and verify the row data a device-join bootstrap must materialize
    /// for the commits this database does not already cover.
    pub(crate) async fn resolve_device_join_bootstrap(
        &mut self,
        plan: coven_database::DeviceJoinBootstrapPlan,
        membership: &coven_protocol::membership::MembershipChain,
        identity: &UserKeypair,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<coven_database::ResolvedDeviceJoinBootstrap, pull::StorePullError> {
        self.pull_history()
            .resolve_device_join_bootstrap(plan, membership, identity, routing_encryption)
            .await
    }

    /// Seed the verifier from the history this device already retains.
    ///
    /// A walk over retained history that has not been seeded re-reads every
    /// commit, its activation head, and its acknowledgement from the provider —
    /// five reads per commit, serial. On a real provider that is the seconds
    /// per commit an owner spent activating a device join. The rows hold all of
    /// it already; a pull seeds from them at the top of every cycle, and the
    /// write path needs the same seed before it walks.
    pub(crate) async fn seed_retained_history(&mut self) -> Result<(), pull::StorePullError> {
        let root = self.history_verifier.verified_root().reference().clone();
        let retained = self.database.retained_merge_replay_inputs(root).await?;
        self.history_verifier.admit_retained_history(&retained)?;
        let refs = retained
            .iter()
            .map(|materialization| materialization.commit_ref().clone())
            .collect::<Vec<_>>();
        self.history_verifier.verify_refs(refs).await?;
        Ok(())
    }

    /// The provider-operation counter of the storage this reads through, so a
    /// run over it can report each stage's count beside its wall time.
    pub(crate) fn provider_requests(
        &self,
    ) -> Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>> {
        self.storage.provider_requests()
    }

    pub(super) fn pull_history(&mut self) -> pull::PullHistory<'_, 'storage> {
        pull::PullHistory::new(
            self.database.clone(),
            self.storage.as_ref(),
            &mut self.history_verifier,
            &self.blob_source,
            &self.blob_cache,
        )
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::circles::VerifiedCircleHistory<'_, 'storage> {
        crate::sync::store::circles::VerifiedCircleHistory::new(
            self.database.clone(),
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn device_join(
        &mut self,
    ) -> crate::sync::store::device_join::history::DeviceJoinHistory<'_, 'storage> {
        crate::sync::store::device_join::history::DeviceJoinHistory::new(
            self.database.clone(),
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn merge_conflict(
        &mut self,
    ) -> crate::sync::store::merge_conflict::MergeConflictHistory<'_, 'storage> {
        crate::sync::store::merge_conflict::MergeConflictHistory::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn device_exclusion(
        &mut self,
    ) -> crate::sync::store::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        crate::sync::store::device_exclusion::DeviceExclusionHistory::new(
            &mut self.history_verifier,
        )
    }

    pub(crate) fn reclaim(&mut self) -> crate::sync::store::reclaim::ReclaimHistory<'_, 'storage> {
        crate::sync::store::reclaim::ReclaimHistory::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn restore_history(&self) -> RestoreHistory<'_, 'storage> {
        RestoreHistory::new(&self.history_verifier)
    }

    pub(crate) fn owner_promotion(&mut self) -> OwnerPromotionHistory<'_, 'storage> {
        OwnerPromotionHistory::new(&mut self.history_verifier)
    }

    pub(crate) fn bind_restore(
        self,
        membership: coven_protocol::membership::MembershipChain,
        identity: UserKeypair,
    ) -> crate::sync::store::authorization::RestoringStore<'storage> {
        let database = self.database.clone();
        let storage = self.storage.as_ref();
        let root = self.history_verifier.verified_root().reference().clone();
        let protocol = self.history_verifier.verified_root().object().value.clone();
        crate::sync::store::authorization::RestoringStore::from_parts(
            self, database, storage, root, protocol, membership, identity,
        )
    }

    pub(crate) async fn provider_binding(
        &self,
    ) -> Result<
        coven_protocol::objects::ResolvedProviderBinding,
        coven_protocol::objects::StorageError,
    > {
        self.storage.provider_binding().await
    }
}
