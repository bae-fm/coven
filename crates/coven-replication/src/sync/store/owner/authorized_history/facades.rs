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
    ) -> crate::sync::store::owner::RestoringStore<'storage> {
        let database = self.database.clone();
        let storage = self.storage.as_ref();
        let root = self.history_verifier.verified_root().reference().clone();
        let protocol = self.history_verifier.verified_root().object().value.clone();
        crate::sync::store::owner::RestoringStore::from_parts(
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
