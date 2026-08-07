use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, coven_database::DbError> {
        coven_database::LocalBlobCleanup::new(&self.database, self.store_dir)
            .drain()
            .await
    }

    pub(crate) fn circles(
        &mut self,
    ) -> crate::sync::store::owner::circles::VerifiedCircleHistory<'_, 'storage> {
        crate::sync::store::owner::circles::VerifiedCircleHistory::new(self)
    }

    pub(crate) async fn discard_circle_operation(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::circle_controls::CircleOperationError> {
        use crate::sync::store::circle_controls::CircleOperationError;

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
                .merge_conflict()
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
                .begin_circle_operation_discard(self.root().clone(), operation_id, nonactivation)
                .await?;
        }
        self.cleanup_circle_operation_candidate(operation_id)
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

    pub(crate) fn circle_activations(
        &mut self,
    ) -> crate::sync::store::owner::circles::activation::CircleActivationVerifier<'_, 'storage>
    {
        crate::sync::store::owner::circles::activation::CircleActivationVerifier::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn circle_packages(
        &mut self,
    ) -> crate::sync::store::owner::circles::packages::CirclePackageReader<'_, 'storage> {
        crate::sync::store::owner::circles::packages::CirclePackageReader::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn circle_acknowledgements(
        &mut self,
    ) -> crate::sync::store::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        crate::sync::store::acknowledgements::CircleAcknowledgementReader::new(
            &self.database,
            self.storage.as_ref(),
            self.history_verifier.verified_root().reference(),
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn circle_snapshots(
        &mut self,
    ) -> crate::sync::store::snapshots::CircleSnapshotReader<'_, 'storage> {
        crate::sync::store::snapshots::CircleSnapshotReader::new(
            &self.database,
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
