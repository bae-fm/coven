use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    /// The verifier every role-scoped view reads through. Roles whose loads
    /// need nothing but the verifier's own vocabulary use it directly instead
    /// of renaming its operations behind another view.
    pub(crate) fn merge_history(&mut self) -> &mut MergeHistoryVerifier<'storage> {
        &mut self.history_verifier
    }

    pub(crate) async fn drain_local_blob_cleanup(&self) -> Result<bool, crate::database::DbError> {
        crate::database::LocalBlobCleanup::new(&self.database, self.store_dir)
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
    ) -> crate::sync::store::owner::circles::acknowledgements::CircleAcknowledgementReader<
        '_,
        'storage,
    > {
        crate::sync::store::owner::circles::acknowledgements::CircleAcknowledgementReader::new(
            &self.database,
            self.storage.as_ref(),
            self.history_verifier.verified_root().reference(),
        )
    }

    pub(crate) fn circle_snapshots(
        &mut self,
    ) -> crate::sync::store::owner::circles::snapshots::CircleSnapshotReader<'_, 'storage> {
        crate::sync::store::owner::circles::snapshots::CircleSnapshotReader::new(
            &self.database,
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn device_join(
        &mut self,
    ) -> crate::sync::store::owner::device_join::history::DeviceJoinHistory<'_, 'storage> {
        crate::sync::store::owner::device_join::history::DeviceJoinHistory::new(
            self.database.clone(),
            self.storage.as_ref(),
            &mut self.history_verifier,
        )
    }

    pub(crate) fn device_exclusion(
        &mut self,
    ) -> crate::sync::store::owner::device_exclusion::DeviceExclusionHistory<'_, 'storage> {
        crate::sync::store::owner::device_exclusion::DeviceExclusionHistory::new(
            &mut self.history_verifier,
        )
    }

    pub(crate) fn reclaim(&mut self) -> ReclaimHistory<'_, 'storage> {
        ReclaimHistory::new(self)
    }

    pub(crate) async fn reclaim_circle_epoch_access(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        control: &coven_protocol::circle::CircleControlCoord,
    ) -> Result<
        Option<coven_protocol::circle_activation::CircleEpochAccess>,
        crate::database::DbError,
    > {
        self.database
            .circle_epoch_access(self.root().clone(), circle_id, control.clone())
            .await
    }

    pub(crate) async fn reclaim_covered_commits(
        &mut self,
        coverage: &CommitFrontier,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::store_commit::VerifiedStoreBatchCommit,
        )>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history_verifier.load_covered_commits(coverage).await
    }

    pub(crate) async fn reclaim_commit_position_covers(
        &mut self,
        covering: &StoreBatchCommitRef,
        covered: &StoreBatchCommitRef,
    ) -> Result<bool, crate::sync::store::owner::pull::CommitCoverageError> {
        self.history_verifier
            .commit_position_covers(covering, covered)
            .await
    }

    pub(crate) async fn reclaim_authorization(
        &mut self,
        reference: &coven_protocol::reclaim::ReclaimAuthorizationRef,
    ) -> Result<
        crate::sync::store::owner::verification::VerifiedReclaimAuthorization,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history_verifier
            .load_reclaim_authorization(reference)
            .await
    }

    pub(crate) async fn reclaim_device_head(
        &mut self,
        reference: &coven_protocol::store_commit::StoreDeviceHeadRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<coven_protocol::store_commit::StoreDeviceHead>,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history_verifier
            .load_head(reference, registration, commit)
            .await
    }

    pub(crate) async fn reclaim_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &coven_protocol::store_commit::StoreDeviceRegistration,
        previous: Option<&coven_protocol::store_commit::VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            coven_protocol::objects::ObjectSlot,
            Option<coven_protocol::store_commit::StoreDeviceHeadRef>,
        ),
        crate::sync::store::StoreError,
    > {
        self.history_verifier
            .exact_next_announcement_slot(registration_ref, registration, previous)
            .await
    }

    pub(crate) async fn reclaim_snapshot_stability(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<
        crate::database::VerifiedStoreSnapshotStability,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history_verifier
            .verify_snapshot_stability(snapshot)
            .await
    }

    pub(crate) async fn select_reclaim_store_snapshot(
        &mut self,
        candidates: Vec<crate::database::PublishedStoreSnapshot>,
    ) -> Result<Option<SelectedStableStoreSnapshot>, crate::sync::store::owner::pull::StorePullError>
    {
        self.history_verifier
            .select_maximal_stable_store_snapshot(candidates)
            .await
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
