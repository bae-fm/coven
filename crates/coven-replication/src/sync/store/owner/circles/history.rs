pub(crate) struct VerifiedCircleHistory<'operation, 'storage> {
    history: &'operation mut crate::sync::store::owner::history::AuthorizedStoreHistory<'storage>,
}

impl<'operation, 'storage> VerifiedCircleHistory<'operation, 'storage> {
    pub(crate) fn new(
        history: &'operation mut crate::sync::store::owner::history::AuthorizedStoreHistory<
            'storage,
        >,
    ) -> Self {
        Self { history }
    }

    pub(crate) fn activations(
        &mut self,
    ) -> super::activation::CircleActivationVerifier<'_, 'storage> {
        self.history.circle_activations()
    }

    pub(crate) fn packages(&mut self) -> super::packages::CirclePackageReader<'_, 'storage> {
        self.history.circle_packages()
    }

    pub(crate) fn acknowledgements(
        &mut self,
    ) -> super::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        self.history.circle_acknowledgements()
    }

    pub(crate) fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.history.root()
    }

    pub(crate) async fn authenticate_commit_bytes(
        &mut self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history
            .authenticate_commit_bytes(reference, bytes)
            .await
    }

    pub(crate) async fn load_commit(
        &mut self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history.load_commit(reference).await
    }

    pub(crate) async fn retained_device_state_for_order(
        &self,
        order: &coven_protocol::store_commit::StoreCommitOrder,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreDeviceStateRef,
            coven_protocol::store_commit::ResolvedStoreDeviceState,
        ),
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history.retained_device_state_for_order(order).await
    }

    pub(crate) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::sync::store::owner::history::abandonment::ExcludedCandidateHeadObservation,
        crate::sync::store::StoreError,
    > {
        self.history
            .observe_excluded_candidate_head(candidate, candidate_commit, candidate_object)
            .await
    }

    pub(crate) async fn cleanup_operation_candidate(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        self.history
            .cleanup_circle_operation_candidate(operation_id)
            .await
    }

    pub(crate) async fn prepare_successor(
        &mut self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &coven_protocol::membership::MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: coven_protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::owner::verified_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::owner::verified_history::PreparedMergeHistorySuccessor,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history
            .prepare_merge_history_successor(
                commit,
                membership,
                recovery_author,
                state_after,
                evidence,
            )
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn snapshots(&mut self) -> super::snapshots::CircleSnapshotReader<'_, 'storage> {
        self.history.circle_snapshots()
    }
}
