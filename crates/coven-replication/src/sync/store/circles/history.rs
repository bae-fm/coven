use crate::sync::store::authorization::history::{cleanup, retained};
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use coven_database::StoreDatabase;
use coven_storage::CloudSyncObjectStorage;

/// The reads and history compositions the Circle subsystem performs, over the
/// four capabilities they need.
pub(crate) struct VerifiedCircleHistory<'operation, 'storage> {
    database: StoreDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> VerifiedCircleHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    pub(crate) fn activations(
        &mut self,
    ) -> super::activation::CircleActivationVerifier<'_, 'storage> {
        super::activation::CircleActivationVerifier::new(&self.database, self.storage, self.history)
    }

    pub(crate) fn packages(&mut self) -> super::packages::CirclePackageReader<'_, 'storage> {
        super::packages::CirclePackageReader::new(&self.database, self.storage, self.history)
    }

    pub(crate) fn acknowledgements(
        &mut self,
    ) -> crate::sync::store::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        crate::sync::store::acknowledgements::CircleAcknowledgementReader::new(
            &self.database,
            self.storage,
            self.history.verified_root().reference(),
        )
    }

    pub(crate) fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        self.history.verified_root().reference()
    }

    pub(crate) async fn authenticate_commit_bytes(
        &mut self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
        bytes: &[u8],
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.authenticate_bytes(reference, bytes).await
    }

    pub(crate) async fn load_commit(
        &mut self,
        reference: &coven_protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<
        coven_protocol::store_commit::VerifiedStoreBatchCommit,
        crate::sync::store::pull::StorePullError,
    > {
        self.history.load_ref(reference).await
    }

    pub(crate) async fn observe_excluded_candidate_head(
        &mut self,
        candidate: &coven_protocol::store_commit::StoreDeviceHead,
        candidate_commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        candidate_object: &coven_protocol::objects::ExactObjectRef,
    ) -> Result<
        crate::sync::store::merge_conflict::ExcludedCandidateHeadObservation,
        crate::sync::store::StoreError,
    > {
        crate::sync::store::merge_conflict::MergeConflictHistory::new(
            &self.database,
            self.storage,
            self.history,
        )
        .observe_excluded_candidate_head(candidate, candidate_commit, candidate_object)
        .await
    }

    pub(crate) async fn discard_operation(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), super::CircleOperationError> {
        use super::CircleOperationError;

        let journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::JournalState(format!(
                    "circle operation {operation_id} is absent"
                ))
            })?;
        if !journal.is_discarding() {
            let discard_candidate = self
                .database
                .circle_operation_discard_candidate(operation_id)
                .await?;
            let Some(nonactivation) =
                crate::sync::store::merge_conflict::MergeConflictHistory::new(
                    &self.database,
                    self.storage,
                    self.history,
                )
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
        self.cleanup_operation_candidate(operation_id).await?;
        self.database
            .finish_circle_operation_discard(operation_id)
            .await?;
        Ok(())
    }

    pub(crate) async fn cleanup_operation_candidate(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        cleanup::cleanup_circle_operation_candidate(
            &self.database,
            self.storage,
            self.history,
            operation_id,
        )
        .await
    }

    pub(crate) async fn prepare_successor(
        &mut self,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
        membership: &coven_protocol::membership::MembershipChain,
        recovery_author: Option<&coven_protocol::store_commit::StoreDeviceRegistrationRef>,
        state_after: coven_protocol::store_commit::ResolvedStoreDeviceState,
        evidence: crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence,
    ) -> Result<
        crate::sync::store::commit_verification::merge_history::PreparedMergeHistorySuccessor,
        crate::sync::store::pull::StorePullError,
    > {
        retained::prepare_merge_history_successor(
            &self.database,
            self.history,
            commit,
            membership,
            recovery_author,
            state_after,
            evidence,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn snapshots(
        &mut self,
    ) -> crate::sync::store::snapshots::CircleSnapshotReader<'_, 'storage> {
        crate::sync::store::snapshots::CircleSnapshotReader::new(
            &self.database,
            self.storage,
            self.history,
        )
    }
}
