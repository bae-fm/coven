use super::STORE_COMMIT_STEP;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use crate::sync::store::CircleOperationError;
use coven_database::StoreDatabase;
use coven_protocol::circle_journal::CircleOperationJournal;
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
            self.history,
        )
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

    pub(crate) fn admit_materialized_publication(
        &mut self,
        materialization: &coven_database::OwnedVerifiedMergeMaterialization,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        self.history
            .admit_retained_history(std::slice::from_ref(materialization))
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

    pub(crate) async fn discard_operation(
        &mut self,
        operation_id: &coven_protocol::circle::CircleOperationId,
    ) -> Result<(), super::CircleOperationError> {
        use super::CircleOperationError;

        let _author = self.database.author_own_stream().await;
        let mut journal = self
            .database
            .circle_operation(operation_id)
            .await?
            .ok_or_else(|| {
                CircleOperationError::JournalState(format!(
                    "circle operation {operation_id} is absent"
                ))
            })?;
        if !journal.is_discarding() {
            let commit = self
                .history
                .authenticate_bytes(
                    journal.operation().commit_ref(),
                    &journal.operation().commit().to_bytes(),
                )
                .await?;
            let ground = self
                .discard_ground(&journal, &commit)
                .await?
                .ok_or_else(|| CircleOperationError::DiscardRequiresNonactivation {
                    operation_id: operation_id.clone(),
                })?;
            self.database
                .begin_circle_operation_discard(journal.clone(), ground)
                .await?;
            journal.begin_discard()?;
        }
        let targets = self
            .database
            .circle_operation_discard_targets(journal.clone())
            .await?;
        crate::sync::store::authorization::delete_candidate_cleanup_targets::<
            CircleOperationError,
        >(self.storage, targets)
        .await?;
        self.database
            .complete_circle_operation_discard(journal)
            .await?;
        Ok(())
    }

    /// What establishes that this candidate can never activate, or `None` when
    /// the outcome is genuinely unknown and the operation must stay durable.
    ///
    /// Three grounds, cheapest first. A candidate whose commit object never
    /// reached the provider cannot be named by any publication entry, and the
    /// journal records which uploads completed. A candidate whose own
    /// coordinate is held by a different accepted commit can never take that
    /// coordinate — the fact that refused it at publication. Otherwise the
    /// author's write grant must have been retired.
    async fn discard_ground(
        &mut self,
        journal: &CircleOperationJournal,
        commit: &coven_protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<Option<coven_database::CircleDiscardGround>, CircleOperationError> {
        if !journal.uploaded.contains(STORE_COMMIT_STEP) {
            return Ok(Some(coven_database::CircleDiscardGround::Unpublished));
        }
        let candidate = commit.reference();
        if let Some(accepted) = self
            .database
            .accepted_commit_at(candidate.coord.clone())
            .await?
        {
            if &accepted != candidate {
                let publication = self
                    .database
                    .store_current_publication()
                    .await?
                    .record()
                    .accepted()
                    .cloned()
                    .ok_or_else(|| {
                        CircleOperationError::InvalidState(
                            "accepted Store history has no publication boundary".to_string(),
                        )
                    })?;
                let coverage = coven_protocol::store_commit::CommitFrontier::from_refs(
                    self.database.materialized_frontier().await?,
                )
                .map_err(CircleOperationError::from)?;
                return Ok(Some(coven_database::CircleDiscardGround::PositionTaken {
                    publication,
                    coverage,
                    accepted,
                }));
            }
        }
        Ok(self
            .history
            .candidate_grant_retirement(&self.database, commit)
            .await?
            .map(|(membership, publication)| {
                coven_database::CircleDiscardGround::AuthorityRetirement {
                    membership,
                    publication,
                }
            }))
    }

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
