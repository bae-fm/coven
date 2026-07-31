use crate::database::StoreDatabase;
use crate::protocol::store_commit::{
    CommitFrontier, StoreBatchCommitRef, StoreDeviceHead, StoreDeviceHeadRef,
    StoreDeviceRegistration, StoreDeviceRegistrationRef, VerifiedStoreBatchCommit,
};

pub(crate) struct ReclaimHistory<'operation, 'storage> {
    database: StoreDatabase,
    storage: &'storage dyn crate::storage::SyncStorage,
    root: &'operation crate::protocol::store_commit::StoreRootRef,
    history: &'operation mut super::super::verified_history::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> ReclaimHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: StoreDatabase,
        storage: &'storage dyn crate::storage::SyncStorage,
        root: &'operation crate::protocol::store_commit::StoreRootRef,
        history: &'operation mut super::super::verified_history::MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            root,
            history,
        }
    }

    pub(crate) fn circle_acknowledgements(
        &mut self,
    ) -> super::super::circles::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        super::super::circles::acknowledgements::CircleAcknowledgementReader::new(
            &self.database,
            self.storage,
            self.root,
        )
    }

    pub(crate) fn circle_snapshots(
        &mut self,
    ) -> super::super::circles::snapshots::CircleSnapshotReader<'_, 'storage> {
        super::super::circles::snapshots::CircleSnapshotReader::new(
            &self.database,
            self.storage,
            self.root,
            self.history,
        )
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, crate::sync::store::owner::pull::StorePullError> {
        self.history.load_ref(reference).await
    }

    pub(crate) async fn load_covered_commits(
        &mut self,
        coverage: &CommitFrontier,
    ) -> Result<
        Vec<(StoreBatchCommitRef, VerifiedStoreBatchCommit)>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history.load_covered_commits(coverage).await
    }

    pub(crate) async fn store_package_targets(
        &mut self,
        coverage: &CommitFrontier,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            crate::protocol::store_commit::StorePackageRef,
        )>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let mut targets = std::collections::BTreeMap::new();
        for (reference, commit) in self.load_covered_commits(coverage).await? {
            if let Some(package) = commit.value().store_package().cloned() {
                targets.insert(reference, package);
            }
        }
        Ok(targets.into_iter().collect())
    }

    pub(crate) async fn circle_package_targets(
        &mut self,
        circle_id: crate::protocol::circle::CircleId,
        coverage: &CommitFrontier,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            crate::protocol::store_commit::CirclePackageRef,
        )>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        let mut targets = std::collections::BTreeMap::new();
        for (reference, commit) in self.load_covered_commits(coverage).await? {
            if let Some(package) = commit
                .value()
                .circle_packages()
                .iter()
                .find(|package| package.circle_id == circle_id)
            {
                targets.insert(reference, package.clone());
            }
        }
        Ok(targets.into_iter().collect())
    }

    pub(crate) async fn commit_position_covers(
        &mut self,
        covering: &StoreBatchCommitRef,
        covered: &StoreBatchCommitRef,
    ) -> Result<bool, crate::sync::store::owner::pull::CommitCoverageError> {
        self.history.commit_position_covers(covering, covered).await
    }

    pub(crate) async fn load_reclaim_authorization(
        &mut self,
        reference: &crate::sync::store::ReclaimAuthorizationRef,
    ) -> Result<
        crate::sync::store::owner::verification::VerifiedReclaimAuthorization,
        crate::storage::StoreObjectError,
    > {
        self.history.load_reclaim_authorization(reference).await
    }

    pub(crate) async fn load_head(
        &mut self,
        reference: &StoreDeviceHeadRef,
        registration: &StoreDeviceRegistration,
        commit: &StoreBatchCommitRef,
    ) -> Result<crate::storage::VerifiedObject<StoreDeviceHead>, crate::storage::StoreObjectError>
    {
        self.history
            .load_head(reference, registration, commit)
            .await
    }

    pub(crate) async fn exact_next_announcement_slot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        previous: Option<&VerifiedStoreBatchCommit>,
    ) -> Result<
        (
            crate::storage::cloud::ObjectSlot,
            Option<StoreDeviceHeadRef>,
        ),
        crate::sync::store::StoreError,
    > {
        self.history
            .exact_next_announcement_slot(registration_ref, registration, previous)
            .await
    }

    pub(crate) async fn verify_currently_materialized(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), crate::sync::store::owner::pull::StorePullError> {
        use crate::sync::store::owner::pull::{
            commit_stream_id, HeldStorePositionReason, MaterializedCheck, StorePullError,
        };

        let stream_id = commit_stream_id(&reference.coord);
        let coverage = self.database.snapshot_coverage_frontier().await?;
        let status = if let Some(actual) = self
            .database
            .exact_materialized_ref(&stream_id, reference.coord.sequence())
            .await?
        {
            if actual == *reference {
                MaterializedCheck::Yes
            } else {
                MaterializedCheck::Held(HeldStorePositionReason::HashMismatch {
                    referenced_device_id: stream_id.clone(),
                    referenced_commit: reference.clone(),
                    materialized_hash: actual.commit_hash,
                })
            }
        } else {
            self.history
                .covered_reference_status(&coverage, &stream_id, reference)
                .await
        };
        match status {
            MaterializedCheck::Yes => Ok(()),
            MaterializedCheck::Missing => Err(StorePullError::Database(
                "Merge activation commit is absent from current accepted history".to_string(),
            )),
            MaterializedCheck::Held(reason) => Err(StorePullError::Database(format!(
                "Merge activation commit is not current accepted history: {reason:?}"
            ))),
        }
    }

    pub(crate) async fn load_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<
        crate::storage::VerifiedObject<StoreDeviceRegistration>,
        crate::storage::StoreObjectError,
    > {
        self.history.load_registration(reference).await
    }

    pub(crate) async fn load_store_snapshot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &crate::protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        (
            crate::protocol::store_commit::StoreSnapshotRef,
            crate::protocol::store_commit::SnapshotMeta,
        ),
        crate::storage::StoreObjectError,
    > {
        self.history
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }

    pub(crate) async fn verify_snapshot_stability(
        &mut self,
        snapshot: &crate::database::PublishedStoreSnapshot,
    ) -> Result<
        crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
        crate::sync::store::owner::pull::StorePullError,
    > {
        self.history.verify_snapshot_stability(snapshot).await
    }

    pub(crate) async fn select_maximal_stable_store_snapshot(
        &mut self,
        candidates: Vec<crate::database::PublishedStoreSnapshot>,
    ) -> Result<
        Option<crate::sync::store::owner::writer::snapshot::SelectedStableStoreSnapshot>,
        crate::sync::store::owner::pull::StorePullError,
    > {
        crate::sync::store::owner::writer::snapshot::
            select_maximal_stable_store_snapshot_with_history(
                self.history,
                candidates,
            )
            .await
    }
}
