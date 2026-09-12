use coven_database::StoreDatabase;
use coven_protocol::store_commit::{
    CommitFrontier, StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    VerifiedStoreBatchCommit,
};
use coven_storage::CloudSyncObjectStorage;

use crate::sync::store::acknowledgements::StoreAckError;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use crate::sync::store::snapshots as snapshot;

/// The reads reclamation performs, holding exactly the capabilities they use:
/// the database that records materialization and coverage, the storage the
/// snapshot and acknowledgement objects live in, and the verifier that
/// authenticates them.
pub(crate) struct ReclaimHistory<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn CloudSyncObjectStorage,
    history: &'operation mut MergeHistoryVerifier<'storage>,
}

/// One device's per-Circle snapshot stream, read in generation order.
pub(crate) struct CircleSnapshotStream {
    pub(crate) author_registration: StoreDeviceRegistrationRef,
    pub(crate) generations: Vec<(
        coven_protocol::store_commit::CircleSnapshotRef,
        coven_protocol::store_commit::CircleSnapshotMeta,
    )>,
}

#[derive(Clone)]
pub(crate) struct SelectedCircleSnapshot {
    pub(crate) author_registration: StoreDeviceRegistrationRef,
    pub(crate) reference: coven_protocol::store_commit::CircleSnapshotRef,
    pub(crate) meta: coven_protocol::store_commit::CircleSnapshotMeta,
    pub(crate) acknowledgements: Vec<coven_protocol::store_commit::CircleAckRef>,
}

impl<'operation, 'storage> ReclaimHistory<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn CloudSyncObjectStorage,
        history: &'operation mut MergeHistoryVerifier<'storage>,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    fn circle_acknowledgements(
        &mut self,
    ) -> crate::sync::store::acknowledgements::CircleAcknowledgementReader<'_, 'storage> {
        crate::sync::store::acknowledgements::CircleAcknowledgementReader::new(
            self.database,
            self.storage,
            self.history,
        )
    }

    fn circle_snapshots(
        &mut self,
    ) -> crate::sync::store::snapshots::CircleSnapshotReader<'_, 'storage> {
        crate::sync::store::snapshots::CircleSnapshotReader::new(
            self.database,
            self.storage,
            self.history,
        )
    }

    pub(crate) async fn load_circle_acknowledgement(
        &mut self,
        reference: &coven_protocol::store_commit::CircleAckRef,
    ) -> Result<coven_protocol::store_commit::CircleAck, StoreAckError> {
        self.circle_acknowledgements().load(reference).await
    }

    pub(crate) async fn stable_circle_acknowledgements(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        coverage: &CommitFrontier,
    ) -> Result<Option<Vec<coven_protocol::store_commit::CircleAckRef>>, StoreAckError> {
        self.circle_acknowledgements()
            .stable_dominating(circle_id, coverage)
            .await
    }

    pub(crate) async fn load_circle_snapshot_stream_refs(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        access: &coven_protocol::circle_activation::CircleEpochAccess,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
    ) -> Result<
        Vec<(
            coven_protocol::store_commit::CircleSnapshotRef,
            coven_protocol::store_commit::CircleSnapshotMeta,
        )>,
        snapshot::SnapshotError,
    > {
        self.circle_snapshots()
            .load_stream_refs(circle_id, access, registration_ref, registration)
            .await
    }

    pub(crate) fn snapshot_authenticates_reclaim_authorization(
        &self,
        authorization: &coven_protocol::reclaim::ReclaimAuthorizationRef,
        activation: &StoreBatchCommitRef,
    ) -> bool {
        self.history
            .snapshot_authenticates_reclaim_authorization(authorization, activation)
    }

    pub(crate) async fn load_ref(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<VerifiedStoreBatchCommit, crate::sync::store::pull::StorePullError> {
        self.history.load_ref(reference).await
    }

    pub(crate) fn verify_commit_acceptance(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        self.history.verify_commit_acceptance(reference)
    }

    pub(crate) async fn current_accepted_snapshot(
        &mut self,
    ) -> Result<
        Option<crate::sync::store::commit_verification::merge_history::AcceptedStoreSnapshot>,
        crate::sync::store::pull::StorePullError,
    > {
        self.history.current_accepted_snapshot().await
    }

    pub(crate) async fn store_blob_is_reclaimable(
        &mut self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, crate::sync::store::pull::StorePullError> {
        let Some(snapshot) = self.history.current_accepted_snapshot().await? else {
            return Ok(false);
        };
        self.history
            .store_snapshot_blob_is_reclaimable(snapshot.snapshot(), blob)
            .await
    }

    /// The Circle's live packages this coverage stands over, read from
    /// reclamation's own live state rather than re-derived by loading the
    /// commits the coverage names. A package the live state no longer holds is
    /// one a completion released, and one whose activating commit object is
    /// gone is still named here.
    pub(crate) fn circle_package_targets(
        &self,
        circle_id: coven_protocol::circle::CircleId,
        coverage: &CommitFrontier,
    ) -> Result<
        Vec<(
            StoreBatchCommitRef,
            coven_protocol::store_commit::CirclePackageRef,
        )>,
        crate::sync::store::pull::StorePullError,
    > {
        let mut targets = self
            .history
            .live_reclaim_state()?
            .packages
            .into_values()
            .filter_map(|retained| match retained.package {
                coven_protocol::reclaim::AudienceBlobBindingPackage::Circle(package)
                    if package.circle_id == circle_id
                        && coverage.covers_commit(&retained.activation) =>
                {
                    Some((retained.activation, package))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        targets.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(targets)
    }

    pub(crate) async fn commit_position_covers(
        &mut self,
        covering: &StoreBatchCommitRef,
        covered: &StoreBatchCommitRef,
    ) -> Result<bool, crate::sync::store::pull::CommitCoverageError> {
        self.history.commit_position_covers(covering, covered).await
    }

    pub(crate) async fn snapshot_covers_target(
        &mut self,
        coverage: &CommitFrontier,
        target: &StoreBatchCommitRef,
    ) -> Result<bool, crate::sync::store::pull::CommitCoverageError> {
        match coverage.0.get(&target.coord.stream_id) {
            Some(covering) => self.commit_position_covers(covering, target).await,
            None => Ok(false),
        }
    }

    /// Load every supplied device's Circle snapshot stream using the current
    /// control's retained keyring. A stream can span epochs, so a key restricted
    /// to one epoch cannot decrypt its older generations.
    pub(crate) async fn load_circle_snapshot_streams(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        current_control: &coven_protocol::circle::CircleControlCoord,
        registrations: &[coven_protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<Vec<CircleSnapshotStream>, crate::sync::store::StoreReclaimError> {
        let access = self
            .database
            .circle_epoch_access(
                self.history.verified_root().reference().clone(),
                circle_id,
                current_control.clone(),
            )
            .await?
            .ok_or_else(|| {
                crate::sync::store::StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} snapshot key is not resolvable from retained controls"
                ))
            })?;
        let mut streams = Vec::new();
        for registration in registrations {
            let registration_ref = registration.reference();
            let registration = registration.value();
            // A device's stream can span epochs. If this reader cannot resolve one
            // generation's key, that stream cannot establish current coverage.
            let generations = match self
                .load_circle_snapshot_stream_refs(
                    circle_id,
                    &access,
                    registration_ref,
                    registration,
                )
                .await
            {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(
                        circle_id = %circle_id,
                        device_id = %registration.device_id,
                        "skip Circle snapshot stream for reclaim coverage: {error}"
                    );
                    continue;
                }
            };
            streams.push(CircleSnapshotStream {
                author_registration: registration_ref.clone(),
                generations,
            });
        }
        Ok(streams)
    }

    /// Return every snapshot generation whose cut every device holding active
    /// Circle access has acknowledged.
    pub(crate) async fn stable_circle_snapshots(
        &mut self,
        circle_id: coven_protocol::circle::CircleId,
        streams: &[CircleSnapshotStream],
    ) -> Result<Vec<SelectedCircleSnapshot>, crate::sync::store::StoreReclaimError> {
        let mut stable = Vec::new();
        for stream in streams {
            for (reference, meta) in &stream.generations {
                if let Some(acknowledgements) = self
                    .stable_circle_acknowledgements(circle_id, &meta.bootstrap.coverage)
                    .await
                    .map_err(crate::sync::store::StoreReclaimError::from)?
                {
                    stable.push(SelectedCircleSnapshot {
                        author_registration: stream.author_registration.clone(),
                        reference: reference.clone(),
                        meta: meta.clone(),
                        acknowledgements,
                    });
                }
            }
        }
        Ok(stable)
    }

    pub(crate) async fn load_reclaim_authorization(
        &mut self,
        reference: &coven_protocol::reclaim::ReclaimAuthorizationRef,
    ) -> Result<
        crate::sync::store::commit_verification::commit::VerifiedReclaimAuthorization,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.load_reclaim_authorization(reference).await
    }

    pub(crate) async fn verify_currently_materialized(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<(), crate::sync::store::pull::StorePullError> {
        use crate::sync::store::pull::{commit_stream_id, MaterializedCheck, StorePullError};

        let stream_id = commit_stream_id(&reference.coord);
        let coverage = self.database.snapshot_coverage_frontier().await?;
        let status = crate::sync::store::pull::materialized_reference_status(
            self.database,
            self.history,
            &coverage,
            &stream_id,
            reference,
        )
        .await?;
        match status {
            MaterializedCheck::Yes => Ok(()),
            MaterializedCheck::Missing => Err(StorePullError::InvalidState(
                "Merge activation commit is absent from current accepted history".to_string(),
            )),
            MaterializedCheck::Held(reason) => Err(StorePullError::InvalidState(format!(
                "Merge activation commit is not current accepted history: {reason:?}"
            ))),
        }
    }

    pub(crate) async fn load_registration(
        &mut self,
        reference: &StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<StoreDeviceRegistration>,
        coven_protocol::objects::StoreObjectError,
    > {
        self.history.load_registration(reference).await
    }

    #[cfg(test)]
    pub(crate) async fn load_store_snapshot(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        reference: &coven_protocol::store_commit::StoreSnapshotRef,
    ) -> Result<
        (
            coven_protocol::store_commit::StoreSnapshotRef,
            coven_protocol::store_commit::SnapshotMeta,
        ),
        coven_protocol::objects::StoreObjectError,
    > {
        self.history
            .load_store_snapshot(registration_ref, registration, reference)
            .await
    }
}
