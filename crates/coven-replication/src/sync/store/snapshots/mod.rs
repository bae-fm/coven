//! Durable exact Store snapshot publication.

mod circle;
mod image;
mod publication;

pub(crate) use circle::{CircleSnapshotReader, CircleSnapshotWriter};
pub(crate) use publication::AuthorizedSnapshotPublication;

pub use image::{PreparedDeviceJoinSnapshot, PreparedSnapshotBootstrap, SnapshotError};

use coven_database::CreatedSnapshot;

use tracing::info;

use super::AuthorizedWriterOperation;
use crate::sync::store::commit_publication::{LocalStoreWriter, SnapshotHistoryConstruction};
use coven_database::StoreDatabase;
use coven_foundation::id_provider::IdProvider;
use coven_foundation::store_dir::StoreDir;
#[cfg(test)]
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
use coven_protocol::store_commit::{
    membership_rollup_semantic_prefix, snapshot_candidate_semantic_prefix,
    snapshot_image_semantic_prefix, CommitFrontier, MembershipRollupRef, ObjectHash,
    SnapshotImageRef, SnapshotMeta, StoreHistoryCut, StoreSnapshotRef, StoreSnapshotState,
};
use coven_storage::CloudSyncObjectStorage;
use std::sync::Arc;

pub(crate) struct SnapshotCut {
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
}

impl SnapshotCut {
    pub(crate) fn new(snapshot: CreatedSnapshot, coverage: CommitFrontier) -> Self {
        Self { snapshot, coverage }
    }

    pub(crate) fn blobs(&self) -> &[coven_protocol::blob::RowBlobRef] {
        self.snapshot.blobs()
    }

    pub(crate) async fn read_image(&self) -> Result<Vec<u8>, coven_database::SnapshotImageError> {
        self.snapshot.read_image().await
    }

    pub(crate) fn coverage(&self) -> &CommitFrontier {
        &self.coverage
    }

    pub(crate) fn into_parts(self) -> (CreatedSnapshot, CommitFrontier) {
        (self.snapshot, self.coverage)
    }

    #[cfg(test)]
    pub(crate) fn image_path_for_test(&self) -> &std::path::Path {
        self.snapshot.image_path_for_test()
    }
}

pub(crate) struct StoreSnapshotCut {
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
    authorship: coven_database::OwnStreamAuthorship,
}

impl StoreSnapshotCut {
    #[cfg(test)]
    pub(crate) fn coverage(&self) -> &CommitFrontier {
        &self.coverage
    }
}

pub(crate) struct AuthorizedSnapshots<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: Arc<dyn CloudSyncObjectStorage>,
    store_dir: &'storage StoreDir,
    local_writer: Arc<LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedSnapshots<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'storage StoreDir,
        local_writer: Arc<LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            store_dir,
            local_writer,
        }
    }

    pub(crate) async fn publish_due_snapshots(
        &mut self,
        created_at: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        rotation_pending: bool,
        commit_threshold: std::num::NonZeroU64,
    ) -> Result<(), crate::sync::cycle::SyncCycleFailure> {
        // Durable Circle work already owns its exact ciphertext. Finishing it
        // neither captures new state nor depends on new snapshot eligibility.
        self.writer
            .snapshot_publication()
            .await
            .resume_pending_circles()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "publish pending Circle snapshots",
                    error,
                )
            })?;
        let resumed = self
            .writer
            .resume_snapshot_publication()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "publish pending Store snapshot",
                    error,
                )
            })?
            .is_some();
        if resumed || rotation_pending {
            return Ok(());
        }

        let publication = self
            .database
            .store_current_publication()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "read accepted Store snapshot cadence",
                    error,
                )
            })?;
        let record = publication.record();
        let current_position = record.accepted().map_or(0, |entry| entry.position.get());
        let snapshot_position = record
            .latest_snapshot()
            .map_or(0, |snapshot| snapshot.publication.position.get());
        // Every accepted entry after the latest snapshot is a Store commit.
        // Derive the interval from that shared record instead of maintaining
        // another counter or inspecting one author's sequence.
        let accepted_commits =
            current_position
                .checked_sub(snapshot_position)
                .ok_or_else(|| {
                    crate::sync::cycle::SyncCycleFailure::operation(
                        "read accepted Store snapshot cadence",
                        SnapshotError::PublicationState(
                            "latest snapshot is ahead of the accepted publication".into(),
                        ),
                    )
                })?;
        let initial_snapshot = record.latest_snapshot().is_none();
        if !initial_snapshot && accepted_commits < commit_threshold.get() {
            return Ok(());
        }

        let author_pubkey = self.local_writer.author_pubkey();
        if let Err(reason) = self.writer.require_current_owner(&author_pubkey) {
            info!(
                device = %author_pubkey,
                %reason,
                "Snapshot skipped: this device may not author a snapshot"
            );
            return Ok(());
        }

        if initial_snapshot {
            info!("Initial sync: pushing snapshot of existing store data");
        } else {
            info!("Snapshot policy triggered, creating snapshot");
        }

        // Circle failures leave this Store snapshot due. Accepting the Store
        // checkpoint first would reset its cadence before dependent work finishes.
        let schema_version = self.database.schema_version();
        self.writer
            .circles()
            .snapshots()
            .push_circle_snapshots(schema_version, created_at, routing_encryption)
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("publish Circle snapshots", error)
            })?;

        let cut = self
            .capture_snapshot_cut(routing_encryption)
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("capture Store snapshot", error)
            })?;
        let meta = self
            .push_snapshot_cut(cut, created_at.to_string())
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation("publish Store snapshot", error)
            })?;
        info!(
            accepted_commits,
            threshold = commit_threshold.get(),
            snapshot = %meta.snapshot_hash(),
            "Snapshot created and pushed"
        );

        Ok(())
    }

    pub(crate) async fn capture_snapshot_cut(
        &mut self,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<StoreSnapshotCut, SnapshotError> {
        let authorship = self.database.author_own_stream().await;
        self.writer.prepare_publication_boundary().await?;
        let (snapshot, coverage) = self
            .database
            .capture_store_snapshot_cut(
                self.writer.store_root().clone(),
                self.store_dir.as_ref().to_path_buf(),
                routing_encryption.cloned(),
            )
            .await?;
        Ok(StoreSnapshotCut {
            snapshot,
            coverage,
            authorship,
        })
    }

    pub(crate) async fn push_snapshot_cut(
        &mut self,
        cut: StoreSnapshotCut,
        created_at: String,
    ) -> Result<SnapshotMeta, SnapshotError> {
        self.push_store_snapshot_with_authorship(
            cut.snapshot,
            cut.coverage,
            self.database.schema_version(),
            created_at,
            cut.authorship,
        )
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn push_store_snapshot(
        &mut self,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let authorship = self.database.author_own_stream().await;
        self.push_store_snapshot_with_authorship(
            snapshot,
            coverage,
            schema_version,
            created_at,
            authorship,
        )
        .await
    }

    async fn push_store_snapshot_with_authorship(
        &mut self,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
        _authorship: coven_database::OwnStreamAuthorship,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let database = self.database.clone();
        let storage = Arc::clone(&self.storage);
        let publication = AuthorizedSnapshotPublication::begin(&database, storage.as_ref()).await;
        let pending = match database.outbound_snapshot_publication().await? {
            Some(pending) => pending,
            None => {
                self.prepare_store_snapshot(
                    coven_database::StoreSnapshotPublicationStage::Initial,
                    snapshot,
                    coverage,
                    schema_version,
                    created_at,
                )
                .await?
            }
        };
        self.publish_pending(&publication, pending).await
    }

    pub(crate) async fn resume_pending_publication(
        &mut self,
    ) -> Result<Option<SnapshotMeta>, SnapshotError> {
        let _authorship = self.database.author_own_stream().await;
        let database = self.database.clone();
        let storage = Arc::clone(&self.storage);
        let publication = AuthorizedSnapshotPublication::begin(&database, storage.as_ref()).await;
        let Some(pending) = database.outbound_snapshot_publication().await? else {
            return Ok(None);
        };
        self.publish_pending(&publication, pending).await.map(Some)
    }

    async fn publish_pending(
        &mut self,
        publication: &AuthorizedSnapshotPublication<'_>,
        mut pending: coven_database::DurableSnapshotPublication,
    ) -> Result<SnapshotMeta, SnapshotError> {
        use crate::sync::store::authorization::history::publication::StoreSnapshotPublicationAttemptOutcome;
        loop {
            publication.complete_candidate_cleanup().await?;
            if let Some(active) = self.database.active_store_publication().await? {
                if active.superseding_snapshot().is_some() {
                    return Ok(self
                        .database
                        .complete_superseded_snapshot_publication(active)
                        .await?);
                }
            }
            match self
                .writer
                .publish_store_snapshot(&pending, publication)
                .await?
            {
                StoreSnapshotPublicationAttemptOutcome::Accepted(accepted) => {
                    return Ok(self
                        .database
                        .complete_snapshot_publication(accepted)
                        .await?);
                }
                StoreSnapshotPublicationAttemptOutcome::Superseded { snapshot, accepted } => {
                    self.database
                        .supersede_snapshot_publication(
                            pending.reference.clone(),
                            snapshot,
                            accepted,
                        )
                        .await?;
                }
                StoreSnapshotPublicationAttemptOutcome::Competing(accepted) => {
                    let (snapshot, coverage) =
                        self.writer.capture_current_store_snapshot_cut().await?;
                    let created_at = pending.meta.value.created_at.clone();
                    pending = self
                        .prepare_store_snapshot(
                            coven_database::StoreSnapshotPublicationStage::Replacing {
                                previous: pending.reference,
                                accepted,
                            },
                            snapshot,
                            coverage,
                            self.database.schema_version(),
                            created_at,
                        )
                        .await?;
                }
            }
        }
    }

    async fn prepare_store_snapshot(
        &mut self,
        stage: coven_database::StoreSnapshotPublicationStage,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
    ) -> Result<coven_database::DurableSnapshotPublication, SnapshotError> {
        let store_root_hash = self.writer.store_root().store_root_hash;
        let membership = self.writer.membership().clone();
        let membership = &membership;
        let database = self.database.clone();
        let storage = Arc::clone(&self.storage);
        let device_id = self.writer.local_device_id().to_string();
        let author = self.local_writer.author_pubkey();
        if !membership.is_owner_now(&author) {
            return Err(SnapshotError::UnauthorizedAuthor(author));
        }
        let publication_previous = database
            .store_current_publication()
            .await
            .map_err(SnapshotError::from)?
            .require_observed()
            .map_err(SnapshotError::from)?
            .clone();
        let history_cut = StoreHistoryCut(coverage.0.clone());
        let (_, resolved_devices) = database
            .store_device_state_for_history_cut(&history_cut)
            .await
            .map_err(SnapshotError::from)?;
        let resolved = membership.resolved();
        let membership_state = coven_protocol::circle_control::StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            resolved_devices.recovery.clone(),
            resolved.state_hash,
        )
        .map_err(SnapshotError::from)?;
        let state = StoreSnapshotState {
            membership: membership_state,
            devices: resolved_devices.clone(),
        };
        let history_summary = self
            .writer
            .prepare_merge_snapshot_history_summary(
                &coverage,
                membership,
                &resolved_devices,
                publication_previous.record(),
            )
            .await
            .map_err(SnapshotError::from)?;
        let rollup_streams = self
            .writer
            .membership_rollup_parts(membership)
            .await
            .map_err(SnapshotError::from)?;
        let rollup = self
            .local_writer
            .sign_membership_rollup(store_root_hash, rollup_streams)
            .map_err(SnapshotError::from)?;
        let storage = storage.as_ref();
        let meta_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let semantic_prefix = snapshot_candidate_semantic_prefix(&device_id, &database.new_id());
        let current_slot = storage
            .allocate_protocol_slot(&meta_context, &semantic_prefix, ".json")
            .await
            .map_err(SnapshotError::Bucket)?;

        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner::Store {
            metadata_slot: current_slot.clone(),
        };
        let (db_image, snapshot_blobs) = Self::prepare_snapshot_blobs(
            snapshot,
            snapshot_owner,
            &history_summary.pending_device_join_snapshot_slots(),
        )?;
        let image_bytes = db_image.read().await.map_err(SnapshotError::from)?;
        let image_hash = ObjectHash::digest(&image_bytes);
        let image_context = ProtocolObjectContext::store_encrypted(
            store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let image_prefix = snapshot_image_semantic_prefix(&current_slot, image_hash);
        let image_slot = storage
            .allocate_protocol_slot(&image_context, &image_prefix, ".db")
            .await
            .map_err(SnapshotError::Bucket)?;
        let image_prepared = storage
            .prepare_protocol_object(&image_context, image_slot, &image_prefix, image_bytes)
            .map_err(SnapshotError::Bucket)?;
        let image = SnapshotImageRef {
            image_hash,
            object: image_prepared.reference().clone(),
        };

        let rollup_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreMembershipRollup,
        );
        let rollup_bytes = rollup.to_bytes();
        let rollup_hash = ObjectHash::digest(&rollup_bytes);
        let rollup_prefix = membership_rollup_semantic_prefix(&current_slot, rollup_hash);
        let rollup_slot = storage
            .allocate_protocol_slot(&rollup_context, &rollup_prefix, ".json")
            .await
            .map_err(SnapshotError::Bucket)?;
        let rollup_prepared = storage
            .prepare_protocol_object(
                &rollup_context,
                rollup_slot,
                &rollup_prefix,
                rollup_bytes.clone(),
            )
            .map_err(SnapshotError::Bucket)?;
        let membership_rollup = MembershipRollupRef {
            rollup_hash,
            object: rollup_prepared.reference().clone(),
        };

        let meta = self
            .local_writer
            .sign_snapshot(
                store_root_hash,
                publication_previous.record().clone(),
                image,
                membership_rollup,
                coverage,
                state,
                history_summary,
                schema_version,
                created_at,
            )
            .map_err(SnapshotError::from)?;
        let meta_prepared = storage
            .prepare_protocol_object(
                &meta_context,
                current_slot,
                &semantic_prefix,
                meta.to_bytes(),
            )
            .map_err(SnapshotError::Bucket)?;
        let snapshot_reference = StoreSnapshotRef {
            snapshot_hash: meta.snapshot_hash(),
            object: meta_prepared.reference().clone(),
        };
        let publication_entry = self
            .local_writer
            .sign_store_snapshot_publication_entry(
                &publication_previous,
                snapshot_reference.clone(),
            )
            .map_err(SnapshotError::from)?;
        let publication_prefix =
            coven_protocol::store_commit::store_publication_entry_semantic_prefix(
                &publication_entry,
            );
        let publication_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StorePublicationEntry,
        );
        let publication_slot = storage
            .allocate_protocol_slot(&publication_context, &publication_prefix, ".json")
            .await
            .map_err(SnapshotError::Bucket)?;
        let prepared_publication = storage
            .prepare_protocol_object(
                &publication_context,
                publication_slot,
                &publication_prefix,
                publication_entry.to_bytes(),
            )
            .map_err(SnapshotError::Bucket)?;
        let replacement = self
            .local_writer
            .advance_store_snapshot_publication(
                &publication_previous,
                &publication_entry,
                &prepared_publication,
            )
            .map_err(SnapshotError::from)?;
        database
            .stage_snapshot_publication(
                stage,
                meta.clone(),
                meta_prepared,
                coven_protocol::prepared_commit::PreparedStorePublication {
                    previous: publication_previous.record().clone(),
                    previous_version: publication_previous.version().clone(),
                    entry: publication_entry,
                    entry_object: prepared_publication.reference().clone(),
                    replacement,
                },
                rollup_bytes,
                rollup_prepared,
                db_image,
                image_prepared,
                snapshot_blobs,
            )
            .await
            .map_err(SnapshotError::from)?;
        let pending = database
            .outbound_snapshot_publication()
            .await
            .map_err(SnapshotError::from)?
            .ok_or_else(|| {
                SnapshotError::PublicationState(
                    "staged snapshot publication row is absent".to_string(),
                )
            })?;
        Ok(pending)
    }

    fn prepare_snapshot_blobs(
        snapshot: CreatedSnapshot,
        owner: coven_protocol::remote_object::SnapshotObjectOwner,
        pending_store_snapshots: &std::collections::BTreeSet<coven_protocol::objects::ObjectSlot>,
    ) -> Result<
        (
            coven_database::SnapshotDatabaseImage,
            Vec<coven_database::PreparedSnapshotBlob>,
        ),
        SnapshotError,
    > {
        let (db_image, blobs) = snapshot.into_parts();
        let preparation = (|| {
            let mut prepared: Vec<coven_database::PreparedSnapshotBlob> = Vec::new();
            let mut exact_bindings = std::collections::BTreeMap::<String, usize>::new();
            for captured in blobs {
                let (coven_protocol::blob::RowBlobAuthority::Remote(authority), Some(stored)) =
                    (captured.authority(), captured.stored())
                else {
                    return Err(SnapshotError::PublishBlobs(format!(
                        "snapshot blob {}/{} has no accepted exact remote binding",
                        captured.blob().namespace,
                        captured.blob().id
                    )));
                };
                // RowBlobRef already binds content and audience to this exact object.
                // Equal plaintext does not identify an uploaded object.
                let key = serde_json::to_string(&(authority, stored))?;
                let binding = coven_protocol::audience_package::RowBlobLocatorBinding::new(
                    captured.table(),
                    captured.row_id(),
                    captured.row_stamp(),
                    captured.column(),
                    stored.clone(),
                )?;
                if let Some(index) = exact_bindings.get(&key).copied() {
                    prepared[index].bindings.push(binding);
                    continue;
                }
                let remote =
                    coven_protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
                        stored,
                        owner.clone(),
                    )?
                    .into_record();
                prepared.push(coven_database::PreparedSnapshotBlob {
                    bindings: vec![binding],
                    authority: authority.clone(),
                    remote,
                });
                exact_bindings.insert(key, prepared.len() - 1);
            }
            Ok::<_, SnapshotError>(prepared)
        })();
        let prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => {
                return db_image
                    .finish_operation(Err(error))
                    .map_err(SnapshotError::from);
            }
        };
        let image = db_image.install_blob_graph(&owner, &prepared, pending_store_snapshots)?;
        Ok((image, prepared))
    }

    #[cfg(test)]
    fn verify_own_snapshot_bytes_for_test(
        &self,
        reference: &StoreSnapshotRef,
        bytes: &[u8],
    ) -> Result<SnapshotMeta, SnapshotError> {
        self.local_writer
            .parse_snapshot(bytes, self.writer.store_root().store_root_hash, reference)
            .map_err(SnapshotError::from)
    }
}

pub(crate) fn coverage_dominates(left: &CommitFrontier, right: &CommitFrontier) -> bool {
    let left = left.clone().into_refs();
    let right = right.clone().into_refs();
    let mut strictly_ahead = left.len() > right.len();
    for (stream, right_ref) in right {
        let Some(left_ref) = left.get(&stream) else {
            return false;
        };
        if left_ref.coord.sequence() < right_ref.coord.sequence()
            || (left_ref.coord.sequence() == right_ref.coord.sequence() && left_ref != &right_ref)
        {
            return false;
        }
        strictly_ahead |= left_ref.coord.sequence() > right_ref.coord.sequence();
    }
    strictly_ahead
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod publication_race_tests;

#[cfg(test)]
mod blob_capture_tests;

#[cfg(test)]
mod cadence_tests;
