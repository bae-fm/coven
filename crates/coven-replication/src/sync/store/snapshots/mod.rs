//! Durable exact Store snapshot publication.

mod circle;
mod image;
mod publication;

pub(crate) use circle::{CircleSnapshotReader, CircleSnapshotWriter};
pub(crate) use publication::AuthorizedSnapshotPublication;

pub(crate) use image::should_create_snapshot;
pub use image::{PreparedSnapshotBootstrap, SnapshotBlobReconcile, SnapshotError};

use coven_database::{CreatedSnapshot, SnapshotBlobAudience};

use tracing::{info, warn};

use super::AuthorizedWriterOperation;
use crate::sync::store::commit_publication::{LocalStoreWriter, SnapshotHistoryConstruction};
use coven_database::StoreDatabase;
use coven_foundation::store_dir::StoreDir;
#[cfg(test)]
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain};
#[cfg(test)]
use coven_protocol::store_commit::StoreSnapshotRef;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, snapshot_slot_prefix, CommitFrontier, ObjectHash,
    SnapshotImageRef, SnapshotMeta, SnapshotSuccessorLink, StoreHistoryCut, StoreSnapshotState,
};
use coven_storage::SyncStorage;
use std::sync::Arc;

pub(crate) struct SnapshotCut {
    pub(crate) snapshot: CreatedSnapshot,
    pub(crate) coverage: CommitFrontier,
}

pub(crate) struct StoreSnapshotCut {
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
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
    storage: Arc<dyn SyncStorage>,
    store_dir: &'storage StoreDir,
    membership: coven_protocol::membership::MembershipChain,
    local_writer: Arc<LocalStoreWriter>,
}

impl<'operation, 'storage> AuthorizedSnapshots<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        store_dir: &'storage StoreDir,
        membership: coven_protocol::membership::MembershipChain,
        local_writer: Arc<LocalStoreWriter>,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            store_dir,
            membership,
            local_writer,
        }
    }

    pub(crate) async fn publish_due_snapshots(
        &mut self,
        created_at: &str,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        rotation_pending: bool,
    ) -> Result<(), crate::sync::cycle::SyncCycleFailure> {
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

        let local_position = self
            .writer
            .latest_local_store_position()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "read local Store snapshot cadence position",
                    error,
                )
            })?;
        let local_seq = local_position
            .as_ref()
            .map_or(0, |reference| reference.coord.sequence());
        let last_snapshot = self
            .database
            .latest_local_store_snapshot()
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "read latest local Store snapshot",
                    error,
                )
            })?;
        let last_snapshot_position = last_snapshot
            .as_ref()
            .map(|snapshot| self.snapshot_position(snapshot));
        let hours_since = match last_snapshot.as_ref() {
            None => None,
            Some(snapshot) => {
                let current =
                    chrono::DateTime::parse_from_rfc3339(created_at).map_err(|error| {
                        crate::sync::cycle::SyncCycleFailure::operation(
                            "read Store snapshot cadence",
                            SnapshotError::PublicationState(format!(
                                "cycle timestamp is invalid: {error}"
                            )),
                        )
                    })?;
                let previous = chrono::DateTime::parse_from_rfc3339(&snapshot.meta.created_at)
                    .map_err(|error| {
                        crate::sync::cycle::SyncCycleFailure::operation(
                            "read Store snapshot cadence",
                            SnapshotError::PublicationState(format!(
                                "published Store snapshot has an invalid creation time: {error}"
                            )),
                        )
                    })?;
                Some(current.signed_duration_since(previous).num_hours().max(0) as u64)
            }
        };
        let initial_snapshot = local_seq == 0 && last_snapshot.is_none();
        if !initial_snapshot
            && !should_create_snapshot(local_seq, last_snapshot_position, hours_since)
        {
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

        let snapshot = self
            .capture_snapshot_cut(self.database.synced_tables().to_vec(), routing_encryption)
            .await;
        match snapshot {
            Ok(cut) => {
                let meta = self
                    .push_snapshot_cut(cut, created_at.to_string())
                    .await
                    .map_err(|error| {
                        crate::sync::cycle::SyncCycleFailure::operation(
                            "publish Store snapshot",
                            error,
                        )
                    })?;
                info!(
                    local_seq,
                    snapshot = %meta.snapshot_hash(),
                    "Snapshot created and pushed"
                );
            }
            Err(error) => warn!("Failed to create snapshot: {error}"),
        }

        let schema_version = self.database.schema_version();
        if let Err(error) = self
            .writer
            .circles()
            .snapshots()
            .push_circle_snapshots(schema_version, created_at, routing_encryption)
            .await
        {
            warn!("Failed to author Circle snapshots: {error}");
        }
        Ok(())
    }

    fn snapshot_position(&self, snapshot: &coven_database::PublishedStoreSnapshot) -> u64 {
        snapshot
            .meta
            .coverage
            .clone()
            .into_refs()
            .remove(&self.writer.announcement_stream_id().to_string())
            .map(|reference| reference.coord.sequence())
            .unwrap_or(0)
    }

    pub(crate) async fn capture_snapshot_cut(
        &self,
        tables: Vec<coven_protocol::synced_schema::SyncedTable>,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<StoreSnapshotCut, coven_database::DbError> {
        let (snapshot, coverage) = self
            .database
            .capture_store_snapshot_cut(
                self.writer.store_root().clone(),
                self.store_dir.as_ref().to_path_buf(),
                tables,
                routing_encryption.cloned(),
            )
            .await?;
        Ok(StoreSnapshotCut { snapshot, coverage })
    }

    pub(crate) async fn push_snapshot_cut(
        &mut self,
        cut: StoreSnapshotCut,
        created_at: String,
    ) -> Result<SnapshotMeta, SnapshotError> {
        self.push_store_snapshot(
            cut.snapshot,
            cut.coverage,
            self.database.schema_version(),
            created_at,
        )
        .await
    }

    pub(crate) async fn push_store_snapshot(
        &mut self,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let store_root_hash = self.writer.store_root().store_root_hash;
        let membership = self.membership.clone();
        let database = self.database.clone();
        let membership = &membership;
        let database = &database;
        let publication = self.writer.snapshot_publication().await;
        publication.drain_spool_cleanup().await?;
        if let Some(pending) = database
            .outbound_snapshot_publication()
            .await
            .map_err(SnapshotError::from)?
        {
            return publication.publish_store(pending).await;
        }
        let device_id = self.writer.local_device_id().to_string();
        let author = self.local_writer.author_pubkey();
        if !membership.is_owner_now(&author) {
            return Err(SnapshotError::UnauthorizedAuthor(author));
        }
        let history_cut = StoreHistoryCut(coverage.0.clone());
        let (devices, resolved_devices) = database
            .store_device_state_for_history_cut(&history_cut)
            .await
            .map_err(SnapshotError::from)?;
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(SnapshotError::PublicationState(
                "snapshot publication requires resolved membership".to_string(),
            ));
        };
        let membership_state = coven_protocol::circle_control::StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            resolved_devices.recovery.clone(),
            resolved.state_hash,
        )
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let state = StoreSnapshotState {
            membership: membership_state,
            devices,
        };
        let history_summary = self
            .writer
            .prepare_merge_snapshot_history_summary(&coverage, membership, &resolved_devices)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let storage = self.storage.as_ref();
        let previous = database
            .latest_local_store_snapshot()
            .await
            .map_err(SnapshotError::from)?;
        let (generation, predecessor, current_slot) = match previous {
            Some(previous) => (
                previous
                    .reference
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| {
                        SnapshotError::PublicationState(
                            "Store snapshot generation overflow".to_string(),
                        )
                    })?,
                Some(previous.reference),
                previous.successor_slot,
            ),
            None => (0, None, self.local_writer.first_snapshot_slot()),
        };

        let snapshot_owner = coven_protocol::remote_object::SnapshotObjectOwner {
            activation: self
                .local_writer
                .snapshot_activation_id()
                .map_err(|error| SnapshotError::Parse(error.to_string()))?,
            generation,
        };
        let (db_image, snapshot_blobs) = self
            .prepare_snapshot_blobs(snapshot, snapshot_owner)
            .await?;
        let image_bytes = db_image
            .read()
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let image_hash = ObjectHash::digest(&image_bytes);
        let image_context = ProtocolObjectContext::store_encrypted(
            store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let image_prefix = snapshot_image_semantic_prefix(&device_id, image_hash);
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

        let meta_context = ProtocolObjectContext::signed_plaintext(
            store_root_hash,
            ProtocolObjectDomain::StoreSnapshotMeta,
        );
        let semantic_prefix = snapshot_slot_prefix(&device_id, generation);
        let next_slot = storage
            .allocate_protocol_slot(
                &meta_context,
                &snapshot_slot_prefix(
                    &device_id,
                    generation.checked_add(1).ok_or_else(|| {
                        SnapshotError::PublicationState(
                            "Store snapshot generation overflow".to_string(),
                        )
                    })?,
                ),
                ".json",
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        let activation = self
            .local_writer
            .snapshot_activation_id()
            .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        let meta = self
            .local_writer
            .sign_snapshot(
                store_root_hash,
                generation,
                predecessor.clone(),
                image,
                coverage,
                state,
                history_summary,
                schema_version,
                created_at,
                SnapshotSuccessorLink {
                    activation,
                    predecessor,
                    next_slot,
                },
            )
            .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        let meta_prepared = storage
            .prepare_protocol_object(
                &meta_context,
                current_slot,
                &semantic_prefix,
                meta.to_bytes(),
            )
            .map_err(SnapshotError::Bucket)?;
        database
            .stage_snapshot_publication(
                meta.clone(),
                meta_prepared,
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
        publication.publish_store(pending).await
    }

    async fn prepare_snapshot_blobs(
        &self,
        snapshot: CreatedSnapshot,
        owner: coven_protocol::remote_object::SnapshotObjectOwner,
    ) -> Result<
        (
            coven_database::SnapshotDatabaseImage,
            Vec<coven_database::PreparedSnapshotBlob>,
        ),
        SnapshotError,
    > {
        let database = &self.database;
        let storage = self.storage.as_ref();
        let authority = self.local_writer.blob_write_authority();
        let CreatedSnapshot {
            db_image,
            mut blobs,
        } = snapshot;
        blobs.sort_by_key(|captured| captured.fact.previous.is_none());
        let mut prepared: Vec<coven_database::PreparedSnapshotBlob> = Vec::new();
        let mut coalesced = std::collections::BTreeMap::<String, usize>::new();
        let preparation = async {
    for captured in blobs {
        let (audience, protection, package_authority) = match captured.audience {
            SnapshotBlobAudience::Store => (
                coven_protocol::blob::locator::RemoteAudience::Store,
                storage.store_blob_protection().map_err(SnapshotError::Bucket)?,
                coven_protocol::audience_package::PackageAudience::Store,
            ),
            SnapshotBlobAudience::Circle { circle_id, control } => {
                let access = database
                    .circle_publication_context(circle_id, control.coordinate().clone())
                    .await
                    .map_err(SnapshotError::from)?;
                let key_fingerprint = access.key_fingerprint();
                (
                    coven_protocol::blob::locator::RemoteAudience::Circle(circle_id),
                    access.blob_protection(),
                    coven_protocol::audience_package::PackageAudience::Circle {
                        circle_id,
                        control: control.coordinate().clone(),
                        key_fingerprint,
                    },
                )
            }
        };
        if captured.fact.blob.provenance == coven_protocol::blob::Provenance::UserProvided
            && captured.fact.previous.is_none()
        {
            return Err(SnapshotError::PublishBlobs(format!(
                "snapshot UserProvided blob {}/{} has no existing exact remote binding",
                captured.fact.blob.namespace, captured.fact.blob.id
            )));
        }
        if captured.fact.blob.provenance == coven_protocol::blob::Provenance::UserProvided {
            let locator = crate::sync::store::commit_publication::prepare_partition_blob_locator(
                &captured.fact,
                audience.clone(),
                &protection,
                &authority,
            )
            .map_err(|error| SnapshotError::PublishBlobs(error.to_string()))?;
            if captured
                .fact
                .previous
                .as_ref()
                .is_none_or(|previous| previous.stored.locator() != &locator)
            {
                return Err(SnapshotError::PublishBlobs(format!(
                    "snapshot UserProvided blob {}/{} does not match its existing exact remote binding",
                    captured.fact.blob.namespace, captured.fact.blob.id
                )));
            }
        }
        let coalesce_key = serde_json::to_string(&(
            &package_authority,
            &captured.fact.blob.namespace,
            &captured.fact.blob.id,
            &captured.fact.blob.scope,
            &captured.fact.blob.cloud_path,
            captured.fact.plaintext_size,
            captured.fact.plaintext_hash,
        ))
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        if let Some(index) = coalesced.get(&coalesce_key).copied() {
            let stored = prepared[index].bindings[0].blob().clone();
            let binding = coven_protocol::audience_package::RowBlobLocatorBinding::new(
                captured.fact.table,
                captured.fact.row_id,
                captured.fact.row_stamp,
                captured.fact.column,
                stored,
            )
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
            prepared[index].bindings.push(binding);
            continue;
        }
        let (binding, blob) = self
            .writer
            .prepare_partition_blob(
                &captured.fact,
                audience,
                protection,
                &authority,
            )
            .await
            .map_err(|error| SnapshotError::PublishBlobs(error.to_string()))?;
        if captured.fact.blob.provenance == coven_protocol::blob::Provenance::UserProvided
            && !blob.uploaded_verified
        {
            return Err(SnapshotError::PublishBlobs(format!(
                "snapshot UserProvided blob {}/{} does not match its existing exact remote binding",
                captured.fact.blob.namespace, captured.fact.blob.id
            )));
        }
        if blob.uploaded_verified {
            storage
                .verify_blob_object(&blob.stored)
                .await
                .map_err(SnapshotError::Bucket)?;
        }
        let spool_path = blob.spool_path;
        if !blob.uploaded_verified && spool_path.is_none() {
            return Err(SnapshotError::PublicationState(
                "prepared snapshot blob awaiting upload has no exact spool".to_string(),
            ));
        }
        let remote = coven_protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
            &blob.stored,
            owner.clone(),
        )
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
        .into_record();
        prepared.push(coven_database::PreparedSnapshotBlob {
            bindings: vec![binding],
            authority: package_authority,
            remote,
            spool_path,
        });
        coalesced.insert(coalesce_key, prepared.len() - 1);
    }
    Ok::<(), SnapshotError>(())
    }.await;
        if let Err(error) = preparation {
            cleanup_snapshot_spools(&prepared)
                .await
                .map_err(|cleanup| {
                    SnapshotError::PublicationState(format!(
                    "snapshot blob preparation failed: {error}; spool cleanup failed: {cleanup}"
                ))
                })?;
            return Err(error);
        }
        if prepared.is_empty() {
            return Ok((db_image, prepared));
        }
        let image = match db_image.install_blob_graph(&prepared) {
            Ok(image) => image,
            Err(error) => {
                cleanup_snapshot_spools(&prepared)
                    .await
                    .map_err(|cleanup| {
                        SnapshotError::PublicationState(format!(
                        "snapshot image closure failed: {error}; spool cleanup failed: {cleanup}"
                    ))
                    })?;
                return Err(error.into());
            }
        };
        Ok((image, prepared))
    }

    #[cfg(test)]
    fn verify_own_snapshot_bytes_for_test(
        &self,
        reference: &StoreSnapshotRef,
        bytes: &[u8],
    ) -> Result<SnapshotMeta, SnapshotError> {
        self.local_writer
            .parse_snapshot_stream_entry(bytes, self.writer.store_root(), reference)
            .map_err(|error| SnapshotError::Parse(error.to_string()))
    }
}

async fn cleanup_snapshot_spools(
    prepared: &[coven_database::PreparedSnapshotBlob],
) -> Result<(), String> {
    let mut paths = std::collections::BTreeSet::new();
    for path in prepared.iter().filter_map(|blob| blob.spool_path.as_ref()) {
        if paths.insert(path.clone()) {
            remove_snapshot_spool(path, true).await?;
        }
    }
    Ok(())
}

async fn remove_snapshot_spool(
    path: &std::path::Path,
    require_present: bool,
) -> Result<(), String> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && !require_present => {
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!("snapshot spool {} is absent", path.display()));
        }
        Err(error) => {
            return Err(format!("remove snapshot spool {}: {error}", path.display()));
        }
    }
    coven_foundation::atomic_file::sync_parent_dir(path).await
}

pub(crate) fn select_maximal_store_snapshot(
    mut candidates: Vec<coven_database::PublishedStoreSnapshot>,
) -> Option<coven_database::PublishedStoreSnapshot> {
    let all = candidates.clone();
    candidates.retain(|snapshot| {
        !all.iter().any(|other| {
            other.reference != snapshot.reference
                && coverage_dominates(&other.meta.coverage, &snapshot.meta.coverage)
        })
    });
    candidates.sort_by_key(|snapshot| snapshot.reference.snapshot_hash);
    candidates.pop()
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
