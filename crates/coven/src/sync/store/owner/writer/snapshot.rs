//! Durable exact Store snapshot publication.

mod image;
mod publication;

pub(super) use publication::AuthorizedSnapshotPublication;

pub(crate) use image::should_create_snapshot;
pub(crate) use image::{bootstrap_from_snapshot, SnapshotBlobReconcile, SnapshotError};

use crate::database::{CreatedSnapshot, SnapshotBlobAudience};

use tracing::{info, warn};

use super::SnapshotHistoryConstruction;
#[cfg(test)]
use crate::keys::UserKeypair;
use crate::protocol::store_commit::{
    snapshot_image_semantic_prefix, snapshot_slot_prefix, CommitFrontier, DeviceStreamAnchor,
    ObjectHash, SnapshotImageRef, SnapshotMeta, SnapshotSuccessorLink, StoreHistoryCut,
    StoreRootRef, StoreSnapshotRef, StoreSnapshotState,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};

pub(crate) struct SnapshotCut {
    pub(crate) snapshot: CreatedSnapshot,
    pub(crate) coverage: CommitFrontier,
}

impl super::AuthorizedWriterOperation<'_> {
    pub(crate) async fn publish_due_snapshots(
        &mut self,
        store_dir: &crate::store_dir::StoreDir,
        created_at: &str,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        rotation_pending: bool,
    ) -> Result<(), crate::sync::cycle::SyncCycleFailure> {
        let keypair = self.writer.identity;
        let resumed = self
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

        let local_position = self.latest_local_store_position().await.map_err(|error| {
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

        let author_pubkey = hex::encode(keypair.public_key());
        if let Err(reason) = self.require_current_owner(&author_pubkey) {
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
            .capture_snapshot_cut(
                store_dir.as_ref().to_path_buf(),
                self.database.synced_tables().to_vec(),
                routing_encryption,
            )
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
            .circles()
            .snapshots()
            .push_circle_snapshots(
                store_dir.as_ref().to_path_buf(),
                schema_version,
                created_at,
                routing_encryption,
            )
            .await
        {
            warn!("Failed to author Circle snapshots: {error}");
        }
        Ok(())
    }

    fn snapshot_position(&self, snapshot: &crate::database::PublishedStoreSnapshot) -> u64 {
        snapshot
            .meta
            .coverage
            .clone()
            .into_refs()
            .remove(&self.announcement_stream_id().to_string())
            .map(|reference| reference.coord.sequence())
            .unwrap_or(0)
    }

    pub(crate) async fn capture_snapshot_cut(
        &self,
        temp_dir: std::path::PathBuf,
        tables: Vec<crate::sync::session::SyncedTable>,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        let (snapshot, coverage) = self
            .database
            .capture_store_snapshot_cut(
                self.store_root().clone(),
                temp_dir,
                tables,
                routing_encryption.cloned(),
            )
            .await?;
        Ok(SnapshotCut { snapshot, coverage })
    }
}

impl super::AuthorizedWriterOperation<'_> {
    pub(crate) async fn push_snapshot_cut(
        &mut self,
        cut: SnapshotCut,
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
        let store_root_hash = self.store_root().store_root_hash;
        let membership = self.membership.clone();
        let database = self.database.clone();
        let membership = &membership;
        let database = &database;
        let publication = self.snapshot_publication().await;
        publication.drain_spool_cleanup().await?;
        if let Some(pending) = database
            .outbound_snapshot_publication()
            .await
            .map_err(publication_error)?
        {
            return publication.publish_store(pending).await;
        }
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        let device_signer = self.writer.device_signer.clone();
        let device_id = registration_ref.device_id.to_string();
        let author = registration.author_pubkey.clone();
        if !membership.is_owner_now(&author) {
            return Err(SnapshotError::UnauthorizedAuthor(author));
        }
        let history_cut = StoreHistoryCut(coverage.0.clone());
        let (devices, resolved_devices) = database
            .store_device_state_for_history_cut(&history_cut)
            .await
            .map_err(publication_error)?;
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(SnapshotError::PublicationState(
                "snapshot publication requires resolved membership".to_string(),
            ));
        };
        let membership_state =
            crate::protocol::circle_control::StoreMembershipStateRef::from_parts(
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
            .prepare_merge_snapshot_history_summary(
                &coverage,
                membership,
                &resolved_devices,
                &registration_ref,
                &registration,
            )
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let storage = self.storage.as_ref();
        let previous = database
            .latest_local_store_snapshot()
            .await
            .map_err(publication_error)?;
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
            None => (0, None, snapshot_first_slot(&registration)?.clone()),
        };

        let snapshot_owner = crate::protocol::remote_object::SnapshotObjectOwner {
            activation: registration
                .store_snapshot_activation(&registration_ref)
                .map_err(|error| SnapshotError::Parse(error.to_string()))?
                .activation_id(),
            generation,
        };
        let (image_bytes, snapshot_blobs) = self
            .prepare_snapshot_blobs(snapshot, snapshot_owner)
            .await?;
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
            .prepare_protocol_object(
                &image_context,
                image_slot,
                &image_prefix,
                image_bytes.clone(),
            )
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
        let activation = registration
            .store_snapshot_activation(&registration_ref)
            .map_err(|error| SnapshotError::Parse(error.to_string()))?
            .activation_id();
        let meta = SnapshotMeta::signed(
            store_root_hash,
            registration_ref.clone(),
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
            &device_signer,
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
                image_bytes,
                image_prepared,
                snapshot_blobs,
            )
            .await
            .map_err(publication_error)?;
        let pending = database
            .outbound_snapshot_publication()
            .await
            .map_err(publication_error)?
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
        owner: crate::protocol::remote_object::SnapshotObjectOwner,
    ) -> Result<(Vec<u8>, Vec<crate::database::PreparedSnapshotBlob>), SnapshotError> {
        let database = &self.database;
        let storage = self.storage.as_ref();
        let registration_ref = &self.writer.registration_ref;
        let registration = &self.writer.registration;
        let authority = crate::storage::BlobWriteAuthority::new(registration_ref, registration)
            .map_err(SnapshotError::Bucket)?;
        let CreatedSnapshot {
            db_image,
            mut blobs,
        } = snapshot;
        blobs.sort_by_key(|captured| captured.fact.previous.is_none());
        let image_store_dir = blobs.first().map(|blob| blob.store_dir.clone());
        let mut prepared: Vec<crate::database::PreparedSnapshotBlob> = Vec::new();
        let mut coalesced = std::collections::BTreeMap::<String, usize>::new();
        let preparation = async {
    for captured in blobs {
        let (audience, protection, package_authority) = match captured.audience {
            SnapshotBlobAudience::Store => (
                crate::blob::locator::RemoteAudience::Store,
                storage.store_blob_protection().map_err(SnapshotError::Bucket)?,
                crate::protocol::audience_package::PackageAudience::Store,
            ),
            SnapshotBlobAudience::Circle { circle_id, control } => {
                let (encryption, key_fingerprint) =
                    database.circle_publication_context(circle_id, control.coordinate().clone())
                    .await
                    .map_err(publication_error)?;
                (
                    crate::blob::locator::RemoteAudience::Circle(circle_id),
                    crate::storage::BlobSpoolProtection::Opaque(encryption),
                    crate::protocol::audience_package::PackageAudience::Circle {
                        circle_id,
                        control: control.coordinate().clone(),
                        key_fingerprint,
                    },
                )
            }
        };
        if captured.fact.blob.provenance == crate::blob::Provenance::UserProvided
            && captured.fact.previous.is_none()
        {
            return Err(SnapshotError::PublishBlobs(format!(
                "snapshot UserProvided blob {}/{} has no existing exact remote binding",
                captured.fact.blob.namespace, captured.fact.blob.id
            )));
        }
        if captured.fact.blob.provenance == crate::blob::Provenance::UserProvided {
            let locator = super::blob_preparation::prepare_partition_blob_locator(
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
            let binding = crate::protocol::audience_package::RowBlobLocatorBinding::new(
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
            .prepare_partition_blob(
                &captured.fact,
                audience,
                protection,
                &authority,
                &captured.store_dir,
            )
            .await
            .map_err(|error| SnapshotError::PublishBlobs(error.to_string()))?;
        if captured.fact.blob.provenance == crate::blob::Provenance::UserProvided
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
        let remote = crate::protocol::remote_object::RemoteObjectRecord::snapshot_activated_blob(
            &blob.stored,
            owner.clone(),
        )
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        prepared.push(crate::database::PreparedSnapshotBlob {
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
        let image_store_dir = image_store_dir.ok_or_else(|| {
            SnapshotError::PublicationState(
                "prepared snapshot blob graph has no captured Store directory".to_string(),
            )
        })?;
        let image = match crate::database::SnapshotDatabaseImage::replace(
            image_store_dir.as_ref().join("snapshot-closure.db"),
            &db_image,
        )
        .and_then(|image| image.install_blob_graph(&prepared))
        {
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
}

async fn cleanup_snapshot_spools(
    prepared: &[crate::database::PreparedSnapshotBlob],
) -> Result<(), String> {
    let mut paths = std::collections::BTreeSet::new();
    for path in prepared.iter().filter_map(|blob| blob.spool_path.as_ref()) {
        if paths.insert(path.clone()) {
            remove_snapshot_spool(path, true).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
impl super::AuthorizedWriterOperation<'_> {
    fn verify_own_snapshot_bytes_for_test(
        &self,
        reference: &StoreSnapshotRef,
        bytes: &[u8],
    ) -> Result<SnapshotMeta, SnapshotError> {
        verify_store_snapshot_bytes(
            self.store_root(),
            &self.writer.registration_ref,
            &self.writer.registration,
            reference,
            bytes,
        )
    }
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
    crate::atomic_file::sync_parent_dir(path).await
}

fn snapshot_first_slot(
    registration: &crate::protocol::store_commit::StoreDeviceRegistration,
) -> Result<&crate::storage::cloud::ObjectSlot, SnapshotError> {
    match &registration.snapshots {
        DeviceStreamAnchor::StoreSnapshots { first_slot } => Ok(first_slot),
        _ => Err(SnapshotError::PublicationState(
            "local Store registration has no snapshot stream anchor".to_string(),
        )),
    }
}

pub(crate) fn verify_store_snapshot_bytes(
    root: &StoreRootRef,
    registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    reference: &StoreSnapshotRef,
    bytes: &[u8],
) -> Result<SnapshotMeta, SnapshotError> {
    let meta = SnapshotMeta::parse_at(bytes, root.store_root_hash, reference, registration)
        .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    let expected_predecessor = meta.predecessor.clone();
    let next_generation = reference
        .generation
        .checked_add(1)
        .ok_or_else(|| SnapshotError::Parse("Store snapshot generation overflow".to_string()))?;
    let activation = registration
        .store_snapshot_activation(registration_ref)
        .map_err(|error| SnapshotError::Parse(error.to_string()))?
        .activation_id();
    if meta.author_registration != *registration_ref
        || meta.successor.activation != activation
        || meta.successor.predecessor != expected_predecessor
        || meta.successor.next_slot.logical_key()
            != format!(
                "{}.json",
                snapshot_slot_prefix(&registration.device_id.to_string(), next_generation)
            )
    {
        return Err(SnapshotError::Parse(
            "Store snapshot metadata is outside its activated exact stream".to_string(),
        ));
    }
    Ok(meta)
}

pub(crate) fn select_maximal_store_snapshot(
    mut candidates: Vec<crate::database::PublishedStoreSnapshot>,
) -> Option<crate::database::PublishedStoreSnapshot> {
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

pub(crate) fn publication_error(error: crate::database::DbError) -> SnapshotError {
    SnapshotError::PublicationState(error.to_string())
}

struct SnapshotBootstrapAuthority<'storage> {
    storage: &'storage dyn SyncStorage,
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
}

struct VerifiedSnapshotBootstrap<'storage> {
    root: crate::sync::store::protocol_root::VerifiedStoreRoot,
    history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
    snapshot: crate::database::PublishedStoreSnapshot,
    image: Vec<u8>,
    stability: crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
    membership: crate::protocol::membership::MembershipChain,
}

impl<'storage> SnapshotBootstrapAuthority<'storage> {
    fn new(
        storage: &'storage dyn SyncStorage,
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        history_verifier: crate::sync::store::owner::verified_history::MergeHistoryVerifier<
            'storage,
        >,
    ) -> Self {
        Self {
            storage,
            root,
            history_verifier,
        }
    }

    async fn select(
        mut self,
        membership_floor: &crate::joining::MembershipFloor,
        binary_schema_version: u32,
    ) -> Result<VerifiedSnapshotBootstrap<'storage>, SnapshotError> {
        let root = self.root.reference();
        let root_value = self.root.protocol();
        if root_value.descriptor.store_root_id() != root.store_root_id {
            return Err(SnapshotError::UnauthorizedAuthor(
                "Store root differs from bootstrap authority".to_string(),
            ));
        }
        let heads = &membership_floor.0;
        let mut registrations = std::collections::BTreeMap::new();
        let mut resolutions = std::collections::BTreeSet::new();
        for reference in heads {
            let head = self
                .history_verifier
                .load_exact_membership_head(reference)
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
            resolutions.extend(head.body.resolutions.iter().cloned());
            let registration = self
                .history_verifier
                .load_registration(&head.body.author_registration)
                .await
                .map_err(|error| SnapshotError::Parse(error.to_string()))?
                .value;
            registrations.insert(head.body.author_registration.clone(), registration);
        }
        let resolutions = resolutions.into_iter().collect::<Vec<_>>();
        let membership = self
            .history_verifier
            .load_membership_at_exact_heads(heads, &resolutions)
            .await
            .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
        let registrations = registrations
            .into_iter()
            .filter(|(_, registration)| membership.is_owner_now(&registration.author_pubkey))
            .collect::<Vec<_>>();
        let mut authorized = Vec::new();
        for (registration_ref, registration) in registrations {
            authorized.extend(
                self.history_verifier
                    .load_store_snapshot_stream(&registration_ref, &registration)
                    .await?,
            );
        }
        let selected = Box::pin(
            self.history_verifier
                .select_maximal_stable_store_snapshot(authorized),
        )
        .await
        .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?
        .ok_or_else(|| {
            SnapshotError::Bucket(crate::storage::StorageError::NotFound(
                "Store snapshot stream".to_string(),
            ))
        })?;
        let chosen = selected.snapshot;
        if chosen.meta.schema_version > binary_schema_version {
            return Err(SnapshotError::SchemaTooNew {
                snapshot_version: chosen.meta.schema_version,
                supported: binary_schema_version,
            });
        }
        let image_context = ProtocolObjectContext::store_encrypted(
            root.store_root_hash,
            ProtocolObjectDomain::StoreSnapshotImage,
        );
        let image = self
            .storage
            .read_protocol_object(
                &image_context,
                &chosen.meta.image.object,
                &snapshot_image_semantic_prefix(
                    &chosen.meta.author_registration.device_id.to_string(),
                    chosen.meta.image.image_hash,
                ),
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        if ObjectHash::digest(&image) != chosen.meta.image.image_hash {
            return Err(SnapshotError::Parse(
                "Store snapshot image differs from its exact reference".to_string(),
            ));
        }
        Ok(VerifiedSnapshotBootstrap {
            root: self.root,
            history_verifier: self.history_verifier,
            snapshot: chosen,
            image,
            stability: selected.stability,
            membership,
        })
    }
}

async fn open_snapshot_bootstrap_authority<'storage>(
    storage: &'storage dyn SyncStorage,
    root: &StoreRootRef,
) -> Result<SnapshotBootstrapAuthority<'storage>, SnapshotError> {
    let (verified_root, history_verifier) =
        crate::sync::store::owner::verified_history::open_merge_history_verifier_with_root(
            SnapshotHistoryConstruction.authorize_history(),
            storage,
            root,
        )
        .await
        .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    Ok(SnapshotBootstrapAuthority::new(
        storage,
        verified_root,
        history_verifier,
    ))
}

impl<'storage> VerifiedSnapshotBootstrap<'storage> {
    async fn founder_registration(
        &self,
    ) -> Result<
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        SnapshotError,
    > {
        self.history_verifier
            .load_founder_registration()
            .await
            .map_err(|error| SnapshotError::Parse(error.to_string()))
    }

    fn into_parts(
        self,
    ) -> (
        crate::sync::store::protocol_root::VerifiedStoreRoot,
        crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
        crate::database::PublishedStoreSnapshot,
        Vec<u8>,
        crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
        crate::protocol::membership::MembershipChain,
    ) {
        (
            self.root,
            self.history_verifier,
            self.snapshot,
            self.image,
            self.stability,
            self.membership,
        )
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
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use crate::database::Database;
    use crate::database::StoreDatabase;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};

    fn open(path: &Path, device_id: &str) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            device_id.to_string(),
            std::sync::Arc::new(crate::clock::SystemClock),
            &crate::sync::test_helpers::test_migrations(),
        )
        .expect("open snapshot test database")
        .0
    }

    fn store_database(database: &Database) -> StoreDatabase {
        StoreDatabase::new(database)
    }

    fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> Arc<CloudSyncStorage> {
        Arc::new(
            CloudSyncStorage::new(
                Arc::new(home.clone()),
                CloudCipher::Plaintext,
                BlobPathScheme::Plain,
                "snapshot-exact-store",
                signer.clone(),
            )
            .expect("construct snapshot test storage"),
        )
    }

    async fn initialize(
        db: &Database,
        storage: &Arc<CloudSyncStorage>,
        signer: &UserKeypair,
    ) -> (StoreRootRef, String) {
        let initialized = crate::sync::store::Store::create(
            store_database(db),
            storage.clone(),
            "snapshot-exact-store",
            signer,
        )
        .await
        .expect("create snapshot test Store");
        let root_ref = initialized.store.store_root().clone();
        let origin = crate::protocol::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: initialized
                .store
                .protocol_root_for_test()
                .descriptor
                .creation_id,
        };
        let device_id = crate::protocol::store_commit::StoreDeviceId::derive(&root_ref, &origin);
        (root_ref, device_id.to_string())
    }

    fn snapshot(bytes: &[u8]) -> CreatedSnapshot {
        CreatedSnapshot {
            db_image: bytes.to_vec(),
            blobs: Vec::new(),
        }
    }

    async fn publish(
        storage: &Arc<CloudSyncStorage>,
        db: &Database,
        signer: &UserKeypair,
        bytes: &[u8],
        created_at: &str,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let store = crate::sync::store::Store::load(
            StoreDatabase::new(db),
            storage.clone(),
            signer.clone(),
        )
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        store
            .authorize_writer()
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
            .push_store_snapshot(
                snapshot(bytes),
                CommitFrontier(BTreeMap::new()),
                1,
                created_at.to_string(),
            )
            .await
    }

    async fn resume(
        storage: &Arc<CloudSyncStorage>,
        db: &Database,
        signer: &UserKeypair,
    ) -> Result<Option<SnapshotMeta>, SnapshotError> {
        let store = crate::sync::store::Store::load(
            StoreDatabase::new(db),
            storage.clone(),
            signer.clone(),
        )
        .await
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        store
            .authorize_writer()
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
            .resume_snapshot_publication()
            .await
    }

    #[tokio::test]
    async fn selector_keeps_semantic_and_stored_snapshot_hashes_distinct() {
        Box::pin(run_selector_keeps_snapshot_hash_domains_distinct()).await;
    }

    async fn run_selector_keeps_snapshot_hash_domains_distinct() {
        let db = crate::sync::test_helpers::open_test_db();
        let signer = UserKeypair::generate();
        let store = crate::sync::test_helpers::TestStore::create(
            &db,
            "snapshot-selector-hash-domains",
            signer.clone(),
        )
        .await
        .expect("create exact snapshot selector Store");
        let device = store
            .open_into(&db)
            .await
            .expect("open exact snapshot selector Store");
        let membership = device
            .membership_for_test()
            .await
            .expect("load exact snapshot selector membership");
        let published = device
            .authorize_writer()
            .await
            .expect("authorize exact snapshot selector writer")
            .push_store_snapshot(
                snapshot(b"snapshot selector image"),
                CommitFrontier(BTreeMap::new()),
                1,
                "2026-07-16T00:00:00Z".to_string(),
            )
            .await
            .expect("publish exact snapshot selector fixture");
        device
            .stage_acknowledgement(
                CommitFrontier(BTreeMap::new()),
                "2026-07-16T00:00:01Z".to_string(),
            )
            .await
            .expect("stage exact snapshot selector acknowledgement");
        device
            .drain_acknowledgements()
            .await
            .expect("activate exact snapshot selector acknowledgement");

        let selected = open_snapshot_bootstrap_authority(&store.storage, &store.root)
            .await
            .expect("open exact snapshot bootstrap authority")
            .select(
                &crate::joining::MembershipFloor(membership.head_refs().to_vec()),
                1,
            )
            .await
            .expect("select verified exact snapshot");
        let (_, _, selected, image, _, _) = selected.into_parts();

        assert_eq!(selected.reference.snapshot_hash, published.snapshot_hash());
        assert_ne!(
            selected.reference.snapshot_hash,
            selected.reference.object.stored_hash(),
        );
        assert_eq!(image, b"snapshot selector image");
    }

    #[tokio::test]
    async fn staged_snapshot_reuses_image_and_metadata_objects_after_restart() {
        let directory = tempfile::tempdir().expect("snapshot database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(1);
        assert!(publish(
            &storage,
            &db,
            &signer,
            b"restart image",
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
        let staged = store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read snapshot outbox")
            .expect("staged snapshot exists");
        drop(db);

        let reopened = open(&path, "snapshot-test-device");
        let published = resume(&storage, &reopened, &signer)
            .await
            .expect("resume snapshot publication")
            .expect("snapshot was pending");
        assert_eq!(published.snapshot_hash(), staged.reference.snapshot_hash);
        assert_eq!(published.image, staged.meta.value.image);
        assert!(store_database(&reopened)
            .outbound_snapshot_publication()
            .await
            .expect("read drained snapshot outbox")
            .is_none());
    }

    #[tokio::test]
    async fn exact_snapshot_loader_rejects_a_tampered_continuation_reference() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        assert!(crate::database::StoreDatabase::new(&db)
            .export_activated_device_continuation(&signer)
            .await
            .expect("export continuation before any snapshot")
            .latest_snapshot
            .is_none());
        publish(
            &storage,
            &db,
            &signer,
            b"continued snapshot",
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish continued snapshot");
        let published = store_database(&db)
            .latest_local_store_snapshot()
            .await
            .expect("load continued snapshot journal")
            .expect("continued snapshot journal exists");
        assert_eq!(
            crate::database::StoreDatabase::new(&db)
                .export_activated_device_continuation(&signer)
                .await
                .expect("export continuation after snapshot")
                .latest_snapshot,
            Some(published.reference.clone()),
        );
        let store = crate::sync::store::Store::load(
            crate::database::StoreDatabase::new(&db),
            storage.clone(),
            signer.clone(),
        )
        .await
        .expect("load continued snapshot Store");
        let mut writer = store
            .authorize_writer()
            .await
            .expect("authorize continued snapshot writer");
        writer
            .load_own_snapshot_for_test(&published.reference)
            .await
            .expect("load exact continued snapshot");

        let mut wrong_reference = published.reference.clone();
        wrong_reference.generation += 1;
        assert!(writer
            .load_own_snapshot_for_test(&wrong_reference)
            .await
            .is_err());

        let mut wrong_hash = published.reference.clone();
        wrong_hash.snapshot_hash = ObjectHash::digest(b"another snapshot");
        assert!(writer
            .load_own_snapshot_for_test(&wrong_hash)
            .await
            .is_err());

        let mut wrong_author = published.meta.clone();
        wrong_author.author_registration.registration_hash = ObjectHash::digest(b"another author");
        assert!(writer
            .verify_own_snapshot_bytes_for_test(&published.reference, &wrong_author.to_bytes())
            .is_err());

        let mut wrong_successor = published.meta;
        wrong_successor.successor.next_slot =
            crate::storage::cloud::ObjectSlot::logical("wrong-successor.json".to_string())
                .expect("valid wrong successor slot");
        assert!(writer
            .verify_own_snapshot_bytes_for_test(&published.reference, &wrong_successor.to_bytes())
            .is_err());
    }

    #[tokio::test]
    async fn lost_snapshot_image_create_response_is_resolved_before_metadata_creation() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_after_call(1);

        let published = publish(
            &storage,
            &db,
            &signer,
            b"lost response image",
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("resolve exact image-create response loss");
        assert_eq!(home.exact_create_count(), 2);
        assert_eq!(
            published.image.image_hash,
            ObjectHash::digest(b"lost response image")
        );
        assert!(store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read completed snapshot outbox")
            .is_none());
    }

    #[tokio::test]
    async fn snapshot_image_is_durable_before_metadata_can_be_created() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(2);

        assert!(publish(
            &storage,
            &db,
            &signer,
            b"ordered image",
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
        let pending = store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read retained snapshot outbox")
            .expect("snapshot remains staged");
        assert!(home
            .get(pending.image.object.slot().logical_key())
            .is_some());
        assert!(home
            .get(pending.reference.object.slot().logical_key())
            .is_none());

        let completed = resume(&storage, &db, &signer)
            .await
            .expect("retry ordered snapshot publication")
            .expect("snapshot remained pending");
        assert_eq!(completed.snapshot_hash(), pending.reference.snapshot_hash);
    }

    #[tokio::test]
    async fn occupied_snapshot_image_slot_blocks_metadata_and_completion() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(1);
        assert!(publish(
            &storage,
            &db,
            &signer,
            b"collision image",
            "2026-07-16T00:00:00Z",
        )
        .await
        .is_err());
        let pending = store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read snapshot outbox")
            .expect("snapshot remains staged");
        let image_slot = pending.image.object.slot().clone();
        home.insert_exact_object(image_slot.logical_key(), b"competing image".to_vec());

        assert!(resume(&storage, &db, &signer).await.is_err());
        assert_eq!(
            home.get(image_slot.logical_key()),
            Some(b"competing image".to_vec())
        );
        assert!(home
            .get(pending.reference.object.slot().logical_key())
            .is_none());
        assert!(store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read retained snapshot outbox")
            .is_some());
        assert!(store_database(&db)
            .latest_local_store_snapshot()
            .await
            .expect("read unpublished snapshot state")
            .is_none());
    }

    #[tokio::test]
    async fn snapshot_predecessor_and_reserved_successor_form_one_exact_chain() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (_root, _) = initialize(&db, &storage, &signer).await;
        let first = publish(
            &storage,
            &db,
            &signer,
            b"first image",
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish first snapshot");
        assert_eq!(first.generation, 0);
        let image_ownership = db
            .remote_object_for_test(first.image.object.clone())
            .await
            .expect("load published snapshot image ownership");
        assert!(matches!(
            image_ownership,
            crate::protocol::remote_object::RemoteObjectRecord::SharedLiveSet(record)
                if matches!(
                    &record.identity.domain,
                    crate::protocol::remote_object::SharedLiveSetObjectDomain::StoreSnapshotImage {
                        reference
                    } if reference == &first.image
                )
        ));
        let first_published = store_database(&db)
            .latest_local_store_snapshot()
            .await
            .expect("read first snapshot")
            .expect("first snapshot exists");
        home.fail_exact_create_before_call(1);
        assert!(publish(
            &storage,
            &db,
            &signer,
            b"second image",
            "2026-07-16T00:00:01Z",
        )
        .await
        .is_err());
        let second = store_database(&db)
            .outbound_snapshot_publication()
            .await
            .expect("read second snapshot")
            .expect("second snapshot remains staged");

        assert_eq!(
            second.meta.value.predecessor,
            Some(first_published.reference.clone())
        );
        assert_eq!(
            second.meta.value.successor.predecessor,
            Some(first_published.reference.clone())
        );
        assert_eq!(second.reference.object.slot(), &first.successor.next_slot);
        assert_eq!(second.reference.generation, first.generation + 1);
        resume(&storage, &db, &signer)
            .await
            .expect("resume second snapshot publication")
            .expect("publish staged second snapshot");
        let published_generations = db
            .test_sql(|database| {
                database.table_row_count(crate::database::DatabaseTestTable::named(
                    "published_store_snapshot",
                ))
            })
            .await
            .expect("count published Store snapshot generations");
        assert_eq!(published_generations, 2);
    }
}
