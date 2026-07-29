//! Durable exact Store snapshot publication.

mod image;

pub use image::{
    bootstrap_from_snapshot, create_snapshot, BootstrapResult, SnapshotBlobReconcile, SnapshotError,
};
pub(crate) use image::{
    create_circle_snapshot_with_host_blobs, create_snapshot_with_host_blobs,
    install_snapshot_blob_graph, open_database_image, should_create_snapshot,
    verify_circle_bootstrap_image, CreatedSnapshot, SnapshotBlobAudience,
};

use tracing::{info, warn};

use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::circle::{CircleBootstrapRef, CircleControlCoord, CircleEpochId, CircleId};
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store_commit::{
    circle_snapshot_image_semantic_prefix, circle_snapshot_slot_prefix,
    snapshot_image_semantic_prefix, snapshot_slot_prefix, CircleSnapshotMeta,
    CircleSnapshotSuccessorLink, CommitFrontier, DeviceStreamAnchor, ObjectHash, SnapshotImageRef,
    SnapshotMeta, SnapshotSuccessorLink, StoreHistoryCut, StoreProtocolError, StoreRootRef,
    StoreSnapshotRef, StoreSnapshotState,
};
use crate::KeyFingerprint;

pub(crate) struct SnapshotCut {
    pub(crate) snapshot: CreatedSnapshot,
    pub(crate) coverage: CommitFrontier,
}

enum SnapshotCutProjection {
    Store {
        routing_encryption: Option<crate::encryption::EncryptionService>,
    },
    Circle {
        routing_encryption: crate::encryption::EncryptionService,
        circle_id: crate::sync::circle::CircleId,
    },
}

impl super::AuthorizedWriterOperation<'_> {
    pub(crate) async fn publish_due_snapshots(
        &mut self,
        store_dir: &crate::store_dir::StoreDir,
        created_at: &str,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
        rotation_pending: bool,
    ) -> Result<(), crate::sync::cycle::SyncCycleFailure> {
        let keypair = self.identity();
        let resumed = drain_outbound_store_snapshot(self.storage(), self.database())
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
            .database()
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
                self.db().synced_tables().to_vec(),
                routing_encryption,
            )
            .await;
        match snapshot {
            Ok(cut) => {
                let meta = self
                    .push_store_snapshot(
                        cut.snapshot,
                        cut.coverage,
                        self.db().schema_version(),
                        created_at.to_string(),
                    )
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

        if let Err(error) = self
            .push_circle_snapshots(
                store_dir.as_ref().to_path_buf(),
                self.db().schema_version(),
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
        self.capture_projected_snapshot_cut(
            temp_dir,
            tables,
            SnapshotCutProjection::Store {
                routing_encryption: routing_encryption.cloned(),
            },
        )
        .await
    }

    pub(crate) async fn capture_circle_snapshot_cut(
        &self,
        temp_dir: std::path::PathBuf,
        routing_encryption: &crate::encryption::EncryptionService,
        circle_id: crate::sync::circle::CircleId,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        self.capture_projected_snapshot_cut(
            temp_dir,
            self.db().synced_tables().to_vec(),
            SnapshotCutProjection::Circle {
                routing_encryption: routing_encryption.clone(),
                circle_id,
            },
        )
        .await
    }

    pub(crate) async fn capture_circle_snapshot_at_cutoff(
        &self,
        temp_dir: std::path::PathBuf,
        routing_encryption: &crate::encryption::EncryptionService,
        circle_id: crate::sync::circle::CircleId,
        cutoff: CommitFrontier,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        let tables = self.db().synced_tables().to_vec();
        let routing_encryption = routing_encryption.clone();
        let blob_decls = self.db().blob_decls();
        let gates = self.db().gates();
        let routing_key = crate::sync::circle::derive_row_routing_key(
            &routing_encryption,
            self.store_root().store_root_hash,
        )
        .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        let retained_merge_materializations =
            crate::sync::store::database::StoreDatabase::from_database(self.db().clone())
                .retained_merge_materialization_cache();
        let root = self.store_root().clone();
        self.db()
            .call(move |connection| {
                // The successor bootstrap image is the accepted-history projection
                // at the exact cutoff, built below with no local write overlays.
                // Locally captured but unpublished rows are absent from that
                // projection by construction, so there is nothing for a
                // write-free-device precondition to protect: an Owner whose only
                // pending write is rotation-blocked can still finalize the close.
                let transaction = connection
                    .unchecked_transaction()
                    .map_err(crate::database::DbError::from)?;
                let mut retained_merge_materializations =
                    retained_merge_materializations.lock().map_err(|_| {
                        crate::database::DbError::Message(
                            "retained Merge materialization cache lock is poisoned".to_string(),
                        )
                    })?;
                let replay = super::pull::replay_retained_merge_projection_on(
                    &transaction,
                    &root,
                    &mut retained_merge_materializations,
                    &blob_decls,
                    &gates,
                    &tables,
                    Some(&routing_key),
                    &std::collections::BTreeSet::new(),
                    Some(&cutoff),
                    false,
                    super::pull::LocalStoreMembership::Current,
                )?;
                transaction
                    .rollback()
                    .map_err(crate::database::DbError::from)?;
                let replay_frontier = CommitFrontier::from_refs(
                    crate::sync::store::database::StoreDatabase::materialized_frontier_on(
                        &replay, None,
                    )?,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
                if replay_frontier != cutoff {
                    return Err(crate::database::DbError::Message(
                        "Circle close cutoff is not an exact retained Store frontier".to_string(),
                    ));
                }
                let snapshot = create_circle_snapshot_with_host_blobs(
                    &replay,
                    &root,
                    &temp_dir,
                    &tables,
                    &routing_encryption,
                    circle_id,
                )
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
                Ok(SnapshotCut {
                    snapshot,
                    coverage: cutoff,
                })
            })
            .await
    }

    async fn capture_projected_snapshot_cut(
        &self,
        temp_dir: std::path::PathBuf,
        tables: Vec<crate::sync::session::SyncedTable>,
        projection: SnapshotCutProjection,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        let root = self.store_root().clone();
        self.db()
            .call(move |connection| {
                require_no_unpublished_store_writes(connection)?;
                let snapshot = match projection {
                    SnapshotCutProjection::Store { routing_encryption } => {
                        create_snapshot_with_host_blobs(
                            connection,
                            &root,
                            &temp_dir,
                            &tables,
                            routing_encryption.as_ref(),
                        )
                    }
                    SnapshotCutProjection::Circle {
                        routing_encryption,
                        circle_id,
                    } => create_circle_snapshot_with_host_blobs(
                        connection,
                        &root,
                        &temp_dir,
                        &tables,
                        &routing_encryption,
                        circle_id,
                    ),
                }
                .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
                let coverage = CommitFrontier::from_refs(
                    crate::sync::store::database::StoreDatabase::materialized_frontier_on(
                        connection, None,
                    )?,
                )
                .map_err(|error| {
                    crate::database::DbError::Message(format!("snapshot coverage: {error}"))
                })?;
                Ok(SnapshotCut { snapshot, coverage })
            })
            .await
    }
}

fn require_no_unpublished_store_writes(
    connection: &rusqlite::Connection,
) -> Result<(), crate::database::DbError> {
    let pending: i64 = connection
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM store_writes
                WHERE status != '\"local_only\"'
                  AND json_extract(status, '$.published') IS NULL
            )",
            [],
            |row| row.get(0),
        )
        .map_err(crate::database::DbError::from)?;
    if pending != 0 {
        return Err(crate::database::DbError::Message(
            "snapshot cut refused while unpublished Store writes exist".to_string(),
        ));
    }
    Ok(())
}

impl super::AuthorizedWriterOperation<'_> {
    pub(crate) async fn push_store_snapshot(
        &mut self,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let storage = self.storage();
        let store_root_hash = self.store_root().store_root_hash;
        let membership = self.membership().clone();
        let database = self.database().clone();
        let membership = &membership;
        let database = &database;
        let _publication = database.lock_snapshot_publication().await;
        drain_snapshot_spool_cleanup(database).await?;
        if let Some(pending) = database
            .outbound_snapshot_publication()
            .await
            .map_err(publication_error)?
        {
            return publish_durable_snapshot(storage, database, pending).await;
        }
        let (registration_ref, registration, device_signer) = {
            let (registration_ref, registration, device_signer) = self.registration();
            (
                registration_ref.clone(),
                registration.clone(),
                device_signer.clone(),
            )
        };
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
        let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(SnapshotError::PublicationState(
                "snapshot publication requires resolved membership".to_string(),
            ));
        };
        let membership_state = crate::sync::circle_control::StoreMembershipStateRef::from_parts(
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
            .history()
            .prepare_merge_snapshot_history_summary(
                &coverage,
                membership,
                &resolved_devices,
                &registration_ref,
                &registration,
            )
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let storage = self.storage();
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

        let snapshot_owner = crate::sync::remote_object::SnapshotObjectOwner {
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
        publish_durable_snapshot(storage, database, pending).await
    }

    async fn prepare_snapshot_blobs(
        &self,
        snapshot: CreatedSnapshot,
        owner: crate::sync::remote_object::SnapshotObjectOwner,
    ) -> Result<(Vec<u8>, Vec<crate::database::PreparedSnapshotBlob>), SnapshotError> {
        let database = self.database();
        let storage = self.storage();
        let (registration_ref, registration, _) = self.registration();
        let authority =
            crate::sync::storage::BlobWriteAuthority::new(registration_ref, registration)
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
                crate::sync::audience_package::PackageAudience::Store,
            ),
            SnapshotBlobAudience::Circle { circle_id, control } => {
                let (encryption, key_fingerprint) =
                    database.circle_publication_context(circle_id, control.coordinate().clone())
                    .await
                    .map_err(publication_error)?;
                (
                    crate::blob::locator::RemoteAudience::Circle(circle_id),
                    crate::sync::storage::BlobSpoolProtection::Opaque(encryption),
                    crate::sync::audience_package::PackageAudience::Circle {
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
            let binding = crate::sync::audience_package::RowBlobLocatorBinding::new(
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
        let remote = crate::sync::remote_object::RemoteObjectRecord::snapshot_activated_blob(
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
        let image = match install_snapshot_blob_graph(db_image, &prepared, &image_store_dir) {
            Ok(image) => image,
            Err(error) => {
                cleanup_snapshot_spools(&prepared)
                    .await
                    .map_err(|cleanup| {
                        SnapshotError::PublicationState(format!(
                        "snapshot image closure failed: {error}; spool cleanup failed: {cleanup}"
                    ))
                    })?;
                return Err(error);
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
        if paths.insert(path.clone()) && !crate::local_blob::remove_file(path).await? {
            return Err(format!(
                "prepared snapshot spool {} is absent",
                path.display()
            ));
        }
    }
    Ok(())
}

pub(crate) async fn drain_outbound_store_snapshot(
    storage: &dyn SyncStorage,
    database: &crate::sync::store::StoreDatabase,
) -> Result<Option<SnapshotMeta>, SnapshotError> {
    let _publication = database.lock_snapshot_publication().await;
    drain_snapshot_spool_cleanup(database).await?;
    let Some(pending) = database
        .outbound_snapshot_publication()
        .await
        .map_err(publication_error)?
    else {
        return Ok(None);
    };
    publish_durable_snapshot(storage, database, pending)
        .await
        .map(Some)
}

async fn publish_durable_snapshot(
    storage: &dyn SyncStorage,
    database: &crate::sync::store::StoreDatabase,
    pending: crate::database::DurableSnapshotPublication,
) -> Result<SnapshotMeta, SnapshotError> {
    let meta = &pending.meta.value;
    let device_id = meta.author_registration.device_id.to_string();
    let image_context = ProtocolObjectContext::store_encrypted(
        meta.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotImage,
    );
    let image_prefix = snapshot_image_semantic_prefix(&device_id, meta.image.image_hash);
    for prepared in &pending.blobs {
        let blob = prepared.bindings[0].blob();
        let uploader = blob.locator().uploader().clone();
        let registration = database
            .activated_store_device_registration(uploader.clone())
            .await
            .map_err(publication_error)?;
        let authority = crate::sync::storage::BlobWriteAuthority::new(&uploader, &registration)
            .map_err(SnapshotError::Bucket)?;
        if let Some(spool_path) = &prepared.spool_path {
            storage
                .create_blob_object_from_file(
                    blob,
                    &authority,
                    spool_path,
                    &crate::storage::cloud::no_progress(),
                )
                .await
                .map_err(SnapshotError::Bucket)?;
        }
        storage
            .verify_blob_object(blob)
            .await
            .map_err(SnapshotError::Bucket)?;
    }
    storage
        .create_protocol_object(&pending.image.prepared)
        .await
        .map_err(SnapshotError::Bucket)?;
    let image_readback = storage
        .read_protocol_object(&image_context, &meta.image.object, &image_prefix)
        .await
        .map_err(SnapshotError::Bucket)?;
    if image_readback != pending.image.bytes {
        return Err(SnapshotError::PublicationState(
            "Store snapshot image exact readback differs from prepared bytes".to_string(),
        ));
    }

    let meta_context = ProtocolObjectContext::signed_plaintext(
        meta.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let meta_prefix = snapshot_slot_prefix(&device_id, pending.reference.generation);
    storage
        .create_protocol_object(&pending.meta.prepared)
        .await
        .map_err(SnapshotError::Bucket)?;
    let meta_readback = storage
        .read_protocol_object(&meta_context, &pending.reference.object, &meta_prefix)
        .await
        .map_err(SnapshotError::Bucket)?;
    if meta_readback != pending.meta.bytes {
        return Err(SnapshotError::PublicationState(
            "Store snapshot metadata exact readback differs from prepared bytes".to_string(),
        ));
    }
    database
        .complete_snapshot_publication(pending.reference)
        .await
        .map_err(publication_error)?;
    drain_snapshot_spool_cleanup(database).await?;
    Ok(pending.meta.value)
}

impl super::AuthorizedWriterOperation<'_> {
    /// Author one Circle snapshot for every Circle this device holds active
    /// package access to, under the same owner/cadence gate the caller already
    /// applied for the Store snapshot. A Circle without active access is not
    /// enumerated (an inactive recipient holds no epoch key) and so is skipped
    /// by construction; a capture or publication failure for one Circle is logged
    /// and does not abort the others or the cycle.
    pub(crate) async fn push_circle_snapshots(
        &self,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), SnapshotError> {
        let inputs = self
            .database()
            .circle_acknowledgement_publication_inputs()
            .await
            .map_err(publication_error)?;
        if inputs.is_empty() {
            return Ok(());
        }
        // Circle row routing ids are derived from the Store generation-one key, not
        // the per-Circle epoch key each input seals with. A scoped Store that holds
        // Circles always carries this routing service; its absence here is a
        // structural contradiction, not a per-Circle skip.
        let store_routing = store_routing.ok_or_else(|| {
            SnapshotError::PublicationState(
                "Circle snapshot authoring requires the Store routing key".to_string(),
            )
        })?;
        for input in inputs {
            // Resume a pending publication for this Circle before authoring
            // another — finishing a durable operation needs no fresh capture.
            if let Some(pending) = self
                .database()
                .outbound_circle_snapshot_publication(input.circle_id)
                .await
                .map_err(publication_error)?
            {
                match publish_durable_circle_snapshot(self.storage(), self.database(), pending)
                    .await
                {
                    Ok(meta) => tracing::info!(
                        circle_id = %input.circle_id,
                        generation = meta.generation,
                        "resumed pending Circle snapshot publication"
                    ),
                    Err(error) => tracing::warn!(
                        circle_id = %input.circle_id,
                        "skip Circle snapshot: pending publication failed: {error}"
                    ),
                }
                continue;
            }
            // Retention: do not author a new generation until every active-access
            // device has acknowledged coverage past the previous one — an
            // unstable snapshot is not yet usable as coverage evidence.
            if let Some(previous) = self
                .database()
                .latest_local_circle_snapshot(input.circle_id)
                .await
                .map_err(publication_error)?
            {
                if !self
                    .circle_snapshot_is_stable(input.circle_id, &input.control, &previous.cut)
                    .await?
                {
                    tracing::debug!(
                        circle_id = %input.circle_id,
                        "skip Circle snapshot: previous generation is not yet acknowledgement-stable"
                    );
                    continue;
                }
            }
            let circle_temp = temp_dir.join(format!("circle-snapshot-{}", input.circle_id));
            std::fs::create_dir_all(&circle_temp).map_err(SnapshotError::Io)?;
            let cut = match self
                .capture_circle_snapshot_cut(circle_temp, store_routing, input.circle_id)
                .await
            {
                Ok(cut) => cut,
                Err(error) => {
                    tracing::warn!(
                        circle_id = %input.circle_id,
                        "skip Circle snapshot: capture failed: {error}"
                    );
                    continue;
                }
            };
            match self
                .push_circle_snapshot(
                    input.circle_id,
                    input.control,
                    input.epoch_id,
                    input.key_fingerprint,
                    input.epoch_encryption,
                    cut.snapshot,
                    cut.coverage,
                    schema_version,
                    created_at.to_string(),
                )
                .await
            {
                Ok(meta) => tracing::info!(
                    circle_id = %input.circle_id,
                    generation = meta.generation,
                    "Circle snapshot created and pushed"
                ),
                Err(error) => tracing::warn!(
                    circle_id = %input.circle_id,
                    "skip Circle snapshot: publication failed: {error}"
                ),
            }
        }
        Ok(())
    }

    /// Whether every device that holds active Circle access has published a
    /// Circle acknowledgement whose accepted Store frontier covers `snapshot_cut`
    /// — the point at which a snapshot at that cut is usable as coverage evidence.
    /// The current active-access device set is enumerated first (every active
    /// Store device owned by a current roster member), so a device that holds
    /// access but has never acknowledged fails the check closed rather than being
    /// invisible. Each device's acknowledgement is decrypted with the epoch key
    /// resolved under the reader's current control (its keyring retains the epochs
    /// it holds, so an acknowledgement sealed under a rotated epoch stays
    /// readable). Mirrors the Store-level `assemble_snapshot_stability`.
    pub(crate) async fn circle_snapshot_is_stable(
        &self,
        circle_id: CircleId,
        current_control: &CircleControlCoord,
        snapshot_cut: &CommitFrontier,
    ) -> Result<bool, SnapshotError> {
        Ok(self
            .stable_circle_acknowledgements_dominating(circle_id, current_control, snapshot_cut)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
            .is_some())
    }
}

#[cfg(test)]
impl super::AuthorizedWriterOperation<'_> {
    pub(crate) async fn load_circle_snapshot_metas_for_test(
        &self,
        circle_id: CircleId,
        encryption: EncryptionService,
    ) -> Result<Vec<CircleSnapshotMeta>, SnapshotError> {
        load_circle_snapshot_stream(
            self.storage(),
            self.store_root(),
            circle_id,
            encryption,
            &self.writer.registration_ref,
            &self.writer.registration,
        )
        .await
    }

    pub(crate) async fn verify_standalone_circle_snapshot_image_for_test(
        &self,
        circle_id: CircleId,
        epoch_encryption: EncryptionService,
        store_routing: &EncryptionService,
    ) -> Result<(), SnapshotError> {
        let stream = self
            .load_circle_snapshot_metas_for_test(circle_id, epoch_encryption.clone())
            .await?;
        let selected = select_maximal_circle_snapshot(stream).ok_or_else(|| {
            SnapshotError::PublicationState("no standalone Circle snapshot to verify".to_string())
        })?;
        let author_device = selected.author_registration.device_id.to_string();
        let image_context = ProtocolObjectContext::circle(
            self.store_root().store_root_hash,
            ProtocolObjectDomain::CircleSnapshotImage,
            epoch_encryption,
        );
        let image = self
            .storage()
            .read_protocol_object(
                &image_context,
                &selected.bootstrap.image.object,
                &circle_snapshot_image_semantic_prefix(
                    circle_id,
                    &author_device,
                    selected.bootstrap.image.image_hash,
                ),
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        let routing_key = crate::sync::circle::derive_row_routing_key(
            store_routing,
            self.store_root().store_root_hash,
        )
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
        verify_circle_bootstrap_image(
            &image,
            &selected.bootstrap,
            circle_id,
            self.db().synced_tables(),
            Some(&routing_key),
        )
    }

    async fn load_own_snapshot_for_test(
        &mut self,
        reference: &StoreSnapshotRef,
    ) -> Result<SnapshotMeta, SnapshotError> {
        let registration_ref = self.writer.registration_ref.clone();
        let registration = self.writer.registration.clone();
        self.history_verifier_mut()
            .commit_verifier()
            .load_store_snapshot(&registration_ref, &registration, reference)
            .await
            .map(|(_, meta)| meta)
            .map_err(SnapshotError::StoreObject)
    }

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

    pub(crate) async fn author_one_circle_snapshot_for_test(
        &self,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: &crate::encryption::EncryptionService,
    ) -> Result<CircleSnapshotMeta, SnapshotError> {
        let input = self
            .database()
            .circle_acknowledgement_publication_inputs()
            .await
            .map_err(publication_error)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                SnapshotError::PublicationState("no active Circle to snapshot".to_string())
            })?;
        std::fs::create_dir_all(&temp_dir).map_err(SnapshotError::Io)?;
        let cut = self
            .capture_circle_snapshot_cut(temp_dir, store_routing, input.circle_id)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        self.push_circle_snapshot(
            input.circle_id,
            input.control,
            input.epoch_id,
            input.key_fingerprint,
            input.epoch_encryption,
            cut.snapshot,
            cut.coverage,
            schema_version,
            created_at.to_string(),
        )
        .await
    }
}

impl super::AuthorizedWriterOperation<'_> {
    #[allow(clippy::too_many_arguments)]
    async fn push_circle_snapshot(
        &self,
        circle_id: CircleId,
        control: CircleControlCoord,
        epoch_id: CircleEpochId,
        key_fingerprint: KeyFingerprint,
        encryption: EncryptionService,
        snapshot: CreatedSnapshot,
        coverage: CommitFrontier,
        schema_version: u32,
        created_at: String,
    ) -> Result<CircleSnapshotMeta, SnapshotError> {
        let storage = self.storage();
        let database = self.database();
        let db = database.sqlite();
        let device_id = self.local_device_id().to_string();
        let root = self.store_root();
        let registration_ref = self.writer.registration_ref.clone();
        let device_signer = &self.writer.device_signer;
        let _publication = database.lock_snapshot_publication().await;
        drain_snapshot_spool_cleanup(database).await?;
        // The image references only already-published Circle blobs, verified exact —
        // the same closure a member-addition bootstrap image carries.
        let blobs =
            super::super::circles::verified_circle_bootstrap_blobs(storage, circle_id, &snapshot)
                .await
                .map_err(|error| SnapshotError::PublishBlobs(error.to_string()))?;
        let image_bytes = snapshot.db_image;
        let image_hash = ObjectHash::digest(&image_bytes);
        let image_context = ProtocolObjectContext::circle(
            root.store_root_hash,
            ProtocolObjectDomain::CircleSnapshotImage,
            encryption.clone(),
        );
        let image_prefix = circle_snapshot_image_semantic_prefix(circle_id, &device_id, image_hash);
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
        let bootstrap = CircleBootstrapRef {
            coverage,
            schema_version,
            sync_routing_hash: db.sync_routing_hash(),
            image: SnapshotImageRef {
                image_hash,
                object: image_prepared.reference().clone(),
            },
            blobs,
        };
        let previous = database
            .latest_local_circle_snapshot(circle_id)
            .await
            .map_err(publication_error)?;
        // Generation zero occupies a deterministic slot a reader can compute (there
        // is no registration snapshot anchor for a per-Circle stream); later
        // generations occupy the predecessor's create-once successor slot.
        let stream_first_slot = crate::storage::cloud::ObjectSlot::logical(format!(
            "{}.json",
            circle_snapshot_slot_prefix(circle_id, &device_id, 0)
        ))
        .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        let (generation, predecessor, current_slot) = match previous {
            Some(previous) => (
                previous
                    .reference
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| {
                        SnapshotError::PublicationState(
                            "Circle snapshot generation overflow".to_string(),
                        )
                    })?,
                Some(previous.reference),
                previous.successor_slot,
            ),
            None => (0, None, stream_first_slot),
        };
        let meta_context = ProtocolObjectContext::circle(
            root.store_root_hash,
            ProtocolObjectDomain::CircleSnapshotMeta,
            encryption,
        );
        let semantic_prefix = circle_snapshot_slot_prefix(circle_id, &device_id, generation);
        let next_slot = storage
            .allocate_protocol_slot(
                &meta_context,
                &circle_snapshot_slot_prefix(
                    circle_id,
                    &device_id,
                    generation.checked_add(1).ok_or_else(|| {
                        SnapshotError::PublicationState(
                            "Circle snapshot generation overflow".to_string(),
                        )
                    })?,
                ),
                ".json",
            )
            .await
            .map_err(SnapshotError::Bucket)?;
        let activation = crate::sync::store_commit::circle_snapshot_stream_activation(
            root.store_root_hash,
            &registration_ref,
            circle_id,
            &device_id,
        )
        .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        let meta = CircleSnapshotMeta::signed(
            root.store_root_hash,
            circle_id,
            registration_ref,
            control,
            epoch_id,
            key_fingerprint,
            generation,
            bootstrap,
            created_at,
            CircleSnapshotSuccessorLink {
                activation,
                predecessor,
                next_slot,
            },
            device_signer,
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
            .stage_circle_snapshot_publication(
                meta.clone(),
                meta_prepared,
                image_bytes,
                image_prepared,
                Vec::new(),
            )
            .await
            .map_err(publication_error)?;
        let pending = database
            .outbound_circle_snapshot_publication(circle_id)
            .await
            .map_err(publication_error)?
            .ok_or_else(|| {
                SnapshotError::PublicationState(
                    "staged Circle snapshot publication row is absent".to_string(),
                )
            })?;
        publish_durable_circle_snapshot(storage, database, pending).await
    }
}

async fn publish_durable_circle_snapshot(
    storage: &dyn SyncStorage,
    database: &crate::sync::store::StoreDatabase,
    pending: crate::database::DurableCircleSnapshotPublication,
) -> Result<CircleSnapshotMeta, SnapshotError> {
    // `create_protocol_object` reads the stored bytes back and refuses different
    // bytes at the slot, so the exact ciphertext of the image and metadata is
    // durable before completion; the sealed plaintext binding was established at
    // prepare time, so no epoch key is needed to confirm the upload.
    storage
        .create_protocol_object(&pending.image.prepared)
        .await
        .map_err(SnapshotError::Bucket)?;
    storage
        .create_protocol_object(&pending.meta.prepared)
        .await
        .map_err(SnapshotError::Bucket)?;
    database
        .complete_circle_snapshot_publication(pending.reference)
        .await
        .map_err(publication_error)?;
    drain_snapshot_spool_cleanup(database).await?;
    Ok(pending.meta.value)
}

async fn drain_snapshot_spool_cleanup(
    database: &crate::sync::store::StoreDatabase,
) -> Result<(), SnapshotError> {
    for path in database
        .snapshot_blob_spool_cleanup_paths()
        .await
        .map_err(publication_error)?
    {
        crate::local_blob::remove_file(&path)
            .await
            .map_err(SnapshotError::PublicationState)?;
        database
            .complete_snapshot_blob_spool_cleanup(&path)
            .await
            .map_err(publication_error)?;
    }
    Ok(())
}

fn snapshot_first_slot(
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
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
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
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

pub(crate) async fn load_store_snapshot_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
) -> Result<Vec<crate::database::PublishedStoreSnapshot>, SnapshotError> {
    let mut slot = snapshot_first_slot(registration)?.clone();
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let mut generation = 0_u64;
    let mut predecessor = None;
    let mut snapshots = Vec::new();
    loop {
        let prefix = snapshot_slot_prefix(&registration.device_id.to_string(), generation);
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(value) => value,
            Err(crate::sync::storage::StorageError::NotFound(_)) => break,
            Err(error) => return Err(SnapshotError::Bucket(error)),
        };
        let verify_root = root.clone();
        let verify_registration_ref = registration_ref.clone();
        let verify_registration = registration.clone();
        let verify_object = object.clone();
        let (reference, meta) = crate::sync::store_objects::run_blocking_object_verification(
            &prefix,
            &object,
            Box::new(move || {
                let semantic_hash = SnapshotMeta::semantic_hash_from_bytes(&bytes)
                    .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                let reference = StoreSnapshotRef {
                    generation,
                    snapshot_hash: semantic_hash,
                    object: verify_object,
                };
                let meta = verify_store_snapshot_bytes(
                    &verify_root,
                    &verify_registration_ref,
                    &verify_registration,
                    &reference,
                    &bytes,
                )
                .map_err(|error| StoreProtocolError::Malformed(error.to_string()))?;
                Ok((reference, meta))
            }),
        )
        .await
        .map_err(SnapshotError::StoreObject)?;
        if meta.predecessor != predecessor {
            return Err(SnapshotError::Parse(
                "Store snapshot stream has an invalid exact predecessor".to_string(),
            ));
        }
        let successor_slot = meta.successor.next_slot.clone();
        slot = successor_slot.clone();
        predecessor = Some(reference.clone());
        snapshots.push(crate::database::PublishedStoreSnapshot {
            reference,
            successor_slot,
            meta,
        });
        generation = generation.checked_add(1).ok_or_else(|| {
            SnapshotError::Parse("Store snapshot generation overflow".to_string())
        })?;
    }
    Ok(snapshots)
}

/// Load one device's Circle snapshot stream, decrypting each metadata object
/// with the Circle epoch key. Generation zero sits at a deterministic slot the
/// reader computes from the author's device id (a per-Circle stream has no
/// registration anchor); later generations follow the create-once successor
/// slot. The predecessor chain is verified exact.
pub(crate) async fn load_circle_snapshot_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    encryption: EncryptionService,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
) -> Result<Vec<CircleSnapshotMeta>, SnapshotError> {
    Ok(load_circle_snapshot_stream_refs(
        storage,
        root,
        circle_id,
        encryption,
        registration_ref,
        registration,
    )
    .await?
    .into_iter()
    .map(|(_, meta)| meta)
    .collect())
}

/// Each Circle snapshot in the per-(device, Circle) stream paired with its exact
/// reference — the reference a reclaim locator or restore selection binds.
pub(crate) async fn load_circle_snapshot_stream_refs(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    encryption: EncryptionService,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
) -> Result<
    Vec<(
        crate::sync::store_commit::CircleSnapshotRef,
        CircleSnapshotMeta,
    )>,
    SnapshotError,
> {
    if registration_ref.device_id != registration.device_id {
        return Err(SnapshotError::Parse(
            "Circle snapshot registration reference names another device".to_string(),
        ));
    }
    let device_id = registration.device_id.to_string();
    let context = ProtocolObjectContext::circle(
        root.store_root_hash,
        ProtocolObjectDomain::CircleSnapshotMeta,
        encryption,
    );
    let mut slot = crate::storage::cloud::ObjectSlot::logical(format!(
        "{}.json",
        circle_snapshot_slot_prefix(circle_id, &device_id, 0)
    ))
    .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    let mut generation = 0_u64;
    let mut predecessor = None;
    let mut snapshots = Vec::new();
    loop {
        let prefix = circle_snapshot_slot_prefix(circle_id, &device_id, generation);
        let (bytes, object) = match storage.read_protocol_slot(&context, &slot, &prefix).await {
            Ok(value) => value,
            Err(crate::sync::storage::StorageError::NotFound(_)) => break,
            Err(error) => return Err(SnapshotError::Bucket(error)),
        };
        let semantic_hash = CircleSnapshotMeta::semantic_hash_from_bytes(&bytes)
            .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        let reference = crate::sync::store_commit::CircleSnapshotRef {
            generation,
            snapshot_hash: semantic_hash,
            object,
        };
        let meta =
            CircleSnapshotMeta::parse_at(&bytes, root.store_root_hash, &reference, registration)
                .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        if meta.circle_id != circle_id || meta.successor.predecessor != predecessor {
            return Err(SnapshotError::Parse(
                "Circle snapshot stream has an invalid exact predecessor".to_string(),
            ));
        }
        slot = meta.successor.next_slot.clone();
        predecessor = Some(reference.clone());
        snapshots.push((reference, meta));
        generation = generation.checked_add(1).ok_or_else(|| {
            SnapshotError::Parse("Circle snapshot generation overflow".to_string())
        })?;
    }
    Ok(snapshots)
}

/// The maximal Circle snapshot by coverage domination among candidates. Stability
/// against Circle acknowledgements is applied by the caller; this returns the
/// snapshot whose cut no other candidate strictly dominates.
pub(crate) fn select_maximal_circle_snapshot(
    mut candidates: Vec<CircleSnapshotMeta>,
) -> Option<CircleSnapshotMeta> {
    let all = candidates.clone();
    candidates.retain(|snapshot| {
        !all.iter().any(|other| {
            other.snapshot_hash() != snapshot.snapshot_hash()
                && coverage_dominates(&other.bootstrap.coverage, &snapshot.bootstrap.coverage)
        })
    });
    candidates.sort_by_key(|snapshot| snapshot.snapshot_hash());
    candidates.pop()
}

/// One image the restoring identity could stage for a Circle: the exact commit
/// that activates its control, and the verified image bound to that control.
struct StagedCircleImageCandidate {
    activation_commit: crate::sync::store_commit::StoreBatchCommitRef,
    image: crate::sync::store::circle_controls::VerifiedCircleImage,
}

impl StagedCircleImageCandidate {
    fn coverage(&self) -> &CommitFrontier {
        &self.image.reference().coverage
    }
}

/// Decide, per retained Circle, what the restoring identity stages: install a
/// verified image, or clear a preserved coverage row it cannot decrypt.
///
/// The restoring identity's access is re-resolved from the verified control chain
/// on the just-installed Store image — never the snapshot author's preserved
/// access caches, which belong to another identity. For each Circle the identity
/// holds active access to, the maximal verified image whose lineage the retained
/// controls prove and whose cut the Store frontier covers is chosen among three
/// candidates: the preserved coverage row, the identity's own leaf-named
/// bootstrap, and the maximal standalone snapshot across the activated devices. A
/// Circle the identity cannot decrypt yields `ClearCoverage`, so no coverage row
/// an inaccessible Circle could replay from survives the restore.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn select_staged_circle_decisions(
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    query_db: &crate::sync::store::StoreDatabase,
    store_frontier: &CommitFrontier,
    restorer_identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<Vec<crate::database::StagedCircleDecision>, SnapshotError> {
    use crate::database::StagedCircleDecision;
    use crate::sync::store::StoreDatabase;

    let sqlite = query_db.sqlite();
    let root = history_verifier.root().clone();
    // The stream-activation index the control-stream authority resolves against is
    // written by the pull, which has not run on a freshly restored device; seed it
    // from the retained materializations selection reads anyway.
    let seed_root = root.clone();
    sqlite
        .call(move |conn| {
            StoreDatabase::seed_stream_activation_index_from_retained_on(conn, &seed_root)
        })
        .await
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    let circle_index = sqlite
        .call(StoreDatabase::circle_control_activation_index_on)
        .await
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    let preserved_rows = sqlite
        .call(StoreDatabase::circle_bootstrap_replay_inputs_on)
        .await
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    let device_registrations = query_db
        .activated_store_device_registration_records()
        .await
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;

    let mut decisions = Vec::new();
    for (circle_id, controls) in circle_index {
        let head_root = root.clone();
        let head_control = sqlite
            .call(move |conn| {
                StoreDatabase::head_circle_control_on(conn, &head_root, circle_id, &controls)
            })
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
        let Some(head_control) = head_control else {
            warn!(%circle_id, "restore selection: Circle has no head control; clearing coverage");
            decisions.push(StagedCircleDecision::ClearCoverage(circle_id));
            continue;
        };
        let head_lookup = head_control.clone();
        let head_commit = sqlite
            .call(move |conn| {
                StoreDatabase::circle_activation_commit_ref_on(conn, circle_id, &head_lookup)
            })
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?
            .ok_or_else(|| {
                SnapshotError::BootstrapState(format!(
                    "restore selection: Circle {circle_id} head control has no activating commit"
                ))
            })?;

        let access = resolve_restorer_circle_access(
            history_verifier,
            query_db,
            restorer_identity,
            routing_key,
            circle_id,
            &head_control,
            &head_commit,
        )
        .await?;
        let (epoch_encryption, leaf_bootstrap) = match access {
            // The restoring identity cannot decrypt this Circle — delete any
            // preserved coverage row so replay never reconstructs an image it has
            // no access to.
            super::super::circles::activation::LocalCircleAccess::NoAccess => {
                decisions.push(StagedCircleDecision::ClearCoverage(circle_id));
                continue;
            }
            super::super::circles::activation::LocalCircleAccess::Active {
                epoch_encryption,
                leaf_bootstrap,
            } => (epoch_encryption, leaf_bootstrap),
        };

        let mut candidates: Vec<StagedCircleImageCandidate> = Vec::new();
        // The restoring identity's own leaf-named bootstrap: the baseline for a
        // Circle whose accessible content predates the identity's join and which no
        // forward replay reconstructs.
        if let Some(image) = leaf_bootstrap {
            candidates.push(StagedCircleImageCandidate {
                activation_commit: head_commit.clone(),
                image,
            });
        }
        for (activation_commit, image) in &preserved_rows {
            if image.circle_id() == circle_id {
                candidates.push(StagedCircleImageCandidate {
                    activation_commit: activation_commit.clone(),
                    image: image.clone(),
                });
            }
        }
        // Standalone Circle snapshots are sealed under the Circle epoch key the
        // identity's active leaf carries, not the Store routing key.
        if let Some(candidate) = select_standalone_snapshot_candidate(
            query_db,
            history_verifier.storage(),
            history_verifier.root(),
            circle_id,
            &head_control,
            &epoch_encryption,
            routing_key,
            &device_registrations,
        )
        .await?
        {
            candidates.push(candidate);
        }

        match choose_maximal_installable_candidate(circle_id, store_frontier, candidates)? {
            Some(candidate) => decisions.push(StagedCircleDecision::Install {
                activation_commit: candidate.activation_commit,
                image: candidate.image,
            }),
            None => {
                warn!(
                    %circle_id,
                    "restore selection: Circle has active access but no coverage image; \
                     it replays from live history if retained"
                );
            }
        }
    }
    Ok(decisions)
}

/// Resolve the restoring identity's own access at a Circle's head control. The
/// head control's activating commit is retained, so its verified materialization
/// carries the already-verified control; only the identity's own access envelope,
/// the Store membership checkpoint, and (if the leaf names one) its own bootstrap
/// image are read from storage. This never re-walks the control's covered-head
/// lineage, which a reclaimed restore may no longer retain.
#[allow(clippy::too_many_arguments)]
async fn resolve_restorer_circle_access(
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    query_db: &crate::sync::store::StoreDatabase,
    restorer_identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    circle_id: CircleId,
    head_control: &CircleControlCoord,
    head_commit: &crate::sync::store_commit::StoreBatchCommitRef,
) -> Result<super::super::circles::activation::LocalCircleAccess, SnapshotError> {
    use crate::sync::store::StoreDatabase;
    let commit_lookup = head_commit.clone();
    let root = history_verifier.root().clone();
    let owned = query_db
        .sqlite()
        .call(move |conn| {
            StoreDatabase::load_retained_merge_materialization_by_ref_on(
                conn,
                &root,
                &commit_lookup,
            )
        })
        .await
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    let commit = owned.commit().clone();
    let reference = owned
        .circle_activations()
        .circles()
        .iter()
        .find(|reference| {
            reference.circle_id == circle_id && reference.control.coord == *head_control
        })
        .ok_or_else(|| {
            SnapshotError::BootstrapState(format!(
                "restore selection: Circle {circle_id} head control is absent from its retained \
                 activation"
            ))
        })?;
    super::super::circles::activation::resolve_local_circle_access(
        history_verifier,
        query_db,
        &commit,
        &reference.reference,
        &reference.control,
        restorer_identity,
        routing_key,
    )
    .await
    .map_err(|error| SnapshotError::BootstrapState(error.to_string()))
}

/// The maximal standalone Circle snapshot across the activated devices whose
/// lineage the retained head control proves, downloaded and verified byte-exact.
#[allow(clippy::too_many_arguments)]
async fn select_standalone_snapshot_candidate(
    query_db: &crate::sync::store::StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    head_control: &CircleControlCoord,
    encryption: &EncryptionService,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    device_registrations: &[(
        crate::sync::store_commit::StoreDeviceRegistrationRef,
        crate::sync::store_commit::StoreDeviceRegistration,
    )],
) -> Result<Option<StagedCircleImageCandidate>, SnapshotError> {
    use crate::sync::store::StoreDatabase;
    let mut installable: Vec<(
        CircleSnapshotMeta,
        crate::sync::store_commit::StoreBatchCommitRef,
    )> = Vec::new();
    for (registration_ref, registration) in device_registrations {
        let stream = load_circle_snapshot_stream(
            storage,
            root,
            circle_id,
            encryption.clone(),
            registration_ref,
            registration,
        )
        .await?;
        for snapshot in stream {
            // A control reclaimed after an epoch close leaves its standalone snapshot
            // superseded by the successor bootstrap the other candidates provide.
            // Resolve retention before the lineage walk: the walk itself reads every
            // covered control's retained activation, so it must not descend into a
            // reclaimed control.
            let retained_control = snapshot.control.clone();
            let activation_commit = query_db
                .sqlite()
                .call(move |conn| {
                    StoreDatabase::retained_circle_activation_commit_ref_on(
                        conn,
                        circle_id,
                        &retained_control,
                    )
                })
                .await
                .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
            let Some(activation_commit) = activation_commit else {
                tracing::debug!(
                    %circle_id,
                    snapshot = %snapshot.snapshot_hash(),
                    "restore selection: standalone Circle snapshot control was reclaimed; \
                     superseded by the retained lineage"
                );
                continue;
            };
            // The head control must prove the snapshot control's lineage.
            let snapshot_control = snapshot.control.clone();
            let head = head_control.clone();
            let lineage_root = root.clone();
            let covered = query_db
                .sqlite()
                .call(move |conn| {
                    let Some(reference) = StoreDatabase::verified_circle_activation_on(
                        conn,
                        &lineage_root,
                        circle_id,
                        &head,
                    )?
                    else {
                        return Ok(false);
                    };
                    StoreDatabase::verified_circle_control_covers_on(
                        conn,
                        &lineage_root,
                        circle_id,
                        &reference.control,
                        &snapshot_control,
                    )
                })
                .await
                .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
            if covered {
                installable.push((snapshot, activation_commit));
            }
        }
    }
    let Some(selected) = select_maximal_circle_snapshot(
        installable
            .iter()
            .map(|(snapshot, _)| snapshot.clone())
            .collect(),
    ) else {
        return Ok(None);
    };
    let activation_commit = installable
        .into_iter()
        .find(|(snapshot, _)| snapshot.snapshot_hash() == selected.snapshot_hash())
        .map(|(_, activation_commit)| activation_commit)
        .expect("the selected standalone snapshot is one of the installable candidates");
    let author_device = selected.author_registration.device_id.to_string();
    let image_context = ProtocolObjectContext::circle(
        root.store_root_hash,
        ProtocolObjectDomain::CircleSnapshotImage,
        encryption.clone(),
    );
    let image_bytes = storage
        .read_protocol_object(
            &image_context,
            &selected.bootstrap.image.object,
            &circle_snapshot_image_semantic_prefix(
                circle_id,
                &author_device,
                selected.bootstrap.image.image_hash,
            ),
        )
        .await
        .map_err(SnapshotError::Bucket)?;
    verify_circle_bootstrap_image(
        &image_bytes,
        &selected.bootstrap,
        circle_id,
        query_db.sqlite().synced_tables(),
        routing_key,
    )
    .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    let image = crate::sync::store::circle_controls::VerifiedCircleImage::from_stored_image(
        circle_id,
        selected.control.clone(),
        selected.bootstrap.clone(),
        image_bytes,
    )
    .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
    Ok(Some(StagedCircleImageCandidate {
        activation_commit,
        image,
    }))
}

/// The candidate whose coverage dominates every other, provided the Store frontier
/// covers its cut. A candidate cut the frontier does not cover is a Circle image
/// newer than replayable history — a typed selection error, never a silent skip.
/// Two incomparable coverage cuts fail loud rather than pick one arbitrarily.
fn choose_maximal_installable_candidate(
    circle_id: CircleId,
    store_frontier: &CommitFrontier,
    mut candidates: Vec<StagedCircleImageCandidate>,
) -> Result<Option<StagedCircleImageCandidate>, SnapshotError> {
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by(|left, right| {
        left.image
            .reference()
            .image
            .image_hash
            .to_string()
            .cmp(&right.image.reference().image.image_hash.to_string())
    });
    let mut maximal_index = None;
    for (index, candidate) in candidates.iter().enumerate() {
        let dominates_all = candidates.iter().enumerate().all(|(other, contender)| {
            other == index
                || candidate.coverage() == contender.coverage()
                || coverage_dominates(candidate.coverage(), contender.coverage())
        });
        if dominates_all {
            maximal_index = Some(index);
            break;
        }
    }
    let maximal_index = maximal_index.ok_or_else(|| {
        SnapshotError::BootstrapState(format!(
            "restore selection: Circle {circle_id} has incomparable coverage candidates"
        ))
    })?;
    let maximal = candidates.swap_remove(maximal_index);
    if !store_frontier.covers(maximal.coverage()) {
        return Err(SnapshotError::BootstrapState(format!(
            "restore selection: Circle {circle_id} image cut is not covered by the Store frontier"
        )));
    }
    Ok(Some(maximal))
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

pub(crate) struct SelectedStableStoreSnapshot {
    pub(crate) snapshot: crate::database::PublishedStoreSnapshot,
    pub(crate) stability: crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
}

pub(crate) async fn select_maximal_stable_store_snapshot_with_history(
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    candidates: Vec<crate::database::PublishedStoreSnapshot>,
) -> Result<Option<SelectedStableStoreSnapshot>, crate::sync::store::owner::pull::StorePullError> {
    let Some(maximal_candidate) = select_maximal_store_snapshot(candidates.clone()) else {
        return Ok(None);
    };
    let maximal_reference = maximal_candidate.reference;
    let mut stable = Vec::new();
    let mut maximal_rejection = None;
    for snapshot in candidates {
        match history_verifier.verify_snapshot_stability(&snapshot).await {
            Ok(stability) => stable.push(SelectedStableStoreSnapshot {
                snapshot,
                stability,
            }),
            Err(error) => match &error {
                crate::sync::store::owner::pull::StorePullError::SnapshotNotStable { .. }
                | crate::sync::store::owner::pull::StorePullError::SnapshotAuthorInactive
                | crate::sync::store::owner::pull::StorePullError::SnapshotAuthorNotOwner => {
                    if snapshot.reference == maximal_reference {
                        maximal_rejection = Some(error);
                    }
                }
                _ => return Err(error),
            },
        }
    }
    let selected = select_maximal_store_snapshot(
        stable
            .iter()
            .map(|candidate| candidate.snapshot.clone())
            .collect(),
    );
    if let Some(selected) = selected {
        let index = stable
            .iter()
            .position(|candidate| candidate.snapshot.reference == selected.reference)
            .ok_or_else(|| {
                crate::sync::store::owner::pull::StorePullError::Database(
                    "stable Store snapshot selection lost its verified candidate".to_string(),
                )
            })?;
        return Ok(Some(stable.swap_remove(index)));
    }
    Err(maximal_rejection.ok_or_else(|| {
        crate::sync::store::owner::pull::StorePullError::Database(
            "Store snapshot candidates produced no stability decision".to_string(),
        )
    })?)
}

fn publication_error(error: crate::database::DbError) -> SnapshotError {
    SnapshotError::PublicationState(error.to_string())
}

struct SnapshotBootstrapAuthority<'storage> {
    history_verifier: crate::sync::store::owner::pull::MergeHistoryVerifier<'storage>,
}

struct VerifiedSnapshotBootstrap<'storage> {
    history_verifier: crate::sync::store::owner::pull::MergeHistoryVerifier<'storage>,
    snapshot: crate::database::PublishedStoreSnapshot,
    image: Vec<u8>,
    stability: crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
    membership: crate::sync::membership::MembershipChain,
}

impl<'storage> SnapshotBootstrapAuthority<'storage> {
    async fn open(
        storage: &'storage dyn SyncStorage,
        root: &StoreRootRef,
    ) -> Result<Self, SnapshotError> {
        let history_verifier =
            crate::sync::store::owner::pull::MergeHistoryVerifier::new(storage, root)
                .await
                .map_err(|error| SnapshotError::Parse(error.to_string()))?;
        Ok(Self { history_verifier })
    }

    async fn select(
        mut self,
        membership_floor: &crate::join_code::MembershipFloor,
        binary_schema_version: u32,
    ) -> Result<VerifiedSnapshotBootstrap<'storage>, SnapshotError> {
        let root = self.history_verifier.root().clone();
        let root_value = self.history_verifier.verified_root();
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
                .commit_verifier_ref()
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
                load_store_snapshot_stream(
                    self.history_verifier.storage(),
                    &root,
                    &registration_ref,
                    &registration,
                )
                .await?,
            );
        }
        let selected = Box::pin(select_maximal_stable_store_snapshot_with_history(
            &mut self.history_verifier,
            authorized,
        ))
        .await
        .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?
        .ok_or_else(|| {
            SnapshotError::Bucket(crate::sync::storage::StorageError::NotFound(
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
            .history_verifier
            .storage()
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
            history_verifier: self.history_verifier,
            snapshot: chosen,
            image,
            stability: selected.stability,
            membership,
        })
    }
}

impl<'storage> VerifiedSnapshotBootstrap<'storage> {
    async fn founder_registration(
        &self,
    ) -> Result<
        crate::sync::store_objects::VerifiedObject<
            crate::sync::store_commit::StoreDeviceRegistration,
        >,
        SnapshotError,
    > {
        self.history_verifier
            .commit_verifier_ref()
            .load_founder_registration()
            .await
            .map_err(|error| SnapshotError::Parse(error.to_string()))
    }

    fn into_parts(
        self,
    ) -> (
        crate::sync::store::owner::pull::MergeHistoryVerifier<'storage>,
        crate::database::PublishedStoreSnapshot,
        Vec<u8>,
        crate::sync::store::owner::pull::VerifiedStoreSnapshotStability,
        crate::sync::membership::MembershipChain,
    ) {
        (
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
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::store::database::StoreDatabase;

    fn open(path: &Path, device_id: &str) -> Database {
        Database::open(
            path,
            crate::sync::test_helpers::test_synced_tables(),
            crate::blob::BLOB_TOMBSTONE_GRACE,
            crate::blob::TransferLimits::one_at_a_time(),
            device_id.to_string(),
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
        let origin = crate::sync::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: initialized
                .store
                .protocol_root_for_test()
                .descriptor
                .creation_id,
        };
        let device_id = crate::sync::store_commit::StoreDeviceId::derive(&root_ref, &origin);
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

        let selected = SnapshotBootstrapAuthority::open(&store.storage, &store.root)
            .await
            .expect("open exact snapshot bootstrap authority")
            .select(
                &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
                1,
            )
            .await
            .expect("select verified exact snapshot");
        let (_, selected, image, _, _) = selected.into_parts();

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
        let published = drain_outbound_store_snapshot(&storage, &StoreDatabase::new(&reopened))
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
        assert!(StoreDatabase::new(&db)
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
            StoreDatabase::new(&db)
                .export_activated_device_continuation(&signer)
                .await
                .expect("export continuation after snapshot")
                .latest_snapshot,
            Some(published.reference.clone()),
        );
        let store = crate::sync::store::Store::load(
            StoreDatabase::new(&db),
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

        let completed = drain_outbound_store_snapshot(&storage, &StoreDatabase::new(&db))
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

        assert!(
            drain_outbound_store_snapshot(&storage, &StoreDatabase::new(&db))
                .await
                .is_err()
        );
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
        let image_object_id = crate::sync::remote_object::remote_object_id(&first.image.object);
        let image_ownership = db
            .call(move |connection| {
                connection
                    .query_row(
                        "SELECT state FROM remote_objects WHERE object_id = ?1",
                        [image_object_id.to_string()],
                        |row| row.get::<_, String>(0),
                    )
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("load published snapshot image ownership");
        let image_ownership: crate::sync::remote_object::RemoteObjectRecord =
            serde_json::from_str(&image_ownership).expect("parse snapshot image ownership");
        assert!(matches!(
            image_ownership,
            crate::sync::remote_object::RemoteObjectRecord::SharedLiveSet(record)
                if matches!(
                    &record.identity.domain,
                    crate::sync::remote_object::SharedLiveSetObjectDomain::StoreSnapshotImage {
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
        drain_outbound_store_snapshot(&storage, &StoreDatabase::new(&db))
            .await
            .expect("resume second snapshot publication")
            .expect("publish staged second snapshot");
        let published_generations = db
            .call(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM published_store_snapshot", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(crate::database::DbError::from)
            })
            .await
            .expect("count published Store snapshot generations");
        assert_eq!(published_generations, 2);
    }

    #[tokio::test]
    async fn circle_snapshot_authors_and_installs_as_a_bootstrap_image() {
        let directory = tempfile::tempdir().expect("snapshot database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "circle-snapshot-device");
        let (root_ref, _) = initialize(&db, &storage, &signer).await;
        db.call(|conn| {
            crate::db::apply_coven_routing_schema(conn).map_err(crate::database::DbError::from)
        })
        .await
        .expect("apply routing schema");
        let (circle_id, control) = db
            .call(|conn| {
                Ok(crate::sync::test_helpers::install_test_active_circle(
                    conn, "snap",
                ))
            })
            .await
            .expect("install active Circle");
        let (encryption, key_fingerprint) = store_database(&db)
            .circle_publication_context(circle_id, control.clone())
            .await
            .expect("resolve Circle publication context");

        crate::sync::store::push_circle_snapshots_for_test(
            &db,
            &storage,
            directory.path().join("snap-temp"),
            db.schema_version(),
            &signer,
            "2026-07-16T00:00:00Z",
            &crate::encryption::EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("author Circle snapshots");

        let stream = crate::sync::store::load_circle_snapshot_metas_for_test(
            &db,
            &storage,
            circle_id,
            encryption.clone(),
            &signer,
        )
        .await
        .expect("load Circle snapshot stream");
        assert_eq!(stream.len(), 1);
        let selected = select_maximal_circle_snapshot(stream).expect("a maximal Circle snapshot");
        assert_eq!(selected.generation, 0);
        assert_eq!(selected.circle_id, circle_id);
        assert_eq!(selected.control, control);
        assert_eq!(selected.key_fingerprint, key_fingerprint);

        // The snapshot image installs with the member-addition bootstrap
        // machinery: identical image format, verified exact.
        let image_context = ProtocolObjectContext::circle(
            root_ref.store_root_hash,
            ProtocolObjectDomain::CircleSnapshotImage,
            encryption.clone(),
        );
        let image = storage
            .read_protocol_object(
                &image_context,
                &selected.bootstrap.image.object,
                &circle_snapshot_image_semantic_prefix(
                    circle_id,
                    &selected.author_registration.device_id.to_string(),
                    selected.bootstrap.image.image_hash,
                ),
            )
            .await
            .expect("read Circle snapshot image");
        let routing_key =
            crate::sync::circle::derive_row_routing_key(&encryption, root_ref.store_root_hash)
                .expect("derive Circle row routing key");
        verify_circle_bootstrap_image(
            &image,
            &selected.bootstrap,
            circle_id,
            &crate::sync::test_helpers::test_synced_tables(),
            Some(&routing_key),
        )
        .expect("Circle snapshot is installable as a bootstrap image");
    }

    #[tokio::test]
    async fn non_member_cannot_decrypt_circle_snapshot() {
        let directory = tempfile::tempdir().expect("snapshot database directory");
        let path = directory.path().join("store.sqlite3");
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(&path, "circle-snapshot-outsider");
        let (root_ref, device_id) = initialize(&db, &storage, &signer).await;
        db.call(|conn| {
            crate::db::apply_coven_routing_schema(conn).map_err(crate::database::DbError::from)
        })
        .await
        .expect("apply routing schema");
        let (circle_id, _control) = db
            .call(|conn| {
                Ok(crate::sync::test_helpers::install_test_active_circle(
                    conn, "snap",
                ))
            })
            .await
            .expect("install active Circle");
        crate::sync::store::push_circle_snapshots_for_test(
            &db,
            &storage,
            directory.path().join("snap-temp"),
            db.schema_version(),
            &signer,
            "2026-07-16T00:00:00Z",
            &crate::encryption::EncryptionService::from_key([42; 32]),
        )
        .await
        .expect("author Circle snapshots");

        // A Store member outside the Circle resolves an unrelated key and cannot
        // open the Circle-sealed snapshot metadata.
        let outsider = crate::encryption::EncryptionService::from_key([7u8; 32]);
        let meta_context = ProtocolObjectContext::circle(
            root_ref.store_root_hash,
            ProtocolObjectDomain::CircleSnapshotMeta,
            outsider,
        );
        let prefix =
            crate::sync::store_commit::circle_snapshot_slot_prefix(circle_id, &device_id, 0);
        let slot = crate::storage::cloud::ObjectSlot::logical(format!("{prefix}.json"))
            .expect("gen-0 slot");
        let denied = storage
            .read_protocol_slot(&meta_context, &slot, &prefix)
            .await;
        assert!(denied.is_err());
    }
}
