//! Durable exact Store snapshot publication.

mod image;

pub use image::{
    bootstrap_from_snapshot, create_snapshot, reconcile_snapshot_blobs, BootstrapResult,
    SnapshotBlobReconcile, SnapshotError,
};
pub(crate) use image::{
    create_circle_snapshot_with_host_blobs, create_snapshot_with_host_blobs,
    install_snapshot_blob_graph, open_database_image, should_create_snapshot,
    verify_circle_bootstrap_image, CreatedSnapshot, SnapshotBlobAudience,
};

use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::sync::circle::{CircleBootstrapRef, CircleControlCoord, CircleEpochId, CircleId};
use crate::sync::membership::MembershipChain;
use crate::sync::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::sync::store_commit::{
    circle_snapshot_image_semantic_prefix, circle_snapshot_slot_prefix,
    snapshot_image_semantic_prefix, snapshot_slot_prefix, CircleSnapshotMeta,
    CircleSnapshotSuccessorLink, CommitFrontier, DeviceStreamAnchor, ObjectHash, SnapshotImageRef,
    SnapshotMeta, SnapshotSuccessorLink, StoreHistoryCut, StoreProtocolError, StoreRootRef,
    StoreSnapshotRef, StoreSnapshotState, StreamActivation,
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

impl super::AuthorizedStore<'_> {
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
                let replay = super::pull::replay_retained_merge_projection_on(
                    &transaction,
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
        self.db()
            .call(move |connection| {
                require_no_unpublished_store_writes(connection)?;
                let snapshot = match projection {
                    SnapshotCutProjection::Store { routing_encryption } => {
                        create_snapshot_with_host_blobs(
                            connection,
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn push_store_snapshot(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
    schema_version: u32,
    keypair: &UserKeypair,
    created_at: String,
    membership: &MembershipChain,
    database: &crate::sync::store::StoreDatabase,
) -> Result<SnapshotMeta, SnapshotError> {
    let db = database.sqlite();
    let _publication = database.lock_snapshot_publication().await;
    drain_snapshot_spool_cleanup(database).await?;
    if let Some(pending) = database
        .outbound_snapshot_publication()
        .await
        .map_err(publication_error)?
    {
        return publish_durable_snapshot(storage, database, pending).await;
    }
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(publication_error)?
        .ok_or_else(|| {
            SnapshotError::PublicationState("local Store device registration is absent".to_string())
        })?;
    let (root, registration_ref, registration, device_signer) =
        crate::sync::store::operations::load_local_store_authority(database, &device_id, keypair)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
    if root.store_root_hash != store_root_hash {
        return Err(SnapshotError::PublicationState(
            "snapshot Store root differs from the activated local root".to_string(),
        ));
    }
    let author = registration.author_pubkey.clone();
    if !membership.is_owner_now(&author) {
        return Err(SnapshotError::UnauthorizedAuthor(author));
    }
    let history_cut = StoreHistoryCut(coverage.0.clone());
    let (devices, resolved_devices) = database
        .store_device_state_for_history_cut(&history_cut)
        .await
        .map_err(publication_error)?;
    let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
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
    let history_summary = super::pull::prepare_merge_snapshot_history_summary(
        database,
        &root,
        &coverage,
        membership,
        &resolved_devices,
        &registration_ref,
        &registration,
    )
    .await
    .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
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
    let (image_bytes, snapshot_blobs) = prepare_snapshot_blobs(
        database,
        storage,
        snapshot,
        snapshot_owner,
        &registration_ref,
        &registration,
    )
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
            SnapshotError::PublicationState("staged snapshot publication row is absent".to_string())
        })?;
    publish_durable_snapshot(storage, database, pending).await
}

#[allow(clippy::too_many_arguments)]
async fn prepare_snapshot_blobs(
    database: &crate::sync::store::StoreDatabase,
    storage: &dyn SyncStorage,
    snapshot: CreatedSnapshot,
    owner: crate::sync::remote_object::SnapshotObjectOwner,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
) -> Result<(Vec<u8>, Vec<crate::database::PreparedSnapshotBlob>), SnapshotError> {
    let authority = crate::sync::storage::BlobWriteAuthority::new(registration_ref, registration)
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
            let locator = super::package_preparation::prepare_partition_blob_locator(
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
        let (binding, blob) = super::package_preparation::prepare_partition_blob(
            database,
            storage,
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

impl super::AuthorizedStore<'_> {
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
        keypair: &UserKeypair,
        created_at: &str,
    ) -> Result<(), SnapshotError> {
        let inputs = self
            .database()
            .circle_acknowledgement_publication_inputs()
            .await
            .map_err(publication_error)?;
        for input in inputs {
            let circle_temp = temp_dir.join(format!("circle-snapshot-{}", input.circle_id));
            std::fs::create_dir_all(&circle_temp).map_err(SnapshotError::Io)?;
            let cut = match self
                .capture_circle_snapshot_cut(circle_temp, &input.encryption, input.circle_id)
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
            match push_circle_snapshot(
                self.storage(),
                self.database(),
                input.circle_id,
                input.control,
                input.epoch_id,
                input.key_fingerprint,
                input.encryption,
                cut.snapshot,
                cut.coverage,
                schema_version,
                keypair,
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
}

#[cfg(test)]
impl super::AuthorizedStore<'_> {
    pub(crate) async fn author_one_circle_snapshot_for_test(
        &self,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        keypair: &UserKeypair,
        created_at: &str,
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
            .capture_circle_snapshot_cut(temp_dir, &input.encryption, input.circle_id)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
        push_circle_snapshot(
            self.storage(),
            self.database(),
            input.circle_id,
            input.control,
            input.epoch_id,
            input.key_fingerprint,
            input.encryption,
            cut.snapshot,
            cut.coverage,
            schema_version,
            keypair,
            created_at.to_string(),
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn push_circle_snapshot(
    storage: &dyn SyncStorage,
    database: &crate::sync::store::StoreDatabase,
    circle_id: CircleId,
    control: CircleControlCoord,
    epoch_id: CircleEpochId,
    key_fingerprint: KeyFingerprint,
    encryption: EncryptionService,
    snapshot: CreatedSnapshot,
    coverage: CommitFrontier,
    schema_version: u32,
    keypair: &UserKeypair,
    created_at: String,
) -> Result<CircleSnapshotMeta, SnapshotError> {
    let db = database.sqlite();
    let _publication = database.lock_snapshot_publication().await;
    drain_snapshot_spool_cleanup(database).await?;
    if let Some(pending) = database
        .outbound_circle_snapshot_publication(circle_id)
        .await
        .map_err(publication_error)?
    {
        return publish_durable_circle_snapshot(storage, database, pending).await;
    }
    let device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(publication_error)?
        .ok_or_else(|| {
            SnapshotError::PublicationState("local Store device registration is absent".to_string())
        })?;
    let (root, registration_ref, _registration, device_signer) =
        crate::sync::store::operations::load_local_store_authority(database, &device_id, keypair)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?;
    // The image references only already-published Circle blobs, verified exact —
    // the same closure a member-addition bootstrap image carries.
    let blobs = crate::sync::store::circle_controls::verified_circle_bootstrap_blobs(
        storage, circle_id, &snapshot,
    )
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
        None => (0, None, stream_first_slot.clone()),
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
    let activation = StreamActivation::device_authorized(
        root.store_root_hash,
        registration_ref.clone(),
        DeviceStreamAnchor::CircleSnapshots {
            circle_id,
            first_slot: stream_first_slot,
        },
    )
    .activation_id();
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

pub async fn load_store_snapshot_ref(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
    reference: &StoreSnapshotRef,
) -> Result<(StoreSnapshotRef, SnapshotMeta), SnapshotError> {
    if registration_ref.device_id != registration.device_id {
        return Err(SnapshotError::Parse(
            "Store snapshot registration reference names another device".to_string(),
        ));
    }
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotMeta,
    );
    let prefix = snapshot_slot_prefix(&registration.device_id.to_string(), reference.generation);
    let bytes = storage
        .read_protocol_object(&context, &reference.object, &prefix)
        .await
        .map_err(SnapshotError::Bucket)?;
    let verify_root = root.clone();
    let verify_registration_ref = registration_ref.clone();
    let verify_registration = registration.clone();
    let verify_reference = reference.clone();
    let meta = crate::sync::store_objects::run_blocking_object_verification(
        &prefix,
        &reference.object,
        Box::new(move || {
            verify_store_snapshot_bytes(
                &verify_root,
                &verify_registration_ref,
                &verify_registration,
                &verify_reference,
                &bytes,
            )
            .map_err(|error| StoreProtocolError::Malformed(error.to_string()))
        }),
    )
    .await
    .map_err(SnapshotError::StoreObject)?;
    Ok((reference.clone(), meta))
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
#[cfg(test)]
pub(crate) async fn load_circle_snapshot_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    circle_id: CircleId,
    encryption: EncryptionService,
    registration_ref: &crate::sync::store_commit::StoreDeviceRegistrationRef,
    registration: &crate::sync::store_commit::StoreDeviceRegistration,
) -> Result<Vec<CircleSnapshotMeta>, SnapshotError> {
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
        predecessor = Some(reference);
        snapshots.push(meta);
        generation = generation.checked_add(1).ok_or_else(|| {
            SnapshotError::Parse("Circle snapshot generation overflow".to_string())
        })?;
    }
    Ok(snapshots)
}

/// The maximal Circle snapshot by coverage domination among candidates. Stability
/// against Circle acknowledgements is applied by the caller; this returns the
/// snapshot whose cut no other candidate strictly dominates.
#[cfg(test)]
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
    pub(crate) stability: crate::sync::store::pull::VerifiedStoreSnapshotStability,
}

pub(crate) async fn select_maximal_stable_store_snapshot(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    candidates: Vec<crate::database::PublishedStoreSnapshot>,
) -> Result<Option<SelectedStableStoreSnapshot>, crate::sync::store::pull::StorePullError> {
    let Some(maximal_candidate) = select_maximal_store_snapshot(candidates.clone()) else {
        return Ok(None);
    };
    let maximal_reference = maximal_candidate.reference;
    let mut stable = Vec::new();
    let mut maximal_rejection = None;
    for snapshot in candidates {
        match super::verify_store_snapshot_stability(storage, root, &snapshot).await {
            Ok(stability) => stable.push(SelectedStableStoreSnapshot {
                snapshot,
                stability,
            }),
            Err(error) => match &error {
                crate::sync::store::pull::StorePullError::SnapshotNotStable { .. }
                | crate::sync::store::pull::StorePullError::SnapshotAuthorInactive
                | crate::sync::store::pull::StorePullError::SnapshotAuthorNotOwner => {
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
                crate::sync::store::pull::StorePullError::Database(
                    "stable Store snapshot selection lost its verified candidate".to_string(),
                )
            })?;
        return Ok(Some(stable.swap_remove(index)));
    }
    Err(maximal_rejection.ok_or_else(|| {
        crate::sync::store::pull::StorePullError::Database(
            "Store snapshot candidates produced no stability decision".to_string(),
        )
    })?)
}

fn publication_error(error: crate::database::DbError) -> SnapshotError {
    SnapshotError::PublicationState(error.to_string())
}

pub(crate) async fn select_store_snapshot(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    membership_floor: &crate::join_code::MembershipFloor,
    binary_schema_version: u32,
) -> Result<
    (
        crate::sync::store_objects::VerifiedObject<crate::sync::store_commit::StoreProtocolRoot>,
        crate::database::PublishedStoreSnapshot,
        Vec<u8>,
        crate::sync::store::pull::VerifiedStoreSnapshotStability,
    ),
    SnapshotError,
> {
    let verified_root = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(|error| SnapshotError::Parse(error.to_string()))?;
    let root_value = &verified_root.value;
    if root_value.descriptor.store_root_id() != root.store_root_id {
        return Err(SnapshotError::UnauthorizedAuthor(
            "Store root differs from bootstrap authority".to_string(),
        ));
    }
    let heads = &membership_floor.0;
    let mut registrations = std::collections::BTreeMap::new();
    let mut resolutions = std::collections::BTreeSet::new();
    for reference in heads {
        let head =
            crate::sync::store::membership::load_exact_membership_head(storage, root, reference)
                .await
                .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
        resolutions.extend(head.body.resolutions.iter().cloned());
        let registration = crate::sync::store_objects::load_registration_ref(
            storage,
            root,
            &head.body.author_registration,
        )
        .await
        .map_err(|error| SnapshotError::Parse(error.to_string()))?
        .value;
        registrations.insert(head.body.author_registration.clone(), registration);
    }
    let resolutions = resolutions.into_iter().collect::<Vec<_>>();
    let membership = crate::sync::store::membership::load_anchored_chain_at_exact_heads(
        storage,
        root,
        &root_value.descriptor.founder_pubkey,
        heads,
        &resolutions,
    )
    .await
    .map_err(|error| SnapshotError::UnauthorizedAuthor(error.to_string()))?;
    let registrations = registrations
        .into_iter()
        .filter(|(_, registration)| membership.is_owner_now(&registration.author_pubkey))
        .collect::<Vec<_>>();
    let mut authorized = Vec::new();
    for (registration_ref, registration) in registrations {
        authorized.extend(
            load_store_snapshot_stream(storage, root, &registration_ref, &registration).await?,
        );
    }
    let selected = Box::pin(select_maximal_stable_store_snapshot(
        storage, root, authorized,
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
    let stability = selected.stability;
    let image_context = ProtocolObjectContext::store_encrypted(
        root.store_root_hash,
        ProtocolObjectDomain::StoreSnapshotImage,
    );
    let image = storage
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
    Ok((verified_root, chosen, image, stability))
}

fn coverage_dominates(left: &CommitFrontier, right: &CommitFrontier) -> bool {
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

    fn storage(home: &InMemoryCloudHome, signer: &UserKeypair) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "snapshot-exact-store",
            signer.clone(),
        )
        .expect("construct snapshot test storage")
    }

    async fn initialize(
        db: &Database,
        storage: &CloudSyncStorage,
        signer: &UserKeypair,
    ) -> (ObjectHash, String, MembershipChain) {
        let root = crate::sync::store::protocol_root::create_store(
            &store_database(db),
            storage,
            "snapshot-exact-store",
            signer,
        )
        .await
        .expect("create snapshot test Store");
        crate::sync::store::ensure_active_registration(&StoreDatabase::new(db), storage)
            .await
            .expect("activate snapshot test registration");
        let root_ref = store_database(db)
            .local_store_root_ref()
            .await
            .expect("read snapshot test Store root")
            .expect("snapshot test Store root exists");
        let origin = crate::sync::store_commit::StoreDeviceRegistrationOrigin::Founder {
            creation_id: root.descriptor.creation_id,
        };
        let device_id = crate::sync::store_commit::StoreDeviceId::derive(&root_ref, &origin);
        let membership =
            crate::sync::store::pull::load_cycle_membership(storage, &store_database(db))
                .await
                .expect("load snapshot test membership")
                .chain
                .expect("snapshot test membership exists");
        (root.object_hash(), device_id.to_string(), membership)
    }

    fn snapshot(bytes: &[u8]) -> CreatedSnapshot {
        CreatedSnapshot {
            db_image: bytes.to_vec(),
            blobs: Vec::new(),
        }
    }

    async fn publish(
        storage: &CloudSyncStorage,
        root: ObjectHash,
        db: &Database,
        signer: &UserKeypair,
        membership: &MembershipChain,
        bytes: &[u8],
        created_at: &str,
    ) -> Result<SnapshotMeta, SnapshotError> {
        push_store_snapshot(
            storage,
            root,
            snapshot(bytes),
            CommitFrontier(BTreeMap::new()),
            1,
            signer,
            created_at.to_string(),
            membership,
            &StoreDatabase::new(db),
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
        let membership = store
            .open_into(&db)
            .await
            .expect("open exact snapshot selector Store");
        let published = push_store_snapshot(
            &store.storage,
            store.root.store_root_hash,
            snapshot(b"snapshot selector image"),
            CommitFrontier(BTreeMap::new()),
            1,
            &signer,
            "2026-07-16T00:00:00Z".to_string(),
            &membership,
            &StoreDatabase::new(&db),
        )
        .await
        .expect("publish exact snapshot selector fixture");
        crate::sync::store::stage_store_acknowledgement_for_test(
            &db,
            &store.storage,
            CommitFrontier(BTreeMap::new()),
            "2026-07-16T00:00:01Z".to_string(),
            &signer,
        )
        .await
        .expect("stage exact snapshot selector acknowledgement");
        crate::sync::store::drain_store_acknowledgements_for_test(&db, &store.storage, &signer)
            .await
            .expect("activate exact snapshot selector acknowledgement");

        let (_, selected, image, _) = select_store_snapshot(
            &store.storage,
            &store.root,
            &crate::join_code::MembershipFloor(membership.head_refs().to_vec()),
            1,
        )
        .await
        .expect("select verified exact snapshot");

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
        let (root, _, membership) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(1);
        assert!(publish(
            &storage,
            root,
            &db,
            &signer,
            &membership,
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
        let (root_hash, device_id, membership) = initialize(&db, &storage, &signer).await;
        assert!(StoreDatabase::new(&db)
            .export_activated_device_continuation(&signer)
            .await
            .expect("export continuation before any snapshot")
            .latest_snapshot
            .is_none());
        publish(
            &storage,
            root_hash,
            &db,
            &signer,
            &membership,
            b"continued snapshot",
            "2026-07-16T00:00:00Z",
        )
        .await
        .expect("publish continued snapshot");
        let root = store_database(&db)
            .local_store_root_ref()
            .await
            .expect("load continued Store root")
            .expect("continued Store root exists");
        let (_, registration_ref, registration, _) =
            crate::sync::store::operations::load_local_store_authority(
                &StoreDatabase::new(&db),
                &device_id,
                &signer,
            )
            .await
            .expect("load continued snapshot authority");
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
        load_store_snapshot_ref(
            &storage,
            &root,
            &registration_ref,
            &registration,
            &published.reference,
        )
        .await
        .expect("load exact continued snapshot");

        let mut wrong_reference = published.reference.clone();
        wrong_reference.generation += 1;
        assert!(load_store_snapshot_ref(
            &storage,
            &root,
            &registration_ref,
            &registration,
            &wrong_reference,
        )
        .await
        .is_err());

        let mut wrong_hash = published.reference.clone();
        wrong_hash.snapshot_hash = ObjectHash::digest(b"another snapshot");
        assert!(load_store_snapshot_ref(
            &storage,
            &root,
            &registration_ref,
            &registration,
            &wrong_hash,
        )
        .await
        .is_err());

        let mut wrong_author = published.meta.clone();
        wrong_author.author_registration.registration_hash = ObjectHash::digest(b"another author");
        assert!(verify_store_snapshot_bytes(
            &root,
            &registration_ref,
            &registration,
            &published.reference,
            &wrong_author.to_bytes(),
        )
        .is_err());

        let mut wrong_successor = published.meta;
        wrong_successor.successor.next_slot =
            crate::storage::cloud::ObjectSlot::logical("wrong-successor.json".to_string())
                .expect("valid wrong successor slot");
        assert!(verify_store_snapshot_bytes(
            &root,
            &registration_ref,
            &registration,
            &published.reference,
            &wrong_successor.to_bytes(),
        )
        .is_err());
    }

    #[tokio::test]
    async fn lost_snapshot_image_create_response_is_resolved_before_metadata_creation() {
        let home = InMemoryCloudHome::new();
        let signer = UserKeypair::generate();
        let storage = storage(&home, &signer);
        let db = open(Path::new(":memory:"), "snapshot-test-device");
        let (root, _, membership) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_after_call(1);

        let published = publish(
            &storage,
            root,
            &db,
            &signer,
            &membership,
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
        let (root, _, membership) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(2);

        assert!(publish(
            &storage,
            root,
            &db,
            &signer,
            &membership,
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
        let (root, _, membership) = initialize(&db, &storage, &signer).await;
        home.fail_exact_create_before_call(1);
        assert!(publish(
            &storage,
            root,
            &db,
            &signer,
            &membership,
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
        let (root, _, membership) = initialize(&db, &storage, &signer).await;
        let first = publish(
            &storage,
            root,
            &db,
            &signer,
            &membership,
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
            root,
            &db,
            &signer,
            &membership,
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
        initialize(&db, &storage, &signer).await;
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
        )
        .await
        .expect("author Circle snapshots");

        let device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .unwrap()
            .expect("local device id");
        let (root_ref, registration_ref, registration, _) =
            crate::sync::store::operations::load_local_store_authority(
                &store_database(&db),
                &device_id,
                &signer,
            )
            .await
            .expect("load local Store authority");

        let stream = load_circle_snapshot_stream(
            &storage,
            &root_ref,
            circle_id,
            encryption.clone(),
            &registration_ref,
            &registration,
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
                    &registration.device_id.to_string(),
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
}
