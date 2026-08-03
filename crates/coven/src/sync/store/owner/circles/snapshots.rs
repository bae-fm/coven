use crate::encryption::EncryptionService;
use crate::keys::UserKeypair;
use crate::protocol::circle::{CircleBootstrapRef, CircleControlCoord, CircleEpochId, CircleId};
use crate::protocol::store_commit::{
    circle_snapshot_image_semantic_prefix, circle_snapshot_slot_prefix, CircleSnapshotMeta,
    CircleSnapshotSuccessorLink, CommitFrontier, ObjectHash, SnapshotImageRef, StoreRootRef,
};
use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain, SyncStorage};
use crate::KeyFingerprint;
use tracing::warn;

use crate::database::{verify_circle_bootstrap_image, CreatedSnapshot};
use crate::sync::store::owner::snapshot::{
    coverage_dominates, publication_error, SnapshotCut, SnapshotError,
};

pub(crate) struct CircleSnapshotWriter<'operation, 'storage> {
    writer: &'operation mut super::AuthorizedWriterOperation<'storage>,
    database: crate::database::StoreDatabase,
    storage: std::sync::Arc<dyn SyncStorage>,
    root: StoreRootRef,
    registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    registration: crate::protocol::store_commit::StoreDeviceRegistration,
    device_signer: UserKeypair,
}

pub(crate) struct CircleSnapshotReader<'operation, 'storage> {
    database: &'operation crate::database::StoreDatabase,
    storage: &'storage dyn SyncStorage,
    history:
        &'operation mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> CircleSnapshotReader<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation crate::database::StoreDatabase,
        storage: &'storage dyn SyncStorage,
        history: &'operation mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<
            'storage,
        >,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    fn root(&self) -> &StoreRootRef {
        self.history.verified_root().reference()
    }
}

impl<'operation, 'storage> CircleSnapshotWriter<'operation, 'storage> {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        writer: &'operation mut super::AuthorizedWriterOperation<'storage>,
        database: crate::database::StoreDatabase,
        storage: std::sync::Arc<dyn SyncStorage>,
        root: StoreRootRef,
        registration_ref: crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: crate::protocol::store_commit::StoreDeviceRegistration,
        device_signer: UserKeypair,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            registration_ref,
            registration,
            device_signer,
        }
    }

    pub(crate) async fn capture_circle_snapshot_cut(
        &self,
        temp_dir: std::path::PathBuf,
        routing_encryption: &crate::encryption::EncryptionService,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        let (snapshot, coverage) = self
            .database
            .capture_circle_snapshot_cut(
                self.root.clone(),
                temp_dir,
                self.database.synced_tables().to_vec(),
                routing_encryption.clone(),
                circle_id,
            )
            .await?;
        Ok(SnapshotCut { snapshot, coverage })
    }

    pub(crate) async fn capture_circle_snapshot_at_cutoff(
        &self,
        temp_dir: std::path::PathBuf,
        routing_encryption: &crate::encryption::EncryptionService,
        circle_id: crate::protocol::circle::CircleId,
        cutoff: CommitFrontier,
    ) -> Result<SnapshotCut, crate::database::DbError> {
        let tables = self.database.synced_tables().to_vec();
        let routing_encryption = routing_encryption.clone();
        let routing_key = crate::protocol::circle::derive_row_routing_key(
            &routing_encryption,
            self.root.store_root_hash,
        )
        .map_err(|error| crate::database::DbError::Message(error.to_string()))?;
        let root = self.root.clone();
        let snapshot = self
            .database
            .capture_circle_snapshot_at_cutoff(
                root,
                temp_dir,
                tables,
                routing_encryption,
                routing_key,
                circle_id,
                cutoff.clone(),
            )
            .await?;
        Ok(SnapshotCut {
            snapshot,
            coverage: cutoff,
        })
    }

    /// Author one Circle snapshot for every Circle this device holds active
    /// package access to, under the same owner/cadence gate the caller already
    /// applied for the Store snapshot. A Circle without active access is not
    /// enumerated (an inactive recipient holds no epoch key) and so is skipped
    /// by construction; a capture or publication failure for one Circle is logged
    /// and does not abort the others or the cycle.
    pub(crate) async fn push_circle_snapshots(
        &mut self,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: Option<&crate::encryption::EncryptionService>,
    ) -> Result<(), SnapshotError> {
        let inputs = self
            .database
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
                .database
                .outbound_circle_snapshot_publication(input.circle_id)
                .await
                .map_err(publication_error)?
            {
                let publication = self.writer.snapshot_publication().await;
                match publication.resume_circle(pending).await {
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
                .database
                .latest_local_circle_snapshot(input.circle_id)
                .await
                .map_err(publication_error)?
            {
                if !self
                    .circle_snapshot_is_stable(input.circle_id, &previous.cut)
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
    /// invisible. Each acknowledgement reference names the exact control that
    /// resolves its encryption key, so an acknowledgement sealed under a rotated
    /// epoch stays readable without probing unrelated controls. Mirrors the
    /// Store-level snapshot stability verification.
    pub(crate) async fn circle_snapshot_is_stable(
        &mut self,
        circle_id: CircleId,
        snapshot_cut: &CommitFrontier,
    ) -> Result<bool, SnapshotError> {
        Ok(self
            .writer
            .circle_history()
            .acknowledgements()
            .stable_dominating(circle_id, snapshot_cut)
            .await
            .map_err(|error| SnapshotError::PublicationState(error.to_string()))?
            .is_some())
    }

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
        let storage = self.storage.as_ref();
        let database = &self.database;
        let db = &database;
        let device_id = self.registration.device_id.to_string();
        let root = &self.root;
        let registration_ref = self.registration_ref.clone();
        let device_signer = self.device_signer.clone();
        let bootstrap_verifier = self.writer.circle_bootstrap_verifier();
        let publication = self.writer.snapshot_publication().await;
        publication.drain_spool_cleanup().await?;
        // The image references only already-published Circle blobs, verified exact —
        // the same closure a member-addition bootstrap image carries.
        let blobs = bootstrap_verifier
            .verify_snapshot_blobs(circle_id, &snapshot)
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
        let activation = crate::protocol::store_commit::circle_snapshot_stream_activation(
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
        publication.publish_circle(pending).await
    }
}

#[cfg(test)]
impl CircleSnapshotWriter<'_, '_> {
    pub(crate) async fn load_circle_snapshot_refs_for_test(
        &mut self,
        circle_id: CircleId,
        encryption: EncryptionService,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::CircleSnapshotRef,
            crate::protocol::store_commit::CircleSnapshotMeta,
        )>,
        SnapshotError,
    > {
        let registration_ref = self.registration_ref.clone();
        let registration = self.registration.clone();
        self.writer
            .circle_history()
            .snapshots()
            .load_stream_refs(circle_id, encryption, &registration_ref, &registration)
            .await
    }

    pub(crate) async fn load_circle_snapshot_metas_for_test(
        &mut self,
        circle_id: CircleId,
        encryption: EncryptionService,
    ) -> Result<Vec<CircleSnapshotMeta>, SnapshotError> {
        let registration_ref = self.registration_ref.clone();
        let registration = self.registration.clone();
        self.writer
            .circle_history()
            .snapshots()
            .load_stream(circle_id, encryption, &registration_ref, &registration)
            .await
    }

    pub(crate) async fn verify_standalone_circle_snapshot_image_for_test(
        &mut self,
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
            self.root.store_root_hash,
            ProtocolObjectDomain::CircleSnapshotImage,
            epoch_encryption,
        );
        let image = self
            .storage
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
        let routing_key = crate::protocol::circle::derive_row_routing_key(
            store_routing,
            self.root.store_root_hash,
        )
        .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
        verify_circle_bootstrap_image(
            &image,
            &selected.bootstrap,
            circle_id,
            self.database.synced_tables(),
            Some(&routing_key),
        )?;
        Ok(())
    }

    pub(crate) async fn author_one_circle_snapshot_for_test(
        &self,
        temp_dir: std::path::PathBuf,
        schema_version: u32,
        created_at: &str,
        store_routing: &crate::encryption::EncryptionService,
    ) -> Result<CircleSnapshotMeta, SnapshotError> {
        let input = self
            .database
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

/// Load one device's Circle snapshot stream, decrypting each metadata object
/// with the Circle epoch key. Generation zero sits at a deterministic slot the
/// reader computes from the author's device id (a per-Circle stream has no
/// registration anchor); later generations follow the create-once successor
/// slot. The predecessor chain is verified exact.
impl CircleSnapshotReader<'_, '_> {
    async fn load_stream(
        &self,
        circle_id: CircleId,
        encryption: EncryptionService,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<Vec<CircleSnapshotMeta>, SnapshotError> {
        Ok(self
            .load_stream_refs(circle_id, encryption, registration_ref, registration)
            .await?
            .into_iter()
            .map(|(_, meta)| meta)
            .collect())
    }

    /// Each Circle snapshot in the per-(device, Circle) stream paired with its exact
    /// reference — the reference a reclaim locator or restore selection binds.
    pub(crate) async fn load_stream_refs(
        &self,
        circle_id: CircleId,
        encryption: EncryptionService,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::CircleSnapshotRef,
            CircleSnapshotMeta,
        )>,
        SnapshotError,
    > {
        let storage = self.storage;
        let root = self.root();
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
                Err(crate::storage::StorageError::NotFound(_)) => break,
                Err(error) => return Err(SnapshotError::Bucket(error)),
            };
            let semantic_hash = CircleSnapshotMeta::semantic_hash_from_bytes(&bytes)
                .map_err(|error| SnapshotError::Parse(error.to_string()))?;
            let reference = crate::protocol::store_commit::CircleSnapshotRef {
                generation,
                snapshot_hash: semantic_hash,
                object,
            };
            let meta = CircleSnapshotMeta::parse_at(
                &bytes,
                root.store_root_hash,
                &reference,
                registration,
            )
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
    activation_commit: crate::protocol::store_commit::StoreBatchCommitRef,
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
impl CircleSnapshotReader<'_, '_> {
    pub(crate) async fn select_staged_decisions(
        &mut self,
        store_frontier: &CommitFrontier,
        restorer_identity: &UserKeypair,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
    ) -> Result<Vec<crate::database::StagedCircleDecision>, SnapshotError> {
        use crate::database::StagedCircleDecision;
        let root = self.root().clone();
        // The stream-activation index the control-stream authority resolves against is
        // written by the pull, which has not run on a freshly restored device; seed it
        // from the retained materializations selection reads anyway.
        let selection = self
            .database
            .prepare_circle_restore_selection(root.clone())
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
        let device_registrations = self
            .database
            .activated_store_device_registration_records()
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;

        let mut decisions = Vec::new();
        for (circle_id, controls) in selection.circles {
            let head = self
                .database
                .circle_restore_head(root.clone(), circle_id, controls)
                .await
                .map_err(|error| SnapshotError::BootstrapState(error.to_string()))?;
            let Some((head_control, head_commit)) = head else {
                warn!(%circle_id, "restore selection: Circle has no head control; clearing coverage");
                decisions.push(StagedCircleDecision::ClearCoverage(circle_id));
                continue;
            };

            let access = self
                .resolve_restorer_access(
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
                super::activation::LocalCircleAccess::NoAccess => {
                    decisions.push(StagedCircleDecision::ClearCoverage(circle_id));
                    continue;
                }
                super::activation::LocalCircleAccess::Active {
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
            for (activation_commit, image) in &selection.preserved_images {
                if image.circle_id() == circle_id {
                    candidates.push(StagedCircleImageCandidate {
                        activation_commit: activation_commit.clone(),
                        image: image.clone(),
                    });
                }
            }
            // Standalone Circle snapshots are sealed under the Circle epoch key the
            // identity's active leaf carries, not the Store routing key.
            if let Some(candidate) = self
                .select_standalone_snapshot_candidate(
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
}

/// Resolve the restoring identity's own access at a Circle's head control. The
/// head control's activating commit is retained, so its verified materialization
/// carries the already-verified control; only the identity's own access envelope,
/// the Store membership checkpoint, and (if the leaf names one) its own bootstrap
/// image are read from storage. This never re-walks the control's covered-head
/// lineage, which a reclaimed restore may no longer retain.
#[allow(clippy::too_many_arguments)]
impl CircleSnapshotReader<'_, '_> {
    async fn resolve_restorer_access(
        &mut self,
        restorer_identity: &UserKeypair,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        circle_id: CircleId,
        head_control: &CircleControlCoord,
        head_commit: &crate::protocol::store_commit::StoreBatchCommitRef,
    ) -> Result<super::activation::LocalCircleAccess, SnapshotError> {
        let commit_lookup = head_commit.clone();
        let root = self.root().clone();
        let owned = self
            .database
            .retained_merge_materialization_by_ref(root, commit_lookup)
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
        super::activation::CircleActivationVerifier::new(self.database, self.storage, self.history)
            .resolve_local_access(
                &commit,
                &reference.reference,
                &reference.control,
                restorer_identity,
                routing_key,
            )
            .await
            .map_err(|error| SnapshotError::BootstrapState(error.to_string()))
    }
}

/// The maximal standalone Circle snapshot across the activated devices whose
/// lineage the retained head control proves, downloaded and verified byte-exact.
#[allow(clippy::too_many_arguments)]
impl CircleSnapshotReader<'_, '_> {
    async fn select_standalone_snapshot_candidate(
        &self,
        circle_id: CircleId,
        head_control: &CircleControlCoord,
        encryption: &EncryptionService,
        routing_key: Option<&crate::protocol::circle::RowRoutingKey>,
        device_registrations: &[crate::protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<Option<StagedCircleImageCandidate>, SnapshotError> {
        let mut installable: Vec<(
            CircleSnapshotMeta,
            crate::protocol::store_commit::StoreBatchCommitRef,
        )> = Vec::new();
        for registration in device_registrations {
            let stream = self
                .load_stream(
                    circle_id,
                    encryption.clone(),
                    registration.reference(),
                    registration.value(),
                )
                .await?;
            for snapshot in stream {
                // A control reclaimed after an epoch close leaves its standalone snapshot
                // superseded by the successor bootstrap the other candidates provide.
                // Resolve retention before the lineage walk: the walk itself reads every
                // covered control's retained activation, so it must not descend into a
                // reclaimed control.
                let retained_control = snapshot.control.clone();
                let activation_commit = self
                    .database
                    .retained_circle_activation_commit_ref(circle_id, retained_control)
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
                let covered = self
                    .database
                    .verified_circle_control_coord_covers(
                        self.root().clone(),
                        circle_id,
                        head,
                        snapshot_control,
                    )
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
            self.root().store_root_hash,
            ProtocolObjectDomain::CircleSnapshotImage,
            encryption.clone(),
        );
        let image_bytes = self
            .storage
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
            self.database.synced_tables(),
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

#[cfg(test)]
mod tests;
